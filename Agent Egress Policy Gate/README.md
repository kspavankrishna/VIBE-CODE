# Agent Egress Policy Gate

An AI agent trace tells you a tool call succeeded. It does not tell you which domain received the data, whether a token rode along in the URL or a header, or whether that call broke policy. This is a single file Python CLI that answers those questions before a trace is trusted, replayed, archived or promoted.

**Language:** Python | **Lines:** 680 | **Added:** 2026-08-04

## What this solves

Once you wire an LLM agent into real systems it becomes an unsupervised HTTP client. It calls MCP servers, hits SaaS APIs, drives a headless browser, posts to webhooks and fires model requests that cost money. All of that is egress, and your trace records only that the call happened and returned 200. It does not judge the call, so the failure mode is quiet: an agent posts a customer record to a `webhook.site` URL that a prompt injected three steps earlier, the span turns green, and nobody notices until the data surfaces somewhere it should not be.

The second failure is credential spread. Traces get written to disk, shipped to a vendor, pasted into a ticket and attached to an incident review. If one carries an `Authorization` header, an `sk-` key in a request body or a token in a query string, you have copied that secret into four more places, each with its own retention policy, and the person who finds it is usually an auditor.

Third is the damage nobody calls a security incident. A mutating POST retried without an idempotency key duplicates records. One event burns 400k tokens because a retrieval step dumped a corpus into the prompt. A request runs outside your residency region. None of that shows up in a viewer built for latency and status, and all of it sits in fields you already have.

This gate reads that trace and applies a written policy to it. It normalizes messy JSON, JSONL and CSV records into one event shape, runs nineteen rules over each event, and returns findings with a severity, evidence and a remediation line. Exit code 1 when anything blocking fires, so it runs as a CI step instead of a dashboard someone forgets to open.

## Why I built it

Existing tooling sits on either side of the problem. Egress firewalls enforce at the network layer, which is correct and useless when the agent runs on a laptop, in a hosted sandbox or inside an MCP server you do not control. Observability platforms render the trace and have no opinion about it. Secret scanners read source code, not the traces where secrets end up. Nothing in between reads an agent trace, applies a policy and returns a pass or fail.

The other constraint was install friction. You need this most at 2am in an incident runner and inside a locked down CI job, neither of which is the place to argue with a package index. One file, standard library only, no config needed.

## When to use it

- A CI job on the repo that ships your agent, failing the build when a trace touches an unapproved host
- Incident review, where you need to answer what left the building and where it went
- Before uploading a trace to a vendor or a ticket, using the redacted output as the shareable copy
- Auditing an MCP server you did not write, by capturing its calls and seeing where they actually go
- Feeding findings into GitHub code scanning or any other SARIF consumer

## How it works

Input handling is forgiving because trace exports are not standardized. `iter_records` reads the file or stdin and sniffs: a leading `[` or `{` means JSON, and `extract_records` unwraps the usual container keys (`events`, `data`, `records`, `trace`, `logs`, `tool_calls`, `requests`). `looks_like_csv` checks whether the first line's cells intersect known URL, method and tool header names. Anything else is JSONL, one object per line. A parse failure raises `GateError` naming the file, line and reason.

Field extraction is what makes arbitrary schemas work. `flatten` walks each record and writes every value twice, once under its dotted path and once under its bare leaf name. `first_by_leaf` resolves a field against sets like `URL_KEYS`, `METHOD_KEYS` and `BODY_KEYS`, so `request.url`, `params.endpoint` and `webhook` land in the same slot, and `first_url` falls back to a regex scan over every string when no named URL field exists. `normalize_event` assembles the `Event` dataclass: tool, URL, method, lowercased headers, content type, region, timeout, payload size, tokens, cost and a `record_hash`, the first 16 hex characters of a SHA-256 over the record's canonical compact JSON, so a report can cite an event without reprinting it.

The numeric fields have their own parsers: `parse_timeout` accepts `1500ms` or `30s`, `payload_size` takes the largest UTF-8 byte length across body shaped fields, and `token_count` sums the usage counters when no total is given.

`audit` runs the rules over one event and returns `Finding` objects. Destination rules cover denied domains, unlisted domains, non HTTPS transport and private network egress. Host matching is `fnmatch` globbing, with an extra case so `*.example.com` also matches the bare apex. `private_host` catches `localhost`, any `.local` suffix and, through the `ipaddress` module, anything private, loopback, link local or reserved, the SSRF and cloud metadata case. Credential rules run the `secret_patterns` regexes (`sk-` keys, GitHub `gh*_` tokens, `AKIA` keys, JWTs) against the URL, every header value and the whole record. The rest check methods, required headers, idempotency keys, regions, content type, payload bytes, tokens, cost and timeout.

Redaction is separate from detection. `redact_url` replaces query values whose key is sensitive or whose value matches a secret pattern and strips userinfo before the `@`. `sanitize` recurses through a record, replacing sensitive keys by name and running `redact_text` over every string, and `--redacted-output` writes those records as JSONL for sharing. `build_report` counts findings by severity, lists the destination hosts and sets `pass` to false when a finding carries a severity in `fail_on` (`critical` and `high` by default). Four renderers share that report: JSON, Markdown, CSV and SARIF 2.1.0.

## Usage

```bash
# Default strict policy, JSON report to stdout
python3 AgentEgressPolicyGate.py --trace agent-trace.jsonl

# Your own policy, Markdown report to a file
python3 AgentEgressPolicyGate.py \
  --trace exports/run-4417.json \
  --policy egress-policy.json \
  --format markdown \
  --output reports/run-4417.md

# CI gate: SARIF for code scanning, block on medium and above,
# plus a sanitized copy of the trace that is safe to share
python3 AgentEgressPolicyGate.py \
  --trace trace.csv \
  --policy egress-policy.json \
  --format sarif --output egress.sarif \
  --fail-on critical,high,medium \
  --redacted-output trace.redacted.jsonl

# Discovery run: read stdin, do not fail on unknown domains
cat run.log | python3 AgentEgressPolicyGate.py --trace - --allow-unlisted-domains
```

A minimal policy file. Anything you leave out keeps the built in default:

```json
{
  "allowed_domains": ["api.stripe.com", "*.githubusercontent.com", "hooks.slack.com"],
  "allowed_regions": ["us-east-1", "eu-west-1"],
  "max_payload_bytes": 65536,
  "max_cost_usd_per_event": 1.5,
  "fail_on": ["critical", "high"]
}
```

## Notes

- Exit codes: 0 when the report passes, 1 when a blocking finding fires or the output pipe closes, 2 for a policy or input error such as malformed JSON or a bad `secret_patterns` regex.
- Python 3 standard library only. No packages, no network calls, no writes outside the paths you pass on the command line.
- It audits, it does not enforce. Nothing blocks a request in flight. It gates evidence, not traffic.
- Out of the box `allowed_domains` is empty and `allow_unlisted_domains` is false, so every external host raises `UnlistedDestination` at high severity. Build the allowlist from a discovery run with `--allow-unlisted-domains` first.
- Secret and prompt injection detection are regex and substring heuristics, so `PromptInjectionForwarded` will flag benign text that discusses system prompts. Triage signals, not proof.
- `flatten` writes bare leaf names alongside dotted paths, so two nested fields sharing a leaf name collide and the last one wins. Lists are walked to 50 elements.
- `domain_owners` sits in the default policy but no rule reads it, and there is no findings dedup, so one event tripping several rules produces several findings.
