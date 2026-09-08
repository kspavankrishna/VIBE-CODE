# Agent Consent Receipt Ledger

An AI agent deleted a production file, pushed to a repo or emailed a customer, and the only evidence a human approved it is a sentence buried in a chat transcript. This is a single file Ruby CLI that reads agent tool call logs and proves, or disproves, that every risky call was covered by a scoped consent receipt.

**Language:** Ruby | **Lines:** 1477 | **Added:** 2026-08-20

## What this solves

Agentic systems now write to real infrastructure. MCP servers, GitHub and Gmail connectors, browser automations, shell tools and deployment bots all take consequential actions, and the audit trail they leave is a conversation log. After an incident the question is never "what did the model say", it is "who approved this action, on this resource, at what time". A transcript shows an approval somewhere above the call and leaves you guessing whether it covered what happened.

The failure modes are boring and repeatable. A human approves one write, the agent reuses that approval for four more calls in the session. An approval granted at 09:00 for staging is picked up at 17:00 against production. A user clicks deny, the runner records it, and the call still runs because nothing cross checks the two records. A bearer token lands in a tool argument and sits in your log pipeline forever. Worst case a customer gets an email they should not have, or a production object is gone, and nobody can say whether a human authorized it.

The quieter cost is that you cannot prove the negative afterwards, so every similar action in the last six months becomes suspect. This tool treats an approval as a receipt with a scope: tool pattern, resource pattern, domain list, expiry, use count, actor and tenant. It walks the log, classifies every call for risk, and checks whether a matching receipt existed at that moment. Out comes a severity ranked finding list, a nonzero exit code when the run breaches your gate, and SARIF for code scanning.

## Why I built it

Existing tooling covers the wrong layer. Runtime permission prompts stop an action in the moment but leave nothing verifiable behind. LLM observability platforms record traces and token counts, not authorization semantics. SIEM rules expect structured audit events that agent runners mostly do not emit. Nobody ships the piece in between: a deterministic checker that reads the JSON your runner already writes and answers whether consent covered the action.

The other reason is portability. Codex, Claude, the OpenAI Responses API, MCP servers and homegrown runners each nest the tool name, arguments and timestamp somewhere different, and a checker that makes you normalize logs first never gets run.

## When to use it

- A CI gate on a repo where agents open pull requests, failing the build when a run touched files outside the approved scope.
- Post incident review, establishing whether the destructive call had an approval and what it covered.
- A nightly sweep over yesterday's MCP logs for secrets that leaked into tool arguments.
- Vetting a new connector by replaying a session and seeing which calls come back with no receipt.
- Catching egress when an agent talks to a domain outside your allow list.

## How it works

`InputParser` reads files or stdin, tries a whole document JSON parse first, and unwraps an object by finding the first array under `events`, `records`, `rows`, `data`, `items`, `messages` or `entries`. Otherwise it falls back to JSONL and names the offending line. Each row becomes an `InputRecord` carrying source, line and index, so every finding points back at it.

`EventNormalizer` does the shape agnostic part. Rather than assume a schema it holds ranked candidate path lists, `TOOL_PATHS`, `ARG_PATHS`, `RESOURCE_PATHS`, `RECEIPT_PATHS`, `APPROVAL_PATHS` and `TIME_PATHS`, walked by `HashTools.deep_fetch` with case insensitive key lookup. When the resource is not in a top level field it falls back to `deep_find_first`, a breadth first walk over nested arguments hunting for `url`, `path` or `repository`. `approval_value` collapses `approved` and `granted` to true, `denied` and `revoked` to false.

`ReceiptLedger` turns each approval event into a `ConsentReceipt`. Scope comes from a `scope`, `consent_scope` or `authorization` sub object, falling back to the raw record, and an absent list defaults to `["*"]` so an unscoped approval reads as wildcard rather than silently narrow. Expiry is the explicit `expires_at`, else issue time plus the policy TTL, default 24 hours. A receipt with no id gets one: 16 hex characters of a SHA256 over source, line, index, actor and timestamp.

`RiskClassifier` decides which calls need a receipt. It joins tool name, action, resource and a 2 KB slice of the arguments, then matches word boundary hint lists, `DESTRUCTIVE_HINTS`, `WRITE_HINTS`, `NETWORK_HINTS`, `SHELL_HINTS` and `EMAIL_HINTS`, giving categories like `destructive`, `write`, `network`, `repo_mutation` and `privileged`. Secret material is separate, matched against `DEFAULT_SECRET_PATTERNS`: AWS `AKIA`, Google `AIza`, GitHub `gh[pousr]_`, `sk-` and Slack `xox` tokens, PEM private key headers and a generic `api_key: value` assignment. Any category means a receipt is required, otherwise the call passes if it matches a safe tool pattern and falls to `require_approval_for` if not.

`ReceiptMatcher` is the core check. With an explicit receipt id it returns `:missing`, `:denied`, `:expired`, `:scope_mismatch`, `:reused` or `:ok`, in that order, so a denial always beats a scope argument. With no id it filters receipts to those not denied, not expired at the event time, same tenant, issued before the call, in scope on tool, resource and domain together, and under their use limit. Zero survivors is `:missing`, more than one is `:ambiguous`, exactly one is consumed. Use counts live in a `Hash.new(0)` keyed by receipt id, which is what makes single use enforcement real: the second call against a `max_uses: 1` receipt gets `:reused`.

`Analyzer` turns all of that into `Finding` structs: `DUPLICATE_CONSENT_RECEIPT`, `BROAD_CONSENT_SCOPE`, `SECRET_LIKE_TOOL_INPUT`, `DENIED_TOOL_USED`, `UNAPPROVED_NETWORK_DOMAIN`, `UNAPPROVED_WRITE_RESOURCE`, plus the matcher statuses as `MISSING_CONSENT_RECEIPT`, `DENIED_ACTION_EXECUTED`, `EXPIRED_CONSENT_RECEIPT`, `OUT_OF_SCOPE_CONSENT_RECEIPT`, `REUSED_SINGLE_USE_RECEIPT` and `AMBIGUOUS_CONSENT_RECEIPT`. A missing receipt escalates to critical when the call was destructive or carried secret material. `Reporter` renders JSON, SARIF 2.1.0 or a Markdown table, `failed?` compares the worst rank against the gate, and the secret finding never prints the secret: its evidence is a truncated SHA256 digest, so the report is safe to attach to a ticket.

## Usage

```bash
# Built in regression run: a 7 event fixture asserting 6 expected findings.
ruby AgentConsentReceiptLedger.rb --self-test

# Starter policy to edit.
ruby AgentConsentReceiptLedger.rb --example-policy > consent-policy.json

# Check a JSONL run log, human readable output.
ruby AgentConsentReceiptLedger.rb --input run.jsonl --policy consent-policy.json --format markdown

# Pipe from stdin, emit SARIF for code scanning.
cat run.jsonl | ruby AgentConsentReceiptLedger.rb --format sarif > agent-consent.sarif

# Policy on the command line. Positional files work too.
ruby AgentConsentReceiptLedger.rb run.json \
  --allow-domain api.github.com \
  --allow-write-root "kspavankrishna/VIBE-CODE" \
  --deny-tool "github._delete_file" \
  --safe-tool "*.read" \
  --fail-on medium
```

Input shape, both records in one array or JSONL stream:

```json
{"type":"approval","timestamp":"2026-04-10T12:00:00Z","receipt_id":"ship-ruby-ledger",
 "approved":true,"actor":"pavan","tenant":"personal",
 "scope":{"tools":["github._create_file"],"resources":["kspavankrishna/VIBE-CODE"],
          "domains":["api.github.com"],"max_uses":1,"expires_at":"2026-04-10T13:00:00Z"}}
{"type":"tool_call","timestamp":"2026-04-10T12:00:30Z","tool":"github._create_file",
 "receipt_id":"ship-ruby-ledger","arguments":{"repository":"kspavankrishna/VIBE-CODE"}}
```

Policy keys, all optional: `approval_ttl_minutes`, `max_uses_per_receipt`, `fail_on`, `require_approval_for`, `deny_tools`, `safe_tools`, `allowed_domains`, `allowed_write_roots` and `secret_patterns`. List flags merge into the file rather than replace it. As a library, `Analyzer.new(records, Policy.new(hash)).run` gives `.findings`, `.receipts`, `.summary` and `.failed?`.

## Notes

- Exit codes: 0 clean, 2 when a finding meets or exceeds `fail_on` (default `high`), 1 on a parse error or missing file. That exit code is the CI gate.
- Verification after the fact, not enforcement. It cannot block a call, it is only as honest as the runner that wrote the log, and receipts are not cryptographically verified, so a forged approval record is accepted as real.
- Classification is keyword based, not semantic. Words like `merge`, `move` and `send` sit in the hint lists and match tool name, action, resource and arguments together, so expect false positives. Tune with `safe_tools`.
- Time checks degrade quietly. Expiry and ordering pass when either side has no parseable timestamp, so an undated log gets scope checks but no expiry check.
- Secret scanning reads at most the first 16 KB of serialized arguments, and an empty `allowed_domains` or `allowed_write_roots` means allow everything, not deny everything.
- Ruby standard library only, no gems, no network calls, no state on disk. The whole log loads into memory, which suits run sized logs and not multi gigabyte archives.
