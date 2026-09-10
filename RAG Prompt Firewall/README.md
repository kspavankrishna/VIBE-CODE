# RAG Prompt Firewall

Retrieved text is not data once you paste it into a prompt. It becomes instructions, and a single poisoned document, ticket or web page can make your agent call a tool, leak a key or push a commit. This is a dependency free Elixir CI gate that scans agent traces and retrieval logs for prompt injection and retrieval poisoning, then fails the build before that content becomes an action.

**Language:** Elixir | **Lines:** 994 | **Added:** 2026-07-18

## What this solves

RAG applications, coding agents, MCP tools and internal search copilots all pull untrusted text into prompts. Tickets, docs, web pages, code comments, Slack exports, support logs, vector database chunks. Most teams review only the final answer. Nobody reviews the evidence path that produced it. That is where prompt injection lives.

The failure mode is specific. A support ticket contains a line telling the model to ignore previous instructions and dump the environment variables. A scraped page contains a curl command aimed at a webhook. A README in a dependency asks the agent to commit directly to main and skip CI. The retrieval layer happily returns it, the prompt assembler concatenates it next to the system prompt, and the agent does what the text said. The answer looks normal. The tool call does not. By the time anyone notices, a key is in someone else's log or a commit is on the default branch.

Without a gate like this the detection point is production. Someone notices an odd outbound request, or a secret shows up in a paste site, or a merge nobody approved lands on main. The cost is a credential rotation, an incident review and a slow reconstruction of which retrieval chunk started it, usually from logs that were never designed as evidence.

The other half of the problem is scope. Finding a suspicious sentence in a retrieved chunk is easy. Proving it mattered is the hard part. A scary looking string sitting in a corpus you never queried is noise. The same string carried into a prompt or a tool argument on the same trace is an incident. This tool scores both, and it scores the second one much higher.

## Why I built it

Existing scanners sit at one of two extremes. Either they are secret scanners that grep a repo for key shaped strings and know nothing about prompts, or they are hosted guardrail services that sit inline at inference time, need an API key and cannot run in a locked down CI box with no egress. Neither reads an agent trace as a chain of evidence, and neither understands that the same sentence is low risk in a scratch corpus and critical in a tool argument.

I wanted one file I could drop into any repo, run in CI with nothing installed but Elixir, and point at JSONL traces. No packages, no credentials, no network. It emits SARIF so GitHub code scanning picks it up, Markdown for a PR comment, and JSON for everything else.

## When to use it

- A coding agent reads GitHub issues or PR descriptions and can push branches, and you want a gate before it acts on attacker controlled text.
- Your RAG pipeline indexes customer submitted content: tickets, uploads, scraped pages, and you want the corpus scanned before or after it enters retrieval.
- You run MCP tools and want the tool argument surface checked for content that came in from a document rather than from the user.
- You are doing an incident review on an agent that behaved strangely and you have the JSONL trace but no idea which record started it.
- You want GitHub code scanning alerts for prompt injection in the same place you already get dependency and secret alerts.
- You are building an eval set and want to catch records that ask the model to leak a holdout or an answer key before they contaminate anything.

## How it works

Input is JSONL, plain text or stdin. `RagPromptFirewall.Input.read_records/2` streams each file line by line, and `jsonish?/1` decides per line whether it looks like JSON by checking for a leading `{` or `[`. JSON lines go through `RagPromptFirewall.Json`, a hand written parser in this file that handles strings, escapes, surrogate pairs, numbers via a regex guard and nested containers. There is no Jason, no Poison, no mix project. A malformed line is not dropped: it is rescanned as plain text and also recorded as a parse error, so a broken log still gets checked. `--strict` turns those parse errors into a failing build.

Every line becomes a record via `RagPromptFirewall.Record.from_value/3`. Text is pulled from a wide field list (`@text_fields`: text, content, prompt, message, document, chunk, input, output, arguments, tool_arguments, query, response and more) and if none of those exist the whole object is re-encoded and scanned anyway. Source, role, trace id and kind come from their own field lists, so it reads OpenTelemetry style spans, LangChain style events and homegrown log lines without a schema. The interesting piece is `classify_surface/3`, which labels each record `trusted_prompt`, `tool_args`, `retrieval`, `prompt` or `plain` from role and event type. Trust is separate: `trusted_source?/2` matches a source against `--trusted-source` exact strings and prefixes, and against `--allow-domain` values using URI host parsing with subdomain suffix matching. Record ids are a truncated SHA-256 of file, line and text, so findings are stable across runs.

Detection is ten regex rules in `RagPromptFirewall.Rule.all/0`, each with a base score: RPF001 Instruction Override (34), RPF002 Secret Exfiltration Request (42), RPF003 Tool Call Steering (31), RPF004 Network Dropper (32), RPF005 Prompt Boundary Spoofing (26), RPF006 Encoded Payload Hint (24), RPF007 Data Laundering (29), RPF008 Evaluation Leakage (27), RPF009 Policy Suppression (33), RPF010 Repository Write Steering (35). The patterns are two part with a bounded gap, for example a verb like reveal or dump within 120 characters of api_key, token or .env, which catches the real phrasing without matching every mention of the word token.

The base score is only the start. `context_score/1` adds the part that makes this different from a grep: plus 22 if the hit is in a tool argument, plus 18 if it is in a retrieval record, plus 8 in a prompt, plus 4 for plain text, minus 8 in a system or developer prompt, minus 18 if the source is trusted and plus 4 if the record carries a trace id at all. Scores clamp to 1 to 100. So the same sentence scores 28 in an untrusted plain log and 53 in a tool argument, which is the correct ordering.

Then the propagation pass, rule RPF900. `propagation_findings/3` groups risky untrusted findings on retrieval and plain surfaces by trace id, groups prompt and tool_args records by trace id, and where a trace appears in both it emits a finding scored at `max(worst poison score, 48) + 14`. That is the evidence claim: this poisoned chunk and this action surface are the same request. The finding is anchored at the sink location (the earliest prompt or tool line in that trace) but carries the poison record's source and snippet, which is exactly what you want to read during a review.

Output goes through `RagPromptFirewall.Report`. JSON carries the full result with counts by severity and by surface. Markdown is a findings table capped at 100 rows plus a parse error table capped at 50. SARIF 2.1.0 maps critical and high to `error`, medium to `warning`, the rest to `note`, and puts the finding fingerprint in `partialFingerprints` so GitHub code scanning deduplicates alerts across runs. Severity is threshold driven: at or above `--fail-at` is critical, 70 or above is high, at or above `--warn-at` is medium.

## Usage

```bash
# scan a trace file, JSON report to stdout
elixir RagPromptFirewall.exs trace.jsonl

# CI gate with SARIF for GitHub code scanning and a PR comment body
elixir RagPromptFirewall.exs \
  --input traces/agent-run.jsonl \
  --input traces/retrieval.jsonl \
  --sarif-out rag-firewall.sarif \
  --markdown-out rag-firewall.md \
  --json-out rag-firewall.json \
  --fail-at 80 --warn-at 45 \
  --explain

# pipe from stdin, trust your own corpus and docs domain
cat trace.jsonl | elixir RagPromptFirewall.exs \
  --trusted-source "internal://policy-corpus" \
  --allow-domain docs.example.com \
  --json-out result.json -

# scan a plain text corpus, fail on any malformed line
elixir RagPromptFirewall.exs --plain --strict corpus.txt

elixir RagPromptFirewall.exs --help
elixir RagPromptFirewall.exs --version   # 2026.04.1
```

## Notes

- Exit codes: 0 for pass or warn, 1 when status is fail, 2 for a bad flag or an unwritable report path. `--strict` makes any parse error or unreadable input a fail.
- Detection is regex only. It catches instruction shaped English. It does not catch a paraphrase in another language, a novel phrasing, or an obfuscated payload. RPF006 flags the hint that something is base64 or hex encoded, it does not decode anything.
- Regexes match case insensitively over the record text, so false positives are expected on security documentation and on prompt injection test corpora. That is what `--trusted-source` and `--allow-domain` are for.
- Text is truncated at `--max-text-bytes` (default 200000, floor 128) on a UTF-8 safe boundary. Anything past that in a very large record is not scanned.
- The propagation rule needs a trace id field on both the poisoned record and the sink. Traces without any of the recognised id fields get scanned individually and never produce an RPF900 finding.
- This is an offline batch scanner, not an inline guardrail. It reads logs after the fact. It does not proxy, block or rewrite anything at inference time.
- Standard library only: `:crypto` for hashing, `URI` for host parsing, `Regex`, `File`. No mix project, no deps, no network calls, no credentials.
