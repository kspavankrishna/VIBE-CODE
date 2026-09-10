# Tenant Stream Budget Ledger

Your AI gateway logs already know that one tenant blew past its daily spend, replayed an idempotency key across two traces and streamed a 160k token prompt out of the wrong region. Nothing reads them until the invoice or the incident arrives. This is a single Zig file that reads gateway JSONL and turns it into spend and safety findings with an exit code.

**Language:** Zig | **Lines:** 692 | **Added:** 2026-08-22

## What this solves

Multi tenant LLM infrastructure fails quietly. A gateway in front of OpenAI, Anthropic, Gemini or a local model route emits a line per request with tenant, model, tokens, cost, latency and status. Those lines ship to a log store and get forgotten. The dashboard shows p95 latency and a request count, both fine, while one tenant burns four times its monthly budget in an afternoon of agent loops. Finance notices thirty days later and nobody can reconstruct which tenant or which prompt shape caused it.

The failure modes this file looks for are the ones that do not trip a normal alert. A request that exceeded the model context window gets silently truncated, so the retrieval evidence or the policy preamble you assembled was never in the prompt. It still returns a 200. A tenant reuses an idempotency key with a different trace id, so the replay is billed twice and the fence you assumed existed does not exist. A request lands in a region outside the residency set you promised in the contract. A stream stays open for 110 seconds and pins an edge worker while the client socket waits. A large paid context misses the prompt cache and costs ten times what it should.

Each of these is visible in the log line at the moment it happens. None of them are visible in an aggregate. The cost is real: budget overruns that surface at invoice time, residency violations that surface at audit time, replayed charges that surface when a customer disputes them and truncated context that surfaces as a quality complaint you cannot reproduce.

The other half of the problem is tooling weight. Checking any of this usually means a log pipeline, a warehouse table and a scheduled query. That is a lot of moving parts for a question you want to ask in CI, in a nightly cron or in the first ten minutes of an incident review.

## Why I built it

I wanted one small binary that could sit next to a gateway JSONL file, an OpenTelemetry export, a CDN worker log or a provider billing trace and produce strict evidence without a runtime stack. No Python environment, no Node install, no container. Zig gives a static binary with a known memory story and no dependency tree beyond the standard library, which matters when the thing you are debugging is the infrastructure itself.

The second reason is output shape. A budget guardrail that only prints to a terminal is not much use. This one emits summary, JSON, Markdown and SARIF from the same analysis, so one run serves a human in an incident channel, a script, a review doc or GitHub code scanning.

## When to use it

- A nightly job that reads yesterday's gateway log and fails if any tenant crossed its daily budget
- Post incident review where you need to know which tenant and which line number started the queue backup
- A CI check on a captured traffic sample before shipping a gateway or routing change
- Auditing data residency after adding a new inference region or provider
- Investigating an unexplained provider bill spike across a shared agent platform
- Verifying that idempotency fencing actually works before you turn on replay heavy retries

## How it works

`main` parses flags into a `Config`, reads input from a path or stdin via `readInput` (capped at `max_input_bytes`, 64 MB), calls `analyze` then `render`, and exits with code 2 if `hasSeverityAtLeast` finds anything at or above the `--fail-on` threshold.

`analyze` splits on newlines, skips blanks and `#` comments, and hands each line to `parseEvent`. There is no JSON parser here. `field` is a hand written key scanner that walks the line for the key name, rejects a match whose preceding character is alphanumeric, `_` or `-` (so searching for `id` does not match inside `trace_id`), skips an optional closing quote and whitespace, requires a `:` or `=`, then reads a quoted string or an unquoted run up to a comma, brace or whitespace. That one routine handles JSONL and key=value logs with the same code path. Most fields have fallback names, and `context_tokens` falls back to `tokens_in + tokens_out` when absent.

Money never touches a float. `parseMoneyMicros` strips a leading `$`, parses the dollar part as an integer and walks up to six decimal digits with a descending place value, producing integer micros. All budget arithmetic is integer, including the 85 percent near limit check, which is written as `cost * 100 >= budget * 85` rather than a division. That is what makes the ledger reproducible across runs and machines.

State is three flat `ArrayList`s inside `Analysis`: `tenants`, `findings` and `seen`. Lookups are linear scans, not hash maps. `tenantIndex` scans by name and appends on miss. `rememberRequest` scans `seen` for a matching `(request_key, tenant)` pair, and on a hit it compares `trace_id` and `status`: a mismatch raises a critical `replay-key-drift`, and a third or later repeat with matching fields raises a high `replay-key-repeat`. Linear scan is right at this cardinality and it keeps allocation to one growable buffer per list.

Rules are split in two. `applyEventRules` runs per line and covers `secret-egress` (from a `secret_hit` field or the words secret, api_key, token_leak or credential appearing in status or phase), `region-residency`, `context-window`, `stream-duration`, `queue-saturation`, `latency-slo`, `provider-throttle` above 60 seconds of retry-after, `tool-call-fanout`, `prompt-cache-miss` for an uncached request above 64k context and 50000 micros, and `failed-request` for any status that is `failed`, `error`, `throttled` or begins with a 4 or 5. `applyTenantRules` runs after the pass and covers `tenant-budget`, `tenant-budget-near`, `tenant-cache-efficiency` and `tenant-failure-rate`, each gated on a minimum request count so a three line file does not produce statistical noise.

`sortFindings` orders by severity descending, then line number ascending, then rule name, so the most serious thing is always first and the ordering is stable. `renderSarif` maps critical and high to SARIF `error` and everything else to `warning`, which is what makes GitHub code scanning treat a budget breach as a blocking result.

## Usage

```bash
# Build and run against a gateway log
zig run TenantStreamBudgetLedger.zig -- --input gateway.jsonl --format markdown

# Or pipe on stdin
cat gateway.jsonl | zig run TenantStreamBudgetLedger.zig --

# Full policy example, exits 2 if anything high or worse is found
zig run TenantStreamBudgetLedger.zig -- \
  --input gateway.jsonl \
  --default-daily-usd 250.00 \
  --tenant-limit-usd research=125.00 \
  --tenant-limit-usd sales=40.00 \
  --max-context-tokens 128000 \
  --max-stream-seconds 90 \
  --max-latency-ms 15000 \
  --max-queue-ms 30000 \
  --max-tool-calls 16 \
  --allowed-regions us,eu,apac \
  --fail-on high \
  --format sarif > ledger.sarif

# Built in self test with an embedded three event sample
zig run TenantStreamBudgetLedger.zig -- --self-test

zig run TenantStreamBudgetLedger.zig -- --help
```

`--help` prints the full list of recognised input fields and their fallback names.

## Notes

- Exit codes: 0 clean, 2 when a finding meets or exceeds `--fail-on`, 64 for a bad or missing flag value. `--self-test` returns a Zig error on failure, which surfaces as a non zero exit and a trace.
- The parser is a key scanner, not a JSON parser. It reads one event per line and will not handle pretty printed multi line JSON, escaped quotes inside values or nested objects that repeat a key name. The first match on the line wins.
- `--allowed-regions` treats an empty region or the literal `unknown` as allowed, so logs missing a region field do not flood you with residency findings. Deliberate, and it also means a missing region is never caught.
- `chunk_count` is parsed into the `Event` struct but no rule currently uses it.
- JSON and SARIF output escape the finding message but not tenant names, rule ids or model names. A tenant name containing a quote or a backslash will produce malformed JSON. Sanitise tenant identifiers upstream.
- All string fields in findings and tenant stats are slices into the input buffer, so the analysis is valid only while that buffer is alive. `main` handles this with `defer`, but anyone reusing `analyze` as a library call must respect it.
- Budgets are treated as daily totals over whatever the file contains. There is no time parsing, no windowing and no rollover. Feed it one day per run.
- Tenant and request lookups are linear scans. Fine for thousands of tenants and keys, not designed for millions of distinct idempotency keys in one file.
