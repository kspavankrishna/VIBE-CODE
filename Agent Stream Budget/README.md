# Agent Stream Budget

LLM and agent traces are newline JSON that nobody reads until the bill arrives. This is a single file Zig CLI that reads JSONL traces, groups them by session, and fails your build when a session crosses a token, cost, tool latency, stream stall, error rate or egress threshold.

**Language:** Zig | **Lines:** 685 | **Added:** 2026-07-16

## What this solves

The failure mode is always the same shape. Someone changes a prompt, adds a tool, or bumps a retry count. Tests pass, because tests assert on output correctness, not on what the run cost or how long a tool blocked the stream. The change ships. Three days later finance asks why the model line item tripled, or a customer says the assistant "just stops talking" halfway through an answer. You go digging in a dashboard that samples a fraction of traffic and aggregates away the exact session you need.

Concretely: a context window that grows from 40k to 190k input tokens per turn because a retriever started stuffing more chunks. A tool that answered in 800 ms and now takes 50 seconds against a cold database. A 14 second gap between stream deltas that no error log records, because nothing errored, the user just closed the tab. A retry loop firing 300 tool calls while every individual call succeeds. An agent echoing whole documents into responses and pushing 200 MB of egress from an edge function. None of these throw. All of them cost money, and the person who notices is a customer or an accountant, not an engineer.

This turns them into a pre merge check. It reads trace files you already produce, from Vercel AI SDK streams, OpenAI Responses API traces, LangGraph runs, an AI gateway audit log, MCP tool logs or an OpenTelemetry style JSONL exporter, computes per session totals, and compares them against a JSON policy. If a session breaches, the process exits 2 and CI goes red with the exact numbers printed. Deterministic, no network, no vendor SDK, no sampling. It also absorbs schema chaos: `input_tokens`, `prompt_tokens`, `tokens_in` and `promptTokenCount` all work, so you do not need a normalization layer in front of the gate.

## Why I built it

Observability for this lives in dashboards, and dashboards are for after the incident. They tell you what happened last Tuesday and do nothing to stop the deploy that causes next Tuesday. The gap is a small deterministic checker that runs where your unit tests run, reads a file, and returns a nonzero exit code. Nobody ships that, because it feels too small to build and too vendor specific to generalize.

The other reason is trust. A gate that fails a build has to be explainable in a pull request or people disable it within a week, so the arithmetic here is deliberately boring: integers, sums, maximums, a ceiling division for error rate, and every breach printed as `actual > limit`.

## When to use it

- A CI job on every PR touching prompts, agent graphs, tool definitions or retriever settings, reading the trace from an eval run.
- A nightly pass over yesterday's gateway audit log to catch cost drift before it becomes a month.
- After a customer reports the assistant "freezing", to find which sessions had stream gaps above your SLO.
- Reviewing a new MCP server before enabling it, to see its worst tool latency and failure rate across a replayed workload.
- Catching egress from an edge function or agent worker that started returning far more bytes than it used to.
- Merging traces from several services that use different field names, without writing a normalizer first.

## How it works

Entry point is `main`. It builds a `GeneralPurposeAllocator`, calls `parseArgs` into a `Config`, loads a `Policy` (defaults if `--policy` is absent), runs `loadInputs` into a `Monitor`, renders, then exits 2 if `--fail-on-risk` was passed and `Monitor.anyPolicyFailure` returns true. Each stage owns an exit code, so a CI script can tell a bad policy file from a bad trace file.

Ingestion is a line scan, not a document parse. `Monitor.ingestBuffer` splits on `\n`, trims whitespace, skips blanks and hands each line to `ingestLine`, which parses it with `std.json.parseFromSlice` and frees it immediately. Peak memory is one line plus the session table, not the file. A line that fails to parse increments `invalid_lines` and is skipped rather than aborting, because real log files get truncated mid write. Whether that fails the build is a policy decision, controlled by `fail_on_invalid_json`.

Field extraction is alias driven. `textField`, `intField` and `numberField` take candidate key lists and return the first that resolves to the right type, and `valueAsU64` also accepts numeric strings and non negative floats. Session identity resolves through `--session-key` first, then `session_id`, `trace_id`, `run_id`, `request_id`, `conversation_id`, `thread_id`, falling back to an `unknown` bucket counted in `anonymous_events`. Sessions live in a `std.StringHashMap(SessionStats)` where `getOrCreateSession` dupes the id once and uses the owned copy as both key and value field, so the map never points at freed JSON memory.

Classification is substring matching through `containsNoCase`. An event is a tool call if it carries a tool name field or its event name contains `tool` or `function_call`. It is stream like if the name contains `stream`, `delta` or `chunk`. It is a failure if the event name or a `status`, `outcome` or `result` field contains `error`, `fail`, `timeout`, `429` or `5xx`, or if the object has an `error` key at all. Heuristic on purpose: precise classification needs a schema, and needing a schema is what stops people adopting the gate.

`timestampField` prefers explicit millisecond keys then falls back to `timestamp`, `ts`, `time` or `created_at`. Integers pass through `normalizeEpoch`, which multiplies by 1000 when the value looks like epoch seconds. Strings go to `parseTimestampString`, handling all digit epochs and fixed offset ISO 8601 with fractional seconds via `daysFromCivil`, the standard days from civil algorithm, then `epochMsUtc`. Stall detection is cheap by design: `observeTimestamp` keeps the previous timestamp and tracks the largest forward delta into `max_gap_ms`, so one pass gives the worst stream gap in the session.

Cost is unsigned integer micro dollars, never floats. `dollarsToMicros` converts a `cost_usd` style float once at the boundary with half up rounding and everything after is integer addition, so a report is identical across runs and machines. Tool error rate is per ten thousand rather than a percentage, computed with `divCeil` so a single failure in a long session never rounds to zero. `SessionStats.riskCount` evaluates nine independent thresholds and returns how many are breached, `renderReasons` prints each as `actual > limit`, and `renderJson` emits a stable `agent-stream-budget/v1` document.

## Usage

```bash
# help and the field names it recognizes
zig run AgentStreamBudget.zig -- --help

# one trace file, text report, built in default policy
zig run AgentStreamBudget.zig -- --input traces.jsonl

# merge several files, custom budget, fail the build on breach
zig run AgentStreamBudget.zig -- \
  --input gateway.jsonl \
  --input agent-worker.jsonl \
  --policy budget.json \
  --fail-on-risk

# read stdin, emit machine readable JSON for a later CI step
kubectl logs deploy/agent-worker | zig run AgentStreamBudget.zig -- --format json > budget.json

# group by a field this tool does not know about
zig run AgentStreamBudget.zig -- --session-key run_uuid --input traces.jsonl

# raise the per file read cap for a very large log
zig run AgentStreamBudget.zig -- --input huge.jsonl --max-input-bytes 1073741824

# build once, run fast in CI
zig build-exe AgentStreamBudget.zig -O ReleaseFast
./AgentStreamBudget --input traces.jsonl --policy budget.json --fail-on-risk
```

Bare arguments with no flag are treated as input paths, so `AgentStreamBudget *.jsonl` works. A policy file is a JSON object whose keys mirror the `Policy` struct, plus a few friendlier aliases:

```json
{
  "max_input_tokens": 180000,
  "max_output_tokens": 120000,
  "max_total_tokens": 200000,
  "max_tool_calls": 40,
  "max_tool_latency_ms": 30000,
  "max_stream_gap_ms": 8000,
  "max_egress_bytes": 67108864,
  "max_cost_usd": 0.35,
  "max_error_rate_percent": 5,
  "fail_on_invalid_json": true
}
```

`max_cost_usd` and `max_usd` override `max_cost_micros`, `max_error_rate_percent` and `max_error_percent` override `max_error_rate_per_10k`, and any key you omit keeps its default.

## Notes

- Exit codes: 0 clean, 2 policy failed, 64 bad arguments, 65 bad policy file, 74 input read error. Without `--fail-on-risk` it always exits 0 and only prints the report, the right mode for a nightly informational job.
- Token counts are summed across every event in a session. If your provider emits cumulative usage on each delta rather than per event deltas, totals inflate. Verify one session by hand first.
- Tool calls are counted per matching event, so a trace logging both `tool_call_start` and `tool_call_end` counts two. Tune `max_tool_calls` to your trace shape.
- ISO 8601 strings shorter than 20 characters are ignored, so `2026-07-16T12:00:00` is skipped while `2026-07-16T12:00:00Z` parses. Only `Z` and fixed numeric offsets are handled. Gap detection assumes lines are roughly in time order, because an out of order event does not extend the gap.
- Egress estimation from stream bodies only applies while a session's `egress_bytes` total is still zero. Once an explicit byte field lands the estimator stops contributing, so log `bytes_out` or `response_bytes` if you want accuracy.
- Failure detection is substring matching, so `error_recovery_succeeded` counts as a failure, and so does any object carrying an `error` key even when it is null. Report ordering follows hash map iteration order and is not stable across runs.
- No dependencies beyond the Zig standard library, no network access, no config discovery. `--max-input-bytes` caps each file and stdin separately, defaulting to 256 MB.
