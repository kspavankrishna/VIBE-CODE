# MCP JSON RPC Profiler

MCP servers look fine in a demo and then quietly burn minutes per agent turn once real developers hit them. This is a single file Swift tool that reads a JSONL transcript of Model Context Protocol JSON-RPC traffic and tells you which method is slow, which one errors, which request ids got reused and which requests never came back.

**Language:** Swift | **Lines:** 894 | **Added:** 2026-05-24

## What this solves

This solves the annoying April 2026 problem where MCP servers, coding agents, local gateways and editor plugins look fine in a demo but silently burn minutes on slow tools, reused JSON-RPC ids, missing responses and flaky tool errors once they hit real developer workflows. The demo transcript is twenty lines long. The real one is forty thousand lines from a day of agent work, and nobody reads it.

The failure modes are specific and they all cost time. A `tools/call` that takes 400ms in local testing takes 9 seconds against a cold database, and because the agent waits synchronously, every turn in the session inherits that latency. A server that reuses JSON-RPC request ids across a session gets responses matched to the wrong request, which shows up later as an agent confidently reporting the output of a different tool. A request that never receives a response leaves the client blocked until its own timeout fires, and the transcript is the only place that fact is visible. An error rate that creeps from 1 percent to 12 percent after a dependency upgrade looks like nothing in any single log line.

Nobody notices any of this from inside the agent. The engineer notices it as "Claude Code feels slow today" or "the tool keeps failing and I don't know why". The transcript already contains the answer. What is missing is something that reads the ledger and gives you numbers plus a non zero exit code. Without a gate in CI, a regression in a tool server ships and the cost lands on every engineer using it, spread thin enough that no single person files a bug. That is the worst kind of production failure: real, expensive and invisible in aggregate.

## Why I built it

Existing tooling is either too big or too far away. APM platforms want you to instrument the server, ship spans to a collector and pay per host, which is a lot of ceremony for a stdio process reading newline delimited JSON. SaaS trace dashboards mean uploading transcripts that contain file paths, prompts and internal tool arguments, and nobody wants that leaving the laptop. Generic log analyzers do not understand that a JSON-RPC id is a correlation key or that a missing response is a real defect.

I wanted something boring and repeatable that Codex, Claude Code, Cursor, OpenCode, GitHub Actions and internal agent runners could all use the same way: point it at raw JSONL, get p50, p95, p99, error rate, stuck request and duplicate id checks, and let it fail the build before a bad tool server wastes a day. One Swift file, no package manifest, nothing beyond Foundation.

## When to use it

- A CI step that runs a scripted agent session, captures the MCP transcript and fails the pipeline when `tools/call` p95 crosses your budget.
- Triaging a specific complaint like "the agent got slow this week" against yesterday's and today's transcripts.
- Auditing a third party MCP server before you let it into the team's default config.
- Investigating an agent that hangs, where you suspect the server never answered a particular request.
- Confirming that a server correctly separates concurrent sessions when you multiplex several connections through one gateway log.
- Producing a Markdown latency table to paste into a pull request that touches tool server code.

## How it works

Input is newline delimited JSON, read through `LineReader`, which pulls 64KB chunks off a `FileHandle`, scans for byte 10 and hands back one line at a time with a trailing `\r` stripped. It never loads the file into memory, so a multi gigabyte transcript costs you one buffer. Inputs can be file paths or `-` for stdin, and with no `--input` at all it reads stdin directly.

`EventExtractor` turns each line into a `JsonRpcEvent`. Real transcripts are rarely bare JSON-RPC, so `unwrapMessage` first checks whether the top level object already looks like JSON-RPC via `looksLikeJsonRpc` (presence of `jsonrpc`, a `method`, or an `id` alongside `result` or `error`), and if not it walks the wrapper keys `message`, `payload`, `body`, `event`, `jsonrpc_message` and `rpc`. Those values can be objects or JSON encoded strings, which is what log shippers usually produce, and `parseNestedJSONString` handles the string case. `classify` then applies the JSON-RPC rules directly: method plus id is a request, method alone is a notification, id plus `result` or `error` is a response, anything else is unknown and just gets counted.

Correlation is the core of the tool. Every request is stored in a `pending` dictionary keyed by `session::id`, built by `JsonRpcEvent.pendingKey`. The session comes from `session`, `session_id`, `connection`, `connection_id`, `transport_id`, `trace_id` or `run_id` on either the message or the wrapper, falling back to `default`, so multiplexed logs do not collide and single session logs still work. When a response arrives, `processResponse` removes the matching entry and computes the duration. Writing into a key that already holds an open request is an error finding for id reuse. A response with no open request is an error. A response with no id is an error. A request with no id is a warning, because it cannot be matched at all.

Timestamps come from `TimestampParser`, which tries `timestamp`, `ts`, `time`, `@timestamp`, `observed_at` and `created_at` on the message first and then the envelope, plus explicit `ts_ms` and `ts_ns` fields. Numeric values above 10,000,000,000 are treated as milliseconds since epoch and anything smaller as seconds, which is the usual heuristic and covers both conventions. Strings are tried as ISO 8601 with fractional seconds and then without. If either side of a pair has no timestamp the call still counts toward completions and errors but increments `missingTimestampPairs` instead of contributing a duration. If the response timestamp is earlier than the request timestamp, the duration is discarded, `negativeClockPairs` increments and a warning is recorded, so a clock skew never turns into a fake fast call.

Per method aggregation lives in `MethodStats`. Durations are kept as a plain array and `percentile` sorts them and interpolates linearly between the two neighbouring ranks, so p95 on eight samples gives a real interpolated value rather than a nearest sample jump. Error rate is errors over completed calls, not over requests, so open requests do not dilute it. `checkBudgets` then walks methods ordered by p95 descending and raises an error finding when p95 exceeds the per method budget from `--method-budget` or the global `--max-p95-ms`, and another when error rate exceeds `--max-error-rate`. Both gates are suppressed until a method has at least `--min-samples` completed calls, which keeps a single unlucky cold start from failing your build. `checkOpenRequests` reports everything left in `pending` at end of input, as an error by default and as a warning under `--allow-open`.

Output is three renderers over the same data. `renderText` prints a padded column table via `TextTable`, `renderMarkdown` prints a GitHub table with pipes escaped and `renderJSON` emits a pretty printed, key sorted object with `summary`, `methods` and `findings`. The process exits 0 when no error severity finding exists and 2 when one does.

## Usage

```bash
# make it executable, or run it through swift directly
chmod +x McpJsonRpcProfiler.swift
./McpJsonRpcProfiler.swift mcp-transcript.jsonl

# equivalent
swift McpJsonRpcProfiler.swift --input mcp-transcript.jsonl

# pipe a live capture in
cat mcp-transcript.jsonl | swift McpJsonRpcProfiler.swift --markdown

# CI gate: tight budget on tool calls, looser everywhere else
swift McpJsonRpcProfiler.swift \
  --input session-a.jsonl --input session-b.jsonl \
  --method-budget tools/call=2500ms \
  --method-budget resources/read=800ms \
  --max-p95-ms 10s \
  --max-error-rate 2% \
  --min-samples 5 \
  --json > mcp-profile.json
echo "exit: $?"

# investigating a hang, keep open requests as warnings only
swift McpJsonRpcProfiler.swift --allow-open --stale-ms 30s transcript.jsonl

# just the numbers, no pass/fail gates
swift McpJsonRpcProfiler.swift --no-default-budgets --markdown transcript.jsonl

swift McpJsonRpcProfiler.swift --help
```

Durations accept a bare number of milliseconds or a suffix of `ms` or `s`. Rates accept `0.05` or `5%`. Defaults are p95 30000ms, error rate 5%, min samples 3, stale 120000ms and failing on open requests.

## Notes

- Exit codes: 0 pass, 2 a budget or correlation error finding fired, 64 bad usage or a malformed input line, 66 an input file could not be opened.
- A single line that is not valid JSON, or whose top level value is not an object, aborts the whole run with exit 64 and a `source:line` message. It does not skip and continue. JSON-RPC batch arrays are therefore not supported.
- `--stale-ms` is parsed and the request age is printed in the open request message, but it does not currently change any severity: the branch that reads it only reassigns warning to warning. Treat it as informational for now.
- `direction` is parsed from `direction`, `dir`, `side` or `flow` and stored on each event, but nothing in the report uses it yet.
- Timestamps are taken from the log, not measured by this tool. If your transcript has no timestamps you still get request, completion, error and notification counts plus correlation checks, with every latency column showing `-`.
- Duplicate id detection is per session key. If your log has no session field, all traffic is bucketed under `default`, which is correct for a single stdio connection and will produce false id reuse findings for a multiplexed log that omits session ids.
- Requests without an id are counted in the method's request total but never correlated, so `requests` can legitimately exceed `completed` without any of them being stuck.
- Foundation only, no third party packages. It runs as a script under `swift`, and it compiles with `swiftc` if you want a binary in CI.
