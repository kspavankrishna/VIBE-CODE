# Agent Trace Protocol Checker

A Racket tool that reads a JSONL trace of an AI agent run and tells you where the tool call protocol was broken. It finds leaked tool calls, orphan results, reused call ids, sequence gaps and calls made after a cancel.

**Language:** Racket | **Lines:** 666 | **Added:** 2026-09-29

## What this solves

Every agent framework writes some kind of event log. A run starts, the model asks for a tool, the tool answers, the run ends. When it all works nobody reads the log. When it does not work you get a support ticket that says "the agent hung" or "it answered with stale data" and a JSONL file with forty thousand lines in it.

Most of the bugs in those files are protocol bugs, not model bugs. A tool call was opened and never closed, so the run sat waiting. Two calls shared an id, so the second result got attached to the wrong call. A result arrived for a call that was never made, usually because a retry layer replayed an old response. The client kept starting tools after the user pressed cancel. Events came out of order because two writers shared one file. The last line is cut off because the process was killed mid write.

You can find every one of these by eye if you have an hour. Agent Trace Protocol Checker finds them in a second and gives each one a stable code, a line number, the run id and the sequence number. That makes it usable in CI, in a log pipeline, as a gate on a replay test suite or as the first thing you run on a bad trace from a customer.

If you searched for an agent trace validator, an LLM tool call log checker, a JSONL agent log linter or a way to detect orphan tool results and leaked tool calls, this is the thing I wanted and could not find.

## Why I built it

I kept writing the same throwaway script. Somebody would paste a trace, I would open a scratch file, write a loop with a hash of open calls and print whatever looked wrong. Each time the script was slightly different and each time it missed a case the last one had caught. I wanted one checker with the rules written down, tested and boring.

I picked Racket for a real reason. A trace checker is a small state machine over a stream of records. Racket is very good at that kind of code: structs, hash tables, pattern of small pure helpers and a test submodule that lives in the same file. The whole thing loads with `racket` alone. There is no build step, no package install and no runtime dependency beyond the standard distribution. You copy one file to a CI runner that has Racket and it runs.

I also wanted the failure modes to be explicit. A checker that says "trace invalid" is useless. This one has 23 named codes, each with a fixed severity, and the code table (`CODES`) is exported so other tools can read it.

## When to use it

Use it when you own something that emits agent events and you want a cheap correctness gate.

- After an integration test that runs an agent against a fake model. Assert that the trace is clean.
- In a nightly job that samples production traces. Alert on any error code that was not there yesterday.
- When debugging a hung run. The leak report names the exact call that never returned.
- When you change a retry, timeout or cancel path. Run the checker on before and after traces and diff the counts.
- When you receive a trace from a user or another team and need to know whether the trace itself can be trusted before you read it.

Do not use it to judge the quality of what the model said. It never looks at message content. It only checks that the envelope and the tool call lifecycle are consistent.

## How it works

### The event shape

Each line is one JSON object. Four fields are required on every event: `run` (a non empty string), `seq` (a non negative integer), `ts` (milliseconds, a non negative number) and `type`. The known types live in `KNOWN-TYPES`: `run_start`, `tool_call`, `tool_result`, `tool_error`, `message`, `cancel` and `run_end`.

A `tool_call` also needs `id` and `tool`. A `tool_result` or `tool_error` needs `id` and may carry `tool`. A `tool_call` may carry `args_hash`, any string you compute yourself, which turns on loop detection. Extra fields are ignored, so you can keep your own payload in the same line.

Runs can be interleaved in one file. State is kept per `run`, so run a and run b can reuse the same call ids without a false alarm.

### One pass, bounded memory

`check-port` reads the input one line at a time with `read-line` and feeds each parsed object to `check-event!`. Nothing holds the whole file. What it does keep per run is a small `run-state`: the last seq, the last timestamp, the table of open calls, the set of call ids ever used, counters and a flag for cancel and end. Memory grows with the number of distinct call ids, not with the number of bytes in the trace.

Blank lines are skipped. CRLF endings are fine. `parse-line` reads one JSON value and then insists the rest of the line is whitespace, because some Racket versions quietly accept trailing garbage and I did not want a corrupted line to pass.

### What is checked

Envelope checks come first. If the envelope is broken, `envelope-problems` returns every problem at once and the event is reported as `E_FIELD` and not processed further.

Sequence checks live in `check-sequence!`. Within a run, `seq` must grow by exactly one. The same value twice gives `E_SEQ_DUP`. A smaller value gives `E_SEQ_ORDER`. A jump gives `E_SEQ_GAP` and the message says how many events are missing, which is the number you want when you suspect dropped log lines.

Clock checks live in `check-clock!`. A timestamp that falls behind the previous one by more than `skew-ms` gives the warning `E_TS_BACKWARDS`. The default tolerance is 50 ms because real machines disagree by a little.

Lifecycle checks live in `check-start!` and `dispatch!`. A run must begin with `run_start` (`E_START`, `E_DUP_START`) and nothing may follow `run_end` (`E_AFTER_END`).

Tool call checks live in `on-call!` and `on-result!`:

- `E_CALL_DUP_ID`: an id was reused. The second call is not recorded, so its result will show up as a double result instead of silently closing the wrong call.
- `E_ORPHAN_RESULT`: a result for an id that was never called.
- `E_DOUBLE_RESULT`: a second result for a call that already finished.
- `E_TOOL_MISMATCH`: the result names a different tool from the call.
- `E_CALL_AFTER_CANCEL`: a new call started after `cancel`. Results for calls already in flight are still fine, because a cancelled run has to drain.
- `E_CONCURRENCY`: more calls in flight than `max-inflight`. It reports only when a new peak is reached, so a fan out of fifty calls gives one line and not forty.
- `E_CALL_BUDGET`: the run passed `max-calls`. Reported once.
- `E_LATENCY`: a result took longer than `max-latency-ms`. This is a warning.
- `E_REPEAT`: the same `tool` and `args_hash` was called more than `repeat-limit` times in a row. This is the cheapest useful loop detector I know. It warns once per streak.

### Leaks, cancels and cut off traces

`on-end!` calls `report-open-calls!` when a run ends. Every call still open is reported oldest first, using the line where the call was made, which is where you want to look. Normally that is an `E_LEAK` error.

There are two cases where a leak is not a bug in the same way. If the run was cancelled, abandoned calls are expected, so they become the warning `E_CANCEL_ABANDONED`. If the trace was cut off, for example by a crash or by `tail -n`, then the missing `run_end` and the open calls are a fact about the capture and not about the agent. Pass `--allow-truncated` and `finish!` downgrades `E_NO_END` and the leaks to warnings. A broken final line becomes `E_TRUNCATED` instead of `E_PARSE`. Without the flag a cut off trace is an error, which is the safe default for CI.

### Reporting

`check-port` returns a `result` struct with the kept violations sorted by line, counts per code, error and warning totals, and the number of runs, events and lines. `max-violations` bounds how many violation records are stored so a trace with a million bad lines cannot eat memory. Counting never stops. The number left out is in `suppressed`.

`format-violation` renders one violation as `file:line: severity CODE [run=... seq=...] message`. `result->jsexpr` produces the JSON form. `exit-code-for` returns 1 when any file has an error, or a warning when `--strict` is set, and 0 otherwise.

## Usage

You need Racket 8 or newer. No packages beyond the standard distribution.

```
racket AgentTraceProtocolChecker.rkt trace.jsonl
racket AgentTraceProtocolChecker.rkt --json --strict run-a.jsonl run-b.jsonl
cat trace.jsonl | racket AgentTraceProtocolChecker.rkt -
racket AgentTraceProtocolChecker.rkt --allow-truncated --max-inflight 4 --max-calls 200 live.jsonl
```

Options, all optional:

| Flag | Default | Meaning |
|---|---|---|
| `--max-inflight N` | 8 | Concurrent calls per run |
| `--max-latency-ms N` | 120000 | Warn when a call is slower |
| `--max-calls N` | 500 | Calls per run |
| `--repeat-limit N` | 4 | Identical consecutive calls |
| `--skew-ms N` | 50 | Timestamp regression tolerated |
| `--max-violations N` | 1000 | Violations kept in the report |
| `--allow-truncated` | off | Cut off trace is a warning |
| `--json` | off | One JSON document on stdout |
| `--strict` | off | Warnings fail the exit code |

Exit codes: 0 clean, 1 violations found, 2 bad flag or unreadable file.

Example input where one call never returns:

```
{"run":"r1","seq":0,"ts":0,"type":"run_start"}
{"run":"r1","seq":1,"ts":5,"type":"tool_call","id":"c1","tool":"search","args_hash":"h1"}
{"run":"r1","seq":2,"ts":9,"type":"tool_call","id":"c2","tool":"read","args_hash":"h2"}
{"run":"r1","seq":3,"ts":12,"type":"tool_result","id":"c1"}
{"run":"r1","seq":4,"ts":20,"type":"run_end"}
```

Output:

```
trace.jsonl:3: error E_LEAK [run=r1 seq=2] call c2 to read (seq 2) never received a result
trace.jsonl: 5 line(s), 5 event(s), 1 run(s), 1 error(s), 0 warning(s)
```

From Racket code you can call the same checker:

```racket
(require "AgentTraceProtocolChecker.rkt")
(define r (check-file "trace.jsonl" default-config))
(displayln (result-errors r))
(for-each (lambda (v) (displayln (format-violation v))) (result-violations r))
```

`check-string` and `check-port` take the same config. Build a custom config with `struct-copy`, for example `(struct-copy config default-config [max-inflight 2])`.

Run the tests with `raco test AgentTraceProtocolChecker.rkt`. There are 16 cases covering clean traces, leaks, cancel handling, orphan and double results, id reuse, sequence problems, interleaved runs, concurrency peaks, repeat streaks, truncation, malformed lines, the violation cap and the JSON shape.

## Notes

- Timestamps are milliseconds. If your logger writes seconds, multiply first or the latency warning will never fire.
- `seq` is per run and starts wherever the first event starts. The checker does not force it to begin at 0, so a trace sliced out of the middle of a run still checks, although you will get `E_START` because `run_start` is missing. That is on purpose.
- The checker trusts `run` and `id` as given. If two processes generate colliding run ids, their events will merge into one run and you will see sequence errors. That is a real bug in the emitter and the report is telling you so.
- A call whose id was reused is not tracked twice. This keeps the leak report honest, but it means the first result may close the wrong call. Fix the id generator before trusting anything else in that trace.
- `E_REPEAT` only works when you supply `args_hash`. Hash the canonical JSON of the arguments. The checker does not compute it because it cannot know your canonicalisation.
- Message content, tool arguments and results are never read, stored or printed, so it is safe to run on traces that contain private data. Only ids, tool names and run ids appear in the output.
- Files are checked independently. Run ids may repeat across files without any interaction.
- It is a linter for protocol shape. It does not prove a trace is complete. A run that lost its final events but still ends cleanly will pass.
