# OTel Agent Critical Path

Your AI agent run took twelve seconds and the trace has forty spans, so where did the time actually go? This is a single file D program that reads OpenTelemetry trace exports and tells you which spans sit on the critical path of each run, which ones only look slow because they ran in parallel, and how much you would really save by speeding each one up.

**Language:** D | **Lines:** 1215 | **Added:** 2026-10-15

## What this solves

Agent traces are hard to read for a reason that normal request traces are not. A web request is mostly a straight line: a handler calls a database and returns. An agent run fans out. One step calls an LLM, the model asks for two tools at once, one tool is an MCP server that calls an HTTP API, the next LLM call waits for the slower of the two tools, and a background span flushes telemetry after the root has already ended. When you sort spans by duration you get a list that is wrong in a specific way: the longest span is not the one holding the run up. A tool call that takes six seconds but runs next to a nine second sibling contributes zero seconds to the wall clock. Cut it to one second and the run does not get faster at all.

The question that matters is the critical path: the chain of spans that actually decides when the run finishes. Trace UIs show a waterfall and leave you to eyeball it. Some show a "critical path" highlight but only inside one trace, one click at a time, and none of that helps when you want to know across ten thousand runs whether the model, the tools or the network is your real latency cost. That is the gap here. OTel Agent Critical Path reads the raw OTLP JSON that the collector file exporter writes, works out the critical path for every trace, and adds it up by span type and by label so you get a ranked list for the whole workload.

It also deals with the ways agent traces are messy in real life:

- Child spans that start before their parent or end after it, because of clock skew between machines or async work that outlives the caller.
- Spans whose parent never arrived, because of tail sampling or a dropped batch.
- Duplicate span ids from exporter retries.
- Parent cycles from bad instrumentation.
- Huge fan out and very deep nesting.
- Input that is a mix of JSONL, pretty printed files and noise.

None of these crash it. Each one is counted and printed as a warning, so you know how much to trust the numbers.

## Why I built it

I kept running into the same argument on agent projects. Someone says the model is slow, someone else says the tools are slow, and both have a screenshot of a trace to prove it. Each screenshot is one run and each run is different. I wanted a number I could put in a pull request: across the last day of traces, this much of the wall clock was spent waiting on this tool, and this much of that could be saved before a parallel sibling would take over.

I also wanted something I could drop on a CI box or a laptop with no database and no running backend. The OpenTelemetry collector can write spans to a file, so a single binary that reads that file is enough. I picked D because it compiles to a small native binary, has fast built in associative arrays and ranges, and lets me write the interval logic plainly without fighting the language. It also has `unittest` blocks inside the source file, so the tricky parts, like clamping and cycle handling, are tested in the same place they live.

## When to use it

- You run LLM agents or MCP based tools and want to know whether to spend your effort on model latency, a slow tool or a slow upstream API.
- You have parallel tool calls and want to know which one is really blocking the next step.
- You want a latency gate in CI. `--max-ms` fails the build when any trace in a test run is slower than your limit.
- You are comparing two versions of an agent. Run it on both exports with `--json` and diff the label table.
- You have partial traces and want to know how partial before trusting a dashboard.

It is not a tracing backend. It does not sample, store or query. It reads files and prints a report.

## How it works

Input goes through `DocSplitter`, a small byte level scanner that finds top level JSON objects and arrays while tracking string and escape state. That means braces inside strings never split a document early, and JSONL, a pretty printed export and concatenated exports all work. A document bigger than `--max-doc-mb` is skipped and counted instead of eating memory. Each document goes to `Store.addDoc`, which walks `resourceSpans`, then `scopeSpans` (or the older `instrumentationLibrarySpans`) and calls `parseSpan` for each span.

`parseSpan` reads the hex trace id, span id and parent id through `normalizeId`, which treats an all zero id as absent. Timestamps go through `toNano`, which accepts the string form that OTLP JSON uses for 64 bit values as well as plain numbers. A span with a bad id, a bad timestamp or an end before its start is dropped and counted in `invalidSpans`. Only a short whitelist of attributes is kept: `gen_ai.operation.name`, `gen_ai.tool.name`, `gen_ai.request.model`, `gen_ai.agent.name`, `mcp.method.name`, `db.system`, `http.request.method` and `rpc.system`, plus `service.name` from the resource. Everything else is discarded so memory stays proportional to span count, not attribute count. `--max-spans` caps the total and the report says so if it was reached.

`analyzeTrace` does the real work for one trace:

1. Spans are deduplicated by span id, keeping the first copy.
2. The primary root is chosen. A span with no parent wins. If there are several, the longest one wins. If there are none, the longest orphan wins. With `--adopt-orphans` every span whose parent is missing is attached to that root.
3. A traversal from the root assigns each span an effective window. A child is clamped into its parent window, and each clamp is counted in `skewedSpans`. Anything not reached from the root is sorted into an orphan subtree or a parent cycle, and both are reported instead of looping.
4. Exclusive time (self time) is the span duration minus the union of its children's windows. This is the real work done by the span itself.
5. `TraceCtx.walk` computes the critical path. For a span it moves a cursor backward from the end of its window. At each step it picks the child that finishes last before the cursor, charges any gap between that child's end and the cursor to the parent, recurses into the child, then moves the cursor to the child's start. Every nanosecond of the root is charged to exactly one span. After each trace, the sum of the charged time is checked against the root duration and a mismatch is counted in `accountingErrors`. It should always be zero.
6. While picking a child, `walk` looks ahead for the next sibling that would take over. The gap between the two end times is stored as headroom. A label's gain is the smaller of its critical time and its headroom, so a span that is critical by one millisecond does not claim a six second saving.

`classify` turns each span into a category (agent, llm, tool, mcp, db, http, rpc or other) and a label such as `llm:<model>` or `tool:<name>`. Labels are cut to `MaxLabelLength` characters and capped at `MaxDistinctLabels` distinct values. Beyond that they fold into `(other labels)`, so a trace with a unique id in every span name cannot blow up memory.

The recursion depth is limited by `MaxWalkDepth`. A span nested deeper than that is charged as a leaf and counted, so a hostile or broken trace cannot overflow the stack. The traversal itself is iterative.

Exit codes: 0 for success, 1 for a usage or read error, 2 when `--max-ms` is exceeded and 3 when `--fail-on-warnings` is set and the input had any warning.

## Usage

Build with the LDC compiler or DMD:

```
ldc2 -O2 OTelAgentCriticalPath.d
```

Run the built in tests:

```
ldc2 -unittest -run OTelAgentCriticalPath.d
```

Analyse a file, several files or standard input:

```
./OTelAgentCriticalPath SampleAgentTrace.jsonl
./OTelAgentCriticalPath day1.jsonl day2.jsonl --top 25 --traces 10
cat export.jsonl | ./OTelAgentCriticalPath --json
```

Options that exist in the code:

- `--json` prints a JSON report with the same data as the text report.
- `--top N` sets how many labels are listed. The default is 15.
- `--traces N` sets how many slowest traces are listed. The default is 5.
- `--root TEXT` keeps only traces whose root span name contains TEXT.
- `--min-ms N` ignores traces shorter than N milliseconds.
- `--max-ms N` exits with code 2 if any trace is slower than N milliseconds.
- `--adopt-orphans` attaches spans with a missing parent to the root.
- `--fail-on-warnings` exits with code 3 if the input produced any warning.
- `--max-doc-mb N` and `--max-spans N` set the memory guards.

A typical CI line:

```
./OTelAgentCriticalPath traces.jsonl --root "agent run" --max-ms 20000 --fail-on-warnings
```

The sample file `SampleAgentTrace.jsonl` is one agent run with an LLM call, two parallel tools, an MCP call that ends in a failing HTTP request and one orphaned span. Running it shows the LLM taking about half of the wall clock and the slow HTTP call on the search path taking about a third, even though the other tool also looks long in a waterfall.

## Notes

- The tool only reads OTLP JSON with hex ids, which is what the collector file exporter produces. Base64 ids from other encoders are not decoded.
- Critical path time is exact for the trace you gave it. It does not predict queueing or retries that would change if a span got faster. Treat gain as an upper bound with a sibling as the limit.
- Clamping a child into its parent hides skew rather than fixing it. If the skew warning is large, fix clock sync before trusting small differences.
- Traces with a lost root are measured from the longest orphan, which understates the real run. The warning tells you how many spans were involved.
- I tested it on a generated set of about 555 thousand spans across five thousand traces plus one chain 60 thousand spans deep. It finished in a few seconds and reported no accounting errors.
- Parallelism is exclusive work divided by wall clock. Near 1.0 means the run is mostly serial and the path is the whole story. Well above 1.0 means you have overlap worth looking at.
