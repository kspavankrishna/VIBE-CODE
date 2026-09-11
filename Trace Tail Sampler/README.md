# Trace Tail Sampler

Head sampling at 5 percent throws away the exact traces you need at 3am: the timeouts, the 502s, the one request that took nine seconds. This is a single file Haskell CLI that reads CSV spans on stdin and makes a deterministic keep or drop decision per span, biased hard toward errors and latency tails.

**Language:** Haskell | **Lines:** 123 | **Added:** 2026-05-24

## What this solves

Telemetry volume grows faster than the budget for it. AI gateways, edge functions, streaming APIs and microservice meshes emit far more spans than any team wants to pay to store, so somebody turns on a 1 percent or 5 percent sampler at the collector and the bill drops. Two weeks later a customer reports intermittent 504s on one route and the trace is gone. Not slow to find. Gone. The sampler discarded it because a uniform random sampler has no idea that a 9 second span matters more than a 40ms one.

That is the failure mode this file attacks. Random head sampling is uniform over traffic, but the value of a span is wildly non uniform. The 5 percent you keep is 5 percent of your healthy traffic and roughly 5 percent of your incidents, which means during an incident that produced eighty failing requests you have four of them, and none of the four are the pathological one. The engineer on call notices. They notice by spending forty minutes reconstructing a request path from application logs because the trace that would have answered it in thirty seconds was never written.

The second failure mode is instability. A sampler that uses a random number generator gives a different answer every time it runs. Replay the same span file through it twice and you get two different sets of kept traces, so you cannot reason about what your sampling config will actually do before shipping it, and you cannot compare two configs on the same data. Any tuning becomes guesswork against a moving target.

This tool fixes both. Keep or drop is a pure function of the span content, so the same input always produces the same output, on any machine, in any order. And the keep probability is not flat: it rises with duration and jumps on any non success status, so slow and failing spans are far more likely to survive than boring ones.

## Why I built it

Collector side tail sampling exists in OpenTelemetry and in vendor agents, but it lives inside a running pipeline. You cannot easily point it at a CSV export of yesterday's spans and ask what would this policy have kept. Tuning a sampling policy usually means changing a config, redeploying the collector, waiting a day and reading a bill. That loop is too slow, and it is destructive: the traces you dropped while testing a bad policy are not coming back.

I wanted something that runs offline against an exported span file, takes three numbers on the command line and prints a per span decision with a reason attached. No agent, no daemon, no config file, no dependency outside base. You can pipe a day of spans through it, count what survives, change the latency threshold and run it again on exactly the same data.

## When to use it

- Sizing a sampling policy before you push it to a production collector, using a CSV export of real span data
- Explaining to finance or to a platform team why the keep rate is what it is, per span, with a stated reason
- Comparing two threshold settings on identical input where a random sampler would give you noise instead of a comparison
- Pre filtering a bulk span export down to the interesting subset before loading it into a query tool
- Building a cheap keep or drop stage in a shell pipeline where installing a vendor agent is not on the table
- Regression testing a sampling config in CI, where the determinism means the expected output file does not churn

## How it works

Input is CSV on stdin with five fields: `trace_id,service,route,duration_ms,status`. `process` splits stdin with `lines`, drops blank lines and drops any line starting with `trace_id,` so a header row is tolerated, then runs `parseSpan` over the rest. `parseSpan` splits on commas with a hand rolled `splitComma` fold, trims each field, lowercases the status and builds a `Span`. A line with the wrong field count returns a `Left` naming the line number.

The scoring lives in two small functions. `pressure` computes `min 1.0 (duration / latencyMs)` and adds `errorBoost` when the status is neither `ok` nor `success`. So a span at exactly the latency threshold contributes 1.0 on its own, and anything past it is clamped. `sampleDecision` then forms the keep probability as `min 1.0 (targetRate + pressure opts s)`. With the defaults, a fast healthy span sits at 0.05, a failing fast span at 0.45, and anything at or beyond 2500ms is at 1.0 and always kept.

The randomness is not random. `fnv64` is FNV-1a, the 64 bit variant: seed 14695981039346656037, XOR each byte then multiply by the prime 1099511628211. `unitHash` takes that hash mod 1000000 and divides by a million to land in [0,1). The span is kept when the hash is at or below the probability. FNV-1a is the right pick here because it is a few lines, has no dependencies, is fast on short strings and spreads well enough that the low order bits behave like a uniform variate. The important property is that it is a hash, not a generator: the decision is a pure function of the key, so it is stable across runs, across processes and across machines.

The hash key is `traceId ++ service ++ route`. `reason` is derived after the fact for human consumption: error boosted, latency tail, deterministic baseline or below sampling threshold. Output is either tab separated via `renderText` with a header row, or a single JSON object via `renderJson` under `--json`. Numbers print through `showFFloat2`, which despite the name emits four decimal places. `main` writes the decisions to stdout and a `kept=N total=M` summary line to stderr, so you can count survivors without parsing the payload.

## Usage

```bash
# compile
ghc -O2 TraceTailSampler.hs -o trace-tail-sampler

# defaults: 5% baseline, 2500ms latency threshold, 0.40 error boost
cat spans.csv | ./trace-tail-sampler

# tighter budget, aggressive on the tail
cat spans.csv | ./trace-tail-sampler --target-rate 0.01 --latency-ms 800 --error-boost 0.60

# JSON out, count the survivors from the stderr summary
cat spans.csv | ./trace-tail-sampler --json > decisions.json

# input format (header row optional)
# trace_id,service,route,duration_ms,status
# 4f9a...,checkout,POST /orders,3120,error
```

Or run it straight with `runghc TraceTailSampler.hs < spans.csv`.

## Notes

- The hash key includes service and route, not just trace_id. Two spans of the same trace on different services get independent baseline decisions. Full trace correlation holds only where service and route match.
- Error detection is a string comparison. Any status that is not `ok` or `success` after lowercasing counts as an error, including an empty field.
- `readDouble` calls `error` on a malformed number, so a bad `duration_ms` or a bad flag value aborts with a Haskell exception rather than the clean exit 64 that a malformed row gets.
- A flag given without its value, for example `--latency-ms` at the end of the arguments, is reported as an unknown option.
- The JSON output carries keep, score, trace_id, service, route and reason. Duration and status appear only in the tab separated output.
- Exit codes: 0 on success, 64 for a bad flag or a malformed CSV row. Parsing stops at the first bad row.
- Imports are base only: Data.Bits, Data.Char, Data.List, Data.Word, Numeric and the System modules. No cabal file, no package set.
- It reads all of stdin before emitting anything, so memory scales with input size. This is a batch tool, not a streaming collector stage.
