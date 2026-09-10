# Token Egress Budget

Your token bill looks fine and your network bill does not. This is a single file C tool that reads gateway logs as CSV, groups them by tenant and route, and fails the build when a route ships too many bytes per token, fails too often or runs too slow.

**Language:** C | **Lines:** 205 | **Added:** 2026-05-24

## What this solves

This solves the April 2026 streaming token egress problem where AI gateways and realtime apps pay for model tokens, network bytes and slow client drains at the same time, but most budgets only watch tokens. Token accounting is the number everyone puts on a dashboard because the model provider hands it to you. Bytes on the wire are the number nobody owns. A route can look cheap in token accounting while it is quietly expensive in Server-Sent Events overhead, verbose JSON framing, retries or mobile network backpressure.

The concrete failure mode: someone changes a streaming endpoint to emit one SSE frame per token instead of batching. Each frame carries an event name, a data prefix, a JSON envelope with a request id and a role field, then two newlines. Token counts do not move at all. Egress per response goes from three bytes per token to twenty. Nobody notices for a month because the LLM cost dashboard is flat. Then the CDN invoice arrives, or the mobile team files a bug about data usage, or a tenant on a metered connection churns. The person who notices is finance, not engineering, and by then the change is thirty deploys deep.

The second failure mode is slower and worse. One tenant hammers a route with prompts that produce near empty completions. Tokens billed are close to zero. Bytes are not, because every failed or empty response still carries full framing, headers and retry traffic. Averaged across all traffic the route looks healthy. Split by tenant and route it is obviously broken. Aggregate dashboards hide the one pair out of four hundred that is the whole problem.

The third is latency drift that never trips a page. Average response time creeps from four seconds to twenty six. Nothing is down so nothing alerts. Connections stay open longer, concurrency climbs and the failure shows up under load as timeouts somewhere else. This tool runs all three checks in one pass over the same log export and returns a nonzero exit code so CI can stop the change.

## Why I built it

Existing cost tooling splits along the wrong seam. Provider dashboards report tokens because tokens are what they sell. CDN dashboards report bytes for the same reason. APM reports latency. None of the three divides by the others, and bytes per token is the ratio that tells you whether a streaming route got worse. Building that view usually means a warehouse job, a scheduled query and a dashboard nobody reads until the invoice.

I wanted the check to live where the change happens, which is CI, and to have no runtime a platform repository does not already have. One C file, standard library only, reads stdin, writes a table or JSON, exits nonzero on violation. It compiles anywhere a compiler exists and runs in a scratch container with no package install step.

## When to use it

- Nightly CI job that exports the last 24 hours of gateway logs and fails when any tenant route pair breaks the egress budget.
- Pre merge gate on a repository that owns SSE or streaming response framing, so a framing change cannot land silently.
- Post incident review where you need per tenant numbers rather than a global average.
- Onboarding a new tenant on a metered or mobile heavy network and wanting a hard ceiling on bytes per token before you commit to a price.
- Comparing two edge runtimes or two proxy configurations by running the same log shape through both and diffing the tables.
- Chargeback and unit economics work where you need bytes and tokens attributed to the same key.

## How it works

Input is CSV on stdin with six columns: `tenant`, `route`, `tokens`, `bytes`, `latency_ms` and `status`. `main` reads lines with `fgets` into an 8192 byte buffer, calls `trim` on each, skips blanks and skips the first row only if it starts with `tenant,`. Everything else goes to `parse_sample`.

`split_csv` is a quote aware splitter. It walks the line once, toggles a `quoted` flag on every double quote and only cuts on commas seen outside quotes, so a route path containing a comma survives. It fills at most eight cells and trims each one. `parse_sample` requires at least six and exits 64 with a row number if the shape is wrong. Numbers go through `parse_double`, which wraps `strtod` and checks both `errno` and the end pointer, so a garbage cell is a hard error rather than a silent zero. The status cell is lowercased in place, and `failed` is set unless the value is exactly `ok` or `success`. That inversion matters: anything unrecognised counts as a failure, which is the safe direction for a budget gate.

Aggregation is a fixed size array of `Rollup` structs with a linear scan lookup in `find_rollup`, keyed by the string `tenant/route`. No hashing, no allocation, no free. The cap is 4096 distinct pairs and exceeding it exits 70 rather than corrupting memory. Linear scan is the right call because real gateway exports have tens to low hundreds of distinct pairs, and a hash table would add code without changing the wall clock. `add_sample` accumulates sample count, failure count, token sum, byte sum, latency sum and a running maximum latency.

`violates` is the whole policy in one function. Bytes per token is `bytes / tokens`, with a deliberate fallback: when a rollup has zero or negative tokens the raw byte total is used as the ratio, so a route that burned bandwidth and produced nothing does not divide by zero and slip through as healthy. Failure rate is failures over samples. Average latency is the unweighted mean per sample. A rollup fails if any of the three exceeds its threshold. Defaults are 9.0 bytes per token, a 0.04 failure rate and 30000 ms average latency, all overridable on the command line.

Output comes from `print_text` or `print_json`. Text mode writes a tab separated table with a leading PASS or FAIL column, sample count, bytes per token, failure rate, average latency and maximum latency. JSON mode writes one object per route plus a top level `status` field, which is what a CI step or a bot posts back to a pull request. After printing, `main` walks the rollups once more and returns 2 if any of them violates. Printing always happens first, so a failing run still gives you the full table to read.

## Usage

```sh
cc -O2 -o token-egress-budget TokenEgressBudget.c

# CSV: tenant,route,tokens,bytes,latency_ms,status
cat gateway.csv | ./token-egress-budget

# tighter budget, JSON for CI
cat gateway.csv | ./token-egress-budget \
  --max-bytes-per-token 6 \
  --max-failure-rate 0.02 \
  --max-latency-ms 12000 \
  --json

echo "exit=$?"   # 0 pass, 2 budget violation
```

Sample input:

```
tenant,route,tokens,bytes,latency_ms,status
acme,/v1/chat/stream,1200,9800,4200,ok
acme,/v1/chat/stream,900,7100,3900,ok
globex,/v1/completions,50,4200,26000,error
```

## Notes

- Exit codes: 0 all routes pass, 2 at least one route violates the budget, 64 bad option or unparseable row, 70 more than 4096 distinct tenant route pairs.
- Maximum latency is reported in the text table but is not part of the pass or fail decision. Only the average is gated. Use it as a signal to go look, not as an alert.
- Lines longer than 8191 bytes are split by `fgets` and the tail is parsed as its own row, which usually fails parsing rather than corrupting a number.
- Tenant names truncate at 95 characters and routes at 159. Two routes that differ only past that point collapse into one rollup.
- The header row is only skipped when it is the first row and begins with `tenant,`. A different column order is not detected, only misread, so export in the documented order.
- The quote aware splitter does not strip surrounding quotes from a cell. Quoted numeric fields will fail `parse_double`. Quote only the text columns if you need to.
- The rollup array lives on the stack at roughly 2.4 MB. Fine on a normal host, worth checking on a thread with a small stack.
- No aggregation across runs, no persistence, no time windowing. It summarises exactly the rows you pipe into it, so you pick the window when you export the CSV.
