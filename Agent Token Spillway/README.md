# Agent Token Spillway

An agent fleet, a RAG tier, an eval runner and a nightly batch job all hit the same model provider quota in the same minute, and your gateway finds out it is over the limit only after it has opened SSE streams and spent prompt cache. This is a single file Zig admission controller that answers accept, defer or reject for every pending LLM request before the expensive call goes out.

**Language:** Zig | **Lines:** 585 | **Added:** 2026-06-15

## What this solves

This solves the April 2026 problem of AI gateway token budget admission control, when agent systems, MCP tool calls, RAG searches, eval runners and streaming LLM requests all compete for one provider quota at once. Teams trust the provider rate limit headers too late, after they have already accepted work, opened streams, spent prompt cache budget and made users wait behind requests that never had a chance to finish inside a deadline or a cost limit.

The failure mode is specific. Your gateway accepts 300 queued requests because none of them individually looks large. Forty are 90k token agent turns with twelve tool calls each. The provider starts returning 429 at request 61, but by then you have committed sixty streams, paid for input tokens on the ones that got partway through and pushed every interactive user behind a batch eval nobody was waiting on. Retries stampede, cache hit rate collapses because the cache entries aged out during the backoff, and the hourly bill is three times the plan. The users see a spinner. Finance sees one tenant burn a month of budget in an afternoon.

The second failure mode is quieter and more expensive. A request runs to completion and only then breaches a tenant budget or a cost cap someone set six weeks ago, so you paid for tokens you cannot bill. Or the prompt plus tool overhead plus reserved output never fit the context window and the provider rejects it after you paid to send it. Or a request with a 4 second deadline sits at queue position 90 behind 32 way concurrency, so you pay full price for a response that arrives too late to read.

This refuses that work up front. It reads a CSV of pending requests on stdin, runs one deterministic pass and emits a decision per request plus backpressure headers your proxy or queue can enforce directly. No service, no database, no network call.

## Why I built it

Every rate limiter I could reach for counts one thing. Token buckets count requests. Cost trackers count dollars after the fact. Nothing weighs tokens, requests, tenant budget, cost cap, context window, residency and deadline pressure in one decision, and tokens are the part that matters, because an LLM request is a variable sized claim on a shared pool, not a unit of work. A 200 token classifier call and a 90k token agent turn cost the same one request against a request limiter. That is how a fleet takes down its own interactive traffic.

The other gap is placement. Admission control belongs in front of the call and has to be cheap enough to run inline, so this is one file, one pass, no allocation beyond an arena. It plans and prints, so you can diff two plans and replay a bad hour to see why each request was refused.

## When to use it

- You run a small internal inference gateway on one provider account and several teams share the quota.
- Your CI runs agent tasks that can flood the same provider your customer facing product uses.
- A research eval queue should soak leftover capacity but must never starve interactive traffic.
- A tenant has a hard monthly spend cap and requests must be refused before the spend, not reconciled after.
- Data residency is a requirement and a request from the wrong region must never reach the provider.
- You want to replay yesterday's queue offline and find out which requests were doomed on arrival.

## How it works

Input is CSV on stdin with exactly 12 columns: `tenant, request_id, idempotency_key, region, prompt_tokens, cache_hit_tokens, max_output_tokens, deadline_ms, priority, tenant_budget_micros, cost_cap_micros, tool_calls`. `isHeader` skips a line starting with `tenant,`. A hand written `splitCsv` handles quoted fields, commas inside quotes and doubled quote escapes, returning `error.UnclosedQuote` rather than guessing, and `parseWorkItem` demands all 12 fields. Money is micros, time is milliseconds, everything is `u64`, so no float rounding touches the cost path.

`estimate` runs first over every `WorkItem`. Cache hit tokens are clamped to prompt tokens, the remainder is uncached prompt, and tool overhead is `tool_calls * tool_call_tokens` at 900 tokens per call by default. Two totals fall out and the distinction is the point. `billable_input_tokens` is uncached prompt plus tool overhead, because a cache hit saves money. `reserved_total_tokens` is the full prompt plus tool overhead plus reserved output, because a cache hit saves nothing in the context window or against a token rate limit. Reserved output is `max_output_tokens` scaled by `output_safety_ppm`, default 1_100_000, so you hold 110 percent of the declared ceiling. `ceilMulDiv` and `addSaturated` widen to `u128` and saturate at `maxInt(u64)` instead of overflowing.

Ordering is `std.sort.pdq` with the `moreUrgent` comparator: higher priority first, then earlier deadline with `deadline_ms == 0` treated as infinity so unbounded work sinks, then cheaper first, then tenant and request id lexicographically. Those last two tiebreakers exist because pdq sort is unstable, and a total order is what makes the plan reproducible. Same input, same plan, every run.

`runPlan` walks the sorted list once and applies nine gates in a fixed order, splitting outcomes into terminal rejects and retryable defers. Rejects will not improve with time: `residency_mismatch` against `--required-region`, `context_window` when reserved total exceeds `--max-context`, `request_cost_cap`, `tenant_budget`, and `deadline_miss`. Defers are the four capacity gates, `tenant_token_rate`, `tenant_request_rate`, `provider_token_rate` and `provider_request_rate`, each carrying a `retry_after_ms` from `retryForShortfall`. That is a token bucket with the refill inverted: instead of sleeping and re-checking, it converts the shortfall into wall clock time against the configured per minute refill, falling back to 60_000 ms when refill is zero.

Deadline pressure is modelled by `queueWaitMs`. Accepted requests divide into waves of `--concurrency`, each wave costs `--provider-p95-ms`, and a request is rejected with `deadline_miss` when its projected wait plus one p95 exceeds its deadline. That reads the running `accepted` counter, so the decision for request N depends on everything admitted before it, which is why sort order is load bearing. On accept the request decrements all five counters in one step and records its `queue_position`. Tenant state lives in a `std.StringHashMap(TenantState)` seeded by `seedTenants`.

Output is TSV by default, or `--json` for an `{"admissions":[...]}` document with proper escaping. Both carry the same `writeHeaderString` output: `x-agent-decision`, `x-agent-reason`, `x-agent-retry-after-ms`, `x-agent-cost-micros`, `x-agent-reserved-tokens` and `x-agent-queue-position`. Copy those onto the response and your proxy, queue and client SDK all agree on what happens next.

## Usage

```bash
# build and test
zig build-exe AgentTokenSpillway.zig -O ReleaseSafe
zig test AgentTokenSpillway.zig

./AgentTokenSpillway --help

# input: 12 column CSV on stdin, header optional
cat pending-agent-requests.csv
# tenant,request_id,idempotency_key,region,prompt_tokens,cache_hit_tokens,max_output_tokens,deadline_ms,priority,tenant_budget_micros,cost_cap_micros,tool_calls
# acme,r-1001,idem-1001,us-east-1,8000,2000,1000,4000,200,2000,0,1
# "acme, inc",r-1002,idem-1002,us-east-1,80000,0,20000,4000,10,2000,0,2

# default TSV plan
./AgentTokenSpillway < pending-agent-requests.csv

# JSON plan with residency enforcement and explicit quotas
./AgentTokenSpillway --json \
  --required-region us-east-1 \
  --tenant-token-capacity 120000 --tenant-token-refill-per-min 120000 \
  --tenant-request-capacity 120 --tenant-request-refill-per-min 120 \
  --provider-token-capacity 900000 --provider-token-refill-per-min 900000 \
  --provider-request-capacity 900 --provider-request-refill-per-min 900 \
  --max-context 128000 \
  --output-safety-ppm 1100000 \
  --tool-call-tokens 900 \
  --price-input-micros-per-1k 150 --price-output-micros-per-1k 600 \
  --provider-p95-ms 1800 \
  --concurrency 32 \
  < pending-agent-requests.csv
```

## Notes

- It is a planner, not an enforcer. It prints decisions and headers; your proxy or queue still has to honour them.
- State is per run. Every invocation seeds both buckets at full capacity, so it plans one admission window rather than tracking a live bucket across processes.
- `idempotency_key` is echoed for correlation but never used for deduplication. Two rows with the same key are two claims on capacity.
- Output prints in sorted priority order, not input order. Join on `request_id` to recover it.
- `cost_cap_micros = 0` means no cap and `deadline_ms = 0` means no deadline. Zero is unset, not a strict limit. Tenant budget seeds from the largest `tenant_budget_micros` across that tenant's rows, so disagreeing rows resolve upward rather than failing loudly.
- The stdin line buffer is 16 KB, so a longer CSV row fails the read rather than truncating. Unknown flags are a hard error; `--help` exits clean, anything else propagates a Zig error and exits non zero.
- The p95 queue model is a wave calculation, not a simulator. It assumes uniform latency and full concurrency, which is pessimistic for short requests.
- Three tests ship in the file: budget rejection, provider token deferral with a non zero retry, and the CSV quoting edge cases.
