# Inference Spend Circuit

A single agent run can fire forty model calls in ten seconds and burn a month of token budget before any dashboard refreshes. This is a small C program that reads request estimates as JSON lines and answers allow or deny per tenant, before the expensive call goes out.

**Language:** C | **Lines:** 523 | **Added:** 2026-07-03

## What this solves

The failure mode is not a traffic spike. It is a spend spike that looks like normal traffic. A requests per second limiter sees five requests and shrugs. Those five carried 180k input tokens each because someone stuffed a repository into context, and the bill for that quiet minute beats the previous week. Request counting and token counting are different problems, and most gateways only do the first.

The shapes are familiar: an eval harness stuck in a retry loop over the weekend, a free tier tenant who found your agent endpoint, a prompt chain that recurses because a tool call keeps failing the same way. Nobody notices, because latency is fine, error rates are fine and the pods are healthy. The signal arrives days later in the invoice, or as a hard 429 from the upstream provider that takes down every tenant at once because one of them ate the shared quota. The noisy neighbour does not get throttled, everyone else does.

The other half is where the check belongs. Post hoc billing analysis tells you what happened. It cannot refuse the call. A budget guard has to sit in the request path and decide in microseconds. If it needs a Redis round trip you have added latency and a failure domain to every inference call, and when Redis is slow you either fail open and lose the guard or fail closed and take an outage. This does the arithmetic locally: a rolling per tenant window in fixed memory, compared against the incoming estimate, out comes a decision with a reason and a retry hint.

## Why I built it

Rate limiter libraries count events. Token buckets, leaky buckets, GCRA, all assume every request costs the same. That was fine for CRUD APIs and is wrong for inference, where two calls to the same endpoint can differ in cost by three orders of magnitude. Cloud budget alerts do understand spend, but they are hourly or daily, and they alert, they do not gate.

The gap in the middle is a per tenant rolling spend window that a sidecar, an Envoy ext_authz service, a Lambda wrapper or a queue worker can consult synchronously. Portable C with no dependencies runs anywhere the request already runs, including places where adding a Redis client is not on the table.

## When to use it

- A multi tenant LLM gateway where one customer must not exhaust the shared provider quota
- An agent runtime where one user action fans out into many model calls and the fan out needs a ceiling
- A CI or eval pipeline that runs overnight and has previously kept spending after a test hung
- A batch inference queue that admits work up to a spend rate and defers the rest with a real retry delay
- Sizing budget limits from live traffic in shadow mode before turning enforcement on
- Any request path already emitting structured access logs that wants a guard with no database behind it

## How it works

Input is newline delimited JSON on stdin, one request estimate per line. Output is one JSON decision per line on stdout. The JSON handling is hand rolled and deliberately small: `parse_json_string` walks a quoted string with escape handling, `find_value` scans for a key followed by a colon, and `json_string`, `json_u64` and `json_int` sit on top. Numbers are accepted bare or quoted, because plenty of log pipelines stringify everything. A `\u` escape collapses to `?` instead of real UTF-16 decoding. This is a scanner, not a validating parser, and it takes the first matching key on the line.

`parse_event` fills an `Event` using fallbacks that match real log schemas. Tenant comes from `tenant` or `workspace_id`. The output estimate comes from `output_tokens_estimate`, then `max_output_tokens`, then `output_tokens`. Cost comes from `cost_micros` or `estimated_cost_micros`, and failing both, `estimated_cost` derives it from the price flags via `mul_div_up_sat`, a ceiling division that saturates instead of overflowing. Every addition in the hot path goes through `add_sat`, so nothing wraps. Per line rejections are `empty_tenant`, `token_overflow`, `empty_spend_estimate` and `line_too_long`, the last draining the rest of an oversized line from stdin rather than splitting it into a phantom second record.

Tenant state lives in a static array `g_tenants`. `tenant_get` hashes the name with FNV-1a and probes linearly from `hash % slots`. Open addressing suits a table that is fixed size, allocated once and never rehashed, so there is no allocator in the request path. When the table is full the probe loop falls through to LRU eviction: scan every slot for the smallest `last_seen_ms` and take it. Eviction resets that tenant's window to zero, so an oversubscribed table forgets budgets. Size `--tenant-slots` above your real tenant count.

The window is a ring of `Bucket` structs, `--buckets` of them, default 120. Bucket width is `ceil(window_ms / bucket_count)`, so 60 seconds across 120 buckets gives 500 ms granularity. Each event maps to `epoch = ts_ms / bucket_ms` and writes into `buckets[epoch % bucket_count]`. `live_bucket` treats a slot as current only if its stored epoch is within `bucket_count` of the present one, which is how the ring self expires with no background timer. `sweep` zeroes dead slots, `sum_window` adds the live ones. Being a sliding window counter, it avoids the fixed window boundary burst where a tenant spends the full budget at 59.9s and again at 60.1s.

`decide` checks in order. A single request bigger than the whole window budget gets its own reason, `single_request_exceeds_token_window` or `single_request_exceeds_cost_window`, because it will never fit however long the caller waits. Then window totals plus this request are compared against `--max-tokens` and `--max-cost-micros`, either disabled by setting it to 0. `--bypass-priority N` passes priority N and above, and its default of 10 disables the bypass. On a denial, `retry_after` computes a real delay: it walks every live bucket, subtracts that bucket's contribution from the window totals, and checks whether the request would fit once that bucket ages out. The earliest expiry that creates enough headroom wins, capped at the window length. That is a genuine wait this long and try again, which is what a backoff loop needs.

`record` charges the bucket only when the request is allowed and bumps an accepted or denied counter either way. Watch the interaction with `--shadow`: it forces `allow` true while leaving `would_allow` real, so shadow traffic is charged. That is deliberate, it keeps shadow window totals identical to what enforcement would have produced.

## Usage

```sh
cc -O2 -o InferenceSpendCircuit InferenceSpendCircuit.c

# defaults: 60s window, 120 buckets, 200k tokens and 5.00 USD equivalent per tenant
./InferenceSpendCircuit < events.ndjson

# shadow rollout, wider window, cost limit only
./InferenceSpendCircuit \
  --window-sec 300 \
  --buckets 240 \
  --max-tokens 0 \
  --max-cost-micros 25000000 \
  --tenant-slots 2048 \
  --shadow < events.ndjson

# let priority 8 and above through regardless of budget
./InferenceSpendCircuit --bypass-priority 8 < events.ndjson

./InferenceSpendCircuit --help
```

Input line:

```json
{"ts_ms":1751520000000,"tenant":"acme","model":"claude-sonnet","input_tokens":18000,"output_tokens_estimate":2000,"priority":3,"request_id":"req-91f"}
```

Output line:

```json
{"line":1,"allow":false,"would_allow":false,"reason":"tenant_cost_window_exhausted","tenant":"acme","model":"claude-sonnet","request_id":"req-91f","tokens":20000,"cost_micros":3900,"window_tokens_before":199000,"window_cost_micros_before":4998000,"retry_after_ms":1500}
```

`retry_after_ms` appears only on a denial. `cost_micros` is either the value you supplied or the estimate derived from the price flags.

## Notes

- Single process, single threaded, no persistence. State dies with the process, and N gateway nodes means N independent windows unless one instance sits behind a shared admission service.
- The tenant table is sized at compile time, `ISC_MAX_TENANTS` 4096 by `ISC_MAX_BUCKETS` 240, roughly 40 MB of BSS regardless of `--tenant-slots`. That flag bounds the probe range, it does not shrink memory.
- Absent `ts_ms` falls back to `time(NULL) * 1000`, so one second resolution. The ring tolerates mild reordering, but an event far in the past lands in a bucket `live_bucket` treats as dead.
- It never calls a provider, never reads real usage and never reconciles against an invoice. A wrong output token estimate makes the accounting wrong in the same direction.
- The per bucket `accepted` and `denied` counters are maintained and summed but never printed. They exist for a stats surface that is not wired up.
- Exit codes: 0 on clean EOF, 1 on a stdin read error, 2 on a bad or unknown argument. A malformed line is not fatal, it becomes a denial with a reason.
- The scanner matches the first occurrence of a key anywhere on the line, nested objects included. Flatten your log lines or make sure those keys appear once.
