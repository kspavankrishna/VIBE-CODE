# Edge Inference Admission

A single file C admission controller that decides whether an LLM request is allowed to start, based on rolling per tenant cost budgets, token budgets, carbon estimates and data residency rules. It runs on stdin, has zero dependencies and never calls the clock.

**Language:** C | **Lines:** 847 | **Added:** 2026-06-10

## What this solves

This solves the April 2026 problem where AI gateways, agent platforms, edge workers and internal developer tools need to stop runaway LLM spend before a streaming request starts, not hours later in a billing dashboard. By the time a dashboard shows the spike the money is gone. Streaming makes it worse: the request is admitted, tokens flow for ninety seconds, and there is no clean point at which anyone decides this one costs too much. The decision happens at admission or it does not happen.

The concrete failure looks like this. One tenant ships an agent loop that retries on every tool error. Each retry carries the full conversation forward, so input tokens grow linearly and cost grows with them. Nobody set `max_output_tokens`. At 03:00 it runs unattended for six hours. Nothing alerts, because per request latency and error rates are normal. The only signal is the invoice, and by then the shared budget is spent and every other tenant on that gateway is degraded.

The second failure is quieter. A request is routed to a region where the tenant's data is not allowed to land. No crash, no error, no log line that stands out. It surfaces in an audit six months later as a compliance question rather than an engineering one. Checking `region` against `required_region` before the call costs nothing.

The usual answer is a Redis backed limiter that counts requests. Counting is close to useless for LLM traffic: a 200 token classification and a 180k token summary are one request each and differ by three orders of magnitude in cost. Budgets have to be denominated in dollars and tokens, not calls per second.

## Why I built it

Existing tooling either sits too far from the hot path or drags in too much. Gateway rate limiters count requests, not dollars. Observability stacks tell you what happened after it happened. Vendor dashboards are read only and lag by hours. The self hosted options assume you will run Redis, Kafka, an OpenTelemetry collector or another SaaS dependency inside the request path of an edge worker, which is exactly where you cannot afford a network hop.

I wanted an admission decision that is a pure function of a text record and the process's own memory. No clock, no sockets, no allocator surprises at steady state. Something you compile into a sidecar, an Envoy external processor, an njs bridge or a load test harness, then replay captured traffic through to see what a policy change would have done before enabling it.

## When to use it

- You run a shared model gateway and one tenant's runaway agent loop can spend the whole platform budget overnight.
- You need `retry_after_ms` in a 429 that reflects when the rolling window will actually have room, not a fixed backoff guess.
- You want to test a new budget policy against yesterday's captured traffic before enforcing it.
- You have a data residency rule and need requests killed at admission when the serving region does not match the required one.
- You price input and output tokens separately and a request counter cannot express your budget.
- You need a carbon ceiling per request enforced rather than reported.

## How it works

The program is a stdin to stdout filter. `main` reads a line with `read_line`, hands it to `parse_record`, then to `process_record`. `parse_record` is a hand written scanner over a small key=value grammar: keys are lowercased and restricted by `is_key_char` to alphanumerics plus `_ - .`, values are bare tokens or double quoted with `\n \r \t \\ \"` escapes, `#` begins a comment and duplicate keys are a hard error. Limits are compile time constants (`FIELD_KEY_MAX` 64, `FIELD_VALUE_MAX` 512, `RECORD_FIELD_MAX` 96) and records are fixed size structs, so parsing allocates nothing but the line buffer.

`build_estimate` turns a `Record` into an `Estimate`, accepting alias sets rather than one canonical name per field: `input_tokens`, `prompt_tokens` and `tokens_in` all work, as do `max_output_tokens`, `output_tokens_est`, `completion_tokens` and `tokens_out`. Cost resolution has a precedence: explicit `cost_usd` wins, then a flat `usd_per_mtok` on total tokens, then input tokens at the input price plus output tokens at the output price, over a million. Missing totals come from the parts, missing output tokens from the total. Carbon is either explicit `carbon_gco2e` or `gco2e_per_1k_tokens` times total tokens over a thousand.

State lives in `TenantTable`, an open addressing hash table with linear probing keyed by FNV-1a (`hash_string`), rehashing at a 0.7 load factor. `tenant_table_get` creates a tenant on first sight rather than requiring registration. Each `TenantState` owns a ring buffer of `Bucket` entries, one per second, sized to `--window-sec`. `bucket_for` indexes by `ts % bucket_count` and zeroes the slot when the stored `ts` does not match, which is how the ring self expires with no sweep. `window_totals` sums only buckets whose `ts` falls in `[now - bucket_count + 1, now]`, so this is an exact rolling window rather than a decaying approximation. A dollar budget that quietly drifts is worse than no budget. A separate `global_state` runs the same machinery for the platform ceiling.

The decision lives in `process_record`. Priority buys headroom through `priority_borrow`, computed as `1.0 + priority * 0.015` and multiplied into `cfg->burst` to give `effective_burst`, which scales all four limits. Priority 10 gets 15 percent more room than priority 0, deliberately modest so an interactive request beats a batch job without anyone setting priority high to opt out. Checks short circuit in a fixed order: carbon budget, region mismatch, tenant USD, tenant tokens, global USD, global tokens. `is_limited` treats any limit near `DBL_MAX` as unset, which is how an absent carbon ceiling is represented without a separate flag.

When a request is denied on a budget, `retry_after_ms` computes an honest wait: it walks buckets forward from the oldest in the window, subtracting each one's contribution and returning the moment enough of the window will have aged out for this request to fit. Real recovery time, not a guess. `--shadow` flips a denial to an allow, sets `shadow=true` and books the usage as if it ran, so a shadow run reproduces real budget pressure. Output goes through `print_value`, which percent encodes anything outside `[A-Za-z0-9_.:/@-]` so a hostile tenant name cannot forge fields in the log line.

## Usage

```sh
cc -std=c17 -O2 -o EdgeInferenceAdmission EdgeInferenceAdmission.c

# gate a stream of requests against per tenant and global budgets
./EdgeInferenceAdmission \
  --window-sec 60 \
  --tenant-usd 0.25 \
  --global-usd 10.0 \
  --tenant-tokens 250000 \
  --global-tokens 5000000 \
  --burst 1.10 \
  --default-input-usd-per-mtok 0.15 \
  --default-output-usd-per-mtok 0.60 \
  --max-carbon-g 5.0 \
  --require-region-match \
  < requests.kv

# dry run a tighter policy against captured traffic without enforcing it
./EdgeInferenceAdmission --shadow --tenant-usd 0.05 < yesterday.kv

# treat malformed lines as allows instead of stopping
./EdgeInferenceAdmission --fail-open < requests.kv

./EdgeInferenceAdmission --help
```

Input records, one per line:

```
ts=1749513600 event=request id=r-1 tenant=acme input_tokens=1800 max_output_tokens=900 priority=7
ts=1749513600 event=request id=r-2 tenant=acme input_tokens=64000 max_output_tokens=4000 usd_out_per_mtok=15.0
ts=1749513601 event=request id=r-3 tenant=beta cost_usd=0.40 region=eu-west-1 required_region=eu-central-1
ts=1749513601 event=observe id=r-1 tenant=acme total_tokens=2410 cost_usd=0.00081
# comments and blank lines are skipped
```

Output, also key=value, one line per record:

```
ts=1749513600 event=decision id=r-1 tenant=acme decision=allow reason=ok shadow=false priority=7 ...
ts=1749513600 event=decision id=r-2 tenant=acme decision=throttle reason=tenant_usd_budget ... retry_after_ms=59000 ...
ts=1749513601 event=decision id=r-3 tenant=beta decision=throttle reason=region_mismatch ... retry_after_ms=0 ...
ts=1749513601 event=observed id=r-1 tenant=acme cost_usd=0.00081000 tokens=2410 ...
```

`event=request` is gated. `event=observe`, `usage` or `settle` books known usage after the fact, which is how you reconcile an estimate against the real bill.

## Notes

- Time comes from the record's `ts`, `time` or `timestamp` field and falls back to the line number. The program never calls the clock, so the same input always produces the same output. Replay is exact, and the caller must supply sane monotonic timestamps.
- The window is `--window-sec` buckets of one second each, so `ts` is assumed to be whole seconds. Sub second timestamps collapse into one bucket, and a timestamp far outside the window resets whichever ring slot it lands on.
- State is per process and in memory only. Run several replicas and each enforces its own budget, so the real ceiling is the configured limit times the replica count.
- Tenants are never evicted and the table only grows. Fine for a bounded tenant list, not for unbounded user IDs used as the tenant key.
- Exit codes: 0 for a clean run, 2 for a malformed line without `--fail-open`, a bad flag or an allocation failure. A parse error without `--fail-open` stops processing at that line; with it, the line prints as `reason=parse_error decision=allow` and the run continues.
- A rejected counter is accumulated per bucket but never printed. Throttle counts have to be derived by counting `decision=throttle` lines downstream.
- Denied requests are not queued, deferred or routed anywhere. This decides admit or throttle and prints the answer. Retry, backoff and fallback routing belong to the caller.
