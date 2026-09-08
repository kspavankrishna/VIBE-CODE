# AI Gateway Quota

One gateway sits in front of OpenAI, Anthropic, Gemini, Groq, DeepSeek and OpenRouter, and every one of them reports rate limits in a different header with a different unit. This is a single Lua module that meters requests, input tokens, output tokens, spend and concurrency per tenant, reserves budget before the upstream call and settles real usage after it.

**Language:** Lua | **Lines:** 1011 | **Added:** 2026-04-16

## What this solves

The first failure mode is a retry storm that costs money. A tenant fires a burst of chat completions, the provider starts returning 429, your gateway retries, the retries also 429, and because nothing at the edge is counting tokens the gateway keeps forwarding traffic that is guaranteed to fail. You pay for the connections, you burn the provider's request quota on rejected calls, and a well behaved tenant gets starved because a noisy neighbour ate the shared limit.

The second is quieter. Request per minute limiting does not describe LLM traffic. One call with a 200k token context and a 4k output costs three orders of magnitude more than a one line classification, but a request counter treats them as identical. So you set RPM low enough to survive the worst case and throttle cheap traffic for no reason, or high enough for cheap traffic and a few long context requests blow through the tokens per minute ceiling. Both show up as unexplained upstream 429s your dashboards say should not happen.

Third, token limits are not spend limits. A tenant can sit inside every TPM ceiling and still run up a bill you never agreed to, because the expensive model costs twenty times the cheap one per token. This module meters four dimensions per subject: requests, input tokens, output tokens and micro USD of spend. It reserves an estimate before proxying, settles the difference when real usage comes back so overestimates get refunded and underestimates become debt, and reads the provider's own rate limit headers to put the subject into a cooldown when the provider says to back off.

## Why I built it

Every gateway stack has a rate limiting plugin and none of them count tokens. Kong, APISIX and stock OpenResty give you fixed or sliding window request counters keyed on a consumer, the right tool for REST and the wrong tool for inference. The vendor SDKs go the other way: they parse their own headers and back off correctly, but they only know about themselves and cannot enforce anything across tenants sharing one key.

Nothing sat in the middle. So this is one dependency free Lua file with a small surface: build it, call `admit` before you proxy, call `observe` after the response. No Redis, no coroutines, no external libraries.

## When to use it

- An OpenResty, Kong or APISIX gateway fronting several LLM providers that each report limits under a different header name.
- A multi tenant product where one customer's batch job must not eat the shared provider quota interactive users depend on.
- A per customer dollar ceiling, for example capping a free tier at a fixed USD per minute whichever model they pick.
- An agent platform where one task fans out into dozens of parallel calls and you need a hard concurrency cap per tenant.
- You are seeing upstream 429s and want the gateway to stop forwarding traffic for as long as the provider asked.
- Streaming, where you know the input token count up front but only learn the output count after the stream closes.

## How it works

The core is a lazy refill token bucket, one per subject per dimension. `_load_bucket` reads the packed state, computes elapsed milliseconds since `updated_ms` and adds `elapsed * limit / 60000` tokens, capped at capacity. No timer and no background sweep, so an untouched bucket costs nothing and reads back correct. Capacity is `limit * burst_multiplier`, default 1.15; the floor is `-capacity * debt_multiplier`, default 3. That negative floor is what makes post hoc settlement safe: a bucket goes into debt and the tenant waits while it refills back through zero.

Four dimensions live in `_bucket_specs`: `requests`, `input_tokens`, `output_tokens` and `cost_micro`. Cost is integer micro USD, so `usd_per_minute` becomes a bucket of `usd_per_minute * 1000000` and no float drift accumulates in stored state. A dimension with no configured limit is skipped.

`admit` builds a subject key from `tenant`, `provider`, `model` and optional `route` (or a caller supplied `scope`), layers the profile as `default`, provider block, `provider.models[model]`, then per call `overrides`, and takes a per subject mutex. `_acquire_lock` uses the store's atomic `add` with a 250 ms lock TTL and a 100 ms deadline, spinning at 5 ms, and `_with_subject_lock` wraps the body in `pcall` so the lock always releases. Inside it checks cooldown, then concurrency against the `active` counter, then each bucket. One short bucket denies the whole call without touching the others, and `retry_after_ms` is the longest wait across all failing buckets, not the first found.

The lease is the settlement record. `encode_lease` packs subject, provider, expiry and the four reserved amounts behind a version byte, with `lease_ttl_ms` (five minutes by default) so a request that dies mid flight cannot leak a concurrency slot. `observe` reloads it under the same lock, deletes it, decrements `active` and calls `_settle_bucket_delta` with `actual - reserved`. Negative deltas refund up to capacity, positive ones deduct to the debt floor. Deleting inside the lock makes `observe` and `release` idempotent, so a second caller gets `already_released` instead of double counting.

`normalize_headers` is the cross provider layer, reading remaining and reset values across `x-ratelimit-*`, bare `ratelimit-*` and `anthropic-ratelimit-*`, with `x-ratelimit-remaining` and `x-ratelimit-reset` as fallback. `parse_reset_delta_ms` accepts the four shapes seen in the wild: Go style durations like `1m30s`, a relative second count, an absolute epoch in seconds or milliseconds told apart by magnitude, and an HTTP date via `ngx.parse_http_time`. `observe` takes the largest of retry after, request reset and token reset, falls back to `fallback_retry_after_ms` on a 429, 503 or 529 with no usable header, and writes a cooldown key that denies the next `admit` with reason `cooldown`. That is the circuit breaker. State itself sits behind a small interface: `resolve_store` takes an `ngx.shared` dict, its name, or any table with `get`, `set`, `add` and `delete`, and everything is stored as plain strings under second granularity TTLs.

## Usage

```lua
local AIGatewayQuota = require("AIGatewayQuota")

local quota = AIGatewayQuota.new({
  store = "ai_quota",          -- ngx.shared dict name, a dict, or omit for MemoryStore
  defaults = { namespace = "aiq:v1", lease_ttl_ms = 300000 },
  profiles = {
    default = { requests_per_minute = 600, input_tokens_per_minute = 400000 },
    anthropic = {
      requests_per_minute = 1000,
      input_tokens_per_minute = 800000,
      output_tokens_per_minute = 160000,
      usd_per_minute = 4.0,
      concurrency = 12,
      models = {
        ["claude-opus-4"] = { usd_per_minute = 1.5, concurrency = 4 },
      },
    },
  },
})

local decision, err = quota:admit({
  tenant = "acct_8812",
  provider = "anthropic",
  model = "claude-opus-4",
  route = "/v1/messages",
  estimated_input_tokens = 18000,
  estimated_output_tokens = 1200,
  estimated_cost_usd = 0.145,
})

if not decision then
  return ngx.exit(500)               -- err is "lock_timeout"
end

if not decision.allowed then
  ngx.header["Retry-After"] = math.ceil(decision.retry_after_ms / 1000)
  ngx.log(ngx.WARN, "throttled: ", decision.reason, " ", decision.bucket or "")
  return ngx.exit(429)
end

-- proxy upstream, then settle with what really happened
quota:observe({
  lease_id = decision.lease_id,
  provider = "anthropic",
  model = "claude-opus-4",
  status = res.status,
  headers = res.headers,
  actual_input_tokens = res.usage.input_tokens,
  actual_output_tokens = res.usage.output_tokens,
  actual_cost_usd = 0.132,
})

-- if the request never reached upstream, hand the reservation back instead
-- decision.release()   or   quota:release(decision.lease_id)

local snap = quota:peek({ tenant = "acct_8812", provider = "anthropic", model = "claude-opus-4" })
-- snap.active, snap.cooldown_ms, snap.requests.remaining, snap.cost_micro.capacity
```

## Notes

- `admit` returns `nil, "lock_timeout"` when the per subject mutex cannot be taken within 100 ms. Pick your own fail open or fail closed policy for that. Other error strings are `params table required`, `lease_id required`, `unknown_lease` and `corrupt_lease`.
- `MemoryStore` is per Lua VM. On OpenResty with several workers, pass an `ngx.shared` dict or each worker enforces its own private copy of the limit.
- The lock relies on the store's `add` being atomic. True for `ngx.shared` and for `MemoryStore` in one VM, not for a naive Redis wrapper unless `add` is `SET NX`.
- Outside OpenResty, `sleep_ms` busy waits on `os.clock` and `now_ms` falls back to `os.time` at one second resolution. `opts.clock` fixes that for the quota logic, but `MemoryStore` TTLs still read the real clock.
- `observe` resolves the profile from the `provider` and `model` you pass it, not the ones recorded at admit time. Pass the same values or settlement lands on a bucket sized by different limits.
- `requests_remaining`, `tokens_remaining` and `concurrency_remaining` come back in `result.feedback` but never feed the buckets. Only reset and retry values drive the cooldown.
- No persistence, no metrics, no logging. It is a decision engine, so wire the returned reasons and `peek` snapshots into your own telemetry.
