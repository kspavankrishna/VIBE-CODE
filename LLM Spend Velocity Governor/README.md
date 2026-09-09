# LLM Spend Velocity Governor

A single tenant's runaway agent loop can burn a month of LLM budget in ten minutes, and most Ruby backends find out when the provider invoice arrives. This is a thread safe choke point that estimates cost before every outbound model call, downgrades the model under budget pressure, and trips a circuit breaker when the *rate* of spend spikes past what that tenant normally does.

**Language:** Ruby | **Lines:** 606 | **Added:** 2026-09-07

## What this solves

This solves a problem every team building an LLM feature into a real Ruby backend hits around month three: someone's per tenant cost graph spikes overnight and nobody finds out until the invoice arrives. Request rate limiting does not catch it, because a request can be small in count and huge in tokens. One call with a 200k token context costs more than a thousand short ones. Counting requests per minute tells you nothing about dollars.

The failure shape is always the same. An agent feature ships, works fine in the demo, then one tenant's automation fires the same expensive prompt hundreds of times in a few minutes. A cron meant to run hourly fires every second. A tool call loop re-triggers itself. Someone finds a prompt injection that makes the agent call itself. The bill for that day looks like a DDoS, and a budget cap alone does not catch it fast enough, because by the time the daily total trips the money is gone.

The static cap has a second failure. When it does trip, every tenant behind it starts getting errors and someone gets paged at 2am to raise a number in a config file. A cost problem becomes an availability problem, usually the more expensive of the two. This file treats spend the way a payments system treats fraud: not just how much but how fast. And when pressure builds it walks down a quality ladder to a cheaper model before refusing anything, so most tenants get degraded service instead of an error page.

## Why I built it

The tooling that exists sits in the wrong place. Provider dashboards are after the fact and aggregate across your whole account, so one tenant's spike stays invisible until it is large. APM cost plugins graph the damage, they do not stop it. Nothing sits between "call the model" and "the model got called" to actually block it, and per tenant attribution is something you end up building yourself anyway.

It is provider agnostic on purpose, so it works the same whether calls go to OpenAI, Anthropic, a self hosted vLLM cluster or three of them mixed through a router. None of it talks to a network. You hand it token counts, it hands back a decision. That makes it safe to unit test and safe to drop straight into a request path. No gems beyond the standard library.

## When to use it

- A Rails or Sinatra app where distinct customers, users or API keys each need their own LLM spend ceiling.
- An agent or tool calling feature where a loop can plausibly re-trigger itself and you want it stopped in minutes, not at the next invoice.
- A Sidekiq fan out where a bad batch queues thousands of model calls and nothing downstream knows the budget.
- A free tier you want to keep serving on a cheap model once the paid headroom is gone.
- A multi tenant SaaS where one enterprise customer's normal spend rate is 100x a small customer's, so a global anomaly threshold is useless.
- Any service that should return a clean 429 with a machine readable reason instead of a 500 when the budget is gone.

## How it works

The entry point is `SpendGovernor::Governor`. Build one per process with a `pricing_table`, a `hard_budget_usd` and a `window_seconds` (`DEFAULT_WINDOW_SECONDS` is 3600). Every call goes through `#authorize` before the provider request and `#record_actual!` after it, or through `#guard`, which does both around a block.

`#authorize` runs a fixed sequence. It checks the `CircuitBreaker` first and returns reason `"circuit_open"` if that tenant's breaker is open. Otherwise it reads current window spend from the ledger, feeds it to the `VelocityDetector`, and if the detector calls it anomalous it trips the breaker and returns `"velocity_anomaly"`. Only then does it price the request: `PricingTable#tier` resolves the model to a `ModelTier` and `ModelTier#cost_for` computes USD from input and output tokens at per 1,000 token rates. If the projection fits under the cap the call is allowed as `"within_budget"`, or `"within_budget_soft_pressure"` once it crosses `soft_threshold_ratio` (default 0.8). That soft reason is the hook to alert on before anything breaks.

If the model does not fit, `PricingTable#downgrade_candidates(from:)` returns every tier whose `quality_rank` is at or below the requested one, sorted cheapest first, and the governor takes the first that fits: `"downgraded_budget_pressure"`. If nothing fits, the call is denied with `"hard_budget_exceeded"` and the breaker records a failure, so repeated denials trip it after `trip_after_failures` (default 3). Every path funnels through `#finalize`, which builds a `Decision` struct carrying allowed, model, reason, estimated cost, window spend, budget, circuit state and tenant id, then fires the optional `on_decision` callback. `Decision` has `to_h` and `to_json` for structured logs.

The ledger is a sliding window with two implementations. `Ledger::InMemory` keeps `[timestamp, amount]` pairs per tenant under a `Monitor` lock and prunes past the cutoff on each read. `Ledger::RedisBucketed` is the multi process version: spend accumulates into fixed width time buckets via `INCRBYFLOAT`, each bucket carries a TTL of four bucket widths so history self expires, and a window read sums the relevant buckets with one `MGET`. Cost per check is O(window / bucket width), not O(requests). It duck types on `#incrbyfloat`, `#expire` and `#mget`, so the redis gem, redis-client or a pool wrapper all work with no declared dependency.

`VelocityDetector` is an exponentially weighted moving average per tenant. `observe!` folds the latest window total into the baseline with `alpha` (default 0.2) and `anomalous?` fires when the current window exceeds `baseline * spike_multiplier` (default 4.0). Two guards keep it quiet: a tenant with no baseline never trips, and a baseline under `min_baseline_usd` (default 0.01) never trips, so three cents becoming fifteen cents is not an incident. Because the baseline is per tenant, a rate that is normal for your biggest customer does not falsely trip for your smallest. `Governor#observe_window!` feeds it at most once per window per tenant, keyed on a floored wall clock slot, so ten calls in one window do not warp that baseline ten times over.

`CircuitBreaker` holds a `State` struct per tenant. `status_for` promotes an open breaker to `:half_open` after `cooldown_seconds` (default 60), `record_success!` closes it, `record_failure!` opens it on threshold or on any failure while half open, and `trip!` opens it immediately for the velocity path. `#guard` re-raises whatever the block raised after recording a breaker failure, so provider timeouts and 5xx responses count the same as budget breaches. `RackMiddleware` stashes the governor and tenant id in `env`, reads the tenant from `HTTP_X_TENANT_ID` by default, and turns an unrescued `BudgetExceededError` into a 429 with a JSON body instead of a 500.

## Usage

```ruby
require_relative "LlmSpendVelocityGovernor"

pricing = SpendGovernor::PricingTable.new(tiers: [
  { name: "flagship", input_cost_per_1k: 0.015,  output_cost_per_1k: 0.075,  quality_rank: 3 },
  { name: "balanced", input_cost_per_1k: 0.003,  output_cost_per_1k: 0.015,  quality_rank: 2 },
  { name: "economy",  input_cost_per_1k: 0.0005, output_cost_per_1k: 0.0015, quality_rank: 1 }
])

governor = SpendGovernor::Governor.new(
  pricing_table: pricing,
  hard_budget_usd: 25.00,
  window_seconds: 3600,
  soft_threshold_ratio: 0.8,
  ledger: SpendGovernor::Ledger::RedisBucketed.new(REDIS, bucket_width_seconds: 60),
  velocity_detector: SpendGovernor::VelocityDetector.new(alpha: 0.2, spike_multiplier: 4.0),
  circuit_breaker: SpendGovernor::CircuitBreaker.new(cooldown_seconds: 60, trip_after_failures: 3),
  on_decision: ->(d) { Rails.logger.info(d.to_json) }
)

# Block form: authorize, run, settle in one place.
governor.guard(tenant_id: "acme", requested_model: "flagship",
               estimated_input_tokens: 4_000, estimated_output_tokens: 1_500) do |model, decision|
  Rails.logger.warn("downgraded to #{model}") if decision.downgraded?("flagship")
  response = MyLlmClient.complete(model: model, prompt: prompt)
  { input_tokens: response.usage.input, output_tokens: response.usage.output }
end
# raises SpendGovernor::BudgetExceededError when denied

# Split form, for async jobs where check and settlement live apart.
decision = governor.authorize(tenant_id: "acme", requested_model: "flagship",
                              estimated_input_tokens: 4_000, estimated_output_tokens: 1_500)
if decision.allowed?
  resp = MyLlmClient.complete(model: decision.model, prompt: prompt)
  governor.record_actual!(tenant_id: "acme", model: decision.model,
                          input_tokens: resp.usage.input, output_tokens: resp.usage.output)
else
  head :too_many_requests # reason: circuit_open, velocity_anomaly or hard_budget_exceeded
end

# Rack, in config/application.rb or config.ru
config.middleware.use SpendGovernor::RackMiddleware, governor: governor
# custom extraction:
# SpendGovernor::RackMiddleware.new(app, governor: governor,
#                                   tenant_extractor: ->(env) { env["warden"].user&.account_id })
```

Run the file directly for a live demo of budget drain, breaker trip and velocity spike.

```
ruby "LlmSpendVelocityGovernor.rb"
```

## Notes

- Only the ledger is shared across processes. `VelocityDetector` baselines, `CircuitBreaker` state and the `observe_window!` bookkeeping live in process memory, so each worker learns its own baseline and trips its own breaker.
- `authorize` does not reserve budget. Two concurrent calls can see the same window spend and both pass, so the cap can be overshot by roughly one in flight request per concurrent caller. It is a governor, not a two phase commit.
- The half open state is reported but not gated. `authorize` only rejects on `:open`, so after cooldown every arriving request is let through, not exactly one probe. `record_actual!` calls `record_success!`, so the first completed call closes it.
- You supply the token counts. No tokenizer, no provider SDK. Estimates drive `authorize`, real usage drives `record_actual!`, and the gap between them is your accuracy budget.
- Prices are USD per 1,000 tokens and are whatever you put in the `PricingTable`. Nothing refreshes them when a provider changes pricing, and there is no handling of cached input rates or batch discounts.
- `RedisBucketed#reset!` uses `KEYS` with a glob, which is O(n) on the keyspace. Fine for tests and admin actions, not a hot path. It no ops silently if the client does not respond to `#keys`.
