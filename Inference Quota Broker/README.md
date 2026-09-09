# Inference Quota Broker

Rails apps, Sidekiq workers, cron backfills and agent jobs all hit the same LLM provider limits at once, then fail in unpredictable ways. This is a Ruby admission control broker that decides before dispatch whether a request is safe to send, and tells you exactly when to retry if it is not.

**Language:** Ruby | **Lines:** 946 | **Added:** 2026-04-25

## What this solves

The failure mode is not "we got a 429 once". It is four limits interacting at once on the same provider key: requests per minute, tokens per minute, a spend budget and a concurrency cap. A plain rate limiter counts one dimension and counts it after the fact. In production that looks like a nightly summarization backfill queueing 4,000 jobs across 25 Sidekiq threads, none of which know the user-facing chat endpoint draws on the same key. The provider returns 429s, your retry logic fires, the retries add load, and the customer-facing feature degrades while a batch nobody was waiting on eats the budget. The person who notices is a customer. Cost overruns have the same shape: a runaway agent loop burns the monthly budget in an afternoon because no single place knew the running total.

The second failure mode is reconciliation. You estimated 8,000 tokens and the model returned 31,000. Charge the estimate and never correct it and your accounting drifts on every call. Charge only actuals and nothing is accounted for between dispatch and completion, so twenty concurrent workers all pass the check at the same instant. The third is a crashed worker: a process gets OOM killed mid request, the provider still served it and still counted it, and a limiter that forgets the uncommitted reservation is permanently optimistic in the wrong direction.

This broker handles all three with leases. You reserve expected usage before dispatch, hold concurrency while the call is in flight, commit real usage when the response lands, or cancel if the work never left the process. If a worker dies and never reports back, expiry conservatively charges the reserved amount so shared quota is not oversubscribed.

## Why I built it

Every rate limiter I could reach for was single dimension and after the fact: count requests, sleep, hope. None modeled the gap between estimated and actual usage, none held concurrency across an in-flight call, none gave a deterministic retry time. `Retry-After` from the provider is a guess about the provider. It says nothing about your own budget, your tenant isolation or the batch job running next to you.

The other gap was answering "when". A good admission decision is not just no, it is no plus the earliest instant at which yes becomes possible. That number is computable from the sliding window contents, and once you have it, exponential backoff and jitter stop being necessary here. So the decision engine is a plain object with an injectable clock and no Redis in the core, testable deterministically and backable by whatever store you want later.

## When to use it

- A Rails monolith where user-facing chat and a batch summarization job share one provider API key.
- Multi-tenant AI features where one enterprise customer must not starve everyone else on the shared model budget.
- An internal inference gateway that must return a deterministic "retry after" instead of passing vendor 429s through.
- A nightly backfill that runs full speed at 3am but yields to interactive traffic during the day.
- Agent loops where one step is expensive enough that a duplicate enqueue actually costs money.
- Anywhere you need a dry run: "if I send this now, does it fit?" without committing anything.

## How it works

State lives in three hashes behind a single `Mutex`: `@scopes`, `@leases` and `@idempotency_index`. A scope is a named budget, registered with `register_scope("openai:gpt-5", policy)` or `upsert_scope` to replace an existing one. Each scope holds a `Policy` struct: a list of `WindowLimit` entries, an optional `concurrency_limit`, a `lease_ttl`, an `expire_strategy` and a `settlement_time_source`. Both structs validate in their constructors and raise `InvalidPolicyError` on non positive capacity or interval, a duplicate limit key or an unknown strategy symbol.

The window accounting is a sliding window log, not a token bucket and not a fixed window counter. Each scope keeps `events_by_unit`, an array of `UsageEvent(at, amount)` per unit in timestamp order. `used_amount` sums every event with `at > now - interval`, then adds the reservation of every active lease created inside that same window. That second half is the important part: an in-flight call counts immediately, so twenty workers checking at once cannot all pass. A log rather than a bucket because the log is what lets you compute exact release times.

`earliest_window_release_at` is that computation. It gathers every contribution inside the window, settled events and active lease reservations alike, works out the overflow above capacity, sorts contributions by time, then walks them oldest first accumulating released amount until enough has aged out to cover the overflow. It returns that contribution's timestamp plus the interval. If the whole window still cannot cover the request it returns `Float::INFINITY`, the honest answer for a reservation larger than the limit itself. Concurrency blocking uses `earliest_concurrency_release_at`, the soonest `expires_at` among active leases, falling back to `now + lease_ttl`.

`analyze_reservation` runs `blocking_reasons_for_scope` over every requested scope and takes the maximum retry time across all reasons, so a multi scope reservation is all or nothing and reports every reason at once. Pass `deadline_at` or `deadline_in` and a `:deadline` reason is appended when the computed retry time falls past it. `plan` runs exactly this and mutates nothing. `reserve` runs it and, on success, creates a `LeaseRecord` with a `SecureRandom.uuid` and registers it in every scope's `active_lease_ids`.

Settlement happens in `commit`. It releases the concurrency hold, then appends usage events to each scope. `normalize_actual` starts from the reservation and overrides per unit, so passing `{ output_tokens: 31_000 }` corrects that one unit and leaves the rest of the estimate intact; passing zero removes a unit entirely. Every reservation implicitly carries `requests: 1.0` via `REQUEST_UNIT` unless you set it yourself. `settlement_time_source` decides whether the event is stamped at `created_at` (the default, so a slow call does not shift its cost into a later window) or at `finished_at`. After committing, `detect_scope_violations` re runs the window check and returns any limit the true actuals pushed over, your signal that an estimate was badly wrong.

`sweep_expired!` runs at the top of every mutating call and every read that reports state. It finds active leases past `expires_at` and, under the default `:charge_reserved` strategy, writes the reserved amounts as real usage before marking the lease `:expired`; under `:cancel` it just releases the hold. Calling `commit` or `cancel` twice returns a `Finalization` with `replayed: true` instead of double charging, and any other transition raises `LeaseStateError`. Idempotency keys are indexed only while a lease is active, so a duplicate `reserve` inside the TTL returns the same lease with `reused: true`, while reusing a key with different scopes or a different reservation raises `InvalidReservationError`. Float comparisons all go through `EPSILON` at 1e-9 and reported amounts are rounded to six decimals.

## Usage

```ruby
require_relative "InferenceQuotaBroker"

broker = InferenceQuotaBroker.new  # optional clock: -> { Time.now.to_f }

broker.register_scope("openai:gpt-5", {
  name: "gpt-5 shared key",
  window_limits: [
    { name: "rpm",    unit: :requests,     capacity: 500,     interval: 60.0 },
    { name: "tpm",    unit: :total_tokens, capacity: 800_000, interval: 60.0 },
    { name: "budget", unit: :usd,          capacity: 250.0,   interval: 86_400.0 }
  ],
  concurrency_limit: 25,
  lease_ttl: 120.0,
  expire_strategy: :charge_reserved,      # or :cancel
  settlement_time_source: :created_at     # or :finished_at
})

broker.register_scope("tenant:acme", {
  window_limits: [{ unit: :total_tokens, capacity: 100_000, interval: 60.0 }]
})

# Dry run, mutates nothing
plan = broker.plan(
  scopes: ["openai:gpt-5", "tenant:acme"],
  reservation: { total_tokens: 8_000, usd: 0.42 }
)
plan.granted?            # => true / false
plan.retry_in            # => seconds until admissible
plan.reasons             # => [{ scope:, type: :window, limit_name:, used:, capacity:, retry_at:, ... }]

# Real reservation, held across the call
decision = broker.reserve(
  scopes: ["openai:gpt-5", "tenant:acme"],
  reservation: { total_tokens: 8_000, usd: 0.42 },
  metadata: { job: "summarize", tenant_id: 41 },
  idempotency_key: "job-8821",
  deadline_in: 30.0,          # or deadline_at:, not both
  lease_ttl: 90.0             # capped to the scope minimum
)

unless decision.granted?
  sleep(decision.retry_in)
  raise Retryable
end

lease_id = decision.lease.id
decision.reused             # true if the idempotency key replayed

begin
  response = call_provider!
  broker.commit(lease_id, actual: { total_tokens: 31_412, usd: 1.64 })
rescue DispatchNeverHappened
  broker.cancel(lease_id)
end

broker.snapshot("openai:gpt-5").to_h   # limits with used / available, plus active leases
broker.fetch_lease(lease_id)           # LeaseView
broker.list_scopes                     # sorted scope names
```

## Notes

- In memory and single process. State dies with the process. For cross process coordination keep this as the decision engine and back the same lease lifecycle with Redis or Postgres. There is no storage abstraction in the file, you would be editing it.
- The default clock is `Process::CLOCK_MONOTONIC`. Explicit `now:` values must be in the same time base as the clock you injected. Mixing monotonic and wall clock silently corrupts the window math.
- Nothing blocks or queues. `reserve` returns immediately. Sleeping and retry ordering are the caller's job, and there is no fairness or FIFO guarantee between competing callers.
- `@leases` is never garbage collected. Committed, cancelled and expired records stay indexed by id forever so `fetch_lease` keeps working, which grows without bound in a long lived high volume process.
- `used_amount` and `earliest_window_release_at` scan events and active leases linearly on every check. Fine for hundreds of events per window, not millions.
- `commit` reports violations after the fact, it does not reject an actual that exceeds capacity. Over budget is detected, never prevented, because the provider already served the request. Expiry is lazy too: a quiet broker holds stale leases until someone calls it.
- Errors are raised, not returned: `UnknownScopeError`, `UnknownLeaseError`, `InvalidReservationError`, `InvalidPolicyError` and `LeaseStateError`, all under `InferenceQuotaBroker::Error`. No CLI, no exit codes, it is a library object.
