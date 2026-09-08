# Inference Admission Ledger

Background jobs, web requests and retry loops all fire model calls at the same provider at the same moment, and by the time the bill arrives one tenant has eaten the entire budget. This is a single Elixir GenServer that decides whether an inference request is allowed to start, before you spend the money.

**Language:** Elixir | **Lines:** 1190 | **Added:** 2026-04-29

## What this solves

Multi tenant LLM admission control for Elixir, Phoenix, Oban, Broadway and plain OTP services where OpenAI, Anthropic, Gemini or internal model calls get expensive or fall over under burst traffic. It stops duplicate retries, noisy neighbour overload and budget drift by turning every model request into a lease with reserved concurrency, reserved token budget and reserved spend held before the call starts.

The failure mode this catches is not "the SDK call failed". That one is loud and your error tracker finds it. The quiet one: a Phoenix request, an Oban job retrying with backoff and a Broadway batch pipeline all reach the provider inside the same second. Nobody is counting, so the provider counts for you and returns 429s. Your retry logic sees the 429, backs off, then fires again into the same crowd. Meanwhile tenant A pushed 4 million input tokens through a summarisation batch and tenant B, a paying customer with a chat box, is throttled by a limit tenant A consumed. Nobody notices until the invoice or the support ticket.

The second quiet failure is counting usage after the fact. If you record tokens only when a response comes back, then during a slow provider window you have hundreds of calls in flight and zero of them counted. The limiter thinks the system is idle and admits hundreds more. Providers are slow precisely when they are overloaded, so the moment you most need the brake is the moment a naive counter has no data. This ledger reserves the estimated cost at admission and settles it only when the work finishes. Dead workers are the third case: a crashed worker never calls back, so an optimistic limiter leaks capacity until a restart. Here every lease carries an expiry, and expiry settles the reservation into the rolling window rather than discarding it.

## Why I built it

Hex has good general purpose rate limiters and they all count one number: requests. Inference does not work that way. The cost of a call is input tokens plus output tokens plus dollars, and the three ceilings bind at different times. A limiter that counts only requests will let ten 200k context calls through while blocking a thousand trivial ones. None of them know what a retry is either, so an Oban job with `max_attempts: 5` is five separate charges for one piece of work.

I wanted one process holding all four decisions together, in plain OTP so it sits in a real supervision tree with no ETS tables to own, no Redis to keep alive and no external dependency. It is node local by design.

## When to use it

- A Phoenix AI gateway where several tenants share one provider API key and one rate limit.
- Oban jobs that call a model and retry on failure, where retry three must not count as a fourth paid request.
- A Broadway pipeline doing batch summarisation that has to stay under a spend ceiling per minute.
- An agent backend where one user turn fans out into twenty tool calls and a runaway loop should hit a wall.
- Streaming responses open for minutes, where a fixed timeout is wrong but an unbounded one is worse.
- An eval harness that must not spend the production budget while sweeping a prompt grid.

## How it works

One `GenServer` holds a `State` struct: active `Lease` structs by id, an idempotency index, concurrency counters for global, tenant and workload, reserved `Usage` totals, and a `Window` per tenant plus one global. `Usage` is the four number vector in `@usage_metrics`: `requests`, `input_tokens`, `output_tokens` and `cost_micros`. Every helper (`usage_add/2`, `usage_sub/2`, `usage_fits?/3`) works on all four at once, and `usage_sub/2` clamps at zero so a double release cannot drive a counter negative.

`admit/3` builds a demand through `build_demand/4`, which validates every integer and returns a `%Decision{status: :invalid, reason: :invalid_demand}` rather than raising into the caller. Then `maybe_duplicate/3` checks the idempotency index, keyed by the `{tenant, idempotency_key}` tuple, and if a live lease matches it hands back that same lease with `status: :duplicate`. A retry for work already in flight gets the original lease, not a second reservation.

Otherwise `check_admission/3` runs a `with` chain in fixed order: global concurrency, tenant concurrency, workload concurrency, global budget, tenant budget. The first failure short circuits into a `%Decision{}` carrying the scope, the blocking metric, the limit, what was observed, what was needed and the headroom left. Budget checks compare `reserved + settled` against the limit, so in flight work counts. `budget_check/7` splits two rejections: a demand larger than the limit itself is impossible and returns `retry_after_ms: nil`, while a demand that merely does not fit now gets a real wait estimate.

The rolling window is an Erlang `:queue` of `%{at_ms, usage}` entries plus a running total, not a fixed bucket. `window_add/3` pushes to the tail and adds to the total, `prune_window_entries/3` peeks at the head and subtracts anything older than `now - window_ms`. That is an exact sliding window with O(1) amortised maintenance and no boundary spike. It also makes `retry_after_ms` honest: `retry_after_for_budget/7` builds the future release events, active leases at their `expires_at_ms` and settled entries at `at_ms + window_ms`, sorts them and replays them with `Enum.reduce_while/3` until the demand fits. That is when capacity really returns, not a guessed constant. For concurrency rejections `min_expiry_delta/2` returns the soonest lease expiry in that scope.

Leases are settled by `complete/4`, released by `cancel/3` or reclaimed on expiry. `sweep/2` runs at the top of every call and on the `:cleanup` timer set by `schedule_cleanup/1`, and for each expired lease it drops the idempotency entry, releases the active counters and settles the original reservation into the window. `heartbeat/3` pushes `expires_at_ms` forward but clamps at the lease's `max_expires_at_ms` hard stop, computed once at grant time, so a long stream stays alive without pinning capacity forever. Lease ids are 18 random bytes, url encoded. Clocks are `System.monotonic_time(:millisecond)`, so NTP steps cannot corrupt a window.

## Usage

```elixir
# In your supervision tree
children = [
  {InferenceAdmissionLedger,
   name: MyApp.Ledger,
   window_ms: 60_000,
   global_concurrency: 64,
   default_tenant_concurrency: 4,
   workload_concurrency: %{batch_summarize: 8, chat: 40},
   global_cost_micros_per_window: 5_000_000,
   tenant_input_tokens_per_window: 400_000,
   tenant_policies: %{
     "enterprise-tenant" => [concurrency: 32, cost_micros_per_window: 2_000_000]
   }}
]

# Before the provider call
case InferenceAdmissionLedger.admit(MyApp.Ledger, tenant_id,
       workload: :chat,
       ttl_ms: 45_000,
       idempotency_key: {:oban_job, job.id},
       estimated_input_tokens: 12_000,
       estimated_output_tokens: 1_500,
       estimated_cost_micros: 42_000,
       meta: %{model: "claude-sonnet"}) do
  {:ok, lease, %{status: :granted}} ->
    # ... make the model call ...
    InferenceAdmissionLedger.complete(MyApp.Ledger, lease,
      %{input_tokens: 11_842, output_tokens: 1_310, cost_micros: 39_500})

  {:ok, lease, %{status: :duplicate}} ->
    {:ok, :already_in_flight, lease.id}

  {:error, %{status: :rejected, reason: reason, retry_after_ms: wait}} ->
    {:snooze, reason, wait}
end

# Long running stream: extend, but never past the hard stop
InferenceAdmissionLedger.heartbeat(MyApp.Ledger, lease, ttl_ms: 30_000)

# Call never reached the provider
InferenceAdmissionLedger.cancel(MyApp.Ledger, lease)

# Operational view: reserved, settled, effective and headroom per tenant
InferenceAdmissionLedger.snapshot(MyApp.Ledger)
InferenceAdmissionLedger.prune(MyApp.Ledger)
```

## Notes

- Node local only. State lives in one process with no ETS, no persistence and no distribution. A restart clears every lease and every window.
- Every admission serialises through one GenServer. Checks are cheap map lookups, but `retry_after_for_budget/7` walks all leases and window entries in scope, so rejection costs more than a grant at high lease counts.
- Expiry settles the reservation, it does not discard it. Bad estimates leak into your accounting when workers die often.
- `priority` is validated and stored on the lease, but nothing schedules on it. There is no queue and no fair share ordering. Rejection is immediate and the caller decides what to do with `retry_after_ms`.
- Workload scope is concurrency only. No per workload token or spend budget, and `future_release_events/3` returns an empty list for that scope.
- `complete/4` always settles exactly one request unless you pass `:requests`. Use `cancel/3` when the provider was never called, otherwise you charge the window for work that never happened.
- `heartbeat/3` accepts a `ttl_ms` of zero, which expires the lease at the next sweep. A `default_lease_ttl_ms` above `max_lease_ttl_ms` raises `ArgumentError` at start.
- Zero dependencies beyond OTP. Uses `:queue` and `:crypto` from the standard library, nothing else.
