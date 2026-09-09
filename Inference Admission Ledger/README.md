# Inference Admission Ledger

Background jobs, web requests and retry loops all hit your model provider at the same moment, one tenant burns the whole budget and everyone else gets throttled. This is a single Elixir GenServer that decides, before the call goes out, whether a request is allowed to start at all.

**Language:** Elixir | **Lines:** 1190 | **Added:** 2026-04-29

## What this solves

The failure mode is not that the SDK call fails. The failure mode is that nothing in your system knows how many model calls are already in flight. A Phoenix controller fires a completion, an Oban worker retries the same job three times because the provider was slow, a Broadway pipeline drains a backlog of 4000 messages, and all of it lands on one API key inside the same ten seconds. The provider returns 429s, your retry logic makes it worse, and the bill for that hour is four times what you modelled.

Cost drift is the quieter half of it. Per-request spend is tiny, so nobody puts a limit on it, and then a prompt template change doubles input tokens across every tenant at once. You find out on the invoice. The same applies to a single tenant with a runaway agent loop: without a per-tenant ceiling, one customer's bad session eats the global rate limit and every other customer sees timeouts. Support notices before monitoring does.

Duplicate work is the third piece. Slow providers cause client timeouts, client timeouts cause retries, and retries cause a second identical call while the first one is still streaming. You pay twice for the same answer and both calls count against the same rate limit. Deduplicating after the fact does not help, because the money is already spent.

This module puts all four decisions in one place: global concurrency, per-tenant concurrency, rolling request, token and spend ceilings, and in-flight duplicate detection. A granted request comes back as a lease holding reserved budget. The budget stays reserved until you complete it, cancel it, or it expires. That means a slow provider cannot trick the ledger into admitting more work than the system can afford, because the reservation is counted from the moment the call starts and not from the moment it finishes.

## Why I built it

Elixir has good primitives for this and no assembled answer. Hammer and ExRated give you request counting but nothing that understands tokens or spend. A `:poolboy` pool or a `Task.Supervisor` with `max_children` gives you concurrency but no idea which tenant is consuming it. Provider SDKs give you retries with backoff, which is exactly the behaviour that turns a hot minute into an outage. Stitching those together per project means every service reimplements the same reservation logic slightly differently, and the differences show up as budget drift.

The specific gap is admission control against estimated usage. Rate limiters count what already happened. An inference gateway needs to count what is about to happen, hold that reservation while the provider is slow, and settle it against reality afterwards. That requires leases, not counters, and it requires being conservative when a caller dies without reporting back.

## When to use it

- A Phoenix AI gateway where several tenants share one provider API key and one rate limit
- Oban or Broadway workers that retry on failure and can duplicate an in-flight model call
- An agent backend where a single loop can issue hundreds of calls and needs a per-tenant ceiling
- Batch summarization or evaluation runs that must not starve interactive traffic on the same key
- Any service with a monthly spend cap where you want requests rejected with a retry hint instead of a surprise invoice
- Streaming inference where a call can run for minutes and you need a heartbeat plus a hard stop

## How it works

The whole ledger is one `GenServer` holding a `State` struct: a map of live leases, an idempotency index, active counters split by total, tenant and workload, reserved `Usage` totals for global and per tenant, and rolling `Window` structs for settled usage. Every metric is one of `@usage_metrics`, which is `[:requests, :input_tokens, :output_tokens, :cost_micros]`. Cost is stored in integer micros so there is no float arithmetic anywhere in the accounting path.

`admit/3` builds a demand map through `build_demand/4`, which validates every option with `validate_positive_integer!` or `validate_non_negative_integer!` and converts an `ArgumentError` into a `%Decision{status: :invalid}` rather than crashing the caller. If an `:idempotency_key` is present, `maybe_duplicate/3` looks up `{tenant, key}` in the idempotency index and, when the matching lease is still alive, returns that same lease with `status: :duplicate` and `reason: :in_flight_duplicate`. Otherwise `check_admission/3` runs a `with` chain in a fixed order: global concurrency, tenant concurrency, workload concurrency, global budget, tenant budget. The first failure wins, so a rejection always names one scope and one metric.

Budget checks compare reserved plus settled against the limit. `check_global_budget/3` adds `state.global_reserved` to `state.global_window.totals`, and the tenant version does the same with the per-tenant maps. `budget_check/7` first looks for an impossible metric, a single reservation larger than the configured limit, and rejects that with `retry_after_ms: nil` because waiting will never help. Otherwise it finds the first blocked metric and computes a real retry hint. `retry_after_for_budget/7` builds the list of future release events, lease expiries plus window entries aging out at `at_ms + window_ms`, sorts them by time and replays them with `Enum.reduce_while/3`, subtracting each release until the demand fits. The answer is the wall clock delay at which the request would actually succeed. Concurrency rejections use `retry_after_for_concurrency/4`, which is the minimum expiry delta across the relevant leases.

The rolling window is an Erlang `:queue` of `%{at_ms:, usage:}` entries plus a running `totals` struct. `window_add/3` pushes an entry and adds to totals. `window_prune/3` pops from the front while the head is at or before `now - window_ms`, subtracting each dropped entry from totals. That is a plain sliding window with O(1) amortised eviction, not a fixed bucket, so a burst at the end of one minute cannot be doubled by a burst at the start of the next.

Granting calls `make_lease_id/0`, which is 18 bytes from `:crypto.strong_rand_bytes/1` encoded as unpadded URL-safe base64, then `reserve_active/2` bumps the counters and adds the reservation to the global and tenant reserved usage. `complete/4` pops the lease, drops the idempotency entry, releases the reservation and settles the actual usage into both windows via `settle_usage/4`. `cancel/3` releases without settling anything. `sweep/2` runs at the top of every call and on the `:cleanup` timer, and for each expired lease it settles the original reservation into the window. That is the conservative choice on purpose: a crashed worker charges its estimate rather than nothing. `heartbeat/3` pushes `expires_at_ms` forward but clamps it to `lease.max_expires_at_ms`, so a stuck stream cannot hold a slot forever. All time comes from `System.monotonic_time(:millisecond)`, so clock changes cannot corrupt the window.

## Usage

```elixir
# In your supervision tree
children = [
  {InferenceAdmissionLedger,
   name: MyApp.Ledger,
   window_ms: 60_000,
   global_concurrency: 64,
   default_tenant_concurrency: 4,
   workload_concurrency: %{batch_summarize: 8},
   global_cost_micros_per_window: 2_000_000,
   tenant_input_tokens_per_window: 400_000,
   tenant_policies: %{
     "acme" => [concurrency: 16, cost_micros_per_window: 500_000]
   }}
]

# Before the provider call
case InferenceAdmissionLedger.admit(MyApp.Ledger, "acme",
       workload: :chat,
       ttl_ms: 45_000,
       idempotency_key: job_id,
       estimated_input_tokens: 3_200,
       estimated_output_tokens: 800,
       estimated_cost_micros: 4_100,
       meta: %{model: "claude-sonnet"}) do
  {:ok, lease, %{status: :granted}} ->
    case call_provider(prompt) do
      {:ok, resp} ->
        InferenceAdmissionLedger.complete(MyApp.Ledger, lease, %{
          input_tokens: resp.usage.input_tokens,
          output_tokens: resp.usage.output_tokens,
          cost_micros: price_micros(resp)
        })

      {:error, :never_sent} ->
        InferenceAdmissionLedger.cancel(MyApp.Ledger, lease)
    end

  {:ok, _lease, %{status: :duplicate, retry_after_ms: ms}} ->
    {:error, {:already_in_flight, ms}}

  {:error, %{status: :rejected, scope: scope, metric: metric, retry_after_ms: ms}} ->
    {:error, {:throttled, scope, metric, ms}}
end

# Long streams: extend the lease, never past its hard stop
InferenceAdmissionLedger.heartbeat(MyApp.Ledger, lease, ttl_ms: 30_000)

# Operational view: reserved, settled, effective and headroom per tenant
InferenceAdmissionLedger.snapshot(MyApp.Ledger)
InferenceAdmissionLedger.prune(MyApp.Ledger)
```

## Notes

- Single process, single node. All state lives in one GenServer with no ETS and no distribution, so every `admit` is a serialized `GenServer.call` with a 5000 ms default `:call_timeout`. It is a real bottleneck at very high call rates and it does not coordinate across a cluster.
- The ledger never queues or waits. It grants or rejects immediately and hands you a `retry_after_ms` hint. Backoff and retry are the caller's job.
- `:priority` is validated and stored on the lease but no admission path reads it. There is no priority preemption or fairness ordering today.
- Expired leases settle their estimate, not their reality. If your estimates are high you will over-charge the window for crashed workers. If they are low you will under-count. Estimates matter.
- No process monitoring. A caller that dies without calling `complete/4` or `cancel/3` holds its reservation until the TTL expires, capped by `max_lease_ttl_ms`, which defaults to 300_000 ms.
- No telemetry events, no logging, no persistence. State is lost on restart, which resets the rolling window and every active lease.
- `complete/4` always settles one request unless you pass `:requests` explicitly, and `start_link/1` raises `ArgumentError` if `default_lease_ttl_ms` exceeds `max_lease_ttl_ms`.
