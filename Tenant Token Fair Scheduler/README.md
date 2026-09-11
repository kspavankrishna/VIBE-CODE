# Tenant Token Fair Scheduler

Five teams share one org-level LLM API key, and the team sending 40k-token prompts quietly eats the whole tokens-per-minute quota while everyone else gets 429s. This is a Deficit Round Robin fair scheduler in Elixir that splits a shared token budget across tenants by cost, not by request count.

**Language:** Elixir | **Lines:** 455 | **Added:** 2026-09-11

## What this solves

You have one rate-limited upstream. One OpenAI or Anthropic key with a tokens-per-minute ceiling, one vLLM cluster on a fixed GPU pool, one egress quota. Several internal services call through it at the same time. The moment that happens you need a fairness policy, and the two policies most gateways reach for are both wrong.

The first is first come first served with no scheduling at all. The noisiest caller wins. A batch job that fires 200 summarisation requests in a burst will hold the upstream for a minute while the interactive chat endpoint your customers are actually looking at times out. Nobody notices until support tickets arrive, because the gateway's own metrics look healthy: it is passing traffic at full rate, just all of it to one tenant.

The second is a fixed per-tenant token bucket. That fixes starvation but wastes capacity. If four of your five tenants are idle at 3am, the fifth still gets exactly one fifth of the quota while 80% of your paid-for throughput evaporates.

Plain round robin does not rescue you either, and this is the part people miss. Round robin gives each tenant one turn per round, so it treats a 50-token classification call and a 50,000-token document analysis as equal. A tenant with huge prompts still crowds out a tenant with small ones, just more politely. The unit of fairness has to be token cost, not request count, because with LLM traffic the variance in cost per request is enormous.

Deficit Round Robin solves exactly this shape of problem. It came out of network routers in the 1990s for variable-sized packets sharing a fixed link, which is structurally the same thing as variable-sized LLM requests sharing a fixed tokens-per-minute budget. Work-conserving, O(1) per dequeue, no starvation and no wasted idle capacity.

## Why I built it

Every internal AI gateway I looked at either had no fairness logic at all or had a per-tenant rate limiter bolted on, and neither is right when your own quota is the scarce resource. The gateway libraries in the ecosystem are built around protecting an upstream from a single client, not around dividing one upstream fairly among many clients whose requests cost wildly different amounts.

The other gap is LLM specific. Classic DRR assumes you know the packet size before you dequeue. With LLMs you do not: output length is unknown until the response has streamed back. So this version charges an estimate at grant time and adds a settle call that corrects the shared bucket once the real usage is known. Without that correction, systematic underestimation drains your real quota while the scheduler's books stay clean.

## When to use it

- An internal LLM gateway where several product teams share one provider key and one TPM ceiling
- An agent platform serving multiple customers off a single upstream, where one customer's long-context run must not stall everyone else
- A self-hosted vLLM or TGI deployment with a fixed GPU pool and no per-caller isolation
- Mixed interactive and batch traffic on the same quota, where chat latency matters and nightly jobs do not
- Any work queue where cost per item varies by two orders of magnitude and round robin by count would be unfair
- You need a caller to get a fast "no" and shed load instead of piling up latency behind a full queue

## How it works

The whole thing is one `GenServer`, `TenantTokenFairScheduler`, with no external dependencies and no database. State is a struct holding a map of `tenants`, a round robin `order` list, an `rr_index`, and a shared token bucket described by `budget`, `budget_cap` and `budget_per_tick`. Each tenant is a `Tenant` struct carrying its `quantum`, `priority`, `max_queue`, current `deficit`, `depth`, an Erlang `:queue` of pending `Request` structs and three counters: `granted`, `dropped` and `expired`.

Admission and execution are two separate decisions, which is the design choice that makes this usable under load. `enqueue/4` is a synchronous call that either accepts the request into the tenant's queue and hands back a `make_ref()` ticket, or immediately returns `{:error, :queue_full}` when `depth >= max_queue`. It never blocks waiting for capacity. Permission to actually run arrives later as a message to the calling process: `{:scheduler_granted, tenant_id, ticket, meta}` or `{:scheduler_expired, tenant_id, ticket}`.

The scheduling round runs on a timer. `schedule_tick/1` uses `Process.send_after`, and each `:tick` does three things in order: tops the shared bucket up by `budget_per_tick` clamped to `budget_cap`, calls `drop_expired/1`, then calls `run_drr_round/1`. `drop_expired/1` walks every tenant queue with `split_expired/3`, which relies on requests being FIFO with TTLs assigned at enqueue time, so expired entries are always a prefix and the scan stops at the first still-fresh entry rather than traversing the whole queue.

The DRR core is `do_round/3` and `drain_tenant/4`. Each visited tenant gets `quantum * multiplier` added to its deficit, where the multiplier comes from `@priority_multiplier`, giving `:interactive` tenants twice the share of `:batch` tenants at the same quantum. `drain_tenant/4` then peeks at the head of the queue and dequeues only while the next request's cost fits inside both the tenant's deficit and the shared budget. Strict head of line, no reordering inside a tenant. When a tenant's queue goes empty its deficit is reset to zero, which is what stops an idle tenant banking unused capacity and hoarding it for a later burst.

Two details matter for fairness over time. `rr_index` persists across ticks and the round resumes where the last one stopped, so no tenant is systematically favoured by always being first in line when the shared budget runs dry mid-round. And `do_round/3` halts as soon as `visited >= count` or `budget <= 0`, which bounds the work per tick at one pass over the tenant list.

`settle/5` is the LLM specific piece. After the real token usage comes back you cast it in, and `handle_cast({:settle, ...})` applies `actual_cost - estimated_cost` against the shared budget. Underestimates claw budget back, overestimates return it. `stats/1` returns a snapshot of budget and per-tenant depth, deficit, priority and counters, which is what you graph and what upstream load shedding reads. Observability goes through an optional `:telemetry` option, a plain three-arity function invoked by `emit/4` on grant, expire, queue-full and settle. It is wrapped in a `try/rescue` so a bad callback logs a warning instead of killing the scheduler.

## Usage

```elixir
children = [
  {TenantTokenFairScheduler,
   name: MyApp.Scheduler,
   budget_per_tick: 6_000,
   budget_cap: 20_000,
   tick_interval_ms: 250,
   default_ttl_ms: 15_000,
   telemetry: fn event, meas, meta -> :telemetry.execute([:llm, event], meas, meta) end}
]

Supervisor.start_link(children, strategy: :one_for_one)

TenantTokenFairScheduler.register_tenant(MyApp.Scheduler, "team-checkout",
  quantum: 2_000, priority: :interactive, max_queue: 64)

TenantTokenFairScheduler.register_tenant(MyApp.Scheduler, "nightly-batch",
  quantum: 2_000, priority: :batch, max_queue: 512)

case TenantTokenFairScheduler.enqueue(MyApp.Scheduler, "team-checkout", 1_800, ttl_ms: 8_000) do
  {:ok, ticket} ->
    receive do
      {:scheduler_granted, "team-checkout", ^ticket, _meta} ->
        {:ok, resp} = call_upstream_llm(prompt)

        TenantTokenFairScheduler.settle(
          MyApp.Scheduler, "team-checkout", ticket, 1_800, resp.usage.total_tokens
        )

      {:scheduler_expired, "team-checkout", ^ticket} ->
        {:error, :timed_out_in_queue}
    after
      5_000 -> {:error, :scheduler_unresponsive}
    end

  {:error, :queue_full} ->
    {:error, :shed_load}

  {:error, :unknown_tenant} ->
    {:error, :not_registered}
end

TenantTokenFairScheduler.stats(MyApp.Scheduler)
# => %{budget: 14_200, budget_cap: 20_000,
#      tenants: %{"team-checkout" => %{depth: 3, deficit: 200, priority: :interactive,
#                                      granted: 812, dropped: 4, expired: 1}, ...}}
```

## Notes

- Single process, single node. All tenant queues live in one GenServer's state, so it is a serialisation point and there is no clustering or shared state across BEAM nodes. Fine for a gateway tier fronting an upstream that is itself the bottleneck, not a fit for tens of thousands of grants per second.
- `clamp/2` only applies the upper cap. It is `min(value, cap)` with no floor, so a large underestimate settled through `settle/5` can push `budget` negative. That is deliberate in effect, the bucket has to refill past the debt before anything is granted again, but it means `budget` in `stats/1` can read below zero.
- `settle/5` ignores the ticket argument entirely. It is a cast that adjusts only the global budget, and the tenant's deficit is not corrected, so per-tenant accounting is based on estimates while the shared bucket is based on truth.
- Nothing enforces that a granted caller actually calls the upstream, or calls it once. A caller that drops a grant on the floor without settling leaves the estimated cost charged against the bucket until the next tick tops it back up. There is no lease or revocation.
- Grants are delivered by `send/2` to the pid that called `enqueue/4`, with no monitoring of that pid. If the caller dies between enqueue and grant the message goes nowhere and the estimated cost is still charged.
- `drop_expired/1` rebuilds the entire tenants map every tick with `Map.new`, and `do_round/3` uses `Enum.at/2` on the order list. Both are linear in tenant count per tick. Comfortable for tens of tenants, not thousands.
- Tenants can be registered but never removed. `register_tenant/3` is idempotent and preserves an existing queue, depth and deficit, but there is no deregister call and the `order` list only grows.
- Priority is a two-level class, `:interactive` or `:batch`, hardcoded in `@priority_multiplier` as 2 and 1. Anything finer is expressed through each tenant's `quantum`.
