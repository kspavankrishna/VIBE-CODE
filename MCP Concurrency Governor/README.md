# MCP Concurrency Governor

One noisy agent session opens forty parallel browser or shell tool calls and every other user on the node waits behind it. This is an OTP GenServer that admits tool calls under per tool concurrency limits, queues the rest by session, and rotates across sessions in round robin order so no single caller can take the whole fleet.

**Language:** Elixir | **Lines:** 895 | **Added:** 2026-04-29

## What this solves

MCP concurrency collapse in Elixir systems where agents, jobs and live sessions all compete for the same expensive tools. In real systems that means browser workers, shell runners, search adapters, eval jobs, vector lookups, or any external step that is slower and scarcer than normal BEAM work. Without a governor, one chat or one tenant can quietly take every slot, create an unbounded wait queue, and make the whole agent stack feel random.

The failure mode is specific and it is not a crash. A single session loops a tool in a fan out, grabs all four headless browser slots, and holds them. Every other session's request now sits behind an unbounded list of that session's pending work. Latency for everyone else goes from two seconds to ninety. The provider side rate limiter starts returning 429s because your node is running more parallelism than the plan allows, so retries pile on top of the backlog. Nothing logs an error, because from the tool's point of view everything is working. The people who notice are the users on the other sessions, then support, then you, three hours later, reading a flat graph that says the tool is healthy.

The second failure mode is the stuck run. A tool call takes a slot, the calling process wedges on a socket read, and that slot is gone forever. With four slots on a hot tool it takes four wedged calls to put the tool permanently offline, and nothing in the state tells you which four.

This module handles both. Concurrency is capped per tool. Waiting is bounded by a queue limit and a queue deadline, so overload turns into a fast `{:error, :queue_full}` or a `:queue_timeout` rejection instead of silent unbounded growth. Every granted slot carries a run TTL, so a wedged call reclaims its own slot on the next sweep and reports itself as `:run_timeout`. Every transition is countable through `stats/1` and inspectable through `list/2`.

## Why I built it

The real 2026 failure mode is not simply "a tool crashed." It is "the tool kept working, but the scheduler around it was unfair, bursty and impossible to debug under load." A plain semaphore gives you a hard concurrency limit and nothing else. It has no notion of who is waiting, so the fastest looping client wins every free slot. It has no queue deadline, so a caller can block for minutes with no way to say when it should give up. It has no cancellation, so a client that walked away still holds its place. It has no stale run cleanup, so a leaked permit is permanent.

Poolboy and its relatives solve worker checkout, not admission control across many logical tools with different limits and different tenants. Oban rate limits at the queue level, not per tool per session. What was missing was a small auditable piece that sits between the agent and the tool, knows about sessions, and makes overload visible instead of destructive.

## When to use it

- A Phoenix or MCP server where several chat sessions share one headless browser pool and one session's fan out starves the others.
- An internal AI gateway fronting a provider with a hard parallel request cap, where you need to hold your own concurrency below the quota rather than absorb 429s.
- An eval or research platform running batch jobs alongside interactive users on the same shell or search adapter, where the batch must not eat every slot.
- A multi tenant tool broker where one tenant looping a tool should degrade only that tenant.
- Any tool call that can wedge, where you would rather reclaim the slot after two minutes than restart the node.
- Debugging a load incident where you need to see, right now, which sessions hold slots and how long the oldest waiter has been queued.

## How it works

The module is a single `GenServer` holding a `%State{}` whose `tools` field maps a tool name to one independent tool state map. Each tool state carries its `%ToolConfig{}` (`limit`, `queue_limit`, `queue_ttl_ms`, `run_ttl_ms`, `max_per_session`), an `inflight` map keyed by request id, an `inflight_by_session` counter map, a `queues` map of session id to an Erlang `:queue`, an `order` queue of session ids, a `queued_count`, and a `counters` map with `granted`, `queued`, `rejected`, `cancelled`, `expired` and `released`. Unknown tools are created on demand from `default_tool_config` when `auto_create_tools?` is true, the default.

The fairness trick is queueing by session instead of by raw request. Waiting requests go into a per session FIFO in `queues`, and the `order` queue holds one entry per session that has work pending. `pop_next_eligible/1` pops a session id off the front of `order`, takes that session's oldest request, and pushes the session to the back of `order` if it still has more queued. That is round robin across sessions with FIFO inside each session. The scan is bounded to `:queue.len(order)` attempts so a state where every waiting session is already at its `max_per_session` cap terminates instead of spinning. `session_at_limit?/2` enforces that cap, and it accepts `:infinity` to disable it.

Admission runs in `handle_call({:request, ...})`. `grant_immediately?/2` grants on the spot only when the tool's queue is completely empty, inflight is under `limit`, and the session is under its own cap. That empty queue condition stops a new arrival from jumping ahead of anyone already waiting. A full queue returns `{:error, :queue_full}` at once, otherwise the request is enqueued and the caller gets `{:queued, %Receipt{}}`. When a slot frees, `schedule_grants/3` loops granting until inflight reaches `limit` or no eligible waiter remains, and each grant is sent as a message tagged `:mcp_concurrency_governor` to the request's `notify_pid`, which defaults to the calling pid. `await/2` is the matching receive.

Grants are leases, not raw permits. `grant_now/4` mints a `grant_token` from 15 bytes of `:crypto.strong_rand_bytes/1` and returns a `%Lease{}`. `release/3` looks the request up in `inflight` and compares tokens, so a release that arrives after the lease already expired and was reissued returns `{:error, :stale_lease}` rather than freeing somebody else's slot. Deadlines are computed with `System.monotonic_time(:millisecond)` so clock changes cannot corrupt them, while the timestamps reported back to callers use `System.system_time(:millisecond)`.

Expiry is a sweep, not per request timers. `schedule_sweep/1` arms a `Process.send_after(self(), :sweep, sweep_interval_ms)` that defaults to one second, and `handle_info(:sweep, ...)` runs `reconcile_all/3` over every tool. Each tool goes through `expire_queued/2`, which rebuilds the per session queues while dropping requests past their queue deadline as `:queue_timeout`, then `expire_inflight/2`, which drops any lease past `expires_at_mono_ms` as `:run_timeout`, then `schedule_grants/3` to fill the freed slots. The same reconcile runs at the top of `request`, `release`, `stats`, `list` and the explicit `sweep/1`, so reads never show state a pending sweep would have cleaned up.

`run/5` is the convenience path: request, await with an optional `await_timeout_ms`, execute the zero arity function, release inside `try/rescue/catch` so a raising or throwing tool call still gives the slot back before it reraises. A caller timeout cancels the queued receipt with reason `:caller_timeout`. Observability is two optional sinks driven by `emit/4`: a `notify` handler, either a unary function or `{Module, :function, extra_args}`, wrapped in `safe_notify/1` so a broken handler logs a warning instead of taking down the governor, and `:telemetry` events under `telemetry_prefix ++ [event]`, emitted only if telemetry is loaded.

## Usage

```elixir
# In your supervision tree
children = [
  {McpConcurrencyGovernor,
   name: McpConcurrencyGovernor,
   tools: %{
     "browser" => [limit: 4, queue_limit: 64, queue_ttl_ms: 20_000, run_ttl_ms: 90_000],
     "shell"   => [limit: 2, max_per_session: 1],
     "search"  => [limit: 16, max_per_session: :infinity]
   },
   default_tool_options: [limit: 4, queue_limit: 128, run_ttl_ms: 120_000],
   auto_create_tools?: true,
   sweep_interval_ms: 1_000,
   telemetry_prefix: [:my_app, :tools],
   notify: fn {event, measurements, metadata} -> Logger.info("#{event} #{inspect(metadata)}") end}
]

# Simplest call: acquire, run, release, all handled
{:ok, html} =
  McpConcurrencyGovernor.run("session-abc", "browser", fn ->
    Browser.fetch("https://example.com")
  end, await_timeout_ms: 15_000, meta: %{url: "https://example.com"})

# Manual lease control
case McpConcurrencyGovernor.request("session-abc", "shell", owner: "worker-7") do
  {:ok, lease} ->
    do_work()
    McpConcurrencyGovernor.release(lease, reason: :ok)

  {:queued, receipt} ->
    case McpConcurrencyGovernor.await(receipt, 10_000) do
      {:ok, lease} -> do_work(); McpConcurrencyGovernor.release(lease)
      {:error, :timeout} -> McpConcurrencyGovernor.cancel(receipt, reason: :gave_up)
      {:error, reason} -> {:error, reason}   # :queue_timeout, :cancelled, ...
    end

  {:error, :queue_full} ->
    {:error, :busy}
end

# Operations
McpConcurrencyGovernor.stats()            # per tool limits, inflight, queued, oldest wait, counters
McpConcurrencyGovernor.list(:inflight)    # who holds slots right now, and for how long
McpConcurrencyGovernor.list(:queued)      # who is waiting, and how long until their deadline
McpConcurrencyGovernor.sweep()            # force expiry now, returns stats
```

## Notes

- Single node, single process. State lives in the GenServer heap with no ETS and no distribution, so it governs one BEAM node, and restarting the process drops all leases and queues. Every request, release, cancel, stats and list call serializes through that one process, which suits tool call rates but not per message hot paths.
- The governor does not monitor lease holders. If a caller dies without calling `release/3` the slot comes back only when `run_ttl_ms` elapses on the next sweep. Set `run_ttl_ms` to a stall you can actually tolerate.
- `cancel/3` scans every tool's queues linearly for the request id, and `expire_queued/2` rebuilds all queues on each sweep tick. Both are O(queued), fine at the default 128 entry queue limit and worth measuring if you raise it a lot.
- `release/3` on an already expired lease returns `{:error, :stale_lease}`, on an unknown request id `{:error, :not_found}`. Cancelling something already granted returns `{:error, :not_queued}`.
- `await/2` only works in the process registered as `notify_pid`, which defaults to the caller of `request/4`. Hand a receipt to another process and you must pass `notify_pid:` explicitly.
- Config is validated at call time and raises `ArgumentError` rather than degrading quietly. `limit`, `queue_ttl_ms`, `run_ttl_ms` and `sweep_interval_ms` must be positive integers, `queue_limit` may be zero to disable queueing, and `max_per_session` accepts `:infinity`. Telemetry is detected at runtime, so the only hard dependencies are OTP and `Logger`.
