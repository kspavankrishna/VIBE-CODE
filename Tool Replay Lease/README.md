# Tool Replay Lease

A retried job, a reconnected model stream or a redelivered webhook can run the same side effect twice: the card gets charged twice, the cluster gets provisioned twice, the GitHub issue gets created twice. This is a single Elixir module that gives every side effecting call an idempotency key, a lease with a fencing token and a cached result, so the second attempt returns the first attempt's answer instead of repeating the work.

**Language:** Elixir | **Lines:** 1028 | **Added:** 2026-04-16

## What this solves

This solves duplicate Elixir AI tool execution, MCP replay bugs, webhook redelivery and job retry races where the same external side effect can run twice. The dangerous failure mode in real Phoenix, Oban, Broadway and GenServer systems is not a crash. It is "the request actually worked, but the worker did not know that and did it again."

Picture the concrete shape of it. An Oban worker posts a charge to a billing API. The HTTP call succeeds, the provider records the charge, then the worker's node is killed before the job is acknowledged. Oban is doing exactly what it is supposed to do when it redelivers the job, and the worker is doing exactly what it is supposed to do when it retries the charge. The customer is charged twice, and the person who notices is a customer, not an engineer. The same story replays with a different cast every time: a GitHub webhook delivery retried after a timeout opens a second pull request, a Broadway consumer reprocessing a batch sends the same email twice, an AI agent whose model stream reconnects re-emits an identical tool call and provisions a second cluster that nobody will ever look at until the bill arrives.

Retry logic alone cannot fix this because the ambiguity is real. When an HTTP call times out, the caller genuinely does not know whether the work happened. The only fix is an out of band record that says "this exact unit of work is in flight" or "this exact unit of work already finished, here is what it returned." That record has to survive the caller crashing, it has to expire so a dead worker does not block a key forever, and it has to stop a resurrected old worker from stomping on the worker that legitimately took over from it.

The usual answer is Redis with SETNX and a Lua script, or a database row with a unique index and a transaction. Both work, both add an external dependency, an operational surface and a network hop to something that in a BEAM system can live in memory in the same node that is already coordinating the work. This module is that in memory version, written to be dropped into an existing app with no new infrastructure.

## Why I built it

Every Elixir codebase I have seen that needed replay protection grew its own half version of it: an ETS table plus a `GenServer` plus a naive "is this key present" check, with no fencing, no completion cache and no visibility into what is currently held. The half version breaks at the interesting moment, which is takeover. A worker stalls past its lease, a second worker picks the key up, the first worker wakes up and writes its result over the second worker's state. Nothing in a plain presence check prevents that.

The other gap is observability. When you do finally get paged because a charge ran twice, you want to answer "who held this key, for how many attempts, and did it complete" from a running node. Most home rolled versions cannot answer any of that. This one keeps owner labels, attempt counts, fencing numbers, timestamps and a SHA-256 fingerprint of the cached result, and emits telemetry for every transition.

## When to use it

- An Oban or Broadway worker calls a billing, payments or provisioning API that must not run twice when the job is retried.
- An MCP server or agent runtime executes tool calls where a reconnected model stream can re-emit the same call id.
- A Phoenix webhook endpoint receives GitHub, Stripe or provider deliveries that are retried on timeout and carry a stable delivery id.
- A long running provisioning task needs to hold a key for minutes, with periodic heartbeats, and hand the key over cleanly if the worker dies.
- You want duplicate suppression on a single node and do not want to add Redis, Postgres advisory locks or another external lock service just for this.
- You need to see, from a running IEx session, which idempotency keys are in flight right now and which ones already completed.

## How it works

The module is one `GenServer` holding one ETS `:set` table. Every public call, `claim/3`, `renew/3`, `complete/4`, `release/3`, `status/2`, `forget/2`, `list/2` and `sweep/1`, is a `GenServer.call` with an `:infinity` timeout, so all state transitions are serialized through a single process and the compare and swap semantics come for free. The ETS table is `:protected` with read and write concurrency enabled and can be given a name through the `:table` option if you want to inspect it from outside.

Each key maps to a plain map with a `:state` field of either `:active` or `:completed`. `claim/3` looks the key up through `current_entry/3`, which lazily deletes rows that are past their expiry before deciding. A missing key produces a fresh active entry with `attempts: 1` and `fence: 1` and returns `{:ok, %Lease{}}`. A live active entry returns `{:busy, %ActiveStatus{}}`. A cached completion returns `{:completed, %CompletedStatus{}}` with the original result term attached, which is the whole point: the retry gets the first attempt's answer without touching the external system.

Takeover is where the fencing token earns its place. A `Lease` carries a `token`, 18 bytes from `:crypto.strong_rand_bytes/1` encoded with `Base.url_encode64/2`, and a monotonically increasing `fence` integer. When an active lease is past `expires_at_mono_ms` and `:allow_takeover?` is true, `claim/3` writes a replacement entry with a new token, `fence + 1` and `attempts + 1`, and emits a `:taken_over` event carrying the previous owner. Every mutating call then runs `same_lease?/2`, which compares key, token and fence together. The stale worker that wakes up late and calls `complete/4` gets `{:error, :stale_lease}`, so it cannot overwrite the newer owner's record or cache a result for work that has been redone. Set `allow_takeover?: false` if you would rather a stuck key stay stuck than risk a second execution.

Time handling uses two clocks on purpose. Expiry arithmetic runs on `System.monotonic_time(:millisecond)` so wall clock jumps and NTP corrections cannot make a lease expire early or late, while the timestamps reported in `ActiveStatus` and `CompletedStatus` come from `System.system_time(:millisecond)` so they line up with your logs. Defaults are a 30 second lease TTL, a 10 minute completion TTL, a 60 second cleanup interval and a 60 minute orphan TTL, all overridable per server and, for the two TTLs, per call. Completion TTL accepts `:infinity`.

Expiry alone does not invalidate a lease, and that distinction matters. `complete/4` and `release/3` use `raw_lookup/2` rather than the expiry aware path, so a worker that overruns its lease can still complete successfully as long as nobody actually took the key over. Only a real takeover, visible as a fence bump, produces `:stale_lease`. Cleanup runs on a `Process.send_after/3` timer into `handle_info(:cleanup, state)` and on demand through `sweep/1`. `sweep_entries/2` walks the table and deletes completed rows past their TTL plus active rows that are `orphaned?/3`, meaning expired and then expired again by a further `orphan_ttl_ms`, which is the grace window that lets a taken over key stay visible for debugging before it disappears.

On completion, `fingerprint_result/1` runs `:erlang.term_to_binary/1` over the result, hashes it with SHA-256 and stores the lowercase hex digest plus the byte size. If the term cannot be serialized the fingerprint is `nil` and nothing fails. Pass `store_result?: false` when the result is large or sensitive and you want the dedupe record and the fingerprint without keeping the payload in memory. Every transition, `:claimed`, `:taken_over`, `:renewed`, `:completed`, `:released`, `:forgotten` and `:swept`, goes through `emit/4`, which calls an optional `:notify` function or MFA inside a rescue and catch wrapper so a broken handler only logs a warning, and then emits a `:telemetry` event under `:telemetry_prefix` if and only if `:telemetry` is actually loaded. There are no dependencies in the file.

## Usage

```elixir
# In your supervision tree
children = [
  {ToolReplayLease,
   name: ToolReplayLease,
   table: :tool_replay_lease,
   lease_ttl_ms: 30_000,
   completion_ttl_ms: 10 * 60_000,
   orphan_ttl_ms: 60 * 60_000,
   telemetry_prefix: [:my_app, :replay]}
]

# The simple path: run a side effect at most once per key
case ToolReplayLease.run("oban:job:billing:98765", fn ->
       Billing.charge!(customer_id, amount)
     end, owner: "billing_worker", meta: %{customer: customer_id}) do
  {:ok, charge, _status} -> {:ok, charge}
  {:completed, charge, _status} -> {:ok, charge}
  {:busy, active} -> {:snooze, active.expires_in_ms}
end

# The manual path, when the work is long and needs heartbeats
case ToolReplayLease.claim("agent:provision_cluster:req_42",
       owner: node(), lease_ttl_ms: 60_000, allow_takeover?: true) do
  {:ok, lease} ->
    {:ok, lease} = ToolReplayLease.renew(lease, lease_ttl_ms: 120_000)
    result = Provisioner.create!(spec)
    {:ok, _completed} =
      ToolReplayLease.complete(lease, result,
        completion_ttl_ms: :infinity, store_result?: true)

  {:completed, done} -> done.result
  {:busy, active} -> {:error, {:in_flight, active.owner}}
end

# Retryable failure: hand the key back instead of caching a lie
ToolReplayLease.release(lease, reason: :upstream_timeout)

# Introspection from IEx
ToolReplayLease.status("oban:job:billing:98765")
ToolReplayLease.list(state: :active, limit: 50)
ToolReplayLease.sweep()
ToolReplayLease.forget("webhook:github:delivery_id")
```

## Notes

- State is in memory only. One `GenServer` and one ETS table on one node. If the process dies or the node restarts, every lease and cached result is gone, and a retry after that will re-execute. This is not a distributed lock and it is not durable, so do not use it as the only guard for money movement across a multi node cluster.
- Every operation is a `GenServer.call`, including `status/2` and `list/2`. The ETS table has read concurrency enabled but nothing reads it directly, so the single process is the throughput ceiling. `list/2` calls `sweep_entries/2` and then `:ets.tab2list/1`, which is O(n) over every row, so keep `:limit` sane and do not poll it from a hot path.
- `run/4` passes the same opts to both `claim/3` and `complete/4`, so `:meta`, `:completion_ttl_ms` and `:store_result?` apply to both. It does not renew for you. If the function outlives the lease TTL and another worker takes the key over, `complete/4` returns `{:error, :stale_lease}` after the side effect has already happened. For long work, claim manually and call `renew/3`, or set a lease TTL longer than your worst case.
- Only successful completions are cached. If the function raises or throws, `run/4` releases the lease and re-raises with the original stacktrace, so the next attempt is free to run. Failures are never deduplicated.
- `claim/3` defaults to `allow_takeover?: true`, which means a stalled worker that is still alive but past its TTL can have its key taken. That trades one risk for another. Pick the TTL deliberately.
- Invalid options raise `ArgumentError` at call time rather than returning an error tuple: empty keys, non positive TTLs, bad `:meta`, bad `:notify` and bad `:telemetry_prefix`. Keys may be binaries, atoms, integers or iodata and are trimmed to a binary.
- Completion results are stored as raw Erlang terms in ETS. A large result is held for the full completion TTL. Use `store_result?: false` when you only need the dedupe decision and the SHA-256 fingerprint.
