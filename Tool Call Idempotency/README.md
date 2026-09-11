# Tool Call Idempotency

An LLM retries a tool call, a webhook fires twice and an Oban job replays, and the same side effect runs three times. This is an Elixir GenServer that gives one caller a lease on an idempotency key, makes everyone else wait on that same key, and serves the result from ETS to callers who show up later.

**Language:** Elixir | **Lines:** 717 | **Added:** 2026-04-16

## What this solves

Duplicate execution is the default failure mode of anything that talks to a model provider or an external API. A model decides its tool call timed out and issues it again. A webhook sender gets no 200 inside its window and redelivers. A user's browser reconnects and the LiveView remounts, replaying the last command. An Oban worker crashes after the HTTP request landed but before the job was acked, so the job runs again on the next pass. Each of these paths is individually reasonable. Together they mean the same `create_ticket` or `send_email` or `trigger_deploy` runs more than once.

What breaks is not subtle. Two emails to the same customer. Two Stripe charges. Two Jira tickets that a human now has to merge. Two deploys racing each other into the same environment. The customer notices before you do, because your logs show two successful calls and nothing looks wrong from the inside. If the duplicate hits a paid model API you also pay twice for tokens you already have.

The harder half of the problem is the concurrent case. Deduplication that only checks a cache is a read then write race: two processes both miss the cache at the same instant and both execute. You need the losing caller to block until the winner finishes, then receive the winner's result, not to run its own copy. That is what a lease is for, and it is the part most homegrown dedupe tables skip.

There is also the aftermath. A lease holder can die mid flight. When it does, everyone waiting on that key must be told, and the key must become claimable again rather than staying wedged forever. This module monitors the owner process and reopens the key when the owner goes down.

## Why I built it

Elixir has plenty of caching (Cachex, Nebulex, plain ETS) and plenty of job libraries with their own uniqueness checks, but those solve adjacent problems. A cache gives you memoization with no mutual exclusion, so concurrent misses both run. Oban's unique jobs work only for jobs that go through Oban, which does not help a synchronous tool call arriving over a Phoenix controller or a channel. Distributed lock libraries give you the mutex but nothing that hands the result back to the callers who lost the race.

What I wanted was one primitive that does all three at once: claim, wait and reuse. Keyed by an idempotency key, payload sensitive so a reused key with different arguments is an error rather than a silently wrong cache hit, and cheap enough to sit on a hot request path.

## When to use it

- An LLM agent loop where the model can reissue the same tool call after a timeout or a stream interruption
- Webhook receivers from Stripe, GitHub, Twilio or anything else that retries until it sees a 200
- A Phoenix controller or LiveView handler where a double click or a reconnect replays a mutating command
- Background workers that may run the same unit of work twice after a crash or a redeploy
- Any call that costs money or creates an external record: payments, emails, tickets, provisioning, deploys
- Fan out where several processes ask for the same expensive result at the same moment and only one should compute it

## How it works

State lives in a single GenServer. It owns two things: a map of active leases keyed by idempotency key, and an ETS table of completed outcomes. The ETS table is created in `init/1` as a `:set` with `read_concurrency` and `write_concurrency` set, and every row is `{key, kind, digest, value, expires_at_ms, seq}` where `kind` is `:ok` or `:error`.

Keys come in through `normalize_key/1`, which accepts a binary or iodata and raises `ArgumentError` on anything else or on an empty key. The payload is fingerprinted by `digest_payload/1`: `:erlang.term_to_binary/1` then SHA-256, hex encoded. That digest is stored on both the lease and the cache row. If a second caller arrives with the same key but a different digest, it gets `{:error, {:payload_conflict, key}}` instead of a stale result. This is the same guard Stripe applies to reused idempotency keys and it catches the real bug of a client generating one key for two different requests.

`claim/4` is the entry point. It sweeps expired leases for that one key, checks the cache via `cache_reply/4` and then branches. A live `:ok` row returns `{:cached, value}`. A live `:error` row returns `{:failed, reason}`. An expired row is deleted on read, which is a lazy expiry on top of the periodic sweep. On a miss, if no lease exists, `open_lease/5` mints a `%Lease{}` with a fresh `make_ref/0` id, the owner pid, a start time and a deadline of `now + lease_ttl_ms`, then calls `Process.monitor/1` on the owner and returns `{:execute, lease}`. If a lease already exists with a matching digest, the caller gets `{:busy, snapshot}` with the remaining milliseconds.

Waiting is the interesting part. `await/4` does not poll. On a busy key it stashes the caller's `from` tag in the lease holder's waiter list and returns `{:noreply, state}`, parking the caller inside its own `GenServer.call`. When the owner calls `finish/4` or `fail/4`, `notify_waiters/2` walks that list and calls `GenServer.reply/2` on each one, so every waiter is released in a single pass with the winner's result. The waiter list is bounded by `waiter_limit` (default 256) and callers past the limit get `{:error, {:too_many_waiters, limit}}` rather than being allowed to pile up without bound.

Ownership is enforced. `fetch_owned_lease/3` matches both the lease reference id and the calling pid, returning `:stale_lease` when the id no longer matches and `:not_owner` when another process presents a valid id. So a late `finish` from a process whose lease already expired cannot overwrite the result of the lease holder that replaced it. Three things can end a lease besides `finish`: `fail/4` records an error outcome, `release/3` gives the key up with a reason and no cached result, and `heartbeat/3` extends the deadline for work that legitimately runs longer than `lease_ttl_ms`. If the owner process dies first, the `{:DOWN, ...}` clause of `handle_info/2` pops the lease and replies `{:error, {:owner_down, reason}}` to everyone waiting.

Cache retention is TTL plus a bounded FIFO. Successful results live `completed_ttl_ms` (default 300000) and failures default to `failure_ttl_ms` of 0, which means failures are not cached at all unless you opt in. That default is deliberate: a retried call after a transient failure should actually retry. Insertion order is tracked by a monotonic `next_seq` counter pushed into an Erlang `:queue`, and `trim_cache/1` pops the oldest sequence numbers until the table is back under `max_completed` (default 10000), skipping tombstoned entries whose sequence no longer matches the live row. Because those skipped entries would otherwise make the queue grow without bound, `maybe_compact_order/1` rebuilds the queue from a fold over ETS when it exceeds `max(cache_entries * 4, limit * 2)`. A `:sweep` message every `sweep_interval_ms` (default 30000) expires leases, purges expired rows, trims and compacts, and `purge/1` forces the same pass on demand. All time is `System.monotonic_time(:millisecond)`, so a clock change cannot expire leases early.

`run/5` is the ergonomic wrapper that puts it together: claim, execute, finish on `{:ok, value}` or a bare value, fail on `{:error, reason}`, and on a busy key fall through to `await`. It wraps the function in `try/rescue/catch`, reports `{:exception, formatted}` or `{kind, formatted}` to the server so waiters are not stranded, then reraises with the original stacktrace preserved.

## Usage

```elixir
# Start it under your supervision tree. child_spec/1 is provided.
children = [
  {ToolCallIdempotency,
   name: ToolCallIdempotency,
   completed_ttl_ms: 300_000,
   failure_ttl_ms: 0,
   lease_ttl_ms: 120_000,
   waiter_limit: 256,
   max_completed: 10_000,
   sweep_interval_ms: 30_000}
]

# The common path: one call does claim, execute, wait and reuse.
ToolCallIdempotency.run("tool_call:" <> call_id, %{to: "a@b.com", body: body}, fn ->
  Mailer.send(to, body)
end, claim_timeout: 5_000, wait_timeout: 30_000, ttl_ms: 600_000)
#=> {:ok, result} | {:error, reason}

# Manual lease control when the work spans processes or needs heartbeats.
case ToolCallIdempotency.claim(ToolCallIdempotency, key, payload, timeout: 5_000) do
  {:execute, lease} ->
    ToolCallIdempotency.heartbeat(lease)              # extend the deadline
    ToolCallIdempotency.finish(lease, value)          # or fail/2, or release/2
  {:cached, value} -> {:ok, value}
  {:failed, reason} -> {:error, reason}
  {:busy, meta} -> ToolCallIdempotency.await(ToolCallIdempotency, key, payload, 30_000)
  {:error, {:payload_conflict, ^key}} -> :reject
end

# Non-blocking check and introspection.
ToolCallIdempotency.peek(key, payload)   #=> :miss | {:cached, v} | {:failed, r} | {:busy, meta}
ToolCallIdempotency.stats()              #=> %ToolCallIdempotency.Stats{}
ToolCallIdempotency.purge()              #=> :ok
```

## Notes

- Single node only. The lease map and the ETS table live in one process on one BEAM node, so two nodes behind a load balancer will each grant their own lease for the same key. For cluster wide idempotency you need Postgres, Redis or a `:global` registered singleton in front of this.
- The ETS table is `:protected` and its reference is never exposed, so every read goes through the GenServer. That serializes lookups on one process. It is fast, but it is a single mailbox and it is the thing to measure if the key space is hot.
- Results are in memory and do not survive a restart of the process. After a crash and supervisor restart every key is claimable again.
- `failure_ttl_ms` defaults to 0, so failures are not cached. Set it only when you want a failing key to stay failed for a window.
- A payload digest change on a live key is a hard `{:error, {:payload_conflict, key}}`, not a cache miss. That is intentional, but it means callers must generate a distinct key per distinct payload.
- `run/4` and the single argument helpers target the module's own name, so the server must be started as `name: ToolCallIdempotency` for them to resolve.
- Lease expiry is a deadline, not cancellation. If a lease times out the original work keeps running in its own process, and its later `finish` is rejected as `:stale_lease`. Use `heartbeat/2` for long jobs, or accept that a second executor may start.
- Invalid options raise `ArgumentError` at `init/1` or at call time rather than being coerced. No test file ships with this module.
