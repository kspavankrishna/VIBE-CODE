# Agent Aware Connection Pool

One AI agent fans out fifty parallel queries, drains every slot in your `database/sql` pool and every other agent sharing that database queues behind it. This is a fair queuing admission layer for Go that hands out connection slots per agent instead of first come first served, and resizes the pool itself from measured latency.

**Language:** Go | **Lines:** 719 | **Added:** 2026-08-29

## What this solves

This solves the connection storm problem that shows up the moment you put more than one AI agent in front of the same database. Once several agents, or several instances of one agent, or parallel tool calls inside one agent hit Postgres or MySQL at once, a plain `database/sql` pool with a fixed `MaxOpenConns` leaves you two bad options. Cap it, and everyone queues first come first served, so one noisy agent starves the rest. Leave it uncapped, and a single burst takes the database down.

The failure shape is always the same. One workflow fans out, the pool empties, every other agent's request queues behind that fan out, p99 latency spikes across every caller at once and nobody can tell from the metrics which caller caused it. Your dashboards show a slow database. They do not show that agent-research-7 is holding eleven of your twelve slots in a retry loop, so the incident is twenty minutes old before anyone finds it and the usual fix is a restart.

The other half is the pool size. `MaxOpenConns` is a number somebody picked once, during a load test that did not look like production. Too small and you queue under normal traffic. Too large and an agent burst pushes the database past what it can serve: saturation, then timeouts, then retries, then a worse burst. One number cannot be right for both the quiet case and the storm, and neither setting stops a broken agent retrying into failures and eating slots a healthy agent needed.

## Why I built it

Fixed pool sizes and plain semaphores have no concept of "which agent" or "how much load can this database take right now", so they cannot fix either half. pgbouncer and driver level pools treat every caller identically because they were built for request handlers of roughly uniform cost, not autonomous agents that decide on their own to issue fifty queries at once.

The fixes already exist, just not in backend code. Routers have solved weighted sharing between competing flows for decades, and TCP solved "how hard can I push before I hurt the thing on the other end" even earlier. This file borrows both and puts them in front of a `*sql.DB`, using nothing but the Go standard library.

## When to use it

- Two or more independent AI agents or agent sessions issuing SQL against one shared database, where a runaway agent must not starve the others.
- A multi tenant service where one heavy tenant's batch job keeps pushing interactive tenants past their latency budget.
- Background workers sharing a pool with user facing traffic, where the user facing callers should carry a higher weight.
- A database whose safe concurrency limit you do not know, so the pool should find it by measurement instead of a number in a config file.
- Any pool where you need per caller attribution: which agent holds slots, which is queued, which has been fenced off.

## How it works

Admission uses Weighted Fair Queuing, the virtual time scheduling routers use to share bandwidth between flows. Every `Acquire` computes a virtual finish time: `vStart` is `max(p.globalVClock, st.lastVFinish)` for that agent, and `vFinish` is `vStart + cost/weight` with `cost` fixed at 1.0. The waiter goes into a `waiterHeap`, a `container/heap` min heap ordered by `vFinish` with a monotonic `seq` counter as tie breaker, so equal finish times stay FIFO. Each `waiter` carries its own `index`, maintained by `heap.Interface`, so a cancelled context removes that waiter in O(log n) instead of scanning the queue.

The weight division is the whole point. An agent that floods the pool pushes its own `lastVFinish` far into the future, so its next request sorts behind requests from quieter agents. It crowds out itself, not everyone else. A weight of 2 advances virtual time at half the rate per request, roughly twice the admission share under contention. `promoteLocked` pops waiters in virtual finish order while `activeSlots < maxSlots`, advances `globalVClock` to the admitted waiter's `vFinish` and closes that waiter's `ready` channel. When the heap drains completely it resets `globalVClock` and every `lastVFinish` to zero, so a long lived pool does not accumulate float64 drift across idle periods.

The live `MaxOpenConns` value is not fixed either. `controlLoop` ticks at `ControlInterval` and calls `adjust`, an AIMD controller: additive increase, multiplicative decrease, the congestion control that makes TCP stable. Every `Lease.Release` folds the lease hold time into `latencyEWMA`, smoothed by `EWMAAlpha`. If that EWMA exceeds `TargetLatency` the controller multiplies `maxSlots` by `AIMDDecreaseFactor`, floored at `MinOpen`. If there is real queue depth and the EWMA sits below 80 percent of `TargetLatency`, it adds `AIMDIncrease`, capped at `MaxOpenCap`. Growth is gentle, retreat is fast. On any change it calls `db.SetMaxOpenConns` and re-runs `promoteLocked`, so a resize lands immediately rather than at the next release.

Failure containment is a circuit breaker held per agent inside `agentState`. `Lease.MarkFailed` flags the work as failed and `release` increments `consecutiveFail`. At `BreakerFailureThreshold`, `breakerOpenUntil` is set to now plus `BreakerCooldown`, the counter resets, and any `Acquire` inside that window returns `ErrAgentCircuitOpen` immediately. Other agents are untouched, because the breaker lives on the agent record, not the pool.

Two details decide whether slot accounting can drift. `Lease.Release` is guarded by a `CompareAndSwapInt32` on `released`, so a double release is a no op rather than a corrupted count. And `Acquire` handles the race where a context expires at the same instant promotion happens: if the waiter is still in the heap it is removed, otherwise the code drains the closed `ready` channel, builds the lease and releases it straight back so the slot is not leaked. `Snapshot` and `PrometheusText` expose the rest: pool budget, active slots, queue depth, latency EWMA and per agent state, with no client library.

## Usage

```go
db, err := sql.Open("pgx", dsn)
if err != nil {
    log.Fatal(err)
}
defer db.Close() // AgentPool never closes the DB, you own it

cfg := DefaultConfig() // MinOpen 4, MaxOpenCap 64, TargetLatency 120ms
cfg.MaxOpenCap = 96
cfg.TargetLatency = 80 * time.Millisecond
cfg.BreakerFailureThreshold = 3

pool := NewAgentPool(db, cfg)
defer pool.Close()

// Simplest path: replace db.QueryContext with pool.QueryContext and pass
// the caller's agent ID plus a weight (0 uses cfg.DefaultWeight).
rows, err := pool.QueryContext(ctx, "agent-research-7", 1,
    "SELECT id, body FROM docs WHERE tenant = $1", tenantID)
if err != nil {
    if errors.Is(err, ErrAgentCircuitOpen) {
        // this agent is fenced off for BreakerCooldown, back off
    }
    return err
}
defer rows.Close()

_, err = pool.ExecContext(ctx, "agent-writer-2", 2,
    "UPDATE docs SET summary = $1 WHERE id = $2", summary, id)

// Manual lease when the slot must be held across a transaction.
lease, err := pool.Acquire(ctx, "agent-writer-2", 2)
if err != nil {
    return err
}
defer lease.Release()

tx, err := db.BeginTx(ctx, nil)
if err != nil {
    lease.MarkFailed() // counts toward this agent's breaker
    return err
}

// Metrics, no Prometheus client library needed.
http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
    io.WriteString(w, pool.PrometheusText())
})

s := pool.Snapshot()
log.Printf("max=%d active=%d queued=%d ewma=%s",
    s.MaxOpen, s.ActiveSlots, s.QueueDepth, s.LatencyEWMA)
```

## Notes

- The file declares `package main` but has no `main` function and no CLI. It is a component, not a program: rename the package or paste it into your own. Standard library only, but you still supply the `database/sql` driver.
- The latency signal is lease hold time, from `acquiredAt` to `Release`, not query execution time. Hold a lease across a long transaction and the controller reads that as database slowness and shrinks the pool.
- `QueryContext` and `ExecContext` release the slot before returning, so admission covers issuing the statement, not the lifetime of the returned `*sql.Rows`. `QueryRowContext` cannot detect failure at all, since `*sql.Row` defers its error to `Scan`, so it never feeds the breaker.
- The `agents` map is never evicted. One entry per unique agent ID lives for the life of the pool, so per request agent IDs will grow memory. Use stable identifiers.
- `Close` stops the control loop and fails every queued waiter with `ErrPoolClosed`. It does not close the underlying `*sql.DB`, and it does not wait for in flight leases to drain.
- `MarkFailed` is the caller's judgement call: mark timeouts, resets and database side errors, not a syntax error from bad input. The pool starts at `MinOpen` and grows only when there is real queue depth, so a workload that never queues never leaves the floor. No tests ship with this file.
