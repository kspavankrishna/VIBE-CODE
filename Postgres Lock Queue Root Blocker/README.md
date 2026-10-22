# Postgres Lock Queue Root Blocker

A read only SQL toolkit that finds the one session behind a Postgres lock pileup and the one statement that makes the pileup worse. It tells you who to cancel and who to terminate, and why, without ever running either.

**Language:** SQL | **Lines:** 237 | **Added:** 2026-10-22

## What this solves

You have seen this outage. Someone runs a harmless looking `ALTER TABLE orders ADD COLUMN` during a quiet moment. Ten seconds later the API is timing out, the connection pool is full and every dashboard is red. The migration is not slow. It is waiting.

Here is what actually happened. Somewhere there is a session that did a `SELECT` inside a transaction and never committed. It holds an `AccessShareLock` on the table, which is harmless on its own. Then your `ALTER TABLE` arrived and asked for an `AccessExclusiveLock`. It cannot get it while the idle session holds the table, so it waits. Postgres lock queues are fair, so every new `SELECT` that arrives after the `ALTER` now queues behind it, even though a plain `SELECT` would never conflict with the idle session. One forgotten transaction plus one DDL statement and the whole table is frozen.

Searching for this on a live system is painful. `pg_stat_activity` shows you fifty sessions with `wait_event_type = Lock`. `pg_locks` shows you hundreds of rows. `pg_blocking_pids()` gives you an array per session, but the array lists the idle session and the queued DDL side by side, and nothing tells you which of those two you should deal with first. The usual copy paste lock query from a wiki prints a flat list of blocked and blocking pairs and leaves the thinking to you at 3am.

`PostgresLockQueueRootBlocker.sql` does the thinking. It builds the wait graph from `pg_blocking_pids`, walks it to the end and returns a short ranked list of things worth acting on:

- **root**: a session that blocks others and is not waiting on anyone. This is the real cause.
- **amplifier**: a waiting statement that other sessions are queued behind. This is usually the DDL, and cancelling it is often the fastest way to get traffic moving again.
- **cycle**: sessions that wait on each other in a loop.

Each row comes with a victim count, the longest wait, the locks involved, a plain advice sentence and the exact `pg_cancel_backend` or `pg_terminate_backend` call you would run. The script never runs it for you.

## Why I built it

I kept writing this query from memory in the middle of incidents and getting it slightly wrong each time. The first version listed blocker and blocked pairs. That was fine with two sessions and useless with eighty. The second version found the root with a recursive query and was happy right up to the moment the graph had a loop in it, at which point it ran forever.

Three things made me want a version I could keep in a repo.

First, the root is not always the right thing to kill. If the root is a 20 minute reporting query, terminating it throws away real work. If the amplifier is a migration that can be retried, cancelling that one releases every queued reader instantly and costs nothing. The metric that matters is how many sessions get released by each action, so the output has a `sole_blocked` column: the number of sessions whose only blocker is this pid. If you cancel an amplifier with `sole_blocked = 40`, forty sessions run immediately.

Second, there are blockers that are not sessions. A prepared transaction that was never committed holds its locks forever, survives restarts and has no backend pid. `pg_blocking_pids` reports it as pid `0`. Most lock queries drop it silently because the join to `pg_stat_activity` finds nothing. That is exactly the case where somebody spends two hours looking for a session that does not exist.

Third, killing the wrong backend is a worse incident than the first one. Anti wraparound autovacuum must not be cancelled. Cancelling an idle session does nothing at all, you need terminate. So the advice is chosen from the state of the blocker, and the script prints SQL for you to read and run yourself. I did not want a function that can end sessions installed on a production database.

## When to use it

- Your app is timing out and `pg_stat_activity` is full of `Lock` waits.
- A migration appears hung and you need to know whether to kill the migration or the thing it waits for.
- You want a dashboard or alert query for lock pileups that is cheaper and clearer than a flat blocked and blocking list.
- You review migrations and want to know which timeouts the server lacks before the next deploy.

It is a diagnostic. It does not replace setting `lock_timeout` on migrations, and it will not help with row level contention that resolves by itself in milliseconds. It also only sees locks that currently exist, so run it while the problem is happening.

Requirements: PostgreSQL 14 or newer, because it reads `pg_locks.waitstart`. Any role can run it. Without `pg_read_all_stats` or superuser, the `query` and `state` columns are hidden for sessions owned by other roles, which limits the advice but not the graph.

## How it works

Everything lives in a schema called `lock_queue`. Installing it is one file. The main function is `lock_queue.triage(warn_victims, crit_victims, crit_wait)`, written as a single SQL statement with these steps.

1. **act**: a materialized snapshot of `pg_stat_activity` that leaves out the session running the query. Reading the view once matters because the view and `pg_locks` are not read atomically, and the code has to survive a pid appearing in one and not the other.
2. **blocked**: calls `pg_blocking_pids` only for sessions whose `wait_event_type` is `Lock`. The function takes a lock manager partition lock, so I avoid calling it for every backend on a busy server.
3. **pending**: one ungranted `pg_locks` row per waiting pid, with a readable target built in a `CASE`. It handles relation, tuple, transaction id and virtual transaction waits and falls back to the lock type for the rest.
4. **edges**: one row per blocker and waiter pair. The `is_queue` flag marks the soft edges: the blocker is itself waiting for the same lock object as the waiter, which means the waiter is stuck behind it in the queue, not behind a lock holder. The comparison uses a row constructor with `IS NOT DISTINCT FROM` over every `pg_locks` identity column, so null columns do not break it.
5. **reach**: a recursive CTE for the transitive closure. It uses `UNION` and tracks only the source and destination, so the working set is bounded by the number of distinct pairs. That is what makes cycles safe: a loop just stops producing new rows. A node that can reach itself is in a cycle.
6. **node**: the counts per pid. `direct_waiters`, `queue_out`, `total_victims`, `sole_blocked` (waiters where the blocker array equals just this pid) and `longest_wait`, taken from `now() - waitstart` of the victims.
7. **prepared**: a one row summary of `pg_prepared_xacts` for the current database, used when the blocker pid is `0`.
8. **detail** and **classified**: attach the activity data, the requested lock and up to four held table locks. Held locks are ranked strongest first with `array_position` over the mode names, so an `AccessExclusiveLock` is never hidden behind a pile of `RowExclusiveLock` entries.

The `kind` is chosen in this order: `cycle`, then `amplifier` (a waiter with `queue_out > 0`), then `root` (not waiting, with victims). Severity is `critical` when `total_victims` reaches `crit_victims` (default 25) or `longest_victim_wait` reaches `crit_wait` (default 30 seconds), `warning` at `warn_victims` (default 5) and `info` otherwise. Rows sort by severity, then victims, then wait.

The advice and the two SQL columns come from a `CASE` over the real situation:

- Idle in transaction roots get `pg_terminate_backend`, with a note that cancel does nothing for an idle session.
- Active roots get `pg_cancel_backend` first and terminate as the fallback.
- An autovacuum worker whose query contains `to prevent wraparound` gets no SQL at all and a warning not to touch it. Ordinary autovacuum gets cancel.
- Pid `0` gets the candidate gids from `pg_prepared_xacts` and, when exactly one exists, a `ROLLBACK PREPARED` line built with `format('%L', gid)` so odd characters in a gid stay quoted.
- Amplifiers get cancel first and say how many sessions would run at once.
- Cycles get advice only. The deadlock detector owns them.

The second function, `lock_queue.guardrails()`, reads `pg_settings` and flags the settings that let a pileup happen: no `lock_timeout`, no `idle_in_transaction_session_timeout`, no `transaction_timeout` on versions that have it, `log_lock_waits` off, a `deadlock_timeout` above five seconds and an unbounded `statement_timeout`. Each row has a verdict and one sentence of advice.

Limits worth knowing. The closure is quadratic in the worst case, which is fine for hundreds of waiting sessions and slow for tens of thousands. Wait start is the time the lock request began, so it is the real lock wait and not the query age. A pid of `0` collapses every prepared transaction into one row, which is why the output lists candidate gids. The snapshot is not transactionally consistent, so a session that finishes mid read can show as `gone`. Run it again.

## Usage

Install once per database, with any role that can create a schema:

```sh
psql "$DATABASE_URL" -f PostgresLockQueueRootBlocker.sql
```

Run the triage during an incident:

```sql
SELECT severity, kind, pid, state, total_victims, sole_blocked, longest_victim_wait,
       advice, suggested_sql, fallback_sql
FROM lock_queue.triage();
```

Lower the thresholds on a small system:

```sql
SELECT * FROM lock_queue.triage(warn_victims => 2, crit_victims => 10, crit_wait => interval '10 seconds');
```

Check the server settings:

```sql
SELECT * FROM lock_queue.guardrails();
```

A typical result for the opening example shows two rows. The first is the root, an idle in transaction session holding `AccessShareLock on orders`, with `suggested_sql` set to `SELECT pg_terminate_backend(<pid>);`. The second is the amplifier, the `ALTER TABLE` waiting for `AccessExclusiveLock on orders`, with `sole_blocked` equal to the number of stuck readers. If the migration can be retried, run its cancel line first and the readers recover at once. Then fix the idle session at your own pace.

To reproduce it locally, open one session with `BEGIN; SELECT * FROM t;`, run `ALTER TABLE t ADD COLUMN c int;` in a second one, run a few `SELECT * FROM t;` in more sessions and call `lock_queue.triage()` from another.

## Notes

- Nothing is executed. The `suggested_sql` and `fallback_sql` columns are text. Read them before you run them.
- The function is volatile and takes no locks beyond what the catalog reads need, so it is safe to run while the system is on fire.
- Run it as a role with `pg_read_all_stats` if you want query text for every session.
- Prepared transactions in other databases of the same cluster will show as pid `0` with a message to look elsewhere. `pg_prepared_xacts` is filtered to the current database.
- Cross database waits show the relation as a bare oid when the table is not in the database you are connected to.
- I tested it on PostgreSQL 16 against four scenarios: an idle session with a queued `ALTER TABLE` and three readers, a prepared transaction with a waiting reader, a real two session deadlock cycle held open by a large `deadlock_timeout`, and the guardrails output on a default install.
- The best fix is still prevention. Run migrations with `SET lock_timeout = '3s'` and a retry loop, and set `idle_in_transaction_session_timeout` for application roles. This script is for the day that did not happen.

Keywords: Postgres lock queue, PostgreSQL blocking queries, pg_blocking_pids, pg_locks, ALTER TABLE hangs, idle in transaction, lock_timeout, prepared transaction blocking, DDL lock queue, find root blocker.
