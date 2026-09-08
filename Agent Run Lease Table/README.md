# Agent Run Lease Table

Two copies of the same background agent wake up on the same job and both start working. This is a single file Rust lease table with TTL leases, heartbeat renewal and monotonic fencing tokens that stops that, backed by nothing but a file on disk.

**Language:** Rust | **Lines:** 1425 | **Added:** 2026-05-15

## What this solves

Duplicate execution. You have a queue consumer, an agent orchestrator or a cron sidecar on cheap ephemeral infrastructure. The container gets OOM killed halfway through a run, the scheduler restarts it, and the new worker picks up a job the old process is still holding. Now two workers call the same paid model endpoint, write the same blob and fire the same webhook. The retry was supposed to be cheap. Instead you pay twice and hear about it from a billing alert.

The naive fixes fail in specific ways. A plain lock file with no expiry deadlocks the moment a worker dies without cleaning up, so somebody SSHes in at 2am to delete a stale `.lock` by hand. A TTL only lock fixes the deadlock and introduces a worse bug: the lease expires while the original process is still alive, stuck in a long GC pause or a slow network call. The new owner starts. Then the old process wakes up, finishes its write and clobbers fresher data. The lock was correct. The write ordering was not.

That is what the fencing token is for. Every acquire on a key bumps a counter that only goes up, and it survives release, expiry and sweep. A worker carries its fence into every downstream write and storage rejects anything below the highest fence it has seen, so a zombie can come back at any point and never look current again. The other half is the state file: rewrite it in place and a power cut mid write leaves a truncated line, so the next reader either errors out or silently loses every lease after the tear. Every mutation here goes temp file, fsync, rename.

## Why I built it

Running Redis, Postgres advisory locks or etcd to coordinate a handful of agent runs on one machine is a lot of moving parts for the problem. You inherit a network dependency and an ops surface larger than the thing you were making reliable. The file locking crates go the other way: an advisory `flock` and nothing else, no TTL, no ownership identity, no heartbeat, no fencing number.

The gap is the middle. One machine, one volume, and a real need for safe lease semantics now. So this is one auditable file, standard library only, with the protocol explicit enough that you can read all of it and decide whether you trust it.

## When to use it

- A queue consumer running two replicas on one volume where processing a message twice is worse than processing it late.
- An AI agent runner that must not fire the same expensive model call or tool side effect twice after a restart.
- A CI helper serializing deploys or migrations across concurrent workflow runs on a shared runner.
- A cron job that occasionally overruns its interval and would otherwise overlap with its own next invocation.

## How it works

The public type is `AgentRunLeaseTable`, built with `new(path)` or `with_config(path, LeaseTableConfig)`. It holds the table path and a sibling lock file named `.{filename}.lock`. `LeaseTableConfig` states every limit as a field: `lock_retry_interval_ms` 25, `lock_timeout_ms` 5000, `stale_lock_age_ms` 30000, `max_records` 100000, byte caps for key, owner, metadata and line, and a `sync_writes` flag.

Mutual exclusion comes from `TableLock::acquire`, which opens the lock path with `create_new(true)`, the O_EXCL create the OS guarantees to be atomic, and writes the pid and a timestamp into it. If it already exists, `stale_lock_should_be_reclaimed` compares the lock mtime against `stale_lock_age_ms` and deletes it when a crashed holder left it behind, then the loop retries until `lock_timeout_ms` elapses and returns `LockTimeout`. The guard is RAII: `impl Drop for TableLock` removes the file, so a panic inside the critical section still releases the lock.

Every mutating call funnels through `with_locked_state`: take the lock, `load_state` parses the whole table, a closure gets `&mut TableState` and returns a `Mutation { value, changed }`, and only a true `changed` triggers `persist_state`. Read, modify, write the whole file under a lock is the entire concurrency model. `TableState` wraps a `BTreeMap<String, EntryState>` so serialization order is deterministic, and `EntryState` is `last_fence` plus an optional active `LeaseRecord`.

`acquire_at` returns `AcquireOutcome::HeldByOther` when an active lease has not expired, otherwise it moves the old record into `replaced_expired`, does a `checked_add(1)` on `last_fence` and writes a fresh `LeaseRecord`. `renew_at` demands a matching owner and fence, returning `Missing`, `NotOwner`, `Expired` or `Renewed`: renewal extends the deadline and never bumps the fence, which is what makes a heartbeat safe. `release` checks owner and fence, clears the active record and deliberately leaves `last_fence` intact. `sweep_expired_at` clears expired entries and returns what it removed. All four have a `_now` wrapper and an explicit `now_ms` form, so the eight unit tests drive the clock instead of sleeping.

On disk it is a header line, `# AgentRunLeaseTable v1`, then one tab separated row per key with seven fields: key, last_fence, owner, acquired_at_ms, renewed_at_ms, expires_at_ms and metadata. `escape_field` and `unescape_field` cover backslash, tab, newline and carriage return so a value can never invent a column or a row. `load_state` is strict on purpose: wrong header, wrong field count, duplicate key, bad escape, oversized line or an inactive row with non zero timestamps all raise `LeaseErrorKind::CorruptState` naming the line. `persist_state` writes `.{name}.tmp-{pid}-{nanos}`, fsyncs, removes the target first on Windows because rename there will not clobber, then renames. `run_cli` prints hand rolled JSON, so no serde.

## Usage

```bash
# std only, no Cargo.toml needed
rustc -O AgentRunLeaseTable.rs -o agent-run-lease-table
rustc --test AgentRunLeaseTable.rs -o lease-tests && ./lease-tests

# claim a job for 5 minutes, with optional trailing metadata
./agent-run-lease-table acquire /srv/state/agents.lease job:42 worker-a 5m kind=ingest
# {"kind":"acquired","lease":{"key":"job:42","owner":"worker-a","fence":1, ...},"replaced_expired":null}

# heartbeat: owner and fence must both match, extends the deadline, fence stays put
./agent-run-lease-table renew /srv/state/agents.lease job:42 worker-a 1 5m phase=stream

# hand it back, last_fence is preserved so the next owner gets fence 2
./agent-run-lease-table release /srv/state/agents.lease job:42 worker-a 1

# whole table, or one key, then clear everything expired
./agent-run-lease-table inspect /srv/state/agents.lease
./agent-run-lease-table inspect /srv/state/agents.lease job:42
./agent-run-lease-table sweep /srv/state/agents.lease
```

Durations accept `ms`, `s`, `m`, `h` and `d` and may be compound, as in `1h30m`. In Rust, lift the type: `AgentRunLeaseTable::new(path).acquire_now(key, owner, ttl_ms, metadata)` returns `AcquireOutcome`.

## Notes

- Correctness rests on the filesystem making `create_new` atomic. That holds on a local disk or a single volume, not over NFS. This is not a cluster coordinator.
- Expiry uses the wall clock through `SystemTime::now`, so workers sharing a table need agreeing clocks.
- Every mutation rewrites the whole file, so cost is linear in key count. A new key past `max_records` is refused with `InvalidArgument`. Built for thousands of keys, not millions.
- `snapshot_at` and `inspect` read without the lock. The atomic rename means you get a complete old or new table, but the Windows remove then rename step leaves a brief window where the file is absent, which a reader sees as an empty table.
- A corrupt table is a hard error, never silently repaired: `CorruptState` names the offending line. A missing or empty file is treated as an empty table.
- The CLI exits 2 on error and 0 otherwise. `held_by_other`, `not_owner`, `expired` and `missing` are successful exits, so shell callers parse the `kind` field, not the exit code.
- Locking blocks via `thread::sleep`, there is no async support, and nothing here runs a heartbeat for you.
