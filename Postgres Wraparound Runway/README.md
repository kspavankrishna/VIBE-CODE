# Postgres Wraparound Runway

Tells you how many hours or days your PostgreSQL cluster has before transaction ID or multixact wraparound forces it to stop taking writes, which tables and sessions are dragging it there and whether autovacuum can freeze them in time.

**Language:** SQL | **Lines:** 1170 | **Added:** 2026-11-19

## What this solves

Every PostgreSQL write gets a 32 bit transaction ID. VACUUM has to freeze old rows before the counter laps them. When it cannot keep up the server first warns, then trips the vacuum failsafe and finally refuses to assign new transaction IDs at all. At that point the database is down for writes until someone runs a long freeze by hand. Multixact IDs, the ones created by `SELECT ... FOR SHARE` and foreign key checks, have the same limit and the same failure.

The usual monitoring query is `SELECT datname, age(datfrozenxid) FROM pg_database` with an alert at some fixed number. That answers the wrong question. An age of 900 million is fine on a cluster that burns 50 transaction IDs a second and an emergency on one that burns 40,000. It also says nothing about why the age is high, which table is responsible, whether the vacuum that should fix it can even succeed or whether thirty tables loaded on the same night are about to need anti wraparound vacuums in the same hour.

Postgres Wraparound Runway is a single SQL file that installs a `wraparound_runway` schema. It measures how fast your cluster consumes transaction IDs and multixact IDs, turns every age into a time, finds the things that pin the freeze horizon and prices the vacuum work against the autovacuum cost limit. The output is a ranked list of findings with the SQL to fix each one, plus a 0, 1 or 2 check for your alerting.

## Why I built it

I have seen the same Postgres wraparound incident shape more than once. Someone opens a transaction in a notebook or leaves a two phase commit prepared after a crashed coordinator. A logical replication slot loses its consumer. Autovacuum keeps launching anti wraparound vacuums, each one scans the table, finishes and the age does not move, because VACUUM cannot freeze anything newer than the oldest running snapshot. The dashboards show `age` climbing in a straight line and the alert fires at a threshold that was picked years ago, often with less time left than the manual freeze needs.

What I wanted was a tool that thinks in time instead of raw numbers, runs inside the database with no agent to deploy and never makes things worse. Monitoring code that takes a relation lock during an incident can queue behind the exact `ALTER TABLE` that caused it. So this reads catalogs and statistics views only. The one optional exact measurement uses a 200 millisecond lock timeout per table.

## When to use it

Install it on any PostgreSQL 14 or newer cluster that handles real write traffic. It is most useful in these situations:

1. High write rate systems such as queues, event stores, ledgers, IoT ingestion and anything doing many small transactions, where the runway is days rather than years.
2. Clusters with logical replication, CDC connectors like Debezium or two phase commit, where a forgotten slot or prepared transaction can pin the horizon silently.
3. Large insert mostly tables, partitioned or not, that were bulk loaded together and will all cross `autovacuum_freeze_max_age` together.
4. Incident response, when `age(datfrozenxid)` is already alarming and you need to know what to vacuum first and whether it will finish before the stop limit.
5. Capacity reviews, to decide whether `autovacuum_vacuum_cost_limit` and `autovacuum_max_workers` are set high enough for the freeze debt you actually carry.

It runs on managed services like Amazon RDS, Aurora PostgreSQL, Google Cloud SQL, AlloyDB and Azure Database for PostgreSQL as long as you can create a schema and functions. On services that hide `pg_control_checkpoint()` it falls back to stored samples or a live probe.

## How it works

**Measuring consumption.** `take_sample()` stores the full 64 bit next transaction ID from `pg_snapshot_xmax(pg_current_snapshot())`, which reads the counter without assigning a transaction ID, so sampling does not consume what it measures. It also stores the next multixact ID derived from `mxid_age()`, the cluster system identifier and the oldest database ages. `consumption_rate()` fits a least squares slope with `regr_slope` over the samples in the window and also reports the peak rate between consecutive samples. Multixact IDs are 32 bit, so the code unwraps them by adding 2^32 whenever the raw value goes backwards. A drop in the 64 bit transaction ID or a change of system identifier means a restore from backup or a `pg_upgrade`. Only samples after the newest break are used. A physical failover keeps the same system identifier, so the history survives promotion.

When fewer than two useful samples exist, `consumption_rate()` uses the last checkpoint record from `pg_control_checkpoint()` as a second point. If that is not allowed and you pass `p_probe_seconds`, it sleeps and measures again, but only under READ COMMITTED, since under REPEATABLE READ the snapshot would not move and the rate would read as zero. The `source` column always says which method produced the number.

**Limits the way the server computes them.** `limits()` returns the freeze max age, the effective failsafe age (the server never lets it fall below 105% of the freeze max age), the warning limit 40 million IDs before wrap and the stop limit 3 million before wrap, for both kinds. `_eta()` turns an age, a target and a rate into a timestamp. It returns `now()` when the target is already passed, `infinity` when nothing is being consumed and NULL when the rate is unknown, so an unknown rate is never shown as safe.

**Cluster runway.** `cluster_runway()` returns one row per database and kind with the time until autovacuum forces a freeze, the failsafe, the warning limit and the stop limit, plus the stop time at the peak rate. `_grade()` makes it critical when the age is past the failsafe or the stop limit is closer than `p_crit_runway`, a warning when the failsafe is closer than `p_warn_runway` or the age is twice the freeze max age and a notice when a forced vacuum is due. It flags invalid databases left by an interrupted `DROP DATABASE` and databases with connections disabled.

**Horizon blockers.** `horizon_blockers()` collects everything that holds the xmin horizon back: client sessions with an old `backend_xmin` or `backend_xid`, prepared transactions, replication slots with `xmin` or `catalog_xmin` and standbys sending `hot_standby_feedback`. Autovacuum workers are excluded because vacuum processes do not hold back each other's horizons. Walsenders are reported once through `pg_stat_replication`. Each row says what it affects (one database, all databases or only system catalogs) and carries the fix: `pg_terminate_backend`, `ROLLBACK PREPARED` or `pg_drop_replication_slot` for inactive slots. A blocker older than `autovacuum_freeze_max_age` is critical, because past that point every anti wraparound vacuum runs, finishes and starts again without lowering any age. `report()` raises that as a separate "freeze floor" finding.

**Per table runway.** `_table_facts()` reads `pg_class`, `pg_index`, `pg_stat_all_tables` and `pg_stat_progress_vacuum`. It resolves the effective `autovacuum_freeze_max_age` and multixact freeze max age from reloptions, including the server rule that a TOAST table with no options of its own uses its parent's. It picks up per table cost limit and delay overrides, notices `autovacuum_enabled = false` and identifies temporary tables owned by other sessions, which autovacuum cannot touch at all. If `pg_visibility` is installed and `p_use_vm` is true, it counts all frozen pages exactly with `pg_visibility_map_summary()`, guarded by a short `lock_timeout`.

`_vacuum_seconds()` prices an aggressive vacuum with the real cost model. Each page not known to be all frozen costs `vacuum_cost_page_miss` plus `vacuum_cost_page_dirty` and each index page costs one miss. The total is divided by `cost_limit / cost_delay`. It is floored by an I/O rate you pass as `p_io_pages_per_sec`. `table_runway()` uses that to answer the question nothing else answers: will the freeze finish in time? It goes critical when even an unthrottled vacuum would finish after the stop limit and warns when the throttled autovacuum would not finish before the failsafe. Past the failsafe it switches to the unthrottled, index skipping estimate, because that is what the server does.

It also detects a pinned table: one that was vacuumed after it crossed its freeze max age but whose age did not drop. The time it crossed is worked out backwards from the age and the rate. That pattern almost always means a horizon blocker. The advice says so instead of telling you to vacuum again.

**Storm forecast.** `storm_forecast()` buckets every table by when it will cross its freeze max age, sums the vacuum work per bucket and runs a queue through the buckets with Lindley's recursion. Since the cost limit is balanced across workers, throttled capacity is one cost limit per second in total. Unthrottled, it is `autovacuum_max_workers` times your I/O rate. Empty buckets drain the backlog. A bucket whose backlog passes `p_crit_backlog` is critical. This is how you find the night when fifty bulk loaded tables all need freezing at once, a week before it happens.

**Report and check.** `report()` merges rates, settings checks (`track_counts` off, `autovacuum` off, freeze max age above one billion), sampler staleness, cluster rows, blockers, tables and storm buckets into one list sorted by severity. `check()` returns 0, 1 or 2 for monitoring.

## Usage

Install into each database you want table level detail for. The sample table only needs to be filled on one database, usually `postgres`.

```bash
psql -v ON_ERROR_STOP=1 -d postgres -f PostgresWraparoundRunway.sql
```

Take samples every five to fifteen minutes. With pg_cron:

```sql
SELECT cron.schedule('wraparound-sample', '*/10 * * * *', 'SELECT wraparound_runway.take_sample()');
SELECT cron.schedule('wraparound-prune', '15 3 * * *', 'SELECT wraparound_runway.prune()');
```

Without pg_cron, any scheduler works: `psql -d postgres -c 'SELECT wraparound_runway.take_sample()'`.

Read the ranked findings:

```sql
SELECT * FROM wraparound_runway.report();
SELECT * FROM wraparound_runway.report(interval '6 hours', 'peak', true);
```

Drill into each part:

```sql
SELECT * FROM wraparound_runway.consumption_rate(interval '24 hours', 5);
SELECT * FROM wraparound_runway.cluster_runway();
SELECT * FROM wraparound_runway.horizon_blockers(10000000);
SELECT * FROM wraparound_runway.table_runway(p_limit => 20, p_io_pages_per_sec => 40000);
SELECT * FROM wraparound_runway.storm_forecast(p_horizon => interval '14 days', p_bucket => interval '6 hours');
SELECT * FROM wraparound_runway.limits();
```

Alerting from a shell or a Nagios style check:

```bash
status=$(psql -d postgres -tAc 'SELECT wraparound_runway.check()')
exit "$status"
```

Grant read access to a monitoring login through the built in role:

```sql
GRANT pg_monitor TO monitoring_user;
```

Uninstall with `DROP SCHEMA wraparound_runway CASCADE;`.

## Notes

Nothing in this file vacuums, terminates, rolls back or drops anything. Every `action_sql` value is text for a human to read and run. Before `ROLLBACK PREPARED`, confirm with whatever coordinator created the transaction. Before dropping a slot, make sure its consumer is really gone, since dropping it forces a full resync for that subscriber.

Vacuum time estimates are upper bounds for cold data. They assume every unfrozen page is read from disk and dirtied. Warm caches and pages that only need a visibility bit make real vacuums faster. Without `pg_visibility` the scan size is all of `relpages`. The `scan_estimate` column tells you which estimate you got. `relpages` is the size the last VACUUM or ANALYZE saw, chosen on purpose so the tool never waits on a relation lock.

The storm forecast assumes tables reach their freeze max age without a regular vacuum freezing them first. Tables with steady updates usually get an aggressive vacuum earlier, once their age passes `vacuum_freeze_table_age`. Static and insert mostly tables do not. Those are exactly the ones that cause storms.

Multixact member space can run out before multixact IDs do. The server then lowers the effective multixact freeze max age on its own. There is no SQL interface that exposes member usage, so this tool cannot see it. If multixact rates look high, check the `pg_multixact/members` directory size as well.

`horizon_blockers()` needs `pg_monitor` or superuser to see other users' sessions. Tables are reported for the database you are connected to, since `pg_class` is per database. `cluster_runway()` covers every database and tells you where to connect next. `take_sample()` refuses to run on a standby, but samples replicate, so `report()` on a standby still has the trend. Tested on PostgreSQL 16 with a cluster aged to 1.8 billion transactions using `pg_resetwal`, including a forgotten prepared transaction pinning the horizon.
