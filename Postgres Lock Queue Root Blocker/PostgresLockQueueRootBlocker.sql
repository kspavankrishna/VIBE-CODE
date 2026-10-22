-- Postgres Lock Queue Root Blocker
-- Read only triage for lock pileups. Needs PostgreSQL 14 or newer (pg_locks.waitstart).
-- Installs one schema, lock_queue, with two functions. Nothing here kills a session.

CREATE SCHEMA IF NOT EXISTS lock_queue;

CREATE OR REPLACE FUNCTION lock_queue.triage(
    warn_victims int      DEFAULT 5,
    crit_victims int      DEFAULT 25,
    crit_wait    interval DEFAULT interval '30 seconds'
)
RETURNS TABLE (
    severity          text,
    kind              text,
    pid               int,
    backend_type      text,
    usename           name,
    application_name  text,
    client_addr       inet,
    state             text,
    xact_age          interval,
    state_age         interval,
    requested_lock    text,
    held_locks        text,
    direct_waiters    int,
    total_victims     int,
    sole_blocked      int,
    longest_victim_wait interval,
    query             text,
    advice            text,
    suggested_sql     text,
    fallback_sql      text
)
LANGUAGE sql
VOLATILE
AS $fn$
WITH RECURSIVE
act AS MATERIALIZED (
    SELECT a.pid, a.backend_type, a.usename, a.application_name, a.client_addr,
           a.state, a.wait_event_type, a.xact_start, a.state_change, a.query
    FROM pg_stat_activity a
    WHERE a.pid <> pg_backend_pid()
),
blocked AS MATERIALIZED (
    SELECT a.pid, pg_blocking_pids(a.pid) AS blockers
    FROM act a
    WHERE a.wait_event_type = 'Lock'
),
pending AS MATERIALIZED (
    SELECT DISTINCT ON (l.pid)
           l.pid, l.locktype, l.database, l.relation, l.page, l.tuple, l.virtualxid,
           l.transactionid, l.classid, l.objid, l.objsubid, l.mode, l.waitstart,
           CASE l.locktype
               WHEN 'relation'      THEN l.relation::regclass::text
               WHEN 'tuple'         THEN l.relation::regclass::text || ' row ' || l.page || ',' || l.tuple
               WHEN 'transactionid' THEN 'xid ' || l.transactionid
               WHEN 'virtualxid'    THEN 'vxid ' || l.virtualxid
               ELSE l.locktype
           END AS target
    FROM pg_locks l
    WHERE NOT l.granted AND l.pid IS NOT NULL
    ORDER BY l.pid, l.waitstart NULLS LAST
),
edges AS MATERIALIZED (
    SELECT b.blocker,
           w.pid AS waiter,
           w.blockers,
           (bp.pid IS NOT NULL
            AND (bp.locktype, bp.database, bp.relation, bp.page, bp.tuple, bp.virtualxid,
                 bp.transactionid, bp.classid, bp.objid, bp.objsubid)
                IS NOT DISTINCT FROM
                (wp.locktype, wp.database, wp.relation, wp.page, wp.tuple, wp.virtualxid,
                 wp.transactionid, wp.classid, wp.objid, wp.objsubid)) AS is_queue
    FROM blocked w
    CROSS JOIN LATERAL unnest(w.blockers) AS b(blocker)
    LEFT JOIN pending wp ON wp.pid = w.pid
    LEFT JOIN pending bp ON bp.pid = b.blocker
),
reach(src, dst) AS (
    SELECT e.blocker, e.waiter FROM edges e
    UNION
    SELECT r.src, e.waiter FROM reach r JOIN edges e ON e.blocker = r.dst
),
node AS (
    SELECT n.pid,
           EXISTS (SELECT 1 FROM edges e WHERE e.waiter = n.pid)                   AS is_waiter,
           EXISTS (SELECT 1 FROM reach r WHERE r.src = n.pid AND r.dst = n.pid)    AS in_cycle,
           (SELECT count(*) FROM edges e WHERE e.blocker = n.pid)::int             AS direct_waiters,
           (SELECT count(*) FROM edges e WHERE e.blocker = n.pid AND e.is_queue)::int AS queue_out,
           (SELECT count(*) FROM reach r WHERE r.src = n.pid AND r.dst <> n.pid)::int AS total_victims,
           (SELECT count(*) FROM edges e
             WHERE e.blocker = n.pid AND e.blockers = ARRAY[n.pid])::int           AS sole_blocked,
           (SELECT max(now() - p.waitstart) FROM reach r
              JOIN pending p ON p.pid = r.dst
             WHERE r.src = n.pid AND r.dst <> n.pid)                               AS longest_wait
    FROM (SELECT blocker AS pid FROM edges UNION SELECT waiter FROM edges) n
),
prepared AS (
    SELECT string_agg(format('%L', x.gid), ', ' ORDER BY x.prepared) AS gids,
           count(*)::int AS cnt,
           min(x.gid) AS only_gid
    FROM pg_prepared_xacts x
    WHERE x.database = current_database()
),
detail AS (
    SELECT n.*,
           a.backend_type, a.usename, a.application_name, a.client_addr, a.state,
           a.xact_start, a.state_change, a.query,
           p.mode || ' on ' || p.target AS requested_lock,
           (SELECT string_agg(h.mode || ' on ' || h.rel, ', ' ORDER BY h.rank)
              FROM (SELECT l.mode, c.oid::regclass::text AS rel,
                           array_position(ARRAY['AccessExclusiveLock','ExclusiveLock','ShareRowExclusiveLock',
                                                'ShareLock','ShareUpdateExclusiveLock','RowExclusiveLock',
                                                'RowShareLock','AccessShareLock'], l.mode) AS rank
                    FROM pg_locks l
                    JOIN pg_class c ON c.oid = l.relation
                    WHERE l.pid = n.pid AND l.granted AND l.locktype = 'relation'
                      AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
                      AND c.relkind IN ('r', 'p', 'm')
                    ORDER BY 3, 2 LIMIT 4) h)                                      AS held_locks
    FROM node n
    LEFT JOIN act a     ON a.pid = n.pid
    LEFT JOIN pending p ON p.pid = n.pid
),
classified AS (
    SELECT d.*,
           CASE
               WHEN d.in_cycle                                  THEN 'cycle'
               WHEN d.is_waiter AND d.queue_out > 0             THEN 'amplifier'
               WHEN NOT d.is_waiter AND d.total_victims > 0     THEN 'root'
           END AS kind
    FROM detail d
)
SELECT
    CASE WHEN c.total_victims >= crit_victims OR c.longest_wait >= crit_wait THEN 'critical'
         WHEN c.total_victims >= warn_victims                                THEN 'warning'
         ELSE 'info' END,
    c.kind,
    c.pid,
    COALESCE(c.backend_type, CASE WHEN c.pid = 0 THEN 'prepared transaction' ELSE 'gone' END),
    c.usename,
    c.application_name,
    c.client_addr,
    c.state,
    now() - c.xact_start,
    now() - c.state_change,
    c.requested_lock,
    c.held_locks,
    c.direct_waiters,
    c.total_victims,
    c.sole_blocked,
    c.longest_wait,
    left(regexp_replace(COALESCE(c.query, ''), '\s+', ' ', 'g'), 240),
    CASE
        WHEN c.kind = 'cycle' THEN
            'Wait cycle. The deadlock detector resolves this after deadlock_timeout. If it lingers, check for a stuck backend or a cycle that spans databases.'
        WHEN c.kind = 'amplifier' THEN
            format('Waiting for %s and %s session(s) are queued behind it (%s would run at once without it). Cancel this statement to release them, then retry under a short lock_timeout.',
                   c.requested_lock, c.direct_waiters, c.sole_blocked)
        WHEN c.pid = 0 THEN
            CASE WHEN pr.cnt = 0
                 THEN 'Blocker has no pid and no prepared transaction in this database. Look in other databases.'
                 ELSE format('Held by a prepared transaction. Candidate gid(s): %s. Check the transaction manager first, then ROLLBACK PREPARED or COMMIT PREPARED.', pr.gids) END
        WHEN c.backend_type = 'autovacuum worker' AND c.query LIKE '%to prevent wraparound%' THEN
            'Anti wraparound autovacuum. Do not cancel it. Stop the DDL that waits on it or wait for it to finish.'
        WHEN c.backend_type = 'autovacuum worker' THEN
            'Autovacuum holds a lock that conflicts with waiting DDL. Cancelling is safe, it restarts later.'
        WHEN c.state LIKE 'idle in transaction%' THEN
            format('Idle inside a transaction for %s and still holding locks. The client forgot to commit or roll back. Terminate it, cancel does nothing for an idle session. Set idle_in_transaction_session_timeout.',
                   date_trunc('second', now() - c.state_change))
        WHEN c.state = 'active' THEN
            format('Running a statement for %s while holding locks. Try cancel first, terminate if it ignores it.',
                   date_trunc('second', now() - c.state_change))
        ELSE 'Holds locks in state ' || COALESCE(c.state, 'unknown') || '. Inspect the client before terminating.'
    END,
    CASE
        WHEN c.kind = 'cycle' OR c.pid = 0 THEN NULL
        WHEN c.backend_type = 'autovacuum worker' AND c.query LIKE '%to prevent wraparound%' THEN NULL
        WHEN c.state LIKE 'idle in transaction%' AND NOT c.is_waiter THEN format('SELECT pg_terminate_backend(%s);', c.pid)
        ELSE format('SELECT pg_cancel_backend(%s);', c.pid)
    END,
    CASE
        WHEN c.kind = 'cycle' OR c.pid = 0 THEN
            CASE WHEN c.pid = 0 AND pr.cnt = 1 THEN format('ROLLBACK PREPARED %L;', pr.only_gid) END
        WHEN c.backend_type = 'autovacuum worker' THEN NULL
        WHEN c.state LIKE 'idle in transaction%' AND NOT c.is_waiter THEN NULL
        ELSE format('SELECT pg_terminate_backend(%s);', c.pid)
    END
FROM classified c
CROSS JOIN prepared pr
WHERE c.kind IS NOT NULL
ORDER BY (CASE WHEN c.total_victims >= crit_victims OR c.longest_wait >= crit_wait THEN 0
               WHEN c.total_victims >= warn_victims THEN 1 ELSE 2 END),
         c.total_victims DESC, c.longest_wait DESC NULLS LAST, c.pid;
$fn$;

COMMENT ON FUNCTION lock_queue.triage(int, int, interval) IS
    'Lists root blockers, queue amplifiers and wait cycles with victim counts and suggested (never executed) SQL.';

CREATE OR REPLACE FUNCTION lock_queue.guardrails()
RETURNS TABLE (setting text, value text, verdict text, advice text)
LANGUAGE sql
STABLE
AS $fn$
SELECT s.name::text,
       current_setting(s.name),
       CASE
           WHEN s.name = 'lock_timeout'                        AND s.setting = '0' THEN 'missing'
           WHEN s.name = 'idle_in_transaction_session_timeout' AND s.setting = '0' THEN 'missing'
           WHEN s.name = 'transaction_timeout'                 AND s.setting = '0' THEN 'missing'
           WHEN s.name = 'log_lock_waits'                      AND s.setting = 'off' THEN 'missing'
           WHEN s.name = 'deadlock_timeout'                    AND s.setting::bigint > 5000 THEN 'high'
           WHEN s.name = 'statement_timeout'                   AND s.setting = '0' THEN 'unbounded'
           ELSE 'ok'
       END,
       CASE s.name
           WHEN 'lock_timeout' THEN
               'Server wide it is blunt. Set it per migration role or with SET LOCAL inside each DDL transaction (2s to 5s) and retry.'
           WHEN 'idle_in_transaction_session_timeout' THEN
               'Without it one forgotten transaction can hold an ACCESS SHARE lock forever and stall every later ALTER TABLE.'
           WHEN 'transaction_timeout' THEN
               'PostgreSQL 17 and newer. Caps total transaction time, covers sessions that keep talking but never commit.'
           WHEN 'log_lock_waits' THEN
               'Turn it on so waits longer than deadlock_timeout are written to the log with the blocker pid.'
           WHEN 'deadlock_timeout' THEN
               'Also the delay before log_lock_waits fires and before cycles are broken. Long values hide pileups.'
           WHEN 'statement_timeout' THEN
               'Fine for superusers and batch roles. Application roles usually want a ceiling.'
       END
FROM pg_settings s
WHERE s.name IN ('lock_timeout', 'idle_in_transaction_session_timeout', 'transaction_timeout',
                 'log_lock_waits', 'deadlock_timeout', 'statement_timeout')
ORDER BY s.name;
$fn$;

COMMENT ON FUNCTION lock_queue.guardrails() IS
    'Shows which timeouts that stop lock pileups are unset on this server.';
