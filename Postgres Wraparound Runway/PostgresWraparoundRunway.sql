-- Postgres Wraparound Runway
-- Forecasts transaction ID and multixact wraparound for PostgreSQL 14 or newer.
-- Installs one schema, wraparound_runway: a sample table plus read only functions.
-- Nothing here vacuums, cancels or drops anything. Suggested SQL is returned as text.
-- No function takes a relation lock unless p_use_vm is true and pg_visibility is
-- installed, and then only with a short lock_timeout per table.
-- Reinstalling is safe: functions are dropped and recreated, samples are kept.

BEGIN;

DO $guard$
BEGIN
    IF current_setting('server_version_num')::int < 140000 THEN
        RAISE EXCEPTION 'wraparound_runway needs PostgreSQL 14 or newer, found %',
            current_setting('server_version');
    END IF;
END
$guard$;

CREATE SCHEMA IF NOT EXISTS wraparound_runway;

CREATE TABLE IF NOT EXISTS wraparound_runway.sample (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    taken_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    system_identifier numeric,
    next_xid          numeric NOT NULL,
    next_mxid_raw     bigint  NOT NULL,
    oldest_xid_age    bigint,
    oldest_mxid_age   bigint
);

CREATE INDEX IF NOT EXISTS sample_taken_at_idx ON wraparound_runway.sample (taken_at);

-- Upgrade path: drop every function in the schema so changed signatures install cleanly.
DO $drop$
DECLARE
    f record;
BEGIN
    FOR f IN
        SELECT p.oid::regprocedure AS sig
        FROM pg_proc p
        WHERE p.pronamespace = 'wraparound_runway'::regnamespace
    LOOP
        EXECUTE format('DROP FUNCTION %s', f.sig);
    END LOOP;
END
$drop$;

-- ---------------------------------------------------------------------------
-- Small helpers
-- ---------------------------------------------------------------------------

CREATE FUNCTION wraparound_runway._rank(p_severity text)
RETURNS int
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    SELECT CASE p_severity
        WHEN 'critical' THEN 3
        WHEN 'warning'  THEN 2
        WHEN 'notice'   THEN 1
        ELSE 0
    END
$$;

CREATE FUNCTION wraparound_runway._setting(p_name text)
RETURNS numeric
LANGUAGE sql STABLE
AS $$
    SELECT s.setting::numeric FROM pg_settings s WHERE s.name = p_name
$$;

CREATE FUNCTION wraparound_runway._opt(p_options text[], p_name text)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    SELECT substr(o, length(p_name) + 2)
    FROM unnest(p_options) AS o
    WHERE o LIKE p_name || '=%'
    LIMIT 1
$$;

-- Time at which an age that grows at p_rate per second reaches p_target.
-- now() when already there, infinity when nothing is being consumed,
-- NULL when the rate is unknown.
CREATE FUNCTION wraparound_runway._eta(p_age numeric, p_target numeric, p_rate numeric)
RETURNS timestamptz
LANGUAGE sql STABLE
AS $$
    SELECT CASE
        WHEN p_age >= p_target                      THEN now()
        WHEN p_rate IS NULL                         THEN NULL
        WHEN p_rate <= 0                            THEN 'infinity'::timestamptz
        WHEN (p_target - p_age) / p_rate > 1e10     THEN 'infinity'::timestamptz
        ELSE now() + make_interval(secs => ((p_target - p_age) / p_rate)::double precision)
    END
$$;

CREATE FUNCTION wraparound_runway._secs(p_seconds numeric)
RETURNS interval
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN p_seconds IS NULL THEN NULL
        ELSE make_interval(secs => round(least(p_seconds, 1e10), 0)::double precision)
    END
$$;

CREATE FUNCTION wraparound_runway._grade(
    p_age          numeric,
    p_freeze_max   numeric,
    p_failsafe     numeric,
    p_eta_failsafe timestamptz,
    p_eta_stop     timestamptz,
    p_crit_runway  interval,
    p_warn_runway  interval
)
RETURNS text
LANGUAGE sql STABLE
AS $$
    SELECT CASE
        WHEN p_age >= p_failsafe
          OR p_eta_stop < now() + p_crit_runway        THEN 'critical'
        WHEN p_eta_failsafe < now() + p_warn_runway
          OR p_age >= 2 * p_freeze_max                 THEN 'warning'
        WHEN p_age >= p_freeze_max                     THEN 'notice'
        ELSE 'ok'
    END
$$;

-- Full 64 bit next transaction ID. Reading the snapshot never assigns an XID,
-- so sampling does not consume the resource it measures.
CREATE FUNCTION wraparound_runway._next_xid()
RETURNS numeric
LANGUAGE sql VOLATILE
AS $$
    SELECT pg_snapshot_xmax(pg_current_snapshot())::text::numeric
$$;

-- Next multixact ID, 32 bit and wrapping. Derived from the current database
-- because mxid_age() measures against the cluster wide next multixact.
CREATE FUNCTION wraparound_runway._next_mxid()
RETURNS bigint
LANGUAGE sql VOLATILE
AS $$
    SELECT (d.datminmxid::text::bigint + mxid_age(d.datminmxid)) % 4294967296
    FROM pg_database d
    WHERE d.datname = current_database()
$$;

CREATE FUNCTION wraparound_runway._system_identifier()
RETURNS numeric
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
    RETURN (SELECT s.system_identifier::numeric FROM pg_control_system() s);
EXCEPTION WHEN insufficient_privilege THEN
    RETURN NULL;
END
$$;

-- ---------------------------------------------------------------------------
-- Limits, as the server computes them
-- ---------------------------------------------------------------------------

CREATE FUNCTION wraparound_runway.limits()
RETURNS TABLE (
    kind           text,
    freeze_max_age bigint,
    failsafe_age   bigint,
    warn_age       bigint,
    stop_age       bigint,
    wrap_age       bigint
)
LANGUAGE sql STABLE
AS $$
    -- The wrap limit is 2^31 - 1 IDs past the oldest frozen ID. The server refuses
    -- new IDs 3,000,000 before it and starts warning 40,000,000 before it.
    -- The failsafe is never earlier than 105% of the freeze max age.
    SELECT 'xid'::text,
           f.v::bigint,
           greatest(s.v, f.v * 1.05)::bigint,
           2147483647 - 40000000,
           2147483647 - 3000000,
           2147483647
    FROM (SELECT wraparound_runway._setting('autovacuum_freeze_max_age') AS v) f,
         (SELECT wraparound_runway._setting('vacuum_failsafe_age') AS v) s
    UNION ALL
    SELECT 'mxid'::text,
           f.v::bigint,
           greatest(s.v, f.v * 1.05)::bigint,
           2147483647 - 40000000,
           2147483647 - 3000000,
           2147483647
    FROM (SELECT wraparound_runway._setting('autovacuum_multixact_freeze_max_age') AS v) f,
         (SELECT wraparound_runway._setting('vacuum_multixact_failsafe_age') AS v) s
$$;

-- ---------------------------------------------------------------------------
-- Sampling
-- ---------------------------------------------------------------------------

CREATE FUNCTION wraparound_runway.take_sample()
RETURNS wraparound_runway.sample
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_row wraparound_runway.sample;
BEGIN
    IF pg_is_in_recovery() THEN
        RAISE EXCEPTION 'take_sample() must run on the primary'
            USING HINT = 'Samples replicate to standbys, so report() on a standby still sees them.';
    END IF;

    INSERT INTO wraparound_runway.sample
        (taken_at, system_identifier, next_xid, next_mxid_raw, oldest_xid_age, oldest_mxid_age)
    SELECT clock_timestamp(),
           wraparound_runway._system_identifier(),
           wraparound_runway._next_xid(),
           wraparound_runway._next_mxid(),
           max(age(d.datfrozenxid)),
           max(mxid_age(d.datminmxid))
    FROM pg_database d
    RETURNING * INTO v_row;

    RETURN v_row;
END
$$;

CREATE FUNCTION wraparound_runway.prune(p_keep interval DEFAULT interval '35 days')
RETURNS bigint
LANGUAGE sql VOLATILE
AS $$
    WITH gone AS (
        DELETE FROM wraparound_runway.sample s
        WHERE s.taken_at < now() - p_keep
        RETURNING 1
    )
    SELECT count(*) FROM gone
$$;

-- ---------------------------------------------------------------------------
-- Consumption rate
-- ---------------------------------------------------------------------------
-- Source order: stored samples spanning at least a minute, then the last
-- checkpoint record, then a live probe if asked for, then whatever is left.
-- A drop in next_xid or a change of system identifier means a restore or a
-- pg_upgrade, so only samples after the newest break are used.

CREATE FUNCTION wraparound_runway.consumption_rate(
    p_window        interval DEFAULT interval '24 hours',
    p_probe_seconds numeric  DEFAULT 0
)
RETURNS TABLE (
    kind         text,
    source       text,
    avg_per_sec  numeric,
    peak_per_sec numeric,
    points       int,
    span         interval,
    note         text
)
LANGUAGE plpgsql VOLATILE
AS $fn$
#variable_conflict use_column
DECLARE
    v_t      timestamptz := clock_timestamp();
    v_x      numeric     := wraparound_runway._next_xid();
    v_m      bigint      := wraparound_runway._next_mxid();
    v_sys    numeric     := wraparound_runway._system_identifier();
    v_n      int;
    v_t0     timestamptz;
    v_xs     double precision;
    v_ms     double precision;
    v_xp     numeric;
    v_mp     numeric;
    v_ck_t   timestamptz;
    v_ck_x   numeric;
    v_ck_m   bigint;
    v_dt     numeric;
    v_dm     numeric;
    v_p_x    numeric;
    v_p_m    bigint;
    v_p_t    timestamptz;
BEGIN
    WITH raw AS (
        SELECT s.id, s.taken_at AS t, s.system_identifier AS sys, s.next_xid AS x, s.next_mxid_raw AS m
        FROM wraparound_runway.sample s
        WHERE s.taken_at >= v_t - p_window AND s.taken_at < v_t
        UNION ALL
        SELECT 9223372036854775807, v_t, v_sys, v_x, v_m
    ),
    lagged AS (
        SELECT r.*, lag(r.x) OVER w AS px, lag(r.sys) OVER w AS psys
        FROM raw r
        WINDOW w AS (ORDER BY r.t, r.id)
    ),
    brk AS (
        SELECT coalesce(max(l.t) FILTER (
                   WHERE l.px IS NOT NULL
                     AND (l.x < l.px OR (l.sys IS NOT NULL AND l.psys IS NOT NULL AND l.sys <> l.psys))),
               '-infinity'::timestamptz) AS cut
        FROM lagged l
    ),
    kept AS (
        SELECT l.id, l.t, l.x, l.m FROM lagged l, brk WHERE l.t >= brk.cut
    ),
    d AS (
        SELECT k.id, k.t, k.x,
               extract(epoch FROM k.t - lag(k.t) OVER w)::numeric AS dt,
               k.x - lag(k.x) OVER w AS dx,
               CASE WHEN k.m - lag(k.m) OVER w < 0
                    THEN k.m - lag(k.m) OVER w + 4294967296
                    ELSE k.m - lag(k.m) OVER w END AS dm
        FROM kept k
        WINDOW w AS (ORDER BY k.t, k.id)
    ),
    u AS (
        SELECT d.*,
               sum(coalesce(d.dm, 0)) OVER (ORDER BY d.t, d.id ROWS UNBOUNDED PRECEDING) AS mu,
               first_value(d.x) OVER (ORDER BY d.t, d.id) AS x0,
               first_value(d.t) OVER (ORDER BY d.t, d.id) AS t0
        FROM d
    )
    SELECT count(*)::int,
           min(u.t),
           regr_slope((u.x - u.x0)::double precision, extract(epoch FROM u.t - u.t0)::double precision),
           regr_slope(u.mu::double precision,          extract(epoch FROM u.t - u.t0)::double precision),
           max(u.dx / u.dt) FILTER (WHERE u.dt >= 1),
           max(u.dm / u.dt) FILTER (WHERE u.dt >= 1)
    INTO v_n, v_t0, v_xs, v_ms, v_xp, v_mp
    FROM u;

    IF v_n >= 2 AND v_t - v_t0 >= interval '60 seconds' THEN
        RETURN QUERY
        SELECT 'xid'::text, 'samples'::text, round(greatest(v_xs, 0)::numeric, 3), round(v_xp, 3),
               v_n, v_t - v_t0, NULL::text
        UNION ALL
        SELECT 'mxid'::text, 'samples'::text, round(greatest(v_ms, 0)::numeric, 3), round(v_mp, 3),
               v_n, v_t - v_t0, NULL::text;
        RETURN;
    END IF;

    BEGIN
        SELECT c.checkpoint_time,
               split_part(c.next_xid, ':', 1)::numeric * 4294967296 + split_part(c.next_xid, ':', 2)::numeric,
               c.next_multixact_id::text::bigint
        INTO v_ck_t, v_ck_x, v_ck_m
        FROM pg_control_checkpoint() c;
    EXCEPTION WHEN insufficient_privilege THEN
        v_ck_t := NULL;
    END;

    IF v_ck_t IS NOT NULL AND v_t - v_ck_t >= interval '60 seconds' AND v_x >= v_ck_x THEN
        v_dt := extract(epoch FROM v_t - v_ck_t);
        v_dm := v_m - v_ck_m;
        IF v_dm < 0 THEN v_dm := v_dm + 4294967296; END IF;
        RETURN QUERY
        SELECT 'xid'::text, 'checkpoint'::text, round((v_x - v_ck_x) / v_dt, 3), NULL::numeric, 2, v_t - v_ck_t,
               'single interval since the last checkpoint; schedule take_sample() for a real trend'::text
        UNION ALL
        SELECT 'mxid'::text, 'checkpoint'::text, round(v_dm / v_dt, 3), NULL::numeric, 2, v_t - v_ck_t,
               'single interval since the last checkpoint; schedule take_sample() for a real trend'::text;
        RETURN;
    END IF;

    IF p_probe_seconds > 0 THEN
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RETURN QUERY
            SELECT k, 'none'::text, NULL::numeric, NULL::numeric, v_n, NULL::interval,
                   'probe skipped: the snapshot is frozen under ' || current_setting('transaction_isolation')
            FROM unnest(ARRAY['xid', 'mxid']) AS k;
            RETURN;
        END IF;
        v_p_t := v_t; v_p_x := v_x; v_p_m := v_m;
        PERFORM pg_sleep(p_probe_seconds);
        v_t := clock_timestamp();
        v_x := wraparound_runway._next_xid();
        v_m := wraparound_runway._next_mxid();
        v_dt := extract(epoch FROM v_t - v_p_t);
        v_dm := v_m - v_p_m;
        IF v_dm < 0 THEN v_dm := v_dm + 4294967296; END IF;
        RETURN QUERY
        SELECT 'xid'::text, 'probe'::text, round((v_x - v_p_x) / v_dt, 3), NULL::numeric, 2, v_t - v_p_t,
               'short live probe; bursty workloads will be under or over stated'::text
        UNION ALL
        SELECT 'mxid'::text, 'probe'::text, round(v_dm / v_dt, 3), NULL::numeric, 2, v_t - v_p_t,
               'short live probe; bursty workloads will be under or over stated'::text;
        RETURN;
    END IF;

    IF v_n >= 2 AND v_t - v_t0 >= interval '1 second' THEN
        RETURN QUERY
        SELECT 'xid'::text, 'samples'::text, round(greatest(v_xs, 0)::numeric, 3), round(v_xp, 3),
               v_n, v_t - v_t0, 'samples span under a minute; treat as rough'::text
        UNION ALL
        SELECT 'mxid'::text, 'samples'::text, round(greatest(v_ms, 0)::numeric, 3), round(v_mp, 3),
               v_n, v_t - v_t0, 'samples span under a minute; treat as rough'::text;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT k, 'none'::text, NULL::numeric, NULL::numeric, v_n, NULL::interval,
           'no rate yet: schedule take_sample(), or pass p_probe_seconds'::text
    FROM unnest(ARRAY['xid', 'mxid']) AS k;
END
$fn$;

CREATE FUNCTION wraparound_runway._planning_rate(
    p_window interval,
    p_basis  text
)
RETURNS TABLE (kind text, source text, rate numeric, peak numeric)
LANGUAGE plpgsql VOLATILE
AS $fn$
#variable_conflict use_column
BEGIN
    IF p_basis NOT IN ('average', 'peak') THEN
        RAISE EXCEPTION 'p_basis must be average or peak, got %', p_basis;
    END IF;
    RETURN QUERY
    SELECT r.kind, r.source,
           CASE WHEN p_basis = 'peak' THEN greatest(r.peak_per_sec, r.avg_per_sec) ELSE r.avg_per_sec END,
           greatest(r.peak_per_sec, r.avg_per_sec)
    FROM wraparound_runway.consumption_rate(p_window) r;
END
$fn$;

-- ---------------------------------------------------------------------------
-- Cluster runway: one row per database and ID kind
-- ---------------------------------------------------------------------------

CREATE FUNCTION wraparound_runway.cluster_runway(
    p_window      interval DEFAULT interval '24 hours',
    p_basis       text     DEFAULT 'average',
    p_crit_runway interval DEFAULT interval '3 days',
    p_warn_runway interval DEFAULT interval '14 days'
)
RETURNS TABLE (
    severity         text,
    datname          name,
    kind             text,
    current_age      bigint,
    pct_of_stop      numeric,
    rate_per_sec     numeric,
    rate_source      text,
    eta_autovacuum   timestamptz,
    eta_failsafe     timestamptz,
    eta_warn_limit   timestamptz,
    eta_stop         timestamptz,
    eta_stop_at_peak timestamptz,
    advice           text
)
LANGUAGE sql VOLATILE
AS $fn$
    WITH rate AS (
        SELECT * FROM wraparound_runway._planning_rate(p_window, p_basis)
    ),
    db AS (
        SELECT d.datname AS dn, d.datallowconn AS allow, d.datconnlimit AS connlimit,
               'xid'::text AS k, age(d.datfrozenxid)::bigint AS a
        FROM pg_database d
        UNION ALL
        SELECT d.datname, d.datallowconn, d.datconnlimit,
               'mxid'::text, mxid_age(d.datminmxid)::bigint
        FROM pg_database d
    ),
    j AS (
        SELECT db.*, l.freeze_max_age AS fma, l.failsafe_age AS fs, l.warn_age AS wa, l.stop_age AS sa,
               r.source AS src, r.rate AS rt, r.peak AS pk,
               wraparound_runway._eta(db.a, l.freeze_max_age, r.rate) AS e_av,
               wraparound_runway._eta(db.a, l.failsafe_age,   r.rate) AS e_fs,
               wraparound_runway._eta(db.a, l.warn_age,       r.rate) AS e_wa,
               wraparound_runway._eta(db.a, l.stop_age,       r.rate) AS e_st,
               wraparound_runway._eta(db.a, l.stop_age,       r.peak) AS e_pk
        FROM db
        JOIN wraparound_runway.limits() l ON l.kind = db.k
        JOIN rate r ON r.kind = db.k
    ),
    g AS (
        SELECT j.*,
               wraparound_runway._grade(j.a, j.fma, j.fs, j.e_fs, j.e_st, p_crit_runway, p_warn_runway) AS sev
        FROM j
    )
    SELECT CASE WHEN g.connlimit = -2 AND g.sev = 'ok' THEN 'notice' ELSE g.sev END,
           g.dn, g.k, g.a,
           round(100.0 * g.a / g.sa, 2),
           g.rt, g.src,
           g.e_av, g.e_fs, g.e_wa, g.e_st, g.e_pk,
           concat_ws(' ',
               CASE WHEN g.connlimit = -2 THEN
                   format('Invalid database left by an interrupted DROP DATABASE: finish it with DROP DATABASE %I.', g.dn) END,
               CASE WHEN g.sev <> 'ok' AND NOT g.allow AND g.connlimit <> -2 THEN
                   format('Connections are disabled. Autovacuum still processes it; to vacuum by hand run ALTER DATABASE %I ALLOW_CONNECTIONS true first.', g.dn) END,
               CASE WHEN g.sev <> 'ok' AND g.allow THEN
                   format('Connect to %I and run SELECT * FROM wraparound_runway.table_runway() to find the tables holding it back.', g.dn) END,
               CASE WHEN g.src = 'none' THEN 'No consumption rate yet, so only the age is graded.' END)
    FROM g
    ORDER BY wraparound_runway._rank(g.sev) DESC, g.a::numeric / g.sa DESC, g.dn, g.k
$fn$;

-- ---------------------------------------------------------------------------
-- Horizon blockers: whatever stops VACUUM from freezing past a point
-- ---------------------------------------------------------------------------

CREATE FUNCTION wraparound_runway.horizon_blockers(
    p_min_age bigint DEFAULT 1000000
)
RETURNS TABLE (
    severity   text,
    kind       text,
    name       text,
    database   name,
    affects    text,
    xmin_age   bigint,
    since      timestamptz,
    detail     text,
    action_sql text
)
LANGUAGE sql STABLE
AS $fn$
    WITH lim AS (
        SELECT l.freeze_max_age AS fma FROM wraparound_runway.limits() l WHERE l.kind = 'xid'
    ),
    b AS (
        SELECT 'session'::text AS k,
               format('pid %s', a.pid) AS nm,
               a.datname AS db,
               CASE WHEN a.datname IS NULL THEN 'all databases'
                    ELSE format('database %I and shared catalogs', a.datname) END AS aff,
               greatest(age(a.backend_xmin), age(a.backend_xid))::bigint AS xa,
               a.xact_start AS st,
               format('%s, user %s, app %s, state %s, %s: %s',
                      a.backend_type, a.usename, nullif(a.application_name, ''),
                      a.state, coalesce(a.wait_event_type || '/' || a.wait_event, 'not waiting'),
                      left(regexp_replace(coalesce(a.query, ''), '\s+', ' ', 'g'), 160)) AS det,
               CASE WHEN a.backend_type = 'client backend'
                    THEN format('SELECT pg_terminate_backend(%s);', a.pid) END AS act
        FROM pg_stat_activity a
        WHERE (a.backend_xmin IS NOT NULL OR a.backend_xid IS NOT NULL)
          AND a.pid <> pg_backend_pid()
          -- VACUUM processes are ignored by other vacuums' horizons, and walsender
          -- feedback is reported from pg_stat_replication below.
          AND a.backend_type NOT IN ('walsender', 'autovacuum worker')
        UNION ALL
        SELECT 'prepared transaction', p.gid, p.database,
               format('database %I and shared catalogs', p.database),
               age(p.transaction)::bigint, p.prepared,
               format('owner %s, prepared %s ago. A forgotten two phase commit never ends on its own.',
                      p.owner, date_trunc('second', now() - p.prepared)),
               format('ROLLBACK PREPARED %L; -- connected to %I, after confirming with the transaction manager',
                      p.gid, p.database)
        FROM pg_prepared_xacts p
        UNION ALL
        SELECT 'replication slot', s.slot_name::text, s.database,
               CASE WHEN s.xmin IS NOT NULL THEN 'all databases'
                    ELSE 'system catalogs in all databases' END,
               greatest(age(s.xmin), age(s.catalog_xmin))::bigint, NULL::timestamptz,
               format('%s slot, %s, wal_status %s',
                      s.slot_type,
                      CASE WHEN s.active THEN 'active pid ' || s.active_pid ELSE 'inactive' END,
                      coalesce(s.wal_status, 'unknown')),
               CASE WHEN NOT s.active THEN format('SELECT pg_drop_replication_slot(%L);', s.slot_name) END
        FROM pg_replication_slots s
        WHERE s.xmin IS NOT NULL OR s.catalog_xmin IS NOT NULL
        UNION ALL
        SELECT 'standby feedback', coalesce(nullif(r.application_name, ''), 'pid ' || r.pid), NULL::name,
               'all databases',
               age(r.backend_xmin)::bigint, r.backend_start,
               format('client %s, state %s. hot_standby_feedback is on and a standby query holds this horizon.',
                      coalesce(host(r.client_addr), 'local'), r.state),
               NULL::text
        FROM pg_stat_replication r
        WHERE r.backend_xmin IS NOT NULL
    )
    SELECT CASE WHEN b.xa >= lim.fma     THEN 'critical'
                WHEN b.xa >= lim.fma / 2 THEN 'warning'
                ELSE 'notice' END,
           b.k, b.nm, b.db, b.aff, b.xa, b.st, b.det, b.act
    FROM b, lim
    WHERE b.xa >= p_min_age
    ORDER BY b.xa DESC, b.st ASC NULLS LAST, b.k, b.nm
$fn$;

-- ---------------------------------------------------------------------------
-- Table facts for the current database
-- ---------------------------------------------------------------------------
-- Reads pg_class and the statistics views only, so it never queues behind an
-- ACCESS EXCLUSIVE lock. relpages is the size VACUUM or ANALYZE last saw.

CREATE FUNCTION wraparound_runway._table_facts(
    p_limit  int     DEFAULT NULL,
    p_use_vm boolean DEFAULT false
)
RETURNS TABLE (
    relid           oid,
    relation        text,
    vacuum_target   text,
    relkind         "char",
    persistence     "char",
    other_temp      boolean,
    xid_age         bigint,
    mxid_age        bigint,
    fma             bigint,
    mfma            bigint,
    av_disabled     boolean,
    heap_pages      bigint,
    index_pages     bigint,
    all_visible     bigint,
    all_frozen      bigint,
    vm_exact        boolean,
    cost_limit      numeric,
    cost_delay_ms   numeric,
    last_vacuum     timestamptz,
    vacuum_running  text
)
LANGUAGE plpgsql VOLATILE
AS $fn$
#variable_conflict use_column
DECLARE
    v_has_vm    boolean := p_use_vm AND EXISTS (SELECT 1 FROM pg_extension e WHERE e.extname = 'pg_visibility');
    v_vm_schema text;
    v_old_lt    text    := current_setting('lock_timeout');
    v_fma       numeric := wraparound_runway._setting('autovacuum_freeze_max_age');
    v_mfma      numeric := wraparound_runway._setting('autovacuum_multixact_freeze_max_age');
    v_limit     numeric := coalesce(nullif(wraparound_runway._setting('autovacuum_vacuum_cost_limit'), -1),
                                    wraparound_runway._setting('vacuum_cost_limit'));
    v_delay     numeric := coalesce(nullif(wraparound_runway._setting('autovacuum_vacuum_cost_delay'), -1),
                                    wraparound_runway._setting('vacuum_cost_delay'));
    rec         record;
    v_frozen    bigint;
BEGIN
    IF v_has_vm THEN
        SELECT n.nspname INTO v_vm_schema
        FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = 'pg_visibility';
        PERFORM set_config('lock_timeout', '200ms', true);
    END IF;

    FOR rec IN
        WITH c AS (
            SELECT c.oid, c.relname, c.relnamespace, c.relkind, c.relpersistence,
                   c.relpages, c.relallvisible,
                   age(c.relfrozenxid)::bigint AS xa,
                   mxid_age(c.relminmxid)::bigint AS ma,
                   p.oid AS parent_oid, p.relname AS parent_name, p.relnamespace AS parent_ns,
                   -- A TOAST table with no options of its own uses its parent's.
                   CASE WHEN c.relkind = 't' AND c.reloptions IS NULL THEN p.reloptions
                        ELSE c.reloptions END AS opts
            FROM pg_class c
            LEFT JOIN pg_class p ON c.relkind = 't' AND p.reltoastrelid = c.oid
            WHERE c.relkind IN ('r', 'm', 't')
        ),
        f AS (
            SELECT c.*,
                   least(v_fma,  wraparound_runway._opt(c.opts, 'autovacuum_freeze_max_age')::numeric)::bigint AS e_fma,
                   least(v_mfma, wraparound_runway._opt(c.opts, 'autovacuum_multixact_freeze_max_age')::numeric)::bigint AS e_mfma,
                   coalesce(wraparound_runway._opt(c.opts, 'autovacuum_enabled'), 'true') IN ('false', 'off', '0', 'no') AS avd,
                   coalesce(wraparound_runway._opt(c.opts, 'autovacuum_vacuum_cost_limit')::numeric, v_limit) AS climit,
                   coalesce(wraparound_runway._opt(c.opts, 'autovacuum_vacuum_cost_delay')::numeric, v_delay) AS cdelay
            FROM c
        )
        SELECT f.oid, f.relkind, f.relpersistence,
               f.relpersistence = 't' AND f.relnamespace <> pg_my_temp_schema() AS other_temp,
               CASE WHEN f.parent_oid IS NOT NULL
                    THEN format('%I.%I (toast)', pn.nspname, f.parent_name)
                    ELSE format('%I.%I', n.nspname, f.relname) END AS rel_label,
               format('%I.%I', n.nspname, f.relname) AS target,
               f.xa, f.ma, f.e_fma, f.e_mfma, f.avd,
               greatest(f.relpages, 0)::bigint AS hp,
               coalesce((SELECT sum(greatest(ic.relpages, 0))
                         FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid
                         WHERE i.indrelid = f.oid), 0)::bigint AS ip,
               greatest(f.relallvisible, 0)::bigint AS av,
               f.climit, f.cdelay,
               greatest(st.last_vacuum, st.last_autovacuum) AS lv,
               (SELECT format('%s%s, %s%% scanned, pid %s',
                              CASE WHEN act.query ILIKE '%to prevent wraparound%' THEN 'anti wraparound ' ELSE '' END,
                              pv.phase,
                              CASE WHEN pv.heap_blks_total > 0
                                   THEN round(100.0 * pv.heap_blks_scanned / pv.heap_blks_total, 1)
                                   ELSE 0 END,
                              pv.pid)
                FROM pg_stat_progress_vacuum pv
                LEFT JOIN pg_stat_activity act ON act.pid = pv.pid
                WHERE pv.relid = f.oid AND pv.datname = current_database()
                LIMIT 1) AS running
        FROM f
        JOIN pg_namespace n ON n.oid = f.relnamespace
        LEFT JOIN pg_namespace pn ON pn.oid = f.parent_ns
        LEFT JOIN pg_stat_all_tables st ON st.relid = f.oid
        ORDER BY greatest(f.xa::numeric / nullif(f.e_fma, 0), f.ma::numeric / nullif(f.e_mfma, 0)) DESC NULLS LAST,
                 f.oid
        LIMIT p_limit
    LOOP
        v_frozen := NULL;
        IF v_has_vm AND NOT rec.other_temp THEN
            BEGIN
                EXECUTE format('SELECT all_frozen FROM %I.pg_visibility_map_summary($1)', v_vm_schema)
                    INTO v_frozen USING rec.oid;
            EXCEPTION WHEN lock_not_available OR insufficient_privilege OR undefined_table THEN
                v_frozen := NULL;
            END;
        END IF;

        relid          := rec.oid;
        relation       := rec.rel_label;
        vacuum_target  := rec.target;
        relkind        := rec.relkind;
        persistence    := rec.relpersistence;
        other_temp     := rec.other_temp;
        xid_age        := rec.xa;
        mxid_age       := rec.ma;
        fma            := rec.e_fma;
        mfma           := rec.e_mfma;
        av_disabled    := rec.avd;
        heap_pages     := rec.hp;
        index_pages    := rec.ip;
        all_visible    := rec.av;
        all_frozen     := v_frozen;
        vm_exact       := v_frozen IS NOT NULL;
        cost_limit     := rec.climit;
        cost_delay_ms  := rec.cdelay;
        last_vacuum    := rec.lv;
        vacuum_running := rec.running;
        RETURN NEXT;
    END LOOP;

    IF v_has_vm THEN
        PERFORM set_config('lock_timeout', v_old_lt, true);
    END IF;
END
$fn$;

-- Seconds an aggressive VACUUM needs for one table under the vacuum cost model.
-- Every page that is not known to be all frozen is read and assumed dirtied,
-- indexes are read once. This is an upper bound for cold data.
CREATE FUNCTION wraparound_runway._vacuum_seconds(
    p_scan_pages    numeric,
    p_index_pages   numeric,
    p_cost_limit    numeric,
    p_cost_delay_ms numeric,
    p_io_pages      numeric,
    p_throttled     boolean
)
RETURNS numeric
LANGUAGE sql STABLE
AS $$
    SELECT CASE
        WHEN NOT p_throttled OR p_cost_delay_ms <= 0 OR p_cost_limit <= 0
            THEN (p_scan_pages + p_index_pages) / p_io_pages
        ELSE greatest(
            (p_scan_pages  * (wraparound_runway._setting('vacuum_cost_page_miss')
                            + wraparound_runway._setting('vacuum_cost_page_dirty'))
           + p_index_pages *  wraparound_runway._setting('vacuum_cost_page_miss'))
            / (p_cost_limit / (p_cost_delay_ms / 1000.0)),
            (p_scan_pages + p_index_pages) / p_io_pages)
    END
$$;

-- ---------------------------------------------------------------------------
-- Table runway for the current database
-- ---------------------------------------------------------------------------

CREATE FUNCTION wraparound_runway.table_runway(
    p_window           interval DEFAULT interval '24 hours',
    p_basis            text     DEFAULT 'average',
    p_limit            int      DEFAULT 50,
    p_use_vm           boolean  DEFAULT true,
    p_io_pages_per_sec numeric  DEFAULT 25000,
    p_crit_runway      interval DEFAULT interval '3 days',
    p_warn_runway      interval DEFAULT interval '14 days'
)
RETURNS TABLE (
    severity               text,
    relation               text,
    relkind                "char",
    xid_age                bigint,
    mxid_age               bigint,
    freeze_max_age         bigint,
    mxid_freeze_max_age    bigint,
    heap_size              text,
    pages_to_scan          bigint,
    scan_estimate          text,
    est_vacuum_throttled   interval,
    est_vacuum_unthrottled interval,
    eta_autovacuum         timestamptz,
    eta_failsafe           timestamptz,
    eta_stop               timestamptz,
    vacuum_running         text,
    last_vacuum            timestamptz,
    advice                 text,
    action_sql             text
)
LANGUAGE sql VOLATILE
AS $fn$
    WITH rate AS (
        SELECT * FROM wraparound_runway._planning_rate(p_window, p_basis)
    ),
    lx AS (SELECT * FROM wraparound_runway.limits() l WHERE l.kind = 'xid'),
    lm AS (SELECT * FROM wraparound_runway.limits() l WHERE l.kind = 'mxid'),
    rx AS (SELECT r.rate FROM rate r WHERE r.kind = 'xid'),
    rm AS (SELECT r.rate FROM rate r WHERE r.kind = 'mxid'),
    t AS (
        SELECT f.*,
               greatest(f.heap_pages - coalesce(f.all_frozen, 0), 0) AS scan,
               greatest(lx.failsafe_age, f.fma  * 1.05)::bigint AS fs_x,
               greatest(lm.failsafe_age, f.mfma * 1.05)::bigint AS fs_m,
               lx.stop_age AS stop_x, lm.stop_age AS stop_m,
               rx.rate AS rate_x, rm.rate AS rate_m
        FROM wraparound_runway._table_facts(p_limit, p_use_vm) f, lx, lm, rx, rm
    ),
    e AS (
        SELECT t.*,
               least(wraparound_runway._eta(t.xid_age,  t.fma,    t.rate_x),
                     wraparound_runway._eta(t.mxid_age, t.mfma,   t.rate_m)) AS e_av,
               least(wraparound_runway._eta(t.xid_age,  t.fs_x,   t.rate_x),
                     wraparound_runway._eta(t.mxid_age, t.fs_m,   t.rate_m)) AS e_fs,
               least(wraparound_runway._eta(t.xid_age,  t.stop_x, t.rate_x),
                     wraparound_runway._eta(t.mxid_age, t.stop_m, t.rate_m)) AS e_st,
               t.xid_age >= t.fs_x OR t.mxid_age >= t.fs_m AS in_failsafe,
               wraparound_runway._vacuum_seconds(
                   greatest(t.heap_pages - coalesce(t.all_frozen, 0), 0), t.index_pages,
                   t.cost_limit, t.cost_delay_ms, p_io_pages_per_sec, true)  AS sec_thr,
               wraparound_runway._vacuum_seconds(
                   greatest(t.heap_pages - coalesce(t.all_frozen, 0), 0), t.index_pages,
                   t.cost_limit, t.cost_delay_ms, p_io_pages_per_sec, false) AS sec_unthr,
               greatest(
                   wraparound_runway._rank(wraparound_runway._grade(
                       t.xid_age,  t.fma,  t.fs_x,
                       wraparound_runway._eta(t.xid_age, t.fs_x, t.rate_x),
                       wraparound_runway._eta(t.xid_age, t.stop_x, t.rate_x),
                       p_crit_runway, p_warn_runway)),
                   wraparound_runway._rank(wraparound_runway._grade(
                       t.mxid_age, t.mfma, t.fs_m,
                       wraparound_runway._eta(t.mxid_age, t.fs_m, t.rate_m),
                       wraparound_runway._eta(t.mxid_age, t.stop_m, t.rate_m),
                       p_crit_runway, p_warn_runway))) AS base_rank
        FROM t
    ),
    s AS (
        SELECT e.*,
               CASE WHEN isfinite(e.e_st) THEN extract(epoch FROM e.e_st - now()) END AS to_stop,
               CASE WHEN isfinite(e.e_fs) THEN extract(epoch FROM e.e_fs - now()) END AS to_fs,
               -- Once past the failsafe, autovacuum drops the cost delay and skips index work.
               CASE WHEN e.in_failsafe THEN e.scan / p_io_pages_per_sec ELSE e.sec_thr END AS sec_eff,
               -- A vacuum that finished after the table crossed its freeze max age
               -- should have reset the age. If the age is still past it, something
               -- held the horizon back during that vacuum.
               coalesce(e.rate_x > 0 AND e.xid_age >= e.fma AND e.vacuum_running IS NULL
                        AND e.last_vacuum > now() - make_interval(secs => ((e.xid_age - e.fma) / e.rate_x)::double precision),
                        false) AS pinned
        FROM e
    ),
    r AS (
        SELECT s.*,
               greatest(
                   s.base_rank,
                   CASE WHEN s.sec_unthr > s.to_stop                            THEN 3
                        WHEN NOT s.in_failsafe AND s.sec_thr > s.to_fs          THEN 2
                        WHEN s.other_temp AND (s.xid_age >= s.fma OR s.mxid_age >= s.mfma) THEN 2
                        ELSE 0 END) AS rk
        FROM s
    )
    SELECT CASE r.rk WHEN 3 THEN 'critical' WHEN 2 THEN 'warning' WHEN 1 THEN 'notice' ELSE 'ok' END,
           r.relation, r.relkind, r.xid_age, r.mxid_age, r.fma, r.mfma,
           pg_size_pretty(r.heap_pages * current_setting('block_size')::bigint),
           r.scan,
           CASE WHEN r.vm_exact THEN 'exact, from pg_visibility'
                ELSE format('upper bound, %s pages already all visible', r.all_visible) END,
           wraparound_runway._secs(r.sec_eff),
           wraparound_runway._secs(r.sec_unthr),
           r.e_av, r.e_fs, r.e_st,
           r.vacuum_running, r.last_vacuum,
           nullif(concat_ws(' ',
               CASE WHEN r.other_temp THEN
                   'Temporary table of another session: autovacuum cannot touch it and only the owning session can vacuum or drop it. Ending that session removes it.' END,
               CASE WHEN r.sec_unthr > r.to_stop THEN
                   'Even an unthrottled VACUUM is estimated to finish after the stop limit. Start it now with INDEX_CLEANUP OFF and clear horizon blockers first.'
                    WHEN NOT r.in_failsafe AND r.sec_thr > r.to_fs THEN
                   'At the current autovacuum cost limit the freeze will not finish before the failsafe trips. Run a manual VACUUM, which is unthrottled by default.' END,
               CASE WHEN r.mxid_age::numeric / nullif(r.mfma, 0) > r.xid_age::numeric / nullif(r.fma, 0)
                     AND r.mxid_age >= r.mfma THEN
                   'Multixact age is the binding limit: look for SELECT ... FOR SHARE or FOR KEY SHARE on hot rows and foreign keys to this table.' END,
               CASE WHEN r.av_disabled AND (r.xid_age >= r.fma OR r.mxid_age >= r.mfma) THEN
                   'autovacuum_enabled is off for this table; anti wraparound vacuums still run, nothing else does.' END,
               CASE WHEN r.vacuum_running IS NOT NULL THEN
                   'A vacuum is already running on it: let it finish rather than starting a second one.' END,
               CASE WHEN r.pinned THEN
                   format('Vacuumed at %s, after it crossed its freeze max age, and the age did not drop: the xmin horizon is pinned. Clear horizon_blockers() first or the next vacuum is wasted.',
                          date_trunc('second', r.last_vacuum))
                    WHEN r.rk >= 1 AND r.vacuum_running IS NULL AND NOT r.other_temp
                     AND r.e_av <= now() THEN
                   'Anti wraparound vacuum is due but not running: check autovacuum_max_workers saturation and horizon_blockers().' END
           ), ''),
           CASE WHEN r.other_temp THEN NULL
                WHEN r.rk >= 3 THEN format('VACUUM (FREEZE, VERBOSE, INDEX_CLEANUP OFF) %s;', r.vacuum_target)
                WHEN r.rk >= 1 THEN format('VACUUM (FREEZE, VERBOSE) %s;', r.vacuum_target)
           END
    FROM r
    ORDER BY r.rk DESC, r.e_st ASC NULLS LAST,
             greatest(r.xid_age::numeric / nullif(r.fma, 0), r.mxid_age::numeric / nullif(r.mfma, 0)) DESC,
             r.relation
$fn$;

-- ---------------------------------------------------------------------------
-- Anti wraparound storm forecast for the current database
-- ---------------------------------------------------------------------------
-- Tables loaded together freeze together. This buckets every table by when it
-- crosses its freeze max age, prices the vacuum work and runs a queue through
-- the buckets so you can see autovacuum fall behind before it does.

CREATE FUNCTION wraparound_runway.storm_forecast(
    p_window           interval DEFAULT interval '24 hours',
    p_basis            text     DEFAULT 'average',
    p_horizon          interval DEFAULT interval '7 days',
    p_bucket           interval DEFAULT interval '1 hour',
    p_io_pages_per_sec numeric  DEFAULT 25000,
    p_crit_backlog     interval DEFAULT interval '6 hours'
)
RETURNS TABLE (
    severity      text,
    bucket_start  timestamptz,
    bucket_end    timestamptz,
    tables_due    int,
    overdue       int,
    heap_due      text,
    work          interval,
    capacity      interval,
    utilization   numeric,
    backlog_after interval,
    biggest       text
)
LANGUAGE plpgsql VOLATILE
AS $fn$
#variable_conflict use_column
DECLARE
    v_rx       numeric;
    v_rm       numeric;
    v_bucket   numeric := extract(epoch FROM p_bucket);
    v_horizon  numeric := extract(epoch FROM p_horizon);
    v_limit    numeric := coalesce(nullif(wraparound_runway._setting('autovacuum_vacuum_cost_limit'), -1),
                                   wraparound_runway._setting('vacuum_cost_limit'));
    v_delay    numeric := coalesce(nullif(wraparound_runway._setting('autovacuum_vacuum_cost_delay'), -1),
                                   wraparound_runway._setting('vacuum_cost_delay'));
    v_workers  numeric := wraparound_runway._setting('autovacuum_max_workers');
    v_cap      numeric;
    v_backlog  numeric := 0;
    v_prev     bigint  := -1;
    v_util     numeric;
    b          record;
BEGIN
    IF v_bucket <= 0 OR v_horizon <= 0 THEN
        RAISE EXCEPTION 'p_bucket and p_horizon must be positive';
    END IF;

    SELECT max(r.rate) FILTER (WHERE r.kind = 'xid'), max(r.rate) FILTER (WHERE r.kind = 'mxid')
    INTO v_rx, v_rm
    FROM wraparound_runway._planning_rate(p_window, p_basis) r;

    IF v_rx IS NULL AND v_rm IS NULL THEN
        RAISE NOTICE 'storm_forecast: no consumption rate yet, schedule take_sample() first';
        RETURN;
    END IF;

    -- The cost limit is shared and balanced across workers, so a throttled
    -- autovacuum delivers one cost limit worth of work per second in total.
    -- Unthrottled, each worker runs at the I/O rate you pass in.
    v_cap := v_bucket * CASE WHEN v_delay > 0 AND v_limit > 0 THEN 1 ELSE v_workers END;

    FOR b IN
        WITH f AS (
            SELECT tf.*,
                   least(
                       CASE WHEN tf.xid_age  >= tf.fma  THEN 0
                            WHEN v_rx > 0 THEN (tf.fma  - tf.xid_age)  / v_rx END,
                       CASE WHEN tf.mxid_age >= tf.mfma THEN 0
                            WHEN v_rm > 0 THEN (tf.mfma - tf.mxid_age) / v_rm END) AS due_in,
                   wraparound_runway._vacuum_seconds(
                       tf.heap_pages, tf.index_pages, v_limit, v_delay, p_io_pages_per_sec, true) AS w
            FROM wraparound_runway._table_facts(NULL, false) tf
            WHERE NOT tf.other_temp
        ),
        g AS (
            SELECT f.*,
                   floor(f.due_in / v_bucket)::bigint AS idx,
                   row_number() OVER (PARTITION BY floor(f.due_in / v_bucket)
                                      ORDER BY f.w DESC, f.relation) AS rn
            FROM f
            WHERE f.due_in IS NOT NULL AND f.due_in < v_horizon
        )
        SELECT g.idx,
               count(*)::int AS n,
               count(*) FILTER (WHERE g.due_in = 0)::int AS n_over,
               sum(g.heap_pages) AS pages,
               sum(g.w) AS w,
               string_agg(format('%s (%s)', g.relation, wraparound_runway._secs(g.w)), ', ' ORDER BY g.w DESC)
                   FILTER (WHERE g.rn <= 3) AS top3
        FROM g
        GROUP BY g.idx
        ORDER BY g.idx
    LOOP
        -- Empty buckets between this one and the last drain the queue.
        IF v_prev >= 0 THEN
            v_backlog := greatest(0, v_backlog - (b.idx - v_prev - 1) * v_cap);
        END IF;
        v_backlog := greatest(0, v_backlog + b.w - v_cap);
        v_util    := round(b.w / v_cap, 2);
        v_prev    := b.idx;

        severity      := CASE WHEN v_backlog >= extract(epoch FROM p_crit_backlog) THEN 'critical'
                              WHEN v_backlog > 0 OR v_util > 0.8                 THEN 'warning'
                              WHEN b.n_over > 0                                   THEN 'notice'
                              ELSE 'ok' END;
        bucket_start  := now() + make_interval(secs => (b.idx * v_bucket)::double precision);
        bucket_end    := bucket_start + p_bucket;
        tables_due    := b.n;
        overdue       := b.n_over;
        heap_due      := pg_size_pretty((b.pages * current_setting('block_size')::bigint)::bigint);
        work          := wraparound_runway._secs(b.w);
        capacity      := wraparound_runway._secs(v_cap);
        utilization   := v_util;
        backlog_after := wraparound_runway._secs(v_backlog);
        biggest       := b.top3;
        RETURN NEXT;
    END LOOP;
END
$fn$;

-- ---------------------------------------------------------------------------
-- One ranked report and a monitoring check
-- ---------------------------------------------------------------------------

CREATE FUNCTION wraparound_runway.report(
    p_window     interval DEFAULT interval '24 hours',
    p_basis      text     DEFAULT 'average',
    p_include_ok boolean  DEFAULT false
)
RETURNS TABLE (
    severity   text,
    area       text,
    subject    text,
    finding    text,
    action_sql text
)
LANGUAGE sql VOLATILE
AS $fn$
    WITH rate AS (
        SELECT * FROM wraparound_runway.consumption_rate(p_window)
    ),
    lim AS (
        SELECT * FROM wraparound_runway.limits()
    ),
    blockers AS (
        SELECT * FROM wraparound_runway.horizon_blockers()
    ),
    items AS (
        SELECT CASE WHEN r.source = 'none' THEN 'warning' ELSE 'ok' END AS sev,
               'rate'::text AS area,
               r.kind AS subj,
               CASE WHEN r.source = 'none' THEN r.note
                    ELSE format('%s per second average, %s peak, from %s over %s (%s points)%s',
                                r.avg_per_sec, coalesce(r.peak_per_sec::text, 'unknown'), r.source,
                                date_trunc('second', r.span), r.points,
                                coalesce('. ' || r.note, '')) END AS finding,
               NULL::text AS act,
               true AS always
        FROM rate r

        UNION ALL
        SELECT 'notice', 'sampler', 'wraparound_runway.sample',
               CASE WHEN max(s.taken_at) IS NULL THEN 'No samples stored. Schedule take_sample() every 5 to 15 minutes for a trend.'
                    ELSE format('Last sample was %s ago; the schedule looks stopped.', date_trunc('second', now() - max(s.taken_at))) END,
               NULL, false
        FROM wraparound_runway.sample s
        HAVING max(s.taken_at) IS NULL OR max(s.taken_at) < now() - interval '1 hour'

        UNION ALL
        SELECT 'critical', 'settings', 'track_counts',
               'track_counts is off, so autovacuum cannot run here at all.',
               'ALTER SYSTEM SET track_counts = on; SELECT pg_reload_conf();', false
        WHERE NOT current_setting('track_counts')::boolean

        UNION ALL
        SELECT 'warning', 'settings', 'autovacuum',
               'autovacuum is off. Anti wraparound vacuums are still forced, but nothing keeps ages low before then.',
               'ALTER SYSTEM SET autovacuum = on; SELECT pg_reload_conf();', false
        WHERE NOT current_setting('autovacuum')::boolean

        UNION ALL
        SELECT 'warning', 'settings', 'autovacuum_' || CASE l.kind WHEN 'xid' THEN '' ELSE 'multixact_' END || 'freeze_max_age',
               format('%s leaves only %s IDs between the first forced vacuum and the stop limit.',
                      l.freeze_max_age, l.stop_age - l.freeze_max_age),
               NULL, false
        FROM lim l
        WHERE l.freeze_max_age > 1000000000

        UNION ALL
        SELECT c.severity, 'cluster', format('%s in %s', c.kind, c.datname),
               format('age %s (%s%% of stop limit), autovacuum due %s, failsafe %s, stop %s, stop at peak rate %s. %s',
                      c.current_age, c.pct_of_stop,
                      coalesce(c.eta_autovacuum::text, 'unknown'), coalesce(c.eta_failsafe::text, 'unknown'),
                      coalesce(c.eta_stop::text, 'unknown'), coalesce(c.eta_stop_at_peak::text, 'unknown'),
                      coalesce(c.advice, '')),
               NULL, false
        FROM wraparound_runway.cluster_runway(p_window, p_basis) c

        UNION ALL
        SELECT b.severity, 'blocker', b.kind || ' ' || b.name,
               format('holds the xmin horizon %s transactions back for %s. %s', b.xmin_age, b.affects, b.detail),
               b.action_sql, false
        FROM blockers b

        UNION ALL
        SELECT 'critical', 'blocker', 'freeze floor',
               format('The oldest horizon is %s transactions old, past autovacuum_freeze_max_age %s. Anti wraparound vacuums will run, finish and start again without lowering ages until that blocker is gone.',
                      max(b.xmin_age), (SELECT l.freeze_max_age FROM lim l WHERE l.kind = 'xid')),
               NULL, false
        FROM blockers b
        HAVING max(b.xmin_age) >= (SELECT l.freeze_max_age FROM lim l WHERE l.kind = 'xid')

        UNION ALL
        SELECT t.severity, 'table', t.relation,
               format('xid age %s of %s, mxid age %s of %s, %s to scan (%s), vacuum about %s throttled or %s flat out, autovacuum due %s, stop %s.%s',
                      t.xid_age, t.freeze_max_age, t.mxid_age, t.mxid_freeze_max_age,
                      t.heap_size, t.scan_estimate, t.est_vacuum_throttled, t.est_vacuum_unthrottled,
                      coalesce(t.eta_autovacuum::text, 'unknown'), coalesce(t.eta_stop::text, 'unknown'),
                      coalesce(' ' || t.advice, '')),
               t.action_sql, false
        FROM wraparound_runway.table_runway(p_window, p_basis) t

        UNION ALL
        SELECT f.severity, 'storm', format('%s to %s', f.bucket_start, f.bucket_end),
               format('%s tables (%s, %s overdue) need %s of vacuum against %s of capacity, backlog after %s. Largest: %s',
                      f.tables_due, f.heap_due, f.overdue, f.work, f.capacity, f.backlog_after, f.biggest),
               NULL, false
        FROM wraparound_runway.storm_forecast(p_window, p_basis) f
        WHERE f.severity IN ('warning', 'critical')
    )
    SELECT i.sev, i.area, i.subj, i.finding, i.act
    FROM items i
    WHERE i.always OR p_include_ok OR i.sev <> 'ok'
    ORDER BY wraparound_runway._rank(i.sev) DESC,
             CASE i.area WHEN 'rate' THEN 0 WHEN 'settings' THEN 1 WHEN 'sampler' THEN 2 WHEN 'cluster' THEN 3
                         WHEN 'blocker' THEN 4 WHEN 'table' THEN 5 ELSE 6 END,
             i.subj
$fn$;

-- 0 ok, 1 warning, 2 critical. Notices do not page anyone.
CREATE FUNCTION wraparound_runway.check(
    p_window interval DEFAULT interval '24 hours',
    p_basis  text     DEFAULT 'average'
)
RETURNS int
LANGUAGE sql VOLATILE
AS $$
    SELECT coalesce(max(CASE r.severity WHEN 'critical' THEN 2 WHEN 'warning' THEN 1 ELSE 0 END), 0)
    FROM wraparound_runway.report(p_window, p_basis) r
$$;

-- ---------------------------------------------------------------------------
-- Privileges: owner plus pg_monitor, nobody else
-- ---------------------------------------------------------------------------

REVOKE ALL ON SCHEMA wraparound_runway FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA wraparound_runway FROM PUBLIC;
GRANT USAGE ON SCHEMA wraparound_runway TO pg_monitor;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA wraparound_runway TO pg_monitor;
GRANT SELECT, INSERT, DELETE ON wraparound_runway.sample TO pg_monitor;

COMMIT;
