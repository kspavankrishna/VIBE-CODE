-- PostgresRoleEscalationPathFinder.sql
--
-- Read only catalog audit. Creates nothing outside its own schema, writes
-- no data, drops nothing. Safe to run against a production primary or a
-- read replica by any role that can read pg_catalog.pg_auth_members.
--
-- Requires PostgreSQL 12 or newer. Tested on 12 through 16.

BEGIN;

CREATE SCHEMA IF NOT EXISTS role_escalation_audit;

COMMENT ON SCHEMA role_escalation_audit IS
  'Read only role-graph privilege escalation audit. Safe to drop with DROP SCHEMA role_escalation_audit CASCADE.';

-- ---------------------------------------------------------------------
-- 1. Sensitive role registry
--
-- Native superusers are always sensitive and are detected dynamically
-- from pg_roles.rolsuper, never from this table. This table only holds
-- the roles that behave like a superuser on managed platforms, where
-- rolsuper is deliberately false so the platform keeps control, plus
-- the built in predefined roles that grant broad read/write/exec power.
-- Add your own site specific roles (a "app_admin" that owns every
-- schema, a legacy "dba" role, etc) with INSERT ... ON CONFLICT.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS role_escalation_audit.sensitive_role_patterns (
  role_name text PRIMARY KEY,
  reason    text NOT NULL
);

INSERT INTO role_escalation_audit.sensitive_role_patterns (role_name, reason) VALUES
  ('rds_superuser',        'AWS RDS/Aurora superuser-equivalent (rolsuper is false by design on RDS)'),
  ('rds_replication',      'AWS RDS replication control role'),
  ('rdsadmin',             'AWS RDS internal service role'),
  ('azure_pg_admin',       'Azure Database for PostgreSQL superuser-equivalent'),
  ('azure_superuser',      'Azure Database for PostgreSQL legacy superuser-equivalent'),
  ('cloudsqlsuperuser',    'Google Cloud SQL superuser-equivalent'),
  ('cloudsqladmin',        'Google Cloud SQL internal service role'),
  ('alloydbsuperuser',     'Google AlloyDB superuser-equivalent'),
  ('alloydbadmin',         'Google AlloyDB internal service role'),
  ('pg_read_all_data',     'Predefined role: SELECT on every table, view and sequence in every database'),
  ('pg_write_all_data',    'Predefined role: INSERT/UPDATE/DELETE on every table in every database'),
  ('pg_execute_server_program', 'Predefined role: run arbitrary programs on the database host via COPY PROGRAM'),
  ('pg_read_server_files', 'Predefined role: read arbitrary files on the database host via COPY/pg_read_file'),
  ('pg_write_server_files','Predefined role: write arbitrary files on the database host via COPY')
ON CONFLICT (role_name) DO UPDATE SET reason = EXCLUDED.reason;

CREATE OR REPLACE VIEW role_escalation_audit.sensitive_roles AS
  SELECT rolname AS role_name, 'native superuser (rolsuper = true)' AS reason, 1 AS severity_rank
  FROM pg_catalog.pg_roles
  WHERE rolsuper
  UNION
  SELECT p.role_name, p.reason,
         CASE WHEN p.role_name IN ('pg_read_all_data', 'pg_write_all_data') THEN 3 ELSE 2 END
  FROM role_escalation_audit.sensitive_role_patterns p
  JOIN pg_catalog.pg_roles r ON r.rolname = p.role_name;

-- ---------------------------------------------------------------------
-- 2. Membership edges: every "member is granted parent" fact currently
-- recorded in pg_auth_members, resolved to role names, restricted to
-- grants that actually let the member act as the parent.
--
-- PostgreSQL 16 added inherit_option and set_option to pg_auth_members
-- (GRANT role TO role WITH INHERIT / WITH SET). A grant with
-- set_option = false blocks SET ROLE, so it cannot be used to assume
-- the parent's privileges even though the membership row exists. Older
-- servers have no such column and every membership always allows
-- SET ROLE, so the filter is skipped there.
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION role_escalation_audit.membership_edges()
RETURNS TABLE (
  member_role  text,
  parent_role  text,
  admin_option boolean,
  mechanism    text
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  has_set_option boolean;
  query text;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'pg_catalog'
      AND table_name = 'pg_auth_members'
      AND column_name = 'set_option'
  ) INTO has_set_option;

  IF has_set_option THEN
    query := $q$
      SELECT rm.rolname::text, rp.rolname::text, am.admin_option, 'membership'::text
      FROM pg_catalog.pg_auth_members am
      JOIN pg_catalog.pg_roles rp ON rp.oid = am.roleid
      JOIN pg_catalog.pg_roles rm ON rm.oid = am.member
      WHERE am.set_option IS NOT FALSE
    $q$;
  ELSE
    query := $q$
      SELECT rm.rolname::text, rp.rolname::text, am.admin_option, 'membership'::text
      FROM pg_catalog.pg_auth_members am
      JOIN pg_catalog.pg_roles rp ON rp.oid = am.roleid
      JOIN pg_catalog.pg_roles rm ON rm.oid = am.member
    $q$;
  END IF;

  RETURN QUERY EXECUTE query;
END;
$$;

-- ---------------------------------------------------------------------
-- 3. CREATEROLE wildcard edges (PostgreSQL < 16 only).
--
-- Before PostgreSQL 16, a non-superuser role with the CREATEROLE
-- attribute could GRANT or REVOKE membership in ANY role that is not
-- itself a superuser, including roles it had never been granted into,
-- roles it did not create and roles owned by someone else. In effect
-- CREATEROLE was "grant yourself into anything non-super" on every
-- server older than 16. PostgreSQL 16 closed this: CREATEROLE now only
-- lets you manage roles you administer, which membership_edges() above
-- already captures through the automatic admin option a creator gets
-- on the roles it creates.
--
-- This function is the reason this tool exists: on a pre-16 cluster,
-- and on the many RDS/Aurora, Cloud SQL and self-hosted forks that are
-- still on 12 through 15, grep-ing GRANT statements will never surface
-- this path because the escalation does not require a prior grant.
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION role_escalation_audit.createrole_wildcard_edges()
RETURNS TABLE (
  member_role  text,
  parent_role  text,
  admin_option boolean,
  mechanism    text
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  IF current_setting('server_version_num')::int >= 160000 THEN
    RETURN;
  END IF;

  RETURN QUERY
    SELECT creator.rolname::text, target.rolname::text, true, 'createrole_wildcard_pre_pg16'::text
    FROM pg_catalog.pg_roles creator
    CROSS JOIN pg_catalog.pg_roles target
    WHERE creator.rolcreaterole
      AND NOT creator.rolsuper
      AND NOT target.rolsuper
      AND target.oid <> creator.oid;
END;
$$;

CREATE OR REPLACE FUNCTION role_escalation_audit.all_edges()
RETURNS TABLE (
  member_role  text,
  parent_role  text,
  admin_option boolean,
  mechanism    text
)
LANGUAGE sql
STABLE
AS $$
  SELECT * FROM role_escalation_audit.membership_edges()
  UNION
  SELECT * FROM role_escalation_audit.createrole_wildcard_edges();
$$;

-- ---------------------------------------------------------------------
-- 4. Escalation path finder.
--
-- Walks all_edges() breadth-outward from every candidate starting role,
-- one SET ROLE hop at a time, and reports the shortest path to each
-- sensitive role it can reach. The path array doubles as the cycle
-- guard (a role already on the path is never revisited), which matters
-- because createrole_wildcard_edges() can and does produce cycles
-- between two roles that both hold CREATEROLE, something the ordinary
-- membership graph can never do since PostgreSQL itself refuses to
-- GRANT a role into a cycle.
--
-- only_login_roles = true (default) starts the walk from rolcanlogin
-- roles only, since those are the credentials an attacker can actually
-- authenticate as. Set it to false to also audit service roles that
-- exist purely to be SET ROLE'd into by application code.
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION role_escalation_audit.find_escalation_paths(
  only_login_roles boolean DEFAULT true,
  max_hops int DEFAULT 12
)
RETURNS TABLE (
  start_role      text,
  escalation_path text[],
  mechanisms      text[],
  target_role     text,
  target_reason   text,
  hop_count       int,
  severity        text
)
LANGUAGE sql
STABLE
AS $$
  WITH RECURSIVE start_roles AS (
    SELECT r.rolname
    FROM pg_catalog.pg_roles r
    WHERE NOT r.rolsuper
      AND (r.rolcanlogin OR NOT only_login_roles)
  ),
  walk AS (
    SELECT
      s.rolname                    AS start_role,
      s.rolname                    AS current_role,
      ARRAY[s.rolname]::text[]     AS path,
      ARRAY[]::text[]              AS mechanisms,
      0                            AS hops
    FROM start_roles s

    UNION ALL

    SELECT
      w.start_role,
      e.parent_role,
      w.path || e.parent_role,
      w.mechanisms || e.mechanism,
      w.hops + 1
    FROM walk w
    JOIN role_escalation_audit.all_edges() e ON e.member_role = w.current_role
    WHERE NOT e.parent_role = ANY (w.path)
      AND w.hops < max_hops
  ),
  reached AS (
    SELECT
      w.start_role,
      w.path,
      w.mechanisms,
      sr.role_name AS target_role,
      sr.reason    AS target_reason,
      sr.severity_rank,
      w.hops,
      row_number() OVER (
        PARTITION BY w.start_role, sr.role_name
        ORDER BY w.hops ASC
      ) AS rn
    FROM walk w
    JOIN role_escalation_audit.sensitive_roles sr ON sr.role_name = w.current_role
    WHERE w.hops > 0
  )
  SELECT
    start_role,
    path,
    mechanisms,
    target_role,
    target_reason,
    hops,
    CASE severity_rank
      WHEN 1 THEN 'CRITICAL'
      WHEN 2 THEN 'HIGH'
      ELSE 'MEDIUM'
    END
  FROM reached
  WHERE rn = 1
  ORDER BY severity_rank, start_role, target_role;
$$;

CREATE OR REPLACE VIEW role_escalation_audit.escalation_findings AS
  SELECT * FROM role_escalation_audit.find_escalation_paths();

-- ---------------------------------------------------------------------
-- 5. Lateral grant risk.
--
-- A role with admin_option on a sensitive role does not gain new
-- privileges itself (it may already have them through membership) but
-- it CAN add other roles to that sensitive role without anyone's
-- review. If that admin role's credentials leak, or the human behind
-- it goes rogue or makes a mistake in a hurry, a brand new backdoor
-- account can be seated in one GRANT with no code review and often no
-- audit log alert, because "granted a role" rarely fires the same
-- alarms as "granted a table permission". This view is the list of
-- accounts that hold that button.
-- ---------------------------------------------------------------------

CREATE OR REPLACE VIEW role_escalation_audit.lateral_grant_risk AS
  SELECT
    e.member_role  AS admin_role,
    e.parent_role  AS sensitive_role,
    sr.reason,
    e.mechanism
  FROM role_escalation_audit.all_edges() e
  JOIN role_escalation_audit.sensitive_roles sr ON sr.role_name = e.parent_role
  WHERE e.admin_option
  ORDER BY sr.severity_rank, e.parent_role, e.member_role;

-- ---------------------------------------------------------------------
-- 6. CI gate summary. A pipeline step can run:
--
--   psql -tAc "SELECT role_escalation_audit.critical_count()" ...
--
-- and fail the job on a non-zero result, the same shape as any other
-- SQL based CI gate in this series.
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION role_escalation_audit.critical_count()
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
  SELECT count(*) FROM role_escalation_audit.escalation_findings WHERE severity = 'CRITICAL';
$$;

CREATE OR REPLACE VIEW role_escalation_audit.summary AS
  SELECT severity, count(*) AS finding_count
  FROM role_escalation_audit.escalation_findings
  GROUP BY severity
  ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END;

COMMIT;
