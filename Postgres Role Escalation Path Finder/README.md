# Postgres Role Escalation Path Finder

Your access review looked at every `GRANT` statement in the migration history and signed off, but Postgres role membership is a graph, not a list, and a login role three hops from a predefined superuser role never shows up in a flat grant list. This is a read only SQL audit that walks the real role graph in `pg_catalog` and tells you exactly which login credentials can reach a superuser equivalent, and how.

**Language:** SQL | **Lines:** 322 | **Added:** 2026-09-25

## What this solves

Every Postgres access review I have seen starts and ends with `\du` or a query against `pg_roles`, which shows you the direct attributes and direct memberships of each role and nothing past that. Nobody actually walks the transitive closure by hand, because doing it correctly means knowing several things that are not written down anywhere near `pg_roles`:

First, role membership is transitive for the purpose of `SET ROLE`. If `app_readonly` is a member of `data_reader`, and `data_reader` is a member of `pg_read_all_data`, then `app_readonly` can read every table in every database on that server, two hops removed from anything a grep for `pg_read_all_data` would find.

Second, before Postgres 16, any non-superuser role with the `CREATEROLE` attribute could grant or revoke membership in any role that was not itself a superuser, including roles it had never been granted into and did not create. This was documented in a warning box in the Postgres manual for years and is exactly the kind of thing that reads as a footnote until you realize your CI deploy user has `CREATEROLE` so it can provision per branch database roles, and that same attribute means it can grant itself into `rds_superuser`, `pg_execute_server_program`, or any other role on the instance with a single `GRANT` it does not need existing membership to issue. Postgres 16 closed this specific hole, but a large share of production databases, and nearly every managed RDS, Aurora and Cloud SQL instance provisioned more than a year or two ago, are still running something older.

Third, on every major managed Postgres platform the actual `rolsuper` flag is false by design. AWS RDS gives you `rds_superuser`, Azure gives you `azure_pg_admin`, Google Cloud SQL gives you `cloudsqlsuperuser`. These roles carry effectively all the power a real superuser has over your data and your schema, but a query that only checks `WHERE rolsuper` will report a perfectly clean, superuser free database while missing every one of these paths.

This script builds the actual reachability graph from `pg_catalog.pg_auth_members` and `pg_catalog.pg_roles`, adds the pre-16 `CREATEROLE` wildcard edges when the connected server is old enough to have them, and returns, for every login role, the shortest path to every sensitive role it can reach and the exact mechanism that makes each hop possible.

## Why I built it

I went looking for a tool that would answer one specific question before a security review: "if this application's database credentials leak, what is the actual blast radius, following every `SET ROLE` a compromised session could make, not just the grants that role was handed directly." Nothing existing does this in plain SQL you can run against a database you do not control the extensions for. Vendor security scanners either need an agent installed or only look at the top level role list. So I wrote the graph walk as a set of catalog only functions and views, using a recursive CTE for the traversal and the role path array itself as the cycle guard, since I wanted something a security engineer or a database owner without shell access to the box can run in a single `psql -f` and immediately query.

The `CREATEROLE` wildcard case is the part I care about most, because it is invisible to every grant-log based tool. It does not require a `GRANT` to have been issued to the specific target role at all, the escalation exists purely because of an attribute on the attacker's own role plus the absence of a superuser flag on the target, so nothing in an audit log or a migration diff will ever surface it. I tested this exact scenario against a live Postgres 16 instance by proving the underlying query independently of the version gate, then confirmed it composes correctly: a `CREATEROLE` role that wildcard grants itself into an otherwise unrelated role which itself already had a legitimate two hop path to the real `postgres` superuser role, and the tool correctly reported that as a two hop `CRITICAL` finding with both mechanisms named in order.

## When to use it

- Before a SOC 2, ISO 27001 or internal access review of a Postgres or Postgres compatible database, to get an actual reachability report instead of a role list
- Right after inheriting a database you did not provision, especially one that has been running since before Postgres 16, to find `CREATEROLE` roles nobody remembers granting
- As a recurring CI or cron gate against a staging or production replica, failing the job when `role_escalation_audit.critical_count()` returns anything above zero
- When deciding whether an application's `CREATEROLE` attribute, often given so it can provision per-tenant schemas or per-branch test databases, is safe to keep on a specific server version
- Auditing a managed Postgres instance (RDS, Aurora, Cloud SQL, AlloyDB, Azure Database for PostgreSQL) where the platform's own superuser-equivalent role hides from a plain `rolsuper` check

## How it works

The script creates one schema, `role_escalation_audit`, and everything else lives inside it. Nothing is created outside that schema and nothing it creates writes to your data; every object it defines is either a read only view or a `STABLE` function over `pg_catalog`.

`sensitive_role_patterns` is a small seed table of known superuser-equivalent role names: the managed platform roles (`rds_superuser`, `azure_pg_admin`, `cloudsqlsuperuser`, `alloydbsuperuser` and their sibling service roles) plus the Postgres predefined roles that grant broad power on any server (`pg_read_all_data`, `pg_write_all_data`, `pg_execute_server_program`, `pg_read_server_files`, `pg_write_server_files`). You extend it with your own site specific roles through a plain `INSERT ... ON CONFLICT`. The `sensitive_roles` view unions that table with every role where `rolsuper` is true, read live off `pg_roles` on each query so it never goes stale.

`membership_edges()` reads `pg_auth_members` and resolves `roleid` and `member` to role names, keeping only rows that actually let the member assume the parent's privileges. On Postgres 16 and newer, a membership grant can be issued `WITH SET FALSE`, which permits inheritance but blocks `SET ROLE`; the function detects the `set_option` column through `information_schema.columns` and filters those rows out, because a role that cannot `SET ROLE` into its parent cannot use that membership to escalate. On older servers, where that column does not exist, every membership row is walkable.

`createrole_wildcard_edges()` is the part that does not exist anywhere else. It checks `current_setting('server_version_num')::int` and returns nothing at all on Postgres 16 and up. Below that version, it produces one synthetic edge from every non-superuser role with `rolcreaterole` set to every other non-superuser role on the server, matching the exact pre-16 `CREATEROLE` grant behavior documented in the Postgres manual. `all_edges()` unions the two edge sets into one graph.

`find_escalation_paths(only_login_roles, max_hops)` is a recursive CTE that starts from every non-superuser role (login roles only by default, since those are actual attacker entry points; pass `false` to also walk service roles meant only to be `SET ROLE`'d into) and follows `all_edges()` outward one hop at a time, appending each new role to a `path` array. A candidate hop is only taken if the target role is not already in that array, which is both the cycle guard and the shortest path filter, and matters specifically because the `CREATEROLE` wildcard edges can form real cycles between two roles that both hold the attribute, something the ordinary membership graph can never do since Postgres itself refuses to `GRANT` a role into a circular reference. Every walk that lands on a role present in `sensitive_roles` is kept, deduplicated to the shortest hop count per `(start_role, target_role)` pair with `row_number()`, and assigned a severity: `CRITICAL` for a path to a native superuser, `HIGH` for a managed platform superuser-equivalent, `MEDIUM` for a predefined data access role.

`escalation_findings` is a view over `find_escalation_paths()` with its defaults, `lateral_grant_risk` lists every role holding `admin_option` on a sensitive role, meaning it can add a new member to that sensitive role without anyone's review, and `critical_count()` returns a single integer meant for a CI gate. `summary` groups the findings by severity for a quick glance.

## Usage

Run the script once against the target database as a role that can read `pg_auth_members` in full, which means a superuser, `rds_superuser` on RDS or any role granted `pg_monitor` or an equivalent read on Postgres 14 and newer, since that version made the catalog readable to all roles:

```
psql -h your-host -U your-admin-user -d your-database -f PostgresRoleEscalationPathFinder.sql
```

Then query the findings directly:

```sql
SELECT start_role, target_role, severity, hop_count, escalation_path, mechanisms
FROM role_escalation_audit.escalation_findings
ORDER BY severity, start_role;

SELECT * FROM role_escalation_audit.lateral_grant_risk;

SELECT * FROM role_escalation_audit.summary;
```

For a CI gate that fails the pipeline on any critical finding:

```
psql -h your-host -U your-admin-user -d your-database -tAc \
  "SELECT role_escalation_audit.critical_count()" | grep -qx 0
```

To audit service roles as well as login roles, call the function directly with its second parameter:

```sql
SELECT * FROM role_escalation_audit.find_escalation_paths(false, 20);
```

Add your own sensitive roles before running the audit:

```sql
INSERT INTO role_escalation_audit.sensitive_role_patterns (role_name, reason)
VALUES ('app_schema_owner', 'owns every application schema, equivalent to a full data breach if reached');
```

The whole script wraps a single transaction and every object is created with `CREATE SCHEMA IF NOT EXISTS` and `CREATE OR REPLACE`, so re-running it against the same database is safe and picks up new roles created since the last run. To remove everything it created, run `DROP SCHEMA role_escalation_audit CASCADE;`.

## Notes

- This was written and tested against a live Postgres 16.13 instance, including a scenario with a real cycle formed by two `CREATEROLE` roles granting each other, which the path array cycle guard resolved instantly rather than looping.
- The `CREATEROLE` wildcard path only activates on servers reporting a version below 16.0. On 16 and newer it correctly returns zero rows, because the automatic admin option a role creator now gets on the roles it creates is already captured by the ordinary `membership_edges()` query, no synthetic edge needed.
- Role membership in Postgres is cluster wide, not per database. Run this once per cluster, against any database on it. The findings will be the same everywhere the role graph is visible, which is everywhere on a normal cluster.
- A membership grant issued `WITH SET FALSE` on Postgres 16 and newer is correctly excluded from the walkable graph, since it permits automatic privilege inheritance but blocks the explicit `SET ROLE` that a compromised session would otherwise use.
- `pg_auth_members` was only readable in full by superusers and role members before Postgres 14. On an older server, connect as a role with enough visibility, typically the platform's admin role, or the findings will silently undercount.
- This tool only reports what the role graph currently allows. It does not detect privilege escalation through function security definer bugs, extension installation, or filesystem access outside the role system, all of which are real but separate risk classes.
