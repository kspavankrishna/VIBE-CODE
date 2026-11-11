# Postgres Migration Lock Linter

A V command line linter that reads your PostgreSQL migration files, works out which lock every statement takes and how long it holds it, then fails CI before a migration freezes reads or writes on a production table. It follows locks across whole transactions, which is where most real migration outages come from.

**Language:** V | **Lines:** 2836 | **Added:** 2026-11-11

## What this solves

Most Postgres migration outages are not caused by one obviously bad statement. They come from statements that look harmless, run in a few milliseconds on a laptop and then stop a production table for minutes.

Some examples I have seen or cleaned up after:

- `ALTER TABLE users ADD COLUMN token uuid DEFAULT gen_random_uuid()` looks like the instant "fast default" that Postgres 11 added. It is not. The default is volatile, so Postgres rewrites every row while holding ACCESS EXCLUSIVE, and nobody can read or write `users` until the rewrite ends.
- `ALTER TABLE orders ALTER COLUMN id TYPE bigint` is the classic integer overflow fix. It rewrites the whole table and every index on it under the strongest lock there is.
- `ALTER TABLE accounts ALTER COLUMN email SET NOT NULL` scans the full table to prove there are no NULLs, again under ACCESS EXCLUSIVE.
- A one line `ALTER TABLE` with no `lock_timeout` waits in the lock queue behind a slow report query. While it waits, every new query on that table queues behind it. A change that needs one millisecond of lock time takes the site down for as long as the report runs.
- The "safe" pattern of `ADD CONSTRAINT ... NOT VALID` followed by `VALIDATE CONSTRAINT` is only safe when the two run in separate transactions. Most migration runners wrap the whole file in one transaction, so the ACCESS EXCLUSIVE lock from the first statement is still held during the validation scan. The code looks correct and the outage still happens.
- `CREATE INDEX CONCURRENTLY` inside a transaction block fails at deploy time, and `CREATE INDEX CONCURRENTLY IF NOT EXISTS` silently keeps an INVALID index after a failed build, so the retry reports success while the index is never used.

This linter catches all of those. It is a Postgres migration safety checker that simulates the lock manager statement by statement, so it can tell the difference between a lock that is brief and a lock that gets stretched across a slow step later in the same transaction.

## Why I built it

There are good tools in this space already. strong_migrations covers Rails, django-migration-linter covers Django and squawk is a solid SQL linter. What I kept missing was transaction awareness. Every check I saw looked at one statement on its own. The incidents I cared about happened because of what else was in the same transaction: a cheap `ADD COLUMN` on one table followed by a backfill `UPDATE`, or an index build after an `ALTER` that already holds ACCESS EXCLUSIVE.

I also wanted one tool that works for any runner. Flyway, Liquibase SQL changesets, golang-migrate, dbmate, sqitch, Atlas, Prisma migrate and raw `psql -f` all end up as plain SQL files, so a linter that reads SQL directly covers all of them.

The second thing I wanted was memory across files. If an earlier migration created `orders.amount` as `numeric(10,2)`, the linter should know that changing it to `numeric(12,2)` is free while changing it to `numeric(12,4)` rewrites the table. Passing the migration history oldest first gives it that schema knowledge.

I wrote it in V because the result is one small native binary with no runtime to install in a CI image, and the standard library is enough for everything here.

## When to use it

- As a required CI check on any pull request that adds files under `migrations/`, `db/migrate/` or similar.
- Before a large table change, to see the lock plan: which tables are locked, in which mode, from which line, and which slow steps run while those locks are held.
- When reviewing a migration written by someone else, or by an AI coding agent, where you want a second opinion on lock behaviour rather than syntax.
- When upgrading Postgres, by running the same files with a different `-pg` value. Several rules change between 11, 12 and 14.

It is not a replacement for testing a migration against a copy of production data. It does not know how many rows a table has. It assumes any table it did not see created in the same file already holds real data, which is the safe assumption for a migration.

## How it works

The program is a single file, `PostgresMigrationLockLinter.v`, in four layers.

**Tokenizer.** `lex_file` splits SQL into statements without being fooled by semicolons in the wrong places. It handles single quoted strings with doubled quotes, `E''` strings with backslash escapes, dollar quoted bodies with any tag, double quoted identifiers, nested `/* */` comments, `--` comments, psql meta commands and SQL standard `BEGIN ATOMIC ... END` function bodies. An unterminated string, comment or dollar quote stops the run with exit code 2 and the file and line. I made that a hard failure on purpose, because linting the rest of a file after a tokenizer error would report nonsense.

Comments are also read for directives through `parse_directive`. `-- migration-lint: ignore PGL001,PGL009 reason` suppresses those rules for the next statement. `-- migration-lint: no-transaction` marks a file that the runner executes without a wrapping transaction.

**Lock model.** Postgres has eight table lock modes, from `lm_access_share` to `lm_access_exclusive`. `conflict_mask` encodes the real conflict table from the Postgres documentation as bitmasks, and `conflicts` answers whether two modes block each other. `blocked_traffic` turns a mode into plain words such as "reads and writes" or "writes". `combine_modes` works out the effective mode when one transaction holds two different locks on the same table, for example SHARE plus SHARE UPDATE EXCLUSIVE behaves like SHARE ROW EXCLUSIVE.

**Transaction simulation.** `lint_file` decides how each file is wrapped. With `-wrap auto` a file runs as one runner transaction unless it contains its own `BEGIN`, which matches how most frameworks behave. `process` opens a single statement transaction for anything outside a block. `acquire` records every lock in `TxState.held` together with the statement that took it, and checks the effective `lock_timeout` from `SET`, `SET LOCAL`, `RESET` and `set_config` calls. `slow_step` is called for anything that scans or rewrites a table. If a write blocking lock from an earlier statement is still held at that point, it reports PGL007. `close_tx` reports transactions that hold ACCESS EXCLUSIVE on more than one existing table and stores a `TxSummary` for the lock plan.

**Statement analysis.** `dispatch` routes each statement. The interesting parts:

- `add_column` checks serial types, identity columns, stored generated columns and the default expression. `default_volatility` classifies functions against `volatile_funcs` and `stable_funcs`. Anything it does not know is reported as PGL014 rather than guessed. On Postgres before 11 any non null default is a rewrite.
- `alter_column` runs `type_change_verdict`, which knows the binary coercible changes Postgres skips: widening varchar, varchar to text, numeric precision increases with the same scale, cidr to inet and timestamp to timestamptz under UTC on 12 and later. Integer widening is called out by name because it is the most common rewrite of all.
- `SET NOT NULL` is allowed without a scan only when an earlier `CHECK (col IS NOT NULL)` was validated and the target is Postgres 12 or later. `not_null_check_column` recognises that pattern and `validate_constraint` records the proof.
- `add_constraint` separates CHECK, FOREIGN KEY, PRIMARY KEY, UNIQUE and EXCLUDE, honours `NOT VALID` and `USING INDEX`, and records foreign key targets so a later `VALIDATE` also locks the right referenced table.
- `create_index`, `handle_drop`, `handle_reindex`, `handle_refresh`, `handle_vacuum`, `handle_cluster` and `handle_truncate` cover the maintenance statements, including the partitioned table case where Postgres rejects `CREATE INDEX CONCURRENTLY` on the parent.
- `TableInfo` keeps column types, proven NOT NULL columns, pending checks and foreign keys across files, so later migrations are judged against what earlier ones built.

Output comes from `render_text`, `render_json` or `render_github`. The GitHub format prints workflow commands, so findings appear as annotations on the pull request diff.

## Usage

Build once:

```
v -prod -o pg-migration-lint PostgresMigrationLockLinter.v
```

Lint one file, or a whole directory in name order:

```
./pg-migration-lint db/migrations/20261111_add_user_token.sql
./pg-migration-lint -pg 15 db/migrations
```

Given this migration:

```sql
ALTER TABLE users ADD COLUMN api_token uuid DEFAULT gen_random_uuid();
CREATE INDEX users_email_idx ON users (email);
```

the linter reports PGL006 (no lock_timeout), PGL003 (volatile default rewrites `users`), PGL001 (index build blocks writes) and PGL007 (the index build runs while ACCESS EXCLUSIVE from line 1 is still held), followed by a lock plan for the transaction.

A version that passes looks like this, split into two files:

```sql
SET lock_timeout = '2s';
ALTER TABLE users ADD COLUMN api_token uuid;
ALTER TABLE users ALTER COLUMN api_token SET DEFAULT gen_random_uuid();
```

```sql
-- migration-lint: no-transaction
SET lock_timeout = '2s';
CREATE INDEX CONCURRENTLY users_email_idx ON users (email);
```

In GitHub Actions:

```
./pg-migration-lint -format github -fail-on error db/migrations
```

If your runner already sets a lock timeout for the migration role, tell the linter so PGL006 stays quiet:

```
./pg-migration-lint -assume-lock-timeout 3s -max-lock-timeout 5s db/migrations
```

Other flags: `-format json` for a single JSON object with `findings`, `transactions` and `summary`; `-wrap file` or `-wrap none` to force how files are wrapped; `-disable PGL009,PGL011` to turn rules off; `-rules` to print every rule; `-version`. Pass `-` to read from stdin. Exit code 0 means clean, 1 means findings at or above `-fail-on` and 2 means a usage, read or parse error.

## Notes

- Rules: PGL001 blocking index build, PGL002 statement not allowed in a transaction block, PGL003 table rewrite, PGL004 validating scan under lock, PGL005 constraint builds an index, PGL006 lock timeout, PGL007 lock held across a slow step, PGL008 several tables under ACCESS EXCLUSIVE, PGL009 backfill inside a migration, PGL010 concurrent index with IF NOT EXISTS, PGL011 breaking drop or rename, PGL012 NOT NULL column without default, PGL013 blocking maintenance, PGL014 unknown default volatility, PGL015 enum value used in the transaction that added it, PGL016 confusing transaction control, PGL017 concurrent index on a partitioned parent, PGL018 code the linter cannot see into.
- Pass migrations oldest first. Column type knowledge only exists for tables the linter has seen created. Without it, type changes are reported with a lower severity and a note that the old type is unknown.
- Tables created in the same file are treated as empty, so creating a table and indexing it straight away is not flagged.
- `DO` blocks, `CALL` and function bodies are not analysed. They show up as PGL018 info so a reviewer knows to read them.
- The linter does not connect to a database. It never sees row counts, existing indexes or the volatility of your own functions, which is why PGL014 asks you to check `pg_proc` rather than guessing.
- Ignore directives apply to one statement. Keep the reason on the same line so the next reviewer knows why the rule was waived.
- Written against the V 0.5 standard library with only `os` and `strings` imported. It reads files and prints a report, writes nothing and makes no network calls.
