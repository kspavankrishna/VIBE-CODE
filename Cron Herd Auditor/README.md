# Cron Herd Auditor

A cron auditor written in Raku that reads your crontabs and Kubernetes CronJob manifests, expands every schedule and shows you the exact minutes where too many jobs start at once. It also catches the schedules that do not mean what they look like, and it prints a stagger plan you can paste back into your files.

**Language:** Raku | **Lines:** 509 | **Added:** 2026-10-17

## What this solves

Cron problems are rarely about one job. They show up when forty jobs written by eleven people all say `0 * * * *` and your database, your API gateway or your cluster autoscaler takes the hit at exactly minute zero of every hour. A thundering herd of scheduled jobs looks like a mystery latency spike in your graphs. Nobody wrote a bad job. Everybody picked the same round number.

The second problem is that a cron expression can be valid and still wrong. These are the ones I see again and again:

- `0 3 13 * 5` looks like "3am on Friday the 13th". In classic cron it fires on every 13th of the month and also on every Friday, because when both day of month and day of week are restricted the match is an OR.
- `*/7 * * * *` looks like "every 7 minutes". The minute counter resets each hour, so the real gaps are 7 minutes seven times and then 4 minutes. If your job takes 5 minutes you get overlapping runs once an hour and no one can explain why.
- `0 0 31 * *` silently skips February, April, June, September and November.
- `0 0 31 2 *` never runs at all and cron will not warn you.
- `30 2 * * *` in a local time zone is skipped on the spring forward night and can run twice on the fall back night.
- `5/15 * * * *` is read as 5-59/15 by some daemons and rejected by others.
- Quartz style tokens like `?`, `L` and `W` get pasted from Java examples into crontabs and Kubernetes manifests where they are simply invalid.

Cron Herd Auditor finds all of these from files on disk. It needs no cluster access, no running daemon and no network.

## Why I built it

I wanted a check I could run in CI against a repo full of manifests and crontabs, before a deploy, that answers two plain questions. What is the worst minute of the week for this fleet. And does every schedule in here do what its author thinks it does.

Most cron tools I found do one of two things. They explain a single expression in English, which does not help when the problem is the interaction between a hundred expressions. Or they are a full scheduler, which is the wrong place to put an audit. I wanted something in between: a read only tool that counts. I picked Raku because grammar style parsing and set operations are short to write in it, and because the tool is a single file with no dependencies beyond the compiler. You can copy it into a repo and run it.

## When to use it

- In a pull request check on a repo that holds Kubernetes manifests with CronJob objects.
- Before you migrate a pile of crontabs from one host to a shared scheduler, where the herd gets added up for the first time.
- After an incident where something spiked "on the hour" and you want proof of how many jobs share that minute.
- When you inherit a server and want to know which entries are dead, duplicated or fragile.
- When you want a quick set of new minute offsets for jobs that all sit at minute zero.

Do not use it as a replacement for a real time zone database check. More on that in the notes.

## How it works

**Parsing.** `parse-field` takes one field and a field kind (`minute`, `hour`, `dom`, `month` or `dow`) and returns the sorted set of values it selects. The ranges come from the `%SPEC` constant, and month and day names (JAN to DEC, SUN to SAT) are resolved by `resolve-value`. It handles lists, ranges, `*`, steps like `*/15` and `10-40/5`, and maps day of week 7 to 0. A range that wraps around such as `22-2` is rejected with a clear message, because cron does not support it and people expect it to. `quartz-token` detects `?`, `#`, `L` and `W` forms and rejects them with an explanation. Names such as JUL and WED are not mistaken for Quartz tokens.

The `@hourly`, `@daily`, `@midnight`, `@weekly`, `@monthly`, `@yearly` and `@annually` aliases live in the `%ALIASES` constant. `@reboot` is skipped because it has no schedule.

**Readers.** `read-crontab` skips comments, blank lines and `NAME=value` environment lines. With `--system` it drops the user column that `/etc/crontab` carries after the five fields. `read-k8s` splits a manifest on `---`, keeps only documents with `kind: CronJob`, and pulls the name, `schedule` and `timeZone` lines. It does not need a YAML library because it only reads those three keys. A line that cannot be read becomes a parse error with its file and line number instead of crashing the run.

**Static checks.** `static-checks` runs rules that do not need the expansion:

- `DOM_DOW_OR` when both day fields are restricted. The classic cron rule is applied: if either day field starts with `*` the match is an AND, otherwise it is an OR.
- `NEVER_FIRES` when no selected month contains any selected day. The table `@DAYS-IN-MONTH` treats February as 29 days, so a job on 29 February reports `LEAP_ONLY` instead.
- `SKIPS_MONTHS` when the highest day is above 28 and some selected months are shorter.
- `DST_WINDOW` when the hour set is not every hour and contains an hour from `@DST-HOURS` (1, 2 or 3), in any zone other than UTC. Zones in `@UTC-ZONES` are exempt. On Kubernetes a CronJob without `timeZone` is treated as the cluster default, which is assumed to be UTC, so it is not flagged. A crontab line is reported as "host local".
- `UNEVEN_STEP` for minute and hour steps that do not divide the field, such as `*/7` or `*/45`, and for steps larger than the field.
- `AMBIGUOUS_STEP` for forms like `5/15`.

**Expansion.** `expand-fires` walks each day of the horizon from `--from` (default today in UTC), tests month, day of month and day of week with the AND or OR rule, and records every minute as an integer offset from the start of the window. Storing integers keeps a `* * * * *` job at about ten thousand numbers for a week, which is fine. `min-gap` then finds the smallest distance between two runs, and `OVERLAP_RISK` is raised when it is below `--max-runtime`.

**Herd detection.** Jobs are grouped by time zone, since minutes in different zones cannot be added together without a time zone database. `analyse-group` builds a load table from minute offset to job count. Any minute at or above `--herd` jobs is a herd. A herd is HIGH when it reaches twice the limit and MEDIUM otherwise. The `TOP_OF_HOUR` finding fires when at least `--herd` jobs sit on minute zero and they make up 40 percent or more of the group.

**Stagger plan.** For jobs whose minute field is exactly `0`, `analyse-group` removes the job from the load table and tries every shift from 0 up to `--max-shift` (default 20, capped at 59). It scores a shift by the worst minute that job would land on and breaks ties with the sum of squared loads, then puts the job back at the best shift. Jobs are processed in name order so the plan is deterministic and the same files always give the same advice. The plan prints the old and new schedule for each moved job, and the peak before and after. Shifts stay inside the same hour, so a daily job stays daily and an hourly job stays hourly.

**Output.** The default is a readable report. `--json` prints one JSON document through `json-of` with the window, parse errors, global findings, per job findings and per group herds and plans. `--fail-on` sets the severity that turns the exit code into 1.

## Usage

The entry point is the `MAIN` sub in `CronHerdAuditor.raku`. You need Rakudo installed.

```
raku CronHerdAuditor.raku [options] <file>...
```

Examples:

```
raku CronHerdAuditor.raku /etc/crontab --system
raku CronHerdAuditor.raku deploy/cronjobs.yaml deploy/more-jobs.yaml --herd=4
raku CronHerdAuditor.raku crontab.txt --days=8 --max-runtime=10 --json
crontab -l | raku CronHerdAuditor.raku - --fail-on=MEDIUM
```

Options:

| Option | Meaning |
|---|---|
| `--days=N` | Days to expand, default 7, maximum 400. Use 8 or more to see weekly jobs twice. |
| `--from=YYYY-MM-DD` | First day of the window in UTC. Default is today. |
| `--herd=N` | Jobs on one minute that count as a herd, default 5. |
| `--max-runtime=M` | Flag jobs whose smallest gap between runs is under M minutes. |
| `--max-shift=M` | Largest minute offset the stagger plan may use, default 20. |
| `--system` | Crontab files have a user column after the schedule. |
| `--json` | Machine readable output. |
| `--fail-on=LEVEL` | INFO, LOW, MEDIUM or HIGH. Default HIGH. |

Exit codes: 0 means clean at the chosen level, 1 means findings at or above `--fail-on` (any parse error counts as HIGH), 2 means a usage or file error.

A file is read as a Kubernetes manifest when its name ends in `.yaml` or `.yml` or when it contains a `kind: CronJob` line. Everything else is read as a crontab. Pass `-` to read from standard input.

## Notes

- **Time zones.** Raku core has no time zone database, so the tool does not convert between zones. It groups jobs by their declared zone and adds up minutes within a group only. The DST check is a flag on the hour range, not a calculation of the exact transition dates for that zone. Treat it as a prompt to look, not a verdict.
- **Cluster default zone.** Kubernetes CronJobs without `timeZone` run in the zone of the controller manager. The tool assumes that is UTC. If your cluster is different, set `timeZone` explicitly, which is better practice anyway.
- **Window limits.** Yearly and monthly jobs may not fire inside a 7 day window. That gives an `INFO` finding called `NO_RUN_IN_WINDOW`, not an error. Raise `--days` if you want to see them.
- **Not covered.** `@every` intervals, Quartz syntax and non standard extensions are rejected on purpose. The `@reboot` alias is ignored. Daemon specific behaviour such as anacron catch up runs, `systemd` timers and `startingDeadlineSeconds` is out of scope.
- **Dialect.** The day of month and day of week rule follows Vixie style cron, which is what most Linux crontabs and Kubernetes use. Some cron implementations treat it differently, so check yours if the result matters.
- **YAML reading.** `read-k8s` is a line reader, not a YAML parser. It handles normal manifests, including multi document files and quoted schedules with trailing comments. Helm templates with template expressions in the schedule line will not parse and are reported as errors, so render them first with `helm template`.
- **Stagger plan.** It only moves jobs whose minute field is exactly `0`, and it is a greedy plan, not an optimum. It is meant to flatten the obvious pile, not to solve a scheduling puzzle. Always read the proposed lines before committing them, since a job may have a real reason to start at the top of the hour.
