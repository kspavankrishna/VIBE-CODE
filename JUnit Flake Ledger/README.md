# JUnit Flake Ledger

A flaky test and a broken test both turn your build red, and most CI setups treat them the same way. This is a single Groovy file that reads JUnit XML from many runs, keeps a ledger of what each test has done over time, and tells you which failures are real.

**Language:** Groovy | **Lines:** 926 | **Added:** 2026-10-18

## What this solves

Every team with a decent sized test suite ends up with the same argument. A build goes red. Someone says it is the flaky one again. Someone else says it might be a real regression. Both are guessing, because nothing in the pipeline remembers what happened on the last fifty runs.

The usual fixes are poor. Blind retries hide flakes without ever counting them. A manual skip list rots because nobody owns it. The tools that do track flakiness are mostly hosted dashboards that need a vendor account and a network call from CI. And the simple scripts that count "failed then passed" miss the more important distinction: a test that fails three commits in a row is not flaky, it is broken, and quarantining it would be a mistake.

JUnit Flake Ledger answers one question per test: given everything we have seen, is this failure a coin flip or a signal? It keeps a small JSON ledger in your repo or CI cache, updates it on every run and gates the build on the answer.

It handles the report shapes that actually show up. Surefire writes flakyFailure and rerunFailure elements. The Gradle test retry plugin writes the same test several times in one file. Some runners write truncated XML when the job is killed. All of that is covered, and the failure modes are explicit instead of silent.

## Why I built it

I kept seeing the same pattern in CI reviews. A team would add retries, the pass rate would go up and the real flake count would be invisible. Then a genuine regression would land, get retried three times, fail three times, and still be dismissed as flaky because the team had learned not to trust red.

The core idea here is that the strongest evidence of flakiness is disagreement on the same commit. If a test fails and passes on identical code, the code did not cause the failure. A flip across different commits is weak evidence, because the code changed in between and the flip may be a real break followed by a real fix. So the ledger groups observations by commit and treats those two kinds of evidence very differently.

I also wanted it to fail loudly when the input is wrong. A green build that parsed zero tests is the worst outcome a test gate can produce. A half written report that hides a failing class is nearly as bad. So the tool refuses to continue on those, exits with its own code and leaves the ledger alone.

Groovy was the right choice because Gradle shops and Jenkins shops already have it on the machine. It is one file, no build step and the only dependency is the Groovy runtime with its JSON module.

## When to use it

- Your CI already produces JUnit XML (Surefire, Gradle, pytest with junitxml, Jest with a JUnit reporter, Go with go-junit-report and so on) and you want to know which tests to quarantine.
- You run a test step more than once per commit, through a retry plugin or a rerun job, and you want that data to mean something.
- You want a gate that blocks real failures but does not block on a test your own history says fails by chance.
- You want a list of quarantined tests to feed into your runner as an exclusion file.
- You want the quarantine to lift itself after a stretch of clean passes, so the list does not grow forever.
- You are losing trust in red builds and need numbers to argue for fixing the worst offenders first.

It is not a replacement for fixing flaky tests. It tells you which ones to fix and keeps them from blocking everyone in the meantime.

## How it works

The entry point is `Main.main`, which calls `execute`. That parses arguments with `Config.parse`, takes an advisory file lock next to the ledger so two CI jobs cannot overwrite each other, loads the ledger, runs `Engine.run`, saves the ledger and prints the report.

**Parsing.** `ReportParser.parse` uses the StAX streaming reader with DTD support and external entities switched off, so a hostile report cannot pull in local files and a large report does not sit in memory. It reads each testcase and looks only at direct children. A failure or error element means the case failed. A flakyFailure or flakyError element, which is how Surefire records a pass after retries, becomes an outcome of R. A skipped case is dropped. If a file ends in the middle of a testcase it is marked unreadable, and its partial results are thrown away.

**Folding retries.** `ReportParser.fold` collapses repeated ids inside one run. If the same test appears as fail then pass, the run records R. All failures record F. All passes record P. With `--distinct-duplicates` the repeats are kept as separate tests instead, which is the right choice if your framework reuses names for parameterised cases.

**Idempotency.** `ReportParser.fingerprint` hashes the commit label plus every file path and content hash. The ledger remembers recent fingerprints, so re running the same CI step does not count the same evidence twice.

**The ledger.** `Ledger` stores, per test, a capped list of observations: commit, outcome, a sequence number and a timestamp. `Ledger.save` writes a temp file and moves it into place so a crash cannot leave half a ledger. A ledger that is not valid JSON, or has a different schema number, stops the run with exit 2 instead of being overwritten.

**Classification.** `Classifier.analyse` groups observations by commit. A commit where one test both passed and failed, or where it needed a retry, counts as mixed. Then it applies these rules in order:

1. BROKEN: it failed on `--broken-streak` commits in a row (default 3). This wins even over flaky history, because a test that fails that consistently is not helped by quarantine.
2. FLAKY: at least `--min-flaky-evidence` mixed commits (default 2).
3. SUSPECT: one mixed commit, or at least `--flip-limit` pass and fail flips across commits over eight or more commits.
4. REGRESSION: it is failing now after passing before.
5. HEALTHY: everything else.

`Classifier.wilson` gives a 95 percent Wilson interval on the attempt level failure rate, so a test that failed once in four attempts is not reported with the same confidence as one that failed 20 times in 80.

**Quarantine with hysteresis.** `Engine.run` quarantines a test the first time it classifies as FLAKY. Release needs `--release-streak` consecutive clean passes (default 20). On release the test's `floorSeq` is set to the current sequence number, so old mixed commits stop counting and the test is not instantly re quarantined by its own history. A quarantined test that turns BROKEN is still blocking. Quarantine never hides a consistent failure.

**The gate.** Only the head commit, the last `--run`, is judged. A test blocks the build only if it failed in every attempt at head and is not an excused quarantined flake. If any attempt at head passed, the test goes in the "flaked at head" list and does not block. The report also prints a flake tax: the chance that a full run goes red from flaky tests alone, worked out as one minus the product of each flaky test's pass probability.

**Coverage checks.** `Engine.run` compares the tests seen at head against the tests seen on the previous commit. If more than `--max-vanish-ratio` have disappeared (default 25 percent) it warns about a missing shard, and `--strict-coverage` turns that into exit 4.

**Output.** `Renderer.render` produces text, json, markdown or github. The github format emits workflow commands with the required escaping, and the markdown format is meant for a job summary.

## Usage

Run it with the Groovy runtime. The last `--run` is the commit being judged.

```
groovy JUnitFlakeLedger.groovy \
  --ledger ci/flake-ledger.json \
  --run 9f3a2c1=build/test-results/test \
  --format markdown \
  --out flake-summary.md \
  --quarantine-out ci/quarantine.txt
```

To compare two attempts of the same commit, give the same label twice:

```
groovy JUnitFlakeLedger.groovy --ledger ci/flake-ledger.json \
  --run 9f3a2c1=attempt1/reports \
  --run 9f3a2c1=attempt2/reports
```

Options that change behaviour: `--window`, `--min-flaky-evidence`, `--broken-streak`, `--release-streak`, `--flip-limit`, `--expire-days`, `--max-vanish-ratio`, `--strict-coverage`, `--distinct-duplicates`, `--fail-on-new-flaky`, `--allow-unreadable`, `--now` and `--no-write`. Run with `--help` for the full list.

Exit codes:

| Code | Meaning |
|---|---|
| 0 | Clean |
| 1 | A blocking failure, or a new quarantine when `--fail-on-new-flaky` is set |
| 2 | Bad arguments, a corrupt ledger or a locked ledger |
| 3 | Report integrity: no tests parsed, a missing report or an unreadable file |
| 4 | Test coverage dropped past the limit with `--strict-coverage` |

You need the Groovy runtime and the groovy-json module on the classpath. A normal Groovy install has both.

## Notes

- **Commit order is ingest order.** The ledger treats commits in the order it first saw them. If you feed it several branches, interleaved commits create flips that look like noise. Mixed commit evidence is unaffected, but for the cleanest signal keep one ledger per long lived branch.
- **Retry duplicates versus parameterised tests.** Repeated ids in one run are treated as retries by default. If your framework gives parameterised cases the same name, use `--distinct-duplicates`.
- **Sharded runs.** Pass every shard in one invocation. If you ingest shards separately the coverage check compares a partial suite to the previous full one and will warn.
- **A rerun can rescue a red head.** If you pass two attempts for the head commit and one passes, the test is listed as flaked at head and does not block. That matches how most teams treat a green rerun, but it is a policy choice, so know it is there.
- **Lock scope.** The lock is advisory and works between processes sharing one filesystem. It will not protect a ledger on two machines writing to the same network share without lock support.
- **Not a flaky test fixer.** It does not read stack traces or guess root causes. The ranked watchlist tells you where to spend the time.
- **Tested against** synthetic Surefire style reports covering flake, break, regression, quarantine release, truncated XML, an external entity attempt and a corrupt ledger.
