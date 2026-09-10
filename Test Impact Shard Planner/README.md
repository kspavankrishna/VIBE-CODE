# Test Impact Shard Planner

A monorepo has more tests than any pull request deserves, but "run only the tests near the diff" quietly skips the one shard that knew the blast radius. This is a single Go file that reads a manifest of tests plus a changed path list and decides which shards to run, in what order, inside a runtime budget, and tells you which changed files nobody is testing.

**Language:** Go | **Lines:** 1080 | **Added:** 2026-08-09

## What this solves

Test impact analysis fails in two directions and both cost real money. Run everything on every commit and CI minutes balloon, the queue backs up, and developers start merging on a green checkmark they stopped reading twenty minutes ago. Run only what the diff touches and you get the other failure: a change to `go.mod` looks like a one line edit to a naive path matcher, so the dependency scanner never fires and a vulnerable transitive package ships. A schema file, a generated client, a feature flag config or an edge worker entry point all look small in a diff and all have blast radius far outside their own directory.

Without something like this the selection logic ends up smeared across a Makefile, a few `if: contains(github.event...)` conditions in a workflow YAML and one Bash script nobody wants to touch. That logic is untestable, undocumented and invisible in review. When it drops a shard you find out in production, not in the pull request. The person who notices is usually on call at 2am, tracing an incident back to a merge whose CI was green because the relevant job was never scheduled.

The other quiet failure is budget. Selection without a cost model just runs whatever matched, so one 40 minute browser suite eats the pipeline while three cheap high signal unit shards sit unscheduled behind it. This planner makes the whole decision explicit: a JSON or Markdown report listing every selected test with the reason it was selected, every skipped test with the reason it was dropped, the changed paths no selected test covers, estimated wall clock time at your parallelism and a set of warnings. You can diff two plans across two commits and see exactly what changed.

## Why I built it

The good test impact tools are attached to build systems. Bazel knows the dependency graph so it can do this properly, but adopting Bazel to get selective testing is a migration measured in quarters. Hosted vendors will do it too, at a per seat price and with your test graph on their servers. In between there is nothing small, auditable and dependency free that takes "here are my tests, here are my changed paths, here is my budget" and returns a defensible plan.

So the manifest is the graph. It is coarser than a real build graph and honest about that, but it is a text file that lives in your repo, gets reviewed like code and encodes things a build graph cannot: flake rate, criticality, owner, shared resource contention and hand written risk rules for the cross cutting files that path matching always gets wrong.

## When to use it

- A monorepo where the full suite runs 45 minutes and most pull requests touch one service
- A pipeline that needs a hard CI minute budget per pull request but must never drop a security or migration gate
- Changes to `go.mod`, `package-lock.json`, a shared protobuf or a feature flag file that need to force specific shards regardless of directory
- Pull requests opened by coding agents, where you want the impact decision written down and reviewable rather than inferred
- A GitHub Actions workflow that needs a dynamically generated job matrix instead of a static list of hardcoded jobs
- Any pipeline where you want a merge gate that fails when a changed file has no test covering it at all

## How it works

Input arrives as a JSON manifest decoded with `DisallowUnknownFields`, so a typo in a key is a hard parse error rather than a silently ignored field. Changed paths come from the manifest itself or from `--changed`, and `loadChangedPaths` sniffs the first byte to accept three shapes: a JSON array, a JSON object carrying `changed_paths`, `files` or `paths`, or plain newline text with `#` comments stripped. That last form is what `git diff --name-only` gives you. `normalizeManifest` then validates and fills defaults: tier defaults to `blocking`, runtime defaults to 60 seconds, parallelism to 1, `min_risk` to 0.25. It rejects a timeout lower than the runtime, a flake rate outside 0 to 1, negative criticality, a risk rule with no weight and any glob that will not compile.

Path matching is a hand rolled glob compiled to a regex in `compilePattern`. A single `*` becomes `[^/]*` so it stops at a path separator, `**` becomes `.*` so it crosses directories, `?` becomes `[^/]`, and regex metacharacters are escaped. `matchPattern` short circuits before that: a pattern with no glob characters is treated as a literal file or a directory prefix, so `services/api` matches everything under it. Paths are normalized first, backslashes to forward slashes, leading `./` and `/` stripped, then `path.Clean`.

Scoring happens in `scoreCandidate`. Each distinct changed path a test matches contributes 2.5. Risk rules add their own weight when the rule pattern matches a changed path, and a rule scoped with `tests` only applies to those named tests. An unscoped rule fires only for a test that already matches the path or already has matches, which stops one broad rule from lighting up the entire suite. Tests that matched something also collect their `criticality` and a tier bonus: 1.25 for `security`, 0.9 for `migration`, 0.75 for `blocking`. Anything in `always_run` or carrying `must_run` gets `forced` and a flat +1000, which parks it at the top of the order and exempts it from the budget check. Every contribution writes a human readable string into the reasons set, so the report explains itself.

Selection is a greedy density ordered knapsack. Cost is `runtime_seconds * (1 + flake_rate * 2)`, so a flaky test is priced above its stopwatch time because a retry is a real cost. `priority` is risk divided by cost, and `Plan` sorts forced tests first then by that ratio, breaking ties on raw risk, then shorter runtime, then name, all with `sort.SliceStable` so the plan is deterministic. It walks that order accumulating runtime and drops anything that would push past `budget_seconds`, recording it as skipped with the reason.

Wall clock estimation in `estimateWallSeconds` is longest processing time first bin packing: sort the selected tests longest first, assign each to the least loaded worker and return the max worker load. That is the standard LPT greedy approximation for makespan, good enough to say whether a plan fits a time box. `uncoveredPaths` diffs the changed set against the union of matched paths on selected tests, and `buildWarnings` flags an empty plan, uncovered files, forced tests that already blew the budget, any high risk test dropped by the budget, and any resource named by two selected tests when parallelism is above 1, which is the shared database or shared staging collision.

Output is Markdown with escaped pipes and backticks, indented JSON of the full `PlanReport`, or a GitHub Actions `{"include": [...]}` matrix carrying name, command, tier, timeout in minutes, risk score, owners, resources and env per job.

## Usage

```bash
# build
go build -o shardplan TestImpactShardPlanner.go

# verify the planner logic with the built in fixture
./shardplan --self-test

# plan from a manifest plus a git diff, human readable
git diff --name-only origin/main... > changed.txt
./shardplan --manifest impact.json --changed changed.txt --format markdown

# generate a GitHub Actions job matrix
./shardplan --manifest impact.json --changed changed.txt --format gha-matrix > matrix.json

# pipe the manifest on stdin, cap the budget, gate the merge
cat impact.json | ./shardplan --changed changed.txt \
  --budget-seconds 600 --parallelism 4 --min-risk 1.0 \
  --format json --fail-on-uncovered
```

Manifest shape:

```json
{
  "budget_seconds": 480,
  "parallelism": 2,
  "min_risk": 1.0,
  "always_run": ["dependency-review"],
  "include_nonblocking": false,
  "tests": [
    {
      "name": "api-unit",
      "command": "go test ./services/api/...",
      "paths": ["services/api/**"],
      "runtime_seconds": 120,
      "timeout_seconds": 300,
      "flake_rate": 0.02,
      "criticality": 2,
      "tier": "blocking",
      "owners": ["platform"],
      "resources": ["postgres"],
      "env": {"GOFLAGS": "-count=1"},
      "must_run": false
    }
  ],
  "risk_rules": [
    {"pattern": "go.mod", "weight": 5, "reason": "dependency graph changed", "tests": ["dependency-review"]}
  ]
}
```

## Notes

- Exit codes: 0 on a rendered plan, 1 on any parse, validation or IO error, 2 when `--fail-on-uncovered` is set and uncovered changed paths remain. The report still prints on exit 2.
- It never executes a test. It plans and prints. Running the commands is your CI system's job.
- There is no dependency graph. Coverage is only as good as the `paths` globs and `risk_rules` you write, so an import added without a manifest update will not be picked up. `--fail-on-uncovered` exists to catch that manifest rot.
- Standard library only, no modules, no vendor SDK. It compiles with `go build` on a bare toolchain.
- Runtime and flake numbers are inputs you supply, not measurements. Feed them from CI history or they are guesses, and the ordering is only as honest as those numbers.
- `min_risk` filters optional tests only. Forced tests bypass both the risk floor and the budget, so a large `always_run` list can push total runtime past `budget_seconds`. That produces a warning, not an error.
- Nonblocking and advisory tier tests are excluded unless `--include-nonblocking` is passed or they are forced. Manifest parsing rejects unknown JSON fields, so a stale manifest fails loudly instead of ignoring a key.
