# Affected Target Planner

An F# command line tool that reads a monorepo target graph plus a list of changed files and tells your CI exactly which targets to build and test, in what order and on which shard. It fails loudly when a changed file belongs to nothing, which is the case most affected target scripts quietly get wrong.

**Language:** F# | **Lines:** 611 | **Added:** 2026-11-05

## What this solves

Every monorepo hits the same wall. The repo grows, CI takes forty minutes and someone says "we should only test what changed". So a script gets written. It runs `git diff --name-only`, maps folders to services and runs those. It works for a month.

Then the real problems show up:

- A change to a shared library should rebuild everything that depends on it, directly or through five other packages. The script only looks one hop.
- Someone edits the root lockfile or a CI workflow and nothing runs, because no folder owns that file. The pull request goes green with zero tests executed.
- Two targets claim the same file and nobody notices until one of them silently stops running.
- A dependency cycle sneaks in and the build tool prints an error three screens long that never names the loop.
- The affected list is correct but unordered, so a shard starts a test before the package it needs is built.
- A rebase or a vendored update touches 80,000 files and the glob matcher grinds for minutes.
- `git diff` quotes paths with unicode characters and the script treats `"caf\303\251.ts"` as a file that does not exist.

Affected Target Planner handles each of those on purpose. It is not a build system. It sits in front of whatever you already use (Bazel, Nx, Turborepo, Gradle, plain Make, shell scripts) and answers one question deterministically: given this diff, what has to run?

## Why I built it

I have seen the "green because nothing ran" failure more than once. A pipeline reports success, the dashboard looks healthy and weeks later someone finds that an entire directory was never tested after a config move. The cause is always the same: the selection logic had a default of "skip" when it did not understand a file.

I wanted the opposite default. If a changed file is not owned by any target and not covered by a global trigger, the tool should refuse to answer and exit with a distinct code. You then decide, in the command line, whether that means fail, run everything or ignore. The decision is explicit and written down in the pipeline file instead of hidden inside a script.

The second reason is ordering. Most affected tools give you a set. CI needs a schedule. Waves tell you what can run in parallel, the critical path tells you the floor on wall clock time and the shard plan tells you how long the whole thing will take with N runners. That lets you answer "is it worth paying for more runners" with a number.

F# fits because the problem is a graph walk with a handful of small record types and the .NET base library already has a good JSON reader and a regex engine with match timeouts. No NuGet packages are needed. It is one source file and one project file.

## When to use it

- Your CI runs everything on every pull request and you want a safe way to cut that down.
- You have an existing affected script and you do not trust it with lockfiles, CI config or shared code.
- You need a stable, reviewable schedule for a matrix job: one wave at a time, N shards each.
- You want to validate the graph itself. The cycle report and the unknown dependency report are useful even if you never shard anything.
- You maintain a build orchestrator and want a reference for how global triggers, ownership and ordering should interact.

It is a poor fit if your build tool already computes a precise action graph from file level inputs, as Bazel does with `bazel query`. Use that. This tool is for repos where the graph is declared by hand or generated coarsely, at package or service level.

## How it works

The code is organised top to bottom in the same order as the pipeline.

**Graph loading.** `parseGraph` reads the JSON with comments and trailing commas allowed. `parseTarget` and `readStrings` convert each entry and reject wrong types with the index of the offending target in the message. A target needs a `name`. It may have `srcs` (globs), `deps` (names of other targets) and `costSeconds`. If cost is missing it defaults to `DefaultCostSeconds` which is 30. Negative, infinite or non numeric costs are rejected. `validateGraph` then collects every structural problem in one pass: duplicate names, self dependencies, unknown dependencies and an empty graph. You get the whole list at once, not one error per run.

**Cycles.** `topoOrder` runs Kahn's algorithm with an ordinal sorted ready set, so the output is identical on every machine. If any node is left over, `findCycle` walks dependency edges inside the leftover set until a node repeats and returns the actual loop. The error reads like `Dependency cycle: //a -> //b -> //c -> //a` and the process exits with `ExitCycle` (4).

**Globs.** `globToRegex` converts a pattern to an anchored regex. `**/` spans any number of directories including none, `*` stays inside one path segment and `?` matches one non separator character. A pattern with no wildcard owns that exact path and everything under it, so `services/api` and `services/api/**` behave the same. A trailing slash is treated as `**`. Every regex gets a one second match timeout (`GlobTimeoutSeconds`) and `matchesAny` turns a timeout into a clear input error instead of a hang.

**Changed files.** `parseChanged` normalises each path through `normalizePath`: backslashes become slashes, a leading `./` goes and doubled slashes collapse. `isBadPath` rejects empty entries, quoted paths (with a hint to use `git diff -z` and `--null`), absolute paths and anything containing a `..` segment. Bad entries are reported together, capped at ten lines. Input can be newline separated or NUL separated.

**Ownership.** In `buildPlan`, each changed file is checked against the global trigger globs first. A match flips the plan to "run everything" and records which file did it. Then the file is matched against every target's `srcs`. Zero owners means the file is unowned. One owner marks that target as directly changed. Several owners mark all of them and also record the file in `MultiOwned`, so overlapping ownership is visible in the output instead of being a silent coin toss.

**Unowned policy.** The `--unowned` flag maps to the `UnownedPolicy` type. `fail` (the default) raises `ExitUnowned` (3) and lists up to twenty files. `all` treats the diff as unsafe and runs every target. `ignore` records the count in the reasons and carries on. There is also a size valve: if the number of changed files is above `--max-changed` (default `DefaultMaxChanged`, 50000) the plan runs everything, since a diff that size is a rebase or a vendor drop and file level precision is meaningless.

**Spreading the change.** Reverse edges are built once. A breadth first walk from the direct set follows them to collect every dependent, however deep. Targets never touched by the walk are reported as skipped.

**Waves and critical path.** Still in `buildPlan`, a single pass over the topological order assigns each affected target a level: zero if none of its dependencies are affected, otherwise one more than the highest affected dependency. The same pass keeps the longest cost chain and a predecessor pointer, so the critical path can be traced back from the most expensive finish.

**Sharding.** `assignShards` splits each wave across `--shards` workers using longest processing time first: sort by cost descending with a name tiebreak, then always drop the next target on the least loaded shard. The planned time is the sum of each wave's busiest shard, because waves are barriers. Comparing `plannedSeconds` against `criticalPath.seconds` shows how much room more runners would actually buy.

**Output.** `renderJson` writes a stable document with `schemaVersion` 1 through `Utf8JsonWriter`. `renderText` writes a short summary for humans. `run` ties it together and maps `PlanError` to an exit code: 0 success, 2 bad input, 3 unowned files, 4 cycle.

## Usage

Build it with the .NET 8 SDK:

```
dotnet build -c Release
```

A small graph in `graph.json`:

```json
{
  "globalTriggers": ["pnpm-lock.yaml", ".github/workflows/**", "tsconfig.base.json"],
  "targets": [
    { "name": "//core",   "srcs": ["packages/core/**"],              "costSeconds": 40 },
    { "name": "//ui",     "srcs": ["packages/ui/**"],   "deps": ["//core"], "costSeconds": 90 },
    { "name": "//api",    "srcs": ["services/api/**"],  "deps": ["//core"], "costSeconds": 150 },
    { "name": "//web",    "srcs": ["apps/web/**"],      "deps": ["//ui", "//api"], "costSeconds": 200 },
    { "name": "//docs",   "srcs": ["docs/**"],                       "costSeconds": 20 }
  ]
}
```

Feed it the diff straight from git, using NUL separation so odd file names survive:

```
git diff --name-only -z origin/main...HEAD | \
  dotnet run -c Release --project . -- --graph graph.json --changed - --null --shards 2
```

The `--changed` argument takes a file path or `-` for standard input. A change to `packages/core/index.ts` produces a JSON plan where `//core` is directly changed, `//ui`, `//api` and `//web` are affected, `//docs` is skipped, the waves run `//core`, then `//ui` with `//api` in parallel, then `//web`, and the critical path is `//core -> //api -> //web`.

For a quick look in a terminal:

```
dotnet run -c Release -- --graph graph.json --changed changed.txt --text
```

Flags that exist in the code: `--graph`, `--changed`, `--null`, `--shards`, `--unowned` (`fail`, `all` or `ignore`), `--max-changed`, `--text` and `--help`.

In CI, read the JSON with `jq` and feed the shards into a matrix:

```
plan=$(git diff --name-only -z origin/main...HEAD | dotnet run -c Release -- --graph graph.json --changed - --null --shards 4)
echo "$plan" | jq -c '.waves[] | {wave: .index, shards: .shards}'
```

Treat exit code 3 as "someone added a file nothing owns". The failure message names the files so the fix is a one line edit to the graph.

## Notes

- I could not run a compiler in the environment where I wrote this entry, so I reviewed the code by hand against the F# rules most likely to bite: no mutable locals captured by closures, no nested matches that swallow later cases and parenthesised calls on overloaded .NET methods. Build it once with `dotnet build` before wiring it into a pipeline and tell me if anything slips through.
- Ownership matching is a linear scan of changed files against target globs. For tens of thousands of files and a few hundred targets that is fine. If you have thousands of targets, index the literal prefixes first.
- Costs are estimates you supply. Feed them from your last run timings and the shard plan gets better each week.
- Globs are repo relative with forward slashes. There is no negation syntax. Express exclusions by splitting a target.
- Cross shard dependencies inside a wave cannot exist, since a dependency always sits in an earlier wave. Between waves you still need your artifacts or remote cache to hand build outputs forward.
- The tool does not touch the network, the repository or any file other than the two inputs.
- Output order is fully deterministic. Two runs on the same inputs give byte identical JSON, so you can cache or diff plans.
