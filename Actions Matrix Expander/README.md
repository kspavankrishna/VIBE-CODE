# Actions Matrix Expander

A GitHub Actions matrix looks simple until include, exclude and a stray unquoted version number get involved. This is a single file ReScript CLI that expands `strategy.matrix` the way GitHub does, tells you exactly which jobs you will get and flags every entry that does not do what you meant.

**Language:** ReScript | **Lines:** 1009 | **Added:** 2026-10-29

## What this solves

You edit a CI matrix, push and wait. Twelve jobs show up when you expected nine. Or a job you needed never appears. Or the PR sits there forever because a required check name no longer exists. The only way to find out is to push and look at the Actions tab.

The reason is that matrix expansion has rules that are easy to get wrong in your head:

- The axes make a cartesian product first.
- `exclude` runs next and only sees the original axes. It cannot see keys that an `include` adds later.
- `exclude` is a partial match, so one entry can remove many combinations.
- `include` then runs in order. An entry is merged into every surviving combination where it does not change an original axis value. If it fits none of them it becomes a standalone job that holds only its own keys and inherits nothing from the axes.
- Values added by one `include` can be overwritten by a later one, so the order of the list changes the result.
- The whole thing fails above 256 jobs or when it expands to zero.

Then there are the quiet mistakes. In YAML an unquoted `3.10` is the number 3.1. A typo such as `pyton` in an exclude removes nothing and raises no error. A `"3.9"` string in an include does not match the number 3.9 in the axis. Each of these gives you a green workflow that does the wrong thing.

This tool runs the real expansion offline from your workflow file and reports all of it before you push.

## Why I built it

I kept losing time to matrix edits that were valid YAML and wrong behaviour. The worst one was a rename. We changed `node: [18, 20]` to `node: [20, 22]` and the repo had a branch rule that required the check called `test (ubuntu-latest, 18)`. Nothing failed. The check just never reported, and the PR waited. Nobody noticed for a day.

So I wanted two things. First, an expander that follows the documented include and exclude semantics exactly and says what it did. Second, a diff mode that compares the job names before and after a change and calls out the ones that disappeared, because those are the names branch protection cares about.

ReScript fits this well. The outcome of each stage is a variant. A job is `Expanded`, `Dynamic` or `Failed`. A finding is `SevError`, `SevWarning` or `SevInfo`. A combination remembers whether it came from `FromBase` or `FromInclude(n)`. The compiler makes sure every path deals with each case. There are no runtime dependencies. It compiles to one CommonJS file that runs on any recent Node.

## When to use it

- Before you merge a change to a build matrix, to see the exact job list.
- In a pull request check, with `diff`, to catch check names that vanish.
- When a matrix produces the wrong number of jobs and you cannot see why.
- When you want a cheap estimate of runner minutes by operating system before you add another axis.
- In CI as a lint step with `--strict`, so dead excludes and no op includes fail the build.

It is not a YAML parser and it does not call GitHub. It works on the JSON form of your workflow. It cannot expand a matrix that is built at run time with `fromJSON`. It will tell you that, and it will not guess.

## How it works

**Input.** Convert the workflow to JSON first, for example with `yq -o=json . .github/workflows/ci.yml`. `loadJson` reads the file with `JSON.parseOrThrow`. If the file has a `jobs` map, `expandWorkflow` walks each job that has `strategy.matrix`. If there is no `jobs` key the whole file is treated as one bare matrix, which is handy for quick experiments.

**Expansion.** `expandMatrix` does the work in the same order GitHub does.

1. It finds the axes: every key except `include` and `exclude`. Each axis must be a non empty list. An empty list is error M003 and a non list is M002.
2. It builds the cartesian product with the first axis varying slowest, which matches the order GitHub shows.
3. For each exclude entry it counts how many base combinations match using `jsonPartial`. A map value in an exclude is a partial match. Everything else uses `jsonEq`, which is strict about types. The entry is applied to the original axes only. If it matched nothing the tool says why through `describeMiss`: the key is not an axis, the value never appears, or the value has the wrong type.
4. For each include entry in order, it finds the surviving base combinations where every axis key in the entry equals the combination value. Those combinations get the extra keys. If none qualify the entry becomes a new job with only its own keys.
5. While merging it records which include set each key through the `setBy` dictionary, so it can report when a later include overwrites an earlier one (M009) or adds nothing at all (M007).
6. It checks the final count against `githubMatrixLimit` (256, or whatever `--max-jobs` says) and against zero.

**Findings.** Every finding has a stable code so you can grep for it or filter it.

| Code | Severity | Meaning |
|---|---|---|
| M001 | error | `strategy.matrix` is not a map |
| M002 | error | an axis is not a list |
| M003 | error | an axis is an empty list |
| M004 | error | the matrix is above the job limit |
| M005 | error | the matrix expands to zero jobs |
| M006 | warning | an exclude removes nothing, with the reason |
| M007 | warning | an include adds nothing |
| M008 | info | an include matches nothing and becomes its own job |
| M009 | warning | a later include overwrites a value set by an earlier one |
| M010 | warning | an include key looks like a typo of an axis |
| M011 | warning | likely unquoted version such as 3.10 read as 3.1 |
| M012 | warning | a type mismatch such as 3.9 against "3.9" |
| M013 | warning | the same value is listed twice in an axis |
| M014 | info | the matrix is an expression and cannot be expanded here |
| M015 | warning | identical jobs appear more than once after expansion |
| M016 | info | `max-parallel` is bigger than the job count |
| M018 | info | an exclude is fully covered by earlier excludes |
| M019 | error | an include or exclude is not a list of maps |

The typo check is `nearAxis`. It treats two keys as near if they match after lowercasing and removing dashes and underscores, or if their `levenshtein` distance is 1 and both are at least four characters long. The version check is `checkVersionFloats`. It fires when an axis holds a value like 3.1 beside 3.8 or 3.9 in the same major version, which is the signature of a truncated 3.10.

**Names and runners.** `renderName` builds the job name the way the check will appear. If the job has a `name` it substitutes the simple `${{ matrix.key }}` references. If not it uses the default form, the job id followed by the matrix values in parentheses. `runnerOf` resolves `runs-on` when it is a literal or exactly `${{ matrix.key }}`. `runnerFamily` buckets labels into linux, windows, macos or other.

**Minutes.** With `--minutes` and `--weight` the tool prints weighted minutes per job. You supply the multipliers. Nothing is hard coded, because runner pricing changes and I did not want a stale table in the code.

**Diff.** `diffWorkflows` expands both files and compares rendered names per job id. It prints removed names with `-` and added names with `+`. With `--fail-on-removed` it exits 1 if any name vanished.

**Exit codes.** 0 means clean. 1 means an error finding, or any warning when `--strict` is set, or a removed name with `--fail-on-removed`. 2 means bad usage or unreadable input. `verdict` and `setExitCode` handle this and the tool sets `process.exitCode` rather than calling `process.exit`, so large piped output is never cut off.

## Usage

Build once with the ReScript compiler. The folder has a `rescript.json` set up for in source CommonJS output.

```sh
npm install rescript
npx rescript build
```

Expand a workflow and list every job:

```sh
yq -o=json . .github/workflows/ci.yml > ci.json
node ActionsMatrixExpander.bs.js expand ci.json --list
```

Only one job, as JSON, with a custom limit:

```sh
node ActionsMatrixExpander.bs.js expand ci.json --job test --json --max-jobs 128
```

Fail CI on any warning and estimate cost with your own multipliers:

```sh
node ActionsMatrixExpander.bs.js expand ci.json --strict --minutes 12 --weight windows=2,macos=10
```

Compare two versions of a workflow and fail if a check name goes away:

```sh
git show origin/main:.github/workflows/ci.yml | yq -o=json . > before.json
yq -o=json . .github/workflows/ci.yml > after.json
node ActionsMatrixExpander.bs.js diff before.json after.json --fail-on-removed
```

Flags: `--job ID`, `--list`, `--json`, `--strict`, `--max-jobs N`, `--minutes M`, `--weight name=number,name=number`, `--fail-on-removed`. Run with a bad argument to print the usage text.

## Notes

- Matching is strict about types. The tool treats the number 3.9 and the string "3.9" as different and warns about it (M012). I did not want to guess how every corner of GitHub coerces them, so check your own workflow if you rely on that.
- Exclude entries use partial matching for nested maps. That follows the docs for the top level and is my reading for nested values.
- Key order in the output follows `JSON.parse`. JavaScript moves integer like keys to the front of an object, so a matrix with a key named `1` can list in a different order than the YAML.
- Display names for map or list values are shown as compact JSON. The exact text GitHub shows for those is not documented, so treat those names as approximate. Names built from strings and numbers are exact.
- Expression values such as `${{ fromJSON(needs.setup.outputs.matrix) }}` are reported as M014 and skipped. The tool never pretends to know them.
- The 256 job limit is the GitHub documented limit at the time of writing. Change it with `--max-jobs` if that changes.
- I checked the expansion against the include and exclude example in the GitHub documentation. The six jobs it produces match the six the docs list, in the same order.
