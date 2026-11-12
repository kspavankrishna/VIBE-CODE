# Flame Graph Regression Attributor

Two flame graphs side by side tell you that something got slower, not which function did it or whether the difference is just sampling noise. This D command line tool diffs folded stack profiles, splits the change into call paths that add up exactly to the total and puts a false discovery rate on every row so CI can gate on it.

**Language:** D | **Lines:** 1896 | **Added:** 2026-11-12

## What this solves

A performance regression lands, someone captures a CPU profile before and after and then stares at two flame graphs trying to spot the wider box. The usual next step is a differential flame graph, which colours frames red or blue by how much their width changed. That picture has four problems that bite in practice.

First, inclusive time is counted again at every level of the stack. If a leaf function regresses by 8 points, every one of its callers up to `main` is also 8 points wider. A table sorted by inclusive delta is a list of the same regression repeated twenty times, with the real cause somewhere near the bottom.

Second, nothing tells you whether a delta is real. A profile with 5,000 samples has a lot of sampling noise. Samples taken a few milliseconds apart are also correlated, which makes the noise worse than a simple count suggests. A frame that moved from 1.0% to 1.4% may be nothing at all. With a few thousand frames in a profile, some of them will always look like they moved.

Third, the frame names do not line up between builds. A one line edit shifts every `file.py:123` suffix below it. Rust appends a different hash to each symbol on every build. The JVM numbers its lambdas differently on each run. `perf` adds `+0x1f` offsets. A naive diff sees every one of those as a function that disappeared and a brand new one that appeared.

Fourth, a compiler inlining decision moves time from a callee into its caller without changing the total. That shows up as one big red box and one big blue box. It is not a regression.

This tool handles all four. It normalises frame names so builds line up, builds one merged call tree for both sides, partitions the total change into rows that do not overlap, tests each row statistically, corrects for the number of rows tested and tags the rows that are only time moving between a caller and its callee.

## Why I built it

I kept running into the same review conversation. A benchmark is 6% slower, somebody attaches two SVG flame graphs and three people squint at them and each name a different function. Then someone reruns the benchmark and the picture changes, because half of what everyone was pointing at was noise.

The existing `difffolded.pl` gives a per stack diff meant for rendering, not a ranked answer. Profilers with a built in compare view mostly show inclusive deltas with no significance test. I wanted something that reads the folded stack format every profiler can already produce (`perf` through `stackcollapse-perf.pl`, async-profiler `collapsed` output, py-spy `--format raw`, rbspy, `cargo flamegraph`, bpftrace) and prints a short table I can paste into a pull request and trust.

I also wanted a CI gate that does not flap. A threshold on raw deltas fails on noise, so people raise the threshold until it catches nothing. A gate that requires both a minimum size and statistical significance is one you can leave switched on, more so when it tells you up front how small a shift it could have detected given the sample count.

D was a good fit: it compiles to one native binary with no runtime to install, the standard library already has the incomplete beta function needed for a Student t test and the built in `unittest` blocks keep the tests in the same file as the code.

## When to use it

- A benchmark or load test regressed and you have a profile from before and after.
- You want a CI step that fails a pull request when a function's cost rises by more than a set number of points, without failing on noise.
- You have several profile runs per side and want the comparison to use the run to run spread instead of pretending samples are independent.
- You are comparing allocation profiles, off CPU time or any other weighted folded stacks, not only CPU samples.
- You need to know which caller is responsible when a shared leaf such as `memcpy` or a hash function got more expensive.

It is not a profiler and it does not draw flame graphs. It reads folded stacks and writes a text or JSON report.

## How it works

**Reading and cleaning.** `parseFoldedLine` splits each line at the last space or tab, so frames that contain spaces such as `operator new` or `handler (app/views.py)` survive. `Ingestor.ingest` strips a byte order mark, skips blank lines and `#` comments, counts malformed lines instead of aborting and truncates stacks deeper than `MaxStackDepth`.

Each raw frame goes through `normalizeFrame` once and is then cached. By default it removes `+0x` offsets (`stripOffset`), maps bare addresses to `[unknown]` (`isBareAddress`), drops Rust symbol hashes (`stripRustHash`), masks embedded hex addresses and JVM lambda or accessor numbers (`maskHex`, `maskDigitsAfter`) and removes `:LINE` suffixes (`stripLineNumbers`). Optional steps are `stripAnnotation` for the `_[j]` style suffixes, `collapseGenericArgs` for template arguments and `mergeThreadName` for thread ids in the first frame. Frame filters use `wildcardMatch`, a small `*` and `?` matcher.

**One tree for both sides.** `Tree.addStack` inserts every stack into a single prefix tree. Each node keeps one self weight per input profile, so base runs and head runs share the same nodes. The tree is capped by `--max-nodes` (default `DefaultMaxNodes`) and exceeding it is a clear error, not a slow death by memory.

**Normalisation.** `analyze` fills a `Design` with one denominator per profile. In `share` mode the denominator is the profile's own total, so deltas are percentage points of the profile. In `per-op` mode it is the operation count you pass in, so deltas are percent of base cost per operation and a uniform slowdown is visible. In `absolute` mode it is 1.

**The test.** `Design.compare` returns base cost, head cost, delta and a p value. With three or more profiles per side (`MinWelchReplicates`) it runs a Welch t test across the runs through `studentTwoSided`, with a variance floor taken from sampling theory so three identical small counts cannot produce an absurdly small p value. With fewer runs it pools the counts and runs a two proportion z test in share mode or a Poisson rate z test otherwise, through `normalTwoSided`. Both count tests inflate the variance by a design effect, because stack samples are not independent. `estimateDesignEffect` measures that factor from replicate spread when at least two runs exist on a side. Without replicates the tool uses `DefaultDesignEffect`, which is 2. If you declare `--weights values` and give fewer than three runs per side, no test is possible and the report says so.

**Exact partition into call paths.** For every node the tool knows the delta of its subtree and the delta of its own self time. It first computes `maxAbs`, the largest delta found anywhere under each node, so a regression and an improvement that cancel out inside a parent are still found. A work list then walks from the root. At each node it emits a `self` row if the node's own time moved by at least `--min-delta`, visits every child whose subtree holds something that large and compares what is left over. A large leftover becomes a `subtree` row when nothing below is individually big enough. It becomes a `rest` row when some children were already split out. Small leftovers go into the unlisted remainder. Because every piece of weight lands in exactly one row or in the remainder, the rows plus the remainder equal the total change. No double counting.

**Shift detection.** A `self` row is tagged `[shift]` when its children moved the opposite way by at least `ShiftRatio` of its size. Any row is tagged when its direct caller's self time moved the opposite way by that much. That is the signature of inlining or outlining. It usually means there is nothing to fix.

**Multiple testing.** Only nodes and frames whose combined base and head cost reaches the threshold are tested, which is a filter that does not depend on the direction of the change. `benjaminiHochberg` then converts those p values to q values. The `q` column is the false discovery rate at which that row would be reported.

**Frame tables.** Self time per frame is also an exact partition of the total change, so the second table ranks frames by self delta and names the caller that contributed most. Inclusive time per frame counts each stack once per frame even under recursion, using a depth first walk that tracks which frames are already on the path.

**Power.** For count based tests the header prints the smallest shift the comparison could detect 80% of the time on a 1% frame and on a 10% frame. If that number is bigger than the regression you are hunting, collect more samples before trusting a clean report.

**Data quality.** Warnings are raised for malformed or truncated lines, for sides below `--min-samples`, for more than `UnknownShareWarn` of time in unresolved frames and for profiles whose frame names barely overlap (`SymbolOverlapWarn`), which usually means one side was not symbolised.

## Usage

Build it with any D compiler. With LDC:

```bash
ldc2 -O2 -release FlameGraphRegressionAttributor.d
```

Run the built in tests:

```bash
ldc2 -unittest -run FlameGraphRegressionAttributor.d
```

Compare two profiles by shape:

```bash
./FlameGraphRegressionAttributor --base before.folded --head after.folded
```

Compare cost per request with three runs per side and fail CI on a 3% total rise or a 1 point rise in any frame:

```bash
./FlameGraphRegressionAttributor \
  --base b1.folded --base b2.folded --base b3.folded \
  --head h1.folded --head h2.folded --head h3.folded \
  --base-ops 50000 --head-ops 50000 \
  --max-total-regression 3 --max-regression 1
```

Read one side from stdin, drop idle stacks and write JSON:

```bash
perf script | stackcollapse-perf.pl | ./FlameGraphRegressionAttributor \
  --base before.folded --head - --exclude-stack 'cpu_idle*' --json > report.json
```

Sample text output:

```
Call path culprits (rows of at least 0.50 pts, rows plus the unlisted remainder add up to the total change)
    delta    base    head       q  kind     path
    +8.35   48.97   57.32  <0.001  self     (3 frames);mod3::fn21 (src/f6.rs);mod4::fn28 (src/f9.rs);mod4::fn2 (src/f9.rs);mod6::fn15 (src/f5.rs);memcpy
    -7.06   46.24   39.18  <0.001  rest     main;serve
  unlisted remainder: -0.03 pts spread over paths below the threshold

Frames by self time change (self deltas across all frames add up to the total change)
    delta    base    head       q  paths  frame
   +10.33   60.34   70.67  <0.001    127  memcpy  <- 81% via mod6::fn15 (src/f5.rs)
```

All options:

| Option | Meaning |
|---|---|
| `--base FILE`, `--head FILE` | Folded stack files, repeatable. `-` reads one profile from stdin |
| `--mode share\|per-op\|absolute` | How weights are normalised. Defaults to `share`. Becomes `per-op` when ops are given |
| `--base-ops N[,N...]`, `--head-ops N[,N...]` | Operations behind each profile, one value or one per file |
| `--weights samples\|values` | Whether weights are sample counts or measured values |
| `--min-delta PTS` | Smallest change worth a row, default 0.5 |
| `--alpha Q` | False discovery rate, default 0.05 |
| `--design-effect auto\|N` | Variance inflation for correlated samples |
| `--min-samples N` | Warn below this many samples per side, default 2000 |
| `--collapse-recursion`, `--collapse-generics`, `--strip-annotations`, `--merge-threads` | Extra frame clean up |
| `--keep-lines`, `--keep-addresses` | Turn off the default clean up |
| `--skip-frame GLOB`, `--exclude-stack GLOB`, `--only-stack GLOB` | Frame and stack filters, repeatable |
| `--top N`, `--full-paths`, `--json` | Output control |
| `--max-regression PTS` | Exit 2 when a frame's inclusive cost rises by this much and the rise is significant |
| `--max-total-regression PCT` | Exit 2 when total cost rises by this much. Not valid in share mode |
| `--strict` | Exit 3 when there are warnings |
| `--max-nodes N` | Call tree size limit |

Exit codes: 0 ok, 1 usage or input error, 2 gate failed, 3 warnings under `--strict`.

## Notes

- Share mode only sees the shape of a profile. If everything got 10% slower evenly, nothing moves. If one subsystem got faster, every other share goes up. Pass `--base-ops` and `--head-ops` whenever you know how much work each profile covers. `--max-total-regression` is rejected in share mode for this reason.
- The default design effect of 2 is a guess. Real profilers on real machines often show more run to run spread than that. Two runs per side let the tool measure it. Three or more per side switch to the Welch test, which is the most trustworthy mode.
- The `rest` rows use a Bonferroni style correction because they are composed after the fact. Their q values are conservative.
- The `--max-regression` gate looks at inclusive deltas on purpose. An inlining change leaves the caller's inclusive time flat, so it does not trip the gate, while a shared leaf that got slower across fifty call sites does.
- Frames are split on `;`. Profiles that embed JVM type signatures such as `Ljava/lang/String;` inside frame names will be split in the wrong places. Turn signatures off in the profiler.
- Line number stripping removes a trailing `:digits` and never touches `::`. Use `--keep-lines` if your frames legitimately end that way.
- Stacks deeper than 4096 frames are truncated at the leaf end and counted in a warning. `--collapse-recursion` usually removes the need.
- Memory grows with the number of distinct call paths times the number of input files. A pair of 240 MB profiles with 7.5 million distinct paths took about 18 seconds on a cloud VM. The default node cap of 4 million is there so a runaway input fails fast with a message.
- Compressed input is not read directly. Pipe it: `zcat after.folded.gz | ./FlameGraphRegressionAttributor --base before.folded --head -`.
- The JSON report carries the schema id `flamegraph-regression-attribution/1` and uses `null` where a p or q value could not be computed.
