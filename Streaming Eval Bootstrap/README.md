# Streaming Eval Bootstrap

You changed a prompt, a reranker or a tool planner, the eval suite says the mean score went up by 0.4 points, and nobody can tell you whether that is a real improvement or noise. This is a single file Scala CLI that reads paired eval rows from stdin and answers that with a deterministic paired bootstrap, a 95 percent confidence interval, a regression risk number and a nonzero exit code when the gate fails.

**Language:** Scala | **Lines:** 130 | **Added:** 2026-05-24

## What this solves

The April 2026 evaluation shipping problem: LLM apps, coding agents, retrievers, rerankers and tool planners need to prove a candidate is better than baseline while eval rows are still streaming out of CI. A single average hides paired variance. Teams need a reproducible regression risk number before switching traffic.

Here is the concrete failure mode. Your harness prints one number per run, mean score. Candidate scores 0.812, baseline scores 0.806, someone calls it a win and merges. Two weeks later a regression shows up on a slice nobody was watching. What actually happened is that 200 of your 300 eval rows moved by fractions of a point in random directions, 4 rows moved a lot, and those 4 rows drove the entire delta. The mean was real arithmetic and a meaningless signal. A bootstrap interval on the same data straddles zero and would have said so before the merge.

The mirror image costs just as much. A genuine improvement gets blocked because a reviewer sees six rows that got worse and nobody can say how likely a real regression is, so the argument is settled by whoever is more senior. Once people stop believing the eval numbers they stop running them.

Then there is nondeterminism. If your significance check reseeds from the clock, the same PR gives a different verdict on rerun and a flaky gate gets disabled within a week. This tool fixes the seed by default, so the same CSV and the same flags produce the same interval in every CI run and on every laptop.

## Why I built it

Every statistics stack that does this well is heavy. Pulling in R, SciPy or a JVM statistics library for one bootstrap interval means a dependency, a build step and a container image in a service repo that has none of those. Teams skip the analysis rather than pay that. The naive inline version, a t test on unpaired means, is wrong here, because eval rows are paired by construction: the same prompt, the same task, scored twice.

So this is one Scala source file with nothing past the standard library. It handles the paired structure correctly, ships its own random number generator so results do not shift with the JDK, and exits with a status code CI already understands. Fork it, read all 130 lines in a sitting, change the percentiles if you want something other than 95 percent.

## When to use it

- A pull request changes a prompt, a retrieval config or a model version and the merge should block automatically if the lift could plausibly be zero.
- Human graders or an LLM judge scored the same 500 tasks under two systems and you want the honest interval, not the headline average.
- You are weighting eval rows by traffic share or task importance and a plain mean would over count rare cases.
- A nightly job pipes a CSV of retrieval metrics into a gate and you want a stable pass or fail without a Python environment on the runner.
- Someone asks in review how confident you are, and you want to answer with a win probability instead of an opinion.
- You need the same verdict reproduced next quarter from the same input file, for an audit or a postmortem.

## How it works

Input is CSV on stdin, parsed by `readSamples()` into an `Iterator[PairSample]`. Each row is `id,baseline,candidate` with an optional fourth `weight` column defaulting to 1.0. Blank lines are dropped and any line starting with `id,` is skipped as a header, so a file with or without one works. Rows split with `line.split(",", -1)` so trailing empty cells survive, and `number()` wraps the parse in a `Try` to name the offending line. Fewer than three cells is a hard error.

The per row quantity is `PairSample.delta`, defined as `(candidate - baseline) * weight`. That is the paired difference, which is the whole point: variance common to both systems on a given row cancels before any averaging. The reported point estimate comes from `mean(data)`, the weighted mean `sum(delta) / sum(weight)` over the real rows, not from the bootstrap distribution.

`bootstrap()` is the core. It draws `options.samples` replicates, 2000 by default. Each replicate resamples the dataset with replacement to its original size, accumulating `sum` of deltas and `weight` of weights, then divides to get that replicate's weighted mean lift. Randomness comes from `XorShift64`, a 64 bit xorshift generator with the standard 13, 7, 17 shift triple, masked nonnegative with `x & Long.MaxValue`. It is seeded from `--seed`, default 17, and a zero seed falls back to the constant 88172645463393265 so the generator never sticks at zero. Using an in file PRNG instead of `scala.util.Random` is what makes output identical across JVMs and Scala versions.

Three counters accumulate while the replicates run. `regressions` increments when a replicate mean falls below `minLift`, `wins` when it is strictly positive, `ties` when it is exactly zero. Divided by the replicate count they become `regressionRisk`, `pWin` and `pTie`. Read the semantics carefully: regression risk is measured against your `--min-lift` threshold, not against zero, so at the default `minLift` of 0.0 it is the bootstrap probability that the true lift is negative.

The replicate means are sorted in place with `scala.util.Sorting.quickSort` and `pct()` reads the 2.5th and 97.5th percentiles with linear interpolation between neighbouring ranks. That is a plain percentile bootstrap, not BCa and not studentized: simple, no bias correction, good enough when the statistic is roughly symmetric, which a mean of paired differences usually is.

Output goes through `renderText` by default, one line of `key=value` pairs that greps cleanly, or `renderMarkdown` under `--markdown` for pasting into a PR comment. The gate is the last thing `main` does: if `regressionRisk > maxRegressionRisk` or `meanLift < minLift` the process exits 2, otherwise 0. Any thrown exception is caught, printed to stderr with a `StreamingEvalBootstrap:` prefix, and exits 64.

## Usage

```bash
# scores.csv:
# id,baseline,candidate,weight
# task-001,0.71,0.79,1.0
# task-002,0.64,0.61,2.0
# task-003,0.88,0.90,

scala run StreamingEvalBootstrap.scala < scores.csv

# or compile once and pipe
scalac StreamingEvalBootstrap.scala
cat scores.csv | scala StreamingEvalBootstrap

# CI gate: 5000 replicates, require 1% lift, allow 2% regression risk
cat evals.csv | scala StreamingEvalBootstrap \
  --samples 5000 --seed 17 \
  --min-lift 0.01 --max-regression-risk 0.02

cat evals.csv | scala StreamingEvalBootstrap --markdown --samples 10000

echo $?   # 0 pass, 2 gate failed, 64 bad input or usage
```

Text output looks like:

```
rows=300 samples=5000 mean_lift=0.021400 ci95=[0.004100,0.038900] regression_risk=0.0112 win_probability=0.9888
```

## Notes

- Everything is read into memory as a `Vector` before resampling. Streaming means rows arriving on stdin from CI, not unbounded input.
- The interval is a percentile bootstrap with no bias correction or acceleration, so heavily skewed delta distributions give a slightly optimistic range.
- Index selection uses `nextPositiveLong() % data.length` and carries the usual modulo bias. Negligible at realistic dataset sizes, not corrected here.
- `regressionRisk` counts replicates below `--min-lift`, not below zero. Raising `--min-lift` raises the reported risk by definition, which is easy to misread on a dashboard.
- `pTie` is computed and carried in `Result` but neither renderer prints it. It is there if you want it.
- Argument parsing is a recursive match and each flag applies `copy` after parsing the tail, so a repeated flag resolves to the leftmost occurrence. Unknown flags throw and exit 64.
- Weights divide as well as multiply, so all zero weights yield 0.0 rather than a division by zero. Negative weights are accepted silently.
- No dependencies beyond `scala.io.Source`, `scala.util.Try` and `scala.util.Sorting`. No build file, no tests in this folder.
