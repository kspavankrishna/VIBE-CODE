# Paired Eval Gate

Your eval suite says the new model scores 0.71 and the old one scores 0.66, so you ship. That difference is often noise, and a plain average over an eval set with repeated runs, correlated task families and difficulty tiers will tell you it is real when it is not.

**Language:** Julia | **Lines:** 518 | **Added:** 2026-04-17

## What this solves

This solves paired LLM evaluation analysis, prompt rollout gating, judge model comparison and agent benchmark release decisions where naive averages give the wrong answer. Two systems get run over the same eval set. Someone computes a mean per system, subtracts, sees a positive number and calls it a win. That throws away the single most useful fact in the data: both systems saw the same items, so the per item difference has far less variance than either mean on its own. Unpaired analysis inflates your uncertainty when the change is genuinely good, and hides your uncertainty when your eval set is dominated by a handful of correlated task families.

The failure mode in production looks like this. You have 300 eval items. 120 of them come from one scraped support ticket corpus, 90 from a synthetic coding set, the rest scattered. Those are not 300 independent observations. They behave more like a dozen. A bootstrap that resamples rows treats every item as independent, so the confidence interval comes out three times too tight, the gate passes and the regression reaches users. Then support tickets spike, someone bisects for two days and finds the eval never had the power to see the difference in the first place.

The second failure mode is repeated measurements. You run each item three times at temperature 0.7 to smooth judge variance. Now one item is three rows. Average the rows naively and a lucky item with five samples outvotes an unlucky item with two. Worse, if one system errored out on a run and the other did not, you silently compare an unequal set. This file collapses repeated rows per item and system first, then requires both systems to be present before an item counts as a pair, and reports exactly how many incomplete pairs it dropped.

The third one is release gating with no margin. Non inferiority and superiority are different questions, and a p value under 0.05 answers neither cleanly. What a release reviewer actually wants is the lower bound of the interval on the difference and a stated margin to compare it against. That is what `gate_decision` returns.

## Why I built it

In April 2026 a lot of teams still run strong eval suites but make launch calls with statistics that ignore pairing, strata and repeated measurements. The tooling exists in R and in Python if you are willing to bring in a stats stack, but Julia eval harnesses and CI quality gates usually end up with a hand rolled `mean(candidate) - mean(baseline)` because the alternative is a dependency graph you do not want inside a build step.

So this is stdlib only. `Random`, `Statistics` and `Printf`, nothing else. It aggregates repeated rows safely, resamples clusters inside each stratum and returns the metrics reviewers actually ask for in AI eval docs: paired delta with a confidence interval, win rate with its own interval, effect size and bootstrap p values.

## When to use it

- Deciding whether a new prompt version actually beats the current one on a shared eval set before rolling it to traffic
- Comparing two judge models where the same responses were graded by both and you need the paired difference not two separate accuracies
- Gating an agent release in CI when the benchmark is built from a few large scraped corpora, so items inside a corpus are correlated
- Running each eval item several times to average out sampling noise, and needing that collapsed correctly instead of by row count
- Reporting easy and hard splits separately in a model card while keeping one overall interval that respects the split structure
- Answering "is the candidate at least as good" with an explicit non inferiority margin instead of a bare significance test

## How it works

Input goes through `peg_rows`, which accepts either a vector of NamedTuples or a NamedTuple of equal length column vectors and normalizes both into a row vector. `peg_prepare_pairs` filters to rows whose `system_col` matches the baseline or candidate name, then groups every row under the four part key `(id, cluster, stratum, system)` in a `PegAggregateBucket`. Repeated rows for the same key are collapsed by the aggregator resolved in `peg_resolve_aggregator`, which accepts `:mean`, `:median`, `:sum` or any function you pass. Scores must be finite. Weights must be finite and strictly positive.

Aggregated cells are then regrouped by `(id, cluster, stratum)` so each item holds a small dict keyed by system name. An item that lacks either system is dropped when `drop_incomplete` is true and raises otherwise, and the dropped count survives into the result. Surviving items become `PegPair` values carrying baseline score, candidate score, delta and three flags: `win`, `loss` and `tie`, decided against `tie_tolerance`. Baseline and candidate weights for the same pair must agree within `atol = 1e-10`, otherwise it errors rather than guessing which weight is correct. Pairs are sorted by stratum, cluster and id so the output is deterministic for a given seed.

`peg_observed_metrics` computes everything on the pair vector using weighted helpers: `peg_weighted_mean` skips non finite entries and non positive weights, `peg_weighted_rate` runs the same machinery over the boolean flags, and `peg_weighted_sd` returns NaN with fewer than two contributing pairs. Relative lift is the delta over the absolute baseline mean and goes NaN when the baseline mean is at machine epsilon. Standardized delta is delta over weighted SD, NaN when the SD is zero or undefined.

The resampling is a stratified cluster bootstrap, also called a block bootstrap. `peg_cluster_index` builds a stratum to cluster to row index map. For each of `draws` iterations, `peg_bootstrap!` walks the strata in sorted order and, inside each stratum, draws that stratum's own cluster count with replacement from its cluster keys, appending every row index of each chosen cluster. Whole clusters move together, which is the point: that is what propagates within corpus correlation into the interval instead of pretending it away. Metrics are recomputed on the resampled pair vector each draw.

Intervals come from `peg_quantile_interval`, a plain percentile bootstrap at the alpha/2 and 1 minus alpha/2 quantiles. Three p values fall out of the delta draws directly: `p_candidate_not_better` is the fraction of draws at or below zero, `p_candidate_worse` is the fraction strictly below, and `p_two_sided` is twice the smaller tail capped at 1. `gate_decision` then compares the interval's lower bound against your margins and returns named booleans. `result_table` flattens the result into a flat NamedTuple for logging or CSV writing.

## Usage

```julia
include("PairedEvalGate.jl")

rows = [
    (item_id = "item-01", system = "baseline-v1",  score = 0.62,
     cluster = "chat", stratum = "easy", weight = 1.0),
    (item_id = "item-01", system = "candidate-v2", score = 0.69,
     cluster = "chat", stratum = "easy", weight = 1.0),
    # ... one row per (item, system, run)
]

result = paired_eval_gate(
    rows;
    baseline    = "baseline-v1",
    candidate   = "candidate-v2",
    id_col      = :item_id,
    system_col  = :system,
    score_col   = :score,
    cluster_col = :cluster,     # nothing means each item is its own cluster
    stratum_col = :stratum,     # nothing means one stratum called "all"
    weight_col  = :weight,      # nothing means every pair weighs 1.0
    aggregate   = :mean,        # :mean, :median, :sum or a function
    draws       = 4000,
    confidence  = 0.95,
    seed        = 7,            # nothing uses the global RNG
    drop_incomplete = true,
    tie_tolerance   = 0.0,
    keep_draws      = false
)

println(result)

gate = gate_decision(result; noninferiority_margin = -0.01, superiority_margin = 0.0)
gate.passes_noninferiority && println("safe to ship")

row = result_table(result)   # flat NamedTuple for CI logs

# columnar input works too
paired_eval_gate(
    (item_id = ids, system = systems, score = scores);
    baseline = "baseline-v1", candidate = "candidate-v2"
)
```

Running the file directly executes the demo in its `PROGRAM_FILE` block, a 12 row six item sample across three clusters and two strata at 500 draws with seed 7:

```bash
julia PairedEvalGate.jl
```

## Notes

- Stdlib only. `Random`, `Statistics` and `Printf`. No DataFrames, no StatsBase, no CSV reader. You bring the rows.
- Intervals are percentile bootstrap, not BCa or studentized. Fine for the sample sizes eval sets usually have, mildly biased in small skewed cases.
- The cluster bootstrap is only as good as your cluster column. If you leave `cluster_col` as nothing every item becomes its own cluster and you are back to an ordinary paired bootstrap over items.
- Strata with very few clusters give unstable intervals. Resampling three clusters with replacement can and will draw the same cluster three times.
- Baseline and candidate weights for a pair must match after aggregation or it raises. That is deliberate. A weight is a property of the item, not of the system.
- `passes_noninferiority` and `passes_superiority` both test the interval lower bound against a margin, so pass a negative `noninferiority_margin` if that is what you mean. Neither field corrects for multiple comparisons, so running the gate across twenty slices will produce false passes.
- Every draw allocates a fresh resampled pair vector, so cost is roughly draws times pair count. 4000 draws over a few thousand pairs is seconds, not minutes, but it is not tuned for millions of rows.
- Errors are `ArgumentError` throughout, raised via `peg_error`. There is no exit code contract and no CLI flag parsing. Wrap it in your own runner script for CI.
