# Eval Power Drift

A candidate model scores half a point higher than control on the eval sheet and the team ships it. Nobody checked whether 14 samples per cohort can even detect half a point, or that cost per request went up 22% in the same run.

**Language:** R | **Lines:** 373 | **Added:** 2026-05-24

## What this solves

The standard AI eval workflow ends at a mean. You export a CSV from the benchmark run, group by variant, compare averages and call the winner. That number is usually underpowered. With 20 or 30 samples per arm and a metric with large per row variance, the sampling noise is wider than the effect you are claiming. The lift is real in the spreadsheet and gone in production. Somebody reruns it a fortnight later, the sign flips, and the team argues about a regression that was never there.

The second failure is quieter and costs more. A reasoning preamble, a RAG change that pulls three extra chunks, a swap to a bigger checkpoint: each often buys a small quality gain with a large increase in tokens, spend, latency and energy, and the eval report tracks only quality, so the tradeoff never reaches the ship decision. Finance notices at month end when the inference line jumps. On call notices when p95 crosses the timeout. Neither gets linked back to the launch, because the launch was approved on one average score.

The third failure is aggregation. A candidate that looks neutral overall can be badly worse on one route, cohort or model tier while the pooled mean stays flat, and the users in that slice are the ones who file tickets. EvalPowerDrift takes the eval CSV you already have and answers the boring question before rollout. Is the observed change large enough relative to its own noise to be believed at this sample size, and did the candidate quietly raise cost, latency, energy or token volume while doing it. It answers per group and exits non zero when any group fails, so CI can block the merge.

## Why I built it

Eval platforms and observability exports give you means and sometimes a p value. They do not give you power. A p value above alpha says nothing about whether the experiment could have detected the effect you cared about, and a p value below alpha on a tiny sample is a coin flip you won. The number that gates a rollout is the minimum detectable effect at the sample size you have, and almost no eval dashboard reports it.

The other constraint was weight. A gate running in CI beside benchmark CSV exports from LangSmith, OpenTelemetry, Vercel AI Gateway or an internal pipeline should not drag a statistics stack into the build image. This is one file of base R with no `library()` calls, reading with `read.csv` and doing the rest with `var`, `pt`, `qt`, `pnorm` and `qnorm`. You can read all of it in one sitting and fork it when your decision rule differs from mine.

## When to use it

- A canary model or prompt claims a quality lift and you need to know whether the sample size supports it before it reaches traffic.
- You are approving a RAG or context change and want the token and cost delta in the same table as the score delta.
- A nightly benchmark job should fail the build on a regression in any single route, cohort or model tier, not only on the pooled mean.
- Someone asks how many eval rows you actually need and you want to push a target effect through `--mde` and read the power back.
- You track inference energy or carbon per request and want it treated as a real regression signal.

## How it works

`parse_args` hand rolls the flag loop, no CLI library. Defaults are baseline `control`, candidate `candidate`, variant column `variant`, metric `score`, direction `higher`, groups `model,route,cohort`, `min_n` 30 and `alpha` 0.05. It rejects a bad direction, `--min-n` below 2 and alpha outside the open interval 0 to 1, then splits `--group-by` through `parse_list`, which also accepts the literal `none` to disable grouping. `main` reads the CSV, enforces the variant and metric columns through `require_columns`, drops any group column absent from the file after warning on stderr, then keeps only baseline and candidate rows.

Grouping uses a joined key rather than a merge. `make_key` pastes the group values per row with a `\001` separator, `split` partitions on that key, and `key_fields` splits it back into named output fields. With no group columns the file becomes a single group named `scope`.

Each group goes to `analyze_group`, and the core test there is Welch's t-test in `welch`. It drops non finite values, takes both means and sample variances, forms the standard error as `sqrt(var_base/n_base + var_cand/n_cand)` and the degrees of freedom by the Welch Satterthwaite approximation. Welch rather than Student because eval arms rarely have equal variance, and a candidate that is more consistent or more erratic than baseline is the exact case pooled variance mishandles. The two sided p value comes from `pt`, the interval from `qt(1 - alpha/2, df)` times the standard error around the delta, and the effect size is Hedges g: Cohen's d on the pooled standard deviation times the small sample correction `1 - 3/(4*(n_base + n_cand - 2) - 1)`. A side with fewer than two usable rows returns NA rather than throwing.

`power_for_effect` is the part most eval tooling skips. It computes two sided power under a normal approximation, where `z_effect` is the target effect over the same standard error, `z_alpha` is `qnorm(1 - alpha/2)`, and power is `pnorm(z_effect - z_alpha) + pnorm(-z_effect - z_alpha)`. With `--mde` it uses your minimum detectable effect, without it the observed delta. Below 0.80 it sets the `underpowered` flag.

`classify` collapses that into one status. FAIL when the primary metric moved significantly the wrong way for `--direction`, or an optional metric got significantly worse, or `--fail-underpowered` is set on an underpowered group. WARN when either arm holds fewer than `--min-n` rows or the group is underpowered. PASS otherwise. `optional_stats` runs the same Welch test on `cost_usd`, `latency_ms`, `energy_wh` and `tokens` when present, marking each bad on a significant increase. Output goes through `emit_tsv`, or `emit_json` with `--json`, both hand written, and the JSON form adds a top level `status` rolled up across groups.

## Usage

```bash
# minimum: a CSV with a variant column and a score column
Rscript EvalPowerDrift.R --input evals.csv --baseline control --candidate canary

# full CI gate: target effect of 0.5 metric units, block on underpowered runs
Rscript EvalPowerDrift.R \
  --input evals.csv \
  --baseline control \
  --candidate canary \
  --metric score \
  --direction higher \
  --group-by model,route,cohort \
  --min-n 50 \
  --alpha 0.01 \
  --mde 0.5 \
  --fail-underpowered \
  --json

# a lower is better metric, no grouping at all
Rscript EvalPowerDrift.R --input latency_eval.csv --metric latency_ms \
  --direction lower --group-by none

Rscript EvalPowerDrift.R --help
```

Input shape. Only `variant` and the metric column are required, the rest are analyzed when present:

```csv
variant,model,route,cohort,score,cost_usd,latency_ms,energy_wh,tokens
control,gpt-x,search,eu,0.81,0.0041,820,0.0009,1420
canary,gpt-x,search,eu,0.86,0.0067,1180,0.0014,2310
```

## Notes

- Base R only, so it runs on any Rscript install with no CRAN step in the build image.
- Power uses a normal approximation, not a non central t distribution, so at small n it runs optimistic. Below roughly 20 rows per arm read it as a floor. Without `--mde` it is observed power taken from the effect you just measured, which is circular and only a rough signal.
- No multiple comparison correction. Grouping by model, route and cohort fires dozens of simultaneous tests, so at alpha 0.05 expect false FAILs. Tighten `--alpha` when the group count is high.
- The four optional metrics are hardcoded as lower is better and `--direction` applies only to the primary metric, so a significant rise in cost, latency, energy or tokens is always a regression.
- Exit codes: 0 for PASS or WARN, 2 when any group is FAIL, 64 for bad arguments or no rows matching the requested variants, 66 when the input file is missing or unreadable. WARN alone never fails the build unless you add `--fail-underpowered`.
- A non numeric value in the metric or any optional column aborts the run, while empty strings and NA are dropped before the test. Numeric TSV fields print with `%.6f` including the row counts, so `n_baseline` reads as `30.000000`. It compares exactly two variants per run, so multi arm sheets need one call per candidate.
