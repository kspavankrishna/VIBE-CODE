# Causal Release Guard

A canary is live, the dashboard average is moving up, and nobody can tell you whether that is a real effect or one heavy region dragging the mean. This is a single Julia file that reads raw observation rows and returns one decision: ship, hold or block.

**Language:** Julia | **Lines:** 675 | **Added:** 2026-07-07

## What this solves

AI products, data pipelines, edge services and DevOps platforms have to decide whether a new model, prompt, routing rule, feature rollout or inference queue policy is actually better before the blast radius gets expensive. Dashboard averages are too late. Plain A/B tests miss the messy parts: stratified traffic, weighted samples, repeated canary looks, heavy tailed latency, token cost outliers and guardrail metrics.

Here is the concrete failure. Your canary sends 5 percent of traffic to a new model, and traffic is not evenly spread: 70 percent is one large region, the rest scattered across small markets. The candidate is worse in every single region, but the region mix shifted between arms and the pooled average comes out positive. Somebody screenshots the green number, the rollout goes to 100 percent and the regression surfaces two days later in a support queue. A pooled mean will never show you that.

The second failure is peeking. Whoever opens the canary dashboard at hour one, hour four and hour twelve calls it the moment a look crosses the threshold. Each look is another chance to see noise, so 0.05 checked four times is not a 0.05 test any more, and nobody traces the bad release back to how it was measured. The third is tail shape. p99 latency and token cost are not normal, a handful of 30 second requests move a mean far more than they should, and a t test there reports a number about outliers rather than about your change.

## Why I built it

Every serious experimentation platform can do stratified effects, sequential looks and guardrail policies. They also want a warehouse, a notebook, a SaaS account or an API key, and what they return is a chart rather than a decision. Fine for a data science team. Useless inside a GitHub Actions job or a Kubernetes canary controller at three in the morning, where you need a verdict a machine can act on.

So this is one deterministic file with nothing outside the Julia standard library. It takes CSV or TSV observations from OpenTelemetry, warehouse exports, gateway logs, eval runners or CI jobs, and prints JSON. Same seed, same input, same answer, which matters when the output gets quoted in a postmortem.

## When to use it

- A model gateway canary is running and you need to know whether the new routing rule cut cost without pushing p99 latency past a guardrail.
- Traffic is stratified by region, market or cohort and the pooled average disagrees with what you see per stratum.
- Your team checks the same rollout every few hours and the threshold should account for this being look five, not look one.
- Observations carry inverse propensity weights from a non randomized rollout, so every unit should not count equally.
- A release pipeline needs ship, hold or block as JSON, not a chart a human has to read.

## How it works

Input parsing is deliberately forgiving. `load_observations` reads the header, calls `detect_delimiter` to pick between comma, tab and semicolon by counting them, then runs every row through `split_record`, a hand written CSV splitter that tracks quote state and handles doubled quotes. Headers go through `normalize_header` and alias lists, so `unit`, `unit_id`, `request_id` and `user_id` all satisfy the unit column. `weight`, `stratum`, `segment` and `timestamp` are optional, defaulting to 1.0, `global` and `all`.

Estimation runs per metric and segment pair. `build_cells` buckets rows into a `CellAccumulator` per stratum, control arm and candidate arm kept separate. `prepare_strata` drops any stratum missing either arm, since a stratum with no counterfactual contributes nothing to a within stratum comparison, then clips both arms with `robust_clip`. That clipping is a median absolute deviation winsorization: take the median, scale the median of absolute deviations by the usual 1.4826 consistency constant and clamp to a band of `--trim-z` scale units either side. When the MAD collapses to zero on constant data it returns the values untouched instead of destroying them. Each surviving stratum gets a mass equal to the geometric mean of its two arm weight sums.

`estimate_effect` is the point estimate: weighted candidate mean minus weighted control mean inside each stratum, averaged across strata by mass. That is what makes the region mix problem disappear. The comparison never crosses a stratum boundary.

Uncertainty is a stratified percentile bootstrap. `bootstrap_effect` calls `resampled_mean`, which draws n indices with replacement from each arm of each stratum and returns the weight ratio estimator over that resample, then recombines the per stratum deltas with the same fixed masses. The RNG is a `MersenneTwister` seeded with `seed + mod(hash((metric, segment)), 1_000_000)`, so every pair gets its own reproducible stream. Non finite draws are discarded and the run throws if fewer than `max(40, div(min(bootstraps, 100), 2))` survive. `percentile` interpolates between order statistics on the sorted draws.

The threshold is where peeking gets handled. `sequential_alpha` spends `alpha / (look * (look + 1))` at look number `look`, the 1/(k(k+1)) telescoping schedule whose infinite sum is exactly one, so total spend across unlimited looks stays under the family alpha. That budget is then split Bonferroni style across the metric and segment pairs. Pass `--look 5` on your fifth check and the interval widens accordingly. Conservative, on purpose.

Policy evaluation is orientation aware. A `MetricPolicy` carries a direction of `:higher` or `:lower`, a role of `:primary` or `:guardrail` and a `min_effect` threshold. `signed_bounds` flips the effect and swaps the interval endpoints for `:lower` metrics so larger signed is always better. `result_status` returns `:pass` when the signed lower bound clears `min_effect`, `:fail` when the signed upper bound sits entirely below it and `:uncertain` otherwise. Uncertain is a first class outcome, not a rounded p value. `decide_release` gates on that: any failing metric blocks, an uncertain primary or guardrail holds, and only an all clear ships.

## Usage

```bash
# canary.csv columns: unit, variant, metric, value
# optional: weight, stratum, segment, timestamp

julia CausalReleaseGuard.jl \
  --input canary.csv \
  --control control \
  --candidate treatment \
  --metric conversion:higher:primary:0.002 \
  --metric p99_latency_ms:lower:guardrail:-5 \
  --metric cost_per_request:lower:guardrail:0 \
  --alpha 0.05 \
  --look 3 \
  --bootstraps 2000 \
  --seed 17 \
  --trim-z 6 \
  --format json

# read from stdin, human readable output
warehouse_export | julia CausalReleaseGuard.jl --input - --format text

julia CausalReleaseGuard.jl --help
```

Called as a module:

```julia
include("CausalReleaseGuard.jl")
using .CausalReleaseGuard

observations = load_observations("canary.csv")
policies = [parse_metric_policy("error_rate:lower:guardrail:-0.001"),
            parse_metric_policy("revenue_per_user:higher:primary:0.01")]

decision, results = analyze(observations;
                            control="control",
                            candidate="treatment",
                            policies=policies,
                            alpha=0.05, look=2, bootstraps=2000)

decision.action   # :ship, :hold or :block
decision.reason   # the metric and segment names that drove it
```

## Notes

- The `unit` column is required and validated but never used for clustering. Rows for the same user count as independent observations, so a metric with many events per user reports intervals that are too narrow. Pre aggregate to one row per unit.
- Any metric in the data without an explicit `--metric` policy silently gets `higher:primary:0.0`. Safe for a conversion metric, wrong for latency, so declare your guardrails.
- Exit codes reflect execution, not the verdict. `main` exits 2 on any error and 0 otherwise, including on `:block`, so a pipeline that wants to fail the build must parse the JSON `action` field itself.
- The interval is a plain percentile bootstrap. No BCa or studentized correction, so it can be off for small strata or skewed estimators. `--bootstraps` has a floor of 80 and defaults to 800.
- Strata present in only one arm are dropped rather than pooled, and the run throws if no stratum has both. Masses stay fixed across draws, so the interval covers noise within strata, not uncertainty in the traffic mix.
- Only two arms per run. With more than two variants you must pass `--candidate`, since inference works only when exactly one non control arm exists. Variant names are lowercased first.
- Dependencies are Printf, Random and Statistics. No packages, no database and no API key.
