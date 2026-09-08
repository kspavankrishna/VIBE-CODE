# Canary Evidence Spender

You are canarying a new model, prompt or router and the eval bill keeps climbing, while nobody can say whether the candidate is actually safe on the one cohort that will break production. This is a single R script that decides, per cohort, whether to pass, fail or keep sampling, and then spends the remaining eval budget on the groups where the evidence is thinnest and the risk is highest.

**Language:** R | **Lines:** 555 | **Added:** 2026-06-11

## What this solves

The normal canary workflow for an LLM change collects a big CSV of scored rows and then dies at the last step. Somebody computes a global mean for control and a global mean for canary, sees +0.4 points, and ships. The global mean is the problem. It averages over cohorts that behave nothing like each other. The candidate can gain two points on easy bulk traffic and lose six on the legal document task with 40 rows, and the pooled number still looks like an improvement. That regression shows up as a support queue, not as a red build.

The second failure mode costs money instead of reputation. Teams that do split by cohort find half their cells underpowered, so they run more evals everywhere, uniformly, because uniform is the easy thing to script. That burns tokens on groups already resolved and leaves the ambiguous ones ambiguous. Whoever owns the API bill notices at the end of the month, and if you track carbon per eval run it is real emissions too.

The third is subtler. "No significant regression" is not the same as "safe". A cohort with 12 rows and huge variance never reaches significance, and a naive gate reads that silence as a pass. What you want is a guardrail statement backed by a one-sided bound, with any group that cannot support it labelled unresolved instead of quietly waved through. This script gives you all three: a decision per cohort, a sample plan for the unresolved ones, and a spending order that funds the riskiest first.

## Why I built it

Eval pipelines already collect everything needed for this: scores, latency, token cost, sometimes a carbon estimate, and enough metadata to split by task, model, route or cohort. What they lack is the piece that turns those columns into a decision and a spending plan. Sequential testing libraries exist, power calculators exist, cost dashboards exist, and none of them talk to each other. The last mile stays manual, so a person eyeballs a table and makes a gut call about whether to continue, stop, pass or roll back.

I wanted that last mile as one file with no package installs, because eval gates run in CI containers where `install.packages` is a five minute tax and a supply chain question. Base R only. It reads a CSV, prints TSV or JSON, sets an exit code. That is the whole contract.

## When to use it

- A model or prompt change is in canary and you need a per cohort pass or fail before promoting, not a blended average.
- Your eval budget for this release is fixed and you must decide which cohorts deserve the remaining runs.
- A regression suite keeps returning "not significant" on small cohorts and you want those flagged unresolved, not clean.
- You track carbon per eval run and the sampling plan has to respect a gram budget alongside the dollar budget.
- You are gating a GitHub Actions or Airflow step and want a non zero exit when a group is failing or starved of budget.
- You are comparing a retrieval policy, tool router or inference gateway where routes are not interchangeable.

## How it works

Input is a flat CSV. `load_data` requires the variant column and the metric column, checks that both the baseline and candidate labels appear, then discards every other variant. `optional_column` picks up `cost_usd`, `carbon_g` and `latency_ms` automatically when those names exist, or takes explicit overrides. `numeric_column` coerces and hard fails on any non numeric cell while tolerating blanks. `orient_metric` negates the metric when `--direction lower` is set, so a latency metric and an accuracy metric go through identical downstream math and a positive `effect_oriented` always means the candidate is better. Grouping is the intersection of the requested group columns and the columns that exist, defaulting to `task,model,route,cohort`. `make_key` joins group values with a `\001` separator, `split` partitions the frame, `key_fields` reverses the join for output, and `--group-by none` collapses everything into one scope called `all`.

The statistics live in `welch_stats`. It computes the Welch standard error `sqrt(var_b/n_b + var_c/n_c)` with Welch-Satterthwaite degrees of freedom, so the two arms are never assumed to share a variance, and the one-sided interval uses `qt(1 - alpha, df)`. The core quantity is the non-inferiority statistic `(effect + guardrail_loss) / se`: its upper tail is `p_safe`, the p-value for rejecting the null that the candidate is worse than the margin, and its lower tail is `p_harm`. `harm_probability` is the same tail read as a probability that the true effect sits below the negative guardrail. A degenerate branch handles zero or non finite standard error with hard zeros and ones instead of dividing by zero.

`decision_for` turns that into one of four labels. Either arm below `--min-n` gives `continue_min_n` and nothing else is considered. Then `fail_harm` when `p_harm <= alpha` and the point effect is past the guardrail, `pass_guardrail` when `p_safe <= alpha` and the one-sided lower bound clears the negative margin, otherwise `continue_uncertain`. `planned_n_per_arm` sizes the next round with the normal approximation `n = ceil(2 * ((z_alpha + z_power) * sd / effect)^2)`, using `pooled_sd` and either the explicit `--mde` or a fallback of the guardrail or the observed effect, whichever is larger. The result is clamped into `[min_n, max_n]` and the shortfall against the smaller arm becomes `requested_more_per_arm`. Terminal decisions zero their own request.

Allocation is a greedy pass in `allocate_budget`, ordered by `allocation_score`, which is `harm_probability * sqrt(requested + 1) / (avg_cost + avg_carbon/1e6)`. Risk in the numerator, price in the denominator, with a square root on the request size so one enormous ask cannot monopolise the queue. Groups too small for a variance estimate get the maximum risk score of 1.0, so unmeasurable cohorts sort to the front rather than being ignored. Remaining budget is the configured total minus observed spend across the file, and each grant is floored by both the dollar and the carbon ceiling before decrementing both. Every row lands as `funded`, `partial`, `budget_exhausted` or `not_needed`. Output goes through `emit_tsv` or a hand written `emit_json`, and `--fail-on-risk` exits 2 on any `fail_harm` or `budget_exhausted` group.

## Usage

```bash
# evals.csv: variant,task,model,route,cohort,score,cost_usd,carbon_g,latency_ms
# one row per eval sample, control and canary rows interleaved

Rscript CanaryEvidenceSpender.R --input evals.csv --baseline control --candidate canary

# lower-is-better metric, explicit guardrail, capped budget, JSON out
Rscript CanaryEvidenceSpender.R \
  --input evals.csv \
  --baseline gpt-prod --candidate gpt-canary \
  --metric latency_ms --direction lower \
  --guardrail-loss 25 --mde 40 \
  --alpha 0.05 --target-power 0.90 \
  --min-n 50 --max-n 2000 \
  --total-budget-usd 400 --total-carbon-g 90000 \
  --group-by task,route \
  --json

# CI gate: exit 2 if any cohort fails the guardrail or gets starved
Rscript CanaryEvidenceSpender.R --input evals.csv --fail-on-risk

Rscript CanaryEvidenceSpender.R --help
```

## Notes

- Base R only. No packages, no network, no state. `read.csv` loads the whole file into memory, so a multi gigabyte eval dump needs pre aggregation.
- Exit codes: 0 clean, 2 risk gate tripped under `--fail-on-risk`, 64 bad arguments, 65 schema or data problem such as a missing column or a non numeric cell, 66 unreadable or missing input file, 70 uncaught error.
- Every group is tested independently at `alpha` with no multiplicity correction. Split by four columns and you have created dozens of tests, so tighten `--alpha` yourself when the group count is large.
- `p_safe` and `harm_probability` are the same number by construction, because the t distribution is symmetric. One reads as a p-value, the other as a probability of exceeding the guardrail. They are not independent evidence.
- Latency is reported only. `latency_mean_ms` and `latency_p95_ms` appear in the table but never influence a decision unless latency is the `--metric` itself.
- Carbon in the ranking score is divided by 1e6 so grams sit on roughly a dollar scale, meaning cost dominates the ordering unless carbon per sample is huge. The budget ceiling is enforced in raw grams.
- Cost and carbon per sample are averaged across both arms, so asymmetric arm pricing makes the forecast approximate. The plan is a forecast, not a reservation: nothing here executes evals or calls a model.
- The power calculation is the normal approximation and assumes independent observations. Repeated prompts against the same document or user will understate variance.
