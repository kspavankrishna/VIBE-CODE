# Agent Run SLO Governor

An AI agent canary looks fine on the dashboard, then ships a 14% worse p95 latency and a quietly higher token bill because someone compared two averages. This is a dependency free TypeScript gate that pairs baseline and candidate runs by run id, bootstraps a confidence interval on the actual gate statistic and fails the build before the regression reaches users.

**Language:** TypeScript | **Lines:** 838 | **Added:** 2026-05-28

## What this solves

The failure mode is boring and it happens constantly. You have a baseline agent and a candidate agent. Somebody exports JSONL telemetry from both, computes mean latency for each side and sees 1,040 ms against 1,055 ms. Ship it. What actually shipped was a candidate whose tail got much worse: a new tool call that occasionally takes four seconds, a retry path that fires on 3% of requests, a route that falls back to a slower provider on cache miss. The mean absorbed it. The p95 did not, and p95 is what users and the on call rotation feel.

The opposite failure is just as common. You look at p95, see it jump 9%, block the release, and it turns out you had 40 runs and the jump was noise. Now you have a gate nobody trusts, so people override it, so the gate stops existing. A single point estimate on a long tail statistic from a few hundred noisy runs is not a decision, it is a coin flip with extra steps.

Third is unpaired comparison. A prompt that hits a tool loop costs ten times what a one shot completion costs. If the two samples drew different mixes of prompts, the delta you measured is mostly prompt mix, not your change, and you will chase a phantom cost regression for two days.

The cost of getting this wrong is real. A 3% cost regression on a million agent runs a month is a line item finance notices at quarter end. A p95 blowout is a support queue. A quality score that drifted 0.02 because a prompt template dropped a constraint is the kind of thing nobody notices for six weeks, until a customer does. This file turns all four into a gate that returns exit code 2 and a list of reasons.

## Why I built it

The tooling that exists sits at the wrong altitude. Observability platforms chart p95, but charting is not gating: they do not know what a baseline is, they will not pair runs and they will not return a non zero exit code in CI. Eval frameworks score quality and mostly ignore latency, cost and error rate. Feature flag platforms do statistics on conversion metrics, not on latency tails from a JSONL export. The gap in the middle is the promotion decision itself, and in most teams that is still a notebook one person runs by hand.

So this is one file, no dependencies, that reads JSONL on stdin and exits non zero. It drops into a GitHub Actions step, a gateway deploy hook or a build script, and it makes the same decision the same way every time.

## When to use it

- Promoting a canary agent build to full traffic and you need a hard yes or no from CI, not a chart somebody eyeballs
- Running shadow traffic where the same request hits both the current and the new agent, giving you natural run id pairs
- Swapping a model, provider or routing policy and needing proof the token bill did not move more than 3%
- Changing a prompt template or tool schema, where quality regression is the risk nobody watches
- Investigating a suspected regression and needing a per cohort breakdown by route, model or region rather than one blended number
- Enforcing an SLO on an agent that already shipped, run nightly against yesterday's paired telemetry

## How it works

`parseJsonlTelemetry` walks the input line by line and hands each object to `normalizeRecord`, which is deliberately forgiving about field names because every exporter names things differently. Run id comes from `runId`, `traceId`, `requestId` or `id`. `normalizeVariant` maps base, control, stable and production to `baseline`, and canary, treatment, shadow and experiment to `candidate`. Cohort falls back through `cohort`, `segment`, `route`, `model`, `provider`, `region` and defaults to `"all"`. Anything that fails validation lands in `rejected` with its line number and raw text, so a broken exporter is visible rather than silently shrinking your sample.

Pairing is the part that matters. `pairRuns` buckets records by cohort plus run id, joined with a unit separator so a cohort name cannot collide with the delimiter. If the same variant appears twice in a bucket, `chooseLater` keeps the record with the higher `timestampMs`, falling back to line number, so a retried run does not double count. Only buckets holding both a baseline and a candidate become a `PairedRun`. Everything else feeds `unpairedCount` and surfaces as a warning, so you find out when your join key is wrong instead of quietly gating on 12% of your data.

The statistics use paired bootstrap resampling, and that is the design decision worth checking. `bootstrapInterval` runs `bootstrapRounds` iterations, 2000 by default. Each round draws `pairs.length` pairs with replacement and recomputes the whole gate statistic on that resample. It resamples pairs, not individual runs, so baseline and candidate stay attached to the same request. The four statistics are `latencyP95DeltaPct` (sorts both arms, takes the 0.95 quantile through `quantileSorted` with linear interpolation, then percent delta), `meanCostDeltaPct`, `errorRateDeltaPercentagePoints` and `meanQualityDelta`. Because the p95 is recomputed inside every round, you get an interval on the p95 delta itself rather than an approximation derived from means. p95 has no closed form standard error you can trust on a skewed latency distribution, so you resample it directly. The randomness is deterministic: `mulberry32` is the PRNG, seeded through the FNV-1a `hashString`, and with no `--seed` the seed comes from `stableSeedForPairs`, built out of the pair data itself. Same telemetry, same verdict, every machine.

The gate ignores the observed point estimate. In `summarizeMetric` a `higher_is_worse` metric passes only when `interval.upper <= threshold`, and `lower_is_worse` (quality) passes only when `interval.lower >= threshold`, so the candidate clears the bar across the whole interval and not just at the midpoint. `DEFAULT_CONFIG` sets 8% p95 latency, 3% mean cost, 0.25 percentage points error rate, -0.005 quality delta, 200 minimum pairs, 30 minimum pairs before a cohort can block, 2000 rounds and 0.95 confidence. `summarizeCohort` runs all four metrics globally and again per cohort, and `collectFailures` turns each failure into a reason string naming the label, the breached bound and the threshold. Global failures always block. A cohort under `minCohortPairs` degrades to a warning, which keeps a thin regional slice from vetoing an otherwise clean release.

## Usage

```bash
# JSON verdict from a file, default thresholds
npx ts-node AgentRunSloGovernor.ts --input=runs.jsonl --format=json

# Markdown summary from stdin, tighter latency budget, strict parsing
cat runs.jsonl | npx ts-node AgentRunSloGovernor.ts \
  --format=markdown \
  --max-p95-latency-delta-pct=5 \
  --max-cost-delta-pct=2 \
  --max-error-rate-delta-pp=0.1 \
  --min-pairs=500 \
  --bootstrap=5000 \
  --confidence=0.99 \
  --strict

# Synthetic data, no input needed, useful for wiring up CI
npx ts-node AgentRunSloGovernor.ts --self-test --format=markdown
```

Input is JSONL, two records per run id sharing a cohort:

```jsonl
{"runId":"run-0001","variant":"baseline","cohort":"tool-heavy","latencyMs":912,"costUsd":0.0151,"qualityScore":0.91,"failed":false}
{"runId":"run-0001","variant":"candidate","cohort":"tool-heavy","latencyMs":934,"costUsd":0.0154,"qualityScore":0.909,"failed":false}
```

Or call the library directly:

```ts
import { AgentRunSloGovernor, parseJsonlTelemetry, renderMarkdown } from "./AgentRunSloGovernor";

const { runs, rejected } = parseJsonlTelemetry(jsonlText, { strict: false });
const evaluation = new AgentRunSloGovernor({
  maxP95LatencyDeltaPct: 5,
  minPairs: 500,
  seed: "release-2026-05-28",
}).evaluate(runs, rejected);

if (evaluation.status === "fail") {
  console.error(renderMarkdown(evaluation));
}
```

## Notes

- Exit codes: `0` pass, `2` gate failure, `1` any thrown error, which includes `--help` and any unknown argument. Help text goes to stderr.
- Requires genuinely paired runs. If the two arms do not share run ids, everything lands in `unpairedRecords`, the pair count falls under `minPairs` and the gate fails on that. This is not a tool for comparing two independent traffic samples.
- `inputTokens` and `outputTokens` are parsed and carried on `AgentRun`, but no metric uses them. Cost gating runs on `costUsd` alone, so emit a cost field or that metric compares zeros and reports a 0% delta.
- `qualityScore` must be in [0, 1] or the record is rejected. A boolean `passed`, `pass` or `accepted` maps to 1 or 0. If no pair carries quality on both sides the metric is skipped and you get a warning, not a failure.
- The last resort branch of `readFailure` regex matches the raw line for error, failed, timeout or rate_limited, so a completion containing the word "error" counts as a failed run. Emit an explicit `failed` or `status` field and this never fires.
- `pctDelta` returns positive infinity when the baseline is zero and the candidate is not, prints as `inf` and fails any `higher_is_worse` gate. Validation also rejects `bootstrapRounds` below 200 and confidence outside (0, 1). Rounds multiply by cohort count, since every cohort gets its own full bootstrap.
- No dependencies beyond Node's `fs`, touched only inside `main`. The class, the parser and both renderers run anywhere.
