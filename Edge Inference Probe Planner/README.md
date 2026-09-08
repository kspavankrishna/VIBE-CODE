# Edge Inference Probe Planner

Your LLM gateway runs across several edge regions and providers, and the dashboard shows one blended latency number that hides the two routes quietly failing. This Swift command line tool reads inference telemetry, ranks every region/provider/model/route combination by risk, then turns that ranking into a synthetic probe plan that fits a fixed dollar budget.

**Language:** Swift | **Lines:** 830 | **Added:** 2026-08-17

## What this solves

Edge inference is not one system, it is a matrix. Every request lands on some combination of region, provider, model, route and residency zone, and each cell has its own p95, error rate, cost per thousand tokens and carbon intensity. Aggregate dashboards average that matrix into one green number, so a single cell can be badly broken while the mean stays fine.

Concretely: you ship a gateway change on Tuesday. Frankfurt on the local Llama deployment runs a 3.1 second p95 and a 9 percent error rate, but carries 2 percent of traffic, so the global p95 barely moves. Nobody notices until a customer pinned to EU residency files tickets on Thursday. Two days of SLO burned, an escalated support thread, and a postmortem that finds you had the telemetry all along, just never sliced by route.

The quieter failure is a route that looks healthy because it has nine samples and all nine succeeded. That is not health, that is absence of evidence. Most tooling scores three samples the same way it scores three thousand, so thin routes get promoted on a coincidence. This planner inverts that: a route below the minimum sample count accrues risk rather than passing, and more of it the further below the floor it sits.

Probes also cost real money, so teams either fire a flat number at every route or guess from a dashboard. This tool converts risk into a weighted allocation that stops when the budget runs out: run 14 probes here, 8 there, and here is the bill.

## Why I built it

Observability platforms store this telemetry well and answer badly the one question a release engineer asks an hour before rollout: which routes do I not trust yet, and where does the probe budget go. Getting it from a hosted dashboard means a bespoke query per dimension, cost and carbon arithmetic by hand, then an undocumented call on sample sufficiency. Not repeatable, and it does not run in CI.

Cost per thousand tokens, carbon grams per thousand tokens, cache hit rate and cold start rate are production signals now, and almost nothing scores them alongside latency and errors in one number. A route that is fast but three times over your cost ceiling is still one you should not promote. So: one file, zero dependencies, eats OpenTelemetry style JSONL, exits non zero on critical.

## When to use it

- Before a multi region rollout, to find which region and provider pairs have no credible evidence behind them yet.
- In CI against a nightly telemetry export with `--fail-on-critical`, so a regressed route blocks the pipeline instead of paging someone later.
- When the probe budget is fixed and you need to decide where the synthetic traffic goes.
- When residency rules require certain regions covered and you need proof telemetry from them exists.

## How it works

Input parsing is deliberately forgiving. `parseObservations` checks the first character: a leading `[` means a JSON array of objects, anything else is JSONL, one object per line, blank lines and `#` comments skipped. Each row becomes an `Observation` through resolvers named `stringField`, `doubleField`, `intField` and `boolField`, each taking accepted key aliases, so `latency_ms`, `latencyMs` and `duration_ms` all land in the same field and numbers arriving as JSON strings are coerced. Gateway logs, CDN worker logs and provider exports go in as they are. Two values are derived here: success falls back to matching the status string against a whitelist, and carbon falls back to `(energy_wh / 1000) * grid_gco2_per_kwh` when no explicit `carbon_g` is present.

Aggregation groups observations into a dictionary keyed by `RouteKey`, a hashable tuple of region, provider, model, route and residency. `RouteStats` accumulates counts and a raw latency array per key, and `RouteMetrics` turns that into rates and percentiles. Percentiles come from `quantile`, linear interpolation between the two nearest order statistics rather than nearest rank, so p95 on a small sample degrades smoothly instead of snapping between observed values. Cost and carbon are normalised per thousand tokens, the only comparison that holds across routes with different traffic shapes.

Scoring lives in `assess` and is additive with per check saturation. Each violated threshold contributes a base penalty plus a term proportional to how far past the limit you are, and each contribution is capped: latency at 35 points, error rate at 38, cost at 18, carbon at 16. Under sampling contributes `22 + gap * 18` where gap is the fraction of the sample floor you are missing. A weak cache hit rate adds 6, only once the route has enough samples for that to mean anything, and excess cold starts add 10. The total is clamped to 100 then bucketed: 70 and above critical, 35 and above warning, and a route under the sample floor is forced to warning regardless. The caps stop one catastrophic dimension saturating the score and hiding the other four.

`buildProbePlan` is a greedy weighted allocator over a hard budget. Candidates are every route with non zero risk or insufficient samples, sorted by risk descending with `RouteKey` ordering as the tie break so output is deterministic. Each gets a weight of `max(5, riskScore)` and a proportional share of the budget, so even a low risk candidate gets a floor rather than zero. Per probe cost comes from that route's own observed cost per thousand tokens scaled to `--probe-token-size`, clamped between 0.0001 and 1.0 dollars, falling back to `--default-probe-cost-usd` when the route has no cost data. Probes requested is the larger of the sample deficit and a severity floor of 8 for critical or 3 otherwise, capped by what the share affords and by `--max-probes-per-route`. Spend comes off a running remainder, so the plan never exceeds your budget.

Required region coverage sits in `planReport`. If `--required-region` names a region absent from the telemetry, the planner synthesises a `RouteAssessment` at risk 75, severity critical, puts it at the top of the ranking and lets it compete for probes like a real route. That catches a region you thought you served but never observed. Output is Markdown with a ranked risk table, an allocation table and per route reasons, or sorted JSON via `renderJson`.

## Usage

```bash
# compile once
swiftc EdgeInferenceProbePlanner.swift -o edge-probe-planner

# verify the build against the built-in fixture
./edge-probe-planner --self-test
# prints: self-test ok

# markdown report from a JSONL export
./edge-probe-planner --input telemetry.jsonl --format markdown

# CI gate: JSON out, tight SLOs, required regions, non-zero exit on critical
./edge-probe-planner \
  --input telemetry.jsonl \
  --format json \
  --max-p95-ms 900 \
  --max-error-rate 1.5% \
  --max-cost-per-1k 0.02 \
  --max-carbon-g-per-1k 30 \
  --min-samples 50 \
  --min-cache-hit-rate 0.10 \
  --max-cold-start-rate 0.05 \
  --probe-budget-usd 25 \
  --default-probe-cost-usd 0.006 \
  --probe-token-size 1800 \
  --max-probes-per-route 40 \
  --required-region iad,fra,bom \
  --fail-on-critical

# reads stdin when --input is omitted
cat gateway-*.jsonl | ./edge-probe-planner --format json
```

Expected record shape, every field optional and alias tolerant:

```json
{"timestamp":"2026-04-12T08:00:01Z","region":"iad","provider":"openai","model":"gpt-4.1-mini","route":"chat","tenant":"alpha","residency":"us","status":"ok","latency_ms":820,"input_tokens":900,"output_tokens":250,"cost_usd":0.012,"energy_wh":0.8,"grid_gco2_per_kwh":390,"cache_hit":false,"cold_start":false}
```

## Notes

- Exit codes: 0 on success, 2 when `--fail-on-critical` is set and any route is critical, 2 on any parse or argument error. `--help` prints usage to stderr prefixed with `error:` but still exits 0, a wart if you script around it.
- Latency values are held in memory per route as a `[Double]` and sorted on each percentile call. Fine for millions of rows, but not a streaming aggregator.
- Rate flags accept a decimal or a percent suffix, so `0.025` and `2.5%` mean the same thing.
- Risk weights and severity cutoffs are hardcoded inside `assess`. Thresholds are tunable from the CLI, the point values assigned when one breaks are not.
- Carbon is only as good as the input. Supply neither `carbon_g` nor an energy plus grid intensity pair and carbon per thousand tokens is zero, so that check never fires.
- It plans probes. It does not run, schedule or dispatch them. There is no network code in the file.
