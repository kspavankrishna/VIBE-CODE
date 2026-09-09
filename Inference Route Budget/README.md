# Inference Route Budget

An AI gateway rollout moves traffic to a cheaper model and quietly breaks three things at once: p99 latency doubles, prompt cache hit rate collapses so the bill goes up anyway, and regulated traffic lands in the wrong jurisdiction. This is a single file OCaml CLI that reads a route export as CSV and says which routes are safe to ship, which should degrade and which must be blocked.

**Language:** OCaml | **Lines:** 982 | **Added:** 2026-07-22

## What this solves

Once a product has more than one model vendor, the routing table stops being a config detail and becomes a budget. Every route carries a latency profile, an error rate, token prices, a prompt cache hit rate, a capacity ceiling, a region and a residency obligation. Those numbers live in different places: observability for latency and errors, a vendor page for prices, the gateway for cache hit rate, a spreadsheet for capacity and a compliance doc nobody opens during a release. Nothing joins them, so the rollout decision gets made on whichever number came up loudest in the review.

The failure mode is specific. Someone points overflow traffic at a cheap third party route because the input price is a third of the primary. That route has a 3600ms p99, a 1.8 percent error rate and an 18 percent cache hit rate. Price per thousand input tokens went down, effective cost per request went up because the cache is missing, and support starts seeing timeouts on the slowest 1 percent of requests. That takes a week to notice and a month to unwind. Worse, the route sits in ap-south-1 while the required jurisdiction is US, so it is now a compliance incident rather than a latency one.

Capacity is the other quiet one. A route carrying 12 rps with 13 rps of rated capacity looks fine on a dashboard, because dashboards show utilization and not headroom. There is no room for a retry storm or a failover from a sibling route, so when the primary sheds load onto it the fallback path becomes the outage.

This tool puts every route into one shape: monthly requests, monthly cost after cache savings, monthly carbon in kg CO2e, capacity headroom, a 0 to 100 score and one of four decisions. It fails closed on residency and negative headroom instead of averaging those risks into a friendly number.

## Why I built it

Gateway products give you dashboards. Dashboards show one metric across many routes and are bad at answering "is this specific route allowed to take more traffic tomorrow". Cost calculators handle token pricing but ignore cache discounts, capacity and residency. Policy engines handle residency but know nothing about p99 or dollars. Nobody joins the numbers that actually decide the question, and doing it by hand in a spreadsheet before every rollout does not survive a real release schedule.

So this is one file with no dependencies beyond the OCaml standard library. No service, no API keys, no config file. Export from LiteLLM, OpenRouter, Vercel AI Gateway, Envoy, Cloudflare Workers AI, a Kubernetes inference service or an internal router, pipe the CSV in and get a deterministic verdict. Same input, same output, every time, which is what makes it usable as a release gate.

## When to use it

- Before shifting production traffic onto a new provider or model version, to check tail latency and cache economics rather than the sticker price.
- In CI on the routing config repo, with `--fail-on high`, so a route that breaches residency or headroom cannot merge.
- When finance asks why the inference bill moved and you need per route cost split into gross input, gross output and cache savings.
- When a compliance review needs proof that regulated data classes carry an explicit `required_jurisdiction` and every route matches it.
- When planning failover, to find routes whose rated capacity leaves no room to absorb a sibling route's traffic.
- When producing a carbon report per route from regional grid intensity and renewable share instead of a flat company average.

## How it works

Input is CSV from `--input PATH` or stdin. `parse_csv_line` is a hand written state machine over each line: it tracks an `in_quotes` flag, handles doubled `""` inside quoted fields and raises `Input_error` on an unterminated quote or a quote that does not start a field. Blank lines and lines starting with `#` are dropped before the header is taken. Headers go through `compact_header`, which lowercases and strips every character that is not a letter or digit, then `canonical_header` maps the result through an alias table, so `P95 Latency (ms)`, `p95_ms` and `latencyP95ms` resolve to one column and `qps` works as well as `rps`. `parse_rate_fraction` accepts `0.01`, `1%` or a bare `1` and normalizes all three. Optional columns get real defaults: `jurisdiction` falls back to `region`, `grid_gco2_per_kwh` to 450, `kwh_per_1k_req` to 0, `capacity_rps` to the observed `rps` and `criticality` to 0.5.

`score_route` does the arithmetic. Monthly requests are `rps *. seconds_per_month`, an average month of 30.4375 days, unless `--monthly-requests` overrides it for every row. Gross input and output cost come from tokens per request times the per 1k price. Cache savings are `gross_input_cost *. cache_hit_rate *. cache_input_discount`, defaulting to a 90 percent discount on cached input tokens, and monthly cost is the sum minus savings floored at zero. Carbon is requests per thousand times `kwh_per_1k_req` times grid intensity times `1 -. renewable_pct /. 100.`, converted to kg CO2e. Headroom is `capacity_rps /. rps -. 1.`, so 0.20 means the route can absorb 20 percent more than it carries today.

Checks emit `finding` records with a code, a severity, a message and one line of advice. Severity is graded, not binary: `severity_for_ratio` takes how far over budget a value sits and picks Medium, High or Critical from per check thresholds, so p95 at 1.1x budget is Medium and p95 at 1.8x is Critical. The codes are `latency_p95`, `latency_p99`, `error_rate`, `timeout_rate`, `prompt_cache`, `capacity_headroom`, `monthly_cost`, `carbon_budget`, `data_residency`, `missing_residency_rule` and `invalid_latency_shape`. Three are hard. A jurisdiction mismatch is always Critical, negative headroom is Critical, and a regulated data class with no residency rule is High. `invalid_latency_shape` catches p99 below p95, which means the metric export is broken and the row cannot be trusted.

Scoring is a weighted penalty subtracted from 100. Reliability is weighted hardest at 60 for error rate and 40 for timeouts, then residency at a flat 100, headroom 25, p99 22, p95 18, cost 20, cache 12 and carbon 10. The total is multiplied by `0.75 +. criticality *. 0.5`, so the same breach on a business critical route scores worse than on a batch route, then clamped to 0 to 100. The decision reads the worst finding first and the score second: any Critical is `block`, any High is `degrade`, Medium is `watch` above 75 and `degrade` below, and a clean route is `preferred` above 90 and `watch` below. `sort_scored` orders preferred, watch, degrade, block, then score descending, then route name so diffs stay stable. Output is `emit_text`, `emit_json` or `emit_csv`, the last flattening only the top finding per row.

## Usage

```bash
# see the expected schema
ocaml InferenceRouteBudget.ml --example > routes.csv

# human readable audit with defaults
ocaml InferenceRouteBudget.ml --input routes.csv

# from stdin, JSON for a pipeline
cat routes.csv | ocaml InferenceRouteBudget.ml --format json

# CI gate: fail the build on any high or critical finding
ocaml InferenceRouteBudget.ml \
  --input routes.csv \
  --max-p95-ms 700 --max-p99-ms 2000 \
  --max-error-rate 0.5% --max-timeout-rate 0.2% \
  --min-cache-hit-rate 45% --min-headroom 0.30 \
  --max-monthly-cost-usd 25000 --max-monthly-carbon-kg 400 \
  --require-residency true --fail-on high

# compare routes at one fixed volume, best 5 only, as CSV
ocaml InferenceRouteBudget.ml --input routes.csv \
  --monthly-requests 50000000 --format csv --top 5

# compile it for CI
ocamlopt InferenceRouteBudget.ml -o irb
./irb --input routes.csv --format json --fail-on critical
```

## Notes

- Required columns: `route`, `provider`, `model`, `region`, `rps`, `p95_ms`, `p99_ms`, `error_rate`, `cost_usd_per_1k_input`, `cost_usd_per_1k_output`, `input_tokens_per_req`, `output_tokens_per_req`. Everything else is optional. A missing required column aborts the run, not just the row.
- Exit codes: 0 clean, 2 a finding reached `--fail-on` (default `high`), 64 config error, 65 input error, 66 system error. `--example` prints a sample CSV and exits 0.
- Cost is a static model: average tokens per request times the price you supply. It knows nothing about committed spend discounts, tiered or batch pricing, reasoning tokens billed separately, or vendor cache pricing that differs from the flat `--cache-input-discount`.
- Carbon is an estimate from inputs you provide. Leave `kwh_per_1k_req` out and it defaults to 0, so every carbon number is 0. The 450 gCO2 per kWh default is a placeholder, not a measurement.
- `capacity_rps` defaults to the observed `rps`, which yields zero headroom and a High finding. That is deliberate. An unknown ceiling should not read as a safe one.
- `--top N` truncates after sorting and the exit code is computed on the truncated list, so a blocking route below the cut will not fail the build. Use `--top` for reading, not for gating.
- The CSV parser handles quoting and embedded commas per line. A newline inside a quoted field is not supported, since input is read line by line.
