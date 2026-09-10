# OTel Inference Cost Ledger

Your LLM bill tripled overnight and nobody can say which service, route, model or tenant did it. This is a single file C# tool that joins OpenTelemetry spans with gateway usage records and prints a cost and latency ledger you can hand to finance and to the on call engineer.

**Language:** C# | **Lines:** 1338 | **Added:** 2026-05-24

## What this solves

Most teams running production inference already have all three pieces of evidence and still cannot answer the question. The provider invoice says the number. The OpenTelemetry traces say which service and route were hot. The gateway log says how many tokens each call burned. Nothing joins them. So when a model rollout doubles spend on a Tuesday, somebody opens a spreadsheet and starts pasting trace ids, request ids and token counts by hand. That takes a day, it is wrong by the time it is finished, and the answer arrives after the spend has already happened.

The specific failure mode is attribution loss. Usage records from an AI gateway carry a request id and token counts but usually no service name, no HTTP route and no tenant. Spans carry service, route and tenant but usually no token counts and no cost. Without the join you can see that spend went up. You cannot see that one internal batch job on `/v1/summarize` for one enterprise tenant is responsible for 80 percent of it, or that a silent fallback from a cheap model to an expensive one is what actually changed. Finance notices at invoice time. Engineering notices when someone complains. Nobody notices during the incident, which is the only moment the information is worth anything.

The second failure mode is quiet cost drift. A prompt cache stops hitting and input token cost goes up 4x with no error and no code change. A model id changes shape after a provider migration and every price lookup silently returns nothing, so your internal dashboard reports a cost that is simply too low. This tool refuses to hide any of that: unmatched usage rows are printed, models with no price are printed by name, and cache hit ratio is a first class column. If you want a build to fail when a migration pushes p95 past your SLO or when a new model id has no price entry, it exits 2 on a guard failure and 1 on bad input.

## Why I built it

The existing options are either a SaaS product that wants your token stream, or a Grafana dashboard that shows spend by model and stops there. Neither gives you per tenant, per route attribution from files you already have on disk, and neither runs inside an incident container with no network egress. I wanted something I could scp into a locked down build agent and run against two JSONL dumps.

It is deliberately dependency free. `System.Text.Json` and the BCL, nothing else. No database, no collector, no agent, no SaaS account. That constraint is the point: it works in CI jobs, cron, Kubernetes one shot jobs and an incident shell where installing anything is not an option.

## When to use it

- The monthly inference bill jumped and you need to name the service, route, tenant and model responsible before the next invoice.
- You are migrating a route from one model to another and want evidence that cost per request went down while p95 latency did not go up.
- A prompt cache regression is suspected and you want cache hit ratio broken down by route and tenant.
- You need per tenant cost numbers for chargeback or for a customer conversation, and the gateway log alone does not know who the tenant was.
- You want a CI gate that fails a build when a new model has no price entry, when usage rows stop matching spans or when any attributed call breaches the latency SLO.
- You are in an incident shell with a span dump and a usage dump and no ability to install tooling.

## How it works

Two JSONL files go in. `SpanIndex.Load` reads the span file one line at a time and builds `TraceSpan` records, and `UsageFile.Load` does the same for usage records. Both parsers are aggressively tolerant of shape, because span exporters disagree with each other. `AttributeReader.ReadAll` merges `attributes`, `resource/attributes`, `resourceAttributes` and `scope/attributes`, handling both the plain object form and the OTLP key/value array form where the value is wrapped in `stringValue`, `intValue`, `doubleValue` or `boolValue`. `JsonAccess` then walks slash separated paths and coerces scalars, so `usage/prompt_tokens_details/cached_tokens` and a flat `cached_input_tokens` both land in the same field.

Field resolution is a fallback chain, not a fixed schema. Service walks `service.name`, `service_name`, `resource/service.name`, then the literal `unknown-service`. Route walks `http.route`, `url.path`, `http.target`, `rpc.method`. Tenant walks `tenant.id`, `organization.id`, `enduser.id`, `user.id`. Model takes `gen_ai.response.model` before `gen_ai.request.model`, which matters because the response model is what you were actually billed for after any provider side routing. Latency comes from `duration_ms`, otherwise from `start_time_unix_nano` and `end_time_unix_nano`, otherwise from parsed ISO timestamps.

The join is the interesting part. `SpanIndex` builds three lookups: a composite trace id plus span id dictionary for exact hits, a request id multimap and a trace id multimap, all case insensitive. `FindBest` tries the exact trace and span pair first, then request id, then trace id. When a bucket holds more than one candidate span, `PickBest` scores each one: 100 points for a matching request id, 40 for a matching trace id, 25 for a matching model, 15 if the span looks inference shaped, 4 if it names a service. Ties break on longer latency, then on file order, which biases toward the parent HTTP span rather than a child retry. A span counts as inference like if it has a model, or any `gen_ai.*` or `llm.*` attribute, or its operation name contains `responses`, `chat.completions`, `embedding` or `inference`.

Costing runs provider first. `CostAttributor.Build` takes the provider reported cost from the usage row if the gateway supplied one, tagging the source `provider`. If not, it falls back to `PriceBook` and `ModelPrice.Estimate`, tagged `price-book`. If neither exists the row is tagged `missing-price` and the cost is left null rather than assumed zero, so your totals are honestly incomplete instead of quietly wrong. `PriceBook.Find` tries the exact model key and then a normalized one that strips a provider prefix before the last slash and anything after a colon, so `openrouter/anthropic/claude-3-7-sonnet:beta` still resolves. `Estimate` splits input tokens into cached and uncached and prices them at separate per million rates. `UsageRecord` clamps cached tokens to input tokens and forces total tokens to be at least input plus output, so a bad row cannot produce a negative bill.

Attributed entries are grouped by the four tuple `LedgerGroupKey(Service, Route, Model, Tenant)` and reduced by `LedgerGroup.FromEntries`. Each group gets request count, known cost, token sums, cache hit ratio and p50, p95, p99 latency from a nearest rank quantile over the sorted latency array. Groups are ordered by known cost descending. `LedgerReport.ToHumanText` prints a fixed width table of the top N, then names the models with no price, then lists up to eight unmatched usage rows with their line numbers so you can go look at them. `WriteJson` emits every group, not just the top N, for downstream tooling.

## Usage

```bash
# minimum run
dotnet run -- --spans spans.jsonl --usage usage.jsonl

# full attribution with a price book and a machine readable report
dotnet run -- \
  --spans spans.jsonl \
  --usage usage.jsonl \
  --prices prices.json \
  --json-out ledger.json \
  --latency-slo-ms 15000 \
  --top 30

# CI gate: exit 2 if anything is unmatched, unpriced or over SLO
dotnet run -- --spans spans.jsonl --usage usage.jsonl --prices prices.json \
  --fail-on-unmatched --fail-on-missing-price --fail-on-slo-breach

# start a price book
dotnet run -- --price-template > prices.json

dotnet run -- --help
```

Price book shape, per million tokens:

```json
{
  "gpt-4.1-mini": { "input_per_million": 0.40, "cached_input_per_million": 0.10, "output_per_million": 1.60 }
}
```

## Notes

- Exit codes: 0 on success, 1 on bad input (missing file, unknown flag, malformed JSON, IO failure), 2 when a `--fail-on-*` guard trips. Guards are opt in, so a plain run only fails on input errors.
- Both inputs must be JSONL, one JSON object per line. Blank lines are skipped. A non object line is a hard error naming the line number. A usage file with zero records is an error, an empty span file is not.
- Everything is loaded into memory. Spans are held as a list plus three dictionaries. Fine for an incident sized dump, not designed for a hundred million rows.
- Unmatched usage rows still appear in the ledger with `unmatched-service`, `unmatched-route` and `unmatched-tenant` placeholders, so their tokens and cost are never dropped from the totals. They just group separately.
- Rows tagged `missing-price` contribute zero to `known_cost`. The report labels it "known cost" for exactly that reason. Read the missing price count before trusting the total.
- No ingestion, no collector, no live tailing, no alerting and no currency conversion. It reads two files, prints a report and exits. Costs are US dollars, formatted to four decimal places.
- Provider reported cost always wins over the price book. If your gateway emits a wrong `cost` field, the price book will not correct it.
