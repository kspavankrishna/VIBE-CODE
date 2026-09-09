# Inference Spend Reconciliation Ledger

Your LLM gateway dashboard, your provider invoice CSV and your own usage telemetry all report a different number for the same month, and nobody can say which one is right. This is a dependency free C# CLI that recomputes every request from a price book and tells you exactly where the three disagree.

**Language:** C# | **Lines:** 1022 | **Added:** 2026-08-15

## What this solves

AI inference spend is one of the few production costs where the system that charges you, the system that logs you and the system that reports to finance are three separate pipelines nobody reconciles. The gateway writes `cost_usd` into its trace. The provider sends a CSV at month end. Your telemetry has token counts. Everyone assumes these agree. They usually do not, and the gap stays invisible until finance asks why the bill jumped 40 percent.

The failure modes are boring and expensive. A retry loop fires the same idempotency key four times because the client treated a 504 as a hard failure, so you pay four times for one answer. A prompt cache is misconfigured: `cached_input_tokens` gets reported but every token bills at full input rate, wiping out the discount you budgeted for. A model is renamed on the provider side, your price book still has the old entry, and a tenant's whole spend falls out of cost attribution. A failed request returns a 500 and still carries tokens and a charge. An invoice row references a request id that exists nowhere in your telemetry.

None of that trips an alert. No exception, no failed deploy, no page. The cost lands in an invoice weeks later, by which time the traces have rotated out of retention and you cannot prove anything. What follows is a spreadsheet built by hand under time pressure, exactly the artifact you do not want load bearing.

This makes the disagreement explicit and machine checkable. It reads your JSONL usage events, recomputes every request from a CSV price book, then compares that independent estimate against the cost the gateway declared inline, the provider invoice at request and tenant/provider/model/day level, and the tenant budgets you committed to. Every mismatch becomes a severity ranked finding with evidence attached.

## Why I built it

The options are a vendor dashboard you cannot audit, a FinOps SaaS product that wants your API keys and your telemetry shipped to its cloud, or a spreadsheet. The dashboard is the thing under suspicion, so it cannot be the referee. The SaaS product is a paid dependency and a data egress decision for what is a few hundred lines of arithmetic. The spreadsheet does not run in CI.

So: one file, no NuGet packages, no network calls, no credentials. It reads files off disk and exits non zero when the numbers do not line up. Wire it into a GitHub Actions or Azure Pipelines step, point it at last night's telemetry export and let it fail the job when a tenant blows a budget or an invoice drifts. SARIF output falls out of that, because the findings then render in GitHub code scanning.

## When to use it

- The provider invoice does not match what your gateway dashboard said for the same period and you need the delta broken down by tenant, model and day.
- You suspect a retry storm or a broken idempotency implementation and want the dollar value of the duplicates, not a count of log lines.
- You enabled prompt caching and want proof the discount shows up in the billed amount.
- You run a multi tenant AI platform and need a nightly job that fails when a tenant crosses its daily or monthly budget.
- You added a new model and want every event confirmed against a price rule before the first invoice arrives.
- A cost figure is going in front of finance or an auditor and you need a reproducible artifact rather than a spreadsheet.

## How it works

Five stages: parse, price, validate, cross check, emit. `Main` wires them together and returns an exit code derived from the highest severity finding.

Ingestion is deliberately forgiving, because usage telemetry is never in the shape you want. `ReadUsageLines` parses each JSONL line into a `JsonDocument`, then `Flatten` writes every leaf into a dictionary twice: under its dotted path such as `usage.prompt_tokens_details.cached_tokens`, and under its bare leaf name. `Normalize` strips non alphanumeric characters and lowercases the rest, so `promptTokens`, `prompt_tokens` and `Prompt-Tokens` collapse to one key, first writer winning. Above that sit alias resolvers `Text`, `Long`, `MoneyOrNull` and `Time`, each taking an ordered list of candidate field names, which is how OpenAI, Anthropic, Azure, Bedrock and gateway shapes all land in the same `UsageEvent` record. `Time` takes ISO 8601 strings and epoch numbers, switching seconds to milliseconds above 10,000,000,000.

Pricing uses `PriceRule` records read from CSV by a hand rolled `ParseCsvRows` state machine that handles quoted fields and doubled quote escapes, so no CsvHelper dependency. Rules match by glob via `Wild`, and specificity is resolved by a `Score` property: an exact provider is worth 3, a wildcard provider 1, a bare `*` 0, with the model side scoring 6, 2 and 0. Rules sort descending by score and the first match wins, so `openai/gpt-4o-2026-01` beats `openai/gpt-4o*` beats `*/*`. `Estimate` does the arithmetic in `decimal`, never float: cached tokens clamp to at most the input count, remaining input bills at the input rate, cache reads and cache writes get their own per million rates, and a `minimum_request_usd` floor applies last. Every money value passes through `Round`, six decimals with `MidpointRounding.AwayFromZero`, so results are identical across runs and machines.

`ValidateEvent` then runs per request integrity checks needing no external source: `missing-request-id`, `missing-or-invalid-timestamp`, `negative-token-count` at critical, `cache-tokens-exceed-input` at high, `failed-request-billed` when a non success status still carries tokens or a charge, and `unpriced-model` under `--require-prices`. Success is inferred loosely by `UsageEvent.Success`: an empty status, `ok`, `success` or any status starting with `2`.

Cross checking is where the value is. `AddReplayFindings` groups events by `StableKey`, which prefers the idempotency key and falls back to the request id, orders each group by timestamp and flags every later event landing inside `--replay-window-min` of the first, carrying the summed dollar value of the duplicates and escalating to critical above 25 dollars of waste. `AddInvoiceFindings` runs two passes, request level by request id and group level on the `Scope` key of tenant/provider/model/date: missing on either side is a high finding, present on both goes through `AddDrift`, which fires only when the gap exceeds both `--max-drift-usd` and `--max-drift-pct`, then escalates to critical past 20 percent. `AddBudgetFindings` sums estimated spend per tenant per day and per month, taking the month key as the first seven characters of the date key. Output is `text`, `json` or `sarif` 2.1.0, and `--self-test` pushes four synthetic events through the whole pipeline, asserts five specific finding codes appear and reparses the JSON and SARIF to confirm both are valid.

## Usage

```bash
# build, single file, no packages
csc -nologo -out:InferenceSpendReconciliationLedger InferenceSpendReconciliationLedger.cs

# verify the build before trusting it
./InferenceSpendReconciliationLedger --self-test

# minimum useful run
./InferenceSpendReconciliationLedger --telemetry usage.jsonl --prices prices.csv

# full reconciliation in CI, SARIF for code scanning
./InferenceSpendReconciliationLedger \
  --telemetry usage.jsonl \
  --prices prices.csv \
  --invoice invoice.csv \
  --budgets budgets.csv \
  --max-drift-pct 1.0 \
  --max-drift-usd 0.05 \
  --replay-window-min 1440 \
  --require-prices \
  --format sarif \
  --fail-on high > findings.sarif
```

CSV shapes the tool expects:

```
# prices.csv (provider and model accept * globs)
provider,model,input_usd_per_million,output_usd_per_million,cached_input_usd_per_million,cache_write_usd_per_million
openai,reasoning-small,1.00,4.00,0.10,1.00

# invoice.csv
tenant,provider,model,date,request_id,amount_usd

# budgets.csv
tenant,daily_budget_usd,monthly_budget_usd
```

## Notes

- Exit codes: 0 clean or below the fail threshold, 2 findings at or above `--fail-on`, 64 bad usage, 65 malformed JSON, 74 IO error, 1 anything else.
- Without `--prices` nothing gets an estimate, so drift, invoice and budget checks go quiet. The tool is only as correct as that CSV.
- Everything ends up in memory. `File.ReadLines` streams the file, but events, the costed list and the grouping dictionaries live in RAM at once. Split very large exports by day first.
- `AddDrift` requires the gap to exceed both the USD and the percentage threshold. Set either to 0 to make the other the only gate.
- Unparseable timestamps become `DateTimeOffset.MinValue` with an `unknown` date key, and `Inside` treats a missing timestamp as inside the replay window. Conservative by design, so undated data can produce false replay findings.
- Cost comes from token counts only. No tiered or volume pricing, no committed use discounts, no currency but USD, no tax, and no per request minimum beyond the optional `minimum_request_usd` column.
- It only reads files. No network calls, no API keys, no provider contact, which is why it is safe to run in CI against exported telemetry.
