# Inference Invoice Reconciler

Your LLM provider invoice says one number and your gateway traces say another. This is a Haskell command line tool that matches every trace to a billing row, recomputes the cost from a pricing card and tells you exactly where the money went missing.

**Language:** Haskell | **Lines:** 356 | **Added:** 2026-05-07

## What this solves

LLM billing reconciliation fails on the ugly details. Provider invoice exports live in one place and gateway or SDK traces live somewhere else, in different formats, with different field names, and nobody ever diffs them line by line. The failure modes stay invisible until someone stares at a month end spend spike and cannot explain it.

The failures this catches are the ones that cost money. A retry loop bills the same request twice, so you pay for a call your application made once. Cached prompt tokens get billed as uncached input, which erases the discount you built the whole prompt architecture around. Invoice rows arrive with no matching trace, meaning you are paying for traffic you did not generate or something else is calling your key. Traces arrive with no invoice row, meaning your internal cost dashboard under-reports and your unit economics are wrong. Token counts drift between what the gateway recorded and what the provider billed.

Nobody notices for weeks. Finance notices first, usually as a variance they cannot explain, and by then the raw exports have rolled off. Engineering gets asked to justify a number it has no independent way to check, because the only source of truth for cost is the invoice itself. That is a bad place to argue from.

This tool gives you the second source of truth. It reads both sides, normalizes them, matches request by request, recomputes what each call should have cost from your own pricing card and reports every disagreement with a dollar amount attached. It exits non zero on real errors, so it can sit in CI or in a pre close gate instead of being run by hand and hoped over.

## Why I built it

Cloud cost tools do not understand token pricing, and the LLM observability platforms that do are usually reading the same telemetry stream that produced the traces. Reconciling a system against itself proves nothing. What was missing was a small, boring, auditable thing that reads two independent exports off disk and disagrees with both when they disagree with each other.

The other reason is format sprawl. Invoices come as CSV, gateway logs as JSONL, SDK dumps as one big JSON array, and output tokens is called `output_tokens` or `completion_tokens` or `usage.output_tokens` depending on who wrote the exporter. A bespoke pandas script per vendor is how this work never gets done. I wanted one binary you point at two paths.

## When to use it

- Month end close, before finance signs off on an AI spend line they cannot independently verify.
- After a spend spike, when you need to know within an hour whether it was volume, retries or a caching regression.
- Right after you enable prompt caching, to confirm the provider is actually applying the cached rate and not billing every token as fresh input.
- When you route through a gateway (LiteLLM, a proxy, an internal router) and want proof that every request that left your network shows up on exactly one invoice line.
- Before you trust a new cost dashboard, as a one off audit against the raw provider export.
- In CI on a nightly export pull, so a billing regression fails a job instead of quietly accruing.

## How it works

The pipeline is: walk, parse, flatten, normalize, match, price, report. `main` parses flags into a `Config`, runs `validate` to confirm the paths exist and the thresholds are non negative, then calls `loadRows` on both paths. `walk` recurses a directory or accepts a single file, keeping anything whose extension passes `okExt`: `.json`, `.jsonl`, `.ndjson`, `.csv` and `.tsv`. `parseFile` dispatches on extension. Delimited files go through `parseDelimited`, which uses a hand rolled `splitCSV` that tracks quote state in a fold so commas inside quoted fields survive. A `.json` file is first tried as one whole document via `rowsFromValue`, and if that decode fails it falls back to `parseJsonl`. Parse errors are retained, not swallowed, and surface later as `ParseWarn` findings with file and line number.

Every row becomes a flat `Map Text Text` through `flatObj`, which walks nested objects and joins the path with dots, so `usage.input_tokens` is reachable. Arrays of scalars collapse to a comma joined string and numbers render with `formatScientific Fixed`, so you never get `1.0e3` where you expected `1000`. Keys pass through `normKey`, `normSeg` and `squash`: lowercase, non alphanumeric characters become underscores, runs collapse, leading and trailing underscores drop. That is what makes `Input Tokens`, `input-tokens` and `INPUT_TOKENS` the same field.

`rowToRec` then builds a `Rec` using `lookupAny`, which tries a list of aliases and accepts either an exact normalized key or a unique dotted suffix match, so `output_tokens` finds `response.usage.output_tokens` and stays silent when the suffix is ambiguous. `num` strips `$`, `USD` and thousands separators before reading a number, and `readTime` takes ISO 8601 via `iso8601ParseM` or a bare epoch value, dividing by 1000 above 1e12 so millisecond timestamps work. A row is kept only if it has an anchor: a request id, a provider or model, or one of input tokens, output tokens or cost.

Matching lives in `reconcile`, a stateful left fold over the traces carrying a `Set` of consumed billing subjects, so no invoice row is ever spent twice. `exact` maps request id to billing rows. If the trace id hits that map, all unconsumed rows under it are taken, and more than one produces a `Duplicate` finding whose amount is the sum of the surplus rows. With no id match, `--require-exact-request-id` stops there and reports `Missing`. Otherwise `fuzzy` runs: candidates must agree on provider and model where both sides have the field, hold all three token counts within `--token-delta`, sit within `--time-slop-seconds`, and pass `anchored`, which requires the pair to share at least one of input tokens, output tokens or a timestamp. Exactly one survivor is a match. Two or more is an `Ambiguous` error rather than a guess, deliberately. Zero is `Missing`. Billing rows the fold never consumed come out as `Orphan`.

Cost checking needs `--pricing`. `pricesFrom` walks the pricing JSON at any depth into a `Map Text Price` keyed `provider/model`, reading `input_per_1m`, `output_per_1m` and their aliases, defaulting the cached rate to the input rate when absent, and taking provider and model from explicit fields or from the nesting path, including a single `openai/gpt-4o` style segment. `expectedCost` charges `input - cached` at the input rate, `cached` at the cached rate and output at the output rate, per million. `uncachedCost` is the same math with the discount removed. `CostMismatch` fires when billed differs from expected by more than `--cost-delta-usd` and carries the signed delta, so you can total the exposure. The `CacheMiss` heuristic fires when the trace shows meaningful cached tokens and either the invoice under-reports cached tokens, or the billed amount sits at or above the fully uncached cost while exceeding the discounted cost. That second clause is the real caching regression detector.

`report` prints record counts, active thresholds, a count per finding kind, the observed billed total and up to 60 findings sorted by severity, kind and subject. `main` exits 2 if any finding has severity `Err`.

## Usage

```sh
# Build (needs aeson, containers, text, time, scientific, vector, directory, filepath)
ghc -O2 InferenceInvoiceReconciler.hs -o inference-invoice-reconciler

# Positional form: traces first, billing second. Either can be a file or a directory.
./inference-invoice-reconciler ./traces/2026-04/ ./invoices/openai-april.csv

# Flag form, with a pricing card so cost and cache checks run
./inference-invoice-reconciler \
  --traces ./gateway-logs/ \
  --billing ./invoices/anthropic-april.jsonl \
  --pricing ./pricing.json

# Tighten the thresholds for a strict pre close gate
./inference-invoice-reconciler traces.jsonl billing.csv \
  --pricing pricing.json \
  --time-slop-seconds 120 \
  --token-delta 0 \
  --cost-delta-usd 0.001 \
  --require-exact-request-id
```

Defaults: `--time-slop-seconds 600`, `--token-delta 32`, `--cost-delta-usd 0.02`. `--help` and `-h` print the usage block.

## Notes

- Exit codes: 0 when nothing crossed the thresholds, 2 when any `Err` finding exists, 64 for bad flags, missing paths, negative thresholds or `--help`.
- Without `--pricing` you still get `Missing`, `Orphan`, `Duplicate`, `Ambiguous` and `TokenMismatch`. `CostMismatch` and `CacheMiss` need the pricing card, and a pricing file that fails to read or decode silently yields an empty map instead of an error, so check the "Pricing cards" count in the header.
- Output is capped at 60 findings. The per kind counts are complete, the printed list is not.
- It never writes anything and never touches the network. Reads files, prints text, exits.
- `same` treats a missing provider or model on either side as agreement, so exports that omit the model field fuzzy match more loosely than you might expect. Use `--require-exact-request-id` when both sides carry ids.
- `Duplicate` fires only through the exact request id path. Two invoice rows for the same call under different ids surface as an `Orphan` instead.
- `matchTrace` returns `Right Nothing` in one branch and `Right (rows, seen)` in another, and `step` matches on both. Those shapes do not unify, so compile it before you depend on it and settle that branch on one return type.
- Dependencies: aeson, containers, text, bytestring, scientific, vector, time, directory, filepath. No CSV library, the delimiter parser is in the file.
