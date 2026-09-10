# Model Gateway Spend Firewall

An AI model gateway routes traffic to OpenAI, Anthropic, Google, Mistral and local inference, and nobody can prove a request was inside budget, inside quota, inside the allowed data region and free of secret-looking prompt material until the invoice arrives. This is a single file PHP CLI that reads raw gateway events and answers that before finance does.

**Language:** PHP | **Lines:** 1089 | **Added:** 2026-08-21

## What this solves

The failure mode is quiet. A retry loop on a streamed request with no `max_output_tokens` bound runs for an hour against a 75 dollar per million token output model. A free tier tenant with a 1 dollar daily budget sends a 900k token prompt. A batch job falls back to a provider that was never on the allow list, in a region your DPA does not cover. None of these throw an error. The gateway returns 200, the job completes, and the number lands 20 days later as a line item nobody can attribute.

Without a control, the bill arrives and someone reconstructs it in a spreadsheet from half remembered token prices. What actually breaks: per request cost caps that live in a design doc and nowhere in code, tenant budgets enforced by trust, prompt caches configured but barely hit so you pay full input price on repeated prefixes and retryable calls with no idempotency key that double charge when the gateway times out after the provider already accepted the request.

The second failure is not about money. Prompt previews and metadata end up in gateway logs carrying API keys, bearer tokens and AWS access key ids more often than anyone admits, and from there they reach your log vendor and your backups. This scans both against a configurable secret pattern list, reports a match as critical and records a SHA-256 digest as evidence rather than the text itself.

## Why I built it

Provider dashboards tell you what you spent, not whether the spend was allowed. Observability tools give you traces and latency, not a price book with cached input rates. Commercial LLM gateways that enforce budgets want your production traffic routed through them, a large trust and latency decision to make for a policy check. And none of it drops into a PHP repository, where plenty of backend traffic still originates.

So: one file, no dependencies, exported gateway events plus a policy JSON, every rule in one deterministic pass, non zero exit when something crosses a line. Same events plus same policy gives the same findings in the same order, which is what makes it a CI gate rather than a dashboard.

## When to use it

- A nightly CI job over yesterday's gateway JSONL, gated with `--fail-on high`, so a runaway tenant fails a pipeline instead of a budget.
- After adding a provider or model route, to confirm the price book covers it and the route is not silently unpriced.
- When your cache hit rate looks wrong and you want per request evidence of large prompts barely using the prompt cache.
- Before signing off on data residency, to prove no request landed outside the allowed regions.
- When you suspect duplicate charges after gateway timeouts and want every retryable call missing an idempotency key listed.

## How it works

Input goes through `MgInputParser`. It tries a whole document `json_decode` first, unwrapping a top level list or an `events`, `requests`, `rows`, `data` or `spans` key, and falls back to line by line JSONL when that throws. Each row becomes an `MgEvent` via `MgEvent::fromRow`, deliberately forgiving about field names: `input_tokens` or `prompt_tokens`, `tenant` or `tenant_id` or `workspace` or `project`, nested under `usage`, `request`, `span` or `attributes`. That is what lets it eat OpenAI style usage blocks, OpenTelemetry span attributes and homegrown logs with no translation layer.

All money is integer micros, never floats. `MgMoney::usdToMicros` converts once at policy load and `MgMoney::tokenCostMicros` does ceiling division (`intdiv($tokens * $micros + 999_999, 1_000_000)`) so a partial million never rounds down in the vendor's favour. Event cost is paid input at the input rate, cached input at the cached rate and output at the output rate, with cached tokens clamped by `min($cached, $input)` so a bad log line cannot produce negative paid input.

`MgPolicy` holds the price book, context windows, budgets and limits. Provider and model matching is glob style through `MgText::wildcard`, which lowercases both sides, runs `preg_quote` and turns escaped `\*` back into `.*`. That is why `claude-opus-4*`, `legacy-*` and `*unsafe` all work in the price book, block lists and context window table. First matching pattern wins, so order matters.

`MgFirewall::analyze` runs a per event pass then an aggregate pass. Per event it prices the call, emits `UNKNOWN_MODEL_PRICE` when nothing matches, then runs `checkProviderAndModel`, `checkRegion`, `checkContext`, `checkStreamingAndRetries`, `checkSecrets`, `checkCacheEfficiency` and `suggestCheaperRoute`. Meanwhile it accumulates three maps keyed by tenant: spend per UTC day, request count per fixed 60 second bucket (`intdiv($timestamp, 60) * 60`) and token count per the same bucket. Fixed buckets, not a sliding window, which is what most gateways enforce and is cheap and order independent. The aggregate pass reports `TENANT_DAILY_BUDGET_EXCEEDED` once per tenant day plus `TENANT_RPM_LIMIT_EXCEEDED` and `TENANT_TPM_LIMIT_EXCEEDED` where a bucket is over.

`suggestCheaperRoute` brute forces the price book. For each allowed, unblocked provider and model pattern it builds a synthetic `MgEvent` with the same token shape, skips candidates whose context window cannot hold the request, prices it and keeps the minimum. It emits `CHEAPER_ROUTE_AVAILABLE` at info only when the best candidate costs under 65 percent of the actual. Findings are `MgFinding` value objects sorted by severity then rule id, and `MgReporter` renders JSON, a Markdown table or SARIF 2.1.0 with file and line as the physical location, which puts findings inline in GitHub code scanning.

## Usage

```bash
# JSON report from an exported gateway log
php ModelGatewaySpendFirewall.php --input events.jsonl --policy policy.json

# Start from a generated policy document
php ModelGatewaySpendFirewall.php --example-policy > policy.json

# CI gate: fail on high or worse, SARIF output
php ModelGatewaySpendFirewall.php \
  --input gateway-events.json \
  --policy policy.json \
  --format sarif \
  --fail-on high > firewall.sarif

# Ad hoc overrides, reading stdin
cat events.jsonl | php ModelGatewaySpendFirewall.php \
  --per-request-cap-usd 1.00 \
  --rpm-limit 60 --tpm-limit 200000 \
  --allow-provider anthropic --allow-provider openai \
  --block-model 'legacy-*' \
  --allow-region eu \
  --format markdown

# Embedded regression checks
php ModelGatewaySpendFirewall.php --self-test
```

Bare arguments are input paths, `--input` may be repeated, and with no inputs it reads stdin.

## Notes

- Exit codes: 0 below the gate, 2 when a finding is at or above `--fail-on` (default `high`), 1 on any error with the message on stderr.
- This is an offline analyzer over logged events, not an inline proxy. It blocks nothing live. `MgFirewall` is a plain class though, so you can build `MgEvent` objects in your own request path and call `analyze` there.
- Costs are estimates from your price book. They will not match an invoice: no cache write charges, no batch or committed use discounts, no image, audio or tool call pricing. The built in defaults will go stale.
- Rate limit findings fire per offending event, so one hot minute produces one finding per request in that bucket. Budget findings are deduplicated to one per tenant day.
- Secret detection is regex only, over prompt preview and metadata flattened to 16000 characters. It stops at the first matching pattern, and a malformed pattern in `secret_patterns` is silently skipped.
- CLI list overrides replace the matching policy file list rather than appending to it. `--allow-provider openai` discards `allowed_providers` from the policy JSON.
- Requires PHP 8.1 or newer for readonly properties and `array_is_list`. No Composer packages, nothing beyond core JSON and PCRE.
- Cheaper route suggestions consider token shape and context window only. They know nothing about capability, quality or latency.
