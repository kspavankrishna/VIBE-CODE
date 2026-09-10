# Prompt Cache Lease Governor

Prompt caching saves real money right up until it caches the wrong thing: a customer record in the wrong region, a credential inside a tool schema, or a policy preamble that went stale three deploys ago. This is a single file Kotlin CLI that scores every cacheable prompt block on cost, hit rate, freshness, tenant budget, data residency and sensitivity together, then tells you to pin, refresh, evict, bypass or quarantine it.

**Language:** Kotlin | **Lines:** 651 | **Added:** 2026-08-13

## What this solves

The prompt cache lease problem shows up when AI agents, RAG systems, MCP tools, long context applications and prompt caching platforms start reusing the same expensive system prompts, tool schemas, retrieval preambles, policy text and customer context. A cache miss wastes money. A bad cache hit is worse: it can leak private data, break data residency or serve stale instructions that quietly change model behavior. Most teams only ever measure the first one.

Here is the failure mode. Someone adds caching to cut input token spend. Hit rate looks great. Six weeks later a ticket reveals the retrieval preamble for a regulated tenant was cached in a region that tenant's contract does not allow, because nobody wired `allowed_regions` into the cache key. Or a tool schema picked up an API key in a default argument and that block has been resident in a shared cache for a month. Nobody notices, because caches are invisible when they work and the only alarm anyone built was on cost.

The quieter failure is economic. A block that changes four times a day gets cached anyway, so you pay to rebuild it four times and collect almost nothing back. A block with a 12 percent hit rate stays pinned because someone pinned it once. One enormous block eats 60 percent of a tenant's daily cache budget. Token savings alone cannot tell you which is which, because they know nothing about rebuild cost, freshness limits or who owns the budget.

Then there is staleness. Cache a policy block with a four hour freshness SLA, let it sit for nine hours, and the model is answering with instructions the business already retired. No error, no log line, output that is simply wrong in a way that reads as confident. Whoever notices is usually a customer.

## Why I built it

Cache infrastructure gives you hit rate and TTL. Neither knows what is inside the block. FinOps tooling gives you cost per tenant but treats a prompt cache as one line item, so it cannot say which block to evict. Data governance tooling scans stores and pipelines, not the ephemeral context you assemble at request time. So the decision to cache a block lands on whoever wrote the feature, gets made once, and never gets revisited. I wanted it to be a scored, reproducible artifact instead: one CSV in, one lease decision per block out, each with a readable reason and a stable lease key, and a nonzero exit code when something crosses a line.

## When to use it

- You are turning on prompt caching for a multi tenant product and want a record of which blocks are safe to pin.
- A tenant has a data residency clause and you need to prove no cached context lands outside their allowed regions.
- Your cache bill jumped and you need the blocks whose rebuild cost is eating the savings.
- A prompt registry or MCP tool catalog grew past the point where anyone can eyeball it.
- You want CI to fail when any block scores CRITICAL, so a tool schema carrying secrets cannot ship cached.
- You are tuning TTLs by hand and want a number derived from change rate and freshness SLA instead of a round guess.

## How it works

Input is CSV, from a file or stdin. `Csv.read` is a hand written parser in the file, no dependencies: quoted fields, doubled `""` escapes, both `\n` and `\r\n` endings, and hard failures on an unterminated quote, an empty or duplicate header column, or a row with more fields than the header. Each row becomes a `Row`, which resolves columns through alias lists, so `hit_rate` or `cache_hit_rate`, `block_id` or `cache_key` land in the same slot. `PromptBlock.from` builds a typed record and fails loudly with the offending column name on a bad value.

`LeaseGovernor.scoreOne` computes four things before deciding anything. Stability is `exp(-changeRatePerDay / 3.5)` times a failure penalty of `min(0.8, observedFailures * 0.12)`, so a block that churns or fails often decays smoothly rather than falling off a cliff. Gross savings is `tokens * requestsPerDay * hitRate * pricePerMillion / 1e6`. Rebuild cost charges every change per day plus one extra refresh if the block is already past its freshness limit, and a small materialization term from `build_millis` comes off too. Net is gross minus both. Budget share is gross over the tenant's declared daily budget.

Privacy is scored separately. The `Sensitivity` enum carries a base risk (public 0.0, internal 0.15, private 0.55, secret 1.0), a PII flag adds 0.25, and any non public block without `retention_safe` adds 0.15, capped at 1.0. Confidence is the product of five factors: traffic volume against the policy minimum, hit rate against the policy minimum, stability, a priority multiplier of `0.5 + priority/2`, and `1 - privacy`. Because it is a product, one bad factor drags the whole score down, and privacy risk of 1.0 zeroes it outright.

The decision is an ordered gate chain, not a weighted sum, and it short circuits on the first match. Secret material quarantines at CRITICAL. A region outside `allowed_regions` quarantines at CRITICAL under strict residency, with the literal `any` acting as a wildcard. Private or PII context without a retention safe provider path quarantines at HIGH. Five or more observed failures, a hit rate under the floor, volume under the floor or net savings under the floor all route to `lowValue`, which returns EVICT if the block is currently cached and BYPASS if it is not. Over the budget share cap, past the freshness limit, fast changing and already halfway through freshness, or confidence under 0.35 all return REFRESH. Whatever survives every gate gets PIN. Ordering matters: safety gates run before economics, so a cheap block never buys its way past a residency violation.

TTL is derived, not configured per row: `maxStalenessMinutes * stability * (0.40 + confidence) / (1 + changeRatePerDay)`, rounded and clamped to the policy floor and ceiling. Each result carries a lease key, the first 24 hex characters of a SHA-256 over app version, tenant, provider, region, block id and content hash. Change the content hash or the region and the key changes, which is what makes it usable as an actual cache key. Rows without `state_hash` get a derived one so the key stays stable across runs.

Output is table, JSONL or Markdown, sorted by severity descending then tenant. `FailOn` compares the report's maximum severity against the threshold and exits 2 if it trips.

## Usage

```bash
# score a CSV, readable table, exit 2 only on CRITICAL (default)
kotlin PromptCacheLeaseGovernor.kt --input prompt-cache.csv

# markdown report for a PR comment
kotlin PromptCacheLeaseGovernor.kt -i prompt-cache.csv -f markdown

# stdin to JSONL, fail the build on anything HIGH or worse
cat cache-metadata.csv | java -jar PromptCacheLeaseGovernor.jar \
  --input - --format jsonl --fail-on high

# tighter economics, looser residency, custom TTL band
kotlin PromptCacheLeaseGovernor.kt -i blocks.csv \
  --min-hit-rate 55 --min-requests 200 --min-savings 1.50 \
  --max-budget-share 0.10 --min-ttl-minutes 15 --max-ttl-minutes 720 \
  --default-price-per-mtok 3.0 --allow-cross-region
```

Required columns: `tenant, provider, region, block_id, tokens, requests_per_day, hit_rate`. Optional: `allowed_regions, sensitivity, contains_pii, contains_secret, retention_safe, age_minutes, max_staleness_minutes, change_rate_per_day, build_millis, price_per_mtok, budget_dollars_per_day, priority, currently_cached, observed_failures, state_hash, source_path, tool_name`. Rate flags and rate columns accept either form: `0.42` and `42` both mean 42 percent.

## Notes

- Exit codes: 0 clean, 2 when max severity meets the `--fail-on` threshold, 64 for bad arguments, 65 for bad CSV or a bad row value, 66 when the input file cannot be read.
- It reads and prints. It never touches a cache, calls a provider or writes a file. The lease key and the decisions are yours to apply.
- Every judgment depends on the CSV being honest. Defaults are deliberately cautious: `retention_safe` defaults false, `contains_pii` defaults true when sensitivity is private, `contains_secret` defaults true when sensitivity is secret, and residency is strict unless you pass `--allow-cross-region`.
- Scoring is stateless and single pass. No history, no trend detection, no memory between runs, so a block that is slowly degrading looks the same as one that was always mediocre.
- Budget share uses gross savings against `budget_dollars_per_day`. A row without that column is treated as unbounded, so the budget gate never fires for it.
- The cost model assumes one input price per million tokens. It does not model separate cache write and cache read pricing tiers, and the materialization term is a fixed constant on `build_millis`, not a measured compute rate.
- The reader pads short rows with empty strings, so a truncated row silently falls back to defaults instead of erroring. Validate upstream if that matters.
