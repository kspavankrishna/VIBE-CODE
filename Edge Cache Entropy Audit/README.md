# Edge Cache Entropy Audit

Your CDN reports a 12 percent hit rate and nobody knows why. This is a single dependency free Node.js script that reads edge logs and tells you which routes burn origin capacity on unique cache keys, and which ones cache private data in a shared surface.

**Language:** JavaScript | **Lines:** 774 | **Added:** 2026-08-11

## What this solves

Edge caching fails silently. Nothing throws, nothing pages, no alert fires. A route ships with `utm_source` left inside the cache key, or somebody appends a session nonce to an API URL for debugging and never removes it. From that moment every request is a unique key and the cache stores exactly one object per user per visit. The dashboard still shows green. Origin CPU creeps up, p95 drifts, the bill grows, and six weeks later somebody opens a ticket asking why the CDN is not doing anything.

The second failure mode is worse and quieter. When a route sets `Vary: Authorization` and the CDN honors it inconsistently, or when a cache key omits the identity dimension entirely, one user's personalized response gets served to the next visitor. That is a cache poisoning incident and a data leak in one, and the people who notice are your customers. The same class of bug puts tokens, emails and JWTs into query strings, which then land in access logs, browser history, referrer headers, analytics pipelines and the cache key itself. Five copies of a secret you meant to keep in a header.

The third is cost. A route with a 0.9 key ratio is not a cache, it is an expensive proxy with a storage bill attached. Hit rate alone never tells you whether the key is wrong, the TTL is wrong or the route is genuinely dynamic. You need cardinality per route sitting next to hit rate per route before the answer is obvious. This gives you all three in one pass over logs you already have, with a CI exit code so a regression blocks the release instead of shipping.

## Why I built it

CDN dashboards show hit rate. They do not show cache key cardinality per route, and they do not tell you when a key contains something that should never have been in a URL. The vendor tools that come close are locked to one provider, so a stack running Cloudflare in front of Vercel with a Fastly shield has three consoles and no shared view. Meanwhile the secret scanners guarding your source tree never look at runtime logs, so a token in a query parameter passes every gate you own.

So: one file, no install step, no vendor SDK, eats whatever log format is already in the pipeline and answers the question directly. Which routes waste money, which routes leak, and is this build worse than the last one.

## When to use it

- Hit rate is under 20 percent on a route you were sure was cacheable and you need to know whether the key, the TTL or the Vary header is at fault
- You are putting an LLM response cache or a RAG search page behind an edge cache and want proof that per-user prompt identifiers are not landing in shared keys
- A security review asks whether tokens, emails or session identifiers ever appear in URLs and you need evidence from real traffic, not a grep over the router
- You inherited a Next.js, Remix or Worker codebase and want a map of what is actually cached
- You want CI to fail when a new route ships with `Vary: Cookie` or a sensitive query parameter
- Origin egress cost jumped and you need to attribute it to routes in one afternoon

## How it works

Input goes through `parseRecords`, which tries formats in order: whole document JSON, then JSONL line by line, then CSV via a hand written `csvLine` scanner that handles quoted cells and doubled quote escapes. `recordsFromJson` unwraps the usual envelope keys (`rows`, `events`, `records`, `logs`, `requests`, `data`) so a raw API dump works unmodified. Field discovery is table driven off the frozen `FIELDS` object: it tries a list of real world names per concept, for example `cf-cache-status`, `x-vercel-cache` and `edge.cache_status` for cache status, while `dig` resolves dotted paths like `http.route` into nested objects. Every concept has a CLI override for odd schemas.

Routes are normalized by `inferRoute` when no route field exists. It splits the path, decodes each segment and replaces anything identifier shaped: six or more digits becomes `:id`, a UUID becomes `:id`, sixteen or more hex characters becomes `:hex`, and any segment over 32 characters with Shannon entropy above 3.4 becomes `:token`. That last rule matters. Entropy over the character distribution separates a slug from a signed token without a pattern list, and `entropy()` is reused on query values through `highEntropy`, which demands length 24 or more after stripping separators plus a score of at least 3.7. Real tokens sit high on that scale, real words do not. When no cache key is logged at all, `fallbackKey` reconstructs the key the CDN most likely built: method, cleaned path, query parameters sorted alphabetically so ordering noise does not inflate cardinality, then the live values of every header named in `Vary`.

Counting is per route through `Counter`, a bounded frequency map. It stores up to `limit` distinct values (`--max-keys`, default 30000) and counts everything beyond that in an `overflow` integer, so `unique` stays approximately right while memory stays flat on hostile input. `addRecord` accumulates hits and misses via `cacheStatus`, which normalizes vendor vocabulary through the `HIT_WORDS` and `MISS_WORDS` sets, counts rows carrying `Authorization` or `Cookie` headers, flags keys over 512 characters and buckets sensitive and tracking parameters. Sampled values pass through `redact` or `redactKey` before reaching output.

`routeFindings` turns per route statistics into findings on fixed thresholds you can read in the source. Sensitive parameters, and `Authorization` or `Cookie` inside `Vary`, are critical. Cache hits observed alongside an auth or cookie header, a key ratio at or above 0.75 on 20 or more requests, and a hit rate below 15 percent with at least 15 misses are high. A key ratio at or above 0.45 is medium, as is tracking noise once the ratio passes 0.25. `Vary: User-Agent` is deliberately medium while other wide Vary headers are high, because user agent variance is usually incompetence rather than a leak.

Output goes through `render` to Markdown, JSON, SARIF 2.1.0 or CSV, and `shouldFail` compares maximum severity against `--fail-on` using the `RANK` map to set exit code 2. `--self-test` builds a 33 row fixture in a temp directory, asserts four specific rules fire, validates the JSON and SARIF renderers and spawns a child process to confirm the exit code contract actually holds.

## Usage

```bash
# Markdown report from a JSONL export, with remediation notes
node EdgeCacheEntropyAudit.js --input edge.jsonl --format markdown --explain

# CI gate: fail the build on anything critical, machine readable output
cat edge.csv | node EdgeCacheEntropyAudit.js --format sarif --fail-on critical > edge.sarif

# Unusual schema, point it at the right columns
node EdgeCacheEntropyAudit.js \
  --input logs.json \
  --route-field http.route \
  --key-field cdn.cache_key \
  --status-field cf-cache-status \
  --user-field tenant_id \
  --service checkout-edge \
  --min-severity medium \
  --format json

# Report only, never fail
node EdgeCacheEntropyAudit.js --input edge.jsonl --fail-on never

# Verify the tool before trusting it
node EdgeCacheEntropyAudit.js --self-test
```

## Notes

- Uses ESM `import`, so run it from a package with `"type": "module"` or rename it to `.mjs`. No third party dependencies, only `node:fs`, `node:os`, `node:path`, `node:child_process` and `node:url`.
- Exit codes: 0 for pass, 2 when maximum severity meets or exceeds `--fail-on`, 1 for an argument or parse error on stderr. `--fail-on never` disables the gate.
- Input is read fully into memory with `readFileSync` and `--max-rows` (default 200000) caps analysis. Rows past the cap are dropped with a warning, not streamed. Feed it a sample or a time window, not a 40 GB archive. `unique` counts are likewise exact only up to `--max-keys`, so cardinality on a very wide route is an upper bound.
- Sensitive parameter detection is regex and heuristic. It flags names matching `SENSITIVE_PARAM`, any value shaped like an email and any value shaped like a JWT. Expect false positives on a parameter innocently named `source` or `ref`, and expect it to miss a low entropy secret under an unusual name.
- Hit rate uses only rows where `cacheStatus` resolved to hit or miss. Unknown statuses show as `n/a`, which is why `CACHE_STATUS_MISSING` is its own finding. The Markdown route table stops at the top 80 routes by volume, so use `--format json` for the full set.
- It reads logs. It does not talk to your CDN API, does not change configuration and does not verify that a fix worked. Rerun it against fresh logs for that.
