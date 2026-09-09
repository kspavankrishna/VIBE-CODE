# Gateway Failover Budget Governor

Routing an LLM request across several providers, regions and fallback clusters is a decision nobody can audit once it is buried inside retry code. This Dart CLI makes it explicit: it scores every endpoint, prints the hard rejection reasons and emits one JSON routing plan per request.

**Language:** Dart | **Lines:** 748 | **Added:** 2026-08-19

## What this solves

The failure mode is familiar to anyone running AI inference through more than one gateway. A provider starts returning 503s, the retry wrapper flips to the next endpoint in a hardcoded list, and that endpoint sits in the wrong region for a customer with a data residency clause. Nobody notices until an audit. Or the fallback costs four times as much per million output tokens, so a burst of retries during a 20 minute incident quietly spends a tenant's whole daily budget.

None of that is exotic. It happens because the routing rule lives in imperative code paths: an if branch here, a config flag someone added during a previous outage. There is no single place that says why endpoint B was chosen over endpoint A for this request, so during an incident the on call engineer reads retry logic instead of reading a decision.

This tool separates the decision from the execution. You give it an endpoint inventory with latency percentiles, success rates, capacity, prices and residency zones, plus tenant budgets, an optional stream of recent observations and the pending requests. It returns, per request, a primary endpoint, up to two fallbacks, an optional shadow endpoint, a hedge delay, a timeout and an estimated cost in USD. Rejected candidates come back with their reason, surviving ones with score, predicted latency, queue delay, risk points and carbon grams. That makes the routing layer testable in CI: replay a bad hour of observations against a proposed inventory and see what becomes unroutable.

## Why I built it

Service meshes and gateway products do weighted routing and circuit breaking, but they route on connection health, not on token cost, tenant budget, residency zone or model family. They do not know a request has a 2200 ms deadline and a two cent ceiling, and they will not refuse to route rather than blow a budget. The cost controls that do exist live in billing dashboards, after the fact.

The gap is a planner that treats cost, latency, residency and fairness as one admission decision and shows its work. One file, no dependencies, so it runs as a CI preflight, inside a Dart edge service or as a control plane beside an existing gateway.

## When to use it

- You run one model family across two or more providers and need a defensible reason for each failover, not a hardcoded ordering.
- A tenant has a hard daily spend limit and requests must be refused at admission, not discovered on the invoice.
- Requests carry residency constraints and cross region fallback is allowed for only some of them.
- You are rolling a new endpoint or self hosted cluster to a slice of traffic and want that allocation stable per request, not reshuffled on retry.
- You want a CI gate that replays production request shapes against a candidate inventory and fails the build if anything becomes unroutable.
- Tail latency is the problem and you want a computed hedge delay instead of a guessed constant.

## How it works

`GatewayFailoverBudgetGovernor.plan(Workload, Snapshot)` is the whole entry point. It scores every endpoint with `_score`, sorts ascending (lower is better, `Score.compareTo` compares the `value` field) and takes the first eligible candidate as primary. A `Score` carries two separate lists: `hardFailures` and `warnings`. Only `hardFailures` makes a candidate ineligible, and `Score.eligible` is just `hardFailures.isEmpty`. That split is the point. A candidate predicted to miss the deadline gets a warning and stays in play. One that violates residency, cannot stream when streaming is required, has an open breaker, is inside its `cooldownUntilEpochMs`, does not match the requested model family, exceeds the request cost cap or exceeds the tenant's remaining budget is out.

The score is a weighted sum. Latency is normalised against the deadline and multiplied by 100. Cost is multiplied by 70000, which is what makes a fraction of a cent comparable to a hundred milliseconds. Risk is `(1 - successRate) * 900` plus a recency penalty. Tail spread (`p99 - p95`) is divided by 35, carbon by 40, and priority is subtracted at `priority / 125`. Violating `preferredProvider` costs 12 points. A region mismatch costs 8 points when `allowCrossRegionFallback` is true and 100000 when it is false. Queue delay is `(queueDepth + inFlight) / capacityPerMinute * 60000`, a Little's law style estimate in milliseconds.

`_recentPenalty` reads the observation stream over a fixed five minute window relative to `nowEpochMs`. It takes the failure ratio in that window, where a failure is `success: false` or a status code of 500 or above, multiplies by 120, then adds 2.5 per throttle, a throttle being a 429 or an `errorClass` containing "timeout". That penalty feeds both the risk term and the predicted latency, so an endpoint flaking for the last two minutes gets pushed down without waiting for a breaker to trip.

Canary and shadow allocation both use `_stablePercent`, a 32 bit FNV-1a hash (offset basis 2166136261, prime 16777619, masked to 32 bits) reduced modulo 100. The canary seed concatenates tenant, idempotency key, request id and endpoint id, so a retried request lands in the same bucket every time instead of shuffling across attempts. FNV-1a is used because it is short, deterministic across processes and allocation free, not because it is cryptographic. The shadow seed is separate, and `_shadowEndpoint` picks a candidate from a different provider than the primary so the comparison is informative.

Fallbacks are filtered through `_pairedCostFits`, which checks primary plus that fallback against both the request `maxUsd` and the tenant's remaining budget. This is what matters during an incident: a retry that fits alone but blows the budget when paired with the attempt already made is never offered. At most two are returned. `_hedgeMs` computes `p95 * 0.65 + queueDelay`, clamped between 50 ms and the deadline minus 50 ms, returning 0 when there is no fallback, when the deadline is under 300 ms, when the primary already misses the deadline or when the hedge would land past 85 percent of the deadline. Timeout is `min(deadlineMs, max(100, primaryLatency * 1.35))`. Tenant admission runs first: if `inFlight >= concurrencyLimit` the request is rejected with `tenant_concurrency_limit` and the scores are still returned for inspection. Parsing goes through the `J` helper, which coerces types leniently. Endpoints and tenants are JSON arrays, requests and observations JSONL, with blank lines and `#` comments skipped by `_jsonLines`.

## Usage

```bash
# built in assertion test, no input files needed
dart GatewayFailoverBudgetGovernor.dart --self-test

# one snapshot file holding endpoints, tenantBudgets, observations and requests
dart GatewayFailoverBudgetGovernor.dart --snapshot snapshot.json

# split inputs: JSON arrays for inventory, JSONL for requests and observations
dart GatewayFailoverBudgetGovernor.dart \
  --endpoints endpoints.json \
  --tenants tenant-budgets.json \
  --observations observations.jsonl \
  --requests requests.jsonl

# CI gate: exit 2 if any request could not be routed
dart GatewayFailoverBudgetGovernor.dart \
  --endpoints endpoints.json --requests requests.jsonl --fail-on-reject
```

`--providers` aliases `--endpoints`, `--tenant-budgets` aliases `--tenants` and `--help` prints usage. Output is one JSON object per line on stdout: `action` of `route` or `reject`, plus `primaryEndpointId`, `fallbackEndpointIds`, `shadowEndpointId`, `hedgeAfterMs`, `timeoutMs`, `estimatedUsd`, `reason`, `notes` and the full `candidateScores` array.

## Notes

- Pure planner. It never sends a request, opens a socket or executes the plan. Your gateway still has to honour what it returns. It is also stateless per invocation, so canary and shadow percentages are hash based approximations over request identity, not exact traffic quotas.
- Exit codes: 0 normal, 2 with `--fail-on-reject` when at least one request was rejected, 64 on a bad argument or malformed JSON, 66 on a missing or unreadable file. Anything else propagates uncaught.
- `--self-test` relies on `assert`, so it verifies nothing unless assertions are enabled. That is the default for `dart run` in JIT mode, not for an AOT compiled binary.
- A region mismatch with `allowCrossRegionFallback: false` scores 100000 points rather than counting as a hard failure. It loses to any in region candidate, but if it is the only candidate it can still win. Residency zones are the hard constraint, region is not.
- Missing fields fall back to defaults instead of erroring: 1500 ms p95, 0.98 success rate, 60 per minute capacity, 0.15 and 0.60 USD per million input and output tokens, unlimited tenant budget. Validate inputs upstream.
- Cost is estimated from `inputTokens` and `maxOutputTokens`, a worst case for output. No cache hits, batch discounts or committed use pricing. Carbon is a flat multiplication weighted low enough that it only breaks near ties.
