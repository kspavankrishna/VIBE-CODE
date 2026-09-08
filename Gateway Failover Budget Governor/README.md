# Gateway Failover Budget Governor

Routing an LLM request across several model providers is a decision with money, latency, data residency and blast radius attached, yet in most systems it lives inside a retry loop nobody can read. This Dart CLI reads an endpoint inventory, tenant budgets and recent health observations, then emits one auditable JSON routing plan per request: primary, fallbacks, hedge delay, shadow endpoint, timeout and the scorecard of every candidate it rejected.

**Language:** Dart | **Lines:** 748 | **Added:** 2026-08-19

## What this solves

The failure mode is familiar to anyone running an AI gateway. You have an OpenAI endpoint in us-east, an Anthropic endpoint in eu-west, a self hosted cluster and a canary carrying a new model, and the routing logic is spread across a client wrapper, a retry helper and a feature flag. When the primary throws 503s at 2am it fails over to whatever is next in a hardcoded list. That endpoint sits in the wrong region for a customer with an EU residency clause, and nobody notices until the compliance review three weeks later.

The second failure is the bill. Retry and hedge logic that fires a second request without checking budget doubles the cost of every call during a partial outage. A tenant with a 20 dollar daily cap burns it in twenty minutes because the fallback costs three times the primary and nothing in the path knows. You find out from the invoice, or from the tenant hitting a hard stop mid workday.

The third is the tail. Naive failover waits out the full timeout before trying anything else, so one slow endpoint turns a 900ms p95 into a 30 second stall for everything in flight. Hedging fixes that only if the delay comes from the endpoint's real p95 and queue depth rather than a constant somebody picked in 2023. Too early and you double spend on healthy traffic. Too late and it never helps.

This answers one question before any of that happens: given the state of the fleet, which endpoint gets this request, what does it fall back to, when does the hedge fire and what will it cost. Every candidate carries its rejection reasons and warnings, so during an incident you read the decision instead of guessing.

## Why I built it

Service meshes route traffic, not inference economics. They do not know a request carries a token budget, a model family requirement, a residency zone constraint and a per tenant daily cap that all have to hold at once. Cost aware LLM routers exist, but most return an endpoint name with no explanation and no way to replay the decision offline against a recorded snapshot.

So: one file, no dependencies, JSON in and JSON out, shows its work. Replay last Tuesday's snapshot to explain a decision after the fact, or run it as a CI preflight on a config change.

## When to use it

- You run inference through more than one provider and need failover that respects data residency instead of picking the next name in a list.
- A tenant has a hard daily spend cap and the request must be rejected before the call goes out.
- You are rolling a new model to a percentage of traffic and want that allocation stable per request, not reshuffled on every retry.
- Tail latency is the problem and you want the hedge delay computed from live p95 and queue depth, not a constant.
- You want a CI gate that fails a config change if any request in your fixture set becomes unroutable.

## How it works

The core is `GatewayFailoverBudgetGovernor.plan(Workload request, Snapshot snapshot)`. It scores every `Endpoint` in the snapshot, sorts ascending, and takes the best eligible candidate as primary. Lower score wins, and `Score` implements `Comparable` so the sort is plain and ascending on one scalar.

`_score` separates hard failures from soft warnings, and that split is the whole design. A hard failure removes the endpoint: missing id, model family mismatch via `Endpoint.matches`, no streaming when the request requires it, an open circuit breaker, an active cooldown against `snapshot.nowMs`, a residency violation from `_residencyOk`, a canary the request was not allocated to, cost above the request `maxUsd` cap, or cost above the tenant's remaining daily budget. Warnings disqualify nothing and ride along in the output: predicted latency over deadline, success rate under 95 percent.

Everything else folds into one scalar. Queue delay is `(queueDepth + inFlight) / capacityPerMinute * 60000`, a Little's law style estimate of the wait before work starts. Predicted latency is `p95 + queueMs + recentPenalty * 80`, where `_recentPenalty` keeps only observation rows for that endpoint inside a five minute window and returns `failures / total * 120 + throttles * 2.5`, a throttle being a 429 or an error class containing "timeout". Risk is `(1 - successRate) * 900 + recentPenalty`. The score adds latency as a fraction of deadline scaled by 100, cost times 70000, risk, 12 points for the wrong preferred provider, 8 for a region mismatch when cross region fallback is allowed and 100000 when it is not, a fairness term, the p99 minus p95 tail spread over 35, carbon grams over 40, and subtracts `priority / 125`. That 100000 is a soft ban, not a hard failure, so an otherwise unroutable request still lands somewhere and the output says why.

`_fairness` carries multi tenancy: half the gap when a request's priority sits below the tenant's `reservedPriority`, plus 25 times concurrency pressure and 15 times budget pressure. A tenant that has burned 80 percent of its cap drifts toward cheaper endpoints without being cut off, and one already at its concurrency limit is rejected before scoring matters, with reason `tenant_concurrency_limit`.

Canary and shadow allocation both use `_stablePercent`, a 32 bit FNV-1a hash mod 100. Canary seeds on `tenantId:idempotencyKey:requestId:endpointId`, so a retry with the same idempotency key lands on the same side of the split every time. No RNG, no sticky session store. Shadow seeds on `tenantId:requestId:shadow` and picks the first eligible endpoint from a different provider than the primary, so the comparison means something.

Fallbacks are the next two eligible candidates passing `_pairedCostFits`: primary plus fallback cost must still fit under both the request cap and the tenant's remaining budget. That is what stops failover from doubling the bill. The hedge in `_hedgeMs` is `p95 * 0.65 + queueMs`, clamped between 50ms and deadline minus 50, and suppressed when there is no fallback, when the deadline is under 300ms, when predicted latency already exceeds the deadline, or when the hedge would land past 85 percent of the deadline. Timeout is `min(deadlineMs, max(100, primaryLatency * 1.35))`. Output is one JSON object per line on stdout.

## Usage

```bash
# built in fixture, exercises route, budget reject and canary paths
dart GatewayFailoverBudgetGovernor.dart --self-test

# single snapshot file with endpoints, tenants, observations and requests
dart GatewayFailoverBudgetGovernor.dart --snapshot snapshot.json

# separate files: endpoints JSON array, requests as JSON Lines
dart GatewayFailoverBudgetGovernor.dart \
  --endpoints endpoints.json \
  --requests requests.jsonl \
  --tenants tenants.json \
  --observations observations.jsonl

# CI gate: exit 2 if any request comes back rejected
dart GatewayFailoverBudgetGovernor.dart \
  --endpoints endpoints.json --requests requests.jsonl --fail-on-reject

dart GatewayFailoverBudgetGovernor.dart --help
```

`--providers` aliases `--endpoints`, `--tenant-budgets` aliases `--tenants`, and flags take `--key value` or `--key=value`. As a library: build a `Snapshot.fromJson` and a `Workload.fromJson`, call `plan`, read `Decision.toJson()`.

## Notes

- It plans, it does not execute. No HTTP client, no retry loop, no hedge firing. `hedgeAfterMs`, `fallbackEndpointIds` and `shadowEndpointId` are instructions for the caller.
- Scoring weights are constants inside `_score`. If your cost per token is an order of magnitude off the defaults, the `cost * 70000` term needs retuning.
- Queue delay is a static snapshot estimate with no memory across requests, so a batch plan ignores load its own earlier decisions just added.
- The observation window is hardcoded to five minutes, and rows with no `epochMs` default to `nowMs` so they always count as recent.
- Exit codes: 0 normal, 2 when `--fail-on-reject` is set and something was rejected, 64 on malformed input or bad arguments, 66 on a missing or unreadable file.
- `--self-test` relies on Dart `assert` statements, live under `dart run` but stripped by `dart compile exe` in release mode. Run it from source.
- Tenant budget checks read `usedUsdToday` and write nothing back, so concurrent planners on one tenant all see the same pre spend figure.
