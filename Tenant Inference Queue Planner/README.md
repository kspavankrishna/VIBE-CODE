# Tenant Inference Queue Planner

An LLM gateway that dispatches first and explains later will blow a tenant's token quota, miss a deadline and leak personal data into the wrong region, all in the same second. This is a single file Dart admission controller that decides accept, defer, reject or shed for every inference request before a single token is spent.

**Language:** Dart | **Lines:** 660 | **Added:** 2026-06-14

## What this solves

This is AI inference admission control for Dart services, Flutter backends, edge workers and MCP agent gateways: deciding which request gets accepted, delayed, rejected or shed before it burns tokens, money, latency budget or user trust. Most multi tenant LLM gateways get built the other way round. A request arrives, the gateway picks a provider by round robin or by a hardcoded preference list, sends it, and learns about the quota wall from a 429 that lands after the tokens are already committed. By then the retry storm is running, the caller has blown its deadline, and nobody can say which tenant caused it.

The failure modes compound. One tenant with a runaway agent loop drains the shared per minute token bucket, so every other tenant on the same key starts seeing 429s they did not cause. A job carrying personal data gets routed to whichever provider had free concurrency, which is a compliance incident, not a latency problem. A batch summarisation job with a two hour deadline and a chat turn with a 900 ms deadline sit in the same FIFO queue, so the chat turn waits and the user watches a spinner. A provider degrades, its observed p95 climbing from 800 ms to 6 seconds, and the router keeps feeding it because nothing in the path separates a provider that is up from one that is up but useless for this deadline.

Who notices depends on which fires. Finance notices the token bill at month end. The on call engineer notices the retry storm at 2 am. The customer notices the spinner immediately. Compliance notices the region violation in an audit months later.

This file makes one decision, once, with everything visible: tokens, requests, deadline, privacy flag, region, context window, tenant budget and provider health are all inputs to the same admission call. The output is an `AdmissionOutcome` carrying the verdict, the chosen provider, estimated cost in USD, a readable reason string, a `retry-after` duration and client visible headers.

## Why I built it

Existing tooling splits this decision across three layers that cannot see each other. The API gateway rate limits per tenant with no idea what a token costs. The provider SDK retries with backoff and no idea what the caller's deadline was. A separate scheduler queues work and has no idea which provider is currently disabled. Each layer is reasonable alone and the composition is wrong: three locally correct calls producing a request that is admitted, queued, retried and then dropped after the deadline has passed.

The heavyweight answer is a real scheduler with its own control plane, which is a lot of infrastructure to decide whether a chat completion should be sent. I wanted something a senior engineer can read end to end in one sitting, with no dependencies beyond `dart:collection` and `dart:math`, deterministic and therefore trivially testable, that drops into a Shelf handler, a Cloud Run service or a queue worker without ceremony.

## When to use it

- Your gateway fronts two or more LLM providers with different per minute token limits, different prices and different regional footprints, and you need to pick one per request.
- One tenant's agent loop is exhausting a shared provider quota and starving everyone else on the same key.
- Requests carry a personal data flag and some providers or regions are contractually off limits for those requests.
- Interactive chat turns and overnight batch jobs share the same queue and the chat turns keep losing.
- A provider is degrading rather than failing outright and you need routing to react to observed p95, not just to hard errors.
- You want a caller facing `retry-after` that is honest, capped by the caller's own deadline instead of a fixed constant.

## How it works

The core type is `TenantInferenceQueuePlanner`, a `const` class holding a `PlannerOptions` bag of tuning constants. Everything else is an immutable value object: `ProviderCapacity` (limits, latency percentiles, per million token pricing, regions, batching support), `TenantBudget` (USD remaining, per minute allowances, max queued jobs, allowed regions, personal data permission, a `priorityFloor`), `InferenceJob` (tenant, model family, region, deadline, token estimates, `InferencePriority`, optional `requiredContextTokens` and `consistencyKey`) and the live state in `ProviderLoad` and `TenantLedger`, both wrapped by a `QueueSnapshot`. Constructors validate hard and throw `ArgumentError` on negative counters, empty ids or a deadline before createdAt. Sets and maps are unmodifiable, so a snapshot cannot be mutated behind the planner's back.

`planOne` runs in three stages. First a tenant gate, `_tenantBlockReason`, which rejects outright on tenant id mismatch, a priority below the tenant's `priorityFloor`, a disallowed region, personal data the tenant policy forbids, a full tenant queue, or a per minute request or token quota this job would exceed. These are terminal: no provider can fix them. Second a provider filter, `_providerBlockReason`, which drops providers that are disabled until a future timestamp, do not serve the job's region, cannot receive personal data, have a context window smaller than `job.contextTokens`, are at max in flight, or whose per minute request or token buckets would overflow. The last check is the interesting one: it compares time to deadline plus `clockSkewAllowance` against `load.latencyFor(provider)`, which prefers observed p95 over configured p95, and drops any provider that cannot physically finish in time.

Survivors get scored by `_score`, a weighted linear function. Headroom blends the post reservation token, request and in flight ratios at 0.45, 0.25 and 0.30, weighted by `headroomWeight` (0.42). Deadline fit comes from `_deadlineFit`, which expresses slack as a multiple of expected latency and saturates at four times latency, weighted 0.38. Priority contributes the job's weight normalised against `incident` (20), weighted 0.08. Subtracted: cost pressure, this job's cost as a fraction of the tenant's remaining budget, clamped to 1 and weighted 0.12; the provider's static `failurePenalty`; a circuit breaker style `consecutiveFailures * 0.04`; and `queued * 0.018`. Candidates sort by score descending with a tie break on provider name, so the winner is deterministic.

When nothing survives, `_shouldShed` picks between shed and defer. Realtime and incident work is never shed. A job past its deadline is shed immediately. Otherwise it sheds only when every provider that serves the region and is not disabled sits at or above `overloadShedRatio` (0.92) on the worst of its three pressure ratios: drop low priority work when the region is saturated, not when one provider is busy. Everything else defers, with a delay from `_retryAfter` that starts at `defaultRetryAfter`, extends to cover the longest provider disable window, then gets capped by `maxClientDefer` (45 s) and by the caller's own deadline minus 100 ms.

Accepted jobs also get a `queuePosition` estimate and a `batchKey`, which groups compatible work as `provider|modelFamily|region|privacy|tokenBucket`, the bucket rounding token estimates up to the next multiple of 512 via `_ceilDiv`. Jobs with a `consistencyKey`, or bound for a provider with batching off or `maxBatchSize == 1`, get `single:<jobId>`, so ordering sensitive work is never merged into a batch. `_headers` emits `x-inference-admission`, `x-inference-planned-at`, `x-inference-provider` and a `retry-after` in whole seconds with a floor of 1.

`planMany` is the batch entry point. It sorts jobs by priority descending, then deadline ascending, then job id, then walks them greedily against a snapshot it mutates as it goes: every accept reserves against a copied `ProviderLoad` and `TenantLedger` via their `reserve` methods, so the second job sees the capacity the first consumed. A job whose tenant is missing from the supplied map is rejected with its own reason rather than throwing.

## Usage

```dart
import 'TenantInferenceQueuePlanner.dart';

final planner = TenantInferenceQueuePlanner(
  options: const PlannerOptions(
    overloadShedRatio: 0.92,
    maxClientDefer: Duration(seconds: 45),
    costWeight: 0.12,
  ),
);

final providers = <ProviderCapacity>[
  ProviderCapacity(
    name: 'primary-eu',
    maxInFlight: 64,
    maxRequestsPerMinute: 600,
    maxTokensPerMinute: 900000,
    contextWindowTokens: 200000,
    p50Latency: const Duration(milliseconds: 700),
    p95Latency: const Duration(milliseconds: 2100),
    inputUsdPerMillionTokens: 3.0,
    outputUsdPerMillionTokens: 15.0,
    regions: {'eu-west-1'},
    acceptsPersonalData: true,
    maxBatchSize: 16,
  ),
  ProviderCapacity(
    name: 'overflow-global',
    maxInFlight: 32,
    maxRequestsPerMinute: 300,
    maxTokensPerMinute: 400000,
    contextWindowTokens: 128000,
    p50Latency: const Duration(milliseconds: 900),
    p95Latency: const Duration(milliseconds: 3400),
    inputUsdPerMillionTokens: 0.8,
    outputUsdPerMillionTokens: 4.0,
    regions: {'*'},
    failurePenalty: 0.05,
  ),
];

final tenant = TenantBudget(
  tenantId: 'acme',
  usdRemaining: 42.50,
  tokensRemainingThisMinute: 120000,
  requestsRemainingThisMinute: 240,
  maxQueuedJobs: 200,
  allowedRegions: {'eu-west-1'},
  allowPersonalData: true,
  priorityFloor: InferencePriority.batch,
);

final now = DateTime.now().toUtc();
final job = InferenceJob(
  jobId: 'job-8812',
  tenantId: 'acme',
  modelFamily: 'sonnet',
  region: 'eu-west-1',
  createdAt: now,
  deadline: now.add(const Duration(seconds: 8)),
  estimatedInputTokens: 4200,
  estimatedOutputTokens: 900,
  priority: InferencePriority.interactive,
  containsPersonalData: true,
);

final snapshot = QueueSnapshot(
  providerLoads: {
    'primary-eu': ProviderLoad(
      providerName: 'primary-eu',
      inFlight: 12,
      queued: 4,
      requestsReservedThisMinute: 180,
      tokensReservedThisMinute: 410000,
      observedP95Latency: const Duration(milliseconds: 2600),
    ),
  },
  tenantLedgers: {
    'acme': TenantLedger(tenantId: 'acme', queuedJobs: 6),
  },
);

final outcome = planner.planOne(
  job: job,
  tenant: tenant,
  providers: providers,
  snapshot: snapshot,
  now: now,
);

print(outcome.kind);          // accept | defer | reject | shed
print(outcome.providerName);  // primary-eu
print(outcome.estimatedCostUsd);
print(outcome.reason);
print(outcome.headers);       // x-inference-admission, retry-after, ...

// Batch planning: reservations accumulate across the list.
final outcomes = planner.planMany(
  jobs: [job],
  tenants: {'acme': tenant},
  providers: providers,
  snapshot: snapshot,
  now: now,
);
for (final o in outcomes) {
  if (o.shouldRetry) print('${o.jobId} retry in ${o.retryAfter}');
}
```

## Notes

- It plans, it does not dispatch. No HTTP client, no execution, no retry loop. You call the provider yourself and own the accounting: after an accept, update `ProviderLoad` and `TenantLedger` in your own store the way `planMany` does internally.
- State is caller supplied and every call is stateless. Two gateway replicas planning against stale snapshots will both admit, so the buckets in `QueueSnapshot` need a shared source of truth if you run more than one instance.
- `usdRemaining` is soft. It feeds the cost pressure term, it is not a hard block. A tenant with almost no budget left still gets admitted, just biased toward cheaper providers. Enforce a hard spend cap upstream.
- `queueSoftLimit` in `PlannerOptions` is validated but read by no decision path in this file. The hard tenant queue limit is `TenantBudget.maxQueuedJobs`, and `maxQueuedJobs: 0` rejects every job for that tenant.
- `queuePosition` is a heuristic from queue depth, tenant pressure and priority offset. Fine as a progress hint. It is not a real position in a real queue.
- Token counts are estimates you supply. Cost, quota checks and batch bucketing are only as accurate as `estimatedInputTokens` and `estimatedOutputTokens`, and nothing here reconciles them against actual usage.
- Weights are tuned for mixed interactive and batch traffic. `_deadlineFit` saturating at four times latency is a judgement call, not a law.
- Deterministic and pure. Same inputs, same outputs, no clock reads when you pass `now`, so the whole admission matrix is easy to cover in tests.
