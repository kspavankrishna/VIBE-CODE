# Hybrid Inference Router

Deciding whether an inference request runs on the local model or goes to the cloud is usually a pile of if-statements that rots the moment latency, battery, thermals, privacy rules or cost targets move. This is one Swift actor that makes the decision from live signals, explains why, and races both paths when the deadline is tight.

**Language:** Swift | **Lines:** 955 | **Added:** 2026-04-23

## What this solves

Hybrid AI stacks are normal now. You have a Core ML or MLX model on device, or a llama.cpp runner on the box, and an OpenAI compatible gateway, Anthropic or Azure behind a network call. Every request needs an answer to the same question: which one. Most codebases answer with a hardcoded rule. Short prompt goes local, network up goes remote. Right on a laptop plugged in at a desk, wrong everywhere else.

The failure shows up as tail latency nobody can explain. The local path looked fast in testing, then the device hits thermal pressure during a long session and p90 triples. Nothing in the routing code knows that, so it keeps feeding work to a path that is now the slow one. Remote is fast until the user is on a constrained connection, and then every request eats its full timeout. Users see a spinner. Support sees "the app is slow" with no reproduction.

Cost and battery break the same way. A route that is cheap per call is not cheap when a background job sends ten thousand of them, and a local model that feels free is draining a phone at a rate nobody budgeted for. Then there is the case nobody wants to explain to legal: regulated content going over the wire because the rule only looked at token count.

## Why I built it

The pieces exist separately: latency trackers, circuit breakers, retry libraries, cost estimators. What is missing in Swift is the thing that combines them into one decision with a stated reason, and that adapts as the routes shift relative to each other during a session. A circuit breaker tells you a route is broken. It does not tell you local is 400ms slower right now but still the better pick, because the request carries user content and the device is plugged in.

The other gap is hedging. Racing two paths is the standard fix for tail latency, but done naively it doubles your bill and your battery drain for nothing. It is only worth it when the routes are close and the primary is genuinely at risk, and evaluating that needs recent health data. So the hedge logic and the health tracking live in the same object.

## When to use it

- An iOS or macOS app with an on device model and a cloud fallback that should trigger on measured tail latency, not a fixed timeout
- A Vapor or server side Swift service running a local runner next to a hosted API, needing per request cost ceilings
- A background job that must stay under a spend budget, where exceeding it should reject rather than silently bill
- Any flow where regulated or user content must never leave the device unless the remote policy explicitly permits it
- An agent runtime issuing many small calls that needs deadline aware hedging on the ones that matter

## How it works

The public surface is the `HybridInferenceRouter` actor, built from exactly two `RoutePolicy` values plus an `Options` struct of tuning constants. Everything validates at init and throws `invalidConfiguration` rather than failing later at an odd angle. A policy holds the static facts about a route: token caps, base p50 and p90 latency, `maxInFlight`, per 1K token cost, energy per 1K tokens in joules, whether it may handle regulated data and whether it is `hedgeable`.

Each route keeps a private `RouteState`: a sliding window of samples capped at `options.windowSize` (default 96), plus exponentially weighted moving averages for latency, failure and timeout. Percentiles come from sorting the window and interpolating between neighbouring samples, so p50 and p90 are real order statistics rather than an average pretending to be a tail. Risk is deliberately pessimistic: `failureRisk` is `max(1 - successRate, ewmaFailure)` and `timeoutRisk` is `max(timeoutRate, ewmaTimeout)`. The EWMA raises the alarm before the window fills, and the window keeps it up after the EWMA has decayed.

`plan(for:)` scores both routes through `estimate(route:policy:state:request:)` in two passes. First the hard gates, which set the score to infinity and attach a reason: policy disabled, token cap busted, `inFlight` at `maxInFlight`, network offline for a remote call, remote not cleared for regulated content, critical thermal state locally, or spend over `remoteBudgetUSD` or draw over `localEnergyBudgetJoules`. Then soft scoring. Score starts as predicted p90 in milliseconds, the percentile plus `inFlight * queuePenaltyPerInFlight`, then adds risk times `failurePenaltyMs` and `timeoutPenaltyMs`, cost times `remoteCostPenaltyMsPerDollar` and energy times `localEnergyPenaltyMsPerJoule`. Dollars, joules and risk all convert into one millisecond currency so they weigh directly against latency.

Situational adjustments follow. Local pays for battery, Low Power Mode and thermal pressure, and earns a `localPrivacyBonusMs` subtraction that grows for sensitive content. Remote pays `constrainedNetworkRemotePenaltyMs` on a weak link and a privacy penalty scaling to double for regulated data. `preferLocal` and `preferRemote` shift both scores by `preferenceBiasMs`, while `requireLocal` and `requireRemote` skip scoring entirely. A p90 past the deadline costs `deadlineMissBasePenaltyMs` plus the overrun times `deadlineMissSlope`. Lower score wins, and the losing estimate stays in the `RoutingDecision` so you can log both.

Hedging is gated hard: both routes feasible and hedgeable, a score gap inside `maxHedgeScoreGapMs`, a secondary no more than `maxSecondaryLatencyGap` slower at p50, and then either elevated recent risk or a primary p90 already past `hedgeTailDeadlineFraction` of the deadline while the secondary still fits. `execute(_:local:remote:)` is generic over any `Sendable` value and runs the hedge in a `withThrowingTaskGroup`: primary starts immediately, secondary sleeps `hedgeDelay` then runs with the remaining deadline, and the first success calls `cancelAll()`. Timing uses `DispatchTime.now().uptimeNanoseconds`, a monotonic clock, so a wall clock jump cannot corrupt the health window. Cancelled attempts release their in flight slot without recording a sample, so no route is punished for losing a race.

## Usage

```swift
let router = try HybridInferenceRouter(
    local: RoutePolicy(
        name: "MLX 8B on device",
        maxPromptTokens: 8_000,
        baseP50Latency: .milliseconds(900),
        baseP90Latency: .milliseconds(2_200),
        maxInFlight: 1,
        energyPer1KTokensJoules: 4.5
    ),
    remote: RoutePolicy(
        name: "Hosted gateway",
        baseP50Latency: .milliseconds(600),
        baseP90Latency: .milliseconds(1_800),
        maxInFlight: 8,
        supportsRegulatedData: false,
        inputCostPer1KUSD: 0.003,
        outputCostPer1KUSD: 0.015
    )
)

let request = HybridInferenceRequest(
    promptTokens: 1_450,
    expectedOutputTokens: 320,
    affinity: .automatic,
    network: .constrained,
    power: .battery,
    privacy: .userContent,
    thermal: .fair,
    deadline: .seconds(3),
    remoteBudgetUSD: 0.02
)

// Decide without running anything, and log the reasoning.
let decision = try await router.plan(for: request)
print(decision.kind, decision.primary as Any, decision.reasons)

// Or decide and run, with hedging handled for you.
let result = try await router.execute(
    request,
    local: { try await onDeviceModel.complete(prompt) },
    remote: { try await gateway.complete(prompt) }
)
print(result.winner, result.latency, result.value)

// Feed telemetry you already collect elsewhere.
await router.record(route: .remote, latency: .milliseconds(740), outcome: .success)

// Inspect route health.
let health = await router.snapshot()
print(health.local.p90Latency as Any, health.remote.successRate)
```

## Notes

- The router senses nothing itself. `network`, `power`, `thermal` and `privacy` arrive per request, so you wire up `NWPathMonitor`, `ProcessInfo.thermalState` and your own classification. Dependency free, but a stale input produces a confident wrong decision.
- It never calls a model. Both paths are `@Sendable` async closures you supply, and asking for a route you did not supply throws `noExecutor`.
- Cancelling the losing hedge only works if your executor is cooperative about cancellation. A blocking call that ignores `Task.checkCancellation` keeps running and keeps billing.
- Cost and energy are estimates from `expectedOutputTokens`, not measured usage, so a wrong output estimate skews the budget gates by the same factor.
- `maxInFlight` is a feasibility gate, not a queue. A saturated route is marked infeasible and traffic moves over or the request is rejected. No waiting line.
- All state is in memory and dies with the process. After a restart both routes fall back to their declared base latencies.
- Exactly two routes. This is not a multi provider load balancer and will not fan out across three cloud vendors. Needs Swift concurrency with `Duration` and actors, so macOS 13, iOS 16 or a matching Linux toolchain. Imports are only `Foundation` and `Dispatch`.
