# Hybrid Inference Router

Deciding whether a Swift app runs a model on device, sends the request to a cloud endpoint, or starts one path and races the other is usually a pile of if-statements that rots the moment latency, battery, thermals or cost move. This is one file that makes that call from real signals and tells you why.

**Language:** Swift | **Lines:** 955 | **Added:** 2026-04-23

## What this solves

This solves the real April 2026 problem of deciding whether a Swift app or Swift service should run inference locally, send it to a remote model, or start on one path and hedge with the other when the deadline is tight. Hybrid AI stacks are normal now across Core ML, MLX, local llama.cpp runners, OpenAI compatible gateways, Anthropic and Azure. The routing logic in front of them is not. It is a branch on `isNetworkAvailable` and maybe a token count, written at a desk on full battery with a cool chassis and a good connection.

The failure is not subtle once you ship. A user in Low Power Mode with a warm phone goes to the on device model because the network check passed and nothing else was considered, generation takes four seconds instead of six hundred milliseconds, and the typing indicator sits there long enough that they leave. Or the inverse: the local model would have answered instantly, but the code prefers the cloud, so every autocomplete costs money and everyone on a train gets a spinner. Nobody files a bug for either.

The second failure is tail latency. Your remote endpoint is fine at p50 and terrible at p90 for twenty minutes during someone else's incident. A router that only knows median latency keeps feeding it. Deadlines blow, timeouts stack, in flight counts climb because nothing drains, and the dashboards still show a healthy average. What you want there is to start on the path you believe in and launch the other a few hundred milliseconds later, cutting the tail without doubling every request.

The third failure is quieter. Regulated content leaves the device because the code has no concept of privacy class, or a batch job spends thirty dollars because no per request budget existed. Here privacy class, remote spend and local energy are inputs that can make a route infeasible outright, not preferences buried in a comment.

## Why I built it

Every hybrid setup I looked at either hardcoded the choice or handed it to a service mesh that has no idea what a thermal state or Low Power Mode is. Server side balancers understand queues and health checks. They do not understand that the local route slows down when the chassis is hot, that battery drain is a cost, or that a request carries its own deadline and dollar ceiling. On the client the usual answer is a boolean and a prayer.

So this is the missing piece: one actor holding a bounded health window for both routes, folding every signal into a single comparable score in milliseconds, returning a decision with its reasons attached so you can log it and argue with it later. Nothing beyond Foundation and Dispatch, so it drops into a Swift package, an iOS support layer or a Vapor target as is.

## When to use it

- You ship an app with an MLX or Core ML model plus a cloud fallback and need one place that decides which runs.
- Your remote provider has a bad tail and you want hedged requests, but only when hedging is worth the duplicate spend.
- Requests carry deadlines, and missing the deadline is worse than taking the slower but safer route.
- Some requests are regulated or user content and must not leave the device, while others can go anywhere.
- A batch job needs a hard per request dollar ceiling on remote spend, or a joule ceiling on local drain.
- You already collect latency and error telemetry and want to feed it into routing instead of relearning it.

## How it works

`HybridInferenceRouter` is an actor holding two `RoutePolicy` values, one for `.local` and one for `.remote`, plus two private `RouteState` structs. `RouteState` is the memory: a bounded array of `RouteSample` capped at `Options.windowSize` (default 96) and trimmed from the front, plus EWMAs of latency, failure and timeout using the `current + alpha * (sample - current)` recurrence in the private `EWMA` enum. Percentiles come from linear interpolation over the sorted window, and `failureRisk` and `timeoutRisk` take `max` of the window rate and the EWMA, so neither a stale window nor a smoothed average hides a fresh problem.

`plan(for:)` holds the whole decision and touches no executor, so you can call it just to see what the router would do. It runs `estimate` per route in two passes. The first collects hard reasons: policy disabled, tokens over the policy cap, `inFlight` at `maxInFlight`, remote while the network is offline, `.regulated` content on a remote policy that forbids it, `.critical` thermal state on local unless affinity is `.requireLocal`, cost over `remoteBudgetUSD`, energy over `localEnergyBudgetJoules`. Any hard reason makes the route infeasible and its score `.infinity`.

The second pass scores, and the trick is that everything is denominated in milliseconds so unlike things compare. It starts at predicted p90 plus `queuePenaltyPerInFlight` per in flight request, then adds `failureRisk * failurePenaltyMs`, `timeoutRisk * timeoutPenaltyMs`, `estimatedCostUSD * remoteCostPenaltyMsPerDollar` (5000 by default, so a cent of spend costs about fifty milliseconds) and `estimatedEnergyJoules * localEnergyPenaltyMsPerJoule`. Context adjustments follow: local pays part of `lowPowerLocalPenaltyMs` on battery and all of it in Low Power Mode, pays thermal penalties at `.serious` and `.critical`, and gets a privacy bonus for sensitive content; remote pays `constrainedNetworkRemotePenaltyMs` on a constrained link and a privacy penalty scaled by class, doubled for `.regulated`. Affinity applies a symmetric `preferenceBiasMs` nudge. If predicted p90 exceeds the deadline, the score takes `deadlineMissBasePenaltyMs` plus the overrun times `deadlineMissSlope`, so a route that cannot make the deadline loses decisively. Lower score wins.

Hedging is deliberately hard to trigger. `shouldHedge` needs hedging allowed, both routes feasible, both policies `hedgeable`, the score gap within `maxHedgeScoreGapMs` and the secondary's p50 no more than `maxSecondaryLatencyGap` behind the primary's. Only then does it ask whether hedging pays: recent risk is high (failure at or above 0.10, timeout at or above 0.05), or the primary's p90 has crossed `hedgeTailDeadlineFraction` of the deadline while the secondary's p50 still fits inside it. `hedgeDelay` takes 35 percent of the primary's p50, clamps it between `minHedgeDelay` and `maxHedgeDelay`, then caps it at the latest launch leaving room for the secondary's p90. That is a tied request with a deferred second launch, and the delay is what stops it duplicating every call.

`execute` runs it.

`runAttempt` increments `inFlight`, timestamps with `DispatchTime.now().uptimeNanoseconds` so a wall clock change cannot corrupt a sample, and races the executor against a `Task.sleep` inside a `withThrowingTaskGroup` when a deadline exists. A hedge adds a second group: primary starts immediately, secondary sleeps the hedge delay then runs with the remaining deadline, the first success calls `group.cancelAll()` and wins, and losers come back as `AttemptEnvelope.abandoned` instead of polluting health stats. Real failures collect into `HybridInferenceRouterError.allAttemptsFailed`. Every finished attempt calls `finish`, which decrements `inFlight` and records the sample, so the next `plan` already knows what happened.

## Usage

```swift
let router = try HybridInferenceRouter(
    local: RoutePolicy(
        name: "MLX 3B on device",
        maxPromptTokens: 4_096,
        baseP50Latency: .milliseconds(420),
        baseP90Latency: .milliseconds(1_100),
        maxInFlight: 1,
        energyPer1KTokensJoules: 12.0
    ),
    remote: RoutePolicy(
        name: "Cloud gateway",
        baseP50Latency: .milliseconds(650),
        baseP90Latency: .milliseconds(2_400),
        maxInFlight: 8,
        supportsRegulatedData: false,
        inputCostPer1KUSD: 0.003,
        outputCostPer1KUSD: 0.015
    ),
    options: .init(windowSize: 128, maxHedgeScoreGapMs: 700)
)

let request = HybridInferenceRequest(
    promptTokens: 1_200,
    expectedOutputTokens: 256,
    affinity: .automatic,
    network: .constrained,
    power: .lowPower,
    privacy: .userContent,
    thermal: .fair,
    deadline: .seconds(3),
    allowHedging: true,
    remoteBudgetUSD: 0.02
)

// Inspect the decision without running anything.
let decision = try await router.plan(for: request)
print(decision.kind, decision.primary as Any, decision.hedgeDelay as Any)
decision.reasons.forEach { print("-", $0) }

// Or run it: the router picks, times, hedges and learns.
let result = try await router.execute(
    request,
    local: { try await onDeviceModel.generate(prompt) },
    remote: { try await cloudClient.generate(prompt) }
)
print(result.winner, result.latency, result.value)

// Feed telemetry you already have, without going through execute.
await router.record(route: .remote, latency: .milliseconds(2_150), outcome: .timeout)

// Health for dashboards.
let snap = await router.snapshot()
print(snap.remote.successRate, snap.remote.p90Latency as Any)
```

## Notes

- Exactly two routes. No N way pool, no tiering inside a route, no sticky sessions.
- It does not measure the device. `network`, `power`, `thermal` and `privacy` are per request inputs you supply from `NWPathMonitor`, `ProcessInfo.processInfo.thermalState` or your own classifier.
- State is in memory and dies with the process. Cold starts fall back to each policy's `baseP50Latency` and `baseP90Latency`, so set those honestly, and use `record` to warm the window from stored telemetry.
- Hedging duplicates work. Both routes may bill you, and cancelling the loser only helps if your executor honours task cancellation. Anything non idempotent should set `allowHedging: false` or `hedgeable: false`.
- `maxInFlight` is a hard gate, not a queue. At the ceiling a route is infeasible, and if both are capped `plan` returns `.reject` and `execute` throws `HybridInferenceRouterError.rejected`.
- `deadline` is a per attempt timeout, the hedged secondary getting it minus the hedge delay. It is not a wall clock budget across retries, and the router never retries on its own.
- Cost and energy are estimated from `expectedOutputTokens`, so an output overrun can breach a budget `plan` called safe. Feed real usage back through `record`.
- One async closure per route returning one `Value`. No streaming, no token callbacks. Wrap a stream in a closure that returns at first token if you route on time to first token.
- Needs Swift concurrency and `Duration`, plus Foundation and Dispatch. Nothing third party. Inputs are checked by `RoutePolicy.validate`, `Options.validate` and `HybridInferenceRequest.validate`.
