# Thermal Aware Inference Throttler

On-device AI apps fire off Core ML predictions, local LLM calls and vision work from every screen with a fixed concurrency number picked at launch, and no idea how hot the chip already is. This is a Swift actor that adapts that number to the real thermal and power state of the device instead of guessing once and never looking again.

**Language:** Swift | **Lines:** 523 | **Added:** 2026-09-04

## What this solves

This solves the problem of on-device AI apps, Core ML models, local LLMs, Apple Intelligence style features, camera pipelines and speech transcription quietly overheating the phone or draining the battery because nothing in the app is watching the thermal state. Most apps set one fixed concurrency number at launch and never touch it again. The moment the device warms up, iOS starts throttling the CPU, GPU and Neural Engine for you anyway, except now it shows up as dropped frames, sluggish replies and a one star review that blames your app instead of the silicon.

The failure mode is specific and it is late. Everything looks fine in the simulator and on a cool phone. Ten or fifteen minutes into real use the device hits `.serious` thermal state, the scheduler starts clawing back clocks, and your inference latency doubles or triples while your app keeps cheerfully submitting the same number of concurrent jobs. Frames drop in the camera preview. A chat reply that took 800 ms now takes 4 seconds. Background summarization that nobody asked for is still competing for the same Neural Engine as the thing the user is staring at. Nobody sees a crash log, so nobody files a bug. It shows up as churn and battery complaints.

The second failure is starvation. When several parts of an app can trigger inference at once, chat plus background export plus live camera, a naive semaphore treats all of them the same. Whatever got there first wins. A batch job queued twenty deep will happily sit in front of the realtime request the user is actually waiting on, and neither one has any notion of a deadline, so requests pile up in a queue for a slot that is never coming.

This file addresses both. It gates every unit of inference work behind a concurrency ceiling that moves with `ProcessInfo.thermalState` and Low Power Mode, and it puts a three tier priority queue with per-request deadlines in front of that ceiling so low value work fails fast instead of squatting on capacity.

## Why I built it

Built because I kept hitting the same bug shape on real projects: works great in the simulator and on a fresh phone, falls apart after ten or fifteen minutes of real use once the device gets warm, because nothing in the app is watching `ProcessInfo.thermalState` or Low Power Mode. Apple gives you the signal. It does not give you anything that acts on it.

I wanted the on-device equivalent of the rate limiters and token budgets everyone already writes for paid cloud LLM APIs, except here the constrained resource is thermal headroom and battery instead of a 429 response from OpenAI or Anthropic. There is plenty of tooling for backing off a remote API. There is almost nothing for backing off your own silicon.

## When to use it

- Shipping Core ML or an on-device LLM where more than one screen can start inference at the same time.
- A live camera or Vision pipeline running while a background export or summarization job is also queued.
- Speech transcription or diarization that has to keep up in realtime while other AI work competes for the same Neural Engine.
- Several agents or features sharing one local model instance in a single process.
- You already see thermal complaints, battery complaints or latency that degrades over a session rather than on the first request.
- You want live thermal state, queue depth and admission counts flowing into your existing telemetry pipeline.

## How it works

The core is `ThermalAwareInferenceThrottler`, a Swift `actor`, so all mutable state is serialized without a lock. It holds a `concurrencyCeiling`, a `Set<UUID>` of `activeTickets` and a `[InferencePriority: [Waiter]]` dictionary of queued waiters. Callers go through `run(priority:deadline:operation:)`, which calls `acquire`, awaits your closure and calls `release` on both the success and the throw path. `acquire` and `release` are public if you need to hold a ticket across something the closure shape cannot express.

The control loop is AIMD, additive increase and multiplicative decrease, the same idea TCP uses for congestion control, applied to the concurrency ceiling instead of a network window. `applyThermalTransition` reads `ProcessInfo.processInfo.thermalState` and branches: `.nominal` and `.fair` call `attemptAdditiveIncrease`, which adds `additiveIncreaseStep` slots but only if `cooldownWindow` seconds have elapsed since the last increase. `.serious` calls `applyMultiplicativeDecrease`, which multiplies the ceiling by `multiplicativeDecreaseFactor` (0.5 by default), floors it at `minConcurrency` and resets the cooldown clock so it cannot climb again the instant after a regression. `.critical` skips the arithmetic and slams the ceiling straight to `minConcurrency`. `applyPowerStateTransition` does the same halving when Low Power Mode turns on and attempts an increase when it turns off. That asymmetry is the whole point: come down fast, go back up one slot at a time so you never overshoot into the same thermal event you just escaped.

Admission is a strict priority queue. `dequeueNextWaiter` walks `InferencePriority.allCases.sorted(by: >)`, so `.realtime` drains before `.interactive` before `.background`, and within a tier it is FIFO via `removeFirst`. `acquire` takes a fast path only when there is free capacity and every queue is empty, which stops a new arrival from jumping a waiting one. Each tier has its own `queueCapacityPerTier` bound, and exceeding it throws `queueFull` immediately rather than growing an unbounded backlog.

Deadlines are enforced in two places. `drainWaitersIfCapacityAvailable` checks each waiter's deadline as it pops it and resumes with `deadlineExceeded` instead of admitting a request that is already stale. Separately, `runExpirySweepLoop` wakes every 250 ms and calls `sweepExpiredWaiters`, so a queued request with a deadline fails on time even when nothing is releasing slots. Cancellation is wired through `withTaskCancellationHandler`: cancelling the calling `Task` removes the waiter and resumes it with `.cancelled`. Under `.serious` or `.critical` thermal state, `rejectQueuedBackgroundWorkIfConfigured` flushes the entire `.background` queue with `thermalRejected` when `rejectBackgroundUnderSeriousThermal` is on, which is the default.

Observation is structured concurrency, not Combine. Two detached tasks started in `init` iterate `NotificationCenter.default.notifications(named:)` for `thermalStateDidChangeNotification` and `powerStateDidChangeNotification`, both capturing `self` weakly. Every transition and every `release` calls `publishSample`, which yields a `ThrottleSample` carrying timestamp, thermal state, Low Power Mode flag, active count, current ceiling, queue depth per tier and cumulative admission and rejection counters into the public `samples` AsyncStream. `currentSample()` gives you the same struct synchronously if you would rather poll.

## Usage

```swift
import Foundation

// One instance per process. Defaults derive from activeProcessorCount.
let throttle = ThermalAwareInferenceThrottler()

// Or tune it.
let tuned = ThermalAwareInferenceThrottler(
    configuration: ThrottleConfiguration(
        baselineConcurrency: 3,
        minConcurrency: 1,
        maxConcurrency: 6,
        additiveIncreaseStep: 1,
        multiplicativeDecreaseFactor: 0.5,
        cooldownWindow: 8.0,
        queueCapacityPerTier: 64,
        rejectBackgroundUnderSeriousThermal: true
    )
)

// Wrap any inference call. The slot is released on success and on throw.
let reply = try await throttle.run(
    priority: .interactive,
    deadline: Date().addingTimeInterval(4)
) {
    try await localModel.generate(prompt: userPrompt)
}

// Background work that should be shed, not queued, when the device is hot.
do {
    _ = try await throttle.run(priority: .background) {
        try await summarizer.summarizeAll()
    }
} catch let error as InferenceThrottleError {
    log("skipped: \(error.description)")
}

// Manual ticket handling. You must release exactly once, on every path.
let ticket = try await throttle.acquire(priority: .realtime)
defer { Task { await throttle.release(ticket) } }

// Live telemetry.
Task {
    for await sample in await throttle.samples {
        telemetry.record(
            thermal: sample.thermalState,
            ceiling: sample.concurrencyCeiling,
            active: sample.activeCount,
            queued: sample.queueDepthByPriority,
            rejections: sample.cumulativeRejections
        )
    }
}

// Retune at runtime, for example when the app backgrounds.
await throttle.updateConfiguration(
    ThrottleConfiguration(baselineConcurrency: 1, maxConcurrency: 2)
)
```

## Notes

- Foundation only. No third party packages, no Combine. The same file compiles unchanged for iOS, iPadOS, macOS, tvOS and watchOS targets, and needs Swift concurrency, so an OS version that supports async/await and actors.
- `currentLowPowerModeState()` returns `isLowPowerModeEnabled` on iOS, tvOS and watchOS, guards it behind `#available(macOS 12.0, *)` on macOS and returns `false` on any other platform. On unsupported platforms the power half of the loop is inert and only thermal state drives the ceiling.
- The ceiling only moves when a notification arrives. There is no polling loop for thermal state, so on a device that stays at a constant state the ceiling stays where it is. It does not creep up on a timer.
- `thermalRejected` is thrown at background requests already sitting in the queue when a `.serious` or `.critical` transition lands. A brand new `acquire` during hot state is still queued or rejected with `queueFull`, not rejected on thermal grounds at submission time.
- `samples` is a single `AsyncStream` built from one continuation. Iterating it from more than one task splits values between consumers rather than broadcasting to all of them. Fan out yourself if you need multiple subscribers.
- The throttle counts slots, not work. It has no idea how expensive any one operation is, and a single very heavy inference call still occupies exactly one slot. It also cannot see load from other processes on the device.
- If you call `acquire` directly you own the `release`, on every error path. Miss one and that slot is leaked for the lifetime of the process.
