//
//  ThermalAwareInferenceThrottler.swift
//
//  An actor-based admission controller for on-device AI inference (Core ML,
//  on-device LLMs, vision, speech) that adapts concurrency to the device's
//  real thermal and power state instead of a fixed, guessed-at limit.
//
//  Usage:
//
//      let throttle = ThermalAwareInferenceThrottler()
//
//      let reply = try await throttle.run(priority: .interactive, deadline: Date().addingTimeInterval(4)) {
//          try await localModel.generate(prompt: userPrompt)
//      }
//
//      Task {
//          for await sample in throttle.samples {
//              telemetry.record(sample)
//          }
//      }
//

import Foundation

// MARK: - Priority

/// Relative importance of a queued unit of on-device inference work.
/// Higher-priority tiers are drained first; within a tier, FIFO order holds.
public enum InferencePriority: Int, Sendable, Hashable, Comparable, CaseIterable {
    case background = 0
    case interactive = 1
    case realtime = 2

    public static func < (lhs: InferencePriority, rhs: InferencePriority) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

// MARK: - Errors

public enum InferenceThrottleError: Error, Sendable, CustomStringConvertible {
    /// The queue for this priority tier is already at capacity; the caller
    /// should back off or drop the request rather than pile on more load.
    case queueFull(priority: InferencePriority, depth: Int)

    /// The request's deadline passed while it was still waiting for a slot.
    case deadlineExceeded(waitedMs: Double)

    /// The device is under thermal pressure and this priority tier is
    /// configured to be shed rather than queued when that happens.
    case thermalRejected(state: ProcessInfo.ThermalState)

    /// The calling `Task` was cancelled while the request was queued.
    case cancelled

    public var description: String {
        switch self {
        case .queueFull(let priority, let depth):
            return "Throttle queue full for \(priority) tier (depth \(depth))."
        case .deadlineExceeded(let waitedMs):
            return "Request missed its deadline after waiting \(Int(waitedMs))ms for a slot."
        case .thermalRejected(let state):
            return "Rejected under thermal state \(state.rawValue) to protect the device."
        case .cancelled:
            return "Request was cancelled while queued for admission."
        }
    }
}

// MARK: - Configuration

public struct ThrottleConfiguration: Sendable {
    /// Starting concurrency ceiling before any thermal/power feedback is applied.
    public var baselineConcurrency: Int
    /// The ceiling never drops below this, even under `.critical` thermal state.
    public var minConcurrency: Int
    /// The ceiling never climbs above this, even when the device is fully cool.
    public var maxConcurrency: Int
    /// How many slots are added per cooldown window once the device is nominal/fair.
    public var additiveIncreaseStep: Int
    /// Multiplier applied to the ceiling on a thermal/power regression (e.g. 0.5 = halve it).
    public var multiplicativeDecreaseFactor: Double
    /// Minimum time between additive increases, so the ceiling doesn't thrash.
    public var cooldownWindow: TimeInterval
    /// Maximum number of queued (not yet admitted) requests per priority tier.
    public var queueCapacityPerTier: Int
    /// If true, background-tier requests are rejected outright (not queued)
    /// while the device is at `.serious` or `.critical` thermal state.
    public var rejectBackgroundUnderSeriousThermal: Bool

    public init(
        baselineConcurrency: Int = max(2, ProcessInfo.processInfo.activeProcessorCount / 2),
        minConcurrency: Int = 1,
        maxConcurrency: Int = ProcessInfo.processInfo.activeProcessorCount,
        additiveIncreaseStep: Int = 1,
        multiplicativeDecreaseFactor: Double = 0.5,
        cooldownWindow: TimeInterval = 8.0,
        queueCapacityPerTier: Int = 64,
        rejectBackgroundUnderSeriousThermal: Bool = true
    ) {
        self.minConcurrency = max(1, minConcurrency)
        self.baselineConcurrency = max(self.minConcurrency, baselineConcurrency)
        self.maxConcurrency = max(self.minConcurrency, maxConcurrency)
        self.additiveIncreaseStep = max(1, additiveIncreaseStep)
        self.multiplicativeDecreaseFactor = min(max(multiplicativeDecreaseFactor, 0.1), 0.9)
        self.cooldownWindow = max(1.0, cooldownWindow)
        self.queueCapacityPerTier = max(1, queueCapacityPerTier)
        self.rejectBackgroundUnderSeriousThermal = rejectBackgroundUnderSeriousThermal
    }
}

// MARK: - Ticket

/// Opaque proof of admission returned by `acquire`. Every ticket obtained
/// must be passed back to `release` (or `run`, which does it for you)
/// exactly once.
public struct ThrottleTicket: Sendable, Equatable {
    fileprivate let id: UUID
    public let priority: InferencePriority
}

// MARK: - Telemetry

public struct ThrottleSample: Sendable {
    public let timestamp: Date
    public let thermalState: ProcessInfo.ThermalState
    public let isLowPowerMode: Bool
    public let activeCount: Int
    public let concurrencyCeiling: Int
    public let queueDepthByPriority: [InferencePriority: Int]
    public let cumulativeAdmissions: Int
    public let cumulativeRejections: Int
}

// MARK: - Controller

/// An actor that gates concurrent on-device inference work behind a
/// concurrency ceiling that adapts to `ProcessInfo.thermalState` and Low
/// Power Mode using an AIMD (additive-increase, multiplicative-decrease)
/// control loop, plus a three-tier priority queue with per-request
/// deadlines so lower-priority work fails fast instead of starving
/// higher-priority work indefinitely.
public actor ThermalAwareInferenceThrottler {

    private struct Waiter {
        let id: UUID
        let priority: InferencePriority
        let enqueuedAt: Date
        let deadline: Date?
        let continuation: CheckedContinuation<ThrottleTicket, Error>
    }

    private var configuration: ThrottleConfiguration
    private var concurrencyCeiling: Int
    private var activeTickets: Set<UUID> = []
    private var waiters: [InferencePriority: [Waiter]]
    private var lastIncreaseAt: Date = .distantPast
    private var cumulativeAdmissions = 0
    private var cumulativeRejections = 0

    private var thermalObserverTask: Task<Void, Never>?
    private var powerObserverTask: Task<Void, Never>?
    private var expirySweeperTask: Task<Void, Never>?

    private let sampleContinuation: AsyncStream<ThrottleSample>.Continuation
    /// A live feed of throttle state. Pipe this into your existing metrics
    /// or logging pipeline; every state transition and every `release` emits
    /// a sample.
    public let samples: AsyncStream<ThrottleSample>

    public init(configuration: ThrottleConfiguration = ThrottleConfiguration()) {
        self.configuration = configuration
        self.concurrencyCeiling = configuration.baselineConcurrency
        var initialWaiters: [InferencePriority: [Waiter]] = [:]
        for priority in InferencePriority.allCases {
            initialWaiters[priority] = []
        }
        self.waiters = initialWaiters

        var continuation: AsyncStream<ThrottleSample>.Continuation!
        self.samples = AsyncStream { continuation = $0 }
        self.sampleContinuation = continuation

        self.thermalObserverTask = Task { [weak self] in
            await self?.observeThermalState()
        }
        self.powerObserverTask = Task { [weak self] in
            await self?.observePowerState()
        }
        self.expirySweeperTask = Task { [weak self] in
            await self?.runExpirySweepLoop()
        }
    }

    deinit {
        thermalObserverTask?.cancel()
        powerObserverTask?.cancel()
        expirySweeperTask?.cancel()
        sampleContinuation.finish()
    }

    // MARK: Public API

    /// Runs `operation` once a slot is available, releasing the slot when it
    /// finishes or throws. This is the entry point most callers should use.
    public func run<T: Sendable>(
        priority: InferencePriority,
        deadline: Date? = nil,
        operation: @Sendable () async throws -> T
    ) async throws -> T {
        let ticket = try await acquire(priority: priority, deadline: deadline)
        do {
            let result = try await operation()
            release(ticket)
            return result
        } catch {
            release(ticket)
            throw error
        }
    }

    /// Waits for (and reserves) a concurrency slot. Prefer `run` unless you
    /// need to hold the ticket across something `run`'s closure shape can't
    /// express; if you call `acquire` directly you must call `release`
    /// yourself, exactly once, including on every error path.
    public func acquire(
        priority: InferencePriority,
        deadline: Date? = nil
    ) async throws -> ThrottleTicket {
        if let deadline, deadline <= Date() {
            throw InferenceThrottleError.deadlineExceeded(waitedMs: 0)
        }

        if activeTickets.count < concurrencyCeiling, allWaitersEmpty() {
            return admit(priority: priority)
        }

        let depth = waiters[priority]?.count ?? 0
        guard depth < configuration.queueCapacityPerTier else {
            cumulativeRejections += 1
            publishSample()
            throw InferenceThrottleError.queueFull(priority: priority, depth: depth)
        }

        let id = UUID()
        let enqueuedAt = Date()

        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<ThrottleTicket, Error>) in
                let waiter = Waiter(
                    id: id,
                    priority: priority,
                    enqueuedAt: enqueuedAt,
                    deadline: deadline,
                    continuation: continuation
                )
                waiters[priority, default: []].append(waiter)
                publishSample()
            }
        } onCancel: {
            Task { await self.cancelWaiter(id: id, priority: priority) }
        }
    }

    /// Frees a slot obtained from `acquire`, admitting the next eligible
    /// waiter (if any) in priority order.
    public func release(_ ticket: ThrottleTicket) {
        activeTickets.remove(ticket.id)
        drainWaitersIfCapacityAvailable()
        publishSample()
    }

    /// A point-in-time snapshot, for callers that want the current state
    /// without subscribing to `samples`.
    public func currentSample() -> ThrottleSample {
        buildSample()
    }

    /// Replaces the tuning knobs at runtime, e.g. to lower `maxConcurrency`
    /// when the app moves to the background.
    public func updateConfiguration(_ newConfiguration: ThrottleConfiguration) {
        configuration = newConfiguration
        concurrencyCeiling = min(concurrencyCeiling, configuration.maxConcurrency)
        concurrencyCeiling = max(concurrencyCeiling, configuration.minConcurrency)
        drainWaitersIfCapacityAvailable()
        publishSample()
    }

    // MARK: Admission bookkeeping

    private func admit(priority: InferencePriority) -> ThrottleTicket {
        let id = UUID()
        activeTickets.insert(id)
        cumulativeAdmissions += 1
        return ThrottleTicket(id: id, priority: priority)
    }

    private func allWaitersEmpty() -> Bool {
        waiters.values.allSatisfy { $0.isEmpty }
    }

    private func dequeueNextWaiter() -> Waiter? {
        for priority in InferencePriority.allCases.sorted(by: >) {
            guard var pending = waiters[priority], !pending.isEmpty else { continue }
            let waiter = pending.removeFirst()
            waiters[priority] = pending
            return waiter
        }
        return nil
    }

    private func drainWaitersIfCapacityAvailable() {
        while activeTickets.count < concurrencyCeiling, let next = dequeueNextWaiter() {
            if let deadline = next.deadline, deadline <= Date() {
                let waitedMs = Date().timeIntervalSince(next.enqueuedAt) * 1000
                next.continuation.resume(throwing: InferenceThrottleError.deadlineExceeded(waitedMs: waitedMs))
                cumulativeRejections += 1
                continue
            }
            let ticket = admit(priority: next.priority)
            next.continuation.resume(returning: ticket)
        }
    }

    private func cancelWaiter(id: UUID, priority: InferencePriority) {
        guard var pending = waiters[priority] else { return }
        guard let index = pending.firstIndex(where: { $0.id == id }) else { return }
        let waiter = pending.remove(at: index)
        waiters[priority] = pending
        waiter.continuation.resume(throwing: InferenceThrottleError.cancelled)
        publishSample()
    }

    private func runExpirySweepLoop() async {
        while !Task.isCancelled {
            try? await Task.sleep(nanoseconds: 250_000_000)
            if Task.isCancelled { break }
            sweepExpiredWaiters()
        }
    }

    private func sweepExpiredWaiters() {
        let now = Date()
        var changed = false
        for priority in InferencePriority.allCases {
            guard let pending = waiters[priority], !pending.isEmpty else { continue }
            var stillWaiting: [Waiter] = []
            stillWaiting.reserveCapacity(pending.count)
            for waiter in pending {
                if let deadline = waiter.deadline, deadline <= now {
                    let waitedMs = now.timeIntervalSince(waiter.enqueuedAt) * 1000
                    waiter.continuation.resume(throwing: InferenceThrottleError.deadlineExceeded(waitedMs: waitedMs))
                    cumulativeRejections += 1
                    changed = true
                } else {
                    stillWaiting.append(waiter)
                }
            }
            waiters[priority] = stillWaiting
        }
        if changed {
            publishSample()
        }
    }

    // MARK: AIMD control loop

    private func attemptAdditiveIncrease() {
        let now = Date()
        guard now.timeIntervalSince(lastIncreaseAt) >= configuration.cooldownWindow else { return }
        guard concurrencyCeiling < configuration.maxConcurrency else { return }
        concurrencyCeiling = min(configuration.maxConcurrency, concurrencyCeiling + configuration.additiveIncreaseStep)
        lastIncreaseAt = now
    }

    private func applyMultiplicativeDecrease() {
        let reduced = Double(concurrencyCeiling) * configuration.multiplicativeDecreaseFactor
        concurrencyCeiling = max(configuration.minConcurrency, Int(reduced.rounded(.down)))
        // Reset the cooldown clock so we don't climb again the instant after a regression.
        lastIncreaseAt = Date()
    }

    private func rejectQueuedBackgroundWorkIfConfigured(state: ProcessInfo.ThermalState) {
        guard configuration.rejectBackgroundUnderSeriousThermal else { return }
        guard let pending = waiters[.background], !pending.isEmpty else { return }
        for waiter in pending {
            waiter.continuation.resume(throwing: InferenceThrottleError.thermalRejected(state: state))
            cumulativeRejections += 1
        }
        waiters[.background] = []
    }

    private func applyThermalTransition() {
        let state = ProcessInfo.processInfo.thermalState
        switch state {
        case .nominal, .fair:
            attemptAdditiveIncrease()
        case .serious:
            applyMultiplicativeDecrease()
            rejectQueuedBackgroundWorkIfConfigured(state: state)
        case .critical:
            concurrencyCeiling = configuration.minConcurrency
            rejectQueuedBackgroundWorkIfConfigured(state: state)
        @unknown default:
            break
        }
        drainWaitersIfCapacityAvailable()
    }

    private func applyPowerStateTransition() {
        if currentLowPowerModeState() {
            applyMultiplicativeDecrease()
        } else {
            attemptAdditiveIncrease()
        }
        drainWaitersIfCapacityAvailable()
    }

    private func currentLowPowerModeState() -> Bool {
        #if os(iOS) || os(tvOS) || os(watchOS)
        return ProcessInfo.processInfo.isLowPowerModeEnabled
        #elseif os(macOS)
        if #available(macOS 12.0, *) {
            return ProcessInfo.processInfo.isLowPowerModeEnabled
        }
        return false
        #else
        return false
        #endif
    }

    // MARK: Observation

    private func observeThermalState() async {
        publishSample()
        let notifications = NotificationCenter.default.notifications(named: ProcessInfo.thermalStateDidChangeNotification)
        for await _ in notifications {
            if Task.isCancelled { break }
            applyThermalTransition()
            publishSample()
        }
    }

    private func observePowerState() async {
        let notifications = NotificationCenter.default.notifications(named: ProcessInfo.powerStateDidChangeNotification)
        for await _ in notifications {
            if Task.isCancelled { break }
            applyPowerStateTransition()
            publishSample()
        }
    }

    // MARK: Telemetry

    private func buildSample() -> ThrottleSample {
        ThrottleSample(
            timestamp: Date(),
            thermalState: ProcessInfo.processInfo.thermalState,
            isLowPowerMode: currentLowPowerModeState(),
            activeCount: activeTickets.count,
            concurrencyCeiling: concurrencyCeiling,
            queueDepthByPriority: waiters.mapValues { $0.count },
            cumulativeAdmissions: cumulativeAdmissions,
            cumulativeRejections: cumulativeRejections
        )
    }

    private func publishSample() {
        sampleContinuation.yield(buildSample())
    }
}

/*
This solves the problem of on-device AI apps, Core ML models, local LLMs,
Apple Intelligence style features, camera pipelines, speech transcription,
quietly overheating the phone or draining the battery because every screen
in the app fires off inference work with zero idea how hot the chip already
is. Most apps set one fixed concurrency number at launch and never touch it
again, so the moment the device warms up, iOS starts throttling the CPU, GPU,
and Neural Engine for you anyway, except now it shows up as dropped frames,
sluggish replies, and a one star review that blames your app instead of the
silicon.

Built because I kept hitting the same bug shape on real projects: works great
in the simulator and on a fresh phone, falls apart after ten or fifteen
minutes of real use once the device gets warm, because nothing in the app is
watching ProcessInfo.thermalState or Low Power Mode. I wanted the on-device
equivalent of the rate limiters and token budgets everyone already writes for
paid cloud LLM APIs, except here the constrained resource is thermal headroom
and battery instead of a 429 response from OpenAI or Anthropic.

Use it when you are shipping Core ML, an on-device LLM, speech, vision, or any
CPU, GPU, or ANE heavy inference on iOS, iPadOS, macOS, tvOS, or watchOS, and
more than one part of the app can trigger inference at the same time, chat
plus background summarization, live camera plus a background export, several
agents sharing one local model. If your app already has a queue of AI work
competing for the same hardware, this is the missing piece that decides who
gets to run right now and who waits or gets rejected.

The trick: it runs an AIMD control loop, the same additive increase,
multiplicative decrease idea TCP uses for congestion control, on the max
concurrency number instead of on a network window. When ProcessInfo reports
serious or critical thermal state, or Low Power Mode turns on, the
concurrency ceiling gets cut in half immediately. Once the device sits at
nominal or fair for a full cooldown window, the ceiling creeps back up one
slot at a time, so it never overshoots and re-triggers the same thermal
event. Sitting on top of that is a three tier priority queue, background,
interactive, realtime, with per-request deadlines, so a background
summarization job gets rejected fast under thermal pressure instead of
silently starving your realtime chat reply, and nothing sits in a queue
forever waiting on a slot that is never coming.

Drop this into any Swift app or SDK doing local AI inference: create one
ThermalAwareInferenceThrottler per process, a singleton or a dependency
injected shared instance, wrap every Core ML prediction, on-device LLM call,
or heavy vision or speech task in
`try await throttle.run(priority:deadline:operation:)`, and subscribe to
`throttle.samples` to pipe live thermal state, concurrency ceiling, and queue
depth straight into whatever telemetry or logging pipeline you already run.
No third party dependencies, no Combine, just Foundation and Swift structured
concurrency, so the same file works unchanged across iOS, iPadOS, macOS,
tvOS, and watchOS targets.
*/
