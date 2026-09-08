import Dispatch
import Foundation

public enum InferenceRoute: String, Sendable {
    case local
    case remote
}

public enum RoutingAffinity: String, Sendable {
    case automatic
    case preferLocal
    case preferRemote
    case requireLocal
    case requireRemote
}

public enum NetworkState: String, Sendable {
    case offline
    case constrained
    case online
}

public enum PowerState: String, Sendable {
    case pluggedIn
    case battery
    case lowPower
}

public enum PrivacyClass: String, Sendable {
    case publicData
    case internalData
    case userContent
    case regulated
}

public enum ThermalState: Int, Sendable, Comparable {
    case nominal = 0
    case fair = 1
    case serious = 2
    case critical = 3

    public static func < (lhs: ThermalState, rhs: ThermalState) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

public enum RoutingDecisionKind: String, Sendable {
    case localOnly
    case remoteOnly
    case hedged
    case reject
}

public enum AttemptOutcome: String, Sendable {
    case success
    case error
    case timeout
}

public struct RoutePolicy: Sendable {
    public var name: String
    public var enabled: Bool
    public var maxPromptTokens: Int?
    public var maxOutputTokens: Int?
    public var baseP50Latency: Duration
    public var baseP90Latency: Duration
    public var maxInFlight: Int
    public var queuePenaltyPerInFlight: Duration
    public var supportsRegulatedData: Bool
    public var inputCostPer1KUSD: Double
    public var outputCostPer1KUSD: Double
    public var energyPer1KTokensJoules: Double
    public var hedgeable: Bool

    public init(
        name: String,
        enabled: Bool = true,
        maxPromptTokens: Int? = nil,
        maxOutputTokens: Int? = nil,
        baseP50Latency: Duration,
        baseP90Latency: Duration,
        maxInFlight: Int = 1,
        queuePenaltyPerInFlight: Duration = .milliseconds(100),
        supportsRegulatedData: Bool = true,
        inputCostPer1KUSD: Double = 0,
        outputCostPer1KUSD: Double = 0,
        energyPer1KTokensJoules: Double = 0,
        hedgeable: Bool = true
    ) {
        self.name = name
        self.enabled = enabled
        self.maxPromptTokens = maxPromptTokens
        self.maxOutputTokens = maxOutputTokens
        self.baseP50Latency = baseP50Latency
        self.baseP90Latency = baseP90Latency
        self.maxInFlight = maxInFlight
        self.queuePenaltyPerInFlight = queuePenaltyPerInFlight
        self.supportsRegulatedData = supportsRegulatedData
        self.inputCostPer1KUSD = inputCostPer1KUSD
        self.outputCostPer1KUSD = outputCostPer1KUSD
        self.energyPer1KTokensJoules = energyPer1KTokensJoules
        self.hedgeable = hedgeable
    }

    public func validate(route: InferenceRoute) throws {
        guard !name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw HybridInferenceRouterError.invalidConfiguration("Policy name for \(route.rawValue) must not be empty.")
        }
        if let maxPromptTokens, maxPromptTokens <= 0 {
            throw HybridInferenceRouterError.invalidConfiguration("maxPromptTokens for \(route.rawValue) must be positive.")
        }
        if let maxOutputTokens, maxOutputTokens <= 0 {
            throw HybridInferenceRouterError.invalidConfiguration("maxOutputTokens for \(route.rawValue) must be positive.")
        }
        guard baseP50Latency > .zero, baseP90Latency >= baseP50Latency else {
            throw HybridInferenceRouterError.invalidConfiguration("Latency defaults for \(route.rawValue) are invalid.")
        }
        guard maxInFlight > 0 else {
            throw HybridInferenceRouterError.invalidConfiguration("maxInFlight for \(route.rawValue) must be positive.")
        }
        guard queuePenaltyPerInFlight >= .zero else {
            throw HybridInferenceRouterError.invalidConfiguration("queuePenaltyPerInFlight for \(route.rawValue) must not be negative.")
        }
        guard inputCostPer1KUSD >= 0, outputCostPer1KUSD >= 0, energyPer1KTokensJoules >= 0 else {
            throw HybridInferenceRouterError.invalidConfiguration("Cost and energy values for \(route.rawValue) must not be negative.")
        }
    }
}

public struct HybridInferenceRequest: Sendable {
    public var id: String
    public var promptTokens: Int
    public var expectedOutputTokens: Int
    public var affinity: RoutingAffinity
    public var network: NetworkState
    public var power: PowerState
    public var privacy: PrivacyClass
    public var thermal: ThermalState
    public var deadline: Duration?
    public var allowHedging: Bool
    public var remoteBudgetUSD: Double?
    public var localEnergyBudgetJoules: Double?

    public init(
        id: String = UUID().uuidString,
        promptTokens: Int,
        expectedOutputTokens: Int,
        affinity: RoutingAffinity = .automatic,
        network: NetworkState = .online,
        power: PowerState = .battery,
        privacy: PrivacyClass = .userContent,
        thermal: ThermalState = .nominal,
        deadline: Duration? = nil,
        allowHedging: Bool = true,
        remoteBudgetUSD: Double? = nil,
        localEnergyBudgetJoules: Double? = nil
    ) {
        self.id = id
        self.promptTokens = promptTokens
        self.expectedOutputTokens = expectedOutputTokens
        self.affinity = affinity
        self.network = network
        self.power = power
        self.privacy = privacy
        self.thermal = thermal
        self.deadline = deadline
        self.allowHedging = allowHedging
        self.remoteBudgetUSD = remoteBudgetUSD
        self.localEnergyBudgetJoules = localEnergyBudgetJoules
    }

    public var totalTokens: Int {
        promptTokens + expectedOutputTokens
    }

    public func validate() throws {
        guard !id.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw HybridInferenceRouterError.invalidRequest("Request id must not be empty.")
        }
        guard promptTokens >= 0, expectedOutputTokens >= 0 else {
            throw HybridInferenceRouterError.invalidRequest("Token counts must not be negative.")
        }
        if let deadline, deadline <= .zero {
            throw HybridInferenceRouterError.invalidRequest("deadline must be greater than zero when provided.")
        }
        if let remoteBudgetUSD, remoteBudgetUSD < 0 {
            throw HybridInferenceRouterError.invalidRequest("remoteBudgetUSD must not be negative.")
        }
        if let localEnergyBudgetJoules, localEnergyBudgetJoules < 0 {
            throw HybridInferenceRouterError.invalidRequest("localEnergyBudgetJoules must not be negative.")
        }
    }
}

public struct RouteEstimate: Sendable {
    public var route: InferenceRoute
    public var feasible: Bool
    public var predictedP50Latency: Duration
    public var predictedP90Latency: Duration
    public var failureRisk: Double
    public var timeoutRisk: Double
    public var estimatedCostUSD: Double
    public var estimatedEnergyJoules: Double
    public var score: Double
    public var reasons: [String]
}

public struct RoutingDecision: Sendable {
    public var kind: RoutingDecisionKind
    public var primary: InferenceRoute?
    public var secondary: InferenceRoute?
    public var hedgeDelay: Duration?
    public var reasons: [String]
    public var local: RouteEstimate
    public var remote: RouteEstimate
}

public struct RouteHealthSnapshot: Sendable {
    public var route: InferenceRoute
    public var policyName: String
    public var sampleCount: Int
    public var inFlight: Int
    public var successRate: Double
    public var timeoutRate: Double
    public var ewmaLatency: Duration?
    public var p50Latency: Duration?
    public var p90Latency: Duration?
}

public struct RouterSnapshot: Sendable {
    public var local: RouteHealthSnapshot
    public var remote: RouteHealthSnapshot
}

public struct ExecutionResult<Value: Sendable>: Sendable {
    public var requestID: String
    public var decision: RoutingDecision
    public var winner: InferenceRoute
    public var latency: Duration
    public var value: Value
}

public struct AttemptFailure: Error, Sendable {
    public var route: InferenceRoute
    public var outcome: AttemptOutcome
    public var message: String
}

public enum HybridInferenceRouterError: Error, LocalizedError, Sendable {
    case invalidConfiguration(String)
    case invalidRequest(String)
    case noExecutor(InferenceRoute)
    case rejected([String])
    case timedOut(route: InferenceRoute, timeout: Duration)
    case allAttemptsFailed([AttemptFailure])

    public var errorDescription: String? {
        switch self {
        case let .invalidConfiguration(message), let .invalidRequest(message):
            return message
        case let .noExecutor(route):
            return "No executor was provided for the \(route.rawValue) route."
        case let .rejected(reasons):
            return reasons.joined(separator: " ")
        case let .timedOut(route, timeout):
            return "The \(route.rawValue) route timed out after \(Format.duration(timeout))."
        case let .allAttemptsFailed(failures):
            return failures.map { "\($0.route.rawValue): \($0.message)" }.joined(separator: "; ")
        }
    }
}

public actor HybridInferenceRouter {
    public struct Options: Sendable {
        public var windowSize: Int
        public var latencyAlpha: Double
        public var failureAlpha: Double
        public var timeoutAlpha: Double
        public var failurePenaltyMs: Double
        public var timeoutPenaltyMs: Double
        public var remoteCostPenaltyMsPerDollar: Double
        public var localEnergyPenaltyMsPerJoule: Double
        public var lowPowerLocalPenaltyMs: Double
        public var seriousThermalLocalPenaltyMs: Double
        public var criticalThermalLocalPenaltyMs: Double
        public var constrainedNetworkRemotePenaltyMs: Double
        public var remotePrivacyPenaltyMs: Double
        public var localPrivacyBonusMs: Double
        public var deadlineMissBasePenaltyMs: Double
        public var deadlineMissSlope: Double
        public var preferenceBiasMs: Double
        public var maxHedgeScoreGapMs: Double
        public var hedgeTailDeadlineFraction: Double
        public var hedgeFailureThreshold: Double
        public var hedgeTimeoutThreshold: Double
        public var hedgeDelayFraction: Double
        public var minHedgeDelay: Duration
        public var maxHedgeDelay: Duration
        public var maxSecondaryLatencyGap: Duration

        public init(
            windowSize: Int = 96,
            latencyAlpha: Double = 0.25,
            failureAlpha: Double = 0.20,
            timeoutAlpha: Double = 0.20,
            failurePenaltyMs: Double = 1800,
            timeoutPenaltyMs: Double = 2600,
            remoteCostPenaltyMsPerDollar: Double = 5000,
            localEnergyPenaltyMsPerJoule: Double = 35,
            lowPowerLocalPenaltyMs: Double = 850,
            seriousThermalLocalPenaltyMs: Double = 1400,
            criticalThermalLocalPenaltyMs: Double = 4000,
            constrainedNetworkRemotePenaltyMs: Double = 500,
            remotePrivacyPenaltyMs: Double = 700,
            localPrivacyBonusMs: Double = 200,
            deadlineMissBasePenaltyMs: Double = 3000,
            deadlineMissSlope: Double = 4,
            preferenceBiasMs: Double = 250,
            maxHedgeScoreGapMs: Double = 850,
            hedgeTailDeadlineFraction: Double = 0.75,
            hedgeFailureThreshold: Double = 0.10,
            hedgeTimeoutThreshold: Double = 0.05,
            hedgeDelayFraction: Double = 0.35,
            minHedgeDelay: Duration = .milliseconds(120),
            maxHedgeDelay: Duration = .milliseconds(900),
            maxSecondaryLatencyGap: Duration = .milliseconds(700)
        ) {
            self.windowSize = windowSize
            self.latencyAlpha = latencyAlpha
            self.failureAlpha = failureAlpha
            self.timeoutAlpha = timeoutAlpha
            self.failurePenaltyMs = failurePenaltyMs
            self.timeoutPenaltyMs = timeoutPenaltyMs
            self.remoteCostPenaltyMsPerDollar = remoteCostPenaltyMsPerDollar
            self.localEnergyPenaltyMsPerJoule = localEnergyPenaltyMsPerJoule
            self.lowPowerLocalPenaltyMs = lowPowerLocalPenaltyMs
            self.seriousThermalLocalPenaltyMs = seriousThermalLocalPenaltyMs
            self.criticalThermalLocalPenaltyMs = criticalThermalLocalPenaltyMs
            self.constrainedNetworkRemotePenaltyMs = constrainedNetworkRemotePenaltyMs
            self.remotePrivacyPenaltyMs = remotePrivacyPenaltyMs
            self.localPrivacyBonusMs = localPrivacyBonusMs
            self.deadlineMissBasePenaltyMs = deadlineMissBasePenaltyMs
            self.deadlineMissSlope = deadlineMissSlope
            self.preferenceBiasMs = preferenceBiasMs
            self.maxHedgeScoreGapMs = maxHedgeScoreGapMs
            self.hedgeTailDeadlineFraction = hedgeTailDeadlineFraction
            self.hedgeFailureThreshold = hedgeFailureThreshold
            self.hedgeTimeoutThreshold = hedgeTimeoutThreshold
            self.hedgeDelayFraction = hedgeDelayFraction
            self.minHedgeDelay = minHedgeDelay
            self.maxHedgeDelay = maxHedgeDelay
            self.maxSecondaryLatencyGap = maxSecondaryLatencyGap
        }

        public func validate() throws {
            guard windowSize >= 8 else {
                throw HybridInferenceRouterError.invalidConfiguration("windowSize must be at least 8.")
            }
            try Self.checkUnit(latencyAlpha, "latencyAlpha")
            try Self.checkUnit(failureAlpha, "failureAlpha")
            try Self.checkUnit(timeoutAlpha, "timeoutAlpha")
            try Self.checkUnit(hedgeTailDeadlineFraction, "hedgeTailDeadlineFraction")
            try Self.checkUnit(hedgeDelayFraction, "hedgeDelayFraction")
            try Self.checkUnit(hedgeFailureThreshold, "hedgeFailureThreshold", allowZero: true)
            try Self.checkUnit(hedgeTimeoutThreshold, "hedgeTimeoutThreshold", allowZero: true)
            guard minHedgeDelay >= .zero, maxHedgeDelay >= minHedgeDelay, maxSecondaryLatencyGap >= .zero else {
                throw HybridInferenceRouterError.invalidConfiguration("Hedge timing values are invalid.")
            }
        }

        private static func checkUnit(_ value: Double, _ name: String, allowZero: Bool = false) throws {
            if allowZero {
                guard value >= 0 && value <= 1 else {
                    throw HybridInferenceRouterError.invalidConfiguration("\(name) must be in [0, 1].")
                }
            } else {
                guard value > 0 && value <= 1 else {
                    throw HybridInferenceRouterError.invalidConfiguration("\(name) must be in (0, 1].")
                }
            }
        }
    }

    private let localPolicy: RoutePolicy
    private let remotePolicy: RoutePolicy
    private let options: Options
    private var local = RouteState()
    private var remote = RouteState()

    public init(local: RoutePolicy, remote: RoutePolicy, options: Options = .init()) throws {
        try local.validate(route: .local)
        try remote.validate(route: .remote)
        try options.validate()
        self.localPolicy = local
        self.remotePolicy = remote
        self.options = options
    }

    public func record(route: InferenceRoute, latency: Duration, outcome: AttemptOutcome, at finishedAt: Date = Date()) {
        switch route {
        case .local:
            local.record(latency: latency, outcome: outcome, finishedAt: finishedAt, options: options)
        case .remote:
            remote.record(latency: latency, outcome: outcome, finishedAt: finishedAt, options: options)
        }
    }

    public func snapshot() -> RouterSnapshot {
        RouterSnapshot(
            local: local.snapshot(route: .local, policy: localPolicy),
            remote: remote.snapshot(route: .remote, policy: remotePolicy)
        )
    }

    public func plan(for request: HybridInferenceRequest) throws -> RoutingDecision {
        try request.validate()
        let localEstimate = estimate(route: .local, policy: localPolicy, state: local, request: request)
        let remoteEstimate = estimate(route: .remote, policy: remotePolicy, state: remote, request: request)
        var reasons: [String] = []

        switch request.affinity {
        case .requireLocal:
            guard localEstimate.feasible else {
                return RoutingDecision(kind: .reject, primary: nil, secondary: nil, hedgeDelay: nil, reasons: localEstimate.reasons + ["Request requires local execution."], local: localEstimate, remote: remoteEstimate)
            }
            return RoutingDecision(kind: .localOnly, primary: .local, secondary: nil, hedgeDelay: nil, reasons: ["Request requires local execution."], local: localEstimate, remote: remoteEstimate)
        case .requireRemote:
            guard remoteEstimate.feasible else {
                return RoutingDecision(kind: .reject, primary: nil, secondary: nil, hedgeDelay: nil, reasons: remoteEstimate.reasons + ["Request requires remote execution."], local: localEstimate, remote: remoteEstimate)
            }
            return RoutingDecision(kind: .remoteOnly, primary: .remote, secondary: nil, hedgeDelay: nil, reasons: ["Request requires remote execution."], local: localEstimate, remote: remoteEstimate)
        case .automatic, .preferLocal, .preferRemote:
            break
        }

        if localEstimate.feasible && !remoteEstimate.feasible {
            return RoutingDecision(kind: .localOnly, primary: .local, secondary: nil, hedgeDelay: nil, reasons: ["Remote is not feasible right now."] + remoteEstimate.reasons, local: localEstimate, remote: remoteEstimate)
        }
        if remoteEstimate.feasible && !localEstimate.feasible {
            return RoutingDecision(kind: .remoteOnly, primary: .remote, secondary: nil, hedgeDelay: nil, reasons: ["Local is not feasible right now."] + localEstimate.reasons, local: localEstimate, remote: remoteEstimate)
        }
        if !localEstimate.feasible && !remoteEstimate.feasible {
            return RoutingDecision(kind: .reject, primary: nil, secondary: nil, hedgeDelay: nil, reasons: localEstimate.reasons + remoteEstimate.reasons + ["Neither route satisfies the current request."], local: localEstimate, remote: remoteEstimate)
        }

        let primary = localEstimate.score <= remoteEstimate.score ? localEstimate : remoteEstimate
        let secondary = primary.route == .local ? remoteEstimate : localEstimate
        reasons.append("\(primary.route.rawValue.capitalized) currently has the lower routing score (\(Format.score(primary.score)) vs \(Format.score(secondary.score))).")

        if shouldHedge(primary: primary, secondary: secondary, request: request) {
            let delay = hedgeDelay(primary: primary, secondary: secondary, request: request)
            reasons.append("Hedging is enabled because the primary path is close to the deadline or recent risk is elevated.")
            reasons.append("Secondary launch delay is \(Format.duration(delay)).")
            return RoutingDecision(kind: .hedged, primary: primary.route, secondary: secondary.route, hedgeDelay: delay, reasons: reasons, local: localEstimate, remote: remoteEstimate)
        }

        reasons.append("A single route is enough because the score gap is meaningful and hedging would mostly add duplicate work.")
        return RoutingDecision(kind: primary.route == .local ? .localOnly : .remoteOnly, primary: primary.route, secondary: nil, hedgeDelay: nil, reasons: reasons, local: localEstimate, remote: remoteEstimate)
    }

    public func execute<Value: Sendable>(
        _ request: HybridInferenceRequest,
        local localExecutor: (@Sendable () async throws -> Value)? = nil,
        remote remoteExecutor: (@Sendable () async throws -> Value)? = nil
    ) async throws -> ExecutionResult<Value> {
        let decision = try plan(for: request)
        switch decision.kind {
        case .localOnly:
            guard let localExecutor else { throw HybridInferenceRouterError.noExecutor(.local) }
            let attempt = try await runAttempt(route: .local, timeout: request.deadline, dropCancellation: true, executor: localExecutor)
            return ExecutionResult(requestID: request.id, decision: decision, winner: .local, latency: attempt.latency, value: attempt.value)
        case .remoteOnly:
            guard let remoteExecutor else { throw HybridInferenceRouterError.noExecutor(.remote) }
            let attempt = try await runAttempt(route: .remote, timeout: request.deadline, dropCancellation: true, executor: remoteExecutor)
            return ExecutionResult(requestID: request.id, decision: decision, winner: .remote, latency: attempt.latency, value: attempt.value)
        case .hedged:
            guard let primary = decision.primary, let secondary = decision.secondary else {
                throw HybridInferenceRouterError.rejected(["Hedged decision was missing route assignments."])
            }
            guard let first = executor(for: primary, local: localExecutor, remote: remoteExecutor) else {
                throw HybridInferenceRouterError.noExecutor(primary)
            }
            guard let second = executor(for: secondary, local: localExecutor, remote: remoteExecutor) else {
                throw HybridInferenceRouterError.noExecutor(secondary)
            }
            let winner = try await runHedge(request: request, decision: decision, primary: primary, primaryExecutor: first, secondary: secondary, secondaryExecutor: second)
            return ExecutionResult(requestID: request.id, decision: decision, winner: winner.route, latency: winner.latency, value: winner.value)
        case .reject:
            throw HybridInferenceRouterError.rejected(decision.reasons)
        }
    }

    private func runHedge<Value: Sendable>(
        request: HybridInferenceRequest,
        decision: RoutingDecision,
        primary: InferenceRoute,
        primaryExecutor: @escaping @Sendable () async throws -> Value,
        secondary: InferenceRoute,
        secondaryExecutor: @escaping @Sendable () async throws -> Value
    ) async throws -> SuccessfulAttempt<Value> {
        let delay = decision.hedgeDelay ?? .zero
        return try await withThrowingTaskGroup(of: AttemptEnvelope<Value>.self) { group in
            group.addTask {
                await self.runEnvelope(route: primary, timeout: request.deadline, startupDelay: .zero, executor: primaryExecutor)
            }
            group.addTask {
                let remaining = Self.remaining(deadline: request.deadline, after: delay)
                return await self.runEnvelope(route: secondary, timeout: remaining, startupDelay: delay, executor: secondaryExecutor)
            }
            var failures: [AttemptFailure] = []
            while let envelope = try await group.next() {
                switch envelope {
                case let .success(success):
                    group.cancelAll()
                    return success
                case let .failure(failure):
                    failures.append(failure)
                case .abandoned:
                    break
                }
            }
            throw HybridInferenceRouterError.allAttemptsFailed(failures)
        }
    }

    private func runEnvelope<Value: Sendable>(
        route: InferenceRoute,
        timeout: Duration?,
        startupDelay: Duration,
        executor: @escaping @Sendable () async throws -> Value
    ) async -> AttemptEnvelope<Value> {
        do {
            if startupDelay > .zero {
                try await Task.sleep(for: startupDelay)
                try Task.checkCancellation()
            }
            let success = try await runAttempt(route: route, timeout: timeout, dropCancellation: true, executor: executor)
            return .success(success)
        } catch is CancellationError {
            return .abandoned
        } catch let error as HybridInferenceRouterError {
            switch error {
            case let .timedOut(route, _):
                return .failure(AttemptFailure(route: route, outcome: .timeout, message: error.errorDescription ?? "Timed out."))
            default:
                return .failure(AttemptFailure(route: route, outcome: .error, message: error.errorDescription ?? "Execution failed."))
            }
        } catch {
            return .failure(AttemptFailure(route: route, outcome: .error, message: String(describing: error)))
        }
    }

    private func runAttempt<Value: Sendable>(
        route: InferenceRoute,
        timeout: Duration?,
        dropCancellation: Bool,
        executor: @escaping @Sendable () async throws -> Value
    ) async throws -> SuccessfulAttempt<Value> {
        start(route)
        let started = DispatchTime.now().uptimeNanoseconds
        do {
            let value = try await Self.withTimeout(route: route, timeout: timeout, executor: executor)
            let latency = Format.latency(from: started)
            finish(route, latency: latency, outcome: .success)
            return SuccessfulAttempt(route: route, latency: latency, value: value)
        } catch is CancellationError {
            if dropCancellation {
                cancel(route)
            } else {
                finish(route, latency: Format.latency(from: started), outcome: .error)
            }
            throw CancellationError()
        } catch let error as HybridInferenceRouterError {
            switch error {
            case .timedOut:
                finish(route, latency: Format.latency(from: started), outcome: .timeout)
            default:
                finish(route, latency: Format.latency(from: started), outcome: .error)
            }
            throw error
        } catch {
            finish(route, latency: Format.latency(from: started), outcome: .error)
            throw error
        }
    }

    private static func withTimeout<Value: Sendable>(
        route: InferenceRoute,
        timeout: Duration?,
        executor: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        guard let timeout else { return try await executor() }
        if timeout <= .zero { throw HybridInferenceRouterError.timedOut(route: route, timeout: .zero) }
        return try await withThrowingTaskGroup(of: Value.self) { group in
            group.addTask { try await executor() }
            group.addTask {
                try await Task.sleep(for: timeout)
                throw HybridInferenceRouterError.timedOut(route: route, timeout: timeout)
            }
            do {
                let value = try await group.next()!
                group.cancelAll()
                return value
            } catch {
                group.cancelAll()
                throw error
            }
        }
    }

    private func estimate(route: InferenceRoute, policy: RoutePolicy, state: RouteState, request: HybridInferenceRequest) -> RouteEstimate {
        var hardReasons: [String] = []
        if !policy.enabled { hardReasons.append("\(policy.name) is disabled.") }
        if let maxPromptTokens = policy.maxPromptTokens, request.promptTokens > maxPromptTokens {
            hardReasons.append("\(policy.name) cannot accept \(request.promptTokens) prompt tokens; its cap is \(maxPromptTokens).")
        }
        if let maxOutputTokens = policy.maxOutputTokens, request.expectedOutputTokens > maxOutputTokens {
            hardReasons.append("\(policy.name) cannot produce \(request.expectedOutputTokens) output tokens; its cap is \(maxOutputTokens).")
        }
        if state.inFlight >= policy.maxInFlight {
            hardReasons.append("\(policy.name) is already at its concurrency ceiling (\(policy.maxInFlight)).")
        }
        if route == .remote && request.network == .offline {
            hardReasons.append("Remote execution is impossible while the network is offline.")
        }
        if route == .remote && request.privacy == .regulated && !policy.supportsRegulatedData {
            hardReasons.append("\(policy.name) is not allowed to handle regulated content.")
        }
        if route == .local && request.thermal == .critical && request.affinity != .requireLocal {
            hardReasons.append("Device thermal state is critical, so local execution is not safe to start.")
        }

        let estimatedCostUSD = estimateCost(policy: policy, request: request)
        let estimatedEnergyJoules = estimateEnergy(policy: policy, request: request)
        if route == .remote, let budget = request.remoteBudgetUSD, estimatedCostUSD > budget {
            hardReasons.append("\(policy.name) would spend \(Format.money(estimatedCostUSD)), above the remote budget of \(Format.money(budget)).")
        }
        if route == .local, let budget = request.localEnergyBudgetJoules, estimatedEnergyJoules > budget {
            hardReasons.append("\(policy.name) would consume about \(Format.number(estimatedEnergyJoules))J, above the local energy budget of \(Format.number(budget))J.")
        }

        let p50Ms = state.p50LatencyMs(fallback: policy.baseP50Latency) + (Double(state.inFlight) * DurationMath.ms(policy.queuePenaltyPerInFlight))
        let p90Ms = max(state.p90LatencyMs(fallback: policy.baseP90Latency), p50Ms) + (Double(state.inFlight) * DurationMath.ms(policy.queuePenaltyPerInFlight))
        let failureRisk = state.failureRisk
        let timeoutRisk = state.timeoutRisk

        var score = p90Ms
        score += failureRisk * options.failurePenaltyMs
        score += timeoutRisk * options.timeoutPenaltyMs
        score += estimatedCostUSD * options.remoteCostPenaltyMsPerDollar
        score += estimatedEnergyJoules * options.localEnergyPenaltyMsPerJoule

        var softReasons: [String] = []
        switch route {
        case .local:
            switch request.power {
            case .pluggedIn:
                break
            case .battery:
                score += options.lowPowerLocalPenaltyMs * 0.35
            case .lowPower:
                score += options.lowPowerLocalPenaltyMs
                softReasons.append("Low Power Mode makes local inference more expensive than usual.")
            }
            switch request.thermal {
            case .nominal, .fair:
                break
            case .serious:
                score += options.seriousThermalLocalPenaltyMs
                softReasons.append("Serious thermal pressure makes local tail latency less trustworthy.")
            case .critical:
                score += options.criticalThermalLocalPenaltyMs
            }
            switch request.privacy {
            case .publicData, .internalData:
                score -= options.localPrivacyBonusMs * 0.5
            case .userContent, .regulated:
                score -= options.localPrivacyBonusMs
            }
        case .remote:
            if request.network == .constrained {
                score += options.constrainedNetworkRemotePenaltyMs
                softReasons.append("Constrained network adds extra remote tail risk.")
            }
            switch request.privacy {
            case .publicData:
                break
            case .internalData:
                score += options.remotePrivacyPenaltyMs * 0.5
            case .userContent:
                score += options.remotePrivacyPenaltyMs
            case .regulated:
                score += options.remotePrivacyPenaltyMs * 2
            }
        }

        switch request.affinity {
        case .automatic, .requireLocal, .requireRemote:
            break
        case .preferLocal:
            score += route == .local ? -options.preferenceBiasMs : options.preferenceBiasMs
        case .preferRemote:
            score += route == .remote ? -options.preferenceBiasMs : options.preferenceBiasMs
        }

        if let deadline = request.deadline {
            let deadlineMs = DurationMath.ms(deadline)
            if p90Ms > deadlineMs {
                let overrun = p90Ms - deadlineMs
                score += options.deadlineMissBasePenaltyMs + (overrun * options.deadlineMissSlope)
                softReasons.append("\(policy.name) predicts a p90 overrun of \(Format.ms(overrun)).")
            } else if p90Ms > deadlineMs * 0.8 {
                softReasons.append("\(policy.name) already uses most of the request deadline.")
            }
        }

        let feasible = hardReasons.isEmpty
        return RouteEstimate(
            route: route,
            feasible: feasible,
            predictedP50Latency: DurationMath.duration(ms: p50Ms),
            predictedP90Latency: DurationMath.duration(ms: p90Ms),
            failureRisk: failureRisk,
            timeoutRisk: timeoutRisk,
            estimatedCostUSD: estimatedCostUSD,
            estimatedEnergyJoules: estimatedEnergyJoules,
            score: feasible ? score : .infinity,
            reasons: hardReasons + softReasons
        )
    }

    private func shouldHedge(primary: RouteEstimate, secondary: RouteEstimate, request: HybridInferenceRequest) -> Bool {
        guard request.allowHedging, primary.feasible, secondary.feasible else { return false }
        guard policy(for: primary.route).hedgeable, policy(for: secondary.route).hedgeable else { return false }
        guard abs(primary.score - secondary.score) <= options.maxHedgeScoreGapMs else { return false }
        let gap = DurationMath.ms(secondary.predictedP50Latency - primary.predictedP50Latency)
        guard gap <= DurationMath.ms(options.maxSecondaryLatencyGap) else { return false }
        let risky = primary.failureRisk >= options.hedgeFailureThreshold || primary.timeoutRisk >= options.hedgeTimeoutThreshold
        if let deadline = request.deadline {
            let threshold = DurationMath.scale(deadline, by: options.hedgeTailDeadlineFraction)
            return risky || (primary.predictedP90Latency >= threshold && secondary.predictedP50Latency <= deadline)
        }
        return risky
    }

    private func hedgeDelay(primary: RouteEstimate, secondary: RouteEstimate, request: HybridInferenceRequest) -> Duration {
        var delay = DurationMath.scale(primary.predictedP50Latency, by: options.hedgeDelayFraction)
        delay = max(options.minHedgeDelay, min(delay, options.maxHedgeDelay))
        if let deadline = request.deadline {
            let latestSafeLaunch = deadline - secondary.predictedP90Latency
            if latestSafeLaunch <= .zero { return .zero }
            delay = min(delay, latestSafeLaunch)
        }
        return max(.zero, delay)
    }

    private func policy(for route: InferenceRoute) -> RoutePolicy {
        route == .local ? localPolicy : remotePolicy
    }

    private func estimateCost(policy: RoutePolicy, request: HybridInferenceRequest) -> Double {
        (Double(request.promptTokens) / 1_000 * policy.inputCostPer1KUSD) + (Double(request.expectedOutputTokens) / 1_000 * policy.outputCostPer1KUSD)
    }

    private func estimateEnergy(policy: RoutePolicy, request: HybridInferenceRequest) -> Double {
        Double(request.totalTokens) / 1_000 * policy.energyPer1KTokensJoules
    }

    private static func remaining(deadline: Duration?, after elapsed: Duration) -> Duration? {
        guard let deadline else { return nil }
        let value = deadline - elapsed
        return value > .zero ? value : .zero
    }

    private func executor<Value: Sendable>(
        for route: InferenceRoute,
        local: (@Sendable () async throws -> Value)?,
        remote: (@Sendable () async throws -> Value)?
    ) -> (@Sendable () async throws -> Value)? {
        route == .local ? local : remote
    }

    private func start(_ route: InferenceRoute) {
        if route == .local { local.start() } else { remote.start() }
    }

    private func finish(_ route: InferenceRoute, latency: Duration, outcome: AttemptOutcome) {
        if route == .local {
            local.finish(latency: latency, outcome: outcome, finishedAt: Date(), options: options)
        } else {
            remote.finish(latency: latency, outcome: outcome, finishedAt: Date(), options: options)
        }
    }

    private func cancel(_ route: InferenceRoute) {
        if route == .local { local.cancel() } else { remote.cancel() }
    }
}

private struct RouteSample: Sendable {
    var latencyMs: Double
    var outcome: AttemptOutcome
    var finishedAt: Date
}

private struct RouteState: Sendable {
    var inFlight = 0
    var samples: [RouteSample] = []
    var ewmaLatencyMs: Double?
    var ewmaFailure = 0.0
    var ewmaTimeout = 0.0

    var successRate: Double {
        guard !samples.isEmpty else { return 1 }
        return Double(samples.filter { $0.outcome == .success }.count) / Double(samples.count)
    }

    var timeoutRate: Double {
        guard !samples.isEmpty else { return 0 }
        return Double(samples.filter { $0.outcome == .timeout }.count) / Double(samples.count)
    }

    var failureRisk: Double { max(1 - successRate, ewmaFailure) }
    var timeoutRisk: Double { max(timeoutRate, ewmaTimeout) }

    mutating func start() { inFlight += 1 }
    mutating func cancel() { inFlight = max(0, inFlight - 1) }

    mutating func finish(latency: Duration, outcome: AttemptOutcome, finishedAt: Date, options: HybridInferenceRouter.Options) {
        inFlight = max(0, inFlight - 1)
        record(latency: latency, outcome: outcome, finishedAt: finishedAt, options: options)
    }

    mutating func record(latency: Duration, outcome: AttemptOutcome, finishedAt: Date, options: HybridInferenceRouter.Options) {
        let latencyMs = max(0, DurationMath.ms(latency))
        samples.append(RouteSample(latencyMs: latencyMs, outcome: outcome, finishedAt: finishedAt))
        if samples.count > options.windowSize {
            samples.removeFirst(samples.count - options.windowSize)
        }
        ewmaLatencyMs = EWMA.next(current: ewmaLatencyMs, sample: latencyMs, alpha: options.latencyAlpha)
        ewmaFailure = EWMA.next(current: ewmaFailure, sample: outcome == .success ? 0.0 : 1.0, alpha: options.failureAlpha)
        ewmaTimeout = EWMA.next(current: ewmaTimeout, sample: outcome == .timeout ? 1.0 : 0.0, alpha: options.timeoutAlpha)
    }

    func p50LatencyMs(fallback: Duration) -> Double {
        percentile(0.50) ?? ewmaLatencyMs ?? DurationMath.ms(fallback)
    }

    func p90LatencyMs(fallback: Duration) -> Double {
        percentile(0.90) ?? max(ewmaLatencyMs ?? DurationMath.ms(fallback), DurationMath.ms(fallback))
    }

    func snapshot(route: InferenceRoute, policy: RoutePolicy) -> RouteHealthSnapshot {
        RouteHealthSnapshot(
            route: route,
            policyName: policy.name,
            sampleCount: samples.count,
            inFlight: inFlight,
            successRate: successRate,
            timeoutRate: timeoutRate,
            ewmaLatency: ewmaLatencyMs.map(DurationMath.duration(ms:)),
            p50Latency: percentile(0.50).map(DurationMath.duration(ms:)),
            p90Latency: percentile(0.90).map(DurationMath.duration(ms:))
        )
    }

    private func percentile(_ p: Double) -> Double? {
        guard !samples.isEmpty else { return nil }
        let sorted = samples.map(\.latencyMs).sorted()
        if sorted.count == 1 { return sorted[0] }
        let position = Double(sorted.count - 1) * min(max(p, 0), 1)
        let lower = Int(position.rounded(.down))
        let upper = Int(position.rounded(.up))
        if lower == upper { return sorted[lower] }
        let weight = position - Double(lower)
        return sorted[lower] + ((sorted[upper] - sorted[lower]) * weight)
    }
}

private struct SuccessfulAttempt<Value: Sendable>: Sendable {
    var route: InferenceRoute
    var latency: Duration
    var value: Value
}

private enum AttemptEnvelope<Value: Sendable>: Sendable {
    case success(SuccessfulAttempt<Value>)
    case failure(AttemptFailure)
    case abandoned
}

private enum EWMA {
    static func next(current: Double?, sample: Double, alpha: Double) -> Double {
        guard let current else { return sample }
        return current + (alpha * (sample - current))
    }

    static func next(current: Double, sample: Double, alpha: Double) -> Double {
        current + (alpha * (sample - current))
    }
}

private enum DurationMath {
    static func ms(_ duration: Duration) -> Double {
        let c = duration.components
        return (Double(c.seconds) * 1_000) + (Double(c.attoseconds) / 1_000_000_000_000_000)
    }

    static func duration(ms: Double) -> Duration {
        .nanoseconds(Int64(max(0, (ms * 1_000_000).rounded())))
    }

    static func scale(_ duration: Duration, by factor: Double) -> Duration {
        Self.duration(ms: ms(duration) * factor)
    }
}

private enum Format {
    static func latency(from startNanoseconds: UInt64) -> Duration {
        let end = DispatchTime.now().uptimeNanoseconds
        return .nanoseconds(Int64(end &- startNanoseconds))
    }

    static func duration(_ duration: Duration) -> String {
        ms(DurationMath.ms(duration))
    }

    static func ms(_ milliseconds: Double) -> String {
        if milliseconds >= 1_000 { return String(format: "%.2fs", milliseconds / 1_000) }
        if milliseconds >= 1 { return String(format: "%.0fms", milliseconds) }
        return String(format: "%.3fms", milliseconds)
    }

    static func money(_ value: Double) -> String {
        String(format: "$%.4f", value)
    }

    static func number(_ value: Double) -> String {
        String(format: "%.3f", value)
    }

    static func score(_ value: Double) -> String {
        value.isFinite ? String(format: "%.1f", value) : "infinity"
    }
}

/*
This solves the real April 2026 problem of deciding whether a Swift app or Swift service should run inference locally, send it to a remote model, or start with one path and hedge with the other when the deadline is too tight. Built because hybrid AI stacks are normal now across Core ML, MLX, local llama.cpp runners, OpenAI-compatible gateways, Anthropic, and Azure, but most routing logic is still a pile of shallow if-statements that breaks as soon as latency, thermal pressure, privacy rules, or cost targets move.

Use it when you have both an on-device or on-box model path and a cloud model path and you need one place to make that decision with real signals. It fits mobile apps, macOS tools, Vapor services, internal SDKs, edge workers, agent runtimes, and any Swift system that has to balance p50 and p90 latency, queue pressure, battery drain, timeout risk, remote spend, and privacy at the same time.

The trick: instead of pretending one metric is enough, this file keeps a bounded recent health window for both local and remote routes, tracks EWMA latency plus failure and timeout risk, converts those signals into a score, and only enables hedging when the two routes are close enough that racing them is actually worth the duplicate work. That gives you a router that behaves like an operator would think during a real incident instead of like a demo app.

Drop this into a Swift package, an iOS or macOS support layer, a server target, or an internal inference runtime. Feed `record` with telemetry if you already have it, or let `execute` learn from live traffic over time. If someone is searching GitHub or Google for Swift hybrid inference router, Swift local vs remote LLM routing, deadline aware AI hedging, MLX fallback to cloud, Apple on-device inference budget control, or Swift inference failover, this is exactly the kind of single-file implementation I would want them to find because it is practical, readable, and immediately useful.
*/