#!/usr/bin/env swift
import Foundation
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

enum PlannerError: Error, CustomStringConvertible {
    case message(String)

    var description: String {
        switch self {
        case .message(let text): return text
        }
    }
}

enum OutputFormat: String {
    case markdown
    case json
}

struct Limits {
    var maxP95Ms: Double = 1400
    var maxErrorRate: Double = 0.025
    var maxCostPer1KTokens: Double = 0.025
    var maxCarbonGPer1KTokens: Double = 35
    var minSamples: Int = 20
    var probeBudgetUsd: Double = 10
    var defaultProbeCostUsd: Double = 0.006
    var probeTokenSize: Int = 1800
    var maxProbesPerRoute: Int = 40
    var minCacheHitRate: Double = 0.08
    var maxColdStartRate: Double = 0.08
}

struct CliOptions {
    var inputPath: String?
    var format: OutputFormat = .markdown
    var limits = Limits()
    var requiredRegions: Set<String> = []
    var failOnCritical = false
    var selfTest = false

    static func parse(_ rawArguments: [String]) throws -> CliOptions {
        var options = CliOptions()
        var index = 0

        func requireValue(_ flag: String) throws -> String {
            guard index + 1 < rawArguments.count else {
                throw PlannerError.message("Missing value for \(flag)")
            }
            index += 1
            return rawArguments[index]
        }

        while index < rawArguments.count {
            let arg = rawArguments[index]
            switch arg {
            case "--input", "-i":
                options.inputPath = try requireValue(arg)
            case "--format":
                let raw = try requireValue(arg).lowercased()
                guard let parsed = OutputFormat(rawValue: raw) else {
                    throw PlannerError.message("Unsupported format '\(raw)'. Use markdown or json.")
                }
                options.format = parsed
            case "--max-p95-ms":
                options.limits.maxP95Ms = try parseDouble(try requireValue(arg), flag: arg)
            case "--max-error-rate":
                options.limits.maxErrorRate = try parseRate(try requireValue(arg), flag: arg)
            case "--max-cost-per-1k":
                options.limits.maxCostPer1KTokens = try parseDouble(try requireValue(arg), flag: arg)
            case "--max-carbon-g-per-1k":
                options.limits.maxCarbonGPer1KTokens = try parseDouble(try requireValue(arg), flag: arg)
            case "--min-samples":
                options.limits.minSamples = try parseInt(try requireValue(arg), flag: arg)
            case "--probe-budget-usd":
                options.limits.probeBudgetUsd = try parseDouble(try requireValue(arg), flag: arg)
            case "--default-probe-cost-usd":
                options.limits.defaultProbeCostUsd = try parseDouble(try requireValue(arg), flag: arg)
            case "--probe-token-size":
                options.limits.probeTokenSize = try parseInt(try requireValue(arg), flag: arg)
            case "--max-probes-per-route":
                options.limits.maxProbesPerRoute = try parseInt(try requireValue(arg), flag: arg)
            case "--min-cache-hit-rate":
                options.limits.minCacheHitRate = try parseRate(try requireValue(arg), flag: arg)
            case "--max-cold-start-rate":
                options.limits.maxColdStartRate = try parseRate(try requireValue(arg), flag: arg)
            case "--required-region":
                let regions = try requireValue(arg).split(separator: ",").map { normalize(String($0)) }
                options.requiredRegions.formUnion(regions.filter { !$0.isEmpty })
            case "--fail-on-critical":
                options.failOnCritical = true
            case "--self-test":
                options.selfTest = true
            case "--help", "-h":
                throw PlannerError.message(Self.usage())
            default:
                throw PlannerError.message("Unknown argument \(arg)\n\n\(Self.usage())")
            }
            index += 1
        }

        return options
    }

    static func usage() -> String {
        return """
        EdgeInferenceProbePlanner.swift

        Reads edge AI inference telemetry as JSON, JSON array, or JSONL and produces a ranked canary probe plan.

        Usage:
          swiftc EdgeInferenceProbePlanner.swift -o edge-probe-planner
          ./edge-probe-planner --input telemetry.jsonl --format markdown --probe-budget-usd 25 --required-region iad,fra

        Important flags:
          --format markdown|json
          --max-p95-ms 1400
          --max-error-rate 0.025
          --max-cost-per-1k 0.025
          --max-carbon-g-per-1k 35
          --min-samples 20
          --probe-budget-usd 10
          --fail-on-critical
          --self-test
        """
    }
}

struct Observation {
    let line: Int
    let timestamp: String
    let region: String
    let provider: String
    let model: String
    let route: String
    let tenant: String
    let residency: String
    let status: String
    let success: Bool
    let latencyMs: Double
    let inputTokens: Int
    let outputTokens: Int
    let costUsd: Double
    let energyWh: Double
    let carbonG: Double
    let cacheHit: Bool
    let coldStart: Bool

    var totalTokens: Int {
        return max(0, inputTokens) + max(0, outputTokens)
    }

    init(object: [String: Any], line: Int) throws {
        self.line = line
        timestamp = stringField(object, keys: ["timestamp", "time", "ts"], defaultValue: "unknown")
        region = normalize(stringField(object, keys: ["region", "edge_region", "pop", "colo"], defaultValue: "unknown"))
        provider = normalize(stringField(object, keys: ["provider", "vendor", "gateway_provider"], defaultValue: "unknown"))
        model = normalize(stringField(object, keys: ["model", "model_id", "deployment"], defaultValue: "unknown"))
        route = normalize(stringField(object, keys: ["route", "endpoint", "operation", "path"], defaultValue: "default"))
        tenant = normalize(stringField(object, keys: ["tenant", "workspace", "project", "account"], defaultValue: "unknown"))
        residency = normalize(stringField(object, keys: ["residency", "data_residency", "jurisdiction"], defaultValue: "unspecified"))
        status = normalize(stringField(object, keys: ["status", "outcome", "result"], defaultValue: "ok"))

        let explicitSuccess = boolField(object, keys: ["success", "ok", "succeeded"])
        let statusSuccess = ["ok", "success", "succeeded", "200", "201", "202", "204", "cache_hit"].contains(status)
        success = explicitSuccess ?? statusSuccess

        latencyMs = max(0, doubleField(object, keys: ["latency_ms", "latencyMs", "duration_ms", "durationMs"], defaultValue: 0))
        inputTokens = max(0, intField(object, keys: ["input_tokens", "tokens_in", "prompt_tokens", "inputTokens"], defaultValue: 0))
        outputTokens = max(0, intField(object, keys: ["output_tokens", "tokens_out", "completion_tokens", "outputTokens"], defaultValue: 0))
        costUsd = max(0, doubleField(object, keys: ["cost_usd", "costUsd", "estimated_cost_usd", "charge_usd"], defaultValue: 0))
        energyWh = max(0, doubleField(object, keys: ["energy_wh", "energyWh", "estimated_energy_wh"], defaultValue: 0))
        let explicitCarbon = doubleField(object, keys: ["carbon_g", "carbonG", "co2e_g", "co2eG"], defaultValue: -1)
        if explicitCarbon >= 0 {
            carbonG = explicitCarbon
        } else {
            let grid = max(0, doubleField(object, keys: ["grid_gco2_per_kwh", "gridCarbonGPerKwh", "carbon_intensity_g_per_kwh"], defaultValue: 0))
            carbonG = (energyWh / 1000) * grid
        }
        cacheHit = boolField(object, keys: ["cache_hit", "cacheHit", "prompt_cache_hit"]) ?? false
        coldStart = boolField(object, keys: ["cold_start", "coldStart", "was_cold_start"]) ?? false
    }
}

struct RouteKey: Hashable, Comparable {
    let region: String
    let provider: String
    let model: String
    let route: String
    let residency: String

    var label: String {
        return "\(region)/\(provider)/\(model)/\(route)"
    }

    static func < (lhs: RouteKey, rhs: RouteKey) -> Bool {
        return lhs.label == rhs.label ? lhs.residency < rhs.residency : lhs.label < rhs.label
    }
}

struct RouteStats {
    var samples = 0
    var successes = 0
    var errors = 0
    var latencies: [Double] = []
    var tokens = 0
    var costUsd = 0.0
    var energyWh = 0.0
    var carbonG = 0.0
    var cacheHits = 0
    var coldStarts = 0
    var tenants: Set<String> = []
    var firstTimestamp = "unknown"
    var lastTimestamp = "unknown"

    mutating func add(_ observation: Observation) {
        if samples == 0 {
            firstTimestamp = observation.timestamp
        }
        samples += 1
        lastTimestamp = observation.timestamp
        successes += observation.success ? 1 : 0
        errors += observation.success ? 0 : 1
        latencies.append(observation.latencyMs)
        tokens += observation.totalTokens
        costUsd += observation.costUsd
        energyWh += observation.energyWh
        carbonG += observation.carbonG
        cacheHits += observation.cacheHit ? 1 : 0
        coldStarts += observation.coldStart ? 1 : 0
        tenants.insert(observation.tenant)
    }
}

struct RouteMetrics {
    let samples: Int
    let successes: Int
    let errors: Int
    let p50Ms: Double
    let p95Ms: Double
    let p99Ms: Double
    let errorRate: Double
    let costUsd: Double
    let costPer1KTokens: Double
    let carbonG: Double
    let carbonGPer1KTokens: Double
    let cacheHitRate: Double
    let coldStartRate: Double
    let tokenVolume: Int
    let tenantCount: Int

    init(stats: RouteStats) {
        samples = stats.samples
        successes = stats.successes
        errors = stats.errors
        p50Ms = quantile(stats.latencies, q: 0.50)
        p95Ms = quantile(stats.latencies, q: 0.95)
        p99Ms = quantile(stats.latencies, q: 0.99)
        errorRate = safeRatio(Double(stats.errors), Double(max(stats.samples, 1)))
        costUsd = stats.costUsd
        costPer1KTokens = stats.tokens > 0 ? stats.costUsd / Double(stats.tokens) * 1000 : 0
        carbonG = stats.carbonG
        carbonGPer1KTokens = stats.tokens > 0 ? stats.carbonG / Double(stats.tokens) * 1000 : 0
        cacheHitRate = safeRatio(Double(stats.cacheHits), Double(max(stats.samples, 1)))
        coldStartRate = safeRatio(Double(stats.coldStarts), Double(max(stats.samples, 1)))
        tokenVolume = stats.tokens
        tenantCount = stats.tenants.count
    }
}

struct RouteAssessment {
    let key: RouteKey
    let metrics: RouteMetrics
    let riskScore: Double
    let severity: String
    let action: String
    let reasons: [String]

    var isCritical: Bool {
        return severity == "critical"
    }
}

struct ProbeAllocation {
    let key: RouteKey
    let probes: Int
    let estimatedCostUsd: Double
    let reason: String
}

struct PlanReport {
    let generatedAt: String
    let observations: Int
    let routeCount: Int
    let limits: Limits
    let requiredRegions: Set<String>
    let assessments: [RouteAssessment]
    let probes: [ProbeAllocation]
    let missingRequiredRegions: [String]

    var criticalCount: Int {
        return assessments.filter { $0.severity == "critical" }.count
    }

    var warningCount: Int {
        return assessments.filter { $0.severity == "warning" }.count
    }
}

func parseDouble(_ raw: String, flag: String) throws -> Double {
    guard let value = Double(raw), value.isFinite else {
        throw PlannerError.message("Invalid numeric value for \(flag): \(raw)")
    }
    return value
}

func parseRate(_ raw: String, flag: String) throws -> Double {
    let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
    if trimmed.hasSuffix("%") {
        let withoutPercent = String(trimmed.dropLast())
        return try parseDouble(withoutPercent, flag: flag) / 100.0
    }
    return try parseDouble(trimmed, flag: flag)
}

func parseInt(_ raw: String, flag: String) throws -> Int {
    guard let value = Int(raw), value >= 0 else {
        throw PlannerError.message("Invalid integer value for \(flag): \(raw)")
    }
    return value
}

func normalize(_ raw: String) -> String {
    return raw.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
}

func safeRatio(_ numerator: Double, _ denominator: Double) -> Double {
    guard denominator > 0 else { return 0 }
    return numerator / denominator
}

func stringField(_ object: [String: Any], keys: [String], defaultValue: String) -> String {
    for key in keys {
        if let value = object[key] as? String, !value.isEmpty {
            return value
        }
        if let value = object[key] as? NSNumber {
            return value.stringValue
        }
    }
    return defaultValue
}

func doubleField(_ object: [String: Any], keys: [String], defaultValue: Double) -> Double {
    for key in keys {
        if let value = object[key] as? Double { return value }
        if let value = object[key] as? Int { return Double(value) }
        if let value = object[key] as? NSNumber { return value.doubleValue }
        if let value = object[key] as? String, let parsed = Double(value) { return parsed }
    }
    return defaultValue
}

func intField(_ object: [String: Any], keys: [String], defaultValue: Int) -> Int {
    for key in keys {
        if let value = object[key] as? Int { return value }
        if let value = object[key] as? NSNumber { return value.intValue }
        if let value = object[key] as? String, let parsed = Int(value) { return parsed }
    }
    return defaultValue
}

func boolField(_ object: [String: Any], keys: [String]) -> Bool? {
    for key in keys {
        if let value = object[key] as? Bool { return value }
        if let value = object[key] as? NSNumber { return value.boolValue }
        if let value = object[key] as? String {
            switch normalize(value) {
            case "true", "yes", "1", "ok", "success", "hit": return true
            case "false", "no", "0", "error", "failed", "miss": return false
            default: continue
            }
        }
    }
    return nil
}

func quantile(_ values: [Double], q: Double) -> Double {
    guard !values.isEmpty else { return 0 }
    let sorted = values.sorted()
    if sorted.count == 1 { return sorted[0] }
    let clamped = min(1, max(0, q))
    let position = clamped * Double(sorted.count - 1)
    let lower = Int(floor(position))
    let upper = Int(ceil(position))
    if lower == upper { return sorted[lower] }
    let fraction = position - Double(lower)
    return sorted[lower] * (1 - fraction) + sorted[upper] * fraction
}

func readInput(path: String?) throws -> String {
    if let path = path {
        return try String(contentsOfFile: path, encoding: .utf8)
    }
    let data = FileHandle.standardInput.readDataToEndOfFile()
    guard let text = String(data: data, encoding: .utf8) else {
        throw PlannerError.message("stdin is not valid UTF-8")
    }
    return text
}

func parseObservations(_ text: String) throws -> [Observation] {
    let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else {
        throw PlannerError.message("No telemetry records found")
    }

    if trimmed.hasPrefix("[") {
        let data = Data(trimmed.utf8)
        let raw = try JSONSerialization.jsonObject(with: data, options: [])
        guard let rows = raw as? [[String: Any]] else {
            throw PlannerError.message("Top-level JSON array must contain objects")
        }
        return try rows.enumerated().map { offset, object in
            try Observation(object: object, line: offset + 1)
        }
    }

    var observations: [Observation] = []
    for (offset, rawLine) in trimmed.split(separator: "\n", omittingEmptySubsequences: false).enumerated() {
        let lineText = rawLine.trimmingCharacters(in: .whitespacesAndNewlines)
        if lineText.isEmpty || lineText.hasPrefix("#") {
            continue
        }
        let data = Data(lineText.utf8)
        let raw = try JSONSerialization.jsonObject(with: data, options: [])
        guard let object = raw as? [String: Any] else {
            throw PlannerError.message("Line \(offset + 1) is not a JSON object")
        }
        observations.append(try Observation(object: object, line: offset + 1))
    }
    guard !observations.isEmpty else {
        throw PlannerError.message("No JSON object records found")
    }
    return observations
}

func aggregate(_ observations: [Observation]) -> [RouteKey: RouteStats] {
    var stats: [RouteKey: RouteStats] = [:]
    for observation in observations {
        let key = RouteKey(
            region: observation.region,
            provider: observation.provider,
            model: observation.model,
            route: observation.route,
            residency: observation.residency
        )
        var bucket = stats[key] ?? RouteStats()
        bucket.add(observation)
        stats[key] = bucket
    }
    return stats
}

func assess(key: RouteKey, stats: RouteStats, limits: Limits) -> RouteAssessment {
    let metrics = RouteMetrics(stats: stats)
    var risk = 0.0
    var reasons: [String] = []

    if metrics.samples < limits.minSamples {
        let gap = Double(limits.minSamples - metrics.samples) / Double(max(limits.minSamples, 1))
        risk += 22 + gap * 18
        reasons.append("under-sampled route: \(metrics.samples) samples, needs \(limits.minSamples)")
    }
    if metrics.p95Ms > limits.maxP95Ms {
        let ratio = metrics.p95Ms / max(limits.maxP95Ms, 1)
        risk += min(35, (ratio - 1) * 30 + 12)
        reasons.append("p95 latency \(formatNumber(metrics.p95Ms))ms exceeds \(formatNumber(limits.maxP95Ms))ms")
    }
    if metrics.errorRate > limits.maxErrorRate {
        let ratio = metrics.errorRate / max(limits.maxErrorRate, 0.0001)
        risk += min(38, (ratio - 1) * 24 + 14)
        reasons.append("error rate \(formatPercent(metrics.errorRate)) exceeds \(formatPercent(limits.maxErrorRate))")
    }
    if metrics.costPer1KTokens > limits.maxCostPer1KTokens {
        let ratio = metrics.costPer1KTokens / max(limits.maxCostPer1KTokens, 0.000001)
        risk += min(18, (ratio - 1) * 12 + 6)
        reasons.append("cost \(formatUsd(metrics.costPer1KTokens))/1k tokens exceeds \(formatUsd(limits.maxCostPer1KTokens))")
    }
    if metrics.carbonGPer1KTokens > limits.maxCarbonGPer1KTokens {
        let ratio = metrics.carbonGPer1KTokens / max(limits.maxCarbonGPer1KTokens, 0.000001)
        risk += min(16, (ratio - 1) * 10 + 5)
        reasons.append("carbon \(formatNumber(metrics.carbonGPer1KTokens))g/1k tokens exceeds \(formatNumber(limits.maxCarbonGPer1KTokens))g")
    }
    if metrics.cacheHitRate < limits.minCacheHitRate && metrics.samples >= limits.minSamples {
        risk += 6
        reasons.append("prompt cache hit rate \(formatPercent(metrics.cacheHitRate)) is below \(formatPercent(limits.minCacheHitRate))")
    }
    if metrics.coldStartRate > limits.maxColdStartRate {
        risk += 10
        reasons.append("cold start rate \(formatPercent(metrics.coldStartRate)) exceeds \(formatPercent(limits.maxColdStartRate))")
    }

    let cappedRisk = min(100, max(0, risk))
    let severity: String
    let action: String
    if cappedRisk >= 70 {
        severity = "critical"
        action = "freeze traffic growth, run synthetic probes, and require a clean p95/error window before promotion"
    } else if cappedRisk >= 35 {
        severity = "warning"
        action = "keep route active but shift bursty traffic away until probe evidence improves"
    } else if metrics.samples < limits.minSamples {
        severity = "warning"
        action = "collect more canary samples before treating this route as healthy"
    } else {
        severity = "ok"
        action = "eligible for normal routing; keep routine probe coverage"
    }

    return RouteAssessment(
        key: key,
        metrics: metrics,
        riskScore: cappedRisk,
        severity: severity,
        action: action,
        reasons: reasons.isEmpty ? ["within configured SLO, cost, carbon, cache, and cold-start limits"] : reasons
    )
}

func buildProbePlan(assessments: [RouteAssessment], limits: Limits) -> [ProbeAllocation] {
    guard limits.probeBudgetUsd > 0 else { return [] }
    let candidates = assessments
        .filter { $0.riskScore > 0 || $0.metrics.samples < limits.minSamples }
        .sorted { lhs, rhs in
            if lhs.riskScore == rhs.riskScore {
                return lhs.key < rhs.key
            }
            return lhs.riskScore > rhs.riskScore
        }
    guard !candidates.isEmpty else { return [] }

    let totalWeight = candidates.reduce(0.0) { partial, assessment in
        partial + max(5, assessment.riskScore)
    }
    var remainingBudget = limits.probeBudgetUsd
    var allocations: [ProbeAllocation] = []

    for candidate in candidates {
        if remainingBudget <= 0 { break }
        let weight = max(5, candidate.riskScore)
        let share = limits.probeBudgetUsd * weight / max(totalWeight, 1)
        let observedCost = candidate.metrics.costPer1KTokens > 0
            ? candidate.metrics.costPer1KTokens * Double(max(limits.probeTokenSize, 1)) / 1000
            : limits.defaultProbeCostUsd
        let costPerProbe = max(0.0001, min(1.0, observedCost))
        let affordable = Int(floor(min(share, remainingBudget) / costPerProbe))
        let neededForSamples = max(0, limits.minSamples - candidate.metrics.samples)
        let severityFloor = candidate.severity == "critical" ? 8 : 3
        let requested = max(neededForSamples, severityFloor)
        let probes = min(limits.maxProbesPerRoute, max(0, min(requested, affordable)))
        if probes <= 0 { continue }

        let cost = Double(probes) * costPerProbe
        remainingBudget -= cost
        let reason = candidate.reasons.first ?? "risk-weighted route coverage"
        allocations.append(ProbeAllocation(key: candidate.key, probes: probes, estimatedCostUsd: cost, reason: reason))
    }

    return allocations
}

func planReport(observations: [Observation], options: CliOptions) -> PlanReport {
    let buckets = aggregate(observations)
    let assessments = buckets.map { key, stats in
        assess(key: key, stats: stats, limits: options.limits)
    }.sorted { lhs, rhs in
        if lhs.riskScore == rhs.riskScore {
            return lhs.key < rhs.key
        }
        return lhs.riskScore > rhs.riskScore
    }
    let regions = Set(buckets.keys.map { $0.region })
    let missingRegions = options.requiredRegions.subtracting(regions).sorted()
    var adjustedAssessments = assessments
    if !missingRegions.isEmpty {
        let missingReason = "required regions missing from telemetry: \(missingRegions.joined(separator: ", "))"
        let synthetic = RouteAssessment(
            key: RouteKey(region: missingRegions.joined(separator: "+"), provider: "unobserved", model: "unobserved", route: "required-region", residency: "unknown"),
            metrics: RouteMetrics(stats: RouteStats()),
            riskScore: 75,
            severity: "critical",
            action: "block launch or add regional probes before rollout",
            reasons: [missingReason]
        )
        adjustedAssessments.insert(synthetic, at: 0)
    }

    return PlanReport(
        generatedAt: isoTimestamp(),
        observations: observations.count,
        routeCount: buckets.count,
        limits: options.limits,
        requiredRegions: options.requiredRegions,
        assessments: adjustedAssessments,
        probes: buildProbePlan(assessments: adjustedAssessments, limits: options.limits),
        missingRequiredRegions: missingRegions
    )
}

func renderMarkdown(_ report: PlanReport) -> String {
    var lines: [String] = []
    lines.append("# Edge inference probe plan")
    lines.append("")
    lines.append("Generated: \(report.generatedAt)")
    lines.append("")
    lines.append("- Observations: \(report.observations)")
    lines.append("- Routes: \(report.routeCount)")
    lines.append("- Critical routes: \(report.criticalCount)")
    lines.append("- Warning routes: \(report.warningCount)")
    lines.append("- Probe budget: \(formatUsd(report.limits.probeBudgetUsd))")
    if !report.missingRequiredRegions.isEmpty {
        lines.append("- Missing required regions: \(report.missingRequiredRegions.joined(separator: ", "))")
    }
    lines.append("")
    lines.append("## Ranked route risk")
    lines.append("")
    lines.append("| Severity | Risk | Route | Samples | p95 ms | Error | Cost/1k | Carbon g/1k | Action |")
    lines.append("| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |")
    for assessment in report.assessments {
        let metrics = assessment.metrics
        lines.append("| \(assessment.severity) | \(formatNumber(assessment.riskScore)) | \(escapeCell(assessment.key.label)) | \(metrics.samples) | \(formatNumber(metrics.p95Ms)) | \(formatPercent(metrics.errorRate)) | \(formatUsd(metrics.costPer1KTokens)) | \(formatNumber(metrics.carbonGPer1KTokens)) | \(escapeCell(assessment.action)) |")
    }
    lines.append("")
    lines.append("## Probe allocation")
    lines.append("")
    if report.probes.isEmpty {
        lines.append("No additional synthetic probes are required under the configured budget and thresholds.")
    } else {
        lines.append("| Route | Probes | Estimated cost | Reason |")
        lines.append("| --- | ---: | ---: | --- |")
        for probe in report.probes {
            lines.append("| \(escapeCell(probe.key.label)) | \(probe.probes) | \(formatUsd(probe.estimatedCostUsd)) | \(escapeCell(probe.reason)) |")
        }
    }
    lines.append("")
    lines.append("## Route notes")
    lines.append("")
    for assessment in report.assessments {
        lines.append("### \(assessment.key.label)")
        lines.append("")
        for reason in assessment.reasons {
            lines.append("- \(reason)")
        }
        lines.append("")
    }
    return lines.joined(separator: "\n")
}

func renderJson(_ report: PlanReport) throws -> String {
    let object: [String: Any] = [
        "generated_at": report.generatedAt,
        "observations": report.observations,
        "routes": report.routeCount,
        "critical_routes": report.criticalCount,
        "warning_routes": report.warningCount,
        "missing_required_regions": report.missingRequiredRegions,
        "limits": [
            "max_p95_ms": report.limits.maxP95Ms,
            "max_error_rate": report.limits.maxErrorRate,
            "max_cost_per_1k_tokens": report.limits.maxCostPer1KTokens,
            "max_carbon_g_per_1k_tokens": report.limits.maxCarbonGPer1KTokens,
            "min_samples": report.limits.minSamples,
            "probe_budget_usd": report.limits.probeBudgetUsd
        ],
        "assessments": report.assessments.map { assessment in
            [
                "route": [
                    "region": assessment.key.region,
                    "provider": assessment.key.provider,
                    "model": assessment.key.model,
                    "operation": assessment.key.route,
                    "residency": assessment.key.residency
                ],
                "severity": assessment.severity,
                "risk_score": rounded(assessment.riskScore),
                "action": assessment.action,
                "reasons": assessment.reasons,
                "metrics": [
                    "samples": assessment.metrics.samples,
                    "successes": assessment.metrics.successes,
                    "errors": assessment.metrics.errors,
                    "p50_ms": rounded(assessment.metrics.p50Ms),
                    "p95_ms": rounded(assessment.metrics.p95Ms),
                    "p99_ms": rounded(assessment.metrics.p99Ms),
                    "error_rate": rounded(assessment.metrics.errorRate),
                    "cost_usd": rounded(assessment.metrics.costUsd),
                    "cost_per_1k_tokens": rounded(assessment.metrics.costPer1KTokens),
                    "carbon_g": rounded(assessment.metrics.carbonG),
                    "carbon_g_per_1k_tokens": rounded(assessment.metrics.carbonGPer1KTokens),
                    "cache_hit_rate": rounded(assessment.metrics.cacheHitRate),
                    "cold_start_rate": rounded(assessment.metrics.coldStartRate),
                    "tokens": assessment.metrics.tokenVolume,
                    "tenants": assessment.metrics.tenantCount
                ]
            ] as [String: Any]
        },
        "probe_plan": report.probes.map { probe in
            [
                "route": [
                    "region": probe.key.region,
                    "provider": probe.key.provider,
                    "model": probe.key.model,
                    "operation": probe.key.route
                ],
                "probes": probe.probes,
                "estimated_cost_usd": rounded(probe.estimatedCostUsd),
                "reason": probe.reason
            ] as [String: Any]
        }
    ]
    let data = try JSONSerialization.data(withJSONObject: object, options: [.prettyPrinted, .sortedKeys])
    guard let text = String(data: data, encoding: .utf8) else {
        throw PlannerError.message("Could not encode JSON report")
    }
    return text
}

func escapeCell(_ text: String) -> String {
    return text.replacingOccurrences(of: "|", with: "\\|").replacingOccurrences(of: "\n", with: " ")
}

func formatNumber(_ value: Double) -> String {
    return String(format: "%.2f", value)
}

func rounded(_ value: Double) -> Double {
    return (value * 10000).rounded() / 10000
}

func formatPercent(_ value: Double) -> String {
    return String(format: "%.2f%%", value * 100)
}

func formatUsd(_ value: Double) -> String {
    return String(format: "$%.4f", value)
}

func isoTimestamp() -> String {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime]
    return formatter.string(from: Date())
}

func run(options: CliOptions) throws -> (String, Bool) {
    let text = try readInput(path: options.inputPath)
    let observations = try parseObservations(text)
    let report = planReport(observations: observations, options: options)
    let rendered: String
    switch options.format {
    case .markdown:
        rendered = renderMarkdown(report)
    case .json:
        rendered = try renderJson(report)
    }
    return (rendered, options.failOnCritical && report.criticalCount > 0)
}

func runSelfTest() throws {
    let sample = """
    {"timestamp":"2026-04-12T08:00:01Z","region":"iad","provider":"openai","model":"gpt-4.1-mini","route":"chat","tenant":"alpha","status":"ok","latency_ms":820,"input_tokens":900,"output_tokens":250,"cost_usd":0.012,"energy_wh":0.8,"grid_gco2_per_kwh":390,"cache_hit":false,"cold_start":false}
    {"timestamp":"2026-04-12T08:00:02Z","region":"iad","provider":"openai","model":"gpt-4.1-mini","route":"chat","tenant":"alpha","status":"error","latency_ms":2100,"input_tokens":900,"output_tokens":0,"cost_usd":0.010,"energy_wh":0.7,"grid_gco2_per_kwh":390,"cache_hit":false,"cold_start":true}
    {"timestamp":"2026-04-12T08:00:03Z","region":"iad","provider":"openai","model":"gpt-4.1-mini","route":"chat","tenant":"beta","status":"ok","latency_ms":1780,"input_tokens":1200,"output_tokens":280,"cost_usd":0.015,"energy_wh":0.9,"grid_gco2_per_kwh":390,"cache_hit":false,"cold_start":true}
    {"timestamp":"2026-04-12T08:00:04Z","region":"sfo","provider":"anthropic","model":"claude-3.7-sonnet","route":"chat","tenant":"alpha","status":"ok","latency_ms":610,"input_tokens":700,"output_tokens":220,"cost_usd":0.008,"energy_wh":0.5,"grid_gco2_per_kwh":120,"cache_hit":true,"cold_start":false}
    {"timestamp":"2026-04-12T08:00:05Z","region":"sfo","provider":"anthropic","model":"claude-3.7-sonnet","route":"chat","tenant":"beta","status":"ok","latency_ms":640,"input_tokens":720,"output_tokens":200,"cost_usd":0.008,"energy_wh":0.5,"grid_gco2_per_kwh":120,"cache_hit":true,"cold_start":false}
    {"timestamp":"2026-04-12T08:00:06Z","region":"fra","provider":"local","model":"llama-4-scout","route":"embed","tenant":"gamma","status":"ok","latency_ms":920,"input_tokens":300,"output_tokens":30,"cost_usd":0.002,"energy_wh":1.4,"grid_gco2_per_kwh":620,"cache_hit":false,"cold_start":false}
    """
    var options = CliOptions()
    options.format = .json
    options.limits.minSamples = 4
    options.limits.maxP95Ms = 1000
    options.limits.maxErrorRate = 0.10
    options.requiredRegions = ["iad", "sfo", "fra", "bom"]
    let observations = try parseObservations(sample)
    guard observations.count == 6 else {
        throw PlannerError.message("self-test expected 6 observations")
    }
    let report = planReport(observations: observations, options: options)
    guard report.criticalCount >= 2 else {
        throw PlannerError.message("self-test expected at least two critical routes")
    }
    let json = try renderJson(report)
    guard json.contains("required regions missing") && json.contains("probe_plan") else {
        throw PlannerError.message("self-test report did not include expected diagnostics")
    }
    let markdown = renderMarkdown(report)
    guard markdown.contains("iad/openai/gpt-4.1-mini/chat") && markdown.contains("Probe allocation") else {
        throw PlannerError.message("self-test markdown missing route or allocation")
    }
    print("self-test ok")
}

func main() -> Int32 {
    do {
        let options = try CliOptions.parse(Array(CommandLine.arguments.dropFirst()))
        if options.selfTest {
            try runSelfTest()
            return 0
        }
        let (output, shouldFail) = try run(options: options)
        print(output)
        return shouldFail ? 2 : 0
    } catch let error as PlannerError {
        FileHandle.standardError.write(Data(("error: \(error.description)\n").utf8))
        return error.description.contains("Usage:") ? 0 : 2
    } catch {
        FileHandle.standardError.write(Data(("error: \(error.localizedDescription)\n").utf8))
        return 2
    }
}

exit(main())

/*
This solves the April 2026 problem of deciding where to spend synthetic AI inference probes before an edge rollout, instead of guessing from a dashboard that mixes good routes with bad ones. Built because real teams now run LLM gateways across regions, providers, prompt caches, carbon budgets, and data residency rules, and one silent weak route can burn latency SLO, customer trust, and cloud spend in the same afternoon. Use it when you have OpenTelemetry JSONL, gateway logs, CDN worker logs, provider invoices, or canary traces and need a Swift command line edge inference routing planner that ranks p95 latency, error rate, cost per 1k tokens, carbon grams per 1k tokens, cache hit rate, cold starts, and missing regional evidence. The trick: it treats low sample count as risk, not as health, then converts the risk into a budgeted probe allocation a release engineer can actually run. Drop this into a repo, compile it with swiftc on macOS or Linux, feed it JSONL from CI, and let the output block weak AI gateway deployments before they become production incidents.
*/