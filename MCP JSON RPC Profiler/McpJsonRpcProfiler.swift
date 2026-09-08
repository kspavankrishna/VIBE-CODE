#!/usr/bin/env swift
import Foundation
#if os(Linux)
import Glibc
#else
import Darwin
#endif

enum AppExit: Int32 {
    case ok = 0
    case budgetFailed = 2
    case usage = 64
    case cannotOpenInput = 66
}

enum OutputFormat: String {
    case text
    case json
    case markdown
}

enum Severity: String {
    case warning
    case error
}

enum EventKind: String {
    case request
    case response
    case notification
    case unknown
}

enum Direction: String {
    case clientToServer = "client_to_server"
    case serverToClient = "server_to_client"
    case unknown
}

enum AppError: Error, CustomStringConvertible {
    case usage(String)
    case invalidJSON(source: String, line: Int, detail: String)
    case cannotOpen(String)

    var description: String {
        switch self {
        case .usage(let message):
            return "\(message)"
        case .invalidJSON(let source, let line, let detail):
            return "\(source):\(line): invalid JSON: \(detail)"
        case .cannotOpen(let path):
            return "cannot open input: \(path)"
        }
    }
}

struct CLIOptions {
    var inputs: [String] = []
    var format: OutputFormat = .text
    var maxP95Ms: Double? = 30_000
    var maxErrorRate: Double? = 0.05
    var methodBudgetsMs: [String: Double] = [:]
    var staleMs: Double = 120_000
    var minSamples: Int = 3
    var failOnOpenRequests: Bool = true
    var showHelp = false

    static func parse(_ args: [String]) throws -> CLIOptions {
        var options = CLIOptions()
        var index = 0

        func requireValue(after option: String, at index: Int) throws -> String {
            guard index + 1 < args.count else {
                throw AppError.usage("missing value after \(option)")
            }
            return args[index + 1]
        }

        while index < args.count {
            let arg = args[index]
            switch arg {
            case "-h", "--help":
                options.showHelp = true
                index += 1
            case "--input", "-i":
                options.inputs.append(try requireValue(after: arg, at: index))
                index += 2
            case "--format":
                let raw = try requireValue(after: arg, at: index)
                guard let format = OutputFormat(rawValue: raw.lowercased()) else {
                    throw AppError.usage("unknown format '\(raw)'; use text, json, or markdown")
                }
                options.format = format
                index += 2
            case "--json":
                options.format = .json
                index += 1
            case "--markdown":
                options.format = .markdown
                index += 1
            case "--max-p95-ms":
                options.maxP95Ms = try parseMilliseconds(try requireValue(after: arg, at: index))
                index += 2
            case "--max-error-rate":
                options.maxErrorRate = try parseRate(try requireValue(after: arg, at: index))
                index += 2
            case "--method-budget":
                let raw = try requireValue(after: arg, at: index)
                let parts = raw.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
                guard parts.count == 2, !parts[0].isEmpty, !parts[1].isEmpty else {
                    throw AppError.usage("method budget must look like tools/call=2500ms")
                }
                options.methodBudgetsMs[String(parts[0])] = try parseMilliseconds(String(parts[1]))
                index += 2
            case "--stale-ms":
                options.staleMs = try parseMilliseconds(try requireValue(after: arg, at: index))
                index += 2
            case "--min-samples":
                let raw = try requireValue(after: arg, at: index)
                guard let parsed = Int(raw), parsed >= 1 else {
                    throw AppError.usage("--min-samples must be a positive integer")
                }
                options.minSamples = parsed
                index += 2
            case "--allow-open":
                options.failOnOpenRequests = false
                index += 1
            case "--no-default-budgets":
                options.maxP95Ms = nil
                options.maxErrorRate = nil
                index += 1
            default:
                if arg.hasPrefix("-") {
                    throw AppError.usage("unknown option: \(arg)")
                }
                options.inputs.append(arg)
                index += 1
            }
        }

        return options
    }

    static func parseMilliseconds(_ text: String) throws -> Double {
        var raw = text.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        var multiplier = 1.0
        if raw.hasSuffix("ms") {
            raw = String(raw.dropLast(2))
        } else if raw.hasSuffix("s") {
            raw = String(raw.dropLast())
            multiplier = 1_000.0
        }
        guard let value = Double(raw), value >= 0 else {
            throw AppError.usage("duration must be a non-negative number, optionally ending in ms or s: \(text)")
        }
        return value * multiplier
    }

    static func parseRate(_ text: String) throws -> Double {
        var raw = text.trimmingCharacters(in: .whitespacesAndNewlines)
        var multiplier = 1.0
        if raw.hasSuffix("%") {
            raw = String(raw.dropLast())
            multiplier = 0.01
        }
        guard let value = Double(raw), value >= 0 else {
            throw AppError.usage("rate must be a non-negative decimal or percent: \(text)")
        }
        let parsed = value * multiplier
        guard parsed <= 1.0 else {
            throw AppError.usage("rate must be <= 1.0 or <= 100%: \(text)")
        }
        return parsed
    }

    static let help = """
    McpJsonRpcProfiler profiles Model Context Protocol JSON-RPC transcripts.

    Usage:
      swift McpJsonRpcProfiler.swift [options] [file.jsonl ...]
      cat mcp-transcript.jsonl | swift McpJsonRpcProfiler.swift --markdown

    Options:
      -i, --input PATH             Read a JSONL transcript file. Use '-' for stdin.
          --format text|json|markdown
          --json                   Emit machine-readable JSON.
          --markdown               Emit a GitHub-ready Markdown report.
          --max-p95-ms VALUE       Global p95 latency budget. Default: 30000ms.
          --max-error-rate VALUE   Global error budget. Accepts 0.05 or 5%. Default: 5%.
          --method-budget M=VALUE  Override p95 budget for one method, for example tools/call=2500ms.
          --stale-ms VALUE         Age for pending request warnings. Default: 120000ms.
          --min-samples N          Minimum completed calls before latency/error budgets apply. Default: 3.
          --allow-open             Do not fail when requests are still open at end of input.
          --no-default-budgets     Disable default p95 and error-rate gates.
      -h, --help                   Show this help.

    Input lines should be JSON objects. The tool accepts plain JSON-RPC messages and common wrappers
    with message, payload, body, timestamp, ts, time, direction, session_id, or trace_id fields.
    """
}

struct Finding {
    let severity: Severity
    let source: String
    let line: Int?
    let method: String?
    let message: String

    func asJSON() -> [String: Any] {
        var object: [String: Any] = [
            "severity": severity.rawValue,
            "source": source,
            "message": message
        ]
        object["line"] = line ?? NSNull()
        object["method"] = method ?? NSNull()
        return object
    }
}

struct PendingRequest {
    let id: String
    let session: String
    let method: String
    let source: String
    let line: Int
    let timestamp: Date?
}

struct JsonRpcEvent {
    let kind: EventKind
    let source: String
    let line: Int
    let timestamp: Date?
    let id: String?
    let method: String?
    let hasError: Bool
    let direction: Direction
    let session: String

    var pendingKey: String? {
        guard let id else { return nil }
        return "\(session)::\(id)"
    }
}

struct MethodStats {
    let method: String
    var requests = 0
    var completed = 0
    var errors = 0
    var notifications = 0
    var missingTimestampPairs = 0
    var negativeClockPairs = 0
    var durationsMs: [Double] = []

    var errorRate: Double {
        guard completed > 0 else { return 0 }
        return Double(errors) / Double(completed)
    }

    var maxMs: Double? {
        durationsMs.max()
    }

    mutating func recordRequest() {
        requests += 1
    }

    mutating func recordNotification() {
        notifications += 1
    }

    mutating func recordResponse(durationMs: Double?, hasError: Bool) {
        completed += 1
        if hasError {
            errors += 1
        }
        if let durationMs {
            durationsMs.append(durationMs)
        } else {
            missingTimestampPairs += 1
        }
    }

    mutating func recordNegativeClock() {
        negativeClockPairs += 1
    }

    func percentile(_ percentile: Double) -> Double? {
        guard !durationsMs.isEmpty else { return nil }
        let sorted = durationsMs.sorted()
        if sorted.count == 1 { return sorted[0] }
        let rank = (percentile / 100.0) * Double(sorted.count - 1)
        let lower = Int(floor(rank))
        let upper = Int(ceil(rank))
        if lower == upper { return sorted[lower] }
        let fraction = rank - Double(lower)
        return sorted[lower] + (sorted[upper] - sorted[lower]) * fraction
    }

    func asJSON() -> [String: Any] {
        var object: [String: Any] = [
            "method": method,
            "requests": requests,
            "completed": completed,
            "errors": errors,
            "error_rate": errorRate,
            "notifications": notifications,
            "missing_timestamp_pairs": missingTimestampPairs,
            "negative_clock_pairs": negativeClockPairs
        ]
        object["p50_ms"] = percentile(50) ?? NSNull()
        object["p95_ms"] = percentile(95) ?? NSNull()
        object["p99_ms"] = percentile(99) ?? NSNull()
        object["max_ms"] = maxMs ?? NSNull()
        return object
    }
}

final class TimestampParser {
    private let isoWithFraction = ISO8601DateFormatter()
    private let isoWithoutFraction = ISO8601DateFormatter()
    private let timestampKeys = ["timestamp", "ts", "time", "@timestamp", "observed_at", "created_at"]

    init() {
        isoWithFraction.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        isoWithoutFraction.formatOptions = [.withInternetDateTime]
    }

    func timestamp(in object: [String: Any]) -> Date? {
        for key in timestampKeys {
            if let parsed = parse(object[key]) {
                return parsed
            }
        }
        if let milliseconds = number(object["ts_ms"]) {
            return Date(timeIntervalSince1970: milliseconds / 1_000.0)
        }
        if let nanoseconds = number(object["ts_ns"]) {
            return Date(timeIntervalSince1970: nanoseconds / 1_000_000_000.0)
        }
        return nil
    }

    private func parse(_ value: Any?) -> Date? {
        guard let value else { return nil }
        if let date = value as? Date { return date }
        if let number = number(value) {
            if number > 10_000_000_000 {
                return Date(timeIntervalSince1970: number / 1_000.0)
            }
            return Date(timeIntervalSince1970: number)
        }
        guard let text = value as? String else { return nil }
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        if let numeric = Double(trimmed) {
            if numeric > 10_000_000_000 {
                return Date(timeIntervalSince1970: numeric / 1_000.0)
            }
            return Date(timeIntervalSince1970: numeric)
        }
        return isoWithFraction.date(from: trimmed) ?? isoWithoutFraction.date(from: trimmed)
    }

    private func number(_ value: Any?) -> Double? {
        if let value = value as? Double { return value }
        if let value = value as? Int { return Double(value) }
        if let value = value as? NSNumber { return value.doubleValue }
        return nil
    }
}

final class EventExtractor {
    private let timestampParser = TimestampParser()
    private let nestedMessageKeys = ["message", "payload", "body", "event", "jsonrpc_message", "rpc"]
    private let sessionKeys = ["session", "session_id", "connection", "connection_id", "transport_id", "trace_id", "run_id"]

    func extract(line: String, lineNumber: Int, source: String) throws -> JsonRpcEvent? {
        let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        guard let data = trimmed.data(using: .utf8) else {
            throw AppError.invalidJSON(source: source, line: lineNumber, detail: "line is not UTF-8")
        }

        let parsed: Any
        do {
            parsed = try JSONSerialization.jsonObject(with: data, options: [])
        } catch {
            throw AppError.invalidJSON(source: source, line: lineNumber, detail: error.localizedDescription)
        }

        guard let envelope = parsed as? [String: Any] else {
            throw AppError.invalidJSON(source: source, line: lineNumber, detail: "top-level value is not an object")
        }

        let message = unwrapMessage(from: envelope) ?? envelope
        let kind = classify(message)
        if kind == .unknown && !looksLikeJsonRpc(message) {
            return JsonRpcEvent(
                kind: .unknown,
                source: source,
                line: lineNumber,
                timestamp: timestampParser.timestamp(in: message) ?? timestampParser.timestamp(in: envelope),
                id: normalizeID(message["id"]),
                method: message["method"] as? String,
                hasError: message["error"] != nil,
                direction: direction(from: envelope, message: message),
                session: session(from: envelope, message: message)
            )
        }

        return JsonRpcEvent(
            kind: kind,
            source: source,
            line: lineNumber,
            timestamp: timestampParser.timestamp(in: message) ?? timestampParser.timestamp(in: envelope),
            id: normalizeID(message["id"]),
            method: message["method"] as? String,
            hasError: message["error"] != nil,
            direction: direction(from: envelope, message: message),
            session: session(from: envelope, message: message)
        )
    }

    private func unwrapMessage(from envelope: [String: Any]) -> [String: Any]? {
        if looksLikeJsonRpc(envelope) {
            return envelope
        }
        for key in nestedMessageKeys {
            if let nested = envelope[key] as? [String: Any], looksLikeJsonRpc(nested) {
                return nested
            }
            if let text = envelope[key] as? String,
               let nested = parseNestedJSONString(text),
               looksLikeJsonRpc(nested) {
                return nested
            }
        }
        return nil
    }

    private func parseNestedJSONString(_ text: String) -> [String: Any]? {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.first == "{" else { return nil }
        guard let data = trimmed.data(using: .utf8) else { return nil }
        return (try? JSONSerialization.jsonObject(with: data, options: [])) as? [String: Any]
    }

    private func classify(_ message: [String: Any]) -> EventKind {
        let method = message["method"] as? String
        let id = normalizeID(message["id"])
        if method != nil && id != nil { return .request }
        if method != nil { return .notification }
        if id != nil && (message.keys.contains("result") || message.keys.contains("error")) { return .response }
        return .unknown
    }

    private func looksLikeJsonRpc(_ message: [String: Any]) -> Bool {
        if message["jsonrpc"] != nil { return true }
        if message["method"] != nil { return true }
        if message["id"] != nil && (message["result"] != nil || message["error"] != nil) { return true }
        return false
    }

    private func direction(from envelope: [String: Any], message: [String: Any]) -> Direction {
        let raw = firstString(keys: ["direction", "dir", "side", "flow"], in: message)
            ?? firstString(keys: ["direction", "dir", "side", "flow"], in: envelope)
            ?? ""
        let lowered = raw.lowercased()
        if lowered.contains("client") && lowered.contains("server") && !lowered.contains("server_to_client") {
            return .clientToServer
        }
        if lowered.contains("server_to_client") || lowered.contains("server->client") || lowered == "in" || lowered == "incoming" {
            return .serverToClient
        }
        if lowered == "out" || lowered == "outgoing" || lowered.contains("client_to_server") || lowered.contains("client->server") {
            return .clientToServer
        }
        return .unknown
    }

    private func session(from envelope: [String: Any], message: [String: Any]) -> String {
        firstString(keys: sessionKeys, in: message) ?? firstString(keys: sessionKeys, in: envelope) ?? "default"
    }

    private func firstString(keys: [String], in object: [String: Any]) -> String? {
        for key in keys {
            if let value = object[key] as? String, !value.isEmpty { return value }
            if let value = object[key] as? NSNumber { return value.stringValue }
        }
        return nil
    }

    private func normalizeID(_ value: Any?) -> String? {
        guard let value else { return nil }
        if value is NSNull { return nil }
        if let string = value as? String { return string }
        if let number = value as? NSNumber { return number.stringValue }
        return String(describing: value)
    }
}

final class LineReader {
    private let handle: FileHandle
    private var buffer = Data()
    private var reachedEOF = false
    private let chunkSize = 64 * 1024

    init(handle: FileHandle) {
        self.handle = handle
    }

    func nextLine() throws -> String? {
        while true {
            if let newline = buffer.firstIndex(of: 10) {
                let lineData = Data(buffer[..<newline])
                buffer.removeSubrange(buffer.startIndex...newline)
                var line = String(decoding: lineData, as: UTF8.self)
                if line.hasSuffix("\r") { line.removeLast() }
                return line
            }

            if reachedEOF {
                guard !buffer.isEmpty else { return nil }
                let lineData = buffer
                buffer.removeAll(keepingCapacity: false)
                var line = String(decoding: lineData, as: UTF8.self)
                if line.hasSuffix("\r") { line.removeLast() }
                return line
            }

            let chunk = try handle.read(upToCount: chunkSize) ?? Data()
            if chunk.isEmpty {
                reachedEOF = true
            } else {
                buffer.append(chunk)
            }
        }
    }
}

final class Profiler {
    private let options: CLIOptions
    private let extractor = EventExtractor()
    private var stats: [String: MethodStats] = [:]
    private var pending: [String: PendingRequest] = [:]
    private var findings: [Finding] = []
    private var totalLines = 0
    private var parsedEvents = 0
    private var unknownEvents = 0
    private var earliestTimestamp: Date?
    private var latestTimestamp: Date?
    private var finalized = false

    init(options: CLIOptions) {
        self.options = options
    }

    func consume(handle: FileHandle, source: String) throws {
        let reader = LineReader(handle: handle)
        var lineNumber = 0
        while let line = try reader.nextLine() {
            lineNumber += 1
            totalLines += 1
            if let event = try extractor.extract(line: line, lineNumber: lineNumber, source: source) {
                process(event)
            }
        }
    }

    func finalize() -> Bool {
        guard !finalized else { return !findings.contains { $0.severity == .error } }
        finalized = true
        checkOpenRequests()
        checkBudgets()
        return !findings.contains { $0.severity == .error }
    }

    func render() -> String {
        switch options.format {
        case .text:
            return renderText()
        case .json:
            return renderJSON()
        case .markdown:
            return renderMarkdown()
        }
    }

    private func process(_ event: JsonRpcEvent) {
        parsedEvents += 1
        if let timestamp = event.timestamp {
            if earliestTimestamp == nil || timestamp < earliestTimestamp! { earliestTimestamp = timestamp }
            if latestTimestamp == nil || timestamp > latestTimestamp! { latestTimestamp = timestamp }
        }

        switch event.kind {
        case .request:
            processRequest(event)
        case .response:
            processResponse(event)
        case .notification:
            mutateStats(method: event.method ?? "<unknown-notification>") { $0.recordNotification() }
        case .unknown:
            unknownEvents += 1
        }
    }

    private func processRequest(_ event: JsonRpcEvent) {
        let method = event.method ?? "<unknown-method>"
        mutateStats(method: method) { $0.recordRequest() }
        guard let key = event.pendingKey, let id = event.id else {
            findings.append(Finding(severity: .warning, source: event.source, line: event.line, method: method, message: "request has no JSON-RPC id, so it cannot be matched to a response"))
            return
        }
        if let older = pending[key] {
            findings.append(Finding(severity: .error, source: event.source, line: event.line, method: method, message: "request id \(id) was reused before the previous \(older.method) request from \(older.source):\(older.line) completed"))
        }
        pending[key] = PendingRequest(id: id, session: event.session, method: method, source: event.source, line: event.line, timestamp: event.timestamp)
    }

    private func processResponse(_ event: JsonRpcEvent) {
        guard let key = event.pendingKey, let id = event.id else {
            findings.append(Finding(severity: .error, source: event.source, line: event.line, method: nil, message: "response is missing a JSON-RPC id"))
            return
        }
        guard let request = pending.removeValue(forKey: key) else {
            findings.append(Finding(severity: .error, source: event.source, line: event.line, method: nil, message: "response id \(id) has no matching open request in session \(event.session)"))
            return
        }

        let duration = durationMs(from: request, to: event)
        mutateStats(method: request.method) { methodStats in
            if duration == nil, request.timestamp != nil, event.timestamp != nil {
                methodStats.recordNegativeClock()
            }
            methodStats.recordResponse(durationMs: duration, hasError: event.hasError)
        }
    }

    private func durationMs(from request: PendingRequest, to event: JsonRpcEvent) -> Double? {
        guard let start = request.timestamp, let end = event.timestamp else { return nil }
        let duration = end.timeIntervalSince(start) * 1_000.0
        if duration < 0 {
            findings.append(Finding(severity: .warning, source: event.source, line: event.line, method: request.method, message: "response timestamp is earlier than request timestamp for id \(request.id)"))
            return nil
        }
        return duration
    }

    private func checkOpenRequests() {
        for request in pending.values.sorted(by: { $0.line < $1.line }) {
            var severity: Severity = options.failOnOpenRequests ? .error : .warning
            var message = "request id \(request.id) for \(request.method) did not receive a response before end of input"
            if let latestTimestamp, let started = request.timestamp {
                let ageMs = latestTimestamp.timeIntervalSince(started) * 1_000.0
                if ageMs < options.staleMs && !options.failOnOpenRequests {
                    severity = .warning
                }
                message += " (open for \(formatMs(ageMs)))"
            }
            findings.append(Finding(severity: severity, source: request.source, line: request.line, method: request.method, message: message))
        }
    }

    private func checkBudgets() {
        for method in orderedMethods() {
            guard let methodStats = stats[method] else { continue }
            let p95Budget = options.methodBudgetsMs[method] ?? options.maxP95Ms
            if let budget = p95Budget,
               methodStats.completed >= options.minSamples,
               let p95 = methodStats.percentile(95),
               p95 > budget {
                findings.append(Finding(severity: .error, source: "budget", line: nil, method: method, message: "p95 latency \(formatMs(p95)) exceeds budget \(formatMs(budget)) after \(methodStats.completed) completed calls"))
            }
            if let maxErrorRate = options.maxErrorRate,
               methodStats.completed >= options.minSamples,
               methodStats.errorRate > maxErrorRate {
                findings.append(Finding(severity: .error, source: "budget", line: nil, method: method, message: "error rate \(formatPercent(methodStats.errorRate)) exceeds budget \(formatPercent(maxErrorRate)) after \(methodStats.completed) completed calls"))
            }
        }
    }

    private func mutateStats(method: String, _ mutate: (inout MethodStats) -> Void) {
        var current = stats[method] ?? MethodStats(method: method)
        mutate(&current)
        stats[method] = current
    }

    private func orderedMethods() -> [String] {
        stats.keys.sorted { left, right in
            let leftP95 = stats[left]?.percentile(95) ?? -1
            let rightP95 = stats[right]?.percentile(95) ?? -1
            if leftP95 == rightP95 { return left < right }
            return leftP95 > rightP95
        }
    }

    private func renderText() -> String {
        var lines: [String] = []
        lines.append("MCP JSON-RPC profile")
        lines.append("lines=\(totalLines) events=\(parsedEvents) unknown=\(unknownEvents) open=\(pending.count) status=\(findings.contains { $0.severity == .error } ? "fail" : "pass")")
        lines.append("")
        lines.append(renderTable())
        if !findings.isEmpty {
            lines.append("")
            lines.append("Findings:")
            for finding in findings {
                lines.append("[\(finding.severity.rawValue)] \(location(finding)): \(finding.message)")
            }
        }
        return lines.joined(separator: "\n")
    }

    private func renderMarkdown() -> String {
        var lines: [String] = []
        lines.append("# MCP JSON-RPC profile")
        lines.append("")
        lines.append("- Lines scanned: \(totalLines)")
        lines.append("- JSON-RPC events: \(parsedEvents)")
        lines.append("- Unknown JSON events: \(unknownEvents)")
        lines.append("- Open requests: \(pending.count)")
        lines.append("- Status: \(findings.contains { $0.severity == .error } ? "fail" : "pass")")
        lines.append("")
        lines.append("| Method | Requests | Completed | Errors | Error rate | Notifications | P50 | P95 | P99 | Max | Missing timestamps |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for method in orderedMethods() {
            guard let s = stats[method] else { continue }
            lines.append("| \(escapeMarkdown(method)) | \(s.requests) | \(s.completed) | \(s.errors) | \(formatPercent(s.errorRate)) | \(s.notifications) | \(formatMs(s.percentile(50))) | \(formatMs(s.percentile(95))) | \(formatMs(s.percentile(99))) | \(formatMs(s.maxMs)) | \(s.missingTimestampPairs) |")
        }
        if !findings.isEmpty {
            lines.append("")
            lines.append("## Findings")
            for finding in findings {
                lines.append("- **\(finding.severity.rawValue)** `\(location(finding))`: \(escapeMarkdown(finding.message))")
            }
        }
        return lines.joined(separator: "\n")
    }

    private func renderJSON() -> String {
        let methods = orderedMethods().compactMap { stats[$0]?.asJSON() }
        let object: [String: Any] = [
            "summary": [
                "lines": totalLines,
                "events": parsedEvents,
                "unknown_events": unknownEvents,
                "open_requests": pending.count,
                "status": findings.contains { $0.severity == .error } ? "fail" : "pass",
                "earliest_timestamp": jsonTimestamp(earliestTimestamp),
                "latest_timestamp": jsonTimestamp(latestTimestamp)
            ],
            "methods": methods,
            "findings": findings.map { $0.asJSON() }
        ]
        guard JSONSerialization.isValidJSONObject(object),
              let data = try? JSONSerialization.data(withJSONObject: object, options: [.prettyPrinted, .sortedKeys]) else {
            return "{\"error\":\"failed to render JSON report\"}"
        }
        return String(decoding: data, as: UTF8.self)
    }

    private func renderTable() -> String {
        var rows: [[String]] = [["Method", "Req", "Done", "Err", "Err%", "Notif", "P50", "P95", "P99", "Max", "NoTS"]]
        for method in orderedMethods() {
            guard let s = stats[method] else { continue }
            rows.append([
                method,
                String(s.requests),
                String(s.completed),
                String(s.errors),
                formatPercent(s.errorRate),
                String(s.notifications),
                formatMs(s.percentile(50)),
                formatMs(s.percentile(95)),
                formatMs(s.percentile(99)),
                formatMs(s.maxMs),
                String(s.missingTimestampPairs)
            ])
        }
        return TextTable.render(rows: rows)
    }

    private func location(_ finding: Finding) -> String {
        if let line = finding.line {
            return "\(finding.source):\(line)"
        }
        return finding.source
    }

    private func isoString(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: date)
    }

    private func jsonTimestamp(_ date: Date?) -> Any {
        guard let date = date else { return NSNull() }
        return isoString(date)
    }
}

struct TextTable {
    static func render(rows: [[String]]) -> String {
        guard let first = rows.first else { return "" }
        var widths = Array(repeating: 0, count: first.count)
        for row in rows {
            for (index, value) in row.enumerated() {
                widths[index] = max(widths[index], value.count)
            }
        }
        return rows.map { row in
            row.enumerated().map { index, value in
                pad(value, to: widths[index])
            }.joined(separator: "  ")
        }.joined(separator: "\n")
    }

    private static func pad(_ value: String, to width: Int) -> String {
        if value.count >= width { return value }
        return value + String(repeating: " ", count: width - value.count)
    }
}

func formatMs(_ value: Double?) -> String {
    guard let value else { return "-" }
    if value < 1_000 {
        return String(format: "%.0fms", value)
    }
    return String(format: "%.2fs", value / 1_000.0)
}

func formatPercent(_ value: Double) -> String {
    String(format: "%.2f%%", value * 100.0)
}

func escapeMarkdown(_ text: String) -> String {
    text.replacingOccurrences(of: "|", with: "\\|")
}

func printError(_ text: String) {
    FileHandle.standardError.write(Data((text + "\n").utf8))
}

func openInput(_ path: String) throws -> FileHandle {
    if path == "-" { return FileHandle.standardInput }
    guard let handle = FileHandle(forReadingAtPath: path) else {
        throw AppError.cannotOpen(path)
    }
    return handle
}

func run() -> Int32 {
    do {
        let options = try CLIOptions.parse(Array(CommandLine.arguments.dropFirst()))
        if options.showHelp {
            print(CLIOptions.help)
            return AppExit.ok.rawValue
        }

        let profiler = Profiler(options: options)
        if options.inputs.isEmpty {
            try profiler.consume(handle: FileHandle.standardInput, source: "stdin")
        } else {
            for path in options.inputs {
                let handle = try openInput(path)
                try profiler.consume(handle: handle, source: path)
                if path != "-" { try? handle.close() }
            }
        }

        let passed = profiler.finalize()
        print(profiler.render())
        return passed ? AppExit.ok.rawValue : AppExit.budgetFailed.rawValue
    } catch let error as AppError {
        printError("McpJsonRpcProfiler: \(error.description)")
        switch error {
        case .cannotOpen:
            return AppExit.cannotOpenInput.rawValue
        default:
            return AppExit.usage.rawValue
        }
    } catch {
        printError("McpJsonRpcProfiler: \(error.localizedDescription)")
        return AppExit.usage.rawValue
    }
}

exit(run())

/*
This solves the annoying April 2026 problem where MCP servers, coding agents, local gateways, and editor plugins look fine in a demo but silently burn minutes on slow tools, reused JSON-RPC ids, missing responses, and flaky tool errors once they hit real developer workflows. Built because OpenAI Codex, Claude Code, Cursor, OpenCode, GitHub Actions, and internal agent runners all need a boring, repeatable MCP JSON-RPC profiler that can read raw JSONL transcripts and fail CI before a bad tool server wastes an engineer's day. Use it when you have Model Context Protocol logs from stdio, WebSocket bridges, edge workers, desktop IDE extensions, or AI gateway traces and you need p50, p95, p99, error rate, stuck request, duplicate id, missing timestamp, and method budget checks without sending private traces to a SaaS dashboard. The trick: it treats the transcript as a ledger of request and response ids, keeps sessions separate when the log gives a session_id or trace_id, and lets every team set method-specific latency budgets like tools/call=2500ms while still producing text, Markdown, or JSON for pull requests and observability pipelines. Drop this into a repository as a single Swift source file, run it with swift McpJsonRpcProfiler.swift --markdown mcp.jsonl, and you get a practical MCP latency profiler, MCP reliability gate, JSON-RPC transcript analyzer, AI agent tool-call audit, and CI/CD regression check that a senior engineer can fork, read, and trust.
*/
