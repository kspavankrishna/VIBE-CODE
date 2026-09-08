const std = @import("std");

const Allocator = std.mem.Allocator;
const max_input_bytes: usize = 64 * 1024 * 1024;

const Severity = enum(u8) {
    info = 0,
    warn = 1,
    high = 2,
    critical = 3,

    fn label(self: Severity) []const u8 {
        return switch (self) {
            .info => "info",
            .warn => "warn",
            .high => "high",
            .critical => "critical",
        };
    }

    fn fromText(text: []const u8) Severity {
        if (asciiEq(text, "critical")) return .critical;
        if (asciiEq(text, "high")) return .high;
        if (asciiEq(text, "warn") or asciiEq(text, "warning")) return .warn;
        return .info;
    }
};

const Limit = struct {
    tenant: []const u8,
    micros: u64,
};

const Config = struct {
    input_path: ?[]const u8 = null,
    format: []const u8 = "summary",
    fail_on: Severity = .critical,
    default_daily_micros: u64 = 250_000_000,
    max_context_tokens: u64 = 128_000,
    max_stream_seconds: u64 = 90,
    max_latency_ms: u64 = 15_000,
    max_queue_ms: u64 = 30_000,
    max_tool_calls: u64 = 16,
    allowed_regions: []const u8 = "us,eu,apac,global",
    tenant_limits: std.ArrayList(Limit),

    fn init(allocator: Allocator) Config {
        return .{ .tenant_limits = std.ArrayList(Limit).init(allocator) };
    }

    fn deinit(self: *Config) void {
        self.tenant_limits.deinit();
    }

    fn limitFor(self: Config, tenant: []const u8) u64 {
        for (self.tenant_limits.items) |item| {
            if (std.mem.eql(u8, item.tenant, tenant)) return item.micros;
        }
        return self.default_daily_micros;
    }
};

const Event = struct {
    line_no: usize,
    tenant: []const u8 = "unknown",
    provider: []const u8 = "unknown",
    model: []const u8 = "unknown",
    region: []const u8 = "unknown",
    request_key: []const u8 = "",
    trace_id: []const u8 = "",
    phase: []const u8 = "",
    status: []const u8 = "",
    tokens_in: u64 = 0,
    tokens_out: u64 = 0,
    context_tokens: u64 = 0,
    cost_micros: u64 = 0,
    latency_ms: u64 = 0,
    queue_ms: u64 = 0,
    stream_seconds: u64 = 0,
    chunk_count: u64 = 0,
    retry_after_ms: u64 = 0,
    tool_calls: u64 = 0,
    cache_hit: bool = false,
    secret_hit: bool = false,
};

const TenantStats = struct {
    name: []const u8,
    requests: u64 = 0,
    failures: u64 = 0,
    total_cost_micros: u64 = 0,
    tokens_in: u64 = 0,
    tokens_out: u64 = 0,
    max_context_tokens: u64 = 0,
    max_latency_ms: u64 = 0,
    max_queue_ms: u64 = 0,
    max_stream_seconds: u64 = 0,
    cache_hits: u64 = 0,
    tool_calls: u64 = 0,
};

const RequestSeen = struct {
    key: []const u8,
    tenant: []const u8,
    trace_id: []const u8,
    status: []const u8,
    first_line: usize,
    count: u64,
};

const Finding = struct {
    sev: Severity,
    line_no: usize,
    tenant: []const u8,
    rule: []const u8,
    message: []const u8,
    value: u64,
    limit: u64,
};

const Analysis = struct {
    allocator: Allocator,
    tenants: std.ArrayList(TenantStats),
    findings: std.ArrayList(Finding),
    seen: std.ArrayList(RequestSeen),
    total_lines: usize = 0,
    parsed_events: usize = 0,

    fn init(allocator: Allocator) Analysis {
        return .{
            .allocator = allocator,
            .tenants = std.ArrayList(TenantStats).init(allocator),
            .findings = std.ArrayList(Finding).init(allocator),
            .seen = std.ArrayList(RequestSeen).init(allocator),
        };
    }

    fn deinit(self: *Analysis) void {
        for (self.findings.items) |item| self.allocator.free(item.message);
        self.tenants.deinit();
        self.findings.deinit();
        self.seen.deinit();
    }

    fn addFinding(
        self: *Analysis,
        sev: Severity,
        line_no: usize,
        tenant: []const u8,
        rule: []const u8,
        message: []const u8,
        value: u64,
        limit: u64,
    ) !void {
        try self.findings.append(.{
            .sev = sev,
            .line_no = line_no,
            .tenant = tenant,
            .rule = rule,
            .message = try self.allocator.dupe(u8, message),
            .value = value,
            .limit = limit,
        });
    }

    fn tenantIndex(self: *Analysis, name: []const u8) !usize {
        for (self.tenants.items, 0..) |tenant, idx| {
            if (std.mem.eql(u8, tenant.name, name)) return idx;
        }
        try self.tenants.append(.{ .name = name });
        return self.tenants.items.len - 1;
    }

    fn rememberRequest(self: *Analysis, event: Event) !void {
        if (event.request_key.len == 0) return;
        for (self.seen.items, 0..) |seen, idx| {
            if (std.mem.eql(u8, seen.key, event.request_key) and std.mem.eql(u8, seen.tenant, event.tenant)) {
                self.seen.items[idx].count += 1;
                if (!std.mem.eql(u8, seen.trace_id, event.trace_id) or !std.mem.eql(u8, seen.status, event.status)) {
                    try self.addFinding(
                        .critical,
                        event.line_no,
                        event.tenant,
                        "replay-key-drift",
                        "same tenant reused an idempotency key with a different trace or status",
                        self.seen.items[idx].count,
                        1,
                    );
                } else if (self.seen.items[idx].count > 2) {
                    try self.addFinding(
                        .high,
                        event.line_no,
                        event.tenant,
                        "replay-key-repeat",
                        "request key repeated more than twice and should be fenced before replay billing",
                        self.seen.items[idx].count,
                        2,
                    );
                }
                return;
            }
        }
        try self.seen.append(.{
            .key = event.request_key,
            .tenant = event.tenant,
            .trace_id = event.trace_id,
            .status = event.status,
            .first_line = event.line_no,
            .count = 1,
        });
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var config = Config.init(allocator);
    defer config.deinit();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--self-test")) {
            try runSelfTest(allocator);
            return;
        } else if (std.mem.eql(u8, arg, "--input")) {
            config.input_path = args.next() orelse return usageAndFail("missing value for --input");
        } else if (std.mem.eql(u8, arg, "--format")) {
            config.format = args.next() orelse return usageAndFail("missing value for --format");
        } else if (std.mem.eql(u8, arg, "--fail-on")) {
            config.fail_on = Severity.fromText(args.next() orelse return usageAndFail("missing value for --fail-on"));
        } else if (std.mem.eql(u8, arg, "--default-daily-usd")) {
            config.default_daily_micros = parseMoneyMicros(args.next() orelse return usageAndFail("missing value for --default-daily-usd")) orelse return usageAndFail("bad money value");
        } else if (std.mem.eql(u8, arg, "--tenant-limit-usd")) {
            const raw = args.next() orelse return usageAndFail("missing value for --tenant-limit-usd");
            try addTenantLimit(&config, raw);
        } else if (std.mem.eql(u8, arg, "--max-context-tokens")) {
            config.max_context_tokens = parseU64(args.next() orelse return usageAndFail("missing value for --max-context-tokens")) orelse return usageAndFail("bad integer");
        } else if (std.mem.eql(u8, arg, "--max-stream-seconds")) {
            config.max_stream_seconds = parseU64(args.next() orelse return usageAndFail("missing value for --max-stream-seconds")) orelse return usageAndFail("bad integer");
        } else if (std.mem.eql(u8, arg, "--max-latency-ms")) {
            config.max_latency_ms = parseU64(args.next() orelse return usageAndFail("missing value for --max-latency-ms")) orelse return usageAndFail("bad integer");
        } else if (std.mem.eql(u8, arg, "--max-queue-ms")) {
            config.max_queue_ms = parseU64(args.next() orelse return usageAndFail("missing value for --max-queue-ms")) orelse return usageAndFail("bad integer");
        } else if (std.mem.eql(u8, arg, "--max-tool-calls")) {
            config.max_tool_calls = parseU64(args.next() orelse return usageAndFail("missing value for --max-tool-calls")) orelse return usageAndFail("bad integer");
        } else if (std.mem.eql(u8, arg, "--allowed-regions")) {
            config.allowed_regions = args.next() orelse return usageAndFail("missing value for --allowed-regions");
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            try printUsage(std.io.getStdOut().writer());
            return;
        } else {
            config.input_path = arg;
        }
    }

    const input = try readInput(allocator, config.input_path);
    defer allocator.free(input);

    var analysis = try analyze(allocator, input, config);
    defer analysis.deinit();

    try render(std.io.getStdOut().writer(), analysis, config);
    if (hasSeverityAtLeast(analysis, config.fail_on)) std.process.exit(2);
}

fn usageAndFail(message: []const u8) !void {
    const err = std.io.getStdErr().writer();
    try err.print("error: {s}\n\n", .{message});
    try printUsage(err);
    std.process.exit(64);
}

fn printUsage(writer: anytype) !void {
    try writer.writeAll(
        \\TenantStreamBudgetLedger.zig audits AI gateway and edge inference JSONL logs.
        \\
        \\Usage:
        \\  zig run TenantStreamBudgetLedger.zig -- --input gateway.jsonl --format markdown
        \\  zig run TenantStreamBudgetLedger.zig -- --self-test
        \\
        \\Inputs may be JSONL or key=value logs. Useful fields:
        \\  tenant provider model region request_id idempotency_key trace_id status phase
        \\  tokens_in tokens_out context_tokens cost_usd latency_ms queue_ms
        \\  stream_seconds chunk_count retry_after_ms cache_hit tool_calls secret_hit
        \\
        \\Policy flags:
        \\  --default-daily-usd 250.00
        \\  --tenant-limit-usd tenant=125.00
        \\  --max-context-tokens 128000
        \\  --max-stream-seconds 90
        \\  --max-latency-ms 15000
        \\  --max-queue-ms 30000
        \\  --max-tool-calls 16
        \\  --allowed-regions us,eu,apac,global
        \\  --fail-on info|warn|high|critical
        \\  --format summary|json|markdown|sarif
        \\
    );
}

fn readInput(allocator: Allocator, path: ?[]const u8) ![]u8 {
    if (path) |p| return std.fs.cwd().readFileAlloc(allocator, p, max_input_bytes);
    var list = std.ArrayList(u8).init(allocator);
    errdefer list.deinit();
    try std.io.getStdIn().reader().readAllArrayList(&list, max_input_bytes);
    return list.toOwnedSlice();
}

fn analyze(allocator: Allocator, input: []const u8, config: Config) !Analysis {
    var analysis = Analysis.init(allocator);
    var lines = std.mem.splitScalar(u8, input, '\n');
    var line_no: usize = 0;
    while (lines.next()) |raw| {
        line_no += 1;
        analysis.total_lines += 1;
        const line = trim(raw);
        if (line.len == 0 or line[0] == '#') continue;
        const event = parseEvent(line, line_no);
        analysis.parsed_events += 1;
        try updateStats(&analysis, event);
        try analysis.rememberRequest(event);
        try applyEventRules(&analysis, event, config);
    }
    try applyTenantRules(&analysis, config);
    sortFindings(&analysis);
    return analysis;
}

fn updateStats(analysis: *Analysis, event: Event) !void {
    const idx = try analysis.tenantIndex(event.tenant);
    var tenant = &analysis.tenants.items[idx];
    tenant.requests += 1;
    tenant.total_cost_micros += event.cost_micros;
    tenant.tokens_in += event.tokens_in;
    tenant.tokens_out += event.tokens_out;
    tenant.max_context_tokens = @max(tenant.max_context_tokens, event.context_tokens);
    tenant.max_latency_ms = @max(tenant.max_latency_ms, event.latency_ms);
    tenant.max_queue_ms = @max(tenant.max_queue_ms, event.queue_ms);
    tenant.max_stream_seconds = @max(tenant.max_stream_seconds, event.stream_seconds);
    tenant.tool_calls += event.tool_calls;
    if (event.cache_hit) tenant.cache_hits += 1;
    if (isFailure(event.status)) tenant.failures += 1;
}

fn applyEventRules(analysis: *Analysis, event: Event, config: Config) !void {
    if (event.secret_hit or containsSecretSignal(event.status) or containsSecretSignal(event.phase)) {
        try analysis.addFinding(.critical, event.line_no, event.tenant, "secret-egress", "event says a secret-like value reached a prompt, tool, or stream", 1, 0);
    }
    if (!regionAllowed(config.allowed_regions, event.region)) {
        try analysis.addFinding(.high, event.line_no, event.tenant, "region-residency", "event region is outside the allowed inference data residency set", 1, 0);
    }
    if (event.context_tokens > config.max_context_tokens) {
        try analysis.addFinding(.critical, event.line_no, event.tenant, "context-window", "request exceeded the configured model context window and may truncate hidden policy or retrieval evidence", event.context_tokens, config.max_context_tokens);
    }
    if (event.stream_seconds > config.max_stream_seconds) {
        try analysis.addFinding(.high, event.line_no, event.tenant, "stream-duration", "stream ran longer than the operational envelope and can pin edge workers or client sockets", event.stream_seconds, config.max_stream_seconds);
    }
    if (event.latency_ms > config.max_latency_ms) {
        try analysis.addFinding(.warn, event.line_no, event.tenant, "latency-slo", "request breached the latency envelope used for human-in-the-loop agent workflows", event.latency_ms, config.max_latency_ms);
    }
    if (event.queue_ms > config.max_queue_ms) {
        try analysis.addFinding(.high, event.line_no, event.tenant, "queue-saturation", "queue wait suggests admission control or tenant shaping is failing before inference starts", event.queue_ms, config.max_queue_ms);
    }
    if (event.retry_after_ms > 60_000) {
        try analysis.addFinding(.warn, event.line_no, event.tenant, "provider-throttle", "provider returned a long retry-after value; route capacity should be drained or hedged", event.retry_after_ms, 60_000);
    }
    if (event.tool_calls > config.max_tool_calls) {
        try analysis.addFinding(.warn, event.line_no, event.tenant, "tool-call-fanout", "tool call fanout is high enough to amplify spend, replay risk, and external side effects", event.tool_calls, config.max_tool_calls);
    }
    if (!event.cache_hit and event.context_tokens > 64_000 and event.cost_micros > 50_000) {
        try analysis.addFinding(.warn, event.line_no, event.tenant, "prompt-cache-miss", "large paid context missed cache and should be inspected before budget alerts become noisy", event.context_tokens, 64_000);
    }
    if (isFailure(event.status)) {
        try analysis.addFinding(.warn, event.line_no, event.tenant, "failed-request", "request ended in a failed or throttled status while still consuming tenant capacity", 1, 0);
    }
}

fn applyTenantRules(analysis: *Analysis, config: Config) !void {
    for (analysis.tenants.items) |tenant| {
        const budget = config.limitFor(tenant.name);
        if (tenant.total_cost_micros > budget) {
            try analysis.addFinding(.critical, 0, tenant.name, "tenant-budget", "tenant spend crossed the daily budget after normalizing costs to integer micros", tenant.total_cost_micros, budget);
        } else if (tenant.total_cost_micros * 100 >= budget * 85) {
            try analysis.addFinding(.high, 0, tenant.name, "tenant-budget-near", "tenant spend is above 85 percent of the daily budget and needs admission shaping", tenant.total_cost_micros, budget);
        }
        if (tenant.requests >= 20 and tenant.cache_hits * 100 < tenant.requests * 25 and tenant.tokens_in > 500_000) {
            try analysis.addFinding(.warn, 0, tenant.name, "tenant-cache-efficiency", "tenant has heavy prompt input with weak cache efficiency across many requests", tenant.cache_hits, tenant.requests);
        }
        if (tenant.failures * 100 >= tenant.requests * 20 and tenant.requests >= 10) {
            try analysis.addFinding(.high, 0, tenant.name, "tenant-failure-rate", "tenant failure rate is high enough to waste provider quota and hide real user demand", tenant.failures, tenant.requests);
        }
    }
}

fn parseEvent(line: []const u8, line_no: usize) Event {
    var event = Event{ .line_no = line_no };
    event.tenant = field(line, "tenant") orelse field(line, "workspace") orelse event.tenant;
    event.provider = field(line, "provider") orelse event.provider;
    event.model = field(line, "model") orelse event.model;
    event.region = field(line, "region") orelse field(line, "data_region") orelse event.region;
    event.request_key = field(line, "idempotency_key") orelse field(line, "request_id") orelse field(line, "id") orelse "";
    event.trace_id = field(line, "trace_id") orelse field(line, "trace") orelse "";
    event.phase = field(line, "phase") orelse field(line, "event") orelse "";
    event.status = field(line, "status") orelse field(line, "code") orelse "";
    event.tokens_in = fieldU64(line, "tokens_in") orelse fieldU64(line, "input_tokens") orelse 0;
    event.tokens_out = fieldU64(line, "tokens_out") orelse fieldU64(line, "output_tokens") orelse 0;
    event.context_tokens = fieldU64(line, "context_tokens") orelse (event.tokens_in + event.tokens_out);
    event.cost_micros = fieldMoney(line, "cost_usd") orelse fieldMoney(line, "usd") orelse fieldU64(line, "cost_micros") orelse 0;
    event.latency_ms = fieldU64(line, "latency_ms") orelse fieldU64(line, "duration_ms") orelse 0;
    event.queue_ms = fieldU64(line, "queue_ms") orelse fieldU64(line, "queued_ms") orelse 0;
    event.stream_seconds = fieldU64(line, "stream_seconds") orelse fieldU64(line, "stream_s") orelse 0;
    event.chunk_count = fieldU64(line, "chunk_count") orelse fieldU64(line, "chunks") orelse 0;
    event.retry_after_ms = fieldU64(line, "retry_after_ms") orelse 0;
    event.tool_calls = fieldU64(line, "tool_calls") orelse fieldU64(line, "tools") orelse 0;
    event.cache_hit = fieldBool(line, "cache_hit") orelse fieldBool(line, "prompt_cache_hit") orelse false;
    event.secret_hit = fieldBool(line, "secret_hit") orelse fieldBool(line, "pii_hit") orelse false;
    return event;
}

fn field(line: []const u8, key: []const u8) ?[]const u8 {
    var cursor: usize = 0;
    while (cursor < line.len) {
        const found = std.mem.indexOfPos(u8, line, cursor, key) orelse return null;
        var pos = found + key.len;
        if (found > 0) {
            const before = line[found - 1];
            if (std.ascii.isAlphanumeric(before) or before == '_' or before == '-') {
                cursor = pos;
                continue;
            }
        }
        if (pos < line.len and line[pos] == '"') pos += 1;
        while (pos < line.len and std.ascii.isWhitespace(line[pos])) pos += 1;
        if (pos >= line.len or (line[pos] != ':' and line[pos] != '=')) {
            cursor = pos;
            continue;
        }
        pos += 1;
        while (pos < line.len and std.ascii.isWhitespace(line[pos])) pos += 1;
        if (pos >= line.len) return null;
        if (line[pos] == '"') {
            pos += 1;
            const start = pos;
            while (pos < line.len and line[pos] != '"') pos += 1;
            return trim(line[start..pos]);
        }
        const start = pos;
        while (pos < line.len and line[pos] != ',' and line[pos] != '}' and !std.ascii.isWhitespace(line[pos])) pos += 1;
        return trim(line[start..pos]);
    }
    return null;
}

fn fieldU64(line: []const u8, key: []const u8) ?u64 {
    return parseU64(field(line, key) orelse return null);
}

fn fieldMoney(line: []const u8, key: []const u8) ?u64 {
    return parseMoneyMicros(field(line, key) orelse return null);
}

fn fieldBool(line: []const u8, key: []const u8) ?bool {
    const raw = field(line, key) orelse return null;
    if (asciiEq(raw, "true") or asciiEq(raw, "yes") or std.mem.eql(u8, raw, "1")) return true;
    if (asciiEq(raw, "false") or asciiEq(raw, "no") or std.mem.eql(u8, raw, "0")) return false;
    return null;
}

fn parseU64(raw: []const u8) ?u64 {
    const clean = trim(raw);
    if (clean.len == 0) return null;
    return std.fmt.parseInt(u64, clean, 10) catch null;
}

fn parseMoneyMicros(raw: []const u8) ?u64 {
    var value = trim(raw);
    if (value.len > 0 and value[0] == '$') value = value[1..];
    const dot = std.mem.indexOfScalar(u8, value, '.');
    if (dot) |idx| {
        const dollars = parseU64(value[0..idx]) orelse return null;
        var micros = dollars * 1_000_000;
        var place: u64 = 100_000;
        var i: usize = idx + 1;
        while (i < value.len and place > 0) : (i += 1) {
            const ch = value[i];
            if (ch < '0' or ch > '9') return null;
            micros += (@as(u64, ch - '0') * place);
            place /= 10;
        }
        return micros;
    }
    return (parseU64(value) orelse return null) * 1_000_000;
}

fn addTenantLimit(config: *Config, raw: []const u8) !void {
    const eq = std.mem.indexOfScalar(u8, raw, '=') orelse return usageAndFail("tenant limit must look like tenant=12.34");
    const tenant = trim(raw[0..eq]);
    const micros = parseMoneyMicros(raw[eq + 1 ..]) orelse return usageAndFail("bad tenant money value");
    try config.tenant_limits.append(.{ .tenant = tenant, .micros = micros });
}

fn isFailure(status: []const u8) bool {
    if (status.len == 0) return false;
    if (asciiEq(status, "failed") or asciiEq(status, "error") or asciiEq(status, "throttled")) return true;
    if (status[0] == '4' or status[0] == '5') return true;
    return false;
}

fn regionAllowed(allowed: []const u8, region: []const u8) bool {
    if (region.len == 0 or asciiEq(region, "unknown")) return true;
    var parts = std.mem.splitScalar(u8, allowed, ',');
    while (parts.next()) |part| {
        if (asciiEq(trim(part), region)) return true;
    }
    return false;
}

fn containsSecretSignal(text: []const u8) bool {
    return containsAscii(text, "secret") or containsAscii(text, "api_key") or containsAscii(text, "token_leak") or containsAscii(text, "credential");
}

fn hasSeverityAtLeast(analysis: Analysis, threshold: Severity) bool {
    for (analysis.findings.items) |finding| {
        if (@intFromEnum(finding.sev) >= @intFromEnum(threshold)) return true;
    }
    return false;
}

fn sortFindings(analysis: *Analysis) void {
    std.mem.sort(Finding, analysis.findings.items, {}, struct {
        fn lessThan(_: void, a: Finding, b: Finding) bool {
            if (@intFromEnum(a.sev) != @intFromEnum(b.sev)) return @intFromEnum(a.sev) > @intFromEnum(b.sev);
            if (a.line_no != b.line_no) return a.line_no < b.line_no;
            return std.mem.lessThan(u8, a.rule, b.rule);
        }
    }.lessThan);
}

fn render(writer: anytype, analysis: Analysis, config: Config) !void {
    if (asciiEq(config.format, "json")) return renderJson(writer, analysis);
    if (asciiEq(config.format, "markdown") or asciiEq(config.format, "md")) return renderMarkdown(writer, analysis, config);
    if (asciiEq(config.format, "sarif")) return renderSarif(writer, analysis);
    return renderSummary(writer, analysis, config);
}

fn renderSummary(writer: anytype, analysis: Analysis, config: Config) !void {
    try writer.print("Tenant stream budget ledger\n", .{});
    try writer.print("events parsed: {d}/{d}\n", .{ analysis.parsed_events, analysis.total_lines });
    try writer.print("tenants: {d}\n", .{analysis.tenants.items.len});
    try writer.print("findings: {d}\n", .{analysis.findings.items.len});
    try writer.print("fail threshold: {s}\n\n", .{config.fail_on.label()});
    for (analysis.tenants.items) |tenant| {
        try writer.print(
            "{s}: requests={d} failures={d} cost=${d}.{d:0>6} tokens={d}/{d} max_context={d} max_latency_ms={d} cache_hits={d}\n",
            .{ tenant.name, tenant.requests, tenant.failures, tenant.total_cost_micros / 1_000_000, tenant.total_cost_micros % 1_000_000, tenant.tokens_in, tenant.tokens_out, tenant.max_context_tokens, tenant.max_latency_ms, tenant.cache_hits },
        );
    }
    if (analysis.findings.items.len > 0) {
        try writer.writeAll("\nTop findings:\n");
        for (analysis.findings.items[0..@min(analysis.findings.items.len, 25)]) |finding| {
            try writer.print("- [{s}] {s} tenant={s} line={d} value={d} limit={d}: {s}\n", .{ finding.sev.label(), finding.rule, finding.tenant, finding.line_no, finding.value, finding.limit, finding.message });
        }
    }
}

fn renderMarkdown(writer: anytype, analysis: Analysis, config: Config) !void {
    try writer.writeAll("# Tenant Stream Budget Ledger\n\n");
    try writer.print("- Events parsed: {d}/{d}\n", .{ analysis.parsed_events, analysis.total_lines });
    try writer.print("- Tenants: {d}\n", .{analysis.tenants.items.len});
    try writer.print("- Findings: {d}\n", .{analysis.findings.items.len});
    try writer.print("- Fail threshold: `{s}`\n\n", .{config.fail_on.label()});
    try writer.writeAll("## Tenant Spend And Capacity\n\n");
    try writer.writeAll("| Tenant | Requests | Failures | Cost USD | Input Tokens | Output Tokens | Max Context | Max Latency ms | Cache Hits |\n");
    try writer.writeAll("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for (analysis.tenants.items) |tenant| {
        try writer.print("| {s} | {d} | {d} | {d}.{d:0>6} | {d} | {d} | {d} | {d} | {d} |\n", .{ tenant.name, tenant.requests, tenant.failures, tenant.total_cost_micros / 1_000_000, tenant.total_cost_micros % 1_000_000, tenant.tokens_in, tenant.tokens_out, tenant.max_context_tokens, tenant.max_latency_ms, tenant.cache_hits });
    }
    try writer.writeAll("\n## Findings\n\n");
    if (analysis.findings.items.len == 0) {
        try writer.writeAll("No findings at the configured policy levels.\n");
        return;
    }
    try writer.writeAll("| Severity | Rule | Tenant | Line | Value | Limit | Message |\n");
    try writer.writeAll("|---|---|---|---:|---:|---:|---|\n");
    for (analysis.findings.items) |finding| {
        try writer.print("| {s} | `{s}` | {s} | {d} | {d} | {d} | {s} |\n", .{ finding.sev.label(), finding.rule, finding.tenant, finding.line_no, finding.value, finding.limit, finding.message });
    }
}

fn renderJson(writer: anytype, analysis: Analysis) !void {
    try writer.writeAll("{\"tenants\":[");
    for (analysis.tenants.items, 0..) |tenant, idx| {
        if (idx > 0) try writer.writeAll(",");
        try writer.print(
            "{{\"name\":\"{s}\",\"requests\":{d},\"failures\":{d},\"cost_micros\":{d},\"tokens_in\":{d},\"tokens_out\":{d},\"max_context_tokens\":{d},\"max_latency_ms\":{d},\"cache_hits\":{d}}}",
            .{ tenant.name, tenant.requests, tenant.failures, tenant.total_cost_micros, tenant.tokens_in, tenant.tokens_out, tenant.max_context_tokens, tenant.max_latency_ms, tenant.cache_hits },
        );
    }
    try writer.writeAll("],\"findings\":[");
    for (analysis.findings.items, 0..) |finding, idx| {
        if (idx > 0) try writer.writeAll(",");
        try writer.print(
            "{{\"severity\":\"{s}\",\"rule\":\"{s}\",\"tenant\":\"{s}\",\"line\":{d},\"value\":{d},\"limit\":{d},\"message\":\"",
            .{ finding.sev.label(), finding.rule, finding.tenant, finding.line_no, finding.value, finding.limit },
        );
        try writeJsonStringBody(writer, finding.message);
        try writer.writeAll("\"}");
    }
    try writer.writeAll("]}\n");
}

fn renderSarif(writer: anytype, analysis: Analysis) !void {
    try writer.writeAll("{\"version\":\"2.1.0\",\"runs\":[{\"tool\":{\"driver\":{\"name\":\"TenantStreamBudgetLedger\",\"informationUri\":\"https://github.com/kspavankrishna/VIBE-CODE\"}},\"results\":[");
    for (analysis.findings.items, 0..) |finding, idx| {
        if (idx > 0) try writer.writeAll(",");
        try writer.print(
            "{{\"ruleId\":\"{s}\",\"level\":\"{s}\",\"message\":{{\"text\":\"",
            .{ finding.rule, if (finding.sev == .critical or finding.sev == .high) "error" else "warning" },
        );
        try writeJsonStringBody(writer, finding.message);
        try writer.print("\"}},\"locations\":[{{\"physicalLocation\":{{\"artifactLocation\":{{\"uri\":\"gateway-log\"}},\"region\":{{\"startLine\":{d}}}}}}}]}}", .{@max(finding.line_no, 1)});
    }
    try writer.writeAll("]}]}\n");
}

fn writeJsonStringBody(writer: anytype, text: []const u8) !void {
    for (text) |ch| {
        switch (ch) {
            '\\' => try writer.writeAll("\\\\"),
            '"' => try writer.writeAll("\\\""),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => try writer.writeByte(ch),
        }
    }
}

fn runSelfTest(allocator: Allocator) !void {
    const sample =
        \\{"tenant":"research","provider":"openai","model":"frontier","region":"eu","request_id":"r1","trace_id":"t1","status":"200","tokens_in":90000,"tokens_out":12000,"context_tokens":160000,"cost_usd":15.25,"latency_ms":21000,"queue_ms":1200,"stream_seconds":110,"cache_hit":false,"tool_calls":4}
        \\{"tenant":"research","provider":"openai","model":"frontier","region":"eu","request_id":"r1","trace_id":"t2","status":"500","tokens_in":90000,"tokens_out":12000,"context_tokens":160000,"cost_usd":15.25,"latency_ms":21000,"queue_ms":42000,"stream_seconds":110,"cache_hit":false,"tool_calls":22}
        \\{"tenant":"sales","provider":"anthropic","model":"router","region":"moon","request_id":"s1","trace_id":"s1","status":"secret_leak","tokens_in":100,"tokens_out":100,"cost_usd":0.01,"latency_ms":100,"secret_hit":true}
    ;
    var config = Config.init(allocator);
    defer config.deinit();
    config.default_daily_micros = 20_000_000;
    config.max_context_tokens = 128_000;
    config.max_stream_seconds = 90;
    var analysis = try analyze(allocator, sample, config);
    defer analysis.deinit();
    if (analysis.parsed_events != 3) return error.SelfTestEventCount;
    if (analysis.tenants.items.len != 2) return error.SelfTestTenantCount;
    if (!hasSeverityAtLeast(analysis, .critical)) return error.SelfTestSeverity;
    var replay = false;
    var region = false;
    var budget = false;
    for (analysis.findings.items) |finding| {
        replay = replay or std.mem.eql(u8, finding.rule, "replay-key-drift");
        region = region or std.mem.eql(u8, finding.rule, "region-residency");
        budget = budget or std.mem.eql(u8, finding.rule, "tenant-budget");
    }
    if (!replay or !region or !budget) return error.SelfTestCoverage;
    try std.io.getStdOut().writer().writeAll("self-test ok\n");
}

fn trim(raw: []const u8) []const u8 {
    return std.mem.trim(u8, raw, " \t\r\n\"");
}

fn asciiEq(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(trim(a), trim(b));
}

fn containsAscii(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (haystack.len < needle.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

/*
This solves a painful April 2026 problem for teams running AI gateways, edge inference workers, streaming chat APIs, and tenant billed agent systems: the logs usually know about context window blowups, prompt cache misses, expensive retries, replayed idempotency keys, data residency drift, long socket streams, and secret egress before dashboards show the real outage. Built because I wanted one small Zig file that can sit next to gateway JSONL, OpenTelemetry exports, CDN worker logs, or provider billing traces and turn them into strict spend and safety evidence without pulling in a runtime stack. Use it when a platform team needs a fast command line guardrail before a release, a nightly budget review, a CI artifact check, or a production incident review for OpenAI, Anthropic, Gemini, local model, or custom inference routes. The trick: it normalizes money to integer micros, keeps tenant state in memory, checks replay keys across traces, treats region and stream duration as first class limits, and emits summary, JSON, Markdown, or SARIF so the same result can feed humans, GitHub code scanning, and automation. Drop this into a repository that owns AI infrastructure, MCP tools, LLM wrappers, edge compute, DevOps admission control, or research pipelines, then point it at real gateway events before high context prompts, tool fanout, cache misses, or tenant quota leaks turn into a weekend fire drill.
*/
