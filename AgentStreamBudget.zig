const std = @import("std");

const Allocator = std.mem.Allocator;

const ExitCode = enum(u8) {
    ok = 0,
    policy_failed = 2,
    usage = 64,
    io = 74,
    data = 65,
};

const OutputFormat = enum {
    text,
    json,
};

const Policy = struct {
    max_input_tokens: u64 = 180_000,
    max_output_tokens: u64 = 120_000,
    max_total_tokens: u64 = 260_000,
    max_tool_calls: u64 = 80,
    max_tool_latency_ms: u64 = 45_000,
    max_stream_gap_ms: u64 = 12_000,
    max_egress_bytes: u64 = 64 * 1024 * 1024,
    max_cost_micros: u64 = 500_000,
    max_error_rate_per_10k: u64 = 700,
    fail_on_invalid_json: bool = true,

    pub fn load(allocator: Allocator, path: []const u8) !Policy {
        var policy = Policy{};
        const data = try std.fs.cwd().readFileAlloc(allocator, path, 4 * 1024 * 1024);
        defer allocator.free(data);

        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, data, .{});
        defer parsed.deinit();

        if (parsed.value != .object) return error.PolicyMustBeObject;
        const obj = parsed.value.object;
        policy.max_input_tokens = intOr(obj, "max_input_tokens", policy.max_input_tokens);
        policy.max_output_tokens = intOr(obj, "max_output_tokens", policy.max_output_tokens);
        policy.max_total_tokens = intOr(obj, "max_total_tokens", policy.max_total_tokens);
        policy.max_tool_calls = intOr(obj, "max_tool_calls", policy.max_tool_calls);
        policy.max_tool_latency_ms = intOr(obj, "max_tool_latency_ms", policy.max_tool_latency_ms);
        policy.max_stream_gap_ms = intOr(obj, "max_stream_gap_ms", policy.max_stream_gap_ms);
        policy.max_egress_bytes = intOr(obj, "max_egress_bytes", policy.max_egress_bytes);
        policy.max_cost_micros = intOr(obj, "max_cost_micros", policy.max_cost_micros);
        if (numberField(obj, &.{"max_cost_usd", "max_usd"})) |usd| {
            policy.max_cost_micros = dollarsToMicros(usd);
        }
        if (numberField(obj, &.{"max_error_rate_percent", "max_error_percent"})) |pct| {
            policy.max_error_rate_per_10k = @as(u64, @intFromFloat(@max(0.0, pct) * 100.0));
        }
        if (boolField(obj, "fail_on_invalid_json")) |enabled| {
            policy.fail_on_invalid_json = enabled;
        }
        return policy;
    }
};

const Config = struct {
    allocator: Allocator,
    inputs: std.ArrayList([]const u8),
    format: OutputFormat = .text,
    policy_path: ?[]const u8 = null,
    fail_on_risk: bool = false,
    session_key: []const u8 = "session_id",
    max_input_bytes: usize = 256 * 1024 * 1024,

    pub fn init(allocator: Allocator) Config {
        return .{
            .allocator = allocator,
            .inputs = std.ArrayList([]const u8).init(allocator),
        };
    }

    pub fn deinit(self: *Config) void {
        self.inputs.deinit();
    }
};

const SessionStats = struct {
    id: []const u8,
    model: []const u8 = "",
    events: u64 = 0,
    invalid_events: u64 = 0,
    input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    cached_tokens: u64 = 0,
    total_tokens: u64 = 0,
    tool_calls: u64 = 0,
    failed_tool_calls: u64 = 0,
    stream_events: u64 = 0,
    stream_bytes: u64 = 0,
    egress_bytes: u64 = 0,
    cost_micros: u64 = 0,
    max_tool_latency_ms: u64 = 0,
    first_ts_ms: ?i64 = null,
    last_ts_ms: ?i64 = null,
    previous_ts_ms: ?i64 = null,
    max_gap_ms: u64 = 0,

    pub fn deinit(self: *SessionStats, allocator: Allocator) void {
        allocator.free(self.id);
        if (self.model.len > 0) allocator.free(self.model);
    }

    fn observeTimestamp(self: *SessionStats, ts_ms: i64) void {
        if (self.first_ts_ms == null or ts_ms < self.first_ts_ms.?) self.first_ts_ms = ts_ms;
        if (self.last_ts_ms == null or ts_ms > self.last_ts_ms.?) self.last_ts_ms = ts_ms;
        if (self.previous_ts_ms) |previous| {
            if (ts_ms > previous) {
                const gap = @as(u64, @intCast(ts_ms - previous));
                if (gap > self.max_gap_ms) self.max_gap_ms = gap;
            }
        }
        self.previous_ts_ms = ts_ms;
    }

    fn errorRatePer10k(self: SessionStats) u64 {
        if (self.tool_calls == 0) return 0;
        return divCeil(self.failed_tool_calls * 10_000, self.tool_calls);
    }

    fn riskCount(self: SessionStats, policy: Policy) u64 {
        var n: u64 = 0;
        if (self.input_tokens > policy.max_input_tokens) n += 1;
        if (self.output_tokens > policy.max_output_tokens) n += 1;
        if (self.total_tokens > policy.max_total_tokens) n += 1;
        if (self.tool_calls > policy.max_tool_calls) n += 1;
        if (self.max_tool_latency_ms > policy.max_tool_latency_ms) n += 1;
        if (self.max_gap_ms > policy.max_stream_gap_ms) n += 1;
        if (self.egress_bytes > policy.max_egress_bytes) n += 1;
        if (self.cost_micros > policy.max_cost_micros) n += 1;
        if (self.errorRatePer10k() > policy.max_error_rate_per_10k) n += 1;
        return n;
    }
};

const Monitor = struct {
    allocator: Allocator,
    sessions: std.StringHashMap(SessionStats),
    total_lines: u64 = 0,
    invalid_lines: u64 = 0,
    anonymous_events: u64 = 0,

    pub fn init(allocator: Allocator) Monitor {
        return .{
            .allocator = allocator,
            .sessions = std.StringHashMap(SessionStats).init(allocator),
        };
    }

    pub fn deinit(self: *Monitor) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.sessions.deinit();
    }

    fn getOrCreateSession(self: *Monitor, raw_id: []const u8) !*SessionStats {
        var gop = try self.sessions.getOrPut(raw_id);
        if (!gop.found_existing) {
            const owned_id = try self.allocator.dupe(u8, raw_id);
            gop.key_ptr.* = owned_id;
            gop.value_ptr.* = .{ .id = owned_id };
        }
        return gop.value_ptr;
    }

    pub fn ingestBuffer(self: *Monitor, config: Config, bytes: []const u8) !void {
        var lines = std.mem.splitScalar(u8, bytes, '\n');
        while (lines.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, " \t\r");
            if (line.len == 0) continue;
            self.total_lines += 1;
            try self.ingestLine(config, line);
        }
    }

    fn ingestLine(self: *Monitor, config: Config, line: []const u8) !void {
        var parsed = std.json.parseFromSlice(std.json.Value, self.allocator, line, .{}) catch {
            self.invalid_lines += 1;
            return;
        };
        defer parsed.deinit();

        if (parsed.value != .object) {
            self.invalid_lines += 1;
            return;
        }

        const obj = parsed.value.object;
        const session_id = textByKey(obj, config.session_key) orelse textField(obj, &.{
            "session_id",
            "trace_id",
            "run_id",
            "request_id",
            "conversation_id",
            "thread_id",
        }) orelse "unknown";
        if (std.mem.eql(u8, session_id, "unknown")) self.anonymous_events += 1;

        const session = try self.getOrCreateSession(session_id);
        session.events += 1;

        if (session.model.len == 0) {
            if (textField(obj, &.{ "model", "model_id", "deployment", "route" })) |model| {
                session.model = try self.allocator.dupe(u8, model);
            }
        }

        if (timestampField(obj)) |ts_ms| session.observeTimestamp(ts_ms);

        const input = intField(obj, &.{ "input_tokens", "prompt_tokens", "tokens_in", "promptTokenCount" }) orelse 0;
        const output = intField(obj, &.{ "output_tokens", "completion_tokens", "tokens_out", "candidatesTokenCount" }) orelse 0;
        const cached = intField(obj, &.{ "cached_tokens", "cache_read_tokens", "cached_input_tokens" }) orelse 0;
        const total = intField(obj, &.{ "total_tokens", "tokens_total", "totalTokenCount" }) orelse input + output;
        session.input_tokens += input;
        session.output_tokens += output;
        session.cached_tokens += cached;
        session.total_tokens += total;

        if (numberField(obj, &.{ "cost_usd", "estimated_cost_usd", "usd", "price_usd" })) |usd| {
            session.cost_micros += dollarsToMicros(usd);
        } else if (intField(obj, &.{ "cost_micros", "estimated_cost_micros" })) |micros| {
            session.cost_micros += micros;
        }

        const event_name = textField(obj, &.{ "event", "type", "kind", "name" }) orelse "";
        const tool_name = textField(obj, &.{ "tool", "tool_name", "tool.name", "function_name" });
        const is_tool_event = tool_name != null or containsNoCase(event_name, "tool") or containsNoCase(event_name, "function_call");
        if (is_tool_event) {
            session.tool_calls += 1;
            const latency = intField(obj, &.{ "latency_ms", "duration_ms", "elapsed_ms", "tool_latency_ms" }) orelse 0;
            if (latency > session.max_tool_latency_ms) session.max_tool_latency_ms = latency;
            if (isFailure(obj, event_name)) session.failed_tool_calls += 1;
        }

        const stream_like = containsNoCase(event_name, "stream") or containsNoCase(event_name, "delta") or containsNoCase(event_name, "chunk");
        if (stream_like) {
            session.stream_events += 1;
            session.stream_bytes += observedBytes(obj, line.len);
        }

        session.egress_bytes += intField(obj, &.{ "egress_bytes", "bytes_out", "response_bytes", "network_bytes" }) orelse 0;
        if (session.egress_bytes == 0 and stream_like) {
            session.egress_bytes += observedBytes(obj, 0);
        }
    }

    fn anyPolicyFailure(self: Monitor, policy: Policy) bool {
        if (policy.fail_on_invalid_json and self.invalid_lines > 0) return true;
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.riskCount(policy) > 0) return true;
        }
        return false;
    }
};

fn parseArgs(allocator: Allocator) !Config {
    var cfg = Config.init(allocator);
    errdefer cfg.deinit();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--input") or std.mem.eql(u8, arg, "-i")) {
            const value = args.next() orelse return error.MissingInputPath;
            try cfg.inputs.append(value);
        } else if (std.mem.eql(u8, arg, "--policy")) {
            cfg.policy_path = args.next() orelse return error.MissingPolicyPath;
        } else if (std.mem.eql(u8, arg, "--format")) {
            const value = args.next() orelse return error.MissingFormat;
            if (std.mem.eql(u8, value, "json")) cfg.format = .json else if (std.mem.eql(u8, value, "text")) cfg.format = .text else return error.BadFormat;
        } else if (std.mem.eql(u8, arg, "--fail-on-risk")) {
            cfg.fail_on_risk = true;
        } else if (std.mem.eql(u8, arg, "--session-key")) {
            cfg.session_key = args.next() orelse return error.MissingSessionKey;
        } else if (std.mem.eql(u8, arg, "--max-input-bytes")) {
            const value = args.next() orelse return error.MissingMaxBytes;
            cfg.max_input_bytes = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            try printUsage(std.io.getStdOut().writer());
            std.process.exit(@intFromEnum(ExitCode.ok));
        } else {
            try cfg.inputs.append(arg);
        }
    }

    return cfg;
}

fn printUsage(writer: anytype) !void {
    try writer.writeAll(
        \\AgentStreamBudget.zig - CI budget gate for JSONL LLM, MCP, and agent stream traces
        \\
        \\Usage:
        \\  zig run AgentStreamBudget.zig -- [--input trace.jsonl] [--policy policy.json] [--format text|json] [--fail-on-risk]
        \\
        \\Input is newline-delimited JSON. The parser accepts common fields such as session_id, trace_id,
        \\run_id, timestamp, model, event, input_tokens, output_tokens, cost_usd, tool_name,
        \\latency_ms, egress_bytes, bytes_out, and status. Multiple --input paths are merged. With no
        \\input path, stdin is read. Policy JSON keys mirror the default Policy struct names.
        \\
    );
}

fn loadInputs(allocator: Allocator, cfg: Config, monitor: *Monitor) !void {
    if (cfg.inputs.items.len == 0) {
        const data = try std.io.getStdIn().reader().readAllAlloc(allocator, cfg.max_input_bytes);
        defer allocator.free(data);
        try monitor.ingestBuffer(cfg, data);
        return;
    }

    for (cfg.inputs.items) |path| {
        const data = try std.fs.cwd().readFileAlloc(allocator, path, cfg.max_input_bytes);
        defer allocator.free(data);
        try monitor.ingestBuffer(cfg, data);
    }
}

fn renderText(writer: anytype, monitor: Monitor, policy: Policy) !void {
    var total_sessions: u64 = 0;
    var risky_sessions: u64 = 0;
    var total_tokens: u64 = 0;
    var total_cost: u64 = 0;
    var total_tools: u64 = 0;

    var it = monitor.sessions.iterator();
    while (it.next()) |entry| {
        total_sessions += 1;
        total_tokens += entry.value_ptr.total_tokens;
        total_cost += entry.value_ptr.cost_micros;
        total_tools += entry.value_ptr.tool_calls;
        if (entry.value_ptr.riskCount(policy) > 0) risky_sessions += 1;
    }

    try writer.print("agent stream budget report\n", .{});
    try writer.print("lines={d} invalid_json={d} sessions={d} risky_sessions={d} tokens={d} tools={d} cost=${d}.{d:0>6}\n\n", .{
        monitor.total_lines,
        monitor.invalid_lines,
        total_sessions,
        risky_sessions,
        total_tokens,
        total_tools,
        total_cost / 1_000_000,
        total_cost % 1_000_000,
    });

    var sit = monitor.sessions.iterator();
    while (sit.next()) |entry| {
        const s = entry.value_ptr.*;
        const risk = s.riskCount(policy);
        try writer.print("session={s} model={s} risk={d} events={d} tokens={d}/{d}/{d} tools={d} failed_tools={d} max_tool_ms={d} max_gap_ms={d} egress={d} cost=${d}.{d:0>6}\n", .{
            s.id,
            if (s.model.len == 0) "unknown" else s.model,
            risk,
            s.events,
            s.input_tokens,
            s.output_tokens,
            s.total_tokens,
            s.tool_calls,
            s.failed_tool_calls,
            s.max_tool_latency_ms,
            s.max_gap_ms,
            s.egress_bytes,
            s.cost_micros / 1_000_000,
            s.cost_micros % 1_000_000,
        });
        if (risk > 0) try renderReasons(writer, s, policy);
    }
}

fn renderReasons(writer: anytype, s: SessionStats, policy: Policy) !void {
    if (s.input_tokens > policy.max_input_tokens) try writer.print("  - input_tokens {d} > {d}\n", .{ s.input_tokens, policy.max_input_tokens });
    if (s.output_tokens > policy.max_output_tokens) try writer.print("  - output_tokens {d} > {d}\n", .{ s.output_tokens, policy.max_output_tokens });
    if (s.total_tokens > policy.max_total_tokens) try writer.print("  - total_tokens {d} > {d}\n", .{ s.total_tokens, policy.max_total_tokens });
    if (s.tool_calls > policy.max_tool_calls) try writer.print("  - tool_calls {d} > {d}\n", .{ s.tool_calls, policy.max_tool_calls });
    if (s.max_tool_latency_ms > policy.max_tool_latency_ms) try writer.print("  - max_tool_latency_ms {d} > {d}\n", .{ s.max_tool_latency_ms, policy.max_tool_latency_ms });
    if (s.max_gap_ms > policy.max_stream_gap_ms) try writer.print("  - max_stream_gap_ms {d} > {d}\n", .{ s.max_gap_ms, policy.max_stream_gap_ms });
    if (s.egress_bytes > policy.max_egress_bytes) try writer.print("  - egress_bytes {d} > {d}\n", .{ s.egress_bytes, policy.max_egress_bytes });
    if (s.cost_micros > policy.max_cost_micros) try writer.print("  - cost_micros {d} > {d}\n", .{ s.cost_micros, policy.max_cost_micros });
    if (s.errorRatePer10k() > policy.max_error_rate_per_10k) try writer.print("  - tool_error_rate_per_10k {d} > {d}\n", .{ s.errorRatePer10k(), policy.max_error_rate_per_10k });
}

fn renderJson(writer: anytype, monitor: Monitor, policy: Policy) !void {
    try writer.writeAll("{\"schema\":\"agent-stream-budget/v1\",");
    try writer.print("\"lines\":{d},\"invalid_json\":{d},\"anonymous_events\":{d},", .{
        monitor.total_lines,
        monitor.invalid_lines,
        monitor.anonymous_events,
    });
    try writer.writeAll("\"sessions\":[");
    var first = true;
    var it = monitor.sessions.iterator();
    while (it.next()) |entry| {
        if (!first) try writer.writeByte(',');
        first = false;
        const s = entry.value_ptr.*;
        try writer.writeByte('{');
        try writer.writeAll("\"id\":");
        try writeJsonString(writer, s.id);
        try writer.writeAll(",\"model\":");
        try writeJsonString(writer, if (s.model.len == 0) "unknown" else s.model);
        try writer.print(",\"risk\":{d},\"events\":{d},\"input_tokens\":{d},\"output_tokens\":{d},\"cached_tokens\":{d},\"total_tokens\":{d},\"tool_calls\":{d},\"failed_tool_calls\":{d},\"stream_events\":{d},\"stream_bytes\":{d},\"egress_bytes\":{d},\"cost_micros\":{d},\"max_tool_latency_ms\":{d},\"max_stream_gap_ms\":{d},\"tool_error_rate_per_10k\":{d}", .{
            s.riskCount(policy),
            s.events,
            s.input_tokens,
            s.output_tokens,
            s.cached_tokens,
            s.total_tokens,
            s.tool_calls,
            s.failed_tool_calls,
            s.stream_events,
            s.stream_bytes,
            s.egress_bytes,
            s.cost_micros,
            s.max_tool_latency_ms,
            s.max_gap_ms,
            s.errorRatePer10k(),
        });
        try writer.writeByte('}');
    }
    try writer.writeAll("]}");
    try writer.writeByte('\n');
}

fn textByKey(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    if (obj.get(key)) |value| return valueAsText(value);
    return null;
}

fn textField(obj: std.json.ObjectMap, keys: []const []const u8) ?[]const u8 {
    for (keys) |key| {
        if (textByKey(obj, key)) |value| return value;
    }
    return null;
}

fn valueAsText(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |s| s,
        else => null,
    };
}

fn intOr(obj: std.json.ObjectMap, key: []const u8, fallback: u64) u64 {
    return intField(obj, &.{key}) orelse fallback;
}

fn intField(obj: std.json.ObjectMap, keys: []const []const u8) ?u64 {
    for (keys) |key| {
        if (obj.get(key)) |value| {
            if (valueAsU64(value)) |n| return n;
        }
    }
    return null;
}

fn numberField(obj: std.json.ObjectMap, keys: []const []const u8) ?f64 {
    for (keys) |key| {
        if (obj.get(key)) |value| {
            switch (value) {
                .integer => |i| return @as(f64, @floatFromInt(i)),
                .float => |f| return f,
                .string => |s| return std.fmt.parseFloat(f64, s) catch null,
                else => {},
            }
        }
    }
    return null;
}

fn boolField(obj: std.json.ObjectMap, key: []const u8) ?bool {
    if (obj.get(key)) |value| {
        return switch (value) {
            .bool => |b| b,
            .string => |s| std.ascii.eqlIgnoreCase(s, "true") or std.mem.eql(u8, s, "1"),
            else => null,
        };
    }
    return null;
}

fn valueAsU64(value: std.json.Value) ?u64 {
    return switch (value) {
        .integer => |i| if (i >= 0) @as(u64, @intCast(i)) else null,
        .float => |f| if (f >= 0) @as(u64, @intFromFloat(f)) else null,
        .string => |s| std.fmt.parseUnsigned(u64, s, 10) catch null,
        else => null,
    };
}

fn timestampField(obj: std.json.ObjectMap) ?i64 {
    for (&.{ "timestamp_ms", "ts_ms", "time_ms" }) |key| {
        if (obj.get(key)) |value| {
            if (valueAsU64(value)) |n| return @as(i64, @intCast(n));
        }
    }
    for (&.{ "timestamp", "ts", "time", "created_at" }) |key| {
        if (obj.get(key)) |value| {
            switch (value) {
                .integer => |i| return normalizeEpoch(i),
                .float => |f| return normalizeEpoch(@as(i64, @intFromFloat(f))),
                .string => |s| return parseTimestampString(s),
                else => {},
            }
        }
    }
    return null;
}

fn normalizeEpoch(raw: i64) i64 {
    if (raw > 4_000_000_000_000) return raw;
    if (raw > 4_000_000_000) return raw;
    return raw * 1000;
}

fn parseTimestampString(s: []const u8) ?i64 {
    if (s.len == 0) return null;
    var all_digits = true;
    for (s) |c| {
        if (!std.ascii.isDigit(c)) {
            all_digits = false;
            break;
        }
    }
    if (all_digits) {
        const n = std.fmt.parseInt(i64, s, 10) catch return null;
        return normalizeEpoch(n);
    }
    if (s.len < 20) return null;
    const year = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
    const month = std.fmt.parseInt(u8, s[5..7], 10) catch return null;
    const day = std.fmt.parseInt(u8, s[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(u8, s[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(u8, s[14..16], 10) catch return null;
    const second = std.fmt.parseInt(u8, s[17..19], 10) catch return null;
    var idx: usize = 19;
    var millis: i64 = 0;
    if (idx < s.len and s[idx] == '.') {
        idx += 1;
        var scale: i64 = 100;
        while (idx < s.len and std.ascii.isDigit(s[idx]) and scale > 0) : (idx += 1) {
            millis += @as(i64, s[idx] - '0') * scale;
            scale = @divTrunc(scale, 10);
        }
        while (idx < s.len and std.ascii.isDigit(s[idx])) idx += 1;
    }
    var tz_offset_minutes: i64 = 0;
    if (idx < s.len and (s[idx] == '+' or s[idx] == '-')) {
        const sign: i64 = if (s[idx] == '+') 1 else -1;
        if (idx + 6 > s.len) return null;
        const th = std.fmt.parseInt(i64, s[idx + 1 .. idx + 3], 10) catch return null;
        const tm = std.fmt.parseInt(i64, s[idx + 4 .. idx + 6], 10) catch return null;
        tz_offset_minutes = sign * (th * 60 + tm);
    }
    var epoch = epochMsUtc(year, month, day, hour, minute, second, millis);
    epoch -= tz_offset_minutes * 60 * 1000;
    return epoch;
}

fn epochMsUtc(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8, millis: i64) i64 {
    const days = daysFromCivil(year, month, day);
    const seconds = days * 86_400 + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
    return seconds * 1000 + millis;
}

fn daysFromCivil(year_in: i32, month_in: u8, day_in: u8) i64 {
    var y: i64 = year_in;
    var m: i64 = month_in;
    const d: i64 = day_in;
    y -= if (m <= 2) 1 else 0;
    const era = if (y >= 0) @divTrunc(y, 400) else @divTrunc(y - 399, 400);
    const yoe = y - era * 400;
    const mp = m + if (m > 2) -3 else 9;
    const doy = @divTrunc(153 * mp + 2, 5) + d - 1;
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn isFailure(obj: std.json.ObjectMap, event_name: []const u8) bool {
    if (containsNoCase(event_name, "error") or containsNoCase(event_name, "fail") or containsNoCase(event_name, "timeout")) return true;
    if (textField(obj, &.{ "status", "outcome", "result" })) |status| {
        if (containsNoCase(status, "error") or containsNoCase(status, "fail") or containsNoCase(status, "timeout")) return true;
        if (containsNoCase(status, "429") or containsNoCase(status, "5xx")) return true;
    }
    if (obj.get("error") != null) return true;
    return false;
}

fn observedBytes(obj: std.json.ObjectMap, fallback: usize) u64 {
    if (intField(obj, &.{ "bytes", "chunk_bytes", "delta_bytes", "content_length" })) |n| return n;
    if (textField(obj, &.{ "content", "delta", "message", "text" })) |s| return s.len;
    return fallback;
}

fn dollarsToMicros(usd: f64) u64 {
    if (usd <= 0) return 0;
    return @as(u64, @intFromFloat(usd * 1_000_000.0 + 0.5));
}

fn divCeil(numerator: u64, denominator: u64) u64 {
    if (denominator == 0) return 0;
    return (numerator + denominator - 1) / denominator;
}

fn containsNoCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (haystack.len < needle.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        var matched = true;
        for (needle, 0..) |c, j| {
            if (std.ascii.toLower(haystack[i + j]) != std.ascii.toLower(c)) {
                matched = false;
                break;
            }
        }
        if (matched) return true;
    }
    return false;
}

fn writeJsonString(writer: anytype, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            0...31 => try writer.print("\\u{x:0>4}", .{c}),
            else => try writer.writeByte(c),
        }
    }
    try writer.writeByte('"');
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var cfg = parseArgs(allocator) catch |err| {
        try std.io.getStdErr().writer().print("argument error: {s}\n\n", .{@errorName(err)});
        try printUsage(std.io.getStdErr().writer());
        std.process.exit(@intFromEnum(ExitCode.usage));
    };
    defer cfg.deinit();

    const policy = if (cfg.policy_path) |path| Policy.load(allocator, path) catch |err| {
        try std.io.getStdErr().writer().print("policy error: {s}\n", .{@errorName(err)});
        std.process.exit(@intFromEnum(ExitCode.data));
    } else Policy{};

    var monitor = Monitor.init(allocator);
    defer monitor.deinit();

    loadInputs(allocator, cfg, &monitor) catch |err| {
        try std.io.getStdErr().writer().print("input error: {s}\n", .{@errorName(err)});
        std.process.exit(@intFromEnum(ExitCode.io));
    };

    const stdout = std.io.getStdOut().writer();
    switch (cfg.format) {
        .text => try renderText(stdout, monitor, policy),
        .json => try renderJson(stdout, monitor, policy),
    }

    if (cfg.fail_on_risk and monitor.anyPolicyFailure(policy)) {
        std.process.exit(@intFromEnum(ExitCode.policy_failed));
    }
}

/*
This solves the boring but painful April 2026 problem of checking LLM streaming logs, MCP tool calls, agent traces, OpenTelemetry-style JSONL, and AI gateway audit files before a release quietly ships a runaway prompt, a slow tool, a surprise bill, or a data egress leak. Built because I kept seeing teams trust dashboards after incidents instead of putting a small deterministic budget gate in CI where it can block the bad deploy. Use it when you have newline-delimited JSON from Vercel AI SDK streams, OpenAI Responses API traces, LangGraph runs, model router logs, edge function telemetry, or internal agent workers and you need a fast Zig CLI that can read stdin, merge trace files, apply a simple JSON policy, and fail the build only when the session actually crosses a cost, token, tool latency, stream stall, error rate, or egress threshold. The trick: it does not assume one vendor schema. It looks for common field names, groups by session_id, trace_id, run_id, or request_id, and keeps the math explainable enough that an infra engineer can paste the output into a pull request without a second tool. Drop this into a repo as AgentStreamBudget.zig, run it with `zig run AgentStreamBudget.zig -- --input traces.jsonl --policy budget.json --fail-on-risk`, and make agent observability, LLM cost control, streaming latency SLO checks, MCP tool safety, and DevOps release gates searchable on GitHub and useful in real production work.
*/
