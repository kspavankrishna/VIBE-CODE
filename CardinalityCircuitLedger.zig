const std = @import("std");

pub const OverflowMode = enum {
    keep,
    bucketize,
    drop_field,
};

pub const Decision = enum {
    keep,
    bucketize,
    drop_field,
};

pub const CircuitError = error{
    DuplicatePolicy,
    InvalidBudget,
    InvalidPrecision,
    InvalidWindow,
};

pub const Options = struct {
    window_ns: i128 = 60 * std.time.ns_per_s,
    seed: u64 = 0x6d5a_56f0_7c2b_9e31,
};

pub const FieldPolicy = struct {
    field: []const u8,
    max_distinct: u32,
    precision: u8 = 12,
    mode: OverflowMode = .bucketize,
    replacement_prefix: []const u8 = "other",
};

pub const Observation = struct {
    decision: Decision,
    value: []const u8,
    estimate: f64,
    allowed_count: u32,
    fingerprint: u64,
};

pub const FieldSnapshot = struct {
    field: []const u8,
    estimate: f64,
    allowed_count: u32,
    observed: u64,
    rejected: u64,
    window_start_ns: ?i128,
};

pub const CardinalityCircuit = struct {
    allocator: std.mem.Allocator,
    fields: std.StringHashMap(FieldState),
    window_ns: i128,
    seed: u64,

    pub fn init(allocator: std.mem.Allocator, options: Options) !CardinalityCircuit {
        if (options.window_ns <= 0) return CircuitError.InvalidWindow;
        return .{
            .allocator = allocator,
            .fields = std.StringHashMap(FieldState).init(allocator),
            .window_ns = options.window_ns,
            .seed = options.seed,
        };
    }

    pub fn deinit(self: *CardinalityCircuit) void {
        var it = self.fields.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.fields.deinit();
    }

    pub fn addPolicy(self: *CardinalityCircuit, policy: FieldPolicy) !void {
        if (policy.max_distinct == 0) return CircuitError.InvalidBudget;
        if (policy.precision < 4 or policy.precision > 18) return CircuitError.InvalidPrecision;
        if (self.fields.contains(policy.field)) return CircuitError.DuplicatePolicy;

        const field_copy = try self.allocator.dupe(u8, policy.field);
        var field_owned = true;
        errdefer if (field_owned) self.allocator.free(field_copy);

        const prefix_copy = try self.allocator.dupe(u8, policy.replacement_prefix);
        var prefix_owned = true;
        errdefer if (prefix_owned) self.allocator.free(prefix_copy);

        var state = try FieldState.init(self.allocator, .{
            .field = field_copy,
            .max_distinct = policy.max_distinct,
            .precision = policy.precision,
            .mode = policy.mode,
            .replacement_prefix = prefix_copy,
        });
        field_owned = false;
        prefix_owned = false;
        errdefer state.deinit(self.allocator);

        try self.fields.put(field_copy, state);
    }

    pub fn observe(
        self: *CardinalityCircuit,
        now_ns: i128,
        field: []const u8,
        value: []const u8,
        replacement_buffer: []u8,
    ) !Observation {
        const fingerprint = hashFieldValue(self.seed, field, value);
        const state = self.fields.getPtr(field) orelse return .{
            .decision = .keep,
            .value = value,
            .estimate = 0,
            .allowed_count = 0,
            .fingerprint = fingerprint,
        };

        state.rotate(now_ns, self.window_ns);
        state.current.offer(fingerprint);
        state.observed +|= 1;

        if (state.allowed_hashes.contains(fingerprint)) {
            return .{
                .decision = .keep,
                .value = value,
                .estimate = state.current.estimate(),
                .allowed_count = @intCast(state.allowed_hashes.count()),
                .fingerprint = fingerprint,
            };
        }

        const allowed_count: u32 = @intCast(state.allowed_hashes.count());
        if (allowed_count < state.policy.max_distinct or state.policy.mode == .keep) {
            if (allowed_count < state.policy.max_distinct) {
                try state.allowed_hashes.put(fingerprint, {});
            }
            return .{
                .decision = .keep,
                .value = value,
                .estimate = state.current.estimate(),
                .allowed_count = @intCast(state.allowed_hashes.count()),
                .fingerprint = fingerprint,
            };
        }

        state.rejected +|= 1;
        switch (state.policy.mode) {
            .keep => unreachable,
            .drop_field => return .{
                .decision = .drop_field,
                .value = "",
                .estimate = state.current.estimate(),
                .allowed_count = @intCast(state.allowed_hashes.count()),
                .fingerprint = fingerprint,
            },
            .bucketize => {
                const replacement = try renderReplacement(
                    state.policy.replacement_prefix,
                    state.policy.field,
                    fingerprint,
                    replacement_buffer,
                );
                return .{
                    .decision = .bucketize,
                    .value = replacement,
                    .estimate = state.current.estimate(),
                    .allowed_count = @intCast(state.allowed_hashes.count()),
                    .fingerprint = fingerprint,
                };
            },
        }
    }

    pub fn snapshot(self: *CardinalityCircuit, field: []const u8) ?FieldSnapshot {
        const state = self.fields.getPtr(field) orelse return null;
        return .{
            .field = state.policy.field,
            .estimate = state.current.estimate(),
            .allowed_count = @intCast(state.allowed_hashes.count()),
            .observed = state.observed,
            .rejected = state.rejected,
            .window_start_ns = state.window_start_ns,
        };
    }

    pub fn writePrometheus(self: *CardinalityCircuit, writer: anytype) !void {
        try writer.writeAll("# HELP cardinality_circuit_estimate Estimated distinct values in the active window.\n");
        try writer.writeAll("# TYPE cardinality_circuit_estimate gauge\n");
        try writer.writeAll("# HELP cardinality_circuit_rejected_total Values bucketized or dropped by this process.\n");
        try writer.writeAll("# TYPE cardinality_circuit_rejected_total counter\n");

        var it = self.fields.iterator();
        while (it.next()) |entry| {
            const state = entry.value_ptr;
            try writer.writeAll("cardinality_circuit_estimate{field=\"");
            try writePrometheusLabel(writer, state.policy.field);
            try writer.print("\"}} {d}\n", .{state.current.estimate()});

            try writer.writeAll("cardinality_circuit_rejected_total{field=\"");
            try writePrometheusLabel(writer, state.policy.field);
            try writer.print("\"}} {d}\n", .{state.rejected});
        }
    }
};

const OwnedPolicy = struct {
    field: []const u8,
    max_distinct: u32,
    precision: u8,
    mode: OverflowMode,
    replacement_prefix: []const u8,
};

const FieldState = struct {
    policy: OwnedPolicy,
    current: HllCounter,
    allowed_hashes: std.AutoHashMap(u64, void),
    observed: u64 = 0,
    rejected: u64 = 0,
    window_start_ns: ?i128 = null,

    fn init(allocator: std.mem.Allocator, policy: OwnedPolicy) !FieldState {
        var current = try HllCounter.init(allocator, policy.precision);
        errdefer current.deinit(allocator);

        return .{
            .policy = policy,
            .current = current,
            .allowed_hashes = std.AutoHashMap(u64, void).init(allocator),
        };
    }

    fn deinit(self: *FieldState, allocator: std.mem.Allocator) void {
        self.current.deinit(allocator);
        self.allowed_hashes.deinit();
        allocator.free(self.policy.field);
        allocator.free(self.policy.replacement_prefix);
    }

    fn rotate(self: *FieldState, now_ns: i128, window_ns: i128) void {
        const bucket_start = floorWindow(now_ns, window_ns);
        if (self.window_start_ns) |start| {
            if (bucket_start <= start) return;
        }

        self.window_start_ns = bucket_start;
        self.current.clear();
        self.allowed_hashes.clearRetainingCapacity();
        self.observed = 0;
        self.rejected = 0;
    }
};

pub const HllCounter = struct {
    precision: u8,
    registers: []u8,

    pub fn init(allocator: std.mem.Allocator, precision: u8) !HllCounter {
        if (precision < 4 or precision > 18) return CircuitError.InvalidPrecision;
        const count = registerCount(precision);
        const registers = try allocator.alloc(u8, count);
        @memset(registers, 0);
        return .{ .precision = precision, .registers = registers };
    }

    pub fn deinit(self: *HllCounter, allocator: std.mem.Allocator) void {
        allocator.free(self.registers);
    }

    pub fn clear(self: *HllCounter) void {
        @memset(self.registers, 0);
    }

    pub fn offer(self: *HllCounter, hash: u64) void {
        const mask: u64 = @intCast(self.registers.len - 1);
        const index: usize = @intCast(hash & mask);
        const rank_value = leadingZeroRank(hash, self.precision);
        if (rank_value > self.registers[index]) {
            self.registers[index] = rank_value;
        }
    }

    pub fn estimate(self: *const HllCounter) f64 {
        const m = @as(f64, @floatFromInt(self.registers.len));
        var inverse_sum: f64 = 0;
        var zero_count: usize = 0;

        for (self.registers) |rank_value| {
            inverse_sum += std.math.pow(f64, 2.0, -@as(f64, @floatFromInt(rank_value)));
            if (rank_value == 0) zero_count += 1;
        }

        var raw = alphaFor(self.registers.len) * m * m / inverse_sum;
        if (raw <= 2.5 * m and zero_count > 0) {
            const zeros = @as(f64, @floatFromInt(zero_count));
            raw = m * @log(m / zeros);
        }
        return raw;
    }
};

fn registerCount(precision: u8) usize {
    var count: usize = 1;
    var i: u8 = 0;
    while (i < precision) : (i += 1) count *= 2;
    return count;
}

fn alphaFor(registers: usize) f64 {
    return switch (registers) {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        else => 0.7213 / (1.0 + 1.079 / @as(f64, @floatFromInt(registers))),
    };
}

fn leadingZeroRank(hash: u64, precision: u8) u8 {
    const shift: u6 = @intCast(precision);
    const remaining_bits: u8 = 64 - precision;
    const suffix = hash >> shift;
    if (suffix == 0) return remaining_bits + 1;

    const normalize_shift: u6 = @intCast(precision);
    const normalized = suffix << normalize_shift;
    const zeros: u8 = @intCast(@clz(normalized));
    const rank_value = zeros + 1;
    const max_rank = remaining_bits + 1;
    return if (rank_value > max_rank) max_rank else rank_value;
}

fn floorWindow(now_ns: i128, window_ns: i128) i128 {
    const normalized = if (now_ns < 0) 0 else now_ns;
    return @divFloor(normalized, window_ns) * window_ns;
}

fn hashFieldValue(seed: u64, field: []const u8, value: []const u8) u64 {
    const field_hash = std.hash.Wyhash.hash(seed ^ 0x9e37_79b9_7f4a_7c15, field);
    return std.hash.Wyhash.hash(field_hash, value);
}

fn renderReplacement(prefix: []const u8, field: []const u8, hash: u64, out: []u8) ![]const u8 {
    var stream = std.io.fixedBufferStream(out);
    const writer = stream.writer();
    try writer.writeAll(prefix);
    try writer.writeByte('_');
    try writeSafeToken(writer, field);
    try writer.writeByte('_');
    try writer.print("{x:0>16}", .{hash});
    return out[0..stream.pos];
}

fn writeSafeToken(writer: anytype, token: []const u8) !void {
    for (token) |byte| {
        if (isTokenByte(byte)) {
            try writer.writeByte(byte);
        } else {
            try writer.writeByte('_');
        }
    }
}

fn isTokenByte(byte: u8) bool {
    return (byte >= 'a' and byte <= 'z') or
        (byte >= 'A' and byte <= 'Z') or
        (byte >= '0' and byte <= '9') or
        byte == '_' or
        byte == '-';
}

fn writePrometheusLabel(writer: anytype, label: []const u8) !void {
    for (label) |byte| {
        switch (byte) {
            '\\' => try writer.writeAll("\\\\"),
            '"' => try writer.writeAll("\\\""),
            '\n' => try writer.writeAll("\\n"),
            else => try writer.writeByte(byte),
        }
    }
}

test "keeps known values and bucketizes only new overflow values" {
    var circuit = try CardinalityCircuit.init(std.testing.allocator, .{ .window_ns = 60 * std.time.ns_per_s, .seed = 7 });
    defer circuit.deinit();

    try circuit.addPolicy(.{ .field = "tenant", .max_distinct = 2, .precision = 6 });

    var replacement: [128]u8 = undefined;
    var obs = try circuit.observe(1, "tenant", "alpha", &replacement);
    try std.testing.expectEqual(Decision.keep, obs.decision);

    obs = try circuit.observe(2, "tenant", "beta", &replacement);
    try std.testing.expectEqual(Decision.keep, obs.decision);

    obs = try circuit.observe(3, "tenant", "alpha", &replacement);
    try std.testing.expectEqual(Decision.keep, obs.decision);

    obs = try circuit.observe(4, "tenant", "gamma", &replacement);
    try std.testing.expectEqual(Decision.bucketize, obs.decision);
    try std.testing.expect(std.mem.startsWith(u8, obs.value, "other_tenant_"));
}

test "window rotation clears the exact allow list" {
    var circuit = try CardinalityCircuit.init(std.testing.allocator, .{ .window_ns = 10, .seed = 9 });
    defer circuit.deinit();

    try circuit.addPolicy(.{ .field = "model", .max_distinct = 1, .precision = 5 });

    var replacement: [128]u8 = undefined;
    _ = try circuit.observe(1, "model", "small", &replacement);
    var obs = try circuit.observe(2, "model", "large", &replacement);
    try std.testing.expectEqual(Decision.bucketize, obs.decision);

    obs = try circuit.observe(11, "model", "large", &replacement);
    try std.testing.expectEqual(Decision.keep, obs.decision);
}

/*
This solves the OpenTelemetry cardinality limiter problem that shows up in AI gateway metrics, agent runtime traces, MCP tool logs, edge inference routers, and Prometheus labels when one tenant, prompt hash, tool name, route, model variant, or request id creates millions of unique values. Built because in April 2026 the hard part is not collecting telemetry, it is keeping useful telemetry without letting a single noisy label break the bill, the dashboard, or the on-call query path. Use it when you need a small Zig library that can sit inside an edge collector, sidecar, model gateway, inference proxy, or streaming DevOps worker and decide whether a label value should stay, be bucketized, or be dropped. The trick: it keeps an exact allow list only up to the budget, uses a compact HyperLogLog sketch to measure real pressure, rotates by time window, and emits Prometheus counters so the operator can see what was protected. Drop this into production code that handles AI infrastructure observability, high-cardinality log cleanup, model serving metrics, OpenTelemetry processors, Prometheus cost control, edge compute telemetry, or multi-tenant developer platform monitoring where label explosion is a real incident pattern, not a theory.
*/
