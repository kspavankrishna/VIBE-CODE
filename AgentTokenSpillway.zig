const std = @import("std");

const Decision = enum {
    accept,
    defer,
    reject,
};

const Reason = enum {
    none,
    residency_mismatch,
    context_window,
    tenant_budget,
    request_cost_cap,
    tenant_token_rate,
    tenant_request_rate,
    provider_token_rate,
    provider_request_rate,
    deadline_miss,
};

const Options = struct {
    tenant_token_capacity: u64 = 120_000,
    tenant_token_refill_per_min: u64 = 120_000,
    tenant_request_capacity: u64 = 120,
    tenant_request_refill_per_min: u64 = 120,
    provider_token_capacity: u64 = 900_000,
    provider_token_refill_per_min: u64 = 900_000,
    provider_request_capacity: u64 = 900,
    provider_request_refill_per_min: u64 = 900,
    max_context_tokens: u64 = 128_000,
    output_safety_ppm: u64 = 1_100_000,
    tool_call_tokens: u64 = 900,
    price_input_micros_per_1k: u64 = 150,
    price_output_micros_per_1k: u64 = 600,
    provider_p95_ms: u64 = 1_800,
    concurrency: u64 = 32,
    required_region: ?[]const u8 = null,
    json: bool = false,
};

const TenantState = struct {
    token_remaining: u64,
    request_remaining: u64,
    budget_remaining_micros: u64,
};

const WorkItem = struct {
    tenant: []const u8,
    request_id: []const u8,
    idempotency_key: []const u8,
    region: []const u8,
    prompt_tokens: u64,
    cache_hit_tokens: u64,
    max_output_tokens: u64,
    deadline_ms: u64,
    priority: u8,
    tenant_budget_micros: u64,
    cost_cap_micros: u64,
    tool_calls: u64,
    billable_input_tokens: u64 = 0,
    reserved_output_tokens: u64 = 0,
    reserved_total_tokens: u64 = 0,
    estimated_cost_micros: u64 = 0,
    queue_position: u64 = 0,
    retry_after_ms: u64 = 0,
    decision: Decision = .defer,
    reason: Reason = .none,
};

fn reasonText(reason: Reason) []const u8 {
    return switch (reason) {
        .none => "none",
        .residency_mismatch => "residency_mismatch",
        .context_window => "context_window",
        .tenant_budget => "tenant_budget",
        .request_cost_cap => "request_cost_cap",
        .tenant_token_rate => "tenant_token_rate",
        .tenant_request_rate => "tenant_request_rate",
        .provider_token_rate => "provider_token_rate",
        .provider_request_rate => "provider_request_rate",
        .deadline_miss => "deadline_miss",
    };
}

fn decisionText(decision: Decision) []const u8 {
    return switch (decision) {
        .accept => "accept",
        .defer => "defer",
        .reject => "reject",
    };
}

fn parseU64(raw: []const u8) !u64 {
    return std.fmt.parseUnsigned(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
}

fn parseU8(raw: []const u8) !u8 {
    const value = try parseU64(raw);
    if (value > 255) return error.PriorityTooLarge;
    return @as(u8, @intCast(value));
}

fn splitCsv(allocator: std.mem.Allocator, line: []const u8) ![][]const u8 {
    var fields = std.ArrayList([]const u8).init(allocator);
    var cell = std.ArrayList(u8).init(allocator);
    var quoted = false;
    var i: usize = 0;

    while (i < line.len) : (i += 1) {
        const ch = line[i];
        if (quoted) {
            if (ch == '"') {
                if (i + 1 < line.len and line[i + 1] == '"') {
                    try cell.append('"');
                    i += 1;
                } else {
                    quoted = false;
                }
            } else {
                try cell.append(ch);
            }
            continue;
        }

        if (ch == '"') {
            if (cell.items.len == 0 or onlySpace(cell.items)) {
                cell.clearRetainingCapacity();
                quoted = true;
            } else {
                try cell.append(ch);
            }
        } else if (ch == ',') {
            try fields.append(try allocator.dupe(u8, std.mem.trim(u8, cell.items, " \t\r\n")));
            cell.clearRetainingCapacity();
        } else {
            try cell.append(ch);
        }
    }

    if (quoted) return error.UnclosedQuote;
    try fields.append(try allocator.dupe(u8, std.mem.trim(u8, cell.items, " \t\r\n")));
    return fields.toOwnedSlice();
}

fn onlySpace(slice: []const u8) bool {
    for (slice) |ch| {
        if (ch != ' ' and ch != '\t') return false;
    }
    return true;
}

fn parseWorkItem(allocator: std.mem.Allocator, line: []const u8) !WorkItem {
    const fields = try splitCsv(allocator, line);
    if (fields.len != 12) return error.BadCsvFieldCount;
    return WorkItem{
        .tenant = fields[0],
        .request_id = fields[1],
        .idempotency_key = fields[2],
        .region = fields[3],
        .prompt_tokens = try parseU64(fields[4]),
        .cache_hit_tokens = try parseU64(fields[5]),
        .max_output_tokens = try parseU64(fields[6]),
        .deadline_ms = try parseU64(fields[7]),
        .priority = try parseU8(fields[8]),
        .tenant_budget_micros = try parseU64(fields[9]),
        .cost_cap_micros = try parseU64(fields[10]),
        .tool_calls = try parseU64(fields[11]),
    };
}

fn parseOptions(allocator: std.mem.Allocator) !Options {
    var options = Options{};
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--tenant-token-capacity")) {
            options.tenant_token_capacity = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--tenant-token-refill-per-min")) {
            options.tenant_token_refill_per_min = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--tenant-request-capacity")) {
            options.tenant_request_capacity = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--tenant-request-refill-per-min")) {
            options.tenant_request_refill_per_min = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--provider-token-capacity")) {
            options.provider_token_capacity = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--provider-token-refill-per-min")) {
            options.provider_token_refill_per_min = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--provider-request-capacity")) {
            options.provider_request_capacity = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--provider-request-refill-per-min")) {
            options.provider_request_refill_per_min = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--max-context")) {
            options.max_context_tokens = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--output-safety-ppm")) {
            options.output_safety_ppm = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--tool-call-tokens")) {
            options.tool_call_tokens = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--price-input-micros-per-1k")) {
            options.price_input_micros_per_1k = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--price-output-micros-per-1k")) {
            options.price_output_micros_per_1k = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--provider-p95-ms")) {
            options.provider_p95_ms = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--concurrency")) {
            options.concurrency = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--required-region")) {
            options.required_region = try allocator.dupe(u8, args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--json")) {
            options.json = true;
        } else if (std.mem.eql(u8, arg, "--help")) {
            return error.HelpRequested;
        } else {
            return error.UnknownOption;
        }
    }

    if (options.concurrency == 0) return error.BadConcurrency;
    if (options.output_safety_ppm == 0) return error.BadSafetyMargin;
    return options;
}

fn ceilMulDiv(value: u64, multiplier: u64, divisor: u64) u64 {
    if (divisor == 0) return std.math.maxInt(u64);
    const numerator = @as(u128, value) * @as(u128, multiplier);
    const div = @as(u128, divisor);
    const quotient = (numerator + div - 1) / div;
    if (quotient > std.math.maxInt(u64)) return std.math.maxInt(u64);
    return @as(u64, @intCast(quotient));
}

fn addSaturated(a: u64, b: u64) u64 {
    const sum = @as(u128, a) + @as(u128, b);
    if (sum > std.math.maxInt(u64)) return std.math.maxInt(u64);
    return @as(u64, @intCast(sum));
}

fn estimate(item: *WorkItem, options: Options) void {
    const cache_hit = @min(item.cache_hit_tokens, item.prompt_tokens);
    const uncached_prompt = item.prompt_tokens - cache_hit;
    const tool_tokens = ceilMulDiv(item.tool_calls, options.tool_call_tokens, 1);
    item.billable_input_tokens = addSaturated(uncached_prompt, tool_tokens);
    item.reserved_output_tokens = ceilMulDiv(item.max_output_tokens, options.output_safety_ppm, 1_000_000);
    item.reserved_total_tokens = addSaturated(addSaturated(item.prompt_tokens, tool_tokens), item.reserved_output_tokens);
    const input_cost = ceilMulDiv(item.billable_input_tokens, options.price_input_micros_per_1k, 1_000);
    const output_cost = ceilMulDiv(item.reserved_output_tokens, options.price_output_micros_per_1k, 1_000);
    item.estimated_cost_micros = addSaturated(input_cost, output_cost);
}

fn moreUrgent(_: void, a: WorkItem, b: WorkItem) bool {
    if (a.priority != b.priority) return a.priority > b.priority;
    const a_deadline = if (a.deadline_ms == 0) std.math.maxInt(u64) else a.deadline_ms;
    const b_deadline = if (b.deadline_ms == 0) std.math.maxInt(u64) else b.deadline_ms;
    if (a_deadline != b_deadline) return a_deadline < b_deadline;
    if (a.estimated_cost_micros != b.estimated_cost_micros) return a.estimated_cost_micros < b.estimated_cost_micros;
    if (!std.mem.eql(u8, a.tenant, b.tenant)) return std.mem.lessThan(u8, a.tenant, b.tenant);
    return std.mem.lessThan(u8, a.request_id, b.request_id);
}

fn seedTenants(allocator: std.mem.Allocator, items: []const WorkItem, options: Options) !std.StringHashMap(TenantState) {
    var tenants = std.StringHashMap(TenantState).init(allocator);
    for (items) |item| {
        var entry = try tenants.getOrPut(item.tenant);
        if (!entry.found_existing) {
            entry.value_ptr.* = TenantState{
                .token_remaining = options.tenant_token_capacity,
                .request_remaining = options.tenant_request_capacity,
                .budget_remaining_micros = item.tenant_budget_micros,
            };
        } else if (item.tenant_budget_micros > entry.value_ptr.budget_remaining_micros) {
            entry.value_ptr.budget_remaining_micros = item.tenant_budget_micros;
        }
    }
    return tenants;
}

fn retryForShortfall(shortfall: u64, refill_per_min: u64) u64 {
    if (shortfall == 0) return 0;
    if (refill_per_min == 0) return 60_000;
    return ceilMulDiv(shortfall, 60_000, refill_per_min);
}

fn queueWaitMs(accepted_before: u64, options: Options) u64 {
    const waves = accepted_before / options.concurrency;
    return ceilMulDiv(waves, options.provider_p95_ms, 1);
}

fn setReject(item: *WorkItem, reason: Reason) void {
    item.decision = .reject;
    item.reason = reason;
    item.retry_after_ms = 0;
}

fn setDefer(item: *WorkItem, reason: Reason, retry_after_ms: u64) void {
    item.decision = .defer;
    item.reason = reason;
    item.retry_after_ms = retry_after_ms;
}

fn runPlan(allocator: std.mem.Allocator, items: []WorkItem, options: Options) !void {
    for (items) |*item| estimate(item, options);
    var tenants = try seedTenants(allocator, items, options);
    defer tenants.deinit();
    std.sort.pdq(WorkItem, items, {}, moreUrgent);

    var provider_tokens_remaining = options.provider_token_capacity;
    var provider_requests_remaining = options.provider_request_capacity;
    var accepted: u64 = 0;

    for (items) |*item| {
        if (options.required_region) |region| {
            if (!std.mem.eql(u8, region, item.region)) {
                setReject(item, .residency_mismatch);
                continue;
            }
        }

        if (item.reserved_total_tokens > options.max_context_tokens) {
            setReject(item, .context_window);
            continue;
        }

        if (item.cost_cap_micros > 0 and item.estimated_cost_micros > item.cost_cap_micros) {
            setReject(item, .request_cost_cap);
            continue;
        }

        const tenant = tenants.getPtr(item.tenant) orelse return error.MissingTenantState;
        if (tenant.budget_remaining_micros < item.estimated_cost_micros) {
            setReject(item, .tenant_budget);
            continue;
        }

        if (tenant.token_remaining < item.reserved_total_tokens) {
            setDefer(item, .tenant_token_rate, retryForShortfall(item.reserved_total_tokens - tenant.token_remaining, options.tenant_token_refill_per_min));
            continue;
        }

        if (tenant.request_remaining == 0) {
            setDefer(item, .tenant_request_rate, retryForShortfall(1, options.tenant_request_refill_per_min));
            continue;
        }

        if (provider_tokens_remaining < item.reserved_total_tokens) {
            setDefer(item, .provider_token_rate, retryForShortfall(item.reserved_total_tokens - provider_tokens_remaining, options.provider_token_refill_per_min));
            continue;
        }

        if (provider_requests_remaining == 0) {
            setDefer(item, .provider_request_rate, retryForShortfall(1, options.provider_request_refill_per_min));
            continue;
        }

        const wait_ms = queueWaitMs(accepted, options);
        if (item.deadline_ms > 0 and addSaturated(wait_ms, options.provider_p95_ms) > item.deadline_ms) {
            setReject(item, .deadline_miss);
            continue;
        }

        item.decision = .accept;
        item.reason = .none;
        item.retry_after_ms = 0;
        item.queue_position = accepted;
        tenant.token_remaining -= item.reserved_total_tokens;
        tenant.request_remaining -= 1;
        tenant.budget_remaining_micros -= item.estimated_cost_micros;
        provider_tokens_remaining -= item.reserved_total_tokens;
        provider_requests_remaining -= 1;
        accepted += 1;
    }
}

fn writeEscapedJson(writer: anytype, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |ch| {
        switch (ch) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => try writer.writeByte(ch),
        }
    }
    try writer.writeByte('"');
}

fn writeHeaderString(item: WorkItem, writer: anytype) !void {
    try writer.print(
        "x-agent-decision={s};x-agent-reason={s};x-agent-retry-after-ms={};x-agent-cost-micros={};x-agent-reserved-tokens={};x-agent-queue-position={}",
        .{ decisionText(item.decision), reasonText(item.reason), item.retry_after_ms, item.estimated_cost_micros, item.reserved_total_tokens, item.queue_position },
    );
}

fn printText(items: []const WorkItem, writer: anytype) !void {
    try writer.writeAll("tenant\trequest_id\tidempotency_key\tdecision\treason\tcost_micros\treserved_tokens\tretry_after_ms\tqueue_position\tbackpressure_headers\n");
    for (items) |item| {
        try writer.print("{s}\t{s}\t{s}\t{s}\t{s}\t{}\t{}\t{}\t{}\t", .{
            item.tenant,
            item.request_id,
            item.idempotency_key,
            decisionText(item.decision),
            reasonText(item.reason),
            item.estimated_cost_micros,
            item.reserved_total_tokens,
            item.retry_after_ms,
            item.queue_position,
        });
        try writeHeaderString(item, writer);
        try writer.writeByte('\n');
    }
}

fn printJson(items: []const WorkItem, writer: anytype) !void {
    try writer.writeAll("{\"admissions\":[");
    for (items, 0..) |item, index| {
        if (index > 0) try writer.writeByte(',');
        try writer.writeAll("{\"tenant\":");
        try writeEscapedJson(writer, item.tenant);
        try writer.writeAll(",\"request_id\":");
        try writeEscapedJson(writer, item.request_id);
        try writer.writeAll(",\"idempotency_key\":");
        try writeEscapedJson(writer, item.idempotency_key);
        try writer.print(",\"decision\":\"{s}\",\"reason\":\"{s}\",\"cost_micros\":{},\"reserved_tokens\":{},\"retry_after_ms\":{},\"queue_position\":{},\"backpressure_headers\":\"", .{
            decisionText(item.decision),
            reasonText(item.reason),
            item.estimated_cost_micros,
            item.reserved_total_tokens,
            item.retry_after_ms,
            item.queue_position,
        });
        try writeHeaderString(item, writer);
        try writer.writeAll("\"}");
    }
    try writer.writeAll("]}\n");
}

fn isHeader(line: []const u8) bool {
    const trimmed = std.mem.trim(u8, line, " \t\r\n");
    return std.mem.startsWith(u8, trimmed, "tenant,");
}

fn usage(writer: anytype) !void {
    try writer.writeAll(
        \\usage: AgentTokenSpillway [options] < pending-agent-requests.csv
        \\
        \\csv columns:
        \\  tenant,request_id,idempotency_key,region,prompt_tokens,cache_hit_tokens,max_output_tokens,deadline_ms,priority,tenant_budget_micros,cost_cap_micros,tool_calls
        \\
        \\important options:
        \\  --json
        \\  --required-region us-east-1
        \\  --tenant-token-capacity 120000
        \\  --provider-token-capacity 900000
        \\  --max-context 128000
        \\  --price-input-micros-per-1k 150
        \\  --price-output-micros-per-1k 600
        \\
    );
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const options = parseOptions(allocator) catch |err| {
        if (err == error.HelpRequested) {
            try usage(std.io.getStdOut().writer());
            return;
        }
        return err;
    };

    var items = std.ArrayList(WorkItem).init(allocator);
    const stdin = std.io.getStdIn().reader();
    var buffer: [16 * 1024]u8 = undefined;
    while (try stdin.readUntilDelimiterOrEof(&buffer, '\n')) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r\n");
        if (trimmed.len == 0 or isHeader(trimmed)) continue;
        try items.append(try parseWorkItem(allocator, trimmed));
    }

    try runPlan(allocator, items.items, options);
    const stdout = std.io.getStdOut().writer();
    if (options.json) {
        try printJson(items.items, stdout);
    } else {
        try printText(items.items, stdout);
    }
}

test "planner accepts urgent work and protects tenant budget" {
    var items = [_]WorkItem{
        .{
            .tenant = "acme",
            .request_id = "r1",
            .idempotency_key = "idem-1",
            .region = "us-east-1",
            .prompt_tokens = 8_000,
            .cache_hit_tokens = 2_000,
            .max_output_tokens = 1_000,
            .deadline_ms = 4_000,
            .priority = 200,
            .tenant_budget_micros = 2_000,
            .cost_cap_micros = 0,
            .tool_calls = 1,
        },
        .{
            .tenant = "acme",
            .request_id = "r2",
            .idempotency_key = "idem-2",
            .region = "us-east-1",
            .prompt_tokens = 80_000,
            .cache_hit_tokens = 0,
            .max_output_tokens = 20_000,
            .deadline_ms = 4_000,
            .priority = 10,
            .tenant_budget_micros = 2_000,
            .cost_cap_micros = 0,
            .tool_calls = 2,
        },
    };
    var options = Options{};
    options.provider_token_capacity = 200_000;
    options.tenant_token_capacity = 200_000;
    try runPlan(std.testing.allocator, items[0..], options);
    try std.testing.expect(items[0].decision == .accept);
    try std.testing.expect(items[1].decision == .reject);
    try std.testing.expect(items[1].reason == .tenant_budget);
}

test "planner defers when provider token capacity is exhausted" {
    var items = [_]WorkItem{
        .{
            .tenant = "search",
            .request_id = "r1",
            .idempotency_key = "idem-a",
            .region = "eu-west-1",
            .prompt_tokens = 10_000,
            .cache_hit_tokens = 0,
            .max_output_tokens = 5_000,
            .deadline_ms = 0,
            .priority = 50,
            .tenant_budget_micros = 100_000,
            .cost_cap_micros = 0,
            .tool_calls = 0,
        },
    };
    var options = Options{};
    options.provider_token_capacity = 1_000;
    options.provider_token_refill_per_min = 60_000;
    try runPlan(std.testing.allocator, items[0..], options);
    try std.testing.expect(items[0].decision == .defer);
    try std.testing.expect(items[0].reason == .provider_token_rate);
    try std.testing.expect(items[0].retry_after_ms > 0);
}

test "csv parser keeps quoted commas and escaped quotes" {
    const fields = try splitCsv(std.testing.allocator, "\"acme, inc\",r1,\"key\"\"7\",us,1,0,1,0,1,100,0,0");
    defer {
        for (fields) |field| std.testing.allocator.free(field);
        std.testing.allocator.free(fields);
    }
    try std.testing.expectEqualStrings("acme, inc", fields[0]);
    try std.testing.expectEqualStrings("key\"7", fields[2]);
}

// This solves the April 2026 problem of AI gateway token budget admission control when
// agent systems, MCP tool calls, RAG searches, eval runners, and streaming LLM requests all
// hit the same provider quota at the same time. Built because I have seen teams trust the
// model provider rate limit headers too late, after they already accepted work, opened SSE
// streams, spent prompt cache budget, and made users wait behind requests that never had a
// chance to finish inside deadline or cost limits. Use it when you run a small inference
// gateway, internal developer platform, research eval queue, CI agent service, edge compute
// worker, or DevOps batch runner and need a deterministic answer for accept, defer, or reject
// before calling an expensive model. The trick: it reserves input tokens, cached prompt savings,
// output safety margin, tenant request limits, provider token capacity, region residency, context
// window size, per-request cost caps, tenant budget, and deadline pressure in one pass, then
// emits boring headers your proxy or queue can enforce. Drop this into a Zig repository or build
// it as a single command line tool for LLM backpressure, AI inference cost guardrails, MCP agent
// quota planning, provider rate limit scheduling, prompt cache aware routing, and production
// incident prevention without adding another service or database.
