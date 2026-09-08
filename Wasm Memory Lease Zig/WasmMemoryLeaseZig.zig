const std = @import("std");

const Lease = struct {
    tenant: []const u8,
    component: []const u8,
    requested: u64,
    priority: u8,
    ttl_ms: u64,
    granted: u64 = 0,

    fn score(self: Lease) u128 {
        const p: u128 = @as(u128, self.priority) + 1;
        const ttl_penalty: u128 = @max(@as(u128, self.ttl_ms), 1);
        return p * 1_000_000_000 / ttl_penalty;
    }
};

const Options = struct {
    capacity: u64 = 128 * 1024 * 1024,
    reserve: u64 = 8 * 1024 * 1024,
    min_grant: u64 = 64 * 1024,
    json: bool = false,
};

fn parseU64(raw: []const u8) !u64 {
    return std.fmt.parseUnsigned(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
}

fn parseU8(raw: []const u8) !u8 {
    const value = try parseU64(raw);
    if (value > 255) return error.PriorityTooLarge;
    return @as(u8, @intCast(value));
}

fn parseOptions(allocator: std.mem.Allocator) !Options {
    var opts = Options{};
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--capacity")) {
            opts.capacity = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--reserve")) {
            opts.reserve = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--min-grant")) {
            opts.min_grant = try parseU64(args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, arg, "--json")) {
            opts.json = true;
        } else {
            return error.UnknownOption;
        }
    }
    if (opts.reserve >= opts.capacity) return error.ReserveExceedsCapacity;
    return opts;
}

fn parseLease(allocator: std.mem.Allocator, line: []const u8) !Lease {
    var cells = std.mem.splitScalar(u8, line, ',');
    const tenant = std.mem.trim(u8, cells.next() orelse return error.BadRow, " \t\r\n");
    const component = std.mem.trim(u8, cells.next() orelse return error.BadRow, " \t\r\n");
    const requested = try parseU64(cells.next() orelse return error.BadRow);
    const priority = try parseU8(cells.next() orelse return error.BadRow);
    const ttl = try parseU64(cells.next() orelse return error.BadRow);
    return Lease{
        .tenant = try allocator.dupe(u8, tenant),
        .component = try allocator.dupe(u8, component),
        .requested = requested,
        .priority = priority,
        .ttl_ms = ttl,
    };
}

fn lessImportant(_: void, a: Lease, b: Lease) bool {
    if (a.score() == b.score()) return a.requested < b.requested;
    return a.score() > b.score();
}

fn plan(leases: []Lease, options: Options) void {
    std.sort.pdq(Lease, leases, {}, lessImportant);
    var remaining = options.capacity - options.reserve;
    for (leases) |*lease| {
        if (remaining == 0) break;
        const grant = @min(lease.requested, remaining);
        if (grant >= options.min_grant or lease.priority >= 200) {
            lease.granted = grant;
            remaining -= grant;
        }
    }
}

fn printText(leases: []const Lease, writer: anytype) !void {
    try writer.writeAll("tenant\tcomponent\trequested\tgranted\tpriority\tttl_ms\n");
    for (leases) |lease| {
        try writer.print("{s}\t{s}\t{}\t{}\t{}\t{}\n", .{ lease.tenant, lease.component, lease.requested, lease.granted, lease.priority, lease.ttl_ms });
    }
}

fn printJson(leases: []const Lease, writer: anytype) !void {
    try writer.writeAll("{\"leases\":[");
    for (leases, 0..) |lease, index| {
        if (index > 0) try writer.writeAll(",");
        try writer.print("{{\"tenant\":\"{s}\",\"component\":\"{s}\",\"requested\":{},\"granted\":{},\"priority\":{},\"ttl_ms\":{}}}", .{ lease.tenant, lease.component, lease.requested, lease.granted, lease.priority, lease.ttl_ms });
    }
    try writer.writeAll("]}\n");
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const options = try parseOptions(allocator);
    var leases = std.ArrayList(Lease).init(allocator);
    const stdin = std.io.getStdIn().reader();
    var buffer: [4096]u8 = undefined;
    while (try stdin.readUntilDelimiterOrEof(&buffer, '\n')) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r\n");
        if (trimmed.len == 0 or std.mem.startsWith(u8, trimmed, "tenant,")) continue;
        try leases.append(try parseLease(allocator, trimmed));
    }
    plan(leases.items, options);
    const stdout = std.io.getStdOut().writer();
    if (options.json) try printJson(leases.items, stdout) else try printText(leases.items, stdout);
}

// This solves the April 2026 edge WebAssembly memory problem where plugin sandboxes,
// AI tool runtimes, and customer extensions compete for tiny per-isolate memory budgets.
// Built because edge compute teams need a deterministic lease planner before a Wasm module
// is admitted, not after an out-of-memory trap has already dropped a request. Use it when
// a gateway, CDN worker, IoT hub, or serverless platform can emit tenant, component,
// requested bytes, priority, and TTL as CSV. The trick: it sorts leases by priority per TTL,
// reserves platform memory first, and refuses grants below a minimum useful size unless the
// caller is emergency priority. Drop this into a systems repository as a single Zig source
// file and it becomes a Wasm memory lease planner, edge sandbox capacity guard, AI plugin
// runtime admission tool, deterministic resource allocator, and practical DevOps utility
// that engineers can reason about from the command line.
