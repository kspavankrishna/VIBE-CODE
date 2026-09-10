//! Zero-allocation, priority-aware backpressure ring for edge/IoT telemetry uplinks,
//! plus a byte-at-a-time CRC-framed decoder for feeding it from an interrupt-driven
//! UART/BLE/LoRa receive path. See the explanation block at the end of this file.

const std = @import("std");
const testing = std.testing;

/// Delivery importance for a telemetry reading. Ordered low -> critical so that
/// `@intFromEnum` comparisons double as priority comparisons.
pub const Priority = enum(u2) {
    low = 0,
    normal = 1,
    high = 2,
    critical = 3,
};

/// What happened when a caller tried to push a reading into the ring.
pub const PushOutcome = enum {
    /// Stored in a free slot.
    accepted,
    /// Merged into an existing same-sensor reading inside the coalesce window.
    coalesced,
    /// Ring was full; a strictly-lower-priority reading was dropped to make room.
    evicted_lower_priority,
    /// Ring was full and nothing of lower priority existed to evict; dropped.
    rejected_full,
};

/// Running counters for observability. Exported as plain data so callers can
/// ship it over their own metrics path (structured log line, CoAP, whatever).
pub const Stats = struct {
    pushed: u32 = 0,
    coalesced: u32 = 0,
    evicted: [4]u32 = .{ 0, 0, 0, 0 },
    rejected: u32 = 0,
    high_water_mark: usize = 0,
};

/// A single telemetry reading with a fixed, comptime-sized inline payload.
/// `max_payload` must fit in a u8 (<= 255) because the wire format carries the
/// payload length in one byte.
pub fn Frame(comptime max_payload: usize) type {
    comptime {
        if (max_payload > 255) {
            @compileError("Frame max_payload must be <= 255 (wire length field is a u8)");
        }
    }
    return struct {
        sensor_id: u16,
        /// Boot-relative milliseconds. See FrameDecoder for why this is only
        /// ever reconstructed from a 32-bit wire value.
        timestamp_ms: u64,
        priority: Priority,
        payload_len: u8 = 0,
        payload: [max_payload]u8 = [_]u8{0} ** max_payload,

        const Self = @This();
        pub const Error = error{PayloadTooLarge};

        pub fn init(sensor_id: u16, timestamp_ms: u64, priority: Priority, payload: []const u8) Error!Self {
            if (payload.len > max_payload) return Error.PayloadTooLarge;
            var f = Self{
                .sensor_id = sensor_id,
                .timestamp_ms = timestamp_ms,
                .priority = priority,
                .payload_len = @intCast(payload.len),
            };
            @memcpy(f.payload[0..payload.len], payload);
            return f;
        }

        pub fn payloadSlice(self: *const Self) []const u8 {
            return self.payload[0..self.payload_len];
        }
    };
}

/// A fixed-capacity, allocation-free, priority-aware ring for buffering
/// telemetry while an uplink is down or congested.
///
/// Design note on complexity: push/pop/coalesce scan is O(capacity), not
/// O(1) or O(log n). A binary heap keyed on priority would give O(log n)
/// eviction, but it needs pointer-stable storage and a comparator that also
/// has to account for sensor-id coalescing, which turns into a second
/// index structure. On a microcontroller with a ring sized in the tens to
/// low hundreds of slots, a linear scan over inline, cache-local memory
/// with zero allocation and zero indirection beats a heap in both code
/// size and worst-case latency. Past a few hundred slots this trade-off
/// flips; this type is intentionally not meant to scale past that.
pub fn BackpressureRing(comptime capacity: usize, comptime max_payload: usize) type {
    comptime {
        if (capacity == 0) @compileError("BackpressureRing capacity must be greater than zero");
    }
    return struct {
        const Self = @This();
        pub const FrameT = Frame(max_payload);

        const Slot = struct {
            frame: FrameT = undefined,
            seq: u64 = 0,
            occupied: bool = false,
        };

        slots: [capacity]Slot = [_]Slot{.{}} ** capacity,
        count: usize = 0,
        next_seq: u64 = 0,
        /// Two readings from the same sensor within this many milliseconds
        /// are merged into one slot (latest value wins) instead of taking a
        /// second slot. This is what actually saves radio wake-ups on a
        /// chatty sensor, not the eviction policy.
        coalesce_window_ms: u64,
        stats: Stats = .{},

        pub fn init(coalesce_window_ms: u64) Self {
            return .{ .coalesce_window_ms = coalesce_window_ms };
        }

        fn findCoalesceCandidate(self: *Self, sensor_id: u16, timestamp_ms: u64) ?usize {
            for (self.slots, 0..) |slot, i| {
                if (!slot.occupied or slot.frame.sensor_id != sensor_id) continue;
                const existing_ts = slot.frame.timestamp_ms;
                const delta = if (timestamp_ms >= existing_ts) timestamp_ms - existing_ts else existing_ts - timestamp_ms;
                if (delta <= self.coalesce_window_ms) return i;
            }
            return null;
        }

        fn findFreeSlot(self: *Self) ?usize {
            for (self.slots, 0..) |slot, i| {
                if (!slot.occupied) return i;
            }
            return null;
        }

        /// Picks the occupied slot with strictly lower priority than
        /// `incoming`; ties broken by oldest sequence number. Returns null
        /// if nothing is evictable (everything is >= incoming priority).
        fn findEvictionVictim(self: *Self, incoming: Priority) ?usize {
            var victim: ?usize = null;
            var victim_priority: Priority = .critical;
            var victim_seq: u64 = std.math.maxInt(u64);
            for (self.slots, 0..) |slot, i| {
                if (!slot.occupied) continue;
                const p = slot.frame.priority;
                if (@intFromEnum(p) >= @intFromEnum(incoming)) continue;
                const better = victim == null or
                    @intFromEnum(p) < @intFromEnum(victim_priority) or
                    (p == victim_priority and slot.seq < victim_seq);
                if (better) {
                    victim = i;
                    victim_priority = p;
                    victim_seq = slot.seq;
                }
            }
            return victim;
        }

        fn oldestOccupied(self: *Self) ?usize {
            var best: ?usize = null;
            var best_seq: u64 = std.math.maxInt(u64);
            for (self.slots, 0..) |slot, i| {
                if (slot.occupied and slot.seq < best_seq) {
                    best = i;
                    best_seq = slot.seq;
                }
            }
            return best;
        }

        pub fn push(self: *Self, frame: FrameT) PushOutcome {
            if (self.findCoalesceCandidate(frame.sensor_id, frame.timestamp_ms)) |idx| {
                self.slots[idx].frame = frame;
                self.stats.coalesced += 1;
                return .coalesced;
            }

            if (self.findFreeSlot()) |idx| {
                self.slots[idx] = .{ .frame = frame, .seq = self.next_seq, .occupied = true };
                self.next_seq += 1;
                self.count += 1;
                self.stats.pushed += 1;
                if (self.count > self.stats.high_water_mark) self.stats.high_water_mark = self.count;
                return .accepted;
            }

            if (self.findEvictionVictim(frame.priority)) |idx| {
                const dropped_priority = self.slots[idx].frame.priority;
                self.stats.evicted[@as(usize, @intFromEnum(dropped_priority))] += 1;
                self.slots[idx] = .{ .frame = frame, .seq = self.next_seq, .occupied = true };
                self.next_seq += 1;
                self.stats.pushed += 1;
                return .evicted_lower_priority;
            }

            self.stats.rejected += 1;
            return .rejected_full;
        }

        /// Pops the oldest occupied reading (insertion order, holes skipped).
        pub fn pop(self: *Self) ?FrameT {
            const idx = self.oldestOccupied() orelse return null;
            const frame = self.slots[idx].frame;
            self.slots[idx].occupied = false;
            self.count -= 1;
            return frame;
        }

        /// Drains up to `out.len` readings, oldest first. Returns how many
        /// were written. Intended for a periodic uplink task that wakes up,
        /// grabs a batch, and ships it in one radio transaction.
        pub fn popBatch(self: *Self, out: []FrameT) usize {
            var n: usize = 0;
            while (n < out.len) : (n += 1) {
                out[n] = self.pop() orelse break;
            }
            return n;
        }

        pub fn len(self: *const Self) usize {
            return self.count;
        }

        pub fn isFull(self: *const Self) bool {
            return self.count == capacity;
        }
    };
}

/// CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF), computed bitwise. No
/// lookup table on purpose: a 256-entry table is 512 bytes of flash that a
/// tiny MCU building this file freestanding may not want to spend just to
/// checksum a few dozen bytes per frame.
fn crc16Ccitt(data: []const u8) u16 {
    var crc: u16 = 0xFFFF;
    for (data) |byte| {
        crc ^= @as(u16, byte) << 8;
        var i: u8 = 0;
        while (i < 8) : (i += 1) {
            if (crc & 0x8000 != 0) {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc = crc << 1;
            }
        }
    }
    return crc;
}

const SYNC0: u8 = 0xAA;
const SYNC1: u8 = 0x55;
/// sensor_id(2) + timestamp_wire(4) + priority(1) + payload_len(1)
const HEADER_LEN: usize = 8;

pub const EncodeError = error{BufferTooSmall};

/// Serializes a Frame to the wire format consumed by FrameDecoder:
///   [0xAA][0x55][sensor_id LE u16][timestamp_ms LE u32][priority u8]
///   [payload_len u8][payload...][crc16 BE u16]
///
/// The timestamp is truncated to 32 bits on the wire (about 49.7 days of
/// range at millisecond resolution) because that is what a boot-relative
/// clock on a battery-powered sensor actually needs; the decoder widens it
/// back into a u64 and leaves epoch reconciliation to the receiver, which
/// is the side that actually knows wall-clock time.
pub fn encodeFrame(comptime max_payload: usize, frame: Frame(max_payload), out: []u8) EncodeError!usize {
    const crc_region_len = 2 + HEADER_LEN + frame.payload_len;
    const total = crc_region_len + 2;
    if (out.len < total) return EncodeError.BufferTooSmall;

    var buf: [2 + HEADER_LEN + max_payload]u8 = undefined;
    buf[0] = SYNC0;
    buf[1] = SYNC1;
    buf[2] = @truncate(frame.sensor_id);
    buf[3] = @truncate(frame.sensor_id >> 8);
    buf[4] = @truncate(frame.timestamp_ms);
    buf[5] = @truncate(frame.timestamp_ms >> 8);
    buf[6] = @truncate(frame.timestamp_ms >> 16);
    buf[7] = @truncate(frame.timestamp_ms >> 24);
    buf[8] = @as(u8, @intFromEnum(frame.priority));
    buf[9] = frame.payload_len;
    @memcpy(buf[10 .. 10 + frame.payload_len], frame.payload[0..frame.payload_len]);

    const crc = crc16Ccitt(buf[0..crc_region_len]);
    @memcpy(out[0..crc_region_len], buf[0..crc_region_len]);
    out[crc_region_len] = @truncate(crc >> 8);
    out[crc_region_len + 1] = @truncate(crc);

    return total;
}

/// Streaming, single-byte-at-a-time decoder for the FrameDecoder wire
/// format. Built for feeding from a UART RX interrupt or a BLE notify
/// callback one byte (or a small chunk) at a time, with no allocation and
/// no requirement that a whole frame arrive contiguously.
pub fn FrameDecoder(comptime max_payload: usize) type {
    comptime {
        if (max_payload > 255) {
            @compileError("FrameDecoder max_payload must be <= 255 (wire length field is a u8)");
        }
    }
    return struct {
        const Self = @This();
        pub const FrameT = Frame(max_payload);

        pub const Event = union(enum) {
            need_more,
            frame: FrameT,
            /// Frame boundaries were found but the checksum didn't match;
            /// the byte that produces this already resynced internally, so
            /// the caller can just keep feeding.
            crc_mismatch,
        };

        const State = enum { sync0, sync1, header, payload, crc0, crc1 };

        state: State = .sync0,
        header_buf: [HEADER_LEN]u8 = undefined,
        header_pos: usize = 0,
        payload_buf: [max_payload]u8 = undefined,
        payload_pos: usize = 0,
        payload_len: u8 = 0,
        crc_buf: [2]u8 = undefined,
        running: [2 + HEADER_LEN + max_payload]u8 = undefined,
        running_len: usize = 0,

        pub fn init() Self {
            return .{};
        }

        fn resetToSync(self: *Self) void {
            self.state = .sync0;
            self.header_pos = 0;
            self.payload_pos = 0;
            self.running_len = 0;
        }

        pub fn feed(self: *Self, byte: u8) Event {
            switch (self.state) {
                .sync0 => {
                    if (byte == SYNC0) {
                        self.resetToSync();
                        self.running[0] = byte;
                        self.running_len = 1;
                        self.state = .sync1;
                    }
                    return .need_more;
                },
                .sync1 => {
                    if (byte == SYNC1) {
                        self.running[self.running_len] = byte;
                        self.running_len += 1;
                        self.state = .header;
                    } else if (byte == SYNC0) {
                        // Stay resynchronizable: this byte could itself be
                        // the start of the real sync sequence.
                        self.running[0] = byte;
                        self.running_len = 1;
                    } else {
                        self.state = .sync0;
                    }
                    return .need_more;
                },
                .header => {
                    self.header_buf[self.header_pos] = byte;
                    self.running[self.running_len] = byte;
                    self.running_len += 1;
                    self.header_pos += 1;
                    if (self.header_pos == HEADER_LEN) {
                        self.payload_len = self.header_buf[7];
                        if (self.payload_len == 0) {
                            self.state = .crc0;
                        } else if (self.payload_len > max_payload) {
                            self.state = .sync0;
                        } else {
                            self.state = .payload;
                        }
                    }
                    return .need_more;
                },
                .payload => {
                    self.payload_buf[self.payload_pos] = byte;
                    self.running[self.running_len] = byte;
                    self.running_len += 1;
                    self.payload_pos += 1;
                    if (self.payload_pos == self.payload_len) {
                        self.state = .crc0;
                    }
                    return .need_more;
                },
                .crc0 => {
                    self.crc_buf[0] = byte;
                    self.state = .crc1;
                    return .need_more;
                },
                .crc1 => {
                    self.crc_buf[1] = byte;
                    self.state = .sync0;

                    const received: u16 = (@as(u16, self.crc_buf[0]) << 8) | @as(u16, self.crc_buf[1]);
                    const computed = crc16Ccitt(self.running[0..self.running_len]);
                    if (received != computed) return .crc_mismatch;

                    const sensor_id: u16 = (@as(u16, self.header_buf[1]) << 8) | @as(u16, self.header_buf[0]);
                    const timestamp_ms: u64 = (@as(u64, self.header_buf[5]) << 24) |
                        (@as(u64, self.header_buf[4]) << 16) |
                        (@as(u64, self.header_buf[3]) << 8) |
                        @as(u64, self.header_buf[2]);
                    const priority: Priority = @enumFromInt(@as(u2, @truncate(self.header_buf[6])));

                    var f = FrameT{
                        .sensor_id = sensor_id,
                        .timestamp_ms = timestamp_ms,
                        .priority = priority,
                        .payload_len = self.payload_len,
                    };
                    @memcpy(f.payload[0..self.payload_len], self.payload_buf[0..self.payload_len]);
                    return .{ .frame = f };
                },
            }
        }
    };
}

test "push/pop preserves insertion order" {
    var ring = BackpressureRing(4, 8).init(0);
    _ = ring.push(try Frame(8).init(1, 100, .normal, "a"));
    _ = ring.push(try Frame(8).init(2, 200, .normal, "b"));
    _ = ring.push(try Frame(8).init(3, 300, .normal, "c"));

    const first = ring.pop() orelse return error.TestUnexpectedResult;
    const second = ring.pop() orelse return error.TestUnexpectedResult;
    const third = ring.pop() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 1), first.sensor_id);
    try testing.expectEqual(@as(u16, 2), second.sensor_id);
    try testing.expectEqual(@as(u16, 3), third.sensor_id);
    try testing.expectEqual(@as(usize, 0), ring.len());
}

test "readings from the same sensor coalesce inside the window" {
    var ring = BackpressureRing(4, 8).init(50);
    _ = ring.push(try Frame(8).init(7, 1000, .normal, "v1"));
    const outcome = ring.push(try Frame(8).init(7, 1030, .normal, "v2"));

    try testing.expectEqual(PushOutcome.coalesced, outcome);
    try testing.expectEqual(@as(usize, 1), ring.len());
    try testing.expectEqual(@as(u32, 1), ring.stats.coalesced);

    const only = ring.pop() orelse return error.TestUnexpectedResult;
    try testing.expectEqualSlices(u8, "v2", only.payloadSlice());
}

test "full ring evicts strictly lower priority, never equal or higher" {
    var ring = BackpressureRing(2, 4).init(0);
    _ = ring.push(try Frame(4).init(1, 0, .low, "x"));
    _ = ring.push(try Frame(4).init(2, 0, .normal, "y"));
    try testing.expect(ring.isFull());

    const evicted = ring.push(try Frame(4).init(3, 0, .critical, "z"));
    try testing.expectEqual(PushOutcome.evicted_lower_priority, evicted);
    try testing.expectEqual(@as(u32, 1), ring.stats.evicted[@as(usize, @intFromEnum(Priority.low))]);

    const rejected = ring.push(try Frame(4).init(4, 0, .normal, "w"));
    try testing.expectEqual(PushOutcome.rejected_full, rejected);
    try testing.expectEqual(@as(u32, 1), ring.stats.rejected);
}

test "high water mark tracks peak occupancy, not current occupancy" {
    var ring = BackpressureRing(3, 4).init(0);
    _ = ring.push(try Frame(4).init(1, 0, .normal, "a"));
    _ = ring.push(try Frame(4).init(2, 0, .normal, "b"));
    _ = ring.push(try Frame(4).init(3, 0, .normal, "c"));
    _ = ring.pop();
    try testing.expectEqual(@as(usize, 3), ring.stats.high_water_mark);
    try testing.expectEqual(@as(usize, 2), ring.len());
}

test "encode -> decode round trip reconstructs the frame" {
    const original = try Frame(8).init(42, 123456, .high, "hello!");
    var wire: [64]u8 = undefined;
    const n = try encodeFrame(8, original, &wire);

    var decoder = FrameDecoder(8).init();
    var decoded: ?Frame(8) = null;
    for (wire[0..n]) |b| {
        switch (decoder.feed(b)) {
            .frame => |f| decoded = f,
            .crc_mismatch => return error.TestUnexpectedResult,
            .need_more => {},
        }
    }

    const f = decoded orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 42), f.sensor_id);
    try testing.expectEqual(@as(u64, 123456), f.timestamp_ms);
    try testing.expectEqual(Priority.high, f.priority);
    try testing.expectEqualSlices(u8, "hello!", f.payloadSlice());
}

test "a corrupted payload byte is caught by the checksum" {
    const original = try Frame(8).init(1, 0, .normal, "abcdef");
    var wire: [64]u8 = undefined;
    const n = try encodeFrame(8, original, &wire);
    wire[12] ^= 0xFF; // flip a payload byte in place

    var decoder = FrameDecoder(8).init();
    var saw_mismatch = false;
    for (wire[0..n]) |b| {
        switch (decoder.feed(b)) {
            .crc_mismatch => saw_mismatch = true,
            else => {},
        }
    }
    try testing.expect(saw_mismatch);
}

test "leading garbage bytes on the wire do not desync the decoder" {
    const original = try Frame(8).init(9, 5000, .critical, "ok");
    var wire: [64]u8 = undefined;
    const n = try encodeFrame(8, original, &wire);

    var decoder = FrameDecoder(8).init();
    var decoded: ?Frame(8) = null;
    for ([_]u8{ 0x00, 0xFF, 0x12, 0x34 }) |garbage| {
        _ = decoder.feed(garbage);
    }
    for (wire[0..n]) |b| {
        switch (decoder.feed(b)) {
            .frame => |f| decoded = f,
            else => {},
        }
    }
    const f = decoded orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 9), f.sensor_id);
}

test "outage simulation: ring stays bounded while offline, drains in order on recovery" {
    var ring = BackpressureRing(8, 4).init(0);
    var sensor_id: u16 = 0;
    while (sensor_id < 20) : (sensor_id += 1) {
        _ = ring.push(try Frame(4).init(sensor_id, @as(u64, sensor_id), .normal, "d"));
    }
    try testing.expect(ring.isFull());
    try testing.expectEqual(@as(usize, 8), ring.stats.high_water_mark);

    var out: [8]Frame(4) = undefined;
    const drained = ring.popBatch(&out);
    try testing.expectEqual(@as(usize, 8), drained);
    try testing.expectEqual(@as(usize, 0), ring.len());
    // All 20 pushes are equal priority (.normal), so eviction never finds a
    // strictly-lower-priority victim: the first 8 fill the ring and the
    // remaining 12 are rejected outright. That means the ring keeps the
    // *oldest* readings under sustained equal-priority pressure, not the
    // newest -- worth knowing before you rely on this for a "give me the
    // latest N" use case instead of "don't lose the first N since the
    // outage started".
    try testing.expectEqual(@as(u32, 12), ring.stats.rejected);
    var expected_sensor_id: u16 = 0;
    for (out[0..drained]) |f| {
        try testing.expectEqual(expected_sensor_id, f.sensor_id);
        expected_sensor_id += 1;
    }
}

// ---------------------------------------------------------------------------
// This solves the problem every battery-powered sensor node hits the moment
// it leaves a lab bench: the uplink (LoRa, BLE, patchy cellular, a serial
// link to a gateway that reboots) goes away for seconds or hours, readings
// keep arriving, and a plain FIFO queue either grows without bound or starts
// dropping whatever happens to be oldest, critical alarms included. This
// file is a fixed-size, zero-heap-allocation ring that instead drops by
// priority, merges repeat readings from the same sensor so a chatty
// accelerometer doesn't crowd out a once-a-minute battery-voltage report,
// and ships a matching streaming decoder so the far end can rebuild frames
// one byte at a time straight out of a UART interrupt, with a CRC16 catching
// line noise before it corrupts your data instead of after.
//
// Built because most "message queue" code either assumes a heap and an OS
// (fine on a gateway, not on the MCU actually touching the sensor) or is a
// toy ring buffer that has no idea what to do when it fills up other than
// drop the head. Priority-aware eviction plus time-window coalescing is the
// part that actually took thought: get both wrong and you either lose the
// alarm that mattered or you burn the radio waking up for near-duplicate
// noise.
//
// Use it when you're writing firmware or an edge agent in Zig that has to
// buffer telemetry locally across an unreliable link: environmental sensor
// nodes, industrial IoT gateways, drone telemetry, vehicle CAN-to-cloud
// bridges, anywhere "the network might just not be there right now" is a
// normal operating condition instead of an edge case.
//
// The trick: instead of a classic head/tail circular buffer (which gets
// gnarly the moment you need to evict from the middle), every slot carries
// its own monotonic sequence number, so push/pop/evict/coalesce are all
// plain linear scans over one flat array. That is O(capacity) instead of
// O(1), and that trade is deliberate: on the ring sizes this is meant for
// (tens to low hundreds of slots on a microcontroller) a scan over
// contiguous, cache-local memory with zero pointers and zero allocation
// beats a "proper" priority queue in both code size and worst-case latency,
// and there is no allocator to fail when the device has been running for
// six months straight.
//
// Drop this into any Zig project (hosted or freestanding/embedded, it never
// touches an allocator or the OS) as a single file: instantiate
// BackpressureRing(capacity, max_payload) on the sensor node to buffer
// readings, and FrameDecoder(max_payload) on the receiving gateway to
// stream-decode whatever the node ships over the wire. Run `zig test
// EdgeTelemetryBackpressureRing.zig` to see the whole thing exercised,
// including a simulated multi-hour outage and a corrupted-frame scenario.
