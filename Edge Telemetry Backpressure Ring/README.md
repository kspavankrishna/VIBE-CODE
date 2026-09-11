# Edge Telemetry Backpressure Ring

A battery powered sensor node loses its uplink for an hour, readings keep arriving, and a plain FIFO either grows without bound or drops whatever is oldest, critical alarms included. This is a fixed size, zero allocation, priority aware ring in Zig that drops by priority instead, plus a CRC framed streaming decoder for the receiving end.

**Language:** Zig | **Lines:** 608 | **Added:** 2026-09-09

## What this solves

Every telemetry device that leaves the lab bench hits the same wall. The uplink is LoRa, BLE, patchy cellular or a serial link to a gateway that reboots on its own schedule, and it goes away for seconds or hours. The sensors do not stop. Readings pile up behind a link that is not there.

What usually happens next is one of two failure modes. The queue is heap backed and unbounded, so the device slowly consumes RAM until the allocator fails or the watchdog fires, six weeks into a deployment that nobody can physically reach. Or the queue is a toy circular buffer with a head and a tail, and when it fills up it overwrites the head. That second one looks safe until you read the data afterwards and discover the tank overflow alarm at minute three of the outage got overwritten by minute forty of ambient temperature readings that nobody will ever look at. The device was up, the buffer never overflowed in any way it reported, and the one reading that justified the whole installation is gone.

There is a quieter cost too. A chatty accelerometer sampling at 50 Hz will fill any buffer with near duplicate values and crowd out the once a minute battery voltage report. When the link comes back the node burns radio time, and therefore battery, shipping noise. Field engineers notice this as batteries that die months early for no visible reason.

On the receiving side, the failure is line noise. A flipped bit in a UART frame becomes a sensor reading that is silently wrong, and wrong data in a time series is worse than missing data because it looks real. You want the checksum to reject it at the framing layer, not have an analyst find it three weeks later.

## Why I built it

Most message queue code assumes a heap and an operating system. That is fine on a gateway and useless on the MCU actually wired to the sensor. The embedded alternatives are ring buffers that have no policy at all beyond dropping the head, and no notion that some readings matter more than others.

Priority aware eviction plus time window coalescing is the part that actually took thought. Get eviction wrong and you lose the alarm. Get coalescing wrong and you wake the radio for duplicates. The framing decoder is bundled because a ring that buffers frames is only half a system if the other end cannot rebuild them a byte at a time out of an interrupt handler.

## When to use it

- An environmental sensor node on LoRa where the gateway is out of range for stretches of the day
- An industrial IoT box that must not lose a fault code when the cellular modem drops
- Drone telemetry where link loss is expected on every flight and altitude or fault frames outrank routine status
- A vehicle CAN to cloud bridge buffering across tunnels and dead zones
- Any firmware where an allocator failure after six months of uptime is not an acceptable outcome
- A gateway side Zig process that has to stream decode frames arriving one byte at a time from a UART RX interrupt or a BLE notify callback

## How it works

The core type is `BackpressureRing(capacity, max_payload)`, a comptime generic over a flat `[capacity]Slot` array. Each `Slot` holds a `Frame`, a monotonic `seq` number and an `occupied` flag. The trick is that sequence number: instead of a classic head and tail circular buffer, which becomes awkward the moment you need to remove an element from the middle, ordering lives in the slot itself. That makes push, pop, evict and coalesce all plain linear scans over one contiguous array with no pointers and no indirection.

`push` runs three steps in order. First `findCoalesceCandidate` scans for an occupied slot with the same `sensor_id` whose `timestamp_ms` is within `coalesce_window_ms` in either direction. On a hit the slot's frame is overwritten in place, the slot keeps its original sequence number so queue position is preserved, and the result is `.coalesced`. Second, `findFreeSlot` takes the first unoccupied slot and returns `.accepted`. Third, if the ring is full, `findEvictionVictim` looks for the occupied slot with strictly lower priority than the incoming frame, breaking ties by oldest sequence number, and returns `.evicted_lower_priority`. If nothing is strictly lower the frame is dropped and you get `.rejected_full`. Priority is a `u2` backed enum ordered low, normal, high, critical, so `@intFromEnum` comparisons are the priority comparison.

`pop` calls `oldestOccupied`, which scans for the lowest sequence number among occupied slots, so drain order is insertion order with holes skipped. `popBatch` fills a caller supplied slice and returns the count, which is the shape a periodic uplink task wants: wake up, grab a batch, ship it in one radio transaction. A `Stats` struct tracks `pushed`, `coalesced`, a per priority `evicted` array, `rejected` and a `high_water_mark` of peak occupancy, exported as plain data so you can ship it over whatever metrics path you already have.

The complexity is O(capacity), not O(log n), and the file says so and defends it. A binary heap needs pointer stable storage plus a second index to handle sensor id coalescing. At tens to low hundreds of slots a linear scan over cache local inline memory wins on code size and worst case latency. Past a few hundred slots the trade flips and this type is explicitly not meant to go there.

The wire side is `encodeFrame` and `FrameDecoder(max_payload)`. The format is two sync bytes `0xAA 0x55`, then sensor id as little endian u16, timestamp as little endian u32, a priority byte, a payload length byte, the payload and finally a big endian CRC16. The timestamp is truncated to 32 bits deliberately, about 49.7 days of boot relative milliseconds, and the decoder widens it back to u64 leaving epoch reconciliation to the receiver that actually knows wall clock time. The checksum is `crc16Ccitt`, CRC-16/CCITT-FALSE with poly 0x1021 and init 0xFFFF, computed bitwise with no lookup table on purpose so a freestanding target does not spend 512 bytes of flash on it.

`FrameDecoder.feed` takes one byte and returns an `Event` union of `need_more`, `frame` or `crc_mismatch`. It is a six state machine: sync0, sync1, header, payload, crc0, crc1. It accumulates every framing byte into a `running` buffer so the CRC covers sync bytes, header and payload together. Resync is handled explicitly: a wrong byte in the sync1 state that happens to be `0xAA` restarts the sequence rather than losing the real frame start, and a declared payload length above `max_payload` throws the decoder back to sync0. A CRC failure already resyncs internally, so the caller just keeps feeding bytes.

## Usage

```zig
const std = @import("std");
const etbr = @import("EdgeTelemetryBackpressureRing.zig");

// Sensor node: 64 slots, 32 byte payloads, coalesce same-sensor readings
// that land within 500 ms of each other.
var ring = etbr.BackpressureRing(64, 32).init(500);

const reading = try etbr.Frame(32).init(
    0x0A12,          // sensor_id
    now_ms,          // boot-relative milliseconds
    .critical,       // .low / .normal / .high / .critical
    "tank_overflow",
);

switch (ring.push(reading)) {
    .accepted, .coalesced => {},
    .evicted_lower_priority => log.warn("dropped a lower-priority reading", .{}),
    .rejected_full => log.err("ring saturated at equal-or-higher priority", .{}),
}

// Periodic uplink task: drain a batch and encode each frame to the wire.
var batch: [16]etbr.Frame(32) = undefined;
const n = ring.popBatch(&batch);
var wire: [128]u8 = undefined;
for (batch[0..n]) |f| {
    const written = try etbr.encodeFrame(32, f, &wire);
    try uart.write(wire[0..written]);
}

// Gateway side: feed RX bytes in one at a time, no buffering required.
var decoder = etbr.FrameDecoder(32).init();
while (uart.readByte()) |b| {
    switch (decoder.feed(b)) {
        .frame => |f| handle(f.sensor_id, f.timestamp_ms, f.priority, f.payloadSlice()),
        .crc_mismatch => metrics.crc_errors += 1,
        .need_more => {},
    }
}
```

Run the suite with `zig test EdgeTelemetryBackpressureRing.zig`. It covers insertion order, coalescing, strict priority eviction, high water marking, encode and decode round trip, a corrupted payload byte, leading garbage on the wire and a simulated outage that fills the ring and drains it.

## Notes

- Not thread safe or interrupt safe. If an ISR pushes while the main loop pops, you own the critical section. There is no lock anywhere in the file.
- Under sustained equal priority pressure the ring keeps the oldest readings and rejects the newest. One of the tests states this outright: 20 normal priority pushes into 8 slots means 8 accepted and 12 rejected. If you want the latest N rather than the first N since the outage started, this is the wrong policy and you will need to change it.
- Coalescing matches on sensor id and time window only, not priority. A low priority repeat from the same sensor inside the window overwrites the stored frame entirely, priority field included. Keep alarms on their own sensor id, or set `coalesce_window_ms` to 0 to switch coalescing off.
- Eviction requires a strictly lower priority victim. Equal priority never evicts, which is what keeps a flood of critical frames from cannibalising each other but also means a full ring of criticals rejects everything.
- Wire timestamps are 32 bits, roughly 49.7 days of milliseconds. Anything longer wraps and the decoder cannot tell. Epoch mapping is the receiver's job.
- `max_payload` must be 255 or less and `capacity` must be greater than zero. Both are `@compileError` checks, not runtime errors.
- CRC16 catches line noise, not tampering. It is an integrity check, not authentication.
- A payload length byte larger than `max_payload` silently resets the decoder to hunting for sync. You get no event for it, so a persistently mismatched `max_payload` between the two ends looks like silence rather than an error.
- No allocator, no OS calls, no imports beyond `std` and `std.testing`. It compiles freestanding.
