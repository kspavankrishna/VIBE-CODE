# Cardinality Circuit Ledger

One tenant id, prompt hash or request id leaking into a metric label turns a 200 series gauge into 4 million series and takes the dashboard, the query path and the observability bill with it. This is a small Zig library that sits in the write path and decides, per value, whether a label stays, gets bucketized or gets dropped.

**Language:** Zig | **Lines:** 421 | **Added:** 2026-06-28

## What this solves

This solves the OpenTelemetry cardinality limiter problem that shows up in AI gateway metrics, agent runtime traces, MCP tool logs, edge inference routers and Prometheus labels when one tenant, prompt hash, tool name, route, model variant or request id creates millions of unique values. Label explosion is not theoretical, it is an incident pattern. Somebody attaches `request_id` to a counter, the change passes review because it looks like one extra label and every distinct value becomes a new time series.

It is quiet for twenty minutes and then loud everywhere at once. Prometheus memory climbs as the head block fills, scrapes time out and recording rules that finished in 40 ms take seconds because the selector now matches a million streams. On a hosted backend the first to notice is finance, because active series are the billing unit. Self hosted, it is whoever gets paged when the TSDB OOMs during a real outage, which is exactly when you needed the metrics.

The usual fixes are each bad in a different way. Dropping the label loses the signal you added it for. Collector relabelling is static, so you must know the bad values in advance and the whole point is that you do not. Backend limits cut you off after the damage is already in the ingest pipeline. Sampling changes what your counters mean. What you want is a budget enforced at emission: the first N distinct values pass through untouched, everything past N collapses into a stable bucket and a HyperLogLog sketch keeps measuring true distinct pressure after the allow list saturates.

## Why I built it

In April 2026 the hard part is not collecting telemetry, it is keeping useful telemetry without letting one noisy label break the bill, the dashboard or the on-call query path. Existing tooling sits in the wrong place. Collector processors run after your process serialized the record and backend limits run after ingest, and neither can tell a field with 900 real values from one with 900 garbage ones, because neither measures distinct pressure separately from the enforcement decision.

I wanted something small enough to link into an edge collector, a sidecar, a model gateway or a streaming DevOps worker without dragging a runtime along. Zig gives you an explicit allocator, no hidden allocation in the hot path and one file you can audit in a sitting. It is std only, nothing to vendor.

## When to use it

- A multi tenant AI gateway where `tenant_id` is fine until one automated customer creates a tenant per request
- An MCP or agent runtime labelling spans with tool names from a dynamic namespace
- A model serving proxy tagging metrics with `model` or `variant`, where canary deploys quietly add a variant string
- An edge log shipper that must bound what it forwards because egress cost is per unique series
- Any Prometheus exporter where you got burned once and want a hard ceiling on series growth
- Log cleanup needing a deterministic replacement token so grouped records still join downstream

## How it works

The core type is `CardinalityCircuit`, built with `init(allocator, Options)` where `Options` carries `window_ns` (default 60 seconds) and a hash `seed`. You register one `FieldPolicy` per label with `addPolicy`: field name, `max_distinct` as the budget, an HLL `precision` from 4 to 18 (default 12), a `replacement_prefix` (default `other`) and an `OverflowMode` of `keep`, `bucketize` or `drop_field`. It copies both strings into its own allocation, with `errdefer` guards so a failure partway through does not leak. Duplicate registration returns `CircuitError.DuplicatePolicy`, a zero budget `InvalidBudget`, a bad precision `InvalidPrecision`.

The hot path is `observe(now_ns, field, value, replacement_buffer)`. It fingerprints with `hashFieldValue`, running Wyhash twice: over the field name with the seed mixed against the golden ratio constant `0x9e37_79b9_7f4a_7c15`, then over the value using that field hash as the seed. Fingerprints are field scoped, so the same string under two labels never collides. An unregistered field returns a passthrough `keep` and touches no state. Otherwise `FieldState.rotate` runs first. Windows are tumbling, not sliding: `floorWindow` does a `divFloor` of the timestamp by `window_ns`, and a newer bucket clears the registers, calls `clearRetainingCapacity` on the allow list and zeroes the counters. Then the fingerprint enters the sketch and `observed` is bumped with a saturating add.

The decision is two tiered and this is the part worth understanding. The exact tier is `allowed_hashes`, an `AutoHashMap(u64, void)` holding at most `max_distinct` fingerprints. A hit is an immediate `keep`, so once a value earns a slot it is never rewritten for the rest of the window. A miss under budget inserts and keeps. A miss at budget under `OverflowMode.keep` passes through without consuming a slot and without counting as rejected, which makes `keep` a pure observation mode for sizing a budget before you enforce one. Under `drop_field` it increments `rejected` and returns an empty value. Under `bucketize` it increments `rejected` and calls `renderReplacement`, writing `prefix_field_hash` into the caller buffer through a `fixedBufferStream`, sanitizing the field via `writeSafeToken` so anything outside `[A-Za-z0-9_-]` becomes an underscore. The same overflow value maps to the same bucket all window.

The approximate tier is `HllCounter`, a textbook HyperLogLog. `init` allocates `2^precision` single byte registers, so precision 12 costs 4 KB per field. `offer` takes the low `precision` bits as the register index, then `leadingZeroRank` shifts those off, counts leading zeros with `@clz`, adds one and clamps to `64 - precision + 1`. `estimate` computes `alpha * m^2 / sum(2^-rank)` using the bias constants in `alphaFor`, falling back to linear counting `m * ln(m / zeros)` when the raw value is at or below `2.5 * m` and empty registers remain. HLL earns its place because it reports real distinct pressure in constant memory, which a saturated allow list cannot.

Reporting is `snapshot(field)`, returning estimate, allowed count, observed, rejected and window start, plus `writePrometheus(writer)`, which emits `cardinality_circuit_estimate` as a gauge and `cardinality_circuit_rejected_total` as a counter per field. Labels go through `writePrometheusLabel`, escaping backslash, double quote and newline so a hostile field name cannot break the scrape.

## Usage

```zig
const std = @import("std");
const ccl = @import("CardinalityCircuitLedger.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var circuit = try ccl.CardinalityCircuit.init(gpa.allocator(), .{
        .window_ns = 60 * std.time.ns_per_s,
        .seed = 0x6d5a_56f0_7c2b_9e31,
    });
    defer circuit.deinit();

    // 500 distinct tenants per minute, overflow collapses into a bucket.
    try circuit.addPolicy(.{
        .field = "tenant_id",
        .max_distinct = 500,
        .precision = 12,
        .mode = .bucketize,
        .replacement_prefix = "other",
    });

    // Observation only: never rewrites, just measures. Use it to size a budget.
    try circuit.addPolicy(.{ .field = "model", .max_distinct = 32, .mode = .keep });

    // Anything past budget loses the field entirely.
    try circuit.addPolicy(.{ .field = "request_id", .max_distinct = 1, .mode = .drop_field });

    var buf: [128]u8 = undefined;
    const now = std.time.nanoTimestamp();

    const obs = try circuit.observe(now, "tenant_id", "acct_91f3c", &buf);
    switch (obs.decision) {
        .keep => emit("tenant_id", obs.value),
        .bucketize => emit("tenant_id", obs.value), // other_tenant_id_9c1a4f7b20de3355
        .drop_field => {},
    }

    if (circuit.snapshot("tenant_id")) |snap| {
        std.debug.print("distinct~{d:.0} allowed={d} observed={d} rejected={d}\n", .{
            snap.estimate, snap.allowed_count, snap.observed, snap.rejected,
        });
    }

    try circuit.writePrometheus(std.io.getStdOut().writer());
}

fn emit(field: []const u8, value: []const u8) void {
    _ = field;
    _ = value;
}
```

Run the two bundled tests:

```
zig test CardinalityCircuitLedger.zig
```

## Notes

- Not thread safe. No mutex or atomic anywhere. Give each worker its own circuit or wrap `observe` yourself, and remember per worker circuits mean per worker budgets.
- `observe` needs a caller supplied `replacement_buffer`. Too small for `prefix_field_16hexdigits` and the write fails, propagating the error. 128 bytes is comfortable.
- Rotation is lazy, driven only by `observe`. Neither `snapshot` nor `writePrometheus` rotates, so a quiet field reports its last active window.
- Rotation wipes the counters, so `observed` and `rejected` are per window, not lifetime. `cardinality_circuit_rejected_total` resets at every boundary despite its counter type.
- Memory per field is `2^precision` bytes of registers plus up to `max_distinct` allow list entries. Precision 12 is 4 KB, precision 18 is 256 KB, so raise it deliberately.
- Unregistered fields are pure passthrough: `keep` with a zero estimate and no tracking. This protects only what you explicitly budget.
- The estimate is approximate, enforcement is not. The allow list decision stays exact whatever the sketch reports.
- Library only, no `main` and no CLI, so no exit codes. It returns Zig errors: `DuplicatePolicy`, `InvalidBudget`, `InvalidPrecision`, `InvalidWindow`, plus allocator and buffer errors.
