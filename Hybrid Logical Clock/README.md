# Hybrid Logical Clock

A hybrid logical clock for Gleam that gives every write in a distributed system a timestamp that is close to real time, never goes backwards and is unique per node. It comes with a last writer wins map on top, so replicas that sync in any order end up with the same data.

**Language:** Gleam | **Lines:** 479 | **Added:** 2026-10-11

## What this solves

If you run more than one writer, you need a way to say which write came last. Wall clocks lie. Two servers disagree by tens of milliseconds on a good day and by seconds after a bad NTP step or a VM pause. Plain `Date.now()` as a last writer wins key means a node with a slow clock silently loses every conflict, and a node with a fast clock wins them all, forever.

The usual fix is a Lamport counter, but a Lamport counter has no relation to real time. You cannot say "show me everything from the last five minutes" and you cannot expire anything by age. A hybrid logical clock (HLC) keeps a wall clock reading and a small counter together. The wall part stays within a bounded distance of real time. The counter breaks ties when two events land in the same millisecond or when the physical clock moves backwards.

This module is a pure Gleam implementation of that idea with the production details that the paper version skips: a drift limit in both directions, counter overflow handling, restart recovery, a sortable wire format and a convergent key value map. It is aimed at edge workers, local first apps, multi region caches and anything that merges state from several writers.

## Why I built it

Most HLC snippets I found are twenty lines from the original paper. They are fine as a diagram and wrong as a dependency. They accept any remote timestamp, so one peer with a clock set to the year 2040 drags your entire cluster into 2040 and every later timestamp you issue is permanently in the future. They crash or wrap when the counter fills. They forget the last timestamp on restart, so after a reboot with a clock that stepped back you can issue a stamp you already handed out. And they return a bare number, so nobody knows how to store it.

I wanted something I could drop into a Gleam service and trust. The rules I set were simple. A bad input must never change the clock I am holding. Every failure must be a named value I can match on, not a panic. The encoded form must sort correctly as plain text so it works as a database key. And the merge logic must be small enough to read in one sitting.

Gleam is a good fit because the clock is a plain immutable value. Every operation returns a new `Clock`, so a rejected remote timestamp leaves the old clock untouched. It also compiles to both Erlang and JavaScript. That matters here because the wire format avoids packing everything into one big integer. A 64 bit pack would be fine on the BEAM and silently lose precision on the JavaScript target, where integers are floats.

## When to use it

Use it when:

- You have several writers and need a deterministic winner for conflicting writes.
- You merge state from replicas, edge nodes, mobile clients or offline first apps.
- You want timestamps that stay readable as real time, for example to prune data older than a day.
- You need sortable ids for events, log entries or object keys that are roughly time ordered across nodes.

Do not use it when:

- You need to detect that two writes were concurrent. An HLC totally orders events, so it will pick a winner and never tell you there was a conflict. If you need that, use version vectors.
- You need external consistency across data centres. That needs bounded clock hardware, not a software clock.
- Your values need field level merging. The map in this module resolves whole values per key.

## How it works

### The timestamp

A `Timestamp` is three fields: `wall_ms`, `counter` and `node`. Ordering is in `compare`: wall time first, then counter, then the node id as a string. Because node ids are unique, no two writers ever produce equal timestamps, which is what makes last writer wins deterministic. `validate` checks that a timestamp fits the ranges of the wire format, using the constants `max_wall_ms` (2^48 minus 1 milliseconds) and `max_counter_limit` (65,535). Node ids must be 1 to `max_node_length` (64) characters of lowercase letters, digits, underscore, dot and colon. No hyphen is allowed, because the hyphen is the separator in the encoded form.

### The clock

`Clock` holds the node id, the last timestamp issued and a `Config`. `default_config` gives a 60 second `max_drift_ms` and a `max_counter` of 65,535. `new` builds a clock for a node that has never written. `resume` builds one from the last persisted timestamp, which you read back with `last_issued`. That is what makes restarts safe: the new clock starts at or above the old one, even if the physical clock went backwards while the process was down. Both constructors validate the config and the node id and return `InvalidConfig` or `InvalidNode` rather than building a broken clock.

`tick` stamps a local event. If the physical time has moved past the last wall value, the wall value follows it and the counter resets to zero. Otherwise the wall value stays put and the counter goes up by one. This is the case that handles a burst inside one millisecond and also a physical clock that stepped backwards.

`observe` folds in a timestamp from another node. The new wall value is the maximum of the physical clock, our last wall value and the remote wall value. The counter then depends on which of those tied for the maximum: if both ours and theirs tied, it is the larger counter plus one; if only one tied, that counter plus one; if neither, zero. The result is strictly greater than both the remote timestamp and everything we issued before. `observe` also refuses a remote timestamp that carries our own node id with `DuplicateNode`, because that means two writers share an id and the tie break guarantee is gone.

### Drift limits and overflow

Both `tick` and `observe` finish through the private `advance` function. It does two things.

First, counter overflow. If the counter goes past `max_counter`, the clock borrows the next millisecond and resets the counter to zero. The result is still strictly increasing. It is a deliberate choice over returning an error, because a hot key being written 70,000 times in one millisecond should not crash a writer.

Second, the drift check. If the wall value would end up more than `max_drift_ms` ahead of the physical clock, the call returns `DriftExceeded` with a `retry_after_ms` hint and the state is not changed. That covers a physical clock that jumped backwards by minutes and a sustained burst that keeps borrowing milliseconds. The remote side of the same rule is `RemoteAhead`, returned by `observe` when a peer is too far in the future. This is the guard against the year 2040 peer. You can log it, drop the message and alert on it, while your own clock stays healthy.

Other named failures are `PhysicalClockOutOfRange` for a nonsense local reading, `WallClockExhausted` when the 48 bit space is used up and `MalformedTimestamp` for bad input. `describe_error` turns any of them into a log line.

### The wire format

`encode` writes `wwwwwwwwwwww-cccc-node`: twelve lowercase hex digits of wall time, four of counter, then the node id. Fixed width hex means a plain string sort gives the same order as `compare`, so the encoded value works directly as a primary key, a sorted set member or an object storage prefix. `decode` is strict. It checks the segment count, the exact widths, the hex alphabet and then runs `validate`. It does not use a lenient integer parser, so inputs like a leading plus sign or an uppercase digit are rejected rather than guessed at.

### The last writer wins map

`LwwMap` stores a `Register` per key, which is a `Cell` (either `Live` with a value or `Tombstone`) plus the `Timestamp` of the write. `lww_set` and `lww_delete` go through `lww_apply`, where the higher timestamp wins and an equal one keeps what is stored. `lww_merge` folds one map into another. Since each step is a max over a total order, merge is commutative, associative and idempotent, so replicas converge whatever order messages arrive in and however often they are duplicated.

Deletes leave a tombstone so a delayed older write cannot bring a key back. For syncing, `lww_high_water` returns the highest stamp in a map, `lww_delta_since` returns only the registers newer than a cursor, oldest first, and `lww_apply_delta` applies them on the other side. `lww_get` hides tombstones, `lww_live_entries` returns live pairs sorted by key so every replica prints the same thing, and `lww_prune_tombstones` removes tombstones older than a wall time horizon.

## Usage

Add the file to a Gleam project as `src/hybrid_logical_clock.gleam`. It only needs `gleam_stdlib`. The caller passes the physical time in milliseconds into every call, so the module has no hidden dependency on the system clock and is simple to test.

```gleam
import hybrid_logical_clock as hlc

pub fn example(now_ms: Int) {
  // one clock per node, node ids must be unique and stable
  let assert Ok(clock) = hlc.new("edge.fra1", now_ms, hlc.default_config())

  // stamp a local write
  let assert Ok(#(clock, stamp)) = hlc.tick(clock, now_ms)
  let wire = hlc.encode(stamp)

  // stamp a receive event from a peer, rejecting bad remotes
  let assert Ok(remote) = hlc.decode("000000000064-0003-edge.ams1")
  case hlc.observe(clock, now_ms, remote) {
    Ok(#(clock, _received_at)) -> persist(hlc.encode(hlc.last_issued(clock)))
    Error(error) -> log(hlc.describe_error(error))
  }

  // replicas that converge
  let a =
    hlc.lww_new()
    |> hlc.lww_set("theme", "dark", stamp)
  let merged = hlc.lww_merge(a, hlc.lww_new())
  let _ = hlc.lww_get(merged, "theme")
  wire
}

// on restart: feed the persisted stamp back in
pub fn restart(saved: String, now_ms: Int) {
  let assert Ok(last) = hlc.decode(saved)
  hlc.resume("edge.fra1", last, now_ms, hlc.default_config())
}

fn persist(_text: String) { Nil }
fn log(_text: String) { Nil }
```

A test file is in `test/hybrid_logical_clock_test.gleam` and uses gleeunit. Run it with `gleam test`.

## Notes

- Persist `last_issued` often enough that a crash loses little, and call `resume` on boot. If you cannot persist on every write, persist a value a few seconds ahead of the clock, the way databases that use HLCs do, and resume from that.
- Node ids must be unique per live writer. Reusing an id across two processes breaks the ordering guarantee and nothing in a pure module can detect it, apart from `DuplicateNode` when you happen to see your own id come back.
- `max_drift_ms` is a trade. A small value rejects more remote messages when clocks are loose. A large value lets one bad peer push your clock further ahead. Sixty seconds is a sane start for servers with NTP. For phones and browsers you will want to pick it deliberately.
- Tombstone pruning is only safe after every replica has acknowledged state past the horizon. A replica that was offline longer than that and still holds the old live value will bring the key back.
- The map resolves ties by keeping the stored value. With unique node ids a tie cannot happen between different writes, so this only matters if that rule is broken.
- The code type checks on the JavaScript target against a stand in standard library, and the test file compiles. I have not run the tests on a real Erlang toolchain from this environment, so run `gleam test` once in your project before depending on it.
