# Deadline Timing Wheel

A hierarchical timing wheel in Odin for the part of an LLM gateway that nobody benchmarks: per request deadlines, idle timeouts and lease expiry at hundreds of thousands of live streams. Schedule, cancel and extend are all O(1), memory is fixed up front and every change is checked against a brute force model.

**Language:** Odin | **Lines:** 1064 | **Added:** 2026-11-10

## What this solves

Every AI gateway, agent runner and MCP proxy ends up holding a lot of timers. A streaming completion has a first token deadline, an idle deadline between tokens and a hard wall clock cap. A tool call has a timeout. A lease on a GPU slot has an expiry. Multiply that by a few hundred thousand open connections and the timer structure stops being a detail.

The usual answer is a binary heap. A heap is fine until you add the one operation these systems do constantly: extending an idle timeout every time a token arrives. A heap cannot move an item cheaply, so most implementations push a fresh copy and mark the old one dead. Now the heap holds stale copies, its size grows with traffic instead of with live connections, and every push and pop pays log n on a structure that no longer fits in cache. Cancellation has the same problem.

A timing wheel does the opposite. Each timer lives in a slot chosen by its expiry tick. Inserting, cancelling and rescheduling are pointer edits on a doubly linked list. Time only costs you work when there is something due. That is the whole idea, but the version that survives production has a lot of sharp corners: clock jumps, handles that outlive their timer, deadlines already in the past, deadlines a year away, a full table, a process that was paused for ten minutes. This entry handles all of them and says what it does in each case.

## Why I built it

I keep running into the same pattern when reading gateway code. Somebody writes a quick `time.AfterFunc` per request, or a heap with lazy deletion, and it works at ten thousand connections. At a million it either burns a core on timer bookkeeping or the heap eats memory because idle extensions pile up stale entries. The fix is known, it is in the Linux kernel and in Netty and in Kafka, but the copies I could find were either tied to one runtime or left out the boring failure modes.

I wanted a version I could read in one sitting, drop into a systems language project and trust. Odin is a good fit: no hidden allocations, explicit allocators, fixed size arrays and bit operations that read plainly. The whole wheel is one pool of entries allocated once, so the hot path never touches the allocator.

I also wanted proof, not vibes. The file ships a differential fuzz test that drives the wheel and a naive model with the same random operations and fails on the first disagreement. I trust that more than any amount of hand written cases.

## When to use it

Use it when you hold more than a few thousand timers and a lot of them get extended or cancelled before they fire:

- Idle and first token timeouts on streaming LLM responses.
- Per tool call deadlines in an agent runner or MCP server.
- Lease and heartbeat expiry for GPU slots, locks or worker registrations.
- Hedged request timers in a router, where most timers are cancelled when the first reply wins.
- Retry backoff queues with a large number of waiting items.

Do not use it when you need sub tick precision. Timers fire on tick boundaries, so with a 1 ms tick a timer can fire up to one tick after its deadline but never before. If you have a few hundred timers a plain sorted list is simpler. If you need cross process timers, this is an in memory structure and has no persistence.

## How it works

The wheel has `LEVELS` levels of `SLOTS` slots. `SLOT_BITS` is 6, so each level has 64 slots and each level covers 64 times the span of the one below it. With five levels that is 2 to the power of 30 ticks, about 12 days at a 1 ms tick. Anything farther out goes to a single list at index `OVERFLOW`, and is re-examined once per full cycle of the top level.

**Placement.** `place` computes the distance from the current tick to the expiry tick and picks the lowest level whose range contains it. The slot is the expiry tick shifted down by that level and masked with `SLOT_MASK`. The entry is appended to the tail of that slot list by `link`, which also sets a bit in the `occupied` bitmap for that level. Appending to the tail is what makes timers that land on the same tick fire in the order they were scheduled.

**Cascade.** When the wheel reaches a tick that is a multiple of a level's unit, `process_tick` drains that level's slot with `drain_slot`. Each entry either fires, because its expiry has arrived, or is placed again, which drops it to a lower and finer level. The levels are walked from the top down so an entry can fall all the way to level zero in one tick. `drain_slot` detaches the whole list before touching it, which is what makes re-placing into the same list safe. The overflow list is drained when the tick is a multiple of the full span.

**Skipping empty time.** A naive wheel walks every tick, so a process that was paused for ten minutes pays for 600000 ticks. Here `wheel_next_event_tick` uses the `occupied` bitmaps. For each level it rotates the bitmap so the next slot to be visited is bit zero, then takes the trailing zero count to find the next non empty slot in constant time. The result is the next tick where anything can happen. `wheel_advance` jumps straight to it. A one hour pause costs a handful of loop iterations, not millions. If the wheel is empty it jumps directly to the target.

**Handles and the pool.** Entries live in one slice allocated in `wheel_init`. A `Handle` is an index plus a generation number. `release` bumps the generation each time an entry is freed, so a handle held after the timer fired or was cancelled is detected. `resolve` returns `Stale_Handle` for that case and `Bad_Handle` for a zero or out of range handle. This matters in real code because cancel and fire race all the time: the response finished at the same moment the timeout was due. A stale cancel is an expected outcome, so it is an error value, not a crash.

**Time rules.**

- `tick_ceil` rounds deadlines up, so a timer can never fire early.
- `expiry_for` clamps a deadline that is already past to the next tick and counts it in `past_due_clamped`. This is deliberate. A past deadline placed in the current slot would sit there until the wheel came around again, which is the classic bug.
- `wheel_advance` ignores a clock that moved backwards, returns zero and counts `clock_regressions`. Pass it a monotonic clock anyway.
- `wheel_next_deadline_ns` returns a lower bound on the next event. It can be earlier than the true deadline because it also counts cascade ticks, never later. Sleep until it, call `wheel_advance`, ask again.

**Capacity.** The pool is fixed. When it is full `wheel_schedule` returns `Full` and bumps `rejected_full`. I chose backpressure over silent growth because a gateway that keeps accepting work while its timer table grows without bound is a gateway that falls over later with no warning. Size the pool for peak concurrency plus headroom and treat `Full` as a signal to shed load.

**Invariants.** `wheel_check_invariants` walks every slot and checks list links, tail pointers, the occupancy bitmaps, the live count, the free list and the key property: every entry sits in a slot that will be visited no later than its expiry tick. It returns an empty string when everything holds or a short description of the first violation. It is cheap enough to run in tests and in a debug build.

**Differential fuzz.** `run_selftest` mixes schedule, cancel, reschedule and advance with deadlines from past due up to 2 to the power of 34 ticks, jumps from one tick to 2 to the power of 31 ticks, clock regressions, a deliberately small pool that hits `Full`, and cancels of handles that are already dead. Each advance is compared with a brute force model on the exact fired set, the handle, the deadline and the timestamp. The wheel invariants are checked every 61 operations, and a final far jump must drain everything.

**Benchmark.** `run_bench` runs the same workload on the wheel and on a binary heap with lazy deletion. Every step advances 1 ms and extends eight random timers. It prints nanoseconds per operation and the heap's peak size, which shows the stale copy growth directly.

## Usage

The files are one Odin package. Build and run from this folder:

```
odin test .
odin run . -o:speed -- selftest 7 200000
odin run . -o:speed -- bench 200000 20000
```

`selftest` takes an optional seed and operation count and exits with status 1 on the first mismatch. `bench` takes an optional timer count and step count.

Using the wheel from your own code:

```odin
w: Wheel
if wheel_init(&w, 100_000, 1_000_000, now_ns) != .None {
	return
}
defer wheel_destroy(&w)

h, err := wheel_schedule(&w, now_ns + 30_000_000_000, request_id)
if err == .Full {
	// shed load
}

// a token arrived: push the idle timeout out
wheel_reschedule(&w, h, now_ns + 30_000_000_000)

// response finished first: stale handle is fine, not fatal
wheel_cancel(&w, h)

// in the loop
expired: [dynamic]Expired
defer delete(expired)
for {
	next, ok := wheel_next_deadline_ns(&w)
	// sleep until next if ok, otherwise until new work arrives
	clear(&expired)
	wheel_advance(&w, monotonic_now_ns, &expired)
	for ev in expired {
		// ev.payload is the id you passed to wheel_schedule
	}
}
```

The second argument to `wheel_init` is capacity and the third is the tick length in nanoseconds. The fourth is the clock reading the wheel treats as tick zero.

## Notes

- Single threaded by design. Put the wheel behind one owner thread or one lock. A sharded set of wheels, keyed by connection, scales better than a shared locked one.
- Expired timers are appended to a caller owned `[dynamic]Expired`. There are no callbacks, so there is no reentrancy to reason about. Reuse the array between calls to keep the loop allocation free once it has grown.
- A mass expiry, for example a million timers sharing one deadline, is processed in one `wheel_advance` call. If that is a latency problem for you, spread deadlines with a little jitter at schedule time.
- Deadlines more than the top level span away use the overflow list. They are correct but get re-examined once per top level cycle, so keep very long timers rare.
- The `Stats` struct counts scheduled, cancelled, rescheduled and fired timers plus cascades, ticks processed, clamped past deadlines, full rejections, stale handles, clock regressions and the high water mark. Export those as metrics. A rising `stale_handles` or `clock_regressions` is worth an alert.
- I wrote this without an Odin compiler in the authoring sandbox, so the first thing to run is `odin test .`. The wheel algorithm itself was checked against the same differential fuzz in a separate reference port before I committed it.
- Odin moves quickly. If a standard library call has been renamed in your compiler version the fix is mechanical, the logic does not depend on it.
