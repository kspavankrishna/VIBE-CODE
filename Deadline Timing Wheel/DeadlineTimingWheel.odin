package main

import "base:intrinsics"
import "core:fmt"
import "core:mem"
import "core:os"
import "core:time"

SLOT_BITS :: 6
SLOTS     :: 1 << SLOT_BITS
SLOT_MASK :: SLOTS - 1
LEVELS    :: 5
OVERFLOW  :: LEVELS
SPAN_BITS :: SLOT_BITS * LEVELS
NO_LINK   :: i32(-1)

Error :: enum {
	None,
	Full,
	Bad_Config,
	Out_Of_Memory,
	Bad_Handle,
	Stale_Handle,
}

Handle :: struct {
	index: u32,
	gen:   u32,
}

Entry :: struct {
	prev:        i32,
	next:        i32,
	expiry_tick: u64,
	deadline_ns: i64,
	payload:     u64,
	gen:         u32,
	level:       i8,
	slot:        u8,
}

Expired :: struct {
	handle:      Handle,
	payload:     u64,
	deadline_ns: i64,
	fired_ns:    i64,
}

Stats :: struct {
	scheduled:         u64,
	cancelled:         u64,
	rescheduled:       u64,
	fired:             u64,
	cascaded:          u64,
	ticks_processed:   u64,
	past_due_clamped:  u64,
	rejected_full:     u64,
	stale_handles:     u64,
	clock_regressions: u64,
	high_water:        int,
}

Wheel :: struct {
	entries:   []Entry,
	free_head: i32,
	live:      int,
	heads:     [LEVELS + 1][SLOTS]i32,
	tails:     [LEVELS + 1][SLOTS]i32,
	occupied:  [LEVELS + 1]u64,
	tick_ns:   i64,
	epoch_ns:  i64,
	now_tick:  u64,
	allocator: mem.Allocator,
	stats:     Stats,
}

wheel_init :: proc(w: ^Wheel, capacity: int, tick_ns: i64, start_ns: i64, allocator := context.allocator) -> Error {
	if capacity <= 0 || capacity >= (1 << 31) || tick_ns <= 0 {
		return .Bad_Config
	}
	entries, err := make([]Entry, capacity, allocator)
	if err != nil {
		return .Out_Of_Memory
	}
	w^ = Wheel{}
	w.entries = entries
	w.allocator = allocator
	w.tick_ns = tick_ns
	w.epoch_ns = start_ns
	for i in 0 ..< capacity {
		w.entries[i] = Entry{prev = NO_LINK, next = i32(i + 1), gen = 1, level = -1}
	}
	w.entries[capacity - 1].next = NO_LINK
	w.free_head = 0
	for lvl in 0 ..= LEVELS {
		for s in 0 ..< SLOTS {
			w.heads[lvl][s] = NO_LINK
			w.tails[lvl][s] = NO_LINK
		}
	}
	return .None
}

wheel_destroy :: proc(w: ^Wheel) {
	if w.entries != nil {
		delete(w.entries, w.allocator)
	}
	w^ = Wheel{}
}

tick_floor :: proc(w: ^Wheel, ns: i64) -> u64 {
	if ns <= w.epoch_ns {
		return 0
	}
	return u64((ns - w.epoch_ns) / w.tick_ns)
}

tick_ceil :: proc(w: ^Wheel, ns: i64) -> u64 {
	if ns <= w.epoch_ns {
		return 0
	}
	d := ns - w.epoch_ns
	q := d / w.tick_ns
	if d % w.tick_ns != 0 {
		q += 1
	}
	return u64(q)
}

tick_to_ns :: proc(w: ^Wheel, tick: u64) -> i64 {
	max_ticks := u64((max(i64) - w.epoch_ns) / w.tick_ns)
	if tick >= max_ticks {
		return max(i64)
	}
	return w.epoch_ns + i64(tick) * w.tick_ns
}

link :: proc(w: ^Wheel, idx: i32, level: int, slot: int) {
	e := &w.entries[idx]
	e.level = i8(level)
	e.slot = u8(slot)
	e.next = NO_LINK
	tail := w.tails[level][slot]
	e.prev = tail
	if tail == NO_LINK {
		w.heads[level][slot] = idx
	} else {
		w.entries[tail].next = idx
	}
	w.tails[level][slot] = idx
	w.occupied[level] |= u64(1) << u64(slot)
}

unlink :: proc(w: ^Wheel, idx: i32) {
	e := &w.entries[idx]
	lvl := int(e.level)
	slot := int(e.slot)
	if e.prev != NO_LINK {
		w.entries[e.prev].next = e.next
	} else {
		w.heads[lvl][slot] = e.next
	}
	if e.next != NO_LINK {
		w.entries[e.next].prev = e.prev
	} else {
		w.tails[lvl][slot] = e.prev
	}
	if w.heads[lvl][slot] == NO_LINK {
		w.occupied[lvl] &~= u64(1) << u64(slot)
	}
	e.prev = NO_LINK
	e.next = NO_LINK
}

release :: proc(w: ^Wheel, idx: i32) {
	e := &w.entries[idx]
	e.gen += 1
	if e.gen == 0 {
		e.gen = 1
	}
	e.level = -1
	e.prev = NO_LINK
	e.next = w.free_head
	w.free_head = idx
	w.live -= 1
}

// Precondition: entry.expiry_tick > w.now_tick.
place :: proc(w: ^Wheel, idx: i32) {
	e := &w.entries[idx]
	delta := e.expiry_tick - w.now_tick
	level := 0
	for level < LEVELS && delta >= (u64(1) << u64((level + 1) * SLOT_BITS)) {
		level += 1
	}
	if level == LEVELS {
		link(w, idx, OVERFLOW, 0)
		return
	}
	slot := int((e.expiry_tick >> u64(level * SLOT_BITS)) & SLOT_MASK)
	link(w, idx, level, slot)
}

resolve :: proc(w: ^Wheel, h: Handle) -> (i32, Error) {
	if h.gen == 0 || int(h.index) >= len(w.entries) {
		return NO_LINK, .Bad_Handle
	}
	e := &w.entries[h.index]
	if e.gen != h.gen || e.level < 0 {
		w.stats.stale_handles += 1
		return NO_LINK, .Stale_Handle
	}
	return i32(h.index), .None
}

expiry_for :: proc(w: ^Wheel, deadline_ns: i64) -> u64 {
	t := tick_ceil(w, deadline_ns)
	earliest := w.now_tick + 1
	if t < earliest {
		w.stats.past_due_clamped += 1
		return earliest
	}
	return t
}

wheel_schedule :: proc(w: ^Wheel, deadline_ns: i64, payload: u64) -> (Handle, Error) {
	if w.free_head == NO_LINK {
		w.stats.rejected_full += 1
		return Handle{}, .Full
	}
	idx := w.free_head
	e := &w.entries[idx]
	w.free_head = e.next
	e.payload = payload
	e.deadline_ns = deadline_ns
	e.expiry_tick = expiry_for(w, deadline_ns)
	w.live += 1
	if w.live > w.stats.high_water {
		w.stats.high_water = w.live
	}
	w.stats.scheduled += 1
	place(w, idx)
	return Handle{index = u32(idx), gen = e.gen}, .None
}

wheel_cancel :: proc(w: ^Wheel, h: Handle) -> Error {
	idx, err := resolve(w, h)
	if err != .None {
		return err
	}
	unlink(w, idx)
	release(w, idx)
	w.stats.cancelled += 1
	return .None
}

wheel_reschedule :: proc(w: ^Wheel, h: Handle, deadline_ns: i64) -> Error {
	idx, err := resolve(w, h)
	if err != .None {
		return err
	}
	unlink(w, idx)
	e := &w.entries[idx]
	e.deadline_ns = deadline_ns
	e.expiry_tick = expiry_for(w, deadline_ns)
	place(w, idx)
	w.stats.rescheduled += 1
	return .None
}

fire :: proc(w: ^Wheel, idx: i32, out: ^[dynamic]Expired, fired_ns: i64) {
	e := &w.entries[idx]
	append(out, Expired{handle = Handle{index = u32(idx), gen = e.gen}, payload = e.payload, deadline_ns = e.deadline_ns, fired_ns = fired_ns})
	w.stats.fired += 1
	release(w, idx)
}

// Detaches the whole slot first so entries can be re-placed (even into the same list) safely.
drain_slot :: proc(w: ^Wheel, level: int, slot: int, out: ^[dynamic]Expired, fired_ns: i64, cascade: bool) {
	cur := w.heads[level][slot]
	w.heads[level][slot] = NO_LINK
	w.tails[level][slot] = NO_LINK
	w.occupied[level] &~= u64(1) << u64(slot)
	for cur != NO_LINK {
		e := &w.entries[cur]
		nxt := e.next
		if e.expiry_tick <= w.now_tick {
			fire(w, cur, out, fired_ns)
		} else {
			w.stats.cascaded += 1
			place(w, cur)
		}
		cur = nxt
	}
	_ = cascade
}

process_tick :: proc(w: ^Wheel, out: ^[dynamic]Expired, fired_ns: i64) {
	t := w.now_tick
	w.stats.ticks_processed += 1
	if t & ((u64(1) << SPAN_BITS) - 1) == 0 && w.occupied[OVERFLOW] != 0 {
		drain_slot(w, OVERFLOW, 0, out, fired_ns, true)
	}
	for level := LEVELS - 1; level >= 1; level -= 1 {
		if t & ((u64(1) << u64(level * SLOT_BITS)) - 1) == 0 {
			slot := int((t >> u64(level * SLOT_BITS)) & SLOT_MASK)
			if (w.occupied[level] >> u64(slot)) & 1 != 0 {
				drain_slot(w, level, slot, out, fired_ns, true)
			}
		}
	}
	slot0 := int(t & SLOT_MASK)
	if (w.occupied[0] >> u64(slot0)) & 1 != 0 {
		drain_slot(w, 0, slot0, out, fired_ns, false)
	}
}

// Lower bound on the next tick where anything can happen (a fire or a cascade).
wheel_next_event_tick :: proc(w: ^Wheel) -> (tick: u64, ok: bool) {
	for level in 0 ..< LEVELS {
		bm := w.occupied[level]
		if bm == 0 {
			continue
		}
		shift := u64(level * SLOT_BITS)
		base := w.now_tick >> shift
		cur := base & SLOT_MASK
		r := (cur + 1) & SLOT_MASK
		rot := bm
		if r != 0 {
			rot = (bm >> r) | (bm << (u64(SLOTS) - r))
		}
		d := u64(intrinsics.count_trailing_zeros(rot)) + 1
		cand := (base + d) << shift
		if !ok || cand < tick {
			tick = cand
			ok = true
		}
	}
	if w.occupied[OVERFLOW] != 0 {
		span := u64(1) << SPAN_BITS
		cand := (w.now_tick / span + 1) * span
		if !ok || cand < tick {
			tick = cand
			ok = true
		}
	}
	return
}

// Never returns a time later than the earliest pending deadline. Sleep until it, call wheel_advance, ask again.
wheel_next_deadline_ns :: proc(w: ^Wheel) -> (ns: i64, ok: bool) {
	if w.live == 0 {
		return 0, false
	}
	t, found := wheel_next_event_tick(w)
	if !found {
		return 0, false
	}
	return tick_to_ns(w, t), true
}

// Appends expired timers to out and returns how many were appended. now_ns must come from a monotonic clock.
wheel_advance :: proc(w: ^Wheel, now_ns: i64, out: ^[dynamic]Expired) -> int {
	target := tick_floor(w, now_ns)
	if target < w.now_tick {
		w.stats.clock_regressions += 1
		return 0
	}
	before := len(out^)
	for w.now_tick < target {
		if w.live == 0 {
			w.now_tick = target
			break
		}
		nt, ok := wheel_next_event_tick(w)
		if !ok || nt > target {
			w.now_tick = target
			break
		}
		w.now_tick = nt
		process_tick(w, out, now_ns)
	}
	return len(out^) - before
}

wheel_check_invariants :: proc(w: ^Wheel) -> string {
	seen := 0
	for level in 0 ..= LEVELS {
		slots := SLOTS
		if level == OVERFLOW {
			slots = 1
		}
		for slot in 0 ..< slots {
			head := w.heads[level][slot]
			bit := (w.occupied[level] >> u64(slot)) & 1
			if (head != NO_LINK) != (bit == 1) {
				return "occupancy bitmap disagrees with list head"
			}
			if head == NO_LINK {
				if w.tails[level][slot] != NO_LINK {
					return "tail set on an empty slot"
				}
				continue
			}
			prev := NO_LINK
			cur := head
			for cur != NO_LINK {
				e := &w.entries[cur]
				if int(e.level) != level || int(e.slot) != slot {
					return "entry level or slot field disagrees with the list holding it"
				}
				if e.prev != prev {
					return "broken prev link"
				}
				if e.expiry_tick <= w.now_tick {
					return "pending entry is already due"
				}
				if level < LEVELS {
					shift := u64(level * SLOT_BITS)
					if int((e.expiry_tick >> shift) & SLOT_MASK) != slot {
						return "entry sits in the wrong slot for its expiry"
					}
					base := w.now_tick >> shift
					cur_slot := base & SLOT_MASK
					d := ((u64(slot) - cur_slot - 1) & SLOT_MASK) + 1
					if ((base + d) << shift) > e.expiry_tick {
						return "slot would be visited after the entry is due"
					}
				}
				seen += 1
				if seen > len(w.entries) {
					return "cycle in a slot list"
				}
				prev = cur
				cur = e.next
			}
			if w.tails[level][slot] != prev {
				return "tail pointer does not match the last entry"
			}
		}
	}
	if seen != w.live {
		return "live count disagrees with entries found in slots"
	}
	free_count := 0
	f := w.free_head
	for f != NO_LINK {
		if w.entries[f].level != -1 {
			return "free list holds a live entry"
		}
		free_count += 1
		if free_count > len(w.entries) {
			return "cycle in the free list"
		}
		f = w.entries[f].next
	}
	if free_count + w.live != len(w.entries) {
		return "free plus live does not equal capacity"
	}
	return ""
}

// ---------------------------------------------------------------------------
// Randomized differential test against a brute force model.
// ---------------------------------------------------------------------------

Rng :: struct {
	s: u64,
}

rng_next :: proc(r: ^Rng) -> u64 {
	x := r.s
	x ~= x >> 12
	x ~= x << 25
	x ~= x >> 27
	r.s = x
	return x * 0x2545F4914F6CDD1D
}

rng_below :: proc(r: ^Rng, n: u64) -> u64 {
	return rng_next(r) % n
}

Model :: struct {
	live:     [dynamic]bool,
	expiry:   [dynamic]u64,
	deadline: [dynamic]i64,
	handle:   [dynamic]Handle,
	now_tick: u64,
}

sample_ticks :: proc(r: ^Rng) -> i64 {
	k := rng_below(r, 100)
	result: i64
	switch {
	case k < 10:
		result = -i64(rng_below(r, 50))
	case k < 40:
		result = i64(rng_below(r, 64))
	case k < 65:
		result = i64(rng_below(r, 4096))
	case k < 80:
		result = i64(rng_below(r, 262144))
	case k < 93:
		result = i64(rng_below(r, u64(1) << 24))
	case k < 98:
		result = i64(rng_below(r, u64(1) << 30))
	case:
		result = i64(rng_below(r, u64(1) << 34))
	}
	return result
}

sample_offset_ns :: proc(r: ^Rng, tick_ns: i64) -> i64 {
	t := sample_ticks(r)
	if t <= 0 {
		return t * tick_ns
	}
	return t * tick_ns + i64(rng_below(r, u64(tick_ns)))
}

model_expiry :: proc(w: ^Wheel, m: ^Model, deadline_ns: i64) -> u64 {
	e := tick_ceil(w, deadline_ns)
	if e < m.now_tick + 1 {
		e = m.now_tick + 1
	}
	return e
}

// Returns the number of failures. Zero means the wheel matched the model on every operation.
run_selftest :: proc(seed: u64, ops: int, verbose: bool) -> int {
	TICK :: i64(1_000_000)
	START :: i64(5_000_000_000)
	w: Wheel
	if err := wheel_init(&w, 4096, TICK, START); err != .None {
		fmt.eprintln("wheel_init failed:", err)
		return 1
	}
	defer wheel_destroy(&w)
	rng := Rng{s = seed | 1}
	m: Model
	defer {
		delete(m.live)
		delete(m.expiry)
		delete(m.deadline)
		delete(m.handle)
	}
	out: [dynamic]Expired
	defer delete(out)
	now_ns := START
	full_seen := 0
	fired_total := 0
	stale_checked := 0

	for op in 0 ..< ops {
		pick := rng_below(&rng, 100)
		switch {
		case pick < 55:
			dl := now_ns + sample_offset_ns(&rng, TICK)
			id := u64(len(m.live))
			h, err := wheel_schedule(&w, dl, id)
			if err == .Full {
				full_seen += 1
				if w.live != len(w.entries) {
					fmt.eprintfln("op %d: Full returned with live=%d capacity=%d", op, w.live, len(w.entries))
					return 1
				}
			} else if err != .None {
				fmt.eprintfln("op %d: schedule returned %v", op, err)
				return 1
			} else {
				append(&m.live, true)
				append(&m.expiry, model_expiry(&w, &m, dl))
				append(&m.deadline, dl)
				append(&m.handle, h)
			}
		case pick < 70:
			if len(m.live) == 0 {
				continue
			}
			id := int(rng_below(&rng, u64(len(m.live))))
			err := wheel_cancel(&w, m.handle[id])
			if m.live[id] {
				if err != .None {
					fmt.eprintfln("op %d: cancel of live timer returned %v", op, err)
					return 1
				}
				m.live[id] = false
			} else {
				stale_checked += 1
				if err != .Stale_Handle {
					fmt.eprintfln("op %d: cancel of dead timer returned %v, wanted Stale_Handle", op, err)
					return 1
				}
			}
		case pick < 80:
			if len(m.live) == 0 {
				continue
			}
			id := int(rng_below(&rng, u64(len(m.live))))
			dl := now_ns + sample_offset_ns(&rng, TICK)
			err := wheel_reschedule(&w, m.handle[id], dl)
			if m.live[id] {
				if err != .None {
					fmt.eprintfln("op %d: reschedule of live timer returned %v", op, err)
					return 1
				}
				m.expiry[id] = model_expiry(&w, &m, dl)
				m.deadline[id] = dl
			} else if err != .Stale_Handle {
				fmt.eprintfln("op %d: reschedule of dead timer returned %v, wanted Stale_Handle", op, err)
				return 1
			}
		case:
			clear(&out)
			if rng_below(&rng, 40) == 0 {
				back := now_ns - i64(rng_below(&rng, 10_000)) * TICK - TICK
				n := wheel_advance(&w, back, &out)
				if n != 0 {
					fmt.eprintfln("op %d: regression advance fired %d timers", op, n)
					return 1
				}
				continue
			}
			jump: i64
			j := rng_below(&rng, 100)
			switch {
			case j < 40:
				jump = 1
			case j < 85:
				jump = i64(rng_below(&rng, 100)) + 1
			case j < 98:
				jump = i64(rng_below(&rng, 5000)) + 1
			case:
				jump = i64(rng_below(&rng, u64(1) << 31)) + 1
			}
			now_ns += jump * TICK + i64(rng_below(&rng, u64(TICK)))
			target := tick_floor(&w, now_ns)
			if target > m.now_tick {
				m.now_tick = target
			}
			expected := 0
			for i in 0 ..< len(m.live) {
				if m.live[i] && m.expiry[i] <= target {
					expected += 1
				}
			}
			n := wheel_advance(&w, now_ns, &out)
			if n != expected {
				fmt.eprintfln("op %d: advance fired %d timers, model expected %d", op, n, expected)
				return 1
			}
			for ev in out {
				id := int(ev.payload)
				if id >= len(m.live) || !m.live[id] {
					fmt.eprintfln("op %d: fired payload %d which is not live in the model", op, ev.payload)
					return 1
				}
				if m.expiry[id] > target || ev.deadline_ns > now_ns || ev.deadline_ns != m.deadline[id] {
					fmt.eprintfln("op %d: payload %d fired early or with a wrong deadline", op, ev.payload)
					return 1
				}
				if ev.handle != m.handle[id] || ev.fired_ns != now_ns {
					fmt.eprintfln("op %d: payload %d came back with the wrong handle or timestamp", op, ev.payload)
					return 1
				}
				m.live[id] = false
			}
			fired_total += n
		}
		if op % 61 == 0 {
			msg := wheel_check_invariants(&w)
			if len(msg) > 0 {
				fmt.eprintfln("op %d: invariant broken: %s", op, msg)
				return 1
			}
		}
	}

	clear(&out)
	now_ns += (i64(1) << 34) * TICK
	remaining := 0
	for i in 0 ..< len(m.live) {
		if m.live[i] {
			remaining += 1
		}
	}
	n := wheel_advance(&w, now_ns, &out)
	if n != remaining || w.live != 0 {
		fmt.eprintfln("drain: fired %d, model expected %d, wheel live=%d", n, remaining, w.live)
		return 1
	}
	if msg := wheel_check_invariants(&w); len(msg) > 0 {
		fmt.eprintfln("drain: invariant broken: %s", msg)
		return 1
	}
	if verbose {
		fmt.printfln("selftest ok: seed=%d ops=%d fired=%d full_rejections=%d stale_checks=%d", seed, ops, fired_total + n, full_seen, stale_checked)
		fmt.printfln("stats: %v", w.stats)
	}
	return 0
}

// ---------------------------------------------------------------------------
// Benchmark: timing wheel against a binary heap with lazy deletion.
// ---------------------------------------------------------------------------

Heap_Item :: struct {
	expiry: u64,
	id:     u32,
	ver:    u32,
}

heap_push :: proc(h: ^[dynamic]Heap_Item, it: Heap_Item) {
	append(h, it)
	i := len(h^) - 1
	for i > 0 {
		p := (i - 1) / 2
		if h^[p].expiry <= h^[i].expiry {
			break
		}
		h^[p], h^[i] = h^[i], h^[p]
		i = p
	}
}

heap_pop :: proc(h: ^[dynamic]Heap_Item) -> Heap_Item {
	top := h^[0]
	last := pop(h)
	n := len(h^)
	if n > 0 {
		h^[0] = last
		i := 0
		for {
			l := 2 * i + 1
			r := l + 1
			s := i
			if l < n && h^[l].expiry < h^[s].expiry {
				s = l
			}
			if r < n && h^[r].expiry < h^[s].expiry {
				s = r
			}
			if s == i {
				break
			}
			h^[s], h^[i] = h^[i], h^[s]
			i = s
		}
	}
	return top
}

run_bench :: proc(timers: int, steps: int) {
	TICK :: i64(1_000_000)
	IDLE :: u64(5_000)
	TOUCHES :: 8

	w: Wheel
	if err := wheel_init(&w, timers, TICK, 0); err != .None {
		fmt.eprintln("wheel_init failed:", err)
		return
	}
	defer wheel_destroy(&w)
	handles := make([]Handle, timers)
	defer delete(handles)
	out: [dynamic]Expired
	defer delete(out)
	rng := Rng{s = 0x9E3779B97F4A7C15}

	for i in 0 ..< timers {
		h, _ := wheel_schedule(&w, i64(IDLE + rng_below(&rng, IDLE)) * TICK, u64(i))
		handles[i] = h
	}
	wheel_ops := 0
	wheel_fired := 0
	start := time.tick_now()
	now := i64(0)
	for _ in 0 ..< steps {
		now += TICK
		clear(&out)
		wheel_advance(&w, now, &out)
		for ev in out {
			wheel_fired += 1
			h, _ := wheel_schedule(&w, now + i64(IDLE) * TICK, ev.payload)
			handles[ev.payload] = h
			wheel_ops += 1
		}
		for _ in 0 ..< TOUCHES {
			i := rng_below(&rng, u64(timers))
			wheel_reschedule(&w, handles[i], now + i64(IDLE) * TICK)
			wheel_ops += 1
		}
	}
	wheel_ns := i64(time.tick_diff(start, time.tick_now()))

	heap: [dynamic]Heap_Item
	defer delete(heap)
	vers := make([]u32, timers)
	defer delete(vers)
	rng = Rng{s = 0x9E3779B97F4A7C15}
	for i in 0 ..< timers {
		heap_push(&heap, Heap_Item{expiry = IDLE + rng_below(&rng, IDLE), id = u32(i), ver = 0})
	}
	heap_ops := 0
	heap_fired := 0
	heap_peak := len(heap)
	hstart := time.tick_now()
	tick := u64(0)
	for _ in 0 ..< steps {
		tick += 1
		for len(heap) > 0 && heap[0].expiry <= tick {
			it := heap_pop(&heap)
			if vers[it.id] != it.ver {
				continue
			}
			heap_fired += 1
			vers[it.id] += 1
			heap_push(&heap, Heap_Item{expiry = tick + IDLE, id = it.id, ver = vers[it.id]})
			heap_ops += 1
		}
		for _ in 0 ..< TOUCHES {
			i := rng_below(&rng, u64(timers))
			vers[i] += 1
			heap_push(&heap, Heap_Item{expiry = tick + IDLE, id = u32(i), ver = vers[i]})
			heap_ops += 1
		}
		if len(heap) > heap_peak {
			heap_peak = len(heap)
		}
	}
	heap_ns := i64(time.tick_diff(hstart, time.tick_now()))

	fmt.printfln("timers=%d steps=%d touches_per_step=%d", timers, steps, TOUCHES)
	fmt.printfln("wheel: %d ops, %d fired, %.1f ns/op, resident entries <= %d", wheel_ops, wheel_fired, f64(wheel_ns) / f64(max(wheel_ops, 1)), w.stats.high_water)
	fmt.printfln("heap : %d ops, %d fired, %.1f ns/op, peak heap size %d (stale copies included)", heap_ops, heap_fired, f64(heap_ns) / f64(max(heap_ops, 1)), heap_peak)
}

parse_uint :: proc(s: string) -> (v: u64, ok: bool) {
	if len(s) == 0 {
		return 0, false
	}
	for c in s {
		if c < '0' || c > '9' {
			return 0, false
		}
		v = v * 10 + u64(c - '0')
	}
	return v, true
}

usage :: proc() {
	fmt.eprintln("usage:")
	fmt.eprintln("  DeadlineTimingWheel selftest [seed] [ops]   differential test against a brute force model")
	fmt.eprintln("  DeadlineTimingWheel bench [timers] [steps]  wheel versus lazy deletion heap")
}

main :: proc() {
	args := os.args
	if len(args) < 2 {
		usage()
		os.exit(2)
	}
	switch args[1] {
	case "selftest":
		seed := u64(1)
		ops := 200_000
		if len(args) > 2 {
			v, ok := parse_uint(args[2])
			if !ok {
				usage()
				os.exit(2)
			}
			seed = v
		}
		if len(args) > 3 {
			v, ok := parse_uint(args[3])
			if !ok || v == 0 {
				usage()
				os.exit(2)
			}
			ops = int(v)
		}
		if run_selftest(seed, ops, true) != 0 {
			os.exit(1)
		}
	case "bench":
		timers := 200_000
		steps := 20_000
		if len(args) > 2 {
			v, ok := parse_uint(args[2])
			if !ok || v == 0 {
				usage()
				os.exit(2)
			}
			timers = int(v)
		}
		if len(args) > 3 {
			v, ok := parse_uint(args[3])
			if !ok || v == 0 {
				usage()
				os.exit(2)
			}
			steps = int(v)
		}
		run_bench(timers, steps)
	case:
		usage()
		os.exit(2)
	}
}
