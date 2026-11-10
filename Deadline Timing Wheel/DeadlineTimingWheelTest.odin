package main

import "core:testing"

TEST_TICK :: i64(1_000_000)

make_wheel :: proc(t: ^testing.T, capacity: int) -> Wheel {
	w: Wheel
	err := wheel_init(&w, capacity, TEST_TICK, 0)
	testing.expect_value(t, err, Error.None)
	return w
}

@(test)
fires_at_deadline_and_never_early :: proc(t: ^testing.T) {
	w := make_wheel(t, 8)
	defer wheel_destroy(&w)
	out: [dynamic]Expired
	defer delete(out)
	_, err := wheel_schedule(&w, 10 * TEST_TICK + 1, 7)
	testing.expect_value(t, err, Error.None)
	testing.expect_value(t, wheel_advance(&w, 10 * TEST_TICK, &out), 0)
	testing.expect_value(t, wheel_advance(&w, 11 * TEST_TICK - 1, &out), 0)
	testing.expect_value(t, wheel_advance(&w, 11 * TEST_TICK, &out), 1)
	testing.expect_value(t, out[0].payload, u64(7))
}

@(test)
same_tick_timers_fire_in_insertion_order :: proc(t: ^testing.T) {
	w := make_wheel(t, 8)
	defer wheel_destroy(&w)
	out: [dynamic]Expired
	defer delete(out)
	for i in 1 ..= 3 {
		wheel_schedule(&w, 5 * TEST_TICK, u64(i))
	}
	wheel_advance(&w, 5 * TEST_TICK, &out)
	testing.expect_value(t, len(out), 3)
	testing.expect_value(t, out[0].payload, u64(1))
	testing.expect_value(t, out[1].payload, u64(2))
	testing.expect_value(t, out[2].payload, u64(3))
}

@(test)
past_due_timer_fires_on_next_tick :: proc(t: ^testing.T) {
	w := make_wheel(t, 4)
	defer wheel_destroy(&w)
	out: [dynamic]Expired
	defer delete(out)
	wheel_advance(&w, 100 * TEST_TICK, &out)
	wheel_schedule(&w, 3 * TEST_TICK, 1)
	testing.expect_value(t, w.stats.past_due_clamped, u64(1))
	testing.expect_value(t, wheel_advance(&w, 100 * TEST_TICK, &out), 0)
	testing.expect_value(t, wheel_advance(&w, 101 * TEST_TICK, &out), 1)
}

@(test)
cancel_then_cancel_again_is_stale_not_fatal :: proc(t: ^testing.T) {
	w := make_wheel(t, 4)
	defer wheel_destroy(&w)
	h, _ := wheel_schedule(&w, 50 * TEST_TICK, 1)
	testing.expect_value(t, wheel_cancel(&w, h), Error.None)
	testing.expect_value(t, wheel_cancel(&w, h), Error.Stale_Handle)
	testing.expect_value(t, wheel_cancel(&w, Handle{}), Error.Bad_Handle)
	testing.expect_value(t, w.live, 0)
}

@(test)
handle_is_stale_after_fire_and_slot_reuse :: proc(t: ^testing.T) {
	w := make_wheel(t, 1)
	defer wheel_destroy(&w)
	out: [dynamic]Expired
	defer delete(out)
	old, _ := wheel_schedule(&w, 2 * TEST_TICK, 1)
	wheel_advance(&w, 2 * TEST_TICK, &out)
	fresh, _ := wheel_schedule(&w, 90 * TEST_TICK, 2)
	testing.expect_value(t, fresh.index, old.index)
	testing.expect_value(t, wheel_cancel(&w, old), Error.Stale_Handle)
	testing.expect_value(t, wheel_cancel(&w, fresh), Error.None)
}

@(test)
reschedule_extends_an_idle_timeout :: proc(t: ^testing.T) {
	w := make_wheel(t, 4)
	defer wheel_destroy(&w)
	out: [dynamic]Expired
	defer delete(out)
	h, _ := wheel_schedule(&w, 30 * TEST_TICK, 1)
	wheel_advance(&w, 20 * TEST_TICK, &out)
	testing.expect_value(t, wheel_reschedule(&w, h, 50 * TEST_TICK), Error.None)
	testing.expect_value(t, wheel_advance(&w, 49 * TEST_TICK, &out), 0)
	testing.expect_value(t, wheel_advance(&w, 50 * TEST_TICK, &out), 1)
}

@(test)
full_wheel_rejects_then_recovers :: proc(t: ^testing.T) {
	w := make_wheel(t, 2)
	defer wheel_destroy(&w)
	a, _ := wheel_schedule(&w, 10 * TEST_TICK, 1)
	wheel_schedule(&w, 10 * TEST_TICK, 2)
	_, err := wheel_schedule(&w, 10 * TEST_TICK, 3)
	testing.expect_value(t, err, Error.Full)
	wheel_cancel(&w, a)
	_, err = wheel_schedule(&w, 10 * TEST_TICK, 3)
	testing.expect_value(t, err, Error.None)
}

@(test)
far_deadlines_survive_the_overflow_list :: proc(t: ^testing.T) {
	w := make_wheel(t, 4)
	defer wheel_destroy(&w)
	out: [dynamic]Expired
	defer delete(out)
	far := (i64(1) << 31) * TEST_TICK
	wheel_schedule(&w, far, 1)
	testing.expect_value(t, wheel_advance(&w, far - TEST_TICK, &out), 0)
	testing.expect_value(t, wheel_advance(&w, far, &out), 1)
	testing.expect_value(t, wheel_check_invariants(&w), "")
}

@(test)
clock_regression_is_ignored_and_counted :: proc(t: ^testing.T) {
	w := make_wheel(t, 4)
	defer wheel_destroy(&w)
	out: [dynamic]Expired
	defer delete(out)
	wheel_schedule(&w, 100 * TEST_TICK, 1)
	wheel_advance(&w, 60 * TEST_TICK, &out)
	testing.expect_value(t, wheel_advance(&w, 10 * TEST_TICK, &out), 0)
	testing.expect_value(t, w.stats.clock_regressions, u64(1))
	testing.expect_value(t, wheel_advance(&w, 100 * TEST_TICK, &out), 1)
}

@(test)
next_deadline_is_a_lower_bound :: proc(t: ^testing.T) {
	w := make_wheel(t, 4)
	defer wheel_destroy(&w)
	_, ok := wheel_next_deadline_ns(&w)
	testing.expect(t, !ok, "empty wheel has no deadline")
	wheel_schedule(&w, 5000 * TEST_TICK, 1)
	ns, ok2 := wheel_next_deadline_ns(&w)
	testing.expect(t, ok2, "pending timer must report a deadline")
	testing.expect(t, ns <= 5000 * TEST_TICK, "bound must not pass the real deadline")
	testing.expect(t, ns > 0, "bound must be in the future")
}

@(test)
differential_fuzz_matches_brute_force_model :: proc(t: ^testing.T) {
	for seed in 1 ..= 5 {
		testing.expect_value(t, run_selftest(u64(seed), 40_000, false), 0)
	}
}
