import gleam/int
import gleam/list
import gleam/order.{Gt, Lt}
import gleam/string
import gleeunit
import hybrid_logical_clock as hlc

pub fn main() {
  gleeunit.main()
}

fn clock(node: String, now: Int) -> hlc.Clock {
  let assert Ok(c) = hlc.new(node, now, hlc.default_config())
  c
}

pub fn same_millisecond_bumps_counter_test() {
  let c = clock("a", 1000)
  let assert Ok(#(c, first)) = hlc.tick(c, 1000)
  let assert Ok(#(_, second)) = hlc.tick(c, 1000)
  assert first == hlc.Timestamp(1000, 0, "a")
  assert second == hlc.Timestamp(1000, 1, "a")
}

pub fn backwards_physical_clock_stays_monotonic_test() {
  let c = clock("a", 5000)
  let assert Ok(#(c, first)) = hlc.tick(c, 5000)
  let assert Ok(#(_, second)) = hlc.tick(c, 4000)
  assert hlc.compare(second, first) == Gt
}

pub fn counter_overflow_borrows_next_millisecond_test() {
  let config = hlc.Config(max_drift_ms: 10, max_counter: 1)
  let assert Ok(c) = hlc.new("a", 1000, config)
  let assert Ok(#(c, _)) = hlc.tick(c, 1000)
  let assert Ok(#(c, _)) = hlc.tick(c, 1000)
  let assert Ok(#(_, third)) = hlc.tick(c, 1000)
  assert third == hlc.Timestamp(1001, 0, "a")
}

pub fn drift_exceeded_leaves_clock_usable_test() {
  let config = hlc.Config(max_drift_ms: 1, max_counter: 65_535)
  let assert Ok(c) = hlc.new("a", 1000, config)
  let assert Ok(#(c, _)) = hlc.tick(c, 1000)
  let assert Ok(#(c, _)) = hlc.tick(c, 1000)
  let assert Error(hlc.DriftExceeded(retry_after_ms: 1, ..)) = hlc.tick(c, 1000)
  let assert Ok(#(_, later)) = hlc.tick(c, 1002)
  assert later.wall_ms == 1002
}

pub fn observe_orders_after_remote_test() {
  let c = clock("a", 1000)
  let remote = hlc.Timestamp(1030, 4, "b")
  let assert Ok(#(c, stamp)) = hlc.observe(c, 1000, remote)
  assert stamp == hlc.Timestamp(1030, 5, "a")
  assert hlc.compare(remote, stamp) == Lt
  assert hlc.last_issued(c) == stamp
}

pub fn observe_rejects_far_future_remote_test() {
  let c = clock("a", 1000)
  let remote = hlc.Timestamp(1000 + 60_001, 0, "b")
  let assert Error(hlc.RemoteAhead(..)) = hlc.observe(c, 1000, remote)
}

pub fn observe_rejects_own_node_id_test() {
  let c = clock("a", 1000)
  let assert Error(hlc.DuplicateNode("a")) =
    hlc.observe(c, 1000, hlc.Timestamp(900, 0, "a"))
}

pub fn resume_never_reissues_old_stamps_test() {
  let persisted = hlc.Timestamp(9000, 7, "a")
  let assert Ok(c) = hlc.resume("a", persisted, 8000, hlc.default_config())
  let assert Ok(#(_, stamp)) = hlc.tick(c, 8000)
  assert stamp == hlc.Timestamp(9000, 8, "a")
}

pub fn encoding_sorts_like_timestamps_test() {
  let stamps = [
    hlc.Timestamp(255, 0, "b"),
    hlc.Timestamp(16, 65_535, "a"),
    hlc.Timestamp(255, 0, "a"),
    hlc.Timestamp(255, 1, "a"),
  ]
  let by_timestamp = list.sort(stamps, hlc.compare)
  let by_text =
    stamps
    |> list.map(hlc.encode)
    |> list.sort(string.compare)
    |> list.map(fn(text) {
      let assert Ok(ts) = hlc.decode(text)
      ts
    })
  assert by_timestamp == by_text
}

pub fn decode_rejects_garbage_test() {
  let assert Error(hlc.MalformedTimestamp(_)) = hlc.decode("nope")
  let assert Error(hlc.MalformedTimestamp(_)) =
    hlc.decode("00000000zzzz-0000-a")
  let assert Error(hlc.InvalidNode(_)) = hlc.decode("000000000001-0000-")
  let assert Error(hlc.MalformedTimestamp(_)) = hlc.decode("0001-0000-a")
  assert int.to_string(1) == "1"
}

pub fn lww_replicas_converge_in_any_order_test() {
  let a = hlc.lww_new() |> hlc.lww_set("k", "from a", hlc.Timestamp(10, 0, "a"))
  let b = hlc.lww_new() |> hlc.lww_set("k", "from b", hlc.Timestamp(10, 0, "b"))
  assert hlc.lww_get(hlc.lww_merge(a, b), "k")
    == hlc.lww_get(hlc.lww_merge(b, a), "k")
  assert hlc.lww_get(hlc.lww_merge(a, b), "k") == Ok("from b")
}

pub fn tombstone_blocks_delayed_older_write_test() {
  let m =
    hlc.lww_new()
    |> hlc.lww_set("k", 1, hlc.Timestamp(10, 0, "a"))
    |> hlc.lww_delete("k", hlc.Timestamp(20, 0, "a"))
    |> hlc.lww_set("k", 2, hlc.Timestamp(15, 0, "b"))
  assert hlc.lww_get(m, "k") == Error(Nil)
}

pub fn delta_roundtrip_matches_full_merge_test() {
  let base = hlc.lww_new() |> hlc.lww_set("x", 1, hlc.Timestamp(1, 0, "a"))
  let newer =
    base
    |> hlc.lww_set("y", 2, hlc.Timestamp(5, 0, "a"))
    |> hlc.lww_delete("x", hlc.Timestamp(6, 0, "a"))
  let assert Ok(cursor) = hlc.lww_high_water(base)
  let delta = hlc.lww_delta_since(newer, cursor)
  assert list.length(delta) == 2
  assert hlc.lww_live_entries(hlc.lww_apply_delta(base, delta))
    == hlc.lww_live_entries(newer)
}

pub fn prune_drops_only_old_tombstones_test() {
  let m =
    hlc.lww_new()
    |> hlc.lww_set("live", 1, hlc.Timestamp(1, 0, "a"))
    |> hlc.lww_delete("old", hlc.Timestamp(2, 0, "a"))
    |> hlc.lww_delete("new", hlc.Timestamp(90, 0, "a"))
  let pruned = hlc.lww_prune_tombstones(m, 50)
  assert list.length(hlc.lww_delta_since(pruned, hlc.Timestamp(0, 0, "a"))) == 2
}
