import gleam/list
import gleam/option.{None, Some}
import gleeunit
import lsm_compaction_planner as lsm

pub fn main() {
  gleeunit.main()
}

fn config() -> lsm.Config {
  lsm.Config(
    level0_trigger: 2,
    level0_slowdown: 4,
    level0_stop: 6,
    base_level_bytes: 1000,
    multiplier: 10,
    level_count: 4,
    max_compaction_bytes: 5000,
    target_table_bytes: 500,
    tombstone_permille: 300,
  )
}

fn t(id: Int, level: Int, lo: String, hi: String, bytes: Int) -> lsm.Table {
  lsm.Table(
    id: id,
    level: level,
    smallest: lo,
    largest: hi,
    bytes: bytes,
    entries: 100,
    tombstones: 0,
    age: id,
  )
}

fn ids(tables: List(lsm.Table)) -> List(Int) {
  list.map(tables, fn(x) { x.id })
}

pub fn idle_when_under_every_trigger_test() {
  let tables = [t(1, 0, "a", "c", 100), t(2, 1, "a", "c", 500)]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  assert p.job == None
  assert p.write_state == lsm.Normal
  assert p.pending_bytes == 0
}

pub fn l0_job_takes_overlapping_tables_and_the_l1_neighbour_test() {
  let tables = [
    t(1, 0, "a", "c", 100),
    t(2, 0, "b", "e", 100),
    t(3, 0, "x", "z", 100),
    t(10, 1, "d", "f", 100),
    t(11, 1, "m", "n", 100),
  ]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  let assert Some(job) = p.job
  assert ids(job.upper) == [1, 2]
  assert ids(job.lower) == [10]
  assert job.reason == lsm.L0Overflow
  assert job.kind == lsm.Merge
  assert job.to_level == 1
}

pub fn l0_closure_follows_chains_of_overlap_test() {
  let tables = [
    t(1, 0, "a", "b", 100),
    t(2, 0, "b", "c", 100),
    t(3, 0, "c", "d", 100),
    t(4, 0, "x", "y", 100),
  ]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  let assert Some(job) = p.job
  assert ids(job.upper) == [1, 2, 3]
}

pub fn level_overflow_prefers_the_table_with_least_overlap_test() {
  let tables = [
    t(20, 1, "a", "c", 600),
    t(21, 1, "d", "f", 600),
    t(30, 2, "a", "b", 5000),
  ]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  let assert Some(job) = p.job
  assert ids(job.upper) == [21]
  assert job.kind == lsm.TrivialMove
  assert job.bytes_written == 0
  assert job.write_amp_permille == 0
  assert job.to_level == 2
}

pub fn over_budget_job_is_deferred_when_writers_are_not_stalled_test() {
  let tables = [
    t(1, 0, "a", "c", 3000),
    t(2, 0, "b", "d", 3000),
    t(10, 1, "a", "z", 900),
  ]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  assert p.job == None
  assert p.deferred == [lsm.Deferral(0, lsm.OverBudget(6900, 5000))]
}

pub fn stopped_level0_forces_the_job_over_budget_test() {
  let tables =
    list.map([1, 2, 3, 4, 5, 6], fn(i) { t(i, 0, "a", "z", 1000) })
  let assert Ok(p) = lsm.plan(config(), tables, [])
  let assert Some(job) = p.job
  assert job.forced
  assert p.write_state == lsm.Stopped
}

pub fn busy_tables_are_never_chosen_test() {
  let tables = [t(1, 0, "a", "c", 100), t(2, 0, "b", "e", 100)]
  let assert Ok(p) = lsm.plan(config(), tables, [2])
  assert p.job == None
  assert p.deferred == [lsm.Deferral(0, lsm.TouchesBusy(2))]
}

pub fn busy_ids_must_exist_test() {
  let tables = [t(1, 0, "a", "c", 100)]
  let assert Error(lsm.UnknownBusyTable(99)) = lsm.plan(config(), tables, [99])
}

pub fn overlapping_tables_in_a_sorted_level_are_rejected_test() {
  let tables = [t(1, 1, "a", "c", 100), t(2, 1, "b", "d", 100)]
  let assert Error(lsm.OverlapInLevel(1, 1, 2)) = lsm.plan(config(), tables, [])
}

pub fn duplicate_ids_are_rejected_test() {
  let tables = [t(1, 0, "a", "c", 100), t(1, 0, "d", "e", 100)]
  let assert Error(lsm.DuplicateTableId(1)) = lsm.plan(config(), tables, [])
}

pub fn bad_config_is_rejected_test() {
  let bad = lsm.Config(..config(), multiplier: 1)
  let assert Error(lsm.InvalidConfig(_)) = lsm.plan(bad, [], [])
}

pub fn data_below_the_range_keeps_tombstones_alive_test() {
  let tables = [
    t(1, 0, "a", "c", 100),
    t(2, 0, "b", "d", 100),
    t(30, 2, "a", "z", 100),
  ]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  let assert Some(job) = p.job
  assert job.drops_tombstones == False
}

pub fn bottom_level_tombstone_rewrite_reclaims_space_test() {
  let heavy =
    lsm.Table(
      id: 50,
      level: 3,
      smallest: "a",
      largest: "m",
      bytes: 800,
      entries: 100,
      tombstones: 50,
      age: 50,
    )
  let assert Ok(p) = lsm.plan(config(), [heavy], [])
  let assert Some(job) = p.job
  assert job.reason == lsm.TombstoneCleanup
  assert job.from_level == 3
  assert job.to_level == 3
  assert job.drops_tombstones
  assert job.bytes_written == 400
  assert job.tables_written == 1
}

pub fn commit_installs_outputs_and_removes_inputs_test() {
  let tables = [
    t(1, 0, "a", "c", 100),
    t(2, 0, "b", "e", 100),
    t(10, 1, "d", "f", 100),
    t(11, 1, "m", "n", 100),
  ]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  let assert Some(job) = p.job
  let output = t(99, 1, "a", "f", 300)
  let assert Ok(next) = lsm.commit(config(), tables, job, [output])
  assert ids(next) == [99, 11]
}

pub fn commit_rejects_output_outside_the_input_span_test() {
  let tables = [t(1, 0, "b", "c", 100), t(2, 0, "b", "d", 100)]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  let assert Some(job) = p.job
  let stray = t(99, 1, "a", "z", 300)
  let assert Error(lsm.CommitMismatch(_)) =
    lsm.commit(config(), tables, job, [stray])
}

pub fn commit_rejects_outputs_that_overlap_each_other_test() {
  let tables = [t(1, 0, "b", "c", 100), t(2, 0, "b", "d", 100)]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  let assert Some(job) = p.job
  let first = t(98, 1, "b", "c", 150)
  let second = t(99, 1, "c", "d", 150)
  let assert Error(lsm.OverlapInLevel(1, _, _)) =
    lsm.commit(config(), tables, job, [first, second])
}

pub fn commit_moves_a_single_table_without_outputs_test() {
  let tables = [t(21, 1, "d", "f", 1500)]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  let assert Some(job) = p.job
  assert job.kind == lsm.TrivialMove
  let assert Ok([moved]) = lsm.commit(config(), tables, job, [])
  assert moved.level == 2
  assert moved.id == 21
}

pub fn commit_detects_a_changed_input_test() {
  let tables = [t(21, 1, "d", "f", 1500)]
  let assert Ok(p) = lsm.plan(config(), tables, [])
  let assert Some(job) = p.job
  let changed = [t(21, 1, "d", "f", 1400)]
  let assert Error(lsm.CommitMismatch(_)) = lsm.commit(config(), changed, job, [])
}
