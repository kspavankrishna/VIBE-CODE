import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order.{type Order, Eq, Gt, Lt}
import gleam/result
import gleam/set.{type Set}
import gleam/string

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// One immutable sorted table on disk. Keys are compared with string.compare,
/// so feed the planner keys in the same order your engine sorts them. Hex or
/// ASCII keys are safe on every target. age is a monotonically increasing
/// sequence number, lower means older.
pub type Table {
  Table(
    id: Int,
    level: Int,
    smallest: String,
    largest: String,
    bytes: Int,
    entries: Int,
    tombstones: Int,
    age: Int,
  )
}

pub type Config {
  Config(
    /// Number of level 0 tables that makes level 0 eligible for compaction.
    level0_trigger: Int,
    /// Level 0 table count at which writers should be slowed down.
    level0_slowdown: Int,
    /// Level 0 table count at which writers must stop. Also lifts the byte
    /// budget on level 0 jobs, because a stalled engine has to make progress.
    level0_stop: Int,
    /// Byte target of level 1. Level n holds base * multiplier^(n - 1).
    base_level_bytes: Int,
    multiplier: Int,
    /// Total levels including level 0. The last one is the bottom level.
    level_count: Int,
    /// Largest input set a single job may read, unless it is forced.
    max_compaction_bytes: Int,
    /// Size at which a compaction output is cut into a new table.
    target_table_bytes: Int,
    /// A table is tombstone heavy when tombstones / entries reaches this
    /// many parts per thousand.
    tombstone_permille: Int,
  )
}

pub type Reason {
  L0Overflow
  LevelOverflow
  TombstoneCleanup
}

pub type Kind {
  /// Inputs are merged and rewritten.
  Merge
  /// A single table slides down a level without being rewritten.
  TrivialMove
}

pub type Job {
  Job(
    reason: Reason,
    kind: Kind,
    from_level: Int,
    to_level: Int,
    upper: List(Table),
    lower: List(Table),
    upper_bytes: Int,
    lower_bytes: Int,
    bytes_read: Int,
    bytes_written: Int,
    tables_written: Int,
    /// Bytes written per byte of upper level data, in parts per thousand.
    /// 1000 is a free ride, 11000 means eleven bytes written per byte moved.
    write_amp_permille: Int,
    /// True when no older data can exist outside the inputs, so tombstones
    /// can be discarded instead of copied forward.
    drops_tombstones: Bool,
    /// True when the input set is over max_compaction_bytes and the job runs
    /// anyway because level 0 hit level0_stop.
    forced: Bool,
  )
}

pub type DeferReason {
  OverBudget(bytes: Int, limit: Int)
  TouchesBusy(table_id: Int)
  NothingEligible
}

pub type Deferral {
  Deferral(level: Int, reason: DeferReason)
}

pub type WriteState {
  Normal
  Slowdown
  Stopped
}

pub type LevelScore {
  LevelScore(level: Int, tables: Int, bytes: Int, score_permille: Int)
}

pub type Plan {
  Plan(
    /// The one job to start now, if any candidate fit.
    job: Option(Job),
    /// Candidates that were tried before the chosen job, in order, with the
    /// reason each was skipped.
    deferred: List(Deferral),
    scores: List(LevelScore),
    write_state: WriteState,
    /// Bytes above target summed over levels 1 and deeper.
    pending_bytes: Int,
  )
}

pub type PlanError {
  InvalidConfig(reason: String)
  InvalidTable(id: Int, reason: String)
  DuplicateTableId(id: Int)
  OverlapInLevel(level: Int, first: Int, second: Int)
  UnknownBusyTable(id: Int)
  CommitMismatch(reason: String)
}

type Candidate {
  SizeCandidate(level: Int)
  TombstoneCandidate(level: Int)
}

/// Keeps level targets from overflowing a JavaScript safe integer.
const max_target = 9_007_199_254_740_991

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Defaults in the same shape as a typical RocksDB style leveled setup:
/// 256 MiB for level 1, ten times growth, seven levels, 64 MiB tables.
pub fn default_config() -> Config {
  Config(
    level0_trigger: 4,
    level0_slowdown: 20,
    level0_stop: 36,
    base_level_bytes: 268_435_456,
    multiplier: 10,
    level_count: 7,
    max_compaction_bytes: 2_147_483_648,
    target_table_bytes: 67_108_864,
    tombstone_permille: 300,
  )
}

pub fn validate_config(config: Config) -> Result(Config, PlanError) {
  let checks = [
    #(config.level0_trigger >= 1, "level0_trigger must be at least 1"),
    #(
      config.level0_slowdown >= config.level0_trigger,
      "level0_slowdown must not be below level0_trigger",
    ),
    #(
      config.level0_stop >= config.level0_slowdown,
      "level0_stop must not be below level0_slowdown",
    ),
    #(
      config.level_count >= 2 && config.level_count <= 16,
      "level_count must be between 2 and 16",
    ),
    #(config.base_level_bytes >= 1, "base_level_bytes must be at least 1"),
    #(config.multiplier >= 2, "multiplier must be at least 2"),
    #(config.target_table_bytes >= 1, "target_table_bytes must be at least 1"),
    #(
      config.max_compaction_bytes >= config.target_table_bytes,
      "max_compaction_bytes must not be below target_table_bytes",
    ),
    #(
      config.tombstone_permille >= 1 && config.tombstone_permille <= 1000,
      "tombstone_permille must be between 1 and 1000",
    ),
  ]
  case list.find(checks, fn(check) { !check.0 }) {
    Ok(#(_, reason)) -> Error(InvalidConfig(reason))
    Error(_) -> Ok(config)
  }
}

/// Byte target of a level. Level 0 is sized by table count, so it has none.
pub fn level_target(config: Config, level: Int) -> Int {
  case level <= 0 {
    True -> 0
    False ->
      case level == 1 {
        True -> config.base_level_bytes
        False ->
          int.min(
            level_target(config, level - 1) * config.multiplier,
            max_target,
          )
      }
  }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Checks every table and the level invariants the planner relies on: ids are
/// unique, ranges are ordered, tombstone counts are sane and levels 1 and
/// deeper never contain overlapping tables. A tree that fails this was
/// corrupted by whoever built the table list, and planning on it would pick
/// jobs that silently drop or resurrect data.
pub fn validate_tables(
  config: Config,
  tables: List(Table),
) -> Result(Nil, PlanError) {
  use _ <- result.try(list.try_each(tables, fn(t) { validate_table(config, t) }))
  use _ <- result.try(check_unique(tables))
  list.try_each(list.range(1, config.level_count - 1), fn(level) {
    let sorted = list.sort(at_level(tables, level), compare_by_smallest)
    case first_overlap(sorted) {
      Some(#(a, b)) -> Error(OverlapInLevel(level, a.id, b.id))
      None -> Ok(Nil)
    }
  })
}

fn validate_table(config: Config, t: Table) -> Result(Nil, PlanError) {
  let checks = [
    #(t.level >= 0 && t.level < config.level_count, "level out of range"),
    #(string.compare(t.smallest, t.largest) != Gt, "smallest is above largest"),
    #(t.bytes >= 0, "bytes is negative"),
    #(t.entries >= 0, "entries is negative"),
    #(
      t.tombstones >= 0 && t.tombstones <= t.entries,
      "tombstones must be between 0 and entries",
    ),
  ]
  case list.find(checks, fn(check) { !check.0 }) {
    Ok(#(_, reason)) -> Error(InvalidTable(t.id, reason))
    Error(_) -> Ok(Nil)
  }
}

fn check_unique(tables: List(Table)) -> Result(Nil, PlanError) {
  list.try_fold(tables, set.new(), fn(seen, t) {
    case set.contains(seen, t.id) {
      True -> Error(DuplicateTableId(t.id))
      False -> Ok(set.insert(seen, t.id))
    }
  })
  |> result.map(fn(_) { Nil })
}

/// Input must be sorted by smallest key. When every adjacent pair is clean
/// no pair anywhere overlaps, so one pass is enough.
fn first_overlap(sorted: List(Table)) -> Option(#(Table, Table)) {
  case sorted {
    [a, b, ..rest] ->
      case string.compare(b.smallest, a.largest) {
        Gt -> first_overlap([b, ..rest])
        _ -> Some(#(a, b))
      }
    _ -> None
  }
}

// ---------------------------------------------------------------------------
// Scores and write state
// ---------------------------------------------------------------------------

/// Score of each level in parts per thousand. Level 0 is table count over the
/// trigger, deeper levels are bytes over target. 1000 or more wants work.
pub fn level_scores(config: Config, tables: List(Table)) -> List(LevelScore) {
  list.map(list.range(0, config.level_count - 1), fn(level) {
    let in_level = at_level(tables, level)
    let count = list.length(in_level)
    let bytes = sum_bytes(in_level)
    let score = case level {
      0 -> count * 1000 / config.level0_trigger
      _ -> bytes * 1000 / level_target(config, level)
    }
    LevelScore(level: level, tables: count, bytes: bytes, score_permille: score)
  })
}

pub fn write_state(config: Config, level0_tables: Int) -> WriteState {
  case level0_tables >= config.level0_stop {
    True -> Stopped
    False ->
      case level0_tables >= config.level0_slowdown {
        True -> Slowdown
        False -> Normal
      }
  }
}

fn pending_bytes(config: Config, scores: List(LevelScore)) -> Int {
  list.fold(scores, 0, fn(acc, s) {
    case s.level >= 1 {
      True -> acc + int.max(0, s.bytes - level_target(config, s.level))
      False -> acc
    }
  })
}

// ---------------------------------------------------------------------------
// Planning
// ---------------------------------------------------------------------------

/// Decide the next compaction. busy lists table ids already claimed by running
/// jobs, those tables are never chosen. The function is pure: same tree, same
/// busy set, same answer. Call it again each time a job finishes or a flush
/// lands. Candidates are tried in this order: level 0 and size overflows by
/// descending score, then tombstone heavy levels from the bottom up. The first
/// candidate that fits the byte budget and avoids busy tables wins, the rest
/// are reported in deferred.
pub fn plan(
  config: Config,
  tables: List(Table),
  busy: List(Int),
) -> Result(Plan, PlanError) {
  use config <- result.try(validate_config(config))
  use _ <- result.try(validate_tables(config, tables))
  use busy_set <- result.try(check_busy(tables, busy))
  let scores = level_scores(config, tables)
  let l0_count = list.length(at_level(tables, 0))
  let #(job, deferred) =
    pick(candidates(config, tables, scores), [], fn(c) {
      build(config, tables, busy_set, l0_count, c)
    })
  Ok(Plan(
    job: job,
    deferred: list.reverse(deferred),
    scores: scores,
    write_state: write_state(config, l0_count),
    pending_bytes: pending_bytes(config, scores),
  ))
}

fn check_busy(
  tables: List(Table),
  busy: List(Int),
) -> Result(Set(Int), PlanError) {
  let known = set.from_list(list.map(tables, fn(t) { t.id }))
  case list.find(busy, fn(id) { !set.contains(known, id) }) {
    Ok(id) -> Error(UnknownBusyTable(id))
    Error(_) -> Ok(set.from_list(busy))
  }
}

fn candidates(
  config: Config,
  tables: List(Table),
  scores: List(LevelScore),
) -> List(Candidate) {
  let bottom = config.level_count - 1
  let by_size =
    scores
    |> list.filter(fn(s) { s.level < bottom && s.score_permille >= 1000 })
    |> list.sort(fn(a, b) {
      case int.compare(b.score_permille, a.score_permille) {
        Eq -> int.compare(a.level, b.level)
        other -> other
      }
    })
    |> list.map(fn(s) { SizeCandidate(s.level) })
  let by_tombstone =
    list.range(1, bottom)
    |> list.filter(fn(level) {
      list.any(at_level(tables, level), fn(t) { is_tombstone_heavy(config, t) })
    })
    |> list.reverse
    |> list.map(fn(level) { TombstoneCandidate(level) })
  list.append(by_size, by_tombstone)
}

fn pick(
  remaining: List(Candidate),
  deferred: List(Deferral),
  try_build: fn(Candidate) -> Result(Job, Deferral),
) -> #(Option(Job), List(Deferral)) {
  case remaining {
    [] -> #(None, deferred)
    [c, ..rest] ->
      case try_build(c) {
        Ok(job) -> #(Some(job), deferred)
        Error(d) -> pick(rest, [d, ..deferred], try_build)
      }
  }
}

fn build(
  config: Config,
  tables: List(Table),
  busy: Set(Int),
  l0_count: Int,
  candidate: Candidate,
) -> Result(Job, Deferral) {
  let bottom = config.level_count - 1
  case candidate {
    SizeCandidate(0) -> {
      let l0 = list.sort(at_level(tables, 0), compare_by_age)
      case l0 {
        [] -> Error(Deferral(0, NothingEligible))
        [seed, ..] ->
          finish(
            config,
            tables,
            busy,
            L0Overflow,
            0,
            l0_closure(l0, seed),
            l0_count >= config.level0_stop,
          )
      }
    }
    SizeCandidate(level) -> {
      let next = at_level(tables, level + 1)
      let rank = fn(t: Table) { overlap_bytes(next, t) * 1000 / int.max(t.bytes, 1) }
      build_ranked(
        config,
        tables,
        busy,
        LevelOverflow,
        level,
        at_level(tables, level),
        rank,
      )
    }
    TombstoneCandidate(level) -> {
      let heavy =
        list.filter(at_level(tables, level), fn(t) {
          is_tombstone_heavy(config, t)
        })
      let rank = fn(t: Table) { 1000 - tombstone_ratio(t) }
      case level == bottom {
        True -> rewrite_bottom(config, busy, level, heavy, rank)
        False ->
          build_ranked(
            config,
            tables,
            busy,
            TombstoneCleanup,
            level,
            heavy,
            rank,
          )
      }
    }
  }
}

/// Level 0 tables overlap each other, so a job has to take the oldest table
/// and every table connected to it by key range, directly or through a chain.
/// Skipping one would leave an older or newer version of a key behind in
/// level 0 after the merged copy landed in level 1.
fn l0_closure(l0: List(Table), seed: Table) -> List(Table) {
  grow(
    list.filter(l0, fn(t) { t.id != seed.id }),
    [seed],
    seed.smallest,
    seed.largest,
  )
}

fn grow(
  pool: List(Table),
  chosen: List(Table),
  lo: String,
  hi: String,
) -> List(Table) {
  let #(hit, rest) = list.partition(pool, fn(t) { overlaps(lo, hi, t) })
  case hit {
    [] -> chosen
    _ -> {
      let lo2 = list.fold(hit, lo, fn(acc, t) { min_key(acc, t.smallest) })
      let hi2 = list.fold(hit, hi, fn(acc, t) { max_key(acc, t.largest) })
      grow(rest, list.append(chosen, hit), lo2, hi2)
    }
  }
}

/// Picks a seed table by ascending rank, then age, then id, and tries each in
/// turn so one busy neighbour does not block the whole level.
fn build_ranked(
  config: Config,
  tables: List(Table),
  busy: Set(Int),
  reason: Reason,
  level: Int,
  eligible: List(Table),
  rank: fn(Table) -> Int,
) -> Result(Job, Deferral) {
  let seeds =
    eligible
    |> list.filter(fn(t) { !set.contains(busy, t.id) })
    |> list.map(fn(t) { #(rank(t), t) })
    |> list.sort(fn(a, b) {
      case int.compare(a.0, b.0) {
        Eq -> compare_by_age(a.1, b.1)
        other -> other
      }
    })
    |> list.map(fn(pair) { pair.1 })
  case seeds {
    [] ->
      case eligible {
        [t, ..] -> Error(Deferral(level, TouchesBusy(t.id)))
        [] -> Error(Deferral(level, NothingEligible))
      }
    [head, ..rest] ->
      first_ok(head, rest, fn(t) {
        finish(config, tables, busy, reason, level, [t], False)
      })
  }
}

/// Tombstones in the bottom level can only be reclaimed by rewriting the
/// table in place, there is nowhere further down to push it.
fn rewrite_bottom(
  config: Config,
  busy: Set(Int),
  level: Int,
  heavy: List(Table),
  rank: fn(Table) -> Int,
) -> Result(Job, Deferral) {
  let free = list.filter(heavy, fn(t) { !set.contains(busy, t.id) })
  let sorted =
    list.sort(free, fn(a, b) {
      case int.compare(rank(a), rank(b)) {
        Eq -> compare_by_age(a, b)
        other -> other
      }
    })
  case sorted, heavy {
    [t, ..], _ ->
      assemble(config, [], busy, TombstoneCleanup, level, level, [t], [], False)
    [], [t, ..] -> Error(Deferral(level, TouchesBusy(t.id)))
    [], [] -> Error(Deferral(level, NothingEligible))
  }
}

fn first_ok(
  head: a,
  rest: List(a),
  f: fn(a) -> Result(b, e),
) -> Result(b, e) {
  case f(head) {
    Ok(value) -> Ok(value)
    Error(first_error) ->
      case try_rest(rest, f) {
        Ok(value) -> Ok(value)
        Error(_) -> Error(first_error)
      }
  }
}

fn try_rest(items: List(a), f: fn(a) -> Result(b, e)) -> Result(b, Nil) {
  case items {
    [] -> Error(Nil)
    [x, ..xs] ->
      case f(x) {
        Ok(value) -> Ok(value)
        Error(_) -> try_rest(xs, f)
      }
  }
}

fn finish(
  config: Config,
  tables: List(Table),
  busy: Set(Int),
  reason: Reason,
  from: Int,
  upper: List(Table),
  forced_ok: Bool,
) -> Result(Job, Deferral) {
  let #(lo, hi) = span_of(upper)
  let lower =
    at_level(tables, from + 1)
    |> list.filter(fn(t) { overlaps(lo, hi, t) })
    |> list.sort(compare_by_smallest)
  assemble(config, tables, busy, reason, from, from + 1, upper, lower, forced_ok)
}

fn assemble(
  config: Config,
  tables: List(Table),
  busy: Set(Int),
  reason: Reason,
  from: Int,
  to: Int,
  upper: List(Table),
  lower: List(Table),
  forced_ok: Bool,
) -> Result(Job, Deferral) {
  let inputs = list.append(upper, lower)
  case list.find(inputs, fn(t) { set.contains(busy, t.id) }) {
    Ok(t) -> Error(Deferral(from, TouchesBusy(t.id)))
    Error(_) -> {
      let bytes = sum_bytes(inputs)
      let over = bytes > config.max_compaction_bytes
      case over && !forced_ok {
        True ->
          Error(Deferral(from, OverBudget(bytes, config.max_compaction_bytes)))
        False -> Ok(make_job(config, tables, reason, from, to, upper, lower, over))
      }
    }
  }
}

fn make_job(
  config: Config,
  tables: List(Table),
  reason: Reason,
  from: Int,
  to: Int,
  upper: List(Table),
  lower: List(Table),
  over: Bool,
) -> Job {
  let bottom = config.level_count - 1
  let inputs = list.append(upper, lower)
  let #(lo, hi) = span_of(inputs)
  let deeper =
    list.any(tables, fn(t) { t.level > to && overlaps(lo, hi, t) })
  let drops = to == bottom || !deeper
  let upper_bytes = sum_bytes(upper)
  let lower_bytes = sum_bytes(lower)
  let total = upper_bytes + lower_bytes
  let rewrite_to_drop = reason == TombstoneCleanup && drops
  let trivial =
    from != to
    && lower == []
    && list.length(upper) == 1
    && !rewrite_to_drop
  let kind = case trivial {
    True -> TrivialMove
    False -> Merge
  }
  let dropped = case drops {
    True -> list.fold(inputs, 0, fn(acc, t) { acc + tombstone_bytes(t) })
    False -> 0
  }
  let written = case kind {
    TrivialMove -> 0
    Merge -> int.max(0, total - dropped)
  }
  let read = case kind {
    TrivialMove -> 0
    Merge -> total
  }
  Job(
    reason: reason,
    kind: kind,
    from_level: from,
    to_level: to,
    upper: upper,
    lower: lower,
    upper_bytes: upper_bytes,
    lower_bytes: lower_bytes,
    bytes_read: read,
    bytes_written: written,
    tables_written: ceil_div(written, config.target_table_bytes),
    write_amp_permille: written * 1000 / int.max(upper_bytes, 1),
    drops_tombstones: drops,
    forced: over,
  )
}

// ---------------------------------------------------------------------------
// Committing a finished job
// ---------------------------------------------------------------------------

/// Apply a finished job to the table list and re-validate the result. For a
/// TrivialMove the outputs argument is ignored and the table is re-leveled.
/// For a Merge, outputs are the tables your engine actually wrote. They must
/// sit at job.to_level, stay inside the key span of the inputs, use fresh ids
/// and leave the level free of overlaps. An empty output is only accepted when
/// the job drops tombstones. Run this before you publish the new version so a
/// buggy merge cannot install a broken tree.
pub fn commit(
  config: Config,
  tables: List(Table),
  job: Job,
  outputs: List(Table),
) -> Result(List(Table), PlanError) {
  use config <- result.try(validate_config(config))
  let inputs = list.append(job.upper, job.lower)
  let input_ids = set.from_list(list.map(inputs, fn(t) { t.id }))
  let missing =
    list.find(inputs, fn(t) {
      !list.any(tables, fn(current) { current == t })
    })
  use _ <- result.try(case missing {
    Ok(t) ->
      Error(CommitMismatch(
        "input table " <> int.to_string(t.id) <> " is missing or has changed",
      ))
    Error(_) -> Ok(Nil)
  })
  let remaining = list.filter(tables, fn(t) { !set.contains(input_ids, t.id) })
  use produced <- result.try(case job.kind {
    TrivialMove ->
      Ok(list.map(job.upper, fn(t) { Table(..t, level: job.to_level) }))
    Merge -> check_outputs(job, inputs, outputs)
  })
  let next = list.append(remaining, produced)
  use _ <- result.try(validate_tables(config, next))
  Ok(list.sort(next, compare_by_level_then_smallest))
}

fn check_outputs(
  job: Job,
  inputs: List(Table),
  outputs: List(Table),
) -> Result(List(Table), PlanError) {
  let #(lo, hi) = span_of(inputs)
  case outputs {
    [] ->
      case job.drops_tombstones {
        True -> Ok([])
        False ->
          Error(CommitMismatch(
            "merge produced no tables but the job does not drop tombstones",
          ))
      }
    _ ->
      case
        list.find(outputs, fn(t) {
          t.level != job.to_level
          || string.compare(t.smallest, lo) == Lt
          || string.compare(t.largest, hi) == Gt
        })
      {
        Ok(t) ->
          Error(CommitMismatch(
            "output table "
            <> int.to_string(t.id)
            <> " is at the wrong level or outside the input key span",
          ))
        Error(_) -> Ok(outputs)
      }
  }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

pub fn describe_job(job: Job) -> String {
  let ids = fn(ts: List(Table)) {
    ts |> list.map(fn(t) { int.to_string(t.id) }) |> string.join(",")
  }
  let reason = case job.reason {
    L0Overflow -> "l0 overflow"
    LevelOverflow -> "level overflow"
    TombstoneCleanup -> "tombstone cleanup"
  }
  let kind = case job.kind {
    Merge -> "merge"
    TrivialMove -> "trivial move"
  }
  reason
  <> ": "
  <> kind
  <> " L"
  <> int.to_string(job.from_level)
  <> " [" <> ids(job.upper) <> "] into L"
  <> int.to_string(job.to_level)
  <> " [" <> ids(job.lower) <> "], read "
  <> int.to_string(job.bytes_read)
  <> " write "
  <> int.to_string(job.bytes_written)
  <> " amp "
  <> int.to_string(job.write_amp_permille)
  <> "/1000"
  <> case job.drops_tombstones {
    True -> ", drops tombstones"
    False -> ""
  }
  <> case job.forced {
    True -> ", forced over budget"
    False -> ""
  }
}

pub fn describe_error(error: PlanError) -> String {
  case error {
    InvalidConfig(reason) -> "invalid config: " <> reason
    InvalidTable(id, reason) ->
      "invalid table " <> int.to_string(id) <> ": " <> reason
    DuplicateTableId(id) -> "duplicate table id " <> int.to_string(id)
    OverlapInLevel(level, a, b) ->
      "tables "
      <> int.to_string(a)
      <> " and "
      <> int.to_string(b)
      <> " overlap in level "
      <> int.to_string(level)
    UnknownBusyTable(id) ->
      "busy list names table " <> int.to_string(id) <> " which is not in the tree"
    CommitMismatch(reason) -> "commit rejected: " <> reason
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn at_level(tables: List(Table), level: Int) -> List(Table) {
  list.filter(tables, fn(t) { t.level == level })
}

fn sum_bytes(tables: List(Table)) -> Int {
  list.fold(tables, 0, fn(acc, t) { acc + t.bytes })
}

fn overlap_bytes(candidates: List(Table), t: Table) -> Int {
  candidates
  |> list.filter(fn(other) { overlaps(t.smallest, t.largest, other) })
  |> sum_bytes
}

fn overlaps(lo: String, hi: String, t: Table) -> Bool {
  string.compare(t.largest, lo) != Lt && string.compare(t.smallest, hi) != Gt
}

fn span_of(tables: List(Table)) -> #(String, String) {
  case tables {
    [] -> #("", "")
    [first, ..rest] ->
      list.fold(rest, #(first.smallest, first.largest), fn(acc, t) {
        #(min_key(acc.0, t.smallest), max_key(acc.1, t.largest))
      })
  }
}

fn min_key(a: String, b: String) -> String {
  case string.compare(a, b) {
    Gt -> b
    _ -> a
  }
}

fn max_key(a: String, b: String) -> String {
  case string.compare(a, b) {
    Lt -> b
    _ -> a
  }
}

fn compare_by_smallest(a: Table, b: Table) -> Order {
  case string.compare(a.smallest, b.smallest) {
    Eq -> int.compare(a.id, b.id)
    other -> other
  }
}

fn compare_by_age(a: Table, b: Table) -> Order {
  case int.compare(a.age, b.age) {
    Eq -> int.compare(a.id, b.id)
    other -> other
  }
}

fn compare_by_level_then_smallest(a: Table, b: Table) -> Order {
  case int.compare(a.level, b.level) {
    Eq -> compare_by_smallest(a, b)
    other -> other
  }
}

fn tombstone_ratio(t: Table) -> Int {
  t.tombstones * 1000 / int.max(t.entries, 1)
}

fn is_tombstone_heavy(config: Config, t: Table) -> Bool {
  t.tombstones > 0
  && t.entries > 0
  && t.tombstones * 1000 >= t.entries * config.tombstone_permille
}

/// Share of the table's bytes taken by tombstones, assuming entries are
/// roughly equal in size. An estimate, good enough to size an output.
fn tombstone_bytes(t: Table) -> Int {
  t.bytes * t.tombstones / int.max(t.entries, 1)
}

fn ceil_div(value: Int, divisor: Int) -> Int {
  case value <= 0 {
    True -> 0
    False -> {
      let padded = value + divisor - 1
      padded / divisor
    }
  }
}
