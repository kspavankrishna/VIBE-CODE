import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/order.{type Order, Eq, Gt, Lt}
import gleam/result
import gleam/string

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

/// Largest wall clock value that fits the 12 hex digit wire format (2^48 - 1
/// milliseconds, which is the year 10889).
pub const max_wall_ms = 281_474_976_710_655

/// Largest logical counter that fits the 4 hex digit wire format.
pub const max_counter_limit = 65_535

/// Longest node id accepted. Node ids are the final tie breaker, so they must
/// be unique per writer and stable across restarts.
pub const max_node_length = 64

const node_alphabet = "abcdefghijklmnopqrstuvwxyz0123456789_.:"

const hex_alphabet = "0123456789abcdef"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A hybrid logical timestamp. Ordering is wall_ms, then counter, then node.
/// Two different writers can never produce equal timestamps as long as node
/// ids are unique, which is what makes last writer wins deterministic.
pub type Timestamp {
  Timestamp(wall_ms: Int, counter: Int, node: String)
}

pub type Config {
  Config(
    /// How far the clock may run ahead of the local physical clock, both when
    /// accepting a remote timestamp and when issuing one.
    max_drift_ms: Int,
    /// Counter ceiling before the clock borrows the next millisecond.
    max_counter: Int,
  )
}

/// Pure clock state. Every operation returns a new Clock and leaves the old
/// one valid, so a rejected timestamp never corrupts the clock you hold.
pub type Clock {
  Clock(node: String, last: Timestamp, config: Config)
}

pub type ClockError {
  InvalidConfig(reason: String)
  InvalidNode(node: String)
  PhysicalClockOutOfRange(physical_ms: Int)
  /// A remote timestamp is further ahead of our physical clock than allowed.
  /// Accepting it would drag every later local timestamp into the future.
  RemoteAhead(remote_ms: Int, physical_ms: Int, allowed_ms: Int)
  /// Issuing a timestamp would put us further ahead of the physical clock than
  /// allowed. Happens after a backwards clock step or a long same millisecond
  /// burst. Wait retry_after_ms and try again, state is unchanged.
  DriftExceeded(
    wall_ms: Int,
    physical_ms: Int,
    allowed_ms: Int,
    retry_after_ms: Int,
  )
  /// Remote timestamp carries our own node id. Two writers share an id.
  DuplicateNode(node: String)
  WallClockExhausted(wall_ms: Int)
  MalformedTimestamp(input: String)
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

pub fn default_config() -> Config {
  Config(max_drift_ms: 60_000, max_counter: max_counter_limit)
}

/// Fresh clock for a node that has never issued a timestamp.
pub fn new(
  node: String,
  physical_ms: Int,
  config: Config,
) -> Result(Clock, ClockError) {
  resume(
    node,
    Timestamp(wall_ms: 0, counter: 0, node: node),
    physical_ms,
    config,
  )
}

/// Rebuild a clock after a restart from the last persisted timestamp. Without
/// this a restarted node can re-issue values it already handed out whenever
/// the physical clock stepped backwards while it was down. The persisted node
/// id is ignored, the clock always uses the node you pass in.
pub fn resume(
  node: String,
  persisted: Timestamp,
  physical_ms: Int,
  config: Config,
) -> Result(Clock, ClockError) {
  use _ <- result.try(validate_config(config))
  use _ <- result.try(validate_node(node))
  use _ <- result.try(check_physical(physical_ms))
  use persisted <- result.try(validate(persisted))
  let last =
    Timestamp(
      wall_ms: persisted.wall_ms,
      counter: persisted.counter,
      node: node,
    )
  case persisted.wall_ms - physical_ms > config.max_drift_ms {
    True ->
      Error(DriftExceeded(
        wall_ms: persisted.wall_ms,
        physical_ms: physical_ms,
        allowed_ms: config.max_drift_ms,
        retry_after_ms: persisted.wall_ms - physical_ms - config.max_drift_ms,
      ))
    False -> Ok(Clock(node: node, last: last, config: config))
  }
}

// ---------------------------------------------------------------------------
// Issuing and observing
// ---------------------------------------------------------------------------

/// Stamp a local event or an outgoing message.
pub fn tick(
  clock: Clock,
  physical_ms: Int,
) -> Result(#(Clock, Timestamp), ClockError) {
  use _ <- result.try(check_physical(physical_ms))
  let last = clock.last
  let #(wall, counter) = case physical_ms > last.wall_ms {
    True -> #(physical_ms, 0)
    False -> #(last.wall_ms, last.counter + 1)
  }
  advance(clock, physical_ms, wall, counter)
}

/// Fold in a timestamp received from another node and stamp the receive
/// event. The result is strictly greater than both the remote timestamp and
/// everything this clock has issued before.
pub fn observe(
  clock: Clock,
  physical_ms: Int,
  remote: Timestamp,
) -> Result(#(Clock, Timestamp), ClockError) {
  use _ <- result.try(check_physical(physical_ms))
  use remote <- result.try(validate(remote))
  use _ <- result.try(case remote.node == clock.node {
    True -> Error(DuplicateNode(clock.node))
    False -> Ok(Nil)
  })
  case remote.wall_ms - physical_ms > clock.config.max_drift_ms {
    True ->
      Error(RemoteAhead(
        remote_ms: remote.wall_ms,
        physical_ms: physical_ms,
        allowed_ms: clock.config.max_drift_ms,
      ))
    False -> {
      let last = clock.last
      let wall = int.max(physical_ms, int.max(last.wall_ms, remote.wall_ms))
      let counter = case wall == last.wall_ms, wall == remote.wall_ms {
        True, True -> int.max(last.counter, remote.counter) + 1
        True, False -> last.counter + 1
        False, True -> remote.counter + 1
        False, False -> 0
      }
      advance(clock, physical_ms, wall, counter)
    }
  }
}

/// Last timestamp this clock issued. Persist it (see resume).
pub fn last_issued(clock: Clock) -> Timestamp {
  clock.last
}

fn advance(
  clock: Clock,
  physical_ms: Int,
  wall: Int,
  counter: Int,
) -> Result(#(Clock, Timestamp), ClockError) {
  // Counter overflow borrows the next millisecond instead of failing. The
  // result is still strictly increasing, and the drift check below bounds how
  // far repeated borrowing can run ahead of real time.
  let #(wall, counter) = case counter > clock.config.max_counter {
    True -> #(wall + 1, 0)
    False -> #(wall, counter)
  }
  let allowed = clock.config.max_drift_ms
  case wall > max_wall_ms, wall - physical_ms > allowed {
    True, _ -> Error(WallClockExhausted(wall))
    _, True ->
      Error(DriftExceeded(
        wall_ms: wall,
        physical_ms: physical_ms,
        allowed_ms: allowed,
        retry_after_ms: wall - physical_ms - allowed,
      ))
    False, False -> {
      let stamp = Timestamp(wall_ms: wall, counter: counter, node: clock.node)
      Ok(#(Clock(..clock, last: stamp), stamp))
    }
  }
}

// ---------------------------------------------------------------------------
// Ordering and wire format
// ---------------------------------------------------------------------------

pub fn compare(a: Timestamp, b: Timestamp) -> Order {
  case int.compare(a.wall_ms, b.wall_ms) {
    Eq ->
      case int.compare(a.counter, b.counter) {
        Eq -> string.compare(a.node, b.node)
        other -> other
      }
    other -> other
  }
}

/// Encode as "wwwwwwwwwwww-cccc-node": 12 lowercase hex digits of wall time,
/// 4 of counter, then the node id. Plain string sort equals timestamp sort,
/// so the value works as a database key or an object store prefix as is.
pub fn encode(ts: Timestamp) -> String {
  pad_hex(ts.wall_ms, 12) <> "-" <> pad_hex(ts.counter, 4) <> "-" <> ts.node
}

pub fn decode(text: String) -> Result(Timestamp, ClockError) {
  case string.split(text, "-") {
    [wall_text, counter_text, node] -> {
      use wall <- result.try(parse_hex(wall_text, 12, text))
      use counter <- result.try(parse_hex(counter_text, 4, text))
      validate(Timestamp(wall_ms: wall, counter: counter, node: node))
    }
    _ -> Error(MalformedTimestamp(text))
  }
}

/// Check a timestamp is inside the ranges the wire format can carry.
pub fn validate(ts: Timestamp) -> Result(Timestamp, ClockError) {
  use _ <- result.try(validate_node(ts.node))
  case
    ts.wall_ms < 0
    || ts.wall_ms > max_wall_ms
    || ts.counter < 0
    || ts.counter > max_counter_limit
  {
    True -> Error(MalformedTimestamp(encode_unchecked(ts)))
    False -> Ok(ts)
  }
}

pub fn describe_error(error: ClockError) -> String {
  case error {
    InvalidConfig(reason) -> "invalid clock config: " <> reason
    InvalidNode(node) -> "invalid node id: '" <> node <> "'"
    PhysicalClockOutOfRange(ms) ->
      "physical clock out of range: " <> int.to_string(ms)
    RemoteAhead(remote, physical, allowed) ->
      "remote timestamp is "
      <> int.to_string(remote - physical)
      <> " ms ahead of local time, limit is "
      <> int.to_string(allowed)
      <> " ms"
    DriftExceeded(_, _, allowed, retry) ->
      "clock is more than "
      <> int.to_string(allowed)
      <> " ms ahead of physical time, retry in "
      <> int.to_string(retry)
      <> " ms"
    DuplicateNode(node) ->
      "remote timestamp uses our own node id '" <> node <> "'"
    WallClockExhausted(ms) -> "wall clock exhausted at " <> int.to_string(ms)
    MalformedTimestamp(input) -> "malformed timestamp: " <> input
  }
}

fn encode_unchecked(ts: Timestamp) -> String {
  int.to_string(ts.wall_ms)
  <> "-"
  <> int.to_string(ts.counter)
  <> "-"
  <> ts.node
}

fn pad_hex(n: Int, width: Int) -> String {
  let digits = string.lowercase(int.to_base16(n))
  string.repeat("0", int.max(0, width - string.length(digits))) <> digits
}

fn parse_hex(part: String, width: Int, whole: String) -> Result(Int, ClockError) {
  let graphemes = string.to_graphemes(part)
  let well_formed =
    list.length(graphemes) == width
    && list.all(graphemes, fn(g) { string.contains(hex_alphabet, g) })
  case well_formed {
    False -> Error(MalformedTimestamp(whole))
    True ->
      int.base_parse(part, 16)
      |> result.replace_error(MalformedTimestamp(whole))
  }
}

fn validate_node(node: String) -> Result(Nil, ClockError) {
  let graphemes = string.to_graphemes(node)
  let ok =
    graphemes != []
    && list.length(graphemes) <= max_node_length
    && list.all(graphemes, fn(g) { string.contains(node_alphabet, g) })
  case ok {
    True -> Ok(Nil)
    False -> Error(InvalidNode(node))
  }
}

fn validate_config(config: Config) -> Result(Nil, ClockError) {
  case config.max_drift_ms < 0, config.max_counter < 0 {
    True, _ -> Error(InvalidConfig("max_drift_ms must not be negative"))
    _, True -> Error(InvalidConfig("max_counter must not be negative"))
    _, _ ->
      case config.max_counter > max_counter_limit {
        True ->
          Error(InvalidConfig(
            "max_counter must be at most " <> int.to_string(max_counter_limit),
          ))
        False -> Ok(Nil)
      }
  }
}

fn check_physical(physical_ms: Int) -> Result(Nil, ClockError) {
  case physical_ms < 0 || physical_ms > max_wall_ms {
    True -> Error(PhysicalClockOutOfRange(physical_ms))
    False -> Ok(Nil)
  }
}

// ---------------------------------------------------------------------------
// Last writer wins map
// ---------------------------------------------------------------------------

pub type Cell(a) {
  Live(value: a)
  Tombstone
}

pub type Register(a) {
  Register(cell: Cell(a), stamp: Timestamp)
}

/// A state based LWW map. merge is commutative, associative and idempotent
/// provided every stamp came from a Clock with a unique node id, so replicas
/// converge regardless of delivery order or duplication.
pub type LwwMap(a) {
  LwwMap(entries: Dict(String, Register(a)))
}

pub fn lww_new() -> LwwMap(a) {
  LwwMap(entries: dict.new())
}

pub fn lww_set(
  map: LwwMap(a),
  key: String,
  value: a,
  stamp: Timestamp,
) -> LwwMap(a) {
  lww_apply(map, key, Register(cell: Live(value), stamp: stamp))
}

/// Deleting keeps a tombstone so a delayed older write cannot resurrect the
/// key on another replica.
pub fn lww_delete(map: LwwMap(a), key: String, stamp: Timestamp) -> LwwMap(a) {
  lww_apply(map, key, Register(cell: Tombstone, stamp: stamp))
}

/// Apply one register. The higher stamp wins, an equal stamp keeps what is
/// already stored.
pub fn lww_apply(
  map: LwwMap(a),
  key: String,
  incoming: Register(a),
) -> LwwMap(a) {
  let wins = case dict.get(map.entries, key) {
    Error(Nil) -> True
    Ok(existing) -> compare(incoming.stamp, existing.stamp) == Gt
  }
  case wins {
    True -> LwwMap(entries: dict.insert(map.entries, key, incoming))
    False -> map
  }
}

pub fn lww_merge(a: LwwMap(v), b: LwwMap(v)) -> LwwMap(v) {
  dict.fold(b.entries, a, fn(acc, key, register) {
    lww_apply(acc, key, register)
  })
}

pub fn lww_get(map: LwwMap(a), key: String) -> Result(a, Nil) {
  case dict.get(map.entries, key) {
    Ok(Register(cell: Live(value), stamp: _)) -> Ok(value)
    _ -> Error(Nil)
  }
}

/// Live entries sorted by key, so output is identical on every replica.
pub fn lww_live_entries(map: LwwMap(a)) -> List(#(String, a)) {
  dict.to_list(map.entries)
  |> list.filter_map(fn(pair) {
    case pair {
      #(key, Register(cell: Live(value), stamp: _)) -> Ok(#(key, value))
      _ -> Error(Nil)
    }
  })
  |> list.sort(fn(x, y) { string.compare(x.0, y.0) })
}

/// Registers stamped strictly after the given timestamp, oldest first. Send
/// this as an anti entropy delta instead of the whole map.
pub fn lww_delta_since(
  map: LwwMap(a),
  after: Timestamp,
) -> List(#(String, Register(a))) {
  dict.to_list(map.entries)
  |> list.filter(fn(pair) { compare({ pair.1 }.stamp, after) == Gt })
  |> list.sort(fn(x, y) {
    case compare({ x.1 }.stamp, { y.1 }.stamp) {
      Eq -> string.compare(x.0, y.0)
      other -> other
    }
  })
}

pub fn lww_apply_delta(
  map: LwwMap(a),
  delta: List(#(String, Register(a))),
) -> LwwMap(a) {
  list.fold(delta, map, fn(acc, pair) { lww_apply(acc, pair.0, pair.1) })
}

/// Highest stamp stored, the cursor to ask peers for the next delta.
pub fn lww_high_water(map: LwwMap(a)) -> Result(Timestamp, Nil) {
  dict.values(map.entries)
  |> list.map(fn(register) { register.stamp })
  |> list.reduce(fn(best, next) {
    case compare(next, best) {
      Gt -> next
      Lt | Eq -> best
    }
  })
}

/// Drop tombstones whose wall time is older than the horizon. Only safe once
/// every replica has acknowledged state past the horizon. A replica that was
/// offline longer than that and still holds the old live value will bring the
/// key back from the dead.
pub fn lww_prune_tombstones(map: LwwMap(a), horizon_ms: Int) -> LwwMap(a) {
  LwwMap(
    entries: dict.filter(map.entries, fn(_key, register) {
      case register.cell {
        Tombstone -> register.stamp.wall_ms >= horizon_ms
        Live(_) -> True
      }
    }),
  )
}
