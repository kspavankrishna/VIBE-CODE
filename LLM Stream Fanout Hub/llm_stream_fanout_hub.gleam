import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// One unit of upstream progress. The hub assigns the sequence number, never
/// the producer, so replay and gap detection stay correct even if a producer
/// double-publishes or publishes out of order.
pub type Frame {
  Token(text: String)
  ToolCall(name: String, arguments: String)
  Completed
  Failed(reason: String)
  Cancelled
}

pub type Envelope {
  Envelope(sequence: Int, frame: Frame)
}

/// What a relay does to its own queue when a subscriber falls behind the
/// live stream faster than it can drain.
pub type OverflowPolicy {
  DropOldest
  Disconnect
}

pub type SubscribeOutcome {
  Subscribed(subscriber_id: Int, relay: Subject(RelayMessage))
  SubscribedWithGap(
    subscriber_id: Int,
    relay: Subject(RelayMessage),
    missing_from: Int,
    missing_through: Int,
  )
  SubscribeRejected(reason: String)
}

pub type PullOutcome {
  Delivered(envelopes: List(Envelope), dropped: Int)
  RelayClosed
}

pub type HubStats {
  HubStats(sequence: Int, buffered: Int, subscriber_count: Int, closed: Bool)
}

pub type RelayStats {
  RelayStats(
    subscriber_id: Int,
    queue_depth: Int,
    dropped: Int,
    policy: OverflowPolicy,
    capacity: Int,
  )
}

pub type HubConfig {
  HubConfig(
    buffer_capacity: Int,
    max_subscribers: Int,
    relay_queue_capacity: Int,
    pull_wait_ms: Int,
  )
}

/// A window of 256 replayable frames, a 1000 subscriber ceiling, a 64 frame
/// per subscriber queue and a 15 second long poll wait. Tune all four for
/// your own traffic before trusting the defaults in production.
pub fn default_config() -> HubConfig {
  HubConfig(
    buffer_capacity: 256,
    max_subscribers: 1000,
    relay_queue_capacity: 64,
    pull_wait_ms: 15_000,
  )
}

// ---------------------------------------------------------------------------
// Hub actor
// ---------------------------------------------------------------------------

pub opaque type HubMessage {
  Publish(frame: Frame)
  Subscribe(
    reply: Subject(SubscribeOutcome),
    resume_after: Option(Int),
    policy: OverflowPolicy,
  )
  Unsubscribe(subscriber_id: Int)
  RelayDown(subscriber_id: Int)
  GetStats(reply: Subject(HubStats))
}

type Subscriber {
  Subscriber(relay: Subject(RelayMessage), monitor: process.Monitor)
}

type HubState {
  HubState(
    config: HubConfig,
    next_sequence: Int,
    buffer: List(Envelope),
    closed: Bool,
    next_subscriber_id: Int,
    subscribers: Dict(Int, Subscriber),
    selector: process.Selector(HubMessage),
  )
}

/// Starts one hub actor. The returned subject is the only handle a producer
/// or subscriber ever needs; every other value in this module flows through
/// it.
pub fn start_hub(
  config: HubConfig,
) -> Result(actor.Started(Subject(HubMessage)), actor.StartError) {
  actor.new_with_initialiser(1000, init_hub(config))
  |> actor.on_message(handle_hub_message)
  |> actor.start
}

fn init_hub(
  config: HubConfig,
) -> fn(Subject(HubMessage)) ->
  Result(actor.Initialised(HubState, HubMessage, Subject(HubMessage)), String) {
  fn(self) {
    let selector = process.new_selector() |> process.select(self)
    let state =
      HubState(
        config: config,
        next_sequence: 1,
        buffer: [],
        closed: False,
        next_subscriber_id: 1,
        subscribers: dict.new(),
        selector: selector,
      )
    Ok(
      actor.initialised(state)
      |> actor.selecting(selector)
      |> actor.returning(self),
    )
  }
}

/// Publishes one frame. Fire and forget: the hub never blocks on a
/// subscriber, however slow or however many there are.
pub fn publish(hub: Subject(HubMessage), frame: Frame) -> Nil {
  actor.send(hub, Publish(frame))
}

pub fn token(hub: Subject(HubMessage), text: String) -> Nil {
  publish(hub, Token(text))
}

pub fn tool_call(
  hub: Subject(HubMessage),
  name: String,
  arguments: String,
) -> Nil {
  publish(hub, ToolCall(name, arguments))
}

/// Marks the run as finished. Publishing again after this is a no-op: a
/// closed hub stays alive and keeps answering subscribe and stats calls, it
/// simply stops accepting new frames.
pub fn complete(hub: Subject(HubMessage)) -> Nil {
  publish(hub, Completed)
}

pub fn fail(hub: Subject(HubMessage), reason: String) -> Nil {
  publish(hub, Failed(reason))
}

pub fn cancel(hub: Subject(HubMessage)) -> Nil {
  publish(hub, Cancelled)
}

/// Joins the broadcast. `resume_after` is the last sequence number the
/// caller already has, exactly like an SSE `Last-Event-ID`: pass `None` for
/// a brand new viewer that wants everything still buffered, or
/// `Some(last_seen)` when reconnecting. If the buffer has already evicted
/// part of that range the reply is `SubscribedWithGap`, still carrying a
/// live relay, so the caller can resync out of band and keep tailing rather
/// than lose the rest of the run.
pub fn subscribe(
  hub: Subject(HubMessage),
  resume_after: Option(Int),
  policy: OverflowPolicy,
  timeout: Int,
) -> SubscribeOutcome {
  actor.call(hub, waiting: timeout, sending: fn(reply) {
    Subscribe(reply, resume_after, policy)
  })
}

/// Leaves the broadcast. Safe to call more than once and safe to call after
/// the relay has already died on its own.
pub fn unsubscribe(hub: Subject(HubMessage), subscriber_id: Int) -> Nil {
  actor.send(hub, Unsubscribe(subscriber_id))
}

pub fn stats(hub: Subject(HubMessage), timeout: Int) -> HubStats {
  actor.call(hub, waiting: timeout, sending: GetStats)
}

fn handle_hub_message(
  state: HubState,
  message: HubMessage,
) -> actor.Next(HubState, HubMessage) {
  case message {
    Publish(frame) -> handle_publish(state, frame)
    Subscribe(reply, resume_after, policy) ->
      handle_subscribe(state, reply, resume_after, policy)
    Unsubscribe(subscriber_id) -> handle_unsubscribe(state, subscriber_id)
    RelayDown(subscriber_id) -> handle_unsubscribe(state, subscriber_id)
    GetStats(reply) -> {
      actor.send(
        reply,
        HubStats(
          sequence: state.next_sequence - 1,
          buffered: list.length(state.buffer),
          subscriber_count: dict.size(state.subscribers),
          closed: state.closed,
        ),
      )
      actor.continue(state)
    }
  }
}

fn handle_publish(
  state: HubState,
  frame: Frame,
) -> actor.Next(HubState, HubMessage) {
  case state.closed {
    True -> actor.continue(state)
    False -> {
      let sequence = state.next_sequence
      let envelope = Envelope(sequence, frame)
      dict.each(state.subscribers, fn(_id, sub) {
        actor.send(sub.relay, Enqueue(envelope))
      })
      let buffer =
        [envelope, ..state.buffer] |> list.take(state.config.buffer_capacity)
      actor.continue(
        HubState(
          ..state,
          buffer: buffer,
          next_sequence: sequence + 1,
          closed: is_terminal(frame),
        ),
      )
    }
  }
}

fn handle_subscribe(
  state: HubState,
  reply: Subject(SubscribeOutcome),
  resume_after: Option(Int),
  policy: OverflowPolicy,
) -> actor.Next(HubState, HubMessage) {
  case dict.size(state.subscribers) >= state.config.max_subscribers {
    True -> {
      actor.send(reply, SubscribeRejected("hub subscriber capacity reached"))
      actor.continue(state)
    }
    False -> {
      let id = state.next_subscriber_id
      let started =
        spawn_relay(
          id,
          state.config.relay_queue_capacity,
          policy,
          state.config.pull_wait_ms,
        )
      case started {
        Error(err) -> {
          actor.send(reply, SubscribeRejected(start_error_to_string(err)))
          actor.continue(state)
        }
        Ok(actor.Started(pid: pid, data: relay)) -> {
          let plan = compute_replay(state.buffer, resume_after)
          list.each(plan.envelopes, fn(e) { actor.send(relay, Enqueue(e)) })
          case plan.gap {
            Some(#(from, through)) ->
              actor.send(reply, SubscribedWithGap(id, relay, from, through))
            None -> actor.send(reply, Subscribed(id, relay))
          }
          let monitor = process.monitor(pid)
          let selector =
            process.select_specific_monitor(state.selector, monitor, fn(_down) {
              RelayDown(id)
            })
          let subscribers =
            dict.insert(state.subscribers, id, Subscriber(relay, monitor))
          actor.continue(
            HubState(
              ..state,
              subscribers: subscribers,
              next_subscriber_id: id + 1,
              selector: selector,
            ),
          )
          |> actor.with_selector(selector)
        }
      }
    }
  }
}

fn handle_unsubscribe(
  state: HubState,
  subscriber_id: Int,
) -> actor.Next(HubState, HubMessage) {
  case dict.get(state.subscribers, subscriber_id) {
    Error(Nil) -> actor.continue(state)
    Ok(sub) -> {
      process.demonitor_process(sub.monitor)
      actor.send(sub.relay, RelayStop)
      let selector =
        process.deselect_specific_monitor(state.selector, sub.monitor)
      let subscribers = dict.delete(state.subscribers, subscriber_id)
      actor.continue(
        HubState(..state, subscribers: subscribers, selector: selector),
      )
      |> actor.with_selector(selector)
    }
  }
}

fn is_terminal(frame: Frame) -> Bool {
  case frame {
    Completed -> True
    Failed(_) -> True
    Cancelled -> True
    Token(_) -> False
    ToolCall(_, _) -> False
  }
}

// ---------------------------------------------------------------------------
// Replay planning: pure, no actor state, easy to test on its own
// ---------------------------------------------------------------------------

type ReplayPlan {
  ReplayPlan(envelopes: List(Envelope), gap: Option(#(Int, Int)))
}

/// `buffer` is newest first, as the hub stores it. Returns the envelopes to
/// hand a new subscriber in chronological order, plus the inclusive
/// `#(missing_from, missing_through)` range if part of what they asked for
/// has already fallen out of the retained window.
fn compute_replay(
  buffer: List(Envelope),
  resume_after: Option(Int),
) -> ReplayPlan {
  let chronological = list.reverse(buffer)
  case resume_after {
    None -> ReplayPlan(chronological, None)
    Some(after) ->
      case chronological {
        [] -> ReplayPlan([], None)
        [first, ..] -> {
          let gap = case after + 1 < first.sequence {
            True -> Some(#(after + 1, first.sequence - 1))
            False -> None
          }
          let envelopes =
            list.filter(chronological, fn(e) { e.sequence > after })
          ReplayPlan(envelopes, gap)
        }
      }
  }
}

fn start_error_to_string(error: actor.StartError) -> String {
  case error {
    actor.InitTimeout -> "relay initialisation timed out"
    actor.InitFailed(reason) -> "relay initialisation failed: " <> reason
    actor.InitExited(_) -> "relay initialisation process exited abnormally"
  }
}

// ---------------------------------------------------------------------------
// Relay actor: one per subscriber, owns the only bounded buffer in the
// system. BEAM mailboxes are unbounded by default, so this is where a slow
// subscriber's backlog actually gets capped instead of quietly growing the
// hub's memory until the node falls over.
// ---------------------------------------------------------------------------

pub opaque type RelayMessage {
  Enqueue(envelope: Envelope)
  Pull(reply: Subject(PullOutcome))
  PullTimeout(token: Int)
  RelayStop
  Stats(reply: Subject(RelayStats))
}

type RelayState {
  RelayState(
    self: Subject(RelayMessage),
    subscriber_id: Int,
    queue: List(Envelope),
    capacity: Int,
    policy: OverflowPolicy,
    pending_pull: Option(Subject(PullOutcome)),
    pull_token: Int,
    pending_timer: Option(process.Timer),
    dropped: Int,
    pull_wait_ms: Int,
  )
}

fn spawn_relay(
  subscriber_id: Int,
  capacity: Int,
  policy: OverflowPolicy,
  pull_wait_ms: Int,
) -> Result(actor.Started(Subject(RelayMessage)), actor.StartError) {
  let capacity = case capacity <= 0 {
    True -> 1
    False -> capacity
  }
  actor.new_with_initialiser(
    1000,
    init_relay(subscriber_id, capacity, policy, pull_wait_ms),
  )
  |> actor.on_message(handle_relay_message)
  |> actor.start
}

fn init_relay(
  subscriber_id: Int,
  capacity: Int,
  policy: OverflowPolicy,
  pull_wait_ms: Int,
) -> fn(Subject(RelayMessage)) ->
  Result(
    actor.Initialised(RelayState, RelayMessage, Subject(RelayMessage)),
    String,
  ) {
  fn(self) {
    let selector = process.new_selector() |> process.select(self)
    let state =
      RelayState(
        self: self,
        subscriber_id: subscriber_id,
        queue: [],
        capacity: capacity,
        policy: policy,
        pending_pull: None,
        pull_token: 0,
        pending_timer: None,
        dropped: 0,
        pull_wait_ms: pull_wait_ms,
      )
    Ok(
      actor.initialised(state)
      |> actor.selecting(selector)
      |> actor.returning(self),
    )
  }
}

/// Asks a relay for its next batch. Blocks up to `timeout` milliseconds,
/// but the relay always answers within its own `pull_wait_ms` (an empty
/// `Delivered([], 0)` if nothing arrived), so keep `timeout` comfortably
/// above the hub's configured `pull_wait_ms` or every idle poll will crash
/// this calling process instead of returning a value.
pub fn pull(relay: Subject(RelayMessage), timeout: Int) -> PullOutcome {
  actor.call(relay, waiting: timeout, sending: Pull)
}

pub fn relay_stats(relay: Subject(RelayMessage), timeout: Int) -> RelayStats {
  actor.call(relay, waiting: timeout, sending: Stats)
}

fn handle_relay_message(
  state: RelayState,
  message: RelayMessage,
) -> actor.Next(RelayState, RelayMessage) {
  case message {
    Enqueue(envelope) -> handle_enqueue(state, envelope)
    Pull(reply) -> handle_pull(state, reply)
    PullTimeout(token) -> handle_pull_timeout(state, token)
    RelayStop -> {
      notify_pull_closed(state)
      cancel_pending_timer(state)
      actor.stop()
    }
    Stats(reply) -> {
      actor.send(
        reply,
        RelayStats(
          subscriber_id: state.subscriber_id,
          queue_depth: list.length(state.queue),
          dropped: state.dropped,
          policy: state.policy,
          capacity: state.capacity,
        ),
      )
      actor.continue(state)
    }
  }
}

fn handle_enqueue(
  state: RelayState,
  envelope: Envelope,
) -> actor.Next(RelayState, RelayMessage) {
  case accept(state, envelope) {
    Error(Nil) -> {
      notify_pull_closed(state)
      cancel_pending_timer(state)
      actor.stop()
    }
    Ok(updated) ->
      case updated.pending_pull {
        Some(reply) -> {
          cancel_pending_timer(updated)
          actor.send(reply, Delivered(updated.queue, updated.dropped))
          actor.continue(
            RelayState(
              ..updated,
              queue: [],
              dropped: 0,
              pending_pull: None,
              pending_timer: None,
            ),
          )
        }
        None -> actor.continue(updated)
      }
  }
}

/// `Error(Nil)` means the queue is full under a `Disconnect` policy: the
/// caller must stop the relay rather than deliver this envelope.
fn accept(state: RelayState, envelope: Envelope) -> Result(RelayState, Nil) {
  case list.length(state.queue) >= state.capacity {
    False ->
      Ok(RelayState(..state, queue: list.append(state.queue, [envelope])))
    True ->
      case state.policy {
        DropOldest -> {
          let trimmed = case state.queue {
            [] -> []
            [_, ..rest] -> rest
          }
          Ok(
            RelayState(
              ..state,
              queue: list.append(trimmed, [envelope]),
              dropped: state.dropped + 1,
            ),
          )
        }
        Disconnect -> Error(Nil)
      }
  }
}

fn handle_pull(
  state: RelayState,
  reply: Subject(PullOutcome),
) -> actor.Next(RelayState, RelayMessage) {
  case state.queue {
    [] -> {
      case state.pending_pull {
        Some(stale) -> actor.send(stale, Delivered([], 0))
        None -> Nil
      }
      cancel_pending_timer(state)
      let token = state.pull_token + 1
      let timer =
        process.send_after(state.self, state.pull_wait_ms, PullTimeout(token))
      actor.continue(
        RelayState(
          ..state,
          pending_pull: Some(reply),
          pull_token: token,
          pending_timer: Some(timer),
        ),
      )
    }
    queued -> {
      actor.send(reply, Delivered(queued, state.dropped))
      actor.continue(RelayState(..state, queue: [], dropped: 0))
    }
  }
}

fn handle_pull_timeout(
  state: RelayState,
  token: Int,
) -> actor.Next(RelayState, RelayMessage) {
  case token == state.pull_token, state.pending_pull {
    True, Some(reply) -> {
      actor.send(reply, Delivered([], 0))
      actor.continue(
        RelayState(..state, pending_pull: None, pending_timer: None),
      )
    }
    _, _ -> actor.continue(state)
  }
}

fn cancel_pending_timer(state: RelayState) -> Nil {
  case state.pending_timer {
    Some(timer) -> {
      let _ = process.cancel_timer(timer)
      Nil
    }
    None -> Nil
  }
}

fn notify_pull_closed(state: RelayState) -> Nil {
  case state.pending_pull {
    Some(reply) -> actor.send(reply, RelayClosed)
    None -> Nil
  }
}
