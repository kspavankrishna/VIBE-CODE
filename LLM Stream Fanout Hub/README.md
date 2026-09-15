# LLM Stream Fanout Hub

One LLM generation runs once and many viewers watch it live, each on its own bounded queue, so a slow viewer never stalls the model and never grows the process that owns it without limit.

**Language:** Gleam | **Lines:** 630 | **Added:** 2026-09-15

## What this solves

Say three people open the same agent run in three browser tabs, or a run needs to go to a live dashboard, a billing sink and an audit log at the same time. The obvious move is to call the model once and hand the same stream of tokens to everyone who is watching. The part that is not obvious is what happens when one of those watchers is slow. A dashboard tab in a background window, a websocket over a bad phone connection, a logging sink stuck on a slow disk: any of them can fall behind the token stream.

On the BEAM, the naive way to fan a stream out is to give every subscriber the raw Erlang process that owns the stream and let them all receive from it directly. That works until one subscriber stalls, because a BEAM process mailbox has no size limit by default. Tokens keep arriving, the slow subscriber's mailbox keeps growing, and the fix people reach for after their node runs out of memory once is usually a global rate limit that punishes everyone instead of just the one struggling watcher. This module is the fix I wanted before hitting that: one hub that owns the sequence of events, and one small relay actor per subscriber that owns a queue with a real, fixed size and a real, chosen policy for what happens when that queue is full.

It also solves the smaller but just as common problem of the subscriber who joins late. A viewer who opens the dashboard three seconds into a run should not just start seeing tokens from the moment they connected. They should get the tokens they missed, up to however much history the hub still has, and if part of what they missed has already been thrown away they should be told exactly which range is gone instead of a hub that silently pretends nothing happened before they showed up.

## Why I built it

I wanted a real exercise in Gleam's actor model past the usual toy counter or echo server, something with actual failure modes worth designing for. Fanout is a good fit for OTP specifically because the interesting bugs in a naive fanout implementation are exactly the ones the actor model is built to avoid: shared mutable subscriber lists edited from two places at once, a slow consumer starving a fast one because they share a data structure, a crashed subscriber leaking its slot forever because nothing was watching it die.

Gleam's actor library (`gleam_otp`) forces you to be explicit about all three. State lives inside one actor and only that actor's message loop touches it. A subscriber that disappears without saying goodbye still gets cleaned up, because the hub monitors the process behind every subscription and reacts to the monitor's exit message the same way it reacts to a polite unsubscribe. And the classic unbounded mailbox problem, which BEAM newcomers usually only learn about the hard way, gets solved once here instead of separately in every project that needs a fanout.

## When to use it

Reach for this when one upstream producer, most likely an LLM completion or an agent run, needs to reach more than one downstream reader and you cannot predict how fast each reader will keep up. Good fits: a shared "watch this run" dashboard with several tabs open on the same session, a single generation that needs to reach a UI, a billing meter and an audit log at once, or an SSE endpoint that needs to support a client reconnecting mid stream with a `Last-Event-ID` style resume token.

Skip it when you have exactly one consumer for one producer. A single `Subject` and a direct `process.send` do that job with less code and one less process to supervise. Also think twice before setting the replay buffer to anything huge: the buffer is a plain list trimmed with `list.take` on every publish, which is O(buffer_capacity) per publish. A window of a few hundred or even a few thousand frames costs nothing you would notice. A window in the millions calls for an actual ring buffer, which this module does not try to be.

## How it works

`start_hub` takes a `HubConfig` (four numbers: `buffer_capacity`, `max_subscribers`, `relay_queue_capacity`, `pull_wait_ms`, with sane values from `default_config`) and starts one hub actor. The producer side is `publish`, plus the thin wrappers `token`, `tool_call`, `complete`, `fail` and `cancel` that build the right `Frame` for you. The hub, not the producer, assigns the sequence number on every frame it accepts, wrapping it into an `Envelope`. That single decision removes a whole family of bugs where a producer publishes out of order or twice and corrupts the sequence: the hub is the only writer of `next_sequence`, so it cannot happen.

Every accepted envelope goes into a bounded, newest-first buffer (trimmed to `buffer_capacity`) and gets pushed out to every live subscriber's relay with a plain, non-blocking `actor.send`. The hub never waits on a subscriber. Once a frame satisfies `is_terminal` (`Completed`, `Failed` or `Cancelled`), the hub marks itself closed: further `publish` calls are quietly ignored, but the hub keeps running and keeps answering `subscribe` and `stats`, because a run finishing is not a reason to stop replaying it to whoever asks next.

`subscribe` is where the interesting decision making happens, in the pure helper `compute_replay`, which takes the current buffer and a `resume_after` sequence number and returns a `ReplayPlan`: the envelopes to hand the new subscriber, plus `Some(#(missing_from, missing_through))` if part of what they asked for has already fallen out of the retained window. Passing `resume_after: None` means "I am new, give me whatever you still have." Passing `Some(last_seen)` is the `Last-Event-ID` case: reconnecting after a drop. If nothing is missing, `subscribe` replies `Subscribed`. If something fell out of the window, it replies `SubscribedWithGap` with the exact missing range, but still hands back a live subscription and replays everything still available, because losing history is never a reason to also lose the future.

Each subscriber gets its own relay actor, spawned by `spawn_relay` with a capacity and an `OverflowPolicy`. The policy only matters in `accept`, the pure function that decides what happens when a relay's queue is already at capacity when a new envelope arrives: `DropOldest` throws away the single oldest queued envelope and counts it in `dropped`, `Disconnect` refuses the envelope entirely and tells the caller to tear the relay down. A crashed or deliberately stopped relay is caught by a per-subscriber monitor set up with `process.select_specific_monitor` right when the subscription is created, so the hub's own selector always includes a live entry for every subscriber and the dead ones get removed by `handle_unsubscribe` whether they left politely or just disappeared.

Reading a relay is `pull`, a long poll: if the relay's queue already has something, it replies immediately with `Delivered`. If it is empty, the relay parks the caller's reply subject and answers with whatever shows up next, or with an empty `Delivered([], 0)` once its own `pull_wait_ms` elapses, whichever comes first. That internal timeout, scheduled with `process.send_after` and guarded by a `pull_token` so a stale timer can never answer a newer call, exists for one reason: `actor.call` crashes the caller if nothing replies before the caller's own timeout, and a naive long poll that just waits forever for data would hit that crash on every quiet stream. As long as you give `pull` a timeout comfortably longer than the hub's `pull_wait_ms`, you always get an answer back, never a crash from waiting.

## Usage

```gleam
import gleam/option.{None}
import llm_stream_fanout_hub as hub

pub fn run_and_watch() {
  let assert Ok(started) = hub.start_hub(hub.default_config())
  let h = started.data

  // one subscriber, tailing from the start of whatever is still buffered
  let assert hub.Subscribed(subscriber_id, relay) =
    hub.subscribe(h, None, hub.DropOldest, 1000)

  // the producer side, wherever your model call actually lives
  hub.token(h, "Once upon a time")
  hub.token(h, " there was a hub.")
  hub.complete(h)

  // the consumer side, one long poll per read
  case hub.pull(relay, 20_000) {
    hub.Delivered(envelopes, dropped) -> handle(envelopes, dropped)
    hub.RelayClosed -> Nil
  }

  hub.unsubscribe(h, subscriber_id)
}
```

## Notes

Drop this file into a project with `gleam_stdlib`, `gleam_erlang` and `gleam_otp` in `gleam.toml` and it compiles and type checks as is. I built it against gleam_otp 1.3.0 and gleam_erlang 1.3.0 on gleam_stdlib 1.0.5, running on Erlang/OTP 27, and ran a set of scripted scenarios against it (basic delivery, a late joiner replaying a finished run, both overflow policies, and gap detection on a deliberately tiny buffer) before trusting any of the numbers above.

A `pull` timeout must stay comfortably above the hub's `pull_wait_ms`, or a quiet stream will crash your reading process instead of returning an empty result, since that crash is `actor.call`'s own behavior on any unanswered call, not something this module adds. `HubStats` only reports what the hub itself knows (sequence, buffered count, subscriber count, closed) rather than querying every relay's queue depth on every stats call, on purpose: a central actor that blocks on N followers to answer one status question is a scaling trap, so per-subscriber detail lives in `relay_stats`, asked of one relay at a time by whoever owns it. `relay_queue_capacity` below one is clamped up to one rather than rejected, since a hub or relay that refuses to start over a config typo is worse than one that quietly does the smallest sane thing.
