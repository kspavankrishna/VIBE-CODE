# LLM Chat Turn Machine

A chat window that streams from an LLM has more ways to break than the demos admit. This is a single file Elm state machine that owns one turn from the moment the user presses send until the answer is really finished, and it refuses to call a truncated stream a success.

**Language:** Elm | **Lines:** 1103 | **Added:** 2026-09-30

## What this solves

Most streaming chat UIs are written as a pile of callbacks around an EventSource or a fetch reader. They work on the happy path and then fall apart in the same five ways.

First, the connection closes without a terminal event. A proxy idles out, a laptop changes network, a load balancer recycles a pod. The UI sees the stream end and marks the message as done. The user now reads half an answer, or worse a tool call with half a JSON argument string, and nothing tells them it is incomplete.

Second, a reconnect replays events. Resumable streams send everything after a last seen sequence number, and many servers overlap a little to be safe. If the client appends every delta it receives you get doubled words in the middle of a sentence.

Third, old streams talk back. The user cancels, edits the prompt and sends again. The first connection is still draining and its events land in the new message. This is the classic stale stream bug and it shows up once a week in production and never on a developer machine.

Fourth, events arrive out of order or go missing. Fan out layers, edge workers and retry queues do not guarantee order. One dropped event in the middle of a tool call means the arguments are not valid JSON and the tool runs with garbage or not at all.

Fifth, retries with no discipline. A 429 turns into a tight loop, or a flaky network turns into forty reconnects that all cost money because the provider bills the input tokens each time.

LlmChatTurnMachine handles all five in one pure module. It does no IO. You give it messages, it gives you a new state and a list of effects to run.

## Why I built it

I kept rewriting the same reducer in different frontends and each rewrite had a different bug. One version appended duplicates after resume. Another one let a cancelled turn write into the next one. The fix was always the same: stop treating the stream as a pipe of text and treat it as a protocol with a sequence number, a turn id, a connection attempt id and an explicit end marker.

Elm is a good fit for this because the whole thing is a state machine with no hidden mutable state. Every rule lives in a function that takes a message and a model. You can replay a recorded session through `update` and get the same answer every time, including the retry jitter, because the random number generator is seeded from `Config.seed`. That makes the odd production incidents reproducible from a log.

It also has no dependencies beyond `elm/core` and `elm/json`. There is no Http, no ports and no Browser import inside the module. Your app decides how to open the stream.

## When to use it

Use it when you are building a chat or agent UI in Elm that talks to an SSE or fetch stream and you care about these things: resuming with a last event id, showing tool calls that have to run on the client, cancelling and regenerating without ghost text, and giving the user an honest failure instead of a half answer.

It also works as a reference if you are writing the same logic in another language. The rules are the valuable part and they translate directly.

Do not use it if your backend gives you no sequence numbers at all. The duplicate and gap handling depends on `seq` being present and starting at 1 for every turn. You can add it in a thin gateway, but without it you only get the stall and close handling.

## How it works

The public type is `Machine`, created with `init` from a `Config`. `defaultConfig` holds sane numbers: a 15 second `connectTimeoutMs`, a 30 second `stallTimeoutMs`, five `maxRetries` without progress and twenty `maxTotalReconnects` per turn. You drive it with `update : Msg -> Machine -> ( Machine, List Effect )`.

**Turns and attempts.** Every `Submit` starts a turn with a fresh `TurnId`. Every connection opened for that turn gets an attempt number. Messages from the host carry the attempt they belong to, and `withAttempt` drops anything that does not match the live attempt. Dropped messages increment `Stats.stale`. This is what kills the cancelled stream bug. An event also has to carry the current turn id inside its `Envelope` or it is counted as stale too.

**Sequence handling.** In `receive`, an event at or below `lastSeq` is a duplicate and is counted in `Stats.duplicates`. An event exactly one above `lastSeq` goes into the `pending` dictionary and `advance` applies every consecutive event it can. An event further ahead is buffered if it is inside `maxReorderWindow` and `pending` has room under `maxPending`, and counted in `Stats.reordered`. If it is further than the window, the machine gives up on this connection and reconnects with `resumeAfter = lastSeq`. A gap that never fills is caught by `gapTimeoutMs` on `Tick`, and the same reconnect path runs.

**Terminal events.** `ConnectionEnded` always goes through `retry` with `PrematureClose`. Only a `Done` event inside the sequence produces `Finished`. That one rule removes the silent truncation problem.

**Tool calls.** `ToolStart`, `ToolArgs` and `ToolEnd` build a `ToolCall` block. Arguments are accumulated as a string and `validateArgs` checks at `ToolEnd` that they parse as a JSON object, with empty arguments treated as an empty object. The result is `Ready` or `InvalidArgs` with the decoder message. If the turn ends while a call is still `Building`, `sealBlock` marks it `Truncated`. Protocol mistakes such as arguments for an unknown call id, a duplicate call id or a call that ends twice abort the turn with `ProtocolViolation`. `readyToolCalls` returns only the safe ones for your dispatcher.

**Retries.** `retry` counts failures in a row and resets the count whenever `advance` applies a real event, so a long stream that reconnects once an hour never runs out of budget. The delay is exponential from `baseBackoffMs` up to `maxBackoffMs` with equal jitter drawn from `nextRandom`, a Park Miller generator. A `Retry-After` value from the host raises the delay but is clamped to `maxRetryAfterMs`. `retryableStatus` treats 408, 425, 429 and every 5xx as retryable and everything else as fatal. When the budget is gone the turn ends with `RetriesExhausted` wrapping the last cause, so the UI can say what actually happened.

**Time.** The machine has no clock. The host sends `Tick` with elapsed milliseconds while `wantsTicks` is true. `tick` handles connect timeouts, stalls, gap timeouts, backoff countdown and the overall `maxTurnMs` deadline.

**Limits.** `appendText` enforces `maxTextChars` and tool arguments are capped by `maxToolArgsChars`. Overflow ends the turn with `TooLarge`. `Malformed` messages are counted and past `maxMalformed` the turn fails with `TooManyMalformed`. A malformed frame is never guessed at, it simply becomes a gap that the resume logic repairs.

**Submit rules.** `Submit` rejects empty and oversized prompts and, unless `replaceWhenBusy` is set, rejects a second prompt while a turn runs. With `replaceWhenBusy` the old turn ends as `Cancelled` first. `Regenerate` marks the last finished turn `Superseded` and runs its prompt again. Rejections come back as `SubmitRejected` effects.

**Decoding.** `decodeEnvelope` and `envelopeDecoder` parse one JSON frame with `turn`, `seq` and a `type` of text, reasoning, tool_start, tool_args, tool_end, usage, done or error. An unknown type becomes `Unknown` so a newer server does not break an older client, and it still consumes its sequence number.

**Reading state.** `phase`, `isBusy`, `activeTurn`, `transcript`, `blocks` and `plainText` give your view what it needs. `Turn.stats` exposes the counters so you can send them to your telemetry.

## Usage

Copy `LlmChatTurnMachine.elm` and `elm.json` into your project. Keep the machine in your model and interpret the effects.

```elm
import LlmChatTurnMachine as Turn

type alias Model =
    { chat : Turn.Machine }

init : Model
init =
    { chat = Turn.init Turn.defaultConfig }

step : Turn.Msg -> Model -> ( Model, Cmd Msg )
step msg model =
    let
        ( chat, effects ) =
            Turn.update msg model.chat
    in
    ( { model | chat = chat }
    , Cmd.batch (List.map runEffect effects)
    )

runEffect : Turn.Effect -> Cmd Msg
runEffect effect =
    case effect of
        Turn.OpenStream req ->
            openStreamPort { turn = req.turn, attempt = req.attempt, resumeAfter = req.resumeAfter, prompt = req.prompt }

        Turn.CloseStream info ->
            closeStreamPort info.attempt

        Turn.TurnEnded turn ->
            saveTurnPort (Turn.plainText turn)

        Turn.SubmitRejected reason ->
            toastPort reason
```

On the JavaScript side open the stream with `resumeAfter` sent as the Last Event ID and report back what happens. A parsed frame becomes `Turn.Received attempt envelope` after `Turn.decodeEnvelope`. A decode error becomes `Turn.Malformed attempt reason`. A successful open is `Turn.Connected attempt`. A failed open is `Turn.ConnectionFailed attempt { status = Just 503, retryAfterMs = Nothing }`. A closed stream is `Turn.ConnectionEnded attempt`. While `Turn.wantsTicks model.chat` is true, send `Turn.Tick 500` every half second. The user actions are `Turn.Submit "text"`, `Turn.Cancel` and `Turn.Regenerate`.

To render, use `Turn.transcript model.chat` and walk `Turn.blocks turn`. Check `turn.outcome` for `Finished`, `Failed` or `Cancelled` so a partial answer can be labelled as partial.

## Notes

The module targets Elm 0.19.1 and needs only `elm/core` and `elm/json`. The included `elm.json` is there so you can compile the file on its own. I could not reach the Elm package registry from the sandbox I wrote this in, so the file was reviewed by hand and not compiled there. Run `elm make LlmChatTurnMachine.elm` first and open an issue if the compiler disagrees with me.

Text limits count UTF 16 code units because that is what `String.length` returns in Elm. That is fine for a safety cap and not meant as a token count.

The wire contract is opinionated: sequence numbers from 1, one turn id per stream and an explicit `done` frame. If your provider streams raw SSE with no ids, put a small gateway in front that stamps them. The gateway is about thirty lines and it buys you correct resume for free.

Tool execution is deliberately out of scope. The machine tells you which calls are `Ready` and leaves running them and feeding results back to you, because that part is always application specific.

One known limit: a replayed stream that changes its mind about earlier events, for example a server that regenerates different text after a resume, cannot be detected. The machine trusts that a given `seq` always carries the same event. If you cannot guarantee that, restart the turn instead of resuming.
