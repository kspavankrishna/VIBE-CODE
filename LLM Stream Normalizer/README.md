# LLM Stream Normalizer

Streaming LLM responses arrive in a different wire format from every provider, and tool call arguments show up as broken JSON fragments spread across dozens of events. This is a single Swift file that parses the transport once, detects the provider from the payload shape and hands the rest of your app one boring event model.

**Language:** Swift | **Lines:** 1191 | **Added:** 2026-04-14

## What this solves

The annoying part of streaming LLM integrations on Apple platforms is that every provider sends a slightly different wire format. OpenAI Chat Completions sends SSE frames with a `choices` array and ends with a literal `data: [DONE]`. OpenAI Responses sends named events like `response.output_text.delta` and `response.function_call_arguments.delta`. Anthropic sends `message_start`, `content_block_start`, `content_block_delta` and `message_stop` with per block indices. Some gateways strip the SSE framing and give you newline delimited JSON. Each one needs its own parser, and the parsers rot the moment a provider adds a field.

The failure that actually costs you is tool call assembly. Arguments do not arrive as one JSON object. They arrive as `{"loc`, then `ation":"Ba`, then `ngalore"}`. If you run `JSONDecoder` on each chunk you get a decode error on every fragment. If you concatenate blindly and decode too early you dispatch a tool call with truncated arguments, which in a real app means a wrong API call, a wrong database write or a crash on force unwrap. Worse, it is intermittent: short arguments arrive in one chunk and work fine in testing, long arguments split and break in production. QA never sees it, users do.

The second failure is buffering at the byte layer. `URLSession.bytes` hands you arbitrary chunk boundaries. A chunk can end halfway through a line, halfway through a multi byte UTF-8 sequence or between the `event:` line and its `data:` line. Naive code that does `String(data:encoding:)` per chunk and splits on newlines drops characters, mangles emoji and silently loses whole events. You notice when a user types something in Japanese and half the reply disappears.

Usage accounting is scattered the same way. OpenAI Chat reports `prompt_tokens` and `completion_tokens`, OpenAI Responses reports `input_tokens`, `output_tokens` plus cached and reasoning token details, Anthropic reports cache creation and cache read counts split across `message_start` and `message_delta`. One cost number per request means three extractors, and you forget one when you add a provider.

## Why I built it

I got tired of rewriting the same fragile SSE parsing and argument assembly code every time a Swift app needed OpenAI, Anthropic or both. The vendor SDKs each solve their own format and none of them agree on an event type, so a client supporting two providers ends up with two parallel streaming paths and two sets of bugs. Swift also has no equivalent of the JavaScript stream helpers, so everybody writes the same buffering loop by hand.

The design goal was that the rest of the codebase should never learn which provider it is talking to. Parse the transport once, detect the provider from real payload shape rather than a config flag, then keep a stateful accumulator that can rebuild tool call arguments without assuming any single chunk is valid JSON.

## When to use it

- Reading `URLSession.AsyncBytes` from OpenAI or Anthropic in a macOS or iOS client and rendering text deltas into a chat view.
- Supporting more than one model provider behind one UI, where switching provider should not change any downstream code.
- Assembling streamed function call arguments before dispatching a tool, without decoding a half written JSON object.
- Sitting behind a proxy or gateway that may hand you SSE on one route and newline delimited JSON on another.
- Tracking per request token usage including cache reads and reasoning tokens across providers that name those fields differently.
- Writing a Vapor service or local agent runner that relays model output and needs a stable internal event type.

## How it works

Two layers. `WireFrameDecoder` owns the bytes and `LLMStreamNormalizer` owns the meaning.

The decoder keeps a `Data` buffer and only ever cuts on a `0x0A` byte, stripping a trailing `0x0D` for CRLF streams. Anything after the last newline stays in the buffer until more bytes arrive, so a chunk that splits a multi byte UTF-8 sequence mid line is harmless. Decoding happens per complete line, and a line that is not valid UTF-8 throws an `NSError` in the `LLMStreamNormalizer` domain with code 1 rather than producing garbage. Transport is locked on the first non empty line: a line starting with `event:`, `data:`, `id:` or `:` locks SSE, anything else locks NDJSON. You can force it with `LLMStreamMode.sse` or `.ndjson` if you already know. SSE frames accumulate into a `PendingSSEFrame` and flush on the blank line, joining multiple `data:` lines with a newline the way the spec requires. Comment only frames set `isCommentOnly` and surface as `.keepAlive` so your idle timer can be reset instead of firing.

Provider detection lives in `selectProvider`. An explicit `providerHint` wins, otherwise the first frame decides and the choice sticks. An event name or a `type` field starting with `response.` means OpenAI Responses. A `type` starting with `message` or `content_block`, or an `error` type alongside a `message` or `delta` object, means Anthropic. A non empty `choices` array means OpenAI Chat Completions. Otherwise it falls back to `genericSSE` or `genericJSON`, which emit nothing unless `emitRawEvents` is on. A bare `[DONE]` from an otherwise unidentified stream is treated as Chat Completions and finalizes the run.

Text is accumulated into a `textChannels` dictionary keyed by a channel string that encodes provider and position: `openai-responses:text:<output_index>:<content_index>`, `openai-chat:choice:<index>` and `anthropic:text:<index>`. Every `LLMTextDelta` carries both the incremental `delta` and the cumulative `textSoFar` for that channel, so parallel choices or multiple content blocks never get interleaved into one string.

Tool calls go through `ToolAccumulator`, one per channel, with a `JSONFragmentTracker` doing the interesting work. The tracker is a small character state machine over unicode scalars: it tracks whether it is inside a string literal, whether the previous character was a backslash escape and the current bracket nesting depth for `{`, `[`, `}` and `]`. Brackets inside string literals are ignored, which is the case that naive brace counting gets wrong. `isLikelyComplete` is true when it has seen non whitespace, is not inside a string and depth is back to zero. That flag rides on every `LLMToolCallDelta` and `LLMToolCall` as `argumentsAreLikelyComplete`, so you can decode at the first safe moment instead of guessing. `replaceContents(with:)` reseeds the tracker when a provider sends the full arguments at the end, as OpenAI Responses does on `response.function_call_arguments.done`.

Completion is idempotent. `finalizeCompletion` is guarded by `didEmitCompletion`, so `response.completed`, `[DONE]`, `message_stop` and a call to `finish()` cannot produce two `.completed` events. When `finalizeToolCallsOnCompletion` is on, any accumulator still open is closed first and emitted as `.toolCallFinished` in sorted channel order, which is how you avoid dropping a tool call when a stream ends without the usual terminator. `snapshot()` gives you the current text channels, tool calls and latest usage at any point without disturbing the stream.

## Usage

```swift
import Foundation

let normalizer = LLMStreamNormalizer(
    options: LLMStreamNormalizerOptions(
        mode: .auto,                        // .sse or .ndjson to skip detection
        providerHint: nil,                  // e.g. .anthropic to skip sniffing
        emitRawEvents: false,               // true to see unmapped frames
        finalizeToolCallsOnCompletion: true
    )
)

var request = URLRequest(url: URL(string: "https://api.anthropic.com/v1/messages")!)
request.httpMethod = "POST"
// set headers and body elsewhere

let (bytes, _) = try await URLSession.shared.bytes(for: request)
var chunk = Data()

for try await byte in bytes {
    chunk.append(byte)
    guard chunk.count >= 1024 else { continue }
    for event in try normalizer.push(data: chunk) { handle(event) }
    chunk.removeAll(keepingCapacity: true)
}

for event in try normalizer.push(data: chunk) { handle(event) }
for event in try normalizer.finish() { handle(event) }   // flushes tail, emits .completed

func handle(_ event: LLMStreamEvent) {
    switch event {
    case .textDelta(let d):
        print(d.channel, d.delta, d.textSoFar.count)
    case .toolCallDelta(let d):
        if d.argumentsAreLikelyComplete {
            print("ready:", d.toolName ?? "?", d.argumentsSoFar)
        }
    case .toolCallFinished(let call):
        print(call.toolCallId, call.toolName ?? "?", call.argumentsJSON)
    case .usage(let u):
        print(u.inputTokens ?? 0, u.outputTokens ?? 0, u.cacheReadInputTokens ?? 0)
    case .completed(let c):
        print(c.reason ?? "", c.textChannels, c.toolCalls.count)
    case .keepAlive:
        break                                // reset your idle timer here
    case .error(let f):
        print("stream error", f.code ?? "", f.message)
    case .raw(let r):
        print(r.transport, r.eventName ?? "", r.data)
    }
}

// State at any point, without consuming events:
let snap = normalizer.snapshot()
```

`push(string:)` and `push(bytes:)` are the same entry point for a `String` or any `Sequence` of `UInt8`.

## Notes

- `LLMStreamNormalizer` is a class holding mutable state with no locking. It is not `Sendable` and not thread safe. Use one instance per stream and drive it from one task. The value types it emits are all `Sendable`.
- `argumentsAreLikelyComplete` is a heuristic, not validation. It counts brackets and quotes, it does not check that the JSON parses, that keys are unique or that the schema matches. Decode and handle the failure.
- Only three providers are mapped: OpenAI Responses, OpenAI Chat Completions and Anthropic. Anything else lands in `genericSSE` or `genericJSON`, which produce no semantic events at all unless you set `emitRawEvents: true`.
- Transport detection locks on the first non empty line and never re-evaluates. A stream that switches framing mid flight will be misparsed. Pass `mode` explicitly if your gateway is unpredictable.
- The SSE `id:` field is recognised for transport detection and then discarded. There is no `Last-Event-ID` tracking, so this does not help you resume a dropped connection. There is also no networking, no retry and no backoff in this file, only Foundation and parsing.
- `response.failed` and Anthropic `error` events emit `.error` but do not finalize the stream. Call `finish()` to get the `.completed` event with whatever was accumulated. Non UTF-8 bytes throw from `push` rather than emitting an error event.
- Anthropic `thinking` content blocks and OpenAI reasoning summary events are not mapped to text channels. Reasoning token counts are still reported through `LLMUsage` where the provider sends them.
