# AI SSE Resume Ledger

A phone flips from wifi to LTE while your model is still streaming, the browser reconnects with `Last-Event-ID`, and your SSE endpoint either replays tool calls the client already rendered or drops the completion frame. This is a bounded, in memory replay ledger for resumable LLM token streaming over Server-Sent Events, in one Kotlin file with no dependencies outside the JDK.

**Language:** Kotlin | **Lines:** 1189 | **Added:** 2026-05-20

## What this solves

Streaming an assistant response is the easy half. The reconnect is the hard half. Your gateway streams eight thousand characters plus six tool calls, and at character three thousand the connection dies. The EventSource reconnects on its own and sends back the last event id it saw. Now the server has to answer a question most streaming code never models: what does this client already have, and what does it still need.

Without a ledger you get one of three bad outcomes. You replay from the beginning, so the user watches the same paragraph type itself twice and the tool cards duplicate. You resume from nothing, so the middle of the answer is missing and the text is quietly truncated, which nobody catches until a ticket says the answer "cut off". Or you retain every frame forever, fine on a laptop and heap pressure at four thousand concurrent streams. That third one is the nasty failure, because it degrades the healthy streams rather than the broken one.

A fourth case buffering alone cannot fix: the process restarts during a deploy, or the session was reclaimed long ago, and a client shows up with a stale cursor. Treat it as valid and you emit frames from a different response entirely, and nothing appears in the logs. So every event id here is namespaced by a per session epoch, and a cursor from an unrecognised epoch gets a snapshot or an explicit `GONE`, never a guess. Tool calls raise the stakes: text is roughly idempotent, a `tool.call` a client acts on twice is not.

## Why I built it

The SSE spec gives you `Last-Event-ID` and stops. It is a header, not a protocol. Ktor, Spring WebFlux and Micronaut hand you a channel to write events into and model no replay window, no checkpoint and no tool state, because that is application semantics and they are right not to guess. Provider SDKs stream tokens and remember nothing. So every team shipping assistant streaming writes the same buffer, finds the same duplicate tool bug in week three, then bolts on a size cap after the first out of memory alert. Redis Streams buys durability at the price of an operational dependency and still knows nothing about snapshots or outstanding tool calls.

## When to use it

- A Ktor or Spring Boot service proxies OpenAI, Anthropic or Gemini streaming to clients that reconnect mid response.
- Duplicated tool events after a reconnect corrupt the client timeline.
- Streaming is clean in staging on one connection and falls apart on flaky mobile networks.
- Heap keeps climbing under concurrent streams because the replay buffer has no ceiling.
- You need a stream to survive a client reconnect but explicitly not a server restart.
- You want reconnect behaviour you can unit test with a fake `Clock` instead of by pulling out a cable.

## How it works

Every event id is a cursor of the form `epoch:seq`, produced by the private `Cursor` class and written into the SSE `id:` field. The epoch is six bytes of `SecureRandom` hex minted per session in `open`, the sequence is a monotonic counter, and `Cursor.parse` splits on the last colon because the epoch is opaque. Because the epoch changes whenever a stream id is reopened, a cursor from a previous life can never be read as a valid position in the current one.

Each `StreamSession` holds an `ArrayDeque<FrameRecord>` as the bounded replay window, a `StringBuilder` of assistant text, and a `LinkedHashMap<String, ToolState>` in LRU access order. `appendFrameLocked` stamps the cursor, calls `frame.encodedBytes()` and adds the result to a running `retainedBytes` total, so the byte budget is measured on the real wire encoding rather than an estimate. Everything runs under one `ReentrantLock`, which is the right trade for critical sections that are microseconds of `StringBuilder` work.

Checkpointing is what makes bounded retention safe. `maybeCheckpointLocked` emits an `assistant.snapshot` frame when `charsSinceCheckpoint` crosses `checkpointEveryChars` (512) or `framesSinceCheckpoint` crosses `checkpointEveryFrames` (24). `buildSnapshotJson` puts the full retained text, `truncatedPrefixChars`, current metadata, every tool state and the terminal state into that one payload. Then `compactToLatestCheckpointLocked` discards every frame older than it, because a snapshot is a self sufficient restore point and the deltas behind it are dead weight. `ensureCheckpointBeforeTerminalLocked` forces one more before `complete` or `fail`, so a client reconnecting after the stream ended gets the whole answer in a single frame.

`resume` is a small decision table, and it is the part worth reading before you trust this. An unknown stream id returns `ReplayMode.GONE`. A missing or unparseable cursor, an epoch mismatch, or a sequence older than the oldest retained frame returns `ReplayMode.SNAPSHOT` starting at the latest checkpoint. A cursor at or past the newest frame returns `ReplayMode.LIVE_DELTA` with no frames, the "already caught up, keep listening" answer. Anything between returns `LIVE_DELTA` with exactly the frames after that cursor, capped by `limit`. Every batch also reports `streamOpen`, `latestEventId` and `checkpointEventId`.

Duplicates are suppressed by SHA-256 fingerprint. `fingerprint` hashes its parts with a zero byte separator so field boundaries cannot be forged, and the key goes into a `dedupeIndex` beside the frame, so a repeated `appendToolCall`, `appendToolResult`, `appendMetadata`, `complete` or `fail` with byte identical arguments returns the original frame rather than emitting a second. Text deltas carry no dedupe key on purpose, since two identical tokens in a row are normal. On the memory side, `enforceBudgetsLocked` writes a checkpoint then evicts from the head until the frame count and byte total are both under their caps, `dropOldestReplayRecordLocked` refuses to drop the oldest checkpoint while other frames remain, text over `maxAssistantTextChars` is trimmed from the front into `truncatedPrefixChars` so a client can tell truncation from data loss, and `pruneExpiredLocked` plus `evictIfNeededLocked` reclaim idle sessions and drop closed ones before open ones.

## Usage

There is no `main` and no CLI. It is one class you construct and call.

```kotlin
val ledger = AiSseResumeLedger(
    AiSseResumeLedger.Config(
        maxSessions = 4_096,
        idleTtl = Duration.ofMinutes(30),
        maxRetainedFramesPerStream = 256,
        maxReplayBytesPerStream = 512 * 1024,
        maxAssistantTextChars = 128 * 1024,
        checkpointEveryChars = 512,
        checkpointEveryFrames = 24
    )
)

val opened = ledger.open(
    streamId = "conv_9f2a",
    metadataJson = """{"model":"claude-opus-5"}"""
)
write(opened.startedFrame.encode())   // id: <epoch>:1, event: response.started

for (delta in gatewayTokens) {
    write(ledger.appendAssistantText("conv_9f2a", delta).encode())
}

write(ledger.appendToolCall(
    streamId = "conv_9f2a",
    toolCallId = "call_1",
    toolName = "search_docs",
    argumentsJson = """{"q":"sse resume"}"""
).encode())

write(ledger.appendToolResult(
    streamId = "conv_9f2a",
    toolCallId = "call_1",
    resultJson = """{"hits":3}"""
).encode())

write(ledger.complete(
    streamId = "conv_9f2a",
    finishReason = "stop",
    usageJson = """{"input":812,"output":1440}"""
).encode())
// or: ledger.fail("conv_9f2a", "upstream_timeout", "gateway closed", retriable = true)
```

Reconnect handler:

```kotlin
val batch = ledger.resume(
    streamId = "conv_9f2a",
    lastEventId = call.request.headers["Last-Event-ID"]
)

when (batch.mode) {
    AiSseResumeLedger.ReplayMode.GONE -> respondNotFound()
    else -> write(batch.encode())   // SNAPSHOT or LIVE_DELTA, already SSE encoded
}
if (!batch.streamOpen) closeConnection()
```

`ledger.drop(id)` removes a session, `ledger.prune()` reclaims idle sessions and re-enforces budgets, `ledger.stats()` returns per stream frames, bytes and tool state counts for a metrics endpoint. Pass a fixed `Clock` in `Config` to drive TTL deterministically in tests.

## Notes

- Single process and in memory. Nothing survives a restart or crosses nodes, so resume needs sticky routing or a single instance. For durability across deploys this is the wrong layer.
- `looksLikeJson` is a shape check, not a parser. It inspects the first and last characters only, so malformed JSON with matching braces is spliced in verbatim. Validate upstream payloads if you do not trust them.
- `resume` does not prune. A session past `idleTtl` stays resumable until `open`, `prune` or `stats` runs the sweep, so call `prune()` on a schedule.
- `complete` and `fail` check the dedupe index before the open flag, so a repeated terminal call with identical arguments returns the cached frame rather than throwing. A different one on a closed stream throws `IllegalStateException`.
- `open` on an already open stream id throws. Reopening a closed id mints a new epoch, which correctly forces old cursors onto the snapshot path.
- `retryMillis` exists on `SseFrame` and `encode` honours it, but the ledger never sets it. `encodedBytes()` encodes each frame a second time to size it. No exit codes, no I/O, no framework: you supply the transport.
