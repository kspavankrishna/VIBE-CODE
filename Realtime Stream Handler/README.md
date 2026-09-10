# Realtime Stream Handler

Streaming an LLM response token by token fires your callback hundreds of times for a single reply. This is a small TypeScript class that buffers those deltas into fixed size chunks before it calls you back, so the UI, the socket or the queue downstream only sees work it can actually keep up with.

**Language:** TypeScript | **Lines:** 74 | **Added:** 2026-04-03

## What this solves

Naive streaming floods callbacks and loses control over buffer management. When you iterate the Anthropic SDK stream directly and push every `text_delta` into React state, into a WebSocket `send`, or into a database write, you get one unit of work per token. A 600 token answer becomes 600 state updates, 600 frames of layout thrash, 600 tiny TCP writes. On a fast connection the model emits deltas quicker than the browser can paint. The tab goes to 100 percent CPU on one chat, the text visibly stutters and users report that the "fast" streaming feels slower than waiting for the whole answer.

The same failure shows up server side and costs more. Every delta forwarded as its own SSE event or its own Kafka message means the per message overhead dominates the payload: a 4 byte token wrapped in 60 bytes of framing. Broker throughput collapses under message count long before it collapses under bytes. If your fan out writes each delta to Redis or Postgres you are doing hundreds of round trips per completion, and with fifty concurrent users that is tens of thousands of round trips a minute for text that would fit in a few kilobytes. Nobody notices in dev with one tester. Everybody notices at 3pm on a launch day.

There is a second, quieter failure. Without a wrapper you have no idea how much streaming traffic you are actually generating. There is no count of deltas, no single place to instrument. When latency regresses you cannot tell whether the model got slower or whether your own callback became the bottleneck, because the callback and the transport are tangled into the same for-await loop.

`StreamOrchestrator` puts a buffer between the SDK and your code. Deltas accumulate into a string. The callback fires only when that string crosses a size threshold you choose, plus one final flush for whatever is left when the stream ends. The full text is still returned, so you keep the complete response for logging or persistence without reassembling it yourself.

## Why I built it

The Anthropic SDK gives you a clean `messages.stream()` and a typed event union. That part is fine. What it does not give you is any opinion about pacing. Every codebase I have seen ends up rewriting the same `for await` loop with the same `event.type === "content_block_delta" && event.delta.type === "text_delta"` guard, and each rewrite re-decides how to batch, or forgets to batch at all. The guard itself is easy to get subtly wrong once thinking blocks and tool use blocks appear in the same stream.

Full streaming frameworks solve this but drag in a runtime, an abstraction over the provider and a plugin model you did not ask for. I wanted the batching decision in one file I can read in a minute, with the SDK types intact and nothing between me and the transport. That is what this is.

## When to use it

- A chat UI where token by token updates make the browser stutter and you want to repaint every 50 characters instead of every token.
- An SSE or WebSocket endpoint where per event framing overhead is larger than the text you are actually sending.
- A pipeline that writes model output into a queue or a database and cannot afford one write per token.
- A fan out job running the same or different prompts across many inputs, where you want them in flight at once and one callback receiving all the text.
- Any point where you need the streamed chunks and the complete final string, without stitching the string back together at the call site.

## How it works

The file exports a single class, `StreamOrchestrator`, plus an internal `StreamCallback` type: `(chunk: string) => void | Promise<void>`. The callback may be sync or async and the class awaits it either way. The constructor takes no arguments and builds `new Anthropic()`, so credentials come from the SDK's own environment resolution, normally `ANTHROPIC_API_KEY`.

Instance state is three fields: the `client`, a `buffer` string and a `metrics` object holding `chunksReceived` and `totalTokens`. The buffer is a plain accumulator, not a ring buffer and not a fixed allocation. It grows by string concatenation and is reset to empty on each flush.

`stream(prompt, onChunk, bufferSize = 50)` is the core. It calls `this.client.messages.stream()` with the model pinned to `claude-3-5-sonnet-20241022`, `max_tokens` fixed at 1024 and the prompt as a single user message. It then iterates the stream with `for await`. Each event is filtered by two checks: `event.type === "content_block_delta"` and `event.delta.type === "text_delta"`. Anything else, message start and stop events, content block boundaries, non text deltas, is ignored. Text that passes the filter is appended to both `this.buffer` and a local `output` string, and `metrics.chunksReceived` is incremented once per delta.

The flush rule is a size threshold and nothing more. After each append, if `this.buffer.length >= bufferSize` the class awaits `onChunk(this.buffer)` and clears the buffer. Because the await happens inside the loop, an async callback applies backpressure: the loop does not pull the next delta until your handler resolves. That is the useful property here. A slow database write or a saturated socket naturally slows consumption instead of piling up an unbounded queue of pending writes. When the stream ends, any remainder is flushed with a final `onChunk` call guarded by `this.buffer.length > 0`, so an exactly-aligned stream does not emit an empty trailing chunk. The method returns `output`, the full concatenated text.

`parallelStream(prompts, onChunk)` maps the prompt array through `stream` and wraps it in `Promise.all`. Every request is started immediately, all of them share the one `onChunk`, and the resolved array preserves input order regardless of which completion finished first. There is no concurrency limit: a hundred prompts means a hundred simultaneous requests. Note that all parallel streams share the single instance level `this.buffer`, so their text interleaves in that buffer and the chunks your callback receives will be a mix. For parallel work where chunk provenance matters, construct one `StreamOrchestrator` per stream.

`getMetrics()` returns the metrics object directly. `chunksReceived` is a live count of text deltas across every call on that instance. It is returned by reference, not copied, so treat it as read only.

## Usage

```ts
import { StreamOrchestrator } from "./RealtimeStreamHandler";

const orch = new StreamOrchestrator();

// Single stream, flush every 50 characters (the default)
const full = await orch.stream(
  "Explain backpressure in one paragraph.",
  (chunk) => process.stdout.write(chunk)
);

// Larger buffer, async callback applies backpressure to the loop
await orch.stream(
  "Summarise this changelog.",
  async (chunk) => { await socket.send(chunk); },
  200
);

// Several prompts in flight at once, one shared callback
const answers = await orch.parallelStream(
  ["Prompt A", "Prompt B", "Prompt C"],
  (chunk) => buffer.push(chunk)
);

console.log(orch.getMetrics()); // { chunksReceived: n, totalTokens: 0 }
```

Requires `@anthropic-ai/sdk` and an API key in the environment. There is no CLI and no `main` in this file, it is a library class only.

## Notes

- `metrics.totalTokens` is declared and initialised to zero but never updated anywhere in the file. Only `chunksReceived` carries real data, and it counts text deltas, not tokens.
- The model id and `max_tokens` are hardcoded. There is no parameter for system prompts, temperature, tools, multi turn message history or a stop reason. Edit the object in `stream()` or fork the class if you need them.
- `bufferSize` is compared against `buffer.length`, which is UTF-16 code units in JavaScript. Emoji and many non Latin scripts count as more than one unit per visible character.
- Flushing is purely length based. Chunks split mid word, mid sentence and mid markdown token. If you are parsing the stream rather than displaying it, you must handle partial syntax yourself.
- `parallelStream` has no concurrency cap and shares one buffer across all in flight streams, so chunks from different prompts interleave in the callback. Use separate instances when you need to tell them apart.
- No retry, no timeout, no abort signal and no try/catch. An SDK error, a rate limit or a dropped connection rejects the returned promise, and any text already sitting in `this.buffer` is lost and stays there, polluting the next call on the same instance.
- Only `text_delta` content is surfaced. Thinking blocks, tool use blocks and any other content type are silently dropped.
