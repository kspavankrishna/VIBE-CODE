# Streaming Context Predictor

Streaming LLM responses do not tell you how big they are until they are already too big. This is a small TypeScript class that watches token chunks as they arrive, projects the next chunk ahead of time and fires an event before the context window overflows.

**Language:** TypeScript | **Lines:** 107 | **Added:** 2026-04-07

## What this solves

Context overflow during streaming is a silent failure. You open a stream, tokens arrive chunk by chunk and everything looks healthy right up to the moment the provider truncates the response or returns a context length error mid generation. The user sees a sentence that stops halfway. Your logs show a completed request. Nobody gets paged because nothing crashed.

The reason it is hard to catch is timing. With a non streaming call you know the token count before you commit. With streaming you only learn the size of a chunk after it has been counted against the window. By the time your counter says you are at the limit you have already blown past it. The check happens one chunk too late, every time.

The cost lands in a few places. Long form generation gets cut off and has to be regenerated, doubling the spend on the most expensive calls you make. Chat UIs with long histories drop the top of the conversation without telling anyone, so the model quietly loses the system prompt and gives worse answers for reasons nobody can reproduce. Agent loops that carry tool output forward hit the wall on iteration nine of ten and lose the run. Support notices before monitoring does, because the symptom is a bad answer rather than an error code.

This class turns that late signal into an early one. It keeps a rolling average of recent chunk sizes, projects the next chunk with a safety multiplier and emits a warning while there is still room to act: checkpoint the context, summarise and restart the stream, drop old turns or degrade gracefully. You get a decision point instead of a truncation.

## Why I built it

Provider SDKs give you usage numbers after the fact. You get prompt tokens and completion tokens when the stream finishes, which is exactly when it is useless for preventing overflow. Tokenizer libraries can count text you already have, but they cost you a tokenizer pass per chunk and they still cannot tell you how big the next chunk will be. Nothing in the standard tooling answers the question that actually matters mid stream: am I about to run out of room.

So the gap is prediction, not measurement. The projection does not need to be clever. Chunk sizes are fairly stable over short windows, so a rolling mean plus a margin is enough to see the wall coming. This file is that idea in about a hundred lines with no dependencies beyond Node's `events`.

## When to use it

- A chat UI where conversation history grows across turns and you need to summarise or trim before the window fills
- Long form generation, a report or a chapter, where a truncated response means paying for the whole call twice
- An agent loop that appends tool output to context and can run for an unpredictable number of iterations
- A streaming proxy or gateway deciding whether to keep relaying or cut over to a fresh stream
- Any place you currently discover overflow from a provider error rather than your own instrumentation

## How it works

The exported class is `StreamingContextPredictor` and it extends Node's `EventEmitter`, so it plugs into an existing streaming handler as a listener target. The constructor takes a `StreamConfig` with three numbers: `contextLimit` is the window size in tokens, `warningThreshold` is the fraction of that limit at which the soft warning starts firing and `overflowMargin` is the reserve you want left untouched.

The hot path is `trackTokens(count)`, called once per chunk with that chunk's token count. It adds the count to `tokensUsed` and pushes it onto `tokenHistory`, a plain array used as a fixed size sliding window: once the length passes 10 the oldest entry is dropped with `shift()`. That window is the whole memory of the class. `getAverageTokenRate()` is an arithmetic mean over it, returning 0 on an empty history so the first call does not divide by zero.

The prediction is one line: `estimatedNext = Math.ceil(avgTokens * 1.2)`. Rolling mean of the last ten chunks, then 20 percent headroom on top so a chunk that runs larger than recent average does not sneak past. Overflow is then declared when `tokensRemaining - estimatedNext < overflowMargin`, which reads as: after the next chunk lands there would be less than the reserve left. Every call returns a `StreamMetrics` object with `tokensUsed`, `tokensRemaining`, `estimatedTokensNext`, the boolean `willOverflow` and `overflowIn`, which is the projected headroom in tokens after the next chunk, floored at zero by `Math.max`.

Two events come out of `trackTokens`. `warning:approaching-limit` fires whenever `tokensUsed >= contextLimit * warningThreshold`, so it repeats on every subsequent chunk once you are past that line. Treat it as a level, not an edge. `critical:overflow-predicted` fires when `willOverflow` is true, and immediately after emitting it the class calls `reset()` on itself. That is deliberate in the author's design: the critical event is the handoff point where you are expected to start a new stream, and the predictor zeroes `tokensUsed` and clears `tokenHistory` so it is ready to track the replacement stream from scratch. `reset()` also emits `stream:reset`, and you can call it yourself at any time. Note the ordering, the metrics object handed to the listener is a snapshot taken before the reset, so the numbers in the event are still the real pre reset state.

Two read paths sit alongside the tracker. `getMetrics()` recomputes the same `StreamMetrics` shape with no mutation and no events, which is what you poll from a UI or a health check. Its overflow test reads `tokensRemaining < estimatedNext + overflowMargin`, algebraically the same condition `trackTokens` uses. `predictOverflow()` is the cheaper variant: it projects `tokensUsed + avgRate` without the 1.2 multiplier and returns `willOverflow` plus `tokensUntilOverflow`, the distance to the limit in tokens. Everything is synchronous arithmetic over at most ten numbers, so calling it per chunk costs effectively nothing.

## Usage

```ts
import StreamingContextPredictor from "./StreamingContextPredictor";

const predictor = new StreamingContextPredictor({
  contextLimit: 128000,
  warningThreshold: 0.8,   // warn once 80% of the window is used
  overflowMargin: 2000,    // keep 2000 tokens in reserve
});

predictor.on("warning:approaching-limit", (m) => {
  console.warn(`approaching limit: ${m.tokensUsed}/${m.tokensUsed + m.tokensRemaining}`);
});

predictor.on("critical:overflow-predicted", (m) => {
  // predictor has already reset itself, start the next stream here
  console.error(`overflow predicted, next chunk ~${m.estimatedTokensNext} tokens`);
  checkpointAndRestartStream();
});

predictor.on("stream:reset", () => console.log("counters cleared"));

// seed with the prompt, then feed every streamed chunk
predictor.trackTokens(promptTokenCount);

for await (const chunk of stream) {
  const metrics = predictor.trackTokens(countTokens(chunk));
  if (metrics.willOverflow) break;
}

// poll without mutating state
const live = predictor.getMetrics();

// cheaper unpadded projection
const { willOverflow, tokensUntilOverflow } = predictor.predictOverflow();

predictor.reset();
```

## Notes

- It does not tokenize anything. You supply the count per chunk, from your provider's usage stream or your own tokenizer. Feed it character or word counts and you get a predictor that is internally consistent and externally wrong.
- `critical:overflow-predicted` resets the instance as a side effect. If you expected the counters to survive so you could inspect them after the event, they will not. Read what you need from the metrics object passed to the listener.
- `warning:approaching-limit` has no de duplication. Once you are over the threshold it fires on every `trackTokens` call until a reset. Debounce in your handler if that is noisy.
- The rolling window is hard coded at 10 entries and the safety multiplier at 1.2. Neither is configurable through `StreamConfig`. Change the constants in the file if your chunk sizes are spikier.
- The prediction looks exactly one chunk ahead. It does not model the full remaining response, so it cannot tell you up front whether a generation fits.
- Nothing validates the config. A zero `contextLimit`, a negative `overflowMargin` or a `warningThreshold` outside 0 to 1 produces nonsense quietly rather than throwing.
- The only dependency is the Node `events` module, so this runs in Node and in bundlers that shim `events`. There is no browser native fallback in the file.
