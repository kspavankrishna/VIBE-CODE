# Token Stream Optimizer

An LLM streams tokens as fast as the network delivers them, and whatever sits downstream of your stream loop has to keep up. This folder holds two takes on the same idea: put a buffer and a rate limiter between the model output and the thing consuming it.

**Language:** Python and TypeScript | **Lines:** 136 (43 Python, 93 TypeScript) | **Added:** 2026-04-03

## What this solves

Naive streaming works fine in a terminal demo and falls apart the moment a real consumer is attached. The Anthropic SDK hands you text fragments as they arrive, sometimes a few characters at a time, sometimes a whole sentence. If every fragment triggers a websocket frame, a React state update, a Redis publish or a database write, you have turned one API call into several thousand downstream events. The browser tab starts dropping frames. The message queue backs up. Nobody looks at the LLM call as the cause because the LLM call itself was fast.

The second failure is rate. A model can emit hundreds of tokens per second, and the system on the other side often cannot absorb that. A webhook receiver with a 100 requests per second cap starts returning 429s. A Postgres connection pool saturates. A frontend event handler runs so often that the main thread never yields. Backpressure is the polite name for this. In production it shows up as timeouts, dropped connections and a support ticket that says the app freezes on long answers.

The third thing that goes wrong is that nobody counts. Token usage is the bill, and if you are not accumulating a count while the stream runs you find out what the feature costs at the end of the month. Same for the kill switch: when the upstream model is degraded or you have blown a budget, you want a flag that stops new streams immediately rather than a deploy.

These two files attack those three problems from different angles. The Python one sits on a live Anthropic stream and reshapes it. The TypeScript one is a self contained pacing and accounting layer that never touches the network.

## Why I built it

Every streaming tutorial ends at `for text in stream.text_stream: print(text)`. That is the entire example. The gap between that line and a service that streams to ten thousand connected clients is all buffering, pacing and metrics, and none of it ships in the SDK. The SDK's job is to give you tokens. What you do about the rate is your problem.

I did not want a framework for it. I wanted the smallest honest version of the pattern in both languages I actually ship in, so I could see the shape of it clearly before deciding whether it deserved to become a library.

## When to use it

- You are streaming model output into a websocket and each fragment becomes a frame, so the client is drowning in tiny messages.
- Your downstream API or database has a documented rate cap and the model happily exceeds it.
- A React or Vue frontend re renders on every chunk and the UI stutters on long generations.
- You are pushing generated text into a queue and want batches of roughly N units instead of character noise.
- You need a running token count and a rough cost figure emitted at the end of every generation.
- You want a manual switch that refuses new streams when the model provider is having a bad day.

## How it works

The Python file, `TokenStreamOptimizer.py`, is three generators layered on top of each other. `streaming_generator` opens `client.messages.stream` with `max_tokens=2048` against `claude-3-5-sonnet-20241022` by default and yields each fragment from `stream.text_stream` inside the context manager, so the HTTP connection closes when iteration ends. `token_aware_chunker` wraps it and keeps a `buffer` string: it appends every incoming fragment, and while the buffer has reached `chunk_size` it yields exactly the first `chunk_size` characters and keeps the remainder. After the source is exhausted it flushes whatever is left. `rate_limited_stream` is the simpler sibling: pass through every chunk and `time.sleep(delay)` between yields, default 0.01 seconds. Because these are generators, memory stays flat no matter how long the response is. Nothing accumulates except the partial buffer.

One accuracy note on the name. `chunk_size` is measured in characters, not tokens. The function slices the buffer with `buffer[:chunk_size]`, so a chunk can end mid word and mid multi byte grapheme cluster if you are unlucky with emoji.

The TypeScript file, `TokenStreamOptimizer.ts`, is a class extending Node's `EventEmitter`, and it works on a string you already have rather than a live API stream. `tokenize` splits text with the regex `/\b\w+\b|[^\w\s]/g`, so words and standalone punctuation become separate units. `streamTokens(text, chunkSize = 5, delayMs = 50)` walks those units in slices and emits a `tokens` event per batch, a `progress` event every twentieth batch and a `complete` event carrying `tokenCount` and `estimated_cost_usd` at the end.

The pacing is a token bucket. `RateLimitConfig` gives you `tokensPerSecond` and `burst`, the bucket starts full at `burst`, and `checkRateLimit` subtracts the batch size when there is enough credit. When there is not, the caller awaits `delayMs` and calls `refillTokens`. Token bucket is the right primitive here because it permits a short burst then settles to a steady average, which is what most rate limited APIs actually enforce. Be aware that this refill is approximate: `refillTokens` adds `tokensPerSecond / 1000` per call and clamps at `burst`, so the effective replenishment depends on how often it is called rather than on elapsed wall clock time.

Two more pieces. `setCircuitBreaker(open)` flips `circuitOpen` and emits `circuit-open`; while open, `streamTokens` throws `Circuit breaker open` on entry. This is a manual breaker, not one that trips itself on a failure threshold. `estimateCost` multiplies `tokenCount / 1000` by a hardcoded `0.0001` and rounds to four decimals, and `getMetrics()` returns the count, the cost and the remaining bucket credit at any moment.

## The two implementations

They share a name and an intent, not a design. The Python version is a real client: it makes the API call and its value is in reshaping a stream it does not control. The TypeScript version never calls an API. It takes a string that already exists, tokenizes it locally and paces the emission. If you feed it a full response, you are simulating a stream, not consuming one.

That difference decides which one you want. Reach for the Python file when you are the process that talks to Anthropic and you need chunk boundaries or a delay between yields, for example a FastAPI endpoint relaying to server sent events. Reach for the TypeScript class when you want the pacing and instrumentation layer: a real token bucket instead of a flat sleep, an event interface instead of a generator, a running token count, a cost estimate and a circuit breaker flag. The Python file has none of that accounting. The TypeScript file has all of it and no network.

The concurrency models differ too. Python generators are pull based, so the consumer sets the pace and backpressure is implicit: if you stop iterating, nothing is produced. The `EventEmitter` in the TypeScript class is push based, so the emitter sets the pace and the token bucket is what stands in for backpressure, because a listener has no way to say slow down. Pull based is safer when the consumer is genuinely slow. Push based is easier to wire into an existing event driven Node service.

If you want the full picture, the honest combination is the Python streaming loop feeding the TypeScript pacing and metrics design, ported to whichever language your service is in.

## Usage

Python, the `__main__` block as written:

```bash
export ANTHROPIC_API_KEY=...
pip install anthropic
python3 "TokenStreamOptimizer.py"
```

```python
from TokenStreamOptimizer import token_aware_chunker, rate_limited_stream, streaming_generator

# 100 character chunks from a live stream
for chunk in token_aware_chunker("Write a function that calculates fibonacci", chunk_size=100):
    print(chunk, end="", flush=True)

# raw passthrough with a 10ms gap between fragments
for chunk in rate_limited_stream("Explain token bucket rate limiting", delay=0.01):
    handle(chunk)
```

TypeScript:

```ts
import TokenStreamOptimizer from './TokenStreamOptimizer';

const opt = new TokenStreamOptimizer({ tokensPerSecond: 100, burst: 10 });

opt.on('tokens', (batch: string[]) => socket.send(batch.join(' ')));
opt.on('progress', ({ processed, total }) => console.log(processed, total));
opt.on('complete', (m) => console.log(m.tokenCount, m.estimated_cost_usd));
opt.on('circuit-open', () => console.warn('halted'));

await opt.streamTokens(responseText, 5, 50);
console.log(opt.getMetrics());

opt.setCircuitBreaker(true); // next streamTokens throws
```

## Notes

- The Python module needs the `anthropic` package and a valid `ANTHROPIC_API_KEY` in the environment. It has no retry, no exponential backoff and no exception handling, so an API error propagates straight to the caller.
- `token_aware_chunker` counts characters, not tokens, despite the name. Chunks can split words and multi byte characters.
- `rate_limited_stream` uses a blocking `time.sleep`. It is fine in a worker or a thread, wrong inside an asyncio event loop.
- The TypeScript class does not call any API. It paces text you already have, so it is a pacing and accounting layer and not a client.
- The refill in `refillTokens` is call driven, not clock driven, so the sustained rate is only loosely tied to the configured `tokensPerSecond`. Treat the bucket as a coarse throttle, not a guarantee.
- `estimateCost` uses a single fixed `0.0001` per thousand tokens with no input and output split and no per model pricing. Change the constant before quoting the number to anyone.
- The circuit breaker is manual only. Nothing in the code trips it on errors or on a failure count, and there is no half open state or automatic reset.
- Both files are single file references with no tests, no config loading and no logging. Copy the pattern, do not import it blind.
