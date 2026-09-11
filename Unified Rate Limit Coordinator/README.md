# Unified Rate Limit Coordinator

One TypeScript class that holds the rate limit rules for every API your app talks to, so concurrency, token budgets, request spacing and 429 backoff live in a single place instead of being reinvented in each caller.

**Language:** TypeScript | **Lines:** 488 | **Added:** 2026-04-10

## What this solves

An app that fans out to an LLM provider, an embeddings endpoint and a search API has three different rate limits with three different shapes. One counts requests per minute. One counts tokens against a reservoir. One only cares about how many calls are in flight at once. So the code grows three ad hoc throttles, usually a `p-limit` here, a `setTimeout` sleep there and a retry wrapper somewhere else. None of them know about each other. None of them know what the server just told you in a `Retry-After` header.

The failure shows up under load. A background job kicks off a batch of 500 embedding calls at the same moment a user request needs one LLM call. The batch drains the shared budget, the user request queues behind it and the p95 latency chart goes vertical. Nobody notices until support tickets arrive, because the throttles are per module and no single place can tell you which key is saturated. Then the provider starts returning 429s, the retry wrapper retries immediately, the retries themselves get 429d and you are in a self inflicted feedback loop that keeps the key blocked longer than the original burst ever would have.

The other failure is quieter and more expensive. Without a reservoir model, you have no idea how close you are to the ceiling until you cross it. Requests go out, some fraction fail, the ones that fail get retried and the retries burn quota that succeeded calls would have used. For a paid API that bills on attempts, you are paying for the failures. For a provider that counts 429s against your account health, repeated bursts can get your rate limit lowered rather than raised.

This file centralizes the pressure control. Each API key or provider gets a `LimitProfile`. Callers ask the coordinator for a lease before doing work and release it after. The coordinator queues, spaces, meters and backs off. When the server pushes back, you hand the response headers to `applyFeedback` and the coordinator adjusts its own view of reality instead of guessing.

## Why I built it

The existing options solve one axis each. `p-limit` and `semaphore` handle concurrency and nothing else. `bottleneck` is closer, it does reservoirs and spacing well, but it does not take server feedback as an input, so you still bolt your own 429 handling onto it. Full gateways like Envoy or a Redis backed limiter are the right answer at scale and the wrong answer for a single Node process that talks to four SaaS APIs.

What I actually wanted was small, in process and feedback driven: something where the 429 you just received changes the behaviour of the next request without any extra plumbing, where a low priority batch job cannot starve a user facing call, and where I can print one table showing every key, its queue depth, its remaining tokens and its average wait. That is under 500 lines with no dependencies, so I wrote it.

## When to use it

- A Node service calling several LLM or embedding providers at once, each with a different requests per minute and tokens per minute ceiling.
- A batch importer running alongside live user traffic on the same API key, where the batch must yield to interactive requests.
- Any client where the provider returns `Retry-After` or `X-RateLimit-Remaining` headers and you want that information to actually change future scheduling.
- Requests that cost different amounts against the same budget, for example a 4000 token prompt versus a 200 token one, where a flat requests per second limiter is the wrong model.
- Long running jobs that need to be cancelled cleanly, where queued work should abort through an `AbortSignal` rather than being fired and forgotten.
- Any point where you want one snapshot showing which key is the bottleneck, without adding a metrics backend.

## How it works

Every registered key gets a `KeyState` in a private `Map`. `registerProfile` runs the input through `normalizeProfile`, which floors and clamps everything: concurrency and reservoir to at least 1, `minSpacingMs` to at least 0, `refillIntervalMs` to at least 1ms with a 60s default, and `cooldownMs` to at least 250ms with a 2s default. `refillAmount` defaults to the full reservoir, which gives you a classic fixed window: the whole budget comes back at once every interval. Re registering an existing key updates the profile in place and clamps live tokens down to the new reservoir rather than resetting state.

The core is a weighted token bucket with interval refill, wrapped in a counting semaphore. `refill` is lazy, not timer driven. It computes elapsed time since `lastRefillAt`, takes `Math.floor(elapsed / refillIntervalMs)` whole intervals, adds that many `refillAmount` batches capped at the reservoir, and advances `lastRefillAt` by exactly the intervals consumed rather than to `now`. That last detail matters. Advancing to `now` would silently discard the remainder of a partial interval and let the bucket drift slower than configured.

`acquire` builds a `QueueEntry`, pushes it and calls `sortQueue`. The ordering is priority descending, then weight ascending, then `seq` ascending. Higher priority wins, ties break toward the cheaper request so a small call is not stuck behind a heavy one, and the monotonic sequence number gives FIFO among true ties. The promise resolves with a `Lease` carrying `key`, `tokenId`, `acquiredAt` and a `release` function.

The scheduler is `pump`, driven by a single `setTimeout` per key that `schedule` resets each time. `pump` refills, calls `compactQueue` to reject any entries whose `AbortSignal` fired, then loops on the head of the queue. `requiredWaitMs` computes the head's wait as the max of three delays: `blockedUntil - now` from server backoff, `lastDispatchAt + minSpacingMs - now` from the spacing rule, and `tokensReadyIn` which projects how many refill intervals it takes to accumulate the missing weight. If the wait is positive, `pump` reschedules itself for exactly that long and returns, so there is no polling. If the wait is zero but `activeCount` has hit the concurrency ceiling, `pump` returns and waits for a `release` to call `schedule` again. Otherwise it shifts the entry off, clears its timeout handle, subtracts the weight, increments `activeCount`, records the wait sample and resolves the lease.

`applyFeedback` is the part that makes this different from a plain limiter. Pass it `observedLimit` and the reservoir is rewritten to what the server says. Pass `observedRemaining` and the token count is clamped to the server's number, so your local bucket resyncs to truth rather than drifting. For backoff it checks three signals in order: an explicit `retryAfterMs`, an absolute `rateLimitResetAtMs` in the future, or a bare `statusCode === 429` which falls back to the profile's `cooldownMs`. Each of those pushes `blockedUntil` forward using `Math.max`, so a later feedback call can never shorten an existing block, and bumps `deniedCount`. Then it reschedules the pump.

Cancellation and cleanup are handled in a few places. `timeoutMs` arms a timer that removes the entry and rejects. An `AbortSignal` gets a one shot listener that does the same with an error whose `name` is `AbortError`, and a signal already aborted at call time rejects before enqueue. `release` is idempotent twice over: a local `released` flag plus a `releasedLeases` set on the state, which self clears past 10,000 entries so it cannot grow without bound. `recentWaitsMs` is a 50 sample sliding window feeding `avgRecentWaitMs` in `snapshot`. `shutdown` flips a `disposed` flag, clears every timer and rejects every queued entry with the given reason, and `assertOpen` makes any later `acquire` or `registerProfile` throw.

## Usage

```ts
import { UnifiedRateLimitCoordinator } from './UnifiedRateLimitCoordinator';

const coordinator = new UnifiedRateLimitCoordinator([
  {
    key: 'openai:chat',
    concurrency: 8,
    reservoir: 90_000,        // tokens per minute
    refillAmount: 90_000,
    refillIntervalMs: 60_000,
    minSpacingMs: 20,
    cooldownMs: 5_000
  },
  { key: 'search:serp', concurrency: 4, reservoir: 60, minSpacingMs: 100 }
]);

// run() acquires, executes and always releases
const reply = await coordinator.run(
  'openai:chat',
  async () => {
    const res = await fetch(url, { method: 'POST', body });

    coordinator.applyFeedback('openai:chat', {
      statusCode: res.status,
      retryAfterMs: Number(res.headers.get('retry-after') ?? 0) * 1000,
      observedRemaining: Number(res.headers.get('x-ratelimit-remaining-tokens') ?? NaN),
      observedLimit: Number(res.headers.get('x-ratelimit-limit-tokens') ?? NaN)
    });

    return res.json();
  },
  { weight: estimatedTokens, priority: 10, timeoutMs: 30_000, signal: abortController.signal }
);

// manual lease when you need to hold the slot across several steps
const lease = await coordinator.acquire('search:serp', { priority: -5 });
try {
  await doWork();
} finally {
  lease.release();
}

console.table(coordinator.snapshot());
// [{ key, queued, activeCount, tokens, blockedUntil, avgRecentWaitMs, deniedCount }]

coordinator.shutdown('deploy restart');
```

## Notes

- Single process and in memory. Two instances of your service do not share a bucket. For a real distributed limit you need Redis or a gateway in front.
- It schedules, it does not retry. `applyFeedback` records the 429 and blocks the key, but resending the failed request is your job. `run` releases the lease in a `finally`, it does not re run the task.
- `acquire` throws immediately if `weight` exceeds the reservoir, and throws if the key was never registered. There is no implicit profile creation.
- Set `refillAmount: 0` and the bucket never refills. `tokensReadyIn` returns `Number.MAX_SAFE_INTEGER` in that case, which parks the queue indefinitely rather than erroring.
- Time comes from `Date.now()` and `setTimeout`. Wall clock jumps, suspended laptops and heavy event loop lag all shift the accounting. The lazy refill absorbs long gaps correctly, the spacing rule does not promise millisecond precision.
- Abort listeners added in `acquire` are registered `{ once: true }` but are not removed once a lease is dispatched, so aborting the signal afterwards calls `removeEntry` on an id no longer in the queue. That is a no op, not a leak of behaviour, but the listener does stay attached for the signal's lifetime.
- `snapshot()` refills every key as a side effect before reporting. Cheap, but it is not a pure read.
