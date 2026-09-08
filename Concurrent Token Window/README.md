# Concurrent Token Window

Fire a few hundred goroutines at a token limited LLM API and you get 429s, a half finished batch and a bill for the retries. This is a single file Go rate limiter that puts a token budget, a concurrency ceiling and a circuit breaker behind one submit call.

**Language:** Go | **Lines:** 158 | **Added:** 2026-04-07

## What this solves

Provider rate limits on LLM APIs are not counted in requests. They are counted in tokens per minute, and every call costs a different number of them. A 400 token classification and a 12,000 token document summary both count as one request but they are thirty times apart on the meter. So the usual worker pool, a `sync.WaitGroup` plus a semaphore sized to 50, is measuring the wrong thing. It happily runs 50 concurrent 12,000 token calls and burns a 90,000 token per minute allowance in under two seconds.

What that looks like in production: you kick off a batch of 5,000 documents overnight. The first few hundred go through. Then the provider returns 429 with a retry after header your client ignores, your retry loop fires the same oversized requests again and the failure rate climbs, because the retries are themselves eating the budget you are waiting to get back. The job dies past the halfway mark with no clean resume point. Whoever owns the pipeline finds out next morning, and finance finds out later, because every failed call that got far enough to be billed is still billed.

Two more failure modes ride along. There is usually no ceiling on in flight work, so ten thousand goroutines each holding a TLS connection and a response buffer will exhaust file descriptors or memory before the rate limiter ever complains. And when the provider degrades the default behaviour is to keep hammering, which turns a two minute blip into a twenty minute outage on your side because thousands of doomed calls have to time out first.

## Why I built it

The pieces exist separately. `golang.org/x/time/rate` gives you a weighted token bucket, `golang.org/x/sync/semaphore` gives you a concurrency cap and there are several good standalone circuit breaker packages. What does not exist is the wiring: a queue that says no instead of growing without bound, one set of error values so a caller can tell a full queue apart from an open breaker and a token model where the caller declares the cost of a call before it runs. Everyone calling a token metered API writes that glue, badly, under deadline. So this is the glue, in one file, standard library only. No `go.mod` to reconcile and no dependency to audit.

## When to use it

- Batch summarising or classifying a large corpus where the per minute token allowance is the binding constraint, not the request count.
- A background worker that shares a provider quota with live user traffic and must not starve it.
- Any client where call cost varies by an order of magnitude, so a request per second limiter is either too slow or too generous.
- Pipelines that need to shed load loudly: `ErrQueueFull` back immediately rather than an unbounded in memory queue.
- Services where a provider outage turns into a thundering retry storm and you want a stop valve.
- Anywhere you need a hard cap on concurrent outbound calls so you do not exhaust connections or memory.

## How it works

The surface is small. `NewConcurrentTokenWindow(maxTokens, tokensPerSec int64, maxConcurrent int)` builds the limiter and starts three background goroutines. `Submit(*TokenRequest)` hands in work, `Close()` shuts the queue and `GetAvailableTokens()` reads the budget. A `TokenRequest` carries an `ID`, the `TokensNeeded` for that call, the work as `Fn func(ctx context.Context) error` and a caller allocated `ResultChan chan error` for the outcome.

Budget accounting is a token bucket. `refillTokens` runs on a 100 millisecond `time.Ticker`, takes the write lock, computes `elapsed := time.Since(ctw.windowStart).Seconds()`, multiplies by `tokensPerSec` and clamps the result to `maxTokens` through the local `min` helper. The refill is derived from elapsed wall time rather than accumulated per tick, so the bucket cannot exceed its ceiling. Read the first bullet under Notes before you trust it in a long running process.

Admission is a single consumer loop. `processQueue` ranges over the buffered `queue` channel, sized `maxConcurrent*2`, one request at a time. It checks `circuitOpen` first; if the breaker has tripped the request fails with `ErrCircuitOpen` and never reaches the network. Then it compares `currentTokens` against `req.TokensNeeded`. If the budget is short the request goes back onto the same channel and the loop sleeps 10 milliseconds, a fixed interval poll rather than exponential backoff, which keeps it alive until the refill catches up. If the budget covers it, the tokens are deducted and the request proceeds.

Concurrency is a counting semaphore built from a buffered channel of capacity `maxConcurrent`. The loop sends into `ctw.sem` before spawning the worker goroutine, so a full semaphore blocks admission itself, and the worker returns its slot with `defer func() { <-ctw.sem }()`. That is the backpressure: no more than `maxConcurrent` calls are ever in flight, and while they are all busy nothing new is admitted. The worker calls `r.Fn(context.Background())`, increments `failureCount` with `atomic.AddInt32` on error and pushes the error onto `r.ResultChan`.

Failure handling is a circuit breaker on its own timer. `monitorCircuitBreaker` ticks every 500 milliseconds, reads `failureCount` atomically, and above 5 it sets `circuitOpen` under the write lock, sleeps 2 seconds, zeroes the counter and closes the circuit again. No half open probe, just a fixed cool off. Shared state is guarded by a `sync.RWMutex`, and `Submit` uses a `select` with a `default` branch so it never blocks: a full queue returns `ErrQueueFull` on the spot and a closed limiter returns `ErrClosed`. All three sentinels are `*TokenError` values, so callers can match with `errors.Is` or a type switch.

## Usage

```go
// The file is package main with no func main, so either add one
// or rename the package before importing it.

ctw := NewConcurrentTokenWindow(
    90000, // maxTokens: bucket ceiling, match your provider's per minute allowance
    1500,  // tokensPerSec: refill rate, 90000/60
    8,     // maxConcurrent: hard ceiling on in flight calls
)
defer ctw.Close()

results := make(chan error, len(docs))

for i, doc := range docs {
    // Buffer the result channel so the worker never blocks on the send.
    rc := make(chan error, 1)

    req := &TokenRequest{
        ID:           fmt.Sprintf("doc-%d", i),
        TokensNeeded: estimateTokens(doc), // your estimate: prompt plus expected completion
        ResultChan:   rc,
        Fn: func(ctx context.Context) error {
            return summarise(ctx, doc)
        },
    }

    switch err := ctw.Submit(req); {
    case errors.Is(err, ErrQueueFull):
        // Shed load: retry later, spill to disk or drop.
        continue
    case err != nil:
        // ErrClosed
        return err
    }

    go func() { results <- <-rc }()
}

// Collect. ErrCircuitOpen means the call never left the process.
for range docs {
    if err := <-results; err != nil {
        log.Printf("call failed: %v", err)
    }
}

log.Printf("budget left: %d", ctw.GetAvailableTokens())
```

## Notes

- `windowStart` is set once in the constructor and never advanced, so `refillTokens` computes the budget from process start and overwrites `currentTokens` outright on every tick. Once `elapsed * tokensPerSec` passes `maxTokens` the bucket pins at full and deductions are erased 100 milliseconds later. To enforce the budget past that first window, advance `windowStart` on refill and add the delta instead of assigning it.
- `processQueue` decrements `currentTokens` while holding `mu.RLock`, which is a read lock and not exclusive. Run it under `go test -race` and it will be flagged. The fix is a plain `Lock` around the check and the deduction together.
- The requeue path pushes back into the same channel this one goroutine drains, and the buffer is only `maxConcurrent*2`, so a queue full of requests that all exceed the budget can wedge the loop. Keep `maxTokens` well above your largest `TokensNeeded`.
- `Close()` sets `running` false and closes `queue`, but `Submit` can still be mid send from another goroutine, and a send on a closed channel panics. `refillTokens` and `monitorCircuitBreaker` have no exit path.
- The breaker trips on a cumulative failure count above 5, not an error rate over a window, so a long lived process will eventually trip on scattered unrelated failures. The declared `failureWindow` field is unused.
- `Fn` always receives `context.Background()`. No per request deadline, no cancellation and no retry of the function itself: an error is counted, returned to the caller and forgotten. Put your own timeout inside `Fn`.
