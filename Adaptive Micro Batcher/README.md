# Adaptive Micro Batcher

Firing hundreds of tiny async calls one at a time is what makes an embeddings bill, a vector upsert loop or a moderation fan-out slow and expensive. This is a single file Dart micro batcher that groups those calls into bounded batches, flushes them on size, weight or latency and recursively splits a failed batch so one poison item cannot sink the rest.

**Language:** Dart | **Lines:** 866 | **Added:** 2026-04-24

## What this solves

The usual pattern is a list and a timer: append to a `List`, set a `Timer` for 20 ms, flush whatever is there, start again. That works on your laptop with ten requests. It breaks the moment traffic is real.

The first failure is memory. A naive buffer has no ceiling, so when the provider slows to a crawl the queue grows without limit and the process gets OOM killed while every caller still holds a `Future` that will never complete. The second is staleness. A request that sat in a buffer for four seconds is worthless: the client on the other end already timed out, the SLA is blown. You still pay the provider to compute a result that goes in the bin.

The third is the ugly one. Most batch APIs are all or nothing at the transport level. One malformed payload, one document over the token limit, one item that trips a validation rule and the whole call throws. With a naive flush loop all 64 callers get the same error, and 63 of them were fine. Without isolation you either fail everyone or you hand write bisect logic around every downstream call. This file gives you a bounded FIFO queue with item and weight ceilings, a configurable overflow policy, per request queue deadlines, a concurrency cap on in flight batches and automatic split retry. One class, nothing outside `dart:async` and `dart:collection`.

## Why I built it

Every Dart service I have written eventually grows a hand rolled batcher, and each one was worse than the last. The ecosystem has stream transformers and debounce helpers, but nothing that treats the queue itself as a resource with limits. Buffer style helpers ignore payload weight, do not cap concurrency, never expire stale entries and have no answer for a partial provider failure.

Batching is the easy half. The hard half is deciding when to flush, keeping the queue from becoming a memory liability and salvaging good work after a bad batch. I wanted one file I could drop into a shelf server, a Cloud Run worker or a CLI pipeline and trust under load.

## When to use it

- You call an embeddings endpoint per document and want 64 documents in one HTTP call, without an unbounded queue when the provider degrades.
- Your vector database upsert path takes writes one at a time off a stream, and single row writes cost an order of magnitude more than batched.
- One oversize payload keeps failing an entire provider batch and you need the other items to still complete.
- A request that waited 200 ms is useless because the caller gave up, and you want it dropped with a clear error rather than executed.
- You ship metrics from a Dart worker and want at most two concurrent flushes so you never stampede the collector.
- Under a spike you would rather shed the oldest queued work than reject new arrivals, or the exact opposite, and want that to be a config flag.

## How it works

`AdaptiveMicroBatcher<TInput, TResult>` wraps a `Queue<_PendingRequest>` and hands each caller a `Completer<TResult>` from `submit`. The queue is strict FIFO. Time comes from a monotonic `Stopwatch` through `_elapsedMicros`, not wall clock, so a system clock jump cannot make a batch fire early or hang. The injectable `clock` only feeds the `DateTime` stamps on `BatchRequest` and `BatchExecutionContext`, so tests stay deterministic without touching dispatch timing.

Admission control runs before anything is queued. `submit` resolves a weight from the explicit `weight` argument or the `estimateWeight` callback, which defaults to 1 per item, then throws `BatchWeightExceededException` if one request could never fit inside `maxBatchWeight` or `maxQueueWeight`. `_makeRoomFor` enforces the queue ceilings. Under `QueueOverflowStrategy.rejectNewest` the new caller gets a `BatchQueueFullException` carrying the counters that tripped. Under `dropOldest` the head is evicted and failed instead, in a loop until there is room. Either way memory is bounded, and the choice of who loses is yours.

The dispatch loop is `_pump`, guarded against reentrancy with `_isPumping` and `_pumpRequested`. A nested call is deferred to a `scheduleMicrotask` rather than recursing, so a synchronous completion inside a runner cannot blow the stack. Each pump runs `_pruneExpired`, failing anything past its `maxQueueDelay` with `BatchRequestExpiredException`, then asks `_selectBatch` for work while `_inFlightBatches < maxConcurrentBatches`. `_selectBatch` walks the queue prefix accumulating count and weight, then returns a `BatchDispatchReason` in priority order: `closing` during a draining shutdown, `size` at `maxBatchItems`, `weight` when the next item would overflow `maxBatchWeight` and `latency` once the head item passes enqueue time plus `maxBatchLatency`. That is the adaptive part. A busy queue flushes on size or weight almost immediately, a quiet one waits out the latency budget. Otherwise `_scheduleNextWakeup` sets exactly one `Timer` for the earliest of the head item's deadline and the soonest expiry in the queue.

Failure isolation lives in `_executeInvocation`. It calls the runner through `_invoke`, which builds the `BatchRequest` list and enforces that the returned `List<BatchItemResult>` matches the input count, raising `BatchProtocolException` otherwise. Per item results complete independently, so a runner can mark item 37 as `BatchItemResult.failure` and everyone else still succeeds. If the whole invocation throws and `failureIsolation` is `splitInHalf`, the batch is bisected and each half re-executed recursively with a child `BatchExecutionContext` carrying the same `rootBatchId`, a `splitRetry` reason and an incremented `splitDepth`. Recursion stops at a single item, failed alone. Splits run inside the same in flight slot, so bisection never breaches `maxConcurrentBatches`. Worst case for a batch of n where every item is bad is 2n-1 invocations, so use `failureIsolation.none` if your downstream cannot absorb that.

Shutdown is explicit. `close(drain: true)` forces every remaining selection to dispatch under the `closing` reason and completes `whenClosed` once the queue is empty and nothing is in flight. `close(drain: false)` fails the backlog immediately with `BatchClosedException` while letting in flight batches finish. `snapshot()` returns a `MicroBatcherSnapshot` with submitted, succeeded, failed, expired, dropped and rejected counters, split invocation counts, peak queue depth and weight, plus `averageQueueWaitMilliseconds` and `averageBatchInvocationMilliseconds`.

## Usage

```dart
import 'AdaptiveMicroBatcher.dart';

final batcher = AdaptiveMicroBatcher<String, List<double>>(
  // Return one result per request, in the same order.
  runBatch: (items, context) async {
    final vectors = await embeddings.embed([for (final r in items) r.input]);
    return [for (final v in vectors) BatchItemResult<List<double>>.success(v)];
  },
  estimateWeight: (text) => text.length,
  maxBatchItems: 96,
  maxBatchWeight: 96 * 1024,
  maxBatchLatency: const Duration(milliseconds: 15),
  maxConcurrentBatches: 4,
  maxQueueItems: 8192,
  maxQueueWeight: 16 * 1024 * 1024,
  queueOverflowStrategy: QueueOverflowStrategy.dropOldest,
  failureIsolation: BatchFailureIsolation.splitInHalf,
);

// Each caller awaits its own result. Batching is invisible to them.
final vector = await batcher.submit(
  'the quick brown fox',
  maxQueueDelay: const Duration(milliseconds: 200),
  traceId: 'req-42',
);

// Inside runBatch, mark one item bad without failing the whole batch:
// BatchItemResult<List<double>>.failure(StateError('payload too large'));

final stats = batcher.snapshot();
print('${stats.totalSucceeded}/${stats.totalSubmitted} ok, '
      'avg wait ${stats.averageQueueWaitMilliseconds}ms, '
      'dropped ${stats.totalDropped}, expired ${stats.totalExpired}');

await batcher.close(drain: true);
await batcher.whenClosed;
```

Errors surface on each caller's own `Future`, so a `try`/`catch` around the await gives you `BatchRequestExpiredException`, `BatchQueueFullException`, `BatchClosedException` or whatever the runner threw.

## Notes

- The runner contract is strict. Return exactly one `BatchItemResult` per request, in the same order. A mismatch raises `BatchProtocolException`, and since that throws inside the invocation it triggers a split retry when isolation is on, so a buggy runner gets bisected before it finally fails.
- Adaptive means the flush trigger adapts to traffic: size, weight, latency or shutdown. There is no feedback loop that tunes `maxBatchItems` from observed provider latency. You pick the ceilings.
- No retries, no backoff, no deduplication, no priority lanes and no per caller fairness. A batch executes once, split retry only reruns the halves of a batch that threw, and ordering is FIFO by design.
- Single isolate only. No locks, no atomics, no cross isolate coordination. Every `submit` must come from the isolate that owns the batcher, and `submit` after `close` throws `BatchClosedException` synchronously rather than returning a failed `Future`.
- Expired and dropped requests count apart from failures, so `totalCompleted` excludes them, and queue wait accumulates only for items that reach dispatch.
- `_scheduleNextWakeup` and `_pruneExpired` both scan the whole queue, O(n) per pump. Fine at the default 4096 item ceiling, worth measuring much higher.
