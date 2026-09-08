# Inference Backpressure Scheduler

One slow SSE client on an LLM gateway can quietly buffer megabytes of generated tokens inside your process while every other stream stalls behind it. This is a header only C++ scheduler that bounds that buffer per stream and globally, ages out stale chunks and hands the write budget around with deficit round robin.

**Language:** C++ | **Lines:** 715 | **Added:** 2026-04-14

## What this solves

Token generation is fast. Downstream delivery is not. A gateway in front of vLLM, TGI or llama.cpp takes tokens at whatever rate the model produces them and pushes them over SSE or WebSocket to clients on hotel wifi, mobile networks and backgrounded browser tabs. When a client stops reading its socket the kernel send buffer fills, the write returns EWOULDBLOCK, and the obvious fix is to append the pending tokens to a per connection deque and retry next loop. Nothing in that fix bounds the deque. A 40 token per second stream that nobody drains for two minutes is a few hundred kilobytes of dead weight. A thousand of them is the RSS graph going vertical and the OOM killer taking out a process that was happily serving 999 healthy streams.

The second failure is unfairness, and it shows up long before memory runs out. A writer loop that walks the connection list in order and flushes everything it can gives the first connection all the socket budget. Streams at the tail get whatever is left. Nobody sees this in a load test with uniform clients. In production it is p99 time between tokens degrading for reasons that have nothing to do with the model, and the traces point at the network because that is where the time went.

The third is stale data. Once a consumer is thirty seconds behind, the tokens in its queue are worthless: the user closed the tab or the request already timed out at the load balancer. Delivering them still costs bytes on the wire and still delays live streams behind them. The bill for all three is gateway restarts under memory pressure, truncated responses when a process dies mid stream, latency regressions blamed on inference, and someone on call reading a heap profile at 3am to find that all the memory is `std::string` payloads in a connection map.

## Why I built it

Rate limiters solve the wrong half. A token bucket at the edge caps how fast requests arrive and does nothing about memory once a request is accepted and generation starts. HTTP/2 and QUIC flow control applies real backpressure but stops at the connection boundary and gives you no policy hook: you cannot say drop the oldest chunks for this lagging stream while guaranteeing the terminal done marker still gets through, and you cannot say tenant A's stream is worth three times tenant B's.

So every gateway grows its own buffering logic inside the write handler. It is rarely tested, it is only exercised on the worst day of the quarter and it usually has no global ceiling at all. I wanted one object that owns the whole policy, takes an injectable clock so the age and idle paths are testable without sleeping, and reports enough counters to put a dashboard on it.

## When to use it

- SSE fanout in front of a local inference server where clients read at wildly different speeds
- A WebSocket gateway multiplexing many completions onto a shared writer thread
- An edge worker or sidecar with a hard memory ceiling that must shed load instead of dying
- Multi tenant serving where a paying tenant should get a larger share of the write budget
- Any stream where the final done marker matters more than the middle chunks
- Debugging a gateway whose memory grows with connection count and never comes back down

## How it works

State lives in a `std::unordered_map<std::string, StreamState>` keyed by stream id, guarded by one `std::mutex`. Each `StreamState` holds a `std::deque<Chunk>`, its own `StreamPolicy`, byte and chunk counters, a deficit counter and a `lastActivity` timestamp. `totalQueuedBytes_` and `totalQueuedChunks_` track the whole scheduler. A parallel `std::vector<std::string> ring_` holds dispatch order and `dispatchCursor_` indexes into it. Chunks get monotonic ids from `nextSequence_`, and the clock is `steady_clock` so nothing breaks when wall time jumps. `normalizePolicy` and `normalizeOptions` clamp caller values up to floors: at least 1024 buffered bytes, 4 chunks, a 128 byte target chunk, weight 1 and a 50ms latency ceiling per stream, plus 1MB of global budget and a 1 second idle TTL.

Ingress goes through `enqueue`. It calls `ensureStreamLocked`, which creates the stream on first sight with a default policy if you never called `registerStream`, or returns null once `maxStreams` is hit. Then `tryCoalesceLocked`: if the tail chunk is not terminal and the combined size still fits inside `targetChunkBytes`, the payload is appended to that tail instead of pushed as a new chunk, `lastSequence` is bumped and the caller gets `Admission::Coalesced`. That is what keeps single token writes from becoming 256 deque entries and 256 socket writes. Two cases are handled first: an empty non terminal payload is accepted and discarded, and a terminal empty payload on an empty queue becomes a zero byte terminal marker so the done event survives.

Enforcement runs in two layers. `enforceStreamLimitLocked` fires when a stream exceeds `maxBufferedBytes` or `maxBufferedChunks` and switches on its `OverflowPolicy`. `RejectIncoming` pops the chunk just pushed, with a preflight through `wouldExceedStreamLimitAfterAppendLocked` so the append is usually avoided. `EvictStream` flushes the queue. `DropOldest` loops on `dropOldestChunkLocked`, which respects `preserveTerminalChunk`: if the head is terminal it uses `std::find_if` to drop the first non terminal chunk instead, and refuses to drop anything when a lone terminal chunk is all that remains. The second layer is `enforceGlobalBudgetLocked`, running while `totalQueuedBytes_` exceeds `globalMaxBufferedBytes`. `selectVictimLocked` scores every non empty stream as `backlogRatio * 2.0 + ageRatio`, backlog being queued bytes over that stream's own limit and age being head chunk age over its latency ceiling. The weighting deliberately punishes a stream that is both fat relative to its own budget and old, not just the largest one. A `DropOldest` victim with more than one chunk gets shaved by a single chunk and the loop retries, otherwise the queue is flushed. If the victim is the stream that just enqueued, `enqueue` returns `Admission::Rejected`.

Egress is `drain(maxBytes)`, and it is deficit round robin, the same scheme network schedulers use. Each visited stream earns a quantum of `max(burstBytes, targetChunkBytes) * weight` into `deficitBytes`, capped by `saturatingAdd` at eight times `burstBytes` so an idle stream cannot bank unlimited credit. A head chunk ships only when it fits both the accumulated deficit and the remaining byte budget, otherwise the cursor advances and a `stalls` counter increments. When `stalls` reaches the ring size the loop exits, which is what stops it spinning when nothing is dispatchable. Two escape hatches: the first record in a batch may overrun `maxBytes` so an oversized chunk never deadlocks the loop, and a terminal chunk raises the deficit to at least its own size so a done marker is never starved. Each `DispatchRecord` carries its sequence range and measured `queueDelay`, which is the number worth putting on a histogram.

Reaping runs on both `enqueue` and `drain`. `trimExpiredStreamsLocked` flushes any queue whose head chunk is older than `maxQueueLatency`, tagged `EvictionReason::MaxQueueLatency`. `trimIdleStreamsLocked` erases streams empty for longer than `idleTtl`, which keeps the map from growing across a long uptime. `cancelStream` drops a stream immediately on disconnect, and `snapshot` returns per stream byte, chunk, drop and eviction counters plus the age of the oldest queued chunk, sorted by queued bytes descending so the noisiest offender is row one.

## Usage

The file is a single translation unit in `namespace vibe` with no `main`, so use it as a header.

```cpp
// g++ -std=c++17 -O2 -pthread your_gateway.cpp -o gateway
#include "InferenceBackpressureScheduler.cpp"   // or rename to .hpp

vibe::SchedulerOptions opts;
opts.globalMaxBufferedBytes = 64 * 1024 * 1024;
opts.maxStreams             = 8192;
opts.maxBatchRecords        = 64;
opts.idleTtl                = vibe::Milliseconds{15000};

vibe::InferenceBackpressureScheduler sched(opts);

// Optional. Streams are auto created on first enqueue with a default policy.
vibe::StreamPolicy paid;
paid.maxBufferedBytes  = 512 * 1024;
paid.targetChunkBytes  = 2048;   // coalesce small token writes up to this
paid.burstBytes        = 16 * 1024;
paid.weight            = 4;      // 4x the DRR quantum of a weight 1 stream
paid.maxQueueLatency   = vibe::Milliseconds{2000};
paid.overflowPolicy    = vibe::OverflowPolicy::DropOldest;
paid.preserveTerminalChunk = true;
sched.registerStream("req-1a2b", paid);

// Producer side: one call per token or per model chunk.
auto r = sched.enqueue("req-1a2b", std::string(token));
if (r.admission == vibe::Admission::Rejected) { /* stop generating */ }
if (r.droppedBytes > 0)                       { /* count the loss */ }

sched.enqueue("req-1a2b", "", vibe::EnqueueOptions{ .terminal = true });

// Writer side: call from your socket loop with the bytes you can actually write.
vibe::DispatchBatch batch = sched.drain(256 * 1024);
for (auto& rec : batch.records) {
    writeToSocket(rec.streamId, rec.payload);       // payload was moved out
    if (rec.terminal) closeStream(rec.streamId);
    observeLatency(rec.queueDelay.count());
}

sched.cancelStream("req-1a2b");                     // client disconnected

vibe::SchedulerSnapshot snap = sched.snapshot();    // metrics, worst stream first
```

## Notes

- No `main`, no tests, no networking and no logging. It is a policy engine. You own the sockets, the retry loop and the metrics export.
- Thread safe through one mutex covering everything, so producers and the drain loop serialize against each other. Shard by stream id across several instances if that becomes the bottleneck.
- Several paths are linear in stream count and run on every `enqueue` and `drain`: `trimExpiredStreamsLocked`, `trimIdleStreamsLocked`, `selectVictimLocked` and the `std::find` inside `removeRingIdLocked`. Fine at the default 4096 stream ceiling. Not free at ten times that.
- `preserveTerminalChunk` only protects against `dropOldestChunkLocked`. A `maxQueueLatency` expiry, an `EvictStream` overflow or a global budget eviction flushes the whole queue including the terminal marker, so treat the terminal flag on a `DispatchRecord` as the only reliable end of stream signal and handle eviction separately.
- `drain` and `snapshot` accept an injectable `now`, which is what makes the age and idle logic testable. `enqueue` always reads `Clock::now()` itself.
- `registerStream` returns `false` for three different situations: empty id, stream limit reached and stream already exists. In the last case it still updates the policy. Check `EnqueueResult.createdStream` when you need to tell them apart.
- Everything is in memory. A restart loses every queued chunk, and payloads are `std::string` copies until `drain` moves them out. Requires C++17 for `std::scoped_lock`, `std::string_view` and structured bindings.
