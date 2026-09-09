# Inference Backpressure Scheduler

One slow SSE client on an LLM gateway can quietly accumulate hundreds of megabytes of unsent tokens and take every other stream down with it. This is a C++17 per stream backpressure scheduler with deficit round robin dispatch, bounded queues, chunk coalescing and eviction of lagging consumers.

**Language:** C++ | **Lines:** 715 | **Added:** 2026-04-14

## What this solves

LLM streaming backpressure in C++ gateways where one slow SSE or WebSocket client can quietly build a huge in-memory queue and hurt every other stream. The generation side is usually fine. vLLM, llama.cpp or TGI produce tokens at a predictable rate. The weak spot is downstream delivery. A phone on a bad connection, a throttled background tab or a corporate proxy that stops reading accepts bytes far slower than the model emits them, and the socket buffer fills. What happens next depends entirely on what the gateway does with the tokens it cannot write yet.

Most hand rolled proxies append to a `std::string` or a `std::deque` per connection and hope the client catches up. It usually does. Then one day it does not. A thousand concurrent streams each holding a few hundred kilobytes of undelivered tokens is a few hundred megabytes of resident memory nobody budgeted for, and the failure is not graceful: the process crosses its cgroup limit and the OOM killer takes out the whole gateway, including the 990 streams that were behaving perfectly. Nobody notices in staging because staging clients read fast.

The second failure costs money before it costs uptime. Tokens that sat in a queue for eight seconds are worthless. The user closed the tab, the request timed out at the load balancer or the client gave up and retried. The gateway keeps writing them to a socket nobody is reading, so latency for the streams still being watched goes up while the writer thread flushes a backlog with no reader.

The third is fairness. A writer loop that walks connections in order and writes as much as each will take gives the most bandwidth to whichever stream has the biggest backlog. That is exactly backwards. This scheduler bounds buffering per stream and globally, ages out stale chunks, coalesces small token writes and picks a victim deliberately when the global budget is blown.

## Why I built it

Every serving stack solves this on the generation side and stops there. Continuous batching, paged attention and admission control all govern how tokens get produced. Almost nothing in the open C++ ecosystem governs how they get delivered. Boost.Asio gives you a socket and a completion handler. It has no opinion about what to do when your write queue reaches 40 MB. Envoy has watermark buffers, but you cannot embed Envoy in a model serving sidecar, and its watermarks do not understand that a terminal token is worth more than a mid stream one.

So the logic gets reimplemented badly in every gateway: an ad hoc size check here, a disconnect there, no fairness, no metrics and no notion of a chunk still worth sending versus one that expired two seconds ago. This file is that logic written once, with a global memory ceiling, per stream policy, deterministic victim selection and a snapshot you can scrape into your metrics pipeline.

## When to use it

- An SSE or WebSocket fanout service in front of vLLM, TGI or llama.cpp where mobile clients read slower than the model generates
- A C++ model serving sidecar that has to survive a thousand concurrent streams inside a fixed memory cgroup
- An edge worker or realtime AI gateway where a stalled TCP connection grows an unbounded write buffer
- A proxy where a burst of tiny token writes turns into a burst of tiny syscalls and you want them merged
- Post incident work after an OOM kill that took out healthy streams alongside the slow client that caused it

## How it works

The public surface is the `vibe::InferenceBackpressureScheduler` class. Producers call `enqueue(streamId, payload, {terminal})` from the token generation path. The writer thread calls `drain(maxBytes)` and gets back a `DispatchBatch` of `DispatchRecord` values, each one a stream id, a payload, a terminal flag, a sequence range and the measured `queueDelay`. Everything is guarded by one `std::mutex` and every internal helper carries a `Locked` suffix so the locking contract is visible at the call site.

Dispatch is deficit round robin. `ring_` holds stream ids and `dispatchCursor_` walks it. Each visit credits the stream a quantum of `max(burstBytes, targetChunkBytes) * weight` into `deficitBytes`, capped at `burstBytes * 8` by `saturatingAdd` so an idle stream cannot bank credit forever and then dominate a batch. The head chunk goes out only if its size fits inside the accumulated deficit, which is what makes DRR fair by bytes rather than by messages: a stream sending 4 KB chunks does not get eight times the bandwidth of one sending 512 B chunks just because both get one turn. A `stalls` counter bounded by `ring_.size()` guarantees the loop terminates when every stream is empty or short of deficit. Two exceptions are deliberate. A terminal chunk bumps `deficitBytes` up to its own size so a stream ending never gets stuck behind its own accounting, and the first record in an empty batch may overrun `maxBytes` so an oversized chunk cannot block the head of the line forever.

Admission runs through gates in `enqueue`. Payloads larger than the stream's `maxBufferedBytes` are rejected outright. Empty non terminal payloads are ignored. An empty terminal payload on an empty queue becomes a bare terminal marker chunk. Otherwise `tryCoalesceLocked` appends the bytes onto the tail chunk when the tail is not terminal and the merged size stays under `targetChunkBytes`, extending `lastSequence` so the sequence range still covers everything merged. That is the syscall amortiser: streams that emit one token at a time dispatch one right sized write instead of forty small ones.

When a queue goes over its limit, `enforceStreamLimitLocked` applies the stream's `OverflowPolicy`. `RejectIncoming` pops the chunk just appended and reports rejection. `DropOldest` calls `dropOldestChunkLocked` until the stream is back under budget, and when `preserveTerminalChunk` is set it skips a terminal chunk at the front and drops the first non terminal chunk instead, so the end of stream signal survives a purge of the middle. `EvictStream` clears the whole queue. Every drop increments `droppedBytes` and `droppedChunks` and stamps a `lastEvictionReason`.

Above that sits the global ceiling. `enforceGlobalBudgetLocked` runs while `totalQueuedBytes_` exceeds `globalMaxBufferedBytes` and picks a victim via `selectVictimLocked`, which scores every non empty stream as `backlogRatio * 2.0 + ageRatio`: how full it is against its own limit, weighted double, plus how old its head chunk is against its own `maxQueueLatency`. That targets the stream both hogging memory and sitting on stale data. A `DropOldest` victim with more than one chunk loses only its oldest chunk and the loop reconsiders, so pressure sheds gradually. Two janitors run at the top of `enqueue` and `drain`: `trimExpiredStreamsLocked` clears any queue whose head has aged past `maxQueueLatency`, and `trimIdleStreamsLocked` erases streams empty for longer than `idleTtl`, which keeps the map from leaking one entry per finished request.

## Usage

```cpp
#include "InferenceBackpressureScheduler.cpp"  // single translation unit, class lives in namespace vibe

vibe::SchedulerOptions opts;
opts.globalMaxBufferedBytes = 64ull * 1024 * 1024;   // hard ceiling across all streams
opts.maxStreams             = 8192;
opts.maxBatchRecords        = 256;
opts.idleTtl                = std::chrono::milliseconds{15000};

vibe::InferenceBackpressureScheduler sched(opts);

// Optional. enqueue() auto creates a stream with default policy if you skip this.
vibe::StreamPolicy policy;
policy.maxBufferedBytes  = 512 * 1024;
policy.maxBufferedChunks = 512;
policy.burstBytes        = 16 * 1024;
policy.targetChunkBytes  = 2048;                     // coalescing target
policy.weight            = 2;                        // DRR share relative to weight 1 streams
policy.maxQueueLatency   = std::chrono::milliseconds{2000};
policy.overflowPolicy    = vibe::OverflowPolicy::DropOldest;
policy.preserveTerminalChunk = true;
sched.registerStream("req-8fa1", policy);

// Producer side, from the token loop.
auto r = sched.enqueue("req-8fa1", "Hello");
if (r.admission == vibe::Admission::Rejected) { /* shed load */ }
vibe::EnqueueOptions endOfStream;
endOfStream.terminal = true;
sched.enqueue("req-8fa1", "", endOfStream);          // queues a bare terminal marker

// Writer side. Ask for at most one socket write worth of bytes.
vibe::DispatchBatch batch = sched.drain(64 * 1024);
for (auto& rec : batch.records) {
    writeToClient(rec.streamId, rec.payload, rec.terminal);   // your transport
    observeLatency(rec.queueDelay);
}

// Client disconnected.
sched.cancelStream("req-8fa1");

// Metrics.
vibe::SchedulerSnapshot snap = sched.snapshot();   // streams sorted by queuedBytes, worst first
```

## Notes

- No `main`, no CLI and no tests. It is a class in `namespace vibe` inside a `.cpp`. Rename it to a header or extract the class to link it into more than one translation unit. C++17 and the standard library only, no external dependencies.
- It does no I/O. It never touches a socket, never spawns a thread and never sleeps. You own the writer loop and decide how often to call `drain`.
- One global `std::mutex` covers all state. Fine for thousands of streams and short critical sections. Not the right shape for a shard per core design under very high call rates.
- `trimExpiredStreamsLocked` drops the entire queue of a latency expired stream, including a terminal chunk. `preserveTerminalChunk` protects against `DropOldest` pressure, not against age expiry.
- `registerStream` returns `false` when the stream already exists, but still overwrites the policy. Treat the return value as "created", not "succeeded". Use `updateStreamPolicy` when you only mean to change policy. `normalizePolicy` and `normalizeOptions` silently raise values that are too small rather than erroring.
- Coalescing mutates the tail chunk in place, so one `enqueue` does not map to one dispatch record. `ring_` removal is a linear scan, so stream teardown is O(n) in live streams.
