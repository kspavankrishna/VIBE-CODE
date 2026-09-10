# Paged KV Cache Allocator

Two requests hit your LLM server with the same 2000 token system prompt and you pay for that prompt's K/V attention tensors twice. This is a from scratch C++ block allocator that gives them the same physical GPU blocks instead, with reference counting, copy on write forking and LRU eviction.

**Language:** C++ | **Lines:** 459 | **Added:** 2026-09-05

## What this solves

Every self hosted LLM inference stack runs into the same memory management problem the moment more than one request is in flight: where do the K/V attention tensors for each token live in GPU memory, and how do you stop that memory being wasted the second two requests share any text at all. Naive serving allocates one contiguous buffer per sequence, sized to the maximum context length. A request that generates 200 tokens against a 32k reservation wastes 99 percent of what it holds. Fragmentation on top of that means you run out of VRAM while a third of the pool is technically unused, and the scheduler starts rejecting requests it could have served.

Prefix duplication is the second half of the failure. Agent runtimes fan out ten parallel tool calls that all carry the same system prompt and tool schema. RAG pipelines inject the same retrieved preamble into every request in a batch. Beam search forks four ways from a shared history. Without sharing, each of those pays full memory cost and full prefill compute for text the server has already processed. You notice it as throughput that collapses under concurrency, time to first token that climbs with batch size, and an out of memory kill at 60 percent utilisation.

What breaks in production is specific. Your max batch size ends up set by the worst case sequence rather than the average one, so you provision GPUs for a load you rarely see. Latency p99 spikes when the scheduler preempts and re evicts sequences. And if you try to hand roll sharing without proper reference counting you get the quiet version of the bug: a block reused while another sequence still points at it, garbage K/V values, and a model that produces subtly wrong tokens with no crash and no log line. That one costs days to find.

This file is the allocator layer that fixes all of it. Fixed size blocks, so there is no fragmentation and no over reservation. Content addressed blocks, so identical prefixes converge on one copy. Reference counts and copy on write, so sharing is safe. LRU retention of unreferenced blocks, so a prefix that comes back thirty seconds later still hits.

## Why I built it

If you have run vLLM or TensorRT-LLM you have already benefited from this idea, PagedAttention plus automatic prefix caching, but you almost certainly never saw how the block manager works inside. The write ups either wave at "radix tree" and move on, or they paste real production code with scheduling, quantisation and distributed placement tangled into the same file. Neither teaches you the invariants.

I wanted the minimal version that is still correct. Real reference counting, real copy on write, real eviction, no shortcut that would silently corrupt a running sequence. It is one dependency free C++17 file you can read start to finish in a sitting, adapt into your own serving engine, or use to teach the concept.

## When to use it

- You are building a C++ inference server and need a block manager before you write the attention kernel that consumes the block table
- An agent runtime fans out many parallel model calls that share a system prompt or a tool schema, and you want to pay for that prefix once
- Beam search or parallel sampling forks sequences at a branch point and you need the shared history to cost nothing
- A batched pipeline sees the same few shot template or RAG preamble across thousands of requests a minute
- You are benchmarking prefix caching strategies and want a readable baseline to modify
- You are teaching or studying LLM serving internals and need something small enough to trace by hand

## How it works

The pool is `num_blocks` fixed size blocks, each holding `block_size_tokens` worth of K/V slots. Each `Block` carries only `ref_count`, `has_hash` and `content_hash`. The tensor storage is deliberately out of scope: callers map a `BlockId` to their own device buffer.

Sharing is driven by a chained content hash, which is what makes this a radix or prefix hash rather than a plain per block hash. `detail::HashTokens` runs FNV-1a over the token ids of a block. `detail::ChainHash` then mixes that into the hash of every block before it using a boost hash_combine style step with the 0x9e3779b97f4a7c15 constant. Two sequences therefore collide on block N only if every block before it, in order, was identical. That is the property that makes reuse safe rather than "similar enough".

`AppendTokens` walks the incoming tokens one at a time. When the sequence has no open block it calls `AllocateBlock` and sets the new block's ref count to one. Tokens accumulate in `Sequence::partial_tokens` until the count reaches `block_size_`, at which point `InternLastBlock` runs exactly once for that block. It computes the chained hash and looks it up in `hash_to_block_`. On a hit it bumps the existing shared block through `TouchForReuse`, swaps it into the sequence's block table, drops the private block with `DecRef` and increments `prefix_cache_hits_`. On a miss the freshly filled private block is stamped with its hash and published into the index, first writer wins. A block is only ever shareable after it is full and therefore immutable.

`ForkSequence` is copy on write with no data movement. Every completed block in the parent is shared by a refcount bump. The trailing partial block, if there is one, is physically duplicated, because sharing a block both sides are still writing into would let parent and child overwrite each other's in progress tokens. If the pool is exhausted mid fork it rolls back every ref it already took and throws `std::runtime_error`, so a failed fork leaks nothing.

Freeing does not immediately return memory. `DecRef` sends a block that hits zero references into the `lru_` list if it has a content hash, and only onto `free_list_` if it never earned one. `AllocateBlock` prefers the free list, and reclaims the front of the LRU list only when the pool is genuinely out of fresh blocks. A reclaimed block's identity is scrubbed from `hash_to_block_` before it is handed back, so a stale prefix can never be hit after its backing block was repurposed. `lru_pos_` maps block id to its `std::list` iterator, which makes the pull out of the middle of the LRU on reuse O(1). Double frees throw `std::logic_error` rather than corrupting counts.

Every public method takes one `std::mutex`. That is a deliberate simplification. Correctness of the refcounting and hash chaining invariants matters more than lock granularity at this scale. `GetStats` returns an `AllocatorStats` with free, cached, active block counts plus hits, misses, evictions and a `HitRate()`.

## Usage

```bash
g++ -std=c++17 -O2 -pthread PagedKvCacheAllocator.cpp -o kvcache_demo
./kvcache_demo
```

The built in `main` runs two scenarios: prefix sharing across three requests plus a beam search fork on a 64 block pool, then forced eviction on a deliberately tiny 4 block pool. Core API:

```cpp
using namespace kvcache;

PagedKvCacheAllocator alloc(/*num_blocks=*/64, /*block_size_tokens=*/8);

SeqId a = alloc.CreateSequence();
alloc.AppendTokens(a, system_prompt);   // returns false if the pool is exhausted
alloc.AppendTokens(a, user_tokens);

SeqId beam2 = alloc.ForkSequence(a);    // throws std::runtime_error if out of blocks

std::vector<BlockId> table = alloc.GetBlockTable(a);  // feed this to your attention kernel
size_t n = alloc.NumCachedTokens(a);

AllocatorStats s = alloc.GetStats();
alloc.FreeSequence(a);
```

## Notes

- This manages block identity and lifetime only. It never touches device memory. Forking a partial block allocates a fresh `BlockId` but copying the actual K/V bytes into it is the caller's job.
- Hash equality is trusted. There is no token by token verification on a prefix hit, so a 64 bit collision would silently share the wrong block. Add a stored token vector and a compare if your threat model needs it.
- `AppendTokens` returning false leaves the sequence consistent but only partly appended. Free other sequences and retry.
- `GetBlockTable`, `NumCachedTokens` and `AppendTokens` throw `std::out_of_range` on an unknown `SeqId`. `FreeSequence` silently ignores one.
- One global mutex serialises all calls. Fine for a few thousand blocks and microsecond scale calls. Shard by block pool if profiling shows contention.
- Eviction is strict LRU over unreferenced hashed blocks only. There is no priority, no pinning of hot system prompts and no preemption of live sequences.
- The class is non copyable. Demo `main` always exits 0 and does not assert, it prints stats for you to read.
