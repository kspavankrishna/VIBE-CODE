# Vector Cache LRU

Embedding vectors are expensive to compute and cheap to re-request, so a naive cache in front of your embedding API grows until the process gets killed. This is a bounded, thread safe LRU cache for `Vec<f32>` embeddings in Rust, with eviction of the least recently used vector once capacity is reached.

**Language:** Rust | **Lines:** 92 | **Added:** 2026-04-08

## What this solves

The failure mode is memory, and it shows up late. A RAG service caches embeddings in a plain `HashMap<String, Vec<f32>>` because that is the obvious thing to write. It works in dev, where you hit the same twenty documents. In production the key space is user queries, chunk hashes or document ids, and it is effectively unbounded. A 1536 dimension float32 embedding is about 6 KB of payload. A million cached vectors is roughly 6 GB before you count key strings and HashMap overhead. On a 4 GB container the OOM killer takes the process, usually under load, usually at the worst time. There is no slow degradation to warn you. The pod just disappears and the restart loop begins.

The other half of the problem is cost. If you drop the cache entirely to stay safe on memory, every repeated query goes back to the embedding provider. Embedding calls are cheap per unit and brutal in aggregate: a chat product that re-embeds the same query text on every retry, every pagination click and every follow up turn is paying for the same vector dozens of times. On self hosted inference you are paying in GPU seconds and queue latency instead of dollars, which shows up as p99 latency your users actually notice.

`VectorCacheLRU` sits between those two outcomes. It keeps a fixed maximum number of entries and throws away the coldest one when a new vector arrives at capacity. Hot vectors stay resident, cold ones leave, memory stops being a function of traffic and becomes a function of a constant you chose. The tradeoff it makes is explicit: you trade cache hit rate for a memory ceiling you can reason about before you deploy.

## Why I built it

Rust's standard library has no LRU. `std::collections::HashMap` is unbounded by design and `BTreeMap` gives you ordering by key, not by recency. The usual answer is to pull in a crate, wrap it in a `Mutex` yourself, then discover that the generic API does not know anything about your workload and you still have to write the memory accounting on top. For a single, well understood job, caching embedding vectors behind a shared handle, that is a lot of dependency surface for a hundred lines of behaviour.

I wanted something I could read in one sitting, drop into an embedding layer as a single file, and reason about completely: one struct, five public methods, no generics to fight, no feature flags and a `memory_usage()` call that tells me in bytes what the float payload is actually costing me right now.

## When to use it

- A RAG pipeline that re-embeds the same query text across retries, follow up turns and pagination, and you want to pay for each vector once.
- A vector search backend where a small set of documents gets queried constantly and the long tail almost never does.
- An embedding service running in a memory capped container, where you need a hard ceiling more than you need a perfect hit rate.
- A batch ingestion job that hits the same chunk hashes repeatedly and would otherwise hammer a paid embeddings API.
- Any place you currently have a bare `HashMap<String, Vec<f32>>` that no code ever removes from.

## How it works

The struct holds three fields. `cache` is an `Arc<Mutex<HashMap<String, (Vec<f32>, usize)>>>` mapping key to the vector plus an insertion counter. `access_order` is an `Arc<Mutex<Vec<String>>>` holding keys ordered oldest first, newest last. `max_entries` is the plain `usize` ceiling. Both shared structures are behind `Arc<Mutex<...>>`, so the cache can be cloned by handle and shared across threads or tasks without any further wrapping.

Recency is tracked with a move to back list rather than the usual intrusive doubly linked list. `get` locks the cache, then the order list, and if the key is present it calls `order.retain(|k| k != key)` to strip the old position and `order.push(key.to_string())` to append it at the hot end, then returns a clone of the vector. `put` does the same `retain` then `push` before inserting, so re-putting an existing key updates it in place instead of creating a duplicate order entry. The list therefore always holds each live key exactly once, with the eviction candidate at index 0.

Eviction runs inside `put` as a `while cache.len() > self.max_entries && !order.is_empty()` loop. Each pass takes `order.first()`, removes it from the front of the list and removes the same key from the map. It is a loop rather than a single removal so the cache converges even if it somehow starts over capacity. Note that eviction is counted in entries, not bytes, so the real memory ceiling is `max_entries` multiplied by your largest expected vector.

The locking order is the same in every method that takes both locks: `cache` first, then `access_order`. That consistency is what keeps this deadlock free. Both guards are held for the whole body of `get` and `put`, which means reads are exclusive, not shared. This is a mutex, not an `RwLock`, so concurrent readers serialise.

`size()` returns the live entry count. `memory_usage()` locks the cache and sums `v.len() * 4` over every stored vector, which is the exact float32 payload in bytes. Two unit tests cover the behaviour that matters: `test_lru_eviction` proves the third insert into a two entry cache drops `"a"`, and `test_access_updates_order` proves that touching `"a"` with `get` before inserting `"c"` makes `"b"` the victim instead.

## Usage

```rust
// single file module, drop VectorCacheLRU.rs into your crate
mod vector_cache_lru;
use vector_cache_lru::VectorCacheLRU;

use std::sync::Arc;
use std::thread;

let cache = Arc::new(VectorCacheLRU::new(10_000)); // max entries, not bytes

// read through pattern
let key = "doc:4f2a:chunk:7";
let vector = match cache.get(key) {
    Some(v) => v,
    None => {
        let v = embed(key);                       // your expensive call
        cache.put(key.to_string(), v.clone());
        v
    }
};

// share the handle across threads
let c2 = Arc::clone(&cache);
thread::spawn(move || {
    c2.put("doc:9911:chunk:0".to_string(), vec![0.1, 0.2, 0.3]);
});

println!("entries: {}", cache.size());
println!("float payload bytes: {}", cache.memory_usage());
```

```bash
cargo test          # runs test_lru_eviction and test_access_updates_order
```

## Notes

- `memory_usage()` counts float payload only, `v.len() * 4`. It excludes key strings, `Vec` and `HashMap` capacity slack and the order list, so true process memory is meaningfully higher than the number it reports.
- Eviction is by entry count, never by bytes. Mixed dimension vectors in one cache make the memory ceiling unpredictable. Size `max_entries` against your largest vector.
- `get` returns a clone of the vector, not a reference or an `Arc`. Every hit allocates and copies. For 1536 dimension embeddings that is a 6 KB memcpy per hit, which is fine for most callers and not free.
- Recency bookkeeping is `Vec::retain` plus `Vec::remove(0)`, both O(n) in cache size. At a few thousand entries this is invisible. At hundreds of thousands under heavy churn it will dominate, and a linked list or an indexed structure is the right answer.
- Every lock uses `.unwrap()`. If a thread panics while holding either mutex the lock is poisoned and all later calls panic too. There is no recovery path and no `try_lock`.
- The `usize` stored alongside each vector is an insertion counter written at `put` time and never read. It is ignored in both `get` and `memory_usage`.
- Both mutexes are held for the whole of `get` and `put`, so this serialises all access. It is thread safe, not concurrent. There is also no TTL, no hit or miss metrics, no persistence and no async API.
- `VectorCacheLRU::new(0)` is legal and gives you a cache that evicts every entry immediately, so every `get` misses.
