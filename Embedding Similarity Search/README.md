# Embedding Similarity Search

A dependency free Mojo kernel that finds the top k nearest embeddings to a query vector by cosine similarity, using SIMD dot products and a bounded min heap so it never sorts more than it has to.

**Language:** Mojo | **Lines:** 324 | **Added:** 2026-09-16

## What this solves

Every RAG pipeline, recommendation system and semantic search feature eventually needs the same primitive: given a query embedding and a pile of stored embeddings, find the handful that are closest by cosine similarity. Most teams reach for a vector database or a library like FAISS the moment this need shows up, and that is often the right call once you are past a few million vectors or need filtering and persistence. But a huge number of real systems never get there. They have a few thousand to a few hundred thousand embeddings sitting in a service's memory, refreshed on a schedule or built once per request, and pulling in a whole vector database just to compute cosine similarity is a heavy answer to a small question.

The naive answer people write instead is a Python loop with numpy, or a brute force scan that sorts every single score before taking the top k. Both of those have real problems. The Python and numpy version pays interpreter and allocation overhead per query that adds up fast under load. The full sort version does `O(n log n)` work when a top k query only ever needed `O(n log k)`, which matters once `n` is in the hundreds of thousands and `k` is 10 or 20. On top of that, almost none of these quick implementations validate their inputs. An embedding provider having a bad day and returning a zero vector or a NaN is not a hypothetical, it happens, and a naive cosine implementation will either crash on a division by zero or silently return garbage rankings that nobody notices until a user complains that search results look wrong.

This file is a single Mojo module, `EmbeddingSimilaritySearch.mojo`, that solves the actual problem carefully: an `EmbeddingIndex` struct that stores embeddings contiguously, computes cosine similarity with a SIMD accelerated dot product, keeps only the top k candidates in a proper binary heap instead of sorting everything, rejects malformed input before it can corrupt the index or the results, and gives you a genuine multi core search path that returns bit for bit the same ranking as the single threaded path no matter how many workers you give it.

## Why I built it

I wanted a version of this that a senior engineer could actually drop into a service and trust, not a toy that only works on the happy path. Three things kept showing up as gaps in the quick implementations I kept running into.

The first is complexity. A lot of top k code either sorts the entire candidate list, which is wasteful once you only want the top 10 out of a hundred thousand, or maintains a plain array and does a linear scan for the minimum on every insert, which is quietly `O(n * k)`. Mojo makes it easy to write a proper binary heap by hand and get `O(n log k)` scanning without reaching for any library, so there was no excuse not to do it right.

The second is memory safety around raw buffers. `EmbeddingIndex` owns a flat `UnsafePointer[Float32]` buffer for the vectors and another one for their precomputed norms. If you let a struct like that be copied by accident, both copies end up pointing at the same memory, and whichever one gets destroyed first frees memory the other is still using, which is a classic double free. I deliberately did not give `EmbeddingIndex` a `__copyinit__`, only a `__moveinit__` and a `__del__`. That means the Mojo compiler itself refuses to compile any code that tries to copy an `EmbeddingIndex`, turning a runtime memory corruption bug into a compile error. That is the kind of guarantee you only get from a language with real ownership tracking, and it is a big part of why this is written in Mojo instead of something like Python or JavaScript.

The third is honesty about validation. Bad embeddings happen: a failed API call that returns all zeros, a NaN that leaked in from an earlier bug, a vector with the wrong dimension because someone swapped models. `add` and `search` both check every value with `_is_finite` before touching the index, and both reject zero vectors explicitly because cosine similarity is undefined when there is nothing to normalize against. None of this is exotic, it is just work that is easy to skip when you are in a hurry, and skipping it is exactly how a search feature ends up quietly broken in production for a week before anyone notices.

## When to use it

Reach for this when you have embeddings from an LLM, an image model or any other fixed dimension vector source, they comfortably fit in memory as float32 (a few hundred thousand vectors at typical embedding dimensions is well within reach of a single machine), and you want fast, exact cosine top k search without adding a vector database dependency to your stack. It fits well inside a service that builds or refreshes its embedding index periodically and then serves many search queries against that snapshot, which is the shape of a lot of RAG retrieval layers, internal search tools and recommendation services.

It is not the right tool once your vectors stop fitting comfortably in RAM, once you need approximate search to trade a little accuracy for a lot of speed at huge scale, or once you need features like filtering by metadata, incremental deletes or distributed sharding across machines. This index is append only and fixed capacity by design (more on that below), so if your workload needs to remove or update vectors in place, you will want to rebuild the index or reach for a purpose built vector database instead.

## How it works

The core data structure is `EmbeddingIndex`, which allocates two flat buffers up front: `data`, sized `dim * capacity`, holding every stored vector back to back in row major order, and `norms`, sized `capacity`, holding each row's precomputed L2 norm. Precomputing the norm at insert time means the hot query path never has to run a square root over stored data, only over the query itself.

`add` takes a `List[Float32]`, checks its length against `self.dim`, checks the index is not already at `self.capacity`, and then validates every element with `_is_finite`, which treats NaN and values outside a sane finite float32 range as invalid. Validation happens in a full pass before anything is written into `data`, so a rejected vector never leaves a half written row sitting at `self.count`. Only after validation passes does it write the row, accumulate the squared sum and store the square root of that sum in `norms`.

The dot product itself lives in `_dot`, a free function that takes two `UnsafePointer[Float32]` buffers and a dimension. It processes `SIMD_WIDTH` floats at a time (`SIMD_WIDTH` is `simdwidthof[DType.float32]()`, so it adapts to whatever the target CPU actually supports) using `load` and a running `SIMD` accumulator reduced with `reduce_add`, then finishes off any remaining floats that do not divide evenly into a full SIMD chunk with a plain scalar loop. That tail loop matters more than it looks: embedding dimensions like 300 or 384 are common and do not always divide evenly by every SIMD width, and skipping the tail silently truncates every dot product in the index.

`search` builds a validated copy of the query into a temporary buffer with `_prepare_buffer` (same NaN and dimension checks as `add`), computes the query's own norm by calling `_dot` on that buffer against itself and taking a square root, and rejects a zero norm query outright since cosine similarity has no defined answer for it. It then calls `_scan_rows`, which walks the requested row range, skips any stored row whose precomputed norm is below `EPSILON` (a degenerate stored vector, treated the same way a zero query is), computes `raw / (q_norm * row_norm)` for every remaining row, and feeds every score above `min_score` into `_offer`.

`_offer` is where the top k logic lives. It keeps a `List[SearchResult]` shaped as a binary min heap of size at most `k`, ordered by `_weaker`, a comparator built on `_better` (higher score wins, ties broken by the lower row index so results are always deterministic). While the heap has room, new candidates are appended and lifted into place with `_sift_up`. Once it is full, a new candidate only gets in if it beats the current root, the weakest of the kept results, in which case it replaces the root and `_sift_down` restores the heap property. This keeps the running cost at `O(log k)` per candidate instead of `O(k)` or `O(n log n)`. Once scanning finishes, `_sort_descending`, a small insertion sort over at most `k` items, turns the heap into the final ordered list, since insertion sort on a handful of items is faster in practice than paying for a general purpose sort.

`search_parallel` adds real multi core execution on top of the same scanning logic. It splits `self.count` rows into `num_workers` contiguous chunks, and inside a `@parameter fn worker` closure calls the same `_scan_rows` on each chunk, storing each worker's local top k list into its own slot of a `partials` list so there is no shared mutable state between threads. `parallelize[worker]` then runs those closures across real OS threads. The key correctness property here is that the true global top k must always be contained inside the union of every partition's own local top k, so after all workers finish, folding every partial result back through a fresh `_offer` pass with the same deterministic comparator reproduces exactly the same ranking `search` would have produced on its own, regardless of how many workers you use. `main` actually checks this by running both paths against the same query and raising an error if they ever disagree, index for index and score for score. I deliberately left the worker count as an explicit caller supplied argument rather than auto detecting core counts, because core count auto detection is known to lie inside containers running under a cgroup CPU quota, and a search service usually wants to pin its own worker count for a predictable latency budget anyway.

## Usage

Run the built in demo and self checks directly:

```
mojo run EmbeddingSimilaritySearch.mojo
```

This builds a 2000 row index of deterministic synthetic vectors, searches it with both `search` and `search_parallel`, asserts the two agree on every result, then exercises the NaN guard and the capacity guard on purpose and prints a confirmation for each.

To compile a standalone binary instead of running through the interpreter:

```
mojo build EmbeddingSimilaritySearch.mojo -o embedding_search
./embedding_search
```

To use it as a library from your own Mojo code, import the two public types and call them the same way `main` does:

```mojo
from EmbeddingSimilaritySearch import EmbeddingIndex, SearchResult

fn build_and_query() raises:
    var index = EmbeddingIndex(768, 100000)
    index.add(my_first_embedding)
    index.add(my_second_embedding)

    var hits = index.search(my_query_embedding, 10)
    for i in range(len(hits)):
        print(String(hits[i].index) + " " + String(hits[i].score))

    var fast_hits = index.search_parallel(my_query_embedding, 10, 8)
```

`add` raises on a dimension mismatch, a NaN or infinite value, or a full index. `search` and `search_parallel` raise on an invalid `k`, an invalid query vector or a zero norm query, and both accept an optional `min_score` argument if you only want cosine scores above a threshold instead of the unconditional top k.

## Notes

The index is fixed capacity and append only on purpose. Growing it would mean reallocating `data` and `norms`, which would invalidate any pointer arithmetic done mid search and is exactly the kind of subtle bug that is easy to introduce and hard to catch in review. If you need to remove or replace vectors, rebuild the index from scratch with the vectors you want to keep, sized to whatever capacity you actually need.

`EmbeddingIndex` is intentionally not copyable. It only implements `__moveinit__` and `__del__`, not `__copyinit__`, so the compiler rejects any attempt to copy it rather than letting two copies fight over freeing the same buffer. `SearchResult`, by contrast, is a small plain value type of an index and a score, and is copyable on purpose since the heap and the sort routines need to move and copy individual results around cheaply.

Reported cosine scores are true values in the minus one to one range, computed by dividing by both the query norm and the stored row norm. If you only care about ranking and not about the absolute score, you could skip dividing by the query norm entirely, since it is a positive constant across every candidate in a single query and cannot change their relative order. This file keeps the division in because a threshold like `min_score` only makes sense against a real cosine value, but it is worth knowing about if you are adapting the hot loop for a use case that only needs ranking.
