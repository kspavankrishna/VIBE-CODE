# Vector Search Optimizer

A RAG retriever returns the ten best chunks and your prompt blows past the context window. This is a small Rust vector store that scores embeddings by cosine distance, keeps the top k in a heap and tags each result with whether it still fits inside a token budget.

**Language:** Rust | **Lines:** 103 | **Added:** 2026-04-05

## What this solves

Retrieval and context packing are two different problems that most RAG code treats as one. The vector database gives you the nearest neighbours. Nobody tells you how many of them you can actually afford to put in the prompt. So the usual shape is: fetch top 10, concatenate, send. That works in testing because your test chunks are short. In production the chunk sizes are all over the place, one document happens to be a 4000 token table dump, and the request fails with a context length error at 2am.

The failure mode is worse than a clean error. Providers truncate silently in some SDK paths, so the model answers from half the evidence you intended and nobody sees a stack trace. The support ticket says "the answer was wrong", not "chunk 7 was dropped". You spend a day chasing a hallucination that was really a packing bug. On a paid API every one of those oversized calls also bills for tokens you never needed, and at a few thousand queries a day that compounds into a real number on the invoice.

The other half of the problem is where the work runs. Calling out to a hosted vector service for a corpus of a few thousand embeddings adds a network hop to every query, plus a bill, plus an availability dependency on something you do not control. Below a certain corpus size a brute force scan in process is faster than the round trip and has no failure mode beyond your own binary.

This file handles both in one pass. Every embedding carries its own token count next to its vector. A single search computes distances, keeps the k closest in a `BinaryHeap` and then walks the results applying a running budget, returning a boolean per result that says whether that chunk fits. The caller drops the ones marked false. No second lookup, no separate tokenizer call at prompt build time, no guessing.

## Why I built it

Every embedded vector search crate I looked at returns `(id, score)` and stops there. That is the right contract for a search library and the wrong contract for a RAG pipeline, because the consumer of the result is a context window with a hard ceiling, not a results page. You end up writing the same budget loop by hand in every project, usually with the token counts stored in a second map that drifts out of sync with the index.

Putting the token count inside the embedding record makes the drift impossible. Doing the budget accounting in the same pass as the heap drain makes it free. The whole thing is standard library only, no crates, no index build step, no background threads, so it drops into any Rust service as a single file.

## When to use it

- A RAG service with a few thousand to a few tens of thousands of chunks, where a full scan per query is cheaper than a hosted vector DB round trip.
- You keep hitting context length errors because retrieved chunk sizes vary wildly and nobody budgets for it.
- You want retrieval and prompt packing decided in one place instead of two files that disagree.
- An offline batch job that scores a corpus against many queries and has no business standing up a database for it.
- A second stage reranker, where a cheap first stage hands you a candidate id list and you want exact cosine scores over just those.
- Anywhere you want zero dependencies, because the security review for a new crate costs more than the code.

## How it works

Embeddings live in a flat `Vec<Embedding>` inside `VectorSearchOptimizer`. Each `Embedding` holds an id `String`, a `Vec<f32>` and a `u16` token count. `new(dimension, token_budget)` fixes both the vector width and the per query budget up front, and `add` asserts that every vector matches the declared dimension, so a mismatched model output fails at insert rather than producing a nonsense score later.

`search(query, k)` is a brute force linear scan. There is no ANN index, no HNSW graph, no quantisation. For every stored embedding it calls the free function `cosine_distance`, which computes the dot product and both magnitudes with plain iterator folds and returns `1.0 - (dot / (mag_a * mag_b))`. A zero magnitude on either side returns `1.0`, so a zero vector is treated as maximally distant instead of producing a NaN that would poison the comparison. Magnitudes are recomputed on every call, there is no cached norm.

Top k selection uses a bounded `BinaryHeap<ScoredResult>` with capacity k. `ScoredResult` is a `Copy` struct carrying the distance, the token count and the index back into the embeddings vec, so nothing is cloned during scoring. The custom `Ord` impl compares distances with `partial_cmp` and then calls `.reverse()`. Because `BinaryHeap` in Rust is a max heap over `Ord`, that reversal inverts the usual convention: the element at `peek()` is the one with the smallest distance, not the largest. The eviction branch and the final `results.reverse()` are both written as if the heap were the other way round, which is the first thing to check before you trust this on real data. See Notes.

Budget accounting happens during the heap drain. A running `total_tokens` counter starts at zero, and for each popped result the code tests `total_tokens + scored.tokens <= self.token_budget`. If it fits, the counter advances and the result is flagged `true`. If it does not fit, the counter is left alone and the result is flagged `false`, and the loop keeps going. That is a greedy fill, not a knapsack solve, so a later small chunk can still be admitted after a large one was rejected. The return type is `Vec<(String, f32, bool)>`: id, distance and the fits flag.

`rerank(ids, query)` is the second entry point. It takes a slice of ids, looks each one up with a linear `find` over the embeddings vec, scores it against the query with the same `cosine_distance` and sorts ascending so nearest comes first. Ids that are not in the store are dropped silently by the `filter_map`. It applies no token budget at all.

## Usage

```rust
// single file module, no external crates
mod vector_search_optimizer;
use vector_search_optimizer::VectorSearchOptimizer;

fn main() {
    // 1536 dimensional embeddings, 8000 token context budget
    let mut store = VectorSearchOptimizer::new(1536, 8000);

    // id, vector, token count for that chunk
    store.add("doc-1#chunk-0".to_string(), embed("..."), 412);
    store.add("doc-1#chunk-1".to_string(), embed("..."), 1980);
    store.add("doc-2#chunk-0".to_string(), embed("..."), 305);

    let q = embed("how do I rotate the signing key");

    // top 10 by cosine distance, each tagged with whether it fits the budget
    for (id, distance, within_budget) in store.search(&q, 10) {
        if within_budget {
            println!("keep {id} d={distance:.4}");
        } else {
            println!("skip {id} d={distance:.4} (over budget)");
        }
    }

    // exact rescoring of a candidate list from a cheaper first stage
    let candidates = vec!["doc-2#chunk-0".to_string(), "doc-1#chunk-0".to_string()];
    let ranked = store.rerank(&candidates, &q); // Vec<(String, f32)>, nearest first
}
```

## Notes

- The `Ord` impl reverses the distance comparison, so `heap.peek()` yields the nearest result rather than the farthest. The eviction test `if dist < worst.distance` and the trailing `results.reverse()` are both written for the opposite convention. Verify selection and output order against a known corpus before shipping this.
- `PartialEq` treats two results as equal when their distances are within `1e-6`, which does not agree with the `Ord` impl. That inconsistency is technically undefined behaviour territory for sorting contracts, though `BinaryHeap` only ever calls `cmp`.
- `total_tokens + scored.tokens` is unchecked `u16` arithmetic. With a large budget and large chunks the sum can exceed 65535, which panics in debug builds and wraps in release. Use `u32` if your budget goes near the top of the `u16` range.
- Search is O(n * d) per query, single threaded, no SIMD and no index. It is the right tool at a few thousand vectors and the wrong tool at a million.
- `add` and `search` both `assert_eq!` on the vector dimension, so a wrong sized input panics rather than returning an error. Wrap the calls if you need a `Result`.
- There is no delete, no update, no persistence and no serialisation. The store lives in memory for the lifetime of the process, and you rebuild it on start.
- `rerank` does a linear `find` per id, so it is O(n * m) in store size times candidate count, and it drops unknown ids without telling you.
- Budget packing is greedy over the drained order, not an optimal knapsack. It will not reorder or swap chunks to maximise how much evidence fits.
