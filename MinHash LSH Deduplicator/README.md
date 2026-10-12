# MinHash LSH Deduplicator

A single file Mojo library that finds near duplicate documents in a stream using MinHash signatures and locality sensitive hashing. It picks its own band layout from your similarity threshold, confirms matches with exact Jaccard and groups duplicates so the oldest document always wins.

**Language:** Mojo | **Lines:** 583 | **Added:** 2026-10-12

## What this solves

Every team that trains or fine tunes a language model, builds a RAG index or scrapes the web ends up with the same mess: the corpus is full of documents that are almost the same. A blog post syndicated to forty sites. A licence header glued onto a thousand source files. A product page scraped on Monday and again on Friday with one price changed. Exact hashing catches byte identical copies and nothing else. One changed comma and the hash is different.

This matters for real reasons. Duplicated training text gets memorised, which hurts quality and leaks data. Duplicated chunks in a vector index waste storage and push the same passage into a top k result five times, which eats your context window. Duplicates between your train and eval sets quietly inflate your benchmark scores. Near duplicate removal is one of the cheapest quality wins in a data pipeline, and it is also the step people skip because the usual tooling is awkward.

The usual options are a Python script with a MinHash library, which is slow once you pass a few million documents, or a Spark job, which is a lot of machinery for a deduplication pass. This file is a third option: a small native library you can drop into a Mojo pipeline, feed documents one at a time and get an answer immediately. It is a streaming design, so you do not need the whole corpus in memory before you can start deciding.

## Why I built it

Most MinHash examples I found stop at the signature. They show you how to hash shingles and compute an estimated Jaccard score, then leave the hard parts to you. The hard parts are what break in production.

First, the band layout. LSH splits a signature into bands of rows, and the numbers you choose decide how many true duplicates you miss and how many unrelated pairs you waste time checking. People copy 20 bands of 5 rows from a blog post and never check whether it suits their threshold. I wanted the layout derived from the threshold, not guessed.

Second, the estimate is only an estimate. With 128 permutations the standard error on the Jaccard score is roughly 0.035, which is fine for finding candidates and not fine for deleting someone's data. I wanted an optional exact check against the real shingle sets before anything is declared a duplicate.

Third, clustering. If A is close to B and B is close to C, you want one cluster with one survivor, and you want the survivor to be stable. Picking whichever document arrived first and never changing your mind is the only rule that gives reproducible results between runs.

Fourth, the ugly inputs. Blank documents, two character documents, a boilerplate page that lands in the same bucket as half your corpus. A naive version divides by zero, silently treats every empty document as a duplicate of every other, or spends quadratic time on one hot bucket. I wanted each of those handled on purpose and visible in the output.

## When to use it

Use it when you need near duplicate removal inside a pipeline and the corpus is somewhere between a few thousand and a few tens of millions of documents. It fits pretraining and fine tuning data cleaning, RAG ingestion where you want to drop repeated chunks before embedding, benchmark contamination checks between a train split and an eval split, and scraper output where the same article appears under many URLs.

Do not use it for semantic duplicates. Two paraphrases of the same idea share almost no character shingles, so they will not match. For that you want embeddings and a vector search. It is also not a cross machine system. All state lives in one process, so if your corpus does not fit in memory you shard by a hash of the content first and run one instance per shard, or you move to a distributed tool.

Memory is the main cost. Each document keeps a signature of `num_perm` 64 bit values, so the default of 128 permutations is 1 KB per document. With `verify_exact` on, it also keeps every unique shingle hash, which is about 8 bytes per distinct character window. For a corpus of long documents you may want to turn that off and rely on the estimate with a stricter threshold.

## How it works

The text goes through `_normalize`. It lowercases ASCII letters, collapses any run of whitespace or control bytes into a single space and trims both ends. Bytes above 127 pass through untouched, so UTF-8 text keeps its identity. This is why a shouted, oddly spaced copy of a document still matches the original.

`_shingle_hashes` slides a window of `shingle_bytes` bytes over the normalised text (the default is `DEFAULT_SHINGLE_BYTES`, which is 5). Each window is hashed with FNV-1a 64 and then passed through `_mix`, a SplitMix64 finaliser that cleans up FNV's weak low bits. The hashes are sorted with `_heapsort` and deduplicated, so the result is a sorted set. A document shorter than the window becomes one shingle of the whole text. That detail matters: without it, every tiny string would produce an empty set and look identical to every other tiny string.

The MinHash signature is built in `add`. For each of the `num_perm` permutations there is a seed, derived once in the constructor from the `seed` argument through `_mix`. A permutation is simulated by XORing each shingle hash with the seed and mixing again. The signature slot keeps the minimum value across all shingles. I used a mixing function instead of the classic `(a*x + b) mod p` family because it avoids a large prime, avoids modular arithmetic and gives well spread values with plain 64 bit wraparound.

Banding is where `choose_band_plan` earns its place. The probability that two documents with true similarity `s` share at least one band is `1 - (1 - s^rows)^bands`, and `collision_probability` computes exactly that. `choose_band_plan` tries every divisor of `num_perm` as a band count, numerically integrates the area where the curve is wrong (collisions below the threshold are false positives, misses above it are false negatives) and returns the split with the lowest weighted cost as a `BandPlan`. The `false_positive_weight` parameter, default 0.5, lets you lean toward fewer misses or fewer wasted checks. Because the exact check removes false positives later, you can often lower it to favour recall.

Each band is hashed into a key by `_band_key` and stored in `BandTable`, a flat open addressing table that doubles in size through `grow` once it is half full. Entries with the same key are linked through the `chain_next` list, so there is no per bucket allocation. When a new document is added, it walks the chain for each of its bands and collects candidates. The `stamp` list makes sure each candidate is only scored once per insert, even if it collides in several bands.

Each candidate is scored with `_estimate`, which counts matching signature slots. If the estimate clears the threshold and `verify_exact` is on, `_exact` rebuilds both shingle sets and calls `exact_jaccard`, a merge over two sorted lists. A pair has to pass both tests. The best scoring match becomes the reported neighbour.

Clusters use a union find with path compression. `_union_keep_oldest` always makes the smaller document id the root, so the survivor of any cluster is the first document that arrived, and the answer does not depend on timing. `DedupDecision.duplicate_of` is that root. `cluster_of` and `cluster_count` read the same structure.

The failure modes are explicit. A blank document gets `STATUS_EMPTY`, still consumes a doc id so ids match your input order, and never enters the band table. A hot bucket is capped by `max_bucket_scan` (default `DEFAULT_MAX_BUCKET_SCAN`, 256) and each truncation increments `scans_truncated`, so you can see when boilerplate is costing you recall. Bad configuration raises in the constructor instead of producing quiet nonsense: a threshold outside (0, 1), fewer than 2 permutations or a shingle size below 1.

## Usage

Construct the deduplicator with a threshold, then call `add` for each document in order. The decision comes back immediately.

```mojo
from MinHashLshDeduplicator import MinHashDeduplicator, STATUS_DUPLICATE, STATUS_KEPT, STATUS_EMPTY

fn run() raises:
    var dedup = MinHashDeduplicator(0.8)

    var first = dedup.add("The quick brown fox jumps over the lazy dog near the river bank.")
    var second = dedup.add("the QUICK brown fox jumps over the lazy dog near the river bank")
    var blank = dedup.add("   ")

    if second.status == STATUS_DUPLICATE:
        print("doc " + String(second.doc_id) + " duplicates doc " + String(second.duplicate_of))
    if blank.status == STATUS_EMPTY:
        print("skipped an empty document")

    print("clusters: " + String(dedup.cluster_count()))
    print("hot buckets truncated: " + String(dedup.scans_truncated))
```

The constructor takes `threshold`, then optional `num_perm`, `shingle_bytes`, `verify_exact`, `max_bucket_scan` and `seed`. To see the band layout before you commit to it, call `choose_band_plan(0.8, 128)` and read `bands`, `rows`, `false_positive_area` and `false_negative_area`.

Running the file directly with `mojo run MinHashLshDeduplicator.mojo` executes `main`, which builds 200 synthetic documents and checks an exact copy, a case and whitespace variant, a three word edit, an unrelated document, a blank document and two identical tiny documents. It raises if any of them is classified wrongly.

## Notes

I wrote this against current Mojo syntax and I have not been able to compile it in the environment that produced it, so treat the first `mojo run` as the real test. Mojo changes quickly, and the most likely friction is small: a renamed standard library call or a change in how structs declare their copy and move behaviour. The algorithm and the self checks do not depend on those details.

The shingle size is a real tuning knob. Five bytes suits English prose. For source code or very short records, try a smaller window. For long legal or technical text, a larger one reduces accidental matches on shared phrases.

Signature building costs `num_perm` times the number of shingles per document and runs on one core. That is the part to parallelise first if you need more throughput, because documents are independent until the band lookup.

The estimator is unbiased but noisy. If you turn off `verify_exact`, set the threshold a few points higher than the similarity you actually care about. Keep exact verification on whenever the output feeds deletion.

Similarity is measured on bytes, not meaning. Reordering paragraphs barely changes the shingle set, so it will usually still be caught. Translating, paraphrasing or rewriting the text will not be.
