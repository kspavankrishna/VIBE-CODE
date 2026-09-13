# Vector Product Quantizer

Storing raw float32 embeddings gets expensive fast: a million 1536 dim vectors from a typical embedding model already costs about 6GB, and once you get into the hundreds of millions of vectors that most RAG and search systems end up with, keeping everything in RAM as raw floats stops being realistic. This tool compresses embedding vectors into tiny byte codes using product quantization and lets you search those codes directly, without ever decompressing them back to full precision.

**Language:** Nim | **Lines:** 655 | **Added:** 2026-09-13

## What this solves

Every vector database and every RAG pipeline eventually hits the same wall: embeddings are big, and there are a lot of them. A 1536 dimension float32 vector is 6144 bytes. At 100 million vectors, that is over 600GB just for the raw numbers, before you add an index on top. Most teams solve this by throwing money at bigger machines, or by pulling in a heavyweight C++ library like FAISS just to get one specific feature: compressed vectors that can still be searched approximately.

Product quantization is the technique that makes this possible, and it is not new, it is what FAISS and most serious vector databases use internally. The idea is to split each vector into M equal size chunks, and for each chunk position, learn a small codebook of K representative sub vectors (typically K=256, so each chunk's code fits in a single byte) using k-means. Once you have the codebooks, any vector can be replaced by a sequence of M byte codes, one per chunk, pointing at the nearest codebook entry for that chunk. A 1536 dimension vector split into 96 chunks becomes 96 bytes instead of 6144, a 64x reduction, and the loss in accuracy is often small enough not to matter for nearest neighbor search.

The harder part, and the part most toy implementations get wrong or skip entirely, is doing the training and the search correctly and safely. This file trains codebooks with k-means++ seeding instead of random initialization (which matters a lot for quality), handles the case where k-means produces an empty cluster (a real failure mode that corrupts a codebook with a NaN centroid if you do not handle it), streams the encode and decode steps so you never need the full dataset in RAM at once, validates every vector for NaN and infinity before training or encoding and implements asymmetric distance computation (ADC) so you can rank a database of byte codes against a full precision query without ever reconstructing the compressed vectors.

## Why I built it

I kept running into the same situation: a project has embeddings, the embeddings are getting big, and pulling in FAISS means a C++ build dependency, a Python wrapper, or a whole vector database service, just to get compression and approximate search for one subsystem. Sometimes you just want a dependency free, auditable, single file tool that does the compression step and hands you either a compressed file you can search yourself, or a reconstruction you can sanity check. Nim is a good fit here because it compiles to a small native binary with no runtime, no garbage collector pauses to worry about during a long training run and direct control over memory layout, which matters when you are moving arrays of millions of float32 values around.

I also wanted something where the failure modes are explicit instead of silent. A lot of k-means code out there will happily produce a codebook with a NaN in it if a cluster goes empty during training, and you will not find out until your search results silently degrade in production. This tool refuses to hide that class of bug.

## When to use it

Reach for this when you have a set of embedding vectors, most likely from an LLM embedding model, a computer vision model or any pipeline that produces fixed size float vectors, and you need to either shrink the storage footprint or speed up approximate nearest neighbor search over a large set of them. It is a good fit for RAG systems with a large document store, recommendation systems with millions of item embeddings or any system where the vectors do not comfortably fit in memory at full precision.

It is not the right tool if you need exact nearest neighbor results. Product quantization is lossy by design, it trades a controlled amount of accuracy for a large reduction in size and search cost. Check the `eval` command's reconstruction error and recall numbers on your own data before trusting it in production, the right tradeoff depends entirely on how tightly clustered your embeddings are and how much compression you actually need. It is also not meant to replace a full vector database if you need filtering, metadata or distributed sharding, it solves the compression and approximate scoring problem specifically, and you can build those other features around it.

## How it works

Vectors go in and out of this tool as flat binary files: a stream of little endian float32 values, D values per vector, with no header, which is exactly what numpy's `array.tofile()` produces, so there is no conversion step needed if your embeddings already live in numpy arrays or any similar buffer.

Training happens in `cmdTrain`. It reads the input file, takes a bounded sample using `reservoirSample` (so a training run never needs the full multi hundred gigabyte dataset in memory, only a configurable sample size), and for each of the M subspaces it isolates that subspace's slice of every sampled vector and calls `trainSubspace`. That function seeds initial centroids with `kmeansPlusPlusInit`, which picks the first centroid at random and then each following centroid with probability proportional to its squared distance from the nearest centroid already chosen, which is the standard k-means++ approach and gives much better codebooks than picking K random points. It then runs Lloyd's iterations, assigning every sample point to its nearest centroid and recomputing centroids as the mean of their assigned points, accumulating those sums in float64 so a subspace with a huge sample does not lose precision the way a running float32 average would. If a cluster ends up with zero points assigned to it, which does happen especially with duplicate or near duplicate input, the code finds the cluster with the most points, splits off the point farthest from that cluster's own centroid and uses it to reseed the empty one, so no codebook entry is left as a stale or NaN value. Training stops early once the total assignment inertia stops improving by more than a small relative threshold, or after the requested iteration count. The finished codebook is written by `writeCodebook` with a small binary header (the `VPQ1` magic, a format version, and the D, M, subDim and K values) followed by the flat centroid array, so a codebook file is fully self describing and `readCodebook` will reject a mismatched or corrupted file instead of misreading garbage.

Encoding, in `cmdEncode`, streams the input file in batches of `ReadBatch` (4096) vectors, and for each vector calls `encodeOne`, which for every subspace does a linear scan over the K centroids using `distSq` and keeps the closest one, writing its index as a single byte. The output codes file starts with a `VPQC` magic, the subquantizer count M and the total vector count, so it too is self describing. Decoding, in `cmdDecode`, is the mirror image: `decodeOne` looks up each byte's centroid and concatenates them back into a full width float32 vector, which gives you an approximation of the original, not an exact copy.

Search is where the real payoff shows up. `cmdSearch` builds a distance table per query with `buildDistanceTable`: for every subspace and every one of the K centroids in that subspace, it precomputes the squared distance (or, with `--metric ip`, the inner product) between the query's subvector and that centroid. That is a small, fixed amount of work per query, K times M numbers. Then, for every stored code in the database, `adcScore` just adds up the M precomputed numbers that correspond to that code's byte values, no floating point vector math against the original data at all. This is the entire point of ADC: the expensive part happens once per query, and scanning millions of stored codes afterward is just cheap table lookups and additions. Results are kept in a bounded heap of `ScoredHit` values, sized to `--topk`, using a max heap trick (the `<` operator is intentionally inverted so the heap's root, index 0, always holds the current worst of the kept results, letting every new candidate be checked against it in constant time).

`cmdEval` is the honesty check. It decodes a random sample of vectors and compares them against the originals to report RMSE and worst case error, and it runs a small brute force nearest neighbor search on the same sample to compare against the ADC search results, reporting a recall@k number. Run this on a slice of your real data before trusting the compressed index for anything important, because how much accuracy you lose depends entirely on how your specific embeddings are distributed.

## Usage

Build it once with the Nim compiler, using a release build so the tight per vector loops in training, encoding and search are properly optimized:

```
nim c -d:release -o:vpq VectorProductQuantizer.nim
```

Train a codebook from a file of embeddings (dimension 1536, split into 96 subquantizers of 16 floats each, 256 clusters per subspace, trained on a sample of 200000 vectors):

```
./vpq train --input embeddings.f32 --dim 1536 --subquant 96 --clusters 256 \
            --iters 25 --sample 200000 --seed 42 --out codebook.vpq
```

Compress the full database against that codebook:

```
./vpq encode --input embeddings.f32 --dim 1536 --codebook codebook.vpq --out codes.vpqc
```

Check what you got:

```
./vpq stats --codebook codebook.vpq
```

Search a file of query vectors against the compressed codes:

```
./vpq search --query queries.f32 --dim 1536 --codebook codebook.vpq \
             --codes codes.vpqc --topk 10 --metric l2 --out results.tsv
```

`results.tsv` comes back as `query_index`, `rank`, `vector_index`, `distance_or_score` columns, one row per result. Check the compression against reality before you rely on it:

```
./vpq eval --input embeddings.f32 --dim 1536 --codebook codebook.vpq \
           --codes codes.vpqc --sample 2000 --topk 10
```

And if you ever need the approximate vectors back in full width float32 for downstream code that expects raw vectors:

```
./vpq decode --codes codes.vpqc --codebook codebook.vpq --out reconstructed.f32
```

## Notes

`--dim` must be evenly divisible by `--subquant`, the tool will tell you every valid subquantizer count for your dimension if you get this wrong. `--clusters` is capped at 256 because each code is stored as one byte, that is a hard limit of the format, not a tunable default. Training loads only a sample into memory (controlled by `--sample`), but encode, decode and search stream through the input files in fixed size batches, so the tool's memory use stays flat regardless of how many vectors you throw at it, only the codebook and the per query distance tables live fully in memory. If you plan to compare vectors by cosine similarity rather than raw distance, normalize your vectors to unit length before training and before encoding, then use `--metric ip`, since inner product on unit vectors is equivalent to cosine similarity. The `eval` command does load the full input and codes into memory for its brute force comparison, so run it against a representative sample or a dataset that already fits in RAM rather than your full production corpus, that is a deliberate scope limit, not an oversight, since encode and search already cover the streaming path for anything larger.
