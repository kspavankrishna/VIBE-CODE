# ANN Recall Auditor

Measure the real recall@k of any approximate nearest neighbour index (HNSW, IVF, PQ, ScaNN, DiskANN or a hosted vector database) against exact ground truth that is proven correct, with tie aware scoring, confidence intervals and a CI gate. One Fortran file, OpenMP parallel, reads the `.npy` files your Python pipeline already writes.

**Language:** Fortran | **Lines:** 1429 | **Added:** 2026-11-21

## What this solves

Every RAG system, semantic search box and recommendation feed sits on an approximate nearest neighbour index. The index is fast because it skips work. The price of skipping work is recall: some of the true nearest neighbours never come back. Recall is the number that decides whether the retrieval step finds the right document, yet most teams measure it badly or not at all.

The usual way to measure it is a short Python script: brute force the true top k with NumPy or FAISS `IndexFlatL2`, intersect the id lists, divide by k. That script has four quiet problems that I kept running into in production.

1. **The ground truth is not exact.** Brute force in float32 through a matrix multiply reorders additions and rounds. On real embedding sets the 10th and 11th neighbours are often separated by less than that rounding error, so the "exact" list is sometimes wrong. Nobody checks, because the tool that would check is the tool that made the mistake.
2. **Ties are scored as misses.** Real corpora contain duplicate and near duplicate documents. If ten copies of the same chunk sit at exactly the same distance and k is 5, any five of them are a correct answer. Strict id matching punishes the index for picking different copies than the brute force pass did. Recall looks worse than it is.
3. **Id mapping bugs look like bad recall.** An index built with internal labels, an export that is 1 based, a sharded store that offsets ids per shard: all of these produce recall near zero and send people tuning `efSearch` for a day when the index is fine.
4. **A single mean hides the tail.** A recall of 0.95 on 500 queries has an uncertainty of about a point either way. Gating a release on the raw mean means a lucky sample passes and an unlucky one fails. The 5% of queries with recall 0.3 never show up at all.

ANN Recall Auditor fixes all four. It builds exact ground truth with a float32 fast path and a float64 proof step, scores ties by distance instead of by id, detects off by one id mappings, reports per query quantiles and the worst queries and gates on the lower bound of a bootstrap confidence interval.

## Why I built it

I was comparing vector index settings for a retrieval pipeline and the recall numbers did not add up. Raising the search depth made recall go down on one run. A second look showed the brute force baseline itself was changing between runs on different machines, because BLAS picked a different blocking and the boundary neighbour flipped. That is the moment you stop trusting every recall number you have ever seen.

I wanted a tool where the ground truth is either provably exact or says plainly that it is not, where duplicates do not distort the score and where the output is a pass or fail a CI job can act on. Fortran is a good fit: the heavy part is a dense matrix product followed by tight scalar loops, the array model maps directly onto row major `.npy` matrices with no copies and the binary compiles anywhere gfortran does.

## When to use it

- Choosing index parameters (`M`, `efConstruction`, `efSearch`, `nlist`, `nprobe`, PQ code size) and wanting numbers you can defend.
- Gating a deploy. Rebuilt the index, changed the embedding model, upgraded the vector database: run the audit in CI and fail when recall drops.
- Checking a hosted vector database. Export what it returned for a query set, audit it against the raw vectors.
- Caching ground truth once with `--write-truth` and reusing it across many index variants with `--truth`.
- Debugging "recall is zero": the offset probe tells you if the ids are shifted before you touch any index setting.

It is not a benchmark harness for latency or throughput. It scores what the index returned, not how fast it returned it.

## How it works

**Reading data.** `npy_read_header` parses the `.npy` preamble for format versions 1, 2 and 3, reads `descr`, `fortran_order` and `shape`, rejects big endian and structured dtypes with a clear message and checks the file size against what the header promises, so a truncated upload fails at load time instead of producing garbage recall. `npy_read_vectors` loads float32 or float64 matrices into a `(d, n)` array where every vector is one contiguous column. C ordered files are read in chunks of `READ_CHUNK_ELEMENTS`, Fortran ordered files one dimension at a time, so neither path needs a second full copy of the base. Every row is checked for NaN and Inf and the first bad row is reported by number. `npy_read_ids` loads int32, int64, uint32 or uint64 id matrices.

**Exact ground truth, proven.** `exact_ground_truth` runs in two passes. The first pass is a blocked float32 `matmul` of base blocks against query blocks. Each OpenMP thread owns whole query blocks and its own product buffer. `offer` keeps the best `k + margin` candidates per query (`DEFAULT_MARGIN` is 32) in a list sorted by distance then id. The second pass, `finalize_query`, recomputes every candidate distance in float64 with direct differences (no `|a|^2 + |b|^2 - 2ab` cancellation) and sorts them with `sort_pairs`.

Then comes the part that makes it exact. `distance_bound` computes a hard upper bound on how wrong the float32 pass could have been for this query, using the classic dot product bound `gamma_d * |q| * |b|` with `gamma_d = d*u / (1 - d*u)` and `u = UNIT_ROUNDOFF_F32`. That bound holds for any summation order, so it holds whatever blocking gfortran or an external BLAS chooses. Every base vector that was not kept had an approximate distance at or above the admission wall, so its exact distance is at least `wall - bound`. If the k-th exact distance is at or below that, nothing outside the candidate list can beat it and the query is certified. If not, that query alone is rescanned against the whole base in float64. The report states how many queries were certified and how many needed a rescan. `BOUND_SAFETY` adds 1% headroom to the bound. A larger `--margin` cuts rescans on very dense data.

**Tie aware recall.** `evaluate_query` does not ask "is this id in the true list". It asks "is this id's exact distance within the k-th true distance", with a tolerance of `DEFAULT_TIE_REL` relative plus `--tie-abs` absolute. Hits are capped at k. This is the right definition when duplicates exist. It is independent of which copy the brute force pass happened to keep. Strict id recall is still reported next to it. The report also counts how many hits only the tie aware rule credited.

**Bad slots.** Negative ids are padding (FAISS writes `-1` when a list is short) and count as misses. Ids outside the base are counted separately as out of range and gated by `--max-invalid-rate`, which defaults to zero because an out of range id is always a bug. Repeated ids are counted as duplicates and score once.

**Offset probe.** For every returned id, `evaluate_query` also checks whether id minus one or id plus one is a true neighbour. If shifted ids match far better than the real ones, the report says which `--id-base` to try and the gate fails, so a mapping bug never gets reported as an index quality problem.

**Statistics.** Per query recall feeds `quantile_sorted` for p50, p10 and p1 and `bootstrap_mean_ci` for a percentile bootstrap interval. The bootstrap draws from `xorshift64`, which uses only shifts and xors, so the interval is identical on every compiler and machine for the same `--seed` (`DEFAULT_SEED`). `sort_real_with_index` ranks the worst queries by recall then distance ratio. The distance ratio is the ann-benchmarks style sum of returned Euclidean (or cosine) distances over the sum of true ones, computed only for queries that returned k distinct valid ids. For inner product it reports mean score lost per slot instead, because a ratio of negative numbers means nothing.

**Cached truth.** `npy_write_ids` writes the exact ids as int64 `.npy` in your id base. `load_ground_truth` reads them back, validates every id, recomputes distances in float64 and sorts, so the tie threshold stays exact. Loaded truth is marked as trusted rather than certified in the report.

**Exit codes.** `EXIT_PASS` (0) on success, `EXIT_GATE_FAIL` (1) when the recall gate, invalid rate or offset probe fails and `EXIT_INPUT_ERROR` (2) for bad files or flags.

## Usage

Build it. OpenMP is optional: without `-fopenmp` the directives are plain comments and it runs on one core.

```bash
gfortran -O3 -march=native -fopenmp AnnRecallAuditor.f90 -o ann-recall-auditor
```

Export the vectors and the index results from Python. Rows of `ann.npy` line up with rows of `queries.npy`. The columns are ranked neighbour ids.

```python
import numpy as np
np.save("base.npy", base_vectors.astype(np.float32))      # (n, d)
np.save("queries.npy", query_vectors.astype(np.float32))  # (q, d)
D, I = index.search(query_vectors, 10)                     # any ANN index
np.save("ann.npy", I.astype(np.int64))                     # (q, 10)
```

Audit and gate on the lower bound of the 95% interval:

```bash
./ann-recall-auditor --base base.npy --queries queries.npy --results ann.npy \
  --k 10 --metric cos --min-recall 0.95 --json recall.json
```

Build ground truth once, then audit many index variants against it:

```bash
./ann-recall-auditor --base base.npy --queries queries.npy --k 100 --write-truth truth.npy
./ann-recall-auditor --base base.npy --queries queries.npy --results hnsw_ef64.npy \
  --truth truth.npy --k 10
```

Main options: `--metric l2|ip|cos`, `--id-base 0|1`, `--gate-on lower|mean`, `--max-invalid-rate`, `--confidence`, `--bootstrap`, `--seed`, `--tie-rel`, `--tie-abs`, `--margin`, `--query-block`, `--base-block`, `--worst`. Run `--help` for the full list. A truth file with more columns than k is fine: the first k are used, the same as for results.

A typical report looks like this:

```
  truth      exact, 500 of 500 certified, 0 float64 rescans, 0.06 s
  recall@10 tie aware   0.8988   95% CI [0.8900, 0.9070]
  recall@10 id match    0.8988
  per query  p50 0.9000   p10 0.8000   p1 0.6000
  slots      5000 total, 0 padding, 0 out of range, 0 duplicate
  gate       PASS (recall lower 0.8900 >= 0.8500)
```

## Notes

- Vectors are stored as float32. Float64 input is narrowed on load, which matches how almost every ANN index stores them. Distances are always computed in float64 from those stored values.
- Metrics follow the index convention. `l2` is squared Euclidean for ranking, `ip` ranks by largest inner product, `cos` by largest cosine. Under `cos` a zero norm query is skipped and reported. A zero norm base vector is treated as orthogonal to everything.
- Memory is the base matrix plus `(k + margin)` candidates per query plus one product block per thread. A million 768 dimension vectors need about 3 GB.
- For larger bases, link an optimised BLAS with `-fexternal-blas -lopenblas` so `matmul` calls `sgemm`. The error bound still holds because it does not depend on summation order.
- If many queries need float64 rescans, the data is very dense near the k-th neighbour. Raise `--margin` and the rescans fall away.
- The host must be little endian. It checks this at start up and stops if not.
- Built and tested with gfortran 13 against NumPy brute force on l2, ip and cos, Fortran ordered float64 input, int32 results, padding, out of range ids, duplicate vectors and shifted ids.
