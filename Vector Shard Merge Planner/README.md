# Vector Shard Merge Planner

Parallel ingestion workers each build their own piece of a vector index, and merging those pieces back into one index is where duplicate ids, resurrected deletes and mismatched embedding models quietly get baked in. This is a single file F# CLI that reads every shard as JSON and hands back a full, auditable merge plan instead of touching your index at all.

**Language:** F# | **Lines:** 1071 | **Added:** 2026-09-12

## What this solves

Most RAG and semantic search setups do not build one vector index in one process. A crawler runs on a schedule, a batch embedding job processes yesterday's documents, three workers pull from a queue at once, and each one writes its own shard so nobody blocks on a shared lock. Sooner or later those shards have to become one index, and that merge step is where a surprising number of production incidents start.

The failures are specific and they repeat across teams. Two workers embed the same document at nearly the same time and now two vectors carry the same document id with different content hashes, and whichever one a naive merge script appends last wins by accident, not by design. A document gets deleted from the source system after it was already embedded into an older shard, and a merge that just unions everything resurrects it, so a user searches and gets back a page that was pulled for a legal reason last week. A team re-embeds their corpus on a new model with a different dimension, half the shards are still on the old model, and a merge that concatenates blindly puts 768 dimension and 1536 dimension vectors in the same ANN index, which either throws at query time or silently returns garbage nearest neighbours depending on which library you use. A near duplicate document, the same page crawled twice with a tracking parameter added to the URL, ends up as two nearly identical vectors that both show up in the same top five results and push out something that would have actually helped the user. None of these are exotic. They are what happens the second week you run a multi worker ingestion pipeline instead of a single script.

Vector Shard Merge Planner takes every shard's entries, resolves exactly one winner per document id with a rule that is the same regardless of which shard is listed first, keeps a hard wall between different embedding spaces, flags likely duplicates instead of guessing which one to keep, respects a capacity budget if your index has one, and assigns every surviving vector to a target shard with consistent hashing so a later resize does not reshuffle everything. It never writes to your actual vector store. It reads shard manifests and writes back a plan: a list of exactly what should happen to every single vector, and why.

## Why I built it

Every merge tool I had seen either trusted the newest wall clock timestamp, which breaks the moment two workers run on hosts with clocks that are even a few seconds apart, or just concatenated shards and left deduplication as a "future problem" that somebody hits in production three months later. Neither approach gives you anything to review before it runs, and neither one tells you what it is about to throw away.

I wanted three properties that most merge scripts do not have. First, determinism: run the planner twice on the same input and get byte identical output, so a flaky merge is never blamed on the planner itself when it is actually a data problem. Second, an explicit paper trail: every single input row ends up somewhere in the output, tagged with one of five outcomes and a plain English reason, so nothing just disappears. Third, safety on the ambiguous cases: a near duplicate flag never auto deletes anything, an embedding dimension mismatch never gets silently coerced, and a corrupt shard gets quarantined as a whole rather than trusted row by row. A planner that is wrong loudly is far cheaper than one that is wrong quietly.

F# fit this well because the whole problem is really a pile of small pure decisions over immutable records: given this set of candidate entries for one document id, which one wins. Given this set of vectors, which pairs are too close to call. Given this ranked list, which entries fit inside a budget. Discriminated unions make the five possible outcomes (keep, drop, quarantine, evict, review) exhaustive and impossible to half handle in a match expression, and there is no shared mutable index state anywhere in the decision logic to get subtly wrong under concurrency, because there is no concurrency: the planner is a batch job over an in memory list.

## When to use it

- Before merging shards from parallel ingestion workers, crawlers or batch embedding jobs into one production index.
- When your corpus spans more than one embedding model or dimension and you need a hard gate instead of hoping nobody mixes them.
- When you support document deletion and need deletes to actually stay deleted through a merge, not just through a direct write path.
- When you are about to resize your index from N shards to M and want most documents to stay where they are instead of a full reshuffle.
- When a compliance or support ticket asks exactly why a specific document id ended up in or out of the index, and "the merge script did it" is not an acceptable answer.
- As a scheduled CI style gate with `--fail-on-quarantine`, so a run with corrupt shard input fails the pipeline instead of quietly merging around the damage.

## How it works

Input is one JSON document with a `shards` array and a `targetShardCount`. Each shard has a `shardId`, an optional `declaredChecksum`, and a list of `entries`, where each entry carries a `docId`, a `vectorId`, an `embeddingDim`, a `logicalClock`, a `contentHash`, an optional `deleted` flag, an optional `importanceScore`, and an optional `vector` array of floats. The whole thing is parsed by a small hand written recursive descent parser (`parseJson`, backed by the `JsonValue` union and the private `JsonParsing` module) rather than a reflection based JSON library, because a discriminated union does not round trip cleanly through most of those without extra plumbing, and a parser you can read top to bottom is one less place to guess about behaviour.

`parseEntry` turns one JSON object into a `ShardEntryInput`, or into a `BadEntry` carrying a reason if a required field is missing, a vector's length does not match its declared `embeddingDim`, or `importanceScore` is not a finite number. A bad entry never aborts the shard around it: it becomes a single row tagged `quarantine` in the final plan, so one corrupt record in fifty thousand does not block everything else. `parseShard` then reads the shard's own fields. Once a shard's entries are parsed, `verifyShard` checks two things before trusting any of them: `internalConsistencyIssue` looks for the same `vectorId` appearing twice inside one shard, which usually means a file got concatenated or written twice, and if a `declaredChecksum` was supplied, `computeShardChecksum` recomputes a SHA-256 over every entry sorted by `vectorId` and compares it. Either failure quarantines the entire shard, because at that point nothing in it is provably intact.

Entries from healthy shards are grouped by `docId`, and `resolveDocGroup` picks exactly one winner per group using `lwwSortKey`: descending logical clock first, then a bias toward a deleted entry over a live one when clocks tie, then content hash, then vector id, all fields of the entry itself, never the order shards happened to be listed in. That clock tiebreak toward deletion is a deliberate choice: if a delete and a concurrent re-insert land on the same logical step, the planner would rather risk hiding a document that should have stayed than risk resurrecting one that a user or a compliance process explicitly removed. If the winner is a tombstone, it and every losing entry in the group get dropped with a reason naming the logical clock that deleted it. Otherwise the losers are dropped as superseded and the winner moves on as a keep candidate.

Those candidates then hit `pickCanonicalDim`, which finds whichever `embeddingDim` the majority of them agree on. Anything with a different dimension is quarantined individually rather than merged into an index it cannot geometrically belong to. What is left goes through `detectNearDuplicates`, which does not do a full pairwise comparison, because that is O(n^2) and does not survive a merge with real volume. Instead it builds a small set of hyperplanes with `buildHyperplanes`, where every hyperplane weight comes from `hyperplaneWeight`, a pure function of a plane index and a dimension index run through `mixSeed`, a splitmix style bit mixer, so the same seed produces the exact same buckets on any machine and any .NET version with nothing stateful to thread through the code. `chooseLshBits` picks how many hyperplanes to use by targeting roughly eight candidates per bucket unless `--lsh-bits` overrides it. Vectors land in a bucket by the sign pattern of their dot product against each hyperplane, and only vectors sharing a bucket are ever compared with `cosineSimilarity`. If a bucket somehow grows past `MaxDuplicateBucketSize` (500), the planner does not fall back to comparing all of them: it flags the whole bucket for review and says so, because silently doing O(n^2) work on a pathological bucket is exactly the kind of failure mode that should be explicit instead of just slow. A pair above `DuplicateCosineThreshold` (0.985 by default) with different document ids gets both sides tagged `review`, never auto dropped, because an approximate similarity score is a good filter and a bad judge of which one is the real duplicate.

Whatever survives dimension and duplicate filtering goes through `enforceCapacity` if a `capacityBudget` was set, sorted by `capacitySortKey`, which orders by importance score, then logical clock, then document id, so eviction order never depends on scan order either. Finally `buildRing` and `placementFor` assign each keeper to a target shard using consistent hashing with virtual nodes (`shardId#vnode` hashed through the same SHA-256 based `hashToUInt64`), which is the reason this uses a ring instead of a plain `hash(docId) mod shardCount`: a mod based scheme reshuffles nearly every document the moment the shard count changes, and a ring only moves roughly one over N of them.

Every action, across every stage, is collected into one flat list and `computePlanHash` sorts it by document id, vector id and source shard before hashing it, so the hash does not depend on any incidental ordering upstream. Two runs on the same input produce the same hash, which is the property that makes the plan safe to review, replay or diff. `mergePlanToJson` renders the whole thing, and `renderText` gives a short human summary for a terminal.

## Usage

Pipe a manifest in on stdin, or point at a file:

```
mono VectorShardMergePlanner.exe --input shards.json --format text
```

or, once compiled with the .NET SDK:

```
dotnet fsi VectorShardMergePlanner.fs -- --input shards.json --target-shards 8
```

The JSON body needs a `shards` array and either a top level `targetShardCount` or the `--target-shards` flag. Useful flags: `--capacity N` caps the merged index size, `--duplicate-threshold 0.98` tightens or loosens near duplicate flagging, `--lsh-bits N` fixes the bucket count instead of auto scaling it, `--virtual-nodes N` changes ring granularity (default 100), `--compact` drops the plan to single line JSON, `--output PATH` writes the plan to a file instead of stdout, and `--fail-on-quarantine` exits 1 if any shard was quarantined, which is what you want on a CI runner. The plan comes back with a `planHash`, per stage counts, the list of quarantined shards and their reasons, and one row per input vector saying whether it was kept and where, dropped and why, quarantined, evicted for capacity, or flagged for review.

## Notes

- This plans a merge. It does not execute one. Applying `keep` rows to your actual index, vector store or ANN library is a separate step you write against your own storage, which is what keeps this tool honest about not needing write access to anything.
- Review is a real fourth outcome, not a soft warning bolted onto keep or drop. A flagged pair stays out of the plan's keep list entirely until something, a human or a second pass, resolves it.
- The tombstone bias at equal logical clocks favours safety over completeness. If your system needs the opposite tradeoff, that one comparison in `lwwSortKey` is the place to change it, and it is worth documenting why if you do.
- Near duplicate detection only runs on entries that included a `vector` field. A metadata only manifest still gets full last writer wins resolution, dimension checks, capacity enforcement and placement, just no duplicate flagging.
- The duplicate check's cost is bounded by design: expect roughly `candidates * lshBits * embeddingDim` multiplications for bucketing, plus a bounded amount of pairwise cosine work inside buckets capped at `MaxDuplicateBucketSize`. Raising `--lsh-bits` shrinks buckets and lowers recall on true duplicates; lowering it does the opposite.
- A `declaredChecksum` is optional per shard. Supply it when a shard is written by a process you do not fully trust, or when you want the planner itself to catch a truncated or corrupted write before it reaches the merge logic at all.
