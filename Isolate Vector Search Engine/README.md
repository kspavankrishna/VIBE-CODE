# Isolate Vector Search Engine

Semantic vector search in Dart that does not freeze your Flutter UI or stall your server's request handler, and does not deep copy every embedding buffer across the isolate boundary on the way in.

**Language:** Dart | **Lines:** 1160 | **Added:** 2026-09-06

## What this solves

On-device RAG, local search over notes or chat history, and offline first AI features all need a way to hold thousands to a few hundred thousand float vectors and scan them fast. None of that can happen on the isolate driving your widgets or your request handler without causing jank or stalling other work.

The failure mode is easy to reproduce. You load 40,000 embeddings of 384 dimensions into a `List<Float32List>` and loop over them computing cosine similarity. On the main isolate that loop is roughly 15 million multiply adds. Frames stop rendering, the user types the sixth character of a query and the field goes dead for a few hundred milliseconds. On a Dart server the same loop blocks every other in flight request on that isolate, so one retrieval call adds tail latency to every unrelated endpoint. Nobody sees a crash. They see an app that feels broken.

The second failure mode shows up after you move the work to a background isolate. Dart deep copies regular `TypedData` on send, so a bulk load of 40,000 vectors copies about 60 MB across the boundary and you pay for it in allocation and GC pressure at exactly the moment the user is waiting. And a naive worker isolate has no way to be told to stop: the user has typed three more characters, four stale scans are still running to completion, and the shard is answering questions nobody is asking anymore. The third failure only bites in production. An isolate can die, and if a shard crashes mid scan every caller waiting on it hangs forever unless someone wrote the code that resolves those futures.

## Why I built it

Every simple version of this I have seen makes one of two mistakes. Either it runs the whole scan on the main isolate and stalls the UI the moment the index gets big, or it throws work at a background isolate but copies every vector across the boundary by value. Neither handles cancellation, and neither survives an isolate death.

The alternative is a native dependency: a plugin, platform channels, per platform builds and a much larger surface to keep working on iOS, Android, desktop and a plain Dart server at once. For index sizes in the thousands to low hundreds of thousands, a well laid out brute force scan spread across a few isolates is fast enough that the native dependency stops being worth what it costs you.

## When to use it

- A Flutter search box or autocomplete field doing embeddings retrieval on every keystroke, where each new query must invalidate the previous one
- On-device RAG over notes, documents or chat history where sending text to a server is not an option
- A Dart backend serving retrieval for a RAG pipeline, with many callers querying concurrently and needing per caller timeouts
- Duplicate detection or similarity matching over a catalog you already hold in memory
- A CLI tool indexing local documents where a native vector database is more setup than the job deserves
- Any Dart process that wants the index in memory plus a hard cap on how much work a slow consumer can pile up

## How it works

`IsolateVectorSearchEngine.spawn` starts a pool of persistent worker isolates, one per shard, defaulting to four. Vectors are assigned by `id % shardCount` in `_shardIndexForId`, so ids round robin across the pool and the index stays balanced without any rebalancing logic. Each shard runs `_shardEntryPoint`, owns a `_ShardState` and answers a small set of plain message classes: `_AddVectorsMessage`, `_SearchMessage`, `_CancelMessage`, `_RemoveMessage`, `_CompactMessage` and `_ShardCountMessage`. Every message carries only primitives and `TypedData`, so no custom codec is needed. `addAll` batches per shard into one flat buffer and ships it as `TransferableTypedData`, which moves the bytes instead of deep copying them.

Inside a shard, vectors live in `_GrowableMatrix`: one flat contiguous `Float32List` of `rows * dimension` floats that starts at 64 rows and doubles on demand, not a `List<Float32List>` of row objects. One allocation to scan instead of thousands keeps the hot loop cache friendly, and that is most of what makes a brute force scan this size fast enough to skip an ANN index. `dot` and `negSquaredEuclidean` walk the buffer directly, and euclidean is scored as negative squared distance so higher is always better and one comparison path serves all three metrics. For `DistanceMetric.cosine` the shard calls `normalizeRow` on every inserted row and the engine calls `_normalized` on the query, turning cosine similarity into a plain dot product. Top-k selection uses `_BoundedTopK`, a min-heap capped at k, so each candidate is an O(log k) `offer` against the heap root instead of a full sort at the end. The same heap merges the per shard results on the main isolate.

Cancellation is why `_ShardState.search` is async. It scans in chunks of 4096 rows and awaits `Future<void>.delayed(Duration.zero)` between them, which yields to that isolate's event loop and lets a queued `_CancelMessage` land in `cancelledRequestIds` before the scan finishes. Cancellation is best effort and bounded by chunk size, not instantaneous. On the caller's side a `VectorSearchCancellationToken` scopes cancellation to one logical lane. Calling `cancel`, or starting another `search` with the same token, supersedes whatever that token had in flight and completes the old future with `VectorSearchCancellationException`, without touching any other caller's concurrent query. A `timeout` does the same through a `Timer`.

Backpressure is a per shard concurrency cap plus a bounded queue. `_dispatchSearch` admits up to `maxInFlightPerShard` searches, parks the rest on a `Completer` in `waiters` and throws `VectorSearchBackpressureException` once `waiters` exceeds `maxQueuedPerShard`, so a slow shard sheds load instead of growing memory without limit. `_SearchDispatch` tracks whether a request is still queued or already handed to the isolate, because cancelling those two states is different work: a queued one is pulled out of `waiters`, a dispatched one gets a `_CancelMessage`.

Each shard is spawned with `errorsAreFatal: true` and both `onError` and `onExit` wired to the handle's own `ReceivePort`, so a crash reaches `_failAllPending`. Add, remove and count callers get a real error there because they need to know their write may not have landed. Search callers get `null`, so a mid flight crash degrades one shard's contribution instead of failing the whole query. `_handleShardDown` then respawns with exponential backoff, `200ms * 2^attempt` capped at 10 seconds, and fires `onShardLost` with exactly the ids that used to live there.

Deletes are tombstones. `remove` adds ids to a `Set<int>` per shard and the scan skips them, so removal is O(1) per id and does not shift the matrix under a running search. `compact` rebuilds the matrix from the surviving rows via `compactKeeping`. `stats()` fans out `_ShardCountMessage` to every live shard and returns live and tombstone counts, query and abort totals, backpressure rejections, respawn count and p50, p95 and p99 latency in microseconds over a rolling window of the last 512 queries.

## Usage

```dart
import 'dart:typed_data';

final engine = await IsolateVectorSearchEngine.spawn(
  dimension: 384,
  shardCount: 4,
  metric: DistanceMetric.cosine,   // or dotProduct, euclidean
  maxInFlightPerShard: 4,
  maxQueuedPerShard: 64,
);

// Bulk load: batched per shard into one flat buffer and moved with
// TransferableTypedData, so there is no per vector deep copy.
final ids = await engine.addAll(embeddings, tags: docIds);
final singleId = await engine.add(oneVector, tag: 'note-42');

// Recover a shard that died: the engine tells you exactly what was lost.
engine.onShardLost = (shardIndex, lostIds) {
  // re-add those vectors from your own store
};

// One token per search field or per logical lane. Each new search on the
// token cancels that caller's previous one and nothing else.
final token = VectorSearchCancellationToken();

try {
  final hits = await engine.search(
    queryVector,
    k: 10,
    timeout: const Duration(milliseconds: 250),
    cancelToken: token,
  );
  for (final hit in hits) {
    print('${hit.id} ${hit.score} ${hit.tag}');
  }
} on VectorSearchCancellationException {
  // superseded by a newer keystroke
} on VectorSearchTimeoutException {
  // slower than the budget
} on VectorSearchBackpressureException catch (e) {
  // shard ${e.shardIndex} queue full, shed this request
}

await engine.remove([singleId]);   // tombstone, O(1) per id
await engine.compact();            // reclaim the tombstoned rows

print(await engine.stats());       // counts, p50/p95/p99, respawns
await engine.dispose();
```

## Notes

- This is an exhaustive brute force scan, not an approximate index. No HNSW, no IVF, no quantization. Query cost is linear in vector count, so plan for thousands to low hundreds of thousands of rows per process, not tens of millions.
- Nothing is persisted. The index lives in isolate memory and is gone on process exit. You own the durable copy, and `onShardLost` exists so you can replay it after a shard dies. Respawn brings back an empty shard, so until you re-add the lost ids searches return partial results from the surviving shards rather than failing.
- Cancellation is bounded by the 4096 row chunk size, not instant. A cancelled scan can still burn one chunk of work before it notices.
- Ids come back as `Int32List`, so this assumes fewer than 2^31 total inserts over the life of the process. Ids are never reused after removal.
- Tags cross the isolate boundary, so they must be values Dart can send: primitives, strings, lists and maps of the same. No closures, no platform handles.
- Euclidean scores are negative squared distances, always at or below zero, and larger still means closer. Any public method after `dispose()` throws `VectorSearchClosedException`.
