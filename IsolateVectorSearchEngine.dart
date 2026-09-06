import 'dart:async';
import 'dart:isolate';
import 'dart:math' as math;
import 'dart:typed_data';

enum DistanceMetric { cosine, dotProduct, euclidean }

class VectorSearchResult {
  const VectorSearchResult({required this.id, required this.score, this.tag});

  final int id;
  final double score;
  final Object? tag;

  @override
  String toString() => 'VectorSearchResult(id: $id, score: $score, tag: $tag)';
}

class VectorSearchException implements Exception {
  VectorSearchException(this.message);
  final String message;
  @override
  String toString() => 'VectorSearchException: $message';
}

class VectorSearchDimensionMismatchException implements Exception {
  VectorSearchDimensionMismatchException(this.expected, this.actual);
  final int expected;
  final int actual;
  @override
  String toString() =>
      'VectorSearchDimensionMismatchException: expected dimension $expected, got $actual';
}

class VectorSearchBackpressureException implements Exception {
  VectorSearchBackpressureException(this.shardIndex, this.queueDepth);
  final int shardIndex;
  final int queueDepth;
  @override
  String toString() =>
      'VectorSearchBackpressureException: shard $shardIndex queue is full '
      '($queueDepth queued requests)';
}

class VectorSearchTimeoutException implements Exception {
  VectorSearchTimeoutException(this.timeout);
  final Duration timeout;
  @override
  String toString() =>
      'VectorSearchTimeoutException: no result within $timeout';
}

class VectorSearchClosedException implements Exception {
  @override
  String toString() => 'VectorSearchClosedException: engine has been disposed';
}

class VectorSearchCancellationException implements Exception {
  @override
  String toString() =>
      'VectorSearchCancellationException: request was cancelled';
}

/// Scopes cancellation to one logical caller: a search box, a live filter,
/// one agent's retrieval step. Calling [cancel], or starting another search
/// with the same token, stops whatever that token currently has in flight
/// without touching any other caller's concurrent query. Reuse one token
/// per UI field or per logical lane; do not share a token across unrelated
/// callers.
class VectorSearchCancellationToken {
  void Function()? _onSupersede;
  bool _hasLiveSearch = false;

  void cancel() {
    _onSupersede?.call();
    _onSupersede = null;
    _hasLiveSearch = false;
  }

  bool get hasLiveSearch => _hasLiveSearch;
}

class VectorSearchStats {
  const VectorSearchStats({
    required this.dimension,
    required this.shardCount,
    required this.shardsAlive,
    required this.vectorCount,
    required this.tombstoneCount,
    required this.totalQueries,
    required this.abortedQueries,
    required this.totalAdds,
    required this.totalRemovals,
    required this.backpressureRejections,
    required this.shardRespawns,
    required this.p50Micros,
    required this.p95Micros,
    required this.p99Micros,
  });

  final int dimension;
  final int shardCount;
  final int shardsAlive;
  final int vectorCount;
  final int tombstoneCount;
  final int totalQueries;
  final int abortedQueries;
  final int totalAdds;
  final int totalRemovals;
  final int backpressureRejections;
  final int shardRespawns;
  final int p50Micros;
  final int p95Micros;
  final int p99Micros;

  @override
  String toString() =>
      'VectorSearchStats(vectors: $vectorCount, tombstones: $tombstoneCount, '
      'shards: $shardsAlive/$shardCount, queries: $totalQueries '
      '(aborted: $abortedQueries), p50: ${p50Micros}us, p95: ${p95Micros}us, '
      'p99: ${p99Micros}us, respawns: $shardRespawns, '
      'rejected: $backpressureRejections)';
}

typedef ShardLostHandler = void Function(int shardIndex, List<int> lostIds);

// --- bounded top-k, shared by shard workers and the cross-shard merge -----

class _TopKEntry {
  _TopKEntry(this.id, this.score, this.tag);
  final int id;
  final double score;
  final Object? tag;
}

/// Min-heap capped at [capacity]. Keeps the [capacity] highest scores seen
/// so far in O(log k) per offer instead of collecting everything and
/// sorting once at the end, which matters once a shard holds tens of
/// thousands of rows.
class _BoundedTopK {
  _BoundedTopK(this.capacity)
    : _heap = List<_TopKEntry?>.filled(capacity, null);

  final int capacity;
  final List<_TopKEntry?> _heap;
  int _size = 0;

  void offer(int id, double score, Object? tag) {
    if (_size < capacity) {
      _heap[_size] = _TopKEntry(id, score, tag);
      _siftUp(_size);
      _size++;
      return;
    }
    if (capacity == 0 || score <= _heap[0]!.score) return;
    _heap[0] = _TopKEntry(id, score, tag);
    _siftDown(0);
  }

  void _siftUp(int index) {
    while (index > 0) {
      final parent = (index - 1) >> 1;
      if (_heap[parent]!.score <= _heap[index]!.score) break;
      final tmp = _heap[parent];
      _heap[parent] = _heap[index];
      _heap[index] = tmp;
      index = parent;
    }
  }

  void _siftDown(int index) {
    while (true) {
      final left = index * 2 + 1;
      final right = left + 1;
      var smallest = index;
      if (left < _size && _heap[left]!.score < _heap[smallest]!.score) {
        smallest = left;
      }
      if (right < _size && _heap[right]!.score < _heap[smallest]!.score) {
        smallest = right;
      }
      if (smallest == index) break;
      final tmp = _heap[smallest];
      _heap[smallest] = _heap[index];
      _heap[index] = tmp;
      index = smallest;
    }
  }

  List<_TopKEntry> drainSortedDescending() {
    final out = <_TopKEntry>[];
    for (var i = 0; i < _size; i++) {
      out.add(_heap[i]!);
    }
    out.sort((a, b) => b.score.compareTo(a.score));
    return out;
  }
}

// --- wire messages between the engine isolate and shard isolates ---------
//
// Every message is a plain class carrying only primitives and TypedData so
// it can cross the isolate boundary without a custom codec. Regular
// TypedData is deep-copied by the VM on send; TransferableTypedData is the
// one exception used below for bulk vector loads, where copying would be
// the expensive part.

class _ShardReady {
  _ShardReady(this.shardIndex, this.port);
  final int shardIndex;
  final SendPort port;
}

class _AddVectorsMessage {
  _AddVectorsMessage({
    required this.requestId,
    required this.transferable,
    required this.count,
    required this.ids,
    required this.tags,
  });
  final int requestId;
  final TransferableTypedData transferable;
  final int count;
  final List<int> ids;
  final List<Object?> tags;
}

class _AddAck {
  _AddAck(this.requestId);
  final int requestId;
}

class _SearchMessage {
  _SearchMessage({
    required this.requestId,
    required this.query,
    required this.k,
  });
  final int requestId;
  final Float32List query;
  final int k;
}

class _SearchResultMessage {
  _SearchResultMessage(
    this.requestId,
    this.ids,
    this.scores,
    this.tags,
    this.cancelled,
  );
  final int requestId;
  final Int32List ids;
  final Float64List scores;
  final List<Object?> tags;
  final bool cancelled;
}

class _CancelMessage {
  _CancelMessage(this.requestId);
  final int requestId;
}

class _RemoveMessage {
  _RemoveMessage(this.requestId, this.ids);
  final int requestId;
  final List<int> ids;
}

class _RemoveAck {
  _RemoveAck(this.requestId, this.removed);
  final int requestId;
  final int removed;
}

class _CompactMessage {
  _CompactMessage(this.requestId);
  final int requestId;
}

class _CompactAck {
  _CompactAck(this.requestId, this.remaining);
  final int requestId;
  final int remaining;
}

class _ShardCountMessage {
  _ShardCountMessage(this.requestId);
  final int requestId;
}

class _ShardCountAck {
  _ShardCountAck(this.requestId, this.liveCount, this.tombstoneCount);
  final int requestId;
  final int liveCount;
  final int tombstoneCount;
}

// --- shard isolate: owns one slice of the index ---------------------------

class _ShardInit {
  _ShardInit(this.shardIndex, this.dimension, this.metric, this.mainPort);
  final int shardIndex;
  final int dimension;
  final DistanceMetric metric;
  final SendPort mainPort;
}

/// A flat, contiguous buffer of `rows * dimension` floats instead of a
/// `List<Float32List>` of row objects. One allocation to scan instead of
/// thousands keeps the hot loop cache-friendly and avoids per-row overhead,
/// which is most of what makes a brute-force scan of this size fast enough
/// to be worth doing without an ANN index.
class _GrowableMatrix {
  _GrowableMatrix(this.dimension) : _data = Float32List(dimension * 64);

  final int dimension;
  Float32List _data;
  int _rows = 0;

  int get rows => _rows;

  void _ensureCapacityFor(int extraRows) {
    final needed = (_rows + extraRows) * dimension;
    if (needed <= _data.length) return;
    var newLength = _data.length;
    while (newLength < needed) {
      newLength *= 2;
    }
    final grown = Float32List(newLength);
    grown.setRange(0, _data.length, _data);
    _data = grown;
  }

  int appendBlock(Float32List flat, int rowCount) {
    _ensureCapacityFor(rowCount);
    final start = _rows * dimension;
    _data.setRange(start, start + rowCount * dimension, flat);
    final firstRow = _rows;
    _rows += rowCount;
    return firstRow;
  }

  void normalizeRow(int row) {
    final start = row * dimension;
    var sumSq = 0.0;
    for (var i = 0; i < dimension; i++) {
      final v = _data[start + i];
      sumSq += v * v;
    }
    if (sumSq <= 0) return;
    final invNorm = 1.0 / math.sqrt(sumSq);
    for (var i = 0; i < dimension; i++) {
      _data[start + i] *= invNorm;
    }
  }

  double dot(int row, Float32List query) {
    final start = row * dimension;
    var acc = 0.0;
    for (var i = 0; i < dimension; i++) {
      acc += _data[start + i] * query[i];
    }
    return acc;
  }

  double negSquaredEuclidean(int row, Float32List query) {
    final start = row * dimension;
    var acc = 0.0;
    for (var i = 0; i < dimension; i++) {
      final d = _data[start + i] - query[i];
      acc += d * d;
    }
    return -acc;
  }

  _GrowableMatrix compactKeeping(List<int> keepRows) {
    final out = _GrowableMatrix(dimension);
    if (keepRows.isEmpty) return out;
    final flat = Float32List(keepRows.length * dimension);
    for (var i = 0; i < keepRows.length; i++) {
      final srcStart = keepRows[i] * dimension;
      flat.setRange(i * dimension, (i + 1) * dimension, _data, srcStart);
    }
    out.appendBlock(flat, keepRows.length);
    return out;
  }
}

class _ShardState {
  _ShardState(this.dimension, this.metric)
    : matrix = _GrowableMatrix(dimension);

  final int dimension;
  final DistanceMetric metric;
  _GrowableMatrix matrix;
  final List<int> ids = <int>[];
  final List<Object?> tags = <Object?>[];
  final Set<int> tombstones = <int>{};
  final Set<int> cancelledRequestIds = <int>{};

  double scoreOf(int row, Float32List query) {
    switch (metric) {
      case DistanceMetric.cosine:
      case DistanceMetric.dotProduct:
        return matrix.dot(row, query);
      case DistanceMetric.euclidean:
        return matrix.negSquaredEuclidean(row, query);
    }
  }

  /// Scans in chunks and yields to the event loop between them so a
  /// `_CancelMessage` sitting in this isolate's mailbox gets a chance to be
  /// processed and observed here before the scan finishes. Cancellation is
  /// therefore best-effort and bounded by chunk size, not instantaneous.
  Future<_SearchResultMessage> search(
    int requestId,
    Float32List query,
    int k,
  ) async {
    final heap = _BoundedTopK(k);
    const chunkSize = 4096;
    var i = 0;
    final total = matrix.rows;
    while (i < total) {
      final end = math.min(i + chunkSize, total);
      for (var row = i; row < end; row++) {
        final id = ids[row];
        if (tombstones.contains(id)) continue;
        heap.offer(id, scoreOf(row, query), tags[row]);
      }
      i = end;
      if (cancelledRequestIds.remove(requestId)) {
        return _SearchResultMessage(
          requestId,
          Int32List(0),
          Float64List(0),
          const [],
          true,
        );
      }
      if (i < total) await Future<void>.delayed(Duration.zero);
    }
    if (cancelledRequestIds.remove(requestId)) {
      return _SearchResultMessage(
        requestId,
        Int32List(0),
        Float64List(0),
        const [],
        true,
      );
    }
    final sorted = heap.drainSortedDescending();
    final resultIds = Int32List(sorted.length);
    final resultScores = Float64List(sorted.length);
    final resultTags = List<Object?>.filled(sorted.length, null);
    for (var j = 0; j < sorted.length; j++) {
      resultIds[j] = sorted[j].id;
      resultScores[j] = sorted[j].score;
      resultTags[j] = sorted[j].tag;
    }
    return _SearchResultMessage(
      requestId,
      resultIds,
      resultScores,
      resultTags,
      false,
    );
  }

  void addBlock(_AddVectorsMessage msg) {
    final flat = msg.transferable.materialize().asFloat32List();
    final firstRow = matrix.appendBlock(flat, msg.count);
    ids.addAll(msg.ids);
    tags.addAll(msg.tags);
    if (metric == DistanceMetric.cosine) {
      for (var r = 0; r < msg.count; r++) {
        matrix.normalizeRow(firstRow + r);
      }
    }
  }

  int removeIds(List<int> toRemove) {
    var removed = 0;
    for (final id in toRemove) {
      if (tombstones.add(id)) removed++;
    }
    return removed;
  }

  int compact() {
    final keepRows = <int>[];
    final keepIds = <int>[];
    final keepTags = <Object?>[];
    for (var row = 0; row < ids.length; row++) {
      if (!tombstones.contains(ids[row])) {
        keepRows.add(row);
        keepIds.add(ids[row]);
        keepTags.add(tags[row]);
      }
    }
    matrix = matrix.compactKeeping(keepRows);
    ids
      ..clear()
      ..addAll(keepIds);
    tags
      ..clear()
      ..addAll(keepTags);
    tombstones.clear();
    return ids.length;
  }
}

void _shardEntryPoint(_ShardInit init) {
  final receivePort = ReceivePort();
  init.mainPort.send(_ShardReady(init.shardIndex, receivePort.sendPort));
  final state = _ShardState(init.dimension, init.metric);

  receivePort.listen((dynamic message) {
    if (message is _AddVectorsMessage) {
      state.addBlock(message);
      init.mainPort.send(_AddAck(message.requestId));
    } else if (message is _SearchMessage) {
      state.search(message.requestId, message.query, message.k).then((result) {
        init.mainPort.send(result);
      });
    } else if (message is _CancelMessage) {
      state.cancelledRequestIds.add(message.requestId);
    } else if (message is _RemoveMessage) {
      final removed = state.removeIds(message.ids);
      init.mainPort.send(_RemoveAck(message.requestId, removed));
    } else if (message is _CompactMessage) {
      final remaining = state.compact();
      init.mainPort.send(_CompactAck(message.requestId, remaining));
    } else if (message is _ShardCountMessage) {
      init.mainPort.send(
        _ShardCountAck(
          message.requestId,
          state.ids.length - state.tombstones.length,
          state.tombstones.length,
        ),
      );
    }
  });
}

// --- main-isolate side handle for one shard -------------------------------

class _PendingSearch {
  _PendingSearch(this.completer);
  final Completer<_SearchResultMessage?> completer;
}

/// Tracks one outstanding search request from the engine's side across its
/// two possible states: queued behind the per-shard concurrency cap (
/// [queueGate] set), or already handed to the shard isolate ([queueGate]
/// null). Cancellation has to handle both without leaking a queue slot or
/// double-counting [ _ShardHandle.inFlightSearchCount ].
class _SearchDispatch {
  _SearchDispatch(this.shardIndex, this.requestId);
  final int shardIndex;
  final int requestId;
  bool cancelled = false;
  Completer<void>? queueGate;
}

class _ShardHandle {
  _ShardHandle(this.shardIndex, this.dimension, this.metric, this.maxInFlight);

  final int shardIndex;
  final int dimension;
  final DistanceMetric metric;
  final int maxInFlight;

  Isolate? _isolate;
  SendPort? _sendPort;
  ReceivePort? _receivePort;
  StreamSubscription<dynamic>? _subscription;
  bool disposed = false;
  bool alive = false;
  int respawnAttempts = 0;

  final Map<int, Completer<void>> _addAcks = <int, Completer<void>>{};
  final Map<int, Completer<int>> _removeAcks = <int, Completer<int>>{};
  final Map<int, Completer<Map<String, int>>> _countAcks =
      <int, Completer<Map<String, int>>>{};
  final Map<int, _PendingSearch> _pendingSearches = <int, _PendingSearch>{};

  int inFlightSearchCount = 0;
  final List<Completer<void>> waiters = <Completer<void>>[];

  void Function(int shardIndex, String reason)? onError;
  void Function(int shardIndex, String reason)? onExit;

  Future<void> start() async {
    final ready = Completer<_ShardReady>();
    final rp = ReceivePort();
    _receivePort = rp;
    _subscription = rp.listen((dynamic message) {
      if (message is _ShardReady) {
        ready.complete(message);
      } else if (message is _AddAck) {
        _addAcks.remove(message.requestId)?.complete();
      } else if (message is _RemoveAck) {
        _removeAcks.remove(message.requestId)?.complete(message.removed);
      } else if (message is _CompactAck) {
        _removeAcks.remove(message.requestId)?.complete(message.remaining);
      } else if (message is _ShardCountAck) {
        _countAcks.remove(message.requestId)?.complete(<String, int>{
          'live': message.liveCount,
          'tombstones': message.tombstoneCount,
        });
      } else if (message is _SearchResultMessage) {
        final pending = _pendingSearches.remove(message.requestId);
        pending?.completer.complete(message.cancelled ? null : message);
        inFlightSearchCount--;
        _drainWaiters();
      } else if (message == null) {
        // errorsAreFatal means a crash sends the error message and then
        // this exit message; `alive` is already false by the time the
        // second one arrives, so it is treated as cleanup, not a new crash.
        if (!disposed && alive) {
          alive = false;
          _failAllPending('shard $shardIndex exited unexpectedly');
          onExit?.call(shardIndex, 'unexpected exit');
        }
        _subscription?.cancel();
        _receivePort?.close();
      } else if (message is List && message.length == 2) {
        alive = false;
        final reason = message[0].toString();
        _failAllPending('shard $shardIndex crashed: $reason');
        onError?.call(shardIndex, reason);
      }
    });
    _isolate = await Isolate.spawn<_ShardInit>(
      _shardEntryPoint,
      _ShardInit(shardIndex, dimension, metric, rp.sendPort),
      onError: rp.sendPort,
      onExit: rp.sendPort,
      errorsAreFatal: true,
    );
    final readyMsg = await ready.future;
    _sendPort = readyMsg.port;
    alive = true;
    respawnAttempts = 0;
  }

  void _drainWaiters() {
    while (waiters.isNotEmpty && inFlightSearchCount < maxInFlight) {
      final gate = waiters.removeAt(0);
      if (!gate.isCompleted) gate.complete();
    }
  }

  /// A dead shard leaves in-flight requests with no isolate left to answer
  /// them. Add/remove callers get a real error (they need to know their
  /// write may not have landed); search callers get `null` so a mid-flight
  /// crash degrades one shard's contribution to a query instead of failing
  /// the whole search.
  void _failAllPending(String reason) {
    final error = VectorSearchException(reason);
    for (final c in _addAcks.values) {
      if (!c.isCompleted) c.completeError(error);
    }
    _addAcks.clear();
    for (final c in _removeAcks.values) {
      if (!c.isCompleted) c.completeError(error);
    }
    _removeAcks.clear();
    for (final c in _countAcks.values) {
      if (!c.isCompleted) c.completeError(error);
    }
    _countAcks.clear();
    for (final p in _pendingSearches.values) {
      if (!p.completer.isCompleted) p.completer.complete(null);
    }
    _pendingSearches.clear();
    inFlightSearchCount = 0;
    for (final w in waiters) {
      if (!w.isCompleted) w.complete();
    }
    waiters.clear();
  }

  Future<void> stop() async {
    disposed = true;
    await _subscription?.cancel();
    _isolate?.kill(priority: Isolate.immediate);
    _receivePort?.close();
    _failAllPending('engine disposed');
  }
}

// --- the public engine -----------------------------------------------------

class IsolateVectorSearchEngine {
  IsolateVectorSearchEngine._(
    this.dimension,
    this.metric,
    this._shards,
    this.maxInFlightPerShard,
    this.maxQueuedPerShard,
  );

  final int dimension;
  final DistanceMetric metric;
  final List<_ShardHandle> _shards;
  final int maxInFlightPerShard;
  final int maxQueuedPerShard;

  int _nextId = 0;
  int _nextRequestId = 0;
  bool _closed = false;

  final List<Set<int>> _liveIdsPerShard = <Set<int>>[];
  final List<int> _recentLatenciesMicros = <int>[];
  static const int _latencyWindow = 512;

  int _totalQueries = 0;
  int _abortedQueries = 0;
  int _totalAdds = 0;
  int _totalRemovals = 0;
  int _backpressureRejections = 0;
  int _shardRespawns = 0;

  ShardLostHandler? onShardLost;

  static Future<IsolateVectorSearchEngine> spawn({
    required int dimension,
    int shardCount = 4,
    DistanceMetric metric = DistanceMetric.cosine,
    int maxInFlightPerShard = 4,
    int maxQueuedPerShard = 64,
  }) async {
    if (dimension <= 0) {
      throw ArgumentError.value(dimension, 'dimension', 'must be positive');
    }
    if (shardCount <= 0) {
      throw ArgumentError.value(shardCount, 'shardCount', 'must be positive');
    }
    final shards = <_ShardHandle>[];
    final engine = IsolateVectorSearchEngine._(
      dimension,
      metric,
      shards,
      maxInFlightPerShard,
      maxQueuedPerShard,
    );
    for (var i = 0; i < shardCount; i++) {
      final shard = _ShardHandle(i, dimension, metric, maxInFlightPerShard);
      shards.add(shard);
      engine._liveIdsPerShard.add(<int>{});
      shard.onError = engine._handleShardDown;
      shard.onExit = engine._handleShardDown;
      await shard.start();
    }
    return engine;
  }

  void _handleShardDown(int shardIndex, String reason) {
    if (_closed) return;
    final lostIds = _liveIdsPerShard[shardIndex].toList(growable: false);
    _liveIdsPerShard[shardIndex].clear();
    onShardLost?.call(shardIndex, lostIds);
    _shardRespawns++;
    final attempt = _shards[shardIndex].respawnAttempts + 1;
    final backoff = Duration(
      milliseconds: math.min(200 * (1 << math.min(attempt, 6)), 10000),
    );
    Future<void>.delayed(backoff, () async {
      if (_closed) return;
      final replacement = _ShardHandle(
        shardIndex,
        dimension,
        metric,
        maxInFlightPerShard,
      )..respawnAttempts = attempt;
      replacement.onError = _handleShardDown;
      replacement.onExit = _handleShardDown;
      try {
        await replacement.start();
        _shards[shardIndex] = replacement;
      } catch (_) {
        _handleShardDown(shardIndex, 'respawn failed');
      }
    });
  }

  int _shardIndexForId(int id) => id % _shards.length;

  Future<int> add(Float32List vector, {Object? tag}) async {
    final ids = await addAll(<Float32List>[vector], tags: <Object?>[tag]);
    return ids.first;
  }

  Future<List<int>> addAll(
    Iterable<Float32List> vectors, {
    List<Object?>? tags,
  }) async {
    _ensureOpen();
    final vectorList = vectors.toList(growable: false);
    if (tags != null && tags.length != vectorList.length) {
      throw ArgumentError('tags length must match vectors length');
    }
    for (final v in vectorList) {
      if (v.length != dimension) {
        throw VectorSearchDimensionMismatchException(dimension, v.length);
      }
    }
    final byShard = <int, List<int>>{};
    final assignedIds = List<int>.filled(vectorList.length, 0);
    for (var i = 0; i < vectorList.length; i++) {
      final id = _nextId++;
      assignedIds[i] = id;
      byShard.putIfAbsent(_shardIndexForId(id), () => <int>[]).add(i);
    }

    final futures = <Future<void>>[];
    for (final entry in byShard.entries) {
      final shardIndex = entry.key;
      final localIndexes = entry.value;
      final flat = Float32List(localIndexes.length * dimension);
      final ids = <int>[];
      final shardTags = <Object?>[];
      for (var row = 0; row < localIndexes.length; row++) {
        final srcIndex = localIndexes[row];
        flat.setRange(
          row * dimension,
          (row + 1) * dimension,
          vectorList[srcIndex],
        );
        final id = assignedIds[srcIndex];
        ids.add(id);
        shardTags.add(tags?[srcIndex]);
        _liveIdsPerShard[shardIndex].add(id);
      }
      final requestId = _nextRequestId++;
      final shard = _shards[shardIndex];
      final completer = Completer<void>();
      shard._addAcks[requestId] = completer;
      shard._sendPort!.send(
        _AddVectorsMessage(
          requestId: requestId,
          transferable: TransferableTypedData.fromList(<TypedData>[flat]),
          count: localIndexes.length,
          ids: ids,
          tags: shardTags,
        ),
      );
      futures.add(completer.future);
    }
    await Future.wait(futures);
    _totalAdds += vectorList.length;
    return assignedIds;
  }

  Future<int> remove(Iterable<int> ids) async {
    _ensureOpen();
    final byShard = <int, List<int>>{};
    for (final id in ids) {
      byShard.putIfAbsent(_shardIndexForId(id), () => <int>[]).add(id);
    }
    final futures = <Future<int>>[];
    for (final entry in byShard.entries) {
      final shard = _shards[entry.key];
      final requestId = _nextRequestId++;
      final completer = Completer<int>();
      shard._removeAcks[requestId] = completer;
      shard._sendPort!.send(_RemoveMessage(requestId, entry.value));
      for (final id in entry.value) {
        _liveIdsPerShard[entry.key].remove(id);
      }
      futures.add(completer.future);
    }
    final counts = await Future.wait(futures);
    final removed = counts.fold<int>(0, (a, b) => a + b);
    _totalRemovals += removed;
    return removed;
  }

  Future<void> compact() async {
    _ensureOpen();
    final futures = <Future<int>>[];
    for (final shard in _shards) {
      final requestId = _nextRequestId++;
      final completer = Completer<int>();
      shard._removeAcks[requestId] = completer;
      shard._sendPort!.send(_CompactMessage(requestId));
      futures.add(completer.future);
    }
    await Future.wait(futures);
  }

  Future<List<VectorSearchResult>> search(
    Float32List query, {
    int k = 10,
    Duration? timeout,
    VectorSearchCancellationToken? cancelToken,
  }) async {
    _ensureOpen();
    if (query.length != dimension) {
      throw VectorSearchDimensionMismatchException(dimension, query.length);
    }
    if (k <= 0) throw ArgumentError.value(k, 'k', 'must be positive');

    cancelToken?.cancel(); // supersede whatever this token had in flight

    final normalizedQuery = metric == DistanceMetric.cosine
        ? _normalized(query)
        : query;
    final stopwatch = Stopwatch()..start();
    final dispatches = <_SearchDispatch>[];
    final futures = <Future<_SearchResultMessage?>>[];

    for (var shardIndex = 0; shardIndex < _shards.length; shardIndex++) {
      final shard = _shards[shardIndex];
      if (!shard.alive) continue;
      final dispatch = _SearchDispatch(shardIndex, _nextRequestId++);
      dispatches.add(dispatch);
      futures.add(_dispatchSearch(shard, dispatch, normalizedQuery, k));
    }

    void stopDispatches() {
      for (final dispatch in dispatches) {
        dispatch.cancelled = true;
        final gate = dispatch.queueGate;
        if (gate != null) {
          _shards[dispatch.shardIndex].waiters.remove(gate);
          if (!gate.isCompleted) gate.complete();
        } else {
          _shards[dispatch.shardIndex]._sendPort?.send(
            _CancelMessage(dispatch.requestId),
          );
        }
      }
    }

    final resultCompleter = Completer<List<VectorSearchResult>>();
    void fail(Object error) {
      if (!resultCompleter.isCompleted) resultCompleter.completeError(error);
    }

    void onSupersede() {
      stopDispatches();
      _abortedQueries++;
      fail(VectorSearchCancellationException());
    }

    cancelToken?._onSupersede = onSupersede;
    cancelToken?._hasLiveSearch = true;

    Timer? timer;
    if (timeout != null) {
      timer = Timer(timeout, () {
        stopDispatches();
        _abortedQueries++;
        fail(VectorSearchTimeoutException(timeout));
      });
    }

    Future.wait(futures).then((results) {
      if (resultCompleter.isCompleted) return;
      final merged = _BoundedTopK(k);
      for (final r in results) {
        if (r == null) continue;
        for (var i = 0; i < r.ids.length; i++) {
          merged.offer(r.ids[i], r.scores[i], r.tags[i]);
        }
      }
      resultCompleter.complete(
        merged
            .drainSortedDescending()
            .map(
              (e) => VectorSearchResult(id: e.id, score: e.score, tag: e.tag),
            )
            .toList(growable: false),
      );
    }, onError: fail);

    try {
      final result = await resultCompleter.future;
      _totalQueries++;
      stopwatch.stop();
      _recordLatency(stopwatch.elapsedMicroseconds);
      return result;
    } finally {
      timer?.cancel();
      if (identical(cancelToken?._onSupersede, onSupersede)) {
        cancelToken?._onSupersede = null;
        cancelToken?._hasLiveSearch = false;
      }
    }
  }

  Future<_SearchResultMessage?> _dispatchSearch(
    _ShardHandle shard,
    _SearchDispatch dispatch,
    Float32List query,
    int k,
  ) async {
    if (shard.inFlightSearchCount >= shard.maxInFlight) {
      if (shard.waiters.length >= maxQueuedPerShard) {
        _backpressureRejections++;
        throw VectorSearchBackpressureException(
          shard.shardIndex,
          shard.waiters.length,
        );
      }
      final gate = Completer<void>();
      dispatch.queueGate = gate;
      shard.waiters.add(gate);
      await gate.future;
      dispatch.queueGate = null;
    }
    if (dispatch.cancelled) return null;
    shard.inFlightSearchCount++;
    final completer = Completer<_SearchResultMessage?>();
    shard._pendingSearches[dispatch.requestId] = _PendingSearch(completer);
    shard._sendPort?.send(
      _SearchMessage(requestId: dispatch.requestId, query: query, k: k),
    );
    return completer.future;
  }

  Float32List _normalized(Float32List v) {
    var sumSq = 0.0;
    for (final x in v) {
      sumSq += x * x;
    }
    if (sumSq <= 0) return v;
    final invNorm = 1.0 / math.sqrt(sumSq);
    final out = Float32List(v.length);
    for (var i = 0; i < v.length; i++) {
      out[i] = v[i] * invNorm;
    }
    return out;
  }

  void _recordLatency(int micros) {
    _recentLatenciesMicros.add(micros);
    if (_recentLatenciesMicros.length > _latencyWindow) {
      _recentLatenciesMicros.removeAt(0);
    }
  }

  int _percentile(double p) {
    if (_recentLatenciesMicros.isEmpty) return 0;
    final sorted = List<int>.from(_recentLatenciesMicros)..sort();
    final index = ((sorted.length - 1) * p).round().clamp(0, sorted.length - 1);
    return sorted[index];
  }

  Future<VectorSearchStats> stats() async {
    var vectorCount = 0;
    var tombstoneCount = 0;
    var shardsAlive = 0;
    final futures = <Future<Map<String, int>>>[];
    for (final shard in _shards) {
      if (!shard.alive) continue;
      shardsAlive++;
      final requestId = _nextRequestId++;
      final completer = Completer<Map<String, int>>();
      shard._countAcks[requestId] = completer;
      shard._sendPort?.send(_ShardCountMessage(requestId));
      futures.add(completer.future);
    }
    final counts = await Future.wait(futures);
    for (final c in counts) {
      vectorCount += c['live'] ?? 0;
      tombstoneCount += c['tombstones'] ?? 0;
    }
    return VectorSearchStats(
      dimension: dimension,
      shardCount: _shards.length,
      shardsAlive: shardsAlive,
      vectorCount: vectorCount,
      tombstoneCount: tombstoneCount,
      totalQueries: _totalQueries,
      abortedQueries: _abortedQueries,
      totalAdds: _totalAdds,
      totalRemovals: _totalRemovals,
      backpressureRejections: _backpressureRejections,
      shardRespawns: _shardRespawns,
      p50Micros: _percentile(0.50),
      p95Micros: _percentile(0.95),
      p99Micros: _percentile(0.99),
    );
  }

  void _ensureOpen() {
    if (_closed) throw VectorSearchClosedException();
  }

  Future<void> dispose() async {
    if (_closed) return;
    _closed = true;
    await Future.wait(_shards.map((s) => s.stop()));
  }
}

/*
This solves the problem of running semantic vector search inside a Dart or
Flutter app without freezing the UI and without copying huge embedding
buffers around every time you touch them. On-device RAG, local search over
notes or chat history, and offline-first AI features all need a way to hold
thousands to a few hundred thousand float vectors and scan them fast, and
none of that can happen on the isolate driving your widgets or your request
handler without causing jank or stalling other work.

Built because every simple version of this I have seen makes one of two
mistakes. Either it runs the whole scan on the main isolate and stalls the
UI or the request handler the moment the index gets big, or it throws work
at a background isolate but copies every vector across the isolate boundary
by value, which gets expensive fast once you are loading a real embedding
count. This engine spreads the index across a pool of persistent worker
isolates, uses TransferableTypedData so a bulk load of vectors moves into a
shard without the usual per-message deep copy, and keeps each shard
scanning in small chunks so a long search can be told to stop instead of
running to completion after nobody wants the answer anymore. It also treats
isolate death as a normal event instead of a fatal one. If a shard crashes
mid-scan, the engine respawns it with backoff and calls back with exactly
which ids used to live there, so the app can push that slice back in from
its own store instead of losing part of the index silently.

Use it when you are building semantic search, RAG retrieval, similarity
matching, recommendation lookups, or duplicate detection in a Dart backend
or a Flutter app, especially anywhere new results should invalidate old
ones, like a live search box, an autocomplete field, or a chat app doing
retrieval on every keystroke. The cancellation token model matters here:
starting a new search on the same token cancels that caller's own stale
work and rejects its future with a clear cancellation error, without ever
touching another caller's concurrent query. A busy multi-user server and a
single search box behave correctly under the exact same engine and the
exact same code path.

The trick is keeping three concerns that most simple implementations mash
together properly separate. Bulk transfer cost is handled by batching adds
per shard into one flat buffer and moving it with TransferableTypedData
instead of copying it. Cancellation is explicit and scoped to a token, so
it never silently drops a request nobody asked to cancel, and it always
resolves the caller's future instead of leaving it hanging. Backpressure
caps how many searches can be in flight per shard, queues a bounded number
past that cap, and rejects fast once the queue itself is full, so a slow
shard degrades instead of letting memory grow without limit. Every one of
those paths, including a mid-flight shard crash, is written to release its
slot and resolve its caller exactly once, because a search engine that
occasionally hangs a caller forever is worse than one that is merely slow.

Drop this into a Flutter app doing on-device embeddings search, a Dart
server handling retrieval for a RAG pipeline, a CLI tool indexing local
documents, or any Dart process that needs approximate or exact nearest
neighbor search over vectors without pulling in a native dependency. If you
are searching for Dart vector search, Flutter on-device RAG, Dart isolate
vector database, TransferableTypedData example, Dart cosine similarity
search, Dart embeddings index, or Flutter semantic search without a
server, this file is meant to be the thing you fork and wire straight into
your app rather than a toy example you have to rebuild from scratch.
*/
