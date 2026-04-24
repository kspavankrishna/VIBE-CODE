import 'dart:async';
import 'dart:collection';

typedef BatchRunner<TInput, TResult>
    = Future<List<BatchItemResult<TResult>>> Function(
        List<BatchRequest<TInput>> items,
        BatchExecutionContext context,
      );

typedef BatchWeightEstimator<TInput> = int Function(TInput item);

class AdaptiveMicroBatcher<TInput, TResult> {
  AdaptiveMicroBatcher({
    required BatchRunner<TInput, TResult> runBatch,
    BatchWeightEstimator<TInput>? estimateWeight,
    DateTime Function()? clock,
    this.maxBatchItems = 64,
    this.maxBatchWeight = 64 * 1024,
    this.maxBatchLatency = const Duration(milliseconds: 25),
    this.maxConcurrentBatches = 2,
    this.maxQueueItems = 4096,
    this.maxQueueWeight = 8 * 1024 * 1024,
    this.queueOverflowStrategy = QueueOverflowStrategy.rejectNewest,
    this.failureIsolation = BatchFailureIsolation.splitInHalf,
  }) : _runBatch = runBatch,
       _estimateWeight = estimateWeight ?? ((_) => 1),
       _clock = clock ?? _defaultClock {
    if (maxBatchItems < 1) {
      throw ArgumentError.value(
        maxBatchItems,
        'maxBatchItems',
        'maxBatchItems must be positive.',
      );
    }
    if (maxBatchWeight < 1) {
      throw ArgumentError.value(
        maxBatchWeight,
        'maxBatchWeight',
        'maxBatchWeight must be positive.',
      );
    }
    if (maxBatchLatency < Duration.zero) {
      throw ArgumentError.value(
        maxBatchLatency,
        'maxBatchLatency',
        'maxBatchLatency must be non-negative.',
      );
    }
    if (maxConcurrentBatches < 1) {
      throw ArgumentError.value(
        maxConcurrentBatches,
        'maxConcurrentBatches',
        'maxConcurrentBatches must be positive.',
      );
    }
    if (maxQueueItems < maxBatchItems) {
      throw ArgumentError.value(
        maxQueueItems,
        'maxQueueItems',
        'maxQueueItems must be greater than or equal to maxBatchItems.',
      );
    }
    if (maxQueueWeight < maxBatchWeight) {
      throw ArgumentError.value(
        maxQueueWeight,
        'maxQueueWeight',
        'maxQueueWeight must be greater than or equal to maxBatchWeight.',
      );
    }
  }

  static DateTime _defaultClock() => DateTime.now().toUtc();

  final BatchRunner<TInput, TResult> _runBatch;
  final BatchWeightEstimator<TInput> _estimateWeight;
  final DateTime Function() _clock;
  final Stopwatch _stopwatch = Stopwatch()..start();
  final Queue<_PendingRequest<TInput, TResult>> _queue =
      Queue<_PendingRequest<TInput, TResult>>();
  final Completer<void> _closedCompleter = Completer<void>();

  final int maxBatchItems;
  final int maxBatchWeight;
  final Duration maxBatchLatency;
  final int maxConcurrentBatches;
  final int maxQueueItems;
  final int maxQueueWeight;
  final QueueOverflowStrategy queueOverflowStrategy;
  final BatchFailureIsolation failureIsolation;

  Timer? _timer;
  bool _closed = false;
  bool _drainOnClose = true;
  bool _isPumping = false;
  bool _pumpRequested = false;
  int _nextSequence = 1;
  int _nextBatchId = 1;
  int _queuedWeight = 0;
  int _inFlightBatches = 0;

  int _submittedCount = 0;
  int _rootBatchCount = 0;
  int _batchInvocationCount = 0;
  int _splitInvocationCount = 0;
  int _splitBatchCount = 0;
  int _successCount = 0;
  int _failureCount = 0;
  int _expiredCount = 0;
  int _droppedCount = 0;
  int _rejectedCount = 0;
  int _totalQueueWaitMicros = 0;
  int _totalExecutionMicros = 0;
  int _maxObservedQueueDepth = 0;
  int _maxObservedQueueWeight = 0;

  bool get isClosed => _closed;

  Future<void> get whenClosed => _closedCompleter.future;

  Future<TResult> submit(
    TInput input, {
    int? weight,
    Duration? maxQueueDelay,
    Object? metadata,
    String? traceId,
  }) {
    if (_closed) {
      throw BatchClosedException(
        'AdaptiveMicroBatcher is closed and cannot accept new items.',
      );
    }
    if (maxQueueDelay != null && maxQueueDelay < Duration.zero) {
      throw ArgumentError.value(
        maxQueueDelay,
        'maxQueueDelay',
        'maxQueueDelay must be non-negative when provided.',
      );
    }

    final resolvedWeight = weight ?? _estimateWeight(input);
    if (resolvedWeight < 1) {
      throw BatchWeightExceededException(
        'Request weight must be positive. Got $resolvedWeight.',
      );
    }
    if (resolvedWeight > maxBatchWeight) {
      throw BatchWeightExceededException(
        'Request weight $resolvedWeight exceeds maxBatchWeight $maxBatchWeight.',
      );
    }
    if (resolvedWeight > maxQueueWeight) {
      throw BatchWeightExceededException(
        'Request weight $resolvedWeight exceeds maxQueueWeight $maxQueueWeight.',
      );
    }

    _makeRoomFor(resolvedWeight);

    final now = _elapsedMicros();
    final pending = _PendingRequest<TInput, TResult>(
      sequence: _nextSequence++,
      input: input,
      enqueuedAt: _clock(),
      enqueuedAtMicros: now,
      dispatchAtMicros: now + maxBatchLatency.inMicroseconds,
      expiresAtMicros: maxQueueDelay == null
          ? null
          : now + maxQueueDelay.inMicroseconds,
      maxQueueDelay: maxQueueDelay,
      weight: resolvedWeight,
      metadata: metadata,
      traceId: traceId,
    );

    _queue.addLast(pending);
    _queuedWeight += resolvedWeight;
    _submittedCount++;
    _maxObservedQueueDepth = _queue.length > _maxObservedQueueDepth
        ? _queue.length
        : _maxObservedQueueDepth;
    _maxObservedQueueWeight = _queuedWeight > _maxObservedQueueWeight
        ? _queuedWeight
        : _maxObservedQueueWeight;

    _pump();
    return pending.completer.future;
  }

  Future<void> close({bool drain = true}) {
    if (_closed) {
      return _closedCompleter.future;
    }

    _closed = true;
    _drainOnClose = drain;

    if (!drain) {
      _failQueued(
        BatchClosedException(
          'AdaptiveMicroBatcher closed before queued work was dispatched.',
        ),
      );
    }

    _pump();
    return _closedCompleter.future;
  }

  MicroBatcherSnapshot snapshot() {
    return MicroBatcherSnapshot(
      isClosed: _closed,
      queuedItems: _queue.length,
      queuedWeight: _queuedWeight,
      inFlightBatches: _inFlightBatches,
      maxObservedQueueDepth: _maxObservedQueueDepth,
      maxObservedQueueWeight: _maxObservedQueueWeight,
      totalSubmitted: _submittedCount,
      totalRootBatches: _rootBatchCount,
      totalBatchInvocations: _batchInvocationCount,
      totalSplitInvocations: _splitInvocationCount,
      totalSplitBatches: _splitBatchCount,
      totalSucceeded: _successCount,
      totalFailed: _failureCount,
      totalExpired: _expiredCount,
      totalDropped: _droppedCount,
      totalRejected: _rejectedCount,
      totalQueueWaitMicros: _totalQueueWaitMicros,
      totalExecutionMicros: _totalExecutionMicros,
    );
  }

  int _elapsedMicros() => _stopwatch.elapsedMicroseconds;

  void _makeRoomFor(int additionalWeight) {
    while (_queue.length >= maxQueueItems ||
        _queuedWeight + additionalWeight > maxQueueWeight) {
      if (queueOverflowStrategy == QueueOverflowStrategy.rejectNewest ||
          _queue.isEmpty) {
        _rejectedCount++;
        throw BatchQueueFullException(
          queuedItems: _queue.length,
          maxQueueItems: maxQueueItems,
          queuedWeight: _queuedWeight,
          maxQueueWeight: maxQueueWeight,
          additionalWeight: additionalWeight,
        );
      }

      final dropped = _queue.removeFirst();
      _queuedWeight -= dropped.weight;
      _droppedCount++;
      _failPending(
        dropped,
        BatchQueueFullException(
          queuedItems: _queue.length,
          maxQueueItems: maxQueueItems,
          queuedWeight: _queuedWeight,
          maxQueueWeight: maxQueueWeight,
          additionalWeight: additionalWeight,
          message:
              'Dropped oldest queued request to make room for newer work.',
        ),
      );
    }
  }

  void _pump() {
    if (_isPumping) {
      _pumpRequested = true;
      return;
    }

    _isPumping = true;
    try {
      _timer?.cancel();
      _timer = null;

      var now = _elapsedMicros();
      _pruneExpired(now);

      while (_inFlightBatches < maxConcurrentBatches) {
        final selection = _selectBatch(now);
        if (selection == null) {
          break;
        }

        final items = _dequeue(selection.count);
        _dispatch(selection.reason, items);
        now = _elapsedMicros();
        _pruneExpired(now);
      }

      _scheduleNextWakeup();
      _completeCloseIfReady();
    } finally {
      _isPumping = false;
      if (_pumpRequested) {
        _pumpRequested = false;
        scheduleMicrotask(_pump);
      }
    }
  }

  void _dispatch(
    BatchDispatchReason reason,
    List<_PendingRequest<TInput, TResult>> items,
  ) {
    if (items.isEmpty) {
      return;
    }

    _inFlightBatches++;
    _rootBatchCount++;
    final batchId = _nextBatchId++;
    final nowMicros = _elapsedMicros();
    final dispatchedAt = _clock();

    for (final item in items) {
      _totalQueueWaitMicros += nowMicros - item.enqueuedAtMicros;
    }

    final context = BatchExecutionContext(
      batchId: batchId,
      rootBatchId: batchId,
      dispatchReason: reason,
      dispatchedAt: dispatchedAt,
      oldestEnqueuedAt: items.first.enqueuedAt,
      newestEnqueuedAt: items.last.enqueuedAt,
      itemCount: items.length,
      totalWeight: _sumWeight(items),
      splitDepth: 0,
    );

    unawaited(_runRootBatch(items, context));
  }

  Future<void> _runRootBatch(
    List<_PendingRequest<TInput, TResult>> items,
    BatchExecutionContext context,
  ) async {
    try {
      await _executeInvocation(items, context);
    } finally {
      _inFlightBatches--;
      _completeCloseIfReady();
      _pump();
    }
  }

  Future<void> _executeInvocation(
    List<_PendingRequest<TInput, TResult>> items,
    BatchExecutionContext context,
  ) async {
    _batchInvocationCount++;
    if (context.splitDepth > 0) {
      _splitInvocationCount++;
    }

    try {
      final stopwatch = Stopwatch()..start();
      final results = await _invoke(items, context);
      stopwatch.stop();
      _totalExecutionMicros += stopwatch.elapsedMicroseconds;

      for (var index = 0; index < items.length; index++) {
        final item = items[index];
        final result = results[index];
        if (result.isSuccess) {
          _successCount++;
          _completeSuccess(item, result.requireValue());
        } else {
          _failureCount++;
          _failPending(item, result.error!, result.stackTrace);
        }
      }
    } catch (error, stackTrace) {
      if (failureIsolation == BatchFailureIsolation.splitInHalf &&
          items.length > 1) {
        _splitBatchCount++;
        final midpoint = items.length ~/ 2;
        final leftItems = items.sublist(0, midpoint);
        final rightItems = items.sublist(midpoint);

        await _executeInvocation(
          leftItems,
          context.child(
            batchId: _nextBatchId++,
            dispatchedAt: _clock(),
            oldestEnqueuedAt: leftItems.first.enqueuedAt,
            newestEnqueuedAt: leftItems.last.enqueuedAt,
            itemCount: leftItems.length,
            totalWeight: _sumWeight(leftItems),
          ),
        );
        await _executeInvocation(
          rightItems,
          context.child(
            batchId: _nextBatchId++,
            dispatchedAt: _clock(),
            oldestEnqueuedAt: rightItems.first.enqueuedAt,
            newestEnqueuedAt: rightItems.last.enqueuedAt,
            itemCount: rightItems.length,
            totalWeight: _sumWeight(rightItems),
          ),
        );
      } else {
        for (final item in items) {
          _failureCount++;
          _failPending(item, error, stackTrace);
        }
      }
    }
  }

  Future<List<BatchItemResult<TResult>>> _invoke(
    List<_PendingRequest<TInput, TResult>> items,
    BatchExecutionContext context,
  ) async {
    final nowMicros = _elapsedMicros();
    final requests = <BatchRequest<TInput>>[
      for (final item in items)
        BatchRequest<TInput>(
          sequence: item.sequence,
          input: item.input,
          enqueuedAt: item.enqueuedAt,
          queueWait: Duration(
            microseconds: nowMicros - item.enqueuedAtMicros,
          ),
          maxQueueDelay: item.maxQueueDelay,
          weight: item.weight,
          metadata: item.metadata,
          traceId: item.traceId,
        ),
    ];

    final results = await _runBatch(requests, context);
    if (results.length != items.length) {
      throw BatchProtocolException(
        'Batch runner returned ${results.length} results for ${items.length} '
        'requests in batch ${context.batchId}. The order and count must match.',
      );
    }
    return results;
  }

  void _completeSuccess(
    _PendingRequest<TInput, TResult> item,
    TResult value,
  ) {
    if (!item.completer.isCompleted) {
      item.completer.complete(value);
    }
  }

  void _failPending(
    _PendingRequest<TInput, TResult> item,
    Object error, [
    StackTrace? stackTrace,
  ]) {
    if (!item.completer.isCompleted) {
      item.completer.completeError(error, stackTrace);
    }
  }

  void _failQueued(Object error, [StackTrace? stackTrace]) {
    while (_queue.isNotEmpty) {
      final item = _queue.removeFirst();
      _queuedWeight -= item.weight;
      _failureCount++;
      _failPending(item, error, stackTrace);
    }
  }

  void _pruneExpired(int nowMicros) {
    if (_queue.isEmpty) {
      return;
    }

    final survivors = Queue<_PendingRequest<TInput, TResult>>();
    while (_queue.isNotEmpty) {
      final item = _queue.removeFirst();
      final expiresAtMicros = item.expiresAtMicros;
      if (expiresAtMicros != null && expiresAtMicros <= nowMicros) {
        _queuedWeight -= item.weight;
        _expiredCount++;
        _failPending(
          item,
          BatchRequestExpiredException(
            'Queued request ${item.sequence} exceeded its maxQueueDelay before dispatch.',
          ),
        );
        continue;
      }
      survivors.addLast(item);
    }
    _queue.addAll(survivors);
  }

  _BatchSelection? _selectBatch(int nowMicros) {
    if (_queue.isEmpty) {
      return null;
    }

    var count = 0;
    var totalWeight = 0;
    var saturatedByWeight = false;
    for (final item in _queue) {
      if (count >= maxBatchItems) {
        break;
      }
      if (count > 0 && totalWeight + item.weight > maxBatchWeight) {
        saturatedByWeight = true;
        break;
      }
      totalWeight += item.weight;
      count++;
      if (totalWeight >= maxBatchWeight) {
        saturatedByWeight = true;
        break;
      }
    }

    if (count == 0) {
      return null;
    }

    if (_closed && _drainOnClose) {
      return _BatchSelection(count: count, reason: BatchDispatchReason.closing);
    }
    if (count >= maxBatchItems) {
      return _BatchSelection(count: count, reason: BatchDispatchReason.size);
    }
    if (saturatedByWeight) {
      return _BatchSelection(count: count, reason: BatchDispatchReason.weight);
    }
    if (nowMicros >= _queue.first.dispatchAtMicros) {
      return _BatchSelection(count: count, reason: BatchDispatchReason.latency);
    }
    return null;
  }

  List<_PendingRequest<TInput, TResult>> _dequeue(int count) {
    final items = <_PendingRequest<TInput, TResult>>[];
    for (var index = 0; index < count; index++) {
      final item = _queue.removeFirst();
      _queuedWeight -= item.weight;
      items.add(item);
    }
    return items;
  }

  void _scheduleNextWakeup() {
    if (_queue.isEmpty) {
      return;
    }

    var nextMicros = _queue.first.dispatchAtMicros;
    for (final item in _queue) {
      final expiresAtMicros = item.expiresAtMicros;
      if (expiresAtMicros != null && expiresAtMicros < nextMicros) {
        nextMicros = expiresAtMicros;
      }
    }

    final nowMicros = _elapsedMicros();
    final delayMicros = nextMicros <= nowMicros ? 0 : nextMicros - nowMicros;
    _timer = Timer(Duration(microseconds: delayMicros), _pump);
  }

  void _completeCloseIfReady() {
    if (_closed &&
        !_closedCompleter.isCompleted &&
        _queue.isEmpty &&
        _inFlightBatches == 0) {
      _closedCompleter.complete();
    }
  }

  int _sumWeight(List<_PendingRequest<TInput, TResult>> items) {
    var total = 0;
    for (final item in items) {
      total += item.weight;
    }
    return total;
  }
}

enum QueueOverflowStrategy {
  rejectNewest,
  dropOldest,
}

enum BatchFailureIsolation {
  none,
  splitInHalf,
}

enum BatchDispatchReason {
  size,
  weight,
  latency,
  closing,
  splitRetry,
}

class BatchRequest<TInput> {
  const BatchRequest({
    required this.sequence,
    required this.input,
    required this.enqueuedAt,
    required this.queueWait,
    required this.weight,
    this.maxQueueDelay,
    this.metadata,
    this.traceId,
  });

  final int sequence;
  final TInput input;
  final DateTime enqueuedAt;
  final Duration queueWait;
  final Duration? maxQueueDelay;
  final int weight;
  final Object? metadata;
  final String? traceId;
}

class BatchExecutionContext {
  const BatchExecutionContext({
    required this.batchId,
    required this.rootBatchId,
    required this.dispatchReason,
    required this.dispatchedAt,
    required this.oldestEnqueuedAt,
    required this.newestEnqueuedAt,
    required this.itemCount,
    required this.totalWeight,
    required this.splitDepth,
  });

  final int batchId;
  final int rootBatchId;
  final BatchDispatchReason dispatchReason;
  final DateTime dispatchedAt;
  final DateTime oldestEnqueuedAt;
  final DateTime newestEnqueuedAt;
  final int itemCount;
  final int totalWeight;
  final int splitDepth;

  Duration get queueSpan => newestEnqueuedAt.difference(oldestEnqueuedAt);

  BatchExecutionContext child({
    required int batchId,
    required DateTime dispatchedAt,
    required DateTime oldestEnqueuedAt,
    required DateTime newestEnqueuedAt,
    required int itemCount,
    required int totalWeight,
  }) {
    return BatchExecutionContext(
      batchId: batchId,
      rootBatchId: rootBatchId,
      dispatchReason: BatchDispatchReason.splitRetry,
      dispatchedAt: dispatchedAt,
      oldestEnqueuedAt: oldestEnqueuedAt,
      newestEnqueuedAt: newestEnqueuedAt,
      itemCount: itemCount,
      totalWeight: totalWeight,
      splitDepth: splitDepth + 1,
    );
  }
}

class BatchItemResult<TResult> {
  const BatchItemResult._(
    this.isSuccess,
    this.value,
    this.error,
    this.stackTrace,
  );

  factory BatchItemResult.success(TResult value) {
    return BatchItemResult<TResult>._(true, value, null, null);
  }

  factory BatchItemResult.failure(Object error, [StackTrace? stackTrace]) {
    return BatchItemResult<TResult>._(false, null, error, stackTrace);
  }

  final bool isSuccess;
  final TResult? value;
  final Object? error;
  final StackTrace? stackTrace;

  TResult requireValue() => value as TResult;
}

class MicroBatcherSnapshot {
  const MicroBatcherSnapshot({
    required this.isClosed,
    required this.queuedItems,
    required this.queuedWeight,
    required this.inFlightBatches,
    required this.maxObservedQueueDepth,
    required this.maxObservedQueueWeight,
    required this.totalSubmitted,
    required this.totalRootBatches,
    required this.totalBatchInvocations,
    required this.totalSplitInvocations,
    required this.totalSplitBatches,
    required this.totalSucceeded,
    required this.totalFailed,
    required this.totalExpired,
    required this.totalDropped,
    required this.totalRejected,
    required this.totalQueueWaitMicros,
    required this.totalExecutionMicros,
  });

  final bool isClosed;
  final int queuedItems;
  final int queuedWeight;
  final int inFlightBatches;
  final int maxObservedQueueDepth;
  final int maxObservedQueueWeight;
  final int totalSubmitted;
  final int totalRootBatches;
  final int totalBatchInvocations;
  final int totalSplitInvocations;
  final int totalSplitBatches;
  final int totalSucceeded;
  final int totalFailed;
  final int totalExpired;
  final int totalDropped;
  final int totalRejected;
  final int totalQueueWaitMicros;
  final int totalExecutionMicros;

  int get totalCompleted => totalSucceeded + totalFailed;

  double get averageQueueWaitMilliseconds {
    if (totalCompleted == 0) {
      return 0.0;
    }
    return totalQueueWaitMicros / totalCompleted / 1000;
  }

  double get averageBatchInvocationMilliseconds {
    if (totalBatchInvocations == 0) {
      return 0.0;
    }
    return totalExecutionMicros / totalBatchInvocations / 1000;
  }
}

class BatchQueueFullException implements Exception {
  BatchQueueFullException({
    required this.queuedItems,
    required this.maxQueueItems,
    required this.queuedWeight,
    required this.maxQueueWeight,
    required this.additionalWeight,
    this.message,
  });

  final int queuedItems;
  final int maxQueueItems;
  final int queuedWeight;
  final int maxQueueWeight;
  final int additionalWeight;
  final String? message;

  @override
  String toString() {
    return message ??
        'BatchQueueFullException: queue is full with $queuedItems/$maxQueueItems '
            'items and $queuedWeight/$maxQueueWeight weight. Additional weight '
            '$additionalWeight cannot be accepted.';
  }
}

class BatchWeightExceededException implements Exception {
  BatchWeightExceededException(this.message);

  final String message;

  @override
  String toString() => 'BatchWeightExceededException: $message';
}

class BatchRequestExpiredException implements Exception {
  BatchRequestExpiredException(this.message);

  final String message;

  @override
  String toString() => 'BatchRequestExpiredException: $message';
}

class BatchClosedException implements Exception {
  BatchClosedException(this.message);

  final String message;

  @override
  String toString() => 'BatchClosedException: $message';
}

class BatchProtocolException implements Exception {
  BatchProtocolException(this.message);

  final String message;

  @override
  String toString() => 'BatchProtocolException: $message';
}

class _PendingRequest<TInput, TResult> {
  _PendingRequest({
    required this.sequence,
    required this.input,
    required this.enqueuedAt,
    required this.enqueuedAtMicros,
    required this.dispatchAtMicros,
    required this.expiresAtMicros,
    required this.maxQueueDelay,
    required this.weight,
    required this.metadata,
    required this.traceId,
  });

  final int sequence;
  final TInput input;
  final DateTime enqueuedAt;
  final int enqueuedAtMicros;
  final int dispatchAtMicros;
  final int? expiresAtMicros;
  final Duration? maxQueueDelay;
  final int weight;
  final Object? metadata;
  final String? traceId;
  final Completer<TResult> completer = Completer<TResult>();
}

class _BatchSelection {
  const _BatchSelection({
    required this.count,
    required this.reason,
  });

  final int count;
  final BatchDispatchReason reason;
}

/*
This solves one of the messiest April 2026 backend and AI infrastructure problems in Dart: you have a lot of small async requests, each one is expensive on its own, and your provider or downstream system gets slower and more expensive when you fire them individually. That shows up with embedding calls, rerank jobs, moderation checks, vector writes, document enrichment, analytics fan-out, and even plain HTTP work behind a Flutter or Dart server app. This file gives you a production-ready micro batcher with queue limits, weighted batches, concurrency caps, per-request queue deadlines, and recursive split isolation when one bad item poisons a whole batch.

Built because the usual “collect a list and flush on a timer” code breaks as soon as traffic gets real. It ignores queue pressure, lets stale work sit forever, and turns one provider error into a full-batch outage. I wanted something I would actually trust in a real service: bounded memory, FIFO behavior, configurable overflow policy, fast dispatch on size or weight, and a fallback that automatically splits a failed batch into smaller pieces until good items can still complete. That split path matters a lot for modern AI APIs where one malformed payload, one oversize document, or one provider edge case can sink an otherwise valid batch.

Use it when you run Dart on the server, in a worker, in a CLI pipeline, or behind a Flutter app that talks to a batching-friendly backend. It is especially useful for OpenAI compatible embeddings, Anthropic or Gemini rerank wrappers, vector database upserts, moderation fan-out, metric shipping, event enrichment, and any workload where throughput and cost both improve when you batch. The public surface stays small on purpose: submit work, implement one batch runner that returns ordered per-item results, and optionally close with drain semantics when your process is shutting down.

The trick: batching is only half of the real problem. The hard part is deciding when to flush, how to stop the queue from becoming a liability, and how to salvage healthy work after a partial provider failure. This implementation keeps a bounded FIFO queue, dispatches immediately when item count or estimated weight fills a batch, dispatches on latency when traffic is light, enforces queue capacity before memory runs away, and expires work that already missed its usefulness window. If a batch invocation throws and isolation is enabled, it recursively splits the batch in half and retries smaller segments in the same invocation slot. That gives you a practical way to isolate poison items without writing custom recovery logic around every downstream API.

Drop this into a Dart 3 backend, a Cloud Run style service, a shelf server, a worker process, a Flutter tooling backend, or a data pipeline that needs disciplined async batching. If someone is searching GitHub or Google for terms like Dart micro batcher, Dart async batch queue, Flutter backend request batching, Dart embeddings batcher, OpenAI embeddings Dart batching, queue deadline batcher, split retry batch executor, or weighted batch processor in Dart, this file is meant to be the thing they can fork, wire up, and ship. It is a single-file utility, but it is built like production code instead of a demo.
*/
