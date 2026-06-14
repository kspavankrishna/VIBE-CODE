import 'dart:collection';
import 'dart:math' as math;

enum InferencePriority {
  background(1),
  batch(2),
  interactive(5),
  realtime(10),
  incident(20);

  const InferencePriority(this.weight);

  final int weight;
}

enum AdmissionKind { accept, defer, reject, shed }

final class ProviderCapacity {
  ProviderCapacity({
    required this.name,
    required this.maxInFlight,
    required this.maxRequestsPerMinute,
    required this.maxTokensPerMinute,
    required this.contextWindowTokens,
    required this.p50Latency,
    required this.p95Latency,
    required this.inputUsdPerMillionTokens,
    required this.outputUsdPerMillionTokens,
    required Set<String> regions,
    this.acceptsPersonalData = false,
    this.supportsBatching = true,
    this.maxBatchSize = 1,
    this.failurePenalty = 0,
  }) : regions = Set.unmodifiable(regions) {
    if (name.trim().isEmpty) {
      throw ArgumentError.value(name, 'name', 'Provider name must not be empty.');
    }
    if (maxInFlight <= 0 || maxRequestsPerMinute <= 0 || maxTokensPerMinute <= 0) {
      throw ArgumentError('Provider limits must be positive for $name.');
    }
    if (contextWindowTokens <= 0 || maxBatchSize <= 0) {
      throw ArgumentError('Provider token and batch limits must be positive for $name.');
    }
    if (p50Latency.isNegative || p95Latency.isNegative || p95Latency < p50Latency) {
      throw ArgumentError('Provider latency bounds are invalid for $name.');
    }
    if (inputUsdPerMillionTokens < 0 || outputUsdPerMillionTokens < 0 || failurePenalty < 0) {
      throw ArgumentError('Provider costs and penalties must be non-negative for $name.');
    }
  }

  final String name;
  final int maxInFlight;
  final int maxRequestsPerMinute;
  final int maxTokensPerMinute;
  final int contextWindowTokens;
  final Duration p50Latency;
  final Duration p95Latency;
  final double inputUsdPerMillionTokens;
  final double outputUsdPerMillionTokens;
  final Set<String> regions;
  final bool acceptsPersonalData;
  final bool supportsBatching;
  final int maxBatchSize;
  final double failurePenalty;

  double estimateCostUsd({required int inputTokens, required int outputTokens}) {
    return (inputTokens * inputUsdPerMillionTokens + outputTokens * outputUsdPerMillionTokens) / 1000000.0;
  }

  bool servesRegion(String region) => regions.contains(region) || regions.contains('*');
}

final class TenantBudget {
  TenantBudget({
    required this.tenantId,
    required this.usdRemaining,
    required this.tokensRemainingThisMinute,
    required this.requestsRemainingThisMinute,
    required this.maxQueuedJobs,
    required Set<String> allowedRegions,
    this.allowPersonalData = false,
    this.priorityFloor = InferencePriority.background,
  }) : allowedRegions = Set.unmodifiable(allowedRegions) {
    if (tenantId.trim().isEmpty) {
      throw ArgumentError.value(tenantId, 'tenantId', 'Tenant id must not be empty.');
    }
    if (usdRemaining < 0 || tokensRemainingThisMinute < 0 || requestsRemainingThisMinute < 0 || maxQueuedJobs < 0) {
      throw ArgumentError('Tenant budget values must be non-negative for $tenantId.');
    }
  }

  final String tenantId;
  final double usdRemaining;
  final int tokensRemainingThisMinute;
  final int requestsRemainingThisMinute;
  final int maxQueuedJobs;
  final Set<String> allowedRegions;
  final bool allowPersonalData;
  final InferencePriority priorityFloor;

  bool regionAllowed(String region) => allowedRegions.isEmpty || allowedRegions.contains(region) || allowedRegions.contains('*');
}

final class InferenceJob {
  InferenceJob({
    required this.jobId,
    required this.tenantId,
    required this.modelFamily,
    required this.region,
    required this.createdAt,
    required this.deadline,
    required this.estimatedInputTokens,
    required this.estimatedOutputTokens,
    this.priority = InferencePriority.interactive,
    this.containsPersonalData = false,
    this.requiredContextTokens,
    this.consistencyKey,
    Map<String, String> metadata = const <String, String>{},
  }) : metadata = UnmodifiableMapView(Map<String, String>.from(metadata)) {
    if (jobId.trim().isEmpty || tenantId.trim().isEmpty || modelFamily.trim().isEmpty || region.trim().isEmpty) {
      throw ArgumentError('Job id, tenant id, model family, and region must not be empty.');
    }
    if (estimatedInputTokens < 0 || estimatedOutputTokens < 0) {
      throw ArgumentError('Estimated token counts must be non-negative for $jobId.');
    }
    if ((requiredContextTokens ?? totalEstimatedTokens) < 0) {
      throw ArgumentError('Required context tokens must be non-negative for $jobId.');
    }
    if (deadline.isBefore(createdAt)) {
      throw ArgumentError('Deadline must not be earlier than createdAt for $jobId.');
    }
  }

  final String jobId;
  final String tenantId;
  final String modelFamily;
  final String region;
  final DateTime createdAt;
  final DateTime deadline;
  final int estimatedInputTokens;
  final int estimatedOutputTokens;
  final InferencePriority priority;
  final bool containsPersonalData;
  final int? requiredContextTokens;
  final String? consistencyKey;
  final UnmodifiableMapView<String, String> metadata;

  int get totalEstimatedTokens => estimatedInputTokens + estimatedOutputTokens;

  int get contextTokens => requiredContextTokens ?? totalEstimatedTokens;

  Duration timeToDeadline(DateTime now) => deadline.difference(now);
}

final class ProviderLoad {
  ProviderLoad({
    required this.providerName,
    this.inFlight = 0,
    this.queued = 0,
    this.requestsReservedThisMinute = 0,
    this.tokensReservedThisMinute = 0,
    this.consecutiveFailures = 0,
    this.disabledUntil,
    this.observedP95Latency,
  }) {
    if (inFlight < 0 || queued < 0 || requestsReservedThisMinute < 0 || tokensReservedThisMinute < 0 || consecutiveFailures < 0) {
      throw ArgumentError('Provider load counters must be non-negative for $providerName.');
    }
  }

  final String providerName;
  final int inFlight;
  final int queued;
  final int requestsReservedThisMinute;
  final int tokensReservedThisMinute;
  final int consecutiveFailures;
  final DateTime? disabledUntil;
  final Duration? observedP95Latency;

  bool isDisabled(DateTime now) => disabledUntil != null && disabledUntil!.isAfter(now);

  Duration latencyFor(ProviderCapacity capacity) => observedP95Latency ?? capacity.p95Latency;

  double inFlightRatio(ProviderCapacity capacity) => _ratio(inFlight, capacity.maxInFlight);

  double requestRatio(ProviderCapacity capacity) => _ratio(requestsReservedThisMinute, capacity.maxRequestsPerMinute);

  double tokenRatio(ProviderCapacity capacity) => _ratio(tokensReservedThisMinute, capacity.maxTokensPerMinute);

  ProviderLoad reserve(InferenceJob job) {
    return ProviderLoad(
      providerName: providerName,
      inFlight: inFlight + 1,
      queued: queued,
      requestsReservedThisMinute: requestsReservedThisMinute + 1,
      tokensReservedThisMinute: tokensReservedThisMinute + job.totalEstimatedTokens,
      consecutiveFailures: consecutiveFailures,
      disabledUntil: disabledUntil,
      observedP95Latency: observedP95Latency,
    );
  }
}

final class PlannerOptions {
  const PlannerOptions({
    this.queueSoftLimit = 500,
    this.maxClientDefer = const Duration(seconds: 45),
    this.defaultRetryAfter = const Duration(seconds: 3),
    this.clockSkewAllowance = const Duration(milliseconds: 250),
    this.overloadShedRatio = 0.92,
    this.queueDepthPenalty = 0.018,
    this.costWeight = 0.12,
    this.deadlineWeight = 0.38,
    this.headroomWeight = 0.42,
    this.priorityWeight = 0.08,
  }) : assert(queueSoftLimit >= 0),
       assert(overloadShedRatio > 0 && overloadShedRatio <= 1),
       assert(queueDepthPenalty >= 0),
       assert(costWeight >= 0),
       assert(deadlineWeight >= 0),
       assert(headroomWeight >= 0),
       assert(priorityWeight >= 0);

  final int queueSoftLimit;
  final Duration maxClientDefer;
  final Duration defaultRetryAfter;
  final Duration clockSkewAllowance;
  final double overloadShedRatio;
  final double queueDepthPenalty;
  final double costWeight;
  final double deadlineWeight;
  final double headroomWeight;
  final double priorityWeight;
}

final class AdmissionOutcome {
  AdmissionOutcome({
    required this.kind,
    required this.jobId,
    required this.tenantId,
    required this.reason,
    required this.score,
    required this.estimatedCostUsd,
    this.providerName,
    this.retryAfter,
    this.queuePosition,
    this.batchKey,
    Map<String, String> headers = const <String, String>{},
  }) : headers = UnmodifiableMapView(Map<String, String>.from(headers));

  final AdmissionKind kind;
  final String jobId;
  final String tenantId;
  final String reason;
  final double score;
  final double estimatedCostUsd;
  final String? providerName;
  final Duration? retryAfter;
  final int? queuePosition;
  final String? batchKey;
  final UnmodifiableMapView<String, String> headers;

  bool get accepted => kind == AdmissionKind.accept;

  bool get shouldRetry => kind == AdmissionKind.defer;
}

final class TenantLedger {
  TenantLedger({
    required this.tenantId,
    this.queuedJobs = 0,
    this.tokensReservedThisMinute = 0,
    this.requestsReservedThisMinute = 0,
    this.usdReserved = 0,
  }) {
    if (queuedJobs < 0 || tokensReservedThisMinute < 0 || requestsReservedThisMinute < 0 || usdReserved < 0) {
      throw ArgumentError('Tenant ledger counters must be non-negative for $tenantId.');
    }
  }

  final String tenantId;
  final int queuedJobs;
  final int tokensReservedThisMinute;
  final int requestsReservedThisMinute;
  final double usdReserved;

  TenantLedger reserve(InferenceJob job, double costUsd) {
    return TenantLedger(
      tenantId: tenantId,
      queuedJobs: queuedJobs + 1,
      tokensReservedThisMinute: tokensReservedThisMinute + job.totalEstimatedTokens,
      requestsReservedThisMinute: requestsReservedThisMinute + 1,
      usdReserved: usdReserved + costUsd,
    );
  }
}

final class QueueSnapshot {
  QueueSnapshot({
    Map<String, ProviderLoad> providerLoads = const <String, ProviderLoad>{},
    Map<String, TenantLedger> tenantLedgers = const <String, TenantLedger>{},
  }) : providerLoads = UnmodifiableMapView(Map<String, ProviderLoad>.from(providerLoads)),
       tenantLedgers = UnmodifiableMapView(Map<String, TenantLedger>.from(tenantLedgers));

  final UnmodifiableMapView<String, ProviderLoad> providerLoads;
  final UnmodifiableMapView<String, TenantLedger> tenantLedgers;

  ProviderLoad loadFor(String providerName) => providerLoads[providerName] ?? ProviderLoad(providerName: providerName);

  TenantLedger ledgerFor(String tenantId) => tenantLedgers[tenantId] ?? TenantLedger(tenantId: tenantId);
}

final class TenantInferenceQueuePlanner {
  const TenantInferenceQueuePlanner({this.options = const PlannerOptions()});

  final PlannerOptions options;

  AdmissionOutcome planOne({
    required InferenceJob job,
    required TenantBudget tenant,
    required Iterable<ProviderCapacity> providers,
    required QueueSnapshot snapshot,
    DateTime? now,
  }) {
    final DateTime effectiveNow = now ?? DateTime.now().toUtc();
    final String? tenantBlock = _tenantBlockReason(job, tenant, snapshot.ledgerFor(tenant.tenantId));
    if (tenantBlock != null) {
      return _terminal(job, tenant, AdmissionKind.reject, tenantBlock, 0, null, effectiveNow);
    }

    final List<_ProviderCandidate> candidates = <_ProviderCandidate>[];
    for (final ProviderCapacity provider in providers) {
      final ProviderLoad load = snapshot.loadFor(provider.name);
      final String? block = _providerBlockReason(job, provider, load, effectiveNow);
      if (block != null) {
        continue;
      }
      candidates.add(_score(job, tenant, provider, load, effectiveNow));
    }

    if (candidates.isEmpty) {
      final AdmissionKind kind = _shouldShed(job, providers, snapshot, effectiveNow) ? AdmissionKind.shed : AdmissionKind.defer;
      final Duration retry = _retryAfter(job, providers, snapshot, effectiveNow);
      return AdmissionOutcome(
        kind: kind,
        jobId: job.jobId,
        tenantId: job.tenantId,
        reason: kind == AdmissionKind.shed
            ? 'No eligible provider has capacity before the deadline; shedding protects realtime work.'
            : 'No eligible provider is available right now; retry after the returned delay.',
        score: 0,
        estimatedCostUsd: 0,
        retryAfter: kind == AdmissionKind.defer ? retry : null,
        headers: _headers(kind, null, retry, effectiveNow),
      );
    }

    candidates.sort((a, b) {
      final int score = b.score.compareTo(a.score);
      if (score != 0) return score;
      return a.provider.name.compareTo(b.provider.name);
    });

    final _ProviderCandidate winner = candidates.first;
    final int queuePosition = _queuePosition(job, tenant, snapshot, winner.load);
    final String batchKey = _batchKey(job, winner.provider);
    return AdmissionOutcome(
      kind: AdmissionKind.accept,
      jobId: job.jobId,
      tenantId: job.tenantId,
      providerName: winner.provider.name,
      reason: winner.reason,
      score: winner.score,
      estimatedCostUsd: winner.estimatedCostUsd,
      retryAfter: Duration.zero,
      queuePosition: queuePosition,
      batchKey: batchKey,
      headers: _headers(AdmissionKind.accept, winner.provider.name, Duration.zero, effectiveNow),
    );
  }

  List<AdmissionOutcome> planMany({
    required Iterable<InferenceJob> jobs,
    required Map<String, TenantBudget> tenants,
    required Iterable<ProviderCapacity> providers,
    required QueueSnapshot snapshot,
    DateTime? now,
  }) {
    final DateTime effectiveNow = now ?? DateTime.now().toUtc();
    final List<InferenceJob> orderedJobs = jobs.toList()
      ..sort((a, b) {
        final int priority = b.priority.weight.compareTo(a.priority.weight);
        if (priority != 0) return priority;
        final int deadline = a.deadline.compareTo(b.deadline);
        if (deadline != 0) return deadline;
        return a.jobId.compareTo(b.jobId);
      });

    final Map<String, ProviderLoad> providerLoads = Map<String, ProviderLoad>.from(snapshot.providerLoads);
    final Map<String, TenantLedger> tenantLedgers = Map<String, TenantLedger>.from(snapshot.tenantLedgers);
    final List<AdmissionOutcome> outcomes = <AdmissionOutcome>[];

    for (final InferenceJob job in orderedJobs) {
      final TenantBudget? tenant = tenants[job.tenantId];
      if (tenant == null) {
        outcomes.add(AdmissionOutcome(
          kind: AdmissionKind.reject,
          jobId: job.jobId,
          tenantId: job.tenantId,
          reason: 'Tenant budget was not supplied for this job.',
          score: 0,
          estimatedCostUsd: 0,
          headers: _headers(AdmissionKind.reject, null, null, effectiveNow),
        ));
        continue;
      }

      final AdmissionOutcome outcome = planOne(
        job: job,
        tenant: tenant,
        providers: providers,
        snapshot: QueueSnapshot(providerLoads: providerLoads, tenantLedgers: tenantLedgers),
        now: effectiveNow,
      );
      outcomes.add(outcome);

      if (outcome.accepted && outcome.providerName != null) {
        providerLoads[outcome.providerName!] = (providerLoads[outcome.providerName!] ?? ProviderLoad(providerName: outcome.providerName!)).reserve(job);
        tenantLedgers[job.tenantId] = (tenantLedgers[job.tenantId] ?? TenantLedger(tenantId: job.tenantId)).reserve(job, outcome.estimatedCostUsd);
      }
    }

    return outcomes;
  }

  String? _tenantBlockReason(InferenceJob job, TenantBudget tenant, TenantLedger ledger) {
    if (tenant.tenantId != job.tenantId) {
      return 'Job tenant does not match the supplied tenant budget.';
    }
    if (job.priority.weight < tenant.priorityFloor.weight) {
      return 'Job priority is below the tenant admission floor.';
    }
    if (!tenant.regionAllowed(job.region)) {
      return 'Tenant is not allowed to run inference in ${job.region}.';
    }
    if (job.containsPersonalData && !tenant.allowPersonalData) {
      return 'Job contains personal data but the tenant policy forbids personal data processing.';
    }
    if (ledger.queuedJobs >= tenant.maxQueuedJobs) {
      return 'Tenant queue is already at its configured limit.';
    }
    if (ledger.requestsReservedThisMinute + 1 > tenant.requestsRemainingThisMinute) {
      return 'Tenant request quota for this minute is exhausted.';
    }
    if (ledger.tokensReservedThisMinute + job.totalEstimatedTokens > tenant.tokensRemainingThisMinute) {
      return 'Tenant token quota for this minute is exhausted.';
    }
    return null;
  }

  String? _providerBlockReason(InferenceJob job, ProviderCapacity provider, ProviderLoad load, DateTime now) {
    if (load.isDisabled(now)) {
      return 'Provider is disabled until ${load.disabledUntil!.toIso8601String()}.';
    }
    if (!provider.servesRegion(job.region)) {
      return 'Provider does not serve ${job.region}.';
    }
    if (job.containsPersonalData && !provider.acceptsPersonalData) {
      return 'Provider cannot receive personal data.';
    }
    if (job.contextTokens > provider.contextWindowTokens) {
      return 'Provider context window is too small.';
    }
    if (load.inFlight >= provider.maxInFlight) {
      return 'Provider in-flight concurrency is full.';
    }
    if (load.requestsReservedThisMinute + 1 > provider.maxRequestsPerMinute) {
      return 'Provider request quota for this minute is full.';
    }
    if (load.tokensReservedThisMinute + job.totalEstimatedTokens > provider.maxTokensPerMinute) {
      return 'Provider token quota for this minute is full.';
    }
    final Duration latestStart = job.timeToDeadline(now) + options.clockSkewAllowance;
    if (latestStart < load.latencyFor(provider)) {
      return 'Provider cannot meet the job deadline at current p95 latency.';
    }
    return null;
  }

  _ProviderCandidate _score(
    InferenceJob job,
    TenantBudget tenant,
    ProviderCapacity provider,
    ProviderLoad load,
    DateTime now,
  ) {
    final double estimatedCostUsd = provider.estimateCostUsd(
      inputTokens: job.estimatedInputTokens,
      outputTokens: job.estimatedOutputTokens,
    );
    final double tenantSpendable = math.max(0.000001, tenant.usdRemaining);
    final double costPressure = (estimatedCostUsd / tenantSpendable).clamp(0.0, 1.0);
    final double tokenHeadroom = 1 - _ratio(load.tokensReservedThisMinute + job.totalEstimatedTokens, provider.maxTokensPerMinute);
    final double requestHeadroom = 1 - _ratio(load.requestsReservedThisMinute + 1, provider.maxRequestsPerMinute);
    final double inFlightHeadroom = 1 - _ratio(load.inFlight + 1, provider.maxInFlight);
    final double headroom = ((tokenHeadroom * 0.45) + (requestHeadroom * 0.25) + (inFlightHeadroom * 0.30)).clamp(0.0, 1.0);
    final Duration latency = load.latencyFor(provider);
    final Duration slack = job.timeToDeadline(now) - latency;
    final double deadlineFit = _deadlineFit(slack, latency);
    final double priorityFit = (job.priority.weight / InferencePriority.incident.weight).clamp(0.0, 1.0);
    final double queuePenalty = load.queued * options.queueDepthPenalty;
    final double score = (headroom * options.headroomWeight) +
        (deadlineFit * options.deadlineWeight) +
        (priorityFit * options.priorityWeight) -
        (costPressure * options.costWeight) -
        provider.failurePenalty -
        (load.consecutiveFailures * 0.04) -
        queuePenalty;

    return _ProviderCandidate(
      provider: provider,
      load: load,
      score: score,
      estimatedCostUsd: estimatedCostUsd,
      reason: 'Accepted by ${provider.name}: score=${score.toStringAsFixed(4)}, headroom=${headroom.toStringAsFixed(3)}, deadlineFit=${deadlineFit.toStringAsFixed(3)}.',
    );
  }

  AdmissionOutcome _terminal(
    InferenceJob job,
    TenantBudget tenant,
    AdmissionKind kind,
    String reason,
    double estimatedCostUsd,
    String? providerName,
    DateTime now,
  ) {
    return AdmissionOutcome(
      kind: kind,
      jobId: job.jobId,
      tenantId: tenant.tenantId,
      providerName: providerName,
      reason: reason,
      score: 0,
      estimatedCostUsd: estimatedCostUsd,
      headers: _headers(kind, providerName, null, now),
    );
  }

  bool _shouldShed(InferenceJob job, Iterable<ProviderCapacity> providers, QueueSnapshot snapshot, DateTime now) {
    if (job.priority.weight >= InferencePriority.realtime.weight) {
      return false;
    }
    if (job.timeToDeadline(now) <= Duration.zero) {
      return true;
    }
    int seen = 0;
    int overloaded = 0;
    for (final ProviderCapacity provider in providers) {
      final ProviderLoad load = snapshot.loadFor(provider.name);
      if (!provider.servesRegion(job.region) || load.isDisabled(now)) {
        continue;
      }
      seen++;
      final double pressure = math.max(load.inFlightRatio(provider), math.max(load.requestRatio(provider), load.tokenRatio(provider)));
      if (pressure >= options.overloadShedRatio) {
        overloaded++;
      }
    }
    return seen > 0 && overloaded == seen;
  }

  Duration _retryAfter(InferenceJob job, Iterable<ProviderCapacity> providers, QueueSnapshot snapshot, DateTime now) {
    Duration best = options.defaultRetryAfter;
    for (final ProviderCapacity provider in providers) {
      final ProviderLoad load = snapshot.loadFor(provider.name);
      if (load.disabledUntil != null && load.disabledUntil!.isAfter(now)) {
        final Duration disabled = load.disabledUntil!.difference(now);
        if (disabled > best) best = disabled;
      }
    }
    final Duration deadlineCap = job.timeToDeadline(now) - const Duration(milliseconds: 100);
    if (deadlineCap <= Duration.zero) {
      return Duration.zero;
    }
    if (best > options.maxClientDefer) {
      best = options.maxClientDefer;
    }
    if (best > deadlineCap) {
      best = deadlineCap;
    }
    return best;
  }

  int _queuePosition(InferenceJob job, TenantBudget tenant, QueueSnapshot snapshot, ProviderLoad selectedLoad) {
    final TenantLedger ledger = snapshot.ledgerFor(tenant.tenantId);
    final double tenantPressure = tenant.maxQueuedJobs == 0 ? 1 : _ratio(ledger.queuedJobs, tenant.maxQueuedJobs);
    final int priorityOffset = InferencePriority.incident.weight - job.priority.weight;
    final int estimate = selectedLoad.queued + (tenantPressure * 12).round() + priorityOffset;
    return math.max(0, estimate);
  }

  String _batchKey(InferenceJob job, ProviderCapacity provider) {
    if (!provider.supportsBatching || provider.maxBatchSize == 1 || job.consistencyKey != null) {
      return 'single:${job.jobId}';
    }
    final String privacy = job.containsPersonalData ? 'pii' : 'nonpii';
    final int tokenBucket = _ceilDiv(math.max(1, job.totalEstimatedTokens), 512) * 512;
    return '${provider.name}|${job.modelFamily}|${job.region}|$privacy|$tokenBucket';
  }

  Map<String, String> _headers(AdmissionKind kind, String? providerName, Duration? retryAfter, DateTime now) {
    final Map<String, String> headers = <String, String>{
      'x-inference-admission': kind.name,
      'x-inference-planned-at': now.toIso8601String(),
    };
    if (providerName != null) {
      headers['x-inference-provider'] = providerName;
    }
    if (retryAfter != null && retryAfter > Duration.zero) {
      headers['retry-after'] = math.max(1, retryAfter.inSeconds).toString();
    }
    return headers;
  }
}

final class _ProviderCandidate {
  const _ProviderCandidate({
    required this.provider,
    required this.load,
    required this.score,
    required this.estimatedCostUsd,
    required this.reason,
  });

  final ProviderCapacity provider;
  final ProviderLoad load;
  final double score;
  final double estimatedCostUsd;
  final String reason;
}

double _ratio(num used, num limit) {
  if (limit <= 0) return 1;
  return (used / limit).clamp(0.0, 1.0).toDouble();
}

double _deadlineFit(Duration slack, Duration latency) {
  if (slack <= Duration.zero) return 0;
  final int latencyMs = math.max(1, latency.inMilliseconds);
  final double multiples = slack.inMilliseconds / latencyMs;
  return (multiples / 4.0).clamp(0.0, 1.0).toDouble();
}

int _ceilDiv(int value, int divisor) => (value + divisor - 1) ~/ divisor;

/*
This solves the boring but painful production problem behind AI inference admission control in Dart services, Flutter backends, edge workers, and MCP agent gateways: deciding which request gets accepted, delayed, rejected, or shed before it burns tokens, money, latency budget, or user trust. Built because April 2026 developer teams are running multiple LLM providers with strict per-minute quotas, regional privacy rules, streaming tool calls, and tenant budgets, yet too many systems still dispatch first and explain the outage later. Use it when your API gateway, queue worker, Shelf server, Cloud Run service, or mobile companion backend needs deterministic multi-tenant AI request planning without pulling in a heavy scheduler. The trick: treat tokens, requests, deadlines, privacy, region, context window, and provider health as one admission decision, then return client-visible headers and stable batch keys so the rest of the platform can behave predictably. Drop this into a Dart repository when you need a serious AI inference queue planner, LLM cost guardrail, provider quota router, realtime agent workload governor, or production-ready backpressure layer that a senior engineer can audit in one file.
*/
