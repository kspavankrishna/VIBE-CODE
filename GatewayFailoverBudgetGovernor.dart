import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;

const String toolName = 'gateway-failover-budget-governor';

class J {
  static Map<String, dynamic> map(Object? value, String name) {
    if (value is Map) {
      return value.map((key, item) => MapEntry(key.toString(), item));
    }
    throw FormatException('Expected object for $name');
  }

  static List<dynamic> list(Object? value) {
    if (value == null) return const [];
    if (value is List) return value;
    throw const FormatException('Expected array');
  }

  static String text(Map<String, dynamic> json, String key, [String fallback = '']) {
    final value = json[key];
    return value == null ? fallback : value.toString();
  }

  static int integer(Map<String, dynamic> json, String key, int fallback) {
    final value = json[key];
    if (value == null) return fallback;
    if (value is int) return value;
    if (value is num) return value.round();
    return int.tryParse(value.toString()) ?? fallback;
  }

  static double number(Map<String, dynamic> json, String key, double fallback) {
    final value = json[key];
    if (value == null) return fallback;
    if (value is num) return value.toDouble();
    return double.tryParse(value.toString()) ?? fallback;
  }

  static bool flag(Map<String, dynamic> json, String key, bool fallback) {
    final value = json[key];
    if (value == null) return fallback;
    if (value is bool) return value;
    final lower = value.toString().toLowerCase();
    if (lower == 'true' || lower == '1' || lower == 'yes') return true;
    if (lower == 'false' || lower == '0' || lower == 'no') return false;
    return fallback;
  }

  static List<String> words(Map<String, dynamic> json, String key) {
    final value = json[key];
    if (value == null) return const [];
    if (value is List) {
      return value.map((item) => item.toString()).toList(growable: false);
    }
    return value
        .toString()
        .split(',')
        .map((part) => part.trim())
        .where((part) => part.isNotEmpty)
        .toList(growable: false);
  }
}

class Endpoint {
  Endpoint(this.id, this.provider, this.model, this.region, this.zones, this.tags,
      this.streaming, this.p95, this.p99, this.success, this.capacity, this.inFlight,
      this.queue, this.inputUsd, this.outputUsd, this.carbonPerK, this.cooldown,
      this.breakerOpen);

  final String id;
  final String provider;
  final String model;
  final String region;
  final List<String> zones;
  final List<String> tags;
  final bool streaming;
  final double p95;
  final double p99;
  final double success;
  final double capacity;
  final int inFlight;
  final int queue;
  final double inputUsd;
  final double outputUsd;
  final double carbonPerK;
  final int cooldown;
  final bool breakerOpen;

  factory Endpoint.fromJson(Map<String, dynamic> json) {
    return Endpoint(
      J.text(json, 'endpointId', J.text(json, 'id', J.text(json, 'name'))),
      J.text(json, 'provider', 'unknown'),
      J.text(json, 'model', 'unknown'),
      J.text(json, 'region', 'global'),
      J.words(json, 'residencyZones'),
      J.words(json, 'tags'),
      J.flag(json, 'supportsStreaming', true),
      math.max(1.0, J.number(json, 'p95LatencyMs', 1500)),
      math.max(1.0, J.number(json, 'p99LatencyMs', 2500)),
      _clamp01(J.number(json, 'successRate', 0.98)),
      math.max(1.0, J.number(json, 'capacityPerMinute', 60)),
      math.max(0, J.integer(json, 'inFlight', 0)),
      math.max(0, J.integer(json, 'queueDepth', 0)),
      J.number(json, 'costPerMillionInputTokens', 0.15),
      J.number(json, 'costPerMillionOutputTokens', 0.60),
      J.number(json, 'carbonGramsPer1kTokens', 0.0),
      J.integer(json, 'cooldownUntilEpochMs', 0),
      J.flag(json, 'breakerOpen', false),
    );
  }

  bool get canary => tags.map((tag) => tag.toLowerCase()).contains('canary');

  bool matches(String family) {
    if (family.isEmpty) return true;
    final wanted = family.toLowerCase();
    final own = model.toLowerCase();
    return own == wanted ||
        own.contains(wanted) ||
        tags.map((tag) => tag.toLowerCase()).contains(wanted);
  }

  double costUsd(int inputTokens, int outputTokens) {
    return inputTokens * inputUsd / 1000000.0 + outputTokens * outputUsd / 1000000.0;
  }

  double carbon(int inputTokens, int outputTokens) {
    return (inputTokens + outputTokens) * carbonPerK / 1000.0;
  }
}

class Tenant {
  Tenant(this.id, this.usedUsd, this.limitUsd, this.reservedPriority, this.inFlight,
      this.concurrencyLimit);

  final String id;
  final double usedUsd;
  final double limitUsd;
  final int reservedPriority;
  final int inFlight;
  final int concurrencyLimit;

  factory Tenant.fromJson(Map<String, dynamic> json) {
    return Tenant(
      J.text(json, 'tenantId', J.text(json, 'id', 'default')),
      J.number(json, 'usedUsdToday', 0),
      J.number(json, 'dailyLimitUsd', double.infinity),
      J.integer(json, 'reservedPriority', 50),
      J.integer(json, 'inFlight', 0),
      math.max(1, J.integer(json, 'concurrencyLimit', 1000000)),
    );
  }

  double get remainingUsd => limitUsd - usedUsd;

  bool get saturated => inFlight >= concurrencyLimit;
}

class Workload {
  Workload(this.id, this.tenantId, this.modelFamily, this.inputTokens,
      this.outputTokens, this.priority, this.deadlineMs, this.maxUsd, this.region,
      this.allowedZones, this.streaming, this.crossRegion, this.canaryPercent,
      this.shadowPercent, this.preferredProvider, this.idempotencyKey);

  final String id;
  final String tenantId;
  final String modelFamily;
  final int inputTokens;
  final int outputTokens;
  final int priority;
  final int deadlineMs;
  final double maxUsd;
  final String region;
  final List<String> allowedZones;
  final bool streaming;
  final bool crossRegion;
  final int canaryPercent;
  final int shadowPercent;
  final String preferredProvider;
  final String idempotencyKey;

  factory Workload.fromJson(Map<String, dynamic> json) {
    final generated = 'request-${DateTime.now().microsecondsSinceEpoch}';
    return Workload(
      J.text(json, 'requestId', J.text(json, 'id', generated)),
      J.text(json, 'tenantId', 'default'),
      J.text(json, 'modelFamily', J.text(json, 'model')),
      math.max(0, J.integer(json, 'inputTokens', 0)),
      math.max(0, J.integer(json, 'maxOutputTokens', 1024)),
      J.integer(json, 'priority', 50).clamp(0, 100).toInt(),
      math.max(1, J.integer(json, 'deadlineMs', 30000)),
      J.number(json, 'maxUsd', double.infinity),
      J.text(json, 'requestedRegion'),
      J.words(json, 'allowedResidencyZones'),
      J.flag(json, 'requiresStreaming', false),
      J.flag(json, 'allowCrossRegionFallback', true),
      J.integer(json, 'canaryPercent', 0).clamp(0, 100).toInt(),
      J.integer(json, 'shadowPercent', 0).clamp(0, 100).toInt(),
      J.text(json, 'preferredProvider'),
      J.text(json, 'idempotencyKey', J.text(json, 'requestId', generated)),
    );
  }
}

class Snapshot {
  Snapshot(this.endpoints, this.tenants, this.observations, this.nowMs);

  final List<Endpoint> endpoints;
  final Map<String, Tenant> tenants;
  final List<Map<String, dynamic>> observations;
  final int nowMs;

  factory Snapshot.fromJson(Map<String, dynamic> json) {
    final tenantMap = <String, Tenant>{};
    for (final raw in J.list(json['tenantBudgets'] ?? json['tenants'])) {
      final tenant = Tenant.fromJson(J.map(raw, 'tenant'));
      tenantMap[tenant.id] = tenant;
    }
    return Snapshot(
      J.list(json['endpoints'] ?? json['providers'])
          .map((raw) => Endpoint.fromJson(J.map(raw, 'endpoint')))
          .toList(growable: false),
      tenantMap,
      J.list(json['observations'])
          .map((raw) => J.map(raw, 'observation'))
          .toList(growable: false),
      J.integer(json, 'nowEpochMs', DateTime.now().millisecondsSinceEpoch),
    );
  }

  Tenant tenant(String id) {
    return tenants[id] ?? Tenant(id, 0, double.infinity, 50, 0, 1000000);
  }
}

class Score implements Comparable<Score> {
  Score(this.endpoint, this.costUsd, this.latencyMs, this.queueMs, this.risk,
      this.carbonGrams, this.hardFailures, this.warnings, this.value);

  final Endpoint endpoint;
  final double costUsd;
  final double latencyMs;
  final double queueMs;
  final double risk;
  final double carbonGrams;
  final List<String> hardFailures;
  final List<String> warnings;
  final double value;

  bool get eligible => hardFailures.isEmpty;

  @override
  int compareTo(Score other) => value.compareTo(other.value);

  Map<String, Object?> toJson() {
    return {
      'endpointId': endpoint.id,
      'provider': endpoint.provider,
      'region': endpoint.region,
      'eligible': eligible,
      'score': _round(value, 4),
      'costUsd': _round(costUsd, 8),
      'predictedLatencyMs': latencyMs.round(),
      'queueDelayMs': queueMs.round(),
      'riskPoints': _round(risk, 4),
      'carbonGrams': _round(carbonGrams, 4),
      'hardFailures': hardFailures,
      'warnings': warnings,
    };
  }
}

class Decision {
  Decision(this.action, this.requestId, this.tenantId, this.primary, this.fallbacks,
      this.shadow, this.hedgeAfterMs, this.timeoutMs, this.estimatedUsd, this.reason,
      this.notes, this.scores);

  final String action;
  final String requestId;
  final String tenantId;
  final String primary;
  final List<String> fallbacks;
  final String shadow;
  final int hedgeAfterMs;
  final int timeoutMs;
  final double estimatedUsd;
  final String reason;
  final List<String> notes;
  final List<Score> scores;

  bool get rejected => action == 'reject';

  Map<String, Object?> toJson() {
    return {
      'action': action,
      'requestId': requestId,
      'tenantId': tenantId,
      'primaryEndpointId': primary.isEmpty ? null : primary,
      'fallbackEndpointIds': fallbacks,
      'shadowEndpointId': shadow.isEmpty ? null : shadow,
      'hedgeAfterMs': hedgeAfterMs == 0 ? null : hedgeAfterMs,
      'timeoutMs': timeoutMs,
      'estimatedUsd': _round(estimatedUsd, 8),
      'reason': reason,
      'notes': notes,
      'candidateScores': scores.map((score) => score.toJson()).toList(),
    };
  }
}

class GatewayFailoverBudgetGovernor {
  Decision plan(Workload request, Snapshot snapshot) {
    final tenant = snapshot.tenant(request.tenantId);
    final scores = snapshot.endpoints
        .map((endpoint) => _score(endpoint, request, tenant, snapshot))
        .toList()
      ..sort();
    if (tenant.saturated) {
      return _reject(request, scores, 'tenant_concurrency_limit',
          ['tenant ${tenant.id} is already at concurrency limit']);
    }
    final eligible = scores.where((score) => score.eligible).toList();
    if (eligible.isEmpty) {
      final reasons = scores.expand((score) => score.hardFailures).toSet().toList()
        ..sort();
      return _reject(request, scores, 'no_eligible_endpoint',
          reasons.isEmpty ? ['no endpoint inventory supplied'] : reasons);
    }
    final primary = eligible.first;
    final fallbacks = eligible
        .skip(1)
        .where((score) => _pairedCostFits(primary, score, request, tenant))
        .take(2)
        .map((score) => score.endpoint.id)
        .toList(growable: false);
    final hedgeMs = _hedgeMs(request, primary, fallbacks.isNotEmpty);
    final timeoutMs =
        math.min(request.deadlineMs, math.max(100, (primary.latencyMs * 1.35).round()));
    final shadow = _shadowEndpoint(request, primary, eligible);
    final notes = <String>[
      'selected ${primary.endpoint.id} after policy checks, latency, cost, queue pressure, recent failures, and tenant budget pressure',
      if (fallbacks.isEmpty) 'no fallback endpoint stayed inside paired cost limits',
      if (hedgeMs > 0) 'hedge can start after ${hedgeMs}ms to protect tail latency',
      if (shadow.isNotEmpty) 'shadow endpoint $shadow is measurement only',
      ...primary.warnings,
    ];
    return Decision('route', request.id, request.tenantId, primary.endpoint.id, fallbacks,
        shadow, hedgeMs, timeoutMs, primary.costUsd, 'eligible_endpoint_selected',
        notes, scores);
  }

  Score _score(Endpoint endpoint, Workload request, Tenant tenant, Snapshot snapshot) {
    final hard = <String>[];
    final warn = <String>[];
    if (endpoint.id.isEmpty) hard.add('endpoint id is missing');
    if (!endpoint.matches(request.modelFamily)) {
      hard.add('${endpoint.id} does not match ${request.modelFamily}');
    }
    if (request.streaming && !endpoint.streaming) hard.add('${endpoint.id} cannot stream');
    if (endpoint.breakerOpen) hard.add('${endpoint.id} breaker is open');
    if (endpoint.cooldown > snapshot.nowMs) hard.add('${endpoint.id} cooling down');
    if (!_residencyOk(endpoint, request)) hard.add('${endpoint.id} violates residency');
    if (endpoint.canary && !_canaryOk(endpoint, request)) {
      hard.add('${endpoint.id} outside canary allocation');
    }
    final cost = endpoint.costUsd(request.inputTokens, request.outputTokens);
    if (cost > request.maxUsd) hard.add('${endpoint.id} exceeds request cost cap');
    if (tenant.remainingUsd.isFinite && cost > tenant.remainingUsd) {
      hard.add('${endpoint.id} exceeds remaining tenant budget');
    }
    final queueMs = (endpoint.queue + endpoint.inFlight) / endpoint.capacity * 60000.0;
    final recent = _recentPenalty(endpoint.id, snapshot);
    final latency = endpoint.p95 + queueMs + recent * 80.0;
    if (latency > request.deadlineMs) warn.add('${endpoint.id} predicted over deadline');
    if (endpoint.success < 0.95) warn.add('${endpoint.id} success below 95 percent');
    final risk = (1 - endpoint.success) * 900.0 + recent;
    final tail = math.max(0.0, endpoint.p99 - endpoint.p95);
    final preferredPenalty = request.preferredProvider.isNotEmpty &&
            request.preferredProvider != endpoint.provider
        ? 12.0
        : 0.0;
    final regionPenalty = request.region.isNotEmpty && request.region != endpoint.region
        ? request.crossRegion
            ? 8.0
            : 100000.0
        : 0.0;
    final fairness = _fairness(request, tenant);
    final carbon = endpoint.carbon(request.inputTokens, request.outputTokens);
    final value = latency / math.max(1, request.deadlineMs) * 100.0 +
        cost * 70000.0 +
        risk +
        preferredPenalty +
        regionPenalty +
        fairness +
        tail / 35.0 +
        carbon / 40.0 -
        request.priority / 125.0;
    return Score(endpoint, cost, latency, queueMs, risk, carbon, hard, warn, value);
  }

  Decision _reject(
      Workload request, List<Score> scores, String reason, List<String> notes) {
    return Decision('reject', request.id, request.tenantId, '', const [], '', 0,
        request.deadlineMs, 0, reason, notes, scores);
  }

  bool _residencyOk(Endpoint endpoint, Workload request) {
    if (request.allowedZones.isEmpty) return true;
    final endpointZones = endpoint.zones.map((zone) => zone.toLowerCase()).toSet();
    final allowed = request.allowedZones.map((zone) => zone.toLowerCase()).toSet();
    if (endpointZones.any(allowed.contains)) return true;
    return request.crossRegion && request.region.isNotEmpty && endpoint.region == request.region;
  }

  bool _canaryOk(Endpoint endpoint, Workload request) {
    if (!endpoint.canary) return true;
    if (request.canaryPercent <= 0) return false;
    final seed = '${request.tenantId}:${request.idempotencyKey}:${request.id}:${endpoint.id}';
    return _stablePercent(seed) < request.canaryPercent;
  }

  bool _pairedCostFits(Score primary, Score fallback, Workload request, Tenant tenant) {
    final paired = primary.costUsd + fallback.costUsd;
    if (paired > request.maxUsd) return false;
    return !tenant.remainingUsd.isFinite || paired <= tenant.remainingUsd;
  }

  int _hedgeMs(Workload request, Score primary, bool hasFallback) {
    if (!hasFallback || request.deadlineMs < 300) return 0;
    if (primary.latencyMs >= request.deadlineMs) return 0;
    final hedge = (primary.endpoint.p95 * 0.65 + primary.queueMs)
        .round()
        .clamp(50, request.deadlineMs - 50)
        .toInt();
    return hedge >= request.deadlineMs * 0.85 ? 0 : hedge;
  }

  String _shadowEndpoint(Workload request, Score primary, List<Score> eligible) {
    if (request.shadowPercent <= 0) return '';
    if (_stablePercent('${request.tenantId}:${request.id}:shadow') >= request.shadowPercent) {
      return '';
    }
    for (final score in eligible) {
      if (score.endpoint.id != primary.endpoint.id &&
          score.endpoint.provider != primary.endpoint.provider) {
        return score.endpoint.id;
      }
    }
    return '';
  }

  double _recentPenalty(String endpointId, Snapshot snapshot) {
    final cutoff = snapshot.nowMs - 5 * 60 * 1000;
    final recent = snapshot.observations.where((obs) {
      return J.text(obs, 'endpointId', J.text(obs, 'id')) == endpointId &&
          J.integer(obs, 'epochMs', snapshot.nowMs) >= cutoff;
    }).toList(growable: false);
    if (recent.isEmpty) return 0.0;
    final failures = recent
        .where((obs) => !J.flag(obs, 'success', true) || J.integer(obs, 'statusCode', 200) >= 500)
        .length;
    final throttles = recent.where((obs) {
      return J.integer(obs, 'statusCode', 200) == 429 ||
          J.text(obs, 'errorClass').toLowerCase().contains('timeout');
    }).length;
    return failures / recent.length * 120.0 + throttles * 2.5;
  }

  double _fairness(Workload request, Tenant tenant) {
    final priorityGap = tenant.reservedPriority - request.priority;
    final priorityPenalty = priorityGap > 0 ? priorityGap / 2.0 : 0.0;
    final concurrencyPressure = tenant.inFlight / tenant.concurrencyLimit;
    final budgetPressure =
        tenant.limitUsd.isFinite && tenant.limitUsd > 0 ? tenant.usedUsd / tenant.limitUsd : 0.0;
    return priorityPenalty + concurrencyPressure * 25.0 + budgetPressure * 15.0;
  }
}

class Args {
  Args(this.values, this.flags);
  final Map<String, String> values;
  final Set<String> flags;

  static Args parse(List<String> raw) {
    final values = <String, String>{};
    final flags = <String>{};
    for (var index = 0; index < raw.length; index++) {
      final arg = raw[index];
      if (!arg.startsWith('--')) throw FormatException('Unexpected argument $arg');
      final name = arg.substring(2);
      final equalAt = name.indexOf('=');
      if (equalAt >= 0) {
        values[name.substring(0, equalAt)] = name.substring(equalAt + 1);
      } else if (index + 1 < raw.length && !raw[index + 1].startsWith('--')) {
        values[name] = raw[++index];
      } else {
        flags.add(name);
      }
    }
    return Args(values, flags);
  }

  bool has(String name) => flags.contains(name) || values.containsKey(name);
  String? value(String name) => values[name];
}

class LoadedInput {
  LoadedInput(this.snapshot, this.requests);
  final Snapshot snapshot;
  final List<Workload> requests;
}

Future<void> main(List<String> rawArgs) async {
  try {
    final args = Args.parse(rawArgs);
    if (args.has('help')) {
      stdout.writeln(_usage());
      return;
    }
    if (args.has('self-test')) {
      _selfTest();
      stdout.writeln('self-test passed');
      return;
    }
    final loaded = await _load(args);
    final governor = GatewayFailoverBudgetGovernor();
    var rejected = 0;
    for (final request in loaded.requests) {
      final decision = governor.plan(request, loaded.snapshot);
      if (decision.rejected) rejected++;
      stdout.writeln(jsonEncode(decision.toJson()));
    }
    if (args.has('fail-on-reject') && rejected > 0) exitCode = 2;
  } on FormatException catch (error) {
    stderr.writeln('$toolName: ${error.message}');
    exitCode = 64;
  } on FileSystemException catch (error) {
    stderr.writeln('$toolName: ${error.message}: ${error.path}');
    exitCode = 66;
  }
}

Future<LoadedInput> _load(Args args) async {
  if (args.value('snapshot') != null) {
    final snapshotJson =
        J.map(jsonDecode(await File(args.value('snapshot')!).readAsString()), 'snapshot');
    return LoadedInput(
      Snapshot.fromJson(snapshotJson),
      J.list(snapshotJson['requests'])
          .map((item) => Workload.fromJson(J.map(item, 'request')))
          .toList(growable: false),
    );
  }
  final endpointsPath = args.value('endpoints') ?? args.value('providers');
  final requestsPath = args.value('requests');
  if (endpointsPath == null || requestsPath == null) {
    throw const FormatException('Supply --snapshot or --endpoints with --requests');
  }
  final tenantPath = args.value('tenants') ?? args.value('tenant-budgets');
  final observationPath = args.value('observations');
  final snapshot = Snapshot.fromJson({
    'nowEpochMs': DateTime.now().millisecondsSinceEpoch,
    'endpoints': J.list(jsonDecode(await File(endpointsPath).readAsString())),
    'tenantBudgets': tenantPath == null
        ? const <dynamic>[]
        : J.list(jsonDecode(await File(tenantPath).readAsString())),
    'observations': observationPath == null
        ? const <Map<String, dynamic>>[]
        : await _jsonLines(observationPath),
  });
  final requests =
      (await _jsonLines(requestsPath)).map((json) => Workload.fromJson(json)).toList();
  return LoadedInput(snapshot, requests);
}

Future<List<Map<String, dynamic>>> _jsonLines(String path) async {
  final rows = <Map<String, dynamic>>[];
  var lineNumber = 0;
  await for (final line
      in File(path).openRead().transform(utf8.decoder).transform(const LineSplitter())) {
    lineNumber++;
    final trimmed = line.trim();
    if (trimmed.isEmpty || trimmed.startsWith('#')) continue;
    rows.add(J.map(jsonDecode(trimmed), '$path:$lineNumber'));
  }
  return rows;
}

String _usage() {
  return [
    'Usage:',
    '  dart GatewayFailoverBudgetGovernor.dart --self-test',
    '  dart GatewayFailoverBudgetGovernor.dart --snapshot snapshot.json',
    '  dart GatewayFailoverBudgetGovernor.dart --endpoints endpoints.json --requests requests.jsonl',
  ].join('\n');
}

void _selfTest() {
  final now = DateTime.utc(2026, 4, 21, 12).millisecondsSinceEpoch;
  final snapshot = Snapshot.fromJson({
    'nowEpochMs': now,
    'endpoints': [
      {
        'endpointId': 'openai-us-primary',
        'provider': 'openai',
        'model': 'frontier-reasoning',
        'region': 'us-east',
        'residencyZones': ['us'],
        'supportsStreaming': true,
        'p95LatencyMs': 850,
        'p99LatencyMs': 1300,
        'successRate': 0.992,
        'capacityPerMinute': 900,
        'costPerMillionInputTokens': 1.1,
        'costPerMillionOutputTokens': 4.4,
      },
      {
        'endpointId': 'anthropic-eu-fallback',
        'provider': 'anthropic',
        'model': 'frontier-reasoning',
        'region': 'eu-west',
        'residencyZones': ['eu'],
        'supportsStreaming': true,
        'p95LatencyMs': 980,
        'p99LatencyMs': 1550,
        'successRate': 0.989,
        'capacityPerMinute': 700,
        'costPerMillionInputTokens': 1.0,
        'costPerMillionOutputTokens': 4.2,
      },
      {
        'endpointId': 'lab-canary-us',
        'provider': 'lab',
        'model': 'frontier-reasoning',
        'region': 'us-east',
        'residencyZones': ['us'],
        'tags': ['canary'],
        'p95LatencyMs': 700,
        'p99LatencyMs': 2400,
        'successRate': 0.970,
        'capacityPerMinute': 100,
        'costPerMillionInputTokens': 0.7,
        'costPerMillionOutputTokens': 2.8,
      }
    ],
    'tenants': [
      {
        'tenantId': 'search',
        'usedUsdToday': 3.2,
        'dailyLimitUsd': 20.0,
        'reservedPriority': 35,
        'inFlight': 2,
        'concurrencyLimit': 20,
      }
    ],
    'observations': [
      {
        'endpointId': 'lab-canary-us',
        'success': false,
        'statusCode': 503,
        'errorClass': 'overloaded',
        'epochMs': now - 30000,
      }
    ],
  });
  final governor = GatewayFailoverBudgetGovernor();
  final routed = governor.plan(Workload.fromJson({
    'requestId': 'req-001',
    'tenantId': 'search',
    'modelFamily': 'frontier-reasoning',
    'inputTokens': 2400,
    'maxOutputTokens': 900,
    'priority': 80,
    'deadlineMs': 2200,
    'maxUsd': 0.02,
    'requestedRegion': 'us-east',
    'allowedResidencyZones': ['us'],
    'requiresStreaming': true,
    'allowCrossRegionFallback': false,
  }), snapshot);
  assert(!routed.rejected);
  assert(routed.primary == 'openai-us-primary');
  final rejected = governor.plan(Workload.fromJson({
    'requestId': 'req-002',
    'tenantId': 'search',
    'modelFamily': 'frontier-reasoning',
    'inputTokens': 2400,
    'maxOutputTokens': 900,
    'deadlineMs': 2200,
    'maxUsd': 0.000001,
    'allowedResidencyZones': ['us'],
  }), snapshot);
  assert(rejected.rejected);
  final canary = governor.plan(Workload.fromJson({
    'requestId': 'req-003',
    'tenantId': 'search',
    'modelFamily': 'frontier-reasoning',
    'inputTokens': 2400,
    'maxOutputTokens': 900,
    'deadlineMs': 2200,
    'maxUsd': 0.02,
    'allowedResidencyZones': ['us'],
    'canaryPercent': 100,
  }), snapshot);
  assert(canary.scores.any((score) => score.endpoint.id == 'lab-canary-us'));
}

double _clamp01(double value) {
  if (value.isNaN || value < 0) return 0;
  if (value > 1) return 1;
  return value;
}

int _stablePercent(String input) {
  var hash = 2166136261;
  for (final unit in input.codeUnits) {
    hash ^= unit;
    hash = (hash * 16777619) & 0xffffffff;
  }
  return hash % 100;
}

double _round(double value, int digits) {
  if (!value.isFinite) return value;
  final factor = math.pow(10, digits).toDouble();
  return (value * factor).round() / factor;
}

/*
This solves the April 2026 problem where teams run AI inference through several
model providers, regional gateways, edge workers, and private fallback clusters,
but the routing choice is still buried inside retry code that nobody can audit.
Built because Pavan would rather see one plain JSON routing decision than debug
a failed agent run after the bill, the latency spike, and the data residency
mistake have already happened. Use it when you need a production Dart AI gateway
failover planner, LLM latency budget governor, model provider cost ceiling
checker, canary rollout allocator, tenant fairness guard, streaming timeout
planner, or DevOps incident preflight for OpenAI, Anthropic, local models, edge
compute endpoints, and private research clusters. The trick: every candidate is
scored with hard rejection reasons and soft warnings before the primary,
fallback, hedge delay, shadow endpoint, and timeout are chosen, so the system
stays explainable during a real outage. Drop this into a Flutter backend, Dart
edge service, CI preflight, internal developer platform, or model gateway control
plane when you want deterministic routing without pulling in a heavy service
mesh just to answer which model endpoint should receive the next request.
*/
