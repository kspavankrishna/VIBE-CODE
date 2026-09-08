import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;

const String _toolVersion = '1.0.0';

void main(List<String> args) async {
  if (args.contains('--help') || args.contains('-h')) {
    stdout.writeln(_usageText);
    return;
  }

  if (args.contains('--example')) {
    stdout.writeln(const JsonEncoder.withIndent('  ').convert(exampleInput()));
    return;
  }

  final rawInput = await stdin.transform(utf8.decoder).join();
  if (rawInput.trim().isEmpty) {
    stderr.writeln('CarbonAwareEvalPlanner: expected JSON on stdin. Try --example.');
    exitCode = 64;
    return;
  }

  try {
    final decoded = jsonDecode(rawInput);
    final planner = CarbonAwareEvalPlanner.fromJson(decoded);
    final plan = planner.plan();
    stdout.writeln(const JsonEncoder.withIndent('  ').convert(plan.toJson()));
    exitCode = plan.unscheduled.isEmpty ? 0 : 2;
  } on FormatException catch (error) {
    stderr.writeln('CarbonAwareEvalPlanner: invalid input: ${error.message}');
    exitCode = 65;
  } on PlannerException catch (error) {
    stderr.writeln('CarbonAwareEvalPlanner: ${error.message}');
    exitCode = 70;
  }
}

const String _usageText = '''
CarbonAwareEvalPlanner.dart v$_toolVersion

Reads a JSON planning request from stdin and writes a deterministic schedule to stdout.

Usage:
  dart CarbonAwareEvalPlanner.dart < request.json
  dart CarbonAwareEvalPlanner.dart --example

Input shape:
  {
    "options": {
      "slotMinutes": 15,
      "defaultCapacity": 2,
      "carbonPriceUsdPerTonne": 90.0,
      "lowConfidenceCarbonPremium": 0.25
    },
    "forecast": [
      {
        "start": "2026-04-15T00:00:00Z",
        "durationMinutes": 60,
        "carbonGramsPerKwh": 210,
        "priceUsdPerKwh": 0.13,
        "confidence": 0.92,
        "capacity": 2,
        "region": "us-west"
      }
    ],
    "workloads": [
      {
        "id": "nightly-agent-eval",
        "earliest": "2026-04-15T00:00:00Z",
        "deadline": "2026-04-15T08:00:00Z",
        "durationMinutes": 90,
        "averageWatts": 420,
        "priority": 80,
        "preemptible": true,
        "tags": ["eval", "github-actions"]
      }
    ]
  }
''';

Map<String, Object?> exampleInput() => <String, Object?>{
      'options': <String, Object?>{
        'slotMinutes': 15,
        'defaultCapacity': 2,
        'carbonPriceUsdPerTonne': 90.0,
        'lowConfidenceCarbonPremium': 0.30,
      },
      'forecast': <Object?>[
        for (var hour = 0; hour < 12; hour++)
          <String, Object?>{
            'start': DateTime.utc(2026, 4, 15, hour).toIso8601String(),
            'durationMinutes': 60,
            'carbonGramsPerKwh': hour >= 2 && hour <= 6 ? 95 : 245,
            'priceUsdPerKwh': hour >= 2 && hour <= 6 ? 0.08 : 0.17,
            'confidence': hour >= 9 ? 0.70 : 0.93,
            'capacity': hour == 3 ? 1 : 2,
            'region': 'us-west',
          },
      ],
      'workloads': <Object?>[
        <String, Object?>{
          'id': 'nightly-rag-regression-eval',
          'earliest': '2026-04-15T00:00:00Z',
          'deadline': '2026-04-15T09:00:00Z',
          'durationMinutes': 105,
          'averageWatts': 540,
          'priority': 95,
          'preemptible': true,
          'tags': <String>['ai-eval', 'rag', 'ci'],
        },
        <String, Object?>{
          'id': 'embedding-backfill-shard-17',
          'earliest': '2026-04-15T01:00:00Z',
          'deadline': '2026-04-15T11:30:00Z',
          'durationMinutes': 180,
          'averageWatts': 310,
          'priority': 55,
          'preemptible': false,
          'tags': <String>['embeddings', 'batch'],
        },
      ],
    };

final RegExp _utcSuffix = RegExp(r'(Z|z|[+-][0-9][0-9]:?[0-9][0-9])$');
final RegExp _safeId = RegExp(r'^[A-Za-z0-9][A-Za-z0-9._:@/+ -]{0,159}$');

class PlannerException implements Exception {
  PlannerException(this.message);
  final String message;

  @override
  String toString() => message;
}

class CarbonAwareEvalPlanner {
  CarbonAwareEvalPlanner({
    required this.options,
    required this.forecast,
    required this.workloads,
  });

  factory CarbonAwareEvalPlanner.fromJson(Object? value) {
    final map = _asMap(value, 'request');
    final options = PlanOptions.fromJson(map['options']);
    final forecast = _asList(map['forecast'], 'forecast')
        .map((entry) => ForecastPoint.fromJson(entry, options))
        .toList(growable: false);
    final workloads = _asList(map['workloads'], 'workloads')
        .map(Workload.fromJson)
        .toList(growable: false);

    if (forecast.isEmpty) {
      throw const FormatException('forecast must contain at least one point');
    }
    if (workloads.isEmpty) {
      throw const FormatException('workloads must contain at least one job');
    }

    return CarbonAwareEvalPlanner(
      options: options,
      forecast: forecast,
      workloads: workloads,
    );
  }

  final PlanOptions options;
  final List<ForecastPoint> forecast;
  final List<Workload> workloads;

  PlanResult plan() {
    final slots = _expandForecast();
    if (slots.isEmpty) {
      throw PlannerException('forecast expands to zero slots');
    }

    final state = _PlanningState(slots);
    final orderedWorkloads = workloads.toList(growable: false)
      ..sort(_compareWorkloadOrder);

    final scheduled = <ScheduledWorkload>[];
    final unscheduled = <UnscheduledWorkload>[];

    for (final workload in orderedWorkloads) {
      final decision = workload.preemptible
          ? _schedulePreemptible(workload, state)
          : _scheduleContiguous(workload, state);
      if (decision == null) {
        unscheduled.add(UnscheduledWorkload(
          id: workload.id,
          reason: _unscheduledReason(workload, slots),
          priority: workload.priority,
          requestedMinutes: workload.durationMinutes,
        ));
        continue;
      }
      state.claim(decision.assignments);
      scheduled.add(decision);
    }

    scheduled.sort((a, b) {
      final byStart = a.start.compareTo(b.start);
      if (byStart != 0) return byStart;
      return a.id.compareTo(b.id);
    });

    return PlanResult(
      version: _toolVersion,
      generatedAt: DateTime.now().toUtc(),
      slotMinutes: options.slotMinutes,
      scheduled: scheduled,
      unscheduled: unscheduled,
      slotUtilization: state.utilizationReport(),
    );
  }

  List<ForecastSlot> _expandForecast() {
    final byStartMillis = <int, ForecastSlot>{};
    for (final point in forecast) {
      for (final slot in point.expand(options.slotMinutes)) {
        byStartMillis[slot.start.millisecondsSinceEpoch] = slot;
      }
    }
    final expanded = byStartMillis.values.toList(growable: false)
      ..sort((a, b) => a.start.compareTo(b.start));
    return expanded;
  }

  ScheduledWorkload? _scheduleContiguous(Workload workload, _PlanningState state) {
    final neededSlots = _ceilDiv(workload.durationMinutes, options.slotMinutes);
    _CandidateBlock? best;

    for (var startIndex = 0; startIndex <= state.slots.length - neededSlots; startIndex++) {
      final assignments = <SlotAssignment>[];
      var remainingMinutes = workload.durationMinutes;
      var score = 0.0;
      var co2Grams = 0.0;
      var energyKwh = 0.0;
      var powerCostUsd = 0.0;
      var fits = true;

      for (var offset = 0; offset < neededSlots; offset++) {
        final slotIndex = startIndex + offset;
        final slot = state.slots[slotIndex];
        final minutes = math.min(options.slotMinutes, remainingMinutes);
        final chunkEnd = slot.start.add(Duration(minutes: minutes));

        if (slot.start.isBefore(workload.earliest) || chunkEnd.isAfter(workload.deadline)) {
          fits = false;
          break;
        }
        if (!state.hasCapacity(slotIndex)) {
          fits = false;
          break;
        }

        final metrics = slot.metricsFor(workload, minutes, options, state.used(slotIndex));
        score += metrics.score;
        co2Grams += metrics.co2Grams;
        energyKwh += metrics.energyKwh;
        powerCostUsd += metrics.powerCostUsd;
        assignments.add(SlotAssignment(
          slotIndex: slotIndex,
          start: slot.start,
          minutes: minutes,
          region: slot.region,
          carbonGramsPerKwh: slot.carbonGramsPerKwh,
          priceUsdPerKwh: slot.priceUsdPerKwh,
        ));
        remainingMinutes -= minutes;
      }

      if (!fits || remainingMinutes > 0) {
        continue;
      }

      final candidate = _CandidateBlock(
        assignments: assignments,
        score: score + workload.contiguityPenalty,
        co2Grams: co2Grams,
        energyKwh: energyKwh,
        powerCostUsd: powerCostUsd,
      );
      if (best == null || candidate.compareTo(best) < 0) {
        best = candidate;
      }
    }

    return best == null ? null : _buildScheduledWorkload(workload, best);
  }

  ScheduledWorkload? _schedulePreemptible(Workload workload, _PlanningState state) {
    final candidates = <_CandidateSlot>[];
    for (var index = 0; index < state.slots.length; index++) {
      final slot = state.slots[index];
      if (!state.hasCapacity(index)) {
        continue;
      }
      if (slot.start.isBefore(workload.earliest)) {
        continue;
      }
      if (slot.start.add(Duration(minutes: options.slotMinutes)).isAfter(workload.deadline)) {
        continue;
      }
      final metrics = slot.metricsFor(workload, options.slotMinutes, options, state.used(index));
      candidates.add(_CandidateSlot(index: index, slot: slot, metrics: metrics));
    }

    candidates.sort((a, b) {
      final byScore = a.metrics.score.compareTo(b.metrics.score);
      if (byScore != 0) return byScore;
      return a.slot.start.compareTo(b.slot.start);
    });

    final neededSlots = _ceilDiv(workload.durationMinutes, options.slotMinutes);
    if (candidates.length < neededSlots) {
      return null;
    }

    var remainingMinutes = workload.durationMinutes;
    var score = 0.0;
    var co2Grams = 0.0;
    var energyKwh = 0.0;
    var powerCostUsd = 0.0;
    final assignments = <SlotAssignment>[];

    for (final candidate in candidates.take(neededSlots)) {
      final minutes = math.min(options.slotMinutes, remainingMinutes);
      final metrics = candidate.slot.metricsFor(workload, minutes, options, state.used(candidate.index));
      score += metrics.score;
      co2Grams += metrics.co2Grams;
      energyKwh += metrics.energyKwh;
      powerCostUsd += metrics.powerCostUsd;
      assignments.add(SlotAssignment(
        slotIndex: candidate.index,
        start: candidate.slot.start,
        minutes: minutes,
        region: candidate.slot.region,
        carbonGramsPerKwh: candidate.slot.carbonGramsPerKwh,
        priceUsdPerKwh: candidate.slot.priceUsdPerKwh,
      ));
      remainingMinutes -= minutes;
    }

    if (remainingMinutes > 0) {
      return null;
    }

    assignments.sort((a, b) => a.start.compareTo(b.start));
    return _buildScheduledWorkload(
      workload,
      _CandidateBlock(
        assignments: assignments,
        score: score,
        co2Grams: co2Grams,
        energyKwh: energyKwh,
        powerCostUsd: powerCostUsd,
      ),
    );
  }

  ScheduledWorkload _buildScheduledWorkload(Workload workload, _CandidateBlock block) {
    final start = block.assignments.first.start;
    final last = block.assignments.last;
    final end = last.start.add(Duration(minutes: last.minutes));
    return ScheduledWorkload(
      id: workload.id,
      start: start,
      end: end,
      requestedMinutes: workload.durationMinutes,
      priority: workload.priority,
      preemptible: workload.preemptible,
      tags: workload.tags,
      assignments: block.assignments,
      estimatedEnergyKwh: _round(block.energyKwh, 6),
      estimatedCo2Grams: _round(block.co2Grams, 3),
      estimatedPowerCostUsd: _round(block.powerCostUsd, 6),
      plannerScore: _round(block.score, 6),
    );
  }

  String _unscheduledReason(Workload workload, List<ForecastSlot> slots) {
    final windowSlots = slots.where((slot) {
      if (slot.start.isBefore(workload.earliest)) return false;
      if (slot.start.add(Duration(minutes: options.slotMinutes)).isAfter(workload.deadline)) return false;
      return true;
    }).length;
    final neededSlots = _ceilDiv(workload.durationMinutes, options.slotMinutes);
    if (windowSlots < neededSlots) {
      return 'deadline window has $windowSlots usable slots, needs $neededSlots';
    }
    if (!workload.preemptible) {
      return 'no contiguous capacity block fits deadline window';
    }
    return 'not enough remaining capacity inside deadline window';
  }
}

class PlanOptions {
  const PlanOptions({
    required this.slotMinutes,
    required this.defaultCapacity,
    required this.carbonPriceUsdPerTonne,
    required this.lowConfidenceCarbonPremium,
    required this.occupancyPenalty,
  });

  factory PlanOptions.fromJson(Object? value) {
    final map = value == null ? <String, Object?>{} : _asMap(value, 'options');
    final options = PlanOptions(
      slotMinutes: _intField(map, 'slotMinutes', defaultValue: 15),
      defaultCapacity: _intField(map, 'defaultCapacity', defaultValue: 1),
      carbonPriceUsdPerTonne: _doubleField(map, 'carbonPriceUsdPerTonne', defaultValue: 0.0),
      lowConfidenceCarbonPremium: _doubleField(map, 'lowConfidenceCarbonPremium', defaultValue: 0.25),
      occupancyPenalty: _doubleField(map, 'occupancyPenalty', defaultValue: 0.015),
    );
    if (options.slotMinutes < 1 || options.slotMinutes > 1440) {
      throw const FormatException('options.slotMinutes must be between 1 and 1440');
    }
    if (options.defaultCapacity < 1) {
      throw const FormatException('options.defaultCapacity must be at least 1');
    }
    if (options.carbonPriceUsdPerTonne < 0) {
      throw const FormatException('options.carbonPriceUsdPerTonne cannot be negative');
    }
    if (options.lowConfidenceCarbonPremium < 0 || options.lowConfidenceCarbonPremium > 5) {
      throw const FormatException('options.lowConfidenceCarbonPremium must be between 0 and 5');
    }
    if (options.occupancyPenalty < 0 || options.occupancyPenalty > 10) {
      throw const FormatException('options.occupancyPenalty must be between 0 and 10');
    }
    return options;
  }

  final int slotMinutes;
  final int defaultCapacity;
  final double carbonPriceUsdPerTonne;
  final double lowConfidenceCarbonPremium;
  final double occupancyPenalty;
}

class ForecastPoint {
  ForecastPoint({
    required this.start,
    required this.durationMinutes,
    required this.carbonGramsPerKwh,
    required this.priceUsdPerKwh,
    required this.confidence,
    required this.capacity,
    required this.region,
  });

  factory ForecastPoint.fromJson(Object? value, PlanOptions options) {
    final map = _asMap(value, 'forecast item');
    final point = ForecastPoint(
      start: _dateField(map, 'start'),
      durationMinutes: _intField(map, 'durationMinutes', defaultValue: options.slotMinutes),
      carbonGramsPerKwh: _doubleField(map, 'carbonGramsPerKwh'),
      priceUsdPerKwh: _doubleField(map, 'priceUsdPerKwh', defaultValue: 0.0),
      confidence: _doubleField(map, 'confidence', defaultValue: 1.0),
      capacity: _intField(map, 'capacity', defaultValue: options.defaultCapacity),
      region: _stringField(map, 'region', defaultValue: 'default'),
    );
    if (point.durationMinutes < options.slotMinutes || point.durationMinutes % options.slotMinutes != 0) {
      throw FormatException('forecast durationMinutes must be a positive multiple of ${options.slotMinutes}');
    }
    if (point.carbonGramsPerKwh < 0 || point.carbonGramsPerKwh > 2500) {
      throw const FormatException('forecast carbonGramsPerKwh must be between 0 and 2500');
    }
    if (point.priceUsdPerKwh < 0 || point.priceUsdPerKwh > 100) {
      throw const FormatException('forecast priceUsdPerKwh must be between 0 and 100');
    }
    if (point.confidence < 0 || point.confidence > 1) {
      throw const FormatException('forecast confidence must be between 0 and 1');
    }
    if (point.capacity < 1) {
      throw const FormatException('forecast capacity must be at least 1');
    }
    return point;
  }

  final DateTime start;
  final int durationMinutes;
  final double carbonGramsPerKwh;
  final double priceUsdPerKwh;
  final double confidence;
  final int capacity;
  final String region;

  Iterable<ForecastSlot> expand(int slotMinutes) sync* {
    for (var offset = 0; offset < durationMinutes; offset += slotMinutes) {
      yield ForecastSlot(
        start: start.add(Duration(minutes: offset)),
        minutes: slotMinutes,
        carbonGramsPerKwh: carbonGramsPerKwh,
        priceUsdPerKwh: priceUsdPerKwh,
        confidence: confidence,
        capacity: capacity,
        region: region,
      );
    }
  }
}

class ForecastSlot {
  const ForecastSlot({
    required this.start,
    required this.minutes,
    required this.carbonGramsPerKwh,
    required this.priceUsdPerKwh,
    required this.confidence,
    required this.capacity,
    required this.region,
  });

  final DateTime start;
  final int minutes;
  final double carbonGramsPerKwh;
  final double priceUsdPerKwh;
  final double confidence;
  final int capacity;
  final String region;

  _SlotMetrics metricsFor(Workload workload, int assignedMinutes, PlanOptions options, int alreadyUsed) {
    final energyKwh = workload.averageWatts * assignedMinutes / 60000.0;
    final effectiveCarbon = carbonGramsPerKwh *
        (1 + ((1 - confidence) * options.lowConfidenceCarbonPremium));
    final co2Grams = energyKwh * carbonGramsPerKwh;
    final powerCostUsd = energyKwh * priceUsdPerKwh;
    final carbonCostUsd = co2Grams / 1000000.0 * options.carbonPriceUsdPerTonne;
    final carbonComponent = effectiveCarbon * energyKwh * workload.carbonWeight;
    final priceComponent = powerCostUsd * 1000 * workload.priceWeight;
    final occupancyComponent = alreadyUsed * options.occupancyPenalty;
    final urgencyComponent = (101 - workload.priority) * 0.0001;
    return _SlotMetrics(
      energyKwh: energyKwh,
      co2Grams: co2Grams,
      powerCostUsd: powerCostUsd,
      score: carbonComponent + priceComponent + carbonCostUsd + occupancyComponent + urgencyComponent,
    );
  }
}

class Workload {
  Workload({
    required this.id,
    required this.earliest,
    required this.deadline,
    required this.durationMinutes,
    required this.averageWatts,
    required this.priority,
    required this.preemptible,
    required this.tags,
    required this.carbonWeight,
    required this.priceWeight,
    required this.contiguityPenalty,
  });

  factory Workload.fromJson(Object? value) {
    final map = _asMap(value, 'workload item');
    final id = _stringField(map, 'id');
    if (!_safeId.hasMatch(id)) {
      throw FormatException('workload id "$id" contains unsupported characters or is too long');
    }
    final workload = Workload(
      id: id,
      earliest: _dateField(map, 'earliest'),
      deadline: _dateField(map, 'deadline'),
      durationMinutes: _intField(map, 'durationMinutes'),
      averageWatts: _doubleField(map, 'averageWatts'),
      priority: _intField(map, 'priority', defaultValue: 50),
      preemptible: _boolField(map, 'preemptible', defaultValue: true),
      tags: _stringListField(map, 'tags'),
      carbonWeight: _doubleField(map, 'carbonWeight', defaultValue: 1.0),
      priceWeight: _doubleField(map, 'priceWeight', defaultValue: 1.0),
      contiguityPenalty: _doubleField(map, 'contiguityPenalty', defaultValue: 0.0),
    );
    if (!workload.deadline.isAfter(workload.earliest)) {
      throw FormatException('workload ${workload.id} deadline must be after earliest');
    }
    if (workload.durationMinutes < 1) {
      throw FormatException('workload ${workload.id} durationMinutes must be at least 1');
    }
    if (workload.averageWatts <= 0 || workload.averageWatts > 1000000) {
      throw FormatException('workload ${workload.id} averageWatts must be between 0 and 1000000');
    }
    if (workload.priority < 0 || workload.priority > 100) {
      throw FormatException('workload ${workload.id} priority must be between 0 and 100');
    }
    if (workload.carbonWeight < 0 || workload.priceWeight < 0) {
      throw FormatException('workload ${workload.id} weights cannot be negative');
    }
    if (workload.contiguityPenalty < 0) {
      throw FormatException('workload ${workload.id} contiguityPenalty cannot be negative');
    }
    return workload;
  }

  final String id;
  final DateTime earliest;
  final DateTime deadline;
  final int durationMinutes;
  final double averageWatts;
  final int priority;
  final bool preemptible;
  final List<String> tags;
  final double carbonWeight;
  final double priceWeight;
  final double contiguityPenalty;

  int slackMinutes(int slotMinutes) {
    final window = deadline.difference(earliest).inMinutes;
    return window - _ceilDiv(durationMinutes, slotMinutes) * slotMinutes;
  }
}

class SlotAssignment {
  const SlotAssignment({
    required this.slotIndex,
    required this.start,
    required this.minutes,
    required this.region,
    required this.carbonGramsPerKwh,
    required this.priceUsdPerKwh,
  });

  final int slotIndex;
  final DateTime start;
  final int minutes;
  final String region;
  final double carbonGramsPerKwh;
  final double priceUsdPerKwh;

  Map<String, Object?> toJson() => <String, Object?>{
        'start': start.toIso8601String(),
        'minutes': minutes,
        'region': region,
        'carbonGramsPerKwh': _round(carbonGramsPerKwh, 3),
        'priceUsdPerKwh': _round(priceUsdPerKwh, 6),
      };
}

class ScheduledWorkload {
  const ScheduledWorkload({
    required this.id,
    required this.start,
    required this.end,
    required this.requestedMinutes,
    required this.priority,
    required this.preemptible,
    required this.tags,
    required this.assignments,
    required this.estimatedEnergyKwh,
    required this.estimatedCo2Grams,
    required this.estimatedPowerCostUsd,
    required this.plannerScore,
  });

  final String id;
  final DateTime start;
  final DateTime end;
  final int requestedMinutes;
  final int priority;
  final bool preemptible;
  final List<String> tags;
  final List<SlotAssignment> assignments;
  final double estimatedEnergyKwh;
  final double estimatedCo2Grams;
  final double estimatedPowerCostUsd;
  final double plannerScore;

  Map<String, Object?> toJson() => <String, Object?>{
        'id': id,
        'start': start.toIso8601String(),
        'end': end.toIso8601String(),
        'requestedMinutes': requestedMinutes,
        'priority': priority,
        'preemptible': preemptible,
        'tags': tags,
        'estimatedEnergyKwh': estimatedEnergyKwh,
        'estimatedCo2Grams': estimatedCo2Grams,
        'estimatedPowerCostUsd': estimatedPowerCostUsd,
        'plannerScore': plannerScore,
        'assignments': assignments.map((assignment) => assignment.toJson()).toList(growable: false),
      };
}

class UnscheduledWorkload {
  const UnscheduledWorkload({
    required this.id,
    required this.reason,
    required this.priority,
    required this.requestedMinutes,
  });

  final String id;
  final String reason;
  final int priority;
  final int requestedMinutes;

  Map<String, Object?> toJson() => <String, Object?>{
        'id': id,
        'reason': reason,
        'priority': priority,
        'requestedMinutes': requestedMinutes,
      };
}

class PlanResult {
  const PlanResult({
    required this.version,
    required this.generatedAt,
    required this.slotMinutes,
    required this.scheduled,
    required this.unscheduled,
    required this.slotUtilization,
  });

  final String version;
  final DateTime generatedAt;
  final int slotMinutes;
  final List<ScheduledWorkload> scheduled;
  final List<UnscheduledWorkload> unscheduled;
  final List<Map<String, Object?>> slotUtilization;

  Map<String, Object?> toJson() {
    final totalEnergyKwh = scheduled.fold<double>(0, (sum, item) => sum + item.estimatedEnergyKwh);
    final totalCo2Grams = scheduled.fold<double>(0, (sum, item) => sum + item.estimatedCo2Grams);
    final totalPowerCostUsd = scheduled.fold<double>(0, (sum, item) => sum + item.estimatedPowerCostUsd);
    return <String, Object?>{
      'version': version,
      'generatedAt': generatedAt.toIso8601String(),
      'slotMinutes': slotMinutes,
      'summary': <String, Object?>{
        'scheduledCount': scheduled.length,
        'unscheduledCount': unscheduled.length,
        'estimatedEnergyKwh': _round(totalEnergyKwh, 6),
        'estimatedCo2Grams': _round(totalCo2Grams, 3),
        'estimatedPowerCostUsd': _round(totalPowerCostUsd, 6),
      },
      'scheduled': scheduled.map((item) => item.toJson()).toList(growable: false),
      'unscheduled': unscheduled.map((item) => item.toJson()).toList(growable: false),
      'slotUtilization': slotUtilization,
    };
  }
}

class _PlanningState {
  _PlanningState(this.slots) : _used = List<int>.filled(slots.length, 0);

  final List<ForecastSlot> slots;
  final List<int> _used;

  bool hasCapacity(int index) => _used[index] < slots[index].capacity;

  int used(int index) => _used[index];

  void claim(List<SlotAssignment> assignments) {
    for (final assignment in assignments) {
      final index = assignment.slotIndex;
      if (!hasCapacity(index)) {
        throw PlannerException('internal planner capacity error at ${slots[index].start.toIso8601String()}');
      }
      _used[index] += 1;
    }
  }

  List<Map<String, Object?>> utilizationReport() {
    final active = <Map<String, Object?>>[];
    for (var index = 0; index < slots.length; index++) {
      if (_used[index] == 0) continue;
      final slot = slots[index];
      active.add(<String, Object?>{
        'start': slot.start.toIso8601String(),
        'used': _used[index],
        'capacity': slot.capacity,
        'region': slot.region,
      });
    }
    return active;
  }
}

class _CandidateSlot {
  const _CandidateSlot({
    required this.index,
    required this.slot,
    required this.metrics,
  });

  final int index;
  final ForecastSlot slot;
  final _SlotMetrics metrics;
}

class _CandidateBlock implements Comparable<_CandidateBlock> {
  const _CandidateBlock({
    required this.assignments,
    required this.score,
    required this.co2Grams,
    required this.energyKwh,
    required this.powerCostUsd,
  });

  final List<SlotAssignment> assignments;
  final double score;
  final double co2Grams;
  final double energyKwh;
  final double powerCostUsd;

  @override
  int compareTo(_CandidateBlock other) {
    final byScore = score.compareTo(other.score);
    if (byScore != 0) return byScore;
    final byCo2 = co2Grams.compareTo(other.co2Grams);
    if (byCo2 != 0) return byCo2;
    return assignments.first.start.compareTo(other.assignments.first.start);
  }
}

class _SlotMetrics {
  const _SlotMetrics({
    required this.energyKwh,
    required this.co2Grams,
    required this.powerCostUsd,
    required this.score,
  });

  final double energyKwh;
  final double co2Grams;
  final double powerCostUsd;
  final double score;
}

int _compareWorkloadOrder(Workload a, Workload b) {
  final aSlack = a.slackMinutes(15);
  final bSlack = b.slackMinutes(15);
  final bySlack = aSlack.compareTo(bSlack);
  if (bySlack != 0) return bySlack;
  final byDeadline = a.deadline.compareTo(b.deadline);
  if (byDeadline != 0) return byDeadline;
  final byPriority = b.priority.compareTo(a.priority);
  if (byPriority != 0) return byPriority;
  final byDuration = b.durationMinutes.compareTo(a.durationMinutes);
  if (byDuration != 0) return byDuration;
  return a.id.compareTo(b.id);
}

Map<String, Object?> _asMap(Object? value, String field) {
  if (value is! Map) {
    throw FormatException('$field must be a JSON object');
  }
  return value.map((key, entry) => MapEntry(key.toString(), entry));
}

List<Object?> _asList(Object? value, String field) {
  if (value is! List) {
    throw FormatException('$field must be a JSON array');
  }
  return value.cast<Object?>();
}

String _stringField(Map<String, Object?> map, String key, {String? defaultValue}) {
  final value = map[key];
  if (value == null) {
    if (defaultValue != null) return defaultValue;
    throw FormatException('$key is required');
  }
  if (value is! String || value.trim().isEmpty) {
    throw FormatException('$key must be a non-empty string');
  }
  return value.trim();
}

int _intField(Map<String, Object?> map, String key, {int? defaultValue}) {
  final value = map[key];
  if (value == null) {
    if (defaultValue != null) return defaultValue;
    throw FormatException('$key is required');
  }
  if (value is int) return value;
  if (value is num && value.isFinite && value == value.roundToDouble()) {
    return value.toInt();
  }
  throw FormatException('$key must be an integer');
}

double _doubleField(Map<String, Object?> map, String key, {double? defaultValue}) {
  final value = map[key];
  if (value == null) {
    if (defaultValue != null) return defaultValue;
    throw FormatException('$key is required');
  }
  if (value is num && value.isFinite) return value.toDouble();
  throw FormatException('$key must be a finite number');
}

bool _boolField(Map<String, Object?> map, String key, {required bool defaultValue}) {
  final value = map[key];
  if (value == null) return defaultValue;
  if (value is bool) return value;
  throw FormatException('$key must be a boolean');
}

List<String> _stringListField(Map<String, Object?> map, String key) {
  final value = map[key];
  if (value == null) return const <String>[];
  if (value is! List) {
    throw FormatException('$key must be an array of strings');
  }
  return value.map((entry) {
    if (entry is! String || entry.trim().isEmpty) {
      throw FormatException('$key must contain only non-empty strings');
    }
    return entry.trim();
  }).toList(growable: false);
}

DateTime _dateField(Map<String, Object?> map, String key) {
  final text = _stringField(map, key);
  if (!_utcSuffix.hasMatch(text)) {
    throw FormatException('$key must include a timezone, for example 2026-04-15T00:00:00Z');
  }
  final parsed = DateTime.tryParse(text);
  if (parsed == null) {
    throw FormatException('$key must be an ISO-8601 timestamp');
  }
  return parsed.toUtc();
}

int _ceilDiv(int value, int divisor) => (value + divisor - 1) ~/ divisor;

double _round(double value, int places) {
  final factor = math.pow(10, places).toDouble();
  return (value * factor).roundToDouble() / factor;
}

/*
This solves the April 2026 problem of running AI evaluation suites, embedding backfills, data pipeline compaction, GitHub Actions jobs, and edge compute batch work without blindly starting everything during the dirtiest and most expensive grid hours. Built because teams now run real model regression tests, RAG quality checks, tool calling audits, and nightly inference workloads that are useful but not always urgent, and the easy default of "run now" wastes money and co2. Use it when a CI queue, Dart backend, Flutter operations console, serverless worker, or internal developer productivity bot has jobs with an earliest time, deadline, expected watts, and a carbon or electricity forecast. The trick: it converts every forecast point into deterministic capacity slots, scores each slot by carbon intensity, electricity price, forecast confidence, occupancy, deadline pressure, and workload priority, then schedules preemptible work across the cleanest windows while keeping non-preemptible work contiguous. Drop this into a repository when you need a Dart carbon aware scheduler, AI eval batch planner, edge compute cost optimizer, CI carbon budget planner, green software automation helper, or data pipeline job scheduler that is plain JSON in and JSON out. I kept it dependency-free so it is easy to fork from GitHub, easy to paste into a build runner, and easy to run in locked-down automation where adding a package just to sort jobs would be silly.
*/
