/*
 * AgentRunSloGovernor.ts
 *
 * Dependency-free TypeScript for gating AI agent rollouts with paired JSONL
 * telemetry. It compares baseline and candidate runs by run id, measures the
 * real p95 latency statistic through paired bootstrap resampling, and blocks
 * canary promotion when latency, token cost, quality, or error rate regress.
 */

declare const require: any;
declare const process: any;
declare const module: any;

export type Variant = "baseline" | "candidate";
export type OutputFormat = "json" | "markdown";
export type Direction = "higher_is_worse" | "lower_is_worse";

export interface AgentRun {
  runId: string;
  variant: Variant;
  cohort: string;
  latencyMs: number;
  inputTokens: number;
  outputTokens: number;
  costUsd: number;
  qualityScore: number | null;
  failed: boolean;
  timestampMs: number | null;
  line: number;
}

export interface RejectedRecord {
  line: number;
  reason: string;
  raw: string;
}

export interface GateConfig {
  maxP95LatencyDeltaPct: number;
  maxMeanCostDeltaPct: number;
  maxErrorRateDeltaPercentagePoints: number;
  minMeanQualityDelta: number;
  minPairs: number;
  minCohortPairs: number;
  bootstrapRounds: number;
  confidence: number;
  seed?: string;
  strictParsing: boolean;
}

export interface PairedRun {
  key: string;
  cohort: string;
  baseline: AgentRun;
  candidate: AgentRun;
}

export interface Interval {
  lower: number;
  median: number;
  upper: number;
  confidence: number;
}

export interface MetricSummary {
  name: string;
  units: string;
  observed: number;
  interval: Interval;
  threshold: number;
  sampleSize: number;
  direction: Direction;
  passed: boolean;
}

export interface CohortSummary {
  cohort: string;
  pairs: number;
  latencyP95DeltaPct: MetricSummary;
  meanCostDeltaPct: MetricSummary;
  errorRateDeltaPercentagePoints: MetricSummary;
  meanQualityDelta: MetricSummary | null;
}

export interface GateEvaluation {
  status: "pass" | "fail";
  generatedAt: string;
  config: GateConfig;
  totalRecords: number;
  rejectedRecords: RejectedRecord[];
  unpairedRecords: number;
  pairCount: number;
  global: CohortSummary;
  cohorts: CohortSummary[];
  reasons: string[];
  warnings: string[];
}

type AnyRecord = Record<string, unknown>;
type Statistic = (pairs: PairedRun[]) => number;

const DEFAULT_CONFIG: GateConfig = {
  maxP95LatencyDeltaPct: 8,
  maxMeanCostDeltaPct: 3,
  maxErrorRateDeltaPercentagePoints: 0.25,
  minMeanQualityDelta: -0.005,
  minPairs: 200,
  minCohortPairs: 30,
  bootstrapRounds: 2000,
  confidence: 0.95,
  strictParsing: false,
};

export class AgentRunSloGovernor {
  private readonly config: GateConfig;

  constructor(config: Partial<GateConfig> = {}) {
    this.config = validateConfig({ ...DEFAULT_CONFIG, ...config });
  }

  evaluate(runs: AgentRun[], rejectedRecords: RejectedRecord[] = []): GateEvaluation {
    const paired = pairRuns(runs);
    const grouped = groupByCohort(paired.pairs);
    const seed = this.config.seed ?? stableSeedForPairs(paired.pairs, this.config);
    const random = mulberry32(hashString(seed));
    const global = summarizeCohort("all", paired.pairs, this.config, random);
    const cohorts = [...grouped.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([cohort, cohortPairs]) => summarizeCohort(cohort, cohortPairs, this.config, random));

    const reasons: string[] = [];
    const warnings: string[] = [];

    if (paired.pairs.length < this.config.minPairs) {
      reasons.push(`Only ${paired.pairs.length} paired runs were available; require at least ${this.config.minPairs}.`);
    }

    if (rejectedRecords.length > 0) {
      const message = `${rejectedRecords.length} malformed telemetry record(s) were rejected.`;
      if (this.config.strictParsing) {
        reasons.push(message);
      } else {
        warnings.push(message);
      }
    }

    if (paired.unpairedCount > 0) {
      warnings.push(`${paired.unpairedCount} record(s) were not paired by run id and cohort.`);
    }

    collectFailures(global, "global", reasons);
    for (const cohort of cohorts) {
      if (cohort.pairs >= this.config.minCohortPairs) {
        collectFailures(cohort, `cohort ${cohort.cohort}`, reasons);
      } else {
        warnings.push(`Cohort ${cohort.cohort} has ${cohort.pairs} paired run(s); cohort blocking needs ${this.config.minCohortPairs}.`);
      }
    }

    if (global.meanQualityDelta === null) {
      warnings.push("Quality regression was skipped because paired qualityScore values were not present.");
    }

    return {
      status: reasons.length === 0 ? "pass" : "fail",
      generatedAt: new Date().toISOString(),
      config: this.config,
      totalRecords: runs.length + rejectedRecords.length,
      rejectedRecords,
      unpairedRecords: paired.unpairedCount,
      pairCount: paired.pairs.length,
      global,
      cohorts,
      reasons,
      warnings,
    };
  }
}

export function parseJsonlTelemetry(input: string, options: { strict?: boolean } = {}): { runs: AgentRun[]; rejected: RejectedRecord[] } {
  const runs: AgentRun[] = [];
  const rejected: RejectedRecord[] = [];
  const lines = input.split(/\r?\n/);

  for (let index = 0; index < lines.length; index += 1) {
    const line = index + 1;
    const raw = lines[index].trim();
    if (raw.length === 0) {
      continue;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (error) {
      rejected.push({ line, reason: `Invalid JSON: ${messageOf(error)}`, raw });
      if (options.strict) {
        break;
      }
      continue;
    }

    const normalized = normalizeRecord(parsed, line, raw);
    if (normalized.ok) {
      runs.push(normalized.run);
    } else {
      rejected.push({ line, reason: normalized.reason, raw });
      if (options.strict) {
        break;
      }
    }
  }

  return { runs, rejected };
}

export function renderJson(evaluation: GateEvaluation): string {
  return `${JSON.stringify(evaluation, null, 2)}\n`;
}

export function renderMarkdown(evaluation: GateEvaluation): string {
  const rows = [
    metricRow(evaluation.global.latencyP95DeltaPct),
    metricRow(evaluation.global.meanCostDeltaPct),
    metricRow(evaluation.global.errorRateDeltaPercentagePoints),
    evaluation.global.meanQualityDelta ? metricRow(evaluation.global.meanQualityDelta) : null,
  ].filter((row): row is string => row !== null);

  const cohortLines = evaluation.cohorts.map((cohort) => {
    const state = cohortHasFailure(cohort) ? "fail" : "pass";
    return `- ${cohort.cohort}: ${state}, ${cohort.pairs} pairs, p95 ${formatNumber(cohort.latencyP95DeltaPct.observed)}%, cost ${formatNumber(cohort.meanCostDeltaPct.observed)}%`;
  });

  return [
    `# Agent Run SLO Gate: ${evaluation.status.toUpperCase()}`,
    "",
    `Paired runs: ${evaluation.pairCount}`,
    `Rejected records: ${evaluation.rejectedRecords.length}`,
    `Unpaired records: ${evaluation.unpairedRecords}`,
    "",
    "| Metric | Observed | Bootstrap interval | Threshold | Result |",
    "| --- | ---: | ---: | ---: | --- |",
    ...rows,
    "",
    "## Reasons",
    ...(evaluation.reasons.length === 0 ? ["- none"] : evaluation.reasons.map((reason) => `- ${reason}`)),
    "",
    "## Warnings",
    ...(evaluation.warnings.length === 0 ? ["- none"] : evaluation.warnings.map((warning) => `- ${warning}`)),
    "",
    "## Cohorts",
    ...(cohortLines.length === 0 ? ["- none"] : cohortLines),
    "",
  ].join("\n");
}

function validateConfig(config: GateConfig): GateConfig {
  const checks: Array<[string, number]> = [
    ["maxP95LatencyDeltaPct", config.maxP95LatencyDeltaPct],
    ["maxMeanCostDeltaPct", config.maxMeanCostDeltaPct],
    ["maxErrorRateDeltaPercentagePoints", config.maxErrorRateDeltaPercentagePoints],
    ["minMeanQualityDelta", config.minMeanQualityDelta],
    ["minPairs", config.minPairs],
    ["minCohortPairs", config.minCohortPairs],
    ["bootstrapRounds", config.bootstrapRounds],
    ["confidence", config.confidence],
  ];

  for (const [name, value] of checks) {
    if (!Number.isFinite(value)) {
      throw new Error(`${name} must be finite.`);
    }
  }
  if (config.minPairs < 1 || Math.floor(config.minPairs) !== config.minPairs) {
    throw new Error("minPairs must be a positive integer.");
  }
  if (config.minCohortPairs < 1 || Math.floor(config.minCohortPairs) !== config.minCohortPairs) {
    throw new Error("minCohortPairs must be a positive integer.");
  }
  if (config.bootstrapRounds < 200 || Math.floor(config.bootstrapRounds) !== config.bootstrapRounds) {
    throw new Error("bootstrapRounds must be an integer of at least 200.");
  }
  if (config.confidence <= 0 || config.confidence >= 1) {
    throw new Error("confidence must be greater than 0 and less than 1.");
  }
  return config;
}

function normalizeRecord(value: unknown, line: number, raw: string): { ok: true; run: AgentRun } | { ok: false; reason: string } {
  if (!isObject(value)) {
    return { ok: false, reason: "Record must be a JSON object." };
  }

  const runId = readString(value, ["runId", "run_id", "traceId", "trace_id", "requestId", "request_id", "id"]);
  if (runId === null) {
    return { ok: false, reason: "Missing run id. Expected runId, traceId, requestId, or id." };
  }

  const variant = normalizeVariant(readString(value, ["variant", "cell", "arm", "treatment", "rollout"]));
  if (variant === null) {
    return { ok: false, reason: "Missing or invalid variant. Expected baseline/control or candidate/canary." };
  }

  const latencyMs = readNumber(value, ["latencyMs", "latency_ms", "durationMs", "duration_ms", "responseMs", "response_ms"]);
  if (latencyMs === null || latencyMs < 0) {
    return { ok: false, reason: "Missing or invalid non-negative latency in milliseconds." };
  }

  const inputTokens = readNumber(value, ["inputTokens", "input_tokens", "promptTokens", "prompt_tokens"], 0);
  const outputTokens = readNumber(value, ["outputTokens", "output_tokens", "completionTokens", "completion_tokens"], 0);
  const costUsd = readNumber(value, ["costUsd", "cost_usd", "usd", "cost", "estimatedCostUsd", "estimated_cost_usd"], 0);
  const qualityScore = readQuality(value);
  const cohort = readString(value, ["cohort", "segment", "route", "model", "provider", "region"]) ?? "all";

  if (inputTokens === null || outputTokens === null || costUsd === null) {
    return { ok: false, reason: "Token and cost fields must be finite when present." };
  }
  if (inputTokens < 0 || outputTokens < 0 || costUsd < 0) {
    return { ok: false, reason: "Token and cost fields must be non-negative." };
  }
  if (qualityScore !== null && (qualityScore < 0 || qualityScore > 1)) {
    return { ok: false, reason: "qualityScore must be between 0 and 1 when present." };
  }

  return {
    ok: true,
    run: {
      runId,
      variant,
      cohort,
      latencyMs,
      inputTokens,
      outputTokens,
      costUsd,
      qualityScore,
      failed: readFailure(value, raw),
      timestampMs: readTimestamp(value),
      line,
    },
  };
}

function pairRuns(runs: AgentRun[]): { pairs: PairedRun[]; unpairedCount: number } {
  const buckets = new Map<string, { baseline?: AgentRun; candidate?: AgentRun; count: number }>();
  for (const run of runs) {
    const key = `${run.cohort}\u001f${run.runId}`;
    const bucket = buckets.get(key) ?? { count: 0 };
    bucket.count += 1;
    if (run.variant === "baseline") {
      bucket.baseline = chooseLater(bucket.baseline, run);
    } else {
      bucket.candidate = chooseLater(bucket.candidate, run);
    }
    buckets.set(key, bucket);
  }

  const pairs: PairedRun[] = [];
  let pairedRecords = 0;
  for (const [key, bucket] of buckets.entries()) {
    if (bucket.baseline && bucket.candidate) {
      pairs.push({ key, cohort: bucket.baseline.cohort, baseline: bucket.baseline, candidate: bucket.candidate });
      pairedRecords += 2;
    }
  }
  pairs.sort((left, right) => left.key.localeCompare(right.key));
  return { pairs, unpairedCount: Math.max(0, runs.length - pairedRecords) };
}

function chooseLater(existing: AgentRun | undefined, next: AgentRun): AgentRun {
  if (!existing) {
    return next;
  }
  const existingRank = existing.timestampMs ?? existing.line;
  const nextRank = next.timestampMs ?? next.line;
  return nextRank >= existingRank ? next : existing;
}

function groupByCohort(pairs: PairedRun[]): Map<string, PairedRun[]> {
  const grouped = new Map<string, PairedRun[]>();
  for (const pair of pairs) {
    const list = grouped.get(pair.cohort) ?? [];
    list.push(pair);
    grouped.set(pair.cohort, list);
  }
  return grouped;
}

function summarizeCohort(cohort: string, pairs: PairedRun[], config: GateConfig, random: () => number): CohortSummary {
  const qualityPairs = pairs.filter((pair) => pair.baseline.qualityScore !== null && pair.candidate.qualityScore !== null);
  return {
    cohort,
    pairs: pairs.length,
    latencyP95DeltaPct: summarizeMetric({
      name: "p95 latency delta",
      units: "%",
      pairs,
      statistic: latencyP95DeltaPct,
      threshold: config.maxP95LatencyDeltaPct,
      direction: "higher_is_worse",
      config,
      random,
    }),
    meanCostDeltaPct: summarizeMetric({
      name: "mean cost delta",
      units: "%",
      pairs,
      statistic: meanCostDeltaPct,
      threshold: config.maxMeanCostDeltaPct,
      direction: "higher_is_worse",
      config,
      random,
    }),
    errorRateDeltaPercentagePoints: summarizeMetric({
      name: "error rate delta",
      units: "percentage points",
      pairs,
      statistic: errorRateDeltaPercentagePoints,
      threshold: config.maxErrorRateDeltaPercentagePoints,
      direction: "higher_is_worse",
      config,
      random,
    }),
    meanQualityDelta: qualityPairs.length === 0
      ? null
      : summarizeMetric({
          name: "mean quality delta",
          units: "score",
          pairs: qualityPairs,
          statistic: meanQualityDelta,
          threshold: config.minMeanQualityDelta,
          direction: "lower_is_worse",
          config,
          random,
        }),
  };
}

function summarizeMetric(input: {
  name: string;
  units: string;
  pairs: PairedRun[];
  statistic: Statistic;
  threshold: number;
  direction: Direction;
  config: GateConfig;
  random: () => number;
}): MetricSummary {
  const observed = input.pairs.length === 0 ? Number.NaN : input.statistic(input.pairs);
  const interval = input.pairs.length === 0
    ? emptyInterval(input.config.confidence)
    : bootstrapInterval(input.pairs, input.statistic, input.config.bootstrapRounds, input.config.confidence, input.random);
  const passed = input.direction === "higher_is_worse"
    ? interval.upper <= input.threshold
    : interval.lower >= input.threshold;

  return {
    name: input.name,
    units: input.units,
    observed,
    interval,
    threshold: input.threshold,
    sampleSize: input.pairs.length,
    direction: input.direction,
    passed,
  };
}

function bootstrapInterval(pairs: PairedRun[], statistic: Statistic, rounds: number, confidence: number, random: () => number): Interval {
  const values: number[] = [];
  const sample: PairedRun[] = new Array(pairs.length);

  for (let round = 0; round < rounds; round += 1) {
    for (let index = 0; index < pairs.length; index += 1) {
      sample[index] = pairs[Math.floor(random() * pairs.length)];
    }
    const value = statistic(sample);
    if (Number.isFinite(value)) {
      values.push(value);
    }
  }

  values.sort((left, right) => left - right);
  const alpha = 1 - confidence;
  return {
    lower: quantileSorted(values, alpha / 2),
    median: quantileSorted(values, 0.5),
    upper: quantileSorted(values, 1 - alpha / 2),
    confidence,
  };
}

function latencyP95DeltaPct(pairs: PairedRun[]): number {
  const baseline = pairs.map((pair) => pair.baseline.latencyMs).sort(numberOrder);
  const candidate = pairs.map((pair) => pair.candidate.latencyMs).sort(numberOrder);
  return pctDelta(quantileSorted(baseline, 0.95), quantileSorted(candidate, 0.95));
}

function meanCostDeltaPct(pairs: PairedRun[]): number {
  return pctDelta(mean(pairs.map((pair) => pair.baseline.costUsd)), mean(pairs.map((pair) => pair.candidate.costUsd)));
}

function errorRateDeltaPercentagePoints(pairs: PairedRun[]): number {
  const baseline = mean(pairs.map((pair) => pair.baseline.failed ? 1 : 0));
  const candidate = mean(pairs.map((pair) => pair.candidate.failed ? 1 : 0));
  return (candidate - baseline) * 100;
}

function meanQualityDelta(pairs: PairedRun[]): number {
  return mean(pairs.map((pair) => (pair.candidate.qualityScore ?? Number.NaN) - (pair.baseline.qualityScore ?? Number.NaN)));
}

function collectFailures(summary: CohortSummary, label: string, reasons: string[]): void {
  const metrics = [
    summary.latencyP95DeltaPct,
    summary.meanCostDeltaPct,
    summary.errorRateDeltaPercentagePoints,
    summary.meanQualityDelta,
  ].filter((metric): metric is MetricSummary => metric !== null);

  for (const metric of metrics) {
    if (!metric.passed) {
      const bound = metric.direction === "higher_is_worse" ? metric.interval.upper : metric.interval.lower;
      const comparator = metric.direction === "higher_is_worse" ? ">" : "<";
      reasons.push(`${label} ${metric.name} ${formatNumber(bound)} ${metric.units} ${comparator} threshold ${formatNumber(metric.threshold)} ${metric.units}.`);
    }
  }
}

function cohortHasFailure(summary: CohortSummary): boolean {
  return !summary.latencyP95DeltaPct.passed
    || !summary.meanCostDeltaPct.passed
    || !summary.errorRateDeltaPercentagePoints.passed
    || summary.meanQualityDelta?.passed === false;
}

function pctDelta(baseline: number, candidate: number): number {
  if (baseline === 0) {
    return candidate === 0 ? 0 : Number.POSITIVE_INFINITY;
  }
  return ((candidate - baseline) / Math.abs(baseline)) * 100;
}

function mean(values: number[]): number {
  const finite = values.filter(Number.isFinite);
  if (finite.length === 0) {
    return Number.NaN;
  }
  let total = 0;
  for (const value of finite) {
    total += value;
  }
  return total / finite.length;
}

function quantileSorted(values: number[], q: number): number {
  if (values.length === 0) {
    return Number.NaN;
  }
  if (values.length === 1) {
    return values[0];
  }
  const position = (values.length - 1) * Math.max(0, Math.min(1, q));
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  const weight = position - lower;
  return values[lower] * (1 - weight) + values[upper] * weight;
}

function numberOrder(left: number, right: number): number {
  return left - right;
}

function emptyInterval(confidence: number): Interval {
  return { lower: Number.NaN, median: Number.NaN, upper: Number.NaN, confidence };
}

function readString(record: AnyRecord, keys: string[]): string | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      return String(value);
    }
  }
  return null;
}

function readNumber(record: AnyRecord, keys: string[], fallback: number | null = null): number | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim() !== "") {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return fallback;
}

function readQuality(record: AnyRecord): number | null {
  const numeric = readNumber(record, ["qualityScore", "quality_score", "score", "evalScore", "eval_score", "reward"], null);
  if (numeric !== null) {
    return numeric;
  }
  const pass = record["passed"] ?? record["pass"] ?? record["accepted"];
  return typeof pass === "boolean" ? (pass ? 1 : 0) : null;
}

function readFailure(record: AnyRecord, raw: string): boolean {
  const failed = record["failed"] ?? record["error"] ?? record["errored"];
  if (typeof failed === "boolean") {
    return failed;
  }
  const ok = record["ok"] ?? record["success"];
  if (typeof ok === "boolean") {
    return !ok;
  }
  const status = readString(record, ["status", "outcome", "finishReason", "finish_reason"]);
  if (status !== null) {
    const lower = status.toLowerCase();
    if (["error", "failed", "timeout", "cancelled", "canceled", "tool_error", "rate_limited"].includes(lower)) {
      return true;
    }
    if (["ok", "success", "succeeded", "complete", "completed"].includes(lower)) {
      return false;
    }
  }
  return /\b(error|failed|timeout|rate_limited)\b/i.test(raw);
}

function readTimestamp(record: AnyRecord): number | null {
  const numeric = readNumber(record, ["timestampMs", "timestamp_ms", "timeMs", "time_ms"], null);
  if (numeric !== null) {
    return numeric;
  }
  const text = readString(record, ["timestamp", "time", "startedAt", "started_at", "createdAt", "created_at"]);
  if (text === null) {
    return null;
  }
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeVariant(value: string | null): Variant | null {
  if (value === null) {
    return null;
  }
  const lower = value.toLowerCase();
  if (["baseline", "base", "control", "before", "stable", "prod", "production"].includes(lower)) {
    return "baseline";
  }
  if (["candidate", "canary", "treatment", "after", "shadow", "new", "experiment"].includes(lower)) {
    return "candidate";
  }
  return null;
}

function isObject(value: unknown): value is AnyRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function stableSeedForPairs(pairs: PairedRun[], config: GateConfig): string {
  const rows = pairs.map((pair) => [
    pair.key,
    pair.baseline.latencyMs,
    pair.candidate.latencyMs,
    pair.baseline.costUsd,
    pair.candidate.costUsd,
    pair.baseline.qualityScore,
    pair.candidate.qualityScore,
    pair.baseline.failed,
    pair.candidate.failed,
  ].join("|"));
  return `${rows.join("\n")}\n${config.bootstrapRounds}\n${config.confidence}`;
}

function hashString(input: string): number {
  let hash = 2166136261;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6D2B79F5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
}

function metricRow(metric: MetricSummary): string {
  const interval = `[${formatNumber(metric.interval.lower)}, ${formatNumber(metric.interval.upper)}]`;
  return `| ${metric.name} | ${formatNumber(metric.observed)} ${metric.units} | ${interval} ${metric.units} | ${formatNumber(metric.threshold)} ${metric.units} | ${metric.passed ? "pass" : "fail"} |`;
}

function formatNumber(value: number): string {
  if (Number.isNaN(value)) {
    return "n/a";
  }
  if (!Number.isFinite(value)) {
    return value > 0 ? "inf" : "-inf";
  }
  if (Math.abs(value) >= 100) {
    return value.toFixed(1);
  }
  if (Math.abs(value) >= 10) {
    return value.toFixed(2);
  }
  return value.toFixed(4);
}

function parseArgs(argv: string[]): { input: string | null; format: OutputFormat; config: Partial<GateConfig>; selfTest: boolean } {
  const config: Partial<GateConfig> = {};
  let input: string | null = null;
  let format: OutputFormat = "json";
  let selfTest = false;

  for (const arg of argv) {
    if (arg === "--self-test") {
      selfTest = true;
    } else if (arg === "--strict") {
      config.strictParsing = true;
    } else if (arg.startsWith("--input=")) {
      input = arg.slice("--input=".length);
    } else if (arg.startsWith("--format=")) {
      const next = arg.slice("--format=".length);
      if (next !== "json" && next !== "markdown") {
        throw new Error("--format must be json or markdown.");
      }
      format = next;
    } else if (arg.startsWith("--max-p95-latency-delta-pct=")) {
      config.maxP95LatencyDeltaPct = Number(arg.split("=")[1]);
    } else if (arg.startsWith("--max-cost-delta-pct=")) {
      config.maxMeanCostDeltaPct = Number(arg.split("=")[1]);
    } else if (arg.startsWith("--max-error-rate-delta-pp=")) {
      config.maxErrorRateDeltaPercentagePoints = Number(arg.split("=")[1]);
    } else if (arg.startsWith("--min-quality-delta=")) {
      config.minMeanQualityDelta = Number(arg.split("=")[1]);
    } else if (arg.startsWith("--min-pairs=")) {
      config.minPairs = Number(arg.split("=")[1]);
    } else if (arg.startsWith("--min-cohort-pairs=")) {
      config.minCohortPairs = Number(arg.split("=")[1]);
    } else if (arg.startsWith("--bootstrap=")) {
      config.bootstrapRounds = Number(arg.split("=")[1]);
    } else if (arg.startsWith("--confidence=")) {
      config.confidence = Number(arg.split("=")[1]);
    } else if (arg.startsWith("--seed=")) {
      config.seed = arg.slice("--seed=".length);
    } else if (arg === "--help" || arg === "-h") {
      throw new Error(helpText());
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  return { input, format, config, selfTest };
}

function helpText(): string {
  return [
    "Usage: ts-node AgentRunSloGovernor.ts --input=runs.jsonl --format=json",
    "",
    "Input is JSONL with baseline and candidate records sharing a runId and cohort.",
    "Useful fields: runId, variant, cohort, latencyMs, inputTokens, outputTokens, costUsd, qualityScore, failed.",
    "",
    "Options:",
    "  --format=json|markdown",
    "  --max-p95-latency-delta-pct=8",
    "  --max-cost-delta-pct=3",
    "  --max-error-rate-delta-pp=0.25",
    "  --min-quality-delta=-0.005",
    "  --min-pairs=200",
    "  --bootstrap=2000",
    "  --confidence=0.95",
    "  --strict",
    "  --self-test",
  ].join("\n");
}

function buildSelfTestJsonl(): string {
  const rows: string[] = [];
  const random = mulberry32(42);
  for (let index = 0; index < 260; index += 1) {
    const cohort = index % 5 === 0 ? "tool-heavy" : "default";
    const baseLatency = 800 + Math.floor(random() * 220);
    const candidateLatency = baseLatency + Math.floor(random() * 35) - 12;
    const baseCost = 0.014 + random() * 0.003;
    const candidateCost = baseCost * (1 + random() * 0.02);
    const quality = 0.87 + random() * 0.08;
    const id = `run-${index.toString().padStart(4, "0")}`;
    rows.push(JSON.stringify({ runId: id, variant: "baseline", cohort, latencyMs: baseLatency, costUsd: baseCost, qualityScore: quality, failed: false }));
    rows.push(JSON.stringify({ runId: id, variant: "candidate", cohort, latencyMs: candidateLatency, costUsd: candidateCost, qualityScore: quality - 0.001, failed: false }));
  }
  return rows.join("\n");
}

export async function main(argv: string[] = process.argv.slice(2)): Promise<number> {
  const { input, format, config, selfTest } = parseArgs(argv);
  const fs = require("fs");
  const text = selfTest
    ? buildSelfTestJsonl()
    : input === null || input === "-"
      ? fs.readFileSync(0, "utf8")
      : fs.readFileSync(input, "utf8");
  const parser = parseJsonlTelemetry(text, { strict: config.strictParsing });
  const evaluation = new AgentRunSloGovernor(config).evaluate(parser.runs, parser.rejected);
  process.stdout.write(format === "markdown" ? renderMarkdown(evaluation) : renderJson(evaluation));
  return evaluation.status === "pass" ? 0 : 2;
}

if (typeof require !== "undefined" && typeof module !== "undefined" && require.main === module) {
  main().then((code) => {
    process.exitCode = code;
  }).catch((error) => {
    process.stderr.write(`${messageOf(error)}\n`);
    process.exitCode = 1;
  });
}

/*
This solves the boring but painful rollout problem where an AI agent looks fine in a dashboard, then quietly ships a worse p95 latency, a higher LLM token bill, or a lower evaluation score because baseline and candidate runs were compared as loose averages. Built because April 2026 teams are running MCP tools, OpenTelemetry traces, streaming model calls, edge inference routes, and JSONL eval exports, but the promotion gate is often still a spreadsheet or a one-off notebook. Use it when you have paired baseline and candidate agent runs and need a repeatable TypeScript SLO gate for canary release, shadow traffic, LLM cost control, quality regression detection, and production AI workflow reliability. The trick: it pairs by run id and cohort first, then uses deterministic paired bootstrap intervals on the actual gate statistic, so noisy prompts, long-tail tools, provider retries, and cache misses do not hide the real delta. Drop this into a CI job, a GitHub Actions workflow, a Vercel or Netlify build step, an internal AI gateway, or a research eval pipeline that reads JSONL telemetry from stdin and must fail loudly before an expensive or unreliable agent release reaches users.
*/
