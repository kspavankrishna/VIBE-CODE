import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { basename } from 'node:path';

export type FailureCategory =
  | 'auth'
  | 'container'
  | 'contract'
  | 'dependency'
  | 'edge-runtime'
  | 'lint'
  | 'network'
  | 'resource'
  | 'schema'
  | 'secret'
  | 'supply-chain'
  | 'test'
  | 'timeout'
  | 'typecheck'
  | 'unknown';

export type RerunAction =
  | 'do_not_rerun'
  | 'investigate_environment'
  | 'rerun_after_fix'
  | 'rerun_once'
  | 'rerun_with_more_resources';

export interface SourceInput {
  readonly name: string;
  readonly text: string;
}

export interface FailureRule {
  readonly id: string;
  readonly category: FailureCategory;
  readonly severity: number;
  readonly confidence: number;
  readonly reason: string;
  readonly rerun: RerunAction;
  readonly labels: readonly string[];
  readonly patterns: readonly RegExp[];
}

export interface FingerprintOptions {
  readonly maxContextLines?: number;
  readonly maxEvidenceLines?: number;
  readonly maxClusters?: number;
  readonly minSeverity?: number;
  readonly includeLowSignal?: boolean;
  readonly now?: () => Date;
  readonly rules?: readonly FailureRule[];
}

export interface LogIssue {
  readonly id: string;
  readonly source: string;
  readonly line: number;
  readonly category: FailureCategory;
  readonly severity: number;
  readonly confidence: number;
  readonly ruleId: string;
  readonly reason: string;
  readonly rerun: RerunAction;
  readonly labels: readonly string[];
  readonly normalized: string;
  readonly rawRedacted: string;
  readonly context: readonly string[];
  readonly signature: string;
  readonly pathHints: readonly string[];
}

export interface FailureCluster {
  readonly fingerprint: string;
  readonly category: FailureCategory;
  readonly severity: number;
  readonly confidence: number;
  readonly count: number;
  readonly sources: readonly string[];
  readonly firstLine: number;
  readonly ruleIds: readonly string[];
  readonly labels: readonly string[];
  readonly rerun: RerunAction;
  readonly ownerHints: readonly string[];
  readonly evidence: readonly string[];
  readonly pathHints: readonly string[];
}

export interface WorkflowFailureReport {
  readonly generatedAt: string;
  readonly sourceCount: number;
  readonly scannedBytes: number;
  readonly scannedLines: number;
  readonly issueCount: number;
  readonly clusterCount: number;
  readonly suppressedLowSignal: number;
  readonly clusters: readonly FailureCluster[];
  readonly recommendations: readonly string[];
}

interface ResolvedOptions {
  readonly maxContextLines: number;
  readonly maxEvidenceLines: number;
  readonly maxClusters: number;
  readonly minSeverity: number;
  readonly includeLowSignal: boolean;
  readonly now: () => Date;
  readonly rules: readonly FailureRule[];
}

interface RuleMatch {
  readonly rule: FailureRule;
  readonly normalizedLine: string;
  readonly rawRedacted: string;
}

interface CliSource {
  readonly name: string;
  readonly path: string;
}

interface CliConfig {
  readonly help: boolean;
  readonly output: 'json' | 'markdown';
  readonly minSeverity?: number;
  readonly maxContextLines?: number;
  readonly maxClusters?: number;
  readonly stdinName: string;
  readonly files: readonly string[];
  readonly namedSources: readonly CliSource[];
}

const DEFAULT_CONTEXT_LINES = 3;
const DEFAULT_EVIDENCE_LINES = 8;
const DEFAULT_MAX_CLUSTERS = 25;
const DEFAULT_MIN_SEVERITY = 1;

export const DEFAULT_FAILURE_RULES: readonly FailureRule[] = [
  {
    id: 'typescript-compile',
    category: 'typecheck',
    severity: 8,
    confidence: 0.96,
    reason: 'TypeScript compile or type checking error is deterministic until code or generated types change.',
    rerun: 'do_not_rerun',
    labels: ['typescript', 'compile', 'deterministic'],
    patterns: [
      /\bTS\d{4}\b/,
      /\bTypeScript\b.*\berror\b/i,
      /\btsc\b.*\bfailed\b/i,
      /\btype\s+error\b/i,
    ],
  },
  {
    id: 'lint-format',
    category: 'lint',
    severity: 6,
    confidence: 0.9,
    reason: 'Lint or format failure is normally reproducible and should be fixed before rerunning a full matrix.',
    rerun: 'do_not_rerun',
    labels: ['lint', 'format', 'deterministic'],
    patterns: [
      /\b(?:eslint|prettier|biome|ruff|flake8|golangci-lint|ktlint|swiftlint)\b.*\b(?:error|failed|violation|would reformat)\b/i,
      /\blint\b.*\bfailed\b/i,
    ],
  },
  {
    id: 'test-assertion',
    category: 'test',
    severity: 7,
    confidence: 0.84,
    reason: 'Assertion failures are usually product or fixture failures, not a useful blind rerun candidate.',
    rerun: 'rerun_after_fix',
    labels: ['test', 'assertion'],
    patterns: [
      /\bAssertionError\b/,
      /\bexpect(?:ed)?\b.*\b(?:to|but|got|received)\b/i,
      /\bFAIL(?:ED)?\b.*\b(?:test|spec|suite)\b/i,
      /\bpanic:\s+test\b/i,
      /\bshould\s+(?:equal|be|contain|match)\b/i,
    ],
  },
  {
    id: 'snapshot-drift',
    category: 'test',
    severity: 6,
    confidence: 0.88,
    reason: 'Snapshot drift needs an intentional approval or fixture update, not a broad rerun.',
    rerun: 'rerun_after_fix',
    labels: ['snapshot', 'test'],
    patterns: [
      /\bsnapshot\b.*\b(?:failed|obsolete|mismatch|update)\b/i,
      /\breceived\b.*\bsnapshot\b/i,
    ],
  },
  {
    id: 'dependency-resolution',
    category: 'dependency',
    severity: 8,
    confidence: 0.91,
    reason: 'Package resolution and lockfile failures are deterministic until the manifest, registry, or lockfile changes.',
    rerun: 'do_not_rerun',
    labels: ['dependencies', 'lockfile', 'supply-chain'],
    patterns: [
      /\b(?:npm|pnpm|yarn)\b.*\b(?:ERR|ERESOLVE|ELOCKVERIFY|YN\d{4})\b/i,
      /\bCannot find module\b/i,
      /\bModule not found\b/i,
      /\bNo matching distribution found\b/i,
      /\bResolutionImpossible\b/i,
      /\b(?:package-lock|pnpm-lock|yarn\.lock|Cargo\.lock|go\.sum)\b.*\b(?:changed|mismatch|out of date|failed)\b/i,
    ],
  },
  {
    id: 'network-provider',
    category: 'network',
    severity: 5,
    confidence: 0.73,
    reason: 'Transient network or provider errors can be retried once after checking rate limits and provider status.',
    rerun: 'rerun_once',
    labels: ['network', 'transient'],
    patterns: [
      /\b(?:ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|ECONNREFUSED|socket hang up)\b/i,
      /\bTLS handshake timeout\b/i,
      /\bHTTP\s+(?:429|5\d\d)\b/i,
      /\b(?:rate limit|too many requests|upstream timeout|gateway timeout)\b/i,
    ],
  },
  {
    id: 'auth-permission',
    category: 'auth',
    severity: 9,
    confidence: 0.88,
    reason: 'Authorization failures usually need permissions, token scope, or environment configuration changes.',
    rerun: 'do_not_rerun',
    labels: ['auth', 'permissions', 'secrets'],
    patterns: [
      /\b(?:401|403)\b.*\b(?:Unauthorized|Forbidden|forbidden|denied)\b/i,
      /\b(?:permission denied|AccessDenied|not authorized|invalid token|bad credentials)\b/i,
      /\bcredentials?\b.*\b(?:missing|expired|invalid|required)\b/i,
    ],
  },
  {
    id: 'resource-pressure',
    category: 'resource',
    severity: 7,
    confidence: 0.82,
    reason: 'Memory, process, and disk pressure may pass with a larger runner but should not be retried endlessly.',
    rerun: 'rerun_with_more_resources',
    labels: ['resource', 'runner', 'capacity'],
    patterns: [
      /\bJavaScript heap out of memory\b/i,
      /\b(?:ENOMEM|ENOSPC|OOMKilled)\b/i,
      /\bNo space left on device\b/i,
      /\b(?:out of memory|disk quota exceeded|killed process)\b/i,
    ],
  },
  {
    id: 'container-build',
    category: 'container',
    severity: 7,
    confidence: 0.8,
    reason: 'Docker and BuildKit failures need cache, image, platform, or registry triage before a rerun.',
    rerun: 'investigate_environment',
    labels: ['docker', 'buildkit', 'container'],
    patterns: [
      /\b(?:docker build|buildkit|buildx|containerd|OCI runtime)\b.*\b(?:failed|error|denied)\b/i,
      /\bfailed to solve\b/i,
      /\b(?:image pull|manifest unknown|no matching manifest)\b/i,
    ],
  },
  {
    id: 'database-schema',
    category: 'schema',
    severity: 8,
    confidence: 0.84,
    reason: 'Schema and migration failures need data or migration ordering fixes before compute is spent again.',
    rerun: 'rerun_after_fix',
    labels: ['database', 'migration', 'schema'],
    patterns: [
      /\b(?:prisma|drizzle|flyway|liquibase|migration)\b.*\b(?:failed|drift|missing|conflict)\b/i,
      /\bSQLSTATE\b/i,
      /\brelation\b.*\bdoes not exist\b/i,
      /\bschema\b.*\b(?:drift|mismatch|validation failed)\b/i,
    ],
  },
  {
    id: 'secret-detected',
    category: 'secret',
    severity: 10,
    confidence: 0.95,
    reason: 'Secret scanning findings need rotation or repository cleanup. Rerunning will not make the leak safe.',
    rerun: 'do_not_rerun',
    labels: ['secret-scanning', 'security'],
    patterns: [
      /\bsecret scanning\b.*\b(?:detected|failed|blocked)\b/i,
      /\b(?:private key|api key|access token)\b.*\b(?:detected|leaked|exposed)\b/i,
      /\b(?:gitleaks|trufflehog|detect-secrets)\b.*\b(?:failed|found)\b/i,
    ],
  },
  {
    id: 'timeout-flake',
    category: 'timeout',
    severity: 5,
    confidence: 0.68,
    reason: 'Timeouts and port collisions are rerunnable once, but repeated signatures should become an owner task.',
    rerun: 'rerun_once',
    labels: ['timeout', 'flake'],
    patterns: [
      /\b(?:Timed out|timeout of \d+ms exceeded|deadline exceeded)\b/i,
      /\b(?:EADDRINUSE|address already in use|port already in use)\b/i,
      /\bflak(?:e|y|iness)\b/i,
    ],
  },
  {
    id: 'edge-runtime-api',
    category: 'edge-runtime',
    severity: 7,
    confidence: 0.81,
    reason: 'Edge runtime failures often come from unavailable Node APIs or platform-specific request limits.',
    rerun: 'rerun_after_fix',
    labels: ['edge-runtime', 'web-platform'],
    patterns: [
      /\b(?:Edge Runtime|Workers runtime|Cloudflare Workers|Vercel Edge|Deno Deploy)\b.*\b(?:unsupported|not defined|failed|exceeded)\b/i,
      /\b(?:process|Buffer|fs|net|tls)\b.*\bnot defined\b/i,
    ],
  },
  {
    id: 'ai-contract',
    category: 'contract',
    severity: 7,
    confidence: 0.83,
    reason: 'Structured output, tool call, and schema contract failures need prompt, schema, or adapter fixes.',
    rerun: 'rerun_after_fix',
    labels: ['ai', 'tool-calls', 'structured-output'],
    patterns: [
      /\b(?:JSON schema validation failed|schema validation failed|ZodError)\b/i,
      /\b(?:structured output|tool call|tool arguments?)\b.*\b(?:invalid|malformed|missing|required)\b/i,
      /\b(?:function call|tool_use)\b.*\b(?:parse|validation)\b.*\bfailed\b/i,
    ],
  },
  {
    id: 'supply-chain-policy',
    category: 'supply-chain',
    severity: 9,
    confidence: 0.86,
    reason: 'Provenance, attestation, SBOM, and CVE policy failures need artifact or policy remediation.',
    rerun: 'do_not_rerun',
    labels: ['sbom', 'provenance', 'security'],
    patterns: [
      /\b(?:SLSA|sigstore|cosign|attestation|provenance)\b.*\b(?:failed|missing|invalid|denied)\b/i,
      /\bSBOM\b.*\b(?:failed|missing|outdated|policy)\b/i,
      /\bCVE-\d{4}-\d+\b/i,
      /\bvulnerabilit(?:y|ies)\b.*\b(?:critical|high|policy|blocked)\b/i,
    ],
  },
];

const SECRET_REPLACERS: readonly [RegExp, string][] = [
  [/\b(Bearer\s+)[A-Za-z0-9._~+\/-]+=*/gi, '$1<redacted>'],
  [/\b(gh[pousr]_|github_pat_)[A-Za-z0-9_]{12,}\b/g, '$1<redacted>'],
  [/\b(sk-(?:proj-)?)[A-Za-z0-9_-]{16,}\b/g, '$1<redacted>'],
  [/\b(AKIA|ASIA)[A-Z0-9]{12,}\b/g, '<aws-key-redacted>'],
  [/([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASS|API_KEY|PRIVATE_KEY|CREDENTIAL)[A-Z0-9_]*=)(['"]?)[^'"\s]+/gi, '$1$2<redacted>'],
  [/(-----BEGIN [A-Z ]+PRIVATE KEY-----)[\s\S]*?(-----END [A-Z ]+PRIVATE KEY-----)/g, '$1<redacted>$2'],
];

const ANSI_PATTERN = /[\u001b\u009b][[\]()#;?]*(?:(?:(?:[a-zA-Z\d]*(?:;[a-zA-Z\d]*)*)?\u0007)|(?:(?:\d{1,4}(?:;\d{0,4})*)?[\dA-PR-TZcf-nq-uy=><~]))/g;
const GITHUB_ANNOTATION = /^::(?:error|warning|notice)(?:\s+[^:]*)?::/i;
const LOG_PREFIX = /^(?:\d{4}-\d{2}-\d{2}T[^\s]+\s+|\[[^\]]{1,40}\]\s+|##\[(?:error|warning|debug|section|endgroup|group)\]\s*)+/i;
const TIMESTAMP_PATTERN = /\b\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b/g;
const UUID_PATTERN = /\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b/gi;
const SHA_PATTERN = /\b[0-9a-f]{32,64}\b/gi;
const DURATION_PATTERN = /\b\d+(?:\.\d+)?\s*(?:ms|s|sec|seconds|m|min|minutes|h|hours)\b/gi;
const URL_QUERY_PATTERN = /(https?:\/\/[^\s?]+)\?[^\s]+/gi;
const LINE_COLUMN_PATTERN = /([:(])\d+(?::\d+)?\)?/g;
const PORT_PATTERN = /\bport\s+\d{2,5}\b/gi;
const MEMORY_ADDRESS_PATTERN = /\b0x[0-9a-f]+\b/gi;
const MULTISPACE_PATTERN = /\s+/g;
const PATH_HINT_PATTERN = /(?:^|\s|['"(])([A-Za-z0-9_.\/-]+\.(?:c|cc|cpp|cs|dart|ex|exs|go|h|hpp|hs|java|jl|js|jsx|json|kt|lua|ml|nix|php|py|r|rb|rs|scala|sh|sql|swift|toml|ts|tsx|yaml|yml|zig|lock|R))(?:[:(]\d+(?::\d+)?)?/g;
const CODE_PATTERN = /\b(?:TS\d{4}|CVE-\d{4}-\d+|SQLSTATE\[[A-Z0-9]+\]|[A-Z]{2,10}\d{3,6}|YN\d{4})\b/g;

export class WorkflowFailureFingerprinter {
  private readonly options: ResolvedOptions;

  constructor(options: FingerprintOptions = {}) {
    this.options = {
      maxContextLines: clampInteger(options.maxContextLines, 0, 20, DEFAULT_CONTEXT_LINES),
      maxEvidenceLines: clampInteger(options.maxEvidenceLines, 1, 30, DEFAULT_EVIDENCE_LINES),
      maxClusters: clampInteger(options.maxClusters, 1, 200, DEFAULT_MAX_CLUSTERS),
      minSeverity: clampInteger(options.minSeverity, 1, 10, DEFAULT_MIN_SEVERITY),
      includeLowSignal: options.includeLowSignal ?? false,
      now: options.now ?? (() => new Date()),
      rules: options.rules?.length ? options.rules : DEFAULT_FAILURE_RULES,
    };
  }

  analyze(inputs: readonly SourceInput[]): WorkflowFailureReport {
    const allIssues: LogIssue[] = [];
    let scannedBytes = 0;
    let scannedLines = 0;
    let suppressedLowSignal = 0;

    for (const input of inputs) {
      scannedBytes += Buffer.byteLength(input.text, 'utf8');
      const lines = splitLines(input.text);
      scannedLines += lines.length;
      const normalizedLines = lines.map((line) => this.normalizeLine(line));

      for (let index = 0; index < lines.length; index += 1) {
        const match = this.matchLine(normalizedLines[index] ?? '', lines[index] ?? '');
        if (!match) {
          continue;
        }
        if (!this.options.includeLowSignal && isLowSignal(match.normalizedLine)) {
          suppressedLowSignal += 1;
          continue;
        }
        const issue = this.makeIssue(input, lines, normalizedLines, index, match);
        if (issue.severity >= this.options.minSeverity) {
          allIssues.push(issue);
        }
      }
    }

    const clusters = this.clusterIssues(allIssues).slice(0, this.options.maxClusters);
    return {
      generatedAt: this.options.now().toISOString(),
      sourceCount: inputs.length,
      scannedBytes,
      scannedLines,
      issueCount: allIssues.length,
      clusterCount: clusters.length,
      suppressedLowSignal,
      clusters,
      recommendations: buildRecommendations(clusters, allIssues.length),
    };
  }

  normalizeLine(raw: string): string {
    let line = stripAnsi(raw);
    line = redactSecrets(line);
    line = line.replace(GITHUB_ANNOTATION, '');
    line = line.replace(LOG_PREFIX, '');
    line = line.replace(URL_QUERY_PATTERN, '$1?<query>');
    line = line.replace(TIMESTAMP_PATTERN, '<timestamp>');
    line = line.replace(UUID_PATTERN, '<uuid>');
    line = line.replace(SHA_PATTERN, '<sha>');
    line = line.replace(DURATION_PATTERN, '<duration>');
    line = line.replace(PORT_PATTERN, 'port <port>');
    line = line.replace(MEMORY_ADDRESS_PATTERN, '<addr>');
    return line.replace(MULTISPACE_PATTERN, ' ').trim();
  }

  toMarkdown(report: WorkflowFailureReport): string {
    const rows = report.clusters.map((cluster) => [
      shortFingerprint(cluster.fingerprint),
      String(cluster.severity),
      cluster.category,
      String(cluster.count),
      cluster.rerun,
      cluster.evidence[0] ?? '',
    ]);

    const table = rows.length
      ? rows.map((row) => `| ${row.map(escapeMarkdownCell).join(' | ')} |`).join('\n')
      : '| none | - | - | - | - | No failure signatures found. |';

    const ownerHints = unique(report.clusters.flatMap((cluster) => cluster.ownerHints));
    return [
      '# Workflow failure fingerprint report',
      '',
      `Generated: ${report.generatedAt}`,
      `Sources: ${report.sourceCount}`,
      `Scanned lines: ${report.scannedLines}`,
      `Issues: ${report.issueCount}`,
      `Clusters: ${report.clusterCount}`,
      '',
      '| Fingerprint | Severity | Category | Count | Rerun | Evidence |',
      '| --- | ---: | --- | ---: | --- | --- |',
      table,
      '',
      '## Recommendations',
      ...report.recommendations.map((line) => `- ${line}`),
      '',
      '## Owner hints',
      ...(ownerHints.length ? ownerHints.map((hint) => `- ${hint}`) : ['- No owner hints detected from paths or signatures.']),
    ].join('\n');
  }

  private matchLine(normalizedLine: string, rawLine: string): RuleMatch | undefined {
    const rawRedacted = redactSecrets(stripAnsi(rawLine));
    const haystack = `${normalizedLine}\n${rawRedacted}`;
    const matches = this.options.rules.filter((rule) => rule.patterns.some((pattern) => pattern.test(haystack)));
    if (!matches.length) {
      return undefined;
    }
    matches.sort((left, right) => (right.severity * right.confidence) - (left.severity * left.confidence));
    return { rule: matches[0] as FailureRule, normalizedLine, rawRedacted };
  }

  private makeIssue(
    input: SourceInput,
    rawLines: readonly string[],
    normalizedLines: readonly string[],
    index: number,
    match: RuleMatch,
  ): LogIssue {
    const start = Math.max(0, index - this.options.maxContextLines);
    const end = Math.min(rawLines.length, index + this.options.maxContextLines + 1);
    const context = normalizedLines.slice(start, end).filter(Boolean);
    const pathHints = extractPathHints(context.join('\n'));
    const signature = signatureFor(match.rule, match.normalizedLine, context, pathHints);

    return {
      id: `${input.name}:${index + 1}:${shortFingerprint(signature)}`,
      source: input.name,
      line: index + 1,
      category: match.rule.category,
      severity: match.rule.severity,
      confidence: match.rule.confidence,
      ruleId: match.rule.id,
      reason: match.rule.reason,
      rerun: match.rule.rerun,
      labels: match.rule.labels,
      normalized: match.normalizedLine,
      rawRedacted: match.rawRedacted,
      context,
      signature,
      pathHints,
    };
  }

  private clusterIssues(issues: readonly LogIssue[]): FailureCluster[] {
    const grouped = new Map<string, LogIssue[]>();
    for (const issue of issues) {
      const bucket = grouped.get(issue.signature);
      if (bucket) {
        bucket.push(issue);
      } else {
        grouped.set(issue.signature, [issue]);
      }
    }

    const clusters = Array.from(grouped.entries()).map(([fingerprint, items]) => {
      const first = items[0] as LogIssue;
      const evidence = unique(items.flatMap((issue) => issue.context.filter(isSignalLine))).slice(0, this.options.maxEvidenceLines);
      const pathHints = unique(items.flatMap((issue) => issue.pathHints)).slice(0, 12);
      const labels = unique(items.flatMap((issue) => issue.labels));
      const ruleIds = unique(items.map((issue) => issue.ruleId));
      const ownerHints = inferOwnerHints(pathHints, labels, first.category);
      return {
        fingerprint,
        category: first.category,
        severity: maxOf(items.map((issue) => issue.severity)),
        confidence: round(mean(items.map((issue) => issue.confidence)), 3),
        count: items.length,
        sources: unique(items.map((issue) => issue.source)),
        firstLine: Math.min(...items.map((issue) => issue.line)),
        ruleIds,
        labels,
        rerun: combineRerunAdvice(items.map((issue) => issue.rerun)),
        ownerHints,
        evidence,
        pathHints,
      } satisfies FailureCluster;
    });

    clusters.sort((left, right) => {
      const severityDelta = right.severity - left.severity;
      if (severityDelta !== 0) {
        return severityDelta;
      }
      const countDelta = right.count - left.count;
      if (countDelta !== 0) {
        return countDelta;
      }
      return right.confidence - left.confidence;
    });
    return clusters;
  }
}

export function analyzeWorkflowFailures(inputs: readonly SourceInput[], options: FingerprintOptions = {}): WorkflowFailureReport {
  return new WorkflowFailureFingerprinter(options).analyze(inputs);
}

export function reportToMarkdown(report: WorkflowFailureReport): string {
  return new WorkflowFailureFingerprinter().toMarkdown(report);
}

function signatureFor(
  rule: FailureRule,
  normalizedLine: string,
  context: readonly string[],
  pathHints: readonly string[],
): string {
  const contextText = context.join('\n');
  const codes = extractCodes(`${normalizedLine}\n${contextText}`);
  const signalLines = context.filter(isSignalLine).map(normalizeForSignature).slice(0, 10);
  const stablePaths = pathHints.map(stablePath).slice(0, 6);
  const material = [
    rule.category,
    rule.id,
    codes.join(','),
    stablePaths.join(','),
    signalLines.join('\n'),
  ].filter(Boolean).join('\n');
  return sha256(material || normalizeForSignature(normalizedLine), 24);
}

function normalizeForSignature(value: string): string {
  return value
    .replace(LINE_COLUMN_PATTERN, '$1<line>')
    .replace(/\b\d+(?:\.\d+)?\b/g, '<num>')
    .replace(/\b(?:runner|worker|job|attempt|pid)[-_ ]?<num>\b/gi, '<runtime-id>')
    .toLowerCase()
    .trim();
}

function stripAnsi(value: string): string {
  return value.replace(ANSI_PATTERN, '');
}

function redactSecrets(value: string): string {
  let result = value;
  for (const [pattern, replacement] of SECRET_REPLACERS) {
    result = result.replace(pattern, replacement);
  }
  return result;
}

function splitLines(value: string): string[] {
  if (!value) {
    return [];
  }
  return value.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
}

function extractCodes(value: string): string[] {
  return unique(Array.from(value.matchAll(CODE_PATTERN), (match) => match[0].toUpperCase())).slice(0, 8);
}

function extractPathHints(value: string): string[] {
  const hints = Array.from(value.matchAll(PATH_HINT_PATTERN), (match) => match[1] ?? '')
    .filter(Boolean)
    .map((hint) => hint.replace(/\\/g, '/'))
    .map((hint) => hint.replace(/^['"(]+|[)'",]+$/g, ''))
    .map(stablePath)
    .filter(Boolean);
  return unique(hints).slice(0, 20);
}

function stablePath(path: string): string {
  const normalized = path.replace(/\\/g, '/').replace(/^\.\//, '');
  const parts = normalized.split('/').filter(Boolean);
  if (parts.length <= 4) {
    return parts.join('/');
  }
  return parts.slice(-4).join('/');
}

function isSignalLine(line: string): boolean {
  if (!line || line.length < 4) {
    return false;
  }
  if (/^(?:at |from |during |caused by:|error:|warning:|failed|fail|panic|expected|received|actual|sqlstate|ts\d{4})/i.test(line)) {
    return true;
  }
  if (CODE_PATTERN.test(line)) {
    CODE_PATTERN.lastIndex = 0;
    return true;
  }
  CODE_PATTERN.lastIndex = 0;
  return /\b(?:error|failed|failure|exception|denied|timeout|oom|missing|invalid|mismatch|cannot|unauthorized|forbidden)\b/i.test(line);
}

function isLowSignal(line: string): boolean {
  return /^(?:error command failed with exit code \d+|process completed with exit code \d+|make: \*+ .* error \d+\.?|failed with errors?\.?|there was a problem with the editor)\s*$/i.test(line);
}

function combineRerunAdvice(actions: readonly RerunAction[]): RerunAction {
  const rank: Record<RerunAction, number> = {
    do_not_rerun: 5,
    rerun_after_fix: 4,
    investigate_environment: 3,
    rerun_with_more_resources: 2,
    rerun_once: 1,
  };
  return actions.reduce<RerunAction>((best, action) => (rank[action] > rank[best] ? action : best), 'rerun_once');
}

function inferOwnerHints(pathHints: readonly string[], labels: readonly string[], category: FailureCategory): string[] {
  const hints = new Set<string>();
  const joined = `${pathHints.join('\n')}\n${labels.join('\n')}`.toLowerCase();

  const checks: readonly [RegExp, string][] = [
    [/\b(?:package\.json|pnpm-lock|yarn\.lock|package-lock|cargo\.toml|cargo\.lock|go\.mod|go\.sum)\b/, 'dependency owner: package manifest or lockfile maintainer'],
    [/\b(?:dockerfile|container|buildkit|buildx|oci|image)\b/, 'platform owner: container build and registry path'],
    [/\b(?:schema|migration|prisma|sql|drizzle|flyway|liquibase)\b/, 'data owner: schema and migration path'],
    [/\b(?:worker|edge|middleware|runtime|cloudflare|vercel)\b/, 'edge owner: runtime compatibility path'],
    [/\b(?:test|spec|fixture|snapshot|jest|vitest|playwright)\b/, 'test owner: assertion, fixture, or browser test path'],
    [/\b(?:secret|credential|token|auth|permission)\b/, 'security owner: credentials, scopes, or policy path'],
    [/\b(?:sbom|slsa|attestation|cosign|provenance|cve)\b/, 'security owner: supply chain policy path'],
  ];

  for (const [pattern, hint] of checks) {
    if (pattern.test(joined)) {
      hints.add(hint);
    }
  }

  if (!hints.size) {
    hints.add(`default owner: ${category} failure triage`);
  }
  return Array.from(hints);
}

function buildRecommendations(clusters: readonly FailureCluster[], issueCount: number): string[] {
  if (!clusters.length) {
    return issueCount
      ? ['Only low signal lines matched. Increase context or include low signal mode if the failing step is heavily summarized.']
      : ['No failure signatures found. Capture raw step logs instead of only a job summary.'];
  }

  const top = clusters[0] as FailureCluster;
  const deterministic = clusters.filter((cluster) => cluster.rerun === 'do_not_rerun' || cluster.rerun === 'rerun_after_fix');
  const transient = clusters.filter((cluster) => cluster.rerun === 'rerun_once' || cluster.rerun === 'rerun_with_more_resources');
  const lines = [
    `Start with ${shortFingerprint(top.fingerprint)}: ${top.category} severity ${top.severity}, seen ${top.count} time(s).`,
  ];

  if (deterministic.length) {
    lines.push(`${deterministic.length} cluster(s) look deterministic. Fix those before spending another full CI matrix rerun.`);
  }
  if (transient.length) {
    lines.push(`${transient.length} cluster(s) may be rerunnable. Limit retries to one and attach the same fingerprint to the rerun note.`);
  }
  const ownerHints = unique(clusters.flatMap((cluster) => cluster.ownerHints));
  if (ownerHints.length) {
    lines.push(`Route by owner hint: ${ownerHints.slice(0, 3).join('; ')}.`);
  }
  return lines;
}

function parseCli(argv: readonly string[]): CliConfig {
  let output: 'json' | 'markdown' = 'json';
  let minSeverity: number | undefined;
  let maxContextLines: number | undefined;
  let maxClusters: number | undefined;
  let stdinName = 'stdin';
  const files: string[] = [];
  const namedSources: CliSource[] = [];

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      return { help: true, output, minSeverity, maxContextLines, maxClusters, stdinName, files, namedSources };
    }
    if (arg === '--json') {
      output = 'json';
      continue;
    }
    if (arg === '--markdown' || arg === '--md') {
      output = 'markdown';
      continue;
    }
    const minMatch = arg.match(/^--min-severity=(\d+)$/);
    if (minMatch) {
      minSeverity = Number.parseInt(minMatch[1] as string, 10);
      continue;
    }
    const contextMatch = arg.match(/^--max-context=(\d+)$/);
    if (contextMatch) {
      maxContextLines = Number.parseInt(contextMatch[1] as string, 10);
      continue;
    }
    const clusterMatch = arg.match(/^--max-clusters=(\d+)$/);
    if (clusterMatch) {
      maxClusters = Number.parseInt(clusterMatch[1] as string, 10);
      continue;
    }
    const stdinMatch = arg.match(/^--stdin-name=(.+)$/);
    if (stdinMatch) {
      stdinName = stdinMatch[1] as string;
      continue;
    }
    const sourceMatch = arg.match(/^--source=([^:]+):(.+)$/);
    if (sourceMatch) {
      namedSources.push({ name: sourceMatch[1] as string, path: sourceMatch[2] as string });
      continue;
    }
    files.push(arg);
  }

  return { help: false, output, minSeverity, maxContextLines, maxClusters, stdinName, files, namedSources };
}

function readCliInputs(config: CliConfig): SourceInput[] {
  const inputs: SourceInput[] = [];
  for (const source of config.namedSources) {
    inputs.push({ name: source.name, text: readFileSync(source.path, 'utf8') });
  }
  for (const file of config.files) {
    inputs.push({ name: basename(file), text: readFileSync(file, 'utf8') });
  }
  if (!inputs.length) {
    inputs.push({ name: config.stdinName, text: readFileSync(0, 'utf8') });
  }
  return inputs;
}

function runCli(): void {
  const config = parseCli(process.argv.slice(2));
  if (config.help) {
    process.stdout.write(helpText());
    return;
  }

  const fingerprinter = new WorkflowFailureFingerprinter({
    minSeverity: config.minSeverity,
    maxContextLines: config.maxContextLines,
    maxClusters: config.maxClusters,
  });
  const report = fingerprinter.analyze(readCliInputs(config));
  const output = config.output === 'markdown'
    ? fingerprinter.toMarkdown(report)
    : `${JSON.stringify(report, null, 2)}\n`;
  process.stdout.write(output.endsWith('\n') ? output : `${output}\n`);
}

function helpText(): string {
  return [
    'WorkflowFailureFingerprint.ts - cluster CI logs into stable failure fingerprints.',
    '',
    'Usage:',
    '  tsx WorkflowFailureFingerprint.ts --markdown github-actions.log',
    '  cat build.log | tsx WorkflowFailureFingerprint.ts --json --stdin-name=linux-node22',
    '  tsx WorkflowFailureFingerprint.ts --source=unit:unit.log --source=e2e:e2e.log',
    '',
    'Options:',
    '  --json                  Emit structured JSON. This is the default.',
    '  --markdown, --md         Emit a compact markdown report.',
    '  --min-severity=N         Keep findings from 1 to 10. Default: 1.',
    '  --max-context=N          Keep N surrounding lines per finding. Default: 3.',
    '  --max-clusters=N         Limit returned clusters. Default: 25.',
    '  --stdin-name=NAME        Name stdin in the report.',
    '  --source=NAME:PATH       Read a file with a stable source name.',
    '',
  ].join('\n');
}

function clampInteger(value: number | undefined, minimum: number, maximum: number, fallback: number): number {
  if (!Number.isFinite(value)) {
    return fallback;
  }
  const integer = Math.trunc(value as number);
  return Math.min(maximum, Math.max(minimum, integer));
}

function sha256(value: string, length: number): string {
  return createHash('sha256').update(value).digest('hex').slice(0, length);
}

function shortFingerprint(value: string): string {
  return value.slice(0, 12);
}

function unique<T>(items: readonly T[]): T[] {
  const seen = new Set<T>();
  const result: T[] = [];
  for (const item of items) {
    if (!seen.has(item)) {
      seen.add(item);
      result.push(item);
    }
  }
  return result;
}

function maxOf(values: readonly number[]): number {
  return values.reduce((max, value) => Math.max(max, value), Number.NEGATIVE_INFINITY);
}

function mean(values: readonly number[]): number {
  if (!values.length) {
    return 0;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function round(value: number, places: number): number {
  const scale = 10 ** places;
  return Math.round(value * scale) / scale;
}

function escapeMarkdownCell(value: string): string {
  return value.replace(/\|/g, '\\|').replace(/`/g, '\\`').slice(0, 180);
}

function isCliInvocation(): boolean {
  const entry = process.argv[1] ? basename(process.argv[1]) : '';
  return entry === 'WorkflowFailureFingerprint.ts' || entry === 'WorkflowFailureFingerprint.js';
}

if (isCliInvocation()) {
  try {
    runCli();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
  }
}

/*
This solves the April 2026 CI problem where AI coding agents, GitHub Actions, Vercel builds, Docker BuildKit jobs, Playwright tests, TypeScript checks, and supply chain policy gates all fail in the same noisy log stream, and teams waste hours rerunning jobs without knowing whether the failure is deterministic or transient. Built because I kept seeing agent-written pull requests produce ten thousand lines of logs where the one useful line was buried under timestamps, runner IDs, secrets, paths, ANSI color, retry noise, and package manager chatter. Use it when you need GitHub Actions failure fingerprinting, CI log clustering, DevOps incident triage, AI agent workflow debugging, flaky test detection, TypeScript build failure routing, container build failure grouping, SBOM policy triage, or structured output contract failure analysis from a plain text log. The trick: normalize only the unstable parts of the log, preserve the error code and useful path hints, redact credentials before hashing, then attach a rerun decision so a human can stop burning minutes on failures that need a code or config fix. Drop this into a repository, pipe raw CI logs through it, save the JSON next to the run, and compare fingerprints across branches so reviewers can say exactly which failure changed and which one is just the same root cause showing up again.
*/
