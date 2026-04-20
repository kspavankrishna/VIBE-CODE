import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";

/**
 * MCP tool-call firewall for agent hosts that need deterministic policy checks,
 * secret-aware redaction, duplicate detection, and result gating.
 */

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];
export interface JsonObject {
  [key: string]: JsonValue;
}

export type DecisionEffect = "allow" | "review" | "deny";
export type PolicyChannel = "args" | "result";
export type RateLimitScope =
  | "global"
  | "principal"
  | "tenant"
  | "provider"
  | "server"
  | "tool"
  | "principal+tool"
  | "tenant+tool"
  | "server+tool"
  | "principal+server+tool";

export interface InvocationContext {
  requestId?: string;
  sessionId?: string;
  principal?: string;
  tenant?: string;
  environment?: string;
  provider?: string;
  server?: string;
  tool?: string;
  operation?: string;
  sourceIp?: string;
  tags?: string[];
  metadata?: Record<string, JsonValue>;
  args: JsonValue;
  timestamp?: number;
}

export interface ResultContext {
  invocation: InvocationContext;
  result: JsonValue;
  timestamp?: number;
}

export type PatternInput =
  | string
  | { exact: string; ignoreCase?: boolean }
  | { glob: string; ignoreCase?: boolean }
  | { regex: string; flags?: string };

export interface FieldPredicate {
  path: string;
  mode?: "any" | "all";
  exists?: boolean;
  equals?: JsonPrimitive;
  notEquals?: JsonPrimitive;
  oneOf?: JsonPrimitive[];
  regex?: string;
  flags?: string;
  numericGte?: number;
  numericLte?: number;
  includesAny?: string[];
  stringLengthLte?: number;
  byteLengthLte?: number;
}

export interface PayloadConstraint {
  requirePaths?: string[];
  forbidPaths?: string[];
  maxPayloadBytes?: number;
  maxDepth?: number;
  maxArrayLength?: number;
  maxObjectKeys?: number;
  maxStringLength?: number;
  duplicateWindowMs?: number;
  duplicateEffect?: Exclude<DecisionEffect, "allow">;
}

export interface FieldActionWhen {
  regex?: string;
  flags?: string;
  stringLengthGte?: number;
  byteLengthGte?: number;
}

export interface FieldActionRule {
  path: string;
  action: "redact" | "truncate" | "hash" | "deny";
  replace?: string;
  maxLength?: number;
  reason?: string;
  when?: FieldActionWhen;
}

export interface RateLimitPolicy {
  id?: string;
  windowMs: number;
  key?: RateLimitScope;
  maxCalls?: number;
  maxPayloadBytes?: number;
  maxRisk?: number;
  effect?: Exclude<DecisionEffect, "allow">;
}

export interface SecretPattern {
  name: string;
  regex: string;
  flags?: string;
  confidence?: number;
  enabled?: boolean;
}

export interface SecretHandling {
  effectOnArgs?: DecisionEffect;
  effectOnResults?: DecisionEffect;
  autoRedact?: boolean;
  redactionText?: string;
  minConfidence?: number;
}

export interface MatchCriteria {
  principals?: PatternInput[];
  tenants?: PatternInput[];
  environments?: PatternInput[];
  providers?: PatternInput[];
  servers?: PatternInput[];
  tools?: PatternInput[];
  operations?: PatternInput[];
  tagsAny?: PatternInput[];
  tagsAll?: PatternInput[];
  metadata?: Record<string, PatternInput[]>;
  argPredicates?: FieldPredicate[];
  resultPredicates?: FieldPredicate[];
}

export interface FirewallRule {
  id: string;
  description?: string;
  effect?: DecisionEffect;
  priority?: number;
  enabled?: boolean;
  expiresAt?: string | number;
  match?: MatchCriteria;
  constraints?: PayloadConstraint;
  redactArgs?: FieldActionRule[];
  redactResult?: FieldActionRule[];
  rateLimit?: RateLimitPolicy | RateLimitPolicy[];
  detectSecrets?: boolean;
  secretHandling?: SecretHandling;
  risk?: number;
}

export interface FirewallPolicy {
  name: string;
  version?: string;
  defaultEffect?: DecisionEffect;
  requireAllowRule?: boolean;
  secretPatterns?: SecretPattern[];
  secretHandling?: SecretHandling;
  rateLimits?: RateLimitPolicy[];
  rules: FirewallRule[];
}

export interface FirewallFinding {
  code:
    | "rule-deny"
    | "rule-review"
    | "constraint-violation"
    | "path-denied"
    | "rate-limit-exceeded"
    | "secret-detected"
    | "duplicate-request";
  severity: "info" | "warning" | "critical";
  message: string;
  path?: string;
  ruleId?: string;
  evidence?: string;
}

export interface FirewallDecisionStats {
  payloadBytes: number;
  maxDepth: number;
  matchedRules: number;
  scannedStrings: number;
  secretFindings: number;
  redactions: number;
  hashes: number;
  truncations: number;
  rateLimitHits: number;
}

export interface FirewallDecision<TChannel extends PolicyChannel = PolicyChannel> {
  channel: TChannel;
  effect: DecisionEffect;
  allowed: boolean;
  requiresReview: boolean;
  policyName: string;
  policyFingerprint: string;
  requestFingerprint: string;
  payloadFingerprint: string;
  matchedRuleIds: string[];
  reasons: string[];
  findings: FirewallFinding[];
  riskScore: number;
  sanitizedPayload: JsonValue;
  stats: FirewallDecisionStats;
  evaluatedAt: number;
}

export interface FirewallStateAdapter {
  recordFingerprint(key: string, ttlMs: number, now: number): { duplicate: boolean; expiresAt: number };
  consumeRateLimit(
    key: string,
    policy: RateLimitPolicy,
    sample: { payloadBytes: number; risk: number },
    now: number
  ): {
    exceeded: boolean;
    snapshot: { calls: number; payloadBytes: number; risk: number; windowStartedAt: number };
  };
  prune(now: number): void;
}

type CanonicalValue = null | boolean | number | string | CanonicalObject | CanonicalValue[];
interface CanonicalObject {
  [key: string]: CanonicalValue;
}

type SelectorToken =
  | { kind: "prop"; key: string }
  | { kind: "index"; index: number }
  | { kind: "wildcard" };

interface NodeRef {
  parent: JsonObject | JsonValue[] | null;
  key: string | number | null;
  value: JsonValue;
  path: string;
}

interface CompiledPattern {
  test(value: string): boolean;
  description: string;
}

interface CompiledFieldPredicate extends FieldPredicate {
  selector: SelectorToken[];
  compiledRegex?: RegExp;
}

interface CompiledFieldActionRule extends FieldActionRule {
  selector: SelectorToken[];
  compiledWhenRegex?: RegExp;
}

interface CompiledSecretPattern {
  name: string;
  regex: RegExp;
  confidence: number;
}

interface NormalizedMatchCriteria {
  principals: CompiledPattern[];
  tenants: CompiledPattern[];
  environments: CompiledPattern[];
  providers: CompiledPattern[];
  servers: CompiledPattern[];
  tools: CompiledPattern[];
  operations: CompiledPattern[];
  tagsAny: CompiledPattern[];
  tagsAll: CompiledPattern[];
  metadata: Record<string, CompiledPattern[]>;
  argPredicates: CompiledFieldPredicate[];
  resultPredicates: CompiledFieldPredicate[];
}

interface NormalizedRateLimit extends RateLimitPolicy {
  id: string;
  key: RateLimitScope;
  effect: Exclude<DecisionEffect, "allow">;
}

interface NormalizedRule {
  id: string;
  description?: string;
  effect?: DecisionEffect;
  priority: number;
  enabled: boolean;
  expiresAtMs?: number;
  match?: NormalizedMatchCriteria;
  constraints?: PayloadConstraint;
  redactArgs: CompiledFieldActionRule[];
  redactResult: CompiledFieldActionRule[];
  rateLimits: NormalizedRateLimit[];
  detectSecrets: boolean;
  secretHandling?: SecretHandling;
  risk: number;
}

interface NormalizedPolicy {
  name: string;
  version?: string;
  defaultEffect: DecisionEffect;
  requireAllowRule: boolean;
  secretPatterns: CompiledSecretPattern[];
  secretHandling: Required<SecretHandling>;
  rateLimits: NormalizedRateLimit[];
  rules: NormalizedRule[];
  fingerprint: string;
}

interface PayloadMetrics {
  payloadBytes: number;
  maxDepth: number;
  maxArrayLength: number;
  maxObjectKeys: number;
  maxStringLength: number;
  stringCount: number;
}

interface ConstraintOutcome {
  code: "constraint-violation" | "duplicate-request";
  message: string;
  effect: Exclude<DecisionEffect, "allow">;
  path?: string;
  evidence?: string;
}

interface AppliedActions {
  payload: JsonValue;
  findings: FirewallFinding[];
  denyReasons: string[];
  redactions: number;
  hashes: number;
  truncations: number;
}

interface SecretScanResult {
  findings: FirewallFinding[];
  paths: string[];
  scannedStrings: number;
}

interface RateBucket {
  windowStartedAt: number;
  lastTouchedAt: number;
  calls: number;
  payloadBytes: number;
  risk: number;
}

export const DEFAULT_SECRET_PATTERNS: ReadonlyArray<SecretPattern> = [
  {
    name: "openai-project-key",
    regex: "\\bsk-(?:proj|live|test)-[A-Za-z0-9_-]{16,}\\b",
    confidence: 0.98,
  },
  {
    name: "anthropic-key",
    regex: "\\bsk-ant-[A-Za-z0-9_-]{16,}\\b",
    confidence: 0.98,
  },
  {
    name: "github-token",
    regex: "\\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,}\\b",
    confidence: 0.97,
  },
  {
    name: "github-pat",
    regex: "\\bgithub_pat_[A-Za-z0-9_]{20,}\\b",
    confidence: 0.97,
  },
  {
    name: "aws-access-key",
    regex: "\\bAKIA[0-9A-Z]{16}\\b",
    confidence: 0.95,
  },
  {
    name: "google-api-key",
    regex: "\\bAIza[0-9A-Za-z\\-_]{35}\\b",
    confidence: 0.92,
  },
  {
    name: "slack-token",
    regex: "\\bxox[baprs]-[A-Za-z0-9-]{10,}\\b",
    confidence: 0.95,
  },
  {
    name: "jwt",
    regex: "\\beyJ[A-Za-z0-9_-]{10,}\\.[A-Za-z0-9._-]{10,}\\.[A-Za-z0-9._-]{10,}\\b",
    confidence: 0.72,
  },
  {
    name: "private-key-pem",
    regex: "-----BEGIN (?:RSA|OPENSSH|EC|DSA|PGP|PRIVATE) KEY-----",
    confidence: 0.99,
  },
  {
    name: "bearer-token",
    regex: "\\bBearer\\s+[A-Za-z0-9._~+\\/-]{16,}\\b",
    flags: "i",
    confidence: 0.85,
  },
];

const STALE_RATE_BUCKET_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_TRUNCATE_LENGTH = 256;

function sha256Hex(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

function stableHash(value: unknown): string {
  return sha256Hex(stableStringifyUnknown(value));
}

function stableStringifyUnknown(value: unknown): string {
  return stableStringifyCanonical(normalizeUnknown(value));
}

function stableStringifyCanonical(value: CanonicalValue): string {
  if (value === null) {
    return "null";
  }
  if (typeof value === "string") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "null";
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringifyCanonical(item)).join(",")}]`;
  }

  const keys = Object.keys(value).sort((left, right) => left.localeCompare(right));
  return `{${keys
    .map((key) => `${JSON.stringify(key)}:${stableStringifyCanonical(value[key])}`)
    .join(",")}}`;
}

function normalizeUnknown(value: unknown): CanonicalValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalizeUnknown(item));
  }
  if (ArrayBuffer.isView(value)) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("base64");
  }
  if (typeof value === "object") {
    const output: CanonicalObject = {};
    const entries = Object.entries(value as Record<string, unknown>).sort(([left], [right]) =>
      left.localeCompare(right)
    );
    for (const [key, entryValue] of entries) {
      if (
        entryValue === undefined ||
        typeof entryValue === "function" ||
        typeof entryValue === "symbol"
      ) {
        continue;
      }
      output[key] = normalizeUnknown(entryValue);
    }
    return output;
  }
  return String(value);
}

function deepCloneJson(value: JsonValue): JsonValue {
  if (value === null || typeof value !== "object") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item) => deepCloneJson(item));
  }
  const output: JsonObject = {};
  for (const key of Object.keys(value)) {
    output[key] = deepCloneJson(value[key]);
  }
  return output;
}

function byteLengthOfString(value: string): number {
  return Buffer.byteLength(value, "utf8");
}

function byteLengthOfJson(value: JsonValue): number {
  return byteLengthOfString(stableStringifyUnknown(value));
}

function ensureArray<T>(value?: T | ReadonlyArray<T>): T[] {
  if (value === undefined) {
    return [];
  }
  return Array.isArray(value) ? [...value] : [value];
}

function normalizeSecretHandling(input?: SecretHandling): Required<SecretHandling> {
  return {
    effectOnArgs: input?.effectOnArgs ?? "review",
    effectOnResults: input?.effectOnResults ?? "review",
    autoRedact: input?.autoRedact ?? true,
    redactionText: input?.redactionText ?? "[REDACTED_SECRET]",
    minConfidence: input?.minConfidence ?? 0.75,
  };
}

function isJsonObject(value: JsonValue): value is JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isPrimitiveValue(value: JsonValue): value is JsonPrimitive {
  return value === null || typeof value !== "object";
}

function primitiveEquals(left: JsonPrimitive, right: JsonPrimitive): boolean {
  return left === right;
}

function escapeRegExp(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function globToRegExp(glob: string, ignoreCase = false): RegExp {
  let pattern = "^";
  for (let index = 0; index < glob.length; index += 1) {
    const character = glob[index];
    if (character === "*") {
      const isDoubleStar = glob[index + 1] === "*";
      pattern += isDoubleStar ? ".*" : "[^/]*";
      if (isDoubleStar) {
        index += 1;
      }
      continue;
    }
    if (character === "?") {
      pattern += ".";
      continue;
    }
    pattern += escapeRegExp(character);
  }
  pattern += "$";
  return new RegExp(pattern, ignoreCase ? "i" : "");
}

function compilePattern(input: PatternInput): CompiledPattern {
  if (typeof input === "string") {
    if (input.includes("*") || input.includes("?")) {
      const regex = globToRegExp(input, false);
      return {
        description: `glob:${input}`,
        test: (value) => regex.test(value),
      };
    }
    return {
      description: `exact:${input}`,
      test: (value) => value === input,
    };
  }

  if ("exact" in input) {
    const expected = input.ignoreCase ? input.exact.toLowerCase() : input.exact;
    return {
      description: `exact:${input.exact}`,
      test: (value) => (input.ignoreCase ? value.toLowerCase() : value) === expected,
    };
  }

  if ("glob" in input) {
    const regex = globToRegExp(input.glob, input.ignoreCase);
    return {
      description: `glob:${input.glob}`,
      test: (value) => regex.test(value),
    };
  }

  const regex = new RegExp(input.regex, input.flags ?? "");
  return {
    description: `regex:${input.regex}`,
    test: (value) => {
      regex.lastIndex = 0;
      return regex.test(value);
    },
  };
}

function compilePatterns(inputs?: PatternInput[]): CompiledPattern[] {
  return ensureArray(inputs).map((input) => compilePattern(input));
}

function formatPropertyPath(parentPath: string, key: string): string {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(key)
    ? `${parentPath}.${key}`
    : `${parentPath}[${JSON.stringify(key)}]`;
}

function formatIndexPath(parentPath: string, index: number): string {
  return `${parentPath}[${index}]`;
}

function parseSelector(path: string): SelectorToken[] {
  const trimmed = path.trim();
  if (trimmed === "" || trimmed === "$") {
    return [];
  }

  let cursor = trimmed;
  if (cursor.startsWith("$.")) {
    cursor = cursor.slice(2);
  } else if (cursor.startsWith("$")) {
    cursor = cursor.slice(1);
  }

  const tokens: SelectorToken[] = [];
  let buffer = "";
  let index = 0;

  const flushBuffer = (): void => {
    if (buffer.length > 0) {
      tokens.push({ kind: "prop", key: buffer });
      buffer = "";
    }
  };

  while (index < cursor.length) {
    const character = cursor[index];

    if (character === ".") {
      flushBuffer();
      index += 1;
      continue;
    }

    if (character !== "[") {
      buffer += character;
      index += 1;
      continue;
    }

    flushBuffer();
    const end = cursor.indexOf("]", index);
    if (end === -1) {
      throw new Error(`Invalid selector "${path}": missing closing bracket.`);
    }

    const inner = cursor.slice(index + 1, end).trim();
    if (inner === "*") {
      tokens.push({ kind: "wildcard" });
    } else if (/^\d+$/.test(inner)) {
      tokens.push({ kind: "index", index: Number.parseInt(inner, 10) });
    } else if (
      (inner.startsWith('"') && inner.endsWith('"')) ||
      (inner.startsWith("'") && inner.endsWith("'"))
    ) {
      if (inner.startsWith('"')) {
        tokens.push({ kind: "prop", key: JSON.parse(inner) as string });
      } else {
        const raw = inner.slice(1, -1).replace(/\\'/g, "'").replace(/\\\\/g, "\\");
        tokens.push({ kind: "prop", key: raw });
      }
    } else {
      throw new Error(`Invalid selector segment "${inner}" in "${path}".`);
    }

    index = end + 1;
  }

  flushBuffer();
  return tokens;
}

function selectNodeRefs(root: JsonValue, selector: SelectorToken[]): NodeRef[] {
  let current: NodeRef[] = [{ parent: null, key: null, value: root, path: "$" }];

  for (const token of selector) {
    const next: NodeRef[] = [];

    for (const ref of current) {
      if (token.kind === "prop") {
        if (isJsonObject(ref.value) && Object.prototype.hasOwnProperty.call(ref.value, token.key)) {
          next.push({
            parent: ref.value,
            key: token.key,
            value: ref.value[token.key],
            path: formatPropertyPath(ref.path, token.key),
          });
        }
        continue;
      }

      if (token.kind === "index") {
        if (Array.isArray(ref.value) && token.index >= 0 && token.index < ref.value.length) {
          next.push({
            parent: ref.value,
            key: token.index,
            value: ref.value[token.index],
            path: formatIndexPath(ref.path, token.index),
          });
        }
        continue;
      }

      if (Array.isArray(ref.value)) {
        for (let wildcardIndex = 0; wildcardIndex < ref.value.length; wildcardIndex += 1) {
          next.push({
            parent: ref.value,
            key: wildcardIndex,
            value: ref.value[wildcardIndex],
            path: formatIndexPath(ref.path, wildcardIndex),
          });
        }
        continue;
      }

      if (isJsonObject(ref.value)) {
        for (const key of Object.keys(ref.value)) {
          next.push({
            parent: ref.value,
            key,
            value: ref.value[key],
            path: formatPropertyPath(ref.path, key),
          });
        }
      }
    }

    current = next;
    if (current.length === 0) {
      break;
    }
  }

  return current;
}

function writeNodeValue(root: JsonValue, ref: NodeRef, nextValue: JsonValue): JsonValue {
  if (ref.parent === null || ref.key === null) {
    return nextValue;
  }
  if (Array.isArray(ref.parent)) {
    ref.parent[ref.key as number] = nextValue;
    return root;
  }
  ref.parent[ref.key as string] = nextValue;
  return root;
}

function compileFieldPredicate(predicate: FieldPredicate): CompiledFieldPredicate {
  return {
    ...predicate,
    selector: parseSelector(predicate.path),
    compiledRegex: predicate.regex ? new RegExp(predicate.regex, predicate.flags ?? "") : undefined,
  };
}

function valueMatchesPredicate(value: JsonValue, predicate: CompiledFieldPredicate): boolean {
  if (predicate.equals !== undefined) {
    if (!isPrimitiveValue(value) || !primitiveEquals(value, predicate.equals)) {
      return false;
    }
  }

  if (predicate.notEquals !== undefined) {
    if (isPrimitiveValue(value) && primitiveEquals(value, predicate.notEquals)) {
      return false;
    }
  }

  if (predicate.oneOf && predicate.oneOf.length > 0) {
    if (!isPrimitiveValue(value) || !predicate.oneOf.some((item) => primitiveEquals(value, item))) {
      return false;
    }
  }

  if (predicate.compiledRegex) {
    if (typeof value !== "string") {
      return false;
    }
    predicate.compiledRegex.lastIndex = 0;
    if (!predicate.compiledRegex.test(value)) {
      return false;
    }
  }

  if (predicate.numericGte !== undefined) {
    if (typeof value !== "number" || value < predicate.numericGte) {
      return false;
    }
  }

  if (predicate.numericLte !== undefined) {
    if (typeof value !== "number" || value > predicate.numericLte) {
      return false;
    }
  }

  if (predicate.includesAny && predicate.includesAny.length > 0) {
    if (typeof value === "string") {
      if (!predicate.includesAny.some((needle) => value.includes(needle))) {
        return false;
      }
    } else if (Array.isArray(value)) {
      const stringValues = value.filter((item): item is string => typeof item === "string");
      if (!predicate.includesAny.some((needle) => stringValues.some((item) => item.includes(needle)))) {
        return false;
      }
    } else {
      return false;
    }
  }

  if (predicate.stringLengthLte !== undefined) {
    if (typeof value !== "string" || value.length > predicate.stringLengthLte) {
      return false;
    }
  }

  if (predicate.byteLengthLte !== undefined) {
    if (typeof value !== "string" || byteLengthOfString(value) > predicate.byteLengthLte) {
      return false;
    }
  }

  return true;
}

function testFieldPredicate(payload: JsonValue, predicate: CompiledFieldPredicate): boolean {
  const refs = selectNodeRefs(payload, predicate.selector);

  if (predicate.exists !== undefined) {
    const exists = refs.length > 0;
    if (exists !== predicate.exists) {
      return false;
    }
    if (!exists && predicate.exists === false) {
      return true;
    }
  }

  if (refs.length === 0) {
    return false;
  }

  const results = refs.map((ref) => valueMatchesPredicate(ref.value, predicate));
  return (predicate.mode ?? "any") === "all" ? results.every(Boolean) : results.some(Boolean);
}

function compileFieldActionRule(rule: FieldActionRule): CompiledFieldActionRule {
  return {
    ...rule,
    selector: parseSelector(rule.path),
    compiledWhenRegex: rule.when?.regex ? new RegExp(rule.when.regex, rule.when.flags ?? "") : undefined,
  };
}

function fieldActionWhenMatches(value: JsonValue, rule: CompiledFieldActionRule): boolean {
  if (!rule.when) {
    return true;
  }

  if (rule.compiledWhenRegex) {
    if (typeof value !== "string") {
      return false;
    }
    rule.compiledWhenRegex.lastIndex = 0;
    if (!rule.compiledWhenRegex.test(value)) {
      return false;
    }
  }

  if (rule.when.stringLengthGte !== undefined) {
    if (typeof value !== "string" || value.length < rule.when.stringLengthGte) {
      return false;
    }
  }

  if (rule.when.byteLengthGte !== undefined) {
    if (typeof value !== "string" || byteLengthOfString(value) < rule.when.byteLengthGte) {
      return false;
    }
  }

  return true;
}

function truncateText(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }
  if (maxLength <= 3) {
    return value.slice(0, maxLength);
  }
  return `${value.slice(0, maxLength - 3)}...`;
}

function createActionReplacement(value: JsonValue, rule: CompiledFieldActionRule): JsonValue | undefined {
  switch (rule.action) {
    case "redact":
      return rule.replace ?? "[REDACTED]";
    case "hash":
      return `sha256:${stableHash(value)}`;
    case "truncate": {
      const maxLength = Math.max(rule.maxLength ?? DEFAULT_TRUNCATE_LENGTH, 1);
      if (typeof value === "string") {
        if (value.length <= maxLength) {
          return undefined;
        }
        return truncateText(value, maxLength);
      }
      if (Array.isArray(value)) {
        if (value.length <= maxLength) {
          return undefined;
        }
        return value.slice(0, maxLength);
      }
      return undefined;
    }
    default:
      return undefined;
  }
}

function applyFieldActions(payload: JsonValue, rules: ReadonlyArray<CompiledFieldActionRule>): AppliedActions {
  let nextPayload = payload;
  const findings: FirewallFinding[] = [];
  const denyReasons: string[] = [];
  let redactions = 0;
  let hashes = 0;
  let truncations = 0;

  for (const rule of rules) {
    const refs = selectNodeRefs(nextPayload, rule.selector);
    for (const ref of refs) {
      if (!fieldActionWhenMatches(ref.value, rule)) {
        continue;
      }

      if (rule.action === "deny") {
        const message =
          rule.reason ??
          `Path ${ref.path} is not allowed by selector ${rule.path}.`;
        findings.push({
          code: "path-denied",
          severity: "critical",
          message,
          path: ref.path,
        });
        denyReasons.push(message);
        continue;
      }

      const replacement = createActionReplacement(ref.value, rule);
      if (replacement === undefined) {
        continue;
      }

      nextPayload = writeNodeValue(nextPayload, ref, replacement);

      if (rule.action === "redact") {
        redactions += 1;
      } else if (rule.action === "hash") {
        hashes += 1;
      } else if (rule.action === "truncate") {
        truncations += 1;
      }
    }
  }

  return {
    payload: nextPayload,
    findings,
    denyReasons,
    redactions,
    hashes,
    truncations,
  };
}

function redactPaths(payload: JsonValue, paths: ReadonlyArray<string>, replacement: string): { payload: JsonValue; count: number } {
  let nextPayload = payload;
  let count = 0;
  const uniquePaths = new Set(paths);

  for (const path of uniquePaths) {
    const refs = selectNodeRefs(nextPayload, parseSelector(path));
    for (const ref of refs) {
      nextPayload = writeNodeValue(nextPayload, ref, replacement);
      count += 1;
    }
  }

  return { payload: nextPayload, count };
}

function walkJson(
  value: JsonValue,
  visitor: (node: JsonValue, path: string, depth: number) => void,
  path = "$",
  depth = 1
): void {
  visitor(value, path, depth);

  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      walkJson(value[index], visitor, formatIndexPath(path, index), depth + 1);
    }
    return;
  }

  if (isJsonObject(value)) {
    for (const key of Object.keys(value)) {
      walkJson(value[key], visitor, formatPropertyPath(path, key), depth + 1);
    }
  }
}

function collectPayloadMetrics(payload: JsonValue): PayloadMetrics {
  const metrics: PayloadMetrics = {
    payloadBytes: byteLengthOfJson(payload),
    maxDepth: 1,
    maxArrayLength: Array.isArray(payload) ? payload.length : 0,
    maxObjectKeys: isJsonObject(payload) ? Object.keys(payload).length : 0,
    maxStringLength: typeof payload === "string" ? payload.length : 0,
    stringCount: typeof payload === "string" ? 1 : 0,
  };

  walkJson(payload, (node, _path, depth) => {
    metrics.maxDepth = Math.max(metrics.maxDepth, depth);

    if (typeof node === "string") {
      metrics.stringCount += depth === 1 && typeof payload === "string" ? 0 : 1;
      metrics.maxStringLength = Math.max(metrics.maxStringLength, node.length);
      return;
    }

    if (Array.isArray(node)) {
      metrics.maxArrayLength = Math.max(metrics.maxArrayLength, node.length);
      return;
    }

    if (isJsonObject(node)) {
      metrics.maxObjectKeys = Math.max(metrics.maxObjectKeys, Object.keys(node).length);
    }
  });

  return metrics;
}

function maskPreview(value: string, keep = 4): string {
  if (value.length <= keep * 2) {
    return "*".repeat(Math.max(value.length, 4));
  }
  return `${value.slice(0, keep)}...${value.slice(-keep)}`;
}

function scanSecrets(
  payload: JsonValue,
  patterns: ReadonlyArray<CompiledSecretPattern>,
  minConfidence: number
): SecretScanResult {
  const findings: FirewallFinding[] = [];
  const paths = new Set<string>();
  const dedupe = new Set<string>();
  let scannedStrings = 0;

  if (patterns.length === 0) {
    return { findings, paths: [], scannedStrings };
  }

  walkJson(payload, (node, path) => {
    if (typeof node !== "string") {
      return;
    }

    scannedStrings += 1;

    for (const pattern of patterns) {
      if (pattern.confidence < minConfidence) {
        continue;
      }

      pattern.regex.lastIndex = 0;
      const match = pattern.regex.exec(node);
      if (!match) {
        continue;
      }

      const key = `${pattern.name}:${path}`;
      if (dedupe.has(key)) {
        continue;
      }

      dedupe.add(key);
      paths.add(path);
      findings.push({
        code: "secret-detected",
        severity: pattern.confidence >= 0.95 ? "critical" : "warning",
        message: `Detected ${pattern.name} in ${path}.`,
        path,
        evidence: maskPreview(match[0]),
      });
    }
  });

  return {
    findings,
    paths: [...paths],
    scannedStrings,
  };
}

function normalizeRateLimit(limit: RateLimitPolicy, fallbackId: string): NormalizedRateLimit {
  if (!Number.isFinite(limit.windowMs) || limit.windowMs <= 0) {
    throw new Error(`Rate limit ${limit.id ?? fallbackId} must have a positive windowMs.`);
  }

  return {
    ...limit,
    id: limit.id ?? fallbackId,
    key: limit.key ?? "global",
    effect: limit.effect ?? "deny",
  };
}

function normalizeMatchCriteria(match?: MatchCriteria): NormalizedMatchCriteria | undefined {
  if (!match) {
    return undefined;
  }

  const metadata: Record<string, CompiledPattern[]> = {};
  for (const [key, patterns] of Object.entries(match.metadata ?? {})) {
    metadata[key] = compilePatterns(patterns);
  }

  return {
    principals: compilePatterns(match.principals),
    tenants: compilePatterns(match.tenants),
    environments: compilePatterns(match.environments),
    providers: compilePatterns(match.providers),
    servers: compilePatterns(match.servers),
    tools: compilePatterns(match.tools),
    operations: compilePatterns(match.operations),
    tagsAny: compilePatterns(match.tagsAny),
    tagsAll: compilePatterns(match.tagsAll),
    metadata,
    argPredicates: ensureArray(match.argPredicates).map((predicate) => compileFieldPredicate(predicate)),
    resultPredicates: ensureArray(match.resultPredicates).map((predicate) => compileFieldPredicate(predicate)),
  };
}

function toTimestamp(value: string | number | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("expiresAt must be a finite number or ISO date string.");
    }
    return value;
  }
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    throw new Error(`Invalid expiresAt value: ${value}`);
  }
  return parsed;
}

function compileSecretPattern(pattern: SecretPattern): CompiledSecretPattern {
  return {
    name: pattern.name,
    regex: new RegExp(pattern.regex, pattern.flags ?? ""),
    confidence: pattern.confidence ?? 0.9,
  };
}

function normalizePolicy(policy: FirewallPolicy): NormalizedPolicy {
  const seenRuleIds = new Set<string>();
  for (const rule of policy.rules) {
    if (seenRuleIds.has(rule.id)) {
      throw new Error(`Duplicate firewall rule id: ${rule.id}`);
    }
    seenRuleIds.add(rule.id);
  }

  const secretPatternSource = policy.secretPatterns === undefined ? DEFAULT_SECRET_PATTERNS : policy.secretPatterns;
  const secretPatterns = secretPatternSource
    .filter((pattern) => pattern.enabled !== false)
    .map((pattern) => compileSecretPattern(pattern));

  const rules = policy.rules
    .map<NormalizedRule>((rule) => ({
      id: rule.id,
      description: rule.description,
      effect: rule.effect,
      priority: rule.priority ?? 0,
      enabled: rule.enabled !== false,
      expiresAtMs: toTimestamp(rule.expiresAt),
      match: normalizeMatchCriteria(rule.match),
      constraints: rule.constraints,
      redactArgs: ensureArray(rule.redactArgs).map((item) => compileFieldActionRule(item)),
      redactResult: ensureArray(rule.redactResult).map((item) => compileFieldActionRule(item)),
      rateLimits: ensureArray(rule.rateLimit).map((limit, index) =>
        normalizeRateLimit(limit, `${rule.id}:${index}`)
      ),
      detectSecrets: rule.detectSecrets ?? false,
      secretHandling: rule.secretHandling,
      risk: rule.risk ?? 0,
    }))
    .sort((left, right) => right.priority - left.priority || left.id.localeCompare(right.id));

  const normalizedRateLimits = ensureArray(policy.rateLimits).map((limit, index) =>
    normalizeRateLimit(limit, `global:${index}`)
  );

  const effectiveSecretHandling = normalizeSecretHandling(policy.secretHandling);

  return {
    name: policy.name,
    version: policy.version,
    defaultEffect: policy.defaultEffect ?? "deny",
    requireAllowRule: policy.requireAllowRule ?? false,
    secretPatterns,
    secretHandling: effectiveSecretHandling,
    rateLimits: normalizedRateLimits,
    rules,
    fingerprint: stableHash({
      name: policy.name,
      version: policy.version ?? null,
      defaultEffect: policy.defaultEffect ?? "deny",
      requireAllowRule: policy.requireAllowRule ?? false,
      secretPatterns: secretPatternSource,
      secretHandling: effectiveSecretHandling,
      rateLimits: normalizedRateLimits,
      rules: policy.rules,
    }),
  };
}

function matchesPatternList(value: string | undefined, patterns: ReadonlyArray<CompiledPattern>): boolean {
  if (patterns.length === 0) {
    return true;
  }
  if (!value) {
    return false;
  }
  return patterns.some((pattern) => pattern.test(value));
}

function matchesTagAny(tags: string[] | undefined, patterns: ReadonlyArray<CompiledPattern>): boolean {
  if (patterns.length === 0) {
    return true;
  }
  if (!tags || tags.length === 0) {
    return false;
  }
  return tags.some((tag) => patterns.some((pattern) => pattern.test(tag)));
}

function matchesTagAll(tags: string[] | undefined, patterns: ReadonlyArray<CompiledPattern>): boolean {
  if (patterns.length === 0) {
    return true;
  }
  if (!tags || tags.length === 0) {
    return false;
  }
  return patterns.every((pattern) => tags.some((tag) => pattern.test(tag)));
}

function matchesMetadata(
  metadata: Record<string, JsonValue> | undefined,
  compiled: Record<string, CompiledPattern[]>
): boolean {
  const keys = Object.keys(compiled);
  if (keys.length === 0) {
    return true;
  }
  if (!metadata) {
    return false;
  }

  for (const key of keys) {
    const value = metadata[key];
    if (value === undefined) {
      return false;
    }
    const candidate = typeof value === "string" ? value : stableStringifyUnknown(value);
    if (!compiled[key].some((pattern) => pattern.test(candidate))) {
      return false;
    }
  }

  return true;
}

function ruleMatches(
  rule: NormalizedRule,
  invocation: InvocationContext,
  resultPayload: JsonValue | undefined,
  channel: PolicyChannel,
  now: number
): boolean {
  if (!rule.enabled) {
    return false;
  }
  if (rule.expiresAtMs !== undefined && now >= rule.expiresAtMs) {
    return false;
  }

  const match = rule.match;
  if (!match) {
    return true;
  }

  if (!matchesPatternList(invocation.principal, match.principals)) {
    return false;
  }
  if (!matchesPatternList(invocation.tenant, match.tenants)) {
    return false;
  }
  if (!matchesPatternList(invocation.environment, match.environments)) {
    return false;
  }
  if (!matchesPatternList(invocation.provider, match.providers)) {
    return false;
  }
  if (!matchesPatternList(invocation.server, match.servers)) {
    return false;
  }
  if (!matchesPatternList(invocation.tool, match.tools)) {
    return false;
  }
  if (!matchesPatternList(invocation.operation, match.operations)) {
    return false;
  }
  if (!matchesTagAny(invocation.tags, match.tagsAny)) {
    return false;
  }
  if (!matchesTagAll(invocation.tags, match.tagsAll)) {
    return false;
  }
  if (!matchesMetadata(invocation.metadata, match.metadata)) {
    return false;
  }
  if (!match.argPredicates.every((predicate) => testFieldPredicate(invocation.args, predicate))) {
    return false;
  }
  if (channel === "result" && match.resultPredicates.length > 0) {
    if (!resultPayload) {
      return false;
    }
    if (!match.resultPredicates.every((predicate) => testFieldPredicate(resultPayload, predicate))) {
      return false;
    }
  }

  return true;
}

function buildInvocationFingerprint(invocation: InvocationContext): string {
  return stableHash({
    principal: invocation.principal ?? null,
    tenant: invocation.tenant ?? null,
    environment: invocation.environment ?? null,
    provider: invocation.provider ?? null,
    server: invocation.server ?? null,
    tool: invocation.tool ?? null,
    operation: invocation.operation ?? null,
    args: invocation.args,
  });
}

function resolveSecretHandling(
  base: Required<SecretHandling>,
  matchedRules: ReadonlyArray<NormalizedRule>
): Required<SecretHandling> {
  const resolved = { ...base };
  for (let index = matchedRules.length - 1; index >= 0; index -= 1) {
    const override = matchedRules[index].secretHandling;
    if (!override) {
      continue;
    }
    if (override.effectOnArgs !== undefined) {
      resolved.effectOnArgs = override.effectOnArgs;
    }
    if (override.effectOnResults !== undefined) {
      resolved.effectOnResults = override.effectOnResults;
    }
    if (override.autoRedact !== undefined) {
      resolved.autoRedact = override.autoRedact;
    }
    if (override.redactionText !== undefined) {
      resolved.redactionText = override.redactionText;
    }
    if (override.minConfidence !== undefined) {
      resolved.minConfidence = override.minConfidence;
    }
  }
  return resolved;
}

function evaluatePayloadConstraints(
  payload: JsonValue,
  metrics: PayloadMetrics,
  constraints: PayloadConstraint | undefined,
  state: FirewallStateAdapter,
  invocationFingerprint: string,
  now: number,
  dedupeNamespace: string,
  channel: PolicyChannel
): ConstraintOutcome[] {
  if (!constraints) {
    return [];
  }

  const outcomes: ConstraintOutcome[] = [];

  for (const path of constraints.requirePaths ?? []) {
    const exists = selectNodeRefs(payload, parseSelector(path)).length > 0;
    if (!exists) {
      outcomes.push({
        code: "constraint-violation",
        effect: "deny",
        path,
        message: `Required path ${path} is missing.`,
      });
    }
  }

  for (const path of constraints.forbidPaths ?? []) {
    const exists = selectNodeRefs(payload, parseSelector(path)).length > 0;
    if (exists) {
      outcomes.push({
        code: "constraint-violation",
        effect: "deny",
        path,
        message: `Forbidden path ${path} is present.`,
      });
    }
  }

  if (constraints.maxPayloadBytes !== undefined && metrics.payloadBytes > constraints.maxPayloadBytes) {
    outcomes.push({
      code: "constraint-violation",
      effect: "deny",
      message: `Payload is ${metrics.payloadBytes} bytes and exceeds the ${constraints.maxPayloadBytes} byte limit.`,
      evidence: `${metrics.payloadBytes}`,
    });
  }
  if (constraints.maxDepth !== undefined && metrics.maxDepth > constraints.maxDepth) {
    outcomes.push({
      code: "constraint-violation",
      effect: "deny",
      message: `Payload depth ${metrics.maxDepth} exceeds the allowed depth ${constraints.maxDepth}.`,
      evidence: `${metrics.maxDepth}`,
    });
  }
  if (constraints.maxArrayLength !== undefined && metrics.maxArrayLength > constraints.maxArrayLength) {
    outcomes.push({
      code: "constraint-violation",
      effect: "deny",
      message: `Payload array length ${metrics.maxArrayLength} exceeds the allowed maximum ${constraints.maxArrayLength}.`,
      evidence: `${metrics.maxArrayLength}`,
    });
  }
  if (constraints.maxObjectKeys !== undefined && metrics.maxObjectKeys > constraints.maxObjectKeys) {
    outcomes.push({
      code: "constraint-violation",
      effect: "deny",
      message: `Payload object key count ${metrics.maxObjectKeys} exceeds the allowed maximum ${constraints.maxObjectKeys}.`,
      evidence: `${metrics.maxObjectKeys}`,
    });
  }
  if (constraints.maxStringLength !== undefined && metrics.maxStringLength > constraints.maxStringLength) {
    outcomes.push({
      code: "constraint-violation",
      effect: "deny",
      message: `Payload string length ${metrics.maxStringLength} exceeds the allowed maximum ${constraints.maxStringLength}.`,
      evidence: `${metrics.maxStringLength}`,
    });
  }

  if (channel === "args" && constraints.duplicateWindowMs !== undefined) {
    const duplicateEffect = constraints.duplicateEffect ?? "review";
    const duplicate = state.recordFingerprint(
      `duplicate:${dedupeNamespace}:${invocationFingerprint}`,
      Math.max(constraints.duplicateWindowMs, 1),
      now
    );
    if (duplicate.duplicate) {
      outcomes.push({
        code: "duplicate-request",
        effect: duplicateEffect,
        message: `A semantically identical invocation already ran within ${constraints.duplicateWindowMs} ms.`,
      });
    }
  }

  return outcomes;
}

function safeIdentityPart(value: string | undefined): string {
  return value && value.length > 0 ? value : "_";
}

function buildRateLimitKey(
  limit: NormalizedRateLimit,
  invocation: InvocationContext,
  channel: PolicyChannel
): string {
  const principal = safeIdentityPart(invocation.principal);
  const tenant = safeIdentityPart(invocation.tenant);
  const provider = safeIdentityPart(invocation.provider);
  const server = safeIdentityPart(invocation.server);
  const tool = safeIdentityPart(invocation.tool);

  switch (limit.key) {
    case "principal":
      return `${limit.id}:${channel}:principal:${principal}`;
    case "tenant":
      return `${limit.id}:${channel}:tenant:${tenant}`;
    case "provider":
      return `${limit.id}:${channel}:provider:${provider}`;
    case "server":
      return `${limit.id}:${channel}:server:${server}`;
    case "tool":
      return `${limit.id}:${channel}:tool:${tool}`;
    case "principal+tool":
      return `${limit.id}:${channel}:principal+tool:${principal}:${tool}`;
    case "tenant+tool":
      return `${limit.id}:${channel}:tenant+tool:${tenant}:${tool}`;
    case "server+tool":
      return `${limit.id}:${channel}:server+tool:${server}:${tool}`;
    case "principal+server+tool":
      return `${limit.id}:${channel}:principal+server+tool:${principal}:${server}:${tool}`;
    case "global":
    default:
      return `${limit.id}:${channel}:global`;
  }
}

export class InMemoryFirewallState implements FirewallStateAdapter {
  private readonly fingerprints = new Map<string, number>();
  private readonly buckets = new Map<string, RateBucket>();

  recordFingerprint(key: string, ttlMs: number, now: number): { duplicate: boolean; expiresAt: number } {
    this.prune(now);
    const currentExpiry = this.fingerprints.get(key);
    const duplicate = typeof currentExpiry === "number" && currentExpiry > now;
    const expiresAt = now + Math.max(ttlMs, 1);
    this.fingerprints.set(key, expiresAt);
    return { duplicate, expiresAt };
  }

  consumeRateLimit(
    key: string,
    policy: RateLimitPolicy,
    sample: { payloadBytes: number; risk: number },
    now: number
  ): {
    exceeded: boolean;
    snapshot: { calls: number; payloadBytes: number; risk: number; windowStartedAt: number };
  } {
    this.prune(now);

    const windowMs = Math.max(policy.windowMs, 1);
    const previous = this.buckets.get(key);
    const bucket: RateBucket =
      previous && now - previous.windowStartedAt < windowMs
        ? { ...previous }
        : {
            windowStartedAt: now,
            lastTouchedAt: now,
            calls: 0,
            payloadBytes: 0,
            risk: 0,
          };

    bucket.calls += 1;
    bucket.payloadBytes += sample.payloadBytes;
    bucket.risk += sample.risk;
    bucket.lastTouchedAt = now;

    this.buckets.set(key, bucket);

    const exceeded =
      (policy.maxCalls !== undefined && bucket.calls > policy.maxCalls) ||
      (policy.maxPayloadBytes !== undefined && bucket.payloadBytes > policy.maxPayloadBytes) ||
      (policy.maxRisk !== undefined && bucket.risk > policy.maxRisk);

    return {
      exceeded,
      snapshot: {
        calls: bucket.calls,
        payloadBytes: bucket.payloadBytes,
        risk: bucket.risk,
        windowStartedAt: bucket.windowStartedAt,
      },
    };
  }

  prune(now: number): void {
    for (const [key, expiresAt] of this.fingerprints) {
      if (expiresAt <= now) {
        this.fingerprints.delete(key);
      }
    }

    for (const [key, bucket] of this.buckets) {
      if (now - bucket.lastTouchedAt > STALE_RATE_BUCKET_MS) {
        this.buckets.delete(key);
      }
    }
  }
}

export class McpInvocationFirewall {
  private readonly normalizedPolicy: NormalizedPolicy;
  private readonly state: FirewallStateAdapter;

  constructor(policy: FirewallPolicy, state?: FirewallStateAdapter) {
    this.normalizedPolicy = normalizePolicy(policy);
    this.state = state ?? new InMemoryFirewallState();
  }

  public evaluateInvocation(context: InvocationContext): FirewallDecision<"args"> {
    const now = context.timestamp ?? Date.now();
    return this.evaluate("args", context, context.args, undefined, now);
  }

  public evaluateResult(context: ResultContext): FirewallDecision<"result"> {
    const now = context.timestamp ?? context.invocation.timestamp ?? Date.now();
    return this.evaluate("result", context.invocation, context.result, context.result, now);
  }

  public explainDecision(decision: FirewallDecision): string {
    const lines = [
      `${decision.effect.toUpperCase()} ${decision.channel} policy=${decision.policyName} risk=${decision.riskScore.toFixed(2)}`,
      `request=${decision.requestFingerprint.slice(0, 12)} payload=${decision.payloadFingerprint.slice(0, 12)}`,
      `matchedRules=${decision.matchedRuleIds.length > 0 ? decision.matchedRuleIds.join(",") : "none"}`,
    ];
    for (const reason of decision.reasons) {
      lines.push(`- ${reason}`);
    }
    return lines.join("\n");
  }

  private evaluate<TChannel extends PolicyChannel>(
    channel: TChannel,
    invocation: InvocationContext,
    payload: JsonValue,
    resultPayload: JsonValue | undefined,
    now: number
  ): FirewallDecision<TChannel> {
    this.state.prune(now);

    const metrics = collectPayloadMetrics(payload);
    const requestFingerprint = buildInvocationFingerprint(invocation);
    const payloadFingerprint = stableHash(payload);
    const matchedRules = this.normalizedPolicy.rules.filter((rule) =>
      ruleMatches(rule, invocation, resultPayload, channel, now)
    );

    let sanitizedPayload = deepCloneJson(payload);
    const reasons: string[] = [];
    const findings: FirewallFinding[] = [];
    const matchedRuleIds = matchedRules.map((rule) => rule.id);
    let riskScore = 0;
    let allowMatched = false;
    let reviewTriggered = false;
    let denyTriggered = false;
    let redactions = 0;
    let hashes = 0;
    let truncations = 0;
    let rateLimitHits = 0;
    let scannedStrings = metrics.stringCount;

    for (const rule of matchedRules) {
      riskScore += rule.risk;

      if (rule.effect === "allow") {
        allowMatched = true;
      } else if (rule.effect === "review") {
        reviewTriggered = true;
        reasons.push(
          rule.description
            ? `Rule ${rule.id} requires review: ${rule.description}`
            : `Rule ${rule.id} requires review.`
        );
        findings.push({
          code: "rule-review",
          severity: "warning",
          message: `Rule ${rule.id} requires review.`,
          ruleId: rule.id,
        });
      } else if (rule.effect === "deny") {
        denyTriggered = true;
        reasons.push(
          rule.description
            ? `Rule ${rule.id} denied the ${channel}: ${rule.description}`
            : `Rule ${rule.id} denied the ${channel}.`
        );
        findings.push({
          code: "rule-deny",
          severity: "critical",
          message: `Rule ${rule.id} denied the ${channel}.`,
          ruleId: rule.id,
        });
      }

      const constraintOutcomes = evaluatePayloadConstraints(
        payload,
        metrics,
        rule.constraints,
        this.state,
        requestFingerprint,
        now,
        rule.id,
        channel
      );
      for (const outcome of constraintOutcomes) {
        if (outcome.effect === "deny") {
          denyTriggered = true;
        } else {
          reviewTriggered = true;
        }
        reasons.push(`Rule ${rule.id}: ${outcome.message}`);
        findings.push({
          code: outcome.code,
          severity: outcome.effect === "deny" ? "critical" : "warning",
          message: outcome.message,
          path: outcome.path,
          ruleId: rule.id,
          evidence: outcome.evidence,
        });
      }

      const actions = applyFieldActions(
        sanitizedPayload,
        channel === "args" ? rule.redactArgs : rule.redactResult
      );
      sanitizedPayload = actions.payload;
      redactions += actions.redactions;
      hashes += actions.hashes;
      truncations += actions.truncations;

      if (actions.findings.length > 0) {
        findings.push(
          ...actions.findings.map((finding) => ({
            ...finding,
            ruleId: rule.id,
          }))
        );
      }
      if (actions.denyReasons.length > 0) {
        denyTriggered = true;
        for (const message of actions.denyReasons) {
          reasons.push(`Rule ${rule.id}: ${message}`);
        }
      }

      for (const limit of rule.rateLimits) {
        const bucketKey = buildRateLimitKey(limit, invocation, channel);
        const rateResult = this.state.consumeRateLimit(
          bucketKey,
          limit,
          { payloadBytes: metrics.payloadBytes, risk: rule.risk },
          now
        );
        if (!rateResult.exceeded) {
          continue;
        }

        rateLimitHits += 1;
        if (limit.effect === "deny") {
          denyTriggered = true;
        } else {
          reviewTriggered = true;
        }

        const message = `Rate limit ${limit.id} exceeded for scope ${limit.key}.`;
        reasons.push(message);
        findings.push({
          code: "rate-limit-exceeded",
          severity: limit.effect === "deny" ? "critical" : "warning",
          message,
          ruleId: rule.id,
          evidence: stableStringifyUnknown(rateResult.snapshot),
        });
      }
    }

    for (const limit of this.normalizedPolicy.rateLimits) {
      const bucketKey = buildRateLimitKey(limit, invocation, channel);
      const rateResult = this.state.consumeRateLimit(
        bucketKey,
        limit,
        { payloadBytes: metrics.payloadBytes, risk: riskScore },
        now
      );
      if (!rateResult.exceeded) {
        continue;
      }

      rateLimitHits += 1;
      if (limit.effect === "deny") {
        denyTriggered = true;
      } else {
        reviewTriggered = true;
      }

      const message = `Global rate limit ${limit.id} exceeded for scope ${limit.key}.`;
      reasons.push(message);
      findings.push({
        code: "rate-limit-exceeded",
        severity: limit.effect === "deny" ? "critical" : "warning",
        message,
        evidence: stableStringifyUnknown(rateResult.snapshot),
      });
    }

    const secretHandling = resolveSecretHandling(this.normalizedPolicy.secretHandling, matchedRules);
    if (this.normalizedPolicy.secretPatterns.length > 0) {
      const scan = scanSecrets(payload, this.normalizedPolicy.secretPatterns, secretHandling.minConfidence);
      scannedStrings = scan.scannedStrings;
      if (scan.findings.length > 0) {
        findings.push(...scan.findings);
        reasons.push(
          `Detected ${scan.findings.length} potential secret${scan.findings.length === 1 ? "" : "s"} in ${
            channel === "args" ? "tool arguments" : "tool results"
          }.`
        );

        const effect = channel === "args" ? secretHandling.effectOnArgs : secretHandling.effectOnResults;
        if (effect === "deny") {
          denyTriggered = true;
        } else if (effect === "review") {
          reviewTriggered = true;
        }

        if (secretHandling.autoRedact) {
          const redacted = redactPaths(sanitizedPayload, scan.paths, secretHandling.redactionText);
          sanitizedPayload = redacted.payload;
          redactions += redacted.count;
        }
      }
    }

    if (this.normalizedPolicy.requireAllowRule && !allowMatched) {
      denyTriggered = true;
      reasons.push("No allow rule matched and the policy requires an explicit allow.");
    }

    let effect: DecisionEffect = this.normalizedPolicy.defaultEffect;
    if (denyTriggered) {
      effect = "deny";
    } else if (reviewTriggered) {
      effect = "review";
    } else if (allowMatched) {
      effect = "allow";
    }

    if (matchedRules.length === 0) {
      reasons.push(`No rule matched; default ${effect} policy applied.`);
    }

    return {
      channel,
      effect,
      allowed: effect !== "deny",
      requiresReview: effect === "review",
      policyName: this.normalizedPolicy.name,
      policyFingerprint: this.normalizedPolicy.fingerprint,
      requestFingerprint,
      payloadFingerprint,
      matchedRuleIds,
      reasons,
      findings,
      riskScore: Number(riskScore.toFixed(3)),
      sanitizedPayload,
      stats: {
        payloadBytes: metrics.payloadBytes,
        maxDepth: metrics.maxDepth,
        matchedRules: matchedRules.length,
        scannedStrings,
        secretFindings: findings.filter((finding) => finding.code === "secret-detected").length,
        redactions,
        hashes,
        truncations,
        rateLimitHits,
      },
      evaluatedAt: now,
    };
  }
}

/**
 * This solves MCP firewall TypeScript, Model Context Protocol security policy, LLM tool-call guardrails, secret redaction, duplicate invocation detection, and tool result DLP in one file. Built because teams keep wiring OpenAI, Anthropic, Vercel AI SDK, LangGraph, and custom agent hosts straight into MCP servers, and the weak point is usually not the model itself. It is the missing control layer between the model and the tool. One bad prompt, one leaked token in a tool result, or one repeated tool retry can turn into real damage fast.
 *
 * Use it when you need a production-ready TypeScript MCP policy engine that can decide allow, review, or deny before a tool runs and again before a tool result gets forwarded back into the model loop. The trick: the policy stays deterministic. It hashes requests in a stable way, matches rules in priority order, can rate-limit by principal or tool, redacts or hashes sensitive fields, spots common secrets, and catches duplicate calls without forcing you into a database or framework.
 *
 * Drop this into any Node or TypeScript agent codebase that needs MCP security, tool permission checks, result filtering, or secret-aware logging. I kept it as a single source file on purpose so Pavan can fork it, read it end to end, and wire it into a host runtime without having to chase a whole package tree first.
 */