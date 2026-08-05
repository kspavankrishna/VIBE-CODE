#!/usr/bin/env node
/*
ToolCallSchemaDriftGuard audits declared AI tool schemas against observed tool
call traces. It is dependency-free TypeScript so it can run in locked down CI,
MCP servers, eval jobs, and incident review machines without installing a
validator package first.
*/

import * as fs from "node:fs";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | JsonObject | JsonValue[];

interface JsonObject {
  [key: string]: JsonValue;
}

type Severity = "critical" | "high" | "medium" | "low";
type OutputFormat = "json" | "markdown" | "sarif" | "baseline";

interface CliOptions {
  schemaPath?: string;
  tracePaths: string[];
  baselinePath?: string;
  outputPath?: string;
  format: OutputFormat;
  failOn: Set<Severity>;
  minCoverage: number;
  maxSamplesPerTool: number;
  allowMissingTrace: boolean;
}

interface JsonSchema {
  [key: string]: any;
}

interface ToolContract {
  name: string;
  schema: JsonSchema;
  schemaHash: string;
  source: string;
  required: Set<string>;
  properties: Map<string, JsonSchema>;
  allowAdditional: boolean;
}

interface ObservedCall {
  name: string;
  args: JsonValue;
  source: string;
  line: number;
  recordHash: string;
  schemaHash?: string;
  raw: JsonObject;
  argumentWasString: boolean;
}

interface SchemaIssue {
  path: string;
  code: string;
  message: string;
  severity: Severity;
}

interface Finding {
  ruleId: string;
  severity: Severity;
  message: string;
  source: string;
  line: number;
  toolName?: string;
  remediation: string;
  evidence: JsonObject;
}

interface ToolSummary {
  name: string;
  schemaHash: string;
  required: string[];
  declaredProperties: string[];
  observedCalls: number;
  invalidCalls: number;
  unknownFields: string[];
  sampledArgumentHashes: string[];
}

interface Report {
  tool: string;
  version: string;
  generatedAtEpoch: number;
  pass: boolean;
  summary: JsonObject;
  tools: ToolSummary[];
  findings: Finding[];
}

interface BaselineTool {
  name: string;
  schemaHash: string;
  required?: string[];
  declaredProperties?: string[];
}

interface Baseline {
  tool?: string;
  generatedAtEpoch?: number;
  tools?: BaselineTool[];
  [key: string]: any;
}

const VERSION = "1.0.0";
const SEVERITIES: Severity[] = ["critical", "high", "medium", "low"];
const TRACE_ARRAY_KEYS = [
  "tool_calls",
  "toolCalls",
  "events",
  "spans",
  "records",
  "requests",
  "steps",
  "messages",
  "data",
  "logs",
];
const ARGUMENT_KEYS = [
  "arguments",
  "args",
  "input",
  "tool_input",
  "toolInput",
  "parameters",
  "params",
  "payload",
  "body",
];
const TOOL_NAME_KEYS = [
  "tool_name",
  "toolName",
  "tool",
  "name",
  "function.name",
  "function_name",
  "callee",
  "operation",
  "span.name",
];
const SCHEMA_HASH_KEYS = [
  "schema_hash",
  "schemaHash",
  "tool_schema_hash",
  "toolSchemaHash",
  "input_schema_hash",
  "inputSchemaHash",
];

class GuardError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GuardError";
  }
}

function main(argv: string[]): number {
  try {
    const options = parseArgs(argv);
    if (!options.schemaPath) {
      throw new GuardError("--schema is required");
    }
    const contracts = loadContracts(options.schemaPath);
    const baseline = options.baselinePath ? loadBaseline(options.baselinePath) : undefined;
    if (options.format === "baseline") {
      const payload = renderBaseline(contracts);
      writeText(options.outputPath, JSON.stringify(payload, null, 2) + "\n");
      return 0;
    }

    const calls = loadCalls(options.tracePaths, options.allowMissingTrace);
    const report = buildReport(contracts, calls, baseline, options);
    const rendered = renderReport(report, options.format);
    writeText(options.outputPath, rendered);
    return report.pass ? 0 : 1;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`ToolCallSchemaDriftGuard: ${message}`);
    return 2;
  }
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    tracePaths: [],
    format: "json",
    failOn: new Set<Severity>(["critical", "high"]),
    minCoverage: 0,
    maxSamplesPerTool: 5,
    allowMissingTrace: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = (): string => {
      i += 1;
      if (i >= argv.length) {
        throw new GuardError(`${arg} needs a value`);
      }
      return argv[i];
    };
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    } else if (arg === "--schema") {
      options.schemaPath = next();
    } else if (arg === "--trace") {
      options.tracePaths.push(next());
    } else if (arg === "--baseline") {
      options.baselinePath = next();
    } else if (arg === "--output" || arg === "-o") {
      options.outputPath = next();
    } else if (arg === "--format") {
      const value = next() as OutputFormat;
      if (!["json", "markdown", "sarif", "baseline"].includes(value)) {
        throw new GuardError("--format must be json, markdown, sarif, or baseline");
      }
      options.format = value;
    } else if (arg === "--fail-on") {
      options.failOn = parseSeveritySet(next());
    } else if (arg === "--min-coverage") {
      options.minCoverage = parseNumber(next(), "--min-coverage", 0, 100);
    } else if (arg === "--max-samples-per-tool") {
      options.maxSamplesPerTool = Math.floor(parseNumber(next(), "--max-samples-per-tool", 0, 1000));
    } else if (arg === "--allow-missing-trace") {
      options.allowMissingTrace = true;
    } else if (arg.startsWith("-")) {
      throw new GuardError(`unknown option ${arg}`);
    } else {
      options.tracePaths.push(arg);
    }
  }
  if (options.tracePaths.length === 0 && options.format !== "baseline") {
    throw new GuardError("at least one --trace path is required");
  }
  return options;
}

function printHelp(): void {
  console.log([
    "Usage: ToolCallSchemaDriftGuard.ts --schema tools.json --trace trace.jsonl [options]",
    "",
    "Options:",
    "  --schema PATH              OpenAI, MCP, LangGraph, or custom tool schema JSON",
    "  --trace PATH               JSON, JSONL, or stdin trace path; repeatable",
    "  --baseline PATH            Optional baseline produced by --format baseline",
    "  --format json|markdown|sarif|baseline",
    "  --fail-on LIST             Comma separated severities that exit 1",
    "  --min-coverage PERCENT     Require observed coverage for declared tools",
    "  --max-samples-per-tool N   Keep at most N argument hashes per tool",
    "  --allow-missing-trace      Keep running when a trace file is absent",
    "  --output PATH              Write output to a file instead of stdout",
  ].join("\n"));
}

function parseSeveritySet(value: string): Set<Severity> {
  const out = new Set<Severity>();
  for (const item of value.split(",")) {
    const trimmed = item.trim().toLowerCase();
    if (!trimmed) {
      continue;
    }
    if (!SEVERITIES.includes(trimmed as Severity)) {
      throw new GuardError(`unknown severity ${trimmed}`);
    }
    out.add(trimmed as Severity);
  }
  return out;
}

function parseNumber(raw: string, label: string, min: number, max: number): number {
  const value = Number(raw);
  if (!Number.isFinite(value) || value < min || value > max) {
    throw new GuardError(`${label} must be a number between ${min} and ${max}`);
  }
  return value;
}

function readText(path: string): string {
  if (path === "-") {
    return fs.readFileSync(0, "utf8");
  }
  try {
    return fs.readFileSync(path, "utf8");
  } catch (error) {
    throw new GuardError(`${path}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function writeText(path: string | undefined, content: string): void {
  if (!path || path === "-") {
    process.stdout.write(content);
    return;
  }
  fs.writeFileSync(path, content, "utf8");
}

function readJson(path: string): any {
  const text = readText(path);
  try {
    return JSON.parse(text);
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new GuardError(`${path}: invalid JSON: ${error.message}`);
    }
    throw error;
  }
}

function loadContracts(path: string): ToolContract[] {
  const parsed = readJson(path);
  const entries = discoverToolEntries(parsed);
  if (entries.length === 0) {
    throw new GuardError(`${path}: no tool schemas found`);
  }
  const seen = new Set<string>();
  const contracts = entries.map((entry, index) => normalizeContract(entry, `${path}#${index + 1}`));
  for (const contract of contracts) {
    if (seen.has(contract.name)) {
      throw new GuardError(`${path}: duplicate tool name ${contract.name}`);
    }
    seen.add(contract.name);
  }
  return contracts.sort((left, right) => left.name.localeCompare(right.name));
}

function discoverToolEntries(value: any): any[] {
  if (Array.isArray(value)) {
    return value;
  }
  if (!isObject(value)) {
    return [];
  }
  for (const key of ["tools", "functions", "toolSchemas", "tool_schemas"]) {
    if (Array.isArray(value[key])) {
      return value[key];
    }
  }
  if (isObject(value.tool) || isObject(value.function)) {
    return [value];
  }
  const mapped: any[] = [];
  for (const [key, nested] of Object.entries(value)) {
    if (isObject(nested)) {
      const objectValue = nested as JsonObject;
      if (objectValue.input_schema || objectValue.inputSchema || objectValue.parameters || objectValue.schema) {
        mapped.push({ name: key, ...objectValue });
      }
    }
  }
  return mapped;
}

function normalizeContract(entry: any, source: string): ToolContract {
  if (!isObject(entry)) {
    throw new GuardError(`${source}: tool entry must be an object`);
  }
  const functionObject = isObject(entry.function) ? entry.function : {};
  const name = firstStringFrom(entry, ["name"]) || firstStringFrom(functionObject, ["name"]);
  if (!name) {
    throw new GuardError(`${source}: tool entry has no name`);
  }
  const schema =
    asSchema(entry.input_schema) ||
    asSchema(entry.inputSchema) ||
    asSchema(entry.parameters) ||
    asSchema(entry.schema) ||
    asSchema(functionObject.parameters) ||
    {};
  const objectSchema = schema.type === "object" || schema.properties || schema.required ? schema : { type: "object", properties: schema };
  const properties = new Map<string, JsonSchema>();
  if (isObject(objectSchema.properties)) {
    for (const [propertyName, propertySchema] of Object.entries(objectSchema.properties)) {
      properties.set(propertyName, asSchema(propertySchema) || {});
    }
  }
  const required = new Set<string>(Array.isArray(objectSchema.required) ? objectSchema.required.map(String) : []);
  return {
    name,
    schema: objectSchema,
    schemaHash: sha256(canonicalize(objectSchema)).slice(0, 16),
    source,
    required,
    properties,
    allowAdditional: objectSchema.additionalProperties !== false,
  };
}

function asSchema(value: any): JsonSchema | undefined {
  return isObject(value) ? (value as JsonSchema) : undefined;
}

function loadBaseline(path: string): Baseline {
  const baseline = readJson(path);
  if (!isObject(baseline)) {
    throw new GuardError(`${path}: baseline must be a JSON object`);
  }
  return baseline as Baseline;
}

function loadCalls(paths: string[], allowMissing: boolean): ObservedCall[] {
  const calls: ObservedCall[] = [];
  for (const path of paths) {
    if (path !== "-" && !fs.existsSync(path)) {
      if (allowMissing) {
        continue;
      }
      throw new GuardError(`${path}: trace file does not exist`);
    }
    const text = readText(path);
    for (const record of parseTraceRecords(text, path)) {
      const call = normalizeCall(record.value, record.source, record.line);
      if (call) {
        calls.push(call);
      }
    }
  }
  return calls;
}

function parseTraceRecords(text: string, source: string): Array<{ value: JsonObject; source: string; line: number }> {
  const trimmed = text.trim();
  if (!trimmed) {
    return [];
  }
  if (trimmed[0] === "{" || trimmed[0] === "[") {
    try {
      const parsed = JSON.parse(trimmed);
      return flattenTraceRecords(parsed, source, 1);
    } catch (error) {
      if (!trimmed.includes("\n")) {
        throw new GuardError(`${source}: invalid JSON trace: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  }
  const rows: Array<{ value: JsonObject; source: string; line: number }> = [];
  const lines = text.split(/\r?\n/);
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line || line.startsWith("#")) {
      continue;
    }
    try {
      const parsed = JSON.parse(line);
      for (const record of flattenTraceRecords(parsed, source, i + 1)) {
        rows.push(record);
      }
    } catch (error) {
      throw new GuardError(`${source}:${i + 1}: invalid JSONL record`);
    }
  }
  return rows;
}

function flattenTraceRecords(value: any, source: string, line: number): Array<{ value: JsonObject; source: string; line: number }> {
  const rows: Array<{ value: JsonObject; source: string; line: number }> = [];
  const visit = (node: any): void => {
    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }
    if (!isObject(node)) {
      return;
    }
    if (looksLikeToolCall(node)) {
      rows.push({ value: node as JsonObject, source, line });
      return;
    }
    for (const key of TRACE_ARRAY_KEYS) {
      const nested = node[key];
      if (Array.isArray(nested)) {
        for (const item of nested) {
          visit(item);
        }
      }
    }
  };
  visit(value);
  return rows;
}

function looksLikeToolCall(value: any): boolean {
  if (!isObject(value)) {
    return false;
  }
  const flat = flatten(value);
  const typeValue = String(flat.type || flat.kind || "").toLowerCase();
  if (typeValue.includes("tool") || typeValue.includes("function_call")) {
    return true;
  }
  return Boolean(findDottedString(flat, TOOL_NAME_KEYS) && findAnyValue(flat, ARGUMENT_KEYS));
}

function normalizeCall(raw: JsonObject, source: string, line: number): ObservedCall | undefined {
  const flat = flatten(raw);
  const rawName = findDottedString(flat, TOOL_NAME_KEYS);
  const argsValue = findAnyValue(flat, ARGUMENT_KEYS);
  if (!rawName || argsValue === undefined) {
    return undefined;
  }
  const parsedArgs = parseArguments(argsValue);
  return {
    name: normalizeToolName(rawName),
    args: parsedArgs.value,
    source,
    line,
    recordHash: sha256(canonicalize(raw)).slice(0, 16),
    schemaHash: findDottedString(flat, SCHEMA_HASH_KEYS),
    raw,
    argumentWasString: parsedArgs.wasString,
  };
}

function parseArguments(value: any): { value: JsonValue; wasString: boolean } {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
      try {
        return { value: JSON.parse(trimmed), wasString: true };
      } catch (_) {
        return { value, wasString: true };
      }
    }
    return { value, wasString: true };
  }
  if (isJsonValue(value)) {
    return { value, wasString: false };
  }
  return { value: String(value), wasString: false };
}

function buildReport(
  contracts: ToolContract[],
  calls: ObservedCall[],
  baseline: Baseline | undefined,
  options: CliOptions,
): Report {
  const contractsByName = new Map<string, ToolContract>();
  for (const contract of contracts) {
    contractsByName.set(contract.name, contract);
  }

  const findings: Finding[] = [];
  const toolStats = new Map<string, { calls: number; invalid: number; unknown: Set<string>; samples: string[] }>();
  for (const contract of contracts) {
    toolStats.set(contract.name, { calls: 0, invalid: 0, unknown: new Set<string>(), samples: [] });
  }

  for (const call of calls) {
    const contract = contractsByName.get(call.name);
    if (!contract) {
      findings.push(makeFinding(
        "UnknownToolCall",
        "high",
        `Observed a call to undeclared tool ${call.name}.`,
        call,
        "Declare the tool in the checked schema file or remove the stale runtime call.",
        { recordHash: call.recordHash, schemaHash: call.schemaHash || null },
      ));
      continue;
    }
    const stat = toolStats.get(contract.name);
    if (stat) {
      stat.calls += 1;
      pushSample(stat.samples, sha256(canonicalize(call.args)).slice(0, 16), options.maxSamplesPerTool);
    }
    if (call.argumentWasString) {
      findings.push(makeFinding(
        "StringifiedArguments",
        "low",
        `Tool ${call.name} received arguments as a JSON string instead of a structured object.`,
        call,
        "Pass structured arguments where possible so validation, logging, and policy checks do not need to parse strings.",
        { recordHash: call.recordHash },
      ));
    }
    if (call.schemaHash && call.schemaHash !== contract.schemaHash) {
      findings.push(makeFinding(
        "RuntimeSchemaHashMismatch",
        "high",
        `Trace schema hash for ${call.name} does not match the declared schema hash.`,
        call,
        "Regenerate the tool manifest or replay traces with the same schema version used by production.",
        { observedSchemaHash: call.schemaHash, declaredSchemaHash: contract.schemaHash },
      ));
    }
    const issues = validateValue(call.args, contract.schema, "$", 0);
    if (issues.length > 0 && stat) {
      stat.invalid += 1;
    }
    for (const issue of issues) {
      if (issue.code === "AdditionalProperty" && stat) {
        const field = issue.path.split(".").pop() || issue.path;
        stat.unknown.add(field);
      }
      findings.push(makeFinding(
        issue.code,
        issue.severity,
        `${call.name} arguments failed schema validation at ${issue.path}: ${issue.message}`,
        call,
        "Update the caller, update the schema, or add a migration shim before this drift reaches production.",
        { path: issue.path, recordHash: call.recordHash },
      ));
    }
  }

  findings.push(...auditCoverage(contracts, toolStats, options));
  if (baseline) {
    findings.push(...auditBaseline(contracts, baseline));
  }

  const blocking = findings.filter((finding) => options.failOn.has(finding.severity));
  const summaries: ToolSummary[] = contracts.map((contract) => {
    const stat = toolStats.get(contract.name);
    return {
      name: contract.name,
      schemaHash: contract.schemaHash,
      required: [...contract.required].sort(),
      declaredProperties: [...contract.properties.keys()].sort(),
      observedCalls: stat ? stat.calls : 0,
      invalidCalls: stat ? stat.invalid : 0,
      unknownFields: stat ? [...stat.unknown].sort() : [],
      sampledArgumentHashes: stat ? stat.samples : [],
    };
  });

  const observedToolCount = summaries.filter((summary) => summary.observedCalls > 0).length;
  const coverage = contracts.length === 0 ? 100 : Math.round((observedToolCount / contracts.length) * 10000) / 100;
  const countsBySeverity: JsonObject = {};
  for (const severity of SEVERITIES) {
    countsBySeverity[severity] = findings.filter((finding) => finding.severity === severity).length;
  }

  return {
    tool: "ToolCallSchemaDriftGuard",
    version: VERSION,
    generatedAtEpoch: Math.floor(Date.now() / 1000),
    pass: blocking.length === 0,
    summary: {
      contractCount: contracts.length,
      observedCallCount: calls.length,
      observedToolCount,
      coveragePercent: coverage,
      findingCount: findings.length,
      blockingFindingCount: blocking.length,
      countsBySeverity,
      failOn: [...options.failOn].sort(),
    },
    tools: summaries,
    findings,
  };
}

function makeFinding(
  ruleId: string,
  severity: Severity,
  message: string,
  call: ObservedCall,
  remediation: string,
  evidence: JsonObject,
): Finding {
  return {
    ruleId,
    severity,
    message,
    source: call.source,
    line: call.line,
    toolName: call.name,
    remediation,
    evidence: cleanEvidence(evidence),
  };
}

function auditCoverage(
  contracts: ToolContract[],
  stats: Map<string, { calls: number; invalid: number; unknown: Set<string>; samples: string[] }>,
  options: CliOptions,
): Finding[] {
  const findings: Finding[] = [];
  const observed = contracts.filter((contract) => (stats.get(contract.name)?.calls || 0) > 0);
  const coverage = contracts.length === 0 ? 100 : (observed.length / contracts.length) * 100;
  if (coverage < options.minCoverage) {
    findings.push({
      ruleId: "ToolCoverageBelowMinimum",
      severity: "medium",
      message: `Observed tool coverage is ${round2(coverage)}%, below the required ${options.minCoverage}%.`,
      source: "coverage",
      line: 1,
      remediation: "Feed a representative trace file or lower the threshold for partial rollouts.",
      evidence: { observedToolCount: observed.length, contractCount: contracts.length },
    });
  }
  for (const contract of contracts) {
    if ((stats.get(contract.name)?.calls || 0) === 0) {
      findings.push({
        ruleId: "DeclaredToolNotObserved",
        severity: "low",
        message: `Declared tool ${contract.name} was not observed in the provided traces.`,
        source: contract.source,
        line: 1,
        toolName: contract.name,
        remediation: "Include representative traces before treating this schema as fully exercised.",
        evidence: { schemaHash: contract.schemaHash },
      });
    }
  }
  return findings;
}

function auditBaseline(contracts: ToolContract[], baseline: Baseline): Finding[] {
  const findings: Finding[] = [];
  const current = new Map<string, ToolContract>();
  for (const contract of contracts) {
    current.set(contract.name, contract);
  }
  const baselineTools = new Map<string, BaselineTool>();
  for (const tool of baseline.tools || []) {
    if (tool && typeof tool.name === "string" && typeof tool.schemaHash === "string") {
      baselineTools.set(tool.name, tool);
    }
  }
  for (const [name, prior] of baselineTools.entries()) {
    const contract = current.get(name);
    if (!contract) {
      findings.push({
        ruleId: "BaselineToolRemoved",
        severity: "medium",
        message: `Tool ${name} existed in the baseline but is missing from the current schema.`,
        source: "baseline",
        line: 1,
        toolName: name,
        remediation: "Confirm old traces and queued jobs cannot still call this tool before removing it.",
        evidence: { baselineSchemaHash: prior.schemaHash },
      });
    } else if (contract.schemaHash !== prior.schemaHash) {
      findings.push({
        ruleId: "SchemaChangedFromBaseline",
        severity: "high",
        message: `Tool ${name} schema changed from the stored baseline.`,
        source: contract.source,
        line: 1,
        toolName: name,
        remediation: "Version the tool, migrate queued work, or accept the baseline change in review.",
        evidence: { baselineSchemaHash: prior.schemaHash, currentSchemaHash: contract.schemaHash },
      });
    }
  }
  for (const contract of contracts) {
    if (!baselineTools.has(contract.name)) {
      findings.push({
        ruleId: "ToolAddedSinceBaseline",
        severity: "medium",
        message: `Tool ${contract.name} is new relative to the baseline.`,
        source: contract.source,
        line: 1,
        toolName: contract.name,
        remediation: "Review ownership, permissions, and trace coverage before approving the new tool.",
        evidence: { currentSchemaHash: contract.schemaHash },
      });
    }
  }
  return findings;
}

function validateValue(value: any, schema: JsonSchema, path: string, depth: number): SchemaIssue[] {
  if (depth > 40) {
    return [{ path, code: "SchemaDepthLimit", message: "schema nesting is too deep to validate safely", severity: "medium" }];
  }
  const issues: SchemaIssue[] = [];
  if (!isObject(schema)) {
    return issues;
  }
  if (schema.anyOf || schema.oneOf) {
    const branches = Array.isArray(schema.anyOf) ? schema.anyOf : schema.oneOf;
    const branchResults = branches.map((branch: any) => validateValue(value, asSchema(branch) || {}, path, depth + 1));
    if (branchResults.some((result: SchemaIssue[]) => result.length === 0)) {
      return [];
    }
    return [{ path, code: "UnionMismatch", message: "value did not match any allowed schema branch", severity: "high" }];
  }
  if (Array.isArray(schema.allOf)) {
    for (const branch of schema.allOf) {
      issues.push(...validateValue(value, asSchema(branch) || {}, path, depth + 1));
    }
  }
  if (schema.const !== undefined && canonicalize(value) !== canonicalize(schema.const)) {
    issues.push({ path, code: "ConstMismatch", message: `expected constant ${preview(schema.const)}`, severity: "high" });
  }
  if (Array.isArray(schema.enum) && !schema.enum.some((entry: any) => canonicalize(entry) === canonicalize(value))) {
    issues.push({ path, code: "EnumMismatch", message: `expected one of ${schema.enum.map(preview).join(", ")}`, severity: "high" });
  }
  const allowedTypes = normalizeTypes(schema.type);
  if (allowedTypes.length > 0 && !allowedTypes.includes(jsonType(value))) {
    if (!(allowedTypes.includes("number") && jsonType(value) === "integer")) {
      issues.push({ path, code: "TypeMismatch", message: `expected ${allowedTypes.join("|")} but got ${jsonType(value)}`, severity: "high" });
      return issues;
    }
  }
  if (jsonType(value) === "object") {
    validateObject(value as JsonObject, schema, path, depth, issues);
  } else if (Array.isArray(value)) {
    validateArray(value, schema, path, depth, issues);
  } else if (typeof value === "string") {
    validateString(value, schema, path, issues);
  } else if (typeof value === "number") {
    validateNumber(value, schema, path, issues);
  }
  return issues;
}

function validateObject(value: JsonObject, schema: JsonSchema, path: string, depth: number, issues: SchemaIssue[]): void {
  const required = Array.isArray(schema.required) ? schema.required.map(String) : [];
  for (const field of required) {
    if (!(field in value)) {
      issues.push({ path: `${path}.${field}`, code: "MissingRequiredProperty", message: "required property is absent", severity: "high" });
    }
  }
  const properties = isObject(schema.properties) ? (schema.properties as { [key: string]: any }) : {};
  for (const [key, nestedValue] of Object.entries(value)) {
    if (properties[key]) {
      issues.push(...validateValue(nestedValue, asSchema(properties[key]) || {}, `${path}.${escapePath(key)}`, depth + 1));
    } else if (schema.additionalProperties === false) {
      issues.push({ path: `${path}.${escapePath(key)}`, code: "AdditionalProperty", message: "property is not declared and additionalProperties is false", severity: "medium" });
    } else if (isObject(schema.additionalProperties)) {
      issues.push(...validateValue(nestedValue, schema.additionalProperties as JsonSchema, `${path}.${escapePath(key)}`, depth + 1));
    }
  }
}

function validateArray(value: JsonValue[], schema: JsonSchema, path: string, depth: number, issues: SchemaIssue[]): void {
  if (typeof schema.minItems === "number" && value.length < schema.minItems) {
    issues.push({ path, code: "MinItems", message: `expected at least ${schema.minItems} items`, severity: "medium" });
  }
  if (typeof schema.maxItems === "number" && value.length > schema.maxItems) {
    issues.push({ path, code: "MaxItems", message: `expected at most ${schema.maxItems} items`, severity: "medium" });
  }
  if (isObject(schema.items)) {
    value.forEach((item, index) => issues.push(...validateValue(item, schema.items as JsonSchema, `${path}[${index}]`, depth + 1)));
  }
}

function validateString(value: string, schema: JsonSchema, path: string, issues: SchemaIssue[]): void {
  if (typeof schema.minLength === "number" && value.length < schema.minLength) {
    issues.push({ path, code: "MinLength", message: `expected at least ${schema.minLength} characters`, severity: "medium" });
  }
  if (typeof schema.maxLength === "number" && value.length > schema.maxLength) {
    issues.push({ path, code: "MaxLength", message: `expected at most ${schema.maxLength} characters`, severity: "medium" });
  }
  if (typeof schema.pattern === "string") {
    try {
      if (!new RegExp(schema.pattern).test(value)) {
        issues.push({ path, code: "PatternMismatch", message: `did not match pattern ${schema.pattern}`, severity: "medium" });
      }
    } catch (_) {
      issues.push({ path, code: "InvalidSchemaPattern", message: `schema pattern ${schema.pattern} is invalid`, severity: "medium" });
    }
  }
}

function validateNumber(value: number, schema: JsonSchema, path: string, issues: SchemaIssue[]): void {
  if (typeof schema.minimum === "number" && value < schema.minimum) {
    issues.push({ path, code: "Minimum", message: `expected >= ${schema.minimum}`, severity: "medium" });
  }
  if (typeof schema.maximum === "number" && value > schema.maximum) {
    issues.push({ path, code: "Maximum", message: `expected <= ${schema.maximum}`, severity: "medium" });
  }
  if (typeof schema.exclusiveMinimum === "number" && value <= schema.exclusiveMinimum) {
    issues.push({ path, code: "ExclusiveMinimum", message: `expected > ${schema.exclusiveMinimum}`, severity: "medium" });
  }
  if (typeof schema.exclusiveMaximum === "number" && value >= schema.exclusiveMaximum) {
    issues.push({ path, code: "ExclusiveMaximum", message: `expected < ${schema.exclusiveMaximum}`, severity: "medium" });
  }
  if (schema.type === "integer" && !Number.isInteger(value)) {
    issues.push({ path, code: "IntegerExpected", message: "expected an integer", severity: "high" });
  }
}

function renderReport(report: Report, format: OutputFormat): string {
  if (format === "json") {
    return JSON.stringify(report, null, 2) + "\n";
  }
  if (format === "markdown") {
    return renderMarkdown(report);
  }
  if (format === "sarif") {
    return renderSarif(report);
  }
  return JSON.stringify({ tools: report.tools }, null, 2) + "\n";
}

function renderMarkdown(report: Report): string {
  const lines: string[] = [
    "# Tool call schema drift guard",
    "",
    `Pass: ${String(report.pass)}`,
    `Contracts: ${report.summary.contractCount}`,
    `Observed calls: ${report.summary.observedCallCount}`,
    `Coverage: ${report.summary.coveragePercent}%`,
    `Findings: ${report.summary.findingCount}`,
    "",
    "## Tool coverage",
    "",
    "| Tool | Schema hash | Calls | Invalid | Unknown fields |",
    "| --- | --- | ---: | ---: | --- |",
  ];
  for (const tool of report.tools) {
    lines.push(`| ${escapeMarkdown(tool.name)} | ${tool.schemaHash} | ${tool.observedCalls} | ${tool.invalidCalls} | ${escapeMarkdown(tool.unknownFields.join(", ") || "-")} |`);
  }
  lines.push("", "## Findings", "");
  if (report.findings.length === 0) {
    lines.push("No findings under the active policy.");
  } else {
    for (const finding of report.findings) {
      lines.push(`- [${finding.severity}] ${finding.ruleId} ${finding.toolName || ""} ${finding.source}:${finding.line} - ${finding.message} Remediation: ${finding.remediation}`);
    }
  }
  return lines.join("\n") + "\n";
}

function renderSarif(report: Report): string {
  const rules = new Map<string, JsonObject>();
  const results: JsonObject[] = [];
  for (const finding of report.findings) {
    rules.set(finding.ruleId, {
      id: finding.ruleId,
      name: finding.ruleId,
      shortDescription: { text: finding.message },
      help: { text: finding.remediation },
    });
    results.push({
      ruleId: finding.ruleId,
      level: finding.severity === "critical" || finding.severity === "high" ? "error" : finding.severity === "medium" ? "warning" : "note",
      message: { text: `${finding.message} ${finding.remediation}` },
      locations: [
        {
          physicalLocation: {
            artifactLocation: { uri: finding.source },
            region: { startLine: finding.line },
          },
        },
      ],
      properties: {
        severity: finding.severity,
        toolName: finding.toolName || null,
        evidence: finding.evidence,
      },
    });
  }
  return JSON.stringify({
    version: "2.1.0",
    $schema: "https://json.schemastore.org/sarif-2.1.0.json",
    runs: [
      {
        tool: {
          driver: {
            name: "ToolCallSchemaDriftGuard",
            version: VERSION,
            informationUri: "https://github.com/kspavankrishna/VIBE-CODE",
            rules: [...rules.values()],
          },
        },
        results,
      },
    ],
  }, null, 2) + "\n";
}

function renderBaseline(contracts: ToolContract[]): Baseline {
  return {
    tool: "ToolCallSchemaDriftGuard",
    generatedAtEpoch: Math.floor(Date.now() / 1000),
    tools: contracts.map((contract) => ({
      name: contract.name,
      schemaHash: contract.schemaHash,
      required: [...contract.required].sort(),
      declaredProperties: [...contract.properties.keys()].sort(),
    })),
  };
}

function canonicalize(value: any): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalize).join(",")}]`;
  }
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalize(value[key])}`).join(",")}}`;
}

function sha256(text: string): string {
  return createHash("sha256").update(text, "utf8").digest("hex");
}

function isObject(value: any): value is JsonObject {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isJsonValue(value: any): value is JsonValue {
  if (value === null || ["string", "number", "boolean"].includes(typeof value)) {
    return true;
  }
  if (Array.isArray(value)) {
    return value.every(isJsonValue);
  }
  if (isObject(value)) {
    return Object.values(value).every(isJsonValue);
  }
  return false;
}

function firstStringFrom(value: any, keys: string[]): string | undefined {
  if (!isObject(value)) {
    return undefined;
  }
  for (const key of keys) {
    const nested = value[key];
    if (typeof nested === "string" && nested.trim()) {
      return normalizeToolName(nested);
    }
  }
  return undefined;
}

function flatten(value: any, prefix = "", out: { [key: string]: any } = {}): { [key: string]: any } {
  if (Array.isArray(value)) {
    value.slice(0, 100).forEach((item, index) => flatten(item, prefix ? `${prefix}.${index}` : String(index), out));
    return out;
  }
  if (!isObject(value)) {
    return out;
  }
  for (const [key, nested] of Object.entries(value)) {
    const dotted = prefix ? `${prefix}.${key}` : key;
    out[dotted] = nested;
    if (!(key in out)) {
      out[key] = nested;
    }
    flatten(nested, dotted, out);
  }
  return out;
}

function findDottedString(flat: { [key: string]: any }, keys: string[]): string | undefined {
  for (const key of keys) {
    const direct = flat[key];
    if (typeof direct === "string" && direct.trim()) {
      return normalizeToolName(direct);
    }
  }
  for (const [key, value] of Object.entries(flat)) {
    const leaf = key.split(".").pop() || key;
    if (keys.includes(leaf) && typeof value === "string" && value.trim()) {
      return normalizeToolName(value);
    }
  }
  return undefined;
}

function findAnyValue(flat: { [key: string]: any }, keys: string[]): any {
  for (const key of keys) {
    if (flat[key] !== undefined) {
      return flat[key];
    }
  }
  for (const [key, value] of Object.entries(flat)) {
    const leaf = key.split(".").pop() || key;
    if (keys.includes(leaf)) {
      return value;
    }
  }
  return undefined;
}

function normalizeToolName(name: string): string {
  return name.trim().replace(/^functions?\./, "").replace(/^tools?\./, "");
}

function normalizeTypes(typeValue: any): string[] {
  const raw = Array.isArray(typeValue) ? typeValue : typeValue ? [typeValue] : [];
  return raw.map(String).map((item) => item.toLowerCase());
}

function jsonType(value: any): string {
  if (value === null) {
    return "null";
  }
  if (Array.isArray(value)) {
    return "array";
  }
  if (typeof value === "number") {
    return Number.isInteger(value) ? "integer" : "number";
  }
  return typeof value;
}

function escapePath(key: string): string {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(key) ? key : JSON.stringify(key);
}

function preview(value: any): string {
  const text = typeof value === "string" ? value : canonicalize(value);
  return text.length > 80 ? `${text.slice(0, 77)}...` : text;
}

function cleanEvidence(value: JsonObject): JsonObject {
  const out: JsonObject = {};
  for (const [key, nested] of Object.entries(value)) {
    if (nested !== null && nested !== undefined && nested !== "") {
      out[key] = nested as JsonValue;
    }
  }
  return out;
}

function pushSample(samples: string[], value: string, limit: number): void {
  if (limit <= 0 || samples.includes(value)) {
    return;
  }
  if (samples.length < limit) {
    samples.push(value);
  }
}

function round2(value: number): number {
  return Math.round(value * 100) / 100;
}

function escapeMarkdown(value: string): string {
  return value.replace(/[|\\]/g, "\\$&");
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  process.exitCode = main(process.argv.slice(2));
}

// This solves the tool call schema drift problem that is quietly breaking AI agent systems, MCP servers, OpenAI Responses API tools, LangGraph workers, browser automation, eval harnesses, and DevOps runbooks in April 2026. Built because teams keep changing a tool argument, enum, required field, baseline manifest, or queued workflow while old traces and live callers still use the previous shape, then the failure shows up as a bad production action instead of a clean CI finding. Use it when you need a TypeScript schema drift guard for agent tool manifests, MCP list_tools output, JSON Schema contracts, tool call JSONL, SARIF security upload, release review, and incident evidence. The trick: it reads real trace records, normalizes the messy names and argument formats that different frameworks emit, validates the observed payloads against declared schemas, compares optional schema hashes and baselines, and turns every mismatch into a structured finding that a reviewer can act on. Drop this into any repository that ships AI tooling, research pipelines, internal automation, edge workers, or developer productivity agents, then run it before a schema update reaches main so the broken contract is visible while it is still cheap to fix.
