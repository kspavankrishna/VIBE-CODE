#!/usr/bin/env node
"use strict";

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const TOOL = "EdgeCacheEntropyAudit";
const VERSION = "1.1.0";

const RANK = new Map([
  ["info", 0],
  ["low", 1],
  ["medium", 2],
  ["high", 3],
  ["critical", 4],
]);

const DEFAULTS = Object.freeze({
  input: "-",
  format: "markdown",
  failOn: "high",
  minSeverity: "low",
  maxRows: 200000,
  maxKeys: 30000,
  service: "edge",
  routeField: "",
  urlField: "",
  keyField: "",
  statusField: "",
  methodField: "",
  varyField: "",
  userField: "",
  explain: false,
  selfTest: false,
});

const FIELDS = Object.freeze({
  url: ["url", "request_url", "requestUrl", "uri", "path", "pathname", "http.url", "http.target", "request.path"],
  route: ["route", "route_id", "routeId", "handler", "endpoint", "matched_route", "http.route", "request.route"],
  key: ["cache_key", "cacheKey", "edge_cache_key", "x_cache_key", "cdn.cache_key", "cloudflare.cache_key", "vercel.cache_key"],
  status: ["cache_status", "cacheStatus", "x_cache", "x-cache", "cf_cache_status", "cf-cache-status", "x-vercel-cache", "edge.cache_status"],
  method: ["method", "request_method", "http.method", "request.method"],
  vary: ["vary", "response.vary", "headers.vary", "response_headers.vary"],
  user: ["user", "user_id", "userId", "tenant_id", "tenantId", "account_id", "accountId", "org_id", "customer_id"],
});

const SENSITIVE_PARAM = [
  /(^|[_-])(token|jwt|bearer|secret|signature|sig|session|sid|auth|password|passwd|pwd|credential|key)($|[_-])/i,
  /(^|[_-])(email|phone|mobile|ssn|aadhaar|pan|passport|dob)($|[_-])/i,
  /(^|[_-])(user|uid|userid|account|tenant|org|customer|viewer|device)($|[_-])/i,
];

const TRACKING_PARAM = [
  /^utm_/i,
  /^(fbclid|gclid|msclkid|yclid|mc_cid|mc_eid|igshid|twclid|li_fat_id)$/i,
  /^(ref|referrer|source|campaign|affiliate|irclickid)$/i,
];

const NOISY_VARY = new Set([
  "authorization",
  "cookie",
  "user-agent",
  "accept-language",
  "x-user-id",
  "x-account-id",
  "x-tenant-id",
  "x-session-id",
  "x-device-id",
]);

const HIT_WORDS = new Set(["hit", "fresh", "revalidated", "updating"]);
const MISS_WORDS = new Set(["miss", "bypass", "expired", "stale", "dynamic", "uncacheable", "none"]);

function help() {
  return `${TOOL} ${VERSION}

Audit CDN, edge compute, and AI application logs for cache key entropy, private
data in shared cache surfaces, unsafe Vary headers, low hit-rate routes, and
tracking parameter noise.

Usage:
  node EdgeCacheEntropyAudit.js --input edge.jsonl --format markdown
  cat edge.csv | node EdgeCacheEntropyAudit.js --format sarif --fail-on critical
  node EdgeCacheEntropyAudit.js --self-test

Input can be JSON, JSONL, or CSV. Common URL, route, cache key, cache status,
method, Vary, header, and user fields are discovered automatically.

Options:
  --input PATH             Input file or - for stdin. Default: -
  --format FORMAT          markdown, json, sarif, or csv. Default: markdown
  --fail-on SEVERITY       Exit 2 at this severity: info, low, medium, high,
                           critical, never. Default: high
  --min-severity LEVEL     Hide lower findings. Default: low
  --max-rows N             Bound CI memory. Default: 200000
  --max-keys N             Exact per-route key cap. Default: 30000
  --service NAME           Report label. Default: edge
  --route-field FIELD      Override route field.
  --url-field FIELD        Override URL/path field.
  --key-field FIELD        Override cache key field.
  --status-field FIELD     Override cache status field.
  --method-field FIELD     Override HTTP method field.
  --vary-field FIELD       Override Vary field.
  --user-field FIELD       Override user/tenant field.
  --explain                Add remediation notes to Markdown.
  --self-test              Run built-in tests.
`;
}

function parseArgs(argv) {
  const o = { ...DEFAULTS };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = () => {
      if (i + 1 >= argv.length) throw new Error(`${a} requires a value`);
      i += 1;
      return argv[i];
    };
    if (a === "--input") o.input = next();
    else if (a === "--format") o.format = next().toLowerCase();
    else if (a === "--fail-on") o.failOn = next().toLowerCase();
    else if (a === "--min-severity") o.minSeverity = next().toLowerCase();
    else if (a === "--max-rows") o.maxRows = positive(next(), a);
    else if (a === "--max-keys") o.maxKeys = positive(next(), a);
    else if (a === "--service") o.service = next();
    else if (a === "--route-field") o.routeField = next();
    else if (a === "--url-field") o.urlField = next();
    else if (a === "--key-field") o.keyField = next();
    else if (a === "--status-field") o.statusField = next();
    else if (a === "--method-field") o.methodField = next();
    else if (a === "--vary-field") o.varyField = next();
    else if (a === "--user-field") o.userField = next();
    else if (a === "--explain") o.explain = true;
    else if (a === "--self-test") o.selfTest = true;
    else if (a === "-h" || a === "--help") {
      process.stdout.write(help());
      process.exit(0);
    } else throw new Error(`unknown argument: ${a}`);
  }
  if (!["markdown", "json", "sarif", "csv"].includes(o.format)) throw new Error(`unknown --format: ${o.format}`);
  if (o.failOn !== "never" && !RANK.has(o.failOn)) throw new Error(`unknown --fail-on: ${o.failOn}`);
  if (!RANK.has(o.minSeverity)) throw new Error(`unknown --min-severity: ${o.minSeverity}`);
  if (!o.service.trim()) throw new Error("--service cannot be empty");
  return o;
}

function positive(value, name) {
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n) || n < 1) throw new Error(`${name} must be a positive integer`);
  return n;
}

function readInput(file) {
  return file === "-" ? fs.readFileSync(0, "utf8") : fs.readFileSync(file, "utf8");
}

function parseRecords(text, source) {
  const body = text.replace(/^\uFEFF/, "").trim();
  if (!body) throw new Error(`no records found in ${source}`);
  const whole = parseJson(body);
  if (whole.ok) return recordsFromJson(whole.value, source);
  const lines = parseJsonl(body);
  if (lines.ok) return lines.value;
  const csv = parseCsv(body);
  if (csv.length) return csv;
  throw new Error(`could not parse ${source} as JSON, JSONL, or CSV`);
}

function parseJson(text) {
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (error) {
    return { ok: false, error };
  }
}

function recordsFromJson(value, source) {
  if (Array.isArray(value)) return value.filter(isObject);
  if (isObject(value)) {
    for (const key of ["rows", "events", "records", "logs", "requests", "data"]) {
      if (Array.isArray(value[key])) return value[key].filter(isObject);
    }
    return [value];
  }
  throw new Error(`JSON in ${source} did not contain object records`);
}

function parseJsonl(text) {
  const out = [];
  let seen = 0;
  const lines = text.split(/\r?\n/);
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line || line.startsWith("#")) continue;
    try {
      const value = JSON.parse(line);
      seen += 1;
      if (isObject(value)) out.push(value);
    } catch (error) {
      return { ok: false, error: new Error(`invalid JSONL at line ${i + 1}: ${error.message}`) };
    }
  }
  return { ok: seen > 0, value: out };
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/).filter((line) => line.trim());
  if (lines.length < 2) return [];
  const headers = csvLine(lines[0]).map((x) => x.trim());
  if (headers.length < 2 || headers.some((x) => !x)) return [];
  return lines.slice(1).map((line) => {
    const cells = csvLine(line);
    const row = {};
    headers.forEach((name, i) => { row[name] = cells[i] ?? ""; });
    return row;
  });
}

function csvLine(line) {
  const cells = [];
  let cell = "";
  let quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const c = line[i];
    if (c === "\"") {
      if (quoted && line[i + 1] === "\"") {
        cell += "\"";
        i += 1;
      } else quoted = !quoted;
    } else if (c === "," && !quoted) {
      cells.push(cell);
      cell = "";
    } else cell += c;
  }
  cells.push(cell);
  return cells;
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function field(row, override, candidates) {
  if (override) return stringify(dig(row, override));
  for (const name of candidates) {
    const value = dig(row, name);
    if (value != null && String(value).trim() !== "") return stringify(value);
  }
  return "";
}

function dig(row, name) {
  if (Object.prototype.hasOwnProperty.call(row, name)) return row[name];
  let cur = row;
  for (const part of name.split(".")) {
    if (!isObject(cur) || !Object.prototype.hasOwnProperty.call(cur, part)) return undefined;
    cur = cur[part];
  }
  return cur;
}

function stringify(value) {
  if (value == null) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

function header(row, name) {
  const wanted = name.toLowerCase();
  for (const source of [row.headers, row.request_headers, row.requestHeaders, row.response_headers, row.responseHeaders]) {
    if (!isObject(source)) continue;
    for (const [key, value] of Object.entries(source)) {
      if (key.toLowerCase() === wanted) return stringify(value);
    }
  }
  for (const [key, value] of Object.entries(row)) {
    const lower = key.toLowerCase();
    if (lower === wanted || lower === `header_${wanted}` || lower === `headers.${wanted}`) return stringify(value);
  }
  return "";
}

function asUrl(raw) {
  const value = String(raw || "").trim();
  if (!value) return null;
  try {
    return new URL(value, "https://edge-cache-audit.local");
  } catch {
    return null;
  }
}

function cleanPath(value) {
  let p = String(value || "/").split("?")[0].split("#")[0].trim();
  if (!p.startsWith("/")) p = `/${p}`;
  return p.replace(/\/{2,}/g, "/") || "/";
}

function inferRoute(urlValue) {
  const parsed = asUrl(urlValue);
  const parts = cleanPath(parsed ? parsed.pathname : urlValue).split("/").filter(Boolean);
  if (!parts.length) return "/";
  return `/${parts.slice(0, 10).map((part) => {
    const decoded = safeDecode(part);
    if (/^[0-9]{6,}$/.test(decoded)) return ":id";
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(decoded)) return ":id";
    if (/^[0-9a-f]{16,}$/i.test(decoded)) return ":hex";
    if (decoded.length > 32 && entropy(decoded) > 3.4) return ":token";
    return decoded.replace(/[^A-Za-z0-9._~:-]/g, "_").slice(0, 64);
  }).join("/")}`;
}

function safeDecode(value) {
  try {
    return decodeURIComponent(String(value).replace(/\+/g, " "));
  } catch {
    return String(value);
  }
}

function splitVary(value) {
  return String(value || "").split(",").map((x) => x.trim().toLowerCase()).filter(Boolean);
}

function cacheStatus(value) {
  const s = String(value || "").trim().toLowerCase();
  if (HIT_WORDS.has(s) || s.includes("hit")) return "hit";
  if (MISS_WORDS.has(s) || s.includes("miss") || s.includes("bypass") || s.includes("dynamic")) return "miss";
  return "unknown";
}

function queryParams(urlValue) {
  const parsed = asUrl(urlValue);
  if (!parsed) return [];
  return [...parsed.searchParams.entries()].map(([name, value]) => ({ name, value }));
}

function fallbackKey(method, urlValue, vary, row) {
  const parsed = asUrl(urlValue);
  const pathname = parsed ? cleanPath(parsed.pathname) : cleanPath(urlValue);
  const query = parsed ? [...parsed.searchParams.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join("&") : "";
  const varyBits = vary.map((h) => `${h}:${header(row, h).slice(0, 120)}`).filter((x) => !x.endsWith(":")).join("|");
  return `${method} ${pathname}${query ? `?${query}` : ""}${varyBits ? ` vary=${varyBits}` : ""}`;
}

function summarize(row, options, number) {
  const url = field(row, options.urlField, FIELDS.url);
  const route = field(row, options.routeField, FIELDS.route).trim() || inferRoute(url);
  const method = (field(row, options.methodField, FIELDS.method) || "GET").trim().toUpperCase();
  const vary = splitVary(field(row, options.varyField, FIELDS.vary) || header(row, "vary"));
  const key = field(row, options.keyField, FIELDS.key).trim() || fallbackKey(method, url, vary, row);
  return {
    number,
    url,
    route,
    method,
    vary,
    key,
    status: cacheStatus(field(row, options.statusField, FIELDS.status)),
    user: field(row, options.userField, FIELDS.user),
    params: queryParams(url),
    auth: Boolean(header(row, "authorization")),
    cookie: Boolean(header(row, "cookie")),
  };
}

class Counter {
  constructor(limit) {
    this.limit = limit;
    this.map = new Map();
    this.overflow = 0;
  }

  add(value) {
    const key = String(value);
    if (this.map.has(key)) this.map.set(key, this.map.get(key) + 1);
    else if (this.map.size < this.limit) this.map.set(key, 1);
    else this.overflow += 1;
  }

  get unique() {
    return this.map.size + this.overflow;
  }

  top(n) {
    return [...this.map.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, n)
      .map(([value, count]) => ({ value, count }));
  }
}

function newStats(route, options) {
  return {
    route,
    requests: 0,
    methods: new Set(),
    keys: new Counter(options.maxKeys),
    users: new Counter(10000),
    sensitive: new Map(),
    tracking: new Map(),
    vary: new Map(),
    entropySamples: [],
    hits: 0,
    misses: 0,
    unknown: 0,
    authRows: 0,
    cookieRows: 0,
    longKeys: 0,
    examples: [],
  };
}

function addRecord(stats, rec) {
  stats.requests += 1;
  stats.methods.add(rec.method);
  stats.keys.add(rec.key);
  if (rec.user) stats.users.add(rec.user);
  if (rec.status === "hit") stats.hits += 1;
  else if (rec.status === "miss") stats.misses += 1;
  else stats.unknown += 1;
  if (rec.auth) stats.authRows += 1;
  if (rec.cookie) stats.cookieRows += 1;
  if (rec.key.length > 512) stats.longKeys += 1;
  for (const h of rec.vary) bump(stats.vary, h);
  for (const param of rec.params) {
    if (isSensitiveParam(param.name, param.value)) bump(stats.sensitive, param.name);
    if (isTrackingParam(param.name)) bump(stats.tracking, param.name);
    if (highEntropy(param.value) && stats.entropySamples.length < 8) {
      stats.entropySamples.push({ row: rec.number, name: param.name, sample: redact(param.value) });
    }
  }
  if (stats.examples.length < 3) stats.examples.push({ row: rec.number, url: rec.url || rec.route, key: redactKey(rec.key) });
}

function finishStats(stats) {
  const observed = stats.hits + stats.misses;
  return {
    route: stats.route,
    requests: stats.requests,
    methods: [...stats.methods].sort(),
    uniqueKeys: stats.keys.unique,
    uniqueUsers: stats.users.unique,
    keyRatio: stats.requests ? stats.keys.unique / stats.requests : 0,
    userRatio: stats.requests ? stats.users.unique / stats.requests : 0,
    hits: stats.hits,
    misses: stats.misses,
    unknown: stats.unknown,
    hitRate: observed ? stats.hits / observed : null,
    sensitive: sortedMap(stats.sensitive),
    tracking: sortedMap(stats.tracking),
    vary: sortedMap(stats.vary),
    entropySamples: stats.entropySamples,
    authRows: stats.authRows,
    cookieRows: stats.cookieRows,
    longKeys: stats.longKeys,
    topKeys: stats.keys.top(3).map((x) => ({ value: redactKey(x.value), count: x.count })),
    examples: stats.examples,
  };
}

function analyze(rows, options) {
  const warnings = [];
  const routes = new Map();
  const limit = Math.min(rows.length, options.maxRows);
  if (rows.length > limit) warnings.push(`input contained ${rows.length} rows; analyzed first ${limit}`);
  for (let i = 0; i < limit; i += 1) {
    const rec = summarize(rows[i], options, i + 1);
    if (!routes.has(rec.route)) routes.set(rec.route, newStats(rec.route, options));
    addRecord(routes.get(rec.route), rec);
  }
  const routeList = [...routes.values()].map(finishStats)
    .sort((a, b) => b.requests - a.requests || a.route.localeCompare(b.route));
  const min = RANK.get(options.minSeverity);
  const findings = routeList.flatMap(routeFindings)
    .filter((f) => RANK.get(f.severity) >= min)
    .sort((a, b) => RANK.get(b.severity) - RANK.get(a.severity) || a.route.localeCompare(b.route) || a.rule.localeCompare(b.rule));
  const maxSeverity = findings.reduce((max, f) => RANK.get(f.severity) > RANK.get(max) ? f.severity : max, "info");
  return {
    tool: TOOL,
    version: VERSION,
    generatedAt: new Date().toISOString(),
    service: options.service,
    analyzedRows: limit,
    routeCount: routeList.length,
    maxSeverity,
    status: shouldFail(maxSeverity, options.failOn) ? "fail" : "pass",
    warnings,
    findings,
    routes: routeList,
  };
}

function routeFindings(route) {
  const out = [];
  const varyNames = route.vary.map((x) => x.name);
  const privateVary = varyNames.filter((h) => h === "authorization" || h === "cookie");
  const noisyVary = varyNames.filter((h) => NOISY_VARY.has(h));

  if (route.sensitive.length) out.push(issue("critical", "SENSITIVE_QUERY_IN_CACHE_SURFACE", route,
    `Sensitive query parameters appear: ${route.sensitive.map((x) => x.name).join(", ")}.`,
    "Move secrets, user identifiers, and personal data out of URLs before they hit cache keys or logs."));
  if (privateVary.length) out.push(issue("critical", "PRIVATE_HEADER_IN_VARY", route,
    `Response Vary includes private headers: ${privateVary.join(", ")}.`,
    "Do not cache shared responses by raw Cookie or Authorization; split public and private handlers."));
  if (route.authRows && route.hits) out.push(issue("high", "AUTHENTICATED_CACHE_HITS", route,
    `${route.hits} cache hits were observed while Authorization headers were present.`,
    "Prove the response is public, or bypass shared cache for authenticated requests."));
  if (route.cookieRows && route.hits) out.push(issue("high", "COOKIE_CACHE_HITS", route,
    `${route.hits} cache hits were observed while Cookie headers were present.`,
    "Strip irrelevant cookies at the edge, or mark personalized responses private."));
  if (route.requests >= 20 && route.keyRatio >= 0.75) out.push(issue("high", "HIGH_CACHE_KEY_CARDINALITY", route,
    `${pct(route.keyRatio)} of requests produced unique cache keys (${route.uniqueKeys}/${route.requests}).`,
    "Canonicalize query params, bucket personalization, and remove request-only noise from cache keys."));
  else if (route.requests >= 20 && route.keyRatio >= 0.45) out.push(issue("medium", "ELEVATED_CACHE_KEY_CARDINALITY", route,
    `${pct(route.keyRatio)} of requests produced unique cache keys (${route.uniqueKeys}/${route.requests}).`,
    "Check whether every key component changes the rendered response."));
  if (route.hitRate != null && route.requests >= 20 && route.misses >= 15 && route.hitRate < 0.15) out.push(issue("high", "EDGE_CACHE_NOT_PAYING_RENT", route,
    `Hit rate is ${pct(route.hitRate)} across ${route.hits + route.misses} observations.`,
    "Either make this route deliberately dynamic or fix key, TTL, Vary, and Cache-Control policy."));
  if (noisyVary.length) out.push(issue(noisyVary.includes("user-agent") ? "medium" : "high", "HIGH_CARDINALITY_VARY_HEADER", route,
    `Vary uses high-cardinality headers: ${noisyVary.join(", ")}.`,
    "Replace raw header variance with small buckets such as device class, locale family, or auth state."));
  if (route.tracking.length && route.keyRatio >= 0.25) out.push(issue("medium", "TRACKING_QUERY_NOISE", route,
    `Tracking parameters are present: ${route.tracking.map((x) => x.name).join(", ")}.`,
    "Drop analytics parameters from cache keys before origin routing."));
  if (route.entropySamples.length) out.push(issue("medium", "HIGH_ENTROPY_QUERY_VALUE", route,
    `High-entropy query values were observed, for example ${route.entropySamples[0].name}=${route.entropySamples[0].sample}.`,
    "Treat high-entropy values as tokens until proven otherwise, then keep them out of shared cache keys."));
  if (route.longKeys) out.push(issue("low", "LONG_CACHE_KEYS", route,
    `${route.longKeys} rows had cache keys longer than 512 characters.`,
    "Hash canonical key material for storage, but keep normalized debug fields readable."));
  if (route.unknown === route.requests) out.push(issue("low", "CACHE_STATUS_MISSING", route,
    "No usable cache status field was found.",
    "Log CDN cache status so misses, hits, and private bypasses can be separated."));
  return out;
}

function issue(severity, rule, route, message, remediation) {
  return { severity, rule, route: route.route, message, remediation };
}

function isSensitiveParam(name, value) {
  const n = String(name || "");
  return SENSITIVE_PARAM.some((rx) => rx.test(n)) || email(value) || jwt(value);
}

function isTrackingParam(name) {
  return TRACKING_PARAM.some((rx) => rx.test(String(name || "")));
}

function email(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || ""));
}

function jwt(value) {
  return /^[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}$/.test(String(value || ""));
}

function highEntropy(value) {
  const text = String(value || "");
  if (text.length < 20) return false;
  if (jwt(text)) return true;
  const compact = text.replace(/[-_.~]/g, "");
  return compact.length >= 24 && entropy(compact) >= 3.7;
}

function entropy(value) {
  const text = String(value || "");
  if (!text) return 0;
  const counts = new Map();
  for (const char of text) counts.set(char, (counts.get(char) || 0) + 1);
  let score = 0;
  for (const count of counts.values()) {
    const p = count / text.length;
    score -= p * Math.log2(p);
  }
  return score;
}

function bump(map, key) {
  map.set(key, (map.get(key) || 0) + 1);
}

function sortedMap(map) {
  return [...map.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([name, count]) => ({ name, count }));
}

function redact(value) {
  const text = String(value || "");
  if (email(text)) {
    const [name, domain] = text.split("@");
    return `${name.slice(0, 2)}...@${domain}`;
  }
  if (text.length <= 8) return text;
  return `${text.slice(0, 4)}...${text.slice(-4)}`;
}

function redactKey(value) {
  let text = String(value || "");
  text = text.replace(/([?&](?:token|jwt|secret|session|sid|signature|sig|password|email)=)[^&\s]+/gi, "$1<redacted>");
  text = text.replace(/(authorization:)([^|\s]+)/gi, "$1<redacted>");
  text = text.replace(/(cookie:)([^|\s]+)/gi, "$1<redacted>");
  return text.length > 180 ? `${text.slice(0, 177)}...` : text;
}

function pct(value) {
  return value == null || Number.isNaN(value) ? "n/a" : `${(value * 100).toFixed(1)}%`;
}

function shouldFail(max, failOn) {
  return failOn !== "never" && RANK.get(max) >= RANK.get(failOn);
}

function md(value) {
  return String(value ?? "").replace(/\|/g, "\\|").replace(/\r?\n/g, " ");
}

function csv(value) {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function renderMarkdown(report, options) {
  const lines = [
    `# ${TOOL} Report`,
    "",
    `- Service: ${report.service}`,
    `- Generated at: ${report.generatedAt}`,
    `- Analyzed rows: ${report.analyzedRows}`,
    `- Routes: ${report.routeCount}`,
    `- Max severity: ${report.maxSeverity}`,
    `- Gate status: ${report.status}`,
    "",
  ];
  if (report.warnings.length) {
    lines.push("## Warnings", "");
    report.warnings.forEach((warning) => lines.push(`- ${md(warning)}`));
    lines.push("");
  }
  lines.push("## Findings", "");
  if (!report.findings.length) lines.push("No findings met the configured severity threshold.");
  else {
    lines.push("| Severity | Rule | Route | Message | Remediation |");
    lines.push("| --- | --- | --- | --- | --- |");
    report.findings.forEach((f) => {
      lines.push(`| ${f.severity} | ${f.rule} | \`${md(f.route)}\` | ${md(f.message)} | ${md(f.remediation)} |`);
    });
  }
  lines.push("", "## Route Metrics", "");
  lines.push("| Route | Requests | Unique keys | Key ratio | Hit rate | Sensitive params | Vary headers |");
  lines.push("| --- | ---: | ---: | ---: | ---: | --- | --- |");
  report.routes.slice(0, 80).forEach((r) => {
    lines.push(`| \`${md(r.route)}\` | ${r.requests} | ${r.uniqueKeys} | ${pct(r.keyRatio)} | ${pct(r.hitRate)} | ${md(r.sensitive.map((x) => x.name).join(", ")) || "-"} | ${md(r.vary.map((x) => x.name).join(", ")) || "-"} |`);
  });
  if (options.explain) {
    lines.push("", "## How To Read This", "");
    lines.push("- High key ratio means the edge cache is acting like a per-request store instead of shared infrastructure.");
    lines.push("- Sensitive query parameters are incidents because URLs leak through logs, browser history, analytics, and cache keys.");
    lines.push("- Raw Cookie, Authorization, or User-Agent variance should normally become a small explicit bucket or a private bypass.");
    lines.push("- Low hit rate with stable keys points at TTL, Cache-Control, revalidation, or origin status behavior.");
  }
  return `${lines.join("\n")}\n`;
}

function renderJson(report) {
  return `${JSON.stringify(report, null, 2)}\n`;
}

function renderCsv(report) {
  const rows = ["severity,rule,route,message,remediation"];
  report.findings.forEach((f) => rows.push([f.severity, f.rule, f.route, f.message, f.remediation].map(csv).join(",")));
  return `${rows.join("\n")}\n`;
}

function renderSarif(report) {
  const rules = new Map();
  report.findings.forEach((f) => {
    if (!rules.has(f.rule)) {
      rules.set(f.rule, { id: f.rule, shortDescription: { text: f.rule }, fullDescription: { text: f.remediation } });
    }
  });
  return `${JSON.stringify({
    version: "2.1.0",
    $schema: "https://json.schemastore.org/sarif-2.1.0.json",
    runs: [{
      tool: { driver: { name: TOOL, version: VERSION, informationUri: "https://github.com/kspavankrishna/VIBE-CODE", rules: [...rules.values()] } },
      results: report.findings.map((f) => ({
        ruleId: f.rule,
        level: f.severity === "critical" || f.severity === "high" ? "error" : f.severity === "medium" ? "warning" : "note",
        message: { text: `${f.route}: ${f.message} ${f.remediation}` },
        locations: [{ physicalLocation: { artifactLocation: { uri: `${report.service}:${f.route}` } } }],
      })),
    }],
  }, null, 2)}\n`;
}

function render(report, options) {
  if (options.format === "markdown") return renderMarkdown(report, options);
  if (options.format === "json") return renderJson(report);
  if (options.format === "sarif") return renderSarif(report);
  if (options.format === "csv") return renderCsv(report);
  throw new Error(`unsupported format: ${options.format}`);
}

function selfTest() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "edge-cache-entropy-audit-"));
  const file = path.join(dir, "edge.jsonl");
  const rows = [];
  for (let i = 0; i < 32; i += 1) {
    rows.push({
      url: `/api/search?q=carbon&email=user${i}@example.com&utm_source=newsletter&nonce=${"abCD12xyZ".repeat(4)}${i}`,
      route: "/api/search",
      method: "GET",
      cache_status: i % 8 === 0 ? "HIT" : "MISS",
      vary: "Accept-Encoding, User-Agent",
      headers: { authorization: "Bearer test", "user-agent": `Browser/${i}` },
      cache_key: `/api/search?q=carbon&email=user${i}@example.com&utm_source=newsletter&nonce=${"abCD12xyZ".repeat(4)}${i}`,
    });
  }
  rows.push({ url: "/assets/app.js?v=42", route: "/assets/app.js", cache_status: "HIT", cache_key: "/assets/app.js?v=42" });
  fs.writeFileSync(file, rows.map((row) => JSON.stringify(row)).join("\n"), "utf8");
  const report = analyze(parseRecords(fs.readFileSync(file, "utf8"), file), { ...DEFAULTS, input: file, failOn: "never" });
  expect(report.findings.some((f) => f.rule === "SENSITIVE_QUERY_IN_CACHE_SURFACE"), "expected sensitive query finding");
  expect(report.findings.some((f) => f.rule === "HIGH_CACHE_KEY_CARDINALITY"), "expected high cardinality finding");
  expect(report.findings.some((f) => f.rule === "HIGH_CARDINALITY_VARY_HEADER"), "expected vary finding");
  expect(report.findings.some((f) => f.rule === "TRACKING_QUERY_NOISE"), "expected tracking finding");
  JSON.parse(renderJson(report));
  JSON.parse(renderSarif(report));
  const child = spawnSync(process.execPath, [__filename, "--input", file, "--format", "json", "--fail-on", "critical"], { encoding: "utf8" });
  expect(child.status === 2, `expected fail-on critical exit 2, got ${child.status}: ${child.stderr}`);
  JSON.parse(child.stdout);
  fs.rmSync(dir, { recursive: true, force: true });
  process.stdout.write(`${TOOL} self-test passed\n`);
}

function expect(condition, message) {
  if (!condition) throw new Error(message);
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.selfTest) {
    selfTest();
    return;
  }
  const rows = parseRecords(readInput(options.input), options.input);
  if (!rows.length) throw new Error("input parsed, but no object records were found");
  const report = analyze(rows, options);
  process.stdout.write(render(report, options));
  if (shouldFail(report.maxSeverity, options.failOn)) process.exitCode = 2;
}

if (process.argv[1] && path.resolve(process.argv[1]) === __filename) {
  try {
    main();
  } catch (error) {
    process.stderr.write(`${TOOL}: ${error.message}\n`);
    process.exitCode = 1;
  }
}

/*
This solves the April 2026 problem where edge compute, CDN caching, AI application routes, serverless APIs, and streaming web products quietly lose most of their value because the cache key contains per-user noise, tracking parameters, cookies, Authorization headers, long prompt identifiers, or raw Vary headers. Built because Pavan wanted a single JavaScript file a platform engineer can drop into GitHub Actions, Vercel, Cloudflare, Fastly, Netlify, Kubernetes ingress, API gateway, Next.js, Remix, Astro, worker, IoT dashboard, carbon credit marketplace, research portal, or AI agent backend logs and immediately see which routes are wasting origin capacity or risking private data in shared cache. Use it when a team is shipping model response caching, RAG search pages, edge rendered dashboards, smart device APIs, data pipeline explorers, or developer productivity tools and needs proof that cache behavior is safe before traffic grows. The trick: it accepts JSON, JSONL, and CSV without dependencies, discovers common log field names, normalizes routes, measures cache key entropy, flags sensitive query parameters, spots dangerous Vary headers, redacts examples, emits Markdown, JSON, CSV, or SARIF, and exits with deterministic CI status so the result can block a risky release. Drop this into repos where people search for edge cache audit, CDN cache key cardinality, JavaScript DevOps cache scanner, Vercel cache debugging, Cloudflare Worker cache safety, AI response cache governance, prompt cache PII detection, cache poisoning prevention, high cardinality Vary header audit, serverless cache hit rate analysis, web performance CI gate, production edge compute observability, and senior-level infrastructure tooling that is practical enough to fork instead of rewrite.
*/
