# MCP Invocation Firewall

An agent host wires a model straight into MCP servers and there is nothing between the two. One prompt injection, one API key echoed back in a tool result, one retry loop hammering the same write, and the damage is already done. This is the missing control layer: a single TypeScript file that decides allow, review or deny both before a tool runs and before its result goes back into the model loop.

**Language:** TypeScript | **Lines:** 1923 | **Added:** 2026-04-20

## What this solves

The weak point in an agent stack is usually not the model. It is the gap between the model and the tool. Teams wire OpenAI, Anthropic, the Vercel AI SDK, LangGraph or a custom host directly into MCP servers, and the MCP server does exactly what it is told. The tool schema says `path: string` so the model sends `../../.ssh/id_rsa` and the file server reads it. The tool schema says `query: string` so a poisoned document convinces the model to send `DROP TABLE`. Nobody wrote a rule saying that was not allowed, because there was no place to write one.

The second failure is quieter and worse. Tool results come back into the context window unfiltered. A CI log contains `ghp_...`, an env dump contains `sk-ant-...`, a support ticket contains a JWT. That string now lives in the conversation, gets echoed into the next completion, gets written to your trace store and shipped to whatever observability vendor you use. You did not leak a credential through your own code. You leaked it through a transcript. Whoever finds it is usually not you, and the clock on rotating that key started hours ago.

The third is cost and blast radius. Agents retry. A loop that calls the same write tool with the same arguments forty times will happily create forty tickets, forty refunds or forty emails, and none of the individual calls looks wrong. Rate limits on the HTTP client do not help, because the model is not rate limited by intent. What you actually want is a check that says: this exact invocation, from this principal, against this tool, already ran ninety seconds ago.

Without a layer like this, the answers to "who called what, with which arguments, and why was it allowed" live in nobody's system. The audit question arrives after the incident, and the honest answer is that the host never made a decision at all.

## Why I built it

Every framework ships some form of tool gating and none of it is deterministic or portable. Approval callbacks give you a yes or no boolean with no reasons, no evidence and no fingerprint. Policy layers that exist tend to be tied to one framework, one runtime or one hosted control plane, so you cannot run the same policy in a test as you run in production. Meanwhile the DLP question, redaction and secret detection on results, is treated as a logging concern rather than a policy concern, so it lands in the wrong place in the pipeline or nowhere at all.

I wanted one deterministic evaluator I could read end to end in an afternoon, run against a fixture in a unit test, and get an identical decision from every time. No database, no framework, no package tree to chase. One file, Node built-ins only, and a decision object that carries its own explanation.

## When to use it

- An agent host calling MCP servers that can write: file systems, ticketing, payments, infra APIs.
- A multi tenant product where tenant A must never reach tenant B's server or tool namespace.
- Tool results that pass through untrusted content: scraped pages, inbound email, support tickets, CI logs.
- Compliance work that needs a per invocation record with a stable fingerprint and a reason list.
- Runaway retry loops where the same tool call repeats and each repeat has a real side effect.
- Staging policy you want to unit test with fixed timestamps before it goes anywhere near production.

## How it works

The entry point is the `McpInvocationFirewall` class. You construct it with a `FirewallPolicy` and optionally a `FirewallStateAdapter`, then call `evaluateInvocation(context)` before the tool runs and `evaluateResult(context)` on the way back. Both return a `FirewallDecision` with `effect` (`allow`, `review` or `deny`), `allowed`, `reasons`, typed `findings`, a `riskScore`, a `sanitizedPayload` and a `stats` block. `explainDecision(decision)` renders that into a short human readable block for a log line.

Policy compiles once in the constructor. `normalizePolicy` rejects duplicate rule ids, drops disabled secret patterns, compiles every matcher and sorts rules by `priority` descending with id as the tiebreak, so ordering is stable across runs. Match patterns come from `compilePattern`: a bare string is exact unless it contains `*` or `?`, in which case it becomes a glob where `**` maps to `.*` and a single `*` maps to `[^/]*`. You can also pass `{exact}`, `{glob}` or `{regex}` explicitly. Field selectors like `$.files[*].path` are parsed by `parseSelector` into property, index and wildcard tokens, then resolved by `selectNodeRefs` into node references that keep a parent pointer, so a match can be rewritten in place.

Identity is a canonical hash. `stableStringifyUnknown` normalizes a value into sorted key order, turns `bigint` into a string, `Date` into ISO, typed arrays into base64 and drops `undefined`, functions and symbols, then `sha256Hex` produces the digest. That gives you three fingerprints per decision: `policyFingerprint` over the whole policy, `requestFingerprint` over identity plus arguments, and `payloadFingerprint` over the payload alone. Two semantically identical calls with different key ordering hash the same, which is exactly what duplicate detection needs.

Evaluation walks the matched rules in priority order and accumulates. Each rule can carry `constraints` (required paths, forbidden paths, byte, depth, array, key and string limits, checked against metrics from `collectPayloadMetrics`), field actions (`redact`, `truncate`, `hash`, `deny`, each optionally guarded by a `when` regex or length test), and one or more rate limits. Rate limiting is a fixed window counter per key, not a token bucket: `consumeRateLimit` resets the bucket once the window has elapsed and trips on `maxCalls`, `maxPayloadBytes` or accumulated `maxRisk`. Scope is chosen from `RateLimitScope`, which includes `principal+tool`, `server+tool` and `principal+server+tool`, so you can cap a single caller against a single dangerous tool without capping the fleet. Duplicate detection uses `recordFingerprint` with a TTL keyed on the rule id and the request fingerprint, and only runs on the `args` channel.

Secret scanning runs last, over the original payload. `DEFAULT_SECRET_PATTERNS` ships ten regexes with confidence scores: OpenAI project keys, Anthropic keys, GitHub tokens and PATs, AWS access keys, Google API keys, Slack tokens, JWTs, PEM private key headers and bearer tokens. Anything below `minConfidence` (0.75 by default) is skipped. Evidence in a finding is masked by `maskPreview`, first four and last four characters only, so the finding itself never leaks the secret. When `autoRedact` is on, `redactPaths` overwrites every hit path in the sanitized copy with `[REDACTED_SECRET]`. Rule level `secretHandling` overrides the policy default, with later matched rules winning.

Effect resolution is strict precedence: any deny wins, otherwise any review wins, otherwise an explicit allow rule wins, otherwise the policy `defaultEffect`, which is `deny` if you do not set it. Set `requireAllowRule: true` and an invocation that matches nothing is denied outright. State lives behind `FirewallStateAdapter`, and `InMemoryFirewallState` is the bundled implementation using two Maps with pruning on every touch and a seven day stale bucket cutoff. Swap in Redis or Postgres by implementing three methods.

## Usage

```ts
import {
  McpInvocationFirewall,
  InMemoryFirewallState,
  type FirewallPolicy,
} from "./McpInvocationFirewall";

const policy: FirewallPolicy = {
  name: "agent-host-prod",
  version: "2026.04.20",
  defaultEffect: "deny",
  requireAllowRule: true,
  rateLimits: [
    { id: "fleet", windowMs: 60_000, key: "global", maxCalls: 600, effect: "deny" },
  ],
  rules: [
    {
      id: "allow-read-tools",
      priority: 100,
      effect: "allow",
      match: { servers: ["docs-*"], tools: [{ glob: "read_*" }] },
      constraints: { maxPayloadBytes: 32_768, maxDepth: 8 },
    },
    {
      id: "guard-file-writes",
      priority: 200,
      effect: "review",
      risk: 3,
      match: {
        tools: ["write_file"],
        argPredicates: [{ path: "$.path", regex: "\\.\\.|^/etc/" }],
      },
      redactArgs: [
        { path: "$.content", action: "truncate", maxLength: 512 },
        { path: "$.token", action: "hash" },
      ],
      rateLimit: [
        { id: "writes", windowMs: 300_000, key: "principal+tool", maxCalls: 20 },
      ],
      constraints: { duplicateWindowMs: 90_000, duplicateEffect: "deny" },
    },
    {
      id: "scan-results",
      priority: 50,
      detectSecrets: true,
      secretHandling: { effectOnResults: "review", autoRedact: true },
      redactResult: [{ path: "$.env[*]", action: "redact", replace: "[ENV]" }],
    },
  ],
};

const firewall = new McpInvocationFirewall(policy, new InMemoryFirewallState());

const pre = firewall.evaluateInvocation({
  requestId: "req_01",
  principal: "user_42",
  tenant: "acme",
  server: "files",
  tool: "write_file",
  args: { path: "/etc/passwd", content: "..." },
});

if (!pre.allowed) {
  console.error(firewall.explainDecision(pre));
  throw new Error(pre.reasons[0]);
}

const raw = await callMcpTool(pre.sanitizedPayload);

const post = firewall.evaluateResult({
  invocation: { principal: "user_42", server: "files", tool: "write_file", args: pre.sanitizedPayload },
  result: raw,
});

return post.allowed ? post.sanitizedPayload : null;
```

## Notes

- Library only. No CLI, no `main`, no I/O, no network calls. Zero runtime dependencies beyond `node:buffer` and `node:crypto`, so it needs Node or a runtime that provides those.
- `evaluateInvocation` and `evaluateResult` are synchronous and have side effects on state: they consume rate limit budget and record duplicate fingerprints even when the decision ends up `deny`. Do not call them twice on the same request as a dry run.
- Rate limiting is a fixed window, not a sliding window or token bucket. A caller can burst across a window boundary at up to twice the nominal limit.
- `InMemoryFirewallState` is per process. In a multi replica deployment the limits and duplicate checks are per replica until you implement `FirewallStateAdapter` against shared storage.
- Secret detection is regex based on ten built in patterns. It will miss custom or high entropy credentials and can false positive on things shaped like a JWT. Tune with your own `secretPatterns` and `minConfidence`.
- Constraints and secret scanning run against the original payload, while field actions mutate a deep clone. A `redact` action will not hide a value from a constraint check in the same evaluation.
- Every decision returns `sanitizedPayload` even on deny. Forwarding it is your call, not the firewall's. There is no enforcement here, only a decision and a reason.
