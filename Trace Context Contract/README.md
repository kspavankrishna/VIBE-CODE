# Trace Context Contract

Your AI agent traces look complete in the dashboard and then fall apart the moment someone needs them as incident evidence. This is a dependency free Scala validator that reads span JSONL, scores the evidence gaps that actually matter and fails CI before the bad traces reach production.

**Language:** Scala | **Lines:** 470 | **Added:** 2026-07-20

## What this solves

An agent stack is usually five or six things stitched together: an OpenTelemetry SDK, a few MCP servers, a streaming LLM client, a queue, an internal framework and whatever the platform team added last quarter. Each one emits spans. Each one spells attributes slightly differently. Nothing enforces a shared contract across them. The dashboards still render fine because a waterfall only needs a trace id, a parent and two timestamps. Everything that makes a trace useful during an incident is optional, so it quietly goes missing.

The bill arrives later. A customer reports a bad answer and you cannot say which model served it, because the span has no `gen_ai.request.model` and no provider field. Finance asks why inference spend jumped 40 percent and nobody can attribute cost per trace, because token counts were dropped at the sampler. A tool executed twice and charged twice, and you cannot prove whether it was one retry or two distinct calls, because the tool spans carry no stable call id and the retry span has no idempotency key. Worst case, someone greps the trace store and finds a raw prompt body with `Authorization: Bearer ...` sitting in plain text, and now you have a security incident inside your observability platform.

The failure mode is specific: the data is not wrong, it is absent. Absent data does not throw. No test catches it. No alert fires. You discover it during the one hour where you cannot afford to discover it, usually with an exec on the call. And by then the spans are already written, already sampled and already aged out of the hot tier.

The other half of the problem is topology. Merge exporters from two services and you get duplicate span ids inside a trace. Sample a parent away and the child points at nothing. Run two machines with drifting clocks and a child span starts before its parent, which quietly reorders your waterfall and makes the slow thing look fast. Every one of these renders as a normal looking trace.

## Why I built it

OpenTelemetry ships collectors and processors, not opinions. It will happily accept a span with no model, no tokens, no cost and a dangling parent, because the spec's job is transport, not editorial judgment. The GenAI semantic conventions exist but they are conventions, and half the ecosystem is on an older spelling or a vendor prefix. Vendor backends will chart whatever you send and invoice you for storing it. None of these tools will tell you, before you ship, that your traces cannot answer the questions you will ask them.

So the gap is a gate. Something that runs on exported spans in CI, knows the common OTel and GenAI field spellings, scores the gaps by how much operational pain they cause and either fails the build or drops a Markdown table into the pull request. It had to be one file with zero dependencies, because a tool that needs its own build setup never makes it into the pipeline.

## When to use it

- Before a release that changes instrumentation, the SDK version or the collector config
- In CI on a captured fixture trace, so a refactor that silently drops token counts fails the build
- After merging exporters from two services, to catch duplicate span ids and dangling parents
- When reconciling an inference invoice and you need to know which traces have no cost or token evidence
- Before an eval run, so the eval output has model and provider attribution attached
- During a post incident retro, to prove whether the trace could have answered the question and where to fix the instrumentation

## How it works

Input is JSONL, one span object per line, read from files or stdin. `readLines` wraps each non blank line in a `Source(path, line, text)` so every finding points back at a real file and line number. `parseSpan` then runs each line through `JsonPairs.validateObject`, a small character scanner that tracks string state, escape state and a bracket stack to confirm the record is a balanced JSON object. It reports the specific defect: unterminated string, extra closing brace, mismatched pair. That is rule TCC001, score 72, and it only becomes a real issue under `--strict`.

Field extraction deliberately does not build an AST. `JsonPairs.Scanner` walks the line, reads any quoted string followed by a colon as a key, reads the value as either a string or a bare token, and accumulates into a `LinkedHashMap[String, Vector[String]]`. Because it is flat, attributes nested under an `attributes` object land in the same map as top level fields, which is exactly what you want when four different SDKs disagree about nesting. Repeated keys accumulate rather than overwrite. On top of that, `first` resolves alias families, so `traceId`, `trace_id` and `trace.id` are the same thing, and `service.name`, `serviceName`, `service_name` and `resource.service.name` all populate `Span.service`. Timestamps go through `toNanos`, which tries a raw `Long` first and falls back to `Instant.parse` for ISO-8601.

Identity failures short circuit at parse time: missing trace id is TCC002, missing span id TCC003, missing name TCC004, and a `service.name` outside a non empty `--allow-service` set is TCC005. Everything that parses goes into `validate`, which runs per span checks then groups by trace id for topology. `aiChecks` fires when the lowercased span name contains llm, model, completion, embedding, rerank, agent or tool, or when any key starts with `gen_ai.`, `llm.` or `ai.`. It then demands model evidence (TCC030), provider evidence (TCC031), token usage (TCC032) and a cost estimate (TCC033, scored lowest at 46 because it is derivable). `retryChecks` fires on retry, replay, resume or idempot in the span name and demands one of five idempotency key spellings, scored 79.

`payloadChecks` is the security pass. For any attribute whose key contains prompt, completion, body or message it scans the value for `api_key`, `password`, `secret`, `bearer `, `authorization:` or `-----begin`, and checks whether the value also carries a redaction marker such as `[redacted]`, `***` or `sha256:`. Sensitive and not redacted is TCC040 at score 92, the highest in the file. A value over 16000 characters with no redaction marker is TCC041 at 66, on the argument that raw payloads belong in controlled evidence storage rather than your trace index.

`topologyChecks` operates on one trace at a time. It builds a span id index, finds duplicate ids from a groupBy (TCC060, score 91), flags parents that are not present in the envelope (TCC061, 73) and compares parent and child timestamps. A child starting more than `--max-clock-skew-ms` before its parent is TCC080; a child ending that far after its parent is TCC082, and spans named async, queue or callback are exempt because legitimate async work genuinely outlives its parent. The tool correlation check is the interesting one: it collects every span that looks like a tool, function or MCP call, counts how many lack a stable call id, and computes the orphan percentage. Below `--max-orphan-tool-percent` each orphan scores 57. Above it, the same finding scores 84. One missing call id is noise. Five percent missing is a correlation problem, and the score reflects that rather than the rule firing a different number of times.

Scoring rolls up simply. `Issue.severity` maps score to critical at 90, high at 80 and medium at 60. `run` takes the max score across all issues, sorts findings by descending score then file, line and rule, and sets status to fail when the max reaches `--fail-at` (default 85), warn when there are issues below that and pass when there are none. `renderJson` emits a single flat object with counts and an issues array, hand escaped through `quote` so there is no JSON library in the dependency graph. `renderMarkdown` emits a summary block and a table capped at 200 rows, with `cell` escaping pipes and collapsing whitespace. `selfTest` runs two inline fixtures, one clean AI span and one deliberately broken tool retry span, and asserts that TCC021, TCC030, TCC040, TCC050, TCC061 and TCC070 all fire.

## Usage

```bash
# single file, JSON report to stdout
scala TraceContextContract.scala trace.jsonl

# stdin, write both reports, tighter gate
cat spans.jsonl | scala TraceContextContract.scala - \
  --json-out report.json \
  --markdown-out report.md \
  --fail-at 80 \
  --strict

# enforce a contract: required attributes and an allowlist of producers
scala TraceContextContract.scala \
  --input agent.jsonl --input gateway.jsonl \
  --require-attribute deployment.environment \
  --require-attribute gen_ai.request.model \
  --allow-service agent-api --allow-service tool-runner \
  --max-clock-skew-ms 5000 \
  --max-orphan-tool-percent 1.0 \
  --json-out report.json

# verify the checker itself
scala TraceContextContract.scala --self-test

scala TraceContextContract.scala --help
```

Exit codes: 0 for pass or warn, 1 when status is fail, 2 for a bad argument. `--self-test` exits 0 or 1 on its own.

## Notes

- `JsonPairs` is a pair scanner, not a JSON parser. It flattens nesting by design, keeps numbers and booleans as raw text, and does not index array elements. A key buried inside an unrelated nested object will still be collected. That is a deliberate trade for alias tolerance across SDKs.
- Detection of AI spans, tool spans and retry spans is keyword matching on the lowercased span name plus key prefixes. A span named `agent_dashboard_render` will be treated as an AI span and asked for a model. Narrow the input or accept the false positive.
- The redaction check is substring matching on plain text. Base64, encoded or structurally obfuscated secrets will pass it. It reduces obvious leakage, it does not certify a trace as clean.
- Input is JSONL only. There is no OTLP protobuf reader, no collector integration and no network I/O of any kind. Export first, then run this.
- All input lines are read into memory before validation, and topology checks materialize a per trace id index. Fine for fixtures and captured windows, not intended for streaming a production firehose.
- `--strict` only changes whether malformed records become scored issues. Parse errors are always counted in `parse_errors` either way.
- Findings are not deduplicated. One span missing model, provider, tokens and cost produces four separate rows. The Markdown table truncates at 200 rows and 180 characters per cell; the JSON report carries everything.
- Required attribute checking has one special case: `service.name` is satisfied by any of its aliases via `Span.service`. Other required keys must match the exact spelling you pass.
