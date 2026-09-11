# Tool Call Schema Drift Guard

Your agent's tool manifest says one thing and your production callers send another. This TypeScript CLI reads the declared JSON Schema for every tool, replays real tool call traces against it and turns every mismatch into a structured finding with a severity, a source line and a remediation.

**Language:** TypeScript | **Lines:** 1146 | **Added:** 2026-08-05

## What this solves

Tool call schema drift is quietly breaking AI agent systems, MCP servers, OpenAI Responses API tools, LangGraph workers, browser automation, eval harnesses and DevOps runbooks. Someone renames an argument from `user_id` to `userId`. Someone adds a value to an enum, or flips a field from optional to required, or tightens `additionalProperties` to `false`. The schema file changes in one PR. The callers, the queued jobs, the cached prompts and the replayed eval traces do not. Nothing fails at compile time because a tool argument object is just JSON crossing a process boundary.

What you get instead is a bad production action. The model emits the old field, the server silently ignores it, and a refund runs with a null amount. Or the arguments arrive as a JSON string rather than a structured object, your policy layer never parses it, and an unchecked payload goes straight through to a write. Or a tool is deleted from the manifest while a worker queue still holds calls to it, so those jobs die on an undeclared tool name hours after the deploy. The person who notices is a customer, or the on-call engineer reading a log line at 2am, not the reviewer who could have caught it in thirty seconds.

The cost is concentrated in the gap between the change and the detection. Schema drift is cheap to fix in review and expensive to fix in an incident, because by then you are reconstructing which calls used which shape from raw traces. This tool moves the detection left. Point it at the tool manifest and a trace file, and it tells you which declared tools were never exercised, which observed calls hit a tool that does not exist, which arguments failed validation and exactly where, and whether the schema hash differs from a stored baseline or from the hash production recorded at call time.

It also gives you the artifact you need afterwards. The JSON report is incident evidence: schema hashes, per tool call counts, invalid counts, unknown field names and deduplicated argument hashes. The SARIF output uploads to GitHub code scanning so drift lands in the security tab next to everything else.

## Why I built it

Generic JSON Schema validators validate one payload against one schema. They do not know what a tool call trace looks like, and every framework emits a different one. OpenAI nests the name under `function.name` and ships arguments as a JSON string. MCP servers use `input_schema`. LangGraph and OpenTelemetry style exporters bury calls inside `spans`, `events` or `steps`. So the actual work is not validation, it is normalizing the mess before validation, then reporting the result in a form CI can gate on.

The other gap is that nothing off the shelf answers the version question. Schema drift is a diff problem, not a single point in time problem. I wanted a baseline file I could commit, a stable schema hash per tool, and a guard that fails the build when a tool's contract changes without review. Dependency free was a hard requirement: this has to run in locked down CI, on an MCP host and on an incident review machine without an npm install first.

## When to use it

- A PR edits `tools.json` or your MCP `list_tools` output and you want CI to block the merge if the contract changed without a baseline update.
- You are about to delete or rename a tool and need to know whether queued jobs and last week's traces still call it.
- An eval suite started failing and you suspect the recorded traces were captured against an older schema version.
- Post incident, you have a JSONL trace dump and need to prove which calls carried arguments the schema never allowed.
- You want GitHub code scanning to surface agent tooling drift alongside your other security findings.
- You are onboarding a third party MCP server and want to see which of its declared tools your traffic actually exercises.

## How it works

The entry point is `main(argv)`, guarded by an `import.meta.url` check so the file works as both an ESM module and a CLI. `parseArgs` builds a `CliOptions` with sensible defaults: JSON output, a `failOn` set of `critical` and `high`, zero minimum coverage and five sampled argument hashes per tool.

`loadContracts` parses the schema file and hands it to `discoverToolEntries`, which accepts four shapes: a bare array, an object with a `tools`, `functions`, `toolSchemas` or `tool_schemas` array, a single entry carrying a `tool` or `function` object, or a map keyed by tool name where each value has an `input_schema`, `inputSchema`, `parameters` or `schema` field. Each entry goes through `normalizeContract`, which resolves the name from `name` or `function.name`, resolves the schema from the same set of aliases plus `function.parameters`, and wraps a bare property bag in `{ type: "object", properties: ... }` when it is not already an object schema. Duplicate tool names are a hard error.

Every contract gets a `schemaHash`: the first 16 hex characters of the SHA-256 of `canonicalize(schema)`. `canonicalize` is a recursive serializer that sorts object keys, so key ordering and whitespace do not change the hash. That is what makes baseline comparison stable across formatters and across the JSON emitters of different languages.

Trace ingestion is the fuzzy part. `parseTraceRecords` first tries to parse the whole file as JSON, and falls back to line by line JSONL if that fails and the text is multi line, skipping blanks and `#` comments. `flattenTraceRecords` then walks the parsed value recursively, descending through any of ten container keys (`tool_calls`, `toolCalls`, `events`, `spans`, `records`, `requests`, `steps`, `messages`, `data`, `logs`) until `looksLikeToolCall` returns true. That predicate accepts a node whose `type` or `kind` contains `tool` or `function_call`, or any node that has both a recognizable tool name key and a recognizable argument key.

Finding those keys is done by `flatten`, which builds a dotted path map of the whole record and also registers each bare leaf key at top level, capping array traversal at the first 100 elements. `findDottedString` and `findAnyValue` then try exact dotted keys first (so `function.name` works directly) and fall back to matching on the last path segment. `normalizeToolName` strips a leading `functions.` or `tools.` prefix. `parseArguments` detects the OpenAI style stringified argument blob, parses it, and records `argumentWasString` so the report can flag it as a `StringifiedArguments` finding.

Validation lives in `validateValue`, a hand written JSON Schema subset walker with a depth limit of 40 that emits `SchemaDepthLimit` rather than recursing forever. It short circuits on `anyOf` and `oneOf` (if any branch validates clean the value passes, otherwise `UnionMismatch`), accumulates `allOf` branches, then checks `const`, `enum` and `type`, with a deliberate carve out so an integer satisfies a `number` type. From there it dispatches to `validateObject` (required properties, per property recursion, `AdditionalProperty` when `additionalProperties` is `false`, and recursion into a schema valued `additionalProperties`), `validateArray` (`minItems`, `maxItems`, `items`), `validateString` (`minLength`, `maxLength`, `pattern`, plus `InvalidSchemaPattern` when the regex itself will not compile) and `validateNumber` (`minimum`, `maximum`, the exclusive variants and `IntegerExpected`).

`buildReport` ties it together. Calls to undeclared tools become `UnknownToolCall` at high severity. A `schema_hash` recorded in the trace that differs from the computed contract hash becomes `RuntimeSchemaHashMismatch`. Per tool it tracks call count, invalid call count, the set of unknown field names and up to N deduplicated argument hashes via `pushSample`. `auditCoverage` adds `ToolCoverageBelowMinimum` and a `DeclaredToolNotObserved` per unexercised tool. `auditBaseline`, when a baseline is supplied, emits `SchemaChangedFromBaseline` (high), `BaselineToolRemoved` (medium) and `ToolAddedSinceBaseline` (medium). The run passes when no finding matches the `failOn` set.

Rendering is three functions. `renderMarkdown` produces a coverage table and a findings list. `renderSarif` emits SARIF 2.1.0 with critical and high mapped to `error`, medium to `warning` and low to `note`. `renderBaseline` writes the committable snapshot of names, hashes, required fields and declared properties.

## Usage

```bash
# Generate the baseline once and commit it
node ToolCallSchemaDriftGuard.ts --schema tools.json --format baseline -o .tooldrift-baseline.json

# Normal CI run: validate traces against the schema and the baseline
node ToolCallSchemaDriftGuard.ts \
  --schema tools.json \
  --trace traces/prod.jsonl \
  --trace traces/evals.json \
  --baseline .tooldrift-baseline.json \
  --format json

# Fail only on critical, require 80 percent of declared tools to be exercised
node ToolCallSchemaDriftGuard.ts --schema mcp-list-tools.json --trace trace.jsonl \
  --fail-on critical --min-coverage 80 --max-samples-per-tool 10

# Human readable review output
node ToolCallSchemaDriftGuard.ts --schema tools.json --trace trace.jsonl --format markdown

# SARIF for GitHub code scanning
node ToolCallSchemaDriftGuard.ts --schema tools.json --trace trace.jsonl \
  --format sarif -o drift.sarif

# Pipe a trace in on stdin
cat trace.jsonl | node ToolCallSchemaDriftGuard.ts --schema tools.json --trace -

# Tolerate an absent trace file in a partial rollout
node ToolCallSchemaDriftGuard.ts --schema tools.json --trace maybe.jsonl --allow-missing-trace
```

Bare positional arguments are treated as extra trace paths. `--help` or `-h` prints the flag list and exits 0.

## Notes

- Exit codes: `0` when no finding matches `--fail-on`, `1` when a blocking finding exists, `2` on any `GuardError` such as a missing `--schema`, invalid JSON, a duplicate tool name or an absent trace file without `--allow-missing-trace`.
- Zero runtime dependencies. It imports only `node:fs`, `node:crypto` and `node:url`. It is ESM and uses `import.meta.url`, so run it through a TypeScript capable runtime or compile it first.
- The validator covers a subset of JSON Schema. There is no `$ref` resolution, no `format` checking, no `patternProperties`, `not`, `uniqueItems`, `multipleOf`, `propertyNames` or tuple style `items` arrays. `oneOf` is validated like `anyOf`, so it will not flag a value that matches more than one branch.
- `pattern` strings are compiled with the native `RegExp` and executed against observed values. A pathological pattern in your own schema can still backtrack badly. This is not a sandbox.
- Key discovery is heuristic. `flatten` registers bare leaf keys as well as dotted paths, so a record with an unrelated nested field called `name` or `params` can be picked up as the tool name or the argument bag. Prefer traces where the dotted path matches one of the known keys.
- Coverage is measured per tool, not per argument or per branch. A tool with one observed call counts as fully covered.
- `--max-samples-per-tool` stores deduplicated 16 character hashes of argument objects, not the arguments themselves. Nothing from the payload body is written into the report, so the output is safe to attach to a ticket.
