# MCP Tool Contract Gate

An MCP server ships a small edit to one tool's JSON Schema, a required field appears or a maximum drops, and every agent, cached plan and eval that was calling that tool starts failing. This is a Haskell CLI that diffs two MCP tool snapshots and fails the build only when the new schema is provably stricter for existing callers.

**Language:** Haskell | **Lines:** 1104 | **Added:** 2026-05-07

## What this solves

Model Context Protocol tool definitions are an API contract, but almost nobody treats them like one. The server publishes `tools/list`, each tool carries an `inputSchema`, and clients, agent runtimes, prompt caches, eval suites and generated SDKs all bake in assumptions about what that schema accepts. The schema lives in the server repo. The callers live somewhere else, often in a different team's release train. Nothing in the protocol tells you when the two drift apart.

The failure mode is quiet and delayed. Someone adds `"required": ["region"]` to a search tool because the new backend needs it. The MCP server deploys fine. Its own tests pass. Then the agent in production keeps emitting the old argument shape it learned from last week's tool list, the server rejects the call, and what the user sees is not a schema error but an agent that has suddenly become unreliable at a task it used to do. The same shape of break comes from a `maxLength` that drops from 4096 to 512, an enum that loses a value, `additionalProperties` flipping from absent to `false`, an `items` schema that got a new type constraint, or a tool that was renamed and so simply vanished from the list.

Tool removal is the worst of the set because it is the easiest to do by accident. Rename `search_docs` to `docs_search` and every stored plan, every few shot example and every hardcoded caller referencing the old name breaks at once. Nothing warns you, because from the server's point of view a rename is just a list that now contains a different string.

The cost is paid by whoever is on call and by whoever has to reconstruct which of the last twelve merges touched a schema. This tool moves that discovery to CI. You snapshot the tool list before and after, run the gate, and it exits 2 with a per tool, per path list of exactly which constraints got stricter. Everything that only widens the contract, new optional properties, new tools, a relaxed maximum, a dropped `required` entry, produces no finding at all.

## Why I built it

JSON Schema diff tools exist but they diff documents, not contracts. They tell you a key changed. They do not tell you whether the change can break a caller, which is the only question that matters at a release gate. Run a generic diff over a real tool list and you get hundreds of lines of noise from reordered keys and edited descriptions, so people stop reading it, so it stops gating anything.

The MCP side of the problem has its own wrinkle. Tool lists come in several shapes depending on who serialised them: a bare array, `{"tools": [...]}`, a `result` wrapper from a raw JSON-RPC capture, or an OpenAI style list where the real payload sits under `function`. The schema key might be `inputSchema`, `input_schema`, `parameters` or `schema`. A gate nobody can point at their existing snapshot format is a gate nobody runs. So the parsing here is deliberately forgiving and the comparison is deliberately strict.

## When to use it

- A CI job on the MCP server repo that compares the tool list from `main` against the tool list from the branch, and blocks merge on a breaking narrowing.
- A pre deploy check that snapshots the running server's `tools/list`, then compares it against the version you are about to push.
- Cutting an SDK or client release whose generated types depend on tool inputs staying stable.
- Investigating an agent that started failing after a server deploy, when you want the specific field rather than a guess.
- Reviewing a large refactor of a tool registry, where you want the ambiguous `anyOf` and `oneOf` rewrites listed separately from the clear breaks.
- Gating a single tool during a staged migration, using `--tool` so unrelated churn does not block you.

## How it works

Input handling is the loose part. `findToolArray` walks the decoded JSON up to six levels deep looking for an array whose every element passes `looksLikeToolCandidate`, meaning it has a `name`, a `toolName` or a `function.name`. It short circuits on any object with a `tools` key. `extractTool` then unwraps a `function` envelope if present and tries `inputSchema`, `input_schema`, `parameters` then `schema`, on the inner object first and the outer object second. A missing or null schema becomes `Bool True` via `normalizeSchemaRoot`, which is JSON Schema for "accepts anything". `buildToolMap` refuses a snapshot with duplicate tool names rather than silently keeping one.

Before comparing, each tool's schema is turned into a `SchemaDoc`: the root value plus `docPointers`, a `Map` from every JSON Pointer in the document to the value at that pointer, built once by `indexPointers` with proper `~0` and `~1` segment escaping. That makes local `$ref` resolution a hash lookup instead of a repeated tree walk. `resolveValue` follows refs with a `seen` set for cycle detection and a hard cap of 16 hops, merging sibling keywords over the target with `KM.union local targetObj` so local keys win. Every case it cannot handle, an unresolvable ref, a cycle, a hop limit, siblings next to a non object target, becomes a `ref-resolution` finding rather than a silent wrong answer.

The comparison itself is a recursive walk over the before schema, driven by an effective type set. `schemaTypes` reads an explicit `type` if there is one, otherwise `inferredTypes` guesses from keyword signals, so a schema with `minLength` and `pattern` is treated as a string schema even with no `type` declared. It also honours the OpenAPI style `nullable` flag. `acceptsType` encodes the one real subtyping rule in JSON Schema: an integer is still accepted where the new schema says `number`. Facet comparisons only run where the types actually overlap, so `compareStringFacets` never fires on a node that was never a string.

Each facet family has its own directional rule and its own finding code. `compareObjectFacets` reports `required-properties-added`, `min-properties-increased`, `max-properties-decreased`, `dependent-required-added` and, for a property that disappeared while `additionalProperties` is `false`, `property-removed`. `compareArrayFacets` covers `minItems`, `maxItems`, `uniqueItems` going false to true, `items`, `prefixItems` tuple positions including trailing slots that fall through to `items`, and `contains` with its `minContains` default of 1. `compareNumericFacets` normalises `minimum` and `exclusiveMinimum` into a `NumericBound` so it can decide strictness including the exclusive versus inclusive tie at the same value, and `multipleOfCompatible` uses exact `Rational` division to see whether the old step is an integer multiple of the new one, which is the condition under which every previously valid value still passes. `compareSchemaModeChange` reduces `additionalProperties`, `propertyNames` and `items` to a three way `SchemaMode` of allow any, reject all or a real subschema, using `isAcceptAllSchema` to treat an object holding only annotation keys such as `title`, `description`, `default` or an `x-` prefix as no constraint at all.

Anything the tool cannot decide soundly is not guessed at. `compareAmbiguousKeys` flags `allOf`, `anyOf`, `oneOf`, `not`, `if`, `then`, `else`, `patternProperties`, `dependentSchemas`, `unevaluatedItems` and `unevaluatedProperties` as manual review, and a changed `pattern` or `format` is ambiguous too since regex subsumption is not something you want a release gate bluffing about. Those land as `Warning` by default and become `Breaking` under `--strict-ambiguous`, which is the whole of what that flag does. Findings are deduplicated through a `Set`, sorted breaking first then by tool, path and code, and truncated for display only: `reportBreakingCount` is always counted over the full set, so `--max-findings` can never hide a failure.

## Usage

```bash
# Build. Needs base, aeson, containers, scientific, text, vector and bytestring.
ghc -O2 McpToolContractGate.hs -o mcp-tool-contract-gate

# Compare two snapshots. Exits 2 if anything narrowed.
./mcp-tool-contract-gate before.json after.json

# Machine readable report for a CI annotation step.
./mcp-tool-contract-gate before.json after.json --json

# Treat anyOf/oneOf/pattern rewrites as breaking too.
./mcp-tool-contract-gate before.json after.json --strict-ambiguous

# Gate only the tools you care about during a staged migration.
./mcp-tool-contract-gate before.json after.json --tool search_docs --tool fetch_page

# One side can come from stdin.
curl -s "$MCP_URL/tools" | ./mcp-tool-contract-gate baseline.json - --max-findings 50
```

Accepted input shapes are a bare array of tools, `{"tools": [...]}`, or those nested inside `result`, `data` or similar wrappers up to six levels deep.

## Notes

- Exit codes: 0 clean, 2 at least one breaking finding, 1 for unreadable or undecodable input, a missing tools array, a duplicate tool name or a `--tool` name absent from both snapshots, 64 for a bad command line.
- Only local refs resolve. `#` and `#/...` pointers work, remote or `$id` based refs are reported as unresolvable rather than fetched, and nothing here touches the network.
- It reasons about the before schema's shape. New properties added in the after snapshot are only examined through `required` and `dependentRequired`, since adding an optional property cannot break an existing caller.
- Regex and format semantics are not evaluated. A tighter `pattern` is a warning, not a break, unless you pass `--strict-ambiguous`.
- `--max-findings` limits printed output only. The truncation notice tells you to rerun with a larger value.
- Output descriptions, titles and other annotation keys are ignored by design, so prose edits never trip the gate.
- One honest defect: `extractTool` calls `fromMaybe` but the `Data.Maybe` import line only brings in `isJust` and `mapMaybe`. Add `fromMaybe` to that import list before building.
