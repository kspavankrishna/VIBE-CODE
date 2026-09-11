# Tool Schema Distiller

Your MCP server exposes 60 tools and every one ships a full JSON Schema. The manifest alone eats most of the context window before the user's request is even read. This is a Python tool that ranks tools against the current task and compresses their JSON Schemas to a token budget you choose, while recording exactly what it threw away.

**Language:** Python | **Lines:** 1311 | **Added:** 2026-05-12

## What this solves

Large tool manifests are normal now. An agent gateway fronting three or four MCP servers easily carries 80 tool definitions, each with a nested `inputSchema` full of enums, `$ref` chains, `oneOf` branches, pattern constraints and paragraph-long field descriptions. That text is correct and useful at runtime. At prompt time it is mostly dead weight. The model does not need a 60 character regex pattern to decide whether to call a search tool.

The failure mode is not a crash. It is quieter and more expensive. Tool selection accuracy drops because the right tool is buried on line 900 of a manifest the model skimmed. Input tokens climb on every turn because the manifest is re-sent each time. Someone adds one more MCP server, the agent quietly gets worse and nobody can point at a stack trace to explain why. Finance notices the bill before engineering notices the regression.

The naive fix is truncation: cut at N characters, keep the first 10 tools, strip every description. That works until it silently drops a required field and the model starts emitting calls that fail server side validation. Truncation has no idea which bytes changed model behavior and which were decoration.

So this treats prompt-time schema as a compression problem with an explicit budget and an explicit loss report. Required properties are protected, enums are truncated with a count marker rather than deleted, local `$ref` targets are followed and re-attached, and everything that did not fit lands in an `omissions` list with a risk level. The tradeoff becomes visible to CI instead of a surprise in production. The original schema stays on your server for runtime validation. This output is for prompt construction only.

## Why I built it

Existing tooling sits at two wrong extremes. JSON Schema libraries validate, dereference and bundle. They are built for correctness, so they make schemas bigger, not smaller. On the other side are prompt compression scripts doing character-level truncation with no schema awareness, which is how you ship a manifest where `required: ["repo", "owner"]` survived but the `repo` property did not. Nothing in between did query-aware selection with a hard token budget and an audit trail.

So: one file, no dependencies, dropped into a prompt-build step. Hand it the user's message, get back a manifest that fits. The audit trail mattered as much as the compression. If a distiller drops a required argument I want the pipeline to fail at build time, not hear about it from a customer.

## When to use it

- An MCP proxy or agent gateway aggregates several servers and the combined tool list no longer fits a sane prompt budget.
- Tool selection quality fell off after you added tools and you suspect manifest size, not model capability.
- You pay per input token on every turn, and the manifest is the largest fixed cost per request.
- An OpenAPI to function-calling converter produced deep `$ref` graphs and 200-value enums nothing needs at selection time.
- CI should fail when a schema change pushes a required argument out of the distilled manifest.

## How it works

`extract_tools` accepts a bare list, an object with a `tools` or `functions` array, or a single tool object, and `normalize_tool` unwraps the OpenAI `{"type": "function", "function": {...}}` envelope and finds the schema under `inputSchema`, `parameters` or `schema`. MCP and OpenAI payloads both land as a `ToolSpec`.

Ranking is lexical, not semantic, and honest about that. `keyword_set` runs text through `explode_identifier`, which splits camelCase on the lowercase-to-uppercase boundary and turns underscores, hyphens and slashes into spaces, so `list_pull_requests` becomes four terms. `normalize_term` does crude suffix stemming and a stopword list drops filler. `lexical_overlap` is intersection size over query term count. `_tool_score` weights that overlap against the tool name at 5.0, the description at 3.2 and schema terms gathered by `collect_schema_terms` at 2.2, plus a capped complexity bonus so a rich tool is not beaten by a stub. No embeddings, no network call, no model in the loop.

Budgeting is the interesting part. `estimate_tokens` is a four-characters-per-token heuristic over canonical JSON from `stable_json`, which sorts keys and strips whitespace so the same schema always measures the same. `_tool_floor` computes the smallest useful form of a tool: name, trimmed description, type and first six required fields, floored at `tool_floor_tokens`. `_select_tools` greedily admits tools in rank order while their floors fit and always keeps at least one. `_allocate_budgets` splits the leftover by the largest remainder method, the apportionment algorithm used for legislative seats, weighted by score times `1 + log1p(complexity)`. Integer shares first, leftover single tokens to the largest fractional parts, so every token is assigned exactly once.

`_distill_node` then walks the schema, dispatching on `schema_kind` to `_distill_object`, `_distill_array`, `_distill_union`, `_distill_primitive` or `_distill_ref`. Objects score properties with `_field_score`: required fields get a flat +5.0, plus query overlap on name and description, plus rewards for enums and constraints, minus a penalty for depth and `deprecated`. Fields are visited required-first, each with a share proportional to its score. On overflow a required field degrades to a `_minimal_fragment` (type, const or short enum, constraints only) and is dropped only if even that will not fit, recording a **high** risk omission. Optional fields drop at **medium** or **low**. Unions sort branches by `_branch_score`, favoring branches with a discriminator-like const tag found by `_const_tag` on keys such as `kind`, `action` or `operation`. `allOf` keeps source order because its branches are conjunctive, and dropping one is always high risk.

Every write goes through `_copy_if_fit`, optimistic write with rollback: set the key, re-measure the node, restore the previous value if it went over, so no partial write escapes. `_distill_enum` grows an enum value by value until the next will not fit, then stamps `x-distilledEnumCount` with the true length so the model knows the list is partial. `$ref` strings collected during the walk are resolved by `_attach_ref_definitions`, a breadth-first queue over the ref graph that distills each target under its own share of the remaining budget and files results under `$defs` or `definitions` by pointer prefix. Still over budget, three fallbacks fire in order: halve the description, strip nested descriptions, strip examples and defaults. `manifest_fingerprint` then hashes the canonical manifest with blake2b at 12 bytes for caching or diffing runs.

## Usage

```bash
# Rank and compress a manifest against the current user task
python ToolSchemaDistiller.py \
  --input tools.json \
  --query "search recent GitHub pull requests and summarize comments" \
  --budget 2800

# Pipe from a server, emit only the manifest array, ready to paste into a prompt
curl -s https://gateway.internal/mcp/tools | python ToolSchemaDistiller.py \
  --input - --query "$USER_TASK" --budget 3200 --top-tools 8 --manifest-only

# CI gate: exit 2 if any high risk omission was recorded
python ToolSchemaDistiller.py --input tools.json --query "create an issue" --budget 1200 --strict
```

As a library:

```python
from ToolSchemaDistiller import DistillConfig, SchemaDistiller, extract_tools

tools = extract_tools(json.load(open("tools.json")))
config = DistillConfig(total_budget_tokens=2800, max_tools=8, max_description_chars=180)
report = SchemaDistiller(config).distill_tools(tools, query="summarize open pull requests")

manifest = report["manifest"]              # list of {name, description, inputSchema}
report["omissionSummary"]                  # {"high": 0, "medium": 2, "low": 5}
report["droppedTools"]                     # tools that did not make the cut
report["fingerprint"]                      # blake2b of the canonical manifest
```

`--max-description-chars` sets the per-description ceiling before sentence selection, and `--include-examples` keeps small `examples` blocks when they fit.

## Notes

- Token counts are an estimate, not a tokenizer. `estimate_tokens` is `ceil(len(text) / 4) + 1` over canonical JSON, so expect drift against a real BPE count and leave headroom near a hard context limit.
- Ranking is pure lexical overlap, no embedding model and no synonym table. A query using different vocabulary than the tool names and descriptions ranks poorly. For semantic matching, rank upstream and feed this a pre-filtered list.
- Only local `$ref` pointers starting with `#/` resolve, with `~0` and `~1` unescaping. Remote and cross-file refs stay as a bare `$ref` and record a high risk omission, so a bundled schema is the safe input.
- The manifest is lossy by design and is not valid for runtime validation. Keep the original server side. The `x-distilledEnumCount` marker and the `omissions` list say where the loss is.
- Exit codes: 0 on success, 2 when `--strict` is set and a high risk omission was recorded, 64 for invalid JSON or a payload with no recognizable tools, 1 for any other failure.
- Standard library only: no requests, no pydantic, no jsonschema. Builtin generic annotations are deferred by `from __future__ import annotations`, so Python 3.8 or newer is enough. Deterministic for a given input, query and config: ties break on lowercase name then source index, so the fingerprint is stable and safe to cache on. Descriptions are trimmed by whole sentence selection ranked on query overlap, so trimming can reorder sentences.
