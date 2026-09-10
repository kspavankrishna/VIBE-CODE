# MCP Manifest Compat Gate

You tighten one field in an MCP tool's inputSchema, merge it, and every agent still holding the old contract starts failing on calls that used to work. This is a single file OCaml CI gate that diffs two tool manifests and tells you, per tool and per JSON pointer path, whether the change is breaking, non breaking or needs a human.

**Language:** OCaml | **Lines:** 2286 | **Added:** 2026-05-07

## What this solves

MCP tool manifests and OpenAI style tool arrays are API contracts, but almost nobody reviews them like one. Someone adds `"required": ["workspace_id"]` to a tool with ten callers, flips `additionalProperties` from true to false, or narrows an enum from six values to four. The diff is three lines and it sails through review. Then production breaks in a way that does not point back at the commit: agents holding the cached manifest keep sending the old payload, and what surfaces is a tool call error inside a model transcript, not a stack trace in your service.

The cost lands on whoever is on call for the agent runtime, not on whoever edited the schema. Cached clients, pinned SDKs, replayed evals and long running sessions all hold contracts you already moved past. Output schemas fail the other direction: a consumer reading `result.items[].id` breaks when you drop `id`, silently, until a parser throws days later in a repo owned by another team.

This gate reads both manifests, walks every tool present in both, resolves the schemas to concrete constraints and compares them. Each difference becomes an issue carrying a severity, a tool name, a surface (`input`, `output`, `tool` or `manifest`), a JSON path like `$.properties.filters.items` and a stable code like `required-added`. Breaking issues exit 2 and stop the pipeline.

Direction is what matters. For inputs, the new schema must still accept everything old callers could send: widening is fine, narrowing is not. For outputs, when both sides publish one, the same comparison runs with the arguments swapped, so the new response still satisfies consumers written against the old shape. That contravariance is what a naive schema diff gets backwards.

## Why I built it

Generic JSON diff tools tell you a byte changed. JSON Schema validators tell you whether one document is valid. Neither answers the real question, which is whether every value the old schema accepted is still accepted by the new one. The semver tooling built for OpenAPI does not read MCP manifests, does not know `inputSchema`, `parameters` and a bare `function` wrapper are the same thing, and does not know an added `required` entry breaks callers while a removed one does not.

I also wanted no dependency tree. One OCaml file against the standard library: its own JSON parser, its own pointer resolver, its own mini validator. Nothing to `opam install`, nothing to audit transitively, and short enough to read end to end before you trust it in CI.

## When to use it

- A pre merge check on any repo holding MCP tool manifests, so a narrowed field fails the build instead of an agent.
- Deciding the version bump on a tool server release when you cannot remember whether last week's edits were additive.
- Reviewing a vendor's manifest update before upgrading the client that talks to it.
- Gating a deliberate breaking migration, to get the exact list of breaks for the changelog.

## How it works

Parsing is done by the internal `Json` module, a recursive descent parser over a `json` variant. Numbers keep their raw literal string, which is what lets the tool tell `1` from `1.0` when comparing `const` values, and failures raise `Json_parse_error` with a `location`, so you get `path:line:column: message`, not "invalid JSON". `parse_manifest` then normalizes four shapes into one `manifest` record holding a `StringMap` of `tool_spec`. Under `--manifest-format auto`, a top level `tools` array is `mcp-tools`, a top level array is `openai-tools`, an object with a `name` is a single tool, anything else is a tool map keyed by name. `build_tool_spec` unwraps an OpenAI `function` wrapper and picks the input schema from the first of `inputSchema`, `input_schema`, `parameters`, `schema` or `input`, the output schema from `outputSchema`, `resultSchema`, `responseSchema` or `returns`. Duplicate names raise `Manifest_error`.

Before comparing, `resolve_schema` chases `$ref`. Only local pointers resolve: `#` and `#/...`, unescaped by `unescape_pointer_segment` for `~0` and `~1`. It carries a `StringSet` of visited pointers and raises `Resolve_error` on a cycle instead of looping forever. Sibling keywords beside a `$ref` merge onto the target, siblings winning. Each schema pair first runs through `semantic_json_equal`, which is `json_equal` over `strip_annotations`: `description`, `title`, `examples`, `default`, `deprecated`, `$comment` and any `x-` key are dropped, so a docs only edit produces nothing.

The comparison is `compare_schema_subset`, mutually recursive with `compare_const_and_enum`, `compare_object_rules`, `compare_additional_properties`, `compare_array_rules`, `compare_string_rules` and `compare_numeric_rules`. Types reduce to a `type_set` via `type_set_of_fields`, which reads an explicit `type`, adds `null` on `nullable: true`, and otherwise infers a hint set from the keywords present. `type_allows` accepts `integer` wherever `number` is allowed. Losing a type is breaking, gaining one is not. Object rules flag added `required` entries and tightened `minProperties` or `maxProperties`, and treat a removed property as breaking only when the new `additionalProperties` is `false`: if extras are still allowed you get a note, and if a fallback schema governs them the old property is checked against it. Numeric bounds fold into a `numeric_bound` record carrying an exclusive flag, so `minimum: 5` versus `exclusiveMinimum: 5` compares correctly.

Enums and consts are checked by replay rather than set logic. Every old enum member and the old `const` runs through `validate_sample`, a tri state validator (`Valid`, `Invalid`, `Unknown`) evaluating the new schema against that literal. A rejected sample becomes a breaking issue quoting the reason, for example `string length 3 is below minimum 5`. It measures strings in UTF-8 code points via `utf8_length` and returns `Unknown` where it cannot decide.

Anything it cannot reason about soundly goes through `unsupported_policy`. The `schema_combinator_keys` set covers `allOf`, `anyOf`, `oneOf`, `not`, `if`, `then`, `else`, `dependentSchemas`, `patternProperties`, `propertyNames`, `unevaluatedProperties`, `contains`, `prefixItems` and `$dynamicRef`. When one of those differs, or a `pattern` changes, or the shape changes structurally, the issue lands at breaking, warning or note depending on `fail`, `warn` or `ignore`. The default is `fail`: unknown means blocked until a human looks. Finally `compare_manifests` adds manifest level issues (`tool-removed` breaking, `tool-added` not), sorts by `severity_rank`, and derives `status` and `recommended_bump`: breaking to major, any warning to `needs-review`, non breaking to minor, notes only to patch, clean to none.

## Usage

```sh
# Run straight from the toplevel, no build step
ocaml McpManifestCompatGate.ml --before old-manifest.json --after new-manifest.json

# Or compile once for CI, no packages required
ocamlfind ocamlopt McpManifestCompatGate.ml -o mcp-compat-gate

# Machine readable report for a CI annotation step
./mcp-compat-gate --before old.json --after new.json --json

# Force a format instead of auto detection
./mcp-compat-gate --before old.json --after new.json --manifest-format openai-tools

# Warn on advanced keywords instead of failing, ignore prose churn,
# skip the reversed output schema check
./mcp-compat-gate --before old.json --after new.json \
  --unsupported-policy warn --ignore-description --skip-output-schema

./mcp-compat-gate --help
```

Text output leads with `Status`, `Recommended version bump` and the tool count and format on each side, then one line per issue:

```
[BREAKING] tool=search_docs surface=input path=$.properties.query code=minLength-tightened minLength increased from 1 to 3
```

The `--json` report carries `status`, `recommendedBump`, the tool counts, the detected formats and an `issues` array of `{severity, tool, surface, path, code, message}`.

## Notes

- Exit codes: `0` for `compatible` or `needs-review`, `2` for any breaking issue, `64` for a bad CLI argument, `65` for a JSON parse or manifest error, `66` for a schema resolution error. A `warn` policy run reports but does not block, by design.
- Only local `$ref` pointers resolve. A `$ref` to another file or an https URL raises a resolution error, surfaced at the unsupported policy severity, never a silent pass.
- `pattern` is never evaluated. Adding one is breaking, removing one is not, changing one goes to the unsupported policy. There is no regex containment check here.
- `format` changes are notes only. The tool does not assume the runtime enforces `date-time` or `email`.
- Combinator keywords are detected, not analyzed. Schemas leaning hard on `allOf` or `oneOf` give review issues, not verdicts.
- A renamed tool reads as one removal plus one addition, so it is reported breaking. That is the right answer for a cached client.
- Requires OCaml 4.08 or later for `Fun.protect`, `Option.bind` and `Float.round`. No opam packages, no build system, one file.
