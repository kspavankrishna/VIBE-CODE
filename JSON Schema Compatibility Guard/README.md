# JSON Schema Compatibility Guard

A schema edit that looks harmless in a diff can reject traffic that used to work. This is a single file Ruby CLI that compares an old JSON Schema against a new one and tells you, with exit codes, whether the new one still accepts everything the old one accepted.

**Language:** Ruby | **Lines:** 1288 | **Added:** 2026-04-15

## What this solves

JSON Schema compatibility checking for MCP tool schemas, OpenAI structured outputs, Anthropic tool definitions, OpenAPI request and response contracts and event payload validation in CI. Schema drift is one of those boring failures that burns release time: one extra required field, one narrower enum, one stricter array rule, and suddenly agents, backends or pipelines start rejecting traffic that used to work.

The failure mode always looks the same. Someone adds `"required": ["tenant_id"]` to a tool input schema because the new code path needs it. The diff is two lines. It ships. Every already deployed agent that calls that tool without `tenant_id` now gets a validation error instead of a result, and because the schema is the contract, that surfaces as a model refusing to call the tool at all rather than as a stack trace anyone can grep. Same story with an enum that drops a value one old client still emits, a `maxLength` cut from 500 to 200 to match a new column width, or an `additionalProperties: false` added for tidiness.

Nobody notices at merge time. It gets noticed by whoever is on call when a partner integration starts 400ing, or by a data engineer three days later when the dead letter queue fills with events that were valid last week. The cost is not the fix, that is usually one line. It is the hours between deploy and diagnosis, plus the reprocessing.

Schema registries solve this for Avro and Protobuf. JSON Schema, where most tool definitions, LLM structured output contracts and webhook payloads actually live, mostly does not get the same treatment. This file gives you the check as a plain script you run on two files.

## Why I built it

The existing options are either a hosted registry you adopt wholesale, or a validator that tells you whether a document matches a schema, which is a different question. I wanted the narrow thing: two schema files in, a verdict and an exit code out, no service, no gem install, no daemon. Ruby stdlib only, so it runs anywhere a Rails or Sidekiq repo already has Ruby.

The other reason is honesty about limits. Full JSON Schema subsumption is undecidable in general, and tools that pretend otherwise give you confident wrong answers on `anyOf` and `if`/`then`. This one reports what it can prove, warns where a human has to look and never claims a change is safe when the construct is beyond its reasoning.

## When to use it

- Gating a pull request that touches an MCP server's tool input schema, before agents in the field start failing calls
- Version bumping an OpenAPI request body and needing to know whether old clients still validate
- Publishing an event payload schema when producers and consumers deploy on different days
- Tightening an LLM structured output contract and wanting to know which enum values you just dropped
- Checking forward compatibility before a rollback, where the old schema has to accept what the new one produces

## How it works

The pipeline is four stages: `Loader`, `Resolver`, `Comparator` and `Report`, driven by `CLI`. `Loader.load_file` parses JSON or YAML by extension, falls back to YAML if JSON parsing fails, then deep stringifies keys so symbol and string keys compare identically. The root must be an object or a boolean, anything else raises `Error`.

`Resolver` flattens the schema into a shape the comparator can walk. It inlines local `$ref` pointers only: an external ref raises, and a ref already on the resolution stack raises as a cycle rather than looping forever. `pointer_lookup` handles `~0` and `~1` unescaping plus array indices, and `@resolved_cache` memoizes each pointer. It strips the keys in `ANNOTATION_KEYS` (`title`, `description`, `default`, `examples`, `$comment` and friends) plus anything starting with `x-`, so a docs only edit produces zero issues, and it normalizes OpenAPI style `nullable: true` into a `type` array containing `null`. Then it folds `allOf`: `merge_all_of_fragments` merges fragment into base keyword by keyword with intersection semantics. `type` becomes a set intersection and raises if empty, `required` unions, `enum` intersects, the `min*` keywords take the max, the `max*` keywords take the min, `uniqueItems` ORs, `multipleOf` merges through `Rational` divisibility and `properties` merges recursively. Keywords it cannot merge safely, like two different `pattern` values, raise rather than guess.

`Comparator#compare_schema` then walks source and candidate in lockstep, carrying a JSON Pointer path built by `Helpers.pointer_join`. It short circuits on identical subtrees and on `source == false`, since a schema that accepts nothing cannot be narrowed. Every check is one sided: it asks only whether the candidate rejects something the source accepted. Type coverage via `candidate_covers_type?`, which treats `number` as covering `integer`. Numeric bounds via `lower_bound` and `upper_bound`, which respect exclusive versus inclusive and flag a same value bound that turned exclusive. String `minLength` up or `maxLength` down. Objects: added `required`, added `dependentRequired`, added `propertyNames`, tightened `additionalProperties`. Arrays: `minItems` up, `maxItems` down, added `uniqueItems`, positional `prefixItems`, tail `items`, added `contains`.

One place it stops reasoning symbolically, deliberately. When the source schema is finite, meaning it has a `const` or an `enum`, `compare_finite_schema` runs each enumerated value through the bundled `Validator` against the candidate and reports the ones that fail, with up to three examples in the issue details. That is exact rather than heuristic, and it hands you the actual rejected values. `Validator` is a self contained JSON Schema subset checker in the same file, covering boolean combiners, types, numeric with `Rational` `multipleOf`, strings, objects, tuple and list arrays, `contains` with `minContains` and `maxContains` plus a small set of string formats when `--strict-format` is on.

When a property disappears from `properties`, the comparator does not assume the worst. It looks at the candidate's `additionalProperties`: `false` is breaking, `true` is a note, and a schema means it recurses and compares the old property schema against the new fallback. Constructs it cannot decide, listed in `UNSUPPORTED_COMPARISON_KEYWORDS`, produce a warning saying exact compatibility is undecidable there rather than a fake verdict. Findings land in `Report` as `Issue` structs at three severities, breaking, warning and note, and `Report#exit_code` derives the process exit from those plus `--fail-on`.

## Usage

```bash
# backward compatibility: does the new schema still accept old payloads
ruby JsonSchemaCompatibilityGuard.rb schemas/tool.v1.json schemas/tool.v2.json

# JSON output for CI, and fail the build on warnings too
ruby JsonSchemaCompatibilityGuard.rb --format json --fail-on warning \
  schemas/order.v1.yaml schemas/order.v2.yaml

# forward compatibility: can the old schema accept what the new one produces
ruby JsonSchemaCompatibilityGuard.rb --mode forward old.json new.json

# treat date-time, date, uuid, email and uri formats as real constraints
ruby JsonSchemaCompatibilityGuard.rb --strict-format old.json new.json

ruby JsonSchemaCompatibilityGuard.rb --help
```

Flags: `--format text|json`, `--mode backward|forward`, `--strict-format`, `--fail-on breaking|warning`, `-h`. Exit `0` compatible, `1` incompatible, `2` error.

## Notes

- Ruby stdlib only: `json`, `yaml`, `set` and `optparse`. No gems, no network, no registry.
- Not a general subsumption prover. `anyOf`, `oneOf`, `not`, `if`/`then`/`else`, `dependentSchemas`, `unevaluated*` and `contentSchema` changes produce warnings, never a proof either way.
- `$ref` must be local and start with `#`. External and cyclic refs raise and exit `2`, so bundle your schemas first.
- Regex `pattern` values are compared by string equality only. An added pattern is breaking, a changed one is a warning. It does not decide regex language inclusion.
- `allOf` folding is strict. Conflicting fragments, an empty type intersection, an empty enum intersection or two different `pattern` values raise rather than produce a wrong merge.
- `--strict-format` only checks `date-time`, `date`, `uuid`, `email` and `uri`/`url`. Every other format string passes.
- Annotation only edits (`title`, `description`, `default`, `examples`, `x-*`) are stripped before comparison and never report as issues.
- With `--fail-on warning` any warning exits `1`. Use it if undecidable changes should block a merge instead of quietly passing.
