# MCP Tool Schema Sanitizer

One MCP tool definition has to survive OpenAI, Anthropic, Gemini and your own agent gateway. The same JSON Schema that validates fine locally gets silently mangled or rejected by one of them, and you find out from a dropped tool call in production.

**Language:** PHP | **Lines:** 1136 | **Added:** 2026-04-15

## What this solves

This solves the annoying JSON Schema cleanup work you hit when one MCP tool definition needs to survive OpenAI Responses API, Anthropic tools, Gemini function calling and your own internal agent gateway without random breakage. I kept seeing perfectly reasonable schemas fail in production over small things: local `$ref` pointers, nullable fields, open objects, tuple arrays or draft-2020 keywords that some providers silently ignore.

The failure mode is rarely a clean error. A provider that does not understand `$ref` may drop the whole property, so the model never learns the field exists and calls your tool with half the arguments. A provider that ignores `additionalProperties` will happily forward whatever extra keys the model hallucinated straight into your handler, which is how an open input object turns into a security problem rather than a validation bug. Tuple style `items` arrays, `patternProperties`, `if`/`then`/`else` and `unevaluatedProperties` are all in the spec and all unreliable in tool calling. `oneOf` with three branches is legal JSON Schema and useless to a model choosing arguments.

The people who notice are your on call engineers, hours later, staring at a tool that worked in staging. The cost is not one broken call. It is drift: schema A works on provider 1, schema B works on provider 2, nobody knows which fields are actually enforced and the input contract quietly stops meaning anything. Once a nullable required field is in there, one provider sends `null`, another omits the key and a third sends the string `"null"`.

This file takes a schema in and gives you back a narrowed, closed, deterministic version plus a list of what it had to change, split into warnings you can live with and errors you should not ship.

## Why I built it

Every PHP MCP server I looked at either trusted the schema you handed it or validated it against the JSON Schema meta schema, which tells you the schema is legal and nothing about whether a tool provider will honour it. Legality is not the constraint here. Portability is. The useful check is narrower than the spec and nobody was shipping it.

The other gap was determinism. If you want to cache tool definitions, diff them in CI or fail a build when a schema changes shape, you need a stable byte for byte serialization and a fingerprint over it. Sorting keys by hand every time you emit a tool is the kind of thing that works until someone adds a property in a different order.

## When to use it

- You publish a PHP MCP server and the same tools get consumed by more than one model provider.
- A Laravel or Symfony backend generates tool schemas from DTOs or annotations, and you want a gate before they go out.
- You are migrating a tool gateway from one provider to another and want to know which schemas will break before you switch.
- You want CI to fail when a tool's input contract changes, using a fingerprint instead of a diff on pretty printed JSON.
- Your schemas come from a third party or a plugin registry and you do not trust their objects to be closed.
- You caught a tool handler receiving keys that were never in the schema.

## How it works

`McpToolSchemaSanitizer::sanitize(array $schema, array $options = [])` is the entry point. It builds a private instance, walks the schema once and returns a `McpToolSchemaSanitizationResult` carrying the rewritten `schema`, a sha256 `fingerprint`, `warnings`, `errors` and `metrics`. The result implements `JsonSerializable`, exposes `isSafe()` when `errors` is empty and `assertSafe()` which throws `McpToolSchemaSanitizationException`. Warnings mean something was rewritten and the result is still shippable. Errors mean the schema said something that cannot be represented portably.

The walk is `sanitizeNode`, recursive and depth tracked, and every node goes through the same fixed pipeline. First `inlineLocalRef` resolves `#/...` pointers against the original document using a JSON Pointer walk with `~0` and `~1` unescaping, merges any sibling keys over the resolved target, and carries a `refStack` so a cycle is caught and reported instead of looping forever. Remote refs and non string refs are rejected. Then `collapseCombinators` flattens `allOf` by merging each fragment through `mergeSchemas`, and hands `anyOf` and `oneOf` to `collapseNullableUnion`, which only accepts the one shape that is safely reducible: exactly two branches where one is `type: null`. Anything else is dropped with an error, because a real union is genuinely ambiguous to a tool caller. `if`, `then`, `else`, `not` and `contains` are removed with warnings.

Then `normalizeCommonKeywords` strips the `ALWAYS_STRIP` list, which is where `$schema`, `$defs`, `definitions`, `patternProperties`, `propertyNames`, `prefixItems`, `unevaluatedProperties`, `contentEncoding` and the rest of the draft only vocabulary go. Enums are deduplicated by their JSON encoding and sorted, `const` is folded into a single value enum, string keywords are dropped if empty, integer bounds are coerced from numeric strings and inverted ranges like `minLength > maxLength` are swapped and reported as errors. `normalizeTypeMetadata` handles OpenAPI style `nullable`, infers a missing `type` from `properties`, `items` or the enum's scalar types, and reduces type arrays: `["string","null"]` becomes an optional string, anything wider keeps the first type and records an error.

Type specific handling follows. `sanitizeObjectNode` normalizes `properties`, filters `required` down to names that actually exist after sanitization, sorts it, and forces `additionalProperties: false` when `force_closed_objects` is on, including replacing a dynamic map schema. `sanitizeArrayNode` fills in missing `items` and reduces tuple arrays to their first entry. `sanitizeScalarNode` deletes keywords that do not apply to the node's type, so a boolean stops carrying `pattern` and an integer stops carrying `minLength`. Because a required property is known before its subtree is visited, the `$requiredProperty` flag flows down and is what lets nullable handling distinguish "make this optional" from "this is required and unfixable".

Canonicalization is the last step. `canonicalizeNode` recurses, sorts `required` and `enum`, then emits keys in the fixed `KEY_ORDER` sequence with everything else `ksort`ed after it. `canonicalJson` encodes with `JSON_UNESCAPED_SLASHES`, `JSON_UNESCAPED_UNICODE` and `JSON_PRESERVE_ZERO_FRACTION`, and the sha256 of that string is the fingerprint. Two semantically identical schemas written in different key order produce the same hash. `enforceLimits` then checks the collected metrics against `max_depth`, `max_properties` and `max_enum_values` and records errors rather than throwing, so you still get the sanitized schema back to look at.

## Usage

```php
<?php

require __DIR__ . '/McpToolSchemaSanitizer.php';

$result = McpToolSchemaSanitizer::sanitize($rawInputSchema);

$result->isSafe();      // false when anything unportable was found
$result->schema;        // the rewritten, closed, key ordered schema
$result->fingerprint;   // sha256 of the canonical JSON
$result->warnings;      // ["$/properties/tags: examples was removed ...", ...]
$result->errors;        // ["$/properties/mode: oneOf is too ambiguous ...", ...]
$result->metrics;       // nodeCount, maxDepth, propertyCount, enumValueCount, descriptionBytes, schemaBytes

// Fail a deploy on an unportable schema
$result->assertSafe();  // throws McpToolSchemaSanitizationException

// Emit a ready to publish MCP tool entry
$tool = McpToolSchemaSanitizer::sanitizeTool('search_orders', $rawInputSchema);
// ['name', 'inputSchema', 'schemaFingerprint', 'schemaWarnings', 'schemaErrors', 'schemaMetrics']

// Every option, with its default
$result = McpToolSchemaSanitizer::sanitize($rawInputSchema, [
    'target'                       => 'cross_provider', // openai|anthropic|gemini|generic
    'force_closed_objects'         => true,
    'inline_local_refs'            => true,
    'strip_titles'                 => true,
    'strip_defaults'               => true,
    'strip_examples'               => true,
    'convert_nullable_to_optional' => true,
    'sort_keys'                    => true,
    'max_depth'                    => 8,
    'max_properties'               => 256,
    'max_enum_values'              => 256,
]);
```

## Notes

- PHP 8.1 or later. It uses readonly properties, `array_is_list`, `str_starts_with` and `match`. No Composer dependencies, no autoloader, one file.
- This is a library, not a CLI. There is no `main`, no argument parser and no exit codes. Wrap it in your own script if you want a build step.
- `target` is validated against `cross_provider`, `openai`, `anthropic`, `gemini` and `generic`, but nothing in the current code branches on it. It is a reserved knob, not a behaviour switch. Everything is tuned for the cross provider intersection.
- Errors do not throw by themselves. `sanitize()` only throws when the root is not an associative array or when an option is invalid. Everything else is collected. Call `assertSafe()` if you want a hard stop.
- An object with no properties serializes as `[]` rather than `{}`, because empty PHP arrays encode as JSON arrays. If a provider is strict about that, cast to `stdClass` before you send it.
- Recursive schemas are not supported by design. A self referencing `$ref` is reported as an error and removed, so a tree shaped input has to be flattened or capped by hand.
- `anyOf` and `oneOf` survive only in the nullable two branch case. A genuine discriminated union will be stripped and reported, which is the correct answer for tool input but does mean the sanitizer will not carry your union through for you.
- Sanitization is lossy on purpose. Descriptions, types and constraints are kept, but validation richness like `patternProperties` or conditional branching is gone. Keep your real server side validation.
