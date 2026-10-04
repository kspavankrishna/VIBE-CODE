#!/usr/bin/env bash
# Runs the runtime suite, then proves every compile time guard actually rejects bad input.
set -u
cd "$(dirname "$0")"
fail=0

haxe -cp . -main McpToolSchemaTest --interp || fail=1

expect() {
  local name="$1" needle="$2"; shift 2
  local out
  if out=$(haxe -cp . -main McpToolSchemaNegative -D "case_$name" "$@" --interp 2>&1); then
    echo "FAIL case_$name compiled but should have been rejected"; fail=1
  elif grep -qF -- "$needle" <<<"$out"; then
    echo "ok   case_$name"
  else
    echo "FAIL case_$name wrong message:"; echo "$out"; fail=1
  fi
}

expect dynamic "is Dynamic and JSON Schema cannot describe it"
expect enum "Haxe enums have no JSON wire form"
expect function "function type"
expect range "no value can pass"
expect default "breaks its own schema"
expect target "cannot apply to a integer field"
expect typo "Unknown metadata :mcpMinimum"
expect pattern "unclosed ("
expect default_enum "breaks its own schema"
expect dupe_field "both serialize as"
expect dupe_tool "Duplicate tool name"
expect name "must match"
expect desc "needs a non empty description"
expect budget "over the 60 byte budget" -D mcp_schema_max_bytes=60
expect not_object "must be an object structure"
expect int_keys "keys are not String"

exit $fail
