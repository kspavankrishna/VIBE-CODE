#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
VERSION="1.0.0"

COMMAND=""
INPUT_PATH=""
OUTPUT_DIR=""
REPORT_PATH=""
PROFILE="generic"
ID_KEY="custom_id"
MAX_SHARD_BYTES=$((45 * 1024 * 1024))
MAX_SHARD_LINES=5000
MAX_LINE_BYTES=$((2 * 1024 * 1024))
FAIL_ON_SECRETS=1
FAIL_ON_PAYLOAD_DUPLICATES=0

TMP_DIR=""
VALIDATED_FILE=""
IDS_FILE=""
PAYLOAD_HASHES_FILE=""
CANONICAL_BYTES_FILE=""
ERRORS_FILE=""
WARNINGS_FILE=""
SHARDS_FILE=""

REQUEST_COUNT=0
RAW_LOGICAL_BYTES=0
CANONICAL_LOGICAL_BYTES=0
MAX_OBSERVED_LINE_BYTES=0
SECRET_HIT_COUNT=0
DUPLICATE_ID_COUNT=0
DUPLICATE_PAYLOAD_COUNT=0
SHARD_COUNT=0

usage() {
  cat <<'EOF_USAGE'
JsonlBatchPreflight.sh

Validate and shard large JSONL request batches before they hit a provider upload API.
The script catches malformed lines, missing IDs, duplicate IDs, duplicate semantic
payloads, secret-like tokens, and request shapes that do not match the selected
provider profile.

Usage:
  JsonlBatchPreflight.sh check --input requests.jsonl [options]
  JsonlBatchPreflight.sh shard --input requests.jsonl --output-dir out [options]

Options:
  --input PATH                    Input JSONL or JSONL.GZ file. Use - for stdin.
  --output-dir DIR               Required for shard.
  --report PATH                  Optional JSON report path.
  --profile NAME                 generic | openai-batch | anthropic-batch.
  --id-key KEY                   Request ID field. Default: custom_id.
  --max-shard-bytes N            Default: 47185920 (45 MiB).
  --max-shard-lines N            Default: 5000.
  --max-line-bytes N             Default: 2097152 (2 MiB).
  --allow-secrets                Downgrade secret matches from error to warning.
  --fail-on-payload-duplicates   Treat duplicate semantic requests as errors.
  --help                         Show this help text.

Examples:
  JsonlBatchPreflight.sh check \
    --input evals.jsonl \
    --profile openai-batch

  JsonlBatchPreflight.sh shard \
    --input backfill.jsonl.gz \
    --output-dir dist \
    --profile anthropic-batch \
    --report dist/manifest.json
EOF_USAGE
}

die() {
  printf '%s: %s\n' "$SCRIPT_NAME" "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

validate_positive_integer() {
  local name="$1"
  local value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || die "$name must be an integer, got: $value"
  (( value > 0 )) || die "$name must be greater than zero, got: $value"
}

count_nonempty_lines() {
  local file="$1"
  awk 'NF { count += 1 } END { print count + 0 }' "$file"
}

join_first_lines() {
  local file="$1"
  local limit="$2"
  awk -v limit="$limit" '
    NF {
      out = out ? out ", " $0 : $0
      count += 1
      if (count >= limit) {
        print out
        exit
      }
    }
    END {
      if (count > 0 && count < limit) {
        print out
      }
    }
  ' "$file"
}

byte_count_text() {
  LC_ALL=C printf '%s' "$1" | wc -c | awk '{print $1}'
}

file_byte_count() {
  LC_ALL=C wc -c < "$1" | tr -d '[:space:]'
}

format_bytes() {
  local bytes="$1"
  awk -v bytes="$bytes" 'BEGIN {
    split("B KiB MiB GiB TiB", units, " ")
    idx = 1
    value = bytes + 0
    while (value >= 1024 && idx < 5) {
      value /= 1024
      idx += 1
    }
    if (idx == 1) {
      printf "%d %s", value, units[idx]
    } else {
      printf "%.2f %s", value, units[idx]
    }
  }'
}

sha256_text() {
  if command -v sha256sum >/dev/null 2>&1; then
    LC_ALL=C printf '%s' "$1" | sha256sum | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    LC_ALL=C printf '%s' "$1" | shasum -a 256 | awk '{print $1}'
  else
    LC_ALL=C printf '%s' "$1" | openssl dgst -sha256 | awk '{print $NF}'
  fi
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    openssl dgst -sha256 "$1" | awk '{print $NF}'
  fi
}

json_array_from_file() {
  local file="$1"
  if [[ -s "$file" ]]; then
    jq -Rsc 'split("\n") | map(select(length > 0))' < "$file"
  else
    printf '[]'
  fi
}

json_object_array_from_file() {
  local file="$1"
  if [[ -s "$file" ]]; then
    jq -s '.' < "$file"
  else
    printf '[]'
  fi
}

add_error() {
  printf '%s\n' "$*" >> "$ERRORS_FILE"
}

add_warning() {
  printf '%s\n' "$*" >> "$WARNINGS_FILE"
}

cleanup() {
  if [[ -n "$TMP_DIR" && -d "$TMP_DIR" ]]; then
    rm -rf "$TMP_DIR"
  fi
}

init_temp_files() {
  TMP_DIR="$(mktemp -d)"
  VALIDATED_FILE="$TMP_DIR/validated.jsonl"
  IDS_FILE="$TMP_DIR/ids.txt"
  PAYLOAD_HASHES_FILE="$TMP_DIR/payload_hashes.txt"
  CANONICAL_BYTES_FILE="$TMP_DIR/canonical_bytes.txt"
  ERRORS_FILE="$TMP_DIR/errors.txt"
  WARNINGS_FILE="$TMP_DIR/warnings.txt"
  SHARDS_FILE="$TMP_DIR/shards.jsonl"

  : > "$VALIDATED_FILE"
  : > "$IDS_FILE"
  : > "$PAYLOAD_HASHES_FILE"
  : > "$CANONICAL_BYTES_FILE"
  : > "$ERRORS_FILE"
  : > "$WARNINGS_FILE"
  : > "$SHARDS_FILE"

  trap cleanup EXIT
}

input_stem() {
  local base
  if [[ "$INPUT_PATH" == "-" ]]; then
    base="batch"
  else
    base="$(basename "$INPUT_PATH")"
  fi
  base="${base%.gz}"
  base="${base%.jsonl}"
  base="${base%.json}"
  [[ -n "$base" ]] || base="batch"
  printf '%s' "$base"
}

stream_input() {
  if [[ "$INPUT_PATH" == "-" ]]; then
    cat
  elif [[ "$INPUT_PATH" == *.gz ]]; then
    gzip -dc -- "$INPUT_PATH"
  else
    cat -- "$INPUT_PATH"
  fi
}

profile_filter() {
  case "$PROFILE" in
    generic)
      cat <<'EOF_FILTER'
((.[$id_key]? // "") | type == "string" and length > 0)
EOF_FILTER
      ;;
    openai-batch)
      cat <<'EOF_FILTER'
((.[$id_key]? // "") | type == "string" and length > 0)
and ((.method? // "") | type == "string" and ascii_downcase == "post")
and ((.url? // "") | type == "string" and test("^/v[0-9]+/"))
and ((.body? // null) | type == "object")
and ((.body.model? // "") | type == "string" and length > 0)
and (((.body? // {}) | (has("input") or has("messages") or has("prompt") or has("text") or has("instructions"))))
EOF_FILTER
      ;;
    anthropic-batch)
      cat <<'EOF_FILTER'
((.[$id_key]? // "") | type == "string" and length > 0)
and ((.params? // null) | type == "object")
and ((.params.model? // "") | type == "string" and length > 0)
and ((.params.max_tokens? // 0) | type == "number" and . > 0)
and ((((.params.messages? // null) | type == "array")) or (((.params.system? // "") | type == "string" and length > 0)))
EOF_FILTER
      ;;
    *)
      die "unsupported profile: $PROFILE"
      ;;
  esac
}

profile_hint() {
  case "$PROFILE" in
    generic)
      printf 'each line must be a JSON object with a non-empty %s string' "$ID_KEY"
      ;;
    openai-batch)
      printf 'expected OpenAI-style batch requests with %s, POST method, /vN URL, and body.model plus input/messages/prompt/text/instructions' "$ID_KEY"
      ;;
    anthropic-batch)
      printf 'expected Anthropic batch requests with %s and params.model, params.max_tokens, plus params.messages or params.system' "$ID_KEY"
      ;;
  esac
}

validate_profile_record() {
  local json="$1"
  local filter
  filter="$(profile_filter)"
  printf '%s\n' "$json" | jq -e --arg id_key "$ID_KEY" "$filter" >/dev/null 2>&1
}

extract_id() {
  printf '%s\n' "$1" | jq -r --arg id_key "$ID_KEY" '.[$id_key]'
}

payload_hash() {
  local stripped
  stripped="$(printf '%s\n' "$1" | jq -cS --arg id_key "$ID_KEY" 'del(.[$id_key])')"
  sha256_text "$stripped"
}

detect_secret_labels() {
  local text="$1"
  local hits=""

  if [[ "$text" =~ sk-[A-Za-z0-9]{20,} ]]; then
    hits="${hits:+$hits,}openai-key"
  fi
  if [[ "$text" =~ github_pat_[A-Za-z0-9_]{20,} ]]; then
    hits="${hits:+$hits,}github-pat"
  fi
  if [[ "$text" =~ gh[pousr]_[A-Za-z0-9]{20,} ]]; then
    hits="${hits:+$hits,}github-token"
  fi
  if [[ "$text" =~ AIza[0-9A-Za-z_-]{35} ]]; then
    hits="${hits:+$hits,}google-api-key"
  fi
  if [[ "$text" =~ (AKIA|ASIA)[0-9A-Z]{16} ]]; then
    hits="${hits:+$hits,}aws-access-key"
  fi
  if [[ "$text" =~ xox[baprs]-[0-9A-Za-z-]{10,} ]]; then
    hits="${hits:+$hits,}slack-token"
  fi
  if [[ "$text" =~ [Bb]earer[[:space:]]+[A-Za-z0-9._=-]{20,} ]]; then
    hits="${hits:+$hits,}bearer-token"
  fi
  if [[ "$text" =~ -----BEGIN[[:space:]][A-Z\ ]+PRIVATE[[:space:]]KEY----- ]]; then
    hits="${hits:+$hits,}private-key"
  fi

  printf '%s' "$hits"
}

validate_one_line() {
  local raw_line="$1"
  local line_no="$2"
  local raw_bytes
  local secret_labels
  local canonical
  local canonical_bytes
  local id_value
  local hash_value

  raw_bytes="$(byte_count_text "$raw_line")"
  RAW_LOGICAL_BYTES=$((RAW_LOGICAL_BYTES + raw_bytes + 1))
  if (( raw_bytes > MAX_OBSERVED_LINE_BYTES )); then
    MAX_OBSERVED_LINE_BYTES="$raw_bytes"
  fi

  if [[ -z "$raw_line" ]]; then
    add_error "line $line_no is blank; JSONL requires exactly one JSON object per line"
    return 0
  fi

  if (( raw_bytes > MAX_LINE_BYTES )); then
    add_error "line $line_no is $raw_bytes bytes, above --max-line-bytes=$MAX_LINE_BYTES"
    return 0
  fi

  secret_labels="$(detect_secret_labels "$raw_line")"
  if [[ -n "$secret_labels" ]]; then
    SECRET_HIT_COUNT=$((SECRET_HIT_COUNT + 1))
    if (( FAIL_ON_SECRETS )); then
      add_error "line $line_no matched secret-like patterns: $secret_labels"
      return 0
    fi
    add_warning "line $line_no matched secret-like patterns: $secret_labels"
  fi

  if ! canonical="$(printf '%s\n' "$raw_line" | jq -cS '.')"; then
    add_error "line $line_no is not valid JSON"
    return 0
  fi

  if ! printf '%s\n' "$canonical" | jq -e 'type == "object"' >/dev/null 2>&1; then
    add_error "line $line_no must be a JSON object"
    return 0
  fi

  if ! validate_profile_record "$canonical"; then
    add_error "line $line_no does not match profile $PROFILE: $(profile_hint)"
    return 0
  fi

  canonical_bytes=$(( $(byte_count_text "$canonical") + 1 ))
  if (( canonical_bytes > MAX_SHARD_BYTES )); then
    add_error "line $line_no expands to $canonical_bytes canonical bytes, above --max-shard-bytes=$MAX_SHARD_BYTES"
    return 0
  fi

  id_value="$(extract_id "$canonical")"
  if [[ "$id_value" == *$'\n'* || "$id_value" == *$'\r'* || "$id_value" == *$'\t'* ]]; then
    add_error "line $line_no has an unsafe $ID_KEY; tabs and newlines are not allowed"
    return 0
  fi

  hash_value="$(payload_hash "$canonical")"

  REQUEST_COUNT=$((REQUEST_COUNT + 1))
  CANONICAL_LOGICAL_BYTES=$((CANONICAL_LOGICAL_BYTES + canonical_bytes))

  printf '%s\n' "$canonical" >> "$VALIDATED_FILE"
  printf '%s\n' "$id_value" >> "$IDS_FILE"
  printf '%s\n' "$hash_value" >> "$PAYLOAD_HASHES_FILE"
  printf '%s\n' "$canonical_bytes" >> "$CANONICAL_BYTES_FILE"
}

validate_input() {
  local raw_line=""
  local line_no=0

  while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
    line_no=$((line_no + 1))
    validate_one_line "$raw_line" "$line_no"
  done < <(stream_input)

  if (( line_no == 0 )); then
    add_error "input is empty"
  fi
  if (( REQUEST_COUNT == 0 )); then
    add_error "no valid requests were found"
  fi
}

analyze_duplicates() {
  local duplicate_ids_file="$TMP_DIR/duplicate_ids.txt"
  local duplicate_payloads_file="$TMP_DIR/duplicate_payloads.txt"
  local sample=""

  if (( REQUEST_COUNT == 0 )); then
    return 0
  fi

  LC_ALL=C sort "$IDS_FILE" | uniq -d > "$duplicate_ids_file"
  DUPLICATE_ID_COUNT="$(count_nonempty_lines "$duplicate_ids_file")"
  if (( DUPLICATE_ID_COUNT > 0 )); then
    sample="$(join_first_lines "$duplicate_ids_file" 8)"
    add_error "duplicate $ID_KEY values detected ($DUPLICATE_ID_COUNT). Sample: $sample"
  fi

  LC_ALL=C sort "$PAYLOAD_HASHES_FILE" | uniq -d > "$duplicate_payloads_file"
  DUPLICATE_PAYLOAD_COUNT="$(count_nonempty_lines "$duplicate_payloads_file")"
  if (( DUPLICATE_PAYLOAD_COUNT > 0 )); then
    if (( FAIL_ON_PAYLOAD_DUPLICATES )); then
      add_error "duplicate semantic payloads detected ($DUPLICATE_PAYLOAD_COUNT hashes repeated after removing $ID_KEY)"
    else
      add_warning "duplicate semantic payloads detected ($DUPLICATE_PAYLOAD_COUNT hashes repeated after removing $ID_KEY)"
    fi
  fi
}

write_shards() {
  local base_name
  local current_file=""
  local current_lines=0
  local current_bytes=0
  local current_first_id=""
  local current_last_id=""
  local shard_index=0
  local record=""
  local record_id=""
  local record_bytes=0
  local shard_bytes=0
  local shard_sha=""

  mkdir -p "$OUTPUT_DIR"
  base_name="$(input_stem)"

  finalize_shard() {
    if [[ -z "$current_file" || ! -f "$current_file" || $current_lines -eq 0 ]]; then
      return 0
    fi

    shard_bytes="$(file_byte_count "$current_file")"
    shard_sha="$(sha256_file "$current_file")"
    printf '%s\n' "$(jq -cn \
      --arg path "$(basename "$current_file")" \
      --arg sha256 "$shard_sha" \
      --arg first_id "$current_first_id" \
      --arg last_id "$current_last_id" \
      --argjson request_count "$current_lines" \
      --argjson byte_count "$shard_bytes" \
      '{path: $path, sha256: $sha256, request_count: $request_count, byte_count: $byte_count, first_id: $first_id, last_id: $last_id}')" >> "$SHARDS_FILE"
  }

  exec 3< "$VALIDATED_FILE"
  exec 4< "$IDS_FILE"
  exec 5< "$CANONICAL_BYTES_FILE"

  while IFS= read -r record <&3 && IFS= read -r record_id <&4 && IFS= read -r record_bytes <&5; do
    if [[ -z "$current_file" ]]; then
      shard_index=$((shard_index + 1))
      current_file="$OUTPUT_DIR/${base_name}.part-$(printf '%04d' "$shard_index").jsonl"
      : > "$current_file"
      current_lines=0
      current_bytes=0
      current_first_id=""
      current_last_id=""
    fi

    if (( current_lines > 0 )) && (( current_lines >= MAX_SHARD_LINES || current_bytes + record_bytes > MAX_SHARD_BYTES )); then
      finalize_shard
      shard_index=$((shard_index + 1))
      current_file="$OUTPUT_DIR/${base_name}.part-$(printf '%04d' "$shard_index").jsonl"
      : > "$current_file"
      current_lines=0
      current_bytes=0
      current_first_id=""
      current_last_id=""
    fi

    if (( current_lines == 0 )); then
      current_first_id="$record_id"
    fi

    printf '%s\n' "$record" >> "$current_file"
    current_lines=$((current_lines + 1))
    current_bytes=$((current_bytes + record_bytes))
    current_last_id="$record_id"
  done

  exec 3<&-
  exec 4<&-
  exec 5<&-

  finalize_shard
  SHARD_COUNT="$shard_index"
}

build_report() {
  local report_path="$1"
  local warnings_json
  local errors_json
  local shards_json

  [[ -n "$report_path" ]] || return 0
  mkdir -p "$(dirname "$report_path")"

  warnings_json="$(json_array_from_file "$WARNINGS_FILE")"
  errors_json="$(json_array_from_file "$ERRORS_FILE")"
  shards_json="$(json_object_array_from_file "$SHARDS_FILE")"

  jq -n \
    --arg script "$SCRIPT_NAME" \
    --arg version "$VERSION" \
    --arg generated_at "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    --arg input "$INPUT_PATH" \
    --arg profile "$PROFILE" \
    --arg id_key "$ID_KEY" \
    --argjson request_count "$REQUEST_COUNT" \
    --argjson raw_logical_bytes "$RAW_LOGICAL_BYTES" \
    --argjson canonical_logical_bytes "$CANONICAL_LOGICAL_BYTES" \
    --argjson max_observed_line_bytes "$MAX_OBSERVED_LINE_BYTES" \
    --argjson max_shard_bytes "$MAX_SHARD_BYTES" \
    --argjson max_shard_lines "$MAX_SHARD_LINES" \
    --argjson max_line_bytes "$MAX_LINE_BYTES" \
    --argjson duplicate_id_count "$DUPLICATE_ID_COUNT" \
    --argjson duplicate_payload_count "$DUPLICATE_PAYLOAD_COUNT" \
    --argjson secret_hit_count "$SECRET_HIT_COUNT" \
    --argjson warning_count "$(count_nonempty_lines "$WARNINGS_FILE")" \
    --argjson error_count "$(count_nonempty_lines "$ERRORS_FILE")" \
    --argjson shard_count "$SHARD_COUNT" \
    --argjson warnings "$warnings_json" \
    --argjson errors "$errors_json" \
    --argjson shards "$shards_json" \
    '{
      script: $script,
      version: $version,
      generated_at_utc: $generated_at,
      input: $input,
      profile: $profile,
      id_key: $id_key,
      thresholds: {
        max_shard_bytes: $max_shard_bytes,
        max_shard_lines: $max_shard_lines,
        max_line_bytes: $max_line_bytes
      },
      summary: {
        request_count: $request_count,
        raw_logical_bytes: $raw_logical_bytes,
        canonical_logical_bytes: $canonical_logical_bytes,
        max_observed_line_bytes: $max_observed_line_bytes,
        duplicate_id_count: $duplicate_id_count,
        duplicate_payload_count: $duplicate_payload_count,
        secret_hit_count: $secret_hit_count,
        warning_count: $warning_count,
        error_count: $error_count,
        shard_count: $shard_count
      },
      warnings: $warnings,
      errors: $errors,
      shards: $shards
    }' > "$report_path"
}

print_issues() {
  local file="$1"
  local prefix="$2"
  local limit="$3"

  awk -v prefix="$prefix" -v limit="$limit" '
    NF {
      print prefix $0 > "/dev/stderr"
      count += 1
      if (count >= limit) {
        exit
      }
    }
  ' "$file"
}

print_summary() {
  printf 'profile: %s\n' "$PROFILE"
  printf 'requests: %s\n' "$REQUEST_COUNT"
  printf 'raw_logical_bytes: %s (%s)\n' "$RAW_LOGICAL_BYTES" "$(format_bytes "$RAW_LOGICAL_BYTES")"
  printf 'canonical_logical_bytes: %s (%s)\n' "$CANONICAL_LOGICAL_BYTES" "$(format_bytes "$CANONICAL_LOGICAL_BYTES")"
  printf 'max_observed_line_bytes: %s (%s)\n' "$MAX_OBSERVED_LINE_BYTES" "$(format_bytes "$MAX_OBSERVED_LINE_BYTES")"
  printf 'duplicate_id_count: %s\n' "$DUPLICATE_ID_COUNT"
  printf 'duplicate_payload_count: %s\n' "$DUPLICATE_PAYLOAD_COUNT"
  printf 'secret_hit_count: %s\n' "$SECRET_HIT_COUNT"
  printf 'warning_count: %s\n' "$(count_nonempty_lines "$WARNINGS_FILE")"
  printf 'error_count: %s\n' "$(count_nonempty_lines "$ERRORS_FILE")"
  if (( SHARD_COUNT > 0 )); then
    printf 'shard_count: %s\n' "$SHARD_COUNT"
  fi
  if [[ -n "$REPORT_PATH" ]]; then
    printf 'report: %s\n' "$REPORT_PATH"
  fi
}

parse_args() {
  if (( $# == 0 )); then
    usage
    exit 1
  fi

  COMMAND="$1"
  shift

  case "$COMMAND" in
    check|shard)
      ;;
    help|-h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown command: $COMMAND"
      ;;
  esac

  while (( $# > 0 )); do
    case "$1" in
      --input)
        (( $# >= 2 )) || die "--input requires a value"
        INPUT_PATH="$2"
        shift 2
        ;;
      --output-dir)
        (( $# >= 2 )) || die "--output-dir requires a value"
        OUTPUT_DIR="$2"
        shift 2
        ;;
      --report)
        (( $# >= 2 )) || die "--report requires a value"
        REPORT_PATH="$2"
        shift 2
        ;;
      --profile)
        (( $# >= 2 )) || die "--profile requires a value"
        PROFILE="$2"
        shift 2
        ;;
      --id-key)
        (( $# >= 2 )) || die "--id-key requires a value"
        ID_KEY="$2"
        shift 2
        ;;
      --max-shard-bytes)
        (( $# >= 2 )) || die "--max-shard-bytes requires a value"
        MAX_SHARD_BYTES="$2"
        shift 2
        ;;
      --max-shard-lines)
        (( $# >= 2 )) || die "--max-shard-lines requires a value"
        MAX_SHARD_LINES="$2"
        shift 2
        ;;
      --max-line-bytes)
        (( $# >= 2 )) || die "--max-line-bytes requires a value"
        MAX_LINE_BYTES="$2"
        shift 2
        ;;
      --allow-secrets)
        FAIL_ON_SECRETS=0
        shift
        ;;
      --fail-on-payload-duplicates)
        FAIL_ON_PAYLOAD_DUPLICATES=1
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done

  [[ -n "$INPUT_PATH" ]] || die "--input is required"
  [[ "$INPUT_PATH" == "-" || -f "$INPUT_PATH" ]] || die "input file does not exist: $INPUT_PATH"

  case "$PROFILE" in
    generic|openai-batch|anthropic-batch)
      ;;
    *)
      die "unsupported --profile value: $PROFILE"
      ;;
  esac

  validate_positive_integer "--max-shard-bytes" "$MAX_SHARD_BYTES"
  validate_positive_integer "--max-shard-lines" "$MAX_SHARD_LINES"
  validate_positive_integer "--max-line-bytes" "$MAX_LINE_BYTES"

  if [[ "$COMMAND" == "shard" ]]; then
    [[ -n "$OUTPUT_DIR" ]] || die "--output-dir is required for shard"
    if [[ -z "$REPORT_PATH" ]]; then
      REPORT_PATH="$OUTPUT_DIR/manifest.json"
    fi
  fi
}

main() {
  local error_count=0
  local warning_count=0

  parse_args "$@"
  require_command jq
  require_command awk
  require_command sort
  if [[ "$INPUT_PATH" == *.gz ]]; then
    require_command gzip
  fi
  if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1 && ! command -v openssl >/dev/null 2>&1; then
    die "missing required hashing command: sha256sum, shasum, or openssl"
  fi

  init_temp_files
  validate_input
  analyze_duplicates

  error_count="$(count_nonempty_lines "$ERRORS_FILE")"
  warning_count="$(count_nonempty_lines "$WARNINGS_FILE")"

  if [[ -n "$REPORT_PATH" && "$COMMAND" == "check" ]]; then
    build_report "$REPORT_PATH"
  fi

  if (( error_count > 0 )); then
    if [[ -n "$REPORT_PATH" && "$COMMAND" == "shard" ]]; then
      build_report "$REPORT_PATH"
    fi
    print_issues "$ERRORS_FILE" 'error: ' 20
    if (( warning_count > 0 )); then
      print_issues "$WARNINGS_FILE" 'warning: ' 20
    fi
    exit 1
  fi

  if [[ "$COMMAND" == "shard" ]]; then
    write_shards
    build_report "$REPORT_PATH"
  fi

  if (( warning_count > 0 )); then
    print_issues "$WARNINGS_FILE" 'warning: ' 20
  fi

  print_summary
}

main "$@"

# This solves a very current 2026 problem: developers upload huge JSONL batches for OpenAI Batch API, Anthropic batch inference, evaluation backfills, and synthetic data jobs, then discover the file was malformed only after waiting on a slow queue or burning real money. Built because the expensive failures are boring and preventable. The common causes are duplicate custom IDs, invalid one-line JSON records, accidental secrets inside prompt payloads, and upload files that should have been split before they ever reached CI or a provider endpoint.
# Built because I wanted one shell script that can sit in a GitHub Action, cron job, Airflow worker, or local terminal and do the ugly preflight work without forcing a full Python packaging story. It canonicalizes every JSON object with jq, validates provider-specific request shape, hashes the payload with the ID stripped out so semantic duplicates are visible, and emits stable shard files plus a manifest that downstream systems can trust.
# Use it when you are preparing offline eval runs, batch prompt migrations, embeddings backfills, large response-generation jobs, or any JSONL upload that can cost time, tokens, GPU budget, or incident cleanup if it is wrong. Use it before provider upload, before object storage publish, and before a CI pipeline marks a data artifact as ready.
# The trick: it treats JSONL as an artifact that deserves the same gatekeeping as a deployable binary. Every line becomes canonical JSON, every request gets checked against the selected profile, and every shard gets a deterministic SHA-256 plus first and last request IDs so retry workflows and incident debugging are not guesswork.
# Drop this into a repository that handles OpenAI batch processing, Anthropic message batches, LLM evaluation pipelines, JSONL dataset validation, or prompt backfill tooling. It is especially useful for teams searching for Bash JSONL validator, OpenAI Batch API preflight script, Anthropic batch request checker, duplicate custom_id detector, secret scanner for prompt payloads, or JSONL sharding manifest generator.
