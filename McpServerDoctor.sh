#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
SCRIPT_VERSION="1.0.0"

DEFAULT_PROTOCOL_VERSION="2025-11-25"
DEFAULT_TIMEOUT_SECONDS=8
DEFAULT_SAMPLE_RESOURCE_READS=2
DEFAULT_MAX_PAGES=5
DEFAULT_WIRE_FORMAT="newline"

PROTOCOL_VERSION="$DEFAULT_PROTOCOL_VERSION"
TIMEOUT_SECONDS="$DEFAULT_TIMEOUT_SECONDS"
SAMPLE_RESOURCE_READS="$DEFAULT_SAMPLE_RESOURCE_READS"
MAX_PAGES="$DEFAULT_MAX_PAGES"
WIRE_FORMAT="$DEFAULT_WIRE_FORMAT"
CLIENT_NAME="McpServerDoctor"
CLIENT_VERSION="$SCRIPT_VERSION"
REPORT_PATH=""
STRICT_MODE=0
VERBOSE=0
KEEP_ARTIFACTS=0
STDERR_TAIL_LINES=80

SERVER_CMD=()
TMP_DIR=""
STDERR_LOG=""
RAW_IN_LOG=""
RAW_OUT_LOG=""
SERVER_REQUEST_METHODS_FILE=""
SERVER_PID=""
FDS_OPEN=0
REQUEST_COUNTER=0
NOTIFICATION_COUNT=0
LOG_NOTIFICATION_COUNT=0
UNEXPECTED_SERVER_REQUESTS=0

INIT_RESPONSE=""
PING_RESPONSE=""
TOOLS_JSON='[]'
PROMPTS_JSON='[]'
RESOURCES_JSON='[]'
RESOURCE_TEMPLATES_JSON='[]'
READ_RESULTS_JSON='[]'
NEGOTIATED_PROTOCOL_VERSION=""
SERVER_INFO_NAME=""
SERVER_INFO_VERSION=""
SERVER_INSTRUCTIONS=""
LAST_RESPONSE=""

ERRORS=()
WARNINGS=()
INFOS=()

usage() {
  cat <<'EOF_USAGE'
McpServerDoctor.sh launches an MCP stdio server, performs the standard initialize
handshake, probes advertised capabilities, validates returned schemas, and emits
a CI-friendly summary plus an optional JSON report.

Usage:
  McpServerDoctor.sh [options] -- <server command> [args...]

Options:
  --protocol-version VERSION     Requested MCP protocol version. Default: 2025-11-25.
  --timeout SECONDS              Per-request timeout. Default: 8.
  --sample-resource-reads N      Read the first N listed resources. Default: 2.
  --max-pages N                  Maximum pages per paginated list request. Default: 5.
  --wire-format MODE             newline | content-length. Default: newline.
  --report PATH                  Write a JSON report to PATH.
  --stderr-tail-lines N          Include the last N stderr lines in terminal output. Default: 80.
  --strict                       Exit non-zero on warnings as well as errors.
  --verbose                      Print more runtime detail.
  --keep-artifacts               Keep temporary raw request/response logs.
  --help                         Show this help text.
  --version                      Print the script version.

Examples:
  McpServerDoctor.sh -- npx -y @modelcontextprotocol/server-filesystem /tmp
  McpServerDoctor.sh --report doctor.json -- node dist/server.js
  McpServerDoctor.sh --wire-format content-length -- python legacy_server.py
EOF_USAGE
}

die() {
  printf '%s: %s\n' "$SCRIPT_NAME" "$*" >&2
  exit 64
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

validate_positive_integer() {
  local name="$1"
  local value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || die "$name must be a positive integer, got: $value"
  (( value > 0 )) || die "$name must be greater than zero, got: $value"
}

validate_nonnegative_integer() {
  local name="$1"
  local value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || die "$name must be zero or a positive integer, got: $value"
}

add_error() {
  ERRORS[${#ERRORS[@]}]="$*"
}

add_warning() {
  WARNINGS[${#WARNINGS[@]}]="$*"
}

add_info() {
  INFOS[${#INFOS[@]}]="$*"
}

now_iso8601() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

command_to_string() {
  local rendered=""
  local arg
  for arg in "$@"; do
    if [ -n "$rendered" ]; then
      rendered+=" "
    fi
    case "$arg" in
      *[!A-Za-z0-9_./:=,@%+-]*)
        rendered+="'$(printf '%s' "$arg" | sed "s/'/'\\''/g")'"
        ;;
      *)
        rendered+="$arg"
        ;;
    esac
  done
  printf '%s' "$rendered"
}

json_escape() {
  local value="$1"
  value=${value//\\/\\\\}
  value=${value//\"/\\\"}
  value=${value//$'\n'/\\n}
  value=${value//$'\r'/\\r}
  value=${value//$'\t'/\\t}
  printf '%s' "$value"
}

json_array_from_lines() {
  local file="$1"
  if [ -s "$file" ]; then
    jq -Rsc 'split("\n") | map(select(length > 0))' < "$file"
  else
    printf '[]'
  fi
}

json_array_from_args() {
  if [ "$#" -eq 0 ]; then
    printf '[]'
  else
    printf '%s\n' "$@" | jq -Rsc 'split("\n") | map(select(length > 0))'
  fi
}

server_is_running() {
  [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null
}

close_channel_fds() {
  if [ "$FDS_OPEN" -eq 1 ]; then
    exec 3>&- 2>/dev/null || true
    exec 3<&- 2>/dev/null || true
    exec 4>&- 2>/dev/null || true
    exec 4<&- 2>/dev/null || true
    FDS_OPEN=0
  fi
}

terminate_server() {
  if ! server_is_running; then
    return 0
  fi

  close_channel_fds
  sleep 0.1

  if server_is_running; then
    kill "$SERVER_PID" 2>/dev/null || true
    sleep 0.2
  fi

  if server_is_running; then
    kill -9 "$SERVER_PID" 2>/dev/null || true
    sleep 0.1
  fi
}

cleanup() {
  set +e
  terminate_server
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ] && [ "$KEEP_ARTIFACTS" -ne 1 ]; then
    rm -rf "$TMP_DIR"
  fi
}

trap cleanup EXIT

start_server() {
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mcp-server-doctor.XXXXXX")"
  STDERR_LOG="$TMP_DIR/server.stderr.log"
  RAW_IN_LOG="$TMP_DIR/client.requests.jsonl"
  RAW_OUT_LOG="$TMP_DIR/server.messages.jsonl"
  SERVER_REQUEST_METHODS_FILE="$TMP_DIR/server.requests.txt"

  : > "$STDERR_LOG"
  : > "$RAW_IN_LOG"
  : > "$RAW_OUT_LOG"
  : > "$SERVER_REQUEST_METHODS_FILE"

  mkfifo "$TMP_DIR/server.stdin" "$TMP_DIR/server.stdout"
  exec 3<> "$TMP_DIR/server.stdin"
  exec 4<> "$TMP_DIR/server.stdout"
  FDS_OPEN=1

  (
    "${SERVER_CMD[@]}"
  ) < "$TMP_DIR/server.stdin" > "$TMP_DIR/server.stdout" 2> "$STDERR_LOG" &
  SERVER_PID=$!

  sleep 0.05
  if ! server_is_running; then
    add_error "server exited immediately after launch"
    return 1
  fi

  add_info "launched server pid $SERVER_PID"
  return 0
}

compact_json() {
  printf '%s' "$1" | jq -c .
}

write_wire_message() {
  local compact="$1"
  case "$WIRE_FORMAT" in
    newline)
      printf '%s\n' "$compact" >&3
      ;;
    content-length)
      local length
      length="$(LC_ALL=C printf '%s' "$compact" | wc -c | awk '{print $1}')"
      printf 'Content-Length: %s\r\n\r\n%s' "$length" "$compact" >&3
      ;;
    *)
      die "unsupported wire format: $WIRE_FORMAT"
      ;;
  esac
}

send_json_message() {
  local message="$1"
  local compact
  if ! compact="$(compact_json "$message" 2>/dev/null)"; then
    add_error "client attempted to send invalid JSON"
    return 1
  fi

  printf '%s\n' "$compact" >> "$RAW_IN_LOG"
  write_wire_message "$compact"
  return 0
}

build_request_json() {
  local id="$1"
  local method="$2"
  local params_json="$3"
  jq -cn --arg id "$id" --arg method "$method" --argjson params "$params_json" '{jsonrpc:"2.0", id:$id, method:$method, params:$params}'
}

build_notification_json() {
  local method="$1"
  local params_json="$2"
  jq -cn --arg method "$method" --argjson params "$params_json" '{jsonrpc:"2.0", method:$method, params:$params}'
}

build_error_response_json() {
  local id="$1"
  local code="$2"
  local message="$3"
  jq -cn --arg id "$id" --argjson code "$code" --arg message "$message" '{jsonrpc:"2.0", id:$id, error:{code:$code, message:$message}}'
}

send_notification() {
  local method="$1"
  local params_json="$2"
  local payload
  payload="$(build_notification_json "$method" "$params_json")"
  send_json_message "$payload"
}

send_request() {
  local method="$1"
  local params_json="$2"
  local id payload response

  REQUEST_COUNTER=$((REQUEST_COUNTER + 1))
  id="doctor-$REQUEST_COUNTER"
  payload="$(build_request_json "$id" "$method" "$params_json")"

  send_json_message "$payload" || return 1
  response="$(await_response "$id" "$TIMEOUT_SECONDS")" || return 1
  LAST_RESPONSE="$response"
  return 0
}

parse_content_length_message() {
  local first_line="$1"
  local timeout="$2"
  local line length body

  line="${first_line%$'\r'}"
  if [[ ! "$line" =~ ^[Cc]ontent-[Ll]ength:[[:space:]]*([0-9]+)$ ]]; then
    add_error "server emitted an unexpected framing line: $line"
    return 2
  fi
  length="${BASH_REMATCH[1]}"

  while IFS= read -r -t "$timeout" -u 4 line; do
    line="${line%$'\r'}"
    [ -z "$line" ] && break
  done

  body="$(dd bs=1 count="$length" <&4 2>/dev/null)"
  printf '%s\n' "$body" >> "$RAW_OUT_LOG"

  if ! printf '%s' "$body" | jq -e . >/dev/null 2>&1; then
    add_error "server wrote invalid JSON in a Content-Length frame"
    return 2
  fi

  printf '%s' "$body"
  return 0
}

receive_message() {
  local timeout="$1"
  local line

  if ! IFS= read -r -t "$timeout" -u 4 line; then
    return 1
  fi

  line="${line%$'\r'}"
  while [ -z "$line" ]; do
    if ! IFS= read -r -t "$timeout" -u 4 line; then
      return 1
    fi
    line="${line%$'\r'}"
  done

  case "$line" in
    Content-Length:*|content-length:*)
      parse_content_length_message "$line" "$timeout"
      return $?
      ;;
  esac

  printf '%s\n' "$line" >> "$RAW_OUT_LOG"
  if ! printf '%s' "$line" | jq -e . >/dev/null 2>&1; then
    add_error "server wrote a non-JSON line to stdout: $line"
    return 2
  fi

  printf '%s' "$line"
  return 0
}

handle_unsolicited_message() {
  local message="$1"
  local has_method has_id method request_id level data

  has_method="$(printf '%s' "$message" | jq -r 'has("method")')"
  has_id="$(printf '%s' "$message" | jq -r 'has("id")')"

  if [ "$has_method" = "true" ] && [ "$has_id" = "false" ]; then
    NOTIFICATION_COUNT=$((NOTIFICATION_COUNT + 1))
    method="$(printf '%s' "$message" | jq -r '.method // ""')"
    case "$method" in
      notifications/message)
        LOG_NOTIFICATION_COUNT=$((LOG_NOTIFICATION_COUNT + 1))
        level="$(printf '%s' "$message" | jq -r '.params.level // "info"')"
        data="$(printf '%s' "$message" | jq -c '.params.data // null')"
        add_info "server log notification [$level]: $data"
        ;;
      notifications/tools/list_changed|notifications/resources/list_changed|notifications/prompts/list_changed)
        add_info "server emitted $method"
        ;;
      *)
        add_info "server emitted notification $method"
        ;;
    esac
    return 0
  fi

  if [ "$has_method" = "true" ] && [ "$has_id" = "true" ]; then
    UNEXPECTED_SERVER_REQUESTS=$((UNEXPECTED_SERVER_REQUESTS + 1))
    method="$(printf '%s' "$message" | jq -r '.method // ""')"
    request_id="$(printf '%s' "$message" | jq -r '.id')"
    printf '%s\n' "$method" >> "$SERVER_REQUEST_METHODS_FILE"
    add_warning "server issued an unexpected server-to-client request: $method"
    send_json_message "$(build_error_response_json "$request_id" -32601 "McpServerDoctor does not implement server-initiated requests")" || true
    return 0
  fi

  if [ "$has_method" = "false" ] && [ "$has_id" = "true" ]; then
    add_warning "received a response for a different request id while waiting: $(printf '%s' "$message" | jq -r '.id')"
    return 0
  fi

  add_warning "received an unclassifiable JSON-RPC message from the server"
  return 0
}

await_response() {
  local wanted_id="$1"
  local timeout="$2"
  local deadline remaining message message_id

  deadline=$(( $(date +%s) + timeout ))

  while :; do
    remaining=$(( deadline - $(date +%s) ))
    if [ "$remaining" -le 0 ]; then
      add_error "timed out waiting for response id $wanted_id"
      return 1
    fi

    if ! message="$(receive_message "$remaining")"; then
      if server_is_running; then
        add_error "timed out waiting for server output while awaiting response id $wanted_id"
      else
        add_error "server exited while waiting for response id $wanted_id"
      fi
      return 1
    fi

    message_id="$(printf '%s' "$message" | jq -r 'if has("id") then .id else empty end')"
    if [ "$message_id" = "$wanted_id" ]; then
      printf '%s' "$message"
      return 0
    fi

    handle_unsolicited_message "$message"
  done
}

extract_error_text() {
  local response="$1"
  printf '%s' "$response" | jq -r '.error.code as $code | .error.message as $message | if .error.data.supported? then ($message + " (supported: " + (.error.data.supported | join(", ")) + ")") else ($message + " (code " + ($code|tostring) + ")") end' 2>/dev/null || printf 'unknown error'
}

validate_initialize_result() {
  local response="$1"

  if printf '%s' "$response" | jq -e '.result.protocolVersion | strings | length > 0' >/dev/null 2>&1; then
    NEGOTIATED_PROTOCOL_VERSION="$(printf '%s' "$response" | jq -r '.result.protocolVersion')"
  else
    add_error "initialize response did not include result.protocolVersion"
  fi

  SERVER_INFO_NAME="$(printf '%s' "$response" | jq -r '.result.serverInfo.name // empty')"
  SERVER_INFO_VERSION="$(printf '%s' "$response" | jq -r '.result.serverInfo.version // empty')"
  SERVER_INSTRUCTIONS="$(printf '%s' "$response" | jq -r '.result.instructions // empty')"

  if [ -z "$SERVER_INFO_NAME" ]; then
    add_error "initialize response did not include result.serverInfo.name"
  fi
  if [ -z "$SERVER_INFO_VERSION" ]; then
    add_error "initialize response did not include result.serverInfo.version"
  fi
  if ! printf '%s' "$response" | jq -e '.result.capabilities | type == "object"' >/dev/null 2>&1; then
    add_error "initialize response did not include a capabilities object"
  fi

  if [ -n "$NEGOTIATED_PROTOCOL_VERSION" ] && [ "$NEGOTIATED_PROTOCOL_VERSION" != "$PROTOCOL_VERSION" ]; then
    add_info "server negotiated protocol $NEGOTIATED_PROTOCOL_VERSION instead of requested $PROTOCOL_VERSION"
  fi
}

probe_initialize() {
  local params
  params="$(jq -cn --arg protocolVersion "$PROTOCOL_VERSION" --arg clientName "$CLIENT_NAME" --arg clientVersion "$CLIENT_VERSION" '{protocolVersion:$protocolVersion, capabilities:{}, clientInfo:{name:$clientName, version:$clientVersion}}')"

  if ! send_request "initialize" "$params"; then
    return 1
  fi
  INIT_RESPONSE="$LAST_RESPONSE"

  if printf '%s' "$INIT_RESPONSE" | jq -e 'has("error")' >/dev/null 2>&1; then
    add_error "initialize failed: $(extract_error_text "$INIT_RESPONSE")"
    return 1
  fi

  validate_initialize_result "$INIT_RESPONSE"
  send_notification "notifications/initialized" '{}' || add_error "failed to send notifications/initialized"
  return 0
}

probe_ping() {
  if ! send_request "ping" '{}'; then
    add_warning "ping request did not complete"
    return 1
  fi
  PING_RESPONSE="$LAST_RESPONSE"
  if printf '%s' "$PING_RESPONSE" | jq -e 'has("error")' >/dev/null 2>&1; then
    add_warning "ping failed: $(extract_error_text "$PING_RESPONSE")"
    return 1
  fi
  add_info "ping succeeded"
  return 0
}

server_advertises_capability() {
  local jq_expr="$1"
  printf '%s' "$INIT_RESPONSE" | jq -e "$jq_expr" >/dev/null 2>&1
}

collect_paginated() {
  local method="$1"
  local result_key="$2"
  local target_var="$3"
  local cursor=""
  local page=1
  local response page_items next_cursor combined seen_file

  combined='[]'
  seen_file="$TMP_DIR/$(printf '%s' "$method" | tr '/:' '__').cursors"
  : > "$seen_file"

  while [ "$page" -le "$MAX_PAGES" ]; do
    local params='{}'
    if [ -n "$cursor" ]; then
      params="$(jq -cn --arg cursor "$cursor" '{cursor:$cursor}')"
    fi

    if ! send_request "$method" "$params"; then
      return 1
    fi
    response="$LAST_RESPONSE"

    if printf '%s' "$response" | jq -e 'has("error")' >/dev/null 2>&1; then
      add_error "$method failed: $(extract_error_text "$response")"
      return 1
    fi

    page_items="$(printf '%s' "$response" | jq -c ".result.${result_key} // []")"
    combined="$(jq -cn --argjson a "$combined" --argjson b "$page_items" '$a + $b')"
    next_cursor="$(printf '%s' "$response" | jq -r '.result.nextCursor // empty')"

    if [ -z "$next_cursor" ]; then
      break
    fi

    if grep -Fxq "$next_cursor" "$seen_file"; then
      add_error "$method returned a repeated nextCursor value: $next_cursor"
      break
    fi

    printf '%s\n' "$next_cursor" >> "$seen_file"
    cursor="$next_cursor"
    page=$((page + 1))
  done

  if [ "$page" -gt "$MAX_PAGES" ] && [ -n "$cursor" ]; then
    add_warning "$method reached the max page limit of $MAX_PAGES"
  fi

  printf -v "$target_var" '%s' "$combined"
}

validate_tool_collection() {
  local json="$1"
  local tool_count duplicate required_missing name schema_type output_schema_type description

  tool_count="$(printf '%s' "$json" | jq 'length')"
  add_info "tools discovered: $tool_count"

  while IFS= read -r duplicate; do
    [ -n "$duplicate" ] && add_error "duplicate tool name: $duplicate"
  done < <(printf '%s' "$json" | jq -r 'sort_by(.name) | group_by(.name)[] | select(length > 1) | .[0].name')

  while IFS= read -r tool; do
    name="$(printf '%s' "$tool" | jq -r '.name // empty')"
    description="$(printf '%s' "$tool" | jq -r '.description // empty')"
    schema_type="$(printf '%s' "$tool" | jq -r '.inputSchema.type // empty')"
    output_schema_type="$(printf '%s' "$tool" | jq -r '.outputSchema.type // empty')"

    if [ -z "$name" ]; then
      add_error "encountered a tool with an empty name"
      continue
    fi

    if [ "$schema_type" != "object" ]; then
      add_error "tool $name does not expose inputSchema.type=object"
    fi

    if [ -n "$output_schema_type" ] && [ "$output_schema_type" != "object" ]; then
      add_error "tool $name has outputSchema.type=$output_schema_type instead of object"
    fi

    if [ -z "$description" ]; then
      add_warning "tool $name does not include a description"
    fi

    while IFS= read -r required_missing; do
      [ -n "$required_missing" ] && add_error "tool $name marks '$required_missing' as required but does not define it in inputSchema.properties"
    done < <(printf '%s' "$tool" | jq -r '((.inputSchema.required // []) - ((.inputSchema.properties // {}) | keys))[]?')
  done < <(printf '%s' "$json" | jq -c '.[]')
}

validate_resource_collection() {
  local json="$1"
  local count duplicate uri name mime size

  count="$(printf '%s' "$json" | jq 'length')"
  add_info "resources discovered: $count"

  while IFS= read -r duplicate; do
    [ -n "$duplicate" ] && add_error "duplicate resource uri: $duplicate"
  done < <(printf '%s' "$json" | jq -r 'sort_by(.uri) | group_by(.uri)[] | select(length > 1) | .[0].uri')

  while IFS= read -r resource; do
    name="$(printf '%s' "$resource" | jq -r '.name // empty')"
    uri="$(printf '%s' "$resource" | jq -r '.uri // empty')"
    mime="$(printf '%s' "$resource" | jq -r '.mimeType // empty')"
    size="$(printf '%s' "$resource" | jq -r '.size // empty')"

    if [ -z "$name" ]; then
      add_error "encountered a resource with an empty name"
    fi
    if [ -z "$uri" ]; then
      add_error "resource '$name' is missing a uri"
    elif [[ ! "$uri" =~ ^[A-Za-z][A-Za-z0-9+.-]*: ]]; then
      add_warning "resource '$name' has a uri without an obvious scheme: $uri"
    fi

    if [ -n "$mime" ] && [[ "$mime" != */* ]]; then
      add_warning "resource '$name' has an unusual mimeType: $mime"
    fi

    if [ -n "$size" ] && ! printf '%s' "$resource" | jq -e '.size | type == "number" and . >= 0' >/dev/null 2>&1; then
      add_warning "resource '$name' has a non-numeric or negative size"
    fi
  done < <(printf '%s' "$json" | jq -c '.[]')
}

validate_resource_template_collection() {
  local json="$1"
  local count duplicate name template mime

  count="$(printf '%s' "$json" | jq 'length')"
  add_info "resource templates discovered: $count"

  while IFS= read -r duplicate; do
    [ -n "$duplicate" ] && add_error "duplicate resource template uriTemplate: $duplicate"
  done < <(printf '%s' "$json" | jq -r 'sort_by(.uriTemplate) | group_by(.uriTemplate)[] | select(length > 1) | .[0].uriTemplate')

  while IFS= read -r resource_template; do
    name="$(printf '%s' "$resource_template" | jq -r '.name // empty')"
    template="$(printf '%s' "$resource_template" | jq -r '.uriTemplate // empty')"
    mime="$(printf '%s' "$resource_template" | jq -r '.mimeType // empty')"

    if [ -z "$name" ]; then
      add_error "encountered a resource template with an empty name"
    fi
    if [ -z "$template" ]; then
      add_error "resource template '$name' is missing uriTemplate"
    fi
    if [ -n "$mime" ] && [[ "$mime" != */* ]]; then
      add_warning "resource template '$name' has an unusual mimeType: $mime"
    fi
  done < <(printf '%s' "$json" | jq -c '.[]')
}

validate_prompt_collection() {
  local json="$1"
  local count duplicate prompt name argument

  count="$(printf '%s' "$json" | jq 'length')"
  add_info "prompts discovered: $count"

  while IFS= read -r duplicate; do
    [ -n "$duplicate" ] && add_error "duplicate prompt name: $duplicate"
  done < <(printf '%s' "$json" | jq -r 'sort_by(.name) | group_by(.name)[] | select(length > 1) | .[0].name')

  while IFS= read -r prompt; do
    name="$(printf '%s' "$prompt" | jq -r '.name // empty')"
    if [ -z "$name" ]; then
      add_error "encountered a prompt with an empty name"
      continue
    fi

    while IFS= read -r argument; do
      [ -n "$argument" ] && add_error "prompt $name declares duplicate argument '$argument'"
    done < <(printf '%s' "$prompt" | jq -r '(.arguments // []) | sort_by(.name) | group_by(.name)[] | select(length > 1) | .[0].name')

    if ! printf '%s' "$prompt" | jq -e 'all((.arguments // [])[]?; (.required // false | type) == "boolean")' >/dev/null 2>&1; then
      add_warning "prompt $name has a non-boolean required flag in arguments"
    fi
  done < <(printf '%s' "$json" | jq -c '.[]')
}

validate_read_result() {
  local uri="$1"
  local response="$2"

  if ! printf '%s' "$response" | jq -e '.result.contents | type == "array" and length >= 1' >/dev/null 2>&1; then
    add_error "resources/read for $uri did not return a non-empty contents array"
    return 1
  fi

  if ! printf '%s' "$response" | jq -e 'all(.result.contents[]; (.uri | type == "string") and (((has("text")|not) != (has("blob")|not))))' >/dev/null 2>&1; then
    add_error "resources/read for $uri returned content items without a valid uri/text-or-blob shape"
    return 1
  fi

  return 0
}

sample_resource_reads() {
  local uri response result
  local sampled=0
  local combined='[]'

  if [ "$SAMPLE_RESOURCE_READS" -le 0 ]; then
    add_info "resource read sampling disabled"
    READ_RESULTS_JSON='[]'
    return 0
  fi

  while IFS= read -r uri; do
    [ -z "$uri" ] && continue

    if ! send_request "resources/read" "$(jq -cn --arg uri "$uri" '{uri:$uri}')"; then
      add_error "resources/read failed for $uri"
      sampled=$((sampled + 1))
      [ "$sampled" -ge "$SAMPLE_RESOURCE_READS" ] && break
      continue
    fi

    response="$LAST_RESPONSE"
    if printf '%s' "$response" | jq -e 'has("error")' >/dev/null 2>&1; then
      add_error "resources/read failed for $uri: $(extract_error_text "$response")"
      sampled=$((sampled + 1))
      [ "$sampled" -ge "$SAMPLE_RESOURCE_READS" ] && break
      continue
    fi

    validate_read_result "$uri" "$response"
    result="$(printf '%s' "$response" | jq -c '.result')"
    combined="$(jq -cn --argjson a "$combined" --argjson b "$result" '$a + [$b]')"
    sampled=$((sampled + 1))
    [ "$sampled" -ge "$SAMPLE_RESOURCE_READS" ] && break
  done < <(printf '%s' "$RESOURCES_JSON" | jq -r '.[].uri')

  READ_RESULTS_JSON="$combined"
  if [ "$sampled" -eq 0 ]; then
    add_info "no listed resources were available for resources/read sampling"
  else
    add_info "sampled resources/read for $sampled resource(s)"
  fi
}

diagnose_stderr() {
  if [ ! -s "$STDERR_LOG" ]; then
    return 0
  fi

  add_info "server wrote $(wc -l < "$STDERR_LOG" | awk '{print $1}') stderr line(s)"

  if grep -Eiq '(^|[^A-Za-z])(panic|traceback|unhandled|fatal|segmentation fault|exception)([^A-Za-z]|$)' "$STDERR_LOG"; then
    add_warning "server stderr contains panic/exception-like text"
  fi
}

write_report() {
  [ -n "$REPORT_PATH" ] || return 0

  local errors_json warnings_json infos_json server_requests_json
  mkdir -p "$(dirname "$REPORT_PATH")"
  errors_json="$(json_array_from_args "${ERRORS[@]}")"
  warnings_json="$(json_array_from_args "${WARNINGS[@]}")"
  infos_json="$(json_array_from_args "${INFOS[@]}")"
  server_requests_json="$(json_array_from_lines "$SERVER_REQUEST_METHODS_FILE")"

  jq -n \
    --arg generatedAt "$(now_iso8601)" \
    --arg scriptVersion "$SCRIPT_VERSION" \
    --arg requestedProtocol "$PROTOCOL_VERSION" \
    --arg negotiatedProtocol "$NEGOTIATED_PROTOCOL_VERSION" \
    --arg serverName "$SERVER_INFO_NAME" \
    --arg serverVersion "$SERVER_INFO_VERSION" \
    --arg instructions "$SERVER_INSTRUCTIONS" \
    --arg command "$(command_to_string "${SERVER_CMD[@]}")" \
    --arg wireFormat "$WIRE_FORMAT" \
    --arg stderrLog "$STDERR_LOG" \
    --arg requestLog "$RAW_IN_LOG" \
    --arg responseLog "$RAW_OUT_LOG" \
    --argjson strictMode "$STRICT_MODE" \
    --argjson notificationCount "$NOTIFICATION_COUNT" \
    --argjson logNotificationCount "$LOG_NOTIFICATION_COUNT" \
    --argjson unexpectedServerRequests "$UNEXPECTED_SERVER_REQUESTS" \
    --argjson tools "$TOOLS_JSON" \
    --argjson prompts "$PROMPTS_JSON" \
    --argjson resources "$RESOURCES_JSON" \
    --argjson resourceTemplates "$RESOURCE_TEMPLATES_JSON" \
    --argjson sampledReads "$READ_RESULTS_JSON" \
    --argjson errors "$errors_json" \
    --argjson warnings "$warnings_json" \
    --argjson infos "$infos_json" \
    --argjson serverRequests "$server_requests_json" \
    '{
      generatedAt: $generatedAt,
      scriptVersion: $scriptVersion,
      command: $command,
      wireFormat: $wireFormat,
      protocol: {
        requested: $requestedProtocol,
        negotiated: $negotiatedProtocol
      },
      server: {
        name: $serverName,
        version: $serverVersion,
        instructions: $instructions
      },
      counts: {
        tools: ($tools | length),
        prompts: ($prompts | length),
        resources: ($resources | length),
        resourceTemplates: ($resourceTemplates | length),
        sampledReads: ($sampledReads | length),
        notifications: $notificationCount,
        logNotifications: $logNotificationCount,
        unexpectedServerRequests: $unexpectedServerRequests
      },
      findings: {
        errors: $errors,
        warnings: $warnings,
        infos: $infos
      },
      artifacts: {
        stderrLog: $stderrLog,
        requestLog: $requestLog,
        responseLog: $responseLog
      },
      serverRequests: $serverRequests,
      strictMode: ($strictMode == 1),
      payloads: {
        tools: $tools,
        prompts: $prompts,
        resources: $resources,
        resourceTemplates: $resourceTemplates,
        sampledReads: $sampledReads
      }
    }' > "$REPORT_PATH"
}

print_stderr_tail() {
  if [ ! -s "$STDERR_LOG" ]; then
    return 0
  fi

  printf '\nServer stderr (last %s lines):\n' "$STDERR_TAIL_LINES"
  tail -n "$STDERR_TAIL_LINES" "$STDERR_LOG"
}

print_list() {
  local label="$1"
  shift
  local item
  for item in "$@"; do
    printf '  - %s\n' "$item"
  done
}

print_summary() {
  local status="OK"
  if [ "${#ERRORS[@]}" -gt 0 ]; then
    status="FAIL"
  elif [ "${#WARNINGS[@]}" -gt 0 ]; then
    status="WARN"
  fi

  printf '%s %s\n' "$SCRIPT_NAME" "$status"
  if [ -n "$SERVER_INFO_NAME" ] || [ -n "$SERVER_INFO_VERSION" ]; then
    printf 'Server: %s %s\n' "$SERVER_INFO_NAME" "$SERVER_INFO_VERSION"
  fi
  if [ -n "$NEGOTIATED_PROTOCOL_VERSION" ]; then
    printf 'Protocol: requested %s, negotiated %s\n' "$PROTOCOL_VERSION" "$NEGOTIATED_PROTOCOL_VERSION"
  else
    printf 'Protocol: requested %s\n' "$PROTOCOL_VERSION"
  fi
  printf 'Counts: %s tools, %s prompts, %s resources, %s templates\n' \
    "$(printf '%s' "$TOOLS_JSON" | jq 'length')" \
    "$(printf '%s' "$PROMPTS_JSON" | jq 'length')" \
    "$(printf '%s' "$RESOURCES_JSON" | jq 'length')" \
    "$(printf '%s' "$RESOURCE_TEMPLATES_JSON" | jq 'length')"
  printf 'Notifications: %s total, %s log messages, %s server requests\n' \
    "$NOTIFICATION_COUNT" "$LOG_NOTIFICATION_COUNT" "$UNEXPECTED_SERVER_REQUESTS"

  if [ "${#ERRORS[@]}" -gt 0 ]; then
    printf '\nErrors:\n'
    print_list "Errors" "${ERRORS[@]}"
  fi
  if [ "${#WARNINGS[@]}" -gt 0 ]; then
    printf '\nWarnings:\n'
    print_list "Warnings" "${WARNINGS[@]}"
  fi
  if [ "$VERBOSE" -eq 1 ] && [ "${#INFOS[@]}" -gt 0 ]; then
    printf '\nNotes:\n'
    print_list "Notes" "${INFOS[@]}"
  fi
  if [ -n "$REPORT_PATH" ]; then
    printf '\nReport: %s\n' "$REPORT_PATH"
  fi
  if [ "$KEEP_ARTIFACTS" -eq 1 ]; then
    printf 'Artifacts: %s\n' "$TMP_DIR"
  fi
}

main() {
  require_command jq

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --protocol-version)
        [ "$#" -ge 2 ] || die "--protocol-version requires a value"
        PROTOCOL_VERSION="$2"
        shift 2
        ;;
      --timeout)
        [ "$#" -ge 2 ] || die "--timeout requires a value"
        TIMEOUT_SECONDS="$2"
        shift 2
        ;;
      --sample-resource-reads)
        [ "$#" -ge 2 ] || die "--sample-resource-reads requires a value"
        SAMPLE_RESOURCE_READS="$2"
        shift 2
        ;;
      --max-pages)
        [ "$#" -ge 2 ] || die "--max-pages requires a value"
        MAX_PAGES="$2"
        shift 2
        ;;
      --wire-format)
        [ "$#" -ge 2 ] || die "--wire-format requires a value"
        WIRE_FORMAT="$2"
        shift 2
        ;;
      --report)
        [ "$#" -ge 2 ] || die "--report requires a path"
        REPORT_PATH="$2"
        shift 2
        ;;
      --stderr-tail-lines)
        [ "$#" -ge 2 ] || die "--stderr-tail-lines requires a value"
        STDERR_TAIL_LINES="$2"
        shift 2
        ;;
      --strict)
        STRICT_MODE=1
        shift
        ;;
      --verbose)
        VERBOSE=1
        shift
        ;;
      --keep-artifacts)
        KEEP_ARTIFACTS=1
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      --version)
        printf '%s %s\n' "$SCRIPT_NAME" "$SCRIPT_VERSION"
        exit 0
        ;;
      --)
        shift
        SERVER_CMD=("$@")
        break
        ;;
      *)
        die "unknown option: $1"
        ;;
    esac
  done

  [ "${#SERVER_CMD[@]}" -gt 0 ] || die "missing server command; pass it after --"

  validate_positive_integer "timeout" "$TIMEOUT_SECONDS"
  validate_nonnegative_integer "sample-resource-reads" "$SAMPLE_RESOURCE_READS"
  validate_positive_integer "max-pages" "$MAX_PAGES"
  validate_positive_integer "stderr-tail-lines" "$STDERR_TAIL_LINES"

  case "$WIRE_FORMAT" in
    newline|content-length) ;;
    *)
      die "--wire-format must be newline or content-length"
      ;;
  esac

  start_server || true

  if [ "${#ERRORS[@]}" -eq 0 ]; then
    probe_initialize || true
  fi

  if [ "${#ERRORS[@]}" -eq 0 ]; then
    probe_ping || true
  fi

  if [ "${#ERRORS[@]}" -eq 0 ] && server_advertises_capability '.result.capabilities.tools != null'; then
    collect_paginated "tools/list" "tools" TOOLS_JSON || true
    validate_tool_collection "$TOOLS_JSON"
  fi

  if [ "${#ERRORS[@]}" -eq 0 ] && server_advertises_capability '.result.capabilities.prompts != null'; then
    collect_paginated "prompts/list" "prompts" PROMPTS_JSON || true
    validate_prompt_collection "$PROMPTS_JSON"
  fi

  if [ "${#ERRORS[@]}" -eq 0 ] && server_advertises_capability '.result.capabilities.resources != null'; then
    collect_paginated "resources/list" "resources" RESOURCES_JSON || true
    validate_resource_collection "$RESOURCES_JSON"

    collect_paginated "resources/templates/list" "resourceTemplates" RESOURCE_TEMPLATES_JSON || true
    validate_resource_template_collection "$RESOURCE_TEMPLATES_JSON"

    sample_resource_reads || true
  fi

  diagnose_stderr
  write_report
  print_summary

  if [ "$VERBOSE" -eq 1 ] || [ "${#ERRORS[@]}" -gt 0 ]; then
    print_stderr_tail
  fi

  if [ "${#ERRORS[@]}" -gt 0 ]; then
    exit 1
  fi
  if [ "$STRICT_MODE" -eq 1 ] && [ "${#WARNINGS[@]}" -gt 0 ]; then
    exit 2
  fi
}

main "$@"

# ------------------------------------------------------------------------------
# This solves the ugly real-world problem of MCP stdio servers looking fine in a
# README and then failing in actual client sessions because the handshake is
# wrong, the server advertises capabilities it does not honor, a tool schema is
# malformed, pagination loops forever, or resource reads silently break. Built
# because MCP server debugging in April 2026 is still too manual: people launch
# a server, stare at JSON, and waste time guessing whether the bug is framing,
# protocol negotiation, bad schemas, or capability drift.
#
# Use it when you are building or operating a Model Context Protocol server and
# you want a production-ready MCP health check, MCP stdio smoke test, MCP server
# contract validator, or CI gate for tools, prompts, and resources. The trick:
# it behaves like a small but disciplined MCP client, performs the initialize
# handshake, captures raw request and response logs, tolerates server
# notifications while it is waiting, validates JSON-RPC structure, checks tool
# inputSchema and outputSchema shapes, inspects prompt arguments, samples
# resources/read, and writes a JSON report you can archive in CI artifacts.
#
# Drop this into a repository that ships an MCP server, a platform team toolbox,
# a release pipeline, or a local developer setup where you need fast answers on
# whether a server is actually safe to wire into Claude, Codex, Cursor, VS Code,
# or any other MCP client. I wrote it to answer the question I always care
# about: “if I hand this server to another engineer right now, will it behave
# like a real MCP service instead of a demo that only works on my machine?”
# ------------------------------------------------------------------------------
