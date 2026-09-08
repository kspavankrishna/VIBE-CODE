#!/usr/bin/env bash
set -euo pipefail

PROGRAM_NAME="$(basename "$0")"
PROGRAM_VERSION="1.0.0"

BLOCKED_EXIT=40
LEASE_CONFLICT_EXIT=41
USAGE_EXIT=64
DEPENDENCY_EXIT=69

ALLOW_PROTECTED=0
ALLOW_STDIN_MANIFEST=0
NO_CONFIRM=0
FINGERPRINT_ONLY=0
SELF_TEST=0
AUDIT_ENABLED=1

KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
EXPECT_CONTEXT_REGEX="${KCG_EXPECT_CONTEXT:-}"
PROTECT_CONTEXT_REGEX="${KCG_PROTECT_CONTEXT:-(^|[-_/])(prod|production|live|shared-prod)([-_/]|$)}"
PROTECT_NAMESPACE_REGEX="${KCG_PROTECT_NAMESPACE:-^(kube-system|kube-public|kube-node-lease|argocd|cert-manager|flux-system|istio-system|ingress-nginx)$}"
ALLOW_NAMESPACE_REGEX="${KCG_ALLOW_NAMESPACE:-}"
CHANGE_TICKET="${KCG_CHANGE_TICKET:-}"
APPROVED_BY="${KCG_APPROVED_BY:-}"
LEASE_TTL_SECONDS="${KCG_LEASE_TTL:-900}"
LEASE_OWNER="${KCG_LEASE_OWNER:-${USER:-unknown}@$(hostname 2>/dev/null || printf 'unknown-host')}"
CONFIRM_TOKEN="${KCG_CONFIRM:-}"

KUBECTL_ARGS=()
REASONS=()
LEASE_HEARTBEAT_PID=""
LEASE_ACTIVE=0
LEASE_FILE_PATH=""
LEASE_LOCK_PATH=""
LEASE_HEARTBEAT_INTERVAL=0

REQUESTED_CONTEXT=""
REQUESTED_NAMESPACE=""
ALL_NAMESPACES=0
USES_STDIN_MANIFEST=0
CLASSIFIED_VERB=""
CLASSIFIED_SUBVERB=""
CLASSIFIED_RESOURCE=""
CLASSIFIED_MUTATING=0
CLASSIFIED_PROTECTED=0
CLASSIFIED_RISK="read"
EFFECTIVE_CONTEXT=""
EFFECTIVE_NAMESPACE=""
DECISION_FINGERPRINT=""

default_state_root() {
  if [ -n "${KCG_STATE_ROOT:-}" ]; then
    printf '%s\n' "$KCG_STATE_ROOT"
    return
  fi

  if [ -n "${XDG_STATE_HOME:-}" ]; then
    printf '%s\n' "${XDG_STATE_HOME}/kube-context-guard"
    return
  fi

  case "${OSTYPE:-}" in
    darwin*)
      printf '%s\n' "${HOME}/Library/Application Support/kube-context-guard"
      ;;
    *)
      printf '%s\n' "${HOME}/.local/state/kube-context-guard"
      ;;
  esac
}

STATE_ROOT="$(default_state_root)"
AUDIT_LOG_PATH="${KCG_AUDIT_LOG:-${STATE_ROOT}/audit.jsonl}"
LEASE_DIR_PATH="${KCG_LEASE_DIR:-${STATE_ROOT}/leases}"
LEASE_LOCK_ROOT="${LEASE_DIR_PATH}/.locks"

usage() {
  cat <<'EOF'
KubeContextGuard.sh wraps kubectl and blocks risky mutations in protected clusters unless
the caller adds an explicit override, a change ticket, and a matching confirmation token.

Usage:
  KubeContextGuard.sh [guard-options] -- <kubectl arguments>
  KubeContextGuard.sh [guard-options] <kubectl arguments>

Guard options:
  --guard-kubectl PATH             Use a specific kubectl binary.
  --guard-expect-context REGEX     Require the effective context to match REGEX.
  --guard-protect-context REGEX    Contexts matching REGEX are treated as protected.
  --guard-protect-namespace REGEX  Namespaces matching REGEX are treated as protected.
  --guard-allow-namespace REGEX    Allow only namespaces matching REGEX.
  --guard-ticket ID                Change ticket or incident ID for protected mutations.
  --guard-approved-by NAME         Optional approver recorded in audit events.
  --guard-owner NAME               Override the lease owner identity.
  --guard-lease-dir PATH           Directory used for mutation lease files.
  --guard-lease-ttl SECONDS        Freshness window for mutation leases. Default: 900.
  --guard-audit-log PATH           JSONL audit log path.
  --guard-allow-protected          Permit protected mutations when other checks pass.
  --guard-confirm TOKEN            Confirmation token for protected mutations.
  --guard-no-confirm               Skip the confirmation-token requirement.
  --guard-allow-stdin-manifest     Permit apply/create/replace using -f - in protected contexts.
  --guard-fingerprint-only         Print the computed operation fingerprint and exit.
  --guard-self-test                Run built-in self-tests.
  --guard-no-audit                 Disable JSONL audit output.
  --guard-help                     Show this help.
  --guard-version                  Print the version.

Examples:
  KubeContextGuard.sh --guard-expect-context '^dev-' -- get pods -n payments
  KubeContextGuard.sh --guard-allow-protected --guard-ticket CHG-4821 \
    --guard-confirm 4f13... -- delete deployment api -n payments
EOF
}

lower() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
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

join_by() {
  local separator="$1"
  shift
  local first=1
  local item
  for item in "$@"; do
    if [ "$first" -eq 1 ]; then
      printf '%s' "$item"
      first=0
    else
      printf '%s%s' "$separator" "$item"
    fi
  done
}

shell_quote() {
  local value="$1"
  case "$value" in
    "")
      printf "''"
      ;;
    *[!A-Za-z0-9_./:=,@%+-]*)
      printf "'%s'" "$(printf '%s' "$value" | sed "s/'/'\\''/g")"
      ;;
    *)
      printf '%s' "$value"
      ;;
  esac
}

shell_join() {
  local rendered=()
  local arg
  for arg in "$@"; do
    rendered+=("$(shell_quote "$arg")")
  done
  join_by " " "${rendered[@]}"
}

now_epoch() {
  date +%s
}

now_iso8601() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

ensure_parent_dir() {
  local path="$1"
  local parent
  parent="$(dirname "$path")"
  mkdir -p "$parent"
}

sha256_text() {
  if command -v shasum >/dev/null 2>&1; then
    printf '%s' "$1" | shasum -a 256 | awk '{print $1}'
    return
  fi

  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$1" | sha256sum | awk '{print $1}'
    return
  fi

  if command -v openssl >/dev/null 2>&1; then
    printf '%s' "$1" | openssl dgst -sha256 -r | awk '{print $1}'
    return
  fi

  printf '%s: missing sha256 implementation (shasum, sha256sum, or openssl)\n' "$PROGRAM_NAME" >&2
  exit "$DEPENDENCY_EXIT"
}

fail() {
  printf '%s: %s\n' "$PROGRAM_NAME" "$1" >&2
  exit "${2:-1}"
}

append_reason() {
  REASONS+=("$1")
}

clear_reasons() {
  REASONS=()
}

matches_regex() {
  local value="$1"
  local pattern="$2"
  if [ -z "$pattern" ]; then
    return 1
  fi
  [[ "$value" =~ $pattern ]]
}

sanitize_filename() {
  printf '%s' "$1" | tr -cs 'A-Za-z0-9._-:' '_'
}

lease_key() {
  printf '%s__%s\n' "$(sanitize_filename "$EFFECTIVE_CONTEXT")" "$(sanitize_filename "$EFFECTIVE_NAMESPACE")"
}

lease_lock_acquire() {
  local waited=0
  mkdir -p "$LEASE_LOCK_ROOT"
  while ! mkdir "$LEASE_LOCK_PATH" 2>/dev/null; do
    waited=$((waited + 1))
    if [ "$waited" -gt 200 ]; then
      return 1
    fi
    sleep 0.1
  done
  return 0
}

lease_lock_release() {
  if [ -n "$LEASE_LOCK_PATH" ] && [ -d "$LEASE_LOCK_PATH" ]; then
    rmdir "$LEASE_LOCK_PATH" >/dev/null 2>&1 || true
  fi
}

LEASE_FILE_OWNER=""
LEASE_FILE_EPOCH=""
LEASE_FILE_FINGERPRINT=""
LEASE_FILE_TICKET=""
LEASE_FILE_COMMAND=""

lease_read() {
  LEASE_FILE_OWNER=""
  LEASE_FILE_EPOCH=""
  LEASE_FILE_FINGERPRINT=""
  LEASE_FILE_TICKET=""
  LEASE_FILE_COMMAND=""

  if [ ! -f "$LEASE_FILE_PATH" ]; then
    return 1
  fi

  local key
  local value
  while IFS='=' read -r key value; do
    case "$key" in
      owner) LEASE_FILE_OWNER="$value" ;;
      epoch) LEASE_FILE_EPOCH="$value" ;;
      fingerprint) LEASE_FILE_FINGERPRINT="$value" ;;
      ticket) LEASE_FILE_TICKET="$value" ;;
      command) LEASE_FILE_COMMAND="$value" ;;
    esac
  done <"$LEASE_FILE_PATH"
  return 0
}

lease_write() {
  local epoch_value="$1"
  local temp_file="${LEASE_FILE_PATH}.tmp.$$"
  cat >"$temp_file" <<EOF
owner=${LEASE_OWNER}
epoch=${epoch_value}
fingerprint=${DECISION_FINGERPRINT}
ticket=${CHANGE_TICKET}
command=$(shell_join "${KUBECTL_ARGS[@]}")
EOF
  mv "$temp_file" "$LEASE_FILE_PATH"
}

lease_start_heartbeat() {
  LEASE_HEARTBEAT_INTERVAL=$((LEASE_TTL_SECONDS / 3))
  if [ "$LEASE_HEARTBEAT_INTERVAL" -lt 5 ]; then
    LEASE_HEARTBEAT_INTERVAL=5
  fi

  (
    while true; do
      sleep "$LEASE_HEARTBEAT_INTERVAL" || exit 0
      if ! lease_lock_acquire; then
        continue
      fi
      if lease_read \
        && [ "$LEASE_FILE_OWNER" = "$LEASE_OWNER" ] \
        && [ "$LEASE_FILE_FINGERPRINT" = "$DECISION_FINGERPRINT" ]; then
        lease_write "$(now_epoch)"
      else
        lease_lock_release
        exit 0
      fi
      lease_lock_release
    done
  ) >/dev/null 2>&1 &
  LEASE_HEARTBEAT_PID="$!"
}

lease_release() {
  if [ -n "$LEASE_HEARTBEAT_PID" ]; then
    kill "$LEASE_HEARTBEAT_PID" >/dev/null 2>&1 || true
    wait "$LEASE_HEARTBEAT_PID" 2>/dev/null || true
    LEASE_HEARTBEAT_PID=""
  fi

  if [ "$LEASE_ACTIVE" -ne 1 ]; then
    return
  fi

  if lease_lock_acquire; then
    if lease_read \
      && [ "$LEASE_FILE_OWNER" = "$LEASE_OWNER" ] \
      && [ "$LEASE_FILE_FINGERPRINT" = "$DECISION_FINGERPRINT" ]; then
      rm -f "$LEASE_FILE_PATH"
    fi
    lease_lock_release
  fi
  LEASE_ACTIVE=0
}

cleanup() {
  lease_release
}

trap cleanup EXIT INT TERM

audit_json_array() {
  local values=("$@")
  local output="["
  local first=1
  local value
  for value in "${values[@]}"; do
    if [ "$first" -eq 1 ]; then
      output="${output}\"$(json_escape "$value")\""
      first=0
    else
      output="${output},\"$(json_escape "$value")\""
    fi
  done
  output="${output}]"
  printf '%s' "$output"
}

audit_event() {
  local status="$1"
  local exit_code="$2"
  local note="$3"

  if [ "$AUDIT_ENABLED" -ne 1 ]; then
    return
  fi

  ensure_parent_dir "$AUDIT_LOG_PATH"
  local reasons_json
  reasons_json="$(audit_json_array "${REASONS[@]}")"
  printf '{"timestamp":"%s","status":"%s","exit_code":%s,"context":"%s","namespace":"%s","verb":"%s","subverb":"%s","resource":"%s","risk":"%s","protected":%s,"owner":"%s","ticket":"%s","approved_by":"%s","fingerprint":"%s","command":"%s","reasons":%s,"note":"%s"}\n' \
    "$(json_escape "$(now_iso8601)")" \
    "$(json_escape "$status")" \
    "$exit_code" \
    "$(json_escape "$EFFECTIVE_CONTEXT")" \
    "$(json_escape "$EFFECTIVE_NAMESPACE")" \
    "$(json_escape "$CLASSIFIED_VERB")" \
    "$(json_escape "$CLASSIFIED_SUBVERB")" \
    "$(json_escape "$CLASSIFIED_RESOURCE")" \
    "$(json_escape "$CLASSIFIED_RISK")" \
    "$CLASSIFIED_PROTECTED" \
    "$(json_escape "$LEASE_OWNER")" \
    "$(json_escape "$CHANGE_TICKET")" \
    "$(json_escape "$APPROVED_BY")" \
    "$(json_escape "$DECISION_FINGERPRINT")" \
    "$(json_escape "$(shell_join "${KUBECTL_ARGS[@]}")")" \
    "$reasons_json" \
    "$(json_escape "$note")" >>"$AUDIT_LOG_PATH"
}

parse_guard_option_value() {
  local option_name="$1"
  local option_value="${2:-}"
  if [ -z "$option_value" ]; then
    fail "missing value for ${option_name}" "$USAGE_EXIT"
  fi
  printf '%s' "$option_value"
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --guard-help)
        usage
        exit 0
        ;;
      --guard-version)
        printf '%s %s\n' "$PROGRAM_NAME" "$PROGRAM_VERSION"
        exit 0
        ;;
      --guard-self-test)
        SELF_TEST=1
        shift
        ;;
      --guard-no-audit)
        AUDIT_ENABLED=0
        shift
        ;;
      --guard-allow-protected)
        ALLOW_PROTECTED=1
        shift
        ;;
      --guard-no-confirm)
        NO_CONFIRM=1
        shift
        ;;
      --guard-allow-stdin-manifest)
        ALLOW_STDIN_MANIFEST=1
        shift
        ;;
      --guard-fingerprint-only)
        FINGERPRINT_ONLY=1
        shift
        ;;
      --guard-kubectl)
        shift
        KUBECTL_BIN="$(parse_guard_option_value --guard-kubectl "${1:-}")"
        shift
        ;;
      --guard-kubectl=*)
        KUBECTL_BIN="${1#--guard-kubectl=}"
        shift
        ;;
      --guard-expect-context)
        shift
        EXPECT_CONTEXT_REGEX="$(parse_guard_option_value --guard-expect-context "${1:-}")"
        shift
        ;;
      --guard-expect-context=*)
        EXPECT_CONTEXT_REGEX="${1#--guard-expect-context=}"
        shift
        ;;
      --guard-protect-context)
        shift
        PROTECT_CONTEXT_REGEX="$(parse_guard_option_value --guard-protect-context "${1:-}")"
        shift
        ;;
      --guard-protect-context=*)
        PROTECT_CONTEXT_REGEX="${1#--guard-protect-context=}"
        shift
        ;;
      --guard-protect-namespace)
        shift
        PROTECT_NAMESPACE_REGEX="$(parse_guard_option_value --guard-protect-namespace "${1:-}")"
        shift
        ;;
      --guard-protect-namespace=*)
        PROTECT_NAMESPACE_REGEX="${1#--guard-protect-namespace=}"
        shift
        ;;
      --guard-allow-namespace)
        shift
        ALLOW_NAMESPACE_REGEX="$(parse_guard_option_value --guard-allow-namespace "${1:-}")"
        shift
        ;;
      --guard-allow-namespace=*)
        ALLOW_NAMESPACE_REGEX="${1#--guard-allow-namespace=}"
        shift
        ;;
      --guard-ticket)
        shift
        CHANGE_TICKET="$(parse_guard_option_value --guard-ticket "${1:-}")"
        shift
        ;;
      --guard-ticket=*)
        CHANGE_TICKET="${1#--guard-ticket=}"
        shift
        ;;
      --guard-approved-by)
        shift
        APPROVED_BY="$(parse_guard_option_value --guard-approved-by "${1:-}")"
        shift
        ;;
      --guard-approved-by=*)
        APPROVED_BY="${1#--guard-approved-by=}"
        shift
        ;;
      --guard-owner)
        shift
        LEASE_OWNER="$(parse_guard_option_value --guard-owner "${1:-}")"
        shift
        ;;
      --guard-owner=*)
        LEASE_OWNER="${1#--guard-owner=}"
        shift
        ;;
      --guard-lease-dir)
        shift
        LEASE_DIR_PATH="$(parse_guard_option_value --guard-lease-dir "${1:-}")"
        LEASE_LOCK_ROOT="${LEASE_DIR_PATH}/.locks"
        shift
        ;;
      --guard-lease-dir=*)
        LEASE_DIR_PATH="${1#--guard-lease-dir=}"
        LEASE_LOCK_ROOT="${LEASE_DIR_PATH}/.locks"
        shift
        ;;
      --guard-lease-ttl)
        shift
        LEASE_TTL_SECONDS="$(parse_guard_option_value --guard-lease-ttl "${1:-}")"
        shift
        ;;
      --guard-lease-ttl=*)
        LEASE_TTL_SECONDS="${1#--guard-lease-ttl=}"
        shift
        ;;
      --guard-audit-log)
        shift
        AUDIT_LOG_PATH="$(parse_guard_option_value --guard-audit-log "${1:-}")"
        shift
        ;;
      --guard-audit-log=*)
        AUDIT_LOG_PATH="${1#--guard-audit-log=}"
        shift
        ;;
      --guard-confirm)
        shift
        CONFIRM_TOKEN="$(parse_guard_option_value --guard-confirm "${1:-}")"
        shift
        ;;
      --guard-confirm=*)
        CONFIRM_TOKEN="${1#--guard-confirm=}"
        shift
        ;;
      --)
        shift
        while [ "$#" -gt 0 ]; do
          KUBECTL_ARGS+=("$1")
          shift
        done
        ;;
      --guard-*)
        fail "unknown guard option: $1" "$USAGE_EXIT"
        ;;
      *)
        KUBECTL_ARGS+=("$1")
        shift
        ;;
    esac
  done
}

parse_kubectl_shape() {
  REQUESTED_CONTEXT=""
  REQUESTED_NAMESPACE=""
  ALL_NAMESPACES=0
  USES_STDIN_MANIFEST=0
  CLASSIFIED_VERB=""
  CLASSIFIED_SUBVERB=""
  CLASSIFIED_RESOURCE=""

  local tokens=()
  local expect_context_value=0
  local expect_namespace_value=0
  local expect_filename_value=0
  local expect_generic_value=0
  local i
  local arg

  for ((i = 0; i < ${#KUBECTL_ARGS[@]}; i++)); do
    arg="${KUBECTL_ARGS[i]}"

    if [ "$expect_context_value" -eq 1 ]; then
      REQUESTED_CONTEXT="$arg"
      expect_context_value=0
      continue
    fi

    if [ "$expect_namespace_value" -eq 1 ]; then
      REQUESTED_NAMESPACE="$arg"
      expect_namespace_value=0
      continue
    fi

    if [ "$expect_filename_value" -eq 1 ]; then
      if [ "$arg" = "-" ]; then
        USES_STDIN_MANIFEST=1
      fi
      expect_filename_value=0
      continue
    fi

    if [ "$expect_generic_value" -eq 1 ]; then
      expect_generic_value=0
      continue
    fi

    case "$arg" in
      --context)
        expect_context_value=1
        ;;
      --context=*)
        REQUESTED_CONTEXT="${arg#--context=}"
        ;;
      -n|--namespace)
        expect_namespace_value=1
        ;;
      --namespace=*)
        REQUESTED_NAMESPACE="${arg#--namespace=}"
        ;;
      -A|--all-namespaces)
        ALL_NAMESPACES=1
        ;;
      -f|--filename)
        expect_filename_value=1
        ;;
      --kustomize|-k|--selector|-l|--field-selector|-o|--output|--request-timeout|--server|--user|--cluster|--token|--cache-dir|--kubeconfig|--as|--as-group|--profile|--profile-output)
        expect_generic_value=1
        ;;
      --filename=*)
        if [ "${arg#--filename=}" = "-" ]; then
          USES_STDIN_MANIFEST=1
        fi
        ;;
      --kustomize=*|--selector=*|--field-selector=*|-o=*|--output=*|--request-timeout=*|--server=*|--user=*|--cluster=*|--token=*|--cache-dir=*|--kubeconfig=*|--as=*|--as-group=*|--profile=*|--profile-output=*)
        ;;
      -*)
        ;;
      *)
        tokens+=("$arg")
        ;;
    esac
  done

  CLASSIFIED_VERB="${tokens[0]:-}"
  CLASSIFIED_SUBVERB="${tokens[1]:-}"
  CLASSIFIED_RESOURCE="${tokens[1]:-}"

  if [ "$(lower "$CLASSIFIED_VERB")" = "rollout" ]; then
    CLASSIFIED_RESOURCE="${tokens[2]:-}"
  elif [ "$(lower "$CLASSIFIED_VERB")" = "auth" ]; then
    CLASSIFIED_RESOURCE="${tokens[2]:-}"
  elif [ "$(lower "$CLASSIFIED_VERB")" = "config" ]; then
    CLASSIFIED_RESOURCE="${tokens[2]:-}"
  fi
}

resolve_effective_context() {
  if [ -n "$REQUESTED_CONTEXT" ]; then
    printf '%s\n' "$REQUESTED_CONTEXT"
    return
  fi

  "$KUBECTL_BIN" config current-context
}

resolve_effective_namespace() {
  if [ "$ALL_NAMESPACES" -eq 1 ]; then
    printf '*\n'
    return
  fi

  if [ -n "$REQUESTED_NAMESPACE" ]; then
    printf '%s\n' "$REQUESTED_NAMESPACE"
    return
  fi

  local namespace
  namespace="$("$KUBECTL_BIN" config view --minify --output 'jsonpath={..namespace}' 2>/dev/null || true)"
  if [ -n "$namespace" ]; then
    printf '%s\n' "$namespace"
  else
    printf 'default\n'
  fi
}

classify_command() {
  clear_reasons
  CLASSIFIED_MUTATING=0
  CLASSIFIED_RISK="read"

  local verb
  local subverb
  local resource
  verb="$(lower "$CLASSIFIED_VERB")"
  subverb="$(lower "$CLASSIFIED_SUBVERB")"
  resource="$(lower "$CLASSIFIED_RESOURCE")"

  case "$verb" in
    get|describe|logs|top|api-resources|api-versions|cluster-info|completion|diff|events|explain|version|wait)
      CLASSIFIED_MUTATING=0
      CLASSIFIED_RISK="read"
      ;;
    auth)
      if [ "$subverb" = "can-i" ]; then
        CLASSIFIED_MUTATING=0
        CLASSIFIED_RISK="read"
      else
        CLASSIFIED_MUTATING=1
        CLASSIFIED_RISK="unknown"
        append_reason "kubectl auth subcommand is not classified as read-only"
      fi
      ;;
    config)
      CLASSIFIED_MUTATING=0
      CLASSIFIED_RISK="config"
      ;;
    exec|attach|cp|debug|port-forward)
      CLASSIFIED_MUTATING=1
      CLASSIFIED_RISK="interactive"
      append_reason "interactive pod access can change live workload state"
      ;;
    apply|create|delete|replace|patch|edit|annotate|label|scale|autoscale|set|expose|run|cordon|uncordon|drain|taint)
      CLASSIFIED_MUTATING=1
      CLASSIFIED_RISK="mutating"
      ;;
    rollout)
      case "$subverb" in
        status|history)
          CLASSIFIED_MUTATING=0
          CLASSIFIED_RISK="read"
          ;;
        restart|undo|pause|resume)
          CLASSIFIED_MUTATING=1
          CLASSIFIED_RISK="mutating"
          ;;
        *)
          CLASSIFIED_MUTATING=1
          CLASSIFIED_RISK="unknown"
          append_reason "unrecognized rollout subcommand treated as mutating"
          ;;
      esac
      ;;
    "")
      fail "missing kubectl command after guard options" "$USAGE_EXIT"
      ;;
    *)
      CLASSIFIED_MUTATING=1
      CLASSIFIED_RISK="unknown"
      append_reason "unknown kubectl verb treated as mutating for safety"
      ;;
  esac

  if [ "$CLASSIFIED_MUTATING" -eq 1 ]; then
    if [ "$ALL_NAMESPACES" -eq 1 ]; then
      append_reason "command spans all namespaces"
    fi

    case "$verb" in
      delete)
        local joined
        joined=" $(lower "$(shell_join "${KUBECTL_ARGS[@]}")") "
        if [[ "$joined" == *" --all "* ]]; then
          append_reason "delete uses --all"
        fi
        case "$resource" in
          namespace|namespaces|ns|clusterrole|clusterroles|clusterrolebinding|clusterrolebindings|node|nodes|crd|crds|customresourcedefinition|customresourcedefinitions|persistentvolume|persistentvolumes|pv|mutatingwebhookconfiguration|mutatingwebhookconfigurations|validatingwebhookconfiguration|validatingwebhookconfigurations)
            append_reason "delete targets a high-blast-radius cluster resource"
            ;;
        esac
        ;;
      apply|create|replace)
        if [ "$USES_STDIN_MANIFEST" -eq 1 ]; then
          append_reason "command uses stdin manifest (-f -)"
        fi
        ;;
      exec|attach|debug|cp)
        append_reason "interactive command bypasses declarative change review"
        ;;
    esac
  fi
}

compute_protection_state() {
  CLASSIFIED_PROTECTED=0
  if matches_regex "$EFFECTIVE_CONTEXT" "$PROTECT_CONTEXT_REGEX"; then
    CLASSIFIED_PROTECTED=1
    append_reason "context matches protected-context policy"
  fi

  if [ "$ALL_NAMESPACES" -eq 1 ] && [ "$CLASSIFIED_MUTATING" -eq 1 ]; then
    CLASSIFIED_PROTECTED=1
    append_reason "all-namespaces mutation can touch protected namespaces"
  elif matches_regex "$EFFECTIVE_NAMESPACE" "$PROTECT_NAMESPACE_REGEX"; then
    CLASSIFIED_PROTECTED=1
    append_reason "namespace matches protected-namespace policy"
  fi
}

enforce_context_policy() {
  if [ -n "$EXPECT_CONTEXT_REGEX" ] && ! matches_regex "$EFFECTIVE_CONTEXT" "$EXPECT_CONTEXT_REGEX"; then
    clear_reasons
    append_reason "effective context does not match expected-context policy"
    audit_event "blocked" "$BLOCKED_EXIT" "context expectation failed"
    fail "refusing to run against context '${EFFECTIVE_CONTEXT}' because it does not match --guard-expect-context" "$BLOCKED_EXIT"
  fi

  if [ -n "$ALLOW_NAMESPACE_REGEX" ]; then
    if [ "$ALL_NAMESPACES" -eq 1 ]; then
      clear_reasons
      append_reason "all-namespaces request bypasses allow-namespace policy"
      audit_event "blocked" "$BLOCKED_EXIT" "namespace allowlist failed"
      fail "refusing to use --all-namespaces while --guard-allow-namespace is configured" "$BLOCKED_EXIT"
    fi

    if ! matches_regex "$EFFECTIVE_NAMESPACE" "$ALLOW_NAMESPACE_REGEX"; then
      clear_reasons
      append_reason "effective namespace does not match allow-namespace policy"
      audit_event "blocked" "$BLOCKED_EXIT" "namespace allowlist failed"
      fail "refusing to run against namespace '${EFFECTIVE_NAMESPACE}' because it does not match --guard-allow-namespace" "$BLOCKED_EXIT"
    fi
  fi
}

compute_fingerprint() {
  DECISION_FINGERPRINT="$(
    sha256_text "$(printf 'context=%s\nnamespace=%s\ncommand=%s\n' \
      "$EFFECTIVE_CONTEXT" \
      "$EFFECTIVE_NAMESPACE" \
      "$(shell_join "${KUBECTL_ARGS[@]}")")"
  )"
}

render_override_hint() {
  local ticket_hint="${CHANGE_TICKET:-CHG-12345}"
  local rerun=("$PROGRAM_NAME" "--guard-allow-protected" "--guard-ticket" "$ticket_hint")
  if [ "$NO_CONFIRM" -ne 1 ]; then
    rerun+=("--guard-confirm" "$DECISION_FINGERPRINT")
  fi
  if [ "$USES_STDIN_MANIFEST" -eq 1 ]; then
    rerun+=("--guard-allow-stdin-manifest")
  fi
  rerun+=("--")
  rerun+=("${KUBECTL_ARGS[@]}")
  shell_join "${rerun[@]}"
}

emit_block_report() {
  printf '%s: blocked protected kubectl operation\n' "$PROGRAM_NAME" >&2
  printf '  context: %s\n' "$EFFECTIVE_CONTEXT" >&2
  printf '  namespace: %s\n' "$EFFECTIVE_NAMESPACE" >&2
  printf '  fingerprint: %s\n' "$DECISION_FINGERPRINT" >&2
  printf '  command: %s\n' "$(shell_join "${KUBECTL_ARGS[@]}")" >&2
  if [ "${#REASONS[@]}" -gt 0 ]; then
    printf '  reasons:\n' >&2
    local reason
    for reason in "${REASONS[@]}"; do
      printf '    - %s\n' "$reason" >&2
    done
  fi
  printf '  rerun: %s\n' "$(render_override_hint)" >&2
}

enforce_protected_mutation_policy() {
  if [ "$CLASSIFIED_MUTATING" -ne 1 ] || [ "$CLASSIFIED_PROTECTED" -ne 1 ]; then
    return
  fi

  if [ "$USES_STDIN_MANIFEST" -eq 1 ] && [ "$ALLOW_STDIN_MANIFEST" -ne 1 ]; then
    append_reason "stdin manifest override is required for protected contexts"
    audit_event "blocked" "$BLOCKED_EXIT" "stdin manifest denied"
    emit_block_report
    fail "stdin manifests are blocked in protected contexts unless --guard-allow-stdin-manifest is set" "$BLOCKED_EXIT"
  fi

  if [ "$ALLOW_PROTECTED" -ne 1 ]; then
    append_reason "protected mutation requires --guard-allow-protected"
    audit_event "blocked" "$BLOCKED_EXIT" "protected mutation override missing"
    emit_block_report
    fail "protected mutation requires --guard-allow-protected" "$BLOCKED_EXIT"
  fi

  if [ -z "$CHANGE_TICKET" ]; then
    append_reason "protected mutation requires a change ticket"
    audit_event "blocked" "$BLOCKED_EXIT" "change ticket missing"
    emit_block_report
    fail "protected mutation requires --guard-ticket" "$BLOCKED_EXIT"
  fi

  if [ "$NO_CONFIRM" -ne 1 ] && [ "$CONFIRM_TOKEN" != "$DECISION_FINGERPRINT" ]; then
    append_reason "confirmation token does not match the current operation fingerprint"
    audit_event "blocked" "$BLOCKED_EXIT" "confirmation token missing or stale"
    emit_block_report
    fail "protected mutation requires --guard-confirm ${DECISION_FINGERPRINT}" "$BLOCKED_EXIT"
  fi
}

acquire_mutation_lease() {
  if [ "$CLASSIFIED_MUTATING" -ne 1 ] || [ "$CLASSIFIED_PROTECTED" -ne 1 ]; then
    return
  fi

  mkdir -p "$LEASE_DIR_PATH"
  LEASE_FILE_PATH="${LEASE_DIR_PATH}/$(lease_key).lease"
  LEASE_LOCK_PATH="${LEASE_LOCK_ROOT}/$(lease_key).lock"

  if ! lease_lock_acquire; then
    append_reason "timed out waiting for lease lock"
    audit_event "blocked" "$LEASE_CONFLICT_EXIT" "lease lock contention"
    fail "timed out waiting for mutation lease lock" "$LEASE_CONFLICT_EXIT"
  fi

  local current_epoch
  current_epoch="$(now_epoch)"
  if lease_read; then
    if [ "$LEASE_FILE_OWNER" = "$LEASE_OWNER" ] && [ "$LEASE_FILE_FINGERPRINT" = "$DECISION_FINGERPRINT" ]; then
      lease_write "$current_epoch"
    elif [ -n "$LEASE_FILE_EPOCH" ] && [ $((current_epoch - LEASE_FILE_EPOCH)) -lt "$LEASE_TTL_SECONDS" ]; then
      append_reason "another session holds a fresh mutation lease"
      local holder="$LEASE_FILE_OWNER"
      lease_lock_release
      audit_event "blocked" "$LEASE_CONFLICT_EXIT" "lease already held"
      fail "mutation lease already held by ${holder}; wait for it to finish or expire" "$LEASE_CONFLICT_EXIT"
    else
      lease_write "$current_epoch"
    fi
  else
    lease_write "$current_epoch"
  fi

  lease_lock_release
  LEASE_ACTIVE=1
  lease_start_heartbeat
}

verify_dependencies() {
  if ! command -v "$KUBECTL_BIN" >/dev/null 2>&1; then
    fail "kubectl binary not found: ${KUBECTL_BIN}" "$DEPENDENCY_EXIT"
  fi

  case "$LEASE_TTL_SECONDS" in
    ''|*[!0-9]*)
      fail "--guard-lease-ttl must be an integer number of seconds" "$USAGE_EXIT"
      ;;
  esac
}

evaluate() {
  verify_dependencies
  parse_kubectl_shape
  EFFECTIVE_CONTEXT="$(resolve_effective_context)"
  EFFECTIVE_NAMESPACE="$(resolve_effective_namespace)"
  classify_command
  compute_protection_state
  enforce_context_policy
  compute_fingerprint
}

run_guarded_kubectl() {
  evaluate

  if [ "$FINGERPRINT_ONLY" -eq 1 ]; then
    printf '%s\n' "$DECISION_FINGERPRINT"
    return 0
  fi

  enforce_protected_mutation_policy
  acquire_mutation_lease
  audit_event "allowed" 0 "command dispatched"

  set +e
  "$KUBECTL_BIN" "${KUBECTL_ARGS[@]}"
  local kubectl_exit="$?"
  set -e

  if [ "$kubectl_exit" -eq 0 ]; then
    audit_event "completed" 0 "kubectl finished successfully"
  else
    append_reason "kubectl exited with a non-zero status"
    audit_event "failed" "$kubectl_exit" "kubectl returned non-zero"
  fi

  return "$kubectl_exit"
}

assert_eq() {
  local expected="$1"
  local actual="$2"
  local label="$3"
  if [ "$expected" != "$actual" ]; then
    printf 'assertion failed: %s\nexpected: %s\nactual: %s\n' "$label" "$expected" "$actual" >&2
    return 1
  fi
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  local label="$3"
  if [[ "$haystack" != *"$needle"* ]]; then
    printf 'assertion failed: %s\nmissing substring: %s\n' "$label" "$needle" >&2
    return 1
  fi
}

make_fake_kubectl() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -ge 2 ] && [ "$1" = "config" ] && [ "$2" = "current-context" ]; then
  printf '%s\n' "${FAKE_CONTEXT:-dev-cluster}"
  exit 0
fi

if [ "$#" -ge 2 ] && [ "$1" = "config" ] && [ "$2" = "view" ]; then
  printf '%s' "${FAKE_NAMESPACE:-default}"
  exit 0
fi

printf '%s\n' "$*" >>"${FAKE_KUBECTL_LOG:?}"
exit 0
EOF
  chmod +x "$path"
}

run_self_tests() {
  local temp_dir
  temp_dir="$(mktemp -d)"
  local fake_kubectl="${temp_dir}/fake-kubectl"
  local fake_log="${temp_dir}/kubectl.log"
  local script_path="${temp_dir}/KubeContextGuard.sh"
  local state_root="${temp_dir}/state"

  cp "$0" "$script_path"
  chmod +x "$script_path"
  : >"$fake_log"
  make_fake_kubectl "$fake_kubectl"

  FAKE_CONTEXT="prod-eu1"
  FAKE_NAMESPACE="payments"
  export FAKE_CONTEXT FAKE_NAMESPACE FAKE_KUBECTL_LOG="$fake_log" KCG_STATE_ROOT="$state_root"

  local output=""
  local status=""
  run_capture output status "$script_path" --guard-kubectl "$fake_kubectl" -- get pods -n payments
  assert_eq "0" "$status" "read operation should be allowed"
  assert_contains "$(cat "$fake_log")" "get pods -n payments" "read command should reach kubectl"

  : >"$fake_log"
  run_capture output status "$script_path" --guard-kubectl "$fake_kubectl" -- delete deployment api -n payments
  assert_eq "$BLOCKED_EXIT" "$status" "protected mutation should be blocked without override"
  assert_contains "$output" "blocked protected kubectl operation" "protected mutation block output"

  local fingerprint
  fingerprint="$("$script_path" --guard-kubectl "$fake_kubectl" --guard-fingerprint-only -- delete deployment api -n payments)"
  : >"$fake_log"
  run_capture output status "$script_path" --guard-kubectl "$fake_kubectl" --guard-allow-protected --guard-ticket CHG-42 --guard-confirm "$fingerprint" -- delete deployment api -n payments
  assert_eq "0" "$status" "protected mutation should run with valid override"
  assert_contains "$(cat "$fake_log")" "delete deployment api -n payments" "mutating command should reach kubectl"

  : >"$fake_log"
  run_capture output status "$script_path" --guard-kubectl "$fake_kubectl" --guard-allow-namespace '^payments$' -- get pods -A
  assert_eq "$BLOCKED_EXIT" "$status" "allow-namespace should reject all-namespaces"
  assert_contains "$output" "--all-namespaces" "allow-namespace block output"

  : >"$fake_log"
  run_capture output status "$script_path" --guard-kubectl "$fake_kubectl" --guard-allow-protected --guard-ticket CHG-42 --guard-confirm "$fingerprint" -- apply -f - -n payments
  assert_eq "$BLOCKED_EXIT" "$status" "stdin manifests should be blocked in protected contexts"
  assert_contains "$output" "stdin manifests are blocked" "stdin manifest block output"

  local lease_dir="${temp_dir}/leases"
  mkdir -p "${lease_dir}"
  local lease_file="${lease_dir}/prod-eu1__payments.lease"
  cat >"$lease_file" <<EOF
owner=other-user@host
epoch=$(now_epoch)
fingerprint=someone-else
ticket=CHG-999
command=kubectl delete deployment api -n payments
EOF
  : >"$fake_log"
  run_capture output status "$script_path" --guard-kubectl "$fake_kubectl" --guard-lease-dir "$lease_dir" --guard-allow-protected --guard-ticket CHG-42 --guard-confirm "$fingerprint" -- delete deployment api -n payments
  assert_eq "$LEASE_CONFLICT_EXIT" "$status" "fresh lease should block overlapping mutation"
  assert_contains "$output" "mutation lease already held" "lease conflict output"

  rm -rf "$temp_dir"
  printf 'self-test passed\n'
}

run_capture() {
  local output_var="$1"
  local status_var="$2"
  shift 2

  local output_text
  local exit_code
  set +e
  output_text="$("$@" 2>&1)"
  exit_code="$?"
  set -e

  printf -v "$output_var" '%s' "$output_text"
  printf -v "$status_var" '%s' "$exit_code"
}

main() {
  parse_args "$@"

  if [ "$SELF_TEST" -eq 1 ]; then
    run_self_tests
    return 0
  fi

  if [ "${#KUBECTL_ARGS[@]}" -eq 0 ]; then
    usage >&2
    return "$USAGE_EXIT"
  fi

  run_guarded_kubectl
}

main "$@"

# This solves accidental production Kubernetes changes from AI agents, shell automation, and tired humans who
# have the right `kubectl` credentials but the wrong execution guardrails. Built because the modern failure mode
# is not usually missing RBAC anymore. It is a model, script, or engineer running a perfectly valid command in
# the wrong cluster, the wrong namespace, or with a blast radius that nobody slowed down long enough to inspect.
# Use it when you are wrapping `kubectl` inside CI jobs, local dev shells, GitHub Actions runners, release bots,
# incident tooling, or agentic coding environments where context switching is constant and a single `delete`,
# `apply`, `rollout restart`, or `exec` in production can turn into an outage, data loss, or a very expensive
# rollback window.
#
# The trick: this file does not try to replace Kubernetes RBAC, admission control, or policy engines. It sits one
# layer earlier and makes intent explicit before the command ever leaves the terminal. It resolves the effective
# context and namespace, classifies whether the command looks read-only, mutating, or interactive, computes a
# deterministic fingerprint from the exact target plus command line, and then applies practical rules that match
# how teams actually work in April 2026. Protected-cluster mutations need an explicit override, a change ticket,
# and a confirmation token tied to the exact command. Stdin manifests like `kubectl apply -f -` are blocked by
# default in protected environments because generated YAML over a pipe is one of the easiest ways for an agent to
# slip around normal review habits. A renewable TTL lease prevents two shells or two agents from mutating the same
# protected slice of the cluster at the same time, and every allow or block decision is written to a JSONL audit
# log so you can answer what happened later without scraping terminal scrollback.
#
# Drop this into a repo as `KubeContextGuard.sh`, mark it executable, and call it anywhere you would normally call
# `kubectl`. The low-friction pattern is to alias `kubectl` in agent shells, CI steps, or SRE runbooks so reads
# stay fast but writes to production become deliberate and traceable. If your team already has change IDs, ticket
# numbers, protected-context naming rules, or namespace ownership rules, wire them in through the guard flags or
# environment variables and keep the defaults tight. The point is simple: let safe reads feel normal, make risky
# writes prove intent, and give yourself a small, portable seatbelt that works even when the rest of the platform
# stack is too heavy, too slow, or too far away from the person or automation actually typing the command.
