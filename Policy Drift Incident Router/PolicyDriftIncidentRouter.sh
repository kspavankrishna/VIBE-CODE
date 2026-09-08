#!/usr/bin/env bash
set -eo pipefail
IFS=$'\n\t'

TOOL_NAME="PolicyDriftIncidentRouter"
TOOL_VERSION="1.0.0"

CHANGES_FILE="-"
RULES_FILE=""
FORMAT="markdown"
FAIL_ON="critical"
MIN_SEVERITY="info"
DEFAULT_OWNER="platform"
QUIET_UNMATCHED=0
STRICT_RULES=0
SELF_TEST=0

declare -a CHANGE_PATHS=()
declare -a RULE_PATTERNS=()
declare -a RULE_SEVERITIES=()
declare -a RULE_OWNERS=()
declare -a RULE_CONTROLS=()
declare -a RULE_RUNBOOKS=()
declare -a RULE_NOTES=()
declare -a ROUTE_PATHS=()
declare -a ROUTE_PATTERNS=()
declare -a ROUTE_SEVERITIES=()
declare -a ROUTE_RANKS=()
declare -a ROUTE_OWNERS=()
declare -a ROUTE_CONTROLS=()
declare -a ROUTE_RUNBOOKS=()
declare -a ROUTE_NOTES=()
declare -a WARNINGS=()

usage() {
  cat <<'USAGE'
PolicyDriftIncidentRouter.sh

Route risky policy, infrastructure, AI-agent, data-pipeline, edge, and DevOps
drift from a changed-file list before it becomes an incident.

Usage:
  bash PolicyDriftIncidentRouter.sh --changes changed.txt [options]
  git diff --name-only origin/main...HEAD | bash PolicyDriftIncidentRouter.sh
  bash PolicyDriftIncidentRouter.sh --self-test

Options:
  --changes PATH          Changed paths file, or - for stdin. Default: -
  --rules PATH            Optional TSV or pipe rule catalog.
  --format FORMAT         markdown, json, sarif, or gha. Default: markdown
  --fail-on SEVERITY      Exit 2 when max severity is at least this level.
                           Values: info, low, medium, high, critical, never
                           Default: critical
  --min-severity LEVEL    Suppress route rows below this level. Default: info
  --default-owner OWNER   Owner for unmatched paths. Default: platform
  --quiet-unmatched       Do not include unmatched paths in reports.
  --strict-rules          Treat malformed rule rows as fatal.
  --self-test             Run built-in behavior tests.
  -h, --help              Show help.

Rule catalog:
  One rule per line. Comments begin with #. Preferred separator is a tab:
    pattern<TAB>severity<TAB>owner<TAB>control<TAB>runbook<TAB>note

  Pipe-separated rows are also accepted:
    policy/**|critical|security|opa_bundle|https://runbooks/policy|OPA drift

Severity order:
  info < low < medium < high < critical
USAGE
}

die() {
  printf '%s: %s\n' "$TOOL_NAME" "$*" >&2
  exit 1
}

warn() {
  WARNINGS+=("$*")
}

lower() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

trim() {
  local value="${1//$'\r'/}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

normalize_path() {
  local value
  value="$(trim "$1")"
  value="${value#\"}"
  value="${value%\"}"
  value="${value#\'}"
  value="${value%\'}"
  value="${value%,}"
  value="${value#./}"
  value="${value#/}"
  while [[ "$value" == *"//"* ]]; do
    value="${value//\/\//\/}"
  done
  printf '%s' "$value"
}

severity_rank() {
  case "$(lower "$(trim "$1")")" in
    info) printf '0' ;;
    low) printf '1' ;;
    medium) printf '2' ;;
    high) printf '3' ;;
    critical) printf '4' ;;
    never) printf '99' ;;
    *) return 1 ;;
  esac
}

rank_name() {
  case "$1" in
    0) printf 'info' ;;
    1) printf 'low' ;;
    2) printf 'medium' ;;
    3) printf 'high' ;;
    4) printf 'critical' ;;
    *) printf 'unknown' ;;
  esac
}

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

gha_escape() {
  local value="$1"
  value="${value//'%'/'%25'}"
  value="${value//$'\r'/'%0D'}"
  value="${value//$'\n'/'%0A'}"
  printf '%s' "$value"
}

md_escape() {
  local value="$1"
  value="${value//|/\\|}"
  value="${value//$'\n'/ }"
  value="${value//$'\r'/ }"
  printf '%s' "$value"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --changes)
        [[ $# -ge 2 ]] || die "--changes requires a path"
        CHANGES_FILE="$2"
        shift 2
        ;;
      --rules)
        [[ $# -ge 2 ]] || die "--rules requires a path"
        RULES_FILE="$2"
        shift 2
        ;;
      --format)
        [[ $# -ge 2 ]] || die "--format requires markdown, json, sarif, or gha"
        FORMAT="$(lower "$2")"
        shift 2
        ;;
      --fail-on)
        [[ $# -ge 2 ]] || die "--fail-on requires a severity"
        FAIL_ON="$(lower "$2")"
        shift 2
        ;;
      --min-severity)
        [[ $# -ge 2 ]] || die "--min-severity requires a severity"
        MIN_SEVERITY="$(lower "$2")"
        shift 2
        ;;
      --default-owner)
        [[ $# -ge 2 ]] || die "--default-owner requires a value"
        DEFAULT_OWNER="$(trim "$2")"
        shift 2
        ;;
      --quiet-unmatched)
        QUIET_UNMATCHED=1
        shift
        ;;
      --strict-rules)
        STRICT_RULES=1
        shift
        ;;
      --self-test)
        SELF_TEST=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done

  case "$FORMAT" in
    markdown|json|sarif|gha) ;;
    *) die "unknown format: $FORMAT" ;;
  esac
  if [[ "$FAIL_ON" != "never" ]]; then
    severity_rank "$FAIL_ON" >/dev/null || die "unknown --fail-on severity: $FAIL_ON"
  fi
  severity_rank "$MIN_SEVERITY" >/dev/null || die "unknown --min-severity: $MIN_SEVERITY"
  [[ -n "$DEFAULT_OWNER" ]] || die "--default-owner cannot be empty"
}

add_change() {
  local path
  path="$(normalize_path "$1")"
  [[ -n "$path" ]] || return 0
  [[ "$path" == \#* ]] && return 0
  local existing
  for existing in "${CHANGE_PATHS[@]}"; do
    [[ "$existing" == "$path" ]] && return 0
  done
  CHANGE_PATHS+=("$path")
}

read_changes_line() {
  local line path first rest
  line="$(trim "$1")"
  [[ -n "$line" ]] || return 0
  [[ "$line" == \#* ]] && return 0
  if [[ "$line" == *$'\t'* ]]; then
    first="${line%%$'\t'*}"
    rest="${line#*$'\t'}"
    case "$first" in
      R*|C*) path="${rest##*$'\t'}" ;;
      A|M|D|R|C|AM|MM|??) path="$rest" ;;
      *) path="$line" ;;
    esac
  elif [[ "$line" == [AMDRC]" "* || "$line" == "?? "* ]]; then
    path="${line#* }"
  else
    path="$line"
  fi
  add_change "$path"
}

load_changes() {
  local line
  if [[ "$CHANGES_FILE" == "-" ]]; then
    while IFS= read -r line; do
      read_changes_line "$line"
    done
  else
    [[ -f "$CHANGES_FILE" ]] || die "changes file not found: $CHANGES_FILE"
    while IFS= read -r line || [[ -n "$line" ]]; do
      read_changes_line "$line"
    done < "$CHANGES_FILE"
  fi
  [[ "${#CHANGE_PATHS[@]}" -gt 0 ]] || die "no changed paths were provided"
}

valid_rule_field() {
  local value="$1"
  [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]]
}

add_rule() {
  local pattern severity owner control runbook note rank
  pattern="$(normalize_path "$1")"
  severity="$(lower "$(trim "$2")")"
  owner="$(trim "$3")"
  control="$(trim "$4")"
  runbook="$(trim "$5")"
  note="$(trim "$6")"
  if [[ -z "$pattern" || -z "$severity" || -z "$owner" || -z "$control" ]]; then
    return 1
  fi
  rank="$(severity_rank "$severity")" || return 1
  valid_rule_field "$pattern" && valid_rule_field "$owner" && valid_rule_field "$control" || return 1
  RULE_PATTERNS+=("$pattern")
  RULE_SEVERITIES+=("$(rank_name "$rank")")
  RULE_OWNERS+=("$owner")
  RULE_CONTROLS+=("$control")
  RULE_RUNBOOKS+=("$runbook")
  RULE_NOTES+=("$note")
}

load_builtin_rules() {
  add_rule ".github/workflows/**" high "platform-ci" "ci_workflow_change" "https://runbooks.local/ci-workflows" "GitHub Actions workflow drift can change release authority, tokens, cache scope, or deployment order."
  add_rule ".github/actions/**" high "platform-ci" "ci_action_change" "https://runbooks.local/ci-actions" "Composite actions affect many repositories and are easy to trust without review."
  add_rule "**/Dockerfile" high "platform-security" "container_build_change" "https://runbooks.local/container-builds" "Container build context, base images, and package managers change runtime attack surface."
  add_rule "**/Containerfile" high "platform-security" "container_build_change" "https://runbooks.local/container-builds" "Containerfile drift carries the same release risk as Dockerfile drift."
  add_rule "**/*.rego" critical "policy-security" "opa_policy_change" "https://runbooks.local/opa-policy" "OPA and Rego changes alter authorization, admission, or deployment policy."
  add_rule "policy/**" critical "policy-security" "policy_bundle_change" "https://runbooks.local/policy-bundles" "Policy bundle changes should have owner review and replay evidence."
  add_rule "policies/**" critical "policy-security" "policy_bundle_change" "https://runbooks.local/policy-bundles" "Policy directory drift often changes production guardrails."
  add_rule "infra/**" high "infra-platform" "infrastructure_change" "https://runbooks.local/infra-drift" "Infrastructure drift can modify blast radius, permissions, regions, or cost controls."
  add_rule "terraform/**" high "infra-platform" "terraform_change" "https://runbooks.local/terraform" "Terraform plans need explicit route owners and state-aware review."
  add_rule "**/*.tf" high "infra-platform" "terraform_change" "https://runbooks.local/terraform" "A Terraform file outside the normal folder still changes infrastructure."
  add_rule "k8s/**" high "runtime-platform" "kubernetes_manifest_change" "https://runbooks.local/kubernetes" "Kubernetes manifest drift can change identity, scheduling, networking, or rollout safety."
  add_rule "kubernetes/**" high "runtime-platform" "kubernetes_manifest_change" "https://runbooks.local/kubernetes" "Kubernetes directories should route to runtime owners before merge."
  add_rule "helm/**" medium "runtime-platform" "helm_chart_change" "https://runbooks.local/helm" "Helm chart changes can hide template-specific runtime behavior."
  add_rule "charts/**" medium "runtime-platform" "helm_chart_change" "https://runbooks.local/helm" "Chart drift needs ownership even when the rendered manifests are not checked in."
  add_rule "mcp/**" critical "ai-platform" "mcp_contract_change" "https://runbooks.local/mcp-contracts" "MCP tool schemas and server definitions affect what coding agents can call."
  add_rule "agents/**" high "ai-platform" "agent_runtime_change" "https://runbooks.local/agent-runtime" "Agent runtime drift can change autonomy, tool access, prompts, retries, and audit logs."
  add_rule "prompts/**" medium "ai-safety" "prompt_contract_change" "https://runbooks.local/prompt-contracts" "Prompt contract edits should be reviewed with eval evidence."
  add_rule "evals/**" high "research-quality" "eval_harness_change" "https://runbooks.local/evals" "Eval harness drift can silently improve metrics without improving behavior."
  add_rule "edge/**" high "edge-platform" "edge_worker_change" "https://runbooks.local/edge-workers" "Edge worker changes affect regional behavior, cache keys, and latency budgets."
  add_rule "workers/**" high "edge-platform" "edge_worker_change" "https://runbooks.local/edge-workers" "Worker directories commonly hold edge compute routing and auth logic."
  add_rule "iot/**" high "device-platform" "iot_control_change" "https://runbooks.local/iot-control" "IoT control path drift can affect devices that are hard to patch quickly."
  add_rule "pipelines/**" high "data-platform" "data_pipeline_change" "https://runbooks.local/data-pipelines" "Data pipeline drift changes contracts, lineage, freshness, and downstream decisions."
  add_rule "db/migrations/**" critical "data-platform" "database_migration_change" "https://runbooks.local/database-migrations" "Database migrations need rollback thinking and owner review."
  add_rule "migrations/**" high "data-platform" "migration_change" "https://runbooks.local/migrations" "Migration drift should not merge as a normal code-only change."
  add_rule "package-lock.json" medium "dependency-governance" "dependency_lock_change" "https://runbooks.local/dependencies" "Lockfile drift changes effective supply chain inputs."
  add_rule "pnpm-lock.yaml" medium "dependency-governance" "dependency_lock_change" "https://runbooks.local/dependencies" "PNPM lockfile changes should route to dependency governance."
  add_rule "uv.lock" medium "dependency-governance" "dependency_lock_change" "https://runbooks.local/dependencies" "Python lockfile drift changes resolved packages."
  add_rule "requirements*.txt" medium "dependency-governance" "dependency_manifest_change" "https://runbooks.local/dependencies" "Python dependency manifests remain common in production jobs."
  add_rule "go.sum" medium "dependency-governance" "dependency_lock_change" "https://runbooks.local/dependencies" "Go module checksum drift changes dependency provenance."
  add_rule "Cargo.lock" medium "dependency-governance" "dependency_lock_change" "https://runbooks.local/dependencies" "Rust lockfile drift changes reproducible builds."
  add_rule "flake.lock" high "supply-chain" "nix_lock_change" "https://runbooks.local/nix-locks" "Nix lock drift can alter system packages, overlays, and binary cache trust."
  add_rule "secrets/**" critical "security" "secret_material_change" "https://runbooks.local/secrets" "Secret material paths should be routed as incidents until proven safe."
  add_rule "**/.env*" critical "security" "env_secret_change" "https://runbooks.local/secrets" "Environment files are frequently copied into places they should never be."
}

load_rule_line() {
  local raw="$1" pattern severity owner control runbook note
  raw="$(trim "$raw")"
  [[ -n "$raw" ]] || return 0
  [[ "$raw" == \#* ]] && return 0
  if [[ "$raw" == *$'\t'* ]]; then
    IFS=$'\t' read -r pattern severity owner control runbook note <<< "$raw"
  else
    IFS='|' read -r pattern severity owner control runbook note <<< "$raw"
  fi
  if ! add_rule "${pattern:-}" "${severity:-}" "${owner:-}" "${control:-}" "${runbook:-}" "${note:-}"; then
    if [[ "$STRICT_RULES" -eq 1 ]]; then
      die "malformed rule row: $raw"
    fi
    warn "ignored malformed rule row: $raw"
  fi
}

load_rules() {
  local line
  if [[ -z "$RULES_FILE" ]]; then
    load_builtin_rules
    return 0
  fi
  [[ -f "$RULES_FILE" ]] || die "rules file not found: $RULES_FILE"
  while IFS= read -r line || [[ -n "$line" ]]; do
    load_rule_line "$line"
  done < "$RULES_FILE"
  [[ "${#RULE_PATTERNS[@]}" -gt 0 ]] || die "no valid rules were loaded"
}

pattern_matches() {
  local pattern path prefix suffix middle
  pattern="$(normalize_path "$1")"
  path="$(normalize_path "$2")"
  [[ -n "$pattern" && -n "$path" ]] || return 1
  if [[ "$pattern" == */** ]]; then
    prefix="${pattern%/**}"
    [[ "$path" == "$prefix" || "$path" == "$prefix/"* ]] && return 0
  fi
  if [[ "$pattern" == **/* ]]; then
    suffix="${pattern#**/}"
    [[ "$path" == "$suffix" || "$path" == */"$suffix" ]] && return 0
  fi
  if [[ "$pattern" == *"**"* ]]; then
    prefix="${pattern%%"**"*}"
    suffix="${pattern##*"**"}"
    if [[ "$path" == "$prefix"* && "$path" == *"$suffix" ]]; then
      middle="${path#"$prefix"}"
      middle="${middle%"$suffix"}"
      [[ -n "$middle" || "$pattern" == "$prefix**$suffix" ]] && return 0
    fi
  fi
  [[ "$path" == $pattern ]]
}

add_route() {
  local path="$1" pattern="$2" severity="$3" owner="$4" control="$5" runbook="$6" note="$7" rank
  rank="$(severity_rank "$severity")" || die "internal severity error: $severity"
  local min_rank
  min_rank="$(severity_rank "$MIN_SEVERITY")"
  [[ "$rank" -ge "$min_rank" ]] || return 0
  ROUTE_PATHS+=("$path")
  ROUTE_PATTERNS+=("$pattern")
  ROUTE_SEVERITIES+=("$severity")
  ROUTE_RANKS+=("$rank")
  ROUTE_OWNERS+=("$owner")
  ROUTE_CONTROLS+=("$control")
  ROUTE_RUNBOOKS+=("$runbook")
  ROUTE_NOTES+=("$note")
}

route_changes() {
  local path i matched
  for path in "${CHANGE_PATHS[@]}"; do
    matched=0
    for ((i = 0; i < ${#RULE_PATTERNS[@]}; i++)); do
      if pattern_matches "${RULE_PATTERNS[$i]}" "$path"; then
        matched=1
        add_route "$path" "${RULE_PATTERNS[$i]}" "${RULE_SEVERITIES[$i]}" "${RULE_OWNERS[$i]}" "${RULE_CONTROLS[$i]}" "${RULE_RUNBOOKS[$i]}" "${RULE_NOTES[$i]}"
      fi
    done
    if [[ "$matched" -eq 0 && "$QUIET_UNMATCHED" -eq 0 ]]; then
      add_route "$path" "(unmatched)" info "$DEFAULT_OWNER" "unmatched_change" "" "No policy drift rule matched this path."
    fi
  done
  [[ "${#ROUTE_PATHS[@]}" -gt 0 ]] || warn "all routes were suppressed by --min-severity or --quiet-unmatched"
}

max_rank() {
  local max=0 rank
  for rank in "${ROUTE_RANKS[@]}"; do
    [[ "$rank" -gt "$max" ]] && max="$rank"
  done
  printf '%s' "$max"
}

gate_status() {
  local max fail_rank
  if [[ "$FAIL_ON" == "never" ]]; then
    printf 'pass'
    return 0
  fi
  max="$(max_rank)"
  fail_rank="$(severity_rank "$FAIL_ON")"
  if [[ "$max" -ge "$fail_rank" ]]; then
    printf 'fail'
  else
    printf 'pass'
  fi
}

render_markdown() {
  local status max severity i
  status="$(gate_status)"
  max="$(max_rank)"
  severity="$(rank_name "$max")"
  printf '# %s Report\n\n' "$TOOL_NAME"
  printf -- '- Version: %s\n' "$TOOL_VERSION"
  printf -- '- Generated at: %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf -- '- Changed paths: %d\n' "${#CHANGE_PATHS[@]}"
  printf -- '- Route rows: %d\n' "${#ROUTE_PATHS[@]}"
  printf -- '- Max severity: %s\n' "$severity"
  printf -- '- Gate status: %s\n\n' "$status"
  if [[ "${#WARNINGS[@]}" -gt 0 ]]; then
    printf '## Warnings\n\n'
    for i in "${WARNINGS[@]}"; do
      printf -- '- %s\n' "$(md_escape "$i")"
    done
    printf '\n'
  fi
  printf '## Routes\n\n'
  if [[ "${#ROUTE_PATHS[@]}" -eq 0 ]]; then
    printf 'No route rows.\n'
    return 0
  fi
  printf '| Path | Severity | Owner | Control | Pattern | Runbook | Note |\n'
  printf '| --- | --- | --- | --- | --- | --- | --- |\n'
  for ((i = 0; i < ${#ROUTE_PATHS[@]}; i++)); do
    printf '| `%s` | %s | %s | %s | `%s` | %s | %s |\n' \
      "$(md_escape "${ROUTE_PATHS[$i]}")" \
      "$(md_escape "${ROUTE_SEVERITIES[$i]}")" \
      "$(md_escape "${ROUTE_OWNERS[$i]}")" \
      "$(md_escape "${ROUTE_CONTROLS[$i]}")" \
      "$(md_escape "${ROUTE_PATTERNS[$i]}")" \
      "$(md_escape "${ROUTE_RUNBOOKS[$i]}")" \
      "$(md_escape "${ROUTE_NOTES[$i]}")"
  done
}

render_json() {
  local status max i comma
  status="$(gate_status)"
  max="$(max_rank)"
  printf '{\n'
  printf '  "tool": "%s",\n' "$TOOL_NAME"
  printf '  "version": "%s",\n' "$TOOL_VERSION"
  printf '  "generated_at": "%s",\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf '  "status": "%s",\n' "$status"
  printf '  "max_severity": "%s",\n' "$(rank_name "$max")"
  printf '  "changed_path_count": %d,\n' "${#CHANGE_PATHS[@]}"
  printf '  "routes": [\n'
  for ((i = 0; i < ${#ROUTE_PATHS[@]}; i++)); do
    comma=","
    [[ "$i" -eq $((${#ROUTE_PATHS[@]} - 1)) ]] && comma=""
    printf '    {"path":"%s","severity":"%s","owner":"%s","control":"%s","pattern":"%s","runbook":"%s","note":"%s"}%s\n' \
      "$(json_escape "${ROUTE_PATHS[$i]}")" \
      "$(json_escape "${ROUTE_SEVERITIES[$i]}")" \
      "$(json_escape "${ROUTE_OWNERS[$i]}")" \
      "$(json_escape "${ROUTE_CONTROLS[$i]}")" \
      "$(json_escape "${ROUTE_PATTERNS[$i]}")" \
      "$(json_escape "${ROUTE_RUNBOOKS[$i]}")" \
      "$(json_escape "${ROUTE_NOTES[$i]}")" \
      "$comma"
  done
  printf '  ],\n'
  printf '  "warnings": ['
  for ((i = 0; i < ${#WARNINGS[@]}; i++)); do
    [[ "$i" -gt 0 ]] && printf ','
    printf '"%s"' "$(json_escape "${WARNINGS[$i]}")"
  done
  printf ']\n'
  printf '}\n'
}

sarif_level() {
  case "$1" in
    critical|high) printf 'error' ;;
    medium) printf 'warning' ;;
    *) printf 'note' ;;
  esac
}

render_sarif() {
  local i comma message
  printf '{\n'
  printf '  "$schema": "https://json.schemastore.org/sarif-2.1.0.json",\n'
  printf '  "version": "2.1.0",\n'
  printf '  "runs": [{\n'
  printf '    "tool": {"driver": {"name": "%s", "version": "%s", "informationUri": "https://github.com/kspavankrishna/VIBE-CODE"}},\n' "$TOOL_NAME" "$TOOL_VERSION"
  printf '    "results": [\n'
  for ((i = 0; i < ${#ROUTE_PATHS[@]}; i++)); do
    comma=","
    [[ "$i" -eq $((${#ROUTE_PATHS[@]} - 1)) ]] && comma=""
    message="${ROUTE_SEVERITIES[$i]} drift route for ${ROUTE_PATHS[$i]} owned by ${ROUTE_OWNERS[$i]}: ${ROUTE_NOTES[$i]}"
    printf '      {"ruleId":"%s","level":"%s","message":{"text":"%s"},"locations":[{"physicalLocation":{"artifactLocation":{"uri":"%s"}}}]}%s\n' \
      "$(json_escape "${ROUTE_CONTROLS[$i]}")" \
      "$(sarif_level "${ROUTE_SEVERITIES[$i]}")" \
      "$(json_escape "$message")" \
      "$(json_escape "${ROUTE_PATHS[$i]}")" \
      "$comma"
  done
  printf '    ]\n'
  printf '  }]\n'
  printf '}\n'
}

render_gha() {
  local i kind message file
  for ((i = 0; i < ${#ROUTE_PATHS[@]}; i++)); do
    case "${ROUTE_SEVERITIES[$i]}" in
      critical|high) kind="error" ;;
      medium) kind="warning" ;;
      *) kind="notice" ;;
    esac
    file="$(gha_escape "${ROUTE_PATHS[$i]}")"
    message="$(gha_escape "${ROUTE_SEVERITIES[$i]} route: owner=${ROUTE_OWNERS[$i]} control=${ROUTE_CONTROLS[$i]} pattern=${ROUTE_PATTERNS[$i]} note=${ROUTE_NOTES[$i]}")"
    printf '::%s file=%s::%s\n' "$kind" "$file" "$message"
  done
}

render_report() {
  case "$FORMAT" in
    markdown) render_markdown ;;
    json) render_json ;;
    sarif) render_sarif ;;
    gha) render_gha ;;
  esac
}

finish_with_gate() {
  if [[ "$(gate_status)" == "fail" ]]; then
    return 2
  fi
  return 0
}

run_self_test() {
  local tmp changes rules out rc self
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/policy-drift-router.XXXXXX")"
  trap 'rm -rf "$tmp"' EXIT
  changes="$tmp/changes.txt"
  rules="$tmp/rules.tsv"
  out="$tmp/out.json"
  self="${BASH_SOURCE[0]}"
  if [[ "$self" != */* ]]; then
    self="$(command -v "$self" 2>/dev/null || printf '%s' "$self")"
  fi
  cat > "$changes" <<'EOF_CHANGES'
M	.github/workflows/deploy.yml
policy/admission/tenant.rego
docs/readme.md
pipelines/carbon_offsets.sql
EOF_CHANGES
  cat > "$rules" <<'EOF_RULES'
policy/**	critical	security	opa_policy	https://runbooks/policy	Policy must replay before merge.
pipelines/**	high	data	data_pipeline	https://runbooks/data	Data contract drift.
EOF_RULES
  bash "$self" --changes "$changes" --rules "$rules" --format json --fail-on never > "$out"
  grep -q '"control":"opa_policy"' "$out" || die "self-test expected opa_policy route"
  grep -q '"path":"pipelines/carbon_offsets.sql"' "$out" || die "self-test expected pipeline route"
  grep -q '"control":"unmatched_change"' "$out" || die "self-test expected unmatched route"
  set +e
  bash "$self" --changes "$changes" --rules "$rules" --format gha --fail-on high >/dev/null
  rc=$?
  set -e
  [[ "$rc" -eq 2 ]] || die "self-test expected fail-on high to exit 2, got $rc"
  printf '%s self-test passed\n' "$TOOL_NAME"
}

main() {
  parse_args "$@"
  if [[ "$SELF_TEST" -eq 1 ]]; then
    run_self_test
    return 0
  fi
  load_changes
  load_rules
  route_changes
  render_report
  finish_with_gate
}

main "$@"

: <<'POLICY_DRIFT_INCIDENT_ROUTER_EXPLANATION'
This solves the April 2026 problem where a normal pull request can quietly change the rules that protect production: OPA policies, GitHub Actions workflows, Terraform files, Kubernetes manifests, MCP tool contracts, AI agent runtime folders, eval harnesses, edge workers, IoT control paths, database migrations, lockfiles, and data pipelines. Built because Pavan wanted a small Bash file that can run anywhere in CI before the expensive systems wake up, read only a changed-file list, and tell the team which owner, runbook, and control should look at the drift before merge. Use it when GitHub Actions, Buildkite, Jenkins, GitLab CI, local pre-push hooks, release trains, infrastructure repositories, AI coding agent repos, carbon credit data pipelines, smart device backends, or edge compute platforms need deterministic policy drift routing without a SaaS dependency. The trick: this script ships useful built-in rules, accepts a plain TSV or pipe-separated rule catalog, normalizes noisy git status input, ranks severity, emits Markdown for humans, JSON for automation, SARIF for code scanning, or GitHub annotation commands, and exits with a predictable gate code when critical or high risk changes need incident-style handling. Drop this into platform engineering repos, DevOps governance checks, AI tooling safety gates, MCP server contract review, infrastructure as code drift detection, monorepo release workflows, data platform migration review, Kubernetes admission policy review, CI workflow hardening, supply chain lockfile monitoring, and production-ready developer productivity automation where people search for policy drift detection, incident routing Bash script, GitHub Actions security gate, OPA Rego change review, Terraform drift owner routing, Kubernetes manifest risk gate, AI agent tool schema governance, SARIF DevOps scanner, edge compute deployment safety, IoT control plane review, and senior-level CI guardrails.
POLICY_DRIFT_INCIDENT_ROUTER_EXPLANATION