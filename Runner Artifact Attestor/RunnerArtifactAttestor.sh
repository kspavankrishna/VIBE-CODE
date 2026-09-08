#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

PROGRAM=${0##*/}
VERSION=1.0.0
MODE=verify
ROOT=.
MANIFEST=
PROVENANCE=
EXPECTED_COMMIT=${GITHUB_SHA:-}
EXPECTED_REPO=${GITHUB_REPOSITORY:-}
ALLOW_EXTRA=0
STRICT_MTIME=0
SKIP_SECRET_SCAN=0
REQUIRE_PROVENANCE_DIGESTS=0
REQUIRE_PROTECTED_REF=0
MAX_SCAN_BYTES=$((1024 * 1024))
MAX_FILE_BYTES=0
FAILURES=0
WARNINGS=0
CHECKED=0
BYTES_SEEN=0
TMPDIR=

DEFAULT_ALLOW_GLOBS=(
  'dist/*' 'build/*' 'out/*' 'release/*' 'artifacts/*' 'target/release/*'
  '*.zip' '*.tar' '*.tar.gz' '*.tgz' '*.jar' '*.war' '*.whl' '*.gem'
  '*.nupkg' '*.deb' '*.rpm' '*.apk' '*.ipa' '*.wasm' '*.so' '*.dylib'
  '*.dll' '*.exe' '*.sbom.json' '*.intoto.jsonl' '*.provenance.json'
)
ALLOW_GLOBS=()
DENY_GLOBS=(
  '.git/*' '.github/*' 'node_modules/*' 'vendor/*' '.terraform/*'
  '*.pem' '*.key' '*.p12' '*.pfx' '*.env' '.env' '.env.*' '*id_rsa*'
)
SECRET_PATTERNS=(
  'AKIA[0-9A-Z]{16}'
  'ASIA[0-9A-Z]{16}'
  'gh[pousr]_[A-Za-z0-9_]{30,}'
  'github_pat_[A-Za-z0-9_]{30,}'
  'sk-[A-Za-z0-9]{20,}'
  'AIza[0-9A-Za-z_-]{30,}'
  'xox[baprs]-[0-9A-Za-z-]{20,}'
  'BEGIN (RSA |OPENSSH |EC |DSA )?PRIVATE KEY'
)

usage() {
  cat <<'USAGE'
RunnerArtifactAttestor.sh create|verify|audit [options]

Creates and verifies a deterministic artifact manifest for ephemeral CI runners.
It is designed for release jobs, AI generated build pipelines, and deploy gates
where artifacts must be proven unchanged before upload.

Options:
  --root DIR                    Artifact root, default: current directory
  --manifest FILE               Manifest path. create writes it; verify reads it
  --provenance FILE             SLSA, in-toto, or GitHub artifact provenance JSON
  --expected-commit SHA         Commit that must appear in provenance
  --expected-repo OWNER/REPO     Repository that must appear in provenance
  --allow GLOB                  Include paths matching glob, repeatable
  --deny GLOB                   Exclude paths matching glob, repeatable
  --allow-extra                 Do not fail on files missing from manifest
  --strict-mtime                Require mtime from manifest to match exactly
  --require-provenance-digests  Every manifest digest must appear in provenance
  --require-protected-ref       Fail on GitHub Actions unless ref is protected
  --max-scan-bytes N            Secret scan limit per file, default 1048576
  --max-file-bytes N            Reject artifact files larger than N bytes
  --skip-secret-scan            Disable built-in high confidence secret checks
  --help                        Show this help

Manifest format:
  sha256<TAB>size_bytes<TAB>mtime_epoch<TAB>relative_path
USAGE
}

note() { printf '%s\n' "$*" >&2; }
warn() { WARNINGS=$((WARNINGS + 1)); printf 'warn: %s\n' "$*" >&2; }
fail() { FAILURES=$((FAILURES + 1)); printf 'fail: %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 2; }
need_value() { [[ $# -ge 2 && -n ${2:-} ]] || die "$1 requires a value"; }

cleanup() {
  [[ -n ${TMPDIR:-} && -d ${TMPDIR:-} ]] && rm -rf "$TMPDIR"
}
trap cleanup EXIT

command_exists() { command -v "$1" >/dev/null 2>&1; }

sha256_file() {
  local file=$1
  if command_exists sha256sum; then
    sha256sum -b "$file" | awk '{print $1}'
  elif command_exists shasum; then
    shasum -a 256 "$file" | awk '{print $1}'
  else
    die 'sha256sum or shasum is required'
  fi
}

file_size() {
  local file=$1
  stat -c '%s' "$file" 2>/dev/null || stat -f '%z' "$file"
}

file_mtime() {
  local file=$1
  stat -c '%Y' "$file" 2>/dev/null || stat -f '%m' "$file"
}

is_hex_sha256() { [[ $1 =~ ^[0-9a-fA-F]{64}$ ]]; }
is_integer() { [[ $1 =~ ^[0-9]+$ ]]; }

safe_path() {
  local path=$1
  [[ -n $path ]] || return 1
  [[ $path != /* ]] || return 1
  [[ $path != .* ]] || return 1
  [[ $path != *$'\t'* ]] || return 1
  [[ $path != *$'\r'* ]] || return 1
  [[ $path != *$'\n'* ]] || return 1
  [[ $path != *//* ]] || return 1
  [[ $path != '..' && $path != ../* && $path != */../* && $path != */.. ]] || return 1
  return 0
}

matches_any() {
  local path=$1 pattern
  shift || true
  for pattern in "$@"; do
    [[ $path == $pattern ]] && return 0
  done
  return 1
}

included_path() {
  local path=$1
  local allow=("${ALLOW_GLOBS[@]}")
  [[ ${#allow[@]} -gt 0 ]] || allow=("${DEFAULT_ALLOW_GLOBS[@]}")
  matches_any "$path" "${DENY_GLOBS[@]}" && return 1
  matches_any "$path" "${allow[@]}" && return 0
  return 1
}

resolve_inputs() {
  [[ -d $ROOT ]] || die "root does not exist: $ROOT"
  ROOT=$(cd "$ROOT" && pwd -P)
  if [[ -n $MANIFEST ]]; then
    local parent
    parent=$(dirname "$MANIFEST")
    [[ -d $parent ]] || [[ $MODE == create ]] || die "manifest directory does not exist: $parent"
  elif [[ $MODE == create ]]; then
    MANIFEST=$ROOT/.runner-artifact-manifest.tsv
  elif [[ $MODE == verify ]]; then
    MANIFEST=$ROOT/.runner-artifact-manifest.tsv
  fi
  if [[ $MODE == verify && ! -f $MANIFEST ]]; then
    die "manifest not found: $MANIFEST"
  fi
  [[ -z $PROVENANCE || -f $PROVENANCE ]] || die "provenance not found: $PROVENANCE"
  TMPDIR=$(mktemp -d)
}

parse_args() {
  if [[ $# -gt 0 && ${1:-} != --* ]]; then
    MODE=$1
    shift
  fi
  case $MODE in create|verify|audit) ;; *) die "unknown mode: $MODE" ;; esac
  while [[ $# -gt 0 ]]; do
    case $1 in
      --root) need_value "$@"; ROOT=$2; shift 2 ;;
      --manifest) need_value "$@"; MANIFEST=$2; shift 2 ;;
      --provenance) need_value "$@"; PROVENANCE=$2; shift 2 ;;
      --expected-commit) need_value "$@"; EXPECTED_COMMIT=$2; shift 2 ;;
      --expected-repo) need_value "$@"; EXPECTED_REPO=$2; shift 2 ;;
      --allow) need_value "$@"; ALLOW_GLOBS+=("$2"); shift 2 ;;
      --deny) need_value "$@"; DENY_GLOBS+=("$2"); shift 2 ;;
      --allow-extra) ALLOW_EXTRA=1; shift ;;
      --strict-mtime) STRICT_MTIME=1; shift ;;
      --skip-secret-scan) SKIP_SECRET_SCAN=1; shift ;;
      --require-provenance-digests) REQUIRE_PROVENANCE_DIGESTS=1; shift ;;
      --require-protected-ref) REQUIRE_PROTECTED_REF=1; shift ;;
      --max-scan-bytes) need_value "$@"; MAX_SCAN_BYTES=$2; shift 2 ;;
      --max-file-bytes) need_value "$@"; MAX_FILE_BYTES=$2; shift 2 ;;
      --help|-h) usage; exit 0 ;;
      *) die "unknown option: $1" ;;
    esac
  done
  is_integer "$MAX_SCAN_BYTES" || die '--max-scan-bytes must be an integer'
  is_integer "$MAX_FILE_BYTES" || die '--max-file-bytes must be an integer'
}

scan_secrets() {
  local file=$1 rel=$2 size=$3 pattern match
  [[ $SKIP_SECRET_SCAN -eq 0 ]] || return 0
  (( size <= MAX_SCAN_BYTES )) || return 0
  for pattern in "${SECRET_PATTERNS[@]}"; do
    if match=$(LC_ALL=C grep -aEInm 1 "$pattern" "$file" 2>/dev/null); then
      fail "possible secret in $rel: ${match%%:*} matches $pattern"
      return 1
    fi
  done
  return 0
}

list_artifacts() {
  (cd "$ROOT" && find . -type f -print) | while IFS= read -r raw; do
    local path=${raw#./}
    safe_path "$path" || continue
    included_path "$path" || continue
    printf '%s\n' "$path"
  done | LC_ALL=C sort
}

check_symlinks() {
  local found=0 raw path
  while IFS= read -r raw; do
    path=${raw#./}
    safe_path "$path" || continue
    included_path "$path" || continue
    fail "artifact path is a symlink, not a regular file: $path"
    found=1
  done < <(cd "$ROOT" && find . -type l -print)
  return $found
}

write_manifest() {
  local out=$TMPDIR/manifest.tsv path file size mtime digest
  check_symlinks || true
  {
    printf '# RunnerArtifactAttestor %s\n' "$VERSION"
    printf '# created_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '# fields=sha256 size_bytes mtime_epoch relative_path\n'
    while IFS= read -r path; do
      file=$ROOT/$path
      size=$(file_size "$file")
      mtime=$(file_mtime "$file")
      digest=$(sha256_file "$file")
      CHECKED=$((CHECKED + 1))
      BYTES_SEEN=$((BYTES_SEEN + size))
      if (( MAX_FILE_BYTES > 0 && size > MAX_FILE_BYTES )); then
        fail "artifact exceeds max file size: $path has $size bytes"
      fi
      scan_secrets "$file" "$path" "$size" || true
      printf '%s\t%s\t%s\t%s\n' "$digest" "$size" "$mtime" "$path"
    done < <(list_artifacts)
  } > "$out"
  if (( CHECKED == 0 )); then
    fail 'no artifacts matched the allow globs'
  fi
  if (( FAILURES == 0 )); then
    mkdir -p "$(dirname "$MANIFEST")"
    mv "$out" "$MANIFEST"
    note "wrote manifest: $MANIFEST"
  fi
}

verify_manifest() {
  local expected=$TMPDIR/expected.txt
  local line digest size mtime path extra file actual_size actual_mtime actual_digest
  : > "$expected"
  check_symlinks || true
  while IFS= read -r line || [[ -n $line ]]; do
    [[ -z $line || $line == \#* ]] && continue
    IFS=$'\t' read -r digest size mtime path extra <<< "$line"
    if [[ -n ${extra:-} ]]; then
      fail "manifest line has too many fields: $line"
      continue
    fi
    if ! is_hex_sha256 "$digest" || ! is_integer "$size" || ! is_integer "$mtime" || ! safe_path "$path"; then
      fail "invalid manifest entry: $line"
      continue
    fi
    printf '%s\n' "$path" >> "$expected"
    file=$ROOT/$path
    if [[ ! -f $file ]]; then
      fail "manifest path is missing: $path"
      continue
    fi
    actual_size=$(file_size "$file")
    actual_mtime=$(file_mtime "$file")
    actual_digest=$(sha256_file "$file")
    CHECKED=$((CHECKED + 1))
    BYTES_SEEN=$((BYTES_SEEN + actual_size))
    [[ $actual_size == "$size" ]] || fail "size mismatch for $path: expected $size got $actual_size"
    [[ $actual_digest == "$digest" ]] || fail "sha256 mismatch for $path"
    if (( STRICT_MTIME == 1 )); then
      [[ $actual_mtime == "$mtime" ]] || fail "mtime mismatch for $path: expected $mtime got $actual_mtime"
    fi
    if (( MAX_FILE_BYTES > 0 && actual_size > MAX_FILE_BYTES )); then
      fail "artifact exceeds max file size: $path has $actual_size bytes"
    fi
    scan_secrets "$file" "$path" "$actual_size" || true
  done < "$MANIFEST"
  if (( CHECKED == 0 )); then
    fail 'manifest did not contain any artifact entries'
  fi
  if (( ALLOW_EXTRA == 0 )); then
    local seen
    while IFS= read -r seen; do
      grep -Fxq -- "$seen" "$expected" || fail "extra artifact not present in manifest: $seen"
    done < <(list_artifacts)
  fi
}

check_provenance() {
  [[ -n $PROVENANCE ]] || return 0
  [[ -z $EXPECTED_COMMIT ]] || grep -Fqi -- "$EXPECTED_COMMIT" "$PROVENANCE" || fail "expected commit not found in provenance: $EXPECTED_COMMIT"
  [[ -z $EXPECTED_REPO ]] || grep -Fqi -- "$EXPECTED_REPO" "$PROVENANCE" || fail "expected repository not found in provenance: $EXPECTED_REPO"
  if (( REQUIRE_PROVENANCE_DIGESTS == 1 )); then
    local digest
    while IFS=$'\t' read -r digest _; do
      [[ -z $digest || $digest == \#* ]] && continue
      grep -Fqi -- "$digest" "$PROVENANCE" || fail "manifest digest missing from provenance: $digest"
    done < "$MANIFEST"
  fi
}

check_runner_context() {
  if [[ -n ${GITHUB_ACTIONS:-} ]]; then
    [[ -z ${GITHUB_SHA:-} || ${GITHUB_SHA:-} =~ ^[0-9a-fA-F]{40}$ ]] || fail "GITHUB_SHA is not a 40 character commit SHA: ${GITHUB_SHA:-}"
    [[ -z ${GITHUB_REPOSITORY:-} || ${GITHUB_REPOSITORY:-} == */* ]] || fail "GITHUB_REPOSITORY is malformed: ${GITHUB_REPOSITORY:-}"
    if (( REQUIRE_PROTECTED_REF == 1 )) && [[ ${GITHUB_REF_PROTECTED:-false} != true ]]; then
      fail 'release gate requires a protected GitHub ref, but GITHUB_REF_PROTECTED is not true'
    fi
  else
    warn 'not running inside GitHub Actions; provenance and runner checks are limited'
  fi
}

summary() {
  if (( FAILURES == 0 )); then
    note "ok: checked $CHECKED artifacts, $BYTES_SEEN bytes, $WARNINGS warnings"
    exit 0
  fi
  note "failed: $FAILURES failures, $WARNINGS warnings, checked $CHECKED artifacts"
  exit 1
}

main() {
  parse_args "$@"
  resolve_inputs
  check_runner_context
  case $MODE in
    create) write_manifest ;;
    verify) verify_manifest; check_provenance ;;
    audit) check_symlinks || true; while IFS= read -r path; do file=$ROOT/$path; size=$(file_size "$file"); CHECKED=$((CHECKED + 1)); BYTES_SEEN=$((BYTES_SEEN + size)); scan_secrets "$file" "$path" "$size" || true; done < <(list_artifacts) ;;
  esac
  summary
}

main "$@"

# Explanation for search, maintenance, and future readers:
# This solves the April 2026 problem where a release job, AI coding agent, or self hosted GitHub Actions runner builds an artifact and nobody can quickly prove that the exact bytes being uploaded are the same bytes that were reviewed, scanned, and tied to the expected commit. Built because Pavan has seen too many pipelines trust a zip file, Docker layer, wheel, jar, wasm bundle, SLSA provenance file, or SBOM just because it exists in the workspace after the build step. Use it when you need a Bash artifact attestation gate, GitHub Actions artifact provenance verifier, CI release manifest checker, in-toto digest guard, SLSA provenance sanity check, secret scanner for build outputs, or a simple DevOps script that works before upload-artifact, npm publish, PyPI publish, container signing, edge deploy, mobile release, or model artifact promotion. The trick: it treats the artifact directory as evidence, writes or verifies a tab separated manifest with sha256, size, mtime, and relative path, rejects unsafe paths and symlinks, checks for high confidence leaked tokens, and optionally makes sure the expected repository, commit, and digests appear in provenance JSON without needing a network call or a stored secret. Drop this into a release workflow, platform engineering template, internal developer portal, research model packaging job, or edge compute deploy step so the final publish stage has a plain, auditable reason to continue or stop.