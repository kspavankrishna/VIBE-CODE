#!/usr/bin/env bash
# AgentWorktreePoolManager.sh -- crash-safe git worktree pool for parallel AI coding agents.
set -Eeuo pipefail
IFS=$'\n\t'

readonly SELF_NAME="AgentWorktreePoolManager"
readonly SELF_VERSION="1.0.0"

# ---------------------------------------------------------------------------
# Defaults (overridable via env, then via CLI flags)
# ---------------------------------------------------------------------------
POOL_NAME="${AWPM_POOL:-default}"
POOL_ROOT="${AWPM_ROOT:-$HOME/.cache/agent-worktree-pool}"
REPO_DIR="${AWPM_REPO:-}"
SLOT_COUNT="${AWPM_SLOTS:-8}"
BASE_REF="${AWPM_BASE:-}"
LABEL=""
ACQUIRE_TIMEOUT="${AWPM_TIMEOUT:-0}"     # 0 = fail fast, "inf" = block forever, N = seconds
CLEAN_ON_RELEASE=0
KEEP_WORKTREE=0
JSON_OUT=0
ASSUME_YES=0
FORCE_GC=0
STALE_AFTER="${AWPM_STALE_SECONDS:-21600}"  # 6h: age gate for --force reclaiming a still-held slot

# Set by cmd_run once a slot is acquired; read by cleanup_slot's EXIT trap.
# Deliberately global (not `local`) -- see the comment above cmd_run's
# acquire loop for why the trap needs these to survive an errexit unwind.
RUN_SLOT=""
RUN_META=""
RUN_WT=""
RUN_BRANCH=""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log() { printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
info() { log "INFO  $*"; }
warn() { log "WARN  $*"; }
err()  { log "ERROR $*"; }
die()  { err "$*"; exit 1; }

on_err() {
  local exit_code=$? line=${1:-?}
  err "unexpected failure at line ${line} (exit ${exit_code})"
}
trap 'on_err $LINENO' ERR

have_cmd() { command -v "$1" >/dev/null 2>&1; }

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
usage() {
  cat <<'EOF'
AgentWorktreePoolManager.sh -- run parallel AI coding agents in isolated,
crash-safe git worktrees, without them ever grabbing the same slot.

USAGE
  AgentWorktreePoolManager.sh <command> [options] [-- command args...]

COMMANDS
  run       Acquire a free slot, materialize an isolated worktree, run a
            command inside it (cwd = worktree), release the slot on exit.
  list      Human-readable table of every slot in the pool.
  status    Machine-readable JSON snapshot of the pool.
  gc        Reclaim slots whose owning process is dead and remove the
            worktrees/branches it left behind.
  prune     Tear down the entire pool (all slots) for this repo+pool name.
  doctor    Sanity-check the environment (git, flock, repo, pool root).
  help      Show this message.

GLOBAL OPTIONS
  --pool NAME       Logical pool name; lets one repo host several
                     independent pools (default: "default", or $AWPM_POOL).
  --repo PATH       Path to the git repo (default: autodetect from cwd).
  --root PATH       Root directory for pool state (default: ~/.cache/agent-worktree-pool).
  --slots N         Number of slots in the pool (default: 8).
  --json            Emit machine-readable JSON where applicable.
  -h, --help        Show this message.

RUN OPTIONS
  --base REF        Ref each acquired worktree is reset to (default: current
                     HEAD's upstream if tracked, else current HEAD).
  --label STRING    Short label folded into the branch name for this run.
  --timeout SECS    How long to wait for a free slot: 0 = fail fast (default),
                     "inf" = wait forever, N = wait up to N seconds.
  --clean-on-release  git reset --hard + git clean -fdx the worktree after
                     the wrapped command exits (default: leave it as-is).
  -- CMD [ARGS...]  Command to run inside the acquired worktree. Required.

GC / PRUNE OPTIONS
  gc always reclaims "stale" slots: no process holds the lock, but the
  worktree/branch/metadata were left behind because cleanup never ran
  (e.g. the whole process tree was killed before its EXIT trap could fire).
  A slot whose lock is STILL actually held by some process -- even one
  whose recorded owner pid is dead, which happens if only the wrapper was
  killed and a child of it survives -- is never touched by default, because
  something may still be writing into that worktree.
  --force           Also reclaim a still-held slot, once it has been held
                     by an untracked/dead-looking owner for --stale-seconds.
                     Use only when you are sure nothing is really running.
  --stale-seconds N Age gate for --force (default 21600 = 6h).
  -y, --yes         Do not prompt for confirmation (prune only).

ENVIRONMENT
  Inside a `run` command, the child process sees:
    AWPM_WORKTREE     absolute path to the isolated worktree (also $PWD)
    AWPM_BRANCH       branch name checked out in that worktree
    AWPM_SLOT         numeric slot index this run holds
    AWPM_BASE_REF     the ref the worktree was reset to

EXAMPLES
  # Run four review agents in parallel, each in its own clean worktree:
  for i in 1 2 3 4; do
    AgentWorktreePoolManager.sh run --pool review --label "reviewer-$i" \
      --clean-on-release -- ./run_review_agent.sh &
  done
  wait

  # See what's currently checked out and by whom:
  AgentWorktreePoolManager.sh list --pool review

  # Reclaim slots abandoned by crashed agents, then inspect as JSON:
  AgentWorktreePoolManager.sh gc --pool review
  AgentWorktreePoolManager.sh status --pool review --json
EOF
}

# ---------------------------------------------------------------------------
# Repo / pool resolution
# ---------------------------------------------------------------------------
resolve_repo() {
  if [[ -n "$REPO_DIR" ]]; then
    REPO_DIR="$(cd "$REPO_DIR" && git rev-parse --show-toplevel 2>/dev/null)" \
      || die "not a git repo: $REPO_DIR"
  else
    REPO_DIR="$(git rev-parse --show-toplevel 2>/dev/null)" \
      || die "not inside a git repo; pass --repo PATH"
  fi
}

repo_identity() {
  local common_dir
  common_dir="$(git -C "$REPO_DIR" rev-parse --git-common-dir)"
  common_dir="$(cd "$REPO_DIR" && cd "$common_dir" && pwd)"
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$common_dir" | sha256sum | cut -c1-16
  elif command -v shasum >/dev/null 2>&1; then
    printf '%s' "$common_dir" | shasum -a 256 | cut -c1-16
  else
    printf '%s' "$common_dir" | cksum | tr -d ' \t' | cut -c1-16
  fi
}

resolve_pool_dir() {
  local ident
  ident="$(repo_identity)"
  POOL_DIR="${POOL_ROOT}/${ident}/${POOL_NAME}"
  LOCKS_DIR="${POOL_DIR}/locks"
  META_DIR="${POOL_DIR}/meta"
  WT_DIR="${POOL_DIR}/worktrees"
}

init_pool_dirs() {
  mkdir -p "$LOCKS_DIR" "$META_DIR" "$WT_DIR"
  local marker="${POOL_DIR}/.repo"
  [[ -f "$marker" ]] || printf '%s\n' "$REPO_DIR" >"$marker"
}

slot_lock_path() { printf '%s/slot-%02d.lock' "$LOCKS_DIR" "$1"; }
slot_meta_path() { printf '%s/slot-%02d.meta' "$META_DIR" "$1"; }
slot_wt_path()   { printf '%s/slot-%02d' "$WT_DIR" "$1"; }

# ---------------------------------------------------------------------------
# Metadata (flat key=value files -- no jq dependency for correctness)
# ---------------------------------------------------------------------------
write_meta() {
  local path="$1" pid="$2" branch="$3" base="$4" label="$5"
  cat >"$path" <<META
pid=${pid}
branch=${branch}
base=${base}
label=${label}
started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
started_epoch=$(date -u '+%s')
host=$(hostname 2>/dev/null || printf 'unknown')
META
}

meta_get() {
  local path="$1" key="$2"
  [[ -f "$path" ]] || return 1
  awk -F= -v k="$key" '$1==k{print substr($0, index($0,"=")+1); found=1} END{exit !found}' "$path"
}

pid_is_alive() {
  local pid="$1"
  [[ -n "$pid" && "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

# Non-destructively probe whether a slot's lock is currently held.
# Returns 0 (free) or 1 (busy) without disturbing an existing holder.
slot_is_free() {
  local lock="$1"
  ( exec 9>"$lock"; flock -n 9 ) 2>/dev/null
}

default_base_ref() {
  local upstream
  if upstream="$(git -C "$REPO_DIR" rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null)"; then
    printf '%s' "$upstream"
  else
    git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD
  fi
}

sanitize_label() {
  local s="${1:-run}"
  s="$(printf '%s' "$s" | tr -c 'A-Za-z0-9._-' '-' | sed -e 's/-\{2,\}/-/g' -e 's/^-//' -e 's/-$//')"
  [[ -n "$s" ]] || s="run"
  printf '%s' "$s"
}

# ---------------------------------------------------------------------------
# run: acquire a slot, build/reset the worktree, exec the wrapped command
# ---------------------------------------------------------------------------
cmd_run() {
  [[ ${#CMD_ARGS[@]} -gt 0 ]] || die "run requires a command after '--'"
  [[ -n "$BASE_REF" ]] || BASE_REF="$(default_base_ref)"
  local label
  label="$(sanitize_label "$LABEL")"

  local deadline=-1
  if [[ "$ACQUIRE_TIMEOUT" != "inf" ]]; then
    deadline=$(( $(date +%s) + ACQUIRE_TIMEOUT ))
  fi

  # RUN_* are deliberately NOT `local`: cleanup_slot below is registered as
  # an EXIT trap, and bash drops a function's locals before an errexit-driven
  # EXIT trap runs, so a trap that reads `local` state here would blow up
  # with "unbound variable" on any failure between acquire and release. Since
  # cmd_run only ever runs once per process, plain globals are safe here.
  local acquired=0
  while :; do
    for (( RUN_SLOT=0; RUN_SLOT<SLOT_COUNT; RUN_SLOT++ )); do
      local lock; lock="$(slot_lock_path "$RUN_SLOT")"
      exec 9>"$lock"
      if flock -n 9; then
        acquired=1
        break
      fi
      exec 9>&-
    done
    [[ $acquired -eq 1 ]] && break
    if [[ "$ACQUIRE_TIMEOUT" == "0" ]]; then
      die "pool '${POOL_NAME}' has no free slot (all ${SLOT_COUNT} busy); use --timeout or increase --slots"
    fi
    if [[ $deadline -ge 0 && $(date +%s) -ge $deadline ]]; then
      die "timed out after ${ACQUIRE_TIMEOUT}s waiting for a free slot in pool '${POOL_NAME}'"
    fi
    sleep 0.5
  done

  # From here on, fd 9 stays open for the lifetime of THIS process. If we
  # die (even kill -9), the kernel closes the fd and the flock is released
  # automatically -- no stale-lock cleanup step is ever required for
  # correctness, only for tidiness (handled by `gc`).
  RUN_META="$(slot_meta_path "$RUN_SLOT")"
  RUN_WT="$(slot_wt_path "$RUN_SLOT")"
  RUN_BRANCH="agent/${POOL_NAME}/${label}/slot${RUN_SLOT}-$(date -u '+%Y%m%dT%H%M%SZ')"

  write_meta "$RUN_META" "$$" "$RUN_BRANCH" "$BASE_REF" "$label"
  info "slot ${RUN_SLOT}: acquired for label='${label}' base='${BASE_REF}' pid=$$"
  trap cleanup_slot EXIT

  if [[ -d "$RUN_WT/.git" || -f "$RUN_WT/.git" ]]; then
    git -C "$REPO_DIR" fetch --quiet --no-tags -- "$(printf '%s' "$BASE_REF" | cut -d/ -f1)" 2>/dev/null || true
    git -C "$RUN_WT" checkout -B "$RUN_BRANCH" "$BASE_REF" --quiet
    git -C "$RUN_WT" reset --hard "$BASE_REF" --quiet
    git -C "$RUN_WT" clean -fdx --quiet
  else
    mkdir -p "$WT_DIR"
    # A worktree directory that vanished without going through `git worktree
    # remove` (a manual rm -rf, a wiped cache volume) leaves the main repo's
    # .git still believing it's registered ("missing but already registered
    # worktree"), which makes a plain `add` fail forever. Prune once and
    # retry before giving up, so the pool self-heals instead of wedging.
    local add_err; add_err="$(mktemp)"
    if ! git -C "$REPO_DIR" worktree add -B "$RUN_BRANCH" "$RUN_WT" "$BASE_REF" --quiet 2>"$add_err"; then
      if grep -q "already registered worktree" "$add_err" 2>/dev/null; then
        warn "slot ${RUN_SLOT}: stale worktree registration for ${RUN_WT}; pruning and retrying"
        git -C "$REPO_DIR" worktree prune
        rm -f "$add_err"
        git -C "$REPO_DIR" worktree add -B "$RUN_BRANCH" "$RUN_WT" "$BASE_REF" --quiet
      else
        cat "$add_err" >&2
        rm -f "$add_err"
        die "slot ${RUN_SLOT}: git worktree add failed"
      fi
    else
      rm -f "$add_err"
    fi
  fi

  info "slot ${RUN_SLOT}: worktree ready at ${RUN_WT} on branch ${RUN_BRANCH}"

  local status=0
  ( cd "$RUN_WT" && \
    AWPM_WORKTREE="$RUN_WT" AWPM_BRANCH="$RUN_BRANCH" AWPM_SLOT="$RUN_SLOT" AWPM_BASE_REF="$BASE_REF" \
    "${CMD_ARGS[@]}" ) || status=$?

  exit "$status"
}

cleanup_slot() {
  local rc=$?
  if [[ $KEEP_WORKTREE -eq 0 && $CLEAN_ON_RELEASE -eq 1 && -d "$RUN_WT" ]]; then
    info "slot ${RUN_SLOT}: cleaning worktree (reset --hard + clean -fdx)"
    git -C "$RUN_WT" reset --hard "$BASE_REF" >/dev/null 2>&1 || true
    git -C "$RUN_WT" clean -fdx >/dev/null 2>&1 || true
  fi
  rm -f "$RUN_META"
  info "slot ${RUN_SLOT}: released (exit ${rc})"
  exec 9>&- 2>/dev/null || true
  return "$rc"
}

# ---------------------------------------------------------------------------
# list / status
# ---------------------------------------------------------------------------
collect_slots() {
  # Emits one line per slot: slot|state|pid|branch|label|started_at|path
  local slot
  for (( slot=0; slot<SLOT_COUNT; slot++ )); do
    local lock meta wt state pid branch label started path
    lock="$(slot_lock_path "$slot")"
    meta="$(slot_meta_path "$slot")"
    wt="$(slot_wt_path "$slot")"
    path="$wt"
    pid="$(meta_get "$meta" pid || true)"
    branch="$(meta_get "$meta" branch || true)"
    label="$(meta_get "$meta" label || true)"
    started="$(meta_get "$meta" started_at || true)"

    if [[ -f "$lock" ]] && ! slot_is_free "$lock"; then
      # Someone (the run wrapper, or a child that inherited its fd) really
      # does hold this lock right now. Trust the lock over the pid: a dead
      # recorded pid with a live lock means a descendant is still working.
      if pid_is_alive "${pid:-}"; then
        state="busy"
      else
        state="held"
      fi
    elif [[ -f "$meta" ]]; then
      # Lock is genuinely free, but the metadata file is still here -- the
      # run that owned this slot never got to run its EXIT trap (hard kill
      # before cleanup). A leftover worktree dir on its own is NOT stale:
      # `run` deliberately reuses it next time via `git checkout -B`, so a
      # cleanly-released slot with no meta is just "free", not something
      # to remove.
      state="stale"
    else
      state="free"
    fi
    printf '%s|%s|%s|%s|%s|%s|%s\n' "$slot" "$state" "${pid:--}" "${branch:--}" "${label:--}" "${started:--}" "$path"
  done
}

cmd_list() {
  init_pool_dirs
  printf '%-4s %-9s %-8s %-40s %-14s %s\n' "SLOT" "STATE" "PID" "BRANCH" "LABEL" "PATH"
  while IFS='|' read -r slot state pid branch label started path; do
    printf '%-4s %-9s %-8s %-40s %-14s %s\n' "$slot" "$state" "$pid" "$branch" "$label" "$path"
  done < <(collect_slots)
}

json_escape() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '%s' "$s"
}

cmd_status() {
  init_pool_dirs
  local first=1
  printf '{"pool":"%s","repo":"%s","slots":%d,"items":[' \
    "$(json_escape "$POOL_NAME")" "$(json_escape "$REPO_DIR")" "$SLOT_COUNT"
  while IFS='|' read -r slot state pid branch label started path; do
    [[ $first -eq 1 ]] || printf ','
    first=0
    printf '{"slot":%s,"state":"%s","pid":"%s","branch":"%s","label":"%s","started_at":"%s","path":"%s"}' \
      "$slot" "$(json_escape "$state")" "$(json_escape "$pid")" "$(json_escape "$branch")" \
      "$(json_escape "$label")" "$(json_escape "$started")" "$(json_escape "$path")"
  done < <(collect_slots)
  printf ']}\n'
}

# ---------------------------------------------------------------------------
# gc: reclaim slots that are safe to reclaim.
#
# "stale"  = lock is genuinely free (no fd anywhere holds it) but leftover
#            worktree/branch/meta remain because the EXIT trap never ran.
#            Always safe: nobody can be using files nobody has locked.
# "held"   = lock is STILL held by some process, even though the pid we
#            recorded for it looks dead (a child inherited the fd and
#            outlived the wrapper). NOT touched unless --force is given
#            and the slot has looked this way for --stale-seconds, because
#            the filesystem underneath it may still be actively written to.
# ---------------------------------------------------------------------------
reclaim_slot() {
  local slot="$1" branch="$2" path="$3"
  rm -f "$(slot_meta_path "$slot")"
  if [[ -d "$path" ]]; then
    git -C "$REPO_DIR" worktree remove --force "$path" 2>/dev/null || rm -rf "$path"
  fi
  git -C "$REPO_DIR" worktree prune >/dev/null 2>&1 || true
  if [[ "$branch" != "-" && -n "$branch" ]]; then
    git -C "$REPO_DIR" branch -D "$branch" >/dev/null 2>&1 || true
  fi
}

cmd_gc() {
  init_pool_dirs
  local reclaimed=0
  while IFS='|' read -r slot state pid branch label started path; do
    case "$state" in
      stale)
        info "slot ${slot}: reclaiming stale leftovers (branch ${branch}, label ${label})"
        reclaim_slot "$slot" "$branch" "$path"
        reclaimed=$((reclaimed + 1))
        ;;
      held)
        if [[ $FORCE_GC -ne 1 ]]; then
          warn "slot ${slot}: still locked by a live process (owner pid ${pid} not tracked); use --force to override, only if you're sure nothing is running"
          continue
        fi
        local epoch age=0
        epoch="$(meta_get "$(slot_meta_path "$slot")" started_epoch 2>/dev/null || true)"
        [[ -n "$epoch" ]] && age=$(( $(date +%s) - epoch ))
        if [[ $age -lt $STALE_AFTER ]]; then
          warn "slot ${slot}: still locked, younger than --stale-seconds (${age}s); leaving for now"
          continue
        fi
        warn "slot ${slot}: force-reclaiming a still-locked slot (age ${age}s, branch ${branch}) -- may disrupt work still in flight"
        reclaim_slot "$slot" "$branch" "$path"
        reclaimed=$((reclaimed + 1))
        ;;
      *) continue ;;
    esac
  done < <(collect_slots)
  info "gc complete: ${reclaimed} slot(s) reclaimed"
}

# ---------------------------------------------------------------------------
# prune: tear down the whole pool for this repo+name
# ---------------------------------------------------------------------------
cmd_prune() {
  init_pool_dirs
  if [[ $ASSUME_YES -eq 0 ]]; then
    read -r -p "Remove pool '${POOL_NAME}' (${SLOT_COUNT} slots) under ${POOL_DIR}? [y/N] " reply
    [[ "$reply" =~ ^[Yy]$ ]] || { info "aborted"; return 0; }
  fi
  local slot
  for (( slot=0; slot<SLOT_COUNT; slot++ )); do
    local wt; wt="$(slot_wt_path "$slot")"
    [[ -d "$wt" ]] && git -C "$REPO_DIR" worktree remove --force "$wt" 2>/dev/null
  done
  git -C "$REPO_DIR" worktree prune >/dev/null 2>&1 || true
  rm -rf "$POOL_DIR"
  info "pool '${POOL_NAME}' removed"
}

# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------
cmd_doctor() {
  local ok=1
  have_cmd git   || { err "required command not found: git"; ok=0; }
  have_cmd flock || { err "required command not found: flock"; ok=0; }
  local gv; gv="$(git --version 2>/dev/null | awk '{print $3}')"
  info "git version: ${gv:-unknown}"
  git -C "$REPO_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
    && info "repo OK: ${REPO_DIR}" \
    || { err "not a usable git repo: ${REPO_DIR}"; ok=0; }
  mkdir -p "$POOL_ROOT" 2>/dev/null && [[ -w "$POOL_ROOT" ]] \
    && info "pool root writable: ${POOL_ROOT}" \
    || { err "pool root not writable: ${POOL_ROOT}"; ok=0; }
  if [[ $ok -eq 1 ]]; then
    info "doctor: all checks passed"
  else
    die "doctor: one or more checks failed"
  fi
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
COMMAND="${1:-}"
[[ $# -gt 0 ]] && shift || true
CMD_ARGS=()

case "$COMMAND" in
  run|list|status|gc|prune|doctor|help|-h|--help|"") ;;
  *) die "unknown command: ${COMMAND} (see --help)" ;;
esac

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pool) POOL_NAME="$2"; shift 2 ;;
    --repo) REPO_DIR="$2"; shift 2 ;;
    --root) POOL_ROOT="$2"; shift 2 ;;
    --slots) SLOT_COUNT="$2"; shift 2 ;;
    --base) BASE_REF="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --timeout) ACQUIRE_TIMEOUT="$2"; shift 2 ;;
    --stale-seconds) STALE_AFTER="$2"; shift 2 ;;
    --clean-on-release) CLEAN_ON_RELEASE=1; shift ;;
    --keep) KEEP_WORKTREE=1; shift ;;
    --json) JSON_OUT=1; shift ;;
    -y|--yes) ASSUME_YES=1; shift ;;
    --force) FORCE_GC=1; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; CMD_ARGS=("$@"); break ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

[[ "$SLOT_COUNT" =~ ^[0-9]+$ && "$SLOT_COUNT" -gt 0 ]] || die "--slots must be a positive integer"

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
main() {
  case "$COMMAND" in
    help|-h|--help|"") usage; exit 0 ;;
  esac

  resolve_repo
  resolve_pool_dir
  init_pool_dirs

  case "$COMMAND" in
    run)    cmd_run ;;
    list)   [[ $JSON_OUT -eq 1 ]] && cmd_status || cmd_list ;;
    status) cmd_status ;;
    gc)     cmd_gc ;;
    prune)  cmd_prune ;;
    doctor) cmd_doctor ;;
  esac
}

main "$@"

# =============================================================================
# WHAT THIS IS AND WHY IT EXISTS -- read this before you use or fork it
# =============================================================================
#
# This solves the race condition you hit the moment you try to run more than
# one AI coding agent against the same git repository at the same time. Two
# agents, two branches, one working tree -- and now agent A's half-finished
# edit is sitting in the files agent B just checked out, or worse, both
# agents grab the same worktree slot and stomp each other's commits. Bare
# "git worktree add" by itself does not stop that: it happily lets two
# processes race to create worktrees at the same path, and if either agent
# crashes (OOM kill, timeout, a bad `rm -rf` in its own generated code) you
# are left with a directory nobody owns, a branch nobody remembers, and a
# lock file that looks held forever. This script is a small, fixed-size pool
# of git worktrees, guarded by flock, that hands out isolated slots to agent
# runs one at a time and gets them back automatically, even when the agent
# dies badly.
#
# Built because I run multiple Claude Code / Codex style agents against the
# same repo in parallel -- one reviewing a PR, one fixing a flaky test, one
# drafting a refactor -- and I got tired of writing the same "mkdir a random
# temp dir, git worktree add, hope nothing collides, remember to clean up"
# glue script for every project. The core trick here (holding an flock file
# descriptor open across the child process's entire lifetime) is the same
# pattern used by systemd and by database connection pools: the operating
# system, not your error handling, is what guarantees the lock gets released
# when the holder dies, including a hard kill -9 or an out-of-memory kill.
# You do not need a heartbeat thread or a lockfile-with-timestamp hack to get
# correctness; you only need `gc` afterward to physically tidy up the
# worktree and branch that a dead agent left behind, because tidiness is a
# housekeeping problem, not a correctness problem.
#
# Use it when you are orchestrating parallel AI coding agents, parallel CI
# jobs, or parallel long running scripts that each need their own clean
# checkout of the same repository and cannot be trusted to coordinate with
# each other. It also works for plain old parallel test sharding or parallel
# codemods where you want N isolated working directories without N manual
# git clones eating your disk.
#
# The trick: acquisition and release are not two separate steps you have to
# remember to pair up. `run` opens a file descriptor on the slot's lock file,
# takes a non-blocking flock on it, and keeps that descriptor open for the
# entire lifetime of the wrapped command, which runs as a real child process
# (not exec'd away) so a bash EXIT trap still fires afterward to reset the
# worktree and delete the metadata file. If the whole process tree dies
# instead of exiting cleanly, the kernel closes every copy of that
# descriptor and the lock releases itself for real, no matter how ugly the
# death was. Where it gets subtle is a kill that only hits the wrapper and
# leaves a grandchild running: that grandchild still holds the same
# descriptor, so the lock correctly stays held even though the pid this
# script recorded is dead, because someone might still be writing into that
# worktree. `gc` only ever removes a slot once the lock itself reports
# free ("stale": leftovers with nobody holding them); a slot that is still
# genuinely locked shows up as "held" and gc leaves it alone unless you pass
# --force with an age past --stale-seconds, on purpose, because you decided
# it is safe. Nothing here depends on jq, python, or any non-POSIX tool
# beyond git and flock (util-linux), so it runs unmodified on any normal
# Linux dev box or CI runner.
#
# Drop this into the root of any git repository (or point --repo at one) and
# call `AgentWorktreePoolManager.sh run --pool agents --label fix-flaky-test
# --clean-on-release -- <your agent command>` from as many parallel shells
# as you like; run `list` or `status --json` to see what is checked out
# right now, and `gc` on a schedule (cron, or at the top of your CI job) to
# sweep up anything a crashed run left behind. Search terms if you found
# this looking for the same problem: git worktree pool, parallel git
# worktrees, flock lock file bash, AI agent sandbox isolation, crash-safe
# git worktree cleanup, parallel coding agent orchestration script.
#
# -- Pavan
