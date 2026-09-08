# Agent Worktree Pool Manager

Two AI coding agents running against the same git repo at the same time will fight over the working tree, and one of them will lose work. This is a fixed size pool of git worktrees, guarded by flock, that hands out isolated slots and takes them back automatically even when an agent dies badly.

**Language:** Bash | **Lines:** 626 | **Added:** 2026-08-30

## What this solves

This solves the race condition you hit the moment you run more than one AI coding agent against the same git repository at the same time. Two agents, two branches, one working tree, and now agent A's half finished edit is sitting in the files agent B just checked out. Or worse, both agents grab the same worktree path and stomp each other's commits. Bare `git worktree add` does not stop that: it lets two processes race to create worktrees at the same path, and the loser dies deep inside a script never written to expect it.

The failure is quiet and expensive. An agent that reads a file mid write by another agent does not crash, it reasons about garbage and produces a confident, wrong diff. A sharded test suite in one checkout reports failures belonging to a different shard. You notice three commits later, bisecting something that never made sense.

The second failure mode is crash debris. Agents get OOM killed, hit orchestrator timeouts or run `rm -rf` in their own generated code, leaving a worktree nobody owns, a branch nobody remembers and a lock that looks held forever. If that directory is deleted by hand or the cache volume is wiped, the main repo's `.git` still believes the worktree is registered, so every future `git worktree add` at that path fails with "missing but already registered worktree". The pool wedges permanently and the next agent run dies at startup.

The naive fix is a lockfile holding a PID and a timestamp, and it is wrong in the interesting case. Kill the wrapper while a grandchild survives and the recorded PID is dead while a process is still writing into that worktree. A PID based reaper deletes files out from under live work. This script treats the kernel's view of the lock as truth and the recorded PID as a hint, which is what makes reclaiming safe.

## Why I built it

Built because I run multiple Claude Code and Codex style agents against the same repo in parallel, one reviewing a PR, one fixing a flaky test, one drafting a refactor, and I got tired of writing the same "mkdir a random temp dir, git worktree add, hope nothing collides, remember to clean up" glue script for every project. Every version of that glue was subtly different and every one of them leaked worktrees.

The gap is real. `git worktree` gives you isolation but no allocation and no lifecycle. Agent orchestrators give you concurrency but assume the filesystem is somebody else's problem. Nothing in between hands out a bounded number of clean checkouts, survives a `kill -9`, and cleans up afterwards without a daemon.

## When to use it

- Running four review agents at once against one repo, each needing its own clean checkout on its own branch.
- A CI job sharding a test suite across N processes without N full clones eating disk.
- Long running codemods that want an isolated tree per target module, bounded so you do not fill the disk.
- Any agent you cannot trust not to `git checkout` under another agent's feet.
- After a batch of agents got OOM killed and left worktrees and branches nobody can identify.
- When you need to see, right now, which parallel runs are live and which are debris.

## How it works

The pool lives outside the repo, under `POOL_ROOT` (default `~/.cache/agent-worktree-pool`), keyed by repo and pool name. `repo_identity` hashes the absolute `git rev-parse --git-common-dir` path with `sha256sum`, falling back to `shasum -a 256` then `cksum`, and takes 16 characters. `resolve_pool_dir` builds `${POOL_ROOT}/${ident}/${POOL_NAME}` with three children: `locks`, `meta` and `worktrees`. Using the common dir means a call made from inside an existing worktree resolves to the same pool as the main checkout.

Allocation is a non blocking flock scan. `cmd_run` loops slots 0 to `SLOT_COUNT` (default 8), opens file descriptor 9 on `slot-NN.lock` and tries `flock -n 9`. First one that takes wins, and the descriptor stays open for the whole lifetime of the process. That is the entire correctness argument: the kernel releases the lock when the last descriptor closes, so a `kill -9`, an OOM kill or a power loss all release it with no heartbeat thread and no stale lock sweeper. Same pattern systemd and database connection pools use. If nothing is free, `--timeout 0` fails fast, `inf` polls at 0.5 second intervals, and an integer sets a deadline computed before the loop starts.

`write_meta` then records pid, branch, base, label, `started_at`, `started_epoch` and host as flat `key=value` lines, read back by an awk one liner in `meta_get`. Flat files on purpose: nothing here needs `jq` to stay correct. The branch is `agent/${POOL_NAME}/${label}/slot${N}-<UTC timestamp>`, with the label pushed through `sanitize_label`. The worktree is created with `git worktree add -B`, or reused via `checkout -B` plus `reset --hard` plus `clean -fdx` if the directory already exists. The add path self heals: on an "already registered worktree" error it runs `git worktree prune` once and retries, which stops a manually deleted directory from wedging the slot forever. The wrapped command runs in a subshell that `cd`s into the worktree, not `exec`, so the `EXIT` trap still fires. `cleanup_slot` optionally resets and cleans, deletes the metadata file, closes fd 9 and returns the child's exit code. `RUN_SLOT`, `RUN_WT` and friends are globals rather than `local`, because bash drops a function's locals before an errexit driven EXIT trap runs and the trap would otherwise die on unbound variables under `set -u`.

Introspection and reclaim both go through `collect_slots`, which classifies each slot by probing the lock non destructively: `slot_is_free` opens it in a subshell and tries `flock -n`, never disturbing a real holder. Lock held plus live pid is **busy**. Lock held plus dead pid is **held**, meaning a child inherited the descriptor and outlived the wrapper. Lock free plus leftover metadata is **stale**, meaning the EXIT trap never ran. Nothing at all is **free**. A leftover worktree directory alone is not stale, since `run` deliberately reuses it. `cmd_gc` reclaims stale slots unconditionally, because nobody can be using files nobody has locked, and refuses to touch held slots unless you pass `--force` and the slot has looked that way longer than `--stale-seconds` (default 21600). `reclaim_slot` deletes the meta file, runs `git worktree remove --force` with an `rm -rf` fallback, prunes, then `branch -D`. `cmd_status` emits JSON built by hand with `json_escape`, and `cmd_prune` tears the pool down after a prompt `-y` skips.

## Usage

```bash
# Check the environment first: git, flock, repo and pool root.
./AgentWorktreePoolManager.sh doctor

# Run four review agents in parallel, each in its own clean worktree.
for i in 1 2 3 4; do
  ./AgentWorktreePoolManager.sh run \
    --pool review \
    --label "reviewer-$i" \
    --slots 8 \
    --timeout inf \
    --base origin/main \
    --clean-on-release \
    -- ./run_review_agent.sh &
done
wait

# The wrapped command runs with cwd = worktree and sees:
#   AWPM_WORKTREE  AWPM_BRANCH  AWPM_SLOT  AWPM_BASE_REF

# What is checked out and by whom.
./AgentWorktreePoolManager.sh list --pool review
./AgentWorktreePoolManager.sh status --pool review --json

# Sweep up after crashed runs. Add --force --stale-seconds 3600 only
# when you are certain nothing is still running.
./AgentWorktreePoolManager.sh gc --pool review

# Tear the whole pool down.
./AgentWorktreePoolManager.sh prune --pool review -y

# Env equivalents: AWPM_POOL AWPM_ROOT AWPM_REPO AWPM_SLOTS
#                  AWPM_BASE AWPM_TIMEOUT AWPM_STALE_SECONDS
```

## Notes

- Requires `git` and `flock`. `flock` ships with util-linux, so this runs unmodified on a Linux box or CI runner but needs `brew install flock` on macOS. `doctor` checks both. No `jq`, no python, nothing else.
- `run` exits with the wrapped command's exit status. Every internal failure goes through `die` and exits 1. An `ERR` trap logs the failing line number first.
- flock is a single machine primitive. Two hosts sharing a pool directory over NFS are not coordinated by this and will collide.
- `--slots` is not stored with the pool. Running `gc` or `list` with a smaller `--slots` than the pool was created with silently ignores the higher slots and their debris.
- `--keep` is parsed and suppresses the `--clean-on-release` cleanup, but it is not listed in `--help`. Treat it as undocumented.
- The reuse path runs `git fetch --no-tags` on the first path segment of `--base`, which assumes a remote qualified ref like `origin/main`. With a plain local branch name the fetch is a swallowed no-op and the worktree resets to whatever that local ref points at.
- `gc` never deletes commits, but for a reclaimed slot `branch -D` discards unmerged work by design. Anything an agent committed and did not push is gone.
- Nothing here sandboxes the agent. A run gets its own checkout, not its own filesystem, network or process namespace.
