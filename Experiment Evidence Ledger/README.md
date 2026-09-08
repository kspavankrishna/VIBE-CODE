# Experiment Evidence Ledger

You reported a benchmark number out of a results directory. Three weeks later a reviewer asks whether those are still the same files. Nothing in the repo can answer that question. This hashes the whole tree into a self verifying manifest and gives you one SHA-256 evidence root you can pin in CI, a release or a paper.

**Language:** Julia | **Lines:** 674 | **Added:** 2026-05-25

## What this solves

This solves the boring but serious problem of proving exactly which model weights, prompt fixtures, evaluation sets, retrieval indexes, generated reports and research inputs belonged to a run. A directory is not a fact. It is a mutable pile of bytes that anybody with write access, any rerun script and any cloud sync client can quietly change between the run that produced the number and the review that questions it.

The failure mode looks like this. An eval run finishes on Tuesday and prints 0.874. Someone regenerates a fixture on Wednesday to debug an unrelated crash. On Thursday the run is repeated for the writeup and prints 0.861. Two people then spend a day arguing about whether that is variance, a regression or a data change, and neither can prove which files moved. The real cost is not the day. It is that every earlier number in the repo becomes suspect, because none were pinned to anything.

The quieter version is worse. Permission bits change and a script that used to execute stops executing. A symbolic link into a shared scratch volume gets repointed at a different checkpoint. Both are invisible to a size check and to a casual `ls`, and both silently change what your pipeline reads. This ledger records the permission mode of every regular file and hashes the literal text of every symbolic link target without following it, so both surface as errors. For publication the same argument holds: without a pinned root, an appendix claiming a set of artifacts is only a promise.

## Why I built it

I kept seeing experiments reported from a directory that changed quietly between the successful run and the review, leaving nobody able to reproduce the result or explain a regression. The existing options miss in the same direction. Git tracks source and refuses to host multi gigabyte checkpoints. `sha256sum -c` covers file content and knows nothing about permission bits, symbolic links, missing files or files that appeared after the fact. DVC and LFS solve storage, a different problem, and arrive with a cache and a remote to configure. None of them lets you print one short string into a paper and later prove a whole tree against it. I wanted a single file with no package installation surprises, a manifest a reviewer can read inside a pull request diff, and strictness about what actually breaks reproducibility rather than only what is easy to hash.

## When to use it

- A GitHub Actions job builds evaluation artifacts and you want the workflow to fail, with inline annotations, if anything under the output directory drifted from the approved run.
- You are publishing a paper and want a SHA-256 string in the appendix a reader can verify against the released bundle.
- A training pipeline writes checkpoints, tokenizer files and config into a run directory, and you need proof of which combination produced the reported metric.
- Two benchmark runs disagree and you want a file level answer to what changed instead of rerunning both.
- A shared scratch volume holds datasets several people can write to, and you want an alarm when someone edits one out from under an in flight experiment.
- You are promoting artifacts from staging to production and want an integrity gate before the copy.

## How it works

The scan is a sorted, deterministic walk. `scan_entries` recurses through a local `visit` closure, calls `sort!(readdir(...))` at every level so ordering never depends on the filesystem, and skips anything `is_omitted` matches by exact path or by prefix against the normalized exclusions. Directories are traversed but never recorded. A symbolic link becomes an `Entry` with kind `'L'` whose digest is the SHA-256 of the target string itself, so the link is described rather than followed. A regular file becomes kind `'F'`, carrying its size and `mode & 0o777`. Anything else, a socket or a FIFO, is a hard failure rather than a silent skip.

File hashing goes through `hash_file_stably`, the part worth trusting. It rejects a path that turned into a symlink between the walk and the read, takes `stat` before, streams the file through a `SHA.SHA2_256_CTX` with a reused 1 MiB buffer so memory stays flat on large checkpoints, then takes `stat` again and fails if size or mtime moved or if the byte count read does not equal the final size. That is a torn read detector. It will not catch a perfectly timed adversarial edit, but it catches the realistic case of a training job still flushing a checkpoint while the snapshot runs.

The evidence root comes from `calculate_evidence_root` with unambiguous framing: `absorb!` feeds each field into the hash followed by a NUL byte, so no two field layouts collide into the same digest. The root covers the format magic `EXPERIMENT_EVIDENCE_LEDGER`, the version, the algorithm name, the root label, the creation timestamp, the self exclusion, every exclusion prefix and then every entry field including kind, octal mode, size, digest, path and link target. Because the exclusion policy is inside the hash you cannot widen the ignore list and keep the same root. A policy change is a change.

The manifest is a tab separated text file. Every path, label and link target passes through `encode_field`, which percent escapes control bytes, `%` and `0x7f`, so a filename containing a newline or tab cannot forge a manifest line. `write_manifest_atomically` refuses to clobber unless `--force`, writes to `.<name>.tmp.<12 random chars>` in the same directory, flushes, then `mv`s into place with the temp file removed in a `finally`. When the manifest sits inside the tree it describes, `relative_if_inside` records its relative path as `self_exclude` so the ledger never hashes itself. `read_manifest` is paranoid on the way back in, rejecting unknown or repeated metadata keys, non canonical or absolute paths, `..` segments, bad digests, negative sizes, modes above `0o777`, a file entry carrying a link target, a link entry carrying permissions, unsorted or duplicated entries and a timestamp without a `Z` suffix. Then it recomputes the root and fails if it disagrees. A hand edited manifest does not load.

Verification optionally compares a pinned root with `constant_time_equal`, rescans the live tree under the manifest's own exclusion policy and runs `compare_entries` for the missing, unexpected and changed sets. `entry_change_message` names the specific difference, type, digest, mode, size or link target. With `--github-actions` each problem becomes a `::error file=...` workflow annotation with the path and message escaped. `diff_snapshots` runs the same comparison between two manifests, prints `ADDED`, `REMOVED` and `CHANGED` lines, and flags `POLICY CHANGED` when the exclusion set or the embedded manifest handling differs.

## Usage

```bash
# Snapshot an approved run. The manifest can live inside the tree it describes.
julia ExperimentEvidenceLedger.jl snapshot ./runs/2026-05-25 ./runs/2026-05-25/EVIDENCE.ledger \
    --label "eval-run-411" \
    --exclude "logs/tmp" \
    --force
# Recorded 3184 entries; evidence root 9f2c...e10b

# Verify in CI against the root you pinned in the release notes or the paper.
julia ExperimentEvidenceLedger.jl verify ./runs/2026-05-25/EVIDENCE.ledger ./runs/2026-05-25 \
    --expect-root 9f2c...e10b \
    --github-actions
# exit 0 clean, exit 1 if anything is missing, changed or unrecorded

# Allow new files but still fail on modified or deleted ones.
julia ExperimentEvidenceLedger.jl verify ./EVIDENCE.ledger ./runs/2026-05-25 --allow-extra --quiet

# Explain what moved between two runs.
julia ExperimentEvidenceLedger.jl diff ./before.ledger ./after.ledger

# Include .git, node_modules and friends.
julia ExperimentEvidenceLedger.jl snapshot ./bundle ./bundle.ledger --no-default-exclusions

julia ExperimentEvidenceLedger.jl --help
```

Called as a library:

```julia
include("ExperimentEvidenceLedger.jl")
using .ExperimentEvidenceLedger

manifest = create_snapshot("runs/411", "runs/411/EVIDENCE.ledger";
                           label = "eval-run-411",
                           exclusions = ["logs", "scratch"],
                           force = true)
println(manifest.evidence_root)

ok = verify_snapshot("runs/411/EVIDENCE.ledger", "runs/411";
                     expect_root = manifest.evidence_root,
                     allow_extra = false,
                     github_actions = false,
                     quiet = true)

identical = diff_snapshots("before.ledger", "after.ledger"; quiet = true)
```

## Notes

- Stdlib only. `Dates`, `Printf`, `Random` and `SHA` ship with Julia, so there is no `Project.toml`, no install step and nothing to break in an offline CI runner.
- Exit codes: `0` success, `1` verification failed or the manifests differ, `2` usage error, unknown flag or any `LedgerError`. Empty arguments print usage to stderr and return `2`.
- The evidence root includes `created_utc`, so two snapshots of byte identical trees taken at different times produce different roots. The root pins a snapshot event, not content alone. If you need pure content addressing this is the wrong shape.
- Directories are never recorded, so an empty directory is invisible and its removal is not flagged. Modification times, ownership, extended attributes, ACLs and setuid or sticky bits are outside the digest. Hard links look like separate files and both copies are hashed in full.
- It records evidence, it does not store content. No restore, no deduplication, no remote. If a file is deleted the ledger proves it is gone and cannot bring it back.
- `hash_file_stably` detects concurrent writes with a stat and byte count check, not a lock. Snapshot after the job finishes, not during. Large trees pay full read cost every time, since there is no mtime based fast path by design. Manifest paths must be canonical relative POSIX paths, so a filename genuinely containing a backslash is rejected.
