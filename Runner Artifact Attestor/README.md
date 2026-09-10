# Runner Artifact Attestor

A release job builds a zip, a wheel, a jar or a wasm bundle on an ephemeral runner, and nobody can prove the bytes about to be uploaded are the bytes that were built and tied to the expected commit. This is a single Bash script that turns the artifact directory into evidence before the upload step runs.

**Language:** Bash | **Lines:** 357 | **Added:** 2026-06-04

## What this solves

The gap sits between the build step and the publish step. Something writes files into `dist/`, then a later step ships whatever is on disk. Nothing in between asserts the file set is the set the build produced. On a self hosted runner with a dirty workspace, a leftover artifact from a previous job gets published alongside the new one. An AI coding agent or a helper script drops a debug bundle or a stray config into the artifact root and `upload-artifact` takes it. The failure is silent. You find out when a customer downloads it.

The second failure mode is a credential inside a build output. Secret scanners run against source in the repo, not against the packed wheel, the compiled bundle or the generated config that ended up in `dist/`. An AWS key id baked into a webpack bundle sails past a repo level scan because the file never existed in git. Once that bundle is on a registry or a CDN it is public and the rotation clock has started.

The third is provenance theatre. A pipeline produces a SLSA attestation, an in-toto file or an SBOM, and everyone downstream treats its presence as proof. Nobody checks that the commit SHA in it is the commit this job is running on, or that its digests match the files being uploaded. A provenance file describing a different build is worse than none, because it buys trust it has not earned. The cost lands on whoever runs the incident: republishing a version, rotating a leaked key, explaining why a release contained a file nobody can account for.

## Why I built it

The tooling is either too heavy or too narrow. Full supply chain stacks like sigstore or signed in-toto chains need keys, network calls, an OIDC identity and a policy engine. That is the right answer eventually, not the one you drop into a workflow on a Tuesday to stop a specific class of mistake. On the other end, `sha256sum -c` verifies digests and nothing else: it will not tell you a file appeared that should not exist, it will not reject a symlink pointing outside the tree and it knows nothing about your commit.

So this is the middle. One file, no dependencies beyond coreutils and `sha256sum` or `shasum`, no network, no stored secret. It runs anywhere Bash runs: GitHub Actions, a self hosted runner, a Jenkins agent, a container build step or your laptop before you publish by hand.

## When to use it

- A release workflow running `upload-artifact`, `npm publish` or a container push, where you want a hard stop if the artifact set changed after the build
- A self hosted runner whose workspace is not guaranteed clean between jobs
- A pipeline where an AI coding agent or a generated script writes into the build directory and you want to know exactly what it produced
- A deploy gate that must confirm the SLSA or in-toto file actually names this commit and this repository before promotion
- A packaging job for model weights, mobile builds or edge bundles where a stray file or leaked token is expensive
- A local pre publish check when you are cutting a release by hand

## How it works

Three modes are selected by the first positional argument: `create`, `verify` and `audit`. `resolve_inputs` resolves `ROOT` to an absolute physical path with `cd && pwd -P`, defaults `MANIFEST` to `.runner-artifact-manifest.tsv` inside the root and creates a scratch directory with `mktemp -d` that an EXIT trap removes.

File selection runs through two glob lists. `included_path` applies `DENY_GLOBS` first and returns on a match, then `ALLOW_GLOBS`, falling back to `DEFAULT_ALLOW_GLOBS` when you passed no `--allow`. Deny always wins. The defaults allow the usual output directories and package extensions plus `*.sbom.json`, `*.intoto.jsonl` and `*.provenance.json`, and deny `.git/*`, `.github/*`, `node_modules/*`, `vendor/*`, `.terraform/*` and credential shapes such as `*.pem`, `*.key` and `.env.*`. Matching is Bash pattern matching inside `[[ ]]`, so `*` crosses directory separators and `dist/*` covers the whole subtree.

Before anything is hashed, `safe_path` rejects paths that are absolute, start with a dot, contain a tab, carriage return or newline, contain a double slash or contain any `..` component. That kills manifest injection through a filename with an embedded tab, and it kills traversal out of the artifact root. `check_symlinks` runs a separate `find . -type l` pass and fails on any symlink that would have been included, because a symlink digest describes the target rather than the artifact.

`write_manifest` streams the sorted list from `list_artifacts` and records `sha256_file`, `file_size` and `file_mtime` per file as a tab separated line: digest, size, mtime epoch, relative path. `sha256_file` prefers `sha256sum` and falls back to `shasum -a 256`, and the `stat` helpers do the same GNU then BSD fallback, which is what makes it work unchanged on macOS runners. Output goes to a temp file and is only moved into place if `FAILURES` is still zero, so a failed create never leaves a half written manifest.

`verify_manifest` reads the manifest line by line, skips comments, splits on tab into four fields plus an `extra` field that must be empty, and validates each entry with `is_hex_sha256`, `is_integer` and `safe_path` before touching the filesystem. It compares size and digest, compares mtime only under `--strict-mtime`, and unless `--allow-extra` is set it walks the live directory again and fails on any included file missing from the manifest. That reverse check is what catches the stray artifact.

`scan_secrets` runs a fixed list of high confidence regexes with `grep -aEInm 1` against every file under `MAX_SCAN_BYTES`: AWS key ids, GitHub tokens in both `gh*_` and `github_pat_` shapes, `sk-` keys, Google `AIza` keys, Slack `xox` tokens and PEM private key headers. Narrow on purpose, since these shapes rarely false positive. `check_provenance` greps the provenance JSON case insensitively for the expected commit and repository, which default to `GITHUB_SHA` and `GITHUB_REPOSITORY`, and under `--require-provenance-digests` for every digest in the manifest. `check_runner_context` validates `GITHUB_SHA` as 40 hex characters, checks the owner slash name shape of `GITHUB_REPOSITORY` and optionally enforces `GITHUB_REF_PROTECTED`. `summary` exits 0 with a count line when `FAILURES` is zero and 1 otherwise.

## Usage

```bash
# In the build job, after the build step, before upload
./RunnerArtifactAttestor.sh create \
  --root dist \
  --manifest .attest/manifest.tsv \
  --max-file-bytes 268435456

# In the publish job, before npm publish or upload-artifact
./RunnerArtifactAttestor.sh verify \
  --root dist \
  --manifest .attest/manifest.tsv \
  --provenance dist/build.intoto.jsonl \
  --expected-commit "$GITHUB_SHA" \
  --expected-repo "$GITHUB_REPOSITORY" \
  --require-provenance-digests \
  --require-protected-ref

# Custom file selection, repeat --allow and --deny as needed
./RunnerArtifactAttestor.sh create \
  --root . \
  --allow 'packages/*/dist/*' \
  --allow '*.whl' \
  --deny '*.map' \
  --strict-mtime

# Scan and count only, no manifest read or written
./RunnerArtifactAttestor.sh audit --root build

./RunnerArtifactAttestor.sh --help
```

## Notes

- Exit codes: 0 when nothing failed, 1 when any check failed, 2 for a usage or setup error raised by `die` such as a missing root or manifest. Warnings never change the exit code.
- Provenance checking is a case insensitive substring grep, not JSON parsing and not signature verification. It confirms the commit, repo and digests appear somewhere in the file. It does not prove the file is authentic.
- `--strict-mtime` is off by default because most build systems do not produce reproducible mtimes. Turn it on only if yours does.
- Mode defaults to `verify`, so running with only flags verifies against `.runner-artifact-manifest.tsv` in the root.
- `create` fails when zero files matched the allow globs, usually a sign the root or the globs are wrong rather than a real empty build.
- The secret scan skips any file larger than `--max-scan-bytes`, default 1048576, so a large bundle goes unscanned unless you raise the limit.
- Symlinks inside the selection are always a failure, never a warning. There is no flag to allow them.
- No network calls, no keys, no config file. It reads `GITHUB_ACTIONS`, `GITHUB_SHA`, `GITHUB_REPOSITORY` and `GITHUB_REF_PROTECTED` when present and warns when they are absent.
