# Repo Auto Shell

One repo, five runtimes, and every laptop set up slightly differently. This is a single Nix file that reads your project layout, installs only the toolchains that repo actually needs and pins every package cache inside the working tree.

**Language:** Nix | **Lines:** 548 | **Added:** 2026-04-17

## What this solves

This solves the annoying April 2026 problem where one repo mixes Python for model work, Node for product code, Rust or Go for hot paths, Playwright for browser checks, and Terraform or Kubernetes for deployment, but every laptop and CI image drifts in a slightly different way. One missing native library, one global cache collision, or one silently different tool version is enough to waste half a day. The failure is never loud. It arrives three commands later, in a stack trace pointing at the wrong thing.

The concrete shapes it takes: `uv sync` writes into a global cache and picks up a wheel resolved for a different Python patch version. `pnpm` reads a stale home directory and installs a lockfile that does not match. `cargo` links against whatever `libclang` the host happens to have, so a crate with bindgen builds on one machine and fails on another. Playwright downloads browsers into a random path, so CI redownloads them every run. A local model cache quietly fills the wrong disk until something unrelated fails with ENOSPC.

Who notices is the part that costs money. A new engineer notices on day one and burns their first afternoon on setup instead of code. CI notices as a red build that passes on rerun, which is worse, because now nobody trusts the signal. And with state scattered across `~/.cargo`, `~/.gradle`, `~/.m2` and `~/.npm`, you cannot separate a code problem from a machine problem without bisecting your own home directory.

## Why I built it

A lot of teams still solve this with a README full of manual setup steps. That is fragile, slow and hard to reproduce when a new engineer or CI runner shows up. The Nix answers go the other way: a full flake with an overlay per language, a devenv config, or a hand written `shell.nix` that grows a branch every time someone adds a language. Both ends are wrong for a repo still changing shape.

I wanted one file you can drop in, read top to bottom in a few minutes, and fork without learning a framework. No flake lock, no module system, no plugin API. Just detection from filesystem markers, a package list per stack and a shell hook that redirects the caches. Everything it decides sits in plain `let` bindings, and `repo-auto-doctor` prints those decisions back at you.

## When to use it

- A polyglot repo where the Python service, the TypeScript frontend and the Rust worker live together and setup instructions have drifted.
- CI images and laptops disagree about a build and you need the same tool set in both places from one definition.
- Onboarding an engineer who should be running tests in ten minutes, not installing four version managers.
- An AI or agent repo where model caches, Playwright browsers and Hugging Face downloads land in unpredictable places on disk.
- An infra repo where Terraform, Helm and kubectl versions matter and nobody trusts whatever is on `PATH`.
- You want a throwaway environment: work, then delete `.cache/repo-auto-shell` and leave the host untouched.

## How it works

The file is a plain Nix function taking `pkgs`, `projectRoot`, boolean overrides (`enableCuda`, `enableRocm`, `enablePlaywright`, `enableKubernetes`, `enableTerraform`, `enableOllama`) and `pythonAttr`, `nodeAttr`, `jdkAttr` for version pinning. It evaluates to a `pkgs.mkShell`. No flake, no overlay, no import beyond nixpkgs.

Detection runs at evaluation time through two helpers. `repoHas` is `builtins.pathExists (root + "/${relative}")`, and `repoHasAny` folds it over a list with `lib.any`. Each stack is one boolean: a marker list ORed with an environment escape hatch. Python keys off `pyproject.toml`, `uv.lock`, `requirements.txt` or `.python-version`. Node off `package.json`, any of four lockfiles, or `tsconfig.json`. Rust off `Cargo.toml`, Go off `go.mod`, JVM off Maven and Gradle files, proto off a `proto` directory or `buf.yaml`, SQL off `prisma/schema.prisma`, `drizzle.config.ts`, `supabase/config.toml` or a `migrations` directory. The override path is `envFlag`, which reads `builtins.getEnv` and accepts `1`, `true`, `yes` or `on` case insensitively. CUDA and ROCm are additionally gated on `pkgs.stdenv.isLinux`, so a Mac cannot accidentally ask for them.

Package selection is defensive on purpose. `collectAttrs` filters a name list down to attributes that actually exist in the set before mapping over them, so a channel missing `delta` or `sccache` degrades to a shell without those tools instead of an eval error. `firstAttr` is the strict variant and throws a named error listing everything it tried. `chooseAttr` layers a caller preference over a fallback chain, which is how `pythonAttr` beats the `python312, python311, python3` ladder.

The cache localization is the part that pays off daily. The `shellHook` sets `REPO_AUTO_CACHE_ROOT` to `$PWD/.cache/repo-auto-shell` and points roughly twenty five variables into it: the three XDG base directories, `UV_CACHE_DIR`, `PIP_CACHE_DIR`, `CARGO_HOME`, `RUSTUP_HOME`, `SCCACHE_DIR`, `GOCACHE`, `GOMODCACHE`, `NPM_CONFIG_CACHE`, `PNPM_HOME`, `PLAYWRIGHT_BROWSERS_PATH`, `HF_HOME`, `OLLAMA_MODELS`, `GRADLE_USER_HOME`, `DOCKER_CONFIG` and a `MAVEN_OPTS` that sets `maven.repo.local`. All are created up front with one `mkdir -p`. `SSL_CERT_FILE` is pinned to the `cacert` bundle, and `UV_LINK_MODE` is set to `copy` because hardlinking across the store boundary is unreliable.

Native linking gets the same treatment, all of it behind `lib.optionalString` guards. `lib.makeLibraryPath` builds `LD_LIBRARY_PATH` from `stdenv.cc.cc` plus openssl, zlib, libffi and sqlite on Linux. `LIBCLANG_PATH` comes from `pkgs.llvmPackages.libclang`, checking for a `lib` output first, which is what fixes bindgen and PyO3 style builds. With CUDA or ROCm on, the first collected package becomes `CUDA_HOME` and `CUDA_PATH`, or `ROCM_PATH`. On Darwin the Security, CoreFoundation and SystemConfiguration frameworks go into `buildInputs`, but only after checking the full `pkgs.darwin.apple_sdk.frameworks` attribute path exists, so newer nixpkgs revisions that dropped it still evaluate.

Two helpers ship inside the shell, both built with `pkgs.writeShellApplication` so they get `set -euo pipefail` and pinned runtime inputs. `repo-auto-doctor` prints the project root, the comma joined `detectedStacks` string and a yes or no line per stack, then probes ten binaries with `command -v` and reports each as on with its resolved path or off. That second half matters: it separates what the shell decided from what is actually reachable on `PATH`. `repo-cache-reset` deletes the cache root.

## Usage

```bash
# drop the file at the repo root, then enter the shell
nix develop -f RepoAutoShell.nix

# or the older CLI
nix-shell RepoAutoShell.nix

# force optional stacks without editing the file
REPO_AUTO_ENABLE_CUDA=1 nix develop -f RepoAutoShell.nix
REPO_AUTO_ENABLE_K8S=1 REPO_AUTO_ENABLE_TERRAFORM=1 nix develop -f RepoAutoShell.nix

# flags: REPO_AUTO_ENABLE_{PYTHON,NODE,RUST,GO,JVM,PROTO,SQL,DOCKER,
# PLAYWRIGHT,K8S,TERRAFORM,OLLAMA,CUDA,ROCM}, values 1 true yes on

# pin language versions from a caller
nix-shell -E 'import ./RepoAutoShell.nix {
  pythonAttr = "python311";
  nodeAttr   = "nodejs_20";
  jdkAttr    = "jdk17_headless";
  enablePlaywright = true;
}'

# point it at a subdirectory
nix-shell -E 'import ./RepoAutoShell.nix { projectRoot = ./services/api; }'

# inside the shell
repo-auto-doctor      # what got enabled, and what is actually on PATH
repo-cache-reset      # delete .cache/repo-auto-shell and start clean
echo "$REPO_AUTO_STACKS"      # e.g. python,node,rust,docker
```

## Notes

- Detection reads the filesystem at evaluation time. Add a `Cargo.toml` while inside the shell and nothing happens until you exit and re enter.
- `envFlag` uses `builtins.getEnv`, which returns empty under pure evaluation. In a flake or with `--pure-eval` the environment overrides silently do nothing. Use the function arguments there.
- `REPO_AUTO_CACHE_ROOT` is `$PWD` based, not root based. Enter the shell from a subdirectory and you get a second cache tree there. Add `.cache/` to `.gitignore`.
- Docker gets `docker-client` and `docker-compose`, not a daemon. You still need Docker Desktop, colima or a socket from the host.
- The SQL stack triggers on a bare `migrations` or `db` directory, a broad marker. It pulls postgresql and redis into repos that only keep SQL files around.
- CUDA and ROCm are Linux only by construction, and `cudaPackages` is unfree, so those need `NIXPKGS_ALLOW_UNFREE=1`. It does not pin nixpkgs either: the shell is only as reproducible as the channel or flake input you hand it as `pkgs`.
