{ pkgs ? import <nixpkgs> { }
, projectRoot ? ./. 
, enableCuda ? false
, enableRocm ? false
, enablePlaywright ? false
, enableKubernetes ? false
, enableTerraform ? false
, enableOllama ? false
, pythonAttr ? null
, nodeAttr ? null
, jdkAttr ? null
}:

let
  lib = pkgs.lib;
  root = projectRoot;
  rootText = toString root;

  lower = value:
    lib.strings.toLower value;

  truthy = value:
    builtins.elem (lower value) [ "1" "true" "yes" "on" ];

  envFlag = name: fallback:
    let
      raw = builtins.getEnv name;
    in
      if raw == "" then fallback else truthy raw;

  repoHas = relative:
    builtins.pathExists (root + "/${relative}");

  repoHasAny = relatives:
    lib.any repoHas relatives;

  collectAttrs = set: names:
    builtins.map
      (name: builtins.getAttr name set)
      (builtins.filter (name: builtins.hasAttr name set) names);

  firstAttr = set: names:
    let
      present = builtins.filter (name: builtins.hasAttr name set) names;
    in
      if present == [] then
        throw "RepoAutoShell.nix could not find any of: ${builtins.concatStringsSep ", " names}"
      else
        builtins.getAttr (builtins.head present) set;

  chooseAttr = set: preferred: fallbacks:
    if preferred != null && builtins.hasAttr preferred set then
      builtins.getAttr preferred set
    else
      firstAttr set fallbacks;

  pythonPkg = chooseAttr pkgs pythonAttr [ "python312" "python311" "python3" ];
  nodePkg = chooseAttr pkgs nodeAttr [ "nodejs_22" "nodejs_20" "nodejs" ];
  jdkPkg = chooseAttr pkgs jdkAttr [ "jdk21_headless" "jdk21" "jdk17_headless" "jdk17" "jdk" ];
  goPkg = firstAttr pkgs [ "go_1_24" "go_1_23" "go" ];

  pythonEnabled =
    repoHasAny [
      "pyproject.toml"
      "uv.lock"
      "requirements.txt"
      "requirements-dev.txt"
      "pytest.ini"
      ".python-version"
    ]
    || envFlag "REPO_AUTO_ENABLE_PYTHON" false;

  nodeEnabled =
    repoHasAny [
      "package.json"
      "pnpm-lock.yaml"
      "package-lock.json"
      "yarn.lock"
      "bun.lock"
      "bun.lockb"
      "tsconfig.json"
    ]
    || envFlag "REPO_AUTO_ENABLE_NODE" false;

  rustEnabled =
    repoHasAny [
      "Cargo.toml"
      "rust-toolchain.toml"
      "rust-toolchain"
    ]
    || envFlag "REPO_AUTO_ENABLE_RUST" false;

  goEnabled =
    repoHasAny [
      "go.mod"
      "go.work"
    ]
    || envFlag "REPO_AUTO_ENABLE_GO" false;

  jvmEnabled =
    repoHasAny [
      "pom.xml"
      "build.gradle"
      "build.gradle.kts"
      "settings.gradle"
      "settings.gradle.kts"
    ]
    || envFlag "REPO_AUTO_ENABLE_JVM" false;

  protoEnabled =
    repoHasAny [
      "proto"
      "buf.yaml"
      "buf.gen.yaml"
    ]
    || envFlag "REPO_AUTO_ENABLE_PROTO" false;

  dockerEnabled =
    repoHasAny [
      "Dockerfile"
      "docker-compose.yml"
      "compose.yaml"
      ".devcontainer/devcontainer.json"
    ]
    || envFlag "REPO_AUTO_ENABLE_DOCKER" false;

  sqlEnabled =
    repoHasAny [
      "prisma/schema.prisma"
      "drizzle.config.ts"
      "supabase/config.toml"
      "migrations"
      "db"
    ]
    || envFlag "REPO_AUTO_ENABLE_SQL" false;

  playwrightEnabled =
    enablePlaywright
    || repoHasAny [
      "playwright.config.ts"
      "playwright.config.js"
      "playwright.config.mjs"
    ]
    || envFlag "REPO_AUTO_ENABLE_PLAYWRIGHT" false;

  kubernetesEnabled =
    enableKubernetes
    || repoHasAny [
      "charts"
      "helmfile.yaml"
      "kustomization.yaml"
      "skaffold.yaml"
    ]
    || envFlag "REPO_AUTO_ENABLE_K8S" false;

  terraformEnabled =
    enableTerraform
    || repoHasAny [
      ".terraform.lock.hcl"
      "main.tf"
      "versions.tf"
      "providers.tf"
    ]
    || envFlag "REPO_AUTO_ENABLE_TERRAFORM" false;

  ollamaEnabled =
    enableOllama
    || repoHasAny [
      "Modelfile"
      "ollama"
    ]
    || envFlag "REPO_AUTO_ENABLE_OLLAMA" false;

  cudaEnabled =
    pkgs.stdenv.isLinux
    && (enableCuda || envFlag "REPO_AUTO_ENABLE_CUDA" false);

  rocmEnabled =
    pkgs.stdenv.isLinux
    && (enableRocm || envFlag "REPO_AUTO_ENABLE_ROCM" false);

  detectedStackList = builtins.filter (name: name != null) [
    (if pythonEnabled then "python" else null)
    (if nodeEnabled then "node" else null)
    (if rustEnabled then "rust" else null)
    (if goEnabled then "go" else null)
    (if jvmEnabled then "jvm" else null)
    (if protoEnabled then "proto" else null)
    (if sqlEnabled then "sql" else null)
    (if dockerEnabled then "docker" else null)
    (if playwrightEnabled then "playwright" else null)
    (if kubernetesEnabled then "k8s" else null)
    (if terraformEnabled then "terraform" else null)
    (if ollamaEnabled then "ollama" else null)
    (if cudaEnabled then "cuda" else null)
    (if rocmEnabled then "rocm" else null)
  ];

  detectedStacks =
    if detectedStackList == [] then
      "base"
    else
      builtins.concatStringsSep "," detectedStackList;

  llvmPackages =
    if builtins.hasAttr "llvmPackages" pkgs then
      collectAttrs pkgs.llvmPackages [ "libclang" ]
    else
      [];

  libclangPackage =
    if llvmPackages == [] then null else builtins.head llvmPackages;

  libclangPath =
    if libclangPackage == null then
      ""
    else if libclangPackage ? lib then
      "${libclangPackage.lib}/lib"
    else
      "${libclangPackage}/lib";

  cudaPackages =
    if cudaEnabled && builtins.hasAttr "cudaPackages" pkgs then
      collectAttrs pkgs.cudaPackages [ "cuda_nvcc" "cudatoolkit" "cuda_cudart" "cuda_cccl" ]
    else
      [];

  rocmPackages =
    if rocmEnabled && builtins.hasAttr "rocmPackages" pkgs then
      collectAttrs pkgs.rocmPackages [ "clr" "hipblas" "rocminfo" ]
    else
      [];

  cudaRoot =
    if cudaPackages == [] then null else builtins.head cudaPackages;

  rocmRoot =
    if rocmPackages == [] then null else builtins.head rocmPackages;

  sharedLibPackages =
    collectAttrs pkgs [ "openssl" "zlib" "libffi" "sqlite" ];

  basePackages =
    [
      pkgs.bashInteractive
      pkgs.cacert
      pkgs.coreutils
      pkgs.curl
      pkgs.findutils
      pkgs.fzf
      pkgs.gawk
      pkgs.git
      pkgs.gnugrep
      pkgs.gnused
      pkgs.jq
      pkgs.just
      pkgs.ripgrep
      pkgs.which
      pkgs.xz
      pkgs.zstd
    ]
    ++ collectAttrs pkgs [
      "fd"
      "wget"
      "zip"
      "unzip"
      "tree"
      "gh"
      "git-lfs"
      "delta"
      "direnv"
      "hyperfine"
      "watchexec"
      "shellcheck"
      "shfmt"
      "actionlint"
      "nil"
      "nixfmt-rfc-style"
      "sccache"
    ];

  nativeBuildPackages =
    collectAttrs pkgs [ "pkg-config" "cmake" ]
    ++ llvmPackages;

  pythonPackages =
    lib.optionals pythonEnabled (
      [ pythonPkg ]
      ++ collectAttrs pkgs [ "uv" "ruff" "pyright" "poetry" ]
    );

  nodePackages =
    lib.optionals nodeEnabled (
      [ nodePkg ]
      ++ collectAttrs pkgs [
        "corepack"
        "pnpm"
        "yarn"
        "bun"
        "typescript-language-server"
      ]
    );

  rustPackages =
    lib.optionals rustEnabled (
      collectAttrs pkgs [
        "rustup"
        "cargo"
        "cargo-nextest"
        "cargo-watch"
        "rust-analyzer"
      ]
    );

  goPackages =
    lib.optionals goEnabled (
      [ goPkg ]
      ++ collectAttrs pkgs [ "gopls" "delve" "gotools" ]
    );

  jvmPackages =
    lib.optionals jvmEnabled (
      [ jdkPkg ]
      ++ collectAttrs pkgs [ "gradle" "maven" "kotlin" ]
    );

  protoPackages =
    lib.optionals protoEnabled (collectAttrs pkgs [ "protobuf" "buf" "grpcurl" ]);

  dockerPackages =
    lib.optionals dockerEnabled (collectAttrs pkgs [ "docker-client" "docker-compose" ]);

  sqlPackages =
    lib.optionals sqlEnabled (collectAttrs pkgs [ "postgresql" "redis" ]);

  browserPackages =
    lib.optionals playwrightEnabled (collectAttrs pkgs [ "playwright-driver" "chromium" "firefox" ]);

  kubernetesPackages =
    lib.optionals kubernetesEnabled (collectAttrs pkgs [ "kubectl" "helm" "kustomize" "stern" ]);

  terraformPackages =
    lib.optionals terraformEnabled (collectAttrs pkgs [ "terraform" "terraform-docs" "tflint" ]);

  aiPackages =
    lib.optionals ollamaEnabled (collectAttrs pkgs [ "ollama" ]);

  linuxRuntimeLibs =
    lib.optionals pkgs.stdenv.isLinux ([ pkgs.stdenv.cc.cc ] ++ collectAttrs pkgs [ "zlib" "openssl" "libffi" "sqlite" ]);

  darwinFrameworks =
    if pkgs.stdenv.isDarwin && pkgs ? darwin && pkgs.darwin ? apple_sdk && pkgs.darwin.apple_sdk ? frameworks then
      with pkgs.darwin.apple_sdk.frameworks;
      [ Security CoreFoundation SystemConfiguration ]
    else
      [];

  libraryPath = lib.makeLibraryPath (linuxRuntimeLibs ++ sharedLibPackages);

  repoAutoDoctor = pkgs.writeShellApplication {
    name = "repo-auto-doctor";
    runtimeInputs = [ pkgs.coreutils pkgs.gnugrep pkgs.gnused ];
    text = ''
      set -euo pipefail

      print_tool() {
        local label="$1"
        local bin="$2"

        if command -v "$bin" >/dev/null 2>&1; then
          echo "$label: on ($(command -v "$bin"))"
        else
          echo "$label: off"
        fi
      }

      cat <<EOF
      RepoAutoShell
      project root: ${rootText}
      detected stacks: ${detectedStacks}
      python enabled: ${if pythonEnabled then "yes" else "no"}
      node enabled: ${if nodeEnabled then "yes" else "no"}
      rust enabled: ${if rustEnabled then "yes" else "no"}
      go enabled: ${if goEnabled then "yes" else "no"}
      jvm enabled: ${if jvmEnabled then "yes" else "no"}
      protobuf enabled: ${if protoEnabled then "yes" else "no"}
      sql tooling enabled: ${if sqlEnabled then "yes" else "no"}
      docker enabled: ${if dockerEnabled then "yes" else "no"}
      playwright enabled: ${if playwrightEnabled then "yes" else "no"}
      kubernetes enabled: ${if kubernetesEnabled then "yes" else "no"}
      terraform enabled: ${if terraformEnabled then "yes" else "no"}
      ollama enabled: ${if ollamaEnabled then "yes" else "no"}
      cuda enabled: ${if cudaEnabled then "yes" else "no"}
      rocm enabled: ${if rocmEnabled then "yes" else "no"}
      cache root: .cache/repo-auto-shell
      EOF

      echo
      print_tool "python" "python"
      print_tool "uv" "uv"
      print_tool "node" "node"
      print_tool "pnpm" "pnpm"
      print_tool "cargo" "cargo"
      print_tool "go" "go"
      print_tool "java" "java"
      print_tool "docker" "docker"
      print_tool "kubectl" "kubectl"
      print_tool "terraform" "terraform"
    '';
  };

  repoCacheReset = pkgs.writeShellApplication {
    name = "repo-cache-reset";
    runtimeInputs = [ pkgs.coreutils ];
    text = ''
      set -euo pipefail
      rm -rf "$PWD/.cache/repo-auto-shell"
      echo "Cleared $PWD/.cache/repo-auto-shell"
    '';
  };

in
pkgs.mkShell {
  packages = builtins.concatLists [
    basePackages
    nativeBuildPackages
    pythonPackages
    nodePackages
    rustPackages
    goPackages
    jvmPackages
    protoPackages
    dockerPackages
    sqlPackages
    browserPackages
    kubernetesPackages
    terraformPackages
    aiPackages
    cudaPackages
    rocmPackages
    [
      repoAutoDoctor
      repoCacheReset
    ]
  ];

  buildInputs = sharedLibPackages ++ darwinFrameworks;

  shellHook = ''
    export REPO_AUTO_SHELL_ROOT=${lib.escapeShellArg rootText}
    export REPO_AUTO_STACKS=${lib.escapeShellArg detectedStacks}
    export REPO_AUTO_CACHE_ROOT="$PWD/.cache/repo-auto-shell"

    export XDG_CACHE_HOME="$REPO_AUTO_CACHE_ROOT/xdg"
    export XDG_STATE_HOME="$REPO_AUTO_CACHE_ROOT/state"
    export XDG_DATA_HOME="$REPO_AUTO_CACHE_ROOT/share"

    export UV_CACHE_DIR="$REPO_AUTO_CACHE_ROOT/uv"
    export PIP_CACHE_DIR="$REPO_AUTO_CACHE_ROOT/pip"
    export POETRY_CACHE_DIR="$REPO_AUTO_CACHE_ROOT/pypoetry"
    export CARGO_HOME="$REPO_AUTO_CACHE_ROOT/cargo"
    export RUSTUP_HOME="$REPO_AUTO_CACHE_ROOT/rustup"
    export SCCACHE_DIR="$REPO_AUTO_CACHE_ROOT/sccache"
    export GOCACHE="$REPO_AUTO_CACHE_ROOT/go-build"
    export GOMODCACHE="$REPO_AUTO_CACHE_ROOT/go-mod"
    export NPM_CONFIG_CACHE="$REPO_AUTO_CACHE_ROOT/npm"
    export YARN_CACHE_FOLDER="$REPO_AUTO_CACHE_ROOT/yarn"
    export PNPM_HOME="$REPO_AUTO_CACHE_ROOT/pnpm-home"
    export BUN_INSTALL_CACHE_DIR="$REPO_AUTO_CACHE_ROOT/bun"
    export PLAYWRIGHT_BROWSERS_PATH="$REPO_AUTO_CACHE_ROOT/playwright"
    export HF_HOME="$REPO_AUTO_CACHE_ROOT/huggingface"
    export HF_HUB_CACHE="$REPO_AUTO_CACHE_ROOT/huggingface/hub"
    export TRANSFORMERS_CACHE="$REPO_AUTO_CACHE_ROOT/huggingface/transformers"
    export OLLAMA_MODELS="$REPO_AUTO_CACHE_ROOT/ollama"
    export GRADLE_USER_HOME="$REPO_AUTO_CACHE_ROOT/gradle"
    export DOCKER_CONFIG="$REPO_AUTO_CACHE_ROOT/docker"
    export MAVEN_OPTS="-Dmaven.repo.local=$REPO_AUTO_CACHE_ROOT/m2"

    export SSL_CERT_FILE="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
    export NIX_SSL_CERT_FILE="$SSL_CERT_FILE"
    export PIP_DISABLE_PIP_VERSION_CHECK=1
    export PYTHONDONTWRITEBYTECODE=1
    export UV_LINK_MODE=copy
    export PATH="$PNPM_HOME:$PATH"

    mkdir -p \
      "$XDG_CACHE_HOME" \
      "$XDG_STATE_HOME" \
      "$XDG_DATA_HOME" \
      "$UV_CACHE_DIR" \
      "$PIP_CACHE_DIR" \
      "$POETRY_CACHE_DIR" \
      "$CARGO_HOME" \
      "$RUSTUP_HOME" \
      "$SCCACHE_DIR" \
      "$GOCACHE" \
      "$GOMODCACHE" \
      "$NPM_CONFIG_CACHE" \
      "$YARN_CACHE_FOLDER" \
      "$PNPM_HOME" \
      "$BUN_INSTALL_CACHE_DIR" \
      "$PLAYWRIGHT_BROWSERS_PATH" \
      "$HF_HOME" \
      "$HF_HUB_CACHE" \
      "$TRANSFORMERS_CACHE" \
      "$OLLAMA_MODELS" \
      "$GRADLE_USER_HOME" \
      "$DOCKER_CONFIG" \
      "$REPO_AUTO_CACHE_ROOT/m2"

    ${lib.optionalString (pkgs.stdenv.isLinux && libraryPath != "") ''
      export LD_LIBRARY_PATH="${libraryPath}:''${LD_LIBRARY_PATH:-}"
    ''}

    ${lib.optionalString (libclangPath != "") ''
      export LIBCLANG_PATH="${libclangPath}"
    ''}

    ${lib.optionalString (cudaRoot != null) ''
      export CUDA_HOME="${toString cudaRoot}"
      export CUDA_PATH="$CUDA_HOME"
    ''}

    ${lib.optionalString (rocmRoot != null) ''
      export ROCM_PATH="${toString rocmRoot}"
    ''}

    if command -v sccache >/dev/null 2>&1; then
      export RUSTC_WRAPPER="$(command -v sccache)"
    fi

    echo "RepoAutoShell: detected $REPO_AUTO_STACKS"
    echo "RepoAutoShell: caches live under .cache/repo-auto-shell"
    echo "RepoAutoShell: run repo-auto-doctor for the full toolchain report"
  '';
}

/*
This solves the annoying April 2026 problem where one repo mixes Python for model work, Node for product code, Rust or Go for hot paths, Playwright for browser checks, and Terraform or Kubernetes for deployment, but every laptop and CI image drifts in a slightly different way. One missing native library, one global cache collision, or one silently different tool version is enough to waste half a day.

Built because I got tired of repos that looked normal at first glance and then failed in five different places: `uv sync` writing into a global cache, `pnpm` using a stale home directory, `cargo` compiling against the wrong clang setup, Playwright downloading browsers into random paths, or local AI model caches filling the wrong disk. A lot of teams still solve this with a README full of manual setup steps. That is fragile, slow, and hard to reproduce when a new engineer or CI runner shows up.

Use it when you want one drop-in Nix shell for a real polyglot repository, especially AI tooling, agent platforms, modern web stacks, infra repos, or data products that combine several runtimes. It auto-detects common project markers, adds the toolchains that matter, localizes the messy caches into `.cache/repo-auto-shell`, and gives you a `repo-auto-doctor` command so you can see what the shell decided to enable. It also lets you force GPU, Playwright, Kubernetes, Terraform, or Ollama support with environment flags instead of editing the file every time.

The trick: this file does two things that save real time. First, it uses filesystem markers like `pyproject.toml`, `package.json`, `Cargo.toml`, `go.mod`, `buf.yaml`, `main.tf`, and Playwright configs to turn features on only when the repo actually needs them. Second, it redirects the noisy caches and machine-specific state into one local tree, which makes cleanup, CI parity, disk management, and debugging much easier. When a tool misbehaves, you no longer have to guess whether it came from some old global config living elsewhere on the machine.

Drop this into the top level of a repo as `RepoAutoShell.nix`, then enter it with `nix develop -f RepoAutoShell.nix` or import it from a flake or another shell definition. If you need CUDA, set `REPO_AUTO_ENABLE_CUDA=1`. If you need Kubernetes tooling, set `REPO_AUTO_ENABLE_K8S=1`. If you want to inspect what the shell enabled, run `repo-auto-doctor`. If a repo needs a slightly different language version, override `pythonAttr`, `nodeAttr`, or `jdkAttr` instead of rewriting the whole shell. I wrote it this way so it is easy to fork, easy to understand in plain English, and still solid enough for serious day-to-day development.
*/