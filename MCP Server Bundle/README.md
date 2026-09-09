# MCP Server Bundle

Your MCP client config works on your laptop and nowhere else. This is a single Nix file that turns a set of stdio MCP server definitions into hermetic launcher scripts plus a generated `mcpServers` manifest, so the same bundle starts identically on a laptop, a CI worker, a devcontainer or an agent host.

**Language:** Nix | **Lines:** 761 | **Added:** 2026-05-11

## What this solves

This solves the April 2026 problem of shipping local MCP server stacks in a way that is actually reproducible across laptops, CI workers, devcontainers and agent hosts. A lot of teams now run a mix of Node, Python, uvx, npx and compiled binaries behind Codex, Claude Desktop, Cursor, Windsurf or internal agent runners, but the painful part is not the server code. The painful part is getting the exact launch command, PATH, working directory and secret handling consistent everywhere without copying the same JSON config into five places.

The failure mode is specific and boring. Someone writes `"command": "npx"` into a client config. It works because their shell has a global Node. A teammate on a fresh machine gets `npx: command not found` and the agent client reports it as a dead server with no useful detail. Another machine has the binary but the process starts in the wrong directory, so a filesystem server silently serves the wrong tree. A CI worker has no `HOME` layout at all. Someone else hardcodes `OPENAI_API_KEY` or `GITHUB_TOKEN` directly into the config file, that file gets committed, and now you have a secret in git history and a rotation job nobody wanted.

What it costs is debugging time spent on process startup instead of tool behaviour. Agent clients are bad at surfacing why a stdio server failed. You get a spinner, then a generic connection error, then you go read logs. The person who notices is whoever joined last week, or the on call engineer when a scheduled agent job stops producing output.

This file removes the whole class of problem by making the launch command a Nix derivation. Each server gets a generated shell wrapper with `set -euo pipefail`, an explicit PATH built from the derivations you listed, literal environment values baked in, required secrets read from the ambient environment at run time with a clear failure message, an existence check on the working directory and an executable check on the resolved command. The manifest the client reads points at that wrapper by store path, not at a bare binary name.

## Why I built it

Built because the normal setup is still messy: someone hardcodes `OPENAI_API_KEY` or `GITHUB_TOKEN` into a config file, someone else depends on `npx` existing globally, another machine has the right package but the wrong working directory, and CI cannot reproduce the bundle that worked on a single developer laptop. Then you lose time debugging agent startup, not the actual tool behavior. I wanted one Nix file that wraps the runtime, keeps secrets out of the store, emits a ready `mcpServers` manifest, and gives a doctor command that explains what was bundled.

The existing options are a `home-manager` module tied to one client, a shell script that assumes your machine, or hand maintained JSON. None of them validate anything before the agent client tries to spawn a process. This validates at evaluation time, so a typo in a server spec is a Nix error with a message naming the server, not a mystery at runtime.

## When to use it

- You run MCP servers from three runtimes at once, say `uvx` for one, `npx` for another and a compiled Go binary for a third, and you want them all to start the same way.
- A new engineer joins and you want their agent client working from a single `nix build` instead of a page of setup instructions.
- CI needs the same MCP stack a developer has, with no global Node or Python on the runner.
- You need secrets to reach the server process without ever landing in a config file or the Nix store.
- You maintain per team or per environment variants of the same fleet and want to fork the bundle rather than rewrite every launcher.
- A server only makes sense on `x86_64-linux` and should quietly drop out of a macOS build.

## How it works

The file is a plain Nix function. Every input goes through a normalizer before anything is generated. `ensureNonEmptyString`, `ensureEnvName`, `ensureBool`, `ensureStringList` and `scalarToString` coerce or reject values, and every failure routes through `fail`, which throws with a `McpServerBundle.nix:` prefix and the offending path. `ensureEnvName` matches `[A-Za-z_][A-Za-z0-9_]*`, so an invalid variable name is caught during evaluation instead of producing broken shell. `scalarToString` accepts strings, paths, ints, floats, bools and derivations, which is what lets you write `${pkgs.nodejs_22}/bin/npx` inline and have it resolve to a store path.

Per server, `normalizeServer` builds the effective spec by layering the bundle defaults under the server overrides. Packages merge through `unique`, literal env merges with `//`, and environment references merge through `mergeEnvRefs`, which converts each reference list into an attrset keyed by target name so the server entry wins over the default. An environment reference is a `{ target, source }` pair, so `fromEnv` accepts either a list of names or an attrset that renames a variable on the way in. `removeEnvTargets` then strips any optional reference whose target is already required, so you never emit two blocks for the same variable. `normalizeCommandSpec` enforces exactly one of `exec`, `command` or `bin` by counting selectors, with `exec` treated as an argv vector split by `head` and `tail`, and any `args` appended after. `cwd` and `workingDirectory` are mutually exclusive. `transport` must be `stdio` or the build throws, because there is no wrapper story for HTTP transports here.

Eligibility is a filter, not an error. `filterAttrs` keeps servers where `enable` is true and `systems` is either empty or contains `pkgs.stdenv.hostPlatform.system`. If nothing survives, `_nonEmptyServerCheck` throws rather than shipping an empty bundle. Launcher names come from `makeLauncherName`, which lowercases, runs `lib.strings.sanitizeDerivationName` and prefixes `mcp-` unless it is already there. Collisions are caught by a `lib.foldl'` over the launcher list carrying a `seen` attrset and a `duplicates` list, which is the cheap way to get set membership in Nix without a real hash set. Two servers that sanitize to the same name fail the build with both names in the message.

Each launcher is a `pkgs.writeShellScriptBin` assembled by `joinSections`, which concatenates non empty blocks with blank lines between them. The order matters: `set -euo pipefail`, then PATH from `lib.makeBinPath` on the selected derivations prepended to `${PATH:-}`, then literal exports escaped with `lib.escapeShellArg`, then required env guards, then optional env guards, then the working directory check, then your `beforeExec` hook, then command resolution, then `exec`. Required variables are read as `${NAME-}` rather than `${NAME}` so `set -u` does not abort before the script can print a useful message. A missing required variable exits 64. A missing working directory exits 66. `renderCommandResolutionBlock` distinguishes the two ways a command can be wrong: a bare name goes through `command -v` and a path is tested with `-x`, and either failure exits 127 with the server name in the message. The final `exec` replaces the shell, so the MCP client talks to the real server process over stdio with no wrapper sitting in the pipe.

The manifest is built with `pkgs.formats.json` and points each server at its launcher store path with empty `args`, since the launcher already carries them. `manifestExtras` per server and `extraManifest` at the top level merge in through `recursiveUpdate`, guarded by `ensureNoReservedKeys` so nobody can overwrite `command`, `args`, `mcpServers` or `xBundle`. The `xBundle` block records the bundle name, the build system, the relative manifest path, `generatedBy` and the server count. `manifestTree` is a `runCommand` that symlinks the manifest into `share/mcp/<name>.json`, and `pkgs.symlinkJoin` merges the launchers, the manifest tree and the optional doctor into one output. The doctor is its own `writeShellScriptBin` with PATH limited to `coreutils`. It locates the manifest relative to its own `bin/` directory using `dirname` and `pwd -P`, falling back to the store path if the joined tree is not there, and supports no argument for a human readable report, `--manifest` to print the path, `--json` to dump the precomputed `doctorPayload`, and `--check-env` to test every required variable across every server and exit 1 if any are missing.

## Usage

```nix
let
  pkgs = import <nixpkgs> { };
  bundle = import ./McpServerBundle.nix {
    inherit pkgs;
    bundleName = "research-agents";
    defaultPackages = [ pkgs.nodejs_22 pkgs.uv ];
    defaultOptionalFromEnv = [ "HTTPS_PROXY" "HTTP_PROXY" ];

    servers = {
      filesystem = {
        command = "${pkgs.nodejs_22}/bin/npx";
        args = [ "-y" "@modelcontextprotocol/server-filesystem" "/srv/workspace" ];
        workingDirectory = "/srv/workspace";
      };

      fetch = {
        exec = [ "uvx" "--from" "mcp-server-fetch" "mcp-server-fetch" ];
        fromEnv = [ "OPENAI_API_KEY" ];
        env.MCP_FETCH_TIMEOUT = 30;
      };

      github = {
        bin = "mcp-server-github";
        packages = [ pkgs.hello ];
        fromEnv = { GITHUB_PERSONAL_ACCESS_TOKEN = "GH_TOKEN"; };
        systems = [ "x86_64-linux" "aarch64-darwin" ];
        launcherName = "gh";
        description = "GitHub issues and PRs";
        beforeExec = "umask 077";
        manifestExtras.disabled = false;
      };

      legacy = {
        command = "old-server";
        enable = false;
      };
    };
  };
in
  bundle.package
```

```bash
# build the bundle
nix build -f ./bundle.nix

# what got bundled
./result/bin/research-agents-mcp-doctor

# path to the generated mcpServers manifest
./result/bin/research-agents-mcp-doctor --manifest

# machine readable bundle description
./result/bin/research-agents-mcp-doctor --json

# verify every required variable is present before starting a client
./result/bin/research-agents-mcp-doctor --check-env

# run one server by hand over stdio
./result/bin/mcp-filesystem
```

The returned attrset exposes `package`, `doctor`, `manifest`, `manifestFile`, `manifestRelativePath`, `launchers`, `launcherPaths`, `servers` and `bundleInfo`, so you can wire the manifest into a `home-manager` file or a devcontainer step without going through the joined output.

## Notes

- stdio only. `transport` throws on anything other than `"stdio"`, because HTTP and SSE servers do not need a launch wrapper.
- Secrets are never written to the Nix store. `fromEnv` and `optionalFromEnv` read from the ambient environment at run time. Anything you put in `env` is baked into a world readable store path, so keep it to non secret settings.
- Exit codes from a launcher: 64 for a missing required variable, 66 for a missing working directory, 127 for a command that is not on PATH or not executable. The doctor uses 64 for bad usage and 1 for a failed `--check-env`.
- `systems` filters silently. A server excluded on the current platform just does not appear in the manifest. If every server is filtered out the build throws rather than producing an empty bundle.
- Launcher names are lowercased and sanitized, so `Fetch` and `fetch` collide and fail the build. That is deliberate, but it means a rename can break a manifest consumer that hardcoded a path.
- `beforeExec` is injected as raw shell with no validation beyond `scalarToString`. It runs after the environment guards and before command resolution. Treat it as trusted input.
- The working directory check only tests that the path is a directory. It does not check permissions, and it does not create the directory for you.
- The doctor only verifies required environment variables and prints the bundle shape. It does not launch servers, does not speak the MCP protocol and will not tell you whether a server actually responds to `initialize`.
