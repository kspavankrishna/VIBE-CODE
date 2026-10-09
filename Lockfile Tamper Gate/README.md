# Lockfile Tamper Gate

A single file Nim CLI that checks an npm package-lock.json for lockfile injection, swapped tarballs, rogue registry hosts and unpinned git dependencies. It also diffs against the lockfile on your target branch so a pull request that quietly rewrites an integrity hash fails CI.

**Language:** Nim | **Lines:** 511 | **Added:** 2026-10-09

## What this solves

Most teams review the source diff in a pull request and wave through the lockfile diff. The lockfile is a few thousand lines of JSON that nobody reads, and that is exactly why it is a good place to hide something. This is called lockfile injection. An attacker, or a careless contributor, edits `package-lock.json` so that a normal looking package name resolves to a different tarball. The `package.json` stays clean. The reviewer sees nothing odd. `npm ci` then installs whatever the lockfile says.

Lockfile Tamper Gate looks for the specific shapes this takes:

- A `resolved` URL that points at a host you did not approve, including look alike hosts such as `registry.npmjs.org.evil.test`.
- A plain `http` or other non https transport.
- A registry package with no integrity hash, or a hash that is not valid base64 of the right length for its algorithm.
- A tarball path that does not match the package it is filed under, for example the entry `left@1.2.3` resolving to `/right/-/right-9.9.9.tgz`.
- A git dependency pinned to a branch or tag instead of a full commit hash.
- The same name and version carrying two different integrity hashes in one lockfile.
- With a base lockfile: the same version with a changed digest, a changed resolved host, a version that went backwards, and packages that newly run install scripts.

It does not talk to the network. It reads one or two JSON files and returns an exit code, so it is safe to run on untrusted pull request branches.

## Why I built it

I kept seeing the same gap. `npm audit` tells you about known vulnerabilities in packages that are already published. It says nothing about whether the lockfile in your pull request still points at the packages it claims to. Tools that do cover this are usually written in JavaScript, which means installing npm packages in order to check npm packages. I wanted something that ships as one static binary, starts in milliseconds and has no dependency tree of its own to worry about.

Nim fits that job well. It compiles to a small native executable through C, the standard library already has a JSON parser, a URI parser and an option parser, and the code reads like Python. The whole tool is one file with no third party imports, so there is nothing to vendor and nothing to pin.

I also wanted the failure modes to be explicit. Every check has a rule id from `R001` to `R011`, a severity, and a message that says what was found and where. You can silence a rule by id or allow a single package by name, but you cannot make the tool quietly pass something it did not understand. A lockfile it cannot parse exits with code 2, not 0.

## When to use it

- As a required CI check on any repository that commits `package-lock.json`.
- On pull requests from forks, where you want a check that cannot be influenced by the contents of the branch. Run it from the base branch checkout and point it at the head lockfile.
- When you run a private registry such as Artifactory or Verdaccio and want to be sure nothing resolves outside it.
- During a dependency bump review, to see exactly which packages gained install scripts.
- As a pre commit hook for people who run `npm install` with a different registry configured on their laptop than in CI.

It is not a replacement for `npm audit`, a vulnerability scanner or a software bill of materials generator. It answers a narrower question: does this lockfile still say what a registry would say, and did anything change that should not have changed.

## How it works

The code is organised as a small pipeline: load, normalise, check each entry, check the whole lock, check the diff, render.

**Loading.** `loadLock` refuses files larger than `MaxLockBytes` (96 MiB), reports missing files and invalid JSON through `LockError`, and passes the parsed document to `parseLock`. `parseLock` requires an integer `lockfileVersion` between 1 and 3. For versions 2 and 3 it reads the flat `packages` map and takes the package name from the `name` field when present, otherwise from the path after the last `node_modules/` (`nameFromPath`). For version 1 it walks the nested `dependencies` tree with `walkV1`, building paths like `a>b`. In version 1, git and file sources live in the `version` field with no `resolved`, so `walkV1` copies the version into `resolved` in that case. Everything is converted to the same `Entry` object, so the rules do not care which npm version wrote the file.

**Per entry checks.** `checkEntry` skips symlinked workspace links, bundled dependencies and workspace members (`isWorkspaceMember`). For everything else it parses `resolved` with `parseUri` and branches on the scheme:

- `file:` sources raise `R006` as a warning.
- Git like sources are detected by `isGitLike`, which checks the scheme against `GitSchemes` and the host against `GitHosts`. They raise `R006`, and then `gitRef` extracts the ref from the URL fragment or from a codeload style path. If `isFullCommit` says it is not a 40 or 64 character hex string, that is `R007`.
- Registry sources must be `https` (`R002`) and the host must pass `hostAllowed` (`R001`). The default allowlist is `DefaultRegistryHost`, which is `registry.npmjs.org`. Extra hosts come from `--allow-host`, and a pattern such as `*.example.com` matches any subdomain but not the bare domain.

Registry sources then go through `checkIntegrity` and `checkTarballPath`. `parseIntegrity` splits the space separated Subresource Integrity string, strips option suffixes, and validates each token with `b64DecodedLen`, which checks canonical padded base64 and returns the decoded byte count without allocating. That count is compared with `expectedDigestBytes`: 20 for sha1, 32 for sha256, 48 for sha384 and 64 for sha512. A short or garbled hash is `R004` as an error. A lockfile that only has a sha1 digest is `R004` as a warning. A missing hash is `R003`.

`checkTarballPath` handles the swap attack. npm style tarball URLs look like `/<name>/-/<bare>-<version>.tgz`. The function URL decodes the path, splits at `/-/`, and checks that the owner part ends with the package name and the file equals the bare name plus version. It uses an ends with check on the owner so that registries such as Artifactory that prefix the path (`/api/npm/repo/left/-/left-1.2.3.tgz`) still pass. Scoped packages work because the bare name is the part after the slash.

**Whole lock check.** `checkConflicts` builds a table of `name@version` to digests. If the same name and version appears twice and any algorithm that both entries share has a different digest, it raises `R008`. It only compares algorithms present on both sides, so one entry with sha512 and another with only sha1 is not a false alarm.

**Diff check.** `checkDiff` indexes the base lockfile by path. For a path in both files with the same version, a changed digest is `R008` and a changed resolved host is `R010` as an error, while a path only change is `R010` as a warning. A version that dropped, compared by the numeric core in `isDowngrade`, is `R011`. A package that is new or newly has `hasInstallScript` is `R009`. If you pass `--max-new-install-scripts=N` and more than N packages qualify, an extra summary `R009` error is added. If the two files use different layouts, the diff stops with a clear message instead of reporting every package as new.

**Output.** `analyze` sorts findings by severity, then rule, then path, so the output is stable between runs and easy to diff. `render` prints text or JSON. The exit code is 1 when any finding reaches `--fail-on` (default error), 0 otherwise and 2 for usage or parse problems.

`selfTest` builds small lockfiles in memory for every rule and asserts the right rule fires, including negative cases such as a clean lock, a scoped package and a properly pinned git dependency.

## Usage

Build it with any recent Nim 2.x:

```
nim c -d:release -o:lockfiletamper LockfileTamperGate.nim
```

Run the built in tests first if you want to confirm your build:

```
./lockfiletamper selftest
```

Check one lockfile:

```
./lockfiletamper check package-lock.json
```

Check a pull request against the target branch, failing on warnings too and capping new install scripts at zero:

```
git show origin/main:package-lock.json > /tmp/base-lock.json
./lockfiletamper check package-lock.json --base=/tmp/base-lock.json --fail-on=warn --max-new-install-scripts=0
```

Allow an internal mirror and one package that is meant to come from git:

```
./lockfiletamper check package-lock.json --allow-host=npm.internal.example --allow-host=*.mirror.example --allow-git=my-forked-lib
```

Machine readable output for a CI annotation step:

```
./lockfiletamper check package-lock.json --format=json
```

List every rule:

```
./lockfiletamper rules
```

Options take the `--name=value` form. Space separated values are not supported.

## Notes

- **No network.** It never fetches a tarball, so it cannot confirm that a hash is the one the registry publishes. It confirms that the lockfile is internally consistent, points where you expect and did not change underneath you. Pair it with `npm ci` which verifies the hash at install time.
- **Duplicate JSON keys.** The standard library parser keeps the last value for a repeated key. A hand edited lockfile with a duplicated path would show only the final entry. If that is part of your threat model, run a strict JSON linter first.
- **Layout match.** The diff needs both files from the same lockfile layout. Regenerate with the same npm major version on both sides.
- **False positives.** Private registries that rewrite tarball paths in unusual ways may trigger `R005`. Use `--ignore=R005` for that rule or open the path format as an issue. Workspace packages and bundled dependencies are skipped on purpose.
- **Scope.** It covers npm lockfile versions 1, 2 and 3. It does not read `yarn.lock` or `pnpm-lock.yaml`.
- **Verified with.** Nim 2.0.17 on Linux, using the `selftest` command and manual runs of the `check` command with and without `--base`.
