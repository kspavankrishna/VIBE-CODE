# Flake Lock Supply Chain Gate

A committed `flake.lock` looks pinned, but it can still hide registry indirection, local path inputs, missing narHash values, unapproved source hosts and revisions nobody has looked at in a year. This is a pure Nix expression that audits the lock graph and fails CI with structured evidence, no nixpkgs, no jq, no shell script.

**Language:** Nix | **Lines:** 761 | **Added:** 2026-08-04

## What this solves

The Nix flake.lock supply chain audit problem shows up when AI agents, build farms, GitHub Actions and developer laptops all update inputs at different times. Everyone treats the lockfile as the contract. It is, right up to the point where one entry stops being immutable. An `indirect` input resolves through whatever registry the client happens to have configured, so the same lock can pull a different repository on a runner than on your machine. A `path` input points at a directory that exists on one laptop and nowhere else. A `github` entry with no `rev` is not a pin at all. A tarball fetched over plain HTTP has no integrity story worth the name.

The failure is quiet. The build passes where the lock was produced, it passes in CI, and it keeps passing until someone runs `nix flake update` and the diff pulls a commit from a fork that was renamed under you. Nobody catches it at review time because a lockfile diff is a wall of hex and the badge is green. The cost lands later: a compromised dependency shipped through a build system everyone assumed was reproducible, with no audit trail explaining why the update looked safe.

The second failure is drift. Duplicate nixpkgs pins are the common one: a dependent flake stops following the root nixpkgs, you carry two revisions, your closure doubles, and your patched glibc is patched in half the tree. Stale pins are the other. An input locked eighteen months ago is not reproducible, it is abandoned. Neither shows up as an error anywhere in the Nix toolchain. This file turns all of it into findings, reading the lock graph directly and applying a policy attrset you control.

## Why I built it

Every existing option pulls in the thing it is supposed to be guarding. A shell script means jq. A flake app means nixpkgs, so the audit tool depends on the graph under audit. Python means a dev shell and the same problem one level down. The actual policy questions are small and entirely answerable from the JSON already in the repo.

So the whole gate is one importable Nix expression with no dependencies beyond `builtins`. Because Nix is lazy, you can read the report without triggering the failure, which matters in CI where you want the SARIF uploaded to code scanning even on the run that blocks the merge.

## When to use it

- CI needs to block a `flake.lock` update introducing an input from a host outside your allowlist.
- Your build farm and your laptops disagree about what a flake resolves to and you suspect an `indirect` registry input.
- A repo has quietly accumulated three nixpkgs revisions and you want it reported, not discovered during a closure audit.
- You want lockfile risk in the GitHub Security tab as SARIF, next to your other code scanning findings.
- A bot is opening dependency update PRs and you need a machine readable reason to approve or reject each one.
- You have a documented exception for one node and want it suppressed in policy, not argued about every review.

## How it works

The entry point takes `lockFile`, an optional pre parsed `lockJson` and a `policy` attrset merged over `defaultPolicy` with `//`. The defaults carry the knobs: `allowedHosts` (github.com, gitlab.com, git.sr.ht, sourcehut.org, codeberg.org), `allowRegistries`, `allowPathInputs`, `requireNarHash`, `requireRevisionPins`, `forbidMovingRefs`, `maxNixpkgsPins`, `failOn` (critical and high), three ignore lists, a `severityOverrides` map and per type freshness limits in `maxAgeDaysByType`.

Parsing is deliberately defensive. `getAttr` returns null instead of throwing on a missing key, and `firstNotNull` folds a candidate list down to the first non null value, so every field reads as "locked, then original, then a fallback". `mkNode` flattens each entry in `nodes` into a record with `type`, `rev`, `narHash`, `lastModified`, `ref`, `url`, `host`, `source` and `inputs`. Host resolution is the fiddly part. `hostFromSource` prefers an explicit `host` attribute, falls back to the canonical host for `github`, `gitlab` and `sourcehut`, and otherwise parses the URL. Two regex matchers cover both forms, `schemeHostFromUrl` for `scheme://host/...` and `scpHostFromUrl` for `user@host:path` SCP style Git remotes, then `normalizeHost` strips any `user@` prefix so the allowlist cannot be spoofed by a userinfo segment.

`nodeFindings` is the rule table. Twelve checks run per node: missing `locked` metadata, unknown source type, indirect registry input, path input, unapproved host, missing narHash, malformed narHash, missing revision pin, malformed revision, moving ref, non HTTPS tarball and staleness. Which types require what is data, not code: `narHashRequiredTypes` and `revisionRequiredTypes` are plain lists. `validNarHash` accepts the SRI prefixes sha256-, sha512- and sha1-. `validRevision` accepts 40 or 64 hex characters, covering Git SHA-1 and SHA-256 object ids. `isMovingRef` tests the requested ref against `disallowedMovingRefs` (main, master, trunk, develop, latest, stable and friends), catching the lock that is pinned today but configured to chase a branch on the next update.

Staleness needs a clock and Nix is pure, so `isStale` only fires when you pass `policy.nowEpoch`. It compares `nowEpoch - lastModified` against `staleLimitForType`, which reads `maxAgeDaysByType` and falls back to `defaultMaxAgeDays` of 180. A limit of 0 disables the check, which is why `path`, `indirect` and `unknown` are zeroed. Graph level checks run separately: `rootEdgeFindings` flags root inputs resolving to nothing or pointing at a node absent from the graph, `requiredRootInputFindings` covers `policy.requiredRootInputs`, and `nixpkgsFindings` identifies nixpkgs nodes by name, registry id or `NixOS/nixpkgs` owner and repo, then counts distinct revisions against `maxNixpkgsPins`.

Every finding passes through `makeFinding`, which applies `severityOverrides` by id, computes `blocking` by testing the final severity against `failOn` and maps severity to a SARIF level via `severityToSarifLevel`. `isIgnored` then filters against the three ignore lists, and both `rawFindings` and `findings` are exported so you can see what was suppressed. The outputs fan out from there: `summary` with counts by severity, `rootInputMatrix` joining each root input to its resolved node and findings, `byNode` keyed by node name, `markdown` for a PR comment and `sarif` as a 2.1.0 run with one rule per finding. `assertNoBlockers` is the gate itself, `if pass then true else throw failureMessage`, lazy, so reading the JSON never trips it.

## Usage

```bash
# Fail the build on blocking findings, with a real clock for staleness
nix eval --impure --expr '(import ./FlakeLockSupplyChainGate.nix {
  lockFile = ./flake.lock;
  policy.nowEpoch = builtins.currentTime;
}).assertNoBlockers'

# Machine readable summary
nix eval --json --expr '(import ./FlakeLockSupplyChainGate.nix {}).summary'

# SARIF for GitHub code scanning
nix eval --raw --expr '(import ./FlakeLockSupplyChainGate.nix {}).sarifJson' > flake-lock.sarif

# Markdown block for a PR comment
nix eval --raw --expr '(import ./FlakeLockSupplyChainGate.nix {}).markdown'

# Tighter policy
nix eval --impure --expr '(import ./FlakeLockSupplyChainGate.nix {
  lockFile = ./flake.lock;
  policy = {
    allowedHosts = [ "github.com" "git.internal.example" ];
    requiredRootInputs = [ "nixpkgs" "flake-utils" ];
    failOn = [ "critical" "high" "medium" ];
    nowEpoch = builtins.currentTime;
    ignoreFindingIds = [ "MovingReferenceRequested" ];
    severityOverrides.StaleLockedInput = "low";
  };
}).assertNoBlockers'
```

Also exposed: `version`, `policy`, `rootName`, `rootInputs`, `rootInputMatrix`, `nodeRecords`, `byNode`, `rawFindings`, `findings`, `blockerFindings`, `summaryJson` and `recommendedCiCommand`.

## Notes

- Staleness is opt in. Without `policy.nowEpoch` the `StaleLockedInput` check never fires, because a pure expression has no clock. `builtins.currentTime` requires `--impure`.
- It audits the lockfile, nothing else. No fetching, no verifying a narHash against real content, no signatures, no CVE feeds. A well formed lock pointing at a malicious commit passes every check here.
- Host allowlisting needs a derivable host. With no `host`, no recognised type and no parseable URL, `record.host` is null and `UnapprovedSourceHost` cannot fire. That surfaces as `UnknownSourceType` at medium severity instead.
- Failure is a `throw`, not an exit code. `assertNoBlockers` makes `nix eval` exit non zero, listing each blocking finding as `Id:node`. Read `summary` or `sarifJson` first if you want the report before the failure.
- `severityOverrides` is keyed by finding id, so an override changes severity for every node triggering that rule. Per node exceptions go through `ignoreNodes`, per root input exceptions through `ignoreRootInputs`.
- SARIF locations are all pinned to line 1 of `policy.ciPath`. The expression does not track byte offsets, so findings do not point at the offending node's line.
