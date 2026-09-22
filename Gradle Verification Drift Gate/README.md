# Gradle Verification Drift Gate

Gradle's dependency verification file is supposed to be the one place your build cannot be fooled about what it downloaded. This is a Groovy CLI that diffs two versions of that file and fails CI the moment the diff quietly makes verification weaker instead of wider.

**Language:** Groovy | **Lines:** 712 | **Added:** 2026-09-22

## What this solves

Gradle has a real answer to dependency substitution attacks: dependency verification. You run `./gradlew --write-verification-metadata sha256` once, it writes `gradle/verification-metadata.xml`, and from then on Gradle refuses to build if a downloaded artifact's hash does not match what is recorded there. Teams that turn this on treat the file the way they treat a lockfile: commit it, review it, trust it.

The problem is that the file is also machine generated, and the same command that keeps it honest is the command that can quietly launder a bad change into it. Run `--write-verification-metadata` again after a dependency has been swapped upstream, or after a build server got compromised, or after someone fat fingered a `mavenLocal()` repository ahead of the real one, and the tool will happily write down whatever hash it downloaded this time, no complaint, no diff highlighted. The file still looks complete. CI still goes green. The one signal that would have caught the problem, an artifact hash changing at a coordinate that is supposed to be immutable, is buried in a multi thousand line XML diff that nobody reads line by line before merging.

The file has other soft spots that are just as easy to miss in review. A `<trust file="*.jar" regex="true"/>` entry looks like three lines in a diff, and it means every artifact matching that pattern skips checksum verification entirely, at any version, forever. A new `<ignored-key>` entry silences a PGP signature failure for one key across the whole build, and if nobody fills in the reason attribute nobody will remember why in six months. Flipping `verify-metadata` or `verify-signatures` from `true` to `false` is a one line change that turns the entire mechanism off, and it reads exactly like a formatting fix in a diff view. None of this is hypothetical: Gradle's own docs warn about the regex trust escape hatch, and the whole reason `verify-metadata.xml` exists is a response to real dependency confusion and artifact substitution attacks against JVM build tooling.

This tool reads a baseline (the last version a human actually reviewed and approved) and a candidate (whatever `--write-verification-metadata` just produced) and turns the diff into a ranked list of findings: what got safer, what got weaker and what needs a human to look at it before the file is allowed to replace the baseline.

## Why I built it

Every other supply chain gate in this collection reads a lockfile that is mostly append only: new entries are additions, and additions are usually fine. Gradle's verification metadata is different, because Gradle itself will happily rewrite an existing entry in place when it re-resolves a dependency, and an in place rewrite of a hash is exactly the thing you cannot tell apart from tampering just by looking at "was this line added." A tool for this file has to diff by coordinate, not by line, and it has to know the difference between a component that is new (fine, usually) and an artifact whose hash changed at a coordinate that already existed (never fine, full stop).

Groovy is not a stylistic choice here. `build.gradle` files, Gradle init scripts and Gradle plugins are written in Groovy on a huge number of real projects, and `groovy` ships next to `gradle` on any machine that already has a Gradle installation through a wrapper or via sdkman. A team that wants this gate does not need to stand up a new toolchain, install a package manager dependency or convince a security review that a new runtime is safe to add to the build. It is one file that runs with the interpreter already sitting on the CI image.

## When to use it

- CI regenerates `verification-metadata.xml` (or a developer runs `--write-verification-metadata` locally) and you want the diff checked before it can replace the committed baseline.
- A dependency bump PR touches the lock file and the verification metadata together, and you want to know if the metadata change is "new component, normal" or "existing hash changed, stop."
- Someone opens a PR that edits `trusted-artifacts` or `ignored-keys` and you want that flagged with the same seriousness as a change to a GitHub Actions permissions block, because functionally it is the same kind of change.
- You want proof, in a format a security reviewer can read in ten seconds, that a dependency your `gradle.lockfile` resolves actually has verification coverage, instead of assuming it does because the file exists.
- You are rolling out dependency verification across a monorepo and want a gate that fails loud on regressions while staying quiet on the normal churn of new dependencies being added with proper hashes.

## How it works

Parsing lives in `parseModelText`, which reads the metadata XML with Groovy's `XmlSlurper` (resolved through `newXmlSlurper`, which looks up `groovy.xml.XmlSlurper` first and falls back to the pre Groovy 3 `groovy.util.XmlSlurper`, so the same file runs on an old bundled Groovy and a fresh standalone install without edits) and builds a `VerificationModel`: the `verify-metadata` and `verify-signatures` flags, the `trusted-artifacts` list as `TrustEntry` records, the `ignored-keys` list as `IgnoredKeyEntry` records, and a `components` map keyed by `group:name:version`, each holding an `ArtifactEntry` per file name with its hash algorithms and values. Every interpolated key is explicitly coerced with `.toString()` before it goes into a map or a comparison, because Groovy's `"${a}:${b}"` produces a `GString`, and a `GString` key can fail to match a plain `String` key on lookup even when the two print identically. That bug cost real debugging time while building this and the fix is now load bearing, not decorative.

The comparison itself is `diffModels`. Global flags are checked first: if `verify-metadata` or `verify-signatures` was `true` in the baseline and is `false` in the candidate, that is `VerificationDisabled` or `SignatureVerificationDisabled`, both critical, no further analysis needed for that half of the file. Components are then walked from the candidate side. A component whose exact `group:name:version` key already existed in the baseline gets a per artifact hash comparison: any algorithm present in both baseline and candidate whose value differs is `ArtifactHashChanged`, critical, because an artifact at a fixed released coordinate should never produce a different hash on a second download. If the baseline had a strong hash (`sha256` or `sha512`, configurable through `Policy.strongHashAlgorithms`) on an artifact and the candidate kept only a weaker one, that is `ArtifactHashAlgorithmDowngraded`, high. A component whose key is new gets two checks instead: `WeakHashOnlyNewComponent` (low) if none of its artifacts carry a strong hash, and `StaleArtifactReintroduced` (high) if its version compares lower than the highest version already seen for that `group:name` in the baseline, using `compareVersions`, a tokenizing comparator (`versionTokens` splits on digit and letter runs, `isNumericToken` decides whether to compare a pair numerically or lexically) that is deliberately a heuristic and not a full Maven or semver ordering, documented as such below. A component present in the baseline and missing from the candidate is `ArtifactVerificationRemoved`, high, but only when a locked coordinate you supplied still resolves it, because losing coverage for a dependency nobody uses anymore is just cleanup.

Trust and ignored key entries are diffed by their own `key()` methods so an unchanged entry never re-triggers. A new `trust` entry is `BroadTrustEntryAdded` (high) if it sets `regex="true"` or omits `group`/`name` entirely, since either one trusts a pattern of artifacts rather than one pinned coordinate; if it is fully group/name scoped but has no `version`, it is `VersionlessTrustEntry` (medium), trusting every release ever published at that coordinate. A new `ignored-key` entry is `NewIgnoredKeyWithoutReason` (high) if the `reason` attribute is blank, or `WeakeningIgnoredKeyAdded` (medium) if a reason is present, because silencing a signature failure is a policy loosening even when it is explained.

The lockfile cross check is the part most teams skip by hand. `resolveLockfileCoords` accepts individual files or a directory (it walks it for anything ending in `.lockfile`, which covers both the single unified `gradle.lockfile` and the legacy per configuration files under `gradle/dependency-locks/`), and `parseLockfileCoords` strips comments and the trailing `=configuration,configuration` suffix to get plain `group:name:version` coordinates. Any locked coordinate that has no matching key in either the baseline or the candidate becomes `UnverifiedLockedDependency`, high: Gradle resolved and locked it, but nobody ever ran the metadata writer against it, so it builds with zero verification.

Every finding carries a stable `id`, a `Severity` and enough context to act on it. `atOrAboveThreshold` compares a finding's severity against `Policy.failOn` (default `high`) using a fixed critical/high/medium/low/info ordering, and `runGate` exits `0` when nothing meets that bar, `1` when something does and `2` on a usage or parsing problem, so it slots into a shell `&&` chain the same way any other CI gate does. Output comes in three shapes from `renderText`, `renderJson` and `renderSarif`: text for a terminal or PR comment, JSON for scripting and SARIF 2.1.0 (one rule per unique finding id, results pointing at the candidate file) for GitHub code scanning, with the same documented limitation as this repository's other SARIF emitters, that locations are pinned to line 1 because the model does not track byte offsets back into the source XML.

The whole thing checks itself. Running with `--selftest` builds a baseline and a candidate entirely from strings embedded in the `SelfTest` class, runs `diffModels` against them with a couple of locked coordinates, and asserts that all eleven finding types fire exactly where expected and nowhere else, plus a handful of direct assertions on `compareVersions`. No temp files, no fixture directory to keep in sync with the code and it is the fastest way to see the full rule set exercised in one run.

## Usage

```bash
# Regenerate metadata, then gate the diff before it replaces the committed baseline
cp gradle/verification-metadata.xml /tmp/verification-metadata.baseline.xml
./gradlew --write-verification-metadata sha256,pgp
groovy GradleVerificationDriftGate.groovy \
  --baseline /tmp/verification-metadata.baseline.xml \
  --candidate gradle/verification-metadata.xml \
  --lockfile gradle.lockfile \
  --fail-on high

# Machine readable report, useful in a PR check
groovy GradleVerificationDriftGate.groovy \
  --baseline base.xml --candidate head.xml --format json --out drift-report.json

# SARIF for the GitHub Security tab
groovy GradleVerificationDriftGate.groovy \
  --baseline base.xml --candidate head.xml --format sarif --out verification-drift.sarif

# Tighter policy: only sha512 counts as strong, only block on critical
cat > policy.json <<'JSON'
{ "strongHashAlgorithms": ["sha512"], "failOn": "critical" }
JSON
groovy GradleVerificationDriftGate.groovy \
  --baseline base.xml --candidate head.xml --policy policy.json

# See every rule fire against embedded fixtures, no files needed
groovy GradleVerificationDriftGate.groovy --selftest
```

Exit codes: `0` the candidate passes the gate, `1` a finding at or above `--fail-on` was raised, `2` a file was missing, the XML did not parse or an argument was wrong. Run `--help` for the full flag list.

## Notes

- This audits the metadata file and an optional lockfile against each other. It does not download anything, does not verify a hash against real network content and does not check a PGP signature itself. A baseline that was wrong on day one stays wrong; the tool only catches the file getting worse from there, which is why the baseline should be the version that a human actually reviewed, not just whatever was there yesterday.
- `compareVersions` is a tokenizing heuristic, not a Maven or semver comparator. It handles the common case of dotted numeric versions with a trailing qualifier well enough to flag a real downgrade, but it does not understand pre-release ordering, so `1.0.0-alpha` can sort above `1.0.0`. Treat `StaleArtifactReintroduced` as a prompt to look at the component, not as an authoritative version ordering.
- `ArtifactVerificationRemoved` only fires when the removed coordinate is still present in a lockfile you passed with `--lockfile`. Without that flag the tool has no way to know whether a removed component is dead code or a live dependency losing coverage, so it stays silent on removals, which is why passing your lockfile matters as much as passing the metadata files.
- SARIF locations always point at line 1 of the candidate file. The model is built from `XmlSlurper`, which does not expose line numbers, so results carry the finding and the coordinate but not a byte accurate jump target.
- `--fail-on none` runs the full diff and prints every finding but always exits `0`, which is useful for a first rollout where you want visibility before you want an enforced gate.
