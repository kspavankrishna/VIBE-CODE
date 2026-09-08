# Composer Risk Ledger

`composer.lock` decides exactly what PHP code runs in production, and almost nobody reviews it. This is a single file PHP tool that turns a lockfile plus optional `composer audit` JSON into a deterministic risk score with JSON, Markdown or SARIF output and a CI exit code.

**Language:** PHP | **Lines:** 816 | **Added:** 2026-07-16

## What this solves

This solves the PHP supply chain problem where `composer.lock` is treated like a boring build artifact even though it decides exactly what code ships to production, CI runners, Laravel apps and Symfony services. A lockfile diff in a pull request is a wall of hashes and version bumps. Reviewers scroll past it. Six months later a transitive dependency is abandoned, published from a personal Git host nobody recognises, and it happens to be a `composer-plugin` that executes code during `composer install` on every CI runner you own.

The failure modes are boring individually and expensive together. An abandoned package stops receiving vulnerability fixes, so a CVE lands and there is no patched version to upgrade to. A GPL or AGPL dependency slips into a commercial product and legal finds out during a customer audit, not during code review. A package locked to `dev-master` or an alpha means your build is only reproducible until someone re-resolves. A wildcard `*` constraint inside a dependency's own `require` block lets a future lockfile update silently widen the range. The person who notices is rarely the person who introduced it, and by then the fix is a migration rather than a version bump.

`composer audit` catches published advisories and nothing else: no view of licences, abandonment, package age, source host, plugin code execution or constraint hygiene. Most tools that do cover that ground want your dependency graph uploaded to a SaaS scanner, which ships private package names off the network. This file does none of that. No dependencies, no network calls, nothing leaves the machine.

## Why I built it

I wanted a Composer security gate that runs offline and gives a number rather than a pass or fail flag. Existing options split badly: `composer audit` covers advisories and stops, commercial SCA platforms cover everything but need an account and an upload, and homegrown scripts end up as one grep per team with no shared scoring.

The other gap is that dependency risk is not binary. A dev only package with a stale release is not the same problem as a production `composer-plugin` from an untrusted host with a high advisory against it, but a boolean gate treats both as fine or fatal. The score here combines independent findings per package, so the worst dependency and the shape of the whole graph are both visible, and the threshold stays a policy decision.

## When to use it

- A dependency update pull request lands and you want a scored diff instead of a wall of version bumps.
- Release approval needs evidence that nothing abandoned, unlicensed or wildcard constrained is shipping.
- A customer or auditor asks for a licence inventory for a PHP service.
- You build air gapped, so uploading a dependency graph to a SaaS scanner is not an option.
- You already run `composer audit --format=json` in CI but nothing consumes the output as a gate.
- You want findings in GitHub code scanning via SARIF upload without buying a scanner.

## How it works

The entry point is `ComposerRiskLedger::analyze(bool $productionOnly)`. It normalises every entry under `packages` and, unless production only mode is on, `packages-dev`, through `normalizePackage()` into a struct carrying name, version, type, a dev flag and the raw lockfile node. Names are lowercased so advisory lookups match regardless of case, and packages are sorted by name before analysis, which with the sorted output makes the report byte stable for the same inputs.

`findingsForPackage()` runs every rule against one package and returns `Finding` objects, a readonly class holding an id, package, severity, integer score, message, an evidence array and a remediation string. Rules cover advisories from `composer audit` JSON (`COMPOSER-AUDIT-CRITICAL` through `COMPOSER-AUDIT-LOW`), abandonment (`COMPOSER-ABANDONED-PACKAGE`, which surfaces the suggested replacement when the lockfile gives one), blocked vendors, licences, untrusted source hosts, stale releases, unstable versions, Composer plugins and loose constraints. `licenseFinding()` returns at most one finding per package, in priority order: missing metadata, blocked licence, not on the allow list, then copyleft. `hostAllowed()` accepts an exact match or a subdomain suffix, so `raw.githubusercontent.com` passes a `github.com` trust entry but `git.internal.example` does not. Release age comes from the lockfile `time` field parsed as UTC, and a package older than twice `maxReleaseAgeDays` escalates from low to medium.

Scoring is where the design choice sits. `combineScores()` treats each finding's score as an independent probability of trouble and combines them with a noisy OR: it multiplies the survival terms `1 - score/100` across all findings and returns `1 - survival` as a percentage. Three medium findings compound into something meaningfully worse than any one of them, but the result saturates towards 100 instead of overflowing the way a sum would. Dev only packages go through `devAdjustedScore()`, which scales the weight by 0.72 with a floor of 8, so a build tool problem still shows up without dominating a production risk number. Every weight lives in the `scoreWeights` policy block.

`overallRisk()` then blends the worst package at 68 percent with the mean of the top eight scores at 32 percent. A pure max hides a graph full of medium problems, a pure mean lets one critical package disappear into a thousand clean ones. `highestPackageRisk` is reported separately so you can gate on either.

`mergePolicy()` only accepts keys that already exist in the default policy, so a typo in a policy file is ignored rather than creating a dead setting, and `scoreWeights` merges key by key with `array_replace`. The policy is hashed with SHA-256 into `policyHash` on every report, so you can prove which policy produced which score. `ComposerRiskCli` parses arguments, reads the audit file in both shapes Composer has used (a top level `advisories` map and a `packages.<name>.advisories` map), then renders JSON, Markdown or SARIF 2.1.0. The SARIF writer dedupes rules by finding id, maps critical and high to `error`, medium to `warning` and the rest to `note`, and sets `security-severity` to score over ten with a 0.1 floor so GitHub code scanning ranks it correctly. The CLI fires only when the file is invoked directly, checked with `realpath($argv[0]) === __FILE__`, so you can `require` it as a library instead.

## Usage

```bash
# Advisory input (optional but recommended)
composer audit --format=json > audit.json

# JSON report, default lockfile, default threshold of 75
php ComposerRiskLedger.php

# SARIF for GitHub code scanning, fail the build at 70
php ComposerRiskLedger.php --audit audit.json --format sarif --fail-at 70 > composer-risk.sarif

# Production dependencies only, custom policy, human readable summary
php ComposerRiskLedger.php --production --policy composer-risk-policy.json --format markdown

php ComposerRiskLedger.php --help
```

Flags: `--lock <path>` (default `composer.lock`), `--audit <path>`, `--policy <path>`, `--format json|sarif|markdown`, `--fail-at 0..100` (default 75), `--production`, `--help` or `-h`.

As a library:

```php
require __DIR__ . '/ComposerRiskLedger.php';

$lock   = json_decode(file_get_contents('composer.lock'), true);
$policy = ['allowCopyleft' => false, 'blockedVendors' => ['acme'], 'scoreWeights' => ['abandoned' => 95]];

$report = (new ComposerRiskLedger($lock, $policy, $advisoriesByName))->analyze(productionOnly: true);

echo ComposerRiskLedger::json($report), $report['overallRisk'];
```

Policy keys accepted: `allowedLicenses`, `blockedLicenses`, `copyleftLicenses`, `allowCopyleft`, `trustedSourceHosts`, `blockedVendors`, `criticalPackages`, `maxReleaseAgeDays`, `failOnComposerPlugins`, `scoreWeights`.

## Notes

- Exit codes: `0` below `--fail-at`, `1` on an input or runtime error written to STDERR, `2` at or above the threshold. The report still prints to STDOUT before a `2`.
- Requires PHP 8.1 or later: readonly promoted properties, `match`, `str_contains` and `str_ends_with` are all used. No Composer dependencies, no network calls.
- It reads only what the lockfile records. No tree resolution, no Packagist calls, no dist hash verification, and no way to tell whether an installed `vendor/` matches the lock.
- Advisories come entirely from the file you pass to `--audit`. Without it the advisory rules produce nothing and the score reflects hygiene signals only.
- `criticalPackages` is accepted and hashed into the policy but no rule consumes it. It does not change any score.
- `isUnstableVersion()` is a substring regex, so any version containing `rc`, `beta`, `alpha`, `snapshot` or `nightly` matches. False positives are possible on unusual version strings.
- Markdown output is truncated: top 25 packages with a non zero score and the first 50 findings. Use JSON or SARIF for the complete set.
- SARIF results all point at `composer.lock` line 1, because the lockfile is machine generated and there is no meaningful line to anchor to.
