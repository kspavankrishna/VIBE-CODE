# Workflow Failure Fingerprint

A CI job fails, dumps ten thousand lines of log, and nobody can tell whether it is a real code bug or a flaky runner. This TypeScript file reads raw CI logs, redacts secrets, normalizes the unstable parts, groups the remaining failure lines into stable SHA-256 fingerprints and attaches a rerun decision to each one.

**Language:** TypeScript | **Lines:** 916 | **Added:** 2026-05-30

## What this solves

The first failure mode is wasted compute. A GitHub Actions matrix goes red. The log is 12,000 lines of npm chatter, ANSI color codes, timestamps, runner IDs and group markers, and somewhere inside it is one `TS2345` or one `ECONNRESET`. The default human response is to click Re-run all jobs. If the failure was a TypeScript error that rerun burns another twenty minutes and comes back red in the same place. If it was a rate limited registry call the rerun works and nobody records that it happened, so the same transient failure costs the next person twenty minutes next week.

The second is that nobody can tell whether two red builds are the same red build. Branch A fails, branch B fails, the reviewer eyeballs both logs, decides they look similar and merges. There is no identifier to compare because a raw log line carries a timestamp, a run ID, a temp path and a duration, so byte comparison always says different and human comparison always says probably the same. The third is routing: a `relation "users" does not exist` line and a `cosign attestation failed` line land in the same alert with the same red X, but one belongs to whoever owns migrations and one belongs to security.

This file turns all three into data. Every matched failure line becomes a `LogIssue` with a category, a severity from 1 to 10, a confidence score, a rerun action and a 24 character hex signature. Issues with identical signatures collapse into a `FailureCluster`. The same root cause on two runners, at two timestamps, in two temp directories produces the same fingerprint.

## Why I built it

CI log parsers already exist but they are either annotation scrapers that only read `::error::` lines, or vendor dashboards you have to ship your logs to. Neither helps when the failure is a Docker BuildKit solve error buried in raw stdout or a Zod validation failure from an AI tool call, and neither gives you a stable identity you can diff across branches. Log clustering libraries give you clusters but no opinion on whether a cluster is worth rerunning.

I wanted one file, no dependencies outside Node builtins, that I could drop next to a workflow, pipe a log through and get back JSON worth committing next to the run. The rerun decision mattered most. Clustering alone still leaves the human guessing.

## When to use it

- A matrix goes red across six runners and you want to know if it is one failure or six.
- An AI coding agent opened a pull request, CI failed and the log is mostly agent generated noise.
- You are deciding whether to hit Re-run failed jobs or fix the code first, and you want that call made from the log rather than a hunch.
- Two branches fail and you need to prove they fail for the same reason before merging either.
- You want to track flaky signatures over time by saving the fingerprint JSON next to each run and counting repeats.
- A failure needs routing and you want the category and path hints to do it instead of a human triager.

## How it works

The entry point is the `WorkflowFailureFingerprinter` class, or the `analyzeWorkflowFailures` helper that wraps it. You give it an array of `SourceInput` objects, each a name and the raw log text. Options pass through `clampInteger`, so a nonsense `--max-clusters=9999` clamps to 200 instead of erroring.

Each line goes through `normalizeLine` first, and order matters. ANSI escapes are stripped, then `redactSecrets` runs six patterns covering Bearer tokens, GitHub `ghp_`/`github_pat_` tokens, OpenAI `sk-` keys, AWS `AKIA`/`ASIA` keys, any `*_TOKEN=`/`*_SECRET=`/`*_PASSWORD=` assignment and full PEM private key blocks. Redaction happens before hashing, which is the point: a leaked token never reaches the SHA-256 input and never appears in the emitted evidence. After that the GitHub annotation prefix, log prefixes like `##[error]`, URL query strings, ISO timestamps, UUIDs, hex SHAs, durations, port numbers and memory addresses each become a placeholder, and whitespace collapses.

Matching is rule based, not statistical. `DEFAULT_FAILURE_RULES` holds 15 rules covering typecheck, lint, test assertions, snapshot drift, dependency resolution, network, auth, resource pressure, container build, database schema, secret scanning, timeout, edge runtime, AI contract failures and supply chain policy. `matchLine` tests each rule's regexes against a haystack built from both the normalized line and the redacted raw line, so a rule still fires on something normalization would have flattened. When several rules match, the list sorts by `severity * confidence` descending and the top rule wins: a secret scanning hit at severity 10 and confidence 0.95 beats a generic test failure on the same line.

Before a match becomes an issue, `isLowSignal` drops the useless summary lines every CI system prints: `Process completed with exit code 1`, `make: *** error 2` and friends. They are counted in `suppressedLowSignal` rather than discarded silently, so an empty report still tells you the log was all summary and no detail. Surviving matches get up to `maxContextLines` lines on each side plus path hints from `PATH_HINT_PATTERN`. Then `signatureFor` builds the hash input from the rule category, the rule id, extracted error codes (`TS\d{4}`, `CVE-\d{4}-\d+`, `SQLSTATE[...]`, yarn `YN\d{4}`), up to six path hints shortened to their last four segments by `stablePath` and up to ten context lines passing `isSignalLine`, each run through `normalizeForSignature` which strips line and column numbers, every remaining number and any `runner-4` or `attempt 2` style runtime ID. That string is SHA-256 hashed and truncated to 24 hex characters.

Clustering is exact grouping on that signature, a `Map<string, LogIssue[]>`, with no fuzzy distance step. All the tolerance lives in normalization, which keeps clustering deterministic and cheap. A cluster takes the maximum severity of its members, the mean confidence, the union of labels, sources and rule ids, and the earliest line number. `combineRerunAdvice` picks the most conservative action using a fixed rank where `do_not_rerun` outranks `rerun_after_fix`, then `investigate_environment`, then `rerun_with_more_resources`, then `rerun_once`, so mixed evidence never downgrades to a blind retry. `inferOwnerHints` runs seven regexes over path hints and labels to suggest dependency, platform, data, edge, test or security ownership. Clusters sort by severity, then count, then confidence, and truncate to `maxClusters`. Output is `JSON.stringify` of the `WorkflowFailureReport`, or the markdown table from `toMarkdown`.

## Usage

```bash
# Markdown report from a saved Actions log
tsx WorkflowFailureFingerprint.ts --markdown github-actions.log

# JSON from stdin, with a name so the matrix leg is identifiable
cat build.log | tsx WorkflowFailureFingerprint.ts --json --stdin-name=linux-node22

# Several logs with stable source names
tsx WorkflowFailureFingerprint.ts --source=unit:unit.log --source=e2e:e2e.log

# Only high severity findings, tighter context, fewer clusters
tsx WorkflowFailureFingerprint.ts --min-severity=7 --max-context=2 --max-clusters=10 ci.log
```

Full flag list: `--json` (default), `--markdown` / `--md`, `--min-severity=N`, `--max-context=N`, `--max-clusters=N`, `--stdin-name=NAME`, `--source=NAME:PATH`, `--help` / `-h`. Bare arguments are treated as file paths. With no files and no named sources it reads stdin.

As a library:

```ts
import {
  analyzeWorkflowFailures,
  reportToMarkdown,
  WorkflowFailureFingerprinter,
  DEFAULT_FAILURE_RULES,
} from './WorkflowFailureFingerprint';

const report = analyzeWorkflowFailures([
  { name: 'unit', text: unitLog },
  { name: 'e2e', text: e2eLog },
]);
console.log(report.clusters[0].fingerprint, report.clusters[0].rerun);
console.log(reportToMarkdown(report));

// Add your own rules on top of the defaults
const custom = new WorkflowFailureFingerprinter({
  rules: [...DEFAULT_FAILURE_RULES, myTerraformRule],
  minSeverity: 6,
  now: () => new Date('2026-05-30T00:00:00Z'),
});
```

## Notes

- Zero external dependencies. It imports only `node:crypto`, `node:fs` and `node:path`. Run it with `tsx`, or compile it and run the `.js`.
- The CLI only activates when `process.argv[1]` basenames to `WorkflowFailureFingerprint.ts` or `WorkflowFailureFingerprint.js`, so importing it as a module never triggers argument parsing. Renaming the file disables the CLI.
- Exit code is 0 on success and 1 only when an exception is thrown, for example a missing input file. Finding critical failures does not exit nonzero. Gate on the JSON yourself if you want that.
- Everything is read into memory. `readFileSync(0, 'utf8')` slurps all of stdin and each file is read whole. Fine for CI logs, not a streaming parser for gigabyte archives.
- Coverage is exactly the 15 regex rules plus whatever you pass in `rules`. An unrecognized toolchain produces no clusters, not a wrong cluster. The `unknown` category exists in the type union but no default rule emits it. Redaction is likewise pattern matching, so do not treat output as certified safe to publish without a look.
- Clustering is exact on the signature hash. If normalization misses an unstable token, say a temp directory name that is not a UUID or SHA, one root cause splits across two fingerprints. Path hints keep only the last four segments, so identical tail paths in different monorepo packages collapse into one hint.
