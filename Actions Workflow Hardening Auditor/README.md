# Actions Workflow Hardening Auditor

A dependency free Crystal CLI that audits GitHub Actions workflows and composite actions for script injection, pwn request checkouts, unpinned actions, loose token permissions and the other ways a CI file ends up handing your repository to a stranger. It reads YAML with real line numbers, prints text, JSON or SARIF and exits non zero so it can gate a pull request.

**Language:** Crystal | **Lines:** 1029 | **Added:** 2026-10-10

## What this solves

Most GitHub Actions security incidents in the last few years did not need a zero day. They needed one `run:` step that pasted `${{ github.event.pull_request.title }}` into a shell script, or one `pull_request_target` workflow that checked out the pull request head. The runner expands the expression before the shell ever sees the script, so a pull request titled with a quote and a command is executed as code, with whatever token and secrets that job holds.

The same pattern shows up again and again: third party actions pinned to a tag the owner can move, `permissions` left unset so the token falls back to a repository default, `secrets: inherit` forwarding every secret to a reusable workflow, self hosted runners listening to fork driven events and `actions/checkout` leaving a token in `.git/config` right before an artifact upload zips the workspace.

Actions Workflow Hardening Auditor looks for exactly these problems in a repository's `.github/workflows` directory, in `.github/actions` composite actions and in a root `action.yml`. Every finding has a rule id, a severity, the real line number, the evidence line, a plain explanation and a concrete fix. If you search for a GitHub Actions security scanner, a workflow linter for expression injection, a pull_request_target checker or a SARIF producing audit tool for CI, this is the kind of thing that fits.

## Why I built it

I wanted a checker I could drop into any repository without installing a Python environment, a Node toolchain or a Go module cache. Crystal compiles to one binary and ships a YAML parser and a SHA256 implementation in the standard library, so a scan of a workflows directory is quick. There is nothing to pin, nothing to update and nothing that could itself be a supply chain risk.

The second reason is that many scanners are good at one thing. Some only pin actions. Some only look for injection and treat every `${{ }}` the same, which drowns the real findings in noise. I wanted one pass that understands which contexts an outsider can write to, which triggers run with secrets and how those two facts combine. A pull request title in a plain `pull_request` workflow is bad. The same title in an `issue_comment` workflow that also writes to `$GITHUB_ENV` is critical. The severity should say so.

The third reason is adoption. A tool that reports 400 findings on day one gets turned off on day two. So there is an inline suppression comment, a rule filter, a severity floor and a baseline file, so a team can fail the build on new problems while it works through the old ones.

## When to use it

Use it as a required check on pull requests that touch `.github/**`, as a nightly job across an organisation checkout, or locally before you push a workflow change. Use the SARIF output to get findings into the GitHub code scanning tab, where they appear on the exact line.

It fits best when you maintain several repositories and want one consistent gate, when you review workflow changes from contributors and want a second pair of eyes, or when you are adopting pinned SHAs and least privilege tokens and want a measurable way to track progress.

It does not talk to the network. It cannot tell you whether a pinned SHA is a malicious commit. It does not resolve tags to SHAs for you. It reads files and tells you what is wrong with the way they are written.

## How it works

The program is one file with one module, `ActionsWorkflowHardeningAuditor`.

**YAML with positions.** `YAML.parse` throws away line numbers, so `TreeBuilder` drives `YAML::PullParser` and builds its own `Node` tree. Each `Node` has a kind (scalar, map or sequence), a start line and a flag for block scalars. That flag matters: for a `run: |` block, the line of an expression is the line of the `|` marker plus one plus the number of newlines before the match, so the finding lands on the exact line inside the script. The builder resolves anchors and aliases, merges `<<` keys, caps nesting at `MAX_DEPTH` and charges every alias for the size of what it points at against `MAX_NODES`. A billion laughs style file is rejected instead of eating memory.

**Fail closed.** Any parse error, oversized file, bad UTF-8 or an alias bomb becomes a `WFA000` finding with high severity. An unreadable workflow is a gap in the audit, so it is reported rather than skipped. Inline suppression is ignored for it.

**Expression analysis.** `expressions` finds every `${{ ... }}` in a script. `normalize_expression` rewrites `github['event']['issue']['title']` and `commits[0].message` into dotted paths with a `*` wildcard, then `mask_strings` blanks string literals so words inside quotes are not mistaken for contexts. A regex (`REF_RE`) pulls out context references, and `classify` sorts each one:

- `ATTACKER_PATHS` lists paths an outside party writes to: issue and pull request titles and bodies, head refs and labels, comment and review bodies, commit messages and author names, `workflow_run` branch names, release names, `client_payload` and `github.head_ref`. A `*` matches one path segment, so `github.event.commits.*.message` covers every commit.
- `CALLER_PATHS` covers `inputs.*` and `github.event.inputs.*`, which someone with dispatch rights controls. That is medium.
- Step and job outputs are low, because they carry whatever the producing step computed.
- When an expression passes an object through `toJSON`, `format` or `join`, a path that is a prefix of an attacker path, such as `github.event`, counts as tainted.

`boolean_only?` removes false positives. `${{ contains(github.event.issue.title, 'x') }}` yields true or false, so it cannot carry a payload and is skipped. An expression that mixes a comparison with `&&` or `||` is not treated as boolean, because those operators can return the string operand.

**Severity is contextual.** `Analyzer#check_expressions` raises a finding to critical when the workflow has a trigger from `PRIVILEGED_TRIGGERS` (`pull_request_target`, `workflow_run`, `issue_comment`, `issues`, `discussion`, `discussion_comment`) or when the script writes to `GITHUB_ENV` or `GITHUB_PATH`, since that turns one injection into a persistent one. The fix text names the exact environment variable to use, for example `Set env: PULL_REQUEST_TITLE: ${{ github.event.pull_request.title }}`.

**Rule set.** `RULES` holds eleven rules plus the parse failure rule:

| Id | What it catches |
|---|---|
| WFA001 | Expression injection into `run:` and `actions/github-script` |
| WFA002 | Pull request head checked out under `pull_request_target`, `workflow_run` or `issue_comment`, including `gh pr checkout` in a shell step |
| WFA003 | Unpinned `uses:`: branches (high), mutable tags, short SHAs, docker tags without a digest |
| WFA004 | Missing `permissions`, `write-all`, write scopes under privileged triggers, `id-token: write` |
| WFA005 | `secrets: inherit` and `toJSON(secrets)` |
| WFA006 | Checkout without `persist-credentials: false` in a job that uploads an artifact |
| WFA007 | Self hosted runners on fork driven triggers |
| WFA008 | `::set-env`, `::add-path` and `ACTIONS_ALLOW_UNSECURE_COMMANDS` |
| WFA009 | `curl` or `wget` piped into a shell, and `iex` |
| WFA010 | Jobs without `timeout-minutes` |
| WFA011 | `workflow_run` workflows that download artifacts |

`check_uses` is deliberately graded. Actions owned by `actions` and `github` on a version tag are low. A third party tag is medium. A branch such as `main` is high.

**Output and CI behaviour.** `Finding` carries a fingerprint, the first 32 hex characters of a SHA256 over the rule, file, job, step index and evidence text. It does not include the line number, so a finding keeps its identity when someone adds a comment above it. `sarif_report` emits SARIF 2.1.0 with `security-severity` scores and `partialFingerprints`, so code scanning can track alerts across commits. `run` returns 0 for a clean pass, 1 when something at or above `--fail-on` remains and 2 for usage or file errors.

## Usage

Build it:

```
crystal build --release ActionsWorkflowHardeningAuditor.cr -o actions-workflow-hardening-auditor
```

Audit a repository root, a workflows directory or single files:

```
./actions-workflow-hardening-auditor .
./actions-workflow-hardening-auditor .github/workflows/release.yml
./actions-workflow-hardening-auditor --fail-on critical --min-severity medium .
```

Try it on the bundled example, which trips most rules:

```
./actions-workflow-hardening-auditor SampleVulnerableWorkflow.yml
```

Output formats and rule filters:

```
./actions-workflow-hardening-auditor --format json . > audit.json
./actions-workflow-hardening-auditor --format sarif . > audit.sarif
./actions-workflow-hardening-auditor --only WFA001,WFA002 .
./actions-workflow-hardening-auditor --ignore WFA010 .
./actions-workflow-hardening-auditor --list-rules
```

Adopt it on an existing repository without a wall of red:

```
./actions-workflow-hardening-auditor --write-baseline wfa-baseline.json .
./actions-workflow-hardening-auditor --baseline wfa-baseline.json .
```

New findings fail the build. Baseline entries that no longer match anything are reported on stderr so you can delete them.

Suppress a single finding on the same line or on a comment line directly above it:

```
# wfa-ignore: WFA003
- uses: vendor/internal-action@v3
```

A minimal workflow step to run it:

```
- run: ./actions-workflow-hardening-auditor --format sarif --fail-on high . > wfa.sarif
```

Options: `--format`, `--fail-on` (info, low, medium, high, critical or none), `--min-severity`, `--ignore`, `--only`, `--baseline`, `--write-baseline`, `--max-files`, `--max-bytes`, `--list-rules`, `--version` and `--help`.

## Notes

- Tested with Crystal 1.14 against a vulnerable sample, a clean workflow, a composite action, malformed YAML and an alias bomb. The sample file is meant to fail.
- Everything is heuristic text analysis of the file. It cannot see repository settings such as the default token permission, branch protection or whether a repository is public, so WFA004 and WFA007 describe risk and do not prove exposure.
- `needs.*.outputs` and `steps.*.outputs` are only low severity. They may be safe, and tracing taint across jobs would need a data flow engine.
- Expressions inside `with:` inputs of arbitrary actions are not flagged, since whether they are dangerous depends on what that action does with them. `actions/github-script` is the exception because its `script` input is code.
- A matrix value in `runs-on` is not resolved, so a self hosted runner chosen through a matrix will be missed.
- Directory scans look at `.github/workflows`, `.github/actions` and a root `action.yml`. Pass other files explicitly.
- The fingerprint ignores line numbers on purpose. Two identical lines in the same step share a fingerprint.
- Inline suppression never applies to `WFA000`.
