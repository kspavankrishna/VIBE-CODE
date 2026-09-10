# Policy Drift Incident Router

A pull request that touches `policy/admission/tenant.rego`, `.github/workflows/deploy.yml` or `db/migrations/` looks exactly like one that touches a README. This Bash script reads the changed-file list, matches it against a rule catalog and names the owner, control and runbook that must look at the change before it merges.

**Language:** Bash | **Lines:** 631 | **Added:** 2026-08-10

## What this solves

This solves the April 2026 problem where a normal pull request can quietly change the rules that protect production: OPA policies, GitHub Actions workflows, Terraform files, Kubernetes manifests, MCP tool contracts, AI agent runtime folders, eval harnesses, edge workers, IoT control paths, database migrations, lockfiles and data pipelines. All of those live in the same repo as application code. All of them arrive through the same review UI. A reviewer scrolling a 40 file diff at 6pm sees green tests and clicks approve.

The failure mode is not that the change was wrong. It is that the wrong person reviewed it. A Rego edit that widens an admission rule is a security change reviewed by a backend engineer. A Terraform edit that flips a region or drops a deletion protection flag is an infra change reviewed by whoever happened to be on the PR. A workflow edit that adds a step with a broad token is a supply chain change reviewed by nobody in particular. Nothing fails in CI, because CI is testing the app, not the guardrails.

The cost sits in the gap between merge and detection. Kubernetes identity drift shows up as a permission error in a service nobody owns. A migration without rollback thinking shows up during the rollback. A lockfile bump shows up as a supply chain finding weeks later. An MCP tool schema change shows up when a coding agent calls something it should not have been able to call. The on-call engineer starts from zero every time, because the routing information that would have named the owner was never attached to the change.

The other half is tooling weight. The systems that could catch this, policy engines, admission controllers, cloud security platforms, all run after merge or after deploy. What you want is a check that runs in the first ten seconds of CI, on a text list of paths, with no network, no daemon and no vendor account.

## Why I built it

Built because Pavan wanted a small Bash file that can run anywhere in CI before the expensive systems wake up, read only a changed-file list, and tell the team which owner, runbook and control should look at the drift before merge. CODEOWNERS gets close but it only assigns reviewers. It carries no severity, no control id, no runbook link and no exit code you can gate on. Policy engines carry all of that but need a bundle, a server or a SaaS tenant.

The middle is what this is: built-in rules that are useful on day one, an optional catalog you override per repo, four output formats and a deterministic exit code. One file, no dependencies beyond a shell, so it behaves the same in GitHub Actions, Buildkite, Jenkins, GitLab CI and a local pre-push hook.

## When to use it

- A monorepo where infra, policy and application code share one review queue and one CODEOWNERS file.
- CI on an infrastructure repo, where `--fail-on critical` should hard stop a PR touching `secrets/**` or `**/.env*`.
- An AI tooling repo where `mcp/**`, `agents/**`, `prompts/**` and `evals/**` need different reviewers than the app.
- A pre-push hook piping `git diff --name-only origin/main...HEAD` so you find out before the PR exists.
- Code scanning ingestion, where the SARIF output puts drift routes next to your other findings.
- Release trains where a migration or data pipeline change should get incident-style handling, not a normal approval.

## How it works

`load_changes` reads paths from a file or stdin, and every line goes through `read_changes_line`, which understands the formats git actually emits. Tab-separated `git diff --name-status` rows are split on the tab, with a rename or copy row (`R*`, `C*`) taking the last field so you get the destination path. Space-separated porcelain rows like `M path` or `?? path` lose their status prefix. Everything else is a bare path. `normalize_path` then strips carriage returns, surrounding quotes, a trailing comma, a leading `./` or `/`, and collapses duplicate slashes. `add_change` dedupes by linear scan.

Rules come from `load_builtin_rules` or from a file. The built-in catalog is 33 `add_rule` calls covering CI workflows and composite actions, Dockerfile and Containerfile, Rego and policy bundles, `infra/**`, Terraform, Kubernetes and Helm, MCP contracts, agent runtimes, prompts and evals, edge workers, IoT, data pipelines, migrations, seven kinds of lockfile and manifest, `secrets/**` and `**/.env*`. Each rule carries pattern, severity, owner, control id, runbook URL and a note. `load_rule_line` splits on tab if the row has one, otherwise on pipe. A row missing pattern, severity, owner or control, or carrying an unknown severity, is rejected by `add_rule`: normally that becomes a warning in the report, and under `--strict-rules` it is fatal.

Matching is `pattern_matches`, a small glob dialect rather than a regex engine. A `dir/**` pattern matches the directory and everything under it. A `**/name` pattern matches at the root or at any depth. A pattern with `**` in the middle is split into prefix and suffix and checked at both ends. Anything else falls through to an unquoted bash pattern comparison, which is why `requirements*.txt` works as written. No regex means a catalog stays readable and cannot blow up on a pathological input.

`route_changes` is a nested loop over paths and rules, and every match emits a route row, so one path can route to several owners at once. That is deliberate: a `.tf` file under `infra/` should reach both the infra rule and the Terraform rule. A path matching nothing gets a synthetic `info` row with control `unmatched_change` and the `--default-owner`, unless `--quiet-unmatched` is set. Severity is stored as a name and as an integer rank from `severity_rank`, ordering info, low, medium, high and critical as 0 to 4, with a sentinel 99 for `never`. `--min-severity` drops rows below its rank inside `add_route`.

The gate is `max_rank` compared against `--fail-on` in `gate_status`, and `finish_with_gate` returns 2 when max severity meets the threshold. Rendering is four independent functions over that state: `render_markdown` writes a summary block, warnings and a seven column table with `md_escape` on every cell; `render_json` emits a routes array through `json_escape`; `render_sarif` emits SARIF 2.1.0 with the control id as `ruleId` and critical or high mapped to `error` by `sarif_level`; `render_gha` emits `::error`, `::warning` and `::notice` workflow commands with `gha_escape` handling percent and newline encoding. `--self-test` writes a fixture changes file and a two rule TSV into a `mktemp -d` directory, re-invokes the script twice against itself, greps the JSON for expected routes and asserts that `--fail-on high` exits 2.

## Usage

```bash
# Straight from git, built-in rules, markdown report
git diff --name-only origin/main...HEAD | bash PolicyDriftIncidentRouter.sh

# Name-status input is understood too, including renames
git diff --name-status origin/main...HEAD > changed.txt
bash PolicyDriftIncidentRouter.sh --changes changed.txt

# Own catalog, JSON out, hard stop on high or critical, malformed rows fatal
bash PolicyDriftIncidentRouter.sh \
  --changes changed.txt --rules .ci/drift-rules.tsv \
  --format json --fail-on high --strict-rules

# GitHub Actions annotations, medium and above, no unmatched noise
git diff --name-only origin/main...HEAD | bash PolicyDriftIncidentRouter.sh \
  --format gha --min-severity medium --quiet-unmatched --default-owner platform

# SARIF for code scanning upload, report only
bash PolicyDriftIncidentRouter.sh --changes changed.txt --format sarif --fail-on never > drift.sarif

# Verify behavior before trusting it in CI
bash PolicyDriftIncidentRouter.sh --self-test
```

Rule catalog rows, tab preferred, pipe accepted:

```
policy/**	critical	policy-security	opa_policy	https://runbooks/policy	Replay before merge.
pipelines/**|high|data-platform|data_pipeline|https://runbooks/data|Data contract drift.
```

## Notes

- Exit codes: 0 when the gate passes, 2 when max severity meets `--fail-on`, 1 for any fatal condition (bad flag, missing changes or rules file, empty changed list, zero valid rules, empty `--default-owner`).
- It routes, it does not read diffs. The script never opens the changed files, so it cannot tell a comment-only Terraform edit from one that deletes a resource. Path is the only signal.
- `--rules` replaces the built-in catalog, it does not extend it. Want both, copy the built-ins into your file.
- Unmatched paths are `info`, so `--fail-on info` fails on essentially every run. Pair it with `--quiet-unmatched` if that is really what you want.
- Matching is O(paths × rules) with a quadratic dedupe on input paths. Fine for a normal PR, noticeable on a ten thousand file change.
- Needs Bash 4 or newer for arrays and `${var//}` expansions, plus `tr` and `date`, and for `--self-test` also `mktemp` and `grep`. No network, no jq, no Python.
- Built-in runbook URLs are `https://runbooks.local/...` placeholders. Replace them before anyone follows one during an incident.
- Severity, owner and control come from the catalog and nothing else. No scoring, no history, no learning. Same input, same output.
