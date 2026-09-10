# Repository Signal Ranker

A 60 file pull request lands and the reviewer has no idea which three files actually matter. This is a single Ruby file that reads `git diff --numstat` on stdin and ranks the changed files by risk, so the human knows where to start.

**Language:** Ruby | **Lines:** 171 | **Added:** 2026-05-24

## What this solves

This solves the April 2026 review overload problem where agent-written pull requests touch many files, but the human reviewer still needs to know which files deserve real attention first. The failure mode is not that nobody reviews the PR. It is that the review budget gets spent in file order. GitHub shows the diff alphabetically by path, so the reviewer burns twenty minutes on `app/components/` churn and rubber stamps the last thirty files, one of which quietly rewrote token validation or bumped a Terraform state backend.

What breaks is specific. A session handling change slips through because it was file 47 of 61 and the reviewer was tired. A lockfile bump pulls in a transitive dependency nobody looked at. A `deploy.yml` edit changes the environment a job runs against and nobody notices until the job runs. These are not subtle bugs. They are files a human would have read carefully if a human had known to read them. The cost is an incident, a rollback and a postmortem concluding "the change was in the diff", which is true and useless.

The second failure is ownership blindness. A file six teams have claimed in CODEOWNERS is riskier than a file one person owns, because it sits on a boundary and nobody holds full context. Review tooling routes notifications to owners and stops there. Summarizers do not fix that either. Code review bots can summarize everything and still miss the practical question: what should Pavan inspect before approving? A generated summary of a 60 file diff reads as uniformly important, so it is uniformly ignored.

## Why I built it

Existing tooling splits into two useless halves. CODEOWNERS routes review requests but assigns no priority. Linters find defects inside files but have no opinion about which files deserve human eyes. Nothing in between reads a raw diff and says "these eight files carry the risk". Risk scoring products exist, they want a server, a dashboard and a subscription.

So this is one Ruby file with zero gems, stdin to stdout, dropping into any CI job that already has Ruby. Scoring is deterministic and the weights sit in plain sight, so when someone disputes a ranking you can point at the line that produced it. The trick is combining churn, path risk, owner spread and local scoring hints into one auditable number that fits in a pull request comment.

## When to use it

- A CI job posts a review checklist on every large PR and needs an ordered list, not a dump
- An agent opened a big refactor PR and you want the files it touched that were not part of the refactor
- A monorepo where security, infra and model code sit beside ordinary application code, with rotating reviewers
- You want a risk file saying "anything under `billing/` is worth 40 extra points" without patching the ranker
- A release branch has 300 changed files and you have an hour before cutting
- Grading review coverage after an incident: would the offending file have made the top ten?

## How it works

Input is git numstat lines. `Parser.from_numstat` splits each into exactly three cells with `line.split(/\s+/, 3)` and raises `ArgumentError` naming the line number if a line has fewer. `Parser.numeric` maps git's `-` placeholder for binary files to `0`, so a binary blob contributes churn zero rather than killing the run. `Parser.normalize_path` strips the quotes git adds around paths with spaces and drops everything up to `" => "` so a rename resolves to the new path. Each line becomes a `Change`, a keyword init `Struct` of path, added, deleted, owners and signals, whose `churn` method is `added + deleted`.

Ownership comes next. `Owners.load` reads a CODEOWNERS shaped file, skips blanks and `#` comments, and compiles each `pattern owner...` line with `Owners.pattern_to_regexp`. That method escapes the pattern, rewrites `\*\*` to `.*` and `\*` to `[^/]*`, then anchors it with `\A...\z`. So `**` crosses directory separators and a single `*` does not, and matching is full path. `Owners.apply` walks every change, `flat_map`s the rules and uniques the result, so a file claimed by three overlapping rules gets the union of their owners. `SignalFile.apply` is the repo specific escape hatch built on the same regexp compiler: a second file of `pattern weight` lines, `Float()` on the weight, every match appended to the change's `signals`. Weights can be negative, which is how you sink generated or vendored directories.

Scoring lives in `RepositorySignalRanker#score` and is a flat sum, no normalisation and no hidden state. The churn term is `Math.log2(change.churn + 2) * @churn_weight`. Log scaling is the point: a 4000 line file is riskier than a 40 line file but not a hundred times riskier, and without the log one generated file dominates every ranking. The `+ 2` keeps the term positive at zero churn. Then flat path bonuses: 30 for `SECURITY_PATTERNS` (`auth`, `token`, `secret`, `crypto`, `session`, `permission`), 18 for `AI_PATTERNS` (`prompt`, `eval`, `model`, `tool`, `agent`, `embedding`), 16 for `INFRA_PATTERNS` (`docker`, `kube`, `terraform`, `helm`, `workflow`, `deploy`), 12 for a path ending in `.lock`, `.yaml`, `.yml` or `.tf`, and 8 for a path with two or fewer segments, on the heuristic that root level config sits closer to the blast radius than a leaf component. Last, `owners.length * @owner_weight` for fan out plus the sum of `signals`, rounded to two decimals.

`rank` pairs each change with its score, sorts by `[-score, change.path]` and truncates to `@max_files`. Negating the score and appending the path breaks ties alphabetically instead of by input order, so the same diff always produces byte identical output. That matters when the result lands in a PR comment that gets diffed across pushes.

`Cli.parse` is a hand rolled argv loop, not OptionParser, and it raises on any unknown flag rather than ignoring it. Output is a tab separated table or, with `--json`, a `JSON.pretty_generate` of score, churn, owners and path, with `json` required lazily so the table path stays dependency free. The whole run sits inside a `rescue StandardError` that writes `RepositorySignalRanker: <message>` to stderr and exits 64, the sysexits `EX_USAGE` code.

## Usage

```bash
# Rank the files changed on this branch
git diff --numstat origin/main...HEAD | ruby RepositorySignalRanker.rb

# Top 15, as JSON, with ownership and repo specific weights
git diff --numstat origin/main...HEAD \
  | ruby RepositorySignalRanker.rb \
      --max-files 15 \
      --owners .github/CODEOWNERS \
      --signals .github/review-signals.txt \
      --json

# Turn churn down and owner fan out up
git diff --numstat HEAD~5..HEAD | ruby RepositorySignalRanker.rb --churn-weight 3.0 --owner-weight 12.0
```

Flags, with defaults: `--max-files 25`, `--owner-weight 6.0`, `--churn-weight 7.0`, `--owners <path>`, `--signals <path>`, `--json`.

An owners file line looks like `src/api/** @security-team @platform`. A signals file line looks like `billing/** 40` or `vendor/** -25`. Both accept `#` comments and blank lines.

## Notes

- Path anchoring is strict. Every pattern is wrapped in `\A...\z`, so `src/**` works and `src/` matches nothing. That is not GitHub CODEOWNERS semantics, which treats a trailing slash as a prefix. Write patterns for this ranker.
- The table header on line 137 is in single quotes, so it prints the literal characters `score\tchurn\towners\tpath` while the data rows below it really are tab separated. The rows are correct, the header is not. Use `--json` if you are parsing output.
- `Hash#slice` is reopened at the bottom of the file and redefined with `fetch`, so it raises `KeyError` on a missing key instead of skipping it. That redefinition is global to the process, worth knowing before you require this file into a larger program.
- Rename handling is partial. `old.rb => new.rb` resolves cleanly, git's brace form `src/{old => new}/f.rb` leaves a stray `}` in the path. Pass `--no-renames` if that matters.
- Any error, a bad numeric cell, an unknown flag, a missing flag argument, exits 64 with a one line stderr message and no partial output.
- It ranks files, it does not read them. No content analysis, no AST, no hunk inspection. A one line change to `auth.rb` and a rewrite of `auth.rb` differ only by the churn term. Missing owners and signals files are silently ignored, so a typo in `--owners` yields a plausible ranking with every owner count at zero.
