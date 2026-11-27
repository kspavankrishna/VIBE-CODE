# Merge Queue Bisect Planner

When a merge queue batch fails, this tells you which PR to blame, which ones are safe to land right now and exactly which CI runs to start next so you find the culprit in the fewest runs. It is a Bayesian group testing planner for GitHub merge queue, GitLab merge trains, Bors and Mergify style batching, written in PureScript.

**Language:** PureScript | **Lines:** 946 | **Added:** 2026-11-27

## What this solves

Batching is the reason merge queues scale. Instead of testing every pull request alone on top of main, the queue groups several PRs and runs CI once for the whole group. When the group passes, everyone lands. When it fails, somebody has to work out which PR broke it. That is where most queues waste their CI budget and their engineers' afternoons.

The usual answer is bisection. Bors splits the batch in half and retries each half. GitHub merge queue removes the group and retests smaller groups in queue order. Both treat every PR as equally suspicious, both treat a red run as the truth and both ignore three things every team running a queue at scale has seen:

1. A failed run is sometimes a flake. Blind bisection will happily eject an innocent PR because a test timed out.
2. Some PRs are much riskier than others. A one line docs change and a 2,000 line refactor of the auth layer should not get the same share of blame.
3. Sometimes nothing in the batch is broken on its own. Two PRs pass alone and fail together (a semantic merge conflict). Or main itself is red. Or the CI runner image changed under you.

Merge Queue Bisect Planner models all three. You describe the batch in a small text file: the PRs in queue order, a prior chance that each one is broken, your flake rate and the CI results you have so far. It replays every result through Bayes' rule, reports the posterior blame per PR, decides which PRs can merge now within your risk budget, ejects the ones that are almost certainly broken and plans the next round of parallel CI runs by maximising expected information gain. You run those, append the results and run it again. In the example below the uncertainty drops from 3.77 bits to 0.31 bits in two rounds.

## Why I built it

I kept seeing the same pattern on busy repositories. A batch of six fails, the queue bisects, one half fails because of a flaky integration test, the other half also fails for the same reason. Three good PRs get kicked out with a red X. Their authors rebase, requeue and wait another hour. Meanwhile the PR that actually broke the build, the risky one everybody suspected from the diff size, was in the half that got unlucky and passed on a retry.

The fix is not a smarter heuristic. It is the right model. Finding defective items with pooled tests is a classic problem called group testing. The noisy version with priors has a clean Bayesian answer. What was missing was a tool that speaks merge queue: queue order, a merge risk budget, ejection thresholds, a CI run budget and parallel runners. So I wrote one. I wrote it in PureScript because the core is pure probability over immutable states and the type checker keeps the bit twiddling honest.

## When to use it

Use it when:

- You run a merge queue or merge train with batches of 2 to 16 PRs and batch failures cost real runner time.
- Your flake rate is not zero. If you have ever retried a red job and watched it go green, it is not zero.
- You can give some PRs a higher prior than others: lines changed, files touched, whether it edits build config, the author's recent revert rate, a risk score from your own tooling.
- You want a written, reproducible reason for every ejection instead of "the bisect said so".

It also works as a library inside a bot. The `evaluate` function in `MergeQueueBisectPlanner.purs` takes a parsed `QueueState` and returns a `Report` with the posterior, the decision and the plan, so a queue controller can call it directly and drive the runs itself.

Do not use it for batches larger than 16. Exact inference enumerates every subset of the batch. Past that size the right move is smaller batches, not a cleverer approximation. The parser refuses bigger batches with that advice.

## How it works

The code is split into four PureScript modules plus a small JavaScript FFI file.

**QueueModel.purs** holds the hypothesis space and the Bayes update. A hypothesis is a `Hyp`:

- `Bad mask` means exactly the PRs in that bitmask are broken on their own. `Bad 0` means every PR is clean and every red run so far was a flake.
- `Conflict i j` means every PR is fine alone but PRs `i` and `j` together break the build.
- `Env` means the base branch or the CI environment is broken, so every run fails no matter what is in it.

`priorPosterior` builds all of them. Subset hypotheses get the product of the per PR priors, scaled by whatever probability is left after the `env` and `conflict` settings. The conflict prior is spread evenly across all pairs. That is 2^n plus n(n-1)/2 plus 1 states, which is 65,657 for a full batch of 16 (`maxBatch`). Zero weight states are dropped up front, so a PR with `prior=0` halves the work.

A CI run is a `Test`, a bitmask of PRs applied on top of the base branch. Mask 0 is the base branch alone. The function `hits` answers whether a hypothesis makes a run fail for a real reason. `failProbability` turns that into a likelihood: `1 - miss` if it hits, `flake` if it does not. `observe` multiplies every state by the likelihood of the result you saw and `normalize` rescales. If every state goes to zero, `normalize` returns `Nothing` and the tool stops with exit code 3, naming the line of the result that broke the model. That only happens when you set `flake` and `miss` to 0 and then feed it a contradiction. That is exactly when you want to be told.

The same `hits` predicate also answers "would merging this set expose a problem", so `cleanProbability` (the chance a set of PRs is genuinely green) falls out for free. `marginalBad`, `conflictInvolvement` and `envMass` give the per PR blame numbers in the report and `entropyBits` gives the remaining uncertainty in bits.

**QueuePlanner.purs** turns the posterior into actions. `decide` ejects any PR whose `marginalBad` reaches the `eject` threshold. If a conflict pair reaches the threshold, it ejects the later PR of the pair and records the partner, because the earlier one is fine on its own. Then it grows a merge set in queue order, adding one PR at a time while `cleanProbability` of the whole set stays at or above `1 - risk`. It stops at the first PR that is neither ejected nor safe. That last rule matters: a queue cannot land PR 5 before PR 4, so the planner never pretends it can. If `envMass` crosses the eject threshold, `decide` sets `halt` and merges nothing, since blaming PRs while main is red is how innocent PRs get ejected.

`candidateTests` proposes runs worth scoring. Scoring all 2^n subsets for every runner would be slow, so it offers the shapes that matter: each suspect alone, queue order prefixes, prefixes ranked by suspicion, the two halves that Bors style bisection would try, the three most likely conflict pairs and the base branch alone while an environment failure is still plausible. Every shape is also offered stacked on top of the PRs already safe to merge, since that is how the queue will test them later. PRs already ejected or cleared are left out of the suspect list.

`planRound` picks up to `runners` parallel runs greedily. For each candidate it computes `jointInformation`, the mutual information between the outcomes of the whole round and the hypothesis: entropy of the joint outcome minus the expected entropy given the hypothesis. The trick that keeps this fast is `bucket`. Two hypotheses that would make the same set of planned runs fail are indistinguishable this round, so the posterior collapses into 2^k buckets keyed by hit pattern, built in one pass with a mutable `STArray`. The joint outcome table is then only 4^k entries, which is why `maxRunners` is 6. Because the runs are independent given the hypothesis, this information is submodular, so the greedy round is within a factor of 1 - 1/e of the best possible round. A candidate that adds less than `minGainBits` (0.01 bits) is not worth a runner and the round stops early. Ties go to the run with fewer PRs.

**QueueInput.purs** parses the state file with line numbered errors for every mistake: unknown directives, duplicate PR ids, reserved ids (`base` and `all`), unknown ids in `observe` lines, probabilities out of range, `risk` of 0 (no finite testing proves a batch green) and `eject` at or below 0.5. `parseProbability` is strict, so `0.1abc` is rejected instead of silently read as 0.1.

**MergeQueueBisectPlanner.purs** is the CLI. `parseArgs` reads the flags, `evaluate` replays the observations and builds the report. Then `renderText` or `renderJson` prints it.

## Usage

You need PureScript 0.15 and the library sources for prelude, effect, exceptions, arrays, strings, integers, numbers, maybe, either, foldable-traversable and st. With spago, list those as dependencies and put the `.purs` and `.js` files from this folder in `src`. Without spago, compile straight with `purs`:

```bash
purs compile 'path/to/package/sources/*/src/**/*.purs' *.purs -o output
node RunMergeQueueBisectPlanner.mjs [--format text|json] [--runners N] <state file | ->
```

`--format` picks text (the default) or JSON. `--runners` overrides the `runners` setting in the file for this run, from 1 to 6. Pass `-` to read the state from stdin. Exit code 0 means a report was printed, 2 means a usage or input error and 3 means the observations contradict the model.

A state file looks like this (it is `Examples/Batch.queue`):

```text
flake 0.03
miss 0.0
env 0.01
conflict 0.02
risk 0.02
eject 0.95
runners 3
budget 12
prior 0.08

pr 4182 prior=0.04
pr 4187 prior=0.15
pr 4190
pr 4191 prior=0.30
pr 4195 prior=0.05
pr 4199

observe all fail
```

The directives are `pr <id> [prior=<p>]` in queue order, `prior` (the default for PRs without one, 0.1 if unset), `flake` (chance a clean run fails, default 0.02), `miss` (chance a broken run passes anyway, default 0), `env` (prior that main or CI is broken, default 0.01), `conflict` (prior that some pair conflicts, default 0.02), `risk` (the most merge risk you accept, default 0.02), `eject` (the blame needed to eject, default 0.95), `runners` (parallel runs per round, default 2) and `budget` (total CI runs for this batch, default 12). Results go in as `observe <targets> pass|fail`, where targets is `all`, `base` or a comma separated list of PR ids.

Run it on the example:

```bash
node RunMergeQueueBisectPlanner.mjs Examples/Batch.queue
```

After the single red run, 4191 carries the most blame at 0.512 because it had the highest prior, 4187 sits at 0.256 and nothing can merge yet. The planner asks for three parallel runs: `4191` alone, `4182,4187,4190` together and `4199` alone, worth about 2.15 bits between them.

`Examples/BatchRound2.queue` is the same file with those three results appended: 4191 passed, the group of three failed and 4199 passed. Run it and 4191 and 4199 drop to roughly zero blame, 4187 rises to 0.581 and the next round asks for `4182,4190`, `4182,4187` and `4187,4190`. That design separates the three remaining suspects in one round instead of three. If 4187 then fails alone and `4182,4190` passes, 4187 is ejected at 0.998 and 4182, 4190 and 4191 merge.

In a CI job, use the JSON output. The `decision` object holds `merge`, `eject`, `pending`, `halt` and `resolved`. Every entry in `nextRound` carries an `observe` string you can append to the state file once that run finishes.

## Notes

- The flake model assumes flaky failures are independent between runs. Real flakes cluster when a shared dependency is down. That is what the `env` hypothesis catches. The planner will ask for a `base` run when it is still plausible.
- Priors are yours to set. Lines changed and touched paths are a decent start. Even rough priors beat equal blame. Wrong priors only cost extra runs because the evidence overrides them.
- Conflicts are modelled for pairs only. A failure that needs three specific PRs together shows up as unexplained mass spread across pairs. The planner can keep asking for runs until the budget runs out. If that happens, split the batch.
- When the budget is spent with PRs still pending, the report tells you to eject them and requeue each one alone. That is the honest fallback and it beats guessing.
- Everything is deterministic. The same file gives the same plan, so you can commit state files next to queue logs and audit every ejection later.
