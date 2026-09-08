# Embedding Drift Attributor

You swapped the embedding model, reran the indexing job and average recall barely moved, so the migration shipped. Three weeks later one tenant, language or document type is quietly getting worse answers and nobody can say which, because the only number anyone looked at was a single global cosine average.

**Language:** Julia | **Lines:** 830 | **Added:** 2026-06-12

## What this solves

This solves the April 2026 problem where a team swaps an embedding model, changes a vector database reindexing job, or moves RAG search traffic to another provider and then has no clear way to see which customer, language, tenant or document cohort actually moved. The aggregate metric is the trap. A migration that leaves 95 percent of your corpus untouched and rotates 5 percent of it hard shows a tiny mean cosine change, passes every dashboard check and still destroys retrieval quality for the customers inside that 5 percent. You find out from a support ticket that says "search got worse", six weeks after rollout, with no way to bisect because the old index is gone.

The opposite failure is just as common. Someone sees cosine distances that feel large and blocks a rollout that was fine. Embedding spaces have their own natural spread, so a distance of 0.12 means something completely different in a tight cohort of near duplicate product descriptions than in one of long multilingual support threads. Without a baseline for what "far apart" means inside that cohort, you are comparing a raw number against intuition calibrated on some other dataset.

Then there is noise. Rank twenty cohorts by drift and act on the top three and you are usually acting on sampling noise. Testing many cohorts with no multiple comparison correction guarantees false alarms, and false alarms are how a rollout gate first gets ignored and then gets deleted. This file takes paired before and after vectors for the same documents, groups them by cohort and answers one question per cohort: did this group move, is the move large relative to how spread out the group already was and is it distinguishable from chance.

## Why I built it

Built because I keep seeing developers compare only top line recall or one average cosine number, then ship a migration that quietly hurts the long tail. The tooling is either a full observability platform you have to send your vectors to, which is a procurement conversation and a data residency problem, or a notebook cell someone wrote once that computes a mean and a histogram. The notebook has no baseline normalization, no significance testing and no attribution, so it cannot tell you whether to block a release.

I wanted the thing in between. One file, nothing outside the Julia standard library, no network call, reads a TSV and prints a table you can paste into a pull request. Deterministic seed, because the output gates a decision.

## When to use it

- Before repointing production RAG search at a new embedding provider, run last month's queried documents through both models and see which cohorts moved.
- After a vector database reindex where chunking or preprocessing changed, to confirm the reindex was actually a no-op.
- As a CI check on a model registry promotion, with `--json` piped into whatever decides if it ships.
- When one customer reports search quality dropped and you need to know whether their tenant is an outlier or everyone moved.
- When comparing a quantized or distilled model against the full precision original and you need per language breakdowns.
- When a rollout decision needs justifying and "average cosine similarity was 0.97" will not survive the follow up question.

## How it works

Input is tab separated: `id`, `cohort`, `before_vector`, `after_vector` and an optional positive `weight`. `parse_sample_line` skips blanks and `#` comments and drops a header row. `parse_vector` strips optional square brackets, splits on whitespace, commas or semicolons, rejects non finite values and enforces `max_dimension`. `maybe_normalize!` L2 normalizes both vectors unless `--raw-vectors` is passed, and throws on a zero norm vector rather than producing NaN downstream.

Memory is bounded by reservoir sampling. Each cohort gets a `CohortReservoir` and `push_reservoir!` implements classic Algorithm R: fill to `sample_cap`, then for each later row draw a slot in `1:seen` and replace if it lands inside the reservoir. That gives a uniform sample of an arbitrarily long stream at fixed cost, so you can point this at a hundred million row export. Each reservoir carries its own RNG from `seeded_rng`, which mixes the seed with an FNV-1a 64 bit hash of the cohort name from `stable_hash`, so cohort A's sampling never perturbs cohort B's.

`compute_metrics` then computes five things per cohort. `cosine_losses` gives the paired per document loss `1 - cos(before, after)`, reduced to a weighted mean and a p95. `weighted_centroid` on each side gives the centroid shift, separating systematic movement from scattered movement. `sliced_wasserstein` projects both clouds onto unit random Gaussian directions from `random_projection_matrix`, sorts each projection and averages the gap between order statistics: a cheap stand in for real Wasserstein distance that catches shape changes, not just mean shifts. `gaussian_mmd2` computes a Gaussian kernel maximum mean discrepancy with bandwidth from the median heuristic in `estimate_bandwidth2`, on a `kernel_cap` subsample because MMD is quadratic in sample count.

Those numbers are meaningless alone, so `baseline_dispersion` samples random within cohort pairs from the before matrix and takes the median cosine distance. That is the cohort's own scale. `risk_score` divides each metric by a unit derived from that dispersion, clips each ratio at 3x and blends them into a 0 to 100 score: 0.28 p95 loss, 0.22 mean loss, 0.22 centroid shift, 0.16 sliced Wasserstein, 0.12 MMD. The p95 carries the most weight on purpose. The long tail is the thing that breaks and the mean is the thing that hides it.

Significance comes from two resampling loops. `bootstrap_interval` resamples rows with replacement, recomputes the metric stack each round and reports the 2.5 and 97.5 percentiles of the score. `permutation_pvalue` runs a paired exchangeability test: for each row it randomly swaps before and after, recomputes the normalized centroid statistic and counts how often the shuffled one beats the observed, giving `(ge + 1) / (permutations + 1)` so the p value can never be zero. `apply_bh!` then runs Benjamini-Hochberg step-up with the monotonicity fix, controlling false discovery rate across cohorts. `decide` combines both axes: `q <= alpha` plus `risk >= risk_alert` gives `block_index_rollout`, size without significance gives `inspect_before_rollout`, significance without size gives `watch_consistent_shift`, neither gives `pass` and a cohort under `min_pairs` gives `not_enough_pairs` with NaN metrics instead of a fabricated score. `finalize_reports!` closes with `contribution`, each cohort's share of total drift weighted by risk times rows seen. That column is the attribution.

## Usage

```bash
# Markdown table to stdout, the default
julia EmbeddingDriftAttributor.jl --input pairs.tsv --markdown

# JSON for a CI gate, wider reservoir, stricter alert threshold
julia EmbeddingDriftAttributor.jl --input pairs.tsv --json --sample-cap 8000 --risk-alert 20

# Reads stdin when --input is omitted
zcat exports/*.tsv.gz | julia EmbeddingDriftAttributor.jl --json > drift.json

# Fast smoke run, skip resampling entirely
julia EmbeddingDriftAttributor.jl -i pairs.tsv --permutations 0 --bootstraps 0

# Skip L2 normalization when magnitude matters to you
julia EmbeddingDriftAttributor.jl -i pairs.tsv --raw-vectors --seed 7
```

Input rows, tabs between fields:

```
doc-4821	tenant-acme-en	0.12,0.09,-0.44	0.10,0.11,-0.41	2.5
doc-4822	tenant-acme-de	[0.31 -0.02 0.77]	[0.29 -0.05 0.74]
```

As a library:

```julia
include("EmbeddingDriftAttributor.jl")
using .EmbeddingDriftAttributor

config = DriftConfig(sample_cap = 8000, risk_alert = 20.0, alpha = 0.01)
report = open(io -> analyze(io, config), "pairs.tsv", "r")

for cohort in report.cohorts
    cohort.decision == "block_index_rollout" && @warn "drift" cohort.cohort cohort.risk_score
end

print(render_json(report))
```

Full flag list is in `help_text()`: `--input/-i`, `--json`, `--markdown`, `--sample-cap`, `--kernel-cap`, `--bootstrap-cap`, `--projections`, `--permutations`, `--bootstraps`, `--min-pairs`, `--seed`, `--risk-alert`, `--alpha`, `--raw-vectors`, `--help/-h`.

## Notes

- Exits 0 on a successful analysis even when a cohort is marked `block_index_rollout`, and exits 2 on any thrown error with the message on stderr. A CI gate has to read the output and decide for itself.
- The permutation test only tests the normalized centroid shift, not the blended risk score. A cohort that changed shape without moving its centroid shows high risk and a weak p value, which is what `inspect_before_rollout` is for. At the default 199 permutations the smallest p value is 0.005, so raise `--permutations` if you test hundreds of cohorts under a tight alpha.
- Weights affect the weighted mean cosine loss and both centroids only. Sliced Wasserstein, MMD and the p95 ignore them.
- Bootstrapping recomputes the whole metric stack including fresh dispersion and projections on every round, 199 by default, which dominates runtime on wide vectors. Set `--bootstraps 0` and the interval columns print as `nan`.
- Dimensions must match between before and after on every row and stay consistent within a cohort, or `samples_to_matrices` throws. Different cohorts may differ. Results depend on row order once a cohort exceeds `sample_cap`: same file and same seed reproduce exactly, a reshuffled file will not.
- Standard library only: Dates, LinearAlgebra, Printf, Random and Statistics. No packages, no network calls, no telemetry. Your vectors never leave the machine.
