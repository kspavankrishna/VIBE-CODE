# Experiment Split Provenance

A dataset split manifest can look perfectly clean while the same record, the same content hash or the same user group sits in both train and test. This is a base R script that reads that manifest and proves whether the split boundary actually held.

**Language:** R | **Lines:** 213 | **Added:** 2026-07-26

## What this solves

The failure is quiet, which is what makes it expensive. You dedupe a corpus, backfill a few months of rows, rechunk documents for a retrieval index or refresh a benchmark, and somewhere in that pipeline a record ends up on both sides of the split. Nothing crashes. The training job runs. Evaluation returns a number that is two or three points better than it should be, and that number goes into a slide, a model card or a release approval. Nobody notices until the model hits production and the offline score stops predicting anything.

Group leakage is the version people miss most. Row ids can be perfectly disjoint across splits while every row still belongs to the same twenty users, the same source documents or the same customer accounts. Chunked RAG corpora make this routine: one PDF becomes forty chunks with forty unique ids, and a random split scatters them across train and test. The retriever then looks brilliant because it has already seen the neighbouring chunk of the exact passage it is being asked to find. Same story for temporal leakage, where an evaluation set that is supposed to sit after a training cutoff quietly contains rows from before it, so the model is scored on a period it was trained on.

The cost lands on whoever has to explain the gap between the eval dashboard and reality: a retrain, a rerun of the whole benchmark and a credibility hit with whoever signed off. Worse, the leak gets reintroduced weeks later by the same pipeline step, because nothing in the stack asserts that a split is disjoint. Experiment trackers record the split name. They do not verify it.

This script turns that assertion into a build step. It reads the manifest your pipeline already produces, checks several independent notions of identity, and writes both a plain findings CSV for a human and a SARIF 2.1.0 report for GitHub code scanning or CI annotations. Set `--fail-on error` and a leak stops the build instead of shipping.

## Why I built it

Experiment tracking tools log the split a row was assigned to. They do not prove the assignment was respected after the next dedup pass, backfill or benchmark refresh. Most teams end up with a one off pandas notebook that checks `set(train.id) & set(test.id)`, which catches the easiest case and misses group leakage and temporal leakage entirely. That notebook never makes it into CI.

The base R constraint was deliberate. Plenty of research and regulated environments will not let you pip install anything into the box that holds the data, but R is already there for the statistics side. This file has zero package dependencies, reads CSV or TSV, keeps source row numbers so a finding points back at a real line, and sends dataset contents nowhere.

## When to use it

- A retrieval or RAG evaluation set was built by chunking documents, and you need to prove no two chunks of one document straddle train and test.
- You just merged a backfill or a dedup pass into a training corpus and want to confirm the old split boundaries survived it.
- A benchmark refresh added rows and you need contamination evidence before publishing a score.
- Evaluation data must sit strictly after a model training cutoff date, and you want that enforced rather than assumed.
- A fine tuning or regression suite is about to be approved for release and governance wants a reviewable leakage artifact, not a verbal assurance.
- You want a leakage gate in CI that annotates the pull request through GitHub code scanning.

## How it works

The entry point is `main()`, wrapped in a top level `tryCatch` that prints any error and exits 64. `parse_args()` walks the argument vector in strict key value pairs against a fixed `key_map`, so an unknown flag or a flag with no value is a hard stop rather than a silent default. `infer_delimiter()` reads only the first line and compares tab counts against comma counts to pick a separator, which `--delimiter csv|tsv` overrides.

`read_manifest()` loads the file with `read.table` using `fill = FALSE` and `comment.char = ""`, so ragged rows fail loudly and a `#` inside data is not treated as a comment. Column names are lowercased and trimmed. The manifest must have a `split` column plus at least one of `id`, `content_hash` or `group`, and split values are lowercased, trimmed and rejected if blank. It then stamps `row_number` as the row index plus one, so every finding cites the physical line in the source file including the header offset.

Detection is a bucketing pass, not a pairwise comparison. `cross_split_findings()` calls `split(which(valid), values[valid])` to group row positions by identity value in one hash pass, which is linear in row count rather than quadratic. Any bucket whose rows touch two or more distinct splits becomes a finding. Severity depends on whether the training split, set with `--train-split` and defaulting to `train`, is one of them: training data on both sides is an `error`, anything else a `warning`. The same routine runs three times over three independent identities, emitting `EPS001` for record id, `EPS002` for content hash and `EPS003` for group. Checking three is the point. A row id can be unique while the content hash is a duplicate, and both can be unique while the group is shared.

`temporal_findings()` handles the cutoff check. With no `--cutoff` it returns nothing. With a cutoff but no `timestamp` column it emits a single `EPS004` warning so the missing check is visible instead of silently skipped. Otherwise it parses the column with `as.POSIXct` in UTC, raises `EPS005` warnings for values that come back `NA`, then flags every row whose split is in `--evaluation-splits` and whose timestamp is at or before the cutoff as an `EPS004` error.

Output is written twice. `write_findings()` emits a CSV with every field quoted and internal quotes doubled through `csv_escape()`. `write_sarif()` builds a SARIF 2.1.0 document by hand with `json_escape()`, declaring all five rules in the tool driver and mapping each finding to a `physicalLocation` pointing at the input path with `startLine` set to the first row of the group. Exit status is derived by ranking the observed maximum severity against `--fail-on`: `none` never fails, `warning` fails on anything found, `error` fails only on errors. A gate trip is exit 2.

## Usage

```bash
# manifest.csv
# split,id,content_hash,group,timestamp,source
# train,doc-1,ab12,acct-9,2025-11-02 10:00:00,crawl
# test,doc-9,ab12,acct-9,2026-03-04 08:30:00,crawl

chmod +x ExperimentSplitProvenance.R

# minimum run, uses default output paths
./ExperimentSplitProvenance.R --input manifest.csv

# full run with a temporal cutoff, wired for CI
Rscript ExperimentSplitProvenance.R \
  --input manifest.tsv \
  --delimiter tsv \
  --train-split train \
  --evaluation-splits validation,test,eval \
  --cutoff "2026-01-01 00:00:00" \
  --output findings.csv \
  --sarif split-provenance.sarif \
  --fail-on error

./ExperimentSplitProvenance.R --help
```

Defaults are `--output split-provenance-findings.csv`, `--sarif split-provenance.sarif`, `--train-split train`, `--evaluation-splits validation,test,eval` and `--fail-on error`. On completion it prints `Audited N rows: E errors, W warnings` to stderr.

## Notes

- Exit codes: 0 clean or below the gate, 2 the `--fail-on` threshold was met, 64 any error including a missing input, a missing `split` column, no identity column, a blank split value, a ragged row or an unparseable `--cutoff`.
- Matching is exact string equality after trimming. No near duplicate detection, no fuzzy matching and no embedding similarity, so a paraphrased or reformatted copy of a training row is invisible unless it shares an id, a hash or a group.
- Timestamp parsing uses base R `as.POSIXct` defaults, so `2026-01-01 00:00:00` is safe. Strict ISO8601 with a `T` separator or a `Z` suffix is not guaranteed across R versions. Values base R cannot read become `EPS005` warnings, and if it cannot pick a format for the column at all the run exits 64.
- Both output files are always written, even with no findings. The CSV then holds only its header row and the SARIF has an empty results array.
- One leaked record can produce several findings. An id collision that is also a hash collision reports as both `EPS001` and `EPS002`, so finding counts are not record counts.
- Row numbers assume one header line and no embedded newlines inside quoted fields. A multiline quoted field shifts reported lines away from the physical file.
- The whole manifest is loaded into memory and bucketed there. Fine for millions of rows on an ordinary machine, but it is not a streaming tool.
- It audits a manifest. It does not read your actual training data, does not fix a bad split and does not generate a corrected one.
