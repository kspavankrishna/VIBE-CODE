# Eval Leakage Sentinel

A benchmark row that also sits in your training corpus turns your eval score into a memorisation score. This is a leakage scanner that catches that overlap before the number reaches a slide: a base R row auditor for CSV eval sets and a Scala MinHash scanner for whole corpora on disk.

**Language:** R and Scala | **Lines:** 1085 total (651 R, 434 Scala) | **Added:** 2026-04-30 (Scala), 2026-07-25 (R)

## What this solves

Benchmark contamination is quiet. Nothing crashes. The eval suite runs green, the score goes up four points, someone posts it in the release channel and the team ships. Weeks later production quality does not match the offline number and nobody can say why. The cause is usually boring: an eval prompt was pasted into a fine tuning file, a RAG fixture was built from the same source document as a holdout item, or a synthetic data run regenerated questions from the benchmark it was scored against.

The specific failure modes all happen on real datasets. Two rows share an `item_id` across train and eval, so one silently overwrites the other in scoring joins. An eval item and a training item cite the same `source_url`, so the model saw the answer document even though the wording differs. The `answer_key` was left inside the visible prompt, so every model aces that item. Eval rows are dated before the training cutoff, which makes the temporal split fiction. And the classic: an eval prompt is a lightly reworded copy of a training prompt.

The cost lands on whoever has to explain the gap, because a contaminated benchmark corrupts every decision downstream of it. Neither tool tells you whether your model is good. They tell you whether the score is allowed to mean anything.

## Why I built it

Most tooling here is either a research notebook that assumes a GPU and a dataset object, or a hosted platform that wants your eval rows in someone else's bucket. Neither fits a small research gate in CI, where the job is read these files, exit non zero if the split is dirty, add no dependency tree. Eval data also lives in awkward places: a CSV in the repo, a JSONL dump beside the prompts, a folder of Markdown fixtures. So R uses base R only, Scala uses the JDK and standard library, and both stay readable, because a gate that blocks a release has to be auditable by the person it just blocked.

## When to use it

- A CI check on the pull request adding rows to a fine tuning corpus, failing if any collide with the eval pack.
- A gate before you publish benchmark numbers, so a contaminated item never becomes a dashboard figure.
- Auditing a vendor or open dataset drop against your own holdout before merging it into training.
- Comparing a production prompt pack against a regression suite, where prompts drift in by copy paste.
- Verifying a synthetic data run did not regenerate the eval set it was seeded from.
- Triage after an eval score jumps suspiciously and you need evidence either way in minutes.

## How it works

The R script audits one CSV row by row. `read_items` normalises column names, requires `item_id`, `split` and `prompt`, fills optional columns like `answer_key`, `source_url` and `created_at`, then builds a `text_for_scan` field per row. `split_role` maps free text split names onto three roles: `train`, `pretrain`, `sft` and `reference` become training, `eval`, `test`, `holdout`, `benchmark` and `gold` become evaluation, anything else becomes `unknown` and raises a medium finding, because an unrecognised split name is itself a risk.

Seven detectors then append to a findings frame. `detect_duplicate_ids` flags repeated ids, critical when they span roles. `detect_source_overlap` merges train and eval rows on `source_url` and marks every hit critical. `detect_answer_in_prompt` does a normalised substring check, ignoring answers under 12 characters. `detect_date_boundary` flags eval rows dated on or before the latest training `created_at`. `detect_empty_or_short_rows` catches blank prompts and eval prompts under 24 normalised characters.

`detect_near_duplicates` does the text work: exact Jaccard over token shingles, made affordable by blocking. `normalize_text` transliterates to ASCII, lowercases, collapses every URL to a single `urltoken` so shared links do not inflate similarity, and strips punctuation. `candidate_pairs_from_blocks` takes the 12 lexicographically smallest shingles of a row as blocking keys, indexes training rows into a hashed environment keyed by shingle, and compares an eval row only against training rows sharing a key. One deliberate escape hatch: if an eval row hits no block and both sides are under 2000 rows, it scans every training row rather than reporting a false clean. Pair generation stops at `--max-pairs` and raises a `truncated` flag in the report. Scores above `--threshold` are high, 0.94 and above critical.

The Scala object works on directory trees instead of rows. `loadDocs` explodes structured files into per row documents: JSONL one doc per line, CSV and TSV one doc per row through a quote aware `splitDelimited`, JSON through `extractJsonStrings`, which collects string values and skips keys by peeking for a following colon. `normalize` applies NFKC, strips the BOM, replaces URLs with `<url>` and emails with `<email>`, unescapes HTML entities and collapses whitespace. Documents under `--min-chars` are dropped before fingerprinting, which kills most boilerplate false positives.

Candidate generation there is probabilistic. `buildShingles` hashes tokens with FNV-1a 64 and folds each window into one 64 bit value through a SplitMix64 style `mix64` finaliser, `signature` computes a 64 permutation MinHash, and `detect` applies locality sensitive hashing: signatures split into 16 bands of 4 rows, each band hashed by `bandHash`, the left side indexed into buckets. Right side documents probe those buckets, collect votes, keep the top `--max-candidates`, and only then pay for exact math. `overlap` is a merge join over two sorted arrays returning Jaccard, containment and shared count in one pass. Containment is the second signal that matters: dividing shared shingles by the smaller set means a short eval item swallowed by a long training document scores near 1.0 while Jaccard stays low. `Match.label` reads those numbers, using a SHA-256 digest for `exact-duplicate` and reserving `subset-leak` for containment above 0.98 with Jaccard under 0.75.

## The two implementations

Same idea at two altitudes, not interchangeable.

R assumes the eval data is already a table with metadata, and most of its value is in checks unrelated to text similarity. Shared source URLs, duplicate ids, answer keys in prompts, eval rows predating the cutoff: overlap matching finds none of those. R also ranks findings by `severity_rank` and gates the exit code with `--fail-on`, so it is the one that belongs in CI. Its weakness is scale. Blocking on 12 sorted shingles is crude, pair vectors grow with `c()` inside a loop, and the fallback under 2000 rows means the worst case is every pair.

Scala assumes nothing but files on disk and scales roughly linearly, because LSH banding never materialises the full pair matrix. It reads more formats, handles Unicode through NFKC where R leans on `iconv`, and adds containment, which R lacks. What it does not do is metadata reasoning: no concept of splits, ids, dates or answer keys, and text output only.

Pick R for a CSV eval set with columns, for the metadata hygiene checks, or when a severity threshold should control the build. Pick Scala for two corpora of loose files, when volume makes exact pairwise Jaccard impractical, or when a small eval item may be hiding inside a large training document. Running both is not redundant. They fail differently.

## Usage

```bash
# R: write a sample CSV showing the expected shape, then audit it
Rscript EvalLeakageSentinel.R --example --output sample.csv
Rscript EvalLeakageSentinel.R --input sample.csv

# R: stricter threshold, wider shingles, JSON report, fail the build on medium
Rscript EvalLeakageSentinel.R --input eval_items.csv \
  --threshold 0.75 --ngram 6 --max-pairs 500000 \
  --format json --output leak_report.json --fail-on medium

# R: report only, never fail the build
Rscript EvalLeakageSentinel.R --input eval_items.csv --format csv --fail-on never

# Scala: compile once, then compare a training corpus against an eval corpus
scalac EvalLeakageSentinel.scala -d out
scala -cp out EvalLeakageSentinel ./corpus/train ./corpus/eval

# Scala: looser thresholds, finer LSH, restricted file types
scala -cp out EvalLeakageSentinel ./corpus/train ./corpus/eval \
  --min-jaccard 0.70 --min-containment 0.85 --shingle-size 6 \
  --hashes 128 --bands 32 --min-chars 200 \
  --include-ext txt,md,jsonl,csv --max-candidates 64 --max-matches 25
```

Both exit 2 on findings, which is what you wire into CI. R also uses 64 for bad flags and 65 for bad input data. Scala uses 64 for a usage or validation error.

## Notes

- The Scala file as committed does not compile. One extra closing brace around `printReport` near line 409 closes the object early and leaves `usage` outside it. Fix the brace balance before `scalac`.
- Scala `--help` exits 64, not 0. An unknown flag throws `IllegalArgumentException` uncaught, and a flag missing its value throws `ArrayIndexOutOfBoundsException`. R handles both through `die`.
- Neither tool understands paraphrase. Both are lexical. An eval item genuinely rewritten with the same answer passes clean, and that class still needs human review or an embedding check.
- R holds every row and shingle set in memory and grows pair vectors inside a loop. Comfortable to low tens of thousands of rows, no further. A run where `pair_review_truncated` is true is not a clean run.
- Short items sit outside the gate by design: Scala drops documents under `--min-chars`, R flags but does not compare eval prompts under 24 normalised characters.
- R reads CSV only and exits 65 on missing required columns. Scala reads each file fully into memory as UTF-8 with no error handling, and symlinks are not followed.
- Both report evidence, neither edits your data. Removing the row, rotating the eval item or labelling the score tainted stays a human decision.
