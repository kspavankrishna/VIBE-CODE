# Eval Holdout Firewall

Protected holdout text leaks into training sets, RAG corpora and agent traces in a form no checksum will ever catch: lowercased, reflowed, wrapped in JSONL, truncated mid passage. This is a single file OCaml CLI that fingerprints your holdouts once, then scans candidate data for near duplicate overlap and exits non zero when it finds any.

**Language:** OCaml | **Lines:** 523 | **Added:** 2026-07-07

## What this solves

This solves the April 2026 problem where teams ship LLM evals, fine tuning sets, RAG corpora, benchmark prompts and agent traces without knowing that protected holdout material has quietly leaked into the candidate data. The leak is almost never a clean copy of a file. Somebody pulls a benchmark into a scratch notebook, a context builder truncates it, a scraper lowercases it, a data loader wraps each item in a JSON object and six weeks later that text is sitting in the fine tuning shard nobody re-reads. The eval number goes up. Everyone celebrates.

Without a check like this the failure surfaces late and expensively. You publish a score, somebody outside the team reproduces it on a clean split and gets four points lower, and now you are re-running the training job, re-writing the model card and explaining why the number in the deck was wrong. In research the same defect kills reproducibility: the paper claims generalization, the reviewer finds test items verbatim in the training corpus, the result is withdrawn.

A normal grep, checksum or exact file diff misses all of it. `sha256sum` compares whole files, so one byte of whitespace defeats it. `grep -F` needs the exact substring, so any normalization defeats it. Exact matching also cannot answer the question you care about, which is not "is this file identical" but "how much of this holdout has bled into this chunk". That is a set overlap question and it needs a set overlap answer, delivered at the pull request where it costs a rerun instead of a retraction.

## Why I built it

The existing options are either too heavy or too dumb. Dedup libraries in the Python ecosystem want a Spark cluster, a config file and a dependency tree before they will tell you anything. Plain `grep` and `diff` tell you nothing once the text has been touched. Nothing in between fits a CI job that has to run in a container, produce reviewable evidence and fail a build with an exit code.

So this is a dependency free OCaml command line guard: one file, standard library only, deterministic output, no config format, no network. It prints JSON lines a human can read and a script can parse, and returns exit code 2 when a candidate is blocked so a GitHub Actions step fails on its own. Commit the manifest next to the eval and the check becomes part of the repository rather than a thing somebody remembers to run.

## When to use it

- Gating a fine tuning dataset PR against the benchmark holdouts your team promised not to train on
- Checking a RAG corpus before indexing, so retrieved chunks are not silently answering eval questions from the source text
- Preflighting a model registry release: scan the training manifest against the eval suite and refuse to publish on a hit
- Auditing agent traces or tool transcripts that were harvested from production and may contain test prompts
- Reproducibility checks on a paper artifact, where you need evidence that the train split and the test split do not overlap
- Reviewing a vendor supplied dataset you did not build and cannot fully trust

## How it works

Two modes, one binary. `manifest` reads holdout files and prints one TSV row per file. `scan` loads that manifest and runs candidate files past it. The first positional argument picks the mode and `parse_args` rejects anything else.

Both modes start with normalization. `tokenize` walks the raw bytes and keeps only `[a-zA-Z0-9_]`, lowercasing through `lower_ascii` and treating everything else as a separator. Punctuation, markup, JSON braces and line breaks all collapse to token boundaries, which is exactly why a passage survives being reflowed or wrapped in JSONL. Each token is hashed to 64 bits by `hash_string`, an FNV-1a style construction using `fnv_offset` and `fnv_prime` folded through `mix64`. Overlapping k-grams of `cfg.kgram` tokens, default 5, are hashed by `hash_kgram`, which mixes each token hash with the constant `separator_mix` (`9e3779b185ebca87`, the 64 bit golden ratio) so token order matters and boundaries cannot be forged by concatenation.

The sketch is a bottom-k MinHash, also called a k minimum values sketch. `insert_smallest` maintains a sorted ascending list of the smallest distinct k-gram hashes, capped at `cfg.signature_size`, default 192. Duplicates are dropped on insert, so repeated boilerplate counts once. Bottom-k fits because the fingerprint is fixed size regardless of document length, fully deterministic with no random seed to record, and sorted, which makes comparison a linear merge. `signature_of_tokens` handles the short document case too: fewer tokens than `kgram` yields a single gram covering everything rather than nothing.

Comparison uses containment, not Jaccard. `intersection_count` merges the two sorted signatures and counts equal values, and `containment_score` divides by `min(|left|, |right|)`. Jaccard would punish a short leaked passage sitting inside a large candidate file because the union term explodes. Containment asks the useful question: what fraction of the smaller signature is present in the larger one.

Scanning is windowed. `scan_candidate` tokenizes the whole candidate, then `scan_chunk` slides a window of `cfg.window_tokens` tokens, default 720, forward by `cfg.stride_tokens`, default 240. The three times overlap means a leaked passage straddling a window boundary is still caught whole by the next one. Every window is fingerprinted independently and scored against every holdout, and any pair at or above `cfg.threshold`, default 0.72, becomes a `hit`. `sort_hits` orders by descending score then candidate then chunk start, `take` trims to `cfg.max_report`, `print_hit` emits one JSON object per line, and `run_scan` folds the per file results and returns 2 if anything was blocked.

The manifest is deliberately boring TSV: id, bytes, tokens, kgrams, comma separated hex signature and escaped source path. `parse_manifest_line` demands exactly six fields and names the offending line number when it does not get them, and `load_manifest` skips blank lines and `#` comments so the file can carry provenance notes. Ids come from `fingerprint_of_text` as the sanitized basename plus the first 12 hex digits of the smallest hash, stable and greppable across runs.

## Usage

```
# Build. Standard library only, no dependencies.
ocamlc -o eval_holdout_firewall EvalHoldoutFirewall.ml

# 1. Fingerprint the holdouts once, commit the manifest.
./eval_holdout_firewall manifest evals/gsm8k_test.jsonl evals/internal_qa.txt > holdouts.tsv

# Tighter fingerprint for short items.
./eval_holdout_firewall manifest --k 4 --signature 256 evals/items/*.txt > holdouts.tsv

# 2. Scan candidate data in CI. Exit 2 means blocked.
./eval_holdout_firewall scan --manifest holdouts.tsv train/shard_00.jsonl train/shard_01.jsonl

# More sensitive threshold, larger windows, always emit a verdict line.
./eval_holdout_firewall scan --manifest holdouts.tsv \
  --threshold 0.55 --window 1024 --stride 256 --emit-clean corpus/rag_chunks.txt

# Pipe from stdin. '-' or no input path both mean stdin.
cat suspicious_trace.txt | ./eval_holdout_firewall scan --manifest holdouts.tsv -

./eval_holdout_firewall --help
```

Sample blocked finding, one JSON object per line:

```
{"verdict":"block","candidate":"train/shard_00.jsonl","chunk_start_token":480,"chunk_end_token":1200,"candidate_tokens":720,"candidate_kgrams":716,"holdout_id":"internal_qa_txt:3f0a91c4be22","holdout_source":"evals/internal_qa.txt","holdout_tokens":9142,"holdout_kgrams":9138,"score":0.859375,"threshold":0.720000}
```

## Notes

- Exit codes: 0 clean, 2 at least one candidate was blocked, 64 usage error, 65 data error such as a malformed manifest row or bad hex, 74 system error from a missing or unreadable file. Manifest mode always exits 0.
- The tokenizer is ASCII only. `is_token_char` accepts `[a-zA-Z0-9_]` and `lower_ascii` folds only `A-Z`, so non ASCII bytes are separators. Text that is mostly CJK, Cyrillic or Devanagari produces almost no tokens and will not be fingerprinted usefully. Accented Latin words split at the accent. JSONL is not parsed either: the file is tokenized as one byte stream, so JSON keys become tokens like everything else.
- Each manifest row fingerprints an entire input file as one document. Point it at a 10,000 item benchmark file and one leaked item will not move a 192 hash signature anywhere near 0.72. To catch item level leakage, split the benchmark into one file per item and pass them all to `manifest`, or lower `--threshold` and accept more noise.
- `read_all` loads each file fully into memory before windowing, and cost is windows times holdouts times signature size, single threaded. Fine for a CI gate over a few hundred holdouts. Not built for multi gigabyte candidates or million document corpora.
- Findings are per window, so one leaked passage usually produces several rows at slightly different scores. `--max-report` caps rows per candidate file, default 40, and truncation is silent.
- It reports overlap. It does not say which direction the copy went, does not quote the matching text and does not remove anything. A human still reads the finding and decides.
