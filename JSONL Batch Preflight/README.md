# JSONL Batch Preflight

A 400 MB JSONL batch gets uploaded to the OpenAI Batch API or Anthropic Message Batches, sits in a queue for hours, then comes back rejected because line 82,417 had a duplicate `custom_id`. This Bash script validates and shards the file before it ever leaves your machine.

**Language:** Bash | **Lines:** 791 | **Added:** 2026-04-22

## What this solves

Batch inference is cheap per token and expensive per mistake. You build a JSONL file from a generator script or a SQL export, you upload it, and the provider takes it. Then you wait. The failure does not show up at upload time. It shows up when the job comes back partially complete, or when the results file has fewer rows than the input, or when two rows share a `custom_id` and you cannot tell which response belongs to which request. By then you have paid for the tokens and burned the queue window.

The specific failure modes are boring and repetitive. A generator emits a blank line at the end of a loop. One record has a trailing comma from a manual edit and is not valid JSON. A prompt template interpolated an environment variable and now a live API key is sitting inside a `messages` array that is about to be shipped to a third party and logged there. The `model` field is missing on 200 rows because a branch in the builder script forgot to set it. A single record ballooned to 8 MB because someone stuffed a whole PDF into the prompt. Every one of those is detectable in seconds with a local pass over the file, and almost nobody does it, because writing the validator is annoying and the failure feels rare until it lands on a backfill that costs real money.

Duplicate IDs deserve special mention. They do not crash anything. The provider accepts the file, runs both requests and returns both responses with the same `custom_id`. Your join back to the source table silently picks one at random. That corruption is invisible in logs and shows up weeks later as an eval score that nobody can reproduce.

## Why I built it

The existing options are a Python script somebody wrote in a notebook and never packaged, or nothing. A Python validator means a virtualenv, a requirements file and a dependency story in every CI runner, cron box and Airflow worker that touches the file. That is a lot of ceremony for what is fundamentally a pass over lines of text with `jq`. I wanted one file that drops into a GitHub Action, a cron job or a terminal with no install step beyond `jq`, and that treats the JSONL artifact with the same gatekeeping you would apply to a deployable binary: parse it, check its shape against the target provider, hash it, split it deterministically and write a manifest the next stage can trust.

## When to use it

- Before uploading an eval set or a prompt backfill to the OpenAI Batch API or Anthropic Message Batches
- In CI, as a gate that fails the build when a generated dataset artifact is malformed
- When a generator script was just refactored and you want proof the output shape did not drift
- When a batch file is too big for the provider upload limit and you need reproducible parts with a manifest
- Before publishing a JSONL dataset to object storage, so consumers get checksums instead of guesses
- After a partial batch failure, to work out whether duplicate IDs or bad records caused it

## How it works

Two subcommands, `check` and `shard`, share the same validation pipeline. `parse_args` handles flags, validates that `--max-shard-bytes`, `--max-shard-lines` and `--max-line-bytes` are positive integers through `validate_positive_integer`, and for `shard` defaults `REPORT_PATH` to `$OUTPUT_DIR/manifest.json` when you did not pass `--report`. `main` then checks the hard dependencies: `jq`, `awk`, `sort`, plus `gzip` when the input ends in `.gz`, and at least one of `sha256sum`, `shasum` or `openssl` for hashing. `init_temp_files` creates a `mktemp -d` workspace and installs a `trap cleanup EXIT` so nothing is left behind.

`validate_input` streams the file through `stream_input`, which reads stdin when `--input -`, decompresses with `gzip -dc` for `.gz` inputs and otherwise cats the file. Each line goes to `validate_one_line`, an ordered gauntlet. Byte length first, since an oversized line is cheap to reject and expensive to parse. Then `detect_secret_labels`, a set of Bash regex matches for OpenAI `sk-` keys, `github_pat_` and `ghp/gho/ghu/ghs/ghr` tokens, Google `AIza` keys, AWS `AKIA`/`ASIA` access key IDs, Slack `xox[baprs]-` tokens, `Bearer` headers and PEM private key headers. Secrets are errors by default and drop to warnings under `--allow-secrets`.

Parsing goes through `jq -cS '.'`, which is the load bearing choice. The `-S` sorts object keys, so the record is canonicalized: two records that differ only in key order become byte identical. That gives you a stable size measurement and, more importantly, it makes semantic deduplication work. `payload_hash` deletes the ID field with `del(.[$id_key])` and takes a SHA-256 of what remains, so two requests with different `custom_id` values but identical prompts hash to the same value. `analyze_duplicates` then does the classic `sort | uniq -d` on the ID list and on the hash list. Duplicate IDs are always errors with a sample of up to eight offenders. Duplicate payloads are warnings unless you pass `--fail-on-payload-duplicates`, because sending the same prompt twice is sometimes deliberate and sometimes an accidental double append.

Shape checking is done by `profile_filter`, which emits a `jq` boolean expression per profile. `generic` only requires a non-empty string at the ID key. `openai-batch` additionally requires `method` equal to `post` case insensitively, a `url` matching `^/v[0-9]+/`, an object `body`, a non-empty `body.model` and at least one of `input`, `messages`, `prompt`, `text` or `instructions`. `anthropic-batch` requires an object `params` with a non-empty `model`, a numeric `max_tokens` greater than zero and either a `params.messages` array or a non-empty `params.system` string. When a line fails, the error message includes `profile_hint` so the message says what was expected rather than just that something was wrong.

`write_shards` is a greedy bin packer over three parallel streams. It opens the validated records, the extracted IDs and the per record canonical byte counts on file descriptors 3, 4 and 5, then reads all three in lockstep in one `while` loop. A new shard is cut when the current one hits `--max-shard-lines` or when adding the next record would push it past `--max-shard-bytes`. Input order is preserved. `finalize_shard` closes out each part with `sha256_file`, its exact byte count and the first and last request IDs, and `build_report` folds those plus the error and warning lists into a single manifest with thresholds, a summary block and a UTC timestamp.

Failure handling is deliberate: if any error was recorded, `main` writes the report first when one was requested, prints up to 20 errors and up to 20 warnings to stderr, and exits 1 without writing a single shard. You never get a half sharded output directory from a bad input.

## Usage

```bash
# Validate an eval set against the OpenAI batch request shape
./JsonlBatchPreflight.sh check \
  --input evals.jsonl \
  --profile openai-batch

# Validate and split a gzipped backfill, writing a manifest
./JsonlBatchPreflight.sh shard \
  --input backfill.jsonl.gz \
  --output-dir dist \
  --profile anthropic-batch \
  --report dist/manifest.json

# Custom ID field, tighter shards, secrets downgraded to warnings
./JsonlBatchPreflight.sh shard \
  --input requests.jsonl \
  --output-dir parts \
  --id-key request_id \
  --max-shard-bytes 20971520 \
  --max-shard-lines 1000 \
  --allow-secrets

# Read from stdin and fail the build on repeated prompts
cat generated.jsonl | ./JsonlBatchPreflight.sh check \
  --input - \
  --fail-on-payload-duplicates \
  --report ci/preflight.json

./JsonlBatchPreflight.sh --help
```

Shards are written as `<stem>.part-0001.jsonl`, `<stem>.part-0002.jsonl` and so on, where the stem comes from the input filename with `.gz`, `.jsonl` and `.json` stripped, or `batch` when reading stdin.

## Notes

- Exit codes are simple: 0 when validation passed, 1 when any error was recorded or when argument parsing failed via `die`. Warnings alone do not change the exit code.
- It is a per line `jq` invocation, not a single streaming pass. That is fine for tens of thousands of records and slow for millions. If your file is in the millions of lines, sample it or expect a long run.
- Secret detection is regex only and runs against the raw line before parsing. It catches common key formats. It will not catch a custom credential scheme, and it can false positive on any long `Bearer` looking string.
- Blank lines are treated as errors, not skipped. Strict JSONL means one object per line and nothing else.
- Duplicates are reported, never removed. The script does not rewrite, repair or deduplicate your data. It refuses to shard and tells you what is wrong.
- Byte accounting is done on canonicalized records with one newline added per line, so the shard sizes reflect what actually gets written, not the size of your original formatting.
- `--max-shard-bytes` also acts as a per record ceiling: a single canonical record larger than one shard is an error, since it could never be packed.
- Shard files are plain uncompressed JSONL. There is no gzip output mode and no upload step. Getting the parts to the provider is your job.
