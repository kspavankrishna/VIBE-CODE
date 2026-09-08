# Eval Artifact Lineage Gate

Your CI job runs an LLM eval suite every night and drops a pile of JSON reports into a bucket. Three weeks later nobody can prove which dataset hash, prompt hash, model id or region produced the numbers that shipped, and nobody can tell whether a report was edited after the run.

**Language:** OCaml | **Lines:** 732 | **Added:** 2026-06-09

## What this solves

AI evaluation produces a lot of small artifacts and almost no trustworthy provenance. A team runs a prompt regression suite, a model router bake off or a leakage scan, writes a report and moves on. The report is a loose blob: latency and cost numbers without the dataset hash behind them, a pass rate without the prompt template version, sitting where anyone with write access can edit a line and leave no trace. When an auditor asks which model and which corpus produced a claim, the honest answer is a shrug.

The concrete failure looks like this. A candidate model gets promoted on a 4 percent quality gain. Two months later someone notices it was scored on a smaller slice of the corpus, so cost per run looked lower and p95 latency looked better because the slow cases never ran. Nobody caught it because the two reports were never compared field by field and nothing required the dataset hash to match. That is a silent regression in production burning real inference spend, and the one who notices is the one paying the bill.

The second failure is evidence tampering, deliberate or not. A rerun overwrites a report, a flaky record gets hand edited out so the build goes green, a leakage counter is quietly zeroed. Without a chained hash there is no way to tell an honest rerun from a doctored file, so every compliance claim about data residency or PII hits rests on trust. And the usual fix, keeping raw prompts so you can reconstruct the run, turns the evidence store into a liability. This records hashes and counters, never payloads.

## Why I built it

Every serious option here wants infrastructure: experiment trackers want a server and an account, attestation frameworks want a signing service and a key custodian. Both are reasonable at scale and both are far too much for a repo that only needs to prove a model change did not get cheaper by evaluating less. The lightweight alternative people reach for instead is a JSON file plus good intentions, which is not evidence.

So this sits in the middle: one OCaml file, no opam packages, no database, no network. SHA-256 lives inside the file, so there is nothing to install and nothing to pin. It reads a plain text ledger any language can append to with a printf and exits nonzero when a candidate run breaks the budget you set.

## When to use it

- A nightly eval suite whose reports must stay diffable and provably unedited months later
- Promoting a candidate model or prompt template, with a hard stop if cost, p95 latency or failure rate drifts past budget
- Proving two runs used the same corpus and template, so a quality gain is real and not a smaller test set
- Enforcing data residency, where every record must carry the same region string and a mismatch breaks the build
- A leakage or PII scan whose hit count must stay at zero, anchored in a hash chain rather than a log line

## How it works

The input is a ledger: one record per line, `key=value` fields split on tabs, or on whitespace when the line has no tab. `parse_ledger_line` skips blanks and `#` comments. `parse_field` lowercases and trims each key, rejects keys outside `[a-z0-9_-.]`, rejects duplicates inside a record and percent decodes the value, so a value can carry any byte the writer encoded.

`canonicalize` is what makes the hashes stable. It drops every key in the ignore set, percent encodes each value with a narrow safe set (`is_canonical_safe` keeps letters, digits and `- _ . / :`) then joins the pairs with newlines. Pairs come from `StringMap.bindings`, so they sort by key and field order stops mattering. `default_ignored_keys` covers timestamps, host, pid, the id and nonce fields, signature and the four hash fields, so a rerun of an identical evaluation hashes the same even though the clock moved. `--ignore` adds more.

Hashing is a from scratch SHA-256 in the `Sha256` module, written on `Int32` with the standard round constants, `rotr`, `ch`, `maj`, the four sigma functions and length padding in `pad`. No crypto dependency, which is the point, and `selftest` checks it against the published vectors for the empty string and `abc` first.

The chain is an append only hash chain, the same shape as a Git commit chain or any tamper evident log. `read_ledger` starts the previous hash at 64 zeros, then per record computes `record_hash = SHA256(canonical)` and `chain_hash = SHA256(prev ^ "\n" ^ record_hash ^ "\n" ^ canonical)`, feeding that forward. Change one byte on line 3 and every chain hash after it changes, so one published head covers the whole file. A record that carries `record_hash`, `chain_hash`, `prev_chain_hash` or `previous_chain_hash` has those compared against the computed values, and a mismatch raises `User_error` naming the line. That is how you verify a ledger somebody else wrote.

`summarize_record` folds records into a `summary`, and `first_present` accepts aliases, so `tokens_in`, `input_tokens` and `prompt_tokens` are one field, as are the latency, cost, leakage, dataset, prompt, model and region families. `boolish_false` reads `0`, `false`, `no`, `failed` and `error` as failures. `percentile` is nearest rank on the sorted array, so p50 and p95 are observed values, never interpolated. `run_gate` prints both summaries as flat `label.key=value` lines then checks them: `check_ratio` on cost and p95 latency, where a zero baseline against a nonzero candidate gives infinity and fails any limit, `check_float_max` on failure rate, `check_int_max` on leakage hits and `record_coverage`, which demands `--min-record-ratio` of the baseline record count and kills the cheaper because it ran less trick. The `--require-same-*` flags add `check_set_equal` on dataset hashes, prompt hashes, model ids and regions. The gate passes only if every check passes.

## Usage

```sh
# verify the built in SHA-256 against known vectors first
ocaml EvalArtifactLineageGate.ml selftest
# -> selftest=ok

# a ledger line: tab or space separated key=value, values percent encoded
# dataset_sha256=9f2b... prompt_sha256=41ca... model=claude-sonnet region=eu-west-1 \
#   tokens_in=812 tokens_out=240 cost_usd=0.0041 latency_ms=734 leakage_hits=0 n=50 passed=1

# aggregate one ledger (a path, or "-" for stdin)
ocaml EvalArtifactLineageGate.ml summarize --input candidate.tsv
# -> ledger.records=120
#    ledger.chain_hash=6d1f...
#    ledger.cost_usd=0.49200000
#    ledger.latency_p95_ms=1180.000
#    ledger.failure_rate=0.000000

# print the per record hash chain plus the canonical bytes that were hashed
ocaml EvalArtifactLineageGate.ml chain --input candidate.tsv --canonical
# -> line=1 record_hash=... prev_chain_hash=000...0 chain_hash=...

# gate a candidate run against a baseline run
ocaml EvalArtifactLineageGate.ml gate \
  --baseline baseline.tsv \
  --candidate candidate.tsv \
  --max-cost-ratio 1.15 \
  --max-p95-latency-ratio 1.25 \
  --max-failure-rate 0.0 \
  --max-leakage-hits 0 \
  --min-record-ratio 1.0 \
  --require-same-dataset --require-same-prompt --require-same-model --require-same-region
# -> baseline.* and candidate.* summary lines
#    PASS cost_ratio observed=1.031000 limit=1.150000 ...
#    FAIL dataset_hashes baseline=9f2b... candidate=1a04...
#    GATE FAIL

# ignore extra volatile keys on top of the built in list
ocaml EvalArtifactLineageGate.ml summarize --input run.tsv --ignore worker,attempt,queue_time

# optional: compile once instead of running under the interpreter
ocamlopt EvalArtifactLineageGate.ml -o eval-gate && ./eval-gate gate --baseline b.tsv --candidate c.tsv
```

## Notes

- Exit codes: 0 when the gate passes, 1 when it fails or the selftest fails, 2 for a bad argument, a malformed ledger, a hash mismatch or an IO error.
- The chain is tamper evident, not tamper proof. There is no signing and no key material, so anyone who can rewrite the whole ledger can recompute every hash. Anchor the chain head somewhere they do not control: a commit, a build log or a separate record.
- Whoever writes the ledger must percent encode values. A raw space or tab inside a value becomes its own token and fails key validation, and a truncated `%` escape is an error.
- Missing numeric fields default to zero, so a record with no `cost_usd` contributes nothing rather than failing. `n` defaults to 1 sample per record and a latency only counts when above zero.
- Comparison is aggregate, not per case: totals and percentiles across whole ledgers, no pairing of individual eval cases and no significance testing. `--min-record-ratio` is the only guard against a candidate that ran fewer cases.
- Numbers go through OCaml's `int_of_string` and `float_of_string`, which accept `0x1f`, `1_000` and `infinity`, and the ledger is read fully into memory. It only reads and reports: writes no ledgers, calls no model, touches no network and needs nothing beyond the OCaml standard library.
