# Inference Batch Reconciler

You submit ten thousand JSONL lines to the OpenAI Batch API or Azure OpenAI batch endpoint, get a result file back, and the two files no longer line up. This is a single file C# tool that joins request JSONL to result JSONL on `custom_id`, classifies every outcome and writes a retry batch holding only the lines that are safe to replay.

**Language:** C# | **Lines:** 1469 | **Added:** 2026-05-21

## What this solves

Batch inference does not fail the way tutorials show. It fails by drifting. You send 10,000 requests, the provider returns 9,987 result lines, and nobody notices the 13 that vanished until a downstream embedding index comes up short a week later. Or the provider retried internally and you get two rows for one `custom_id`, a 200 and a 429, and whichever your naive `foreach` writes last becomes the truth. Or an export was partial and result lines carry a `custom_id` that never existed in the request file, which usually means two jobs got crossed.

Without a reconciler the standard failure mode is a person diffing JSONL by hand at 11pm. The second is worse: a script that re-submits everything that was not a clean 200. That replays context_length_exceeded and content_policy_violation failures that will fail identically forever, burns real tokens and buries the permanent errors under retry noise. On a large backfill that is a spend problem which surfaces in a finance review, not a dashboard.

The subtle one is duplicate `custom_id` in the request file itself. If the same id appears twice with different bodies, the join key is no longer a key, and a retry file built on it will silently drop or duplicate work. This tool refuses to guess. It marks the id as an input conflict and blocks it from the retry batch. Who notices, in order: the pipeline owner sees a row count mismatch, the model team sees an eval with holes, finance sees the token bill. This moves detection to the minute the batch lands.

## Why I built it

Provider SDKs give you the batch download. They do not give you the ledger reconciliation. The client libraries hand back result objects and every team then writes the same throwaway script: dictionary keyed on `custom_id`, check status code, dump the failures. That script always misses the same four things: canonical payload comparison, duplicate result arbitration, a real transient versus permanent split and a refusal to auto-retry what it does not understand.

So: one file, no dependencies beyond the BCL, usable from a .NET 8 worker, a console app or a CI gate, producing a human summary and a machine readable JSON report from one pass. A reconciler and a classifier, not another SDK wrapper.

## When to use it

- The result file has fewer lines than the request file and you need to know which `custom_id` values are missing.
- You want to replay only the transient failures from a 50k line embeddings job without re-paying for what already succeeded.
- Two result files got concatenated, or the provider retried internally, and you have conflicting rows under one id.
- The batch runs as a CI or pipeline step and you want a non-zero exit code when anything needs a human.
- You are migrating models or response formats across a backfill and want a per request audit trail of what failed permanently and why.

## How it works

`Main` parses flags through `CliConfig.Parse`, builds a `BatchReconciler` with `BatchReconcilerOptions` and calls `Reconcile`. Both files stream through `File.ReadLines` and parse one line at a time with `JsonDocument`. Blank lines are skipped, and anything that is not a JSON object is a hard `CliException`.

`RequestFileIndex.Load` reads `custom_id`, `method` (defaulting to POST), `url` and the raw `body`, then computes a fingerprint through `Hashing.ComputeRequestFingerprint`: a SHA-256 hex digest over method, url and the body run through `JsonCanonicalizer.Canonicalize`, which sorts object properties in ordinal name order and preserves array order. That canonicalization is the load bearing detail. Two lines differing only in key ordering or whitespace hash identically, so a duplicate `custom_id` with the same payload is separated from one with a genuinely different payload. First occurrence wins the dictionary slot, later ones become `DuplicateRequestRecord` entries carrying both line numbers and a `SameFingerprint` flag.

`ResultFileIndex.Load` is deliberately forgiving about shape, because batch result envelopes vary. Status code is probed at `status_code`, then `response.status_code`, then `result.status_code`. Error code is probed at `error.code`, `error.type`, `response.body.error.code`, `response.body.error.type` and the `result.*` equivalents, with the same chain for `error.message`. `JsonValueReader.TryResolve` walks those paths and returns null instead of throwing on a missing segment. Results are bucketed per `custom_id`, and a line with no `custom_id` is kept as an unjoinable record instead of dropped.

`Reconcile` then iterates the ordered requests. An id in the duplicate set becomes `BatchOutcomeKind.InputConflict` with `RetryDecision.ManualReview` and is never retried. A request with no matching result becomes `MissingResult`, retryable by default and switched off with `--no-retry-missing`. Everything else goes through `DuplicateResultAnalysis.Create`, which compares result fingerprints built from status code, error code, error message and canonicalized response body. Agreeing duplicates are harmless. Disagreeing ones flag the entry for manual review with both line numbers recorded. The winner comes from `SelectPreferred` on a small score: 3 for a 2xx with no structured error, 2 for anything `LooksRetryable` recognizes, 1 otherwise, lowest line breaking ties. A clean success beats a transient failure for the same id, the right call when a provider retried internally.

`RetryClassifier.Classify` decides the outcome. A 2xx with no error object is `Succeeded`. Retryable is the union of a status set (408, 409, 425, 429, 500, 502, 503, 504, 529), an error code set including `rate_limit_exceeded`, `overloaded`, `batch_expired` and `api_connection_error`, and message substrings for timeout, temporarily unavailable, try again and overloaded. Permanent covers `context_length_exceeded`, `content_policy_violation`, `invalid_request_error`, `insufficient_quota` and friends, any 4xx outside the retryable set, plus substrings for invalid, unsupported, permission, quota and not found. The default branch matters most: anything the rules cannot classify returns as a permanent failure with `RequiresManualReview` set, so it never silently enters the retry batch.

`WriteRetryRequests` then writes each retryable request's original `RawLine` verbatim, so the retry file is byte identical to the source with no re-serialization risk. `WriteSummaryJson` emits counts plus a per entry array with outcome, retry decision, both line numbers, status code, error code, message and diagnostics.

## Usage

```bash
# reconcile and print a summary
dotnet run -- --requests requests.jsonl --results results.jsonl

# produce a retry batch and a machine readable report
dotnet run -- \
  --requests requests.jsonl \
  --results results.jsonl \
  --retry-out retry.jsonl \
  --summary-json report.json

# treat missing results as non retryable, show more diagnostics
dotnet run -- --requests requests.jsonl --results results.jsonl \
  --no-retry-missing --max-diagnostics 50

# exit codes: 0 clean, 1 input or CLI error, 2 manual review required
dotnet run -- --requests r.jsonl --results o.jsonl --retry-out retry.jsonl || \
  echo "batch needs attention"
```

Expected line shapes, from the built in help:

```
request: {"custom_id":"job-1","method":"POST","url":"/v1/responses","body":{...}}
result:  {"custom_id":"job-1","response":{"status_code":200,"body":{...}},"error":null}
```

As a library, construct `new BatchReconciler(options)` and call `Reconcile`. The returned `BatchReconciliationReport` exposes `TotalRequests`, `SucceededCount`, `RetryableFailureCount`, `PermanentFailureCount`, `MissingCount`, `InputConflictCount`, `RequiresManualReview` and `Entries`.

## Notes

- Exit 0 is clean, exit 1 is a CLI or input error, exit 2 means something needs a human: an unclassifiable result, conflicting duplicates, a duplicate request `custom_id`, an unknown result id or a result line with no `custom_id`. Treat 2 as a gate, not a warning.
- It calls no API. It reconciles files. Submitting `retry.jsonl` is your job, and so is backoff between attempts.
- Requests are held as an ordered list plus a dictionary, results as a dictionary of lists, so peak memory scales with line count. Only parsing streams.
- A duplicated request `custom_id` blocks that id entirely, first occurrence included. Fix the source batch and rerun.
- Classification falls back to English substring probes on error messages. Localized or unusually worded provider errors land in the manual review branch, the safe direction but one that needs attention.
- `--max-diagnostics` truncates only the human summary. The `--summary-json` report always carries every entry. The retry file replays the source line as read, so provider fields beyond `custom_id`, `method`, `url` and `body` survive untouched, written UTF-8 with no BOM.
- Requires .NET 8 or later for `SHA256.HashData`, `Utf8JsonWriter.WriteRawValue` and file scoped types. No external packages.
