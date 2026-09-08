# Inference Batch Reconciler

You submit ten thousand inference requests to the OpenAI Batch API or Azure OpenAI batch, get a result file back, and the two files do not line up. This is a single file C# tool that joins request JSONL against result JSONL on `custom_id`, classifies every failure as transient or permanent, and writes a retry batch you can resubmit without thinking twice.

**Language:** C# | **Lines:** 1469 | **Added:** 2026-05-21

## What this solves

Batch inference does not fail the way the docs imply. It fails by drift. You send 10,000 lines, you get 9,987 back. Thirteen requests have no result line at all: no error, no status code, just absent. Nobody notices until a downstream job reports a null embedding or an eval harness silently scores a smaller denominator than last week. The typical response is somebody opening both JSONL files in an editor and diffing by eye, which does not work past a few hundred lines and does not work at all when the result file arrives in more than one export.

The second failure is worse because it looks fine. The provider retried internally, or a partial export got concatenated with a full one, and now the same `custom_id` appears twice in the result file with two different payloads. One says 200 with a completion, one says 429. If your loader takes the first row it sees, the output is nondeterministic across reruns of the same batch. If it takes the last, it is nondeterministic in a different direction. Either way you cannot reproduce yesterday's dataset, and the person who notices is the person doing the model comparison a month later, when the source files are gone.

The third failure costs money. Somebody writes a retry script that resubmits every non 200 line. That resubmits the `context_length_exceeded` rows, the `content_policy_violation` rows, the `invalid_request_error` rows, and the `insufficient_quota` rows, all of which fail again for the same reason, at full token cost, forever. Or the opposite: somebody decides retries are risky and ships the batch with the 429s missing, so a chunk of the corpus quietly does not exist. Both of these are routine in eval backfills, synthetic data generation and embedding jobs where the batch is large enough that nobody reads it.

This tool makes the join explicit and the retry decision auditable. Every request gets exactly one outcome. Every retryable line lands in a new JSONL file, byte identical to the original request line. Anything it cannot classify with confidence gets flagged for a human instead of being guessed at, and the process exits with code 2 so a CI step or an orchestrator can stop the pipeline rather than continue on bad data.

## Why I built it

The provider SDKs give you the batch file back and stop there. They do not tell you which requests never came back, they do not deduplicate a result file that has been exported twice, and they have no opinion about which error codes are worth retrying. Every team building an offline inference pipeline writes the same throwaway Python script to do this, and that script almost always takes the first matching row, ignores duplicates, and retries on any status other than 200.

I wanted the reconciliation itself to be the artifact: one file, no dependencies beyond the base class library, a machine readable summary for automation and a human summary for the person on call. C# because these pipelines often sit inside a .NET worker or a backend service that already owns the batch submission, and shelling out to a Python script from a hosted service is a deployment problem nobody wants.

## When to use it

- A batch job comes back with fewer result lines than you submitted and you need to know exactly which `custom_id` values are missing before you rerun anything.
- The result file was exported in pieces or the provider retried internally, and you now have duplicate rows for the same `custom_id` with conflicting payloads.
- You want a retry batch that contains the 429s and 503s but not the content policy rejections, generated automatically instead of by hand.
- You are running an eval backfill or a synthetic data generation job in CI and want the build to fail when the batch did not reconcile cleanly.
- You are migrating a large corpus to a new response format and need to separate real schema rejections from transient provider load.
- You need an audit trail showing why each request was or was not replayed, months after the batch ran.

## How it works

Both files are streamed line by line with `File.ReadLines` and parsed one JSON document at a time, so memory scales with the record count rather than the file size. `RequestFileIndex.Load` reads the request side, requiring `custom_id`, `url` and `body` on every line, defaulting `method` to POST, and building a `BatchRequestRecord` that keeps the original raw line verbatim. `ResultFileIndex.Load` reads the result side and is deliberately forgiving about shape: `JsonValueReader` walks a set of fallback paths for each field, so a status code is read from root `status_code`, then `response.status_code`, then `result.status_code`, and an error code is read from `error.code`, `error.type`, `response.body.error.code`, `response.body.error.type` and the equivalent `result.*` paths. That is what lets the same tool handle OpenAI style, Azure style and slightly mangled intermediate formats without a schema per provider.

The core trick is fingerprinting. `JsonCanonicalizer.Canonicalize` reparses a JSON payload and rewrites it with object properties sorted by ordinal name comparison, arrays left in order and numbers written raw, then `Hashing.ComputeSha256Hex` takes SHA-256 over the canonical form. A request fingerprint covers uppercased method, trimmed URL and canonical body. A result fingerprint covers status code, error code, error message and canonical response body. Canonicalizing first is what makes duplicate detection correct: two rows that differ only in key order or whitespace hash identically and are treated as the same result, while two rows that genuinely disagree hash differently and get escalated. Without that step you either miss real conflicts or raise false ones on cosmetic formatting differences.

Duplicates on the two sides are handled differently on purpose. A repeated `custom_id` in the request file is an input defect: `RequestFileIndex` keeps only the first occurrence, records a `DuplicateRequestRecord` with both line numbers and whether the fingerprints matched, and `BatchReconciler.Reconcile` marks that request as `BatchOutcomeKind.InputConflict` with `RetryDecision.ManualReview`. It is excluded from the retry batch entirely, because you cannot know which of the two payloads the result belongs to. A repeated `custom_id` in the result file is a provider or export artifact, so `DuplicateResultAnalysis.Create` compares every row's fingerprint against the first, records a diagnostic naming both line numbers for each disagreement, and picks a winner with a deterministic scoring rule: 3 for a 2xx with no structured error, 2 for a status code in the transient set, 1 otherwise, ties broken by lowest line number. The pick is stable across runs, and if the rows disagreed at all the entry is still flagged for manual review.

`RetryClassifier.Classify` then decides the outcome for the selected result. A 2xx with no structured error is `Succeeded`. Otherwise it checks the retryable side first: status code in `RetryableStatusCodes` (408, 409, 425, 429, 500, 502, 503, 504 and 529), or error code in `RetryableErrorCodes` (`rate_limit_exceeded`, `server_error`, `batch_expired`, `overloaded`, `request_timeout` and friends), or an error message containing timeout, temporarily unavailable, try again or overloaded. Then the permanent side: error code in `PermanentErrorCodes` (`context_length_exceeded`, `content_policy_violation`, `invalid_request_error`, `insufficient_quota`, `model_not_found` and friends), any 4xx that is not in the retryable set, or a message containing invalid, unsupported, permission, quota or not found. Anything that matches neither is not guessed at. It becomes a `PermanentFailure` with `RetryDecision.ManualReview` and `RequiresManualReview` set, which keeps it out of the retry file and out of your token budget.

Requests with no matching result at all become `MissingResult`. By default they are treated as retryable, on the reasoning that the original request line never got a verdict so replaying it is safe. `--no-retry-missing` flips that to `NotEligible` for jobs where a resubmit is not idempotent. Result rows whose `custom_id` is absent from the request file, and rows with no `custom_id` at all, are collected separately as `UnknownResultRecord` values and reported rather than dropped.

Output comes from `BatchReconciliationReport`. `WriteRetryRequests` writes each eligible request's original raw line to the `--retry-out` path in UTF-8 without a BOM, so the retry batch is a byte level replay of the submitted request rather than a reserialization that might change the payload. `WriteSummaryJson` emits an indented report with counts, a per entry array carrying outcome, retry decision, line numbers, status, error code, message, duplicate count and diagnostics, plus detail arrays for the unknown and headerless result rows. `ToSummaryText` prints the human version, capping diagnostics at `--max-diagnostics` (default 16) and ordering them so duplicate requests and unjoinable result lines surface before per request notes. `RequiresManualReview` is true if any entry needs review or if there are any duplicate requests, unknown results or results with no `custom_id`, and that flag is what drives the exit code.

## Usage

```bash
# Reconcile only, print the human summary
dotnet run -- --requests requests.jsonl --results results.jsonl

# Full run: retry batch plus machine readable report
dotnet run -- \
  --requests requests.jsonl \
  --results results.jsonl \
  --retry-out retry.jsonl \
  --summary-json reconciliation.json \
  --max-diagnostics 40

# Treat missing result lines as non retryable
dotnet run -- --requests requests.jsonl --results results.jsonl --no-retry-missing

# Help
dotnet run -- --help
```

Expected line shapes, straight from the help text:

```json
{"custom_id":"job-1","method":"POST","url":"/v1/responses","body":{...}}
{"custom_id":"job-1","response":{"status_code":200,"body":{...}},"error":null}
```

As a library inside an existing .NET service:

```csharp
using VibeCode;

var options = new BatchReconcilerOptions { RetryMissingResults = true, MaxDiagnostics = 32 };
options.RetryableErrorCodes.Add("model_overloaded");   // sets are mutable
options.PermanentErrorCodes.Add("billing_hard_limit");

var report = new BatchReconciler(options).Reconcile("requests.jsonl", "results.jsonl");

if (report.RequiresManualReview)
{
    foreach (var entry in report.Entries.Where(e => e.RequiresManualReview))
        Console.Error.WriteLine($"{entry.CustomId}: {entry.Summary}");
}

report.WriteRetryRequests("retry.jsonl");
report.WriteSummaryJson("reconciliation.json");
```

## Notes

- Exit codes: 0 clean, 1 input or parse error with the message on stderr, 2 reconciled but something needs a human. A CI gate should treat 2 as a stop, not a warning.
- It reconciles, it does not submit. Uploading `retry.jsonl` back to the provider is your job, and so is any backoff between attempts. There is no HTTP client in this file.
- The whole request index is held in memory. Lines stream from disk one at a time, but `OrderedRequests`, the `custom_id` dictionary and every result bucket stay resident for the run.
- Malformed JSON on any line throws out of `JsonDocument.Parse` and aborts the run with exit code 1. There is no skip and continue mode, and no partial report on a bad line.
- Classification is policy driven, not semantic. A 501 or 505 matches no rule, since the permanent status check only covers 4xx, so it falls to the message heuristics and then to manual review. That is intentional but it means unusual 5xx codes will accumulate in the review pile until you add them to `RetryableStatusCodes`.
- The message substring heuristics are blunt. An error message containing the word invalid is treated as permanent even when the underlying cause was transient, so the error code path should be preferred wherever the provider populates one.
- A duplicate `custom_id` in the request file blocks that id from ever entering the retry batch, by design. Fix the source batch generator rather than working around it here.
- Requires .NET 8 or newer for `#nullable enable`, file scoped namespaces, `file` scoped types and `SHA256.HashData`. No NuGet packages, no external dependencies.
