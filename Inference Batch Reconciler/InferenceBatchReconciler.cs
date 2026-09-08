#nullable enable
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace VibeCode;

/// <summary>
/// Reconciles batch inference request JSONL with result JSONL, classifies outcomes, and emits a retry batch.
/// Designed for modern OpenAI-style or Azure OpenAI-style batch jobs where custom_id is the durable join key.
/// </summary>
public static class InferenceBatchReconciler
{
    private const int ExitOk = 0;
    private const int ExitInputError = 1;
    private const int ExitManualReview = 2;

    public static int Main(string[] args)
    {
        try
        {
            var cli = CliConfig.Parse(args);
            if (cli.ShowHelp)
            {
                PrintHelp();
                return ExitOk;
            }

            var reconciler = new BatchReconciler(cli.Options);
            var report = reconciler.Reconcile(cli.RequestsPath!, cli.ResultsPath!);

            if (!string.IsNullOrWhiteSpace(cli.RetryOutputPath))
            {
                report.WriteRetryRequests(cli.RetryOutputPath!);
            }

            if (!string.IsNullOrWhiteSpace(cli.SummaryJsonPath))
            {
                report.WriteSummaryJson(cli.SummaryJsonPath!);
            }

            Console.Out.Write(report.ToSummaryText());
            return report.RequiresManualReview ? ExitManualReview : ExitOk;
        }
        catch (CliException ex)
        {
            Console.Error.WriteLine(ex.Message);
            Console.Error.WriteLine("Use --help for usage.");
            return ExitInputError;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("InferenceBatchReconciler failed: " + ex.Message);
            return ExitInputError;
        }
    }

    private static void PrintHelp()
    {
        Console.WriteLine("InferenceBatchReconciler");
        Console.WriteLine("Reconcile request JSONL with result JSONL and produce a retry batch for transient failures.");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run -- --requests requests.jsonl --results results.jsonl [options]");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --requests <path>         Required. Original batch request JSONL.");
        Console.WriteLine("  --results <path>          Required. Batch result JSONL.");
        Console.WriteLine("  --retry-out <path>        Optional. Write retryable requests as JSONL.");
        Console.WriteLine("  --summary-json <path>     Optional. Write a machine-readable summary report.");
        Console.WriteLine("  --no-retry-missing        Do not treat missing results as retryable.");
        Console.WriteLine("  --max-diagnostics <n>     Maximum diagnostics in human summary. Default: 16.");
        Console.WriteLine("  --help                    Show this help text.");
        Console.WriteLine();
        Console.WriteLine("Expected request line shape:");
        Console.WriteLine("  {\"custom_id\":\"job-1\",\"method\":\"POST\",\"url\":\"/v1/responses\",\"body\":{...}}");
        Console.WriteLine();
        Console.WriteLine("Expected result line shape:");
        Console.WriteLine("  {\"custom_id\":\"job-1\",\"response\":{\"status_code\":200,\"body\":{...}},\"error\":null}");
    }
}

file sealed class CliConfig
{
    private CliConfig()
    {
    }

    public string? RequestsPath { get; private init; }
    public string? ResultsPath { get; private init; }
    public string? RetryOutputPath { get; private init; }
    public string? SummaryJsonPath { get; private init; }
    public bool ShowHelp { get; private init; }
    public BatchReconcilerOptions Options { get; private init; } = new();

    public static CliConfig Parse(string[] args)
    {
        var options = new BatchReconcilerOptions();
        string? requestsPath = null;
        string? resultsPath = null;
        string? retryOutputPath = null;
        string? summaryJsonPath = null;
        var showHelp = false;

        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--help":
                case "-h":
                    showHelp = true;
                    break;
                case "--requests":
                    requestsPath = ReadRequiredValue(args, ref index, "--requests");
                    break;
                case "--results":
                    resultsPath = ReadRequiredValue(args, ref index, "--results");
                    break;
                case "--retry-out":
                    retryOutputPath = ReadRequiredValue(args, ref index, "--retry-out");
                    break;
                case "--summary-json":
                    summaryJsonPath = ReadRequiredValue(args, ref index, "--summary-json");
                    break;
                case "--no-retry-missing":
                    options.RetryMissingResults = false;
                    break;
                case "--max-diagnostics":
                    options.MaxDiagnostics = ParsePositiveInt(ReadRequiredValue(args, ref index, "--max-diagnostics"), "--max-diagnostics");
                    break;
                default:
                    throw new CliException("Unknown argument: " + args[index]);
            }
        }

        if (!showHelp)
        {
            if (string.IsNullOrWhiteSpace(requestsPath))
            {
                throw new CliException("Missing required argument --requests.");
            }

            if (string.IsNullOrWhiteSpace(resultsPath))
            {
                throw new CliException("Missing required argument --results.");
            }
        }

        options.Validate();

        return new CliConfig
        {
            RequestsPath = requestsPath,
            ResultsPath = resultsPath,
            RetryOutputPath = retryOutputPath,
            SummaryJsonPath = summaryJsonPath,
            ShowHelp = showHelp,
            Options = options,
        };
    }

    private static string ReadRequiredValue(string[] args, ref int index, string flag)
    {
        if (index + 1 >= args.Length)
        {
            throw new CliException("Missing value for " + flag + ".");
        }

        index++;
        return args[index];
    }

    private static int ParsePositiveInt(string value, string flag)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0)
        {
            throw new CliException(flag + " expects a positive integer.");
        }

        return parsed;
    }
}

public sealed class BatchReconciler
{
    private readonly BatchReconcilerOptions _options;

    public BatchReconciler(BatchReconcilerOptions? options = null)
    {
        _options = options ?? new BatchReconcilerOptions();
        _options.Validate();
    }

    public BatchReconciliationReport Reconcile(string requestsPath, string resultsPath)
    {
        var requests = RequestFileIndex.Load(requestsPath);
        var results = ResultFileIndex.Load(resultsPath);

        var duplicateRequestIds = new HashSet<string>(
            requests.DuplicateRequests.Select(record => record.CustomId),
            StringComparer.Ordinal);

        var unknownResults = new List<UnknownResultRecord>();
        foreach (var pair in results.ByCustomId)
        {
            if (!requests.ByCustomId.ContainsKey(pair.Key))
            {
                foreach (var result in pair.Value)
                {
                    unknownResults.Add(new UnknownResultRecord(
                        result.LineNumber,
                        result.CustomId,
                        "Result custom_id does not exist in the original request file.",
                        result.StatusCode,
                        result.ErrorCode,
                        result.ErrorMessage));
                }
            }
        }

        var entries = new List<BatchReconciliationEntry>(requests.OrderedRequests.Count);
        var retryableRequests = new List<BatchRequestRecord>();

        foreach (var request in requests.OrderedRequests)
        {
            if (duplicateRequestIds.Contains(request.CustomId))
            {
                var duplicates = requests.DuplicateRequests
                    .Where(record => string.Equals(record.CustomId, request.CustomId, StringComparison.Ordinal))
                    .ToArray();

                entries.Add(BatchReconciliationEntry.CreateInputConflict(
                    request,
                    "Duplicate custom_id in request file. Retry generation is blocked until the source batch is fixed.",
                    duplicates.Select(BuildDuplicateRequestDiagnostic)));
                continue;
            }

            if (!results.ByCustomId.TryGetValue(request.CustomId, out var resultGroup) || resultGroup.Count == 0)
            {
                var missingEntry = BatchReconciliationEntry.CreateMissing(
                    request,
                    _options.RetryMissingResults
                        ? RetryDecision.EligibleForRetry
                        : RetryDecision.NotEligible,
                    _options.RetryMissingResults
                        ? "No result line matched this request. The original request is safe to retry."
                        : "No result line matched this request. Missing results are configured as non-retryable.");

                entries.Add(missingEntry);
                if (missingEntry.RetryDecision == RetryDecision.EligibleForRetry)
                {
                    retryableRequests.Add(request);
                }

                continue;
            }

            var duplicateAnalysis = DuplicateResultAnalysis.Create(resultGroup);
            var classification = RetryClassifier.Classify(duplicateAnalysis.Selected, _options);
            var entry = BatchReconciliationEntry.CreateResolved(
                request,
                duplicateAnalysis,
                classification);

            entries.Add(entry);
            if (entry.RetryDecision == RetryDecision.EligibleForRetry && !entry.RequiresManualReview)
            {
                retryableRequests.Add(request);
            }
        }

        return new BatchReconciliationReport(
            requestsPath,
            resultsPath,
            _options,
            requests.OrderedRequests,
            entries,
            requests.DuplicateRequests,
            unknownResults,
            results.ResultsMissingCustomId,
            retryableRequests);
    }

    private static string BuildDuplicateRequestDiagnostic(DuplicateRequestRecord record)
    {
        var sameness = record.SameFingerprint ? "same payload" : "different payload";
        return string.Create(
            CultureInfo.InvariantCulture,
            $"Request lines {record.FirstLineNumber} and {record.DuplicateLineNumber} reuse custom_id '{record.CustomId}' with {sameness}.");
    }
}

public sealed class BatchReconcilerOptions
{
    public bool RetryMissingResults { get; set; } = true;

    public int MaxDiagnostics { get; set; } = 16;

    public ISet<int> RetryableStatusCodes { get; } = new HashSet<int>
    {
        408,
        409,
        425,
        429,
        500,
        502,
        503,
        504,
        529,
    };

    public ISet<string> RetryableErrorCodes { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "api_connection_error",
        "batch_expired",
        "conflict_error",
        "overloaded",
        "rate_limit_exceeded",
        "request_timeout",
        "server_error",
        "service_unavailable",
        "timeout",
        "temporarily_unavailable",
    };

    public ISet<string> PermanentErrorCodes { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "authentication_error",
        "content_policy_violation",
        "context_length_exceeded",
        "insufficient_quota",
        "invalid_api_key",
        "invalid_request_error",
        "invalid_response_format",
        "model_not_found",
        "not_found_error",
        "permission_error",
        "unsupported_model",
        "unsupported_response_format",
        "validation_error",
    };

    public void Validate()
    {
        if (MaxDiagnostics <= 0)
        {
            throw new CliException("--max-diagnostics must be greater than zero.");
        }
    }
}

public sealed class BatchReconciliationReport
{
    private readonly IReadOnlyList<BatchRequestRecord> _orderedRequests;
    private readonly IReadOnlyList<BatchRequestRecord> _retryableRequests;

    public BatchReconciliationReport(
        string requestsPath,
        string resultsPath,
        BatchReconcilerOptions options,
        IReadOnlyList<BatchRequestRecord> orderedRequests,
        IReadOnlyList<BatchReconciliationEntry> entries,
        IReadOnlyList<DuplicateRequestRecord> duplicateRequests,
        IReadOnlyList<UnknownResultRecord> unknownResults,
        IReadOnlyList<UnknownResultRecord> resultsMissingCustomId,
        IReadOnlyList<BatchRequestRecord> retryableRequests)
    {
        RequestsPath = requestsPath;
        ResultsPath = resultsPath;
        Options = options;
        Entries = entries;
        DuplicateRequests = duplicateRequests;
        UnknownResults = unknownResults;
        ResultsMissingCustomId = resultsMissingCustomId;
        _orderedRequests = orderedRequests;
        _retryableRequests = retryableRequests;
    }

    public string RequestsPath { get; }

    public string ResultsPath { get; }

    public BatchReconcilerOptions Options { get; }

    public IReadOnlyList<BatchReconciliationEntry> Entries { get; }

    public IReadOnlyList<DuplicateRequestRecord> DuplicateRequests { get; }

    public IReadOnlyList<UnknownResultRecord> UnknownResults { get; }

    public IReadOnlyList<UnknownResultRecord> ResultsMissingCustomId { get; }

    public int TotalRequests => _orderedRequests.Count;

    public int SucceededCount => Entries.Count(entry => entry.Outcome == BatchOutcomeKind.Succeeded);

    public int RetryableFailureCount => Entries.Count(entry => entry.Outcome == BatchOutcomeKind.RetryableFailure);

    public int PermanentFailureCount => Entries.Count(entry => entry.Outcome == BatchOutcomeKind.PermanentFailure);

    public int MissingCount => Entries.Count(entry => entry.Outcome == BatchOutcomeKind.MissingResult);

    public int InputConflictCount => Entries.Count(entry => entry.Outcome == BatchOutcomeKind.InputConflict);

    public bool RequiresManualReview =>
        Entries.Any(entry => entry.RequiresManualReview) ||
        DuplicateRequests.Count > 0 ||
        UnknownResults.Count > 0 ||
        ResultsMissingCustomId.Count > 0;

    public void WriteRetryRequests(string path)
    {
        using var writer = new StreamWriter(path, false, new UTF8Encoding(false));
        foreach (var request in _retryableRequests)
        {
            writer.WriteLine(request.RawLine);
        }
    }

    public void WriteSummaryJson(string path)
    {
        using var stream = File.Create(path);
        using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true });

        writer.WriteStartObject();
        writer.WriteString("requests_path", RequestsPath);
        writer.WriteString("results_path", ResultsPath);
        writer.WriteBoolean("requires_manual_review", RequiresManualReview);
        writer.WriteNumber("total_requests", TotalRequests);
        writer.WriteNumber("succeeded", SucceededCount);
        writer.WriteNumber("retryable_failures", RetryableFailureCount);
        writer.WriteNumber("permanent_failures", PermanentFailureCount);
        writer.WriteNumber("missing_results", MissingCount);
        writer.WriteNumber("input_conflicts", InputConflictCount);
        writer.WriteNumber("unknown_results", UnknownResults.Count);
        writer.WriteNumber("results_missing_custom_id", ResultsMissingCustomId.Count);
        writer.WriteNumber("retryable_requests", _retryableRequests.Count);

        writer.WritePropertyName("entries");
        writer.WriteStartArray();
        foreach (var entry in Entries)
        {
            writer.WriteStartObject();
            writer.WriteString("custom_id", entry.CustomId);
            writer.WriteString("outcome", entry.Outcome.ToString());
            writer.WriteString("retry_decision", entry.RetryDecision.ToString());
            writer.WriteBoolean("requires_manual_review", entry.RequiresManualReview);
            writer.WriteNumber("request_line", entry.RequestLineNumber);
            if (entry.ResultLineNumber is int resultLine)
            {
                writer.WriteNumber("result_line", resultLine);
            }

            if (entry.StatusCode is int statusCode)
            {
                writer.WriteNumber("status_code", statusCode);
            }

            if (!string.IsNullOrWhiteSpace(entry.ErrorCode))
            {
                writer.WriteString("error_code", entry.ErrorCode);
            }

            if (!string.IsNullOrWhiteSpace(entry.ErrorMessage))
            {
                writer.WriteString("error_message", entry.ErrorMessage);
            }

            writer.WriteNumber("duplicate_result_count", entry.DuplicateResultCount);
            writer.WriteString("summary", entry.Summary);
            writer.WritePropertyName("diagnostics");
            writer.WriteStartArray();
            foreach (var diagnostic in entry.Diagnostics)
            {
                writer.WriteStringValue(diagnostic);
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        writer.WriteEndArray();

        writer.WritePropertyName("duplicate_requests");
        writer.WriteStartArray();
        foreach (var duplicate in DuplicateRequests)
        {
            writer.WriteStartObject();
            writer.WriteString("custom_id", duplicate.CustomId);
            writer.WriteNumber("first_line", duplicate.FirstLineNumber);
            writer.WriteNumber("duplicate_line", duplicate.DuplicateLineNumber);
            writer.WriteBoolean("same_fingerprint", duplicate.SameFingerprint);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();

        WriteUnknownResults(writer, "unknown_results_detail", UnknownResults);
        WriteUnknownResults(writer, "results_missing_custom_id_detail", ResultsMissingCustomId);

        writer.WriteEndObject();
        writer.Flush();
    }

    public string ToSummaryText()
    {
        var builder = new StringBuilder(4096);
        builder.AppendLine("InferenceBatchReconciler summary");
        builder.Append("Requests: ").Append(TotalRequests)
            .Append(" | succeeded: ").Append(SucceededCount)
            .Append(" | retryable: ").Append(Entries.Count(entry => entry.RetryDecision == RetryDecision.EligibleForRetry))
            .Append(" | permanent: ").Append(PermanentFailureCount)
            .Append(" | missing: ").Append(MissingCount)
            .Append(" | input-conflicts: ").Append(InputConflictCount)
            .AppendLine();
        builder.Append("Retry batch lines: ").Append(_retryableRequests.Count)
            .Append(" | unknown results: ").Append(UnknownResults.Count)
            .Append(" | missing custom_id results: ").Append(ResultsMissingCustomId.Count)
            .Append(" | manual review: ").Append(RequiresManualReview ? "yes" : "no")
            .AppendLine();
        builder.AppendLine();

        var interesting = BuildDiagnostics().Take(Options.MaxDiagnostics).ToArray();
        if (interesting.Length == 0)
        {
            builder.AppendLine("No diagnostics. Request/result sets reconciled cleanly.");
            return builder.ToString();
        }

        builder.AppendLine("Diagnostics:");
        foreach (var diagnostic in interesting)
        {
            builder.Append(" - ").AppendLine(diagnostic);
        }

        return builder.ToString();
    }

    private IEnumerable<string> BuildDiagnostics()
    {
        foreach (var duplicate in DuplicateRequests)
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"Request custom_id '{duplicate.CustomId}' is duplicated at lines {duplicate.FirstLineNumber} and {duplicate.DuplicateLineNumber}.");
        }

        foreach (var result in ResultsMissingCustomId)
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"Result line {result.LineNumber} has no custom_id and cannot be joined back to a request.");
        }

        foreach (var result in UnknownResults)
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"Result line {result.LineNumber} references unknown custom_id '{result.CustomId}'.");
        }

        foreach (var entry in Entries.Where(entry => entry.RequiresManualReview))
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"Request '{entry.CustomId}' needs manual review: {entry.Summary}");
        }

        foreach (var entry in Entries.Where(entry => entry.RetryDecision == RetryDecision.EligibleForRetry))
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"Request '{entry.CustomId}' is eligible for retry: {entry.Summary}");
        }

        foreach (var entry in Entries.Where(entry => entry.Outcome == BatchOutcomeKind.PermanentFailure))
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"Request '{entry.CustomId}' is a permanent failure: {entry.Summary}");
        }
    }

    private static void WriteUnknownResults(Utf8JsonWriter writer, string propertyName, IReadOnlyList<UnknownResultRecord> records)
    {
        writer.WritePropertyName(propertyName);
        writer.WriteStartArray();
        foreach (var record in records)
        {
            writer.WriteStartObject();
            writer.WriteNumber("line", record.LineNumber);
            if (!string.IsNullOrWhiteSpace(record.CustomId))
            {
                writer.WriteString("custom_id", record.CustomId);
            }

            writer.WriteString("reason", record.Reason);
            if (record.StatusCode is int statusCode)
            {
                writer.WriteNumber("status_code", statusCode);
            }

            if (!string.IsNullOrWhiteSpace(record.ErrorCode))
            {
                writer.WriteString("error_code", record.ErrorCode);
            }

            if (!string.IsNullOrWhiteSpace(record.ErrorMessage))
            {
                writer.WriteString("error_message", record.ErrorMessage);
            }

            writer.WriteEndObject();
        }

        writer.WriteEndArray();
    }
}

public sealed class BatchReconciliationEntry
{
    private BatchReconciliationEntry(
        string customId,
        int requestLineNumber,
        int? resultLineNumber,
        BatchOutcomeKind outcome,
        RetryDecision retryDecision,
        bool requiresManualReview,
        int? statusCode,
        string? errorCode,
        string? errorMessage,
        int duplicateResultCount,
        string summary,
        IReadOnlyList<string> diagnostics)
    {
        CustomId = customId;
        RequestLineNumber = requestLineNumber;
        ResultLineNumber = resultLineNumber;
        Outcome = outcome;
        RetryDecision = retryDecision;
        RequiresManualReview = requiresManualReview;
        StatusCode = statusCode;
        ErrorCode = errorCode;
        ErrorMessage = errorMessage;
        DuplicateResultCount = duplicateResultCount;
        Summary = summary;
        Diagnostics = diagnostics;
    }

    public string CustomId { get; }

    public int RequestLineNumber { get; }

    public int? ResultLineNumber { get; }

    public BatchOutcomeKind Outcome { get; }

    public RetryDecision RetryDecision { get; }

    public bool RequiresManualReview { get; }

    public int? StatusCode { get; }

    public string? ErrorCode { get; }

    public string? ErrorMessage { get; }

    public int DuplicateResultCount { get; }

    public string Summary { get; }

    public IReadOnlyList<string> Diagnostics { get; }

    public static BatchReconciliationEntry CreateInputConflict(
        BatchRequestRecord request,
        string summary,
        IEnumerable<string> diagnostics)
    {
        return new BatchReconciliationEntry(
            request.CustomId,
            request.LineNumber,
            null,
            BatchOutcomeKind.InputConflict,
            RetryDecision.ManualReview,
            requiresManualReview: true,
            statusCode: null,
            errorCode: null,
            errorMessage: null,
            duplicateResultCount: 0,
            summary,
            diagnostics.ToArray());
    }

    public static BatchReconciliationEntry CreateMissing(
        BatchRequestRecord request,
        RetryDecision retryDecision,
        string summary)
    {
        return new BatchReconciliationEntry(
            request.CustomId,
            request.LineNumber,
            null,
            BatchOutcomeKind.MissingResult,
            retryDecision,
            requiresManualReview: retryDecision == RetryDecision.ManualReview,
            statusCode: null,
            errorCode: null,
            errorMessage: null,
            duplicateResultCount: 0,
            summary,
            Array.Empty<string>());
    }

    public static BatchReconciliationEntry CreateResolved(
        BatchRequestRecord request,
        DuplicateResultAnalysis duplicateAnalysis,
        ResultClassification classification)
    {
        var diagnostics = new List<string>();
        if (duplicateAnalysis.ConflictingDuplicates.Count > 0)
        {
            diagnostics.AddRange(duplicateAnalysis.ConflictingDuplicates);
        }

        if (!string.IsNullOrWhiteSpace(classification.Reason))
        {
            diagnostics.Add(classification.Reason);
        }

        return new BatchReconciliationEntry(
            request.CustomId,
            request.LineNumber,
            duplicateAnalysis.Selected.LineNumber,
            classification.Outcome,
            classification.RetryDecision,
            classification.RequiresManualReview || duplicateAnalysis.RequiresManualReview,
            duplicateAnalysis.Selected.StatusCode,
            duplicateAnalysis.Selected.ErrorCode,
            duplicateAnalysis.Selected.ErrorMessage,
            duplicateAnalysis.All.Count,
            classification.Summary,
            diagnostics);
    }
}

public sealed record BatchRequestRecord(
    int LineNumber,
    string CustomId,
    string Method,
    string Url,
    string BodyRawJson,
    string Fingerprint,
    string RawLine);

public sealed record BatchResultRecord(
    int LineNumber,
    string CustomId,
    int? StatusCode,
    string? ErrorCode,
    string? ErrorMessage,
    string? ProviderRequestId,
    bool HasStructuredError,
    string? ResponseBodyRawJson,
    string Fingerprint,
    string RawLine);

public sealed record DuplicateRequestRecord(
    string CustomId,
    int FirstLineNumber,
    int DuplicateLineNumber,
    bool SameFingerprint,
    string FirstFingerprint,
    string DuplicateFingerprint);

public sealed record UnknownResultRecord(
    int LineNumber,
    string? CustomId,
    string Reason,
    int? StatusCode,
    string? ErrorCode,
    string? ErrorMessage);

public enum BatchOutcomeKind
{
    Succeeded,
    RetryableFailure,
    PermanentFailure,
    MissingResult,
    InputConflict,
}

public enum RetryDecision
{
    NotEligible,
    EligibleForRetry,
    ManualReview,
}

file sealed class RequestFileIndex
{
    private RequestFileIndex(
        IReadOnlyList<BatchRequestRecord> orderedRequests,
        IReadOnlyDictionary<string, BatchRequestRecord> byCustomId,
        IReadOnlyList<DuplicateRequestRecord> duplicateRequests)
    {
        OrderedRequests = orderedRequests;
        ByCustomId = byCustomId;
        DuplicateRequests = duplicateRequests;
    }

    public IReadOnlyList<BatchRequestRecord> OrderedRequests { get; }

    public IReadOnlyDictionary<string, BatchRequestRecord> ByCustomId { get; }

    public IReadOnlyList<DuplicateRequestRecord> DuplicateRequests { get; }

    public static RequestFileIndex Load(string path)
    {
        if (!File.Exists(path))
        {
            throw new CliException("Request file does not exist: " + path);
        }

        var orderedRequests = new List<BatchRequestRecord>();
        var byCustomId = new Dictionary<string, BatchRequestRecord>(StringComparer.Ordinal);
        var duplicateRequests = new List<DuplicateRequestRecord>();
        var lineNumber = 0;

        foreach (var rawLine in File.ReadLines(path))
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(rawLine))
            {
                continue;
            }

            using var document = JsonDocument.Parse(rawLine);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new CliException($"Request line {lineNumber} must be a JSON object.");
            }

            var root = document.RootElement;
            var customId = JsonValueReader.ReadRequiredString(root, "custom_id", lineNumber, "request");
            var method = JsonValueReader.ReadOptionalString(root, "method") ?? "POST";
            var url = JsonValueReader.ReadRequiredString(root, "url", lineNumber, "request");
            if (!root.TryGetProperty("body", out var body))
            {
                throw new CliException($"Request line {lineNumber} is missing body.");
            }

            var bodyRawJson = body.GetRawText();
            var fingerprint = Hashing.ComputeRequestFingerprint(method, url, bodyRawJson);
            var request = new BatchRequestRecord(
                lineNumber,
                customId,
                method.ToUpperInvariant(),
                url,
                bodyRawJson,
                fingerprint,
                rawLine);

            if (byCustomId.TryGetValue(customId, out var existing))
            {
                duplicateRequests.Add(new DuplicateRequestRecord(
                    customId,
                    existing.LineNumber,
                    request.LineNumber,
                    string.Equals(existing.Fingerprint, request.Fingerprint, StringComparison.Ordinal),
                    existing.Fingerprint,
                    request.Fingerprint));
                continue;
            }

            byCustomId.Add(customId, request);
            orderedRequests.Add(request);
        }

        if (orderedRequests.Count == 0)
        {
            throw new CliException("Request file is empty after blank lines were removed.");
        }

        return new RequestFileIndex(orderedRequests, byCustomId, duplicateRequests);
    }
}

file sealed class ResultFileIndex
{
    private ResultFileIndex(
        IReadOnlyDictionary<string, List<BatchResultRecord>> byCustomId,
        IReadOnlyList<UnknownResultRecord> resultsMissingCustomId)
    {
        ByCustomId = byCustomId;
        ResultsMissingCustomId = resultsMissingCustomId;
    }

    public IReadOnlyDictionary<string, List<BatchResultRecord>> ByCustomId { get; }

    public IReadOnlyList<UnknownResultRecord> ResultsMissingCustomId { get; }

    public static ResultFileIndex Load(string path)
    {
        if (!File.Exists(path))
        {
            throw new CliException("Result file does not exist: " + path);
        }

        var byCustomId = new Dictionary<string, List<BatchResultRecord>>(StringComparer.Ordinal);
        var resultsMissingCustomId = new List<UnknownResultRecord>();
        var lineNumber = 0;

        foreach (var rawLine in File.ReadLines(path))
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(rawLine))
            {
                continue;
            }

            using var document = JsonDocument.Parse(rawLine);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new CliException($"Result line {lineNumber} must be a JSON object.");
            }

            var root = document.RootElement;
            var customId = JsonValueReader.ReadOptionalString(root, "custom_id");

            var statusCode =
                JsonValueReader.ReadOptionalInt(root, "status_code") ??
                JsonValueReader.ReadNestedOptionalInt(root, "response", "status_code") ??
                JsonValueReader.ReadNestedOptionalInt(root, "result", "status_code");

            var errorCode =
                JsonValueReader.ReadNestedOptionalString(root, "error", "code") ??
                JsonValueReader.ReadNestedOptionalString(root, "error", "type") ??
                JsonValueReader.ReadNestedOptionalString(root, "response", "body", "error", "code") ??
                JsonValueReader.ReadNestedOptionalString(root, "response", "body", "error", "type") ??
                JsonValueReader.ReadNestedOptionalString(root, "result", "error", "code") ??
                JsonValueReader.ReadNestedOptionalString(root, "result", "error", "type");

            var errorMessage =
                JsonValueReader.ReadNestedOptionalString(root, "error", "message") ??
                JsonValueReader.ReadNestedOptionalString(root, "response", "body", "error", "message") ??
                JsonValueReader.ReadNestedOptionalString(root, "result", "error", "message");

            var providerRequestId =
                JsonValueReader.ReadNestedOptionalString(root, "response", "request_id") ??
                JsonValueReader.ReadNestedOptionalString(root, "result", "request_id") ??
                JsonValueReader.ReadOptionalString(root, "id");

            var responseBodyRawJson =
                JsonValueReader.ReadNestedRawJson(root, "response", "body") ??
                JsonValueReader.ReadNestedRawJson(root, "result", "body");

            var hasStructuredError = !string.IsNullOrWhiteSpace(errorCode) || !string.IsNullOrWhiteSpace(errorMessage);
            var fingerprint = Hashing.ComputeResultFingerprint(statusCode, errorCode, responseBodyRawJson, errorMessage);

            if (string.IsNullOrWhiteSpace(customId))
            {
                resultsMissingCustomId.Add(new UnknownResultRecord(
                    lineNumber,
                    null,
                    "Result line is missing custom_id.",
                    statusCode,
                    errorCode,
                    errorMessage));
                continue;
            }

            var record = new BatchResultRecord(
                lineNumber,
                customId,
                statusCode,
                errorCode,
                errorMessage,
                providerRequestId,
                hasStructuredError,
                responseBodyRawJson,
                fingerprint,
                rawLine);

            if (!byCustomId.TryGetValue(customId, out var bucket))
            {
                bucket = new List<BatchResultRecord>();
                byCustomId.Add(customId, bucket);
            }

            bucket.Add(record);
        }

        return new ResultFileIndex(byCustomId, resultsMissingCustomId);
    }
}

file sealed class DuplicateResultAnalysis
{
    private DuplicateResultAnalysis(
        IReadOnlyList<BatchResultRecord> all,
        BatchResultRecord selected,
        IReadOnlyList<string> conflictingDuplicates,
        bool requiresManualReview)
    {
        All = all;
        Selected = selected;
        ConflictingDuplicates = conflictingDuplicates;
        RequiresManualReview = requiresManualReview;
    }

    public IReadOnlyList<BatchResultRecord> All { get; }

    public BatchResultRecord Selected { get; }

    public IReadOnlyList<string> ConflictingDuplicates { get; }

    public bool RequiresManualReview { get; }

    public static DuplicateResultAnalysis Create(IReadOnlyList<BatchResultRecord> results)
    {
        if (results.Count == 0)
        {
            throw new InvalidOperationException("Duplicate result analysis requires at least one result.");
        }

        if (results.Count == 1)
        {
            return new DuplicateResultAnalysis(results, results[0], Array.Empty<string>(), requiresManualReview: false);
        }

        var conflicts = new List<string>();
        var first = results[0];
        var hasConflict = false;

        for (var index = 1; index < results.Count; index++)
        {
            var candidate = results[index];
            if (string.Equals(first.Fingerprint, candidate.Fingerprint, StringComparison.Ordinal))
            {
                continue;
            }

            hasConflict = true;
            conflicts.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"Result lines {first.LineNumber} and {candidate.LineNumber} disagree for custom_id '{first.CustomId}'."));
        }

        var selected = SelectPreferred(results);
        return new DuplicateResultAnalysis(results, selected, conflicts, hasConflict);
    }

    private static BatchResultRecord SelectPreferred(IReadOnlyList<BatchResultRecord> results)
    {
        return results
            .OrderByDescending(result => Score(result))
            .ThenBy(result => result.LineNumber)
            .First();
    }

    private static int Score(BatchResultRecord result)
    {
        if (result.StatusCode is >= 200 and < 300 && !result.HasStructuredError)
        {
            return 3;
        }

        if (RetryClassifier.LooksRetryable(result))
        {
            return 2;
        }

        return 1;
    }
}

file sealed class ResultClassification
{
    public ResultClassification(
        BatchOutcomeKind outcome,
        RetryDecision retryDecision,
        bool requiresManualReview,
        string summary,
        string reason)
    {
        Outcome = outcome;
        RetryDecision = retryDecision;
        RequiresManualReview = requiresManualReview;
        Summary = summary;
        Reason = reason;
    }

    public BatchOutcomeKind Outcome { get; }

    public RetryDecision RetryDecision { get; }

    public bool RequiresManualReview { get; }

    public string Summary { get; }

    public string Reason { get; }
}

file static class RetryClassifier
{
    public static ResultClassification Classify(BatchResultRecord result, BatchReconcilerOptions options)
    {
        if (result.StatusCode is >= 200 and < 300 && !result.HasStructuredError)
        {
            return new ResultClassification(
                BatchOutcomeKind.Succeeded,
                RetryDecision.NotEligible,
                requiresManualReview: false,
                "Completed successfully.",
                "Selected result line returned a 2xx response without a structured error payload.");
        }

        if (LooksRetryable(result, options))
        {
            return new ResultClassification(
                BatchOutcomeKind.RetryableFailure,
                RetryDecision.EligibleForRetry,
                requiresManualReview: false,
                BuildRetryableSummary(result),
                BuildRetryableReason(result));
        }

        if (LooksPermanent(result, options))
        {
            return new ResultClassification(
                BatchOutcomeKind.PermanentFailure,
                RetryDecision.NotEligible,
                requiresManualReview: false,
                BuildPermanentSummary(result),
                BuildPermanentReason(result));
        }

        return new ResultClassification(
            BatchOutcomeKind.PermanentFailure,
            RetryDecision.ManualReview,
            requiresManualReview: true,
            "Result could not be confidently classified as transient or permanent.",
            "The result shape does not match the retry classifier rules. Review the raw result before replaying.");
    }

    public static bool LooksRetryable(BatchResultRecord result)
    {
        return result.StatusCode is 408 or 409 or 425 or 429 or 500 or 502 or 503 or 504 or 529;
    }

    private static bool LooksRetryable(BatchResultRecord result, BatchReconcilerOptions options)
    {
        if (result.StatusCode is int statusCode && options.RetryableStatusCodes.Contains(statusCode))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(result.ErrorCode) && options.RetryableErrorCodes.Contains(result.ErrorCode))
        {
            return true;
        }

        var message = result.ErrorMessage ?? string.Empty;
        return message.Contains("timeout", StringComparison.OrdinalIgnoreCase)
            || message.Contains("temporarily unavailable", StringComparison.OrdinalIgnoreCase)
            || message.Contains("try again", StringComparison.OrdinalIgnoreCase)
            || message.Contains("overloaded", StringComparison.OrdinalIgnoreCase);
    }

    private static bool LooksPermanent(BatchResultRecord result, BatchReconcilerOptions options)
    {
        if (!string.IsNullOrWhiteSpace(result.ErrorCode) && options.PermanentErrorCodes.Contains(result.ErrorCode))
        {
            return true;
        }

        if (result.StatusCode is int statusCode)
        {
            if (statusCode is >= 400 and < 500 && !options.RetryableStatusCodes.Contains(statusCode))
            {
                return true;
            }
        }

        var message = result.ErrorMessage ?? string.Empty;
        return message.Contains("invalid", StringComparison.OrdinalIgnoreCase)
            || message.Contains("unsupported", StringComparison.OrdinalIgnoreCase)
            || message.Contains("permission", StringComparison.OrdinalIgnoreCase)
            || message.Contains("quota", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase);
    }

    private static string BuildRetryableSummary(BatchResultRecord result)
    {
        if (result.StatusCode is int statusCode)
        {
            return string.Create(CultureInfo.InvariantCulture, $"Transient failure ({statusCode}). Safe to include in a retry batch.");
        }

        return "Transient failure without HTTP status. Safe to include in a retry batch.";
    }

    private static string BuildRetryableReason(BatchResultRecord result)
    {
        var builder = new StringBuilder();
        if (result.StatusCode is int statusCode)
        {
            builder.Append("Status code ").Append(statusCode).Append(" matches the retryable status policy.");
        }
        else
        {
            builder.Append("Error code or message matches the retryable policy.");
        }

        if (!string.IsNullOrWhiteSpace(result.ErrorCode))
        {
            builder.Append(" Error code: ").Append(result.ErrorCode).Append('.');
        }

        if (!string.IsNullOrWhiteSpace(result.ErrorMessage))
        {
            builder.Append(" Message: ").Append(result.ErrorMessage);
        }

        return builder.ToString();
    }

    private static string BuildPermanentSummary(BatchResultRecord result)
    {
        if (!string.IsNullOrWhiteSpace(result.ErrorCode))
        {
            return string.Create(CultureInfo.InvariantCulture, $"Permanent failure ({result.ErrorCode}). Retrying would likely repeat the same rejection.");
        }

        if (result.StatusCode is int statusCode)
        {
            return string.Create(CultureInfo.InvariantCulture, $"Permanent failure ({statusCode}). Retrying would likely repeat the same rejection.");
        }

        return "Permanent failure. Retrying would likely repeat the same rejection.";
    }

    private static string BuildPermanentReason(BatchResultRecord result)
    {
        var builder = new StringBuilder("Failure matched the permanent policy.");
        if (result.StatusCode is int statusCode)
        {
            builder.Append(" Status code: ").Append(statusCode).Append('.');
        }

        if (!string.IsNullOrWhiteSpace(result.ErrorCode))
        {
            builder.Append(" Error code: ").Append(result.ErrorCode).Append('.');
        }

        if (!string.IsNullOrWhiteSpace(result.ErrorMessage))
        {
            builder.Append(" Message: ").Append(result.ErrorMessage);
        }

        return builder.ToString();
    }
}

file static class JsonValueReader
{
    public static string ReadRequiredString(JsonElement root, string propertyName, int lineNumber, string fileKind)
    {
        var value = ReadOptionalString(root, propertyName);
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new CliException($"{fileKind} line {lineNumber} is missing required string property '{propertyName}'.");
        }

        return value;
    }

    public static string? ReadOptionalString(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var element))
        {
            return null;
        }

        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null,
        };
    }

    public static int? ReadOptionalInt(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var element))
        {
            return null;
        }

        return element.ValueKind == JsonValueKind.Number && element.TryGetInt32(out var value)
            ? value
            : null;
    }

    public static string? ReadNestedOptionalString(JsonElement root, params string[] path)
    {
        var element = TryResolve(root, path);
        return element is null ? null : ReadStringFromElement(element.Value);
    }

    public static int? ReadNestedOptionalInt(JsonElement root, params string[] path)
    {
        var element = TryResolve(root, path);
        if (element is null)
        {
            return null;
        }

        return element.Value.ValueKind == JsonValueKind.Number && element.Value.TryGetInt32(out var value)
            ? value
            : null;
    }

    public static string? ReadNestedRawJson(JsonElement root, params string[] path)
    {
        var element = TryResolve(root, path);
        return element?.GetRawText();
    }

    private static JsonElement? TryResolve(JsonElement root, params string[] path)
    {
        var current = root;
        foreach (var segment in path)
        {
            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(segment, out current))
            {
                return null;
            }
        }

        return current;
    }

    private static string? ReadStringFromElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null,
        };
    }
}

file static class JsonCanonicalizer
{
    public static string Canonicalize(string rawJson)
    {
        using var document = JsonDocument.Parse(rawJson);
        var buffer = new ArrayBufferWriter<byte>(Math.Max(rawJson.Length, 128));
        using var writer = new Utf8JsonWriter(buffer);
        WriteElement(document.RootElement, writer);
        writer.Flush();
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static void WriteElement(JsonElement element, Utf8JsonWriter writer)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var property in element.EnumerateObject().OrderBy(property => property.Name, StringComparer.Ordinal))
                {
                    writer.WritePropertyName(property.Name);
                    WriteElement(property.Value, writer);
                }

                writer.WriteEndObject();
                break;
            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var item in element.EnumerateArray())
                {
                    WriteElement(item, writer);
                }

                writer.WriteEndArray();
                break;
            case JsonValueKind.String:
                writer.WriteStringValue(element.GetString());
                break;
            case JsonValueKind.Number:
                writer.WriteRawValue(element.GetRawText());
                break;
            case JsonValueKind.True:
                writer.WriteBooleanValue(true);
                break;
            case JsonValueKind.False:
                writer.WriteBooleanValue(false);
                break;
            case JsonValueKind.Null:
                writer.WriteNullValue();
                break;
            default:
                writer.WriteStringValue(element.GetRawText());
                break;
        }
    }
}

file static class Hashing
{
    public static string ComputeRequestFingerprint(string method, string url, string bodyRawJson)
    {
        return ComputeSha256Hex(string.Concat(
            method.Trim().ToUpperInvariant(),
            "\n",
            url.Trim(),
            "\n",
            JsonCanonicalizer.Canonicalize(bodyRawJson)));
    }

    public static string ComputeResultFingerprint(int? statusCode, string? errorCode, string? responseBodyRawJson, string? errorMessage)
    {
        var canonicalBody = string.IsNullOrWhiteSpace(responseBodyRawJson)
            ? "<none>"
            : JsonCanonicalizer.Canonicalize(responseBodyRawJson);

        return ComputeSha256Hex(string.Concat(
            statusCode?.ToString(CultureInfo.InvariantCulture) ?? "<null>",
            "\n",
            errorCode ?? "<null>",
            "\n",
            errorMessage ?? "<null>",
            "\n",
            canonicalBody));
    }

    private static string ComputeSha256Hex(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash);
    }
}

file sealed class CliException : Exception
{
    public CliException(string message)
        : base(message)
    {
    }
}

/*
This solves a very practical OpenAI Batch API and Azure OpenAI batch JSONL problem: you submit thousands of inference requests, get one or more result files back, and then realize the request ledger and result ledger no longer match cleanly. Built because real April 2026 LLM data pipelines fail in ugly ways, not in textbook ways. You get missing result lines, duplicate result rows, mixed transient and permanent errors, provider-side retries, partial exports, and stale replay files that make people manually diff JSONL in production. Use it when you need a C# batch result reconciler, retry JSONL generator, duplicate detector, and failure classifier for offline inference, embeddings, eval backfills, synthetic data generation, or large response-format migrations. The trick: it canonicalizes JSON before hashing, so duplicate payloads and conflicting payloads are separated correctly, and it refuses to silently auto-retry entries that still need manual review. Drop this into any .NET 8 worker, console app, backend service, CI check, or research pipeline when you want a production-ready LLM batch reconciliation utility that is easy to fork, easy to trust, and immediately useful for real AI infrastructure work.
*/