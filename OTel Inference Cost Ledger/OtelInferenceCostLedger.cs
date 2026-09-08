#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace VibeCode;

/// <summary>
/// Builds a cost and latency ledger by joining OpenTelemetry-style spans with LLM gateway usage records.
/// The tool is intentionally dependency-free so it can run inside incident containers, CI jobs, and locked-down build agents.
/// </summary>
public static class OtelInferenceCostLedger
{
    private const int ExitOk = 0;
    private const int ExitInputError = 1;
    private const int ExitGuardFailed = 2;

    public static int Main(string[] args)
    {
        try
        {
            var options = CliOptions.Parse(args);
            if (options.ShowHelp)
            {
                PrintHelp();
                return ExitOk;
            }

            if (options.PrintPriceTemplate)
            {
                PrintPriceTemplate();
                return ExitOk;
            }

            var spans = SpanIndex.Load(options.SpansPath!);
            var usage = UsageFile.Load(options.UsagePath!);
            var prices = PriceBook.Load(options.PricesPath);
            var report = CostAttributor.Build(spans, usage, prices, options);

            Console.Out.Write(report.ToHumanText(options.Top));
            if (!string.IsNullOrWhiteSpace(options.JsonOutputPath))
            {
                report.WriteJson(options.JsonOutputPath!);
            }

            return report.HasGuardFailure ? ExitGuardFailed : ExitOk;
        }
        catch (CliException ex)
        {
            Console.Error.WriteLine(ex.Message);
            Console.Error.WriteLine("Use --help for usage.");
            return ExitInputError;
        }
        catch (JsonException ex)
        {
            Console.Error.WriteLine("Invalid JSON input: " + ex.Message);
            return ExitInputError;
        }
        catch (IOException ex)
        {
            Console.Error.WriteLine("I/O failure: " + ex.Message);
            return ExitInputError;
        }
    }

    private static void PrintHelp()
    {
        Console.WriteLine("OtelInferenceCostLedger");
        Console.WriteLine("Join OpenTelemetry span JSONL with LLM gateway usage JSONL and attribute token cost to service, route, model, and tenant.");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run -- --spans spans.jsonl --usage usage.jsonl [options]");
        Console.WriteLine();
        Console.WriteLine("Required:");
        Console.WriteLine("  --spans <path>             JSONL file containing flattened span records.");
        Console.WriteLine("  --usage <path>             JSONL file containing model usage records.");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --prices <path>            Optional model price book JSON. Provider cost fields are used first when present.");
        Console.WriteLine("  --json-out <path>          Optional machine-readable report path.");
        Console.WriteLine("  --latency-slo-ms <n>       Latency SLO for attributed calls. Default: 30000.");
        Console.WriteLine("  --top <n>                  Number of expensive groups shown in the text report. Default: 20.");
        Console.WriteLine("  --fail-on-unmatched        Exit 2 when any usage record cannot be matched to a span.");
        Console.WriteLine("  --fail-on-missing-price    Exit 2 when any priced model is missing price data and provider cost is absent.");
        Console.WriteLine("  --fail-on-slo-breach       Exit 2 when any attributed call exceeds --latency-slo-ms.");
        Console.WriteLine("  --price-template           Print a price book template and exit.");
        Console.WriteLine("  --help                     Show this help text.");
        Console.WriteLine();
        Console.WriteLine("Useful span attributes: service.name, http.route, tenant.id, enduser.id, gen_ai.request.model, gen_ai.response.model,");
        Console.WriteLine("gen_ai.request.id, llm.request_id, openai.request_id, trace_id, span_id, and duration_ms.");
    }

    private static void PrintPriceTemplate()
    {
        Console.WriteLine("{");
        Console.WriteLine("  \"gpt-4.1-mini\": { \"input_per_million\": 0.00, \"cached_input_per_million\": 0.00, \"output_per_million\": 0.00 },");
        Console.WriteLine("  \"claude-3-7-sonnet\": { \"input_per_million\": 0.00, \"cached_input_per_million\": 0.00, \"output_per_million\": 0.00 },");
        Console.WriteLine("  \"gemini-2.5-pro\": { \"input_per_million\": 0.00, \"cached_input_per_million\": 0.00, \"output_per_million\": 0.00 }");
        Console.WriteLine("}");
    }
}

file sealed class CliOptions
{
    public string? SpansPath { get; private init; }
    public string? UsagePath { get; private init; }
    public string? PricesPath { get; private init; }
    public string? JsonOutputPath { get; private init; }
    public int LatencySloMs { get; private init; } = 30_000;
    public int Top { get; private init; } = 20;
    public bool FailOnUnmatched { get; private init; }
    public bool FailOnMissingPrice { get; private init; }
    public bool FailOnSloBreach { get; private init; }
    public bool PrintPriceTemplate { get; private init; }
    public bool ShowHelp { get; private init; }

    public static CliOptions Parse(string[] args)
    {
        string? spansPath = null;
        string? usagePath = null;
        string? pricesPath = null;
        string? jsonOutputPath = null;
        var latencySloMs = 30_000;
        var top = 20;
        var failOnUnmatched = false;
        var failOnMissingPrice = false;
        var failOnSloBreach = false;
        var priceTemplate = false;
        var help = false;

        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--spans":
                    spansPath = ReadValue(args, ref index, "--spans");
                    break;
                case "--usage":
                    usagePath = ReadValue(args, ref index, "--usage");
                    break;
                case "--prices":
                    pricesPath = ReadValue(args, ref index, "--prices");
                    break;
                case "--json-out":
                    jsonOutputPath = ReadValue(args, ref index, "--json-out");
                    break;
                case "--latency-slo-ms":
                    latencySloMs = ParsePositiveInt(ReadValue(args, ref index, "--latency-slo-ms"), "--latency-slo-ms");
                    break;
                case "--top":
                    top = ParsePositiveInt(ReadValue(args, ref index, "--top"), "--top");
                    break;
                case "--fail-on-unmatched":
                    failOnUnmatched = true;
                    break;
                case "--fail-on-missing-price":
                    failOnMissingPrice = true;
                    break;
                case "--fail-on-slo-breach":
                    failOnSloBreach = true;
                    break;
                case "--price-template":
                    priceTemplate = true;
                    break;
                case "--help":
                case "-h":
                    help = true;
                    break;
                default:
                    throw new CliException("Unknown argument: " + args[index]);
            }
        }

        if (!help && !priceTemplate)
        {
            if (string.IsNullOrWhiteSpace(spansPath))
            {
                throw new CliException("Missing required argument --spans.");
            }

            if (string.IsNullOrWhiteSpace(usagePath))
            {
                throw new CliException("Missing required argument --usage.");
            }
        }

        return new CliOptions
        {
            SpansPath = spansPath,
            UsagePath = usagePath,
            PricesPath = pricesPath,
            JsonOutputPath = jsonOutputPath,
            LatencySloMs = latencySloMs,
            Top = top,
            FailOnUnmatched = failOnUnmatched,
            FailOnMissingPrice = failOnMissingPrice,
            FailOnSloBreach = failOnSloBreach,
            PrintPriceTemplate = priceTemplate,
            ShowHelp = help,
        };
    }

    private static string ReadValue(string[] args, ref int index, string flag)
    {
        if (index + 1 >= args.Length || args[index + 1].StartsWith("--", StringComparison.Ordinal))
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

file sealed class SpanIndex
{
    private readonly IReadOnlyList<TraceSpan> _spans;
    private readonly Dictionary<string, List<TraceSpan>> _byRequestId;
    private readonly Dictionary<string, List<TraceSpan>> _byTraceId;
    private readonly Dictionary<string, TraceSpan> _byTraceAndSpan;

    private SpanIndex(IReadOnlyList<TraceSpan> spans)
    {
        _spans = spans;
        _byRequestId = new Dictionary<string, List<TraceSpan>>(StringComparer.OrdinalIgnoreCase);
        _byTraceId = new Dictionary<string, List<TraceSpan>>(StringComparer.OrdinalIgnoreCase);
        _byTraceAndSpan = new Dictionary<string, TraceSpan>(StringComparer.OrdinalIgnoreCase);

        foreach (var span in spans)
        {
            Add(_byTraceId, span.TraceId, span);
            Add(_byRequestId, span.RequestId, span);

            if (!string.IsNullOrWhiteSpace(span.TraceId) && !string.IsNullOrWhiteSpace(span.SpanId))
            {
                var key = JoinKey(span.TraceId!, span.SpanId!);
                _byTraceAndSpan.TryAdd(key, span);
            }
        }
    }

    public int Count => _spans.Count;

    public static SpanIndex Load(string path)
    {
        if (!File.Exists(path))
        {
            throw new CliException("Span file does not exist: " + path);
        }

        var spans = new List<TraceSpan>();
        var lineNumber = 0;
        foreach (var line in File.ReadLines(path))
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            using var document = JsonDocument.Parse(line);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new CliException($"Span line {lineNumber} must be a JSON object.");
            }

            spans.Add(TraceSpan.FromJson(document.RootElement, lineNumber));
        }

        return new SpanIndex(spans);
    }

    public TraceSpan? FindBest(UsageRecord usage)
    {
        if (!string.IsNullOrWhiteSpace(usage.TraceId) && !string.IsNullOrWhiteSpace(usage.SpanId))
        {
            if (_byTraceAndSpan.TryGetValue(JoinKey(usage.TraceId!, usage.SpanId!), out var exact))
            {
                return exact;
            }
        }

        if (!string.IsNullOrWhiteSpace(usage.RequestId) && _byRequestId.TryGetValue(usage.RequestId!, out var byRequest))
        {
            return PickBest(byRequest, usage);
        }

        if (!string.IsNullOrWhiteSpace(usage.TraceId) && _byTraceId.TryGetValue(usage.TraceId!, out var byTrace))
        {
            return PickBest(byTrace, usage);
        }

        return null;
    }

    private static TraceSpan PickBest(IReadOnlyList<TraceSpan> candidates, UsageRecord usage)
    {
        return candidates
            .OrderByDescending(span => Score(span, usage))
            .ThenByDescending(span => span.LatencyMs ?? 0)
            .ThenBy(span => span.LineNumber)
            .First();
    }

    private static int Score(TraceSpan span, UsageRecord usage)
    {
        var score = 0;
        if (!string.IsNullOrWhiteSpace(usage.RequestId) && StringEquals(span.RequestId, usage.RequestId))
        {
            score += 100;
        }

        if (!string.IsNullOrWhiteSpace(usage.TraceId) && StringEquals(span.TraceId, usage.TraceId))
        {
            score += 40;
        }

        if (!string.IsNullOrWhiteSpace(usage.Model) && StringEquals(span.Model, usage.Model))
        {
            score += 25;
        }

        if (span.IsInferenceLike)
        {
            score += 15;
        }

        if (!string.IsNullOrWhiteSpace(span.Service))
        {
            score += 4;
        }

        return score;
    }

    private static void Add(Dictionary<string, List<TraceSpan>> map, string? key, TraceSpan span)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return;
        }

        if (!map.TryGetValue(key, out var list))
        {
            list = new List<TraceSpan>();
            map.Add(key, list);
        }

        list.Add(span);
    }

    private static bool StringEquals(string? left, string? right)
    {
        return string.Equals(left, right, StringComparison.OrdinalIgnoreCase);
    }

    private static string JoinKey(string traceId, string spanId)
    {
        return traceId + ":" + spanId;
    }
}

file sealed class TraceSpan
{
    private TraceSpan(
        int lineNumber,
        string? traceId,
        string? spanId,
        string? requestId,
        string service,
        string route,
        string tenant,
        string? model,
        string operation,
        string status,
        double? latencyMs,
        bool isInferenceLike)
    {
        LineNumber = lineNumber;
        TraceId = NormalizeId(traceId);
        SpanId = NormalizeId(spanId);
        RequestId = NormalizeId(requestId);
        Service = service;
        Route = route;
        Tenant = tenant;
        Model = model;
        Operation = operation;
        Status = status;
        LatencyMs = latencyMs;
        IsInferenceLike = isInferenceLike;
    }

    public int LineNumber { get; }
    public string? TraceId { get; }
    public string? SpanId { get; }
    public string? RequestId { get; }
    public string Service { get; }
    public string Route { get; }
    public string Tenant { get; }
    public string? Model { get; }
    public string Operation { get; }
    public string Status { get; }
    public double? LatencyMs { get; }
    public bool IsInferenceLike { get; }

    public static TraceSpan FromJson(JsonElement root, int lineNumber)
    {
        var attributes = AttributeReader.ReadAll(root);
        var traceId = JsonAccess.GetString(root, "trace_id", "traceId", "context/trace_id") ?? Attr(attributes, "trace_id");
        var spanId = JsonAccess.GetString(root, "span_id", "spanId", "context/span_id") ?? Attr(attributes, "span_id");
        var requestId =
            Attr(attributes, "gen_ai.request.id") ??
            Attr(attributes, "llm.request_id") ??
            Attr(attributes, "openai.request_id") ??
            Attr(attributes, "request.id") ??
            Attr(attributes, "http.request_id") ??
            Attr(attributes, "x-request-id") ??
            JsonAccess.GetString(root, "request_id", "requestId", "id");

        var service =
            Attr(attributes, "service.name") ??
            JsonAccess.GetString(root, "service_name", "serviceName", "resource/service.name") ??
            "unknown-service";

        var route =
            Attr(attributes, "http.route") ??
            Attr(attributes, "url.path") ??
            Attr(attributes, "http.target") ??
            Attr(attributes, "rpc.method") ??
            JsonAccess.GetString(root, "route", "name") ??
            "unknown-route";

        var tenant =
            Attr(attributes, "tenant.id") ??
            Attr(attributes, "organization.id") ??
            Attr(attributes, "enduser.id") ??
            Attr(attributes, "user.id") ??
            JsonAccess.GetString(root, "tenant", "tenant_id") ??
            "unknown-tenant";

        var model =
            Attr(attributes, "gen_ai.response.model") ??
            Attr(attributes, "gen_ai.request.model") ??
            Attr(attributes, "llm.model") ??
            Attr(attributes, "ai.model") ??
            JsonAccess.GetString(root, "model", "model_name", "response/model");

        var operation =
            Attr(attributes, "gen_ai.operation.name") ??
            Attr(attributes, "llm.operation") ??
            JsonAccess.GetString(root, "name", "operation") ??
            "unknown-operation";

        var status =
            Attr(attributes, "otel.status_code") ??
            JsonAccess.GetString(root, "status/code", "status", "statusCode") ??
            "unknown";

        var latencyMs =
            JsonAccess.GetDouble(root, "duration_ms", "latency_ms", "elapsed_ms") ??
            TryParseDouble(Attr(attributes, "duration_ms")) ??
            TryParseDouble(Attr(attributes, "gen_ai.usage.latency_ms")) ??
            DurationFromTimestamps(root);

        var inferenceLike =
            !string.IsNullOrWhiteSpace(model) ||
            attributes.Keys.Any(key => key.StartsWith("gen_ai.", StringComparison.OrdinalIgnoreCase) || key.StartsWith("llm.", StringComparison.OrdinalIgnoreCase)) ||
            operation.Contains("responses", StringComparison.OrdinalIgnoreCase) ||
            operation.Contains("chat.completions", StringComparison.OrdinalIgnoreCase) ||
            operation.Contains("embedding", StringComparison.OrdinalIgnoreCase) ||
            operation.Contains("inference", StringComparison.OrdinalIgnoreCase);

        return new TraceSpan(lineNumber, traceId, spanId, requestId, service, route, tenant, model, operation, status, latencyMs, inferenceLike);
    }

    private static string? Attr(IReadOnlyDictionary<string, string> attributes, string key)
    {
        return attributes.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;
    }

    private static double? DurationFromTimestamps(JsonElement root)
    {
        var startNs = JsonAccess.GetInt64(root, "start_time_unix_nano", "startTimeUnixNano", "start_unix_nano");
        var endNs = JsonAccess.GetInt64(root, "end_time_unix_nano", "endTimeUnixNano", "end_unix_nano");
        if (startNs is not null && endNs is not null && endNs >= startNs)
        {
            return (endNs.Value - startNs.Value) / 1_000_000.0;
        }

        var start = JsonAccess.GetDateTime(root, "start_time", "startTime", "timestamp");
        var end = JsonAccess.GetDateTime(root, "end_time", "endTime");
        if (start is not null && end is not null && end >= start)
        {
            return (end.Value - start.Value).TotalMilliseconds;
        }

        return null;
    }

    private static double? TryParseDouble(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) ? parsed : null;
    }

    private static string? NormalizeId(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}

file sealed class UsageFile
{
    private UsageFile(IReadOnlyList<UsageRecord> records)
    {
        Records = records;
    }

    public IReadOnlyList<UsageRecord> Records { get; }

    public static UsageFile Load(string path)
    {
        if (!File.Exists(path))
        {
            throw new CliException("Usage file does not exist: " + path);
        }

        var records = new List<UsageRecord>();
        var lineNumber = 0;
        foreach (var line in File.ReadLines(path))
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            using var document = JsonDocument.Parse(line);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new CliException($"Usage line {lineNumber} must be a JSON object.");
            }

            records.Add(UsageRecord.FromJson(document.RootElement, lineNumber));
        }

        if (records.Count == 0)
        {
            throw new CliException("Usage file contains no JSON records.");
        }

        return new UsageFile(records);
    }
}

file sealed class UsageRecord
{
    private UsageRecord(
        int lineNumber,
        string? traceId,
        string? spanId,
        string? requestId,
        string? model,
        long inputTokens,
        long cachedInputTokens,
        long outputTokens,
        long totalTokens,
        double? latencyMs,
        string status,
        decimal? providerCost)
    {
        LineNumber = lineNumber;
        TraceId = Normalize(traceId);
        SpanId = Normalize(spanId);
        RequestId = Normalize(requestId);
        Model = Normalize(model);
        InputTokens = Math.Max(0, inputTokens);
        CachedInputTokens = Math.Max(0, Math.Min(cachedInputTokens, InputTokens));
        OutputTokens = Math.Max(0, outputTokens);
        TotalTokens = Math.Max(totalTokens, InputTokens + OutputTokens);
        LatencyMs = latencyMs;
        Status = string.IsNullOrWhiteSpace(status) ? "unknown" : status;
        ProviderCost = providerCost;
    }

    public int LineNumber { get; }
    public string? TraceId { get; }
    public string? SpanId { get; }
    public string? RequestId { get; }
    public string? Model { get; }
    public long InputTokens { get; }
    public long CachedInputTokens { get; }
    public long OutputTokens { get; }
    public long TotalTokens { get; }
    public double? LatencyMs { get; }
    public string Status { get; }
    public decimal? ProviderCost { get; }

    public static UsageRecord FromJson(JsonElement root, int lineNumber)
    {
        var attributes = AttributeReader.ReadAll(root);
        var traceId = JsonAccess.GetString(root, "trace_id", "traceId") ?? Attr(attributes, "trace_id");
        var spanId = JsonAccess.GetString(root, "span_id", "spanId") ?? Attr(attributes, "span_id");
        var requestId =
            JsonAccess.GetString(root, "request_id", "requestId", "id", "custom_id", "response/request_id") ??
            Attr(attributes, "gen_ai.request.id") ??
            Attr(attributes, "llm.request_id") ??
            Attr(attributes, "openai.request_id");
        var model =
            JsonAccess.GetString(root, "model", "model_name", "response/model", "usage/model") ??
            Attr(attributes, "gen_ai.response.model") ??
            Attr(attributes, "gen_ai.request.model") ??
            Attr(attributes, "llm.model");

        var inputTokens =
            JsonAccess.GetInt64(root, "input_tokens", "prompt_tokens", "usage/input_tokens", "usage/prompt_tokens") ?? 0;
        var outputTokens =
            JsonAccess.GetInt64(root, "output_tokens", "completion_tokens", "usage/output_tokens", "usage/completion_tokens") ?? 0;
        var cachedInputTokens =
            JsonAccess.GetInt64(root, "cached_input_tokens", "usage/cached_input_tokens", "usage/input_tokens_details/cached_tokens", "usage/prompt_tokens_details/cached_tokens") ?? 0;
        var totalTokens =
            JsonAccess.GetInt64(root, "total_tokens", "usage/total_tokens") ?? inputTokens + outputTokens;
        var latencyMs = JsonAccess.GetDouble(root, "latency_ms", "duration_ms", "usage/latency_ms") ?? TryParseDouble(Attr(attributes, "duration_ms"));
        var status = JsonAccess.GetString(root, "status", "finish_reason", "response/status") ?? Attr(attributes, "otel.status_code") ?? "unknown";
        var cost = JsonAccess.GetDecimal(root, "cost", "usd_cost", "usage/cost", "billing/cost_usd");

        return new UsageRecord(lineNumber, traceId, spanId, requestId, model, inputTokens, cachedInputTokens, outputTokens, totalTokens, latencyMs, status, cost);
    }

    private static string? Attr(IReadOnlyDictionary<string, string> attributes, string key)
    {
        return attributes.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;
    }

    private static double? TryParseDouble(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) ? parsed : null;
    }

    private static string? Normalize(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}

file sealed class PriceBook
{
    private readonly Dictionary<string, ModelPrice> _prices;

    private PriceBook(Dictionary<string, ModelPrice> prices)
    {
        _prices = prices;
    }

    public int Count => _prices.Count;

    public static PriceBook Load(string? path)
    {
        var prices = new Dictionary<string, ModelPrice>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(path))
        {
            return new PriceBook(prices);
        }

        if (!File.Exists(path))
        {
            throw new CliException("Price book does not exist: " + path);
        }

        using var document = JsonDocument.Parse(File.ReadAllText(path));
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new CliException("Price book must be a JSON object keyed by model name.");
        }

        foreach (var modelProperty in document.RootElement.EnumerateObject())
        {
            if (modelProperty.Value.ValueKind != JsonValueKind.Object)
            {
                throw new CliException("Price entry for " + modelProperty.Name + " must be a JSON object.");
            }

            var entry = modelProperty.Value;
            var input = JsonAccess.GetDecimal(entry, "input_per_million", "input_per_1m", "inputPerMillion") ?? 0m;
            var cached = JsonAccess.GetDecimal(entry, "cached_input_per_million", "cached_input_per_1m", "cachedInputPerMillion") ?? input;
            var output = JsonAccess.GetDecimal(entry, "output_per_million", "output_per_1m", "outputPerMillion") ?? 0m;
            prices[modelProperty.Name] = new ModelPrice(input, cached, output);
        }

        return new PriceBook(prices);
    }

    public ModelPrice? Find(string? model)
    {
        if (string.IsNullOrWhiteSpace(model))
        {
            return null;
        }

        if (_prices.TryGetValue(model, out var exact))
        {
            return exact;
        }

        var normalized = NormalizeModel(model);
        return _prices.TryGetValue(normalized, out var normalizedPrice) ? normalizedPrice : null;
    }

    private static string NormalizeModel(string model)
    {
        var value = model.Trim();
        var slash = value.LastIndexOf('/');
        if (slash >= 0 && slash + 1 < value.Length)
        {
            value = value[(slash + 1)..];
        }

        var colon = value.IndexOf(':', StringComparison.Ordinal);
        return colon > 0 ? value[..colon] : value;
    }
}

file sealed record ModelPrice(decimal InputPerMillion, decimal CachedInputPerMillion, decimal OutputPerMillion)
{
    public decimal Estimate(long inputTokens, long cachedInputTokens, long outputTokens)
    {
        var cached = Math.Max(0, Math.Min(cachedInputTokens, inputTokens));
        var uncached = Math.Max(0, inputTokens - cached);
        return (uncached / 1_000_000m * InputPerMillion) +
               (cached / 1_000_000m * CachedInputPerMillion) +
               (Math.Max(0, outputTokens) / 1_000_000m * OutputPerMillion);
    }
}

file static class CostAttributor
{
    public static LedgerReport Build(SpanIndex spans, UsageFile usage, PriceBook prices, CliOptions options)
    {
        var entries = new List<LedgerEntry>(usage.Records.Count);
        foreach (var record in usage.Records)
        {
            var span = spans.FindBest(record);
            var model = record.Model ?? span?.Model;
            var price = prices.Find(model);
            var providerCost = record.ProviderCost;
            var estimatedCost = providerCost ?? price?.Estimate(record.InputTokens, record.CachedInputTokens, record.OutputTokens);
            var costSource = providerCost is not null ? "provider" : price is not null ? "price-book" : "missing-price";
            var latencyMs = record.LatencyMs ?? span?.LatencyMs;
            var matched = span is not null;
            var sloBreached = latencyMs is not null && latencyMs.Value > options.LatencySloMs;

            entries.Add(new LedgerEntry(
                record.LineNumber,
                matched,
                span?.LineNumber,
                record.TraceId ?? span?.TraceId,
                record.SpanId ?? span?.SpanId,
                record.RequestId ?? span?.RequestId,
                span?.Service ?? "unmatched-service",
                span?.Route ?? "unmatched-route",
                span?.Tenant ?? "unmatched-tenant",
                model ?? "unknown-model",
                record.InputTokens,
                record.CachedInputTokens,
                record.OutputTokens,
                record.TotalTokens,
                latencyMs,
                estimatedCost,
                costSource,
                record.Status,
                sloBreached));
        }

        var groups = entries
            .GroupBy(entry => new LedgerGroupKey(entry.Service, entry.Route, entry.Model, entry.Tenant))
            .Select(LedgerGroup.FromEntries)
            .OrderByDescending(group => group.KnownCost)
            .ThenByDescending(group => group.Requests)
            .ToArray();

        var hasGuardFailure =
            (options.FailOnUnmatched && entries.Any(entry => !entry.MatchedSpan)) ||
            (options.FailOnMissingPrice && entries.Any(entry => entry.CostSource == "missing-price")) ||
            (options.FailOnSloBreach && entries.Any(entry => entry.SloBreached));

        return new LedgerReport(spans.Count, prices.Count, entries, groups, options.LatencySloMs, hasGuardFailure);
    }
}

file sealed class LedgerReport
{
    public LedgerReport(
        int spanCount,
        int priceCount,
        IReadOnlyList<LedgerEntry> entries,
        IReadOnlyList<LedgerGroup> groups,
        int latencySloMs,
        bool hasGuardFailure)
    {
        SpanCount = spanCount;
        PriceCount = priceCount;
        Entries = entries;
        Groups = groups;
        LatencySloMs = latencySloMs;
        HasGuardFailure = hasGuardFailure;
    }

    public int SpanCount { get; }
    public int PriceCount { get; }
    public IReadOnlyList<LedgerEntry> Entries { get; }
    public IReadOnlyList<LedgerGroup> Groups { get; }
    public int LatencySloMs { get; }
    public bool HasGuardFailure { get; }
    public decimal KnownCost => Entries.Where(entry => entry.Cost is not null).Sum(entry => entry.Cost!.Value);
    public long InputTokens => Entries.Sum(entry => entry.InputTokens);
    public long CachedInputTokens => Entries.Sum(entry => entry.CachedInputTokens);
    public long OutputTokens => Entries.Sum(entry => entry.OutputTokens);
    public int UnmatchedUsage => Entries.Count(entry => !entry.MatchedSpan);
    public int MissingPrice => Entries.Count(entry => entry.CostSource == "missing-price");
    public int SloBreaches => Entries.Count(entry => entry.SloBreached);

    public string ToHumanText(int top)
    {
        var builder = new StringBuilder(8192);
        builder.AppendLine("OtelInferenceCostLedger report");
        builder.Append("spans: ").Append(SpanCount)
            .Append(" | usage: ").Append(Entries.Count)
            .Append(" | price-book models: ").Append(PriceCount)
            .Append(" | matched: ").Append(Entries.Count - UnmatchedUsage)
            .Append(" | unmatched: ").Append(UnmatchedUsage)
            .AppendLine();
        builder.Append("known cost: ").Append(FormatMoney(KnownCost))
            .Append(" | input tokens: ").Append(InputTokens.ToString("N0", CultureInfo.InvariantCulture))
            .Append(" | cached input: ").Append(CachedInputTokens.ToString("N0", CultureInfo.InvariantCulture))
            .Append(" | output tokens: ").Append(OutputTokens.ToString("N0", CultureInfo.InvariantCulture))
            .AppendLine();
        builder.Append("missing price rows: ").Append(MissingPrice)
            .Append(" | latency SLO: ").Append(LatencySloMs).Append(" ms")
            .Append(" | SLO breaches: ").Append(SloBreaches)
            .Append(" | guard failed: ").Append(HasGuardFailure ? "yes" : "no")
            .AppendLine();
        builder.AppendLine();

        builder.AppendLine("Top attributed groups by known cost:");
        builder.AppendLine("cost        reqs  p95ms    cache%  service                 route                     model                     tenant");
        builder.AppendLine("----------  ----  -------  ------  ----------------------  ------------------------  ------------------------  ----------------");
        foreach (var group in Groups.Take(top))
        {
            builder.Append(FormatMoney(group.KnownCost).PadLeft(10)).Append("  ")
                .Append(group.Requests.ToString(CultureInfo.InvariantCulture).PadLeft(4)).Append("  ")
                .Append(FormatDouble(group.P95LatencyMs).PadLeft(7)).Append("  ")
                .Append(FormatPercent(group.CacheHitRatio).PadLeft(6)).Append("  ")
                .Append(Clip(group.Key.Service, 22).PadRight(22)).Append("  ")
                .Append(Clip(group.Key.Route, 24).PadRight(24)).Append("  ")
                .Append(Clip(group.Key.Model, 24).PadRight(24)).Append("  ")
                .Append(Clip(group.Key.Tenant, 16))
                .AppendLine();
        }

        var missingModels = Entries
            .Where(entry => entry.CostSource == "missing-price")
            .Select(entry => entry.Model)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(model => model, StringComparer.OrdinalIgnoreCase)
            .Take(12)
            .ToArray();
        if (missingModels.Length > 0)
        {
            builder.AppendLine();
            builder.Append("Models without provider cost or price-book rate: ").AppendLine(string.Join(", ", missingModels));
        }

        var unmatched = Entries.Where(entry => !entry.MatchedSpan).Take(8).ToArray();
        if (unmatched.Length > 0)
        {
            builder.AppendLine();
            builder.AppendLine("Unmatched usage rows:");
            foreach (var entry in unmatched)
            {
                builder.Append(" - usage line ").Append(entry.UsageLineNumber)
                    .Append(" request_id=").Append(entry.RequestId ?? "<none>")
                    .Append(" trace_id=").Append(entry.TraceId ?? "<none>")
                    .Append(" model=").Append(entry.Model)
                    .AppendLine();
            }
        }

        return builder.ToString();
    }

    public void WriteJson(string path)
    {
        using var stream = File.Create(path);
        using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true });
        writer.WriteStartObject();
        writer.WriteNumber("spans", SpanCount);
        writer.WriteNumber("usage_records", Entries.Count);
        writer.WriteNumber("price_book_models", PriceCount);
        writer.WriteNumber("known_cost", KnownCost);
        writer.WriteNumber("input_tokens", InputTokens);
        writer.WriteNumber("cached_input_tokens", CachedInputTokens);
        writer.WriteNumber("output_tokens", OutputTokens);
        writer.WriteNumber("unmatched_usage", UnmatchedUsage);
        writer.WriteNumber("missing_price_rows", MissingPrice);
        writer.WriteNumber("latency_slo_ms", LatencySloMs);
        writer.WriteNumber("slo_breaches", SloBreaches);
        writer.WriteBoolean("guard_failed", HasGuardFailure);

        writer.WritePropertyName("groups");
        writer.WriteStartArray();
        foreach (var group in Groups)
        {
            writer.WriteStartObject();
            writer.WriteString("service", group.Key.Service);
            writer.WriteString("route", group.Key.Route);
            writer.WriteString("model", group.Key.Model);
            writer.WriteString("tenant", group.Key.Tenant);
            writer.WriteNumber("requests", group.Requests);
            writer.WriteNumber("known_cost", group.KnownCost);
            writer.WriteNumber("input_tokens", group.InputTokens);
            writer.WriteNumber("cached_input_tokens", group.CachedInputTokens);
            writer.WriteNumber("output_tokens", group.OutputTokens);
            writer.WriteNumber("cache_hit_ratio", group.CacheHitRatio);
            if (group.P50LatencyMs is not null) writer.WriteNumber("p50_latency_ms", group.P50LatencyMs.Value);
            if (group.P95LatencyMs is not null) writer.WriteNumber("p95_latency_ms", group.P95LatencyMs.Value);
            if (group.P99LatencyMs is not null) writer.WriteNumber("p99_latency_ms", group.P99LatencyMs.Value);
            writer.WriteNumber("slo_breaches", group.SloBreaches);
            writer.WriteNumber("unmatched", group.Unmatched);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
        writer.Flush();
    }

    private static string FormatMoney(decimal value)
    {
        return "$" + value.ToString("0.0000", CultureInfo.InvariantCulture);
    }

    private static string FormatDouble(double? value)
    {
        return value is null ? "n/a" : value.Value.ToString("0", CultureInfo.InvariantCulture);
    }

    private static string FormatPercent(double value)
    {
        return (value * 100).ToString("0.0", CultureInfo.InvariantCulture);
    }

    private static string Clip(string value, int max)
    {
        if (value.Length <= max)
        {
            return value;
        }

        return max <= 1 ? value[..max] : value[..(max - 1)] + "~";
    }
}

file sealed record LedgerEntry(
    int UsageLineNumber,
    bool MatchedSpan,
    int? SpanLineNumber,
    string? TraceId,
    string? SpanId,
    string? RequestId,
    string Service,
    string Route,
    string Tenant,
    string Model,
    long InputTokens,
    long CachedInputTokens,
    long OutputTokens,
    long TotalTokens,
    double? LatencyMs,
    decimal? Cost,
    string CostSource,
    string Status,
    bool SloBreached);

file sealed record LedgerGroupKey(string Service, string Route, string Model, string Tenant);

file sealed class LedgerGroup
{
    private LedgerGroup(
        LedgerGroupKey key,
        int requests,
        decimal knownCost,
        long inputTokens,
        long cachedInputTokens,
        long outputTokens,
        double cacheHitRatio,
        double? p50LatencyMs,
        double? p95LatencyMs,
        double? p99LatencyMs,
        int sloBreaches,
        int unmatched)
    {
        Key = key;
        Requests = requests;
        KnownCost = knownCost;
        InputTokens = inputTokens;
        CachedInputTokens = cachedInputTokens;
        OutputTokens = outputTokens;
        CacheHitRatio = cacheHitRatio;
        P50LatencyMs = p50LatencyMs;
        P95LatencyMs = p95LatencyMs;
        P99LatencyMs = p99LatencyMs;
        SloBreaches = sloBreaches;
        Unmatched = unmatched;
    }

    public LedgerGroupKey Key { get; }
    public int Requests { get; }
    public decimal KnownCost { get; }
    public long InputTokens { get; }
    public long CachedInputTokens { get; }
    public long OutputTokens { get; }
    public double CacheHitRatio { get; }
    public double? P50LatencyMs { get; }
    public double? P95LatencyMs { get; }
    public double? P99LatencyMs { get; }
    public int SloBreaches { get; }
    public int Unmatched { get; }

    public static LedgerGroup FromEntries(IGrouping<LedgerGroupKey, LedgerEntry> group)
    {
        var entries = group.ToArray();
        var inputTokens = entries.Sum(entry => entry.InputTokens);
        var cachedTokens = entries.Sum(entry => entry.CachedInputTokens);
        var latencies = entries.Where(entry => entry.LatencyMs is not null).Select(entry => entry.LatencyMs!.Value).ToArray();
        return new LedgerGroup(
            group.Key,
            entries.Length,
            entries.Where(entry => entry.Cost is not null).Sum(entry => entry.Cost!.Value),
            inputTokens,
            cachedTokens,
            entries.Sum(entry => entry.OutputTokens),
            inputTokens == 0 ? 0 : (double)cachedTokens / inputTokens,
            Quantile(latencies, 0.50),
            Quantile(latencies, 0.95),
            Quantile(latencies, 0.99),
            entries.Count(entry => entry.SloBreached),
            entries.Count(entry => !entry.MatchedSpan));
    }

    private static double? Quantile(double[] values, double quantile)
    {
        if (values.Length == 0)
        {
            return null;
        }

        Array.Sort(values);
        var index = (int)Math.Ceiling(values.Length * quantile) - 1;
        index = Math.Clamp(index, 0, values.Length - 1);
        return values[index];
    }
}

file static class AttributeReader
{
    public static IReadOnlyDictionary<string, string> ReadAll(JsonElement root)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        MergeContainer(result, TryGet(root, "attributes"));
        MergeContainer(result, TryGet(root, "resource/attributes"));
        MergeContainer(result, TryGet(root, "resourceAttributes"));
        MergeContainer(result, TryGet(root, "scope/attributes"));
        return result;
    }

    private static void MergeContainer(Dictionary<string, string> target, JsonElement? container)
    {
        if (container is null)
        {
            return;
        }

        if (container.Value.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in container.Value.EnumerateObject())
            {
                var scalar = ToScalar(property.Value);
                if (!string.IsNullOrWhiteSpace(scalar))
                {
                    target[property.Name] = scalar;
                }
            }
        }
        else if (container.Value.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in container.Value.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object || !item.TryGetProperty("key", out var keyElement))
                {
                    continue;
                }

                var key = ToScalar(keyElement);
                if (string.IsNullOrWhiteSpace(key))
                {
                    continue;
                }

                if (item.TryGetProperty("value", out var valueElement))
                {
                    var value = ToOtelValue(valueElement);
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        target[key] = value;
                    }
                }
            }
        }
    }

    private static JsonElement? TryGet(JsonElement root, string path)
    {
        var current = root;
        foreach (var segment in path.Split('/'))
        {
            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(segment, out current))
            {
                return null;
            }
        }

        return current;
    }

    private static string? ToOtelValue(JsonElement value)
    {
        if (value.ValueKind != JsonValueKind.Object)
        {
            return ToScalar(value);
        }

        foreach (var key in new[] { "stringValue", "intValue", "doubleValue", "boolValue" })
        {
            if (value.TryGetProperty(key, out var typed))
            {
                return ToScalar(typed);
            }
        }

        return ToScalar(value);
    }

    private static string? ToScalar(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => null,
            _ => element.GetRawText(),
        };
    }
}

file static class JsonAccess
{
    public static string? GetString(JsonElement root, params string[] paths)
    {
        foreach (var path in paths)
        {
            if (TryGet(root, path, out var value))
            {
                var scalar = Scalar(value);
                if (!string.IsNullOrWhiteSpace(scalar))
                {
                    return scalar;
                }
            }
        }

        return null;
    }

    public static long? GetInt64(JsonElement root, params string[] paths)
    {
        foreach (var path in paths)
        {
            if (!TryGet(root, path, out var value))
            {
                continue;
            }

            if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var number))
            {
                return number;
            }

            var scalar = Scalar(value);
            if (long.TryParse(scalar, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            {
                return parsed;
            }
        }

        return null;
    }

    public static double? GetDouble(JsonElement root, params string[] paths)
    {
        foreach (var path in paths)
        {
            if (!TryGet(root, path, out var value))
            {
                continue;
            }

            if (value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var number))
            {
                return number;
            }

            var scalar = Scalar(value);
            if (double.TryParse(scalar, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
            {
                return parsed;
            }
        }

        return null;
    }

    public static decimal? GetDecimal(JsonElement root, params string[] paths)
    {
        foreach (var path in paths)
        {
            if (!TryGet(root, path, out var value))
            {
                continue;
            }

            if (value.ValueKind == JsonValueKind.Number && value.TryGetDecimal(out var number))
            {
                return number;
            }

            var scalar = Scalar(value);
            if (decimal.TryParse(scalar, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
            {
                return parsed;
            }
        }

        return null;
    }

    public static DateTimeOffset? GetDateTime(JsonElement root, params string[] paths)
    {
        foreach (var path in paths)
        {
            if (!TryGet(root, path, out var value))
            {
                continue;
            }

            var scalar = Scalar(value);
            if (DateTimeOffset.TryParse(scalar, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed))
            {
                return parsed;
            }
        }

        return null;
    }

    private static bool TryGet(JsonElement root, string path, out JsonElement value)
    {
        value = root;
        foreach (var segment in path.Split('/'))
        {
            if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty(segment, out value))
            {
                return false;
            }
        }

        return true;
    }

    private static string? Scalar(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => null,
            _ => element.GetRawText(),
        };
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
This solves the April 2026 problem where teams can see LLM bills, OpenTelemetry traces, and gateway logs, but still cannot answer which service, route, model, or tenant burned the money during an incident. Built because I have seen backend teams manually paste trace ids, request ids, and token counts into spreadsheets when a model rollout or agent workflow suddenly gets expensive. Use it when you run OpenAI, Azure OpenAI, Anthropic, Gemini, vLLM, LiteLLM, or an internal AI gateway and you need a C# cost attribution ledger that works from plain JSONL in CI, cron, Kubernetes jobs, or an incident shell. The trick: it joins by request id first, then trace and span ids, keeps unmatched usage visible instead of hiding it, separates provider reported cost from price book estimates, and turns cache hit ratio, p95 latency, missing model prices, and SLO breaches into a report you can ship to finance, infra, and product owners. Drop this into a .NET 8 repo, observability pipeline, DevOps runbook, model migration project, or AI platform cost control check when you want practical OpenTelemetry LLM cost tracking, inference usage analytics, token billing reconciliation, model routing audit evidence, and production AI infrastructure accountability without adding another database or SaaS dependency.
*/
