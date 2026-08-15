// InferenceSpendReconciliationLedger.cs
// Dependency-free C# CLI for auditing AI inference spend from JSONL telemetry, CSV prices, invoices, and budgets.

#nullable enable

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace VibeCodeDaily;

internal static class Program
{
public static int Main(string[] args)
{
try
{
var options = Options.Parse(args);
if (options.Help)
{
Console.WriteLine(HelpText());
return 0;
}

if (options.SelfTest)
{
return SelfTest(Console.Out);
}

if (string.IsNullOrWhiteSpace(options.TelemetryPath))
{
throw new UsageException("missing --telemetry <usage.jsonl>");
}

var events = ReadUsageFile(options.TelemetryPath);
var prices = ReadPrices(options.PricePath);
var invoices = ReadInvoices(options.InvoicePath);
var budgets = ReadBudgets(options.BudgetPath);
var result = Reconcile(events, prices, invoices, budgets, options);
Emit(result, options.Format, Console.Out);
return result.MaxSeverity >= options.FailOn ? 2 : 0;
}
catch (UsageException ex)
{
Console.Error.WriteLine("usage: " + ex.Message);
return 64;
}
catch (JsonException ex)
{
Console.Error.WriteLine("json: " + ex.Message);
return 65;
}
catch (IOException ex)
{
Console.Error.WriteLine("io: " + ex.Message);
return 74;
}
catch (Exception ex)
{
Console.Error.WriteLine("fatal: " + ex.Message);
return 1;
}
}

private static string HelpText() => string.Join(Environment.NewLine, new[]
{
"InferenceSpendReconciliationLedger",
"",
"Reconcile AI inference telemetry JSONL against a model price book, declared gateway charges, provider invoice rows, replay keys, and tenant budgets.",
"",
"Build:",
"  csc -nologo -out:InferenceSpendReconciliationLedger InferenceSpendReconciliationLedger.cs",
"",
"Run:",
"  ./InferenceSpendReconciliationLedger --telemetry usage.jsonl --prices prices.csv",
"  ./InferenceSpendReconciliationLedger --telemetry usage.jsonl --prices prices.csv --invoice invoice.csv --budgets budgets.csv --format sarif --fail-on high",
"",
"Options:",
"  --telemetry <file>          JSONL usage events. Common OpenAI, Anthropic, Azure, Bedrock, and gateway field names are normalized.",
"  --prices <file>             CSV columns: provider,model,input_usd_per_million,output_usd_per_million,cached_input_usd_per_million,cache_write_usd_per_million.",
"  --invoice <file>            Optional CSV columns: tenant,provider,model,date,request_id,amount_usd.",
"  --budgets <file>            Optional CSV columns: tenant,daily_budget_usd,monthly_budget_usd.",
"  --max-drift-pct <n>         Percentage drift allowed before a finding. Default: 1.0.",
"  --max-drift-usd <n>         Absolute USD drift allowed before a finding. Default: 0.05.",
"  --replay-window-min <n>     Duplicate replay window. Default: 1440.",
"  --require-prices            Emit findings for unpriced provider/model pairs.",
"  --format <text|json|sarif>  Output format. Default: text.",
"  --fail-on <severity>        low, medium, high, critical. Default: high.",
"  --self-test                 Run deterministic built-in checks.",
"  --help                      Show this help."
});

private sealed class Options
{
public string? TelemetryPath { get; private set; }
public string? PricePath { get; private set; }
public string? InvoicePath { get; private set; }
public string? BudgetPath { get; private set; }
public string Format { get; private set; } = "text";
public Severity FailOn { get; private set; } = Severity.High;
public decimal MaxDriftPct { get; private set; } = 1.0m;
public decimal MaxDriftUsd { get; private set; } = 0.05m;
public int ReplayWindowMinutes { get; private set; } = 1440;
public bool RequirePrices { get; private set; }
public bool Help { get; private set; }
public bool SelfTest { get; private set; }

public static Options Parse(string[] args)
{
var options = new Options();
for (var i = 0; i < args.Length; i++)
{
switch (args[i])
{
case "--telemetry":
options.TelemetryPath = Need(args, ref i, "--telemetry");
break;
case "--prices":
options.PricePath = Need(args, ref i, "--prices");
break;
case "--invoice":
options.InvoicePath = Need(args, ref i, "--invoice");
break;
case "--budgets":
options.BudgetPath = Need(args, ref i, "--budgets");
break;
case "--format":
options.Format = Need(args, ref i, "--format").ToLowerInvariant();
if (options.Format != "text" && options.Format != "json" && options.Format != "sarif")
{
throw new UsageException("--format must be text, json, or sarif");
}
break;
case "--fail-on":
if (!Enum.TryParse<Severity>(Need(args, ref i, "--fail-on"), true, out var severity) || severity == Severity.Info)
{
throw new UsageException("--fail-on must be low, medium, high, or critical");
}
options.FailOn = severity;
break;
case "--max-drift-pct":
options.MaxDriftPct = Decimal(Need(args, ref i, "--max-drift-pct"), "--max-drift-pct");
break;
case "--max-drift-usd":
options.MaxDriftUsd = Decimal(Need(args, ref i, "--max-drift-usd"), "--max-drift-usd");
break;
case "--replay-window-min":
options.ReplayWindowMinutes = PositiveInt(Need(args, ref i, "--replay-window-min"), "--replay-window-min");
break;
case "--require-prices":
options.RequirePrices = true;
break;
case "--self-test":
options.SelfTest = true;
break;
case "--help":
case "-h":
options.Help = true;
break;
default:
throw new UsageException("unknown option " + args[i]);
}
}

return options;
}

private static string Need(string[] args, ref int index, string option)
{
if (index + 1 >= args.Length || args[index + 1].StartsWith("--", StringComparison.Ordinal))
{
throw new UsageException(option + " requires a value");
}

index++;
return args[index];
}

private static decimal Decimal(string value, string option)
{
if (decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) && parsed >= 0m)
{
return parsed;
}

throw new UsageException(option + " must be a non-negative decimal");
}

private static int PositiveInt(string value, string option)
{
if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed > 0)
{
return parsed;
}

throw new UsageException(option + " must be a positive integer");
}
}

private enum Severity
{
Info = 0,
Low = 1,
Medium = 2,
High = 3,
Critical = 4
}

private sealed record UsageEvent(
int Line,
string RequestId,
string ReplayKey,
string Tenant,
string Provider,
string Model,
string Status,
DateTimeOffset Timestamp,
long InputTokens,
long OutputTokens,
long CachedInputTokens,
long CacheWriteTokens,
decimal? DeclaredCostUsd)
{
public string DateKey => Timestamp == DateTimeOffset.MinValue ? "unknown" : Timestamp.UtcDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
public bool Success => string.IsNullOrWhiteSpace(Status) || Status.Equals("ok", StringComparison.OrdinalIgnoreCase) || Status.Equals("success", StringComparison.OrdinalIgnoreCase) || Status.StartsWith("2", StringComparison.Ordinal);
public string StableKey => !string.IsNullOrWhiteSpace(ReplayKey) ? ReplayKey : RequestId;
}

private sealed record PriceRule(string Provider, string Model, decimal Input, decimal Output, decimal CachedInput, decimal CacheWrite, decimal Minimum)
{
public bool Matches(UsageEvent e) => Wild(Provider, e.Provider) && Wild(Model, e.Model);
public int Score => (Provider == "*" ? 0 : Provider.Contains('*') ? 1 : 3) + (Model == "*" ? 0 : Model.Contains('*') ? 2 : 6);
public decimal Estimate(UsageEvent e)
{
var cached = Math.Min(Math.Max(e.CachedInputTokens, 0), Math.Max(e.InputTokens, 0));
var input = Math.Max(e.InputTokens - cached, 0);
var output = Math.Max(e.OutputTokens, 0);
var write = Math.Max(e.CacheWriteTokens, 0);
var cost = input / 1_000_000m * Input + output / 1_000_000m * Output + cached / 1_000_000m * CachedInput + write / 1_000_000m * CacheWrite;
return Round(Math.Max(cost, Minimum));
}
}

private sealed record InvoiceRow(string Tenant, string Provider, string Model, string DateKey, string RequestId, decimal AmountUsd);
private sealed record BudgetRow(string Tenant, decimal DailyUsd, decimal MonthlyUsd);
private sealed record Costed(UsageEvent Event, PriceRule? Rule, decimal? EstimatedUsd);
private sealed record Summary(string Tenant, string Provider, string Model, string DateKey, int Requests, long InputTokens, long OutputTokens, long CachedInputTokens, decimal EstimatedUsd, decimal DeclaredUsd);
private sealed record Finding(Severity Severity, string Code, string Message, string Key, decimal DriftUsd, decimal DriftPct, Dictionary<string, string> Evidence);
private sealed record Result(List<Costed> Events, List<Summary> Summaries, List<Finding> Findings)
{
public Severity MaxSeverity => Findings.Count == 0 ? Severity.Info : Findings.Max(f => f.Severity);
public decimal EstimatedUsd => Round(Events.Where(e => e.EstimatedUsd.HasValue).Sum(e => e.EstimatedUsd!.Value));
public decimal DeclaredUsd => Round(Events.Where(e => e.Event.DeclaredCostUsd.HasValue).Sum(e => e.Event.DeclaredCostUsd!.Value));
public int Unpriced => Events.Count(e => !e.EstimatedUsd.HasValue);
}

private sealed class UsageException : Exception
{
public UsageException(string message) : base(message) { }
}

private static List<UsageEvent> ReadUsageFile(string path)
{
if (!File.Exists(path))
{
throw new IOException("telemetry file not found: " + path);
}

return ReadUsageLines(File.ReadLines(path)).ToList();
}

private static IEnumerable<UsageEvent> ReadUsageLines(IEnumerable<string> lines)
{
var lineNo = 0;
foreach (var line in lines)
{
lineNo++;
if (string.IsNullOrWhiteSpace(line))
{
continue;
}

using var doc = JsonDocument.Parse(line);
var flat = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
Flatten(doc.RootElement, "", flat);
var requestId = Text(flat, "request_id", "requestId", "id", "response.id", "trace.request_id");
var replayKey = Text(flat, "idempotency_key", "idempotencyKey", "replay_key", "dedupe_key", "cache_key", "request_hash");
yield return new UsageEvent(
lineNo,
requestId,
replayKey,
TextOr(flat, "unknown", "tenant", "tenant_id", "workspace", "project", "team", "customer", "user"),
TextOr(flat, "unknown", "provider", "vendor", "platform", "api_provider", "gateway.provider"),
TextOr(flat, "unknown", "model", "model_name", "deployment", "response.model", "gateway.model"),
Text(flat, "status", "outcome", "http.status", "http_status", "response.status"),
Time(flat, "timestamp", "time", "created_at", "started_at", "request_started_at", "ts"),
Long(flat, "input_tokens", "prompt_tokens", "usage.input_tokens", "usage.prompt_tokens", "tokens.input", "promptTokens"),
Long(flat, "output_tokens", "completion_tokens", "usage.output_tokens", "usage.completion_tokens", "tokens.output", "completionTokens"),
Long(flat, "cached_input_tokens", "cached_tokens", "prompt_cache_hit_tokens", "cache_read_input_tokens", "usage.prompt_tokens_details.cached_tokens", "usage.input_token_details.cached_tokens"),
Long(flat, "cache_write_tokens", "cache_creation_input_tokens", "prompt_cache_write_tokens", "usage.cache_write_tokens"),
MoneyOrNull(flat, "cost_usd", "charge_usd", "billed_usd", "amount_usd", "usage.cost_usd", "gateway.cost_usd"));
}
}

private static void Flatten(JsonElement element, string prefix, Dictionary<string, JsonElement> flat)
{
if (element.ValueKind != JsonValueKind.Object)
{
Add(flat, prefix, element);
return;
}

foreach (var property in element.EnumerateObject())
{
var dotted = string.IsNullOrWhiteSpace(prefix) ? property.Name : prefix + "." + property.Name;
if (property.Value.ValueKind == JsonValueKind.Object)
{
Flatten(property.Value, dotted, flat);
}
else
{
Add(flat, dotted, property.Value);
Add(flat, property.Name, property.Value);
}
}
}

private static void Add(Dictionary<string, JsonElement> flat, string key, JsonElement value)
{
if (string.IsNullOrWhiteSpace(key))
{
return;
}

var normalized = Normalize(key);
if (!flat.ContainsKey(normalized))
{
flat[normalized] = value.Clone();
}
}

private static List<PriceRule> ReadPrices(string? path)
{
if (string.IsNullOrWhiteSpace(path))
{
return new List<PriceRule>();
}

return CsvDictionaries(File.ReadAllText(path))
.Select(row => new PriceRule(
Cell(row, "provider", fallback: "*"),
Cell(row, "model", fallback: "*"),
DecimalCell(row, "input_usd_per_million", "input"),
DecimalCell(row, "output_usd_per_million", "output"),
DecimalCell(row, "cached_input_usd_per_million", "cached_input", "cache_read"),
DecimalCell(row, "cache_write_usd_per_million", "cache_write"),
DecimalCell(row, "minimum_request_usd", "minimum")))
.OrderByDescending(r => r.Score)
.ToList();
}

private static List<InvoiceRow> ReadInvoices(string? path)
{
if (string.IsNullOrWhiteSpace(path))
{
return new List<InvoiceRow>();
}

return CsvDictionaries(File.ReadAllText(path))
.Select(row => new InvoiceRow(
Cell(row, "tenant", fallback: "unknown"),
Cell(row, "provider", fallback: "unknown"),
Cell(row, "model", fallback: "unknown"),
DateCell(row, "date"),
Cell(row, "request_id"),
DecimalCell(row, "amount_usd", "cost_usd", "charge_usd", "billed_usd")))
.ToList();
}

private static List<BudgetRow> ReadBudgets(string? path)
{
if (string.IsNullOrWhiteSpace(path))
{
return new List<BudgetRow>();
}

return CsvDictionaries(File.ReadAllText(path))
.Select(row => new BudgetRow(Cell(row, "tenant"), DecimalCell(row, "daily_budget_usd", "daily"), DecimalCell(row, "monthly_budget_usd", "monthly")))
.Where(row => !string.IsNullOrWhiteSpace(row.Tenant))
.ToList();
}

private static Result Reconcile(List<UsageEvent> events, List<PriceRule> prices, List<InvoiceRow> invoices, List<BudgetRow> budgets, Options options)
{
var costed = events.Select(e =>
{
var rule = prices.FirstOrDefault(p => p.Matches(e));
return new Costed(e, rule, rule?.Estimate(e));
}).ToList();

var findings = new List<Finding>();
foreach (var item in costed)
{
ValidateEvent(item, options, findings);
if (item.EstimatedUsd.HasValue && item.Event.DeclaredCostUsd.HasValue)
{
AddDrift(findings, "declared-cost-drift", item.Event.RequestId, "Gateway declared cost differs from the price book estimate.", item.EstimatedUsd.Value, item.Event.DeclaredCostUsd.Value, options, Evidence(item.Event));
}
}

AddReplayFindings(costed, options, findings);
AddInvoiceFindings(costed, invoices, options, findings);
AddBudgetFindings(costed, budgets, findings);

var summaries = costed
.GroupBy(c => Scope(c.Event.Tenant, c.Event.Provider, c.Event.Model, c.Event.DateKey))
.Select(g => new Summary(
g.First().Event.Tenant,
g.First().Event.Provider,
g.First().Event.Model,
g.First().Event.DateKey,
g.Count(),
g.Sum(x => x.Event.InputTokens),
g.Sum(x => x.Event.OutputTokens),
g.Sum(x => x.Event.CachedInputTokens),
Round(g.Where(x => x.EstimatedUsd.HasValue).Sum(x => x.EstimatedUsd!.Value)),
Round(g.Where(x => x.Event.DeclaredCostUsd.HasValue).Sum(x => x.Event.DeclaredCostUsd!.Value))))
.OrderByDescending(s => s.EstimatedUsd)
.ThenBy(s => s.Tenant, StringComparer.OrdinalIgnoreCase)
.ToList();

return new Result(costed, summaries, findings.OrderByDescending(f => f.Severity).ThenBy(f => f.Code, StringComparer.Ordinal).ToList());
}

private static void ValidateEvent(Costed item, Options options, List<Finding> findings)
{
var e = item.Event;
if (string.IsNullOrWhiteSpace(e.RequestId))
{
findings.Add(FindingFor(Severity.Medium, "missing-request-id", "Usage event has no stable request id.", e, "line", e.Line.ToString(CultureInfo.InvariantCulture)));
}

if (e.Timestamp == DateTimeOffset.MinValue)
{
findings.Add(FindingFor(Severity.Low, "missing-or-invalid-timestamp", "Usage event timestamp could not be parsed.", e, "line", e.Line.ToString(CultureInfo.InvariantCulture)));
}

if (e.InputTokens < 0 || e.OutputTokens < 0 || e.CachedInputTokens < 0 || e.CacheWriteTokens < 0)
{
findings.Add(FindingFor(Severity.Critical, "negative-token-count", "Token counters must never be negative.", e, "line", e.Line.ToString(CultureInfo.InvariantCulture)));
}

if (e.CachedInputTokens > e.InputTokens && e.InputTokens >= 0)
{
findings.Add(FindingFor(Severity.High, "cache-tokens-exceed-input", "Cached input tokens exceed total input tokens, so cache savings and billing math are not trustworthy.", e, "cached_input_tokens", e.CachedInputTokens.ToString(CultureInfo.InvariantCulture)));
}

if (!e.Success && (e.InputTokens > 0 || e.OutputTokens > 0 || e.DeclaredCostUsd.GetValueOrDefault() > 0m))
{
findings.Add(FindingFor(Severity.Medium, "failed-request-billed", "A non-success request still carries usage or charge data.", e, "status", e.Status));
}

if (item.Rule == null && options.RequirePrices)
{
findings.Add(FindingFor(Severity.High, "unpriced-model", "No price rule matched this provider and model.", e, "provider_model", e.Provider + "/" + e.Model));
}
}

private static void AddReplayFindings(List<Costed> costed, Options options, List<Finding> findings)
{
foreach (var group in costed.Where(c => !string.IsNullOrWhiteSpace(c.Event.StableKey)).GroupBy(c => c.Event.StableKey, StringComparer.OrdinalIgnoreCase))
{
var ordered = group.OrderBy(c => c.Event.Timestamp).ToList();
if (ordered.Count < 2)
{
continue;
}

var first = ordered[0];
var duplicates = ordered.Skip(1).Where(c => Inside(first.Event.Timestamp, c.Event.Timestamp, options.ReplayWindowMinutes)).ToList();
if (duplicates.Count == 0)
{
continue;
}

var waste = Round(duplicates.Where(c => c.EstimatedUsd.HasValue).Sum(c => c.EstimatedUsd!.Value));
findings.Add(new Finding(waste > 25m ? Severity.Critical : Severity.High, "duplicate-replay-charge", "The same request or idempotency key appears multiple times inside the replay window.", group.Key, waste, 0m, new Dictionary<string, string>
{
["tenant"] = first.Event.Tenant,
["provider"] = first.Event.Provider,
["model"] = first.Event.Model,
["first_line"] = first.Event.Line.ToString(CultureInfo.InvariantCulture),
["duplicates"] = duplicates.Count.ToString(CultureInfo.InvariantCulture),
["estimated_replay_waste_usd"] = Money(waste)
}));
}
}

private static void AddInvoiceFindings(List<Costed> costed, List<InvoiceRow> invoices, Options options, List<Finding> findings)
{
if (invoices.Count == 0)
{
return;
}

var byRequest = costed
.Where(c => !string.IsNullOrWhiteSpace(c.Event.RequestId) && c.EstimatedUsd.HasValue)
.GroupBy(c => c.Event.RequestId, StringComparer.OrdinalIgnoreCase)
.ToDictionary(g => g.Key, g => Round(g.Sum(x => x.EstimatedUsd!.Value)), StringComparer.OrdinalIgnoreCase);

foreach (var invoice in invoices.Where(i => !string.IsNullOrWhiteSpace(i.RequestId)))
{
if (!byRequest.TryGetValue(invoice.RequestId, out var expected))
{
findings.Add(new Finding(Severity.High, "invoice-request-missing-from-telemetry", "Invoice row references a request id that is absent from telemetry.", invoice.RequestId, invoice.AmountUsd, 100m, new Dictionary<string, string>
{
["tenant"] = invoice.Tenant,
["provider"] = invoice.Provider,
["model"] = invoice.Model,
["invoice_amount_usd"] = Money(invoice.AmountUsd)
}));
}
else
{
AddDrift(findings, "invoice-request-drift", invoice.RequestId, "Provider invoice request charge differs from telemetry estimate.", expected, invoice.AmountUsd, options, new Dictionary<string, string>
{
["tenant"] = invoice.Tenant,
["provider"] = invoice.Provider,
["model"] = invoice.Model,
["scope"] = "request"
});
}
}

var eventGroups = costed
.Where(c => c.EstimatedUsd.HasValue)
.GroupBy(c => Scope(c.Event.Tenant, c.Event.Provider, c.Event.Model, c.Event.DateKey))
.ToDictionary(g => g.Key, g => Round(g.Sum(c => c.EstimatedUsd!.Value)), StringComparer.OrdinalIgnoreCase);

foreach (var group in invoices.GroupBy(i => Scope(i.Tenant, i.Provider, i.Model, i.DateKey)))
{
var actual = Round(group.Sum(i => i.AmountUsd));
if (!eventGroups.TryGetValue(group.Key, out var expected))
{
findings.Add(new Finding(Severity.High, "invoice-group-missing-from-telemetry", "Invoice group has no matching telemetry group.", group.Key, actual, 100m, new Dictionary<string, string> { ["invoice_amount_usd"] = Money(actual) }));
}
else
{
AddDrift(findings, "invoice-group-drift", group.Key, "Provider invoice group charge differs from telemetry estimate.", expected, actual, options, new Dictionary<string, string> { ["scope"] = "tenant/provider/model/date" });
}
}
}

private static void AddBudgetFindings(List<Costed> costed, List<BudgetRow> budgets, List<Finding> findings)
{
foreach (var budget in budgets)
{
if (budget.DailyUsd > 0m)
{
foreach (var day in costed.Where(c => c.Event.Tenant.Equals(budget.Tenant, StringComparison.OrdinalIgnoreCase) && c.EstimatedUsd.HasValue).GroupBy(c => c.Event.DateKey))
{
var spend = Round(day.Sum(c => c.EstimatedUsd!.Value));
if (spend > budget.DailyUsd)
{
var drift = Round(spend - budget.DailyUsd);
findings.Add(new Finding(drift > budget.DailyUsd * 0.25m ? Severity.Critical : Severity.High, "tenant-daily-budget-exceeded", "Tenant estimated daily inference spend exceeded the declared budget.", budget.Tenant + "/" + day.Key, drift, Percent(drift, budget.DailyUsd), new Dictionary<string, string>
{
["tenant"] = budget.Tenant,
["date"] = day.Key,
["spend_usd"] = Money(spend),
["budget_usd"] = Money(budget.DailyUsd)
}));
}
}
}

if (budget.MonthlyUsd > 0m)
{
foreach (var month in costed.Where(c => c.Event.Tenant.Equals(budget.Tenant, StringComparison.OrdinalIgnoreCase) && c.EstimatedUsd.HasValue).GroupBy(c => c.Event.DateKey.Length >= 7 ? c.Event.DateKey.Substring(0, 7) : "unknown"))
{
var spend = Round(month.Sum(c => c.EstimatedUsd!.Value));
if (spend > budget.MonthlyUsd)
{
var drift = Round(spend - budget.MonthlyUsd);
findings.Add(new Finding(drift > budget.MonthlyUsd * 0.25m ? Severity.Critical : Severity.High, "tenant-monthly-budget-exceeded", "Tenant estimated monthly inference spend exceeded the declared budget.", budget.Tenant + "/" + month.Key, drift, Percent(drift, budget.MonthlyUsd), new Dictionary<string, string>
{
["tenant"] = budget.Tenant,
["month"] = month.Key,
["spend_usd"] = Money(spend),
["budget_usd"] = Money(budget.MonthlyUsd)
}));
}
}
}
}
}

private static void AddDrift(List<Finding> findings, string code, string key, string message, decimal expected, decimal actual, Options options, Dictionary<string, string> evidence)
{
var drift = Round(actual - expected);
var pct = Percent(Math.Abs(drift), expected);
if (Math.Abs(drift) <= options.MaxDriftUsd || pct <= options.MaxDriftPct)
{
return;
}

evidence["expected_usd"] = Money(expected);
evidence["actual_usd"] = Money(actual);
findings.Add(new Finding(pct > 20m ? Severity.Critical : Severity.High, code, message, key, drift, pct, evidence));
}

private static Finding FindingFor(Severity severity, string code, string message, UsageEvent e, string evidenceKey, string evidenceValue)
{
var evidence = Evidence(e);
evidence[evidenceKey] = evidenceValue;
return new Finding(severity, code, message, string.IsNullOrWhiteSpace(e.RequestId) ? "line:" + e.Line.ToString(CultureInfo.InvariantCulture) : e.RequestId, 0m, 0m, evidence);
}

private static Dictionary<string, string> Evidence(UsageEvent e) => new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
["line"] = e.Line.ToString(CultureInfo.InvariantCulture),
["request_id"] = e.RequestId,
["tenant"] = e.Tenant,
["provider"] = e.Provider,
["model"] = e.Model,
["date"] = e.DateKey
};

private static void Emit(Result result, string format, TextWriter writer)
{
if (format == "json")
{
var payload = new
{
tool = "InferenceSpendReconciliationLedger",
generated_at_utc = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture),
totals = Totals(result),
summaries = result.Summaries,
findings = ProjectFindings(result.Findings)
};
writer.WriteLine(JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true }));
return;
}

if (format == "sarif")
{
var rules = result.Findings.GroupBy(f => f.Code).Select(g => new
{
id = g.Key,
name = g.Key,
shortDescription = new { text = g.First().Message },
properties = new { precision = "high" }
}).ToList();
var sarif = new Dictionary<string, object>
{
["version"] = "2.1.0",
["$schema"] = "https://json.schemastore.org/sarif-2.1.0.json",
["runs"] = new[]
{
new
{
tool = new { driver = new { name = "InferenceSpendReconciliationLedger", informationUri = "https://github.com/kspavankrishna/VIBE-CODE", rules } },
results = result.Findings.Select(f => new
{
ruleId = f.Code,
level = f.Severity >= Severity.High ? "error" : f.Severity == Severity.Medium ? "warning" : "note",
message = new { text = f.Message },
properties = new { severity = f.Severity.ToString().ToLowerInvariant(), key = f.Key, drift_usd = f.DriftUsd, drift_pct = f.DriftPct, evidence = f.Evidence }
}).ToList()
}
}
};
writer.WriteLine(JsonSerializer.Serialize(sarif, new JsonSerializerOptions { WriteIndented = true }));
return;
}

writer.WriteLine("Inference spend reconciliation");
writer.WriteLine("events=" + result.Events.Count + " estimated_usd=" + Money(result.EstimatedUsd) + " declared_usd=" + Money(result.DeclaredUsd) + " unpriced_events=" + result.Unpriced + " findings=" + result.Findings.Count + " max_severity=" + result.MaxSeverity.ToString().ToLowerInvariant());
writer.WriteLine();
writer.WriteLine("Top spend groups");
foreach (var summary in result.Summaries.Take(20))
{
writer.WriteLine("- " + summary.Tenant + " " + summary.Provider + "/" + summary.Model + " " + summary.DateKey + " requests=" + summary.Requests + " input=" + summary.InputTokens + " output=" + summary.OutputTokens + " cached=" + summary.CachedInputTokens + " estimated_usd=" + Money(summary.EstimatedUsd) + " declared_usd=" + Money(summary.DeclaredUsd));
}

writer.WriteLine();
writer.WriteLine(result.Findings.Count == 0 ? "No findings." : "Findings");
foreach (var finding in result.Findings)
{
writer.WriteLine("- [" + finding.Severity.ToString().ToLowerInvariant() + "] " + finding.Code + " key=" + finding.Key);
writer.WriteLine("  " + finding.Message);
if (finding.DriftUsd != 0m || finding.DriftPct != 0m)
{
writer.WriteLine("  drift_usd=" + Money(finding.DriftUsd) + " drift_pct=" + finding.DriftPct.ToString("0.####", CultureInfo.InvariantCulture));
}

foreach (var pair in finding.Evidence.OrderBy(p => p.Key, StringComparer.OrdinalIgnoreCase))
{
if (!string.IsNullOrWhiteSpace(pair.Value))
{
writer.WriteLine("  " + pair.Key + "=" + pair.Value);
}
}
}
}

private static object Totals(Result result) => new
{
events = result.Events.Count,
estimated_usd = result.EstimatedUsd,
declared_usd = result.DeclaredUsd,
unpriced_events = result.Unpriced,
findings = result.Findings.Count,
max_severity = result.MaxSeverity.ToString().ToLowerInvariant()
};

private static IEnumerable<object> ProjectFindings(IEnumerable<Finding> findings) => findings.Select(f => new
{
severity = f.Severity.ToString().ToLowerInvariant(),
f.Code,
f.Message,
f.Key,
f.DriftUsd,
f.DriftPct,
f.Evidence
});

private static IReadOnlyList<Dictionary<string, string>> CsvDictionaries(string text)
{
var rows = ParseCsvRows(text).Where(row => row.Any(cell => !string.IsNullOrWhiteSpace(cell))).ToList();
if (rows.Count == 0)
{
return Array.Empty<Dictionary<string, string>>();
}

var headers = rows[0].Select(Normalize).ToList();
var output = new List<Dictionary<string, string>>();
foreach (var row in rows.Skip(1))
{
var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
for (var i = 0; i < headers.Count && i < row.Count; i++)
{
dict[headers[i]] = row[i].Trim();
}

output.Add(dict);
}

return output;
}

private static List<List<string>> ParseCsvRows(string text)
{
var rows = new List<List<string>>();
var row = new List<string>();
var field = new StringBuilder();
var quoted = false;
for (var i = 0; i < text.Length; i++)
{
var ch = text[i];
if (quoted)
{
if (ch == '"' && i + 1 < text.Length && text[i + 1] == '"')
{
field.Append('"');
i++;
}
else if (ch == '"')
{
quoted = false;
}
else
{
field.Append(ch);
}
}
else if (ch == '"')
{
quoted = true;
}
else if (ch == ',')
{
row.Add(field.ToString());
field.Clear();
}
else if (ch == '\n')
{
row.Add(field.ToString());
field.Clear();
rows.Add(row);
row = new List<string>();
}
else if (ch != '\r')
{
field.Append(ch);
}
}

row.Add(field.ToString());
rows.Add(row);
return rows;
}

private static string Cell(Dictionary<string, string> row, string key, string fallback = "")
{
return row.TryGetValue(Normalize(key), out var value) && !string.IsNullOrWhiteSpace(value) ? value.Trim() : fallback;
}

private static decimal DecimalCell(Dictionary<string, string> row, params string[] keys)
{
foreach (var key in keys)
{
if (row.TryGetValue(Normalize(key), out var value) && decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
{
return parsed;
}
}

return 0m;
}

private static string DateCell(Dictionary<string, string> row, string key)
{
var raw = Cell(row, key, "unknown");
return DateTimeOffset.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed) ? parsed.UtcDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) : raw;
}

private static string Text(Dictionary<string, JsonElement> flat, params string[] aliases)
{
foreach (var alias in aliases)
{
if (flat.TryGetValue(Normalize(alias), out var value))
{
return value.ValueKind == JsonValueKind.String ? value.GetString() ?? "" : value.ToString();
}
}

return "";
}

private static string TextOr(Dictionary<string, JsonElement> flat, string fallback, params string[] aliases)
{
var value = Text(flat, aliases);
return string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();
}

private static long Long(Dictionary<string, JsonElement> flat, params string[] aliases)
{
foreach (var alias in aliases)
{
if (!flat.TryGetValue(Normalize(alias), out var value))
{
continue;
}

if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var n))
{
return n;
}

if (value.ValueKind == JsonValueKind.String && long.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s))
{
return s;
}
}

return 0L;
}

private static decimal? MoneyOrNull(Dictionary<string, JsonElement> flat, params string[] aliases)
{
foreach (var alias in aliases)
{
if (!flat.TryGetValue(Normalize(alias), out var value))
{
continue;
}

if (value.ValueKind == JsonValueKind.Number && value.TryGetDecimal(out var n))
{
return n;
}

if (value.ValueKind == JsonValueKind.String && decimal.TryParse(value.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var s))
{
return s;
}
}

return null;
}

private static DateTimeOffset Time(Dictionary<string, JsonElement> flat, params string[] aliases)
{
var text = Text(flat, aliases);
if (string.IsNullOrWhiteSpace(text))
{
return DateTimeOffset.MinValue;
}

if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var epoch))
{
return epoch > 10_000_000_000L ? DateTimeOffset.FromUnixTimeMilliseconds(epoch) : DateTimeOffset.FromUnixTimeSeconds(epoch);
}

return DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed) ? parsed.ToUniversalTime() : DateTimeOffset.MinValue;
}

private static bool Wild(string pattern, string value)
{
pattern = string.IsNullOrWhiteSpace(pattern) ? "*" : pattern.Trim();
value ??= "";
if (pattern == "*")
{
return true;
}

var parts = pattern.Split('*');
var cursor = 0;
for (var i = 0; i < parts.Length; i++)
{
var part = parts[i];
if (part.Length == 0)
{
continue;
}

var index = value.IndexOf(part, cursor, StringComparison.OrdinalIgnoreCase);
if (index < 0 || (i == 0 && !pattern.StartsWith("*", StringComparison.Ordinal) && index != 0))
{
return false;
}

cursor = index + part.Length;
}

return pattern.EndsWith("*", StringComparison.Ordinal) || parts.Length == 0 || value.EndsWith(parts[parts.Length - 1], StringComparison.OrdinalIgnoreCase);
}

private static bool Inside(DateTimeOffset a, DateTimeOffset b, int minutes)
{
return a == DateTimeOffset.MinValue || b == DateTimeOffset.MinValue || Math.Abs((b - a).TotalMinutes) <= minutes;
}

private static string Scope(string tenant, string provider, string model, string date)
{
return string.Join("/", new[] { tenant, provider, model, date }.Select(s => s.Trim().ToLowerInvariant()));
}

private static string Normalize(string value)
{
var builder = new StringBuilder(value.Length);
foreach (var ch in value)
{
if (char.IsLetterOrDigit(ch))
{
builder.Append(char.ToLowerInvariant(ch));
}
}

return builder.ToString();
}

private static decimal Percent(decimal numerator, decimal denominator)
{
return denominator == 0m ? numerator == 0m ? 0m : 100m : Math.Round(numerator / Math.Abs(denominator) * 100m, 4, MidpointRounding.AwayFromZero);
}

private static decimal Round(decimal value) => Math.Round(value, 6, MidpointRounding.AwayFromZero);
private static string Money(decimal value) => Round(value).ToString("0.######", CultureInfo.InvariantCulture);

private static int SelfTest(TextWriter writer)
{
var usage = new[]
{
"{\"timestamp\":\"2026-04-21T10:00:00Z\",\"request_id\":\"req-1\",\"idempotency_key\":\"idem-1\",\"tenant\":\"search\",\"provider\":\"openai\",\"model\":\"reasoning-small\",\"input_tokens\":1000000,\"output_tokens\":500000,\"cached_input_tokens\":400000,\"cost_usd\":2.00,\"status\":\"ok\"}",
"{\"timestamp\":\"2026-04-21T10:03:00Z\",\"request_id\":\"req-1b\",\"idempotency_key\":\"idem-1\",\"tenant\":\"search\",\"provider\":\"openai\",\"model\":\"reasoning-small\",\"input_tokens\":1000000,\"output_tokens\":500000,\"cached_input_tokens\":400000,\"cost_usd\":2.00,\"status\":\"ok\"}",
"{\"timestamp\":\"2026-04-21T10:05:00Z\",\"request_id\":\"req-2\",\"tenant\":\"search\",\"provider\":\"openai\",\"model\":\"reasoning-small\",\"input_tokens\":500000,\"output_tokens\":100000,\"cached_input_tokens\":600000,\"cost_usd\":0.20,\"status\":\"ok\"}",
"{\"timestamp\":\"2026-04-21T10:06:00Z\",\"request_id\":\"req-3\",\"tenant\":\"research\",\"provider\":\"other\",\"model\":\"unknown\",\"input_tokens\":100,\"output_tokens\":100,\"status\":\"500\"}"
};
var priceCsv = "provider,model,input_usd_per_million,output_usd_per_million,cached_input_usd_per_million,cache_write_usd_per_million\nopenai,reasoning-small,1.00,4.00,0.10,1.00\n";
var budgetCsv = "tenant,daily_budget_usd,monthly_budget_usd\nsearch,0.90,5.00\n";
var options = Options.Parse(new[] { "--telemetry", "unused.jsonl", "--max-drift-pct", "1", "--require-prices" });
var result = Reconcile(ReadUsageLines(usage).ToList(), ReadPricesFromCsv(priceCsv), new List<InvoiceRow>(), ReadBudgetsFromCsv(budgetCsv), options);
var required = new[] { "duplicate-replay-charge", "cache-tokens-exceed-input", "tenant-daily-budget-exceeded", "failed-request-billed", "unpriced-model" };
foreach (var code in required)
{
if (!result.Findings.Any(f => f.Code == code))
{
writer.WriteLine("self-test failed: missing " + code);
return 1;
}
}

using var json = new StringWriter(CultureInfo.InvariantCulture);
Emit(result, "json", json);
JsonDocument.Parse(json.ToString()).Dispose();
using var sarif = new StringWriter(CultureInfo.InvariantCulture);
Emit(result, "sarif", sarif);
JsonDocument.Parse(sarif.ToString()).Dispose();
writer.WriteLine("self-test passed");
return 0;
}

private static List<PriceRule> ReadPricesFromCsv(string csv) => CsvDictionaries(csv)
.Select(row => new PriceRule(Cell(row, "provider", fallback: "*"), Cell(row, "model", fallback: "*"), DecimalCell(row, "input_usd_per_million", "input"), DecimalCell(row, "output_usd_per_million", "output"), DecimalCell(row, "cached_input_usd_per_million", "cached_input", "cache_read"), DecimalCell(row, "cache_write_usd_per_million", "cache_write"), DecimalCell(row, "minimum_request_usd", "minimum")))
.OrderByDescending(r => r.Score)
.ToList();

private static List<BudgetRow> ReadBudgetsFromCsv(string csv) => CsvDictionaries(csv)
.Select(row => new BudgetRow(Cell(row, "tenant"), DecimalCell(row, "daily_budget_usd", "daily"), DecimalCell(row, "monthly_budget_usd", "monthly")))
.Where(row => !string.IsNullOrWhiteSpace(row.Tenant))
.ToList();
}

/*
This solves the April 2026 problem where AI teams trust gateway dashboards, provider invoice CSV files, and telemetry traces even when the numbers quietly disagree. Built because Pavan wanted a plain C# inference spend reconciliation ledger that a developer can drop into a .NET DevOps repository, GitHub Actions job, Azure pipeline, finance audit, or model gateway review without storing provider API keys or secrets. Use it when OpenAI, Anthropic, Azure OpenAI, Bedrock, local model gateway, proxy, or agent platform events arrive as JSONL and you need to explain duplicate idempotency charges, replay waste, prompt cache misses, cached token billing, tenant budget overruns, invoice drift, and model price book mistakes. The trick: normalize messy JSON field names, keep the price book in CSV, estimate each request deterministically, compare declared cost and invoice cost at request and tenant/provider/model/day levels, and emit text, JSON, or SARIF for engineers and finance reviewers. Drop this into any AI infrastructure repo that needs searchable GitHub code for LLM cost reconciliation, AI inference invoice audit, prompt cache billing verification, C# DevOps cost controls, and production-ready AI platform spend governance without a fragile spreadsheet.
*/
