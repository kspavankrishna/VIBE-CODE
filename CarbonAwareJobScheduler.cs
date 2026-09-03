#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace CarbonAwareScheduling;

/// <summary>One forecast bucket: the grid's carbon intensity, and optionally spot price, at a point in time.</summary>
public readonly record struct ForecastSample(DateTimeOffset Timestamp, double CarbonIntensityGco2PerKwh, double? PricePerKwh);

/// <summary>Thrown when a carbon forecast provider could not be reached after its retry budget is exhausted.</summary>
public sealed class CarbonForecastUnavailableException : Exception
{
    public CarbonForecastUnavailableException(string message, Exception? inner = null) : base(message, inner) { }
}

/// <summary>Source of forward-looking grid carbon intensity (and optionally price) data for a region/zone.</summary>
public interface ICarbonIntensityProvider
{
    Task<IReadOnlyList<ForecastSample>> GetForecastAsync(string region, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default);
}

/// <summary>Whatever actually runs the job once the scheduler decides "now" — a Kubernetes Job, a CI runner, a batch API call.</summary>
public interface IJobExecutor
{
    Task ExecuteAsync(ScheduledJob job, CancellationToken ct);
}

public enum JobStatus
{
    Pending,
    Scheduled,
    Dispatching,
    Completed,
    Failed,
}

/// <summary>A deferrable unit of work: a CI build, a batch/training run, a data pipeline job — anything that can
/// tolerate running somewhere inside a window instead of exactly now.</summary>
public sealed record ScheduledJob
{
    public required Guid Id { get; init; }
    public required string Region { get; init; }
    public required TimeSpan EstimatedDuration { get; init; }
    public required DateTimeOffset EarliestStart { get; init; }
    public required DateTimeOffset Deadline { get; init; }

    /// <summary>Rough energy draw for this job, used only to weight the carbon score consistently across jobs.</summary>
    public double EstimatedKwh { get; init; } = 1.0;

    public int Priority { get; init; }

    public string? Name { get; init; }
}

/// <summary>Live scheduling state for one submitted job, mutated in place as forecasts refresh.</summary>
public sealed class JobExecutionState
{
    public required ScheduledJob Job { get; init; }
    public JobStatus Status { get; set; } = JobStatus.Pending;
    public DateTimeOffset? PlannedStart { get; set; }
    public double? PlannedScore { get; set; }

    /// <summary>True if the plan is a deadline-safety fallback rather than a genuine low-carbon window
    /// (forecast ran out, the provider was down, or there simply wasn't enough runway to defer).</summary>
    public bool IsFallback { get; set; }

    public int RescheduleCount { get; set; }
    public Exception? LastError { get; set; }
}

public sealed class CarbonSchedulerOptions
{
    /// <summary>Granularity the forecast is expected to be reported at. Windows are evaluated in units of this size.</summary>
    public int ForecastBucketMinutes { get; init; } = 15;

    /// <summary>How often the scheduler re-pulls forecasts and re-scores all pending jobs.</summary>
    public TimeSpan ReevaluationInterval { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>How far ahead of a job's planned start the scheduler hands it to the executor.</summary>
    public TimeSpan DispatchLeadTime { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>Relative weight of grid carbon intensity in the composite score. Weights need not sum to 1.</summary>
    public double CarbonWeight { get; init; } = 0.7;

    /// <summary>Relative weight of electricity spot price in the composite score, when a provider supplies price.</summary>
    public double CostWeight { get; init; } = 0.3;

    public TimeSpan ForecastCacheTtl { get; init; } = TimeSpan.FromMinutes(30);
}

/// <summary>Result of scoring the best available execution window for one job against one forecast.</summary>
public readonly record struct WindowDecision(DateTimeOffset Start, double Score, bool IsFallback, bool ForecastCoverageComplete);

/// <summary>
/// The scheduling core, kept separate from I/O so it is trivial to unit test: given a forecast and a job's
/// constraints, find the contiguous window that minimizes weighted carbon + cost, in O(n) after sorting.
/// </summary>
public static class OptimalWindowFinder
{
    public static WindowDecision FindBestWindow(
        IReadOnlyList<ForecastSample> forecast,
        TimeSpan duration,
        DateTimeOffset earliestStart,
        DateTimeOffset deadline,
        int bucketMinutes,
        double carbonWeight,
        double costWeight)
    {
        if (duration <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(duration));

        if (deadline <= earliestStart)
        {
            // No room left at all. Fail open: run now rather than silently dropping the job.
            return new WindowDecision(earliestStart, double.PositiveInfinity, true, false);
        }

        var bucket = TimeSpan.FromMinutes(Math.Max(1, bucketMinutes));
        int bucketsNeeded = (int)Math.Ceiling(duration / bucket);
        if (bucketsNeeded < 1) bucketsNeeded = 1;

        var samples = forecast
            .Where(s => s.Timestamp >= earliestStart && s.Timestamp < deadline)
            .OrderBy(s => s.Timestamp)
            .ToArray();

        var latestPossibleStart = deadline - duration;
        var safeFallbackStart = latestPossibleStart < earliestStart ? earliestStart : latestPossibleStart;

        if (samples.Length < bucketsNeeded)
        {
            // Not enough forecast coverage to score a single full window. Take the latest slot that still
            // meets the deadline — carbon-blind, but correctness (never miss a deadline) always wins.
            return new WindowDecision(safeFallbackStart, double.PositiveInfinity, true, false);
        }

        int n = samples.Length;
        var carbonPrefix = new double[n + 1];
        var costPrefix = new double[n + 1];
        for (int i = 0; i < n; i++)
        {
            carbonPrefix[i + 1] = carbonPrefix[i] + samples[i].CarbonIntensityGco2PerKwh;
            costPrefix[i + 1] = costPrefix[i] + (samples[i].PricePerKwh ?? 0.0);
        }

        double bestScore = double.PositiveInfinity;
        int bestStartIndex = -1;

        // Classic prefix-sum sliding window: every candidate window's totals are O(1) to derive, so scanning
        // all valid start positions is O(n) total instead of O(n * windowSize).
        for (int start = 0; start + bucketsNeeded <= n; start++)
        {
            var windowStartTime = samples[start].Timestamp;
            if (windowStartTime > latestPossibleStart) break;

            double carbonSum = carbonPrefix[start + bucketsNeeded] - carbonPrefix[start];
            double costSum = costPrefix[start + bucketsNeeded] - costPrefix[start];

            double avgCarbon = carbonSum / bucketsNeeded;
            double avgCost = costSum / bucketsNeeded;
            double score = carbonWeight * avgCarbon + costWeight * avgCost;

            if (score < bestScore)
            {
                bestScore = score;
                bestStartIndex = start;
            }
        }

        if (bestStartIndex < 0)
        {
            return new WindowDecision(safeFallbackStart, double.PositiveInfinity, true, true);
        }

        return new WindowDecision(samples[bestStartIndex].Timestamp, bestScore, false, true);
    }
}

/// <summary>Wraps any provider with a per-region TTL cache, and serves stale data instead of failing outright
/// when the upstream call errors — a rate-limited or briefly-down forecast API should degrade, not break scheduling.</summary>
public sealed class CachingCarbonIntensityProvider : ICarbonIntensityProvider
{
    private readonly ICarbonIntensityProvider _inner;
    private readonly TimeSpan _ttl;
    private readonly ConcurrentDictionary<string, (DateTimeOffset FetchedAt, IReadOnlyList<ForecastSample> Samples)> _cache = new();

    public CachingCarbonIntensityProvider(ICarbonIntensityProvider inner, TimeSpan ttl)
    {
        _inner = inner;
        _ttl = ttl;
    }

    public async Task<IReadOnlyList<ForecastSample>> GetForecastAsync(string region, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        var key = $"{region}|{from:yyyyMMddHHmm}|{to:yyyyMMddHHmm}";

        if (_cache.TryGetValue(key, out var cached) && DateTimeOffset.UtcNow - cached.FetchedAt < _ttl)
        {
            return cached.Samples;
        }

        try
        {
            var fresh = await _inner.GetForecastAsync(region, from, to, ct).ConfigureAwait(false);
            _cache[key] = (DateTimeOffset.UtcNow, fresh);
            return fresh;
        }
        catch (Exception) when (cached.Samples is { Count: > 0 })
        {
            return cached.Samples;
        }
    }
}

/// <summary>
/// Real-shaped ElectricityMaps forecast client: GET /v3/carbon-intensity/forecast?zone={zone} with an
/// auth-token header, exponential-backoff retries, and a typed exception on exhausted retries so callers
/// can decide how to fail (the scheduler itself fails open rather than blocking a job past its deadline).
/// </summary>
public sealed class ElectricityMapsCarbonIntensityProvider : ICarbonIntensityProvider
{
    private const string Endpoint = "https://api.electricitymap.org/v3/carbon-intensity/forecast";

    private readonly HttpClient _http;
    private readonly string _apiToken;
    private readonly int _maxRetries;

    public ElectricityMapsCarbonIntensityProvider(HttpClient http, string apiToken, int maxRetries = 3)
    {
        _http = http;
        _apiToken = apiToken;
        _maxRetries = maxRetries;
    }

    public async Task<IReadOnlyList<ForecastSample>> GetForecastAsync(string region, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        Exception? lastError = null;

        for (int attempt = 0; attempt <= _maxRetries; attempt++)
        {
            if (attempt > 0)
            {
                var backoff = TimeSpan.FromSeconds(Math.Pow(2, attempt));
                await Task.Delay(backoff, ct).ConfigureAwait(false);
            }

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, $"{Endpoint}?zone={Uri.EscapeDataString(region)}");
                request.Headers.Add("auth-token", _apiToken);

                using var response = await _http.SendAsync(request, ct).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    lastError = new HttpRequestException($"ElectricityMaps returned {(int)response.StatusCode} for zone '{region}'.");
                    continue;
                }

                var payload = await response.Content
                    .ReadFromJsonAsync<ElectricityMapsForecastResponse>(cancellationToken: ct)
                    .ConfigureAwait(false);

                if (payload?.Forecast is null)
                {
                    lastError = new JsonException("ElectricityMaps response was missing the 'forecast' array.");
                    continue;
                }

                return payload.Forecast
                    .Where(f => f.Datetime >= from && f.Datetime <= to)
                    .Select(f => new ForecastSample(f.Datetime, f.CarbonIntensity, null))
                    .OrderBy(s => s.Timestamp)
                    .ToArray();
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && ex is HttpRequestException or TaskCanceledException or JsonException)
            {
                lastError = ex;
            }
        }

        throw new CarbonForecastUnavailableException(
            $"Failed to retrieve carbon forecast for zone '{region}' after {_maxRetries + 1} attempts.", lastError);
    }

    private sealed class ElectricityMapsForecastResponse
    {
        [JsonPropertyName("forecast")]
        public List<ElectricityMapsForecastEntry>? Forecast { get; set; }
    }

    private sealed class ElectricityMapsForecastEntry
    {
        [JsonPropertyName("carbonIntensity")]
        public double CarbonIntensity { get; set; }

        [JsonPropertyName("datetime")]
        public DateTimeOffset Datetime { get; set; }
    }
}

/// <summary>
/// Continuously re-scores every pending job against the freshest carbon/price forecast and dispatches it
/// at the lowest-scoring window that still respects its deadline. Designed to run as a long-lived background
/// loop (call <see cref="RunAsync"/> from a hosted service, a console app, or a worker) and to fail safe:
/// a dead forecast API or a job with no runway left never blocks execution past its deadline.
/// </summary>
public sealed class CarbonAwareScheduler
{
    private static readonly ActivitySource Activity = new("CarbonAwareScheduling.Scheduler");

    private readonly ICarbonIntensityProvider _provider;
    private readonly IJobExecutor _executor;
    private readonly CarbonSchedulerOptions _options;
    private readonly Action<string> _log;
    private readonly ConcurrentDictionary<Guid, JobExecutionState> _jobs = new();

    public CarbonAwareScheduler(
        ICarbonIntensityProvider provider,
        IJobExecutor executor,
        CarbonSchedulerOptions? options = null,
        Action<string>? logger = null)
    {
        _provider = provider;
        _executor = executor;
        _options = options ?? new CarbonSchedulerOptions();
        _log = logger ?? (_ => { });
    }

    /// <summary>Registers a job for carbon-aware scheduling. Safe to call from any thread at any time.</summary>
    public Guid Submit(ScheduledJob job)
    {
        _jobs[job.Id] = new JobExecutionState { Job = job };
        return job.Id;
    }

    public bool TryGetState(Guid jobId, out JobExecutionState? state) => _jobs.TryGetValue(jobId, out state);

    public IReadOnlyCollection<JobExecutionState> Snapshot() => _jobs.Values.ToArray();

    /// <summary>Runs the reevaluate-and-dispatch loop until <paramref name="ct"/> is cancelled.</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_options.ReevaluationInterval);

        // Plan immediately so freshly submitted jobs don't sit idle for a full interval before getting a start time.
        await TickAsync(ct).ConfigureAwait(false);

        while (!ct.IsCancellationRequested && await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
        {
            await TickAsync(ct).ConfigureAwait(false);
        }
    }

    internal async Task TickAsync(CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        var actionable = _jobs.Values
            .Where(s => s.Status is JobStatus.Pending or JobStatus.Scheduled)
            .ToArray();

        if (actionable.Length == 0) return;

        var forecastByRegion = new Dictionary<string, IReadOnlyList<ForecastSample>>();

        foreach (var group in actionable.GroupBy(s => s.Job.Region))
        {
            var horizonEnd = group.Max(s => s.Job.Deadline);
            try
            {
                forecastByRegion[group.Key] = await _provider
                    .GetForecastAsync(group.Key, now, horizonEnd, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log($"[carbon-scheduler] forecast unavailable for region '{group.Key}': {ex.Message}. " +
                     "Jobs in this region fall back to the latest deadline-safe start.");
                forecastByRegion[group.Key] = Array.Empty<ForecastSample>();
            }
        }

        foreach (var state in actionable)
        {
            try
            {
                EvaluateJob(state, forecastByRegion[state.Job.Region], now);
            }
            catch (Exception ex)
            {
                // One bad job must never take the reevaluation loop down for everyone else.
                state.LastError = ex;
                _log($"[carbon-scheduler] failed to evaluate job {state.Job.Id}: {ex.Message}");
            }
        }

        var dueForDispatch = _jobs.Values
            .Where(s => s.Status == JobStatus.Scheduled
                        && s.PlannedStart.HasValue
                        && s.PlannedStart.Value - now <= _options.DispatchLeadTime)
            .ToArray();

        foreach (var state in dueForDispatch)
        {
            _ = DispatchAsync(state, ct);
        }
    }

    private void EvaluateJob(JobExecutionState state, IReadOnlyList<ForecastSample> forecast, DateTimeOffset now)
    {
        var job = state.Job;
        var effectiveEarliestStart = job.EarliestStart > now ? job.EarliestStart : now;

        if (effectiveEarliestStart >= job.Deadline)
        {
            state.PlannedStart = now;
            state.PlannedScore = null;
            state.IsFallback = true;
            state.Status = JobStatus.Scheduled;
            return;
        }

        var decision = OptimalWindowFinder.FindBestWindow(
            forecast,
            job.EstimatedDuration,
            effectiveEarliestStart,
            job.Deadline,
            _options.ForecastBucketMinutes,
            _options.CarbonWeight,
            _options.CostWeight);

        using var activity = Activity.StartActivity("carbon_scheduler.reschedule", ActivityKind.Internal);
        activity?.SetTag("job.id", job.Id);
        activity?.SetTag("job.region", job.Region);
        activity?.SetTag("decision.start", decision.Start);
        activity?.SetTag("decision.score", decision.Score);
        activity?.SetTag("decision.is_fallback", decision.IsFallback);

        state.PlannedStart = decision.Start;
        state.PlannedScore = double.IsPositiveInfinity(decision.Score) ? null : decision.Score;
        state.IsFallback = decision.IsFallback;
        state.RescheduleCount++;
        state.Status = JobStatus.Scheduled;
    }

    private async Task DispatchAsync(JobExecutionState state, CancellationToken ct)
    {
        if (!TryTransition(state, JobStatus.Scheduled, JobStatus.Dispatching)) return;

        using var activity = Activity.StartActivity("carbon_scheduler.dispatch", ActivityKind.Producer);
        activity?.SetTag("job.id", state.Job.Id);
        activity?.SetTag("job.fallback", state.IsFallback);

        try
        {
            await _executor.ExecuteAsync(state.Job, ct).ConfigureAwait(false);
            state.Status = JobStatus.Completed;
        }
        catch (Exception ex)
        {
            state.LastError = ex;
            state.Status = JobStatus.Failed;
            _log($"[carbon-scheduler] job {state.Job.Id} failed during execution: {ex.Message}");
        }
    }

    private static bool TryTransition(JobExecutionState state, JobStatus expected, JobStatus next)
    {
        lock (state)
        {
            if (state.Status != expected) return false;
            state.Status = next;
            return true;
        }
    }
}

// ---------------------------------------------------------------------------------------------------------
// WHAT THIS IS AND WHY IT EXISTS
//
// This solves the problem of GPU training runs, CI/CD pipelines, and batch data jobs all running the instant
// they're triggered, on whatever electricity the grid happens to be burning at that second, even when the
// job has hours of slack before it actually needs to finish. Most teams pay for GPU-hours and complain about
// their cloud carbon numbers, but nothing in a normal .NET/Kubernetes/CI stack ever asks "could this job wait
// forty minutes for a cleaner half hour on the grid without breaking anything." This file is that missing
// piece: a carbon-aware job scheduler for C#/.NET that takes a job with a duration estimate and a hard
// deadline, pulls a real grid-carbon-intensity forecast (wired to the ElectricityMaps API, the same kind of
// feed used by Google's and Microsoft's own carbon-aware scheduling work), and picks the lowest-carbon window
// inside that deadline using a prefix-sum sliding window so the search is O(n) instead of the naive O(n *
// window size). Built because carbon credits and offsets treat the symptom after the fact, buying paper to
// cancel emissions that already happened, while shifting compute in time is free, needs no purchase, and
// actually cuts the energy mix a job draws from. Use it when you run recurring GPU/CPU-heavy jobs — model
// training, nightly batch ETL, large CI matrices, backfills — that have real slack between "earliest it can
// start" and "latest it must finish," and where you want that slack spent automatically instead of manually
// re-triggering jobs at 3am when the grid is cleaner. The trick is treating scheduling as a pure, testable
// algorithm (OptimalWindowFinder) completely separate from the I/O-heavy parts (the HTTP calls to the
// forecast API, the caching, the actual dispatch to Kubernetes/CI/whatever executor you plug in), so the
// carbon math can be unit tested in milliseconds while the live system still fails open: if the forecast API
// is down, rate-limited, or simply doesn't have enough coverage, the scheduler falls back to the latest slot
// that still meets the deadline rather than ever silently dropping or indefinitely delaying a job past when
// it was promised. Drop this into any .NET worker service, Kubernetes controller, Azure Function, or CI
// orchestrator that currently fires jobs on a cron or on-trigger basis — implement ICarbonIntensityProvider
// against whatever forecast feed you have access to (ElectricityMaps, WattTime, a national grid API, or a
// static file for testing), implement IJobExecutor to call your existing job-launch code, submit jobs with a
// duration and deadline, and call RunAsync once from your app's startup. No other infrastructure required.
// ---------------------------------------------------------------------------------------------------------
