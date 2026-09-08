# Carbon Aware Job Scheduler

GPU training runs, CI matrices and nightly ETL fire the instant they are triggered, burning whatever the grid happens to be generating at that second, even when the job has six hours of slack. This is a single file C# scheduler that spends that slack for you: give it a duration estimate and a hard deadline, and it runs the job in the cleanest window the grid forecast offers inside that deadline.

**Language:** C# | **Lines:** 514 | **Added:** 2026-09-03

## What this solves

The failure mode is not a crash. It is a silent, permanent tax. A nightly training job kicks off at 19:00 because that is what the cron says. At 19:00 the grid in that region is running peak gas because everyone got home and turned on the air conditioning. The same job at 03:00 would have drawn from wind that is currently being curtailed, at a fraction of the carbon intensity and often a lower spot price. Nothing in a normal .NET, Kubernetes or CI stack ever asks whether the job could have waited. So it never does, every night, for years.

Nobody notices until someone has to produce a sustainability number, and the honest answer to why the emissions look like that is that no one ever looked. The same problem shows up as cloud spend, because carbon intensity and electricity price are correlated in most grids. You are paying more for worse.

The naive fix breaks things. Somebody writes a script that delays jobs until 03:00, the forecast API rate limits during an incident, and a batch job with a contractual delivery deadline sits waiting for data that will never arrive. Or the forecast covers twelve hours, the job window is thirty, and the scheduler has no opinion about the uncovered part. Deferral without a hard deadline guarantee is worse than no deferral, because the failure stays invisible until the deadline is already missed. This file handles both halves: it finds the genuinely cheapest window when it can, and otherwise falls back to the latest start that still meets the deadline and marks the plan as a fallback so you see it in telemetry.

## Why I built it

Carbon credits and offsets treat the symptom after the fact. You buy paper to cancel emissions that already happened. Shifting compute in time is free and actually changes the energy mix a job draws from. The tooling exists in research papers and in Google's and Microsoft's internal systems, but there is no drop in piece for a .NET worker service that already runs jobs on a cron.

The other gap was testability. Every carbon scheduling example I found welded the HTTP call to the scoring logic, so you could not unit test "does this pick the right window" without a live API key and a network.

## When to use it

- Nightly batch ETL or backfills that must finish before the morning reports but can start any time after the source system closes.
- Model training and fine tuning runs where the only real constraint is "results by standup tomorrow".
- Large CI matrices or scheduled regression suites that nobody watches in real time.
- Jobs in a region with a volatile grid mix, where intraday carbon intensity swings by a factor of two or more.
- Any cron workload someone has manually re-triggered at 3am to save money.
- You want an audit trail showing which jobs genuinely shifted and which were deadline fallbacks.

## How it works

The scoring core is `OptimalWindowFinder.FindBestWindow`, a static function with no I/O. It takes a forecast, a duration, an earliest start, a deadline and the two weights, and returns a `WindowDecision(Start, Score, IsFallback, ForecastCoverageComplete)`. It filters the `ForecastSample` list to the legal range, sorts by timestamp, converts the duration into a bucket count via `Math.Ceiling(duration / bucket)`, then builds two prefix sum arrays, one for carbon and one for price. Every candidate window's total is then two subtractions, so scanning all valid start positions is O(n) instead of the naive O(n × windowSize). The score is `carbonWeight * avgCarbon + costWeight * avgCost`, and the loop breaks once a candidate start passes `deadline - duration`. Weights need not sum to 1.

Two fallback paths exist and both are deliberate. If `deadline <= earliestStart` there is no room at all, so it returns the earliest start with a score of `double.PositiveInfinity` and `IsFallback` true. If the forecast has fewer samples than the window needs, it returns `safeFallbackStart`, which is `deadline - duration` clamped to never precede the earliest start. That is the latest slot that still meets the deadline. Carbon blind, but the deadline holds.

`CarbonAwareScheduler` is the live loop. `Submit` registers a job into a `ConcurrentDictionary<Guid, JobExecutionState>` from any thread. `RunAsync` calls `TickAsync` once immediately so fresh jobs do not idle for a whole interval, then drives it from a `PeriodicTimer` on `ReevaluationInterval`. Each tick groups actionable jobs by region, pulls one forecast per region out to the furthest deadline in that group, and on any provider exception logs and substitutes an empty forecast so those jobs take the deadline safe fallback rather than throwing. Every job is then scored inside its own try/catch, because one bad job must never take down the loop for everyone else. Jobs whose planned start is within `DispatchLeadTime` go to `DispatchAsync`.

State transitions go through `TryTransition`, a compare and set under a `lock` on the state object, which is what stops a job being dispatched twice when two ticks overlap. Every reschedule and dispatch emits an `Activity` from the `ActivitySource` named `CarbonAwareScheduling.Scheduler`, tagged with job id, region, chosen start, score and fallback flag, so an OpenTelemetry exporter picks it up with no extra wiring.

On the I/O side, `ElectricityMapsCarbonIntensityProvider` does a GET against `https://api.electricitymap.org/v3/carbon-intensity/forecast?zone={zone}` with the token in an `auth-token` header, retrying on `HttpRequestException`, `TaskCanceledException` and `JsonException` with exponential backoff of `2^attempt` seconds, then throwing `CarbonForecastUnavailableException` with the last error attached. `CachingCarbonIntensityProvider` wraps any provider with a per key TTL cache and serves stale samples when the inner call throws. A rate limited forecast API should degrade, not break scheduling.

## Usage

```csharp
using CarbonAwareScheduling;

// 1. Plug in whatever actually launches your work.
sealed class KubernetesJobExecutor : IJobExecutor
{
    public Task ExecuteAsync(ScheduledJob job, CancellationToken ct)
        => MyExistingLauncher.StartAsync(job.Name ?? job.Id.ToString(), ct);
}

var http = new HttpClient();
var provider = new CachingCarbonIntensityProvider(
    new ElectricityMapsCarbonIntensityProvider(http, apiToken: Environment.GetEnvironmentVariable("EM_TOKEN")!, maxRetries: 3),
    ttl: TimeSpan.FromMinutes(30));

var scheduler = new CarbonAwareScheduler(
    provider,
    new KubernetesJobExecutor(),
    new CarbonSchedulerOptions
    {
        ForecastBucketMinutes  = 60,                      // match your feed's granularity
        ReevaluationInterval   = TimeSpan.FromMinutes(5),
        DispatchLeadTime       = TimeSpan.FromMinutes(1),
        CarbonWeight           = 0.7,
        CostWeight             = 0.3,
    },
    logger: Console.WriteLine);

var id = scheduler.Submit(new ScheduledJob
{
    Id               = Guid.NewGuid(),
    Name             = "nightly-embedding-refresh",
    Region           = "DE",                              // ElectricityMaps zone
    EstimatedDuration = TimeSpan.FromHours(2),
    EarliestStart    = DateTimeOffset.UtcNow,
    Deadline         = DateTimeOffset.UtcNow.AddHours(14),
});

// Fire the loop once from startup. It runs until the token is cancelled.
_ = scheduler.RunAsync(cts.Token);

if (scheduler.TryGetState(id, out var state))
    Console.WriteLine($"{state!.Job.Name}: {state.PlannedStart} score={state.PlannedScore} fallback={state.IsFallback}");

foreach (var s in scheduler.Snapshot())
    Console.WriteLine($"{s.Job.Id} {s.Status} reschedules={s.RescheduleCount}");
```

To test the algorithm alone, call `OptimalWindowFinder.FindBestWindow` with a hand built `ForecastSample[]`. No network, no mocks.

## Notes

- All state lives in memory. A process restart loses every submitted job, planned start and status. Persist submissions yourself if that matters.
- `ScheduledJob.EstimatedKwh`, `Priority` and `Name` are carried through but the scoring and dispatch logic never reads them. Priority does not order dispatch.
- `CarbonSchedulerOptions.ForecastCacheTtl` exists but is not wired into anything. Pass the TTL to the `CachingCarbonIntensityProvider` constructor instead.
- The ElectricityMaps provider always sets `PricePerKwh` to null, so `CostWeight` contributes zero unless you implement `ICarbonIntensityProvider` against a feed supplying price.
- The cache key is `region|from|to` to the minute and `from` is `UtcNow` at each tick, so consecutive ticks miss. It buys you outage tolerance more than reduced request volume.
- Dispatch is fire and forget (`_ = DispatchAsync(...)`). Failures land on `LastError` with status `Failed` and are logged, never rethrown, and nothing caps simultaneous dispatches.
- Window starts snap to forecast sample timestamps. Set `ForecastBucketMinutes` to match your feed or the bucket count will be wrong.
- Needs .NET 6 or later for `PeriodicTimer`, and C# 11 for `required` members. No NuGet packages beyond the base class library.
