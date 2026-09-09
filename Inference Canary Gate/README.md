# Inference Canary Gate

A canary rollout looks fine on average latency and error count, then p99 doubles and the slowest users start timing out. This is a single file C# canary gate that compares a baseline arm against a candidate arm and returns promote, hold or abort with the evidence attached.

**Language:** C# | **Lines:** 886 | **Added:** 2026-04-22

## What this solves

This solves canary deployment analysis for AI inference services, ASP.NET APIs, model gateways and .NET background workers where average latency hides the real problem. When you roll out a new OpenAI compatible proxy, an Anthropic integration, an Azure OpenAI path or a token accounting change, you care about three things at once: did failures go up, did slow requests go up and did p95 or p99 get ugly. Most rollout code answers at most one of those.

The failure mode is a candidate that is indistinguishable on the mean and clearly worse in the tail. Baseline p99 is 3.1 seconds, the candidate lands at 4.4 because a new streaming path adds a buffer flush under contention, and mean latency moves 40ms. Nobody notices on the dashboard. Meanwhile the 1 percent of requests that were already slow cross the client timeout, retries pile up, rate limits fire back 429s and a small regression becomes a queue backup. The people who notice are on call at 2am, not in the release review.

The opposite failure is promoting on noise. Forty samples in the canary, zero failures, someone says ship it. Forty samples with zero failures is consistent with a true failure rate above 7 percent, so a point estimate at that size is not evidence. This gate refuses to decide until each arm has enough traffic and compares interval bounds rather than point rates. There is a cost dimension too: two candidates can post identical success rates and very different token throughput because a prompt change altered output length, so both arms carry tokens per request and tokens per second.

## Why I built it

A lot of rollout code in real teams is still too shallow. People compare averages, eyeball Grafana or hardcode a one off threshold in a YAML file, then miss the exact regressions that hurt users. Progressive delivery tooling that does this properly assumes a service mesh, a metrics backend and a controller, which is a lot of infrastructure before you can answer one question about one deployment.

I wanted a C# canary gate a backend engineer can drop into a service, a deployment controller or a load test harness without a statistics package, a database or an observability stack. The bounded window keeps memory predictable, Wilson score intervals keep the rate checks honest at small sample sizes and the tail guard catches the case where the mean looks fine and p99 exploded.

## When to use it

- Shipping a new inference model or provider behind an existing API and needing an objective promote or roll back signal.
- Changing provider failover, retry or cache behaviour where the regression shows up only in the tail.
- Swapping prompt construction and needing rollout evidence that includes cost per request.
- Rolling an MCP or agent runtime change against live baseline traffic before cutover.
- Gating a risky ASP.NET middleware release inside a deployment job.
- Load testing two builds and wanting a decision rather than two CSV files.

## How it works

Each arm gets an `ObservationWindow`, a fixed size ring buffer sized by `MaxWindowSamples` (default 4096). `Add` writes into the free slot until the buffer fills, then overwrites the oldest entry and advances `_start`. That is the whole memory story: two arrays allocated once, no growth, no eviction pass, no time based expiry. All mutation goes through one `lock (_sync)`, and `Evaluate` takes both snapshots inside that lock then releases it before any math, so recording threads are never blocked by analysis.

`Record` classifies failure at write time: a request is a failure if `Succeeded` is false, or the status is 500 or above, or `CountClientErrorsAsFailures` is set and the status is 4xx. `Analyze` then makes one pass over the snapshot, accumulating mean and variance with Welford's online algorithm to avoid the catastrophic cancellation of a naive sum of squares, and counting failures, slow requests (at or above `SlowRequestThreshold`, default 4 seconds), 2xx, 429, 4xx and 5xx, min and max latency and total tokens. Then it sorts the latency array and reads p50, p95 and p99 with linear interpolation between neighbouring order statistics. Exact percentiles over the window, not a sketch, which is affordable because the window is bounded.

Rate uncertainty comes from `WilsonInterval.FromCounts`. Wilson beats the normal approximation because failure rates in a healthy service sit near zero, where the normal interval degenerates: it reaches below zero or collapses to zero width at exactly the point you care about. Wilson stays inside [0, 1] at small n. Its z value comes from `InferenceCanaryMath.InverseStandardNormalCdf`, Acklam's rational approximation to the inverse normal CDF, which keeps the file dependency free.

`Decide` runs a fixed ladder. Below `MinDecisionSamplesPerArm` (default 200) on either arm it holds and says so. Next it builds both rate intervals at `AbortConfidence` (default 0.99) and aborts if the candidate lower bound sits above the baseline upper bound plus the allowed delta. Stricter confidence for aborts and looser `PromoteConfidence` (default 0.95) for promotions is deliberate: a high bar for calling a regression real, a separate high bar for calling a candidate clean. Then the tail guard, where allowed p95 is `baseline.P95 * MaxP95RegressionRatio + AbsoluteP95Slack` and p99 has the same shape. The absolute slack matters because a pure ratio is useless on fast endpoints where 10 percent of 20ms is noise. It aborts only when p95 and p99 both breach, so one noisy quantile cannot kill a rollout.

Only after every hard stop passes does it ask about promotion, requiring the candidate upper bound at or below the baseline upper bound plus delta on both rates, with both tail quantiles inside their allowed values. The tail check arms separately at `MinQuantileSamplesPerArm` (default 400), so a candidate with enough traffic to decide but not enough to trust its p99 gets a hold and a signal saying the guard is not armed. Every branch appends a readable line to `Signals`, and the returned `CanaryEvaluation` carries the decision, a summary, those signals and an `InferenceArmSnapshot` per arm.

## Usage

```csharp
using VibeCode;

var gate = new InferenceCanaryGate(new InferenceCanaryGateOptions
{
    MaxWindowSamples = 4096,
    MinDecisionSamplesPerArm = 200,
    MinQuantileSamplesPerArm = 400,
    PromoteConfidence = 0.95,
    AbortConfidence = 0.99,
    AllowedFailureRateDelta = 0.0025,
    AllowedSlowRateDelta = 0.01,
    SlowRequestThreshold = TimeSpan.FromSeconds(4),
    MaxP95RegressionRatio = 1.10,
    MaxP99RegressionRatio = 1.15,
    AbsoluteP95Slack = TimeSpan.FromMilliseconds(150),
    AbsoluteP99Slack = TimeSpan.FromMilliseconds(250),
    CountClientErrorsAsFailures = false,
});

// Feed it from your HTTP client, proxy, queue consumer or load harness.
gate.RecordBaseline(InferenceObservation.Success(
    latency: TimeSpan.FromMilliseconds(820),
    statusCode: 200,
    inputTokens: 1450,
    outputTokens: 260));

gate.RecordCandidate(InferenceObservation.Failure(
    latency: TimeSpan.FromMilliseconds(5200),
    statusCode: 503));

gate.RecordRange(DeploymentArm.Candidate, candidateObservations);

var result = gate.Evaluate();

Console.WriteLine($"{result.Decision}: {result.Summary}");
foreach (var signal in result.Signals)
{
    Console.WriteLine($"  - {signal}");
}

Console.WriteLine($"p99 = {result.Candidate.P99Latency.TotalMilliseconds:0.##}ms");
Console.WriteLine($"failures = {result.Candidate.FailureRate:P2} " +
                  $"[{result.Candidate.FailureRateInterval.Lower:P2}, {result.Candidate.FailureRateInterval.Upper:P2}]");
Console.WriteLine($"tokens/sec = {result.Candidate.TokensPerSecond:0.##}");

switch (result.Decision)
{
    case CanaryDecisionKind.Promote: await rollout.ShiftAllTrafficAsync(); break;
    case CanaryDecisionKind.Abort:   await rollout.RollBackAsync();       break;
    case CanaryDecisionKind.Hold:    break;
}

gate.Reset(DeploymentArm.Candidate); // or Reset() to clear both arms
```

## Notes

- No CLI, no `Main`, no I/O and nothing async. It is a library type in namespace `VibeCode` hosted in your own service, job or test. It never calls your rollout controller, it returns a decision.
- The window is sample bounded, not time bounded. If traffic stops, stale samples sit in the buffer until new ones push them out. For a time horizon, call `Reset` on a schedule.
- Promotion is impossible until both arms reach `MinQuantileSamplesPerArm`, because `quantileReady` requires an armed tail guard. Below that, the gate can only hold or abort.
- Abort on tail latency requires p95 and p99 to both breach. A candidate that blows up p99 alone will not abort, but it will not promote either, it holds with a signal naming the breach.
- `Evaluate` sorts a copy of each window, so it is O(n log n) per call with n capped at `MaxWindowSamples`. Fine every few seconds, wasteful per request. `Validate()` runs in the constructor and throws on bad options, including `AbortConfidence` below `PromoteConfidence`.
- Rate comparison is a bounds test with a slack term, not a two proportion hypothesis test, and it does not correct for repeated looks at the same data. Treat a long run of `Evaluate` calls as monitoring, not a controlled sequential test.
