using System;
using System.Collections.Generic;
using System.Globalization;

namespace VibeCode;

/// <summary>
/// Makes promote, hold, or abort decisions for a canary rollout by comparing recent baseline and
/// candidate observations with bounded windows, Wilson score intervals, and tail-latency guard rails.
/// </summary>
public sealed class InferenceCanaryGate
{
    private readonly object _sync = new();
    private readonly InferenceCanaryGateOptions _options;
    private readonly ObservationWindow _baseline;
    private readonly ObservationWindow _candidate;

    public InferenceCanaryGate(InferenceCanaryGateOptions? options = null)
    {
        _options = options ?? new InferenceCanaryGateOptions();
        _options.Validate();

        _baseline = new ObservationWindow(_options.MaxWindowSamples);
        _candidate = new ObservationWindow(_options.MaxWindowSamples);
    }

    public InferenceCanaryGateOptions Options => _options;

    public void Record(DeploymentArm arm, InferenceObservation observation)
    {
        var sample = ObservationSample.FromObservation(observation, _options);

        lock (_sync)
        {
            GetWindow(arm).Add(sample);
        }
    }

    public void RecordRange(DeploymentArm arm, IEnumerable<InferenceObservation> observations)
    {
        ArgumentNullException.ThrowIfNull(observations);

        lock (_sync)
        {
            var window = GetWindow(arm);
            foreach (var observation in observations)
            {
                window.Add(ObservationSample.FromObservation(observation, _options));
            }
        }
    }

    public void RecordBaseline(InferenceObservation observation) => Record(DeploymentArm.Baseline, observation);

    public void RecordCandidate(InferenceObservation observation) => Record(DeploymentArm.Candidate, observation);

    public void Reset(DeploymentArm? arm = null)
    {
        lock (_sync)
        {
            if (arm is null || arm == DeploymentArm.Baseline)
            {
                _baseline.Clear();
            }

            if (arm is null || arm == DeploymentArm.Candidate)
            {
                _candidate.Clear();
            }
        }
    }

    public CanaryEvaluation Evaluate()
    {
        ObservationSample[] baselineSamples;
        ObservationSample[] candidateSamples;

        lock (_sync)
        {
            baselineSamples = _baseline.Snapshot();
            candidateSamples = _candidate.Snapshot();
        }

        var baseline = Analyze(DeploymentArm.Baseline, baselineSamples);
        var candidate = Analyze(DeploymentArm.Candidate, candidateSamples);

        return Decide(baseline, candidate);
    }

    private ObservationWindow GetWindow(DeploymentArm arm)
        => arm == DeploymentArm.Baseline ? _baseline : _candidate;

    private ArmMetrics Analyze(DeploymentArm arm, ObservationSample[] samples)
    {
        if (samples.Length == 0)
        {
            return ArmMetrics.Empty(arm);
        }

        var latencies = new double[samples.Length];
        double meanLatencyMs = 0d;
        double latencyM2 = 0d;
        double minLatencyMs = double.MaxValue;
        double maxLatencyMs = 0d;
        long firstTimestampMs = long.MaxValue;
        long lastTimestampMs = long.MinValue;
        long totalTokens = 0L;
        int failureCount = 0;
        int slowCount = 0;
        int status2xxCount = 0;
        int status429Count = 0;
        int status4xxCount = 0;
        int status5xxCount = 0;

        for (int index = 0; index < samples.Length; index++)
        {
            var sample = samples[index];
            latencies[index] = sample.LatencyMs;

            if (sample.Failure)
            {
                failureCount++;
            }

            if (sample.LatencyMs >= _options.SlowRequestThreshold.TotalMilliseconds)
            {
                slowCount++;
            }

            if (sample.StatusCode >= 200 && sample.StatusCode < 300)
            {
                status2xxCount++;
            }

            if (sample.StatusCode == 429)
            {
                status429Count++;
            }

            if (sample.StatusCode >= 400 && sample.StatusCode < 500)
            {
                status4xxCount++;
            }

            if (sample.StatusCode >= 500)
            {
                status5xxCount++;
            }

            minLatencyMs = Math.Min(minLatencyMs, sample.LatencyMs);
            maxLatencyMs = Math.Max(maxLatencyMs, sample.LatencyMs);
            firstTimestampMs = Math.Min(firstTimestampMs, sample.UnixTimeMilliseconds);
            lastTimestampMs = Math.Max(lastTimestampMs, sample.UnixTimeMilliseconds);
            totalTokens += sample.TotalTokens;

            var delta = sample.LatencyMs - meanLatencyMs;
            meanLatencyMs += delta / (index + 1);
            var delta2 = sample.LatencyMs - meanLatencyMs;
            latencyM2 += delta * delta2;
        }

        Array.Sort(latencies);

        var sampleCount = samples.Length;
        var failureRate = failureCount / (double)sampleCount;
        var slowRate = slowCount / (double)sampleCount;
        var variance = sampleCount > 1 ? latencyM2 / (sampleCount - 1) : 0d;
        var standardDeviationLatencyMs = Math.Sqrt(Math.Max(variance, 0d));
        var observedSeconds = sampleCount > 1
            ? Math.Max(0.001d, (lastTimestampMs - firstTimestampMs) / 1000d)
            : 0d;
        var requestsPerSecond = sampleCount > 1 ? sampleCount / observedSeconds : sampleCount;
        var meanTokensPerRequest = totalTokens / (double)sampleCount;
        var tokensPerSecond = sampleCount > 1 ? totalTokens / observedSeconds : totalTokens;

        return new ArmMetrics
        {
            Arm = arm,
            SampleCount = sampleCount,
            FailureCount = failureCount,
            SlowCount = slowCount,
            Status2xxCount = status2xxCount,
            Status429Count = status429Count,
            Status4xxCount = status4xxCount,
            Status5xxCount = status5xxCount,
            FailureRate = failureRate,
            SlowRate = slowRate,
            FailureRateInterval = WilsonInterval.FromCounts(failureCount, sampleCount, _options.PromoteConfidence),
            SlowRateInterval = WilsonInterval.FromCounts(slowCount, sampleCount, _options.PromoteConfidence),
            MeanLatencyMs = meanLatencyMs,
            StandardDeviationLatencyMs = standardDeviationLatencyMs,
            P50LatencyMs = Statistics.PercentileSorted(latencies, 0.50d),
            P95LatencyMs = Statistics.PercentileSorted(latencies, 0.95d),
            P99LatencyMs = Statistics.PercentileSorted(latencies, 0.99d),
            MinLatencyMs = minLatencyMs,
            MaxLatencyMs = maxLatencyMs,
            RequestsPerSecond = requestsPerSecond,
            MeanTokensPerRequest = meanTokensPerRequest,
            TokensPerSecond = tokensPerSecond,
            FirstObservedAt = DateTimeOffset.FromUnixTimeMilliseconds(firstTimestampMs),
            LastObservedAt = DateTimeOffset.FromUnixTimeMilliseconds(lastTimestampMs),
        };
    }

    private CanaryEvaluation Decide(ArmMetrics baseline, ArmMetrics candidate)
    {
        var signals = new List<string>();

        if (baseline.SampleCount < _options.MinDecisionSamplesPerArm || candidate.SampleCount < _options.MinDecisionSamplesPerArm)
        {
            signals.Add(
                $"Need at least {_options.MinDecisionSamplesPerArm} samples per arm before making a decision. " +
                $"Baseline has {baseline.SampleCount}; candidate has {candidate.SampleCount}.");

            return BuildEvaluation(
                CanaryDecisionKind.Hold,
                "Waiting for enough canary traffic before making a rollout decision.",
                signals,
                baseline,
                candidate);
        }

        var baselineAbortFailure = WilsonInterval.FromCounts(baseline.FailureCount, baseline.SampleCount, _options.AbortConfidence);
        var candidateAbortFailure = WilsonInterval.FromCounts(candidate.FailureCount, candidate.SampleCount, _options.AbortConfidence);
        var baselineAbortSlow = WilsonInterval.FromCounts(baseline.SlowCount, baseline.SampleCount, _options.AbortConfidence);
        var candidateAbortSlow = WilsonInterval.FromCounts(candidate.SlowCount, candidate.SampleCount, _options.AbortConfidence);

        if (candidateAbortFailure.Lower > baselineAbortFailure.Upper + _options.AllowedFailureRateDelta)
        {
            signals.Add(
                $"Candidate failure lower bound {AsPercent(candidateAbortFailure.Lower)} is above baseline upper bound " +
                $"{AsPercent(baselineAbortFailure.Upper)} plus allowed delta {AsPercent(_options.AllowedFailureRateDelta)}.");

            return BuildEvaluation(
                CanaryDecisionKind.Abort,
                "Abort rollout: candidate failure rate is materially worse than baseline.",
                signals,
                baseline,
                candidate);
        }

        if (candidateAbortSlow.Lower > baselineAbortSlow.Upper + _options.AllowedSlowRateDelta)
        {
            signals.Add(
                $"Candidate slow-request lower bound {AsPercent(candidateAbortSlow.Lower)} is above baseline upper bound " +
                $"{AsPercent(baselineAbortSlow.Upper)} plus allowed delta {AsPercent(_options.AllowedSlowRateDelta)}.");

            return BuildEvaluation(
                CanaryDecisionKind.Abort,
                "Abort rollout: candidate slow-request rate is materially worse than baseline.",
                signals,
                baseline,
                candidate);
        }

        var hasQuantileGuard = baseline.SampleCount >= _options.MinQuantileSamplesPerArm
            && candidate.SampleCount >= _options.MinQuantileSamplesPerArm;

        var allowedP95LatencyMs = baseline.P95LatencyMs * _options.MaxP95RegressionRatio + _options.AbsoluteP95Slack.TotalMilliseconds;
        var allowedP99LatencyMs = baseline.P99LatencyMs * _options.MaxP99RegressionRatio + _options.AbsoluteP99Slack.TotalMilliseconds;

        if (hasQuantileGuard
            && candidate.P95LatencyMs > allowedP95LatencyMs
            && candidate.P99LatencyMs > allowedP99LatencyMs)
        {
            signals.Add(
                $"Candidate tail latency exceeded the hard guard. p95={AsMilliseconds(candidate.P95LatencyMs)} " +
                $"allowed={AsMilliseconds(allowedP95LatencyMs)}; p99={AsMilliseconds(candidate.P99LatencyMs)} " +
                $"allowed={AsMilliseconds(allowedP99LatencyMs)}.");

            return BuildEvaluation(
                CanaryDecisionKind.Abort,
                "Abort rollout: candidate tail latency regressed past the configured guard rails.",
                signals,
                baseline,
                candidate);
        }

        if (!hasQuantileGuard)
        {
            signals.Add(
                $"Tail-latency guard arms at {_options.MinQuantileSamplesPerArm} samples per arm. " +
                $"Baseline has {baseline.SampleCount}; candidate has {candidate.SampleCount}.");
        }

        var baselinePromoteFailure = WilsonInterval.FromCounts(baseline.FailureCount, baseline.SampleCount, _options.PromoteConfidence);
        var candidatePromoteFailure = WilsonInterval.FromCounts(candidate.FailureCount, candidate.SampleCount, _options.PromoteConfidence);
        var baselinePromoteSlow = WilsonInterval.FromCounts(baseline.SlowCount, baseline.SampleCount, _options.PromoteConfidence);
        var candidatePromoteSlow = WilsonInterval.FromCounts(candidate.SlowCount, candidate.SampleCount, _options.PromoteConfidence);

        var failureReady = candidatePromoteFailure.Upper <= baselinePromoteFailure.Upper + _options.AllowedFailureRateDelta;
        var slowReady = candidatePromoteSlow.Upper <= baselinePromoteSlow.Upper + _options.AllowedSlowRateDelta;
        var quantileReady = hasQuantileGuard
            && candidate.P95LatencyMs <= allowedP95LatencyMs
            && candidate.P99LatencyMs <= allowedP99LatencyMs;

        if (!failureReady)
        {
            signals.Add(
                $"Candidate failure upper bound {AsPercent(candidatePromoteFailure.Upper)} still exceeds baseline upper bound " +
                $"{AsPercent(baselinePromoteFailure.Upper)} plus delta {AsPercent(_options.AllowedFailureRateDelta)}.");
        }

        if (!slowReady)
        {
            signals.Add(
                $"Candidate slow-request upper bound {AsPercent(candidatePromoteSlow.Upper)} still exceeds baseline upper bound " +
                $"{AsPercent(baselinePromoteSlow.Upper)} plus delta {AsPercent(_options.AllowedSlowRateDelta)}.");
        }

        if (hasQuantileGuard && !quantileReady)
        {
            signals.Add(
                $"Candidate tail latency has not cleared promotion limits. p95={AsMilliseconds(candidate.P95LatencyMs)} " +
                $"allowed={AsMilliseconds(allowedP95LatencyMs)}; p99={AsMilliseconds(candidate.P99LatencyMs)} " +
                $"allowed={AsMilliseconds(allowedP99LatencyMs)}.");
        }

        if (failureReady && slowReady && quantileReady)
        {
            signals.Add("Failure rate, slow-request rate, and tail latency are all inside promotion limits.");

            return BuildEvaluation(
                CanaryDecisionKind.Promote,
                "Promote rollout: candidate has cleared the configured canary guard rails.",
                signals,
                baseline,
                candidate);
        }

        if (!hasQuantileGuard && failureReady && slowReady)
        {
            return BuildEvaluation(
                CanaryDecisionKind.Hold,
                "Traffic looks healthy so far, but the tail-latency guard is not armed yet.",
                signals,
                baseline,
                candidate);
        }

        return BuildEvaluation(
            CanaryDecisionKind.Hold,
            "Candidate is not bad enough to abort, but it has not cleared promotion guard rails yet.",
            signals,
            baseline,
            candidate);
    }

    private static CanaryEvaluation BuildEvaluation(
        CanaryDecisionKind decision,
        string summary,
        List<string> signals,
        ArmMetrics baseline,
        ArmMetrics candidate)
    {
        return new CanaryEvaluation
        {
            EvaluatedAt = DateTimeOffset.UtcNow,
            Decision = decision,
            Summary = summary,
            Signals = signals.ToArray(),
            Baseline = baseline.ToSnapshot(),
            Candidate = candidate.ToSnapshot(),
        };
    }

    private static string AsPercent(double value)
        => (value * 100d).ToString("0.###", CultureInfo.InvariantCulture) + "%";

    private static string AsMilliseconds(double value)
        => value.ToString("0.##", CultureInfo.InvariantCulture) + "ms";

    private sealed class ObservationWindow
    {
        private readonly ObservationSample[] _buffer;
        private int _start;
        private int _count;

        public ObservationWindow(int capacity)
        {
            _buffer = new ObservationSample[capacity];
        }

        public void Add(ObservationSample sample)
        {
            if (_count < _buffer.Length)
            {
                var writeIndex = (_start + _count) % _buffer.Length;
                _buffer[writeIndex] = sample;
                _count++;
                return;
            }

            _buffer[_start] = sample;
            _start = (_start + 1) % _buffer.Length;
        }

        public ObservationSample[] Snapshot()
        {
            var snapshot = new ObservationSample[_count];
            for (int index = 0; index < _count; index++)
            {
                snapshot[index] = _buffer[(_start + index) % _buffer.Length];
            }

            return snapshot;
        }

        public void Clear()
        {
            _start = 0;
            _count = 0;
        }
    }

    private readonly record struct ObservationSample(
        long UnixTimeMilliseconds,
        double LatencyMs,
        bool Failure,
        int StatusCode,
        int InputTokens,
        int OutputTokens)
    {
        public int TotalTokens => InputTokens + OutputTokens;

        public static ObservationSample FromObservation(InferenceObservation observation, InferenceCanaryGateOptions options)
        {
            if (observation.Latency < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(observation), "Latency must be non-negative.");
            }

            var failure = !observation.Succeeded;
            if (!failure && observation.StatusCode >= 500)
            {
                failure = true;
            }

            if (!failure && options.CountClientErrorsAsFailures && observation.StatusCode is >= 400 and < 500)
            {
                failure = true;
            }

            return new ObservationSample(
                observation.Timestamp.ToUnixTimeMilliseconds(),
                observation.Latency.TotalMilliseconds,
                failure,
                observation.StatusCode,
                Math.Max(observation.InputTokens, 0),
                Math.Max(observation.OutputTokens, 0));
        }
    }

    private sealed record ArmMetrics
    {
        public DeploymentArm Arm { get; init; }
        public int SampleCount { get; init; }
        public int FailureCount { get; init; }
        public int SlowCount { get; init; }
        public int Status2xxCount { get; init; }
        public int Status429Count { get; init; }
        public int Status4xxCount { get; init; }
        public int Status5xxCount { get; init; }
        public double FailureRate { get; init; }
        public double SlowRate { get; init; }
        public WilsonInterval FailureRateInterval { get; init; }
        public WilsonInterval SlowRateInterval { get; init; }
        public double MeanLatencyMs { get; init; }
        public double StandardDeviationLatencyMs { get; init; }
        public double P50LatencyMs { get; init; }
        public double P95LatencyMs { get; init; }
        public double P99LatencyMs { get; init; }
        public double MinLatencyMs { get; init; }
        public double MaxLatencyMs { get; init; }
        public double RequestsPerSecond { get; init; }
        public double MeanTokensPerRequest { get; init; }
        public double TokensPerSecond { get; init; }
        public DateTimeOffset? FirstObservedAt { get; init; }
        public DateTimeOffset? LastObservedAt { get; init; }

        public static ArmMetrics Empty(DeploymentArm arm)
            => new ArmMetrics
            {
                Arm = arm,
                FailureRateInterval = new WilsonInterval(0d, 1d),
                SlowRateInterval = new WilsonInterval(0d, 1d),
            };

        public InferenceArmSnapshot ToSnapshot()
        {
            return new InferenceArmSnapshot
            {
                Arm = Arm,
                SampleCount = SampleCount,
                FailureCount = FailureCount,
                SlowCount = SlowCount,
                Status2xxCount = Status2xxCount,
                Status429Count = Status429Count,
                Status4xxCount = Status4xxCount,
                Status5xxCount = Status5xxCount,
                FailureRate = FailureRate,
                FailureRateInterval = FailureRateInterval,
                SlowRate = SlowRate,
                SlowRateInterval = SlowRateInterval,
                MeanLatency = ToTimeSpan(MeanLatencyMs),
                StandardDeviationLatency = ToTimeSpan(StandardDeviationLatencyMs),
                P50Latency = ToTimeSpan(P50LatencyMs),
                P95Latency = ToTimeSpan(P95LatencyMs),
                P99Latency = ToTimeSpan(P99LatencyMs),
                MinLatency = ToTimeSpan(MinLatencyMs),
                MaxLatency = ToTimeSpan(MaxLatencyMs),
                RequestsPerSecond = RequestsPerSecond,
                MeanTokensPerRequest = MeanTokensPerRequest,
                TokensPerSecond = TokensPerSecond,
                FirstObservedAt = FirstObservedAt,
                LastObservedAt = LastObservedAt,
            };
        }

        private static TimeSpan ToTimeSpan(double milliseconds)
        {
            if (double.IsNaN(milliseconds) || double.IsInfinity(milliseconds))
            {
                return TimeSpan.Zero;
            }

            return TimeSpan.FromMilliseconds(Math.Max(0d, milliseconds));
        }
    }

    private static class Statistics
    {
        public static double PercentileSorted(double[] sortedValues, double probability)
        {
            if (sortedValues.Length == 0)
            {
                return 0d;
            }

            if (sortedValues.Length == 1)
            {
                return sortedValues[0];
            }

            var clamped = Math.Clamp(probability, 0d, 1d);
            var position = (sortedValues.Length - 1) * clamped;
            var lowerIndex = (int)Math.Floor(position);
            var upperIndex = (int)Math.Ceiling(position);
            var weight = position - lowerIndex;

            if (lowerIndex == upperIndex)
            {
                return sortedValues[lowerIndex];
            }

            return sortedValues[lowerIndex] + ((sortedValues[upperIndex] - sortedValues[lowerIndex]) * weight);
        }

        public static double InverseStandardNormalCdf(double probability)
        {
            if (probability <= 0d || probability >= 1d)
            {
                throw new ArgumentOutOfRangeException(nameof(probability), "Probability must be between 0 and 1.");
            }

            const double a1 = -39.6968302866538d;
            const double a2 = 220.946098424521d;
            const double a3 = -275.928510446969d;
            const double a4 = 138.357751867269d;
            const double a5 = -30.6647980661472d;
            const double a6 = 2.50662827745924d;

            const double b1 = -54.4760987982241d;
            const double b2 = 161.585836858041d;
            const double b3 = -155.698979859887d;
            const double b4 = 66.8013118877197d;
            const double b5 = -13.2806815528857d;

            const double c1 = -0.00778489400243029d;
            const double c2 = -0.322396458041136d;
            const double c3 = -2.40075827716184d;
            const double c4 = -2.54973253934373d;
            const double c5 = 4.37466414146497d;
            const double c6 = 2.93816398269878d;

            const double d1 = 0.00778469570904146d;
            const double d2 = 0.32246712907004d;
            const double d3 = 2.445134137143d;
            const double d4 = 3.75440866190742d;

            const double lowRegion = 0.02425d;
            const double highRegion = 1d - lowRegion;

            if (probability < lowRegion)
            {
                var q = Math.Sqrt(-2d * Math.Log(probability));
                return (((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6)
                    / ((((d1 * q + d2) * q + d3) * q + d4) * q + 1d);
            }

            if (probability > highRegion)
            {
                var q = Math.Sqrt(-2d * Math.Log(1d - probability));
                return -(((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6)
                    / ((((d1 * q + d2) * q + d3) * q + d4) * q + 1d);
            }

            var centered = probability - 0.5d;
            var r = centered * centered;
            return (((((a1 * r + a2) * r + a3) * r + a4) * r + a5) * r + a6) * centered
                / (((((b1 * r + b2) * r + b3) * r + b4) * r + b5) * r + 1d);
        }
    }
}

public enum CanaryDecisionKind
{
    Hold,
    Promote,
    Abort,
}

public enum DeploymentArm
{
    Baseline,
    Candidate,
}

public sealed record InferenceCanaryGateOptions
{
    public int MaxWindowSamples { get; init; } = 4096;
    public int MinDecisionSamplesPerArm { get; init; } = 200;
    public int MinQuantileSamplesPerArm { get; init; } = 400;
    public double PromoteConfidence { get; init; } = 0.95d;
    public double AbortConfidence { get; init; } = 0.99d;
    public double AllowedFailureRateDelta { get; init; } = 0.0025d;
    public double AllowedSlowRateDelta { get; init; } = 0.01d;
    public TimeSpan SlowRequestThreshold { get; init; } = TimeSpan.FromSeconds(4);
    public double MaxP95RegressionRatio { get; init; } = 1.10d;
    public double MaxP99RegressionRatio { get; init; } = 1.15d;
    public TimeSpan AbsoluteP95Slack { get; init; } = TimeSpan.FromMilliseconds(150);
    public TimeSpan AbsoluteP99Slack { get; init; } = TimeSpan.FromMilliseconds(250);
    public bool CountClientErrorsAsFailures { get; init; }

    public void Validate()
    {
        if (MaxWindowSamples < 32)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxWindowSamples), "MaxWindowSamples must be at least 32.");
        }

        if (MinDecisionSamplesPerArm < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(MinDecisionSamplesPerArm), "MinDecisionSamplesPerArm must be positive.");
        }

        if (MinQuantileSamplesPerArm < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(MinQuantileSamplesPerArm), "MinQuantileSamplesPerArm must be positive.");
        }

        ValidateProbability(nameof(PromoteConfidence), PromoteConfidence, 0.5d, 1d);
        ValidateProbability(nameof(AbortConfidence), AbortConfidence, 0.5d, 1d);
        ValidateProbability(nameof(AllowedFailureRateDelta), AllowedFailureRateDelta, 0d, 1d, true);
        ValidateProbability(nameof(AllowedSlowRateDelta), AllowedSlowRateDelta, 0d, 1d, true);

        if (AbortConfidence < PromoteConfidence)
        {
            throw new InvalidOperationException("AbortConfidence should be greater than or equal to PromoteConfidence.");
        }

        if (SlowRequestThreshold < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(SlowRequestThreshold), "SlowRequestThreshold must be non-negative.");
        }

        if (AbsoluteP95Slack < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(AbsoluteP95Slack), "AbsoluteP95Slack must be non-negative.");
        }

        if (AbsoluteP99Slack < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(AbsoluteP99Slack), "AbsoluteP99Slack must be non-negative.");
        }

        if (MaxP95RegressionRatio < 1d)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxP95RegressionRatio), "MaxP95RegressionRatio must be at least 1.0.");
        }

        if (MaxP99RegressionRatio < 1d)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxP99RegressionRatio), "MaxP99RegressionRatio must be at least 1.0.");
        }
    }

    private static void ValidateProbability(string name, double value, double minExclusive, double maxExclusive, bool allowZero = false)
    {
        if (allowZero)
        {
            if (value < 0d || value >= maxExclusive)
            {
                throw new ArgumentOutOfRangeException(name, $"{name} must be in the range [0, {maxExclusive}).");
            }

            return;
        }

        if (value <= minExclusive || value >= maxExclusive)
        {
            throw new ArgumentOutOfRangeException(name, $"{name} must be in the range ({minExclusive}, {maxExclusive}).");
        }
    }
}

public readonly record struct InferenceObservation(
    DateTimeOffset Timestamp,
    TimeSpan Latency,
    bool Succeeded,
    int StatusCode = 200,
    int InputTokens = 0,
    int OutputTokens = 0)
{
    public static InferenceObservation Success(
        TimeSpan latency,
        int statusCode = 200,
        int inputTokens = 0,
        int outputTokens = 0,
        DateTimeOffset? timestamp = null)
        => new(timestamp ?? DateTimeOffset.UtcNow, latency, true, statusCode, inputTokens, outputTokens);

    public static InferenceObservation Failure(
        TimeSpan latency,
        int statusCode = 500,
        int inputTokens = 0,
        int outputTokens = 0,
        DateTimeOffset? timestamp = null)
        => new(timestamp ?? DateTimeOffset.UtcNow, latency, false, statusCode, inputTokens, outputTokens);
}

public readonly record struct WilsonInterval(double Lower, double Upper)
{
    public static WilsonInterval FromCounts(int eventCount, int totalCount, double confidence)
    {
        if (eventCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(eventCount));
        }

        if (totalCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(totalCount));
        }

        if (eventCount > totalCount)
        {
            throw new ArgumentOutOfRangeException(nameof(eventCount), "Event count cannot exceed total count.");
        }

        if (totalCount == 0)
        {
            return new WilsonInterval(0d, 1d);
        }

        var z = InferenceCanaryMath.InverseStandardNormalCdf(0.5d + (confidence / 2d));
        var n = totalCount;
        var p = eventCount / (double)n;
        var zSquared = z * z;
        var denominator = 1d + (zSquared / n);
        var center = p + (zSquared / (2d * n));
        var margin = z * Math.Sqrt((p * (1d - p) / n) + (zSquared / (4d * n * n)));

        var lower = Math.Max(0d, (center - margin) / denominator);
        var upper = Math.Min(1d, (center + margin) / denominator);
        return new WilsonInterval(lower, upper);
    }
}

public sealed record InferenceArmSnapshot
{
    public DeploymentArm Arm { get; init; }
    public int SampleCount { get; init; }
    public int FailureCount { get; init; }
    public int SlowCount { get; init; }
    public int Status2xxCount { get; init; }
    public int Status429Count { get; init; }
    public int Status4xxCount { get; init; }
    public int Status5xxCount { get; init; }
    public double FailureRate { get; init; }
    public WilsonInterval FailureRateInterval { get; init; }
    public double SlowRate { get; init; }
    public WilsonInterval SlowRateInterval { get; init; }
    public TimeSpan MeanLatency { get; init; }
    public TimeSpan StandardDeviationLatency { get; init; }
    public TimeSpan P50Latency { get; init; }
    public TimeSpan P95Latency { get; init; }
    public TimeSpan P99Latency { get; init; }
    public TimeSpan MinLatency { get; init; }
    public TimeSpan MaxLatency { get; init; }
    public double RequestsPerSecond { get; init; }
    public double MeanTokensPerRequest { get; init; }
    public double TokensPerSecond { get; init; }
    public DateTimeOffset? FirstObservedAt { get; init; }
    public DateTimeOffset? LastObservedAt { get; init; }
}

public sealed record CanaryEvaluation
{
    public DateTimeOffset EvaluatedAt { get; init; }
    public CanaryDecisionKind Decision { get; init; }
    public string Summary { get; init; } = string.Empty;
    public IReadOnlyList<string> Signals { get; init; } = Array.Empty<string>();
    public InferenceArmSnapshot Baseline { get; init; } = new InferenceArmSnapshot();
    public InferenceArmSnapshot Candidate { get; init; } = new InferenceArmSnapshot();
}

internal static class InferenceCanaryMath
{
    public static double InverseStandardNormalCdf(double probability)
    {
        if (probability <= 0d || probability >= 1d)
        {
            throw new ArgumentOutOfRangeException(nameof(probability), "Probability must be between 0 and 1.");
        }

        const double a1 = -39.6968302866538d;
        const double a2 = 220.946098424521d;
        const double a3 = -275.928510446969d;
        const double a4 = 138.357751867269d;
        const double a5 = -30.6647980661472d;
        const double a6 = 2.50662827745924d;

        const double b1 = -54.4760987982241d;
        const double b2 = 161.585836858041d;
        const double b3 = -155.698979859887d;
        const double b4 = 66.8013118877197d;
        const double b5 = -13.2806815528857d;

        const double c1 = -0.00778489400243029d;
        const double c2 = -0.322396458041136d;
        const double c3 = -2.40075827716184d;
        const double c4 = -2.54973253934373d;
        const double c5 = 4.37466414146497d;
        const double c6 = 2.93816398269878d;

        const double d1 = 0.00778469570904146d;
        const double d2 = 0.32246712907004d;
        const double d3 = 2.445134137143d;
        const double d4 = 3.75440866190742d;

        const double lowRegion = 0.02425d;
        const double highRegion = 1d - lowRegion;

        if (probability < lowRegion)
        {
            var q = Math.Sqrt(-2d * Math.Log(probability));
            return (((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6)
                / ((((d1 * q + d2) * q + d3) * q + d4) * q + 1d);
        }

        if (probability > highRegion)
        {
            var q = Math.Sqrt(-2d * Math.Log(1d - probability));
            return -(((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6)
                / ((((d1 * q + d2) * q + d3) * q + d4) * q + 1d);
        }

        var centered = probability - 0.5d;
        var r = centered * centered;
        return (((((a1 * r + a2) * r + a3) * r + a4) * r + a5) * r + a6) * centered
            / (((((b1 * r + b2) * r + b3) * r + b4) * r + b5) * r + 1d);
    }
}

/*
This solves canary deployment analysis for AI inference services, ASP.NET APIs, model gateways, and .NET background workers where average latency hides the real problem. When you roll out a new OpenAI-compatible proxy, Anthropic integration, Azure OpenAI path, retrieval pipeline, or token accounting change, you usually care about three things at once: did failures go up, did slow requests go up, and did p95 or p99 latency get ugly. This file gives you one place to feed baseline and candidate observations and get a clean promote, hold, or abort answer back.

Built because a lot of rollout code in real teams is still too shallow. People compare averages, eyeball dashboards, or hardcode one-off thresholds in YAML, then miss the exact regressions that hurt users in production. I wanted a C# canary gate that a backend engineer can drop into a service, a deployment controller, or a load-test harness without bringing in a statistics package, a database, or a full observability stack. The bounded window keeps the memory profile predictable, the Wilson score intervals make the rate checks more trustworthy at small sample sizes, and the tail-latency guard stops the classic “mean looks fine but p99 exploded” failure mode.

Use it when you are shipping a new inference model, changing provider failover logic, tuning cache behavior, swapping prompt construction code, rolling out an MCP or agent runtime change, or validating a risky ASP.NET middleware release. It also works for generic HTTP canaries, queue consumers, and worker services as long as you can emit timestamp, latency, status, and optional token counts. The token metrics are there because modern AI systems often need rollout evidence that is aware of cost and throughput, not just request success.

The trick: this file does not pretend there is one magic metric. It keeps a bounded recent sample window for each arm, computes exact rolling percentiles from that window, tracks failure and slow-request rates with Wilson score intervals, and makes decisions in a sequence that matches how operators think in real incidents. First it checks for obvious abort conditions with stricter confidence. Then it checks whether p95 and p99 are outside the allowed regression budget. Only after the hard-stop checks pass does it ask whether the candidate is good enough to promote. That means you get conservative aborts, cleaner holds, and promotions that have actually cleared more than one weak signal.

Drop this into a .NET 8 or .NET 9 service, a deployment job, a release validator, an integration test, or a benchmark harness. Feed it observations from your HTTP client, reverse proxy, queue processor, or synthetic load run. The public API is intentionally small: record baseline and candidate observations, call Evaluate, and wire the returned decision into your rollout controller or dashboard. If someone is searching GitHub or Google for terms like C# canary deployment gate, .NET rollout guard, AI inference canary analysis, p95 p99 latency gate, Wilson interval canary check, OpenAI gateway rollout safety, or ASP.NET progressive delivery metrics, this is exactly the kind of single-file implementation they can fork and use.
*/
