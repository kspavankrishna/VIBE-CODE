# Streaming Kernel Drift Detector

Feature vectors and embeddings shift long before a dashboard shows it. This is a single file Julia detector that watches a live stream, compares it against a slow moving reference and raises a flag when the distribution has actually moved, using fixed memory and no stored history.

**Language:** Julia | **Lines:** 104 | **Added:** 2026-04-10

## What this solves

The failure mode is quiet. You deploy a new encoder, a tokenizer changes, a partner starts sending a slightly different payload, and the vectors flowing through your pipeline are no longer the vectors your index or your model was fitted on. Nothing throws. Latency is fine. Error rate is fine. Retrieval quality slides, ranking gets subtly worse, a classifier that used to sit at 0.91 drifts to 0.84, and the first person who notices is a customer saying search stopped working. By then you are two weeks past the deploy and you have no idea which deploy.

Standard monitoring misses it because standard monitoring watches scalars. You alert on mean latency, mean score, null rate. A distribution can move hard in a high dimensional space while every marginal mean stays roughly where it was: a rotation, a mode split, a new cluster in one corner of the space. Watching the full joint distribution properly means storing a reference window, which means memory that grows with your stream and an answer that arrives hours late.

The other common answer is an offline two sample test, KS per dimension or MMD on a stored window. That works but it is retrospective and expensive. You hold a window in memory, you pick a window size, and you get an answer on a schedule rather than at the moment the stream changes. For a pipeline pushing thousands of vectors a minute, an hourly batch job is a slow smoke alarm.

This file is the online version. One detector object, constant memory whatever the stream length, one cheap update per sample, and a boolean that flips when accumulated evidence crosses your threshold. It sits inline in the ingest path, not in a nightly job.

## Why I built it

There is plenty of drift tooling for tabular features and almost none of it works on dense vectors. The tabular tools give you per column histograms and PSI, the wrong shape of answer for a 768 dimensional embedding. The kernel two sample literature gives you the right answer, MMD, but the reference implementations assume two batches sitting in memory. Nothing in the middle: kernel level sensitivity, streaming interface, bounded memory.

So I wrote the middle. Random Fourier features turn the kernel mean embedding into a fixed length vector you can maintain incrementally, and a Page Hinkley test on the distance between two decaying means turns that into an alarm instead of a noisy time series. A hundred lines of Julia with nothing outside the standard library, so you can read all of it before you trust it.

## When to use it

- An embedding pipeline feeding a vector index, where today's vectors may no longer look like the vectors you built the index from.
- Guarding a deployed model at inference time, so covariate shift shows up before accuracy metrics catch up.
- Telemetry from devices or services where a firmware or config change silently alters the feature distribution.
- A/B rollouts where you want an objective signal that the treatment population's features diverged from baseline.
- Any online loop where storing a reference window is unacceptable, for memory reasons or because the data cannot be retained.

## How it works

The core object is the mutable struct `StreamingKernelDriftDetector`. Construction takes the input dimension plus keyword arguments `features`, `gamma`, `decay_live`, `decay_ref`, `threshold`, `burn_in` and `seed`. A `MersenneTwister(seed)` draws the projection matrix `W` as `sqrt(2gamma) .* randn(rng, features, dim)` and a `phase` vector uniform over `2π`. Seeding matters: detectors built with the same seed share the same projection and their scores are comparable, detectors with different seeds are not.

`embed` is the random Fourier features trick. It computes `sqrt(2 / features) .* cos.(W * x .+ phase)`, mapping an input into a fixed length space where the inner product approximates a Gaussian RBF kernel with bandwidth set by `gamma`. That is the whole reason this works in constant memory. The mean of these embeddings over a set of points is the kernel mean embedding of that distribution, and the distance between two such means estimates Maximum Mean Discrepancy. Instead of holding two batches of raw vectors and computing a kernel matrix, you hold two vectors of length `features` and take a norm.

`update!` maintains those two means. `mean_live` decays at `decay_live` (default 0.92, fast, tracks recent behaviour) and `mean_ref` at `decay_ref` (default 0.995, slow, the reference). Both are exponentially weighted moving averages over the same embedded sample `z`, differing only in memory length. During the first `burn_in` samples the detector runs a plain running average with weight `1 / seen`, pins `mean_ref` to `mean_live` and returns score zero with drift false, so a cold start does not alarm. `distance` is the L2 norm between the two means and that is the drift score.

Raw score is noisy, so the alarm sits on a Page Hinkley test. Each step accumulates `ph_stat += score - 0.02`, where 0.02 is the tolerance for normal wobble. `ph_min` tracks the running minimum of that cumulative sum and drift fires when `ph_stat - ph_min > threshold`. Standard change point construction: a small persistent bias integrates upward and escapes the running minimum, symmetric noise does not. It trades a little detection latency for far fewer false alarms than thresholding the instantaneous score.

When drift fires, `reset_reference!` snaps `mean_ref` onto `mean_live` and zeroes `ph_stat` and `ph_min`. The detector adopts the new distribution as baseline and hunts for the next change instead of alarming continuously about a shift you already know about. `update!` returns a named tuple `(score, drift)` per sample, and `snapshot` returns `seen`, the rounded `score`, `ph_stat`, `threshold` and `features` for logging. The demo in the `abspath(PROGRAM_FILE) == @__FILE__` block runs 220 six dimensional samples with a hard mean shift at step 140 and prints the detected change points, so you can measure detection lag for a given threshold before wiring it to anything real.

## Usage

```bash
# run the built in demo: 220 samples, mean shift injected at t=140
julia "StreamingKernelDriftDetector.jl"
```

```julia
include("StreamingKernelDriftDetector.jl")

# 128 dimensional input stream
det = StreamingKernelDriftDetector(128;
    features   = 256,    # random Fourier features, more is smoother and slower
    gamma      = 0.22,   # RBF bandwidth, must match your feature scale
    decay_live = 0.92,   # fast EWMA
    decay_ref  = 0.995,  # slow EWMA, the reference
    threshold  = 1.15,   # Page Hinkley alarm level
    burn_in    = 40,     # samples before scoring starts
    seed       = 42)

for x in stream                      # x :: Vector{Float64}, length 128
    result = update!(det, x)
    if result.drift
        @warn "distribution shift" score=result.score state=snapshot(det)
    end
end

snapshot(det)   # (seen=..., score=..., ph_stat=..., threshold=..., features=...)
reset_reference!(det)   # manually rebaseline, e.g. after a planned deploy
```

## Notes

- The Page Hinkley tolerance of 0.02 is hardcoded inside `update!`, not a constructor argument. If your score baseline sits well above or below it, edit the constant or the alarm fires constantly or never.
- `threshold` is empirical, not a p value. No statistical guarantee attaches to it and the right value depends on `features`, `gamma` and your data scale. Calibrate by replaying your own stream.
- Detection is one sided and unscaled. It catches the distance growing, says nothing about a stream getting tighter, and `gamma` assumes inputs on a sensible scale because there is no internal standardisation.
- `embed` requires a `Vector{Float64}` of exactly `dim` length and errors on mismatch. No batching, no matrix input, one sample per call.
- The struct is mutated in place with no lock, so one detector per stream and per thread. Nothing is serialised, so a process restart loses the learned reference.
- Cost per sample is one `features` by `dim` matrix vector product. Memory is `features * dim` for `W` plus a few vectors of length `features`, constant in samples seen.
- Standard library only: LinearAlgebra, Random and Statistics. Nothing to install. The script prints a snapshot and the drift indices, then exits normally with no special exit codes.
