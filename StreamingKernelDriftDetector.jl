using LinearAlgebra
using Random
using Statistics

mutable struct StreamingKernelDriftDetector
    dim::Int
    features::Int
    gamma::Float64
    decay_live::Float64
    decay_ref::Float64
    threshold::Float64
    burn_in::Int
    W::Matrix{Float64}
    phase::Vector{Float64}
    mean_live::Vector{Float64}
    mean_ref::Vector{Float64}
    ph_stat::Float64
    ph_min::Float64
    seen::Int
end

function StreamingKernelDriftDetector(dim::Int; features=128, gamma=0.35,
    decay_live=0.92, decay_ref=0.995, threshold=0.9, burn_in=40, seed=42)
    rng = MersenneTwister(seed)
    W = sqrt(2gamma) .* randn(rng, features, dim)
    phase = 2π .* rand(rng, features)
    zerosv = zeros(features)
    StreamingKernelDriftDetector(dim, features, gamma, decay_live, decay_ref, threshold,
        burn_in, W, phase, copy(zerosv), copy(zerosv), 0.0, 0.0, 0)
end

function embed(det::StreamingKernelDriftDetector, x::Vector{Float64})
    length(x) == det.dim || error("Dimension mismatch")
    return sqrt(2 / det.features) .* cos.(det.W * x .+ det.phase)
end

function distance(det::StreamingKernelDriftDetector)
    return norm(det.mean_live - det.mean_ref)
end

function reset_reference!(det::StreamingKernelDriftDetector)
    det.mean_ref .= det.mean_live
    det.ph_stat = 0.0
    det.ph_min = 0.0
    return det
end

function update!(det::StreamingKernelDriftDetector, x::Vector{Float64})
    z = embed(det, x)
    det.seen += 1

    if det.seen <= det.burn_in
        α = 1 / det.seen
        det.mean_live .= (1 - α) .* det.mean_live .+ α .* z
        det.mean_ref .= det.mean_live
        return (score=0.0, drift=false)
    end

    det.mean_live .= det.decay_live .* det.mean_live .+ (1 - det.decay_live) .* z
    det.mean_ref .= det.decay_ref .* det.mean_ref .+ (1 - det.decay_ref) .* z
    score = distance(det)

    det.ph_stat += score - 0.02
    det.ph_min = min(det.ph_min, det.ph_stat)
    drift = (det.ph_stat - det.ph_min) > det.threshold

    if drift
        reset_reference!(det)
    end
    return (score=score, drift=drift)
end

function snapshot(det::StreamingKernelDriftDetector)
    (
        seen=det.seen,
        score=round(distance(det), digits=5),
        ph_stat=round(det.ph_stat, digits=5),
        threshold=det.threshold,
        features=det.features
    )
end

if abspath(PROGRAM_FILE) == @__FILE__
    rng = MersenneTwister(7)
    det = StreamingKernelDriftDetector(6; features=192, gamma=0.22, threshold=1.15)
    hits = Int[]

    for t in 1:220
        base = t < 140 ? [0.3, -0.1, 0.5, 0.0, 0.2, -0.4] : [1.1, 0.4, -0.2, 0.7, -0.3, 0.6]
        x = base .+ 0.22 .* randn(rng, 6)
        result = update!(det, x)
        result.drift && push!(hits, t)
    end

    println("Detector snapshot: ", snapshot(det))
    println("Detected drift points: ", hits)
end

#=
================================================================================
EXPLANATION
This solves a messy streaming problem: embeddings and feature vectors drift long before dashboards make it obvious. Built because retrieval quality, ranking behavior, and model outputs can slide quietly after a deployment or data shift. Use it on vector streams, telemetry features, or any online ML pipe where you need early warning without storing the whole history. The trick is random Fourier features plus a Page Hinkley style detector, so you track kernel level distribution change with fixed memory and cheap updates.
================================================================================
=#
