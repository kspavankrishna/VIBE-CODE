module EmbeddingDriftAttributor

using Dates
using LinearAlgebra
using Printf
using Random
using Statistics

export DriftConfig, PairSample, CohortReport, DriftReport, analyze, render_markdown, render_json, main

Base.@kwdef mutable struct DriftConfig
    sample_cap::Int = 4096
    kernel_cap::Int = 512
    bootstrap_cap::Int = 768
    projections::Int = 96
    permutations::Int = 199
    bootstraps::Int = 199
    min_pairs::Int = 20
    seed::Int = 42026
    normalize::Bool = true
    alpha::Float64 = 0.05
    risk_alert::Float64 = 18.0
    max_dimension::Int = 8192
end

struct PairSample
    id::String
    cohort::String
    before::Vector{Float64}
    after::Vector{Float64}
    weight::Float64
end

mutable struct CohortReservoir
    cap::Int
    seen::Int
    samples::Vector{PairSample}
end

mutable struct CohortReport
    cohort::String
    pairs_seen::Int
    pairs_used::Int
    dimensions::Int
    baseline_dispersion::Float64
    mean_cosine_loss::Float64
    p95_cosine_loss::Float64
    centroid_shift::Float64
    sliced_wasserstein::Float64
    mmd2::Float64
    risk_score::Float64
    risk_low::Float64
    risk_high::Float64
    p_value::Float64
    q_value::Float64
    contribution::Float64
    decision::String
end

struct DriftReport
    generated_at::String
    total_seen::Int
    total_used::Int
    config::DriftConfig
    cohorts::Vector{CohortReport}
end

function validate_config!(config::DriftConfig)
    config.sample_cap > 0 || throw(ArgumentError("sample_cap must be positive"))
    config.kernel_cap > 1 || throw(ArgumentError("kernel_cap must be greater than 1"))
    config.bootstrap_cap > 1 || throw(ArgumentError("bootstrap_cap must be greater than 1"))
    config.projections > 0 || throw(ArgumentError("projections must be positive"))
    config.permutations >= 0 || throw(ArgumentError("permutations cannot be negative"))
    config.bootstraps >= 0 || throw(ArgumentError("bootstraps cannot be negative"))
    config.min_pairs > 1 || throw(ArgumentError("min_pairs must be greater than 1"))
    config.seed >= 0 || throw(ArgumentError("seed cannot be negative"))
    0.0 < config.alpha < 1.0 || throw(ArgumentError("alpha must be between 0 and 1"))
    config.risk_alert > 0.0 || throw(ArgumentError("risk_alert must be positive"))
    config.max_dimension > 0 || throw(ArgumentError("max_dimension must be positive"))
    return config
end

function stable_hash(text::AbstractString)
    value = UInt64(0xcbf29ce484222325)
    prime = UInt64(0x100000001b3)
    for byte in codeunits(text)
        value = (value ⊻ UInt64(byte)) * prime
    end
    return value
end

function seeded_rng(config::DriftConfig, label::AbstractString)
    mixed = (UInt64(config.seed) ⊻ stable_hash(label)) % UInt64(typemax(Int32) - 1)
    return MersenneTwister(Int(mixed) + 1)
end

function new_reservoir(cap::Int)
    return CohortReservoir(cap, 0, PairSample[])
end

function push_reservoir!(bucket::CohortReservoir, sample::PairSample, rng::AbstractRNG)
    bucket.seen += 1
    if length(bucket.samples) < bucket.cap
        push!(bucket.samples, sample)
        return
    end
    slot = rand(rng, 1:bucket.seen)
    if slot <= bucket.cap
        bucket.samples[slot] = sample
    end
end

function parse_vector(raw::AbstractString, line_no::Int, field_name::AbstractString, max_dimension::Int)
    text = strip(raw)
    if startswith(text, "[") && endswith(text, "]")
        text = strip(text[2:end - 1])
    end
    isempty(text) && throw(ArgumentError("line $(line_no): $(field_name) vector is empty"))
    tokens = split(text, r"[\s,;]+"; keepempty = false)
    length(tokens) <= max_dimension || throw(ArgumentError("line $(line_no): $(field_name) has $(length(tokens)) dimensions, above max_dimension $(max_dimension)"))
    values = Vector{Float64}(undef, length(tokens))
    for (index, token) in enumerate(tokens)
        try
            values[index] = parse(Float64, token)
        catch err
            throw(ArgumentError("line $(line_no): cannot parse $(field_name) dimension $(index) as Float64: $(token)"))
        end
        isfinite(values[index]) || throw(ArgumentError("line $(line_no): $(field_name) dimension $(index) is not finite"))
    end
    return values
end

function maybe_normalize!(values::Vector{Float64}, line_no::Int, field_name::AbstractString, enabled::Bool)
    enabled || return values
    nrm = norm(values)
    nrm > 0.0 || throw(ArgumentError("line $(line_no): $(field_name) vector has zero norm"))
    @inbounds for index in eachindex(values)
        values[index] /= nrm
    end
    return values
end

function parse_sample_line(line::AbstractString, line_no::Int, config::DriftConfig)
    stripped = strip(line)
    (isempty(stripped) || startswith(stripped, "#")) && return nothing

    parts = split(chomp(line), '\t'; keepempty = false)
    if length(parts) < 4
        throw(ArgumentError("line $(line_no): expected tab separated id, cohort, before_vector, after_vector, optional weight"))
    end

    first = lowercase(strip(parts[1]))
    second = lowercase(strip(parts[2]))
    if first in ("id", "doc_id", "document_id", "key") && occursin("cohort", second)
        return nothing
    end

    id = strip(parts[1])
    cohort = strip(parts[2])
    isempty(id) && throw(ArgumentError("line $(line_no): id is empty"))
    isempty(cohort) && throw(ArgumentError("line $(line_no): cohort is empty"))

    before = parse_vector(parts[3], line_no, "before", config.max_dimension)
    after = parse_vector(parts[4], line_no, "after", config.max_dimension)
    length(before) == length(after) || throw(ArgumentError("line $(line_no): before and after dimensions differ"))
    maybe_normalize!(before, line_no, "before", config.normalize)
    maybe_normalize!(after, line_no, "after", config.normalize)

    weight = 1.0
    if length(parts) >= 5 && !isempty(strip(parts[5]))
        weight = parse(Float64, strip(parts[5]))
        isfinite(weight) && weight > 0.0 || throw(ArgumentError("line $(line_no): weight must be a positive finite number"))
    end

    return PairSample(id, cohort, before, after, weight)
end

function collect_samples(io::IO, config::DriftConfig)
    validate_config!(config)
    reservoirs = Dict{String, CohortReservoir}()
    rngs = Dict{String, MersenneTwister}()
    total_seen = 0

    for (line_no, line) in enumerate(eachline(io))
        sample = parse_sample_line(line, line_no, config)
        sample === nothing && continue
        total_seen += 1
        if !haskey(reservoirs, sample.cohort)
            reservoirs[sample.cohort] = new_reservoir(config.sample_cap)
            rngs[sample.cohort] = seeded_rng(config, "reservoir:" * sample.cohort)
        end
        push_reservoir!(reservoirs[sample.cohort], sample, rngs[sample.cohort])
    end

    return reservoirs, total_seen
end

function samples_to_matrices(samples::Vector{PairSample})
    isempty(samples) && throw(ArgumentError("cannot build matrices from empty samples"))
    n = length(samples)
    d = length(samples[1].before)
    before = Matrix{Float64}(undef, n, d)
    after = Matrix{Float64}(undef, n, d)
    weights = Vector{Float64}(undef, n)

    @inbounds for i in 1:n
        length(samples[i].before) == d || throw(ArgumentError("cohort $(samples[i].cohort) has mixed before dimensions"))
        length(samples[i].after) == d || throw(ArgumentError("cohort $(samples[i].cohort) has mixed after dimensions"))
        for j in 1:d
            before[i, j] = samples[i].before[j]
            after[i, j] = samples[i].after[j]
        end
        weights[i] = samples[i].weight
    end

    return before, after, weights
end

function subset_rows(matrix::Matrix{Float64}, indices::Vector{Int})
    result = Matrix{Float64}(undef, length(indices), size(matrix, 2))
    @inbounds for (row, original) in enumerate(indices)
        for col in 1:size(matrix, 2)
            result[row, col] = matrix[original, col]
        end
    end
    return result
end

function subset_values(values::Vector{Float64}, indices::Vector{Int})
    result = Vector{Float64}(undef, length(indices))
    @inbounds for (row, original) in enumerate(indices)
        result[row] = values[original]
    end
    return result
end

function sampled_indices(rng::AbstractRNG, n::Int, cap::Int)
    if n <= cap
        return collect(1:n)
    end
    perm = randperm(rng, n)
    indices = sort!(perm[1:cap])
    return indices
end

function bootstrap_indices(rng::AbstractRNG, n::Int, cap::Int)
    draw = min(n, cap)
    indices = Vector{Int}(undef, draw)
    @inbounds for i in 1:draw
        indices[i] = rand(rng, 1:n)
    end
    return indices
end

function row_dot(left::Matrix{Float64}, i::Int, right::Matrix{Float64}, j::Int)
    acc = 0.0
    @inbounds for col in 1:size(left, 2)
        acc += left[i, col] * right[j, col]
    end
    return acc
end

function row_norm(matrix::Matrix{Float64}, row::Int)
    acc = 0.0
    @inbounds for col in 1:size(matrix, 2)
        acc += matrix[row, col] * matrix[row, col]
    end
    return sqrt(acc)
end

function squared_row_distance(left::Matrix{Float64}, i::Int, right::Matrix{Float64}, j::Int)
    acc = 0.0
    @inbounds for col in 1:size(left, 2)
        delta = left[i, col] - right[j, col]
        acc += delta * delta
    end
    return acc
end

function percentile(values::Vector{Float64}, p::Float64)
    isempty(values) && return NaN
    0.0 <= p <= 1.0 || throw(ArgumentError("percentile p must be between 0 and 1"))
    ordered = sort(values)
    length(ordered) == 1 && return ordered[1]
    pos = 1.0 + (length(ordered) - 1) * p
    lo = floor(Int, pos)
    hi = ceil(Int, pos)
    if lo == hi
        return ordered[lo]
    end
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac
end

function weighted_mean(values::Vector{Float64}, weights::Vector{Float64})
    length(values) == length(weights) || throw(ArgumentError("values and weights have different lengths"))
    numerator = 0.0
    denominator = 0.0
    @inbounds for i in eachindex(values)
        numerator += values[i] * weights[i]
        denominator += weights[i]
    end
    denominator > 0.0 || return NaN
    return numerator / denominator
end

function weighted_centroid(matrix::Matrix{Float64}, weights::Vector{Float64})
    d = size(matrix, 2)
    center = zeros(Float64, d)
    total = 0.0
    @inbounds for row in 1:size(matrix, 1)
        w = weights[row]
        total += w
        for col in 1:d
            center[col] += w * matrix[row, col]
        end
    end
    total > 0.0 || throw(ArgumentError("total weight must be positive"))
    @inbounds for col in 1:d
        center[col] /= total
    end
    return center
end

function cosine_losses(before::Matrix{Float64}, after::Matrix{Float64})
    n = size(before, 1)
    losses = Vector{Float64}(undef, n)
    @inbounds for row in 1:n
        denom = max(row_norm(before, row) * row_norm(after, row), eps(Float64))
        sim = clamp(row_dot(before, row, after, row) / denom, -1.0, 1.0)
        losses[row] = max(0.0, 1.0 - sim)
    end
    return losses
end

function baseline_dispersion(before::Matrix{Float64}, rng::AbstractRNG; max_pairs::Int = 4096)
    n = size(before, 1)
    n < 2 && return 1e-6
    rounds = min(max_pairs, n * (n - 1))
    distances = Vector{Float64}(undef, rounds)
    @inbounds for draw in 1:rounds
        i = rand(rng, 1:n)
        j = rand(rng, 1:n - 1)
        j >= i && (j += 1)
        denom = max(row_norm(before, i) * row_norm(before, j), eps(Float64))
        sim = clamp(row_dot(before, i, before, j) / denom, -1.0, 1.0)
        distances[draw] = max(0.0, 1.0 - sim)
    end
    return max(percentile(distances, 0.50), 1e-6)
end

function random_projection_matrix(rng::AbstractRNG, d::Int, projections::Int)
    matrix = Matrix{Float64}(undef, d, projections)
    @inbounds for col in 1:projections
        norm2 = 0.0
        for row in 1:d
            value = randn(rng)
            matrix[row, col] = value
            norm2 += value * value
        end
        scale = 1.0 / max(sqrt(norm2), eps(Float64))
        for row in 1:d
            matrix[row, col] *= scale
        end
    end
    return matrix
end

function projected_values(matrix::Matrix{Float64}, projection::Matrix{Float64}, col::Int)
    values = Vector{Float64}(undef, size(matrix, 1))
    @inbounds for row in 1:size(matrix, 1)
        acc = 0.0
        for dim in 1:size(matrix, 2)
            acc += matrix[row, dim] * projection[dim, col]
        end
        values[row] = acc
    end
    return values
end

function sliced_wasserstein(before::Matrix{Float64}, after::Matrix{Float64}, projections::Matrix{Float64})
    total = 0.0
    count = size(projections, 2)
    for col in 1:count
        left = sort!(projected_values(before, projections, col))
        right = sort!(projected_values(after, projections, col))
        m = min(length(left), length(right))
        gap = 0.0
        @inbounds for i in 1:m
            gap += abs(left[i] - right[i])
        end
        total += gap / m
    end
    return total / count
end

function estimate_bandwidth2(before::Matrix{Float64}, after::Matrix{Float64}, rng::AbstractRNG; max_pairs::Int = 2048)
    combined = vcat(before, after)
    n = size(combined, 1)
    n < 2 && return 1.0
    distances = Vector{Float64}(undef, min(max_pairs, n * (n - 1)))
    @inbounds for draw in eachindex(distances)
        i = rand(rng, 1:n)
        j = rand(rng, 1:n - 1)
        j >= i && (j += 1)
        distances[draw] = squared_row_distance(combined, i, combined, j)
    end
    return max(percentile(distances, 0.50), 1e-9)
end

function gaussian_mmd2(before::Matrix{Float64}, after::Matrix{Float64}, bandwidth2::Float64)
    n = size(before, 1)
    m = size(after, 1)
    n == 0 && return 0.0
    m == 0 && return 0.0
    scale = 2.0 * max(bandwidth2, 1e-9)

    xx = 0.0
    @inbounds for i in 1:n, j in 1:n
        xx += exp(-squared_row_distance(before, i, before, j) / scale)
    end

    yy = 0.0
    @inbounds for i in 1:m, j in 1:m
        yy += exp(-squared_row_distance(after, i, after, j) / scale)
    end

    xy = 0.0
    @inbounds for i in 1:n, j in 1:m
        xy += exp(-squared_row_distance(before, i, after, j) / scale)
    end

    value = (xx / (n * n)) + (yy / (m * m)) - (2.0 * xy / (n * m))
    return max(0.0, value)
end

function risk_score(mean_loss::Float64, p95_loss::Float64, centroid_shift::Float64, sw::Float64, mmd2::Float64, dispersion::Float64)
    cosine_unit = max(dispersion, 1e-6)
    vector_unit = max(sqrt(2.0 * dispersion), 1e-6)
    mmd_unit = max(sqrt(dispersion), 1e-6)

    p95_part = min(p95_loss / cosine_unit, 3.0) / 3.0
    mean_part = min(mean_loss / cosine_unit, 3.0) / 3.0
    center_part = min(centroid_shift / vector_unit, 3.0) / 3.0
    sw_part = min(sw / vector_unit, 3.0) / 3.0
    mmd_part = min(sqrt(max(mmd2, 0.0)) / mmd_unit, 3.0) / 3.0

    return 100.0 * (0.28 * p95_part + 0.22 * mean_part + 0.22 * center_part + 0.16 * sw_part + 0.12 * mmd_part)
end

function compute_metrics(before::Matrix{Float64}, after::Matrix{Float64}, weights::Vector{Float64}, config::DriftConfig, rng::AbstractRNG)
    d = size(before, 2)
    projection_count = min(config.projections, max(8, d * 4))
    projections = random_projection_matrix(rng, d, projection_count)
    losses = cosine_losses(before, after)
    mean_loss = weighted_mean(losses, weights)
    p95_loss = percentile(losses, 0.95)
    base_dispersion = baseline_dispersion(before, rng)

    before_center = weighted_centroid(before, weights)
    after_center = weighted_centroid(after, weights)
    centroid = norm(after_center - before_center)
    sw = sliced_wasserstein(before, after, projections)

    idx = sampled_indices(rng, size(before, 1), config.kernel_cap)
    before_kernel = subset_rows(before, idx)
    after_kernel = subset_rows(after, idx)
    bandwidth2 = estimate_bandwidth2(before_kernel, after_kernel, rng)
    mmd2 = gaussian_mmd2(before_kernel, after_kernel, bandwidth2)
    score = risk_score(mean_loss, p95_loss, centroid, sw, mmd2, base_dispersion)

    return (; base_dispersion, mean_loss, p95_loss, centroid, sw, mmd2, score)
end

function bootstrap_interval(before::Matrix{Float64}, after::Matrix{Float64}, weights::Vector{Float64}, config::DriftConfig, rng::AbstractRNG)
    config.bootstraps == 0 && return (NaN, NaN)
    n = size(before, 1)
    scores = Vector{Float64}(undef, config.bootstraps)
    @inbounds for round in 1:config.bootstraps
        indices = bootstrap_indices(rng, n, config.bootstrap_cap)
        xb = subset_rows(before, indices)
        yb = subset_rows(after, indices)
        wb = subset_values(weights, indices)
        metrics = compute_metrics(xb, yb, wb, config, rng)
        scores[round] = metrics.score
    end
    return percentile(scores, 0.025), percentile(scores, 0.975)
end

function centroid_statistic(before::Matrix{Float64}, after::Matrix{Float64}, weights::Vector{Float64}, dispersion::Float64)
    before_center = weighted_centroid(before, weights)
    after_center = weighted_centroid(after, weights)
    return norm(after_center - before_center) / max(sqrt(2.0 * dispersion), 1e-6)
end

function permutation_pvalue(before::Matrix{Float64}, after::Matrix{Float64}, weights::Vector{Float64}, config::DriftConfig, rng::AbstractRNG, dispersion::Float64)
    config.permutations == 0 && return 1.0
    n = size(before, 1)
    indices = sampled_indices(rng, n, config.bootstrap_cap)
    xb = subset_rows(before, indices)
    yb = subset_rows(after, indices)
    wb = subset_values(weights, indices)
    observed = centroid_statistic(xb, yb, wb, dispersion)

    ge = 1
    swapped_before = similar(xb)
    swapped_after = similar(yb)
    @inbounds for round in 1:config.permutations
        for row in 1:size(xb, 1)
            swap = rand(rng, Bool)
            for col in 1:size(xb, 2)
                if swap
                    swapped_before[row, col] = yb[row, col]
                    swapped_after[row, col] = xb[row, col]
                else
                    swapped_before[row, col] = xb[row, col]
                    swapped_after[row, col] = yb[row, col]
                end
            end
        end
        stat = centroid_statistic(swapped_before, swapped_after, wb, dispersion)
        stat + 1e-12 >= observed && (ge += 1)
    end
    return ge / (config.permutations + 1)
end

function analyze_cohort(name::String, bucket::CohortReservoir, config::DriftConfig)
    before, after, weights = samples_to_matrices(bucket.samples)
    rng = seeded_rng(config, "cohort:" * name)
    metrics = compute_metrics(before, after, weights, config, rng)
    low, high = bootstrap_interval(before, after, weights, config, rng)
    p = permutation_pvalue(before, after, weights, config, rng, metrics.base_dispersion)

    return CohortReport(
        name,
        bucket.seen,
        length(bucket.samples),
        size(before, 2),
        metrics.base_dispersion,
        metrics.mean_loss,
        metrics.p95_loss,
        metrics.centroid,
        metrics.sw,
        metrics.mmd2,
        metrics.score,
        low,
        high,
        p,
        1.0,
        0.0,
        "pending",
    )
end

function apply_bh!(reports::Vector{CohortReport})
    isempty(reports) && return reports
    order = sortperm([report.p_value for report in reports])
    m = length(reports)
    running = 1.0
    for rank in m:-1:1
        index = order[rank]
        q = min(running, reports[index].p_value * m / rank)
        reports[index].q_value = clamp(q, 0.0, 1.0)
        running = reports[index].q_value
    end
    return reports
end

function decide(report::CohortReport, config::DriftConfig)
    if report.pairs_used < config.min_pairs
        return "not_enough_pairs"
    elseif report.q_value <= config.alpha && report.risk_score >= config.risk_alert
        return "block_index_rollout"
    elseif report.risk_score >= config.risk_alert
        return "inspect_before_rollout"
    elseif report.q_value <= config.alpha
        return "watch_consistent_shift"
    else
        return "pass"
    end
end

function finalize_reports!(reports::Vector{CohortReport}, config::DriftConfig)
    apply_bh!(reports)
    total_weighted_risk = sum(max(report.risk_score, 0.0) * report.pairs_seen for report in reports)
    for report in reports
        report.contribution = total_weighted_risk > 0.0 ? 100.0 * max(report.risk_score, 0.0) * report.pairs_seen / total_weighted_risk : 0.0
        report.decision = decide(report, config)
    end
    sort!(reports, by = report -> (-report.risk_score, report.cohort))
    return reports
end

function analyze(io::IO, config::DriftConfig = DriftConfig())
    validate_config!(config)
    reservoirs, total_seen = collect_samples(io, config)
    reports = CohortReport[]

    for name in sort(collect(keys(reservoirs)))
        bucket = reservoirs[name]
        if length(bucket.samples) >= config.min_pairs
            push!(reports, analyze_cohort(name, bucket, config))
        else
            push!(reports, CohortReport(name, bucket.seen, length(bucket.samples), 0, NaN, NaN, NaN, NaN, NaN, NaN, 0.0, NaN, NaN, 1.0, 1.0, 0.0, "not_enough_pairs"))
        end
    end

    finalize_reports!(reports, config)
    total_used = sum(report.pairs_used for report in reports)
    stamp = Dates.format(Dates.now(Dates.UTC), dateformat"yyyy-mm-ddTHH:MM:SS") * "Z"
    return DriftReport(stamp, total_seen, total_used, config, reports)
end

function fmt(value::Float64; digits::Int = 4)
    if isnan(value)
        return "nan"
    elseif isinf(value)
        return value > 0 ? "inf" : "-inf"
    end
    return @sprintf("%.*f", digits, value)
end

function render_markdown(report::DriftReport)
    io = IOBuffer()
    println(io, "# Embedding Drift Attribution Report")
    println(io)
    println(io, "Generated: `$(report.generated_at)`")
    println(io, "Rows seen: `$(report.total_seen)`, rows used after reservoir sampling: `$(report.total_used)`")
    println(io, "Input format: `id<TAB>cohort<TAB>before_vector<TAB>after_vector<TAB>optional_weight`, vectors comma or space separated.")
    println(io)
    println(io, "| cohort | decision | seen | used | dim | risk | 95% risk CI | q | p | contribution | p95 cosine loss | centroid | sliced W | MMD2 |")
    println(io, "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for item in report.cohorts
        ci = isnan(item.risk_low) ? "nan" : "$(fmt(item.risk_low; digits = 2))-$(fmt(item.risk_high; digits = 2))"
        println(io, "| $(item.cohort) | $(item.decision) | $(item.pairs_seen) | $(item.pairs_used) | $(item.dimensions) | $(fmt(item.risk_score; digits = 2)) | $(ci) | $(fmt(item.q_value; digits = 4)) | $(fmt(item.p_value; digits = 4)) | $(fmt(item.contribution; digits = 1))% | $(fmt(item.p95_cosine_loss; digits = 5)) | $(fmt(item.centroid_shift; digits = 5)) | $(fmt(item.sliced_wasserstein; digits = 5)) | $(fmt(item.mmd2; digits = 6)) |")
    end
    println(io)
    println(io, "Decision rule: block a rollout when the cohort has both Benjamini-Hochberg adjusted `q <= alpha` and `risk >= risk_alert`. Inspect means the shift is practically large but the permutation test did not clear the configured false discovery threshold.")
    return String(take!(io))
end

function json_escape(text::AbstractString)
    io = IOBuffer()
    for char in text
        if char == '\\'
            print(io, "\\\\")
        elseif char == '"'
            print(io, "\\\"")
        elseif char == '\n'
            print(io, "\\n")
        elseif char == '\r'
            print(io, "\\r")
        elseif char == '\t'
            print(io, "\\t")
        elseif Int(char) < 0x20
            print(io, "\\u", lpad(string(Int(char), base = 16), 4, '0'))
        else
            print(io, char)
        end
    end
    return String(take!(io))
end

function json_number(value::Float64)
    isfinite(value) || return "null"
    return @sprintf("%.10g", value)
end

function render_json(report::DriftReport)
    io = IOBuffer()
    println(io, "{")
    println(io, "  \"generated_at\": \"$(json_escape(report.generated_at))\",")
    println(io, "  \"total_seen\": $(report.total_seen),")
    println(io, "  \"total_used\": $(report.total_used),")
    println(io, "  \"config\": {")
    println(io, "    \"sample_cap\": $(report.config.sample_cap), \"kernel_cap\": $(report.config.kernel_cap), \"bootstrap_cap\": $(report.config.bootstrap_cap),")
    println(io, "    \"projections\": $(report.config.projections), \"permutations\": $(report.config.permutations), \"bootstraps\": $(report.config.bootstraps),")
    println(io, "    \"min_pairs\": $(report.config.min_pairs), \"seed\": $(report.config.seed), \"normalize\": $(report.config.normalize),")
    println(io, "    \"alpha\": $(json_number(report.config.alpha)), \"risk_alert\": $(json_number(report.config.risk_alert))")
    println(io, "  },")
    println(io, "  \"cohorts\": [")
    for (index, item) in enumerate(report.cohorts)
        comma = index == length(report.cohorts) ? "" : ","
        println(io, "    {")
        println(io, "      \"cohort\": \"$(json_escape(item.cohort))\",")
        println(io, "      \"decision\": \"$(json_escape(item.decision))\",")
        println(io, "      \"pairs_seen\": $(item.pairs_seen), \"pairs_used\": $(item.pairs_used), \"dimensions\": $(item.dimensions),")
        println(io, "      \"baseline_dispersion\": $(json_number(item.baseline_dispersion)),")
        println(io, "      \"mean_cosine_loss\": $(json_number(item.mean_cosine_loss)), \"p95_cosine_loss\": $(json_number(item.p95_cosine_loss)),")
        println(io, "      \"centroid_shift\": $(json_number(item.centroid_shift)), \"sliced_wasserstein\": $(json_number(item.sliced_wasserstein)), \"mmd2\": $(json_number(item.mmd2)),")
        println(io, "      \"risk_score\": $(json_number(item.risk_score)), \"risk_low\": $(json_number(item.risk_low)), \"risk_high\": $(json_number(item.risk_high)),")
        println(io, "      \"p_value\": $(json_number(item.p_value)), \"q_value\": $(json_number(item.q_value)), \"contribution\": $(json_number(item.contribution))")
        println(io, "    }$(comma)")
    end
    println(io, "  ]")
    println(io, "}")
    return String(take!(io))
end

function help_text()
    return """
EmbeddingDriftAttributor.jl

Usage:
  julia EmbeddingDriftAttributor.jl --input pairs.tsv --markdown
  julia EmbeddingDriftAttributor.jl --input pairs.tsv --json --sample-cap 8000 --risk-alert 20

Input TSV columns:
  id<TAB>cohort<TAB>before_vector<TAB>after_vector<TAB>optional_weight

Vector format:
  0.12,0.09,-0.44 or [0.12 0.09 -0.44]

Options:
  --input, -i PATH       Read TSV from PATH instead of stdin
  --json                Emit JSON
  --markdown            Emit Markdown, the default
  --sample-cap N        Reservoir sample per cohort, default 4096
  --kernel-cap N        Rows used for MMD, default 512
  --bootstrap-cap N     Rows per bootstrap/permutation draw, default 768
  --projections N       Random projections for sliced Wasserstein, default 96
  --permutations N      Paired swap tests, default 199
  --bootstraps N        Bootstrap confidence intervals, default 199
  --min-pairs N         Minimum rows per cohort, default 20
  --seed N              Deterministic seed, default 42026
  --risk-alert X        Practical risk threshold, default 18.0
  --alpha X             FDR threshold, default 0.05
  --raw-vectors         Do not L2 normalize vectors before analysis
  --help, -h            Show this help
"""
end

function take_arg(args::Vector{String}, i::Int, name::String)
    i < length(args) || throw(ArgumentError("$(name) requires a value"))
    return args[i + 1]
end

function parse_cli(args::Vector{String})
    config = DriftConfig()
    input_path = nothing
    format = "markdown"
    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("--help", "-h")
            print(help_text())
            return nothing
        elseif arg in ("--input", "-i")
            input_path = take_arg(args, i, arg)
            i += 2
        elseif arg == "--json"
            format = "json"
            i += 1
        elseif arg == "--markdown"
            format = "markdown"
            i += 1
        elseif arg == "--sample-cap"
            config.sample_cap = parse(Int, take_arg(args, i, arg))
            i += 2
        elseif arg == "--kernel-cap"
            config.kernel_cap = parse(Int, take_arg(args, i, arg))
            i += 2
        elseif arg == "--bootstrap-cap"
            config.bootstrap_cap = parse(Int, take_arg(args, i, arg))
            i += 2
        elseif arg == "--projections"
            config.projections = parse(Int, take_arg(args, i, arg))
            i += 2
        elseif arg == "--permutations"
            config.permutations = parse(Int, take_arg(args, i, arg))
            i += 2
        elseif arg == "--bootstraps"
            config.bootstraps = parse(Int, take_arg(args, i, arg))
            i += 2
        elseif arg == "--min-pairs"
            config.min_pairs = parse(Int, take_arg(args, i, arg))
            i += 2
        elseif arg == "--seed"
            config.seed = parse(Int, take_arg(args, i, arg))
            i += 2
        elseif arg == "--risk-alert"
            config.risk_alert = parse(Float64, take_arg(args, i, arg))
            i += 2
        elseif arg == "--alpha"
            config.alpha = parse(Float64, take_arg(args, i, arg))
            i += 2
        elseif arg == "--raw-vectors"
            config.normalize = false
            i += 1
        else
            throw(ArgumentError("unknown argument: $(arg)"))
        end
    end
    validate_config!(config)
    return config, input_path, format
end

function main(args::Vector{String} = ARGS)
    parsed = parse_cli(args)
    parsed === nothing && return 0
    config, input_path, format = parsed

    if input_path === nothing
        report = analyze(stdin, config)
    else
        report = open(input_path, "r") do io
            analyze(io, config)
        end
    end

    if format == "json"
        print(render_json(report))
    else
        print(render_markdown(report))
    end
    return 0
end

if abspath(PROGRAM_FILE) == @__FILE__
    try
        exit(main())
    catch err
        println(stderr, "EmbeddingDriftAttributor.jl: ", err)
        exit(2)
    end
end

#=
This solves the April 2026 problem where a team swaps an embedding model, changes a vector database reindexing job, or moves RAG search traffic to another provider and then has no clear way to see which customer, language, tenant, or document cohort actually moved. Built because I keep seeing developers compare only top line recall or one average cosine number, then ship a migration that quietly hurts the long tail. Use it when you have paired old and new vectors and need a practical embedding drift detector, RAG evaluation gate, vector search migration report, semantic search quality check, or AI retrieval regression guard that runs from a plain TSV file without sending data to another service. The trick: it keeps memory bounded with per cohort reservoir sampling, normalizes vectors, combines paired cosine loss, centroid movement, sliced Wasserstein distance, Gaussian MMD, bootstrap risk intervals, and Benjamini-Hochberg corrected permutation tests so the output separates real drift from random noise. Drop this into a CI job, notebook batch, data pipeline, model registry check, or vector index rollout script before rebuilding production search. It is intentionally boring to operate: one Julia file, deterministic seed, Markdown or JSON output, no stored secrets, no network call, and enough detail that a senior engineer can fork it for embedding model drift analysis, AI search monitoring, RAG index validation, and production vector database release checks.
=#
