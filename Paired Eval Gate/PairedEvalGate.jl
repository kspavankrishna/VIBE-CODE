#
# PairedEvalGate.jl
#
# Dependency-free Julia utility for paired, clustered, stratified bootstrap on
# evaluation datasets. Intended for LLM evaluation, agent benchmark rollouts,
# judge-model comparisons, prompt experiments, and release gating decisions.
#

using Random
using Statistics
using Printf

struct PegPair
    id::String
    cluster::String
    stratum::String
    weight::Float64
    baseline::Float64
    candidate::Float64
    delta::Float64
    win::Bool
    loss::Bool
    tie::Bool
end

struct PegInterval
    low::Float64
    high::Float64
end

struct PairedEvalGateResult
    baseline::String
    candidate::String
    draws::Int
    confidence::Float64
    dropped_incomplete_pairs::Int
    pair_count::Int
    cluster_count::Int
    stratum_count::Int
    baseline_mean::Float64
    candidate_mean::Float64
    delta_mean::Float64
    relative_lift::Float64
    win_rate::Float64
    loss_rate::Float64
    tie_rate::Float64
    standardized_delta::Float64
    baseline_interval::PegInterval
    candidate_interval::PegInterval
    delta_interval::PegInterval
    win_interval::PegInterval
    p_candidate_not_better::Float64
    p_candidate_worse::Float64
    p_two_sided::Float64
    pairs::Vector{PegPair}
    bootstrap_draws::Union{Nothing, Dict{Symbol, Vector{Float64}}}
end

Base.show(io::IO, interval::PegInterval) = print(io, "[", interval.low, ", ", interval.high, "]")

function Base.show(io::IO, result::PairedEvalGateResult)
    @printf(io, "%s vs %s paired bootstrap\n", result.candidate, result.baseline)
    @printf(io, "pairs=%d  clusters=%d  strata=%d  draws=%d\n",
            result.pair_count, result.cluster_count, result.stratum_count, result.draws)
    @printf(io, "candidate_mean=%.6f  baseline_mean=%.6f\n",
            result.candidate_mean, result.baseline_mean)
    @printf(io, "delta_mean=%.6f  CI[%.2f]=[%.6f, %.6f]\n",
            result.delta_mean, result.confidence,
            result.delta_interval.low, result.delta_interval.high)
    @printf(io, "win_rate=%.6f  CI[%.2f]=[%.6f, %.6f]\n",
            result.win_rate, result.confidence,
            result.win_interval.low, result.win_interval.high)
    @printf(io, "p(candidate<=baseline)=%.6f  p(two-sided)=%.6f",
            result.p_candidate_not_better, result.p_two_sided)
end

struct PegAggregateBucket
    scores::Vector{Float64}
    weights::Vector{Float64}
end

PegAggregateBucket() = PegAggregateBucket(Float64[], Float64[])

peg_error(msg) = throw(ArgumentError(msg))

peg_to_symbol(x::Symbol) = x
peg_to_symbol(x::AbstractString) = Symbol(x)
peg_to_symbol(x) = peg_error("column selectors must be symbols or strings")

peg_to_string(x) = ismissing(x) ? "<missing>" : string(x)

function peg_getfield(row::NamedTuple, col::Symbol)
    hasproperty(row, col) || peg_error("missing column $(col)")
    return getproperty(row, col)
end

function peg_rows(data)
    if data isa AbstractVector
        all(row -> row isa NamedTuple, data) || peg_error("vector inputs must contain named tuples")
        return collect(data)
    elseif data isa NamedTuple
        names = propertynames(data)
        length(names) > 0 || peg_error("columnar input must contain at least one column")
        cols = map(name -> getproperty(data, name), names)
        lengths = unique(length.(cols))
        length(lengths) == 1 || peg_error("all column vectors must have the same length")
        n = first(lengths)
        n > 0 || peg_error("columnar input must not be empty")
        row_type = NamedTuple{Tuple(names)}
        return [row_type(Tuple(col[i] for col in cols)) for i in 1:n]
    else
        peg_error("data must be a vector of named tuples or a named tuple of column vectors")
    end
end

function peg_resolve_aggregator(aggregate)
    if aggregate isa Function
        return aggregate
    elseif aggregate == :mean
        return x -> mean(x)
    elseif aggregate == :median
        return x -> median(x)
    elseif aggregate == :sum
        return x -> sum(x)
    end
    peg_error("aggregate must be :mean, :median, :sum, or a custom function")
end

function peg_weighted_mean(values::AbstractVector{<:Real}, weights::AbstractVector{<:Real})
    length(values) == length(weights) || peg_error("values and weights must have the same length")
    numerator = 0.0
    denominator = 0.0
    for (v, w) in zip(values, weights)
        if isfinite(v) && isfinite(w) && w > 0
            numerator += Float64(v) * Float64(w)
            denominator += Float64(w)
        end
    end
    denominator == 0.0 && return NaN
    numerator / denominator
end

peg_weighted_rate(flags::AbstractVector{Bool}, weights::AbstractVector{<:Real}) =
    peg_weighted_mean(Float64.(flags), Float64.(weights))

function peg_weighted_sd(values::AbstractVector{<:Real}, weights::AbstractVector{<:Real})
    mu = peg_weighted_mean(values, weights)
    isfinite(mu) || return NaN
    numerator = 0.0
    denominator = 0.0
    count = 0
    for (v, w) in zip(values, weights)
        if isfinite(v) && isfinite(w) && w > 0
            numerator += Float64(w) * (Float64(v) - mu)^2
            denominator += Float64(w)
            count += 1
        end
    end
    count < 2 && return NaN
    sqrt(numerator / denominator)
end

function peg_quantile_interval(values::Vector{Float64}, confidence::Float64)
    alpha = 1.0 - confidence
    qs = quantile(values, [alpha / 2.0, 1.0 - alpha / 2.0])
    PegInterval(Float64(qs[1]), Float64(qs[2]))
end

function peg_prepare_pairs(
    data;
    baseline,
    candidate,
    id_col = :item_id,
    system_col = :system,
    score_col = :score,
    cluster_col = nothing,
    stratum_col = nothing,
    weight_col = nothing,
    aggregate = :mean,
    drop_incomplete = true,
    tie_tolerance = 0.0
)
    baseline = peg_to_string(baseline)
    candidate = peg_to_string(candidate)
    baseline == candidate && peg_error("baseline and candidate must be different")
    tie_tolerance >= 0 || peg_error("tie_tolerance must be non-negative")

    id_col = peg_to_symbol(id_col)
    system_col = peg_to_symbol(system_col)
    score_col = peg_to_symbol(score_col)
    cluster_col = isnothing(cluster_col) ? nothing : peg_to_symbol(cluster_col)
    stratum_col = isnothing(stratum_col) ? nothing : peg_to_symbol(stratum_col)
    weight_col = isnothing(weight_col) ? nothing : peg_to_symbol(weight_col)

    rows = peg_rows(data)
    filtered = NamedTuple[]
    for row in rows
        system_value = peg_to_string(peg_getfield(row, system_col))
        if system_value == baseline || system_value == candidate
            push!(filtered, row)
        end
    end
    isempty(filtered) && peg_error("no rows matched baseline and candidate")

    aggregate_fn = peg_resolve_aggregator(aggregate)
    grouped = Dict{NTuple{4, String}, PegAggregateBucket}()

    for row in filtered
        id_value = peg_to_string(peg_getfield(row, id_col))
        cluster_value = isnothing(cluster_col) ? id_value : peg_to_string(peg_getfield(row, cluster_col))
        stratum_value = isnothing(stratum_col) ? "all" : peg_to_string(peg_getfield(row, stratum_col))
        system_value = peg_to_string(peg_getfield(row, system_col))
        score_value = Float64(peg_getfield(row, score_col))
        weight_value = isnothing(weight_col) ? 1.0 : Float64(peg_getfield(row, weight_col))
        isfinite(score_value) || peg_error("score values must be finite")
        (isfinite(weight_value) && weight_value > 0) || peg_error("weights must be finite and strictly positive")

        key = (id_value, cluster_value, stratum_value, system_value)
        bucket = get!(grouped, key, PegAggregateBucket())
        push!(bucket.scores, score_value)
        push!(bucket.weights, weight_value)
    end

    aggregated = Dict{NTuple{4, String}, NamedTuple{(:score, :weight), Tuple{Float64, Float64}}}()
    for (key, bucket) in grouped
        score = aggregate_fn(bucket.scores)
        weight = aggregate_fn(bucket.weights)
        (score isa Real && isfinite(Float64(score))) || peg_error("aggregate must produce finite numeric scores")
        (weight isa Real && isfinite(Float64(weight)) && Float64(weight) > 0) ||
            peg_error("aggregate must produce finite positive weights")
        aggregated[key] = (score = Float64(score), weight = Float64(weight))
    end

    by_pair = Dict{NTuple{3, String}, Dict{String, NamedTuple{(:score, :weight), Tuple{Float64, Float64}}}}()
    for ((id_value, cluster_value, stratum_value, system_value), payload) in aggregated
        group = get!(by_pair, (id_value, cluster_value, stratum_value), Dict{String, NamedTuple{(:score, :weight), Tuple{Float64, Float64}}}())
        group[system_value] = payload
    end

    dropped = 0
    pairs = PegPair[]
    for ((id_value, cluster_value, stratum_value), systems) in by_pair
        haskey(systems, baseline) && haskey(systems, candidate) || begin
            if drop_incomplete
                dropped += 1
                continue
            end
            peg_error("incomplete pair for id=$(id_value) cluster=$(cluster_value) stratum=$(stratum_value)")
        end

        baseline_payload = systems[baseline]
        candidate_payload = systems[candidate]
        isapprox(baseline_payload.weight, candidate_payload.weight; atol = 1e-10, rtol = 1e-10) ||
            peg_error("pair weights must match after aggregation for id=$(id_value) cluster=$(cluster_value) stratum=$(stratum_value)")

        delta = candidate_payload.score - baseline_payload.score
        push!(
            pairs,
            PegPair(
                id_value,
                cluster_value,
                stratum_value,
                baseline_payload.weight,
                baseline_payload.score,
                candidate_payload.score,
                delta,
                delta > tie_tolerance,
                delta < -tie_tolerance,
                abs(delta) <= tie_tolerance
            )
        )
    end

    isempty(pairs) && peg_error("no complete pairs remained after preprocessing")
    sort!(pairs, by = p -> (p.stratum, p.cluster, p.id))
    return pairs, dropped
end

function peg_observed_metrics(pairs::Vector{PegPair})
    weights = [pair.weight for pair in pairs]
    baselines = [pair.baseline for pair in pairs]
    candidates = [pair.candidate for pair in pairs]
    deltas = [pair.delta for pair in pairs]
    wins = [pair.win for pair in pairs]
    losses = [pair.loss for pair in pairs]
    ties = [pair.tie for pair in pairs]

    baseline_mean = peg_weighted_mean(baselines, weights)
    candidate_mean = peg_weighted_mean(candidates, weights)
    delta_mean = peg_weighted_mean(deltas, weights)
    delta_sd = peg_weighted_sd(deltas, weights)

    return (
        baseline_mean = baseline_mean,
        candidate_mean = candidate_mean,
        delta_mean = delta_mean,
        relative_lift = abs(baseline_mean) <= eps(Float64) ? NaN : delta_mean / abs(baseline_mean),
        win_rate = peg_weighted_rate(wins, weights),
        loss_rate = peg_weighted_rate(losses, weights),
        tie_rate = peg_weighted_rate(ties, weights),
        standardized_delta = (!isfinite(delta_sd) || delta_sd == 0.0) ? NaN : delta_mean / delta_sd
    )
end

function peg_cluster_index(pairs::Vector{PegPair})
    strata = Dict{String, Dict{String, Vector{Int}}}()
    for (idx, pair) in enumerate(pairs)
        clusters = get!(strata, pair.stratum, Dict{String, Vector{Int}}())
        rows = get!(clusters, pair.cluster, Int[])
        push!(rows, idx)
    end
    strata
end

function peg_bootstrap!(
    rng::AbstractRNG,
    pairs::Vector{PegPair},
    cluster_index::Dict{String, Dict{String, Vector{Int}}},
    draws::Int,
    confidence::Float64
)
    delta_draws = Vector{Float64}(undef, draws)
    win_draws = Vector{Float64}(undef, draws)
    baseline_draws = Vector{Float64}(undef, draws)
    candidate_draws = Vector{Float64}(undef, draws)

    stratum_keys = sort(collect(keys(cluster_index)))

    for draw_idx in 1:draws
        sampled_indices = Int[]
        sizehint!(sampled_indices, length(pairs))

        for stratum in stratum_keys
            cluster_rows = cluster_index[stratum]
            cluster_keys = sort(collect(keys(cluster_rows)))
            nclusters = length(cluster_keys)
            for _ in 1:nclusters
                chosen = cluster_keys[rand(rng, 1:nclusters)]
                append!(sampled_indices, cluster_rows[chosen])
            end
        end

        boot_pairs = pairs[sampled_indices]
        metrics = peg_observed_metrics(boot_pairs)
        delta_draws[draw_idx] = metrics.delta_mean
        win_draws[draw_idx] = metrics.win_rate
        baseline_draws[draw_idx] = metrics.baseline_mean
        candidate_draws[draw_idx] = metrics.candidate_mean
    end

    return (
        delta = delta_draws,
        win = win_draws,
        baseline = baseline_draws,
        candidate = candidate_draws,
        delta_interval = peg_quantile_interval(delta_draws, confidence),
        win_interval = peg_quantile_interval(win_draws, confidence),
        baseline_interval = peg_quantile_interval(baseline_draws, confidence),
        candidate_interval = peg_quantile_interval(candidate_draws, confidence),
        p_candidate_not_better = mean(delta_draws .<= 0.0),
        p_candidate_worse = mean(delta_draws .< 0.0),
        p_two_sided = min(1.0, 2.0 * min(mean(delta_draws .<= 0.0), mean(delta_draws .>= 0.0)))
    )
end

function paired_eval_gate(
    data;
    baseline,
    candidate,
    id_col = :item_id,
    system_col = :system,
    score_col = :score,
    cluster_col = nothing,
    stratum_col = nothing,
    weight_col = nothing,
    aggregate = :mean,
    draws::Integer = 4000,
    confidence::Real = 0.95,
    seed = nothing,
    drop_incomplete::Bool = true,
    tie_tolerance::Real = 0.0,
    keep_draws::Bool = false
)
    draws >= 200 || peg_error("draws must be at least 200")
    (0.0 < Float64(confidence) < 1.0) || peg_error("confidence must be in (0, 1)")

    pairs, dropped = peg_prepare_pairs(
        data;
        baseline = baseline,
        candidate = candidate,
        id_col = id_col,
        system_col = system_col,
        score_col = score_col,
        cluster_col = cluster_col,
        stratum_col = stratum_col,
        weight_col = weight_col,
        aggregate = aggregate,
        drop_incomplete = drop_incomplete,
        tie_tolerance = Float64(tie_tolerance)
    )

    observed = peg_observed_metrics(pairs)
    cluster_index = peg_cluster_index(pairs)
    rng = isnothing(seed) ? Random.default_rng() : MersenneTwister(seed)
    bootstrap = peg_bootstrap!(rng, pairs, cluster_index, Int(draws), Float64(confidence))

    draw_store = if keep_draws
        Dict(
            :delta_mean => bootstrap.delta,
            :win_rate => bootstrap.win,
            :baseline_mean => bootstrap.baseline,
            :candidate_mean => bootstrap.candidate
        )
    else
        nothing
    end

    return PairedEvalGateResult(
        peg_to_string(baseline),
        peg_to_string(candidate),
        Int(draws),
        Float64(confidence),
        dropped,
        length(pairs),
        length(unique([pair.cluster for pair in pairs])),
        length(unique([pair.stratum for pair in pairs])),
        observed.baseline_mean,
        observed.candidate_mean,
        observed.delta_mean,
        observed.relative_lift,
        observed.win_rate,
        observed.loss_rate,
        observed.tie_rate,
        observed.standardized_delta,
        bootstrap.baseline_interval,
        bootstrap.candidate_interval,
        bootstrap.delta_interval,
        bootstrap.win_interval,
        bootstrap.p_candidate_not_better,
        bootstrap.p_candidate_worse,
        bootstrap.p_two_sided,
        pairs,
        draw_store
    )
end

function gate_decision(
    result::PairedEvalGateResult;
    noninferiority_margin::Real = 0.0,
    superiority_margin::Real = 0.0
)
    return (
        baseline = result.baseline,
        candidate = result.candidate,
        lower_bound = result.delta_interval.low,
        upper_bound = result.delta_interval.high,
        noninferiority_margin = Float64(noninferiority_margin),
        superiority_margin = Float64(superiority_margin),
        passes_noninferiority = result.delta_interval.low > noninferiority_margin,
        passes_superiority = result.delta_interval.low > superiority_margin,
        p_candidate_not_better = result.p_candidate_not_better,
        p_two_sided = result.p_two_sided
    )
end

function result_table(result::PairedEvalGateResult)
    return (
        baseline = result.baseline,
        candidate = result.candidate,
        pair_count = result.pair_count,
        cluster_count = result.cluster_count,
        stratum_count = result.stratum_count,
        baseline_mean = result.baseline_mean,
        candidate_mean = result.candidate_mean,
        delta_mean = result.delta_mean,
        delta_ci_low = result.delta_interval.low,
        delta_ci_high = result.delta_interval.high,
        win_rate = result.win_rate,
        win_ci_low = result.win_interval.low,
        win_ci_high = result.win_interval.high,
        p_candidate_not_better = result.p_candidate_not_better,
        p_two_sided = result.p_two_sided
    )
end

if abspath(PROGRAM_FILE) == @__FILE__
    sample = [
        (item_id = "item-01", system = "baseline-v1", score = 0.62, cluster = "chat", stratum = "easy", weight = 1.0),
        (item_id = "item-01", system = "candidate-v2", score = 0.69, cluster = "chat", stratum = "easy", weight = 1.0),
        (item_id = "item-02", system = "baseline-v1", score = 0.58, cluster = "chat", stratum = "easy", weight = 1.2),
        (item_id = "item-02", system = "candidate-v2", score = 0.63, cluster = "chat", stratum = "easy", weight = 1.2),
        (item_id = "item-03", system = "baseline-v1", score = 0.74, cluster = "search", stratum = "easy", weight = 0.9),
        (item_id = "item-03", system = "candidate-v2", score = 0.79, cluster = "search", stratum = "easy", weight = 0.9),
        (item_id = "item-04", system = "baseline-v1", score = 0.49, cluster = "search", stratum = "hard", weight = 1.0),
        (item_id = "item-04", system = "candidate-v2", score = 0.56, cluster = "search", stratum = "hard", weight = 1.0),
        (item_id = "item-05", system = "baseline-v1", score = 0.66, cluster = "coding", stratum = "hard", weight = 1.1),
        (item_id = "item-05", system = "candidate-v2", score = 0.72, cluster = "coding", stratum = "hard", weight = 1.1),
        (item_id = "item-06", system = "baseline-v1", score = 0.71, cluster = "coding", stratum = "hard", weight = 1.1),
        (item_id = "item-06", system = "candidate-v2", score = 0.75, cluster = "coding", stratum = "hard", weight = 1.1)
    ]

    demo = paired_eval_gate(
        sample;
        baseline = "baseline-v1",
        candidate = "candidate-v2",
        id_col = :item_id,
        system_col = :system,
        score_col = :score,
        cluster_col = :cluster,
        stratum_col = :stratum,
        weight_col = :weight,
        draws = 500,
        seed = 7
    )
    println(demo)
end

# This solves paired LLM evaluation analysis, prompt rollout gating, judge-model comparison, and agent benchmark release decisions where naive averages give the wrong answer. Built because in April 2026 a lot of teams still run strong eval suites but make launch calls with statistics that ignore pairing, strata, and repeated measurements. Use it when you need clustered paired bootstrap confidence intervals, win rates, and go or no-go release checks for model, prompt, tool, or agent changes in Julia without pulling in a package stack. The trick: it aggregates repeated rows safely, resamples clusters inside each stratum, and returns the metrics reviewers actually ask for in AI eval docs. Drop this into a Julia eval repo, CI quality gate, internal benchmark harness, or research workflow when you need practical paired bootstrap analysis for LLM evals, agent benchmarking, and model comparison.