# CausalReleaseGuard.jl
module CausalReleaseGuard

using Printf
using Random
using Statistics

export Observation,
       MetricPolicy,
       MetricResult,
       ReleaseDecision,
       load_observations,
       parse_metric_policy,
       analyze,
       decide_release,
       main

const DEFAULT_CONTROL = "control"
const DEFAULT_FORMAT = "json"

struct Observation
    unit::String
    variant::String
    metric::String
    value::Float64
    weight::Float64
    stratum::String
    segment::String
    timestamp::String
end

struct MetricPolicy
    name::String
    direction::Symbol
    role::Symbol
    min_effect::Float64
end

struct MetricResult
    metric::String
    segment::String
    role::Symbol
    direction::Symbol
    effect::Float64
    signed_effect::Float64
    ci_low::Float64
    ci_high::Float64
    signed_low::Float64
    signed_high::Float64
    min_effect::Float64
    p_value::Float64
    status::Symbol
    n_control::Int
    n_candidate::Int
    strata::Int
    alpha_used::Float64
end

struct ReleaseDecision
    action::Symbol
    reason::String
    control::String
    candidate::String
    alpha::Float64
    alpha_used::Float64
    look::Int
end

mutable struct Options
    input::String
    control::String
    candidate::Union{Nothing,String}
    policies::Vector{MetricPolicy}
    alpha::Float64
    look::Int
    bootstraps::Int
    seed::Int
    trim_z::Float64
    format::String
end

Options() = Options("", DEFAULT_CONTROL, nothing, MetricPolicy[], 0.05, 1, 800, 17, 6.0, DEFAULT_FORMAT)

mutable struct CellAccumulator
    control_values::Vector{Float64}
    control_weights::Vector{Float64}
    candidate_values::Vector{Float64}
    candidate_weights::Vector{Float64}
end

CellAccumulator() = CellAccumulator(Float64[], Float64[], Float64[], Float64[])

struct PreparedStratum
    name::String
    control_values::Vector{Float64}
    control_weights::Vector{Float64}
    candidate_values::Vector{Float64}
    candidate_weights::Vector{Float64}
    mass::Float64
end

function normalize_header(s::AbstractString)::String
    out = lowercase(strip(String(s)))
    out = replace(out, "-" => "_")
    out = replace(out, " " => "_")
    return out
end

function detect_delimiter(header::AbstractString)::Char
    commas = count(==(','), header)
    tabs = count(==('\t'), header)
    semis = count(==(';'), header)
    if tabs >= commas && tabs >= semis
        return '\t'
    elseif semis >= commas
        return ';'
    end
    return ','
end

function split_record(line::AbstractString, delimiter::Char)::Vector{String}
    fields = String[]
    buf = IOBuffer()
    in_quote = false
    i = firstindex(line)
    while i <= lastindex(line)
        c = line[i]
        if in_quote
            if c == '"'
                j = nextind(line, i)
                if j <= lastindex(line) && line[j] == '"'
                    print(buf, '"')
                    i = j
                else
                    in_quote = false
                end
            else
                print(buf, c)
            end
        elseif c == '"'
            in_quote = true
        elseif c == delimiter
            push!(fields, String(take!(buf)))
        else
            print(buf, c)
        end
        i = nextind(line, i)
    end
    in_quote && throw(ArgumentError("CSV row has an unclosed quote: $(line)"))
    push!(fields, String(take!(buf)))
    return fields
end

function header_index(headers::Vector{String})::Dict{String,Int}
    index = Dict{String,Int}()
    for (i, header) in enumerate(headers)
        key = normalize_header(header)
        isempty(key) || haskey(index, key) || (index[key] = i)
    end
    return index
end

function choose_column(index::Dict{String,Int}, names::Vector{String}, label::String)::Int
    for name in names
        key = normalize_header(name)
        haskey(index, key) && return index[key]
    end
    throw(ArgumentError("missing required $(label) column; tried $(join(names, ", "))"))
end

function optional_column(index::Dict{String,Int}, names::Vector{String})::Union{Nothing,Int}
    for name in names
        key = normalize_header(name)
        haskey(index, key) && return index[key]
    end
    return nothing
end

function value_at(row::Vector{String}, idx::Union{Nothing,Int}, fallback::String="")::String
    idx === nothing && return fallback
    idx > length(row) && return fallback
    return strip(row[idx])
end

function parse_float_field(raw::AbstractString, column::String, line_no::Int; default::Union{Nothing,Float64}=nothing)::Float64
    text = replace(strip(String(raw)), "_" => "")
    if isempty(text)
        default === nothing && throw(ArgumentError("line $(line_no): empty numeric value in $(column)"))
        return default
    end
    parsed = tryparse(Float64, text)
    parsed === nothing && throw(ArgumentError("line $(line_no): cannot parse $(column)=$(raw) as Float64"))
    isfinite(parsed) || throw(ArgumentError("line $(line_no): $(column) must be finite"))
    return parsed
end

function load_observations(path::AbstractString)::Vector{Observation}
    io = path == "-" ? stdin : open(path, "r")
    try
        header_line = eof(io) ? throw(ArgumentError("input is empty")) : readline(io)
        delimiter = detect_delimiter(header_line)
        headers = split_record(header_line, delimiter)
        index = header_index(headers)
        unit_col = choose_column(index, ["unit", "unit_id", "id", "request_id", "user_id", "account_id"], "unit")
        variant_col = choose_column(index, ["variant", "arm", "treatment", "release", "group"], "variant")
        metric_col = choose_column(index, ["metric", "metric_name", "name"], "metric")
        value_col = choose_column(index, ["value", "metric_value", "y", "amount"], "value")
        weight_col = optional_column(index, ["weight", "sample_weight", "ipw", "inverse_propensity_weight"])
        stratum_col = optional_column(index, ["stratum", "block", "region", "market", "cohort"])
        segment_col = optional_column(index, ["segment", "slice", "feature", "platform"])
        timestamp_col = optional_column(index, ["timestamp", "ts", "time", "event_time"])

        observations = Observation[]
        line_no = 1
        for line in eachline(io)
            line_no += 1
            isempty(strip(line)) && continue
            row = split_record(line, delimiter)
            unit = value_at(row, unit_col)
            variant = lowercase(value_at(row, variant_col))
            metric = value_at(row, metric_col)
            value = parse_float_field(value_at(row, value_col), "value", line_no)
            weight = parse_float_field(value_at(row, weight_col, "1.0"), "weight", line_no, default=1.0)
            weight > 0.0 || throw(ArgumentError("line $(line_no): weight must be positive"))
            stratum = value_at(row, stratum_col, "global")
            segment = value_at(row, segment_col, "all")
            timestamp = value_at(row, timestamp_col, "")
            isempty(unit) && throw(ArgumentError("line $(line_no): unit is empty"))
            isempty(variant) && throw(ArgumentError("line $(line_no): variant is empty"))
            isempty(metric) && throw(ArgumentError("line $(line_no): metric is empty"))
            push!(observations, Observation(unit, variant, metric, value, weight, stratum, segment, timestamp))
        end
        isempty(observations) && throw(ArgumentError("input has no observations"))
        return observations
    finally
        path == "-" || close(io)
    end
end

function parse_metric_policy(spec::AbstractString)::MetricPolicy
    parts = split(String(spec), ':')
    length(parts) >= 2 || throw(ArgumentError("metric policy must be name:direction[:role[:min_effect]]"))
    name = strip(parts[1])
    direction = Symbol(lowercase(strip(parts[2])))
    role = length(parts) >= 3 && !isempty(strip(parts[3])) ? Symbol(lowercase(strip(parts[3]))) : :primary
    min_effect = length(parts) >= 4 && !isempty(strip(parts[4])) ? parse(Float64, strip(parts[4])) : 0.0
    isempty(name) && throw(ArgumentError("metric policy name cannot be empty"))
    direction in (:higher, :lower) || throw(ArgumentError("metric direction must be higher or lower"))
    role in (:primary, :guardrail) || throw(ArgumentError("metric role must be primary or guardrail"))
    isfinite(min_effect) || throw(ArgumentError("min_effect must be finite"))
    return MetricPolicy(name, direction, role, min_effect)
end

function infer_candidate(observations::Vector{Observation}, control::String)::String
    variants = sort!(collect(Set(o.variant for o in observations)))
    candidates = [variant for variant in variants if variant != control]
    if length(candidates) != 1
        throw(ArgumentError("cannot infer candidate variant; found $(join(variants, ", ")). Pass --candidate."))
    end
    return candidates[1]
end

function infer_policies(observations::Vector{Observation}, policies::Vector{MetricPolicy})::Dict{String,MetricPolicy}
    by_name = Dict{String,MetricPolicy}()
    for policy in policies
        by_name[policy.name] = policy
    end
    for metric in sort!(collect(Set(o.metric for o in observations)))
        haskey(by_name, metric) || (by_name[metric] = MetricPolicy(metric, :higher, :primary, 0.0))
    end
    return by_name
end

function robust_clip(values::Vector{Float64}, z::Float64)::Vector{Float64}
    isempty(values) && return Float64[]
    center = median(values)
    deviations = abs.(values .- center)
    scale = 1.4826 * median(deviations)
    if !isfinite(scale) || scale <= eps(Float64)
        return copy(values)
    end
    lo = center - z * scale
    hi = center + z * scale
    return [clamp(v, lo, hi) for v in values]
end

function weighted_mean(values::Vector{Float64}, weights::Vector{Float64})::Float64
    length(values) == length(weights) || throw(ArgumentError("values and weights length mismatch"))
    total_weight = 0.0
    total = 0.0
    for i in eachindex(values)
        w = weights[i]
        if w > 0.0 && isfinite(w)
            total_weight += w
            total += w * values[i]
        end
    end
    total_weight > 0.0 || return NaN
    return total / total_weight
end

function build_cells(observations::Vector{Observation}, metric::String, segment::String, control::String, candidate::String)::Dict{String,CellAccumulator}
    cells = Dict{String,CellAccumulator}()
    for o in observations
        o.metric == metric || continue
        o.segment == segment || continue
        o.variant == control || o.variant == candidate || continue
        acc = get!(cells, o.stratum) do
            CellAccumulator()
        end
        if o.variant == control
            push!(acc.control_values, o.value)
            push!(acc.control_weights, o.weight)
        else
            push!(acc.candidate_values, o.value)
            push!(acc.candidate_weights, o.weight)
        end
    end
    return cells
end

function prepare_strata(cells::Dict{String,CellAccumulator}, trim_z::Float64)::Vector{PreparedStratum}
    strata = PreparedStratum[]
    for name in sort!(collect(keys(cells)))
        acc = cells[name]
        isempty(acc.control_values) && continue
        isempty(acc.candidate_values) && continue
        control_values = robust_clip(acc.control_values, trim_z)
        candidate_values = robust_clip(acc.candidate_values, trim_z)
        control_mass = sum(acc.control_weights)
        candidate_mass = sum(acc.candidate_weights)
        mass = sqrt(control_mass * candidate_mass)
        mass > 0.0 || continue
        push!(strata, PreparedStratum(name, control_values, copy(acc.control_weights), candidate_values, copy(acc.candidate_weights), mass))
    end
    return strata
end

function estimate_effect(strata::Vector{PreparedStratum})::Float64
    weighted_delta = 0.0
    total_mass = 0.0
    for s in strata
        control_mean = weighted_mean(s.control_values, s.control_weights)
        candidate_mean = weighted_mean(s.candidate_values, s.candidate_weights)
        isfinite(control_mean) && isfinite(candidate_mean) || continue
        weighted_delta += s.mass * (candidate_mean - control_mean)
        total_mass += s.mass
    end
    total_mass > 0.0 || return NaN
    return weighted_delta / total_mass
end

function resampled_mean(values::Vector{Float64}, weights::Vector{Float64}, rng::AbstractRNG)::Float64
    n = length(values)
    n == 0 && return NaN
    total = 0.0
    total_weight = 0.0
    for _ in 1:n
        i = rand(rng, 1:n)
        w = weights[i]
        total += w * values[i]
        total_weight += w
    end
    total_weight > 0.0 || return NaN
    return total / total_weight
end

function bootstrap_effect(strata::Vector{PreparedStratum}, rng::AbstractRNG)::Float64
    weighted_delta = 0.0
    total_mass = 0.0
    for s in strata
        control_mean = resampled_mean(s.control_values, s.control_weights, rng)
        candidate_mean = resampled_mean(s.candidate_values, s.candidate_weights, rng)
        isfinite(control_mean) && isfinite(candidate_mean) || continue
        weighted_delta += s.mass * (candidate_mean - control_mean)
        total_mass += s.mass
    end
    total_mass > 0.0 || return NaN
    return weighted_delta / total_mass
end

function percentile(sorted_values::Vector{Float64}, p::Float64)::Float64
    isempty(sorted_values) && return NaN
    n = length(sorted_values)
    p = clamp(p, 0.0, 1.0)
    rank = 1.0 + (n - 1) * p
    lo = floor(Int, rank)
    hi = ceil(Int, rank)
    lo == hi && return sorted_values[lo]
    frac = rank - lo
    return sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac
end

function sequential_alpha(alpha::Float64, look::Int, metric_count::Int)::Float64
    alpha > 0.0 && alpha < 1.0 || throw(ArgumentError("alpha must be between 0 and 1"))
    look >= 1 || throw(ArgumentError("look must be >= 1"))
    metric_count >= 1 || throw(ArgumentError("metric_count must be >= 1"))
    spend = alpha / (look * (look + 1))
    return spend / metric_count
end

function signed_bounds(direction::Symbol, effect::Float64, lo::Float64, hi::Float64)::Tuple{Float64,Float64,Float64}
    if direction == :higher
        return effect, lo, hi
    end
    return -effect, -hi, -lo
end

function result_status(signed_low::Float64, signed_high::Float64, min_effect::Float64)::Symbol
    signed_low >= min_effect && return :pass
    signed_high < min_effect && return :fail
    return :uncertain
end

function analyze_metric(observations::Vector{Observation}, policy::MetricPolicy, segment::String, control::String, candidate::String;
                        alpha_used::Float64, bootstraps::Int, seed::Int, trim_z::Float64)::MetricResult
    cells = build_cells(observations, policy.name, segment, control, candidate)
    strata = prepare_strata(cells, trim_z)
    isempty(strata) && throw(ArgumentError("metric $(policy.name)/$(segment) has no overlapping strata between $(control) and $(candidate)"))
    effect = estimate_effect(strata)
    rng = MersenneTwister(seed + mod(hash((policy.name, segment)), 1_000_000))
    draws = Float64[]
    sizehint!(draws, bootstraps)
    for _ in 1:bootstraps
        draw = bootstrap_effect(strata, rng)
        isfinite(draw) && push!(draws, draw)
    end
    length(draws) >= max(40, div(min(bootstraps, 100), 2)) || throw(ArgumentError("not enough bootstrap draws for $(policy.name)/$(segment)"))
    sort!(draws)
    lo = percentile(draws, alpha_used / 2.0)
    hi = percentile(draws, 1.0 - alpha_used / 2.0)
    signed_effect, signed_low, signed_high = signed_bounds(policy.direction, effect, lo, hi)
    signed_draws = policy.direction == :higher ? draws : sort!([-d for d in draws])
    p_value = (count(d -> d <= policy.min_effect, signed_draws) + 1.0) / (length(signed_draws) + 1.0)
    status = result_status(signed_low, signed_high, policy.min_effect)
    n_control = sum(length(s.control_values) for s in strata)
    n_candidate = sum(length(s.candidate_values) for s in strata)
    return MetricResult(policy.name, segment, policy.role, policy.direction, effect, signed_effect, lo, hi,
                        signed_low, signed_high, policy.min_effect, p_value, status, n_control, n_candidate,
                        length(strata), alpha_used)
end

function metric_segments(observations::Vector{Observation})::Vector{Tuple{String,String}}
    pairs = Set{Tuple{String,String}}()
    for o in observations
        push!(pairs, (o.metric, o.segment))
    end
    return sort!(collect(pairs), by=x -> (x[1], x[2]))
end

function analyze(observations::Vector{Observation}; control::String=DEFAULT_CONTROL, candidate::Union{Nothing,String}=nothing,
                 policies::Vector{MetricPolicy}=MetricPolicy[], alpha::Float64=0.05, look::Int=1,
                 bootstraps::Int=800, seed::Int=17, trim_z::Float64=6.0)::Tuple{ReleaseDecision,Vector{MetricResult}}
    normalized_control = lowercase(control)
    normalized_candidate = candidate === nothing ? infer_candidate(observations, normalized_control) : lowercase(candidate)
    normalized_control != normalized_candidate || throw(ArgumentError("control and candidate cannot match"))
    bootstraps >= 80 || throw(ArgumentError("bootstraps must be at least 80"))
    by_metric = infer_policies(observations, policies)
    pairs = [(metric, segment) for (metric, segment) in metric_segments(observations) if haskey(by_metric, metric)]
    isempty(pairs) && throw(ArgumentError("no metric segments available for analysis"))
    alpha_used = sequential_alpha(alpha, look, length(pairs))
    results = MetricResult[]
    for (metric, segment) in pairs
        push!(results, analyze_metric(observations, by_metric[metric], segment, normalized_control, normalized_candidate;
                                      alpha_used=alpha_used, bootstraps=bootstraps, seed=seed, trim_z=trim_z))
    end
    decision = decide_release(results, normalized_control, normalized_candidate, alpha, alpha_used, look)
    return decision, results
end

function decide_release(results::Vector{MetricResult}, control::String, candidate::String, alpha::Float64, alpha_used::Float64, look::Int)::ReleaseDecision
    primaries = [r for r in results if r.role == :primary]
    guardrails = [r for r in results if r.role == :guardrail]
    failing = [r for r in results if r.status == :fail]
    uncertain_primary = [r for r in primaries if r.status == :uncertain]
    uncertain_guardrails = [r for r in guardrails if r.status == :uncertain]
    if isempty(primaries)
        return ReleaseDecision(:hold, "no primary metric policy was provided", control, candidate, alpha, alpha_used, look)
    elseif !isempty(failing)
        names = join(["$(r.metric)/$(r.segment)" for r in failing], ", ")
        return ReleaseDecision(:block, "statistically clear regression or missed threshold: $(names)", control, candidate, alpha, alpha_used, look)
    elseif !isempty(uncertain_primary)
        names = join(["$(r.metric)/$(r.segment)" for r in uncertain_primary], ", ")
        return ReleaseDecision(:hold, "primary metric still uncertain: $(names)", control, candidate, alpha, alpha_used, look)
    elseif !isempty(uncertain_guardrails)
        names = join(["$(r.metric)/$(r.segment)" for r in uncertain_guardrails], ", ")
        return ReleaseDecision(:hold, "guardrail still uncertain: $(names)", control, candidate, alpha, alpha_used, look)
    end
    return ReleaseDecision(:ship, "primary metrics passed and guardrails stayed inside policy", control, candidate, alpha, alpha_used, look)
end

function json_escape(s::AbstractString)::String
    buf = IOBuffer()
    for c in String(s)
        if c == '"'
            print(buf, "\\\"")
        elseif c == '\\'
            print(buf, "\\\\")
        elseif c == '\n'
            print(buf, "\\n")
        elseif c == '\r'
            print(buf, "\\r")
        elseif c == '\t'
            print(buf, "\\t")
        elseif Int(c) < 0x20
            @printf(buf, "\\u%04x", Int(c))
        else
            print(buf, c)
        end
    end
    return String(take!(buf))
end

json_string(s::AbstractString)::String = "\"$(json_escape(s))\""
json_atom(x::Symbol)::String = json_string(String(x))
json_atom(x::AbstractString)::String = json_string(x)
json_atom(x::Integer)::String = string(x)
json_atom(x::Float64)::String = isfinite(x) ? @sprintf("%.12g", x) : "null"

function write_json(io::IO, decision::ReleaseDecision, results::Vector{MetricResult})::Nothing
    println(io, "{")
    println(io, "  \"action\": ", json_atom(decision.action), ",")
    println(io, "  \"reason\": ", json_atom(decision.reason), ",")
    println(io, "  \"control\": ", json_atom(decision.control), ",")
    println(io, "  \"candidate\": ", json_atom(decision.candidate), ",")
    println(io, "  \"alpha\": ", json_atom(decision.alpha), ",")
    println(io, "  \"alpha_used_per_metric\": ", json_atom(decision.alpha_used), ",")
    println(io, "  \"look\": ", json_atom(decision.look), ",")
    println(io, "  \"metrics\": [")
    for (i, r) in enumerate(results)
        println(io, "    {")
        println(io, "      \"metric\": ", json_atom(r.metric), ",")
        println(io, "      \"segment\": ", json_atom(r.segment), ",")
        println(io, "      \"role\": ", json_atom(r.role), ",")
        println(io, "      \"direction\": ", json_atom(r.direction), ",")
        println(io, "      \"effect_candidate_minus_control\": ", json_atom(r.effect), ",")
        println(io, "      \"signed_effect\": ", json_atom(r.signed_effect), ",")
        println(io, "      \"ci_low\": ", json_atom(r.ci_low), ",")
        println(io, "      \"ci_high\": ", json_atom(r.ci_high), ",")
        println(io, "      \"signed_low\": ", json_atom(r.signed_low), ",")
        println(io, "      \"signed_high\": ", json_atom(r.signed_high), ",")
        println(io, "      \"min_effect\": ", json_atom(r.min_effect), ",")
        println(io, "      \"p_value_bootstrap\": ", json_atom(r.p_value), ",")
        println(io, "      \"status\": ", json_atom(r.status), ",")
        println(io, "      \"n_control\": ", json_atom(r.n_control), ",")
        println(io, "      \"n_candidate\": ", json_atom(r.n_candidate), ",")
        println(io, "      \"strata\": ", json_atom(r.strata), ",")
        println(io, "      \"alpha_used\": ", json_atom(r.alpha_used))
        print(io, "    }")
        i == length(results) ? println(io) : println(io, ",")
    end
    println(io, "  ]")
    println(io, "}")
    return nothing
end

function write_text(io::IO, decision::ReleaseDecision, results::Vector{MetricResult})::Nothing
    println(io, "decision: ", decision.action)
    println(io, "reason: ", decision.reason)
    println(io, "control: ", decision.control, " candidate: ", decision.candidate)
    println(io, "alpha_used_per_metric: ", @sprintf("%.6g", decision.alpha_used), " look: ", decision.look)
    println(io)
    for r in results
        println(io, "$(r.metric)/$(r.segment) [$(r.role), $(r.direction)] status=$(r.status)")
        println(io, "  effect candidate-control = ", @sprintf("%.6g", r.effect),
                "  signed = ", @sprintf("%.6g", r.signed_effect),
                "  signed_ci = [", @sprintf("%.6g", r.signed_low), ", ", @sprintf("%.6g", r.signed_high), "]")
        println(io, "  min_effect = ", @sprintf("%.6g", r.min_effect),
                "  p_bootstrap = ", @sprintf("%.6g", r.p_value),
                "  n = ", r.n_control, "/", r.n_candidate,
                "  strata = ", r.strata)
    end
    return nothing
end

function usage(io::IO=stdout)::Nothing
    print(io, """
Usage: julia CausalReleaseGuard.jl --input canary.csv --candidate treatment [options]

CSV columns: unit, variant, metric, value. Optional columns: weight, stratum, segment, timestamp.
Metric policy syntax: --metric name:higher|lower:primary|guardrail:min_effect

Options:
  --input PATH            CSV/TSV file, or - for stdin
  --control NAME          Control variant, default control
  --candidate NAME        Candidate variant; inferred when there is exactly one non-control arm
  --metric SPEC           Repeatable metric policy, for example latency:lower:guardrail:-5
  --alpha VALUE           Family alpha before sequential spending, default 0.05
  --look N                Sequential look number, default 1
  --bootstraps N          Stratified bootstrap draws, default 800
  --seed N                Deterministic RNG seed, default 17
  --trim-z VALUE          Robust MAD clipping width, default 6
  --format json|text      Output format, default json
  --help                  Show this help
""")
    return nothing
end

function require_value(args::Vector{String}, i::Int, flag::String)::String
    i + 1 <= length(args) || throw(ArgumentError("$(flag) needs a value"))
    return args[i + 1]
end

function parse_args(args::Vector{String})::Options
    opts = Options()
    i = 1
    while i <= length(args)
        arg = args[i]
        if arg == "--help" || arg == "-h"
            usage(stdout)
            exit(0)
        elseif arg == "--input"
            opts.input = require_value(args, i, arg); i += 1
        elseif arg == "--control"
            opts.control = lowercase(require_value(args, i, arg)); i += 1
        elseif arg == "--candidate"
            opts.candidate = lowercase(require_value(args, i, arg)); i += 1
        elseif arg == "--metric"
            push!(opts.policies, parse_metric_policy(require_value(args, i, arg))); i += 1
        elseif arg == "--alpha"
            opts.alpha = parse(Float64, require_value(args, i, arg)); i += 1
        elseif arg == "--look"
            opts.look = parse(Int, require_value(args, i, arg)); i += 1
        elseif arg == "--bootstraps"
            opts.bootstraps = parse(Int, require_value(args, i, arg)); i += 1
        elseif arg == "--seed"
            opts.seed = parse(Int, require_value(args, i, arg)); i += 1
        elseif arg == "--trim-z"
            opts.trim_z = parse(Float64, require_value(args, i, arg)); i += 1
        elseif arg == "--format"
            opts.format = lowercase(require_value(args, i, arg)); i += 1
        elseif startswith(arg, "--")
            throw(ArgumentError("unknown option $(arg)"))
        elseif isempty(opts.input)
            opts.input = arg
        else
            throw(ArgumentError("unexpected positional argument $(arg)"))
        end
        i += 1
    end
    isempty(opts.input) && throw(ArgumentError("--input is required"))
    opts.format in ("json", "text") || throw(ArgumentError("--format must be json or text"))
    opts.trim_z > 0.0 || throw(ArgumentError("--trim-z must be positive"))
    return opts
end

function main(args::Vector{String}=ARGS)::Nothing
    try
        opts = parse_args(args)
        observations = load_observations(opts.input)
        decision, results = analyze(observations; control=opts.control, candidate=opts.candidate,
                                    policies=opts.policies, alpha=opts.alpha, look=opts.look,
                                    bootstraps=opts.bootstraps, seed=opts.seed, trim_z=opts.trim_z)
        if opts.format == "json"
            write_json(stdout, decision, results)
        else
            write_text(stdout, decision, results)
        end
    catch err
        println(stderr, "CausalReleaseGuard error: ", sprint(showerror, err))
        exit(2)
    end
    return nothing
end

end

if abspath(PROGRAM_FILE) == @__FILE__
    using .CausalReleaseGuard
    CausalReleaseGuard.main()
end

#=
This solves the April 2026 release problem where AI products, data pipelines, edge services, and DevOps platforms need to decide whether a new model, prompt, compiler flag, routing rule, feature rollout, or inference queue policy is actually better before the blast radius gets expensive. Built because normal dashboard averages are too late and plain A/B tests miss the messy parts: stratified traffic, weighted samples, repeated canary looks, heavy-tailed latency, token cost outliers, guardrail metrics, and teams checking the same experiment every few hours. Use it when you have CSV or TSV observations from OpenTelemetry, warehouse exports, gateway logs, eval runners, carbon aware schedulers, CI release jobs, or streaming data quality checks and you need one deterministic Julia file that gives a ship, hold, or block decision. The trick: it combines robust MAD clipping, stratified weighted effects, bootstrap uncertainty, sequential alpha spending, and primary versus guardrail policy checks without requiring a database, notebook, SaaS experiment tool, or private API key. Drop this into a release pipeline, model gateway, research evaluation harness, GitHub Actions job, Kubernetes canary controller, edge compute rollout, or developer productivity system where a senior engineer wants clear causal release guardrails instead of another vague chart.
=#