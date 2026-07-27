#!/usr/bin/env julia

using Dates
using Printf

const VERSION = "1.0.0"
const DEFAULTS = Dict(
    "input" => "", "output" => "stream-replay-findings.csv",
    "sarif" => "stream-replay-fence.sarif", "plan" => "stream-replay-plan.csv",
    "delimiter" => "", "max_lag_seconds" => "300", "max_gap" => "0",
    "max_retry_count" => "5", "batch_bytes" => "50000000",
    "expected_schema" => "", "allowed_regions" => "", "fail_on" => "error",
    "self_test" => "false"
)

const ALIASES = Dict(
    "stream" => ["stream", "topic", "queue", "pipeline"],
    "partition" => ["partition", "shard", "segment"],
    "offset" => ["offset", "sequence", "seq", "lsn"],
    "event_id" => ["event_id", "id", "message_id", "record_id"],
    "idempotency_key" => ["idempotency_key", "dedupe_key", "request_id"],
    "event_time" => ["event_time", "occurred_at", "source_time", "created_at"],
    "arrival_time" => ["arrival_time", "ingested_at", "received_at", "observed_at"],
    "payload_hash" => ["payload_hash", "content_hash", "body_hash", "sha256"],
    "schema_version" => ["schema_version", "schema", "contract_version"],
    "region" => ["region", "cloud_region", "residency_region"],
    "operation" => ["operation", "op", "event_type"],
    "retry_count" => ["retry_count", "attempt", "attempts", "retries"],
    "byte_size" => ["byte_size", "bytes", "payload_bytes", "size_bytes"]
)
const REQUIRED = ["stream", "partition", "offset", "event_id", "idempotency_key", "event_time", "arrival_time", "payload_hash"]
const RULES = Dict(
    "SRF001" => "Malformed row or missing replay identity",
    "SRF010" => "Duplicate event identity",
    "SRF011" => "Conflicting idempotency key",
    "SRF012" => "Repeated payload hash under different event ids",
    "SRF020" => "Partition offset gap, duplicate, or regression",
    "SRF030" => "Replay watermark or timestamp violation",
    "SRF040" => "Retry loop or write amplification risk",
    "SRF050" => "Unexpected schema version",
    "SRF060" => "Region outside replay residency boundary"
)

struct Finding
    rule::String
    severity::String
    message::String
    rows::Vector{Int}
    key::String
    stream::String
    partition::String
    action::String
end

struct Event
    row::Int
    stream::String
    partition::String
    offset::Union{Int,Nothing}
    event_id::String
    idem::String
    event_time::Union{DateTime,Nothing}
    arrival_time::Union{DateTime,Nothing}
    hash::String
    schema::String
    region::String
    op::String
    retry::Int
    bytes::Int
end

function usage(code::Int = 0)
    io = code == 0 ? stdout : stderr
    println(io, """
StreamReplayFence.jl $(VERSION)

Usage: julia StreamReplayFence.jl --input replay.csv [options]

Required logical columns, with aliases accepted:
  stream, partition, offset, event_id, idempotency_key, event_time, arrival_time, payload_hash

Options:
  --output PATH             Findings CSV
  --sarif PATH              SARIF 2.1.0 report
  --plan PATH               Replay batch plan CSV
  --delimiter csv|tsv       Force delimiter
  --max-lag-seconds N       Allowed arrival/watermark lag, default 300
  --max-gap N               Allowed offset gap per partition, default 0
  --max-retry-count N       Retry budget per key, default 5
  --batch-bytes N           Replay plan batch size, default 50000000
  --expected-schema CSV     Allowed schema versions
  --allowed-regions CSV     Allowed cloud or residency regions
  --fail-on none|warning|error
  --self-test
""")
    exit(code)
end

function parse_args(args)
    opts = copy(DEFAULTS)
    i = 1
    while i <= length(args)
        arg = args[i]
        if arg == "--help"
            usage(0)
        elseif arg == "--self-test"
            opts["self_test"] = "true"; i += 1
        elseif startswith(arg, "--")
            key = replace(arg[3:end], "-" => "_")
            haskey(opts, key) || error("unknown option $(arg)")
            i < length(args) || error("missing value for $(arg)")
            opts[key] = args[i + 1]; i += 2
        else
            error("unexpected positional argument $(arg)")
        end
    end
    opts["fail_on"] in ["none", "warning", "error"] || error("--fail-on must be none, warning, or error")
    opts["delimiter"] in ["", "csv", "tsv"] || error("--delimiter must be csv or tsv")
    return opts
end

norm(s) = replace(lowercase(strip(s)), r"^_+|_+$" => "")
header_name(s) = norm(replace(s, r"[^A-Za-z0-9]+" => "_"))
csvlist(s) = [strip(x) for x in split(s, ",") if !isempty(strip(x))]
rank(s) = s == "error" ? 2 : s == "warning" ? 1 : 0
partkey(e::Event) = e.stream * "|" * e.partition
stampkey(x) = x === nothing ? DateTime(9999, 1, 1) : x
seconds(a::DateTime, b::DateTime) = Dates.value(b - a) / 1000.0

function intopt(opts, key; min = 0)
    value = tryparse(Int, strip(opts[key]))
    value !== nothing && value >= min || error("--$(replace(key, "_" => "-")) must be an integer >= $(min)")
    return value
end

function splitrow(line::String, delim::Char, n::Int)
    out, buf, quoted = String[], IOBuffer(), false
    i = firstindex(line)
    while i <= lastindex(line)
        c = line[i]
        if quoted
            if c == '"'
                j = nextind(line, i)
                if j <= lastindex(line) && line[j] == '"'
                    print(buf, '"'); i = j
                else
                    quoted = false
                end
            else
                print(buf, c)
            end
        elseif c == '"'
            quoted = true
        elseif c == delim
            push!(out, String(take!(buf)))
        else
            print(buf, c)
        end
        i = nextind(line, i)
    end
    quoted && error("unclosed quoted field at line $(n)")
    push!(out, String(take!(buf)))
    return out
end

function readtable(path, requested)
    isempty(path) && usage(1)
    isfile(path) || error("input does not exist: $(path)")
    lines = readlines(path)
    hidx = findfirst(x -> !isempty(strip(x)), lines)
    hidx === nothing && error("input is empty: $(path)")
    delim = requested == "tsv" ? '\t' : requested == "csv" ? ',' :
        count(==('\t'), lines[hidx]) > count(==(','), lines[hidx]) ? '\t' : ','
    headers = header_name.(splitrow(lines[hidx], delim, hidx))
    length(unique(headers)) == length(headers) || error("duplicate header after normalization")
    rows = Vector{Dict{String,String}}(); nums = Int[]; findings = Finding[]
    for n in (hidx + 1):length(lines)
        isempty(strip(lines[n])) && continue
        cells = splitrow(lines[n], delim, n)
        if length(cells) != length(headers)
            push!(findings, Finding("SRF001", "error", "row has $(length(cells)) fields but header has $(length(headers))", [n], "field-count", "", "", "fix exporter before replay"))
            cells = length(cells) < length(headers) ? vcat(cells, fill("", length(headers) - length(cells))) : cells[1:length(headers)]
        end
        d = Dict{String,String}()
        for (i, h) in enumerate(headers)
            d[h] = strip(cells[i])
        end
        push!(rows, d); push!(nums, n)
    end
    return rows, nums, findings
end

function value(row, logical)
    for key in ALIASES[logical]
        haskey(row, key) && !isempty(strip(row[key])) && return strip(row[key])
    end
    return ""
end

const FORMATS = [dateformat"yyyy-mm-ddTHH:MM:SS.sss", dateformat"yyyy-mm-ddTHH:MM:SS", dateformat"yyyy-mm-dd HH:MM:SS.sss", dateformat"yyyy-mm-dd HH:MM:SS", dateformat"yyyy-mm-dd"]

function parsetime(raw, field, row, findings)
    isempty(raw) && (push!(findings, Finding("SRF001", "error", "missing timestamp $(field)", [row], field, "", "", "emit ISO 8601 UTC timestamps")); return nothing)
    s = replace(strip(raw), r"Z$" => "")
    s = replace(s, r"([+-][0-9][0-9]:[0-9][0-9])$" => "")
    m = match(r"^(.*\.[0-9][0-9][0-9])[0-9]+$", s)
    m === nothing || (s = m.captures[1])
    for f in FORMATS
        try
            return DateTime(s, f)
        catch
        end
    end
    push!(findings, Finding("SRF001", "error", "cannot parse $(field): $(raw)", [row], field, "", "", "use yyyy-mm-ddTHH:MM:SSZ"))
    return nothing
end

function parseint(raw, field, row, findings; required = false, default = 0)
    if isempty(raw)
        required && push!(findings, Finding("SRF001", "error", "missing numeric $(field)", [row], field, "", "", "populate $(field)"))
        return required ? nothing : default
    end
    x = tryparse(Int, raw)
    if x === nothing || x < 0
        push!(findings, Finding("SRF001", "error", "invalid integer $(field): $(raw)", [row], field, "", "", "emit a nonnegative integer"))
        return required ? nothing : default
    end
    return x
end

function loadevents(path, opts)
    rows, nums, findings = readtable(path, opts["delimiter"])
    events = Event[]
    for (r, n) in zip(rows, nums)
        for f in REQUIRED
            isempty(value(r, f)) && push!(findings, Finding("SRF001", "error", "missing required field $(f)", [n], f, value(r, "stream"), value(r, "partition"), "export $(f) before replay"))
        end
        off = parseint(value(r, "offset"), "offset", n, findings, required = true)
        retry = parseint(value(r, "retry_count"), "retry_count", n, findings, default = 0)
        bytes = parseint(value(r, "byte_size"), "byte_size", n, findings, default = 1)
        push!(events, Event(n, value(r, "stream"), value(r, "partition"), off,
            value(r, "event_id"), value(r, "idempotency_key"),
            parsetime(value(r, "event_time"), "event_time", n, findings),
            parsetime(value(r, "arrival_time"), "arrival_time", n, findings),
            value(r, "payload_hash"), value(r, "schema_version"),
            lowercase(value(r, "region")), lowercase(value(r, "operation")),
            retry === nothing ? 0 : retry, max(bytes === nothing ? 1 : bytes, 1)))
    end
    return events, findings
end

function add!(findings, rule, sev, msg, bucket; key = "", action = "inspect")
    rows = [e.row for e in bucket]
    push!(findings, Finding(rule, sev, msg, rows, key, isempty(bucket) ? "" : bucket[1].stream, isempty(bucket) ? "" : bucket[1].partition, action))
end

function buckets(events, f)
    d = Dict{String,Vector{Event}}()
    for e in events
        k = f(e); isempty(k) && continue
        push!(get!(d, k, Event[]), e)
    end
    return d
end

uniq(v) = Set([x for x in v if !isempty(x)])

function audit!(events, findings, opts)
    maxlag, maxgap, maxretry = intopt(opts, "max_lag_seconds"), intopt(opts, "max_gap"), intopt(opts, "max_retry_count")
    schemas, regions = Set(csvlist(opts["expected_schema"])), Set(lowercase.(csvlist(opts["allowed_regions"])))
    for e in events
        !isempty(schemas) && isempty(e.schema) && push!(findings, Finding("SRF050", "warning", "schema version missing", [e.row], "schema_version", e.stream, e.partition, "include schema_version"))
        !isempty(schemas) && !isempty(e.schema) && !(e.schema in schemas) && push!(findings, Finding("SRF050", "error", "schema $(e.schema) is not allowed", [e.row], e.schema, e.stream, e.partition, "hold until transformer matches schema"))
        !isempty(regions) && isempty(e.region) && push!(findings, Finding("SRF060", "warning", "region missing while residency is enforced", [e.row], "region", e.stream, e.partition, "add cloud region"))
        !isempty(regions) && !isempty(e.region) && !(e.region in regions) && push!(findings, Finding("SRF060", "error", "region $(e.region) is outside allowed replay boundary", [e.row], e.region, e.stream, e.partition, "route to regional worker"))
        if e.event_time !== nothing && e.arrival_time !== nothing
            lag = seconds(e.event_time, e.arrival_time)
            lag < -60 && push!(findings, Finding("SRF030", "warning", "arrival time is before event time", [e.row], @sprintf("%.3f", lag), e.stream, e.partition, "check clock skew"))
            lag > maxlag && push!(findings, Finding("SRF030", lag > max(2 * maxlag, maxlag + 3600) ? "error" : "warning", "arrival lag exceeds watermark", [e.row], @sprintf("%.3f", lag), e.stream, e.partition, "isolate late replay rows"))
        end
        e.retry > maxretry && push!(findings, Finding("SRF040", e.retry > max(2 * maxretry, maxretry + 3) ? "error" : "warning", "retry count exceeds replay budget", [e.row], e.idem, e.stream, e.partition, "inspect idempotency key"))
    end
    for (id, b) in buckets(events, e -> e.event_id)
        length(b) > 1 && add!(findings, "SRF010", length(uniq([e.hash for e in b])) > 1 || length(uniq([e.op for e in b])) > 1 ? "error" : "warning", "event id repeats across manifest", b, key = id, action = "dedupe or repair producer identity")
    end
    for (idem, b) in buckets(events, e -> e.idem)
        length(b) > 1 && add!(findings, length(uniq([e.hash for e in b])) > 1 ? "SRF011" : "SRF040", length(uniq([e.hash for e in b])) > 1 ? "error" : "warning", "idempotency key repeats or conflicts", b, key = idem, action = "collapse attempts to one write")
    end
    for (hash, b) in buckets(events, e -> e.hash)
        length(b) > 1 && length(uniq([e.event_id for e in b])) > 1 && add!(findings, "SRF012", "warning", "payload hash maps to multiple event ids", b, key = hash, action = "verify fanout or dedupe content")
    end
    for (pk, b) in buckets(events, partkey)
        byoff = buckets(b, e -> e.offset === nothing ? "" : string(e.offset))
        for (off, same) in byoff
            length(same) > 1 && add!(findings, "SRF020", "error", "partition offset $(off) appears more than once", same, key = pk, action = "dedupe manifest")
        end
        ordered = sort([e for e in b if e.offset !== nothing], by = e -> (e.offset, e.row))
        last = nothing
        for e in ordered
            last !== nothing && e.offset - last - 1 > maxgap && push!(findings, Finding("SRF020", "error", "partition offset gap exceeds $(maxgap)", [e.row], pk, e.stream, e.partition, "backfill or advance checkpoint"))
            last = e.offset
        end
        arrival = sort(b, by = e -> (stampkey(e.arrival_time), e.row))
        hi_off, hi_time = nothing, nothing
        for e in arrival
            e.offset !== nothing && hi_off !== nothing && e.offset < hi_off && push!(findings, Finding("SRF020", "warning", "arrival order regresses offset", [e.row], pk, e.stream, e.partition, "run partition serially"))
            e.offset !== nothing && (hi_off = hi_off === nothing ? e.offset : max(hi_off, e.offset))
            if e.event_time !== nothing
                hi_time !== nothing && seconds(e.event_time, hi_time) > maxlag && push!(findings, Finding("SRF030", "error", "event-time watermark regresses beyond lag budget", [e.row], @sprintf("%.3f", seconds(e.event_time, hi_time)), e.stream, e.partition, "split late events from normal replay"))
                hi_time = hi_time === nothing ? e.event_time : max(hi_time, e.event_time)
            end
        end
    end
end

csvq(x) = "\"" * replace(string(x), "\"" => "\"\"") * "\""
jsonq(x) = replace(replace(replace(replace(replace(string(x), "\\" => "\\\\"), "\"" => "\\\""), "\n" => "\\n"), "\r" => "\\r"), "\t" => "\\t")

function writelines(path, lines)
    dir = dirname(abspath(path)); isdir(dir) || mkpath(dir)
    tmp = tempname(dir)
    open(tmp, "w") do io
        foreach(line -> println(io, line), lines)
    end
    mv(tmp, path; force = true)
end

function writecsv(path, header, rows)
    writelines(path, vcat([join(csvq.(header), ",")], [join(csvq.(r), ",") for r in rows]))
end

function writefindings(path, findings)
    rows = [[f.rule, f.severity, f.message, join(string.(f.rows), ";"), f.key, f.stream, f.partition, f.action] for f in findings]
    writecsv(path, ["rule_id", "severity", "message", "rows", "key", "stream", "partition", "recommendation"], rows)
end

function writesarif(path, findings, input)
    schema = "\$schema"
    rules = join(["{\"id\":\"$(jsonq(k))\",\"shortDescription\":{\"text\":\"$(jsonq(RULES[k]))\"}}" for k in sort(collect(keys(RULES)))], ",")
    results = join(["{\"ruleId\":\"$(jsonq(f.rule))\",\"level\":\"$(f.severity == "error" ? "error" : f.severity == "warning" ? "warning" : "note")\",\"message\":{\"text\":\"$(jsonq(f.message * (isempty(f.key) ? "" : " (key: $(f.key))")))\"},\"locations\":[{\"physicalLocation\":{\"artifactLocation\":{\"uri\":\"$(jsonq(input))\"},\"region\":{\"startLine\":$(isempty(f.rows) ? 1 : minimum(f.rows))}}}]}" for f in findings], ",")
    writelines(path, ["{\"$(schema)\":\"https://json.schemastore.org/sarif-2.1.0.json\",\"version\":\"2.1.0\",\"runs\":[{\"tool\":{\"driver\":{\"name\":\"StreamReplayFence\",\"informationUri\":\"https://github.com/kspavankrishna/VIBE-CODE\",\"rules\":[$(rules)]}},\"results\":[$(results)]}]}"])
end

function writeplan(path, events, findings, opts)
    cap = intopt(opts, "batch_bytes", min = 1); maxretry = intopt(opts, "max_retry_count")
    bad = Set{Int}()
    for f in findings
        f.severity == "error" && union!(bad, f.rows)
    end
    rows, bid = Vector{Vector{String}}(), 1
    for (_, b) in sort(collect(buckets(events, partkey)), by = x -> x[1])
        batch, bytes = Event[], 0
        for e in sort(b, by = e -> (e.offset === nothing ? typemax(Int) : e.offset, e.row))
            if !isempty(batch) && bytes + e.bytes > cap
                push!(rows, planrow(bid, batch, bad, maxretry)); bid += 1; empty!(batch); bytes = 0
            end
            push!(batch, e); bytes += e.bytes
        end
        isempty(batch) || (push!(rows, planrow(bid, batch, bad, maxretry)); bid += 1)
    end
    writecsv(path, ["batch_id", "stream", "partition", "from_offset", "to_offset", "events", "bytes", "blocking_errors", "recommended_action"], rows)
end

function planrow(id, b, bad, maxretry)
    offs = [e.offset for e in b if e.offset !== nothing]
    block = count(e -> e.row in bad, b)
    action = block > 0 ? "hold" : maximum([e.retry for e in b]) > maxretry ? "inspect" : "replay"
    return [string(id), b[1].stream, b[1].partition, isempty(offs) ? "" : string(minimum(offs)), isempty(offs) ? "" : string(maximum(offs)), string(length(b)), string(sum(e.bytes for e in b)), string(block), action]
end

function run(opts)
    events, findings = loadevents(opts["input"], opts)
    audit!(events, findings, opts)
    sort!(findings, by = f -> (-rank(f.severity), f.rule, isempty(f.rows) ? 0 : minimum(f.rows)))
    writefindings(opts["output"], findings); writesarif(opts["sarif"], findings, opts["input"]); writeplan(opts["plan"], events, findings, opts)
    @printf(stderr, "Audited %d replay rows: %d errors, %d warnings\n", length(events), count(f -> f.severity == "error", findings), count(f -> f.severity == "warning", findings))
    opts["fail_on"] == "none" && return 0
    threshold = opts["fail_on"] == "warning" ? 1 : 2
    return (isempty(findings) ? 0 : maximum(rank(f.severity) for f in findings)) >= threshold ? 2 : 0
end

function selftest()
    dir = mktempdir(); input = joinpath(dir, "events.csv")
    writelines(input, [
        "stream,partition,offset,event_id,idempotency_key,event_time,arrival_time,payload_hash,schema_version,region,operation,retry_count,byte_size",
        "orders,0,10,e1,k1,2026-04-01T10:00:00Z,2026-04-01T10:00:01Z,h1,v3,us-east-1,insert,0,100",
        "orders,0,10,e2,k2,2026-04-01T10:00:02Z,2026-04-01T10:00:03Z,h2,v3,us-east-1,insert,0,100",
        "orders,0,14,e3,k3,2026-04-01T09:30:00Z,2026-04-01T10:40:00Z,h3,v2,eu-west-1,insert,9,100",
        "orders,0,15,e4,k1,2026-04-01T10:00:05Z,2026-04-01T10:00:06Z,hx,v3,us-east-1,insert,1,100"
    ])
    opts = copy(DEFAULTS); opts["input"] = input; opts["output"] = joinpath(dir, "f.csv"); opts["sarif"] = joinpath(dir, "f.sarif"); opts["plan"] = joinpath(dir, "p.csv"); opts["expected_schema"] = "v3"; opts["allowed_regions"] = "us-east-1"; opts["fail_on"] = "none"
    code = run(opts); text = read(opts["output"], String)
    all(occursin(rule, text) for rule in ["SRF011", "SRF020", "SRF030", "SRF040", "SRF050", "SRF060"]) || error("self-test missed an expected rule")
    println("StreamReplayFence self-test passed"); return code
end

function main()
    try
        opts = parse_args(ARGS)
        return opts["self_test"] == "true" ? selftest() : run(opts)
    catch err
        println(stderr, "StreamReplayFence: ", sprint(showerror, err)); return 64
    end
end

exit(main())

#=
This solves the April 2026 event replay problem that keeps hurting data engineering, AI ingestion, RAG indexing, webhook delivery, CDC repair, and DevOps incident recovery: teams need to replay records after a bad deploy, schema migration, model eval rebuild, or streaming outage, but they do not know whether the manifest is safe. Built because a replay CSV can contain duplicate idempotency keys, offset gaps, late event-time rows, retry loops, mixed schemas, and wrong cloud regions while still looking normal in a spreadsheet. Use it when Kafka, Kinesis, Pub/Sub, Redpanda, SQS, webhook queues, feature pipelines, vector indexing jobs, or model evaluation backfills need a plain audit before workers start writing side effects again. The trick: it checks identity, ordering, watermark, retry, schema, residency, and batch planning in one dependency-free Julia command, then writes human CSV, SARIF for GitHub code scanning, and a replay plan that marks each partition batch as replay, inspect, or hold. Drop this into an incident repo, platform tools folder, CI job, release runbook, or data platform monorepo whenever you want searchable GitHub evidence for safe stream replay, idempotency validation, event ordering verification, data pipeline recovery, and production replay governance without sending customer payloads to another service.
=#
