module ExperimentEvidenceLedger

using Dates
using Printf
using Random
using SHA

export main, create_snapshot, verify_snapshot, diff_snapshots

const FORMAT_MAGIC = "EXPERIMENT_EVIDENCE_LEDGER"
const FORMAT_VERSION = "1"
const HASH_ALGORITHM = "sha256"
const TIMESTAMP_FORMAT = dateformat"yyyy-mm-ddTHH:MM:SS.sss"
const BUFFER_SIZE = 1024 * 1024
const DEFAULT_EXCLUSIONS = [
    ".git",
    ".hg",
    ".jj",
    ".svn",
    ".venv",
    "__pycache__",
    "node_modules",
]
const METADATA_FIELDS = Set([
    "algorithm",
    "root_label",
    "created_utc",
    "self_exclude",
    "entry_count",
    "evidence_root",
])

struct LedgerError <: Exception
    message::String
end

Base.showerror(io::IO, error::LedgerError) = print(io, error.message)

struct Entry
    kind::Char
    mode::UInt32
    size::Int64
    digest::String
    path::String
    target::String
end

struct Manifest
    root_label::String
    created_utc::String
    exclusions::Vector{String}
    self_exclude::Union{Nothing,String}
    entries::Vector{Entry}
    evidence_root::String
end

fail(message::AbstractString) = throw(LedgerError(String(message)))

function valid_digest(value::AbstractString)::Bool
    return occursin(r"^[0-9a-f]{64}$", value)
end

function require_digest(value::AbstractString, label::AbstractString)::String
    text = String(value)
    valid_digest(text) || fail("$(label) must be a lowercase 64-character SHA-256 digest")
    return text
end

function encode_field(value::AbstractString)::String
    io = IOBuffer()
    for byte in codeunits(String(value))
        if byte <= 0x1f || byte == 0x25 || byte == 0x7f
            @printf(io, "%%%02X", byte)
        else
            write(io, byte)
        end
    end
    return String(take!(io))
end

function decode_field(value::AbstractString)::String
    encoded = collect(codeunits(String(value)))
    decoded = UInt8[]
    index = 1
    while index <= length(encoded)
        byte = encoded[index]
        if byte == 0x25
            index + 2 <= length(encoded) || fail("manifest contains an incomplete percent escape")
            hex = String(encoded[index + 1:index + 2])
            parsed = tryparse(UInt8, hex; base = 16)
            isnothing(parsed) && fail("manifest contains an invalid percent escape: %$(hex)")
            push!(decoded, parsed::UInt8)
            index += 3
        else
            push!(decoded, byte)
            index += 1
        end
    end
    return String(decoded)
end

function validate_relative_path(value::AbstractString, label::AbstractString)::String
    path = String(value)
    isempty(path) && fail("$(label) cannot be empty")
    occursin('\0', path) && fail("$(label) contains a NUL byte")
    occursin('\\', path) && fail("$(label) must use '/' as its separator")
    startswith(path, "/") && fail("$(label) must be relative")
    parts = split(path, '/'; keepempty = true)
    any(part -> isempty(part) || part == "." || part == "..", parts) &&
        fail("$(label) is not a canonical relative path: $(path)")
    return path
end

function normalized_exclusion(value::AbstractString)::String
    raw = replace(String(value), '\\' => '/')
    return validate_relative_path(raw, "exclude prefix")
end

function normalized_exclusions(values::Vector{String})::Vector{String}
    prefixes = normalized_exclusion.(values)
    sort!(unique!(prefixes))
    return prefixes
end

function relative_if_inside(root::String, candidate::String)::Union{Nothing,String}
    relative = replace(relpath(candidate, root), '\\' => '/')
    if relative == ".." || startswith(relative, "../") || startswith(relative, "/")
        return nothing
    end
    return validate_relative_path(relative, "manifest path inside root")
end

function exact_omit_set(path::Union{Nothing,String})::Set{String}
    paths = Set{String}()
    isnothing(path) || push!(paths, path)
    return paths
end

function is_omitted(path::String, exclusions::Vector{String}, exact_omits::Set{String})::Bool
    path in exact_omits && return true
    for prefix in exclusions
        if path == prefix || startswith(path, prefix * "/")
            return true
        end
    end
    return false
end

function hash_bytes(bytes)::String
    return bytes2hex(SHA.sha256(bytes))
end

function hash_file_stably(path::String)::Tuple{Int64,String,UInt32}
    islink(path) && fail("path changed to a symbolic link before hashing: $(path)")
    before = stat(path)
    context = SHA.SHA2_256_CTX()
    total = Int64(0)
    buffer = Vector{UInt8}(undef, BUFFER_SIZE)
    open(path, "r") do io
        while !eof(io)
            count = readbytes!(io, buffer, length(buffer))
            count == 0 && break
            SHA.update!(context, @view buffer[1:count])
            total += Int64(count)
        end
    end
    after = stat(path)
    if islink(path) || before.size != after.size || before.mtime != after.mtime || total != after.size
        fail("file changed while it was being hashed: $(path)")
    end
    mode = UInt32(after.mode) & UInt32(0o777)
    return (total, bytes2hex(SHA.digest!(context)), mode)
end

function scan_entries(root::String, exclusions::Vector{String}, exact_omits::Set{String})::Vector{Entry}
    isdir(root) || fail("snapshot root is not a directory: $(root)")
    entries = Entry[]

    function visit(directory::String, relative_directory::String)
        names = sort!(readdir(directory))
        for name in names
            relative = isempty(relative_directory) ? name : relative_directory * "/" * name
            is_omitted(relative, exclusions, exact_omits) && continue
            full_path = joinpath(directory, name)
            if islink(full_path)
                target = String(readlink(full_path))
                digest = hash_bytes(codeunits(target))
                push!(entries, Entry('L', UInt32(0), Int64(ncodeunits(target)), digest, relative, target))
            elseif isdir(full_path)
                visit(full_path, relative)
            elseif isfile(full_path)
                size, digest, mode = hash_file_stably(full_path)
                push!(entries, Entry('F', mode, size, digest, relative, ""))
            else
                fail("unsupported filesystem object encountered: $(relative)")
            end
        end
    end

    visit(root, "")
    sort!(entries; by = entry -> entry.path)
    paths = [entry.path for entry in entries]
    length(paths) == length(unique(paths)) || fail("scanner produced duplicate paths")
    return entries
end

function absorb!(context, value::AbstractString)
    SHA.update!(context, codeunits(String(value)))
    SHA.update!(context, UInt8[0x00])
end

function calculate_evidence_root(
    root_label::String,
    created_utc::String,
    exclusions::Vector{String},
    self_exclude::Union{Nothing,String},
    entries::Vector{Entry},
)::String
    context = SHA.SHA2_256_CTX()
    absorb!(context, FORMAT_MAGIC)
    absorb!(context, FORMAT_VERSION)
    absorb!(context, HASH_ALGORITHM)
    absorb!(context, root_label)
    absorb!(context, created_utc)
    absorb!(context, isnothing(self_exclude) ? "" : self_exclude)
    for exclusion in exclusions
        absorb!(context, "exclude")
        absorb!(context, exclusion)
    end
    for entry in entries
        absorb!(context, "entry")
        absorb!(context, string(entry.kind))
        absorb!(context, string(entry.mode; base = 8, pad = 3))
        absorb!(context, string(entry.size))
        absorb!(context, entry.digest)
        absorb!(context, entry.path)
        absorb!(context, entry.target)
    end
    return bytes2hex(SHA.digest!(context))
end

function materialize_manifest(
    root_label::String,
    created_utc::String,
    exclusions::Vector{String},
    self_exclude::Union{Nothing,String},
    entries::Vector{Entry},
)::Manifest
    evidence_root = calculate_evidence_root(root_label, created_utc, exclusions, self_exclude, entries)
    return Manifest(root_label, created_utc, exclusions, self_exclude, entries, evidence_root)
end

function write_manifest_atomically(path::String, manifest::Manifest; force::Bool = false)
    destination = normpath(abspath(path))
    ispath(destination) && !force && fail("manifest already exists; pass --force to replace it: $(destination)")
    directory = dirname(destination)
    isdir(directory) || mkpath(directory)
    temporary = joinpath(directory, ".$(basename(destination)).tmp.$(randstring(12))")
    try
        open(temporary, "w") do io
            println(io, FORMAT_MAGIC, '\t', FORMAT_VERSION)
            println(io, "algorithm\t", HASH_ALGORITHM)
            println(io, "root_label\t", encode_field(manifest.root_label))
            println(io, "created_utc\t", manifest.created_utc)
            println(io, "self_exclude\t", isnothing(manifest.self_exclude) ? "" : encode_field(manifest.self_exclude))
            for exclusion in manifest.exclusions
                println(io, "exclude\t", encode_field(exclusion))
            end
            println(io, "entry_count\t", length(manifest.entries))
            println(io, "evidence_root\t", manifest.evidence_root)
            for entry in manifest.entries
                println(
                    io,
                    "entry\t",
                    entry.kind,
                    '\t',
                    string(entry.mode; base = 8, pad = 3),
                    '\t',
                    entry.size,
                    '\t',
                    entry.digest,
                    '\t',
                    encode_field(entry.path),
                    '\t',
                    encode_field(entry.target),
                )
            end
            flush(io)
        end
        mv(temporary, destination; force = force)
    finally
        isfile(temporary) && rm(temporary; force = true)
    end
end

function store_once!(metadata::Dict{String,String}, key::String, value::String)
    haskey(metadata, key) && fail("manifest repeats metadata field: $(key)")
    metadata[key] = value
end

function require_metadata(metadata::Dict{String,String}, key::String)::String
    haskey(metadata, key) || fail("manifest is missing metadata field: $(key)")
    return metadata[key]
end

function read_manifest(path::String)::Manifest
    source = normpath(abspath(path))
    isfile(source) || fail("manifest does not exist: $(source)")
    lines = readlines(source)
    isempty(lines) && fail("manifest is empty")
    header = split(lines[1], '\t'; keepempty = true)
    header == [FORMAT_MAGIC, FORMAT_VERSION] || fail("unsupported manifest format or version")

    metadata = Dict{String,String}()
    exclusions = String[]
    entries = Entry[]
    for line in lines[2:end]
        isempty(line) && fail("manifest contains an empty line")
        fields = split(line, '\t'; keepempty = true)
        key = fields[1]
        if key == "exclude"
            length(fields) == 2 || fail("invalid exclude line in manifest")
            push!(exclusions, normalized_exclusion(decode_field(fields[2])))
        elseif key == "entry"
            length(fields) == 7 || fail("invalid entry line in manifest")
            kind_text = fields[2]
            ncodeunits(kind_text) == 1 || fail("invalid entry type")
            kind = only(kind_text)
            (kind == 'F' || kind == 'L') || fail("unknown entry type: $(kind)")
            mode = tryparse(UInt32, fields[3]; base = 8)
            isnothing(mode) && fail("invalid permission mode for entry")
            mode > UInt32(0o777) && fail("entry permission mode has unsupported bits")
            size = tryparse(Int64, fields[4])
            (isnothing(size) || size < 0) && fail("invalid size for entry")
            digest = require_digest(fields[5], "entry digest")
            entry_path = validate_relative_path(decode_field(fields[6]), "entry path")
            target = decode_field(fields[7])
            kind == 'F' && !isempty(target) && fail("regular file entry has a link target")
            kind == 'L' && mode != UInt32(0) && fail("symbolic link entry has file permissions")
            push!(entries, Entry(kind, mode::UInt32, size::Int64, digest, entry_path, target))
        else
            key in METADATA_FIELDS || fail("unknown metadata field in manifest: $(key)")
            length(fields) == 2 || fail("invalid metadata line in manifest")
            store_once!(metadata, key, fields[2])
        end
    end

    require_metadata(metadata, "algorithm") == HASH_ALGORITHM || fail("manifest hash algorithm is unsupported")
    root_label = decode_field(require_metadata(metadata, "root_label"))
    isempty(root_label) && fail("manifest root label cannot be empty")
    created_utc = require_metadata(metadata, "created_utc")
    endswith(created_utc, "Z") || fail("manifest creation timestamp must use a UTC Z suffix")
    tryparse(DateTime, chop(created_utc; tail = 1), TIMESTAMP_FORMAT) === nothing &&
        fail("manifest creation timestamp is invalid")
    encoded_self = require_metadata(metadata, "self_exclude")
    self_exclude = isempty(encoded_self) ? nothing : validate_relative_path(decode_field(encoded_self), "self exclusion")
    expected_count = tryparse(Int, require_metadata(metadata, "entry_count"))
    (isnothing(expected_count) || expected_count < 0) && fail("manifest entry count is invalid")
    expected_count == length(entries) || fail("manifest entry count does not match its entries")
    evidence_root = require_digest(require_metadata(metadata, "evidence_root"), "evidence root")

    normalized = normalized_exclusions(exclusions)
    normalized == exclusions || fail("manifest exclusions must be sorted and unique")
    paths = [entry.path for entry in entries]
    paths == sort(paths) || fail("manifest entries must be sorted by path")
    length(paths) == length(unique(paths)) || fail("manifest contains duplicate entries")
    calculated = calculate_evidence_root(root_label, created_utc, normalized, self_exclude, entries)
    calculated == evidence_root || fail("manifest evidence root does not match its content")
    return Manifest(root_label, created_utc, normalized, self_exclude, entries, evidence_root)
end

function create_snapshot(
    root_path::String,
    manifest_path::String;
    label::Union{Nothing,String} = nothing,
    exclusions::Vector{String} = copy(DEFAULT_EXCLUSIONS),
    force::Bool = false,
)::Manifest
    root = normpath(abspath(root_path))
    isdir(root) || fail("snapshot root is not a directory: $(root)")
    output = normpath(abspath(manifest_path))
    output == root && fail("manifest path cannot be the snapshot root directory")
    normalized = normalized_exclusions(exclusions)
    self_exclude = relative_if_inside(root, output)
    exact_omits = exact_omit_set(self_exclude)
    root_label = isnothing(label) ? basename(root) : String(label)
    isempty(root_label) && fail("root label cannot be empty")
    created_utc = Dates.format(now(UTC), TIMESTAMP_FORMAT) * "Z"
    entries = scan_entries(root, normalized, exact_omits)
    manifest = materialize_manifest(root_label, created_utc, normalized, self_exclude, entries)
    write_manifest_atomically(output, manifest; force = force)
    return manifest
end

function constant_time_equal(left::AbstractString, right::AbstractString)::Bool
    a = collect(codeunits(String(left)))
    b = collect(codeunits(String(right)))
    length(a) == length(b) || return false
    difference = UInt8(0)
    for index in eachindex(a)
        difference |= xor(a[index], b[index])
    end
    return difference == UInt8(0)
end

function github_property(value::String)::String
    return replace(value, "%" => "%25", "\r" => "%0D", "\n" => "%0A", ":" => "%3A", "," => "%2C")
end

function github_message(value::String)::String
    return replace(value, "%" => "%25", "\r" => "%0D", "\n" => "%0A")
end

function report_problem(path::String, message::String; github_actions::Bool = false)
    if github_actions
        println("::error file=$(github_property(path))::$(github_message(message))")
    else
        println(stderr, "ERROR: $(path): $(message)")
    end
end

function entries_by_path(entries::Vector{Entry})::Dict{String,Entry}
    return Dict(entry.path => entry for entry in entries)
end

function entry_equal(left::Entry, right::Entry)::Bool
    return left.kind == right.kind && left.mode == right.mode && left.size == right.size &&
           left.digest == right.digest && left.target == right.target
end

function entry_change_message(expected::Entry, actual::Entry)::String
    if expected.kind != actual.kind
        return "type changed from $(expected.kind) to $(actual.kind)"
    elseif expected.digest != actual.digest
        return "content digest changed from $(expected.digest) to $(actual.digest)"
    elseif expected.mode != actual.mode
        before = string(expected.mode; base = 8, pad = 3)
        after = string(actual.mode; base = 8, pad = 3)
        return "permission mode changed from $(before) to $(after)"
    elseif expected.size != actual.size
        return "size changed from $(expected.size) to $(actual.size) bytes"
    else
        return "symbolic link target changed"
    end
end

function compare_entries(expected::Vector{Entry}, actual::Vector{Entry})
    expected_map = entries_by_path(expected)
    actual_map = entries_by_path(actual)
    missing = sort!(collect(setdiff(keys(expected_map), keys(actual_map))))
    unexpected = sort!(collect(setdiff(keys(actual_map), keys(expected_map))))
    changed = String[]
    for path in sort!(collect(intersect(keys(expected_map), keys(actual_map))))
        entry_equal(expected_map[path], actual_map[path]) || push!(changed, path)
    end
    return (expected_map, actual_map, missing, unexpected, changed)
end

function verify_snapshot(
    manifest_path::String,
    root_path::String;
    expect_root::Union{Nothing,String} = nothing,
    allow_extra::Bool = false,
    github_actions::Bool = false,
    quiet::Bool = false,
)::Bool
    manifest = read_manifest(manifest_path)
    if !isnothing(expect_root)
        expected_root = require_digest(expect_root, "expected root")
        if !constant_time_equal(expected_root, manifest.evidence_root)
            report_problem(manifest_path, "pinned evidence root does not match manifest"; github_actions = github_actions)
            return false
        end
    end
    root = normpath(abspath(root_path))
    exact_omits = exact_omit_set(manifest.self_exclude)
    current = scan_entries(root, manifest.exclusions, exact_omits)
    expected_map, actual_map, missing, unexpected, changed = compare_entries(manifest.entries, current)
    for path in missing
        report_problem(path, "file recorded in evidence ledger is missing"; github_actions = github_actions)
    end
    for path in changed
        message = entry_change_message(expected_map[path], actual_map[path])
        report_problem(path, message; github_actions = github_actions)
    end
    if !allow_extra
        for path in unexpected
            report_problem(path, "unrecorded file exists under snapshot root"; github_actions = github_actions)
        end
    elseif !quiet
        for path in unexpected
            println("ALLOWED EXTRA: $(path)")
        end
    end
    verified = isempty(missing) && isempty(changed) && (allow_extra || isempty(unexpected))
    if verified && !quiet
        println("Verified $(length(manifest.entries)) entries; evidence root $(manifest.evidence_root)")
    end
    return verified
end

function diff_snapshots(left_path::String, right_path::String; quiet::Bool = false)::Bool
    left = read_manifest(left_path)
    right = read_manifest(right_path)
    left_map, right_map, removed, added, changed = compare_entries(left.entries, right.entries)
    policy_changed = left.exclusions != right.exclusions || left.self_exclude != right.self_exclude
    identical = isempty(removed) && isempty(added) && isempty(changed) && !policy_changed
    if policy_changed
        println("POLICY CHANGED: exclusions or embedded-manifest handling differs")
    end
    for path in added
        println("ADDED: $(path) [$(right_map[path].digest)]")
    end
    for path in removed
        println("REMOVED: $(path) [$(left_map[path].digest)]")
    end
    for path in changed
        println("CHANGED: $(path): $(entry_change_message(left_map[path], right_map[path]))")
    end
    if identical && !quiet
        println("No artifact differences; evidence root $(right.evidence_root)")
    elseif !quiet
        println("Before root: $(left.evidence_root)")
        println("After root:  $(right.evidence_root)")
    end
    return identical
end

function print_usage(io::IO = stdout)
    println(io, "ExperimentEvidenceLedger.jl - deterministic evidence for research and AI artifacts")
    println(io)
    println(io, "Usage:")
    println(io, "  julia ExperimentEvidenceLedger.jl snapshot ROOT MANIFEST [options]")
    println(io, "  julia ExperimentEvidenceLedger.jl verify MANIFEST ROOT [options]")
    println(io, "  julia ExperimentEvidenceLedger.jl diff OLD_MANIFEST NEW_MANIFEST [--quiet]")
    println(io)
    println(io, "Snapshot options:")
    println(io, "  --exclude PREFIX          Omit a relative path prefix; repeat as needed")
    println(io, "  --no-default-exclusions   Include dependency and VCS directories unless excluded")
    println(io, "  --label TEXT              Stable human name for the artifact set")
    println(io, "  --force                   Atomically replace an existing manifest")
    println(io, "  --quiet                   Do not print the created root")
    println(io)
    println(io, "Verify options:")
    println(io, "  --expect-root SHA256      Require a root pinned in CI, a release, or a paper")
    println(io, "  --allow-extra             Permit files that were not in the snapshot")
    println(io, "  --github-actions          Emit workflow annotation errors")
    println(io, "  --quiet                   Print errors only")
end

function take_value(args::Vector{String}, index::Int, flag::String)::Tuple{String,Int}
    index < length(args) || fail("$(flag) requires a value")
    return (args[index + 1], index + 2)
end

function run_snapshot(args::Vector{String})::Int
    length(args) >= 2 || fail("snapshot requires ROOT and MANIFEST")
    root, output = args[1], args[2]
    user_exclusions = String[]
    use_defaults = true
    label = nothing
    force = false
    quiet = false
    index = 3
    while index <= length(args)
        flag = args[index]
        if flag == "--exclude"
            value, index = take_value(args, index, flag)
            push!(user_exclusions, value)
        elseif flag == "--no-default-exclusions"
            use_defaults = false
            index += 1
        elseif flag == "--label"
            value, index = take_value(args, index, flag)
            label = value
        elseif flag == "--force"
            force = true
            index += 1
        elseif flag == "--quiet"
            quiet = true
            index += 1
        else
            fail("unknown snapshot option: $(flag)")
        end
    end
    exclusions = use_defaults ? vcat(DEFAULT_EXCLUSIONS, user_exclusions) : user_exclusions
    manifest = create_snapshot(root, output; label = label, exclusions = exclusions, force = force)
    if !quiet
        println("Recorded $(length(manifest.entries)) entries; evidence root $(manifest.evidence_root)")
    end
    return 0
end

function run_verify(args::Vector{String})::Int
    length(args) >= 2 || fail("verify requires MANIFEST and ROOT")
    manifest, root = args[1], args[2]
    expect_root = nothing
    allow_extra = false
    github_actions = false
    quiet = false
    index = 3
    while index <= length(args)
        flag = args[index]
        if flag == "--expect-root"
            value, index = take_value(args, index, flag)
            expect_root = value
        elseif flag == "--allow-extra"
            allow_extra = true
            index += 1
        elseif flag == "--github-actions"
            github_actions = true
            index += 1
        elseif flag == "--quiet"
            quiet = true
            index += 1
        else
            fail("unknown verify option: $(flag)")
        end
    end
    ok = verify_snapshot(
        manifest,
        root;
        expect_root = expect_root,
        allow_extra = allow_extra,
        github_actions = github_actions,
        quiet = quiet,
    )
    return ok ? 0 : 1
end

function run_diff(args::Vector{String})::Int
    length(args) >= 2 || fail("diff requires OLD_MANIFEST and NEW_MANIFEST")
    length(args) <= 3 || fail("diff accepts only --quiet after its manifest paths")
    quiet = length(args) == 3 && args[3] == "--quiet"
    length(args) == 3 && !quiet && fail("unknown diff option: $(args[3])")
    return diff_snapshots(args[1], args[2]; quiet = quiet) ? 0 : 1
end

function main(args::Vector{String} = copy(ARGS))::Int
    isempty(args) && (print_usage(stderr); return 2)
    command = popfirst!(args)
    if command == "--help" || command == "-h" || command == "help"
        print_usage()
        return 0
    end
    try
        if command == "snapshot"
            return run_snapshot(args)
        elseif command == "verify"
            return run_verify(args)
        elseif command == "diff"
            return run_diff(args)
        else
            fail("unknown command: $(command)")
        end
    catch error
        if error isa LedgerError
            println(stderr, "error: ", error.message)
        else
            println(stderr, "error: ", sprint(showerror, error))
        end
        return 2
    end
end

end # module ExperimentEvidenceLedger

if abspath(PROGRAM_FILE) == @__FILE__
    exit(ExperimentEvidenceLedger.main())
end

#=
This solves the boring but serious problem of proving exactly which model weights, prompt fixtures, evaluation sets, retrieval indexes, generated reports, and research inputs belonged to a run. Built because I kept seeing experiments reported from a directory that changed quietly between the successful run and the review, leaving nobody able to reproduce the result or explain a regression. Use it when a Julia, Python, or mixed-language pipeline produces artifacts that must be pinned in GitHub Actions, a release bundle, an internal benchmark, or a paper appendix. The trick: it hashes files as a sorted ledger, records executable permission changes and symbolic links without following them, includes the exclusion policy in the evidence root, and lets CI compare that root with a value stored somewhere reviewers can see. Drop this into a tools directory or an artifact-producing repository, run `snapshot` after an approved run, pin the printed SHA-256 evidence root in CI or release notes, and run `verify` before publishing or promoting outputs. I wrote the manifest format to be readable in a pull request, atomic on write, free of package installation surprises, and strict about changed files, extra files, path traversal, and altered policy. Searching for reproducible AI evaluation artifacts, machine learning dataset integrity, SHA-256 experiment provenance, Julia research reproducibility, or CI artifact verification should lead to a practical file that can actually guard a real result instead of another checklist nobody runs.
=#
