module AffectedTargetPlanner

open System
open System.Collections.Generic
open System.Globalization
open System.IO
open System.Text
open System.Text.Json
open System.Text.RegularExpressions

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

exception PlanError of int * string

[<Literal>]
let ExitOk = 0

[<Literal>]
let ExitBadInput = 2

[<Literal>]
let ExitUnowned = 3

[<Literal>]
let ExitCycle = 4

[<Literal>]
let DefaultCostSeconds = 30.0

[<Literal>]
let DefaultMaxChanged = 50000

[<Literal>]
let GlobTimeoutSeconds = 1.0

type Target =
    { Name: string
      Srcs: string list
      Deps: string list
      Cost: float }

type Graph =
    { GlobalTriggers: string list
      Targets: Target list }

type UnownedPolicy =
    | FailOnUnowned
    | AffectAll
    | IgnoreUnowned

type Options =
    { GraphPath: string
      ChangedPath: string
      Shards: int
      Unowned: UnownedPolicy
      Json: bool
      NullSeparated: bool
      MaxChanged: int }

type Plan =
    { Mode: string
      Reasons: string list
      ChangedCount: int
      TotalTargets: int
      Direct: string list
      Affected: string list
      Unowned: string list
      MultiOwned: (string * string list) list
      Waves: (int * string list list) list
      CriticalSeconds: float
      CriticalPath: string list
      SerialSeconds: float
      PlannedSeconds: float }

let usage =
    String.Join(
        "\n",
        [ "AffectedTargetPlanner: decide which build targets a change touches, in what order and on which shard."
          ""
          "Usage:"
          "  AffectedTargetPlanner --graph graph.json --changed files.txt [options]"
          ""
          "Options:"
          "  --graph PATH       Target graph JSON (required)."
          "  --changed PATH     Changed file list, one repo relative path per line. Use - for stdin (required)."
          "  --null             Changed list is NUL separated, as produced by git diff -z."
          "  --shards N         Parallel shards per wave (default 1)."
          "  --unowned MODE     fail (default), all or ignore: what to do with files no target owns."
          "  --max-changed N    More changed files than this means run everything (default 50000)."
          "  --text             Print a human summary instead of JSON."
          "  -h, --help         Show this help."
          ""
          "Exit codes: 0 ok, 2 bad input, 3 unowned files under --unowned fail, 4 dependency cycle." ]
    )

// ---------------------------------------------------------------------------
// Paths and globs
// ---------------------------------------------------------------------------

let normalizePath (raw: string) : string =
    let p = raw.Trim().Replace('\\', '/')
    let p = if p.StartsWith("./", StringComparison.Ordinal) then p.Substring 2 else p
    let p = p.Replace("//", "/")
    p

let isBadPath (p: string) : string option =
    if p.Length = 0 then
        Some "empty path"
    elif p.StartsWith("\"", StringComparison.Ordinal) && p.EndsWith("\"", StringComparison.Ordinal) then
        Some "quoted path, rerun git with -z and pass --null"
    elif p.StartsWith("/", StringComparison.Ordinal) then
        Some "absolute path, expected a repo relative path"
    elif p.Split('/') |> Array.exists (fun s -> s = "..") then
        Some "path escapes the repo root"
    else
        None

let hasWildcard (s: string) = s.IndexOfAny([| '*'; '?' |]) >= 0

/// Turns a glob into an anchored regex. `**/` spans directories (including none),
/// `*` stays inside one segment, `?` is one non separator character. A pattern with
/// no wildcard owns that exact path and everything below it.
let globToRegex (pattern: string) : Regex =
    let pat = normalizePath pattern
    let pat = if pat.EndsWith("/", StringComparison.Ordinal) then pat + "**" else pat
    let sb = StringBuilder("^")
    if not (hasWildcard pat) then
        sb.Append(Regex.Escape(pat)).Append("(?:/.*)?") |> ignore
    else
        let mutable i = 0
        while i < pat.Length do
            let c = pat.[i]
            if c = '*' then
                let doubled = i + 1 < pat.Length && pat.[i + 1] = '*'
                if doubled && i + 2 < pat.Length && pat.[i + 2] = '/' then
                    sb.Append("(?:.*/)?") |> ignore
                    i <- i + 3
                elif doubled then
                    sb.Append(".*") |> ignore
                    i <- i + 2
                else
                    sb.Append("[^/]*") |> ignore
                    i <- i + 1
            elif c = '?' then
                sb.Append("[^/]") |> ignore
                i <- i + 1
            else
                sb.Append(Regex.Escape(string c)) |> ignore
                i <- i + 1
    sb.Append("$") |> ignore
    Regex(
        sb.ToString(),
        RegexOptions.CultureInvariant ||| RegexOptions.Singleline,
        TimeSpan.FromSeconds(GlobTimeoutSeconds)
    )

let matchesAny (regexes: Regex list) (path: string) : bool =
    try
        regexes |> List.exists (fun r -> r.IsMatch path)
    with :? RegexMatchTimeoutException ->
        raise (PlanError(ExitBadInput, sprintf "A glob timed out on '%s'. Simplify the pattern." path))

// ---------------------------------------------------------------------------
// Graph loading and validation
// ---------------------------------------------------------------------------

let readStrings (el: JsonElement) (prop: string) (where: string) : string list =
    match el.TryGetProperty(prop) with
    | true, v when v.ValueKind = JsonValueKind.Array ->
        [ for x in v.EnumerateArray() do
              if x.ValueKind <> JsonValueKind.String then
                  raise (PlanError(ExitBadInput, sprintf "%s: '%s' must contain only strings" where prop))
              yield x.GetString() ]
    | true, v when v.ValueKind = JsonValueKind.Null -> []
    | true, _ -> raise (PlanError(ExitBadInput, sprintf "%s: '%s' must be an array of strings" where prop))
    | false, _ -> []

let parseTarget (idx: int) (t: JsonElement) : Target =
    let where = sprintf "targets[%d]" idx
    if t.ValueKind <> JsonValueKind.Object then
        raise (PlanError(ExitBadInput, where + " must be an object"))
    let name =
        match t.TryGetProperty("name") with
        | true, el when el.ValueKind = JsonValueKind.String -> el.GetString().Trim()
        | _ -> ""
    if name.Length = 0 then
        raise (PlanError(ExitBadInput, where + " needs a non empty string 'name'"))
    let cost =
        match t.TryGetProperty("costSeconds") with
        | true, el when el.ValueKind = JsonValueKind.Number -> el.GetDouble()
        | true, el when el.ValueKind = JsonValueKind.Null -> DefaultCostSeconds
        | true, _ -> raise (PlanError(ExitBadInput, sprintf "%s (%s): costSeconds must be a number" where name))
        | false, _ -> DefaultCostSeconds
    if Double.IsNaN cost || Double.IsInfinity cost || cost < 0.0 then
        raise (PlanError(ExitBadInput, sprintf "%s (%s): costSeconds must be finite and not negative" where name))
    { Name = name
      Srcs = readStrings t "srcs" (where + " " + name)
      Deps = readStrings t "deps" (where + " " + name) |> List.map (fun d -> d.Trim()) |> List.distinct
      Cost = cost }

let parseGraph (json: string) : Graph =
    let opts = JsonDocumentOptions(CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true)
    use doc =
        try
            JsonDocument.Parse(json, opts)
        with :? JsonException as e ->
            raise (PlanError(ExitBadInput, "Graph is not valid JSON: " + e.Message))
    let root = doc.RootElement
    if root.ValueKind <> JsonValueKind.Object then
        raise (PlanError(ExitBadInput, "Graph root must be an object"))
    let triggers = readStrings root "globalTriggers" "graph"
    match root.TryGetProperty("targets") with
    | true, arr when arr.ValueKind = JsonValueKind.Array ->
        let targets = [ for (idx, t) in Seq.indexed (arr.EnumerateArray()) -> parseTarget idx t ]
        { GlobalTriggers = triggers
          Targets = targets }
    | _ -> raise (PlanError(ExitBadInput, "Graph needs a 'targets' array"))

/// Collects every structural problem at once so a broken graph is fixed in one pass.
let validateGraph (g: Graph) : unit =
    let errors = List<string>()
    let names = HashSet<string>(StringComparer.Ordinal)
    for t in g.Targets do
        if not (names.Add t.Name) then
            errors.Add(sprintf "duplicate target '%s'" t.Name)
    for t in g.Targets do
        for d in t.Deps do
            if d = t.Name then
                errors.Add(sprintf "target '%s' depends on itself" t.Name)
            elif not (names.Contains d) then
                errors.Add(sprintf "target '%s' depends on unknown target '%s'" t.Name d)
    if g.Targets.IsEmpty then
        errors.Add "graph has no targets"
    if errors.Count > 0 then
        raise (PlanError(ExitBadInput, "Invalid graph:\n  " + String.Join("\n  ", errors)))

// ---------------------------------------------------------------------------
// Ordering and cycles
// ---------------------------------------------------------------------------

/// Walks dependency edges inside the leftover set until a node repeats. Every leftover
/// node has a dependency that is also leftover, so the walk must close a loop.
let findCycle (deps: IDictionary<string, string list>) (leftover: HashSet<string>) : string list =
    let seen = Dictionary<string, int>(StringComparer.Ordinal)
    let path = List<string>()
    let rec walk (n: string) : string list =
        match seen.TryGetValue n with
        | true, idx -> (path.GetRange(idx, path.Count - idx) |> List.ofSeq) @ [ n ]
        | _ ->
            seen.[n] <- path.Count
            path.Add n
            let next = deps.[n] |> List.filter leftover.Contains |> List.sortWith (fun a b -> String.CompareOrdinal(a, b)) |> List.head
            walk next
    walk (leftover |> Seq.minBy id)

/// Kahn's algorithm with an ordinal sorted ready set, so output is identical on every run.
/// Dependencies always come before dependents. Raises ExitCycle with a concrete loop.
let topoOrder (g: Graph) : string list =
    let deps = Dictionary<string, string list>(StringComparer.Ordinal)
    let rdeps = Dictionary<string, List<string>>(StringComparer.Ordinal)
    for t in g.Targets do
        deps.[t.Name] <- t.Deps
        rdeps.[t.Name] <- List<string>()
    for t in g.Targets do
        for d in t.Deps do
            rdeps.[d].Add t.Name
    let remaining = Dictionary<string, int>(StringComparer.Ordinal)
    for t in g.Targets do
        remaining.[t.Name] <- t.Deps.Length
    let ready = SortedSet<string>(StringComparer.Ordinal)
    for kv in remaining do
        if kv.Value = 0 then
            ready.Add kv.Key |> ignore
    let order = List<string>()
    while ready.Count > 0 do
        let n = ready.Min
        ready.Remove n |> ignore
        order.Add n
        for r in rdeps.[n] do
            remaining.[r] <- remaining.[r] - 1
            if remaining.[r] = 0 then
                ready.Add r |> ignore
    if order.Count < g.Targets.Length then
        let leftover = HashSet<string>(StringComparer.Ordinal)
        for kv in remaining do
            if kv.Value > 0 then
                leftover.Add kv.Key |> ignore
        let cyc = findCycle deps leftover
        raise (PlanError(ExitCycle, "Dependency cycle: " + String.Join(" -> ", cyc)))
    List.ofSeq order

// ---------------------------------------------------------------------------
// Changed file handling
// ---------------------------------------------------------------------------

let parseChanged (text: string) (nullSeparated: bool) : string list =
    let parts =
        if nullSeparated then
            text.Split('\000')
        else
            text.Split([| '\n' |], StringSplitOptions.None)
    let problems = List<string>()
    let cleaned =
        [ for raw in parts do
              let p = normalizePath raw
              if p.Length > 0 then
                  match isBadPath p with
                  | Some why -> problems.Add(sprintf "'%s': %s" p why)
                  | None -> yield p ]
    if problems.Count > 0 then
        let shown = problems |> Seq.truncate 10
        raise (PlanError(ExitBadInput, "Bad changed file entries:\n  " + String.Join("\n  ", shown)))
    cleaned |> List.distinct

// ---------------------------------------------------------------------------
// Planning
// ---------------------------------------------------------------------------

let ordinal = StringComparer.Ordinal

let sortedNames (names: seq<string>) : string list =
    names |> Seq.sortWith (fun a b -> String.CompareOrdinal(a, b)) |> List.ofSeq

/// Longest-processing-time-first split of one wave over `shards` workers.
let assignShards (shards: int) (costs: IDictionary<string, float>) (wave: string list) : string list list * float =
    let loads = Array.zeroCreate<float> shards
    let bins = Array.init shards (fun _ -> List<string>())
    let ordered =
        wave
        |> List.sortWith (fun a b ->
            let c = compare costs.[b] costs.[a]
            if c <> 0 then c else String.CompareOrdinal(a, b))
    for name in ordered do
        let mutable best = 0
        for i in 1 .. shards - 1 do
            if loads.[i] < loads.[best] then
                best <- i
        bins.[best].Add name
        loads.[best] <- loads.[best] + costs.[name]
    let result = bins |> Array.map List.ofSeq |> List.ofArray |> List.filter (fun b -> not b.IsEmpty)
    result, (if loads.Length = 0 then 0.0 else Array.max loads)

let buildPlan (g: Graph) (changed: string list) (opts: Options) : Plan =
    let order = topoOrder g
    let byName = g.Targets |> List.map (fun t -> t.Name, t) |> dict
    let costs = g.Targets |> List.map (fun t -> t.Name, t.Cost) |> dict
    let triggerRx = g.GlobalTriggers |> List.map globToRegex
    let owners = g.Targets |> List.map (fun t -> t, t.Srcs |> List.map globToRegex)

    let reasons = List<string>()
    let direct = HashSet<string>(ordinal)
    let unowned = List<string>()
    let multi = List<string * string list>()
    let mutable everything = false

    if changed.Length > opts.MaxChanged then
        everything <- true
        reasons.Add(sprintf "%d changed files exceeds --max-changed %d, running everything" changed.Length opts.MaxChanged)

    for f in changed do
        if matchesAny triggerRx f then
            everything <- true
            reasons.Add(sprintf "global trigger matched %s" f)
        let hits = owners |> List.filter (fun (_, rx) -> matchesAny rx f) |> List.map (fun (t, _) -> t.Name)
        match hits with
        | [] -> unowned.Add f
        | [ one ] -> direct.Add one |> ignore
        | many ->
            for h in many do
                direct.Add h |> ignore
            multi.Add(f, sortedNames many)

    if unowned.Count > 0 then
        match opts.Unowned with
        | FailOnUnowned ->
            let shown = unowned |> Seq.truncate 20 |> String.concat "\n  "
            raise (
                PlanError(
                    ExitUnowned,
                    sprintf "%d changed file(s) belong to no target and no global trigger:\n  %s\nAdd them to a target, add a global trigger or rerun with --unowned all or ignore." unowned.Count shown
                )
            )
        | AffectAll ->
            everything <- true
            reasons.Add(sprintf "%d unowned file(s), policy is all" unowned.Count)
        | IgnoreUnowned ->
            reasons.Add(sprintf "%d unowned file(s) ignored by policy" unowned.Count)

    // Reverse transitive closure from the directly owned targets.
    let rdeps = Dictionary<string, List<string>>(ordinal)
    for t in g.Targets do
        rdeps.[t.Name] <- List<string>()
    for t in g.Targets do
        for d in t.Deps do
            rdeps.[d].Add t.Name
    let affected = HashSet<string>(ordinal)
    if everything then
        for t in g.Targets do
            affected.Add t.Name |> ignore
    else
        let q = Queue<string>()
        for d in direct do
            if affected.Add d then
                q.Enqueue d
        while q.Count > 0 do
            let n = q.Dequeue()
            for r in rdeps.[n] do
                if affected.Add r then
                    q.Enqueue r

    // Wave depth and critical path inside the affected subgraph, in dependency order.
    let level = Dictionary<string, int>(ordinal)
    let finish = Dictionary<string, float>(ordinal)
    let pred = Dictionary<string, string option>(ordinal)
    for n in order do
        if affected.Contains n then
            let t = byName.[n]
            let ds = t.Deps |> List.filter affected.Contains
            level.[n] <- (if ds.IsEmpty then 0 else 1 + (ds |> List.map (fun d -> level.[d]) |> List.max))
            match ds with
            | [] ->
                finish.[n] <- t.Cost
                pred.[n] <- None
            | _ ->
                let best = ds |> List.maxBy (fun d -> finish.[d])
                finish.[n] <- t.Cost + finish.[best]
                pred.[n] <- Some best

    let critical =
        if affected.Count = 0 then
            []
        else
            let tail = affected |> Seq.maxBy (fun n -> finish.[n], n)
            let rec back (n: string) (acc: string list) =
                match pred.[n] with
                | Some p -> back p (n :: acc)
                | None -> n :: acc
            back tail []

    let waveCount = if affected.Count = 0 then 0 else (level.Values |> Seq.max) + 1
    let wavePlans =
        [ for w in 0 .. waveCount - 1 do
              let members = affected |> Seq.filter (fun n -> level.[n] = w) |> sortedNames
              let bins, makespan = assignShards opts.Shards costs members
              yield (w, bins), makespan ]
    let waves = wavePlans |> List.map fst
    let planned = wavePlans |> List.sumBy snd

    let mode =
        if affected.Count = 0 then "none"
        elif everything then "all"
        else "affected"

    { Mode = mode
      Reasons = List.ofSeq reasons |> List.truncate 25
      ChangedCount = changed.Length
      TotalTargets = g.Targets.Length
      Direct = sortedNames direct
      Affected = sortedNames affected
      Unowned = List.ofSeq unowned
      MultiOwned = List.ofSeq multi
      Waves = waves
      CriticalSeconds = (if critical.IsEmpty then 0.0 else finish.[List.last critical])
      CriticalPath = critical
      SerialSeconds = affected |> Seq.sumBy (fun n -> costs.[n])
      PlannedSeconds = planned }

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

let r3 (x: float) = Math.Round(x, 3)

let writeStrings (w: Utf8JsonWriter) (name: string) (items: string list) =
    w.WriteStartArray(name)
    for i in items do
        w.WriteStringValue(i)
    w.WriteEndArray()

let renderJson (p: Plan) : string =
    use ms = new MemoryStream()
    do
        use w = new Utf8JsonWriter(ms, JsonWriterOptions(Indented = true))
        w.WriteStartObject()
        w.WriteNumber("schemaVersion", 1)
        w.WriteString("mode", p.Mode)
        writeStrings w "reasons" p.Reasons
        w.WriteNumber("changedFiles", p.ChangedCount)
        w.WriteNumber("totalTargets", p.TotalTargets)
        w.WriteNumber("affectedCount", p.Affected.Length)
        w.WriteNumber("skippedCount", p.TotalTargets - p.Affected.Length)
        writeStrings w "directlyChanged" p.Direct
        writeStrings w "affected" p.Affected
        writeStrings w "unownedFiles" p.Unowned
        w.WriteStartArray("multiOwnedFiles")
        for (f, os) in p.MultiOwned do
            w.WriteStartObject()
            w.WriteString("file", f)
            writeStrings w "owners" os
            w.WriteEndObject()
        w.WriteEndArray()
        w.WriteStartArray("waves")
        for (idx, bins) in p.Waves do
            w.WriteStartObject()
            w.WriteNumber("index", idx)
            w.WriteStartArray("shards")
            for b in bins do
                w.WriteStartArray()
                for n in b do
                    w.WriteStringValue(n)
                w.WriteEndArray()
            w.WriteEndArray()
            w.WriteEndObject()
        w.WriteEndArray()
        w.WriteStartObject("criticalPath")
        w.WriteNumber("seconds", r3 p.CriticalSeconds)
        writeStrings w "targets" p.CriticalPath
        w.WriteEndObject()
        w.WriteNumber("serialSeconds", r3 p.SerialSeconds)
        w.WriteNumber("plannedSeconds", r3 p.PlannedSeconds)
        w.WriteEndObject()
        w.Flush()
    Encoding.UTF8.GetString(ms.ToArray())

let renderText (p: Plan) : string =
    let sb = StringBuilder()
    let line (s: string) = sb.AppendLine(s) |> ignore
    line (sprintf "mode: %s" p.Mode)
    for r in p.Reasons do
        line (sprintf "reason: %s" r)
    line (sprintf "changed files: %d" p.ChangedCount)
    line (sprintf "affected: %d of %d targets (%d directly changed)" p.Affected.Length p.TotalTargets p.Direct.Length)
    for (f, os) in p.MultiOwned do
        line (sprintf "shared file %s owned by %s" f (String.Join(", ", os)))
    for (idx, bins) in p.Waves do
        line (sprintf "wave %d" idx)
        bins |> List.iteri (fun i b -> line (sprintf "  shard %d: %s" i (String.Join(", ", b))))
    line (sprintf "critical path: %.1fs via %s" p.CriticalSeconds (String.Join(" -> ", p.CriticalPath)))
    line (sprintf "serial: %.1fs, planned with shards: %.1fs" p.SerialSeconds p.PlannedSeconds)
    sb.ToString()

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

let parseIntIn (flag: string) (v: string) (lo: int) (hi: int) : int =
    match Int32.TryParse(v, NumberStyles.None, CultureInfo.InvariantCulture) with
    | true, n when n >= lo && n <= hi -> n
    | _ -> raise (PlanError(ExitBadInput, sprintf "%s must be an integer from %d to %d" flag lo hi))

let parsePolicy (v: string) : UnownedPolicy =
    match v with
    | "fail" -> FailOnUnowned
    | "all" -> AffectAll
    | "ignore" -> IgnoreUnowned
    | _ -> raise (PlanError(ExitBadInput, "--unowned must be fail, all or ignore"))

let rec parseArgs (o: Options) (args: string list) : Options =
    match args with
    | [] -> o
    | "--graph" :: v :: rest -> parseArgs { o with GraphPath = v } rest
    | "--changed" :: v :: rest -> parseArgs { o with ChangedPath = v } rest
    | "--null" :: rest -> parseArgs { o with NullSeparated = true } rest
    | "--text" :: rest -> parseArgs { o with Json = false } rest
    | "--shards" :: v :: rest -> parseArgs { o with Shards = parseIntIn "--shards" v 1 1024 } rest
    | "--max-changed" :: v :: rest -> parseArgs { o with MaxChanged = parseIntIn "--max-changed" v 1 Int32.MaxValue } rest
    | "--unowned" :: v :: rest -> parseArgs { o with Unowned = parsePolicy v } rest
    | ("-h" | "--help") :: _ -> raise (PlanError(ExitOk, usage))
    | a :: _ -> raise (PlanError(ExitBadInput, sprintf "Unknown or incomplete argument '%s'\n\n%s" a usage))

let readAllInput (path: string) : string =
    try
        if path = "-" then
            use reader = new StreamReader(Console.OpenStandardInput(), UTF8Encoding(false))
            reader.ReadToEnd()
        else
            File.ReadAllText(path, UTF8Encoding(false))
    with
    | :? IOException as e -> raise (PlanError(ExitBadInput, sprintf "Cannot read '%s': %s" path e.Message))
    | :? UnauthorizedAccessException as e -> raise (PlanError(ExitBadInput, sprintf "Cannot read '%s': %s" path e.Message))

let run (argv: string[]) : int =
    try
        let defaults =
            { GraphPath = ""
              ChangedPath = ""
              Shards = 1
              Unowned = FailOnUnowned
              Json = true
              NullSeparated = false
              MaxChanged = DefaultMaxChanged }
        let opts = parseArgs defaults (List.ofArray argv)
        if opts.GraphPath = "" || opts.ChangedPath = "" then
            raise (PlanError(ExitBadInput, "--graph and --changed are both required\n\n" + usage))
        if opts.GraphPath = "-" && opts.ChangedPath = "-" then
            raise (PlanError(ExitBadInput, "Only one input can come from stdin"))
        let graph = parseGraph (readAllInput opts.GraphPath)
        validateGraph graph
        let changed = parseChanged (readAllInput opts.ChangedPath) opts.NullSeparated
        let plan = buildPlan graph changed opts
        let out = if opts.Json then renderJson plan else renderText plan
        Console.Out.WriteLine(out)
        ExitOk
    with PlanError(code, msg) ->
        if code = ExitOk then Console.Out.WriteLine(msg) else Console.Error.WriteLine("error: " + msg)
        code

[<EntryPoint>]
let main argv = run argv
