module LogTemplateMiner

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

[<Literal>]
let Wildcard = "<*>"

type Level =
    | Info
    | Warn
    | Severe

type Config =
    { Depth: int
      SimThreshold: float
      MaxChildren: int
      MaxClusters: int
      MaxTokens: int
      MaxLineChars: int
      Fold: bool
      Json: bool }

let defaultConfig =
    { Depth = 4
      SimThreshold = 0.5
      MaxChildren = 64
      MaxClusters = 10000
      MaxTokens = 128
      MaxLineChars = 8192
      Fold = true
      Json = true }

[<AllowNullLiteral>]
type Cluster(id: int, template: string[], line: int64, sample: string) =
    member val Id = id
    member val Template = template with get, set
    member val Count = 1L with get, set
    member val FirstLine = line
    member val LastLine = line with get, set
    member val Errors = 0L with get, set
    member val Warns = 0L with get, set
    member val Folded = 0L with get, set
    member val Hint: string = null with get, set
    member val Sample = sample
    member val Evicted = false with get, set

type Node() =
    member val Children = Dictionary<string, Node>(StringComparer.Ordinal)
    member val Clusters = ResizeArray<Cluster>()

type Stats =
    { Lines: int64
      Records: int64
      Blank: int64
      Folded: int64
      JsonFallbacks: int64
      MaskTimeouts: int64
      EvictedClusters: int64
      EvictedRecords: int64
      LiveClusters: int }

// ---------------------------------------------------------------------------
// Masking: volatile values become typed placeholders before tokenising.
// Every regex has a match timeout so a hostile line cannot stall the pipeline.
// ---------------------------------------------------------------------------

let private mkRegex (pattern: string) =
    Regex(pattern, RegexOptions.CultureInvariant ||| RegexOptions.Compiled, TimeSpan.FromMilliseconds 50.0)

let builtInMasks: (Regex * string) list =
    [ mkRegex @"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?", "<TS>"
      mkRegex @"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b", "<UUID>"
      mkRegex @"[\w.+-]+@[\w-]+(?:\.[\w-]+)+", "<EMAIL>"
      mkRegex @"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?\b", "<IP>"
      mkRegex @"\b0x[0-9a-fA-F]+\b", "<HEX>"
      mkRegex @"\b[0-9a-fA-F]{12,}\b", "<HEX>"
      mkRegex @"(?<![\w.])\d+(?:\.\d+)?(?:ms|us|ns|s|m|h|[KMG]i?B|B|%)?(?!\w)", "<NUM>" ]

let private isCont (line: string) =
    line.Length > 0
    && (line[0] = ' '
        || line[0] = '\t'
        || line.StartsWith("Caused by", StringComparison.Ordinal)
        || line.StartsWith("Traceback (most recent call last)", StringComparison.Ordinal)
        || line.StartsWith("Suppressed:", StringComparison.Ordinal))

let private hasDigit (t: string) =
    let mutable found = false
    let mutable i = 0
    while not found && i < t.Length do
        if Char.IsDigit t[i] then found <- true
        i <- i + 1
    found

let private isPlaceholder (t: string) =
    t.Length > 1 && t[0] = '<' && t[t.Length - 1] = '>'

/// Tokens that look like data (digits, placeholders, blobs) share one tree branch.
let private keyOf (t: string) =
    if isPlaceholder t || hasDigit t || t.Length > 64 then Wildcard else t

let parseLevel (s: string) =
    match s.ToUpperInvariant() with
    | "ERROR" | "ERR" | "FATAL" | "CRITICAL" | "PANIC" | "SEVERE" -> Severe
    | "WARN" | "WARNING" -> Warn
    | _ -> Info

let private levelTrim = [| '['; ']'; ':'; '<'; '>'; '('; ')' |]

let private detectLevel (tokens: string[]) =
    let mutable lvl = Info
    let mutable i = 0
    while lvl = Info && i < min 8 tokens.Length do
        lvl <- parseLevel (tokens[i].Trim(levelTrim))
        i <- i + 1
    lvl

let private msgKeys = [| "msg"; "message"; "log"; "body"; "text"; "event" |]
let private lvlKeys = [| "level"; "severity"; "lvl"; "levelname" |]

/// Pulls the message and level out of one JSON log line. None means "treat as raw text".
let extractJson (line: string) : (string * Level) option =
    try
        use doc = JsonDocument.Parse(line, JsonDocumentOptions(MaxDepth = 16))
        let root = doc.RootElement
        if root.ValueKind <> JsonValueKind.Object then
            None
        else
            let find (keys: string[]) =
                keys
                |> Array.tryPick (fun k ->
                    match root.TryGetProperty(k) with
                    | true, v -> Some v
                    | _ -> None)
            match find msgKeys with
            | None -> None
            | Some m ->
                let text =
                    if m.ValueKind = JsonValueKind.String then m.GetString() else m.GetRawText()
                let lvl =
                    match find lvlKeys with
                    | Some l when l.ValueKind = JsonValueKind.String -> parseLevel (l.GetString())
                    | _ -> Info
                if isNull text then None else Some(text, lvl)
    with :? JsonException ->
        None

// ---------------------------------------------------------------------------
// The miner: a fixed depth prefix tree (Drain style) with bounded memory.
// Depth 1 is the token count, the next Depth-2 levels are leading tokens and
// the leaf holds clusters that are compared by token overlap.
// ---------------------------------------------------------------------------

type Miner(cfg: Config, extraMasks: (Regex * string) list) =
    let masks = extraMasks @ builtInMasks
    let roots = Dictionary<int, Node>()
    let live = Dictionary<int, Cluster>()
    let separators = [| ' '; '\t' |]
    let mutable nextId = 1
    let mutable lines = 0L
    let mutable records = 0L
    let mutable blank = 0L
    let mutable folded = 0L
    let mutable jsonFallbacks = 0L
    let mutable maskTimeouts = 0L
    let mutable evictedClusters = 0L
    let mutable evictedRecords = 0L
    let mutable last: Cluster = null

    let applyMasks (s: string) =
        let mutable cur = s
        for (rx, rep) in masks do
            try
                cur <- rx.Replace(cur, rep)
            with :? RegexMatchTimeoutException ->
                maskTimeouts <- maskTimeouts + 1L
        cur

    let leafFor (tokens: string[]) : Node =
        let len = tokens.Length
        let mutable node =
            match roots.TryGetValue len with
            | true, n -> n
            | _ ->
                let n = Node()
                roots[len] <- n
                n
        for i in 0 .. (min (cfg.Depth - 2) len) - 1 do
            let key = keyOf tokens[i]
            let next =
                match node.Children.TryGetValue key with
                | true, c -> c
                | _ ->
                    let wild = if node.Children.ContainsKey Wildcard then 1 else 0
                    let nonWild = node.Children.Count - wild
                    let useKey = key <> Wildcard && nonWild < cfg.MaxChildren - 1
                    let k = if useKey then key else Wildcard
                    match node.Children.TryGetValue k with
                    | true, c -> c
                    | _ ->
                        let c = Node()
                        node.Children[k] <- c
                        c
            node <- next
        node

    /// Wildcard positions are skipped so a fully generalised template cannot absorb everything.
    let similarity (template: string[]) (tokens: string[]) =
        let mutable same = 0
        for i in 0 .. tokens.Length - 1 do
            if template[i] <> Wildcard && String.Equals(template[i], tokens[i], StringComparison.Ordinal) then
                same <- same + 1
        float same / float tokens.Length

    /// Evicts the least recently seen 10 percent plus any overflow, in one pass.
    let evictIfNeeded () =
        if live.Count > cfg.MaxClusters then
            let take = (live.Count - cfg.MaxClusters) + max 1 (cfg.MaxClusters / 10)
            let victims =
                live.Values |> Seq.sortBy (fun c -> c.LastLine) |> Seq.truncate take |> Seq.toArray
            for v in victims do
                v.Evicted <- true
                live.Remove(v.Id) |> ignore
                evictedClusters <- evictedClusters + 1L
                evictedRecords <- evictedRecords + v.Count

    member _.Config = cfg

    member _.Clusters: Cluster[] = live.Values |> Seq.toArray

    member _.Stats: Stats =
        { Lines = lines
          Records = records
          Blank = blank
          Folded = folded
          JsonFallbacks = jsonFallbacks
          MaskTimeouts = maskTimeouts
          EvictedClusters = evictedClusters
          EvictedRecords = evictedRecords
          LiveClusters = live.Count }

    /// Adds one message. Returns the cluster id, or 0 when the message had no tokens.
    member _.Add(message: string, level: Level option) : int =
        records <- records + 1L
        let masked = applyMasks message
        let raw = masked.Split(separators, StringSplitOptions.RemoveEmptyEntries)
        if raw.Length = 0 then
            blank <- blank + 1L
            last <- null
            0
        else
            let tokens = if raw.Length > cfg.MaxTokens then Array.sub raw 0 cfg.MaxTokens else raw
            let lvl =
                match level with
                | Some l -> l
                | None -> detectLevel tokens
            let leaf = leafFor tokens
            let mutable best: Cluster = null
            let mutable bestSim = -1.0
            let mutable stale = false
            for c in leaf.Clusters do
                if c.Evicted then
                    stale <- true
                else
                    let s = similarity c.Template tokens
                    if s > bestSim || (s = bestSim && c.Count > best.Count) then
                        best <- c
                        bestSim <- s
            if stale then leaf.Clusters.RemoveAll(fun c -> c.Evicted) |> ignore
            let target =
                if not (isNull best) && bestSim >= cfg.SimThreshold then
                    let t = best.Template
                    for i in 0 .. t.Length - 1 do
                        if t[i] <> tokens[i] then t[i] <- Wildcard
                    best.Count <- best.Count + 1L
                    best.LastLine <- records
                    best
                else
                    let sample = if message.Length > 240 then message.Substring(0, 240) else message
                    let c = Cluster(nextId, Array.copy tokens, records, sample)
                    nextId <- nextId + 1
                    leaf.Clusters.Add c
                    live[c.Id] <- c
                    evictIfNeeded ()
                    c
            match lvl with
            | Severe -> target.Errors <- target.Errors + 1L
            | Warn -> target.Warns <- target.Warns + 1L
            | Info -> ()
            last <- target
            target.Id

    /// Feeds one physical input line: folds stack trace continuations, unwraps JSON logs.
    member this.Feed(line: string) : int =
        lines <- lines + 1L
        if cfg.Fold && not (isNull last) && isCont line then
            folded <- folded + 1L
            last.Folded <- last.Folded + 1L
            if isNull last.Hint then
                let t = line.Trim()
                last.Hint <- if t.Length > 200 then t.Substring(0, 200) else t
            0
        else
            let trimmed = line.TrimStart()
            if cfg.Json && trimmed.Length > 0 && trimmed[0] = '{' then
                match extractJson trimmed with
                | Some(msg, lvl) -> this.Add(msg, Some lvl)
                | None ->
                    jsonFallbacks <- jsonFallbacks + 1L
                    this.Add(line, None)
            else
                this.Add(line, None)

// ---------------------------------------------------------------------------
// Bounded line reader: a single enormous line cannot exhaust memory.
// ---------------------------------------------------------------------------

let readLines (reader: TextReader) (maxChars: int) (truncated: int64 ref) (onLine: string -> unit) =
    let buffer = Array.zeroCreate<char> 16384
    let sb = StringBuilder()
    let overflow = ref false
    let pending = ref false
    let emit () =
        if sb.Length > 0 && sb[sb.Length - 1] = '\r' then sb.Length <- sb.Length - 1
        if overflow.Value then truncated.Value <- truncated.Value + 1L
        onLine (sb.ToString())
        sb.Clear() |> ignore
        overflow.Value <- false
        pending.Value <- false
    let mutable n = reader.Read(buffer, 0, buffer.Length)
    while n > 0 do
        for i in 0 .. n - 1 do
            let ch = buffer[i]
            if ch = '\n' then
                emit ()
            else
                pending.Value <- true
                if sb.Length < maxChars then sb.Append(ch) |> ignore else overflow.Value <- true
        n <- reader.Read(buffer, 0, buffer.Length)
    if pending.Value then emit ()

// ---------------------------------------------------------------------------
// Snapshots and drift
// ---------------------------------------------------------------------------

type Snapshot = { Total: int64; Templates: (string[] * int64)[] }

let snapshotOf (miner: Miner) : Snapshot =
    let s = miner.Stats
    { Total = s.Records - s.Blank
      Templates = miner.Clusters |> Array.sortByDescending (fun c -> c.Count) |> Array.map (fun c -> c.Template, c.Count) }

let saveSnapshot (path: string) (snap: Snapshot) =
    let tmp = path + ".tmp"
    do
        use fs = File.Create tmp
        use w = new Utf8JsonWriter(fs, JsonWriterOptions(Indented = true))
        w.WriteStartObject()
        w.WriteNumber("version", 1)
        w.WriteNumber("total", snap.Total)
        w.WriteStartArray("templates")
        for (t, c) in snap.Templates do
            w.WriteStartObject()
            w.WriteString("template", String.Join(" ", t))
            w.WriteNumber("count", c)
            w.WriteEndObject()
        w.WriteEndArray()
        w.WriteEndObject()
        w.Flush()
    File.Move(tmp, path, true)

let loadSnapshot (path: string) : Snapshot =
    use doc = JsonDocument.Parse(File.ReadAllBytes path, JsonDocumentOptions(MaxDepth = 8))
    let root = doc.RootElement
    let version =
        match root.TryGetProperty("version") with
        | true, v when v.ValueKind = JsonValueKind.Number -> v.GetInt32()
        | _ -> 0
    if version <> 1 then failwithf "Unsupported snapshot version %d in %s" version path
    let total = root.GetProperty("total").GetInt64()
    let templates =
        root.GetProperty("templates").EnumerateArray()
        |> Seq.map (fun e ->
            let t = e.GetProperty("template").GetString()
            t.Split(' ', StringSplitOptions.RemoveEmptyEntries), e.GetProperty("count").GetInt64())
        |> Seq.toArray
    { Total = total; Templates = templates }

type Drift =
    { New: Cluster[]
      Vanished: (string[] * int64)[]
      Moved: (string[] * int64 * int64 * float)[] }

let private covers (b: string[]) (t: string[]) =
    b.Length = t.Length && Array.forall2 (fun x y -> x = Wildcard || x = y) b t

let private specificity (t: string[]) =
    t |> Array.sumBy (fun x -> if x = Wildcard then 0 else 1)

/// A baseline template covers a current one when every literal token matches.
/// Each current template is credited to its most specific covering baseline template.
let compareBaseline (baseline: Snapshot) (miner: Miner) (shift: float) (minCount: int64) (minNew: int64) : Drift =
    let cur = snapshotOf miner
    let byLen = Dictionary<int, ResizeArray<int>>()
    baseline.Templates
    |> Array.iteri (fun i (t, _) ->
        match byLen.TryGetValue(t.Length) with
        | true, l -> l.Add i
        | _ ->
            let l = ResizeArray<int>()
            l.Add i
            byLen[t.Length] <- l)
    let curCounts = Array.zeroCreate<int64> baseline.Templates.Length
    let fresh = ResizeArray<Cluster>()
    for c in miner.Clusters do
        let mutable bestIdx = -1
        let mutable bestSpec = -1
        match byLen.TryGetValue(c.Template.Length) with
        | true, cands ->
            for i in cands do
                let (b, _) = baseline.Templates[i]
                if covers b c.Template then
                    let sp = specificity b
                    if sp > bestSpec then
                        bestSpec <- sp
                        bestIdx <- i
        | _ -> ()
        if bestIdx < 0 then
            if c.Count >= minNew then fresh.Add c
        else
            curCounts[bestIdx] <- curCounts[bestIdx] + c.Count
    let vanished = ResizeArray<string[] * int64>()
    let moved = ResizeArray<string[] * int64 * int64 * float>()
    baseline.Templates
    |> Array.iteri (fun i (t, bc) ->
        let cc = curCounts[i]
        if cc = 0L then
            if bc >= minCount then vanished.Add((t, bc))
        elif baseline.Total > 0L && cur.Total > 0L && max bc cc >= minCount then
            let ratio = (float cc / float cur.Total) / (float bc / float baseline.Total)
            if ratio >= shift || ratio <= 1.0 / shift then moved.Add((t, bc, cc, ratio)))
    { New = fresh.ToArray() |> Array.sortByDescending (fun c -> c.Count)
      Vanished = vanished.ToArray() |> Array.sortByDescending snd
      Moved = moved.ToArray() |> Array.sortByDescending (fun (_, _, _, r) -> abs (log r)) }

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

let private fmt (n: int64) = n.ToString("N0", CultureInfo.InvariantCulture)

let private tmpl (t: string[]) = String.Join(" ", t)

let writeText (w: TextWriter) (miner: Miner) (readerTruncated: int64) (top: int) =
    let s = miner.Stats
    let total = max 1L (s.Records - s.Blank)
    w.WriteLine(
        sprintf
            "lines %s  records %s  folded %s  blank %s  clusters %s"
            (fmt s.Lines) (fmt s.Records) (fmt s.Folded) (fmt s.Blank) (fmt (int64 s.LiveClusters))
    )
    if s.EvictedClusters > 0L then
        w.WriteLine(
            sprintf "evicted %s clusters covering %s records (raise --max-clusters to keep them)"
                (fmt s.EvictedClusters) (fmt s.EvictedRecords)
        )
    if readerTruncated > 0L then
        w.WriteLine(sprintf "truncated %s over long lines at --max-line characters" (fmt readerTruncated))
    if s.JsonFallbacks > 0L then
        w.WriteLine(sprintf "json fallbacks %s lines started with { but had no usable message field" (fmt s.JsonFallbacks))
    if s.MaskTimeouts > 0L then
        w.WriteLine(sprintf "mask timeouts %s (a regex hit its 50 ms limit and was skipped for that line)" (fmt s.MaskTimeouts))
    w.WriteLine()
    w.WriteLine(sprintf "%6s %12s %7s %9s %9s  %s" "id" "count" "share" "errors" "warns" "template")
    let ranked =
        miner.Clusters |> Array.sortBy (fun c -> -c.Count, c.Id) |> Array.truncate (max 0 top)
    for c in ranked do
        w.WriteLine(
            sprintf "%6d %12s %6.2f%% %9s %9s  %s"
                c.Id (fmt c.Count) (float c.Count * 100.0 / float total) (fmt c.Errors) (fmt c.Warns) (tmpl c.Template)
        )
        if not (isNull c.Hint) then w.WriteLine(sprintf "%6s %12s %7s %9s %9s    hint: %s" "" "" "" "" "" c.Hint)

let writeDrift (w: TextWriter) (d: Drift) =
    w.WriteLine()
    w.WriteLine(sprintf "drift: %d new, %d vanished, %d shifted" d.New.Length d.Vanished.Length d.Moved.Length)
    for c in d.New do
        w.WriteLine(sprintf "  NEW      %10s  errors %-6s %s" (fmt c.Count) (fmt c.Errors) (tmpl c.Template))
    for (t, bc) in d.Vanished do
        w.WriteLine(sprintf "  VANISHED %10s  %s" (fmt bc) (tmpl t))
    for (t, bc, cc, r) in d.Moved do
        w.WriteLine(sprintf "  SHIFT    %s -> %s  x%.2f  %s" (fmt bc) (fmt cc) r (tmpl t))

let writeJson (stream: Stream) (miner: Miner) (readerTruncated: int64) (top: int) (drift: Drift option) =
    use w = new Utf8JsonWriter(stream, JsonWriterOptions(Indented = true))
    let s = miner.Stats
    w.WriteStartObject()
    w.WriteStartObject("stats")
    w.WriteNumber("lines", s.Lines)
    w.WriteNumber("records", s.Records)
    w.WriteNumber("folded", s.Folded)
    w.WriteNumber("blank", s.Blank)
    w.WriteNumber("liveClusters", s.LiveClusters)
    w.WriteNumber("evictedClusters", s.EvictedClusters)
    w.WriteNumber("evictedRecords", s.EvictedRecords)
    w.WriteNumber("truncatedLines", readerTruncated)
    w.WriteNumber("jsonFallbacks", s.JsonFallbacks)
    w.WriteNumber("maskTimeouts", s.MaskTimeouts)
    w.WriteEndObject()
    w.WriteStartArray("clusters")
    for c in miner.Clusters |> Array.sortBy (fun c -> -c.Count, c.Id) |> Array.truncate (max 0 top) do
        w.WriteStartObject()
        w.WriteNumber("id", c.Id)
        w.WriteNumber("count", c.Count)
        w.WriteNumber("errors", c.Errors)
        w.WriteNumber("warns", c.Warns)
        w.WriteNumber("firstRecord", c.FirstLine)
        w.WriteNumber("lastRecord", c.LastLine)
        w.WriteString("template", tmpl c.Template)
        w.WriteString("sample", c.Sample)
        if not (isNull c.Hint) then w.WriteString("hint", c.Hint)
        w.WriteEndObject()
    w.WriteEndArray()
    match drift with
    | None -> ()
    | Some d ->
        w.WriteStartObject("drift")
        w.WriteStartArray("new")
        for c in d.New do
            w.WriteStartObject()
            w.WriteNumber("count", c.Count)
            w.WriteNumber("errors", c.Errors)
            w.WriteString("template", tmpl c.Template)
            w.WriteEndObject()
        w.WriteEndArray()
        w.WriteStartArray("vanished")
        for (t, bc) in d.Vanished do
            w.WriteStartObject()
            w.WriteNumber("baselineCount", bc)
            w.WriteString("template", tmpl t)
            w.WriteEndObject()
        w.WriteEndArray()
        w.WriteStartArray("shifted")
        for (t, bc, cc, r) in d.Moved do
            w.WriteStartObject()
            w.WriteNumber("baselineCount", bc)
            w.WriteNumber("currentCount", cc)
            w.WriteNumber("rateRatio", r)
            w.WriteString("template", tmpl t)
            w.WriteEndObject()
        w.WriteEndArray()
        w.WriteEndObject()
    w.WriteEndObject()
    w.Flush()

// ---------------------------------------------------------------------------
// Self test: cheap invariants that must hold before the miner is trusted.
// ---------------------------------------------------------------------------

let selfTest () : string list =
    let failures = ResizeArray<string>()
    let check name ok = if not ok then failures.Add name
    let cfg = defaultConfig
    // grouping and masking
    let m = Miner(cfg, [])
    for i in 1..50 do
        m.Feed(sprintf "2026-10-08T10:00:%02dZ INFO user %d logged in from 10.0.0.%d" (i % 60) i (i % 250)) |> ignore
    m.Feed("2026-10-08T10:01:00Z ERROR disk /dev/sda1 is 97% full") |> ignore
    check "masked lines collapse into one cluster" (m.Clusters |> Array.filter (fun c -> c.Count = 50L) |> Array.length = 1)
    check "different shape stays separate" (m.Clusters.Length = 2)
    check "error counted" (m.Clusters |> Array.sumBy (fun c -> c.Errors) = 1L)
    // wildcard generalisation
    let g = Miner(cfg, [])
    g.Feed "connect to alpha failed" |> ignore
    g.Feed "connect to bravo failed" |> ignore
    g.Feed "connect to charlie failed" |> ignore
    check "literal difference becomes wildcard" (g.Clusters.Length = 1 && g.Clusters[0].Template[2] = Wildcard)
    // folding
    let f = Miner(cfg, [])
    f.Feed "ERROR request failed" |> ignore
    f.Feed "\tat com.example.Handler.run(Handler.java:42)" |> ignore
    f.Feed "\tat com.example.Server.loop(Server.java:7)" |> ignore
    check "stack frames fold into their record" (f.Stats.Records = 1L && f.Stats.Folded = 2L && not (isNull f.Clusters[0].Hint))
    // json
    let j = Miner(cfg, [])
    j.Feed("""{"level":"error","msg":"retry 3 failed for job 9f"}""") |> ignore
    j.Feed("""{"level":"error","msg":"retry 4 failed for job 9f"}""") |> ignore
    j.Feed("""{"nope":1}""") |> ignore
    check "json messages cluster and keep level" (j.Clusters |> Array.exists (fun c -> c.Count = 2L && c.Errors = 2L))
    check "json without message falls back" (j.Stats.JsonFallbacks = 1L)
    // eviction bound
    let e = Miner({ cfg with MaxClusters = 20 }, [])
    let enc (n: int) =
        String([| char (97 + n % 26); char (97 + (n / 26) % 26); char (97 + (n / 676) % 26) |])
    for i in 1..500 do
        e.Feed(sprintf "%s %s %s %s" (enc i) (enc (i * 7 + 1)) (enc (i * 13 + 5)) (enc (i * 31 + 9))) |> ignore
    check "cluster count stays bounded" (e.Stats.LiveClusters <= 20)
    check "evictions are accounted" (e.Stats.EvictedClusters > 0L && e.Stats.EvictedRecords > 0L)
    // reader bound
    let truncated = ref 0L
    let got = ResizeArray<string>()
    readLines (new StringReader("short\r\n" + String('a', 100) + "\nlast")) 10 truncated got.Add
    check "reader truncates long lines" (truncated.Value = 1L && got.Count = 3 && got[1].Length = 10 && got[0] = "short")
    // baseline drift
    let b = Miner(cfg, [])
    for i in 1..100 do b.Feed(sprintf "cache hit for key %d" i) |> ignore
    for i in 1..100 do b.Feed(sprintf "legacy path %d taken" i) |> ignore
    let snap = snapshotOf b
    let c = Miner(cfg, [])
    for i in 1..100 do c.Feed(sprintf "cache hit for key %d" i) |> ignore
    for i in 1..100 do c.Feed(sprintf "circuit open for upstream %d" i) |> ignore
    let d = compareBaseline snap c 5.0 20L 1L
    check "new template detected" (d.New.Length = 1 && (tmpl d.New[0].Template).StartsWith("circuit"))
    check "vanished template detected" (d.Vanished.Length = 1)
    List.ofSeq failures

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

type Options =
    { Files: string list
      Cfg: Config
      Masks: (Regex * string) list
      Top: int
      AsJson: bool
      Annotate: string option
      SaveSnapshot: string option
      Baseline: string option
      Shift: float
      MinCount: int64
      MinNew: int64
      FailOnNew: int option
      SelfTest: bool
      Help: bool }

let usage =
    """LogTemplateMiner: cluster log lines into templates, bounded memory, one pass.

usage: LogTemplateMiner [options] [file ...]     (no file or "-" reads stdin)

clustering
  --sim F             similarity threshold 0 to 1, default 0.5
  --depth N           prefix tree depth, at least 3, default 4
  --max-children N    children per tree node before overflow to <*>, default 64
  --max-clusters N    live cluster cap, least recently seen are evicted, default 10000
  --max-tokens N      tokens kept per message, default 128
  --max-line N        characters kept per physical line, default 8192
  --mask NAME=REGEX   extra mask, replaces matches with <NAME>, may repeat
  --raw               do not unwrap JSON log lines
  --no-fold           do not fold stack trace continuation lines

output
  --top N             clusters to print, default 50
  --format text|json  default text
  --annotate PATH     write "record<TAB>clusterId" for every record
  --save-snapshot P   write the final templates as a snapshot
  --baseline PATH     compare against a snapshot and print drift
  --shift F           rate ratio that counts as a shift, default 5
  --min-count N       ignore templates below this count when judging drift, default 20
  --min-new N         a new template needs this many hits to be reported, default 1
  --fail-on-new N     exit 3 when more than N new templates appear (needs --baseline)

other
  --self-test         run built in checks and exit
  --help

exit codes: 0 ok, 1 runtime error, 2 bad usage, 3 drift gate tripped, 4 self test failed"""

let private tryFloat (s: string) =
    match Double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture) with
    | true, v when not (Double.IsNaN v) && not (Double.IsInfinity v) -> Some v
    | _ -> None

let private tryInt (s: string) =
    match Int32.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture) with
    | true, v -> Some v
    | _ -> None

let parseArgs (argv: string[]) : Result<Options, string> =
    let mutable o =
        { Files = []
          Cfg = defaultConfig
          Masks = []
          Top = 50
          AsJson = false
          Annotate = None
          SaveSnapshot = None
          Baseline = None
          Shift = 5.0
          MinCount = 20L
          MinNew = 1L
          FailOnNew = None
          SelfTest = false
          Help = false }
    let err: string option ref = ref None
    let i = ref 0
    let fail msg = if err.Value.IsNone then err.Value <- Some msg
    let next (name: string) =
        if i.Value + 1 < argv.Length then
            i.Value <- i.Value + 1
            argv[i.Value]
        else
            fail (sprintf "%s needs a value" name)
            ""
    let intIn name lo hi =
        match tryInt (next name) with
        | Some v when v >= lo && v <= hi -> v
        | _ ->
            fail (sprintf "%s must be an integer from %d to %d" name lo hi)
            lo
    while i.Value < argv.Length && err.Value.IsNone do
        match argv[i.Value] with
        | "--help" | "-h" -> o <- { o with Help = true }
        | "--self-test" -> o <- { o with SelfTest = true }
        | "--raw" -> o <- { o with Cfg = { o.Cfg with Json = false } }
        | "--no-fold" -> o <- { o with Cfg = { o.Cfg with Fold = false } }
        | "--sim" ->
            match tryFloat (next "--sim") with
            | Some v when v > 0.0 && v <= 1.0 -> o <- { o with Cfg = { o.Cfg with SimThreshold = v } }
            | _ -> fail "--sim must be a number above 0 and at most 1"
        | "--shift" ->
            match tryFloat (next "--shift") with
            | Some v when v > 1.0 -> o <- { o with Shift = v }
            | _ -> fail "--shift must be a number above 1"
        | "--depth" -> o <- { o with Cfg = { o.Cfg with Depth = intIn "--depth" 3 12 } }
        | "--max-children" -> o <- { o with Cfg = { o.Cfg with MaxChildren = intIn "--max-children" 2 100000 } }
        | "--max-clusters" -> o <- { o with Cfg = { o.Cfg with MaxClusters = intIn "--max-clusters" 10 5000000 } }
        | "--max-tokens" -> o <- { o with Cfg = { o.Cfg with MaxTokens = intIn "--max-tokens" 4 4096 } }
        | "--max-line" -> o <- { o with Cfg = { o.Cfg with MaxLineChars = intIn "--max-line" 64 10000000 } }
        | "--top" -> o <- { o with Top = intIn "--top" 0 1000000 }
        | "--min-count" -> o <- { o with MinCount = int64 (intIn "--min-count" 0 Int32.MaxValue) }
        | "--min-new" -> o <- { o with MinNew = int64 (intIn "--min-new" 1 Int32.MaxValue) }
        | "--fail-on-new" -> o <- { o with FailOnNew = Some(intIn "--fail-on-new" 0 Int32.MaxValue) }
        | "--format" ->
            match next "--format" with
            | "text" -> o <- { o with AsJson = false }
            | "json" -> o <- { o with AsJson = true }
            | _ -> fail "--format must be text or json"
        | "--annotate" -> o <- { o with Annotate = Some(next "--annotate") }
        | "--save-snapshot" -> o <- { o with SaveSnapshot = Some(next "--save-snapshot") }
        | "--baseline" -> o <- { o with Baseline = Some(next "--baseline") }
        | "--mask" ->
            let spec = next "--mask"
            let eq = spec.IndexOf '='
            if eq < 1 || eq = spec.Length - 1 then
                fail "--mask needs NAME=REGEX"
            else
                let name = spec.Substring(0, eq).ToUpperInvariant()
                if not (Regex.IsMatch(name, "^[A-Z][A-Z0-9_]*$")) then
                    fail "--mask NAME must be letters, digits or underscore"
                else
                    try
                        let rx =
                            Regex(spec.Substring(eq + 1), RegexOptions.CultureInvariant, TimeSpan.FromMilliseconds 50.0)
                        o <- { o with Masks = o.Masks @ [ rx, "<" + name + ">" ] }
                    with :? ArgumentException as ex ->
                        fail (sprintf "--mask regex is invalid: %s" ex.Message)
        | a when a.StartsWith("--", StringComparison.Ordinal) -> fail (sprintf "unknown option %s" a)
        | file -> o <- { o with Files = o.Files @ [ file ] }
        i.Value <- i.Value + 1
    match err.Value with
    | Some e -> Error e
    | None ->
        if o.FailOnNew.IsSome && o.Baseline.IsNone then Error "--fail-on-new needs --baseline"
        else Ok o

let run (o: Options) : int =
    let miner = Miner(o.Cfg, o.Masks)
    let truncated = ref 0L
    use annot: StreamWriter =
        match o.Annotate with
        | Some p -> new StreamWriter(p, false, UTF8Encoding(false))
        | None -> null
    let onLine (line: string) =
        let id = miner.Feed line
        if id > 0 && not (isNull annot) then
            annot.Write(miner.Stats.Records)
            annot.Write('\t')
            annot.WriteLine(id)
    let readOne (path: string) =
        if path = "-" then
            readLines Console.In o.Cfg.MaxLineChars truncated onLine
        else
            use r = new StreamReader(path, UTF8Encoding(false), true, 65536)
            readLines r o.Cfg.MaxLineChars truncated onLine
    if o.Files.IsEmpty then
        use r = new StreamReader(Console.OpenStandardInput(), UTF8Encoding(false), true, 65536)
        readLines r o.Cfg.MaxLineChars truncated onLine
    else
        for f in o.Files do readOne f
    let drift =
        match o.Baseline with
        | Some p ->
            let snap = loadSnapshot p
            Some(compareBaseline snap miner o.Shift o.MinCount o.MinNew)
        | None -> None
    match o.SaveSnapshot with
    | Some p -> saveSnapshot p (snapshotOf miner)
    | None -> ()
    if o.AsJson then
        use out = Console.OpenStandardOutput()
        writeJson out miner truncated.Value o.Top drift
        out.WriteByte(10uy)
    else
        writeText Console.Out miner truncated.Value o.Top
        match drift with
        | Some d -> writeDrift Console.Out d
        | None -> ()
    match drift, o.FailOnNew with
    | Some d, Some limit when d.New.Length > limit ->
        eprintfn "drift gate: %d new templates, limit %d" d.New.Length limit
        3
    | _ -> 0

[<EntryPoint>]
let main argv =
    match parseArgs argv with
    | Error e ->
        eprintfn "error: %s\n\n%s" e usage
        2
    | Ok o when o.Help ->
        printfn "%s" usage
        0
    | Ok o when o.SelfTest ->
        match selfTest () with
        | [] ->
            printfn "self test: all checks passed"
            0
        | failed ->
            for f in failed do eprintfn "self test failed: %s" f
            4
    | Ok o ->
        try
            run o
        with
        | :? IOException as ex ->
            eprintfn "io error: %s" ex.Message
            1
        | :? UnauthorizedAccessException as ex ->
            eprintfn "access error: %s" ex.Message
            1
        | ex ->
            eprintfn "error: %s" ex.Message
            1
