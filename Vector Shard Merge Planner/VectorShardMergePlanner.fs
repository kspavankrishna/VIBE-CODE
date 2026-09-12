// VectorShardMergePlanner.fs
// Plans a deterministic, idempotent merge of vector index shards without touching storage.
module VectorShardMergePlanner

open System
open System.Text
open System.Collections.Generic
open System.Security.Cryptography

// ---------------------------------------------------------------------------
// Small helpers the old FSharp.Core here does not ship (no Option.defaultValue,
// no Result type). Kept tiny and local rather than pulled from a package.
// ---------------------------------------------------------------------------

let optionDefault (fallback: 'a) (value: 'a option) : 'a =
    match value with
    | Some v -> v
    | None -> fallback

exception PlannerError of string

// ---------------------------------------------------------------------------
// A tiny hand written JSON value, parser and printer. No System.Text.Json
// reflection over discriminated unions, no NuGet package, one file, one
// dependency: the .NET base class library.
// ---------------------------------------------------------------------------

type JsonValue =
    | JNull
    | JBool of bool
    | JNumber of float
    | JString of string
    | JArray of JsonValue list
    | JObject of (string * JsonValue) list

exception JsonError of string

type private JsonParserState =
    { Text: string
      mutable Pos: int
      Length: int }

module private JsonParsing =

    let mk (s: string) : JsonParserState =
        { Text = s; Pos = 0; Length = s.Length }

    let peek (st: JsonParserState) : char option =
        if st.Pos < st.Length then Some st.Text.[st.Pos] else None

    let peekAt (st: JsonParserState) (offset: int) : char option =
        let p = st.Pos + offset
        if p >= 0 && p < st.Length then Some st.Text.[p] else None

    let advance (st: JsonParserState) : unit =
        st.Pos <- st.Pos + 1

    let fail (st: JsonParserState) (msg: string) : 'a =
        raise (JsonError (sprintf "%s at position %d" msg st.Pos))

    let isWs (c: char) : bool =
        c = ' ' || c = '\t' || c = '\n' || c = '\r'

    let skipWs (st: JsonParserState) : unit =
        let mutable go = true
        while go do
            match peek st with
            | Some c when isWs c -> advance st
            | _ -> go <- false

    let expect (st: JsonParserState) (c: char) : unit =
        match peek st with
        | Some actual when actual = c -> advance st
        | Some actual -> fail st (sprintf "expected '%c' but found '%c'" c actual)
        | None -> fail st (sprintf "expected '%c' but found end of input" c)

    let matchLiteral (st: JsonParserState) (lit: string) : bool =
        if st.Pos + lit.Length > st.Length then false
        else
            let slice = st.Text.Substring(st.Pos, lit.Length)
            if slice = lit then
                st.Pos <- st.Pos + lit.Length
                true
            else
                false

    let parseHex4 (st: JsonParserState) : int =
        if st.Pos + 4 > st.Length then fail st "truncated \\u escape"
        let s = st.Text.Substring(st.Pos, 4)
        st.Pos <- st.Pos + 4
        match Int32.TryParse(s, Globalization.NumberStyles.AllowHexSpecifier, Globalization.CultureInfo.InvariantCulture) with
        | true, v -> v
        | false, _ -> fail st (sprintf "invalid \\u escape '%s'" s)

    let parseString (st: JsonParserState) : string =
        expect st '"'
        let sb = StringBuilder()
        let mutable closed = false
        while not closed do
            match peek st with
            | None -> fail st "unterminated string"
            | Some '"' ->
                advance st
                closed <- true
            | Some '\\' ->
                advance st
                match peek st with
                | Some '"' -> sb.Append('"') |> ignore; advance st
                | Some '\\' -> sb.Append('\\') |> ignore; advance st
                | Some '/' -> sb.Append('/') |> ignore; advance st
                | Some 'b' -> sb.Append('\b') |> ignore; advance st
                | Some 'f' -> sb.Append('') |> ignore; advance st
                | Some 'n' -> sb.Append('\n') |> ignore; advance st
                | Some 'r' -> sb.Append('\r') |> ignore; advance st
                | Some 't' -> sb.Append('\t') |> ignore; advance st
                | Some 'u' ->
                    advance st
                    let code = parseHex4 st
                    sb.Append(char code) |> ignore
                | Some other -> fail st (sprintf "invalid escape '\\%c'" other)
                | None -> fail st "unterminated escape"
            | Some c ->
                sb.Append(c) |> ignore
                advance st
        sb.ToString()

    let isDigit (c: char) : bool = c >= '0' && c <= '9'

    let parseNumber (st: JsonParserState) : float =
        let start = st.Pos
        if peek st = Some '-' then advance st
        let mutable go = true
        while go do
            match peek st with
            | Some c when isDigit c -> advance st
            | _ -> go <- false
        if peek st = Some '.' then
            advance st
            let mutable go2 = true
            while go2 do
                match peek st with
                | Some c when isDigit c -> advance st
                | _ -> go2 <- false
        match peek st with
        | Some c when c = 'e' || c = 'E' ->
            advance st
            match peek st with
            | Some c2 when c2 = '+' || c2 = '-' -> advance st
            | _ -> ()
            let mutable go3 = true
            while go3 do
                match peek st with
                | Some c3 when isDigit c3 -> advance st
                | _ -> go3 <- false
        | _ -> ()
        let text = st.Text.Substring(start, st.Pos - start)
        match Double.TryParse(text, Globalization.NumberStyles.Float, Globalization.CultureInfo.InvariantCulture) with
        | true, v -> v
        | false, _ -> fail st (sprintf "invalid number literal '%s'" text)

    let rec parseValue (st: JsonParserState) : JsonValue =
        skipWs st
        match peek st with
        | Some '"' -> JString (parseString st)
        | Some '{' -> parseObject st
        | Some '[' -> parseArray st
        | Some c when c = '-' || isDigit c -> JNumber (parseNumber st)
        | Some 't' ->
            if matchLiteral st "true" then JBool true else fail st "invalid literal"
        | Some 'f' ->
            if matchLiteral st "false" then JBool false else fail st "invalid literal"
        | Some 'n' ->
            if matchLiteral st "null" then JNull else fail st "invalid literal"
        | Some c -> fail st (sprintf "unexpected character '%c'" c)
        | None -> fail st "unexpected end of input"

    and parseObject (st: JsonParserState) : JsonValue =
        expect st '{'
        skipWs st
        let fields = ResizeArray<string * JsonValue>()
        if peek st = Some '}' then
            advance st
        else
            let mutable go = true
            while go do
                skipWs st
                let key = parseString st
                skipWs st
                expect st ':'
                let value = parseValue st
                fields.Add((key, value))
                skipWs st
                match peek st with
                | Some ',' -> advance st
                | Some '}' -> advance st; go <- false
                | _ -> fail st "expected ',' or '}'"
        JObject (List.ofSeq fields)

    and parseArray (st: JsonParserState) : JsonValue =
        expect st '['
        skipWs st
        let items = ResizeArray<JsonValue>()
        if peek st = Some ']' then
            advance st
        else
            let mutable go = true
            while go do
                let value = parseValue st
                items.Add(value)
                skipWs st
                match peek st with
                | Some ',' -> advance st
                | Some ']' -> advance st; go <- false
                | _ -> fail st "expected ',' or ']'"
        JArray (List.ofSeq items)

let parseJson (input: string) : JsonValue =
    let st = JsonParsing.mk input
    let value = JsonParsing.parseValue st
    JsonParsing.skipWs st
    if st.Pos <> st.Length then
        raise (JsonError (sprintf "trailing content at position %d" st.Pos))
    value

let private escapeJsonString (s: string) : string =
    let sb = StringBuilder()
    sb.Append('"') |> ignore
    for c in s do
        match c with
        | '"' -> sb.Append("\\\"") |> ignore
        | '\\' -> sb.Append("\\\\") |> ignore
        | '\n' -> sb.Append("\\n") |> ignore
        | '\r' -> sb.Append("\\r") |> ignore
        | '\t' -> sb.Append("\\t") |> ignore
        | c when int c < 0x20 -> sb.Append(sprintf "\\u%04x" (int c)) |> ignore
        | c -> sb.Append(c) |> ignore
    sb.Append('"') |> ignore
    sb.ToString()

let jsonToString (pretty: bool) (root: JsonValue) : string =
    let sb = StringBuilder()
    let indentUnit = "  "
    let newline (depth: int) =
        if pretty then
            sb.Append('\n') |> ignore
            for _ in 1 .. depth do sb.Append(indentUnit) |> ignore
    let rec go (depth: int) (v: JsonValue) : unit =
        match v with
        | JNull -> sb.Append("null") |> ignore
        | JBool true -> sb.Append("true") |> ignore
        | JBool false -> sb.Append("false") |> ignore
        | JNumber n ->
            if Double.IsNaN n || Double.IsInfinity n then sb.Append("0") |> ignore
            elif n = Math.Floor(n) && Math.Abs(n) < 1e15 then
                sb.Append((int64 n).ToString(Globalization.CultureInfo.InvariantCulture)) |> ignore
            else
                sb.Append(n.ToString("R", Globalization.CultureInfo.InvariantCulture)) |> ignore
        | JString s -> sb.Append(escapeJsonString s) |> ignore
        | JArray [] -> sb.Append("[]") |> ignore
        | JArray items ->
            sb.Append('[') |> ignore
            items
            |> List.iteri (fun i item ->
                if i > 0 then sb.Append(',') |> ignore
                newline (depth + 1)
                go (depth + 1) item)
            newline depth
            sb.Append(']') |> ignore
        | JObject [] -> sb.Append("{}") |> ignore
        | JObject fields ->
            sb.Append('{') |> ignore
            fields
            |> List.iteri (fun i (key, value) ->
                if i > 0 then sb.Append(',') |> ignore
                newline (depth + 1)
                sb.Append(escapeJsonString key) |> ignore
                sb.Append(':') |> ignore
                if pretty then sb.Append(' ') |> ignore
                go (depth + 1) value)
            newline depth
            sb.Append('}') |> ignore
    go 0 root
    sb.ToString()

let tryField (name: string) (v: JsonValue) : JsonValue option =
    match v with
    | JObject fields -> fields |> List.tryFind (fun (k, _) -> k = name) |> Option.map snd
    | _ -> None

let asStringField (name: string) (v: JsonValue) : string option =
    tryField name v |> Option.bind (function JString s -> Some s | _ -> None)

let asFloatField (name: string) (v: JsonValue) : float option =
    tryField name v |> Option.bind (function JNumber n -> Some n | _ -> None)

let asBoolField (name: string) (v: JsonValue) : bool option =
    tryField name v |> Option.bind (function JBool b -> Some b | _ -> None)

let asArrayField (name: string) (v: JsonValue) : JsonValue list option =
    tryField name v |> Option.bind (function JArray xs -> Some xs | _ -> None)

// ---------------------------------------------------------------------------
// Hashing. sha256Hex is used three ways: shard checksum verification, the
// plan hash that proves two runs on the same input agree, and the consistent
// hash ring used for shard placement.
// ---------------------------------------------------------------------------

let sha256Bytes (s: string) : byte[] =
    use sha = SHA256.Create()
    sha.ComputeHash(Encoding.UTF8.GetBytes(s: string))

let sha256Hex (s: string) : string =
    sha256Bytes s
    |> Array.map (fun b -> b.ToString("x2"))
    |> String.concat ""

let hashToUInt64 (s: string) : uint64 =
    let bytes = sha256Bytes s
    let mutable v = 0UL
    for i in 0 .. 7 do
        v <- (v <<< 8) ||| (uint64 bytes.[i])
    v

// ---------------------------------------------------------------------------
// Domain
// ---------------------------------------------------------------------------

type ShardEntryInput =
    { DocId: string
      VectorId: string
      EmbeddingDim: int
      LogicalClock: int64
      ContentHash: string
      Deleted: bool
      ImportanceScore: float
      Vector: float[] option }

type ParsedEntry =
    | GoodEntry of ShardEntryInput
    | BadEntry of vectorIdHint: string * reason: string

type ShardInput =
    { ShardId: string
      DeclaredChecksum: string option
      RawEntries: ParsedEntry list }

type PlannerOptions =
    { TargetShardCount: int
      VirtualNodesPerShard: int
      CapacityBudget: int option
      DuplicateCosineThreshold: float
      LshBitsOverride: int option
      LshSeed: uint64
      MaxDuplicateBucketSize: int }

type PlanAction =
    | Keep of targetShard: int
    | Drop of reason: string
    | Quarantine of reason: string
    | Evict of reason: string
    | Review of reason: string

type PlannedEntry =
    { DocId: string
      VectorId: string
      SourceShard: string
      Action: PlanAction }

type QuarantinedShardInfo =
    { ShardId: string
      Reason: string }

type MergeStats =
    { TotalEntries: int
      Kept: int
      Dropped: int
      Quarantined: int
      Evicted: int
      Reviewed: int
      QuarantinedShardCount: int }

type MergePlan =
    { PlanHash: string
      CanonicalEmbeddingDim: int option
      LshBitsUsed: int option
      TargetShardCount: int
      Stats: MergeStats
      QuarantinedShards: QuarantinedShardInfo list
      Entries: PlannedEntry list }

type private LocatedEntry =
    { ShardId: string
      Entry: ShardEntryInput }

// ---------------------------------------------------------------------------
// Parsing shard input into the domain. A malformed entry never aborts the
// whole shard: it becomes a single quarantined row so one corrupt record in
// fifty thousand does not block the merge.
// ---------------------------------------------------------------------------

let private requireFiniteFloat (fieldName: string) (value: float) : unit =
    if Double.IsNaN value || Double.IsInfinity value then
        raise (PlannerError (sprintf "field '%s' must be a finite number" fieldName))

let parseEntry (json: JsonValue) : ParsedEntry =
    let hint =
        asStringField "vectorId" json
        |> optionDefault (asStringField "docId" json |> optionDefault "<unknown>")
    try
        let docId =
            match asStringField "docId" json with
            | Some s when s.Length > 0 -> s
            | _ -> raise (PlannerError "missing or empty 'docId'")
        let vectorId =
            match asStringField "vectorId" json with
            | Some s when s.Length > 0 -> s
            | _ -> raise (PlannerError "missing or empty 'vectorId'")
        let embeddingDim =
            match asFloatField "embeddingDim" json with
            | Some n when n >= 1.0 -> int n
            | _ -> raise (PlannerError "missing or invalid 'embeddingDim'")
        let logicalClock =
            match asFloatField "logicalClock" json with
            | Some n -> int64 n
            | None -> raise (PlannerError "missing 'logicalClock'")
        let contentHash =
            asStringField "contentHash" json |> optionDefault ""
        let deleted =
            asBoolField "deleted" json |> optionDefault false
        let importanceScore =
            asFloatField "importanceScore" json |> optionDefault 0.0
        requireFiniteFloat "importanceScore" importanceScore
        let vector =
            match asArrayField "vector" json with
            | None -> None
            | Some items ->
                let values =
                    items
                    |> List.map (function
                        | JNumber n ->
                            requireFiniteFloat "vector[]" n
                            n
                        | _ -> raise (PlannerError "'vector' must contain only numbers"))
                let arr = Array.ofList values
                if arr.Length <> embeddingDim then
                    raise (PlannerError (sprintf "vector length %d does not match embeddingDim %d" arr.Length embeddingDim))
                Some arr
        GoodEntry
            { DocId = docId
              VectorId = vectorId
              EmbeddingDim = embeddingDim
              LogicalClock = logicalClock
              ContentHash = contentHash
              Deleted = deleted
              ImportanceScore = importanceScore
              Vector = vector }
    with
    | PlannerError msg -> BadEntry (hint, msg)

let parseShard (json: JsonValue) : ShardInput =
    let shardId =
        match asStringField "shardId" json with
        | Some s when s.Length > 0 -> s
        | _ -> raise (PlannerError "shard is missing a non empty 'shardId'")
    let entries =
        match asArrayField "entries" json with
        | Some xs -> xs |> List.map parseEntry
        | None -> []
    { ShardId = shardId
      DeclaredChecksum = asStringField "declaredChecksum" json
      RawEntries = entries }

// ---------------------------------------------------------------------------
// Shard verification: a duplicate vectorId inside one shard, or a checksum
// that does not match the entries actually present, quarantines the whole
// shard rather than silently trusting half of a corrupt file.
// ---------------------------------------------------------------------------

let computeShardChecksum (entries: ShardEntryInput list) : string =
    entries
    |> List.sortBy (fun e -> e.VectorId)
    |> List.map (fun e ->
        String.concat "|"
            [ e.DocId; e.VectorId; e.ContentHash; string e.LogicalClock; string e.Deleted ])
    |> String.concat "\n"
    |> sha256Hex

let private internalConsistencyIssue (entries: ShardEntryInput list) : string option =
    entries
    |> List.groupBy (fun e -> e.VectorId)
    |> List.tryFind (fun (_, xs) -> List.length xs > 1)
    |> Option.map (fun (vid, xs) ->
        sprintf "duplicate vectorId '%s' appears %d times in one shard" vid (List.length xs))

type private ShardVerification =
    | ShardOk of shardId: string * validEntries: ShardEntryInput list
    | ShardBad of QuarantinedShardInfo

let private verifyShard (shard: ShardInput) : ShardVerification =
    let goodEntries =
        shard.RawEntries
        |> List.choose (function GoodEntry e -> Some e | BadEntry _ -> None)
    match internalConsistencyIssue goodEntries with
    | Some reason -> ShardBad { ShardId = shard.ShardId; Reason = reason }
    | None ->
        match shard.DeclaredChecksum with
        | None -> ShardOk (shard.ShardId, goodEntries)
        | Some declared ->
            let computed = computeShardChecksum goodEntries
            if String.Equals(computed, declared, StringComparison.OrdinalIgnoreCase) then
                ShardOk (shard.ShardId, goodEntries)
            else
                let reason = sprintf "checksum mismatch: declared %s, computed %s" declared computed
                ShardBad { ShardId = shard.ShardId; Reason = reason }

// ---------------------------------------------------------------------------
// Last writer wins resolution per document id. The sort key is a pure
// function of the entry's own fields, never of scan order, which is what
// makes resolveDocGroup produce the same winner no matter which shard is
// listed first in the input.
// ---------------------------------------------------------------------------

let private lwwSortKey (le: LocatedEntry) =
    let deletedRank = if le.Entry.Deleted then 0 else 1
    (-le.Entry.LogicalClock, deletedRank, le.Entry.ContentHash, le.Entry.VectorId, le.ShardId)

let private resolveDocGroup (docId: string) (group: LocatedEntry list) : PlannedEntry list * LocatedEntry option =
    match group |> List.sortBy lwwSortKey with
    | [] -> ([], None)
    | winner :: losers ->
        let loserReason =
            if winner.Entry.Deleted then
                sprintf "tombstoned: doc deleted at logical clock %d by shard %s" winner.Entry.LogicalClock winner.ShardId
            else
                sprintf "superseded by vectorId '%s' (logical clock %d) from shard %s" winner.Entry.VectorId winner.Entry.LogicalClock winner.ShardId
        let loserActions =
            losers
            |> List.map (fun le ->
                { DocId = docId; VectorId = le.Entry.VectorId; SourceShard = le.ShardId; Action = Drop loserReason })
        if winner.Entry.Deleted then
            let winnerAction =
                { DocId = docId
                  VectorId = winner.Entry.VectorId
                  SourceShard = winner.ShardId
                  Action = Drop (sprintf "tombstone: doc deleted at logical clock %d" winner.Entry.LogicalClock) }
            (winnerAction :: loserActions, None)
        else
            (loserActions, Some winner)

// ---------------------------------------------------------------------------
// Embedding dimension gate. Two entries can only sit in the same ANN index
// if they came from the same embedding space. Rather than guess, the
// canonical dimension is whichever dimension the majority of winners agree
// on, and anything else is quarantined individually.
// ---------------------------------------------------------------------------

let private pickCanonicalDim (candidates: LocatedEntry list) : int option =
    match candidates with
    | [] -> None
    | _ ->
        candidates
        |> List.countBy (fun le -> le.Entry.EmbeddingDim)
        |> List.sortBy (fun (dim, count) -> (-count, dim))
        |> List.head
        |> fst
        |> Some

// ---------------------------------------------------------------------------
// Near duplicate detection via random hyperplane (SimHash style) bucketing.
// Comparing every candidate against every other candidate is O(n^2) and does
// not survive a merge with hundreds of thousands of vectors, so vectors are
// first grouped into buckets that agree on a short hash signature, and only
// vectors inside the same bucket are ever compared with a real cosine
// similarity. Every hyperplane weight is a pure function of its own plane
// index and dimension index (mixSeed), not a stream from a seeded PRNG
// object, so the buckets are identical on any machine, any run, any .NET
// version, given the same seed.
// ---------------------------------------------------------------------------

let private mixSeed (a: uint64) (b: uint64) : uint64 =
    let mutable z = a ^^^ (b + 0x9E3779B97F4A7C15UL + (a <<< 6) + (a >>> 2))
    z <- (z ^^^ (z >>> 30)) * 0xBF58476D1CE4E5B9UL
    z <- (z ^^^ (z >>> 27)) * 0x94D049BB133111EBUL
    z ^^^ (z >>> 27)

let private toUnitDouble (x: uint64) : float =
    float (x >>> 11) * (1.0 / 9007199254740992.0)

let private hyperplaneWeight (seed: uint64) (planeIndex: int) (dimIndex: int) : float =
    let mixed = mixSeed (mixSeed seed (uint64 planeIndex)) (uint64 dimIndex)
    2.0 * (toUnitDouble mixed) - 1.0

let chooseLshBits (candidateCount: int) (requested: int option) : int =
    match requested with
    | Some b -> max 1 (min 24 b)
    | None ->
        if candidateCount <= 8 then 2
        else
            let targetBucketSize = 8.0
            let raw = Math.Ceiling(Math.Log(float candidateCount / targetBucketSize, 2.0))
            int raw |> max 2 |> min 24

let private buildHyperplanes (seed: uint64) (bits: int) (dim: int) : float[][] =
    Array.init bits (fun p -> Array.init dim (fun d -> hyperplaneWeight seed p d))

let private dotProduct (a: float[]) (b: float[]) : float =
    let mutable acc = 0.0
    for i in 0 .. a.Length - 1 do
        acc <- acc + a.[i] * b.[i]
    acc

let private l2Norm (a: float[]) : float =
    sqrt (dotProduct a a)

let cosineSimilarity (a: float[]) (b: float[]) : float =
    let na = l2Norm a
    let nb = l2Norm b
    if na = 0.0 || nb = 0.0 then 0.0
    else (dotProduct a b) / (na * nb)

let private signatureOf (hyperplanes: float[][]) (v: float[]) : int =
    let mutable acc = 0
    for p in 0 .. hyperplanes.Length - 1 do
        if dotProduct hyperplanes.[p] v >= 0.0 then
            acc <- acc ||| (1 <<< p)
    acc

/// Flags near duplicate vectors for manual review. It never drops or keeps
/// on their behalf: an automated cosine threshold is a good filter and a
/// bad judge, so the planner hands both candidates back with a Review
/// action instead of guessing which one is the real duplicate.
let private detectNearDuplicates
    (seed: uint64)
    (bitsOverride: int option)
    (threshold: float)
    (maxBucketSize: int)
    (candidates: LocatedEntry[])
    : Dictionary<int, string> =
    let reviewReasons = Dictionary<int, string>()
    let withVectors =
        candidates
        |> Array.mapi (fun i le -> (i, le))
        |> Array.choose (fun (i, le) -> le.Entry.Vector |> Option.map (fun v -> (i, le, v)))
    if withVectors.Length < 2 then
        reviewReasons
    else
        let dim = withVectors.[0] |> (fun (_, _, v) -> v.Length)
        let bits = chooseLshBits withVectors.Length bitsOverride
        let hyperplanes = buildHyperplanes seed bits dim
        let buckets = Dictionary<int, ResizeArray<int>>()
        for (localIdx, _, v) in withVectors do
            let sig_ = signatureOf hyperplanes v
            match buckets.TryGetValue sig_ with
            | true, existing -> existing.Add(localIdx)
            | false, _ ->
                let created = ResizeArray<int>()
                created.Add(localIdx)
                buckets.[sig_] <- created
        let byLocalIdx = Dictionary<int, (int * LocatedEntry * float[])>()
        for entry in withVectors do
            let (localIdx, _, _) = entry
            byLocalIdx.[localIdx] <- entry
        for kv in buckets do
            let members = kv.Value
            if members.Count > maxBucketSize then
                for localIdx in members do
                    reviewReasons.[localIdx] <- sprintf "duplicate check bucket too large (%d entries) for exhaustive comparison, verify externally" members.Count
            else
                for i in 0 .. members.Count - 1 do
                    for j in i + 1 .. members.Count - 1 do
                        let (idxA, leA, vecA) = byLocalIdx.[members.[i]]
                        let (idxB, leB, vecB) = byLocalIdx.[members.[j]]
                        if leA.Entry.DocId <> leB.Entry.DocId then
                            let sim = cosineSimilarity vecA vecB
                            if sim >= threshold then
                                if not (reviewReasons.ContainsKey idxA) then
                                    reviewReasons.[idxA] <- sprintf "possible duplicate of docId '%s' at cosine %.4f" leB.Entry.DocId sim
                                if not (reviewReasons.ContainsKey idxB) then
                                    reviewReasons.[idxB] <- sprintf "possible duplicate of docId '%s' at cosine %.4f" leA.Entry.DocId sim
        reviewReasons

// ---------------------------------------------------------------------------
// Capacity enforcement: deterministic priority order, ties broken by docId
// so two runs never disagree on who gets evicted.
// ---------------------------------------------------------------------------

let private capacitySortKey (le: LocatedEntry) =
    (-le.Entry.ImportanceScore, -le.Entry.LogicalClock, le.Entry.DocId)

let private enforceCapacity (budget: int option) (candidates: LocatedEntry list) : LocatedEntry list * PlannedEntry list =
    let total = List.length candidates
    match budget with
    | None -> (candidates, [])
    | Some b when b >= total -> (candidates, [])
    | Some b ->
        let ordered = candidates |> List.sortBy capacitySortKey
        let kept, evicted = ordered |> List.splitAt (max 0 b)
        let evictedActions =
            evicted
            |> List.mapi (fun i le ->
                { DocId = le.Entry.DocId
                  VectorId = le.Entry.VectorId
                  SourceShard = le.ShardId
                  Action = Evict (sprintf "capacity budget exceeded: rank %d of %d, budget %d" (b + i + 1) total b) })
        (kept, evictedActions)

// ---------------------------------------------------------------------------
// Placement via consistent hashing with virtual nodes. A plain
// hash(docId) mod shardCount reshuffles almost everything the moment
// shardCount changes; a ring with virtual nodes moves roughly 1/N of the
// documents instead, which is the entire point of using one here.
// ---------------------------------------------------------------------------

let buildRing (shardCount: int) (virtualNodes: int) : (uint64 * int)[] =
    [| for s in 0 .. shardCount - 1 do
        for v in 0 .. virtualNodes - 1 do
            yield (hashToUInt64 (sprintf "shard-%d#vnode-%d" s v), s) |]
    |> Array.sortBy fst

let placementFor (ring: (uint64 * int)[]) (docId: string) : int =
    let target = hashToUInt64 docId
    let mutable lo = 0
    let mutable hi = ring.Length - 1
    let mutable found = -1
    while lo <= hi do
        let mid = (lo + hi) / 2
        let (h, _) = ring.[mid]
        if h >= target then
            found <- mid
            hi <- mid - 1
        else
            lo <- mid + 1
    let idx = if found >= 0 then found else 0
    snd ring.[idx]

// ---------------------------------------------------------------------------
// Plan hashing and orchestration
// ---------------------------------------------------------------------------

let private actionTag (a: PlanAction) : string =
    match a with
    | Keep shard -> sprintf "keep:%d" shard
    | Drop reason -> sprintf "drop:%s" reason
    | Quarantine reason -> sprintf "quarantine:%s" reason
    | Evict reason -> sprintf "evict:%s" reason
    | Review reason -> sprintf "review:%s" reason

let computePlanHash (entries: PlannedEntry list) : string =
    entries
    |> List.sortBy (fun e -> (e.DocId, e.VectorId, e.SourceShard))
    |> List.map (fun e -> String.concat "|" [ e.DocId; e.VectorId; e.SourceShard; actionTag e.Action ])
    |> String.concat "\n"
    |> sha256Hex

let planMerge (shards: ShardInput list) (options: PlannerOptions) : MergePlan =
    if options.TargetShardCount < 1 then
        raise (PlannerError "targetShardCount must be at least 1")

    let totalRawEntries =
        shards |> List.sumBy (fun s -> List.length s.RawEntries)

    // Step 1: verify each shard's internal consistency and checksum.
    let verifications = shards |> List.map verifyShard

    let quarantinedShards =
        verifications
        |> List.choose (function ShardBad info -> Some info | ShardOk _ -> None)

    let quarantinedShardIds =
        quarantinedShards |> List.map (fun q -> q.ShardId) |> Set.ofList

    // Every raw entry belonging to a quarantined shard becomes a quarantine
    // action, good or bad, because the shard's checksum no longer vouches
    // for any single row inside it.
    let actionsFromQuarantinedShards =
        shards
        |> List.filter (fun s -> quarantinedShardIds.Contains s.ShardId)
        |> List.collect (fun shard ->
            let reason =
                quarantinedShards
                |> List.tryFind (fun q -> q.ShardId = shard.ShardId)
                |> Option.map (fun q -> sprintf "shard quarantined: %s" q.Reason)
                |> optionDefault "shard quarantined"
            shard.RawEntries
            |> List.map (fun raw ->
                match raw with
                | GoodEntry e ->
                    { DocId = e.DocId; VectorId = e.VectorId; SourceShard = shard.ShardId; Action = Quarantine reason }
                | BadEntry (hint, badReason) ->
                    { DocId = hint; VectorId = hint; SourceShard = shard.ShardId; Action = Quarantine (sprintf "%s; also malformed: %s" reason badReason) }))

    // Malformed entries in otherwise healthy shards are quarantined one row
    // at a time; the rest of the shard proceeds normally.
    let actionsFromMalformedEntries =
        shards
        |> List.filter (fun s -> not (quarantinedShardIds.Contains s.ShardId))
        |> List.collect (fun shard ->
            shard.RawEntries
            |> List.choose (function
                | BadEntry (hint, reason) ->
                    Some { DocId = hint; VectorId = hint; SourceShard = shard.ShardId; Action = Quarantine (sprintf "malformed entry: %s" reason) }
                | GoodEntry _ -> None))

    // Step 2: gather live, well formed entries from healthy shards and
    // resolve one winner per document id with last writer wins semantics.
    let located =
        verifications
        |> List.choose (function
            | ShardOk (shardId, entries) -> Some (shardId, entries)
            | ShardBad _ -> None)
        |> List.collect (fun (shardId, entries) ->
            entries |> List.map (fun e -> { ShardId = shardId; Entry = e }))

    let groups = located |> List.groupBy (fun le -> le.Entry.DocId)

    let resolvedPairs = groups |> List.map (fun (docId, group) -> resolveDocGroup docId group)
    let lwwActions = resolvedPairs |> List.collect fst
    let liveCandidates = resolvedPairs |> List.choose snd

    // Step 3: enforce a single embedding space across everything that will
    // share one ANN index.
    let canonicalDim = pickCanonicalDim liveCandidates
    let dimOk, dimMismatch =
        match canonicalDim with
        | None -> ([], [])
        | Some dim ->
            liveCandidates |> List.partition (fun le -> le.Entry.EmbeddingDim = dim)
    let dimMismatchActions =
        match canonicalDim with
        | None -> []
        | Some dim ->
            dimMismatch
            |> List.map (fun le ->
                { DocId = le.Entry.DocId
                  VectorId = le.Entry.VectorId
                  SourceShard = le.ShardId
                  Action = Quarantine (sprintf "embedding dimension mismatch: entry has dim %d, canonical merge dim is %d" le.Entry.EmbeddingDim dim) })

    // Step 4: flag near duplicates for review, without removing them.
    let dimOkArray = Array.ofList dimOk
    let reviewReasons = detectNearDuplicates options.LshSeed options.LshBitsOverride options.DuplicateCosineThreshold options.MaxDuplicateBucketSize dimOkArray
    let lshBitsUsed =
        let withVectorCount = dimOkArray |> Array.filter (fun le -> Option.isSome le.Entry.Vector) |> Array.length
        if withVectorCount < 2 then None
        else Some (chooseLshBits withVectorCount options.LshBitsOverride)

    let reviewed, clean =
        dimOkArray
        |> Array.mapi (fun i le -> (i, le))
        |> Array.toList
        |> List.partition (fun (i, _) -> reviewReasons.ContainsKey i)

    let reviewActions =
        reviewed
        |> List.map (fun (i, le) ->
            { DocId = le.Entry.DocId
              VectorId = le.Entry.VectorId
              SourceShard = le.ShardId
              Action = Review reviewReasons.[i] })

    let cleanCandidates = clean |> List.map snd

    // Step 5: enforce the capacity budget, if any, on what remains.
    let finalKeepCandidates, evictActions = enforceCapacity options.CapacityBudget cleanCandidates

    // Step 6: place survivors on a consistent hash ring.
    let ring = buildRing options.TargetShardCount options.VirtualNodesPerShard
    let keepActions =
        finalKeepCandidates
        |> List.map (fun le ->
            { DocId = le.Entry.DocId
              VectorId = le.Entry.VectorId
              SourceShard = le.ShardId
              Action = Keep (placementFor ring le.Entry.DocId) })

    let allActions =
        actionsFromQuarantinedShards
        @ actionsFromMalformedEntries
        @ lwwActions
        @ dimMismatchActions
        @ reviewActions
        @ evictActions
        @ keepActions

    let countWhere (f: PlanAction -> bool) =
        allActions |> List.filter (fun a -> f a.Action) |> List.length

    let stats =
        { TotalEntries = totalRawEntries
          Kept = countWhere (function Keep _ -> true | _ -> false)
          Dropped = countWhere (function Drop _ -> true | _ -> false)
          Quarantined = countWhere (function Quarantine _ -> true | _ -> false)
          Evicted = countWhere (function Evict _ -> true | _ -> false)
          Reviewed = countWhere (function Review _ -> true | _ -> false)
          QuarantinedShardCount = List.length quarantinedShards }

    { PlanHash = computePlanHash allActions
      CanonicalEmbeddingDim = canonicalDim
      LshBitsUsed = lshBitsUsed
      TargetShardCount = options.TargetShardCount
      Stats = stats
      QuarantinedShards = quarantinedShards
      Entries = allActions |> List.sortBy (fun e -> (e.DocId, e.VectorId, e.SourceShard)) }

// ---------------------------------------------------------------------------
// Output rendering
// ---------------------------------------------------------------------------

let private actionToJson (a: PlanAction) : JsonValue =
    match a with
    | Keep shard -> JObject [ ("type", JString "keep"); ("targetShard", JNumber (float shard)) ]
    | Drop reason -> JObject [ ("type", JString "drop"); ("reason", JString reason) ]
    | Quarantine reason -> JObject [ ("type", JString "quarantine"); ("reason", JString reason) ]
    | Evict reason -> JObject [ ("type", JString "evict"); ("reason", JString reason) ]
    | Review reason -> JObject [ ("type", JString "review"); ("reason", JString reason) ]

let private plannedEntryToJson (e: PlannedEntry) : JsonValue =
    JObject
        [ ("docId", JString e.DocId)
          ("vectorId", JString e.VectorId)
          ("sourceShard", JString e.SourceShard)
          ("action", actionToJson e.Action) ]

let mergePlanToJson (plan: MergePlan) : JsonValue =
    JObject
        [ ("planHash", JString plan.PlanHash)
          ("canonicalEmbeddingDim",
            match plan.CanonicalEmbeddingDim with
            | Some d -> JNumber (float d)
            | None -> JNull)
          ("lshBitsUsed",
            match plan.LshBitsUsed with
            | Some b -> JNumber (float b)
            | None -> JNull)
          ("targetShardCount", JNumber (float plan.TargetShardCount))
          ("stats",
            JObject
                [ ("totalEntries", JNumber (float plan.Stats.TotalEntries))
                  ("kept", JNumber (float plan.Stats.Kept))
                  ("dropped", JNumber (float plan.Stats.Dropped))
                  ("quarantined", JNumber (float plan.Stats.Quarantined))
                  ("evicted", JNumber (float plan.Stats.Evicted))
                  ("reviewed", JNumber (float plan.Stats.Reviewed))
                  ("quarantinedShardCount", JNumber (float plan.Stats.QuarantinedShardCount)) ])
          ("quarantinedShards",
            JArray (plan.QuarantinedShards |> List.map (fun q -> JObject [ ("shardId", JString q.ShardId); ("reason", JString q.Reason) ])))
          ("entries", JArray (plan.Entries |> List.map plannedEntryToJson)) ]

let renderText (plan: MergePlan) : string =
    let sb = StringBuilder()
    sb.AppendLine(sprintf "plan hash        : %s" plan.PlanHash) |> ignore
    sb.AppendLine(sprintf "target shards    : %d" plan.TargetShardCount) |> ignore
    sb.AppendLine(sprintf "canonical dim    : %s" (match plan.CanonicalEmbeddingDim with Some d -> string d | None -> "n/a")) |> ignore
    sb.AppendLine(sprintf "lsh bits used    : %s" (match plan.LshBitsUsed with Some b -> string b | None -> "n/a")) |> ignore
    sb.AppendLine(sprintf "total entries    : %d" plan.Stats.TotalEntries) |> ignore
    sb.AppendLine(sprintf "  keep           : %d" plan.Stats.Kept) |> ignore
    sb.AppendLine(sprintf "  drop           : %d" plan.Stats.Dropped) |> ignore
    sb.AppendLine(sprintf "  quarantine     : %d" plan.Stats.Quarantined) |> ignore
    sb.AppendLine(sprintf "  evict          : %d" plan.Stats.Evicted) |> ignore
    sb.AppendLine(sprintf "  review         : %d" plan.Stats.Reviewed) |> ignore
    sb.AppendLine(sprintf "quarantined shards: %d" plan.Stats.QuarantinedShardCount) |> ignore
    for q in plan.QuarantinedShards do
        sb.AppendLine(sprintf "  - %s: %s" q.ShardId q.Reason) |> ignore
    sb.ToString()

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

type private CliArgs =
    { InputPath: string option
      OutputPath: string option
      Format: string
      TargetShardsOverride: int option
      CapacityOverride: int option
      DuplicateThresholdOverride: float option
      LshBitsOverride: int option
      VirtualNodesOverride: int option
      Pretty: bool
      FailOnQuarantine: bool }

let private defaultCliArgs =
    { InputPath = None
      OutputPath = None
      Format = "json"
      TargetShardsOverride = None
      CapacityOverride = None
      DuplicateThresholdOverride = None
      LshBitsOverride = None
      VirtualNodesOverride = None
      Pretty = true
      FailOnQuarantine = false }

let rec private parseArgs (args: string list) (acc: CliArgs) : CliArgs =
    match args with
    | [] -> acc
    | "--input" :: v :: rest -> parseArgs rest { acc with InputPath = Some v }
    | "--output" :: v :: rest -> parseArgs rest { acc with OutputPath = Some v }
    | "--format" :: v :: rest -> parseArgs rest { acc with Format = v }
    | "--target-shards" :: v :: rest -> parseArgs rest { acc with TargetShardsOverride = Some (int v) }
    | "--capacity" :: v :: rest -> parseArgs rest { acc with CapacityOverride = Some (int v) }
    | "--duplicate-threshold" :: v :: rest -> parseArgs rest { acc with DuplicateThresholdOverride = Some (float v) }
    | "--lsh-bits" :: v :: rest -> parseArgs rest { acc with LshBitsOverride = Some (int v) }
    | "--virtual-nodes" :: v :: rest -> parseArgs rest { acc with VirtualNodesOverride = Some (int v) }
    | "--compact" :: rest -> parseArgs rest { acc with Pretty = false }
    | "--fail-on-quarantine" :: rest -> parseArgs rest { acc with FailOnQuarantine = true }
    | unknown :: _ -> raise (PlannerError (sprintf "unknown argument '%s'" unknown))

[<EntryPoint>]
let main argv =
    try
        let cli = parseArgs (List.ofArray argv) defaultCliArgs
        let inputText =
            match cli.InputPath with
            | Some path -> IO.File.ReadAllText path
            | None -> Console.In.ReadToEnd()
        let json = parseJson inputText
        let shards =
            match asArrayField "shards" json with
            | Some xs -> xs |> List.map parseShard
            | None -> raise (PlannerError "input JSON must have a 'shards' array")
        let targetShardCount =
            match cli.TargetShardsOverride with
            | Some n -> n
            | None ->
                match asFloatField "targetShardCount" json with
                | Some n -> int n
                | None -> raise (PlannerError "targetShardCount is required, in the JSON body or via --target-shards")
        let capacityBudget =
            match cli.CapacityOverride with
            | Some n -> Some n
            | None -> asFloatField "capacityBudget" json |> Option.map int
        let virtualNodes =
            match cli.VirtualNodesOverride with
            | Some n -> n
            | None -> asFloatField "virtualNodesPerShard" json |> Option.map int |> optionDefault 100
        let duplicateThreshold =
            match cli.DuplicateThresholdOverride with
            | Some t -> t
            | None -> asFloatField "duplicateCosineThreshold" json |> optionDefault 0.985
        let lshBits =
            match cli.LshBitsOverride with
            | Some b -> Some b
            | None -> asFloatField "lshBits" json |> Option.map int
        let options =
            { TargetShardCount = targetShardCount
              VirtualNodesPerShard = virtualNodes
              CapacityBudget = capacityBudget
              DuplicateCosineThreshold = duplicateThreshold
              LshBitsOverride = lshBits
              LshSeed = 0x9E3779B97F4A7C15UL
              MaxDuplicateBucketSize = 500 }
        let plan = planMerge shards options
        let output =
            if cli.Format = "text" then renderText plan
            else jsonToString cli.Pretty (mergePlanToJson plan)
        match cli.OutputPath with
        | Some path -> IO.File.WriteAllText(path, output)
        | None -> printfn "%s" output
        if cli.FailOnQuarantine && not (List.isEmpty plan.QuarantinedShards) then 1
        else 0
    with
    | JsonError msg ->
        eprintfn "input JSON error: %s" msg
        2
    | PlannerError msg ->
        eprintfn "error: %s" msg
        2
    | ex ->
        eprintfn "error: %s" ex.Message
        2
