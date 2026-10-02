// Rate limit header normalizer and wait planner. ReScript 11, Node target, zero dependencies.

type bucket = {
  limit: option<float>,
  remaining: option<float>,
  resetMs: option<float>,
}

type snapshot = {
  requests: bucket,
  tokens: bucket,
  inputTokens: bucket,
  outputTokens: bucket,
  retryAfterMs: option<float>,
  shouldRetry: option<bool>,
  skewMs: float,
  warnings: array<string>,
}

type need = {
  needRequests: float,
  needTokens: float,
  needInput: float,
  needOutput: float,
}

type policy = {
  baseBackoffMs: float,
  maxBackoffMs: float,
  maxWaitMs: float,
  safetyPadMs: float,
  jitterRatio: float,
}

type giveUp =
  | NeverFits(string)
  | DeadlineExceeded
  | ServerSaidNoRetry
  | WaitAboveCap

type decision =
  | Proceed
  | Wait({waitMs: float, binding: string})
  | GiveUp({reason: giveUp, detail: string})

let defaultPolicy: policy = {
  baseBackoffMs: 500.0,
  maxBackoffMs: 60000.0,
  maxWaitMs: 300000.0,
  safetyPadMs: 250.0,
  jitterRatio: 0.15,
}

let isEmpty = (b: bucket) =>
  switch (b.limit, b.remaining, b.resetMs) {
  | (None, None, None) => true
  | _ => false
  }

// ---------- small helpers ----------

let isFiniteNum = (x: float) => !Js.Float.isNaN(x) && x != infinity && x != neg_infinity

let clampMin = (x: float, lo: float) => x < lo ? lo : x

let dateMs = (s: string): float => Js.Date.getTime(Js.Date.fromString(s))

let parseNumber = (s: string): option<float> => {
  let t = Js.String2.trim(s)
  if t == "" {
    None
  } else {
    let n = Js.Float.fromString(t)
    isFiniteNum(n) ? Some(n) : None
  }
}

let unitMs = (u: string): float =>
  switch u {
  | "ms" => 1.0
  | "s" => 1000.0
  | "m" => 60000.0
  | "h" => 3600000.0
  | _ => 86400000.0
  }

// Parses "6m0s", "1.5s", "20ms", "1h2m3s". Returns None unless the whole string is consumed.
let parseDuration = (s: string): option<float> => {
  let t = Js.String2.toLowerCase(Js.String2.trim(s))
  if t == "" {
    None
  } else {
    let re = %re("/(\d+(?:\.\d+)?)(ms|h|m|s|d)/g")
    let total = ref(0.0)
    let consumed = ref(0)
    let go = ref(true)
    while go.contents {
      switch Js.Re.exec_(re, t) {
      | None => go := false
      | Some(r) => {
          let caps = Js.Re.captures(r)
          let whole = switch Js.Nullable.toOption(caps[0]) {
          | Some(w) => w
          | None => ""
          }
          let num = switch Js.Nullable.toOption(caps[1]) {
          | Some(n) => Js.Float.fromString(n)
          | None => nan
          }
          let u = switch Js.Nullable.toOption(caps[2]) {
          | Some(x) => x
          | None => "s"
          }
          if Js.Re.index(r) != consumed.contents {
            // gap between matches means stray characters
            go := false
            consumed := -1
          } else {
            total := total.contents +. num *. unitMs(u)
            consumed := consumed.contents + Js.String2.length(whole)
          }
        }
      }
    }
    if consumed.contents == Js.String2.length(t) && consumed.contents > 0 {
      Some(total.contents)
    } else {
      None
    }
  }
}

// ---------- header access ----------

let lowerKeys = (h: Js.Dict.t<string>): Js.Dict.t<string> => {
  let out = Js.Dict.empty()
  Js.Dict.entries(h)->Js.Array2.forEach(((k, v)) => Js.Dict.set(out, Js.String2.toLowerCase(k), v))
  out
}

let firstHeader = (h: Js.Dict.t<string>, names: array<string>): option<string> => {
  let found = ref(None)
  names->Js.Array2.forEach(n =>
    if Belt.Option.isNone(found.contents) {
      switch Js.Dict.get(h, n) {
      | Some(v) if Js.String2.trim(v) != "" => found := Some(v)
      | _ => ()
      }
    }
  )
  found.contents
}

// An absolute or relative reset value to milliseconds from "now" on the server clock.
// Accepts: duration strings, bare seconds, epoch seconds, epoch ms, RFC 3339 and HTTP dates.
let parseResetMs = (raw: string, nowMs: float, skewMs: float): option<float> =>
  switch parseDuration(raw) {
  | Some(ms) => Some(ms)
  | None =>
    switch parseNumber(raw) {
    | Some(n) =>
      if n >= 1e12 {
        Some(clampMin(n -. (nowMs +. skewMs), 0.0))
      } else if n >= 1e9 {
        Some(clampMin(n *. 1000.0 -. (nowMs +. skewMs), 0.0))
      } else {
        Some(clampMin(n, 0.0) *. 1000.0)
      }
    | None => {
        let d = dateMs(raw)
        isFiniteNum(d) ? Some(clampMin(d -. (nowMs +. skewMs), 0.0)) : None
      }
    }
  }

// Retry-After: delta seconds (may be fractional in the wild) or HTTP date. retry-after-ms wins when present.
let parseRetryAfter = (h: Js.Dict.t<string>, nowMs: float, skewMs: float): option<float> =>
  switch firstHeader(h, ["retry-after-ms"]) {
  | Some(v) =>
    switch parseNumber(v) {
    | Some(n) => Some(clampMin(n, 0.0))
    | None => None
    }
  | None =>
    switch firstHeader(h, ["retry-after"]) {
    | None => None
    | Some(v) =>
      switch parseNumber(v) {
      | Some(n) => Some(clampMin(n, 0.0) *. 1000.0)
      | None =>
        switch parseDuration(v) {
        | Some(ms) => Some(ms)
        | None => {
            let d = dateMs(v)
            isFiniteNum(d) ? Some(clampMin(d -. (nowMs +. skewMs), 0.0)) : None
          }
        }
      }
    }
  }

// RateLimit: limit=100, remaining=50, reset=30 (IETF draft structured field, simplified).
let parseStructured = (v: string): Js.Dict.t<string> => {
  let out = Js.Dict.empty()
  Js.String2.split(v, ",")->Js.Array2.forEach(part => {
    let kv = Js.String2.split(Js.String2.trim(part), "=")
    if Js.Array2.length(kv) == 2 {
      Js.Dict.set(out, Js.String2.toLowerCase(Js.String2.trim(kv[0])), Js.String2.trim(kv[1]))
    }
  })
  out
}

let readBucket = (
  h: Js.Dict.t<string>,
  name: string,
  nowMs: float,
  skewMs: float,
  warnings: array<string>,
): bucket => {
  let num = names =>
    switch firstHeader(h, names) {
    | None => None
    | Some(v) =>
      switch parseNumber(v) {
      | Some(n) => Some(n)
      | None => {
          let _ = Js.Array2.push(warnings, "unparseable value for " ++ Js.Array2.joinWith(names, "|"))
          None
        }
      }
    }
  let limit = num(["anthropic-ratelimit-" ++ name ++ "-limit", "x-ratelimit-limit-" ++ name])
  let remaining = num([
    "anthropic-ratelimit-" ++ name ++ "-remaining",
    "x-ratelimit-remaining-" ++ name,
  ])
  let resetNames = ["anthropic-ratelimit-" ++ name ++ "-reset", "x-ratelimit-reset-" ++ name]
  let resetMs = switch firstHeader(h, resetNames) {
  | None => None
  | Some(v) =>
    switch parseResetMs(v, nowMs, skewMs) {
    | Some(ms) => Some(ms)
    | None => {
        let _ = Js.Array2.push(warnings, "unparseable reset for " ++ name)
        None
      }
    }
  }
  // A remaining above the limit means a stale or mixed header set. Trust the limit.
  let remaining = switch (limit, remaining) {
  | (Some(l), Some(r)) if r > l => {
      let _ = Js.Array2.push(warnings, name ++ ": remaining above limit, clamped")
      Some(l)
    }
  | (_, Some(r)) if r < 0.0 => Some(0.0)
  | (_, r) => r
  }
  {limit, remaining, resetMs}
}

let normalize = (headers: Js.Dict.t<string>, nowMs: float): snapshot => {
  let h = lowerKeys(headers)
  let warnings = []
  // Skew: how far the server clock is ahead of ours. Absolute timestamps are corrected by it.
  let skewMs = switch Js.Dict.get(h, "date") {
  | None => 0.0
  | Some(d) => {
      let p = dateMs(d)
      if isFiniteNum(p) {
        p -. nowMs
      } else {
        let _ = Js.Array2.push(warnings, "unparseable Date header, assuming no skew")
        0.0
      }
    }
  }
  let requests = readBucket(h, "requests", nowMs, skewMs, warnings)
  let tokens = readBucket(h, "tokens", nowMs, skewMs, warnings)
  let inputTokens = readBucket(h, "input-tokens", nowMs, skewMs, warnings)
  let outputTokens = readBucket(h, "output-tokens", nowMs, skewMs, warnings)

  // Generic IETF style headers describe the request quota when nothing vendor specific exists.
  let generic = switch Js.Dict.get(h, "ratelimit") {
  | Some(v) => parseStructured(v)
  | None => Js.Dict.empty()
  }
  let pick = (plain, key) =>
    switch Js.Dict.get(h, plain) {
    | Some(v) => Some(v)
    | None => Js.Dict.get(generic, key)
    }
  let requests = if isEmpty(requests) {
    {
      limit: pick("ratelimit-limit", "limit")->Belt.Option.flatMap(parseNumber),
      remaining: pick("ratelimit-remaining", "remaining")->Belt.Option.flatMap(parseNumber),
      resetMs: pick("ratelimit-reset", "reset")->Belt.Option.flatMap(v =>
        parseResetMs(v, nowMs, skewMs)
      ),
    }
  } else {
    requests
  }

  let shouldRetry = switch Js.Dict.get(h, "x-should-retry") {
  | Some("true") => Some(true)
  | Some("false") => Some(false)
  | _ => None
  }
  {
    requests,
    tokens,
    inputTokens,
    outputTokens,
    retryAfterMs: parseRetryAfter(h, nowMs, skewMs),
    shouldRetry,
    skewMs,
    warnings,
  }
}

// ---------- planning ----------

// mulberry32, deterministic so a plan can be replayed from its seed.
@val external imul: (int, int) => int = "Math.imul"

let toU32: int => float = %raw(`(x) => x >>> 0`)

let rand01 = (seed: int): float => {
  let a = ref(seed + 0x6D2B79F5)
  let t = imul(lxor(a.contents, lsr(a.contents, 15)), lor(a.contents, 1))
  let t = lxor(t + imul(lxor(t, lsr(t, 7)), lor(t, 61)), t)
  toU32(lxor(t, lsr(t, 14))) /. 4294967296.0
}

let exponentialBackoff = (p: policy, attempt: int): float => {
  let raw = p.baseBackoffMs *. Js.Math.pow_float(~base=2.0, ~exp=Js.Int.toFloat(attempt))
  raw > p.maxBackoffMs ? p.maxBackoffMs : raw
}

// ageMs is time spent since the headers were observed. Reset windows shrink by it.
let plan = (
  snap: snapshot,
  need: need,
  p: policy,
  ~attempt: int,
  ~ageMs: float,
  ~deadlineMs: option<float>,
  ~seed: int,
): decision => {
  let noRetry = switch snap.shouldRetry {
  | Some(false) => true
  | _ => false
  }
  if noRetry && attempt > 0 {
    GiveUp({reason: ServerSaidNoRetry, detail: "x-should-retry is false"})
  } else {
    let dims = [
      ("requests", snap.requests, need.needRequests),
      ("tokens", snap.tokens, need.needTokens),
      ("input-tokens", snap.inputTokens, need.needInput),
      ("output-tokens", snap.outputTokens, need.needOutput),
    ]
    let impossible = dims->Js.Array2.find(((_, b, n)) =>
      switch b.limit {
      | Some(l) => n > l
      | None => false
      }
    )
    switch impossible {
    | Some((name, _, _)) =>
      GiveUp({
        reason: NeverFits(name),
        detail: "requested amount exceeds the " ++ name ++ " limit, no wait can fix it",
      })
    | None => {
        let bestWait = ref(0.0)
        let binding = ref("none")
        dims->Js.Array2.forEach(((name, b, n)) =>
          switch (b.remaining, b.resetMs) {
          | (Some(r), reset) if r < n => {
              let w = switch reset {
              | Some(ms) => clampMin(ms -. ageMs, 0.0)
              | None => exponentialBackoff(p, attempt)
              }
              if w >= bestWait.contents {
                bestWait := w
                binding := name
              }
            }
          | _ => ()
          }
        )
        switch snap.retryAfterMs {
        | Some(ra) if clampMin(ra -. ageMs, 0.0) > bestWait.contents => {
            bestWait := clampMin(ra -. ageMs, 0.0)
            binding := "retry-after"
          }
        | _ => ()
        }
        // No header guidance at all after a failure: fall back to capped exponential backoff.
        if binding.contents == "none" && attempt > 0 {
          bestWait := exponentialBackoff(p, attempt)
          binding := "backoff"
        }
        if binding.contents == "none" {
          Proceed
        } else {
          // Jitter only goes upward so we never retry before the server said we could.
          let jittered =
            bestWait.contents *. (1.0 +. p.jitterRatio *. rand01(seed)) +. p.safetyPadMs
          if jittered > p.maxWaitMs {
            GiveUp({
              reason: WaitAboveCap,
              detail: "needed wait exceeds maxWaitMs, binding " ++ binding.contents,
            })
          } else {
            switch deadlineMs {
            | Some(d) if jittered > d =>
              GiveUp({
                reason: DeadlineExceeded,
                detail: "wait would pass the caller deadline, binding " ++ binding.contents,
              })
            | _ => Wait({waitMs: Js.Math.ceil_float(jittered), binding: binding.contents})
            }
          }
        }
      }
    }
  }
}

// ---------- JSON edge ----------

let reasonName = (g: giveUp): string =>
  switch g {
  | NeverFits(_) => "never_fits"
  | DeadlineExceeded => "deadline_exceeded"
  | ServerSaidNoRetry => "server_said_no_retry"
  | WaitAboveCap => "wait_above_cap"
  }

let optNum = (o: option<float>): Js.Json.t =>
  switch o {
  | Some(n) => Js.Json.number(n)
  | None => Js.Json.null
  }

let bucketJson = (b: bucket): Js.Json.t =>
  Js.Json.object_(
    Js.Dict.fromArray([
      ("limit", optNum(b.limit)),
      ("remaining", optNum(b.remaining)),
      ("resetMs", optNum(b.resetMs)),
    ]),
  )

let snapshotJson = (s: snapshot): Js.Json.t =>
  Js.Json.object_(
    Js.Dict.fromArray([
      ("requests", bucketJson(s.requests)),
      ("tokens", bucketJson(s.tokens)),
      ("inputTokens", bucketJson(s.inputTokens)),
      ("outputTokens", bucketJson(s.outputTokens)),
      ("retryAfterMs", optNum(s.retryAfterMs)),
      ("skewMs", Js.Json.number(s.skewMs)),
      ("warnings", Js.Json.stringArray(s.warnings)),
    ]),
  )

let decisionJson = (d: decision): Js.Json.t =>
  switch d {
  | Proceed => Js.Json.object_(Js.Dict.fromArray([("action", Js.Json.string("proceed"))]))
  | Wait({waitMs, binding}) =>
    Js.Json.object_(
      Js.Dict.fromArray([
        ("action", Js.Json.string("wait")),
        ("waitMs", Js.Json.number(waitMs)),
        ("binding", Js.Json.string(binding)),
      ]),
    )
  | GiveUp({reason, detail}) =>
    Js.Json.object_(
      Js.Dict.fromArray([
        ("action", Js.Json.string("give_up")),
        ("reason", Js.Json.string(reasonName(reason))),
        ("detail", Js.Json.string(detail)),
      ]),
    )
  }

let objField = (j: Js.Json.t, key: string): option<Js.Json.t> =>
  switch Js.Json.decodeObject(j) {
  | Some(o) => Js.Dict.get(o, key)
  | None => None
  }

let numField = (j: Js.Json.t, key: string, default: float): float =>
  switch objField(j, key)->Belt.Option.flatMap(Js.Json.decodeNumber) {
  | Some(n) => n
  | None => default
  }

// Header values may arrive as strings or numbers depending on the HTTP client.
let headersOfJson = (j: Js.Json.t): Js.Dict.t<string> => {
  let out = Js.Dict.empty()
  switch Js.Json.decodeObject(j) {
  | None => ()
  | Some(o) =>
    Js.Dict.entries(o)->Js.Array2.forEach(((k, v)) =>
      switch Js.Json.classify(v) {
      | Js.Json.JSONString(s) => Js.Dict.set(out, k, s)
      | Js.Json.JSONNumber(n) => Js.Dict.set(out, k, Js.Float.toString(n))
      | Js.Json.JSONArray(a) =>
        // repeated header: first value wins
        switch Js.Array2.length(a) > 0 ? Js.Json.decodeString(a[0]) : None {
        | Some(s) => Js.Dict.set(out, k, s)
        | None => ()
        }
      | _ => ()
      }
    )
  }
  out
}

// Entry point used by the CLI and by tests: one JSON request in, one JSON answer out.
let run = (input: Js.Json.t): Js.Json.t => {
  let nowMs = numField(input, "nowMs", Js.Date.now())
  let headers = switch objField(input, "headers") {
  | Some(h) => headersOfJson(h)
  | None => Js.Dict.empty()
  }
  let n = switch objField(input, "need") {
  | Some(x) => {
      needRequests: numField(x, "requests", 1.0),
      needTokens: numField(x, "tokens", 0.0),
      needInput: numField(x, "inputTokens", 0.0),
      needOutput: numField(x, "outputTokens", 0.0),
    }
  | None => {needRequests: 1.0, needTokens: 0.0, needInput: 0.0, needOutput: 0.0}
  }
  let p = switch objField(input, "policy") {
  | Some(x) => {
      baseBackoffMs: numField(x, "baseBackoffMs", defaultPolicy.baseBackoffMs),
      maxBackoffMs: numField(x, "maxBackoffMs", defaultPolicy.maxBackoffMs),
      maxWaitMs: numField(x, "maxWaitMs", defaultPolicy.maxWaitMs),
      safetyPadMs: numField(x, "safetyPadMs", defaultPolicy.safetyPadMs),
      jitterRatio: numField(x, "jitterRatio", defaultPolicy.jitterRatio),
    }
  | None => defaultPolicy
  }
  let snap = normalize(headers, nowMs)
  let deadline = objField(input, "deadlineMs")->Belt.Option.flatMap(Js.Json.decodeNumber)
  let decision = plan(
    snap,
    n,
    p,
    ~attempt=Belt.Float.toInt(numField(input, "attempt", 0.0)),
    ~ageMs=numField(input, "ageMs", 0.0),
    ~deadlineMs=deadline,
    ~seed=Belt.Float.toInt(numField(input, "seed", 1.0)),
  )
  Js.Json.object_(
    Js.Dict.fromArray([("snapshot", snapshotJson(snap)), ("decision", decisionJson(decision))]),
  )
}

@module("fs") external readFileSync: (int, string) => string = "readFileSync"

let main = () => {
  let raw = readFileSync(0, "utf8")
  switch Js.Json.parseExn(raw) {
  | exception _ => {
      Js.Console.error("input is not valid JSON")
      %raw(`process.exit(2)`)
    }
  | j => Js.Console.log(Js.Json.stringify(run(j)))
  }
}

let isMain: bool = %raw(`require.main === module`)
if isMain {
  main()
}
