// CORS Preflight Simulator. ReScript 12, Node 20+ target, zero runtime dependencies.

type severity = SevError | SevWarning | SevInfo

type finding = {severity: severity, code: string, message: string}

type credentials = CredOmit | CredSameOrigin | CredInclude

// Header names are stored lowercase. Values keep their original bytes.
type headerList = array<(string, string)>

type httpResponse = {status: int, headers: headerList}

type expectation = ExpectAllow | ExpectBlock

type caseInput = {
  name: string,
  url: string,
  origin: string,
  method: string,
  headers: headerList,
  credentials: credentials,
  readHeaders: array<string>,
  // A recorded preflight (from a HAR) tells us exactly what the browser asked for.
  observedPreflight: option<(string, array<string>)>,
  preflights: array<(string, httpResponse)>,
  redirects: array<httpResponse>,
  response: option<httpResponse>,
  expect: option<expectation>,
}

type stage = AtPreflight(string) | AtRedirect(int) | AtResponse

type outcome =
  | SameOrigin
  | Allowed
  | PreflightPassed
  | Blocked(stage, string)
  | ScriptError(string)
  | NotEvaluated(string)

type cacheWindow = {declared: option<int>, chromium: int, firefox: int, webkit: int}

type caseReport = {
  name: string,
  method: string,
  url: string,
  origin: string,
  credentials: credentials,
  preflightReasons: array<string>,
  acrm: option<string>,
  acrh: option<string>,
  outcome: outcome,
  findings: array<finding>,
  caches: array<(string, cacheWindow)>,
  readable: array<string>,
  hidden: array<string>,
  expectationMet: option<bool>,
}

type analyzed = {
  method: string,
  headers: headerList,
  unsafe: array<string>,
  reasons: array<string>,
}

exception InputError(string)

// ---------- constants from the Fetch standard ----------

let safelistedMethods = ["GET", "HEAD", "POST"]

// Only these six are uppercased by fetch(). Everything else is sent byte for byte.
let normalizedMethods = ["DELETE", "GET", "HEAD", "OPTIONS", "POST", "PUT"]

let forbiddenMethods = ["CONNECT", "TRACE", "TRACK"]

let forbiddenRequestHeaders = [
  "accept-charset",
  "accept-encoding",
  "access-control-request-headers",
  "access-control-request-method",
  "connection",
  "content-length",
  "cookie",
  "cookie2",
  "date",
  "dnt",
  "expect",
  "host",
  "keep-alive",
  "origin",
  "referer",
  "set-cookie",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "via",
]

let methodOverrideHeaders = ["x-http-method", "x-http-method-override", "x-method-override"]

let safelistedContentTypes = [
  "application/x-www-form-urlencoded",
  "multipart/form-data",
  "text/plain",
]

let safelistedResponseHeaders = [
  "cache-control",
  "content-language",
  "content-length",
  "content-type",
  "expires",
  "last-modified",
  "pragma",
]

let forbiddenResponseHeaders = ["set-cookie", "set-cookie2"]

// "*" in Access-Control-Allow-Headers never covers these.
let nonWildcardRequestHeaders = ["authorization"]

let requestBodyHeaders = [
  "content-encoding",
  "content-language",
  "content-location",
  "content-type",
]

let redirectStatuses = [301, 302, 303, 307, 308]

let maxRedirects = 20

let safelistedValueMax = 128

let safelistTotalMax = 1024

// Fetch default when Access-Control-Max-Age is absent or invalid.
let defaultPreflightMaxAge = 5

// Upper bounds each engine applies to Access-Control-Max-Age, in seconds.
let chromiumMaxAgeCap = 7200
let firefoxMaxAgeCap = 86400
let webkitMaxAgeCap = 600

// Hosts under .invalid can never resolve, so probe origins cannot belong to anyone.
let probeHost = "cors-probe.invalid"

// ---------- bindings ----------

@module("node:fs") external readFileSync: (string, string) => string = "readFileSync"
@val @scope("process") external argv: array<string> = "argv"
@val @scope("Buffer") external byteLength: string => int = "byteLength"
let setExitCode: int => unit = %raw("(c) => { process.exitCode = c }")

type url
@new external makeUrl: string => url = "URL"
@new external makeUrlWithBase: (string, string) => url = "URL"
@get external urlOrigin: url => string = "origin"
@get external urlHref: url => string = "href"
@get external urlProtocol: url => string = "protocol"
@get external urlUsername: url => string = "username"
@get external urlPassword: url => string = "password"
@get external urlHostname: url => string = "hostname"
@get external urlPort: url => string = "port"

let parseUrl = (s: string): option<url> =>
  try Some(makeUrl(s)) catch {
  | _ => None
  }

let resolveUrl = (s: string, base: string): option<url> =>
  try Some(makeUrlWithBase(s, base)) catch {
  | _ => None
  }

type abortSignal
@val @scope("AbortSignal") external timeoutSignal: int => abortSignal = "timeout"

type fetchInit = {
  method: string,
  headers: dict<string>,
  redirect: string,
  signal: abortSignal,
}
type fetchHeaders
type fetchResponse
@val external fetch: (string, fetchInit) => promise<fetchResponse> = "fetch"
@get external fetchStatus: fetchResponse => int = "status"
@get external fetchHeaders: fetchResponse => fetchHeaders = "headers"
@send external forEachHeader: (fetchHeaders, (string, string) => unit) => unit = "forEach"
let discardBody: fetchResponse => promise<unit> = %raw(`
  async (r) => { try { if (r.body) await r.body.cancel(); } catch (_) {} }
`)

// ---------- small helpers ----------

let push = (findings: array<finding>, severity, code, message) =>
  if !(findings->Array.some(f => f.code == code && f.message == message)) {
    findings->Array.push({severity, code, message})
  }

let severityName = s =>
  switch s {
  | SevError => "error"
  | SevWarning => "warning"
  | SevInfo => "info"
  }

let credentialsName = c =>
  switch c {
  | CredOmit => "omit"
  | CredSameOrigin => "same-origin"
  | CredInclude => "include"
  }

let parseCredentials = (s: string): option<credentials> =>
  switch s {
  | "omit" => Some(CredOmit)
  | "same-origin" => Some(CredSameOrigin)
  | "include" => Some(CredInclude)
  | _ => None
  }

let codeAt = (s: string, i: int): int => s->String.charCodeAt(i)->Option.getOr(0)

let everyChar = (s: string, pred: int => bool): bool => {
  let ok = ref(true)
  let i = ref(0)
  let n = String.length(s)
  while ok.contents && i.contents < n {
    if !pred(codeAt(s, i.contents)) {
      ok := false
    }
    i := i.contents + 1
  }
  ok.contents
}

let isAlnum = c => (c >= 48 && c <= 57) || c >= 65 && c <= 90 || (c >= 97 && c <= 122)

// RFC 9110 tchar.
let isTokenChar = c => isAlnum(c) || "!#$%&'*+-.^_`|~"->String.includes(String.fromCharCode(c))

let isToken = (s: string) => String.length(s) > 0 && everyChar(s, isTokenChar)

// Fetch: a CORS-unsafe request-header byte.
let isCorsUnsafeByte = c =>
  (c < 0x20 && c != 0x09) ||
  c == 0x22 ||
  c == 0x28 ||
  c == 0x29 ||
  c == 0x3a ||
  c == 0x3c ||
  c == 0x3e ||
  c == 0x3f ||
  c == 0x40 ||
  c == 0x5b ||
  c == 0x5c ||
  c == 0x5d ||
  c == 0x7b ||
  c == 0x7d ||
  c == 0x7f

let isLanguageByte = c =>
  isAlnum(c) ||
  c == 0x20 ||
  c == 0x2a ||
  c == 0x2c ||
  c == 0x2d ||
  c == 0x2e ||
  c == 0x3b ||
  c == 0x3d

let isHttpWhitespace = c => c == 0x20 || c == 0x09 || c == 0x0a || c == 0x0d

let trimHttp = (s: string): string => {
  let n = String.length(s)
  let a = ref(0)
  let b = ref(n)
  while a.contents < n && isHttpWhitespace(codeAt(s, a.contents)) {
    a := a.contents + 1
  }
  while b.contents > a.contents && isHttpWhitespace(codeAt(s, b.contents - 1)) {
    b := b.contents - 1
  }
  s->String.slice(~start=a.contents, ~end=b.contents)
}

let uniqueKeep = (xs: array<string>): array<string> => {
  let out = []
  xs->Array.forEach(x =>
    if !(out->Array.includes(x)) {
      out->Array.push(x)
    }
  )
  out
}

let uniqueSorted = (xs: array<string>): array<string> =>
  uniqueKeep(xs)->Array.toSorted(String.compare)

let quote = s => "\"" ++ s ++ "\""

// Fetch "get": every value for the name, joined with ", ". Duplicates stay visible.
let getHeader = (headers: headerList, name: string): option<string> => {
  let values = headers->Array.filterMap(((n, v)) => n == name ? Some(v) : None)
  Array.length(values) == 0 ? None : Some(values->Array.join(", "))
}

let countHeader = (headers: headerList, name: string) =>
  headers->Array.filter(((n, _)) => n == name)->Array.length

// Split a comma list, trim, drop empty members, require every member to be a token.
let parseTokenList = (value: string): result<array<string>, string> => {
  let items = value->String.split(",")->Array.map(trimHttp)->Array.filter(s => s != "")
  switch items->Array.find(s => !isToken(s)) {
  | Some(bad) => Error(bad)
  | None => Ok(items)
  }
}

let headerTokenList = (headers: headerList, name: string): result<array<string>, string> =>
  switch getHeader(headers, name) {
  | None => Ok([])
  | Some(v) => parseTokenList(v)
  }

let isRedirect = status => redirectStatuses->Array.includes(status)

let originOfUrl = (u: string): string =>
  switch parseUrl(u) {
  | Some(p) => urlOrigin(p)
  | None => "null"
  }

// The opaque origin "null" is never same origin with anything, including itself.
let sameOrigin = (originSer: string, u: string) =>
  originSer != "null" && originOfUrl(u) == originSer

let sameOriginUrls = (a: string, b: string) => {
  let oa = originOfUrl(a)
  oa != "null" && oa == originOfUrl(b)
}

// ---------- request side: what the browser sends ----------

let parseMimeEssence = (value: string): option<string> => {
  let head = switch value->String.indexOf(";") {
  | -1 => value
  | i => value->String.slice(~start=0, ~end=i)
  }
  switch trimHttp(head)->String.split("/") {
  | [t, s] =>
    let s = trimHttp(s)
    isToken(t) && isToken(s) ? Some(String.toLowerCase(t ++ "/" ++ s)) : None
  | _ => None
  }
}

// Fetch "simple range header value", with the safelist rule that suffix ranges are excluded.
let simpleRangeSafelisted = (value: string): bool => {
  let n = String.length(value)
  let pos = ref(0)
  let skipWs = () =>
    while (
      pos.contents < n &&
        (codeAt(value, pos.contents) == 0x20 || codeAt(value, pos.contents) == 0x09)
    ) {
      pos := pos.contents + 1
    }
  let digits = () => {
    let start = pos.contents
    while (
      pos.contents < n && codeAt(value, pos.contents) >= 48 && codeAt(value, pos.contents) <= 57
    ) {
      pos := pos.contents + 1
    }
    value->String.slice(~start, ~end=pos.contents)
  }
  if !(value->String.startsWith("bytes")) {
    false
  } else {
    pos := 5
    skipWs()
    if pos.contents >= n || codeAt(value, pos.contents) != 0x3d {
      false
    } else {
      pos := pos.contents + 1
      skipWs()
      let first = digits()
      skipWs()
      if pos.contents >= n || codeAt(value, pos.contents) != 0x2d {
        false
      } else {
        pos := pos.contents + 1
        skipWs()
        let last = digits()
        pos.contents == n &&
        first != "" &&
        (last == "" ||
          switch (Float.fromString(first), Float.fromString(last)) {
          | (Some(a), Some(b)) => a <= b
          | _ => false
          })
      }
    }
  }
}

// Returns why a header is not CORS safelisted, or None when it is.
let safelistProblem = (name: string, value: string): option<string> =>
  if byteLength(value) > safelistedValueMax {
    Some("value is longer than " ++ Int.toString(safelistedValueMax) ++ " bytes")
  } else {
    switch name {
    | "accept" =>
      everyChar(value, c => !isCorsUnsafeByte(c)) ? None : Some("value contains a CORS unsafe byte")
    | "accept-language" | "content-language" =>
      everyChar(value, isLanguageByte)
        ? None
        : Some("value has bytes outside 0-9 A-Z a-z space * , - . ; =")
    | "content-type" =>
      if !everyChar(value, c => !isCorsUnsafeByte(c)) {
        Some("value contains a CORS unsafe byte")
      } else {
        switch parseMimeEssence(value) {
        | None => Some("value is not a parsable MIME type")
        | Some(e) if safelistedContentTypes->Array.includes(e) => None
        | Some(e) =>
          Some(e ++ " is not application/x-www-form-urlencoded, multipart/form-data or text/plain")
        }
      }
    | "range" =>
      simpleRangeSafelisted(value)
        ? None
        : Some("only a single bytes=N- or bytes=N-M range is safelisted")
    | _ => Some("not a CORS safelisted header")
    }
  }

let isForbiddenRequestHeader = (name: string, value: string) =>
  forbiddenRequestHeaders->Array.includes(name) ||
  name->String.startsWith("proxy-") ||
  name->String.startsWith("sec-") ||
  (methodOverrideHeaders->Array.includes(name) &&
    value
    ->String.split(",")
    ->Array.some(m => forbiddenMethods->Array.includes(String.toUpperCase(trimHttp(m)))))

let hasBadValueByte = (v: string) => !everyChar(v, c => c != 0x00 && c != 0x0a && c != 0x0d)

let analyzeRequest = (c: caseInput, findings: array<finding>): result<analyzed, string> => {
  let upper = String.toUpperCase(c.method)
  if !isToken(c.method) {
    let m = "method " ++ quote(c.method) ++ " is not a valid HTTP token, fetch() throws TypeError"
    push(findings, SevError, "R006", m)
    Error(m)
  } else if forbiddenMethods->Array.includes(upper) {
    let m =
      "method " ++ upper ++ " is forbidden, fetch() throws TypeError before any network activity"
    push(findings, SevError, "R001", m)
    Error(m)
  } else {
    let method = normalizedMethods->Array.includes(upper) ? upper : c.method
    if method != upper {
      push(
        findings,
        SevWarning,
        "R002",
        "method " ++
        quote(method) ++
        " is sent exactly as written because fetch() only uppercases DELETE, GET, HEAD, OPTIONS, POST and PUT. " ++
        "Access-Control-Allow-Methods is matched case sensitively, so an allow list with " ++
        upper ++ " will not match",
      )
    }
    let bad = c.headers->Array.find(((n, v)) => !isToken(n) || hasBadValueByte(v))
    switch bad {
    | Some((n, _)) =>
      let m =
        "header " ++
        quote(n) ++ " has an invalid name or a NUL, CR or LF in its value, fetch() throws TypeError"
      push(findings, SevError, "R006", m)
      Error(m)
    | None =>
      let kept = []
      c.headers->Array.forEach(((n, v)) => {
        let v = trimHttp(v)
        if isForbiddenRequestHeader(n, v) {
          push(
            findings,
            SevInfo,
            "R003",
            "header " ++
            n ++ " is a forbidden request header, the browser silently drops the value your code sets",
          )
        } else {
          kept->Array.push((n, v))
        }
      })
      let reasons = []
      let unsafe = switch c.observedPreflight {
      | Some((_, names)) =>
        reasons->Array.push("the recorded preflight asked for it")
        names->Array.map(String.toLowerCase)->uniqueSorted
      | None =>
        let unsafe = []
        let potentially = []
        let total = ref(0)
        kept->Array.forEach(((n, v)) =>
          switch safelistProblem(n, v) {
          | Some(why) =>
            unsafe->Array.push(n)
            reasons->Array.push("header " ++ n ++ ": " ++ why)
          | None =>
            potentially->Array.push(n)
            total := total.contents + byteLength(v)
          }
        )
        if total.contents > safelistTotalMax && Array.length(potentially) > 0 {
          push(
            findings,
            SevWarning,
            "R005",
            "safelisted header values add up to " ++
            Int.toString(total.contents) ++
            " bytes, over the " ++
            Int.toString(
              safelistTotalMax,
            ) ++ " byte limit, so every one of them now needs a preflight",
          )
          potentially->Array.forEach(n => {
            unsafe->Array.push(n)
            reasons->Array.push("header " ++ n ++ ": safelisted values exceed the combined limit")
          })
        }
        uniqueSorted(unsafe)
      }
      let method = switch c.observedPreflight {
      | Some((m, _)) => m
      | None => method
      }
      if !(safelistedMethods->Array.includes(method)) {
        reasons->Array.unshift("method " ++ method ++ " is not GET, HEAD or POST")
      }
      Ok({method, headers: kept, unsafe, reasons: uniqueKeep(reasons)})
    }
  }
}

let needsPreflight = (method: string, unsafe: array<string>) =>
  !(safelistedMethods->Array.includes(method)) || Array.length(unsafe) > 0

// ---------- response side: what the browser checks ----------

let diagnoseOrigin = (acao: string, originSer: string, cred: credentials): string =>
  if acao == "*" && cred == CredInclude {
    "Access-Control-Allow-Origin is * but the request is credentialed, and * is ignored for credentialed requests. " ++ "Echo the exact origin and send Access-Control-Allow-Credentials: true"
  } else if acao->String.includes(",") {
    "Access-Control-Allow-Origin carries more than one value (" ++
    acao ++ "). Browsers need exactly one. Usually two layers, such as the app and a proxy or CDN, both add it"
  } else if originSer == "null" {
    "the request origin serializes as null (opaque document, sandboxed frame or a cross origin redirect) but the server answered " ++
    quote(acao)
  } else if (
    acao->String.endsWith("/") &&
      acao->String.slice(~start=0, ~end=String.length(acao) - 1) == originSer
  ) {
    "Access-Control-Allow-Origin has a trailing slash. It must be the bare origin " ++ originSer
  } else if String.toLowerCase(acao) == originSer {
    "Access-Control-Allow-Origin differs from " ++
    originSer ++ " only by letter case, and the match is byte for byte"
  } else {
    switch parseUrl(acao) {
    | Some(p) if urlOrigin(p) == originSer =>
      "Access-Control-Allow-Origin " ++
      quote(acao) ++
      " names the right site but is not the serialized origin. Send exactly " ++
      originSer
    | Some(p)
      if urlHostname(p) == originOfUrl(originSer)->parseUrl->Option.mapOr("", urlHostname) =>
      "Access-Control-Allow-Origin " ++
      quote(acao) ++
      " has the right host but a different scheme or port than " ++
      originSer
    | _ => "the server allows " ++ quote(acao) ++ " but the request origin is " ++ originSer
    }
  }

// Fetch "CORS check".
let corsCheck = (headers: headerList, originSer: string, cred: credentials): result<unit, string> =>
  switch getHeader(headers, "access-control-allow-origin") {
  | None => Error("no Access-Control-Allow-Origin header")
  | Some(acao) =>
    if cred != CredInclude && acao == "*" {
      Ok()
    } else if acao != originSer {
      Error(diagnoseOrigin(acao, originSer, cred))
    } else if cred != CredInclude {
      Ok()
    } else {
      switch getHeader(headers, "access-control-allow-credentials") {
      | Some("true") => Ok()
      | None =>
        Error(
          "the request is credentialed but Access-Control-Allow-Credentials is missing. It must be exactly true",
        )
      | Some(v) =>
        Error(
          "Access-Control-Allow-Credentials is " ++
          quote(v) ++ ". It must be exactly true, lowercase, one value",
        )
      }
    }
  }

// Config hazards that matter even when this particular request passes.
let lintCorsHeaders = (headers: headerList, ~isPreflight: bool, findings: array<finding>) => {
  let where = isPreflight ? "preflight response" : "response"
  let acao = getHeader(headers, "access-control-allow-origin")
  let acac = getHeader(headers, "access-control-allow-credentials")
  if (
    countHeader(headers, "access-control-allow-origin") > 1 ||
      acao->Option.mapOr(false, v => v->String.includes(","))
  ) {
    push(
      findings,
      SevError,
      "H001",
      where ++ " sends Access-Control-Allow-Origin more than once. Every browser rejects that, whatever the values",
    )
  }
  switch acao {
  | Some("null") =>
    push(
      findings,
      acac == Some("true") ? SevError : SevWarning,
      "H002",
      where ++
      " grants the origin null. Any sandboxed iframe, data: URL or local file can claim that origin" ++ (
        acac == Some("true") ? ", and credentials are allowed too" : ""
      ),
    )
  | _ => ()
  }
  switch acao {
  | Some(v) if v != "*" =>
    let vary = getHeader(headers, "vary")->Option.getOr("")->String.toLowerCase
    let members = vary->String.split(",")->Array.map(trimHttp)
    if !(members->Array.includes("origin")) && !(members->Array.includes("*")) {
      push(
        findings,
        isPreflight ? SevInfo : SevWarning,
        "H003",
        where ++
        " sets Access-Control-Allow-Origin to a specific origin without Vary: Origin. " ++ "A shared cache or CDN can hand this response, and its CORS grant, to a different origin",
      )
    }
  | _ => ()
  }
  switch acac {
  | Some(v) if v != "true" =>
    push(
      findings,
      SevWarning,
      "H004",
      where ++
      " sends Access-Control-Allow-Credentials: " ++
      v ++ ". Only the exact value true has any effect",
    )
  | _ => ()
  }
  if acao == Some("*") && acac == Some("true") {
    push(
      findings,
      SevWarning,
      "H005",
      where ++ " pairs Access-Control-Allow-Origin: * with credentials true. Credentialed requests always fail against that pairing",
    )
  }
}

let statusHint = (status: int): string =>
  if status == 401 || status == 403 {
    ". Preflights never carry cookies or Authorization, so an auth layer that runs before CORS handling rejects them. Answer OPTIONS before auth"
  } else if status == 404 || status == 405 || status == 501 {
    ". The route has no OPTIONS handler. Add one or let the CORS middleware answer OPTIONS for every path"
  } else if status >= 500 {
    ". The server failed on OPTIONS, often because a handler tried to parse a body that preflights never send"
  } else {
    ""
  }

let maxAgeWindow = (headers: headerList, findings: array<finding>): cacheWindow => {
  let declared = switch getHeader(headers, "access-control-max-age") {
  | None =>
    push(
      findings,
      SevInfo,
      "P008",
      "no Access-Control-Max-Age, so browsers cache this preflight for " ++
      Int.toString(
        defaultPreflightMaxAge,
      ) ++ " seconds and nearly every call pays an extra round trip",
    )
    None
  | Some(v) =>
    let v = trimHttp(v)
    if v != "" && everyChar(v, c => c >= 48 && c <= 57) {
      switch Int.fromString(v) {
      | Some(n) => Some(n)
      | None => Some(firefoxMaxAgeCap * 10)
      }
    } else {
      push(
        findings,
        SevWarning,
        "P008",
        "Access-Control-Max-Age " ++
        quote(v) ++
        " is not a non negative integer, browsers fall back to " ++
        Int.toString(defaultPreflightMaxAge) ++ " seconds",
      )
      None
    }
  }
  let base = declared->Option.getOr(defaultPreflightMaxAge)
  let w = {
    declared,
    chromium: Math.Int.min(base, chromiumMaxAgeCap),
    firefox: Math.Int.min(base, firefoxMaxAgeCap),
    webkit: Math.Int.min(base, webkitMaxAgeCap),
  }
  switch declared {
  | Some(n) if n > webkitMaxAgeCap =>
    push(
      findings,
      SevInfo,
      "P008",
      "Access-Control-Max-Age " ++
      Int.toString(n) ++
      " is capped: Chromium keeps " ++
      Int.toString(w.chromium) ++
      "s, Firefox " ++
      Int.toString(w.firefox) ++
      "s, WebKit " ++
      Int.toString(w.webkit) ++ "s",
    )
  | _ => ()
  }
  w
}

// Fetch "CORS-preflight fetch", steps after the response arrives. Collects every
// problem so one run shows the whole fix, and returns the first as the block reason.
let evalPreflight = (
  resp: httpResponse,
  ~originSer: string,
  ~cred: credentials,
  ~method: string,
  ~unsafe: array<string>,
  ~allNames: array<string>,
  findings: array<finding>,
): result<cacheWindow, string> => {
  let problems = []
  let fail = (code, message) => {
    push(findings, SevError, code, message)
    problems->Array.push(message)
  }
  let status = Int.toString(resp.status)
  if isRedirect(resp.status) {
    fail(
      "P001",
      "preflight answered " ++
      status ++ ". Preflights never follow redirects. Serve OPTIONS on the final URL, or fix the trailing slash or http to https hop",
    )
  } else if resp.status < 200 || resp.status > 299 {
    fail(
      "P001",
      "preflight answered " ++
      status ++
      ", and only 200 to 299 counts as ok" ++
      statusHint(resp.status),
    )
  }
  lintCorsHeaders(resp.headers, ~isPreflight=true, findings)

  // A failed status is the whole story; header checks on a 401 page are noise.
  if Array.length(problems) == 0 {
    switch corsCheck(resp.headers, originSer, cred) {
    | Error(why) => fail("P002", "preflight CORS check failed: " ++ why)
    | Ok() => ()
    }
    let credentialed = cred == CredInclude
    switch (
      headerTokenList(resp.headers, "access-control-allow-methods"),
      headerTokenList(resp.headers, "access-control-allow-headers"),
    ) {
    | (Error(bad), _) =>
      fail(
        "P006",
        "Access-Control-Allow-Methods has " ++
        quote(bad) ++ " which is not a method token, so the whole preflight is a network error",
      )
    | (_, Error(bad)) =>
      fail(
        "P006",
        "Access-Control-Allow-Headers has " ++
        quote(bad) ++ " which is not a header name, so the whole preflight is a network error",
      )
    | (Ok(methods), Ok(names)) =>
      let lowerNames = names->Array.map(String.toLowerCase)
      let methodOk =
        safelistedMethods->Array.includes(method) ||
        methods->Array.includes(method) ||
        (!credentialed && methods->Array.includes("*"))
      if !methodOk {
        let detail = if Array.length(methods) == 0 {
          "there is no Access-Control-Allow-Methods"
        } else if methods->Array.includes("*") && credentialed {
          "the only match would be *, and * is a literal string for credentialed requests"
        } else if methods->Array.some(m => String.toUpperCase(m) == String.toUpperCase(method)) {
          "the allow list has it in different letter case and the comparison is case sensitive"
        } else {
          "the allow list is " ++ methods->Array.join(", ")
        }
        fail("P003", "method " ++ method ++ " is not allowed: " ++ detail)
      }
      if credentialed && (methods->Array.includes("*") || lowerNames->Array.includes("*")) {
        push(
          findings,
          SevWarning,
          "P007",
          "the preflight uses * in an allow list for a credentialed request, where it only matches a method or header literally named *",
        )
      }
      let authMissing =
        allNames->Array.filter(n =>
          nonWildcardRequestHeaders->Array.includes(n) && !(lowerNames->Array.includes(n))
        )
      authMissing->Array.forEach(n =>
        fail(
          "P005",
          "header " ++
          n ++
          " must be listed by name in Access-Control-Allow-Headers" ++ (
            lowerNames->Array.includes("*") ? ". The * wildcard never covers it" : ""
          ),
        )
      )
      let missing =
        unsafe->Array.filter(n =>
          !(authMissing->Array.includes(n)) &&
          !(lowerNames->Array.includes(n)) &&
          (credentialed || !(lowerNames->Array.includes("*")))
        )
      if Array.length(missing) > 0 {
        fail(
          "P004",
          "Access-Control-Allow-Headers does not cover " ++
          missing->Array.join(", ") ++ (
            Array.length(names) == 0
              ? ", and the header is absent"
              : ". It lists " ++ names->Array.join(", ")
          ),
        )
      }
    }
  }
  switch problems[0] {
  | Some(first) => Error(first)
  | None => Ok(maxAgeWindow(resp.headers, findings))
  }
}

// Which response headers a script can read through response.headers.get().
let exposure = (
  resp: httpResponse,
  cred: credentials,
  readHeaders: array<string>,
  findings: array<finding>,
) => {
  let exposeList = switch headerTokenList(resp.headers, "access-control-expose-headers") {
  | Ok(xs) => xs->Array.map(String.toLowerCase)
  | Error(bad) =>
    push(
      findings,
      SevWarning,
      "A004",
      "Access-Control-Expose-Headers has " ++
      quote(bad) ++ " which is not a header name, so browsers expose nothing extra",
    )
    []
  }
  let wildcard = exposeList->Array.includes("*")
  if wildcard && cred == CredInclude {
    push(
      findings,
      SevInfo,
      "A003",
      "Access-Control-Expose-Headers: * is literal for credentialed requests, so it exposes nothing. List the names",
    )
  }
  let canRead = (n: string) =>
    !(forbiddenResponseHeaders->Array.includes(n)) &&
    (safelistedResponseHeaders->Array.includes(n) ||
    exposeList->Array.includes(n) ||
    (wildcard && cred != CredInclude))
  let names =
    resp.headers
    ->Array.map(((n, _)) => n)
    ->Array.filter(n => !(n->String.startsWith("access-control-")))
    ->uniqueSorted
  let readable = names->Array.filter(canRead)
  let hidden = names->Array.filter(n => !canRead(n))
  readHeaders->Array.forEach(r => {
    let r = String.toLowerCase(r)
    if !canRead(r) {
      push(
        findings,
        SevWarning,
        "A002",
        "your code reads " ++
        r ++
        " but the browser hides it and headers.get() returns null" ++ (
          forbiddenResponseHeaders->Array.includes(r)
            ? ". Set-Cookie is never readable from script"
            : ". Add it to Access-Control-Expose-Headers"
        ),
      )
    }
  })
  (readable, hidden)
}

// ---------- the simulation ----------

let findPreflight = (c: caseInput, u: string): option<httpResponse> => {
  let norm = (s: string) => parseUrl(s)->Option.mapOr(s, urlHref)
  c.preflights->Array.find(((k, _)) => norm(k) == norm(u))->Option.map(((_, r)) => r)
}

let simulate = (c: caseInput): caseReport => {
  let findings = []
  let caches = []
  let readable = ref([])
  let hidden = ref([])
  let acrm = ref(None)
  let acrh = ref(None)
  let reasons = ref([])
  let sentMethod = ref(c.method)
  let originSer = ref(c.origin)
  let outcome = switch parseUrl(c.url) {
  | None => ScriptError("URL " ++ quote(c.url) ++ " does not parse, fetch() throws TypeError")
  | Some(u) if urlUsername(u) != "" || urlPassword(u) != "" =>
    let m = "the URL contains a username or password, fetch() throws TypeError"
    push(findings, SevError, "R008", m)
    ScriptError(m)
  | Some(u) if urlProtocol(u) != "http:" && urlProtocol(u) != "https:" =>
    NotEvaluated("scheme " ++ urlProtocol(u) ++ " does not go through the CORS protocol")
  | Some(u) =>
    let originParsed = if c.origin == "null" {
      Some("null")
    } else {
      parseUrl(c.origin)->Option.map(urlOrigin)
    }
    switch originParsed {
    | None => NotEvaluated("origin " ++ quote(c.origin) ++ " does not parse")
    | Some(ser) =>
      if ser != c.origin {
        push(
          findings,
          SevInfo,
          "R007",
          "origin " ++
          quote(c.origin) ++
          " serializes as " ++
          ser ++ ", which is what the browser sends",
        )
      }
      originSer := ser
      switch analyzeRequest(c, findings) {
      | Error(m) => ScriptError(m)
      | Ok(a) =>
        reasons := a.reasons
        sentMethod := a.method
        let current = ref(urlHref(u))
        let method = ref(a.method)
        let headers = ref(a.headers)
        let unsafe = ref(a.unsafe)
        let tainted = ref(false)
        let corsMode = ref(false)
        let preflighted = []
        let hop = ref(0)
        let result = ref(None)
        let hops = Array.length(c.redirects)
        while Option.isNone(result.contents) {
          let i = hop.contents
          let resp = i < hops ? c.redirects[i] : c.response
          let originNow = tainted.contents ? "null" : ser
          if tainted.contents || !sameOrigin(ser, current.contents) {
            corsMode := true
          }
          let needs = corsMode.contents && needsPreflight(method.contents, unsafe.contents)
          if needs && !(preflighted->Array.includes(current.contents)) {
            if Option.isNone(acrm.contents) {
              acrm := Some(method.contents)
              acrh := (
                  Array.length(unsafe.contents) > 0 ? Some(unsafe.contents->Array.join(",")) : None
                )
            }
            switch findPreflight(c, current.contents) {
            | None =>
              result :=
                Some(
                  NotEvaluated(
                    "a preflight to " ++
                    current.contents ++ " is required but no preflight response was supplied",
                  ),
                )
            | Some(p) if p.status == 0 =>
              result :=
                Some(
                  NotEvaluated(
                    "the preflight has status 0, it was blocked or aborted before headers were recorded",
                  ),
                )
            | Some(p) =>
              let allNames = headers.contents->Array.map(((n, _)) => n)
              let allNames = Array.concat(allNames, unsafe.contents)->uniqueSorted
              switch evalPreflight(
                p,
                ~originSer=originNow,
                ~cred=c.credentials,
                ~method=method.contents,
                ~unsafe=unsafe.contents,
                ~allNames,
                findings,
              ) {
              | Error(why) => result := Some(Blocked(AtPreflight(current.contents), why))
              | Ok(w) =>
                caches->Array.push((current.contents, w))
                preflighted->Array.push(current.contents)
              }
            }
          }
          if Option.isNone(result.contents) {
            switch resp {
            | None =>
              result := Some(needs ? PreflightPassed : NotEvaluated("no response was supplied"))
            | Some(r) if r.status == 0 =>
              result :=
                Some(
                  NotEvaluated(
                    "the response has status 0, it was blocked or aborted before headers were recorded",
                  ),
                )
            | Some(r) =>
              if corsMode.contents {
                lintCorsHeaders(r.headers, ~isPreflight=false, findings)
                switch corsCheck(r.headers, originNow, c.credentials) {
                | Error(why) =>
                  if i < hops {
                    push(
                      findings,
                      SevError,
                      "D001",
                      "redirect " ++ Int.toString(i + 1) ++ " failed the CORS check: " ++ why,
                    )
                    result := Some(Blocked(AtRedirect(i), why))
                  } else {
                    push(findings, SevError, "A001", "response CORS check failed: " ++ why)
                    result := Some(Blocked(AtResponse, why))
                  }
                | Ok() => ()
                }
              }
              if Option.isNone(result.contents) {
                if i < hops {
                  let label = "redirect " ++ Int.toString(i + 1)
                  switch getHeader(r.headers, "location") {
                  | _ if !isRedirect(r.status) =>
                    result :=
                      Some(
                        NotEvaluated(
                          label ++
                          " has status " ++
                          Int.toString(r.status) ++ ", which is not a redirect",
                        ),
                      )
                  | None => result := Some(NotEvaluated(label ++ " has no Location header"))
                  | Some(loc) =>
                    switch resolveUrl(loc, current.contents) {
                    | None =>
                      push(
                        findings,
                        SevError,
                        "D005",
                        label ++ " Location " ++ quote(loc) ++ " does not parse",
                      )
                      result := Some(Blocked(AtRedirect(i), "Location does not parse"))
                    | Some(next) if urlProtocol(next) != "http:" && urlProtocol(next) != "https:" =>
                      push(findings, SevError, "D005", label ++ " points at a non HTTP scheme")
                      result := Some(Blocked(AtRedirect(i), "redirect to a non HTTP scheme"))
                    | Some(_) if i + 1 > maxRedirects =>
                      push(
                        findings,
                        SevError,
                        "D005",
                        "more than " ++ Int.toString(maxRedirects) ++ " redirects",
                      )
                      result := Some(Blocked(AtRedirect(i), "too many redirects"))
                    | Some(next)
                      if corsMode.contents &&
                      (urlUsername(next) != "" || urlPassword(next) != "") &&
                      !sameOrigin(ser, urlHref(next)) =>
                      let m =
                        label ++ " goes to a URL with embedded credentials, which CORS requests refuse"
                      push(findings, SevError, "D002", m)
                      result := Some(Blocked(AtRedirect(i), m))
                    | Some(next) =>
                      let nextHref = urlHref(next)
                      if (
                        !tainted.contents &&
                        !sameOriginUrls(current.contents, nextHref) &&
                        !sameOrigin(ser, current.contents)
                      ) {
                        tainted := true
                        push(
                          findings,
                          SevInfo,
                          "D003",
                          label ++
                          " moves a cross origin request to another origin, so from here on the browser sends Origin: null " ++ "and every later response must allow null or *",
                        )
                      }
                      let s = r.status
                      if (
                        ((s == 301 || s == 302) && method.contents == "POST") ||
                          (s == 303 && method.contents != "GET" && method.contents != "HEAD")
                      ) {
                        push(
                          findings,
                          SevInfo,
                          "D004",
                          label ++
                          " (" ++
                          Int.toString(s) ++
                          ") turns " ++
                          method.contents ++ " into GET and drops the body headers",
                        )
                        method := "GET"
                        headers :=
                          headers.contents->Array.filter(((n, _)) =>
                            !(requestBodyHeaders->Array.includes(n))
                          )
                        unsafe :=
                          unsafe.contents->Array.filter(n =>
                            !(requestBodyHeaders->Array.includes(n))
                          )
                      }
                      current := nextHref
                    }
                  }
                } else if corsMode.contents {
                  let (rd, hd) = exposure(r, c.credentials, c.readHeaders, findings)
                  readable := rd
                  hidden := hd
                  result := Some(Allowed)
                } else {
                  result := Some(SameOrigin)
                }
              }
            }
          }
          hop := i + 1
        }
        result.contents->Option.getOr(NotEvaluated("no result"))
      }
    }
  }
  let expectationMet = c.expect->Option.map(e =>
    switch (e, outcome) {
    | (ExpectAllow, Allowed | SameOrigin) => true
    | (ExpectAllow, PreflightPassed) => Option.isNone(c.response)
    | (ExpectBlock, Blocked(_) | ScriptError(_)) => true
    | _ => false
    }
  )
  switch expectationMet {
  | Some(false) =>
    push(
      findings,
      SevError,
      "E001",
      "expected " ++
      (c.expect == Some(ExpectAllow) ? "allow" : "block") ++ " but the browser would not do that",
    )
  | _ => ()
  }
  {
    name: c.name,
    method: sentMethod.contents,
    url: c.url,
    origin: originSer.contents,
    credentials: c.credentials,
    preflightReasons: Option.isSome(acrm.contents) ? reasons.contents : [],
    acrm: acrm.contents,
    acrh: acrh.contents,
    outcome,
    findings,
    caches,
    readable: readable.contents,
    hidden: hidden.contents,
    expectationMet,
  }
}

// ---------- input decoding ----------

let fieldString = (d: dict<JSON.t>, key: string, ctx: string): option<string> =>
  switch d->Dict.get(key) {
  | None | Some(Null) => None
  | Some(String(s)) => Some(s)
  | Some(_) => throw(InputError(ctx ++ "." ++ key ++ " must be a string"))
  }

let decodeHeaders = (j: option<JSON.t>, ctx: string): headerList =>
  switch j {
  | None | Some(Null) => []
  | Some(Object(d)) =>
    d
    ->Dict.toArray
    ->Array.flatMap(((k, v)) =>
      switch v {
      | String(s) => [(String.toLowerCase(k), s)]
      | Number(n) => [(String.toLowerCase(k), Float.toString(n))]
      | Array(vs) =>
        vs->Array.map(x =>
          switch x {
          | String(s) => (String.toLowerCase(k), s)
          | _ => throw(InputError(ctx ++ "." ++ k ++ " must hold strings"))
          }
        )
      | _ => throw(InputError(ctx ++ "." ++ k ++ " must be a string or a list of strings"))
      }
    )
  | Some(Array(items)) =>
    items->Array.map(item =>
      switch item {
      | Array([String(n), String(v)]) => (String.toLowerCase(n), v)
      | Object(o) =>
        switch (o->Dict.get("name"), o->Dict.get("value")) {
        | (Some(String(n)), Some(String(v))) => (String.toLowerCase(n), v)
        | _ => throw(InputError(ctx ++ " entries need string name and value"))
        }
      | _ => throw(InputError(ctx ++ " entries must be [name, value] or {name, value}"))
      }
    )
  | Some(_) => throw(InputError(ctx ++ " must be an object or a list"))
  }

let decodeResponse = (j: JSON.t, ctx: string): httpResponse =>
  switch j {
  | Object(d) =>
    let status = switch d->Dict.get("status") {
    | None => 200
    | Some(Number(n)) => Float.toInt(n)
    | Some(_) => throw(InputError(ctx ++ ".status must be a number"))
    }
    {status, headers: decodeHeaders(d->Dict.get("headers"), ctx ++ ".headers")}
  | _ => throw(InputError(ctx ++ " must be an object"))
  }

let decodeCase = (j: JSON.t, idx: int, defaults: dict<JSON.t>): caseInput => {
  let ctx = "cases[" ++ Int.toString(idx) ++ "]"
  switch j {
  | Object(d) =>
    let get = (k: string) =>
      switch fieldString(d, k, ctx) {
      | Some(s) => Some(s)
      | None => fieldString(defaults, k, "defaults")
      }
    let url = switch get("url") {
    | Some(u) => u
    | None => throw(InputError(ctx ++ " needs a url"))
    }
    let origin = switch get("origin") {
    | Some(o) => o
    | None => throw(InputError(ctx ++ " needs an origin, the page that makes the request"))
    }
    let credentials = switch get("credentials") {
    | None => CredSameOrigin
    | Some(s) =>
      switch parseCredentials(s) {
      | Some(c) => c
      | None => throw(InputError(ctx ++ ".credentials must be omit, same-origin or include"))
      }
    }
    let preflights = []
    switch d->Dict.get("preflight") {
    | None | Some(Null) => ()
    | Some(p) => preflights->Array.push((url, decodeResponse(p, ctx ++ ".preflight")))
    }
    switch d->Dict.get("preflights") {
    | Some(Object(m)) =>
      m
      ->Dict.toArray
      ->Array.forEach(((k, p)) =>
        preflights->Array.push((k, decodeResponse(p, ctx ++ ".preflights." ++ k)))
      )
    | None | Some(Null) => ()
    | Some(_) => throw(InputError(ctx ++ ".preflights must map URLs to responses"))
    }
    let redirects = switch d->Dict.get("redirects") {
    | None | Some(Null) => []
    | Some(Array(xs)) =>
      xs->Array.mapWithIndex((x, k) =>
        decodeResponse(x, ctx ++ ".redirects[" ++ Int.toString(k) ++ "]")
      )
    | Some(_) => throw(InputError(ctx ++ ".redirects must be a list"))
    }
    let readHeaders = switch d->Dict.get("readHeaders") {
    | None | Some(Null) => []
    | Some(Array(xs)) =>
      xs->Array.map(x =>
        switch x {
        | String(s) => s
        | _ => throw(InputError(ctx ++ ".readHeaders must hold strings"))
        }
      )
    | Some(_) => throw(InputError(ctx ++ ".readHeaders must be a list"))
    }
    let expect = switch fieldString(d, "expect", ctx) {
    | None => None
    | Some("allow") => Some(ExpectAllow)
    | Some("block") => Some(ExpectBlock)
    | Some(_) => throw(InputError(ctx ++ ".expect must be allow or block"))
    }
    let defaultHeaders = decodeHeaders(defaults->Dict.get("headers"), "defaults.headers")
    let ownHeaders = decodeHeaders(d->Dict.get("headers"), ctx ++ ".headers")
    let ownNames = ownHeaders->Array.map(((n, _)) => n)
    {
      name: get("name")->Option.getOr(ctx),
      url,
      origin,
      method: get("method")->Option.getOr("GET"),
      headers: Array.concat(
        defaultHeaders->Array.filter(((n, _)) => !(ownNames->Array.includes(n))),
        ownHeaders,
      ),
      credentials,
      readHeaders,
      observedPreflight: None,
      preflights,
      redirects,
      response: switch d->Dict.get("response") {
      | None | Some(Null) => None
      | Some(r) => Some(decodeResponse(r, ctx ++ ".response"))
      },
      expect,
    }
  | _ => throw(InputError(ctx ++ " must be an object"))
  }
}

let decodeCasesFile = (j: JSON.t): array<caseInput> =>
  switch j {
  | Array(xs) => xs->Array.mapWithIndex((x, i) => decodeCase(x, i, Dict.make()))
  | Object(d) =>
    let defaults = switch d->Dict.get("defaults") {
    | Some(Object(o)) => o
    | None | Some(Null) => Dict.make()
    | Some(_) => throw(InputError("defaults must be an object"))
    }
    switch d->Dict.get("cases") {
    | Some(Array(xs)) => xs->Array.mapWithIndex((x, i) => decodeCase(x, i, defaults))
    | _ => throw(InputError("the file needs a cases list"))
    }
  | _ => throw(InputError("the file must be a list of cases or an object with cases"))
  }

// Headers a browser adds on its own that are neither forbidden nor safelisted.
// Without a recorded preflight they would look script set and fake a preflight.
let browserAddedHeaders = ["user-agent", "cache-control", "pragma", "priority"]

type harEntry = {
  hMethod: string,
  hUrl: string,
  hHeaders: headerList,
  hResponse: httpResponse,
}

let decodeHar = (j: JSON.t, defaultCred: credentials): array<caseInput> => {
  let entries = switch j {
  | Object(root) =>
    switch root->Dict.get("log") {
    | Some(Object(log)) =>
      switch log->Dict.get("entries") {
      | Some(Array(xs)) => xs
      | _ => throw(InputError("HAR log.entries is missing"))
      }
    | _ => throw(InputError("HAR log is missing"))
    }
  | _ => throw(InputError("a HAR file is a JSON object with a log"))
  }
  let parsed = entries->Array.mapWithIndex((e, i) => {
    let ctx = "log.entries[" ++ Int.toString(i) ++ "]"
    switch e {
    | Object(d) =>
      switch (d->Dict.get("request"), d->Dict.get("response")) {
      | (Some(Object(rq)), Some(rs)) => {
          hMethod: fieldString(rq, "method", ctx ++ ".request")->Option.getOr("GET"),
          hUrl: fieldString(rq, "url", ctx ++ ".request")->Option.getOr(""),
          hHeaders: decodeHeaders(
            rq->Dict.get("headers"),
            ctx ++ ".request.headers",
          )->Array.filter(((n, _)) => !(n->String.startsWith(":"))),
          hResponse: {
            let r = decodeResponse(rs, ctx ++ ".response")
            {...r, headers: r.headers->Array.filter(((n, _)) => !(n->String.startsWith(":")))}
          },
        }
      | _ => throw(InputError(ctx ++ " needs request and response"))
      }
    | _ => throw(InputError(ctx ++ " must be an object"))
    }
  })
  let isPreflight = (e: harEntry) =>
    e.hMethod == "OPTIONS" && Option.isSome(getHeader(e.hHeaders, "access-control-request-method"))
  let used = []
  let cases = []
  parsed->Array.forEachWithIndex((e, i) =>
    switch getHeader(e.hHeaders, "origin") {
    | Some(origin) if !isPreflight(e) =>
      let cred = Option.isSome(getHeader(e.hHeaders, "cookie")) ? CredInclude : defaultCred
      let pre = ref(None)
      let k = ref(i - 1)
      while Option.isNone(pre.contents) && k.contents >= 0 {
        switch parsed[k.contents] {
        | Some(p)
          if isPreflight(p) &&
          !(used->Array.includes(k.contents)) &&
          p.hUrl == e.hUrl &&
          getHeader(p.hHeaders, "origin") == Some(origin) &&
          getHeader(p.hHeaders, "access-control-request-method") == Some(e.hMethod) =>
          pre := Some((k.contents, p))
        | _ => ()
        }
        k := k.contents - 1
      }
      let observed = pre.contents->Option.map(((idx, p)) => {
        used->Array.push(idx)
        let names = switch getHeader(p.hHeaders, "access-control-request-headers") {
        | Some(v) => parseTokenList(v)->Result.getOr([])
        | None => []
        }
        (e.hMethod, names)
      })
      cases->Array.push({
        name: "#" ++ Int.toString(i) ++ " " ++ e.hMethod ++ " " ++ e.hUrl,
        url: e.hUrl,
        origin,
        method: e.hMethod,
        headers: e.hHeaders->Array.filter(((n, v)) =>
          !(browserAddedHeaders->Array.includes(n)) && !isForbiddenRequestHeader(n, v)
        ),
        credentials: cred,
        readHeaders: [],
        observedPreflight: observed,
        preflights: pre.contents->Option.mapOr([], ((_, p)) => [(e.hUrl, p.hResponse)]),
        redirects: [],
        response: Some(e.hResponse),
        expect: None,
      })
    | _ => ()
    }
  )
  // A preflight with no matching request usually means the preflight failed and
  // the browser never sent the real call. Those are the interesting ones.
  parsed->Array.forEachWithIndex((p, i) =>
    if isPreflight(p) && !(used->Array.includes(i)) {
      let method = getHeader(p.hHeaders, "access-control-request-method")->Option.getOr("GET")
      let names = switch getHeader(p.hHeaders, "access-control-request-headers") {
      | Some(v) => parseTokenList(v)->Result.getOr([])
      | None => []
      }
      cases->Array.push({
        name: "#" ++
        Int.toString(i) ++
        " preflight for " ++
        method ++
        " " ++
        p.hUrl ++ " (no request followed)",
        url: p.hUrl,
        origin: getHeader(p.hHeaders, "origin")->Option.getOr("null"),
        method,
        headers: [],
        credentials: defaultCred,
        readHeaders: [],
        observedPreflight: Some((method, names)),
        preflights: [(p.hUrl, p.hResponse)],
        redirects: [],
        response: None,
        expect: None,
      })
    }
  )
  cases
}

// ---------- output ----------

let stageName = s =>
  switch s {
  | AtPreflight(_) => "preflight"
  | AtRedirect(i) => "redirect " ++ Int.toString(i + 1)
  | AtResponse => "response"
  }

let outcomeLabel = o =>
  switch o {
  | SameOrigin => "SAME ORIGIN, CORS does not apply"
  | Allowed => "ALLOWED"
  | PreflightPassed => "PREFLIGHT PASSED, actual response not supplied"
  | Blocked(s, why) => "BLOCKED at " ++ stageName(s) ++ ": " ++ why
  | ScriptError(why) => "THROWS in script: " ++ why
  | NotEvaluated(why) => "NOT EVALUATED: " ++ why
  }

let outcomeCode = o =>
  switch o {
  | SameOrigin => "same-origin"
  | Allowed => "allowed"
  | PreflightPassed => "preflight-passed"
  | Blocked(_) => "blocked"
  | ScriptError(_) => "script-error"
  | NotEvaluated(_) => "not-evaluated"
  }

let printReport = (r: caseReport) => {
  Console.log("case " ++ quote(r.name))
  Console.log("  " ++ r.method ++ " " ++ r.url)
  Console.log("  origin " ++ r.origin ++ ", credentials " ++ credentialsName(r.credentials))
  if Array.length(r.preflightReasons) > 0 {
    Console.log("  preflight required because:")
    r.preflightReasons->Array.forEach(x => Console.log("    - " ++ x))
  } else if Option.isNone(r.acrm) {
    Console.log("  no preflight needed")
  }
  r.acrm->Option.forEach(m => Console.log("  Access-Control-Request-Method: " ++ m))
  r.acrh->Option.forEach(h => Console.log("  Access-Control-Request-Headers: " ++ h))
  Console.log("  result " ++ outcomeLabel(r.outcome))
  r.caches->Array.forEach(((u, w)) =>
    Console.log(
      "  preflight cache for " ++
      u ++
      ": Chromium " ++
      Int.toString(w.chromium) ++
      "s, Firefox " ++
      Int.toString(w.firefox) ++
      "s, WebKit " ++
      Int.toString(w.webkit) ++ "s",
    )
  )
  if Array.length(r.hidden) > 0 {
    Console.log("  hidden from script: " ++ r.hidden->Array.join(", "))
  }
  r.findings->Array.forEach(f =>
    Console.log("  [" ++ severityName(f.severity) ++ " " ++ f.code ++ "] " ++ f.message)
  )
  Console.log("")
}

let strArr = (xs: array<string>): JSON.t => JSON.Encode.array(xs->Array.map(JSON.Encode.string))

let optStr = (o: option<string>): JSON.t => o->Option.mapOr(JSON.Encode.null, JSON.Encode.string)

let reportJson = (r: caseReport): JSON.t => {
  let reason = switch r.outcome {
  | Blocked(_, w) | ScriptError(w) | NotEvaluated(w) => JSON.Encode.string(w)
  | _ => JSON.Encode.null
  }
  let stage = switch r.outcome {
  | Blocked(s, _) => JSON.Encode.string(stageName(s))
  | _ => JSON.Encode.null
  }
  JSON.Encode.object(
    Dict.fromArray([
      ("name", JSON.Encode.string(r.name)),
      ("method", JSON.Encode.string(r.method)),
      ("url", JSON.Encode.string(r.url)),
      ("origin", JSON.Encode.string(r.origin)),
      ("credentials", JSON.Encode.string(credentialsName(r.credentials))),
      ("outcome", JSON.Encode.string(outcomeCode(r.outcome))),
      ("stage", stage),
      ("reason", reason),
      ("preflightReasons", strArr(r.preflightReasons)),
      ("accessControlRequestMethod", optStr(r.acrm)),
      ("accessControlRequestHeaders", optStr(r.acrh)),
      (
        "preflightCache",
        JSON.Encode.array(
          r.caches->Array.map(((u, w)) =>
            JSON.Encode.object(
              Dict.fromArray([
                ("url", JSON.Encode.string(u)),
                ("declared", w.declared->Option.mapOr(JSON.Encode.null, n => JSON.Encode.int(n))),
                ("chromium", JSON.Encode.int(w.chromium)),
                ("firefox", JSON.Encode.int(w.firefox)),
                ("webkit", JSON.Encode.int(w.webkit)),
              ]),
            )
          ),
        ),
      ),
      ("readableHeaders", strArr(r.readable)),
      ("hiddenHeaders", strArr(r.hidden)),
      ("expectationMet", r.expectationMet->Option.mapOr(JSON.Encode.null, JSON.Encode.bool)),
      (
        "findings",
        JSON.Encode.array(
          r.findings->Array.map(f =>
            JSON.Encode.object(
              Dict.fromArray([
                ("severity", JSON.Encode.string(severityName(f.severity))),
                ("code", JSON.Encode.string(f.code)),
                ("message", JSON.Encode.string(f.message)),
              ]),
            )
          ),
        ),
      ),
    ]),
  )
}

// A case fails the run when its expectation is wrong, when it has no expectation and
// the browser would block it, or when it carries an error level hazard.
let caseFails = (r: caseReport, strict: bool) => {
  let hazard =
    r.findings->Array.some(f =>
      (f.severity == SevError &&
        (f.code->String.startsWith("H") || f.code->String.startsWith("X") || f.code == "E001")) ||
        (strict && f.severity == SevWarning)
    )
  let unexpectedBlock = switch (r.expectationMet, r.outcome) {
  | (None, Blocked(_) | ScriptError(_)) => true
  | _ => false
  }
  hazard || unexpectedBlock || r.expectationMet == Some(false)
}

// ---------- live probe ----------

type probeVariant = {label: string, probeOrigin: string, trusted: bool, subdomain: bool}

let isIpHost = (h: string) =>
  h->String.includes(":") || everyChar(h, c => (c >= 48 && c <= 57) || c == 0x2e)

// Origins that a broken allow list tends to accept: substring checks, unanchored
// regexes, unescaped dots, scheme or port blind comparisons and null.
let probeOrigins = (trusted: string): array<probeVariant> =>
  switch parseUrl(trusted) {
  | None => throw(InputError("--origin " ++ quote(trusted) ++ " does not parse as an origin"))
  | Some(t) =>
    let scheme = urlProtocol(t)
    let host = urlHostname(t)
    let port = urlPort(t)
    let withPort = h => scheme ++ "//" ++ h ++ (port == "" ? "" : ":" ++ port)
    let variants = [
      {label: "trusted", probeOrigin: urlOrigin(t), trusted: true, subdomain: false},
      {label: "unrelated", probeOrigin: "https://" ++ probeHost, trusted: false, subdomain: false},
      {label: "null", probeOrigin: "null", trusted: false, subdomain: false},
      {
        label: "suffix",
        probeOrigin: withPort(host ++ "." ++ probeHost),
        trusted: false,
        subdomain: false,
      },
      {
        label: "prefix",
        probeOrigin: withPort("corsprobe" ++ host),
        trusted: false,
        subdomain: false,
      },
      {
        label: "subdomain",
        probeOrigin: withPort("corsprobe." ++ host),
        trusted: false,
        subdomain: true,
      },
    ]
    if !isIpHost(host) && host->String.includes(".") {
      let i = host->String.indexOf(".")
      let dotted = host->String.slice(~start=0, ~end=i) ++ "x" ++ host->String.slice(~start=i + 1)
      variants->Array.push({
        label: "unescaped-dot",
        probeOrigin: withPort(dotted),
        trusted: false,
        subdomain: false,
      })
    }
    if scheme == "https:" {
      variants->Array.push({
        label: "http-downgrade",
        probeOrigin: "http://" ++ host ++ (port == "" ? "" : ":" ++ port),
        trusted: false,
        subdomain: false,
      })
    }
    if port == "" {
      variants->Array.push({
        label: "other-port",
        probeOrigin: withPort(host) ++ ":8443",
        trusted: false,
        subdomain: false,
      })
    }
    variants
  }

let errorText: JsExn.t => string = %raw(`
  (e) => {
    const c = e && e.cause;
    if (c && (c.code || c.message)) return (c.code ? c.code + " " : "") + (c.message || "");
    return (e && e.name === "TimeoutError") ? "timed out" : String((e && e.message) || e);
  }
`)

let sendProbe = async (url: string, method: string, headers: dict<string>, timeoutMs: int): result<
  httpResponse,
  string,
> =>
  try {
    let r = await fetch(
      url,
      {method, headers, redirect: "manual", signal: timeoutSignal(timeoutMs)},
    )
    let hs = []
    fetchHeaders(r)->forEachHeader((v, k) => hs->Array.push((String.toLowerCase(k), v)))
    await discardBody(r)
    Ok({status: fetchStatus(r), headers: hs})
  } catch {
  | JsExn(e) => Error(errorText(e))
  }

type probeResult = {variant: probeVariant, verdict: string, report: option<caseReport>}

let runProbe = async (
  url: string,
  ~trusted: string,
  ~method: string,
  ~headers: headerList,
  ~cred: credentials,
  ~sendActual: bool,
  ~timeoutMs: int,
): array<probeResult> => {
  let template = {
    name: "probe",
    url,
    origin: trusted,
    method,
    headers,
    credentials: cred,
    readHeaders: [],
    observedPreflight: None,
    preflights: [],
    redirects: [],
    response: None,
    expect: None,
  }
  let a = switch analyzeRequest(template, []) {
  | Ok(a) => a
  | Error(m) => throw(InputError(m))
  }
  let needs = needsPreflight(a.method, a.unsafe)
  let safeToSend = a.method == "GET" || a.method == "HEAD"
  let results = []
  let variants = probeOrigins(trusted)
  for i in 0 to Array.length(variants) - 1 {
    let v = variants->Array.getUnsafe(i)
    let pre = if needs {
      let h = Dict.fromArray([
        ("origin", v.probeOrigin),
        ("access-control-request-method", a.method),
      ])
      if Array.length(a.unsafe) > 0 {
        h->Dict.set("access-control-request-headers", a.unsafe->Array.join(","))
      }
      Some(await sendProbe(url, "OPTIONS", h, timeoutMs))
    } else {
      None
    }
    let actual = if !needs || safeToSend || sendActual {
      let h = Dict.fromArray(a.headers)
      h->Dict.set("origin", v.probeOrigin)
      Some(await sendProbe(url, a.method, h, timeoutMs))
    } else {
      None
    }
    let failure = switch (pre, actual) {
    | (Some(Error(e)), _) | (_, Some(Error(e))) => Some(e)
    | _ => None
    }
    switch failure {
    | Some(e) => results->Array.push({variant: v, verdict: "no answer: " ++ e, report: None})
    | None =>
      let preResp = switch pre {
      | Some(Ok(p)) => Some(p)
      | _ => None
      }
      let actResp = switch actual {
      | Some(Ok(r)) => Some(r)
      | _ => None
      }
      let report = simulate({
        ...template,
        name: v.label,
        origin: v.probeOrigin,
        preflights: preResp->Option.mapOr([], p => [(url, p)]),
        response: actResp,
      })
      let last = switch (actResp, preResp) {
      | (Some(r), _) | (None, Some(r)) => r.headers
      | (None, None) => []
      }
      let acao = getHeader(last, "access-control-allow-origin")
      let credGrant =
        acao == Some(v.probeOrigin) &&
          getHeader(last, "access-control-allow-credentials") == Some("true")
      let granted = switch report.outcome {
      | Allowed | PreflightPassed => true
      | _ => false
      }
      let verdict = if v.trusted {
        if granted {
          "allowed"
        } else {
          push(
            report.findings,
            SevError,
            "X004",
            "the trusted origin " ++
            v.probeOrigin ++
            " is refused: " ++
            outcomeLabel(report.outcome),
          )
          "refused"
        }
      } else if acao == Some(v.probeOrigin) {
        if v.subdomain {
          push(
            report.findings,
            credGrant ? SevWarning : SevInfo,
            "X003",
            "any subdomain is accepted" ++ (
              credGrant
                ? " with credentials, so one XSS or takeover on any subdomain reads authenticated data"
                : ""
            ),
          )
          credGrant ? "subdomain allowed with credentials" : "subdomain allowed"
        } else if credGrant {
          push(
            report.findings,
            SevError,
            "X001",
            "the server reflects the untrusted origin " ++
            v.probeOrigin ++ " with Access-Control-Allow-Credentials: true. A page there can read authenticated responses",
          )
          "REFLECTED with credentials"
        } else {
          push(
            report.findings,
            SevWarning,
            "X002",
            "the server reflects the untrusted origin " ++
            v.probeOrigin ++ ". Without credentials the damage is limited to what anonymous callers can see",
          )
          "reflected without credentials"
        }
      } else if acao == Some("*") {
        push(
          report.findings,
          SevInfo,
          "X006",
          "the server answers * to every origin, which suits public data only",
        )
        "wildcard"
      } else {
        "refused"
      }
      results->Array.push({variant: v, verdict, report: Some(report)})
    }
  }
  results
}

// ---------- CLI ----------

let usage = `usage:
  node CorsPreflightSimulator.bs.js simulate <cases.json> [--json] [--strict]
  node CorsPreflightSimulator.bs.js har <capture.har> [--credentials omit|same-origin|include] [--all] [--json] [--strict]
  node CorsPreflightSimulator.bs.js probe <url> --origin <trusted origin> [--method M] [--header "Name: value"]...
        [--credentials mode] [--send-actual] [--timeout ms] [--json] [--strict]

exit codes: 0 every case behaves as expected, 1 a case fails or a hazard was found, 2 bad input`

type cliOptions = {
  positional: array<string>,
  asJson: bool,
  strict: bool,
  all: bool,
  sendActual: bool,
  credentials: option<credentials>,
  origin: option<string>,
  method: string,
  headers: headerList,
  timeoutMs: int,
}

let parseArgs = (args: array<string>): cliOptions => {
  let positional = []
  let asJson = ref(false)
  let strict = ref(false)
  let all = ref(false)
  let sendActual = ref(false)
  let credentials = ref(None)
  let origin = ref(None)
  let method = ref("GET")
  let headers = []
  let timeoutMs = ref(10000)
  let i = ref(0)
  let value = flag => {
    i := i.contents + 1
    switch args[i.contents] {
    | Some(v) => v
    | None => throw(InputError(flag ++ " needs a value"))
    }
  }
  while i.contents < Array.length(args) {
    switch args->Array.getUnsafe(i.contents) {
    | "--json" => asJson := true
    | "--strict" => strict := true
    | "--all" => all := true
    | "--send-actual" => sendActual := true
    | "--credentials" =>
      credentials :=
        switch parseCredentials(value("--credentials")) {
        | Some(c) => Some(c)
        | None => throw(InputError("--credentials must be omit, same-origin or include"))
        }
    | "--origin" => origin := Some(value("--origin"))
    | "--method" => method := value("--method")
    | "--header" =>
      let h = value("--header")
      switch h->String.indexOf(":") {
      | -1 | 0 => throw(InputError("--header must look like \"Name: value\""))
      | k =>
        headers->Array.push((
          String.toLowerCase(trimHttp(h->String.slice(~start=0, ~end=k))),
          trimHttp(h->String.slice(~start=k + 1)),
        ))
      }
    | "--timeout" =>
      timeoutMs :=
        switch Int.fromString(value("--timeout")) {
        | Some(n) if n > 0 => n
        | _ => throw(InputError("--timeout must be a positive number of milliseconds"))
        }
    | a if a->String.startsWith("--") => throw(InputError("unknown flag " ++ a))
    | a => positional->Array.push(a)
    }
    i := i.contents + 1
  }
  {
    positional,
    asJson: asJson.contents,
    strict: strict.contents,
    all: all.contents,
    sendActual: sendActual.contents,
    credentials: credentials.contents,
    origin: origin.contents,
    method: method.contents,
    headers,
    timeoutMs: timeoutMs.contents,
  }
}

let readJsonFile = (path: string): JSON.t => {
  let text = try readFileSync(path, "utf8") catch {
  | JsExn(e) =>
    throw(
      InputError("cannot read " ++ path ++ ": " ++ JsExn.message(e)->Option.getOr("unknown error")),
    )
  }
  try JSON.parseOrThrow(text) catch {
  | JsExn(e) =>
    throw(
      InputError(path ++ " is not valid JSON: " ++ JsExn.message(e)->Option.getOr("parse error")),
    )
  }
}

let summarize = (reports: array<caseReport>, strict: bool) => {
  let count = pred => reports->Array.filter(r => pred(r.outcome))->Array.length
  let failing = reports->Array.filter(r => caseFails(r, strict))->Array.length
  let summary = [
    ("cases", Array.length(reports)),
    ("allowed", count(o => o == Allowed)),
    ("preflightPassed", count(o => o == PreflightPassed)),
    (
      "blocked",
      count(o =>
        switch o {
        | Blocked(_) | ScriptError(_) => true
        | _ => false
        }
      ),
    ),
    (
      "notEvaluated",
      count(o =>
        switch o {
        | NotEvaluated(_) => true
        | _ => false
        }
      ),
    ),
    ("sameOrigin", count(o => o == SameOrigin)),
    ("failing", failing),
  ]
  (summary, failing)
}

let emitCases = (reports: array<caseReport>, ~asJson, ~strict, ~all) => {
  let (summary, failing) = summarize(reports, strict)
  let shown = all ? reports : reports->Array.filter(r => r.outcome != SameOrigin)
  if asJson {
    Console.log(
      JSON.stringifyAny(
        JSON.Encode.object(
          Dict.fromArray([
            (
              "summary",
              JSON.Encode.object(
                Dict.fromArray(summary->Array.map(((k, n)) => (k, JSON.Encode.int(n)))),
              ),
            ),
            ("cases", JSON.Encode.array(shown->Array.map(reportJson))),
          ]),
        ),
        ~space=2,
      )->Option.getOr("{}"),
    )
  } else {
    shown->Array.forEach(printReport)
    Console.log(summary->Array.map(((k, n)) => k ++ " " ++ Int.toString(n))->Array.join(", "))
  }
  setExitCode(failing > 0 ? 1 : 0)
}

let emitProbe = (results: array<probeResult>, ~asJson, ~strict) => {
  let failing = results->Array.filter(p =>
    switch p.report {
    | Some(r) =>
      r.findings->Array.some(f =>
        (f.code->String.startsWith("X") || f.code->String.startsWith("H")) &&
          (f.severity == SevError || (strict && f.severity == SevWarning))
      )
    | None => strict || p.variant.trusted
    }
  )
  if asJson {
    Console.log(
      JSON.stringifyAny(
        JSON.Encode.array(
          results->Array.map(p =>
            JSON.Encode.object(
              Dict.fromArray([
                ("variant", JSON.Encode.string(p.variant.label)),
                ("origin", JSON.Encode.string(p.variant.probeOrigin)),
                ("verdict", JSON.Encode.string(p.verdict)),
                ("report", p.report->Option.mapOr(JSON.Encode.null, reportJson)),
              ]),
            )
          ),
        ),
        ~space=2,
      )->Option.getOr("[]"),
    )
  } else {
    results->Array.forEach(p => {
      Console.log(
        p.variant.label->String.padEnd(15, " ") ++
        p.variant.probeOrigin->String.padEnd(52, " ") ++
        p.verdict,
      )
      p.report->Option.forEach(r =>
        r.findings->Array.forEach(
          f =>
            if (
              p.variant.trusted ||
              (f.severity != SevInfo &&
                (f.code->String.startsWith("X") || f.code->String.startsWith("H")))
            ) {
              Console.log("    [" ++ severityName(f.severity) ++ " " ++ f.code ++ "] " ++ f.message)
            },
        )
      )
    })
    Console.log(
      Int.toString(Array.length(failing)) ++
      " of " ++
      Int.toString(Array.length(results)) ++ " probes failing",
    )
  }
  setExitCode(Array.length(failing) > 0 ? 1 : 0)
}

let fail = (message: string) => {
  Console.error("error: " ++ message)
  Console.error(usage)
  setExitCode(2)
}

let main = async () => {
  let args = argv->Array.slice(~start=2)
  try {
    let o = parseArgs(args->Array.slice(~start=1))
    switch (args[0], o.positional) {
    | (Some("simulate"), [path]) =>
      let reports = readJsonFile(path)->decodeCasesFile->Array.map(simulate)
      emitCases(reports, ~asJson=o.asJson, ~strict=o.strict, ~all=true)
    | (Some("har"), [path]) =>
      let cases = decodeHar(readJsonFile(path), o.credentials->Option.getOr(CredSameOrigin))
      emitCases(cases->Array.map(simulate), ~asJson=o.asJson, ~strict=o.strict, ~all=o.all)
    | (Some("probe"), [url]) =>
      switch o.origin {
      | None => fail("probe needs --origin, the page origin you trust")
      | Some(trusted) =>
        let results = await runProbe(
          url,
          ~trusted,
          ~method=o.method,
          ~headers=o.headers,
          ~cred=o.credentials->Option.getOr(CredInclude),
          ~sendActual=o.sendActual,
          ~timeoutMs=o.timeoutMs,
        )
        emitProbe(results, ~asJson=o.asJson, ~strict=o.strict)
      }
    | (Some("help" | "--help" | "-h"), _) => Console.log(usage)
    | _ => fail("expected a command and exactly one input")
    }
  } catch {
  | InputError(m) => fail(m)
  }
}

main()->ignore
