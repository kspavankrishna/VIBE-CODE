# CORS Preflight Simulator

A browser says "blocked by CORS policy" and tells you almost nothing else. This ReScript CLI runs the actual Fetch standard CORS algorithm offline against your request and your server's headers, names the exact rule that fails and probes a live API for broken origin allow lists.

**Language:** ReScript | **Lines:** 2166 | **Added:** 2026-11-28

## What this solves

CORS errors are some of the most common and most wasted hours in web development. The browser console gives you one line. The server logs show a 200 or a 204. Nobody can see the decision because it happens inside the browser after the response arrives.

The rules behind that decision are precise but spread across the Fetch standard and they contain traps that look fine in code review:

- `fetch(url, {method: "patch"})` sends the method in lowercase. Only DELETE, GET, HEAD, OPTIONS, POST and PUT are uppercased. `Access-Control-Allow-Methods: PATCH` is then compared byte for byte and does not match.
- `*` in `Access-Control-Allow-Origin`, `Access-Control-Allow-Methods`, `Access-Control-Allow-Headers` or `Access-Control-Expose-Headers` is a literal string once credentials are included.
- `Authorization` is never covered by `Access-Control-Allow-Headers: *`, even without credentials.
- `Content-Type: application/json` always triggers a preflight. So does a quote character inside `text/plain; charset="utf-8"`.
- Preflights never carry cookies or tokens. An auth middleware that runs before the CORS middleware answers 401 and the real call never leaves the browser.
- Preflights never follow redirects. A trailing slash rewrite or an http to https hop on OPTIONS kills the request.
- A cross origin redirect to a third origin turns the request origin into `null`. The next server must allow `null` or `*`.
- Two layers adding `Access-Control-Allow-Origin` (the app and a CDN, say) produce two values and every browser rejects that.
- A reflected origin without `Vary: Origin` lets a shared cache hand one site's CORS grant to another.
- Your code reads `response.headers.get("x-request-id")` and gets `null` because the header was never exposed.

This tool models all of that. You give it a request and the responses. It tells you what the browser would send, whether a preflight is needed and why, what `Access-Control-Request-Method` and `Access-Control-Request-Headers` will be, where the request is blocked, how long each engine caches the preflight and which response headers your script can actually read.

## Why I built it

I was debugging a CORS failure on an upload endpoint that only failed in production. Locally it worked. The difference turned out to be a CDN that added its own `Access-Control-Allow-Origin` on top of the app. In another project the PATCH endpoint failed only from one client because a developer wrote the method in lowercase. Each of these took hours because the only feedback loop was a browser console and a redeploy.

I wanted three things. First, a simulator that follows the spec exactly so I could paste a request and response and get a precise answer. Second, a way to replay a HAR file exported from DevTools so a failing session from a user could be explained offline. Third, a live probe that tests the origin allow list with the origins that broken regexes tend to accept, because a misconfigured CORS policy with credentials is a real data exposure bug and not just an annoyance.

ReScript suits this. The outcome is a variant: `SameOrigin`, `Allowed`, `PreflightPassed`, `Blocked(stage, reason)`, `ScriptError(reason)` or `NotEvaluated(reason)`. The stage is `AtPreflight(url)`, `AtRedirect(n)` or `AtResponse`. The compiler forces every path to handle every case. It compiles to a single CommonJS file with no runtime dependencies.

## When to use it

- When a browser reports a CORS error and you want the precise failing rule in plain words.
- As CORS contract tests in CI. Each case can carry `"expect": "allow"` or `"expect": "block"` and the run exits 1 when the browser would disagree.
- To explain a HAR file from a user or a QA session without reproducing their environment.
- Before shipping an API gateway or CDN change, to confirm the CORS headers still survive every layer.
- As part of a security review, with `probe`, to check whether the server reflects untrusted origins with credentials.
- To tune `Access-Control-Max-Age` once you see how each engine caps it.

It does not replace a browser. It does not model Private Network Access or Local Network Access permission prompts, which are still moving in Chromium. It does not model cookies or SameSite rules. It only tells you whether CORS lets the response through.

## How it works

**Request side.** `analyzeRequest` follows what `fetch()` does before anything touches the network. A method listed in `forbiddenMethods` (CONNECT, TRACE, TRACK) throws, which becomes finding R001. A method is uppercased only when it appears in `normalizedMethods`. Anything else stays as written and raises R002 when it has lowercase letters. Headers in `forbiddenRequestHeaders`, any `proxy-` or `sec-` header and method override headers carrying a forbidden method are dropped with R003, the same way a browser silently drops them.

Every remaining header goes through `safelistProblem`. It applies the spec rules for `accept`, `accept-language`, `content-language`, `content-type` and `range`: the `isCorsUnsafeByte` byte set, the `isLanguageByte` set, a real MIME essence parse in `parseMimeEssence` against `safelistedContentTypes`, the single range rule in `simpleRangeSafelisted` and the `safelistedValueMax` limit of 128 bytes. If the safelisted values together exceed `safelistTotalMax` (1024 bytes) they all become unsafe, which is R005. The unsafe names are lowercased and sorted exactly as the browser puts them into `Access-Control-Request-Headers`. `needsPreflight` is true when the method is outside `safelistedMethods` or any unsafe name exists.

**Preflight.** `evalPreflight` mirrors the CORS preflight fetch. A redirect status or anything outside 200 to 299 fails with P001 and `statusHint` explains the usual cause: auth before CORS for 401 and 403, a missing OPTIONS route for 404, 405 and 501, a body parser for 5xx. Then `corsCheck` runs the spec CORS check, `parseTokenList` reads the allow lists and fails the whole preflight on a member that is not a token (P006). The method rule (P003) and the header rule (P004) respect the credentials mode. Names in `nonWildcardRequestHeaders` must be listed by name (P005). All problems are collected so one run shows the full fix. The first becomes the block reason.

`maxAgeWindow` reads `Access-Control-Max-Age`, falls back to `defaultPreflightMaxAge` (5 seconds) and applies `chromiumMaxAgeCap` (7200), `firefoxMaxAgeCap` (86400) and `webkitMaxAgeCap` (600).

**Origin mismatch diagnosis.** When the origin check fails, `diagnoseOrigin` names the near miss: a wildcard with credentials, more than one value, a trailing slash, letter case, a path or explicit default port, a scheme or port mismatch or a `null` request origin.

**Redirects.** `simulate` walks every hop in `redirects`. Each hop must pass the CORS check once the request is cross origin (D001). Location URLs with embedded credentials fail (D002). A hop to a third origin sets the tainted flag and from then on the origin is `null` (D003). 301 and 302 on POST and 303 on anything except GET or HEAD rewrite the method to GET and drop `requestBodyHeaders` (D004). More than `maxRedirects` hops fail (D005). A hop to a new URL that still needs a preflight looks it up in `preflights`, just as a browser preflights the new URL.

**Response.** `exposure` decides which headers script can read from `safelistedResponseHeaders`, `Access-Control-Expose-Headers` and the credentials mode. `forbiddenResponseHeaders` stay hidden always. Every name in `readHeaders` that script cannot read is A002.

**Hazards.** `lintCorsHeaders` flags configuration problems even when the request passes: duplicate origin headers (H001), a granted `null` origin (H002), a specific origin without `Vary: Origin` (H003), a credentials value other than `true` (H004) and `*` paired with credentials (H005).

**HAR replay.** `decodeHar` turns every entry with an `Origin` header into a case. It pairs each request with the most recent unused OPTIONS entry for the same URL, origin and method and uses that recorded `Access-Control-Request-Headers` as the truth, since a HAR cannot tell which headers your script set. Headers in `browserAddedHeaders` and forbidden headers are dropped. A `Cookie` header marks the case as credentialed. An OPTIONS entry with no request after it becomes its own case, because that is usually the preflight that failed.

**Live probe.** `probeOrigins` builds variants from your trusted origin: the origin itself, an unrelated host under `probeHost` (`cors-probe.invalid`, which can never resolve), `null`, a suffix trick, a prefix trick, a subdomain, an unescaped dot, an http downgrade and another port. `runProbe` sends a preflight for each one through `sendProbe` with a timeout and manual redirects. It sends the real request only for GET and HEAD unless you pass `--send-actual`, so it never fires a PUT or DELETE at your API by accident. A reflected untrusted origin with credentials is X001. Without credentials it is X002. An accepted subdomain is X003. A refused trusted origin is X004. A wildcard answer is X006.

**Exit codes.** `caseFails` marks a case as failing when its expectation is wrong (E001), when it has no expectation and the browser would block it or when it carries an error level hazard. `--strict` also fails on warnings. Exit 0 means everything behaved, 1 means something failed, 2 means bad input.

## Usage

Build once. The folder has a `rescript.json` for in source CommonJS output. Node 20 or newer runs the result.

```sh
npm install rescript@12
npx rescript build
```

Run the bundled examples. Exit 1 is expected here because one case deliberately models an auth layer that rejects the preflight.

```sh
node CorsPreflightSimulator.bs.js simulate ExampleCases.json
```

A cases file is a list or an object with `defaults` and `cases`. Each case takes `url`, `origin`, `method`, `headers`, `credentials` (`omit`, `same-origin` or `include`, default `same-origin` like `fetch()`), `readHeaders`, `preflight`, `preflights` (a map from URL to response), `redirects`, `response` and `expect`. Headers can be an object, a list of `[name, value]` pairs or a list of `{name, value}` objects. Use the list forms to model duplicate headers.

```json
{
  "name": "upload",
  "url": "https://api.example.com/v1/files/42",
  "origin": "https://app.example.com",
  "method": "PUT",
  "credentials": "include",
  "headers": {"content-type": "application/json"},
  "preflight": {
    "status": 204,
    "headers": {
      "access-control-allow-origin": "https://app.example.com",
      "access-control-allow-credentials": "true",
      "access-control-allow-methods": "PUT",
      "access-control-allow-headers": "content-type",
      "access-control-max-age": "600",
      "vary": "Origin"
    }
  },
  "expect": "allow"
}
```

Replay a HAR exported from DevTools. Same origin entries are hidden unless you pass `--all`.

```sh
node CorsPreflightSimulator.bs.js har session.har --credentials include
```

Probe a live API for origin allow list bugs, as JSON for a pipeline:

```sh
node CorsPreflightSimulator.bs.js probe https://api.example.com/v1/me --origin https://app.example.com \
  --header "Authorization: Bearer placeholder" --timeout 5000 --json
```

Fail CI on warnings too:

```sh
node CorsPreflightSimulator.bs.js simulate cors-contract.json --strict
```

## Notes

- Only probe servers you own or are allowed to test. Probes send real requests with spoofed `Origin` headers.
- Probes go out from Node, which has no cookie jar. The verdict on credentials comes from the headers the server returns, which is what a browser would act on.
- Engine caps for `Access-Control-Max-Age` are current as of late 2026. Chromium also partitions its preflight cache by top level site, so a cached preflight on one site does not help another.
- `PreflightPassed` means the preflight cleared and no actual response was supplied. With `"expect": "allow"` that counts as a pass only when the case has no `response`.
- Finding codes are stable, so you can grep for them or filter them in the JSON output.
- The ReScript source passes the compiler with every warning enabled except the few listed in `rescript.json`.
