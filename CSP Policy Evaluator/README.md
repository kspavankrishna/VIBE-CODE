# CSP Policy Evaluator

A Content Security Policy evaluator in pure Elm that follows the CSP Level 3 matching rules the way a browser does. It tells you whether a script, frame, eval or inline handler would actually be allowed across every policy on the page, then audits the whole set for the mistakes that quietly turn a CSP into decoration.

**Language:** Elm | **Lines:** 3184 | **Added:** 2026-11-26

## What this solves

Most teams ship a Content Security Policy once, see the browser console go quiet and never look at it again. The header looks strict. It often is not. The problems I keep finding in real policies are not exotic, they are small and silent:

- A missing semicolon. `script-src 'self' object-src 'none'` reads fine to a human. The browser sees a host called `object-src` inside `script-src` and no object-src directive at all.
- `self` written without quotes, which becomes a host name that matches nothing you meant.
- `'unsafe-inline'` next to a nonce, where people assume it weakens the policy. It does not, the nonce switches it off. The reverse mistake is more common: `'unsafe-inline'` with no nonce, which allows every injected `<script>`.
- `https:` or `*` in script-src, which allows script from any host an attacker can register.
- A public CDN in script-src, such as cdn.jsdelivr.net or unpkg.com. The same goes for a shared hosting suffix like `*.amazonaws.com` or `*.vercel.app`. Anyone can publish a file there, so the allowlist is a bypass.
- `'strict-dynamic'` without a nonce, which blocks every script and breaks the page.
- No `base-uri`, so an injected `<base href>` repoints every relative script, nonced ones included.
- `frame-ancestors` in a meta tag, where browsers ignore it.
- A report only policy with no `report-uri` or `report-to`, which reports to nobody.
- Two enforced policies from two layers of the stack (an app and a CDN edge rule) that each look fine but together block the nonced scripts the app depends on.

On top of the audit, the module answers the question people actually ask during a migration: "if I ship this policy, will this specific URL or inline block still run?" It evaluates a load, an inline script, an event handler, a `javascript:` URL, eval, WebAssembly compile, a form submit, a `<base>` href or a framing origin against every enforced and report only policy at once. It also says which directive decided it and why.

## Why I built it

I wanted a CSP checker I could run in CI on every deploy and trust to agree with Chrome and Firefox. The online evaluators are useful for a quick look but they check one policy string. They do not resolve fallback the way the spec does and they do not answer "is this URL allowed". Writing small regex checks in a pipeline is worse. CSP matching has real rules: scheme upgrades, wildcard subdomains that do not match the bare domain, directory paths that stop counting after a redirect, ports that default per scheme, nonces that apply to elements but not to attributes, hashes that only cover handlers when `'unsafe-hashes'` is present. Get one of those wrong and your checker either cries wolf or misses the hole.

I picked Elm on purpose. The whole evaluator is pure: strings in, decisions out, no clock and no IO. The type system makes the source expression grammar explicit as a `Source` union. Every case of it is handled and the compiler refuses to build if one is missed. The same module runs in a browser dashboard, in a node worker for CI or inside an Elm admin tool.

## When to use it

- In CI, after a deploy to staging: fetch the real response headers and meta tags, feed them in and fail the build on high findings.
- During a move from an allowlist policy to nonces with `'strict-dynamic'`: ship the new policy as report only, list the scripts your page loads as checks and see which ones the new policy would block before you enforce it.
- When a CDN, reverse proxy or framework adds its own CSP header next to yours. The evaluator treats them as separate policies, as browsers do. Then it finds the conflicts.
- In a security review, to explain why a specific payload would or would not run under a given policy, with the deciding directive named.
- In an internal Elm or JavaScript tool that shows teams their CSP posture.

It is not a crawler and it does not fetch anything. You give it the headers. That keeps it deterministic and easy to test.

## How it works

The code lives in `CspPolicyEvaluator.elm`. `CspPolicyEvaluatorCli.elm` is a headless worker around it. `CspPolicyEvaluatorSelfCheck.elm` runs 40 scenario checks.

**Parsing.** `parseHeader` takes a header value and a `Disposition` (`Enforce` or `ReportOnly`) and splits it on commas, because one header line can carry several policies. `parseMeta` takes the content of a meta tag and never splits on commas, which matches the spec; a comma there is flagged as `comma-in-meta`. Each policy is split on semicolons, directive names are lower cased. The first copy of a duplicate directive wins. Every source token goes through `parseSource`, which returns a `Source`: keywords like `KwSelf` and `KwStrictDynamic`, `Nonce`, `Hash` with `Sha256`, `Sha384` or `Sha512`, `SchemeSource`, `HostExpr` with a `HostSource` record, `Wildcard` or `Invalid` with a reason. Parse time findings include `missing-semicolon`, `unquoted-keyword`, `unknown-directive` with a "did you mean" built from an edit distance over `knownDirectives`, `removed-directive`, `none-with-sources`, `weak-nonce` (shorter than `minNonceChars`, which is 22 base64 characters or 128 bits) and `meta-ignored-directive`.

**Matching.** `matchesSource` implements the CSP3 algorithm. `schemePartMatch` lets `http` cover `https` and `ws` cover `wss`. `hostPartMatch` treats `*.example.com` as subdomains only, never the bare domain. `portPartMatch` uses the scheme default when no port is written and accepts `:*`. `pathPartMatch` treats a path ending in `/` as a directory and anything else as an exact file, with percent decoding. The path is skipped when the request was redirected. `selfMatch` allows the same host on an upgraded scheme with default ports. `Wildcard` matches http, https and the page's own scheme, so `*` never matches `data:` or `blob:`.

**Fallback.** `fallbackChain` lists what a browser consults for each directive: `script-src-elem` falls back to `script-src` and then `default-src`, `worker-src` goes through `child-src` and `script-src`, `frame-src` through `child-src`. `base-uri`, `form-action` and `frame-ancestors` have no fallback, so `default-src 'none'` does not restrict forms. `effectiveDirective` applies the chain and skips directives a meta policy cannot use.

**Evaluation.** `evaluate` takes the page URL, the list of policies and a `Check`. It resolves relative URLs with `resolveUrl`, applies `upgrade-insecure-requests` from any enforced policy and runs `verdictFor` on each policy. For script loads `loadDecision` checks the nonce first, then `'strict-dynamic'` (parser inserted scripts are blocked, script inserted ones are trusted), then the URL. `inlineDecision` follows the spec order for inline content: `'unsafe-inline'` only counts when there is no nonce, hash or `'strict-dynamic'` in the list. Nonces apply to elements only. Hashes apply to handlers and `javascript:` URLs only with `'unsafe-hashes'`. Digest values are compared after `normalizeBase64`, so base64url and padding differences do not matter. The result is a `Decision` with `allowed`, the effective URL, `blockedBy`, `reportedBy` and one `Verdict` per policy carrying the deciding directive and a reason.

**Audit.** `audit` takes a `Config` (`bypassHosts` and `failOn`), the page URL and the policies. `analyseScripts` gives each policy a `ScriptPosture`: `Unrestricted`, `InlineInjectable`, `Bypassable` or `Strict`, plus findings such as `script-unsafe-inline`, `script-data-uri`, `script-broad-source`, `script-bypass-host`, `script-host-allowlist`, `script-self-allowlist`, `script-unsafe-eval`, `strict-dynamic-without-nonce`, `object-src-missing` and `base-uri-missing`. Hosts are checked against `defaultBypassHosts` by `bypassCovers`, which understands wildcards on both sides, so `https://assets.s3.amazonaws.com` and `*.cloudfront.net` both hit.

Then the set is judged as a set. A request must pass every enforced policy, so if one enforced policy is `Strict`, script findings on a weaker sibling are lowered to Info with a note naming the strict one. Findings on report only policies are capped at Low and prefixed, except `report-only-without-endpoint`, which is the one thing that matters about them. `conflicts` takes the nonce from each enforced nonce policy, runs `evaluate` with an `InlineScript` check and reports `policies-conflict` when another enforced policy blocks it. Set level findings cover `no-policy`, `no-enforced-policy` and `frame-ancestors-missing`. The `Report` has the combined posture, the index of the strict policy if there is one, findings sorted by severity and `pass`, which is false when any finding is at or above `failOn`.

`encodeReport`, `encodeDecision`, `encodePolicy` and `encodeFinding` turn everything into JSON.

## Usage

As a library inside Elm:

```elm
import CspPolicyEvaluator as Csp exposing (Check(..), Kind(..))

page : Maybe Csp.Url
page =
    Csp.parseUrl "https://shop.example.com/checkout"

policies : List Csp.Policy
policies =
    Csp.parseHeader Csp.Enforce "default-src 'self'; script-src 'self' https://cdn.jsdelivr.net; object-src 'none'"
        ++ Csp.parseHeader Csp.ReportOnly "script-src 'nonce-R4nd0mPerResponseValue22' 'strict-dynamic'; base-uri 'none'; report-to csp"

-- with origin taken from page:
-- Csp.evaluate origin policies (Load { kind = Script, url = "/static/app.js", nonce = Nothing, parserInserted = True, redirected = False })
-- Csp.evaluate origin policies (InlineScript { nonce = Nothing, digests = [] })
-- Csp.audit Csp.defaultConfig origin policies
```

From node or CI, build the worker once:

```
elm make CspPolicyEvaluatorCli.elm --output=csp.js
```

Write the input as JSON. `headers` are enforced headers, `reportOnlyHeaders` are Content-Security-Policy-Report-Only values and `metas` are meta tag contents. Policies are indexed in that order. `failOn` is high, medium, low or info. `bypassHosts` replaces the default list and `extraBypassHosts` adds to it. Each check has a `type` (`load`, `inline-script`, `script-attribute`, `javascript-url`, `inline-style`, `style-attribute`, `eval`, `wasm`, `form`, `base` or `frame-ancestor`) and may carry `expect` set to allow or block.

```json
{
  "origin": "https://shop.example.com/checkout",
  "headers": ["default-src 'self'; script-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; object-src 'none'"],
  "reportOnlyHeaders": ["script-src 'nonce-R4nd0mPerResponseValue22' 'strict-dynamic'; base-uri 'none'; report-to csp"],
  "failOn": "high",
  "checks": [
    { "label": "own bundle", "type": "load", "kind": "script", "url": "/static/app.js", "expect": "allow" },
    { "label": "injected inline", "type": "inline-script", "expect": "block" },
    { "label": "late widget", "type": "load", "kind": "script", "url": "https://widgets.example.net/w.js", "parserInserted": false },
    { "label": "handler", "type": "script-attribute", "digests": ["sha256-dGVzdGRpZ2VzdA=="] }
  ]
}
```

Run it and get a non zero exit code when the audit fails or a check does not match its expectation:

```
node -e 'const {Elm}=require("./csp.js");const input=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));Elm.CspPolicyEvaluatorCli.init({flags:input}).ports.emit.subscribe(out=>{console.log(out);process.exitCode=JSON.parse(out).pass?0:1})' csp-input.json
```

The output has `pass`, `auditPass`, `checksPass`, the parsed `policies`, the `report` with `scriptPosture`, `counts` and `findings`. There is one entry per check with the full `decision`. For the input above the audit fails on `script-unsafe-inline` and `script-bypass-host`. The injected inline script is reported as allowed, so its `block` expectation fails. The own bundle check shows that the report only nonce policy would block it, which is exactly what you want to know before enforcing.

To run the scenario checks:

```
elm make CspPolicyEvaluatorSelfCheck.elm --output=check.js
node -e 'const {Elm}=require("./check.js");Elm.CspPolicyEvaluatorSelfCheck.init().ports.report.subscribe(console.log)'
```

## Notes

- Dependencies are only `elm/core` and `elm/json`. `elm.json` is included.
- The module does not hash content. Pass digests you computed yourself with `crypto.subtle.digest` or `openssl dgst -sha256 -binary | base64`. `parseDigest` accepts `sha256-`, `sha384-` and `sha512-` values.
- `'strict-dynamic'` is applied to script element loads. Worker loads use plain URL matching. SRI hashes on external scripts are not modelled.
- Paths are ignored for `frame-ancestors`, matching what browsers do with ancestor origins.
- `upgrade-insecure-requests` is applied to subresource loads only, not to form submits or navigations.
- The bypass host list is opinionated. It targets hosts where anyone can publish JavaScript. If your company owns a bucket on one of them, the finding still stands, because an attacker can make their own bucket on the same suffix.
- A Strict posture means injected markup cannot run script under the policy. It does not mean your nonces are random per response or that your server never reflects them. Check that separately.
- Report only findings are capped at Low on purpose. They show migration risk, not current exposure.
- The worker sends its JSON one tick after init through `Process.sleep 0`, so a node subscriber attached right after `init` always receives it.
