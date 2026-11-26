port module CspPolicyEvaluatorSelfCheck exposing (main)

{-| Scenario checks for CspPolicyEvaluator. Build it with `elm make` and run the
output under node, see the README. Prints one PASS or FAIL line per scenario and
a summary line. No test package is needed.
-}

import CspPolicyEvaluator as Csp exposing (Check(..), Kind(..), ScriptPosture(..), Severity(..))
import Process
import Task


port report : String -> Cmd msg


type Msg
    = Go


main : Program () () Msg
main =
    Platform.worker
        { init = \_ -> ( (), Task.perform (\_ -> Go) (Process.sleep 0) )
        , update = \Go model -> ( model, report (summarise results) )
        , subscriptions = \_ -> Sub.none
        }


summarise : List ( String, Bool ) -> String
summarise rs =
    let
        line ( name, ok ) =
            (if ok then
                "PASS "

             else
                "FAIL "
            )
                ++ name

        failed =
            List.length (List.filter (not << Tuple.second) rs)
    in
    String.join "\n" (List.map line rs)
        ++ "\n"
        ++ String.fromInt (List.length rs - failed)
        ++ " passed, "
        ++ String.fromInt failed
        ++ " failed"



-- FIXTURES


https : Csp.Url
https =
    urlOrBlank "https://app.example.com/dashboard/index.html"


http : Csp.Url
http =
    urlOrBlank "http://app.example.com/"


urlOrBlank : String -> Csp.Url
urlOrBlank raw =
    Csp.parseUrl raw |> Maybe.withDefault { scheme = "", host = "", port_ = Nothing, path = "" }


goodNonce : String
goodNonce =
    "c2VsZmNoZWNrLW5vbmNlLXZhbHVl"


enforced : String -> List Csp.Policy
enforced =
    Csp.parseHeader Csp.Enforce


reportOnly : String -> List Csp.Policy
reportOnly =
    Csp.parseHeader Csp.ReportOnly


loadScript : String -> Bool -> Check
loadScript url parser =
    Load { kind = Script, url = url, nonce = Nothing, parserInserted = parser, redirected = False }


load : Kind -> String -> Check
load kind url =
    Load { kind = kind, url = url, nonce = Nothing, parserInserted = True, redirected = False }


allowed : Csp.Url -> List Csp.Policy -> Check -> Bool
allowed origin policies check =
    (Csp.evaluate origin policies check).allowed


issueCodes : List Csp.Policy -> List String
issueCodes policies =
    List.concatMap (\p -> List.map .code p.issues) policies


auditCodes : List Csp.Policy -> List String
auditCodes policies =
    List.map .code (Csp.audit Csp.defaultConfig https policies).findings


severityOf : String -> List Csp.Policy -> Maybe Severity
severityOf code policies =
    (Csp.audit Csp.defaultConfig https policies).findings
        |> List.filter (\f -> f.code == code)
        |> List.head
        |> Maybe.map .severity


digest : String -> Csp.Digest
digest raw =
    Csp.parseDigest raw |> Maybe.withDefault { algo = Csp.Sha256, value = "" }



-- SCENARIOS


results : List ( String, Bool )
results =
    [ ( "comma in a header splits into two policies"
      , List.length (enforced "default-src 'self', script-src 'none'") == 2
      )
    , ( "comma in a meta policy is one policy and is flagged"
      , List.member "comma-in-meta" (issueCodes [ Csp.parseMeta "script-src 'self',https://x.example.com" ])
      )
    , ( "missing semicolon is detected"
      , List.member "missing-semicolon" (issueCodes (enforced "script-src 'self' object-src 'none'"))
      )
    , ( "unquoted self is flagged"
      , List.member "unquoted-keyword" (issueCodes (enforced "script-src self"))
      )
    , ( "typo directive gets a suggestion"
      , enforced "scirpt-src 'self'"
            |> List.concatMap .issues
            |> List.any (\f -> f.code == "unknown-directive" && String.contains "`script-src`" f.message)
      )
    , ( "duplicate directive keeps the first"
      , let
            ps =
                enforced "script-src 'none'; script-src *"
        in
        List.member "duplicate-directive" (issueCodes ps)
            && not (allowed https ps (loadScript "https://cdn.example.com/a.js" True))
      )
    , ( "wildcard subdomain matches subdomains only"
      , let
            ps =
                enforced "script-src *.example.com"
        in
        allowed https ps (loadScript "https://cdn.example.com/x.js" True)
            && not (allowed https ps (loadScript "https://example.com/x.js" True))
      )
    , ( "directory path matches below it, not siblings"
      , let
            ps =
                enforced "script-src https://cdn.example.com/js/"
        in
        allowed https ps (loadScript "https://cdn.example.com/js/app.js" True)
            && not (allowed https ps (loadScript "https://cdn.example.com/jsx/app.js" True))
            && not (allowed https ps (loadScript "https://cdn.example.com/js" True))
      )
    , ( "path is ignored after a redirect"
      , allowed https
            (enforced "script-src https://cdn.example.com/js/")
            (Load { kind = Script, url = "https://cdn.example.com/other/app.js", nonce = Nothing, parserInserted = True, redirected = True })
      )
    , ( "'self' on an http page allows the https upgrade"
      , allowed http (enforced "script-src 'self'") (loadScript "https://app.example.com/a.js" True)
      )
    , ( "'self' does not cover another port"
      , not (allowed https (enforced "script-src 'self'") (loadScript "https://app.example.com:8443/a.js" True))
      )
    , ( "relative URLs resolve against the page"
      , allowed https (enforced "script-src https://app.example.com/dashboard/") (loadScript "app.js" True)
      )
    , ( "upgrade-insecure-requests rewrites before matching"
      , let
            d =
                Csp.evaluate https (enforced "default-src https:; upgrade-insecure-requests") (load Image "http://img.example.net/a.png")
        in
        d.allowed && d.effectiveUrl == Just "https://img.example.net/a.png"
      )
    , ( "* does not match data: URLs"
      , not (allowed https (enforced "img-src *") (load Image "data:image/png;base64,AAAA"))
      )
    , ( "nonce switches off 'unsafe-inline'"
      , let
            ps =
                enforced ("script-src 'nonce-" ++ goodNonce ++ "' 'unsafe-inline'")
        in
        not (allowed https ps (InlineScript { nonce = Nothing, digests = [] }))
            && allowed https ps (InlineScript { nonce = Just goodNonce, digests = [] })
      )
    , ( "'strict-dynamic' blocks parser inserted, trusts script inserted"
      , let
            ps =
                enforced ("script-src 'nonce-" ++ goodNonce ++ "' 'strict-dynamic' https:")
        in
        not (allowed https ps (loadScript "https://evil.example.net/x.js" True))
            && allowed https ps (loadScript "https://widgets.example.net/x.js" False)
      )
    , ( "hashes cover handlers only with 'unsafe-hashes'"
      , let
            dg =
                digest "sha256-dGVzdGRpZ2VzdA=="

            plain =
                enforced "script-src 'sha256-dGVzdGRpZ2VzdA=='"

            withHashes =
                enforced "script-src 'unsafe-hashes' 'sha256-dGVzdGRpZ2VzdA=='"
        in
        not (allowed https plain (ScriptAttribute { digests = [ dg ] }))
            && allowed https withHashes (ScriptAttribute { digests = [ dg ] })
            && allowed https plain (InlineScript { nonce = Nothing, digests = [ dg ] })
      )
    , ( "base64url digest equals base64 hash source"
      , allowed https (enforced "script-src 'sha256-ab+/cd=='") (InlineScript { nonce = Nothing, digests = [ digest "sha256-ab-_cd" ] })
      )
    , ( "eval needs 'unsafe-eval', wasm takes 'wasm-unsafe-eval'"
      , let
            ps =
                enforced "script-src 'self' 'wasm-unsafe-eval'"
        in
        not (allowed https ps Eval) && allowed https ps WasmCompile
      )
    , ( "report only policy reports but does not block"
      , let
            d =
                Csp.evaluate https (enforced "script-src 'self'" ++ reportOnly "script-src 'none'; report-to csp") (loadScript "https://app.example.com/a.js" True)
        in
        d.allowed && List.length d.reportedBy == 1 && List.isEmpty d.blockedBy
      )
    , ( "two enforced policies must both allow"
      , not (allowed https (enforced "script-src https:" ++ enforced "script-src 'self'") (loadScript "https://cdn.other.net/x.js" True))
      )
    , ( "frame-ancestors in meta is ignored and flagged"
      , let
            ps =
                [ Csp.parseMeta "frame-ancestors 'none'" ]
        in
        allowed https ps (EmbedIn "https://evil.example.net")
            && List.member "meta-ignored-directive" (issueCodes ps)
      )
    , ( "form-action does not fall back to default-src"
      , allowed https (enforced "default-src 'none'") (FormSubmit "https://evil.example.net/collect")
      )
    , ( "explicit port wildcard and http to https scheme upgrade"
      , allowed https (enforced "connect-src https://api.example.com:*") (load Connect "https://api.example.com:8443/v1")
            && allowed https (enforced "connect-src http://api.example.com") (load Connect "https://api.example.com/v1")
            && not (allowed https (enforced "connect-src http://api.example.com") (load Connect "https://api.example.com:8443/v1"))
      )
    , ( "URL parsing normalises host, default port and dot segments"
      , Csp.parseUrl "https://App.Example.COM:443/a/../b?x=1#y" == Just { scheme = "https", host = "app.example.com", port_ = Nothing, path = "/b" }
      )
    , ( "strict nonce policy audits clean"
      , let
            r =
                Csp.audit Csp.defaultConfig https (enforced ("script-src 'nonce-" ++ goodNonce ++ "' 'strict-dynamic'; object-src 'none'; base-uri 'none'; frame-ancestors 'self'"))
        in
        r.posture == Strict && r.pass && List.all (\f -> f.severity == Info) r.findings
      )
    , ( "public CDN in script-src is a high finding"
      , let
            ps =
                enforced "script-src 'self' https://cdnjs.cloudflare.com; object-src 'none'; base-uri 'self'"

            r =
                Csp.audit Csp.defaultConfig https ps
        in
        r.posture == Bypassable && not r.pass && severityOf "script-bypass-host" ps == Just High
      )
    , ( "attacker hostable suffix is matched through subdomains"
      , severityOf "script-bypass-host" (enforced "script-src https://assets.s3.amazonaws.com; object-src 'none'; base-uri 'none'") == Just High
      )
    , ( "unsafe-inline without nonce is inline injectable"
      , let
            ps =
                enforced "script-src 'self' 'unsafe-inline'"
        in
        (Csp.audit Csp.defaultConfig https ps).posture == InlineInjectable && severityOf "script-unsafe-inline" ps == Just High
      )
    , ( "data: in script-src is inline injectable"
      , (Csp.audit Csp.defaultConfig https (enforced "script-src 'self' data:")).posture == InlineInjectable
      )
    , ( "missing script-src and default-src is unrestricted"
      , let
            r =
                Csp.audit Csp.defaultConfig https (enforced "img-src 'self'")
        in
        r.posture == Unrestricted && List.member "script-unrestricted" (List.map .code r.findings)
      )
    , ( "a strict policy downgrades a weak sibling and the conflict is reported"
      , let
            ps =
                enforced ("script-src 'nonce-" ++ goodNonce ++ "' 'strict-dynamic'; object-src 'none'; base-uri 'none'; frame-ancestors 'self'")
                    ++ enforced "script-src https:"

            r =
                Csp.audit Csp.defaultConfig https ps
        in
        r.posture
            == Strict
            && r.strictPolicy
            == Just 0
            && severityOf "script-broad-source" ps
            == Just Info
            && List.member "policies-conflict" (List.map .code r.findings)
      )
    , ( "report only without endpoint keeps its severity"
      , let
            ps =
                reportOnly "script-src 'self'"
        in
        severityOf "report-only-without-endpoint" ps == Just Medium && List.member "no-enforced-policy" (auditCodes ps)
      )
    , ( "findings on report only policies are capped at low"
      , severityOf "script-unsafe-inline" (enforced "script-src 'self'; object-src 'none'; base-uri 'none'" ++ reportOnly "script-src 'unsafe-inline'; report-uri /csp") == Just Low
      )
    , ( "'none' with other sources is flagged"
      , List.member "none-with-sources" (issueCodes (enforced "img-src 'none' https://img.example.com"))
      )
    , ( "short nonce is flagged"
      , List.member "weak-nonce" (issueCodes (enforced "script-src 'nonce-abc123'"))
      )
    , ( "sha1 hash is rejected"
      , List.member "invalid-source" (issueCodes (enforced "script-src 'sha1-dGVzdA=='"))
      )
    , ( "'strict-dynamic' without nonce or hash is flagged"
      , List.member "strict-dynamic-without-nonce" (auditCodes (enforced "script-src 'strict-dynamic'; object-src 'none'; base-uri 'none'"))
      )
    , ( "missing object-src and base-uri are flagged"
      , let
            codes =
                auditCodes (enforced ("script-src 'nonce-" ++ goodNonce ++ "'"))
        in
        List.member "object-src-missing" codes && List.member "base-uri-missing" codes
      )
    , ( "frame-ancestors missing across the set is flagged once"
      , List.length (List.filter ((==) "frame-ancestors-missing") (auditCodes (enforced "default-src 'self'" ++ enforced "img-src 'self'"))) == 1
      )
    ]
