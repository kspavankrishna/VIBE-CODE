module CspPolicyEvaluator exposing
    ( Check(..)
    , Config
    , Decision
    , Delivery(..)
    , Digest
    , Directive
    , Disposition(..)
    , Finding
    , HashAlgo(..)
    , HostSource
    , Kind(..)
    , Policy
    , PortSpec(..)
    , Report
    , ScriptPosture(..)
    , Severity(..)
    , Source(..)
    , Url
    , Verdict
    , audit
    , defaultBypassHosts
    , defaultConfig
    , effectiveDirective
    , encodeDecision
    , encodeFinding
    , encodePolicy
    , encodeReport
    , evaluate
    , fallbackChain
    , kindDirective
    , kindFromString
    , matchesSource
    , matchesSourceList
    , parseDigest
    , parseHeader
    , parseMeta
    , parseSource
    , parseUrl
    , postureToString
    , resolveUrl
    , scriptPosture
    , severityFromString
    , severityToString
    , sourceToString
    )

{-| Content Security Policy evaluator that follows the CSP Level 3 matching rules.

It parses header and meta delivered policies the way a browser does, answers
"would this load, inline block, eval or frame be allowed" across every policy on
the page at once, and audits the set for the mistakes that turn a CSP into
decoration. Pure Elm, no IO, no clock, only elm/core and elm/json.

-}

import Json.Encode as E



-- TYPES


{-| Whether a policy blocks or only reports.
-}
type Disposition
    = Enforce
    | ReportOnly


{-| Where the policy came from. Meta delivered policies cannot use
frame-ancestors, report-uri or sandbox, and cannot be report only.
-}
type Delivery
    = Header
    | Meta


type HashAlgo
    = Sha256
    | Sha384
    | Sha512


type PortSpec
    = NoPort
    | AnyPort
    | Port Int


{-| A host-source expression such as `https://*.example.com:8443/js/`.
`host` is lower cased and may start with `*.` or be exactly `*`.
-}
type alias HostSource =
    { scheme : Maybe String
    , host : String
    , port_ : PortSpec
    , path : String
    }


type Source
    = KwNone
    | KwSelf
    | KwUnsafeInline
    | KwUnsafeEval
    | KwWasmUnsafeEval
    | KwUnsafeHashes
    | KwStrictDynamic
    | KwReportSample
    | KwInlineSpeculationRules
    | Nonce String
    | Hash HashAlgo String
    | SchemeSource String
    | HostExpr HostSource
    | Wildcard
    | Invalid String String


type alias Directive =
    { name : String
    , tokens : List String
    , sources : List Source
    }


type alias Policy =
    { disposition : Disposition
    , delivery : Delivery
    , raw : String
    , directives : List Directive
    , issues : List Finding
    }


type Severity
    = High
    | Medium
    | Low
    | Info


{-| `policy` is the index of the policy in the list passed to `audit`, or -1
for a finding about the whole set.
-}
type alias Finding =
    { severity : Severity
    , code : String
    , policy : Int
    , directive : String
    , message : String
    }


{-| A parsed URL. `port_` is Nothing when the port is absent or equal to the
scheme default, the same normalisation the URL standard applies.
-}
type alias Url =
    { scheme : String
    , host : String
    , port_ : Maybe Int
    , path : String
    }


type Kind
    = Script
    | Style
    | Image
    | Font
    | Connect
    | Frame
    | FencedFrame
    | Worker
    | Manifest
    | Media
    | Object


{-| A digest of inline content, the same base64 text you would put after
`sha256-` in a policy. Compute it with crypto.subtle or openssl, the module
does not hash content itself.
-}
type alias Digest =
    { algo : HashAlgo
    , value : String
    }


type Check
    = Load { kind : Kind, url : String, nonce : Maybe String, parserInserted : Bool, redirected : Bool }
    | InlineScript { nonce : Maybe String, digests : List Digest }
    | ScriptAttribute { digests : List Digest }
    | JavascriptUrl { digests : List Digest }
    | InlineStyle { nonce : Maybe String, digests : List Digest }
    | StyleAttribute { digests : List Digest }
    | Eval
    | WasmCompile
    | FormSubmit String
    | BaseUrl String
    | EmbedIn String


type alias Verdict =
    { policy : Int
    , disposition : Disposition
    , directive : Maybe String
    , allowed : Bool
    , reason : String
    }


type alias Decision =
    { check : String
    , allowed : Bool
    , effectiveUrl : Maybe String
    , blockedBy : List Verdict
    , reportedBy : List Verdict
    , verdicts : List Verdict
    }


type ScriptPosture
    = Unrestricted
    | InlineInjectable
    | Bypassable
    | Strict


type alias Config =
    { bypassHosts : List String
    , failOn : Severity
    }


type alias Report =
    { posture : ScriptPosture
    , strictPolicy : Maybe Int
    , findings : List Finding
    , pass : Bool
    , failOn : Severity
    }



-- CONSTANTS


{-| Hosts where anyone can publish JavaScript: public npm CDNs and shared
hosting suffixes. Allowlisting one of them in script-src lets an attacker load
their own file, or an old library with known script gadgets, from a host the
policy trusts. Override or extend through `Config.bypassHosts`.
-}
defaultBypassHosts : List String
defaultBypassHosts =
    [ "cdnjs.cloudflare.com"
    , "cdn.jsdelivr.net"
    , "unpkg.com"
    , "esm.sh"
    , "cdn.skypack.dev"
    , "ajax.googleapis.com"
    , "storage.googleapis.com"
    , "www.google.com"
    , "*.appspot.com"
    , "*.firebaseapp.com"
    , "*.web.app"
    , "*.amazonaws.com"
    , "*.cloudfront.net"
    , "*.azureedge.net"
    , "*.blob.core.windows.net"
    , "*.herokuapp.com"
    , "*.github.io"
    , "raw.githack.com"
    , "*.vercel.app"
    , "*.netlify.app"
    , "*.pages.dev"
    , "*.workers.dev"
    , "*.glitch.me"
    ]


defaultConfig : Config
defaultConfig =
    { bypassHosts = defaultBypassHosts
    , failOn = High
    }


fetchDirectives : List String
fetchDirectives =
    [ "child-src"
    , "connect-src"
    , "default-src"
    , "fenced-frame-src"
    , "font-src"
    , "frame-src"
    , "img-src"
    , "manifest-src"
    , "media-src"
    , "object-src"
    , "script-src"
    , "script-src-elem"
    , "script-src-attr"
    , "style-src"
    , "style-src-elem"
    , "style-src-attr"
    , "worker-src"
    ]


sourceListDirectives : List String
sourceListDirectives =
    fetchDirectives ++ [ "base-uri", "form-action", "frame-ancestors" ]


otherDirectives : List String
otherDirectives =
    [ "sandbox"
    , "report-uri"
    , "report-to"
    , "upgrade-insecure-requests"
    , "require-trusted-types-for"
    , "trusted-types"
    , "webrtc"
    ]


knownDirectives : List String
knownDirectives =
    sourceListDirectives ++ otherDirectives


removedDirectives : List String
removedDirectives =
    [ "block-all-mixed-content"
    , "prefetch-src"
    , "navigate-to"
    , "plugin-types"
    , "referrer"
    , "reflected-xss"
    , "disown-opener"
    , "require-sri-for"
    , "manifest"
    ]


metaIgnoredDirectives : List String
metaIgnoredDirectives =
    [ "frame-ancestors", "report-uri", "sandbox" ]


bareKeywords : List String
bareKeywords =
    [ "self", "none", "unsafe-inline", "unsafe-eval", "wasm-unsafe-eval", "unsafe-hashes", "strict-dynamic", "report-sample" ]


scriptFindingCodes : List String
scriptFindingCodes =
    [ "script-unrestricted"
    , "script-unsafe-inline"
    , "script-data-uri"
    , "script-broad-source"
    , "script-bypass-host"
    , "script-host-allowlist"
    , "script-self-allowlist"
    , "script-insecure-source"
    , "script-unsafe-eval"
    , "object-src-missing"
    , "object-src-broad"
    , "base-uri-missing"
    ]


{-| A nonce shorter than 16 decoded bytes (22 base64 characters) is below the
128 bits the spec asks for.
-}
minNonceChars : Int
minNonceChars =
    22



-- PARSING


{-| Parse a Content-Security-Policy or Content-Security-Policy-Report-Only
header value. Commas separate policies, so one header line can carry several.
-}
parseHeader : Disposition -> String -> List Policy
parseHeader disposition raw =
    String.split "," raw
        |> List.map String.trim
        |> List.filter (not << String.isEmpty)
        |> List.map (parseSerialized disposition Header)


{-| Parse the content attribute of `<meta http-equiv="Content-Security-Policy">`.
A meta policy is always one enforced policy; a comma is not a separator here.
-}
parseMeta : String -> Policy
parseMeta raw =
    parseSerialized Enforce Meta raw


type alias ParseAcc =
    { seen : List String
    , directives : List Directive
    , issues : List Finding
    }


parseSerialized : Disposition -> Delivery -> String -> Policy
parseSerialized disposition delivery raw =
    let
        step piece acc =
            case String.words piece of
                [] ->
                    acc

                nameRaw :: values ->
                    let
                        name =
                            String.toLower nameRaw
                    in
                    if not (isDirectiveName name) then
                        { acc | issues = acc.issues ++ [ issue Medium "invalid-directive-name" name ("`" ++ nameRaw ++ "` is not a valid directive name, the browser drops this directive.") ] }

                    else if List.member name acc.seen then
                        { acc | issues = acc.issues ++ [ issue Medium "duplicate-directive" name ("`" ++ name ++ "` appears twice in one policy. Browsers keep the first and ignore this one, values are not merged.") ] }

                    else
                        let
                            directive =
                                buildDirective name values
                        in
                        { seen = name :: acc.seen
                        , directives = acc.directives ++ [ directive ]
                        , issues = acc.issues ++ directiveIssues delivery disposition directive
                        }

        result =
            List.foldl step { seen = [], directives = [], issues = [] } (String.split ";" raw)

        emptyIssue =
            if List.isEmpty result.directives then
                [ issue High "empty-policy" "" "The policy has no valid directives, so it restricts nothing." ]

            else
                []

        metaReportOnly =
            if delivery == Meta && disposition == ReportOnly then
                [ issue Medium "meta-report-only" "" "Report only policies cannot be delivered in a meta element, browsers ignore it." ]

            else
                []
    in
    { disposition = disposition
    , delivery = delivery
    , raw = raw
    , directives = result.directives
    , issues = emptyIssue ++ metaReportOnly ++ result.issues
    }


issue : Severity -> String -> String -> String -> Finding
issue severity code directive message =
    { severity = severity, code = code, policy = 0, directive = directive, message = message }


buildDirective : String -> List String -> Directive
buildDirective name values =
    { name = name
    , tokens = values
    , sources =
        if List.member name sourceListDirectives then
            List.map parseSource values

        else
            []
    }


directiveIssues : Delivery -> Disposition -> Directive -> List Finding
directiveIssues delivery disposition d =
    let
        name =
            d.name

        nameLevel =
            if List.member name removedDirectives then
                [ issue Low "removed-directive" name ("`" ++ name ++ "` was removed from CSP and current browsers ignore it.") ]

            else if not (List.member name knownDirectives) then
                case closestDirective name of
                    Just guess ->
                        [ issue Medium "unknown-directive" name ("`" ++ name ++ "` is not a CSP directive, did you mean `" ++ guess ++ "`? As written it is ignored.") ]

                    Nothing ->
                        [ issue Low "unknown-directive" name ("`" ++ name ++ "` is not a CSP directive and is ignored.") ]

            else
                []

        metaLevel =
            if delivery == Meta && List.member name metaIgnoredDirectives then
                [ issue Medium "meta-ignored-directive" name ("`" ++ name ++ "` has no effect in a meta element. Send it as a response header.") ]

            else
                []

        uirReportOnly =
            if name == "upgrade-insecure-requests" && disposition == ReportOnly then
                [ issue Low "upgrade-in-report-only" name "upgrade-insecure-requests is ignored in a report only policy." ]

            else
                []

        sourceLevel =
            if List.member name sourceListDirectives then
                sourceIssues delivery d

            else
                []
    in
    nameLevel ++ metaLevel ++ uirReportOnly ++ sourceLevel


sourceIssues : Delivery -> Directive -> List Finding
sourceIssues delivery d =
    let
        name =
            d.name

        perToken ( token, source ) =
            let
                lower =
                    String.toLower token

                semicolon =
                    if List.member lower knownDirectives then
                        [ issue High "missing-semicolon" name ("`" ++ token ++ "` appears as a source inside `" ++ name ++ "`. A semicolon is missing, so the browser reads it as a host name and the directive you meant to write after it does not exist.") ]

                    else
                        []

                bare =
                    if List.member lower bareKeywords || List.any (\p -> String.startsWith p lower) [ "nonce-", "sha256-", "sha384-", "sha512-" ] then
                        [ issue Medium "unquoted-keyword" name ("`" ++ token ++ "` needs single quotes. Unquoted, it is a host name and matches nothing you intended.") ]

                    else
                        []

                comma =
                    if delivery == Meta && String.contains "," token then
                        [ issue Medium "comma-in-meta" name ("`" ++ token ++ "` contains a comma. In a meta element commas do not split policies, so this token is garbage.") ]

                    else
                        []

                invalid =
                    case source of
                        Invalid _ why ->
                            [ issue Medium "invalid-source" name ("`" ++ token ++ "` is ignored: " ++ why ++ ".") ]

                        _ ->
                            []

                nonceStrength =
                    case source of
                        Nonce v ->
                            if String.length (stripPadding v) < minNonceChars then
                                [ issue Medium "weak-nonce" name ("Nonce `" ++ v ++ "` is shorter than 128 bits. Generate at least 16 random bytes per response.") ]

                            else
                                []

                        _ ->
                            []

                misplaced =
                    case source of
                        KwStrictDynamic ->
                            if List.member name [ "script-src", "script-src-elem", "default-src" ] then
                                []

                            else
                                [ issue Low "keyword-not-applicable" name ("'strict-dynamic' has no effect in `" ++ name ++ "`.") ]

                        _ ->
                            []
            in
            semicolon ++ bare ++ comma ++ invalid ++ nonceStrength ++ misplaced

        noneMixed =
            if List.member KwNone d.sources && List.length d.sources > 1 then
                [ issue Medium "none-with-sources" name ("`" ++ name ++ "` lists 'none' together with other sources. 'none' is then ignored and the other sources apply.") ]

            else
                []
    in
    List.concatMap perToken (List.map2 Tuple.pair d.tokens d.sources) ++ noneMixed


{-| Parse one source expression the way the CSP grammar does. Anything the
grammar rejects comes back as `Invalid token reason`.
-}
parseSource : String -> Source
parseSource token =
    let
        lower =
            String.toLower token
    in
    case lower of
        "'none'" ->
            KwNone

        "'self'" ->
            KwSelf

        "'unsafe-inline'" ->
            KwUnsafeInline

        "'unsafe-eval'" ->
            KwUnsafeEval

        "'wasm-unsafe-eval'" ->
            KwWasmUnsafeEval

        "'unsafe-hashes'" ->
            KwUnsafeHashes

        "'strict-dynamic'" ->
            KwStrictDynamic

        "'report-sample'" ->
            KwReportSample

        "'inline-speculation-rules'" ->
            KwInlineSpeculationRules

        "*" ->
            Wildcard

        _ ->
            if String.startsWith "'" token then
                if String.length token > 2 && String.endsWith "'" token then
                    parseQuoted token (String.slice 1 -1 token)

                else
                    Invalid token "the quote is not closed"

            else if isSchemeSource lower then
                SchemeSource (String.dropRight 1 lower)

            else
                case parseHostSource token of
                    Just h ->
                        HostExpr h

                    Nothing ->
                        Invalid token "it is not a valid host, scheme or keyword source"


parseQuoted : String -> String -> Source
parseQuoted token inner =
    let
        lowerInner =
            String.toLower inner

        valued prefix make =
            let
                v =
                    String.dropLeft (String.length prefix) inner
            in
            if isBase64Value v then
                make v

            else
                Invalid token "the value is not base64 or base64url"
    in
    if String.startsWith "nonce-" lowerInner then
        valued "nonce-" Nonce

    else if String.startsWith "sha256-" lowerInner then
        valued "sha256-" (Hash Sha256)

    else if String.startsWith "sha384-" lowerInner then
        valued "sha384-" (Hash Sha384)

    else if String.startsWith "sha512-" lowerInner then
        valued "sha512-" (Hash Sha512)

    else if List.any (\p -> String.startsWith p lowerInner) [ "sha1-", "sha224-", "md5-" ] then
        Invalid token "CSP only accepts sha256, sha384 and sha512 hashes"

    else
        Invalid token "it is not a CSP keyword"


parseHostSource : String -> Maybe HostSource
parseHostSource raw =
    let
        ( scheme, rest ) =
            case String.indexes "://" raw of
                i :: _ ->
                    ( Just (String.toLower (String.left i raw)), String.dropLeft (i + 3) raw )

                [] ->
                    ( Nothing, raw )

        ( hostPort, path ) =
            case String.indexes "/" rest of
                i :: _ ->
                    ( String.left i rest, String.dropLeft i rest )

                [] ->
                    ( rest, "" )

        hostAndPort =
            case String.split ":" hostPort of
                [ h ] ->
                    Just ( h, NoPort )

                [ h, "*" ] ->
                    Just ( h, AnyPort )

                [ h, p ] ->
                    String.toInt p
                        |> Maybe.andThen
                            (\n ->
                                if n >= 0 && n <= 65535 && String.all Char.isDigit p then
                                    Just ( h, Port n )

                                else
                                    Nothing
                            )

                _ ->
                    Nothing

        schemeOk =
            case scheme of
                Just s ->
                    isSchemeName s

                Nothing ->
                    True

        pathOk =
            not (String.contains ";" path || String.contains "," path || String.contains "?" path || String.contains "#" path)
    in
    case hostAndPort of
        Just ( h, p ) ->
            let
                host =
                    String.toLower h
            in
            if schemeOk && pathOk && isHostPattern host then
                Just { scheme = scheme, host = host, port_ = p, path = path }

            else
                Nothing

        Nothing ->
            Nothing


{-| Parse `sha256-<base64>` style digest text into a `Digest`.
-}
parseDigest : String -> Maybe Digest
parseDigest raw =
    let
        s =
            String.trim raw

        lower =
            String.toLower s

        take prefix algo =
            let
                v =
                    String.dropLeft (String.length prefix) s
            in
            if isBase64Value v then
                Just { algo = algo, value = v }

            else
                Nothing
    in
    if String.startsWith "sha256-" lower then
        take "sha256-" Sha256

    else if String.startsWith "sha384-" lower then
        take "sha384-" Sha384

    else if String.startsWith "sha512-" lower then
        take "sha512-" Sha512

    else
        Nothing



-- URLS


{-| Parse an absolute URL. Hosts are lower cased, a default port becomes
Nothing, an empty hierarchical path becomes "/".
-}
parseUrl : String -> Maybe Url
parseUrl raw =
    let
        s =
            String.trim raw
    in
    case String.indexes ":" s of
        [] ->
            Nothing

        i :: _ ->
            let
                scheme =
                    String.toLower (String.left i s)

                rest =
                    String.dropLeft (i + 1) s
            in
            if not (isSchemeName scheme) then
                Nothing

            else if String.startsWith "//" rest then
                parseAuthority scheme (String.dropLeft 2 rest)

            else
                Just { scheme = scheme, host = "", port_ = Nothing, path = stripQueryFragment rest }


parseAuthority : String -> String -> Maybe Url
parseAuthority scheme rest =
    let
        endIdx =
            [ "/", "?", "#" ]
                |> List.filterMap (\c -> List.head (String.indexes c rest))
                |> List.minimum
                |> Maybe.withDefault (String.length rest)

        authority =
            String.left endIdx rest

        tail =
            String.dropLeft endIdx rest

        hostPort =
            case List.reverse (String.indexes "@" authority) of
                j :: _ ->
                    String.dropLeft (j + 1) authority

                [] ->
                    authority

        split =
            if String.startsWith "[" hostPort then
                case String.indexes "]" hostPort of
                    k :: _ ->
                        let
                            after =
                                String.dropLeft (k + 1) hostPort
                        in
                        if after == "" then
                            Just ( String.left (k + 1) hostPort, "" )

                        else if String.startsWith ":" after then
                            Just ( String.left (k + 1) hostPort, String.dropLeft 1 after )

                        else
                            Nothing

                    [] ->
                        Nothing

            else
                case String.indexes ":" hostPort of
                    k :: _ ->
                        Just ( String.left k hostPort, String.dropLeft (k + 1) hostPort )

                    [] ->
                        Just ( hostPort, "" )

        portResult portRaw =
            if portRaw == "" then
                Just Nothing

            else if String.all Char.isDigit portRaw then
                String.toInt portRaw
                    |> Maybe.andThen
                        (\n ->
                            if n > 65535 then
                                Nothing

                            else if Just n == defaultPort scheme then
                                Just Nothing

                            else
                                Just (Just n)
                        )

            else
                Nothing
    in
    case split of
        Just ( hostRaw, portRaw ) ->
            let
                host =
                    String.toLower hostRaw
                        |> (\h ->
                                if String.endsWith "." h then
                                    String.dropRight 1 h

                                else
                                    h
                           )

                path0 =
                    normalizePath (stripQueryFragment tail)
            in
            case portResult portRaw of
                Just p ->
                    if host == "" then
                        Nothing

                    else
                        Just
                            { scheme = scheme
                            , host = host
                            , port_ = p
                            , path =
                                if path0 == "" then
                                    "/"

                                else
                                    path0
                            }

                Nothing ->
                    Nothing

        Nothing ->
            Nothing


{-| Resolve a possibly relative URL against the protected document's URL.
Handles `//host/x`, `/x`, `x` and absolute URLs.
-}
resolveUrl : Url -> String -> Maybe Url
resolveUrl base raw =
    let
        s =
            String.trim raw
    in
    if String.isEmpty s then
        Nothing

    else if String.startsWith "//" s then
        parseUrl (base.scheme ++ ":" ++ s)

    else if String.startsWith "/" s then
        Just { base | path = normalizePath (stripQueryFragment s) }

    else
        case parseUrl s of
            Just u ->
                Just u

            Nothing ->
                Just { base | path = normalizePath (directoryOf base.path ++ stripQueryFragment s) }


urlToString : Url -> String
urlToString u =
    if u.host == "" then
        u.scheme ++ ":" ++ u.path

    else
        u.scheme
            ++ "://"
            ++ u.host
            ++ (case u.port_ of
                    Just p ->
                        ":" ++ String.fromInt p

                    Nothing ->
                        ""
               )
            ++ u.path


stripQueryFragment : String -> String
stripQueryFragment s =
    case [ "?", "#" ] |> List.filterMap (\c -> List.head (String.indexes c s)) |> List.minimum of
        Just i ->
            String.left i s

        Nothing ->
            s


directoryOf : String -> String
directoryOf path =
    case List.reverse (String.indexes "/" path) of
        i :: _ ->
            String.left (i + 1) path

        [] ->
            "/"


normalizePath : String -> String
normalizePath path =
    if not (String.contains "/." path) then
        path

    else
        let
            segments =
                String.split "/" path

            lastIsDot =
                case List.reverse segments of
                    s :: _ ->
                        s == "." || s == ".."

                    [] ->
                        False

            step seg acc =
                case seg of
                    "." ->
                        acc

                    ".." ->
                        case acc of
                            [ "" ] ->
                                acc

                            _ :: rest ->
                                rest

                            [] ->
                                []

                    _ ->
                        seg :: acc

            kept =
                List.reverse (List.foldl step [] segments)

            joined =
                String.join "/" kept
        in
        if lastIsDot then
            joined ++ "/"

        else
            joined


upgradeUrl : Url -> Url
upgradeUrl u =
    let
        port80 p =
            if p == Just 80 then
                Nothing

            else
                p
    in
    case u.scheme of
        "http" ->
            { u | scheme = "https", port_ = port80 u.port_ }

        "ws" ->
            { u | scheme = "wss", port_ = port80 u.port_ }

        _ ->
            u


defaultPort : String -> Maybe Int
defaultPort scheme =
    case scheme of
        "http" ->
            Just 80

        "https" ->
            Just 443

        "ws" ->
            Just 80

        "wss" ->
            Just 443

        "ftp" ->
            Just 21

        _ ->
            Nothing


effectivePort : Url -> Maybe Int
effectivePort u =
    case u.port_ of
        Just p ->
            Just p

        Nothing ->
            defaultPort u.scheme



-- MATCHING


{-| CSP3 scheme-part matching. `http` sources also cover `https`, `ws` covers
`wss`, `http` and `https`, `wss` covers `https`.
-}
schemePartMatch : String -> String -> Bool
schemePartMatch a b =
    a
        == b
        || (a == "http" && b == "https")
        || (a == "ws" && List.member b [ "wss", "http", "https" ])
        || (a == "wss" && b == "https")


hostPartMatch : String -> String -> Bool
hostPartMatch pattern host =
    if String.startsWith "*" pattern then
        not (isIpLiteral host) && String.endsWith (String.dropLeft 1 pattern) host

    else
        pattern == host


portPartMatch : PortSpec -> Url -> Bool
portPartMatch spec url =
    case spec of
        AnyPort ->
            True

        NoPort ->
            url.port_ == Nothing

        Port p ->
            let
                actual =
                    effectivePort url
            in
            actual == Just p || (p == 80 && actual == Just 443 && (url.scheme == "https" || url.scheme == "wss"))


pathPartMatch : String -> String -> Bool
pathPartMatch a b =
    if a == "" then
        True

    else if a == "/" && b == "" then
        True

    else
        let
            exact =
                not (String.endsWith "/" a)

            aSegs =
                String.split "/" a

            bSegs =
                String.split "/" b

            compared =
                if exact then
                    aSegs

                else
                    List.take (List.length aSegs - 1) aSegs
        in
        if List.length aSegs > List.length bSegs then
            False

        else if exact && List.length aSegs /= List.length bSegs then
            False

        else
            List.all identity (List.map2 (\x y -> percentDecode x == percentDecode y) compared bSegs)


selfMatch : Url -> Url -> Bool
selfMatch origin url =
    let
        isDefault u =
            u.port_ == Nothing

        portsOk =
            effectivePort origin == effectivePort url || (isDefault origin && isDefault url)
    in
    origin.host
        /= ""
        && origin.host
        == url.host
        && ((origin.scheme == url.scheme && effectivePort origin == effectivePort url)
                || (portsOk
                        && (url.scheme == "https" || url.scheme == "wss" || (origin.scheme == "http" && (url.scheme == "http" || url.scheme == "ws")))
                   )
           )


{-| Does one source expression allow `url` for a document at `origin`?
`redirected` is True after a redirect, when path parts stop counting.
-}
matchesSource : Url -> Bool -> Url -> Source -> Bool
matchesSource origin redirected url source =
    case source of
        Wildcard ->
            url.scheme == "http" || url.scheme == "https" || url.scheme == origin.scheme

        SchemeSource s ->
            schemePartMatch s url.scheme

        HostExpr h ->
            let
                schemeOk =
                    case h.scheme of
                        Just s ->
                            schemePartMatch s url.scheme

                        Nothing ->
                            schemePartMatch origin.scheme url.scheme
            in
            url.host
                /= ""
                && schemeOk
                && hostPartMatch h.host url.host
                && portPartMatch h.port_ url
                && (redirected || pathPartMatch h.path url.path)

        KwSelf ->
            selfMatch origin url

        _ ->
            False


matchesSourceList : Url -> Bool -> Url -> List Source -> Bool
matchesSourceList origin redirected url sources =
    List.any (matchesSource origin redirected url) sources


{-| The directive names a browser consults, in order, for a given directive.
-}
fallbackChain : String -> List String
fallbackChain name =
    case name of
        "script-src-elem" ->
            [ "script-src-elem", "script-src", "default-src" ]

        "script-src-attr" ->
            [ "script-src-attr", "script-src", "default-src" ]

        "style-src-elem" ->
            [ "style-src-elem", "style-src", "default-src" ]

        "style-src-attr" ->
            [ "style-src-attr", "style-src", "default-src" ]

        "worker-src" ->
            [ "worker-src", "child-src", "script-src", "default-src" ]

        "frame-src" ->
            [ "frame-src", "child-src", "default-src" ]

        "fenced-frame-src" ->
            [ "fenced-frame-src", "frame-src", "child-src", "default-src" ]

        "base-uri" ->
            [ "base-uri" ]

        "form-action" ->
            [ "form-action" ]

        "frame-ancestors" ->
            [ "frame-ancestors" ]

        "default-src" ->
            [ "default-src" ]

        other ->
            [ other, "default-src" ]


{-| The directive that actually governs `name` in a policy, after fallback.
Directives a meta element cannot carry are skipped for meta policies.
-}
effectiveDirective : String -> Policy -> Maybe Directive
effectiveDirective name policy =
    let
        usable d =
            not (policy.delivery == Meta && List.member d.name metaIgnoredDirectives)

        find n =
            List.filter (\d -> d.name == n && usable d) policy.directives |> List.head
    in
    List.foldl
        (\n found ->
            case found of
                Just _ ->
                    found

                Nothing ->
                    find n
        )
        Nothing
        (fallbackChain name)


kindDirective : Kind -> String
kindDirective kind =
    case kind of
        Script ->
            "script-src-elem"

        Style ->
            "style-src-elem"

        Image ->
            "img-src"

        Font ->
            "font-src"

        Connect ->
            "connect-src"

        Frame ->
            "frame-src"

        FencedFrame ->
            "fenced-frame-src"

        Worker ->
            "worker-src"

        Manifest ->
            "manifest-src"

        Media ->
            "media-src"

        Object ->
            "object-src"


kindFromString : String -> Maybe Kind
kindFromString s =
    case String.toLower s of
        "script" ->
            Just Script

        "style" ->
            Just Style

        "image" ->
            Just Image

        "img" ->
            Just Image

        "font" ->
            Just Font

        "connect" ->
            Just Connect

        "frame" ->
            Just Frame

        "fenced-frame" ->
            Just FencedFrame

        "worker" ->
            Just Worker

        "manifest" ->
            Just Manifest

        "media" ->
            Just Media

        "object" ->
            Just Object

        _ ->
            Nothing



-- EVALUATION


{-| Decide one check against every policy on the page. A request is allowed
only if every enforced policy allows it; report only policies add to
`reportedBy` but never block.
-}
evaluate : Url -> List Policy -> Check -> Decision
evaluate origin policies check =
    let
        upgrade =
            List.any (\p -> p.disposition == Enforce && hasDirective "upgrade-insecure-requests" p) policies

        effectiveUrl =
            case check of
                Load r ->
                    resolveUrl origin r.url
                        |> Maybe.map
                            (\u ->
                                if upgrade then
                                    upgradeUrl u

                                else
                                    u
                            )

                FormSubmit raw ->
                    resolveUrl origin raw

                BaseUrl raw ->
                    resolveUrl origin raw

                EmbedIn raw ->
                    parseUrl raw

                _ ->
                    Nothing

        verdicts =
            List.indexedMap (\i p -> verdictFor origin effectiveUrl i p check) policies
    in
    { check = describeCheck check
    , allowed = List.all (\v -> v.allowed || v.disposition == ReportOnly) verdicts
    , effectiveUrl = Maybe.map urlToString effectiveUrl
    , blockedBy = List.filter (\v -> not v.allowed && v.disposition == Enforce) verdicts
    , reportedBy = List.filter (\v -> not v.allowed && v.disposition == ReportOnly) verdicts
    , verdicts = verdicts
    }


verdictFor : Url -> Maybe Url -> Int -> Policy -> Check -> Verdict
verdictFor origin maybeUrl index policy check =
    let
        make directive allowed reason =
            { policy = index, disposition = policy.disposition, directive = directive, allowed = allowed, reason = reason }

        withDirective name decide =
            case effectiveDirective name policy of
                Nothing ->
                    make Nothing True ("no `" ++ name ++ "` and no fallback directive in this policy")

                Just d ->
                    let
                        ( ok, why ) =
                            decide d
                    in
                    make (Just d.name) ok why

        urlCheck name redirected =
            case maybeUrl of
                Nothing ->
                    make (Just name) False "the URL could not be parsed"

                Just url ->
                    withDirective name (\d -> urlDecision origin redirected url d)
    in
    case check of
        Load r ->
            case maybeUrl of
                Nothing ->
                    make (Just (kindDirective r.kind)) False "the URL could not be parsed"

                Just url ->
                    withDirective (kindDirective r.kind) (\d -> loadDecision origin url r d)

        InlineScript r ->
            withDirective "script-src-elem" (\d -> inlineDecision True True r.nonce r.digests d)

        ScriptAttribute r ->
            withDirective "script-src-attr" (\d -> inlineDecision True False Nothing r.digests d)

        JavascriptUrl r ->
            withDirective "script-src-elem" (\d -> inlineDecision True False Nothing r.digests d)

        InlineStyle r ->
            withDirective "style-src-elem" (\d -> inlineDecision False True r.nonce r.digests d)

        StyleAttribute r ->
            withDirective "style-src-attr" (\d -> inlineDecision False False Nothing r.digests d)

        Eval ->
            withDirective "script-src"
                (\d ->
                    if List.member KwUnsafeEval d.sources then
                        ( True, "'unsafe-eval' is present" )

                    else
                        ( False, "eval and new Function need 'unsafe-eval'" )
                )

        WasmCompile ->
            withDirective "script-src"
                (\d ->
                    if List.member KwWasmUnsafeEval d.sources || List.member KwUnsafeEval d.sources then
                        ( True, "'wasm-unsafe-eval' or 'unsafe-eval' is present" )

                    else
                        ( False, "WebAssembly compilation needs 'wasm-unsafe-eval'" )
                )

        FormSubmit _ ->
            urlCheck "form-action" False

        BaseUrl _ ->
            urlCheck "base-uri" False

        EmbedIn _ ->
            if policy.delivery == Meta then
                make Nothing True "frame-ancestors is ignored in a meta policy"

            else
                urlCheck "frame-ancestors" True


urlDecision : Url -> Bool -> Url -> Directive -> ( Bool, String )
urlDecision origin redirected url d =
    case List.filter (matchesSource origin redirected url) d.sources |> List.head of
        Just s ->
            ( True, "matched " ++ sourceToString s ++ " in `" ++ d.name ++ "`" )

        Nothing ->
            ( False, "no source in `" ++ d.name ++ "` matches " ++ urlToString url )


loadDecision : Url -> Url -> { kind : Kind, url : String, nonce : Maybe String, parserInserted : Bool, redirected : Bool } -> Directive -> ( Bool, String )
loadDecision origin url r d =
    let
        nonceApplies =
            r.kind == Script || r.kind == Style

        nonceHit =
            nonceApplies && nonceMatches r.nonce d.sources

        strictDynamic =
            r.kind == Script && List.member KwStrictDynamic d.sources
    in
    if nonceHit then
        ( True, "nonce matches `" ++ d.name ++ "`" )

    else if strictDynamic then
        if r.parserInserted then
            ( False, "'strict-dynamic' in `" ++ d.name ++ "` ignores host and scheme sources, and this parser inserted script has no matching nonce or hash" )

        else
            ( True, "'strict-dynamic' trusts a script inserted by already trusted script" )

    else
        urlDecision origin r.redirected url d


inlineDecision : Bool -> Bool -> Maybe String -> List Digest -> Directive -> ( Bool, String )
inlineDecision isScript isElement nonce digests d =
    let
        sources =
            d.sources

        hasNonceOrHash =
            List.any isNonceOrHash sources

        strictDynamic =
            isScript && List.member KwStrictDynamic sources

        hasUnsafeInline =
            List.member KwUnsafeInline sources

        unsafeInline =
            hasUnsafeInline && not hasNonceOrHash && not strictDynamic

        nonceHit =
            isElement && nonceMatches nonce sources

        hashesUsable =
            isElement || List.member KwUnsafeHashes sources

        hashHit =
            hashesUsable && List.any (\dg -> List.any (hashMatches dg) sources) digests
    in
    if unsafeInline then
        ( True, "'unsafe-inline' in `" ++ d.name ++ "` with no nonce, hash or 'strict-dynamic' beside it" )

    else if nonceHit then
        ( True, "nonce matches `" ++ d.name ++ "`" )

    else if hashHit then
        ( True, "hash matches `" ++ d.name ++ "`" )

    else if hasUnsafeInline then
        ( False, "'unsafe-inline' in `" ++ d.name ++ "` is switched off by a nonce, hash or 'strict-dynamic' in the same list" )

    else if not isElement && not (List.member KwUnsafeHashes sources) && List.any isHash sources && not (List.isEmpty digests) then
        ( False, "hashes only cover event handlers and javascript: URLs when 'unsafe-hashes' is present" )

    else
        ( False, "no nonce, hash or 'unsafe-inline' in `" ++ d.name ++ "` allows this" )


nonceMatches : Maybe String -> List Source -> Bool
nonceMatches nonce sources =
    case nonce of
        Just n ->
            n /= "" && List.member (Nonce n) sources

        Nothing ->
            False


hashMatches : Digest -> Source -> Bool
hashMatches dg source =
    case source of
        Hash algo v ->
            algo == dg.algo && normalizeBase64 v == normalizeBase64 dg.value

        _ ->
            False


describeCheck : Check -> String
describeCheck check =
    case check of
        Load r ->
            "load " ++ kindDirective r.kind ++ " " ++ r.url

        InlineScript _ ->
            "inline script element"

        ScriptAttribute _ ->
            "inline event handler attribute"

        JavascriptUrl _ ->
            "javascript: URL navigation"

        InlineStyle _ ->
            "inline style element"

        StyleAttribute _ ->
            "style attribute"

        Eval ->
            "eval or new Function"

        WasmCompile ->
            "WebAssembly compile"

        FormSubmit u ->
            "form submit to " ++ u

        BaseUrl u ->
            "base element href " ++ u

        EmbedIn u ->
            "framed by " ++ u



-- AUDIT


type alias ScriptAnalysis =
    { posture : ScriptPosture
    , findings : List Finding
    }


{-| The script injection posture of one policy on its own.
-}
scriptPosture : Config -> Url -> Policy -> ScriptPosture
scriptPosture config origin policy =
    (analyseScripts config origin policy).posture


analyseScripts : Config -> Url -> Policy -> ScriptAnalysis
analyseScripts config origin policy =
    case effectiveDirective "script-src-elem" policy of
        Nothing ->
            { posture = Unrestricted
            , findings = [ issue High "script-unrestricted" "script-src" "There is no script-src or default-src, so any script from anywhere runs, inline included." ]
            }

        Just d ->
            let
                s =
                    List.filter (not << isInvalid) d.sources

                name =
                    d.name

                nonceOrHash =
                    List.any isNonceOrHash s

                strictDynamic =
                    List.member KwStrictDynamic s

                onlyNone =
                    List.all (\x -> x == KwNone || x == KwReportSample) s

                inlineEffective =
                    List.member KwUnsafeInline s && not nonceOrHash && not strictDynamic

                hasData =
                    List.member (SchemeSource "data") s

                broad =
                    List.filter isBroad s

                urlSources =
                    List.filter isUrlSource s

                hosts =
                    List.filterMap asHost s

                bypassHits =
                    List.filter (\h -> List.any (bypassCovers h.host) config.bypassHosts) hosts

                insecure =
                    List.filter (isInsecureFor origin) s

                list items =
                    String.join ", " items

                posture =
                    if onlyNone then
                        Strict

                    else if inlineEffective then
                        InlineInjectable

                    else if strictDynamic then
                        Strict

                    else if hasData then
                        InlineInjectable

                    else if not (List.isEmpty broad) || not (List.isEmpty bypassHits) || not (List.isEmpty urlSources) then
                        Bypassable

                    else
                        Strict

                when cond f =
                    if cond then
                        [ f ]

                    else
                        []

                findings =
                    List.concat
                        [ when inlineEffective (issue High "script-unsafe-inline" name ("'unsafe-inline' in `" ++ name ++ "` lets any injected <script> or event handler run. Replace it with nonces or hashes."))
                        , when (List.member KwUnsafeInline s && not inlineEffective) (issue Info "unsafe-inline-fallback" name "'unsafe-inline' is ignored here because a nonce, hash or 'strict-dynamic' is present. It only helps browsers older than CSP2.")
                        , when (not strictDynamic && hasData) (issue High "script-data-uri" name ("`data:` in `" ++ name ++ "` lets an attacker write <script src=\"data:...\">, which is the same as allowing inline script."))
                        , when (not strictDynamic && not (List.isEmpty broad)) (issue High "script-broad-source" name ("`" ++ list (List.map sourceToString broad) ++ "` in `" ++ name ++ "` allows script from hosts an attacker controls."))
                        , when (not strictDynamic && not (List.isEmpty bypassHits)) (issue High "script-bypass-host" name ("`" ++ list (List.map hostToString bypassHits) ++ "` serves files anyone can publish. An attacker loads their own script, or an old library with script gadgets, from a host this policy trusts."))
                        , when (not strictDynamic && List.isEmpty broad && List.isEmpty bypassHits && List.any isHostOrScheme urlSources) (issue Medium "script-host-allowlist" name ("`" ++ name ++ "` trusts hosts instead of nonces or hashes. Any JSONP endpoint, upload path or old library on those hosts bypasses it. Prefer a nonce with 'strict-dynamic'."))
                        , when (not strictDynamic && not (List.any isHostOrScheme urlSources) && List.member KwSelf urlSources) (issue Low "script-self-allowlist" name ("'self' in `" ++ name ++ "` is only safe if no same origin URL serves attacker influenced JavaScript, such as JSONP callbacks or user uploads."))
                        , when (not strictDynamic && not (List.isEmpty insecure)) (issue Low "script-insecure-source" name ("`" ++ list (List.map sourceToString insecure) ++ "` allows script over plain http from an https page."))
                        , when (strictDynamic && not nonceOrHash) (issue Medium "strict-dynamic-without-nonce" name "'strict-dynamic' without a nonce or hash means no script can start the trust chain. The page will load no script at all.")
                        ]

                evalFindings =
                    case effectiveDirective "script-src" policy of
                        Just e ->
                            when (List.member KwUnsafeEval e.sources) (issue Medium "script-unsafe-eval" e.name "'unsafe-eval' lets injected strings reach eval, new Function and string setTimeout. If you only need WebAssembly, use 'wasm-unsafe-eval'.")

                        Nothing ->
                            []

                objectFindings =
                    case effectiveDirective "object-src" policy of
                        Nothing ->
                            [ issue Medium "object-src-missing" "object-src" "No object-src or default-src. <object> and <embed> can load active content from anywhere. Add object-src 'none'." ]

                        Just o ->
                            when (List.any isBroad o.sources) (issue Medium "object-src-broad" o.name ("`" ++ o.name ++ "` allows <object> and <embed> from any host. Set object-src 'none'."))

                baseFindings =
                    when (effectiveDirective "base-uri" policy == Nothing && not onlyNone)
                        (issue Medium "base-uri-missing" "base-uri" "No base-uri. An injected <base href> repoints every relative script URL, nonced ones included. Add base-uri 'none' or 'self'.")
            in
            { posture = posture
            , findings = findings ++ evalFindings ++ objectFindings ++ baseFindings
            }


policyFindings : Config -> Url -> Policy -> List Finding
policyFindings config origin policy =
    let
        script =
            analyseScripts config origin policy

        style =
            case effectiveDirective "style-src-elem" policy of
                Just d ->
                    if List.member KwUnsafeInline d.sources && not (List.any isNonceOrHash d.sources) then
                        [ issue Low "style-unsafe-inline" d.name "'unsafe-inline' for styles lets injected CSS read attribute values through selectors, a known data exfiltration path." ]

                    else
                        []

                Nothing ->
                    []

        reporting =
            if policy.disposition == ReportOnly && not (hasDirective "report-uri" policy || hasDirective "report-to" policy) then
                [ issue Medium "report-only-without-endpoint" "" "This report only policy has no report-uri or report-to, so violations go nowhere except the console and a SecurityPolicyViolationEvent." ]

            else
                []
    in
    policy.issues ++ script.findings ++ style ++ reporting


{-| Audit every policy on the page as a set. Findings are sorted by severity.
Script findings on a weak policy are downgraded to Info when another enforced
policy is strict, because every enforced policy must allow a script, so a
second weak policy cannot open a hole.
-}
audit : Config -> Url -> List Policy -> Report
audit config origin policies =
    let
        indexed =
            List.indexedMap Tuple.pair policies

        enforced =
            List.filter (\( _, p ) -> p.disposition == Enforce) indexed

        postures =
            List.map (\( i, p ) -> ( i, scriptPosture config origin p )) enforced

        best =
            List.sortBy (\( _, posture ) -> negate (postureRank posture)) postures |> List.head

        combinedPosture =
            Maybe.map Tuple.second best |> Maybe.withDefault Unrestricted

        strictIndex =
            case best of
                Just ( i, Strict ) ->
                    Just i

                _ ->
                    Nothing

        adjust i p f =
            let
                stamped =
                    { f | policy = i }
            in
            if p.disposition == ReportOnly && f.code /= "report-only-without-endpoint" then
                { stamped
                    | severity = capSeverity Low stamped.severity
                    , message = "[report only] " ++ stamped.message
                }

            else
                case strictIndex of
                    Just s ->
                        if s /= i && List.member f.code scriptFindingCodes then
                            { stamped
                                | severity = Info
                                , message = stamped.message ++ " Policy " ++ String.fromInt s ++ " is strict and also enforced, so this alone does not open an injection path."
                            }

                        else
                            stamped

                    Nothing ->
                        stamped

        perPolicy =
            List.concatMap (\( i, p ) -> List.map (adjust i p) (policyFindings config origin p)) indexed

        setLevel =
            List.concat
                [ if List.isEmpty policies then
                    [ { severity = High, code = "no-policy", policy = -1, directive = "", message = "No Content-Security-Policy was supplied." } ]

                  else if List.isEmpty enforced then
                    [ { severity = High, code = "no-enforced-policy", policy = -1, directive = "", message = "Every policy is report only, nothing is blocked." } ]

                  else
                    []
                , if not (List.isEmpty enforced) && not (List.any (\( _, p ) -> p.delivery == Header && hasDirective "frame-ancestors" p) enforced) then
                    [ { severity = Low, code = "frame-ancestors-missing", policy = -1, directive = "frame-ancestors", message = "No enforced header policy sets frame-ancestors, so the page can be framed for clickjacking unless X-Frame-Options covers it." } ]

                  else
                    []
                , conflicts origin policies enforced
                ]

        findings =
            (setLevel ++ perPolicy)
                |> List.sortBy (\f -> severityRank f.severity)

        pass =
            not (List.any (\f -> severityRank f.severity <= severityRank config.failOn) findings)
    in
    { posture = combinedPosture
    , strictPolicy = strictIndex
    , findings = findings
    , pass = pass
    , failOn = config.failOn
    }


{-| A nonce one enforced policy relies on is useless if another enforced
policy blocks nonced inline script. The page then breaks in production.
-}
conflicts : Url -> List Policy -> List ( Int, Policy ) -> List Finding
conflicts origin policies enforced =
    let
        noncesOf p =
            case effectiveDirective "script-src-elem" p of
                Just d ->
                    List.filterMap
                        (\s ->
                            case s of
                                Nonce n ->
                                    Just n

                                _ ->
                                    Nothing
                        )
                        d.sources

                Nothing ->
                    []

        check ( i, p ) =
            case noncesOf p of
                n :: _ ->
                    let
                        decision =
                            evaluate origin policies (InlineScript { nonce = Just n, digests = [] })
                    in
                    List.map
                        (\v ->
                            { severity = Medium
                            , code = "policies-conflict"
                            , policy = v.policy
                            , directive = Maybe.withDefault "" v.directive
                            , message = "Policy " ++ String.fromInt i ++ " allows nonced scripts but policy " ++ String.fromInt v.policy ++ " blocks them (" ++ v.reason ++ "). Both are enforced, so nonced scripts will not run."
                            }
                        )
                        (List.filter (\v -> v.policy /= i) decision.blockedBy)

                [] ->
                    []
    in
    List.concatMap check enforced



-- HELPERS


hasDirective : String -> Policy -> Bool
hasDirective name policy =
    List.any (\d -> d.name == name && not (policy.delivery == Meta && List.member name metaIgnoredDirectives)) policy.directives


isNonceOrHash : Source -> Bool
isNonceOrHash s =
    case s of
        Nonce _ ->
            True

        Hash _ _ ->
            True

        _ ->
            False


isHash : Source -> Bool
isHash s =
    case s of
        Hash _ _ ->
            True

        _ ->
            False


isInvalid : Source -> Bool
isInvalid s =
    case s of
        Invalid _ _ ->
            True

        _ ->
            False


isUrlSource : Source -> Bool
isUrlSource s =
    case s of
        Wildcard ->
            True

        SchemeSource _ ->
            True

        HostExpr _ ->
            True

        KwSelf ->
            True

        _ ->
            False


isHostOrScheme : Source -> Bool
isHostOrScheme s =
    isUrlSource s && s /= KwSelf


isBroad : Source -> Bool
isBroad s =
    case s of
        Wildcard ->
            True

        SchemeSource scheme ->
            List.member scheme [ "http", "https", "ws", "wss", "blob", "filesystem" ]

        HostExpr h ->
            h.host == "*"

        _ ->
            False


asHost : Source -> Maybe HostSource
asHost s =
    case s of
        HostExpr h ->
            Just h

        _ ->
            Nothing


isInsecureFor : Url -> Source -> Bool
isInsecureFor origin s =
    origin.scheme
        == "https"
        && (case s of
                HostExpr h ->
                    h.scheme == Just "http"

                _ ->
                    False
           )


{-| Does a policy host pattern cover a host where anyone can publish?
Entries starting with `*.` mean "any subdomain of".
-}
bypassCovers : String -> String -> Bool
bypassCovers policyHost entry =
    let
        entryWild =
            String.startsWith "*." entry

        base =
            if entryWild then
                String.dropLeft 2 entry

            else
                entry
    in
    if String.startsWith "*" policyHost then
        let
            suffix =
                String.dropLeft 1 policyHost
        in
        suffix == "" || String.endsWith suffix ("." ++ base) || (entryWild && String.endsWith ("." ++ base) suffix)

    else if entryWild then
        String.endsWith ("." ++ base) policyHost

    else
        policyHost == base


postureRank : ScriptPosture -> Int
postureRank p =
    case p of
        Unrestricted ->
            0

        InlineInjectable ->
            1

        Bypassable ->
            2

        Strict ->
            3


severityRank : Severity -> Int
severityRank s =
    case s of
        High ->
            0

        Medium ->
            1

        Low ->
            2

        Info ->
            3


{-| Caps a severity so it is no worse than `cap`.
-}
capSeverity : Severity -> Severity -> Severity
capSeverity cap s =
    if severityRank s < severityRank cap then
        cap

    else
        s


isDirectiveName : String -> Bool
isDirectiveName s =
    s /= "" && String.all (\c -> Char.isAlphaNum c || c == '-') s


isSchemeName : String -> Bool
isSchemeName s =
    case String.uncons s of
        Just ( c, rest ) ->
            Char.isAlpha c && String.all (\x -> Char.isAlphaNum x || x == '+' || x == '-' || x == '.') rest

        Nothing ->
            False


isSchemeSource : String -> Bool
isSchemeSource s =
    String.endsWith ":" s && isSchemeName (String.dropRight 1 s)


isHostPattern : String -> Bool
isHostPattern host =
    let
        labelOk l =
            l /= "" && String.all (\c -> Char.isAlphaNum c || c == '-') l

        labels h =
            List.all labelOk (String.split "." h)
    in
    if host == "*" then
        True

    else if String.startsWith "*." host then
        labels (String.dropLeft 2 host)

    else
        labels host


isIpLiteral : String -> Bool
isIpLiteral host =
    String.startsWith "[" host || (host /= "" && String.all (\c -> Char.isDigit c || c == '.') host)


isBase64Value : String -> Bool
isBase64Value v =
    let
        body =
            stripPadding v

        padding =
            String.length v - String.length body
    in
    body /= "" && padding <= 2 && String.all (\c -> Char.isAlphaNum c || c == '+' || c == '/' || c == '-' || c == '_') body


stripPadding : String -> String
stripPadding v =
    if String.endsWith "=" v then
        stripPadding (String.dropRight 1 v)

    else
        v


{-| CSP3 treats base64url and base64 digests as equal, padding included or not.
-}
normalizeBase64 : String -> String
normalizeBase64 v =
    stripPadding v |> String.replace "-" "+" |> String.replace "_" "/"


percentDecode : String -> String
percentDecode s =
    let
        hex c =
            let
                code =
                    Char.toCode (Char.toLower c)
            in
            if code >= 48 && code <= 57 then
                Just (code - 48)

            else if code >= 97 && code <= 102 then
                Just (code - 87)

            else
                Nothing

        go chars acc =
            case chars of
                '%' :: a :: b :: rest ->
                    case ( hex a, hex b ) of
                        ( Just x, Just y ) ->
                            go rest (Char.fromCode (x * 16 + y) :: acc)

                        _ ->
                            go (a :: b :: rest) ('%' :: acc)

                c :: rest ->
                    go rest (c :: acc)

                [] ->
                    List.reverse acc
    in
    String.fromList (go (String.toList s) [])


closestDirective : String -> Maybe String
closestDirective name =
    (knownDirectives ++ removedDirectives)
        |> List.map (\k -> ( levenshtein name k, k ))
        |> List.filter (\( dist, _ ) -> dist <= 2)
        |> List.sortBy Tuple.first
        |> List.head
        |> Maybe.map Tuple.second


levenshtein : String -> String -> Int
levenshtein a b =
    let
        bChars =
            String.toList b

        nextRow ( i, ca ) prev =
            let
                go prevRow chars left acc =
                    case ( prevRow, chars ) of
                        ( diag :: up :: restPrev, cb :: restChars ) ->
                            let
                                cost =
                                    if ca == cb then
                                        0

                                    else
                                        1

                                v =
                                    min (min (up + 1) (left + 1)) (diag + cost)
                            in
                            go (up :: restPrev) restChars v (v :: acc)

                        _ ->
                            List.reverse acc
            in
            go prev bChars (i + 1) [ i + 1 ]

        final =
            List.foldl nextRow (List.range 0 (List.length bChars)) (List.indexedMap Tuple.pair (String.toList a))
    in
    List.reverse final |> List.head |> Maybe.withDefault 0



-- STRINGS


hostToString : HostSource -> String
hostToString h =
    (case h.scheme of
        Just s ->
            s ++ "://"

        Nothing ->
            ""
    )
        ++ h.host
        ++ (case h.port_ of
                NoPort ->
                    ""

                AnyPort ->
                    ":*"

                Port p ->
                    ":" ++ String.fromInt p
           )
        ++ h.path


sourceToString : Source -> String
sourceToString s =
    case s of
        KwNone ->
            "'none'"

        KwSelf ->
            "'self'"

        KwUnsafeInline ->
            "'unsafe-inline'"

        KwUnsafeEval ->
            "'unsafe-eval'"

        KwWasmUnsafeEval ->
            "'wasm-unsafe-eval'"

        KwUnsafeHashes ->
            "'unsafe-hashes'"

        KwStrictDynamic ->
            "'strict-dynamic'"

        KwReportSample ->
            "'report-sample'"

        KwInlineSpeculationRules ->
            "'inline-speculation-rules'"

        Nonce v ->
            "'nonce-" ++ v ++ "'"

        Hash algo v ->
            "'" ++ algoToString algo ++ "-" ++ v ++ "'"

        SchemeSource scheme ->
            scheme ++ ":"

        HostExpr h ->
            hostToString h

        Wildcard ->
            "*"

        Invalid token _ ->
            token


algoToString : HashAlgo -> String
algoToString a =
    case a of
        Sha256 ->
            "sha256"

        Sha384 ->
            "sha384"

        Sha512 ->
            "sha512"


severityToString : Severity -> String
severityToString s =
    case s of
        High ->
            "high"

        Medium ->
            "medium"

        Low ->
            "low"

        Info ->
            "info"


severityFromString : String -> Maybe Severity
severityFromString s =
    case String.toLower s of
        "high" ->
            Just High

        "medium" ->
            Just Medium

        "low" ->
            Just Low

        "info" ->
            Just Info

        _ ->
            Nothing


postureToString : ScriptPosture -> String
postureToString p =
    case p of
        Unrestricted ->
            "unrestricted"

        InlineInjectable ->
            "inline-injectable"

        Bypassable ->
            "bypassable"

        Strict ->
            "strict"


dispositionToString : Disposition -> String
dispositionToString d =
    case d of
        Enforce ->
            "enforce"

        ReportOnly ->
            "report-only"



-- JSON


encodeFinding : Finding -> E.Value
encodeFinding f =
    E.object
        [ ( "severity", E.string (severityToString f.severity) )
        , ( "code", E.string f.code )
        , ( "policy", E.int f.policy )
        , ( "directive", E.string f.directive )
        , ( "message", E.string f.message )
        ]


encodeReport : Report -> E.Value
encodeReport r =
    let
        count sev =
            List.length (List.filter (\f -> f.severity == sev) r.findings)
    in
    E.object
        [ ( "pass", E.bool r.pass )
        , ( "failOn", E.string (severityToString r.failOn) )
        , ( "scriptPosture", E.string (postureToString r.posture) )
        , ( "strictPolicy", Maybe.map E.int r.strictPolicy |> Maybe.withDefault E.null )
        , ( "counts"
          , E.object
                [ ( "high", E.int (count High) )
                , ( "medium", E.int (count Medium) )
                , ( "low", E.int (count Low) )
                , ( "info", E.int (count Info) )
                ]
          )
        , ( "findings", E.list encodeFinding r.findings )
        ]


encodeVerdict : Verdict -> E.Value
encodeVerdict v =
    E.object
        [ ( "policy", E.int v.policy )
        , ( "disposition", E.string (dispositionToString v.disposition) )
        , ( "directive", Maybe.map E.string v.directive |> Maybe.withDefault E.null )
        , ( "allowed", E.bool v.allowed )
        , ( "reason", E.string v.reason )
        ]


encodeDecision : Decision -> E.Value
encodeDecision d =
    E.object
        [ ( "check", E.string d.check )
        , ( "allowed", E.bool d.allowed )
        , ( "effectiveUrl", Maybe.map E.string d.effectiveUrl |> Maybe.withDefault E.null )
        , ( "blockedBy", E.list encodeVerdict d.blockedBy )
        , ( "reportedBy", E.list encodeVerdict d.reportedBy )
        , ( "verdicts", E.list encodeVerdict d.verdicts )
        ]


encodePolicy : Int -> Policy -> E.Value
encodePolicy index p =
    E.object
        [ ( "index", E.int index )
        , ( "disposition", E.string (dispositionToString p.disposition) )
        , ( "delivery"
          , E.string
                (case p.delivery of
                    Header ->
                        "header"

                    Meta ->
                        "meta"
                )
          )
        , ( "directives"
          , E.list
                (\d ->
                    E.object
                        [ ( "name", E.string d.name )
                        , ( "sources", E.list (E.string << sourceToString) (List.filter (not << isInvalid) d.sources) )
                        , ( "tokens", E.list E.string d.tokens )
                        ]
                )
                p.directives
          )
        ]
