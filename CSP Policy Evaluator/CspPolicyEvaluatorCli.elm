port module CspPolicyEvaluatorCli exposing (main)

{-| Headless entry point. Run it as a Platform.worker under node: pass the
request as flags, read one JSON report from the `emit` port. See the README for
the input shape and a CI exit code snippet.
-}

import CspPolicyEvaluator as Csp
import Json.Decode as D
import Json.Encode as E
import Process
import Task


port emit : String -> Cmd msg


type Msg
    = Go


{-| The report is sent one tick after init so the host has time to subscribe
to the port before the message goes out.
-}
main : Program D.Value D.Value Msg
main =
    Platform.worker
        { init = \flags -> ( flags, Task.perform (\_ -> Go) (Process.sleep 0) )
        , update = \Go flags -> ( flags, emit (run flags) )
        , subscriptions = \_ -> Sub.none
        }


type alias Input =
    { origin : String
    , headers : List String
    , reportOnlyHeaders : List String
    , metas : List String
    , checks : List D.Value
    , failOn : String
    , bypassHosts : Maybe (List String)
    , extraBypassHosts : List String
    }


inputDecoder : D.Decoder Input
inputDecoder =
    D.map8 Input
        (D.field "origin" D.string)
        (optionalList "headers" D.string)
        (optionalList "reportOnlyHeaders" D.string)
        (optionalList "metas" D.string)
        (optionalList "checks" D.value)
        (D.oneOf [ D.field "failOn" D.string, D.succeed "high" ])
        (D.maybe (D.field "bypassHosts" (D.list D.string)))
        (optionalList "extraBypassHosts" D.string)


optionalList : String -> D.Decoder a -> D.Decoder (List a)
optionalList name decoder =
    D.oneOf [ D.field name (D.list decoder), D.succeed [] ]


run : D.Value -> String
run flags =
    case D.decodeValue inputDecoder flags of
        Err err ->
            failure ("bad input: " ++ D.errorToString err)

        Ok input ->
            case ( Csp.parseUrl input.origin, Csp.severityFromString input.failOn ) of
                ( Nothing, _ ) ->
                    failure ("origin is not an absolute URL: " ++ input.origin)

                ( _, Nothing ) ->
                    failure ("failOn must be high, medium, low or info, got " ++ input.failOn)

                ( Just origin, Just failOn ) ->
                    let
                        policies =
                            List.concatMap (Csp.parseHeader Csp.Enforce) input.headers
                                ++ List.concatMap (Csp.parseHeader Csp.ReportOnly) input.reportOnlyHeaders
                                ++ List.map Csp.parseMeta input.metas

                        config =
                            { bypassHosts = Maybe.withDefault Csp.defaultBypassHosts input.bypassHosts ++ input.extraBypassHosts
                            , failOn = failOn
                            }

                        decodedChecks =
                            List.map (D.decodeValue expectationDecoder) input.checks

                        badCheck =
                            List.indexedMap Tuple.pair decodedChecks
                                |> List.filterMap
                                    (\( i, r ) ->
                                        case r of
                                            Err e ->
                                                Just ("check " ++ String.fromInt i ++ ": " ++ D.errorToString e)

                                            Ok _ ->
                                                Nothing
                                    )
                    in
                    case badCheck of
                        first :: _ ->
                            failure first

                        [] ->
                            let
                                expectations =
                                    List.filterMap Result.toMaybe decodedChecks

                                report =
                                    Csp.audit config origin policies

                                outcomes =
                                    List.map
                                        (\x ->
                                            let
                                                decision =
                                                    Csp.evaluate origin policies x.check

                                                asExpected =
                                                    case x.expect of
                                                        Just wanted ->
                                                            wanted == decision.allowed

                                                        Nothing ->
                                                            True
                                            in
                                            ( x, decision, asExpected )
                                        )
                                        expectations

                                checksPass =
                                    List.all (\( _, _, ok ) -> ok) outcomes
                            in
                            E.encode 2
                                (E.object
                                    [ ( "ok", E.bool True )
                                    , ( "pass", E.bool (report.pass && checksPass) )
                                    , ( "auditPass", E.bool report.pass )
                                    , ( "checksPass", E.bool checksPass )
                                    , ( "policies", E.list identity (List.indexedMap Csp.encodePolicy policies) )
                                    , ( "report", Csp.encodeReport report )
                                    , ( "checks", E.list encodeOutcome outcomes )
                                    ]
                                )


type alias Expectation =
    { label : String
    , expect : Maybe Bool
    , check : Csp.Check
    }


expectationDecoder : D.Decoder Expectation
expectationDecoder =
    D.map3 Expectation
        (D.oneOf [ D.field "label" D.string, D.succeed "" ])
        (D.maybe (D.field "expect" D.string)
            |> D.andThen
                (\m ->
                    case m of
                        Nothing ->
                            D.succeed Nothing

                        Just "allow" ->
                            D.succeed (Just True)

                        Just "block" ->
                            D.succeed (Just False)

                        Just other ->
                            D.fail ("expect must be allow or block, got " ++ other)
                )
        )
        checkDecoder


encodeOutcome : ( Expectation, Csp.Decision, Bool ) -> E.Value
encodeOutcome ( x, decision, asExpected ) =
    E.object
        [ ( "label", E.string x.label )
        , ( "expect"
          , case x.expect of
                Just True ->
                    E.string "allow"

                Just False ->
                    E.string "block"

                Nothing ->
                    E.null
          )
        , ( "asExpected", E.bool asExpected )
        , ( "decision", Csp.encodeDecision decision )
        ]


failure : String -> String
failure message =
    E.encode 2 (E.object [ ( "ok", E.bool False ), ( "error", E.string message ) ])


checkDecoder : D.Decoder Csp.Check
checkDecoder =
    D.field "type" D.string |> D.andThen checkOfType


checkOfType : String -> D.Decoder Csp.Check
checkOfType t =
    let
        digests =
            optionalList "digests" D.string
                |> D.andThen
                    (\raws ->
                        case List.filter (\r -> Csp.parseDigest r == Nothing) raws of
                            bad :: _ ->
                                D.fail ("digest must look like sha256-<base64>, got " ++ bad)

                            [] ->
                                D.succeed (List.filterMap Csp.parseDigest raws)
                    )

        nonce =
            D.maybe (D.field "nonce" D.string)

        url =
            D.field "url" D.string
    in
    case t of
        "load" ->
            D.map5
                (\kind u n parser redirected -> Csp.Load { kind = kind, url = u, nonce = n, parserInserted = parser, redirected = redirected })
                (D.field "kind" D.string
                    |> D.andThen
                        (\k ->
                            case Csp.kindFromString k of
                                Just kind ->
                                    D.succeed kind

                                Nothing ->
                                    D.fail ("unknown kind " ++ k)
                        )
                )
                url
                nonce
                (D.oneOf [ D.field "parserInserted" D.bool, D.succeed True ])
                (D.oneOf [ D.field "redirected" D.bool, D.succeed False ])

        "inline-script" ->
            D.map2 (\n d -> Csp.InlineScript { nonce = n, digests = d }) nonce digests

        "script-attribute" ->
            D.map (\d -> Csp.ScriptAttribute { digests = d }) digests

        "javascript-url" ->
            D.map (\d -> Csp.JavascriptUrl { digests = d }) digests

        "inline-style" ->
            D.map2 (\n d -> Csp.InlineStyle { nonce = n, digests = d }) nonce digests

        "style-attribute" ->
            D.map (\d -> Csp.StyleAttribute { digests = d }) digests

        "eval" ->
            D.succeed Csp.Eval

        "wasm" ->
            D.succeed Csp.WasmCompile

        "form" ->
            D.map Csp.FormSubmit url

        "base" ->
            D.map Csp.BaseUrl url

        "frame-ancestor" ->
            D.map Csp.EmbedIn url

        other ->
            D.fail ("unknown check type " ++ other)
