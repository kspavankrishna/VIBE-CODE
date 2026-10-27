port module AgentApprovalQueueSelfCheck exposing (main)

{-| Scenario checks for AgentApprovalQueue. Compile with `elm make` and run the
output under node, see the README. Prints one PASS or FAIL line per scenario and a
final summary line. No test package is needed.
-}

import AgentApprovalQueue as Q exposing (Effect(..), Msg(..), Refusal(..), Risk(..), Verdict(..))
import Json.Decode as Decode
import Json.Encode as Encode


port report : String -> Cmd msg


main : Program () () ()
main =
    Platform.worker
        { init = \_ -> ( (), report (summarise results) )
        , update = \_ m -> ( m, Cmd.none )
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
        ++ "\nSUMMARY "
        ++ String.fromInt (List.length rs - failed)
        ++ " passed, "
        ++ String.fromInt failed
        ++ " failed"



-- HARNESS


cfg : Q.Config
cfg =
    { defaultConfig_
        | rules =
            [ { tool = "shell.*", risk = Dual }
            , { tool = "read.*", risk = Auto }
            , { tool = "db.drop*", risk = Deny }
            , { tool = "db.*", risk = Review }
            ]
        , reviewers = [ "alice", "bob", "carol", "dave" ]
        , minReviewMs = 1000
        , ttlReviewMs = 60000
        , ttlDualMs = 120000
        , fatigueMax = 3
        , fatigueWindowMs = 10000
        , maxWaitingPerAgent = 3
        , maxBatch = 3
        , maxArgsChars = 200
    }


defaultConfig_ : Q.Config
defaultConfig_ =
    Q.defaultConfig


sub : String -> String -> String -> Q.Submission
sub id tool args =
    { id = id, agent = "agent1", requestedBy = "dave", tool = tool, args = args }


opts : Q.ApproveOptions
opts =
    { note = "", grantMs = Nothing }


run : Q.Config -> List Msg -> ( Q.Model, List Effect )
run config msgs =
    List.foldl
        (\m ( model, acc ) ->
            let
                ( next, effs ) =
                    Q.update m model
            in
            ( next, acc ++ effs )
        )
        ( Q.init config, [] )
        msgs


continue : Q.Model -> List Msg -> ( Q.Model, List Effect )
continue start msgs =
    List.foldl
        (\m ( model, acc ) ->
            let
                ( next, effs ) =
                    Q.update m model
            in
            ( next, acc ++ effs )
        )
        ( start, [] )
        msgs


dispatches : List Effect -> List String
dispatches effs =
    List.filterMap
        (\e ->
            case e of
                Dispatch d ->
                    Just d.id

                _ ->
                    Nothing
        )
        effs


refusals : List Effect -> List Refusal
refusals effs =
    List.filterMap
        (\e ->
            case e of
                Refused _ _ r ->
                    Just r

                _ ->
                    Nothing
        )
        effs


responses : List Effect -> List ( String, Verdict )
responses effs =
    List.filterMap
        (\e ->
            case e of
                Respond id _ v ->
                    Just ( id, v )

                _ ->
                    Nothing
        )
        effs


audits : List Effect -> List Int
audits effs =
    List.filterMap
        (\e ->
            case e of
                Audit a ->
                    Just a.seq

                _ ->
                    Nothing
        )
        effs


review : Int -> String -> String -> List Msg
review t id who =
    [ View t id who, Approve (t + 1000) id who opts ]



-- SCENARIOS


results : List ( String, Bool )
results =
    [ canonicalization
    , classification
    , autoAndDeny
    , reviewDwell
    , selfApproval
    , dualControl
    , argumentSwap
    , idempotentRetry
    , coalescing
    , expiry
    , grants
    , fatigue
    , batches
    , overload
    , withdrawal
    , reject
    , snapshot
    , oversize
    , auditOrder
    , fingerprints
    ]


canonicalization : ( String, Bool )
canonicalization =
    let
        canon text =
            Decode.decodeString Decode.value text
                |> Result.mapError Decode.errorToString
                |> Result.andThen Q.canonicalJson
    in
    ( "canonical json ignores key order and unicode escapes"
    , canon "{\"b\":1,\"a\":\"\\u0072m\"}" == canon "{ \"a\" : \"rm\", \"b\": 1 }"
        && canon "{\"b\":1,\"a\":\"\\u0072m\"}" == Ok "{\"a\":\"rm\",\"b\":1}"
    )


classification : ( String, Bool )
classification =
    ( "classify: longest rule wins, triggers only raise, unknown falls back"
    , Q.classify cfg "db.query" "{}"
        == Review
        && Q.classify cfg "db.drop_all" "{}"
        == Deny
        && Q.classify cfg "read.file" "{}"
        == Auto
        && Q.classify cfg "read.file" "{\"cmd\":\"RM -RF /\"}"
        == Dual
        && Q.classify cfg "mystery" "{}"
        == Review
    )


autoAndDeny : ( String, Bool )
autoAndDeny =
    let
        ( _, effs ) =
            run cfg [ Submit 0 (sub "a1" "read.file" "{}"), Submit 0 (sub "d1" "db.drop_all" "{}") ]
    in
    ( "auto dispatches at once, deny never dispatches"
    , dispatches effs == [ "a1" ] && List.member ( "d1", Denied Q.PolicyDenied ) (responses effs)
    )


reviewDwell : ( String, Bool )
reviewDwell =
    let
        ( model, effs ) =
            run cfg
                [ Submit 0 (sub "c1" "db.query" "{}")
                , Approve 100 "c1" "alice" opts
                , View 200 "c1" "alice"
                , Approve 700 "c1" "alice" opts
                , Approve 1200 "c1" "alice" opts
                ]
    in
    ( "approval needs a view and the minimum dwell time"
    , refusals effs == [ NotViewed, ReviewedTooFast 500 ]
        && dispatches effs == [ "c1" ]
        && Q.lookup "c1" model == Q.Decided (Approved (Q.ByReviewers [ "alice" ]))
    )


selfApproval : ( String, Bool )
selfApproval =
    let
        ( _, effs ) =
            run cfg ([ Submit 0 (sub "c1" "db.query" "{}") ] ++ review 0 "c1" "dave" ++ review 0 "c1" "mallory")
    in
    ( "requester cannot approve and strangers are refused"
    , refusals effs == [ SelfApproval, NotAuthorized, NotAuthorized ] && dispatches effs == []
    )


dualControl : ( String, Bool )
dualControl =
    let
        ( _, effs ) =
            run cfg
                ([ Submit 0 (sub "s1" "shell.run" "{}") ]
                    ++ review 0 "s1" "alice"
                    ++ [ Approve 3000 "s1" "alice" opts ]
                    ++ review 4000 "s1" "bob"
                )
    in
    ( "dual risk needs two distinct reviewers"
    , refusals effs == [ AlreadyApprovedByYou ] && dispatches effs == [ "s1" ]
    )


argumentSwap : ( String, Bool )
argumentSwap =
    let
        ( model, effs ) =
            run cfg [ Submit 0 (sub "c1" "db.query" "{\"q\":1}"), Submit 10 (sub "c1" "db.query" "{\"q\":2}") ]
    in
    ( "reusing a call id with new arguments denies the pending call"
    , Q.lookup "c1" model == Q.Decided (Denied Q.ArgsChanged) && dispatches effs == [] && refusals effs == [ IdReuse ]
    )


idempotentRetry : ( String, Bool )
idempotentRetry =
    let
        ( _, effs ) =
            run cfg ([ Submit 0 (sub "c1" "db.query" "{}") ] ++ review 0 "c1" "alice" ++ [ Submit 5000 (sub "c1" "db.query" "{}") ])
    in
    ( "retrying a settled call repeats the answer without dispatching twice"
    , dispatches effs == [ "c1" ] && List.length (List.filter (\( id, _ ) -> id == "c1") (responses effs)) == 2
    )


coalescing : ( String, Bool )
coalescing =
    let
        ( model, effs ) =
            run cfg ([ Submit 0 (sub "c1" "db.query" "{}"), Submit 1 (sub "c2" "db.query" "{}") ] ++ review 0 "c1" "alice")

        asked =
            List.length (List.filter (\e -> case e of
                                                NeedsReview _ ->
                                                    True

                                                _ ->
                                                    False
                                     ) effs)
    in
    ( "identical calls share one review and both run"
    , asked == 1 && dispatches effs == [ "c1", "c2" ] && (Q.stats model).coalesced == 1
    )


expiry : ( String, Bool )
expiry =
    let
        ( model, effs ) =
            run cfg
                [ Submit 0 (sub "c1" "db.query" "{}")
                , Tick 59999
                , Tick 60000
                , View 61000 "c1" "alice"
                , Approve 70000 "c1" "alice" opts
                ]
    in
    ( "calls expire on tick, schedule the timer and refuse late approval"
    , Q.lookup "c1" model == Q.Decided (Denied Q.Expired)
        && List.member (ScheduleTick (Just 60000)) effs
        && List.member (ScheduleTick Nothing) effs
        && dispatches effs == []
    )


grants : ( String, Bool )
grants =
    let
        withGrant =
            { note = "ok", grantMs = Just 5000 }

        ( model, effs ) =
            run cfg
                [ Submit 0 (sub "c1" "db.query" "{}")
                , View 0 "c1" "alice"
                , Approve 1000 "c1" "alice" withGrant
                , Submit 2000 (sub "c2" "db.query" "{}")
                , Submit 2100 (sub "c3" "db.query" "{\"x\":1}")
                , Submit 7000 (sub "c4" "db.query" "{}")
                , Submit 7000 (sub "s1" "shell.run" "{}")
                , View 7000 "s1" "bob"
                , Approve 9000 "s1" "bob" withGrant
                ]
    in
    ( "grants cover the exact call once, expire, and never apply to dual"
    , dispatches effs == [ "c1", "c2" ]
        && Q.lookup "c2" model == Q.Decided (Approved (Q.ByGrant "alice"))
        && (case Q.lookup "c3" model of
                Q.StillWaiting _ ->
                    True

                _ ->
                    False
           )
        && refusals effs == [ GrantNotAllowed ]
    )


fatigue : ( String, Bool )
fatigue =
    let
        submits =
            List.map (\n -> Submit 0 (sub ("c" ++ String.fromInt n) "db.query" ("{\"n\":" ++ String.fromInt n ++ "}"))) [ 1, 2, 3 ]

        views =
            List.map (\n -> View 0 ("c" ++ String.fromInt n) "alice") [ 1, 2, 3 ]

        approvals =
            List.map (\n -> Approve (2000 + n) ("c" ++ String.fromInt n) "alice" opts) [ 1, 2, 3 ]

        ( model, effs ) =
            run { cfg | maxWaitingPerAgent = 10 }
                (submits ++ views ++ approvals ++ [ Submit 2100 (sub "c4" "db.query" "{\"n\":4}"), View 2100 "c4" "alice", Approve 3200 "c4" "alice" opts ])

        ( _, later ) =
            continue model [ Approve 13000 "c4" "alice" opts ]
    in
    ( "fatigue brake stops a fourth quick approval and lifts after the window"
    , (case refusals effs of
        [ FatigueBrake ms ] ->
            ms > 0

        _ ->
            False
      )
        && dispatches later == [ "c4" ]
    )


batches : ( String, Bool )
batches =
    let
        ( _, effs ) =
            run cfg
                [ Submit 0 (sub "c1" "db.query" "{\"n\":1}")
                , Submit 0 (sub "c2" "db.query" "{\"n\":2}")
                , Submit 0 (sub "s1" "shell.run" "{}")
                , View 0 "c1" "alice"
                , View 0 "c2" "alice"
                , View 0 "s1" "alice"
                , ApproveBatch 2000 [ "c1", "s1", "c2" ] "alice"
                , ApproveBatch 2000 [ "a", "b", "c", "d" ] "alice"
                ]
    in
    ( "batch skips dual risk and rejects oversized lists"
    , dispatches effs == [ "c1", "c2" ] && refusals effs == [ BatchNotAllowed Dual, BatchTooLarge ]
    )


overload : ( String, Bool )
overload =
    let
        ( _, effs ) =
            run cfg (List.map (\n -> Submit 0 (sub ("c" ++ String.fromInt n) "db.query" ("{\"n\":" ++ String.fromInt n ++ "}"))) [ 1, 2, 3, 4 ])
    in
    ( "one agent cannot flood the review queue"
    , List.member ( "c4", Denied Q.Overloaded ) (responses effs)
    )


withdrawal : ( String, Bool )
withdrawal =
    let
        ( model, effs ) =
            run cfg
                [ Submit 0 (sub "c1" "db.query" "{}")
                , Submit 1 (sub "c2" "db.query" "{}")
                , Withdraw 5 "c1" "intruder"
                , Withdraw 6 "c1" "agent1"
                ]
    in
    ( "agent can withdraw one of two merged calls, others cannot"
    , refusals effs == [ NotAuthorized ]
        && Q.lookup "c1" model == Q.Decided (Denied Q.Withdrawn)
        && (case Q.lookup "c2" model of
                Q.StillWaiting item ->
                    item.ids == [ "c2" ]

                _ ->
                    False
           )
    )


reject : ( String, Bool )
reject =
    let
        ( model, effs ) =
            run cfg [ Submit 0 (sub "s1" "shell.run" "{}"), Reject 5 "s1" "carol" "touches prod" ]
    in
    ( "one rejection vetoes even a dual call"
    , Q.lookup "s1" model == Q.Decided (Denied (Q.RejectedBy "carol" "touches prod")) && dispatches effs == []
    )


snapshot : ( String, Bool )
snapshot =
    let
        ( model, _ ) =
            run cfg
                [ Submit 0 (sub "c1" "db.query" "{}")
                , Submit 0 (sub "s1" "shell.run" "{}")
                , View 0 "s1" "alice"
                , Approve 2000 "s1" "alice" opts
                , Submit 0 (sub "r1" "read.file" "{}")
                ]

        restored =
            Decode.decodeValue (Q.snapshotDecoder cfg) (Q.encodeSnapshot model)
    in
    ( "snapshot round trip keeps waiting items, approvals and settled answers"
    , case restored of
        Ok m ->
            List.map .key (Q.waitingItems m)
                == List.map .key (Q.waitingItems model)
                && Q.lookup "r1" m
                == Q.Decided (Approved Q.ByPolicy)
                && (case Q.lookup "s1" m of
                        Q.StillWaiting item ->
                            List.length item.approvals == 1

                        _ ->
                            False
                   )
                && (Tuple.first (Q.update (Tick 999999) m) |> Q.lookup "c1") == Q.Decided (Denied Q.Expired)

        Err _ ->
            False
    )


oversize : ( String, Bool )
oversize =
    let
        ( _, effs ) =
            run cfg [ Submit 0 (sub "c1" "db.query" (String.repeat 300 "x")) ]
    in
    ( "oversized arguments are denied at the door"
    , responses effs == [ ( "c1", Denied Q.TooLarge ) ]
    )


auditOrder : ( String, Bool )
auditOrder =
    let
        ( _, effs ) =
            run cfg ([ Submit 0 (sub "c1" "db.query" "{}") ] ++ review 0 "c1" "alice")

        seqs =
            audits effs
    in
    ( "audit sequence numbers are strictly increasing"
    , List.length seqs >= 3 && seqs == List.sort seqs && seqs == List.range 1 (List.length seqs)
    )


fingerprints : ( String, Bool )
fingerprints =
    ( "fingerprint is stable, 16 hex chars and sensitive to both parts"
    , Q.fingerprint "t" "{}"
        == Q.fingerprint "t" "{}"
        && String.length (Q.fingerprint "t" "{}")
        == 16
        && Q.fingerprint "t" "{}"
        /= Q.fingerprint "t" "{ }"
        && Q.fingerprint "t" "{}"
        /= Q.fingerprint "u" "{}"
        && Encode.encode 0 (Encode.string (Q.fingerprint "t" "{}"))
        /= ""
    )
