module AgentApprovalQueue exposing
    ( Model, Config, defaultConfig, init
    , Msg(..), ApproveOptions, update
    , Effect(..), AuditEntry, Refusal(..), Verdict(..), ApprovedVia(..), DenyReason(..)
    , Risk(..), Rule, Trigger, classify
    , Submission, submissionDecoder, canonicalJson, fingerprint
    , Item, Status(..), lookup, waitingItems, grantCount, Stats, stats
    , encodeSnapshot, snapshotDecoder
    )

{-| A pure approval queue for AI agent tool calls that need a human decision.

The hard part of human in the loop is not the button. It is everything around it:
an agent that resubmits the same call with different arguments after approval, a
reviewer who taps approve on forty rows without reading one, a "second pair of eyes"
that turns out to be the person who asked for the action, a late approval that lands
after the call already timed out, and a page reload that silently forgets what was
pending. This module owns those cases and performs no IO.

The host feeds `Msg` values in (agent submissions, reviewer clicks, timer ticks) and
executes the returned `Effect` values (run the tool, answer the agent, ping a
reviewer, persist an audit record, arm a timer).

Rules the queue enforces, in the order a call meets them:

  - the call is fingerprinted from the tool name and canonical arguments, so key
    order and unicode escapes cannot dodge a rule or an approval
  - effective risk is the stricter of the tool rule and any argument trigger, and an
    unknown tool falls back to `defaultRisk`, which should not be `Auto`
  - `Deny` never reaches a human, `Auto` never waits, `Review` needs one reviewer,
    `Dual` needs two distinct reviewers, neither of whom asked for the call
  - an approval is bound to the exact arguments that were shown, and reusing a call
    id with different arguments denies the pending call
  - a reviewer must have opened the call and stayed on it for `minReviewMs`
  - a reviewer who approves too fast in a window hits a fatigue brake
  - everything expires, including approvals, grants and the call itself

-}

import Bitwise
import Dict exposing (Dict)
import Json.Decode as Decode exposing (Decoder)
import Json.Encode as Encode



-- IDENTIFIERS


type alias CallId =
    String


type alias AgentId =
    String


type alias Reviewer =
    String



-- RISK POLICY


type Risk
    = Auto
    | Review
    | Dual
    | Deny


{-| Tool name rule. `tool` is an exact name or a prefix ending in `*`. When several
rules match, the longest `tool` string wins, so `db.drop*` beats `db.*`.
-}
type alias Rule =
    { tool : String
    , risk : Risk
    }


{-| Case insensitive substring found in the canonical arguments. A trigger can only
raise risk, never lower it.
-}
type alias Trigger =
    { needle : String
    , risk : Risk
    }


type alias Config =
    { rules : List Rule
    , triggers : List Trigger
    , defaultRisk : Risk
    , reviewers : List Reviewer
    , ttlReviewMs : Int
    , ttlDualMs : Int
    , minReviewMs : Int
    , maxWaiting : Int
    , maxWaitingPerAgent : Int
    , maxSettled : Int
    , maxBatch : Int
    , maxArgsChars : Int
    , maxNoteChars : Int
    , maxGrantMs : Int
    , maxGrantUses : Int
    , fatigueWindowMs : Int
    , fatigueMax : Int
    }


{-| Conservative defaults. An empty `reviewers` list means any identifier may review,
which is only sensible in development. Set it in production.
-}
defaultConfig : Config
defaultConfig =
    { rules = []
    , triggers =
        [ { needle = "rm -rf", risk = Dual }
        , { needle = "drop table", risk = Dual }
        , { needle = "--force", risk = Dual }
        ]
    , defaultRisk = Review
    , reviewers = []
    , ttlReviewMs = 15 * 60 * 1000
    , ttlDualMs = 60 * 60 * 1000
    , minReviewMs = 1500
    , maxWaiting = 200
    , maxWaitingPerAgent = 20
    , maxSettled = 500
    , maxBatch = 25
    , maxArgsChars = 16384
    , maxNoteChars = 500
    , maxGrantMs = 30 * 60 * 1000
    , maxGrantUses = 20
    , fatigueWindowMs = 60 * 1000
    , fatigueMax = 12
    }


riskRank : Risk -> Int
riskRank risk =
    case risk of
        Auto ->
            0

        Review ->
            1

        Dual ->
            2

        Deny ->
            3


stricter : Risk -> Risk -> Risk
stricter a b =
    if riskRank a >= riskRank b then
        a

    else
        b


riskName : Risk -> String
riskName risk =
    case risk of
        Auto ->
            "auto"

        Review ->
            "review"

        Dual ->
            "dual"

        Deny ->
            "deny"


riskFromName : String -> Maybe Risk
riskFromName name =
    case name of
        "auto" ->
            Just Auto

        "review" ->
            Just Review

        "dual" ->
            Just Dual

        "deny" ->
            Just Deny

        _ ->
            Nothing


ruleMatches : String -> Rule -> Bool
ruleMatches tool rule =
    if String.endsWith "*" rule.tool then
        String.startsWith (String.dropRight 1 rule.tool) tool

    else
        rule.tool == tool


{-| Effective risk for a call. Pure and exposed so a UI can preview a decision.
-}
classify : Config -> String -> String -> Risk
classify cfg tool canonicalArgs =
    let
        base =
            cfg.rules
                |> List.filter (ruleMatches tool)
                |> List.sortBy (\r -> negate (String.length r.tool))
                |> List.head
                |> Maybe.map .risk
                |> Maybe.withDefault cfg.defaultRisk

        haystack =
            String.toLower canonicalArgs
    in
    cfg.triggers
        |> List.filter (\t -> t.needle /= "" && String.contains (String.toLower t.needle) haystack)
        |> List.foldl (\t acc -> stricter acc t.risk) base



-- CANONICAL ARGUMENTS AND FINGERPRINT


type J
    = JNull
    | JBool Bool
    | JNum Float
    | JStr String
    | JArr (List J)
    | JObj (List ( String, J ))


jDecoder : Decoder J
jDecoder =
    Decode.oneOf
        [ Decode.null JNull
        , Decode.map JBool Decode.bool
        , Decode.map JNum Decode.float
        , Decode.map JStr Decode.string
        , Decode.map JArr (Decode.list (Decode.lazy (\_ -> jDecoder)))
        , Decode.map JObj (Decode.keyValuePairs (Decode.lazy (\_ -> jDecoder)))
        ]


render : J -> String
render j =
    case j of
        JNull ->
            "null"

        JBool True ->
            "true"

        JBool False ->
            "false"

        JNum n ->
            String.fromFloat n

        JStr s ->
            Encode.encode 0 (Encode.string s)

        JArr items ->
            "[" ++ String.join "," (List.map render items) ++ "]"

        JObj pairs ->
            let
                body =
                    pairs
                        |> List.sortBy Tuple.first
                        |> List.map (\( k, v ) -> Encode.encode 0 (Encode.string k) ++ ":" ++ render v)
            in
            "{" ++ String.join "," body ++ "}"


{-| Render any JSON value with sorted object keys and no whitespace. Because the
text is rebuilt from the decoded value, `rm` and `rm` come out identical.
-}
canonicalJson : Decode.Value -> Result String String
canonicalJson value =
    Decode.decodeValue jDecoder value
        |> Result.map render
        |> Result.mapError Decode.errorToString


{-| Two independent 32 bit FNV-1a passes joined as 16 hex characters. This is an
index and display key only. Every equality check that matters compares the full
canonical text, so a crafted collision cannot transfer an approval.
-}
fingerprint : String -> String -> String
fingerprint tool canonicalArgs =
    let
        text =
            tool ++ "\u{0000}" ++ canonicalArgs
    in
    hex32 (fnv 2166136261 text) ++ hex32 (fnv 2539156620 text)


fnv : Int -> String -> Int
fnv seed text =
    String.foldl
        (\c h ->
            let
                x =
                    Bitwise.shiftRightZfBy 0 (Bitwise.xor h (Char.toCode c))

                high =
                    Bitwise.shiftRightZfBy 0 (Bitwise.shiftLeftBy 24 x)
            in
            Bitwise.shiftRightZfBy 0 (high + x * 403)
        )
        seed
        text


hex32 : Int -> String
hex32 n =
    String.padLeft 8 '0' (hexDigits n "")


hexDigits : Int -> String -> String
hexDigits n acc =
    if n <= 0 then
        acc

    else
        hexDigits (n // 16) (String.fromChar (hexChar (modBy 16 n)) ++ acc)


hexChar : Int -> Char
hexChar d =
    String.toList "0123456789abcdef"
        |> List.drop d
        |> List.head
        |> Maybe.withDefault '0'



-- SUBMISSION


type alias Submission =
    { id : CallId
    , agent : AgentId
    , requestedBy : Reviewer
    , tool : String
    , args : String
    }


{-| Wire shape: `{ "id", "agent", "requestedBy", "tool", "arguments": any JSON }`.
`arguments` is canonicalised on the way in.
-}
submissionDecoder : Decoder Submission
submissionDecoder =
    Decode.map5
        (\id agent requestedBy tool args ->
            { id = id, agent = agent, requestedBy = requestedBy, tool = tool, args = args }
        )
        (Decode.field "id" Decode.string)
        (Decode.field "agent" Decode.string)
        (Decode.field "requestedBy" Decode.string)
        (Decode.field "tool" Decode.string)
        (Decode.field "arguments" Decode.value
            |> Decode.andThen
                (\v ->
                    case canonicalJson v of
                        Ok text ->
                            Decode.succeed text

                        Err e ->
                            Decode.fail e
                )
        )



-- STATE


type alias Approval =
    { by : Reviewer
    , at : Int
    , note : String
    }


{-| One decision shared by every call id that asked for the same thing. `ids` is
never empty and `key` never changes, so a withdrawn first id cannot orphan the item.
-}
type alias Item =
    { key : CallId
    , ids : List CallId
    , agent : AgentId
    , requestedBy : Reviewer
    , tool : String
    , args : String
    , fingerprint : String
    , risk : Risk
    , submittedAt : Int
    , expiresAt : Int
    , views : Dict Reviewer Int
    , approvals : List Approval
    }


type alias Grant =
    { agent : AgentId
    , tool : String
    , args : String
    , grantedBy : Reviewer
    , expiresAt : Int
    , usesLeft : Int
    }


type alias Settled =
    { id : CallId
    , agent : AgentId
    , tool : String
    , args : String
    , fingerprint : String
    , risk : Risk
    , at : Int
    , verdict : Verdict
    }


type ApprovedVia
    = ByReviewers (List Reviewer)
    | ByGrant Reviewer
    | ByPolicy


type DenyReason
    = PolicyDenied
    | Expired
    | RejectedBy Reviewer String
    | Withdrawn
    | ArgsChanged
    | Overloaded
    | TooLarge


type Verdict
    = Approved ApprovedVia
    | Denied DenyReason


type alias Stats =
    { submitted : Int
    , approved : Int
    , denied : Int
    , expired : Int
    , refused : Int
    , coalesced : Int
    , grantUses : Int
    }


type Model
    = Model State


type alias State =
    { config : Config
    , waiting : Dict CallId Item
    , index : Dict CallId CallId
    , grants : Dict String Grant
    , settled : List Settled
    , fatigue : Dict Reviewer (List Int)
    , seq : Int
    , scheduled : Maybe Int
    , counters : Stats
    }


init : Config -> Model
init cfg =
    Model
        { config = cfg
        , waiting = Dict.empty
        , index = Dict.empty
        , grants = Dict.empty
        , settled = []
        , fatigue = Dict.empty
        , seq = 0
        , scheduled = Nothing
        , counters =
            { submitted = 0
            , approved = 0
            , denied = 0
            , expired = 0
            , refused = 0
            , coalesced = 0
            , grantUses = 0
            }
        }



-- MESSAGES AND EFFECTS


type alias ApproveOptions =
    { note : String
    , grantMs : Maybe Int
    }


{-| Every message carries `now` in epoch milliseconds from the host. The queue never
reads a clock, which keeps it deterministic and replayable.
-}
type Msg
    = Submit Int Submission
    | View Int CallId Reviewer
    | Approve Int CallId Reviewer ApproveOptions
    | ApproveBatch Int (List CallId) Reviewer
    | Reject Int CallId Reviewer String
    | Withdraw Int CallId AgentId
    | RevokeGrants Int Reviewer (Maybe AgentId)
    | Tick Int


type Refusal
    = UnknownCall
    | AlreadySettled Verdict
    | NotAuthorized
    | SelfApproval
    | AlreadyApprovedByYou
    | NotViewed
    | ReviewedTooFast Int
    | FatigueBrake Int
    | GrantNotAllowed
    | BatchNotAllowed Risk
    | BatchTooLarge
    | IdReuse


type alias AuditEntry =
    { seq : Int
    , at : Int
    , id : CallId
    , actor : String
    , event : String
    , detail : String
    }


type Effect
    = Dispatch { id : CallId, agent : AgentId, tool : String, args : String }
    | Respond CallId AgentId Verdict
    | NeedsReview Item
    | Refused CallId Reviewer Refusal
    | Audit AuditEntry
    | ScheduleTick (Maybe Int)




-- UPDATE


{-| Apply one message. Effects come back in the order the host should run them:
audit first inside each step so a crash after dispatch still leaves a record.
-}
update : Msg -> Model -> ( Model, List Effect )
update msg (Model s0) =
    let
        ( s1, effects ) =
            step msg s0

        ( s2, finished ) =
            finish s1 effects
    in
    ( Model s2, finished )


step : Msg -> State -> ( State, List Effect )
step msg s =
    case msg of
        Submit now sub ->
            submit now sub s

        View now id who ->
            view now id who s

        Approve now id who opts ->
            approve now id who opts s

        ApproveBatch now ids who ->
            approveBatch now ids who s

        Reject now id who reason ->
            reject now id who reason s

        Withdraw now id agent ->
            withdraw now id agent s

        RevokeGrants now who maybeAgent ->
            revokeGrants now who maybeAgent s

        Tick now ->
            tick now s


{-| Number the audit entries and keep the host timer pointing at the earliest
deadline. Both are done here so no step can forget them.
-}
finish : State -> List Effect -> ( State, List Effect )
finish s effects =
    let
        numbered =
            List.foldl
                (\eff ( n, acc ) ->
                    case eff of
                        Audit entry ->
                            ( n + 1, Audit { entry | seq = n } :: acc )

                        other ->
                            ( n, other :: acc )
                )
                ( s.seq + 1, [] )
                effects

        nextSeq =
            Tuple.first numbered - 1

        deadline =
            Dict.values s.waiting
                |> List.map .expiresAt
                |> List.minimum

        timer =
            if deadline /= s.scheduled then
                [ ScheduleTick deadline ]

            else
                []
    in
    ( { s | seq = nextSeq, scheduled = deadline }
    , List.reverse (Tuple.second numbered) ++ timer
    )


audit : Int -> CallId -> String -> String -> String -> Effect
audit at id actor event detail =
    Audit { seq = 0, at = at, id = id, actor = actor, event = event, detail = detail }


isReviewer : Config -> Reviewer -> Bool
isReviewer cfg who =
    List.isEmpty cfg.reviewers || List.member who cfg.reviewers


clip : Int -> String -> String
clip n text =
    String.left n text


refuse : Int -> CallId -> Reviewer -> Refusal -> State -> ( State, List Effect )
refuse now id who why s =
    let
        c =
            s.counters
    in
    ( { s | counters = { c | refused = c.refused + 1 } }
    , [ audit now id who "refused" (refusalName why), Refused id who why ]
    )


refusalName : Refusal -> String
refusalName why =
    case why of
        UnknownCall ->
            "unknown call"

        AlreadySettled v ->
            "already settled: " ++ verdictName v

        NotAuthorized ->
            "not authorized"

        SelfApproval ->
            "requester cannot approve own call"

        AlreadyApprovedByYou ->
            "reviewer already approved"

        NotViewed ->
            "call was not opened by reviewer"

        ReviewedTooFast ms ->
            "review too fast, " ++ String.fromInt ms ++ "ms short"

        FatigueBrake ms ->
            "fatigue brake, retry in " ++ String.fromInt ms ++ "ms"

        GrantNotAllowed ->
            "grant not allowed for this call"

        BatchNotAllowed risk ->
            "batch refuses risk " ++ riskName risk

        BatchTooLarge ->
            "batch too large"

        IdReuse ->
            "call id reused with different arguments"


verdictName : Verdict -> String
verdictName v =
    case v of
        Approved (ByReviewers who) ->
            "approved by " ++ String.join "+" who

        Approved (ByGrant who) ->
            "approved by grant of " ++ who

        Approved ByPolicy ->
            "approved by policy"

        Denied PolicyDenied ->
            "denied by policy"

        Denied Expired ->
            "expired"

        Denied (RejectedBy who why) ->
            "rejected by " ++ who ++ ": " ++ why

        Denied Withdrawn ->
            "withdrawn"

        Denied ArgsChanged ->
            "arguments changed"

        Denied Overloaded ->
            "queue overloaded"

        Denied TooLarge ->
            "arguments too large"



-- LOOKUP


type Found
    = Live Item
    | Done Settled
    | Missing


find : CallId -> State -> Found
find id s =
    case Dict.get id s.index |> Maybe.andThen (\key -> Dict.get key s.waiting) of
        Just item ->
            Live item

        Nothing ->
            case List.filter (\r -> r.id == id) s.settled of
                r :: _ ->
                    Done r

                [] ->
                    Missing


newItem : Int -> Config -> Submission -> Risk -> Item
newItem now cfg sub risk =
    { key = sub.id
    , ids = [ sub.id ]
    , agent = sub.agent
    , requestedBy = sub.requestedBy
    , tool = sub.tool
    , args = sub.args
    , fingerprint = fingerprint sub.tool sub.args
    , risk = risk
    , submittedAt = now
    , expiresAt =
        now
            + (if risk == Dual then
                cfg.ttlDualMs

               else
                cfg.ttlReviewMs
              )
    , views = Dict.empty
    , approvals = []
    }


grantKey : String -> AgentId -> String
grantKey fp agent =
    fp ++ "|" ++ agent



-- SETTLING


settle : Int -> Verdict -> Item -> State -> ( State, List Effect )
settle now verdict item s =
    settleIds now verdict item.ids item s


{-| Close some or all call ids of an item. Every id gets a settled record, an answer
to the agent and an audit line. Only approved verdicts dispatch, once per id.
-}
settleIds : Int -> Verdict -> List CallId -> Item -> State -> ( State, List Effect )
settleIds now verdict ids item s =
    let
        remaining =
            List.filter (\i -> not (List.member i ids)) item.ids

        waiting =
            if List.isEmpty remaining then
                Dict.remove item.key s.waiting

            else
                Dict.insert item.key { item | ids = remaining } s.waiting

        index =
            List.foldl Dict.remove s.index ids

        records =
            List.map
                (\id ->
                    { id = id
                    , agent = item.agent
                    , tool = item.tool
                    , args = item.args
                    , fingerprint = item.fingerprint
                    , risk = item.risk
                    , at = now
                    , verdict = verdict
                    }
                )
                ids

        perId id =
            [ audit now id (actorOf verdict item) (verdictName verdict) item.tool
            , Respond id item.agent verdict
            ]
                ++ (case verdict of
                        Approved _ ->
                            [ Dispatch { id = id, agent = item.agent, tool = item.tool, args = item.args } ]

                        Denied _ ->
                            []
                   )

        c =
            s.counters

        n =
            List.length ids

        counters =
            case verdict of
                Approved _ ->
                    { c | approved = c.approved + n }

                Denied Expired ->
                    { c | denied = c.denied + n, expired = c.expired + n }

                Denied _ ->
                    { c | denied = c.denied + n }
    in
    ( { s
        | waiting = waiting
        , index = index
        , settled = List.take s.config.maxSettled (records ++ s.settled)
        , counters = counters
      }
    , List.concatMap perId ids
    )


actorOf : Verdict -> Item -> String
actorOf verdict item =
    case verdict of
        Approved (ByReviewers who) ->
            String.join "+" who

        Approved (ByGrant who) ->
            who

        Approved ByPolicy ->
            "policy"

        Denied (RejectedBy who _) ->
            who

        Denied _ ->
            item.agent



-- SUBMIT


submit : Int -> Submission -> State -> ( State, List Effect )
submit now sub s0 =
    let
        c0 =
            s0.counters

        s =
            { s0 | counters = { c0 | submitted = c0.submitted + 1 } }

        cfg =
            s.config
    in
    case find sub.id s of
        Live item ->
            if sameCall item sub then
                ( s, [] )

            else
                -- Same id, different call: treat as tampering and drop the pending call.
                let
                    ( s1, effs ) =
                        settle now (Denied ArgsChanged) item s

                    ( s2, more ) =
                        refuse now sub.id sub.requestedBy IdReuse s1
                in
                ( s2, effs ++ more )

        Done r ->
            if r.agent == sub.agent && r.tool == sub.tool && r.args == sub.args then
                -- Idempotent retry: repeat the answer, never dispatch twice.
                ( s, [ Respond r.id r.agent r.verdict ] )

            else
                let
                    ( s1, more ) =
                        refuse now sub.id sub.requestedBy IdReuse s
                in
                ( s1, Respond sub.id sub.agent (Denied ArgsChanged) :: more )

        Missing ->
            if String.length sub.args > cfg.maxArgsChars then
                deniedAtDoor now sub TooLarge s

            else
                admit now sub (classify cfg sub.tool sub.args) s


sameCall : Item -> Submission -> Bool
sameCall item sub =
    item.agent == sub.agent && item.tool == sub.tool && item.args == sub.args


deniedAtDoor : Int -> Submission -> DenyReason -> State -> ( State, List Effect )
deniedAtDoor now sub reason s =
    settle now (Denied reason) (newItem now s.config sub Deny) s


admit : Int -> Submission -> Risk -> State -> ( State, List Effect )
admit now sub risk s =
    let
        cfg =
            s.config

        item =
            newItem now cfg sub risk

        waitingList =
            Dict.values s.waiting
    in
    case risk of
        Deny ->
            deniedAtDoor now sub PolicyDenied s

        Auto ->
            settle now (Approved ByPolicy) item s

        _ ->
            case useGrant now item s of
                Just ( grantor, s1 ) ->
                    let
                        c =
                            s1.counters

                        ( s2, effs ) =
                            settle now (Approved (ByGrant grantor)) item { s1 | counters = { c | grantUses = c.grantUses + 1 } }
                    in
                    ( s2, audit now sub.id grantor "grant used" item.tool :: effs )

                Nothing ->
                    case List.filter (\w -> sameCall w sub) waitingList of
                        twin :: _ ->
                            coalesce now sub twin s

                        [] ->
                            if List.length waitingList >= cfg.maxWaiting then
                                deniedAtDoor now sub Overloaded s

                            else if List.length (List.filter (\w -> w.agent == sub.agent) waitingList) >= cfg.maxWaitingPerAgent then
                                deniedAtDoor now sub Overloaded s

                            else
                                ( { s
                                    | waiting = Dict.insert item.key item s.waiting
                                    , index = Dict.insert item.key item.key s.index
                                  }
                                , [ audit now sub.id sub.requestedBy "queued" (riskName risk ++ " " ++ item.tool)
                                  , NeedsReview item
                                  ]
                                )


coalesce : Int -> Submission -> Item -> State -> ( State, List Effect )
coalesce now sub twin s =
    let
        c =
            s.counters
    in
    ( { s
        | waiting = Dict.insert twin.key { twin | ids = twin.ids ++ [ sub.id ] } s.waiting
        , index = Dict.insert sub.id twin.key s.index
        , counters = { c | coalesced = c.coalesced + 1 }
      }
    , [ audit now sub.id sub.requestedBy "coalesced" ("into " ++ twin.key) ]
    )


{-| A grant only covers `Review` risk and only the exact tool and arguments it was
issued for. Dual calls always meet two humans.
-}
useGrant : Int -> Item -> State -> Maybe ( Reviewer, State )
useGrant now item s =
    if item.risk /= Review then
        Nothing

    else
        let
            key =
                grantKey item.fingerprint item.agent
        in
        case Dict.get key s.grants of
            Just g ->
                if g.agent == item.agent && g.tool == item.tool && g.args == item.args && g.expiresAt > now && g.usesLeft > 0 then
                    let
                        grants =
                            if g.usesLeft <= 1 then
                                Dict.remove key s.grants

                            else
                                Dict.insert key { g | usesLeft = g.usesLeft - 1 } s.grants
                    in
                    Just ( g.grantedBy, { s | grants = grants } )

                else
                    Nothing

            Nothing ->
                Nothing



-- VIEW


view : Int -> CallId -> Reviewer -> State -> ( State, List Effect )
view now id who s =
    if not (isReviewer s.config who) then
        refuse now id who NotAuthorized s

    else
        case find id s of
            Live item ->
                if Dict.member who item.views then
                    ( s, [] )

                else
                    ( { s | waiting = Dict.insert item.key { item | views = Dict.insert who now item.views } s.waiting }
                    , [ audit now id who "viewed" item.tool ]
                    )

            Done r ->
                refuse now id who (AlreadySettled r.verdict) s

            Missing ->
                refuse now id who UnknownCall s



-- APPROVE


approve : Int -> CallId -> Reviewer -> ApproveOptions -> State -> ( State, List Effect )
approve now id who opts s =
    case find id s of
        Missing ->
            refuse now id who UnknownCall s

        Done r ->
            refuse now id who (AlreadySettled r.verdict) s

        Live item ->
            if item.expiresAt <= now then
                let
                    ( s1, effs ) =
                        settle now (Denied Expired) item s

                    ( s2, more ) =
                        refuse now id who (AlreadySettled (Denied Expired)) s1
                in
                ( s2, effs ++ more )

            else
                case checkApprover now who item opts s of
                    Err why ->
                        refuse now id who why s

                    Ok () ->
                        record now id who opts item s


checkApprover : Int -> Reviewer -> Item -> ApproveOptions -> State -> Result Refusal ()
checkApprover now who item opts s =
    let
        cfg =
            s.config

        recent =
            Dict.get who s.fatigue
                |> Maybe.withDefault []
                |> List.filter (\t -> t > now - cfg.fatigueWindowMs)
    in
    if not (isReviewer cfg who) then
        Err NotAuthorized

    else if who == item.requestedBy || who == item.agent then
        Err SelfApproval

    else if List.any (\a -> a.by == who) item.approvals then
        Err AlreadyApprovedByYou

    else if opts.grantMs /= Nothing && (item.risk /= Review || Maybe.withDefault 0 opts.grantMs <= 0) then
        Err GrantNotAllowed

    else
        case Dict.get who item.views of
            Nothing ->
                Err NotViewed

            Just seen ->
                if now - seen < cfg.minReviewMs then
                    Err (ReviewedTooFast (cfg.minReviewMs - (now - seen)))

                else if List.length recent >= cfg.fatigueMax then
                    Err (FatigueBrake (Maybe.withDefault 0 (List.minimum recent) + cfg.fatigueWindowMs - now))

                else
                    Ok ()


record : Int -> CallId -> Reviewer -> ApproveOptions -> Item -> State -> ( State, List Effect )
record now id who opts item s0 =
    let
        cfg =
            s0.config

        fatigue =
            Dict.insert who
                (now :: (Dict.get who s0.fatigue |> Maybe.withDefault [] |> List.filter (\t -> t > now - cfg.fatigueWindowMs)))
                s0.fatigue

        s =
            { s0 | fatigue = fatigue }

        approvals =
            item.approvals ++ [ { by = who, at = now, note = clip cfg.maxNoteChars opts.note } ]

        needed =
            if item.risk == Dual then
                2

            else
                1
    in
    if List.length approvals >= needed then
        let
            ( s1, effs ) =
                settle now (Approved (ByReviewers (List.map .by approvals))) item s

            s2 =
                case opts.grantMs of
                    Just ms ->
                        { s1
                            | grants =
                                Dict.insert (grantKey item.fingerprint item.agent)
                                    { agent = item.agent
                                    , tool = item.tool
                                    , args = item.args
                                    , grantedBy = who
                                    , expiresAt = now + min ms cfg.maxGrantMs
                                    , usesLeft = cfg.maxGrantUses
                                    }
                                    s1.grants
                        }

                    Nothing ->
                        s1
        in
        if opts.grantMs /= Nothing then
            ( s2, audit now id who "grant issued" (grantNote opts) :: effs )

        else
            ( s2, effs )

    else
        let
            waiting =
                { item | approvals = approvals }
        in
        ( { s | waiting = Dict.insert item.key waiting s.waiting }
        , [ audit now id who "first approval" (String.fromInt (List.length approvals) ++ " of " ++ String.fromInt needed)
          , NeedsReview waiting
          ]
        )


grantNote : ApproveOptions -> String
grantNote opts =
    case opts.grantMs of
        Just ms ->
            String.fromInt ms ++ "ms requested"

        Nothing ->
            ""


approveBatch : Int -> List CallId -> Reviewer -> State -> ( State, List Effect )
approveBatch now ids who s0 =
    if List.length ids > s0.config.maxBatch then
        refuse now (String.join "," (List.take 3 ids)) who BatchTooLarge s0

    else
        List.foldl
            (\id ( s, acc ) ->
                let
                    ( s1, effs ) =
                        case find id s of
                            Live item ->
                                if item.risk /= Review then
                                    refuse now id who (BatchNotAllowed item.risk) s

                                else
                                    approve now id who { note = "batch", grantMs = Nothing } s

                            _ ->
                                approve now id who { note = "batch", grantMs = Nothing } s
                in
                ( s1, acc ++ effs )
            )
            ( s0, [] )
            ids



-- REJECT, WITHDRAW, REVOKE


reject : Int -> CallId -> Reviewer -> String -> State -> ( State, List Effect )
reject now id who reason s =
    if not (isReviewer s.config who) then
        refuse now id who NotAuthorized s

    else
        case find id s of
            Live item ->
                settle now (Denied (RejectedBy who (clip s.config.maxNoteChars reason))) item s

            Done r ->
                refuse now id who (AlreadySettled r.verdict) s

            Missing ->
                refuse now id who UnknownCall s


withdraw : Int -> CallId -> AgentId -> State -> ( State, List Effect )
withdraw now id agent s =
    case find id s of
        Live item ->
            if item.agent /= agent then
                refuse now id agent NotAuthorized s

            else
                settleIds now (Denied Withdrawn) [ id ] item s

        Done r ->
            refuse now id agent (AlreadySettled r.verdict) s

        Missing ->
            refuse now id agent UnknownCall s


revokeGrants : Int -> Reviewer -> Maybe AgentId -> State -> ( State, List Effect )
revokeGrants now who maybeAgent s =
    if not (isReviewer s.config who) then
        refuse now "*" who NotAuthorized s

    else
        let
            keep _ g =
                case maybeAgent of
                    Just agent ->
                        g.agent /= agent

                    Nothing ->
                        False

            kept =
                Dict.filter keep s.grants

            removed =
                Dict.size s.grants - Dict.size kept
        in
        ( { s | grants = kept }
        , [ audit now "*" who "grants revoked" (String.fromInt removed) ]
        )



-- TICK


tick : Int -> State -> ( State, List Effect )
tick now s0 =
    let
        due =
            Dict.values s0.waiting
                |> List.filter (\i -> i.expiresAt <= now)
                |> List.sortBy .expiresAt

        ( s1, effs ) =
            List.foldl
                (\item ( s, acc ) ->
                    let
                        ( s2, e ) =
                            settle now (Denied Expired) item s
                    in
                    ( s2, acc ++ e )
                )
                ( s0, [] )
                due

        window =
            s1.config.fatigueWindowMs
    in
    ( { s1
        | grants = Dict.filter (\_ g -> g.expiresAt > now && g.usesLeft > 0) s1.grants
        , fatigue =
            s1.fatigue
                |> Dict.map (\_ ts -> List.filter (\t -> t > now - window) ts)
                |> Dict.filter (\_ ts -> not (List.isEmpty ts))
      }
    , effs
    )



-- QUERIES


type Status
    = StillWaiting Item
    | Decided Verdict
    | Unknown


lookup : CallId -> Model -> Status
lookup id (Model s) =
    case find id s of
        Live item ->
            StillWaiting item

        Done r ->
            Decided r.verdict

        Missing ->
            Unknown


{-| Review order: soonest expiry first, so the call about to time out is on top.
-}
waitingItems : Model -> List Item
waitingItems (Model s) =
    Dict.values s.waiting
        |> List.sortBy (\i -> ( i.expiresAt, negate (riskRank i.risk), i.key ))


grantCount : Model -> Int
grantCount (Model s) =
    Dict.size s.grants


stats : Model -> Stats
stats (Model s) =
    s.counters



-- SNAPSHOT


{-| Waiting items, grants and settled records, enough to survive a reload. Restoring
keeps the original deadlines, so the next `Tick` expires whatever aged out while the
page was away.
-}
encodeSnapshot : Model -> Encode.Value
encodeSnapshot (Model s) =
    Encode.object
        [ ( "v", Encode.int 1 )
        , ( "seq", Encode.int s.seq )
        , ( "waiting", Encode.list encodeItem (Dict.values s.waiting) )
        , ( "grants", Encode.list encodeGrant (Dict.values s.grants) )
        , ( "settled", Encode.list encodeSettled s.settled )
        ]


snapshotDecoder : Config -> Decoder Model
snapshotDecoder cfg =
    Decode.map4
        (\seq items grants settled ->
            let
                base =
                    case init cfg of
                        Model st ->
                            st
            in
            Model
                { base
                    | seq = seq
                    , waiting = Dict.fromList (List.map (\i -> ( i.key, i )) items)
                    , index = Dict.fromList (List.concatMap (\i -> List.map (\id -> ( id, i.key )) i.ids) items)
                    , grants = Dict.fromList (List.map (\g -> ( grantKey (fingerprint g.tool g.args) g.agent, g )) grants)
                    , settled = List.take cfg.maxSettled settled
                }
        )
        (Decode.field "seq" Decode.int)
        (Decode.field "waiting" (Decode.list itemDecoder))
        (Decode.field "grants" (Decode.list grantDecoder))
        (Decode.field "settled" (Decode.list settledDecoder))


encodeItem : Item -> Encode.Value
encodeItem i =
    Encode.object
        [ ( "key", Encode.string i.key )
        , ( "ids", Encode.list Encode.string i.ids )
        , ( "agent", Encode.string i.agent )
        , ( "requestedBy", Encode.string i.requestedBy )
        , ( "tool", Encode.string i.tool )
        , ( "args", Encode.string i.args )
        , ( "risk", Encode.string (riskName i.risk) )
        , ( "submittedAt", Encode.int i.submittedAt )
        , ( "expiresAt", Encode.int i.expiresAt )
        , ( "views", Encode.dict identity Encode.int i.views )
        , ( "approvals"
          , Encode.list
                (\a ->
                    Encode.object
                        [ ( "by", Encode.string a.by )
                        , ( "at", Encode.int a.at )
                        , ( "note", Encode.string a.note )
                        ]
                )
                i.approvals
          )
        ]


riskDecoder : Decoder Risk
riskDecoder =
    Decode.string
        |> Decode.andThen
            (\name ->
                case riskFromName name of
                    Just r ->
                        Decode.succeed r

                    Nothing ->
                        Decode.fail ("unknown risk " ++ name)
            )


itemDecoder : Decoder Item
itemDecoder =
    Decode.map8
        (\key ids agent requestedBy tool args risk ( submittedAt, expiresAt ) ->
            { key = key
            , ids = ids
            , agent = agent
            , requestedBy = requestedBy
            , tool = tool
            , args = args
            , fingerprint = fingerprint tool args
            , risk = risk
            , submittedAt = submittedAt
            , expiresAt = expiresAt
            , views = Dict.empty
            , approvals = []
            }
        )
        (Decode.field "key" Decode.string)
        (Decode.field "ids" (Decode.list Decode.string) |> Decode.andThen nonEmpty)
        (Decode.field "agent" Decode.string)
        (Decode.field "requestedBy" Decode.string)
        (Decode.field "tool" Decode.string)
        (Decode.field "args" Decode.string)
        (Decode.field "risk" riskDecoder)
        (Decode.map2 Tuple.pair (Decode.field "submittedAt" Decode.int) (Decode.field "expiresAt" Decode.int))
        |> Decode.andThen
            (\item ->
                Decode.map2
                    (\views approvals -> { item | views = views, approvals = approvals })
                    (Decode.field "views" (Decode.dict Decode.int))
                    (Decode.field "approvals"
                        (Decode.list
                            (Decode.map3 Approval
                                (Decode.field "by" Decode.string)
                                (Decode.field "at" Decode.int)
                                (Decode.field "note" Decode.string)
                            )
                        )
                    )
            )


nonEmpty : List a -> Decoder (List a)
nonEmpty xs =
    if List.isEmpty xs then
        Decode.fail "item with no call ids"

    else
        Decode.succeed xs


encodeGrant : Grant -> Encode.Value
encodeGrant g =
    Encode.object
        [ ( "agent", Encode.string g.agent )
        , ( "tool", Encode.string g.tool )
        , ( "args", Encode.string g.args )
        , ( "grantedBy", Encode.string g.grantedBy )
        , ( "expiresAt", Encode.int g.expiresAt )
        , ( "usesLeft", Encode.int g.usesLeft )
        ]


grantDecoder : Decoder Grant
grantDecoder =
    Decode.map6 Grant
        (Decode.field "agent" Decode.string)
        (Decode.field "tool" Decode.string)
        (Decode.field "args" Decode.string)
        (Decode.field "grantedBy" Decode.string)
        (Decode.field "expiresAt" Decode.int)
        (Decode.field "usesLeft" Decode.int)


encodeVerdict : Verdict -> Encode.Value
encodeVerdict v =
    case v of
        Approved (ByReviewers who) ->
            Encode.object [ ( "kind", Encode.string "reviewers" ), ( "who", Encode.list Encode.string who ) ]

        Approved (ByGrant who) ->
            Encode.object [ ( "kind", Encode.string "grant" ), ( "who", Encode.list Encode.string [ who ] ) ]

        Approved ByPolicy ->
            Encode.object [ ( "kind", Encode.string "policy" ) ]

        Denied (RejectedBy who why) ->
            Encode.object [ ( "kind", Encode.string "rejected" ), ( "who", Encode.list Encode.string [ who ] ), ( "why", Encode.string why ) ]

        Denied reason ->
            Encode.object [ ( "kind", Encode.string (denyName reason) ) ]


denyName : DenyReason -> String
denyName reason =
    case reason of
        PolicyDenied ->
            "policy_denied"

        Expired ->
            "expired"

        RejectedBy _ _ ->
            "rejected"

        Withdrawn ->
            "withdrawn"

        ArgsChanged ->
            "args_changed"

        Overloaded ->
            "overloaded"

        TooLarge ->
            "too_large"


verdictDecoder : Decoder Verdict
verdictDecoder =
    Decode.field "kind" Decode.string
        |> Decode.andThen
            (\kind ->
                let
                    who =
                        Decode.oneOf [ Decode.field "who" (Decode.list Decode.string), Decode.succeed [] ]
                in
                case kind of
                    "reviewers" ->
                        Decode.map (Approved << ByReviewers) who

                    "grant" ->
                        Decode.map (\w -> Approved (ByGrant (String.join "" w))) who

                    "policy" ->
                        Decode.succeed (Approved ByPolicy)

                    "rejected" ->
                        Decode.map2 (\w why -> Denied (RejectedBy (String.join "" w) why))
                            who
                            (Decode.field "why" Decode.string)

                    "policy_denied" ->
                        Decode.succeed (Denied PolicyDenied)

                    "expired" ->
                        Decode.succeed (Denied Expired)

                    "withdrawn" ->
                        Decode.succeed (Denied Withdrawn)

                    "args_changed" ->
                        Decode.succeed (Denied ArgsChanged)

                    "overloaded" ->
                        Decode.succeed (Denied Overloaded)

                    "too_large" ->
                        Decode.succeed (Denied TooLarge)

                    other ->
                        Decode.fail ("unknown verdict " ++ other)
            )


encodeSettled : Settled -> Encode.Value
encodeSettled r =
    Encode.object
        [ ( "id", Encode.string r.id )
        , ( "agent", Encode.string r.agent )
        , ( "tool", Encode.string r.tool )
        , ( "args", Encode.string r.args )
        , ( "risk", Encode.string (riskName r.risk) )
        , ( "at", Encode.int r.at )
        , ( "verdict", encodeVerdict r.verdict )
        ]


settledDecoder : Decoder Settled
settledDecoder =
    Decode.map7
        (\id agent tool args risk at verdict ->
            { id = id
            , agent = agent
            , tool = tool
            , args = args
            , fingerprint = fingerprint tool args
            , risk = risk
            , at = at
            , verdict = verdict
            }
        )
        (Decode.field "id" Decode.string)
        (Decode.field "agent" Decode.string)
        (Decode.field "tool" Decode.string)
        (Decode.field "args" Decode.string)
        (Decode.field "risk" riskDecoder)
        (Decode.field "at" Decode.int)
        (Decode.field "verdict" verdictDecoder)
