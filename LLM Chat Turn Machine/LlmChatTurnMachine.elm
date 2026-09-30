module LlmChatTurnMachine exposing
    ( Machine, Config, defaultConfig, init
    , Msg(..), Effect(..), update
    , Phase(..), phase, isBusy, wantsTicks, transcript, activeTurn
    , Turn, TurnId, Outcome(..), FinishReason(..), Failure(..), Usage, Stats
    , Block(..), ToolCall, ToolStatus(..), blocks, plainText, readyToolCalls
    , Envelope, StreamEvent(..), envelopeDecoder, decodeEnvelope
    )

{-| A pure state machine for one chat window talking to a streaming LLM endpoint.

The machine owns everything that goes wrong between "user pressed send" and "the
answer is complete": stale streams from a cancelled turn, duplicate events after a
resume, events that arrive out of order, silent stalls, connections that close
without a terminal event, tool call arguments that never finish, and retry storms.

It performs no IO. The host application (ports, Http, EventSource wrapper) turns
`Effect` values into real work and feeds the outcome back as `Msg` values.

Wire contract the machine expects from the server, per turn:

  - every event carries the turn id and a sequence number starting at 1
  - a resumed stream repeats or continues from `resumeAfter + 1`
  - a stream is complete only after a `Done` event, never after a plain close

-}

import Dict exposing (Dict)
import Json.Decode as Decode exposing (Decoder)



-- CONFIG


type alias Config =
    { connectTimeoutMs : Int
    , stallTimeoutMs : Int
    , gapTimeoutMs : Int
    , maxTurnMs : Int
    , maxRetries : Int
    , maxTotalReconnects : Int
    , baseBackoffMs : Int
    , maxBackoffMs : Int
    , maxRetryAfterMs : Int
    , maxReorderWindow : Int
    , maxPending : Int
    , maxMalformed : Int
    , maxTextChars : Int
    , maxToolArgsChars : Int
    , maxPromptChars : Int
    , maxFinishedTurns : Int
    , replaceWhenBusy : Bool
    , seed : Int
    }


defaultConfig : Config
defaultConfig =
    { connectTimeoutMs = 15000
    , stallTimeoutMs = 30000
    , gapTimeoutMs = 5000
    , maxTurnMs = 600000
    , maxRetries = 5
    , maxTotalReconnects = 20
    , baseBackoffMs = 500
    , maxBackoffMs = 15000
    , maxRetryAfterMs = 60000
    , maxReorderWindow = 64
    , maxPending = 128
    , maxMalformed = 8
    , maxTextChars = 2000000
    , maxToolArgsChars = 1000000
    , maxPromptChars = 200000
    , maxFinishedTurns = 200
    , replaceWhenBusy = False
    , seed = 1
    }



-- WIRE TYPES


type alias TurnId =
    Int


type alias Envelope =
    { turn : TurnId
    , seq : Int
    , event : StreamEvent
    }


type StreamEvent
    = TextDelta String
    | ReasoningDelta String
    | ToolStart { callId : String, name : String }
    | ToolArgs { callId : String, chunk : String }
    | ToolEnd String
    | UsageUpdate Usage
    | Done FinishReason
    | ServerError { code : String, message : String, retryable : Bool }
    | Unknown String


type FinishReason
    = Stop
    | Length
    | ToolCalls
    | ContentFilter
    | OtherReason String


type alias Usage =
    { inputTokens : Int
    , outputTokens : Int
    }



-- TURN MODEL


type Block
    = Text String
    | Reasoning String
    | Tool ToolCall


type alias ToolCall =
    { callId : String
    , name : String
    , args : String
    , status : ToolStatus
    }


type ToolStatus
    = Building
    | Ready
    | InvalidArgs String
    | Truncated


type Outcome
    = Running
    | Finished FinishReason
    | Failed Failure
    | Cancelled
    | Superseded


type Failure
    = ProtocolViolation String
    | HttpStatus Int
    | ServerReported String String
    | RetriesExhausted Failure
    | ConnectTimeout
    | Stalled
    | GapTimeout
    | GapTooWide
    | PrematureClose
    | NetworkError
    | TooLarge String
    | TooManyMalformed
    | DeadlineExceeded


type alias Stats =
    { duplicates : Int
    , stale : Int
    , reordered : Int
    , gaps : Int
    , malformed : Int
    , reconnects : Int
    }


type alias Turn =
    { id : TurnId
    , prompt : String
    , revBlocks : List Block
    , usage : Maybe Usage
    , outcome : Outcome
    , stats : Stats
    }


blocks : Turn -> List Block
blocks turn =
    List.reverse turn.revBlocks


{-| Concatenated visible answer text, reasoning excluded.
-}
plainText : Turn -> String
plainText turn =
    blocks turn
        |> List.filterMap
            (\b ->
                case b of
                    Text s ->
                        Just s

                    _ ->
                        Nothing
            )
        |> String.concat


{-| Tool calls whose arguments are complete and valid JSON objects.
-}
readyToolCalls : Turn -> List ToolCall
readyToolCalls turn =
    blocks turn
        |> List.filterMap
            (\b ->
                case b of
                    Tool call ->
                        if call.status == Ready then
                            Just call

                        else
                            Nothing

                    _ ->
                        Nothing
            )



-- MACHINE


type Conn
    = Connecting Int
    | Open
    | Waiting Int


type alias Active =
    { turn : Turn
    , conn : Conn
    , attempt : Int
    , lastSeq : Int
    , pending : Dict Int StreamEvent
    , idleMs : Int
    , gapMs : Int
    , elapsedMs : Int
    , failures : Int
    , textChars : Int
    }


type Machine
    = Machine
        { config : Config
        , nextTurn : TurnId
        , nextAttempt : Int
        , rng : Int
        , finished : List Turn
        , active : Maybe Active
        }


init : Config -> Machine
init cfg =
    Machine
        { config = cfg
        , nextTurn = 1
        , nextAttempt = 1
        , rng = Basics.max 1 (modBy 2147483646 (abs cfg.seed) + 1)
        , finished = []
        , active = Nothing
        }


type Msg
    = Submit String
    | Regenerate
    | Cancel
    | Connected Int
    | Received Int Envelope
    | Malformed Int String
    | ConnectionFailed Int { status : Maybe Int, retryAfterMs : Maybe Int }
    | ConnectionEnded Int
    | Tick Int


type Effect
    = OpenStream { turn : TurnId, attempt : Int, resumeAfter : Int, prompt : String }
    | CloseStream { attempt : Int }
    | TurnEnded Turn
    | SubmitRejected String


type Phase
    = Idle
    | ConnectingPhase
    | Streaming
    | Backoff { retryInMs : Int }


phase : Machine -> Phase
phase (Machine m) =
    case m.active of
        Nothing ->
            Idle

        Just a ->
            case a.conn of
                Connecting _ ->
                    ConnectingPhase

                Open ->
                    Streaming

                Waiting ms ->
                    Backoff { retryInMs = ms }


isBusy : Machine -> Bool
isBusy (Machine m) =
    m.active /= Nothing


{-| True while the host should deliver `Tick` messages (a 250 to 1000 ms timer is plenty).
-}
wantsTicks : Machine -> Bool
wantsTicks =
    isBusy


activeTurn : Machine -> Maybe Turn
activeTurn (Machine m) =
    Maybe.map .turn m.active


transcript : Machine -> List Turn
transcript (Machine m) =
    case m.active of
        Nothing ->
            m.finished

        Just a ->
            m.finished ++ [ a.turn ]



-- UPDATE


update : Msg -> Machine -> ( Machine, List Effect )
update msg machine =
    case msg of
        Submit text ->
            submit text machine

        Regenerate ->
            regenerate machine

        Cancel ->
            cancel machine

        Tick delta ->
            tick (Basics.max 0 delta) machine

        Connected attempt ->
            withAttempt attempt machine <|
                \m a ->
                    case a.conn of
                        Open ->
                            ( setActive { a | idleMs = 0 } m, [] )

                        _ ->
                            ( setActive { a | conn = Open, idleMs = 0 } m, [] )

        Received attempt envelope ->
            withAttempt attempt machine (receive envelope)

        Malformed attempt _ ->
            withAttempt attempt machine <|
                \m a ->
                    let
                        a1 =
                            mapStats (\s -> { s | malformed = s.malformed + 1 }) a
                    in
                    if a1.turn.stats.malformed > m.config.maxMalformed then
                        terminate (Failed TooManyMalformed) m a1

                    else
                        ( setActive { a1 | idleMs = 0 } m, [] )

        ConnectionFailed attempt info ->
            withAttempt attempt machine <|
                \m a ->
                    case info.status of
                        Just code ->
                            if retryableStatus code then
                                retry (HttpStatus code) info.retryAfterMs m a

                            else
                                terminate (Failed (HttpStatus code)) m a

                        Nothing ->
                            retry NetworkError info.retryAfterMs m a

        ConnectionEnded attempt ->
            -- A close with no Done event is a truncation, never a success.
            withAttempt attempt machine (retry PrematureClose Nothing)


unwrap : Machine -> Inner
unwrap (Machine m) =
    m


type alias Inner =
    { config : Config
    , nextTurn : TurnId
    , nextAttempt : Int
    , rng : Int
    , finished : List Turn
    , active : Maybe Active
    }


withAttempt : Int -> Machine -> (Inner -> Active -> ( Machine, List Effect )) -> ( Machine, List Effect )
withAttempt attempt machine f =
    let
        m =
            unwrap machine
    in
    case m.active of
        Nothing ->
            ( machine, [] )

        Just a ->
            if a.attempt == attempt && not (isWaiting a.conn) then
                f m a

            else
                ( Machine { m | active = Just (mapStats (\s -> { s | stale = s.stale + 1 }) a) }, [] )


isWaiting : Conn -> Bool
isWaiting conn =
    case conn of
        Waiting _ ->
            True

        _ ->
            False


setActive : Active -> Inner -> Machine
setActive a m =
    Machine { m | active = Just a }


mapStats : (Stats -> Stats) -> Active -> Active
mapStats f a =
    let
        turn =
            a.turn
    in
    { a | turn = { turn | stats = f turn.stats } }


emptyStats : Stats
emptyStats =
    { duplicates = 0, stale = 0, reordered = 0, gaps = 0, malformed = 0, reconnects = 0 }



-- SUBMIT, REGENERATE, CANCEL


submit : String -> Machine -> ( Machine, List Effect )
submit raw machine =
    let
        m =
            unwrap machine

        text =
            String.trim raw
    in
    if text == "" then
        ( machine, [ SubmitRejected "empty prompt" ] )

    else if String.length text > m.config.maxPromptChars then
        ( machine, [ SubmitRejected "prompt too long" ] )

    else
        case m.active of
            Just a ->
                if m.config.replaceWhenBusy then
                    let
                        ( m1, fx ) =
                            terminate Cancelled m a

                        ( m2, fx2 ) =
                            startTurn text (unwrap m1)
                    in
                    ( m2, fx ++ fx2 )

                else
                    ( machine, [ SubmitRejected "a turn is already running" ] )

            Nothing ->
                startTurn text m


startTurn : String -> Inner -> ( Machine, List Effect )
startTurn prompt m =
    let
        turn =
            { id = m.nextTurn
            , prompt = prompt
            , revBlocks = []
            , usage = Nothing
            , outcome = Running
            , stats = emptyStats
            }

        a =
            { turn = turn
            , conn = Connecting 0
            , attempt = 0
            , lastSeq = 0
            , pending = Dict.empty
            , idleMs = 0
            , gapMs = 0
            , elapsedMs = 0
            , failures = 0
            , textChars = 0
            }
    in
    openAttempt { m | nextTurn = m.nextTurn + 1 } a


regenerate : Machine -> ( Machine, List Effect )
regenerate machine =
    let
        m =
            unwrap machine
    in
    case ( m.active, List.reverse m.finished ) of
        ( Nothing, last :: olderRev ) ->
            let
                superseded =
                    { last | outcome = Superseded }
            in
            startTurn last.prompt { m | finished = List.reverse (superseded :: olderRev) }

        ( Just _, _ ) ->
            ( machine, [ SubmitRejected "a turn is already running" ] )

        _ ->
            ( machine, [ SubmitRejected "nothing to regenerate" ] )


cancel : Machine -> ( Machine, List Effect )
cancel machine =
    case (unwrap machine).active of
        Nothing ->
            ( machine, [] )

        Just a ->
            terminate Cancelled (unwrap machine) a


openAttempt : Inner -> Active -> ( Machine, List Effect )
openAttempt m a =
    let
        attempt =
            m.nextAttempt

        a1 =
            { a
                | attempt = attempt
                , conn = Connecting 0
                , idleMs = 0
                , gapMs = 0
                , pending = Dict.empty
            }
    in
    ( Machine { m | nextAttempt = attempt + 1, active = Just a1 }
    , [ OpenStream
            { turn = a.turn.id
            , attempt = attempt
            , resumeAfter = a.lastSeq
            , prompt = a.turn.prompt
            }
      ]
    )



-- TERMINATION AND RETRY


terminate : Outcome -> Inner -> Active -> ( Machine, List Effect )
terminate outcome m a =
    let
        turn0 =
            a.turn

        turn =
            { turn0 | outcome = outcome, revBlocks = List.map sealBlock turn0.revBlocks }

        finished =
            (m.finished ++ [ turn ])
                |> (\all -> List.drop (List.length all - m.config.maxFinishedTurns) all)

        close =
            if isWaiting a.conn then
                []

            else
                [ CloseStream { attempt = a.attempt } ]
    in
    ( Machine { m | active = Nothing, finished = finished }
    , close ++ [ TurnEnded turn ]
    )


sealBlock : Block -> Block
sealBlock block =
    case block of
        Tool call ->
            if call.status == Building then
                Tool { call | status = Truncated }

            else
                block

        _ ->
            block


retryableStatus : Int -> Bool
retryableStatus code =
    code == 408 || code == 425 || code == 429 || code >= 500


retry : Failure -> Maybe Int -> Inner -> Active -> ( Machine, List Effect )
retry cause retryAfter m a =
    let
        cfg =
            m.config

        failures =
            a.failures + 1
    in
    if failures > cfg.maxRetries || a.turn.stats.reconnects >= cfg.maxTotalReconnects then
        terminate (Failed (RetriesExhausted cause)) m a

    else
        let
            ( r, rng1 ) =
                nextRandom m.rng

            expo =
                Basics.min cfg.maxBackoffMs (cfg.baseBackoffMs * (2 ^ Basics.min 20 (failures - 1)))

            half =
                expo // 2

            jittered =
                half + modBy (half + 1) r

            delay =
                case retryAfter of
                    Just ra ->
                        Basics.max jittered (Basics.min cfg.maxRetryAfterMs (Basics.max 0 ra))

                    Nothing ->
                        jittered

            a1 =
                mapStats (\s -> { s | reconnects = s.reconnects + 1 })
                    { a | conn = Waiting (Basics.max 1 delay), failures = failures, pending = Dict.empty, gapMs = 0 }

            close =
                if isWaiting a.conn then
                    []

                else
                    [ CloseStream { attempt = a.attempt } ]
        in
        ( Machine { m | rng = rng1, active = Just a1 }, close )


{-| Park-Miller minimal standard generator. Deterministic so tests can replay a run.
-}
nextRandom : Int -> ( Int, Int )
nextRandom s =
    let
        n =
            modBy 2147483647 (s * 48271)
    in
    ( n, if n == 0 then 1 else n )



-- TICK


tick : Int -> Machine -> ( Machine, List Effect )
tick delta machine =
    let
        m =
            unwrap machine
    in
    case m.active of
        Nothing ->
            ( machine, [] )

        Just a0 ->
            let
                a =
                    { a0 | elapsedMs = a0.elapsedMs + delta }
            in
            if a.elapsedMs >= m.config.maxTurnMs then
                terminate (Failed DeadlineExceeded) m a

            else
                case a.conn of
                    Waiting left ->
                        if left - delta <= 0 then
                            openAttempt m a

                        else
                            ( setActive { a | conn = Waiting (left - delta) } m, [] )

                    Connecting waited ->
                        if waited + delta >= m.config.connectTimeoutMs then
                            retry ConnectTimeout Nothing m a

                        else
                            ( setActive { a | conn = Connecting (waited + delta) } m, [] )

                    Open ->
                        let
                            idle =
                                a.idleMs + delta

                            gap =
                                if Dict.isEmpty a.pending then
                                    0

                                else
                                    a.gapMs + delta

                            a1 =
                                { a | idleMs = idle, gapMs = gap }
                        in
                        if idle >= m.config.stallTimeoutMs then
                            retry Stalled Nothing m a1

                        else if gap >= m.config.gapTimeoutMs then
                            retry GapTimeout Nothing m (mapStats (\s -> { s | gaps = s.gaps + 1 }) a1)

                        else
                            ( setActive a1 m, [] )



-- RECEIVE


type Step
    = Continue Active
    | Finish FinishReason Active
    | Reconnect Failure Active
    | Abort Failure Active


receive : Envelope -> Inner -> Active -> ( Machine, List Effect )
receive env m a0 =
    let
        cfg =
            m.config

        a =
            { a0 | conn = Open }
    in
    if env.turn /= a.turn.id then
        ( setActive (mapStats (\s -> { s | stale = s.stale + 1 }) a) m, [] )

    else if env.seq < 1 then
        terminate (Failed (ProtocolViolation "sequence numbers start at 1")) m a

    else if env.seq <= a.lastSeq || Dict.member env.seq a.pending then
        ( setActive (mapStats (\s -> { s | duplicates = s.duplicates + 1 }) a) m, [] )

    else if env.seq == a.lastSeq + 1 || (env.seq - a.lastSeq <= cfg.maxReorderWindow && Dict.size a.pending < cfg.maxPending) then
        let
            reordered =
                if env.seq == a.lastSeq + 1 then
                    a

                else
                    mapStats (\s -> { s | reordered = s.reordered + 1 }) a

            a1 =
                { reordered | pending = Dict.insert env.seq env.event reordered.pending, idleMs = 0 }
        in
        finishStep m (advance cfg a1)

    else
        retry GapTooWide Nothing m (mapStats (\s -> { s | gaps = s.gaps + 1 }) a)


finishStep : Inner -> Step -> ( Machine, List Effect )
finishStep m step =
    case step of
        Continue a ->
            ( setActive a m, [] )

        Finish reason a ->
            terminate (Finished reason) m a

        Reconnect cause a ->
            retry cause Nothing m a

        Abort failure a ->
            terminate (Failed failure) m a


advance : Config -> Active -> Step
advance cfg a =
    case Dict.get (a.lastSeq + 1) a.pending of
        Nothing ->
            Continue
                (if Dict.isEmpty a.pending then
                    { a | gapMs = 0 }

                 else
                    a
                )

        Just event ->
            let
                a1 =
                    { a
                        | pending = Dict.remove (a.lastSeq + 1) a.pending
                        , lastSeq = a.lastSeq + 1
                        , failures = 0
                    }
            in
            case applyEvent cfg event a1 of
                Continue a2 ->
                    advance cfg a2

                other ->
                    other


applyEvent : Config -> StreamEvent -> Active -> Step
applyEvent cfg event a =
    case event of
        TextDelta s ->
            appendText cfg Text s a

        ReasoningDelta s ->
            appendText cfg Reasoning s a

        Unknown _ ->
            Continue a

        UsageUpdate usage ->
            Continue (mapTurn (\t -> { t | usage = Just usage }) a)

        ToolStart info ->
            if findTool info.callId a.turn /= Nothing then
                Abort (ProtocolViolation ("duplicate tool call id " ++ info.callId)) a

            else
                Continue
                    (mapTurn
                        (\t -> { t | revBlocks = Tool { callId = info.callId, name = info.name, args = "", status = Building } :: t.revBlocks })
                        a
                    )

        ToolArgs info ->
            case findTool info.callId a.turn of
                Nothing ->
                    Abort (ProtocolViolation ("arguments for unknown tool call " ++ info.callId)) a

                Just call ->
                    if call.status /= Building then
                        Abort (ProtocolViolation ("arguments after end of tool call " ++ info.callId)) a

                    else if String.length call.args + String.length info.chunk > cfg.maxToolArgsChars then
                        Abort (TooLarge "tool arguments") a

                    else
                        Continue (updateTool info.callId (\c -> { c | args = c.args ++ info.chunk }) a)

        ToolEnd callId ->
            case findTool callId a.turn of
                Nothing ->
                    Abort (ProtocolViolation ("end of unknown tool call " ++ callId)) a

                Just call ->
                    if call.status /= Building then
                        Abort (ProtocolViolation ("tool call ended twice " ++ callId)) a

                    else
                        Continue (updateTool callId (\c -> { c | status = validateArgs c.args }) a)

        Done reason ->
            Finish reason a

        ServerError info ->
            if info.retryable then
                Reconnect (ServerReported info.code info.message) a

            else
                Abort (ServerReported info.code info.message) a


appendText : Config -> (String -> Block) -> String -> Active -> Step
appendText cfg make chunk a =
    if a.textChars + String.length chunk > cfg.maxTextChars then
        Abort (TooLarge "response text") a

    else
        let
            merge existing =
                case ( make "", existing ) of
                    ( Text _, Text s ) ->
                        Just (Text (s ++ chunk))

                    ( Reasoning _, Reasoning s ) ->
                        Just (Reasoning (s ++ chunk))

                    _ ->
                        Nothing

            revBlocks =
                case a.turn.revBlocks of
                    top :: rest ->
                        case merge top of
                            Just joined ->
                                joined :: rest

                            Nothing ->
                                make chunk :: top :: rest

                    [] ->
                        [ make chunk ]
        in
        Continue (mapTurn (\t -> { t | revBlocks = revBlocks }) { a | textChars = a.textChars + String.length chunk })


mapTurn : (Turn -> Turn) -> Active -> Active
mapTurn f a =
    { a | turn = f a.turn }


findTool : String -> Turn -> Maybe ToolCall
findTool callId turn =
    turn.revBlocks
        |> List.filterMap
            (\b ->
                case b of
                    Tool call ->
                        if call.callId == callId then
                            Just call

                        else
                            Nothing

                    _ ->
                        Nothing
            )
        |> List.head


updateTool : String -> (ToolCall -> ToolCall) -> Active -> Active
updateTool callId f =
    mapTurn
        (\t ->
            { t
                | revBlocks =
                    List.map
                        (\b ->
                            case b of
                                Tool call ->
                                    if call.callId == callId then
                                        Tool (f call)

                                    else
                                        b

                                _ ->
                                    b
                        )
                        t.revBlocks
            }
        )


{-| Empty arguments mean an empty object. Anything else must parse as a JSON object.
-}
validateArgs : String -> ToolStatus
validateArgs raw =
    if String.trim raw == "" then
        Ready

    else
        case Decode.decodeString (Decode.keyValuePairs Decode.value) raw of
            Ok _ ->
                Ready

            Err err ->
                InvalidArgs (Decode.errorToString err)



-- DECODING


decodeEnvelope : String -> Result String Envelope
decodeEnvelope raw =
    Decode.decodeString envelopeDecoder raw
        |> Result.mapError Decode.errorToString


envelopeDecoder : Decoder Envelope
envelopeDecoder =
    Decode.map3 Envelope
        (Decode.field "turn" Decode.int)
        (Decode.field "seq" Decode.int)
        eventDecoder


eventDecoder : Decoder StreamEvent
eventDecoder =
    Decode.field "type" Decode.string
        |> Decode.andThen
            (\kind ->
                case kind of
                    "text" ->
                        Decode.map TextDelta (Decode.field "text" Decode.string)

                    "reasoning" ->
                        Decode.map ReasoningDelta (Decode.field "text" Decode.string)

                    "tool_start" ->
                        Decode.map2 (\id name -> ToolStart { callId = id, name = name })
                            (Decode.field "call_id" Decode.string)
                            (Decode.field "name" Decode.string)

                    "tool_args" ->
                        Decode.map2 (\id chunk -> ToolArgs { callId = id, chunk = chunk })
                            (Decode.field "call_id" Decode.string)
                            (Decode.field "chunk" Decode.string)

                    "tool_end" ->
                        Decode.map ToolEnd (Decode.field "call_id" Decode.string)

                    "usage" ->
                        Decode.map2 (\i o -> UsageUpdate { inputTokens = i, outputTokens = o })
                            (Decode.field "input_tokens" Decode.int)
                            (Decode.field "output_tokens" Decode.int)

                    "done" ->
                        Decode.map Done (Decode.field "reason" Decode.string |> Decode.map finishReason)

                    "error" ->
                        Decode.map3 (\c msg r -> ServerError { code = c, message = msg, retryable = r })
                            (Decode.field "code" Decode.string)
                            (Decode.oneOf [ Decode.field "message" Decode.string, Decode.succeed "" ])
                            (Decode.oneOf [ Decode.field "retryable" Decode.bool, Decode.succeed False ])

                    other ->
                        Decode.succeed (Unknown other)
            )


finishReason : String -> FinishReason
finishReason s =
    case s of
        "stop" ->
            Stop

        "length" ->
            Length

        "tool_calls" ->
            ToolCalls

        "content_filter" ->
            ContentFilter

        other ->
            OtherReason other
