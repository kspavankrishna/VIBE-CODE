%%% AgentRestartStormBreaker
%%%
%%% Fault-isolating admission controller for concurrent AI agent / tool-call
%%% tasks. Sits in front of task execution instead of relying on a plain OTP
%%% `supervisor`'s shared restart-intensity counter, because that counter is
%%% global to all of a supervisor's children: one poison task crashing
%%% repeatedly, plus ordinary background failures from unrelated tasks, can
%%% together exceed max_restart_intensity and take the whole supervisor (and
%%% every in-flight sibling task) down with it. This module isolates each
%%% task in its own unlinked, monitored process, classifies why it failed,
%%% quarantines only the specific offending fingerprint, and separately
%%% trips a fleet-wide breaker when failures look systemic rather than
%%% localized to one payload.
-module('AgentRestartStormBreaker').
-behaviour(gen_server).

-export([start_link/1, start_link/2, submit/3, submit/4, stats/1,
         reset_fingerprint/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(DEFAULT_TIMEOUT_MS, 30000).
-define(DEFAULT_STORM_WINDOW_MS, 10000).
-define(DEFAULT_STORM_THRESHOLD, 25).
-define(DEFAULT_STORM_COOLDOWN_MS, 30000).
-define(DEFAULT_STORM_MAX_COOLDOWN_MS, 300000).
-define(DEFAULT_PROBE_LIMIT, 3).
-define(SWEEP_INTERVAL_MS, 60000).
-define(STALE_AFTER_MS, 600000).

-record(state, {
    name,
    ledger,
    storm_events = [],
    storm_state = closed,
    storm_until = 0,
    storm_cooldown_ms = ?DEFAULT_STORM_COOLDOWN_MS,
    storm_window_ms = ?DEFAULT_STORM_WINDOW_MS,
    storm_threshold = ?DEFAULT_STORM_THRESHOLD,
    probe_limit = ?DEFAULT_PROBE_LIMIT,
    probe_count = 0,
    probe_successes = 0,
    counters = #{admitted => 0, denied_quarantine => 0, denied_storm => 0,
                 succeeded => 0, failed => 0}
}).

%%%===================================================================
%%% Public API
%%%===================================================================

start_link(Name) -> start_link(Name, #{}).

start_link(Name, Opts) when is_atom(Name), is_map(Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Opts], []).

%% submit/3,4 runs TaskFun() in its own isolated, monitored worker process.
%%
%% Fingerprint identifies *what kind of task this is* for quarantine
%% purposes (e.g. {TenantId, PromptHash} or {Tool, ArgsHash}) - it is
%% caller-defined and deliberately not derived from the task result, so the
%% same bad input retried under a different call still lands on the same
%% ledger entry.
%%
%% Opts:
%%   timeout    - hard wall-clock budget in ms for the task (default 30000).
%%                Enforced with timer:kill_after/2 against the worker
%%                process itself, independent of whether the calling
%%                process is still around to collect the result.
%%   classifier - fun(Reason) -> atom() classifying a failure reason into
%%                one of timeout | rate_limited | tool_error |
%%                invalid_output | crash (or any other atom, which is
%%                treated with the same policy as crash). Defaults to
%%                default_classifier/1. A classifier that throws, or
%%                returns a non-atom, is treated as `crash` - it can never
%%                take down the breaker or the caller.
%%
%% Returns {ok, Result} | {error, {quarantined, RemainingMs}} |
%%         {error, {storm_cooldown, RemainingMs}} |
%%         {error, {Class, Reason}}.
submit(Server, Fingerprint, TaskFun) ->
    submit(Server, Fingerprint, TaskFun, #{}).

submit(Server, Fingerprint, TaskFun, Opts) when is_function(TaskFun, 0) ->
    case gen_server:call(Server, {admit, Fingerprint}, 5000) of
        {error, _} = Denied ->
            Denied;
        {ok, Ticket} ->
            Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT_MS),
            Classifier = maps:get(classifier, Opts, fun default_classifier/1),
            Client = self(),
            Tag = make_ref(),
            {Pid, MonRef} = spawn_monitor(
                fun() -> run_worker(Server, Client, Tag, Ticket, TaskFun, Classifier) end),
            _ = timer:kill_after(Timeout, Pid),
            await_result(Server, Ticket, Pid, MonRef, Tag, Classifier)
    end.

%% Point-in-time snapshot: storm state, counters, quarantined fingerprints.
stats(Server) -> gen_server:call(Server, stats).

%% Operator escape hatch: a human fixed the bug behind a poison fingerprint
%% and wants it let back in immediately, without waiting out quarantine_ms.
reset_fingerprint(Server, Fingerprint) ->
    gen_server:call(Server, {reset_fingerprint, Fingerprint}).

%%%===================================================================
%%% Worker isolation
%%%===================================================================

run_worker(Server, Client, Tag, Ticket, TaskFun, Classifier) ->
    try TaskFun() of
        Result ->
            gen_server:cast(Server, {report, Ticket, success}),
            Client ! {Tag, {ok, Result}}
    catch
        Type:Reason ->
            Class = safe_classify(Classifier, {Type, Reason}),
            gen_server:cast(Server, {report, Ticket, {failure, Class, Reason}}),
            Client ! {Tag, {error, Class, Reason}}
    end.

await_result(Server, Ticket, Pid, MonRef, Tag, Classifier) ->
    receive
        {Tag, {ok, Result}} ->
            erlang:demonitor(MonRef, [flush]),
            {ok, Result};
        {Tag, {error, Class, Reason}} ->
            erlang:demonitor(MonRef, [flush]),
            {error, {Class, Reason}};
        {'DOWN', MonRef, process, Pid, Reason} ->
            %% The worker never got to self-report: either timer:kill_after
            %% fired (Reason == killed, classified below as `timeout`) or it
            %% died in a way try/catch cannot intercept. Report on its
            %% behalf so the ledger still learns about the failure.
            Class = safe_classify(Classifier, Reason),
            gen_server:cast(Server, {report, Ticket, {failure, Class, Reason}}),
            {error, {Class, Reason}}
    end.

safe_classify(Classifier, Reason) ->
    try Classifier(Reason) of
        Class when is_atom(Class) -> Class;
        _ -> crash
    catch
        _:_ -> crash
    end.

%% Default classification for common OTP/HTTP failure shapes. Notably maps
%% the `killed` reason (produced by our own timer:kill_after hard timeout)
%% to `timeout` rather than `crash` - conflating the two would apply the
%% long poison-payload quarantine to a task that was merely slow, e.g.
%% because it was waiting on a stalled upstream connection.
default_classifier({Type, Reason}) when Type =:= error; Type =:= exit; Type =:= throw ->
    classify_reason(Reason);
default_classifier(Reason) ->
    classify_reason(Reason).

classify_reason(timeout) -> timeout;
classify_reason(killed) -> timeout;
classify_reason({timeout, _}) -> timeout;
classify_reason({shutdown, timeout}) -> timeout;
classify_reason({http_error, 429, _}) -> rate_limited;
classify_reason({rate_limited, _}) -> rate_limited;
classify_reason(rate_limited) -> rate_limited;
classify_reason({tool_error, _}) -> tool_error;
classify_reason({invalid_output, _}) -> invalid_output;
classify_reason({badjson, _}) -> invalid_output;
classify_reason(_) -> crash.

%%%===================================================================
%%% Per-classification policy
%%%===================================================================

%% max_failures = infinity means this classification never quarantines a
%% fingerprint on its own (rate_limited is the provider's fault, not the
%% payload's) - it still feeds the fleet-wide storm signal, and with a
%% higher storm_weight, because synchronized rate limiting across many
%% distinct fingerprints is strong evidence of a systemic outage.
policy(crash)          -> #{max_failures => 3,        window_ms => 60000,  quarantine_ms => 300000, storm_weight => 1};
policy(invalid_output) -> #{max_failures => 3,        window_ms => 60000,  quarantine_ms => 300000, storm_weight => 1};
policy(tool_error)     -> #{max_failures => 5,        window_ms => 120000, quarantine_ms => 120000, storm_weight => 1};
policy(timeout)        -> #{max_failures => 5,        window_ms => 120000, quarantine_ms => 90000,  storm_weight => 2};
policy(rate_limited)   -> #{max_failures => infinity, window_ms => 60000,  quarantine_ms => 0,      storm_weight => 3};
policy(_Other)         -> #{max_failures => 3,        window_ms => 60000,  quarantine_ms => 180000, storm_weight => 1}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name, Opts]) ->
    Ledger = ets:new(agent_restart_storm_breaker_ledger, [set, protected]),
    erlang:send_after(?SWEEP_INTERVAL_MS, self(), sweep),
    State = #state{
        name = Name,
        ledger = Ledger,
        storm_window_ms = maps:get(storm_window_ms, Opts, ?DEFAULT_STORM_WINDOW_MS),
        storm_threshold = maps:get(storm_threshold, Opts, ?DEFAULT_STORM_THRESHOLD),
        storm_cooldown_ms = maps:get(storm_cooldown_ms, Opts, ?DEFAULT_STORM_COOLDOWN_MS),
        probe_limit = maps:get(probe_limit, Opts, ?DEFAULT_PROBE_LIMIT)
    },
    {ok, State}.

handle_call({admit, Fingerprint}, _From, State) ->
    NowMs = now_ms(),
    case fingerprint_quarantined(Fingerprint, State, NowMs) of
        {true, RemainingMs} ->
            {reply, {error, {quarantined, RemainingMs}}, bump_counter(denied_quarantine, State)};
        false ->
            case storm_gate(State, NowMs) of
                {deny, RemainingMs, State1} ->
                    {reply, {error, {storm_cooldown, RemainingMs}}, bump_counter(denied_storm, State1)};
                {allow, IsProbe, State1} ->
                    Ticket = #{fingerprint => Fingerprint, probe => IsProbe},
                    {reply, {ok, Ticket}, bump_counter(admitted, State1)}
            end
    end;
handle_call({reset_fingerprint, Fingerprint}, _From, State) ->
    ets:delete(State#state.ledger, Fingerprint),
    {reply, ok, State};
handle_call(stats, _From, State) ->
    {reply, build_stats(State), State};
handle_call(_Other, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({report, Ticket, Outcome}, State) ->
    #{fingerprint := FP, probe := IsProbe} = Ticket,
    NowMs = now_ms(),
    case Outcome of
        success ->
            ledger_clear_or_decay(State#state.ledger, FP, NowMs),
            State1 = bump_counter(succeeded, State),
            {noreply, storm_on_success(IsProbe, State1)};
        {failure, Class, _Reason} ->
            ledger_record_failure(State#state.ledger, FP, Class, NowMs),
            State1 = bump_counter(failed, State),
            State2 = storm_record_failure(FP, Class, State1, NowMs),
            {noreply, storm_on_failure(IsProbe, State2)}
    end;
handle_cast(_Other, State) ->
    {noreply, State}.

handle_info(sweep, State) ->
    sweep_ledger(State#state.ledger, now_ms()),
    erlang:send_after(?SWEEP_INTERVAL_MS, self(), sweep),
    {noreply, State};
handle_info(_Other, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Fingerprint ledger (ETS: {Fingerprint, FailCount, WindowStartMs, LastClass, QuarantinedUntilMs})
%%%===================================================================

fingerprint_quarantined(FP, State, NowMs) ->
    case ets:lookup(State#state.ledger, FP) of
        [{FP, _Count, _WStart, _Class, QUntil}] when QUntil > NowMs -> {true, QUntil - NowMs};
        _ -> false
    end.

ledger_record_failure(Tid, FP, Class, NowMs) ->
    Policy = policy(Class),
    WindowMs = maps:get(window_ms, Policy),
    MaxFailures = maps:get(max_failures, Policy),
    QuarantineMs = maps:get(quarantine_ms, Policy),
    {Count0, WStart0, QUntil0} = case ets:lookup(Tid, FP) of
        [{FP, C, W, _PrevClass, QU}] -> {C, W, QU};
        [] -> {0, NowMs, 0}
    end,
    {Count1, WStart1} = case NowMs - WStart0 > WindowMs of
        true -> {1, NowMs};
        false -> {Count0 + 1, WStart0}
    end,
    QUntil1 = case MaxFailures =/= infinity andalso Count1 >= MaxFailures of
        true -> NowMs + QuarantineMs;
        false -> QUntil0
    end,
    ets:insert(Tid, {FP, Count1, WStart1, Class, QUntil1}),
    ok.

%% A success decays the failure count by one instead of clearing it
%% outright, so a fingerprint that fails occasionally in a long session
%% doesn't get treated as spotless after a single lucky retry. Once the
%% count reaches zero the entry is deleted, which together with sweep/1
%% keeps this table from growing without bound over the process lifetime.
ledger_clear_or_decay(Tid, FP, NowMs) ->
    case ets:lookup(Tid, FP) of
        [{FP, Count, WStart, Class, QUntil}] when QUntil =< NowMs ->
            case Count - 1 of
                N when N =< 0 -> ets:delete(Tid, FP);
                N -> ets:insert(Tid, {FP, N, WStart, Class, QUntil})
            end;
        _ -> ok
    end.

%% Purges fingerprints that are both out of quarantine and have not been
%% touched in ?STALE_AFTER_MS - covers entries that failed a couple of
%% times and were then simply never submitted again, which decay-on-success
%% alone would never clean up.
sweep_ledger(Tid, NowMs) ->
    ets:foldl(fun({FP, _Count, WStart, _Class, QUntil}, Acc) ->
        case QUntil =< NowMs andalso (NowMs - WStart) > ?STALE_AFTER_MS of
            true -> ets:delete(Tid, FP);
            false -> ok
        end,
        Acc
    end, ok, Tid).

%%%===================================================================
%%% Fleet-wide storm breaker (closed -> open -> half_open -> closed)
%%%===================================================================

storm_gate(State = #state{storm_state = open, storm_until = Until}, NowMs) when NowMs >= Until ->
    storm_gate(State#state{storm_state = half_open, probe_count = 0, probe_successes = 0}, NowMs);
storm_gate(State = #state{storm_state = open, storm_until = Until}, NowMs) ->
    {deny, Until - NowMs, State};
storm_gate(State = #state{storm_state = half_open, probe_count = PC, probe_limit = PL}, _NowMs) when PC >= PL ->
    {deny, 1000, State};
storm_gate(State = #state{storm_state = half_open, probe_count = PC}, _NowMs) ->
    {allow, true, State#state{probe_count = PC + 1}};
storm_gate(State = #state{storm_state = closed}, _NowMs) ->
    {allow, false, State}.

%% Records this failure in the sliding storm window (one slot per distinct
%% fingerprint, refreshed to the latest timestamp/weight) and trips the
%% breaker if the weighted count of *distinct* fingerprints failing inside
%% storm_window_ms crosses storm_threshold. Deduping by fingerprint means a
%% single already-quarantined poison payload can contribute at most
%% max_failures entries before it stops generating new ones - it cannot by
%% itself manufacture a storm.
storm_record_failure(FP, Class, State, NowMs) ->
    Weight = maps:get(storm_weight, policy(Class), 1),
    Pruned = prune_storm_events(State#state.storm_events, NowMs, State#state.storm_window_ms),
    Events = upsert_latest(Pruned, FP, NowMs, Weight),
    Score = lists:foldl(fun({_Ts, _FP, W}, Acc) -> Acc + W end, 0, Events),
    State1 = State#state{storm_events = Events},
    case State1#state.storm_state of
        closed when Score >= State1#state.storm_threshold -> trip_storm(State1, NowMs);
        _ -> State1
    end.

trip_storm(State, NowMs) ->
    State#state{
        storm_state = open,
        storm_until = NowMs + State#state.storm_cooldown_ms,
        probe_count = 0,
        probe_successes = 0
    }.

%% A probe failing during half_open is decisive on its own: whatever caused
%% the storm is still there, so reopen immediately (don't wait for the
%% score to cross the threshold again) with the cooldown doubled, capped at
%% ?DEFAULT_STORM_MAX_COOLDOWN_MS.
storm_on_failure(true, State) ->
    NowMs = now_ms(),
    NewCooldown = min(State#state.storm_cooldown_ms * 2, ?DEFAULT_STORM_MAX_COOLDOWN_MS),
    State#state{
        storm_state = open,
        storm_until = NowMs + NewCooldown,
        storm_cooldown_ms = NewCooldown,
        probe_count = 0,
        probe_successes = 0
    };
storm_on_failure(false, State) -> State.

%% Concurrent probes can interleave; a success landing after a sibling
%% probe's failure already reopened the breaker simply counts toward the
%% *next* half-open cycle instead of being discarded. Reopening always
%% resets probe_successes to zero, so a stray success can never falsely
%% close the breaker early.
storm_on_success(true, State) ->
    Successes = State#state.probe_successes + 1,
    case Successes >= State#state.probe_limit of
        true ->
            State#state{
                storm_state = closed,
                storm_events = [],
                probe_count = 0,
                probe_successes = 0,
                storm_cooldown_ms = ?DEFAULT_STORM_COOLDOWN_MS
            };
        false ->
            State#state{probe_successes = Successes}
    end;
storm_on_success(false, State) -> State.

prune_storm_events(Events, NowMs, WindowMs) ->
    Cutoff = NowMs - WindowMs,
    [E || {Ts, _FP, _W} = E <- Events, Ts >= Cutoff].

upsert_latest(Events, FP, NowMs, Weight) ->
    Rest = [E || {_Ts, FP0, _W} = E <- Events, FP0 =/= FP],
    [{NowMs, FP, Weight} | Rest].

%%%===================================================================
%%% Misc
%%%===================================================================

bump_counter(Key, State) ->
    Counters = maps:update_with(Key, fun(V) -> V + 1 end, 1, State#state.counters),
    State#state{counters = Counters}.

build_stats(State) ->
    NowMs = now_ms(),
    Quarantined = ets:foldl(fun({FP, _Count, _WStart, Class, QUntil}, Acc) ->
        case QUntil > NowMs of
            true -> [{FP, Class, QUntil - NowMs} | Acc];
            false -> Acc
        end
    end, [], State#state.ledger),
    #{
        storm_state => State#state.storm_state,
        storm_remaining_ms => max(0, State#state.storm_until - NowMs),
        counters => State#state.counters,
        quarantined_count => length(Quarantined),
        quarantined => Quarantined,
        ledger_size => ets:info(State#state.ledger, size)
    }.

now_ms() -> erlang:system_time(millisecond).
