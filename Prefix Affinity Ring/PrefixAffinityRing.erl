-module('PrefixAffinityRing').
-behaviour(gen_server).

-export([start_link/1, start_link/2, stop/1]).
-export([prefix_key/2, prefix_key/3, prefix_key_from_blocks/3]).
-export([acquire/2, release/3]).
-export([add_replica/2, update_replica/3, drain/2, remove_replica/2]).
-export([preview/2, owner/2, sweep/1, snapshot/1]).
-export([selftest/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(MASK64, 16#FFFFFFFFFFFFFFFF).
-define(TWO53, 9007199254740992.0).
-define(DEFAULT_BLOCK_BYTES, 256).
-define(DEFAULT_DEPTH, 4).
-define(DEFAULT_LOAD_FACTOR, 1.25).
-define(DEFAULT_FAILURE_THRESHOLD, 3).
-define(DEFAULT_BASE_COOLDOWN_MS, 1000).
-define(DEFAULT_MAX_COOLDOWN_MS, 60000).
-define(DEFAULT_LEASE_TTL_MS, 120000).
-define(DEFAULT_SWEEP_MS, 5000).
-define(CALL_TIMEOUT, 5000).
-define(MAX_LEVEL, 20).

-record(replica, {
    id,
    weight,
    max_inflight,
    seed,
    inflight = 0,
    status = active,
    health = healthy,
    fails = 0,
    level = 0
}).

-record(lease, {replica, owner, mon, deadline}).

-record(st, {
    replicas = #{},
    leases = #{},
    monitors = #{},
    clock,
    load_factor,
    threshold,
    base_cooldown,
    max_cooldown,
    lease_ttl,
    sweep_ms,
    timer = undefined,
    stats
}).

start_link(Opts) when is_map(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

start_link(Name, Opts) when is_atom(Name), is_map(Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, Opts, []).

stop(Server) ->
    gen_server:stop(Server).

prefix_key(Tenant, Prompt) ->
    prefix_key(Tenant, Prompt, #{}).

prefix_key(Tenant, Prompt, Opts) when is_binary(Tenant), is_binary(Prompt), is_map(Opts) ->
    Block = pos_int(maps:get(block_bytes, Opts, ?DEFAULT_BLOCK_BYTES), block_bytes),
    Depth = pos_int(maps:get(depth, Opts, ?DEFAULT_DEPTH), depth),
    Complete = (byte_size(Prompt) div Block) * Block,
    Take =
        case Complete of
            0 -> byte_size(Prompt);
            _ -> min(Depth * Block, Complete)
        end,
    Head = binary:part(Prompt, 0, Take),
    crypto:hash(sha256, [<<"pfx1", (byte_size(Tenant)):32>>, Tenant, Head]).

prefix_key_from_blocks(Tenant, Blocks, Depth) when is_binary(Tenant), is_list(Blocks) ->
    N = pos_int(Depth, depth),
    Taken = lists:sublist(Blocks, N),
    true = lists:all(fun is_binary/1, Taken),
    Framed = [[<<(byte_size(B)):32>>, B] || B <- Taken],
    crypto:hash(sha256, [<<"blk1", (byte_size(Tenant)):32>>, Tenant, Framed]).

acquire(Server, Key) when is_binary(Key) ->
    gen_server:call(Server, {acquire, Key, self()}, ?CALL_TIMEOUT).

release(Server, Lease, Outcome) when is_reference(Lease) ->
    true = valid_outcome(Outcome),
    gen_server:call(Server, {release, Lease, Outcome}, ?CALL_TIMEOUT).

add_replica(Server, Spec) when is_map(Spec) ->
    gen_server:call(Server, {add_replica, Spec}, ?CALL_TIMEOUT).

update_replica(Server, Id, Changes) when is_map(Changes) ->
    gen_server:call(Server, {update_replica, Id, Changes}, ?CALL_TIMEOUT).

drain(Server, Id) ->
    gen_server:call(Server, {drain, Id}, ?CALL_TIMEOUT).

remove_replica(Server, Id) ->
    gen_server:call(Server, {remove_replica, Id}, ?CALL_TIMEOUT).

preview(Server, Key) when is_binary(Key) ->
    gen_server:call(Server, {preview, Key}, ?CALL_TIMEOUT).

sweep(Server) ->
    gen_server:call(Server, sweep, ?CALL_TIMEOUT).

snapshot(Server) ->
    gen_server:call(Server, snapshot, ?CALL_TIMEOUT).

owner(Key, Specs) when is_binary(Key), is_list(Specs) ->
    case rank_ids(Key, Specs) of
        [] -> undefined;
        [Id | _] -> Id
    end.

rank_ids(Key, Specs) ->
    Rs = [
        #replica{id = Id, weight = W, seed = seed(Id)}
     || {Id, W} <- Specs
    ],
    [R#replica.id || R <- ranked(hash64(Key), Rs)].

init(Opts) ->
    process_flag(trap_exit, true),
    Clock = maps:get(clock, Opts, fun() -> erlang:monotonic_time(millisecond) end),
    LoadFactor = number_ge(maps:get(load_factor, Opts, ?DEFAULT_LOAD_FACTOR), 1.0, load_factor),
    St0 = #st{
        clock = Clock,
        load_factor = LoadFactor,
        threshold = pos_int(maps:get(failure_threshold, Opts, ?DEFAULT_FAILURE_THRESHOLD), failure_threshold),
        base_cooldown = pos_int(maps:get(base_cooldown_ms, Opts, ?DEFAULT_BASE_COOLDOWN_MS), base_cooldown_ms),
        max_cooldown = pos_int(maps:get(max_cooldown_ms, Opts, ?DEFAULT_MAX_COOLDOWN_MS), max_cooldown_ms),
        lease_ttl = pos_int(maps:get(lease_ttl_ms, Opts, ?DEFAULT_LEASE_TTL_MS), lease_ttl_ms),
        sweep_ms = maps:get(sweep_ms, Opts, ?DEFAULT_SWEEP_MS),
        stats = #{
            acquired => 0,
            affinity => 0,
            spilled => 0,
            overflow => 0,
            probes => 0,
            rejected => 0,
            ejections => 0,
            expired => 0
        }
    },
    St1 = lists:foldl(
        fun(Spec, Acc) ->
            case do_add(Spec, Acc) of
                {ok, Acc2} -> Acc2;
                {error, Reason} -> erlang:error({bad_replica, Spec, Reason})
            end
        end,
        St0,
        maps:get(replicas, Opts, [])
    ),
    {ok, schedule_sweep(St1)}.

handle_call({acquire, Key, Owner}, _From, St) ->
    Now = now_ms(St),
    case choose(hash64(Key), St, Now) of
        {ok, R, Rank, Mode} ->
            {Ref, St1} = grant(R, Owner, Mode, Now, St),
            {reply, {ok, Ref, R#replica.id, Rank}, bump_acquire(Mode, Rank, St1)};
        {error, Reason} ->
            {reply, {error, Reason}, bump(rejected, St)}
    end;
handle_call({release, Ref, Outcome}, _From, St) ->
    {Reply, St1} = finish(Ref, Outcome, St, now_ms(St)),
    {reply, Reply, St1};
handle_call({add_replica, Spec}, _From, St) ->
    case do_add(Spec, St) of
        {ok, St1} -> {reply, ok, St1};
        {error, _} = E -> {reply, E, St}
    end;
handle_call({update_replica, Id, Changes}, _From, St) ->
    case maps:find(Id, St#st.replicas) of
        error ->
            {reply, {error, unknown_replica}, St};
        {ok, R} ->
            try
                W = maps:get(weight, Changes, R#replica.weight),
                M = maps:get(max_inflight, Changes, R#replica.max_inflight),
                R1 = R#replica{
                    weight = number_gt0(W, weight),
                    max_inflight = pos_int(M, max_inflight)
                },
                {reply, ok, St#st{replicas = maps:put(Id, R1, St#st.replicas)}}
            catch
                error:{bad_option, _, _} = E -> {reply, {error, E}, St}
            end
    end;
handle_call({drain, Id}, _From, St) ->
    case maps:find(Id, St#st.replicas) of
        error ->
            {reply, {error, unknown_replica}, St};
        {ok, #replica{inflight = 0}} ->
            {reply, ok, St#st{replicas = maps:remove(Id, St#st.replicas)}};
        {ok, R} ->
            R1 = R#replica{status = draining},
            {reply, ok, St#st{replicas = maps:put(Id, R1, St#st.replicas)}}
    end;
handle_call({remove_replica, Id}, _From, St) ->
    case maps:is_key(Id, St#st.replicas) of
        true -> {reply, ok, St#st{replicas = maps:remove(Id, St#st.replicas)}};
        false -> {reply, {error, unknown_replica}, St}
    end;
handle_call({preview, Key}, _From, St) ->
    Active = [R || R <- maps:values(St#st.replicas), R#replica.status =:= active],
    {reply, [R#replica.id || R <- ranked(hash64(Key), Active)], St};
handle_call(sweep, _From, St) ->
    {reply, ok, do_sweep(St)};
handle_call(snapshot, _From, St) ->
    Now = now_ms(St),
    Rs = [
        #{
            id => R#replica.id,
            weight => R#replica.weight,
            max_inflight => R#replica.max_inflight,
            inflight => R#replica.inflight,
            status => R#replica.status,
            health => public_health(R#replica.health, Now),
            consecutive_failures => R#replica.fails,
            ejection_level => R#replica.level
        }
     || R <- lists:keysort(#replica.id, maps:values(St#st.replicas))
    ],
    {reply, #{replicas => Rs, leases => maps:size(St#st.leases), stats => St#st.stats}, St};
handle_call(_Other, _From, St) ->
    {reply, {error, unknown_call}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({'DOWN', Mon, process, _Pid, _Reason}, St) ->
    case maps:find(Mon, St#st.monitors) of
        {ok, Ref} ->
            {_, St1} = finish(Ref, cancelled, St, now_ms(St)),
            {noreply, St1};
        error ->
            {noreply, St}
    end;
handle_info(sweep_tick, St) ->
    {noreply, schedule_sweep(do_sweep(St#st{timer = undefined}))};
handle_info(_Info, St) ->
    {noreply, St}.

terminate(_Reason, St) ->
    _ = cancel_timer(St#st.timer),
    ok.

do_add(Spec, St) ->
    try
        Id = maps:get(id, Spec),
        true = is_binary(Id) orelse is_atom(Id),
        W = number_gt0(maps:get(weight, Spec, 1), weight),
        M = pos_int(maps:get(max_inflight, Spec, 64), max_inflight),
        case maps:is_key(Id, St#st.replicas) of
            true ->
                {error, already_present};
            false ->
                R = #replica{id = Id, weight = W, max_inflight = M, seed = seed(Id)},
                {ok, St#st{replicas = maps:put(Id, R, St#st.replicas)}}
        end
    catch
        error:{bad_option, _, _} = E -> {error, E};
        error:{badkey, K} -> {error, {missing, K}};
        error:{badmatch, _} -> {error, bad_id}
    end.

choose(KeyH, #st{replicas = Rs, load_factor = C}, Now) ->
    Active = [R || R <- maps:values(Rs), R#replica.status =:= active],
    case Active of
        [] ->
            {error, no_replicas};
        _ ->
            Ranked = ranked(KeyH, Active),
            Healthy = [R || R <- Active, R#replica.health =:= healthy],
            Total = lists:sum([R#replica.inflight || R <- Healthy]) + 1,
            WSum = lists:sum([R#replica.weight || R <- Healthy]),
            case walk(Ranked, 0, Now, Total, WSum, C) of
                {ok, _, _, _} = Hit -> Hit;
                none -> overflow(Ranked, Healthy, Now)
            end
    end.

walk([], _Rank, _Now, _Total, _WSum, _C) ->
    none;
walk([R | Rest], Rank, Now, Total, WSum, C) ->
    case availability(R, Now) of
        probe ->
            {ok, R, Rank, probe};
        available ->
            Limit = max(1, ceil(C * Total * R#replica.weight / WSum)),
            case R#replica.inflight + 1 =< Limit of
                true -> {ok, R, Rank, normal};
                false -> walk(Rest, Rank + 1, Now, Total, WSum, C)
            end;
        unavailable ->
            walk(Rest, Rank + 1, Now, Total, WSum, C)
    end.

overflow(Ranked, Healthy, Now) ->
    Indexed = lists:zip(lists:seq(0, length(Ranked) - 1), Ranked),
    Avail = [{Rank, R} || {Rank, R} <- Indexed, availability(R, Now) =:= available],
    case Avail of
        [] ->
            case Healthy of
                [] -> {error, no_healthy_replicas};
                _ -> {error, overloaded}
            end;
        _ ->
            Best = lists:min([
                {R#replica.inflight / R#replica.weight, Rank, R}
             || {Rank, R} <- Avail
            ]),
            {_, BestRank, BestR} = Best,
            {ok, BestR, BestRank, overflow}
    end.

availability(#replica{health = healthy, inflight = I, max_inflight = M}, _Now) ->
    case I < M of
        true -> available;
        false -> unavailable
    end;
availability(#replica{health = {ejected, Until}}, Now) when Now >= Until ->
    probe;
availability(_, _Now) ->
    unavailable.

grant(R, Owner, Mode, Now, St) ->
    Ref = make_ref(),
    Mon = erlang:monitor(process, Owner),
    Health =
        case Mode of
            probe -> {probing, Ref};
            _ -> R#replica.health
        end,
    R1 = R#replica{inflight = R#replica.inflight + 1, health = Health},
    Lease = #lease{
        replica = R#replica.id,
        owner = Owner,
        mon = Mon,
        deadline = Now + St#st.lease_ttl
    },
    St1 = St#st{
        replicas = maps:put(R#replica.id, R1, St#st.replicas),
        leases = maps:put(Ref, Lease, St#st.leases),
        monitors = maps:put(Mon, Ref, St#st.monitors)
    },
    {Ref, St1}.

finish(Ref, Outcome, St, Now) ->
    case maps:take(Ref, St#st.leases) of
        error ->
            {{error, unknown_lease}, St};
        {Lease, Leases} ->
            erlang:demonitor(Lease#lease.mon, [flush]),
            St1 = St#st{
                leases = Leases,
                monitors = maps:remove(Lease#lease.mon, St#st.monitors)
            },
            {ok, settle(Lease#lease.replica, Ref, Outcome, St1, Now)}
    end.

settle(Id, Ref, Outcome, St, Now) ->
    case maps:find(Id, St#st.replicas) of
        error ->
            St;
        {ok, R} ->
            R1 = R#replica{inflight = max(0, R#replica.inflight - 1)},
            {R2, Ejected} = health_after(R1, Ref, Outcome, St, Now),
            St1 = case Ejected of
                true -> bump(ejections, St);
                false -> St
            end,
            case R2#replica.status =:= draining andalso R2#replica.inflight =:= 0 of
                true -> St1#st{replicas = maps:remove(Id, St1#st.replicas)};
                false -> St1#st{replicas = maps:put(Id, R2, St1#st.replicas)}
            end
    end.

health_after(R, Ref, ok, _St, _Now) ->
    case R#replica.health of
        {probing, Ref} -> {R#replica{health = healthy, fails = 0, level = 0}, false};
        healthy -> {R#replica{fails = 0}, false};
        _ -> {R, false}
    end;
health_after(R, Ref, {error, _}, St, Now) ->
    case R#replica.health of
        {probing, Ref} ->
            {eject(R, St, Now), true};
        healthy ->
            F = R#replica.fails + 1,
            case F >= St#st.threshold of
                true -> {eject(R, St, Now), true};
                false -> {R#replica{fails = F}, false}
            end;
        _ ->
            {R, false}
    end;
health_after(R, Ref, cancelled, _St, Now) ->
    case R#replica.health of
        {probing, Ref} -> {R#replica{health = {ejected, Now}}, false};
        _ -> {R, false}
    end.

eject(R, St, Now) ->
    Level = min(R#replica.level, ?MAX_LEVEL),
    Cool = min(St#st.max_cooldown, St#st.base_cooldown * (1 bsl Level)),
    Jitter = trunc(Cool * 0.2 * (erlang:phash2({R#replica.id, Level}, 1000) / 1000)),
    R#replica{
        health = {ejected, Now + Cool + Jitter},
        fails = 0,
        level = Level + 1
    }.

do_sweep(St) ->
    Now = now_ms(St),
    Expired = [Ref || {Ref, L} <- maps:to_list(St#st.leases), L#lease.deadline =< Now],
    lists:foldl(
        fun(Ref, Acc) ->
            {_, Acc1} = finish(Ref, {error, lease_expired}, Acc, Now),
            bump(expired, Acc1)
        end,
        St,
        Expired
    ).

schedule_sweep(#st{sweep_ms = infinity} = St) ->
    St;
schedule_sweep(#st{sweep_ms = Ms} = St) when is_integer(Ms), Ms > 0 ->
    _ = cancel_timer(St#st.timer),
    St#st{timer = erlang:send_after(Ms, self(), sweep_tick)}.

cancel_timer(undefined) -> ok;
cancel_timer(T) -> erlang:cancel_timer(T).

ranked(KeyH, Replicas) ->
    Scored = [{-score(KeyH, R), R#replica.id, R} || R <- Replicas],
    [R || {_, _, R} <- lists:sort(Scored)].

score(KeyH, #replica{seed = Seed, weight = W}) ->
    H = mix64(KeyH bxor Seed),
    U = ((H bsr 11) + 0.5) / ?TWO53,
    W / (-math:log(U)).

mix64(X) ->
    Z1 = (X + 16#9E3779B97F4A7C15) band ?MASK64,
    Z2 = ((Z1 bxor (Z1 bsr 30)) * 16#BF58476D1CE4E5B9) band ?MASK64,
    Z3 = ((Z2 bxor (Z2 bsr 27)) * 16#94D049BB133111EB) band ?MASK64,
    Z3 bxor (Z3 bsr 31).

hash64(Bin) ->
    <<H:64, _/binary>> = crypto:hash(sha256, Bin),
    H.

seed(Id) when is_binary(Id) -> mix64(hash64(Id));
seed(Id) when is_atom(Id) -> seed(atom_to_binary(Id, utf8)).

public_health(healthy, _Now) -> healthy;
public_health({ejected, Until}, Now) when Now >= Until -> {ejected, probe_ready};
public_health({ejected, Until}, Now) -> {ejected, Until - Now};
public_health({probing, _}, _Now) -> probing.

valid_outcome(ok) -> true;
valid_outcome(cancelled) -> true;
valid_outcome({error, _}) -> true;
valid_outcome(_) -> false.

now_ms(#st{clock = Clock}) -> Clock().

bump(Key, #st{stats = S} = St) ->
    St#st{stats = maps:update_with(Key, fun(V) -> V + 1 end, 1, S)}.

bump_acquire(Mode, Rank, St) ->
    St1 = bump(acquired, St),
    case {Mode, Rank} of
        {probe, _} -> bump(probes, St1);
        {overflow, _} -> bump(overflow, St1);
        {normal, 0} -> bump(affinity, St1);
        {normal, _} -> bump(spilled, St1)
    end.

pos_int(V, _Name) when is_integer(V), V > 0 -> V;
pos_int(V, Name) -> erlang:error({bad_option, Name, V}).

number_gt0(V, _Name) when is_number(V), V > 0 -> V;
number_gt0(V, Name) -> erlang:error({bad_option, Name, V}).

number_ge(V, Min, _Name) when is_number(V), V >= Min -> V;
number_ge(V, _Min, Name) -> erlang:error({bad_option, Name, V}).

selftest() ->
    ok = t_prefix_key(),
    ok = t_minimal_disruption(),
    ok = t_weights(),
    ok = t_bounded_load(),
    ok = t_health_cycle(),
    ok = t_owner_down(),
    ok = t_lease_expiry(),
    ok = t_drain(),
    ok = t_validation(),
    ok.

keys(N) ->
    [<<"key-", (integer_to_binary(I))/binary>> || I <- lists:seq(1, N)].

fake_clock() ->
    A = atomics:new(1, []),
    {A, fun() -> atomics:get(A, 1) end}.

advance(A, Ms) ->
    atomics:add(A, 1, Ms).

t_prefix_key() ->
    Sys = binary:copy(<<"s">>, 1024),
    K1 = prefix_key(<<"acme">>, <<Sys/binary, "question one">>),
    K2 = prefix_key(<<"acme">>, <<Sys/binary, "a different question">>),
    K3 = prefix_key(<<"other">>, <<Sys/binary, "question one">>),
    true = K1 =:= K2,
    true = K1 =/= K3,
    S1 = prefix_key(<<"acme">>, <<"hi">>),
    S2 = prefix_key(<<"acme">>, <<"ho">>),
    true = S1 =/= S2,
    true = prefix_key(<<"a">>, <<"bc">>) =/= prefix_key(<<"ab">>, <<"c">>),
    B1 = prefix_key_from_blocks(<<"t">>, [<<"a">>, <<"b">>, <<"c">>], 2),
    B2 = prefix_key_from_blocks(<<"t">>, [<<"a">>, <<"b">>, <<"z">>], 2),
    true = B1 =:= B2,
    true = prefix_key_from_blocks(<<"t">>, [<<"ab">>], 2) =/= prefix_key_from_blocks(<<"t">>, [<<"a">>, <<"b">>], 2),
    ok.

t_minimal_disruption() ->
    Ids = [<<"r", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 8)],
    Specs = [{Id, 1} || Id <- Ids],
    Without = [S || {Id, _} = S <- Specs, Id =/= <<"r3">>],
    Ks = keys(20000),
    Moved = lists:foldl(
        fun(K, Acc) ->
            Before = owner(K, Specs),
            After = owner(K, Without),
            case Before of
                <<"r3">> -> Acc + 1;
                _ ->
                    Before = After,
                    Acc
            end
        end,
        0,
        Ks
    ),
    Frac = Moved / 20000,
    true = Frac > 0.10 andalso Frac < 0.15,
    ok.

t_weights() ->
    Specs = [{<<"a">>, 1}, {<<"b">>, 2}, {<<"c">>, 1}],
    N = 40000,
    Counts = lists:foldl(
        fun(K, Acc) -> maps:update_with(owner(K, Specs), fun(V) -> V + 1 end, 1, Acc) end,
        #{},
        keys(N)
    ),
    Share = maps:get(<<"b">>, Counts) / N,
    true = abs(Share - 0.5) < 0.02,
    ok.

t_bounded_load() ->
    Specs = [#{id => <<"r", (integer_to_binary(I))/binary>>, weight => 1, max_inflight => 1000} || I <- lists:seq(1, 4)],
    {ok, S} = start_link(#{replicas => Specs, sweep_ms => infinity}),
    Key = <<"hot-system-prompt">>,
    [Head | _] = preview(S, Key),
    lists:foreach(fun(_) -> {ok, _, _, _} = acquire(S, Key) end, lists:seq(1, 100)),
    #{replicas := Rs, stats := Stats} = snapshot(S),
    Loads = [{maps:get(id, R), maps:get(inflight, R)} || R <- Rs],
    100 = lists:sum([L || {_, L} <- Loads]),
    true = lists:max([L || {_, L} <- Loads]) =< 32,
    true = proplists:get_value(Head, Loads) >= 25,
    true = maps:get(spilled, Stats) > 0,
    stop(S).

t_health_cycle() ->
    {A, Clock} = fake_clock(),
    {ok, S} = start_link(#{
        replicas => [#{id => <<"x">>}, #{id => <<"y">>}],
        failure_threshold => 2,
        base_cooldown_ms => 1000,
        max_cooldown_ms => 8000,
        clock => Clock,
        sweep_ms => infinity
    }),
    Key = <<"some-prefix">>,
    [Head, Other] = preview(S, Key),
    {ok, L1, Head, 0} = acquire(S, Key),
    ok = release(S, L1, {error, upstream_503}),
    {ok, L2, Head, 0} = acquire(S, Key),
    ok = release(S, L2, {error, upstream_503}),
    {ok, L3, Other, 1} = acquire(S, Key),
    ok = release(S, L3, ok),
    advance(A, 2000),
    {ok, Probe, Head, 0} = acquire(S, Key),
    {ok, L4, Other, 1} = acquire(S, Key),
    ok = release(S, L4, ok),
    ok = release(S, Probe, ok),
    {ok, L5, Head, 0} = acquire(S, Key),
    ok = release(S, L5, ok),
    {error, unknown_lease} = release(S, L5, ok),
    #{replicas := Rs, stats := Stats} = snapshot(S),
    true = lists:all(fun(R) -> maps:get(health, R) =:= healthy end, Rs),
    1 = maps:get(ejections, Stats),
    1 = maps:get(probes, Stats),
    stop(S).

t_owner_down() ->
    {ok, S} = start_link(#{replicas => [#{id => <<"x">>}], sweep_ms => infinity}),
    Self = self(),
    Pid = spawn(fun() ->
        {ok, _, _, _} = acquire(S, <<"k">>),
        Self ! acquired
    end),
    receive acquired -> ok after 2000 -> erlang:error(timeout) end,
    MRef = erlang:monitor(process, Pid),
    receive {'DOWN', MRef, process, Pid, _} -> ok after 2000 -> erlang:error(timeout) end,
    ok = wait_inflight(S, 0, 50),
    stop(S).

wait_inflight(_S, _N, 0) ->
    erlang:error(inflight_not_released);
wait_inflight(S, N, Tries) ->
    #{replicas := [R]} = snapshot(S),
    case maps:get(inflight, R) of
        N -> ok;
        _ ->
            timer:sleep(20),
            wait_inflight(S, N, Tries - 1)
    end.

t_lease_expiry() ->
    {A, Clock} = fake_clock(),
    {ok, S} = start_link(#{
        replicas => [#{id => <<"x">>}],
        lease_ttl_ms => 500,
        failure_threshold => 1,
        clock => Clock,
        sweep_ms => infinity
    }),
    {ok, L, _, _} = acquire(S, <<"k">>),
    advance(A, 499),
    ok = sweep(S),
    #{leases := 1} = snapshot(S),
    advance(A, 1),
    ok = sweep(S),
    #{leases := 0, stats := Stats, replicas := [R]} = snapshot(S),
    1 = maps:get(expired, Stats),
    0 = maps:get(inflight, R),
    true = maps:get(health, R) =/= healthy,
    {error, unknown_lease} = release(S, L, ok),
    {error, no_healthy_replicas} = acquire(S, <<"k">>),
    stop(S).

t_drain() ->
    {ok, S} = start_link(#{
        replicas => [#{id => <<"x">>}, #{id => <<"y">>}],
        sweep_ms => infinity
    }),
    Key = <<"k">>,
    [Head, Other] = preview(S, Key),
    {ok, L, Head, 0} = acquire(S, Key),
    ok = drain(S, Head),
    [Other] = preview(S, Key),
    {ok, L2, Other, 0} = acquire(S, Key),
    ok = release(S, L2, ok),
    ok = release(S, L, ok),
    #{replicas := [Only]} = snapshot(S),
    Other = maps:get(id, Only),
    {error, unknown_replica} = drain(S, Head),
    stop(S).

t_validation() ->
    {ok, S} = start_link(#{sweep_ms => infinity}),
    {error, no_replicas} = acquire(S, <<"k">>),
    {error, {bad_option, weight, 0}} = add_replica(S, #{id => <<"a">>, weight => 0}),
    {error, {missing, id}} = add_replica(S, #{}),
    ok = add_replica(S, #{id => <<"a">>}),
    {error, already_present} = add_replica(S, #{id => <<"a">>}),
    ok = update_replica(S, <<"a">>, #{max_inflight => 1}),
    {ok, _, _, _} = acquire(S, <<"k">>),
    {error, overloaded} = acquire(S, <<"k">>),
    {error, unknown_replica} = update_replica(S, <<"zz">>, #{weight => 2}),
    stop(S).
