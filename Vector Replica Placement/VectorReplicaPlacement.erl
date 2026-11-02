-module('VectorReplicaPlacement').
-export([main/1, plan/1, plan/2, render/1, default_options/0, exit_code_for_test/1]).

default_options() ->
    #{max_fill => 0.85,
      tolerance => 0.05,
      max_moves => 25,
      max_bytes => infinity,
      zone_spread => true}.

-define(EPS, 1.0e-9).


main(Args) ->
    case parse_args(Args, #{}, undefined) of
        {ok, File, Over, PlanOut} ->
            run(File, Over, PlanOut);
        {error, Why} ->
            io:format(standard_error, "error: ~s~n", [Why]),
            usage(),
            halt(1)
    end.

usage() ->
    io:format(standard_error,
              "usage: escript VectorReplicaPlacement.erl CLUSTER.terms"
              " [--max-moves N] [--max-bytes N] [--max-fill F] [--tolerance F]"
              " [--no-zone-spread] [--plan-out FILE]~n", []).

parse_args([], _Over, undefined) -> {error, "missing cluster file"};
parse_args([], Over, File) -> {ok, File, Over, undefined};
parse_args(Args, Over, File) -> parse_args2(Args, Over, File, undefined).

parse_args2([], Over, File, Out) ->
    case File of
        undefined -> {error, "missing cluster file"};
        _ -> {ok, File, Over, Out}
    end;
parse_args2(["--max-moves", V | T], O, F, P) ->
    num_opt(max_moves, V, int, O, T, F, P);
parse_args2(["--max-bytes", V | T], O, F, P) ->
    num_opt(max_bytes, V, int, O, T, F, P);
parse_args2(["--max-fill", V | T], O, F, P) ->
    num_opt(max_fill, V, float, O, T, F, P);
parse_args2(["--tolerance", V | T], O, F, P) ->
    num_opt(tolerance, V, float, O, T, F, P);
parse_args2(["--no-zone-spread" | T], O, F, P) ->
    parse_args2(T, O#{zone_spread => false}, F, P);
parse_args2(["--plan-out", V | T], O, F, _P) ->
    parse_args2(T, O, F, V);
parse_args2(["--" ++ Flag | _], _, _, _) ->
    {error, "unknown or incomplete flag --" ++ Flag};
parse_args2([Path | T], O, undefined, P) ->
    parse_args2(T, O, Path, P);
parse_args2([Extra | _], _, _, _) ->
    {error, "unexpected argument " ++ Extra}.

num_opt(Key, Text, Kind, O, T, F, P) ->
    case parse_number(Text, Kind) of
        {ok, N} -> parse_args2(T, O#{Key => N}, F, P);
        error -> {error, io_lib:format("bad value ~s for ~p", [Text, Key])}
    end.

parse_number(Text, int) ->
    try {ok, list_to_integer(Text)} catch _:_ -> error end;
parse_number(Text, float) ->
    try {ok, list_to_float(Text)}
    catch _:_ ->
        try {ok, float(list_to_integer(Text))} catch _:_ -> error end
    end.

run(File, Over, PlanOut) ->
    case file:consult(File) of
        {ok, [Input]} ->
            case plan(Input, Over) of
                {ok, Result} ->
                    io:put_chars(render(Result)),
                    write_plan(PlanOut, Result),
                    halt(exit_code(Result));
                {error, Reasons} ->
                    [io:format(standard_error, "invalid input: ~p~n", [R])
                     || R <- Reasons],
                    halt(1)
            end;
        {ok, Other} ->
            io:format(standard_error,
                      "error: expected exactly one term, found ~p~n",
                      [length(Other)]),
            halt(1);
        {error, Reason} ->
            io:format(standard_error, "error: cannot read ~s: ~p~n",
                      [File, Reason]),
            halt(1)
    end.

write_plan(undefined, _) -> ok;
write_plan(Path, #{steps := Steps}) ->
    Body = [io_lib:format("~p.~n", [S]) || S <- Steps],
    ok = file:write_file(Path, Body).

exit_code_for_test(R) -> exit_code(R).

exit_code(#{unplaceable := U, blocked := B, issues := I}) ->
    Hard = [X || X <- I, hard_issue(X)],
    case U =:= [] andalso B =:= [] andalso Hard =:= [] of
        true -> 0;
        false -> 2
    end.

hard_issue({overfull_node, _, _}) -> true;
hard_issue({drain_blocked, _}) -> true;
hard_issue(_) -> false.


plan(Input) -> plan(Input, #{}).

plan(Input, Over) when is_map(Input), is_map(Over) ->
    Opts = maps:merge(maps:merge(default_options(),
                                 maps:get(options, Input, #{})), Over),
    Errors = validate_options(Opts)
        ++ validate_nodes(maps:get(nodes, Input, undefined))
        ++ validate_shards(maps:get(shards, Input, undefined)),
    case Errors of
        [] -> {ok, solve(Opts, maps:get(nodes, Input),
                         maps:get(shards, Input))};
        _ -> {error, Errors}
    end;
plan(_, _) ->
    {error, [input_must_be_a_map]}.

validate_options(O) ->
    chk(is_number(maps:get(max_fill, O))
        andalso maps:get(max_fill, O) > 0
        andalso maps:get(max_fill, O) =< 1, {bad_option, max_fill})
    ++ chk(is_number(maps:get(tolerance, O))
           andalso maps:get(tolerance, O) >= 0, {bad_option, tolerance})
    ++ chk(is_count_or_inf(maps:get(max_moves, O)), {bad_option, max_moves})
    ++ chk(is_count_or_inf(maps:get(max_bytes, O)), {bad_option, max_bytes})
    ++ chk(is_boolean(maps:get(zone_spread, O)), {bad_option, zone_spread}).

is_count_or_inf(infinity) -> true;
is_count_or_inf(N) -> is_integer(N) andalso N >= 0.

chk(true, _) -> [];
chk(false, E) -> [E].

validate_nodes(L) when is_list(L), L =/= [] ->
    Ids = [maps:get(id, N, undefined) || N <- L, is_map(N)],
    chk(length(Ids) =:= length(L), node_entry_not_a_map)
    ++ chk(length(lists:usort(Ids)) =:= length(Ids), duplicate_node_id)
    ++ lists:append([validate_node(N) || N <- L, is_map(N)]);
validate_nodes(_) ->
    [nodes_must_be_a_non_empty_list].

validate_node(N) ->
    Id = maps:get(id, N, undefined),
    chk(Id =/= undefined, node_missing_id)
    ++ chk(maps:is_key(zone, N), {node_missing_zone, Id})
    ++ chk(is_integer(maps:get(capacity, N, undefined))
           andalso maps:get(capacity, N) > 0, {node_bad_capacity, Id})
    ++ chk(is_boolean(maps:get(draining, N, false)), {node_bad_draining, Id}).

validate_shards(L) when is_list(L) ->
    Ids = [maps:get(id, S, undefined) || S <- L, is_map(S)],
    chk(length(Ids) =:= length(L), shard_entry_not_a_map)
    ++ chk(length(lists:usort(Ids)) =:= length(Ids), duplicate_shard_id)
    ++ lists:append([validate_shard(S) || S <- L, is_map(S)]);
validate_shards(_) ->
    [shards_must_be_a_list].

validate_shard(S) ->
    Id = maps:get(id, S, undefined),
    chk(Id =/= undefined, shard_missing_id)
    ++ chk(is_integer(maps:get(size, S, undefined))
           andalso maps:get(size, S) >= 0, {shard_bad_size, Id})
    ++ chk(is_integer(maps:get(replicas, S, undefined))
           andalso maps:get(replicas, S) >= 1, {shard_bad_replicas, Id})
    ++ chk(is_list(maps:get(placed, S, [])), {shard_bad_placed, Id}).


solve(Opts, NodeList, ShardList) ->
    Caps = maps:from_list([{maps:get(id, N), maps:get(capacity, N)}
                           || N <- NodeList]),
    Zones = maps:from_list([{maps:get(id, N), maps:get(zone, N)}
                            || N <- NodeList]),
    Drain = maps:from_list([{maps:get(id, N), maps:get(draining, N, false)}
                            || N <- NodeList]),
    Sizes = maps:from_list([{maps:get(id, S), maps:get(size, S)}
                            || S <- ShardList]),
    Want = maps:from_list([{maps:get(id, S), maps:get(replicas, S)}
                           || S <- ShardList]),
    {Orig, Warn0} = load_placement(ShardList, Caps),
    Base = #{opts => Opts, caps => Caps, zones => Zones, drain => Drain,
             sizes => Sizes, want => Want, moves => 0, bytes => 0,
             reasons => #{}, issues => [], unplaceable => [],
             restore => []},
    Keep = maps:map(fun(_S, Ns) ->
                            [N || N <- Ns, not maps:get(N, Drain)]
                    end, Orig),
    S0 = Base#{target => Keep, usage => usage_of(Keep, Sizes)},
    S1 = trim(S0),
    S2 = repair(S1, Orig),
    S3 = retain_for_drain(S2, Orig),
    S4 = case maps:get(zone_spread, Opts) of
             true -> zone_fix(S3);
             false -> S3
         end,
    S5 = balance(S4),
    Final = maps:get(target, S5),
    Before = usage_of(Orig, Sizes),
    After = maps:get(usage, S5),
    {Steps, Blocked} = schedule(Orig, Final, S5),
    Active = active_nodes(S5),
    #{steps => Steps,
      blocked => Blocked,
      unplaceable => lists:reverse(maps:get(unplaceable, S5)),
      issues => Warn0 ++ lists:reverse(maps:get(issues, S5))
                ++ final_issues(S5),
      nodes => [#{id => N,
                  zone => maps:get(N, Zones),
                  draining => maps:get(N, Drain),
                  capacity => maps:get(N, Caps),
                  before => maps:get(N, Before, 0),
                  'after' => maps:get(N, After, 0)}
                || N <- lists:sort(maps:keys(Caps))],
      spread_before => spread(Before, Active, Caps),
      spread_after => spread(After, Active, Caps),
      moves => maps:get(moves, S5),
      bytes => maps:get(bytes, S5),
      options => Opts}.

load_placement(ShardList, Caps) ->
    lists:foldl(
      fun(S, {Acc, Warn}) ->
              Id = maps:get(id, S),
              Raw = maps:get(placed, S, []),
              Uniq = dedupe(Raw),
              Dups = [{duplicate_replica, Id, N}
                      || N <- lists:usort(Raw -- Uniq)],
              {Known, Gone} = lists:partition(
                                fun(N) -> maps:is_key(N, Caps) end, Uniq),
              Lost = [{lost_replica, Id, N} || N <- Gone],
              {Acc#{Id => Known}, Warn ++ Dups ++ Lost}
      end, {#{}, []}, ShardList).

dedupe(L) -> dedupe(L, []).
dedupe([], Acc) -> lists:reverse(Acc);
dedupe([H | T], Acc) ->
    case lists:member(H, Acc) of
        true -> dedupe(T, Acc);
        false -> dedupe(T, [H | Acc])
    end.

usage_of(Placement, Sizes) ->
    maps:fold(fun(S, Ns, Acc) ->
                      Size = maps:get(S, Sizes),
                      lists:foldl(fun(N, A) ->
                                          maps:update_with(
                                            N, fun(V) -> V + Size end,
                                            Size, A)
                                  end, Acc, Ns)
              end, #{}, Placement).

active_nodes(#{caps := Caps, drain := Drain}) ->
    lists:sort([N || N <- maps:keys(Caps), not maps:get(N, Drain)]).

fill(N, #{usage := U, caps := C}) ->
    maps:get(N, U, 0) / maps:get(N, C).

fits(N, Size, MaxFill, #{usage := U, caps := C}) ->
    maps:get(N, U, 0) + Size =< maps:get(N, C) * MaxFill + ?EPS.

fits_physical(N, Size, #{usage := U, caps := C}) ->
    maps:get(N, U, 0) + Size =< maps:get(N, C).

zones_of(Ns, #{zones := Z}) ->
    lists:usort([maps:get(N, Z) || N <- Ns]).

zone_goal(R, St) ->
    Active = active_nodes(St),
    min(R, length(zones_of(Active, St))).

add_issue(I, St = #{issues := L}) -> St#{issues := [I | L]}.

put_target(S, Ns, St = #{target := T}) -> St#{target := T#{S => Ns}}.

adjust_usage(N, Delta, St = #{usage := U}) ->
    St#{usage := U#{N => maps:get(N, U, 0) + Delta}}.

budget_ok(Size, #{moves := M, bytes := B, opts := O}) ->
    MaxM = maps:get(max_moves, O),
    MaxB = maps:get(max_bytes, O),
    (MaxM =:= infinity orelse M + 1 =< MaxM)
    andalso (MaxB =:= infinity orelse B + Size =< MaxB).

spend(Size, St = #{moves := M, bytes := B}) ->
    St#{moves := M + 1, bytes := B + Size}.

reason(S, N, Why, St = #{reasons := R}) -> St#{reasons := R#{{S, N} => Why}}.

ordered_shards(St = #{sizes := Sizes}) ->
    _ = St,
    Pairs = [{-Size, S} || {S, Size} <- maps:to_list(Sizes)],
    [S || {_, S} <- lists:sort(Pairs)].


trim(St) ->
    lists:foldl(fun(S, A) -> trim_shard(S, A) end, St, ordered_shards(St)).

trim_shard(S, St = #{target := T, want := W, sizes := Sz}) ->
    Ns = maps:get(S, T),
    R = maps:get(S, W),
    case length(Ns) > R of
        false -> St;
        true ->
            Zs = [maps:get(N, maps:get(zones, St)) || N <- Ns],
            Score = fun(N) ->
                            Z = maps:get(N, maps:get(zones, St)),
                            Dup = length([x || Z2 <- Zs, Z2 =:= Z]) > 1,
                            {case Dup of true -> 0; false -> 1 end,
                             -fill(N, St), N}
                    end,
            Victim = element(3, lists:min([Score(N) || N <- Ns])),
            St1 = adjust_usage(Victim, -maps:get(S, Sz), St),
            trim_shard(S, put_target(S, Ns -- [Victim], St1))
    end.


repair(St, Orig) ->
    lists:foldl(fun(S, Acc) -> repair_shard(S, Orig, Acc) end,
                St, repair_order(St, Orig)).

repair_order(St = #{target := T, want := W}, _Orig) ->
    Short = [S || S <- ordered_shards(St),
                  length(maps:get(S, T)) < maps:get(S, W)],
    Short.

repair_shard(S, Orig, St = #{target := T, want := W, sizes := Sz,
                              opts := O}) ->
    Ns = maps:get(S, T),
    case length(Ns) >= maps:get(S, W) of
        true -> St;
        false ->
            Size = maps:get(S, Sz),
            Max = maps:get(max_fill, O),
            Cands = candidates(Ns, St),
            Strict = [N || N <- Cands, fits(N, Size, Max, St)],
            {Pick, Tight} =
                case Strict of
                    [] -> {[N || N <- Cands, fits_physical(N, Size, St)],
                           true};
                    _ -> {Strict, false}
                end,
            case Pick of
                [] ->
                    Why = case Cands of
                              [] -> no_distinct_node;
                              _ -> no_capacity
                          end,
                    St1 = St#{unplaceable :=
                                  [{S, Why} | maps:get(unplaceable, St)]},
                    St1;
                _ ->
                    Used = zones_of(Ns, St),
                    Zs = maps:get(zones, St),
                    Key = fun(N) ->
                                  Fresh = case lists:member(maps:get(N, Zs),
                                                            Used) of
                                              true -> 1;
                                              false -> 0
                                          end,
                                  NewFill = (maps:get(N, maps:get(usage, St),
                                                      0) + Size)
                                            / maps:get(N, maps:get(caps, St)),
                                  {Fresh, NewFill, N}
                          end,
                    {_, _, Best} = lists:min([Key(N) || N <- Pick]),
                    St1 = adjust_usage(Best, Size, St),
                    St2 = put_target(S, Ns ++ [Best], St1),
                    Kind = case maps:get(S, Orig) of
                               [] -> restore;
                               _ -> repair
                           end,
                    St3 = reason(S, Best, Kind, St2),
                    St4 = case Kind of
                              restore -> St3#{restore :=
                                                  [S | maps:get(restore, St3)]};
                              _ -> St3
                          end,
                    St5 = case Tight of
                              true -> add_issue({above_max_fill_to_repair,
                                                 S, Best}, St4);
                              false -> St4
                          end,
                    repair_shard(S, Orig, St5)
            end
    end.

candidates(Held, St = #{caps := Caps}) ->
    [N || N <- lists:sort(maps:keys(Caps)),
          not maps:get(N, maps:get(drain, St)),
          not lists:member(N, Held)].


retain_for_drain(St, Orig) ->
    lists:foldl(
      fun(S, Acc = #{target := T, want := W, sizes := Sz}) ->
              Ns = maps:get(S, T),
              Need = maps:get(S, W) - length(Ns),
              Stuck = [N || N <- maps:get(S, Orig), not lists:member(N, Ns),
                            maps:get(N, maps:get(drain, Acc))],
              case Need > 0 of
                  false -> Acc;
                  true ->
                      Keep = lists:sublist(Stuck, Need),
                      Size = maps:get(S, Sz),
                      Acc1 = lists:foldl(
                               fun(N, A) -> adjust_usage(N, Size, A) end,
                               Acc, Keep),
                      Acc2 = put_target(S, Ns ++ Keep, Acc1),
                      lists:foldl(
                        fun(N, A) -> add_issue({drain_blocked, {S, N}}, A) end,
                        Acc2, Keep)
              end
      end, St, ordered_shards(St)).


zone_fix(St) ->
    lists:foldl(fun(S, A) -> zone_fix_shard(S, A) end, St, ordered_shards(St)).

zone_fix_shard(S, St = #{target := T, want := W, sizes := Sz, opts := O}) ->
    Ns = maps:get(S, T),
    Goal = zone_goal(maps:get(S, W), St),
    case length(Ns) =:= maps:get(S, W) andalso
         length(zones_of(Ns, St)) < Goal of
        false -> St;
        true ->
            Size = maps:get(S, Sz),
            Zs = maps:get(zones, St),
            Have = zones_of(Ns, St),
            Dupes = [N || N <- Ns,
                          length([x || M <- Ns,
                                       maps:get(M, Zs) =:= maps:get(N, Zs)]) > 1],
            Targets = [N || N <- candidates(Ns, St),
                            not lists:member(maps:get(N, Zs), Have),
                            fits(N, Size, maps:get(max_fill, O), St)],
            case {Dupes, Targets} of
                {[], _} -> St;
                {_, []} -> add_issue({zone_spread_unmet, S}, St);
                _ ->
                    case budget_ok(Size, St) of
                        false -> add_issue({zone_spread_unmet, S}, St);
                        true ->
                            From = element(2, lists:max(
                                                [{fill(N, St), N}
                                                 || N <- Dupes])),
                            To = element(2, lists:min(
                                              [{fill(N, St), N}
                                               || N <- Targets])),
                            St1 = do_move(S, From, To, zone_spread, St),
                            zone_fix_shard(S, St1)
                    end
            end
    end.

do_move(S, From, To, Why, St = #{target := T, sizes := Sz}) ->
    Size = maps:get(S, Sz),
    Ns = (maps:get(S, T) -- [From]) ++ [To],
    St1 = adjust_usage(From, -Size, St),
    St2 = adjust_usage(To, Size, St1),
    St3 = put_target(S, Ns, St2),
    St4 = spend(Size, St3),
    Carry = case maps:get({S, From}, maps:get(reasons, St4), Why) of
                repair -> repair;
                restore -> restore;
                _ -> Why
            end,
    reason(S, To, Carry, St4).


balance(St) ->
    Active = active_nodes(St),
    Fills = lists:reverse(lists:sort([{fill(N, St), N} || N <- Active])),
    case Fills of
        [] -> St;
        _ ->
            {Hi, _} = hd(Fills),
            {Lo, _} = lists:last(Fills),
            Max = maps:get(max_fill, maps:get(opts, St)),
            Tol = maps:get(tolerance, maps:get(opts, St)),
            case Hi - Lo =< Tol + ?EPS andalso Hi =< Max + ?EPS of
                true -> St;
                false -> balance_step(Fills, St)
            end
    end.

balance_step(Fills, St) ->
    case first_move(Fills, Fills, St) of
        none ->
            add_issue(balance_stalled, St);
        {ok, {_Key, S, From, To}} ->
            Size = maps:get(S, maps:get(sizes, St)),
            case budget_ok(Size, St) of
                false -> add_issue(move_budget_exhausted, St);
                true -> balance(do_move(S, From, To, balance, St))
            end
    end.

first_move([], _All, _St) -> none;
first_move([{HiFill, Hot} | Rest], All, St) ->
    T = maps:get(target, St),
    Sz = maps:get(sizes, St),
    Zs = maps:get(zones, St),
    Max = maps:get(max_fill, maps:get(opts, St)),
    Cap = maps:get(Hot, maps:get(caps, St)),
    OnHot = [S || {S, Ns} <- maps:to_list(T), lists:member(Hot, Ns)],
    Colds = [N || {F, N} <- All, N =/= Hot, F < HiFill - ?EPS],
    Options =
        [{{max(HiFill - Size / Cap,
              (maps:get(N, maps:get(usage, St), 0) + Size)
              / maps:get(N, maps:get(caps, St))), Size, S, N},
          S, Hot, N}
         || S <- OnHot,
            Size <- [maps:get(S, Sz)],
            Size > 0,
            N <- Colds,
            not lists:member(N, maps:get(S, T)),
            fits(N, Size, Max, St),
            zone_safe(maps:get(S, T), Hot, N, Zs)],
    Improving = [O || {{NewMax, _, _, _}, _, _, _} = O <- Options,
                      NewMax < HiFill - ?EPS],
    case Improving of
        [] -> first_move(Rest, All, St);
        _ -> {ok, lists:min(Improving)}
    end.

zone_safe(Ns, From, To, Zs) ->
    Before = lists:usort([maps:get(N, Zs) || N <- Ns]),
    After = lists:usort([maps:get(N, Zs) || N <- (Ns -- [From]) ++ [To]]),
    length(After) >= length(Before).

final_issues(St = #{caps := Caps, opts := O}) ->
    Max = maps:get(max_fill, O),
    [{overfull_node, N, fill(N, St)}
     || N <- lists:sort(maps:keys(Caps)),
        not maps:get(N, maps:get(drain, St)),
        fill(N, St) > Max + ?EPS].

spread(Usage, Active, Caps) ->
    case Active of
        [] -> 0.0;
        _ ->
            Fs = [maps:get(N, Usage, 0) / maps:get(N, Caps) || N <- Active],
            lists:max(Fs) - lists:min(Fs)
    end.


schedule(Orig, Final, St = #{sizes := Sz, caps := Caps, reasons := Rs,
                             target := _}) ->
    Phys0 = usage_of(Orig, Sz),
    Items = [item(S, Orig, Final, Rs) || S <- schedule_order(Orig, Final, St)],
    Live = [I || I = {_, Cs, Ds} <- Items, Cs =/= [] orelse Ds =/= []],
    run_passes(Live, Phys0, Caps, Sz, [], St).

schedule_order(Orig, Final, St) ->
    Changed = [S || S <- ordered_shards(St),
                    maps:get(S, Orig) -- maps:get(S, Final) =/= []
                    orelse maps:get(S, Final) -- maps:get(S, Orig) =/= []],
    Short = [S || S <- Changed,
                  length(maps:get(S, Orig)) < maps:get(S, maps:get(want, St))],
    Short ++ (Changed -- Short).

item(S, Orig, Final, Rs) ->
    O = maps:get(S, Orig),
    F = maps:get(S, Final),
    Adds = F -- O,
    Drops = O -- F,
    Staying = [N || N <- O, lists:member(N, F)],
    Copies = [begin
                  Src = case {Staying, O} of
                            {[X | _], _} -> X;
                            {[], [Y | _]} -> Y;
                            {[], []} -> backup
                        end,
                  Op = case O of [] -> restore; _ -> copy end,
                  {Op, S, Src, Dst, maps:get({S, Dst}, Rs, balance)}
              end || Dst <- Adds],
    {S, Copies, Drops}.

run_passes([], _Phys, _Caps, _Sz, Out, _St) ->
    {lists:reverse(Out), []};
run_passes(Items, Phys, Caps, Sz, Out, St) ->
    {Items1, Phys1, Out1, Progress} =
        lists:foldl(
          fun(Item, {Keep, P, O, Prog}) ->
                  {S, Copies, Drops} = Item,
                  Size = maps:get(S, Sz),
                  {Done, Left, P1} = take_copies(Copies, Size, P, Caps),
                  CopyOut = [copy_step(C, Size) || C <- Done],
                  case Left of
                      [] ->
                          DropOut = [#{op => drop, shard => S, node => N,
                                       bytes => Size} || N <- Drops],
                          P2 = lists:foldl(
                                 fun(N, A) -> A#{N => maps:get(N, A) - Size} end,
                                 P1, Drops),
                          {Keep, P2, lists:reverse(CopyOut ++ DropOut) ++ O,
                           Prog orelse Done =/= [] orelse Drops =/= []};
                      _ ->
                          {[{S, Left, Drops} | Keep], P1,
                           lists:reverse(CopyOut) ++ O,
                           Prog orelse Done =/= []}
                  end
          end, {[], Phys, Out, false}, Items),
    case {Items1, Progress} of
        {[], _} -> {lists:reverse(Out1), []};
        {_, false} ->
            Blocked = [{S, [{Dst, Why} || {_, _, _, Dst, Why} <- Cs]}
                       || {S, Cs, _} <- lists:reverse(Items1)],
            {lists:reverse(Out1), Blocked};
        {_, true} ->
            run_passes(lists:reverse(Items1), Phys1, Caps, Sz, Out1, St)
    end.

take_copies(Copies, Size, Phys, Caps) ->
    lists:foldl(
      fun(C = {_, _, _, Dst, _}, {Done, Left, P}) ->
              Cur = maps:get(Dst, P, 0),
              case Cur + Size =< maps:get(Dst, Caps) of
                  true -> {Done ++ [C], Left, P#{Dst => Cur + Size}};
                  false -> {Done, Left ++ [C], P}
              end
      end, {[], [], Phys}, Copies).

copy_step({Op, S, Src, Dst, Why}, Size) ->
    #{op => Op, shard => S, from => Src, to => Dst, bytes => Size,
      reason => Why}.


render(R = #{steps := Steps, nodes := Nodes}) ->
    Lines =
        [io_lib:format("replica placement plan~n", []),
         io_lib:format("  steps: ~p  copies: ~p  moved bytes: ~p~n",
                       [length(Steps), length([x || #{op := Op} <- Steps, Op =/= drop]),
                        copy_bytes(Steps)]),
         io_lib:format("  fill spread: ~s -> ~s~n",
                       [pct(maps:get(spread_before, R)),
                        pct(maps:get(spread_after, R))]),
         io_lib:format("~nnodes~n", [])]
        ++ [node_line(N) || N <- Nodes]
        ++ [io_lib:format("~nsteps~n", [])]
        ++ [step_line(I, S) || {I, S} <- lists:zip(lists:seq(1, length(Steps)),
                                                   Steps)]
        ++ section("unplaceable", maps:get(unplaceable, R))
        ++ section("blocked by transient capacity", maps:get(blocked, R))
        ++ section("issues", maps:get(issues, R)),
    lists:flatten(Lines).

copy_bytes(Steps) ->
    lists:sum([B || #{op := Op, bytes := B} <- Steps, Op =/= drop]).

pct(F) -> io_lib:format("~.1f%", [F * 100]).

node_line(#{id := Id, zone := Z, draining := D, capacity := C,
            before := B, 'after' := A}) ->
    io_lib:format("  ~p zone=~p~s before=~s after=~s~n",
                  [Id, Z, case D of true -> " draining"; false -> "" end,
                   pct(B / C), pct(A / C)]).

step_line(I, #{op := drop, shard := S, node := N, bytes := B}) ->
    io_lib:format("  ~3..0w drop    ~p on ~p (~p bytes)~n", [I, S, N, B]);
step_line(I, #{op := Op, shard := S, from := F, to := T, bytes := B,
               reason := Why}) ->
    io_lib:format("  ~3..0w ~-7s ~p ~p -> ~p (~p bytes) because ~p~n",
                  [I, atom_to_list(Op), S, F, T, B, Why]).

section(_Title, []) -> [];
section(Title, Items) ->
    [io_lib:format("~n~s~n", [Title])
     | [io_lib:format("  ~p~n", [X]) || X <- Items]].
