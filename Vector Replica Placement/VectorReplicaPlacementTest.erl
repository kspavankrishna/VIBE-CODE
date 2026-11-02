-module('VectorReplicaPlacementTest').
-include_lib("eunit/include/eunit.hrl").

node(Id, Zone, Cap) -> #{id => Id, zone => Zone, capacity => Cap}.
node(Id, Zone, Cap, Drain) -> #{id => Id, zone => Zone, capacity => Cap, draining => Drain}.
shard(Id, Size, R, Placed) -> #{id => Id, size => Size, replicas => R, placed => Placed}.

plan_ok(Nodes, Shards, Opts) ->
    {ok, R} = 'VectorReplicaPlacement':plan(#{nodes => Nodes, shards => Shards, options => Opts}),
    R.

final_holders(Shards, Steps) ->
    Start = maps:from_list([{maps:get(id, S), maps:get(placed, S)} || S <- Shards]),
    lists:foldl(fun(#{op := drop, shard := S, node := N}, A) -> A#{S := maps:get(S, A) -- [N]};
                   (#{shard := S, to := N}, A) -> A#{S := maps:get(S, A) ++ [N]}
                end, Start, Steps).

min_live_never_below(Shards, Steps, Floor) ->
    Start = maps:from_list([{maps:get(id, S), maps:get(placed, S)} || S <- Shards]),
    lists:all(fun(X) -> X end, element(2, lists:foldl(
        fun(Step, {A, Oks}) ->
            S = maps:get(shard, Step),
            A1 = case Step of
                     #{op := drop, node := N} -> A#{S := maps:get(S, A) -- [N]};
                     #{to := N} -> A#{S := maps:get(S, A) ++ [N]}
                 end,
            {A1, [length(maps:get(S, A1)) >= min(Floor, length(maps:get(S, A))) | Oks]}
        end, {Start, []}, Steps))).

drain_replaces_before_dropping_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000), node(c, z3, 1000, true)],
    Shards = [shard(s1, 100, 2, [b, c])],
    R = plan_ok(Nodes, Shards, #{}),
    Steps = maps:get(steps, R),
    ?assertMatch([#{op := copy, to := a}, #{op := drop, node := c}], Steps),
    ?assertEqual([], maps:get(unplaceable, R)).

drain_blocked_keeps_replica_test() ->
    Nodes = [node(a, z1, 1000), node(c, z3, 1000, true)],
    Shards = [shard(s1, 100, 2, [a, c])],
    R = plan_ok(Nodes, Shards, #{}),
    ?assertEqual([], maps:get(steps, R)),
    ?assert(lists:member({drain_blocked, {s1, c}}, maps:get(issues, R))),
    ?assertEqual(2, 'VectorReplicaPlacement':exit_code_for_test(R)).

lost_node_triggers_repair_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000), node(c, z3, 1000)],
    Shards = [shard(s1, 100, 2, [a, gone])],
    R = plan_ok(Nodes, Shards, #{}),
    ?assert(lists:member({lost_replica, s1, gone}, maps:get(issues, R))),
    Final = final_holders(Shards, maps:get(steps, R)),
    ?assertEqual(2, length(maps:get(s1, Final) -- [gone])).

total_loss_is_a_restore_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000)],
    R = plan_ok(Nodes, [shard(s1, 100, 2, [])], #{}),
    ?assertMatch([#{op := restore, from := backup}, #{op := restore, from := backup}],
                 maps:get(steps, R)).

zones_are_preferred_for_repair_test() ->
    Nodes = [node(a, z1, 1000), node(b, z1, 1000), node(c, z2, 1000)],
    R = plan_ok(Nodes, [shard(s1, 100, 2, [a])], #{}),
    ?assertMatch([#{op := copy, to := c}], maps:get(steps, R)).

zone_spread_moves_one_replica_test() ->
    Nodes = [node(a, z1, 1000), node(b, z1, 1000), node(c, z2, 1000)],
    Shards = [shard(s1, 100, 2, [a, b])],
    R = plan_ok(Nodes, Shards, #{tolerance => 1.0}),
    Final = final_holders(Shards, maps:get(steps, R)),
    ?assert(lists:member(c, maps:get(s1, Final))),
    ?assertEqual(2, length(maps:get(s1, Final))).

zone_spread_can_be_disabled_test() ->
    Nodes = [node(a, z1, 1000), node(b, z1, 1000), node(c, z2, 1000)],
    R = plan_ok(Nodes, [shard(s1, 100, 2, [a, b])], #{zone_spread => false, tolerance => 1.0}),
    ?assertEqual([], maps:get(steps, R)).

move_budget_is_respected_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000)],
    Shards = [shard(list_to_atom("s" ++ integer_to_list(I)), 100, 1, [a]) || I <- lists:seq(1, 6)],
    R = plan_ok(Nodes, Shards, #{max_moves => 2}),
    Copies = [S || S = #{op := Op} <- maps:get(steps, R), Op =/= drop],
    ?assertEqual(2, length(Copies)),
    ?assert(lists:member(move_budget_exhausted, maps:get(issues, R))).

byte_budget_is_respected_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000)],
    Shards = [shard(s1, 300, 1, [a]), shard(s2, 300, 1, [a]), shard(s3, 100, 1, [a])],
    R = plan_ok(Nodes, Shards, #{max_bytes => 350, tolerance => 0.0}),
    ?assert(maps:get(bytes, R) =< 350).

balance_reduces_spread_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000), node(c, z3, 1000)],
    Shards = [shard(list_to_atom("s" ++ integer_to_list(I)), 100, 1, [a]) || I <- lists:seq(1, 6)],
    R = plan_ok(Nodes, Shards, #{max_moves => infinity}),
    ?assert(maps:get(spread_after, R) < maps:get(spread_before, R)),
    ?assert(maps:get(spread_after, R) =< 0.2 + 1.0e-9).

balance_never_collapses_zones_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000), node(c, z1, 1000)],
    Shards = [shard(s1, 400, 2, [a, b]), shard(s2, 400, 2, [a, b])],
    R = plan_ok(Nodes, Shards, #{max_moves => infinity, tolerance => 0.0}),
    Final = final_holders(Shards, maps:get(steps, R)),
    Zone = fun(N) -> case N of b -> z2; _ -> z1 end end,
    [?assertEqual(2, length(lists:usort([Zone(N) || N <- Ns]))) || {_, Ns} <- maps:to_list(Final)].

excess_replicas_are_trimmed_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000), node(c, z3, 1000)],
    Shards = [shard(s1, 100, 1, [a, b, c])],
    R = plan_ok(Nodes, Shards, #{}),
    Final = final_holders(Shards, maps:get(steps, R)),
    ?assertEqual(1, length(maps:get(s1, Final))).

unplaceable_when_no_distinct_node_test() ->
    R = plan_ok([node(a, z1, 1000)], [shard(s1, 100, 2, [a])], #{}),
    ?assertEqual([{s1, no_distinct_node}], maps:get(unplaceable, R)).

repair_may_exceed_max_fill_but_not_capacity_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000)],
    Shards = [shard(s1, 900, 2, [a])],
    R = plan_ok(Nodes, Shards, #{}),
    ?assertMatch([#{op := copy, to := b}], maps:get(steps, R)),
    ?assert(lists:member({above_max_fill_to_repair, s1, b}, maps:get(issues, R))),
    R2 = plan_ok(Nodes, [shard(s1, 1100, 2, [a])], #{}),
    ?assertEqual([{s1, no_capacity}], maps:get(unplaceable, R2)).

copies_wait_for_space_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000, true), node(c, z3, 1000)],
    Shards = [shard(s1, 600, 1, [b]), shard(s2, 600, 1, [a])],
    R = plan_ok(Nodes, Shards, #{max_moves => infinity, tolerance => 0.0, max_fill => 1.0}),
    ?assertEqual([], maps:get(blocked, R)),
    ?assert(min_live_never_below(Shards, maps:get(steps, R), 1)).

plan_is_deterministic_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000), node(c, z3, 1000)],
    Shards = [shard(list_to_atom("s" ++ integer_to_list(I)), 50 + I, 2, [a, b]) || I <- lists:seq(1, 8)],
    ?assertEqual(plan_ok(Nodes, Shards, #{}), plan_ok(Nodes, Shards, #{})).

validation_reports_everything_test() ->
    {error, E} = 'VectorReplicaPlacement':plan(
        #{nodes => [node(a, z1, 0), node(a, z1, 10)],
          shards => [shard(s1, -1, 0, [])],
          options => #{max_fill => 2}}),
    ?assert(lists:member({bad_option, max_fill}, E)),
    ?assert(lists:member(duplicate_node_id, E)),
    ?assert(lists:member({node_bad_capacity, a}, E)),
    ?assert(lists:member({shard_bad_size, s1}, E)),
    ?assert(lists:member({shard_bad_replicas, s1}, E)).

duplicate_placed_entries_are_flagged_test() ->
    Nodes = [node(a, z1, 1000), node(b, z2, 1000)],
    R = plan_ok(Nodes, [shard(s1, 100, 1, [a, a])], #{}),
    ?assert(lists:member({duplicate_replica, s1, a}, maps:get(issues, R))).
