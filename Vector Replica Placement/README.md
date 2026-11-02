# Vector Replica Placement

A single file Erlang planner that decides where vector index shard replicas should live, then hands you an ordered list of copy and drop steps that never leave a shard with fewer copies than you asked for. It is built for the day a node dies, a zone drains or your cluster has quietly drifted into a lopsided mess.

**Language:** Erlang | **Lines:** 680 | **Added:** 2026-11-02

## What this solves

Every vector database, search cluster and embedding store eventually has the same chore: shards live on nodes, each shard has a replica count and something changed. A node was lost. A node is being drained for a kernel upgrade. Someone added capacity and the new boxes are empty while the old ones sit at 90 percent. The built in rebalancers in most systems either do too much (they shuffle terabytes to chase a perfect average) or too little (they only react after a disk fills).

The failure modes are well known and they still bite:

- A rebalance moves a replica off a draining node before its replacement has finished copying, so for a while you run one copy short.
- Two replicas of a shard end up in the same availability zone because the balancer only looked at disk usage.
- A repair puts a replica on a node that is already above its safe fill level and the next merge or compaction runs it out of disk.
- A plan looks fine on paper but step 4 needs space that only step 9 frees, so it deadlocks halfway through.
- Nobody can say how many bytes a plan will move before it runs, so it gets kicked off during peak traffic.

Vector Replica Placement plans all of this in one pass, offline, from a plain description of the cluster. It does not touch your cluster. It prints what it would do and why. It also tells you plainly when something cannot be done.

## Why I built it

I kept seeing the same pattern in post mortems for embedding stores and search clusters. The data was fine. The replicas were fine. The plan was the problem. Somebody wrote a shell loop or a quick script that said move shard X from node A to node B, and it ran in the wrong order, ignored zones or blew the byte budget for the night.

Erlang suits this. The language was designed around machines that fail and keep running, so its data structures (maps, lists, pattern matching) make a planner like this short and easy to read. I also wanted something with no dependencies: no rebar, no Hex packages, nothing but a stock OTP install. You can copy the file onto a bastion host and run it with `escript`.

The other reason is that I wanted the hard cases written down as code. Total shard loss, replicas pointing at nodes that no longer exist, a drain that cannot finish and capacity that only frees up midway are all handled and all covered by tests.

## When to use it

Use it when you operate any sharded and replicated store and you need a reviewable plan before you touch production. Typical moments:

- A node or zone was lost and you want repair copies placed with zone spread.
- You are draining nodes for maintenance and want proof that no shard dips below its replica count.
- You added nodes and want the minimum number of moves that evens out fill ratios, capped by a move count or a byte budget.
- You are doing capacity planning and want to know whether the cluster can even absorb a drain (the planner reports `no_capacity` and `no_distinct_node` for shards that cannot be placed).
- You want a deterministic plan to attach to a change ticket. Same input, same output, every time.

Do not use it as a live controller. It plans. Your own tooling executes the steps and re-runs the planner against fresh state afterwards.

## How it works

The entry point is `main/1` for the command line and `plan/1` or `plan/2` for use as a library. Both end in `solve/3`, which runs a fixed pipeline over an immutable state map.

**Input checks.** `plan/2` merges `default_options/0` with the options in the input and any overrides, then runs `validate_options/1`, `validate_nodes/1` and `validate_shards/1`. All errors are collected and returned together as `{error, Reasons}` so you fix the file once. Duplicate ids, non positive capacity, bad replica counts and out of range fill limits are all caught here.

**Loading the current placement.** `load_placement/2` drops replicas that point at nodes missing from the node list and reports them as `{lost_replica, Shard, Node}`. It also removes duplicates and reports `{duplicate_replica, Shard, Node}`. Replicas on draining nodes are not counted in the target layout, which is what forces the evacuation.

**Trim.** `trim/1` removes surplus replicas when a shard holds more copies than requested. It prefers to drop a copy that shares a zone with another copy and then the copy on the fullest node.

**Repair.** `repair/2` handles shards below their replica count, largest first. For each missing copy it ranks candidate nodes by a key of whether the zone is already used by this shard, the fill ratio after placement and the node id as a tiebreak. Nodes must be below `max_fill` first. If none qualifies the planner falls back to physical capacity and records `{above_max_fill_to_repair, Shard, Node}`, because running a little hot beats running short of copies. If a shard has no surviving replica at all the step is emitted as `restore` from `backup` rather than `copy`.

**Drain safety.** `retain_for_drain/2` keeps a draining replica online when no replacement could be placed and records `{drain_blocked, {Shard, Node}}`. The planner will not plan a drop that takes a shard under its replica count.

**Zone spread.** `zone_fix/1` and `zone_fix_shard/2` look for shards whose copies share a zone when more zones are available than copies are in use. They move a copy from the fullest node in a duplicated zone to the emptiest node in a fresh zone. Each move spends from the budget.

**Balance.** `balance/1` and `first_move/3` compute a fill ratio for each live node, then search from the hottest node down for the move that most reduces the larger of the two fill ratios involved. A move only counts if it strictly improves on the hot node, which guarantees termination. `zone_safe/4` refuses any move that would reduce a shard's zone diversity. The loop stops when the gap between hottest and coldest is within `tolerance` and nobody exceeds `max_fill`, or when `budget_ok/2` says the move count or byte budget is spent. A stalled search is reported as `balance_stalled`.

**Safe ordering.** `schedule/3` and `run_passes/6` turn the final layout into steps. Copies for a shard always precede its drops. Each copy is checked against the real physical capacity of the destination at that moment, using a running usage table. If a copy does not fit yet the shard waits and other shards go first, since their drops may free the space. Whatever never fits ends up under `blocked`, with the destination and reason.

**Report.** `render/1` prints node fills before and after, the ordered steps with reasons (`repair`, `restore`, `zone_spread`, `balance`) and any `unplaceable`, `blocked` or `issues` sections.

## Usage

Describe the cluster as one Erlang term in a file. Capacities and sizes are integers in whatever unit you like, as long as it is the same unit throughout.

```erlang
#{options => #{max_moves => 10, max_fill => 0.85},
  nodes => [#{id => n1, zone => a, capacity => 1000},
            #{id => n4, zone => b, capacity => 1000, draining => true},
            #{id => n5, zone => c, capacity => 1000}],
  shards => [#{id => s1, size => 300, replicas => 2, placed => [n1, n4]}]}.
```

Run it with a stock OTP install:

```
escript VectorReplicaPlacement.erl ClusterExample.terms
escript VectorReplicaPlacement.erl ClusterExample.terms --max-moves 40 --max-bytes 500000 --plan-out plan.terms
```

Flags: `--max-moves N`, `--max-bytes N`, `--max-fill F`, `--tolerance F`, `--no-zone-spread` and `--plan-out FILE`. Flags override the options in the file. The plan file holds one step map per line as an Erlang term, ready for your own executor to read with `file:consult/1`.

Exit codes: 0 when the plan is clean, 1 for bad input, 2 when something is unplaceable or blocked or when a drain cannot finish or a node stays over `max_fill`.

From Erlang code:

```erlang
{ok, Result} = 'VectorReplicaPlacement':plan(Input, #{max_moves => 5}),
io:put_chars('VectorReplicaPlacement':render(Result)).
```

Run the tests with:

```
erlc VectorReplicaPlacement.erl VectorReplicaPlacementTest.erl
erl -noshell -eval "eunit:test('VectorReplicaPlacementTest', []), halt()."
```

## Notes

- The planner is offline and pure. It never talks to a cluster and it never writes anything except the optional plan file.
- Output is deterministic. Ties break on node and shard ids, so two runs on the same input match byte for byte. A test checks this.
- Move cost is counted as copies and bytes. Repair and restore copies are mandatory and are not charged to the budget. Zone and balance moves are charged.
- It balances fill ratio, not query load. If one shard is hot, weight its size upward in the input.
- Zones are treated as flat labels. If you have racks inside zones, encode the rack as the zone and set replicas accordingly.
- Greedy balancing is not globally optimal. It is chosen because it gives small, explainable plans that stop early. Re-run after each batch of steps lands.
- Source files: `VectorReplicaPlacement.erl` (planner), `VectorReplicaPlacementTest.erl` (18 eunit tests) and `ClusterExample.terms` (a sample cluster with a draining node, a lost node and an empty shard).
