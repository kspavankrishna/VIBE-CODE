# PDB Drain Planner

A Kubernetes node drain simulator in Ada that replays the real eviction API rules for PodDisruptionBudgets against a snapshot of your cluster. It tells you before the upgrade window which nodes will drain, how long it will take and the exact pod and budget that will hang the drain.

**Language:** Ada | **Lines:** 2125 | **Added:** 2026-11-22

## What this solves

Every managed Kubernetes upgrade, node pool rotation, Karpenter consolidation or kernel patch ends with the same step: cordon a node, evict its pods, wait. The waiting is where things go wrong. `kubectl drain` retries a refused eviction every few seconds forever, GKE and EKS surge upgrades sit on a node for an hour before they give up, AKS reports a generic failure. The cause is nearly always a PodDisruptionBudget that cannot be satisfied right now or can never be satisfied at all.

The usual offenders are well known but hard to spot by reading YAML:

- A budget of `minAvailable: 1` on a workload with one replica. It never allows a disruption.
- A budget whose pods are partly crashlooping. The healthy count is already at the floor so no healthy pod can leave.
- Two budgets that select the same pod. The eviction API refuses the pod outright with an error, not a retry.
- A percentage or `maxUnavailable` budget over pods with no scalable controller. The disruption controller cannot compute it and allows zero.
- Replacement pods that have nowhere to land because the new pool is smaller than the old one, so the budget stays exhausted while they sit Pending.
- Pods evicted from the first old node landing on the second old node, then being evicted again. Every one of those doubles the disruption your users see.

A linter can catch the first item. The rest depend on timing, readiness, capacity and the order nodes are drained in. PDB Drain Planner simulates the drain tick by tick so all of them show up, with the cause attached, in one run.

## Why I built it

I kept watching node pool upgrades stall at 2 am on a single StatefulSet replica whose budget nobody remembered. The fix took one minute. Finding it took forty, because the upgrade tool only said "drain timed out" and `kubectl drain` only said "Cannot evict pod as it would violate the pod's disruption budget" in a loop. I wanted something I could run in CI or right before a maintenance window that answers three questions with evidence: will this drain finish, how long will it take and what exactly stops it.

I wrote it in Ada because the core is bookkeeping that must never drift: healthy counts per budget, resources per node, pods in exactly one state. Range checked integers and runtime assertions (`-gnata -gnato`) turn any accounting bug into a loud failure instead of a wrong plan. The tool reads a plain text snapshot so it has no runtime dependencies at all. A small Bash and jq exporter builds that snapshot from a live cluster or from saved JSON.

## When to use it

- Before a node pool upgrade or rotation, to find blockers while there is still time to fix them.
- In CI on a cluster snapshot, failing the pipeline when a new budget would make the next upgrade impossible.
- When choosing `--max-parallel` for a surge upgrade, to see how much faster parallel drains really are under your budgets.
- When a drain is already stuck, to get the reason for every remaining pod at once instead of reading events pod by pod.
- After incident reviews, to replay a saved support bundle offline with `--from-dir`.

It is not a replacement for the scheduler. It models requests, pod slots and node pool pinning. It does not model affinity, anti affinity, taints, topology spread or volume zones. See Notes for the full list.

## How it works

The program runs in five phases from the main block: `Parse_Arguments`, `Load`, `Resolve`, `Lint`, `Simulate`, then `Attribute` and one of `Report_Text` or `Report_Json`.

**Loading.** `Load` reads one record per line. `Parse_Line` handles four record types: `node`, `workload`, `pdb` and `pod`. Resource values go through `Parse_Quantity`, which understands Kubernetes quantities (`500m`, `2`, `0.5`, `512Mi`, `13Gi`, `8000000Ki`, `1G`) and rounds up into millicores or bytes. `Parse_Request` accepts a sum of container requests joined with `+` and a maximum over alternatives joined with `^`, so the exporter can express the effective request the scheduler uses: the larger of the app containers plus sidecars and the biggest init container. Every value is checked against `Quantity_Limit`, lines against `Max_Line_Length` and the file against `Max_Records`. Bad input stops with the line number and exit code `Exit_Usage` (2).

**Selectors.** `Parse_Selector` reads `matchLabels` pairs as `key=value` and `matchExpressions` as `key@in:a|b`, `key@notin:a|b`, `key@exists` and `key@notexists`. `*` is the empty selector that matches every pod in the namespace, as in `policy/v1`. `!` is a null selector that matches nothing. `Selects` evaluates them with the real semantics, including the fact that `NotIn` matches pods that do not carry the key at all.

**Resolution.** `Resolve` links pods to nodes and workloads, books each pod's requests on its node with `Assign`, then computes for every budget its members, healthy count, the number of members without a controller and the scale: the sum of replicas over the distinct workloads it covers.

**Budget arithmetic.** These functions mirror the disruption controller. `Expected` is the member count for an integer `minAvailable` and the controller scale otherwise. `Desired` is `minAvailable`, a rounded up percentage of expected or expected minus `maxUnavailable`. `Allowed` is healthy minus desired, never below zero. `Budget_Error` is true when a percentage or `maxUnavailable` budget covers pods with no controller, which the real controller cannot compute. `Ever_Allows` asks whether the budget could ever allow a disruption with every member healthy.

**Static findings.** `Lint` reports problems that need no simulation: `ZERO_BUDGET`, `PDB_UNCOMPUTABLE`, `PDB_DEGRADED`, `PDB_SELECTS_NOTHING`, `POD_MULTIPLE_PDBS`, `UNMANAGED_POD`, `LOCAL_STORAGE`, `SINGLE_REPLICA_OUTAGE`, `REPLICA_DRIFT` and `CAPACITY_SHORTFALL`. A finding is an error when it touches a node you are draining and a warning otherwise. The capacity check sums what must leave the drained nodes per pinned pool and overall, then compares it to free room on kept schedulable nodes. That check is necessary but not sufficient, so the simulation still has the final say on bin packing.

**The eviction rules.** `Judge` decides what the eviction API would say for one pod right now, in the same order the real server and `kubectl drain` apply:

1. DaemonSet and mirror pods are skipped, as with `--ignore-daemonsets`.
2. A pod with no controller is refused unless `--force` is set. With it the pod is evicted and never comes back.
3. A pod with an `emptyDir` volume is refused unless `--delete-emptydir-data` is set.
4. A pod covered by more than one budget is refused.
5. An unready pod is allowed when its budget uses `AlwaysAllow`. Under `IfHealthyBudget` it is allowed when healthy is at least desired and desired is above zero.
6. Otherwise the pod is allowed only while `Allowed` is above zero.

**The simulation.** `Simulate` advances in ticks of `--tick-seconds`. Each tick it marks pods ready whose start time has passed, lets `Start_Drains` cordon new targets up to `--max-parallel`, evicts every pod `Judge` allows with `Evict`, places pending replacements with `Place`, then marks nodes with nothing movable left as drained. `Start_Drains` picks the easiest node first: fewest blocked pods, then fewest movable pods, then name. `Place` scores candidates with `Score`, which copies the default scheduler's LeastAllocated strategy. It only considers nodes where `Fits` holds: active, not cordoned, same pool when the pod is pinned, enough CPU, memory and pod slots. A placed replacement becomes ready after the workload's ready seconds. `Set_Status` is the single place where pod state changes, so budget healthy counts, the lowest healthy value and the largest simultaneous disruption stay exact.

A pod that was unready in the snapshot stays unready after it moves. The planner never assumes a reschedule fixes a crashloop. That is the conservative choice and it is usually right.

**Stalls.** When a tick changes nothing and no pod is on its way to ready, nothing can change any more. The draining nodes are parked as `stuck` and the next targets start, so one run reports every blocker in the cluster instead of only the first. `--max-steps`, capped by `Max_Steps_Cap`, guards against runaway input and yields `TIMEOUT`.

**Attribution.** `Attribute` walks every target that did not drain and records each remaining pod with a code and a reason. `Explain_Budget` says why a budget is at zero: it can never be satisfied, its evicted pods are Pending with no room, its pods are unready for good or it is waiting on pods that are still starting. Pods left Pending at the end are reported as `NO_CAPACITY`.

**Output.** `Report_Text` prints findings, the drain plan with start and end time per node, blockers, the budgets touched with their lowest healthy count, then a summary with total and repeat evictions. When pods were evicted twice it suggests `--cordon-all`, which cordons every target up front so replacements cannot land on a node that is about to drain. `Report_Json` writes the same data for pipelines. The exit code is `Exit_Complete` (0) only when every target drains, nothing is left Pending and there are no error findings. Otherwise it is `Exit_Blocked` (1). `--strict` turns warnings into failures too.

## Usage

Build with GNAT. The compiler warns that the file name does not match the unit name because the file uses PascalCase. That warning is harmless.

```
gnatmake -gnat2012 -gnata -gnato -O2 -o pdbdrainplanner PdbDrainPlanner.adb
```

Export a snapshot from the current kubectl context. The drain selector is a comma separated list of node label pairs that must all match:

```
./ClusterSnapshotExport.sh --drain-selector cloud.google.com/gke-nodepool=pool-1 --ready-seconds 45 > snapshot.txt
./pdbdrainplanner --cordon-all --max-parallel 2 snapshot.txt
```

You can also name nodes directly with `--drain-nodes node-a,node-b`, pick the pool label with `--pool-label`, read another context with `--context` or work offline from a directory holding `nodes.json`, `pods.json`, `pdbs.json` and `controllers.json` with `--from-dir`. The planner reads standard input when the path is `-` or missing.

A snapshot can be written by hand too:

```
node old-a general 4     16Gi 110 drain schedulable
node new-a general 3920m 15Gi 110 keep  schedulable
workload shop/web 3 20
workload shop/db  1 60
pdb shop/web maxUnavailable 1 IfHealthyBudget app=web
pdb shop/db  minAvailable   1 IfHealthyBudget app=db
pod shop/web-1 old-a shop/web 250m 512Mi ready - app=web,tier=front -
pod shop/web-2 new-a shop/web 250m 512Mi ready - app=web,tier=front -
pod shop/web-3 new-a shop/web 250m 512Mi ready - app=web,tier=front -
pod shop/db-0  old-a shop/db  1    4Gi   ready - app=db emptydir
```

The fields are:

```
node NAME POOL CPU MEMORY PODS keep|drain schedulable|cordoned
workload NAMESPACE/NAME REPLICAS READY_SECONDS
pdb NAMESPACE/NAME minAvailable|maxUnavailable N|N% IfHealthyBudget|AlwaysAllow SELECTOR
pod NAMESPACE/NAME NODE WORKLOAD|- CPU MEMORY ready|unready POOL|- LABELS|- FLAGS|-
```

`FLAGS` is `-` or a comma list of `ds`, `mirror` and `emptydir`. Running the planner on the snapshot above prints the `ZERO_BUDGET` error for `shop/db`, the `LOCAL_STORAGE` error for `shop/db-0`, a plan where `old-a` ends as `stuck` and a blocker line naming the pod and the reason. Relax the `shop/db` budget to `minAvailable 0` (or run two replicas) and pass `--delete-emptydir-data`. The rerun ends with `result: COMPLETE` and the drain time.

Options:

```
--tick-seconds N        seconds per simulated eviction retry (default 5)
--max-parallel N        nodes drained at the same time (default 1)
--max-steps N           safety limit on simulated ticks (default 100000)
--cordon-all            cordon every drain target before the first eviction
--force                 allow pods without a controller (they are deleted)
--delete-emptydir-data  allow pods with emptyDir volumes
--json                  machine readable output
--strict                exit 1 on any warning as well
```

## Notes

- Lines: 1977 in `PdbDrainPlanner.adb` and 148 in `ClusterSnapshotExport.sh`. The exporter needs bash 4 and jq 1.6 or newer. It only reads from the cluster.
- The scheduler model covers requests, pod slots and pool pinning through a `nodeSelector` on the pool label. Affinity, anti affinity, taints, tolerations, topology spread, PVC zone binding and priority preemption are not modelled. A plan that passes here can still fail on those. A plan that fails here will fail in the cluster too unless one of them frees room.
- Every replacement is assumed to come back after the workload's ready seconds. The exporter cannot know real startup times, so set `--ready-seconds` to something honest or edit the `workload` lines.
- Owners other than ReplicaSet, StatefulSet and ReplicationController, such as Jobs or Argo Rollouts, are exported as a workload whose replicas equal the pods seen. The real controller may treat a percentage budget over them differently.
- A pod whose controller disappeared between the two kubectl calls makes the planner stop with an unknown workload error. Export again.
- `--force` mirrors `kubectl drain --force`: unmanaged pods still go through their budgets and are not recreated.
- Termination grace periods are not modelled. Add them to ready seconds when they are long.
- Keywords: Kubernetes node drain, PodDisruptionBudget simulator, PDB blocking drain, kubectl drain stuck, cannot evict pod disruption budget, node pool upgrade planner, GKE EKS AKS surge upgrade, Karpenter consolidation, unhealthyPodEvictionPolicy, eviction API.
