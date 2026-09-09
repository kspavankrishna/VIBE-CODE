# GPU Spot Checkpoint Planner

Cheap spot GPUs stop being cheap the moment a node is preempted and you lose an hour of unwritten training state. This is a single file C++ command line planner that decides which jobs are safe on spot capacity, how often each one should checkpoint and which jobs belong on reserved capacity instead.

**Language:** C++ | **Lines:** 925 | **Added:** 2026-08-18

## What this solves

The failure mode is always discovered after the node dies. An eval sweep, a model regression test or a batch RAG rebuild gets placed on preemptible GPUs because the per hour price is a third of on-demand. The job checkpoints every 20 minutes because that number was hardcoded in a YAML file eighteen months ago, and the node loses a GPU roughly every four hours. Each preemption costs you half a checkpoint interval of lost compute plus the restart plus another write, and the job goes back into the queue it just waited 22 minutes to leave. Nobody sees a single alarming event. What they see is a bill that says spot saved money and a release gate that slipped two days.

The second failure is the SLO one. Queue age is already burned before the job starts running. Add expected preemption overhead to the raw runtime and finish time crosses the deadline, but no ordinary scheduler models that, so the job is admitted, runs, restarts twice and misses. The person who notices is the release manager, not the platform team, and by then the only fix is emergency reserved capacity at full price.

Third: nobody checks scratch space against checkpoint size. A job writing 180 GB checkpoints onto a node with 100 GB of local scratch fails on its first write, the most expensive possible time to find out. Budget is the same class of problem: a four GPU, 7.5 hour sweep at $0.72 per GPU hour runs about $21.60 against a $4 ceiling that exists for a reason.

This tool takes your capacity list and your job list, scores every job against every node and returns a verdict per job: run here at this checkpoint interval, run with aggressive checkpointing, run now and drain the queue or do not run this on spot at all. It prints expected cost, eviction probability, recommended checkpoint interval, projected finish time and carbon in kilograms, so the decision is auditable rather than a vibe.

## Why I built it

Every piece of this exists somewhere. Cloud consoles show spot interruption frequency. Schedulers do bin packing. Optimal checkpoint intervals are solved in the HPC literature. What does not exist is one thing you can run in CI that takes all three together and returns an answer a human can act on. Kubernetes schedulers do not know your checkpoint write bandwidth. Cost tools do not know your SLO. Nothing reconciles business priority with eviction rate.

So it is one C++20 file with no dependencies beyond the standard library. No service, no database, no Python package, no cloud SDK. It reads JSONL or key=value lines on stdin and writes a table, JSON or CSV. That makes it forkable, which matters more than being complete, because your eviction rates and priority weights are not mine.

## When to use it

- A batch eval or RAG rebuild pipeline is moving to spot GPUs and you need to know which jobs actually survive preemption
- A release gate keeps missing its deadline and you suspect restart overhead rather than raw compute
- You are picking a checkpoint interval by guessing, or using the same interval for a 1 GB checkpoint and a 180 GB one
- CI needs a gate that fails the build when a plan puts a priority 5 job on capacity with a 30 percent eviction chance
- Finance asks why the spot bill went up while throughput went down
- Carbon reporting means placement should weigh grid intensity, not just dollars

## How it works

Input is line oriented. `parse_record` accepts either a JSON object (via the hand written `parse_json_object`, which flattens one level into a string map) or shell style `key=value` tokens via `parse_key_values`, so both forms mix freely in one file. Blank lines and `#` comments are skipped. `record_kind` infers node versus job when `kind` is absent, keying off `price_per_gpu_hour`/`free_gpus` against `gpu_hours`/`slo_minutes`. `build_node` and `build_job` apply defaults then validate hard: negative prices, `free_gpus` above `gpus`, priority outside 1 to 5 and `min_checkpoint_minutes` above `max_checkpoint_minutes` all throw. Errors accumulate per line into a `ParseError` list with line numbers instead of aborting on the first bad record, so you see every problem in one pass.

The checkpoint interval comes from Young's formula. `recommended_checkpoint_minutes` computes `sqrt(2 * write_minutes * mtbf_minutes)`, where `mtbf_minutes` is `60 / eviction_rate_per_hour` and `write_minutes` comes from `checkpoint_write_minutes`, which turns `checkpoint_gb` into megabytes and divides by the node's `checkpoint_write_mbps`. That is the classic interval balancing write cost against expected lost work. The result is clamped to the job's own `min_checkpoint_minutes` and `max_checkpoint_minutes`. On a node with effectively no eviction rate the interval falls back to the job maximum, since there is nothing to defend against.

Eviction risk is modelled as a Poisson process. `evaluate_candidate` takes `1 - exp(-eviction_rate_per_hour * gpu_hours)` as the probability of at least one preemption and `eviction_rate_per_hour * gpu_hours` as the expected interrupt count. Expected overhead is the number of checkpoint writes plus expected interrupts times the loss per interrupt, where loss per interrupt is half a checkpoint interval (average work since the last write) plus `interrupt_cost_minutes` plus one more write. Finish time is `queue_age_minutes + runtime + expected_overhead`, and that is the number compared against the SLO.

Feasibility is a list of named reasons, not a boolean: `insufficient_free_gpus`, `checkpoint_scratch_too_small` (scratch must be at least twice the checkpoint size), `job_budget_exceeded`, `slo_would_be_missed` and `spot_eviction_risk_too_high`. The risk ceiling is per job. `risk_limit_for` takes the global `--max-spot-risk` and tightens it to 0.08 for priority 5 and 0.14 for priority 4, or loosens it to at least 0.35 for priority 1 and 2, so a release gate and a nightly experiment are held to different standards from the same file.

Ranking is one scalar score, lower is better: dollar cost, plus carbon weighted at 0.035 (0.18 with `--prefer-low-carbon`), plus a spot risk penalty scaled by eviction probability and priority, plus an overhead penalty, plus a lateness penalty past the SLO, plus 10000 per infeasibility reason. That last term is a soft barrier, so infeasible nodes sink to the bottom but still rank among themselves, which is what makes the "why did nothing fit" output useful. `build_plan` sorts jobs by priority descending, then tighter SLO first, then oldest queue age, then greedily assigns each to its best feasible node and decrements `free_gpus` there so later jobs see real remaining capacity.

`action_for` turns the choice into a verb: `run`, `run_with_aggressive_checkpointing` above 75 percent of the job's risk ceiling, `run_now_and_drain_queue` past 85 percent of the SLO and `split_job_or_use_reserved_capacity` when nothing fit. Output goes through `render_table`, `render_json` or `render_csv`.

## Usage

```bash
g++ -std=c++20 -O2 -o gpu-spot-planner GpuSpotCheckpointPlanner.cpp

# built in scenario, verifies the scoring end to end
./gpu-spot-planner --self-test

# plan from a file
./gpu-spot-planner --input capacity_and_jobs.jsonl --format table

# JSON for a pipeline, fail the build if anything lands unscheduled
./gpu-spot-planner -i plan.jsonl -f json --fail-on unscheduled

# tighter global risk ceiling and carbon aware placement
./gpu-spot-planner -i plan.jsonl --max-spot-risk 0.10 --prefer-low-carbon

# stdin also works, key=value lines are accepted alongside JSONL
cat <<'IN' | ./gpu-spot-planner -f csv
kind=node id=spot_a region=us-east-1 pool=spot gpus=4 free_gpus=4 price_per_gpu_hour=0.72 eviction_rate_per_hour=0.24 checkpoint_write_mbps=900 scratch_gb=900
kind=node id=reserved_a pool=on-demand gpus=2 free_gpus=2 price_per_gpu_hour=2.35 eviction_rate_per_hour=0.002 scratch_gb=600
kind=job id=release_gate gpus=1 business_priority=5 gpu_hours=1.8 checkpoint_gb=14 max_checkpoint_minutes=8 queue_age_minutes=22 slo_minutes=170 interrupt_cost_minutes=25 max_cost_usd=6.0
IN
```

## Notes

- Exit codes: 0 for a clean plan, 1 for parse errors, a missing input file, no node records, no job records or a bad flag, 2 when the plan violates your `--fail-on` condition (none, unscheduled, risk or budget). Wire 2 into CI.
- `gpu_hours` is used both as wall clock duration (runtime and eviction exposure) and multiplied by GPU count for cost. If your jobs scale sublinearly across GPUs, the cost figure is an upper bound.
- Eviction rate is an input, not something the tool measures. Garbage in, confident garbage out. Feed it real interruption frequency from your provider or your own logs.
- Placement is greedy in priority order. It does not backtrack and will not find a globally cheaper packing, which is a fair tradeoff for a planner you can read in one sitting. The JSON parser is also minimal: flat one level objects only, no nested objects or arrays, and unicode `\u` escapes are rejected rather than decoded.
- Carbon is `gpu_hours * gpus * watts_per_gpu/1000 * carbon_g_per_kwh/1000`. It ignores PUE, idle draw and embodied carbon. A comparison signal between nodes, not a reportable figure.
- It plans only. It does not submit, launch, checkpoint or talk to any scheduler, and the output is a decision you or your automation still has to act on.
