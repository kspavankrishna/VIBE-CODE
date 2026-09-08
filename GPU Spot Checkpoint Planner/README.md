# GPU Spot Checkpoint Planner

Cheap spot GPUs stop being cheap the moment a node gets evicted and the run loses an hour of unsaved work. This is a single file C++ command line planner that decides, per job, which GPU pool to run on, how often to checkpoint and whether the deadline survives the expected interruptions.

**Language:** C++ | **Lines:** 925 | **Added:** 2026-08-18

## What this solves

This solves the ugly April 2026 problem where an AI infra team wants cheap spot GPU capacity but cannot explain which inference eval, model regression test, batch RAG rebuild or research sweep is actually safe to run there. Somebody sees spot pricing at a third of on-demand, moves the eval queue onto preemptible nodes and everyone claps. Two weeks later the release gate eval is three hours late, nobody can say why and the savings never showed up because half the runs were repeated.

The failure is always found after the node dies, never before. The checkpoint interval was picked by guesswork, so the job writes state every twenty minutes on a pool that evicts every forty. The queue SLO was half burned by wait time before the job even started. The retry cost was invisible: nobody counted the minutes lost rerunning from the last checkpoint plus the container pull and model reload. And the scratch disk was smaller than two copies of the checkpoint.

Concretely: a 7.5 GPU hour sweep on a pool with a 0.24 per hour eviction rate has roughly an 83 percent chance of being interrupted at least once. If the checkpoint takes four minutes to write and you write every twenty minutes, a fifth of the run goes to checkpointing alone and each eviction costs another ten minutes plus restart. The scheduler still reports the job as running. The team that owns the release gate finds out at 2 AM that the eval never finished. This planner turns that into numbers before submission, scoring every job against every pool on write time, eviction probability, expected lost minutes, deadline pressure, budget, free GPUs, scratch, carbon and priority, then emitting a decision: run, run with aggressive checkpointing, run now and drain the queue or split the job and use reserved capacity.

## Why I built it

Cloud schedulers know capacity and price. They do not know your checkpoint size, your restart cost or your deadline, so they cannot tell you a job is arithmetically doomed on the pool they just placed it on. Kubernetes will happily schedule a pod onto a node reclaimed twenty minutes later, and the cost dashboards are all backward looking.

I wanted something that runs before submission with no service, no database, no Python package and no cloud SDK. You already have the numbers in a scheduler dump, a notebook, a CI eval config or a capacity report. Pipe them in as JSONL or key=value lines and get a decision table back.

## When to use it

- You are moving a nightly eval suite or model regression run onto preemptible capacity and want to know which jobs are safe there
- A release gate eval keeps missing its deadline and you need to prove whether the cause is evictions, checkpoint overhead or queue wait
- You are choosing a checkpoint interval for a long training job and are currently guessing
- Your CI runner needs a gate that fails the pipeline when the plan puts a high priority workload on risky capacity
- Finance is asking why the spot migration did not cut spend and you need per job cost and expected overhead numbers
- You want to bias placement toward lower carbon intensity regions without ignoring cost

## How it works

Input is line oriented. `parse_record` skips blanks and `#` comments, then dispatches on the first character: `{` goes to `parse_json_object`, anything else to `parse_key_values`, which uses `split_unquoted` so a quoted value with a space survives. `record_kind` reads an explicit `kind` or `type`, and failing that infers one from the fields present. `parse_input` collects every `ParseError` with its line number instead of dying on the first, so a bad feed shows all its problems at once.

The placement math lives in `evaluate_candidate`. Write time is `checkpoint_gb * 1024 / checkpoint_write_mbps / 60` minutes. The interval comes from `recommended_checkpoint_minutes`, which is the Young/Daly optimum: `sqrt(2 * write_minutes * mtbf_minutes)` with MTBF as `60 / eviction_rate_per_hour`. That is the classic result for the interval minimising checkpoint cost plus rework, and it fits because the two costs pull opposite ways. Checkpoint too often and you pay write time every interval. Checkpoint too rarely and each eviction costs more redone work. The result is clamped into the job's own min and max window, and a node with an effectively zero eviction rate uses the job maximum instead.

Eviction is modelled as a Poisson process. `eviction_probability` is `1 - exp(-rate * gpu_hours)`, the chance of at least one interrupt, and `expected_interrupts` is `rate * gpu_hours`. Expected overhead is checkpoint count times write time, plus expected interrupts times the loss per interrupt, where that loss is half an interval (the average kill point) plus `interrupt_cost_minutes` plus one more write. `finish_minutes` adds queue age and runtime, and that is what gets compared against the SLO.

Feasibility is a list of named reasons, not a boolean: `insufficient_free_gpus`, `checkpoint_scratch_too_small`, `job_budget_exceeded`, `slo_would_be_missed` and `spot_eviction_risk_too_high`. The risk ceiling is per job. `risk_limit_for` starts from `--max-spot-risk`, tightens it to 0.08 for priority 5 and 0.14 for priority 4 and loosens it to at least 0.35 for priority 1 and 2, so a release gate and a throwaway sweep never get the same tolerance. Scoring is a weighted sum in dollars: cost, plus carbon times a weight that jumps from 0.035 to 0.18 under `--prefer-low-carbon`, plus a spot risk penalty of `eviction_probability * priority_weight * 35`, plus overhead and lateness penalties scaled by `priority_weight` (`1 + business_priority * 0.8`), plus a flat 10000 per infeasibility reason. That last term is a soft barrier: infeasible candidates sink but stay ordered among themselves, so when nothing fits the output still names the closest miss and why it failed.

`build_plan` is greedy. Jobs are stable sorted by priority descending, then SLO ascending, then queue age descending, so urgent work claims capacity first. Every node is scored for each job, the best feasible one wins and that node's `free_gpus` is decremented before the next job runs. `action_for` labels the outcome, one of the three renderers prints it and `should_fail` converts the plan into an exit code for CI.

## Usage

```bash
# build (standard library only, no dependencies)
g++ -std=c++20 -O2 -o gpu-spot-planner GpuSpotCheckpointPlanner.cpp

# verify the binary against the embedded fixture
./gpu-spot-planner --self-test

# plan from a file
./gpu-spot-planner --input fleet.jsonl --format table

# plan from stdin, JSON out, bias toward low carbon regions
your-capacity-exporter | ./gpu-spot-planner -f json --prefer-low-carbon

# CI gate: exit 2 if any job lands on capacity that is too risky
./gpu-spot-planner -i fleet.jsonl --fail-on risk --max-spot-risk 0.15
```

Input accepts JSONL and key=value lines, mixed freely in one stream:

```
{"kind":"node","id":"cheap_spot_a","pool":"spot","gpus":4,"free_gpus":4,"price_per_gpu_hour":0.72,"eviction_rate_per_hour":0.24,"checkpoint_write_mbps":900,"carbon_g_per_kwh":210,"scratch_gb":900}
kind=node id=reserved_a pool=on-demand gpus=2 free_gpus=2 price_per_gpu_hour=2.35 eviction_rate_per_hour=0.002 scratch_gb=600
kind=job id=release_gate gpus=1 business_priority=5 gpu_hours=1.8 checkpoint_gb=14 max_checkpoint_minutes=8 slo_minutes=170 interrupt_cost_minutes=25 max_cost_usd=6.0
```

Flags: `--input/-i`, `--format/-f` (`table`, `json`, `csv`), `--fail-on` (`none`, `unscheduled`, `risk`, `budget`), `--max-spot-risk` (0 to 0.95, default 0.25), `--prefer-low-carbon`, `--self-test`, `--help/-h`.

## Notes

- Exit codes: 0 for a clean plan, 1 for parse errors, a missing input file, no node records or no job records, 2 when the plan rendered but tripped `--fail-on`. Parse errors go to stderr with line numbers and nothing is planned.
- Eviction rates, write throughput, watts per GPU and grid carbon intensity are inputs, not measurements. The planner never calls a cloud API. Stale numbers give you confident nonsense, so wire the eviction rate to real preemption history.
- Placement is one node per job. A job needing 8 GPUs will not spread over two nodes with 4 free each, it goes unscheduled with `insufficient_free_gpus`. The `split_job_or_use_reserved_capacity` action is a label telling you to do that yourself.
- The greedy pass never backtracks. A high priority job taking the last cheap slot can push a lower priority job onto expensive capacity where a different assignment would have been better overall. Priority order is the contract, not optimal packing.
- Cost is `gpu_hours * gpus * price_per_gpu_hour`, so `gpu_hours` is per GPU wall clock hours, not aggregate. The budget check uses clean runtime cost and does not add expected interrupt overhead.
- The JSON reader handles flat objects only: nested objects, arrays and `\uXXXX` escapes throw. The CSV writer does no quoting, so an id containing a comma corrupts that format. Defaults are opinionated (400 MB/s writes, 650 W per GPU, 450 g CO2 per kWh, 100 GB scratch), so override the ones that matter for your fleet.
