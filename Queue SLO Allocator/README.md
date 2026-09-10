# Queue SLO Allocator

You have 400 worker slots and 30 queues that together want 700. This is a single file C command line tool that reads queue telemetry as CSV and decides who gets how many workers, ranked by SLO risk instead of by who shouted first.

**Language:** C | **Lines:** 419 | **Added:** 2026-07-07

## What this solves

The April 2026 queue capacity problem: AI eval runners, batch inference jobs, ETL backfills, MCP tool workers, edge image pipelines and smart IoT ingest queues all compete for the same limited concurrency while users still expect latency SLOs to hold. Concurrency is finite because a provider rate limit, a GPU pool, a database connection cap or a runner budget says so. Demand is not. Every launch week, every provider incident and every end of month billing freeze becomes the same argument about who gets slots.

Without something like this the allocation happens by hand in a war room. Someone bumps the HPA max on the queue that is paging, starving a quieter queue whose deadline is actually tighter. Someone else gives the loud tenant twice the workers it needs because the backlog number looked scary, not realising that backlog was already draining inside its deadline window. The failure mode is not a crash. It is slow, invisible misallocation: the eval queue finishes early with spare capacity while the customer facing inference queue misses its p95 deadline for four hours.

The cost is SLO breaches a customer notices, wasted concurrency on queues that were already fine (real money per hour on GPU backed inference) and an audit gap: asked later why queue X had 12 workers and queue Y had 3, the honest answer is usually "that is what the config said after the last incident". This makes the decision a deterministic function. Same CSV in, same plan out, intermediate numbers printed so the result is reviewable.

## Why I built it

Most teams already have every input this needs sitting in Prometheus or Datadog: backlog depth, arrival rate, per worker throughput, p95 latency, deadline and a priority tier. What they do not have is the step that turns those six numbers into a worker count. Kubernetes HPA scales one workload against one metric and knows nothing about the other nine sharing the same quota. Celery, Sidekiq and Nomad let you set pool sizes but will not tell you what the sizes should be. Fair share schedulers live inside big cluster managers and are not extractable. So the arithmetic gets done in a spreadsheet during an incident, or not at all.

I wanted one file with no database, no broker client and no cloud SDK, that reads CSV on stdin, writes a plan on stdout and exits non zero when the plan does not fit.

## When to use it

- A provider incident halves your inference concurrency and you have five minutes to decide which tenants absorb the loss.
- Launch week: three teams each want their eval runner pool doubled and the runner budget is fixed.
- A model migration leaves an ETL backfill with a million item backlog while live traffic still has a 2 second deadline to hold.
- Nightly CI that fails the build when required capacity exceeds the provisioned pool, so you find out before the pager does.
- Sizing a Kubernetes HPA `maxReplicas` or a Nomad task group from measured throughput rather than a guess.
- Postmortem work where you need to show what the allocation should have been given the telemetry at the time.

## How it works

Input is CSV on stdin, ten columns, the last two optional: `tenant,queue,backlog,arrival_per_sec,service_per_sec_per_worker,p95_ms,deadline_ms,priority[,min_workers,max_workers]`. `read_queues` skips blank lines and `#` comments, detects a header via `field_is_header` when the first field lowercases to `tenant` or `queue`, and calls `parse_csv`, a quote aware scanner that handles `""` escaping. Numeric fields go through `parse_double` or `parse_int_field`, which reject trailing garbage, `ERANGE` overflow and non finite values, then exit 64 naming the offending line. `parse_queue` rejects negative backlog, arrival, p95 or priority, and requires a positive service rate, a positive deadline and `max_workers >= min_workers`.

`compute_queue` is the model. It picks a drain window first: the smaller of `--horizon-sec` and the queue's own deadline in seconds, floored at 1 second. Demand is `arrival_per_sec + backlog / drain_window`, so a backlog is only an emergency if it will not drain inside that window. Base worker count is `demand / (service_per_sec_per_worker * target_utilization)`, and the default target utilization of 0.82 is the headroom term. Sizing to full utilization is how queues go unstable under variance.

Latency pressure enters as a multiplier. `latency_ratio` is `p95_ms / deadline_ms`. At or under 1 the multiplier is 1. Once p95 is past the deadline it is the ratio itself, clamped to 4 by `clamp_double`, so a queue at three times its deadline asks for three times the steady state workers to dig out. The result is ceiled, raised to `min_workers`, then capped at `max_workers`. Both numbers reach the output as `required_uncapped` and `required`, which is how you tell a real shortfall from a fence you set yourself.

Risk is a separate scalar: `(1 + priority) * (latency_ratio^2 + backlog_seconds / drain_window + 0.25)`. Squaring the latency ratio makes the ranking convex, so a queue at 2x its deadline outranks two queues at 1x rather than tying with them. The 0.25 floor keeps a healthy queue from scoring zero. Allocation in `allocate` is then a greedy loop, one worker per iteration: score every queue that still has room via `allocation_score`, give the slot to the highest scorer. The score is explicitly two phase. Any queue below its `min_workers` gets a flat `1000000.0` bonus, so all floors are satisfied before a single discretionary worker is handed out. After that it is `risk * 100 + deficit * (1 + priority)`, so risk dominates and remaining deficit breaks ties. Equal scores resolve to the lowest index because the comparison is strict `>`, which is what makes runs reproducible.

`classify` labels each row in priority order: `minimum_deficit` if the floor could not be met, `capped_by_max_workers` if the true requirement exceeded your ceiling, `worker_deficit` if slots ran out, `latency_recovering` if the queue is fully staffed but still past its deadline, otherwise `ok`. Output is TSV on stdout with a one line summary on stderr, or a single JSON object with `--json`. `has_deficit` drives the exit code.

## Usage

```sh
cc -O2 -o queue-slo-allocator QueueSloAllocator.c -lm

cat > queues.csv <<'EOF'
tenant,queue,backlog,arrival_per_sec,service_per_sec_per_worker,p95_ms,deadline_ms,priority,min_workers,max_workers
acme,live-inference,1200,45,3.5,2400,2000,3,4,64
acme,eval-runner,90000,5,1.2,800,60000,1,1,40
globex,etl-backfill,2400000,0,12,500,900000,0,,
globex,tool-worker,300,18,6,150,1000,2,2,
EOF

# TSV plan on stdout, summary line on stderr
./queue-slo-allocator --workers 400 < queues.csv

# JSON for a bot or a CI gate
./queue-slo-allocator --workers 400 --json < queues.csv

# Tighter drain horizon and a more conservative utilization target
./queue-slo-allocator --workers 400 --horizon-sec 120 --target-utilization 0.7 < queues.csv

# Fail CI when the provisioned pool cannot cover requirements
./queue-slo-allocator --workers 400 < queues.csv > plan.tsv || echo "capacity shortfall"

./queue-slo-allocator --help
```

## Notes

- Exit codes: 0 when every queue got what it required, 2 when any queue is short or capped below its true requirement, 64 for bad arguments, bad CSV or empty input. `--help` exits 0.
- Limits are fixed at build time: 4096 queues, 8192 byte lines, 16 CSV fields, 256 bytes per field, 96 bytes for names. Long names truncate silently, an over long line is a hard error, and too many queues tells you to raise `QSA_MAX_QUEUES` and rebuild.
- The allocator loop is O(total_workers x queues) because it really does assign one slot at a time. Fine at a few thousand workers across a few thousand queues. Pass a worker count in the millions and expect it to sit there.
- A planner, not a controller. It does not talk to Kubernetes, Nomad, Celery, a broker or any cloud API. It prints a plan and you apply it.
- The queueing model is deliberately simple: arrival plus linear backlog drain, divided by capacity with a utilization margin. No M/M/c wait time calculation, no variance term, no arrival distribution. The latency multiplier is a corrective heuristic, not a derived service time.
- Every queue is capped at its own `required`, so leftover capacity is reported as `unused` rather than distributed. To spread the surplus, feed it back as raised `min_workers`. Input is stdin only, and an omitted `max_workers` defaults to `INT_MAX / 4`, which shows up as that large number in the `max` column.
