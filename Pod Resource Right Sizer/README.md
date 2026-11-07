# Pod Resource Right Sizer

A Crystal CLI that reads container usage samples as JSONL and recommends Kubernetes CPU and memory requests. It refuses to guess when the data is thin, unstable or clipped by an OOM kill.

**Language:** Crystal | **Lines:** 516 | **Added:** 2026-11-07

## What this solves

Most Kubernetes clusters waste money in the same way. Someone picked a CPU request of 1000m and a memory request of 2Gi on day one, the service settled at 200m and 350Mi, and nobody touched it again. Multiply that by a few hundred workloads and you are paying for nodes that sit mostly empty.

The usual fix is a right sizing tool, and the naive version of it is a one liner: take the average, add 20 percent, print it. That version hurts you in production. Averages hide bursts. A quiet weekend makes every service look oversized. A container that was OOM killed at its limit shows usage that stops at the limit, so the tool thinks demand is flat and recommends the same number that already killed it. A pod stuck in a crash loop reports tiny usage because it keeps restarting, and the tool tells you to shrink it further.

Pod Resource Right Sizer is built around those failure modes. It is a single Crystal file with no shards, reads samples from a file or stdin, and gives you a per container recommendation with a status, an action and notes that explain every judgment call. If it cannot make a safe call it says so and leaves the numbers alone.

Search terms this is meant to answer: Kubernetes right sizing, resource requests recommendation, pod CPU and memory requests, OOMKilled memory limit tuning, VPA alternative, cluster cost reduction, FinOps for Kubernetes.

## Why I built it

I kept running into two bad options. The Vertical Pod Autoscaler is good but it wants to live inside the cluster and evict pods, which many teams will not allow. The dashboards from cost tools tell you a workload is oversized but give you a number you cannot trust for memory. I wanted something that runs in CI or on a laptop, takes an export from Prometheus or any metrics store, and prints a recommendation I would actually be willing to commit to a Helm values file.

Crystal fits this well. It compiles to a single static binary that is fast on a few million samples, and the syntax is readable enough that a platform team can change the policy without a long onboarding. The policy lives in plain constants at the top of the file, so a reviewer can see in ten seconds what the tool assumes.

## When to use it

- You export per container usage on a schedule and want a weekly right sizing report.
- You want to gate deploys: run it in CI with `--fail-on-risk` so a service that is already hitting its memory limit blocks the change.
- You are not allowed to run VPA in update mode but still want its logic.
- You are about to reduce node count and need to know which requests are safe to cut.

Do not use it for workloads with strongly weekly or monthly cycles unless your export covers a full cycle. The default minimum is 24 hours of history, which is a floor, not a recommendation. Give it a week.

## How it works

Input is one JSON object per line. Each line is a `RawSample` with `ts` (unix seconds), `namespace`, `workload`, `container`, `cpu_millicores` and `memory_bytes`. Optional fields are `pod`, `cpu_request_millicores`, `memory_request_bytes`, `memory_limit_bytes`, `oom_killed`, `restarts` and `revision`.

`load` parses the stream. A malformed line is counted in `LoadStats` and skipped, because truncated last lines are normal in exports. `valid_sample?` rejects negative or non finite values. If the unreadable fraction goes above `max_bad_fraction` (5 percent by default) the run stops with exit code 3 and prints the first few bad lines. Silent data loss is worse than a loud failure.

`analyze` works on one workload and container at a time:

1. Duplicate scrapes of the same pod at the same second are dropped with a `Set`.
2. If more than one `revision` appears, only the newest revision is used, as long as it has at least `min_samples` samples. Otherwise the older revisions stay in and a note warns you the answer may be stale. A new release changes memory behavior, and sizing from last month's code is a common source of bad requests.
3. Each pod's first `warmup_seconds` (120 by default) are dropped. JIT compilation, cache fill and migrations inflate early usage and would push the target up forever.
4. The `min_samples` and `min_span_hours` gates run next. Fail either and the status is `insufficient_data` with no recommendation.
5. Weights come from time, not from sample count. The median scrape interval sets a cap of twice the median, and each sample counts for the interval it covers up to that cap. `weighted_quantile` then computes the quantile. A gap longer than the cap is treated as missing data instead of a long flat stretch. If more than 20 percent of intervals are gaps, a note says percentiles are less trustworthy.
6. Restarts are counted per pod using positive deltas only, since counters reset when a pod is replaced. More than `max_restarts` restarts with no OOM flag sets status `unstable`. Usage during a crash loop is biased low, so the tool holds instead of shrinking.

CPU is the weighted p95 times `1 + cpu_headroom`, floored at `MIN_CPU_MILLICORES` and rounded up by `cpu_step`. A CPU limit is never recommended. A limit causes CFS throttling when the node still has spare cycles, which shows up as latency that no dashboard blames on the limit. If the observed peak is more than four times the target, a note flags a bursty workload.

Memory is the larger of the weighted p99 plus `mem_headroom` and the observed peak plus `peak_headroom`. It is rounded up to `MEMORY_STEP_BYTES` (16 MiB) and floored at `MIN_MEMORY_BYTES`. The suggestion is meant for request and limit together so the pod is Guaranteed for memory.

The key rule is censoring. If any sample is OOM flagged, or any sample reached `CENSOR_RATIO` (97 percent) of the memory limit, observed usage is clipped by the limit and tells you nothing about real demand. In that case the target becomes the larger of the current limit and the peak, times `1 + oom_bump` (25 percent by default), the workload is marked urgent, and the tool will never recommend a decrease.

`decide` turns targets into actions. Changes smaller than `deadband` (10 percent) are `keep`. Increases apply in full. Decreases are capped at `max_decrease` per run (50 percent), reported as `decrease_stepped` when the cap applies, so one quiet week cannot strip a service. A missing current request becomes `set`. Unstable workloads get `hold`. Savings are multiplied by a replica estimate taken from the distinct pods seen in the last hour of data.

`render_table` and `render_json` print the result. `run` returns exit code 0 normally, 1 for `--fail-on-risk` with an urgent workload, 2 for bad flags and 3 for unusable input. `Config#validate!` rejects nonsense such as a quantile above 1.

## Usage

Build and run:

```
crystal build --release PodResourceRightSizer.cr -o pod_resource_right_sizer
./pod_resource_right_sizer --input SampleUsage.jsonl
./pod_resource_right_sizer --input SampleUsage.jsonl --format json
cat usage.jsonl | ./pod_resource_right_sizer --namespace shop --fail-on-risk
```

The folder includes `SampleUsage.jsonl`, 48 hours of two pods for a service in the `shop` namespace that requests 1000m and 2Gi but uses far less. The table output lists the workload, status, current and new CPU, current and new memory, the action for each, pod count and hours of history.

Flags: `--input`, `--format`, `--namespace`, `--min-samples`, `--min-span-hours`, `--warmup-seconds`, `--cpu-quantile`, `--cpu-headroom`, `--mem-quantile`, `--mem-headroom`, `--peak-headroom`, `--oom-bump`, `--deadband`, `--max-decrease`, `--max-restarts`, `--max-bad-fraction`, `--fail-on-risk` and `--help`.

## Notes

- The tool only reads data and prints. It never talks to a cluster.
- It sizes per container, not per pod. Sidecars get their own row.
- Memory is censored detection based on what you export. If your export has no `memory_limit_bytes` and no `oom_killed`, the OOM protection cannot fire. Export both.
- Heap based runtimes such as the JVM or Go with a soft limit can show memory that tracks the limit by design. If the workload sits near its limit by choice, set the runtime limit lower than the container limit or raise `--oom-bump` knowingly.
- Quantiles are exact over the retained samples, not sketched, so very large inputs use memory proportional to the sample count for one workload at a time.
- I could not run the Crystal compiler in the environment that produced this entry, so build it once and run the sample before trusting it in a pipeline. Please open an issue if the build reports anything.
