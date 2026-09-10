# Tenant GPU Lease Planner

Deciding how many GPUs each inference tenant needs is usually a spreadsheet guess. This is a single file Go planner that reads a JSON description of your GPU pools and tenant demand, then returns a deterministic placement plan with replica counts, hourly cost, carbon estimate, predicted p99 latency and an explicit reason for every tenant it could not place.

**Language:** Go | **Lines:** 832 | **Added:** 2026-06-02

## What this solves

The failure mode is quiet and expensive. Someone sizes an LLM serving deployment by dividing expected requests per second by a throughput number from a benchmark blog, rounds up and ships it. Then real traffic arrives with 6000 token prompts and 96 concurrent requests, the KV cache pushes each replica past 80 GB of VRAM, the scheduler starts OOM killing pods and p99 latency goes from a promised 1.8 seconds to whatever the retry storm produces. Nobody notices in staging because staging runs one request at a time.

The second failure mode is money. You reserve H100 capacity for a batch eval job that would have run fine on cheaper L40S spot instances, or you place a tenant with a strict data residency requirement in a region that does not satisfy it and find out during an audit. A GPU fleet is a lease market with hard constraints: VRAM per card, tokens per second, price per hour, preemption risk, health, availability windows, residency tags and confidential compute. Picking the cheapest pool without checking the rest is how a team ends up paying reserved prices for spot workloads and spot prices for workloads that cannot tolerate preemption.

The third failure mode is that nobody can explain the plan afterwards. A tenant asks why their service landed on a pool in another region, or why their request was refused entirely, and the answer is a shrug plus a Slack thread. This planner emits a `reasons` array on every allocation and, when `explainRejected` is set, a per pool `candidateFailures` list saying exactly which constraint each pool violated for that tenant. Without that, capacity decisions live in one engineer's head and get rebuilt badly every quarter.

## Why I built it

Kubernetes device plugins and cluster autoscalers place pods, they do not size them. They will happily schedule a replica count you chose wrongly. Cloud cost tools tell you what you already spent. Neither one connects token shape to VRAM to throughput to latency to price to carbon in one pass, and neither one gives you a machine readable artifact you can diff in a pull request or fail a CI job on.

I wanted a tool with no dependencies beyond the Go standard library that reads JSON on stdin and writes JSON on stdout, so it drops into a pre deploy gate, a capacity review, or an internal developer portal without dragging in a scheduler framework.

## When to use it

- Sizing a new multi tenant LLM serving cluster before you commit to reserved GPU capacity for a quarter.
- Deciding whether a batch eval workload can move onto spot L40S while a production RAG API stays on reserved H100.
- Running a pre deploy CI check that fails when a tenant's declared token shape and concurrency no longer fit the pools you own.
- Answering an auditor or a customer about which region and which residency tags a workload landed in and why.
- Comparing two candidate fleet designs by hourly cost and grams of CO2 per hour before the purchase order goes out.

## How it works

`main` parses two flags, reads at most `maxInputBytes` (8 MB) from stdin through an `io.LimitReader`, and decodes a `PlannerRequest` with `DisallowUnknownFields` so a typo in a policy key is an error rather than a silently ignored field. Everything real happens in `Plan`.

`Plan` first calls `normalizePolicy`, which fills defaults and normalizes the five scoring weights so they sum to one. If every weight is zero it installs the built in profile: cost 0.24, carbon 0.14, latency 0.22, reliability 0.30, locality 0.10. Overcommit is clamped to 0.5 through 1.25, minimum health score defaults to 0.70 and maximum preemption rate to 0.08. Then `normalizePools` and `normalizeTenants` validate hard, reject duplicate IDs and negative numbers, then emit warnings for anything they inferred. That includes `estimateModelMemoryGB`, which scans the model name for a number immediately followed by the letter `b` (so `llama-3.3-70b` yields 70) and returns `size * 1.15 + 2.0` GB, plus a default token shape of 2048 prompt and 512 completion tokens when both are omitted.

Placement is a greedy pass in a stable priority order: `mustRun` first, then descending `priority`, then ID as a deterministic tiebreak. For each tenant, `rankCandidates` runs every pool through `evaluateCandidate`, which applies the hard filters in order: remaining leases, availability window via `poolAvailable`, region policy, residency tag intersection, confidential compute, spot acceptance, health floor and preemption ceiling. A pool that fails records a readable reason instead of disappearing.

Sizing is `findReplicaCount`. It walks replica counts upward from `max(1, minReplicas)` to the lower of `maxReplicas` and the pool's remaining leases, and returns the first count that satisfies memory, throughput and latency at once. Memory comes from `replicaMemoryGB`: model weights, adapter weights, runtime overhead and KV cache, where per replica concurrency is `ceil(peakConcurrentRequests / replicas)` and KV cache per request falls back to `estimateKVCacheMB` (sequence length times model GB times 0.00018, clamped between 32 MB and 4096 MB). Required throughput comes from `requiredThroughputTPS`, the larger of the rate driven number (RPS times sequence tokens) and a Little's Law style concurrency number (concurrency times sequence tokens divided by the latency budget, discounted by 0.75). Latency comes from `predictedP99LatencyMs`, a 15 ms fixed overhead plus service time multiplied by a queueing penalty of `1 + u^4 / max(0.05, 1 - u)`, where `u` is utilization clamped at 0.98. That penalty is the standard queueing blow up shape: nearly free below 60 percent utilization, vertical near saturation, which is exactly what a linear capacity model misses.

Surviving candidates are then priced. Cost is replicas times hourly price, carbon is `carbonGramsPerHour` (replicas times watts, divided by 1000, times grams of CO2 per kWh). Tenant budget and policy carbon ceiling are checked here and can still reject a candidate. `scoreCandidate` produces the final weighted sum from five sub scores in the 0 to 1 range: cost against budget, carbon against ceiling, latency against the tenant p99 target, reliability as `healthScore * (1 - preemptionRate)` with a 0.75 multiplier applied to spot pools when `preferReservedCapacity` is set and locality as 1.0 in region or 0.35 cross region. Candidates sort by score descending, then cost, then carbon, then pool ID, so the output is reproducible for identical input.

The winner mutates its `poolState` in place, decrementing `remaining` and accumulating cost, carbon and tenant IDs. That mutation is why priority order matters. High priority tenants take the best capacity, later tenants plan against what is left and a `PoolUtilization` record per pool shows what was consumed.

## Usage

```bash
# build
go build -o gpu-lease-planner TenantGpuLeasePlanner.go

# print a working example request you can edit
./gpu-lease-planner --example --pretty > request.json

# plan against it
./gpu-lease-planner --pretty < request.json

# or in one shot, no build step
go run TenantGpuLeasePlanner.go --pretty < request.json

# CI gate: fail if any tenant could not be placed
go run TenantGpuLeasePlanner.go < request.json \
  | jq -e '.summary.tenantsRejected == 0'
```

Request shape, trimmed to the required fields:

```json
{
  "now": "2026-04-15T12:00:00Z",
  "policy": { "allowCrossRegion": true, "explainRejected": true, "maxCarbonGramsPerHour": 900 },
  "pools": [
    { "id": "iad-h100-reserved", "region": "us-east-1", "gpuType": "h100-80gb",
      "count": 8, "vramGb": 80, "tokensPerSecond": 9800, "hourlyPriceUsd": 3.90,
      "powerWatts": 700, "carbonIntensityGco2Kwh": 370, "healthScore": 0.99,
      "confidentialCompute": true, "residencyTags": ["us"] }
  ],
  "tenants": [
    { "id": "rag-prod", "model": "llama-3.3-70b", "region": "us-east-1",
      "promptTokens": 6000, "completionTokens": 800, "requestsPerSecond": 6.5,
      "peakConcurrentRequests": 96, "p99LatencyMs": 1800,
      "budgetPerHourUsd": 30, "priority": 90 }
  ]
}
```

## Notes

- Greedy, not optimal. Tenants are placed one at a time in priority order with no backtracking, so a lower priority tenant can be rejected even though a different global assignment would have fit everything.
- One pool per tenant. A tenant's replicas all land in a single pool. There is no splitting across pools or regions, and no tensor or pipeline parallelism across cards: `findReplicaCount` requires one replica to fit in one GPU's VRAM times the overcommit ratio.
- The latency model is an estimate, not a benchmark. Service time comes from the pool's declared `tokensPerSecond` for a single replica plus a queueing penalty. Batching effects, prefill versus decode split and continuous batching are not modelled. Feed it measured throughput, not vendor peak numbers.
- Carbon is reported as zero when either `powerWatts` or `carbonIntensityGco2Kwh` is missing, so a partially filled fleet description will understate emissions rather than error.
- Availability windows are best effort. `poolAvailable` treats an unparseable `availableFrom` or `availableUntil` as no constraint instead of failing.
- `summary.usedCrossRegionFallback` compares the allocated region against the tenant's declared region, so tenants that omit `region` entirely will flip that flag true. Set a region on every tenant if you rely on it.
- Exit code 1 with a message on stderr for any input or validation failure: empty stdin, oversized input, unknown JSON fields, duplicate IDs, missing pools or tenants, or a model with no `modelMemoryGb` and no parseable size in its name. A successful run exits 0 even when every tenant was rejected, so check `summary.tenantsRejected` yourself.
- Standard library only. No modules, no config file, no network calls, no state between runs.
