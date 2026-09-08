# Edge Quota Balancer

One region of your API is throwing 429s while three other regions sit half idle on the same global quota. This is a 150 line Lua script that reads per region quota telemetry as CSV and prints the exact set of quota transfers that would fix it.

**Language:** Lua | **Lines:** 150 | **Added:** 2026-05-24

## What this solves

Rate limits are almost never global. They get sliced per region, per point of presence, per worker pool or per API key, and the slicing is decided once at provisioning time and then forgotten. Traffic is not so obedient. A product launch in Singapore, a bot wave on your Frankfurt edge, a batch job scheduled for 02:00 UTC in one zone: any of these pushes a single slice into its ceiling while the account level quota sits half consumed. The provider then does exactly what it promised and starts rejecting requests in that one region.

The production failure mode looks like this. Error rates climb in one region only. Dashboards show total capacity healthy, so the first diagnosis pass finds nothing. Someone pages the on-call, who has no fast way to answer the question that matters: real shortfall or bad distribution. Under pressure the safe move is buying capacity, so the monthly bill rises permanently to fix a thirty minute skew, and the region holding 40 percent spare never gives any of it up because nobody had a number to justify moving it.

The second failure mode is worse. Someone rebalances by hand, reads usage off a dashboard and pulls quota from a region that looked idle but was idle because it was already failing. Low usage plus a high error rate means the region is dropping traffic, not that it has room. Donating from it deepens the outage, and raw usage hides this completely.

This script answers both questions in about a second from data you already export. It sorts every region into needy or safe donor, sizes and caps each move, and prints a plan you can read, diff or feed into automation. It changes nothing itself, and it never drains a region showing errors.

## Why I built it

Every quota tool I found sits on one of two extremes. Either it is a control plane feature locked inside one vendor, blind to the Cloudflare limit sitting next to the AI gateway limit next to your internal ingest cap, or it is a dashboard that shows utilization and stops, leaving the allocation decision to a human at 3am. Nothing in between took numbers in and gave a decision out.

The other gap was portability. During an incident you want the planner running in a debug container, a jump host or a CI job with nothing installed. Lua needs no package manager, so one dependency free source file is the whole deployment story. CSV on stdin, plan on stdout, exit code as the signal.

## When to use it

- A regional API gateway is returning 429s while your account level quota shows plenty of headroom.
- You are about to file a capacity increase request and want proof that redistribution will not solve it first.
- A nightly job checks quota telemetry and should open a ticket only when a rebalance is actually available.
- You run an edge or IoT ingest fleet where each point of presence has its own limit and demand moves by timezone.
- You want a dry run plan, with a hard ceiling on any single move, before an automated controller touches live quota.

## How it works

Input is CSV on stdin with six fields per row: service, region, limit, used, ewma and error_rate. `read_rows` pulls lines with `io.lines`, skips blanks and skips a header beginning with `service,`. Parsing goes through a small hand written `split` that tracks a `quoted` flag so commas inside quoted cells do not break the row, and every numeric field goes through `number`, which raises a named error instead of silently producing nil. Anything past the sixth column is ignored, and fewer than six columns is a hard error.

The core idea is the `pressure` function, which does not trust `used` on its own. It takes `math.max(row.used, row.ewma)` as projected demand, so a region whose exponentially weighted moving average is climbing counts as busy even when its current sample is low, divides by the limit and adds `error_rate * 2`. That doubled error term is the whole safety argument: a region shedding traffic reports low usage with nonzero errors, and the penalty pushes it back up the ranking so it is never mistaken for spare capacity. A limit of zero or less returns pressure 1.

`plan` groups rows with `group_by_service` so quota only ever moves within one service, the only move a provider will honor. Inside each group rows sort by descending pressure, then split into two lists. A row is needy if `used` crossed `limit * (1 - target_headroom)` or pressure is above the hardcoded 0.92 threshold. A row is a donor only if it sits below `limit * (1 - target_headroom * 2)`, a deliberately stricter bar, and its error rate is under 0.02. That double headroom band stops the planner oscillating: a region cannot donate and then need quota back in the same pass.

Allocation is a greedy pass. For each needy row it computes `deficit` as the amount by which usage exceeds the headroom line, then walks the donor list in pressure order taking `math.min(deficit, spare, opts.max_transfer)` from each, where `spare` is the donor's distance from its own threshold. Each move is recorded with `math.floor(amount)` and state mutates in place: donor `used` up, needy `limit` up, so later needy rows see what earlier ones already claimed. One sweep per service, no convergence loop.

Rendering is either `render_text`, a tab separated table with a header row, or `render_json`, one `{"moves":[...]}` object written through a `json_escape` covering backslashes and quotes. The run sits inside a `pcall`. It exits 2 when the plan holds a move and 0 when empty, so cron can branch without parsing stdout. On error it writes `EdgeQuotaBalancer: <message>` to stderr and exits 64.

## Usage

```bash
# text plan, defaults: 18 percent target headroom, 100000 unit cap per move
lua EdgeQuotaBalancer.lua < quota_snapshot.csv

# JSON plan with a tighter headroom target and a smaller per move ceiling
lua EdgeQuotaBalancer.lua --json --target-headroom 0.25 --max-transfer 50000 < quota_snapshot.csv

# use the exit code: 2 means a rebalance is available, 0 means nothing to do
lua EdgeQuotaBalancer.lua < quota_snapshot.csv > plan.tsv
case $? in 2) echo "rebalance available" ;; 0) echo "balanced" ;; *) echo "input error" ;; esac
```

Input format, header optional:

```csv
service,region,limit,used,ewma,error_rate
inference-api,us-east,100000,94000,96000,0.031
inference-api,eu-west,100000,41000,39500,0.001
inference-api,ap-south,100000,52000,50000,0.004
```

Text output is `service`, `from`, `to`, `amount` separated by tabs. JSON is one object with a `moves` array of the same four fields.

## Notes

- It plans, it does not apply. Nothing calls a provider API, so wiring the output into a control plane is your job and your risk.
- Services are iterated with `pairs`, so service order in the output is not deterministic across runs. Sort downstream if you diff plans.
- One greedy pass per service, no iteration to convergence. Deficit left unfilled after the donor list runs out is dropped silently, not reported.
- Amounts are floored to integers while internal arithmetic keeps full precision, so printed moves can total marginally below the modelled transfer.
- Three constants are hardcoded: the 2x error rate weight, the 0.92 needy threshold and the 0.02 donor error ceiling. Only headroom and the transfer cap are flags.
- The CSV reader strips every double quote character and has no support for escaped quotes inside a quoted field. Exit codes: 0 for no moves, 2 for a plan with moves, 64 for a parse or argument error. Treat 2 as a signal, not a failure.
