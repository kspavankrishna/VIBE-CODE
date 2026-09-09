# Inference Hedge Planner

Multi provider LLM routing goes wrong when one model or region suddenly goes long tail, starts throwing 429s, or quietly becomes the most expensive path for the same request. This is a Go planner that scores every eligible provider on tail latency, cost, error rate, throttle rate and concurrency pressure, then decides whether a hedge request is worth firing and how long to wait before firing it.

**Language:** Go | **Lines:** 1160 | **Added:** 2026-05-16

## What this solves

By April 2026 a lot of production AI systems are no longer single provider. Teams split traffic across OpenAI, Anthropic, Gemini, local gateways and regional failover stacks, but most routing code is still a pile of if statements, static priorities and hand wavy latency guesses. That works until it does not. The usual failure: your primary region starts returning 429 on one call in eight, p95 goes from 1.6 seconds to 4.2 seconds, and the router keeps sending everything there because the config says priority one. Nothing is down. Nothing pages. Users watch a spinner and conversion drops.

The second failure is invisible. Somebody routes a request needing strict JSON mode and tool calling to a deployment that supports neither, or sends 210k tokens at a model with a 200k window. You get a 400, retry logic kicks in, and the same broken request burns three provider calls before anyone reads the logs. This planner kills those requests before they leave the process, with a named reason attached.

The third failure is money. Blind hedging, dual sending the moment latency looks slightly off, roughly doubles the bill on hedged requests. Hedge 30 percent of a workload costing 0.12 USD a call and that is real spend nobody notices until the invoice. Here a hedge must be justified by the primary's own health signals, and the worst case dual send cost is checked against the request budget first. Without that you get the on call engineer who cannot explain why one region eats all the traffic, and a p99 your dashboards call fine because you only chart the mean.

## Why I built it

Existing routing layers each pick one axis and ignore the rest. Load balancers know health but nothing about token pricing or context windows. Gateway configs know priorities but not live concurrency pressure. Client SDK retry logic reacts after a failure instead of predicting one. The hedging that does exist is usually a fixed delay constant somebody guessed once, either too aggressive on healthy providers or too slow to help on sick ones.

I wanted one place holding all of it: capability gating, budget gating, health gating, a scoring model tunable without recompiling, and a hedge delay that moves with the primary's measured tail. It also had to be inspectable, because a router you cannot explain in a postmortem is a router you will eventually rip out.

## When to use it

- You run the same prompt across two or more vendors and need to pick per request, not per deployment.
- Your p95 is fine but your p99 is not, and you want a second lane opened only when the primary's tail actually predicts a miss.
- You have per request budgets and need a provider refused when its estimated cost blows the cap, hedge included.
- A request needs strict JSON, tool calling, reasoning mode, vision or streaming and you want mismatches rejected before the call.
- You want to test routing behaviour in CI by feeding JSON fixtures in and diffing the plan that comes out.
- You run region diverse failover and the hedge must land in a different vendor or region than the primary.

## How it works

The entry point is `PlanRouting(input PlanInput)`. `PlanInput` carries a `RequestShape`, a slice of `ProviderSnapshot` and an optional `PlannerConfig`. Config goes through `Normalize()` first, which fills every zero valued weight from `DefaultPlannerConfig()` and clamps the ratio fields into sane ranges, so a partial config never produces a degenerate scoring model. `validateRequest` rejects negative token counts, negative budgets and throttle caps outside 0 to 1.

Every provider runs through `evaluateCandidate`, a two stage filter. Stage one is hard rejection, and it accumulates reasons rather than bailing on the first, so you see all the problems at once: missing name or model, denied vendor or region, a `DisabledUntil` still in the future, each unmet capability flag, a context window smaller than input plus cached input plus expected output, a max output smaller than expected output, a throttle rate above the request's own cap, headroom below `MinHealthyHeadroom`, saturation or error rate or throttle rate at or above their hard limits, and estimated cost over `MaxCostUSD`. Vendor and region matching goes through `normalizeKey`, so "OpenAI " and "openai" are one key.

Stage two is scoring, a weighted linear sum where lower wins. `EstimateCostUSD` prices uncached input, cached input and output separately per 1K tokens, falling back to the uncached rate when a provider has no cached price. Incomplete latency profiles are backfilled: a missing p50 becomes 0.65 of p95, a missing p95 becomes 1.55 of p50, an absent profile defaults to 450 ms, and p95 is floored at p50. The score adds `LatencyWeight * (p95 / slo)`, `CostWeight * (cost / budget)`, error and throttle rates at their weights, saturation raised to the power 1.35 so crowding hurts superlinearly, `TailWeight * ((p95 - p50) / p50)` to punish spread rather than absolute slowness, a flat `WarmPenalty` for cold deployments and a small penalty from any `RetryAfterMs` the provider last returned. Preferred vendors subtract the full `PreferenceBonus`, preferred regions half of it. `sortCandidates` is a stable sort with a tie break chain of score, cost, p95 then provider name, comparing floats through `nearlyEqual` at 1e-9.

`chooseHedge` is deliberately conservative. It returns nothing unless hedging is allowed, `MaxParallel` is at least 2, a second eligible provider exists and `shouldHedge` fires. `shouldHedge` fires on any one of four triggers: primary error rate, throttle rate or saturation at or above their hedge triggers, or predicted primary p95 at or above `HedgeP95TriggerFraction` of the SLO, 90 percent by default. Candidates that would push combined cost past the budget are dropped. The rest get their score adjusted: same vendor pays `VendorDiversityPenalty`, same region pays `RegionDiversityPenalty`, a diverse vendor or region earns a bonus, and small credits go to candidates with a faster tail or lower cost than the primary.

`computeHedgeDelay` holds the tail awareness. Base delay is `p50 * 0.90 + (p95 - p50) * 0.25 + jitter * 0.50`, so the planner waits out most of a normal response before spending a second call and stretches that wait when spread is wide. Sick primaries shorten it: 0.60 on an error trigger, 0.65 on throttle, 0.75 on saturation, 0.90 when the hedge has the better tail. Reasoning requests stretch it by 1.10. The result is capped at 65 percent of the SLO and clamped into `MinHedgeDelayMs` to `MaxHedgeDelayMs`, 120 ms to 1800 ms by default. `predictHedgedP95` reports the minimum of the primary's p95 and delay plus the hedge's p95.

The file also ships `ObservationStore`, an optional in process feedback loop. `Start(name)` bumps an inflight counter and returns a finisher closure guarded by `sync.Once`. `Record` decays error and throttle rates through `decayEWMA`, a time weighted exponentially weighted moving average keyed on a half life rather than a fixed alpha, so a provider that goes quiet decays toward its last sample instead of freezing. HTTP 429 and 529 both count as throttling. Latencies land in `LatencyWindow`, a ring buffer of 256 samples whose `Quantile` sorts a copy and linearly interpolates. `Apply` and `ApplyAll` merge that live view into your static snapshots.

## Usage

```bash
# Print a complete example PlanInput you can edit
go run InferenceHedgePlanner.go example > input.json

# Plan from a file
go run InferenceHedgePlanner.go plan -input input.json

# Or pipe it in on stdin, compact output
go run InferenceHedgePlanner.go example | go run InferenceHedgePlanner.go plan -pretty=false
```

As a library inside a Go service:

```go
store := NewObservationStore(2 * time.Minute)

done := store.Start("openai-us-east-primary")
// ... issue the request ...
done(Observation{Latency: elapsed, HTTPStatus: 200})

plan, err := PlanRouting(PlanInput{
    Request: RequestShape{
        ID:                     "req_001",
        InputTokens:            14000,
        CachedInputTokens:      4000,
        ExpectedOutputTokens:   1800,
        NeedsJSON:              true,
        NeedsTools:             true,
        LatencySLOMs:           2200,
        MaxCostUSD:             0.16,
        HedgeAllowed:           true,
        MaxParallel:            2,
        RequireVendorDiversity: true,
    },
    Providers: store.ApplyAll(staticSnapshots),
})
// plan.Primary, plan.Hedge, plan.HedgeAfterMs, plan.Reasons, plan.Rejected
```

## Notes

- `PlanRouting` has a typo'd variable in the hedge block: `chooseHedge` is assigned to `headge` then read as `hedge`, and `headgeSelected` is assigned then read as `hedgeSelected`. Rename both before you build, otherwise the file will not compile.
- This plans, it does not execute. No HTTP client, no retry loop, no cancellation of the losing lane. You still write the code that fires the primary, waits `HedgeAfterMs`, fires the hedge and takes the first good answer.
- `ObservationStore` is per process and guarded by a plain mutex, so on a fleet each instance sees only its own traffic. Feed shared telemetry into the static snapshot fields if you need a global view.
- The CLI decoder uses `DisallowUnknownFields` and rejects trailing JSON, so a misspelled key fails loudly instead of being ignored. Deliberate for CI fixtures, annoying if you wanted to pass extra metadata through.
- Scoring is a hand tuned linear model. No learning, no bandit, no feedback on whether a past decision was right. The default weights are a starting point, not a claim about your workload.
- `computeSaturation` returns a flat 0.25 when a provider reports inflight requests but no `ConcurrencyLimit`. Skip concurrency limits and the saturation term is mostly noise.
- `Apply` takes the max of static and observed error and throttle rates, so a stale pessimistic snapshot acts as a floor and live recovery will not show until you update it.
- Exit codes: 2 for a missing or unknown subcommand, which prints usage to stderr, and 1 for any decode, validation or planning error, including no eligible providers after filtering.
