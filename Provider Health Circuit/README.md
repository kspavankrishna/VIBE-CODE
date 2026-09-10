# Provider Health Circuit

One OpenAI region starts returning 429s and your JVM service keeps hammering it for six more minutes because the retry loop has no memory. This is a single file Scala circuit breaker and provider router that remembers which upstream is sick, stops sending traffic there and picks the next healthy one by score.

**Language:** Scala | **Lines:** 666 | **Added:** 2026-04-16

## What this solves

Multi provider AI failover and circuit breaking for Scala services, Play backends, Akka or Pekko workers, Kafka consumers, agent runtimes and JVM gateways that call OpenAI, Anthropic, Gemini, Groq, DeepSeek, Ollama or OpenRouter from one system. The failure mode is familiar. A provider starts throttling, your HTTP client retries with backoff, the retry also gets a 429, and every queued request pays a full timeout before it fails over. p99 goes vertical, the worker pool fills with threads waiting on a dead upstream, and the queue backs up behind it. Nobody notices until the pager fires on consumer lag rather than on the provider itself.

Without state held between requests, every call rediscovers the outage from scratch. Request 1 finds the provider throttling. So does request 2000. You burn money on tokens for calls that get retried anyway, and wall clock on timeouts a health aware router would have skipped. When the provider recovers you also have no controlled way to find out: either it stays blacklisted behind a flag someone flips by hand, or you dump full load onto a half recovered upstream and knock it over again.

The second failure mode is slow rather than broken. A provider does not error, it degrades: p95 goes from 800 ms to 9 seconds. Nothing in a plain circuit breaker trips because there are no exceptions to count. Requests succeed, users wait, streaming stalls. This tracks an EWMA of latency per provider and folds it into the routing score, so a slow provider bleeds traffic before it becomes a hard failure. Cost is the third. Route across providers at different prices and naive failover sends everything to whoever answers first, often the expensive one, which `maxCostPer1k` and `Strategy.Cheapest` exist to stop.

## Why I built it

In April 2026 a lot of JVM teams are routing inference across several providers, but the failure handling is still scattered across retries, feature flags, ad hoc health checks and one off sticky routing code. Each piece is reasonable alone. Together they have no shared view of provider health, so nothing can make the one decision that matters: given what just happened, where does this request go.

Resilience4j gives you a breaker per call site but no notion of a fleet of interchangeable providers, no cost awareness and no tenant stickiness. Service meshes work at the network layer, which does not help when the difference between healthy and sick is a 429 body or a 9 second first token. I wanted one object I hold in a service, ask for a provider, report what happened and get honest state back for a health endpoint. Standard library only.

## When to use it

- Your inference gateway calls three providers for the same model class and needs one place to decide who handles the next request.
- A provider region throws 429s at 3 am and traffic should drain off it in seconds, then probe back automatically instead of a human flipping a flag.
- Per tenant sessions should keep landing on the same provider for cache and conversation continuity, until that provider goes unhealthy.
- Some providers do vision or function calling and others do not, so routing has to filter on capability before scoring.
- You pay different rates per provider and want a hard cost ceiling on background jobs while premium traffic stays on the fast one.
- You need a `/health` endpoint showing real circuit state, in flight count, EWMA latency and failure counters instead of a config dump.

## How it works

Everything lives in one class, `ProviderHealthCircuit`, built from a `Seq[ProviderConfig]` and an optional `Settings`. State sits in a `mutable.LinkedHashMap[String, MutableProvider]` guarded by a single `AnyRef` monitor, and every public method takes that lock, so it is safe to share across threads without a concurrent collection. `normalizeConfig` lowercases every name, tag and capability at construction, and `require` rejects duplicate names, non positive weights and negative costs before the object exists.

The main flow is a lease. `checkout` returns `Either[SelectionFailure, Checkout]`: it refreshes circuits, filters candidates, scores them, increments `inFlight` on the winner, mints a monotonic lease id from an `AtomicLong` and stores a `Lease` holding the provider name and start time. You make the call and pass the result to `complete(leaseId, outcome)`, which decrements `inFlight`, backfills `latencyMs` from the lease start if you did not supply it, then applies the outcome. `record(providerName, outcome)` skips the lease entirely. The lease exists so in flight load is measured rather than guessed, which is what makes the `maxInFlight` cap and the load term mean anything.

Circuit state is the standard three state machine: `Closed`, `Open`, `HalfOpen`. `classifyOutcome` sorts each result into `Success`, `Throttle`, `Timeout` or `Failure` in that order: the `timeout` flag, a timeout looking error string or status 408 or 504 gives Timeout; the `throttle` flag or status 429, 503 or 529 gives Throttle; any non empty error or status at or above 400 gives Failure. Throttles trip faster than plain failures, `throttleThreshold` 2 against `failureThreshold` 4, because a 429 wave says something about the provider and not about your request. `openCircuit` applies exponential backoff: the open window is `halfOpenAfterMs * backoffMultiplier^(reopenAttempts - 1)` clamped to `maxOpenMs`, so defaults give 15s, 30s, 60s, 120s, capped at 180s. `refreshCircuits` runs at the top of every public entry point and flips any Open provider past `openedUntilMs` into HalfOpen. There is no background thread. Time advances only when you call something.

HalfOpen is deliberately narrow. `isAvailable` lets at most `halfOpenProbeLimit` requests through, default 1, and the score adds `halfOpenBonus`, which is negative 0.15, so a probing provider only wins when nothing healthier is free. One bad outcome of any class while HalfOpen reopens the circuit and bumps `reopenAttempts`, lengthening the next window. One success calls `closeCircuit`, zeroing state and the backoff counter.

Scoring is where routing happens. `scoreProvider` sums the static `weight`, an EWMA of success (`successAlpha` 0.15), a latency term `1 / (1 + latency/latencyBudgetMs)` weighted 0.25, a load term `1 - inFlight/maxInFlight` weighted 0.20, a cost penalty normalized against 100 and scaled by strategy (`Cheapest` full, `Balanced` half, `LowestLatency` a quarter), a sticky term, a 0.2 bonus for anything in `preferred`, the HalfOpen penalty, and a freshness penalty capped at 0.1 that decays a provider whose last observed outcome is stale. Stickiness uses MurmurHash3 over `providerName + ":" + stickyKey`, masked positive and bucketed into 10000, giving the same tenant the same deterministic per provider offset on every request with no session store. Weighted at 0.10 it is a nudge, not a pin, so a sick sticky provider still loses. Candidates sort by descending score with name as tiebreak, so selection is deterministic for a given state.

Latency uses a classic EWMA with `latencyAlpha` 0.20, seeded on the first sample rather than from zero, so one slow request does not dominate but a sustained shift moves the score within a handful of calls. Scores pass through `roundScore`, a `BigDecimal` HALF_UP round to six places, so snapshots stay comparable instead of showing float noise. When nothing is selectable, `SelectionFailure` carries `retryAfterMs`, the minimum across providers of the remaining open window or a flat 250 ms for one merely at its in flight cap, plus a snapshot of every provider so the caller can log why.

## Usage

```scala
import ProviderHealthCircuit._

val router = new ProviderHealthCircuit(
  Seq(
    ProviderConfig(
      name = "anthropic",
      baseUrl = Some("https://api.anthropic.com"),
      tags = Set("primary", "us"),
      capabilities = Set("chat", "vision", "tools"),
      metadata = Map("model" -> "claude-sonnet"),
      weight = 1.2,
      costPer1k = Some(3.0),
      maxInFlight = Some(64)
    ),
    ProviderConfig(
      name = "openai",
      baseUrl = Some("https://api.openai.com"),
      capabilities = Set("chat", "vision", "tools"),
      costPer1k = Some(2.5),
      maxInFlight = Some(64)
    ),
    ProviderConfig(name = "groq", capabilities = Set("chat"), costPer1k = Some(0.6))
  ),
  Settings(failureThreshold = 4, throttleThreshold = 2, halfOpenAfterMs = 15000L)
)

router.checkout(
  CheckoutRequest(
    requiredCapabilities = Set("chat", "tools"),
    excluded = Set("groq"),
    maxCostPer1k = Some(4.0),
    strategy = Strategy.Balanced,
    tenant = Some("acme-corp")
  )
) match {
  case Right(checkout) =>
    val res = callProvider(checkout)   // use checkout.baseUrl / checkout.metadata
    router.complete(checkout.leaseId, Outcome(status = Some(res.code), latencyMs = Some(res.tookMs)))

  case Left(failure) =>
    logger.warn(s"no provider available, retry in ${failure.retryAfterMs}ms: ${failure.providers}")
}

// report a throttle without a lease
router.record("openai", Outcome(status = Some(429), throttle = true))

// health endpoint
val states: Vector[ProviderSnapshot] = router.snapshot()
val one: Option[ProviderSnapshot]    = router.providerSnapshot("anthropic")

// clear one provider or the whole table
router.reset(Some("openai"))
router.reset()
```

For deterministic tests, pass `nowMs` on `CheckoutRequest` and `Outcome` to drive the clock yourself instead of `System.currentTimeMillis()`.

## Notes

- Single JVM, in memory only. Nothing is shared across pods and nothing survives a restart. Two replicas learn about an outage independently.
- One global lock around every public method. Selection is O(providers) per checkout, fine for a handful and not built for thousands.
- Leases leak if you never call `complete`. No timeout, no sweeper, so an abandoned lease keeps `inFlight` elevated forever. Wrap the call in try/finally. `reset` is the only way to clear stuck leases.
- No HTTP client, no retries, no hedging, no active health probing. It decides and records. Making the call and retrying elsewhere is yours.
- `snapshot()` and `providerSnapshot()` always use the wall clock and ignore any injected time.
- Timeout detection on error strings is substring matching on "timeout" plus four known values, so differently worded errors classify as plain Failure and trip the slower threshold of 4.
- Cost normalizes against a hardcoded 100.0 and clamps at 1.0, so any two providers above 100 per 1k score identically on cost, and a large `weight` effectively pins a provider until its circuit opens.
