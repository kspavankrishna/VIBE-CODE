# Agent Tool Call Circuit Breaker

An LLM agent that keeps calling a dead tool burns real money on every attempt, and a classic consecutive failure circuit breaker never trips because the model rewrites its arguments between retries. This is a single Kotlin file that trips on how expensive the failures were, not just how many there were.

**Language:** Kotlin | **Lines:** 602 | **Added:** 2026-09-02

## What this solves

This solves the specific way tool calls break inside an autonomous agent loop, not just "a flaky HTTP dependency." A plain Hystrix style breaker that counts consecutive failures gets fooled constantly here. The model reads the error, decides the problem was its own query, changes a parameter and calls again. To a naive counter that is a fresh attempt, not a repeat failure, even though the tool is clearly dead. The breaker stays closed and the loop keeps going.

Every one of those doomed calls costs real money. The failure returns to the context window, the model reasons about it, produces another tool call and pays for the whole turn. A search API that starts returning 503 can eat ten or fifteen dollars of tokens before anyone notices, and the failing calls are often the expensive ones: a call that streamed 4000 tokens before erroring costs far more than one that 400s in 30 milliseconds. Existing breakers count both as one failure.

The blast radius is wider in multi tenant setups. One customer with a broken integration generates the failure traffic, and if the breaker is keyed on the tool name alone the trip punishes every other tenant calling that same healthy tool. Your on call engineer sees error rate on a shared route. Finance sees a token bill that does not match usage.

## Why I built it

I kept watching agent loops in a project of mine hammer a search API that had already started 503ing, and none of the circuit breaker libraries I looked at treated "this failure was expensive" as a signal. They only treat it as "this failure happened." Resilience4j and Hystrix give you one knob, failure rate, and one scope, the whole named tool. That is the wrong shape for an agent gateway where the interesting variable is cost per doomed attempt and the interesting scope is tool plus tenant.

The second reason is packaging. This compiles with nothing but the Kotlin standard library and `java.util.concurrent`, so `kotlinc AgentToolCallCircuitBreaker.kt` is enough to try it without adding Resilience4j, a coroutines runtime or a metrics library to the build.

## When to use it

- You run an agent framework or MCP gateway where the model chooses which external tool to call and how many times.
- A tool fails expensively, burning tokens or seconds before erroring, so a failure count understates the damage.
- You are multi tenant and one customer's dead integration keeps tripping a shared route for everyone else.
- Agents in your system mint dynamic tool identities per session, so a static map of breakers would grow without bound.
- You want the agent to stop on its own turn by seeing a `retryAfter` hint in the tool result.
- You need this on the JVM without adding a resilience library to the dependency tree.

## How it works

State lives in `AgentToolCallCircuitBreaker`, one instance per logical tool key, with the usual three states in `CircuitState`. The accounting underneath is a `SlidingWindow`, a fixed size ring buffer of `bucketCount` time buckets covering `windowDuration`. Each bucket holds four numbers: call count, failure count, total cost and failed cost. `slotFor` maps a timestamp to a ring index by integer division, and `rotateIfStale` zeroes a bucket the first time a write lands in a slot whose recorded start no longer matches. That lazy rotation is why an idle breaker costs nothing between calls. There is no sweeper thread.

`WindowTotals` derives the two signals: `failureRate` is failures over calls, and `costWeightedFailureRate` is failed cost over total cost. `evaluateClosedState` trips if either crosses its own threshold, `failureRateThreshold` (default 0.5) or `costWeightedFailureRateThreshold` (default 0.35), and only once `minimumCallsInWindow` calls have landed so one early failure cannot trip anything. Two independent trip conditions is the whole point: a tool that fails cheaply gets retried longer than one that fails expensively, and you set that trade off with two separate numbers.

Cost is caller defined. `execute` takes a `costEstimator` lambda of `(result, error, latencyMillis) -> CallOutcome`, invoked in a `finally` block so it sees either the return value or the thrown exception. That is how you report tokens actually consumed on a call that burned them before failing. `costUnits` must be greater than zero. `executeUnweighted` fixes cost at 1.0 and gives you plain failure rate behaviour.

Admission runs through `admit`, which returns an `Admission` record under a `ReentrantLock`. Closed admits. Open first calls `maybeExpireOpen`, and if `nextProbeAt` has passed it flips to half open so this caller becomes the probe. Otherwise it builds a `CircuitOpenException` carrying the remaining wait and the current cost weighted failure rate, and the tool is never invoked. Half open tries `Semaphore.tryAcquire` against `halfOpenProbePermits`, and a caller that misses gets `CircuitProbeSaturatedException`. That semaphore stops recovery probes from becoming their own thundering herd. Permits are drained and reissued on entry into half open so a stale release cannot inflate the budget.

Backoff is exponential with jitter. `backoffFor` computes `baseOpenDuration * backoffMultiplier^(consecutiveOpens - 1)`, clamps the exponent at 20 so `pow` cannot overflow, caps the result at `maxOpenDuration` and applies plus or minus `jitterFraction` of uniform random jitter so a fleet of processes does not probe in lockstep. `evaluateHalfOpenState` needs `halfOpenSuccessesToClose` consecutive successes to close and reset `consecutiveOpens`, while one failure reopens immediately when `reopenOnFirstProbeFailure` is set.

`AgentToolCallCircuitBreakerRegistry` handles bulkheading. It is a `ConcurrentHashMap` of breakers created on first use by `breakerFor`, plus a second map of per key overrides applied through `configureKey`. Key it on `toolName:tenantId` and one noisy tenant trips only their own circuit. Because agents invent tool identities at runtime, `evictIdle` drops breakers that are both `CLOSED` and idle longer than `idleEvictionAfter`, so a long running process does not grow unboundedly. Call it from a housekeeping task, not the request path. `snapshots` returns a `CircuitSnapshot` list for a metrics endpoint, and an optional `CircuitTransitionListener` fires a `CircuitTransitionEvent` on every state change.

## Usage

```kotlin
// Compile and run the built in demo:
//   kotlinc AgentToolCallCircuitBreaker.kt -include-runtime -d abc.jar && java -jar abc.jar

val registry = AgentToolCallCircuitBreakerRegistry(
    defaultConfig = CircuitBreakerConfig(
        windowDuration = Duration.ofSeconds(60),
        bucketCount = 12,
        minimumCallsInWindow = 8,
        failureRateThreshold = 0.5,
        costWeightedFailureRateThreshold = 0.35,
        baseOpenDuration = Duration.ofSeconds(5),
        maxOpenDuration = Duration.ofMinutes(10),
        halfOpenProbePermits = 1,
        halfOpenSuccessesToClose = 2,
    ),
    listener = CircuitTransitionListener { e ->
        log.warn("circuit ${e.toolKey}: ${e.from} -> ${e.to} costFailRate=${e.recentCostWeightedFailureRate}")
    },
)

// One circuit per tool per tenant, so a noisy tenant trips only their own.
val toolKey = "search_api:tenant-42"

try {
    val result = registry.execute<SearchResult>(
        toolKey = toolKey,
        costEstimator = { res, err, latencyMs ->
            CallOutcome(
                success = err == null,
                costUnits = res?.tokensUsed?.toDouble() ?: tokensBurnedBeforeError(err),
                latencyMillis = latencyMs,
            )
        },
    ) { searchApi.query(args) }
    return toolResultFor(result)
} catch (e: CircuitOpenException) {
    // Hand the retryAfter hint back to the model so it stops instead of retrying.
    return toolErrorFor("tool '${e.toolKey}' is unavailable, retry after ${e.retryAfter.toMillis()}ms")
} catch (e: CircuitProbeSaturatedException) {
    return toolErrorFor("tool '${e.toolKey}' is recovering, another probe is in flight")
}

// Cost weighting not needed? Every call counts as 1.0.
registry.executeUnweighted(toolKey) { searchApi.query(args) }

// Ops overrides and housekeeping.
registry.breakerFor(toolKey).trip("health check failed")
registry.breakerFor(toolKey).reset()
registry.snapshots().forEach { println(it) }
scheduler.scheduleAtFixedRate({ registry.evictIdle() }, 5, 5, MINUTES)
```

## Notes

- State is per process. Nothing is shared across instances, so in a fleet each JVM learns the dependency is down on its own. No Redis, no gossip layer.
- `execute` is blocking. It takes a plain `() -> T` lambda and holds a `ReentrantLock` around admission and outcome recording. It works from a coroutine but is not a suspending API, so pick the dispatcher yourself.
- `CircuitTransitionListener` fires while the breaker lock is held. A slow listener stalls every caller of that key. Keep it to a counter increment or a queue offer, never a network call.
- If `costEstimator` throws, or returns `costUnits <= 0` and fails the `require`, that exception is raised from the `finally` block and masks the tool's own exception. The half open probe permit is released after `recordOutcome`, so a throwing estimator also leaks that permit. Make your estimator total.
- Outcomes landing while the state is already `OPEN` are recorded in the window but ignored for transitions. That is deliberate for stragglers that raced the expiry check.
- The injected `clock` drives window accounting and transitions, but `idleFor` and the last activity timestamp use `System.currentTimeMillis()` directly, so a fake clock in tests will not move idle eviction.
- `configureKey` only takes effect before that key's breaker is created. Calling it later is a no op, and `evictIdle` skips any breaker that is not `CLOSED`.
- The `main` function is a self contained smoke test against `FlakySimulatedTool`, not part of the library surface. It prints the transition log, a final snapshot per key and a counts summary, and always exits 0.
