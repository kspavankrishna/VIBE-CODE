import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

/**
 * AgentToolCallCircuitBreaker
 *
 * A dependency-free, cost-aware circuit breaker for LLM agent tool-calling loops.
 * See the explanation block at the bottom of this file for the full story.
 */

// ---------------------------------------------------------------------------
// Public exceptions
// ---------------------------------------------------------------------------

/** Thrown instead of invoking the underlying tool when its breaker is OPEN. */
class CircuitOpenException(
    val toolKey: String,
    val retryAfter: Duration,
    val recentFailureRate: Double,
) : RuntimeException(
    "circuit open for '$toolKey': retry after ${retryAfter.toMillis()}ms " +
        "(recent cost-weighted failure rate ${"%.2f".format(recentFailureRate)})"
)

/** Thrown when a HALF_OPEN probe slot is not available; the caller should back off. */
class CircuitProbeSaturatedException(val toolKey: String) :
    RuntimeException("no probe slot available for '$toolKey' while circuit is half-open")

// ---------------------------------------------------------------------------
// Public data types
// ---------------------------------------------------------------------------

enum class CircuitState { CLOSED, OPEN, HALF_OPEN }

/**
 * The outcome of one tool invocation, as reported back to the breaker.
 *
 * @param success            whether the call should count as healthy
 * @param costUnits          caller-defined cost of this attempt (tokens, cents, seconds -
 *                           whatever unit the caller wants to weight failures by). Must be > 0.
 * @param latencyMillis      observed latency, used only for metrics/listeners
 */
data class CallOutcome(
    val success: Boolean,
    val costUnits: Double,
    val latencyMillis: Long,
)

/** Emitted on every state transition so callers can wire metrics/logging/alerting. */
data class CircuitTransitionEvent(
    val toolKey: String,
    val from: CircuitState,
    val to: CircuitState,
    val at: Instant,
    val recentFailureRate: Double,
    val recentCostWeightedFailureRate: Double,
)

fun interface CircuitTransitionListener {
    fun onTransition(event: CircuitTransitionEvent)
}

/** Point-in-time snapshot of one breaker, safe to log or expose on a metrics endpoint. */
data class CircuitSnapshot(
    val toolKey: String,
    val state: CircuitState,
    val totalCalls: Long,
    val totalFailures: Long,
    val recentFailureRate: Double,
    val recentCostWeightedFailureRate: Double,
    val consecutiveOpens: Int,
    val nextProbeAt: Instant?,
    val lastTransitionAt: Instant?,
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/**
 * Tunables for one breaker. Defaults are chosen for a "flaky external HTTP tool called
 * from an autonomous agent loop" workload: quick to trip, quick to probe again, but with
 * exponential backoff so a genuinely dead dependency does not get hammered.
 */
data class CircuitBreakerConfig(
    /** Failures are tracked in fixed-size time buckets that slide as time passes. */
    val windowDuration: Duration = Duration.ofSeconds(60),
    val bucketCount: Int = 12,
    /** Minimum number of calls in the window before the failure rate is trusted at all. */
    val minimumCallsInWindow: Int = 8,
    /** Trip when the *plain* failure rate crosses this fraction (0.0 - 1.0). */
    val failureRateThreshold: Double = 0.5,
    /**
     * Trip when the *cost-weighted* failure rate crosses this fraction. Costed failures
     * (e.g. a call that burned 4000 tokens before erroring) push this up faster than a
     * cheap failure would, so expensive dead ends get cut off sooner than the plain
     * failure-rate threshold alone would allow.
     */
    val costWeightedFailureRateThreshold: Double = 0.35,
    /** Base delay before the first HALF_OPEN probe after tripping. */
    val baseOpenDuration: Duration = Duration.ofSeconds(5),
    /** Ceiling for the exponential backoff applied on repeated trips. */
    val maxOpenDuration: Duration = Duration.ofMinutes(10),
    val backoffMultiplier: Double = 2.0,
    /** +/- fraction of jitter applied to every computed open duration. */
    val jitterFraction: Double = 0.2,
    /** How many concurrent probe calls are allowed through while HALF_OPEN. */
    val halfOpenProbePermits: Int = 1,
    /** Consecutive probe successes required to fully close the circuit again. */
    val halfOpenSuccessesToClose: Int = 2,
    /** A single probe failure while HALF_OPEN re-opens immediately when true. */
    val reopenOnFirstProbeFailure: Boolean = true,
    /** Breakers idle longer than this are eligible for eviction by [AgentToolCallCircuitBreakerRegistry.evictIdle]. */
    val idleEvictionAfter: Duration = Duration.ofHours(2),
) {
    init {
        require(bucketCount >= 3) { "bucketCount must be >= 3 for a meaningful sliding window" }
        require(minimumCallsInWindow >= 1) { "minimumCallsInWindow must be >= 1" }
        require(failureRateThreshold in 0.0..1.0) { "failureRateThreshold must be in [0,1]" }
        require(costWeightedFailureRateThreshold in 0.0..1.0) { "costWeightedFailureRateThreshold must be in [0,1]" }
        require(baseOpenDuration > Duration.ZERO) { "baseOpenDuration must be positive" }
        require(maxOpenDuration >= baseOpenDuration) { "maxOpenDuration must be >= baseOpenDuration" }
        require(backoffMultiplier >= 1.0) { "backoffMultiplier must be >= 1.0" }
        require(jitterFraction in 0.0..0.9) { "jitterFraction must be in [0,0.9]" }
        require(halfOpenProbePermits >= 1) { "halfOpenProbePermits must be >= 1" }
        require(halfOpenSuccessesToClose >= 1) { "halfOpenSuccessesToClose must be >= 1" }
    }

    val bucketDuration: Duration get() = windowDuration.dividedBy(bucketCount.toLong())
}

// ---------------------------------------------------------------------------
// Sliding window bucket accounting
// ---------------------------------------------------------------------------

/**
 * Fixed-size ring of time buckets. Each bucket accumulates raw call counts and cost-weighted
 * failure mass for one slice of the window; buckets whose slot has aged out are cleared lazily
 * on the next write, so idle breakers cost nothing to maintain between calls.
 */
private class SlidingWindow(private val bucketCount: Int, private val bucketMillis: Long) {
    private val bucketStartEpoch = LongArray(bucketCount) { -1L }
    private val calls = LongArray(bucketCount)
    private val failures = LongArray(bucketCount)
    private val costTotal = DoubleArray(bucketCount)
    private val costFailures = DoubleArray(bucketCount)

    private fun slotFor(epochMillis: Long): Int = ((epochMillis / bucketMillis) % bucketCount).toInt()

    private fun rotateIfStale(index: Int, bucketStart: Long) {
        if (bucketStartEpoch[index] != bucketStart) {
            bucketStartEpoch[index] = bucketStart
            calls[index] = 0
            failures[index] = 0
            costTotal[index] = 0.0
            costFailures[index] = 0.0
        }
    }

    fun record(nowMillis: Long, outcome: CallOutcome) {
        val index = slotFor(nowMillis)
        val bucketStart = nowMillis - (nowMillis % bucketMillis)
        rotateIfStale(index, bucketStart)
        calls[index] += 1
        costTotal[index] += outcome.costUnits
        if (!outcome.success) {
            failures[index] += 1
            costFailures[index] += outcome.costUnits
        }
    }

    /** Aggregates every bucket still inside the window as of [nowMillis]. */
    fun snapshot(nowMillis: Long): WindowTotals {
        var totalCalls = 0L
        var totalFailures = 0L
        var totalCost = 0.0
        var totalCostFailures = 0.0
        val oldestValidStart = nowMillis - bucketMillis.toDouble().let { it * bucketCount }.toLong()
        for (i in 0 until bucketCount) {
            val start = bucketStartEpoch[i]
            if (start < 0 || start < oldestValidStart) continue
            totalCalls += calls[i]
            totalFailures += failures[i]
            totalCost += costTotal[i]
            totalCostFailures += costFailures[i]
        }
        return WindowTotals(totalCalls, totalFailures, totalCost, totalCostFailures)
    }
}

private data class WindowTotals(
    val calls: Long,
    val failures: Long,
    val costTotal: Double,
    val costFailures: Double,
) {
    val failureRate: Double get() = if (calls == 0L) 0.0 else failures.toDouble() / calls.toDouble()
    val costWeightedFailureRate: Double get() = if (costTotal <= 0.0) 0.0 else costFailures / costTotal
}

// ---------------------------------------------------------------------------
// One breaker per tool key
// ---------------------------------------------------------------------------

/**
 * A single circuit breaker guarding one logical tool. "Logical tool" is caller-defined - it
 * is typically `toolName` alone, or `toolName + ':' + tenantId` when you want one noisy tenant
 * to trip its own circuit without punishing everyone else calling the same tool (bulkheading).
 */
class AgentToolCallCircuitBreaker internal constructor(
    val toolKey: String,
    private val config: CircuitBreakerConfig,
    private val clock: () -> Instant,
    private val listener: CircuitTransitionListener?,
) {
    private val lock = ReentrantLock()
    private val window = SlidingWindow(config.bucketCount, config.bucketDuration.toMillis())

    @Volatile private var state: CircuitState = CircuitState.CLOSED
    @Volatile private var openedAt: Instant? = null
    @Volatile private var nextProbeAt: Instant? = null
    @Volatile private var consecutiveOpens: Int = 0
    @Volatile private var halfOpenConsecutiveSuccesses: Int = 0
    @Volatile private var lastTransitionAt: Instant? = null

    private val totalCalls = AtomicLong(0)
    private val totalFailures = AtomicLong(0)
    private val lastActivityMillis = AtomicLong(System.currentTimeMillis())
    private val halfOpenPermits = Semaphore(config.halfOpenProbePermits)

    /**
     * Runs [call], gated by the breaker. Throws [CircuitOpenException] without invoking [call]
     * if the circuit is OPEN, or [CircuitProbeSaturatedException] if the circuit is HALF_OPEN
     * and no probe slot is free. On any other path [call] runs exactly once.
     *
     * [costEstimator] converts the successful result (or the caught exception) into a
     * [CallOutcome]; this lets the caller report, say, tokens actually consumed even on
     * a failed call that still burned tokens before erroring out.
     */
    fun <T> execute(costEstimator: (result: T?, error: Throwable?, latencyMillis: Long) -> CallOutcome, call: () -> T): T {
        lastActivityMillis.set(System.currentTimeMillis())
        val admitted = admit()
        if (!admitted.allowed) {
            throw admitted.rejection ?: error("unreachable: rejected admission without a reason")
        }
        val acquiredProbePermit = admitted.acquiredProbePermit
        val start = System.nanoTime()
        var result: T? = null
        var error: Throwable? = null
        try {
            result = call()
            return result
        } catch (t: Throwable) {
            error = t
            throw t
        } finally {
            val latencyMillis = (System.nanoTime() - start) / 1_000_000
            val outcome = costEstimator(result, error, latencyMillis)
            require(outcome.costUnits > 0.0) { "costUnits must be > 0; report a small nominal cost for free calls" }
            recordOutcome(outcome)
            if (acquiredProbePermit) halfOpenPermits.release()
        }
    }

    /** Convenience overload for callers that do not care about cost weighting: cost is fixed at 1.0. */
    fun <T> executeUnweighted(call: () -> T): T =
        execute({ _, error, _ -> CallOutcome(success = error == null, costUnits = 1.0, latencyMillis = 0) }, call)

    fun snapshot(): CircuitSnapshot = lock.withLock {
        val totals = window.snapshot(clock().toEpochMilli())
        CircuitSnapshot(
            toolKey = toolKey,
            state = state,
            totalCalls = totalCalls.get(),
            totalFailures = totalFailures.get(),
            recentFailureRate = totals.failureRate,
            recentCostWeightedFailureRate = totals.costWeightedFailureRate,
            consecutiveOpens = consecutiveOpens,
            nextProbeAt = nextProbeAt,
            lastTransitionAt = lastTransitionAt,
        )
    }

    fun idleFor(): Duration = Duration.ofMillis(max(0, System.currentTimeMillis() - lastActivityMillis.get()))

    /** Manually force the circuit open, e.g. from an out-of-band health check or an ops override. */
    fun trip(reason: String) = lock.withLock {
        if (state != CircuitState.OPEN) transitionTo(CircuitState.OPEN, forcedReason = reason)
    }

    /** Manually force the circuit closed, discarding backoff state. Use sparingly. */
    fun reset() = lock.withLock {
        consecutiveOpens = 0
        halfOpenConsecutiveSuccesses = 0
        transitionTo(CircuitState.CLOSED, forcedReason = "manual reset")
    }

    // -- internals -----------------------------------------------------------------------

    private data class Admission(val allowed: Boolean, val acquiredProbePermit: Boolean, val rejection: RuntimeException?)

    private fun admit(): Admission = lock.withLock {
        maybeExpireOpen()
        return when (state) {
            CircuitState.CLOSED -> Admission(true, false, null)
            CircuitState.OPEN -> {
                val wait = Duration.between(clock(), nextProbeAt ?: clock()).let { if (it.isNegative) Duration.ZERO else it }
                val totals = window.snapshot(clock().toEpochMilli())
                Admission(false, false, CircuitOpenException(toolKey, wait, totals.costWeightedFailureRate))
            }
            CircuitState.HALF_OPEN -> {
                if (halfOpenPermits.tryAcquire()) {
                    Admission(true, true, null)
                } else {
                    Admission(false, false, CircuitProbeSaturatedException(toolKey))
                }
            }
        }
    }

    /** If OPEN and the backoff has elapsed, move to HALF_OPEN so the next call can probe. */
    private fun maybeExpireOpen() {
        if (state == CircuitState.OPEN) {
            val at = nextProbeAt
            if (at != null && !clock().isBefore(at)) {
                transitionTo(CircuitState.HALF_OPEN)
            }
        }
    }

    private fun recordOutcome(outcome: CallOutcome) {
        totalCalls.incrementAndGet()
        if (!outcome.success) totalFailures.incrementAndGet()
        lock.withLock {
            window.record(clock().toEpochMilli(), outcome)
            when (state) {
                CircuitState.CLOSED -> evaluateClosedState()
                CircuitState.HALF_OPEN -> evaluateHalfOpenState(outcome)
                CircuitState.OPEN -> Unit // a straggling call can land after expiry raced us; ignore for state purposes
            }
        }
    }

    private fun evaluateClosedState() {
        val totals = window.snapshot(clock().toEpochMilli())
        if (totals.calls < config.minimumCallsInWindow) return
        val tripOnPlainRate = totals.failureRate >= config.failureRateThreshold
        val tripOnCostRate = totals.costWeightedFailureRate >= config.costWeightedFailureRateThreshold
        if (tripOnPlainRate || tripOnCostRate) {
            transitionTo(CircuitState.OPEN)
        }
    }

    private fun evaluateHalfOpenState(outcome: CallOutcome) {
        if (outcome.success) {
            halfOpenConsecutiveSuccesses += 1
            if (halfOpenConsecutiveSuccesses >= config.halfOpenSuccessesToClose) {
                consecutiveOpens = 0
                transitionTo(CircuitState.CLOSED)
            }
        } else if (config.reopenOnFirstProbeFailure) {
            transitionTo(CircuitState.OPEN)
        }
    }

    private fun transitionTo(target: CircuitState, forcedReason: String? = null) {
        val from = state
        if (from == target && forcedReason == null) return
        state = target
        lastTransitionAt = clock()
        halfOpenConsecutiveSuccesses = 0
        when (target) {
            CircuitState.OPEN -> {
                consecutiveOpens += 1
                openedAt = clock()
                nextProbeAt = clock().plus(backoffFor(consecutiveOpens))
            }
            CircuitState.HALF_OPEN -> {
                halfOpenPermits.drainPermits()
                repeat(config.halfOpenProbePermits) { halfOpenPermits.release() }
            }
            CircuitState.CLOSED -> {
                openedAt = null
                nextProbeAt = null
            }
        }
        val totals = window.snapshot(clock().toEpochMilli())
        listener?.onTransition(
            CircuitTransitionEvent(
                toolKey = toolKey,
                from = from,
                to = target,
                at = lastTransitionAt!!,
                recentFailureRate = totals.failureRate,
                recentCostWeightedFailureRate = totals.costWeightedFailureRate,
            )
        )
    }

    private fun backoffFor(attempt: Int): Duration {
        val exponent = min(attempt - 1, 20) // guard against overflow in pow() for pathological attempt counts
        val raw = config.baseOpenDuration.toMillis() * config.backoffMultiplier.pow(exponent)
        val capped = min(raw, config.maxOpenDuration.toMillis().toDouble())
        val jitterSpan = capped * config.jitterFraction
        val jittered = capped + Random.nextDouble(-jitterSpan, jitterSpan)
        return Duration.ofMillis(max(0.0, jittered).toLong())
    }
}

// ---------------------------------------------------------------------------
// Registry: bulkheaded breakers keyed by tool identity, with idle eviction
// ---------------------------------------------------------------------------

/**
 * Owns one [AgentToolCallCircuitBreaker] per key, created on first use. Safe for concurrent
 * access from many agent-loop coroutines/threads calling many distinct tools at once.
 *
 * Agents often mint dynamic tool identities (per-session MCP server names, per-tenant routes),
 * so [evictIdle] is provided to bound memory growth in long-running processes; call it from a
 * periodic housekeeping task rather than on every request.
 */
class AgentToolCallCircuitBreakerRegistry(
    private val defaultConfig: CircuitBreakerConfig = CircuitBreakerConfig(),
    private val clock: () -> Instant = Instant::now,
    private val listener: CircuitTransitionListener? = null,
) {
    private val breakers = ConcurrentHashMap<String, AgentToolCallCircuitBreaker>()
    private val perKeyConfig = ConcurrentHashMap<String, CircuitBreakerConfig>()

    /** Overrides the config for one key before its breaker is first created. No-op afterwards. */
    fun configureKey(toolKey: String, config: CircuitBreakerConfig) {
        perKeyConfig[toolKey] = config
    }

    fun breakerFor(toolKey: String): AgentToolCallCircuitBreaker =
        breakers.computeIfAbsent(toolKey) { key ->
            AgentToolCallCircuitBreaker(key, perKeyConfig[key] ?: defaultConfig, clock, listener)
        }

    fun <T> execute(
        toolKey: String,
        costEstimator: (result: T?, error: Throwable?, latencyMillis: Long) -> CallOutcome,
        call: () -> T,
    ): T = breakerFor(toolKey).execute(costEstimator, call)

    fun <T> executeUnweighted(toolKey: String, call: () -> T): T = breakerFor(toolKey).executeUnweighted(call)

    fun snapshots(): List<CircuitSnapshot> = breakers.values.map { it.snapshot() }

    /** Removes breakers that have seen no traffic for longer than their configured idle TTL. */
    fun evictIdle(): Int {
        var evicted = 0
        val iterator = breakers.entries.iterator()
        while (iterator.hasNext()) {
            val (key, breaker) = iterator.next()
            val ttl = (perKeyConfig[key] ?: defaultConfig).idleEvictionAfter
            if (breaker.snapshot().state == CircuitState.CLOSED && breaker.idleFor() > ttl) {
                iterator.remove()
                evicted++
            }
        }
        return evicted
    }
}

// ---------------------------------------------------------------------------
// Demo / smoke test - run with: kotlinc AgentToolCallCircuitBreaker.kt -include-runtime -d abc.jar && java -jar abc.jar
// ---------------------------------------------------------------------------

private class FlakySimulatedTool(private val failFor: IntRange, private val costOnFailure: Double) {
    private var call = 0
    fun invoke(): String {
        call++
        if (call in failFor) throw RuntimeException("simulated upstream 503 on call #$call")
        return "ok-$call"
    }
    fun estimatedCost(failed: Boolean): Double = if (failed) costOnFailure else 1.0
}

fun main() {
    val events = mutableListOf<String>()
    val registry = AgentToolCallCircuitBreakerRegistry(
        defaultConfig = CircuitBreakerConfig(
            windowDuration = Duration.ofSeconds(10),
            bucketCount = 5,
            minimumCallsInWindow = 4,
            failureRateThreshold = 0.5,
            costWeightedFailureRateThreshold = 0.4,
            baseOpenDuration = Duration.ofMillis(200),
            maxOpenDuration = Duration.ofSeconds(2),
            halfOpenSuccessesToClose = 2,
        ),
        listener = CircuitTransitionListener { event ->
            events += "[${event.at}] ${event.toolKey}: ${event.from} -> ${event.to} " +
                "(failRate=${"%.2f".format(event.recentFailureRate)}, " +
                "costFailRate=${"%.2f".format(event.recentCostWeightedFailureRate)})"
        },
    )

    val tool = FlakySimulatedTool(failFor = 3..9, costOnFailure = 40.0)
    val toolKey = "search_api:tenant-42"

    var successes = 0
    var openRejections = 0
    var probeRejections = 0
    var otherFailures = 0

    repeat(40) { attempt ->
        try {
            val result = registry.execute<String>(
                toolKey = toolKey,
                costEstimator = { _, error, _ -> CallOutcome(success = error == null, costUnits = if (error != null) 40.0 else 1.0, latencyMillis = 0) },
            ) { tool.invoke() }
            successes++
            println("attempt $attempt -> $result")
        } catch (e: CircuitOpenException) {
            openRejections++
            println("attempt $attempt -> short-circuited, retry after ${e.retryAfter.toMillis()}ms")
        } catch (e: CircuitProbeSaturatedException) {
            probeRejections++
            println("attempt $attempt -> probe slot busy")
        } catch (e: RuntimeException) {
            otherFailures++
            println("attempt $attempt -> tool error: ${e.message}")
        }
        if (attempt % 6 == 0) Thread.sleep(250) // let backoff windows actually elapse in the demo
    }

    println()
    println("--- transition log ---")
    events.forEach(::println)

    println()
    println("--- final snapshot ---")
    registry.snapshots().forEach { snap ->
        println(
            "${snap.toolKey}: state=${snap.state} totalCalls=${snap.totalCalls} " +
                "totalFailures=${snap.totalFailures} recentFailRate=${"%.2f".format(snap.recentFailureRate)} " +
                "recentCostFailRate=${"%.2f".format(snap.recentCostWeightedFailureRate)} " +
                "consecutiveOpens=${snap.consecutiveOpens}"
        )
    }

    println()
    println("summary: successes=$successes openRejections=$openRejections probeRejections=$probeRejections otherFailures=$otherFailures")
}

// ---------------------------------------------------------------------------
// EXPLANATION (read me before you drop this in)
// ---------------------------------------------------------------------------
//
// This solves the specific way tool calls break inside an autonomous agent loop, not just
// "a flaky HTTP dependency." When an LLM agent is driving tool calls, a plain Hystrix-style
// breaker that only counts consecutive failures gets fooled constantly, because the model
// keeps changing its arguments between retries, so the failures never look "consecutive" to
// a naive counter even though the underlying tool is clearly dead. Worse, every one of those
// doomed calls costs real money: the model reads the error, reasons about it, and tries again,
// which is tokens spent chasing a dependency that was never coming back this minute.
//
// Built because I kept watching agent loops in a project of mine burn ten or fifteen dollars
// of tokens hammering a search API that had already started 503ing, and none of the circuit
// breaker libraries I found treated "this failure was expensive" as a signal, only "this
// failure happened." So this one tracks two failure rates side by side in a sliding window:
// the plain fraction of calls that failed, and a cost-weighted fraction where you tell it how
// expensive each attempt was. A tool that fails cheaply can keep getting retried longer than
// one that fails expensively, and you control that trade-off with two separate thresholds
// instead of one.
//
// Use it when you're building or operating an agent framework, an MCP server gateway, or any
// backend that lets an LLM call external tools, APIs, sub-agents, or other services on its own,
// and you want a single dependency-free Kotlin file you can paste into a JVM project without
// pulling in Resilience4j, Hystrix, or a coroutines runtime. It also fits multi-tenant setups
// well: key the registry by "toolName:tenantId" and one noisy customer's dead integration trips
// only their own circuit, not everyone else calling the same tool.
//
// The trick is the two-signal trip condition plus the registry-level bulkheading. Most breakers
// give you one knob (failure rate) and one scope (the whole tool). This gives you failure rate
// and cost-weighted failure rate as independent trip conditions, and lets every distinct key
// carry its own state, backoff, and half-open probe budget, so a single flaky tenant or a single
// expensive failing tool doesn't take down calls to a healthy one that happens to share a name.
// The half-open state also uses a semaphore to cap how many probe calls can be in flight at
// once, so recovery checks don't themselves become a thundering herd against a tool that's just
// coming back up.
//
// Drop this into any JVM backend, Ktor service, Spring Boot app, or plain Kotlin CLI that
// orchestrates LLM tool calls, MCP tool invocations, or agent-to-agent RPCs. Wrap each outbound
// call in `registry.execute(toolKey, costEstimator) { theActualCall() }`, catch
// CircuitOpenException at the boundary where you format the tool result back to the model, and
// surface the retryAfter hint in that error message so the agent stops immediately instead of
// spending another turn on a dependency you already know is down. It compiles standalone with
// nothing but the Kotlin standard library, so `kotlinc AgentToolCallCircuitBreaker.kt` is enough
// to try it - no build file, no dependency resolution, no version conflicts to sort out first.
