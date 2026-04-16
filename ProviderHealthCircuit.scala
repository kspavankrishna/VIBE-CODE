import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

final class ProviderHealthCircuit(
    providerConfigs: Seq[ProviderHealthCircuit.ProviderConfig],
    settings: ProviderHealthCircuit.Settings = ProviderHealthCircuit.Settings()
) {
  import ProviderHealthCircuit._

  require(providerConfigs.nonEmpty, "providerConfigs must not be empty")

  private val configsByName: Map[String, ProviderConfig] =
    providerConfigs.map(normalizeConfig).map(config => config.name -> config).toMap

  require(configsByName.size == providerConfigs.size, "provider names must be unique after normalization")

  private val providers: mutable.LinkedHashMap[String, MutableProvider] = {
    val table = mutable.LinkedHashMap.empty[String, MutableProvider]
    configsByName.valuesIterator.foreach { config =>
      table.put(config.name, MutableProvider.fromConfig(config))
    }
    table
  }

  private val leases = mutable.HashMap.empty[Long, Lease]
  private val sequence = new AtomicLong(0L)
  private val lock = new AnyRef

  def checkout(request: CheckoutRequest = CheckoutRequest()): Either[SelectionFailure, Checkout] =
    lock.synchronized {
      val nowMs = request.nowMs.getOrElse(currentTimeMillis())
      refreshCircuits(nowMs)

      chooseCandidate(request, nowMs) match {
        case Some((provider, score)) =>
          provider.inFlight += 1
          if (provider.circuitState == CircuitState.HalfOpen) {
            provider.halfOpenProbes += 1
          }

          val leaseId = sequence.incrementAndGet()
          val lease = Lease(leaseId, provider.name, nowMs, request)
          leases.put(leaseId, lease)

          Right(
            Checkout(
              leaseId = leaseId,
              provider = provider.name,
              baseUrl = provider.baseUrl,
              metadata = provider.metadata,
              score = score,
              circuitState = provider.circuitState
            )
          )

        case None =>
          Left(
            SelectionFailure(
              retryAfterMs = retryAfterAcrossProviders(nowMs),
              providers = snapshotUnsafe(nowMs)
            )
          )
      }
    }

  def complete(leaseId: Long, outcome: Outcome): Boolean =
    lock.synchronized {
      val nowMs = outcome.nowMs.getOrElse(currentTimeMillis())
      refreshCircuits(nowMs)

      leases.remove(leaseId) match {
        case Some(lease) =>
          providers.get(lease.provider).exists { provider =>
            provider.inFlight = math.max(provider.inFlight - 1, 0)
            val settledOutcome =
              if (outcome.latencyMs.isDefined) outcome
              else outcome.copy(latencyMs = Some(math.max(nowMs - lease.startedAtMs, 0L)))
            applyOutcome(provider, settledOutcome, nowMs)
            true
          }
        case None =>
          false
      }
    }

  def record(providerName: String, outcome: Outcome): Boolean =
    lock.synchronized {
      val nowMs = outcome.nowMs.getOrElse(currentTimeMillis())
      refreshCircuits(nowMs)
      providers.get(normalizeName(providerName)).exists { provider =>
        applyOutcome(provider, outcome, nowMs)
        true
      }
    }

  def snapshot(): Vector[ProviderSnapshot] =
    lock.synchronized {
      val nowMs = currentTimeMillis()
      refreshCircuits(nowMs)
      snapshotUnsafe(nowMs)
    }

  def providerSnapshot(providerName: String): Option[ProviderSnapshot] =
    lock.synchronized {
      val nowMs = currentTimeMillis()
      refreshCircuits(nowMs)
      providers.get(normalizeName(providerName)).map(snapshotFor(_, nowMs))
    }

  def reset(providerName: Option[String] = None): Unit =
    lock.synchronized {
      providerName.map(normalizeName) match {
        case Some(name) =>
          providers.get(name).foreach { provider =>
            resetProvider(provider)
            dropLeasesFor(name)
          }
        case None =>
          providers.valuesIterator.foreach(resetProvider)
          leases.clear()
      }
    }

  private def chooseCandidate(
      request: CheckoutRequest,
      nowMs: Long
  ): Option[(MutableProvider, Double)] = {
    val requiredCapabilities = request.requiredCapabilities.map(normalizeName)
    val requiredTags = request.requiredTags.map(normalizeName)
    val preferred = request.preferred.map(normalizeName)
    val excluded = request.excluded.map(normalizeName)

    providers.valuesIterator
      .filter(provider =>
        !excluded.contains(provider.name) &&
          requiredCapabilities.subsetOf(provider.capabilities) &&
          requiredTags.subsetOf(provider.tags) &&
          request.maxCostPer1k.forall(maxCost => provider.costPer1k.forall(_ <= maxCost)) &&
          isAvailable(provider)
      )
      .map { provider =>
        provider -> scoreProvider(
          provider = provider,
          strategy = request.strategy,
          preferred = preferred,
          stickyKey = request.stickyKey.orElse(request.tenant),
          nowMs = nowMs
        )
      }
      .toVector
      .sortBy { case (provider, score) => (-score, provider.name) }
      .headOption
  }

  private def applyOutcome(provider: MutableProvider, outcome: Outcome, nowMs: Long): Unit = {
    val classification = classifyOutcome(outcome)

    outcome.latencyMs.foreach { latencyMs =>
      provider.ewmaLatencyMs = Some(ewma(provider.ewmaLatencyMs.getOrElse(latencyMs.toDouble), latencyMs.toDouble, settings.latencyAlpha))
    }

    provider.lastStatus = outcome.status
    provider.lastError = outcome.error
    provider.lastCostPer1k = outcome.costPer1k
    provider.lastOutcomeAtMs = Some(nowMs)

    classification match {
      case OutcomeClass.Success =>
        provider.ewmaSuccess = ewma(provider.ewmaSuccess, 1.0, settings.successAlpha)
        provider.totalSuccesses += 1
        provider.consecutiveFailures = 0
        provider.consecutiveThrottles = 0
        closeCircuit(provider)

      case OutcomeClass.Throttle =>
        provider.ewmaSuccess = ewma(provider.ewmaSuccess, 0.0, settings.successAlpha)
        provider.totalThrottles += 1
        provider.consecutiveFailures += 1
        provider.consecutiveThrottles += 1
        if (
          provider.circuitState == CircuitState.HalfOpen ||
          provider.consecutiveThrottles >= settings.throttleThreshold
        ) {
          openCircuit(provider, nowMs)
        }

      case OutcomeClass.Timeout =>
        provider.ewmaSuccess = ewma(provider.ewmaSuccess, 0.0, settings.successAlpha)
        provider.totalTimeouts += 1
        provider.consecutiveFailures += 1
        provider.consecutiveThrottles = 0
        if (
          provider.circuitState == CircuitState.HalfOpen ||
          provider.consecutiveFailures >= settings.failureThreshold
        ) {
          openCircuit(provider, nowMs)
        }

      case OutcomeClass.Failure =>
        provider.ewmaSuccess = ewma(provider.ewmaSuccess, 0.0, settings.successAlpha)
        provider.totalFailures += 1
        provider.consecutiveFailures += 1
        provider.consecutiveThrottles = 0
        if (
          provider.circuitState == CircuitState.HalfOpen ||
          provider.consecutiveFailures >= settings.failureThreshold
        ) {
          openCircuit(provider, nowMs)
        }
    }
  }

  private def scoreProvider(
      provider: MutableProvider,
      strategy: Strategy,
      preferred: Set[String],
      stickyKey: Option[String],
      nowMs: Long
  ): Double = {
    val successComponent = provider.ewmaSuccess
    val latencyComponent = provider.ewmaLatencyMs match {
      case Some(latencyMs) =>
        val ratio = latencyMs / math.max(settings.latencyBudgetMs.toDouble, 1.0)
        1.0 / (1.0 + ratio)
      case None =>
        1.0
    }

    val loadComponent = provider.maxInFlight match {
      case Some(maxInFlight) =>
        val used = math.min(provider.inFlight.toDouble / math.max(maxInFlight.toDouble, 1.0), 1.0)
        1.0 - used
      case None =>
        1.0
    }

    val costComponent = provider.costPer1k match {
      case Some(cost) =>
        val normalized = math.min(cost / 100.0, 1.0)
        strategy match {
          case Strategy.Cheapest      => -normalized
          case Strategy.LowestLatency => -normalized * 0.25
          case Strategy.Balanced      => -normalized * 0.5
        }
      case None =>
        0.0
    }

    val stickyComponent =
      stickyKey match {
        case Some(key) =>
          val bucket = ((murmurHash(provider.name + ":" + key) & Int.MaxValue) % 10000)
          bucket.toDouble / 10000.0
        case None =>
          0.0
      }

    val preferredBonus = if (preferred.contains(provider.name)) 0.2 else 0.0
    val halfOpenPenalty = if (provider.circuitState == CircuitState.HalfOpen) settings.halfOpenBonus else 0.0
    val freshnessPenalty =
      provider.lastOutcomeAtMs match {
        case Some(lastOutcomeAtMs) =>
          val ageMs = math.max(nowMs - lastOutcomeAtMs, 0L)
          math.min(ageMs.toDouble / 1800000.0, 0.1)
        case None =>
          0.0
      }

    roundScore(
      provider.weight +
        successComponent +
        (latencyComponent * settings.latencyWeight) +
        (loadComponent * settings.loadWeight) +
        (costComponent * settings.costWeight) +
        (stickyComponent * settings.stickyWeight) +
        preferredBonus +
        halfOpenPenalty -
        freshnessPenalty
    )
  }

  private def refreshCircuits(nowMs: Long): Unit = {
    providers.valuesIterator.foreach { provider =>
      if (provider.circuitState == CircuitState.Open && provider.openedUntilMs <= nowMs) {
        provider.circuitState = CircuitState.HalfOpen
        provider.halfOpenProbes = 0
      }
    }
  }

  private def retryAfterAcrossProviders(nowMs: Long): Long =
    providers.valuesIterator
      .flatMap(retryAfter(_, nowMs))
      .toVector
      .sorted
      .headOption
      .getOrElse(settings.halfOpenAfterMs)

  private def retryAfter(provider: MutableProvider, nowMs: Long): Option[Long] =
    provider.circuitState match {
      case CircuitState.Open =>
        Some(math.max(provider.openedUntilMs - nowMs, 0L))
      case CircuitState.Closed | CircuitState.HalfOpen =>
        provider.maxInFlight match {
          case Some(maxInFlight) if provider.inFlight >= maxInFlight => Some(250L)
          case _                                                     => None
        }
    }

  private def isAvailable(provider: MutableProvider): Boolean =
    provider.circuitState match {
      case CircuitState.Open =>
        false
      case CircuitState.HalfOpen =>
        capacityReady(provider) && provider.halfOpenProbes < settings.halfOpenProbeLimit
      case CircuitState.Closed =>
        capacityReady(provider)
    }

  private def capacityReady(provider: MutableProvider): Boolean =
    provider.maxInFlight.forall(maxInFlight => provider.inFlight < maxInFlight)

  private def openCircuit(provider: MutableProvider, nowMs: Long): Unit = {
    provider.reopenAttempts += 1
    val multiplier = math.pow(settings.backoffMultiplier, math.max(provider.reopenAttempts - 1, 0))
    val delayMs = math.min(math.round(settings.halfOpenAfterMs.toDouble * multiplier), settings.maxOpenMs.toLong)

    provider.circuitState = CircuitState.Open
    provider.openedUntilMs = nowMs + delayMs
    provider.halfOpenProbes = 0
  }

  private def closeCircuit(provider: MutableProvider): Unit = {
    provider.circuitState = CircuitState.Closed
    provider.openedUntilMs = 0L
    provider.reopenAttempts = 0
    provider.halfOpenProbes = 0
  }

  private def resetProvider(provider: MutableProvider): Unit = {
    provider.circuitState = CircuitState.Closed
    provider.openedUntilMs = 0L
    provider.reopenAttempts = 0
    provider.halfOpenProbes = 0
    provider.consecutiveFailures = 0
    provider.consecutiveThrottles = 0
    provider.totalSuccesses = 0L
    provider.totalFailures = 0L
    provider.totalThrottles = 0L
    provider.totalTimeouts = 0L
    provider.inFlight = 0
    provider.ewmaLatencyMs = None
    provider.ewmaSuccess = 1.0
    provider.lastStatus = None
    provider.lastError = None
    provider.lastCostPer1k = None
    provider.lastOutcomeAtMs = None
  }

  private def dropLeasesFor(providerName: String): Unit = {
    val staleLeaseIds = leases.valuesIterator.filter(_.provider == providerName).map(_.leaseId).toVector
    staleLeaseIds.foreach(leases.remove)
  }

  private def snapshotUnsafe(nowMs: Long): Vector[ProviderSnapshot] =
    providers.valuesIterator.toVector.sortBy(_.name).map(snapshotFor(_, nowMs))

  private def snapshotFor(provider: MutableProvider, nowMs: Long): ProviderSnapshot =
    ProviderSnapshot(
      name = provider.name,
      baseUrl = provider.baseUrl,
      tags = provider.tags,
      capabilities = provider.capabilities,
      weight = provider.weight,
      costPer1k = provider.costPer1k,
      maxInFlight = provider.maxInFlight,
      inFlight = provider.inFlight,
      circuitState = provider.circuitState,
      retryAfterMs = retryAfter(provider, nowMs),
      consecutiveFailures = provider.consecutiveFailures,
      consecutiveThrottles = provider.consecutiveThrottles,
      totalSuccesses = provider.totalSuccesses,
      totalFailures = provider.totalFailures,
      totalThrottles = provider.totalThrottles,
      totalTimeouts = provider.totalTimeouts,
      ewmaLatencyMs = provider.ewmaLatencyMs.map(roundScore),
      ewmaSuccess = roundScore(provider.ewmaSuccess),
      lastStatus = provider.lastStatus,
      lastError = provider.lastError,
      lastCostPer1k = provider.lastCostPer1k,
      lastOutcomeAtMs = provider.lastOutcomeAtMs,
      liveScore = scoreProvider(provider, Strategy.Balanced, Set.empty, None, nowMs)
    )

  private def normalizeConfig(config: ProviderConfig): ProviderConfig = {
    val name = normalizeName(config.name)
    require(name.nonEmpty, "provider name must not be blank")
    require(config.weight > 0.0, s"provider weight must be > 0 for $name")
    require(config.maxInFlight.forall(_ > 0), s"provider maxInFlight must be > 0 for $name")
    require(config.costPer1k.forall(_ >= 0.0), s"provider costPer1k must be >= 0 for $name")

    config.copy(
      name = name,
      baseUrl = config.baseUrl.map(_.trim).filter(_.nonEmpty),
      tags = config.tags.map(normalizeName),
      capabilities = config.capabilities.map(normalizeName),
      metadata = config.metadata,
      weight = config.weight
    )
  }

  private def classifyOutcome(outcome: Outcome): OutcomeClass = {
    val status = outcome.status
    val error = outcome.error.map(_.trim).filter(_.nonEmpty)

    if (outcome.timeout || error.exists(isTimeoutError) || status.exists(TimeoutStatuses.contains)) {
      OutcomeClass.Timeout
    } else if (outcome.throttle || status.exists(ThrottleStatuses.contains)) {
      OutcomeClass.Throttle
    } else if (error.nonEmpty || status.exists(_ >= 400)) {
      OutcomeClass.Failure
    } else {
      OutcomeClass.Success
    }
  }

  private def normalizeName(value: String): String = value.trim.toLowerCase

  private def isTimeoutError(error: String): Boolean = {
    val normalized = error.toLowerCase
    TimeoutErrors.contains(normalized) || normalized.contains("timeout")
  }

  private def ewma(current: Double, sample: Double, alpha: Double): Double =
    (current * (1.0 - alpha)) + (sample * alpha)

  private def roundScore(value: Double): Double =
    BigDecimal(value).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble

  private def murmurHash(value: String): Int = scala.util.hashing.MurmurHash3.stringHash(value)

  private def currentTimeMillis(): Long = System.currentTimeMillis()
}

object ProviderHealthCircuit {
  sealed trait CircuitState
  object CircuitState {
    case object Closed extends CircuitState
    case object Open extends CircuitState
    case object HalfOpen extends CircuitState
  }

  sealed trait Strategy
  object Strategy {
    case object Balanced extends Strategy
    case object Cheapest extends Strategy
    case object LowestLatency extends Strategy
  }

  sealed trait OutcomeClass
  object OutcomeClass {
    case object Success extends OutcomeClass
    case object Failure extends OutcomeClass
    case object Throttle extends OutcomeClass
    case object Timeout extends OutcomeClass
  }

  final case class ProviderConfig(
      name: String,
      baseUrl: Option[String] = None,
      tags: Set[String] = Set.empty,
      capabilities: Set[String] = Set.empty,
      metadata: Map[String, String] = Map.empty,
      weight: Double = 1.0,
      costPer1k: Option[Double] = None,
      maxInFlight: Option[Int] = None
  )

  final case class Settings(
      failureThreshold: Int = 4,
      throttleThreshold: Int = 2,
      halfOpenAfterMs: Long = 15000L,
      maxOpenMs: Long = 180000L,
      backoffMultiplier: Double = 2.0,
      latencyAlpha: Double = 0.20,
      successAlpha: Double = 0.15,
      latencyBudgetMs: Long = 2500L,
      costWeight: Double = 0.15,
      latencyWeight: Double = 0.25,
      loadWeight: Double = 0.20,
      stickyWeight: Double = 0.10,
      halfOpenBonus: Double = -0.15,
      halfOpenProbeLimit: Int = 1
  ) {
    require(failureThreshold > 0, "failureThreshold must be > 0")
    require(throttleThreshold > 0, "throttleThreshold must be > 0")
    require(halfOpenAfterMs > 0L, "halfOpenAfterMs must be > 0")
    require(maxOpenMs >= halfOpenAfterMs, "maxOpenMs must be >= halfOpenAfterMs")
    require(backoffMultiplier >= 1.0, "backoffMultiplier must be >= 1.0")
    require(latencyAlpha > 0.0 && latencyAlpha <= 1.0, "latencyAlpha must be between 0 and 1")
    require(successAlpha > 0.0 && successAlpha <= 1.0, "successAlpha must be between 0 and 1")
    require(latencyBudgetMs > 0L, "latencyBudgetMs must be > 0")
    require(costWeight >= 0.0, "costWeight must be >= 0")
    require(latencyWeight >= 0.0, "latencyWeight must be >= 0")
    require(loadWeight >= 0.0, "loadWeight must be >= 0")
    require(stickyWeight >= 0.0, "stickyWeight must be >= 0")
    require(halfOpenProbeLimit > 0, "halfOpenProbeLimit must be > 0")
  }

  final case class CheckoutRequest(
      requiredCapabilities: Set[String] = Set.empty,
      requiredTags: Set[String] = Set.empty,
      preferred: Set[String] = Set.empty,
      excluded: Set[String] = Set.empty,
      maxCostPer1k: Option[Double] = None,
      strategy: Strategy = Strategy.Balanced,
      stickyKey: Option[String] = None,
      tenant: Option[String] = None,
      nowMs: Option[Long] = None
  )

  final case class Outcome(
      status: Option[Int] = None,
      error: Option[String] = None,
      latencyMs: Option[Long] = None,
      costPer1k: Option[Double] = None,
      throttle: Boolean = false,
      timeout: Boolean = false,
      nowMs: Option[Long] = None
  )

  final case class Checkout(
      leaseId: Long,
      provider: String,
      baseUrl: Option[String],
      metadata: Map[String, String],
      score: Double,
      circuitState: CircuitState
  )

  final case class Lease(
      leaseId: Long,
      provider: String,
      startedAtMs: Long,
      request: CheckoutRequest
  )

  final case class ProviderSnapshot(
      name: String,
      baseUrl: Option[String],
      tags: Set[String],
      capabilities: Set[String],
      weight: Double,
      costPer1k: Option[Double],
      maxInFlight: Option[Int],
      inFlight: Int,
      circuitState: CircuitState,
      retryAfterMs: Option[Long],
      consecutiveFailures: Int,
      consecutiveThrottles: Int,
      totalSuccesses: Long,
      totalFailures: Long,
      totalThrottles: Long,
      totalTimeouts: Long,
      ewmaLatencyMs: Option[Double],
      ewmaSuccess: Double,
      lastStatus: Option[Int],
      lastError: Option[String],
      lastCostPer1k: Option[Double],
      lastOutcomeAtMs: Option[Long],
      liveScore: Double
  )

  final case class SelectionFailure(
      retryAfterMs: Long,
      providers: Vector[ProviderSnapshot]
  )

  private final case class MutableProvider(
      name: String,
      baseUrl: Option[String],
      tags: Set[String],
      capabilities: Set[String],
      metadata: Map[String, String],
      weight: Double,
      costPer1k: Option[Double],
      maxInFlight: Option[Int],
      var circuitState: CircuitState,
      var openedUntilMs: Long,
      var reopenAttempts: Int,
      var halfOpenProbes: Int,
      var consecutiveFailures: Int,
      var consecutiveThrottles: Int,
      var totalSuccesses: Long,
      var totalFailures: Long,
      var totalThrottles: Long,
      var totalTimeouts: Long,
      var inFlight: Int,
      var ewmaLatencyMs: Option[Double],
      var ewmaSuccess: Double,
      var lastStatus: Option[Int],
      var lastError: Option[String],
      var lastCostPer1k: Option[Double],
      var lastOutcomeAtMs: Option[Long]
  )

  private object MutableProvider {
    def fromConfig(config: ProviderConfig): MutableProvider =
      MutableProvider(
        name = config.name,
        baseUrl = config.baseUrl,
        tags = config.tags,
        capabilities = config.capabilities,
        metadata = config.metadata,
        weight = config.weight,
        costPer1k = config.costPer1k,
        maxInFlight = config.maxInFlight,
        circuitState = CircuitState.Closed,
        openedUntilMs = 0L,
        reopenAttempts = 0,
        halfOpenProbes = 0,
        consecutiveFailures = 0,
        consecutiveThrottles = 0,
        totalSuccesses = 0L,
        totalFailures = 0L,
        totalThrottles = 0L,
        totalTimeouts = 0L,
        inFlight = 0,
        ewmaLatencyMs = None,
        ewmaSuccess = 1.0,
        lastStatus = None,
        lastError = None,
        lastCostPer1k = None,
        lastOutcomeAtMs = None
      )
  }

  private val TimeoutErrors = Set("timeout", "connect_timeout", "read_timeout", "pool_timeout")
  private val ThrottleStatuses = Set(429, 503, 529)
  private val TimeoutStatuses = Set(408, 504)
}

/*
This solves multi-provider AI failover and circuit breaking for Scala services,
Play backends, Akka or Pekko workers, Kafka consumers, agent runtimes, and JVM
gateways that call OpenAI, Anthropic, Gemini, Groq, DeepSeek, Ollama, or
OpenRouter from one system.

Built because in April 2026 a lot of JVM teams are routing inference across
several providers, but the failure handling is still scattered across retries,
feature flags, ad hoc health checks, and one-off sticky routing code. That
works until 429 spikes, regional incidents, or slow upstreams start causing
budget leaks and latency cliffs.

Use it when you need one place to pick a healthy provider, keep in-flight load
under control, react to throttling and timeout waves, and preserve tenant or
session stickiness without shipping a giant dependency stack.

The trick: it keeps fast in-memory provider state, scores live candidates on
latency, cost, load, and stickiness, then moves providers through closed, open,
and half-open circuit states with exponential recovery windows.

Drop this into a Scala API gateway, inference router, backend-for-frontend,
queue worker, or agent platform where provider outages and rate limits can turn
into expensive retries, broken UX, and noisy incident pages.
*/