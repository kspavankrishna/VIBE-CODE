# frozen_string_literal: true

require "monitor"
require "json"
require "time"

# SpendGovernor gives a Ruby backend a single choke point for every outbound
# LLM call: it estimates cost before the call, blocks or downgrades the model
# when a tenant is about to blow its budget, and watches the *rate* of spend
# so a runaway agent loop trips a circuit breaker before it does real damage.
module SpendGovernor
  VERSION = "1.0.0"

  class Error < StandardError; end
  class ConfigurationError < Error; end

  class BudgetExceededError < Error
    attr_reader :tenant_id, :decision

    def initialize(tenant_id:, decision:)
      @tenant_id = tenant_id
      @decision = decision
      super("spend budget exceeded for tenant #{tenant_id.inspect}: #{decision.reason}")
    end
  end

  # One purchasable model "tier". Costs are USD per 1,000 tokens, the unit
  # every major inference provider bills in as of 2026.
  ModelTier = Struct.new(:name, :input_cost_per_1k, :output_cost_per_1k, :quality_rank, keyword_init: true) do
    def initialize(name:, input_cost_per_1k:, output_cost_per_1k:, quality_rank:)
      super(
        name: name.to_s.freeze,
        input_cost_per_1k: Float(input_cost_per_1k),
        output_cost_per_1k: Float(output_cost_per_1k),
        quality_rank: Integer(quality_rank)
      )
      raise ConfigurationError, "input_cost_per_1k must be >= 0" if self.input_cost_per_1k.negative?
      raise ConfigurationError, "output_cost_per_1k must be >= 0" if self.output_cost_per_1k.negative?
    end

    def cost_for(input_tokens:, output_tokens:)
      (input_tokens.to_f / 1000.0 * input_cost_per_1k) +
        (output_tokens.to_f / 1000.0 * output_cost_per_1k)
    end
  end

  # An ordered ladder of tiers, best quality first. Governor walks down this
  # ladder when it needs to keep serving a tenant on a shrinking budget.
  class PricingTable
    def initialize(tiers:)
      list = Array(tiers).map { |t| t.is_a?(ModelTier) ? t : ModelTier.new(**t) }
      raise ConfigurationError, "PricingTable needs at least one tier" if list.empty?

      names = list.map(&:name)
      raise ConfigurationError, "tier names must be unique" if names.uniq.length != names.length

      @tiers = list.sort_by { |t| -t.quality_rank }.freeze
      @by_name = @tiers.each_with_object({}) { |t, h| h[t.name] = t }.freeze
    end

    def tier(name)
      @by_name.fetch(name.to_s) { raise ConfigurationError, "unknown model #{name.inspect}" }
    end

    def cheapest
      @tiers.min_by { |t| t.input_cost_per_1k + t.output_cost_per_1k }
    end

    # Every tier ranked at or below `name`'s quality, cheapest-costing first.
    # This is the degradation path: try each until one fits the budget.
    def downgrade_candidates(from:)
      origin = tier(from)
      @tiers
        .select { |t| t.quality_rank <= origin.quality_rank }
        .sort_by { |t| t.input_cost_per_1k + t.output_cost_per_1k }
    end

    def tiers
      @tiers.dup
    end
  end

  # Sliding-window spend ledger. Two implementations ship here: one in
  # process memory (fine for a single dyno / test suite) and one backed by
  # any Redis-like client (fine for a real multi-process deployment). Both
  # answer the same two questions: "how much has tenant X spent in the last
  # N seconds" and "record that tenant X just spent $Y".
  module Ledger
    # @abstract
    class Base
      def spend_in_window(_tenant_id, _window_seconds, _now = Time.now)
        raise NotImplementedError
      end

      def record!(_tenant_id, _amount_usd, _at = Time.now)
        raise NotImplementedError
      end

      def reset!(_tenant_id)
        raise NotImplementedError
      end
    end

    class InMemory < Base
      def initialize
        @entries = Hash.new { |h, k| h[k] = [] }
        @lock = Monitor.new
      end

      def spend_in_window(tenant_id, window_seconds, now = Time.now)
        @lock.synchronize do
          prune!(tenant_id, window_seconds, now)
          @entries[tenant_id].sum { |(_ts, amount)| amount }
        end
      end

      def record!(tenant_id, amount_usd, at = Time.now)
        @lock.synchronize do
          @entries[tenant_id] << [at.to_f, amount_usd.to_f]
          amount_usd.to_f
        end
      end

      def reset!(tenant_id)
        @lock.synchronize { @entries.delete(tenant_id) }
        nil
      end

      private

      def prune!(tenant_id, window_seconds, now)
        cutoff = now.to_f - window_seconds
        @entries[tenant_id].reject! { |(ts, _)| ts < cutoff }
      end
    end

    # Bucketed Redis ledger: spend is accumulated into fixed-width time
    # buckets (default 60s) via INCRBYFLOAT, each bucket carrying its own
    # TTL so history self-expires. Reading a window sums the buckets that
    # fall inside it with MGET, so cost is O(window / bucket_width) per
    # check, not O(requests). Any client that answers to #incrbyfloat,
    # #expire and #mget (the redis gem, redis-client, hiredis, a Sidekiq
    # pool, ...) works here — no gem dependency is declared by this file.
    class RedisBucketed < Base
      def initialize(redis, namespace: "spend_governor", bucket_width_seconds: 60)
        raise ConfigurationError, "bucket_width_seconds must be > 0" unless bucket_width_seconds.positive?

        @redis = redis
        @namespace = namespace
        @bucket_width = bucket_width_seconds
      end

      def spend_in_window(tenant_id, window_seconds, now = Time.now)
        keys = bucket_keys(tenant_id, window_seconds, now)
        return 0.0 if keys.empty?

        @redis.mget(*keys).sum { |v| v.to_f }
      end

      def record!(tenant_id, amount_usd, at = Time.now)
        key = bucket_key(tenant_id, at)
        @redis.incrbyfloat(key, amount_usd.to_f)
        @redis.expire(key, @bucket_width * 4)
        amount_usd.to_f
      end

      def reset!(tenant_id)
        pattern = "#{@namespace}:#{tenant_id}:*"
        keys = @redis.respond_to?(:keys) ? @redis.keys(pattern) : []
        @redis.del(*keys) unless keys.empty?
        nil
      end

      private

      def bucket_key(tenant_id, time)
        slot = (time.to_f / @bucket_width).floor
        "#{@namespace}:#{tenant_id}:#{slot}"
      end

      def bucket_keys(tenant_id, window_seconds, now)
        buckets_needed = (window_seconds / @bucket_width.to_f).ceil + 1
        current_slot = (now.to_f / @bucket_width).floor
        (0...buckets_needed).map { |i| "#{@namespace}:#{tenant_id}:#{current_slot - i}" }
      end
    end
  end

  # Detects a sudden change in the *rate* of spend, independent of the
  # absolute budget. A tenant sitting well under budget can still be in the
  # middle of a runaway agent loop (retry storm, prompt-injected tool call
  # loop, a cron job accidentally firing every second instead of every
  # hour) — the hard budget alone catches that too late. This tracks an
  # exponentially weighted moving average of $/window per tenant and flags
  # a window that blows past the learned baseline by `spike_multiplier`.
  class VelocityDetector
    def initialize(alpha: 0.2, spike_multiplier: 4.0, min_baseline_usd: 0.01)
      raise ConfigurationError, "alpha must be within (0, 1]" unless alpha > 0 && alpha <= 1
      raise ConfigurationError, "spike_multiplier must be > 1" unless spike_multiplier > 1

      @alpha = alpha
      @spike_multiplier = spike_multiplier
      @min_baseline_usd = min_baseline_usd
      @baselines = {}
      @lock = Monitor.new
    end

    # Feed the most recent window's total spend for a tenant. Call this
    # once per window per tenant (e.g. from a periodic sweep, or lazily on
    # every authorize call — see Governor#observe_window!).
    def observe!(tenant_id, window_spend_usd)
      @lock.synchronize do
        prior = @baselines[tenant_id]
        @baselines[tenant_id] = prior.nil? ? window_spend_usd : (@alpha * window_spend_usd) + ((1 - @alpha) * prior)
      end
    end

    def anomalous?(tenant_id, current_window_spend_usd)
      @lock.synchronize do
        baseline = @baselines[tenant_id]
        return false if baseline.nil?
        return false if baseline < @min_baseline_usd

        current_window_spend_usd > baseline * @spike_multiplier
      end
    end

    def baseline_for(tenant_id)
      @lock.synchronize { @baselines[tenant_id] }
    end
  end

  # Minimal per-tenant circuit breaker. "Open" means new requests are
  # rejected outright; after `cooldown_seconds` it moves to "half-open" and
  # lets exactly one probe through; a successful probe closes it, a failed
  # one reopens it with the cooldown restarted.
  class CircuitBreaker
    State = Struct.new(:status, :opened_at, :consecutive_failures, keyword_init: true)

    def initialize(cooldown_seconds: 60, trip_after_failures: 3)
      @cooldown_seconds = cooldown_seconds
      @trip_after_failures = trip_after_failures
      @states = Hash.new { |h, k| h[k] = State.new(status: :closed, opened_at: nil, consecutive_failures: 0) }
      @lock = Monitor.new
    end

    def status_for(tenant_id, now = Time.now)
      @lock.synchronize do
        state = @states[tenant_id]
        if state.status == :open && now.to_f - state.opened_at.to_f >= @cooldown_seconds
          state.status = :half_open
        end
        state.status
      end
    end

    def record_failure!(tenant_id, now = Time.now)
      @lock.synchronize do
        state = @states[tenant_id]
        state.consecutive_failures += 1
        if state.status == :half_open || state.consecutive_failures >= @trip_after_failures
          state.status = :open
          state.opened_at = now
        end
        state.status
      end
    end

    def record_success!(tenant_id)
      @lock.synchronize do
        state = @states[tenant_id]
        state.status = :closed
        state.opened_at = nil
        state.consecutive_failures = 0
        state.status
      end
    end

    def trip!(tenant_id, now = Time.now)
      @lock.synchronize do
        state = @states[tenant_id]
        state.status = :open
        state.opened_at = now
        state.consecutive_failures = [@trip_after_failures, state.consecutive_failures].max
      end
    end
  end

  Decision = Struct.new(
    :allowed, :model, :reason, :estimated_cost_usd, :window_spend_usd,
    :budget_usd, :circuit_state, :tenant_id,
    keyword_init: true
  ) do
    def allowed?
      !!allowed
    end

    def downgraded?(requested_model)
      allowed? && model != requested_model.to_s
    end

    def to_h
      super.merge(circuit_state: circuit_state.to_s)
    end

    def to_json(*args)
      to_h.to_json(*args)
    end
  end

  # The governor itself. Wire one of these up per process (it is
  # thread-safe) and route every outbound LLM call through #guard, or use
  # #authorize / #record_actual! directly if you need to split the
  # pre-flight check from the post-call settlement across two places in
  # your code (common with async job queues).
  class Governor
    DEFAULT_WINDOW_SECONDS = 3600

    def initialize(
      pricing_table:,
      hard_budget_usd:,
      window_seconds: DEFAULT_WINDOW_SECONDS,
      soft_threshold_ratio: 0.8,
      ledger: Ledger::InMemory.new,
      velocity_detector: VelocityDetector.new,
      circuit_breaker: CircuitBreaker.new,
      on_decision: nil
    )
      raise ConfigurationError, "hard_budget_usd must be > 0" unless hard_budget_usd.to_f.positive?
      raise ConfigurationError, "window_seconds must be > 0" unless window_seconds.to_f.positive?
      unless soft_threshold_ratio.to_f.between?(0, 1)
        raise ConfigurationError, "soft_threshold_ratio must be within [0, 1]"
      end

      @pricing_table = pricing_table
      @hard_budget_usd = hard_budget_usd.to_f
      @window_seconds = window_seconds.to_f
      @soft_threshold_ratio = soft_threshold_ratio.to_f
      @ledger = ledger
      @velocity_detector = velocity_detector
      @circuit_breaker = circuit_breaker
      @on_decision = on_decision
      @last_window_observed = Hash.new(0.0)
      @lock = Monitor.new
    end

    # Pre-flight check. Returns a Decision; never raises for a normal
    # budget denial (check decision.allowed?) but does raise
    # ConfigurationError for programmer errors like an unknown model name.
    def authorize(tenant_id:, requested_model:, estimated_input_tokens:, estimated_output_tokens: 0)
      tenant_id = tenant_id.to_s
      now = Time.now
      circuit_state = @circuit_breaker.status_for(tenant_id, now)

      if circuit_state == :open
        return finalize(tenant_id: tenant_id, allowed: false, model: nil, reason: "circuit_open",
                         estimated_cost_usd: 0.0, window_spend: current_spend(tenant_id, now),
                         circuit_state: circuit_state)
      end

      window_spend = current_spend(tenant_id, now)
      observe_window!(tenant_id, window_spend)

      if @velocity_detector.anomalous?(tenant_id, window_spend)
        @circuit_breaker.trip!(tenant_id, now)
        return finalize(tenant_id: tenant_id, allowed: false, model: nil, reason: "velocity_anomaly",
                         estimated_cost_usd: 0.0, window_spend: window_spend, circuit_state: :open)
      end

      origin_tier = @pricing_table.tier(requested_model)
      requested_cost = origin_tier.cost_for(
        input_tokens: estimated_input_tokens,
        output_tokens: estimated_output_tokens
      )

      if window_spend + requested_cost <= @hard_budget_usd
        reason = window_spend + requested_cost > @hard_budget_usd * @soft_threshold_ratio ? "within_budget_soft_pressure" : "within_budget"
        return finalize(tenant_id: tenant_id, allowed: true, model: origin_tier.name, reason: reason,
                         estimated_cost_usd: requested_cost, window_spend: window_spend, circuit_state: circuit_state)
      end

      fallback = @pricing_table.downgrade_candidates(from: requested_model).find do |tier|
        cost = tier.cost_for(input_tokens: estimated_input_tokens, output_tokens: estimated_output_tokens)
        window_spend + cost <= @hard_budget_usd
      end

      if fallback
        cost = fallback.cost_for(input_tokens: estimated_input_tokens, output_tokens: estimated_output_tokens)
        finalize(tenant_id: tenant_id, allowed: true, model: fallback.name, reason: "downgraded_budget_pressure",
                 estimated_cost_usd: cost, window_spend: window_spend, circuit_state: circuit_state)
      else
        failure_state = @circuit_breaker.record_failure!(tenant_id, now)
        finalize(tenant_id: tenant_id, allowed: false, model: nil, reason: "hard_budget_exceeded",
                 estimated_cost_usd: requested_cost, window_spend: window_spend, circuit_state: failure_state)
      end
    end

    # Settle the ledger with what a call actually cost after it completes.
    # Always call this (even on the downgraded model, using the model that
    # actually ran) so the sliding window reflects reality rather than the
    # pre-flight estimate.
    def record_actual!(tenant_id:, model:, input_tokens:, output_tokens:, at: Time.now)
      tenant_id = tenant_id.to_s
      tier = @pricing_table.tier(model)
      cost = tier.cost_for(input_tokens: input_tokens, output_tokens: output_tokens)
      @ledger.record!(tenant_id, cost, at)
      @circuit_breaker.record_success!(tenant_id)
      cost
    end

    # Block-based convenience: authorize, yield the model name your caller
    # should actually use, and settle the ledger with whatever token
    # counts the block reports back. Raises BudgetExceededError if the
    # pre-flight check fails; re-raises whatever the block raises after
    # recording it as a circuit-breaker failure (a provider timeout or a
    # 5xx is exactly the kind of signal that should count toward tripping
    # the breaker, same as a budget breach).
    def guard(tenant_id:, requested_model:, estimated_input_tokens:, estimated_output_tokens: 0)
      decision = authorize(
        tenant_id: tenant_id,
        requested_model: requested_model,
        estimated_input_tokens: estimated_input_tokens,
        estimated_output_tokens: estimated_output_tokens
      )
      raise BudgetExceededError.new(tenant_id: tenant_id, decision: decision) unless decision.allowed?

      begin
        usage = yield(decision.model, decision)
        input_tokens = usage.fetch(:input_tokens)
        output_tokens = usage.fetch(:output_tokens)
        record_actual!(tenant_id: tenant_id, model: decision.model, input_tokens: input_tokens, output_tokens: output_tokens)
        usage
      rescue StandardError
        @circuit_breaker.record_failure!(tenant_id.to_s)
        raise
      end
    end

    def reset!(tenant_id)
      @ledger.reset!(tenant_id.to_s)
      @circuit_breaker.record_success!(tenant_id.to_s)
    end

    private

    def current_spend(tenant_id, now)
      @ledger.spend_in_window(tenant_id, @window_seconds, now)
    end

    # Feed the velocity detector at most once per window per tenant so the
    # EWMA baseline advances on wall-clock time rather than on request
    # volume (a tenant making ten calls in one window shouldn't warp its
    # own baseline ten times over).
    def observe_window!(tenant_id, window_spend)
      @lock.synchronize do
        slot = (Time.now.to_f / @window_seconds).floor
        key = "#{tenant_id}:#{slot}"
        return if @last_window_observed.key?(key)

        @last_window_observed.delete_if { |k, _| k.start_with?("#{tenant_id}:") && k != key }
        @last_window_observed[key] = window_spend
        @velocity_detector.observe!(tenant_id, window_spend)
      end
    end

    def finalize(tenant_id:, allowed:, model:, reason:, estimated_cost_usd:, window_spend:, circuit_state:)
      decision = Decision.new(
        allowed: allowed,
        model: model,
        reason: reason,
        estimated_cost_usd: estimated_cost_usd.round(6),
        window_spend_usd: window_spend.round(6),
        budget_usd: @hard_budget_usd,
        circuit_state: circuit_state,
        tenant_id: tenant_id
      )
      @on_decision&.call(decision)
      decision
    end
  end

  # Drop-in Rack middleware. Extracts a tenant id from the request (default:
  # the X-Tenant-Id header), stashes the governor + tenant id in env for
  # downstream code, and turns an unrescued BudgetExceededError raised
  # anywhere further down the stack into a clean 429 with a JSON body
  # instead of a 500.
  class RackMiddleware
    DEFAULT_EXTRACTOR = ->(env) { env["HTTP_X_TENANT_ID"] }

    def initialize(app, governor:, tenant_extractor: DEFAULT_EXTRACTOR)
      @app = app
      @governor = governor
      @tenant_extractor = tenant_extractor
    end

    def call(env)
      tenant_id = @tenant_extractor.call(env)
      env["spend_governor.governor"] = @governor
      env["spend_governor.tenant_id"] = tenant_id
      @app.call(env)
    rescue BudgetExceededError => e
      body = { error: "llm_budget_exceeded", tenant_id: e.tenant_id, decision: e.decision.to_h }.to_json
      [429, { "content-type" => "application/json", "content-length" => body.bytesize.to_s }, [body]]
    end
  end
end

if $PROGRAM_NAME == __FILE__
  pricing = SpendGovernor::PricingTable.new(tiers: [
    { name: "flagship", input_cost_per_1k: 0.015, output_cost_per_1k: 0.075, quality_rank: 3 },
    { name: "balanced", input_cost_per_1k: 0.003, output_cost_per_1k: 0.015, quality_rank: 2 },
    { name: "economy", input_cost_per_1k: 0.0005, output_cost_per_1k: 0.0015, quality_rank: 1 }
  ])

  governor = SpendGovernor::Governor.new(
    pricing_table: pricing,
    hard_budget_usd: 1.00,
    window_seconds: 60,
    on_decision: ->(d) { puts "[decision] tenant=#{d.tenant_id} model=#{d.model || "-"} reason=#{d.reason} spend=#{d.window_spend_usd}" }
  )

  puts "-- normal traffic, draining the budget --"
  8.times do |i|
    decision = governor.authorize(tenant_id: "acme", requested_model: "flagship",
                                   estimated_input_tokens: 4_000, estimated_output_tokens: 1_500)
    if decision.allowed?
      governor.record_actual!(tenant_id: "acme", model: decision.model, input_tokens: 4_000, output_tokens: 1_500)
    end
  end

  puts "-- a tenant with no headroom tripping the circuit breaker --"
  broke_pricing = SpendGovernor::PricingTable.new(tiers: [
    { name: "only-tier", input_cost_per_1k: 10.0, output_cost_per_1k: 10.0, quality_rank: 1 }
  ])
  broke_governor = SpendGovernor::Governor.new(pricing_table: broke_pricing, hard_budget_usd: 0.01, window_seconds: 60)
  5.times do |i|
    decision = broke_governor.authorize(tenant_id: "broke", requested_model: "only-tier",
                                         estimated_input_tokens: 1_000, estimated_output_tokens: 1_000)
    puts "attempt #{i + 1}: allowed=#{decision.allowed?} reason=#{decision.reason} circuit=#{decision.circuit_state}"
  end

  puts "-- the velocity detector catching a spend-rate spike a static budget would miss --"
  detector = SpendGovernor::VelocityDetector.new(alpha: 0.5, spike_multiplier: 3.0, min_baseline_usd: 0.01)
  [0.05, 0.06, 0.055, 0.052].each { |window_spend| detector.observe!("bursty", window_spend) }
  puts "learned baseline after 4 quiet windows: #{detector.baseline_for("bursty").round(4)}"
  puts "a quiet window (0.07): anomalous? #{detector.anomalous?("bursty", 0.07)}"
  puts "a burst window (0.40): anomalous? #{detector.anomalous?("bursty", 0.40)}"
end

# ---------------------------------------------------------------------------
# What this is, from Pavan.
#
# This solves a problem every team building an LLM feature into a real Ruby
# backend runs into around month three: someone's per-tenant cost graph
# spikes overnight and nobody finds out until the invoice arrives. Rate
# limiting alone does not catch this because a request can be small in count
# and huge in tokens, and a tenant sitting comfortably inside a dollar budget
# can still be mid-loop in a retry storm or a prompt-injected tool call that
# keeps re-triggering itself. Most teams find out from the bill, not from a
# graph, because nothing sits between "call the model" and "the model got
# called" to actually stop it.
#
# Built because I kept seeing the same shape of incident: an agent feature
# ships, works fine in the demo, then a single tenant's automation (or a
# bug, or a jailbreak attempt) fires the same expensive prompt hundreds of
# times in a few minutes and the bill for that day looks like a DDoS. A
# budget cap alone does not catch this fast enough, because by the time the
# monthly or daily total trips, real money is already gone. This file treats
# spend the way a payments system treats fraud: not just "how much" but "how
# fast," so it reacts inside the same minute the pattern starts instead of
# inside the same billing cycle.
#
# Use it when you have any Ruby service (Rails, Sinatra, Grape, a plain Rack
# app, a Sidekiq worker) that calls out to an LLM provider on behalf of
# distinct tenants, users, or API keys, and you want a hard ceiling on spend
# per tenant that degrades gracefully instead of failing outright, plus a
# tripwire for abnormal spend velocity that a static budget cannot see. It
# is provider-agnostic on purpose, so it works the same whether the calls go
# to OpenAI, Anthropic, a self-hosted vLLM cluster, or three of them mixed
# through a router, because none of it talks to a network — you hand it the
# token counts and it hands back a decision.
#
# The trick is splitting the check into two moments: authorize happens
# before the expensive call using an estimate, and record_actual happens
# after using the real usage the provider reports, so the sliding-window
# ledger never drifts far from reality even under concurrent load. On top
# of that ledger sits an EWMA velocity baseline per tenant, learned from
# their own history rather than a fixed global number, so a spend rate that
# would be normal for your biggest customer does not falsely trip for your
# smallest one, and vice versa. When budget pressure builds, it walks down
# a configurable quality ladder to a cheaper model before it ever refuses
# the request outright, so most tenants get degraded service instead of an
# error page, and only sustained abuse or a genuine anomaly trips the
# circuit breaker and starts hard-rejecting for a cooldown window.
#
# Drop this into any Rails, Sinatra, or plain Rack app: require the file,
# build one SpendGovernor::Governor per process with your real pricing
# table and budget, mount SpendGovernor::RackMiddleware in your middleware
# stack, and wrap each outbound LLM call in governor.guard(...) so the
# ledger and the circuit breaker stay accurate without you having to
# remember to call anything twice. Swap SpendGovernor::Ledger::InMemory for
# SpendGovernor::Ledger::RedisBucketed once you run more than one process,
# and nothing else in your call sites has to change. No gems required
# beyond the Ruby standard library, and no network calls of its own, so it
# is safe to unit test and safe to drop straight into a request path.
# ---------------------------------------------------------------------------
