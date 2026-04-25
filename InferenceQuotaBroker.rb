# frozen_string_literal: true

require "securerandom"
require "thread"

class InferenceQuotaBroker
  EPSILON = 1e-9
  REQUEST_UNIT = :requests
  DEFAULT_EXPIRE_STRATEGY = :charge_reserved
  DEFAULT_SETTLEMENT_TIME_SOURCE = :created_at

  class Error < StandardError; end
  class UnknownScopeError < Error; end
  class UnknownLeaseError < Error; end
  class InvalidReservationError < Error; end
  class InvalidPolicyError < Error; end
  class LeaseStateError < Error; end

  WindowLimit = Struct.new(:name, :unit, :capacity, :interval, keyword_init: true) do
    def initialize(name: nil, unit:, capacity:, interval:)
      super(
        name: (name || unit).to_s,
        unit: unit.to_sym,
        capacity: Float(capacity),
        interval: Float(interval)
      )

      raise InvalidPolicyError, "limit capacity must be positive" unless self.capacity.positive?
      raise InvalidPolicyError, "limit interval must be positive" unless self.interval.positive?
    end

    def to_h
      {
        name: name,
        unit: unit,
        capacity: capacity,
        interval: interval
      }
    end
  end

  Policy = Struct.new(
    :name,
    :window_limits,
    :concurrency_limit,
    :lease_ttl,
    :expire_strategy,
    :settlement_time_source,
    keyword_init: true
  ) do
    VALID_EXPIRE_STRATEGIES = [:charge_reserved, :cancel].freeze
    VALID_SETTLEMENT_TIME_SOURCES = [:created_at, :finished_at].freeze

    def initialize(
      name: nil,
      window_limits: [],
      concurrency_limit: nil,
      lease_ttl: 30.0,
      expire_strategy: DEFAULT_EXPIRE_STRATEGY,
      settlement_time_source: DEFAULT_SETTLEMENT_TIME_SOURCE
    )
      limits = Array(window_limits).map do |limit|
        limit.is_a?(WindowLimit) ? limit : WindowLimit.new(**limit)
      end

      super(
        name: name.to_s,
        window_limits: limits.freeze,
        concurrency_limit: concurrency_limit.nil? ? nil : Integer(concurrency_limit),
        lease_ttl: Float(lease_ttl),
        expire_strategy: expire_strategy.to_sym,
        settlement_time_source: settlement_time_source.to_sym
      )

      if !self.concurrency_limit.nil? && self.concurrency_limit <= 0
        raise InvalidPolicyError, "concurrency_limit must be positive when set"
      end

      raise InvalidPolicyError, "lease_ttl must be positive" unless self.lease_ttl.positive?

      unless VALID_EXPIRE_STRATEGIES.include?(self.expire_strategy)
        raise InvalidPolicyError, "expire_strategy must be one of #{VALID_EXPIRE_STRATEGIES.join(", ")}"
      end

      unless VALID_SETTLEMENT_TIME_SOURCES.include?(self.settlement_time_source)
        raise InvalidPolicyError,
              "settlement_time_source must be one of #{VALID_SETTLEMENT_TIME_SOURCES.join(", ")}"
      end

      seen = {}
      self.window_limits.each do |limit|
        key = [limit.name, limit.unit, limit.interval]
        raise InvalidPolicyError, "duplicate limit #{key.inspect}" if seen[key]

        seen[key] = true
      end

      @max_interval_by_unit = nil
    end

    def max_interval_by_unit
      @max_interval_by_unit ||= begin
        mapping = {}

        window_limits.each do |limit|
          current = mapping[limit.unit]
          mapping[limit.unit] = current ? [current, limit.interval].max : limit.interval
        end

        mapping.freeze
      end
    end

    def to_h
      {
        name: name,
        window_limits: window_limits.map(&:to_h),
        concurrency_limit: concurrency_limit,
        lease_ttl: lease_ttl,
        expire_strategy: expire_strategy,
        settlement_time_source: settlement_time_source
      }
    end
  end

  Decision = Struct.new(
    :granted,
    :lease,
    :reused,
    :retry_at,
    :retry_in,
    :reasons,
    :reservation,
    :scopes,
    keyword_init: true
  ) do
    def granted?
      !!granted
    end

    def to_h
      {
        granted: granted?,
        lease: lease && lease.to_h,
        reused: !!reused,
        retry_at: retry_at,
        retry_in: retry_in,
        reasons: reasons,
        reservation: reservation,
        scopes: scopes
      }
    end
  end

  Finalization = Struct.new(
    :lease,
    :state,
    :replayed,
    :violations,
    keyword_init: true
  ) do
    def to_h
      {
        lease: lease && lease.to_h,
        state: state,
        replayed: !!replayed,
        violations: violations
      }
    end
  end

  LeaseView = Struct.new(
    :id,
    :state,
    :scopes,
    :reservation,
    :actual,
    :metadata,
    :idempotency_key,
    :created_at,
    :expires_at,
    :committed_at,
    :cancelled_at,
    :expired_at,
    keyword_init: true
  ) do
    def active?
      state == :active
    end

    def to_h
      {
        id: id,
        state: state,
        scopes: scopes,
        reservation: reservation,
        actual: actual,
        metadata: metadata,
        idempotency_key: idempotency_key,
        created_at: created_at,
        expires_at: expires_at,
        committed_at: committed_at,
        cancelled_at: cancelled_at,
        expired_at: expired_at
      }
    end
  end

  Snapshot = Struct.new(
    :scope,
    :now,
    :policy,
    :active_leases,
    :limits,
    keyword_init: true
  ) do
    def to_h
      {
        scope: scope,
        now: now,
        policy: policy,
        active_leases: active_leases,
        limits: limits
      }
    end
  end

  ScopeState = Struct.new(:policy, :events_by_unit, :active_lease_ids, keyword_init: true)
  UsageEvent = Struct.new(:at, :amount)
  LeaseRecord = Struct.new(
    :id,
    :scopes,
    :reservation,
    :actual,
    :metadata,
    :idempotency_key,
    :created_at,
    :expires_at,
    :state,
    :committed_at,
    :cancelled_at,
    :expired_at,
    keyword_init: true
  )

  def initialize(clock: nil)
    @clock = clock || proc { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
    @mutex = Mutex.new
    @scopes = {}
    @leases = {}
    @idempotency_index = {}
  end

  def register_scope(scope_name, policy)
    normalized_name = normalize_scope_name(scope_name)
    normalized_policy = normalize_policy(policy)

    @mutex.synchronize do
      raise InvalidPolicyError, "scope #{normalized_name.inspect} already exists" if @scopes.key?(normalized_name)

      @scopes[normalized_name] = ScopeState.new(
        policy: normalized_policy,
        events_by_unit: Hash.new { |hash, key| hash[key] = [] },
        active_lease_ids: {}
      )
    end

    self
  end

  def upsert_scope(scope_name, policy)
    normalized_name = normalize_scope_name(scope_name)
    normalized_policy = normalize_policy(policy)

    @mutex.synchronize do
      state = @scopes[normalized_name]

      if state.nil?
        @scopes[normalized_name] = ScopeState.new(
          policy: normalized_policy,
          events_by_unit: Hash.new { |hash, key| hash[key] = [] },
          active_lease_ids: {}
        )
      else
        state.policy = normalized_policy
      end
    end

    self
  end

  def plan(scopes:, reservation:, lease_ttl: nil, deadline_at: nil, deadline_in: nil, now: nil)
    @mutex.synchronize do
      current_time = normalize_now(now)
      sweep_expired!(current_time)
      normalized_scopes = normalize_scope_list(scopes)
      normalized_reservation = normalize_reservation(reservation)
      ttl = resolve_lease_ttl(normalized_scopes, lease_ttl)
      deadline = resolve_deadline(current_time, deadline_at, deadline_in)

      analyze_reservation(
        scopes: normalized_scopes,
        reservation: normalized_reservation,
        lease_ttl: ttl,
        deadline_at: deadline,
        now: current_time
      )
    end
  end

  def reserve(
    scopes:,
    reservation:,
    metadata: {},
    lease_ttl: nil,
    deadline_at: nil,
    deadline_in: nil,
    idempotency_key: nil,
    now: nil
  )
    @mutex.synchronize do
      current_time = normalize_now(now)
      sweep_expired!(current_time)
      normalized_scopes = normalize_scope_list(scopes)
      normalized_reservation = normalize_reservation(reservation)
      ttl = resolve_lease_ttl(normalized_scopes, lease_ttl)
      deadline = resolve_deadline(current_time, deadline_at, deadline_in)
      normalized_metadata = normalize_metadata(metadata)

      unless idempotency_key.nil?
        existing = find_active_idempotent_lease(idempotency_key)

        if existing
          ensure_matching_idempotent_request!(
            lease: existing,
            scopes: normalized_scopes,
            reservation: normalized_reservation
          )

          return Decision.new(
            granted: true,
            lease: lease_view(existing),
            reused: true,
            retry_at: nil,
            retry_in: 0.0,
            reasons: [],
            reservation: normalized_reservation.dup,
            scopes: normalized_scopes.dup
          )
        end
      end

      decision = analyze_reservation(
        scopes: normalized_scopes,
        reservation: normalized_reservation,
        lease_ttl: ttl,
        deadline_at: deadline,
        now: current_time
      )

      return decision unless decision.granted?

      lease = LeaseRecord.new(
        id: SecureRandom.uuid,
        scopes: normalized_scopes.freeze,
        reservation: normalized_reservation.freeze,
        actual: nil,
        metadata: normalized_metadata.freeze,
        idempotency_key: idempotency_key,
        created_at: current_time,
        expires_at: current_time + ttl,
        state: :active,
        committed_at: nil,
        cancelled_at: nil,
        expired_at: nil
      )

      @leases[lease.id] = lease
      normalized_scopes.each { |scope_name| @scopes.fetch(scope_name).active_lease_ids[lease.id] = true }
      @idempotency_index[idempotency_key] = lease.id unless idempotency_key.nil?

      Decision.new(
        granted: true,
        lease: lease_view(lease),
        reused: false,
        retry_at: nil,
        retry_in: 0.0,
        reasons: [],
        reservation: normalized_reservation.dup,
        scopes: normalized_scopes.dup
      )
    end
  end

  def commit(lease_id, actual: nil, now: nil)
    @mutex.synchronize do
      current_time = normalize_now(now)
      sweep_expired!(current_time)
      lease = @leases[lease_id]
      raise UnknownLeaseError, "unknown lease #{lease_id.inspect}" if lease.nil?

      case lease.state
      when :committed
        return Finalization.new(
          lease: lease_view(lease),
          state: :committed,
          replayed: true,
          violations: []
        )
      when :active
        # continue
      else
        raise LeaseStateError, "cannot commit lease #{lease.id} from state #{lease.state}"
      end

      normalized_actual = normalize_actual(lease.reservation, actual)

      release_active_lease!(lease)

      lease.scopes.each do |scope_name|
        scope_state = @scopes.fetch(scope_name)
        policy = scope_state.policy
        settled_at = settlement_time(policy, lease, current_time)
        append_events!(scope_state, normalized_actual, settled_at)
      end

      lease.actual = normalized_actual.freeze
      lease.state = :committed
      lease.committed_at = current_time
      clear_idempotency(lease)

      violations = lease.scopes.flat_map do |scope_name|
        detect_scope_violations(scope_name, current_time)
      end

      Finalization.new(
        lease: lease_view(lease),
        state: :committed,
        replayed: false,
        violations: violations
      )
    end
  end

  def cancel(lease_id, now: nil)
    @mutex.synchronize do
      current_time = normalize_now(now)
      sweep_expired!(current_time)
      lease = @leases[lease_id]
      raise UnknownLeaseError, "unknown lease #{lease_id.inspect}" if lease.nil?

      case lease.state
      when :cancelled
        return Finalization.new(
          lease: lease_view(lease),
          state: :cancelled,
          replayed: true,
          violations: []
        )
      when :active
        # continue
      else
        raise LeaseStateError, "cannot cancel lease #{lease.id} from state #{lease.state}"
      end

      release_active_lease!(lease)
      lease.state = :cancelled
      lease.cancelled_at = current_time
      clear_idempotency(lease)

      Finalization.new(
        lease: lease_view(lease),
        state: :cancelled,
        replayed: false,
        violations: []
      )
    end
  end

  def fetch_lease(lease_id)
    @mutex.synchronize do
      lease = @leases[lease_id]
      raise UnknownLeaseError, "unknown lease #{lease_id.inspect}" if lease.nil?

      lease_view(lease)
    end
  end

  def snapshot(scope_name, now: nil)
    @mutex.synchronize do
      current_time = normalize_now(now)
      sweep_expired!(current_time)
      normalized_name = normalize_scope_name(scope_name)
      scope_state = @scopes[normalized_name]
      raise UnknownScopeError, "unknown scope #{normalized_name.inspect}" if scope_state.nil?

      policy = scope_state.policy
      limits = policy.window_limits.map do |limit|
        used = used_amount(scope_state, limit.unit, limit.interval, current_time)
        {
          name: limit.name,
          unit: limit.unit,
          interval: limit.interval,
          capacity: limit.capacity,
          used: round_amount(used),
          available: round_amount([limit.capacity - used, 0.0].max)
        }
      end

      Snapshot.new(
        scope: normalized_name,
        now: current_time,
        policy: policy.to_h,
        active_leases: active_leases_for_scope(scope_state).map { |lease| lease_view(lease).to_h },
        limits: limits
      )
    end
  end

  def list_scopes
    @mutex.synchronize do
      @scopes.keys.sort
    end
  end

  private

  def normalize_policy(policy)
    return policy if policy.is_a?(Policy)

    Policy.new(**policy)
  end

  def normalize_scope_name(scope_name)
    value = scope_name.to_s.strip
    raise UnknownScopeError, "scope name cannot be empty" if value.empty?

    value
  end

  def normalize_scope_list(scopes)
    list = Array(scopes).map { |scope_name| normalize_scope_name(scope_name) }.uniq
    raise UnknownScopeError, "at least one scope is required" if list.empty?

    list.each do |scope_name|
      raise UnknownScopeError, "unknown scope #{scope_name.inspect}" unless @scopes.key?(scope_name)
    end

    list
  end

  def normalize_reservation(reservation)
    source = reservation || {}
    raise InvalidReservationError, "reservation must be a hash" unless source.is_a?(Hash)

    normalized = {}

    source.each do |unit, amount|
      key = unit.to_sym
      value = Float(amount)
      raise InvalidReservationError, "reservation amounts must be non-negative" if value.negative?
      next if value <= EPSILON

      normalized[key] = value
    end

    normalized[REQUEST_UNIT] ||= 1.0
    normalized
  end

  def normalize_actual(reservation, actual)
    if actual.nil?
      return reservation.dup
    end

    raise InvalidReservationError, "actual usage must be a hash" unless actual.is_a?(Hash)

    normalized = reservation.dup

    actual.each do |unit, amount|
      key = unit.to_sym
      value = Float(amount)
      raise InvalidReservationError, "actual usage amounts must be non-negative" if value.negative?

      if value <= EPSILON
        normalized.delete(key)
      else
        normalized[key] = value
      end
    end

    normalized[REQUEST_UNIT] ||= reservation[REQUEST_UNIT] || 1.0
    normalized
  end

  def normalize_metadata(metadata)
    return {} if metadata.nil?
    raise InvalidReservationError, "metadata must be a hash" unless metadata.is_a?(Hash)

    duplicated = {}
    metadata.each { |key, value| duplicated[key] = value }
    duplicated
  end

  def normalize_now(now)
    now.nil? ? @clock.call.to_f : Float(now)
  end

  def resolve_deadline(now, deadline_at, deadline_in)
    if !deadline_at.nil? && !deadline_in.nil?
      raise InvalidReservationError, "provide either deadline_at or deadline_in, not both"
    end

    if deadline_at
      Float(deadline_at)
    elsif deadline_in
      now + Float(deadline_in)
    end
  end

  def resolve_lease_ttl(scopes, override_ttl)
    base_ttl = scopes.map { |scope_name| @scopes.fetch(scope_name).policy.lease_ttl }.min
    return base_ttl if override_ttl.nil?

    requested_ttl = Float(override_ttl)
    raise InvalidReservationError, "lease_ttl must be positive" unless requested_ttl.positive?

    [requested_ttl, base_ttl].min
  end

  def analyze_reservation(scopes:, reservation:, lease_ttl:, deadline_at:, now:)
    reasons = []
    retry_at = now

    scopes.each do |scope_name|
      scope_state = @scopes.fetch(scope_name)
      scope_reasons = blocking_reasons_for_scope(scope_name, scope_state, reservation, lease_ttl, now)

      reasons.concat(scope_reasons)

      scope_reasons.each do |reason|
        retry_at = [retry_at, reason[:retry_at]].max
      end
    end

    if reasons.empty?
      return Decision.new(
        granted: true,
        lease: nil,
        reused: false,
        retry_at: nil,
        retry_in: 0.0,
        reasons: [],
        reservation: reservation.dup,
        scopes: scopes.dup
      )
    end

    if !deadline_at.nil? && retry_at > deadline_at + EPSILON
      reasons << {
        scope: nil,
        type: :deadline,
        retry_at: retry_at,
        deadline_at: deadline_at,
        message: "reservation would not become admissible before the deadline"
      }
    end

    Decision.new(
      granted: false,
      lease: nil,
      reused: false,
      retry_at: retry_at,
      retry_in: [retry_at - now, 0.0].max,
      reasons: reasons,
      reservation: reservation.dup,
      scopes: scopes.dup
    )
  end

  def blocking_reasons_for_scope(scope_name, scope_state, reservation, lease_ttl, now)
    prune_scope!(scope_state, now)

    reasons = []
    policy = scope_state.policy
    active_leases = active_leases_for_scope(scope_state)

    if policy.concurrency_limit
      concurrency_used = active_leases.length

      if concurrency_used + 1 > policy.concurrency_limit
        retry_at = earliest_concurrency_release_at(active_leases, now + lease_ttl)
        reasons << {
          scope: scope_name,
          type: :concurrency,
          limit_name: :concurrency,
          unit: :leases,
          used: concurrency_used,
          requested: 1,
          capacity: policy.concurrency_limit,
          retry_at: retry_at,
          message: "concurrency limit reached"
        }
      end
    end

    policy.window_limits.each do |limit|
      requested = reservation.fetch(limit.unit, 0.0)
      next if requested <= EPSILON

      used = used_amount(scope_state, limit.unit, limit.interval, now)
      next if used + requested <= limit.capacity + EPSILON

      retry_at = earliest_window_release_at(scope_state, limit, requested, now)
      reasons << {
        scope: scope_name,
        type: :window,
        limit_name: limit.name,
        unit: limit.unit,
        used: round_amount(used),
        requested: round_amount(requested),
        capacity: round_amount(limit.capacity),
        retry_at: retry_at,
        message: "#{limit.name} limit reached"
      }
    end

    reasons
  end

  def used_amount(scope_state, unit, interval, now)
    total = 0.0
    lower_bound = now - interval

    events = scope_state.events_by_unit[unit]
    events.each do |event|
      total += event.amount if event.at > lower_bound
    end

    active_leases_for_scope(scope_state).each do |lease|
      amount = lease.reservation.fetch(unit, 0.0)
      next if amount <= EPSILON
      next unless lease.created_at > lower_bound

      total += amount
    end

    total
  end

  def earliest_window_release_at(scope_state, limit, requested, now)
    lower_bound = now - limit.interval
    contributions = []

    scope_state.events_by_unit[limit.unit].each do |event|
      next unless event.at > lower_bound

      contributions << [event.at, event.amount]
    end

    active_leases_for_scope(scope_state).each do |lease|
      amount = lease.reservation.fetch(limit.unit, 0.0)
      next if amount <= EPSILON
      next unless lease.created_at > lower_bound

      contributions << [lease.created_at, amount]
    end

    used = contributions.inject(0.0) { |sum, (_, amount)| sum + amount }
    overflow = used + requested - limit.capacity
    return now if overflow <= EPSILON

    released = 0.0
    contributions.sort_by!(&:first)

    contributions.each do |occurred_at, amount|
      released += amount
      return occurred_at + limit.interval if released + EPSILON >= overflow
    end

    Float::INFINITY
  end

  def earliest_concurrency_release_at(active_leases, fallback)
    soonest = active_leases.map(&:expires_at).min
    soonest || fallback
  end

  def active_leases_for_scope(scope_state)
    scope_state.active_lease_ids.keys.map { |lease_id| @leases[lease_id] }.compact.select { |lease| lease.state == :active }
  end

  def append_events!(scope_state, amounts, timestamp)
    amounts.each do |unit, amount|
      next if amount <= EPSILON

      events = scope_state.events_by_unit[unit]
      event = UsageEvent.new(timestamp, amount)

      if !events.empty? && events[-1].at > timestamp
        events << event
        events.sort_by!(&:at)
      else
        events << event
      end
    end
  end

  def detect_scope_violations(scope_name, now)
    scope_state = @scopes.fetch(scope_name)
    prune_scope!(scope_state, now)
    policy = scope_state.policy

    policy.window_limits.each_with_object([]) do |limit, list|
      used = used_amount(scope_state, limit.unit, limit.interval, now)
      next if used <= limit.capacity + EPSILON

      list << {
        scope: scope_name,
        type: :window,
        limit_name: limit.name,
        unit: limit.unit,
        used: round_amount(used),
        capacity: round_amount(limit.capacity),
        over_by: round_amount(used - limit.capacity)
      }
    end
  end

  def prune_scope!(scope_state, now)
    scope_state.policy.max_interval_by_unit.each do |unit, interval|
      threshold = now - interval
      events = scope_state.events_by_unit[unit]
      next if events.empty?

      drop_count = 0
      events.each do |event|
        if event.at <= threshold
          drop_count += 1
        else
          break
        end
      end

      events.shift(drop_count) if drop_count.positive?
    end

    scope_state.active_lease_ids.delete_if do |lease_id, _|
      lease = @leases[lease_id]
      lease.nil? || lease.state != :active
    end
  end

  def sweep_expired!(now)
    active_expired = @leases.values.select { |lease| lease.state == :active && lease.expires_at <= now + EPSILON }

    active_expired.each do |lease|
      release_active_lease!(lease)

      lease.scopes.each do |scope_name|
        scope_state = @scopes.fetch(scope_name)
        policy = scope_state.policy
        next unless policy.expire_strategy == :charge_reserved

        settled_at = settlement_time(policy, lease, now)
        append_events!(scope_state, lease.reservation, settled_at)
      end

      lease.state = :expired
      lease.expired_at = now
      clear_idempotency(lease)
    end
  end

  def release_active_lease!(lease)
    lease.scopes.each do |scope_name|
      scope_state = @scopes.fetch(scope_name)
      scope_state.active_lease_ids.delete(lease.id)
    end
  end

  def settlement_time(policy, lease, current_time)
    if policy.settlement_time_source == :finished_at
      current_time
    else
      lease.created_at
    end
  end

  def find_active_idempotent_lease(idempotency_key)
    lease_id = @idempotency_index[idempotency_key]
    return nil if lease_id.nil?

    lease = @leases[lease_id]
    return nil if lease.nil? || lease.state != :active

    lease
  end

  def ensure_matching_idempotent_request!(lease:, scopes:, reservation:)
    return if lease.scopes == scopes && lease.reservation == reservation

    raise InvalidReservationError, "idempotency_key reused with different request parameters"
  end

  def clear_idempotency(lease)
    return if lease.idempotency_key.nil?

    @idempotency_index.delete(lease.idempotency_key)
  end

  def lease_view(lease)
    LeaseView.new(
      id: lease.id,
      state: lease.state,
      scopes: lease.scopes.dup,
      reservation: lease.reservation.dup,
      actual: lease.actual && lease.actual.dup,
      metadata: lease.metadata.dup,
      idempotency_key: lease.idempotency_key,
      created_at: lease.created_at,
      expires_at: lease.expires_at,
      committed_at: lease.committed_at,
      cancelled_at: lease.cancelled_at,
      expired_at: lease.expired_at
    )
  end

  def round_amount(value)
    return value if value.is_a?(Integer)

    ((value * 1_000_000.0).round / 1_000_000.0)
  end
end

=begin
This solves the ugly real-world problem where Rails apps, Sidekiq workers, cron backfills, and agent jobs all hit the same AI provider limits at once and then fall over in unpredictable ways. In April 2026 that usually means request-per-minute, token-per-minute, cost budget, and concurrency limits are all interacting at the same time across OpenAI, Anthropic, Gemini, or internal gateways. A plain rate limiter is not enough because modern AI requests are estimated before they run, actual usage arrives later, retries duplicate traffic, and long-running responses can leave your system unsure whether quota was really spent.

Built because the painful part is not “slow down a little.” The painful part is deciding before dispatch whether a request is safe to send, returning a useful retry time instead of random 429 handling, keeping in-flight work visible, and reconciling estimated usage with actual usage after the response finishes. That is why this file uses leases. You reserve expected usage up front, hold concurrency while the call is active, commit real usage when the provider response is done, or cancel the lease if the job never left your process. If a worker dies and never reports back, expiry can still conservatively charge the reserved amount so you do not oversubscribe shared quotas.

Use it when you have multiple producers sharing one upstream model budget: web requests plus background jobs, multi-tenant AI features in one Rails monolith, batch summarization mixed with user-facing chat, or an internal inference gateway that must give deterministic “retry after” answers. You can define separate scopes for a model, account, customer, or provider key and require one reservation to pass all of them together. That makes it useful for both global budget enforcement and tenant isolation without adding Redis-specific code into the core logic.

The trick: active leases count immediately, settled events are pruned with sliding windows, and retry timing is computed from the oldest usage that must age out for the next request to fit. That gives you a direct admission-control answer instead of spraying retries and hoping the upstream vendor calms down. Idempotency keys are included because duplicate enqueue and duplicate HTTP retry bugs are common in AI pipelines now, especially when one request can be expensive enough to matter.

Drop this into a Ruby service object, a Rails initializer, a Sidekiq server middleware stack, or an internal platform gem. Register scopes like `openai:gpt-5`, `anthropic:claude-opus`, or `tenant:enterprise-acme`, then call `plan` for dry-run admission checks, `reserve` before dispatch, `commit` with actual tokens and cost after completion, and `cancel` when work never left the process. If you later want cross-process coordination, keep this file as the decision engine and swap the in-memory state for Redis or Postgres-backed storage around the same lease lifecycle. That separation is the reason the code is useful in a real system and not just a toy rate limiter for a demo.
=end
