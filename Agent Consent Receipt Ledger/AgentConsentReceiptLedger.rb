#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "optparse"
require "time"
require "uri"
require "digest"
require "set"

module AgentConsentReceiptLedger
  VERSION = "1.0.0"

  SEVERITY_RANK = {
    "info" => 0,
    "low" => 1,
    "medium" => 2,
    "high" => 3,
    "critical" => 4
  }.freeze

  DEFAULT_SECRET_PATTERNS = [
    /AKIA[0-9A-Z]{16}/,
    /ASIA[0-9A-Z]{16}/,
    /AIza[0-9A-Za-z_\-]{35}/,
    /gh[pousr]_[A-Za-z0-9_]{30,}/,
    /sk-[A-Za-z0-9]{20,}/,
    /xox[baprs]-[A-Za-z0-9\-]{20,}/,
    /-----BEGIN (RSA |EC |OPENSSH |DSA |)?PRIVATE KEY-----/,
    /(?i)(api[_-]?key|secret|token|password|passwd|authorization)\s*[:=]\s*["']?[A-Za-z0-9._\-\/+=]{12,}/
  ].freeze

  SAFE_TOOL_HINTS = %w[
    list
    read
    search
    fetch_file
    get
    inspect
    status
    compare
    summarize
  ].freeze

  DESTRUCTIVE_HINTS = %w[
    archive
    cancel
    chmod
    close
    deactivate
    delete
    destroy
    disable
    drop
    force
    kill
    merge
    move
    purge
    remove
    reset
    revoke
    rotate
    send
    terminate
    trash
    truncate
  ].freeze

  WRITE_HINTS = %w[
    add
    apply
    commit
    create
    deploy
    draft
    edit
    label
    patch
    post
    publish
    put
    push
    replace
    resolve
    schedule
    submit
    update
    upload
    write
  ].freeze

  NETWORK_HINTS = %w[
    browser
    crawl
    curl
    fetch
    firecrawl
    http
    request
    scrape
    web
  ].freeze

  SHELL_HINTS = %w[
    bash
    command
    exec
    osascript
    powershell
    shell
    terminal
    zsh
  ].freeze

  EMAIL_HINTS = %w[
    calendar
    email
    forward
    gmail
    invite
    mail
    meeting
    outlook
    reply
    send
  ].freeze

  Finding = Struct.new(
    :id,
    :severity,
    :title,
    :message,
    :source,
    :line,
    :event_index,
    :tool,
    :receipt_id,
    :resource,
    :categories,
    :evidence,
    keyword_init: true
  ) do
    def to_h
      {
        id: id,
        severity: severity,
        title: title,
        message: message,
        source: source,
        line: line,
        event_index: event_index,
        tool: tool,
        receipt_id: receipt_id,
        resource: resource,
        categories: categories,
        evidence: evidence
      }.compact
    end
  end

  InputRecord = Struct.new(:raw, :source, :line, :index, keyword_init: true)

  NormalizedEvent = Struct.new(
    :index,
    :line,
    :source,
    :time,
    :type,
    :tool,
    :action,
    :resource,
    :domain,
    :args,
    :receipt_id,
    :approval,
    :actor,
    :tenant,
    :risk,
    :raw,
    keyword_init: true
  ) do
    def tool_event?
      type == "tool" || (!tool.to_s.empty? && type != "approval")
    end

    def approval_event?
      type == "approval"
    end
  end

  ConsentReceipt = Struct.new(
    :id,
    :approved,
    :issued_at,
    :expires_at,
    :actor,
    :tenant,
    :tool_patterns,
    :resource_patterns,
    :domains,
    :max_uses,
    :source,
    :line,
    :raw,
    keyword_init: true
  ) do
    def denied?
      approved == false
    end

    def expired_at?(time)
      return false unless expires_at && time

      time > expires_at
    end
  end

  class ParseError < StandardError; end

  class HashTools
    def self.deep_fetch(hash, paths)
      paths.each do |path|
        cursor = hash
        path.each do |key|
          break unless cursor.is_a?(Hash)

          cursor = value_for(cursor, key)
        end
        return cursor unless cursor.nil?
      end
      nil
    end

    def self.value_for(hash, key)
      return nil unless hash.is_a?(Hash)

      return hash[key] if hash.key?(key)

      string_key = key.to_s
      return hash[string_key] if hash.key?(string_key)

      symbol_key = key.to_sym
      return hash[symbol_key] if hash.key?(symbol_key)

      down = string_key.downcase
      found = hash.keys.find { |candidate| candidate.to_s.downcase == down }
      found ? hash[found] : nil
    end

    def self.deep_find_first(hash, wanted)
      queue = [hash]
      until queue.empty?
        item = queue.shift
        case item
        when Hash
          wanted.each do |key|
            value = value_for(item, key)
            return value unless value.nil?
          end
          item.each_value { |value| queue << value if value.is_a?(Hash) || value.is_a?(Array) }
        when Array
          item.each { |value| queue << value if value.is_a?(Hash) || value.is_a?(Array) }
        end
      end
      nil
    end

    def self.deep_string(value, limit = 16_000)
      text = case value
             when String
               value
             else
               JSON.generate(value)
             end
      text.bytesize > limit ? text.byteslice(0, limit) : text
    rescue JSON::GeneratorError
      value.inspect.byteslice(0, limit)
    end

    def self.stringify_keys(value)
      case value
      when Hash
        value.each_with_object({}) { |(key, child), memo| memo[key.to_s] = stringify_keys(child) }
      when Array
        value.map { |child| stringify_keys(child) }
      else
        value
      end
    end

    def self.list_value(value)
      case value
      when nil
        []
      when Array
        value.flat_map { |item| list_value(item) }
      when Hash
        value.values.flat_map { |item| list_value(item) }
      else
        value.to_s.split(/[,\s]+/).reject(&:empty?)
      end
    end
  end

  class InputParser
    EVENT_ARRAY_KEYS = %w[events records rows data items messages entries].freeze

    def self.parse_paths(paths)
      if paths.empty?
        parse_stream($stdin.read, "stdin")
      else
        paths.flat_map { |path| parse_stream(File.read(path), path) }
      end
    end

    def self.parse_stream(text, source)
      stripped = text.to_s.strip
      return [] if stripped.empty?

      begin
        parsed = JSON.parse(stripped)
        rows = events_from_document(parsed)
        return rows.each_with_index.map do |row, offset|
          InputRecord.new(raw: row, source: source, line: nil, index: offset)
        end
      rescue JSON::ParserError
        parse_json_lines(text, source)
      end
    end

    def self.events_from_document(parsed)
      case parsed
      when Array
        parsed
      when Hash
        key = EVENT_ARRAY_KEYS.find { |candidate| parsed[candidate].is_a?(Array) }
        key ? parsed[key] : [parsed]
      else
        raise ParseError, "input JSON must be an object, an array, or JSONL objects"
      end
    end

    def self.parse_json_lines(text, source)
      records = []
      text.each_line.with_index(1) do |line, line_number|
        stripped = line.strip
        next if stripped.empty?

        begin
          parsed = JSON.parse(stripped)
        rescue JSON::ParserError => error
          raise ParseError, "#{source}:#{line_number}: invalid JSONL: #{error.message}"
        end
        records << InputRecord.new(raw: parsed, source: source, line: line_number, index: records.length)
      end
      records
    end
  end

  class Policy
    attr_reader :approval_ttl_seconds,
                :max_uses_per_receipt,
                :required_tool_patterns,
                :denied_tool_patterns,
                :allowed_domains,
                :allowed_write_roots,
                :safe_tool_patterns,
                :fail_on,
                :secret_patterns

    def initialize(values = {})
      values = HashTools.stringify_keys(values || {})
      @approval_ttl_seconds = integer(values["approval_ttl_minutes"], 24 * 60) * 60
      @max_uses_per_receipt = integer(values["max_uses_per_receipt"], 1)
      @required_tool_patterns = array(values["require_approval_for"] || values["required_tool_patterns"] || ["*"])
      @denied_tool_patterns = array(values["deny_tools"] || values["denied_tool_patterns"])
      @allowed_domains = array(values["allowed_domains"]).map(&:downcase)
      @allowed_write_roots = array(values["allowed_write_roots"])
      @safe_tool_patterns = array(values["safe_tools"] || SAFE_TOOL_HINTS)
      @fail_on = severity(values["fail_on"] || "high")
      @secret_patterns = DEFAULT_SECRET_PATTERNS + array(values["secret_patterns"]).map { |pattern| Regexp.new(pattern) }
    end

    def self.load(path)
      return new if path.nil? || path.empty?

      new(JSON.parse(File.read(path)))
    rescue JSON::ParserError => error
      raise ParseError, "#{path}: invalid policy JSON: #{error.message}"
    end

    def denied_tool?(tool)
      match_any?(@denied_tool_patterns, tool)
    end

    def explicit_required_tool?(tool)
      match_any?(@required_tool_patterns, tool)
    end

    def safe_tool?(tool)
      match_any?(@safe_tool_patterns, tool)
    end

    def allowed_domain?(domain)
      return true if domain.to_s.empty?
      return true if @allowed_domains.empty?

      domain = domain.downcase
      @allowed_domains.any? do |pattern|
        wildcard_match?(pattern, domain) || domain.end_with?(".#{pattern}")
      end
    end

    def allowed_write_resource?(resource)
      return true if resource.to_s.empty?
      return true if @allowed_write_roots.empty?

      @allowed_write_roots.any? { |pattern| wildcard_match?(pattern, resource) }
    end

    def fail_rank
      SEVERITY_RANK.fetch(@fail_on)
    end

    def to_h
      {
        approval_ttl_minutes: @approval_ttl_seconds / 60,
        max_uses_per_receipt: @max_uses_per_receipt,
        require_approval_for: @required_tool_patterns,
        deny_tools: @denied_tool_patterns,
        allowed_domains: @allowed_domains,
        allowed_write_roots: @allowed_write_roots,
        safe_tools: @safe_tool_patterns,
        fail_on: @fail_on
      }
    end

    def self.wildcard_match?(pattern, value)
      File.fnmatch?(pattern.to_s.downcase, value.to_s.downcase, File::FNM_EXTGLOB)
    end

    private

    def array(value)
      HashTools.list_value(value)
    end

    def integer(value, fallback)
      return fallback if value.nil? || value.to_s.empty?

      Integer(value)
    rescue ArgumentError
      fallback
    end

    def severity(value)
      text = value.to_s.downcase
      SEVERITY_RANK.key?(text) ? text : "high"
    end

    def match_any?(patterns, value)
      value = value.to_s
      patterns.any? { |pattern| Policy.wildcard_match?(pattern, value) }
    end

    def wildcard_match?(pattern, value)
      Policy.wildcard_match?(pattern, value)
    end
  end

  class EventNormalizer
    TIME_PATHS = [
      %w[timestamp],
      %w[time],
      %w[created_at],
      %w[createdAt],
      %w[ts],
      %w[event timestamp],
      %w[metadata timestamp]
    ].freeze

    TOOL_PATHS = [
      %w[tool],
      %w[tool_name],
      %w[toolName],
      %w[name],
      %w[function name],
      %w[toolCall name],
      %w[mcp tool],
      %w[action tool],
      %w[server tool],
      %w[request tool]
    ].freeze

    ACTION_PATHS = [
      %w[action],
      %w[operation],
      %w[method],
      %w[verb],
      %w[type],
      %w[event],
      %w[function name],
      %w[request method]
    ].freeze

    ARG_PATHS = [
      %w[arguments],
      %w[args],
      %w[input],
      %w[parameters],
      %w[payload],
      %w[request],
      %w[body],
      %w[toolCall arguments],
      %w[function arguments]
    ].freeze

    RESOURCE_PATHS = [
      %w[resource],
      %w[target],
      %w[path],
      %w[file],
      %w[url],
      %w[uri],
      %w[repository],
      %w[repo],
      %w[project],
      %w[request url],
      %w[request path],
      %w[arguments path],
      %w[arguments url],
      %w[args path],
      %w[args url],
      %w[input path],
      %w[input url]
    ].freeze

    RECEIPT_PATHS = [
      %w[receipt_id],
      %w[receiptId],
      %w[approval_id],
      %w[approvalId],
      %w[consent_id],
      %w[consentId],
      %w[authorization_id],
      %w[authorizationId],
      %w[approval receipt_id],
      %w[consent receipt_id],
      %w[arguments approval_id],
      %w[args approval_id],
      %w[input approval_id]
    ].freeze

    APPROVAL_PATHS = [
      %w[approved],
      %w[approval],
      %w[consent],
      %w[decision],
      %w[status],
      %w[authorization status]
    ].freeze

    ACTOR_PATHS = [
      %w[actor],
      %w[user],
      %w[approver],
      %w[principal],
      %w[metadata actor],
      %w[approval approver]
    ].freeze

    TENANT_PATHS = [
      %w[tenant],
      %w[workspace],
      %w[org],
      %w[organization],
      %w[account],
      %w[metadata tenant]
    ].freeze

    RISK_PATHS = [
      %w[risk],
      %w[risk_level],
      %w[severity],
      %w[metadata risk]
    ].freeze

    def self.normalize(record)
      raw = HashTools.stringify_keys(record.raw)
      args = HashTools.deep_fetch(raw, ARG_PATHS)
      tool = first_string(HashTools.deep_fetch(raw, TOOL_PATHS))
      action = first_string(HashTools.deep_fetch(raw, ACTION_PATHS))
      receipt_id = first_string(HashTools.deep_fetch(raw, RECEIPT_PATHS))
      approval = approval_value(HashTools.deep_fetch(raw, APPROVAL_PATHS))
      event_type = classify_event(raw, tool, action, approval)
      resource = first_string(HashTools.deep_fetch(raw, RESOURCE_PATHS)) ||
                 first_string(HashTools.deep_find_first(args, %w[url uri path repository repo file target resource]))
      time = parse_time(HashTools.deep_fetch(raw, TIME_PATHS))

      NormalizedEvent.new(
        index: record.index,
        line: record.line,
        source: record.source,
        time: time,
        type: event_type,
        tool: normalize_tool(tool, raw),
        action: action.to_s,
        resource: resource.to_s,
        domain: domain_for(resource),
        args: args || {},
        receipt_id: receipt_id.to_s,
        approval: approval,
        actor: first_string(HashTools.deep_fetch(raw, ACTOR_PATHS)).to_s,
        tenant: first_string(HashTools.deep_fetch(raw, TENANT_PATHS)).to_s,
        risk: first_string(HashTools.deep_fetch(raw, RISK_PATHS)).to_s.downcase,
        raw: raw
      )
    end

    def self.parse_time(value)
      return nil if value.nil? || value.to_s.empty?
      return Time.at(value).utc if value.is_a?(Numeric)

      Time.parse(value.to_s).utc
    rescue ArgumentError
      nil
    end

    def self.domain_for(value)
      text = value.to_s
      return "" unless text.start_with?("http://", "https://")

      URI.parse(text).host.to_s.downcase
    rescue URI::InvalidURIError
      ""
    end

    def self.first_string(value)
      case value
      when nil
        nil
      when String
        value
      when Numeric, TrueClass, FalseClass
        value.to_s
      when Array
        value.map { |item| first_string(item) }.find { |item| item && !item.empty? }
      when Hash
        value["name"] || value["id"] || value["path"] || value["url"] || value["value"]
      else
        value.to_s
      end
    end

    def self.approval_value(value)
      return nil if value.nil?
      return value if value == true || value == false

      text = value.to_s.downcase
      return true if %w[approved approve allowed allow granted grant yes true accepted].include?(text)
      return false if %w[denied deny rejected reject blocked block no false revoked].include?(text)

      nil
    end

    def self.classify_event(raw, tool, action, approval)
      joined = [raw["type"], raw["event"], raw["kind"], action].compact.join(" ").downcase
      return "approval" if !approval.nil? || joined.match?(/approval|consent|authorization|permit/)
      return "tool" if tool && !tool.empty?
      return "tool" if joined.match?(/tool|function_call|mcp|command|shell|browser/)

      "event"
    end

    def self.normalize_tool(tool, raw)
      candidate = tool.to_s
      server = HashTools.deep_fetch(raw, [%w[server], %w[mcp server], %w[request server]])
      return candidate unless !server.to_s.empty? && !candidate.include?(".")

      "#{server}.#{candidate}"
    end
  end

  class ReceiptLedger
    def initialize(events, policy)
      @events = events
      @policy = policy
    end

    def receipts
      @receipts ||= @events.select(&:approval_event?).map { |event| receipt_from(event) }.compact
    end

    def receipt_from(event)
      id = event.receipt_id
      id = stable_receipt_id(event) if id.empty?
      scope = scope_hash(event.raw)
      issued_at = event.time || Time.now.utc
      ttl_expiry = issued_at + @policy.approval_ttl_seconds
      explicit_expiry = EventNormalizer.parse_time(scope["expires_at"] || scope["expiresAt"] || event.raw["expires_at"])

      ConsentReceipt.new(
        id: id,
        approved: event.approval != false,
        issued_at: issued_at,
        expires_at: explicit_expiry || ttl_expiry,
        actor: event.actor,
        tenant: event.tenant,
        tool_patterns: list_scope(scope, %w[tools tool_patterns allowed_tools tool]),
        resource_patterns: list_scope(scope, %w[resources resource_patterns allowed_resources paths path repository repo]),
        domains: list_scope(scope, %w[domains allowed_domains domain]).map(&:downcase),
        max_uses: integer(scope["max_uses"] || scope["maxUses"], @policy.max_uses_per_receipt),
        source: event.source,
        line: event.line,
        raw: event.raw
      )
    end

    private

    def scope_hash(raw)
      scope = raw["scope"]
      scope = raw["consent_scope"] if scope.nil?
      scope = raw["authorization"] if scope.nil?
      scope = raw["approval"] if scope.nil?
      scope.is_a?(Hash) ? scope : raw
    end

    def list_scope(scope, keys)
      values = keys.flat_map { |key| HashTools.list_value(HashTools.value_for(scope, key)) }
      values.empty? ? ["*"] : values
    end

    def integer(value, fallback)
      return fallback if value.nil? || value.to_s.empty?

      Integer(value)
    rescue ArgumentError
      fallback
    end

    def stable_receipt_id(event)
      Digest::SHA256.hexdigest("#{event.source}:#{event.line}:#{event.index}:#{event.actor}:#{event.time}")[0, 16]
    end
  end

  class RiskClassifier
    def initialize(policy)
      @policy = policy
    end

    def categories_for(event)
      text = [event.tool, event.action, event.resource, HashTools.deep_string(event.args, 2_000)].join(" ").downcase
      categories = []
      categories << "denied_tool" if @policy.denied_tool?(event.tool)
      categories << "destructive" if contains_hint?(text, DESTRUCTIVE_HINTS)
      categories << "write" if contains_hint?(text, WRITE_HINTS)
      categories << "network" if network_event?(event, text)
      categories << "shell" if contains_hint?(text, SHELL_HINTS)
      categories << "email_calendar" if contains_hint?(text, EMAIL_HINTS)
      categories << "secret_material" if secret_like?(event.args)
      categories << "repo_mutation" if repo_mutation?(text)
      categories << "privileged" if privileged?(text)
      categories.uniq
    end

    def requires_receipt?(event, categories)
      return false unless event.tool_event?
      return true if categories.any?
      return false if @policy.safe_tool?(event.tool)

      @policy.explicit_required_tool?(event.tool)
    end

    def secret_like?(value)
      text = HashTools.deep_string(value)
      @policy.secret_patterns.any? { |pattern| text.match?(pattern) }
    end

    private

    def contains_hint?(text, hints)
      hints.any? { |hint| text.match?(/\b#{Regexp.escape(hint)}\b/) || text.include?("_#{hint}") || text.include?("#{hint}_") }
    end

    def network_event?(event, text)
      !event.domain.empty? || contains_hint?(text, NETWORK_HINTS)
    end

    def repo_mutation?(text)
      text.match?(/\b(git|github|repo|repository|branch|commit|pull request|pull_request)\b/) &&
        contains_hint?(text, WRITE_HINTS + DESTRUCTIVE_HINTS)
    end

    def privileged?(text)
      text.match?(/\b(admin|sudo|root|iam|role|permission|scope|oauth|service account|credential)\b/)
    end
  end

  class ReceiptMatcher
    def initialize(receipts)
      @receipts = receipts
      @uses = Hash.new(0)
    end

    def match(event)
      if !event.receipt_id.empty?
        receipt = @receipts.find { |candidate| candidate.id == event.receipt_id }
        return [receipt, :missing] if receipt.nil?
        return [receipt, :denied] if receipt.denied?
        return [receipt, :expired] if receipt.expired_at?(event.time)
        return [receipt, :scope_mismatch] unless in_scope?(receipt, event)
        return [receipt, :reused] if @uses[receipt.id] >= receipt.max_uses

        @uses[receipt.id] += 1
        return [receipt, :ok]
      end

      candidates = @receipts.select do |receipt|
        !receipt.denied? &&
          !receipt.expired_at?(event.time) &&
          same_tenant?(receipt, event) &&
          issued_before?(receipt, event) &&
          in_scope?(receipt, event) &&
          @uses[receipt.id] < receipt.max_uses
      end

      return [nil, :missing] if candidates.empty?
      return [nil, :ambiguous] if candidates.length > 1

      receipt = candidates.first
      @uses[receipt.id] += 1
      [receipt, :ok]
    end

    def in_scope?(receipt, event)
      tool_match = wildcard_any?(receipt.tool_patterns, event.tool)
      resource_match = event.resource.empty? || wildcard_any?(receipt.resource_patterns, event.resource)
      domain_match = event.domain.empty? || receipt.domains.include?("*") || receipt.domains.any? do |pattern|
        Policy.wildcard_match?(pattern, event.domain) || event.domain.end_with?(".#{pattern}")
      end
      tool_match && resource_match && domain_match
    end

    private

    def wildcard_any?(patterns, value)
      patterns.any? { |pattern| pattern == "*" || Policy.wildcard_match?(pattern, value) }
    end

    def same_tenant?(receipt, event)
      receipt.tenant.to_s.empty? || event.tenant.to_s.empty? || receipt.tenant == event.tenant
    end

    def issued_before?(receipt, event)
      return true unless receipt.issued_at && event.time

      receipt.issued_at <= event.time
    end
  end

  class Analyzer
    attr_reader :events, :receipts, :findings

    def initialize(records, policy)
      @policy = policy
      @events = records.map { |record| EventNormalizer.normalize(record) }
      @receipts = ReceiptLedger.new(@events, policy).receipts
      @classifier = RiskClassifier.new(policy)
      @matcher = ReceiptMatcher.new(@receipts)
      @findings = []
    end

    def run
      check_receipts
      @events.each { |event| analyze_event(event) }
      self
    end

    def summary
      counts = Hash.new(0)
      @findings.each { |finding| counts[finding.severity] += 1 }
      {
        version: VERSION,
        events: @events.length,
        tool_events: @events.count(&:tool_event?),
        receipts: @receipts.length,
        approved_receipts: @receipts.count { |receipt| receipt.approved == true },
        denied_receipts: @receipts.count(&:denied?),
        findings: @findings.length,
        severity_counts: SEVERITY_RANK.keys.each_with_object({}) { |severity, memo| memo[severity] = counts[severity] }
      }
    end

    def failed?
      @findings.any? { |finding| SEVERITY_RANK.fetch(finding.severity) >= @policy.fail_rank }
    end

    private

    def check_receipts
      ids = Hash.new { |hash, key| hash[key] = [] }
      @receipts.each { |receipt| ids[receipt.id] << receipt }
      ids.each_value do |same_id|
        next if same_id.length == 1

        receipt = same_id.first
        add(
          id: "DUPLICATE_CONSENT_RECEIPT",
          severity: "high",
          title: "Receipt id is reused by multiple approval records",
          message: "Receipt #{receipt.id} appears #{same_id.length} times. A verifier cannot prove which human approval governed later tool calls.",
          event: event_for_receipt(receipt),
          receipt: receipt,
          categories: ["approval_integrity"],
          evidence: { duplicate_count: same_id.length }
        )
      end

      @receipts.each do |receipt|
        next unless receipt.approved == true && receipt.tool_patterns.include?("*") && receipt.resource_patterns.include?("*")

        add(
          id: "BROAD_CONSENT_SCOPE",
          severity: "medium",
          title: "Consent receipt has wildcard tool and resource scope",
          message: "Receipt #{receipt.id} can match any tool and any resource. That is hard to defend after an autonomous agent changes production state.",
          event: event_for_receipt(receipt),
          receipt: receipt,
          categories: ["approval_scope"],
          evidence: { tools: receipt.tool_patterns, resources: receipt.resource_patterns }
        )
      end
    end

    def analyze_event(event)
      return unless event.tool_event?

      categories = @classifier.categories_for(event)
      check_secret_material(event, categories)
      check_policy_boundaries(event, categories)
      return unless @classifier.requires_receipt?(event, categories)

      receipt, status = @matcher.match(event)
      case status
      when :ok
        nil
      when :missing
        add(
          id: "MISSING_CONSENT_RECEIPT",
          severity: severity_for(categories, "high"),
          title: "Risky tool call has no matching consent receipt",
          message: "Tool #{display(event.tool)} touched #{display(event.resource)} without a valid approval receipt tied to its scope.",
          event: event,
          receipt: receipt,
          categories: categories,
          evidence: { receipt_id: event.receipt_id, actor: event.actor, tenant: event.tenant }
        )
      when :denied
        add(
          id: "DENIED_ACTION_EXECUTED",
          severity: "critical",
          title: "Tool call references a denied consent receipt",
          message: "Tool #{display(event.tool)} used receipt #{receipt.id}, but that receipt records a denial. This should fail the run immediately.",
          event: event,
          receipt: receipt,
          categories: categories,
          evidence: receipt_evidence(receipt)
        )
      when :expired
        add(
          id: "EXPIRED_CONSENT_RECEIPT",
          severity: "high",
          title: "Tool call used an expired consent receipt",
          message: "Receipt #{receipt.id} expired at #{receipt.expires_at.utc.iso8601}, before this tool call was recorded.",
          event: event,
          receipt: receipt,
          categories: categories,
          evidence: receipt_evidence(receipt)
        )
      when :scope_mismatch
        add(
          id: "OUT_OF_SCOPE_CONSENT_RECEIPT",
          severity: "high",
          title: "Consent receipt does not cover the tool call scope",
          message: "Receipt #{receipt.id} exists, but it does not cover tool #{display(event.tool)}, resource #{display(event.resource)}, and domain #{display(event.domain)} together.",
          event: event,
          receipt: receipt,
          categories: categories,
          evidence: receipt_evidence(receipt)
        )
      when :reused
        add(
          id: "REUSED_SINGLE_USE_RECEIPT",
          severity: "high",
          title: "Consent receipt was reused beyond its limit",
          message: "Receipt #{receipt.id} was used more than its allowed #{receipt.max_uses} time(s). Reuse breaks auditability for repeated state changes.",
          event: event,
          receipt: receipt,
          categories: categories,
          evidence: receipt_evidence(receipt)
        )
      when :ambiguous
        add(
          id: "AMBIGUOUS_CONSENT_RECEIPT",
          severity: "medium",
          title: "Multiple receipts match a risky tool call",
          message: "Tool #{display(event.tool)} had more than one eligible receipt. Attach the explicit receipt id so the audit trail is deterministic.",
          event: event,
          categories: categories,
          evidence: { receipt_id: event.receipt_id }
        )
      end
    end

    def check_secret_material(event, categories)
      return unless categories.include?("secret_material")

      add(
        id: "SECRET_LIKE_TOOL_INPUT",
        severity: "critical",
        title: "Tool input appears to contain secret material",
        message: "Tool #{display(event.tool)} received text that looks like a token, key, password, or authorization header. Store references, not raw secrets, in agent transcripts.",
        event: event,
        categories: categories,
        evidence: { input_digest: Digest::SHA256.hexdigest(HashTools.deep_string(event.args))[0, 16] }
      )
    end

    def check_policy_boundaries(event, categories)
      if categories.include?("denied_tool")
        add(
          id: "DENIED_TOOL_USED",
          severity: "critical",
          title: "Tool matches the policy deny list",
          message: "Tool #{display(event.tool)} is denied by policy. No consent receipt should override a hard deny rule.",
          event: event,
          categories: categories
        )
      end

      if categories.include?("network") && !@policy.allowed_domain?(event.domain)
        add(
          id: "UNAPPROVED_NETWORK_DOMAIN",
          severity: "high",
          title: "Tool call reaches a domain outside the policy allow list",
          message: "Domain #{display(event.domain)} is not present in allowed_domains. External agent egress should be reviewed before the request leaves the workspace.",
          event: event,
          categories: categories,
          evidence: { allowed_domains: @policy.allowed_domains }
        )
      end

      if categories.include?("write") && !@policy.allowed_write_resource?(event.resource)
        add(
          id: "UNAPPROVED_WRITE_RESOURCE",
          severity: "high",
          title: "Write target is outside the policy resource allow list",
          message: "Resource #{display(event.resource)} is not covered by allowed_write_roots. This catches repo, drive, database, and deployment writes aimed at the wrong place.",
          event: event,
          categories: categories,
          evidence: { allowed_write_roots: @policy.allowed_write_roots }
        )
      end
    end

    def severity_for(categories, fallback)
      return "critical" if categories.include?("destructive") || categories.include?("secret_material")
      return "high" if categories.include?("write") || categories.include?("repo_mutation")

      fallback
    end

    def add(id:, severity:, title:, message:, event:, categories:, receipt: nil, evidence: {})
      @findings << Finding.new(
        id: id,
        severity: severity,
        title: title,
        message: message,
        source: event&.source,
        line: event&.line,
        event_index: event&.index,
        tool: event&.tool,
        receipt_id: receipt&.id || event&.receipt_id,
        resource: event&.resource,
        categories: categories,
        evidence: evidence
      )
    end

    def event_for_receipt(receipt)
      @events.find { |event| event.approval_event? && event.receipt_id == receipt.id } ||
        NormalizedEvent.new(source: receipt.source, line: receipt.line, index: nil)
    end

    def receipt_evidence(receipt)
      {
        receipt_id: receipt.id,
        issued_at: receipt.issued_at&.utc&.iso8601,
        expires_at: receipt.expires_at&.utc&.iso8601,
        tool_patterns: receipt.tool_patterns,
        resource_patterns: receipt.resource_patterns,
        domains: receipt.domains,
        max_uses: receipt.max_uses
      }
    end

    def display(value)
      text = value.to_s
      text.empty? ? "(unknown)" : text
    end
  end

  class Reporter
    def initialize(analyzer, policy)
      @analyzer = analyzer
      @policy = policy
    end

    def render(format)
      case format
      when "json"
        JSON.pretty_generate(to_json_report)
      when "sarif"
        JSON.pretty_generate(to_sarif)
      when "markdown"
        to_markdown
      else
        raise ParseError, "unknown format #{format.inspect}; use json, sarif, or markdown"
      end
    end

    def to_json_report
      {
        summary: @analyzer.summary,
        policy: @policy.to_h,
        findings: @analyzer.findings.map(&:to_h)
      }
    end

    def to_sarif
      rules = @analyzer.findings.map(&:id).uniq.sort.map do |id|
        {
          id: id,
          shortDescription: { text: id.split("_").map(&:capitalize).join(" ") },
          helpUri: "https://github.com/kspavankrishna/VIBE-CODE",
          properties: { tags: ["agent-consent", "tool-audit", "mcp", "devops"] }
        }
      end
      results = @analyzer.findings.map do |finding|
        {
          ruleId: finding.id,
          level: sarif_level(finding.severity),
          message: { text: finding.message },
          locations: [
            {
              physicalLocation: {
                artifactLocation: { uri: finding.source || "stdin" },
                region: { startLine: finding.line || 1 }
              }
            }
          ],
          properties: {
            severity: finding.severity,
            tool: finding.tool,
            receipt_id: finding.receipt_id,
            resource: finding.resource,
            categories: finding.categories,
            evidence: finding.evidence
          }.compact
        }
      end

      {
        version: "2.1.0",
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        runs: [
          {
            tool: {
              driver: {
                name: "AgentConsentReceiptLedger",
                informationUri: "https://github.com/kspavankrishna/VIBE-CODE",
                semanticVersion: VERSION,
                rules: rules
              }
            },
            invocations: [{ executionSuccessful: !@analyzer.failed? }],
            results: results
          }
        ]
      }
    end

    def to_markdown
      lines = []
      summary = @analyzer.summary
      lines << "# Agent Consent Receipt Ledger"
      lines << ""
      lines << "- Events reviewed: #{summary[:events]}"
      lines << "- Tool calls reviewed: #{summary[:tool_events]}"
      lines << "- Consent receipts found: #{summary[:receipts]}"
      lines << "- Findings: #{summary[:findings]}"
      lines << "- Gate: fail on #{@policy.fail_on} or worse"
      lines << ""
      if @analyzer.findings.empty?
        lines << "No risky tool-call consent gaps were found."
      else
        lines << "| Severity | Rule | Tool | Receipt | Resource | Message |"
        lines << "| --- | --- | --- | --- | --- | --- |"
        @analyzer.findings.each do |finding|
          lines << [
            finding.severity,
            finding.id,
            finding.tool.to_s.empty? ? "unknown" : finding.tool,
            finding.receipt_id.to_s.empty? ? "none" : finding.receipt_id,
            finding.resource.to_s.empty? ? "unknown" : finding.resource,
            finding.message
          ].map { |cell| escape_markdown_cell(cell) }.join(" | ").prepend("| ").concat(" |")
        end
      end
      lines.join("\n")
    end

    private

    def sarif_level(severity)
      case severity
      when "critical", "high"
        "error"
      when "medium"
        "warning"
      else
        "note"
      end
    end

    def escape_markdown_cell(value)
      value.to_s.gsub("|", "\\|").gsub("\n", " ")
    end
  end

  class ExamplePolicy
    def self.to_json
      JSON.pretty_generate(
        {
          approval_ttl_minutes: 240,
          max_uses_per_receipt: 1,
          fail_on: "high",
          require_approval_for: ["*"],
          deny_tools: ["*.delete_secret", "shell.rm_rf", "git.force_push"],
          safe_tools: ["*.fetch", "*.read", "*.list", "github.compare*"],
          allowed_domains: ["api.github.com", "github.com", "company.internal"],
          allowed_write_roots: ["kspavankrishna/VIBE-CODE", "*.rb", "deployments/staging/*"],
          secret_patterns: ["(?i)bearer\\s+[a-z0-9._\\-]{20,}"]
        }
      )
    end
  end

  class SelfTest
    def self.run!
      base = Time.utc(2026, 4, 10, 12, 0, 0)
      input = [
        {
          type: "approval",
          timestamp: base.iso8601,
          receipt_id: "ship-ruby-ledger",
          approved: true,
          actor: "pavan",
          tenant: "personal",
          scope: {
            tools: ["github._create_file"],
            resources: ["kspavankrishna/VIBE-CODE", "AgentConsentReceiptLedger.rb"],
            domains: ["api.github.com"],
            max_uses: 1,
            expires_at: (base + 3600).iso8601
          }
        },
        {
          type: "tool_call",
          timestamp: (base + 30).iso8601,
          tool: "github._create_file",
          action: "create_file",
          receipt_id: "ship-ruby-ledger",
          tenant: "personal",
          arguments: {
            repository: "kspavankrishna/VIBE-CODE",
            path: "AgentConsentReceiptLedger.rb"
          }
        },
        {
          type: "tool_call",
          timestamp: (base + 60).iso8601,
          tool: "shell.exec",
          action: "delete",
          arguments: {
            command: "rm -rf /srv/prod/cache"
          }
        },
        {
          type: "tool_call",
          timestamp: (base + 90).iso8601,
          tool: "http.fetch",
          action: "request",
          url: "https://unapproved.example/upload",
          arguments: {
            authorization: "Bearer sk-abcdefghijklmnopqrstuvwxyz123456"
          }
        },
        {
          type: "approval",
          timestamp: (base + 100).iso8601,
          receipt_id: "denied-prod-delete",
          approved: false,
          actor: "pavan",
          scope: {
            tools: ["github._delete_file"],
            resources: ["production/*"],
            domains: ["api.github.com"]
          }
        },
        {
          type: "tool_call",
          timestamp: (base + 120).iso8601,
          tool: "github._delete_file",
          action: "delete_file",
          receipt_id: "denied-prod-delete",
          arguments: {
            repository: "kspavankrishna/VIBE-CODE",
            path: "production/key.txt"
          }
        },
        {
          type: "tool_call",
          timestamp: (base + 130).iso8601,
          tool: "github._create_file",
          action: "create_file",
          receipt_id: "ship-ruby-ledger",
          tenant: "personal",
          arguments: {
            repository: "kspavankrishna/VIBE-CODE",
            path: "AgentConsentReceiptLedger.rb"
          }
        }
      ]
      records = InputParser.parse_stream(JSON.generate(input), "self-test.json")
      policy = Policy.new(
        "allowed_domains" => ["api.github.com"],
        "allowed_write_roots" => ["kspavankrishna/VIBE-CODE", "AgentConsentReceiptLedger.rb", "production/*"],
        "deny_tools" => ["github._delete_file"],
        "safe_tools" => ["github._fetch"]
      )
      analyzer = Analyzer.new(records, policy).run
      ids = analyzer.findings.map(&:id).to_set
      expected = %w[
        MISSING_CONSENT_RECEIPT
        SECRET_LIKE_TOOL_INPUT
        UNAPPROVED_NETWORK_DOMAIN
        DENIED_TOOL_USED
        DENIED_ACTION_EXECUTED
        REUSED_SINGLE_USE_RECEIPT
      ]
      missing = expected.reject { |id| ids.include?(id) }
      raise "self-test missing findings: #{missing.join(", ")}" unless missing.empty?
      raise "first approved write should not be flagged" if analyzer.findings.any? do |finding|
        finding.event_index == 1 && finding.id == "MISSING_CONSENT_RECEIPT"
      end

      "self-test ok: #{analyzer.findings.length} findings across #{analyzer.events.length} events"
    end
  end

  class CLI
    DEFAULT_OPTIONS = {
      format: "json",
      inputs: [],
      policy: nil,
      fail_on: nil,
      allowed_domains: [],
      allowed_write_roots: [],
      require_approval_for: [],
      deny_tools: [],
      safe_tools: []
    }.freeze

    def self.run(argv)
      new(argv).run
    rescue ParseError => error
      warn "AgentConsentReceiptLedger: #{error.message}"
      1
    rescue Errno::ENOENT => error
      warn "AgentConsentReceiptLedger: #{error.message}"
      1
    end

    def initialize(argv)
      @argv = argv
      @options = Marshal.load(Marshal.dump(DEFAULT_OPTIONS))
    end

    def run
      parse!
      if @options[:self_test]
        puts SelfTest.run!
        return 0
      end
      if @options[:example_policy]
        puts ExamplePolicy.to_json
        return 0
      end

      policy = merged_policy
      records = InputParser.parse_paths(@options[:inputs])
      analyzer = Analyzer.new(records, policy).run
      puts Reporter.new(analyzer, policy).render(@options[:format])
      analyzer.failed? ? 2 : 0
    end

    private

    def parse!
      parser = OptionParser.new do |opts|
        opts.banner = "Usage: ruby AgentConsentReceiptLedger.rb [options] [log.json|log.jsonl ...]"
        opts.on("--input PATH", "Read an input JSON or JSONL file. May be repeated.") { |path| @options[:inputs] << path }
        opts.on("--policy PATH", "Read a JSON policy file.") { |path| @options[:policy] = path }
        opts.on("--format FORMAT", "Render json, sarif, or markdown. Default: json.") { |value| @options[:format] = value }
        opts.on("--fail-on SEVERITY", "Exit 2 when this severity or worse appears. Default: high.") { |value| @options[:fail_on] = value }
        opts.on("--allow-domain DOMAIN", "Allow an egress domain or wildcard. May be repeated.") { |value| @options[:allowed_domains] << value }
        opts.on("--allow-write-root PATTERN", "Allow a write resource pattern. May be repeated.") { |value| @options[:allowed_write_roots] << value }
        opts.on("--require-approval PATTERN", "Require approval for a tool wildcard. May be repeated.") { |value| @options[:require_approval_for] << value }
        opts.on("--deny-tool PATTERN", "Deny a tool wildcard. May be repeated.") { |value| @options[:deny_tools] << value }
        opts.on("--safe-tool PATTERN", "Mark a read-only tool wildcard as safe. May be repeated.") { |value| @options[:safe_tools] << value }
        opts.on("--example-policy", "Print a starter policy JSON document.") { @options[:example_policy] = true }
        opts.on("--self-test", "Run built-in regression tests.") { @options[:self_test] = true }
        opts.on("--version", "Print version.") do
          puts VERSION
          exit 0
        end
      end
      parser.parse!(@argv)
      @options[:inputs].concat(@argv)
      unless %w[json sarif markdown].include?(@options[:format])
        raise ParseError, "--format must be json, sarif, or markdown"
      end
    end

    def merged_policy
      base = @options[:policy] ? JSON.parse(File.read(@options[:policy])) : {}
      base = HashTools.stringify_keys(base)
      merge_list!(base, "allowed_domains", @options[:allowed_domains])
      merge_list!(base, "allowed_write_roots", @options[:allowed_write_roots])
      merge_list!(base, "require_approval_for", @options[:require_approval_for])
      merge_list!(base, "deny_tools", @options[:deny_tools])
      merge_list!(base, "safe_tools", @options[:safe_tools])
      base["fail_on"] = @options[:fail_on] if @options[:fail_on]
      Policy.new(base)
    rescue JSON::ParserError => error
      raise ParseError, "#{@options[:policy]}: invalid policy JSON: #{error.message}"
    end

    def merge_list!(hash, key, values)
      return if values.empty?

      current = HashTools.list_value(hash[key])
      hash[key] = (current + values).uniq
    end
  end
end

if __FILE__ == $PROGRAM_NAME
  exit AgentConsentReceiptLedger::CLI.run(ARGV)
end

=begin
This solves the consent receipt gap that shows up when agentic coding systems, MCP tools, browser automations, GitHub connectors, Gmail connectors, calendar connectors, shell commands, and deployment bots all write to real systems but leave only a vague chat transcript behind. Built because April 2026 developer teams need a plain Ruby command they can run in CI to prove that a risky tool call had a specific human approval receipt, a tight tool scope, a matching repository or URL target, a useful expiry time, and no raw secret material in the log. Use it when you are reviewing Codex, Claude, OpenAI Responses API, MCP server, internal agent runner, DevOps bot, SRE automation, research pipeline, data pipeline, or AI coding assistant transcripts before the run can push, deploy, email, delete, archive, or touch production data. The trick: it treats approvals as scoped receipts instead of chat vibes, then checks time, tenant, tool wildcard, resource wildcard, domain allow list, single-use limits, denied decisions, network egress, repo mutation, shell execution, and secret-looking inputs in one deterministic ledger. Drop this into a repository, point it at JSON or JSONL tool-call logs, emit JSON, Markdown, or SARIF, and make your agent governance searchable for phrases like AI agent consent receipt ledger, MCP audit trail, tool call approval gate, autonomous coding assistant safety, GitHub connector approval verification, agent DevOps compliance, and production-ready Ruby security CLI.
=end
