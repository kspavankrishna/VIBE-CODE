local AIGatewayQuota = {}
AIGatewayQuota.__index = AIGatewayQuota
AIGatewayQuota.VERSION = "2026.04"

local MemoryStore = {}
MemoryStore.__index = MemoryStore

local DEFAULTS = {
  namespace = "aiq:v1",
  burst_multiplier = 1.15,
  debt_multiplier = 3,
  bucket_ttl_ms = 600000,
  lease_ttl_ms = 300000,
  cooldown_ttl_ms = 600000,
  lock_ttl_ms = 250,
  lock_timeout_ms = 100,
  lock_sleep_ms = 5,
  fallback_retry_after_ms = 1000,
}

local DURATION_UNITS_MS = {
  ms = 1,
  s = 1000,
  m = 60000,
  h = 3600000,
}

local REQUEST_REMAINING_HEADERS = {
  "x-ratelimit-remaining-requests",
  "ratelimit-remaining-requests",
  "anthropic-ratelimit-requests-remaining",
}

local REQUEST_RESET_HEADERS = {
  "x-ratelimit-reset-requests",
  "ratelimit-reset-requests",
  "anthropic-ratelimit-requests-reset",
}

local TOKEN_REMAINING_HEADERS = {
  "x-ratelimit-remaining-tokens",
  "ratelimit-remaining-tokens",
  "anthropic-ratelimit-tokens-remaining",
}

local TOKEN_RESET_HEADERS = {
  "x-ratelimit-reset-tokens",
  "ratelimit-reset-tokens",
  "anthropic-ratelimit-tokens-reset",
}

local SHARED_REMAINING_HEADERS = {
  "x-ratelimit-remaining",
  "ratelimit-remaining",
}

local SHARED_RESET_HEADERS = {
  "x-ratelimit-reset",
  "ratelimit-reset",
}

local CONCURRENCY_REMAINING_HEADERS = {
  "x-ratelimit-remaining-concurrency",
  "ratelimit-remaining-concurrency",
}

local function is_table(value)
  return type(value) == "table"
end

local function copy_table(source)
  local target = {}
  if not is_table(source) then
    return target
  end
  for key, value in pairs(source) do
    target[key] = value
  end
  return target
end

local function merge_table(target, source, skip_key)
  if not is_table(source) then
    return target
  end
  for key, value in pairs(source) do
    if key ~= skip_key then
      target[key] = value
    end
  end
  return target
end

local function trim(value)
  return (tostring(value or ""):match("^%s*(.-)%s*$"))
end

local function to_number(value, fallback)
  if value == nil then
    return fallback
  end
  if type(value) == "number" then
    return value
  end
  local cleaned = trim(value):gsub(",", "")
  local parsed = tonumber(cleaned)
  if parsed == nil then
    return fallback
  end
  return parsed
end

local function floor_int(value)
  return math.floor(value + 0.0000001)
end

local function round_int(value)
  if value >= 0 then
    return math.floor(value + 0.5)
  end
  return math.ceil(value - 0.5)
end

local function clamp(value, min_value, max_value)
  if value < min_value then
    return min_value
  end
  if value > max_value then
    return max_value
  end
  return value
end

local function ttl_seconds(ms)
  if ms == nil or ms <= 0 then
    return 1
  end
  return math.max(1, math.ceil(ms / 1000))
end

local function now_ms(clock)
  if type(clock) == "function" then
    local value = clock()
    if value > 100000000000 then
      return floor_int(value)
    end
    return floor_int(value * 1000)
  end
  if type(ngx) == "table" and type(ngx.now) == "function" then
    return floor_int(ngx.now() * 1000)
  end
  return os.time() * 1000
end

local function sleep_ms(ms)
  if ms <= 0 then
    return
  end
  if type(ngx) == "table" and type(ngx.sleep) == "function" then
    ngx.sleep(ms / 1000)
    return
  end
  local deadline = os.clock() + (ms / 1000)
  while os.clock() < deadline do
  end
end

local function escape_segment(value)
  local text = tostring(value or "")
  return (text:gsub("[^%w%._%-]", function(ch)
    return string.format("_%02X", string.byte(ch))
  end))
end

local function normalize_headers_map(headers)
  local out = {}
  if not is_table(headers) then
    return out
  end
  for key, value in pairs(headers) do
    local normalized_key = string.lower(tostring(key))
    if type(value) == "table" then
      value = value[1]
    end
    if value ~= nil then
      out[normalized_key] = tostring(value)
    end
  end
  return out
end

local function parse_duration_ms(value)
  if value == nil then
    return nil
  end
  local text = trim(value)
  if text == "" then
    return nil
  end
  local total = 0
  local saw_unit = false
  for amount, unit in text:gmatch("([%+%-]?%d+%.?%d*)(%a+)") do
    local multiplier = DURATION_UNITS_MS[unit]
    if not multiplier then
      return nil
    end
    total = total + (tonumber(amount) * multiplier)
    saw_unit = true
  end
  if saw_unit then
    return math.max(0, round_int(total))
  end
  return nil
end

local function parse_http_time_ms(value)
  if type(ngx) == "table" and type(ngx.parse_http_time) == "function" then
    local parsed = ngx.parse_http_time(value)
    if parsed then
      return parsed * 1000
    end
  end
  return nil
end

local function parse_reset_delta_ms(value, current_ms)
  if value == nil then
    return nil
  end
  local duration_ms = parse_duration_ms(value)
  if duration_ms then
    return duration_ms
  end
  local numeric = to_number(value, nil)
  if numeric then
    if numeric > 1000000000000 then
      return math.max(0, floor_int(numeric - current_ms))
    end
    if numeric > 1000000000 then
      return math.max(0, floor_int((numeric * 1000) - current_ms))
    end
    return math.max(0, floor_int(numeric * 1000))
  end
  local http_time_ms = parse_http_time_ms(value)
  if http_time_ms then
    return math.max(0, http_time_ms - current_ms)
  end
  return nil
end

local function first_number(map, keys)
  for _, key in ipairs(keys) do
    local value = to_number(map[key], nil)
    if value ~= nil then
      return value
    end
  end
  return nil
end

local function first_reset_delta_ms(map, keys, current_ms)
  for _, key in ipairs(keys) do
    local value = parse_reset_delta_ms(map[key], current_ms)
    if value ~= nil then
      return value
    end
  end
  return nil
end

local function encode_bucket_state(tokens, updated_ms)
  return string.format("%.6f|%d", tokens, updated_ms)
end

local function decode_bucket_state(raw, capacity, current_ms)
  if type(raw) ~= "string" then
    return capacity, current_ms
  end
  local tokens_text, updated_text = raw:match("^([%-0-9%.]+)|(%d+)$")
  local tokens = tonumber(tokens_text)
  local updated_ms = tonumber(updated_text)
  if tokens == nil or updated_ms == nil then
    return capacity, current_ms
  end
  return tokens, updated_ms
end

local function encode_lease(lease)
  return table.concat({
    "1",
    lease.subject,
    tostring(lease.provider or "default"),
    tostring(lease.expires_ms or 0),
    tostring(lease.requests or 0),
    tostring(lease.input_tokens or 0),
    tostring(lease.output_tokens or 0),
    tostring(lease.cost_micro or 0),
    tostring(lease.concurrency_reserved or 0),
  }, "|")
end

local function decode_lease(raw)
  if type(raw) ~= "string" then
    return nil
  end
  local version, subject, provider, expires_ms, requests, input_tokens, output_tokens, cost_micro, concurrency_reserved =
    raw:match("^([^|]+)|([^|]+)|([^|]+)|([^|]+)|([^|]+)|([^|]+)|([^|]+)|([^|]+)|([^|]+)$")
  if version ~= "1" then
    return nil
  end
  return {
    subject = subject,
    provider = provider,
    expires_ms = to_number(expires_ms, 0),
    requests = to_number(requests, 0),
    input_tokens = to_number(input_tokens, 0),
    output_tokens = to_number(output_tokens, 0),
    cost_micro = to_number(cost_micro, 0),
    concurrency_reserved = to_number(concurrency_reserved, 0),
  }
end

function MemoryStore.new()
  return setmetatable({ data = {} }, MemoryStore)
end

function MemoryStore:_purge_if_expired(key, current_ms)
  local item = self.data[key]
  if not item then
    return nil
  end
  if item.expires_at_ms and item.expires_at_ms <= current_ms then
    self.data[key] = nil
    return nil
  end
  return item
end

function MemoryStore:get(key)
  local item = self:_purge_if_expired(key, now_ms())
  if item then
    return item.value
  end
  return nil
end

function MemoryStore:set(key, value, ttl_s)
  local expires_at_ms = nil
  if ttl_s and ttl_s > 0 then
    expires_at_ms = now_ms() + (ttl_s * 1000)
  end
  self.data[key] = {
    value = value,
    expires_at_ms = expires_at_ms,
  }
  return true
end

function MemoryStore:add(key, value, ttl_s)
  if self:_purge_if_expired(key, now_ms()) then
    return nil, "exists"
  end
  return self:set(key, value, ttl_s)
end

function MemoryStore:delete(key)
  self.data[key] = nil
  return true
end

local function resolve_store(store)
  if type(store) == "string" then
    if type(ngx) == "table" and type(ngx.shared) == "table" and ngx.shared[store] then
      return ngx.shared[store]
    end
    error("shared dict not found: " .. tostring(store))
  end
  if is_table(store)
    and type(store.get) == "function"
    and type(store.set) == "function"
    and type(store.add) == "function"
    and type(store.delete) == "function" then
    return store
  end
  error("opts.store must be an ngx.shared dict, shared dict name, or MemoryStore")
end

function AIGatewayQuota.new(opts)
  opts = opts or {}

  local self = setmetatable({}, AIGatewayQuota)
  self.store = resolve_store(opts.store or MemoryStore.new())
  self.clock = opts.clock
  self.defaults = copy_table(DEFAULTS)
  merge_table(self.defaults, opts.defaults)
  self.profile_tree = opts.profiles or opts.limits or {}
  self.sequence = 0

  return self
end

function AIGatewayQuota.memory_store()
  return MemoryStore.new()
end

AIGatewayQuota.MemoryStore = MemoryStore

function AIGatewayQuota:subject_key(params)
  if params.scope then
    return escape_segment(params.scope)
  end

  local segments = {
    escape_segment(params.tenant or params.account or params.consumer or "global"),
    escape_segment(params.provider or "default"),
    escape_segment(params.model or "default"),
  }

  if params.route then
    segments[#segments + 1] = escape_segment(params.route)
  end

  return table.concat(segments, ":")
end

function AIGatewayQuota:_normalize_rate_limit(value)
  local parsed = to_number(value, nil)
  if parsed == nil or parsed <= 0 then
    return nil
  end
  return parsed
end

function AIGatewayQuota:_normalize_profile(profile)
  local normalized = copy_table(profile)
  normalized.burst_multiplier = math.max(1, to_number(normalized.burst_multiplier, self.defaults.burst_multiplier))
  normalized.debt_multiplier = math.max(1, to_number(normalized.debt_multiplier, self.defaults.debt_multiplier))
  normalized.lease_ttl_ms = math.max(1000, floor_int(to_number(normalized.lease_ttl_ms, self.defaults.lease_ttl_ms)))
  normalized.fallback_retry_after_ms = math.max(250, floor_int(to_number(normalized.fallback_retry_after_ms, self.defaults.fallback_retry_after_ms)))
  normalized.requests_per_minute = self:_normalize_rate_limit(normalized.requests_per_minute)
  normalized.input_tokens_per_minute = self:_normalize_rate_limit(normalized.input_tokens_per_minute)
  normalized.output_tokens_per_minute = self:_normalize_rate_limit(normalized.output_tokens_per_minute)
  normalized.usd_per_minute = self:_normalize_rate_limit(normalized.usd_per_minute)
  normalized.concurrency = math.max(0, floor_int(to_number(normalized.concurrency, 0)))
  return normalized
end

function AIGatewayQuota:_resolve_profile(params)
  local merged = {}

  merge_table(merged, self.profile_tree.default)

  local provider_config = nil
  if params.provider and is_table(self.profile_tree[params.provider]) then
    provider_config = self.profile_tree[params.provider]
  end

  if provider_config then
    merge_table(merged, provider_config, "models")
    if params.model and is_table(provider_config.models) and is_table(provider_config.models[params.model]) then
      merge_table(merged, provider_config.models[params.model])
    end
  end

  merge_table(merged, params.overrides)

  return self:_normalize_profile(merged)
end

function AIGatewayQuota:_store_set(key, value, ttl_ms)
  local ok, err = self.store:set(key, value, ttl_seconds(ttl_ms))
  if ok == nil or ok == false then
    error("failed to set state for " .. key .. ": " .. tostring(err))
  end
end

function AIGatewayQuota:_store_add(key, value, ttl_ms)
  return self.store:add(key, value, ttl_seconds(ttl_ms))
end

function AIGatewayQuota:_lock_key(subject)
  return self.defaults.namespace .. ":lock:" .. subject
end

function AIGatewayQuota:_bucket_key(subject, bucket_name)
  return self.defaults.namespace .. ":bucket:" .. subject .. ":" .. bucket_name
end

function AIGatewayQuota:_lease_key(lease_id)
  return self.defaults.namespace .. ":lease:" .. lease_id
end

function AIGatewayQuota:_active_key(subject)
  return self.defaults.namespace .. ":active:" .. subject
end

function AIGatewayQuota:_cooldown_key(subject)
  return self.defaults.namespace .. ":cooldown:" .. subject
end

function AIGatewayQuota:_acquire_lock(subject)
  local key = self:_lock_key(subject)
  local started_ms = now_ms(self.clock)
  local token = string.format("%d:%d", started_ms, self.sequence + 1)
  local deadline_ms = started_ms + self.defaults.lock_timeout_ms

  repeat
    local ok = self:_store_add(key, token, self.defaults.lock_ttl_ms)
    if ok then
      return key
    end
    if now_ms(self.clock) >= deadline_ms then
      return nil, "lock_timeout"
    end
    sleep_ms(self.defaults.lock_sleep_ms)
  until false
end

function AIGatewayQuota:_with_subject_lock(subject, fn)
  local lock_key, err = self:_acquire_lock(subject)
  if not lock_key then
    return nil, err
  end

  local ok, a, b, c = pcall(fn)
  self.store:delete(lock_key)
  if not ok then
    error(a)
  end
  return a, b, c
end

function AIGatewayQuota:_bucket_capacity(limit, profile)
  return limit * profile.burst_multiplier
end

function AIGatewayQuota:_bucket_debt_floor(limit, profile)
  return -self:_bucket_capacity(limit, profile) * profile.debt_multiplier
end

function AIGatewayQuota:_bucket_ttl_ms(limit, profile)
  local rate_per_ms = limit / 60000
  if rate_per_ms <= 0 then
    return self.defaults.bucket_ttl_ms
  end
  local refill_to_full_ms = self:_bucket_capacity(limit, profile) / rate_per_ms
  return math.max(self.defaults.bucket_ttl_ms, round_int(refill_to_full_ms * 2))
end

function AIGatewayQuota:_bucket_specs(profile, amounts)
  return {
    {
      name = "requests",
      limit = profile.requests_per_minute,
      amount = amounts.requests,
    },
    {
      name = "input_tokens",
      limit = profile.input_tokens_per_minute,
      amount = amounts.input_tokens,
    },
    {
      name = "output_tokens",
      limit = profile.output_tokens_per_minute,
      amount = amounts.output_tokens,
    },
    {
      name = "cost_micro",
      limit = profile.usd_per_minute and (profile.usd_per_minute * 1000000) or nil,
      amount = amounts.cost_micro,
    },
  }
end

function AIGatewayQuota:_bucket_spec_by_name(profile, bucket_name)
  if bucket_name == "requests" then
    return {
      name = bucket_name,
      limit = profile.requests_per_minute,
      amount = 0,
    }
  end
  if bucket_name == "input_tokens" then
    return {
      name = bucket_name,
      limit = profile.input_tokens_per_minute,
      amount = 0,
    }
  end
  if bucket_name == "output_tokens" then
    return {
      name = bucket_name,
      limit = profile.output_tokens_per_minute,
      amount = 0,
    }
  end
  if bucket_name == "cost_micro" then
    return {
      name = bucket_name,
      limit = profile.usd_per_minute and (profile.usd_per_minute * 1000000) or nil,
      amount = 0,
    }
  end
  return nil
end

function AIGatewayQuota:_load_bucket(subject, spec, profile, current_ms)
  local capacity = self:_bucket_capacity(spec.limit, profile)
  local key = self:_bucket_key(subject, spec.name)
  local raw = self.store:get(key)
  local tokens, updated_ms = decode_bucket_state(raw, capacity, current_ms)
  local rate_per_ms = spec.limit / 60000

  if current_ms > updated_ms and rate_per_ms > 0 then
    tokens = math.min(capacity, tokens + ((current_ms - updated_ms) * rate_per_ms))
    updated_ms = current_ms
  end

  return {
    key = key,
    name = spec.name,
    limit = spec.limit,
    amount = spec.amount,
    tokens = tokens,
    updated_ms = updated_ms,
    capacity = capacity,
    debt_floor = self:_bucket_debt_floor(spec.limit, profile),
    ttl_ms = self:_bucket_ttl_ms(spec.limit, profile),
  }
end

function AIGatewayQuota:_save_bucket(bucket)
  self:_store_set(bucket.key, encode_bucket_state(bucket.tokens, bucket.updated_ms), bucket.ttl_ms)
end

function AIGatewayQuota:_retry_after_ms(bucket, amount)
  if amount <= 0 or bucket.tokens >= amount then
    return 0
  end
  local rate_per_ms = bucket.limit / 60000
  if rate_per_ms <= 0 then
    return 2147483647
  end
  return math.max(1, math.ceil((amount - bucket.tokens) / rate_per_ms))
end

function AIGatewayQuota:_settle_bucket_delta(subject, bucket_name, profile, delta, current_ms)
  if not delta or delta == 0 then
    return
  end

  local spec = self:_bucket_spec_by_name(profile, bucket_name)
  if not spec or not spec.limit or spec.limit <= 0 then
    return
  end

  local bucket = self:_load_bucket(subject, spec, profile, current_ms)
  bucket.tokens = clamp(bucket.tokens - delta, bucket.debt_floor, bucket.capacity)
  bucket.updated_ms = current_ms
  self:_save_bucket(bucket)
end

function AIGatewayQuota:_get_active(subject)
  return to_number(self.store:get(self:_active_key(subject)), 0) or 0
end

function AIGatewayQuota:_set_active(subject, value, ttl_ms)
  local key = self:_active_key(subject)
  if value <= 0 then
    self.store:delete(key)
    return
  end
  self:_store_set(key, tostring(value), math.max(ttl_ms or 0, self.defaults.lease_ttl_ms))
end

function AIGatewayQuota:_get_cooldown_until(subject)
  return to_number(self.store:get(self:_cooldown_key(subject)), 0) or 0
end

function AIGatewayQuota:_set_cooldown_until(subject, until_ms, current_ms)
  if until_ms <= current_ms then
    return
  end
  self:_store_set(
    self:_cooldown_key(subject),
    tostring(until_ms),
    (until_ms - current_ms) + self.defaults.cooldown_ttl_ms
  )
end

function AIGatewayQuota:_next_lease_id(subject, current_ms)
  self.sequence = self.sequence + 1
  local worker_pid = 0
  if type(ngx) == "table" and ngx.worker and type(ngx.worker.pid) == "function" then
    worker_pid = ngx.worker.pid()
  end
  return string.format("%s:%d:%d:%d", subject, current_ms, worker_pid, self.sequence)
end

function AIGatewayQuota:normalize_headers(provider, headers, status, current_ms)
  local normalized = normalize_headers_map(headers)
  local now_value = current_ms or now_ms(self.clock)

  local retry_after_ms = parse_reset_delta_ms(normalized["retry-after-ms"], now_value)
  if retry_after_ms == nil then
    retry_after_ms = parse_reset_delta_ms(normalized["retry-after"], now_value)
  end

  local shared_reset_ms = first_reset_delta_ms(normalized, SHARED_RESET_HEADERS, now_value)

  return {
    provider = provider,
    status = to_number(status, status),
    retry_after_ms = retry_after_ms,
    request_reset_ms = first_reset_delta_ms(normalized, REQUEST_RESET_HEADERS, now_value) or shared_reset_ms,
    token_reset_ms = first_reset_delta_ms(normalized, TOKEN_RESET_HEADERS, now_value) or shared_reset_ms,
    requests_remaining = first_number(normalized, REQUEST_REMAINING_HEADERS) or first_number(normalized, SHARED_REMAINING_HEADERS),
    tokens_remaining = first_number(normalized, TOKEN_REMAINING_HEADERS) or first_number(normalized, SHARED_REMAINING_HEADERS),
    concurrency_remaining = first_number(normalized, CONCURRENCY_REMAINING_HEADERS),
  }
end

function AIGatewayQuota:admit(params)
  if not is_table(params) then
    return nil, "params table required"
  end

  local current_ms = params.now_ms or now_ms(self.clock)
  local subject = self:subject_key(params)
  local profile = self:_resolve_profile(params)
  local amounts = {
    requests = math.max(1, floor_int(to_number(params.request_weight, 1))),
    input_tokens = math.max(0, floor_int(to_number(params.estimated_input_tokens or params.input_tokens, 0))),
    output_tokens = math.max(0, floor_int(to_number(params.estimated_output_tokens or params.output_tokens, 0))),
    cost_micro = math.max(0, round_int(to_number(params.estimated_cost_usd, 0) * 1000000)),
  }

  local result, err = self:_with_subject_lock(subject, function()
    local cooldown_until = self:_get_cooldown_until(subject)
    if cooldown_until > current_ms then
      return {
        allowed = false,
        reason = "cooldown",
        retry_after_ms = cooldown_until - current_ms,
        subject = subject,
      }
    end

    local active = self:_get_active(subject)
    if profile.concurrency > 0 and active >= profile.concurrency then
      return {
        allowed = false,
        reason = "concurrency",
        retry_after_ms = math.max(25, floor_int(profile.lease_ttl_ms / math.max(1, profile.concurrency * 4))),
        subject = subject,
        active = active,
      }
    end

    local buckets = {}
    local denied_bucket = nil
    local denied_retry_ms = 0

    for _, spec in ipairs(self:_bucket_specs(profile, amounts)) do
      if spec.limit and spec.limit > 0 and spec.amount > 0 then
        local bucket = self:_load_bucket(subject, spec, profile, current_ms)
        buckets[#buckets + 1] = bucket
        if bucket.tokens < spec.amount then
          local wait_ms = self:_retry_after_ms(bucket, spec.amount)
          if wait_ms > denied_retry_ms then
            denied_retry_ms = wait_ms
            denied_bucket = spec.name
          end
        end
      end
    end

    if denied_bucket then
      return {
        allowed = false,
        reason = "bucket_exhausted",
        bucket = denied_bucket,
        retry_after_ms = denied_retry_ms,
        subject = subject,
      }
    end

    for _, bucket in ipairs(buckets) do
      bucket.tokens = bucket.tokens - bucket.amount
      bucket.updated_ms = current_ms
      self:_save_bucket(bucket)
    end

    local concurrency_reserved = 0
    if profile.concurrency > 0 then
      concurrency_reserved = 1
      self:_set_active(subject, active + 1, profile.lease_ttl_ms)
    end

    local lease_id = self:_next_lease_id(subject, current_ms)
    local lease = {
      subject = subject,
      provider = escape_segment(params.provider or "default"),
      expires_ms = current_ms + profile.lease_ttl_ms,
      requests = amounts.requests,
      input_tokens = amounts.input_tokens,
      output_tokens = amounts.output_tokens,
      cost_micro = amounts.cost_micro,
      concurrency_reserved = concurrency_reserved,
    }

    self:_store_set(self:_lease_key(lease_id), encode_lease(lease), profile.lease_ttl_ms)

    return {
      allowed = true,
      lease_id = lease_id,
      subject = subject,
      retry_after_ms = 0,
      release = function()
        return self:release(lease_id)
      end,
    }
  end)

  if not result then
    return nil, err
  end

  return result
end

function AIGatewayQuota:release(lease_or_id)
  local lease_id = lease_or_id
  if type(lease_or_id) == "table" then
    lease_id = lease_or_id.lease_id
  end
  if not lease_id then
    return nil, "lease_id required"
  end

  local raw = self.store:get(self:_lease_key(lease_id))
  if not raw then
    return nil, "unknown_lease"
  end

  local lease = decode_lease(raw)
  if not lease then
    return nil, "corrupt_lease"
  end

  local result, err = self:_with_subject_lock(lease.subject, function()
    local current_raw = self.store:get(self:_lease_key(lease_id))
    if not current_raw then
      return {
        released = false,
        reason = "already_released",
      }
    end

    local live_lease = decode_lease(current_raw) or lease
    self.store:delete(self:_lease_key(lease_id))

    if live_lease.concurrency_reserved == 1 then
      local active = self:_get_active(live_lease.subject)
      self:_set_active(live_lease.subject, active - 1, 0)
    end

    return {
      released = true,
      subject = live_lease.subject,
    }
  end)

  if not result then
    return nil, err
  end

  return result
end

function AIGatewayQuota:observe(params)
  if not is_table(params) then
    return nil, "params table required"
  end

  local lease_id = params.lease_id
  if not lease_id and is_table(params.decision) then
    lease_id = params.decision.lease_id
  end
  if not lease_id and is_table(params.lease) then
    lease_id = params.lease.lease_id
  end
  if not lease_id then
    return nil, "lease_id required"
  end

  local raw = self.store:get(self:_lease_key(lease_id))
  if not raw then
    return nil, "unknown_lease"
  end

  local lease = decode_lease(raw)
  if not lease then
    return nil, "corrupt_lease"
  end

  local current_ms = params.now_ms or now_ms(self.clock)
  local provider = params.provider or lease.provider
  local profile = self:_resolve_profile({
    provider = provider,
    model = params.model,
    overrides = params.overrides,
  })
  local feedback = self:normalize_headers(provider, params.headers or {}, params.status, current_ms)

  local result, err = self:_with_subject_lock(lease.subject, function()
    local live_raw = self.store:get(self:_lease_key(lease_id))
    if not live_raw then
      return {
        released = false,
        reason = "already_released",
        feedback = feedback,
      }
    end

    local live_lease = decode_lease(live_raw) or lease
    self.store:delete(self:_lease_key(lease_id))

    if live_lease.concurrency_reserved == 1 then
      local active = self:_get_active(live_lease.subject)
      self:_set_active(live_lease.subject, active - 1, 0)
    end

    local actual_input_tokens = math.max(0, floor_int(to_number(params.actual_input_tokens, live_lease.input_tokens)))
    local actual_output_tokens = math.max(0, floor_int(to_number(params.actual_output_tokens, live_lease.output_tokens)))
    local actual_cost_micro = math.max(
      0,
      round_int(to_number(params.actual_cost_usd, live_lease.cost_micro / 1000000) * 1000000)
    )

    self:_settle_bucket_delta(live_lease.subject, "input_tokens", profile, actual_input_tokens - live_lease.input_tokens, current_ms)
    self:_settle_bucket_delta(live_lease.subject, "output_tokens", profile, actual_output_tokens - live_lease.output_tokens, current_ms)
    self:_settle_bucket_delta(live_lease.subject, "cost_micro", profile, actual_cost_micro - live_lease.cost_micro, current_ms)

    local status_code = floor_int(to_number(params.status, 0) or 0)
    local cooldown_ms = math.max(
      feedback.retry_after_ms or 0,
      feedback.request_reset_ms or 0,
      feedback.token_reset_ms or 0
    )

    if (status_code == 429 or status_code == 503 or status_code == 529) and cooldown_ms == 0 then
      cooldown_ms = profile.fallback_retry_after_ms
    end

    if cooldown_ms > 0 then
      self:_set_cooldown_until(live_lease.subject, current_ms + cooldown_ms, current_ms)
    end

    return {
      released = true,
      subject = live_lease.subject,
      cooldown_ms = cooldown_ms,
      feedback = feedback,
    }
  end)

  if not result then
    return nil, err
  end

  return result
end

function AIGatewayQuota:peek(params)
  params = params or {}
  local current_ms = params.now_ms or now_ms(self.clock)
  local subject = self:subject_key(params)
  local profile = self:_resolve_profile(params)

  local snapshot = {
    subject = subject,
    active = self:_get_active(subject),
    cooldown_ms = math.max(0, self:_get_cooldown_until(subject) - current_ms),
  }

  for _, name in ipairs({ "requests", "input_tokens", "output_tokens", "cost_micro" }) do
    local spec = self:_bucket_spec_by_name(profile, name)
    if spec and spec.limit and spec.limit > 0 then
      local bucket = self:_load_bucket(subject, spec, profile, current_ms)
      snapshot[name] = {
        remaining = bucket.tokens,
        capacity = bucket.capacity,
      }
    end
  end

  return snapshot
end

setmetatable(AIGatewayQuota, {
  __call = function(_, opts)
    return AIGatewayQuota.new(opts)
  end,
})

return AIGatewayQuota

--[[
This solves cross-provider AI rate limiting and token budget control for OpenResty, Kong, APISIX, and Lua gateway stacks that sit in front of LLM APIs. Built because I kept seeing the same April 2026 problem: one gateway fronts OpenAI, Anthropic, Gemini, Groq, DeepSeek, or OpenRouter, but every provider reports limits differently and every retry storm gets expensive fast. Use it when you need per-tenant request limits, input token limits, output token limits, spend limits, cooldown handling, and optional concurrency caps in one Lua module. The trick: it reserves budget from estimates before proxying upstream, then settles the delta after the real usage comes back, so overestimates get refunded and underestimates become debt instead of silent overspend. Drop this into an AI gateway, LLM proxy, agent platform, inference router, or API edge service where predictable token accounting and fewer 429s matter.
]]
