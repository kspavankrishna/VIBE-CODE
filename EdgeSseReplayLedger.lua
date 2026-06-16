local EdgeSseReplayLedger = {
  VERSION = "1.0.0",
}

EdgeSseReplayLedger.__index = EdgeSseReplayLedger

local DEFAULT_PATTERNS = {
  { "([Bb]earer%s+)[%w%._%-]+", "%1[REDACTED]" },
  { "(sk%-)[%w_%-]+", "%1[REDACTED]" },
  { "(gh[pousr]_[%w_%-][%w_%-]*)", "[REDACTED_GITHUB_TOKEN]" },
  { "([Aa][Pp][Ii][-_]?[Kk][Ee][Yy]%s*[:=]%s*)[%w%._%-]+", "%1[REDACTED]" },
  { "([Ss][Ee][Cc][Rr][Ee][Tt]%s*[:=]%s*)[%w%._%-]+", "%1[REDACTED]" },
}

local REDACT_KEYS = {
  "api_key",
  "apikey",
  "authorization",
  "access_token",
  "refresh_token",
  "id_token",
  "token",
  "password",
  "secret",
  "client_secret",
}

local function fail(message)
  error("EdgeSseReplayLedger: " .. message, 2)
end

local function ensure_number(name, value, min_value)
  if type(value) ~= "number" or value ~= value then
    fail(name .. " must be a number")
  end
  if min_value and value < min_value then
    fail(name .. " must be >= " .. tostring(min_value))
  end
  return value
end

local function option_number(options, name, fallback, min_value)
  local value = options[name]
  if value == nil then
    return fallback
  end
  return ensure_number(name, value, min_value)
end

local function now_millis()
  return os.time() * 1000
end

local function safe_string(value)
  if value == nil then
    return ""
  end
  if type(value) == "string" then
    return value
  end
  return tostring(value)
end

local function clean_header_value(value)
  value = safe_string(value)
  value = value:gsub("[\r\n]", " ")
  return value
end

local function copy_patterns(patterns)
  local out = {}
  for i = 1, #patterns do
    out[i] = { patterns[i][1], patterns[i][2] }
  end
  return out
end

local function hash32(text)
  local hash = 5381
  for i = 1, #text do
    hash = (hash * 33 + text:byte(i)) % 4294967296
  end
  return string.format("%08x", hash)
end

local function json_escape(value)
  value = safe_string(value)
  value = value:gsub("\\", "\\\\")
  value = value:gsub('"', '\\"')
  value = value:gsub("\b", "\\b")
  value = value:gsub("\f", "\\f")
  value = value:gsub("\n", "\\n")
  value = value:gsub("\r", "\\r")
  value = value:gsub("\t", "\\t")
  return '"' .. value .. '"'
end

local function starts_with(value, prefix)
  return value:sub(1, #prefix) == prefix
end

local function trim(value)
  return (safe_string(value):gsub("^%s+", ""):gsub("%s+$", ""))
end

local function split_lines(value)
  value = safe_string(value)
  local lines = {}
  if value == "" then
    return lines
  end
  value = value:gsub("\r\n", "\n"):gsub("\r", "\n")
  local start = 1
  while true do
    local pos = value:find("\n", start, true)
    if not pos then
      lines[#lines + 1] = value:sub(start)
      break
    end
    lines[#lines + 1] = value:sub(start, pos - 1)
    start = pos + 1
  end
  return lines
end

local function encode_kv_json(map)
  local keys = {}
  for key in pairs(map) do
    keys[#keys + 1] = key
  end
  table.sort(keys)
  local parts = {}
  for i = 1, #keys do
    local key = keys[i]
    local value = map[key]
    local encoded
    if type(value) == "number" then
      encoded = tostring(value)
    elseif type(value) == "boolean" then
      encoded = value and "true" or "false"
    elseif value == nil then
      encoded = "null"
    else
      encoded = json_escape(value)
    end
    parts[#parts + 1] = json_escape(key) .. ":" .. encoded
  end
  return "{" .. table.concat(parts, ",") .. "}"
end

local function redact_json_keys(text, replacement)
  for _, key in ipairs(REDACT_KEYS) do
    local quoted = '"' .. key .. '"%s*:%s*"[^"]*"'
    text = text:gsub(quoted, '"' .. key .. '":"' .. replacement .. '"')
    local squoted = "'" .. key .. "'%s*:%s*'[^']*'"
    text = text:gsub(squoted, "'" .. key .. "':'" .. replacement .. "'")
  end
  return text
end

local function new_parser(on_event)
  return {
    buffer = "",
    fields_seen = false,
    id = nil,
    event = nil,
    retry = nil,
    data = {},
    comments = 0,
    on_event = on_event,
  }
end

local function parser_reset(parser)
  parser.fields_seen = false
  parser.id = nil
  parser.event = nil
  parser.retry = nil
  parser.data = {}
  parser.comments = 0
end

local function parser_dispatch(parser)
  if not parser.fields_seen then
    parser_reset(parser)
    return
  end
  local event = {
    id = parser.id,
    event = parser.event or "message",
    retry = parser.retry,
    data = table.concat(parser.data, "\n"),
    comments = parser.comments,
  }
  parser_reset(parser)
  parser.on_event(event)
end

local function parser_line(parser, line)
  if line == "" then
    parser_dispatch(parser)
    return
  end
  if starts_with(line, ":") then
    parser.comments = parser.comments + 1
    return
  end

  local field, value = line:match("^([^:]*): ?(.*)$")
  if not field then
    field = line
    value = ""
  end

  parser.fields_seen = true
  if field == "data" then
    parser.data[#parser.data + 1] = value
  elseif field == "event" then
    parser.event = value
  elseif field == "id" then
    if not value:find("\0", 1, true) then
      parser.id = value
    end
  elseif field == "retry" then
    if value:match("^%d+$") then
      parser.retry = tonumber(value)
    end
  end
end

local function parser_feed(parser, chunk)
  if type(chunk) ~= "string" then
    fail("parser chunk must be a string")
  end
  parser.buffer = parser.buffer .. chunk
  while true do
    local pos = parser.buffer:find("\n", 1, true)
    if not pos then
      break
    end
    local line = parser.buffer:sub(1, pos - 1)
    parser.buffer = parser.buffer:sub(pos + 1)
    if line:sub(-1) == "\r" then
      line = line:sub(1, -2)
    end
    parser_line(parser, line)
  end
end

local function parser_finish(parser)
  if parser.buffer ~= "" then
    local line = parser.buffer
    parser.buffer = ""
    if line:sub(-1) == "\r" then
      line = line:sub(1, -2)
    end
    parser_line(parser, line)
  end
  if parser.fields_seen then
    parser_dispatch(parser)
  end
end

function EdgeSseReplayLedger.new(options)
  options = options or {}
  if type(options) ~= "table" then
    fail("options must be a table")
  end

  local self = setmetatable({}, EdgeSseReplayLedger)
  self.max_events = option_number(options, "max_events", 4096, 1)
  self.max_bytes = option_number(options, "max_bytes", 8 * 1024 * 1024, 1024)
  self.default_tenant = options.default_tenant or "default"
  self.redact = options.redact ~= false
  self.redaction_replacement = options.redaction_replacement or "[REDACTED]"
  self.patterns = copy_patterns(options.patterns or DEFAULT_PATTERNS)
  self.clock = options.clock or now_millis
  self.events = {}
  self.by_id = {}
  self.first_seq = 1
  self.next_seq = 1
  self.count = 0
  self.total_bytes = 0
  self.generated = 0
  self.metrics = {
    accepted = 0,
    duplicate = 0,
    conflict = 0,
    evicted = 0,
    redacted = 0,
    stale_replay = 0,
  }
  return self
end

function EdgeSseReplayLedger:redact_data(data)
  data = safe_string(data)
  if not self.redact or data == "" then
    return data, false
  end

  local original = data
  data = redact_json_keys(data, self.redaction_replacement)
  for i = 1, #self.patterns do
    data = data:gsub(self.patterns[i][1], self.patterns[i][2])
  end

  local changed = data ~= original
  if changed then
    self.metrics.redacted = self.metrics.redacted + 1
  end
  return data, changed
end

function EdgeSseReplayLedger:make_id(event)
  if event.id and event.id ~= "" then
    return clean_header_value(event.id)
  end
  self.generated = self.generated + 1
  local seed = table.concat({
    tostring(self.clock()),
    tostring(self.generated),
    safe_string(event.event),
    safe_string(event.data),
  }, ":")
  return "edge-" .. tostring(self.generated) .. "-" .. hash32(seed)
end

function EdgeSseReplayLedger:normalize(event, meta)
  if type(event) ~= "table" then
    fail("event must be a table")
  end
  meta = meta or {}
  local data, redacted = self:redact_data(event.data)
  local id = self:make_id(event)
  local event_name = clean_header_value(event.event or "message")
  if event_name == "" then
    event_name = "message"
  end

  local retry = event.retry
  if retry ~= nil then
    retry = ensure_number("retry", retry, 0)
  end

  local tenant = meta.tenant or event.tenant or self.default_tenant
  tenant = clean_header_value(tenant)
  if tenant == "" then
    tenant = self.default_tenant
  end

  local received_at_ms = meta.received_at_ms or event.received_at_ms or self.clock()
  received_at_ms = ensure_number("received_at_ms", received_at_ms, 0)
  local digest = hash32(table.concat({ id, event_name, data, tenant }, "\30"))
  local bytes = #id + #event_name + #data + #tenant + 64

  return {
    id = id,
    event = event_name,
    data = data,
    retry = retry,
    tenant = tenant,
    received_at_ms = received_at_ms,
    digest = digest,
    bytes = bytes,
    redacted = redacted,
    comments = event.comments or 0,
  }
end

function EdgeSseReplayLedger:evict_if_needed()
  while self.count > self.max_events or self.total_bytes > self.max_bytes do
    local oldest = self.events[self.first_seq]
    self.events[self.first_seq] = nil
    self.first_seq = self.first_seq + 1
    if oldest then
      self.by_id[oldest.id] = nil
      self.total_bytes = self.total_bytes - oldest.bytes
      self.count = self.count - 1
      self.metrics.evicted = self.metrics.evicted + 1
    end
  end
end

function EdgeSseReplayLedger:append(event, meta)
  local normalized = self:normalize(event, meta)
  local existing_seq = self.by_id[normalized.id]
  if existing_seq then
    local existing = self.events[existing_seq]
    if existing and existing.digest == normalized.digest then
      self.metrics.duplicate = self.metrics.duplicate + 1
      return "duplicate", existing
    end
    self.metrics.conflict = self.metrics.conflict + 1
    return "conflict", existing
  end

  local seq = self.next_seq
  self.next_seq = self.next_seq + 1
  normalized.seq = seq
  self.events[seq] = normalized
  self.by_id[normalized.id] = seq
  self.count = self.count + 1
  self.total_bytes = self.total_bytes + normalized.bytes
  self.metrics.accepted = self.metrics.accepted + 1
  self:evict_if_needed()
  return "accepted", normalized
end

function EdgeSseReplayLedger:ingest_sse(chunk, meta)
  local result = {
    accepted = 0,
    duplicate = 0,
    conflict = 0,
  }
  local parser = new_parser(function(event)
    local status = self:append(event, meta)
    result[status] = result[status] + 1
  end)
  parser_feed(parser, chunk)
  parser_finish(parser)
  return result
end

function EdgeSseReplayLedger:ingest_lines(lines, meta)
  local accepted = 0
  for _, line in ipairs(lines) do
    local event = {
      event = "message",
      data = line,
    }
    local status = self:append(event, meta)
    if status == "accepted" then
      accepted = accepted + 1
    end
  end
  return accepted
end

function EdgeSseReplayLedger:first_available_id()
  for seq = self.first_seq, self.next_seq - 1 do
    local event = self.events[seq]
    if event then
      return event.id
    end
  end
  return nil
end

function EdgeSseReplayLedger:last_available_id()
  for seq = self.next_seq - 1, self.first_seq, -1 do
    local event = self.events[seq]
    if event then
      return event.id
    end
  end
  return nil
end

function EdgeSseReplayLedger:replay_after(last_event_id, options)
  options = options or {}
  local limit_events = options.limit_events or self.max_events
  local limit_bytes = options.limit_bytes or self.max_bytes
  local tenant = options.tenant
  local include_tail_on_stale = options.include_tail_on_stale == true
  ensure_number("limit_events", limit_events, 1)
  ensure_number("limit_bytes", limit_bytes, 1)

  local start_seq = self.first_seq
  local status = "ok"
  if last_event_id and last_event_id ~= "" then
    local seq = self.by_id[last_event_id]
    if seq then
      start_seq = seq + 1
    else
      status = "stale"
      self.metrics.stale_replay = self.metrics.stale_replay + 1
      if not include_tail_on_stale then
        return {
          status = status,
          events = {},
          first_available_id = self:first_available_id(),
          last_available_id = self:last_available_id(),
        }
      end
    end
  end

  local replay = {}
  local bytes = 0
  for seq = start_seq, self.next_seq - 1 do
    local event = self.events[seq]
    if event and (not tenant or event.tenant == tenant) then
      if #replay >= limit_events then
        break
      end
      if bytes + event.bytes > limit_bytes then
        break
      end
      replay[#replay + 1] = event
      bytes = bytes + event.bytes
    end
  end

  return {
    status = status,
    events = replay,
    bytes = bytes,
    first_available_id = self:first_available_id(),
    last_available_id = self:last_available_id(),
  }
end

function EdgeSseReplayLedger.to_sse(event)
  if type(event) ~= "table" then
    fail("event must be a table")
  end
  local out = {}
  if event.id and event.id ~= "" then
    out[#out + 1] = "id: " .. clean_header_value(event.id)
  end
  if event.event and event.event ~= "message" then
    out[#out + 1] = "event: " .. clean_header_value(event.event)
  end
  if event.retry then
    out[#out + 1] = "retry: " .. tostring(math.floor(event.retry))
  end
  local lines = split_lines(event.data or "")
  if #lines == 0 then
    out[#out + 1] = "data:"
  else
    for i = 1, #lines do
      out[#out + 1] = "data: " .. lines[i]
    end
  end
  out[#out + 1] = ""
  return table.concat(out, "\n") .. "\n"
end

function EdgeSseReplayLedger:event_to_json(event)
  local parts = {
    json_escape("id") .. ":" .. json_escape(event.id),
    json_escape("event") .. ":" .. json_escape(event.event),
    json_escape("tenant") .. ":" .. json_escape(event.tenant),
    json_escape("data") .. ":" .. json_escape(event.data),
    json_escape("digest") .. ":" .. json_escape(event.digest),
    json_escape("received_at_ms") .. ":" .. tostring(event.received_at_ms),
    json_escape("bytes") .. ":" .. tostring(event.bytes),
  }
  if event.retry then
    parts[#parts + 1] = json_escape("retry") .. ":" .. tostring(event.retry)
  end
  return "{" .. table.concat(parts, ",") .. "}"
end

function EdgeSseReplayLedger:replay_to_json(replay)
  local events = {}
  for i = 1, #replay.events do
    events[i] = self:event_to_json(replay.events[i])
  end
  local head = {
    json_escape("status") .. ":" .. json_escape(replay.status),
    json_escape("bytes") .. ":" .. tostring(replay.bytes or 0),
    json_escape("first_available_id") .. ":" .. (replay.first_available_id and json_escape(replay.first_available_id) or "null"),
    json_escape("last_available_id") .. ":" .. (replay.last_available_id and json_escape(replay.last_available_id) or "null"),
    json_escape("events") .. ":[" .. table.concat(events, ",") .. "]",
  }
  return "{" .. table.concat(head, ",") .. "}"
end

function EdgeSseReplayLedger:summary()
  return {
    version = EdgeSseReplayLedger.VERSION,
    count = self.count,
    total_bytes = self.total_bytes,
    max_events = self.max_events,
    max_bytes = self.max_bytes,
    first_seq = self.first_seq,
    next_seq = self.next_seq,
    first_available_id = self:first_available_id(),
    last_available_id = self:last_available_id(),
    accepted = self.metrics.accepted,
    duplicate = self.metrics.duplicate,
    conflict = self.metrics.conflict,
    evicted = self.metrics.evicted,
    redacted = self.metrics.redacted,
    stale_replay = self.metrics.stale_replay,
  }
end

function EdgeSseReplayLedger:summary_json()
  return encode_kv_json(self:summary())
end

function EdgeSseReplayLedger:backpressure_headers(replay)
  replay = replay or {}
  local headers = {
    ["x-sse-ledger-events"] = tostring(self.count),
    ["x-sse-ledger-bytes"] = tostring(self.total_bytes),
    ["x-sse-ledger-first-id"] = self:first_available_id() or "",
    ["x-sse-ledger-last-id"] = self:last_available_id() or "",
    ["x-sse-ledger-replay-status"] = replay.status or "ok",
  }
  if replay.status == "stale" then
    headers["retry-after"] = "1"
  end
  return headers
end

local function read_stdin()
  local chunks = {}
  while true do
    local chunk = io.read(8192)
    if not chunk then
      break
    end
    chunks[#chunks + 1] = chunk
  end
  return table.concat(chunks)
end

local function usage()
  return table.concat({
    "usage: lua EdgeSseReplayLedger.lua [options] < stream.sse",
    "",
    "options:",
    "  --max-events N          bounded event retention, default 4096",
    "  --max-bytes N           bounded byte retention, default 8388608",
    "  --tenant NAME           tenant label to attach to stdin events",
    "  --last-event-id ID      replay events after this SSE id",
    "  --include-tail-on-stale emit retained tail when Last-Event-ID has aged out",
    "  --raw-replay            print replay as text/event-stream",
    "  --json                  print JSON summary or JSON replay metadata",
    "  --no-redact             disable default secret redaction",
    "  --self-test             run embedded checks",
    "  --help                  show this help",
  }, "\n")
end

local function parse_args(argv)
  local config = {
    max_events = 4096,
    max_bytes = 8 * 1024 * 1024,
    tenant = "default",
    last_event_id = nil,
    include_tail_on_stale = false,
    raw_replay = false,
    json = false,
    redact = true,
    self_test = false,
  }

  local i = 1
  while i <= #argv do
    local item = argv[i]
    if item == "--max-events" then
      i = i + 1
      config.max_events = tonumber(argv[i]) or fail("--max-events requires a number")
    elseif item == "--max-bytes" then
      i = i + 1
      config.max_bytes = tonumber(argv[i]) or fail("--max-bytes requires a number")
    elseif item == "--tenant" then
      i = i + 1
      config.tenant = argv[i] or fail("--tenant requires a value")
    elseif item == "--last-event-id" then
      i = i + 1
      config.last_event_id = argv[i] or fail("--last-event-id requires a value")
    elseif item == "--include-tail-on-stale" then
      config.include_tail_on_stale = true
    elseif item == "--raw-replay" then
      config.raw_replay = true
    elseif item == "--json" then
      config.json = true
    elseif item == "--no-redact" then
      config.redact = false
    elseif item == "--self-test" then
      config.self_test = true
    elseif item == "--help" or item == "-h" then
      config.help = true
    else
      fail("unknown option " .. tostring(item))
    end
    i = i + 1
  end
  return config
end

local function expect(condition, message)
  if not condition then
    error("self-test failed: " .. message, 2)
  end
end

local function run_self_test()
  local tick = 1700000000000
  local ledger = EdgeSseReplayLedger.new({
    max_events = 3,
    max_bytes = 4096,
    clock = function()
      tick = tick + 1
      return tick
    end,
  })

  local input = table.concat({
    "id: a",
    "event: token",
    "data: hello",
    "",
    "id: b",
    "data: {\"api_key\":\"sk-live-secret\",\"ok\":true}",
    "",
    "id: c",
    "data: third",
    "",
    "id: d",
    "data: fourth",
    "",
  }, "\n")

  local result = ledger:ingest_sse(input, { tenant = "acme" })
  expect(result.accepted == 4, "accepts four events")
  expect(ledger.count == 3, "evicts to max_events")
  expect(ledger:first_available_id() == "b", "oldest id is b after eviction")
  expect(ledger.metrics.redacted == 1, "redacts one event")

  local replay = ledger:replay_after("b")
  expect(replay.status == "ok", "known id replay is ok")
  expect(#replay.events == 2, "replays c and d")
  expect(replay.events[1].id == "c", "first replay id is c")

  local stale = ledger:replay_after("a")
  expect(stale.status == "stale", "evicted id is stale")
  expect(#stale.events == 0, "stale replay is empty unless requested")

  local tail = ledger:replay_after("a", { include_tail_on_stale = true })
  expect(tail.status == "stale", "tail marks stale")
  expect(#tail.events == 3, "tail returns retained events")

  local status, existing = ledger:append({ id = "d", data = "fourth" }, { tenant = "acme" })
  expect(status == "duplicate", "same id and digest is duplicate")
  expect(existing.id == "d", "duplicate returns existing event")

  local conflict = ledger:append({ id = "d", data = "changed" }, { tenant = "acme" })
  expect(conflict == "conflict", "same id with changed data conflicts")

  local sse = EdgeSseReplayLedger.to_sse({ id = "x", event = "delta", data = "one\ntwo", retry = 2500 })
  expect(sse:find("id: x", 1, true) ~= nil, "serializes id")
  expect(sse:find("data: two", 1, true) ~= nil, "serializes multiline data")

  return "self-test ok"
end

local function main(argv)
  local config = parse_args(argv)
  if config.help then
    io.write(usage(), "\n")
    return 0
  end
  if config.self_test then
    io.write(run_self_test(), "\n")
    return 0
  end

  local ledger = EdgeSseReplayLedger.new({
    max_events = config.max_events,
    max_bytes = config.max_bytes,
    redact = config.redact,
  })
  local input = read_stdin()
  ledger:ingest_sse(input, { tenant = config.tenant })

  if config.last_event_id then
    local replay = ledger:replay_after(config.last_event_id, {
      tenant = config.tenant,
      include_tail_on_stale = config.include_tail_on_stale,
    })
    if config.raw_replay then
      for i = 1, #replay.events do
        io.write(EdgeSseReplayLedger.to_sse(replay.events[i]))
      end
    else
      io.write(ledger:replay_to_json(replay), "\n")
    end
    return 0
  end

  if config.json then
    io.write(ledger:summary_json(), "\n")
  else
    local summary = ledger:summary()
    io.write("events=", summary.count, " bytes=", summary.total_bytes)
    io.write(" first=", summary.first_available_id or "")
    io.write(" last=", summary.last_available_id or "")
    io.write(" accepted=", summary.accepted)
    io.write(" duplicate=", summary.duplicate)
    io.write(" conflict=", summary.conflict)
    io.write(" evicted=", summary.evicted)
    io.write(" redacted=", summary.redacted, "\n")
  end
  return 0
end

EdgeSseReplayLedger._main = main
EdgeSseReplayLedger._self_test = run_self_test

if ... == nil then
  local ok, err = pcall(main, arg or {})
  if not ok then
    io.stderr:write(tostring(err), "\n")
    os.exit(1)
  end
end

return EdgeSseReplayLedger

--[[
This solves the April 2026 developer problem of keeping AI chat, coding agent, RAG, and tool-calling Server-Sent Events reliable when a browser tab, mobile network, CDN hop, or edge worker disconnects halfway through a streaming response. Built because I keep seeing teams treat Last-Event-ID as an afterthought, then lose model deltas, leak secrets into replay logs, or replay the wrong tenant's stream during a support incident. Use it when you run an OpenResty gateway, Lua sidecar, API proxy, local DevOps harness, streaming LLM test rig, or research inference queue that needs deterministic SSE parsing, bounded replay, redaction, idempotency, stale cursor detection, and plain JSON diagnostics without a database. The trick: the file does the boring parts carefully in one place, including multiline data fields, generated ids, duplicate and conflict detection, byte-capped retention, tenant filters, safe text/event-stream serialization, and embedded self-tests. Drop this into an AI gateway repository, edge compute proxy, developer productivity tool, CI replay checker, model streaming observability pipeline, or MCP agent infrastructure project when search terms like SSE replay ledger, AI streaming reliability, Last-Event-ID recovery, Lua OpenResty LLM gateway, token stream audit log, and production Server-Sent Events backpressure actually need working code instead of another blog post.
]]
