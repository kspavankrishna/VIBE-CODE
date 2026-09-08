#!/usr/bin/env lua

local VERSION = "2026.04.1"
local JSON_NULL = {}

local function stderr(line)
  io.stderr:write(line, "\n")
end

local function die(message, code)
  stderr("error: " .. tostring(message))
  os.exit(code or 1)
end

local function trim(value)
  return (tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", ""))
end

local function lower(value)
  return string.lower(tostring(value or ""))
end

local function starts_with(value, prefix)
  return value:sub(1, #prefix) == prefix
end

local function read_all(path)
  local handle, err
  if path and path ~= "-" then
    handle, err = io.open(path, "rb")
  else
    handle = io.stdin
  end
  if not handle then
    die("cannot open " .. tostring(path) .. ": " .. tostring(err))
  end
  local data = handle:read("*a") or ""
  if handle ~= io.stdin then
    handle:close()
  end
  return data
end

local function read_lines(path)
  local data = read_all(path)
  local lines = {}
  data = data:gsub("\r\n", "\n"):gsub("\r", "\n")
  if #data == 0 then
    return lines
  end
  if data:sub(-1) ~= "\n" then
    data = data .. "\n"
  end
  for line in data:gmatch("(.-)\n") do
    lines[#lines + 1] = line
  end
  return lines
end

local function utf8_from_codepoint(codepoint)
  if codepoint <= 0x7f then
    return string.char(codepoint)
  elseif codepoint <= 0x7ff then
    local b1 = 0xc0 + math.floor(codepoint / 0x40)
    local b2 = 0x80 + (codepoint % 0x40)
    return string.char(b1, b2)
  elseif codepoint <= 0xffff then
    local b1 = 0xe0 + math.floor(codepoint / 0x1000)
    local b2 = 0x80 + (math.floor(codepoint / 0x40) % 0x40)
    local b3 = 0x80 + (codepoint % 0x40)
    return string.char(b1, b2, b3)
  elseif codepoint <= 0x10ffff then
    local b1 = 0xf0 + math.floor(codepoint / 0x40000)
    local b2 = 0x80 + (math.floor(codepoint / 0x1000) % 0x40)
    local b3 = 0x80 + (math.floor(codepoint / 0x40) % 0x40)
    local b4 = 0x80 + (codepoint % 0x40)
    return string.char(b1, b2, b3, b4)
  end
  error("invalid unicode codepoint " .. tostring(codepoint))
end

local JsonParser = {}
JsonParser.__index = JsonParser

function JsonParser:new(source)
  return setmetatable({ source = source, pos = 1, len = #source }, self)
end

function JsonParser:peek()
  return self.source:sub(self.pos, self.pos)
end

function JsonParser:skip_space()
  while self.pos <= self.len do
    local c = self:peek()
    if c == " " or c == "\n" or c == "\r" or c == "\t" then
      self.pos = self.pos + 1
    else
      return
    end
  end
end

function JsonParser:expect(text)
  if self.source:sub(self.pos, self.pos + #text - 1) ~= text then
    error("expected " .. text .. " at byte " .. self.pos)
  end
  self.pos = self.pos + #text
end

function JsonParser:parse_string()
  self:expect('"')
  local out = {}
  while self.pos <= self.len do
    local c = self:peek()
    self.pos = self.pos + 1
    if c == '"' then
      return table.concat(out)
    elseif c == "\\" then
      local esc = self:peek()
      self.pos = self.pos + 1
      if esc == '"' or esc == "\\" or esc == "/" then
        out[#out + 1] = esc
      elseif esc == "b" then
        out[#out + 1] = "\b"
      elseif esc == "f" then
        out[#out + 1] = "\f"
      elseif esc == "n" then
        out[#out + 1] = "\n"
      elseif esc == "r" then
        out[#out + 1] = "\r"
      elseif esc == "t" then
        out[#out + 1] = "\t"
      elseif esc == "u" then
        local hex = self.source:sub(self.pos, self.pos + 3)
        if not hex:match("^[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]$") then
          error("bad unicode escape at byte " .. self.pos)
        end
        self.pos = self.pos + 4
        local code = tonumber(hex, 16)
        if code >= 0xd800 and code <= 0xdbff and self.source:sub(self.pos, self.pos + 1) == "\\u" then
          self.pos = self.pos + 2
          local low_hex = self.source:sub(self.pos, self.pos + 3)
          if not low_hex:match("^[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]$") then
            error("bad low surrogate at byte " .. self.pos)
          end
          self.pos = self.pos + 4
          local low = tonumber(low_hex, 16)
          if low < 0xdc00 or low > 0xdfff then
            error("invalid low surrogate at byte " .. (self.pos - 4))
          end
          code = 0x10000 + ((code - 0xd800) * 0x400) + (low - 0xdc00)
        end
        out[#out + 1] = utf8_from_codepoint(code)
      else
        error("bad escape at byte " .. (self.pos - 1))
      end
    else
      local byte = c:byte()
      if byte and byte < 32 then
        error("control character in string at byte " .. (self.pos - 1))
      end
      out[#out + 1] = c
    end
  end
  error("unterminated string")
end

function JsonParser:parse_number()
  local start = self.pos
  if self:peek() == "-" then
    self.pos = self.pos + 1
  end
  if self:peek() == "0" then
    self.pos = self.pos + 1
  else
    if not self:peek():match("%d") then
      error("bad number at byte " .. self.pos)
    end
    while self:peek():match("%d") do
      self.pos = self.pos + 1
    end
  end
  if self:peek() == "." then
    self.pos = self.pos + 1
    if not self:peek():match("%d") then
      error("bad fractional number at byte " .. self.pos)
    end
    while self:peek():match("%d") do
      self.pos = self.pos + 1
    end
  end
  local p = self:peek()
  if p == "e" or p == "E" then
    self.pos = self.pos + 1
    p = self:peek()
    if p == "+" or p == "-" then
      self.pos = self.pos + 1
    end
    if not self:peek():match("%d") then
      error("bad exponent at byte " .. self.pos)
    end
    while self:peek():match("%d") do
      self.pos = self.pos + 1
    end
  end
  local raw = self.source:sub(start, self.pos - 1)
  local value = tonumber(raw)
  if value == nil then
    error("number out of range at byte " .. start)
  end
  return value
end

function JsonParser:parse_array()
  self:expect("[")
  self:skip_space()
  local out = {}
  if self:peek() == "]" then
    self.pos = self.pos + 1
    return out
  end
  while true do
    out[#out + 1] = self:parse_value()
    self:skip_space()
    local c = self:peek()
    if c == "]" then
      self.pos = self.pos + 1
      return out
    elseif c == "," then
      self.pos = self.pos + 1
      self:skip_space()
    else
      error("expected comma or ] at byte " .. self.pos)
    end
  end
end

function JsonParser:parse_object()
  self:expect("{")
  self:skip_space()
  local out = {}
  if self:peek() == "}" then
    self.pos = self.pos + 1
    return out
  end
  while true do
    if self:peek() ~= '"' then
      error("expected object key at byte " .. self.pos)
    end
    local key = self:parse_string()
    self:skip_space()
    self:expect(":")
    self:skip_space()
    out[key] = self:parse_value()
    self:skip_space()
    local c = self:peek()
    if c == "}" then
      self.pos = self.pos + 1
      return out
    elseif c == "," then
      self.pos = self.pos + 1
      self:skip_space()
    else
      error("expected comma or } at byte " .. self.pos)
    end
  end
end

function JsonParser:parse_value()
  self:skip_space()
  local c = self:peek()
  if c == '"' then
    return self:parse_string()
  elseif c == "{" then
    return self:parse_object()
  elseif c == "[" then
    return self:parse_array()
  elseif c == "t" then
    self:expect("true")
    return true
  elseif c == "f" then
    self:expect("false")
    return false
  elseif c == "n" then
    self:expect("null")
    return JSON_NULL
  elseif c == "-" or c:match("%d") then
    return self:parse_number()
  end
  error("unexpected JSON value at byte " .. self.pos)
end

local function json_decode(source)
  local parser = JsonParser:new(source)
  local value = parser:parse_value()
  parser:skip_space()
  if parser.pos <= parser.len then
    error("trailing data at byte " .. parser.pos)
  end
  return value
end

local function json_quote(value)
  local out = {'"'}
  for i = 1, #value do
    local c = value:sub(i, i)
    local b = c:byte()
    if c == '"' then
      out[#out + 1] = '\\"'
    elseif c == "\\" then
      out[#out + 1] = "\\\\"
    elseif c == "\b" then
      out[#out + 1] = "\\b"
    elseif c == "\f" then
      out[#out + 1] = "\\f"
    elseif c == "\n" then
      out[#out + 1] = "\\n"
    elseif c == "\r" then
      out[#out + 1] = "\\r"
    elseif c == "\t" then
      out[#out + 1] = "\\t"
    elseif b < 32 then
      out[#out + 1] = string.format("\\u%04x", b)
    else
      out[#out + 1] = c
    end
  end
  out[#out + 1] = '"'
  return table.concat(out)
end

local function is_array(tbl)
  local max = 0
  local count = 0
  for key, _ in pairs(tbl) do
    if type(key) ~= "number" or key < 1 or key % 1 ~= 0 then
      return false
    end
    if key > max then
      max = key
    end
    count = count + 1
  end
  return max == count
end

local function sorted_keys(tbl)
  local keys = {}
  for key, _ in pairs(tbl) do
    keys[#keys + 1] = key
  end
  table.sort(keys, function(a, b) return tostring(a) < tostring(b) end)
  return keys
end

local function json_encode(value)
  local kind = type(value)
  if value == JSON_NULL then
    return "null"
  elseif kind == "nil" then
    return "null"
  elseif kind == "boolean" then
    return value and "true" or "false"
  elseif kind == "number" then
    if value ~= value or value == math.huge or value == -math.huge then
      error("cannot encode non-finite number")
    end
    return string.format("%.17g", value)
  elseif kind == "string" then
    return json_quote(value)
  elseif kind == "table" then
    if is_array(value) then
      local out = {}
      for i = 1, #value do
        out[i] = json_encode(value[i])
      end
      return "[" .. table.concat(out, ",") .. "]"
    end
    local out = {}
    for _, key in ipairs(sorted_keys(value)) do
      out[#out + 1] = json_quote(tostring(key)) .. ":" .. json_encode(value[key])
    end
    return "{" .. table.concat(out, ",") .. "}"
  end
  error("cannot encode " .. kind)
end

local function split_escaped(value, separator)
  local out, cell = {}, {}
  local escaped = false
  for i = 1, #value do
    local c = value:sub(i, i)
    if escaped then
      cell[#cell + 1] = c
      escaped = false
    elseif c == "\\" then
      escaped = true
    elseif c == separator then
      out[#out + 1] = trim(table.concat(cell))
      cell = {}
    else
      cell[#cell + 1] = c
    end
  end
  if escaped then
    cell[#cell + 1] = "\\"
  end
  out[#out + 1] = trim(table.concat(cell))
  return out
end

local function escape_lua_pattern(c)
  if c:match("[%^%$%(%)%%%.%[%]%+%-%?]") then
    return "%" .. c
  end
  return c
end

local function glob_to_pattern(glob)
  local out = {"^"}
  local i = 1
  while i <= #glob do
    local c = glob:sub(i, i)
    local n = glob:sub(i + 1, i + 1)
    if c == "*" and n == "*" then
      out[#out + 1] = ".*"
      i = i + 2
    elseif c == "*" then
      out[#out + 1] = "[^/]*"
      i = i + 1
    elseif c == "?" then
      out[#out + 1] = "[^/]"
      i = i + 1
    else
      out[#out + 1] = escape_lua_pattern(c)
      i = i + 1
    end
  end
  out[#out + 1] = "$"
  return table.concat(out)
end

local function glob_match(glob, value)
  return tostring(value or ""):match(glob_to_pattern(tostring(glob or ""))) ~= nil
end

local SEVERITY = { none = 0, low = 1, medium = 2, moderate = 2, high = 3, critical = 4, block = 5 }

local function path_get(root, path)
  local current = root
  for part in tostring(path or ""):gmatch("[^%.]+") do
    if type(current) ~= "table" then
      return nil
    end
    current = current[part]
    if current == JSON_NULL then
      return nil
    end
  end
  return current
end

local function stringify(value)
  if value == nil or value == JSON_NULL then
    return ""
  elseif type(value) == "table" then
    return json_encode(value)
  end
  return tostring(value)
end

local function compare_values(actual, expected)
  local an = tonumber(actual)
  local en = tonumber(expected)
  if an ~= nil and en ~= nil then
    if an < en then return -1 end
    if an > en then return 1 end
    return 0
  end
  local ar = SEVERITY[lower(actual)]
  local er = SEVERITY[lower(expected)]
  if ar ~= nil and er ~= nil then
    if ar < er then return -1 end
    if ar > er then return 1 end
    return 0
  end
  local a = stringify(actual)
  local e = stringify(expected)
  if a < e then return -1 end
  if a > e then return 1 end
  return 0
end

local OPERATORS = {"!~", ">=", "<=", "!=", "~", "=", ">", "<"}

local function parse_constraint(raw)
  raw = trim(raw)
  if raw == "" or raw == "true" then
    return nil
  end
  for _, op in ipairs(OPERATORS) do
    local start_pos, end_pos = raw:find(op, 1, true)
    if start_pos then
      local field = trim(raw:sub(1, start_pos - 1))
      local expected = trim(raw:sub(end_pos + 1))
      if field == "" or expected == "" then
        error("bad constraint: " .. raw)
      end
      return { field = field, op = op, expected = expected, raw = raw }
    end
  end
  error("constraint lacks operator: " .. raw)
end

local function parse_constraints(raw)
  local constraints = {}
  for _, item in ipairs(split_escaped(raw or "", ";")) do
    local constraint = parse_constraint(item)
    if constraint then
      constraints[#constraints + 1] = constraint
    end
  end
  return constraints
end

local function constraint_matches(event, constraint)
  local actual = path_get(event, constraint.field)
  local op = constraint.op
  local expected = constraint.expected
  if op == "~" then
    return glob_match(expected, stringify(actual))
  elseif op == "!~" then
    return not glob_match(expected, stringify(actual))
  elseif op == "=" then
    return compare_values(actual, expected) == 0
  elseif op == "!=" then
    return compare_values(actual, expected) ~= 0
  elseif op == ">" then
    return compare_values(actual, expected) > 0
  elseif op == "<" then
    return compare_values(actual, expected) < 0
  elseif op == ">=" then
    return compare_values(actual, expected) >= 0
  elseif op == "<=" then
    return compare_values(actual, expected) <= 0
  end
  return false
end

local function rule_score(rule)
  local text = table.concat({ rule.subject, rule.tool, rule.resource }, "|")
  local wildcards = select(2, text:gsub("%*", "")) + select(2, text:gsub("%?", ""))
  return (#text - wildcards * 8) + (#rule.constraints * 20)
end

local function parse_budget(raw)
  raw = trim(raw or "")
  if raw == "" or raw == "-" then
    return nil
  end
  local value = tonumber(raw)
  if not value or value < 0 then
    error("bad budget_ms: " .. raw)
  end
  return value
end

local function parse_policy_line(line, line_number)
  local fields = split_escaped(line, "|")
  if #fields < 5 then
    error("line " .. line_number .. " needs effect|subject|tool|resource|constraints")
  end
  local effect = lower(fields[1])
  if effect ~= "allow" and effect ~= "deny" then
    error("line " .. line_number .. " has unknown effect " .. fields[1])
  end
  local rule = {
    id = "R" .. tostring(line_number),
    line = line_number,
    effect = effect,
    subject = fields[2] ~= "" and fields[2] or "*",
    tool = fields[3] ~= "" and fields[3] or "*",
    resource = fields[4] ~= "" and fields[4] or "*",
    constraints = parse_constraints(fields[5] or ""),
    budget_ms = parse_budget(fields[6]),
    reason = fields[7] or "",
  }
  rule.score = rule_score(rule)
  return rule
end

local function load_policy(path)
  local rules = {}
  for line_number, line in ipairs(read_lines(path)) do
    local clean = trim(line)
    if clean ~= "" and not starts_with(clean, "#") then
      if lower(clean):match("^effect%s*|") then
        -- header row
      else
        local ok, rule_or_error = pcall(parse_policy_line, clean, line_number)
        if not ok then
          die("policy " .. tostring(path) .. ": " .. tostring(rule_or_error))
        end
        rules[#rules + 1] = rule_or_error
      end
    end
  end
  if #rules == 0 then
    die("policy has no rules: " .. tostring(path))
  end
  table.sort(rules, function(a, b)
    if a.score == b.score then
      return a.line < b.line
    end
    return a.score > b.score
  end)
  return rules
end

local function first_non_empty(...)
  for i = 1, select("#", ...) do
    local value = select(i, ...)
    if value ~= nil and value ~= JSON_NULL and stringify(value) ~= "" then
      return stringify(value)
    end
  end
  return nil
end

local function normalized_field(event, name)
  if name == "subject" then
    return first_non_empty(event.subject, event.actor, event.principal, event.agent, event.agent_id, event.user, "unknown")
  elseif name == "tool" then
    return first_non_empty(event.tool, event.tool_name, event.name, event.method, event.rpc, "unknown")
  elseif name == "resource" then
    return first_non_empty(event.resource, event.target, event.repository, event.url, event.path, path_get(event, "args.path"), "*")
  end
  return ""
end

local function rule_matches(rule, event)
  if not glob_match(rule.subject, normalized_field(event, "subject")) then
    return false
  end
  if not glob_match(rule.tool, normalized_field(event, "tool")) then
    return false
  end
  if not glob_match(rule.resource, normalized_field(event, "resource")) then
    return false
  end
  for _, constraint in ipairs(rule.constraints) do
    if not constraint_matches(event, constraint) then
      return false
    end
  end
  return true
end

local function evaluate_event(rules, event, opts)
  local matching_denies, matching_allows = {}, {}
  for _, rule in ipairs(rules) do
    if rule_matches(rule, event) then
      if rule.effect == "deny" then
        matching_denies[#matching_denies + 1] = rule
      else
        matching_allows[#matching_allows + 1] = rule
      end
    end
  end

  local decision = {
    subject = normalized_field(event, "subject"),
    tool = normalized_field(event, "tool"),
    resource = normalized_field(event, "resource"),
    action = "deny",
    reason = "no matching allow rule",
    matched_rules = {},
    warnings = {},
  }

  if #matching_denies > 0 then
    local rule = matching_denies[1]
    decision.action = "deny"
    decision.reason = rule.reason ~= "" and rule.reason or ("matched deny rule " .. rule.id)
    decision.matched_rules[#decision.matched_rules + 1] = rule.id
    return decision
  end

  if #matching_allows > 0 then
    local rule = matching_allows[1]
    decision.action = "allow"
    decision.reason = rule.reason ~= "" and rule.reason or ("matched allow rule " .. rule.id)
    decision.matched_rules[#decision.matched_rules + 1] = rule.id
    local latency = tonumber(first_non_empty(event.latency_ms, event.duration_ms, event.elapsed_ms))
    if rule.budget_ms and latency and latency > rule.budget_ms then
      decision.warnings[#decision.warnings + 1] = "latency " .. tostring(latency) .. "ms exceeded rule budget " .. tostring(rule.budget_ms) .. "ms"
    end
    return decision
  end

  if opts.fail_closed then
    decision.action = "deny"
  else
    decision.action = "allow"
    decision.reason = "no rule matched and fail-open mode is enabled"
  end
  return decision
end

local function load_events(path)
  local events = {}
  for line_number, line in ipairs(read_lines(path)) do
    local clean = trim(line)
    if clean ~= "" then
      local ok, value = pcall(json_decode, clean)
      if ok and type(value) == "table" then
        value.__line = line_number
        events[#events + 1] = { line = line_number, event = value }
      else
        events[#events + 1] = {
          line = line_number,
          invalid = true,
          error = tostring(value),
          raw = clean,
          event = { subject = "invalid-json", tool = "parse", resource = "line:" .. tostring(line_number) },
        }
      end
    end
  end
  return events
end

local function emit_jsonl(rules, events, opts)
  for _, item in ipairs(events) do
    local decision
    if item.invalid then
      decision = {
        action = "deny",
        subject = "invalid-json",
        tool = "parse",
        resource = "line:" .. tostring(item.line),
        reason = item.error,
        matched_rules = {},
        warnings = { "event line could not be parsed" },
      }
    else
      decision = evaluate_event(rules, item.event, opts)
    end
    decision.line = item.line
    print(json_encode(decision))
  end
end

local function emit_markdown(rules, events, opts)
  local counts = { allow = 0, deny = 0, warnings = 0 }
  local decisions = {}
  for _, item in ipairs(events) do
    local decision
    if item.invalid then
      decision = {
        action = "deny",
        subject = "invalid-json",
        tool = "parse",
        resource = "line:" .. tostring(item.line),
        reason = item.error,
        matched_rules = {},
        warnings = { "event line could not be parsed" },
        line = item.line,
      }
    else
      decision = evaluate_event(rules, item.event, opts)
      decision.line = item.line
    end
    decisions[#decisions + 1] = decision
    counts[decision.action] = counts[decision.action] + 1
    if #decision.warnings > 0 then
      counts.warnings = counts.warnings + 1
    end
  end

  print("# MCP Tool Policy Report")
  print("")
  print("- Policy rules: " .. tostring(#rules))
  print("- Events checked: " .. tostring(#events))
  print("- Allowed: " .. tostring(counts.allow))
  print("- Denied: " .. tostring(counts.deny))
  print("- Events with warnings: " .. tostring(counts.warnings))
  print("")
  print("| line | action | subject | tool | resource | reason |")
  print("| ---: | --- | --- | --- | --- | --- |")
  for _, decision in ipairs(decisions) do
    local reason = decision.reason:gsub("|", "\\|")
    print("| " .. tostring(decision.line) .. " | " .. decision.action .. " | " .. decision.subject .. " | " .. decision.tool .. " | " .. decision.resource .. " | " .. reason .. " |")
  end
end

local function lua_quote(value)
  return string.format("%q", tostring(value or ""))
end

local function emit_lua_policy(rules, opts)
  print("-- generated by McpToolPolicyCompiler.lua " .. VERSION)
  print("return {")
  print("  version = " .. lua_quote(VERSION) .. ",")
  print("  fail_closed = " .. tostring(opts.fail_closed) .. ",")
  print("  rules = {")
  for _, rule in ipairs(rules) do
    print("    {")
    print("      id = " .. lua_quote(rule.id) .. ", effect = " .. lua_quote(rule.effect) .. ",")
    print("      subject = " .. lua_quote(rule.subject) .. ", tool = " .. lua_quote(rule.tool) .. ", resource = " .. lua_quote(rule.resource) .. ",")
    if rule.budget_ms then
      print("      budget_ms = " .. tostring(rule.budget_ms) .. ",")
    end
    print("      reason = " .. lua_quote(rule.reason) .. ",")
    print("      constraints = {")
    for _, constraint in ipairs(rule.constraints) do
      print("        { field = " .. lua_quote(constraint.field) .. ", op = " .. lua_quote(constraint.op) .. ", expected = " .. lua_quote(constraint.expected) .. " },")
    end
    print("      },")
    print("    },")
  end
  print("  },")
  print("}")
end

local function usage()
  return [[
McpToolPolicyCompiler.lua validates MCP and agent tool calls against a compact edge policy.

Policy line format:
  effect|subject|tool|resource|constraints|budget_ms|reason

Examples:
  allow|agent:ci|github.*|repo:kspavankrishna/**|risk<=medium;cost_usd<=0.25|30000|approved CI automation
  deny|*|shell.exec|*|args.command~*rm -rf*||dangerous shell command

Commands:
  lua McpToolPolicyCompiler.lua --policy policy.txt --events calls.jsonl --format jsonl
  lua McpToolPolicyCompiler.lua --policy policy.txt --events calls.jsonl --format markdown
  lua McpToolPolicyCompiler.lua --policy policy.txt --format lua
  lua McpToolPolicyCompiler.lua --self-test

Flags:
  --policy PATH       policy file, required except for --self-test
  --events PATH       JSONL event file, defaults to stdin for jsonl and markdown
  --format NAME       jsonl, markdown, or lua; default jsonl
  --fail-open         allow unmatched events; default is fail-closed
  --fail-closed       deny unmatched events
]]
end

local function parse_args(argv)
  local opts = { format = "jsonl", fail_closed = true, self_test = false }
  local i = 1
  while i <= #argv do
    local a = argv[i]
    if a == "--policy" then
      i = i + 1
      opts.policy = argv[i]
    elseif a == "--events" then
      i = i + 1
      opts.events = argv[i]
    elseif a == "--format" then
      i = i + 1
      opts.format = argv[i]
    elseif a == "--fail-open" then
      opts.fail_closed = false
    elseif a == "--fail-closed" then
      opts.fail_closed = true
    elseif a == "--self-test" then
      opts.self_test = true
    elseif a == "--help" or a == "-h" then
      io.write(usage())
      os.exit(0)
    else
      die("unknown option " .. tostring(a) .. "\n" .. usage(), 2)
    end
    i = i + 1
  end
  if opts.format ~= "jsonl" and opts.format ~= "markdown" and opts.format ~= "lua" then
    die("unknown format " .. tostring(opts.format), 2)
  end
  return opts
end

local function assert_true(name, value)
  if not value then
    error("self-test failed: " .. name)
  end
end

local function self_test()
  local decoded = json_decode('{"subject":"agent:ci","tool":"github.create","resource":"repo:kspavankrishna/VIBE-CODE","risk":"low","args":{"path":"src/a.lua"},"cost_usd":0.03}')
  assert_true("json subject", decoded.subject == "agent:ci")
  assert_true("json nested", path_get(decoded, "args.path") == "src/a.lua")
  assert_true("glob star", glob_match("github.*", "github.create"))
  assert_true("glob double star", glob_match("repo:**/VIBE-CODE", "repo:kspavankrishna/VIBE-CODE"))
  local rule = parse_policy_line("allow|agent:*|github.*|repo:**|risk<=medium;cost_usd<=0.25|100|ok", 7)
  assert_true("rule match", rule_matches(rule, decoded))
  local decision = evaluate_event({ rule }, decoded, { fail_closed = true })
  assert_true("decision allow", decision.action == "allow")
  local deny = parse_policy_line("deny|*|github.*|*|args.path~**/.env||secret", 8)
  local secret = json_decode('{"subject":"agent:ci","tool":"github.create","resource":"repo:x","args":{"path":"prod/.env"}}')
  local blocked = evaluate_event({ deny, rule }, secret, { fail_closed = true })
  assert_true("deny wins", blocked.action == "deny")
  print("self-test ok")
end

local function main(argv)
  local opts = parse_args(argv)
  if opts.self_test then
    self_test()
    return
  end
  if not opts.policy then
    die("--policy is required\n" .. usage(), 2)
  end
  local rules = load_policy(opts.policy)
  if opts.format == "lua" then
    emit_lua_policy(rules, opts)
    return
  end
  local events = load_events(opts.events or "-")
  if opts.format == "markdown" then
    emit_markdown(rules, events, opts)
  else
    emit_jsonl(rules, events, opts)
  end
end

if arg then
  main(arg)
end

--[[
This solves the messy April 2026 problem where teams are adding MCP servers, agent tool calls, GitHub automation, shell helpers, database utilities, and browser workers faster than their security review process can keep up. Built because I wanted a small Lua file that can sit near OpenResty, Kong, CI logs, or an agent runner and make the same allow or deny decision every time from a plain text policy. Use it when a developer needs MCP tool policy enforcement, agent tool audit logs, AI gateway guardrails, JSONL tool call review, edge compute authorization, or DevOps approval checks without pulling in a large service. The trick: the policy language stays boring on purpose, but the matcher handles glob resources, nested JSON fields, severity comparisons, numeric spend limits, deny precedence, fail closed behavior, Markdown reports, JSONL decisions, and compiled Lua output for gateways. Drop this into a repository that needs searchable GitHub code for MCP security policy compiler, agent tool firewall, AI infrastructure guardrails, OpenResty policy engine, Lua DevOps automation, and production-ready tool governance, then wire it before expensive or dangerous tool calls so humans can review the rules instead of reverse engineering another agent trace.
]]
