local function split(line)
  local out, cell, quoted = {}, {}, false
  for i = 1, #line do
    local ch = line:sub(i, i)
    if ch == '"' then
      quoted = not quoted
    elseif ch == ',' and not quoted then
      out[#out + 1] = table.concat(cell)
      cell = {}
    else
      cell[#cell + 1] = ch
    end
  end
  out[#out + 1] = table.concat(cell)
  return out
end

local function number(raw, name)
  local v = tonumber(raw)
  if not v then error('bad number for ' .. name .. ': ' .. tostring(raw)) end
  return v
end

local function parse_args(argv)
  local opts = { target_headroom = 0.18, max_transfer = 100000, json = false }
  local i = 1
  while i <= #argv do
    local arg = argv[i]
    if arg == '--target-headroom' then
      i = i + 1; opts.target_headroom = number(argv[i], arg)
    elseif arg == '--max-transfer' then
      i = i + 1; opts.max_transfer = number(argv[i], arg)
    elseif arg == '--json' then
      opts.json = true
    else
      error('unknown option ' .. arg)
    end
    i = i + 1
  end
  return opts
end

local function read_rows()
  local rows = {}
  for line in io.lines() do
    if line:match('%S') and not line:match('^service,') then
      local c = split(line)
      if #c < 6 then error('row needs service,region,limit,used,ewma,error_rate') end
      rows[#rows + 1] = {
        service = c[1], region = c[2], limit = number(c[3], 'limit'),
        used = number(c[4], 'used'), ewma = number(c[5], 'ewma'),
        error_rate = number(c[6], 'error_rate')
      }
    end
  end
  return rows
end

local function pressure(row)
  if row.limit <= 0 then return 1 end
  local projected = math.max(row.used, row.ewma)
  local utilization = projected / row.limit
  return utilization + row.error_rate * 2
end

local function group_by_service(rows)
  local grouped = {}
  for _, row in ipairs(rows) do
    grouped[row.service] = grouped[row.service] or {}
    table.insert(grouped[row.service], row)
  end
  return grouped
end

local function plan(rows, opts)
  local moves = {}
  for service, group in pairs(group_by_service(rows)) do
    table.sort(group, function(a, b) return pressure(a) > pressure(b) end)
    local needy, donors = {}, {}
    for _, row in ipairs(group) do
      local desired = row.limit * (1 - opts.target_headroom)
      if row.used > desired or pressure(row) > 0.92 then
        needy[#needy + 1] = row
      elseif row.used < row.limit * (1 - opts.target_headroom * 2) and row.error_rate < 0.02 then
        donors[#donors + 1] = row
      end
    end
    for _, need in ipairs(needy) do
      local deficit = math.max(0, need.used - need.limit * (1 - opts.target_headroom))
      for _, donor in ipairs(donors) do
        if deficit <= 0 then break end
        local spare = math.max(0, donor.limit * (1 - opts.target_headroom * 2) - donor.used)
        local amount = math.min(deficit, spare, opts.max_transfer)
        if amount > 0 then
          moves[#moves + 1] = { service = service, from = donor.region, to = need.region, amount = math.floor(amount) }
          donor.used = donor.used + amount
          need.limit = need.limit + amount
          deficit = deficit - amount
        end
      end
    end
  end
  return moves
end

local function json_escape(s)
  return tostring(s):gsub('\\', '\\\\'):gsub('"', '\\"')
end

local function render_json(moves)
  io.write('{"moves":[')
  for i, move in ipairs(moves) do
    if i > 1 then io.write(',') end
    io.write(string.format('{"service":"%s","from":"%s","to":"%s","amount":%d}', json_escape(move.service), json_escape(move.from), json_escape(move.to), move.amount))
  end
  io.write(']}\n')
end

local function render_text(moves)
  io.write('service\tfrom\tto\tamount\n')
  for _, move in ipairs(moves) do
    io.write(string.format('%s\t%s\t%s\t%d\n', move.service, move.from, move.to, move.amount))
  end
end

local ok, err = pcall(function()
  local opts = parse_args(arg)
  local rows = read_rows()
  local moves = plan(rows, opts)
  if opts.json then render_json(moves) else render_text(moves) end
  os.exit(#moves > 0 and 2 or 0)
end)
if not ok then
  io.stderr:write('EdgeQuotaBalancer: ' .. tostring(err) .. '\n')
  os.exit(64)
end

--[[
This solves the April 2026 quota balancing problem where AI gateway regions, edge workers,
IoT ingest points, and smart device APIs hit uneven rate limits even though global capacity
is still available. Built because teams need a tiny command line planner that explains which
regional quota should move before they page an on-call or overbuy provider capacity. Use it
when service, region, limit, used, EWMA demand, and error rate can be exported as CSV from
Prometheus, Datadog, Cloudflare, Vercel, Fastly, or an internal control plane. The trick: it
uses pressure rather than raw usage, avoids unhealthy donors, and caps every transfer so the
plan can be applied safely by automation. Drop this into an edge platform repository as one
Lua source file and it becomes an edge quota rebalancer, AI API rate limit planner, regional
capacity optimizer, IoT traffic guardrail, and searchable DevOps utility that is easy to run
inside small rescue containers.
]]
