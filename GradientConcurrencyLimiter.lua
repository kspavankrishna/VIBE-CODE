--[[
GradientConcurrencyLimiter.lua

Adaptive, gradient-based concurrency limiting for AI inference gateways
(OpenResty / Kong / APISIX / plain lua-nginx-module).

See the explanation block at the end of this file for the full writeup.
]]

local _M = { _VERSION = "1.0.0" }
_M.__index = _M

-- ---------------------------------------------------------------------
-- ngx shim: this file runs unmodified either inside OpenResty (where
-- `ngx` is the real API) or under a plain `lua`/`luajit` interpreter for
-- local testing and CI, where we fake just enough of `ngx` to exercise
-- the algorithm deterministically. Detecting "are we inside OpenResty"
-- by checking `ngx.config` is the standard idiom because `ngx` itself
-- may already be a global left behind by an unrelated harness.
-- ---------------------------------------------------------------------
local ngx = _G.ngx
local RUNNING_STANDALONE = not (ngx and ngx.config)

if RUNNING_STANDALONE then
  local fake_shared_store = {}

  local function new_fake_dict()
    local store = {}
    local dict = {}

    function dict:get(key)
      return store[key]
    end

    function dict:set(key, value)
      store[key] = value
      return true
    end

    function dict:add(key, value)
      if store[key] ~= nil then
        return false, "exists"
      end
      store[key] = value
      return true
    end

    -- incr(key, delta, init): atomic in real ngx.shared.DICT because
    -- nginx workers serialize through the shm segment's internal lock;
    -- our fake version is single-threaded Lua so a plain add is fine.
    function dict:incr(key, delta, init)
      local cur = store[key]
      if cur == nil then
        cur = init or 0
      end
      cur = cur + delta
      store[key] = cur
      return cur
    end

    return dict
  end

  ngx = {
    config = nil, -- stays nil/false so RUNNING_STANDALONE stays accurate
    shared = setmetatable({}, {
      __index = function(t, name)
        local d = fake_shared_store[name]
        if not d then
          d = new_fake_dict()
          fake_shared_store[name] = d
        end
        return d
      end,
    }),
    now = function()
      return os.clock() + (os.time() - os.time()) -- placeholder, overridden below
    end,
  }

  -- Standalone clock: os.clock() only has ~10ms resolution on some
  -- platforms and doesn't advance during pure computation reliably
  -- across all Lua builds, so the self-test drives a virtual clock
  -- explicitly instead of relying on wall time. See VirtualClock below.
  _G.ngx = ngx
end

-- ---------------------------------------------------------------------
-- Virtual/real clock abstraction. In production this is ngx.now()
-- (cached per-request timer, cheap). In the standalone self-test we
-- advance a virtual clock ourselves so a 10,000-request simulation
-- doesn't take 10,000 real seconds.
-- ---------------------------------------------------------------------
local VirtualClock = { t = 0 }
function VirtualClock.now()
  if not RUNNING_STANDALONE then
    return ngx.now()
  end
  return VirtualClock.t
end
function VirtualClock.advance(dt)
  VirtualClock.t = VirtualClock.t + dt
end

local function clamp(v, lo, hi)
  if v < lo then return lo end
  if v > hi then return hi end
  return v
end

-- ---------------------------------------------------------------------
-- Defaults. Tuned for LLM/inference backends: latency is dominated by
-- batch composition and KV-cache pressure rather than network RTT, so
-- windows are wider (seconds, not milliseconds) than a typical L7 LB.
-- ---------------------------------------------------------------------
local DEFAULTS = {
  initial_limit   = 8,     -- starting concurrency ceiling per limiter key
  min_limit       = 1,     -- never throttle to zero; one slot always admitted
  max_limit       = 512,   -- hard safety ceiling regardless of gradient math
  smoothing       = 0.2,   -- EWMA weight applied to each new limit estimate
  rtt_alpha       = 0.15,  -- EWMA weight for the short-term latency estimate
  min_rtt_growth  = 1.05,  -- periodic upward nudge so a stale low doesn't stick
  probe_interval  = 30.0,  -- seconds between forced min_rtt re-measurement
  drop_penalty    = 0.85,  -- multiplicative hit to the gradient on a dropped call
  min_gradient    = 0.5,   -- floor on the gradient so limit never collapses in one step
}

-- ---------------------------------------------------------------------
-- Shared-dict field accessors.
--
-- ngx.shared.DICT can only store scalars, not tables, and each nginx
-- worker process runs its own Lua VM with no shared heap -- the shm
-- dict is the only thing every worker actually sees. We therefore keep
-- one flat key per field instead of a serialized blob, and we accept
-- last-write-wins races on the float fields (limit, min_rtt, ewma_rtt):
-- this is a slow feedback-control loop reacting to seconds-scale
-- trends, not a correctness-critical counter, so an occasional lost
-- update from a concurrent writer is invisible in the output and not
-- worth paying a cross-worker lock on the hot request path for.
-- The two integer fields that DO need to be exact -- in_flight and
-- drops -- use dict:incr, which nginx implements as a genuine atomic
-- shm operation.
-- ---------------------------------------------------------------------
local function key(prefix, field)
  return prefix .. ":" .. field
end

local function get_num(dict, k, default)
  local v = dict:get(k)
  if v == nil then
    return default
  end
  return v
end

local Limiter = setmetatable({}, { __index = _M })
Limiter.__index = Limiter

--- Construct (or re-attach to) a limiter identified by `name`.
-- Multiple nginx workers calling `new` with the same `dict` and `name`
-- share state automatically because they're reading/writing the same
-- shm segment; no explicit registration step is needed.
-- @param dict   an ngx.shared.DICT (e.g. ngx.shared.gradient_limiter)
-- @param name   string key namespace, e.g. "model:gpt-large" -- give
--               each backend with a distinct latency profile its own
--               name so a slow embedding model doesn't starve a fast
--               classifier model sharing the same gateway.
-- @param opts   optional overrides for any DEFAULTS field.
function _M.new(dict, name, opts)
  assert(dict, "GradientConcurrencyLimiter.new requires an ngx.shared.DICT")
  assert(type(name) == "string" and #name > 0, "GradientConcurrencyLimiter.new requires a name")

  local self = setmetatable({}, Limiter)
  self.dict = dict
  self.prefix = "gcl:" .. name
  self.opts = {}
  for k, v in pairs(DEFAULTS) do
    self.opts[k] = (opts and opts[k] ~= nil) and opts[k] or v
  end

  -- Seed initial state only if nothing is there yet (dict:add is a
  -- no-op if the key already exists), so re-`new`-ing an existing
  -- limiter name -- e.g. after a config reload -- never resets a
  -- warmed-up limit back to the cold-start default.
  dict:add(key(self.prefix, "limit"), self.opts.initial_limit)
  dict:add(key(self.prefix, "min_rtt"), math.huge)
  dict:add(key(self.prefix, "ewma_rtt"), 0)
  dict:add(key(self.prefix, "in_flight"), 0)
  dict:add(key(self.prefix, "drops"), 0)
  dict:add(key(self.prefix, "total"), 0)
  dict:add(key(self.prefix, "last_probe"), VirtualClock.now())

  return self
end

--- Try to admit one request. Call this in access_by_lua_block (or
-- equivalent) before proxying to the inference backend.
-- @return token, err, retry_after
--   token is non-nil on admission; pass it to :release() later.
--   on rejection, token is nil, err is "limit_exceeded", and
--   retry_after is a jittered second count safe to hand back in a
--   Retry-After header to spread out synchronized client retries.
function Limiter:acquire()
  local p = self.prefix
  local limit = get_num(self.dict, key(p, "limit"), self.opts.initial_limit)
  local in_flight = self.dict:incr(key(p, "in_flight"), 1, 0)

  if in_flight > limit then
    self.dict:incr(key(p, "in_flight"), -1, 0)
    self.dict:incr(key(p, "drops"), 1, 0)

    -- Fail fast rather than queue: an admitted-but-queued inference
    -- request still occupies a GPU-side KV cache slot on many serving
    -- stacks (vLLM, TGI) the moment it reaches the backend, so queuing
    -- at the gateway just delays the overload instead of preventing
    -- it. Rejecting immediately protects the backend and gives the
    -- client a clean, fast signal to retry elsewhere or back off.
    local ewma_rtt = get_num(self.dict, key(p, "ewma_rtt"), 0.5)
    local base = math.max(ewma_rtt, 0.05)
    local jitter = base * (0.5 + math.random() * 0.5)
    return nil, "limit_exceeded", math.floor((base + jitter) * 100 + 0.5) / 100
  end

  return { start = VirtualClock.now(), limit_snapshot = limit }, nil, nil
end

--- Report the outcome of a previously admitted request. Call this in
-- log_by_lua_block (or a pcall-wrapped finally block) so a request
-- that errors out still releases its slot and still feeds the
-- controller a real latency/drop sample -- skipping failed requests
-- would make the limiter blind to exactly the condition it exists to
-- detect.
-- @param token    the token returned by :acquire()
-- @param dropped  true if the backend call failed, timed out, or was
--                 cancelled (as opposed to completing with a normal
--                 status code, even a slow one)
function Limiter:release(token, dropped)
  if not token then return end
  local p = self.prefix
  self.dict:incr(key(p, "in_flight"), -1, 0)

  local rtt = math.max(VirtualClock.now() - token.start, 1e-6)
  self:_update(rtt, dropped and true or false)
end

-- Core control loop: the Gradient2-style algorithm used by Netflix's
-- concurrency-limits and Envoy's adaptive concurrency filter, adapted
-- for LLM inference where "RTT" is dominated by batch scheduling and
-- decode length rather than network hops.
--
--   gradient   = clamp(min_rtt / ewma_rtt, min_gradient, 1.0)
--   new_limit  = old_limit * gradient + sqrt(old_limit)
--
-- The sqrt(old_limit) term is Little's-law headroom: it lets the
-- limiter admit a small burst above the steady-state estimate instead
-- of clamping traffic to a razor's edge, at the cost of a
-- proportionally smaller allowance as the limit itself grows.
function Limiter:_update(rtt, dropped)
  local p = self.prefix
  local o = self.opts

  self.dict:incr(key(p, "total"), 1, 0)

  local min_rtt = get_num(self.dict, key(p, "min_rtt"), math.huge)
  local ewma_rtt = get_num(self.dict, key(p, "ewma_rtt"), rtt)
  local limit = get_num(self.dict, key(p, "limit"), o.initial_limit)
  local last_probe = get_num(self.dict, key(p, "last_probe"), VirtualClock.now())

  -- Track the short-term latency estimate.
  ewma_rtt = ewma_rtt == 0 and rtt or (ewma_rtt * (1 - o.rtt_alpha) + rtt * o.rtt_alpha)

  -- min_rtt tracks the best latency we've ever measured, which is our
  -- proxy for "backend fully idle, no queuing". Left alone forever
  -- this would ratchet down and never recover if the true floor rises
  -- (e.g. a model swap to a larger checkpoint) or never rise back up
  -- after a one-off outlier drags it artificially low. So: take any
  -- new lower reading immediately, but every probe_interval seconds,
  -- also nudge it upward slightly to force the gradient to
  -- periodically re-justify the current limit against fresh evidence
  -- rather than an increasingly stale historical best.
  local now = VirtualClock.now()
  if rtt < min_rtt then
    min_rtt = rtt
  end
  if now - last_probe > o.probe_interval then
    min_rtt = min_rtt * o.min_rtt_growth
    last_probe = now
  end
  if min_rtt == math.huge then
    min_rtt = rtt
  end

  local gradient = clamp(min_rtt / math.max(ewma_rtt, 1e-6), o.min_gradient, 1.0)
  if dropped then
    gradient = gradient * o.drop_penalty
  end

  local headroom = math.sqrt(math.max(limit, 1))
  local target = limit * gradient + headroom
  local new_limit = limit * (1 - o.smoothing) + target * o.smoothing
  new_limit = clamp(new_limit, o.min_limit, o.max_limit)

  self.dict:set(key(p, "min_rtt"), min_rtt)
  self.dict:set(key(p, "ewma_rtt"), ewma_rtt)
  self.dict:set(key(p, "limit"), new_limit)
  self.dict:set(key(p, "last_probe"), last_probe)
end

--- Snapshot for metrics export (Prometheus, statsd, logs).
function Limiter:stats()
  local p = self.prefix
  return {
    limit     = get_num(self.dict, key(p, "limit"), self.opts.initial_limit),
    in_flight = get_num(self.dict, key(p, "in_flight"), 0),
    min_rtt   = get_num(self.dict, key(p, "min_rtt"), 0),
    ewma_rtt  = get_num(self.dict, key(p, "ewma_rtt"), 0),
    drops     = get_num(self.dict, key(p, "drops"), 0),
    total     = get_num(self.dict, key(p, "total"), 0),
  }
end

_M.Limiter = Limiter

--[[
Example OpenResty wiring (nginx.conf):

  http {
    lua_shared_dict gradient_limiter 1m;

    server {
      location /v1/chat/completions {
        access_by_lua_block {
          local GCL = require "GradientConcurrencyLimiter"
          local limiter = GCL.new(ngx.shared.gradient_limiter, "model:gpt-large")
          local token, err, retry_after = limiter:acquire()
          if not token then
            ngx.header["Retry-After"] = tostring(retry_after)
            ngx.status = 429
            ngx.say('{"error":"backend_saturated"}')
            return ngx.exit(429)
          end
          ngx.ctx.gcl_token = token
          ngx.ctx.gcl_limiter = limiter
        }
        proxy_pass http://inference_backend;
        log_by_lua_block {
          local limiter = ngx.ctx.gcl_limiter
          if limiter then
            local dropped = (ngx.status >= 500) or (tonumber(ngx.var.upstream_status) == nil)
            limiter:release(ngx.ctx.gcl_token, dropped)
          end
        }
      }
    }
  }

Give each distinct backend/model its own limiter name -- a slow
70B-parameter model and a fast embedding endpoint have wildly
different latency floors, and sharing one limiter between them would
have the fast endpoint's traffic constantly reset min_rtt out from
under the slow one.
]]

-- ===================================================================
-- Self-test / demo. Runs only when this file is executed directly
-- (via `lua GradientConcurrencyLimiter.lua` or `luajit ...`), using
-- the standalone ngx shim and virtual clock defined above. This is
-- not a toy: it simulates three realistic backend regimes back to
-- back and asserts the controller actually adapts to each, so cloning
-- this file and running it once is enough to see the algorithm work
-- without standing up nginx.
-- ===================================================================
if RUNNING_STANDALONE and arg and arg[0] and arg[0]:find("GradientConcurrencyLimiter") then
  math.randomseed(42)

  local shared = ngx.shared.gradient_limiter_test
  local limiter = _M.new(shared, "demo-model", { probe_interval = 5.0 })

  -- Simulates a backend whose latency grows with how many requests are
  -- currently admitted (i.e. real queuing behavior under load), plus a
  -- little noise, with three phases: healthy, saturated, recovered.
  local function backend_rtt(admitted, phase)
    local base = 0.05 -- 50ms floor: one forward pass, warm cache
    local noise = (math.random() - 0.5) * 0.01
    if phase == "healthy" then
      return base + admitted * 0.01 + noise
    elseif phase == "saturated" then
      -- GPU thrashing / batch queue blowup: a flat overload tax plus a
      -- per-request cost that grows with how many concurrent requests
      -- actually got admitted (more admitted -> worse batching/KV
      -- cache pressure -> everyone in that batch pays more).
      return base + 0.3 + admitted * 0.006 + noise
    else -- "recovered"
      return base + admitted * 0.008 + noise
    end
  end

  -- Each tick simulates one batch of concurrent requests arriving
  -- together (as a real traffic surge would): acquire all of them
  -- first -- so in_flight genuinely builds up and the limiter can
  -- actually shed the overflow -- then resolve the whole batch against
  -- one shared latency figure driven by how many were admitted, and
  -- only then release them. Sequentially acquiring-and-immediately-
  -- releasing one request at a time (the earlier version of this
  -- harness) never let in_flight exceed 1, so the controller never saw
  -- real concurrency pressure and never had anything to shed.
  local function run_phase(name, seconds, requests_per_tick)
    io.write(string.format("\n-- phase: %s --\n", name))
    local ticks = math.floor(seconds)
    for tick = 1, ticks do
      local tokens = {}
      for _ = 1, requests_per_tick do
        local token = limiter:acquire()
        if token then
          tokens[#tokens + 1] = token
        end
      end

      local rtt = backend_rtt(#tokens, name)
      local dropped = rtt > 2.0 -- treat pathological stalls as failures
      VirtualClock.advance(rtt)
      for _, token in ipairs(tokens) do
        limiter:release(token, dropped)
      end

      if tick % 5 == 0 or tick == ticks then
        local s = limiter:stats()
        io.write(string.format(
          "  t=%3ds  limit=%6.2f  in_flight=%d  ewma_rtt=%.3fs  min_rtt=%.3fs  drops=%d  total=%d\n",
          math.floor(VirtualClock.now()), s.limit, s.in_flight, s.ewma_rtt, s.min_rtt, s.drops, s.total))
      end
      VirtualClock.advance(0.2)
    end
    return limiter:stats()
  end

  local s1 = run_phase("healthy", 20, 6)
  local s2 = run_phase("saturated", 25, 150)
  local s3 = run_phase("recovered", 40, 8)

  io.write("\n-- assertions --\n")
  local ok = true

  if not (s2.limit < s1.limit) then
    ok = false
    io.write("FAIL: expected limit to shrink under saturation\n")
  else
    io.write(string.format("PASS: limit shrank under saturation (%.2f -> %.2f)\n", s1.limit, s2.limit))
  end

  if not (s3.limit > s2.limit) then
    ok = false
    io.write("FAIL: expected limit to recover once the backend healed\n")
  else
    io.write(string.format("PASS: limit recovered after healing (%.2f -> %.2f)\n", s2.limit, s3.limit))
  end

  if s2.drops <= 0 then
    ok = false
    io.write("FAIL: expected at least one shed request during saturation\n")
  else
    io.write(string.format("PASS: shed %d requests during saturation instead of queuing them\n", s2.drops))
  end

  io.write(ok and "\nALL CHECKS PASSED\n" or "\nSOME CHECKS FAILED\n")
  os.exit(ok and 0 or 1)
end

return _M

--[[
=======================================================================
WHAT THIS IS AND WHY IT EXISTS (read this before you use it)
=======================================================================

This solves the "what number do I put in limit_conn" problem for AI
inference gateways. Every team running an LLM or embedding backend
behind nginx, Kong, or APISIX eventually hardcodes a max-concurrent-
requests number for that upstream, and that number is wrong within a
week: GPU batch latency swings hard depending on prompt length, KV
cache pressure, whether the model just got hot-swapped to a bigger
checkpoint, or whether autoscaling just added a node. A static limit
that's safe at 2am is either leaving GPU idle at peak or is already
too high and causing cascading timeouts. This file replaces that
static number with a controller that watches actual round-trip
latency and adjusts the concurrency ceiling itself, request by
request, with zero manual tuning.

Built because I kept seeing the same failure mode on inference
gateways: someone sets limit_conn to whatever seemed to work in
staging, traffic patterns shift, latency creeps up, requests start
queuing behind each other, queuing makes latency worse, and the whole
backend falls into a latency spiral that a fixed number can never see
coming or back off from. The fix that actually works in production
load balancers (Netflix's concurrency-limits library, Envoy's adaptive
concurrency filter) is a gradient/TCP-Vegas-style feedback loop: track
the best latency you've ever seen as your "idle" baseline, compare it
to what you're seeing right now, and shrink or grow how many requests
you let through based on that ratio. Nobody had ported that specific
algorithm into a plain, dependency-free Lua module aimed at LLM
inference gateways, so this is that port, tuned for the failure modes
that show up there specifically -- fail-fast rejection instead of
queuing, because a queued inference request is often still holding a
GPU-side KV cache slot the instant it lands on the backend, so queuing
at the gateway doesn't actually protect anything.

Use it when you're running any AI/LLM/embedding backend behind an
OpenResty-based gateway (raw lua-nginx-module, Kong, APISIX, or
straight nginx-with-Lua) and you're currently either hardcoding a
concurrency limit per upstream or not limiting concurrency at all and
occasionally taking the whole backend down with a traffic spike. Drop
one limiter per distinct backend/model -- a 70B chat model and a small
embedding endpoint have completely different latency floors and
should never share one limiter instance, or the fast endpoint's
traffic will keep resetting the slow endpoint's learned baseline.

The trick: it doesn't try to predict load, it just measures the ratio
between the best latency you've ever recorded (a stand-in for "the
backend is idle and this is as fast as it gets") and your current
smoothed latency, and lets that ratio directly scale the concurrency
ceiling up or down every single request. When the backend is healthy
the ratio sits near 1.0 and the limit slowly climbs, feeling out extra
headroom. When the backend starts queuing, latency rises, the ratio
drops, and the limit shrinks immediately -- before you'd see a spike
in your error rate dashboard. A small periodic upward nudge to the
recorded "best latency" stops the controller from getting permanently
stuck too conservative because of one lucky low reading early on, and
a Little's-law-shaped headroom term (square root of the current limit)
lets it tolerate normal burstiness without either overreacting to
every jiggle or oscillating. All cross-worker state lives in a single
ngx.shared.DICT, which is the only thing nginx worker processes
actually share, so this works correctly across a multi-worker
deployment without an external Redis or coordination service.

Drop this into any OpenResty-based LLM gateway (Kong plugin, APISIX
plugin, or a bare access_by_lua_block / log_by_lua_block pair, wiring
shown above the self-test) as a `require("GradientConcurrencyLimiter")`
with zero external dependencies -- no LuaRocks packages, no cjson, no
Redis. It also runs standalone under plain `lua` or `luajit` with no
nginx installed at all (`lua GradientConcurrencyLimiter.lua`), which
runs a three-phase healthy/saturated/recovered simulation and asserts
the limit actually shrinks under load and recovers afterward, so you
can verify the algorithm behaves correctly on your machine before you
ever point it at a real GPU backend.
=======================================================================
]]
