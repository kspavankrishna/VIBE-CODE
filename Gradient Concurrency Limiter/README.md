# Gradient Concurrency Limiter

Static concurrency limits on an LLM inference gateway are wrong within a week. This is a dependency free Lua module for OpenResty that replaces the hardcoded `limit_conn` number with a latency driven feedback controller, adjusting the concurrency ceiling itself on every request.

**Language:** Lua | **Lines:** 546 | **Added:** 2026-09-10

## What this solves

This solves the "what number do I put in limit_conn" problem for AI inference gateways. Every team running an LLM or embedding backend behind nginx, Kong or APISIX eventually hardcodes a max concurrent requests number for that upstream, and that number is wrong within a week. GPU batch latency swings hard with prompt length, KV cache pressure, a model hot swapped to a bigger checkpoint or autoscaling adding a node. A static limit that is safe at 2am is either leaving GPU idle at peak or is already too high and causing cascading timeouts.

The failure mode repeats. Someone sets `limit_conn` to whatever worked in staging. Traffic shifts, latency creeps up, requests queue behind each other, queuing makes latency worse, and the backend falls into a spiral a fixed number can never see coming. By the time the error rate dashboard spikes the backend has been saturated for minutes. Who notices first: users sitting on requests that will time out, and the engineer paged for 504s that started life as a 200ms latency drift.

The other half of the problem is the overflow. Most gateway limiters queue. On an inference backend that protects nothing, because an admitted request often occupies a GPU side KV cache slot the moment it lands on serving stacks like vLLM or TGI. Queuing just delays the overload. This module rejects immediately with a jittered `Retry-After`, keeping the backend's working set bounded.

## Why I built it

The fix that works in production load balancers already exists. Netflix's concurrency-limits library and Envoy's adaptive concurrency filter both run a gradient, TCP Vegas style feedback loop: track the best latency you have ever seen as an idle baseline, compare it to what you are seeing now, and scale how many requests you let through by that ratio.

Nobody had ported that algorithm into a plain Lua module aimed at LLM inference gateways. This is that port, tuned for the failure modes there: wider windows, because inference latency is dominated by batch composition and decode length rather than network RTT, and fail fast rejection instead of queuing. No LuaRocks packages, no cjson, no Redis.

## When to use it

- You run an LLM or embedding backend behind OpenResty, Kong, APISIX or bare nginx with Lua and you hardcode a per upstream concurrency number.
- You are not limiting concurrency at all and a traffic spike occasionally takes the backend down.
- Your backend's latency floor moves on you: model hot swaps, quantization changes, autoscaling adding or removing GPU nodes.
- You want the gateway to back off before the error rate dashboard reacts.
- Several models share one gateway and a slow 70B chat model keeps starving a fast classifier endpoint.
- You want to verify the control loop on your laptop before pointing it at a real GPU backend.

## How it works

The public surface is four calls. `_M.new(dict, name, opts)` attaches to an `ngx.shared.DICT` under the prefix `gcl:<name>` and seeds state with `dict:add`, a no op when the key exists, so re running `new` after a config reload never resets a warmed up limit to the cold start default. `Limiter:acquire()` returns a token or a rejection, `Limiter:release(token, dropped)` returns the slot and feeds the controller a latency sample, and `Limiter:stats()` gives a snapshot for Prometheus or statsd.

`acquire` reads the current `limit`, then does an atomic `dict:incr` on `in_flight`. If the result exceeds the limit it decrements back, increments `drops` and returns `nil, "limit_exceeded", retry_after`. That retry value comes from the current EWMA latency with a 50ms floor, multiplied by a random factor between 1.5x and 2.0x. The jitter matters: without it every rejected client retries on the same beat and you get a synchronized herd on the next window.

The control loop in `Limiter:_update` is a Gradient2 style algorithm. `ewma_rtt` is an exponentially weighted moving average of round trip time with `rtt_alpha` 0.15. `min_rtt` is the best latency ever measured, standing in for "backend idle, no queuing". The gradient is `clamp(min_rtt / ewma_rtt, min_gradient, 1.0)`, floored at 0.5 so the limit cannot collapse in one step, and multiplied by `drop_penalty` 0.85 when the caller flags a request as dropped. The target is `limit * gradient + sqrt(limit)`. That square root is Little's law shaped headroom: it admits a small burst above the steady state estimate instead of clamping traffic to a razor's edge, and the allowance shrinks proportionally as the limit grows. The target is blended into the existing limit with `smoothing` 0.2 and clamped between `min_limit` 1 and `max_limit` 512.

The `min_rtt` ratchet keeps this honest over long uptimes. Any new lower reading is taken immediately. Left alone, one lucky low sample would pin the baseline forever and hold the controller permanently too conservative, and a real floor rise from a model swap would never register. So every `probe_interval` seconds, 30 by default, `min_rtt` is nudged up by `min_rtt_growth` 1.05, forcing the gradient to re justify the current limit against fresh evidence. The nudge lands on the next sample after the interval elapses, not on a timer.

Cross worker state is deliberately flat. Each nginx worker has its own Lua VM with no shared heap, so the shm dict is the only thing every worker sees and it stores scalars only, one key per field rather than a serialized blob. `in_flight` and `drops` use `dict:incr`, a genuine atomic shm operation in nginx. The float fields `limit`, `min_rtt` and `ewma_rtt` accept last write wins races on purpose: this is a slow loop reacting to seconds scale trends, and a cross worker lock on the hot path would cost more than an occasional lost update.

An `ngx` shim and a `VirtualClock` mean that when `ngx.config` is absent the file fakes enough of `ngx.shared.DICT` to run under plain `lua` or `luajit`. The self test drives three phases through `run_phase`: healthy at 6 requests per tick, saturated at 150, recovered at 8, against a `backend_rtt` model where latency grows with how many requests got admitted. It asserts the limit shrank under saturation, recovered afterwards and that requests were shed rather than queued.

## Usage

```lua
-- nginx.conf
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
```

Overriding defaults and reading metrics:

```lua
local limiter = GCL.new(ngx.shared.gradient_limiter, "model:embed-small", {
  initial_limit  = 32,
  max_limit      = 256,
  probe_interval = 10.0,
  smoothing      = 0.3,
})

local s = limiter:stats()
-- s.limit, s.in_flight, s.min_rtt, s.ewma_rtt, s.drops, s.total
```

Run the built in three phase simulation with no nginx installed:

```sh
lua GradientConcurrencyLimiter.lua
# or
luajit GradientConcurrencyLimiter.lua
```

## Notes

- Give each backend or model its own limiter name. A fast embedding endpoint sharing a limiter with a slow chat model keeps resetting `min_rtt` out from under it, and the slow model ends up throttled to nothing.
- `release` must be called for every admitted token. If the log phase is skipped, `in_flight` leaks upward and the limiter slowly closes. No timeout reaper here.
- Classifying a request as `dropped` is the caller's job. The wiring above treats 5xx and a missing `upstream_status` as dropped. A slow but successful response is not a drop, it feeds the latency signal.
- `token.limit_snapshot` is recorded at admission but not consumed by `release` or `_update`. It exists for callers that want to log the ceiling a request was admitted under.
- The shm dict must be declared and sized in `nginx.conf`, stores scalars only, and is never evicted or expired by this module.
- The self test runs only when the file is executed directly and `arg[0]` contains the filename. It exits 0 when all three assertions pass and 1 otherwise, so it drops into CI as is.
- This controls concurrency, not request rate and not token throughput. No per tenant fairness, no priority classes, no idea how many tokens a prompt will generate.
