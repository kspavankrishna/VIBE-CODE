# Agent Tool Retry Budget

An AI agent hits a rate limit or a dead MCP transport, retries the tool call and either duplicates a side effect that was never safe to repeat or burns the inference budget on an error that was never going to clear. This is a single file Elixir gate that reads tool call rows and decides, per row, whether the retry is allowed and how long to wait.

**Language:** Elixir | **Lines:** 130 | **Added:** 2026-05-24

## What this solves

This solves the April 2026 agent retry problem where AI tool calls hit rate limits, streaming timeouts, flaky MCP transports and overloaded internal APIs, then retry in ways that duplicate side effects or burn inference budget. The failure is rarely the first error. It is the retry policy behind it.

Two things go wrong in production. The first is duplication. An agent calls a tool that charges a card, files a ticket or sends a webhook, the transport drops after the write but before the response, and the framework retries because it saw a timeout. Timeouts look retryable to every generic retry library ever written. They are only retryable when the call itself is idempotent, and the library has no idea whether it is. The customer sees two charges. Support sees the duplicate before you do.

The second is budget burn. A provider returns `overloaded` or `rate_limited`, the agent retries, the provider is still overloaded, and every attempt carries real token cost and real wall clock. A long running loop with three retries per tool and four tools per step quietly multiplies spend by an order of magnitude. Nobody notices until the invoice, or until the request has blown its latency SLO. Worse, everyone retrying on the same interval synchronises into a retry storm that keeps the upstream pinned down.

The reason this is a file and not a config block is auditability. When someone asks why attempt 3 of a payment tool went out, you want a row with a reason string on it, not a stack trace through a retry decorator. That is what this produces: one output row per input row, with `retry`, `delay_ms` and a plain English `reason`.

## Why I built it

Built because agent frameworks often expose retry knobs but do not give platform teams a small, auditable budget gate for each tool call. You get `max_retries: 3` and a backoff helper. What you do not get is a place to say "this call is not idempotent so no retry, ever" or "we have already spent 28 of our 30 seconds so stop" or "show me the decision for every call in yesterday's logs". Those questions land on the platform team and the answer usually involves reading framework source.

The other gap is testing. Most backoff implementations call a random number generator, so the delay you get in CI is not the delay in the golden file and people either stub the RNG or stop asserting on delays. Deterministic jitter fixes that without giving up the desynchronisation jitter exists to provide.

## When to use it

- You have agent or MCP tool call logs and want to replay them through a retry policy before shipping the policy.
- A tool that writes something got retried on a timeout and you need a hard rule that blocks non-idempotent retries.
- You are sizing a budget and want to see how many calls get denied at `--max-attempts 2` versus `--max-attempts 5`.
- You want a CI gate that fails the build when a fixture of tool calls produces any denied retry.
- An upstream is returning `overloaded` and you need backoff delays you can put in a runbook.
- You want the dollar cost behind a batch of retry decisions before you approve the policy.

## How it works

One module, `AgentToolRetryBudget`, with a struct holding five knobs: `max_attempts` (3), `max_elapsed_ms` (30_000), `base_backoff_ms` (250), `jitter_ms` (75) and `retryable_codes`, a `MapSet` of `timeout`, `rate_limited`, `overloaded` and `transport`. The four numeric knobs are settable from the command line. The code set is not.

`parse_args/1` is a small state machine built out of one `Enum.reduce`. On a flag like `--max-attempts` it writes a sentinel atom, `:pending_max_attempts`, into that field, and the next clause pattern matches on the sentinel to consume the following token through `parse_int/1`, which accepts only whole non negative integers. Anything that is neither a known flag nor a pending value raises `ArgumentError`.

`parse_line/1` splits a trimmed line on commas into `tool,code,elapsed_ms,attempt,idempotent[,cost_usd]`. The idempotency flag is truthy for `true`, `1` or `yes` and false for everything else, which is the safe default: if you cannot tell the tool is idempotent, it is not. `parse_cost/1` runs `Float.parse` on whatever is left after the fifth field and falls back to `0.0` rather than failing the row. A `rescue` rewrites any parse failure into one message naming the expected column order.

`decision/2` is the gate and it is deliberately boring. Four booleans: the code is in `retryable_codes`, `attempt < max_attempts`, `elapsed_ms < max_elapsed_ms` and the row is idempotent. A retry is allowed only when all four hold, so idempotency is a veto that no amount of remaining budget can override. `reason/4` then runs a `cond` in fixed precedence order: not idempotent, not retryable, attempts exhausted, elapsed budget exhausted, allowed. You get the most fundamental reason, not whichever check ran first.

Delay comes from `backoff/2`: binary exponential backoff, `base_backoff_ms * 2^(attempt - 1)`, with the exponent floored at zero so attempt 0 and attempt 1 both get the base delay. The jitter is the interesting part. Instead of `:rand.uniform` it uses `rem(:erlang.phash2({attempt, jitter_ms}), jitter_ms + 1)`, a hash of the attempt number folded into the jitter window. That is deterministic jitter: the same attempt always gets the same offset, so tests and CI output stay reproducible, while different attempts still land on different points in the window. The result is clamped with `min/2` against `max_elapsed_ms`.

`run/2` wires it together. Lines are split with `trim: true`, any line starting with `tool,` is dropped so a CSV header passes through harmlessly, then each row is parsed and decided. `render/1` writes a tab separated table to stdout. `summarize/1` reduces the decisions into allow and deny counts plus a `cost_at_risk` total, printed to stderr so it never contaminates the table when you pipe it.

## Usage

```bash
# defaults: 3 attempts, 30s elapsed budget, 250ms base backoff, 75ms jitter window
cat tool_calls.csv | elixir AgentToolRetryBudget.ex

# tighter budget for a latency sensitive path
cat tool_calls.csv | elixir AgentToolRetryBudget.ex \
  --max-attempts 2 \
  --max-elapsed-ms 8000 \
  --base-backoff-ms 400 \
  --jitter-ms 120

# input format, header optional
# tool,code,elapsed_ms,attempt,idempotent[,cost_usd]
# --jitter-ms 0 collapses the jitter window so the delay is exactly the base backoff
printf 'tool,code,elapsed_ms,attempt,idempotent,cost_usd\n%s\n%s\n' \
  'search_docs,rate_limited,1200,1,true,0.004' \
  'charge_card,timeout,900,1,false,0.031' \
  | elixir AgentToolRetryBudget.ex --jitter-ms 0

# stdout, tab separated (spaced out here for readability)
# tool         code          attempt  retry  delay_ms  reason
# search_docs  rate_limited  1        true   250       retry allowed inside budget
# charge_card  timeout       1        false  0         tool call is not idempotent
#
# stderr: allowed=1 denied=1 cost_at_risk=0.035
# with the default 75ms window, delay_ms is 250 plus a fixed per attempt offset

# CI gate: exit 2 if any row would be denied a retry
cat fixtures/tool_calls.csv | elixir AgentToolRetryBudget.ex || echo "policy denied a retry"
```

## Notes

- It decides, it does not act. Nothing here sleeps, calls a tool or performs a retry. `delay_ms` is a number for your scheduler to honour.
- `retryable_codes` is hard coded in the struct. Changing the set means editing line 8, there is no flag for it.
- `cost_at_risk` sums `cost_usd` across every row, allowed and denied alike. It is the cost of the whole batch, not of the denials only.
- `parse_args/1` does not verify that each flag got a value. A flag followed immediately by another flag leaves a sentinel atom in that field, and Elixir term ordering puts every integer below every atom, so that budget check silently becomes always true. Pass values with your flags.
- The delay is clamped to `max_elapsed_ms`, not to the time remaining. A row near its elapsed limit can still get a delay that overshoots it.
- Any line starting with `tool,` is treated as a header and dropped, so a data row whose tool is named `tool` is skipped.
- Exit codes: 0 when every row was allowed, 2 when at least one was denied, 64 on any parse or argument error.
- Standard library Elixir only. No mix project, no deps, no config file.
