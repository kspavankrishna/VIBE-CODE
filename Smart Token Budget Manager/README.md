# Smart Token Budget Manager

LLM API bills have no ceiling by default. This is a small Python guard that checks a request against hourly, daily and monthly token limits before you send it, then records what the call actually cost so the next check is accurate.

**Language:** Python | **Lines:** 127 | **Added:** 2026-04-05

## What this solves

The failure mode is boring and expensive. You ship a feature that calls GPT-4 or Claude, a retry loop misbehaves or one user discovers they can paste a 200 page PDF into your summarizer, and nothing in your stack says no. The API accepts every request. The provider dashboard updates on a lag. You find out at the end of the month, or when finance forwards the invoice and asks what happened on the 14th. There is no runtime object anywhere in the code that knows how many tokens this user has already burned today.

The second failure is per request blowup rather than volume. One prompt with a huge context window attached costs more than a thousand normal calls. Rate limiting by requests per second does not catch it, because it is a single request. You need a limit expressed in tokens, checked before the call goes out, not a counter of HTTP calls.

The third failure is attribution. When the bill does arrive you cannot answer the two questions that matter: which user did this and on which model. Provider billing gives you a total. It will not tell you that one internal project quietly moved from claude-sonnet to claude-opus and multiplied its output cost by five. Without per user and per project tagging at the call site, every cost investigation turns into log archaeology.

This file is the missing object. It holds a config with the limits you care about, a log of every recorded call, and two entry points: one you call before the API request and one you call after. That is the whole contract. Nothing here talks to a provider, so it drops into any client library or FastAPI handler without pulling in an SDK.

## Why I built it

Most cost tooling for LLM APIs is observability, not enforcement. Dashboards, exports and Langfuse style traces tell you what already happened. They are useful for postmortems and useless for prevention, because by the time the data lands the money is spent. The provider side spend caps are account wide and blunt: they either do nothing or they kill your whole product for every user at once.

What I wanted was something that runs in the request path, knows the caller, and returns a plain boolean plus a reason string. No database, no background job, no service to deploy. Timestamps on a list of usage records are enough to slice by any time window you want, and that trick keeps the whole thing to one file with zero dependencies outside the standard library.

## When to use it

- A public or freemium AI feature where one abusive account can outspend all your paying users combined.
- An internal agent or batch job that loops, where a bug means it calls the model until you notice.
- Multi tenant SaaS where each customer needs their own daily token allowance enforced, not just measured.
- A prototype about to be shown to real users, where you want a hard per request ceiling before anyone pastes a book into the prompt box.
- Cost estimation in a quote or preview flow, where you want to show a user roughly what a call will cost before they confirm it.
- Any service where you need per user and per project cost attribution that the provider invoice will never give you.

## How it works

Three pieces. `TokenUsage` is a dataclass holding one completed call: `timestamp`, `model`, `input_tokens`, `output_tokens`, computed `cost`, plus `user_id` and `project_id` for attribution. `BudgetConfig` holds the policy: optional `hourly_limit`, `daily_limit` and `monthly_limit`, a `per_request_limit` defaulting to 100000 tokens, and a `model_costs` table mapping model names to input and output rates per 1000 tokens. The shipped table covers gpt-4, claude-opus and claude-sonnet. `SmartTokenBudgetManager` wraps a `BudgetConfig`, an in memory `usage_log` list, an `alerts` list and a `blocked_requests` counter.

`check_before_request(model, estimated_tokens, user_id)` is the gate. It returns a `(bool, Optional[str])` tuple so the caller gets both the decision and a human readable reason. First it compares `estimated_tokens` against `per_request_limit` and rejects outright if the single call is too big. Then it computes three cutoffs from `time.time()`: 3600, 86400 and 2592000 seconds back. For each configured limit it walks `usage_log`, sums `input_tokens + output_tokens` for records newer than the cutoff and matching the `user_id`, and rejects if that running total plus the estimate would cross the line. These are sliding windows, not calendar buckets. The month window is a rolling 30 days.

There is no ring buffer and no aggregation index. Every check is a linear scan over the full log, filtered by timestamp and user. That trade keeps the accounting exact and the code auditable, and it costs O(n) per check where n is every call recorded in the process. At a few thousand records that is microseconds. At a few million it is not, and you should be trimming the log or moving the counters into Redis.

`record_usage(model, input_tokens, output_tokens, user_id, project_id)` is the post call half. It looks up the model in `model_costs`, falls back to a default rate of 0.001 input and 0.002 output if the model is unknown rather than raising, then computes `cost` as `(input_tokens * input_rate + output_tokens * output_rate) / 1000`. The division by 1000 is why the rate table is expressed per 1000 tokens. It appends a `TokenUsage` record and, if the single call cost more than 1.00, pushes a string onto `alerts` naming the amount and the user. That threshold is hardcoded.

`get_stats(hours, user_id)` slices the log by a cutoff of `hours * 3600` seconds and optionally by user, then returns a dict with request count, input, output and total token sums, total and average cost formatted as dollar strings, and `top_model`, chosen as the model with the largest summed output token count in the window. Cost fields come back as preformatted strings, so treat the return value as a report, not as numbers to do arithmetic on.

`predict_cost(model, prompt_length, max_tokens)` estimates spend before a call using the standard four characters per token heuristic: `prompt_length // 4` for input, `max_tokens` taken at face value for output, both priced through the same rate table. It returns a float, not a string. Use it for preview and quoting, and remember it is an upper bound on output since models usually stop short of `max_tokens`.

## Usage

```python
from SmartTokenBudgetManager import SmartTokenBudgetManager, BudgetConfig

config = BudgetConfig(
    hourly_limit=50_000,
    daily_limit=500_000,
    monthly_limit=10_000_000,
    per_request_limit=100_000,
)
budget = SmartTokenBudgetManager(config)

# 1. estimate before you commit
estimate = budget.predict_cost("claude-sonnet", prompt_length=len(prompt), max_tokens=800)

# 2. gate the call
ok, reason = budget.check_before_request(
    model="claude-sonnet",
    estimated_tokens=len(prompt) // 4 + 800,
    user_id="user_9134",
)
if not ok:
    raise RuntimeError(reason)   # e.g. "Would exceed daily limit (498200 + 4300)"

# 3. call your provider, then record the real numbers
response = client.messages.create(...)
budget.record_usage(
    model="claude-sonnet",
    input_tokens=response.usage.input_tokens,
    output_tokens=response.usage.output_tokens,
    user_id="user_9134",
    project_id="summarizer",
)

# 4. read it back
print(budget.get_stats(hours=24, user_id="user_9134"))
print(budget.alerts)
```

## Notes

- State is in memory only. `usage_log` and `alerts` die with the process, so every restart resets every window. Persist or externalize the counters if you run more than one worker.
- Not thread safe and not async safe. `usage_log` is a plain list with no lock. Under concurrency two requests can both pass the check before either records usage, so limits are approximate at high parallelism.
- `check_before_request` accepts a `model` argument but does not use it. There are no per model limits, only per user token limits.
- `per_request_limit` is checked globally, not per user, and unlike the three window limits it is always enforced because it has a non optional default of 100000.
- Windows are rolling, not calendar aligned. "Monthly" means the last 2592000 seconds, which is 30 days. If you need billing period alignment you have to change that constant.
- `blocked_requests` is initialized to 0 and never incremented anywhere in the file. It is a placeholder, not a working counter.
- The $1.00 high cost alert threshold is hardcoded inside `record_usage` and is not part of `BudgetConfig`.
- Unknown models silently price at 0.001 in and 0.002 out per 1000 tokens instead of raising, so a typo in a model name produces wrong costs rather than an error. Keep `model_costs` current, the built in rates are a snapshot and provider pricing moves.
- Needs Python 3.9 or newer for the builtin `tuple[...]` annotation. No third party dependencies. `hashlib`, `defaultdict`, `datetime` and `timedelta` are imported but unused.
