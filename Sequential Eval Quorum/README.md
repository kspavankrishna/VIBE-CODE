# Sequential Eval Quorum

Evaluator results for an LLM or agent release arrive as a noisy stream of pairwise events, not a clean spreadsheet, and somebody still has to answer promote or rollback. This is a dependency free Elixir gate that counts each observation once, tracks per cohort statistics and returns an explicit decision with the numbers behind it.

**Language:** Elixir | **Lines:** 950 | **Added:** 2026-06-17

## What this solves

You change a prompt, swap a model, or edit tool routing. An eval harness starts emitting baseline versus candidate scores from CI jobs, shadow traffic, benchmark workers and production canaries. The events do not arrive in order. Retries duplicate them. Some carry a numeric score, some carry only a judge verdict string like `"candidate"`. Some are hours old because a worker was backed up. Now decide whether to ship.

Without something like this the team either waits for the full expensive suite, which costs days and real money on evaluator calls, or ships the canary after forty lucky wins and argues later. Same root cause both ways: the decision rule lives in someone's head instead of in code. What goes wrong is boring and expensive. Duplicate retries get counted twice and inflate the win rate, so a flat candidate looks like a winner. One task family dominates the sample because it is cheapest to run, so the aggregate hides a regression in the cohort carrying most of your traffic. A stale batch from before the last prompt fix lands in the same bucket as fresh evidence. An eval job runs unbounded overnight and burns thousands of dollars, because nothing had a spend limit.

Cohort skew and duplicate counting get noticed by customers, after the rollout, in the slice you were not sampling. Runaway cost gets noticed by whoever reads the bill. Neither argument is winnable without the raw counts, which is exactly what nobody kept. This module holds that state and returns one of five statuses: `:promote`, `:rollback`, `:hold`, `:stop_budget` or `:continue`, each with a machine readable `reason`, a plain English `advice` string and the statistics behind it.

## Why I built it

Sequential testing libraries exist, but they assume clean tidy data and they mostly live in Python notebooks. The gap is on the ingestion side. In an Elixir service receiving evaluator events over Broadway or Oban, the hard part is not the Wilson formula, it is that the stream is dirty: mixed key types, retries, half filled events, string numbers, missing cohorts and no spend ceiling. An eval framework tells you the score. It does not tell you to stop.

The other gap is cost and cohort coverage as stopping conditions. Most stat helpers stop on significance or sample count only. In LLM evaluation the two things that bite are an expensive evaluator and an aggregate that lies when one task family is oversampled. Both are configuration here, not a wrapper someone writes later and forgets.

## When to use it

- An Oban or Broadway worker ingests judge results for a candidate prompt and something must decide when there is enough evidence to promote.
- A model swap is running in shadow traffic and you need a hard spend cap that halts the run before the bill grows.
- At least once delivery means the same evaluation event arrives three times and must be counted once.
- Your benchmark covers retrieval, tool use and long context, and the aggregate must not be trusted until every cohort has real samples.
- A release dashboard or Livebook needs a JSON friendly verdict with counts, intervals and the per cohort breakdown.
- MCP tool reliability or RAG answer quality is compared against an incumbent and the evaluator itself sometimes errors.

## How it works

The module is a plain struct, not a process. You hold the state and thread it through. `new/1` takes a keyword list or a map and coerces every option, so `min_samples: "200"` survives and a garbage value falls back to the default instead of crashing at decision time. `Config` carries the policy: `min_samples` 48, `max_samples` 1200, `confidence` 0.95 clamped into 0.5 to 0.9999, `min_effect` and `min_win_rate_lift` 0.02, `max_error_rate` 0.08, `tie_epsilon` 1.0e-9, with `max_cost_micros` and `freshness_window_ms` defaulting to `:infinity`. Pin `now_ms` for deterministic tests.

`add/2` runs `normalize_observation/2`, a `with` chain over id, delta, cost, timestamp and freshness. Lookup goes through `first_value/2` and `option/3`, which check the atom key and the string key, so a JSON decoded map works without a conversion pass, and aliases are accepted throughout: `:id`, `:event_id` or `:run_id`; `:stratum`, `:cohort` or `:task_family`; `:cost_micros` or `:cost_usd`. Delta resolution is ordered in `normalize_delta/1`: an explicit `:delta` wins, otherwise candidate minus baseline score, otherwise a judge verdict mapped to 1.0, -1.0 or 0.0 by `normalize_outcome/1`, which reads atoms and case insensitive strings like `"candidate_win"` and `"draw"`. Anything else is rejected, and a rejection stores `%{reason: reason, at_ms: ...}` only, never the observation body, so prompts and traces never sit in gate state.

Deduplication is a `MapSet` of normalized ids, and a repeat returns `{:duplicate, state}` unchanged. `observation_id/1` builds a stable key by joining parts with a NUL byte and hashing SHA-256 to lowercase hex, over task, candidate version, baseline version and rubric rather than raw user text. Accepted evidence lands in a `Stratum` struct holding samples, wins, losses, ties, `sum_delta`, `sum_sq_delta`, cost, errors and the latest timestamp. Those sufficient statistics are what make it streaming: memory grows with cohorts, not observations. Win, loss or tie comes from `classify_delta/2` against `tie_epsilon`, not exact float equality.

Statistics run in `summary/1`. Pairwise win rate uses a Wilson score interval, chosen over the naive normal approximation because it behaves at small samples and near the 0 and 1 boundaries where a canary starts, with ties counted as half a win. The z value comes from `inverse_normal_cdf/1`, an Acklam style rational approximation, so any confidence level works without a stats dependency. Mean delta is stratified: `effective_weights/2` uses your configured `strata_weights` when present and sample proportional weights otherwise, then estimator variance is summed as `w^2 * s^2 / n` per stratum and the interval is mean plus or minus z times its square root.

`decision/1` is a single `cond` evaluated top down, and the order is the policy. Cost budget first, so `:stop_budget` beats every statistical conclusion. Then no evidence, underfilled required strata, `min_samples`, then evaluator error rate, which returns `:rollback` because a broken evaluator invalidates the comparison rather than proving a regression. Only then does it look at effect: promote if the lower bound of the mean delta interval clears `min_effect`, rollback if the upper bound falls below its negative, then the same two tests against the Wilson win rate bounds around `0.5 ± min_win_rate_lift`. Hitting `max_samples` with nothing cleared returns `:hold`, a refusal to auto promote on a tie.

## Usage

```elixir
gate =
  SequentialEvalQuorum.new(
    min_samples: 200,
    max_samples: 5_000,
    confidence: 0.95,
    min_effect: 0.01,
    min_win_rate_lift: 0.03,
    max_error_rate: 0.05,
    max_cost_micros: 25_000_000,
    required_strata: ["retrieval", "tool_use", "long_context"],
    min_samples_per_stratum: 40,
    strata_weights: %{"retrieval" => 0.6, "tool_use" => 0.3, "long_context" => 0.1},
    freshness_window_ms: 6 * 60 * 60 * 1000
  )

id = SequentialEvalQuorum.observation_id(["task-8814", "cand-v37", "base-v36", "rubric-v3"])

{:ok, gate} =
  SequentialEvalQuorum.add(gate, %{
    id: id,
    stratum: "retrieval",
    baseline_score: 0.71,
    candidate_score: 0.78,
    cost_usd: 0.0042,
    observed_at_ms: System.system_time(:millisecond),
    error: false
  })

# Judge verdicts work too, with string keys straight off a JSON decode.
{:ok, gate} =
  SequentialEvalQuorum.add(gate, %{
    "event_id" => "judge-99120",
    "task_family" => "tool_use",
    "outcome" => "candidate_win",
    "cost_micros" => 3_100
  })

# Batch ingest returns a small, log-safe report.
{gate, report} = SequentialEvalQuorum.add_many(gate, event_stream)
# report => %{accepted: 412, duplicates: 19, rejected: %{stale_observation: 3}}

case SequentialEvalQuorum.decision(gate) do
  %{status: :promote} = d -> Release.promote(d.reason, d.mean_delta_interval)
  %{status: :rollback} = d -> Release.rollback(d.reason)
  %{status: :stop_budget} -> Evaluators.halt()
  %{status: :hold} = d -> Release.escalate(d.advice)
  %{status: :continue} -> :keep_sampling
end

# Plain map for any JSON encoder, telemetry metadata or a dashboard.
gate |> SequentialEvalQuorum.decision() |> SequentialEvalQuorum.decision_to_map()

# Full statistics without the verdict, including the per stratum breakdown.
SequentialEvalQuorum.summary(gate)

# Same policy, evidence cleared.
gate = SequentialEvalQuorum.reset(gate)
```

## Notes

- No external dependencies. It uses `:crypto`, `:math`, `Base`, `MapSet` and `System` only, so it drops in without touching mix.exs.
- Not a GenServer and not concurrency safe. The struct is immutable and the caller owns it. Put it behind a single process or an Agent if several workers ingest into the same gate.
- The intervals are fixed sample Wilson and normal intervals evaluated repeatedly as data arrives. There is no alpha spending or always valid correction, so peeking inflates the false positive rate. `min_samples` and the practical thresholds blunt that, they do not eliminate it.
- `estimated_win_rate` is stratum weighted but `win_rate_interval` is the pooled unweighted Wilson interval, and the win rate tests use the pooled one. When configured weights differ sharply from the observed sample mix, the two describe slightly different quantities.
- The dedupe `MapSet` and the rejection list grow without bound for the life of a gate. Call `reset/1` between candidates on a long lived one.
- An error flagged observation still counts as a sample and its delta still contributes to the mean. Errors only drive `error_rate` and the `max_error_rate` gate. Freshness rejects only observations older than the window, so future timestamps pass.
- Rejection reasons are `:missing_id`, `:missing_delta_scores_or_outcome`, `:invalid_cost`, `:invalid_observed_at_ms`, `:stale_observation` and `:observation_must_be_map`.
