defmodule SequentialEvalQuorum do
  @moduledoc """
  Dependency-free sequential evaluation gate for streamed AI and agent canary evidence.

  The module accepts pairwise baseline-versus-candidate observations, deduplicates
  retries by observation id, keeps per-stratum statistics, and returns an explicit
  rollout decision. It is designed for Elixir services that receive evaluator
  events from CI jobs, shadow traffic, benchmark workers, or production canaries.
  """

  defmodule Config do
    @moduledoc false

    @type t :: %__MODULE__{
            min_samples: pos_integer(),
            max_samples: pos_integer() | :infinity,
            min_samples_per_stratum: non_neg_integer(),
            confidence: float(),
            min_effect: float(),
            min_win_rate_lift: float(),
            tie_epsilon: float(),
            max_cost_micros: non_neg_integer() | :infinity,
            max_error_rate: float(),
            required_strata: [String.t()],
            strata_weights: %{String.t() => float()},
            freshness_window_ms: non_neg_integer() | :infinity,
            now_ms: integer() | nil
          }

    defstruct min_samples: 48,
              max_samples: 1_200,
              min_samples_per_stratum: 0,
              confidence: 0.95,
              min_effect: 0.02,
              min_win_rate_lift: 0.02,
              tie_epsilon: 1.0e-9,
              max_cost_micros: :infinity,
              max_error_rate: 0.08,
              required_strata: [],
              strata_weights: %{},
              freshness_window_ms: :infinity,
              now_ms: nil
  end

  defmodule Stratum do
    @moduledoc false

    @type t :: %__MODULE__{
            samples: non_neg_integer(),
            wins: non_neg_integer(),
            losses: non_neg_integer(),
            ties: non_neg_integer(),
            sum_delta: float(),
            sum_sq_delta: float(),
            cost_micros: non_neg_integer(),
            errors: non_neg_integer(),
            latest_at_ms: integer() | nil
          }

    defstruct samples: 0,
              wins: 0,
              losses: 0,
              ties: 0,
              sum_delta: 0.0,
              sum_sq_delta: 0.0,
              cost_micros: 0,
              errors: 0,
              latest_at_ms: nil
  end

  defmodule Decision do
    @moduledoc false

    @type status :: :continue | :promote | :rollback | :hold | :stop_budget

    @type t :: %__MODULE__{
            status: status(),
            reason: String.t(),
            advice: String.t(),
            confidence: float(),
            samples: non_neg_integer(),
            estimated_win_rate: float(),
            win_rate_interval: {float(), float()},
            mean_delta: float(),
            mean_delta_interval: {float(), float()},
            total_cost_micros: non_neg_integer(),
            error_rate: float(),
            strata: map()
          }

    defstruct status: :continue,
              reason: "not_evaluated",
              advice: "collect more evidence",
              confidence: 0.95,
              samples: 0,
              estimated_win_rate: 0.5,
              win_rate_interval: {0.0, 1.0},
              mean_delta: 0.0,
              mean_delta_interval: {0.0, 0.0},
              total_cost_micros: 0,
              error_rate: 0.0,
              strata: %{}
  end

  @type observation :: %{
          optional(:id) => term(),
          optional(:event_id) => term(),
          optional(:run_id) => term(),
          optional(:stratum) => term(),
          optional(:cohort) => term(),
          optional(:task_family) => term(),
          optional(:baseline_score) => number() | String.t(),
          optional(:candidate_score) => number() | String.t(),
          optional(:delta) => number() | String.t(),
          optional(:outcome) => :win | :loss | :tie | String.t(),
          optional(:cost_micros) => number() | String.t(),
          optional(:cost_usd) => number() | String.t(),
          optional(:observed_at_ms) => integer(),
          optional(:timestamp_ms) => integer(),
          optional(:error) => boolean()
        }

  @type t :: %__MODULE__{
          config: Config.t(),
          seen: MapSet.t(String.t()),
          strata: %{String.t() => Stratum.t()},
          accepted: non_neg_integer(),
          rejected: [map()]
        }

  defstruct config: %Config{},
            seen: MapSet.new(),
            strata: %{},
            accepted: 0,
            rejected: []

  @doc """
  Builds an empty quorum gate.

  Important options:

    * `:min_samples` - minimum accepted observations before a terminal decision.
    * `:confidence` - central confidence level for intervals, usually `0.95`.
    * `:min_effect` - minimum score delta worth promoting.
    * `:min_win_rate_lift` - minimum pairwise win-rate lift over 50 percent.
    * `:required_strata` - cohorts that must be represented before deciding.
    * `:strata_weights` - production traffic weights for weighted mean delta.
    * `:max_cost_micros` - hard evaluator spend budget in micro-dollars.
  """
  @spec new(keyword() | map()) :: t()
  def new(opts \\ []) do
    %__MODULE__{config: build_config(opts)}
  end

  @doc """
  Adds one observation.

  The function returns `{:ok, state}` for accepted evidence, `{:duplicate, state}`
  when an id was already counted, and `{:error, reason, state}` for malformed or
  stale evidence. Rejections do not retain the original observation body, which
  keeps prompts, traces, and customer text out of the gate state.
  """
  @spec add(t(), observation()) :: {:ok, t()} | {:duplicate, t()} | {:error, atom(), t()}
  def add(%__MODULE__{} = state, observation) do
    case normalize_observation(observation, state.config) do
      {:ok, normalized} ->
        if MapSet.member?(state.seen, normalized.id) do
          {:duplicate, state}
        else
          {:ok, insert_observation(state, normalized)}
        end

      {:error, reason} ->
        rejected = %{reason: reason, at_ms: current_ms(state.config)}
        {:error, reason, %{state | rejected: [rejected | state.rejected]}}
    end
  end

  @doc """
  Adds many observations and returns `{state, report}`.

  The report is intentionally small so it can be logged safely from an ingestion
  worker without leaking the actual prompt, completion, tool trace, or evaluator
  payload that produced the score.
  """
  @spec add_many(t(), Enumerable.t()) :: {t(), map()}
  def add_many(%__MODULE__{} = state, observations) do
    initial = %{accepted: 0, duplicates: 0, rejected: %{}}

    Enum.reduce(observations, {state, initial}, fn observation, {acc_state, report} ->
      case add(acc_state, observation) do
        {:ok, next_state} ->
          {next_state, %{report | accepted: report.accepted + 1}}

        {:duplicate, next_state} ->
          {next_state, %{report | duplicates: report.duplicates + 1}}

        {:error, reason, next_state} ->
          rejected = Map.update(report.rejected, reason, 1, &(&1 + 1))
          {next_state, %{report | rejected: rejected}}
      end
    end)
  end

  @doc """
  Returns the current rollout decision.
  """
  @spec decision(t()) :: Decision.t()
  def decision(%__MODULE__{} = state) do
    summary = summary(state)
    config = state.config
    missing = underfilled_required_strata(state)

    cond do
      budget_exceeded?(summary, config) ->
        build_decision(
          :stop_budget,
          "cost_budget_exhausted",
          "stop evaluators, keep the incumbent, and review the spend cap",
          summary,
          config
        )

      summary.samples == 0 ->
        build_decision(
          :continue,
          "no_evidence_yet",
          "send the first baseline-versus-candidate evaluator events",
          summary,
          config
        )

      missing != [] ->
        build_decision(
          :continue,
          "waiting_for_required_strata:#{Enum.join(missing, ",")}",
          "keep sampling the missing task families before trusting the aggregate",
          summary,
          config
        )

      summary.samples < config.min_samples ->
        build_decision(
          :continue,
          "waiting_for_min_samples",
          "collect at least #{config.min_samples} accepted observations",
          summary,
          config
        )

      summary.error_rate > config.max_error_rate ->
        build_decision(
          :rollback,
          "evaluator_error_rate_too_high",
          "rollback the canary path or fix the evaluator before comparing scores",
          summary,
          config
        )

      lower(summary.mean_delta_interval) > config.min_effect ->
        build_decision(
          :promote,
          "candidate_score_delta_cleared",
          "promote the candidate behind the same guard and keep monitoring drift",
          summary,
          config
        )

      upper(summary.mean_delta_interval) < -config.min_effect ->
        build_decision(
          :rollback,
          "candidate_score_delta_regressed",
          "rollback because the candidate is worse by a practical margin",
          summary,
          config
        )

      lower(summary.win_rate_interval) > 0.5 + config.min_win_rate_lift ->
        build_decision(
          :promote,
          "candidate_pairwise_win_rate_cleared",
          "promote because the candidate wins enough paired judgments",
          summary,
          config
        )

      upper(summary.win_rate_interval) < 0.5 - config.min_win_rate_lift ->
        build_decision(
          :rollback,
          "candidate_pairwise_win_rate_regressed",
          "rollback because paired judgments favor the incumbent",
          summary,
          config
        )

      max_samples_reached?(summary, config) ->
        build_decision(
          :hold,
          "max_samples_without_clear_effect",
          "do not promote automatically; inspect slices or raise the sample budget",
          summary,
          config
        )

      true ->
        build_decision(
          :continue,
          "intervals_overlap_practical_threshold",
          "continue sampling until the interval clears the threshold or budget ends",
          summary,
          config
        )
    end
  end

  @doc """
  Returns a stable, log-safe summary map.
  """
  @spec summary(t()) :: map()
  def summary(%__MODULE__{} = state) do
    config = state.config
    total = aggregate(state.strata)
    z = z_value(config.confidence)
    weighted = weighted_stats(state.strata, config, z)
    successes = total.wins + total.ties * 0.5
    win_interval = wilson_interval(successes, total.samples, z)
    weighted_win_rate = weighted_win_rate(state.strata, config)

    %{
      samples: total.samples,
      accepted: state.accepted,
      rejected: rejection_summary(state.rejected),
      duplicates_tracked: MapSet.size(state.seen),
      wins: total.wins,
      losses: total.losses,
      ties: total.ties,
      estimated_win_rate: weighted_win_rate,
      win_rate_interval: win_interval,
      mean_delta: weighted.mean_delta,
      mean_delta_interval: weighted.mean_delta_interval,
      total_cost_micros: total.cost_micros,
      error_rate: safe_div(total.errors, total.samples),
      confidence: config.confidence,
      strata: summarize_strata(state.strata)
    }
  end

  @doc """
  Converts a decision struct to a plain map for JSON encoders, telemetry metadata,
  or release dashboards.
  """
  @spec decision_to_map(Decision.t()) :: map()
  def decision_to_map(%Decision{} = decision) do
    %{
      status: decision.status,
      reason: decision.reason,
      advice: decision.advice,
      confidence: decision.confidence,
      samples: decision.samples,
      estimated_win_rate: decision.estimated_win_rate,
      win_rate_interval: decision.win_rate_interval,
      mean_delta: decision.mean_delta,
      mean_delta_interval: decision.mean_delta_interval,
      total_cost_micros: decision.total_cost_micros,
      error_rate: decision.error_rate,
      strata: decision.strata
    }
  end

  @doc """
  Resets counted evidence while keeping the same gate configuration.
  """
  @spec reset(t()) :: t()
  def reset(%__MODULE__{config: config}) do
    %__MODULE__{config: config}
  end

  @doc """
  Produces a deterministic dedupe key for an evaluator event.

  Use this when the upstream worker does not already have a stable id. The fields
  should identify the evaluated task, candidate version, baseline version, and
  evaluator rubric. Do not include raw user text; hash a content address upstream
  if the task payload itself must participate in the id.
  """
  @spec observation_id(Enumerable.t()) :: String.t()
  def observation_id(parts) do
    parts
    |> Enum.map(&safe_id_part/1)
    |> Enum.join("\0")
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp insert_observation(%__MODULE__{} = state, normalized) do
    stratum = Map.get(state.strata, normalized.stratum, %Stratum{})
    updated = add_to_stratum(stratum, normalized, state.config)

    %{
      state
      | accepted: state.accepted + 1,
        seen: MapSet.put(state.seen, normalized.id),
        strata: Map.put(state.strata, normalized.stratum, updated)
    }
  end

  defp add_to_stratum(%Stratum{} = stratum, normalized, config) do
    outcome = classify_delta(normalized.delta, config.tie_epsilon)

    %Stratum{
      stratum
      | samples: stratum.samples + 1,
        wins: stratum.wins + count_if(outcome == :win),
        losses: stratum.losses + count_if(outcome == :loss),
        ties: stratum.ties + count_if(outcome == :tie),
        sum_delta: stratum.sum_delta + normalized.delta,
        sum_sq_delta: stratum.sum_sq_delta + normalized.delta * normalized.delta,
        cost_micros: stratum.cost_micros + normalized.cost_micros,
        errors: stratum.errors + count_if(normalized.errored),
        latest_at_ms: max_nullable(stratum.latest_at_ms, normalized.observed_at_ms)
    }
  end

  defp build_decision(status, reason, advice, summary, config) do
    %Decision{
      status: status,
      reason: reason,
      advice: advice,
      confidence: config.confidence,
      samples: summary.samples,
      estimated_win_rate: summary.estimated_win_rate,
      win_rate_interval: summary.win_rate_interval,
      mean_delta: summary.mean_delta,
      mean_delta_interval: summary.mean_delta_interval,
      total_cost_micros: summary.total_cost_micros,
      error_rate: summary.error_rate,
      strata: summary.strata
    }
  end

  defp build_config(opts) do
    options = options_to_map(opts)

    %Config{
      min_samples: positive_integer(option(options, :min_samples, 48), 48),
      max_samples: max_integer_or_infinity(option(options, :max_samples, 1_200), 1_200),
      min_samples_per_stratum:
        non_negative_integer(option(options, :min_samples_per_stratum, 0), 0),
      confidence: bounded_float(option(options, :confidence, 0.95), 0.5, 0.9999, 0.95),
      min_effect: non_negative_float(option(options, :min_effect, 0.02), 0.02),
      min_win_rate_lift:
        non_negative_float(option(options, :min_win_rate_lift, 0.02), 0.02),
      tie_epsilon: non_negative_float(option(options, :tie_epsilon, 1.0e-9), 1.0e-9),
      max_cost_micros:
        max_integer_or_infinity(option(options, :max_cost_micros, :infinity), :infinity),
      max_error_rate: bounded_float(option(options, :max_error_rate, 0.08), 0.0, 1.0, 0.08),
      required_strata: normalize_required_strata(option(options, :required_strata, [])),
      strata_weights: normalize_weights(option(options, :strata_weights, %{})),
      freshness_window_ms:
        max_integer_or_infinity(option(options, :freshness_window_ms, :infinity), :infinity),
      now_ms: optional_integer(option(options, :now_ms, nil))
    }
  end

  defp normalize_observation(observation, %Config{} = config) when is_map(observation) do
    with {:ok, id} <- normalize_id(first_value(observation, [:id, :event_id, :run_id])),
         {:ok, delta} <- normalize_delta(observation),
         {:ok, cost_micros} <- normalize_cost_micros(observation),
         {:ok, observed_at_ms} <- normalize_observed_at_ms(observation, config),
         :ok <- check_freshness(observed_at_ms, config) do
      {:ok,
       %{
         id: id,
         delta: delta,
         stratum: normalize_stratum(first_value(observation, [:stratum, :cohort, :task_family])),
         cost_micros: cost_micros,
         observed_at_ms: observed_at_ms,
         errored: truthy?(first_value(observation, [:error, :errored]))
       }}
    end
  end

  defp normalize_observation(_observation, _config), do: {:error, :observation_must_be_map}

  defp normalize_delta(observation) do
    delta = first_value(observation, [:delta, :score_delta])
    candidate = first_value(observation, [:candidate_score, :candidate, :new_score])
    baseline = first_value(observation, [:baseline_score, :baseline, :old_score])
    outcome = first_value(observation, [:outcome, :judgment, :winner])

    cond do
      match?({:ok, _}, number(delta)) ->
        number(delta)

      match?({:ok, _}, number(candidate)) and match?({:ok, _}, number(baseline)) ->
        {:ok, candidate_number(candidate) - candidate_number(baseline)}

      normalize_outcome(outcome) == :win ->
        {:ok, 1.0}

      normalize_outcome(outcome) == :loss ->
        {:ok, -1.0}

      normalize_outcome(outcome) == :tie ->
        {:ok, 0.0}

      true ->
        {:error, :missing_delta_scores_or_outcome}
    end
  end

  defp candidate_number(value) do
    {:ok, parsed} = number(value)
    parsed
  end

  defp normalize_cost_micros(observation) do
    micros = first_value(observation, [:cost_micros, :cost_micro_usd])
    usd = first_value(observation, [:cost_usd, :usd])

    cond do
      is_nil(micros) and is_nil(usd) ->
        {:ok, 0}

      match?({:ok, _}, number(micros)) ->
        {:ok, max(0, round(candidate_number(micros)))}

      match?({:ok, _}, number(usd)) ->
        {:ok, max(0, round(candidate_number(usd) * 1_000_000))}

      true ->
        {:error, :invalid_cost}
    end
  end

  defp normalize_observed_at_ms(observation, config) do
    observed_at = first_value(observation, [:observed_at_ms, :timestamp_ms, :at_ms])

    cond do
      is_nil(observed_at) -> {:ok, current_ms(config)}
      match?({:ok, _}, number(observed_at)) -> {:ok, round(candidate_number(observed_at))}
      true -> {:error, :invalid_observed_at_ms}
    end
  end

  defp check_freshness(_observed_at_ms, %Config{freshness_window_ms: :infinity}), do: :ok

  defp check_freshness(observed_at_ms, %Config{} = config) do
    if observed_at_ms >= current_ms(config) - config.freshness_window_ms do
      :ok
    else
      {:error, :stale_observation}
    end
  end

  defp aggregate(strata) do
    Enum.reduce(strata, empty_total(), fn {_name, stratum}, acc ->
      %{
        acc
        | samples: acc.samples + stratum.samples,
          wins: acc.wins + stratum.wins,
          losses: acc.losses + stratum.losses,
          ties: acc.ties + stratum.ties,
          sum_delta: acc.sum_delta + stratum.sum_delta,
          sum_sq_delta: acc.sum_sq_delta + stratum.sum_sq_delta,
          cost_micros: acc.cost_micros + stratum.cost_micros,
          errors: acc.errors + stratum.errors
      }
    end)
  end

  defp empty_total do
    %{
      samples: 0,
      wins: 0,
      losses: 0,
      ties: 0,
      sum_delta: 0.0,
      sum_sq_delta: 0.0,
      cost_micros: 0,
      errors: 0
    }
  end

  defp weighted_stats(strata, config, z) do
    weights = effective_weights(strata, config)

    if map_size(weights) == 0 do
      %{mean_delta: 0.0, mean_delta_interval: {0.0, 0.0}}
    else
      {mean, estimator_variance} =
        Enum.reduce(weights, {0.0, 0.0}, fn {name, weight}, {mean_acc, var_acc} ->
          stratum = Map.fetch!(strata, name)
          stratum_mean = mean_delta(stratum)
          stratum_variance = sample_variance(stratum)
          variance_piece = weight * weight * stratum_variance / max(stratum.samples, 1)
          {mean_acc + weight * stratum_mean, var_acc + variance_piece}
        end)

      margin = z * :math.sqrt(max(estimator_variance, 0.0))
      %{mean_delta: mean, mean_delta_interval: {mean - margin, mean + margin}}
    end
  end

  defp weighted_win_rate(strata, config) do
    weights = effective_weights(strata, config)

    if map_size(weights) == 0 do
      0.5
    else
      Enum.reduce(weights, 0.0, fn {name, weight}, acc ->
        stratum = Map.fetch!(strata, name)
        acc + weight * adjusted_win_rate(stratum)
      end)
    end
  end

  defp effective_weights(strata, %Config{strata_weights: configured}) when map_size(configured) > 0 do
    configured
    |> Enum.filter(fn {name, weight} -> weight > 0.0 and Map.has_key?(strata, name) end)
    |> normalize_weight_pairs()
  end

  defp effective_weights(strata, _config) do
    strata
    |> Enum.filter(fn {_name, stratum} -> stratum.samples > 0 end)
    |> Enum.map(fn {name, stratum} -> {name, stratum.samples * 1.0} end)
    |> normalize_weight_pairs()
  end

  defp normalize_weight_pairs(pairs) do
    total = Enum.reduce(pairs, 0.0, fn {_name, weight}, acc -> acc + weight end)

    if total <= 0.0 do
      %{}
    else
      Map.new(pairs, fn {name, weight} -> {name, weight / total} end)
    end
  end

  defp summarize_strata(strata) do
    strata
    |> Enum.sort_by(fn {name, _stratum} -> name end)
    |> Map.new(fn {name, stratum} ->
      {name,
       %{
         samples: stratum.samples,
         wins: stratum.wins,
         losses: stratum.losses,
         ties: stratum.ties,
         mean_delta: mean_delta(stratum),
         adjusted_win_rate: adjusted_win_rate(stratum),
         cost_micros: stratum.cost_micros,
         error_rate: safe_div(stratum.errors, stratum.samples),
         latest_at_ms: stratum.latest_at_ms
       }}
    end)
  end

  defp mean_delta(%Stratum{samples: 0}), do: 0.0
  defp mean_delta(%Stratum{} = stratum), do: stratum.sum_delta / stratum.samples

  defp adjusted_win_rate(%Stratum{samples: 0}), do: 0.5

  defp adjusted_win_rate(%Stratum{} = stratum) do
    (stratum.wins + stratum.ties * 0.5) / stratum.samples
  end

  defp sample_variance(%Stratum{samples: samples}) when samples < 2, do: 0.0

  defp sample_variance(%Stratum{} = stratum) do
    numerator = stratum.sum_sq_delta - stratum.sum_delta * stratum.sum_delta / stratum.samples
    max(numerator / (stratum.samples - 1), 0.0)
  end

  defp wilson_interval(_successes, samples, _z) when samples <= 0, do: {0.0, 1.0}

  defp wilson_interval(successes, samples, z) do
    n = samples * 1.0
    phat = successes / n
    z2 = z * z
    denominator = 1.0 + z2 / n
    center = (phat + z2 / (2.0 * n)) / denominator
    spread = z * :math.sqrt((phat * (1.0 - phat) + z2 / (4.0 * n)) / n) / denominator
    {clamp(center - spread, 0.0, 1.0), clamp(center + spread, 0.0, 1.0)}
  end

  defp z_value(confidence) do
    confidence
    |> clamp(0.5, 0.9999)
    |> then(fn central -> inverse_normal_cdf(0.5 + central / 2.0) end)
  end

  defp inverse_normal_cdf(p) when p > 0.0 and p < 1.0 do
    a1 = -39.69683028665376
    a2 = 220.9460984245205
    a3 = -275.9285104469687
    a4 = 138.357751867269
    a5 = -30.66479806614716
    a6 = 2.506628277459239
    b1 = -54.47609879822406
    b2 = 161.5858368580409
    b3 = -155.6989798598866
    b4 = 66.80131188771972
    b5 = -13.28068155288572
    c1 = -0.007784894002430293
    c2 = -0.3223964580411365
    c3 = -2.400758277161838
    c4 = -2.549732539343734
    c5 = 4.374664141464968
    c6 = 2.938163982698783
    d1 = 0.007784695709041462
    d2 = 0.3224671290700398
    d3 = 2.445134137142996
    d4 = 3.754408661907416
    plow = 0.02425
    phigh = 1.0 - plow

    cond do
      p < plow ->
        q = :math.sqrt(-2.0 * :math.log(p))
        (((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6) /
          ((((d1 * q + d2) * q + d3) * q + d4) * q + 1.0)

      p <= phigh ->
        q = p - 0.5
        r = q * q

        (((((a1 * r + a2) * r + a3) * r + a4) * r + a5) * r + a6) * q /
          (((((b1 * r + b2) * r + b3) * r + b4) * r + b5) * r + 1.0)

      true ->
        q = :math.sqrt(-2.0 * :math.log(1.0 - p))

        -(((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6) /
          ((((d1 * q + d2) * q + d3) * q + d4) * q + 1.0)
    end
  end

  defp underfilled_required_strata(%__MODULE__{} = state) do
    Enum.filter(state.config.required_strata, fn name ->
      samples = state.strata |> Map.get(name, %Stratum{}) |> Map.fetch!(:samples)
      samples < state.config.min_samples_per_stratum
    end)
  end

  defp budget_exceeded?(_summary, %Config{max_cost_micros: :infinity}), do: false
  defp budget_exceeded?(summary, %Config{} = config), do: summary.total_cost_micros >= config.max_cost_micros

  defp max_samples_reached?(_summary, %Config{max_samples: :infinity}), do: false
  defp max_samples_reached?(summary, %Config{} = config), do: summary.samples >= config.max_samples

  defp rejection_summary(rejected) do
    Enum.reduce(rejected, %{}, fn %{reason: reason}, acc -> Map.update(acc, reason, 1, &(&1 + 1)) end)
  end

  defp classify_delta(delta, epsilon) when delta > epsilon, do: :win
  defp classify_delta(delta, epsilon) when delta < -epsilon, do: :loss
  defp classify_delta(_delta, _epsilon), do: :tie

  defp normalize_outcome(value) when value in [:win, :candidate, :candidate_win, :better], do: :win
  defp normalize_outcome(value) when value in [:loss, :baseline, :baseline_win, :worse], do: :loss
  defp normalize_outcome(value) when value in [:tie, :draw, :equal, :same], do: :tie

  defp normalize_outcome(value) when is_binary(value) do
    value
    |> String.trim()
    |> String.downcase()
    |> case do
      "win" -> :win
      "candidate" -> :win
      "candidate_win" -> :win
      "better" -> :win
      "loss" -> :loss
      "baseline" -> :loss
      "baseline_win" -> :loss
      "worse" -> :loss
      "tie" -> :tie
      "draw" -> :tie
      "equal" -> :tie
      "same" -> :tie
      _ -> :unknown
    end
  end

  defp normalize_outcome(_value), do: :unknown

  defp normalize_id(nil), do: {:error, :missing_id}

  defp normalize_id(value) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: {:error, :missing_id}, else: {:ok, value}
  end

  defp normalize_id(value) when is_atom(value) or is_integer(value), do: {:ok, to_string(value)}

  defp normalize_id(value) do
    {:ok, value |> :erlang.term_to_binary() |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)}
  end

  defp normalize_stratum(nil), do: "default"

  defp normalize_stratum(value) when is_binary(value) do
    value
    |> String.trim()
    |> case do
      "" -> "default"
      other -> other
    end
  end

  defp normalize_stratum(value), do: value |> to_string() |> normalize_stratum()

  defp normalize_required_strata(values) when is_list(values) do
    values
    |> Enum.map(&normalize_stratum/1)
    |> Enum.uniq()
  end

  defp normalize_required_strata(value), do: [normalize_stratum(value)]

  defp normalize_weights(weights) when is_map(weights) do
    weights
    |> Enum.reduce(%{}, fn {name, weight}, acc ->
      case number(weight) do
        {:ok, parsed} when parsed > 0.0 -> Map.put(acc, normalize_stratum(name), parsed)
        _ -> acc
      end
    end)
  end

  defp normalize_weights(_weights), do: %{}

  defp first_value(map, keys) do
    Enum.reduce_while(keys, nil, fn key, _acc ->
      value = option(map, key, nil)
      if is_nil(value), do: {:cont, nil}, else: {:halt, value}
    end)
  end

  defp option(map, key, default) when is_map(map) and is_atom(key) do
    string_key = Atom.to_string(key)

    cond do
      Map.has_key?(map, key) -> Map.get(map, key)
      Map.has_key?(map, string_key) -> Map.get(map, string_key)
      true -> default
    end
  end

  defp option(map, key, default) when is_map(map), do: Map.get(map, key, default)
  defp option(_map, _key, default), do: default

  defp options_to_map(options) when is_map(options), do: options
  defp options_to_map(options) when is_list(options), do: Enum.into(options, %{})
  defp options_to_map(_options), do: %{}

  defp number(value) when is_integer(value), do: {:ok, value * 1.0}

  defp number(value) when is_float(value) do
    if value == value, do: {:ok, value}, else: {:error, :nan}
  end

  defp number(value) when is_binary(value) do
    trimmed = String.trim(value)

    case Float.parse(trimmed) do
      {parsed, ""} -> {:ok, parsed}
      {parsed, rest} when String.trim(rest) == "" -> {:ok, parsed}
      _ -> {:error, :not_number}
    end
  end

  defp number(_value), do: {:error, :not_number}

  defp positive_integer(value, fallback) do
    case number(value) do
      {:ok, parsed} when parsed >= 1.0 -> round(parsed)
      _ -> fallback
    end
  end

  defp non_negative_integer(value, fallback) do
    case number(value) do
      {:ok, parsed} when parsed >= 0.0 -> round(parsed)
      _ -> fallback
    end
  end

  defp optional_integer(nil), do: nil

  defp optional_integer(value) do
    case number(value) do
      {:ok, parsed} -> round(parsed)
      _ -> nil
    end
  end

  defp max_integer_or_infinity(value, _fallback) when value in [:infinity, "infinity", "Infinity"], do: :infinity

  defp max_integer_or_infinity(value, fallback) do
    case number(value) do
      {:ok, parsed} when parsed >= 1.0 -> round(parsed)
      _ -> fallback
    end
  end

  defp non_negative_float(value, fallback) do
    case number(value) do
      {:ok, parsed} when parsed >= 0.0 -> parsed
      _ -> fallback
    end
  end

  defp bounded_float(value, low, high, fallback) do
    case number(value) do
      {:ok, parsed} -> clamp(parsed, low, high)
      _ -> fallback
    end
  end

  defp clamp(value, low, high), do: value |> max(low) |> min(high)

  defp safe_div(_numerator, denominator) when denominator in [0, 0.0], do: 0.0
  defp safe_div(numerator, denominator), do: numerator / denominator

  defp lower({value, _high}), do: value
  defp upper({_low, value}), do: value

  defp count_if(true), do: 1
  defp count_if(false), do: 0
  defp count_if(nil), do: 0
  defp count_if(_truthy), do: 1

  defp truthy?(value) when value in [true, 1, "1", "true", "TRUE", "yes", "YES"], do: true
  defp truthy?(_value), do: false

  defp max_nullable(nil, value), do: value
  defp max_nullable(value, nil), do: value
  defp max_nullable(left, right), do: max(left, right)

  defp current_ms(%Config{now_ms: now_ms}) when is_integer(now_ms), do: now_ms
  defp current_ms(_config), do: System.system_time(:millisecond)

  defp safe_id_part(value) when is_binary(value), do: value
  defp safe_id_part(value) when is_atom(value) or is_integer(value) or is_float(value), do: to_string(value)
  defp safe_id_part(value), do: value |> :erlang.term_to_binary() |> Base.encode64(padding: false)
end

# This solves the April 2026 problem where AI agent releases, prompt changes, model swaps, and tool-routing edits produce evaluator results as a noisy stream instead of a clean spreadsheet. Built because Pavan kept seeing teams either wait too long for expensive eval suites or ship a canary after a few lucky wins, then argue later about duplicate retries, missing cohorts, stale runs, and hidden evaluator cost. Use it when an Elixir, Phoenix, Oban, Broadway, or Livebook workflow needs a real sequential evaluation gate for LLM regression testing, agent benchmark canaries, prompt A/B testing, model rollout safety, MCP tool reliability checks, RAG answer quality comparison, or production shadow traffic scoring. The trick: every observation is counted once by a stable id, scores are summarized per stratum, configured traffic weights are respected, Wilson win-rate bounds and weighted mean-delta intervals drive the decision, and spend limits stop the run before an evaluation job quietly burns money. Drop this into an ingestion service, CI evaluator collector, release dashboard, or internal research platform when you need a searchable, fork-worthy SequentialEvalQuorum Elixir module that explains promote, rollback, hold, and continue decisions in plain numbers instead of vague confidence talk.
