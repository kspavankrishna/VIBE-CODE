defmodule AgentBudgetQuorum do
  @moduledoc """
  Deterministic admission control for expensive agent tool calls.

  Modern agent systems often ask more than one model, provider, or worker to
  propose the same external side effect: run a deploy, charge a customer, write
  a ticket, send an email, reserve a GPU, or call an internal MCP tool. This
  module turns those competing proposals into one auditable decision. It checks
  idempotency, provider quorum, risk, freshness, leases, and spend limits before
  anything outside the BEAM is allowed to run.

  The module is deliberately storage agnostic. Keep the returned ledger in an
  Agent, GenServer, ETS owner process, durable event log, or request scoped
  reducer. Every public function returns the next ledger instead of mutating
  hidden state, which makes the admission path replayable during incident review.
  """

  defmodule Ledger do
    @moduledoc """
    In-memory shape for pending reservations, recent fingerprints, and spend.
    Persist this structure or its event log if admission must survive restarts.
    """

    defstruct policy: %{},
              seen: %{},
              reservations: %{},
              spend: %{
                reserved_micros: 0,
                committed_micros: 0,
                by_run_reserved: %{},
                by_run_committed: %{},
                by_tool_reserved: %{},
                by_tool_committed: %{}
              }
  end

  defmodule Decision do
    @moduledoc """
    The result returned by `admit/3`. A status of `:accepted` means the caller
    may execute the side effect and must later call `record_result/4`.
    """

    defstruct status: :rejected,
              reason: nil,
              reasons: [],
              fingerprint: nil,
              run_id: nil,
              tool: nil,
              mode: :execute,
              cost_micros: 0,
              risk: 0.0,
              required_quorum_weight: 0.0,
              observed_quorum_weight: 0.0,
              providers: [],
              expires_at_ms: nil,
              at_ms: nil,
              metadata: %{}
  end

  @type run_id :: String.t()
  @type tool :: String.t()
  @type provider :: String.t()
  @type fingerprint :: String.t()
  @type micros :: non_neg_integer()
  @type millis :: integer()
  @type proposal :: map()
  @type policy :: map()

  @missing :__agent_budget_quorum_missing__

  @doc """
  Builds a conservative policy. Pass overrides for budget limits, provider
  weights, risk quorum buckets, allowlists, and TTLs.
  """
  @spec default_policy(policy()) :: policy()
  def default_policy(overrides \\ %{}) when is_map(overrides) do
    %{
      max_clock_skew_ms: 15_000,
      max_proposal_age_ms: 60_000,
      duplicate_ttl_ms: 10 * 60_000,
      lease_ttl_ms: 120_000,
      total_budget_micros: nil,
      run_budget_micros: %{},
      tool_budget_micros: %{},
      provider_weights: %{},
      minimum_confidence: 0.55,
      max_risk: 0.92,
      risk_quorums: [
        {0.30, 1.0},
        {0.65, 2.0},
        {0.85, 3.0},
        {1.0, 4.0}
      ],
      allowed_tools: :any,
      denied_tools: MapSet.new(),
      dry_run_tools: MapSet.new()
    }
    |> Map.merge(overrides)
    |> normalize_policy()
  end

  @doc """
  Creates a new empty ledger from a normalized policy.
  """
  @spec new(policy()) :: %Ledger{}
  def new(policy \\ default_policy()) when is_map(policy) do
    %Ledger{policy: normalize_policy(policy)}
  end

  @doc """
  Attempts to admit one proposal or a list of proposals for the same intended
  side effect. The highest weighted fingerprint group wins; all checks are then
  applied to that group.
  """
  @spec admit(%Ledger{}, proposal() | [proposal()], millis()) ::
          {:ok, %Decision{}, %Ledger{}} | {:error, %Decision{}, %Ledger{}}
  def admit(%Ledger{} = ledger, proposals, now_ms \\ system_ms()) do
    ledger = compact(ledger, now_ms)

    case normalize_all(List.wrap(proposals), now_ms) do
      {:ok, []} ->
        decision = malformed_decision(:no_proposals, now_ms)
        {:error, decision, ledger}

      {:ok, normalized} ->
        group = strongest_fingerprint_group(normalized, ledger.policy)
        evaluate_group(ledger, group, now_ms)

      {:error, {index, reason}} ->
        decision = malformed_decision({:malformed_proposal, index, reason}, now_ms)
        {:error, decision, ledger}
    end
  end

  @doc """
  Records the execution result for an accepted fingerprint. This releases the
  active lease, moves charged cost from reserved to committed spend, and keeps a
  short-lived duplicate fence so retries do not re-run the side effect.
  """
  @spec record_result(%Ledger{}, fingerprint(), map() | atom(), millis()) ::
          {:ok, map(), %Ledger{}} | {:error, map(), %Ledger{}}
  def record_result(%Ledger{} = ledger, fingerprint, result \\ %{}, now_ms \\ system_ms()) do
    ledger = compact(ledger, now_ms)
    normalized_result = normalize_result(result)

    case Map.fetch(ledger.reservations, fingerprint) do
      {:ok, reservation} ->
        charged = result_cost(normalized_result, reservation.cost_micros)

        spend =
          ledger.spend
          |> release_spend(reservation.run_id, reservation.tool, reservation.cost_micros)
          |> commit_spend(reservation.run_id, reservation.tool, charged)

        seen_entry = %{
          run_id: reservation.run_id,
          tool: reservation.tool,
          providers: reservation.providers,
          reserved_micros: reservation.cost_micros,
          charged_micros: charged,
          status: get_any(normalized_result, [:status, "status"], :recorded),
          metadata: Map.get(normalized_result, :metadata, %{}),
          completed_at_ms: now_ms,
          expires_at_ms: now_ms + ledger.policy.duplicate_ttl_ms
        }

        next = %{
          ledger
          | reservations: Map.delete(ledger.reservations, fingerprint),
            seen: Map.put(ledger.seen, fingerprint, seen_entry),
            spend: spend
        }

        {:ok, seen_entry, next}

      :error ->
        {:error, %{error: :unknown_or_expired_fingerprint, fingerprint: fingerprint}, ledger}
    end
  end

  @doc """
  Removes expired duplicate fences and leases. Expired leases release reserved
  budget because the caller did not report an execution result in time.
  """
  @spec compact(%Ledger{}, millis()) :: %Ledger{}
  def compact(%Ledger{} = ledger, now_ms \\ system_ms()) do
    {active_reservations, expired_reservations} =
      Enum.split_with(ledger.reservations, fn {_fingerprint, reservation} ->
        reservation.expires_at_ms > now_ms
      end)

    spend =
      Enum.reduce(expired_reservations, ledger.spend, fn {_fingerprint, reservation}, acc ->
        release_spend(acc, reservation.run_id, reservation.tool, reservation.cost_micros)
      end)

    seen =
      ledger.seen
      |> Enum.reject(fn {_fingerprint, entry} -> entry.expires_at_ms <= now_ms end)
      |> Map.new()

    %{ledger | reservations: Map.new(active_reservations), seen: seen, spend: spend}
  end

  @doc """
  Produces a stable fingerprint for a tool call. Provide `arguments_digest` if
  arguments are already canonicalized upstream; otherwise this function sorts
  maps before hashing the Erlang external term format.
  """
  @spec fingerprint(run_id(), tool(), term(), String.t() | nil, String.t() | nil) :: fingerprint()
  def fingerprint(run_id, tool, arguments, arguments_digest \\ nil, idempotency_key \\ nil) do
    case optional_string(idempotency_key, nil) do
      nil ->
        digest = optional_string(arguments_digest, nil) || digest_term(arguments)
        digest_term(["agent-budget-quorum-v1", to_string(run_id), to_string(tool), digest])

      key ->
        "idempotency:" <> key
    end
  end

  @doc """
  Returns a serializable view of a decision for logs, spans, or audit rows.
  """
  @spec decision_to_map(%Decision{}) :: map()
  def decision_to_map(%Decision{} = decision), do: Map.from_struct(decision)

  @doc """
  Lightweight ledger summary suitable for dashboards. It intentionally excludes
  full seen entries because those may include operator metadata.
  """
  @spec summary(%Ledger{}) :: map()
  def summary(%Ledger{} = ledger) do
    %{
      active_reservations: map_size(ledger.reservations),
      duplicate_fences: map_size(ledger.seen),
      reserved_micros: ledger.spend.reserved_micros,
      committed_micros: ledger.spend.committed_micros,
      by_run_reserved: ledger.spend.by_run_reserved,
      by_run_committed: ledger.spend.by_run_committed,
      by_tool_reserved: ledger.spend.by_tool_reserved,
      by_tool_committed: ledger.spend.by_tool_committed
    }
  end

  defp normalize_all(proposals, now_ms) do
    result =
      proposals
      |> Enum.with_index()
      |> Enum.reduce_while({:ok, []}, fn {proposal, index}, {:ok, acc} ->
        case normalize_proposal(proposal, now_ms) do
          {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
          {:error, reason} -> {:halt, {:error, {index, reason}}}
        end
      end)

    case result do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp normalize_proposal(proposal, now_ms) when is_map(proposal) do
    with {:ok, run_id} <- required_string(proposal, :run_id),
         {:ok, tool} <- required_string(proposal, :tool),
         {:ok, provider} <- required_string(proposal, :provider),
         {:ok, cost_micros} <- non_negative_integer(get_any(proposal, [:cost_micros, "cost_micros"], 0), :cost_micros),
         {:ok, risk} <- unit_float(get_any(proposal, [:risk, "risk"], 0.5), :risk),
         {:ok, confidence} <- unit_float(get_any(proposal, [:confidence, "confidence"], 1.0), :confidence),
         {:ok, observed_at_ms} <- integer_ms(get_any(proposal, [:observed_at_ms, "observed_at_ms"], now_ms), :observed_at_ms) do
      arguments = get_any(proposal, [:arguments, "arguments"], %{})
      arguments_digest = optional_string(get_any(proposal, [:arguments_digest, "arguments_digest"], nil), nil)
      idempotency_key = optional_string(get_any(proposal, [:idempotency_key, "idempotency_key"], nil), nil)
      capability = optional_string(get_any(proposal, [:capability, "capability"], nil), nil)
      fp = fingerprint(run_id, tool, arguments, arguments_digest, idempotency_key)

      {:ok,
       %{
         run_id: run_id,
         tool: tool,
         provider: provider,
         arguments: arguments,
         arguments_digest: arguments_digest,
         idempotency_key: idempotency_key,
         capability: capability,
         fingerprint: fp,
         cost_micros: cost_micros,
         risk: risk,
         confidence: confidence,
         observed_at_ms: observed_at_ms,
         raw: proposal
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_proposal(_proposal, _now_ms), do: {:error, :proposal_must_be_a_map}

  defp strongest_fingerprint_group(proposals, policy) do
    proposals
    |> Enum.group_by(& &1.fingerprint)
    |> Enum.max_by(fn {_fingerprint, candidates} ->
      {quorum_weight(candidates, policy), length(candidates), max_confidence(candidates)}
    end)
    |> elem(1)
  end

  defp evaluate_group(%Ledger{} = ledger, group, now_ms) do
    policy = ledger.policy
    candidate = representative(group, policy)
    providers = group |> Enum.map(& &1.provider) |> Enum.uniq() |> Enum.sort()
    errors = proposal_errors(group, policy, now_ms)
    observed_weight = quorum_weight(group, policy)
    {required_weight, quorum_bucket} = required_quorum(policy, candidate.risk)

    cond do
      Map.has_key?(ledger.reservations, candidate.fingerprint) ->
        reject(ledger, candidate, :duplicate_inflight, [], observed_weight, required_weight, providers, now_ms)

      Map.has_key?(ledger.seen, candidate.fingerprint) ->
        reject(ledger, candidate, :duplicate_recently_completed, [], observed_weight, required_weight, providers, now_ms)

      errors != [] ->
        reject(ledger, candidate, hd(errors), errors, observed_weight, required_weight, providers, now_ms)

      observed_weight < required_weight ->
        reject(
          ledger,
          candidate,
          :quorum_not_met,
          [{:required_weight, required_weight}, {:observed_weight, observed_weight}, {:risk_bucket, quorum_bucket}],
          observed_weight,
          required_weight,
          providers,
          now_ms
        )

      true ->
        case budget_status(ledger, candidate) do
          {:ok, budget_snapshot} ->
            accept(ledger, candidate, observed_weight, required_weight, providers, budget_snapshot, now_ms)

          {:error, reason, budget_snapshot} ->
            reject(ledger, candidate, reason, [budget_snapshot], observed_weight, required_weight, providers, now_ms)
        end
    end
  end

  defp representative(group, policy) do
    base =
      Enum.max_by(group, fn proposal ->
        {provider_weight(proposal.provider, policy), proposal.confidence, -proposal.cost_micros}
      end)

    %{base | cost_micros: max_cost(group), risk: max_risk(group), confidence: max_confidence(group)}
  end

  defp proposal_errors(group, policy, now_ms) do
    candidate = representative(group, policy)

    []
    |> add_if({:tool_denied, candidate.tool}, MapSet.member?(policy.denied_tools, candidate.tool))
    |> add_if({:tool_not_allowed, candidate.tool}, not allowed_tool?(policy.allowed_tools, candidate.tool))
    |> add_if({:risk_too_high, candidate.risk, policy.max_risk}, candidate.risk > policy.max_risk)
    |> add_if({:confidence_too_low, max_confidence(group), policy.minimum_confidence}, max_confidence(group) < policy.minimum_confidence)
    |> add_if(:proposal_from_future, Enum.any?(group, &(&1.observed_at_ms - now_ms > policy.max_clock_skew_ms)))
    |> add_if(:proposal_expired, Enum.any?(group, &(now_ms - &1.observed_at_ms > policy.max_proposal_age_ms)))
    |> Enum.reverse()
  end

  defp accept(ledger, candidate, observed_weight, required_weight, providers, budget_snapshot, now_ms) do
    mode = if MapSet.member?(ledger.policy.dry_run_tools, candidate.tool), do: :dry_run, else: :execute
    expires_at_ms = now_ms + ledger.policy.lease_ttl_ms

    reservation = %{
      run_id: candidate.run_id,
      tool: candidate.tool,
      providers: providers,
      cost_micros: candidate.cost_micros,
      risk: candidate.risk,
      reserved_at_ms: now_ms,
      expires_at_ms: expires_at_ms,
      mode: mode
    }

    next = %{
      ledger
      | reservations: Map.put(ledger.reservations, candidate.fingerprint, reservation),
        spend: reserve_spend(ledger.spend, candidate.run_id, candidate.tool, candidate.cost_micros)
    }

    decision = %Decision{
      status: :accepted,
      reason: :admitted,
      fingerprint: candidate.fingerprint,
      run_id: candidate.run_id,
      tool: candidate.tool,
      mode: mode,
      cost_micros: candidate.cost_micros,
      risk: candidate.risk,
      required_quorum_weight: required_weight,
      observed_quorum_weight: observed_weight,
      providers: providers,
      expires_at_ms: expires_at_ms,
      at_ms: now_ms,
      metadata: %{budget: budget_snapshot}
    }

    {:ok, decision, next}
  end

  defp reject(ledger, candidate, reason, reasons, observed_weight, required_weight, providers, now_ms) do
    decision = %Decision{
      status: :rejected,
      reason: reason,
      reasons: List.wrap(reasons),
      fingerprint: candidate.fingerprint,
      run_id: candidate.run_id,
      tool: candidate.tool,
      cost_micros: candidate.cost_micros,
      risk: candidate.risk,
      required_quorum_weight: required_weight,
      observed_quorum_weight: observed_weight,
      providers: providers,
      at_ms: now_ms
    }

    {:error, decision, ledger}
  end

  defp malformed_decision(reason, now_ms) do
    %Decision{status: :rejected, reason: reason, reasons: [reason], at_ms: now_ms}
  end

  defp budget_status(ledger, candidate) do
    policy = ledger.policy
    spend = ledger.spend
    cost = candidate.cost_micros

    checks = [
      {:total, policy.total_budget_micros, spend.reserved_micros + spend.committed_micros},
      {:run, Map.get(policy.run_budget_micros, candidate.run_id), run_used(spend, candidate.run_id)},
      {:tool, Map.get(policy.tool_budget_micros, candidate.tool), tool_used(spend, candidate.tool)}
    ]

    case Enum.find(checks, fn {_scope, limit, used} -> is_integer(limit) and used + cost > limit end) do
      nil ->
        {:ok,
         %{
           projected_total_micros: spend.reserved_micros + spend.committed_micros + cost,
           projected_run_micros: run_used(spend, candidate.run_id) + cost,
           projected_tool_micros: tool_used(spend, candidate.tool) + cost
         }}

      {scope, limit, used} ->
        {:error, {:budget_exceeded, scope}, %{limit_micros: limit, used_micros: used, requested_micros: cost}}
    end
  end

  defp reserve_spend(spend, run_id, tool, cost) do
    %{
      spend
      | reserved_micros: spend.reserved_micros + cost,
        by_run_reserved: increment(spend.by_run_reserved, run_id, cost),
        by_tool_reserved: increment(spend.by_tool_reserved, tool, cost)
    }
  end

  defp release_spend(spend, run_id, tool, cost) do
    %{
      spend
      | reserved_micros: max(spend.reserved_micros - cost, 0),
        by_run_reserved: decrement(spend.by_run_reserved, run_id, cost),
        by_tool_reserved: decrement(spend.by_tool_reserved, tool, cost)
    }
  end

  defp commit_spend(spend, run_id, tool, cost) do
    %{
      spend
      | committed_micros: spend.committed_micros + cost,
        by_run_committed: increment(spend.by_run_committed, run_id, cost),
        by_tool_committed: increment(spend.by_tool_committed, tool, cost)
    }
  end

  defp increment(map, key, amount), do: Map.update(map, key, amount, &(&1 + amount))

  defp decrement(map, key, amount) do
    Map.update(map, key, 0, fn current -> max(current - amount, 0) end)
  end

  defp run_used(spend, run_id) do
    Map.get(spend.by_run_reserved, run_id, 0) + Map.get(spend.by_run_committed, run_id, 0)
  end

  defp tool_used(spend, tool) do
    Map.get(spend.by_tool_reserved, tool, 0) + Map.get(spend.by_tool_committed, tool, 0)
  end

  defp quorum_weight(group, policy) do
    group
    |> Enum.filter(&(&1.confidence >= policy.minimum_confidence))
    |> Enum.map(& &1.provider)
    |> Enum.uniq()
    |> Enum.reduce(0.0, fn provider, acc -> acc + provider_weight(provider, policy) end)
  end

  defp provider_weight(provider, policy) do
    Map.get(policy.provider_weights, provider, 1.0)
  end

  defp required_quorum(policy, risk) do
    policy.risk_quorums
    |> Enum.sort_by(fn {max_risk, _weight} -> max_risk end)
    |> Enum.find({1.0, :infinity}, fn {max_risk, _weight} -> risk <= max_risk end)
    |> then(fn {bucket, weight} -> {weight, bucket} end)
  end

  defp max_cost(group), do: group |> Enum.map(& &1.cost_micros) |> Enum.max()
  defp max_risk(group), do: group |> Enum.map(& &1.risk) |> Enum.max()
  defp max_confidence(group), do: group |> Enum.map(& &1.confidence) |> Enum.max()

  defp allowed_tool?(:any, _tool), do: true
  defp allowed_tool?(%MapSet{} = allowed, tool), do: MapSet.member?(allowed, tool)
  defp allowed_tool?(allowed, tool) when is_list(allowed), do: tool in Enum.map(allowed, &to_string/1)
  defp allowed_tool?(_, _tool), do: false

  defp normalize_policy(policy) do
    policy
    |> Map.update(:provider_weights, %{}, &normalize_weight_map/1)
    |> Map.update(:run_budget_micros, %{}, &normalize_int_map/1)
    |> Map.update(:tool_budget_micros, %{}, &normalize_int_map/1)
    |> Map.update(:allowed_tools, :any, &normalize_allowed_tools/1)
    |> Map.update(:denied_tools, MapSet.new(), &to_string_set/1)
    |> Map.update(:dry_run_tools, MapSet.new(), &to_string_set/1)
    |> Map.update(:risk_quorums, [{1.0, 1.0}], &normalize_risk_quorums/1)
  end

  defp normalize_weight_map(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), parse_float!(value, 1.0)} end)
  end

  defp normalize_weight_map(_), do: %{}

  defp normalize_int_map(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), parse_int!(value, 0)} end)
  end

  defp normalize_int_map(_), do: %{}

  defp normalize_allowed_tools(:any), do: :any
  defp normalize_allowed_tools("any"), do: :any
  defp normalize_allowed_tools(values), do: to_string_set(values)

  defp to_string_set(%MapSet{} = set), do: MapSet.new(set, &to_string/1)
  defp to_string_set(values) when is_list(values), do: MapSet.new(values, &to_string/1)
  defp to_string_set(nil), do: MapSet.new()
  defp to_string_set(value), do: MapSet.new([to_string(value)])

  defp normalize_risk_quorums(values) when is_list(values) do
    Enum.map(values, fn
      {risk, weight} -> {parse_float!(risk, 1.0), parse_float!(weight, 1.0)}
      [risk, weight] -> {parse_float!(risk, 1.0), parse_float!(weight, 1.0)}
      _ -> {1.0, 1.0}
    end)
  end

  defp normalize_risk_quorums(_), do: [{1.0, 1.0}]

  defp required_string(map, key) do
    case get_any(map, [key, Atom.to_string(key)], @missing) do
      @missing -> {:error, {:missing, key}}
      value -> nonempty_string(value, key)
    end
  end

  defp nonempty_string(value, key) do
    value = to_string(value)

    if String.trim(value) == "" do
      {:error, {:blank, key}}
    else
      {:ok, value}
    end
  rescue
    Protocol.UndefinedError -> {:error, {:not_stringable, key}}
  end

  defp optional_string(nil, default), do: default
  defp optional_string("", default), do: default
  defp optional_string(value, _default) when is_binary(value), do: value
  defp optional_string(value, _default) when is_atom(value), do: Atom.to_string(value)
  defp optional_string(value, _default) when is_integer(value), do: Integer.to_string(value)
  defp optional_string(_value, default), do: default

  defp non_negative_integer(value, field) when is_integer(value) and value >= 0, do: {:ok, value}
  defp non_negative_integer(value, field) when is_float(value) and value >= 0, do: {:ok, round(value)}

  defp non_negative_integer(value, field) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _ -> {:error, {:invalid_non_negative_integer, field}}
    end
  end

  defp non_negative_integer(_value, field), do: {:error, {:invalid_non_negative_integer, field}}

  defp integer_ms(value, field) when is_integer(value), do: {:ok, value}

  defp integer_ms(value, field) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> {:ok, parsed}
      _ -> {:error, {:invalid_millis, field}}
    end
  end

  defp integer_ms(_value, field), do: {:error, {:invalid_millis, field}}

  defp unit_float(value, field) do
    parsed = parse_float!(value, :invalid)

    cond do
      parsed == :invalid -> {:error, {:invalid_float, field}}
      parsed < 0.0 or parsed > 1.0 -> {:error, {:outside_zero_one, field}}
      true -> {:ok, parsed}
    end
  end

  defp parse_float!(value, _default) when is_float(value), do: value
  defp parse_float!(value, _default) when is_integer(value), do: value * 1.0

  defp parse_float!(value, default) when is_binary(value) do
    case Float.parse(value) do
      {parsed, ""} -> parsed
      _ -> default
    end
  end

  defp parse_float!(_value, default), do: default

  defp parse_int!(value, _default) when is_integer(value), do: value
  defp parse_int!(value, _default) when is_float(value), do: round(value)

  defp parse_int!(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _ -> default
    end
  end

  defp parse_int!(_value, default), do: default

  defp get_any(map, keys, default) do
    Enum.reduce_while(keys, default, fn key, _acc ->
      if is_map_key(map, key), do: {:halt, Map.get(map, key)}, else: {:cont, default}
    end)
  end

  defp normalize_result(result) when is_map(result), do: result
  defp normalize_result(status), do: %{status: status}

  defp result_cost(result, default) do
    case non_negative_integer(get_any(result, [:charged_micros, "charged_micros"], default), :charged_micros) do
      {:ok, value} -> value
      {:error, _reason} -> default
    end
  end

  defp digest_term(value) do
    :crypto.hash(:sha256, :erlang.term_to_binary(canonical(value)))
    |> Base.encode16(case: :lower)
  end

  defp canonical(value) when is_map(value) do
    value
    |> Enum.map(fn {key, item} -> {canonical_key(key), canonical(item)} end)
    |> Enum.sort_by(fn {key, _item} -> key end)
  end

  defp canonical(value) when is_list(value), do: Enum.map(value, &canonical/1)
  defp canonical(value) when is_tuple(value), do: value |> Tuple.to_list() |> canonical()
  defp canonical(value), do: value

  defp canonical_key(key) when is_atom(key), do: Atom.to_string(key)
  defp canonical_key(key), do: to_string(key)

  defp add_if(errors, reason, true), do: [reason | errors]
  defp add_if(errors, _reason, false), do: errors

  defp system_ms, do: System.system_time(:millisecond)
end

# This solves the April 2026 problem where agentic developer tools, MCP servers, AI gateways, and internal automation runners can produce the same expensive tool call more than once because several models, retries, or workers agree on an action at nearly the same time. Built because I wanted a small Elixir file that a real production team can drop in front of side effects like deployments, billing writes, GPU reservations, pull request changes, incident tickets, webhook sends, or data pipeline mutations without building a whole policy service first. Use it when one AI agent says run the tool, another agent says the same thing, and you need a deterministic answer about whether the action has enough quorum, enough budget, fresh timestamps, safe risk, and no recent duplicate fingerprint. The trick: proposals are grouped by a stable fingerprint, provider votes are weighted once per provider, risk decides the required quorum, and accepted work receives a short lease so retries cannot silently double-spend. Drop this into an Elixir GenServer, Oban worker, Phoenix API, Livebook research harness, or BEAM-based DevOps controller as an AI tool call admission controller, MCP tool replay guard, LLM side effect budget ledger, agent workflow idempotency layer, streaming AI automation safety gate, or production AI infrastructure quorum checker.
