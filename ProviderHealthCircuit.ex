defmodule ProviderHealthCircuit do
  @moduledoc false

  use GenServer

  @default_failure_threshold 4
  @default_throttle_threshold 2
  @default_half_open_after_ms 15_000
  @default_max_open_ms 180_000
  @default_backoff_multiplier 2.0
  @default_latency_alpha 0.2
  @default_success_alpha 0.15
  @default_latency_budget_ms 2_500
  @default_cost_weight 0.15
  @default_latency_weight 0.25
  @default_load_weight 0.2
  @default_sticky_weight 0.1
  @default_half_open_bonus -0.15
  @default_probe_limit 1

  @timeout_errors [:timeout, :connect_timeout, :read_timeout, :pool_timeout]
  @throttle_statuses MapSet.new([429, 503, 529])
  @timeout_statuses MapSet.new([408, 504])
  @failure_statuses MapSet.new([500, 502, 503, 504, 521, 522, 523, 524, 529])

  defmodule Lease do
    @enforce_keys [:ref, :provider, :started_at_ms]
    defstruct [:ref, :provider, :started_at_ms, :request]

    @type t :: %__MODULE__{
            ref: reference(),
            provider: String.t(),
            started_at_ms: non_neg_integer(),
            request: map()
          }
  end

  defmodule Provider do
    @enforce_keys [:name]
    defstruct name: nil,
              base_url: nil,
              tags: MapSet.new(),
              capabilities: MapSet.new(),
              metadata: %{},
              weight: 1.0,
              cost_per_1k: nil,
              max_in_flight: :infinity,
              circuit_state: :closed,
              opened_until_ms: 0,
              reopen_attempts: 0,
              half_open_probes: 0,
              consecutive_failures: 0,
              consecutive_throttles: 0,
              total_successes: 0,
              total_failures: 0,
              total_throttles: 0,
              total_timeouts: 0,
              in_flight: 0,
              ewma_latency_ms: nil,
              ewma_success: 1.0,
              last_status: nil,
              last_error: nil,
              last_cost_per_1k: nil,
              last_outcome_at_ms: nil

    @type t :: %__MODULE__{
            name: String.t(),
            base_url: String.t() | nil,
            tags: MapSet.t(term()),
            capabilities: MapSet.t(term()),
            metadata: map(),
            weight: float(),
            cost_per_1k: number() | nil,
            max_in_flight: pos_integer() | :infinity,
            circuit_state: :closed | :open | :half_open,
            opened_until_ms: non_neg_integer(),
            reopen_attempts: non_neg_integer(),
            half_open_probes: non_neg_integer(),
            consecutive_failures: non_neg_integer(),
            consecutive_throttles: non_neg_integer(),
            total_successes: non_neg_integer(),
            total_failures: non_neg_integer(),
            total_throttles: non_neg_integer(),
            total_timeouts: non_neg_integer(),
            in_flight: non_neg_integer(),
            ewma_latency_ms: float() | nil,
            ewma_success: float(),
            last_status: integer() | nil,
            last_error: term(),
            last_cost_per_1k: number() | nil,
            last_outcome_at_ms: non_neg_integer() | nil
          }
  end

  defmodule State do
    @enforce_keys [:providers]
    defstruct providers: %{},
              leases: %{},
              failure_threshold: @default_failure_threshold,
              throttle_threshold: @default_throttle_threshold,
              half_open_after_ms: @default_half_open_after_ms,
              max_open_ms: @default_max_open_ms,
              backoff_multiplier: @default_backoff_multiplier,
              latency_alpha: @default_latency_alpha,
              success_alpha: @default_success_alpha,
              latency_budget_ms: @default_latency_budget_ms,
              cost_weight: @default_cost_weight,
              latency_weight: @default_latency_weight,
              load_weight: @default_load_weight,
              sticky_weight: @default_sticky_weight,
              half_open_bonus: @default_half_open_bonus,
              half_open_probe_limit: @default_probe_limit

    @type t :: %__MODULE__{
            providers: %{required(String.t()) => Provider.t()},
            leases: %{required(reference()) => Lease.t()},
            failure_threshold: pos_integer(),
            throttle_threshold: pos_integer(),
            half_open_after_ms: pos_integer(),
            max_open_ms: pos_integer(),
            backoff_multiplier: float(),
            latency_alpha: float(),
            success_alpha: float(),
            latency_budget_ms: pos_integer(),
            cost_weight: float(),
            latency_weight: float(),
            load_weight: float(),
            sticky_weight: float(),
            half_open_bonus: float(),
            half_open_probe_limit: pos_integer()
          }
  end

  @type provider_name :: String.t()
  @type request :: map()
  @type outcome :: map()
  @type checkout_result ::
          {:ok,
           %{
             lease_ref: reference(),
             provider: String.t(),
             base_url: String.t() | nil,
             metadata: map(),
             score: float(),
             circuit_state: :closed | :half_open
           }}
          | {:error, :no_provider, map()}

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec checkout(GenServer.server(), request()) :: checkout_result()
  def checkout(server \\ __MODULE__, request \\ %{}) do
    GenServer.call(server, {:checkout, Map.new(request)})
  end

  @spec complete(GenServer.server(), reference(), outcome()) :: :ok | {:error, :unknown_lease}
  def complete(server \\ __MODULE__, lease_ref, outcome \\ %{}) when is_reference(lease_ref) do
    GenServer.call(server, {:complete, lease_ref, Map.new(outcome)})
  end

  @spec record(GenServer.server(), provider_name(), outcome()) :: :ok | {:error, :unknown_provider}
  def record(server \\ __MODULE__, provider_name, outcome \\ %{}) do
    GenServer.call(server, {:record, normalize_name(provider_name), Map.new(outcome)})
  end

  @spec snapshot(GenServer.server()) :: [map()]
  def snapshot(server \\ __MODULE__) do
    GenServer.call(server, :snapshot)
  end

  @spec provider_snapshot(GenServer.server(), provider_name()) :: {:ok, map()} | {:error, :unknown_provider}
  def provider_snapshot(server \\ __MODULE__, provider_name) do
    GenServer.call(server, {:provider_snapshot, normalize_name(provider_name)})
  end

  @spec reset(GenServer.server(), :all | provider_name()) :: :ok | {:error, :unknown_provider}
  def reset(server \\ __MODULE__, provider_name \\ :all) do
    name =
      case provider_name do
        :all -> :all
        other -> normalize_name(other)
      end

    GenServer.call(server, {:reset, name})
  end

  @impl true
  def init(opts) do
    providers =
      opts
      |> Keyword.fetch!(:providers)
      |> Enum.map(&normalize_provider!/1)
      |> Map.new(fn provider -> {provider.name, provider} end)

    state = %State{
      providers: providers,
      failure_threshold: positive_int(Keyword.get(opts, :failure_threshold, @default_failure_threshold)),
      throttle_threshold: positive_int(Keyword.get(opts, :throttle_threshold, @default_throttle_threshold)),
      half_open_after_ms:
        positive_int(Keyword.get(opts, :half_open_after_ms, @default_half_open_after_ms)),
      max_open_ms: positive_int(Keyword.get(opts, :max_open_ms, @default_max_open_ms)),
      backoff_multiplier: positive_float(Keyword.get(opts, :backoff_multiplier, @default_backoff_multiplier)),
      latency_alpha: probability(Keyword.get(opts, :latency_alpha, @default_latency_alpha)),
      success_alpha: probability(Keyword.get(opts, :success_alpha, @default_success_alpha)),
      latency_budget_ms:
        positive_int(Keyword.get(opts, :latency_budget_ms, @default_latency_budget_ms)),
      cost_weight: non_negative_float(Keyword.get(opts, :cost_weight, @default_cost_weight)),
      latency_weight: non_negative_float(Keyword.get(opts, :latency_weight, @default_latency_weight)),
      load_weight: non_negative_float(Keyword.get(opts, :load_weight, @default_load_weight)),
      sticky_weight: non_negative_float(Keyword.get(opts, :sticky_weight, @default_sticky_weight)),
      half_open_bonus: as_float(Keyword.get(opts, :half_open_bonus, @default_half_open_bonus)),
      half_open_probe_limit:
        positive_int(Keyword.get(opts, :half_open_probe_limit, @default_probe_limit))
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:checkout, request}, _from, state) do
    now_ms = current_time_ms(request)
    state = refresh_circuits(state, now_ms)

    case choose_provider(state, request, now_ms) do
      {:ok, provider, score} ->
        lease = %Lease{
          ref: make_ref(),
          provider: provider.name,
          started_at_ms: now_ms,
          request: Map.drop(request, [:now_ms])
        }

        provider =
          provider
          |> increment_in_flight()
          |> maybe_increment_half_open_probe()

        state =
          state
          |> put_provider(provider)
          |> put_lease(lease)

        reply = %{
          lease_ref: lease.ref,
          provider: provider.name,
          base_url: provider.base_url,
          metadata: provider.metadata,
          score: score,
          circuit_state: provider.circuit_state
        }

        {:reply, {:ok, reply}, state}

      {:error, details} ->
        {:reply, {:error, :no_provider, details}, state}
    end
  end

  def handle_call({:complete, lease_ref, outcome}, _from, state) do
    now_ms = current_time_ms(outcome)
    state = refresh_circuits(state, now_ms)

    case Map.pop(state.leases, lease_ref) do
      {nil, _leases} ->
        {:reply, {:error, :unknown_lease}, state}

      {%Lease{} = lease, leases} ->
        provider = Map.fetch!(state.providers, lease.provider)
        provider = decrement_in_flight(provider)
        outcome = Map.put_new(outcome, :latency_ms, max(now_ms - lease.started_at_ms, 0))
        provider = apply_outcome(provider, state, outcome, now_ms)
        state = %{state | leases: leases} |> put_provider(provider)
        {:reply, :ok, state}
    end
  end

  def handle_call({:record, provider_name, outcome}, _from, state) do
    now_ms = current_time_ms(outcome)
    state = refresh_circuits(state, now_ms)

    case Map.fetch(state.providers, provider_name) do
      {:ok, provider} ->
        provider = apply_outcome(provider, state, outcome, now_ms)
        {:reply, :ok, put_provider(state, provider)}

      :error ->
        {:reply, {:error, :unknown_provider}, state}
    end
  end

  def handle_call(:snapshot, _from, state) do
    now_ms = now_ms()
    state = refresh_circuits(state, now_ms)
    {:reply, Enum.map(sorted_providers(state.providers), &provider_summary(&1, state, now_ms)), state}
  end

  def handle_call({:provider_snapshot, provider_name}, _from, state) do
    now_ms = now_ms()
    state = refresh_circuits(state, now_ms)

    case Map.fetch(state.providers, provider_name) do
      {:ok, provider} -> {:reply, {:ok, provider_summary(provider, state, now_ms)}, state}
      :error -> {:reply, {:error, :unknown_provider}, state}
    end
  end

  def handle_call({:reset, :all}, _from, state) do
    providers =
      state.providers
      |> Enum.map(fn {name, provider} -> {name, reset_provider(provider)} end)
      |> Map.new()

    {:reply, :ok, %{state | providers: providers, leases: %{}}}
  end

  def handle_call({:reset, provider_name}, _from, state) do
    case Map.fetch(state.providers, provider_name) do
      {:ok, provider} ->
        provider = reset_provider(provider)
        leases =
          state.leases
          |> Enum.reject(fn {_ref, lease} -> lease.provider == provider_name end)
          |> Map.new()

        {:reply, :ok, %{state | providers: Map.put(state.providers, provider_name, provider), leases: leases}}

      :error ->
        {:reply, {:error, :unknown_provider}, state}
    end
  end

  defp choose_provider(state, request, now_ms) do
    required_caps = normalize_set(Map.get(request, :required_capabilities, []))
    required_tags = normalize_set(Map.get(request, :required_tags, []))
    preferred = normalize_name_set(Map.get(request, :preferred, []))
    excluded = normalize_name_set(Map.get(request, :excluded, []))
    strategy = Map.get(request, :strategy, :balanced)
    sticky_key = Map.get(request, :sticky_key) || Map.get(request, :tenant)
    max_cost_per_1k = Map.get(request, :max_cost_per_1k)

    candidates =
      state.providers
      |> Map.values()
      |> Enum.filter(fn provider ->
        not MapSet.member?(excluded, provider.name) and
          capabilities_match?(provider, required_caps) and
          tags_match?(provider, required_tags) and
          cost_match?(provider, max_cost_per_1k) and
          available?(provider, state)
      end)
      |> Enum.map(fn provider ->
        score = score_provider(provider, state, strategy, preferred, sticky_key, now_ms)
        {provider, score}
      end)
      |> Enum.sort_by(fn {provider, score} -> {-score, provider.name} end)

    case candidates do
      [{provider, score} | _] ->
        {:ok, provider, score}

      [] ->
        retry_after_ms =
          state.providers
          |> Map.values()
          |> Enum.map(&retry_after_ms(&1, now_ms))
          |> Enum.reject(&is_nil/1)
          |> Enum.min(fn -> state.half_open_after_ms end)

        {:error,
         %{
           retry_after_ms: retry_after_ms,
           providers: Enum.map(sorted_providers(state.providers), &provider_summary(&1, state, now_ms))
         }}
    end
  end

  defp apply_outcome(provider, state, outcome, now_ms) do
    classification = classify_outcome(outcome)
    latency_ms = positive_number_or_nil(Map.get(outcome, :latency_ms))
    status = Map.get(outcome, :status)
    error = Map.get(outcome, :error)
    cost_per_1k = number_or_nil(Map.get(outcome, :cost_per_1k))

    provider =
      provider
      |> update_latency(latency_ms, state.latency_alpha)
      |> Map.put(:last_status, normalize_status(status))
      |> Map.put(:last_error, error)
      |> Map.put(:last_cost_per_1k, cost_per_1k)
      |> Map.put(:last_outcome_at_ms, now_ms)

    case classification do
      :success ->
        provider
        |> update_success_ewma(1.0, state.success_alpha)
        |> close_circuit()
        |> Map.update!(:total_successes, &(&1 + 1))
        |> Map.put(:consecutive_failures, 0)
        |> Map.put(:consecutive_throttles, 0)

      :throttle ->
        provider
        |> update_success_ewma(0.0, state.success_alpha)
        |> Map.update!(:total_throttles, &(&1 + 1))
        |> Map.put(:consecutive_failures, provider.consecutive_failures + 1)
        |> Map.put(:consecutive_throttles, provider.consecutive_throttles + 1)
        |> maybe_open_circuit(state, now_ms, :throttle)

      :timeout ->
        provider
        |> update_success_ewma(0.0, state.success_alpha)
        |> Map.update!(:total_timeouts, &(&1 + 1))
        |> Map.put(:consecutive_failures, provider.consecutive_failures + 1)
        |> Map.put(:consecutive_throttles, 0)
        |> maybe_open_circuit(state, now_ms, :failure)

      :failure ->
        provider
        |> update_success_ewma(0.0, state.success_alpha)
        |> Map.update!(:total_failures, &(&1 + 1))
        |> Map.put(:consecutive_failures, provider.consecutive_failures + 1)
        |> Map.put(:consecutive_throttles, 0)
        |> maybe_open_circuit(state, now_ms, :failure)
    end
  end

  defp maybe_open_circuit(provider, state, now_ms, :throttle) do
    threshold_hit? =
      provider.circuit_state == :half_open or
        provider.consecutive_throttles >= state.throttle_threshold

    if threshold_hit? do
      open_circuit(provider, state, now_ms)
    else
      provider
    end
  end

  defp maybe_open_circuit(provider, state, now_ms, :failure) do
    threshold_hit? =
      provider.circuit_state == :half_open or
        provider.consecutive_failures >= state.failure_threshold

    if threshold_hit? do
      open_circuit(provider, state, now_ms)
    else
      provider
    end
  end

  defp open_circuit(provider, state, now_ms) do
    reopen_attempts = provider.reopen_attempts + 1
    multiplier = :math.pow(state.backoff_multiplier, max(reopen_attempts - 1, 0))
    delay_ms = min(round(state.half_open_after_ms * multiplier), state.max_open_ms)

    %{
      provider
      | circuit_state: :open,
        opened_until_ms: now_ms + delay_ms,
        reopen_attempts: reopen_attempts,
        half_open_probes: 0
    }
  end

  defp close_circuit(provider) do
    %{
      provider
      | circuit_state: :closed,
        opened_until_ms: 0,
        reopen_attempts: 0,
        half_open_probes: 0
    }
  end

  defp refresh_circuits(state, now_ms) do
    providers =
      state.providers
      |> Enum.map(fn {name, provider} ->
        refreshed =
          if provider.circuit_state == :open and provider.opened_until_ms <= now_ms do
            %{provider | circuit_state: :half_open, half_open_probes: 0}
          else
            provider
          end

        {name, refreshed}
      end)
      |> Map.new()

    %{state | providers: providers}
  end

  defp available?(provider, state) do
    circuit_ready?(provider, state) and capacity_ready?(provider)
  end

  defp circuit_ready?(%Provider{circuit_state: :closed}, _state), do: true
  defp circuit_ready?(%Provider{circuit_state: :open}, _state), do: false

  defp circuit_ready?(%Provider{circuit_state: :half_open} = provider, state) do
    provider.half_open_probes < state.half_open_probe_limit and capacity_ready?(provider)
  end

  defp capacity_ready?(%Provider{max_in_flight: :infinity}), do: true
  defp capacity_ready?(%Provider{max_in_flight: max, in_flight: in_flight}), do: in_flight < max

  defp retry_after_ms(%Provider{circuit_state: :open, opened_until_ms: opened_until_ms}, now_ms) do
    max(opened_until_ms - now_ms, 0)
  end

  defp retry_after_ms(%Provider{max_in_flight: :infinity}, _now_ms), do: nil
  defp retry_after_ms(%Provider{max_in_flight: max, in_flight: in_flight}, _now_ms) when in_flight >= max, do: 250
  defp retry_after_ms(_provider, _now_ms), do: nil

  defp score_provider(provider, state, strategy, preferred, sticky_key, now_ms) do
    success_component = provider.ewma_success
    latency_component = latency_component(provider, state.latency_budget_ms)
    load_component = load_component(provider)
    cost_component = cost_component(provider, strategy)
    sticky_component = sticky_component(provider, sticky_key)
    preferred_bonus = if MapSet.member?(preferred, provider.name), do: 0.2, else: 0.0
    half_open_penalty = if provider.circuit_state == :half_open, do: state.half_open_bonus, else: 0.0
    freshness_penalty = freshness_penalty(provider, now_ms)

    weighted =
      success_component +
        latency_component * state.latency_weight +
        load_component * state.load_weight +
        cost_component * state.cost_weight +
        sticky_component * state.sticky_weight +
        preferred_bonus +
        half_open_penalty -
        freshness_penalty

    Float.round(provider.weight + weighted, 6)
  end

  defp latency_component(%Provider{ewma_latency_ms: nil}, _budget_ms), do: 1.0

  defp latency_component(%Provider{ewma_latency_ms: latency_ms}, budget_ms) do
    ratio = latency_ms / max(budget_ms, 1)
    1.0 / (1.0 + ratio)
  end

  defp load_component(%Provider{max_in_flight: :infinity}), do: 1.0

  defp load_component(%Provider{max_in_flight: max, in_flight: in_flight}) do
    used = min(in_flight / max(max, 1), 1.0)
    1.0 - used
  end

  defp cost_component(%Provider{cost_per_1k: nil}, _strategy), do: 0.0
  defp cost_component(%Provider{cost_per_1k: cost}, :cheapest), do: -normalize_cost(cost)
  defp cost_component(%Provider{cost_per_1k: cost}, :lowest_latency), do: -normalize_cost(cost) * 0.25
  defp cost_component(%Provider{cost_per_1k: cost}, :balanced), do: -normalize_cost(cost) * 0.5
  defp cost_component(%Provider{cost_per_1k: cost}, _strategy), do: -normalize_cost(cost) * 0.5

  defp normalize_cost(cost) when is_number(cost), do: min(cost / 100.0, 1.0)

  defp sticky_component(_provider, nil), do: 0.0

  defp sticky_component(provider, sticky_key) do
    bucket = :erlang.phash2({provider.name, sticky_key}, 10_000)
    bucket / 10_000
  end

  defp freshness_penalty(%Provider{last_outcome_at_ms: nil}, _now_ms), do: 0.0

  defp freshness_penalty(%Provider{last_outcome_at_ms: last_outcome_at_ms}, now_ms) do
    age_ms = max(now_ms - last_outcome_at_ms, 0)
    min(age_ms / 1_800_000, 0.1)
  end

  defp capabilities_match?(provider, required) do
    MapSet.size(required) == 0 or MapSet.subset?(required, provider.capabilities)
  end

  defp tags_match?(provider, required) do
    MapSet.size(required) == 0 or MapSet.subset?(required, provider.tags)
  end

  defp cost_match?(_provider, nil), do: true
  defp cost_match?(%Provider{cost_per_1k: nil}, _max_cost_per_1k), do: true
  defp cost_match?(%Provider{cost_per_1k: cost}, max_cost_per_1k), do: cost <= max_cost_per_1k

  defp classify_outcome(outcome) do
    status = normalize_status(Map.get(outcome, :status))
    error = Map.get(outcome, :error)

    cond do
      error in @timeout_errors -> :timeout
      MapSet.member?(@throttle_statuses, status) -> :throttle
      Map.get(outcome, :throttle, false) -> :throttle
      MapSet.member?(@timeout_statuses, status) -> :timeout
      MapSet.member?(@failure_statuses, status) -> :failure
      is_binary(error) and String.contains?(String.downcase(error), "timeout") -> :timeout
      not is_nil(error) -> :failure
      is_integer(status) and status >= 400 -> :failure
      true -> :success
    end
  end

  defp provider_summary(provider, state, now_ms) do
    %{
      name: provider.name,
      base_url: provider.base_url,
      tags: provider.tags |> MapSet.to_list() |> Enum.sort(),
      capabilities: provider.capabilities |> MapSet.to_list() |> Enum.sort(),
      weight: provider.weight,
      cost_per_1k: provider.cost_per_1k,
      max_in_flight: provider.max_in_flight,
      in_flight: provider.in_flight,
      circuit_state: provider.circuit_state,
      retry_after_ms: retry_after_ms(provider, now_ms),
      consecutive_failures: provider.consecutive_failures,
      consecutive_throttles: provider.consecutive_throttles,
      total_successes: provider.total_successes,
      total_failures: provider.total_failures,
      total_throttles: provider.total_throttles,
      total_timeouts: provider.total_timeouts,
      ewma_latency_ms: provider.ewma_latency_ms,
      ewma_success: Float.round(provider.ewma_success, 4),
      last_status: provider.last_status,
      last_error: provider.last_error,
      last_cost_per_1k: provider.last_cost_per_1k,
      last_outcome_at_ms: provider.last_outcome_at_ms,
      live_score: score_provider(provider, state, :balanced, MapSet.new(), nil, now_ms)
    }
  end

  defp normalize_provider!(provider) when is_map(provider) do
    provider
    |> Enum.into([])
    |> normalize_provider!()
  end

  defp normalize_provider!(provider) when is_list(provider) do
    name =
      provider
      |> Keyword.fetch!(:name)
      |> normalize_name()

    if name == "" do
      raise ArgumentError, "provider name must not be blank"
    end

    max_in_flight =
      case Keyword.get(provider, :max_in_flight, :infinity) do
        :infinity -> :infinity
        value -> positive_int(value)
      end

    %Provider{
      name: name,
      base_url: blank_to_nil(Keyword.get(provider, :base_url)),
      tags: normalize_set(Keyword.get(provider, :tags, [])),
      capabilities: normalize_set(Keyword.get(provider, :capabilities, [])),
      metadata: Map.new(Keyword.get(provider, :metadata, %{})),
      weight: positive_float(Keyword.get(provider, :weight, 1.0)),
      cost_per_1k: number_or_nil(Keyword.get(provider, :cost_per_1k)),
      max_in_flight: max_in_flight
    }
  end

  defp normalize_provider!(_provider) do
    raise ArgumentError, "provider entries must be maps or keyword lists"
  end

  defp normalize_name(value) when is_atom(value), do: value |> Atom.to_string() |> String.trim()
  defp normalize_name(value) when is_binary(value), do: String.trim(value)
  defp normalize_name(value), do: value |> to_string() |> String.trim()

  defp normalize_set(%MapSet{} = values), do: values

  defp normalize_set(values) do
    values
    |> List.wrap()
    |> Enum.reject(&is_nil/1)
    |> Enum.map(fn
      value when is_binary(value) -> String.trim(value)
      value -> value
    end)
    |> MapSet.new()
  end

  defp normalize_name_set(%MapSet{} = values) do
    values
    |> MapSet.to_list()
    |> normalize_name_set()
  end

  defp normalize_name_set(values) do
    values
    |> List.wrap()
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&normalize_name/1)
    |> MapSet.new()
  end

  defp increment_in_flight(provider), do: %{provider | in_flight: provider.in_flight + 1}

  defp decrement_in_flight(provider) do
    %{provider | in_flight: max(provider.in_flight - 1, 0)}
  end

  defp maybe_increment_half_open_probe(%Provider{circuit_state: :half_open} = provider) do
    %{provider | half_open_probes: provider.half_open_probes + 1}
  end

  defp maybe_increment_half_open_probe(provider), do: provider

  defp update_latency(provider, nil, _alpha), do: provider

  defp update_latency(provider, latency_ms, alpha) do
    next =
      ewma(provider.ewma_latency_ms, latency_ms, alpha, fn current, sample ->
        current * (1.0 - alpha) + sample * alpha
      end)

    %{provider | ewma_latency_ms: Float.round(next, 3)}
  end

  defp update_success_ewma(provider, sample, alpha) do
    next =
      ewma(provider.ewma_success, sample, alpha, fn current, point ->
        current * (1.0 - alpha) + point * alpha
      end)

    %{provider | ewma_success: Float.round(next, 6)}
  end

  defp ewma(nil, sample, _alpha, _fun), do: sample * 1.0
  defp ewma(current, sample, _alpha, fun), do: fun.(current * 1.0, sample * 1.0)

  defp put_provider(state, provider) do
    %{state | providers: Map.put(state.providers, provider.name, provider)}
  end

  defp put_lease(state, lease) do
    %{state | leases: Map.put(state.leases, lease.ref, lease)}
  end

  defp sorted_providers(providers) do
    providers
    |> Map.values()
    |> Enum.sort_by(& &1.name)
  end

  defp reset_provider(provider) do
    %Provider{
      provider
      | circuit_state: :closed,
        opened_until_ms: 0,
        reopen_attempts: 0,
        half_open_probes: 0,
        consecutive_failures: 0,
        consecutive_throttles: 0,
        total_successes: 0,
        total_failures: 0,
        total_throttles: 0,
        total_timeouts: 0,
        in_flight: 0,
        ewma_latency_ms: nil,
        ewma_success: 1.0,
        last_status: nil,
        last_error: nil,
        last_cost_per_1k: nil,
        last_outcome_at_ms: nil
    }
  end

  defp normalize_status(value) when is_integer(value), do: value

  defp normalize_status(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {status, ""} -> status
      _ -> nil
    end
  end

  defp normalize_status(_value), do: nil

  defp current_time_ms(map) when is_map(map) do
    case Map.get(map, :now_ms) do
      value when is_integer(value) and value > 0 -> value
      value when is_float(value) and value > 0 -> round(value)
      _ -> now_ms()
    end
  end

  defp now_ms do
    System.system_time(:millisecond)
  end

  defp positive_int(value) when is_integer(value) and value > 0, do: value
  defp positive_int(value) when is_float(value) and value > 0, do: round(value)

  defp positive_int(value) when is_binary(value) do
    value
    |> String.trim()
    |> Integer.parse()
    |> case do
      {int, ""} when int > 0 -> int
      _ -> raise ArgumentError, "expected a positive integer, got: #{inspect(value)}"
    end
  end

  defp positive_int(value) do
    raise ArgumentError, "expected a positive integer, got: #{inspect(value)}"
  end

  defp positive_float(value) when is_integer(value) and value > 0, do: value * 1.0
  defp positive_float(value) when is_float(value) and value > 0, do: value

  defp positive_float(value) do
    raise ArgumentError, "expected a positive float, got: #{inspect(value)}"
  end

  defp probability(value) do
    value = as_float(value)

    if value <= 0.0 or value > 1.0 do
      raise ArgumentError, "expected a value between 0 and 1, got: #{inspect(value)}"
    end

    value
  end

  defp non_negative_float(value) do
    value = as_float(value)

    if value < 0.0 do
      raise ArgumentError, "expected a non-negative float, got: #{inspect(value)}"
    end

    value
  end

  defp as_float(value) when is_integer(value), do: value * 1.0
  defp as_float(value) when is_float(value), do: value

  defp as_float(value) do
    raise ArgumentError, "expected a numeric value, got: #{inspect(value)}"
  end

  defp positive_number_or_nil(nil), do: nil
  defp positive_number_or_nil(value) when is_integer(value) and value >= 0, do: value * 1.0
  defp positive_number_or_nil(value) when is_float(value) and value >= 0, do: value
  defp positive_number_or_nil(_value), do: nil

  defp number_or_nil(nil), do: nil
  defp number_or_nil(value) when is_integer(value), do: value
  defp number_or_nil(value) when is_float(value), do: value
  defp number_or_nil(_value), do: nil

  defp blank_to_nil(nil), do: nil

  defp blank_to_nil(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp blank_to_nil(value), do: to_string(value)
end

# This solves multi-provider AI failover and circuit breaking for Elixir and
# Phoenix systems that call OpenAI, Anthropic, Gemini, Groq, DeepSeek, Ollama,
# or OpenRouter from one app.
#
# Built because real April 2026 teams are routing inference across several
# providers, but many Elixir stacks still glue retries, health checks, and
# provider choice together in ad hoc code that gets painful once 429 bursts,
# regional outages, or slow upstreams start stacking up.
#
# Use it when you need one place to choose a healthy provider, cap in-flight
# pressure, react to 429 and 5xx waves, and keep sticky routing stable for
# tenants, sessions, or long-running agent jobs.
#
# The trick: it keeps provider health in memory, scores live candidates with
# latency, load, cost, and stickiness, then moves providers through closed,
# open, and half-open circuit states without forcing a big dependency tree.
#
# Drop this into a Phoenix app, Broadway worker, LiveView backend,
# GenServer-based AI gateway, or inference router where provider outages and
# rate limits can burn latency and budget fast.