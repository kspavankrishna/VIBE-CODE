defmodule InferenceAdmissionLedger do
  @moduledoc """
  Local admission control for expensive inference traffic in Elixir systems.

  This module gives Phoenix, Oban, Broadway, or plain OTP applications one place
  to make four decisions before a model call starts:

  - whether a new request fits the current global concurrency budget
  - whether a tenant still has room inside its own concurrency budget
  - whether the request fits rolling request, token, and spend ceilings
  - whether the request is a retry for work that is already in flight

  A granted admission returns a lease that must later be completed, cancelled, or
  allowed to expire. Active leases hold reserved budget so duplicate workers,
  retries, and bursty traffic cannot overrun the real limits just because the
  model provider responds slowly.

  The implementation is intentionally conservative. When a lease expires without a
  completion signal, the original reservation is settled into the rolling window.
  That keeps the ledger safe under worker crashes and network partitions.
  """

  use GenServer

  @default_window_ms 60_000
  @default_cleanup_interval_ms 5_000
  @default_lease_ttl_ms 30_000
  @default_max_lease_ttl_ms 300_000
  @default_global_concurrency 128
  @default_tenant_concurrency 8
  @usage_metrics [:requests, :input_tokens, :output_tokens, :cost_micros]

  @type limit :: pos_integer() | :infinity
  @type metric :: :requests | :input_tokens | :output_tokens | :cost_micros | :concurrency
  @type tenant_policy :: %{
          optional(:concurrency) => limit(),
          optional(:requests_per_window) => limit(),
          optional(:input_tokens_per_window) => limit(),
          optional(:output_tokens_per_window) => limit(),
          optional(:cost_micros_per_window) => limit()
        }

  defmodule Usage do
    @moduledoc false
    defstruct requests: 0, input_tokens: 0, output_tokens: 0, cost_micros: 0

    @type t :: %__MODULE__{
            requests: non_neg_integer(),
            input_tokens: non_neg_integer(),
            output_tokens: non_neg_integer(),
            cost_micros: non_neg_integer()
          }
  end

  defmodule Lease do
    @moduledoc false
    @enforce_keys [
      :id,
      :tenant,
      :workload,
      :reservation,
      :issued_at_ms,
      :expires_at_ms,
      :max_expires_at_ms,
      :last_seen_at_ms
    ]
    defstruct [
      :id,
      :tenant,
      :workload,
      :priority,
      :idempotency_key,
      :reservation,
      :issued_at_ms,
      :expires_at_ms,
      :max_expires_at_ms,
      :last_seen_at_ms,
      meta: %{}
    ]

    @type t :: %__MODULE__{
            id: binary(),
            tenant: term(),
            workload: term(),
            priority: non_neg_integer(),
            idempotency_key: term() | nil,
            reservation: InferenceAdmissionLedger.Usage.t(),
            issued_at_ms: non_neg_integer(),
            expires_at_ms: non_neg_integer(),
            max_expires_at_ms: non_neg_integer(),
            last_seen_at_ms: non_neg_integer(),
            meta: map()
          }
  end

  defmodule Decision do
    @moduledoc false
    defstruct status: nil,
              reason: nil,
              scope: nil,
              metric: nil,
              retry_after_ms: 0,
              limit: nil,
              observed: nil,
              needed: nil,
              remaining: nil,
              lease_id: nil,
              detail: nil

    @type t :: %__MODULE__{
            status: :granted | :duplicate | :rejected | :invalid,
            reason: atom() | nil,
            scope: :global | :tenant | :workload | nil,
            metric: InferenceAdmissionLedger.metric() | nil,
            retry_after_ms: non_neg_integer() | nil,
            limit: non_neg_integer() | :infinity | nil,
            observed: non_neg_integer() | nil,
            needed: non_neg_integer() | nil,
            remaining: non_neg_integer() | :infinity | nil,
            lease_id: binary() | nil,
            detail: String.t() | nil
          }
  end

  defmodule Window do
    @moduledoc false
    defstruct totals: %Usage{}, entries: :queue.new()

    @type entry :: %{at_ms: non_neg_integer(), usage: InferenceAdmissionLedger.Usage.t()}
    @type t :: %__MODULE__{totals: InferenceAdmissionLedger.Usage.t(), entries: :queue.queue(entry())}
  end

  defmodule State do
    @moduledoc false
    defstruct name: nil,
              policy: %{},
              leases: %{},
              idempotency: %{},
              active_total: 0,
              active_by_tenant: %{},
              active_by_workload: %{},
              global_reserved: %Usage{},
              tenant_reserved: %{},
              global_window: %Window{},
              tenant_windows: %{},
              cleanup_timer: nil
  end

  @doc """
  Starts the ledger.

  Supported options:

  - `:name`
  - `:window_ms`
  - `:cleanup_interval_ms`
  - `:default_lease_ttl_ms`
  - `:max_lease_ttl_ms`
  - `:global_concurrency`
  - `:default_tenant_concurrency`
  - `:workload_concurrency`
  - `:global_requests_per_window`
  - `:tenant_requests_per_window`
  - `:global_input_tokens_per_window`
  - `:tenant_input_tokens_per_window`
  - `:global_output_tokens_per_window`
  - `:tenant_output_tokens_per_window`
  - `:global_cost_micros_per_window`
  - `:tenant_cost_micros_per_window`
  - `:tenant_policies`
  """
  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec child_spec(Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @doc """
  Attempts to reserve capacity for a new inference request.

  Accepted admission options:

  - `:workload` default `:default`
  - `:priority` default `50`
  - `:ttl_ms` default `default_lease_ttl_ms`
  - `:idempotency_key` to collapse duplicate retries while work is in flight
  - `:estimated_input_tokens`
  - `:estimated_output_tokens`
  - `:estimated_cost_micros`
  - `:meta` any map or keyword list with caller metadata
  - `:call_timeout` timeout for the GenServer call itself
  """
  @spec admit(GenServer.server(), term(), Keyword.t()) ::
          {:ok, Lease.t(), Decision.t()} | {:error, Decision.t()}
  def admit(server \\ __MODULE__, tenant, opts \\ []) when is_list(opts) do
    timeout = Keyword.get(opts, :call_timeout, 5_000)
    GenServer.call(server, {:admit, tenant, opts}, timeout)
  end

  @doc """
  Completes a lease and settles the actual usage into the rolling window.

  `usage` can be a map or keyword list with:

  - `:input_tokens`
  - `:output_tokens`
  - `:cost_micros`

  Completing a lease always settles exactly one request into the request window.
  Use `cancel/3` if the upstream call never actually happened.
  """
  @spec complete(GenServer.server(), Lease.t() | binary(), map() | Keyword.t(), Keyword.t()) ::
          {:ok, map()} | {:error, :unknown_lease}
  def complete(server \\ __MODULE__, lease_or_id, usage, opts \\ []) do
    timeout = keyword_or_map_get(opts, :call_timeout, 5_000)
    GenServer.call(server, {:complete, lease_id(lease_or_id), usage}, timeout)
  end

  @doc """
  Cancels a lease and releases the reservation without settling a request.
  """
  @spec cancel(GenServer.server(), Lease.t() | binary(), Keyword.t()) ::
          {:ok, Lease.t()} | {:error, :unknown_lease}
  def cancel(server \\ __MODULE__, lease_or_id, opts \\ []) do
    timeout = Keyword.get(opts, :call_timeout, 5_000)
    GenServer.call(server, {:cancel, lease_id(lease_or_id)}, timeout)
  end

  @doc """
  Refreshes a lease heartbeat.

  A heartbeat may extend the expiration time, but never beyond the lease's
  `max_expires_at_ms` hard stop.
  """
  @spec heartbeat(GenServer.server(), Lease.t() | binary(), Keyword.t()) ::
          {:ok, Lease.t()} | {:error, :unknown_lease}
  def heartbeat(server \\ __MODULE__, lease_or_id, opts \\ []) do
    timeout = Keyword.get(opts, :call_timeout, 5_000)
    GenServer.call(server, {:heartbeat, lease_id(lease_or_id), opts}, timeout)
  end

  @doc """
  Returns a snapshot of active reservations, settled window usage, and headroom.
  """
  @spec snapshot(GenServer.server(), Keyword.t()) :: map()
  def snapshot(server \\ __MODULE__, opts \\ []) do
    timeout = Keyword.get(opts, :call_timeout, 5_000)
    GenServer.call(server, :snapshot, timeout)
  end

  @doc """
  Prunes expired leases and expired window entries immediately.
  """
  @spec prune(GenServer.server(), Keyword.t()) :: map()
  def prune(server \\ __MODULE__, opts \\ []) do
    timeout = Keyword.get(opts, :call_timeout, 5_000)
    GenServer.call(server, :prune, timeout)
  end

  @impl true
  def init(opts) do
    policy = normalize_policy(opts)
    state = %State{name: Keyword.get(opts, :name, __MODULE__), policy: policy}
    {:ok, schedule_cleanup(state)}
  end

  @impl true
  def handle_call({:admit, tenant, opts}, _from, state) do
    now = now_ms()
    state = sweep(state, now)

    reply =
      case build_demand(tenant, opts, state.policy, now) do
        {:error, decision} ->
          {:error, decision}

        {:ok, demand} ->
          case maybe_duplicate(state, demand, now) do
            {:duplicate, lease, decision} ->
              {:ok, lease, decision}

            :miss ->
              case check_admission(state, demand, now) do
                :ok ->
                  {lease, next_state} = grant(state, demand, now)
                  decision = %Decision{status: :granted, lease_id: lease.id, retry_after_ms: 0}
                  {{:ok, lease, decision}, next_state}

                {:error, decision} ->
                  {:error, decision}
              end
          end
      end

    case reply do
      {{:ok, lease, decision}, next_state} -> {:reply, {:ok, lease, decision}, next_state}
      {:ok, lease, decision} -> {:reply, {:ok, lease, decision}, state}
      {:error, decision} -> {:reply, {:error, decision}, state}
    end
  end

  @impl true
  def handle_call({:complete, lease_id, usage}, _from, state) do
    now = now_ms()
    state = sweep(state, now)

    case Map.pop(state.leases, lease_id) do
      {nil, _leases} ->
        {:reply, {:error, :unknown_lease}, state}

      {lease, leases} ->
        actual = normalize_usage(usage, 1)

        next_state =
          state
          |> Map.put(:leases, leases)
          |> drop_idempotency(lease)
          |> release_active(lease)
          |> settle_usage(lease.tenant, now, actual)
          |> compact_state()

        reply = %{lease: lease, reserved: lease.reservation, settled: actual}
        {:reply, {:ok, reply}, next_state}
    end
  end

  @impl true
  def handle_call({:cancel, lease_id}, _from, state) do
    now = now_ms()
    state = sweep(state, now)

    case Map.pop(state.leases, lease_id) do
      {nil, _leases} ->
        {:reply, {:error, :unknown_lease}, state}

      {lease, leases} ->
        next_state =
          state
          |> Map.put(:leases, leases)
          |> drop_idempotency(lease)
          |> release_active(lease)
          |> compact_state()

        {:reply, {:ok, lease}, next_state}
    end
  end

  @impl true
  def handle_call({:heartbeat, lease_id, opts}, _from, state) do
    now = now_ms()
    state = sweep(state, now)

    case Map.get(state.leases, lease_id) do
      nil ->
        {:reply, {:error, :unknown_lease}, state}

      lease ->
        ttl_ms =
          opts
          |> Keyword.get(:ttl_ms, state.policy.default_lease_ttl_ms)
          |> validate_non_negative_integer!(:ttl_ms)

        next_expiry = min(now + min(ttl_ms, state.policy.max_lease_ttl_ms), lease.max_expires_at_ms)
        updated_lease = %{lease | last_seen_at_ms: now, expires_at_ms: next_expiry}
        next_state = put_in(state.leases[lease_id], updated_lease)
        {:reply, {:ok, updated_lease}, next_state}
    end
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    now = now_ms()
    state = sweep(state, now)
    {:reply, build_snapshot(state), state}
  end

  @impl true
  def handle_call(:prune, _from, state) do
    now = now_ms()
    state = sweep(state, now)
    {:reply, build_snapshot(state), state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    next_state =
      state
      |> sweep(now_ms())
      |> schedule_cleanup()

    {:noreply, next_state}
  end

  defp normalize_policy(opts) do
    policy = %{
      window_ms: validate_positive_integer!(Keyword.get(opts, :window_ms, @default_window_ms), :window_ms),
      cleanup_interval_ms:
        validate_positive_integer!(
          Keyword.get(opts, :cleanup_interval_ms, @default_cleanup_interval_ms),
          :cleanup_interval_ms
        ),
      default_lease_ttl_ms:
        validate_positive_integer!(
          Keyword.get(opts, :default_lease_ttl_ms, @default_lease_ttl_ms),
          :default_lease_ttl_ms
        ),
      max_lease_ttl_ms:
        validate_positive_integer!(
          Keyword.get(opts, :max_lease_ttl_ms, @default_max_lease_ttl_ms),
          :max_lease_ttl_ms
        ),
      global_concurrency:
        normalize_limit(Keyword.get(opts, :global_concurrency, @default_global_concurrency), :global_concurrency),
      default_tenant_concurrency:
        normalize_limit(
          Keyword.get(opts, :default_tenant_concurrency, @default_tenant_concurrency),
          :default_tenant_concurrency
        ),
      workload_concurrency:
        normalize_limit_map(Keyword.get(opts, :workload_concurrency, %{}), :workload_concurrency),
      global_requests_per_window:
        normalize_limit(
          Keyword.get(opts, :global_requests_per_window, :infinity),
          :global_requests_per_window
        ),
      tenant_requests_per_window:
        normalize_limit(
          Keyword.get(opts, :tenant_requests_per_window, :infinity),
          :tenant_requests_per_window
        ),
      global_input_tokens_per_window:
        normalize_limit(
          Keyword.get(opts, :global_input_tokens_per_window, :infinity),
          :global_input_tokens_per_window
        ),
      tenant_input_tokens_per_window:
        normalize_limit(
          Keyword.get(opts, :tenant_input_tokens_per_window, :infinity),
          :tenant_input_tokens_per_window
        ),
      global_output_tokens_per_window:
        normalize_limit(
          Keyword.get(opts, :global_output_tokens_per_window, :infinity),
          :global_output_tokens_per_window
        ),
      tenant_output_tokens_per_window:
        normalize_limit(
          Keyword.get(opts, :tenant_output_tokens_per_window, :infinity),
          :tenant_output_tokens_per_window
        ),
      global_cost_micros_per_window:
        normalize_limit(
          Keyword.get(opts, :global_cost_micros_per_window, :infinity),
          :global_cost_micros_per_window
        ),
      tenant_cost_micros_per_window:
        normalize_limit(
          Keyword.get(opts, :tenant_cost_micros_per_window, :infinity),
          :tenant_cost_micros_per_window
        ),
      tenant_policies: normalize_tenant_policies(Keyword.get(opts, :tenant_policies, %{}))
    }

    if policy.default_lease_ttl_ms > policy.max_lease_ttl_ms do
      raise ArgumentError,
            ":default_lease_ttl_ms cannot be greater than :max_lease_ttl_ms"
    end

    policy
  end

  defp normalize_tenant_policies(policies) when is_map(policies) do
    Enum.into(policies, %{}, fn {tenant, config} ->
      config_map = if is_list(config), do: Map.new(config), else: config

      unless is_map(config_map) do
        raise ArgumentError, ":tenant_policies values must be maps or keyword lists"
      end

      normalized = %{}
      normalized = maybe_put_limit(normalized, :concurrency, Map.get(config_map, :concurrency))
      normalized = maybe_put_limit(normalized, :requests_per_window, Map.get(config_map, :requests_per_window))
      normalized = maybe_put_limit(normalized, :input_tokens_per_window, Map.get(config_map, :input_tokens_per_window))
      normalized = maybe_put_limit(normalized, :output_tokens_per_window, Map.get(config_map, :output_tokens_per_window))
      normalized = maybe_put_limit(normalized, :cost_micros_per_window, Map.get(config_map, :cost_micros_per_window))
      {tenant, normalized}
    end)
  end

  defp normalize_tenant_policies(_), do: raise(ArgumentError, ":tenant_policies must be a map")

  defp maybe_put_limit(map, _key, nil), do: map
  defp maybe_put_limit(map, key, value), do: Map.put(map, key, normalize_limit(value, key))

  defp normalize_limit_map(map, field) when is_map(map) do
    Enum.into(map, %{}, fn {key, value} -> {key, normalize_limit(value, field)} end)
  end

  defp normalize_limit_map(_, field), do: raise(ArgumentError, "#{inspect(field)} must be a map")

  defp normalize_limit(:infinity, _field), do: :infinity
  defp normalize_limit(value, field), do: validate_positive_integer!(value, field)

  defp build_demand(nil, _opts, _policy, _now) do
    {:error, %Decision{status: :invalid, reason: :invalid_demand, detail: "tenant is required"}}
  end

  defp build_demand(tenant, opts, policy, _now) do
    try do
      ttl_ms =
        opts
        |> Keyword.get(:ttl_ms, policy.default_lease_ttl_ms)
        |> validate_positive_integer!(:ttl_ms)
        |> min(policy.max_lease_ttl_ms)

      priority =
        opts
        |> Keyword.get(:priority, 50)
        |> validate_non_negative_integer!(:priority)

      reservation =
        %Usage{
          requests: 1,
          input_tokens:
            opts
            |> Keyword.get(:estimated_input_tokens, 0)
            |> validate_non_negative_integer!(:estimated_input_tokens),
          output_tokens:
            opts
            |> Keyword.get(:estimated_output_tokens, 0)
            |> validate_non_negative_integer!(:estimated_output_tokens),
          cost_micros:
            opts
            |> Keyword.get(:estimated_cost_micros, 0)
            |> validate_non_negative_integer!(:estimated_cost_micros)
        }

      demand = %{
        tenant: tenant,
        workload: Keyword.get(opts, :workload, :default),
        priority: priority,
        ttl_ms: ttl_ms,
        idempotency_key: Keyword.get(opts, :idempotency_key),
        reservation: reservation,
        meta: normalize_meta(Keyword.get(opts, :meta, %{}))
      }

      {:ok, demand}
    rescue
      error in ArgumentError ->
        {:error, %Decision{status: :invalid, reason: :invalid_demand, detail: Exception.message(error)}}
    end
  end

  defp normalize_meta(meta) when is_map(meta), do: meta
  defp normalize_meta(meta) when is_list(meta) and Keyword.keyword?(meta), do: Map.new(meta)
  defp normalize_meta(_), do: raise(ArgumentError, ":meta must be a map or keyword list")

  defp maybe_duplicate(state, %{idempotency_key: nil}, _now), do: :miss

  defp maybe_duplicate(state, %{tenant: tenant, idempotency_key: key}, now) do
    case Map.get(state.idempotency, {tenant, key}) do
      nil ->
        :miss

      lease_id ->
        case Map.get(state.leases, lease_id) do
          nil ->
            :miss

          lease ->
            decision = %Decision{
              status: :duplicate,
              reason: :in_flight_duplicate,
              retry_after_ms: max(lease.expires_at_ms - now, 0),
              lease_id: lease.id,
              detail: "duplicate request matched an active lease"
            }

            {:duplicate, lease, decision}
        end
    end
  end

  defp check_admission(state, demand, now) do
    with :ok <- check_global_concurrency(state, demand, now),
         :ok <- check_tenant_concurrency(state, demand, now),
         :ok <- check_workload_concurrency(state, demand, now),
         :ok <- check_global_budget(state, demand, now),
         :ok <- check_tenant_budget(state, demand, now) do
      :ok
    end
  end

  defp check_global_concurrency(state, _demand, now) do
    limit = state.policy.global_concurrency

    if fits_limit?(state.active_total, 1, limit) do
      :ok
    else
      {:error,
       %Decision{
         status: :rejected,
         reason: :global_concurrency,
         scope: :global,
         metric: :concurrency,
         limit: limit,
         observed: state.active_total,
         needed: 1,
         remaining: 0,
         retry_after_ms: retry_after_for_concurrency(state, now, :global, nil)
       }}
    end
  end

  defp check_tenant_concurrency(state, demand, now) do
    policy = tenant_policy(state.policy, demand.tenant)
    current = Map.get(state.active_by_tenant, demand.tenant, 0)
    limit = policy.concurrency

    if fits_limit?(current, 1, limit) do
      :ok
    else
      {:error,
       %Decision{
         status: :rejected,
         reason: :tenant_concurrency,
         scope: :tenant,
         metric: :concurrency,
         limit: limit,
         observed: current,
         needed: 1,
         remaining: 0,
         retry_after_ms: retry_after_for_concurrency(state, now, :tenant, demand.tenant)
       }}
    end
  end

  defp check_workload_concurrency(state, demand, now) do
    limit = Map.get(state.policy.workload_concurrency, demand.workload, :infinity)
    current = Map.get(state.active_by_workload, demand.workload, 0)

    if fits_limit?(current, 1, limit) do
      :ok
    else
      {:error,
       %Decision{
         status: :rejected,
         reason: :workload_concurrency,
         scope: :workload,
         metric: :concurrency,
         limit: limit,
         observed: current,
         needed: 1,
         remaining: 0,
         retry_after_ms: retry_after_for_concurrency(state, now, :workload, demand.workload)
       }}
    end
  end

  defp check_global_budget(state, demand, now) do
    current = usage_add(state.global_reserved, state.global_window.totals)

    limits = %{
      requests: state.policy.global_requests_per_window,
      input_tokens: state.policy.global_input_tokens_per_window,
      output_tokens: state.policy.global_output_tokens_per_window,
      cost_micros: state.policy.global_cost_micros_per_window
    }

    budget_check(:global, nil, current, demand.reservation, limits, state, now)
  end

  defp check_tenant_budget(state, demand, now) do
    policy = tenant_policy(state.policy, demand.tenant)
    reserved = Map.get(state.tenant_reserved, demand.tenant, %Usage{})
    settled = Map.get(state.tenant_windows, demand.tenant, %Window{}).totals
    current = usage_add(reserved, settled)

    limits = %{
      requests: policy.requests_per_window,
      input_tokens: policy.input_tokens_per_window,
      output_tokens: policy.output_tokens_per_window,
      cost_micros: policy.cost_micros_per_window
    }

    budget_check(:tenant, demand.tenant, current, demand.reservation, limits, state, now)
  end

  defp budget_check(scope, scope_key, current, needed, limits, state, now) do
    impossible_metric =
      Enum.find(@usage_metrics, fn metric ->
        case Map.fetch!(limits, metric) do
          :infinity -> false
          limit -> usage_value(needed, metric) > limit
        end
      end)

    cond do
      impossible_metric ->
        limit = Map.fetch!(limits, impossible_metric)

        {:error,
         %Decision{
           status: :rejected,
           reason: rejection_reason(scope, impossible_metric),
           scope: scope,
           metric: impossible_metric,
           limit: limit,
           observed: usage_value(current, impossible_metric),
           needed: usage_value(needed, impossible_metric),
           remaining: max(limit - usage_value(current, impossible_metric), 0),
           retry_after_ms: nil,
           detail: "single reservation is larger than the configured limit"
         }}

      usage_fits?(current, needed, limits) ->
        :ok

      true ->
        metric = first_blocked_metric(current, needed, limits)
        limit = Map.fetch!(limits, metric)
        observed = usage_value(current, metric)

        {:error,
         %Decision{
           status: :rejected,
           reason: rejection_reason(scope, metric),
           scope: scope,
           metric: metric,
           limit: limit,
           observed: observed,
           needed: usage_value(needed, metric),
           remaining: max(limit - observed, 0),
           retry_after_ms: retry_after_for_budget(state, now, scope, scope_key, current, needed, limits)
         }}
    end
  end

  defp grant(state, demand, now) do
    lease_id = make_lease_id()
    max_expires_at_ms = now + state.policy.max_lease_ttl_ms
    expires_at_ms = min(now + demand.ttl_ms, max_expires_at_ms)

    lease = %Lease{
      id: lease_id,
      tenant: demand.tenant,
      workload: demand.workload,
      priority: demand.priority,
      idempotency_key: demand.idempotency_key,
      reservation: demand.reservation,
      issued_at_ms: now,
      expires_at_ms: expires_at_ms,
      max_expires_at_ms: max_expires_at_ms,
      last_seen_at_ms: now,
      meta: demand.meta
    }

    next_state =
      state
      |> put_lease(lease)
      |> reserve_active(lease)

    {lease, next_state}
  end

  defp put_lease(state, lease) do
    next_state = %{state | leases: Map.put(state.leases, lease.id, lease)}

    if is_nil(lease.idempotency_key) do
      next_state
    else
      %{next_state | idempotency: Map.put(next_state.idempotency, {lease.tenant, lease.idempotency_key}, lease.id)}
    end
  end

  defp reserve_active(state, lease) do
    %{
      state
      | active_total: state.active_total + 1,
        active_by_tenant: increment_counter(state.active_by_tenant, lease.tenant),
        active_by_workload: increment_counter(state.active_by_workload, lease.workload),
        global_reserved: usage_add(state.global_reserved, lease.reservation),
        tenant_reserved: update_usage_map(state.tenant_reserved, lease.tenant, lease.reservation, &usage_add/2)
    }
  end

  defp release_active(state, lease) do
    %{
      state
      | active_total: max(state.active_total - 1, 0),
        active_by_tenant: decrement_counter(state.active_by_tenant, lease.tenant),
        active_by_workload: decrement_counter(state.active_by_workload, lease.workload),
        global_reserved: usage_sub(state.global_reserved, lease.reservation),
        tenant_reserved: update_usage_map(state.tenant_reserved, lease.tenant, lease.reservation, &usage_sub/2)
    }
  end

  defp drop_idempotency(state, %{idempotency_key: nil}), do: state

  defp drop_idempotency(state, lease) do
    %{state | idempotency: Map.delete(state.idempotency, {lease.tenant, lease.idempotency_key})}
  end

  defp settle_usage(state, tenant, now, usage) do
    tenant_window = Map.get(state.tenant_windows, tenant, %Window{})

    %{
      state
      | global_window: window_add(state.global_window, now, usage),
        tenant_windows: Map.put(state.tenant_windows, tenant, window_add(tenant_window, now, usage))
    }
  end

  defp sweep(state, now) do
    state = prune_windows(state, now)

    {expired, live} = Enum.split_with(state.leases, fn {_id, lease} -> lease.expires_at_ms <= now end)
    state = %{state | leases: Map.new(live)}

    state =
      Enum.reduce(expired, state, fn {_id, lease}, acc ->
        acc
        |> drop_idempotency(lease)
        |> release_active(lease)
        |> settle_usage(lease.tenant, now, lease.reservation)
      end)

    compact_state(state)
  end

  defp prune_windows(state, now) do
    %{
      state
      | global_window: window_prune(state.global_window, now, state.policy.window_ms),
        tenant_windows:
          Enum.into(state.tenant_windows, %{}, fn {tenant, window} ->
            {tenant, window_prune(window, now, state.policy.window_ms)}
          end)
    }
  end

  defp window_add(window, at_ms, usage) do
    entry = %{at_ms: at_ms, usage: usage}
    %{window | totals: usage_add(window.totals, usage), entries: :queue.in(entry, window.entries)}
  end

  defp window_prune(window, now, window_ms) do
    cutoff = now - window_ms
    prune_window_entries(window.entries, window.totals, cutoff)
  end

  defp prune_window_entries(entries, totals, cutoff) do
    case :queue.peek(entries) do
      {:value, %{at_ms: at_ms, usage: usage}} when at_ms <= cutoff ->
        {{:value, _entry}, rest} = :queue.out(entries)
        prune_window_entries(rest, usage_sub(totals, usage), cutoff)

      _ ->
        %Window{entries: entries, totals: totals}
    end
  end

  defp retry_after_for_concurrency(state, now, :global, _scope_key) do
    min_expiry_delta(Map.values(state.leases), now)
  end

  defp retry_after_for_concurrency(state, now, :tenant, tenant) do
    leases = Enum.filter(Map.values(state.leases), &(&1.tenant == tenant))
    min_expiry_delta(leases, now)
  end

  defp retry_after_for_concurrency(state, now, :workload, workload) do
    leases = Enum.filter(Map.values(state.leases), &(&1.workload == workload))
    min_expiry_delta(leases, now)
  end

  defp retry_after_for_budget(state, now, scope, scope_key, current, needed, limits) do
    events = future_release_events(state, scope, scope_key)

    case Enum.sort_by(events, & &1.at_ms)
         |> Enum.reduce_while(current, fn event, usage ->
           next_usage = usage_sub(usage, event.usage)

           if usage_fits?(next_usage, needed, limits) do
             {:halt, max(event.at_ms - now, 0)}
           else
             {:cont, next_usage}
           end
         end) do
      wait_ms when is_integer(wait_ms) -> wait_ms
      _ -> nil
    end
  end

  defp future_release_events(state, :global, _scope_key) do
    active = Enum.map(Map.values(state.leases), &%{at_ms: &1.expires_at_ms, usage: &1.reservation})
    settled = window_release_events(state.global_window, state.policy.window_ms)
    active ++ settled
  end

  defp future_release_events(state, :tenant, tenant) do
    active =
      state.leases
      |> Map.values()
      |> Enum.filter(&(&1.tenant == tenant))
      |> Enum.map(&%{at_ms: &1.expires_at_ms, usage: &1.reservation})

    settled =
      state.tenant_windows
      |> Map.get(tenant, %Window{})
      |> window_release_events(state.policy.window_ms)

    active ++ settled
  end

  defp future_release_events(_state, :workload, _workload), do: []

  defp window_release_events(window, window_ms) do
    window.entries
    |> :queue.to_list()
    |> Enum.map(fn %{at_ms: at_ms, usage: usage} -> %{at_ms: at_ms + window_ms, usage: usage} end)
  end

  defp build_snapshot(state) do
    global_effective = usage_add(state.global_reserved, state.global_window.totals)

    tenants =
      [Map.keys(state.active_by_tenant), Map.keys(state.tenant_reserved), Map.keys(state.tenant_windows)]
      |> List.flatten()
      |> Enum.uniq()
      |> Enum.sort_by(&inspect/1)
      |> Enum.into(%{}, fn tenant ->
        policy = tenant_policy(state.policy, tenant)
        reserved = Map.get(state.tenant_reserved, tenant, %Usage{})
        settled = Map.get(state.tenant_windows, tenant, %Window{}).totals
        effective = usage_add(reserved, settled)

        {tenant,
         %{
           active: Map.get(state.active_by_tenant, tenant, 0),
           reserved: reserved,
           settled: settled,
           effective: effective,
           headroom: usage_headroom(effective, tenant_budget_limits(policy))
         }}
      end)

    %{
      policy: state.policy,
      active_total: state.active_total,
      active_by_workload: state.active_by_workload,
      global: %{
        reserved: state.global_reserved,
        settled: state.global_window.totals,
        effective: global_effective,
        headroom: usage_headroom(global_effective, global_budget_limits(state.policy))
      },
      tenants: tenants,
      leases: Enum.sort_by(Map.values(state.leases), & &1.expires_at_ms)
    }
  end

  defp tenant_policy(policy, tenant) do
    overrides = Map.get(policy.tenant_policies, tenant, %{})

    %{
      concurrency: Map.get(overrides, :concurrency, policy.default_tenant_concurrency),
      requests_per_window: Map.get(overrides, :requests_per_window, policy.tenant_requests_per_window),
      input_tokens_per_window:
        Map.get(overrides, :input_tokens_per_window, policy.tenant_input_tokens_per_window),
      output_tokens_per_window:
        Map.get(overrides, :output_tokens_per_window, policy.tenant_output_tokens_per_window),
      cost_micros_per_window: Map.get(overrides, :cost_micros_per_window, policy.tenant_cost_micros_per_window)
    }
  end

  defp global_budget_limits(policy) do
    %{
      requests: policy.global_requests_per_window,
      input_tokens: policy.global_input_tokens_per_window,
      output_tokens: policy.global_output_tokens_per_window,
      cost_micros: policy.global_cost_micros_per_window
    }
  end

  defp tenant_budget_limits(policy) do
    %{
      requests: policy.requests_per_window,
      input_tokens: policy.input_tokens_per_window,
      output_tokens: policy.output_tokens_per_window,
      cost_micros: policy.cost_micros_per_window
    }
  end

  defp usage_headroom(current, limits) do
    Enum.into(@usage_metrics, %{}, fn metric ->
      value =
        case Map.fetch!(limits, metric) do
          :infinity -> :infinity
          limit -> max(limit - usage_value(current, metric), 0)
        end

      {metric, value}
    end)
  end

  defp first_blocked_metric(current, needed, limits) do
    Enum.find(@usage_metrics, fn metric ->
      case Map.fetch!(limits, metric) do
        :infinity -> false
        limit -> usage_value(current, metric) + usage_value(needed, metric) > limit
      end
    end)
  end

  defp rejection_reason(:global, :requests), do: :global_requests_budget
  defp rejection_reason(:global, :input_tokens), do: :global_input_budget
  defp rejection_reason(:global, :output_tokens), do: :global_output_budget
  defp rejection_reason(:global, :cost_micros), do: :global_cost_budget
  defp rejection_reason(:tenant, :requests), do: :tenant_requests_budget
  defp rejection_reason(:tenant, :input_tokens), do: :tenant_input_budget
  defp rejection_reason(:tenant, :output_tokens), do: :tenant_output_budget
  defp rejection_reason(:tenant, :cost_micros), do: :tenant_cost_budget
  defp rejection_reason(:workload, metric), do: {:workload_budget, metric}

  defp usage_fits?(current, needed, limits) do
    Enum.all?(@usage_metrics, fn metric ->
      case Map.fetch!(limits, metric) do
        :infinity -> true
        limit -> usage_value(current, metric) + usage_value(needed, metric) <= limit
      end
    end)
  end

  defp fits_limit?(_current, _needed, :infinity), do: true
  defp fits_limit?(current, needed, limit), do: current + needed <= limit

  defp normalize_usage(usage, requests_default) do
    source = if is_list(usage), do: Map.new(usage), else: usage

    unless is_map(source) do
      raise ArgumentError, "usage must be a map or keyword list"
    end

    %Usage{
      requests:
        source
        |> Map.get(:requests, requests_default)
        |> validate_non_negative_integer!(:requests),
      input_tokens:
        source
        |> Map.get(:input_tokens, 0)
        |> validate_non_negative_integer!(:input_tokens),
      output_tokens:
        source
        |> Map.get(:output_tokens, 0)
        |> validate_non_negative_integer!(:output_tokens),
      cost_micros:
        source
        |> Map.get(:cost_micros, 0)
        |> validate_non_negative_integer!(:cost_micros)
    }
  end

  defp usage_add(left, right) do
    %Usage{
      requests: left.requests + right.requests,
      input_tokens: left.input_tokens + right.input_tokens,
      output_tokens: left.output_tokens + right.output_tokens,
      cost_micros: left.cost_micros + right.cost_micros
    }
  end

  defp usage_sub(left, right) do
    %Usage{
      requests: max(left.requests - right.requests, 0),
      input_tokens: max(left.input_tokens - right.input_tokens, 0),
      output_tokens: max(left.output_tokens - right.output_tokens, 0),
      cost_micros: max(left.cost_micros - right.cost_micros, 0)
    }
  end

  defp usage_value(usage, metric), do: Map.fetch!(usage, metric)

  defp zero_usage?(usage) do
    Enum.all?(@usage_metrics, fn metric -> usage_value(usage, metric) == 0 end)
  end

  defp update_usage_map(map, key, delta, updater) do
    current = Map.get(map, key, %Usage{})
    next = updater.(current, delta)

    if zero_usage?(next) do
      Map.delete(map, key)
    else
      Map.put(map, key, next)
    end
  end

  defp increment_counter(map, key), do: Map.update(map, key, 1, &(&1 + 1))

  defp decrement_counter(map, key) do
    case Map.get(map, key, 0) - 1 do
      count when count > 0 -> Map.put(map, key, count)
      _ -> Map.delete(map, key)
    end
  end

  defp min_expiry_delta([], _now), do: nil

  defp min_expiry_delta(leases, now) do
    leases
    |> Enum.map(&max(&1.expires_at_ms - now, 0))
    |> Enum.min(fn -> nil end)
  end

  defp compact_state(state) do
    tenant_windows =
      Enum.into(state.tenant_windows, %{}, fn {tenant, window} ->
        if zero_usage?(window.totals) and :queue.is_empty(window.entries) do
          nil
        else
          {tenant, window}
        end
      end)
      |> Enum.reject(&is_nil/1)
      |> Map.new()

    %{
      state
      | tenant_windows: tenant_windows,
        cleanup_timer: state.cleanup_timer
    }
  end

  defp schedule_cleanup(state) do
    if state.cleanup_timer do
      Process.cancel_timer(state.cleanup_timer)
    end

    timer = Process.send_after(self(), :cleanup, state.policy.cleanup_interval_ms)
    %{state | cleanup_timer: timer}
  end

  defp make_lease_id do
    18
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp lease_id(%Lease{id: id}), do: id
  defp lease_id(id) when is_binary(id), do: id

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp keyword_or_map_get(value, key, default) when is_list(value), do: Keyword.get(value, key, default)
  defp keyword_or_map_get(value, key, default) when is_map(value), do: Map.get(value, key, default)
  defp keyword_or_map_get(_value, _key, default), do: default

  defp validate_positive_integer!(value, field) when is_integer(value) and value > 0, do: value

  defp validate_positive_integer!(value, field) do
    raise ArgumentError, "#{inspect(field)} must be a positive integer, got: #{inspect(value)}"
  end

  defp validate_non_negative_integer!(value, field) when is_integer(value) and value >= 0, do: value

  defp validate_non_negative_integer!(value, field) do
    raise ArgumentError, "#{inspect(field)} must be a non-negative integer, got: #{inspect(value)}"
  end
end

# This solves multi-tenant LLM admission control for Elixir, Phoenix, Oban, Broadway, and OTP services where OpenAI, Anthropic, Gemini, or internal model calls can get expensive or fail under burst traffic. It prevents duplicate retries, noisy-neighbor overload, and budget drift by turning every model request into a lease with reserved concurrency, reserved token budget, and reserved spend before the call starts.
#
# Built because the real failure mode in April 2026 is usually not "the SDK call failed". The real problem is that background jobs, web requests, and retry loops all hit the provider at the same time, then one tenant burns the entire model budget while another tenant gets throttled. I wrote this in a plain OTP style so it can sit in a production supervision tree, survive worker crashes safely, and keep the numbers conservative when callers disappear.
#
# Use it when you need Elixir AI rate limiting, LLM budget enforcement, inference concurrency control, request deduplication with idempotency keys, rolling token windows, or cost guardrails in a Phoenix AI gateway. It is especially useful for chat apps, agent backends, batch summarization workers, streaming inference pipelines, evaluation harnesses, and internal developer tooling where retries are common and spend needs to stay predictable.
#
# The trick: reservations are held while work is active, then converted into settled window usage on completion or timeout. That means slow providers and worker crashes cannot trick the ledger into admitting more work than the system can really afford. Heartbeats can extend active work, but only up to a hard stop, so long-running streams do not pin the scheduler forever.
#
# Drop this into an Elixir app that already owns the actual provider call path, start it under a supervisor, call `admit/3` before the upstream request, call `complete/4` with real usage after the response, and call `cancel/3` when the request never reached the model. If you are searching for Elixir LLM admission control, Phoenix AI spend limits, OpenAI quota protection, Anthropic concurrency control, or OTP-based model request dedupe, this file is meant to be a practical starting point you can fork and wire into production.