defmodule TenantTokenFairScheduler do
  @moduledoc """
  A Deficit Round Robin (DRR) fair scheduler for LLM token budgets shared
  across many tenants behind a single rate-limited upstream (one org-level
  OpenAI/Anthropic/vLLM key, one GPU pool, one egress quota — anything with
  a hard tokens-per-minute or requests-per-minute ceiling that several
  internal teams or customers draw from concurrently).

  ## The problem this targets

  Most internal AI gateways enforce fairness with either a plain per-tenant
  token bucket (which wastes capacity when one tenant is idle and another
  is starved) or a naive round-robin queue (which ignores that requests
  have wildly different token costs, so "one request per tenant per turn"
  still lets a tenant with huge prompts crowd out a tenant with small ones).

  DRR fixes exactly this: each tenant accrues a "deficit" every scheduling
  round proportional to its weight, and can only dequeue a request whose
  cost fits inside its accumulated deficit. Idle tenants don't hoard
  capacity — an empty queue resets its deficit to zero — and busy tenants
  never get starved because every active tenant gets its quantum every
  round, in order.

  ## Usage

      children = [
        {TenantTokenFairScheduler, name: MyApp.Scheduler, budget_per_tick: 6_000,
         budget_cap: 20_000, tick_interval_ms: 250}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

      TenantTokenFairScheduler.register_tenant(MyApp.Scheduler, "team-checkout",
        quantum: 2_000, priority: :interactive, max_queue: 64)

      case TenantTokenFairScheduler.enqueue(MyApp.Scheduler, "team-checkout", 1_800) do
        {:ok, ticket} ->
          receive do
            {:scheduler_granted, "team-checkout", ^ticket, _meta} ->
              {:ok, resp} = call_upstream_llm(prompt)
              TenantTokenFairScheduler.settle(MyApp.Scheduler, "team-checkout", ticket, 1_800, resp.usage.total_tokens)
            {:scheduler_expired, "team-checkout", ^ticket} ->
              {:error, :timed_out_in_queue}
          after
            5_000 -> {:error, :scheduler_unresponsive}
          end

        {:error, :queue_full} ->
          {:error, :shed_load}
      end

  Enqueue never blocks past admission control: it either accepts the
  request into the tenant's queue immediately or rejects it immediately
  with `{:error, :queue_full}` so the caller can shed load instead of
  piling up latency. Whether the request actually runs is a *second*,
  asynchronous decision delivered as a message once the scheduler's
  ticking round grants it real budget.
  """

  use GenServer
  require Logger

  @type tenant_id :: term()
  @type ticket :: reference()
  @type priority :: :interactive | :batch

  @default_quantum 1_000
  @default_max_queue 100
  @default_ttl_ms 15_000
  @priority_multiplier %{interactive: 2, batch: 1}

  defmodule Request do
    @moduledoc false
    defstruct [:ticket, :from_pid, :cost, :enqueued_at, :ttl_ms]
  end

  defmodule Tenant do
    @moduledoc false
    defstruct quantum: 1_000,
              priority: :batch,
              max_queue: 100,
              deficit: 0,
              depth: 0,
              queue: :queue.new(),
              granted: 0,
              dropped: 0,
              expired: 0
  end

  defstruct tenants: %{},
            order: [],
            rr_index: 0,
            budget: 0,
            budget_cap: 10_000,
            budget_per_tick: 1_000,
            tick_interval_ms: 200,
            default_ttl_ms: @default_ttl_ms,
            next_ticket_debug: 0,
            telemetry: nil

  # ------------------------------------------------------------------
  # Public API
  # ------------------------------------------------------------------

  @doc """
  Starts the scheduler. Options:

    * `:name` - registered process name (defaults to the module).
    * `:budget_cap` - maximum burst capacity of the shared token bucket.
    * `:budget_per_tick` - tokens added to the shared bucket every tick.
    * `:tick_interval_ms` - how often a DRR scheduling round runs.
    * `:default_ttl_ms` - how long a queued request may wait before it
      is dropped and the caller notified with `:scheduler_expired`.
    * `:telemetry` - optional `fun(event, measurements, metadata)` callback
      invoked on grant/expire/drop for observability, without requiring
      the `:telemetry` dependency.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

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
  Registers a tenant (idempotent — re-registering resets its config but
  keeps its live queue and deficit intact).
  """
  @spec register_tenant(GenServer.server(), tenant_id(), keyword()) :: :ok
  def register_tenant(server \\ __MODULE__, tenant_id, opts \\ []) do
    GenServer.call(server, {:register_tenant, tenant_id, opts})
  end

  @doc """
  Admits a request into a tenant's queue. Returns `{:ok, ticket}` when
  accepted, or `{:error, :queue_full}` when the tenant's queue is already
  at capacity — a fast, synchronous backpressure signal. Actual execution
  permission arrives later as a `{:scheduler_granted, tenant_id, ticket,
  meta}` or `{:scheduler_expired, tenant_id, ticket}` message sent to the
  calling process.
  """
  @spec enqueue(GenServer.server(), tenant_id(), pos_integer(), keyword()) ::
          {:ok, ticket()} | {:error, :queue_full | :unknown_tenant}
  def enqueue(server \\ __MODULE__, tenant_id, estimated_cost, opts \\ [])
      when is_integer(estimated_cost) and estimated_cost > 0 do
    GenServer.call(server, {:enqueue, tenant_id, estimated_cost, opts})
  end

  @doc """
  True-up the shared budget after the real cost of a granted request is
  known. LLM output length is unknowable at admission time, so the
  scheduler charges the shared bucket the *estimated* cost at grant time
  and this call corrects the drift once the real usage is known. Costing
  a request only once, at estimate time, would let systematic
  underestimation quietly starve the bucket without ever showing up in
  the DRR accounting.
  """
  @spec settle(GenServer.server(), tenant_id(), ticket(), pos_integer(), non_neg_integer()) :: :ok
  def settle(server \\ __MODULE__, tenant_id, ticket, estimated_cost, actual_cost) do
    GenServer.cast(server, {:settle, tenant_id, ticket, estimated_cost, actual_cost})
  end

  @doc """
  Returns a snapshot of scheduler state useful for dashboards and
  load-shedding decisions upstream of `enqueue/4`.
  """
  @spec stats(GenServer.server()) :: map()
  def stats(server \\ __MODULE__) do
    GenServer.call(server, :stats)
  end

  # ------------------------------------------------------------------
  # GenServer callbacks
  # ------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %__MODULE__{
      budget_cap: Keyword.get(opts, :budget_cap, 10_000),
      budget_per_tick: Keyword.get(opts, :budget_per_tick, 1_000),
      tick_interval_ms: Keyword.get(opts, :tick_interval_ms, 200),
      default_ttl_ms: Keyword.get(opts, :default_ttl_ms, @default_ttl_ms),
      telemetry: Keyword.get(opts, :telemetry, fn _event, _meas, _meta -> :ok end)
    }

    state = %{state | budget: state.budget_per_tick}
    schedule_tick(state.tick_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:register_tenant, tenant_id, opts}, _from, state) do
    tenant = %Tenant{
      quantum: Keyword.get(opts, :quantum, @default_quantum),
      priority: Keyword.get(opts, :priority, :batch),
      max_queue: Keyword.get(opts, :max_queue, @default_max_queue)
    }

    existing = Map.get(state.tenants, tenant_id)

    merged =
      if existing do
        %Tenant{tenant | queue: existing.queue, depth: existing.depth, deficit: existing.deficit}
      else
        tenant
      end

    order = if Map.has_key?(state.tenants, tenant_id), do: state.order, else: state.order ++ [tenant_id]
    tenants = Map.put(state.tenants, tenant_id, merged)
    {:reply, :ok, %{state | tenants: tenants, order: order}}
  end

  def handle_call({:enqueue, tenant_id, cost, opts}, {from_pid, _}, state) do
    case Map.fetch(state.tenants, tenant_id) do
      :error ->
        {:reply, {:error, :unknown_tenant}, state}

      {:ok, tenant} when tenant.depth >= tenant.max_queue ->
        tenant = %{tenant | dropped: tenant.dropped + 1}
        emit(state, :scheduler_queue_full, %{cost: cost}, %{tenant: tenant_id})
        {:reply, {:error, :queue_full}, put_tenant(state, tenant_id, tenant)}

      {:ok, tenant} ->
        ticket = make_ref()
        ttl_ms = Keyword.get(opts, :ttl_ms, state.default_ttl_ms)

        request = %Request{
          ticket: ticket,
          from_pid: from_pid,
          cost: cost,
          enqueued_at: System.monotonic_time(:millisecond),
          ttl_ms: ttl_ms
        }

        tenant = %{tenant | queue: :queue.in(request, tenant.queue), depth: tenant.depth + 1}
        {:reply, {:ok, ticket}, put_tenant(state, tenant_id, tenant)}
    end
  end

  def handle_call(:stats, _from, state) do
    tenant_stats =
      Map.new(state.tenants, fn {id, t} ->
        {id,
         %{
           depth: t.depth,
           deficit: t.deficit,
           priority: t.priority,
           granted: t.granted,
           dropped: t.dropped,
           expired: t.expired
         }}
      end)

    {:reply, %{budget: state.budget, budget_cap: state.budget_cap, tenants: tenant_stats}, state}
  end

  @impl true
  def handle_cast({:settle, tenant_id, _ticket, estimated_cost, actual_cost}, state) do
    delta = actual_cost - estimated_cost
    new_budget = clamp(state.budget - delta, state.budget_cap)
    emit(state, :scheduler_settled, %{delta: delta}, %{tenant: tenant_id})
    {:noreply, %{state | budget: new_budget}}
  end

  @impl true
  def handle_info(:tick, state) do
    state = %{state | budget: clamp(state.budget + state.budget_per_tick, state.budget_cap)}
    state = drop_expired(state)
    state = run_drr_round(state)
    schedule_tick(state.tick_interval_ms)
    {:noreply, state}
  end

  # ------------------------------------------------------------------
  # DRR scheduling core
  # ------------------------------------------------------------------

  # One pass over the round-robin order, resuming from where the previous
  # tick left off so no tenant is systematically favored by always being
  # first in line when the shared budget runs out mid-round.
  defp run_drr_round(%{order: []} = state), do: state

  defp run_drr_round(state) do
    count = length(state.order)
    do_round(state, count, 0)
  end

  defp do_round(state, count, visited) when visited >= count or state.budget <= 0 do
    state
  end

  defp do_round(state, count, visited) do
    tenant_id = Enum.at(state.order, rem(state.rr_index, count))
    tenant = Map.fetch!(state.tenants, tenant_id)

    multiplier = Map.get(@priority_multiplier, tenant.priority, 1)
    tenant = %{tenant | deficit: tenant.deficit + tenant.quantum * multiplier}

    {tenant, budget} = drain_tenant(tenant, state.budget, tenant_id, state)

    tenant =
      if :queue.is_empty(tenant.queue) do
        %{tenant | deficit: 0}
      else
        tenant
      end

    state = put_tenant(%{state | budget: budget}, tenant_id, tenant)
    state = %{state | rr_index: rem(state.rr_index + 1, count)}
    do_round(state, count, visited + 1)
  end

  # Dequeues as many head-of-line requests as fit inside both the
  # tenant's deficit and the shared budget, in order, stopping as soon as
  # either is insufficient for the next request (strict head-of-line —
  # no reordering within a tenant's own queue).
  defp drain_tenant(tenant, budget, tenant_id, state) do
    case :queue.peek(tenant.queue) do
      :empty ->
        {tenant, budget}

      {:value, %Request{cost: cost}} when cost > tenant.deficit or cost > budget ->
        {tenant, budget}

      {:value, %Request{cost: cost} = req} ->
        {{:value, ^req}, rest} = :queue.out(tenant.queue)
        send(req.from_pid, {:scheduler_granted, tenant_id, req.ticket, %{estimated_cost: cost}})
        emit(state, :scheduler_granted, %{cost: cost}, %{tenant: tenant_id})

        tenant = %{
          tenant
          | queue: rest,
            depth: tenant.depth - 1,
            deficit: tenant.deficit - cost,
            granted: tenant.granted + 1
        }

        drain_tenant(tenant, budget - cost, tenant_id, state)
    end
  end

  defp drop_expired(state) do
    now = System.monotonic_time(:millisecond)

    tenants =
      Map.new(state.tenants, fn {tenant_id, tenant} ->
        {kept, expired_reqs} = split_expired(tenant.queue, now, [])

        Enum.each(expired_reqs, fn req ->
          send(req.from_pid, {:scheduler_expired, tenant_id, req.ticket})
          emit(state, :scheduler_expired, %{waited_ms: now - req.enqueued_at}, %{tenant: tenant_id})
        end)

        dropped_count = length(expired_reqs)

        {tenant_id,
         %{
           tenant
           | queue: kept,
             depth: tenant.depth - dropped_count,
             expired: tenant.expired + dropped_count
         }}
      end)

    %{state | tenants: tenants}
  end

  # Requests are FIFO and TTLs are assigned at enqueue time, so expired
  # requests are always a prefix of the queue — no need to scan past the
  # first still-fresh entry.
  defp split_expired(queue, now, expired_acc) do
    case :queue.peek(queue) do
      {:value, %Request{enqueued_at: enq, ttl_ms: ttl} = req} when now - enq >= ttl ->
        {_, rest} = :queue.out(queue)
        split_expired(rest, now, [req | expired_acc])

      _ ->
        {queue, Enum.reverse(expired_acc)}
    end
  end

  defp put_tenant(state, tenant_id, tenant) do
    %{state | tenants: Map.put(state.tenants, tenant_id, tenant)}
  end

  defp clamp(value, cap), do: value |> min(cap)

  defp schedule_tick(interval_ms), do: Process.send_after(self(), :tick, interval_ms)

  defp emit(state, event, measurements, metadata) do
    try do
      state.telemetry.(event, measurements, metadata)
    rescue
      error -> Logger.warning("TenantTokenFairScheduler telemetry callback raised: #{inspect(error)}")
    end

    :ok
  end
end

# ---------------------------------------------------------------------
# What this actually is, in plain words
# ---------------------------------------------------------------------
#
# This solves the fairness problem every team hits the moment more than
# one internal service shares a single rate-limited LLM key or GPU pool.
# You get one org-level quota from OpenAI, Anthropic, or your own vLLM
# cluster, and five different teams' services all call through it. Give
# each tenant its own plain token bucket and you either waste capacity
# when a tenant is quiet, or you let one chatty tenant with huge prompts
# starve everyone else because a naive round-robin queue treats a
# 50-token request and a 50,000-token request as the same "turn."
#
# Built because I kept seeing internal AI gateways solve this with
# either nothing (first-come-first-served, so the noisiest team wins) or
# a fixed per-tenant rate limit (so idle capacity gets wasted instead of
# handed to whoever actually needs it right now). Deficit Round Robin is
# a decades-old fair-queuing algorithm from network routers, built for
# exactly this shape of problem — variable-sized packets sharing a fixed
# link — and it maps onto variable-sized LLM requests sharing a fixed
# token-per-minute budget almost without modification.
#
# Use it when you're running an internal LLM gateway, an agent platform
# serving multiple teams or customers off one upstream key, or any queue
# where "cost per item" varies a lot and you need fairness that doesn't
# waste idle capacity or let big requests starve small ones.
#
# The trick: every tenant gets a growing "deficit" each scheduling round,
# and can only dequeue a request that costs less than its current
# deficit. Busy tenants build up deficit and eventually drain big
# requests; idle tenants have their deficit reset to zero the moment
# their queue empties, so they can never bank unused capacity and hoard
# it. On top of the classic algorithm, this version adds two things DRR
# papers don't have to worry about: admission control that rejects
# instead of blocking when a tenant's queue is full (so callers can shed
# load instead of piling up latency), and a settle/5 call that corrects
# the shared budget after the fact, because with LLMs you don't actually
# know how many tokens a request cost until the response has already
# streamed back — you can only charge an estimate up front.
#
# Drop this into any Elixir supervision tree as a single GenServer child
# in front of whatever HTTP client actually calls your LLM provider.
# Register each internal caller as a tenant with a weight and a priority
# class, enqueue before every upstream call, wait for the grant message,
# then settle the real cost once you have it. No external dependencies,
# no database, just OTP.
