defmodule ToolCallIdempotency do
  @moduledoc false
  use GenServer

  @default_completed_ttl_ms 300_000
  @default_failure_ttl_ms 0
  @default_lease_ttl_ms 120_000
  @default_waiter_limit 256
  @default_max_completed 10_000
  @default_sweep_interval_ms 30_000

  defmodule Lease do
    @enforce_keys [:id, :key, :digest, :owner, :started_at_ms, :deadline_ms]
    defstruct [:id, :key, :digest, :owner, :started_at_ms, :deadline_ms]

    @type t :: %__MODULE__{
            id: reference(),
            key: binary(),
            digest: binary(),
            owner: pid(),
            started_at_ms: non_neg_integer(),
            deadline_ms: non_neg_integer()
          }
  end

  defmodule Stats do
    defstruct [
      :active_leases,
      :waiting_callers,
      :cached_results,
      :completed_ttl_ms,
      :failure_ttl_ms,
      :lease_ttl_ms,
      :waiter_limit,
      :max_completed
    ]

    @type t :: %__MODULE__{
            active_leases: non_neg_integer(),
            waiting_callers: non_neg_integer(),
            cached_results: non_neg_integer(),
            completed_ttl_ms: pos_integer(),
            failure_ttl_ms: non_neg_integer(),
            lease_ttl_ms: pos_integer(),
            waiter_limit: pos_integer(),
            max_completed: pos_integer() | :infinity
          }
  end

  @type server :: GenServer.server()
  @type claim_result ::
          {:execute, Lease.t()}
          | {:cached, term()}
          | {:failed, term()}
          | {:busy, map()}
          | {:error, term()}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
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

  def run(key, payload, fun, opts \\ []) when is_function(fun, 0) do
    run(__MODULE__, key, payload, fun, opts)
  end

  def run(server, key, payload, fun, opts) when is_function(fun, 0) do
    case claim(server, key, payload, timeout: Keyword.get(opts, :claim_timeout, 5_000)) do
      {:execute, lease} ->
        execute_lease(server, lease, fun, opts)

      {:cached, value} ->
        {:ok, value}

      {:failed, reason} ->
        {:error, reason}

      {:busy, _meta} ->
        await(server, key, payload, Keyword.get(opts, :wait_timeout, :infinity))

      {:error, reason} ->
        {:error, reason}
    end
  end

  def claim(key, payload \\ nil, opts \\ []) do
    claim(__MODULE__, key, payload, opts)
  end

  def claim(server, key, payload, opts) do
    normalized_key = normalize_key(key)
    digest = digest_payload(payload)
    GenServer.call(server, {:claim, normalized_key, digest, self()}, Keyword.get(opts, :timeout, 5_000))
  end

  def await(key, payload \\ nil, timeout \\ :infinity) do
    await(__MODULE__, key, payload, timeout)
  end

  def await(server, key, payload, timeout) do
    normalized_key = normalize_key(key)
    digest = digest_payload(payload)
    GenServer.call(server, {:await, normalized_key, digest}, timeout)
  end

  def peek(key, payload \\ nil, opts \\ []) do
    peek(__MODULE__, key, payload, opts)
  end

  def peek(server, key, payload, opts) do
    normalized_key = normalize_key(key)
    digest = digest_payload(payload)
    GenServer.call(server, {:peek, normalized_key, digest}, Keyword.get(opts, :timeout, 5_000))
  end

  def finish(%Lease{} = lease, value, opts \\ []) do
    finish(__MODULE__, lease, value, opts)
  end

  def finish(server, %Lease{} = lease, value, opts) do
    GenServer.call(server, {:finish, lease, value, Keyword.get(opts, :ttl_ms)}, Keyword.get(opts, :timeout, 5_000))
  end

  def fail(%Lease{} = lease, reason, opts \\ []) do
    fail(__MODULE__, lease, reason, opts)
  end

  def fail(server, %Lease{} = lease, reason, opts) do
    GenServer.call(server, {:fail, lease, reason, Keyword.get(opts, :ttl_ms)}, Keyword.get(opts, :timeout, 5_000))
  end

  def release(%Lease{} = lease, reason \\ :released, opts \\ []) do
    release(__MODULE__, lease, reason, opts)
  end

  def release(server, %Lease{} = lease, reason, opts) do
    GenServer.call(server, {:release, lease, reason}, Keyword.get(opts, :timeout, 5_000))
  end

  def heartbeat(%Lease{} = lease, opts \\ []) do
    heartbeat(__MODULE__, lease, opts)
  end

  def heartbeat(server, %Lease{} = lease, opts) do
    GenServer.call(server, {:heartbeat, lease, Keyword.get(opts, :ttl_ms)}, Keyword.get(opts, :timeout, 5_000))
  end

  def stats(opts \\ []) do
    stats(__MODULE__, opts)
  end

  def stats(server, opts) do
    GenServer.call(server, :stats, Keyword.get(opts, :timeout, 5_000))
  end

  def purge(opts \\ []) do
    purge(__MODULE__, opts)
  end

  def purge(server, opts) do
    GenServer.call(server, :purge, Keyword.get(opts, :timeout, 5_000))
  end

  @impl true
  def init(opts) do
    state = %{
      table: :ets.new(__MODULE__, [:set, :protected, read_concurrency: true, write_concurrency: true]),
      completed_ttl_ms:
        positive_integer(Keyword.get(opts, :completed_ttl_ms, @default_completed_ttl_ms), :completed_ttl_ms),
      failure_ttl_ms:
        non_negative_integer(Keyword.get(opts, :failure_ttl_ms, @default_failure_ttl_ms), :failure_ttl_ms),
      lease_ttl_ms: positive_integer(Keyword.get(opts, :lease_ttl_ms, @default_lease_ttl_ms), :lease_ttl_ms),
      waiter_limit: positive_integer(Keyword.get(opts, :waiter_limit, @default_waiter_limit), :waiter_limit),
      max_completed: normalize_max_completed(Keyword.get(opts, :max_completed, @default_max_completed)),
      sweep_interval_ms:
        positive_integer(Keyword.get(opts, :sweep_interval_ms, @default_sweep_interval_ms), :sweep_interval_ms),
      next_seq: 1,
      order: :queue.new(),
      leases: %{},
      monitors: %{}
    }

    schedule_sweep(state.sweep_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:claim, key, digest, owner}, _from, state) do
    now = now_ms()
    state = sweep_expired_leases(state, now, [key])

    case cache_reply(state, key, digest, now) do
      {:reply, reply, state} ->
        {:reply, reply, state}

      {:miss, state} ->
        case Map.get(state.leases, key) do
          nil ->
            {lease, state} = open_lease(state, key, digest, owner, now)
            {:reply, {:execute, lease}, state}

          %{lease: lease} ->
            if lease.digest == digest do
              {:reply, {:busy, lease_snapshot(lease, now)}, state}
            else
              {:reply, {:error, {:payload_conflict, key}}, state}
            end
        end
    end
  end

  def handle_call({:await, key, digest}, from, state) do
    now = now_ms()
    state = sweep_expired_leases(state, now, [key])

    case cache_reply(state, key, digest, now) do
      {:reply, {:cached, value}, state} ->
        {:reply, {:ok, value}, state}

      {:reply, {:failed, reason}, state} ->
        {:reply, {:error, reason}, state}

      {:reply, {:error, reason}, state} ->
        {:reply, {:error, reason}, state}

      {:miss, state} ->
        case Map.get(state.leases, key) do
          nil ->
            {:reply, {:error, :not_found}, state}

          %{lease: lease, waiter_count: waiter_count} = holder ->
            cond do
              lease.digest != digest ->
                {:reply, {:error, {:payload_conflict, key}}, state}

              waiter_count >= state.waiter_limit ->
                {:reply, {:error, {:too_many_waiters, state.waiter_limit}}, state}

              true ->
                updated_holder = %{
                  holder
                  | waiters: [from | holder.waiters],
                    waiter_count: holder.waiter_count + 1
                }

                {:noreply, put_lease_holder(state, key, updated_holder)}
            end
        end
    end
  end

  def handle_call({:peek, key, digest}, _from, state) do
    now = now_ms()
    state = sweep_expired_leases(state, now, [key])

    case cache_reply(state, key, digest, now) do
      {:reply, reply, state} ->
        {:reply, reply, state}

      {:miss, state} ->
        case Map.get(state.leases, key) do
          nil -> {:reply, :miss, state}
          %{lease: lease} -> {:reply, {:busy, lease_snapshot(lease, now)}, state}
        end
    end
  end

  def handle_call({:finish, %Lease{} = lease, value, ttl_override}, {caller, _}, state) do
    now = now_ms()
    state = sweep_expired_leases(state, now, [lease.key])

    case fetch_owned_lease(state, lease, caller) do
      {:ok, holder} ->
        ttl_ms = ttl_override || state.completed_ttl_ms
        {holder, state} = pop_lease(state, lease.key)
        state = put_cache(state, lease.key, lease.digest, :ok, value, ttl_ms, now)
        notify_waiters(holder.waiters, {:ok, value})
        {:reply, :ok, post_cache_maintenance(state, now)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:fail, %Lease{} = lease, reason, ttl_override}, {caller, _}, state) do
    now = now_ms()
    state = sweep_expired_leases(state, now, [lease.key])

    case fetch_owned_lease(state, lease, caller) do
      {:ok, holder} ->
        failure_ttl_ms =
          case ttl_override do
            nil -> state.failure_ttl_ms
            value -> non_negative_integer(value, :failure_ttl_ms)
          end

        {holder, state} = pop_lease(state, lease.key)

        state =
          if failure_ttl_ms > 0 do
            put_cache(state, lease.key, lease.digest, :error, reason, failure_ttl_ms, now)
          else
            state
          end

        notify_waiters(holder.waiters, {:error, reason})
        {:reply, :ok, post_cache_maintenance(state, now)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:release, %Lease{} = lease, reason}, {caller, _}, state) do
    now = now_ms()
    state = sweep_expired_leases(state, now, [lease.key])

    case fetch_owned_lease(state, lease, caller) do
      {:ok, holder} ->
        {holder, state} = pop_lease(state, lease.key)
        notify_waiters(holder.waiters, {:error, reason})
        {:reply, :ok, state}

      {:error, release_reason} ->
        {:reply, {:error, release_reason}, state}
    end
  end

  def handle_call({:heartbeat, %Lease{} = lease, ttl_override}, {caller, _}, state) do
    now = now_ms()
    state = sweep_expired_leases(state, now, [lease.key])

    ttl_ms =
      case ttl_override do
        nil -> state.lease_ttl_ms
        value -> positive_integer(value, :lease_ttl_ms)
      end

    case fetch_owned_lease(state, lease, caller) do
      {:ok, holder} ->
        renewed = %{holder.lease | deadline_ms: now + ttl_ms}
        updated_holder = %{holder | lease: renewed}
        {:reply, {:ok, renewed}, put_lease_holder(state, lease.key, updated_holder)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:stats, _from, state) do
    waiting_callers = Enum.reduce(state.leases, 0, fn {_key, holder}, acc -> acc + holder.waiter_count end)

    {:reply,
     %Stats{
       active_leases: map_size(state.leases),
       waiting_callers: waiting_callers,
       cached_results: cache_size(state),
       completed_ttl_ms: state.completed_ttl_ms,
       failure_ttl_ms: state.failure_ttl_ms,
       lease_ttl_ms: state.lease_ttl_ms,
       waiter_limit: state.waiter_limit,
       max_completed: state.max_completed
     }, state}
  end

  def handle_call(:purge, _from, state) do
    now = now_ms()
    {:reply, :ok, perform_sweep(state, now)}
  end

  @impl true
  def handle_info(:sweep, state) do
    now = now_ms()
    schedule_sweep(state.sweep_interval_ms)
    {:noreply, perform_sweep(state, now)}
  end

  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor_ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {key, monitors} ->
        case Map.pop(state.leases, key) do
          {nil, leases} ->
            {:noreply, %{state | leases: leases, monitors: monitors}}

          {%{waiters: waiters}, leases} ->
            notify_waiters(waiters, {:error, {:owner_down, reason}})
            {:noreply, %{state | leases: leases, monitors: monitors}}
        end
    end
  end

  defp execute_lease(server, %Lease{} = lease, fun, opts) do
    success_ttl = Keyword.get(opts, :ttl_ms)
    failure_ttl = Keyword.get(opts, :failure_ttl_ms)
    finish_timeout = Keyword.get(opts, :finish_timeout, 5_000)

    try do
      case fun.() do
        {:ok, value} ->
          case finish(server, lease, value, ttl_ms: success_ttl, timeout: finish_timeout) do
            :ok -> {:ok, value}
            {:error, reason} -> {:error, {:finish_failed, reason}}
          end

        {:error, reason} ->
          _ = fail(server, lease, reason, ttl_ms: failure_ttl, timeout: finish_timeout)
          {:error, reason}

        value ->
          case finish(server, lease, value, ttl_ms: success_ttl, timeout: finish_timeout) do
            :ok -> {:ok, value}
            {:error, reason} -> {:error, {:finish_failed, reason}}
          end
      end
    rescue
      exception ->
        stacktrace = __STACKTRACE__
        formatted = Exception.format(:error, exception, stacktrace)
        _ = fail(server, lease, {:exception, formatted}, ttl_ms: failure_ttl, timeout: finish_timeout)
        reraise exception, stacktrace
    catch
      kind, reason ->
        stacktrace = __STACKTRACE__
        formatted = Exception.format(kind, reason, stacktrace)
        _ = fail(server, lease, {kind, formatted}, ttl_ms: failure_ttl, timeout: finish_timeout)
        :erlang.raise(kind, reason, stacktrace)
    end
  end

  defp cache_reply(state, key, digest, now) do
    case :ets.lookup(state.table, key) do
      [] ->
        {:miss, state}

      [{^key, _kind, _stored_digest, _value, expires_at_ms, _seq}] when expires_at_ms <= now ->
        :ets.delete(state.table, key)
        {:miss, state}

      [{^key, _kind, stored_digest, _value, _expires_at_ms, _seq}] when stored_digest != digest ->
        {:reply, {:error, {:payload_conflict, key}}, state}

      [{^key, :ok, _stored_digest, value, _expires_at_ms, _seq}] ->
        {:reply, {:cached, value}, state}

      [{^key, :error, _stored_digest, reason, _expires_at_ms, _seq}] ->
        {:reply, {:failed, reason}, state}
    end
  end

  defp open_lease(state, key, digest, owner, now) do
    lease = %Lease{
      id: make_ref(),
      key: key,
      digest: digest,
      owner: owner,
      started_at_ms: now,
      deadline_ms: now + state.lease_ttl_ms
    }

    monitor_ref = Process.monitor(owner)

    holder = %{
      lease: lease,
      owner_monitor: monitor_ref,
      waiters: [],
      waiter_count: 0
    }

    {lease,
     %{
       state
       | leases: Map.put(state.leases, key, holder),
         monitors: Map.put(state.monitors, monitor_ref, key)
     }}
  end

  defp fetch_owned_lease(state, %Lease{} = lease, caller) do
    case Map.get(state.leases, lease.key) do
      nil ->
        {:error, :stale_lease}

      %{lease: %{id: active_id, owner: active_owner}} = holder
      when active_id == lease.id and active_owner == caller ->
        {:ok, holder}

      %{lease: %{id: active_id}} when active_id != lease.id ->
        {:error, :stale_lease}

      _holder ->
        {:error, :not_owner}
    end
  end

  defp pop_lease(state, key) do
    case Map.pop(state.leases, key) do
      {nil, leases} ->
        {nil, %{state | leases: leases}}

      {%{owner_monitor: monitor_ref} = holder, leases} ->
        Process.demonitor(monitor_ref, [:flush])
        {holder, %{state | leases: leases, monitors: Map.delete(state.monitors, monitor_ref)}}
    end
  end

  defp put_lease_holder(state, key, holder) do
    %{state | leases: Map.put(state.leases, key, holder)}
  end

  defp put_cache(state, key, digest, kind, value, ttl_ms, now) do
    validated_ttl_ms = positive_integer(ttl_ms, :ttl_ms)
    seq = state.next_seq
    expires_at_ms = now + validated_ttl_ms
    :ets.insert(state.table, {key, kind, digest, value, expires_at_ms, seq})

    %{
      state
      | next_seq: seq + 1,
        order: :queue.in({seq, key}, state.order)
    }
  end

  defp perform_sweep(state, now) do
    state
    |> sweep_expired_leases(now)
    |> purge_expired_cache(now)
    |> trim_cache()
    |> maybe_compact_order()
  end

  defp post_cache_maintenance(state, now) do
    state
    |> purge_expired_cache(now)
    |> trim_cache()
    |> maybe_compact_order()
  end

  defp sweep_expired_leases(state, now, only_keys \\ nil) do
    keys =
      case only_keys do
        nil -> Map.keys(state.leases)
        provided -> provided
      end

    Enum.reduce(keys, state, fn key, acc ->
      case Map.get(acc.leases, key) do
        %{lease: %{deadline_ms: deadline_ms}, waiters: waiters} when deadline_ms <= now ->
          {holder, acc} = pop_lease(acc, key)
          notify_waiters((holder && holder.waiters) || waiters, {:error, :lease_expired})
          acc

        _ ->
          acc
      end
    end)
  end

  defp purge_expired_cache(state, now) do
    expired_keys =
      :ets.foldl(
        fn {key, _kind, _digest, _value, expires_at_ms, _seq}, acc ->
          if expires_at_ms <= now, do: [key | acc], else: acc
        end,
        [],
        state.table
      )

    Enum.each(expired_keys, &:ets.delete(state.table, &1))
    state
  end

  defp trim_cache(%{max_completed: :infinity} = state), do: state

  defp trim_cache(state) do
    do_trim_cache(state, state.max_completed)
  end

  defp do_trim_cache(state, limit) do
    if cache_size(state) <= limit do
      state
    else
      case :queue.out(state.order) do
        {:empty, _queue} ->
          state

        {{:value, {seq, key}}, next_queue} ->
          state = %{state | order: next_queue}

          case :ets.lookup(state.table, key) do
            [{^key, _kind, _digest, _value, _expires_at_ms, current_seq}] when current_seq == seq ->
              :ets.delete(state.table, key)

            _ ->
              :ok
          end

          do_trim_cache(state, limit)
      end
    end
  end

  defp maybe_compact_order(state) do
    cache_entries = cache_size(state)
    queue_size = :queue.len(state.order)
    threshold = max(cache_entries * 4, numeric_limit(state.max_completed) * 2)

    cond do
      cache_entries == 0 and queue_size > 0 ->
        %{state | order: :queue.new()}

      queue_size <= threshold ->
        state

      true ->
        rebuilt_order =
          :ets.foldl(
            fn {key, _kind, _digest, _value, _expires_at_ms, seq}, acc ->
              [{seq, key} | acc]
            end,
            [],
            state.table
          )
          |> Enum.sort_by(fn {seq, _key} -> seq end)
          |> :queue.from_list()

        %{state | order: rebuilt_order}
    end
  end

  defp cache_size(state) do
    case :ets.info(state.table, :size) do
      :undefined -> 0
      size -> size
    end
  end

  defp notify_waiters(waiters, reply) do
    Enum.each(waiters, &GenServer.reply(&1, reply))
  end

  defp lease_snapshot(%Lease{} = lease, now) do
    %{
      key: lease.key,
      lease_id: lease.id,
      owner: lease.owner,
      started_at_ms: lease.started_at_ms,
      deadline_ms: lease.deadline_ms,
      remaining_ms: max(lease.deadline_ms - now, 0)
    }
  end

  defp schedule_sweep(interval_ms) do
    Process.send_after(self(), :sweep, interval_ms)
  end

  defp now_ms do
    System.monotonic_time(:millisecond)
  end

  defp normalize_key(key) when is_binary(key) do
    if byte_size(key) == 0 do
      raise ArgumentError, "idempotency key cannot be empty"
    end

    key
  end

  defp normalize_key(key) when is_list(key) do
    key
    |> IO.iodata_to_binary()
    |> normalize_key()
  end

  defp normalize_key(_key) do
    raise ArgumentError, "idempotency key must be a binary or iodata"
  end

  defp digest_payload(payload) do
    encoded_payload = :erlang.term_to_binary(payload)
    Base.encode16(:crypto.hash(:sha256, encoded_payload), case: :lower)
  end

  defp positive_integer(value, _name) when is_integer(value) and value > 0, do: value

  defp positive_integer(value, name) do
    raise ArgumentError, "#{name} must be a positive integer, got: #{inspect(value)}"
  end

  defp non_negative_integer(value, _name) when is_integer(value) and value >= 0, do: value

  defp non_negative_integer(value, name) do
    raise ArgumentError, "#{name} must be a non-negative integer, got: #{inspect(value)}"
  end

  defp normalize_max_completed(:infinity), do: :infinity
  defp normalize_max_completed(value) when is_integer(value) and value > 0, do: value

  defp normalize_max_completed(value) do
    raise ArgumentError, "max_completed must be a positive integer or :infinity, got: #{inspect(value)}"
  end

  defp numeric_limit(:infinity), do: @default_max_completed
  defp numeric_limit(value), do: value
end

# This solves duplicate LLM tool calls, webhook retries, and background job replays in Elixir systems that talk to AI providers or execute expensive side effects. Built because in April 2026 it is still common for Phoenix apps, Oban workers, browser reconnects, and model retry loops to trigger the same action twice and create double emails, double tickets, or duplicate deploys. Use it when you need one process to win a key, everyone else to wait, and later callers to reuse a short lived success or failure instead of running the work again. The trick: a single GenServer owns the lease and waiter queue, while ETS keeps recent outcomes fast enough for hot paths. Drop this into any Elixir or Phoenix service that needs idempotency keys, LLM tool call deduplication, webhook dedupe, retry safe job execution, or API side effect protection.
