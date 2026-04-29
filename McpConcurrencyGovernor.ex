defmodule McpConcurrencyGovernor do
  @moduledoc """
  A small OTP governor for MCP and agent tool concurrency.

  This module solves a common 2026 production problem: a few noisy sessions can open too
  many parallel tool calls, starve other users, overflow provider quotas, and create huge
  backlogs around browsers, shells, search, eval runners, or vector calls.

  The governor provides:

    * per-tool concurrency limits
    * bounded waiting queues
    * round-robin fairness across sessions
    * queue expiration and stuck-run expiration
    * queue cancellation
    * optional notify and telemetry hooks
  """

  use GenServer

  require Logger

  @message_tag :mcp_concurrency_governor

  @default_limit 4
  @default_queue_limit 128
  @default_queue_ttl_ms 30_000
  @default_run_ttl_ms 120_000
  @default_max_per_session 1
  @default_sweep_interval_ms 1_000
  @default_telemetry_prefix [:mcp_concurrency_governor]

  defmodule Lease do
    @enforce_keys [
      :request_id,
      :grant_token,
      :tool,
      :session_id,
      :owner,
      :granted_at_unix_ms,
      :run_ttl_ms,
      :expires_in_ms
    ]
    defstruct [
      :request_id,
      :grant_token,
      :tool,
      :session_id,
      :owner,
      :granted_at_unix_ms,
      :run_ttl_ms,
      :expires_in_ms,
      :queue_wait_ms,
      meta: %{}
    ]
  end

  defmodule Receipt do
    @enforce_keys [:request_id, :tool, :session_id, :owner, :queued_at_unix_ms, :expires_in_ms]
    defstruct [:request_id, :tool, :session_id, :owner, :queued_at_unix_ms, :expires_in_ms, meta: %{}]
  end

  defmodule ToolConfig do
    @enforce_keys [:name, :limit, :queue_limit, :queue_ttl_ms, :run_ttl_ms, :max_per_session]
    defstruct [:name, :limit, :queue_limit, :queue_ttl_ms, :run_ttl_ms, :max_per_session]
  end

  defmodule State do
    @enforce_keys [:tools, :default_tool_config, :auto_create_tools?, :sweep_interval_ms, :notify, :telemetry_prefix]
    defstruct [
      :tools,
      :default_tool_config,
      :auto_create_tools?,
      :sweep_interval_ms,
      :notify,
      :telemetry_prefix,
      sweep_timer: nil
    ]
  end

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def child_spec(opts) do
    name = Keyword.get(opts, :name, __MODULE__)

    %{
      id: name,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  def request(server \\ __MODULE__, session_id, tool, opts \\ []) do
    GenServer.call(server, {:request, session_id, tool, opts}, :infinity)
  end

  def await(receipt_or_request_id, timeout_ms \\ :infinity)

  def await(%Receipt{request_id: request_id}, timeout_ms), do: await(request_id, timeout_ms)

  def await(request_id, :infinity) when is_binary(request_id) do
    receive do
      {@message_tag, :granted, ^request_id, %Lease{} = lease} -> {:ok, lease}
      {@message_tag, :rejected, ^request_id, reason} -> {:error, reason}
      {@message_tag, :expired, ^request_id, reason} -> {:error, reason}
    end
  end

  def await(request_id, timeout_ms) when is_binary(request_id) and is_integer(timeout_ms) and timeout_ms >= 0 do
    receive do
      {@message_tag, :granted, ^request_id, %Lease{} = lease} -> {:ok, lease}
      {@message_tag, :rejected, ^request_id, reason} -> {:error, reason}
      {@message_tag, :expired, ^request_id, reason} -> {:error, reason}
    after
      timeout_ms -> {:error, :timeout}
    end
  end

  def run(server \\ __MODULE__, session_id, tool, fun, opts \\ []) when is_function(fun, 0) do
    await_timeout_ms = Keyword.get(opts, :await_timeout_ms, :infinity)
    request_opts = Keyword.drop(opts, [:await_timeout_ms, :release_reason])

    case request(server, session_id, tool, request_opts) do
      {:ok, lease} ->
        execute_and_release(server, lease, fun, opts)

      {:queued, receipt} ->
        case await(receipt, await_timeout_ms) do
          {:ok, lease} ->
            execute_and_release(server, lease, fun, opts)

          {:error, :timeout} ->
            _ = cancel(server, receipt, reason: :caller_timeout)
            {:error, :timeout}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def release(server \\ __MODULE__, %Lease{} = lease, opts \\ []) do
    GenServer.call(server, {:release, lease, opts}, :infinity)
  end

  def cancel(server \\ __MODULE__, %Receipt{request_id: request_id}, opts), do: cancel(server, request_id, opts)
  def cancel(server, request_id, opts) when is_binary(request_id), do: GenServer.call(server, {:cancel, request_id, opts}, :infinity)

  def stats(server \\ __MODULE__) do
    GenServer.call(server, :stats, :infinity)
  end

  def list(server \\ __MODULE__, scope \\ :all) do
    GenServer.call(server, {:list, scope}, :infinity)
  end

  def sweep(server \\ __MODULE__) do
    GenServer.call(server, :sweep, :infinity)
  end

  @impl true
  def init(opts) do
    default_tool_config =
      Keyword.get(opts, :default_tool_options, [])
      |> normalize_tool_config("__default__", nil)

    state = %State{
      tools: normalize_tools(Keyword.get(opts, :tools, %{}), default_tool_config),
      default_tool_config: default_tool_config,
      auto_create_tools?: boolean!(Keyword.get(opts, :auto_create_tools?, true), :auto_create_tools?),
      sweep_interval_ms:
        positive_integer!(Keyword.get(opts, :sweep_interval_ms, @default_sweep_interval_ms), :sweep_interval_ms),
      notify: normalize_notify(Keyword.get(opts, :notify)),
      telemetry_prefix: normalize_telemetry_prefix(Keyword.get(opts, :telemetry_prefix, @default_telemetry_prefix))
    }

    {:ok, schedule_sweep(state)}
  end

  @impl true
  def handle_call({:request, raw_session_id, raw_tool, opts}, from, state) do
    now_mono = monotonic_ms()
    now_unix = unix_ms()
    session_id = normalize_session_id(raw_session_id)
    tool = normalize_tool_name(raw_tool)

    with {:ok, state} <- reconcile_tool(state, tool, now_mono, now_unix) do
      tool_state = Map.fetch!(state.tools, tool)
      request = build_request(opts, from, session_id, tool, tool_state.config, now_mono, now_unix)

      cond do
        grant_immediately?(tool_state, session_id) ->
          {next_tool_state, lease, events} = grant_now(tool_state, request, now_mono, now_unix)
          next_state = put_tool_state(state, tool, next_tool_state)
          emit(next_state, :granted, %{wait_ms: 0}, %{tool: lease.tool, session_id: lease.session_id, request_id: lease.request_id})
          emit_events(next_state, events)
          {:reply, {:ok, lease}, next_state}

        tool_state.queued_count >= tool_state.config.queue_limit ->
          next_state = update_tool_state(state, tool, fn ts -> bump_counter(ts, :rejected) end)
          emit_events(next_state, [reject_event(request, :queue_full)])
          {:reply, {:error, :queue_full}, next_state}

        true ->
          queued_tool_state = enqueue_request(tool_state, request)
          {scheduled_tool_state, events} = schedule_grants(queued_tool_state, now_mono, now_unix)
          next_state = put_tool_state(state, tool, scheduled_tool_state)
          emit_events(next_state, events)
          {:reply, {:queued, receipt_from_request(request, now_mono)}, next_state}
      end
    else
      {:error, :unknown_tool} -> {:reply, {:error, :unknown_tool}, state}
    end
  end

  def handle_call({:release, %Lease{} = lease, opts}, _from, state) do
    now_mono = monotonic_ms()
    now_unix = unix_ms()
    tool = normalize_tool_name(lease.tool)

    with {:ok, state} <- reconcile_tool(state, tool, now_mono, now_unix) do
      tool_state = Map.fetch!(state.tools, tool)

      case Map.get(tool_state.inflight, lease.request_id) do
        nil ->
          {:reply, {:error, :not_found}, state}

        inflight when inflight.grant_token != lease.grant_token ->
          {:reply, {:error, :stale_lease}, state}

        inflight ->
          next_tool_state =
            tool_state
            |> drop_inflight(inflight)
            |> bump_counter(:released)

          {scheduled_tool_state, events} = schedule_grants(next_tool_state, now_mono, now_unix)
          next_state = put_tool_state(state, tool, scheduled_tool_state)
          runtime_ms = now_mono - inflight.granted_at_mono_ms
          emit_events(next_state, [release_event(inflight, runtime_ms, Keyword.get(opts, :reason)) | events])
          {:reply, :ok, next_state}
      end
    else
      {:error, :unknown_tool} -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:cancel, request_id, opts}, _from, state) do
    now_mono = monotonic_ms()
    now_unix = unix_ms()
    reason = Keyword.get(opts, :reason, :cancelled)

    {reply, next_state, events} =
      Enum.reduce_while(Map.keys(state.tools), {{:error, :not_found}, state, []}, fn tool, {_reply, acc_state, _events} ->
        case reconcile_tool(acc_state, tool, now_mono, now_unix) do
          {:error, :unknown_tool} ->
            {:cont, {{:error, :not_found}, acc_state, []}}

          {:ok, refreshed_state} ->
            tool_state = Map.fetch!(refreshed_state.tools, tool)

            case remove_queued_request(tool_state, request_id) do
              {:ok, next_tool_state, request} ->
                next_state = put_tool_state(refreshed_state, tool, bump_counter(next_tool_state, :cancelled))
                {:halt, {:ok, next_state, [reject_event(request, reason)]}}

              :not_found ->
                if Map.has_key?(tool_state.inflight, request_id) do
                  {:halt, {{:error, :not_queued}, refreshed_state, []}}
                else
                  {:cont, {{:error, :not_found}, refreshed_state, []}}
                end
            end
        end
      end)

    emit_events(next_state, events)
    {:reply, reply, next_state}
  end

  def handle_call(:stats, _from, state) do
    now_mono = monotonic_ms()
    now_unix = unix_ms()
    next_state = reconcile_all(state, now_mono, now_unix)
    {:reply, build_stats(next_state, now_mono), next_state}
  end

  def handle_call({:list, scope}, _from, state) do
    now_mono = monotonic_ms()
    now_unix = unix_ms()
    next_state = reconcile_all(state, now_mono, now_unix)
    {:reply, build_list(next_state, scope, now_mono), next_state}
  end

  def handle_call(:sweep, _from, state) do
    now_mono = monotonic_ms()
    now_unix = unix_ms()
    next_state = reconcile_all(state, now_mono, now_unix)
    {:reply, build_stats(next_state, now_mono), next_state}
  end

  @impl true
  def handle_info(:sweep, state) do
    now_mono = monotonic_ms()
    now_unix = unix_ms()
    next_state = state |> reconcile_all(now_mono, now_unix) |> schedule_sweep()
    {:noreply, next_state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.sweep_timer, do: Process.cancel_timer(state.sweep_timer)
    :ok
  end

  defp execute_and_release(server, lease, fun, opts) do
    try do
      result = fun.()
      _ = release(server, lease, reason: Keyword.get(opts, :release_reason, :ok))
      {:ok, result}
    rescue
      exception ->
        stacktrace = __STACKTRACE__
        _ = release(server, lease, reason: {:exception, exception})
        reraise exception, stacktrace
    catch
      kind, reason ->
        _ = release(server, lease, reason: {kind, reason})
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp reconcile_all(state, now_mono, now_unix) do
    Enum.reduce(Map.keys(state.tools), state, fn tool, acc ->
      case reconcile_tool(acc, tool, now_mono, now_unix) do
        {:ok, next_state} -> next_state
        {:error, :unknown_tool} -> acc
      end
    end)
  end

  defp reconcile_tool(state, tool, now_mono, now_unix) do
    with {:ok, state} <- ensure_tool_state(state, tool) do
      tool_state = Map.fetch!(state.tools, tool)
      {swept_tool_state, events} = sweep_tool(tool_state, now_mono, now_unix)
      next_state = put_tool_state(state, tool, swept_tool_state)
      emit_events(next_state, events)
      {:ok, next_state}
    end
  end

  defp ensure_tool_state(state, tool) do
    cond do
      Map.has_key?(state.tools, tool) ->
        {:ok, state}

      state.auto_create_tools? ->
        config = %{state.default_tool_config | name: tool}
        {:ok, put_tool_state(state, tool, blank_tool_state(config))}

      true ->
        {:error, :unknown_tool}
    end
  end

  defp sweep_tool(tool_state, now_mono, now_unix) do
    {tool_state, events1} = expire_queued(tool_state, now_mono)
    {tool_state, events2} = expire_inflight(tool_state, now_mono)
    {tool_state, events3} = schedule_grants(tool_state, now_mono, now_unix)
    {tool_state, events1 ++ events2 ++ events3}
  end

  defp expire_queued(tool_state, now_mono) do
    base = %{tool_state | queues: %{}, order: :queue.new(), queued_count: 0}

    {next_tool_state, events} =
      Enum.reduce(tool_state.queues, {base, []}, fn {session_id, queue}, {acc, acc_events} ->
        {kept_queue, kept_count, new_events} =
          queue
          |> :queue.to_list()
          |> Enum.reduce({:queue.new(), 0, []}, fn request, {q, count, events} ->
            if now_mono >= request.expires_at_mono_ms do
              {q, count, [reject_event(request, :queue_timeout) | events]}
            else
              {:queue.in(request, q), count + 1, events}
            end
          end)

        acc =
          if kept_count == 0 do
            acc
          else
            acc
            |> put_queue(session_id, kept_queue)
            |> push_session(session_id)
            |> inc_queued(kept_count)
          end

        {acc, Enum.reverse(new_events) ++ acc_events}
      end)

    {bump_counter(next_tool_state, :expired, length(events)), Enum.reverse(events)}
  end

  defp expire_inflight(tool_state, now_mono) do
    Enum.reduce(Map.values(tool_state.inflight), {tool_state, []}, fn inflight, {acc, events} ->
      if now_mono >= inflight.expires_at_mono_ms do
        next_acc = acc |> drop_inflight(inflight) |> bump_counter(:expired)
        {next_acc, [expire_event(inflight, :run_timeout) | events]}
      else
        {acc, events}
      end
    end)
    |> then(fn {next_tool_state, events} -> {next_tool_state, Enum.reverse(events)} end)
  end

  defp schedule_grants(tool_state, now_mono, now_unix) do
    do_schedule_grants(tool_state, now_mono, now_unix, [])
  end

  defp do_schedule_grants(tool_state, now_mono, now_unix, events) do
    if map_size(tool_state.inflight) < tool_state.config.limit do
      case pop_next_eligible(tool_state) do
        {:ok, request, next_tool_state} ->
          {granted_tool_state, lease, new_events} = grant_now(next_tool_state, request, now_mono, now_unix)
          do_schedule_grants(granted_tool_state, now_mono, now_unix, [grant_event(lease, request.notify_pid) | new_events ++ events])

        :none ->
          {tool_state, Enum.reverse(events)}
      end
    else
      {tool_state, Enum.reverse(events)}
    end
  end

  defp grant_now(tool_state, request, now_mono, now_unix) do
    inflight = %{
      request_id: request.request_id,
      grant_token: new_id(),
      tool: request.tool,
      session_id: request.session_id,
      owner: request.owner,
      notify_pid: request.notify_pid,
      meta: request.meta,
      queue_wait_ms: now_mono - request.enqueued_at_mono_ms,
      run_ttl_ms: request.run_ttl_ms,
      granted_at_unix_ms: now_unix,
      granted_at_mono_ms: now_mono,
      expires_at_mono_ms: now_mono + request.run_ttl_ms
    }

    next_tool_state =
      tool_state
      |> put_inflight(inflight)

    {next_tool_state, lease_from_inflight(inflight, now_mono), []}
  end

  defp pop_next_eligible(tool_state), do: do_pop_next_eligible(tool_state, :queue.len(tool_state.order))
  defp do_pop_next_eligible(_tool_state, 0), do: :none

  defp do_pop_next_eligible(tool_state, attempts_left) do
    case :queue.out(tool_state.order) do
      {:empty, _} ->
        :none

      {{:value, session_id}, rest_order} ->
        tool_state = %{tool_state | order: rest_order}
        current_inflight = Map.get(tool_state.inflight_by_session, session_id, 0)

        case Map.get(tool_state.queues, session_id) do
          nil ->
            do_pop_next_eligible(tool_state, attempts_left - 1)

          queue when session_at_limit?(tool_state.config, current_inflight) ->
            do_pop_next_eligible(push_session(tool_state, session_id), attempts_left - 1)

          queue ->
            case :queue.out(queue) do
              {:empty, _} ->
                do_pop_next_eligible(%{tool_state | queues: Map.delete(tool_state.queues, session_id)}, attempts_left - 1)

              {{:value, request}, rest_queue} ->
                next_tool_state =
                  if :queue.is_empty(rest_queue) do
                    %{tool_state | queues: Map.delete(tool_state.queues, session_id)}
                  else
                    tool_state
                    |> put_queue(session_id, rest_queue)
                    |> push_session(session_id)
                  end
                  |> dec_queued()

                {:ok, request, next_tool_state}
            end
        end
    end
  end

  defp enqueue_request(tool_state, request) do
    queue = Map.get(tool_state.queues, request.session_id, :queue.new())
    first_for_session? = :queue.is_empty(queue)

    tool_state =
      tool_state
      |> put_queue(request.session_id, :queue.in(request, queue))
      |> inc_queued(1)
      |> bump_counter(:queued)

    if first_for_session?, do: push_session(tool_state, request.session_id), else: tool_state
  end

  defp remove_queued_request(tool_state, request_id) do
    Enum.reduce_while(tool_state.queues, :not_found, fn {session_id, queue}, _acc ->
      case Enum.split_with(:queue.to_list(queue), fn request -> request.request_id != request_id end) do
        {_prefix, []} ->
          {:cont, :not_found}

        {prefix, [request | suffix]} ->
          rebuilt = Enum.reduce(prefix ++ suffix, :queue.new(), fn item, acc -> :queue.in(item, acc) end)

          next_tool_state =
            if :queue.is_empty(rebuilt) do
              tool_state
              |> remove_session(session_id)
              |> then(&%{&1 | queues: Map.delete(&1.queues, session_id)})
            else
              put_queue(tool_state, session_id, rebuilt)
            end
            |> dec_queued()

          {:halt, {:ok, next_tool_state, request}}
      end
    end)
  end

  defp grant_immediately?(tool_state, session_id) do
    tool_state.queued_count == 0 and
      map_size(tool_state.inflight) < tool_state.config.limit and
      not session_at_limit?(tool_state.config, Map.get(tool_state.inflight_by_session, session_id, 0))
  end

  defp session_at_limit?(%ToolConfig{max_per_session: :infinity}, _count), do: false
  defp session_at_limit?(%ToolConfig{max_per_session: limit}, count), do: count >= limit

  defp put_inflight(tool_state, inflight) do
    %{
      tool_state
      | inflight: Map.put(tool_state.inflight, inflight.request_id, inflight),
        inflight_by_session: Map.update(tool_state.inflight_by_session, inflight.session_id, 1, &(&1 + 1)),
        counters: Map.update!(tool_state.counters, :granted, &(&1 + 1))
    }
  end

  defp drop_inflight(tool_state, inflight) do
    next_counts =
      case Map.fetch!(tool_state.inflight_by_session, inflight.session_id) do
        1 -> Map.delete(tool_state.inflight_by_session, inflight.session_id)
        count -> Map.put(tool_state.inflight_by_session, inflight.session_id, count - 1)
      end

    %{tool_state | inflight: Map.delete(tool_state.inflight, inflight.request_id), inflight_by_session: next_counts}
  end

  defp build_stats(state, now_mono) do
    state.tools
    |> Enum.map(fn {_tool, tool_state} ->
      ages =
        tool_state.queues
        |> Enum.flat_map(fn {_session_id, queue} -> :queue.to_list(queue) end)
        |> Enum.map(fn request -> now_mono - request.enqueued_at_mono_ms end)

      %{
        tool: tool_state.config.name,
        limit: tool_state.config.limit,
        queue_limit: tool_state.config.queue_limit,
        max_per_session: tool_state.config.max_per_session,
        inflight_count: map_size(tool_state.inflight),
        queued_count: tool_state.queued_count,
        sessions_waiting: map_size(tool_state.queues),
        oldest_queue_age_ms: if(ages == [], do: 0, else: Enum.max(ages)),
        counters: tool_state.counters
      }
    end)
    |> Enum.sort_by(& &1.tool)
  end

  defp build_list(state, :queued, now_mono), do: %{queued: queued_rows(state, now_mono)}
  defp build_list(state, :inflight, now_mono), do: %{inflight: inflight_rows(state, now_mono)}

  defp build_list(state, :all, now_mono) do
    %{queued: queued_rows(state, now_mono), inflight: inflight_rows(state, now_mono)}
  end

  defp queued_rows(state, now_mono) do
    state.tools
    |> Enum.flat_map(fn {_tool, tool_state} ->
      tool_state.queues
      |> Enum.flat_map(fn {_session_id, queue} ->
        Enum.map(:queue.to_list(queue), fn request ->
          %{
            request_id: request.request_id,
            tool: request.tool,
            session_id: request.session_id,
            owner: request.owner,
            queued_at_unix_ms: request.enqueued_at_unix_ms,
            expires_in_ms: max(request.expires_at_mono_ms - now_mono, 0),
            meta: request.meta
          }
        end)
      end)
    end)
    |> Enum.sort_by(&{&1.tool, &1.queued_at_unix_ms, &1.request_id})
  end

  defp inflight_rows(state, now_mono) do
    state.tools
    |> Enum.flat_map(fn {_tool, tool_state} ->
      tool_state.inflight
      |> Map.values()
      |> Enum.map(fn inflight ->
        %{
          request_id: inflight.request_id,
          tool: inflight.tool,
          session_id: inflight.session_id,
          owner: inflight.owner,
          granted_at_unix_ms: inflight.granted_at_unix_ms,
          expires_in_ms: max(inflight.expires_at_mono_ms - now_mono, 0),
          queue_wait_ms: inflight.queue_wait_ms,
          meta: inflight.meta
        }
      end)
    end)
    |> Enum.sort_by(&{&1.tool, &1.granted_at_unix_ms, &1.request_id})
  end

  defp receipt_from_request(request, now_mono) do
    %Receipt{
      request_id: request.request_id,
      tool: request.tool,
      session_id: request.session_id,
      owner: request.owner,
      queued_at_unix_ms: request.enqueued_at_unix_ms,
      expires_in_ms: max(request.expires_at_mono_ms - now_mono, 0),
      meta: request.meta
    }
  end

  defp lease_from_inflight(inflight, now_mono) do
    %Lease{
      request_id: inflight.request_id,
      grant_token: inflight.grant_token,
      tool: inflight.tool,
      session_id: inflight.session_id,
      owner: inflight.owner,
      granted_at_unix_ms: inflight.granted_at_unix_ms,
      run_ttl_ms: inflight.run_ttl_ms,
      expires_in_ms: max(inflight.expires_at_mono_ms - now_mono, 0),
      queue_wait_ms: inflight.queue_wait_ms,
      meta: inflight.meta
    }
  end

  defp build_request(opts, from, session_id, tool, config, now_mono, now_unix) do
    opts = normalize_opts(opts)

    %{
      request_id: new_id(),
      tool: tool,
      session_id: session_id,
      owner: normalize_owner(Map.get(opts, :owner), from),
      notify_pid: normalize_notify_pid(Map.get(opts, :notify_pid), from),
      meta: normalize_meta(Map.get(opts, :meta, %{})),
      run_ttl_ms: positive_integer!(Map.get(opts, :run_ttl_ms, config.run_ttl_ms), :run_ttl_ms),
      enqueued_at_unix_ms: now_unix,
      enqueued_at_mono_ms: now_mono,
      expires_at_mono_ms: now_mono + positive_integer!(Map.get(opts, :queue_ttl_ms, config.queue_ttl_ms), :queue_ttl_ms)
    }
  end

  defp blank_tool_state(config) do
    %{
      config: config,
      inflight: %{},
      inflight_by_session: %{},
      queues: %{},
      order: :queue.new(),
      queued_count: 0,
      counters: %{granted: 0, queued: 0, rejected: 0, cancelled: 0, expired: 0, released: 0}
    }
  end

  defp normalize_tools(specs, default_tool_config) when is_map(specs) do
    specs
    |> Enum.map(fn {tool, opts} ->
      tool_name = normalize_tool_name(tool)
      {tool_name, blank_tool_state(normalize_tool_config(opts, tool_name, default_tool_config))}
    end)
    |> Map.new()
  end

  defp normalize_tools(specs, default_tool_config) when is_list(specs) do
    specs |> Enum.into(%{}) |> normalize_tools(default_tool_config)
  end

  defp normalize_tool_config(opts, tool_name, nil) do
    normalize_tool_config(opts, tool_name, %ToolConfig{
      name: tool_name,
      limit: @default_limit,
      queue_limit: @default_queue_limit,
      queue_ttl_ms: @default_queue_ttl_ms,
      run_ttl_ms: @default_run_ttl_ms,
      max_per_session: @default_max_per_session
    })
  end

  defp normalize_tool_config(opts, tool_name, %ToolConfig{} = base) do
    opts = normalize_opts(opts)

    %ToolConfig{
      name: tool_name,
      limit: positive_integer!(Map.get(opts, :limit, base.limit), :limit),
      queue_limit: non_negative_integer!(Map.get(opts, :queue_limit, base.queue_limit), :queue_limit),
      queue_ttl_ms: positive_integer!(Map.get(opts, :queue_ttl_ms, base.queue_ttl_ms), :queue_ttl_ms),
      run_ttl_ms: positive_integer!(Map.get(opts, :run_ttl_ms, base.run_ttl_ms), :run_ttl_ms),
      max_per_session: positive_integer_or_infinity!(Map.get(opts, :max_per_session, base.max_per_session), :max_per_session)
    }
  end

  defp grant_event(lease, notify_pid), do: {:granted, lease, notify_pid}
  defp reject_event(request, reason), do: {:rejected, request.request_id, request.notify_pid, reason}
  defp expire_event(inflight, reason), do: {:expired, inflight.request_id, inflight.notify_pid, reason}
  defp release_event(inflight, runtime_ms, reason), do: {:released, inflight.request_id, runtime_ms, reason, inflight.tool, inflight.session_id}

  defp emit_events(state, events) do
    Enum.each(events, fn
      {:granted, %Lease{} = lease, notify_pid} ->
        safe_send(notify_pid, {@message_tag, :granted, lease.request_id, lease})
        emit(state, :granted, %{wait_ms: lease.queue_wait_ms}, %{tool: lease.tool, session_id: lease.session_id, request_id: lease.request_id})

      {:rejected, request_id, notify_pid, reason} ->
        safe_send(notify_pid, {@message_tag, :rejected, request_id, reason})
        emit(state, :rejected, %{}, %{request_id: request_id, reason: reason})

      {:expired, request_id, notify_pid, reason} ->
        safe_send(notify_pid, {@message_tag, :expired, request_id, reason})
        emit(state, :expired, %{}, %{request_id: request_id, reason: reason})

      {:released, request_id, runtime_ms, reason, tool, session_id} ->
        emit(state, :released, %{runtime_ms: runtime_ms}, %{request_id: request_id, reason: reason, tool: tool, session_id: session_id})
    end)
  end

  defp put_tool_state(state, tool, tool_state), do: %{state | tools: Map.put(state.tools, tool, tool_state)}
  defp update_tool_state(state, tool, fun), do: put_tool_state(state, tool, fun.(Map.fetch!(state.tools, tool)))
  defp put_queue(tool_state, session_id, queue), do: %{tool_state | queues: Map.put(tool_state.queues, session_id, queue)}
  defp push_session(tool_state, session_id), do: %{tool_state | order: :queue.in(session_id, tool_state.order)}
  defp remove_session(tool_state, session_id), do: %{tool_state | order: rebuild_order(tool_state.order, session_id)}
  defp inc_queued(tool_state, amount), do: %{tool_state | queued_count: tool_state.queued_count + amount}
  defp dec_queued(tool_state), do: %{tool_state | queued_count: max(tool_state.queued_count - 1, 0)}

  defp bump_counter(tool_state, key, amount \\ 1) do
    %{tool_state | counters: Map.update!(tool_state.counters, key, &(&1 + amount))}
  end

  defp rebuild_order(order, session_id) do
    order
    |> :queue.to_list()
    |> Enum.reject(&(&1 == session_id))
    |> Enum.reduce(:queue.new(), fn item, acc -> :queue.in(item, acc) end)
  end

  defp normalize_opts(opts) when is_map(opts), do: Map.new(opts)
  defp normalize_opts(opts) when is_list(opts), do: Map.new(opts)
  defp normalize_opts(other), do: raise(ArgumentError, "expected options as map or keyword list, got: #{inspect(other)}")

  defp normalize_tool_name(tool) when is_binary(tool), do: tool |> String.trim() |> non_empty!("tool")
  defp normalize_tool_name(tool) when is_atom(tool), do: tool |> Atom.to_string() |> normalize_tool_name()
  defp normalize_tool_name(other), do: raise(ArgumentError, "tool must be a binary or atom, got: #{inspect(other)}")

  defp normalize_session_id(session_id) when is_binary(session_id), do: session_id |> String.trim() |> non_empty!("session_id")
  defp normalize_session_id(session_id) when is_atom(session_id), do: session_id |> Atom.to_string() |> normalize_session_id()
  defp normalize_session_id(session_id) when is_integer(session_id), do: session_id |> Integer.to_string() |> normalize_session_id()
  defp normalize_session_id(other), do: raise(ArgumentError, "session_id must be a binary, atom, or integer, got: #{inspect(other)}")

  defp normalize_owner(nil, {pid, _tag}), do: "#{inspect(pid)} on #{node(pid)}"
  defp normalize_owner(owner, _from) when is_binary(owner), do: owner |> String.trim() |> non_empty!("owner")
  defp normalize_owner(other, _from), do: raise(ArgumentError, ":owner must be nil or a non-empty binary, got: #{inspect(other)}")

  defp normalize_notify_pid(nil, {pid, _tag}), do: pid
  defp normalize_notify_pid(pid, _from) when is_pid(pid), do: pid
  defp normalize_notify_pid(other, _from), do: raise(ArgumentError, ":notify_pid must be nil or a pid, got: #{inspect(other)}")

  defp normalize_meta(nil), do: %{}
  defp normalize_meta(meta) when is_map(meta), do: meta
  defp normalize_meta(meta) when is_list(meta), do: Map.new(meta)
  defp normalize_meta(other), do: raise(ArgumentError, ":meta must be a map, keyword list, or nil, got: #{inspect(other)}")

  defp normalize_notify(nil), do: nil
  defp normalize_notify(fun) when is_function(fun, 1), do: fun
  defp normalize_notify({module, function, extra}) when is_atom(module) and is_atom(function) and is_list(extra), do: {module, function, extra}
  defp normalize_notify(other), do: raise(ArgumentError, ":notify must be nil, a unary function, or {Module, :function, extra_args}, got: #{inspect(other)}")

  defp normalize_telemetry_prefix(nil), do: nil
  defp normalize_telemetry_prefix(false), do: nil

  defp normalize_telemetry_prefix(prefix) when is_list(prefix) do
    if prefix != [] and Enum.all?(prefix, &is_atom/1), do: prefix, else: raise(ArgumentError, ":telemetry_prefix must be nil or a non-empty atom list")
  end

  defp normalize_telemetry_prefix(other) do
    raise ArgumentError, ":telemetry_prefix must be nil, false, or a non-empty atom list, got: #{inspect(other)}"
  end

  defp positive_integer!(value, _name) when is_integer(value) and value > 0, do: value
  defp positive_integer!(value, name), do: raise(ArgumentError, "#{name} must be a positive integer, got: #{inspect(value)}")

  defp non_negative_integer!(value, _name) when is_integer(value) and value >= 0, do: value
  defp non_negative_integer!(value, name), do: raise(ArgumentError, "#{name} must be a non-negative integer, got: #{inspect(value)}")

  defp positive_integer_or_infinity!(:infinity, _name), do: :infinity
  defp positive_integer_or_infinity!(value, name), do: positive_integer!(value, name)

  defp boolean!(value, _name) when is_boolean(value), do: value
  defp boolean!(value, name), do: raise(ArgumentError, "#{name} must be a boolean, got: #{inspect(value)}")

  defp non_empty!("", name), do: raise(ArgumentError, "#{name} must not be empty")
  defp non_empty!(value, _name), do: value

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
  defp unix_ms, do: System.system_time(:millisecond)

  defp schedule_sweep(state) do
    if state.sweep_timer, do: Process.cancel_timer(state.sweep_timer)
    %{state | sweep_timer: Process.send_after(self(), :sweep, state.sweep_interval_ms)}
  end

  defp new_id do
    15
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp emit(state, event, measurements, metadata) do
    emit_notify(state.notify, event, measurements, metadata)
    emit_telemetry(state.telemetry_prefix, event, measurements, metadata)
  end

  defp emit_notify(nil, _event, _measurements, _metadata), do: :ok
  defp emit_notify(fun, event, measurements, metadata) when is_function(fun, 1), do: safe_notify(fn -> fun.({event, measurements, metadata}) end)
  defp emit_notify({module, function, extra}, event, measurements, metadata), do: safe_notify(fn -> apply(module, function, [event, measurements, metadata | extra]) end)

  defp emit_telemetry(nil, _event, _measurements, _metadata), do: :ok

  defp emit_telemetry(prefix, event, measurements, metadata) do
    if Code.ensure_loaded?(:telemetry) and function_exported?(:telemetry, :execute, 3) do
      :telemetry.execute(prefix ++ [event], measurements, metadata)
    else
      :ok
    end
  end

  defp safe_notify(fun) do
    fun.()
  rescue
    exception ->
      Logger.warning("McpConcurrencyGovernor notify handler failed: #{Exception.message(exception)}")
      :ok
  catch
    kind, reason ->
      Logger.warning("McpConcurrencyGovernor notify handler failed: #{inspect({kind, reason})}")
      :ok
  end

  defp safe_send(pid, message) when is_pid(pid) do
    send(pid, message)
    :ok
  end
end

# This solves MCP concurrency collapse in Elixir systems where agents, jobs, and live sessions all compete for the same expensive tools. In real systems that means browser workers, shell runners, search adapters, eval jobs, vector lookups, or any external step that is slower and scarcer than normal BEAM work. Without a governor, one chat or one tenant can quietly take every slot, create an unbounded wait queue, and make the whole agent stack feel random.
# Built because the real 2026 failure mode is not simply "a tool crashed." It is "the tool kept working, but the scheduler around it was unfair, bursty, and impossible to debug under load." A plain semaphore helps with hard concurrency limits, but it does not give session fairness, queue deadlines, cancellation, or stale run cleanup.
# Use it when your Phoenix app, MCP server, Oban worker, internal AI gateway, or research platform needs practical per-tool limits, per-session fairness, bounded waiting, and lease-based cleanup for slow tool calls.
# The trick: queue by session instead of by raw request and rotate across sessions in round-robin order. That keeps one noisy client from monopolizing the whole fleet while still staying small enough to audit. The queue TTL and run TTL make overload visible and recoverable instead of silently destructive.
# Drop this into an Elixir monolith, tool broker, multi-agent control plane, or platform service when you need fair MCP backpressure, safer tool admission control, and better multi-tenant throughput on a single BEAM node.
