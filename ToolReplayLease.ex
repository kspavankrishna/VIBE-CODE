defmodule ToolReplayLease do
  @moduledoc """
  `ToolReplayLease` is a small OTP service for replay protection around side-effecting work.

  In 2026 a lot of Elixir systems call external tools from AI agents, MCP servers, Oban jobs,
  Phoenix channels, Broadway consumers, and webhook handlers. Retries are normal, but retries
  around side effects are dangerous. A model stream can reconnect and re-emit the same tool call,
  a queue can redeliver a job after the HTTP call already succeeded, or a worker can crash after
  creating infrastructure but before the acknowledgement is persisted.

  This module gives you:

    * `claim/3` to acquire a lease for an idempotency key
    * `renew/3` to extend long-running work
    * `complete/4` to cache successful completion
    * `release/3` to give up a lease without marking success
    * `status/2` and `list/2` for introspection
    * `run/4` as a safe wrapper around a function

  The implementation uses a single `GenServer` plus ETS, so reads stay cheap and state
  transitions stay serialized. It has no external dependencies. If `:telemetry` is present in
  your project, the module will emit telemetry events automatically.

  Typical keys:

    * `mcp:tool_call:session_123:call_9`
    * `oban:job:billing:98765`
    * `webhook:github:delivery_id`
    * `agent:provision_cluster:request_id`
  """

  use GenServer

  require Logger

  @default_lease_ttl_ms 30_000
  @default_completion_ttl_ms 10 * 60_000
  @default_cleanup_interval_ms 60_000
  @default_orphan_ttl_ms 60 * 60_000
  @default_telemetry_prefix [:tool_replay_lease]

  @type server :: GenServer.server()
  @type key :: String.t()
  @type ttl_ms :: pos_integer() | :infinity
  @type meta :: map()

  defmodule Lease do
    @moduledoc "Opaque lease data returned by `claim/3`."

    @enforce_keys [:key, :token, :fence, :owner, :lease_ttl_ms, :claimed_at_unix_ms, :expires_in_ms]
    defstruct [
      :key,
      :token,
      :fence,
      :owner,
      :lease_ttl_ms,
      :claimed_at_unix_ms,
      :expires_in_ms,
      meta: %{}
    ]

    @type t :: %__MODULE__{
            key: String.t(),
            token: String.t(),
            fence: pos_integer(),
            owner: term(),
            lease_ttl_ms: pos_integer(),
            claimed_at_unix_ms: integer(),
            expires_in_ms: non_neg_integer(),
            meta: map()
          }
  end

  defmodule ActiveStatus do
    @moduledoc "Status for an in-flight lease holder."

    @enforce_keys [
      :key,
      :owner,
      :fence,
      :attempts,
      :lease_ttl_ms,
      :claimed_at_unix_ms,
      :updated_at_unix_ms,
      :expires_in_ms,
      :stale?
    ]
    defstruct [
      :key,
      :owner,
      :fence,
      :attempts,
      :lease_ttl_ms,
      :claimed_at_unix_ms,
      :updated_at_unix_ms,
      :expires_in_ms,
      :stale?,
      meta: %{}
    ]

    @type t :: %__MODULE__{
            key: String.t(),
            owner: term(),
            fence: pos_integer(),
            attempts: pos_integer(),
            lease_ttl_ms: pos_integer(),
            claimed_at_unix_ms: integer(),
            updated_at_unix_ms: integer(),
            expires_in_ms: non_neg_integer(),
            stale?: boolean(),
            meta: map()
          }
  end

  defmodule CompletedStatus do
    @moduledoc "Status for a cached successful completion."

    @enforce_keys [
      :key,
      :owner,
      :fence,
      :attempts,
      :claimed_at_unix_ms,
      :completed_at_unix_ms,
      :completion_ttl_ms,
      :expires_in_ms,
      :result_fingerprint,
      :result_size_bytes
    ]
    defstruct [
      :key,
      :owner,
      :fence,
      :attempts,
      :claimed_at_unix_ms,
      :completed_at_unix_ms,
      :completion_ttl_ms,
      :expires_in_ms,
      :result_fingerprint,
      :result_size_bytes,
      result: nil,
      meta: %{}
    ]

    @type t :: %__MODULE__{
            key: String.t(),
            owner: term(),
            fence: pos_integer(),
            attempts: pos_integer(),
            claimed_at_unix_ms: integer(),
            completed_at_unix_ms: integer(),
            completion_ttl_ms: ToolReplayLease.ttl_ms(),
            expires_in_ms: non_neg_integer() | :infinity,
            result_fingerprint: String.t() | nil,
            result_size_bytes: non_neg_integer() | nil,
            result: term(),
            meta: map()
          }
  end

  defmodule State do
    @moduledoc false

    @enforce_keys [
      :table,
      :lease_ttl_ms,
      :completion_ttl_ms,
      :cleanup_interval_ms,
      :orphan_ttl_ms,
      :notify,
      :telemetry_prefix
    ]
    defstruct [
      :table,
      :lease_ttl_ms,
      :completion_ttl_ms,
      :cleanup_interval_ms,
      :orphan_ttl_ms,
      :notify,
      :telemetry_prefix,
      cleanup_timer: nil
    ]
  end

  @type status :: ActiveStatus.t() | CompletedStatus.t()

  @doc """
  Starts the lease service.

  ## Options

    * `:name` - registered name for the server
    * `:table` - named ETS table atom if you want external inspection
    * `:lease_ttl_ms` - default active lease duration
    * `:completion_ttl_ms` - default completed result TTL, or `:infinity`
    * `:cleanup_interval_ms` - periodic cleanup cadence
    * `:orphan_ttl_ms` - how long stale active rows survive before cleanup removes them
    * `:notify` - `fn event -> ... end` or `{Module, :function, extra_args}`
    * `:telemetry_prefix` - event prefix, defaults to `[:tool_replay_lease]`
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
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

  @doc """
  Claims an idempotency key.

  Returns one of:

    * `{:ok, lease}` when this caller won the lease
    * `{:busy, active_status}` when another live owner still holds the lease
    * `{:completed, completed_status}` when a successful result is still cached

  ## Claim options

    * `:owner` - readable owner label for logs and dashboards
    * `:meta` - map or keyword metadata persisted alongside the lease
    * `:lease_ttl_ms` - override the default active TTL for this claim
    * `:completion_ttl_ms` - default TTL to use later on `complete/4`
    * `:allow_takeover?` - if true, a stale active lease may be replaced by a new owner
  """
  @spec claim(server(), key(), keyword()) ::
          {:ok, Lease.t()} | {:busy, ActiveStatus.t()} | {:completed, CompletedStatus.t()}
  def claim(server \\ __MODULE__, key, opts \\ []) do
    GenServer.call(server, {:claim, key, opts}, :infinity)
  end

  @doc """
  Renews an existing lease.

  Returns `{:error, :stale_lease}` when the key has already been taken over or completed.
  """
  @spec renew(server(), Lease.t(), keyword()) :: {:ok, Lease.t()} | {:error, :not_found | :stale_lease}
  def renew(server \\ __MODULE__, %Lease{} = lease, opts \\ []) do
    GenServer.call(server, {:renew, lease, opts}, :infinity)
  end

  @doc """
  Marks a lease as completed and caches the result.

  ## Options

    * `:meta` - completion metadata to merge into the existing record
    * `:completion_ttl_ms` - override the completion TTL for this result
    * `:store_result?` - if false, cache only metadata and fingerprint, not the full result term
  """
  @spec complete(server(), Lease.t(), term(), keyword()) ::
          {:ok, CompletedStatus.t()} | {:error, :not_found | :stale_lease}
  def complete(server \\ __MODULE__, %Lease{} = lease, result, opts \\ []) do
    GenServer.call(server, {:complete, lease, result, opts}, :infinity)
  end

  @doc """
  Releases an active lease without marking success.

  This is what you want after a retryable failure. The next caller may claim the key immediately.
  """
  @spec release(server(), Lease.t(), keyword()) :: :ok | {:error, :not_found | :stale_lease}
  def release(server \\ __MODULE__, %Lease{} = lease, opts \\ []) do
    GenServer.call(server, {:release, lease, opts}, :infinity)
  end

  @doc """
  Returns the current status for a key.
  """
  @spec status(server(), key()) :: :missing | {:active, ActiveStatus.t()} | {:completed, CompletedStatus.t()}
  def status(server \\ __MODULE__, key) do
    GenServer.call(server, {:status, key}, :infinity)
  end

  @doc """
  Deletes any cached or active entry for the key.
  """
  @spec forget(server(), key()) :: :ok
  def forget(server \\ __MODULE__, key) do
    GenServer.call(server, {:forget, key}, :infinity)
  end

  @doc """
  Lists all active and completed rows.

  ## Options

    * `:state` - `:all`, `:active`, or `:completed`
    * `:limit` - maximum number of rows, or `:infinity`
  """
  @spec list(server(), keyword()) :: [status()]
  def list(server \\ __MODULE__, opts \\ []) do
    GenServer.call(server, {:list, opts}, :infinity)
  end

  @doc """
  Forces a cleanup pass and returns cleanup counts.
  """
  @spec sweep(server()) :: %{deleted_completed: non_neg_integer(), deleted_orphans: non_neg_integer()}
  def sweep(server \\ __MODULE__) do
    GenServer.call(server, :sweep, :infinity)
  end

  @doc """
  Runs a function under a replay lease.

  Returns:

    * `{:ok, result, completed_status}` on fresh successful execution
    * `{:completed, cached_result, completed_status}` when the key already completed
    * `{:busy, active_status}` when another live owner still holds the lease

  If the function raises or throws, the lease is released before the error is re-raised.
  """
  @spec run(server(), key(), (() -> term()), keyword()) ::
          {:ok, term(), CompletedStatus.t()}
          | {:completed, term(), CompletedStatus.t()}
          | {:busy, ActiveStatus.t()}
          | {:error, :not_found | :stale_lease}
  def run(server \\ __MODULE__, key, fun, opts \\ []) when is_function(fun, 0) do
    case claim(server, key, opts) do
      {:ok, lease} ->
        try do
          result = fun.()

          case complete(server, lease, result, opts) do
            {:ok, completed} -> {:ok, result, completed}
            {:error, reason} -> {:error, reason}
          end
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

      {:completed, completed} ->
        {:completed, completed.result, completed}

      {:busy, active} ->
        {:busy, active}
    end
  end

  @impl true
  def init(opts) do
    state = %State{
      table: create_table(Keyword.get(opts, :table)),
      lease_ttl_ms: positive_integer!(Keyword.get(opts, :lease_ttl_ms, @default_lease_ttl_ms), :lease_ttl_ms),
      completion_ttl_ms:
        ttl_or_infinity!(
          Keyword.get(opts, :completion_ttl_ms, @default_completion_ttl_ms),
          :completion_ttl_ms
        ),
      cleanup_interval_ms:
        positive_integer!(
          Keyword.get(opts, :cleanup_interval_ms, @default_cleanup_interval_ms),
          :cleanup_interval_ms
        ),
      orphan_ttl_ms:
        positive_integer!(Keyword.get(opts, :orphan_ttl_ms, @default_orphan_ttl_ms), :orphan_ttl_ms),
      notify: normalize_notify(Keyword.get(opts, :notify)),
      telemetry_prefix: normalize_telemetry_prefix(Keyword.get(opts, :telemetry_prefix, @default_telemetry_prefix))
    }

    {:ok, schedule_cleanup(state)}
  end

  @impl true
  def handle_call({:claim, raw_key, opts}, from, state) do
    key = normalize_key(raw_key)
    now_mono = monotonic_ms()
    now_unix = unix_ms()
    claim_opts = claim_options(opts, state, from)

    reply =
      case current_entry(state, key, now_mono) do
        nil ->
          entry =
            build_active_entry(
              key,
              claim_opts.owner,
              claim_opts.meta,
              claim_opts.lease_ttl_ms,
              claim_opts.completion_ttl_ms,
              1,
              1,
              now_mono,
              now_unix
            )

          :ets.insert(state.table, {key, entry})
          lease = lease_from_entry(entry, now_mono)
          emit(state, :claimed, %{attempts: 1, ttl_ms: entry.lease_ttl_ms}, %{key: key, owner: entry.owner, fence: 1})
          {:ok, lease}

        %{state: :completed} = entry ->
          {:completed, completed_status_from_entry(entry, now_mono)}

        %{state: :active} = entry ->
          if expired?(entry, now_mono) and claim_opts.allow_takeover? do
            next_attempts = entry.attempts + 1
            next_fence = entry.fence + 1

            replacement =
              build_active_entry(
                key,
                claim_opts.owner,
                claim_opts.meta,
                claim_opts.lease_ttl_ms,
                claim_opts.completion_ttl_ms,
                next_attempts,
                next_fence,
                now_mono,
                now_unix
              )

            previous_owner = entry.owner
            :ets.insert(state.table, {key, replacement})
            lease = lease_from_entry(replacement, now_mono)

            emit(
              state,
              :taken_over,
              %{attempts: next_attempts, ttl_ms: replacement.lease_ttl_ms},
              %{key: key, owner: replacement.owner, previous_owner: previous_owner, fence: next_fence}
            )

            {:ok, lease}
          else
            {:busy, active_status_from_entry(entry, now_mono)}
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:renew, %Lease{} = lease, opts}, _from, state) do
    now_mono = monotonic_ms()
    now_unix = unix_ms()
    renew_opts = renew_options(opts)

    reply =
      case raw_lookup(state.table, lease.key) do
        nil ->
          {:error, :not_found}

        %{state: :active} = entry ->
          if same_lease?(entry, lease) do
            next_ttl = renew_opts.lease_ttl_ms || entry.lease_ttl_ms
            next_meta = Map.merge(entry.meta, renew_opts.meta)

            renewed = %{
              entry
              | lease_ttl_ms: next_ttl,
                meta: next_meta,
                updated_at_unix_ms: now_unix,
                updated_at_mono_ms: now_mono,
                expires_at_mono_ms: now_mono + next_ttl
            }

            :ets.insert(state.table, {lease.key, renewed})
            renewed_lease = lease_from_entry(renewed, now_mono)

            emit(state, :renewed, %{ttl_ms: next_ttl}, %{key: lease.key, owner: renewed.owner, fence: renewed.fence})
            {:ok, renewed_lease}
          else
            {:error, :stale_lease}
          end

        %{state: :completed} = entry ->
          if same_lease?(entry, lease), do: {:error, :stale_lease}, else: {:error, :stale_lease}
      end

    {:reply, reply, state}
  end

  def handle_call({:complete, %Lease{} = lease, result, opts}, _from, state) do
    now_mono = monotonic_ms()
    now_unix = unix_ms()
    complete_opts = complete_options(opts, state)

    reply =
      case raw_lookup(state.table, lease.key) do
        nil ->
          {:error, :not_found}

        %{state: :completed} = entry ->
          if same_lease?(entry, lease) do
            {:ok, completed_status_from_entry(entry, now_mono)}
          else
            {:error, :stale_lease}
          end

        %{state: :active} = entry ->
          if same_lease?(entry, lease) do
            completed = build_completed_entry(entry, result, complete_opts, now_mono, now_unix)
            :ets.insert(state.table, {lease.key, completed})
            status = completed_status_from_entry(completed, now_mono)

            emit(
              state,
              :completed,
              %{attempts: completed.attempts, result_size_bytes: completed.result_size_bytes || 0},
              %{
                key: completed.key,
                owner: completed.owner,
                fence: completed.fence,
                result_fingerprint: completed.result_fingerprint
              }
            )

            {:ok, status}
          else
            {:error, :stale_lease}
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:release, %Lease{} = lease, opts}, _from, state) do
    reason = Keyword.get(opts, :reason)

    reply =
      case raw_lookup(state.table, lease.key) do
        nil ->
          {:error, :not_found}

        %{state: :completed} = entry ->
          if same_lease?(entry, lease), do: :ok, else: {:error, :stale_lease}

        %{state: :active} = entry ->
          if same_lease?(entry, lease) do
            :ets.delete(state.table, lease.key)

            emit(
              state,
              :released,
              %{attempts: entry.attempts, ttl_ms: entry.lease_ttl_ms},
              %{key: entry.key, owner: entry.owner, fence: entry.fence, reason: reason}
            )

            :ok
          else
            {:error, :stale_lease}
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:status, raw_key}, _from, state) do
    key = normalize_key(raw_key)
    now_mono = monotonic_ms()

    reply =
      case current_entry(state, key, now_mono) do
        nil -> :missing
        %{state: :active} = entry -> {:active, active_status_from_entry(entry, now_mono)}
        %{state: :completed} = entry -> {:completed, completed_status_from_entry(entry, now_mono)}
      end

    {:reply, reply, state}
  end

  def handle_call({:forget, raw_key}, _from, state) do
    key = normalize_key(raw_key)
    :ets.delete(state.table, key)
    emit(state, :forgotten, %{}, %{key: key})
    {:reply, :ok, state}
  end

  def handle_call({:list, opts}, _from, state) do
    _ = sweep_entries(state, monotonic_ms())
    now_mono = monotonic_ms()
    filter = normalize_state_filter(Keyword.get(opts, :state, :all))
    limit = normalize_limit(Keyword.get(opts, :limit, :infinity))

    rows =
      state.table
      |> :ets.tab2list()
      |> Enum.map(fn {_key, entry} -> materialize_status(entry, now_mono) end)
      |> Enum.filter(&match_filter?(&1, filter))
      |> Enum.sort_by(fn
        %ActiveStatus{key: key, claimed_at_unix_ms: claimed_at} -> {0, key, claimed_at}
        %CompletedStatus{key: key, completed_at_unix_ms: completed_at} -> {1, key, completed_at}
      end)
      |> maybe_take(limit)

    {:reply, rows, state}
  end

  def handle_call(:sweep, _from, state) do
    counts = sweep_entries(state, monotonic_ms())

    emit(state, :swept, counts, %{table: inspect(state.table)})
    {:reply, counts, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    counts = sweep_entries(state, monotonic_ms())

    emit(state, :swept, counts, %{table: inspect(state.table), periodic?: true})
    {:noreply, schedule_cleanup(state)}
  end

  @impl true
  def terminate(_reason, state) do
    if state.cleanup_timer do
      Process.cancel_timer(state.cleanup_timer)
    end

    :ok
  end

  defp create_table(nil) do
    :ets.new(__MODULE__, [
      :set,
      :protected,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])
  end

  defp create_table(name) when is_atom(name) do
    :ets.new(name, [
      :set,
      :named_table,
      :protected,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])
  end

  defp create_table(other) do
    raise ArgumentError, ":table must be nil or an atom, got: #{inspect(other)}"
  end

  defp schedule_cleanup(state) do
    if state.cleanup_timer do
      Process.cancel_timer(state.cleanup_timer)
    end

    timer = Process.send_after(self(), :cleanup, state.cleanup_interval_ms)
    %{state | cleanup_timer: timer}
  end

  defp claim_options(opts, state, from) do
    %{
      owner: Keyword.get_lazy(opts, :owner, fn -> default_owner(from) end),
      meta: normalize_meta(Keyword.get(opts, :meta, %{})),
      lease_ttl_ms:
        positive_integer!(Keyword.get(opts, :lease_ttl_ms, state.lease_ttl_ms), :lease_ttl_ms),
      completion_ttl_ms:
        ttl_or_infinity!(
          Keyword.get(opts, :completion_ttl_ms, state.completion_ttl_ms),
          :completion_ttl_ms
        ),
      allow_takeover?: boolean!(Keyword.get(opts, :allow_takeover?, true), :allow_takeover?)
    }
  end

  defp renew_options(opts) do
    ttl_override =
      case Keyword.get(opts, :lease_ttl_ms) do
        nil -> nil
        value -> positive_integer!(value, :lease_ttl_ms)
      end

    %{
      lease_ttl_ms: ttl_override,
      meta: normalize_meta(Keyword.get(opts, :meta, %{}))
    }
  end

  defp complete_options(opts, state) do
    %{
      meta: normalize_meta(Keyword.get(opts, :meta, %{})),
      completion_ttl_ms:
        ttl_or_infinity!(
          Keyword.get(opts, :completion_ttl_ms, state.completion_ttl_ms),
          :completion_ttl_ms
        ),
      store_result?: boolean!(Keyword.get(opts, :store_result?, true), :store_result?)
    }
  end

  defp current_entry(state, key, now_mono) do
    case raw_lookup(state.table, key) do
      nil ->
        nil

      %{state: :completed} = entry ->
        if expired?(entry, now_mono) do
          :ets.delete(state.table, key)
          nil
        else
          entry
        end

      %{state: :active} = entry ->
        if orphaned?(entry, now_mono, state.orphan_ttl_ms) do
          :ets.delete(state.table, key)
          nil
        else
          entry
        end
    end
  end

  defp raw_lookup(table, key) do
    case :ets.lookup(table, key) do
      [{^key, entry}] -> entry
      [] -> nil
    end
  end

  defp build_active_entry(key, owner, meta, lease_ttl_ms, completion_ttl_ms, attempts, fence, now_mono, now_unix) do
    %{
      state: :active,
      key: key,
      token: new_token(),
      owner: owner,
      meta: meta,
      attempts: attempts,
      fence: fence,
      lease_ttl_ms: lease_ttl_ms,
      completion_ttl_ms: completion_ttl_ms,
      claimed_at_unix_ms: now_unix,
      claimed_at_mono_ms: now_mono,
      updated_at_unix_ms: now_unix,
      updated_at_mono_ms: now_mono,
      expires_at_mono_ms: now_mono + lease_ttl_ms
    }
  end

  defp build_completed_entry(active_entry, result, complete_opts, now_mono, now_unix) do
    {fingerprint, size_bytes} = fingerprint_result(result)
    stored_result = if complete_opts.store_result?, do: result, else: nil

    %{
      state: :completed,
      key: active_entry.key,
      token: active_entry.token,
      owner: active_entry.owner,
      meta: Map.merge(active_entry.meta, complete_opts.meta),
      attempts: active_entry.attempts,
      fence: active_entry.fence,
      lease_ttl_ms: active_entry.lease_ttl_ms,
      completion_ttl_ms: complete_opts.completion_ttl_ms,
      claimed_at_unix_ms: active_entry.claimed_at_unix_ms,
      completed_at_unix_ms: now_unix,
      updated_at_unix_ms: now_unix,
      updated_at_mono_ms: now_mono,
      expires_at_mono_ms: ttl_to_expiry(now_mono, complete_opts.completion_ttl_ms),
      result: stored_result,
      result_fingerprint: fingerprint,
      result_size_bytes: size_bytes
    }
  end

  defp lease_from_entry(entry, now_mono) do
    %Lease{
      key: entry.key,
      token: entry.token,
      fence: entry.fence,
      owner: entry.owner,
      lease_ttl_ms: entry.lease_ttl_ms,
      claimed_at_unix_ms: entry.claimed_at_unix_ms,
      expires_in_ms: ttl_remaining(entry.expires_at_mono_ms, now_mono),
      meta: entry.meta
    }
  end

  defp active_status_from_entry(entry, now_mono) do
    %ActiveStatus{
      key: entry.key,
      owner: entry.owner,
      fence: entry.fence,
      attempts: entry.attempts,
      lease_ttl_ms: entry.lease_ttl_ms,
      claimed_at_unix_ms: entry.claimed_at_unix_ms,
      updated_at_unix_ms: entry.updated_at_unix_ms,
      expires_in_ms: ttl_remaining(entry.expires_at_mono_ms, now_mono),
      stale?: expired?(entry, now_mono),
      meta: entry.meta
    }
  end

  defp completed_status_from_entry(entry, now_mono) do
    %CompletedStatus{
      key: entry.key,
      owner: entry.owner,
      fence: entry.fence,
      attempts: entry.attempts,
      claimed_at_unix_ms: entry.claimed_at_unix_ms,
      completed_at_unix_ms: entry.completed_at_unix_ms,
      completion_ttl_ms: entry.completion_ttl_ms,
      expires_in_ms: ttl_remaining(entry.expires_at_mono_ms, now_mono),
      result_fingerprint: entry.result_fingerprint,
      result_size_bytes: entry.result_size_bytes,
      result: entry.result,
      meta: entry.meta
    }
  end

  defp materialize_status(%{state: :active} = entry, now_mono), do: active_status_from_entry(entry, now_mono)
  defp materialize_status(%{state: :completed} = entry, now_mono), do: completed_status_from_entry(entry, now_mono)

  defp same_lease?(entry, %Lease{} = lease) do
    entry.key == lease.key and entry.token == lease.token and entry.fence == lease.fence
  end

  defp sweep_entries(state, now_mono) do
    {completed_keys, orphan_keys} =
      state.table
      |> :ets.tab2list()
      |> Enum.reduce({[], []}, fn {key, entry}, {completed_acc, orphan_acc} ->
        cond do
          entry.state == :completed and expired?(entry, now_mono) ->
            {[key | completed_acc], orphan_acc}

          entry.state == :active and orphaned?(entry, now_mono, state.orphan_ttl_ms) ->
            {completed_acc, [key | orphan_acc]}

          true ->
            {completed_acc, orphan_acc}
        end
      end)

    Enum.each(completed_keys, &:ets.delete(state.table, &1))
    Enum.each(orphan_keys, &:ets.delete(state.table, &1))

    %{
      deleted_completed: length(completed_keys),
      deleted_orphans: length(orphan_keys)
    }
  end

  defp match_filter?(%ActiveStatus{}, :all), do: true
  defp match_filter?(%CompletedStatus{}, :all), do: true
  defp match_filter?(%ActiveStatus{}, :active), do: true
  defp match_filter?(%CompletedStatus{}, :completed), do: true
  defp match_filter?(_, _), do: false

  defp maybe_take(list, :infinity), do: list
  defp maybe_take(list, limit), do: Enum.take(list, limit)

  defp normalize_key(key) when is_binary(key) do
    trimmed = String.trim(key)

    if trimmed == "" do
      raise ArgumentError, "key must not be empty"
    else
      trimmed
    end
  end

  defp normalize_key(key) when is_atom(key) or is_integer(key), do: key |> to_string() |> normalize_key()
  defp normalize_key(key) when is_list(key), do: key |> IO.iodata_to_binary() |> normalize_key()

  defp normalize_key(other) do
    raise ArgumentError, "key must be a binary, atom, integer, or iodata, got: #{inspect(other)}"
  end

  defp normalize_meta(nil), do: %{}
  defp normalize_meta(meta) when is_map(meta), do: meta
  defp normalize_meta(meta) when is_list(meta), do: Map.new(meta)

  defp normalize_meta(other) do
    raise ArgumentError, ":meta must be a map, keyword list, or nil, got: #{inspect(other)}"
  end

  defp normalize_notify(nil), do: nil
  defp normalize_notify(fun) when is_function(fun, 1), do: fun
  defp normalize_notify({module, function, extra}) when is_atom(module) and is_atom(function) and is_list(extra), do: {module, function, extra}

  defp normalize_notify(other) do
    raise ArgumentError,
          ":notify must be nil, a unary function, or {Module, :function, extra_args}, got: #{inspect(other)}"
  end

  defp normalize_telemetry_prefix(nil), do: nil
  defp normalize_telemetry_prefix(false), do: nil

  defp normalize_telemetry_prefix(prefix) when is_list(prefix) do
    if prefix != [] and Enum.all?(prefix, &is_atom/1) do
      prefix
    else
      raise ArgumentError, ":telemetry_prefix must be nil or a non-empty atom list"
    end
  end

  defp normalize_telemetry_prefix(other) do
    raise ArgumentError, ":telemetry_prefix must be nil, false, or a non-empty atom list, got: #{inspect(other)}"
  end

  defp normalize_state_filter(:all), do: :all
  defp normalize_state_filter(:active), do: :active
  defp normalize_state_filter(:completed), do: :completed

  defp normalize_state_filter(other) do
    raise ArgumentError, ":state must be :all, :active, or :completed, got: #{inspect(other)}"
  end

  defp normalize_limit(:infinity), do: :infinity
  defp normalize_limit(limit) when is_integer(limit) and limit >= 0, do: limit

  defp normalize_limit(other) do
    raise ArgumentError, ":limit must be a non-negative integer or :infinity, got: #{inspect(other)}"
  end

  defp positive_integer!(value, name) when is_integer(value) and value > 0, do: value

  defp positive_integer!(value, name) do
    raise ArgumentError, "#{name} must be a positive integer, got: #{inspect(value)}"
  end

  defp ttl_or_infinity!(:infinity, _name), do: :infinity
  defp ttl_or_infinity!(value, name), do: positive_integer!(value, name)

  defp boolean!(value, _name) when is_boolean(value), do: value

  defp boolean!(value, name) do
    raise ArgumentError, "#{name} must be a boolean, got: #{inspect(value)}"
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
  defp unix_ms, do: System.system_time(:millisecond)

  defp ttl_to_expiry(_now_mono, :infinity), do: :infinity
  defp ttl_to_expiry(now_mono, ttl_ms), do: now_mono + ttl_ms

  defp ttl_remaining(:infinity, _now_mono), do: :infinity
  defp ttl_remaining(expires_at_mono_ms, now_mono), do: max(expires_at_mono_ms - now_mono, 0)

  defp expired?(%{expires_at_mono_ms: :infinity}, _now_mono), do: false
  defp expired?(%{expires_at_mono_ms: expires_at_mono_ms}, now_mono), do: now_mono >= expires_at_mono_ms

  defp orphaned?(entry, now_mono, orphan_ttl_ms) do
    expired?(entry, now_mono) and now_mono >= entry.expires_at_mono_ms + orphan_ttl_ms
  end

  defp new_token do
    18
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp default_owner({pid, _tag}) do
    "#{inspect(pid)} on #{node(pid)}"
  end

  defp fingerprint_result(result) do
    try do
      binary = :erlang.term_to_binary(result)
      fingerprint = :crypto.hash(:sha256, binary) |> Base.encode16(case: :lower)
      {fingerprint, byte_size(binary)}
    rescue
      _ -> {nil, nil}
    catch
      _, _ -> {nil, nil}
    end
  end

  defp emit(state, event, measurements, metadata) do
    emit_notify(state.notify, event, measurements, metadata)
    emit_telemetry(state.telemetry_prefix, event, measurements, metadata)
  end

  defp emit_notify(nil, _event, _measurements, _metadata), do: :ok

  defp emit_notify(fun, event, measurements, metadata) when is_function(fun, 1) do
    safe_notify(fn -> fun.({event, measurements, metadata}) end)
  end

  defp emit_notify({module, function, extra}, event, measurements, metadata) do
    safe_notify(fn -> apply(module, function, [event, measurements, metadata | extra]) end)
  end

  defp safe_notify(fun) do
    fun.()
  rescue
    exception ->
      Logger.warning("ToolReplayLease notify handler failed: #{Exception.message(exception)}")
      :ok
  catch
    kind, reason ->
      Logger.warning("ToolReplayLease notify handler failed: #{inspect({kind, reason})}")
      :ok
  end

  defp emit_telemetry(nil, _event, _measurements, _metadata), do: :ok

  defp emit_telemetry(prefix, event, measurements, metadata) do
    if Code.ensure_loaded?(:telemetry) and function_exported?(:telemetry, :execute, 3) do
      :telemetry.execute(prefix ++ [event], measurements, metadata)
    else
      :ok
    end
  end
end

# This solves duplicate Elixir AI tool execution, MCP replay bugs, webhook redelivery, and job retry races where the same external side effect can run twice.
# Built because in real Phoenix, Oban, Broadway, and GenServer systems, the dangerous failure mode is not a crash. It is "the request actually worked, but the worker did not know that and did it again."
# Use it when you call billing APIs, cloud provisioning, GitHub mutations, ticket creation, or any MCP tool that must stay idempotent under retries.
# The trick: keep a short lease for active work, a longer TTL for completed work, and a fencing token so an old worker cannot overwrite a newer owner after takeover.
# Drop this into an Elixir monolith, Phoenix app, AI agent runtime, MCP server, or queue worker pool when you need replay protection, idempotency keys, duplicate suppression, and clear observability without pulling in Redis or another external lock service.
