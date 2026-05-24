defmodule AgentToolRetryBudget do
  @moduledoc false

  defstruct max_attempts: 3,
            max_elapsed_ms: 30_000,
            base_backoff_ms: 250,
            jitter_ms: 75,
            retryable_codes: MapSet.new(["timeout", "rate_limited", "overloaded", "transport"])

  def parse_args(args) do
    Enum.reduce(args, %__MODULE__{}, fn
      "--max-attempts", acc -> %{acc | max_attempts: :pending_max_attempts}
      "--max-elapsed-ms", acc -> %{acc | max_elapsed_ms: :pending_max_elapsed_ms}
      "--base-backoff-ms", acc -> %{acc | base_backoff_ms: :pending_base_backoff_ms}
      "--jitter-ms", acc -> %{acc | jitter_ms: :pending_jitter_ms}
      value, %{max_attempts: :pending_max_attempts} = acc -> %{acc | max_attempts: parse_int(value)}
      value, %{max_elapsed_ms: :pending_max_elapsed_ms} = acc -> %{acc | max_elapsed_ms: parse_int(value)}
      value, %{base_backoff_ms: :pending_base_backoff_ms} = acc -> %{acc | base_backoff_ms: parse_int(value)}
      value, %{jitter_ms: :pending_jitter_ms} = acc -> %{acc | jitter_ms: parse_int(value)}
      other, _acc -> raise ArgumentError, "unknown option #{other}"
    end)
  end

  def parse_int(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> parsed
      _ -> raise ArgumentError, "bad integer #{inspect(value)}"
    end
  end

  def parse_line(line) do
    [tool, code, elapsed, attempt, idempotent | rest] = String.split(String.trim(line), ",")
    %{
      tool: tool,
      code: code,
      elapsed_ms: parse_int(elapsed),
      attempt: parse_int(attempt),
      idempotent: idempotent in ["true", "1", "yes"],
      cost_usd: parse_cost(rest)
    }
  rescue
    _ -> raise ArgumentError, "row needs tool,code,elapsed_ms,attempt,idempotent[,cost_usd]"
  end

  def parse_cost([]), do: 0.0
  def parse_cost([raw | _]) do
    case Float.parse(raw) do
      {v, ""} -> v
      _ -> 0.0
    end
  end

  def decision(row, config) do
    retryable = MapSet.member?(config.retryable_codes, row.code)
    attempts_left = row.attempt < config.max_attempts
    time_left = row.elapsed_ms < config.max_elapsed_ms
    allowed = retryable and attempts_left and time_left and row.idempotent
    delay = if allowed, do: backoff(row.attempt, config), else: 0
    reason = reason(row, retryable, attempts_left, time_left)
    Map.merge(row, %{retry: allowed, delay_ms: delay, reason: reason})
  end

  defp backoff(attempt, config) do
    exponential = config.base_backoff_ms * round(:math.pow(2, max(attempt - 1, 0)))
    deterministic_jitter = rem(:erlang.phash2({attempt, config.jitter_ms}), config.jitter_ms + 1)
    min(exponential + deterministic_jitter, config.max_elapsed_ms)
  end

  defp reason(row, retryable, attempts_left, time_left) do
    cond do
      not row.idempotent -> "tool call is not idempotent"
      not retryable -> "error code is not retryable"
      not attempts_left -> "attempt budget exhausted"
      not time_left -> "elapsed budget exhausted"
      true -> "retry allowed inside budget"
    end
  end

  def summarize(decisions) do
    Enum.reduce(decisions, %{allow: 0, deny: 0, cost_at_risk: 0.0}, fn row, acc ->
      key = if row.retry, do: :allow, else: :deny
      acc
      |> Map.update!(key, &(&1 + 1))
      |> Map.update!(:cost_at_risk, &(&1 + row.cost_usd))
    end)
  end

  def render(decisions) do
    header = "tool\tcode\tattempt\tretry\tdelay_ms\treason"
    rows = Enum.map(decisions, fn row ->
      Enum.join([row.tool, row.code, row.attempt, row.retry, row.delay_ms, row.reason], "\t")
    end)
    Enum.join([header | rows], "\n") <> "\n"
  end

  def run(args, input) do
    config = parse_args(args)
    decisions =
      input
      |> String.split("\n", trim: true)
      |> Enum.reject(&String.starts_with?(&1, "tool,"))
      |> Enum.map(&parse_line/1)
      |> Enum.map(&decision(&1, config))

    IO.write(render(decisions))
    summary = summarize(decisions)
    IO.puts(:stderr, "allowed=#{summary.allow} denied=#{summary.deny} cost_at_risk=#{Float.round(summary.cost_at_risk, 4)}")
    if summary.deny > 0, do: 2, else: 0
  end
end

try do
  input = IO.read(:stdio, :all)
  System.halt(AgentToolRetryBudget.run(System.argv(), input))
rescue
  error ->
    IO.puts(:stderr, "AgentToolRetryBudget: #{Exception.message(error)}")
    System.halt(64)
end

# This solves the April 2026 agent retry problem where AI tool calls hit rate limits,
# streaming timeouts, flaky MCP transports, and overloaded internal APIs, then retry in ways
# that duplicate side effects or burn inference budget. Built because agent frameworks often
# expose retry knobs but do not give platform teams a small, auditable budget gate for each
# tool call. Use it when logs can provide tool, error code, elapsed milliseconds, attempt,
# idempotency, and optional cost. The trick: it refuses non-idempotent retries, caps attempts
# and elapsed time, and uses deterministic jitter so tests and CI output stay reproducible.
# Drop this into an Elixir service, Oban job, Livebook, or CI repository as one source file
# and it becomes an AI agent retry budget calculator, MCP tool safety gate, idempotent retry
# planner, LLM cost guardrail, and developer productivity utility with search-friendly names.
