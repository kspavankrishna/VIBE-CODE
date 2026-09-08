# Agent Budget Quorum

Two AI agents propose the same deploy at the same instant and both get to run it. This is a deterministic admission controller in Elixir that groups competing tool call proposals by fingerprint, requires a risk weighted provider quorum, checks a spend ledger and hands out a short lease so the side effect fires once.

**Language:** Elixir | **Lines:** 695 | **Added:** 2026-07-03

## What this solves

Agent systems fan out. You ask two models for a plan, or run a primary and a shadow, or an orchestrator retries a worker whose reply got lost on the wire. Each path can independently decide the right next action is "call the deploy tool" or "issue the refund" or "reserve eight A100s". Nothing in a normal MCP server, tool router or LLM gateway stops the second one. The side effect happens, and it happens twice.

The failure is not subtle when it lands. A double deploy means two rollouts racing and a service flapping between two SHAs. A double refund means money left the account twice and finance notices at month end, not at request time. The person who notices is rarely the person who wrote the agent loop, and by then the trace that would explain it has rotated out.

The second failure is quieter and more expensive. Nothing tracks aggregate spend across proposals. One agent run loops, each iteration proposes a legitimate paid tool call, every call individually passes review, and the run burns a month of budget in forty minutes. Per call rate limits miss this because no single call is abnormal. You need a ledger that knows the run already reserved 4.2 million micros before it lets the next call through.

This module answers one question and returns an audit record for the answer: may this proposal execute, right now, given what has already been reserved, committed, seen and leased.

## Why I built it

Every real fix for this lives inside a bigger product. Temporal gives you idempotency but you have to move your agent into Temporal. A billing gateway gives you spend limits but knows nothing about provider agreement. Feature flags give you a kill switch but no notion of quorum. Nobody ships a small thing that sits directly in front of the side effect and says yes or no with a reason you can log.

So: one file, no dependencies beyond `:crypto` and the standard library, droppable into an existing GenServer, Oban worker, Phoenix controller or Livebook harness. Storage agnostic on purpose, and every function returns the next ledger instead of mutating hidden state, so the admission path replays during an incident review.

## When to use it

- Two models, or a primary and a shadow, both propose the same deploy or config write and you want agreement before it runs.
- An MCP server exposes paid or destructive tools and the same call arrives twice because a retry fired after a timeout.
- An agent run can loop and you need a hard ceiling on what that single run may spend.
- Some tools need more than one confident provider to authorize them, while a cheap lookup should pass on one vote.
- You want a rollout where a tool is admitted but executed in dry run mode while you watch the decision log.
- An auditor asks why a billing write was allowed and you need the reason, the providers, the weights and the projected spend at that instant.

## How it works

Proposals are plain maps with atom or string keys. `normalize_proposal/2` pulls `run_id`, `tool` and `provider` as required non blank strings, coerces `cost_micros` to a non negative integer, validates `risk` and `confidence` into the closed zero to one range, and reads `observed_at_ms`. Anything malformed halts the batch as `{:malformed_proposal, index, reason}`, so a bad payload is never partially applied.

Identity is a SHA256 fingerprint over the `run_id`, the `tool` and either a caller supplied `arguments_digest` or one computed here. The interesting part is `canonical/1`: it walks the argument term, stringifies map keys, sorts map entries by key and flattens tuples to lists before `:erlang.term_to_binary`. So `%{service: "checkout", sha: "9f2c1d"}` from one model and `%{"sha" => "9f2c1d", "service" => "checkout"}` from another collapse to the same digest. An `idempotency_key` wins outright and makes the fingerprint `"idempotency:" <> key`, fencing a call whose arguments legitimately differ across providers.

`admit/3` takes one proposal or a list, groups by fingerprint, then `strongest_fingerprint_group/2` picks the winner by `{quorum_weight, length, max_confidence}`. Quorum weight sums `provider_weights` over the distinct providers in the group, counting only proposals whose confidence clears `minimum_confidence`, so a provider that votes three times still votes once and a hedging provider does not vote at all. Unlisted providers weigh 1.0. `representative/2` then takes the strongest proposal but overrides its cost, risk and confidence with the group maximum. Pessimistic on purpose: if any voter thinks this is expensive or risky, the whole group is.

Required quorum comes from `risk_quorums`, a list of `{max_risk, required_weight}` buckets sorted ascending and matched on the first bucket where `risk <= max_risk`. The default ladder is weight 1.0 below 0.30 risk, 2.0 below 0.65, 3.0 below 0.85 and 4.0 up to 1.0, so a cheap read passes on one provider while a destructive call needs three or four independent weighted votes. Buckets that fail to cover the observed risk fall back to a required weight of `:infinity` and reject, which is the safe direction.

Checks then run in a `cond` with fixed precedence: an active lease is `:duplicate_inflight`, a recent completion is `:duplicate_recently_completed`, then the errors from `proposal_errors/3` (`tool_denied`, `tool_not_allowed`, `risk_too_high`, `confidence_too_low`, `proposal_from_future` past `max_clock_skew_ms`, `proposal_expired` past `max_proposal_age_ms`), then `quorum_not_met`, then `budget_status/2` over total, run and tool scope, where used is reserved plus committed.

Acceptance is a two phase reservation, not fire and forget. `accept/6` writes a reservation keyed by fingerprint with `expires_at_ms = now + lease_ttl_ms`, adds the cost to `reserved_micros` and the per run and per tool maps, and sets `mode` to `:dry_run` if the tool is in `dry_run_tools`. The caller executes, then calls `record_result/4`, which releases the reservation, commits the actually charged amount and writes a `seen` fence expiring after `duplicate_ttl_ms`. If the caller dies mid execution, `compact/2` runs at the head of every `admit` and `record_result`, drops the stale lease and releases its budget so a crashed worker does not strand your ceiling forever.

## Usage

```elixir
policy =
  AgentBudgetQuorum.default_policy(%{
    total_budget_micros: 50_000_000,
    run_budget_micros: %{"run-42" => 2_000_000},
    tool_budget_micros: %{"deploy.release" => 5_000_000},
    provider_weights: %{"claude" => 1.5, "gpt" => 1.0, "static-linter" => 0.5},
    denied_tools: ["billing.refund_all"],
    dry_run_tools: ["deploy.release"],
    minimum_confidence: 0.6,
    lease_ttl_ms: 90_000
  })

ledger = AgentBudgetQuorum.new(policy)

proposals = [
  %{
    run_id: "run-42",
    tool: "deploy.release",
    provider: "claude",
    arguments: %{service: "checkout", sha: "9f2c1d"},
    cost_micros: 250_000,
    risk: 0.72,
    confidence: 0.88
  },
  %{
    "run_id" => "run-42",
    "tool" => "deploy.release",
    "provider" => "gpt",
    "arguments" => %{"sha" => "9f2c1d", "service" => "checkout"},
    "cost_micros" => 250_000,
    "risk" => 0.70,
    "confidence" => 0.81
  }
]

case AgentBudgetQuorum.admit(ledger, proposals) do
  {:ok, decision, ledger} ->
    # decision.mode is :execute or :dry_run, decision.fingerprint is the lease key
    Logger.info(AgentBudgetQuorum.decision_to_map(decision))
    outcome = run_the_tool(decision)

    {:ok, fence, ledger} =
      AgentBudgetQuorum.record_result(
        ledger,
        decision.fingerprint,
        %{status: :ok, charged_micros: 231_400, metadata: %{run_url: outcome.url}}
      )

    {ledger, fence}

  {:error, decision, ledger} ->
    # decision.reason is :quorum_not_met, :duplicate_inflight, {:budget_exceeded, :run}, ...
    Logger.warning(AgentBudgetQuorum.decision_to_map(decision))
    {ledger, :blocked}
end

# compute a fingerprint upfront, read a dashboard view, sweep expired leases
AgentBudgetQuorum.fingerprint("run-42", "deploy.release", %{service: "checkout"})
AgentBudgetQuorum.summary(ledger)
ledger = AgentBudgetQuorum.compact(ledger)
```

## Notes

- Not concurrency safe on its own. Every function is pure and returns the next ledger, so two processes calling `admit/3` against the same ledger value both get admitted. Serialize it through one GenServer, one Agent owner or one row lock.
- Storage is your problem. The `Ledger` struct is in memory only. Restart the owning process and every lease and duplicate fence is gone, so an in flight side effect can be re proposed.
- `now_ms` is injectable on `admit/3`, `record_result/4` and `compact/2`, defaulting to `System.system_time(:millisecond)`. A badly skewed clock is rejected as `:proposal_from_future` or `:proposal_expired` rather than silently trusted.
- It admits, it does not execute. It never calls your tool and never rolls anything back. Forget `record_result/4` and the lease just expires, releasing the reserved budget on the next compact.
- Costs are integers in micros, and a missing or unparseable `charged_micros` falls back to the reserved estimate, so a sloppy result payload overcounts rather than undercounts. `record_result/4` on an unknown or expired fingerprint returns `{:error, %{error: :unknown_or_expired_fingerprint, ...}, ledger}` untouched, which is your signal the lease TTL is shorter than real execution time.
- A few private validation clauses bind a `field` argument they do not use in the fast path, so `mix compile` emits unused variable warnings. Cosmetic, not behavioural.
