# Agent Restart Storm Breaker

An Erlang/OTP admission controller that stops one bad AI agent task from taking down every other task running next to it, by replacing a plain supervisor's shared restart counter with per-task fingerprint quarantine and a separate fleet-wide breaker for real outages.

**Language:** Erlang | **Lines:** 451 | **Added:** 2026-09-11

## What this solves

If you run concurrent AI agent tasks (tool calls, sub-agent workers, batch inference jobs) under a normal OTP `supervisor`, you already get "let it crash" for free: a worker dies, the supervisor restarts it, life goes on. Except OTP supervisors don't track failures per child — they track one shared counter across the whole supervisor, `intensity` failures inside `period` seconds. That counter does not know or care which child is failing.

Put that together with real agent traffic and you get a specific, ugly failure mode: one "poison" task — a prompt that reliably crashes your tool-call parser, a malformed function-call payload the JSON decoder chokes on every single time — restarts over and over. Meanwhile a normal background failure rate from completely unrelated tasks (a flaky network blip here, a slow response there) is also ticking against that same shared counter. Eventually the combined count crosses `max_restart_intensity`, and the supervisor's response is to give up entirely: it terminates every child, itself, and propagates the exit up the tree. One bad payload just took down every in-flight task that happened to be running alongside it, including ones that had nothing wrong with them.

The opposite failure mode is just as real: sometimes lots of unrelated tasks really do start failing together, because the upstream LLM provider is down or rate limiting everyone. In that case, restarting each task individually and hoping is exactly the wrong move — you want to stop hammering the provider fleet-wide until it recovers, not spin up a hundred fresh retries into an outage.

`AgentRestartStormBreaker` handles both cases with two independent mechanisms instead of one shared counter:

- **Per-fingerprint quarantine** — a specific task identity (you decide what identity means: tenant + prompt hash, tool + argument hash, whatever "the same thing" means for your system) that keeps failing gets locked out for a cooldown period, without touching anyone else's tasks.
- **Fleet-wide storm breaker** — a classic closed → open → half-open circuit breaker, but tripped by *how many distinct fingerprints* are failing in a short window, not by one endpoint's error rate. That's the signal that separates "one bad payload" from "the provider is down for everybody."

## Why I built it

I kept seeing the same pattern described for other languages in this repository — circuit breakers, retry budgets, admission ledgers — all guarding a single dimension: cost, concurrency, or one endpoint's health. None of them were solving the specific OTP problem, which is that `supervisor`'s own crash-escalation semantics are the wrong shape for a fleet of independent, untrusted-input agent tasks. That's a genuinely Erlang problem, not a generic one, and it deserves an Erlang-native answer built out of primitives BEAM gives you for free: `spawn_monitor` for cheap isolated processes, ETS for a lock-free shared ledger, and ordinary message passing for reporting outcomes back without ever linking task code to the thing making admission decisions.

The design constraint I cared about most: the breaker itself must be structurally incapable of crashing because of a task it admitted. Not "unlikely to crash" — incapable. That's why every worker is `spawn_monitor`'d, never `spawn_link`'d, and why even a caller-supplied classifier function that throws garbage is caught and downgraded to a safe default rather than trusted.

## When to use it

Use this in front of any pool of concurrent, independently-triggered AI agent or tool-call tasks where:

- Task inputs come from many different sources (tenants, users, upstream queues) and you cannot guarantee none of them is malformed or adversarial.
- A single bad input crashing repeatedly must not affect anyone else's in-flight work.
- You also need protection against a correlated, provider-side outage — not just isolated bad payloads — without writing two separate breaker implementations.
- You're already living in a BEAM system (Elixir or Erlang) and want the isolation boundary to be a real OS-level-cheap process, not a thread pool or an async task with shared exception state.

Don't reach for this if you only have one task type running serially, or if your supervisor already only ever supervises tasks that share one true fate (in that case OTP's default shared-intensity behavior is *correct*, not a bug — a database connection pool going down really should take down everything depending on it).

## How it works

The module is a single `gen_server` (`'AgentRestartStormBreaker'`, quoted because it's PascalCase, which Erlang allows for any atom) plus a small supervisor (`'AgentRestartStormBreakerSup'`) that keeps it running. The gen_server never executes task code itself — it only makes admission decisions and tracks outcomes. Actual task execution happens in a throwaway worker process spawned by the *caller*.

**Admission (`submit/3,4`)** — the caller calls `gen_server:call(Server, {admit, Fingerprint})`. The `handle_call({admit, Fingerprint}, ...)` clause checks two independent things:

1. `fingerprint_quarantined/3` looks the fingerprint up in the `ledger` ETS table (`{Fingerprint, FailCount, WindowStartMs, LastClass, QuarantinedUntilMs}`). If it's still inside its quarantine window, admission is denied immediately with `{error, {quarantined, RemainingMs}}` — no worker process is even spawned.
2. `storm_gate/2` checks the fleet-wide breaker state (`closed | open | half_open`, held in `#state.storm_state`). In `open`, everything is denied with `{error, {storm_cooldown, RemainingMs}}`. In `half_open`, a bounded number of "probe" admissions (`probe_limit`, default 3) are let through to test the water; beyond that, still denied. In `closed`, admission proceeds normally.

If admitted, `submit/4` gets back a `Ticket` map (`#{fingerprint => FP, probe => IsProbe}`) and spawns the actual work: `spawn_monitor(fun() -> run_worker(...) end)`. Critically, `timer:kill_after(Timeout, Pid)` is armed against that worker right away — so even if the calling process itself dies before collecting a result, the worker is still bounded and gets killed, rather than leaking forever waiting for a caller that will never read its mailbox.

**Isolated execution (`run_worker/6`)** — runs `TaskFun()` inside a `try ... catch Type:Reason -> ...`. On success it casts `{report, Ticket, success}` back to the breaker and messages the result to the caller via a private reference (`Tag`), not the monitor reference — the monitor is only ever used to detect a worker that died *without* reporting. On failure it classifies the reason with `safe_classify/2` (which downgrades any classifier exception or non-atom return to `crash`) and reports `{failure, Class, Reason}`.

**The fallback path (`await_result/6`)** — if the worker is killed by `timer:kill_after` or dies some other way `try/catch` can't intercept, the caller receives a `'DOWN'` message instead of a tagged result. It classifies the raw exit reason itself and reports the failure on the worker's behalf, so the ledger never silently misses a hard-killed task. This is also where the important little correctness detail lives: `classify_reason(killed) -> timeout`. `timer:kill_after` always kills with reason `killed`, and if that were left to fall through to the generic `crash` bucket, a task that was merely slow (waiting on a stalled connection) would get the long, `crash`-grade quarantine meant for deterministic poison payloads instead of the shorter `timeout` one.

**Classification and policy (`default_classifier/1`, `classify_reason/1`, `policy/1`)** — reasons are sorted into `timeout | rate_limited | tool_error | invalid_output | crash`, and each classification has its own `policy/1` entry: `max_failures`, `window_ms`, `quarantine_ms`, and `storm_weight`. `rate_limited` is special — `max_failures => infinity` means it can never quarantine a fingerprint by itself (a 429 is the provider's fault, not the payload's), but it carries the highest `storm_weight` (3), because many fingerprints getting rate-limited together in a short window is exactly the systemic-outage signal the fleet breaker exists to catch.

**The ledger (`ledger_record_failure/4`, `ledger_clear_or_decay/3`, `sweep_ledger/2`)** — failures increment a per-fingerprint counter inside a rolling `window_ms`; crossing `max_failures` sets `QuarantinedUntilMs`. A success doesn't wipe the slate clean — it decays the count by one via `ledger_clear_or_decay/3`, so a fingerprint that fails occasionally over a long session isn't treated as spotless after one lucky retry, and the entry is only deleted once its count reaches zero. A periodic `sweep` message (self-scheduled every `?SWEEP_INTERVAL_MS`, handled in `handle_info(sweep, State)`) purges anything that's been idle past `?STALE_AFTER_MS` without ever coming back — otherwise the ETS table would grow by one entry for every fingerprint ever seen, for the lifetime of the process.

**The storm breaker (`storm_record_failure/4`, `trip_storm/2`, `storm_on_failure/2`, `storm_on_success/2`)** — every failure is recorded into a sliding window (`#state.storm_events`, a list of `{Timestamp, Fingerprint, Weight}`), deduplicated per fingerprint via `upsert_latest/4` so one repeatedly-failing (and soon-to-be-quarantined) fingerprint can only ever occupy one slot — it cannot manufacture a storm on its own. When the weighted sum of distinct fingerprints failing inside `storm_window_ms` crosses `storm_threshold`, `trip_storm/2` opens the breaker for `storm_cooldown_ms`. Once that expires, `storm_gate/2` transitions to `half_open` and starts admitting probes. A probe that fails reopens immediately with the cooldown doubled (capped at `?DEFAULT_STORM_MAX_COOLDOWN_MS`); enough consecutive probe successes (`probe_limit`) closes the breaker and resets the storm window.

**Observability and manual override (`stats/1`, `reset_fingerprint/2`)** — `stats/1` returns a snapshot map: `storm_state`, `storm_remaining_ms`, running `counters` (admitted / denied_quarantine / denied_storm / succeeded / failed), and the currently quarantined fingerprint list. `reset_fingerprint/2` is the operator escape hatch — once a human has actually fixed the bug behind a poison fingerprint, they shouldn't have to wait out `quarantine_ms` to find out it's fixed.

## Usage

```erlang
{ok, _Pid} = 'AgentRestartStormBreakerSup':start_link(),
Server = 'AgentRestartStormBreaker',

Fingerprint = {TenantId, erlang:phash2(NormalizedPrompt)},

case 'AgentRestartStormBreaker':submit(
       Server,
       Fingerprint,
       fun() -> call_agent_tool(ToolName, Args) end,
       #{timeout => 15000,
         classifier => fun my_reason_classifier/1}) of
    {ok, Result} ->
        handle_result(Result);
    {error, {quarantined, RemainingMs}} ->
        %% This exact task identity has failed repeatedly; don't retry it
        %% yet, surface it to whoever owns that tenant/prompt instead.
        defer(Fingerprint, RemainingMs);
    {error, {storm_cooldown, RemainingMs}} ->
        %% Fleet-wide breaker is open; the provider or shared dependency
        %% looks like it's down for everyone, not just this task.
        backoff_everything(RemainingMs);
    {error, {Class, Reason}} ->
        log_failure(Fingerprint, Class, Reason)
end,

'AgentRestartStormBreaker':stats(Server).
%% => #{storm_state => closed, storm_remaining_ms => 0,
%%      counters => #{admitted => .., denied_quarantine => .., ...},
%%      quarantined_count => 0, quarantined => [], ledger_size => 0}
```

`submit/3` is the same call with default options (`timeout => 30000`, and `default_classifier/1` as the classifier). Wire `'AgentRestartStormBreakerSup'` into your own application's supervision tree as a normal child instead of calling `start_link/0` directly if you already have a top-level supervisor.

## Notes

- Workers are always `spawn_monitor`'d, never `spawn_link`'d — this is the whole point. A worker's exit only ever produces a `'DOWN'` message, never an exit signal, so it structurally cannot propagate to the breaker or to the calling process.
- The hard per-task timeout is enforced with `timer:kill_after/2` against the worker process directly, so it holds even if the caller that submitted the task dies before the task finishes — nothing is left waiting forever on a caller that will never read a result.
- The ETS ledger table is created unnamed and kept in `#state.ledger`, deliberately not as a `named_table` derived from a dynamic name — atoms in Erlang are never garbage collected, so building atom names out of anything but fixed, startup-time literals is worth avoiding on principle in a long-running system.
- Concurrent probes during `half_open` can interleave: a success that lands after a sibling probe's failure already reopened the breaker simply counts toward the *next* half-open cycle rather than being discarded. This is intentional and safe — `storm_on_failure/2` always resets `probe_successes` to zero on reopen, so a stray late success can never falsely close the breaker early.
- This module makes admission and isolation decisions; it deliberately does not implement retry scheduling or exponential backoff for the caller. `{error, {quarantined, RemainingMs}}` and `{error, {storm_cooldown, RemainingMs}}` both tell you exactly how long to wait — what you do with that (drop the task, queue it, page someone) is a policy decision that belongs to the caller, not to the breaker.
- Tested against Erlang/OTP 25 with `erlc` and a scripted `gen_server`-level test run covering: quarantine after repeated crashes, the `reset_fingerprint/2` override, successful-task decay, `killed`-as-`timeout` classification under a hard timeout, a simulated storm tripping the fleet breaker and denying even a never-seen fingerprint, and the half-open probe cycle closing the breaker again after enough consecutive successes.
