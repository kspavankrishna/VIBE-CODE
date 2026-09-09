# Inference Lease Allocator

An LLM inference request arrives and something has to decide, right now, which provider runs it, or whether it should wait, or whether it should be refused outright. This is that decision, written in OCaml as one pure function over a provider list, a tenant budget and a request.

**Language:** OCaml | **Lines:** 724 | **Added:** 2026-07-02

## What this solves

Most AI gateways route on one number. Cheapest endpoint, or lowest p50, or round robin over a static list. That works until a real constraint bites. A tenant burns its monthly spend by 11am because nothing checked the running total before dispatch. A request carrying regulated health data lands in a region the contract never permitted, and nobody notices until an audit. A provider sits at 30 percent error rate and the router keeps feeding it traffic. An interactive request with a 10 second deadline queues behind batch work and returns nothing.

The costliest failure is the silent one. The request is accepted, money is spent, the context window is filled, and only then does the provider reject it for a missing JSON schema mode or an output cap that was 4k when you needed 16k. You paid for the prompt tokens and got an error. The second costliest is the unexplainable one. Someone asks why `req_8814` went to the EU endpoint instead of the US one, and the answer is a grep through unstructured logs. Without a decision record attached to the admission, that question has no answer.

This file makes the admission decision explicit and total. Every request produces exactly one of three outcomes: `Admit` with a lease, `Queue` with a retry ticket, or `Reject` with a reason and a per provider diagnostic list. Nothing is admitted before model, features, context size, output cap, residency, data class, cooldown, capacity, health, cost, token quota, carbon and deadline have all been checked against that specific provider.

## Why I built it

Existing routers sit in the wrong layer. Load balancers know about connections, not token budgets. Cost dashboards tell you what you spent yesterday, not whether this request should be allowed today. Policy engines express residency rules but do not know that a 128k context window and a 16k output cap are two different limits failing two different ways. So the checks end up scattered across a proxy config, a middleware, a Terraform variable and an oral tradition.

I wanted one function, stdlib only, no floating point, that takes the whole state and returns the whole decision. Small enough to read in an afternoon, strict enough to sit in front of a production gateway, deterministic enough to replay yesterday's traffic against a changed policy and diff the outcomes.

## When to use it

- Several teams share one budget and one set of provider keys behind an internal AI gateway.
- You need proof that a regulated payload never left an approved jurisdiction.
- Interactive and batch traffic share endpoints, and the interactive requests keep missing deadlines.
- Your fleet is heterogeneous: different context limits, tool calling support, prices and carbon intensity.
- You want to test a policy change offline by replaying request records through `allocate` and diffing decisions.
- Every admission needs a stable id and structured fields you can ship into an audit table.

## How it works

The entry point is `allocate : provider list -> tenant_state -> request -> decision`. `validate_request` runs first and collects field problems (empty `request_id`, non positive `max_completion_tokens`, a `deadline_ms` not after `now_ms`). Any problem short circuits to `Reject` with `InvalidRequest`. Then tenant identity is matched, and if `tenant.active_leases >= tenant.concurrency_limit` the request queues immediately, because no provider choice fixes a tenant level cap.

Each provider goes through `evaluate_provider`, a fixed order chain of hard gates returning `Ok candidate` or `Error provider_diagnostic`. The order is cheapest first: provider field validation, tenant mismatch, exact model match, `requires_json_schema` and `requires_tool_calls`, `total_requested_tokens` against `max_context_tokens`, completion tokens against `max_output_tokens`, `residency_allowed`, `data_class_allowed`, cooldown, capacity, health. Only then cost, then carbon, then latency. A provider that fails on jurisdiction never pays for the arithmetic.

Residency comes from `routing_mode`. `RegionalOnly` requires the provider region to be in `allowed_regions`, `AllowEquivalentJurisdiction` accepts a region or a jurisdiction match, `AllowAnyRegion` skips the geographic check. On top of that: if `data_class` is `Regulated _` the request must carry at least one residency hint, otherwise every provider is blocked. Unconstrained regulated traffic fails closed. Acceptance is ranked through `data_class_rank`, so a provider accepting `Confidential` implicitly accepts `Internal` and `Public`, while `Regulated` never falls out of the ranking. `Regulated "*"` accepts any regime; any other `Regulated` entry must match the regime string exactly.

Money and carbon math is integer only. Prices are `int64` micro USD per million tokens and `estimated_cost_microusd` uses `ceil_div_i64`, so rounding always goes against the caller. `estimated_carbon_mg` runs total tokens times `millijoules_per_token` times `carbon_g_per_kwh` through a 3,600,000 divisor to land in milligrams. `estimated_latency_ms` starts from p95, adds a queue penalty when no lease slots are open and a decode penalty of 15ms per 128 completion tokens, then gets checked against real slack, `deadline_ms - now_ms`. A provider that cannot finish in time is rejected with `DeadlineTooTight` rather than admitted and hoped for.

Survivors are scored by `score_candidate`, lower wins. Weights come from `priority`: `Critical` is (7,1,1) on latency, cost and carbon, `Interactive` is (4,2,2), `Bulk` is (1,4,5). Two terms push traffic off hot and flaky endpoints: a load term from active leases plus queue depth over parallel capacity, and an error term of `error_rate_ppm / 50`. `compare_candidate` breaks ties on latency, then cost, then `provider_id`. No randomness, no clock read anywhere: `now_ms` arrives on the request, so identical inputs give the identical winner and the identical `lease_id`, a DJB2 style `stable_hash` over tenant, request id, idempotency key, provider id and `now_ms`.

When nothing survives, the diagnostics decide. If any reason is queueable (`TenantConcurrencyFull`, `ProviderCapacityFull`, `ProviderCoolingDown`) and no budget or carbon cap was hit, the request gets a `Queue` ticket whose `retry_after_ms` is the earliest provider cooldown, or now plus a 250ms floor. If a budget was exceeded, queueing is suppressed and the answer is a straight `Reject`, because waiting will not create money. State updates stay separate and pure: `apply_lease` returns a new tenant and provider list with cost subtracted, tokens reserved and lease counters incremented, so you commit or discard at your own consistency boundary.

## Usage

```bash
# compile and run the built in assertions
ocamlopt -o allocator InferenceLeaseAllocator.ml && ./allocator --self-check
# InferenceLeaseAllocator self-check passed

# or run it straight through the toplevel
ocaml InferenceLeaseAllocator.ml --self-check
```

```ocaml
let tenant = default_tenant "tenant-a" in
let req = default_request ~request_id:"req_1" ~now_ms:1_000_000 () in
let primary = default_provider () in
let backup =
  { (default_provider ~provider_id:"edge-b" ~region:"eu-west-1" ()) with
    jurisdiction = "EU"; p95_latency_ms = 1_800 }
in

match allocate [ backup; primary ] tenant req with
| Admit lease ->
    print_endline (decision_to_logfmt (Admit lease));
    let tenant', providers' = apply_lease tenant [ backup; primary ] lease in
    dispatch_to lease.lease_provider_id lease.lease_id tenant' providers'
| Queue ticket ->
    retry_after ticket.retry_after_ms ticket.position_hint
| Reject rejection ->
    List.iter print_endline (diagnostics_to_logfmt rejection.diagnostics);
    fail_with (reason_to_string rejection.rejected_reason)
```

`admitted_provider_id` and `rejected_reasons` are helpers for metrics and tests. `default_provider`, `default_tenant` and `default_request` take optional labelled arguments and keep fixtures short.

## Notes

- Stdlib only. No opam packages, no dune file, no C stubs. A bare `ocamlopt` invocation builds it. Everything is exported with no `.mli`, so write an interface file if you vendor it.
- `let () = ...` at the bottom runs on link and reads `Sys.argv`. Linking into an existing binary runs that check at startup, harmless unless your first argument is `--self-check`.
- It allocates leases but does not track them. No expiry sweeper, no release function, no store. `expires_at_ms` is advisory and reconciling unused leases is on you.
- `allocate` is `O(n)` over the providers plus a sort of the survivors. Fine for tens of providers, not tens of thousands.
- Latency, cost and carbon are estimates from static provider fields. Nothing feeds real outcomes back, so `p95_latency_ms`, `error_rate_ppm` and `queue_depth` must be refreshed by whatever observes your fleet. Token counts are inputs too: there is no tokenizer here.
- The health cutoff is a constant, `error_rate_ppm >= 250_000`. No gradual degradation, no circuit breaker, no half open probing. Cooldowns arrive externally via `cooldown_until_ms`.
- `self_check ()` covers four paths: normal admission, tenant concurrency queueing, regulated data with no residency hints, and a cost cap nothing can satisfy. Smoke test, not a test suite.
