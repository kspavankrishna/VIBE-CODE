# Data Residency Route Planner

Your AI gateway picks the cheapest model endpoint and quietly ships a German customer's PII to a US region with a training-enabled endpoint and 30 day retention. Nobody notices until the audit. This is a single file Python planner that runs the legal rules first and the cost math second, and emits an auditable JSON decision for every inference request.

**Language:** Python | **Lines:** 792 | **Added:** 2026-05-29

## What this solves

Routing logic for LLM inference starts simple and rots fast. Day one it is a dict mapping tenant to region. Six months later it is a YAML file, three feature flags and a fallback branch somebody added during an incident. The cost optimiser and the compliance rules live in different files, owned by different people, and neither knows the other exists. The failure mode is not a crash. It is a request that succeeds, returns a good answer and violates a contract.

Concretely: a PHI request from a US clinic lands on an EU route because the EU route was cheaper that minute. A PCI payload goes to a provider whose retention is 30 days when the contract says 1. A fallback fires during a regional outage and drops the training_disabled requirement, because it was written as a plain "try the other region" branch. All of these look like a normal 200 in your logs. You find out when a customer's security team asks for evidence, or when a data protection officer asks which region processed a given request on a given day and you cannot reconstruct it. The bill is a contract renegotiation, a breach notification or a failed SOC 2 or HIPAA review.

This file makes the ordering explicit. Hard rules run as a filter before any scoring, so a disallowed route is never in the ranking at all. A route that wins on cost, latency and carbon is discarded outright if it fails residency, certification, retention, training use, tenant scoping, context size or capacity. Rejections come back with reasons attached, and every routed decision carries a SHA-256 evidence hash.

## Why I built it

Plenty of LLM routers exist. Almost all treat compliance as a tag filter bolted onto a load balancer: a label match, maybe a region allowlist, then straight into latency and price. That holds until the rules stop being one dimensional. Real residency policy is a cross product of user country, data class, tenant contract, provider certification, retention ceiling and training use, and the answer differs per request, not per deployment. A policy engine like OPA can express it, but then you own a second runtime and a bundle pipeline, and you still write the scoring half yourself.

I wanted it in one auditable file with zero dependencies, droppable into an AI gateway, a FastAPI middleware, a CI policy gate or a batch replay job without adding infrastructure.

## When to use it

- One AI product serving EU and US customers, where the same prompt must land in different regions depending on who sent it.
- A contract says PHI never leaves US soil and is never used for training, and you have to prove it per request.
- Your gateway has a cost optimiser and you want a hard guarantee it cannot pick an illegal route to save money.
- You are replaying production traffic against a proposed policy change to see what becomes unroutable.
- A CI gate that fails the build when a new route or policy edit strands a class of requests.
- An auditor asks which provider, region and retention bucket handled request X.

## How it works

The core is `DataResidencyRoutePlanner`, built from a JSON config with a `routes` array and an optional `policy` object. Both parse into frozen dataclasses: `Route`, `Policy` and `InferenceRequest`. Keys are read in both snake_case and camelCase via `get_any`, string sets pass through `text_set` which lowercases and accepts a list or a comma separated string, and numbers pass through `parse_float` and `int_value`, which reject booleans, non finite values and anything under a stated minimum. Bad config raises `PlannerConfigError` at load time, not at request time.

`decide()` runs two phases. Phase one is `_reject_reasons`, a pure filter over enabled state, health against `policy.min_route_health` (default 0.65), provider allowlists and denylists at policy and request level, model and region denylists, tenant scoping, `max_context_tokens`, remaining capacity, `allowed_data_classes`, training use for anything in `SENSITIVE_CLASSES` (pii, phi, pci, secrets, biometric, financial, health), certifications, residency and retention. It also applies the deadline: estimated latency is `latency_p95_ms + decode_ms_per_output_token * output_tokens`, checked against the request deadline or `policy.default_deadline_ms`. It collects every reason, not just the first.

Residency is set intersection, not string equality. Each route carries `residency_groups`, defaulting to its jurisdiction and region when unstated. `_residency_reasons` builds required sets from three independent sources: the request's `required_residency_groups`, the policy's `allowed_residency_by_country` for the user country and `allowed_residency_by_data_class` per data class. A route must intersect every one of them. `_missing_certifications` works the same way, unioning request level requirements with `required_certifications_by_data_class`. Retention takes the minimum over matching data classes, so a request tagged both pii and pci gets the tighter ceiling.

Phase two scores only survivors. `_score_candidates` normalises latency, cost, carbon and headroom across the eligible set with min max scaling, via `lower_is_better` and `higher_is_better`, which both collapse to 1.0 when candidates are identical so a lone eligible route does not score zero. Residency affinity is the fraction of desired groups the route covers, and health passes through raw. The weighted sum uses `DEFAULT_WEIGHTS` (latency 0.28, cost 0.22, headroom 0.18, carbon 0.14, residency 0.12, health 0.06), normalised to sum to one at policy load, then multiplied by the route's own `weight`. Capacity is a greedy reservation: `_reserved_tokens` per route feeds the headroom figure and selection adds the request's tokens unless `reserve_capacity=False`, so a JSONL batch spreads instead of piling onto one route.

Ties break deterministically. `stable_tiebreaker` hashes the sticky key (or request id) with the route id using blake2b at 8 byte digest size and scales the result by 1e-9: too small to overturn a real scoring gap, big enough to split exact ties, and stable across processes, so a sticky key keeps landing on the same route with no shared state. Ordering is descending score, then latency, then cost, then route id. Output is one JSON object per request: status, versions, tenant, the selected candidate with its components and estimates, up to `max_alternatives` runners up, the `hard_rejections` map and an `evidence_hash` computed as SHA-256 over canonical JSON of the policy version, the public request shape and the public route shape. Request metadata is never echoed, only folded into `metadata_hash`, so decisions log without carrying payload content.

## Usage

```bash
# Print a production shaped sample config to start from
python3 DataResidencyRoutePlanner.py --sample-config > policy.json

# Run the built in assertions
python3 DataResidencyRoutePlanner.py --self-test

# Plan a JSONL stream of requests
echo '{"request_id":"r1","tenant":"alpha","user_country":"DE","data_classes":["pii"],"input_tokens":1200,"output_tokens":400}' \
  | python3 DataResidencyRoutePlanner.py --config policy.json --pretty

# From a file, without consuming capacity (each request scored independently)
python3 DataResidencyRoutePlanner.py --config policy.json --requests traffic.jsonl --no-reserve
echo "exit code: $?"   # 0 if every request routed, 2 if any was unroutable
```

As a library:

```python
from DataResidencyRoutePlanner import DataResidencyRoutePlanner, InferenceRequest

planner = DataResidencyRoutePlanner.from_config(config_dict)
request = InferenceRequest.from_mapping({
    "tenant": "clinic",
    "user_country": "US",
    "data_classes": ["phi"],
    "input_tokens": 1800,
    "output_tokens": 700,
    "sticky_key": "session-9f2",
})
decision = planner.decide(request, reserve_capacity=True)
if decision["status"] == "routed":
    endpoint = decision["selected"]["route"]["endpoint"]
```

## Notes

- Standard library only. No third party packages, no network calls, no clock. It plans, it does not dispatch: you take the selected route's `endpoint` and make the call yourself.
- Capacity reservation is in memory, per planner instance and per process. No sliding window, no decay. It models one planning batch, not a distributed rate limiter, and parallel workers each think they own the full capacity.
- `health` is a number you supply, not something the planner probes. Token counts are caller supplied too, so cost, carbon, latency and capacity are only as good as your estimates.
- `preferred_regions` is parsed and echoed in the evidence payload but never affects filtering or scoring. Route `tags` are carried for audit, not matched on. Express preference through `required_residency_groups` or route weights.
- Exit code 2 covers both "a request was unroutable" and "config or request parse failed", so the code alone cannot tell them apart. Parse failures print to stderr prefixed with `DataResidencyRoutePlanner:` and emit no decision lines.
- Residency is set intersection over strings you define. The file encodes no legal knowledge of GDPR, HIPAA or PCI-DSS. It enforces the policy you write, repeatably. Getting that policy right is still your job.
- `--self-test` is two end to end assertions over the sample config, not a test suite.
