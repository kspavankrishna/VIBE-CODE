# Retry Amplification Analyzer

A Clojure tool that reads your service call graph and retry settings, then tells you how much load a single outage will really create, before production tells you the hard way. It finds retry storms, nested retries, deadline inversions and unsafe retry policy from one EDN file and can fail a CI job.

**Language:** Clojure | **Lines:** 408 | **Added:** 2026-11-03

## What this solves

Every team sets retries one service at a time. The gateway retries twice, the checkout service retries three times, the payment client retries twice. Each setting looks sensible alone. Together they multiply: one user request that hits a dead database becomes 3 x 4 x 3 = 36 attempts against that database. This is the classic retry storm, and it is the reason a small blip turns into an hours long outage. The extra load keeps the struggling service down, so the retries never stop. People call that a metastable failure.

Nobody owns the product of retries across a call chain. Code review sees one client config at a time. Load tests run on a healthy system. The first time anyone computes the real number is in the incident review.

This tool computes it. Give it the topology and it answers:

- How many requests will each service see when everything is fine, when every retry fires, and when your declared retry budgets hold?
- Which service goes over its capacity if something below it fails?
- Which paths contribute most to the storm?
- Where does a caller time out while the callee keeps working, so the work is wasted?
- Which retries are unsafe: not idempotent, no jitter, no backoff, no budget, or stacked on top of another retry layer?
- Is there a call cycle? With retries, a cycle has no finite worst case.

## Why I built it

I kept seeing the same post mortem with different service names. Someone adds a retry to a client library because a call was flaky. Six months later a downstream deploy goes bad and the whole fleet falls over in under a minute. The fix is always the same conversation: who retries, how many times, at which layer. The argument is hard to win because nobody has a number.

Service mesh dashboards show you what happened. Chaos tools break things on purpose. Neither one tells you in a pull request that this new `:retries 3` on the checkout to payments edge pushes the ledger to 27 times its normal load. I wanted a check that sits next to the config, runs in a second and has no dependencies, so it can live in any repo.

Clojure suited the job. A topology is data, the analysis is a handful of pure functions over that data, and the report is data again. No framework, no service to run. I used only `clojure.core`, `clojure.edn` and `clojure.string`, so it runs with a plain `clojure -M` or under Babashka.

## When to use it

- You are adding or changing retry, timeout or fanout settings and want to know the blast radius.
- You are reviewing an architecture and want an honest worst case number for each service.
- You are sizing capacity and need to know whether you plan for healthy load or outage load.
- You want a CI gate that stops someone from adding a retry to a non idempotent call.
- You are writing a post mortem and want to show how the multiplication happened.

It is not a load tester and it does not watch live traffic. It works on the numbers you declare. If your declared timeouts and retries are wrong, the answer is wrong, so keep the file generated from real config where you can.

## How it works

The input is one EDN map with an `:entry` service, an `:entry-rps` and a `:services` map. Each service can have `:capacity-rps`, `:mode` (`:sequential` or `:parallel`), `:local-ms` and a `:calls` vector. Each call has `:to`, `:timeout-ms`, and optionally `:fanout`, `:retries`, `:backoff-ms`, `:backoff-cap-ms`, `:jitter`, `:idempotent` and `:budget-ratio`. `ExampleTopology.edn` in this folder shows every field.

**Validation.** `validate` returns a vector of every problem it finds, not just the first. Unknown callees, negative retries, missing timeouts and a bad entry service all come back together, and `analyze` returns `{:errors [...]}` instead of throwing.

**Graph walk.** `edges-of` flattens the services into normalised edges with defaults filled in. `walk` does a depth first traversal from the entry service. It gives back a post order, which has callees before callers, and any cycles it meets. If there is a cycle, `analyze` stops there and reports a critical `:cycle` finding, because load numbers would be infinite.

**Three load models.** `multipliers` pushes load through the graph in caller first order using an edge factor:

- `normal-factor` is just `:fanout`. This is healthy traffic.
- `storm-factor` is `fanout * (1 + retries)`. This assumes every attempt fails and every retry is spent. Because the retrying service repeats its own downstream calls on each attempt, factors multiply down the chain.
- `budgeted-factor` is `fanout * (1 + min(retries, budget-ratio))`. A retry budget caps extra attempts as a share of first attempts, so a ten percent budget means at most 1.1 x the calls however many retries are configured.

Each multiplier is scaled by `:entry-rps`, so the report speaks in requests per second.

**Latency and orphaned work.** `backoff-total-ms` sums the exponential sleeps with the cap applied to each one. `edge-worst-ms` is `(retries + 1) * timeout` plus that backoff. `node-worst-times` rolls these up from the leaves. A `:sequential` service adds its calls up, multiplying by fanout. A `:parallel` service takes the slowest one. `deadline-findings` then compares each edge: if the callee can run longer than the caller's per attempt timeout, the caller has moved on while the callee keeps burning capacity. That is a `:deadline-inversion`.

**Retry layering.** `retry-depths` counts how many retrying edges sit below each node. `policy-findings` uses it to flag `:nested-retry`, where a retrying edge points at a service that also retries.

**Findings.** The rest of `policy-findings` and `load-findings` produce typed findings with a severity, a location, a message and a fix:

- `:retry-storm-overload` (critical): healthy load fits, but even the budgeted worst case exceeds `:capacity-rps`.
- `:already-overloaded` (critical): healthy load already exceeds capacity.
- `:depends-on-retry-budget` (medium): full outage load exceeds capacity and only the declared budgets save you. Budgets must be enforced in code.
- `:amplification-high` and `:amplification-warn`: storm load over healthy load crosses `:fail-amp` (default 10) or `:warn-amp` (default 4).
- `:non-idempotent-retry` (high), `:idempotency-undeclared`, `:no-jitter`, `:no-retry-budget`, `:zero-backoff`.
- `:unreachable` (info) for services nobody calls.

For overload findings, `heaviest-paths` enumerates entry to target paths, pruned to nodes that can actually reach the target, ranked by storm multiplier. It is bounded by `:max-steps` so a huge mesh cannot hang the run, and it reports when the search was truncated.

**Report and exit code.** `analyze` returns the nodes table, the sorted findings and `:max-severity`. `render-text` prints it. `run` is the CLI entry point and returns the exit code.

Honest limits: the storm model is a worst case where every attempt fails, not a forecast. It treats a retry as repeating the entire downstream subtree. Real clients that fail fast on an open circuit breaker will do better. The point is to know the ceiling.

## Usage

Run it against the example:

```
clojure -M RetryAmplificationAnalyzer.clj ExampleTopology.edn
```

Flags:

```
clojure -M RetryAmplificationAnalyzer.clj topology.edn --fail-on high --format text
clojure -M RetryAmplificationAnalyzer.clj topology.edn --format edn
clojure -M RetryAmplificationAnalyzer.clj topology.edn --warn-amp 3 --fail-amp 6
```

Exit codes: `0` when nothing reaches the `--fail-on` severity (default `high`), `1` when something does, `2` for a bad file or invalid topology. Severities are `info`, `low`, `medium`, `high` and `critical`.

From a REPL:

```clojure
(load-file "RetryAmplificationAnalyzer.clj")
(require '[retry-amplification-analyzer :as ra])
(def report (ra/analyze (clojure.edn/read-string (slurp "ExampleTopology.edn"))))
(println (ra/render-text report))
(map :id (:findings report))
```

On the example, the gateway sends 300 rps and checkout retries payments three times with no jitter. The report shows payments at x12 and the ledger at x27 over healthy load, flags the non idempotent payments retry, and shows that the checkout to payments call can run 1650 ms inside a 600 ms caller timeout.

Tests are in `RetryAmplificationAnalyzerTest.clj`. Run them from this folder:

```
clojure -J-Danalyzer.dir=. -M RetryAmplificationAnalyzerTest.clj
```

## Notes

- Only `clojure.core`, `clojure.edn` and `clojure.string` are used. EDN is read with `clojure.edn/read-string`, so a topology file cannot execute code.
- A call to the same callee twice is two edges with separate policies.
- `:retries` must be a non negative integer. `:budget-ratio` is between 0 and 1.
- When `:jitter` is anything other than `true` on a retrying edge, you get the `:no-jitter` finding. Same idea for `:idempotent`: `false` is a high finding, a missing value is a medium one.
- Where fanout is on a `:parallel` service the calls count as one wave in the latency maths but still multiply load.
- Generating the EDN from your real client configs is the best way to keep this honest. A stale file gives a confident wrong answer.
- Ideas I left out on purpose: circuit breaker modelling, hedged requests and adaptive concurrency limits. Each changes the maths and deserves its own model.
