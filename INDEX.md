# VIBE CODE Index

Every entry in this repository. One folder per idea. Each folder holds the source file and a README explaining what the code solves, why it exists and how it works.

**Entries:** 153  |  **Source files:** 156  |  **Languages used:** 27 of 50  |  **Lines of code:** 108,148  |  **Span:** 2026-04-03 to 2026-09-12

This file is the register the daily routine reads. It tallies the Language column, picks whichever language has the fewest entries and writes the next one in that language. Ties break in the canonical order below, so languages sitting at zero get filled first and the spread keeps evening out. Do not delete this file.

## House style

Every README in this repository follows these rules. Match them.

- No em dashes. Use a comma, a full stop or a colon instead.
- No Oxford commas. Write "a, b and c" not "a, b, and c".
- Minimal hyphens. Prefer "production ready" over "production-ready".
- Plain direct language, varied sentence rhythm, sounding like a working engineer explaining his own code.
- No emoji, no throat clearing. The first sentence states the problem.
- Banned: delve, leverage, robust, seamless, cutting-edge, landscape, realm, elevate, unlock, tapestry, game-changer, "in today's fast-paced".
- Never describe an API, function or flag that does not exist in the code.
- Never put a string in an example that could match a real credential pattern. GitHub push protection will reject the commit.

## Language coverage

Canonical order. A language at zero is next in line.

| # | Language | Ext | Entries |
|---:|---|---|---:|
| 1 | Python | .py | 12 |
| 2 | TypeScript | .ts | 12 |
| 3 | Rust | .rs | 8 |
| 4 | Go | .go | 6 |
| 5 | Bash | .sh | 8 |
| 6 | JavaScript | .js | 4 |
| 7 | Kotlin | .kt | 4 |
| 8 | Java | .java | 5 |
| 9 | C# | .cs | 6 |
| 10 | Swift | .swift | 5 |
| 11 | C++ | .cpp | 5 |
| 12 | Dart | .dart | 6 |
| 13 | Ruby | .rb | 5 |
| 14 | PHP | .php | 6 |
| 15 | Zig | .zig | 6 |
| 16 | Lua | .lua | 6 |
| 17 | Elixir | .ex | 9 |
| 18 | Scala | .scala | 4 |
| 19 | Haskell | .hs | 7 |
| 20 | OCaml | .ml | 7 |
| 21 | C | .c | 7 |
| 22 | R | .R | 4 |
| 23 | Julia | .jl | 6 |
| 24 | Nix | .nix | 4 |
| 25 | Erlang | .erl | 1 |
| 26 | Clojure | .clj | 1 |
| 27 | F# | .fs | 1 |
| 28 | Nim | .nim | 0 |
| 29 | Crystal | .cr | 0 |
| 30 | Gleam | .gleam | 0 |
| 31 | Mojo | .mojo | 0 |
| 32 | Odin | .odin | 0 |
| 33 | V | .v | 0 |
| 34 | D | .d | 0 |
| 35 | Perl | .pl | 0 |
| 36 | Raku | .raku | 0 |
| 37 | Groovy | .groovy | 0 |
| 38 | Objective-C | .m | 0 |
| 39 | PowerShell | .ps1 | 0 |
| 40 | SQL | .sql | 0 |
| 41 | Solidity | .sol | 0 |
| 42 | Fortran | .f90 | 0 |
| 43 | Ada | .adb | 0 |
| 44 | Racket | .rkt | 0 |
| 45 | Elm | .elm | 0 |
| 46 | PureScript | .purs | 0 |
| 47 | ReScript | .res | 0 |
| 48 | Tcl | .tcl | 0 |
| 49 | Haxe | .hx | 0 |
| 50 | CUDA | .cu | 0 |

## Topics

| Topic | Entries |
|---|---:|
| MCP and Agent Tooling | 31 |
| LLM and Inference | 24 |
| Cost and Quota | 20 |
| Streaming and Parsing | 19 |
| Infra and DevOps | 18 |
| Security and Supply Chain | 14 |
| Evals and Experiments | 13 |
| Data and Vector | 8 |
| Edge and Carbon | 6 |

## All entries

Sorted alphabetically. The routine appends new rows here and keeps this order.

| Entry | Language | Topic | Added | What it does |
|---|---|---|---|---|
| [AI Gateway Quota](AI%20Gateway%20Quota/) | Lua | Cost and Quota | 2026-04-16 | Lua gateway quota that reserves token and spend budget before an LLM call and settles real usage after |
| [AI SSE JSON Assembler](AI%20SSE%20JSON%20Assembler/) | C++ | Streaming and Parsing | 2026-04-23 | Reassembles fragmented AI SSE streams into complete validated JSON values with hard memory and depth caps |
| [AI SSE Resume Ledger](AI%20SSE%20Resume%20Ledger/) | Kotlin | Streaming and Parsing | 2026-05-20 | Bounded in memory SSE replay ledger for resumable LLM token streams with checkpoints and tool dedupe |
| [AI Stream SSE Reader](AI%20Stream%20SSE%20Reader/) | C# | Streaming and Parsing | 2026-04-14 | Single file C# Server Sent Events reader that joins multi-line data fields, skips heartbeats and caps event size |
| [Adaptive Micro Batcher](Adaptive%20Micro%20Batcher/) | Dart | Infra and DevOps | 2026-04-24 | Dart micro batcher that flushes on size, weight or latency and splits failed batches to isolate poison items |
| [Agent Aware Connection Pool](Agent%20Aware%20Connection%20Pool/) | Go | MCP and Agent Tooling | 2026-08-29 | Go database/sql admission layer using weighted fair queuing per agent plus an AIMD controller that resizes the pool |
| [Agent Budget Quorum](Agent%20Budget%20Quorum/) | Elixir | MCP and Agent Tooling | 2026-07-03 | Deterministic admission control for agent tool calls with fingerprint quorum, leases and a spend ledger |
| [Agent Consent Receipt Ledger](Agent%20Consent%20Receipt%20Ledger/) | Ruby | MCP and Agent Tooling | 2026-08-20 | Audits AI agent tool call logs to prove every risky call had a scoped, unexpired consent receipt |
| [Agent Context Shard Planner](Agent%20Context%20Shard%20Planner/) | Haskell | MCP and Agent Tooling | 2026-05-07 | Packs a code manifest into prompt-sized shards, keeping dependency cycles intact and boundaries deterministic |
| [Agent Egress Policy Gate](Agent%20Egress%20Policy%20Gate/) | Python | Security and Supply Chain | 2026-08-04 | Audits AI agent, MCP and CI egress traces against a policy for leaked secrets, rogue domains and budgets |
| [Agent Memory Reconciler](Agent%20Memory%20Reconciler/) | Clojure | MCP and Agent Tooling | 2026-09-11 | Delta-state CRDT memory store that merges concurrent multi-agent writes with causal tombstone GC |
| [Agent Restart Storm Breaker](Agent%20Restart%20Storm%20Breaker/) | Erlang | MCP and Agent Tooling | 2026-09-11 | Erlang admission control that quarantines poison agent tasks and trips a fleet-wide restart storm breaker |
| [Agent Rules Backdoor Scanner](Agent%20Rules%20Backdoor%20Scanner/) | Rust | Security and Supply Chain | 2026-06-01 | Dependency free Rust CI scanner that finds prompt injection and hidden backdoors in AI agent rules files |
| [Agent Run Continuity Fence](Agent%20Run%20Continuity%20Fence/) | Java | MCP and Agent Tooling | 2026-08-14 | Java CLI that turns agent transcripts and traces into a verified continuity packet and fails CI on unsafe resumes |
| [Agent Run Lease Table](Agent%20Run%20Lease%20Table/) | Rust | Infra and DevOps | 2026-05-15 | Single file Rust lease table with TTL leases, heartbeat renewal and fencing tokens, backed by one file |
| [Agent Run Provenance Gate](Agent%20Run%20Provenance%20Gate/) | Rust | MCP and Agent Tooling | 2026-08-08 | Audits JSON/JSONL AI agent traces for replayable provenance and fails CI when run ids, model pins, hashes, tool digests or approvals are missing |
| [Agent Run SLO Governor](Agent%20Run%20SLO%20Governor/) | TypeScript | Evals and Experiments | 2026-05-28 | Gates AI agent canary rollouts on paired bootstrap intervals for p95 latency, cost, error and quality |
| [Agent Stream Budget](Agent%20Stream%20Budget/) | Zig | MCP and Agent Tooling | 2026-07-16 | Zig CLI that reads JSONL LLM and agent traces, groups by session, and fails CI on token, cost, latency, stall or egress breaches |
| [Agent Token Spillway](Agent%20Token%20Spillway/) | Zig | Cost and Quota | 2026-06-15 | Single file Zig admission controller that accepts, defers or rejects LLM requests before tokens are spent |
| [Agent Tool Call Circuit Breaker](Agent%20Tool%20Call%20Circuit%20Breaker/) | Kotlin | MCP and Agent Tooling | 2026-09-02 | Cost weighted circuit breaker for LLM agent tool calls, with per tenant bulkheading and jittered backoff |
| [Agent Tool Retry Budget](Agent%20Tool%20Retry%20Budget/) | Elixir | MCP and Agent Tooling | 2026-05-24 | Elixir gate that scores agent tool call retries against attempt, elapsed and idempotency budgets |
| [Agent Worktree Pool Manager](Agent%20Worktree%20Pool%20Manager/) | Bash | MCP and Agent Tooling | 2026-08-30 | Crash-safe flock-guarded git worktree pool that hands isolated slots to parallel AI coding agents |
| [Batch Request Sharder](Batch%20Request%20Sharder/) | Haskell | LLM and Inference | 2026-05-07 | Deterministic Haskell sharder that packs LLM batch queues under token, byte, deadline and per-tenant limits |
| [Build Kit Cache Forensics](Build%20Kit%20Cache%20Forensics/) | JavaScript | Infra and DevOps | 2026-05-19 | Finds the first uncached BuildKit vertex behind a slow Docker build and the Dockerfile line that caused it |
| [Canary Evidence Spender](Canary%20Evidence%20Spender/) | R | Evals and Experiments | 2026-06-11 | Per-cohort Welch non-inferiority gate for LLM canary evals that plans and budgets the next samples by risk |
| [Carbon Aware Eval Planner](Carbon%20Aware%20Eval%20Planner/) | Dart | Edge and Carbon | 2026-05-24 | Dart CLI that schedules deadline bound AI eval and batch jobs into the cleanest and cheapest grid slots |
| [Carbon Aware Job Scheduler](Carbon%20Aware%20Job%20Scheduler/) | C# | Edge and Carbon | 2026-09-03 | Defers batch and GPU jobs to the lowest-carbon window before their deadline using a grid intensity forecast |
| [Carbon Credit Double Count Guard](Carbon%20Credit%20Double%20Count%20Guard/) | PHP | Edge and Carbon | 2026-09-08 | PHP 8 CLI and library that catches overlapping carbon credit serial ranges before a double sold trade closes |
| [Cardinality Circuit Ledger](Cardinality%20Circuit%20Ledger/) | Zig | Infra and DevOps | 2026-06-28 | Zig cardinality circuit breaker that caps distinct metric label values per window and buckets the overflow |
| [Causal Release Guard](Causal%20Release%20Guard/) | Julia | Evals and Experiments | 2026-07-07 | Stratified bootstrap canary analysis in Julia that returns a ship, hold or block release decision |
| [Complexity Regression Sentinel](Complexity%20Regression%20Sentinel/) | Rust | Infra and DevOps | 2026-08-28 | Rust CLI that diffs per function loop nesting between git refs and fails CI on O(n^2) regressions |
| [Composer Risk Ledger](Composer%20Risk%20Ledger/) | PHP | Security and Supply Chain | 2026-07-16 | Scores composer.lock supply chain risk offline and gates CI with JSON, Markdown or SARIF output |
| [Concurrent Token Window](Concurrent%20Token%20Window/) | Go | LLM and Inference | 2026-04-07 | Go rate limiter combining a token budget, a concurrency semaphore and a circuit breaker for token metered LLM APIs |
| [Context Window Optimizer](Context%20Window%20Optimizer/) | Python | LLM and Inference | 2026-04-09 | Scores LLM chat history by recency, role and query overlap, then evicts weakest turns to fit a token budget |
| [Context Window Packer](Context%20Window%20Packer/) | Python | LLM and Inference | 2026-04-09 | Packs chat history into an LLM context window, pinning critical messages and reserving output tokens |
| [Data Residency Route Planner](Data%20Residency%20Route%20Planner/) | Python | LLM and Inference | 2026-05-29 | Routes LLM inference to compliant endpoints, enforcing data residency and retention before cost scoring |
| [Dynamic Token Prioritizer](Dynamic%20Token%20Prioritizer/) | Python | LLM and Inference | 2026-04-06 | Ranks LLM tokens by rarity, position and semantic score so context overflow drops filler instead of the tail |
| [Edge Cache Entropy Audit](Edge%20Cache%20Entropy%20Audit/) | JavaScript | Edge and Carbon | 2026-08-11 | Audits CDN and edge logs for cache key entropy, private data in shared caches and low hit rate routes |
| [Edge Inference Admission](Edge%20Inference%20Admission/) | C | Cost and Quota | 2026-06-10 | C admission controller that throttles LLM requests on rolling per tenant cost, token and carbon budgets |
| [Edge Inference Probe Planner](Edge%20Inference%20Probe%20Planner/) | Swift | Edge and Carbon | 2026-08-17 | Ranks edge LLM routes by latency, error, cost and carbon risk, then allocates synthetic probes to a budget |
| [Edge Quota Balancer](Edge%20Quota%20Balancer/) | Lua | Cost and Quota | 2026-05-24 | Lua CLI that reads per region quota telemetry as CSV and prints a safe, capped quota rebalancing plan |
| [Edge SSE Replay Ledger](Edge%20SSE%20Replay%20Ledger/) | Lua | Streaming and Parsing | 2026-06-16 | Single file Lua SSE ledger with bounded replay, Last-Event-ID recovery, secret redaction and tenant filters |
| [Edge Telemetry Backpressure Ring](Edge%20Telemetry%20Backpressure%20Ring/) | Zig | Edge and Carbon | 2026-09-09 | A battery powered sensor node loses its uplink for an hour, readings keep arriving, and a plain FIFO either |
| [Embedding Drift Attributor](Embedding%20Drift%20Attributor/) | Julia | Data and Vector | 2026-06-12 | Attributes embedding drift to the cohorts that moved, with permutation tests and bootstrap risk intervals |
| [Eval Artifact Lineage Gate](Eval%20Artifact%20Lineage%20Gate/) | OCaml | Evals and Experiments | 2026-06-09 | Hash chained ledger and CI gate for LLM eval runs, proving dataset, prompt, model and cost lineage |
| [Eval Holdout Firewall](Eval%20Holdout%20Firewall/) | OCaml | Evals and Experiments | 2026-07-07 | OCaml CLI that fingerprints eval holdouts with bottom-k MinHash and blocks contaminated candidate data in CI |
| [Eval Leakage Sentinel](Eval%20Leakage%20Sentinel/) | Scala and R | Evals and Experiments | 2026-04-30 | Scans training and eval data for benchmark leakage using Jaccard shingles in R and MinHash LSH in Scala |
| [Eval Power Drift](Eval%20Power%20Drift/) | R | Evals and Experiments | 2026-05-24 | Welch t-test and power gate for AI eval CSVs that flags underpowered wins and hidden cost drift |
| [Evidence Pack Planner](Evidence%20Pack%20Planner/) | TypeScript | Data and Vector | 2026-05-14 | Packs retrieval hits into a token budgeted evidence set with dedup, source diversity and injection filtering |
| [Experiment Evidence Ledger](Experiment%20Evidence%20Ledger/) | Julia | Evals and Experiments | 2026-05-25 | Hashes a run directory into one SHA-256 evidence root so CI can prove experiment artifacts never drifted |
| [Experiment Split Provenance](Experiment%20Split%20Provenance/) | R | Evals and Experiments | 2026-07-26 | Base R audit that catches id, group and temporal leakage across ML dataset splits and reports it as CSV plus SARIF |
| [Flake Lock Supply Chain Gate](Flake%20Lock%20Supply%20Chain%20Gate/) | Nix | Security and Supply Chain | 2026-08-04 | Pure Nix expression that audits flake.lock for supply chain risk and gates CI with SARIF output |
| [GPU Lease Broker](GPU%20Lease%20Broker/) | C | Infra and DevOps | 2026-04-17 | TTL based GPU leases using atomic mkdir locks, with auto renewal, stale reaping and CUDA_VISIBLE_DEVICES |
| [GPU Spot Checkpoint Planner](GPU%20Spot%20Checkpoint%20Planner/) | C++ | Infra and DevOps | 2026-08-18 | Plans GPU spot placement and checkpoint intervals from eviction risk, SLO, budget and carbon |
| [Gateway Failover Budget Governor](Gateway%20Failover%20Budget%20Governor/) | Dart | LLM and Inference | 2026-08-19 | Dart CLI that scores LLM gateway endpoints on cost, latency, residency and tenant budget, then emits an auditable JSON failover plan per request |
| [Gradient Concurrency Limiter](Gradient%20Concurrency%20Limiter/) | Lua | LLM and Inference | 2026-09-10 | Adaptive gradient-based concurrency limiter in Lua for OpenResty LLM inference gateways, no dependencies |
| [HTML Prompt Injection Firewall](HTML%20Prompt%20Injection%20Firewall/) | PHP | Security and Supply Chain | 2026-04-27 | Single file PHP firewall that extracts LLM safe text from untrusted HTML and scores it for prompt injection |
| [Hybrid Inference Router](Hybrid%20Inference%20Router/) | Swift | LLM and Inference | 2026-04-23 | Swift actor that routes each inference request local or remote from live latency, cost, battery and privacy signals, with deadline aware hedging |
| [Incremental JSON Boundary Tracker](Incremental%20JSON%20Boundary%20Tracker/) | Python | Streaming and Parsing | 2026-04-10 | Character level state machine that detects when the first complete JSON value has arrived in an LLM token stream |
| [Inference Admission Ledger](Inference%20Admission%20Ledger/) | Elixir | Cost and Quota | 2026-04-29 | Elixir GenServer that leases concurrency, token and spend budget to LLM calls before they start |
| [Inference Backpressure Scheduler](Inference%20Backpressure%20Scheduler/) | C++ | LLM and Inference | 2026-04-14 | C++17 per-stream backpressure scheduler for LLM token streaming with deficit round robin dispatch and bounded queues |
| [Inference Batch Reconciler](Inference%20Batch%20Reconciler/) | C# | LLM and Inference | 2026-05-21 | Joins batch inference request and result JSONL on custom_id, classifies failures and emits a safe retry file |
| [Inference Canary Gate](Inference%20Canary%20Gate/) | C# | Infra and DevOps | 2026-04-22 | Canary rollout gate for .NET inference services using Wilson intervals and p95/p99 tail guards |
| [Inference Hedge Planner](Inference%20Hedge%20Planner/) | Go | LLM and Inference | 2026-05-16 | Go planner that scores LLM providers on tail latency, cost and health, then times an adaptive hedge |
| [Inference Invoice Reconciler](Inference%20Invoice%20Reconciler/) | Haskell | Cost and Quota | 2026-05-07 | Reconciles LLM provider invoices against gateway traces to catch duplicate, orphaned and mis-cached billing |
| [Inference Lease Allocator](Inference%20Lease%20Allocator/) | OCaml | LLM and Inference | 2026-07-02 | OCaml admission controller that admits, queues or rejects LLM inference requests against budget, residency, health and deadline limits |
| [Inference Quota Broker](Inference%20Quota%20Broker/) | Ruby | Cost and Quota | 2026-04-25 | Ruby lease-based admission control broker for multi-dimensional LLM rate, token, cost and concurrency quotas |
| [Inference Route Budget](Inference%20Route%20Budget/) | OCaml | Cost and Quota | 2026-07-22 | Audits AI gateway routes from a CSV export against cost, latency, carbon and data residency budgets |
| [Inference Spend Circuit](Inference%20Spend%20Circuit/) | C | Cost and Quota | 2026-07-03 | Per tenant rolling token and spend admission control for LLM gateways, in dependency free C |
| [Inference Spend Reconciliation Ledger](Inference%20Spend%20Reconciliation%20Ledger/) | C# | Cost and Quota | 2026-08-15 | Your LLM gateway dashboard, your provider invoice CSV and your own usage telemetry all report a different |
| [Interleaved Tool Call Assembler](Interleaved%20Tool%20Call%20Assembler/) | Rust | MCP and Agent Tooling | 2026-04-20 | Streaming LLM tool calls arrive as fragments: a partial function name here, a slice of JSON arguments there, |
| [Isolate Vector Search Engine](Isolate%20Vector%20Search%20Engine/) | Dart | Data and Vector | 2026-09-06 | Sharded in-memory vector search across Dart isolates with cancellation, backpressure and crash recovery |
| [JSON RPC Frame Codec](JSON%20RPC%20Frame%20Codec/) | OCaml | Streaming and Parsing | 2026-04-17 | A stdin pipe hands you bytes, not messages |
| [JSON Schema Compatibility Guard](JSON%20Schema%20Compatibility%20Guard/) | Ruby | Streaming and Parsing | 2026-04-15 | A schema edit that looks harmless in a diff can reject traffic that used to work |
| [JSONL Batch Preflight](JSONL%20Batch%20Preflight/) | Bash | LLM and Inference | 2026-04-22 | Bash preflight that validates, dedupes, secret-scans and shards JSONL batch files before provider upload |
| [JSONL Secret Firewall](JSONL%20Secret%20Firewall/) | C++ | Security and Supply Chain | 2026-04-14 | Your JSONL logs contain bearer tokens |
| [Kube Context Guard](Kube%20Context%20Guard/) | Bash | Infra and DevOps | 2026-04-22 | You have credentials for six clusters and your shell remembers only one context |
| [LLM Spend Velocity Governor](LLM%20Spend%20Velocity%20Governor/) | Ruby | Cost and Quota | 2026-09-07 | A single tenant's runaway agent loop can burn a month of LLM budget in ten minutes, and most Ruby backends |
| [LLM Stream Normalizer](LLM%20Stream%20Normalizer/) | Swift | Streaming and Parsing | 2026-04-14 | Streaming LLM responses arrive in a different wire format from every provider, and tool call arguments show |
| [LLM Stream Processor](LLM%20Stream%20Processor/) | Bash | Streaming and Parsing | 2026-04-07 | Streaming LLM responses arrive as half objects |
| [MCP Call Coalescer](MCP%20Call%20Coalescer/) | Java | MCP and Agent Tooling | 2026-04-14 | Java request coalescer with TTL cache that collapses duplicate concurrent MCP tool calls into one upstream call per key |
| [MCP Concurrency Governor](MCP%20Concurrency%20Governor/) | Elixir | MCP and Agent Tooling | 2026-04-29 | OTP GenServer that caps per-tool concurrency with session round-robin queues, TTL deadlines and lease-based cleanup |
| [MCP Egress Boundary](MCP%20Egress%20Boundary/) | Nix | MCP and Agent Tooling | 2026-05-27 | An MCP server needs one token, one repository checkout and one API destination |
| [MCP Invocation Firewall](MCP%20Invocation%20Firewall/) | TypeScript | MCP and Agent Tooling | 2026-04-20 | Deterministic TypeScript policy engine that allows, reviews or denies MCP tool calls and redacts secrets in results |
| [MCP JSON RPC Profiler](MCP%20JSON%20RPC%20Profiler/) | Swift | MCP and Agent Tooling | 2026-05-24 | MCP servers look fine in a demo and then quietly burn minutes per agent turn once real developers hit them |
| [MCP Manifest Compat Gate](MCP%20Manifest%20Compat%20Gate/) | OCaml | MCP and Agent Tooling | 2026-05-07 | OCaml CI gate that diffs two MCP tool manifests and flags breaking JSON Schema contract changes per tool |
| [MCP Server Bundle](MCP%20Server%20Bundle/) | Nix | MCP and Agent Tooling | 2026-05-11 | Your MCP client config works on your laptop and nowhere else |
| [MCP Server Doctor](MCP%20Server%20Doctor/) | Bash | MCP and Agent Tooling | 2026-05-18 | An MCP stdio server can pass its own tests, publish a clean README and still break the moment a real client |
| [MCP Stdio Watchdog](MCP%20Stdio%20Watchdog/) | Kotlin | MCP and Agent Tooling | 2026-04-22 | A stdio MCP server hangs mid `tools/call`, the client keeps waiting forever, and nobody can tell whether the |
| [MCP Tool Contract Gate](MCP%20Tool%20Contract%20Gate/) | Haskell | MCP and Agent Tooling | 2026-05-07 | An MCP server ships a small edit to one tool's JSON Schema, a required field appears or a maximum drops, and |
| [MCP Tool Policy Compiler](MCP%20Tool%20Policy%20Compiler/) | Lua | MCP and Agent Tooling | 2026-07-17 | Single file Lua policy engine that replays MCP and agent tool call JSONL against a plain text allow/deny rule file |
| [MCP Tool Router](MCP%20Tool%20Router/) | TypeScript | MCP and Agent Tooling | 2026-04-06 | Every MCP server past three tools turns into a giant if/else dispatch block with no input validation, so a |
| [MCP Tool Schema Sanitizer](MCP%20Tool%20Schema%20Sanitizer/) | PHP | MCP and Agent Tooling | 2026-04-15 | Rewrites MCP tool JSON Schemas into a portable, closed, deterministic form with a sha256 fingerprint |
| [MCP Trace Sanitizer](MCP%20Trace%20Sanitizer/) | C | Security and Supply Chain | 2026-07-24 | MCP tool traces, LLM gateway JSONL, browser automation transcripts and CI logs quietly carry real API keys, |
| [Model Drift Triage](Model%20Drift%20Triage/) | Haskell | Evals and Experiments | 2026-07-07 | Haskell module that compares baseline vs candidate LLM telemetry windows per slice and returns a typed rollout verdict |
| [Model Gateway Spend Firewall](Model%20Gateway%20Spend%20Firewall/) | PHP | Cost and Quota | 2026-08-21 | Single file PHP CLI that audits AI gateway events against price books, tenant budgets, quotas and prompt secrets |
| [OTel Inference Cost Ledger](OTel%20Inference%20Cost%20Ledger/) | C# | Cost and Quota | 2026-05-24 | Joins OpenTelemetry spans with LLM gateway usage to attribute token cost and p95 latency per service and tenant |
| [Paged KV Cache Allocator](Paged%20KV%20Cache%20Allocator/) | C++ | LLM and Inference | 2026-09-05 | Block-based KV cache allocator in C++ with prefix hash sharing, copy-on-write forks and LRU eviction |
| [Paired Eval Gate](Paired%20Eval%20Gate/) | Julia | Evals and Experiments | 2026-04-17 | Stratified cluster paired bootstrap in Julia for LLM eval release gates, with win rates, CIs and margins |
| [Policy Drift Incident Router](Policy%20Drift%20Incident%20Router/) | Bash | Infra and DevOps | 2026-08-10 | Bash CI gate that routes risky policy, infra, AI-agent and pipeline file changes to owners, controls and runbooks |
| [Prompt Cache Cutover](Prompt%20Cache%20Cutover/) | OCaml | Cost and Quota | 2026-05-24 | OCaml CLI that reads route-level token CSV and returns per-route prompt cache break-even and enable verdicts |
| [Prompt Cache Lease Governor](Prompt%20Cache%20Lease%20Governor/) | Kotlin | Cost and Quota | 2026-08-13 | Kotlin CLI that scores prompt cache blocks on cost, freshness, residency and sensitivity, then pins, refreshes, evicts, bypasses or quarantines each one |
| [Prompt Cache Planner](Prompt%20Cache%20Planner/) | Python | LLM and Inference | 2026-04-18 | Splits LLM requests into a stable cacheable prefix and live suffix, then explains why prompt caching missed |
| [Prompt Injection Edge Guard](Prompt%20Injection%20Edge%20Guard/) | Lua | Security and Supply Chain | 2026-06-29 | Deterministic Lua guard that scores, redacts and budgets untrusted text before it reaches an LLM |
| [Prompt Template Compiler](Prompt%20Template%20Compiler/) | JavaScript | LLM and Inference | 2026-08-31 | Compiles {{ }} prompt templates once into a JS function with schema linting, token budget bounds and injection neutralization |
| [Provider Health Circuit](Provider%20Health%20Circuit/) | Scala | LLM and Inference | 2026-04-16 | Scala circuit breaker and health scored router that fails over between LLM providers |
| [Queue SLO Allocator](Queue%20SLO%20Allocator/) | C | Infra and DevOps | 2026-07-07 | Allocates a fixed worker pool across competing queues by SLO risk, from CSV telemetry on stdin |
| [RAG Prompt Firewall](RAG%20Prompt%20Firewall/) | Elixir | Security and Supply Chain | 2026-07-18 | Dependency-free Elixir CI gate that scans agent traces for prompt injection and retrieval poisoning, emits SARIF |
| [Realtime Stream Handler](Realtime%20Stream%20Handler/) | TypeScript | Streaming and Parsing | 2026-04-03 | Buffers Anthropic SDK text deltas into fixed size chunks with async backpressure and parallel stream fan out |
| [Repo Auto Shell](Repo%20Auto%20Shell/) | Nix | Infra and DevOps | 2026-04-17 | Single Nix file that detects a repo's stacks and pins every toolchain cache inside the working tree |
| [Repository Signal Ranker](Repository%20Signal%20Ranker/) | Ruby | Infra and DevOps | 2026-05-24 | Ranks changed files from git diff --numstat by churn, path risk and CODEOWNERS fan out so reviewers start right |
| [Runner Artifact Attestor](Runner%20Artifact%20Attestor/) | Bash | Security and Supply Chain | 2026-06-04 | Bash CI gate that writes and verifies a sha256 artifact manifest, blocks symlinks, stray files, leaked secrets and mismatched provenance before upload |
| [SBOM Vulnerability Drift Gate](SBOM%20Vulnerability%20Drift%20Gate/) | Go | Security and Supply Chain | 2026-04-22 | Diffs two Trivy or Grype JSON scans and fails CI only on newly introduced vulnerabilities and severity escalations |
| [SSE Stream Decoder](SSE%20Stream%20Decoder/) | C | Streaming and Parsing | 2026-04-17 | Chunk-safe C decoder for Server-Sent Events with CRLF handling, bounded buffers and Last-Event-ID tracking |
| [Semantic Pixel Renderer](Semantic%20Pixel%20Renderer/) | JavaScript | LLM and Inference | 2026-04-05 | Deterministic text to canvas pixel art in the browser via semantic hashing, no API key and no network |
| [Sequential Eval Quorum](Sequential%20Eval%20Quorum/) | Elixir | Evals and Experiments | 2026-06-17 | Sequential eval gate in Elixir that dedupes streamed canary evidence and returns promote or rollback |
| [Smart Doc Chunker](Smart%20Doc%20Chunker/) | Bash | Data and Vector | 2026-04-05 | Bash chunker that splits .txt/.md/.log files into token bounded chunks and emits a manifest.json |
| [Smart Token Budget Manager](Smart%20Token%20Budget%20Manager/) | Python | Cost and Quota | 2026-04-05 | In-process token and cost budget guard for LLM APIs, with per user sliding window limits and pre-call cost estimates |
| [Stable Code Chunk Planner](Stable%20Code%20Chunk%20Planner/) | Java | LLM and Inference | 2026-04-22 | Deterministic Java CLI that chunks a repo at declaration boundaries and emits an NDJSON manifest with stable IDs |
| [Stable HTML Change Detector](Stable%20HTML%20Change%20Detector/) | TypeScript | Streaming and Parsing | 2026-04-27 | Fingerprints semantic HTML blocks so page monitors alert on real content changes, not framework churn |
| [Stream Replay Fence](Stream%20Replay%20Fence/) | Julia | Streaming and Parsing | 2026-07-27 | Audits a stream replay manifest for duplicate identity, offset gaps, watermark and retry violations |
| [Streaming Context Predictor](Streaming%20Context%20Predictor/) | TypeScript | Streaming and Parsing | 2026-04-07 | Predicts LLM context overflow mid stream from a rolling token average and emits warnings before truncation |
| [Streaming Eval Bootstrap](Streaming%20Eval%20Bootstrap/) | Scala | Evals and Experiments | 2026-05-24 | Deterministic paired bootstrap CLI that turns streamed eval CSV rows into a CI gate with regression risk |
| [Streaming Kernel Drift Detector](Streaming%20Kernel%20Drift%20Detector/) | Julia | Data and Vector | 2026-04-10 | Feature vectors and embeddings shift long before a dashboard shows it |
| [Streaming Request Deduplicator](Streaming%20Request%20Deduplicator/) | TypeScript | Streaming and Parsing | 2026-04-08 | Coalesces identical in-flight streaming requests behind one signature so a single upstream call fans out to many handlers |
| [Structured JSON Repair](Structured%20JSON%20Repair/) | Python | Streaming and Parsing | 2026-04-10 | Deterministic repair of almost-valid LLM JSON: fences, bare keys, Python literals, comments, trailing commas |
| [Structured JSON Stream Decoder](Structured%20JSON%20Stream%20Decoder/) | Dart | Streaming and Parsing | 2026-04-14 | Dart incremental JSON decoder that extracts complete, verified JSON documents from streamed LLM text over SSE or WebSockets |
| [Structured Output Drift Gate](Structured%20Output%20Drift%20Gate/) | Java | Streaming and Parsing | 2026-05-20 | The same prompt, run twice against the same model, returns JSON that is subtly different: a key vanishes, a |
| [Tenant GPU Lease Planner](Tenant%20GPU%20Lease%20Planner/) | Go | Infra and DevOps | 2026-06-02 | Deciding how many GPUs each inference tenant needs is usually a spreadsheet guess |
| [Tenant Inference Queue Planner](Tenant%20Inference%20Queue%20Planner/) | Dart | Cost and Quota | 2026-06-14 | An LLM gateway that dispatches first and explains later will blow a tenant's token quota, miss a deadline and |
| [Tenant Stream Budget Ledger](Tenant%20Stream%20Budget%20Ledger/) | Zig | Cost and Quota | 2026-08-22 | Your AI gateway logs already know that one tenant blew past its daily spend, replayed an idempotency key |
| [Tenant Token Fair Scheduler](Tenant%20Token%20Fair%20Scheduler/) | Elixir | Cost and Quota | 2026-09-11 | Deficit Round Robin scheduler in Elixir that fairly splits a shared LLM token budget across tenants by cost |
| [Test Impact Shard Planner](Test%20Impact%20Shard%20Planner/) | Go | Infra and DevOps | 2026-08-09 | A monorepo has more tests than any pull request deserves, but "run only the tests near the diff" quietly |
| [Thermal Aware Inference Throttler](Thermal%20Aware%20Inference%20Throttler/) | Swift | LLM and Inference | 2026-09-04 | Swift actor that throttles on-device AI inference concurrency using AIMD driven by thermal state and Low Power Mode |
| [Token Budget Manager](Token%20Budget%20Manager/) | Rust | LLM and Inference | 2026-04-07 | Lock-free Rust token budget accountant with RAII leases that auto-release unconsumed LLM context on drop |
| [Token Egress Budget](Token%20Egress%20Budget/) | C | Cost and Quota | 2026-05-24 | Your token bill looks fine and your network bill does not |
| [Token Stream Optimizer](Token%20Stream%20Optimizer/) | Python and TypeScript | Streaming and Parsing | 2026-04-03 | An LLM streams tokens as fast as the network delivers them, and whatever sits downstream of your stream loop |
| [Tool Call Idempotency](Tool%20Call%20Idempotency/) | Elixir | MCP and Agent Tooling | 2026-04-16 | Elixir GenServer that leases idempotency keys, parks concurrent callers and caches results in ETS to stop duplicate tool calls |
| [Tool Call Schema Drift Guard](Tool%20Call%20Schema%20Drift%20Guard/) | TypeScript | MCP and Agent Tooling | 2026-08-05 | Your agent's tool manifest says one thing and your production callers send another |
| [Tool Replay Lease](Tool%20Replay%20Lease/) | Elixir | MCP and Agent Tooling | 2026-04-16 | A retried job, a reconnected model stream or a redelivered webhook can run the same side effect twice: the |
| [Tool Schema Distiller](Tool%20Schema%20Distiller/) | Python | MCP and Agent Tooling | 2026-05-12 | Ranks and compresses MCP/OpenAPI tool schemas into a token-budgeted prompt manifest with an omission audit |
| [Trace Context Contract](Trace%20Context%20Contract/) | Scala | MCP and Agent Tooling | 2026-07-20 | Your AI agent traces look complete in the dashboard and then fall apart the moment someone needs them as |
| [Trace Secret Scrubber](Trace%20Secret%20Scrubber/) | Python | Security and Supply Chain | 2026-04-10 | Your service logs a prompt, a tool call and a dict of request headers |
| [Trace Tail Sampler](Trace%20Tail%20Sampler/) | Haskell | Infra and DevOps | 2026-05-24 | Deterministic FNV-1a trace sampler that keeps error and latency-tail spans instead of a flat random slice |
| [Unified Rate Limit Coordinator](Unified%20Rate%20Limit%20Coordinator/) | TypeScript | LLM and Inference | 2026-04-10 | One TypeScript class that holds the rate limit rules for every API your app talks to, so concurrency, token |
| [Untrusted Code Resource Governor](Untrusted%20Code%20Resource%20Governor/) | Java | Security and Supply Chain | 2026-09-03 | Runs untrusted or LLM-generated Java in-process under wall clock, CPU and allocation budgets with a blocklist classloader |
| [Vector Cache LRU](Vector%20Cache%20LRU/) | Rust | LLM and Inference | 2026-04-08 | Thread safe bounded LRU cache for f32 embedding vectors in Rust, with byte level payload accounting |
| [Vector Index Lease Guard](Vector%20Index%20Lease%20Guard/) | Haskell | Data and Vector | 2026-07-21 | Haskell CI gate that fails the build on stale, expired, cross-tenant or revoked RAG vector index leases |
| [Vector Search Optimizer](Vector%20Search%20Optimizer/) | Rust | Data and Vector | 2026-04-05 | Brute force Rust cosine vector search that tags each top k result with whether it fits a token budget |
| [Vector Shard Merge Planner](Vector%20Shard%20Merge%20Planner/) | F# | Data and Vector | 2026-09-12 | Deterministic F# CLI that plans a vector shard merge with LWW resolution, dimension gating and LSH dedup |
| [Wasm Memory Lease Zig](Wasm%20Memory%20Lease%20Zig/) | Zig | Infra and DevOps | 2026-05-24 | Deterministic Wasm memory lease planner in Zig that grants tenant sandbox memory by priority-per-TTL from CSV |
| [Webhook Replay Fence](Webhook%20Replay%20Fence/) | PHP | Security and Supply Chain | 2026-05-24 | Single file PHP webhook replay fence: fingerprints deliveries, prunes by TTL and locks a file ledger |
| [Workflow Failure Fingerprint](Workflow%20Failure%20Fingerprint/) | TypeScript | Infra and DevOps | 2026-05-30 | Clusters raw CI logs into stable redacted failure fingerprints with a severity, category and rerun decision |
