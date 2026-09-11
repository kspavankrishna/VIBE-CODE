# VIBE CODE

One day. One piece of code. Every single day.

This is my personal coding lab. Self contained, production grade programs I write and push daily. Each one is a standalone idea: some practical, some experimental, all built to stay sharp and explore what is possible.  
Languages rotate. Topics shift. The only constant is showing up.

**149 entries  |  24 languages  |  105,786 lines  |  2026-04-03 to 2026-09-10**

---

## [Browse the full index](INDEX.md)

Every entry with its language, topic, date and a one line summary. Fastest way to find something useful.

---

## How this repo is laid out

Every idea gets its own folder holding the source file and a README that explains the problem it solves, how the mechanism actually works and how to run it.

```
Context Window Optimizer/
    ContextWindowOptimizer.py
    README.md
```

Open any folder and the README renders straight away. No hunting through comments to work out what a file does.

---

## Recently added

| Entry | Language | Added | What it does |
|---|---|---|---|
| [Gradient Concurrency Limiter](Gradient%20Concurrency%20Limiter/) | Lua | 2026-09-10 | Adaptive gradient-based concurrency limiter in Lua for OpenResty LLM inference gateways, no dependencies |
| [Edge Telemetry Backpressure Ring](Edge%20Telemetry%20Backpressure%20Ring/) | Zig | 2026-09-09 | A battery powered sensor node loses its uplink for an hour, readings keep arriving, and a plain FIFO either |
| [Carbon Credit Double Count Guard](Carbon%20Credit%20Double%20Count%20Guard/) | PHP | 2026-09-08 | PHP 8 CLI and library that catches overlapping carbon credit serial ranges before a double sold trade closes |
| [LLM Spend Velocity Governor](LLM%20Spend%20Velocity%20Governor/) | Ruby | 2026-09-07 | A single tenant's runaway agent loop can burn a month of LLM budget in ten minutes, and most Ruby backends |
| [Isolate Vector Search Engine](Isolate%20Vector%20Search%20Engine/) | Dart | 2026-09-06 | Sharded in-memory vector search across Dart isolates with cancellation, backpressure and crash recovery |
| [Paged KV Cache Allocator](Paged%20KV%20Cache%20Allocator/) | C++ | 2026-09-05 | Block-based KV cache allocator in C++ with prefix hash sharing, copy-on-write forks and LRU eviction |
| [Thermal Aware Inference Throttler](Thermal%20Aware%20Inference%20Throttler/) | Swift | 2026-09-04 | Swift actor that throttles on-device AI inference concurrency using AIMD driven by thermal state and Low Power Mode |
| [Carbon Aware Job Scheduler](Carbon%20Aware%20Job%20Scheduler/) | C# | 2026-09-03 | Defers batch and GPU jobs to the lowest-carbon window before their deadline using a grid intensity forecast |
| [Untrusted Code Resource Governor](Untrusted%20Code%20Resource%20Governor/) | Java | 2026-09-03 | Runs untrusted or LLM-generated Java in-process under wall clock, CPU and allocation budgets with a blocklist classloader |
| [Agent Tool Call Circuit Breaker](Agent%20Tool%20Call%20Circuit%20Breaker/) | Kotlin | 2026-09-02 | Cost weighted circuit breaker for LLM agent tool calls, with per tenant bulkheading and jittered backoff |

---

## What you will find here

- MCP and Agent Tooling (29)
- LLM and Inference (24)
- Cost and Quota (19)
- Streaming and Parsing (19)
- Infra and DevOps (18)
- Security and Supply Chain (14)
- Evals and Experiments (13)
- Data and Vector (7)
- Edge and Carbon (6)

---

## Languages

Python (12) | TypeScript (12) | Rust (8) | Go (6) | Bash (8) | JavaScript (4) | Kotlin (4) | Java (5) | C# (6) | Swift (5) | C++ (5) | Dart (6) | Ruby (5) | PHP (6) | Zig (6) | Lua (6) | Elixir (8) | Scala (4) | Haskell (7) | OCaml (7) | C (7) | R (4) | Julia (6) | Nix (4)

Each new entry goes to whichever language currently has the fewest, so coverage keeps evening out over time.

---

## Explore and collaborate

Fork this repo. Run the code. Break it. Improve it.  
Have ideas or feedback? Reach out.  
Follow this repo for daily updates.

---

## Contact

Ideas, suggestions or want to collaborate?  
Email: kspavankrishna@gmail.com

---

## License

@kspavankrishna  
www.kspavankrishna.com  
Pavan
