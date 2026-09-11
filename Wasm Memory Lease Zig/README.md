# Wasm Memory Lease Zig

WebAssembly sandboxes on an edge node share one fixed memory budget, and nothing decides who gets what until a module already traps on out of memory. This is a deterministic Wasm memory lease planner in Zig: feed it CSV lease requests on stdin, get a grant plan on stdout before anything is admitted.

**Language:** Zig | **Lines:** 135 | **Added:** 2026-05-24

## What this solves

This solves the April 2026 edge WebAssembly memory problem where plugin sandboxes, AI tool runtimes and customer extensions compete for tiny per isolate memory budgets. A CDN worker node, an IoT hub or a serverless runtime has a hard ceiling on linear memory and every tenant module wants a slice. The usual answer is to admit modules in arrival order and let the allocator sort it out. It does not sort it out. It traps.

The failure mode is specific. A low priority background extension arrives first and grows its linear memory to whatever it asked for, because nothing said no. Two seconds later the request path module serving customer traffic tries to grow and traps. The request drops. The trace shows an out of memory inside a Wasm instance with no useful attribution, so the on call engineer spends an hour deciding whether the bug is in the host, the guest or the allocator. It is none of them. Admission was never planned.

The second failure is quieter and more expensive. Nobody reserved memory for the platform itself, which needs headroom for its buffers, the module cache and the instance tables. Tenants take the full ceiling, the host runs out on its own path, and the node goes unhealthy instead of one tenant getting a refusal. That is every tenant on the box, not just the greedy one. Third is shredding: hand out whatever is left and you grant a tenant 12 KB when its module needs a page or two to do anything. It fails anyway and that memory is gone from the pool.

## Why I built it

Edge compute teams need a deterministic answer before a Wasm module is admitted, not a postmortem after a trap has already dropped a request. The runtimes do not give you this. Wasmtime, WasmEdge and the proprietary edge sandboxes give you a per instance limit and a trap when it is exceeded, which is enforcement, not planning. Kubernetes style quotas operate on pods and are far too coarse for hundreds of isolates in one process. Cgroups do not see individual linear memories at all.

So the planning layer is left to the platform team and usually ends up as an unreviewed loop in the control plane that nobody trusts. I wanted the decision in one file with no dependencies and no ambiguity: same CSV in, same plan out, every time. Zig suits it because it compiles to a static binary you can drop onto a node or into a CI job, and there is no garbage collector to perturb a program whose whole job is reasoning about memory.

## When to use it

- A gateway or CDN node runs untrusted tenant Wasm modules and you must pick which to admit this cycle, before any instantiate.
- An AI tool runtime loads plugin sandboxes on demand and emergency priority tools must survive a crunch that squeezes everything else out.
- An IoT hub with a few hundred megabytes total needs platform headroom carved out before tenant extensions get any.
- You want a CI gate that fails a release when the declared memory of all bundled components stops fitting the node budget.
- A node keeps going unhealthy under load and you want to see which leases a given capacity would and would not have satisfied.
- A control plane already emits tenant, component, requested bytes, priority and TTL, and you need that turned into a grant decision in a pipeline step.

## How it works

The unit of work is a `Lease`: tenant, component, `requested` bytes, an 8 bit `priority`, a `ttl_ms` and a `granted` field that starts at zero and gets filled in by the planner. Input is CSV on stdin in that column order. `main` reads line by line into a 4096 byte stack buffer, skips blank lines and any line starting with `tenant,` so a header row is tolerated, then hands each row to `parseLease`, which splits on commas, trims whitespace and copies the string fields into an arena. Everything allocates from one `ArenaAllocator` over `page_allocator` and is freed in a single shot when `main` returns, so there is no per lease cleanup path to get wrong.

Ranking happens in `Lease.score`. The score is `(priority + 1) * 1_000_000_000 / max(ttl_ms, 1)`, computed in `u128` so the billion scale numerator cannot overflow. That is priority per unit of TTL: a short lived lease outranks a long lived one at the same priority, because it returns its memory sooner. The plus one keeps priority zero from collapsing the score, and the `max(ttl_ms, 1)` keeps a zero TTL row from dividing by zero. `lessImportant` orders by descending score and breaks ties by preferring the smaller `requested`, the classic greedy packing heuristic: when two leases are equally deserving, take the cheaper one so more fit.

`plan` sorts in place with `std.sort.pdq`, Zig's pattern defeating quicksort, chosen because it is fast and the comparator already defines a total order, so stability buys nothing. It then computes `remaining = capacity - reserve`, the platform reservation carved out before tenants see a byte, and walks the sorted slice greedily. Each lease gets `min(requested, remaining)`, recorded only if it clears `min_grant` or sits in the `priority >= 200` emergency band. Otherwise the lease is skipped with `granted` left at zero and the loop moves on, so a tenant asking for more than the pool has left does not block smaller leases behind it. The loop breaks early only when `remaining` hits exactly zero. This is a single pass greedy knapsack approximation, not an optimal solve, and that is deliberate: an operator reads the sorted output top to bottom and sees why each decision was made.

`parseOptions` walks `std.process.argsWithAllocator` and accepts `--capacity`, `--reserve`, `--min-grant` and `--json`, the sizes as unsigned decimal byte counts. Anything else returns `error.UnknownOption`, a flag with no value returns `error.MissingValue`, and a reserve at or above capacity returns `error.ReserveExceedsCapacity` before any work happens. Defaults are 128 MiB capacity, 8 MiB reserve and a 64 KiB minimum grant. Output is `printText`, a tab separated table, or `printJson`, one `{"leases":[...]}` object. Both print every input lease including the refused ones, so a zero in the granted column is the refusal record.

## Usage

```sh
# build
zig build-exe WasmMemoryLeaseZig.zig -O ReleaseSafe

# CSV columns: tenant,component,requested_bytes,priority,ttl_ms
cat > leases.csv <<'CSV'
tenant,component,requested,priority,ttl_ms
acme,edge-auth,4194304,200,500
acme,image-resize,16777216,80,30000
globex,ai-tool-runtime,8388608,150,2000
initech,analytics-beacon,33554432,10,600000
CSV

# defaults: 128 MiB capacity, 8 MiB platform reserve, 64 KiB minimum grant
./WasmMemoryLeaseZig < leases.csv

# tighter node, bigger platform reservation, JSON for a control plane
./WasmMemoryLeaseZig \
  --capacity 33554432 \
  --reserve 4194304 \
  --min-grant 131072 \
  --json < leases.csv

# run without building a binary
zig run WasmMemoryLeaseZig.zig -- --json < leases.csv
```

## Notes

- All sizes are plain byte counts. There is no KB or MB suffix parsing, so `--capacity 128MB` is a parse error, not 128 megabytes.
- Malformed rows abort the whole run rather than being skipped: a missing column gives `error.BadRow`, a priority above 255 gives `error.PriorityTooLarge` and a non numeric field gives a parse error. These propagate out of `main`, so the process exits non zero with a Zig error trace. Lines longer than the 4096 byte buffer also fail the read.
- The JSON writer does not escape its string fields, so a tenant or component name containing a double quote or a backslash produces invalid JSON. Sanitise upstream or use the text mode.
- There is no per tenant fairness cap. One tenant submitting many high scoring leases can take the entire pool. Priority and TTL are the only levers.
- The planner is stateless. It does not track what is already granted, it enforces nothing and it never talks to a Wasm runtime. It produces a plan, and acting on it is the caller's job. The emergency band bypasses `min_grant` but not the ceiling: an emergency lease arriving at `remaining` 0 still gets nothing.
- Written against the older Zig standard library IO API (`std.io.getStdIn`, `ArrayList.init` taking an allocator). Newer Zig releases changed both, so expect small edits on a current toolchain.
