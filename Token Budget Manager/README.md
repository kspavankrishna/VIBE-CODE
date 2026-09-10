# Token Budget Manager

Streaming LLM responses blow past your context window budget because nobody is counting until the API returns an error. This is a lock free token budget accountant in Rust that reserves capacity before a call starts, tracks consumption as tokens arrive and returns whatever was not used the moment the lease goes out of scope.

**Language:** Rust | **Lines:** 106 | **Added:** 2026-04-07

## What this solves

You have a Rust backend that fans out to Claude or any other LLM API. Ten concurrent requests, each one streaming, each one holding some slice of a shared context window or a shared per minute token allowance. Nothing in the standard client tells you how much of that shared pool is already spoken for. So request eleven starts, the provider counts the tokens you did not count, and you get a 400 or a 429 halfway through a stream that the user is already watching render. The half finished response is worthless and you have paid for the prompt tokens anyway.

The failure mode is worse when the budget is a context window rather than a rate limit. A summarizer that keeps appending to a conversation will silently walk the history up to the model limit, then one day a slightly longer input tips it over and the whole request fails. It fails in production, on a real user, at the exact moment the conversation got interesting enough to be long. Nobody sees it in testing because test conversations are short.

The other thing that goes wrong is over reservation. A naive fix is to pre allocate a fixed slice of budget to every in flight request. That works until a request finishes early, having used four hundred of the two thousand tokens it claimed. Those sixteen hundred stay locked up until something frees them, and if the task panicked or returned early, nothing ever does. The pool leaks capacity request by request until a restart clears it.

This file addresses all three: a reservation you take before you spend, a consumption counter you update mid stream and a `Drop` implementation that gives back the unconsumed remainder automatically, including on panic and on early return.

## Why I built it

Every Rust backend integrating AI needs to prevent budget overruns, track consumption mid stream and signal backpressure before hitting limits. The provider SDKs do not give you this. They report usage after the response completes, which is exactly one moment too late to make a scheduling decision. Rate limiter crates solve a different problem: they gate request counts over time, not variable sized token draws against a fixed pool where the true cost is only known after the fact.

Wrapping a `Mutex<usize>` around a counter is the obvious move and it is the wrong one, because the hot path is a check and an increment on a value many streaming tasks touch. Atomics with an RAII lease express the ownership better: holding the lease is holding the reservation, and the compiler enforces the release.

## When to use it

- A Rust service streaming completions from several concurrent tasks against one shared context or token pool
- An agent loop where each tool call needs a guaranteed slice of the remaining window before it is allowed to run
- A batch job that must stop enqueuing work at 85 percent of budget instead of crashing at 100
- Any code path where a task can return early or panic mid stream and you cannot afford the reservation to leak
- A dashboard or log line that needs a live usage percentage rather than a post hoc total

## How it works

`TokenBudgetManager` holds four fields: an immutable `total_budget`, two `Arc<AtomicUsize>` counters named `used` and `reserved` and a precomputed `warn_threshold`. `new` sets that threshold at 85 percent of the total budget by casting through `f64`. There is no lock anywhere in the file.

The split between `used` and `reserved` is the core idea. `reserved` is capacity that is claimed but not yet spent. `used` is capacity that is definitely gone. `reserve(tokens)` loads both with `Ordering::Acquire`, sums them with the requested amount and compares against `total_budget`. If the sum overflows the budget it returns `Err(BudgetExceeded)` carrying `requested`, `available` computed with `saturating_sub` and `total_budget`, so the caller can log or retry with a smaller ask instead of guessing. Otherwise it does a `fetch_add` on `reserved` and hands back a `TokenLease`.

`TokenLease` is the RAII half. It carries a tuple of the two `Arc<AtomicUsize>` handles produced by the private `clone_refs`, plus `tokens` (the size of the reservation) and `consumed`. As the stream arrives you call `consume(n)`, which sets `consumed` to `n.min(self.tokens)`, clamping so a lease can never report spending more than it reserved. `remaining_in_lease` is the saturating difference.

The `Drop` implementation is where the accounting closes. It subtracts the full `tokens` from `reserved` and, if anything was consumed, adds `consumed` to `used`. So a lease of 2000 that consumed 400 releases all 2000 from reserved and moves only 400 into used. The other 1600 becomes available again with no explicit call. Because this runs in `Drop`, it also runs on unwind, which is the property you actually want on an error path.

`remaining` reports `total_budget` minus `used` plus `reserved`, saturating at zero. `is_warning_level` compares `used` against the precomputed threshold, which is your backpressure signal: stop admitting new work while it is true. `usage_percent` gives the same thing as a float for logging. `commit(tokens)` is a direct unreserved add to `used`, for costs you learn about outside the lease flow.

## Usage

```rust
// 128k context window as the shared pool
let budget = TokenBudgetManager::new(128_000);

// Reserve before the call. Non blocking, fails fast.
match budget.reserve(4_000) {
    Ok(mut lease) => {
        // stream, updating the lease as tokens arrive
        let mut seen = 0;
        while let Some(chunk) = stream.next().await {
            seen += chunk.token_count;
            lease.consume(seen);
            if lease.remaining_in_lease() == 0 {
                break;
            }
        }
        // lease drops here: reserved is released, `seen` moves into used
    }
    Err(e) => {
        eprintln!(
            "over budget: wanted {}, only {} free of {}",
            e.requested, e.available, e.total_budget
        );
    }
}

// Backpressure check before admitting more work
if budget.is_warning_level() {
    println!("at {:.1}% of budget, pausing intake", budget.usage_percent());
}

println!("{} tokens still free", budget.remaining());

// Cost you learned about outside a lease
budget.commit(120);
```

## Notes

- `reserve` is not atomic as a whole. It loads, checks, then does a separate `fetch_add`, so two threads reserving at the same instant can both pass the check and push the total past `total_budget`. Under real contention you want a compare and swap loop on a single packed counter. Treat the current check as advisory rather than a hard cap.
- `consume` sets rather than accumulates. Call it with the running total seen so far, not with each chunk delta, or you will only record the last chunk.
- `commit` adds to `used` without touching `reserved`. It is for costs outside the lease flow. Calling it for tokens a lease already covers double counts them.
- `TokenBudgetManager` is not `Clone` and not wrapped in an `Arc` here. To share it across tasks you wrap it yourself. The `TokenLease` is independently shareable because it carries `Arc` handles.
- `usage_percent` divides by `total_budget` with no zero guard. A manager built with `new(0)` returns NaN or infinity.
- No dependencies beyond `std`. No async runtime assumption, no tokenizer, no counting. You supply the token numbers, this only does the arithmetic and the lifetime.
- `warn_threshold` is fixed at 85 percent inside `new` and there is no setter.
