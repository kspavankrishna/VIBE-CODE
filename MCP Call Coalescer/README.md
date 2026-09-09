# MCP Call Coalescer

Ten threads in the same Java service ask for the same expensive MCP tool result inside the same 50 milliseconds, and you pay for ten calls instead of one. This is a single file request coalescer with a TTL cache that collapses those duplicates into one upstream call per key.

**Language:** Java | **Lines:** 109 | **Added:** 2026-04-14

## What this solves

Duplicate MCP and agent tool calls pile up when several parts of a Java service ask for the same expensive result at nearly the same time. Modern agent backends fan out. A single user turn triggers a search call, an embeddings lookup, a repo read and two or three hosted tool invocations, and those paths often converge on identical arguments without knowing about each other. Nothing in the code is wrong. Each caller is doing its job. The result is a thundering herd against one endpoint with the exact same payload.

Without something in the middle, the first thing that breaks is your bill. Vector search, embeddings and hosted tool endpoints are priced per call, and a fan out that issues five identical requests costs five times what it should. The second thing that breaks is latency, because most upstream services rate limit per key or per tenant. Your redundant calls eat the same quota your useful calls need, so the useful ones queue behind them and your p99 climbs. Under real load the 429s start, retries stack on top of the original duplicates and the herd gets worse rather than better.

The third failure is subtler and it is the one that actually pages someone. Identical requests issued microseconds apart can return slightly different answers when the upstream is eventually consistent or when the tool has any nondeterminism in it. Two branches of the same agent turn then disagree about what the tool said. You end up debugging a reasoning bug that is really a caching bug. Nobody notices until a user reports that the assistant contradicted itself inside one response.

The naive fix is a synchronized cache. That trades a stampede for a lock convoy and it usually still lets N callers past the miss path at the same time, because the check and the fill are not atomic. What you want is one in flight call per key, every other caller parked on the same result, plus a short TTL so a fresh answer serves the next few requests without hitting the network at all.

## Why I built it

Guava and Caffeine both do loading caches well, and if you already depend on one of them you should probably use it. The problem is that agent code lives in the async world. Loaders return `CompletableFuture`, not values, and the classic loading cache wants a blocking loader on a thread you would rather not tie up. Caffeine has an async cache that fits, but pulling a caching library into a service just to deduplicate a handful of tool calls is a dependency you then have to version, shade and explain in review.

The other reason is cancellation. When you hand every caller a direct reference to the shared future, one caller who times out and cancels can kill the upstream work that four other callers are still waiting on. That is a real production incident and it is easy to write by accident. This file exists to be copied into a service, read in one sitting and trusted, with zero dependencies beyond the JDK.

## When to use it

- An agent turn fans out to search, embeddings and a repo read, and several branches request the same tool with identical arguments.
- You wrap MCP tools behind a Java gateway and one upstream provider rate limits you per API key.
- A hot config or catalog lookup is hit by every request handler and the underlying data changes at most once a minute.
- Model helper calls where the same prompt fragment gets embedded repeatedly inside a single workflow.
- Internal platform RPCs that are cheap individually but get called thousands of times per second with a small set of distinct keys.
- Any place where a timeout on one caller must not cancel work that other callers are still waiting on.

## How it works

The class is `McpCallCoalescer<V>`, parameterised on the value type, with a `String` key you construct yourself. Two concurrent maps carry the state. `inFlight` maps a key to the `CompletableFuture<V>` representing the single call currently running for that key. `cache` maps a key to a `CacheEntry<V>` record holding the value and an absolute `expiresAtMs` deadline. A third structure, `accessOrder`, is a `ConcurrentLinkedQueue<String>` used for eviction bookkeeping.

Everything happens in `get(String key, Supplier<CompletableFuture<V>> loader)`. It reads the cache first. On a fresh hit it offers the key to `accessOrder` and returns an already completed future, so the fast path allocates almost nothing and never touches the loader. On an expired hit it removes the stale entry with the two argument `cache.remove(key, cached)`, which only deletes if the entry is still the one it saw, so a concurrent refresh does not get clobbered.

The coalescing itself is a compare and set on `inFlight`. The caller creates a `pending` future and does `inFlight.putIfAbsent(key, pending)`. If that returns non null, another thread already owns this key, and the current caller gets `shared.thenApply(Function.identity())`. That `thenApply` is the important detail. Each caller receives its own dependent future, so cancelling or timing out one caller's handle does not cancel the shared upstream work that everyone else is still waiting on. The winner of the `putIfAbsent` is the only thread that invokes the loader.

The winner calls `loader.get()`, null checks the returned future and attaches a `whenComplete` callback. That callback removes the key from `inFlight` first, then either completes `pending` exceptionally or writes a new `CacheEntry` with `System.currentTimeMillis() + ttlMillis`, offers the key to `accessOrder`, runs `prune` and completes `pending` with the value. Failures are deliberately not cached, so a broken upstream is retried by the next caller rather than serving a poisoned entry for the whole TTL. A `try/catch (Throwable)` around the loader invocation covers loaders that throw synchronously, and it removes the `inFlight` mapping before failing, so a throwing loader cannot wedge a key permanently.

Errors pass through `unwrap`, which strips a `CompletionException` or `ExecutionException` wrapper and returns the cause. Callers see the real exception instead of the async plumbing.

Eviction lives in `prune(long now)`. It first drops every expired entry with `removeIf`, then while the cache is still over `maxEntries` it polls `accessOrder` for a victim and removes it. This is an approximate LRU, not a strict one. The queue accumulates duplicate keys because every hit offers the key again, and a polled key that is no longer in the cache is simply skipped. That is the deliberate tradeoff: no global lock, bounded memory, ordering that is right most of the time. `cachedSize()` prunes before reporting, and `inFlightCount()` exposes how many distinct calls are running right now, which is the number you graph when you want to prove the coalescer is working.

## Usage

```java
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

// 30 second TTL, at most 500 cached entries
McpCallCoalescer<String> coalescer =
    new McpCallCoalescer<>(Duration.ofSeconds(30), 500);

// Key must encode the tool name AND the arguments
String key = "mcp:search:" + query;

CompletableFuture<String> result = coalescer.get(key, () ->
    mcpClient.callToolAsync("search", Map.of("q", query)));

result.thenAccept(System.out::println);

// Drop one entry after a known write
coalescer.invalidate(key);

// Observability
int cached = coalescer.cachedSize();
int running = coalescer.inFlightCount();

// Nuke the whole cache
coalescer.clear();
```

Compile and use it as a plain class. There is no `main` method and no CLI:

```bash
javac McpCallCoalescer.java
```

## Notes

- Java 17 or newer is required, because `CacheEntry` is a record. No third party dependencies at all.
- Eviction is approximate LRU. `accessOrder` holds duplicate keys and stale entries, so it grows on a hot key until `prune` drains it. Memory is bounded by cache size, not by queue size.
- `prune` only runs on a successful load and inside `cachedSize()`. If nothing is written and nothing is measured, expired entries sit in the map until the next write.
- `invalidate(key)` removes the cache entry only. It does not cancel an in flight call, and a call already running will still write its result to the cache afterwards. `clear()` likewise leaves `inFlight` untouched.
- Failures are never cached. Every caller that arrives after a failure triggers a fresh attempt, so pair this with your own retry budget or circuit breaker if the upstream can fail hard.
- TTL uses `System.currentTimeMillis()`, so a wall clock jump shifts expiry. A `ttl` shorter than one millisecond is clamped to 1 ms, and `maxEntries` below 1 throws `IllegalArgumentException`.
- In memory and single JVM. There is no cross process coordination, so N instances behind a load balancer still make up to N upstream calls per key.
- Key construction is your job. If two logically different calls produce the same string you will serve the wrong cached value.
