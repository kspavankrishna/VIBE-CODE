import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

public final class McpCallCoalescer<V> {
    private record CacheEntry<V>(V value, long expiresAtMs) {}

    private final ConcurrentHashMap<String, CompletableFuture<V>> inFlight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CacheEntry<V>> cache = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> accessOrder = new ConcurrentLinkedQueue<>();
    private final long ttlMillis;
    private final int maxEntries;

    public McpCallCoalescer(Duration ttl, int maxEntries) {
        this.ttlMillis = Math.max(1L, Objects.requireNonNull(ttl, "ttl").toMillis());
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be at least 1");
        }
        this.maxEntries = maxEntries;
    }

    public CompletableFuture<V> get(String key, Supplier<CompletableFuture<V>> loader) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(loader, "loader");
        long now = System.currentTimeMillis();
        CacheEntry<V> cached = cache.get(key);
        if (cached != null && cached.expiresAtMs() > now) {
            accessOrder.offer(key);
            return CompletableFuture.completedFuture(cached.value());
        }
        if (cached != null) {
            cache.remove(key, cached);
        }

        CompletableFuture<V> pending = new CompletableFuture<>();
        CompletableFuture<V> shared = inFlight.putIfAbsent(key, pending);
        if (shared != null) {
            return shared.thenApply(Function.identity());
        }

        try {
            CompletableFuture<V> upstream = Objects.requireNonNull(loader.get(), "loader returned null");
            upstream.whenComplete((value, error) -> {
                inFlight.remove(key, pending);
                if (error != null) {
                    pending.completeExceptionally(unwrap(error));
                    return;
                }
                cache.put(key, new CacheEntry<>(value, System.currentTimeMillis() + ttlMillis));
                accessOrder.offer(key);
                prune(System.currentTimeMillis());
                pending.complete(value);
            });
        } catch (Throwable error) {
            inFlight.remove(key, pending);
            pending.completeExceptionally(error);
        }
        return pending.thenApply(Function.identity());
    }

    public void invalidate(String key) {
        cache.remove(key);
    }

    public void clear() {
        cache.clear();
        accessOrder.clear();
    }

    public int cachedSize() {
        prune(System.currentTimeMillis());
        return cache.size();
    }

    public int inFlightCount() {
        return inFlight.size();
    }

    private void prune(long now) {
        cache.entrySet().removeIf(entry -> entry.getValue().expiresAtMs() <= now);
        while (cache.size() > maxEntries) {
            String victim = accessOrder.poll();
            if (victim == null) {
                return;
            }
            CacheEntry<V> entry = cache.get(victim);
            if (entry != null && (entry.expiresAtMs() <= now || cache.size() > maxEntries)) {
                cache.remove(victim, entry);
            }
        }
    }

    private Throwable unwrap(Throwable error) {
        if ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null) {
            return error.getCause();
        }
        return error;
    }
}

/*
This solves duplicate MCP and agent tool calls piling up when several parts of a Java service ask for the same expensive result at nearly the same time. Built because modern agent backends fan out to search, embeddings, repo reads, and hosted tools, then accidentally stampede the same endpoint with identical arguments. Use it when you want one in-flight call per key plus a short TTL cache for fresh results. The trick: every caller gets its own dependent future, so one timeout or cancellation does not kill the shared upstream work. Drop this into any Java 17+ service that wraps MCP tools, HTTP fetchers, model helpers, or internal platform RPCs.
*/