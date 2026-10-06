# Prefix Affinity Ring

An Erlang gen_server that routes LLM requests to GPU replicas so the same prompt prefix keeps landing on the same machine, without letting one hot system prompt melt that machine. It uses weighted rendezvous hashing, bounded load spilling, health ejection with half open probes and lease tracking.

**Language:** Erlang | **Lines:** 692 | **Added:** 2026-10-06

## What this solves

Modern inference servers such as vLLM and SGLang keep a prefix cache. If two requests start with the same system prompt, tool schema or few shot block, the second one skips most of the prefill work and the time to first token drops hard. That only works when both requests reach the same replica. A plain round robin or least connections balancer scatters your traffic over every replica, so each replica has to build the same cache entries on its own and your hit rate falls to roughly one divided by the number of replicas.

The usual fix is consistent hashing on something like the user id or the system prompt. That has three problems in production:

1. One popular prefix, for example the system prompt of your biggest product, hashes to one replica. That replica saturates while the rest sit idle. Hashing alone has no idea about load.
2. When a replica dies or you scale up, a naive modulo hash reshuffles almost every key and you throw away the whole fleet cache at once.
3. A replica that is returning errors keeps receiving its share of keys, because the hash does not care that it is sick. When it comes back you either hammer it at once or never send it traffic again.

Prefix Affinity Ring handles all three in one small OTP process. It ranks replicas per key using weighted rendezvous hashing, spills to the next replica in that same stable order only when the preferred one is over its fair share, and takes sick replicas out of rotation with a backoff and a single probe request to bring them back.

## Why I built it

I kept seeing the same pattern in agent and RAG setups. Teams put a prefix cache in the inference server, then put an ordinary load balancer in front and wondered why the cache hit rate stayed low. When they tried hash based routing they hit the hot prefix problem within a week and went back to round robin. The two ideas that fix this properly are old and well known: rendezvous hashing (Thaler and Ravishankar) for the stable ranking and bounded loads (Mirrokni, Thorup and Zadimoghaddam) for the spill rule. I could not find a small, dependency free implementation that also dealt with the operational parts: leases that get cleaned up when a caller crashes, a probe that does not stampede a recovering replica, and a drain that waits for in flight work.

I wrote it in Erlang because the problem is mostly bookkeeping under concurrency: who holds a lease, what happens when the holder dies, when does a timer fire. The BEAM gives monitors, a single threaded state owner and cheap timers for free, so the code stays short and the failure behaviour is easy to reason about. The module is a plain gen_server with no dependencies beyond the crypto application that ships with OTP.

## When to use it

Use it when you run several inference replicas behind your own gateway and you care about prefix cache hits. Typical cases:

* A fleet of vLLM or SGLang pods with prefix caching on.
* An agent platform where every request carries a long shared system prompt plus tool definitions.
* A RAG service where many requests share the same retrieved document header.
* Any gateway that wants affinity but must not create a hot spot.

Do not use it if your replicas have no local cache, because then plain least loaded routing is simpler and just as good. Also note it is a routing core, not a proxy. It tells you which replica to call and keeps count of what is in flight. You still make the HTTP call yourself.

## How it works

**Keys.** `prefix_key/3` builds the routing key from a tenant and the prompt bytes. It cuts the prompt into complete blocks of `block_bytes` (default 256), keeps the first `depth` blocks (default 4) and hashes them with SHA 256 together with a length prefixed tenant. Two prompts that share the first `depth` blocks get the same key, which is the point. If the prompt is shorter than one block the whole prompt is hashed. The tenant is part of the hash on purpose: it stops one tenant from steering its traffic onto the replica that holds another tenant's cache, which matters because prefix cache hits are observable through latency. If your inference engine already exposes block hashes use `prefix_key_from_blocks/3`, which takes a list of binaries and a depth and frames each block with its length so that `[<<"ab">>]` and `[<<"a">>, <<"b">>]` never collide.

**Ranking.** Each replica has a seed derived from its id with `seed/1`. For a key, `score/2` mixes the key hash with the seed using the `mix64/1` finaliser, turns the result into a number U between 0 and 1 and returns `weight / -ln(U)`. The replica with the highest score wins. This is weighted rendezvous hashing: each replica wins a share of keys proportional to its weight, and removing a replica only moves the keys that replica owned. `ranked/2` sorts replicas by score with the id as a tie break, so the order is fully deterministic. The pure function `owner/2` exposes this ranking for tests and offline analysis.

**Bounded load.** `acquire/2` calls `choose/3`, which walks the ranked list. For each replica `walk/6` computes a limit of `ceil(load_factor * (total_inflight + 1) * weight / weight_sum)` using only healthy replicas, and accepts the replica if one more request keeps it under that limit. With the default `load_factor` of 1.25 no replica ever carries more than 25 percent over its fair share. A hot prefix therefore fills its preferred replica to that bound, then spills to the second replica in its own stable order, then the third. The spill target for a key is always the same, so even overflow traffic builds a small, reusable cache set instead of spraying everywhere. The reply is `{ok, Lease, ReplicaId, Rank}` where `Rank` is zero for the preferred replica, so you can log expected cache hits against real ones.

**Overflow and errors.** If every healthy replica is over its soft limit, `overflow/3` picks the one with the lowest inflight to weight ratio that is still under its hard `max_inflight`. If nothing is under the hard cap you get `{error, overloaded}`. If there are no replicas at all you get `{error, no_replicas}`. If replicas exist but all are ejected or probing you get `{error, no_healthy_replicas}`. These are different problems and callers should handle them differently.

**Health.** `release/3` takes `ok`, `cancelled` or `{error, Reason}`. The `health_after/5` function counts consecutive errors per replica. At `failure_threshold` (default 3) `eject/3` takes the replica out of rotation for `base_cooldown_ms` times two to the power of its ejection level, capped at `max_cooldown_ms`, plus a deterministic jitter of up to 20 percent so a whole fleet of gateways does not retry in the same millisecond. Once the cooldown passes, `availability/2` reports the replica as `probe` and the next request that ranks it first becomes the probe lease. While a probe is out no other request reaches that replica. A successful probe resets health and level. A failed probe ejects again with a longer cooldown. A cancelled probe goes back to probe ready without punishment. Late results from leases issued before the ejection cannot flip the state, because only the lease named in the probing state can recover it.

**Leases.** Every acquire monitors the calling process. If the caller dies the `DOWN` handler releases the lease as `cancelled`, which is neutral for health because a crashed client is not the replica's fault. Leases also carry a deadline of `lease_ttl_ms` (default 120000). `do_sweep/1` runs on a timer or when you call `sweep/1`, and expires overdue leases as errors, since a request that never finished is evidence of a hung replica. A second release of the same lease returns `{error, unknown_lease}`, so double release is safe.

**Fleet changes.** `add_replica/2` and `update_replica/3` change the fleet live. `drain/2` removes a replica from ranking at once so its keys move, then deletes it when its last lease finishes. `remove_replica/2` is the forced version. `snapshot/1` returns per replica load and health plus counters such as `affinity`, `spilled`, `overflow`, `probes`, `rejected`, `ejections` and `expired`.

**Tests.** `selftest/0` runs nine checks without any test framework: key framing, minimal disruption when a replica is removed (every key that did not belong to it keeps its owner), weighted share, the bounded load invariant under a single hot key, the full eject and probe cycle on an injected clock, lease cleanup on caller death, lease expiry, drain and option validation.

## Usage

Compile and run the self test:

```
erlc -o ebin PrefixAffinityRing.erl
erl -noshell -pa ebin -eval "io:format(\"~p~n\", ['PrefixAffinityRing':selftest()]), halt()."
```

Start the router and route a request:

```erlang
{ok, Router} = 'PrefixAffinityRing':start_link(#{
    replicas => [
        #{id => <<"gpu-a">>, weight => 2, max_inflight => 32},
        #{id => <<"gpu-b">>, weight => 1, max_inflight => 16}
    ],
    load_factor => 1.25,
    failure_threshold => 3
}),

Key = 'PrefixAffinityRing':prefix_key(<<"acme">>, SystemPromptAndUserMessage),

case 'PrefixAffinityRing':acquire(Router, Key) of
    {ok, Lease, ReplicaId, Rank} ->
        Result = call_replica(ReplicaId, Request),
        Outcome = case Result of
            {ok, _} -> ok;
            {error, Why} -> {error, Why}
        end,
        'PrefixAffinityRing':release(Router, Lease, Outcome);
    {error, overloaded} ->
        shed_load();
    {error, Reason} ->
        fail_fast(Reason)
end.
```

Here `call_replica/2`, `shed_load/0` and `fail_fast/1` are yours. Operations:

```erlang
'PrefixAffinityRing':add_replica(Router, #{id => <<"gpu-c">>, weight => 1}),
'PrefixAffinityRing':update_replica(Router, <<"gpu-a">>, #{weight => 3}),
'PrefixAffinityRing':drain(Router, <<"gpu-b">>),
'PrefixAffinityRing':preview(Router, Key),
'PrefixAffinityRing':snapshot(Router).
```

All start options are optional: `load_factor`, `failure_threshold`, `base_cooldown_ms`, `max_cooldown_ms`, `lease_ttl_ms`, `sweep_ms` (an integer or `infinity`), `clock` (a zero arity fun returning milliseconds) and `replicas`. Use `start_link/2` with an atom to register the process under a name.

## Notes

* Replica ids must be binaries or atoms. Keep them stable across restarts, because the id is what the hash is built on. Renaming a replica moves its keys.
* Choose `depth` times `block_bytes` to cover your shared system prompt and little more. Too shallow and unrelated requests collide on one replica. Too deep and users of the same system prompt split across replicas and lose the cache.
* The soft limit uses only healthy replicas, so when one is ejected the others get larger fair shares automatically. The hard `max_inflight` still protects each one.
* The state lives in one process. That is deliberate: acquire and release are a few map lookups and a sort over the replica list, which is fine for hundreds of replicas and tens of thousands of requests a second. If you need more, run one router per gateway node. Each node then keeps its own counts, so the global bound becomes approximate. The ranking stays identical on every node because it depends only on the key and the replica ids.
* Rendezvous scoring costs one hash mix per replica per request. There is no ring to rebuild when the fleet changes.
* Failures are never hidden. A rejected acquire is counted and returned, an expired lease counts as an error against its replica, and nothing retries behind your back. Retry policy belongs to the caller.
* It does not talk to the network and does not read your prompts beyond hashing the leading bytes. Nothing is stored except counters and lease records.
