# Consistent Hash Rebalance Auditor

A V command line tool that tells you how much cache, shard or GPU routing traffic will actually move when you add, remove or reweight nodes. It replays the same keys through ring hashing, weighted rendezvous hashing, jump hash and Maglev, then compares each result to the theoretical minimum.

**Language:** V | **Lines:** 743 | **Added:** 2026-10-14

## What this solves

Everyone says "we use consistent hashing" and stops thinking. Then a node leaves, the cache hit rate falls off a cliff and nobody can say why. The usual reason is that "consistent hashing" covers four quite different algorithms, and each one has its own way of failing when the node list changes.

Jump hash is the clearest example. It is fast and almost perfectly even, but it only knows a count of buckets. Remove a node from the middle of your list and every bucket after it shifts down by one position. In the test run I did with ten nodes and the fourth one removed, jump hash moved 68.9% of all keys when the minimum possible was 10%. That is a 6.9x excess, and it is a cache stampede waiting for a bad Tuesday. The same algorithm moves exactly the minimum when you add a node at the tail. So whether it is safe depends on which node you remove, and a spreadsheet will never tell you that.

Ring hashing has the opposite problem. Movement is close to minimal, but load is lumpy. With 160 virtual points per node the busiest node in my run carried 1.14x its fair share. Maglev is very even and moves almost the minimum, but it leaks a small amount of traffic between nodes that did not need to change. Weighted rendezvous is even and minimal but costs one hash per node per lookup.

This tool measures all of that on your real node lists and your real keys. It answers four questions with numbers:

1. How much traffic moves compared with the least that could move?
2. How much of the movement is needless, meaning it went off a node that lost no share or onto a node that gained none?
3. How uneven is the load before and after?
4. Does any node end up with no keys at all?

It exits with code 2 when a threshold you set is broken, so you can put it in a pipeline that reviews changes to a node list.

## Why I built it

I kept seeing the same incident in different clothes. A team resizes a prefix cache or a KV cache tier, or drains a GPU pool behind a router, and a chunk of the warm state goes cold. In LLM serving this hurts more than it used to. Routing by prompt prefix only pays off while the prefix lands on the replica that already holds it. Move 60% of prefixes and you pay for 60% of your prefill again, all at once.

Most write ups on consistent hashing give you a diagram and a paragraph of theory. What I wanted was a tool I could point at two text files and a sample of production keys and get a plain verdict. I also wanted the comparison to be fair. Weighted nodes make that hard, because a node with weight 4 should own four times the keys, and "minimum movement" is no longer the number of keys on the removed node. It is the total share that has to change hands. The audit computes that figure properly and judges every algorithm against it.

I wrote it in V because the point is a small dependency free binary you can drop into a CI image. It is one file, it compiles in a second and the standard library is enough.

## When to use it

Use it before you change a node list that sits behind a hash based router. Typical cases:

- Resizing a Redis, Memcached or in process cache fleet.
- Draining or adding replicas in a prefix affinity LLM router.
- Moving a sharded vector index or a queue partition map.
- Choosing between ring, rendezvous, jump and Maglev for a new service.
- Reviewing a pull request that edits a node inventory file.

Do not use it as a load test. It measures where keys land, not how fast the machines answer. It also does not model replication. It reports the primary owner of each key only.

## How it works

**Hashing.** `hash64` folds a string with FNV-1a and passes it through `mix64`, a splitmix64 finalizer. The `-seed` option is xored in, so you can run the same audit under a different hash universe and see whether a verdict depends on lucky placement. Keys come from `load_key_hashes`, which hashes either the lines of your `-keys` file or `key:0` to `key:N` for the synthetic default of 200000 keys (`default_count`).

**Node files.** `load_nodes` reads one node per line as `name` or `name weight`. It strips `#` comments, rejects duplicate names, rejects weights that are zero, negative, NaN, infinite or above 1e9, and reports the file and line number of every problem. A file with no nodes is an error.

**Routers.** `build_router` builds one router per algorithm. Ring, rendezvous and Maglev sort nodes by name first, so the order in your file never changes the answer. Jump hash keeps your order on purpose, because order is the thing it depends on.

- Ring: each node gets `round(weight * vnodes)` points, at least one, hashed from `name#index`. Points are sorted by hash with ties broken by node index. `Router.route` finds the first point at or after the key hash with a binary search and wraps at the end.
- Rendezvous: `route` scores every node with `weight / -ln(u)`, where `u` comes from `unit_open`, which maps a hash into the open interval (0, 1) so the logarithm is always finite. Highest score wins. This is the standard weighted form and it gives each node a share proportional to its weight.
- Jump: `jump_bucket` is the published jump consistent hash loop. Weights are ignored and the report says so, and the effective weights it is judged against are all equal.
- Maglev: `populate_maglev` builds the lookup table the way the paper describes. Each node has an offset and a skip derived from its name, walks its own permutation and claims the next free slot in turn. A node with more weight earns more turns per round through a credit counter. The table size must be prime and at least the node count. The check is `is_prime`, and the default is 65537 (`default_table`).

**Analysis.** `analyze` builds a router for the before list and one for the after list, then sends every key through both. It keeps a universe of all node names so keys can be compared across the two lists even though each router numbers its nodes differently. For each key that changes owner it records a loss and a gain. From that it reports:

- `moved_fraction`: keys that changed owner.
- `ideal_fraction`: the minimum possible, computed as the sum over nodes of the share gained, using normalised weights. For a removal this is the removed node's share. For a pure reweight it is the total share that must shift.
- `excess_ratio`: moved divided by ideal. If nothing had to move but keys did, `moved_without_ideal` is set and the ratio is undefined.
- `needless_moves`: moves where the source node did not lose share or the destination did not gain any. This is what exposes jump hash after a mid list removal and the small leak in Maglev.
- `imbalance_before` and `imbalance_after`: the busiest node's load divided by its weight adjusted fair share.
- `min_load_after`, `cv_after` and `starved_nodes`: the quietest node, the spread of load and any live node that owns zero keys.

**Gates.** `gate` compares each report to `-max-excess`, `-max-needless` and `-max-imbalance` and `run` returns `exit_gate` (2) if any fail. Bad input returns `exit_input` (1). A clean run returns `exit_ok` (0).

## Usage

Build it:

```
v -prod -o rebalance-audit ConsistentHashRebalanceAuditor.v
```

Two node files, one name per line with an optional weight:

```
# before.txt
cache-00
cache-01
cache-02 2
cache-03
```

Compare them with every algorithm:

```
./rebalance-audit before.txt after.txt
```

Use your own keys, which is far better than the synthetic set. Repeat a key to give it more traffic weight:

```
./rebalance-audit before.txt after.txt -keys prefix_sample.txt -algo ring,rendezvous,maglev
```

Gate a pipeline. This fails the job if any chosen algorithm moves more than 1.25x the minimum, wastes more than 1% of traffic or leaves a node above 1.2x its fair share:

```
./rebalance-audit before.txt after.txt -algo rendezvous,maglev -max-excess 1.25 -max-needless 0.01 -max-imbalance 1.2
```

Options: `-algo`, `-keys`, `-count`, `-vnodes`, `-table`, `-seed`, `-top`, `-json`, `-max-excess`, `-max-needless`, `-max-imbalance`. Run with `-h` for the full text. With `-json` the output is a single object holding `reports` and `failures`, one report per algorithm.

## Notes

- Results are for one hash seed. Run two or three seeds. If a verdict flips between seeds, your margin is too thin for the ring or Maglev settings you picked.
- Ring imbalance shrinks as you raise `-vnodes`, at the cost of memory and a slower build. Try 160, 400 and 1000 and watch `imbalance_after`.
- Maglev needs a table much larger than the node count. A common rule is at least 100 times the nodes. Small tables raise both imbalance and the needless move rate.
- Jump hash is not wrong, it is specialised. If you only ever append and remove at the tail, it is the best of the four. The tool will show it at 1.0x. If you ever remove from the middle, it will show the damage.
- Weighted rendezvous is the most accurate and the slowest per lookup. For a few hundred nodes that rarely matters.
- A key file with a heavy hot key will make load look worse than a uniform sample does. That is the real picture, not a bug.
- The tool reads files and prints a report. It never writes anything and makes no network calls.
- I compiled and ran it with V 0.5.2. The numbers quoted above come from a ten node run with 200000 synthetic keys and seed 0.
