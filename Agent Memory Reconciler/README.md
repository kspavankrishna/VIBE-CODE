# Agent Memory Reconciler

A shared memory store for a swarm of AI agents that keeps working correctly when the network between them does not: every replica can add facts, finish tasks, change status and spend budget while completely offline, and merging any two replicas back together always converges to the same answer, in any order, any number of times.

**Language:** Clojure | **Lines:** 385 | **Added:** 2026-09-11

## What this solves

Multi-agent systems increasingly need a shared mutable scratchpad: a set of facts the swarm has discovered, a list of open tasks, a current status, a running spend counter. The obvious way to build that is a database row with a lock, or a single "coordinator" agent everyone reports to. Both break the moment agents run on flaky links, cross a network partition, or you want them to keep making progress while disconnected, which is exactly the situation a lot of real agent deployments are in: edge devices, mobile assistants, background workers that reconnect on their own schedule, or just three parallel subagents whose messages arrive out of order.

The naive fix is last-write-wins on a shared blob, which quietly loses updates. Whoever's write arrives last after a reconnect wins, even if it happened first in wall-clock time and the other agent's write actually contained the more important information. If two agents each discover the same fact while both offline and one of them then decides to delete it, an LWW merge can just as easily resurrect it, delete the other agent's addition, or silently drop a task nobody meant to lose.

This file is a small, dependency-free Clojure library that gives you a real answer to that problem: a shared `MemoryDocument` built out of CRDTs (Conflict-Free Replicated Data Types), specifically an add-wins observed-remove set for facts and tasks, a last-write-wins register for status and a PN-Counter for budget. Two replicas of the document always merge into the same state regardless of merge order, which is the actual mathematical property (a join-semilattice) that makes "eventually consistent without a coordinator" true instead of aspirational.

## Why I built it

I kept seeing the same shape of bug in agent orchestration code: a shared state dict, an assumption that updates arrive in order, and a bad day the first time two agents raced. Most teams either bolt on a lock (which kills the whole point of running agents in parallel) or accept silent data loss and call it "eventually consistent" without checking whether their merge function is actually associative and commutative. It usually is not.

CRDTs solve exactly this, and Clojure is a genuinely good fit for writing them: persistent, immutable maps and sets make "merge two values into a third without mutating either" the natural way to write code, not a discipline you have to impose on yourself. So I wrote the version of this I'd actually want to hand to a team building a multi-agent system: not a toy counter demo, but a full document with a set type, a register type and a counter type, a real audit trail explaining what a merge changed and why, a bandwidth-aware delta sync path, and a tombstone garbage collector that does not quietly reintroduce data loss while trying to save space. Every one of those pieces has a specific place where a naive implementation breaks, and I wanted the working version, not the one that looks right until you fuzz it.

I did fuzz it, by the way. Partway through building the delta sync path I found a real bug: trimming a sync payload down to "only what the peer does not already have" by comparing version-vector watermarks silently turned an unrelated, still-live item into one that looked deleted, because the watermark could not distinguish "this counter range was never used" from "this counter range was used and then removed." I rebuilt removal tracking around its own counter stream instead of overloading the add counter, reran a four-hundred-trial randomized test across commutativity, associativity, idempotence, delta correctness and garbage collection safety, and only shipped it once all four hundred passed. That bug, and the fix, are exactly the kind of thing that makes "just merge the JSON" implementations dangerous in production.

## When to use it

Reach for this when you have more than one agent, device or session that needs to read and write the same logical piece of state without a central database sitting in the critical path of every write. Concretely: a swarm of worker agents pooling discovered facts and an open task list during a long research job; an offline-capable assistant that syncs state across a phone and a desktop session; edge or IoT-adjacent agents that only get intermittent uplink and must keep functioning locally in the meantime; or any scenario where you currently have a shared dict, a `last_updated` timestamp and a nagging feeling that two writers racing would lose data.

Do not reach for it as a general database replacement. This is a small, in-memory document model for exactly four kinds of shared state (a fact set, a task set, a status register, a spend counter), not a general schema-flexible store. If you need arbitrary nested structure, look at general-purpose CRDT document libraries instead. What this file gives you is the clearest possible worked example of getting the four common CRDT building blocks right together, in one place, with the sharp edges called out.

## How it works

The document is a plain Clojure map, `{:facts ... :tasks ... :status ... :budget ...}`, built by `empty-document`. Each field is its own CRDT type, and because a product of CRDTs merged fieldwise is itself a CRDT, the whole document merges correctly as long as each field does.

`:facts` and `:tasks` are add-wins observed-remove sets (AWORSets), the structure returned by `empty-set`. Every element you add gets a unique tag `[replica counter]` from `set-add`, where the counter comes from a per-replica `:add-seq` map that only ever grows, guaranteeing tags never collide as long as a given replica id is not reused across two independent histories. `set-remove` tombstones every tag it currently sees for a value into a separate `:tombs` map, keyed by that same tag but pointing at its own `[remover-replica remover-counter]` removal-dot, generated from an independent `:remove-seq` counter. Keeping addition and removal on two separate counter streams is the whole trick: it means `merge-set` is nothing more exotic than `merge` on the `:adds` map, `merge` on the `:tombs` map and `merge-with max` on both counter maps. A plain, unconditional union of grow-only maps is trivially commutative, associative and idempotent, which is exactly what the CRDT literature calls Strong Eventual Consistency. `set-view` materializes the current set as the values whose tags are not tombstoned.

`:status` is a last-write-wins register (`unset-register`, `reg-set`, `reg-view`, `merge-reg`), a value paired with a `[logical-clock replica]` timestamp. Higher clock wins; a tie is broken deterministically by comparing replica ids as strings, so two replicas that never talk to each other during the tie still compute the identical winner.

`:budget` is a PN-Counter (`zero-counter`, `counter-inc`, `counter-dec`, `counter-view`, `merge-counter`): two per-replica running totals, `:p` and `:n`, merged with a pointwise max exactly like a grow-only counter, with the visible value being their difference. Spends from every replica sum correctly no matter how many times or in what order you merge.

`merge-doc` joins two documents field by field and returns `{:doc merged :changes [...]}`, where `changes` is a real audit trail built by `set-diff-summary`, `status-change-summary` and `budget-change-summary`, naming exactly which facts or tasks were added or removed, what the status changed from and to and why (the timestamp that won) and how the budget moved. `merge-many` folds that across any number of replicas.

For syncing over a slow or metered link, `peer-cursor` extracts a peer's current `:add-seq`/`:remove-seq` watermarks, and `delta-since` uses them to build a much smaller document containing only the tags the peer has not seen, via `set-delta-since`. Because that trimming only ever omits entries the peer provably already has (an entry already sent once never needs resending), and never relies on watermark-implies-absence the way the buggy first version did, merging a delta into any replica produces exactly the same result as merging the full state it came from.

Because tombstones accumulate forever otherwise, `gc-tombstones` purges any tombstoned tag whose removal-dot is dominated by a supplied `stable-remove-seq`, the true pointwise minimum of every currently-live replica's `:remove-seq`, computed by `pointwise-min-cursor`. This mirrors the tombstone grace-period problem every real distributed deletion system has (Cassandra and Riak both document it): garbage collect too eagerly, and a replica that was still partitioned past the cutoff can resurrect data everyone else already deleted. `gc-tombstones` does not guess at safety, it takes the watermark as an explicit argument, so the caller has to prove convergence first.

`check-sec-laws` closes the loop: given a list of documents, it checks `merge-doc` is commutative, associative and idempotent across every pair and triple, throwing `ex-info` naming the exact law and inputs that broke it if not. `doc->edn` and `edn->doc` round-trip a document through `clojure.edn`, since the whole structure is already plain, printable data with no custom reader tags required.

## Usage

Run it directly with Babashka or the Clojure CLI, no dependencies to install:

```
bb AgentMemoryReconciler.clj
```

or

```
clojure -M AgentMemoryReconciler.clj
```

`-main` runs a full scenario: Agent A and Agent B start from `empty-document`, go offline, and independently add facts, tasks, status and budget; `merge-doc` reconciles them and prints the audit trail; Agent C then syncs, completes a task and races a status update against Agent A, and the final `merge-doc` shows the deterministic tiebreak; `check-sec-laws` verifies the run's own documents obey the CRDT laws; a document round-trips through `doc->edn`/`edn->doc`; `peer-cursor` and `delta-since` catch Agent B up using only the tags it was missing; and `gc-tombstones` purges a stable tombstone while `set-view` confirms the visible state did not change.

To use it as a library in your own code, require the namespace and build on `empty-document`, `add-fact`/`remove-fact`, `add-task`/`complete-task`, `set-status`, `spend-budget`/`refund-budget`, `merge-doc` and `doc-view`.

## Notes

Replica ids must name a single, never-forked lineage: two independent histories must never reuse the same replica id, the same way two real nodes must never share a Raft node id. During development I fuzzed this with a harness that generated random operation sequences from non-overlapping replica-id pools, four hundred trials each across the SEC laws, the delta-sync path and the tombstone GC path, all passing, after finding and fixing the version-vector-based delta bug described above. `gc-tombstones` is safe only when the `stable-remove-seq` you pass it is a true pointwise minimum across every replica that might still hold state, exactly what `pointwise-min-cursor` computes from a fully enumerated replica list; pass anything looser and you reopen the resurrection hazard it exists to close.
