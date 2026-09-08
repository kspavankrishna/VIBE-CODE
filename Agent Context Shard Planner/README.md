# Agent Context Shard Planner

A repository is too large for one prompt, so you split it. Split it alphabetically or by directory and the coding agent loses the dependency edges it needed, then patches a file it never saw. This is a Haskell CLI that packs a file manifest into prompt sized shards, keeps strongly related files together and produces the same boundaries every run.

**Language:** Haskell | **Lines:** 1135 | **Added:** 2026-05-07

## What this solves

This solves a real AI coding workflow problem that keeps showing up in 2026: one repo is too large for a single clean prompt, but splitting it badly makes agents lose dependency context and create noisy patches. The hard part is not counting tokens. Anybody can sum a column and cut when the total crosses 32k. The hard part is keeping strongly related files together, isolating huge cycles safely and doing it in a deterministic way so teams can rerun the planner in CI and get stable shard boundaries.

The failure mode looks like this. You fan a codebase out to N agent workers. Chunk 7 contains `src/auth.ts` but not `src/session.ts`, which `auth.ts` imports and mutually depends on. The agent reads the half it was given, infers the rest from the function names and rewrites a signature the file outside its window still calls the old way. Nothing fails at plan time. It fails at merge, or at review when a human has to work out why three workers produced three incompatible diffs of the same subsystem. Every one of them billed you full input tokens for the privilege.

The second failure is churn. A chunker that is non deterministic, or that re-balances whenever one file grows by 200 tokens, never lets your prompt cache warm and makes Tuesday's eval run incomparable to Thursday's. The third is the single giant file: a generated client, a 40k token schema dump. A naive packer drops it, splits it mid function or silently blows the window. Here it is detected, isolated in its own shard and reported as a warning.

## Why I built it

Existing chunkers are either text splitters that know nothing about imports, or code intelligence platforms that want to own your whole build. Very little sits in between: something that takes the token counts and dependency edges you already extracted with your own tokenizer and parser, then does the one job of packing them well. Bin packing with a locality objective deserves a small, well defined tool.

I also wanted the plan to be auditable. A cross shard edge count, per shard utilization and an explicit warning list let you inspect it before spending a dollar on inference, and assert on those numbers in CI. Haskell fit because the whole thing is a pure function from a manifest string to a report: `planContextShards` takes a `Config` and a `String` and returns `Either String Report`. All the IO lives in `main`, `readInput` and `failWith`, so the planner is easy to test and cannot go accidentally order dependent.

## When to use it

- You fan a repo out to parallel coding agents and need each worker to get a self contained slice, not an arbitrary slab.
- You are building a context pack step in CI that must produce identical output across runs so prompt caches stay warm.
- Your monorepo has cyclic modules that every naive splitter tears in half.
- You want to see, before paying for inference, whether a budget yields four shards with eleven cross shard edges or nine shards with two hundred.
- You need an entrypoint or a spec file pinned into the highest priority shard because every agent has to see it first.
- You run an eval harness comparing multi agent plans and need the context partition held constant between arms.

## How it works

Input is a tab separated manifest, four or five columns per line: `path`, `tokens`, `priority`, `deps`, `tags`. `numberedRelevantLines` drops blank lines and `#` comments. `parseManifest` builds an `UnresolvedNode` per line, rejects duplicate paths outright, then `resolveNode` drops self dependencies and any dep pointing outside the manifest. Both drops warn rather than fail, because a real extractor always hands you edges into node_modules and generated files you deliberately excluded. Tags are lowercased into a `Set String`.

Cycles come first. `buildComponents` feeds the nodes into `Data.Graph.stronglyConnComp` and turns each strongly connected component into a `Component`. That is the key structural decision: mutually dependent files are welded into one indivisible group before any packing happens, so an import cycle can never be torn across shards. Each component also carries `compInternalNeighbors`, an undirected adjacency map built by `buildInternalNeighbors` over only the edges internal to that component. Files with no cycles fall out as single member components, which keeps granularity fine everywhere else.

`componentToRawUnits` decides whether a component survives whole. If it fits the usable budget (`cfgMaxTokens` minus `cfgReserveTokens`) and is under `--max-files`, it becomes one unit with an id like `c7`. Otherwise `splitComponent` runs a greedy first fit pass with a locality tie break. `memberOrderKey` sorts members by pinned, entry, internal degree, `scoreFile`, token size and path. `scoreFile` is your priority plus 25.0 for a `pin` tag, 8.0 for `entry`, 1.0 for `test` and a 0.5 penalty for `leaf`. `scoreBin` then ranks open bins by how many of that file's internal neighbors are already inside, then by tightest slack, then by bin number. Pieces get ids like `c7-p2`, and every split warns with the component, its size and the reason it was cut. A file bigger than the budget gets a dedicated bin flagged `binOversize` plus a warning. Pass `--deny-oversize-singletons` and it hard fails instead, which is what you want in CI.

`hydrateUnits` lifts the file level graph to the unit level, building both `unitDeps` and `unitRevDeps` so the packer sees neighbors in both directions. `planShards` folds `placeUnit` over the units, ordered by pinned, entry, neighbor count, priority and size. For each unit, `choosePlacement` scores every shard it fits into with the tuple `(cutEdges, negate colocated, 0, slack, shardFiles, shardId)` and compares that against opening a new shard, scored `(assignedNeighbors, 0, 1, slack, 0, nextShardId)`. Read it left to right: minimize edges cut against neighbors already placed, then maximize neighbors colocated, then prefer an existing shard over a new one, then take the tightest fit. Locality beats packing efficiency by construction. `canFitUnit` enforces the token budget and the file cap, `canCreateNewShard` enforces `--max-shards`, and a unit with nowhere to go fails loudly rather than overflowing quietly.

`buildReport` recomputes cross shard edges from the original file level graph, not the unit graph, so the number you read is the count of real import edges an agent cannot follow inside its window. It reports outgoing and incoming counts per shard, neighbor shard ids, utilization against the usable budget and a per file external dependency count. `renderPretty`, `renderJson` and `renderTsv` are hand rolled, JSON escaper and `showHex4` control character path included, so the binary needs nothing beyond `base` and `containers`.

## Usage

```bash
# build (needs only base + containers, no cabal file required)
ghc -O2 AgentContextShardPlanner.hs -o shardplan

# defaults: 32000 max tokens, 4000 reserved, 24 files per shard, pretty output
./shardplan manifest.tsv

# tune the window, cap the plan, emit JSON for a downstream step
./shardplan --input manifest.tsv \
  --max-tokens 128000 \
  --reserve 8000 \
  --max-files 40 \
  --max-shards 12 \
  --format json > plan.json

# read from stdin, fail hard if any single file exceeds the usable budget
generate-manifest | ./shardplan - --deny-oversize-singletons --format tsv
```

Real columns are separated by single tabs. `deps` and `tags` accept `-` for empty. Example aligned with spaces for readability:

```
# path            tokens  priority  deps                       tags
src/App.tsx       2100    8.5       src/router.ts,src/auth.ts  pin,entry
src/router.ts     900     4.0       src/auth.ts                -
src/auth.ts       1400    6.25      src/router.ts              -
src/util/date.ts  180     0.5       -                          leaf
```

Short flags mirror the long ones: `-i`, `-t`, `-r`, `-f`. A bare positional argument is the input path, and `-` means stdin.

## Notes

- It never reads your source files. Token counts, priorities, edges and tags all come from you, so the plan is exactly as accurate as your tokenizer and import extractor.
- Grouping is by strongly connected component, not weakly connected component. Two files linked by a one way import are separate units and can land in different shards. Only genuine cycles are welded together.
- Errors go to stderr and exit 1: malformed line, duplicate path, non positive token count, negative priority, empty manifest, an oversize file under `--deny-oversize-singletons`, or a unit that fits nowhere once `--max-shards` is reached. Success and `--help` exit 0.
- Placement is greedy, not optimal. It minimizes cut edges against units already placed, so it is a good local decision in a fixed order rather than a global minimum cut.
- `--help` is only recognized as the last argument. Unknown flags starting with `-` are rejected with the usage text, and `--max-tokens` must be strictly greater than `--reserve`.
- Each shard's `neighbors` list covers only the outgoing direction. Incoming edges are a count, not a list.
