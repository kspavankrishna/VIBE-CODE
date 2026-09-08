# Batch Request Sharder

Splitting a queue of LLM requests into batch files by counting to N breaks the moment token limits, byte limits, deadlines and noisy tenants all apply at once. This is a deterministic Haskell sharder that packs a mixed request queue into provider safe batch shards with stable, replayable IDs.

**Language:** Haskell | **Lines:** 593 | **Added:** 2026-05-07

## What this solves

This solves OpenAI Batch API sharding, Anthropic message batch partitioning and the general problem of building safe multi-tenant LLM batch files when token limits, byte limits, deadlines and fairness rules all apply at once. The naive version is always the same: chunk the list every N items and submit. It has four failure modes, and all four show up in production. The first is mixed backends: chunk a queue holding two models or two endpoints and the provider rejects the file outright, or worse, half of it succeeds. The sharder groups on `BatchKey`, which is provider, model, endpoint and optional region, so a shard is by construction submittable to exactly one backend. Those strings are case folded and trimmed first, so `OpenAI ` and `openai` do not silently become two partitions with half the work each.

The second is limit overflow. Batch APIs cap requests per file, tokens per job and raw file size, and counting items guards only the first cap. A chunk of 20,000 short prompts is fine, 20,000 long ones is a rejected job, and you find out after the upload. `fitsExisting` checks all three global caps plus two per tenant caps before any item lands.

The third is tenant starvation. One customer dumps 400,000 enrichment rows into the queue and every batch file for the next hour is theirs. `maxItemsPerTenantPerShard` and `maxTokensPerTenantPerShard` bound the blast radius, and the placement scorer actively spreads a tenant's items across shards rather than merely capping them.

The fourth is the incident review two weeks later, when somebody asks which shard a failed request went into and whether a replay rebuilds the same file. If your shard IDs are UUIDs or timestamps you cannot answer. Here the ID is a FNV-1a 64 bit fingerprint over the partition key, lane, ordinal and every item ID in the shard, so identical input rebuilds an identical ID.

## Why I built it

Every batching library I found either handles one dimension (bin pack by size) or is welded to one provider's SDK. Nothing covered the combination that matters in a model gateway: several caps at once, a deadline aware lane split and multi-tenant fairness, with output stable enough to diff between two planning runs. The bin packing literature is well covered, the operational wrapper around it is not.

This is also the kind of code that gets written badly under deadline pressure, buried in a worker loop, then never revisited. As a pure function with a validated input contract it is testable without a queue, a network or a provider account, and `planBatchShardsAt` takes the clock reading as an argument instead of reading the clock, so a plan is reproducible in a test.

## When to use it

- You are building an OpenAI Batch API or Anthropic message batch submitter and need to split a heterogeneous queue into valid job files.
- Your gateway serves several customers off one API key and one tenant's bulk job keeps crowding out everyone else.
- You run offline inference or dataset enrichment where a few rows have a hard deadline and most do not, and the urgent ones need their own lane.
- A batch job failed and you need to rebuild the exact same shard to replay it, not an approximation of it.
- You want to see, before submitting anything, how full each planned shard is and which tenants are in it.
- You validate a queue in CI and want structured errors instead of a runtime crash on a bad row.

## How it works

Planning runs in three stages: validate, partition, pack. `planBatchShardsAt` takes the current `UTCTime`, a `ShardLimits` record and the raw items, and returns `Either [PlanningError] [BatchShard]`. `validateInputs` normalizes first, then validates the normalized form: `normalizeItem` strips `itemId`, replaces a blank `tenantId` with `"default"`, case folds the four `BatchKey` fields and collapses a blank `payloadRef` to `Nothing`. Three validators then run and their outputs are concatenated, so you get every problem in one pass. `validateLimits` rejects non positive caps and out of order cutoffs, `validateItems` rejects empty required fields and non positive estimates, `duplicateErrors` counts item IDs and flags anything seen twice.

Partitioning uses `partitionKeyAt`, pairing the item's `BatchKey` with an `UrgencyBand`. `urgencyBandAt` takes `floor (diffUTCTime deadline now)` and compares it against `hotCutoffSeconds`, `warmCutoffSeconds` and `normalCutoffSeconds` to get `Hot`, `Warm`, `Normal` or `Backlog`; no deadline means `Backlog`. Items land in a strict `Map PartitionKey [BatchItem]` walked with `Map.toAscList`, so output order comes from the `Ord` instances, not input order. Inside a partition, `sortItems` composes comparators through their `Semigroup` instance: priority descending, then deadline (dated items first, earliest first), then tokens descending, then bytes descending, then item ID as a total tiebreak. Big items first is the decreasing pass of first fit decreasing, and it stops a shard being closed out by a tail of tiny items.

Placement is best fit, not first fit. `choosePlacement` scores every open shard the item legally fits into and takes the minimum. `placementScore` returns a four element tuple compared lexicographically: `tenantPenalty`, the larger of the tenant's projected share of items and of tokens in parts per million, pulling the item toward the shard where its tenant is least dominant; `wasteScore`, a weighted sum of leftover item slots, tokens and bytes, favouring the tightest fit; `negate projectedItems`, preferring the fuller shard on a tie; and `wsOrdinal`, so ties resolve to the lowest numbered shard and nothing depends on traversal luck. If nothing fits, a new `WorkingShard` is appended. Before any of that, `fitsFresh` tests the item against an empty shard and returns `OversizedItem` if even that would not hold it, turning an infinite shard creation loop into a clean error.

Accumulation uses `Data.Sequence` for O(1) appends and a per shard `Map Text TenantUsage` carrying live counts, so the fit checks never rescan shard contents. `buildShardId` then hashes the joined partition fields plus every item ID with FNV-1a 64 (offset basis 14695981039346656037, prime 1099511628211), zero padded to 16 hex characters as `batch-<lane>-<digest>`. FNV-1a is a fast non cryptographic fingerprint for identity and replay here, not a security primitive.

Reporting is separate and pure. `summarizePlan` folds the shards into a `PlanSummary` of totals, per lane counts, maxima and a distinct tenant count. `shardUtilizationPpm` returns the worst of the three global caps in parts per million, so 750000 means the tightest dimension is 75 percent full. `renderShardReport` prints that summary plus a block per shard with lane, backend, totals, utilization, earliest deadline and a per tenant breakdown.

## Usage

There is no `main` in this file, it is a library module. Import it and call `planBatchShardsAt`.

```haskell
{-# LANGUAGE OverloadedStrings #-}

import BatchRequestSharder
import Data.Time (getCurrentTime, addUTCTime)
import qualified Data.Text.IO as TIO

main :: IO ()
main = do
  now <- getCurrentTime
  let key = BatchKey
        { keyProvider = "openai"
        , keyModel    = "gpt-4o-mini"
        , keyEndpoint = "/v1/chat/completions"
        , keyRegion   = Just "us-east-1"
        }
      mk name tenant tokens deadline = BatchItem
        { itemId          = name
        , tenantId        = tenant
        , priority        = 5
        , deadlineAt      = deadline
        , estimatedTokens = tokens
        , estimatedBytes  = tokens * 4
        , batchKey        = key
        , payloadRef      = Just ("s3://jobs/" <> name <> ".json")
        }
      items =
        [ mk "req-1" "acme"   1200 (Just (addUTCTime 600 now))   -- hot
        , mk "req-2" "acme"    900 (Just (addUTCTime 5400 now))  -- warm
        , mk "req-3" "globex"  400 Nothing                       -- backlog
        ]

      -- or override: defaultShardLimits { maxTokensPerShard = 500_000 }
      limits = defaultShardLimits

  case planBatchShardsAt now limits items of
    Left errs    -> mapM_ print errs
    Right shards -> do
      TIO.putStrLn (renderShardReport limits shards)
      mapM_ (\s -> print (shardId s, shardUtilizationPpm limits s)) shards
```

`defaultShardLimits` ships 20,000 items, 3,500,000 tokens and 80,000,000 bytes per shard, 10,000 items and 1,500,000 tokens per tenant per shard, with lane cutoffs at 15 minutes, 2 hours and 24 hours.

## Notes

- It plans, it does not submit. No HTTP client, no JSON encoder, no NDJSON writer. You take `shardItems` and serialize it for whichever provider you target.
- Token and byte figures are your estimates. A low tokenizer estimate yields a shard the planner calls valid and the provider calls oversized, so pad your numbers.
- Validation errors accumulate, placement errors do not. `Either` short circuits inside the packing fold, so one `OversizedItem` ends that partition and the first failing partition ends the plan.
- Packing is O(items x open shards) worst case, and `placeItem` uses `!!`, `replaceAt` and list append on the shard list. Fine for hundreds of shards per partition, not for tens of thousands.
- An item whose deadline has already passed lands in `Hot`, since the computed seconds go negative. There is no separate overdue lane. Shard IDs cover membership and ordinal, so adding one item renames every shard it touches: deliberate for replay identity, but IDs are not stable across a changed queue.
- Needs only base, containers, text, time and bytestring, plus `NumericUnderscores` for literals like `3_500_000`: free under GHC2021, explicit under `default-language: Haskell2010`.
