{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module BatchRequestSharder
  ( BatchKey(..)
  , BatchItem(..)
  , ShardLimits(..)
  , UrgencyBand(..)
  , TenantUsage(..)
  , BatchShard(..)
  , PlanSummary(..)
  , PlanningError(..)
  , defaultShardLimits
  , planBatchShardsAt
  , summarizePlan
  , renderPlanSummary
  , renderShardReport
  , shardUtilizationPpm
  ) where

import Data.Bits (xor)
import Data.Foldable (toList)
import Data.List (foldl', sortBy)
import qualified Data.Map.Strict as Map
import Data.Map.Strict (Map)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Ord (Down(..), comparing)
import qualified Data.Sequence as Seq
import Data.Sequence (Seq, (|>))
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time (UTCTime, diffUTCTime)
import qualified Data.ByteString as BS
import Data.Word (Word64)
import Numeric (showHex)

data BatchKey = BatchKey
  { keyProvider :: !Text
  , keyModel :: !Text
  , keyEndpoint :: !Text
  , keyRegion :: !(Maybe Text)
  } deriving (Eq, Ord, Show)

data BatchItem = BatchItem
  { itemId :: !Text
  , tenantId :: !Text
  , priority :: !Int
  , deadlineAt :: !(Maybe UTCTime)
  , estimatedTokens :: !Int
  , estimatedBytes :: !Int
  , batchKey :: !BatchKey
  , payloadRef :: !(Maybe Text)
  } deriving (Eq, Show)

data ShardLimits = ShardLimits
  { maxItemsPerShard :: !Int
  , maxTokensPerShard :: !Int
  , maxBytesPerShard :: !Int
  , maxItemsPerTenantPerShard :: !Int
  , maxTokensPerTenantPerShard :: !Int
  , hotCutoffSeconds :: !Int
  , warmCutoffSeconds :: !Int
  , normalCutoffSeconds :: !Int
  } deriving (Eq, Show)

data UrgencyBand
  = Hot
  | Warm
  | Normal
  | Backlog
  deriving (Eq, Ord, Show, Enum, Bounded)

data TenantUsage = TenantUsage
  { tenantItemCount :: !Int
  , tenantTokenTotal :: !Int
  } deriving (Eq, Show)

data BatchShard = BatchShard
  { shardId :: !Text
  , shardOrdinal :: !Int
  , shardKey :: !BatchKey
  , shardUrgency :: !UrgencyBand
  , shardItems :: ![BatchItem]
  , shardItemTotal :: !Int
  , shardTokenTotal :: !Int
  , shardByteTotal :: !Int
  , shardTenantUsage :: !(Map Text TenantUsage)
  } deriving (Eq, Show)

data PlanSummary = PlanSummary
  { totalShards :: !Int
  , totalItems :: !Int
  , totalTokens :: !Int
  , totalBytes :: !Int
  , hotShardCount :: !Int
  , warmShardCount :: !Int
  , normalShardCount :: !Int
  , backlogShardCount :: !Int
  , maxObservedItems :: !Int
  , maxObservedTokens :: !Int
  , maxObservedBytes :: !Int
  , tenantCount :: !Int
  } deriving (Eq, Show)

data PlanningError
  = InvalidLimit !Text
  | EmptyField !Text !Text
  | NonPositiveEstimate !Text !Text !Int
  | DuplicateItemId !Text
  | OversizedItem !Text !Text
  deriving (Eq, Show)

data PartitionKey = PartitionKey !BatchKey !UrgencyBand
  deriving (Eq, Ord, Show)

data WorkingShard = WorkingShard
  { wsOrdinal :: !Int
  , wsKey :: !BatchKey
  , wsUrgency :: !UrgencyBand
  , wsItems :: !(Seq BatchItem)
  , wsItemTotal :: !Int
  , wsTokenTotal :: !Int
  , wsByteTotal :: !Int
  , wsTenantUsage :: !(Map Text TenantUsage)
  } deriving (Eq, Show)

defaultShardLimits :: ShardLimits
defaultShardLimits =
  ShardLimits
    { maxItemsPerShard = 20_000
    , maxTokensPerShard = 3_500_000
    , maxBytesPerShard = 80_000_000
    , maxItemsPerTenantPerShard = 10_000
    , maxTokensPerTenantPerShard = 1_500_000
    , hotCutoffSeconds = 15 * 60
    , warmCutoffSeconds = 2 * 60 * 60
    , normalCutoffSeconds = 24 * 60 * 60
    }

planBatchShardsAt :: UTCTime -> ShardLimits -> [BatchItem] -> Either [PlanningError] [BatchShard]
planBatchShardsAt now limits rawItems = do
  normalizedItems <- validateInputs limits rawItems
  let grouped =
        Map.fromListWith (++)
          [ (partitionKeyAt now limits item, [item])
          | item <- normalizedItems
          ]
  fmap concat $
    traverse
      (\(partitionKey, items) -> packPartition partitionKey limits items)
      (Map.toAscList grouped)

summarizePlan :: [BatchShard] -> PlanSummary
summarizePlan shards =
  let urgencyCounts =
        foldl'
          (\acc shard -> Map.insertWith (+) (shardUrgency shard) (1 :: Int) acc)
          Map.empty
          shards
      tenantsSeen =
        foldl'
          (\acc shard ->
             foldl'
               (\inner item -> Map.insert (tenantId item) () inner)
               acc
               (shardItems shard)
          )
          Map.empty
          shards
  in PlanSummary
       { totalShards = length shards
       , totalItems = sum (map shardItemTotal shards)
       , totalTokens = sum (map shardTokenTotal shards)
       , totalBytes = sum (map shardByteTotal shards)
       , hotShardCount = Map.findWithDefault 0 Hot urgencyCounts
       , warmShardCount = Map.findWithDefault 0 Warm urgencyCounts
       , normalShardCount = Map.findWithDefault 0 Normal urgencyCounts
       , backlogShardCount = Map.findWithDefault 0 Backlog urgencyCounts
       , maxObservedItems = safeMaximum (map shardItemTotal shards)
       , maxObservedTokens = safeMaximum (map shardTokenTotal shards)
       , maxObservedBytes = safeMaximum (map shardByteTotal shards)
       , tenantCount = Map.size tenantsSeen
       }

renderPlanSummary :: PlanSummary -> Text
renderPlanSummary PlanSummary{..} =
  T.unlines
    [ "Batch Request Sharder Summary"
    , "  total shards: " <> showT totalShards
    , "  total items: " <> showT totalItems
    , "  total tokens: " <> showT totalTokens
    , "  total bytes: " <> showT totalBytes
    , "  urgency split: hot=" <> showT hotShardCount
        <> ", warm=" <> showT warmShardCount
        <> ", normal=" <> showT normalShardCount
        <> ", backlog=" <> showT backlogShardCount
    , "  max shard items: " <> showT maxObservedItems
    , "  max shard tokens: " <> showT maxObservedTokens
    , "  max shard bytes: " <> showT maxObservedBytes
    , "  tenants seen: " <> showT tenantCount
    ]

renderShardReport :: ShardLimits -> [BatchShard] -> Text
renderShardReport limits shards =
  T.unlines $
    renderPlanSummary (summarizePlan shards) : map renderOne shards
  where
    renderOne shard =
      let tenantSummary =
            T.intercalate
              ", "
              [ tenant <> ":" <> showT (tenantItemCount usage) <> " items/" <> showT (tenantTokenTotal usage) <> " tok"
              | (tenant, usage) <- Map.toAscList (shardTenantUsage shard)
              ]
          earliest =
            maybe "none" showUtc (minimumDeadline (shardItems shard))
      in T.intercalate
           "\n"
           [ ""
           , "Shard " <> showT (shardOrdinal shard) <> " [" <> shardId shard <> "]"
           , "  lane: " <> renderUrgency (shardUrgency shard)
           , "  provider/model/endpoint: "
               <> keyProvider (shardKey shard) <> "/"
               <> keyModel (shardKey shard) <> "/"
               <> keyEndpoint (shardKey shard)
           , "  items/tokens/bytes: "
               <> showT (shardItemTotal shard) <> "/"
               <> showT (shardTokenTotal shard) <> "/"
               <> showT (shardByteTotal shard)
           , "  utilization ppm: " <> showT (shardUtilizationPpm limits shard)
           , "  earliest deadline: " <> earliest
           , "  tenants: " <> tenantSummary
           ]

shardUtilizationPpm :: ShardLimits -> BatchShard -> Int
shardUtilizationPpm ShardLimits{..} BatchShard{..} =
  maximum
    [ ratioPpm shardItemTotal maxItemsPerShard
    , ratioPpm shardTokenTotal maxTokensPerShard
    , ratioPpm shardByteTotal maxBytesPerShard
    ]

packPartition :: PartitionKey -> ShardLimits -> [BatchItem] -> Either [PlanningError] [BatchShard]
packPartition partitionKey limits items = do
  working <- foldl' step (Right []) (sortItems items)
  pure (finalizePartition partitionKey working)
  where
    step acc item = acc >>= placeItem partitionKey limits item

placeItem :: PartitionKey -> ShardLimits -> BatchItem -> [WorkingShard] -> Either [PlanningError] [WorkingShard]
placeItem partitionKey limits item shards
  | not (fitsFresh limits item) =
      Left
        [ OversizedItem
            (itemId item)
            "Item cannot fit into an empty shard under the supplied limits."
        ]
  | otherwise =
      case choosePlacement limits item shards of
        Nothing ->
          let nextOrdinal = length shards + 1
          in Right (shards ++ [singletonWorkingShard partitionKey nextOrdinal item])
        Just shardIndex ->
          let updated = addItemToWorkingShard item (shards !! shardIndex)
          in Right (replaceAt shardIndex updated shards)

choosePlacement :: ShardLimits -> BatchItem -> [WorkingShard] -> Maybe Int
choosePlacement limits item shards =
  case candidates of
    [] -> Nothing
    _ -> Just (snd (head candidates))
  where
    candidates =
      sortBy (comparing fst)
        [ (placementScore limits item shard, index)
        | (index, shard) <- zip [0 ..] shards
        , fitsExisting limits item shard
        ]

placementScore :: ShardLimits -> BatchItem -> WorkingShard -> (Int, Int, Int, Int)
placementScore ShardLimits{..} item shard =
  let projectedItems = wsItemTotal shard + 1
      projectedTokens = wsTokenTotal shard + estimatedTokens item
      projectedBytes = wsByteTotal shard + estimatedBytes item
      usage = Map.findWithDefault zeroTenantUsage (tenantId item) (wsTenantUsage shard)
      projectedTenantItems = tenantItemCount usage + 1
      projectedTenantTokens = tenantTokenTotal usage + estimatedTokens item
      tenantPenalty =
        max
          (ratioPpm projectedTenantItems projectedItems)
          (ratioPpm projectedTenantTokens projectedTokens)
      wasteScore =
           (maxItemsPerShard - projectedItems) * 1_000_000
        +  (maxTokensPerShard - projectedTokens) * 10
        +  ((maxBytesPerShard - projectedBytes) `div` 512)
      densityPreference = negate projectedItems
  in (tenantPenalty, wasteScore, densityPreference, wsOrdinal shard)

fitsFresh :: ShardLimits -> BatchItem -> Bool
fitsFresh ShardLimits{..} BatchItem{..} =
     estimatedTokens <= maxTokensPerShard
  && estimatedBytes <= maxBytesPerShard
  && 1 <= maxItemsPerShard
  && 1 <= maxItemsPerTenantPerShard
  && estimatedTokens <= maxTokensPerTenantPerShard

fitsExisting :: ShardLimits -> BatchItem -> WorkingShard -> Bool
fitsExisting ShardLimits{..} BatchItem{..} WorkingShard{..} =
     wsItemTotal + 1 <= maxItemsPerShard
  && wsTokenTotal + estimatedTokens <= maxTokensPerShard
  && wsByteTotal + estimatedBytes <= maxBytesPerShard
  && let usage = Map.findWithDefault zeroTenantUsage tenantId wsTenantUsage
     in tenantItemCount usage + 1 <= maxItemsPerTenantPerShard
        && tenantTokenTotal usage + estimatedTokens <= maxTokensPerTenantPerShard

singletonWorkingShard :: PartitionKey -> Int -> BatchItem -> WorkingShard
singletonWorkingShard (PartitionKey key urgency) ordinal item =
  WorkingShard
    { wsOrdinal = ordinal
    , wsKey = key
    , wsUrgency = urgency
    , wsItems = Seq.singleton item
    , wsItemTotal = 1
    , wsTokenTotal = estimatedTokens item
    , wsByteTotal = estimatedBytes item
    , wsTenantUsage =
        Map.singleton
          (tenantId item)
          (TenantUsage 1 (estimatedTokens item))
    }

addItemToWorkingShard :: BatchItem -> WorkingShard -> WorkingShard
addItemToWorkingShard item shard@WorkingShard{..} =
  let updatedUsage =
        Map.alter
          updateTenant
          (tenantId item)
          wsTenantUsage
  in shard
       { wsItems = wsItems |> item
       , wsItemTotal = wsItemTotal + 1
       , wsTokenTotal = wsTokenTotal + estimatedTokens item
       , wsByteTotal = wsByteTotal + estimatedBytes item
       , wsTenantUsage = updatedUsage
       }
  where
    updateTenant Nothing = Just (TenantUsage 1 (estimatedTokens item))
    updateTenant (Just usage) =
      Just
        usage
          { tenantItemCount = tenantItemCount usage + 1
          , tenantTokenTotal = tenantTokenTotal usage + estimatedTokens item
          }

finalizePartition :: PartitionKey -> [WorkingShard] -> [BatchShard]
finalizePartition partitionKey =
  zipWith finalizeOne [1 ..]
  where
    finalizeOne ordinal shard =
      let finalizedItems = toList (wsItems shard)
      in BatchShard
           { shardId = buildShardId partitionKey ordinal finalizedItems
           , shardOrdinal = ordinal
           , shardKey = wsKey shard
           , shardUrgency = wsUrgency shard
           , shardItems = finalizedItems
           , shardItemTotal = wsItemTotal shard
           , shardTokenTotal = wsTokenTotal shard
           , shardByteTotal = wsByteTotal shard
           , shardTenantUsage = wsTenantUsage shard
           }

buildShardId :: PartitionKey -> Int -> [BatchItem] -> Text
buildShardId (PartitionKey key urgency) ordinal items =
  let parts =
        [ keyProvider key
        , keyModel key
        , keyEndpoint key
        , fromMaybe "" (keyRegion key)
        , renderUrgency urgency
        , showT ordinal
        ] ++ map itemId items
      digest = fnv1a64Hex (TE.encodeUtf8 (T.intercalate "\n" parts))
  in T.intercalate "-" ["batch", renderUrgency urgency, digest]

validateInputs :: ShardLimits -> [BatchItem] -> Either [PlanningError] [BatchItem]
validateInputs limits items =
  case errors of
    [] -> Right normalized
    _ -> Left errors
  where
    normalized = map normalizeItem items
    errors = validateLimits limits ++ validateItems normalized ++ duplicateErrors normalized

validateLimits :: ShardLimits -> [PlanningError]
validateLimits ShardLimits{..} =
  catMaybes
    [ positiveLimit "maxItemsPerShard" maxItemsPerShard
    , positiveLimit "maxTokensPerShard" maxTokensPerShard
    , positiveLimit "maxBytesPerShard" maxBytesPerShard
    , positiveLimit "maxItemsPerTenantPerShard" maxItemsPerTenantPerShard
    , positiveLimit "maxTokensPerTenantPerShard" maxTokensPerTenantPerShard
    , nonNegativeLimit "hotCutoffSeconds" hotCutoffSeconds
    , nonNegativeLimit "warmCutoffSeconds" warmCutoffSeconds
    , nonNegativeLimit "normalCutoffSeconds" normalCutoffSeconds
    , orderedCutoffs hotCutoffSeconds warmCutoffSeconds normalCutoffSeconds
    ]

validateItems :: [BatchItem] -> [PlanningError]
validateItems = concatMap validateOne
  where
    validateOne BatchItem{..} =
      catMaybes
        [ requireNonEmpty "itemId" itemId
        , requireNonEmpty "tenantId" tenantId
        , requireNonEmpty "provider" (keyProvider batchKey)
        , requireNonEmpty "model" (keyModel batchKey)
        , requireNonEmpty "endpoint" (keyEndpoint batchKey)
        , if estimatedTokens <= 0
            then Just (NonPositiveEstimate itemId "estimatedTokens must be greater than zero." estimatedTokens)
            else Nothing
        , if estimatedBytes <= 0
            then Just (NonPositiveEstimate itemId "estimatedBytes must be greater than zero." estimatedBytes)
            else Nothing
        ]

duplicateErrors :: [BatchItem] -> [PlanningError]
duplicateErrors items =
  [ DuplicateItemId duplicateId
  | (duplicateId, count) <- Map.toList counts
  , count > (1 :: Int)
  ]
  where
    counts =
      foldl'
        (\acc item -> Map.insertWith (+) (itemId item) (1 :: Int) acc)
        Map.empty
        items

normalizeItem :: BatchItem -> BatchItem
normalizeItem item@BatchItem{..} =
  item
    { itemId = clean itemId
    , tenantId = normalizeTenant tenantId
    , batchKey = normalizeKey batchKey
    , payloadRef = normalizeOptionalText payloadRef
    }
  where
    clean = T.strip
    normalizeTenant tenant =
      let trimmed = clean tenant
      in if T.null trimmed then "default" else trimmed

normalizeOptionalText :: Maybe Text -> Maybe Text
normalizeOptionalText Nothing = Nothing
normalizeOptionalText (Just value) =
  let trimmed = T.strip value
  in if T.null trimmed then Nothing else Just trimmed

normalizeKey :: BatchKey -> BatchKey
normalizeKey BatchKey{..} =
  BatchKey
    { keyProvider = foldKey keyProvider
    , keyModel = foldKey keyModel
    , keyEndpoint = foldKey keyEndpoint
    , keyRegion = normalizeOptionalText (fmap foldKey keyRegion)
    }
  where
    foldKey = T.toCaseFold . T.strip

partitionKeyAt :: UTCTime -> ShardLimits -> BatchItem -> PartitionKey
partitionKeyAt now limits item =
  PartitionKey (batchKey item) (urgencyBandAt now limits item)

urgencyBandAt :: UTCTime -> ShardLimits -> BatchItem -> UrgencyBand
urgencyBandAt now ShardLimits{..} BatchItem{..} =
  case deadlineAt of
    Nothing -> Backlog
    Just deadline ->
      let seconds = floor (diffUTCTime deadline now) :: Int
      in if seconds <= hotCutoffSeconds
           then Hot
         else if seconds <= warmCutoffSeconds
           then Warm
         else if seconds <= normalCutoffSeconds
           then Normal
         else Backlog

sortItems :: [BatchItem] -> [BatchItem]
sortItems =
  sortBy $
    comparing (Down . priority)
      <> comparing deadlineRank
      <> comparing (Down . estimatedTokens)
      <> comparing (Down . estimatedBytes)
      <> comparing itemId

deadlineRank :: BatchItem -> (Int, Maybe UTCTime, Text)
deadlineRank BatchItem{..} =
  case deadlineAt of
    Nothing -> (1, Nothing, itemId)
    Just deadline -> (0, Just deadline, itemId)

minimumDeadline :: [BatchItem] -> Maybe UTCTime
minimumDeadline =
  foldl' pickEarlier Nothing . map deadlineAt
  where
    pickEarlier Nothing candidate = candidate
    pickEarlier current Nothing = current
    pickEarlier (Just left) (Just right)
      | right < left = Just right
      | otherwise = Just left

renderUrgency :: UrgencyBand -> Text
renderUrgency Hot = "hot"
renderUrgency Warm = "warm"
renderUrgency Normal = "normal"
renderUrgency Backlog = "backlog"

showUtc :: UTCTime -> Text
showUtc = T.pack . show

ratioPpm :: Int -> Int -> Int
ratioPpm _ denominator
  | denominator <= 0 = 0
ratioPpm numerator denominator =
  (numerator * 1_000_000) `div` denominator

safeMaximum :: [Int] -> Int
safeMaximum [] = 0
safeMaximum values = maximum values

zeroTenantUsage :: TenantUsage
zeroTenantUsage = TenantUsage 0 0

replaceAt :: Int -> a -> [a] -> [a]
replaceAt index newValue values =
  let (prefix, suffix) = splitAt index values
  in case suffix of
       [] -> values
       (_ : rest) -> prefix ++ (newValue : rest)

positiveLimit :: Text -> Int -> Maybe PlanningError
positiveLimit name value
  | value <= 0 = Just (InvalidLimit (name <> " must be greater than zero."))
  | otherwise = Nothing

nonNegativeLimit :: Text -> Int -> Maybe PlanningError
nonNegativeLimit name value
  | value < 0 = Just (InvalidLimit (name <> " must not be negative."))
  | otherwise = Nothing

orderedCutoffs :: Int -> Int -> Int -> Maybe PlanningError
orderedCutoffs hotCutoff warmCutoff normalCutoff
  | hotCutoff > warmCutoff =
      Just (InvalidLimit "hotCutoffSeconds must be less than or equal to warmCutoffSeconds.")
  | warmCutoff > normalCutoff =
      Just (InvalidLimit "warmCutoffSeconds must be less than or equal to normalCutoffSeconds.")
  | otherwise = Nothing

requireNonEmpty :: Text -> Text -> Maybe PlanningError
requireNonEmpty fieldName value
  | T.null (T.strip value) = Just (EmptyField fieldName "Field must not be empty.")
  | otherwise = Nothing

showT :: Show a => a -> Text
showT = T.pack . show

padLeft :: Int -> Char -> Text -> Text
padLeft width fillChar value =
  T.replicate (max 0 (width - T.length value)) (T.singleton fillChar) <> value

fnv1a64Hex :: BS.ByteString -> Text
fnv1a64Hex bytes =
  padLeft 16 '0' (T.pack (showHex (fnv1a64 bytes) ""))

fnv1a64 :: BS.ByteString -> Word64
fnv1a64 =
  BS.foldl'
    step
    14695981039346656037
  where
    step hashValue byteValue =
      (hashValue `xor` fromIntegral byteValue) * 1099511628211

{- 
This solves OpenAI Batch API sharding, Anthropic message batch partitioning, and the general problem of building safe multi-tenant LLM batch files when token limits, byte limits, deadlines, and fairness rules all matter at the same time. Built because the quick version of this problem is always “split the list every N items,” and that version breaks the moment one tenant sends a flood of work, one urgent eval job needs to land now, or a retry has to be explained two weeks later during an incident review. Use it when you need a Haskell batch request sharder for AI infrastructure, model gateways, offline inference pipelines, dataset enrichment jobs, or NDJSON batch preprocessing where stable grouping and predictable shard IDs matter.

The trick: I do not treat the queue as a flat list. I first group by provider, model, endpoint, and urgency lane so different execution backends do not get mixed together by accident. Inside each partition I sort by operational value: higher priority first, earlier deadlines first, then larger requests so capacity gets packed on purpose instead of by luck. Placement is deterministic and biased toward tenant fairness plus dense shards, which makes the output more useful for real OpenAI batch jobs, Anthropic bulk requests, internal inference workers, and retry queues that need stable replay behavior.

Drop this into a Haskell service, job preprocessor, AI batch scheduler, data pipeline worker, research platform, or LLM operations codebase that needs production-ready request sharding with token budgeting, byte budgeting, urgency lanes, tenant guardrails, and deterministic fingerprints. If someone finds this by searching for Haskell OpenAI Batch API sharding, Haskell Anthropic batch scheduler, deterministic LLM batch partitioner, multi-tenant token limit planner, or NDJSON batch request balancer, this is exactly the kind of code I wanted them to land on: practical, readable, and ready to fork into a real system.
-}
