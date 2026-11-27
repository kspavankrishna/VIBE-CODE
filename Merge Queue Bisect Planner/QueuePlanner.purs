module QueuePlanner
  ( Thresholds
  , Ejection
  , Decision
  , PlannedTest
  , Plan
  , maxRunners
  , minGainBits
  , decide
  , candidateTests
  , planRound
  , jointInformation
  ) where

import Prelude

import Data.Array as Array
import Data.Array.ST as STA
import Data.Foldable (foldl, for_, sum)
import Data.Int (pow)
import Data.Int.Bits ((.|.))
import Data.Maybe (Maybe(..))
import Data.Number (abs)
import QueueModel (Hyp, Model, Posterior, Test, binaryEntropy, bit, cleanProbability, conflictInvolvement, envMass, failProbability, hasBit, hits, marginalBad, maskOf, pairMasses, plogp, popCount, upTo)

type Thresholds =
  { risk :: Number
  , eject :: Number
  }

-- | partner is set when the PR is ejected because it breaks the build only
-- | together with an earlier PR in the batch.
type Ejection =
  { index :: Int
  , partner :: Maybe Int
  , probability :: Number
  }

type Decision =
  { merge :: Int
  , mergeClean :: Number
  , eject :: Array Ejection
  , pending :: Array Int
  , halt :: Boolean
  , resolved :: Boolean
  }

type PlannedTest =
  { test :: Test
  , failChance :: Number
  , gain :: Number
  }

type Plan =
  { tests :: Array PlannedTest
  , information :: Number
  }

-- | The joint outcome table grows as 4^k, so six parallel runs per round is the cap.
maxRunners :: Int
maxRunners = 6

-- | A run that is expected to teach less than this many bits is not worth a runner.
minGainBits :: Number
minGainBits = 0.01

-- | Turn the posterior into queue actions.
-- | Ejections are per PR. Merging follows queue order: the merge set grows one PR at a time
-- | while the chance that the whole set is green stays above 1 - risk, and it stops at the
-- | first PR that is neither ejected nor safe, because a merge queue cannot land PR 5 before PR 4.
decide :: Model -> Thresholds -> Posterior -> Decision
decide model th post =
  { merge: mergeMask
  , mergeClean: if mergeMask == 0 then 1.0 else cleanProbability post mergeMask
  , eject: ejected
  , pending
  , halt
  , resolved: not halt && Array.null pending
  }
  where
  positions = upTo 0 (model.size - 1)
  halt = envMass post >= th.eject

  direct = Array.mapMaybe directEjection positions
  directEjection i =
    let
      p = marginalBad post i
    in
      if p >= th.eject then Just { index: i, partner: Nothing, probability: p } else Nothing
  directMask = maskOf (map _.index direct)

  conflicted = Array.nubByEq (\a b -> a.index == b.index) (Array.mapMaybe conflictEjection (Array.sortBy (\a b -> compare b.w a.w) (pairMasses post)))
  conflictEjection pm =
    if pm.w >= th.eject && not (hasBit directMask pm.i) && not (hasBit directMask pm.j) then Just { index: pm.j, partner: Just pm.i, probability: pm.w }
    else Nothing

  ejected = direct <> conflicted
  ejectMask = maskOf (map _.index ejected)

  growMerge mask rest = case Array.uncons rest of
    Nothing -> mask
    Just { head: i, tail } ->
      if hasBit ejectMask i then growMerge mask tail
      else
        let
          next = mask .|. bit i
        in
          if cleanProbability post next >= 1.0 - th.risk then growMerge next tail else mask

  mergeMask = if halt then 0 else growMerge 0 positions
  pending = Array.filter (\i -> not (hasBit ejectMask i) && not (hasBit mergeMask i)) positions

-- | Candidate runs worth scoring. Exhaustive search over 2^n subsets per runner is too slow,
-- | so this proposes the shapes that real queues use plus the shapes that information
-- | theory favours: singletons, queue order prefixes, prefixes ranked by suspicion, the two
-- | halves Bors style bisection would try, the most likely conflict pairs and the base branch
-- | alone when an environment failure is still plausible. Each shape is also offered stacked
-- | on top of the PRs that are already safe to merge, because that is how the queue will
-- | eventually test the pending ones.
candidateTests :: Model -> Posterior -> Decision -> Array Test
candidateTests model post decision =
  Array.filter (\t -> t /= decision.merge || t == 0) (Array.nub (base <> raw <> stacked))
  where
  positions = upTo 0 (model.size - 1)
  ejectMask = maskOf (map _.index decision.eject)
  suspicion i = marginalBad post i + conflictInvolvement post i
  suspects = Array.filter (\i -> suspicion i > 1.0e-9 && not (hasBit decision.merge i) && not (hasBit ejectMask i)) positions
  bySuspicion = Array.sortBy (\a b -> compare (suspicion b) (suspicion a) <> compare a b) suspects
  prefixesOf xs = map (\r -> maskOf (Array.take r xs)) (upTo 1 (Array.length xs))
  half = Array.length suspects / 2
  halves = if half > 0 then [ maskOf (Array.take half suspects), maskOf (Array.drop half suspects) ] else []
  singles = map bit suspects
  topPairs = Array.take 3 (Array.sortBy (\a b -> compare b.w a.w) (Array.filter (\p -> p.w > 1.0e-9 && not (hasBit ejectMask p.i) && not (hasBit ejectMask p.j)) (pairMasses post)))
  pairs = map (\p -> bit p.i .|. bit p.j) topPairs
  base = if envMass post > 1.0e-9 then [ 0 ] else []
  raw = singles <> prefixesOf suspects <> prefixesOf bySuspicion <> halves <> pairs
  stacked = if decision.merge == 0 then [] else map (_ .|. decision.merge) raw

type Live = { hyp :: Hyp, w :: Number, pat :: Int }

-- | Sum posterior mass by hit pattern. Bit r of a pattern says whether planned run r
-- | would fail for a real reason under that hypothesis. Hypotheses that share a pattern
-- | are indistinguishable by this round, so the round only needs 2^k numbers.
bucket :: Int -> (Live -> Int) -> Array Live -> Array Number
bucket size patternOf states = STA.run do
  arr <- STA.unsafeThaw (Array.replicate size 0.0)
  for_ states \s -> STA.modify (patternOf s) (_ + s.w) arr
  pure arr

-- | Mutual information in bits between the outcomes of k parallel runs and the hypothesis.
-- | I = H(outcomes) - H(outcomes | hypothesis). Runs are independent given the hypothesis:
-- | a real break fails with probability 1 - miss, a clean run fails with probability flake.
jointInformation :: Model -> Int -> Array Number -> Number
jointInformation model k mass = max 0.0 (outcomeEntropy - conditionalEntropy)
  where
  runs = upTo 0 (k - 1)
  size = pow 2 k
  pFail pat r = if hasBit pat r then 1.0 - model.miss else model.flake
  likelihood o pat = foldl (\acc r -> let p = pFail pat r in acc * (if hasBit o r then p else 1.0 - p)) 1.0 runs
  outcomeChance o = sum (Array.mapWithIndex (\pat m -> if m > 0.0 then m * likelihood o pat else 0.0) mass)
  outcomeEntropy = negate (sum (map (\o -> plogp (outcomeChance o)) (upTo 0 (size - 1))))
  conditionalEntropy = sum (Array.mapWithIndex (\pat m -> m * sum (map (\r -> binaryEntropy (pFail pat r)) runs)) mass)

type Scored = { test :: Test, info :: Number }

better :: Maybe Scored -> Scored -> Maybe Scored
better Nothing x = Just x
better (Just a) x
  | x.info > a.info + 1.0e-12 = Just x
  | abs (x.info - a.info) <= 1.0e-12 && popCount x.test < popCount a.test = Just x
  | otherwise = Just a

type Search =
  { chosen :: Array PlannedTest
  , states :: Array Live
  , info :: Number
  }

-- | Greedy selection of up to runners parallel runs. Each step adds the candidate that
-- | raises the joint information of the round the most. Joint information is submodular
-- | here, so greedy is within (1 - 1/e) of the best possible round, and it never picks a
-- | run that only repeats what an earlier run in the same round already answers.
planRound :: Model -> Posterior -> Array Test -> Int -> Plan
planRound model post candidates runners =
  let
    total = sum (map _.w post)
    live = map (\s -> { hyp: s.hyp, w: s.w / total, pat: 0 }) (Array.filter (\s -> s.w / total > 1.0e-12) post)
    done = search { chosen: [], states: live, info: 0.0 }
  in
    { tests: done.chosen, information: done.info }
  where
  search :: Search -> Search
  search acc =
    if Array.length acc.chosen >= runners then acc else extend acc

  extend :: Search -> Search
  extend acc =
    let
      k = Array.length acc.chosen
      taken = map _.test acc.chosen
      open = Array.filter (\c -> not (Array.elem c taken)) candidates
      patternWith c s = if hits c s.hyp then s.pat + bit k else s.pat
      score c = { test: c, info: jointInformation model (k + 1) (bucket (pow 2 (k + 1)) (patternWith c) acc.states) }
    in
      case foldl better Nothing (map score open) of
        Nothing -> acc
        Just b ->
          if b.info - acc.info < minGainBits then acc
          else
            search
              { chosen: Array.snoc acc.chosen
                  { test: b.test
                  , failChance: sum (map (\s -> s.w * failProbability model s.hyp b.test) acc.states)
                  , gain: b.info - acc.info
                  }
              , states: map (\s -> s { pat = patternWith b.test s }) acc.states
              , info: b.info
              }
