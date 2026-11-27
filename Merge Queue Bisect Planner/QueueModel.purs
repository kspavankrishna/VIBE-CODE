module QueueModel
  ( Hyp(..)
  , Model
  , Weighted
  , Posterior
  , Test
  , Outcome(..)
  , PairMass
  , maxBatch
  , upTo
  , bit
  , hasBit
  , maskOf
  , popCount
  , hits
  , failProbability
  , priorPosterior
  , observe
  , normalize
  , totalMass
  , marginalBad
  , conflictInvolvement
  , pairMasses
  , envMass
  , cleanProbability
  , entropyBits
  , binaryEntropy
  , plogp
  , log2
  ) where

import Prelude

import Data.Array as Array
import Data.Foldable (foldl, sum)
import Data.Int (pow, toNumber)
import Data.Int.Bits (shl, (.&.), (.|.))
import Data.Maybe (Maybe(..), fromMaybe)
import Data.Number (ln2, log)

-- | One explanation for the failures seen so far.
-- | Bad mask: exactly the PRs in mask are broken on their own (mask 0 means every PR is clean).
-- | Conflict i j: every PR is fine alone, but i and j together break the build.
-- | Env: the base branch or the CI environment is broken, so every run fails.
data Hyp
  = Bad Int
  | Conflict Int Int
  | Env

derive instance eqHyp :: Eq Hyp

type Model =
  { size :: Int
  , priors :: Array Number
  , flake :: Number
  , miss :: Number
  , envPrior :: Number
  , conflictPrior :: Number
  }

type Weighted = { hyp :: Hyp, w :: Number }

type Posterior = Array Weighted

-- | A CI run is a bitmask over batch positions. Bit i set means PR i is applied
-- | on top of the base branch. Mask 0 is the base branch alone.
type Test = Int

data Outcome
  = Pass
  | Fail

derive instance eqOutcome :: Eq Outcome

type PairMass = { i :: Int, j :: Int, w :: Number }

-- | 2^16 subset hypotheses is the most we enumerate exactly. Bigger batches
-- | should be split before they reach the queue.
maxBatch :: Int
maxBatch = 16

-- | Inclusive range that is empty when lo > hi. Data.Array.range counts down
-- | in that case, which is never what a loop over PR positions wants.
upTo :: Int -> Int -> Array Int
upTo lo hi = if lo > hi then [] else Array.range lo hi

bit :: Int -> Int
bit i = shl 1 i

hasBit :: Int -> Int -> Boolean
hasBit mask i = (mask .&. bit i) /= 0

maskOf :: Array Int -> Int
maskOf = foldl (\m i -> m .|. bit i) 0

popCount :: Int -> Int
popCount = go 0
  where
  go acc m = if m == 0 then acc else go (acc + 1) (m .&. (m - 1))

-- | Does this hypothesis make the test fail for a real reason?
-- | The same predicate answers "does merging mask expose this problem".
hits :: Test -> Hyp -> Boolean
hits test = case _ of
  Bad b -> (b .&. test) /= 0
  Conflict i j -> hasBit test i && hasBit test j
  Env -> true

failProbability :: Model -> Hyp -> Test -> Number
failProbability model hyp test =
  if hits test hyp then 1.0 - model.miss else model.flake

priorPosterior :: Model -> Posterior
priorPosterior model =
  Array.filter (\s -> s.w > 0.0) (subsets <> pairs <> env)
  where
  n = model.size
  positions = upTo 0 (n - 1)
  pairCount = n * (n - 1) / 2
  conflictShare = if pairCount == 0 then 0.0 else model.conflictPrior
  badShare = 1.0 - model.envPrior - conflictShare
  q i = fromMaybe 0.0 (Array.index model.priors i)
  subsetWeight mask =
    badShare * foldl (\acc i -> acc * (if hasBit mask i then q i else 1.0 - q i)) 1.0 positions
  subsets = map (\mask -> { hyp: Bad mask, w: subsetWeight mask }) (upTo 0 (pow 2 n - 1))
  pairs = do
    i <- positions
    j <- upTo (i + 1) (n - 1)
    pure { hyp: Conflict i j, w: conflictShare / toNumber pairCount }
  env = if model.envPrior > 0.0 then [ { hyp: Env, w: model.envPrior } ] else []

-- | Bayes update for one CI result. Flaky failures are treated as independent
-- | between runs, which is the standard assumption and the one that breaks
-- | first when a flake is really an infrastructure outage (that is what Env is for).
observe :: Model -> Test -> Outcome -> Posterior -> Posterior
observe model test outcome = map update
  where
  update s =
    let
      p = failProbability model s.hyp test
      like = case outcome of
        Fail -> p
        Pass -> 1.0 - p
    in
      s { w = s.w * like }

totalMass :: Posterior -> Number
totalMass post = sum (map _.w post)

-- | Renormalise and drop hypotheses that the evidence has ruled out.
-- | Nothing means every hypothesis was ruled out: the observations contradict the model.
normalize :: Posterior -> Maybe Posterior
normalize post =
  let
    total = totalMass post
  in
    if total <= 0.0 || total /= total then Nothing
    else Just (Array.filter (\s -> s.w > 0.0) (map (\s -> s { w = s.w / total }) post))

massWhere :: (Hyp -> Boolean) -> Posterior -> Number
massWhere f post = foldl (\acc s -> if f s.hyp then acc + s.w else acc) 0.0 post

marginalBad :: Posterior -> Int -> Number
marginalBad post i = massWhere isBad post
  where
  isBad = case _ of
    Bad b -> hasBit b i
    _ -> false

conflictInvolvement :: Posterior -> Int -> Number
conflictInvolvement post i = massWhere involved post
  where
  involved = case _ of
    Conflict a b -> a == i || b == i
    _ -> false

pairMasses :: Posterior -> Array PairMass
pairMasses = Array.mapMaybe toPair
  where
  toPair s = case s.hyp of
    Conflict i j -> Just { i, j, w: s.w }
    _ -> Nothing

envMass :: Posterior -> Number
envMass = massWhere (_ == Env)

-- | Probability that applying mask on top of the base branch is genuinely green.
cleanProbability :: Posterior -> Int -> Number
cleanProbability post mask = 1.0 - massWhere (hits mask) post

log2 :: Number -> Number
log2 x = log x / ln2

plogp :: Number -> Number
plogp p = if p <= 0.0 then 0.0 else p * log2 p

binaryEntropy :: Number -> Number
binaryEntropy p = if p <= 0.0 || p >= 1.0 then 0.0 else negate (plogp p + plogp (1.0 - p))

entropyBits :: Posterior -> Number
entropyBits post = negate (sum (map (\s -> plogp s.w) post))
