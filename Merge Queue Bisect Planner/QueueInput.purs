module QueueInput
  ( QueueState
  , Observation
  , parseState
  , parseProbability
  ) where

import Prelude

import Data.Array as Array
import Data.Either (Either(..))
import Data.Foldable (foldM)
import Data.Int as Int
import Data.Maybe (Maybe(..), fromMaybe)
import Data.Number as Number
import Data.String (Pattern(..), Replacement(..))
import Data.String as String
import Data.String.CodeUnits (toCharArray)
import Data.Traversable (traverse)
import QueueModel (Model, Outcome(..), Test, maskOf, maxBatch, upTo)
import QueuePlanner (Thresholds, maxRunners)

type Observation =
  { test :: Test
  , outcome :: Outcome
  , line :: Int
  , text :: String
  }

type QueueState =
  { ids :: Array String
  , model :: Model
  , thresholds :: Thresholds
  , runners :: Int
  , budget :: Int
  , observations :: Array Observation
  }

type RawPr = { id :: String, prior :: Maybe Number, line :: Int }

type RawObservation = { targets :: String, outcome :: Outcome, line :: Int, text :: String }

type Draft =
  { prs :: Array RawPr
  , observations :: Array RawObservation
  , flake :: Number
  , miss :: Number
  , env :: Number
  , conflict :: Number
  , risk :: Number
  , eject :: Number
  , prior :: Number
  , runners :: Int
  , budget :: Int
  }

-- | Defaults are deliberately conservative. A 2 percent flake rate and a 2 percent
-- | merge risk match what a healthy monorepo CI tends to see; tune both from your own history.
defaults :: Draft
defaults =
  { prs: []
  , observations: []
  , flake: 0.02
  , miss: 0.0
  , env: 0.01
  , conflict: 0.02
  , risk: 0.02
  , eject: 0.95
  , prior: 0.1
  , runners: 2
  , budget: 12
  }

lineError :: forall a. Int -> String -> Either String a
lineError n msg = Left ("line " <> show n <> ": " <> msg)

numericChars :: Array Char
numericChars = toCharArray "0123456789.eE+-"

-- | Strict decimal parse. parseFloat alone would accept "0.1abc" as 0.1.
parseProbability :: String -> Maybe Number
parseProbability raw =
  if raw == "" || not (Array.all (\c -> Array.elem c numericChars) (toCharArray raw)) then Nothing
  else Number.fromString raw

type Range = { lo :: Number, hi :: Number, hiOpen :: Boolean }

ranged :: Int -> String -> Range -> String -> Either String Number
ranged n name r raw = case parseProbability raw of
  Nothing -> lineError n (name <> " must be a number, got '" <> raw <> "'")
  Just v
    | v < r.lo || v > r.hi || (r.hiOpen && v == r.hi) ->
        lineError n (name <> " must be in [" <> show r.lo <> ", " <> show r.hi <> (if r.hiOpen then ")" else "]") <> ", got " <> raw)
    | otherwise -> Right v

positive :: Int -> String -> Int -> String -> Either String Int
positive n name hi raw = case Int.fromString raw of
  Nothing -> lineError n (name <> " must be a whole number, got '" <> raw <> "'")
  Just v
    | v < 1 || v > hi -> lineError n (name <> " must be between 1 and " <> show hi <> ", got " <> raw)
    | otherwise -> Right v

parseOutcome :: Int -> String -> Either String Outcome
parseOutcome n = case _ of
  "pass" -> Right Pass
  "fail" -> Right Fail
  other -> lineError n ("result must be pass or fail, got '" <> other <> "'")

addPr :: Draft -> Int -> String -> Maybe Number -> Either String Draft
addPr d n ident prior
  | ident == "base" || ident == "all" = lineError n ("'" <> ident <> "' is reserved for observe lines and cannot be a PR id")
  | String.contains (Pattern ",") ident = lineError n ("PR id '" <> ident <> "' cannot contain a comma")
  | otherwise = case Array.find (\p -> p.id == ident) d.prs of
      Just first -> lineError n ("duplicate pr '" <> ident <> "', first declared on line " <> show first.line)
      Nothing -> Right (d { prs = Array.snoc d.prs { id: ident, prior, line: n } })

cleanLine :: String -> String
cleanLine =
  String.trim
    <<< String.replaceAll (Pattern "\t") (Replacement " ")
    <<< String.replaceAll (Pattern "\r") (Replacement "")

probabilityRange :: Range
probabilityRange = { lo: 0.0, hi: 1.0, hiOpen: false }

parseLine :: Draft -> { n :: Int, text :: String } -> Either String Draft
parseLine d ln =
  let
    clean = cleanLine ln.text
    n = ln.n
    ws = Array.filter (_ /= "") (String.split (Pattern " ") clean)
  in
    if clean == "" || String.take 1 clean == "#" then Right d
    else case ws of
      [ "pr", ident ] -> addPr d n ident Nothing
      [ "pr", ident, opt ] -> case String.stripPrefix (Pattern "prior=") opt of
        Nothing -> lineError n ("expected prior=<probability> after the PR id, got '" <> opt <> "'")
        Just v -> ranged n "prior" probabilityRange v >>= \p -> addPr d n ident (Just p)
      [ "prior", v ] -> ranged n "prior" probabilityRange v <#> \p -> d { prior = p }
      [ "flake", v ] -> ranged n "flake" { lo: 0.0, hi: 0.5, hiOpen: true } v <#> \p -> d { flake = p }
      [ "miss", v ] -> ranged n "miss" { lo: 0.0, hi: 0.5, hiOpen: true } v <#> \p -> d { miss = p }
      [ "env", v ] -> ranged n "env" { lo: 0.0, hi: 1.0, hiOpen: true } v <#> \p -> d { env = p }
      [ "conflict", v ] -> ranged n "conflict" { lo: 0.0, hi: 1.0, hiOpen: true } v <#> \p -> d { conflict = p }
      [ "risk", v ] -> ranged n "risk" { lo: 0.0, hi: 0.5, hiOpen: true } v >>= \p ->
        if p <= 0.0 then lineError n "risk must be above 0: no finite amount of testing proves a batch is green" else Right (d { risk = p })
      [ "eject", v ] -> ranged n "eject" { lo: 0.5, hi: 1.0, hiOpen: false } v >>= \p ->
        if p <= 0.5 then lineError n "eject must be above 0.5 or a coin flip would eject PRs" else Right (d { eject = p })
      [ "runners", v ] -> positive n "runners" maxRunners v <#> \r -> d { runners = r }
      [ "budget", v ] -> positive n "budget" 1000 v <#> \b -> d { budget = b }
      [ "observe", targets, result ] -> parseOutcome n result <#> \o ->
        d { observations = Array.snoc d.observations { targets, outcome: o, line: n, text: clean } }
      _ -> case Array.head ws of
        Just w | Array.elem w [ "pr", "prior", "flake", "miss", "env", "conflict", "risk", "eject", "runners", "budget", "observe" ] ->
          lineError n ("wrong number of arguments for '" <> w <> "' in: " <> clean)
        _ -> lineError n ("unknown directive: " <> clean)

findTarget :: Array String -> Int -> String -> String -> Either String Int
findTarget ids n targets ident
  | ident == "" = lineError n ("empty PR id in '" <> targets <> "'")
  | otherwise = case Array.elemIndex ident ids of
      Nothing -> lineError n ("unknown PR id '" <> ident <> "'. Declare it with a pr line first")
      Just i -> Right i

resolveTargets :: Array String -> Int -> String -> Either String Test
resolveTargets ids n targets
  | targets == "base" = Right 0
  | targets == "all" = Right (maskOf (upTo 0 (Array.length ids - 1)))
  | otherwise = maskOf <$> traverse (findTarget ids n targets) (String.split (Pattern ",") targets)

resolveObservation :: Array String -> RawObservation -> Either String Observation
resolveObservation ids o = resolveTargets ids o.line o.targets <#> \test ->
  { test, outcome: o.outcome, line: o.line, text: o.text }

finish :: Draft -> Either String QueueState
finish d = do
  let
    count = Array.length d.prs
    ids = map _.id d.prs
  when (count == 0) (Left "no pr lines: describe the batch with one 'pr <id>' line per pull request, in queue order")
  when (count > maxBatch)
    ( Left
        ( "batch has " <> show count <> " PRs but exact inference stops at " <> show maxBatch
            <> ". Split the batch: queues that admit more than that per group lose more to bisection than they gain from batching"
        )
    )
  when (d.env + d.conflict >= 1.0) (Left "env plus conflict must leave room for the per PR hypotheses (their sum must be below 1)")
  when (d.flake + d.miss >= 1.0) (Left "flake plus miss must be below 1 or a test result carries no information")
  observations <- traverse (resolveObservation ids) d.observations
  pure
    { ids
    , model:
        { size: count
        , priors: map (\p -> fromMaybe d.prior p.prior) d.prs
        , flake: d.flake
        , miss: d.miss
        , envPrior: d.env
        , conflictPrior: if count < 2 then 0.0 else d.conflict
        }
    , thresholds: { risk: d.risk, eject: d.eject }
    , runners: d.runners
    , budget: d.budget
    , observations
    }

parseState :: String -> Either String QueueState
parseState text =
  foldM parseLine defaults (Array.mapWithIndex (\i t -> { n: i + 1, text: t }) (String.split (Pattern "\n") text))
    >>= finish
