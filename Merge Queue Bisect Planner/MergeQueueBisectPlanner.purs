module MergeQueueBisectPlanner
  ( main
  , Report
  , evaluate
  ) where

import Prelude

import Data.Array as Array
import Data.Either (Either(..))
import Data.Foldable (foldM, intercalate)
import Data.Int as Int
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.Monoid (power)
import Data.Number.Format (fixed, toStringWith)
import Data.String as String
import Effect (Effect)
import Effect.Exception (message, try)
import QueueInput (QueueState, parseState)
import QueueModel (Hyp(..), Posterior, Test, conflictInvolvement, entropyBits, envMass, hasBit, marginalBad, normalize, observe, priorPosterior, upTo)
import QueuePlanner (Decision, Plan, candidateTests, decide, maxRunners, planRound)

foreign import readTextFile :: String -> Effect String
foreign import argv :: Effect (Array String)
foreign import writeStdout :: String -> Effect Unit
foreign import writeStderr :: String -> Effect Unit
foreign import exitWith :: Int -> Effect Unit
foreign import quote :: String -> String

type Options =
  { format :: String
  , runners :: Maybe Int
  , path :: Maybe String
  , help :: Boolean
  }

type Report =
  { state :: QueueState
  , posterior :: Posterior
  , decision :: Decision
  , runsUsed :: Int
  , runsLeft :: Int
  , roundSize :: Int
  , plan :: Plan
  }

usage :: String
usage =
  intercalate "\n"
    [ "usage: node RunMergeQueueBisectPlanner.mjs [--format text|json] [--runners N] <state file | ->"
    , ""
    , "Reads a merge queue batch (pr lines, model settings, observe lines) and prints the"
    , "posterior blame per PR, which PRs can merge or must be ejected, and the CI runs to start next."
    , ""
    , "exit codes: 0 report printed, 2 usage or input error, 3 observations contradict the model"
    , ""
    ]

parseArgs :: Array String -> Either String Options
parseArgs = go { format: "text", runners: Nothing, path: Nothing, help: false }
  where
  go opts args = case Array.uncons args of
    Nothing -> Right opts
    Just { head, tail } -> case head of
      "--help" -> Right (opts { help = true })
      "-h" -> Right (opts { help = true })
      "--format" -> case Array.uncons tail of
        Just { head: f, tail: rest } | f == "text" || f == "json" -> go (opts { format = f }) rest
        _ -> Left "--format needs text or json"
      "--runners" -> case Array.uncons tail of
        Just { head: r, tail: rest } -> case Int.fromString r of
          Just v | v >= 1 && v <= maxRunners -> go (opts { runners = Just v }) rest
          _ -> Left ("--runners needs a whole number from 1 to " <> show maxRunners)
        Nothing -> Left ("--runners needs a whole number from 1 to " <> show maxRunners)
      other ->
        if other /= "-" && String.take 1 other == "-" then Left ("unknown option " <> other)
        else case opts.path of
          Nothing -> go (opts { path = Just other }) tail
          Just _ -> Left "only one state file can be given"

-- | Replay every observation in file order. The first one that leaves no hypothesis
-- | standing is reported with its line, because that is the run whose result the
-- | operator should double check (or the model setting, usually flake 0, to relax).
evaluate :: QueueState -> Maybe Int -> Either String Report
evaluate st runnerOverride = do
  prior <- case normalize (priorPosterior st.model) of
    Nothing -> Left "the prior has no mass: check that the priors and env and conflict settings are not all zero"
    Just p -> Right p
  posterior <- foldM step prior st.observations
  let
    decision = decide st.model st.thresholds posterior
    runsUsed = Array.length st.observations
    runsLeft = max 0 (st.budget - runsUsed)
    roundSize = min runsLeft (fromMaybe st.runners runnerOverride)
    plan =
      if decision.resolved || roundSize == 0 then { tests: [], information: 0.0 }
      else planRound st.model posterior (candidateTests st.model posterior decision) roundSize
  pure { state: st, posterior, decision, runsUsed, runsLeft, roundSize, plan }
  where
  step post o = case normalize (observe st.model o.test o.outcome post) of
    Nothing -> Left
      ( "line " <> show o.line <> ": '" <> o.text <> "' is impossible under every hypothesis."
          <> " With flake or miss at 0 the model treats CI as perfectly reliable, so one contradicting run breaks it."
          <> " Recheck that result or set flake and miss above 0."
      )
    Just p -> Right p

fmt :: Int -> Number -> String
fmt digits x = toStringWith (fixed digits) x

idOf :: QueueState -> Int -> String
idOf st i = fromMaybe ("#" <> show i) (Array.index st.ids i)

namesOf :: QueueState -> Int -> String
namesOf st mask = intercalate "," (map (idOf st) (Array.filter (hasBit mask) (upTo 0 (st.model.size - 1))))

testLabel :: QueueState -> Test -> String
testLabel st mask = if mask == 0 then "base" else namesOf st mask

hypLabel :: QueueState -> Hyp -> String
hypLabel st = case _ of
  Bad 0 -> "every PR clean (the failures were flakes)"
  Bad b -> "broken on its own: " <> namesOf st b
  Conflict i j -> "broken only together: " <> idOf st i <> " + " <> idOf st j
  Env -> "base branch or CI environment broken"

topExplanations :: Posterior -> Array { hyp :: Hyp, w :: Number }
topExplanations post = Array.take 6 (Array.filter (\s -> s.w >= 0.001) (Array.sortBy (\a b -> compare b.w a.w) post))

statusOf :: Report -> Int -> String
statusOf r i
  | hasBit r.decision.merge i = "merge"
  | otherwise = case Array.find (\e -> e.index == i) r.decision.eject of
      Just { partner: Just p } -> "eject (conflicts with " <> idOf r.state p <> ")"
      Just _ -> "eject"
      Nothing -> if r.decision.halt then "halted" else "pending"

padRight :: Int -> String -> String
padRight width s = s <> power " " (width - String.length s)

renderText :: Report -> String
renderText r =
  intercalate "\n" (header <> table <> envLine <> explanations <> decisionLines <> planLines) <> "\n"
  where
  st = r.state
  positions = upTo 0 (st.model.size - 1)
  width = max 6 (Array.foldl (\m i -> max m (String.length (idOf st i))) 0 positions + 2)
  header =
    [ "Merge queue bisect plan"
    , "batch " <> show st.model.size <> " PRs, " <> show r.runsUsed <> " results recorded, "
        <> show r.runsLeft
        <> " of "
        <> show st.budget
        <> " CI runs left, uncertainty "
        <> fmt 2 (entropyBits r.posterior)
        <> " bits"
    , ""
    , padRight width "PR" <> padRight 8 "prior" <> padRight 9 "P(bad)" <> padRight 12 "P(conflict)" <> "status"
    ]
  table = map row positions
  row i =
    padRight width (idOf st i)
      <> padRight 8 (fmt 3 (fromMaybe 0.0 (Array.index st.model.priors i)))
      <> padRight 9 (fmt 3 (marginalBad r.posterior i))
      <> padRight 12 (fmt 3 (conflictInvolvement r.posterior i))
      <> statusOf r i
  envLine = [ "", "P(base or CI broken) " <> fmt 3 (envMass r.posterior) ]
  explanations = [ "", "Most likely explanations" ] <> map (\s -> "  " <> fmt 3 s.w <> "  " <> hypLabel st s.hyp) (topExplanations r.posterior)
  d = r.decision
  ejectNames = intercalate ", " (map (\e -> idOf st e.index <> " (" <> fmt 3 e.probability <> ")") d.eject)
  decisionLines =
    [ "", "Decision" ]
      <> (if d.halt then [ "  HALT the queue: the base branch or CI is the most likely culprit. Fix it before blaming any PR." ] else [])
      <> (if d.merge /= 0 then [ "  merge now: " <> namesOf st d.merge <> " (green with probability " <> fmt 4 d.mergeClean <> ", risk budget " <> fmt 3 st.thresholds.risk <> ")" ] else [ "  merge now: nothing yet" ])
      <> (if Array.null d.eject then [] else [ "  eject: " <> ejectNames ])
      <> (if Array.null d.pending then [] else [ "  pending: " <> intercalate ", " (map (idOf st) d.pending) ])
  planLines
    | d.resolved = [ "", "Resolved: every PR is either merged or ejected. No more CI runs needed.", "" ]
    | r.runsLeft == 0 =
        [ "", "Budget spent with PRs still pending. Eject the pending PRs and requeue each one alone.", "" ]
    | Array.null r.plan.tests =
        [ "", "No run can teach more than the minimum gain. Flake is too high to separate these PRs; eject the pending ones and requeue alone.", "" ]
    | otherwise =
        [ ""
        , "Next round: " <> show (Array.length r.plan.tests) <> " parallel runs, expected information " <> fmt 2 r.plan.information <> " bits"
        ]
          <> Array.mapWithIndex
            ( \k t ->
                "  run " <> show (k + 1) <> ": " <> padRight 24 (testLabel st t.test)
                  <> "P(fail) "
                  <> fmt 3 t.failChance
                  <> "  adds "
                  <> fmt 2 t.gain
                  <> " bits"
            )
            r.plan.tests
          <> [ "Record each result as: observe <ids|base|all> pass|fail", "" ]

jsonNumber :: Number -> String
jsonNumber x = toStringWith (fixed 6) x

jsonArray :: Array String -> String
jsonArray xs = "[" <> intercalate "," xs <> "]"

jsonIds :: QueueState -> Int -> String
jsonIds st mask = jsonArray (map (quote <<< idOf st) (Array.filter (hasBit mask) (upTo 0 (st.model.size - 1))))

renderJson :: Report -> String
renderJson r =
  "{"
    <> intercalate ","
      [ field "prs" (jsonArray (map prJson positions))
      , field "envProbability" (jsonNumber (envMass r.posterior))
      , field "uncertaintyBits" (jsonNumber (entropyBits r.posterior))
      , field "runsUsed" (show r.runsUsed)
      , field "runsLeft" (show r.runsLeft)
      , field "explanations" (jsonArray (map explanationJson (topExplanations r.posterior)))
      , field "decision" decisionJson
      , field "nextRound" (jsonArray (map testJson r.plan.tests))
      , field "nextRoundBits" (jsonNumber r.plan.information)
      ]
    <> "}\n"
  where
  st = r.state
  d = r.decision
  positions = upTo 0 (st.model.size - 1)
  field k v = quote k <> ":" <> v
  prJson i =
    "{" <> intercalate ","
      [ field "id" (quote (idOf st i))
      , field "prior" (jsonNumber (fromMaybe 0.0 (Array.index st.model.priors i)))
      , field "badProbability" (jsonNumber (marginalBad r.posterior i))
      , field "conflictProbability" (jsonNumber (conflictInvolvement r.posterior i))
      , field "status" (quote (statusOf r i))
      ]
      <> "}"
  explanationJson s = "{" <> field "probability" (jsonNumber s.w) <> "," <> field "explanation" (quote (hypLabel st s.hyp)) <> "}"
  ejectJson e =
    "{" <> field "id" (quote (idOf st e.index)) <> "," <> field "probability" (jsonNumber e.probability) <> ","
      <> field "conflictsWith" (maybe "null" (quote <<< idOf st) e.partner)
      <> "}"
  decisionJson =
    "{" <> intercalate ","
      [ field "halt" (if d.halt then "true" else "false")
      , field "resolved" (if d.resolved then "true" else "false")
      , field "merge" (jsonIds st d.merge)
      , field "mergeGreenProbability" (jsonNumber d.mergeClean)
      , field "eject" (jsonArray (map ejectJson d.eject))
      , field "pending" (jsonArray (map (quote <<< idOf st) d.pending))
      ]
      <> "}"
  testJson t =
    "{" <> intercalate ","
      [ field "prs" (if t.test == 0 then "[]" else jsonIds st t.test)
      , field "base" (if t.test == 0 then "true" else "false")
      , field "observe" (quote (testLabel st t.test))
      , field "failProbability" (jsonNumber t.failChance)
      , field "gainBits" (jsonNumber t.gain)
      ]
      <> "}"

failWith :: Int -> String -> Effect Unit
failWith code msg = do
  writeStderr ("merge-queue-bisect-planner: " <> msg <> "\n")
  exitWith code

run :: Options -> String -> Effect Unit
run opts text = case parseState text of
  Left err -> failWith 2 err
  Right st -> case evaluate st opts.runners of
    Left err -> failWith 3 err
    Right report -> writeStdout (if opts.format == "json" then renderJson report else renderText report)

main :: Effect Unit
main = do
  args <- argv
  case parseArgs args of
    Left err -> do
      writeStderr usage
      failWith 2 err
    Right opts ->
      if opts.help then writeStdout usage
      else case opts.path of
        Nothing -> do
          writeStderr usage
          failWith 2 "missing state file (use - for stdin)"
        Just path -> do
          read <- try (readTextFile path)
          case read of
            Left e -> failWith 2 ("cannot read " <> path <> ": " <> message e)
            Right text -> run opts text
