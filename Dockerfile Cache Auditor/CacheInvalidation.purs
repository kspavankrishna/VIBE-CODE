module CacheInvalidation
  ( Rebuilt
  , StagePlan
  , Plan
  , Changes
  , planInvalidation
  ) where

import Prelude

import CacheRules (declaredArgs, isBuildStep, refsIn, runWeight)
import Data.Array as Array
import Data.Either (Either(..))
import Data.Foldable (any, foldl, sum)
import Data.Maybe (Maybe(..), fromMaybe, isJust, maybe)
import Data.String as Str
import Data.String.CodeUnits as CU
import DockerfileParser (Dockerfile, Instruction, Stage, flagValue, resolveStage, sourcesOf, stageDeps, words)
import PathGlob (contextMatches, dockerignoreIgnores, normalizePath)

type Changes =
  { paths :: Array String
  , ignore :: Array String
  , args :: Array String
  , target :: Maybe String
  }

type Rebuilt =
  { line :: Int
  , keyword :: String
  , summary :: String
  , weight :: Int
  , reason :: Maybe String
  , stage :: Int
  , movable :: Boolean
  }

type StagePlan =
  { stage :: Int
  , name :: Maybe String
  , firstMissLine :: Maybe Int
  , rebuilt :: Array Rebuilt
  , totalWeight :: Int
  , rebuiltWeight :: Int
  }

type Plan =
  { stages :: Array StagePlan
  , ignoredPaths :: Array String
  , effectivePaths :: Array String
  , totalWeight :: Int
  , rebuiltWeight :: Int
  , percent :: Int
  , target :: Int
  , firstMissLine :: Maybe Int
  , positional :: Array Rebuilt
  }

type Env =
  { df :: Dockerfile
  , paths :: Array String
  , args :: Array String
  , hit :: Array Int
  }

type Spec = Array { k :: String, v :: String }

mountSpecs :: Instruction -> Array Spec
mountSpecs i = map parse (Array.filter (\f -> f.name == "mount") i.flags)
  where
  parse f = map kv (Str.split (Str.Pattern ",") f.value)
  kv s = case CU.indexOf (Str.Pattern "=") s of
    Just n -> { k: CU.take n s, v: CU.drop (n + 1) s }
    Nothing -> { k: s, v: "" }

specValue :: String -> Spec -> Maybe String
specValue key spec = map _.v (Array.find (\p -> p.k == key) spec)

firstMatch :: Array String -> String -> Maybe String
firstMatch paths src = Array.find (contextMatches src) paths

isRemote :: String -> Boolean
isRemote s = any (\p -> Str.contains (Str.Pattern p) (CU.take 8 s)) [ "http://", "https://", "git@", "git://" ]

summarize :: Instruction -> String
summarize i =
  let
    flagText = map (\f -> "--" <> f.name <> (if f.value == "" then "" else "=" <> f.value)) i.flags
    flat = Str.joinWith " " (words (Str.joinWith " " ([ i.keyword ] <> flagText <> [ i.args ])))
  in
    if CU.length flat > 72 then CU.take 69 flat <> "..." else flat

hitStage :: Env -> String -> Maybe Int
hitStage env ref = case resolveStage env.df ref of
  Just n | Array.elem n env.hit -> Just n
  _ -> Nothing

trigger :: Env -> Stage -> Maybe String -> Instruction -> { reason :: Maybe String, pending :: Maybe String }
trigger env st pending i = case i.keyword of
  "FROM" ->
    let
      base = Array.find (\o -> o.index < st.index && map Str.toLower o.name == Just (Str.toLower st.image)) env.df.stages
      baseHit = case base of
        Just b | Array.elem b.index env.hit -> Just ("base stage " <> show b.index <> " is rebuilt")
        _ -> Nothing
      argHit = map (\a -> "build arg " <> a <> " changed and selects the base image") (Array.find (\a -> Array.elem a env.args) (refsIn st.image))
    in
      { reason: orElse baseHit argHit, pending }
  "COPY" -> { reason: copyReason, pending }
  "ADD" -> { reason: copyReason, pending }
  "ARG" ->
    { reason: Nothing
    , pending: case Array.find (\a -> Array.elem a env.args) (declaredArgs i) of
        Just a -> Just a
        Nothing -> pending
    }
  "ENV" -> { reason: refReason, pending }
  "LABEL" -> { reason: refReason, pending }
  "RUN" ->
    { reason: orElse (map (\a -> "build arg " <> a <> " changed and this is the first RUN after its ARG") pending) (orElse mountReason Nothing)
    , pending: Nothing
    }
  _ -> { reason: Nothing, pending }
  where
  orElse a b = case a of
    Just _ -> a
    Nothing -> b

  copyReason = case flagValue "from" i of
    Just ref -> map (\n -> "copies from stage " <> show n <> ", which is rebuilt") (hitStage env ref)
    Nothing ->
      Array.findMap
        (\src -> if isRemote src then Nothing else map (\p -> "source `" <> display src <> "` matches changed `" <> p <> "`") (firstMatch env.paths src))
        (sourcesOf i).sources

  display s = if s == "" then "." else s

  refReason = map (\a -> "build arg " <> a <> " is expanded here") (Array.find (\a -> Array.elem a env.args) (refsIn i.args))

  mountReason = Array.findMap oneMount (mountSpecs i)

  oneMount spec = case specValue "from" spec of
    Just ref -> map (\n -> "mounts stage " <> show n <> ", which is rebuilt") (hitStage env ref)
    Nothing ->
      if fromMaybe "bind" (specValue "type" spec) /= "bind" then Nothing
      else
        let
          src = fromMaybe "" (specValue "source" spec)
        in
          map (\p -> "bind mount source `" <> display src <> "` matches changed `" <> p <> "`") (firstMatch env.paths src)

closure :: Dockerfile -> Int -> Array Int
closure df t = go [ t ] []
  where
  go todo seen = case Array.uncons todo of
    Nothing -> Array.sort seen
    Just { head, tail } ->
      if Array.elem head seen then go tail seen
      else go (tail <> maybe [] (stageDeps df) (Array.index df.stages head)) (Array.snoc seen head)

planStage :: Env -> Stage -> StagePlan
planStage env st =
  let
    units = Array.cons st.from st.body
    scan = foldl step { pending: Nothing, out: [] } units
    step acc u =
      let
        r = trigger env st acc.pending u
      in
        { pending: r.pending, out: Array.snoc acc.out r.reason }
    reasons = scan.out
    firstIdx = Array.findIndex isJust reasons
    mkRebuilt r =
      { line: r.x.line
      , keyword: r.x.keyword
      , summary: summarize r.x
      , weight: runWeight r.x
      , reason: join (Array.index reasons r.i)
      , stage: st.index
      , movable: runWeight r.x >= 6 && not (isBuildStep r.x)
      }
    rebuilt = case firstIdx of
      Nothing -> []
      Just k -> map mkRebuilt (Array.filter (\r -> r.i >= k) (Array.mapWithIndex (\i x -> { i, x }) units))
  in
    { stage: st.index
    , name: st.name
    , firstMissLine: map (\k -> fromMaybe st.from.line (map _.line (Array.index units k))) firstIdx
    , rebuilt
    , totalWeight: sum (map runWeight st.body)
    , rebuiltWeight: sum (map _.weight rebuilt)
    }

planInvalidation :: Dockerfile -> Changes -> Either String Plan
planInvalidation df ch = case Array.length df.stages of
  0 -> Left "the Dockerfile has no FROM instruction"
  n -> case targetIndex n of
    Nothing -> Left ("target stage `" <> fromMaybe "" ch.target <> "` does not exist")
    Just target ->
      let
        normalized = Array.nub (Array.filter (_ /= "") (map (normalizePath <<< Str.replaceAll (Str.Pattern "\\") (Str.Replacement "/")) ch.paths))
        ignored = Array.filter (dockerignoreIgnores ch.ignore) normalized
        effective = Array.filter (not <<< dockerignoreIgnores ch.ignore) normalized
        needed = Array.mapMaybe (Array.index df.stages) (closure df target)
        run acc st =
          let
            p = planStage { df, paths: effective, args: ch.args, hit: acc.hit } st
          in
            { hit: if isJust p.firstMissLine then Array.snoc acc.hit st.index else acc.hit
            , plans: Array.snoc acc.plans p
            }
        done = foldl run { hit: [], plans: [] } needed
        total = sum (map _.totalWeight done.plans)
        rebuiltW = sum (map _.rebuiltWeight done.plans)
        allRebuilt = Array.concatMap _.rebuilt done.plans
      in
        Right
          { stages: done.plans
          , ignoredPaths: ignored
          , effectivePaths: effective
          , totalWeight: total
          , rebuiltWeight: rebuiltW
          , percent: if total == 0 then 0 else (rebuiltW * 100) `div` total
          , target
          , firstMissLine: Array.head (Array.sort (Array.mapMaybe _.firstMissLine done.plans))
          , positional: Array.filter (\r -> r.movable && r.reason == Nothing) allRebuilt
          }
  where
  targetIndex n = case ch.target of
    Nothing -> Just (n - 1)
    Just t -> resolveStage df t
