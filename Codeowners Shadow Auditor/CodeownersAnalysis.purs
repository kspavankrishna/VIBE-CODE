module CodeownersAnalysis
  ( Severity(..)
  , Finding
  , RuleStat
  , OwnerCount
  , Coverage
  , Report
  , severityLabel
  , parseSeverity
  , lint
  , coverage
  , analyze
  , ownersOf
  ) where

import Prelude

import CodeownersFile (Issue(..), Rule, isEffective)
import CodeownersPattern (Pattern, Token(..), covers, matchesPath, sameShape, splitPath)
import Data.Array as Array
import Data.Map as Map
import Data.Maybe (Maybe(..), fromMaybe)
import Data.String as Str
import Data.Tuple (Tuple(..))

data Severity
  = Info
  | Warn
  | Error

derive instance eqSeverity :: Eq Severity
derive instance ordSeverity :: Ord Severity

severityLabel :: Severity -> String
severityLabel Info = "info"
severityLabel Warn = "warn"
severityLabel Error = "error"

parseSeverity :: String -> Maybe Severity
parseSeverity "info" = Just Info
parseSeverity "warn" = Just Warn
parseSeverity "error" = Just Error
parseSeverity _ = Nothing

type Finding =
  { severity :: Severity
  , code :: String
  , line :: Int
  , message :: String
  }

type RuleStat =
  { line :: Int
  , matched :: Int
  , won :: Int
  }

type OwnerCount =
  { owner :: String
  , files :: Int
  }

type Coverage =
  { total :: Int
  , owned :: Int
  , unowned :: Array String
  , owners :: Array OwnerCount
  , ruleStats :: Array RuleStat
  }

type Report =
  { rules :: Int
  , effective :: Int
  , findings :: Array Finding
  , coverage :: Maybe Coverage
  }

type Live =
  { rule :: Rule
  , pat :: Pattern
  }

live :: Array Rule -> Array Live
live = Array.mapMaybe \r ->
  if isEffective r then map (\p -> { rule: r, pat: p }) r.pattern else Nothing

fileCount :: Int -> String
fileCount n = show n <> (if n == 1 then " file" else " files")

joinWith :: String -> Array String -> String
joinWith = Str.joinWith

issueFinding :: Rule -> Issue -> Finding
issueFinding r issue = case issue of
  BadOwner o ->
    { severity: Error
    , code: "invalid-owner"
    , line: r.line
    , message: "owner " <> o <> " is not @user, @org/team or an email address, so GitHub skips this whole line and the path stays with the previous matching rule"
    }
  Negation ->
    { severity: Error
    , code: "unsupported-negation"
    , line: r.line
    , message: "pattern " <> r.text <> " starts with !, which CODEOWNERS does not support, so the rule is skipped"
    }
  CharRange ->
    { severity: Error
    , code: "unsupported-range"
    , line: r.line
    , message: "pattern " <> r.text <> " uses [ ] character ranges, which CODEOWNERS does not support, so the rule is skipped"
    }
  Escape ->
    { severity: Error
    , code: "unsupported-escape"
    , line: r.line
    , message: "pattern " <> r.text <> " uses a backslash escape other than a leading \\#, so the rule is skipped"
    }
  EmptyPattern ->
    { severity: Error
    , code: "empty-pattern"
    , line: r.line
    , message: "pattern " <> r.text <> " has no path segment and matches nothing"
    }
  RepeatedOwner o ->
    { severity: Warn
    , code: "repeated-owner"
    , line: r.line
    , message: "owner " <> o <> " is listed more than once on this line"
    }

shadowFindings :: Array Live -> Array Finding
shadowFindings eff = Array.catMaybes (Array.mapWithIndex check eff)
  where
  check i e =
    map (finding e) (Array.find (\q -> covers q.pat e.pat) (Array.drop (i + 1) eff))

  finding e q
    | sameShape e.pat q.pat =
        { severity: Warn
        , code: "duplicate-pattern"
        , line: e.rule.line
        , message: "pattern " <> e.rule.text <> " is repeated on line " <> show q.rule.line <> ", and the last match wins, so this line never decides ownership"
        }
    | otherwise =
        { severity: Warn
        , code: "shadowed"
        , line: e.rule.line
        , message: "every path matched by " <> e.rule.text <> " is also matched by " <> q.rule.text <> " on line " <> show q.rule.line <> ", and the last match wins, so this line never decides ownership"
        }

isCatchAll :: Pattern -> Boolean
isCatchAll p = p.tokens == [ DoubleStar, Glob "*" ]

overrideFindings :: Array Live -> Array Finding
overrideFindings eff = Array.catMaybes (Array.mapWithIndex check eff)
  where
  check j e = do
    q <- Array.last (Array.filter (\x -> covers x.pat e.pat) (Array.take j eff))
    let dropped = Array.filter (\o -> not (Array.elem o e.rule.owners)) q.rule.owners
    if Array.null dropped || Array.null e.rule.owners || isCatchAll q.pat || sameShape q.pat e.pat then
      Nothing
    else
      Just
        { severity: Info
        , code: "owner-override"
        , line: e.rule.line
        , message: "paths matched by " <> e.rule.text <> " no longer need " <> joinWith " " dropped <> " from line " <> show q.rule.line <> ", because CODEOWNERS replaces owners and never adds to them"
        }

clearsFindings :: Array Live -> Array Finding
clearsFindings eff = Array.mapMaybe check eff
  where
  check e
    | Array.null e.rule.owners =
        Just
          { severity: Info
          , code: "clears-ownership"
          , line: e.rule.line
          , message: "pattern " <> e.rule.text <> " has no owners, so matching paths become unowned on purpose"
          }
    | otherwise = Nothing

byLine :: Finding -> Finding -> Ordering
byLine a b = compare a.line b.line <> compare b.severity a.severity <> compare a.code b.code

lint :: Array Rule -> Array Finding
lint rules =
  Array.sortBy byLine
    ( Array.concat
        [ Array.concatMap (\r -> map (issueFinding r) r.issues) rules
        , shadowFindings eff
        , overrideFindings eff
        , clearsFindings eff
        ]
    )
  where
  eff = live rules

ownersOf :: Array Rule -> Array String -> Array String
ownersOf rules segs =
  case Array.last (Array.filter (\e -> matchesPath e.pat segs) (live rules)) of
    Nothing -> []
    Just e -> e.rule.owners

coverage :: Array Rule -> Array String -> Coverage
coverage rules files =
  { total: Array.length rows
  , owned: Array.length (Array.filter (\r -> not (Array.null (ownersFor r))) rows)
  , unowned: map _.path (Array.filter (\r -> Array.null (ownersFor r)) rows)
  , owners: sortedOwners
  , ruleStats: Array.mapWithIndex stat eff
  }
  where
  eff = live rules

  rows :: Array { path :: String, hits :: Array Int }
  rows = map row (Array.nub files)

  row p =
    let
      segs = splitPath p
    in
      { path: p
      , hits: Array.catMaybes (Array.mapWithIndex (\i e -> if matchesPath e.pat segs then Just i else Nothing) eff)
      }

  ownersFor r = case Array.last r.hits >>= Array.index eff of
    Nothing -> []
    Just e -> e.rule.owners

  stat i e =
    { line: e.rule.line
    , matched: Array.length (Array.filter (\r -> Array.elem i r.hits) rows)
    , won: Array.length (Array.filter (\r -> Array.last r.hits == Just i) rows)
    }

  tally = Array.foldl (\m r -> Array.foldl (\m2 o -> Map.insertWith (+) o 1 m2) m (ownersFor r)) Map.empty rows

  sortedOwners =
    Array.sortBy (\a b -> compare b.files a.files <> compare a.owner b.owner)
      (map (\(Tuple o c) -> { owner: o, files: c }) (Map.toUnfoldable tally))

coverageFindings :: Array Finding -> Coverage -> Array Finding
coverageFindings known cov = Array.concatMap check cov.ruleStats
  where
  flagged line = Array.any (\f -> f.line == line && (f.code == "shadowed" || f.code == "duplicate-pattern")) known

  check s
    | s.matched == 0 =
        [ { severity: Warn
          , code: "unmatched-rule"
          , line: s.line
          , message: "matches none of the " <> fileCount cov.total <> " supplied, so the pattern is stale or mistyped"
          }
        ]
    | s.won == 0 && not (flagged s.line) =
        [ { severity: Warn
          , code: "never-wins"
          , line: s.line
          , message: "matches " <> fileCount s.matched <> " but later rules claim every one of them, so the owners here never apply"
          }
        ]
    | otherwise = []

analyze :: Array Rule -> Maybe (Array String) -> Report
analyze rules files =
  { rules: Array.length rules
  , effective: Array.length (live rules)
  , findings: Array.sortBy byLine (static <> fromMaybe [] (map (coverageFindings static) cov))
  , coverage: cov
  }
  where
  static = lint rules
  cov = map (coverage rules) files
