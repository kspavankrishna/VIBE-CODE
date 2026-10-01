module DockerfileCacheAuditor
  ( main
  , Options
  , Format(..)
  , parseArgs
  , audit
  , renderText
  , renderJson
  , usage
  ) where

import Prelude

import CacheInvalidation (Changes, Plan, planInvalidation)
import CacheRules (Finding, Severity(..), auditDockerfile, parseSeverity, severityLabel)
import Data.Array as Array
import Data.Either (Either(..), either)
import Data.Enum (fromEnum)
import Data.Foldable (foldl)
import Data.Int as Int
import Data.Maybe (Maybe(..), fromMaybe)
import Data.String as Str
import Data.String.CodeUnits as CU
import Data.Tuple (Tuple(..))
import DockerfileParser (Dockerfile, parseDockerfile)
import Effect (Effect)
import Effect.Exception (message, try)

foreign import readTextFileImpl :: String -> Effect String
foreign import fileExists :: String -> Effect Boolean
foreign import argv :: Effect (Array String)
foreign import writeStdout :: String -> Effect Unit
foreign import writeStderr :: String -> Effect Unit
foreign import exitWith :: Int -> Effect Unit

data Format = Text | Json

derive instance eqFormat :: Eq Format

type Options =
  { dockerfile :: String
  , context :: Maybe String
  , changed :: Maybe String
  , args :: Array String
  , target :: Maybe String
  , format :: Format
  , failOn :: Severity
  , maxRebuild :: Maybe Int
  }

usage :: String
usage = Str.joinWith "\n"
  [ "Usage: DockerfileCacheAuditor [options] <Dockerfile>"
  , ""
  , "  --context DIR          build context root, used to find .dockerignore (default: Dockerfile directory)"
  , "  --changed FILE         newline separated list of changed paths, or - for stdin"
  , "  --arg NAME             a build arg whose value changed, repeatable"
  , "  --target STAGE         build target stage name or index (default: last stage)"
  , "  --format text|json     report format (default: text)"
  , "  --fail-on LEVEL        info, warn or error: lowest severity that fails the run (default: error)"
  , "  --max-rebuild PERCENT  fail when the predicted rebuild weight is above this percent"
  , ""
  , "Exit codes: 0 pass, 1 gate failed, 2 usage or input error"
  , ""
  ]

parseArgs :: Array String -> Either String Options
parseArgs = go { dockerfile: "", context: Nothing, changed: Nothing, args: [], target: Nothing, format: Text, failOn: Error, maxRebuild: Nothing }
  where
  go :: Options -> Array String -> Either String Options
  go o rest = case Array.uncons rest of
    Nothing ->
      if o.dockerfile == "" then Left "missing Dockerfile path"
      else Right o
    Just { head: flag, tail } ->
      if CU.take 2 flag == "--" then withValue o flag tail
      else if o.dockerfile /= "" then Left ("unexpected extra argument `" <> flag <> "`")
      else go (o { dockerfile = flag }) tail

  withValue o flag tail = case Array.uncons tail of
    Nothing -> Left ("option " <> flag <> " needs a value")
    Just { head: v, tail: more } -> case flag of
      "--context" -> go (o { context = Just v }) more
      "--changed" -> go (o { changed = Just v }) more
      "--arg" -> go (o { args = Array.snoc o.args v }) more
      "--target" -> go (o { target = Just v }) more
      "--format" -> case v of
        "text" -> go (o { format = Text }) more
        "json" -> go (o { format = Json }) more
        _ -> Left ("unknown format `" <> v <> "`")
      "--fail-on" -> case parseSeverity v of
        Just s -> go (o { failOn = s }) more
        Nothing -> Left ("unknown severity `" <> v <> "`")
      "--max-rebuild" -> case Int.fromString v of
        Just n | n >= 0 && n <= 100 -> go (o { maxRebuild = Just n }) more
        _ -> Left "--max-rebuild needs a whole number from 0 to 100"
      _ -> Left ("unknown option `" <> flag <> "`")

type Report =
  { dockerfile :: Dockerfile
  , findings :: Array Finding
  , plan :: Maybe Plan
  , gateFailed :: Boolean
  , gateReasons :: Array String
  }

audit :: Options -> Dockerfile -> Maybe (Array String) -> Array String -> Either String Report
audit o df dockerignore changedPaths = do
  let
    findings = auditDockerfile { dockerignore } df
    wantsPlan = o.changed /= Nothing || not (Array.null o.args)
  plan <-
    if wantsPlan then map Just (planInvalidation df (changes dockerignore))
    else Right Nothing
  let
    worst = foldl (\acc f -> if f.severity > acc then f.severity else acc) Info findings
    severityFail = not (Array.null findings) && worst >= o.failOn
    rebuildFail = case Tuple plan o.maxRebuild of
      Tuple (Just p) (Just limit) -> p.percent > limit
      _ -> false
    reasons = Array.catMaybes
      [ if severityFail then Just ("findings at or above " <> severityLabel o.failOn) else Nothing
      , if rebuildFail then Just ("predicted rebuild is " <> maybeShow (map _.percent plan) <> "% of the build weight, above the " <> maybeShow o.maxRebuild <> "% budget") else Nothing
      ]
  pure { dockerfile: df, findings, plan, gateFailed: not (Array.null reasons), gateReasons: reasons }
  where
  changes di = { paths: changedPaths, ignore: fromMaybe [] di, args: o.args, target: o.target } :: Changes
  maybeShow = case _ of
    Just n -> show n
    Nothing -> "?"

pad :: Int -> String -> String
pad n s = s <> Str.joinWith "" (Array.replicate (n - CU.length s) " ")

plural :: Int -> String -> String
plural n w = show n <> " " <> w <> (if n == 1 then "" else "s")

countOf :: Severity -> Array Finding -> Int
countOf s = Array.length <<< Array.filter (\f -> f.severity == s)

stageLabel :: Dockerfile -> Int -> String
stageLabel df i = case Array.index df.stages i of
  Just st -> "stage " <> show i <> maybe' st.name
  Nothing -> "stage " <> show i
  where
  maybe' = case _ of
    Just n -> " (" <> n <> ")"
    Nothing -> ""

renderText :: String -> Report -> String
renderText path r = Str.joinWith "\n" (Array.concat [ header, findingLines, planLines, gateLines, [ "" ] ])
  where
  df = r.dockerfile
  header =
    [ "Dockerfile Cache Auditor: " <> path
    , plural (Array.length df.stages) "stage" <> ", " <> plural (countOf Error r.findings) "error" <> ", " <> plural (countOf Warn r.findings) "warning" <> ", " <> plural (countOf Info r.findings) "note"
    , ""
    ]
  findingLines =
    if Array.null r.findings then [ "No cache findings." ]
    else Array.concatMap renderFinding r.findings
  renderFinding f =
    [ Str.toUpper (severityLabel f.severity) <> " " <> f.id <> " line " <> show f.line <> ", " <> stageLabel df f.stage
    , "  " <> f.message
    , "  fix: " <> f.fix
    , ""
    ]
  planLines = case r.plan of
    Nothing -> []
    Just p ->
      Array.concat
        [ [ "Cache invalidation, target " <> stageLabel df p.target
          , "  changed paths: " <> show (Array.length p.effectivePaths) <> " reach the build context, " <> show (Array.length p.ignoredPaths) <> " ignored by .dockerignore"
          ]
        , Array.concatMap stageBlock p.stages
        , [ "  predicted rebuild: " <> show p.rebuiltWeight <> " of " <> show p.totalWeight <> " weight (" <> show p.percent <> "%)" ]
        , case p.firstMissLine of
            Just l -> [ "  first cache miss: line " <> show l ]
            Nothing -> [ "  first cache miss: none, every layer is served from cache" ]
        , if Array.null p.positional then []
          else
            [ "  dependency layers that rerun only because they sit below the first miss:" ]
              <> map (\x -> "    line " <> show x.line <> "  " <> x.summary) p.positional
        ]
  stageBlock sp =
    case sp.firstMissLine of
      Nothing -> [ "  " <> stageLabel df sp.stage <> ": cached" ]
      Just l ->
        [ "  " <> stageLabel df sp.stage <> ": first miss at line " <> show l ]
          <> map rebuiltLine (Array.filter (\x -> x.weight > 0 || x.reason /= Nothing) sp.rebuilt)
  rebuiltLine x =
    "    " <> pad 6 ("L" <> show x.line) <> pad 4 ("w" <> show x.weight) <> x.summary
      <> case x.reason of
        Just why -> "   <- " <> why
        Nothing -> ""
  gateLines =
    if r.gateFailed then [ "", "FAILED: " <> Str.joinWith "; " r.gateReasons ]
    else [ "", "PASSED" ]

data J
  = JS String
  | JN Int
  | JB Boolean
  | JA (Array J)
  | JO (Array (Tuple String J))
  | JNull

jsonString :: String -> String
jsonString s = "\"" <> Str.joinWith "" (map esc (CU.toCharArray s)) <> "\""
  where
  esc c = case c of
    '"' -> "\\\""
    '\\' -> "\\\\"
    '\n' -> "\\n"
    '\r' -> "\\r"
    '\t' -> "\\t"
    _ -> if fromEnum c < 32 then "\\u00" <> hex2 (fromEnum c) else CU.singleton c
  hex2 n = digit (n / 16) <> digit (n `mod` 16)
  digit n = fromMaybe "0" (Array.index [ "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f" ] n)

showJ :: J -> String
showJ = case _ of
  JS s -> jsonString s
  JN n -> show n
  JB b -> if b then "true" else "false"
  JNull -> "null"
  JA xs -> "[" <> Str.joinWith "," (map showJ xs) <> "]"
  JO kvs -> "{" <> Str.joinWith "," (map (\(Tuple k v) -> jsonString k <> ":" <> showJ v) kvs) <> "}"

optInt :: Maybe Int -> J
optInt = case _ of
  Just n -> JN n
  Nothing -> JNull

optStr :: Maybe String -> J
optStr = case _ of
  Just s -> JS s
  Nothing -> JNull

renderJson :: String -> Report -> String
renderJson path r =
  showJ
    ( JO
        [ Tuple "dockerfile" (JS path)
        , Tuple "stages" (JN (Array.length r.dockerfile.stages))
        , Tuple "findings" (JA (map findingJ r.findings))
        , Tuple "invalidation" (case r.plan of
            Just p -> planJ p
            Nothing -> JNull)
        , Tuple "passed" (JB (not r.gateFailed))
        , Tuple "gateReasons" (JA (map JS r.gateReasons))
        ]
    )
    <> "\n"
  where
  findingJ f =
    JO
      [ Tuple "id" (JS f.id)
      , Tuple "severity" (JS (severityLabel f.severity))
      , Tuple "line" (JN f.line)
      , Tuple "stage" (JN f.stage)
      , Tuple "message" (JS f.message)
      , Tuple "fix" (JS f.fix)
      ]
  planJ p =
    JO
      [ Tuple "target" (JN p.target)
      , Tuple "firstMissLine" (optInt p.firstMissLine)
      , Tuple "totalWeight" (JN p.totalWeight)
      , Tuple "rebuiltWeight" (JN p.rebuiltWeight)
      , Tuple "percent" (JN p.percent)
      , Tuple "effectivePaths" (JA (map JS p.effectivePaths))
      , Tuple "ignoredPaths" (JA (map JS p.ignoredPaths))
      , Tuple "stagePlans" (JA (map stagePlanJ p.stages))
      , Tuple "positional" (JA (map rebuiltJ p.positional))
      ]
  stagePlanJ sp =
    JO
      [ Tuple "stage" (JN sp.stage)
      , Tuple "name" (optStr sp.name)
      , Tuple "firstMissLine" (optInt sp.firstMissLine)
      , Tuple "totalWeight" (JN sp.totalWeight)
      , Tuple "rebuiltWeight" (JN sp.rebuiltWeight)
      , Tuple "rebuilt" (JA (map rebuiltJ sp.rebuilt))
      ]
  rebuiltJ x =
    JO
      [ Tuple "line" (JN x.line)
      , Tuple "keyword" (JS x.keyword)
      , Tuple "summary" (JS x.summary)
      , Tuple "weight" (JN x.weight)
      , Tuple "reason" (optStr x.reason)
      ]

dirname :: String -> String
dirname p = case Array.last (Array.mapWithIndex (\i c -> { i, c }) (CU.toCharArray p) # Array.filter (\x -> x.c == '/')) of
  Just { i: 0 } -> "/"
  Just { i } -> CU.take i p
  Nothing -> "."

readLines :: String -> Array String
readLines t = Array.filter (not <<< Str.null) (map Str.trim (Str.split (Str.Pattern "\n") t))

readIgnore :: Options -> Effect (Maybe (Array String))
readIgnore o = do
  let
    sibling = o.dockerfile <> ".dockerignore"
    root = fromMaybe (dirname o.dockerfile) o.context <> "/.dockerignore"
  hasSibling <- fileExists sibling
  hasRoot <- fileExists root
  let
    chosen = if hasSibling then Just sibling else if hasRoot then Just root else Nothing
  case chosen of
    Nothing -> pure Nothing
    Just p -> do
      res <- try (readTextFileImpl p)
      pure (either (const Nothing) (Just <<< readLines) res)

fail :: String -> Effect Unit
fail msg = writeStderr (msg <> "\n") *> exitWith 2

main :: Effect Unit
main = do
  raw <- argv
  case parseArgs raw of
    Left err -> writeStderr ("error: " <> err <> "\n\n" <> usage) *> exitWith 2
    Right o -> do
      src <- try (readTextFileImpl o.dockerfile)
      case src of
        Left e -> fail ("error: cannot read " <> o.dockerfile <> ": " <> message e)
        Right text -> do
          di <- readIgnore o
          changedText <- case o.changed of
            Nothing -> pure (Right "")
            Just p -> try (readTextFileImpl p)
          case changedText of
            Left e -> fail ("error: cannot read changed paths: " <> message e)
            Right ct ->
              case audit o (parseDockerfile text) di (readLines ct) of
                Left err -> fail ("error: " <> err)
                Right report -> do
                  writeStdout (if o.format == Json then renderJson o.dockerfile report else renderText o.dockerfile report)
                  exitWith (if report.gateFailed then 1 else 0)
