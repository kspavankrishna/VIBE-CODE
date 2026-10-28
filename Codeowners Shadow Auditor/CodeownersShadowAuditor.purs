module CodeownersShadowAuditor
  ( main
  , Options
  , Format(..)
  , parseArgs
  , renderText
  , renderJson
  , gate
  , usage
  ) where

import Prelude

import CodeownersAnalysis (Report, Severity(..), analyze, parseSeverity, severityLabel)
import CodeownersFile (parseCodeowners)
import Data.Array as Array
import Data.Either (Either(..))
import Data.Int as Int
import Data.Maybe (Maybe(..), fromMaybe)
import Data.String as Str
import Effect (Effect)
import Effect.Exception (message, try)

foreign import readTextFile :: String -> Effect String
foreign import argv :: Effect (Array String)
foreign import writeStdout :: String -> Effect Unit
foreign import writeStderr :: String -> Effect Unit
foreign import exitWith :: Int -> Effect Unit
foreign import quote :: String -> String

data Format
  = Text
  | Json

derive instance eqFormat :: Eq Format

type Options =
  { codeowners :: String
  , files :: Maybe String
  , format :: Format
  , failOn :: Severity
  , maxUnowned :: Maybe Int
  , unownedLimit :: Int
  }

usage :: String
usage = Str.joinWith "\n"
  [ "usage: RunCodeownersShadowAuditor.mjs [options] <CODEOWNERS file>"
  , ""
  , "  --files FILE        newline separated repository paths, or - for stdin"
  , "  --format text|json  report format, default text"
  , "  --fail-on LEVEL     info, warn or error, default error"
  , "  --max-unowned N     fail when more than N files have no owner"
  , "  --unowned-limit N   unowned paths listed in the text report, default 25"
  , ""
  , "exit codes: 0 pass, 1 gate failed, 2 usage or input error"
  ]

defaults :: String -> Options
defaults path =
  { codeowners: path
  , files: Nothing
  , format: Text
  , failOn: Error
  , maxUnowned: Nothing
  , unownedLimit: 25
  }

parseArgs :: Array String -> Either String Options
parseArgs args = go args Nothing { files: Nothing, format: Text, failOn: Error, maxUnowned: Nothing, unownedLimit: 25 }
  where
  go rest path opts = case Array.uncons rest of
    Nothing -> case path of
      Nothing -> Left "missing CODEOWNERS file"
      Just p -> Right ((defaults p) { files = opts.files, format = opts.format, failOn = opts.failOn, maxUnowned = opts.maxUnowned, unownedLimit = opts.unownedLimit })
    Just { head: "--files", tail } -> withValue "--files" tail \v t -> go t path (opts { files = Just v })
    Just { head: "--format", tail } -> withValue "--format" tail \v t -> case v of
      "text" -> go t path (opts { format = Text })
      "json" -> go t path (opts { format = Json })
      _ -> Left ("--format must be text or json, got " <> v)
    Just { head: "--fail-on", tail } -> withValue "--fail-on" tail \v t -> case parseSeverity v of
      Just s -> go t path (opts { failOn = s })
      Nothing -> Left ("--fail-on must be info, warn or error, got " <> v)
    Just { head: "--max-unowned", tail } -> withValue "--max-unowned" tail \v t -> case Int.fromString v of
      Just n | n >= 0 -> go t path (opts { maxUnowned = Just n })
      _ -> Left ("--max-unowned needs a whole number, got " <> v)
    Just { head: "--unowned-limit", tail } -> withValue "--unowned-limit" tail \v t -> case Int.fromString v of
      Just n | n >= 0 -> go t path (opts { unownedLimit = n })
      _ -> Left ("--unowned-limit needs a whole number, got " <> v)
    Just { head: h, tail }
      | Str.take 2 h == "--" -> Left ("unknown option " <> h)
      | otherwise -> case path of
          Nothing -> go tail (Just h) opts
          Just _ -> Left ("unexpected extra argument " <> h)

  withValue name rest k = case Array.uncons rest of
    Nothing -> Left (name <> " needs a value")
    Just { head: v, tail } -> k v tail

gate :: Options -> Report -> Boolean
gate opts report = not (severityFails || unownedFails)
  where
  severityFails = Array.any (\f -> f.severity >= opts.failOn) report.findings
  unownedFails = case opts.maxUnowned, report.coverage of
    Just limit, Just cov -> Array.length cov.unowned > limit
    _, _ -> false

renderText :: Options -> Report -> String
renderText opts report = Str.joinWith "\n" (header <> findings <> covered) <> "\n"
  where
  header =
    [ "CODEOWNERS audit: " <> show report.rules <> " rules, " <> show report.effective <> " effective"
    ]

  findings
    | Array.null report.findings = [ "no findings" ]
    | otherwise = map line report.findings

  line f = pad 6 (severityLabel f.severity) <> " line " <> pad 5 (show f.line) <> pad 22 f.code <> f.message

  covered = case report.coverage of
    Nothing -> []
    Just cov ->
      [ ""
      , "coverage: " <> show cov.total <> " files, " <> show cov.owned <> " owned, " <> show (Array.length cov.unowned) <> " unowned"
      ]
        <> (if Array.null cov.unowned then [] else [ "unowned files:" ])
        <> map (\p -> "  " <> p) (Array.take opts.unownedLimit cov.unowned)
        <> (if Array.length cov.unowned > opts.unownedLimit then [ "  and " <> show (Array.length cov.unowned - opts.unownedLimit) <> " more" ] else [])
        <> (if Array.null cov.owners then [] else [ "files per owner:" ])
        <> map (\o -> "  " <> pad 40 o.owner <> show o.files) cov.owners

  pad n s = s <> Str.joinWith "" (Array.replicate (n - Str.length s) " ") <> (if Str.length s >= n then " " else "")

jsonArray :: Array String -> String
jsonArray xs = "[" <> Str.joinWith "," xs <> "]"

renderJson :: Report -> String
renderJson report =
  "{"
    <> "\"rules\":" <> show report.rules
    <> ",\"effective\":" <> show report.effective
    <> ",\"findings\":" <> jsonArray (map finding report.findings)
    <> ",\"coverage\":" <> fromMaybe "null" (map cov report.coverage)
    <> "}\n"
  where
  finding f =
    "{\"severity\":" <> quote (severityLabel f.severity)
      <> ",\"code\":" <> quote f.code
      <> ",\"line\":" <> show f.line
      <> ",\"message\":" <> quote f.message
      <> "}"

  cov c =
    "{\"total\":" <> show c.total
      <> ",\"owned\":" <> show c.owned
      <> ",\"unowned\":" <> jsonArray (map quote c.unowned)
      <> ",\"owners\":" <> jsonArray (map (\o -> "{\"owner\":" <> quote o.owner <> ",\"files\":" <> show o.files <> "}") c.owners)
      <> ",\"rules\":" <> jsonArray (map (\s -> "{\"line\":" <> show s.line <> ",\"matched\":" <> show s.matched <> ",\"won\":" <> show s.won <> "}") c.ruleStats)
      <> "}"

splitLines :: String -> Array String
splitLines s =
  Array.filter (_ /= "")
    (map Str.trim (Str.split (Str.Pattern "\n") (Str.replaceAll (Str.Pattern "\r") (Str.Replacement "") s)))

fail :: Int -> String -> Effect Unit
fail code msg = do
  writeStderr (msg <> "\n")
  exitWith code

main :: Effect Unit
main = do
  args <- argv
  case parseArgs args of
    Left err -> fail 2 (err <> "\n\n" <> usage)
    Right opts
      | opts.codeowners == "-" && opts.files == Just "-" -> fail 2 "CODEOWNERS and --files cannot both read stdin"
      | otherwise -> do
          loaded <- try do
            text <- readTextFile opts.codeowners
            fileText <- case opts.files of
              Nothing -> pure Nothing
              Just f -> map Just (readTextFile f)
            pure { text, fileText }
          case loaded of
            Left e -> fail 2 ("cannot read input: " <> message e)
            Right { text, fileText } -> do
              let report = analyze (parseCodeowners text) (map splitLines fileText)
              writeStdout (if opts.format == Json then renderJson report else renderText opts report)
              exitWith (if gate opts report then 0 else 1)
