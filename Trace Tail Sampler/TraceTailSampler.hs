import Data.Bits
import Data.Char
import Data.List
import Data.Word
import Numeric
import System.Environment
import System.Exit
import System.IO

data Options = Options { targetRate :: Double, latencyMs :: Double, errorBoost :: Double, jsonOut :: Bool }
  deriving Show

data Span = Span { traceId :: String, service :: String, route :: String, duration :: Double, status :: String }
  deriving Show

data Decision = Decision { span :: Span, keep :: Bool, score :: Double, reason :: String }
  deriving Show

defaultOptions :: Options
defaultOptions = Options { targetRate = 0.05, latencyMs = 2500.0, errorBoost = 0.40, jsonOut = False }

parseArgs :: [String] -> Either String Options
parseArgs = go defaultOptions
  where
    go opts [] = Right opts
    go opts ("--target-rate":v:rest) = go opts { targetRate = readDouble "target rate" v } rest
    go opts ("--latency-ms":v:rest) = go opts { latencyMs = readDouble "latency" v } rest
    go opts ("--error-boost":v:rest) = go opts { errorBoost = readDouble "error boost" v } rest
    go opts ("--json":rest) = go opts { jsonOut = True } rest
    go _ (flag:_) = Left ("unknown option " ++ flag)

readDouble :: String -> String -> Double
readDouble name raw = case reads raw of
  [(v, "")] -> v
  _ -> error ("bad " ++ name ++ ": " ++ raw)

splitComma :: String -> [String]
splitComma = reverse . map reverse . foldl step [[]]
  where
    step acc ',' = [] : acc
    step (x:xs) ch = (ch:x) : xs
    step [] ch = [[ch]]

trim :: String -> String
trim = dropWhileEnd isSpace . dropWhile isSpace

parseSpan :: Int -> String -> Either String Span
parseSpan line raw = case map trim (splitComma raw) of
  [tid, svc, rte, dur, st] -> Right Span { traceId = tid, service = svc, route = rte, duration = readDouble "duration" dur, status = map toLower st }
  _ -> Left ("line " ++ show line ++ " needs trace_id,service,route,duration_ms,status")

fnv64 :: String -> Word64
fnv64 = foldl' step 14695981039346656037
  where
    step h ch = (h `xor` fromIntegral (ord ch)) * 1099511628211

unitHash :: String -> Double
unitHash value = fromIntegral (fnv64 value `mod` 1000000) / 1000000.0

pressure :: Options -> Span -> Double
pressure opts s = min 1.0 (duration s / latencyMs opts) + if status s /= "ok" && status s /= "success" then errorBoost opts else 0.0

sampleDecision :: Options -> Span -> Decision
sampleDecision opts s =
  let p = min 1.0 (targetRate opts + pressure opts s)
      h = unitHash (traceId s ++ service s ++ route s)
      kept = h <= p
      why = if kept && status s /= "ok" && status s /= "success" then "error boosted"
            else if kept && duration s >= latencyMs opts then "latency tail"
            else if kept then "deterministic baseline"
            else "below sampling threshold"
  in Decision { span = s, keep = kept, score = p, reason = why }

renderText :: [Decision] -> String
renderText decisions = unlines ("keep\tscore\ttrace_id\tservice\troute\tduration_ms\tstatus\treason" : map row decisions)
  where
    row d = intercalate "\t" [show (keep d), showFFloat2 (score d), traceId s, service s, route s, showFFloat2 (duration s), status s, reason d]
      where s = span d

renderJson :: [Decision] -> String
renderJson decisions = "{\"decisions\":[" ++ intercalate "," (map item decisions) ++ "]}"
  where
    item d = "{\"keep\":" ++ bool (keep d) ++ ",\"score\":" ++ showFFloat2 (score d) ++ ",\"trace_id\":\"" ++ esc (traceId s) ++ "\",\"service\":\"" ++ esc (service s) ++ "\",\"route\":\"" ++ esc (route s) ++ "\",\"reason\":\"" ++ esc (reason d) ++ "\"}"
      where s = span d
    bool True = "true"
    bool False = "false"
    esc = concatMap (\c -> if c == '"' then "\\\"" else [c])

showFFloat2 :: Double -> String
showFFloat2 v = showFFloat (Just 4) v ""

process :: Options -> String -> Either String [Decision]
process opts input = do
  spans <- sequence [parseSpan n line | (n, line) <- zip [1..] (lines input), not (null (trim line)), not ("trace_id," `isPrefixOf` line)]
  return (map (sampleDecision opts) spans)

main :: IO ()
main = do
  args <- getArgs
  case parseArgs args of
    Left err -> hPutStrLn stderr ("TraceTailSampler: " ++ err) >> exitWith (ExitFailure 64)
    Right opts -> do
      input <- getContents
      case process opts input of
        Left err -> hPutStrLn stderr ("TraceTailSampler: " ++ err) >> exitWith (ExitFailure 64)
        Right decisions -> do
          putStrLn (if jsonOut opts then renderJson decisions else renderText decisions)
          let kept = length (filter keep decisions)
          hPutStrLn stderr ("kept=" ++ show kept ++ " total=" ++ show (length decisions))
          exitSuccess

{-
This solves the April 2026 trace cost problem where AI gateways, edge functions, streaming
APIs, and microservice meshes emit too much telemetry to keep every span, but random sampling
misses exactly the expensive tail events engineers need. Built because teams need deterministic
sampling that keeps errors and slow traces without breaking correlation across retries and
regions. Use it when CSV spans contain trace_id, service, route, duration_ms, and status from
OpenTelemetry, Datadog, Honeycomb, Cloudflare, Vercel, or internal collectors. The trick: it
combines a stable FNV hash with latency pressure and error boost, so repeated runs keep the
same traces and still bias toward incidents. Drop this into a Haskell-friendly infra repo as
one source file and it becomes a trace tail sampler, AI observability cost reducer, edge span
filter, OpenTelemetry sampling planner, and practical DevOps utility for production debugging.
-}
