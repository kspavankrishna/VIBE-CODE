{-# LANGUAGE OverloadedStrings #-}

import Control.Applicative ((<|>))
import Control.Exception (SomeException, try)
import Control.Monad (forM, unless, when)
import Data.Aeson
import qualified Data.Aeson.Key as Key
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString as BS
import Data.Char (isAlphaNum, isDigit, toLower)
import Data.List (foldl', sortOn)
import qualified Data.List as List
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (catMaybes, fromMaybe, isJust, listToMaybe, mapMaybe)
import qualified Data.Set as Set
import Data.Set (Set)
import Data.Scientific (FPFormat (Fixed), formatScientific)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import Data.Time (UTCTime, diffUTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Time.Format.ISO8601 (iso8601ParseM)
import Numeric (showFFloat)
import System.Directory (doesDirectoryExist, doesFileExist, listDirectory)
import System.Environment (getArgs)
import System.Exit (ExitCode (..), exitWith)
import System.FilePath ((</>), takeExtension)
import System.IO (hPutStrLn, stderr)
import Text.Read (readMaybe)

data Config = Config
  { cTracePath :: FilePath,
    cBillPath :: FilePath,
    cPricingPath :: Maybe FilePath,
    cTimeSlop :: Double,
    cTokenDelta :: Int,
    cCostDelta :: Double,
    cRequireExact :: Bool
  }

data Rec = Rec
  { rSource :: Text,
    rReq :: Maybe Text,
    rProvider :: Maybe Text,
    rModel :: Maybe Text,
    rInput :: Maybe Int,
    rCached :: Maybe Int,
    rOutput :: Maybe Int,
    rCost :: Maybe Double,
    rTime :: Maybe UTCTime
  }

data Price = Price {pIn :: Double, pCached :: Double, pOut :: Double}
data Kind = Missing | Orphan | Duplicate | Ambiguous | TokenMismatch | CostMismatch | CacheMiss | ParseWarn deriving (Eq, Ord, Show)
data Sev = Err | Warn deriving (Eq, Ord, Show)
data Finding = Finding {fSev :: Sev, fKind :: Kind, fSubject :: Text, fDetail :: Text, fAmount :: Maybe Double}

main :: IO ()
main = do
  args <- getArgs
  case parseArgs args of
    Left err -> hPutStrLn stderr (err <> "\n" <> usage) >> exitWith (ExitFailure 64)
    Right cfg -> do
      errs <- validate cfg
      unless (null errs) $ hPutStrLn stderr (unlines errs <> usage) >> exitWith (ExitFailure 64)
      (traceRows, traceErrs) <- loadRows (cTracePath cfg)
      (billRows, billErrs) <- loadRows (cBillPath cfg)
      prices <- maybe (pure Map.empty) loadPricing (cPricingPath cfg)
      let traces = mapMaybe rowToRec traceRows
      let bills = mapMaybe rowToRec billRows
      let findings = parseWarnings traceErrs billErrs traceRows traces billRows bills ++ reconcile cfg prices traces bills
      report cfg prices traces bills findings
      exitWith $ if any ((== Err) . fSev) findings then ExitFailure 2 else ExitSuccess

parseArgs :: [String] -> Either String Config
parseArgs xs = go xs def [] where
  def = Config "" "" Nothing 600 32 0.02 False
  go [] cfg pos = case pos of
    [a,b] -> Right cfg{cTracePath=a,cBillPath=b}
    [] | not (null (cTracePath cfg) || null (cBillPath cfg)) -> Right cfg
    _ -> Left "Usage requires <traces> <billing>."
  go ("--traces":v:rest) cfg pos = go rest cfg{cTracePath=v} pos
  go ("--billing":v:rest) cfg pos = go rest cfg{cBillPath=v} pos
  go ("--pricing":v:rest) cfg pos = go rest cfg{cPricingPath=Just v} pos
  go ("--time-slop-seconds":v:rest) cfg pos = maybe (Left "Bad --time-slop-seconds") (\n -> go rest cfg{cTimeSlop=n} pos) (readMaybe v)
  go ("--token-delta":v:rest) cfg pos = maybe (Left "Bad --token-delta") (\n -> go rest cfg{cTokenDelta=n} pos) (readMaybe v)
  go ("--cost-delta-usd":v:rest) cfg pos = maybe (Left "Bad --cost-delta-usd") (\n -> go rest cfg{cCostDelta=n} pos) (readMaybe v)
  go ("--require-exact-request-id":rest) cfg pos = go rest cfg{cRequireExact=True} pos
  go ("--help":_) _ _ = Left "Help requested."
  go ("-h":_) _ _ = Left "Help requested."
  go (flag:_) _ _ | "--" `List.isPrefixOf` flag = Left ("Unknown flag: " <> flag)
  go (v:rest) cfg pos = go rest cfg (pos ++ [v])

validate :: Config -> IO [String]
validate cfg = do
  t1 <- doesFileExist (cTracePath cfg); t2 <- doesDirectoryExist (cTracePath cfg)
  b1 <- doesFileExist (cBillPath cfg); b2 <- doesDirectoryExist (cBillPath cfg)
  p <- maybe (pure True) doesFileExist (cPricingPath cfg)
  pure $ [ "Trace input not found." | not (t1 || t2) ]
      ++ [ "Billing input not found." | not (b1 || b2) ]
      ++ [ "Pricing input not found." | not p ]
      ++ [ "time-slop-seconds must be >= 0." | cTimeSlop cfg < 0 ]
      ++ [ "token-delta must be >= 0." | cTokenDelta cfg < 0 ]
      ++ [ "cost-delta-usd must be >= 0." | cCostDelta cfg < 0 ]

loadRows :: FilePath -> IO ([Map Text Text], [Text])
loadRows root = do
  files <- walk root
  rs <- forM files parseFile
  pure (concatMap fst rs, concatMap snd rs)

walk :: FilePath -> IO [FilePath]
walk root = do
  f <- doesFileExist root; d <- doesDirectoryExist root
  if f then pure [root | okExt root] else
    if d then concat <$> mapM (walk . (root </>)) =<< listDirectory root else pure []

okExt :: FilePath -> Bool
okExt p = map toLower (takeExtension p) `elem` [".json",".jsonl",".ndjson",".csv",".tsv"]

parseFile :: FilePath -> IO ([Map Text Text], [Text])
parseFile path = do
  bytesE <- try (BS.readFile path) :: IO (Either SomeException BS.ByteString)
  case bytesE of
    Left e -> pure ([], [T.pack path <> ": " <> T.pack (show e)])
    Right bytes -> pure $ case map toLower (takeExtension path) of
      ".csv" -> parseDelimited ',' path bytes
      ".tsv" -> parseDelimited '\t' path bytes
      ".json" -> either (const (parseJsonl path bytes)) (rowsFromValue path) (eitherDecodeStrict' bytes)
      _ -> parseJsonl path bytes

parseDelimited :: Char -> FilePath -> BS.ByteString -> ([Map Text Text], [Text])
parseDelimited delim path bytes = case filter (not . T.null . T.strip) (T.lines (utf8 bytes)) of
  [] -> ([], [])
  h:rows ->
    let headers = map normKey (splitCSV delim h)
        mk line = Map.fromList [(k, T.strip v) | (k,v) <- zip headers (splitCSV delim line), not (T.null k)]
    in (map mk rows, [])

parseJsonl :: FilePath -> BS.ByteString -> ([Map Text Text], [Text])
parseJsonl path bytes = foldl' step ([], []) (zip [1::Int ..] (T.lines (utf8 bytes))) where
  step (rows, errs) (ln, line)
    | T.null (T.strip line) = (rows, errs)
    | otherwise = case eitherDecodeStrict' (TE.encodeUtf8 line) of
        Right v -> case flatObj v of
          Just m -> (m:rows, errs)
          Nothing -> (rows, errs ++ [loc ln <> " expected JSON object"])
        Left e -> (rows, errs ++ [loc ln <> T.pack e])
  loc ln = T.pack path <> ":" <> T.pack (show ln) <> ": "

rowsFromValue :: FilePath -> Value -> ([Map Text Text], [Text])
rowsFromValue path v = case v of
  Object _ -> maybe ([], [T.pack path <> ": expected object"]) (\m -> ([m], [])) (flatObj v)
  Array xs -> foldl' step ([], []) (zip [1::Int ..] (V.toList xs))
  _ -> ([], [T.pack path <> ": top-level JSON must be object or array"])
  where
    step (rows, errs) (i, x) = case flatObj x of
      Just m -> (m:rows, errs)
      Nothing -> (rows, errs ++ [T.pack path <> ":" <> T.pack (show i) <> ": expected object"])

flatObj :: Value -> Maybe (Map Text Text)
flatObj (Object o) = Just (Map.fromListWith keepShort (go [] (Object o))) where
  go pre val = case val of
    Object inner -> concatMap (\(k,x) -> go (pre ++ [normSeg (Key.toText k)]) x) (KM.toList inner)
    Array xs -> let s = mapMaybe scalar (V.toList xs) in if null s then [] else [(joinKey pre, T.intercalate "," s)]
    _ -> maybe [] (\t -> [(joinKey pre, T.strip t)]) (scalar val)
  scalar (String t) = Just t
  scalar (Number n) = Just (T.pack (formatScientific Fixed Nothing n))
  scalar (Bool True) = Just "true"
  scalar (Bool False) = Just "false"
  scalar _ = Nothing
  keepShort a b = if T.length a <= T.length b then a else b
flatObj _ = Nothing

rowToRec :: Map Text Text -> Maybe Rec
rowToRec m
  | all (not . isJust) anchors = Nothing
  | otherwise = Just Rec
      { rSource = fromMaybe "unknown" (lookupAny ["request_id","id","source"] m) <> "#" <> fromMaybe "row" (lookupAny ["line","row"] m <|> Just "1"),
        rReq = clean <$> lookupAny ["request_id","external_request_id","response_id","id","request.id","response.id"] m,
        rProvider = clean <$> lookupAny ["provider","vendor","gateway.provider","billing_provider"] m,
        rModel = clean <$> lookupAny ["model","model_name","route.model","deployment"] m,
        rInput = lookupInt ["input_tokens","prompt_tokens","usage.input_tokens","usage.prompt_tokens","billed_input_tokens"] m,
        rCached = lookupInt ["cached_input_tokens","usage.cached_input_tokens","prompt_tokens_details.cached_tokens","billed_cached_input_tokens"] m,
        rOutput = lookupInt ["output_tokens","completion_tokens","usage.output_tokens","usage.completion_tokens","billed_output_tokens"] m,
        rCost = lookupDouble ["cost_usd","usd_cost","total_cost_usd","amount_usd","cost"] m,
        rTime = lookupTime ["timestamp","time","ts","created_at","started_at","completed_at","invoice_timestamp"] m
      }
  where
    anchors = [lookupAny ["request_id","id"] m, lookupAny ["provider","model"] m, fmap (T.pack . show) (lookupInt ["input_tokens","output_tokens","cost_usd"] m)]

loadPricing :: FilePath -> IO (Map Text Price)
loadPricing path = do
  e <- try (BS.readFile path) :: IO (Either SomeException BS.ByteString)
  pure $ case e of
    Right bytes -> either (const Map.empty) pricesFrom (eitherDecodeStrict' bytes)
    Left _ -> Map.empty

pricesFrom :: Value -> Map Text Price
pricesFrom v = Map.fromList (go [] v) where
  go path' (Object o) =
    let self = case priceFrom path' o of Just p -> [p]; Nothing -> []
        kids = concatMap (\(k,x) -> go (path' ++ [Key.toText k]) x) (KM.toList o)
    in self ++ kids
  go path' (Array xs) = concatMap (go path') (V.toList xs)
  go _ _ = []
  priceFrom path' o = do
    let m = fromMaybe Map.empty (flatObj (Object o))
    i <- lookupDouble ["input_per_1m","input_price_per_1m","prompt_usd_per_1m"] m
    o' <- lookupDouble ["output_per_1m","output_price_per_1m","completion_usd_per_1m"] m
    let c = fromMaybe i (lookupDouble ["cached_input_per_1m","cached_prompt_usd_per_1m"] m)
    (p,mdl) <- case (lookupAny ["provider"] m, lookupAny ["model","model_name"] m, reverse path') of
      (Just p, Just mdl, _) -> Just (clean p, clean mdl)
      (_, _, seg:_) | "/" `T.isInfixOf` seg -> case T.splitOn "/" seg of [p,mdl] -> Just (clean p, clean mdl); _ -> Nothing
      (_, _, mdl:p:_) -> Just (clean p, clean mdl)
      _ -> Nothing
    pure (p <> "/" <> mdl, Price i c o')

reconcile :: Config -> Map Text Price -> [Rec] -> [Rec] -> [Finding]
reconcile cfg prices traces bills = traceFindings ++ orphanFindings where
  exact = Map.fromListWith (++) [(k,[b]) | b <- bills, Just k <- [rReq b]]
  (used, traceFindings) = foldl' step (Set.empty, []) traces
  step (seen, acc) tr = case matchTrace seen tr of
    Left f -> (seen, acc ++ [f])
    Right Nothing -> (seen, acc ++ [mk Err Missing (subject tr) "No billing row matched this trace." (rCost tr)])
    Right (matched, seen') -> (seen', acc ++ comparePair tr matched)
  matchTrace seen tr = case rReq tr >>= (`Map.lookup` exact) of
    Just xs ->
      let ys = filter (\b -> not (Set.member (subject b) seen)) xs
      in if null ys then Right Nothing else Right (ys, foldl' (flip (Set.insert . subject)) seen ys)
    Nothing | cRequireExact cfg -> Right Nothing
    Nothing -> case fuzzy seen tr of
      [] -> Right Nothing
      [b] -> Right ([b], Set.insert (subject b) seen)
      bs -> Left (mk Err Ambiguous (subject tr) "More than one billing row was a plausible fuzzy match." Nothing)
  fuzzy seen tr =
    [ b | b <- bills
        , not (Set.member (subject b) seen)
        , same rProvider tr b, same rModel tr b
        , closeI (rInput tr) (rInput b), closeI (rCached tr) (rCached b), closeI (rOutput tr) (rOutput b)
        , maybe True (<= cTimeSlop cfg) (gap <$> rTime tr <*> rTime b)
        , anchored tr b
    ]
  tracePrice tr = do p <- rProvider tr; m <- rModel tr; Map.lookup (p <> "/" <> m) prices
  comparePair tr matched =
    let b = head (sortOn (score tr) matched)
        dup = [mk Err Duplicate (subject tr) "More than one billing row shares this request." (extra matched) | length matched > 1]
        toks = catMaybes
          [ tok tr "input tokens" (rInput tr) (rInput b)
          , tok tr "cached input tokens" (rCached tr) (rCached b)
          , tok tr "output tokens" (rOutput tr) (rOutput b)
          ]
        costFind = case (tracePrice tr, rCost b) of
          (Just p, Just actual) ->
            let expected = expectedCost p tr in [mk Err CostMismatch (subject tr) ("Expected " <> tUsd expected <> " but billed " <> tUsd actual <> ".") (Just (actual - expected)) | abs (actual - expected) > cCostDelta cfg]
          _ -> []
        cacheFind = case (tracePrice tr, rCost b, rCached tr, rCached b) of
          (Just p, Just actual, Just cachedT, billCached) ->
            let uncached = uncachedCost p tr
                discounted = expectedCost p tr
                billCache = fromMaybe 0 billCached
            in [mk Err CacheMiss (subject tr) "Cached prompt tokens appear to have been billed as uncached input." (Just actual) | cachedT > cTokenDelta cfg && (billCache + cTokenDelta cfg < cachedT || (pCached p < pIn p && actual + cCostDelta cfg >= uncached && actual > discounted + cCostDelta cfg))]
          _ -> []
    in dup ++ toks ++ costFind ++ cacheFind
  orphanFindings = [mk Err Orphan (subject b) "Billing row did not reconcile back to any trace." (rCost b) | b <- bills, not (Set.member (subject b) used)]
  subject r = fromMaybe (rSource r) (rReq r)
  same f a b = maybe True id ((==) <$> f a <*> f b)
  closeI a b = case (a,b) of (Just x, Just y) -> abs (x-y) <= cTokenDelta cfg; _ -> True
  tok tr label a b = case (a,b) of (Just x, Just y) | abs (x-y) > cTokenDelta cfg -> Just (mk Err TokenMismatch (subject tr) (label <> " differ: trace=" <> T.pack (show x) <> ", bill=" <> T.pack (show y) <> ".") Nothing) ; _ -> Nothing
  gap a b = abs (realToFrac (diffUTCTime a b))
  anchored a b = isJust (rInput a >>= const (rInput b)) || isJust (rOutput a >>= const (rOutput b)) || isJust (rTime a >>= const (rTime b))
  score tr b = (dist tr b, maybe 999999 id (gap <$> rTime tr <*> rTime b))
  dist a b = d rInput + d rCached + d rOutput where d f = maybe 0 abs ((-) <$> f a <*> f b)
  extra xs = let cs = mapMaybe rCost (drop 1 (sortOn subject xs)) in if null cs then Nothing else Just (sum cs)

expectedCost :: Price -> Rec -> Double
expectedCost p r = uncached * pIn p / 1e6 + cached * pCached p / 1e6 + out * pOut p / 1e6 where
  total = fromIntegral (fromMaybe 0 (rInput r)); cached = fromIntegral (max 0 (fromMaybe 0 (rCached r))); uncached = max 0 (total - cached); out = fromIntegral (fromMaybe 0 (rOutput r))

uncachedCost :: Price -> Rec -> Double
uncachedCost p r = total * pIn p / 1e6 + out * pOut p / 1e6 where
  total = fromIntegral (fromMaybe 0 (rInput r)); out = fromIntegral (fromMaybe 0 (rOutput r))

parseWarnings :: [Text] -> [Text] -> [Map Text Text] -> [Rec] -> [Map Text Text] -> [Rec] -> [Finding]
parseWarnings te be tr trs br brs =
  map (\e -> mk Warn ParseWarn "trace-parse" e Nothing) te
  ++ map (\e -> mk Warn ParseWarn "billing-parse" e Nothing) be
  ++ [mk Warn ParseWarn "trace-count" ("Ignored " <> T.pack (show (length tr - length trs)) <> " trace rows with no recognizable billing fields.") Nothing | length tr > length trs]
  ++ [mk Warn ParseWarn "billing-count" ("Ignored " <> T.pack (show (length br - length brs)) <> " billing rows with no recognizable billing fields.") Nothing | length br > length brs]

report :: Config -> Map Text Price -> [Rec] -> [Rec] -> [Finding] -> IO ()
report cfg prices traces bills fs = do
  putStrLn "InferenceInvoiceReconciler"
  putStrLn $ "Scanned " <> show (length traces) <> " trace records and " <> show (length bills) <> " billing records. Pricing cards: " <> show (Map.size prices) <> "."
  putStrLn $ "Time slop=" <> showFFloat (Just 0) (cTimeSlop cfg) "" <> "s token-delta=" <> show (cTokenDelta cfg) <> " cost-delta=" <> usd (cCostDelta cfg)
  let counts = Map.toList (Map.fromListWith (+) [(fKind f, 1 :: Int) | f <- fs])
  unless (null counts) $ putStrLn "Counts:" >> mapM_ (\(k,n) -> putStrLn ("  " <> show k <> ": " <> show n)) (sortOn fst counts)
  let billed = sum (mapMaybe rCost bills)
  when (any isJust (map rCost bills)) $ putStrLn ("Observed billed total: " <> usd billed)
  if null fs then putStrLn "No findings crossed the configured thresholds." else do
    putStrLn "Findings:"
    mapM_ showFinding (take 60 (sortOn (\f -> (fSev f, fKind f, fSubject f)) fs))

showFinding :: Finding -> IO ()
showFinding f = do
  putStrLn ("  [" <> show (fSev f) <> "] " <> show (fKind f) <> " " <> T.unpack (fSubject f) <> maybe "" ((" amount=" <>) . usd) (fAmount f))
  putStrLn ("      " <> T.unpack (fDetail f))

lookupAny :: [Text] -> Map Text Text -> Maybe Text
lookupAny ks m = listToMaybe (mapMaybe hit ks) where
  hit k = let n = normKey k; ex = Map.lookup n m; suf = [v | (fk,v) <- Map.toList m, fk == n || ("." <> n) `T.isSuffixOf` fk]
          in ex <|> case List.nub suf of [v] -> Just v; _ -> Nothing
lookupInt ks m = lookupAny ks m >>= readInt
lookupDouble ks m = lookupAny ks m >>= readD
lookupTime ks m = lookupAny ks m >>= readTime
readInt t = case (readMaybe (T.unpack (num t)) :: Maybe Integer) of Just n -> Just (fromIntegral n); Nothing -> round <$> (readMaybe (T.unpack (num t)) :: Maybe Double)
readD t = readMaybe (T.unpack (num t))
readTime t = case readMaybe (T.unpack (T.strip t)) :: Maybe Double of
  Just n | T.all (\c -> isDigit c || c == '.') (T.strip t) -> Just (posixSecondsToUTCTime (realToFrac (if n > 1000000000000 then n/1000 else n)))
  _ -> iso8601ParseM (T.unpack (T.strip t))
num = T.filter (\c -> isDigit c || c `elem` ("-+eE." :: String)) . T.replace "," "" . T.replace "$" "" . T.replace "USD" "" . T.replace "usd" ""
normKey = T.intercalate "." . filter (not . T.null) . map normSeg . T.split (`elem` ['.', '/'])
normSeg = squash . T.dropAround (== '_') . T.map (\c -> if isAlphaNum c then toLower c else '_')
squash = T.pack . reverse . snd . T.foldl' (\(u,acc) c -> if c == '_' && u then (True,acc) else (c=='_', c:acc)) (False, [])
joinKey = T.intercalate "." . filter (not . T.null)
clean = T.toLower . T.dropAround (\c -> c == '"' || c == '\'' || c == ' ')
splitCSV d = reverse . finish . T.foldl' step (T.empty, [], False) where
  step (cur, acc, q) c | c == '"' = (cur, acc, not q)
                       | c == d && not q = (T.empty, T.strip cur : acc, False)
                       | otherwise = (T.snoc cur c, acc, q)
  finish (cur, acc, _) = T.strip cur : acc
utf8 = TE.decodeUtf8With (\_ _ -> Just '?')
mk s k sub det amt = Finding s k sub det amt
usd x = "$" <> showFFloat (Just 6) x ""
tUsd = T.pack . usd

usage :: String
usage = unlines
  [ "Usage: InferenceInvoiceReconciler <traces> <billing> [options]"
  , "  --pricing <path>"
  , "  --time-slop-seconds <n>   Default 600"
  , "  --token-delta <n>         Default 32"
  , "  --cost-delta-usd <n>      Default 0.02"
  , "  --require-exact-request-id"
  ]

{-
This solves LLM billing reconciliation for real April 2026 engineering work where teams have provider invoice exports in one place and gateway or SDK traces somewhere else. Built because AI cost reviews usually fail on the ugly details: duplicate retry billing, cached prompt tokens charged like uncached input, orphaned invoice rows, and request ids that disappeared between layers.

Use it when you need a serious command line gate before finance close, before a model rollout, or before you trust a monthly spend dashboard. The trick: it accepts JSON, JSONL, CSV, and TSV, flattens nested fields, normalizes common token and request id aliases, matches invoices exactly when ids exist, and falls back to conservative fuzzy matching only when it has enough signal.

Drop this into any Haskell-capable repo when you want an AI invoice audit tool, OpenAI or Anthropic billing validator, prompt caching cost checker, or gateway reconciliation script that is still readable during an outage. I wrote it the way I would want to debug a spend spike myself: plain text output, hard failures for real mismatches, and enough detail to explain what was missing, duplicated, mismatched, or billed in a suspicious way.
-}
