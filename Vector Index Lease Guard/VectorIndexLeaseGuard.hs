module Main where

import Control.Exception (IOException, try)
import Control.Monad (unless, when)
import Data.Char (isAlphaNum, isDigit, isHexDigit, isSpace, ord, toLower)
import Data.List (foldl', intercalate, isInfixOf, nub, sortOn, stripPrefix)
import Data.Maybe (fromMaybe, isJust)
import System.Environment (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.IO (hPutStrLn, stderr)

data OutputFormat
  = FormatText
  | FormatJson
  | FormatSarif
  deriving (Eq, Show)

data Config = Config
  { cfgInput :: Maybe FilePath,
    cfgNow :: Maybe DayStamp,
    cfgFormat :: OutputFormat,
    cfgFailWarnings :: Bool,
    cfgSelfTest :: Bool,
    cfgHelp :: Bool
  }
  deriving (Show)

data DayStamp = DayStamp Int Int Int
  deriving (Eq, Ord)

instance Show DayStamp where
  show (DayStamp y m d) = pad 4 y ++ "-" ++ pad 2 m ++ "-" ++ pad 2 d

data Severity
  = SevError
  | SevWarning
  | SevInfo
  deriving (Eq, Show)

data Finding = Finding
  { fSeverity :: Severity,
    fLeaseId :: String,
    fLine :: Int,
    fCode :: String,
    fField :: String,
    fMessage :: String,
    fRepair :: String
  }
  deriving (Eq, Show)

data Lease = Lease
  { leaseLine :: Int,
    leaseFields :: [(String, String)]
  }
  deriving (Eq, Show)

data Analysis = Analysis
  { analysisChecked :: Int,
    analysisFindings :: [Finding]
  }
  deriving (Eq, Show)

main :: IO ()
main = do
  args <- getArgs
  case parseArgs args of
    Left err -> do
      hPutStrLn stderr err
      hPutStrLn stderr usage
      exitFailure
    Right cfg
      | cfgHelp cfg -> putStrLn usage
      | cfgSelfTest cfg -> runSelfTest
      | otherwise -> do
          loaded <- readInput cfg
          case loaded of
            Left err -> do
              hPutStrLn stderr err
              exitFailure
            Right raw -> do
              let result = analyze cfg raw
              putStrLn (renderAnalysis cfg result)
              if resultFails cfg result then exitFailure else exitSuccess

defaultConfig :: Config
defaultConfig =
  Config
    { cfgInput = Nothing,
      cfgNow = Nothing,
      cfgFormat = FormatText,
      cfgFailWarnings = False,
      cfgSelfTest = False,
      cfgHelp = False
    }

usage :: String
usage =
  unlines
    [ "VectorIndexLeaseGuard - fail closed on stale or unsafe RAG/vector index leases",
      "",
      "Usage:",
      "  runghc VectorIndexLeaseGuard.hs [--input FILE|-] [--now YYYY-MM-DD]",
      "                                [--format text|json|sarif] [--fail-warn]",
      "  runghc VectorIndexLeaseGuard.hs --self-test",
      "",
      "Input is one lease per line as space separated key=value pairs.",
      "Blank lines and # comments are ignored. Quote values that contain spaces.",
      "",
      "Required keys:",
      "  id tenant allowedTenants region allowedRegions",
      "  sourceHash indexedSourceHash embeddingModel modelDigest expectedModelDigest",
      "  createdAt expiresAt privacyClass maxPrivacyClass",
      "  retentionDays maxRetentionDays chunkTokens maxChunkTokens overlapTokens maxOverlapPct",
      "  license allowedLicenses schemaVersion indexerVersion",
      "",
      "Optional keys:",
      "  now sourceDeletedAt deletionRequested revocationGraceDays",
      "",
      "Example:",
      "  id=kb-prod tenant=acme allowedTenants=acme region=us-east-1 allowedRegions=us-east-1 \\",
      "  sourceHash=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \\",
      "  indexedSourceHash=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \\",
      "  embeddingModel=text-embedding-3-large@2026-04-01 modelDigest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \\",
      "  expectedModelDigest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \\",
      "  createdAt=2026-04-01 expiresAt=2026-05-01 privacyClass=pii-minimized \\",
      "  maxPrivacyClass=restricted retentionDays=30 maxRetentionDays=45 chunkTokens=820 \\",
      "  maxChunkTokens=1200 overlapTokens=80 maxOverlapPct=20 license=internal-ai-ok \\",
      "  allowedLicenses=internal-ai-ok schemaVersion=2 indexerVersion=rag-indexer@2026.04.0"
    ]

parseArgs :: [String] -> Either String Config
parseArgs = go defaultConfig
  where
    go cfg [] = Right cfg
    go cfg ("--help" : _) = Right cfg {cfgHelp = True}
    go cfg ("-h" : _) = Right cfg {cfgHelp = True}
    go cfg ("--self-test" : rest) = go cfg {cfgSelfTest = True} rest
    go cfg ("--fail-warn" : rest) = go cfg {cfgFailWarnings = True} rest
    go cfg ("--input" : filePath : rest) = go cfg {cfgInput = Just filePath} rest
    go _ ("--input" : []) = Left "--input requires a file path or -"
    go cfg ("--now" : rawDay : rest) =
      case parseDay rawDay of
        Left err -> Left ("invalid --now: " ++ err)
        Right day -> go cfg {cfgNow = Just day} rest
    go _ ("--now" : []) = Left "--now requires YYYY-MM-DD"
    go cfg ("--format" : rawFormat : rest) =
      case parseFormat rawFormat of
        Nothing -> Left ("unsupported --format: " ++ rawFormat)
        Just fmt -> go cfg {cfgFormat = fmt} rest
    go _ ("--format" : []) = Left "--format requires text, json, or sarif"
    go cfg (arg : rest)
      | Just filePath <- stripPrefix "--input=" arg = go cfg {cfgInput = Just filePath} rest
      | Just rawDay <- stripPrefix "--now=" arg =
          case parseDay rawDay of
            Left err -> Left ("invalid --now: " ++ err)
            Right day -> go cfg {cfgNow = Just day} rest
      | Just rawFormat <- stripPrefix "--format=" arg =
          case parseFormat rawFormat of
            Nothing -> Left ("unsupported --format: " ++ rawFormat)
            Just fmt -> go cfg {cfgFormat = fmt} rest
      | otherwise = Left ("unknown argument: " ++ arg)

parseFormat :: String -> Maybe OutputFormat
parseFormat raw =
  case lower raw of
    "text" -> Just FormatText
    "plain" -> Just FormatText
    "json" -> Just FormatJson
    "sarif" -> Just FormatSarif
    _ -> Nothing

readInput :: Config -> IO (Either String String)
readInput cfg =
  case cfgInput cfg of
    Nothing -> Right <$> getContents
    Just "-" -> Right <$> getContents
    Just filePath -> do
      outcome <- try (readFile filePath) :: IO (Either IOException String)
      pure $
        case outcome of
          Left err -> Left ("could not read input file " ++ show filePath ++ ": " ++ show err)
          Right raw -> Right raw

analyze :: Config -> String -> Analysis
analyze cfg raw =
  let (parseFindings, leases) = parseLeases raw
      localFindings = concatMap (validateLease cfg) leases
      globalFindings = duplicateIdFindings leases
      orderedFindings = sortOn findingSortKey (parseFindings ++ globalFindings ++ localFindings)
   in Analysis (length leases) orderedFindings

parseLeases :: String -> ([Finding], [Lease])
parseLeases raw =
  let step (findings, leases) (lineNumber, rawLine) =
        let cleaned = trim (stripComment rawLine)
         in if null cleaned
              then (findings, leases)
              else case parseLeaseLine lineNumber cleaned of
                Left finding -> (finding : findings, leases)
                Right lease -> (findings, lease : leases)
      (fs, ls) = foldl' step ([], []) (zip [1 ..] (lines raw))
   in (reverse fs, reverse ls)

parseLeaseLine :: Int -> String -> Either Finding Lease
parseLeaseLine lineNumber raw =
  case splitTokens raw of
    Left err -> Left (parseFinding lineNumber err)
    Right [] -> Left (parseFinding lineNumber "line did not contain any key=value pairs")
    Right tokens ->
      case traverse parseToken tokens of
        Left err -> Left (parseFinding lineNumber err)
        Right fields -> Right Lease {leaseLine = lineNumber, leaseFields = fields}

splitTokens :: String -> Either String [String]
splitTokens = go [] [] False False
  where
    go current tokens inQuote escaped [] =
      case (inQuote, current) of
        (True, _) -> Left "unterminated quoted value"
        (False, []) -> Right (reverse tokens)
        (False, _) -> Right (reverse (reverse current : tokens))
    go current tokens inQuote escaped (ch : rest)
      | escaped = go (ch : current) tokens inQuote False rest
      | inQuote && ch == '\\' = go current tokens inQuote True rest
      | ch == '"' = go current tokens (not inQuote) False rest
      | not inQuote && isSpace ch =
          if null current
            then go current tokens inQuote False rest
            else go [] (reverse current : tokens) inQuote False rest
      | otherwise = go (ch : current) tokens inQuote False rest

parseToken :: String -> Either String (String, String)
parseToken token =
  case break (== '=') token of
    ("", _) -> Left ("empty key in token " ++ show token)
    (_, "") -> Left ("missing = in token " ++ show token)
    (key, '=' : value)
      | null value -> Left ("empty value for key " ++ show key)
      | otherwise -> Right (normalizeKey key, value)
    _ -> Left ("invalid token " ++ show token)

parseFinding :: Int -> String -> Finding
parseFinding lineNumber message =
  Finding
    { fSeverity = SevError,
      fLeaseId = "<parse>",
      fLine = lineNumber,
      fCode = "parse_error",
      fField = "line",
      fMessage = message,
      fRepair = "Use one lease per line with strict key=value tokens. Quote values that contain spaces."
    }

stripComment :: String -> String
stripComment = go False False
  where
    go _ _ [] = []
    go inQuote escaped (ch : rest)
      | escaped = ch : go inQuote False rest
      | inQuote && ch == '\\' = ch : go inQuote True rest
      | ch == '"' = ch : go (not inQuote) False rest
      | not inQuote && ch == '#' = []
      | otherwise = ch : go inQuote False rest

validateLease :: Config -> Lease -> [Finding]
validateLease cfg lease =
  concat
    [ duplicateFieldFindings lease,
      requiredFieldFindings lease,
      hashFindings lease,
      modelFindings lease,
      dateFindings cfg lease,
      memberFindings lease "tenant" "allowedTenants" "tenant_boundary_violation" "Move the index under the right tenant or add an explicit tenant allowance.",
      memberFindings lease "region" "allowedRegions" "region_boundary_violation" "Rebuild the index in an approved region or update the lease with the approved data residency list.",
      privacyFindings lease,
      retentionFindings lease,
      chunkFindings lease,
      licenseFindings lease,
      schemaFindings lease,
      deletionFindings cfg lease
    ]

requiredFields :: [String]
requiredFields =
  [ "id",
    "tenant",
    "allowedTenants",
    "region",
    "allowedRegions",
    "sourceHash",
    "indexedSourceHash",
    "embeddingModel",
    "modelDigest",
    "expectedModelDigest",
    "createdAt",
    "expiresAt",
    "privacyClass",
    "maxPrivacyClass",
    "retentionDays",
    "maxRetentionDays",
    "chunkTokens",
    "maxChunkTokens",
    "overlapTokens",
    "maxOverlapPct",
    "license",
    "allowedLicenses",
    "schemaVersion",
    "indexerVersion"
  ]

requiredFieldFindings :: Lease -> [Finding]
requiredFieldFindings lease =
  [ mkFinding SevError lease "missing_field" field ("missing required field " ++ field) "Add the field to the lease manifest before promoting the vector index."
    | field <- requiredFields,
      not (hasField lease field)
  ]

duplicateFieldFindings :: Lease -> [Finding]
duplicateFieldFindings lease =
  [ mkFinding SevError lease "duplicate_field" key ("field " ++ key ++ " appears more than once after key normalization") "Keep exactly one value for the field so the promotion gate cannot read an ambiguous policy."
    | key <- duplicates (map fst (leaseFields lease))
  ]

duplicateIdFindings :: [Lease] -> [Finding]
duplicateIdFindings = go []
  where
    go _ [] = []
    go seen (lease : rest) =
      case fieldValue lease "id" of
        Nothing -> go seen rest
        Just ident
          | canonical ident `elem` seen ->
              mkFinding SevError lease "duplicate_lease_id" "id" ("lease id " ++ ident ++ " appears more than once") "Use stable unique ids so incident reports and SARIF results point to one index lease."
                : go seen rest
          | otherwise -> go (canonical ident : seen) rest

hashFindings :: Lease -> [Finding]
hashFindings lease =
  concat
    [ validateHash lease "sourceHash",
      validateHash lease "indexedSourceHash",
      matchFields lease "sourceHash" "indexedSourceHash" "stale_source_hash" "The indexed source hash does not match the current source hash." "Rebuild embeddings from the current source snapshot before serving this index."
    ]

modelFindings :: Lease -> [Finding]
modelFindings lease =
  concat
    [ validateHash lease "modelDigest",
      validateHash lease "expectedModelDigest",
      matchFields lease "modelDigest" "expectedModelDigest" "embedding_model_drift" "The embedding model digest differs from the expected promoted digest." "Re-embed with the approved model build or update the approval after eval signoff.",
      case fieldValue lease "embeddingModel" of
        Nothing -> []
        Just modelName
          | "@" `isInfixOf` modelName -> []
          | otherwise ->
              [ mkFinding SevWarning lease "unpinned_embedding_model" "embeddingModel" "embedding model name is not pinned with a version marker" "Use a reproducible name such as text-embedding-3-large@2026-04-01."
              ],
      case fieldValue lease "indexerVersion" of
        Nothing -> []
        Just version
          | "@" `isInfixOf` version || ":" `isInfixOf` version -> []
          | otherwise ->
              [ mkFinding SevWarning lease "unpinned_indexer_version" "indexerVersion" "indexerVersion is not tied to a build, commit, or release marker" "Pin the exact indexer build so a stale index can be reproduced during an incident."
              ]
    ]

dateFindings :: Config -> Lease -> [Finding]
dateFindings cfg lease =
  let nowResult = effectiveNow cfg lease
      createdResult = dateField lease "createdAt"
      expiresResult = dateField lease "expiresAt"
      invalids = resultFindings [nowResult, createdResult, expiresResult]
      comparisons =
        case (nowResult, createdResult, expiresResult) of
          (Right nowDay, Right createdDay, Right expiresDay) ->
            concat
              [ if createdDay > nowDay
                  then [mkFinding SevError lease "created_in_future" "createdAt" "lease creation date is after the evaluation date" "Check clock skew, the manifest date, or the --now value before promoting."]
                  else [],
                if expiresDay < nowDay
                  then [mkFinding SevError lease "expired_lease" "expiresAt" "lease is expired for the evaluation date" "Rebuild and reapprove the index lease with a fresh expiration before serving traffic."]
                  else [],
                if expiresDay < createdDay
                  then [mkFinding SevError lease "expiration_before_creation" "expiresAt" "lease expires before it was created" "Correct the createdAt and expiresAt fields before relying on the lease."]
                  else []
              ]
          _ -> []
   in invalids ++ comparisons

memberFindings :: Lease -> String -> String -> String -> String -> [Finding]
memberFindings lease valueField allowedField code repair =
  case (fieldValue lease valueField, fieldValue lease allowedField) of
    (Just value, Just allowedRaw) ->
      let allowed = map canonical (csvValues allowedRaw)
       in if "*" `elem` allowed || canonical value `elem` allowed
            then []
            else
              [ mkFinding SevError lease code valueField (valueField ++ " value " ++ show value ++ " is not in " ++ allowedField) repair
              ]
    _ -> []

privacyFindings :: Lease -> [Finding]
privacyFindings lease =
  case (fieldValue lease "privacyClass", fieldValue lease "maxPrivacyClass") of
    (Just actual, Just allowed) ->
      case (privacyRank actual, privacyRank allowed) of
        (Nothing, _) ->
          [mkFinding SevError lease "unknown_privacy_class" "privacyClass" ("unknown privacyClass " ++ show actual) privacyRepair]
        (_, Nothing) ->
          [mkFinding SevError lease "unknown_privacy_limit" "maxPrivacyClass" ("unknown maxPrivacyClass " ++ show allowed) privacyRepair]
        (Just actualRank, Just allowedRank)
          | actualRank <= allowedRank -> []
          | otherwise ->
              [mkFinding SevError lease "privacy_class_too_high" "privacyClass" "lease privacy class is stricter than the index approval allows" "Either lower the data class by redaction and minimization, or route this index through a stricter approval path."]
    _ -> []
  where
    privacyRepair = "Use one of public, none, internal, pii-minimized, restricted, or sensitive."

retentionFindings :: Lease -> [Finding]
retentionFindings lease =
  let retention = intField lease "retentionDays"
      maximumRetention = intField lease "maxRetentionDays"
      invalids = resultFindings [retention, maximumRetention]
      comparisons =
        case (retention, maximumRetention) of
          (Right days, Right maxDays) ->
            concat
              [ if days <= 0
                  then [mkFinding SevError lease "non_positive_retention" "retentionDays" "retentionDays must be positive" "Set a retention period that is explicit and greater than zero."]
                  else [],
                if maxDays <= 0
                  then [mkFinding SevError lease "non_positive_retention_limit" "maxRetentionDays" "maxRetentionDays must be positive" "Set a maximum retention period that is explicit and greater than zero."]
                  else [],
                if days > maxDays
                  then [mkFinding SevError lease "retention_exceeds_policy" "retentionDays" "lease retention exceeds the approved policy maximum" "Shorten the lease retention or get the data owner to approve a larger maximum."]
                  else []
              ]
          _ -> []
   in invalids ++ comparisons

chunkFindings :: Lease -> [Finding]
chunkFindings lease =
  let chunkTokens = intField lease "chunkTokens"
      maxChunkTokens = intField lease "maxChunkTokens"
      overlapTokens = intField lease "overlapTokens"
      maxOverlapPct = intField lease "maxOverlapPct"
      invalids = resultFindings [chunkTokens, maxChunkTokens, overlapTokens, maxOverlapPct]
      comparisons =
        case (chunkTokens, maxChunkTokens, overlapTokens, maxOverlapPct) of
          (Right chunk, Right maxChunk, Right overlap, Right maxPct) ->
            let overlapPct = if chunk <= 0 then 100 else (overlap * 100) `div` chunk
             in concat
                  [ if chunk <= 0
                      then [mkFinding SevError lease "non_positive_chunk" "chunkTokens" "chunkTokens must be positive" "Emit chunk token counts from the indexer before promotion."]
                      else [],
                    if maxChunk <= 0
                      then [mkFinding SevError lease "non_positive_chunk_limit" "maxChunkTokens" "maxChunkTokens must be positive" "Set the maximum chunk size from the retrieval policy."]
                      else [],
                    if overlap < 0
                      then [mkFinding SevError lease "negative_overlap" "overlapTokens" "overlapTokens cannot be negative" "Fix the chunker accounting before promotion."]
                      else [],
                    if chunk > maxChunk
                      then [mkFinding SevError lease "chunk_too_large" "chunkTokens" "chunk size exceeds the policy limit" "Rechunk the source so retrieval snippets stay inside the reviewed maximum."]
                      else [],
                    if overlap >= chunk
                      then [mkFinding SevError lease "overlap_not_smaller_than_chunk" "overlapTokens" "overlap must be smaller than the chunk size" "Fix chunker settings; equal or larger overlap repeats whole chunks and burns context."]
                      else [],
                    if overlapPct > maxPct
                      then [mkFinding SevWarning lease "overlap_ratio_high" "overlapTokens" ("overlap is " ++ show overlapPct ++ "% of the chunk") "Lower overlap or raise maxOverlapPct only after retrieval quality testing."]
                      else []
                  ]
          _ -> []
   in invalids ++ comparisons

licenseFindings :: Lease -> [Finding]
licenseFindings lease =
  case (fieldValue lease "license", fieldValue lease "allowedLicenses") of
    (Just licenseValue, Just allowedRaw) ->
      let allowed = map canonical (csvValues allowedRaw)
       in if "*" `elem` allowed || canonical licenseValue `elem` allowed
            then []
            else
              [ mkFinding SevError lease "license_not_allowed" "license" ("license " ++ show licenseValue ++ " is not approved for embedding") "Remove the source, add a legal approval, or use an index whose license list explicitly allows this data."
              ]
    _ -> []

schemaFindings :: Lease -> [Finding]
schemaFindings lease =
  case intField lease "schemaVersion" of
    Left finding
      | hasField lease "schemaVersion" -> [finding]
      | otherwise -> []
    Right version
      | version >= 2 -> []
      | otherwise ->
          [ mkFinding SevError lease "schema_version_too_old" "schemaVersion" "schemaVersion must be at least 2 for deletion and digest checks" "Regenerate the lease with the April 2026 schema before promotion."
          ]

deletionFindings :: Config -> Lease -> [Finding]
deletionFindings cfg lease =
  let nowResult = effectiveNow cfg lease
      deletedResult = maybe (Right Nothing) (fmap Just . parseLeaseDay lease "sourceDeletedAt") (fieldValue lease "sourceDeletedAt")
      graceResult =
        case fieldValue lease "revocationGraceDays" of
          Nothing -> Right 0
          Just _ -> intField lease "revocationGraceDays"
      requestedResult =
        case fieldValue lease "deletionRequested" of
          Nothing -> Right False
          Just raw -> parseBoolField lease "deletionRequested" raw
      invalids = resultFindings [fmap (const ()) nowResult, fmap (const ()) deletedResult, fmap (const ()) graceResult, fmap (const ()) requestedResult]
      requestFinding =
        case (requestedResult, deletedResult) of
          (Right True, Right Nothing) ->
            [ mkFinding SevError lease "deletion_requested_without_date" "sourceDeletedAt" "deletionRequested is true but sourceDeletedAt is missing" "Record the deletion request date so the revocation window is auditable."
            ]
          _ -> []
      deletedFindings =
        case (nowResult, deletedResult, graceResult) of
          (Right nowDay, Right (Just deletedDay), Right graceDays) ->
            let deadline = toOrdinal deletedDay + max 0 graceDays
                nowOrdinal = toOrdinal nowDay
             in if nowOrdinal > deadline
                  then
                    [ mkFinding SevError lease "deleted_source_indexed" "sourceDeletedAt" "source was deleted before the allowed revocation window ended" "Purge this index and rebuild without the deleted source before it can serve traffic."
                    ]
                  else
                    [ mkFinding SevWarning lease "revocation_grace_active" "sourceDeletedAt" "source has a deletion timestamp and is still inside the revocation grace window" "Schedule purge before the grace window expires and keep this warning visible in CI."
                    ]
          _ -> []
   in invalids ++ requestFinding ++ deletedFindings

validateHash :: Lease -> String -> [Finding]
validateHash lease field =
  case fieldValue lease field of
    Nothing -> []
    Just value
      | isSha256 value -> []
      | otherwise ->
          [ mkFinding SevError lease "invalid_sha256" field (field ++ " must use sha256:<64 lowercase-or-uppercase-hex>") "Write content-addressed sha256 digests so stale embeddings can be detected deterministically."
          ]

matchFields :: Lease -> String -> String -> String -> String -> String -> [Finding]
matchFields lease leftField rightField code message repair =
  case (fieldValue lease leftField, fieldValue lease rightField) of
    (Just leftValue, Just rightValue)
      | leftValue == rightValue -> []
      | otherwise -> [mkFinding SevError lease code rightField message repair]
    _ -> []

dateField :: Lease -> String -> Either Finding DayStamp
dateField lease field =
  case fieldValue lease field of
    Nothing -> Left (mkFinding SevError lease "missing_field" field ("missing required field " ++ field) "Add the date in YYYY-MM-DD form.")
    Just raw -> parseLeaseDay lease field raw

parseLeaseDay :: Lease -> String -> String -> Either Finding DayStamp
parseLeaseDay lease field raw =
  case parseDay raw of
    Left err -> Left (mkFinding SevError lease "invalid_date" field err "Use a normalized UTC calendar date such as 2026-04-30.")
    Right day -> Right day

effectiveNow :: Config -> Lease -> Either Finding DayStamp
effectiveNow cfg lease =
  case cfgNow cfg of
    Just day -> Right day
    Nothing ->
      case fieldValue lease "now" of
        Nothing ->
          Left
            ( mkFinding
                SevError
                lease
                "missing_now"
                "now"
                "no evaluation date was provided by --now or by a per-record now field"
                "Pass --now YYYY-MM-DD in CI so lease expiry and deletion checks are deterministic."
            )
        Just raw -> parseLeaseDay lease "now" raw

intField :: Lease -> String -> Either Finding Int
intField lease field =
  case fieldValue lease field of
    Nothing -> Left (mkFinding SevError lease "missing_field" field ("missing required field " ++ field) "Add a base-10 integer value.")
    Just raw ->
      case parseNonNegativeInt raw of
        Nothing -> Left (mkFinding SevError lease "invalid_integer" field (field ++ " must be a non-negative base-10 integer") "Write integer policy limits without units or decimal points.")
        Just number -> Right number

parseBoolField :: Lease -> String -> String -> Either Finding Bool
parseBoolField lease field raw =
  case lower raw of
    "true" -> Right True
    "false" -> Right False
    "yes" -> Right True
    "no" -> Right False
    "1" -> Right True
    "0" -> Right False
    _ -> Left (mkFinding SevError lease "invalid_boolean" field (field ++ " must be true or false") "Use true or false for boolean policy fields.")

resultFindings :: [Either Finding a] -> [Finding]
resultFindings = foldr collect []
  where
    collect (Left finding) acc = finding : acc
    collect (Right _) acc = acc

fieldValue :: Lease -> String -> Maybe String
fieldValue lease field = lookup (normalizeKey field) (leaseFields lease)

hasField :: Lease -> String -> Bool
hasField lease field = isJust (fieldValue lease field)

mkFinding :: Severity -> Lease -> String -> String -> String -> String -> Finding
mkFinding severity lease code field message repair =
  Finding
    { fSeverity = severity,
      fLeaseId = fromMaybe "<missing-id>" (fieldValue lease "id"),
      fLine = leaseLine lease,
      fCode = code,
      fField = field,
      fMessage = message,
      fRepair = repair
    }

findingSortKey :: Finding -> (Int, Int, String)
findingSortKey finding = (fLine finding, severityRank (fSeverity finding), fCode finding)

severityRank :: Severity -> Int
severityRank SevError = 0
severityRank SevWarning = 1
severityRank SevInfo = 2

severityLabel :: Severity -> String
severityLabel SevError = "error"
severityLabel SevWarning = "warning"
severityLabel SevInfo = "info"

severityShort :: Severity -> String
severityShort SevError = "E"
severityShort SevWarning = "W"
severityShort SevInfo = "I"

resultFails :: Config -> Analysis -> Bool
resultFails cfg result =
  any isBlocking (analysisFindings result)
  where
    isBlocking finding =
      fSeverity finding == SevError
        || (cfgFailWarnings cfg && fSeverity finding == SevWarning)

renderAnalysis :: Config -> Analysis -> String
renderAnalysis cfg result =
  case cfgFormat cfg of
    FormatText -> renderText cfg result
    FormatJson -> renderJson cfg result
    FormatSarif -> renderSarif cfg result

renderText :: Config -> Analysis -> String
renderText cfg result =
  let status = if resultFails cfg result then "FAIL" else "PASS"
      findings = analysisFindings result
      errorCount = countSeverity SevError findings
      warningCount = countSeverity SevWarning findings
      header =
        "VectorIndexLeaseGuard: "
          ++ status
          ++ " checked="
          ++ show (analysisChecked result)
          ++ " errors="
          ++ show errorCount
          ++ " warnings="
          ++ show warningCount
      body =
        if null findings
          then ["No findings. Every parsed vector index lease is fresh enough to promote."]
          else map renderFindingText findings
   in unlines (header : body)

renderFindingText :: Finding -> String
renderFindingText finding =
  "["
    ++ severityShort (fSeverity finding)
    ++ "] line "
    ++ show (fLine finding)
    ++ " lease="
    ++ fLeaseId finding
    ++ " code="
    ++ fCode finding
    ++ " field="
    ++ fField finding
    ++ " - "
    ++ fMessage finding
    ++ "\n    repair: "
    ++ fRepair finding

renderJson :: Config -> Analysis -> String
renderJson cfg result =
  jsonObject
    [ ("tool", jsonString "VectorIndexLeaseGuard"),
      ("status", jsonString (if resultFails cfg result then "fail" else "pass")),
      ("checked", show (analysisChecked result)),
      ("errors", show (countSeverity SevError findings)),
      ("warnings", show (countSeverity SevWarning findings)),
      ("failWarnings", jsonBool (cfgFailWarnings cfg)),
      ("findings", jsonArray (map findingJson findings))
    ]
  where
    findings = analysisFindings result

findingJson :: Finding -> String
findingJson finding =
  jsonObject
    [ ("severity", jsonString (severityLabel (fSeverity finding))),
      ("line", show (fLine finding)),
      ("leaseId", jsonString (fLeaseId finding)),
      ("code", jsonString (fCode finding)),
      ("field", jsonString (fField finding)),
      ("message", jsonString (fMessage finding)),
      ("repair", jsonString (fRepair finding))
    ]

renderSarif :: Config -> Analysis -> String
renderSarif cfg result =
  jsonObject
    [ ("version", jsonString "2.1.0"),
      ("$schema", jsonString "https://json.schemastore.org/sarif-2.1.0.json"),
      ( "runs",
        jsonArray
          [ jsonObject
              [ ( "tool",
                  jsonObject
                    [ ( "driver",
                        jsonObject
                          [ ("name", jsonString "VectorIndexLeaseGuard"),
                            ("informationUri", jsonString "https://github.com/kspavankrishna/VIBE-CODE"),
                            ("semanticVersion", jsonString "1.0.0")
                          ]
                      )
                    ]
                ),
                ("invocations", jsonArray [jsonObject [("executionSuccessful", jsonBool (not (resultFails cfg result)))]]),
                ("results", jsonArray (map sarifResult (analysisFindings result)))
              ]
          ]
      )
    ]

sarifResult :: Finding -> String
sarifResult finding =
  jsonObject
    [ ("ruleId", jsonString (fCode finding)),
      ("level", jsonString (sarifLevel (fSeverity finding))),
      ("message", jsonObject [("text", jsonString (fMessage finding))]),
      ( "locations",
        jsonArray
          [ jsonObject
              [ ( "physicalLocation",
                  jsonObject
                    [ ("artifactLocation", jsonObject [("uri", jsonString "vector-index-leases.txt")]),
                      ("region", jsonObject [("startLine", show (fLine finding))])
                    ]
                )
              ]
          ]
      ),
      ( "properties",
        jsonObject
          [ ("leaseId", jsonString (fLeaseId finding)),
            ("field", jsonString (fField finding)),
            ("repair", jsonString (fRepair finding))
          ]
      )
    ]

sarifLevel :: Severity -> String
sarifLevel SevError = "error"
sarifLevel SevWarning = "warning"
sarifLevel SevInfo = "note"

jsonObject :: [(String, String)] -> String
jsonObject pairs = "{" ++ intercalate "," [jsonString key ++ ":" ++ value | (key, value) <- pairs] ++ "}"

jsonArray :: [String] -> String
jsonArray values = "[" ++ intercalate "," values ++ "]"

jsonBool :: Bool -> String
jsonBool True = "true"
jsonBool False = "false"

jsonString :: String -> String
jsonString value = "\"" ++ concatMap escapeJson value ++ "\""

escapeJson :: Char -> String
escapeJson ch =
  case ch of
    '"' -> "\\\""
    '\\' -> "\\\\"
    '\n' -> "\\n"
    '\r' -> "\\r"
    '\t' -> "\\t"
    _ | ord ch < 32 -> "\\u" ++ hex4 (ord ch)
    _ -> [ch]

countSeverity :: Severity -> [Finding] -> Int
countSeverity severity = length . filter ((== severity) . fSeverity)

isSha256 :: String -> Bool
isSha256 value =
  case stripPrefix "sha256:" value of
    Just digest -> length digest == 64 && all isHexDigit digest
    Nothing -> False

privacyRank :: String -> Maybe Int
privacyRank raw =
  case canonical raw of
    "public" -> Just 0
    "none" -> Just 0
    "internal" -> Just 1
    "pii-minimized" -> Just 2
    "piiminimized" -> Just 2
    "restricted" -> Just 3
    "sensitive" -> Just 4
    _ -> Nothing

parseDay :: String -> Either String DayStamp
parseDay raw =
  let dayText = take 10 raw
   in if length dayText == 10
        && dayText !! 4 == '-'
        && dayText !! 7 == '-'
        && all isDigit (take 4 dayText ++ take 2 (drop 5 dayText) ++ drop 8 dayText)
        then
          let year = read (take 4 dayText)
              month = read (take 2 (drop 5 dayText))
              day = read (drop 8 dayText)
           in if validDate year month day
                then Right (DayStamp year month day)
                else Left ("invalid calendar date " ++ show dayText)
        else Left ("expected YYYY-MM-DD date, got " ++ show raw)

validDate :: Int -> Int -> Int -> Bool
validDate year month day =
  year >= 1970
    && month >= 1
    && month <= 12
    && day >= 1
    && day <= daysInMonth year month

daysInMonth :: Int -> Int -> Int
daysInMonth year month =
  case month of
    1 -> 31
    2 -> if isLeapYear year then 29 else 28
    3 -> 31
    4 -> 30
    5 -> 31
    6 -> 30
    7 -> 31
    8 -> 31
    9 -> 30
    10 -> 31
    11 -> 30
    12 -> 31
    _ -> 0

isLeapYear :: Int -> Bool
isLeapYear year =
  year `mod` 400 == 0 || (year `mod` 4 == 0 && year `mod` 100 /= 0)

toOrdinal :: DayStamp -> Int
toOrdinal (DayStamp year month day) =
  daysBeforeYear year + sum [daysInMonth year m | m <- [1 .. month - 1]] + day

daysBeforeYear :: Int -> Int
daysBeforeYear year =
  let previous = year - 1
   in 365 * previous + previous `div` 4 - previous `div` 100 + previous `div` 400

parseNonNegativeInt :: String -> Maybe Int
parseNonNegativeInt raw
  | null raw = Nothing
  | length raw > 9 = Nothing
  | all isDigit raw = Just (read raw)
  | otherwise = Nothing

csvValues :: String -> [String]
csvValues = filter (not . null) . map trim . splitOn ','

splitOn :: Char -> String -> [String]
splitOn delimiter = go []
  where
    go current [] = [reverse current]
    go current (ch : rest)
      | ch == delimiter = reverse current : go [] rest
      | otherwise = go (ch : current) rest

duplicates :: [String] -> [String]
duplicates values =
  [value | value <- nub values, length (filter (== value) values) > 1]

normalizeKey :: String -> String
normalizeKey = map toLower . filter isAlphaNum

canonical :: String -> String
canonical = map normalizeValueChar . trim
  where
    normalizeValueChar ch
      | ch == '_' || isSpace ch = '-'
      | otherwise = toLower ch

lower :: String -> String
lower = map toLower

trim :: String -> String
trim = dropWhileEnd isSpace . dropWhile isSpace

dropWhileEnd :: (a -> Bool) -> [a] -> [a]
dropWhileEnd predicate = reverse . dropWhile predicate . reverse

pad :: Int -> Int -> String
pad width number =
  let raw = show number
   in replicate (max 0 (width - length raw)) '0' ++ raw

hex4 :: Int -> String
hex4 number =
  [ hexDigit ((number `div` 4096) `mod` 16),
    hexDigit ((number `div` 256) `mod` 16),
    hexDigit ((number `div` 16) `mod` 16),
    hexDigit (number `mod` 16)
  ]

hexDigit :: Int -> Char
hexDigit number = "0123456789abcdef" !! number

runSelfTest :: IO ()
runSelfTest = do
  let cfg = defaultConfig {cfgNow = Just (DayStamp 2026 4 20)}
      good = analyze cfg sampleGood
      bad = analyze cfg sampleBad
      goodErrors = countSeverity SevError (analysisFindings good)
      badCodes = map fCode (analysisFindings bad)
      neededCodes =
        [ "stale_source_hash",
          "embedding_model_drift",
          "expired_lease",
          "tenant_boundary_violation",
          "region_boundary_violation",
          "license_not_allowed"
        ]
      missingCodes = [code | code <- neededCodes, code `notElem` badCodes]
  when (goodErrors /= 0) $ do
    hPutStrLn stderr "self-test failed: valid lease produced errors"
    putStrLn (renderText cfg good)
    exitFailure
  unless (null missingCodes) $ do
    hPutStrLn stderr ("self-test failed: missing expected finding codes " ++ intercalate "," missingCodes)
    putStrLn (renderText cfg bad)
    exitFailure
  putStrLn "self-test passed"
  exitSuccess

sampleGood :: String
sampleGood =
  unlines
    [ "id=kb-prod tenant=acme allowedTenants=acme,platform region=us-east-1 allowedRegions=us-east-1,us-west-2 sourceHash=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa indexedSourceHash=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa embeddingModel=text-embedding-3-large@2026-04-01 modelDigest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb expectedModelDigest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb createdAt=2026-04-01 expiresAt=2026-05-01 privacyClass=pii-minimized maxPrivacyClass=restricted retentionDays=30 maxRetentionDays=45 chunkTokens=820 maxChunkTokens=1200 overlapTokens=80 maxOverlapPct=20 license=internal-ai-ok allowedLicenses=internal-ai-ok,mit schemaVersion=2 indexerVersion=rag-indexer@2026.04.0"
    ]

sampleBad :: String
sampleBad =
  unlines
    [ "id=kb-prod tenant=wrong-tenant allowedTenants=acme region=eu-central-1 allowedRegions=us-east-1 sourceHash=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa indexedSourceHash=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc embeddingModel=text-embedding-3-large modelDigest=sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd expectedModelDigest=sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee createdAt=2026-03-01 expiresAt=2026-04-01 privacyClass=sensitive maxPrivacyClass=restricted retentionDays=90 maxRetentionDays=45 chunkTokens=1600 maxChunkTokens=1200 overlapTokens=700 maxOverlapPct=20 license=unknown-web allowedLicenses=internal-ai-ok schemaVersion=1 indexerVersion=dev sourceDeletedAt=2026-04-01 revocationGraceDays=3"
    ]

{-
This solves the April 2026 problem where a RAG or vector search index can look ready in CI while it is actually built from stale source text, the wrong embedding model, a revoked document, the wrong tenant, or a region that the data owner never approved. Built because Pavan would rather have a small deterministic promotion gate than debug hallucinated answers after a customer asks why deleted knowledge is still being retrieved. Use it when your AI tooling, developer productivity platform, research system, data pipeline, edge compute service, or DevOps workflow writes vector index lease manifests before production rollout. The trick: every lease is treated like a time boxed contract, not a friendly metadata note, so hashes, model digests, privacy class, license, retention, chunk size, overlap, tenant, region, and deletion grace are checked together with a hard failing exit code and machine readable JSON or SARIF. Drop this into GitHub Actions, Buildkite, Datadog CI, a Kubernetes admission job, or a predeploy script beside your embeddings job; searchers looking for stale RAG index guard, vector database lease validation, AI retrieval compliance, embedding model drift detection, deleted document purge check, data residency gate, and production RAG CI should find exactly what they need here.
-}
