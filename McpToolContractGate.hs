{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Applicative ((<|>))
import Control.Monad (unless)
import Data.Aeson (Object, Value (..), eitherDecode', encode, object, (.=))
import qualified Data.Aeson.Key as Key
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BL8
import qualified Data.List as List
import Data.List (foldl')
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import Data.Maybe (isJust, mapMaybe)
import Data.Ratio (denominator)
import Data.Scientific (Scientific)
import qualified Data.Scientific as Scientific
import System.Environment (getArgs)
import System.Exit (ExitCode (..), exitWith)
import System.IO (stderr, hPutStrLn)
import Text.Read (readMaybe)

data Command = CommandHelp | CommandRun Config

data Config = Config
  { cfgBeforePath :: FilePath
  , cfgAfterPath :: FilePath
  , cfgJsonOutput :: Bool
  , cfgStrictAmbiguous :: Bool
  , cfgMaxFindings :: Int
  , cfgToolFilters :: Set.Set T.Text
  }
  deriving (Show)

data Severity = Breaking | Warning
  deriving (Eq, Ord, Show)

data Finding = Finding
  { findingSeverity :: Severity
  , findingTool :: T.Text
  , findingPath :: T.Text
  , findingCode :: T.Text
  , findingMessage :: T.Text
  }
  deriving (Eq, Ord, Show)

data Tool = Tool
  { toolName :: T.Text
  , toolSchema :: Value
  , toolDoc :: SchemaDoc
  }
  deriving (Show)

data SchemaDoc = SchemaDoc
  { docRoot :: Value
  , docPointers :: Map.Map T.Text Value
  }
  deriving (Show)

data Report = Report
  { reportBeforeToolCount :: Int
  , reportAfterToolCount :: Int
  , reportAddedTools :: [T.Text]
  , reportRemovedTools :: [T.Text]
  , reportFindings :: [Finding]
  , reportBreakingCount :: Int
  , reportWarningCount :: Int
  , reportTruncated :: Bool
  }
  deriving (Show)

data JType = TyNull | TyBoolean | TyInteger | TyNumber | TyString | TyArray | TyObject
  deriving (Eq, Ord, Enum, Bounded, Show)

data ResolveResult = ResolveResult
  { rrValue :: Value
  , rrWarnings :: [T.Text]
  }
  deriving (Show)

data SchemaMode = ModeAllowAny | ModeRejectAll | ModeSchema Value
  deriving (Show)

data NumericBound = NumericBound
  { boundValue :: Scientific
  , boundExclusive :: Bool
  }
  deriving (Eq, Show)

defaultConfig :: Config
defaultConfig =
  Config
    { cfgBeforePath = ""
    , cfgAfterPath = ""
    , cfgJsonOutput = False
    , cfgStrictAmbiguous = False
    , cfgMaxFindings = 200
    , cfgToolFilters = Set.empty
    }

allTypes :: Set.Set JType
allTypes = Set.fromList [minBound .. maxBound]

annotationKeys :: Set.Set T.Text
annotationKeys =
  Set.fromList
    [ "$comment"
    , "$defs"
    , "$id"
    , "$schema"
    , "default"
    , "definitions"
    , "deprecated"
    , "description"
    , "example"
    , "examples"
    , "readOnly"
    , "title"
    , "writeOnly"
    ]

main :: IO ()
main = do
  args <- getArgs
  case parseArgs args of
    Left err -> do
      hPutStrLn stderr err
      hPutStrLn stderr usage
      exitWith (ExitFailure 64)
    Right CommandHelp -> do
      putStrLn usage
      exitWith ExitSuccess
    Right (CommandRun cfg) -> run cfg

run :: Config -> IO ()
run cfg = do
  beforeBytes <- readInput (cfgBeforePath cfg)
  afterBytes <- readInput (cfgAfterPath cfg)
  beforeValue <- decodeInput "before snapshot" beforeBytes
  afterValue <- decodeInput "after snapshot" afterBytes

  beforeTools <- either dieInput pure (extractTools beforeValue)
  afterTools <- either dieInput pure (extractTools afterValue)
  beforeMap0 <- either dieInput pure (buildToolMap beforeTools)
  afterMap0 <- either dieInput pure (buildToolMap afterTools)

  let beforeMap = applyToolFilter (cfgToolFilters cfg) beforeMap0
      afterMap = applyToolFilter (cfgToolFilters cfg) afterMap0
      matchedFilters = Map.keysSet beforeMap `Set.union` Map.keysSet afterMap
      missingFilters = cfgToolFilters cfg `Set.difference` matchedFilters

  unless (Set.null missingFilters) $ do
    hPutStrLn stderr ("requested tool names were not found in either snapshot: " <> T.unpack (renderTexts (Set.toAscList missingFilters)))
    exitWith (ExitFailure 1)

  let report = compareToolSets cfg beforeMap afterMap
  if cfgJsonOutput cfg
    then BL8.putStrLn (encode (reportToJson report))
    else renderReport report

  exitWith $ if reportBreakingCount report > 0 then ExitFailure 2 else ExitSuccess

parseArgs :: [String] -> Either String Command
parseArgs args
  | any (`elem` ["-h", "--help"]) args = Right CommandHelp
  | otherwise = finalize =<< go defaultConfig [] args
  where
    go :: Config -> [String] -> [String] -> Either String (Config, [String])
    go cfg pos [] = Right (cfg, pos)
    go cfg pos ("--json" : rest) = go cfg {cfgJsonOutput = True} pos rest
    go cfg pos ("--strict-ambiguous" : rest) = go cfg {cfgStrictAmbiguous = True} pos rest
    go cfg pos ("--max-findings" : value : rest) =
      case readMaybe value :: Maybe Int of
        Just n | n > 0 -> go cfg {cfgMaxFindings = n} pos rest
        _ -> Left "--max-findings expects a positive integer"
    go _ _ ["--max-findings"] = Left "--max-findings expects a value"
    go cfg pos ("--tool" : value : rest) = go cfg {cfgToolFilters = Set.insert (T.pack value) (cfgToolFilters cfg)} pos rest
    go _ _ ["--tool"] = Left "--tool expects a tool name"
    go cfg pos (arg : rest)
      | "--" `List.isPrefixOf` arg = Left ("unknown flag: " <> arg)
      | otherwise = go cfg (pos ++ [arg]) rest

    finalize (cfg, pos) =
      case pos of
        [beforePath, afterPath]
          | beforePath == "-" && afterPath == "-" -> Left "at most one input can be read from stdin"
          | otherwise -> Right (CommandRun cfg {cfgBeforePath = beforePath, cfgAfterPath = afterPath})
        _ -> Left "expected exactly two input paths: <before.json> <after.json>"

readInput :: FilePath -> IO BL.ByteString
readInput "-" = BL.getContents
readInput path = BL.readFile path

decodeInput :: String -> BL.ByteString -> IO Value
decodeInput label payload =
  case eitherDecode' payload of
    Left err -> do
      hPutStrLn stderr (label <> ": " <> err)
      exitWith (ExitFailure 1)
    Right value -> pure value

dieInput :: String -> IO a
dieInput message = do
  hPutStrLn stderr message
  exitWith (ExitFailure 1)

extractTools :: Value -> Either String [Tool]
extractTools value =
  case findToolArray 6 value of
    Nothing -> Left "could not find a tools array. Accepted shapes include a top-level array, {\"tools\": [...]}, or nested result/data wrappers."
    Just entries -> mapM extractTool entries

findToolArray :: Int -> Value -> Maybe [Value]
findToolArray depth value
  | depth < 0 = Nothing
  | otherwise =
      case value of
        Array arr
          | V.null arr -> Just []
          | V.all looksLikeToolCandidate arr -> Just (V.toList arr)
          | otherwise -> firstJust (map (findToolArray (depth - 1)) (V.toList arr))
        Object obj ->
          case lookupValue "tools" obj of
            Just (Array arr) -> Just (V.toList arr)
            _ -> firstJust (map (findToolArray (depth - 1) . snd) (KM.toList obj))
        _ -> Nothing

looksLikeToolCandidate :: Value -> Bool
looksLikeToolCandidate (Object obj) =
  isJust (lookupTextAny ["name", "toolName"] obj)
    || case lookupValue "function" obj of
      Just (Object fn) -> isJust (lookupTextAny ["name"] fn)
      _ -> False
looksLikeToolCandidate _ = False

extractTool :: Value -> Either String Tool
extractTool raw@(Object outer) =
  let inner =
        case lookupValue "function" outer of
          Just (Object fn) -> fn
          _ -> outer
      name = lookupTextAny ["name", "toolName"] inner <|> lookupTextAny ["name", "toolName"] outer
      schema =
        normalizeSchemaRoot
          ( fromMaybe
              (Bool True)
              ( firstJust
                  [ lookupValue "inputSchema" inner
                  , lookupValue "input_schema" inner
                  , lookupValue "parameters" inner
                  , lookupValue "schema" inner
                  , lookupValue "inputSchema" outer
                  , lookupValue "input_schema" outer
                  , lookupValue "parameters" outer
                  , lookupValue "schema" outer
                  ]
              )
          )
   in case name of
        Nothing -> Left ("tool entry is missing a name: " <> renderValue raw)
        Just toolNameText ->
          pure
            Tool
              { toolName = toolNameText
              , toolSchema = schema
              , toolDoc = mkSchemaDoc schema
              }
extractTool raw = Left ("tool entry is not an object: " <> renderValue raw)

normalizeSchemaRoot :: Value -> Value
normalizeSchemaRoot Null = Bool True
normalizeSchemaRoot value = value

buildToolMap :: [Tool] -> Either String (Map.Map T.Text Tool)
buildToolMap = go Map.empty
  where
    go acc [] = Right acc
    go acc (tool : rest)
      | Map.member (toolName tool) acc = Left ("duplicate tool name in snapshot: " <> T.unpack (toolName tool))
      | otherwise = go (Map.insert (toolName tool) tool acc) rest

applyToolFilter :: Set.Set T.Text -> Map.Map T.Text Tool -> Map.Map T.Text Tool
applyToolFilter filters
  | Set.null filters = id
  | otherwise = Map.filterWithKey (\name _ -> Set.member name filters)

compareToolSets :: Config -> Map.Map T.Text Tool -> Map.Map T.Text Tool -> Report
compareToolSets cfg before after =
  let beforeNames = Map.keysSet before
      afterNames = Map.keysSet after
      removed = Set.toAscList (beforeNames `Set.difference` afterNames)
      added = Set.toAscList (afterNames `Set.difference` beforeNames)
      removedFindings =
        [ breakingAt name "tool" "tool-removed" ("Tool \"" <> name <> "\" was removed from the snapshot.")
        | name <- removed
        ]
      sharedNames = Set.toAscList (beforeNames `Set.intersection` afterNames)
      sharedFindings =
        concatMap
          (\name ->
             let beforeTool = before Map.! name
                 afterTool = after Map.! name
              in compareSchemas cfg name "inputSchema" (toolDoc beforeTool) (toolDoc afterTool) (toolSchema beforeTool) (toolSchema afterTool)
          )
          sharedNames
      allFindings = sortFindings (dedupeFindings (removedFindings ++ sharedFindings))
      shownFindings = take (cfgMaxFindings cfg) allFindings
      breakingCount = length (filter ((== Breaking) . findingSeverity) allFindings)
      warningCount = length (filter ((== Warning) . findingSeverity) allFindings)
   in Report
        { reportBeforeToolCount = Map.size before
        , reportAfterToolCount = Map.size after
        , reportAddedTools = added
        , reportRemovedTools = removed
        , reportFindings = shownFindings
        , reportBreakingCount = breakingCount
        , reportWarningCount = warningCount
        , reportTruncated = length allFindings > cfgMaxFindings cfg
        }

reportToJson :: Report -> Value
reportToJson report =
  object
    [ "before_tool_count" .= reportBeforeToolCount report
    , "after_tool_count" .= reportAfterToolCount report
    , "added_tools" .= reportAddedTools report
    , "removed_tools" .= reportRemovedTools report
    , "breaking_count" .= reportBreakingCount report
    , "warning_count" .= reportWarningCount report
    , "truncated" .= reportTruncated report
    , "findings" .= map findingToJson (reportFindings report)
    ]

findingToJson :: Finding -> Value
findingToJson finding =
  object
    [ "severity" .= severityText (findingSeverity finding)
    , "tool" .= findingTool finding
    , "path" .= findingPath finding
    , "code" .= findingCode finding
    , "message" .= findingMessage finding
    ]

renderReport :: Report -> IO ()
renderReport report = do
  putStrLn "McpToolContractGate"
  putStrLn
    ( "Compared "
        <> show (reportBeforeToolCount report)
        <> " tools before and "
        <> show (reportAfterToolCount report)
        <> " tools after. Added "
        <> show (length (reportAddedTools report))
        <> ", removed "
        <> show (length (reportRemovedTools report))
        <> "."
    )
  putStrLn
    ( "Breaking findings: "
        <> show (reportBreakingCount report)
        <> ". Warning findings: "
        <> show (reportWarningCount report)
        <> "."
    )
  if null (reportFindings report)
    then putStrLn "No breaking or ambiguous contract drift was detected."
    else mapM_ printFinding (reportFindings report)
  whenTrue (reportTruncated report) $
    putStrLn "Output was truncated. Re-run with a larger --max-findings value to print the rest."

printFinding :: Finding -> IO ()
printFinding finding = do
  putStrLn
    ( "["
        <> T.unpack (severityText (findingSeverity finding))
        <> "] "
        <> T.unpack (findingTool finding)
        <> " "
        <> T.unpack (findingPath finding)
        <> " ("
        <> T.unpack (findingCode finding)
        <> ")"
    )
  putStrLn ("  " <> T.unpack (findingMessage finding))

compareSchemas :: Config -> T.Text -> T.Text -> SchemaDoc -> SchemaDoc -> Value -> Value -> [Finding]
compareSchemas cfg tool path beforeDoc afterDoc beforeSchema afterSchema =
  let ResolveResult beforeResolved beforeNotes = resolveValue beforeDoc beforeSchema
      ResolveResult afterResolved afterNotes = resolveValue afterDoc afterSchema
      refFindings =
        [ ambiguousAt cfg tool path "ref-resolution" note
        | note <- Set.toAscList (Set.fromList (beforeNotes ++ afterNotes))
        ]
   in refFindings ++ compareResolved cfg tool path beforeDoc afterDoc beforeResolved afterResolved

compareResolved :: Config -> T.Text -> T.Text -> SchemaDoc -> SchemaDoc -> Value -> Value -> [Finding]
compareResolved cfg tool path beforeDoc afterDoc beforeSchema afterSchema =
  case (beforeSchema, afterSchema) of
    (Bool False, Bool False) -> []
    (Bool False, _) -> []
    (_, Bool True) -> []
    (Bool True, Bool True) -> []
    (_, Bool False) ->
      [breakingAt tool path "schema-rejects-all" "The schema now rejects every input value."]
    (Bool True, _) ->
      [breakingAt tool path "schema-narrowed-from-any" "The schema changed from unconstrained input to constrained input."]
    (Object beforeObj, Object afterObj) ->
      compareObjectSchema cfg tool path beforeDoc afterDoc beforeObj afterObj
    (Object _, _) ->
      [ambiguousAt cfg tool path "schema-form-changed" "The schema changed from an object-based JSON Schema to a non-object form. Review this node manually."]
    (_, Object _) ->
      [ambiguousAt cfg tool path "schema-form-changed" "The schema changed to an object-based JSON Schema from a non-object form. Review this node manually."]
    _ -> []

compareObjectSchema :: Config -> T.Text -> T.Text -> SchemaDoc -> SchemaDoc -> Object -> Object -> [Finding]
compareObjectSchema cfg tool path beforeDoc afterDoc beforeObj afterObj =
  let beforeValue = Object beforeObj
      afterValue = Object afterObj
      beforeTypes = schemaTypes beforeValue
      afterTypes = schemaTypes afterValue
      literalFindings = compareLiteralConstraints tool path beforeValue afterValue
      typeFindings = compareTypeConstraints tool path beforeTypes afterTypes
      objectFindings =
        if Set.member TyObject beforeTypes && acceptsType afterTypes TyObject
          then compareObjectFacets cfg tool path beforeDoc afterDoc beforeObj afterObj
          else []
      arrayFindings =
        if Set.member TyArray beforeTypes && acceptsType afterTypes TyArray
          then compareArrayFacets cfg tool path beforeDoc afterDoc beforeObj afterObj
          else []
      stringFindings =
        if Set.member TyString beforeTypes && acceptsType afterTypes TyString
          then compareStringFacets cfg tool path beforeObj afterObj
          else []
      numericFindings =
        if numericTypesOverlap beforeTypes afterTypes
          then compareNumericFacets tool path beforeObj afterObj
          else []
      ambiguousFindings = compareAmbiguousKeys cfg tool path beforeObj afterObj
   in literalFindings ++ typeFindings ++ objectFindings ++ arrayFindings ++ stringFindings ++ numericFindings ++ ambiguousFindings

compareLiteralConstraints :: T.Text -> T.Text -> Value -> Value -> [Finding]
compareLiteralConstraints tool path beforeSchema afterSchema =
  case (literalValueSet beforeSchema, literalValueSet afterSchema) of
    (Nothing, Nothing) -> []
    (Nothing, Just _) ->
      [breakingAt tool (childPath path "enum") "literal-set-added" "The schema now restricts input to explicit literal values."]
    (Just _, Nothing) -> []
    (Just beforeValues, Just afterValues) ->
      let removed = Set.toAscList (beforeValues `Set.difference` afterValues)
       in if null removed
            then []
            else
              [breakingAt tool (childPath path "enum") "literal-values-removed" ("Explicit literal values were removed: " <> summarizeValues removed)]

compareTypeConstraints :: T.Text -> T.Text -> Set.Set JType -> Set.Set JType -> [Finding]
compareTypeConstraints tool path beforeTypes afterTypes =
  let removedTypes = filter (not . acceptsType afterTypes) (Set.toAscList beforeTypes)
   in if null removedTypes
        then []
        else
          [breakingAt tool (childPath path "type") "type-narrowed" ("The schema no longer accepts these JSON types: " <> renderTypes removedTypes)]

compareObjectFacets :: Config -> T.Text -> T.Text -> SchemaDoc -> SchemaDoc -> Object -> Object -> [Finding]
compareObjectFacets cfg tool path beforeDoc afterDoc beforeObj afterObj =
  let beforeRequired = requiredSet beforeObj
      afterRequired = requiredSet afterObj
      newRequired = Set.toAscList (afterRequired `Set.difference` beforeRequired)
      requiredFindings =
        if null newRequired
          then []
          else [breakingAt tool (childPath path "required") "required-properties-added" ("New required properties were added: " <> renderTexts newRequired)]
      minPropertyFindings = compareMinInt tool (childPath path "minProperties") "min-properties-increased" "minProperties" (lookupInt "minProperties" beforeObj) (lookupInt "minProperties" afterObj)
      maxPropertyFindings = compareMaxInt tool (childPath path "maxProperties") "max-properties-decreased" "maxProperties" (lookupInt "maxProperties" beforeObj) (lookupInt "maxProperties" afterObj)
      beforeAdditional = schemaModeFor beforeDoc (lookupValue "additionalProperties" beforeObj)
      afterAdditional = schemaModeFor afterDoc (lookupValue "additionalProperties" afterObj)
      additionalFindings =
        compareSchemaModeChange
          cfg
          tool
          (childPath path "additionalProperties")
          beforeDoc
          afterDoc
          "additional-properties-tightened"
          "additionalProperties became stricter, so unknown keys that were previously accepted may now fail."
          beforeAdditional
          afterAdditional
      beforePropertyNames = schemaModeFor beforeDoc (lookupValue "propertyNames" beforeObj)
      afterPropertyNames = schemaModeFor afterDoc (lookupValue "propertyNames" afterObj)
      propertyNameFindings =
        compareSchemaModeChange
          cfg
          tool
          (childPath path "propertyNames")
          beforeDoc
          afterDoc
          "property-names-tightened"
          "propertyNames became stricter, so object keys that were previously accepted may now fail."
          beforePropertyNames
          afterPropertyNames
      beforeProps = propertiesMap beforeObj
      afterProps = propertiesMap afterObj
      propertyFindings = concatMap (compareProperty beforeProps afterProps afterAdditional) (Map.toList beforeProps)
      dependencyFindings = compareDependentRequired tool path beforeObj afterObj
   in requiredFindings
        ++ minPropertyFindings
        ++ maxPropertyFindings
        ++ additionalFindings
        ++ propertyNameFindings
        ++ propertyFindings
        ++ dependencyFindings
  where
    compareProperty beforeProps afterProps afterAdditional (propName, beforePropSchema) =
      case Map.lookup propName afterProps of
        Just afterPropSchema -> compareSchemas cfg tool (propertyPath path propName) beforeDoc afterDoc beforePropSchema afterPropSchema
        Nothing ->
          case afterAdditional of
            ModeAllowAny -> []
            ModeRejectAll ->
              [ breakingAt
                  tool
                  (propertyPath path propName)
                  "property-removed"
                  ("Property \"" <> propName <> "\" was removed while additionalProperties rejects unknown keys, so older callers that still send it will fail.")
              ]
            ModeSchema schema ->
              compareSchemas cfg tool (childPath path ("additionalProperties[" <> propName <> "]")) beforeDoc afterDoc beforePropSchema schema

compareDependentRequired :: T.Text -> T.Text -> Object -> Object -> [Finding]
compareDependentRequired tool path beforeObj afterObj =
  let beforeDeps = dependentRequiredMap beforeObj
      afterDeps = dependentRequiredMap afterObj
   in concatMap compareEntry (Map.toList afterDeps)
  where
    compareEntry (propName, afterDepsForProp) =
      let beforeDepsForProp = Map.findWithDefault Set.empty propName beforeDeps
          newDependencies = Set.toAscList (afterDepsForProp `Set.difference` beforeDepsForProp)
       in if null newDependencies
            then []
            else
              [ breakingAt
                  tool
                  (childPath path ("dependentRequired[" <> propName <> "]"))
                  "dependent-required-added"
                  ("When \"" <> propName <> "\" is present, these extra properties are now required: " <> renderTexts newDependencies)
              ]

compareArrayFacets :: Config -> T.Text -> T.Text -> SchemaDoc -> SchemaDoc -> Object -> Object -> [Finding]
compareArrayFacets cfg tool path beforeDoc afterDoc beforeObj afterObj =
  let minItemFindings = compareMinInt tool (childPath path "minItems") "min-items-increased" "minItems" (lookupInt "minItems" beforeObj) (lookupInt "minItems" afterObj)
      maxItemFindings = compareMaxInt tool (childPath path "maxItems") "max-items-decreased" "maxItems" (lookupInt "maxItems" beforeObj) (lookupInt "maxItems" afterObj)
      beforeUnique = maybe False id (lookupBool "uniqueItems" beforeObj)
      afterUnique = maybe False id (lookupBool "uniqueItems" afterObj)
      uniqueItemFindings =
        if (not beforeUnique) && afterUnique
          then [breakingAt tool (childPath path "uniqueItems") "unique-items-added" "uniqueItems changed from false to true, so arrays with duplicates may now fail."]
          else []
      beforeItems = schemaModeFor beforeDoc (lookupValue "items" beforeObj)
      afterItems = schemaModeFor afterDoc (lookupValue "items" afterObj)
      itemFindings =
        compareSchemaModeChange
          cfg
          tool
          (childPath path "items")
          beforeDoc
          afterDoc
          "items-tightened"
          "items became stricter, so array elements that were previously accepted may now fail."
          beforeItems
          afterItems
      prefixFindings = comparePrefixItems cfg tool path beforeDoc afterDoc beforeObj afterObj afterItems
      containsFindings = compareContains cfg tool path beforeDoc afterDoc beforeObj afterObj
   in minItemFindings ++ maxItemFindings ++ uniqueItemFindings ++ itemFindings ++ prefixFindings ++ containsFindings

comparePrefixItems :: Config -> T.Text -> T.Text -> SchemaDoc -> SchemaDoc -> Object -> Object -> SchemaMode -> [Finding]
comparePrefixItems cfg tool path beforeDoc afterDoc beforeObj afterObj afterItems =
  let beforePrefix = maybe [] V.toList (lookupArray "prefixItems" beforeObj)
      afterPrefix = maybe [] V.toList (lookupArray "prefixItems" afterObj)
      sharedCount = min (length beforePrefix) (length afterPrefix)
      sharedFindings =
        [ compareSchemas cfg tool (indexPath (childPath path "prefixItems") idx) beforeDoc afterDoc (beforePrefix !! idx) (afterPrefix !! idx)
        | idx <- [0 .. sharedCount - 1]
        ]
      trailingBefore = drop sharedCount beforePrefix
      trailingFindings =
        concat
          [ case afterItems of
              ModeAllowAny -> []
              ModeRejectAll ->
                [breakingAt tool (indexPath (childPath path "prefixItems") (sharedCount + idx)) "tuple-slot-removed" "A previously accepted tuple position is now rejected because items is false."]
              ModeSchema itemSchema ->
                compareSchemas cfg tool (indexPath (childPath path "items") (sharedCount + idx)) beforeDoc afterDoc oldSchema itemSchema
          | (idx, oldSchema) <- zip [0 ..] trailingBefore
          ]
   in concat sharedFindings ++ trailingFindings

compareContains :: Config -> T.Text -> T.Text -> SchemaDoc -> SchemaDoc -> Object -> Object -> [Finding]
compareContains cfg tool path beforeDoc afterDoc beforeObj afterObj =
  case (lookupValue "contains" beforeObj, lookupValue "contains" afterObj) of
    (Nothing, Nothing) -> []
    (Nothing, Just _) ->
      [breakingAt tool (childPath path "contains") "contains-added" "A contains constraint was added, so arrays may now need at least one matching element."]
    (Just _, Nothing) -> []
    (Just beforeContains, Just afterContains) ->
      compareSchemas cfg tool (childPath path "contains") beforeDoc afterDoc beforeContains afterContains
        ++ compareMinInt
          tool
          (childPath path "minContains")
          "min-contains-increased"
          "minContains"
          (Just (containsMin beforeObj))
          (Just (containsMin afterObj))
        ++ compareMaxInt
          tool
          (childPath path "maxContains")
          "max-contains-decreased"
          "maxContains"
          (lookupInt "maxContains" beforeObj)
          (lookupInt "maxContains" afterObj)

compareStringFacets :: Config -> T.Text -> T.Text -> Object -> Object -> [Finding]
compareStringFacets cfg tool path beforeObj afterObj =
  compareMinInt tool (childPath path "minLength") "min-length-increased" "minLength" (lookupInt "minLength" beforeObj) (lookupInt "minLength" afterObj)
    ++ compareMaxInt tool (childPath path "maxLength") "max-length-decreased" "maxLength" (lookupInt "maxLength" beforeObj) (lookupInt "maxLength" afterObj)
    ++ compareTextConstraint cfg tool (childPath path "pattern") "pattern" (lookupText "pattern" beforeObj) (lookupText "pattern" afterObj)
    ++ compareTextConstraint cfg tool (childPath path "format") "format" (lookupText "format" beforeObj) (lookupText "format" afterObj)
    ++ compareTextConstraint cfg tool (childPath path "contentEncoding") "contentEncoding" (lookupText "contentEncoding" beforeObj) (lookupText "contentEncoding" afterObj)
    ++ compareTextConstraint cfg tool (childPath path "contentMediaType") "contentMediaType" (lookupText "contentMediaType" beforeObj) (lookupText "contentMediaType" afterObj)

compareNumericFacets :: T.Text -> T.Text -> Object -> Object -> [Finding]
compareNumericFacets tool path beforeObj afterObj =
  lowerBoundFindings ++ upperBoundFindings ++ multipleOfFindings
  where
    lowerBoundFindings =
      case (lowerBound beforeObj, lowerBound afterObj) of
        (_, Nothing) -> []
        (Nothing, Just newBound) ->
          [breakingAt tool (childPath path "minimum") "minimum-tightened" ("A lower numeric bound was added at " <> renderBound newBound <> ".")]
        (Just oldBound, Just newBound)
          | stricterLowerBound oldBound newBound ->
              [breakingAt tool (childPath path "minimum") "minimum-tightened" ("The lower numeric bound became stricter from " <> renderBound oldBound <> " to " <> renderBound newBound <> ".")]
          | otherwise -> []
    upperBoundFindings =
      case (upperBound beforeObj, upperBound afterObj) of
        (_, Nothing) -> []
        (Nothing, Just newBound) ->
          [breakingAt tool (childPath path "maximum") "maximum-tightened" ("An upper numeric bound was added at " <> renderBound newBound <> ".")]
        (Just oldBound, Just newBound)
          | stricterUpperBound oldBound newBound ->
              [breakingAt tool (childPath path "maximum") "maximum-tightened" ("The upper numeric bound became stricter from " <> renderBound oldBound <> " to " <> renderBound newBound <> ".")]
          | otherwise -> []
    multipleOfFindings =
      case (lookupScientific "multipleOf" beforeObj, lookupScientific "multipleOf" afterObj) of
        (_, Nothing) -> []
        (Nothing, Just newMultiple) ->
          [breakingAt tool (childPath path "multipleOf") "multiple-of-added" ("multipleOf was added at " <> renderScientific newMultiple <> ".")]
        (Just oldMultiple, Just newMultiple)
          | oldMultiple == newMultiple -> []
          | multipleOfCompatible oldMultiple newMultiple -> []
          | otherwise ->
              [breakingAt tool (childPath path "multipleOf") "multiple-of-tightened" ("multipleOf changed from " <> renderScientific oldMultiple <> " to " <> renderScientific newMultiple <> ".")]

compareAmbiguousKeys :: Config -> T.Text -> T.Text -> Object -> Object -> [Finding]
compareAmbiguousKeys cfg tool path beforeObj afterObj =
  concatMap compareKey keys
  where
    keys = ["allOf", "anyOf", "oneOf", "not", "if", "then", "else", "patternProperties", "dependentSchemas", "unevaluatedItems", "unevaluatedProperties"]
    compareKey key =
      case (lookupValue key beforeObj, lookupValue key afterObj) of
        (Nothing, Nothing) -> []
        (Nothing, Just _) -> [ambiguousAt cfg tool (childPath path key) "complex-keyword-added" (key <> " was added. Review this schema branch manually.")]
        (Just _, Nothing) -> []
        (Just oldValue, Just newValue)
          | oldValue == newValue -> []
          | otherwise -> [ambiguousAt cfg tool (childPath path key) "complex-keyword-changed" (key <> " changed. Review this schema branch manually.")]

compareSchemaModeChange :: Config -> T.Text -> T.Text -> SchemaDoc -> SchemaDoc -> T.Text -> T.Text -> SchemaMode -> SchemaMode -> [Finding]
compareSchemaModeChange cfg tool path beforeDoc afterDoc code message beforeMode afterMode =
  case (beforeMode, afterMode) of
    (ModeAllowAny, ModeAllowAny) -> []
    (ModeAllowAny, ModeRejectAll) -> [breakingAt tool path code message]
    (ModeAllowAny, ModeSchema _) -> [breakingAt tool path code message]
    (ModeRejectAll, _) -> []
    (ModeSchema _, ModeAllowAny) -> []
    (ModeSchema _, ModeRejectAll) -> [breakingAt tool path code message]
    (ModeSchema beforeSchema, ModeSchema afterSchema) -> compareSchemas cfg tool path beforeDoc afterDoc beforeSchema afterSchema

schemaTypes :: Value -> Set.Set JType
schemaTypes (Bool True) = allTypes
schemaTypes (Bool False) = Set.empty
schemaTypes value@(Object obj) =
  let baseTypes =
        case explicitTypes obj of
          Just declared | not (Set.null declared) -> declared
          _ -> inferredTypes value
      nullable = maybe False id (lookupBool "nullable" obj)
   in if nullable then Set.insert TyNull baseTypes else baseTypes
schemaTypes _ = allTypes

explicitTypes :: Object -> Maybe (Set.Set JType)
explicitTypes obj =
  case lookupValue "type" obj of
    Just (String typeName) -> Just (Set.singleton (parseTypeName typeName))
    Just (Array arr) ->
      let parsed = Set.fromList (mapMaybe valueToType (V.toList arr))
       in Just parsed
    _ -> Nothing
  where
    valueToType (String typeName) = Just (parseTypeName typeName)
    valueToType _ = Nothing

inferredTypes :: Value -> Set.Set JType
inferredTypes value@(Object obj) =
  let objectSignals = hasAnyKey obj ["properties", "required", "additionalProperties", "minProperties", "maxProperties", "propertyNames", "patternProperties", "dependentRequired", "dependentSchemas", "unevaluatedProperties"]
      arraySignals = hasAnyKey obj ["items", "prefixItems", "contains", "minItems", "maxItems", "uniqueItems", "unevaluatedItems"]
      stringSignals = hasAnyKey obj ["minLength", "maxLength", "pattern", "format", "contentEncoding", "contentMediaType"]
      numberSignals = hasAnyKey obj ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"]
      literalSignals = literalTypes value
      inferred =
        Set.unions
          [ if objectSignals then Set.singleton TyObject else Set.empty
          , if arraySignals then Set.singleton TyArray else Set.empty
          , if stringSignals then Set.singleton TyString else Set.empty
          , if numberSignals then Set.singleton TyNumber else Set.empty
          , literalSignals
          ]
   in if Set.null inferred then allTypes else inferred
inferredTypes _ = allTypes

parseTypeName :: T.Text -> JType
parseTypeName typeName =
  case T.toLower typeName of
    "null" -> TyNull
    "boolean" -> TyBoolean
    "integer" -> TyInteger
    "number" -> TyNumber
    "string" -> TyString
    "array" -> TyArray
    "object" -> TyObject
    _ -> TyObject

literalTypes :: Value -> Set.Set JType
literalTypes value =
  case literalValues value of
    Nothing -> Set.empty
    Just values -> Set.fromList (mapMaybe literalType values)

literalValues :: Value -> Maybe [Value]
literalValues (Object obj) =
  case lookupValue "const" obj of
    Just constValue -> Just [constValue]
    Nothing ->
      case lookupValue "enum" obj of
        Just (Array arr) -> Just (V.toList arr)
        _ -> Nothing
literalValues _ = Nothing

literalValueSet :: Value -> Maybe (Set.Set T.Text)
literalValueSet value = fmap (Set.fromList . map renderValue) (literalValues value)

literalType :: Value -> Maybe JType
literalType Null = Just TyNull
literalType (Bool _) = Just TyBoolean
literalType (String _) = Just TyString
literalType (Array _) = Just TyArray
literalType (Object _) = Just TyObject
literalType (Number n)
  | scientificIsInteger n = Just TyInteger
  | otherwise = Just TyNumber

acceptsType :: Set.Set JType -> JType -> Bool
acceptsType types TyInteger = Set.member TyInteger types || Set.member TyNumber types
acceptsType types ty = Set.member ty types

numericTypesOverlap :: Set.Set JType -> Set.Set JType -> Bool
numericTypesOverlap beforeTypes afterTypes =
  (Set.member TyInteger beforeTypes && acceptsType afterTypes TyInteger)
    || (Set.member TyNumber beforeTypes && acceptsType afterTypes TyNumber)

requiredSet :: Object -> Set.Set T.Text
requiredSet obj =
  case lookupArray "required" obj of
    Nothing -> Set.empty
    Just arr -> Set.fromList [text | String text <- V.toList arr]

propertiesMap :: Object -> Map.Map T.Text Value
propertiesMap obj =
  case lookupObject "properties" obj of
    Nothing -> Map.empty
    Just props -> Map.fromList [(Key.toText key, value) | (key, value) <- KM.toList props]

dependentRequiredMap :: Object -> Map.Map T.Text (Set.Set T.Text)
dependentRequiredMap obj =
  case lookupObject "dependentRequired" obj of
    Nothing -> Map.empty
    Just deps ->
      Map.fromList
        [ (Key.toText key, Set.fromList [dep | String dep <- V.toList arr])
        | (key, Array arr) <- KM.toList deps
        ]

schemaModeFor :: SchemaDoc -> Maybe Value -> SchemaMode
schemaModeFor _ Nothing = ModeAllowAny
schemaModeFor doc (Just rawSchema) =
  let ResolveResult resolved _ = resolveValue doc rawSchema
   in if isRejectAllSchema resolved
        then ModeRejectAll
        else
          if isAcceptAllSchema resolved
            then ModeAllowAny
            else ModeSchema resolved

isRejectAllSchema :: Value -> Bool
isRejectAllSchema (Bool False) = True
isRejectAllSchema _ = False

isAcceptAllSchema :: Value -> Bool
isAcceptAllSchema (Bool True) = True
isAcceptAllSchema (Object obj) =
  let keys = map Key.toText (KM.keys obj)
   in all isAnnotationKey keys
isAcceptAllSchema _ = False

isAnnotationKey :: T.Text -> Bool
isAnnotationKey key = Set.member key annotationKeys || "x-" `T.isPrefixOf` key

resolveValue :: SchemaDoc -> Value -> ResolveResult
resolveValue doc = go Set.empty 0
  where
    go seen depth value@(Object obj)
      | Just (String ref) <- lookupValue "$ref" obj =
          if depth >= 16
            then ResolveResult value ["Stopped resolving refs after 16 hops at " <> ref <> "."]
            else
              if Set.member ref seen
                then ResolveResult value ["Detected a local ref cycle at " <> ref <> "."]
                else
                  case resolveLocalRef doc ref of
                    Nothing -> ResolveResult value ["Could not resolve local ref " <> ref <> "."]
                    Just target ->
                      let local = Object (deleteValue "$ref" obj)
                          (merged, mergeWarnings) = mergeRefTarget target local
                          ResolveResult finalValue nestedWarnings = go (Set.insert ref seen) (depth + 1) merged
                       in ResolveResult finalValue (mergeWarnings ++ nestedWarnings)
    go _ _ value = ResolveResult value []

resolveLocalRef :: SchemaDoc -> T.Text -> Maybe Value
resolveLocalRef doc ref
  | ref == "#" = Just (docRoot doc)
  | "#/" `T.isPrefixOf` ref = Map.lookup (T.drop 1 ref) (docPointers doc)
  | otherwise = Nothing

mergeRefTarget :: Value -> Value -> (Value, [T.Text])
mergeRefTarget target (Object local)
  | KM.null local = (target, [])
  | otherwise =
      case target of
        Object targetObj -> (Object (KM.union local targetObj), [])
        _ -> (target, ["Ignored sibling keywords next to a non-object $ref target."])
mergeRefTarget target _ = (target, ["Ignored an unexpected schema fragment next to a $ref."])

mkSchemaDoc :: Value -> SchemaDoc
mkSchemaDoc root = SchemaDoc root (indexPointers root)

indexPointers :: Value -> Map.Map T.Text Value
indexPointers = go "" Map.empty
  where
    go pointer acc value =
      let acc' = Map.insert pointer value acc
       in case value of
            Object obj ->
              foldl'
                (\current (key, child) -> go (pointer <> "/" <> escapePointerSegment (Key.toText key)) current child)
                acc'
                (KM.toList obj)
            Array arr ->
              V.ifoldl'
                (\current idx child -> go (pointer <> "/" <> T.pack (show idx)) current child)
                acc'
                arr
            _ -> acc'

escapePointerSegment :: T.Text -> T.Text
escapePointerSegment = T.replace "/" "~1" . T.replace "~" "~0"

lookupValue :: T.Text -> Object -> Maybe Value
lookupValue key obj = KM.lookup (Key.fromText key) obj

deleteValue :: T.Text -> Object -> Object
deleteValue key obj = KM.delete (Key.fromText key) obj

lookupObject :: T.Text -> Object -> Maybe Object
lookupObject key obj =
  case lookupValue key obj of
    Just (Object value) -> Just value
    _ -> Nothing

lookupArray :: T.Text -> Object -> Maybe (V.Vector Value)
lookupArray key obj =
  case lookupValue key obj of
    Just (Array value) -> Just value
    _ -> Nothing

lookupText :: T.Text -> Object -> Maybe T.Text
lookupText key obj =
  case lookupValue key obj of
    Just (String value) -> Just value
    _ -> Nothing

lookupTextAny :: [T.Text] -> Object -> Maybe T.Text
lookupTextAny keys obj = firstJust (map (`lookupText` obj) keys)

lookupBool :: T.Text -> Object -> Maybe Bool
lookupBool key obj =
  case lookupValue key obj of
    Just (Bool value) -> Just value
    _ -> Nothing

lookupScientific :: T.Text -> Object -> Maybe Scientific
lookupScientific key obj =
  case lookupValue key obj of
    Just (Number value) -> Just value
    _ -> Nothing

lookupInt :: T.Text -> Object -> Maybe Int
lookupInt key obj = lookupScientific key obj >>= Scientific.toBoundedInteger

hasAnyKey :: Object -> [T.Text] -> Bool
hasAnyKey obj = any (\key -> isJust (lookupValue key obj))

lowerBound :: Object -> Maybe NumericBound
lowerBound obj =
  case lookupScientific "exclusiveMinimum" obj of
    Just value -> Just (NumericBound value True)
    Nothing -> fmap (`NumericBound` False) (lookupScientific "minimum" obj)

upperBound :: Object -> Maybe NumericBound
upperBound obj =
  case lookupScientific "exclusiveMaximum" obj of
    Just value -> Just (NumericBound value True)
    Nothing -> fmap (`NumericBound` False) (lookupScientific "maximum" obj)

stricterLowerBound :: NumericBound -> NumericBound -> Bool
stricterLowerBound oldBound newBound =
  case compare (boundValue newBound) (boundValue oldBound) of
    GT -> True
    LT -> False
    EQ -> boundExclusive newBound && not (boundExclusive oldBound)

stricterUpperBound :: NumericBound -> NumericBound -> Bool
stricterUpperBound oldBound newBound =
  case compare (boundValue newBound) (boundValue oldBound) of
    LT -> True
    GT -> False
    EQ -> boundExclusive newBound && not (boundExclusive oldBound)

containsMin :: Object -> Int
containsMin obj =
  case lookupValue "contains" obj of
    Nothing -> 0
    Just _ -> maybe 1 id (lookupInt "minContains" obj)

multipleOfCompatible :: Scientific -> Scientific -> Bool
multipleOfCompatible oldMultiple newMultiple
  | newMultiple == 0 = False
  | otherwise = denominator (toRational oldMultiple / toRational newMultiple) == 1

compareMinInt :: T.Text -> T.Text -> T.Text -> T.Text -> Maybe Int -> Maybe Int -> [Finding]
compareMinInt tool path code label beforeValue afterValue =
  case (beforeValue, afterValue) of
    (_, Nothing) -> []
    (Nothing, Just newValue)
      | newValue <= 0 -> []
      | otherwise -> [breakingAt tool path code (label <> " was added at " <> tshow newValue <> ".")]
    (Just oldValue, Just newValue)
      | newValue > oldValue -> [breakingAt tool path code (label <> " increased from " <> tshow oldValue <> " to " <> tshow newValue <> ".")]
      | otherwise -> []

compareMaxInt :: T.Text -> T.Text -> T.Text -> T.Text -> Maybe Int -> Maybe Int -> [Finding]
compareMaxInt tool path code label beforeValue afterValue =
  case (beforeValue, afterValue) of
    (_, Nothing) -> []
    (Nothing, Just newValue) -> [breakingAt tool path code (label <> " was added at " <> tshow newValue <> ".")]
    (Just oldValue, Just newValue)
      | newValue < oldValue -> [breakingAt tool path code (label <> " decreased from " <> tshow oldValue <> " to " <> tshow newValue <> ".")]
      | otherwise -> []

compareTextConstraint :: Config -> T.Text -> T.Text -> T.Text -> Maybe T.Text -> Maybe T.Text -> [Finding]
compareTextConstraint cfg tool path label beforeValue afterValue =
  case (beforeValue, afterValue) of
    (Nothing, Nothing) -> []
    (Nothing, Just newValue) -> [breakingAt tool path (T.toLower label <> "-added") (label <> " was added: " <> newValue <> ".")]
    (Just _, Nothing) -> []
    (Just oldValue, Just newValue)
      | oldValue == newValue -> []
      | otherwise -> [ambiguousAt cfg tool path (T.toLower label <> "-changed") (label <> " changed from " <> oldValue <> " to " <> newValue <> ".")]

breakingAt :: T.Text -> T.Text -> T.Text -> T.Text -> Finding
breakingAt tool path code message = Finding Breaking tool path code message

ambiguousAt :: Config -> T.Text -> T.Text -> T.Text -> T.Text -> Finding
ambiguousAt cfg tool path code message = Finding severity tool path code message
  where
    severity = if cfgStrictAmbiguous cfg then Breaking else Warning

severityText :: Severity -> T.Text
severityText Breaking = "breaking"
severityText Warning = "warning"

propertyPath :: T.Text -> T.Text -> T.Text
propertyPath base name = childPath base ("properties[" <> name <> "]")

childPath :: T.Text -> T.Text -> T.Text
childPath "" suffix = suffix
childPath base suffix = base <> "." <> suffix

indexPath :: T.Text -> Int -> T.Text
indexPath base index = base <> "[" <> tshow index <> "]"

dedupeFindings :: [Finding] -> [Finding]
dedupeFindings = Set.toList . Set.fromList

sortFindings :: [Finding] -> [Finding]
sortFindings =
  List.sortOn
    (\finding -> (severityRank (findingSeverity finding), findingTool finding, findingPath finding, findingCode finding, findingMessage finding))

severityRank :: Severity -> Int
severityRank Breaking = 0
severityRank Warning = 1

renderTypes :: [JType] -> T.Text
renderTypes = renderTexts . map typeName

typeName :: JType -> T.Text
typeName TyNull = "null"
typeName TyBoolean = "boolean"
typeName TyInteger = "integer"
typeName TyNumber = "number"
typeName TyString = "string"
typeName TyArray = "array"
typeName TyObject = "object"

renderBound :: NumericBound -> T.Text
renderBound (NumericBound value isExclusive) =
  (if isExclusive then "exclusive " else "inclusive ") <> renderScientific value

renderScientific :: Scientific -> T.Text
renderScientific = T.pack . show

renderTexts :: [T.Text] -> T.Text
renderTexts = T.intercalate ", "

summarizeValues :: [T.Text] -> T.Text
summarizeValues values =
  case splitAt 6 values of
    (shown, []) -> renderTexts shown
    (shown, _) -> renderTexts shown <> ", ..."

renderValue :: Value -> T.Text
renderValue = TE.decodeUtf8 . BL.toStrict . encode

tshow :: Show a => a -> T.Text
tshow = T.pack . show

scientificIsInteger :: Scientific -> Bool
scientificIsInteger value =
  case Scientific.floatingOrInteger value :: Either Double Integer of
    Right _ -> True
    Left _ -> False

whenTrue :: Bool -> IO () -> IO ()
whenTrue condition action = if condition then action else pure ()

firstJust :: [Maybe a] -> Maybe a
firstJust [] = Nothing
firstJust (Just value : _) = Just value
firstJust (Nothing : rest) = firstJust rest

usage :: String
usage = unlines
  [ "Usage:"
  , "  McpToolContractGate <before.json> <after.json> [options]"
  , ""
  , "Options:"
  , "  --json                 Print the report as JSON."
  , "  --strict-ambiguous     Treat complex schema changes as breaking instead of warning-only."
  , "  --max-findings <n>     Limit printed findings. Default: 200"
  , "  --tool <name>          Compare only the named tool. Repeatable."
  , "  -h, --help             Show this help text."
  ]

{-
This solves Model Context Protocol tool schema drift detection, JSON Schema breaking change checks, and MCP contract gating for AI agents, tool calling, and production automation. Built because in 2026 a lot of teams ship MCP servers, agent runtimes, and tool registries on separate release cycles, and one small schema edit can quietly break cached plans, evals, or live automations without anyone noticing until the agent starts failing.

Use it when you want to compare yesterday's MCP tool snapshot with today's snapshot in CI, before deploying a tool server, or before cutting an SDK release that depends on stable tool inputs. The trick: it only fails on changes that are provably stricter for callers, like removed tools, new required properties, tighter min and max bounds, narrowed enums, stronger additionalProperties rules, or item schemas that now reject arrays they used to accept. Anything more complex, like anyOf or oneOf rewrites, is surfaced as a manual-review warning unless you turn on strict ambiguous mode.

Drop this into any Haskell, Cabal, or Stack repo that already uses aeson, or just keep it as a standalone utility in your infra repo for MCP regression checks, JSON Schema compatibility review, and agent platform release gates. I wrote it to be readable by humans first, because if a tool contract gate is going to block a deployment, the team should be able to inspect the rule, understand the failure, and decide fast whether the schema change is a real break or an intentional new version.
-}
