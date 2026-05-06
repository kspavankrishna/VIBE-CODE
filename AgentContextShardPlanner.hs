module Main where

import Control.Monad (foldM, when)
import Data.Char (isSpace, toLower)
import Data.Graph (SCC (..), stronglyConnComp)
import Data.List (dropWhileEnd, foldl', intercalate, isPrefixOf, sortOn)
import qualified Data.Map.Strict as Map
import Data.Map.Strict (Map)
import qualified Data.Set as Set
import Data.Set (Set)
import Data.Ord (Down (..))
import Numeric (showFFloat)
import System.Environment (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.IO (hPutStrLn, stderr)
import Text.Read (readMaybe)

data OutputFormat
  = FormatPretty
  | FormatJson
  | FormatTsv
  deriving (Eq, Show)

data Config = Config
  { cfgInputPath :: Maybe FilePath
  , cfgMaxTokens :: Int
  , cfgReserveTokens :: Int
  , cfgMaxFiles :: Int
  , cfgFormat :: OutputFormat
  , cfgAllowOversizeSingles :: Bool
  , cfgMaxShards :: Maybe Int
  }
  deriving (Eq, Show)

data UnresolvedNode = UnresolvedNode
  { unPath :: FilePath
  , unTokens :: Int
  , unPriority :: Double
  , unDepsRaw :: [FilePath]
  , unTags :: Set String
  }
  deriving (Eq, Show)

data FileNode = FileNode
  { fnPath :: FilePath
  , fnTokens :: Int
  , fnPriority :: Double
  , fnDeps :: Set FilePath
  , fnTags :: Set String
  }
  deriving (Eq, Show)

data Problem = Problem
  { pbNodes :: [FileNode]
  , pbWarnings :: [String]
  }
  deriving (Eq, Show)

data Component = Component
  { compId :: Int
  , compMembers :: [FileNode]
  , compTokens :: Int
  , compInternalNeighbors :: Map FilePath (Set FilePath)
  }
  deriving (Eq, Show)

data UnitOrigin
  = WholeComponent Int
  | SplitComponent Int Int
  deriving (Eq, Show)

data RawUnit = RawUnit
  { ruId :: String
  , ruMembers :: [FileNode]
  , ruOrigin :: UnitOrigin
  , ruOversize :: Bool
  }
  deriving (Eq, Show)

data Unit = Unit
  { unitId :: String
  , unitMembers :: [FileNode]
  , unitPaths :: [FilePath]
  , unitTokens :: Int
  , unitPriorityScore :: Double
  , unitPinned :: Bool
  , unitEntry :: Bool
  , unitOversize :: Bool
  , unitDeps :: Set String
  , unitRevDeps :: Set String
  , unitOrigin :: UnitOrigin
  }
  deriving (Eq, Show)

data Shard = Shard
  { shardId :: Int
  , shardUnits :: [Unit]
  , shardTokens :: Int
  , shardFiles :: Int
  , shardUnitIds :: Set String
  }
  deriving (Eq, Show)

data PlannerState = PlannerState
  { psShards :: [Shard]
  , psUnitToShard :: Map String Int
  , psNextShardId :: Int
  , psWarnings :: [String]
  }
  deriving (Eq, Show)

data FileView = FileView
  { fvPath :: FilePath
  , fvTokens :: Int
  , fvPriority :: Double
  , fvTags :: [String]
  , fvExternalDeps :: Int
  }
  deriving (Eq, Show)

data ShardView = ShardView
  { svId :: Int
  , svTokens :: Int
  , svFileCount :: Int
  , svUtilizationPct :: Double
  , svOutgoingCrossEdges :: Int
  , svIncomingCrossEdges :: Int
  , svNeighborShards :: [Int]
  , svOversize :: Bool
  , svFiles :: [FileView]
  }
  deriving (Eq, Show)

data Report = Report
  { repFileCount :: Int
  , repUnitCount :: Int
  , repShardCount :: Int
  , repCrossShardEdges :: Int
  , repWarnings :: [String]
  , repShards :: [ShardView]
  }
  deriving (Eq, Show)

data BinState = BinState
  { binNumber :: Int
  , binMembers :: [FileNode]
  , binTokens :: Int
  , binPaths :: Set FilePath
  , binOversize :: Bool
  }
  deriving (Eq, Show)

defaultConfig :: Config
defaultConfig =
  Config
    { cfgInputPath = Nothing
    , cfgMaxTokens = 32000
    , cfgReserveTokens = 4000
    , cfgMaxFiles = 24
    , cfgFormat = FormatPretty
    , cfgAllowOversizeSingles = True
    , cfgMaxShards = Nothing
    }

main :: IO ()
main = do
  args <- getArgs
  config <- case parseArgs args of
    Left err -> failWith err
    Right Nothing -> putStrLn usage >> exitSuccess
    Right (Just cfg) -> pure cfg
  contents <- readInput (cfgInputPath config)
  report <- case planContextShards config contents of
    Left err -> failWith err
    Right x -> pure x
  putStr (renderReport config report)

failWith :: String -> IO a
failWith err = do
  hPutStrLn stderr err
  exitFailure

readInput :: Maybe FilePath -> IO String
readInput Nothing = getContents
readInput (Just "-") = getContents
readInput (Just path) = readFile path

parseArgs :: [String] -> Either String (Maybe Config)
parseArgs = go defaultConfig
  where
    go cfg [] = Right (Just cfg)
    go _ ["--help"] = Right Nothing
    go _ ["-h"] = Right Nothing
    go cfg ("--input" : path : rest) = go (cfg {cfgInputPath = Just path}) rest
    go cfg ("-i" : path : rest) = go (cfg {cfgInputPath = Just path}) rest
    go cfg ("--max-tokens" : value : rest) = do
      parsed <- parsePositiveInt "--max-tokens" value
      go (cfg {cfgMaxTokens = parsed}) rest
    go cfg ("-t" : value : rest) = do
      parsed <- parsePositiveInt "-t" value
      go (cfg {cfgMaxTokens = parsed}) rest
    go cfg ("--reserve" : value : rest) = do
      parsed <- parseNonNegativeInt "--reserve" value
      go (cfg {cfgReserveTokens = parsed}) rest
    go cfg ("-r" : value : rest) = do
      parsed <- parseNonNegativeInt "-r" value
      go (cfg {cfgReserveTokens = parsed}) rest
    go cfg ("--max-files" : value : rest) = do
      parsed <- parsePositiveInt "--max-files" value
      go (cfg {cfgMaxFiles = parsed}) rest
    go cfg ("-f" : value : rest) = do
      parsed <- parsePositiveInt "-f" value
      go (cfg {cfgMaxFiles = parsed}) rest
    go cfg ("--max-shards" : value : rest) = do
      parsed <- parsePositiveInt "--max-shards" value
      go (cfg {cfgMaxShards = Just parsed}) rest
    go cfg ("--format" : value : rest) = do
      parsed <- parseFormat value
      go (cfg {cfgFormat = parsed}) rest
    go cfg ("--deny-oversize-singletons" : rest) =
      go (cfg {cfgAllowOversizeSingles = False}) rest
    go cfg (arg : rest)
      | "-" `isPrefixOf` arg = Left ("unknown flag: " ++ arg ++ "\n\n" ++ usage)
      | cfgInputPath cfg == Nothing = go (cfg {cfgInputPath = Just arg}) rest
      | otherwise = Left ("unexpected positional argument: " ++ arg ++ "\n\n" ++ usage)

parseFormat :: String -> Either String OutputFormat
parseFormat raw =
  case lower raw of
    "pretty" -> Right FormatPretty
    "json" -> Right FormatJson
    "tsv" -> Right FormatTsv
    other -> Left ("unsupported format: " ++ other ++ " (expected pretty, json, or tsv)")

parsePositiveInt :: String -> String -> Either String Int
parsePositiveInt label value =
  case readMaybe value of
    Just n | n > 0 -> Right n
    _ -> Left (label ++ " must be a positive integer, got: " ++ value)

parseNonNegativeInt :: String -> String -> Either String Int
parseNonNegativeInt label value =
  case readMaybe value of
    Just n | n >= 0 -> Right n
    _ -> Left (label ++ " must be a non-negative integer, got: " ++ value)

planContextShards :: Config -> String -> Either String Report
planContextShards cfg input = do
  validateConfig cfg
  problem <- parseManifest input
  when (null (pbNodes problem)) $
    Left "manifest is empty after removing blank lines and comments"
  (units, warnings) <- buildUnits cfg problem
  state <- planShards cfg units
  pure (buildReport cfg units (pbNodes problem) (uniquePreserve (warnings ++ psWarnings state)) (psShards state))

validateConfig :: Config -> Either String ()
validateConfig cfg = do
  when (cfgMaxTokens cfg <= cfgReserveTokens cfg) $
    Left "--max-tokens must be greater than --reserve"
  when (cfgMaxFiles cfg <= 0) $
    Left "--max-files must be greater than zero"
  case cfgMaxShards cfg of
    Just n | n <= 0 -> Left "--max-shards must be greater than zero"
    _ -> Right ()

usableTokens :: Config -> Int
usableTokens cfg = cfgMaxTokens cfg - cfgReserveTokens cfg

parseManifest :: String -> Either String Problem
parseManifest input = do
  unresolved <- traverse parseLine (numberedRelevantLines input)
  let duplicates = findDuplicates (map unPath unresolved)
  when (not (null duplicates)) $
    Left ("duplicate paths in manifest: " ++ intercalate ", " duplicates)
  let knownPaths = Set.fromList (map unPath unresolved)
      resolved = map (resolveNode knownPaths) unresolved
      nodes = map fst resolved
      warnings = concatMap snd resolved
  pure Problem {pbNodes = sortOn fnPath nodes, pbWarnings = warnings}

numberedRelevantLines :: String -> [(Int, String)]
numberedRelevantLines =
  filter (not . isSkippableLine . snd) . zip [1 ..] . lines
  where
    isSkippableLine raw =
      let stripped = trim raw
       in null stripped || "#" `isPrefixOf` stripped

parseLine :: (Int, String) -> Either String UnresolvedNode
parseLine (lineNo, rawLine) =
  case splitOn '\t' rawLine of
    [pathField, tokenField, priorityField, depsField] ->
      buildNode lineNo pathField tokenField priorityField depsField "-"
    [pathField, tokenField, priorityField, depsField, tagsField] ->
      buildNode lineNo pathField tokenField priorityField depsField tagsField
    cols ->
      Left
        ( "line "
            ++ show lineNo
            ++ " must have 4 or 5 tab-separated columns, found "
            ++ show (length cols)
        )

buildNode :: Int -> String -> String -> String -> String -> String -> Either String UnresolvedNode
buildNode lineNo rawPath rawTokens rawPriority rawDeps rawTags = do
  let path = trim rawPath
  when (null path) $
    Left ("line " ++ show lineNo ++ " has an empty path")
  tokens <- case readMaybe (trim rawTokens) of
    Just n | n > 0 -> Right n
    _ -> Left ("line " ++ show lineNo ++ " has invalid token count: " ++ rawTokens)
  priority <- case readMaybe (trim rawPriority) of
    Just n | n >= 0 -> Right n
    _ -> Left ("line " ++ show lineNo ++ " has invalid priority score: " ++ rawPriority)
  let deps = parseCommaField rawDeps
      tags = Set.fromList (map lower (parseCommaField rawTags))
  pure
    UnresolvedNode
      { unPath = path
      , unTokens = tokens
      , unPriority = priority
      , unDepsRaw = deps
      , unTags = tags
      }

resolveNode :: Set FilePath -> UnresolvedNode -> (FileNode, [String])
resolveNode knownPaths unresolved =
  let rawDeps = filter (/= unPath unresolved) (unDepsRaw unresolved)
      knownDeps = Set.fromList [dep | dep <- rawDeps, dep `Set.member` knownPaths]
      missingDeps = [dep | dep <- rawDeps, dep `Set.notMember` knownPaths]
      selfWarning =
        if unPath unresolved `elem` unDepsRaw unresolved
          then
            [ "dropped self-dependency from "
                ++ unPath unresolved
            ]
          else []
      missingWarnings =
        [ "dropped unknown dependency "
            ++ dep
            ++ " referenced by "
            ++ unPath unresolved
        | dep <- missingDeps
        ]
   in ( FileNode
          { fnPath = unPath unresolved
          , fnTokens = unTokens unresolved
          , fnPriority = unPriority unresolved
          , fnDeps = knownDeps
          , fnTags = unTags unresolved
          }
      , selfWarning ++ missingWarnings
      )

buildUnits :: Config -> Problem -> Either String ([Unit], [String])
buildUnits cfg problem = do
  let components = buildComponents (pbNodes problem)
  componentOutputs <- traverse (componentToRawUnits cfg) components
  let rawUnits = concatMap fst componentOutputs
      warnings = pbWarnings problem ++ concatMap snd componentOutputs
  pure (hydrateUnits rawUnits, warnings)

buildComponents :: [FileNode] -> [Component]
buildComponents nodes =
  zipWith buildComponent [1 ..] sccs
  where
    sortedNodes = sortOn fnPath nodes
    graphInput =
      [ (node, fnPath node, Set.toList (fnDeps node))
      | node <- sortedNodes
      ]
    sccs = stronglyConnComp graphInput

buildComponent :: Int -> SCC FileNode -> Component
buildComponent cid scc =
  let members = sortOn fnPath (flattenScc scc)
   in Component
        { compId = cid
        , compMembers = members
        , compTokens = sum (map fnTokens members)
        , compInternalNeighbors = buildInternalNeighbors members
        }

flattenScc :: SCC FileNode -> [FileNode]
flattenScc (AcyclicSCC node) = [node]
flattenScc (CyclicSCC nodes) = nodes

buildInternalNeighbors :: [FileNode] -> Map FilePath (Set FilePath)
buildInternalNeighbors members =
  foldl' addNode seed members
  where
    memberPaths = Set.fromList (map fnPath members)
    seed = Map.fromList [(fnPath node, Set.empty) | node <- members]
    addNode acc node =
      foldl'
        (addEdge (fnPath node))
        acc
        [ dep
        | dep <- Set.toList (fnDeps node)
        , dep `Set.member` memberPaths
        , dep /= fnPath node
        ]
    addEdge src acc dst =
      Map.insertWith Set.union src (Set.singleton dst) $
        Map.insertWith Set.union dst (Set.singleton src) acc

componentToRawUnits :: Config -> Component -> Either String ([RawUnit], [String])
componentToRawUnits cfg component
  | compTokens component <= budget && length (compMembers component) <= cfgMaxFiles cfg =
      Right ([wholeUnit], [])
  | otherwise = do
      bins <- splitComponent cfg component
      let rawUnits = map toRawUnit bins
          warnings = buildSplitWarnings bins
      pure (rawUnits, warnings)
  where
    budget = usableTokens cfg
    wholeUnit =
      RawUnit
        { ruId = componentUnitId (compId component)
        , ruMembers = compMembers component
        , ruOrigin = WholeComponent (compId component)
        , ruOversize = False
        }
    toRawUnit bin =
      RawUnit
        { ruId = splitUnitId (compId component) (binNumber bin)
        , ruMembers = sortOn fnPath (binMembers bin)
        , ruOrigin = SplitComponent (compId component) (binNumber bin)
        , ruOversize = binOversize bin
        }
    buildSplitWarnings bins =
      let reasons = splitReasons cfg component
          prefix =
            "split component "
              ++ componentUnitId (compId component)
              ++ " ("
              ++ show (length (compMembers component))
              ++ " files, "
              ++ show (compTokens component)
              ++ " tokens) into "
              ++ show (length bins)
              ++ " bins because "
              ++ intercalate " and " reasons
          oversizeWarnings =
            [ "kept oversize file "
                ++ fnPath node
                ++ " alone at "
                ++ show (fnTokens node)
                ++ " tokens because usable budget is "
                ++ show budget
            | bin <- bins
            , binOversize bin
            , node <- binMembers bin
            ]
       in prefix : oversizeWarnings

splitReasons :: Config -> Component -> [String]
splitReasons cfg component =
  concat
    [ [ "usable token budget "
          ++ show (usableTokens cfg)
          ++ " was smaller than component size"
      | compTokens component > usableTokens cfg
      ]
    , [ "max file cap "
          ++ show (cfgMaxFiles cfg)
          ++ " was exceeded"
      | length (compMembers component) > cfgMaxFiles cfg
      ]
    ]

splitComponent :: Config -> Component -> Either String [BinState]
splitComponent cfg component =
  foldM placeMember [] orderedMembers
  where
    budget = usableTokens cfg
    neighborMap = compInternalNeighbors component
    orderedMembers =
      sortOn memberOrderKey (compMembers component)
    memberOrderKey node =
      ( Down (boolScore (hasTag "pin" (fnTags node)))
      , Down (boolScore (hasTag "entry" (fnTags node)))
      , Down (Set.size (Map.findWithDefault Set.empty (fnPath node) neighborMap))
      , Down (round (scoreFile node * 1000) :: Int)
      , Down (fnTokens node)
      , fnPath node
      )
    placeMember bins node
      | fnTokens node > budget =
          if cfgAllowOversizeSingles cfg
            then Right (bins ++ [newOversizeBin node])
            else
              Left
                ( "file "
                    ++ fnPath node
                    ++ " needs "
                    ++ show (fnTokens node)
                    ++ " tokens, which exceeds usable budget "
                    ++ show budget
                )
      | otherwise =
          case viableBins of
            [] -> Right (bins ++ [freshBin node False])
            _ ->
              let chosen = head (sortOn scoreBin viableBins)
               in Right (replaceBin chosen (insertIntoBin chosen node) bins)
      where
        viableBins =
          [ bin
          | bin <- bins
          , not (binOversize bin)
          , binTokens bin + fnTokens node <= budget
          , length (binMembers bin) + 1 <= cfgMaxFiles cfg
          ]
        scoreBin bin =
          let locality =
                Set.size
                  ( Map.findWithDefault Set.empty (fnPath node) neighborMap
                      `Set.intersection` binPaths bin
                  )
              slack = budget - (binTokens bin + fnTokens node)
           in (negate locality, slack, binNumber bin)
        freshBin item oversize =
          BinState
            { binNumber = length bins + 1
            , binMembers = [item]
            , binTokens = fnTokens item
            , binPaths = Set.singleton (fnPath item)
            , binOversize = oversize
            }
        newOversizeBin item = freshBin item True
        insertIntoBin bin item =
          bin
            { binMembers = item : binMembers bin
            , binTokens = binTokens bin + fnTokens item
            , binPaths = Set.insert (fnPath item) (binPaths bin)
            }

replaceBin :: BinState -> BinState -> [BinState] -> [BinState]
replaceBin old new =
  map (\candidate -> if binNumber candidate == binNumber old then new else candidate)

hydrateUnits :: [RawUnit] -> [Unit]
hydrateUnits rawUnits =
  map toUnit rawUnits
  where
    pathToUnit =
      Map.fromList
        [ (fnPath node, ruId rawUnit)
        | rawUnit <- rawUnits
        , node <- ruMembers rawUnit
        ]
    forwardDeps =
      Map.fromList
        [ (ruId rawUnit, depsForRawUnit rawUnit)
        | rawUnit <- rawUnits
        ]
    reverseDeps =
      Map.fromListWith Set.union
        [ (targetUnit, Set.singleton sourceUnit)
        | (sourceUnit, deps) <- Map.toList forwardDeps
        , targetUnit <- Set.toList deps
        ]
    depsForRawUnit rawUnit =
      Set.fromList
        [ targetUnit
        | node <- ruMembers rawUnit
        , dep <- Set.toList (fnDeps node)
        , Just targetUnit <- [Map.lookup dep pathToUnit]
        , targetUnit /= ruId rawUnit
        ]
    toUnit rawUnit =
      let sortedMembers = sortOn fnPath (ruMembers rawUnit)
          memberPaths = map fnPath sortedMembers
       in Unit
            { unitId = ruId rawUnit
            , unitMembers = sortedMembers
            , unitPaths = memberPaths
            , unitTokens = sum (map fnTokens sortedMembers)
            , unitPriorityScore = sum (map scoreFile sortedMembers)
            , unitPinned = any (hasTag "pin" . fnTags) sortedMembers
            , unitEntry = any (hasTag "entry" . fnTags) sortedMembers
            , unitOversize = ruOversize rawUnit
            , unitDeps = Map.findWithDefault Set.empty (ruId rawUnit) forwardDeps
            , unitRevDeps = Map.findWithDefault Set.empty (ruId rawUnit) reverseDeps
            , unitOrigin = ruOrigin rawUnit
            }

scoreFile :: FileNode -> Double
scoreFile node =
  fnPriority node
    + bonus "pin" 25.0
    + bonus "entry" 8.0
    + bonus "test" 1.0
    + bonus "leaf" (-0.5)
  where
    bonus tagName amount =
      if hasTag tagName (fnTags node)
        then amount
        else 0.0

planShards :: Config -> [Unit] -> Either String PlannerState
planShards cfg units =
  foldM (placeUnit cfg) emptyState orderedUnits
  where
    emptyState =
      PlannerState
        { psShards = []
        , psUnitToShard = Map.empty
        , psNextShardId = 1
        , psWarnings = []
        }
    orderedUnits = sortOn unitOrderKey units
    unitOrderKey unit =
      ( Down (boolScore (unitPinned unit))
      , Down (boolScore (unitEntry unit))
      , Down (Set.size (Set.union (unitDeps unit) (unitRevDeps unit)))
      , Down (round (unitPriorityScore unit * 1000) :: Int)
      , Down (unitTokens unit)
      , headOr "" (unitPaths unit)
      )

placeUnit :: Config -> PlannerState -> Unit -> Either String PlannerState
placeUnit cfg state unit
  | unitOversize unit =
      if cfgAllowOversizeSingles cfg && length (unitMembers unit) == 1
        then appendNewShard cfg state unit warning
        else
          Left
            ( "oversize unit "
                ++ unitId unit
                ++ " could not be placed safely"
            )
  | otherwise =
      case choosePlacement cfg state unit of
        Left err -> Left err
        Right (Just shard) -> pure (attachToExistingShard state shard unit)
        Right Nothing -> appendNewShard cfg state unit Nothing
  where
    warning =
      Just
        ( "placed oversize file "
            ++ headOr (unitId unit) (unitPaths unit)
            ++ " in a dedicated shard because it needs "
            ++ show (unitTokens unit)
            ++ " tokens and usable budget is "
            ++ show (usableTokens cfg)
        )

choosePlacement :: Config -> PlannerState -> Unit -> Either String (Maybe Shard)
choosePlacement cfg state unit =
  case scoredCandidates of
    [] ->
      if canCreateNewShard cfg state
        then Right Nothing
        else
          Left
            ( "no shard can fit unit "
                ++ unitId unit
                ++ " and --max-shards would be exceeded"
            )
    _ ->
      case snd (head (sortOn fst scoredCandidates)) of
        ExistingShard shard -> Right (Just shard)
        NewShard -> Right Nothing
  where
    fittingShards = filter (canFitUnit cfg unit) (psShards state)
    scoredExisting =
      [ (existingScore shard, ExistingShard shard)
      | shard <- fittingShards
      ]
    scoredNew =
      if canCreateNewShard cfg state
        then [(newShardScore, NewShard)]
        else []
    scoredCandidates = scoredExisting ++ scoredNew
    neighborIds = Set.union (unitDeps unit) (unitRevDeps unit)
    assignedNeighbors =
      Set.filter (`Map.member` psUnitToShard state) neighborIds
    existingScore shard =
      let colocated = Set.size (assignedNeighbors `Set.intersection` shardUnitIds shard)
          cutEdges = Set.size assignedNeighbors - colocated
          slack = usableTokens cfg - (shardTokens shard + unitTokens unit)
       in (cutEdges, negate colocated, 0 :: Int, slack, shardFiles shard, shardId shard)
    newShardScore =
      let slack = usableTokens cfg - unitTokens unit
       in (Set.size assignedNeighbors, 0 :: Int, 1 :: Int, slack, 0 :: Int, psNextShardId state)

data PlacementTarget
  = ExistingShard Shard
  | NewShard

canFitUnit :: Config -> Unit -> Shard -> Bool
canFitUnit cfg unit shard =
  shardTokens shard + unitTokens unit <= usableTokens cfg
    && shardFiles shard + length (unitPaths unit) <= cfgMaxFiles cfg

canCreateNewShard :: Config -> PlannerState -> Bool
canCreateNewShard cfg state =
  case cfgMaxShards cfg of
    Nothing -> True
    Just limit -> psNextShardId state <= limit

attachToExistingShard :: PlannerState -> Shard -> Unit -> PlannerState
attachToExistingShard state target unit =
  state
    { psShards = map attach (psShards state)
    , psUnitToShard = Map.insert (unitId unit) (shardId target) (psUnitToShard state)
    }
  where
    attach shard
      | shardId shard == shardId target = addUnit shard unit
      | otherwise = shard

appendNewShard :: Config -> PlannerState -> Unit -> Maybe String -> Either String PlannerState
appendNewShard cfg state unit maybeWarning
  | not (canCreateNewShard cfg state) =
      Left
        ( "cannot create a new shard for unit "
            ++ unitId unit
            ++ " because --max-shards would be exceeded"
        )
  | otherwise =
      let newId = psNextShardId state
          shard = addUnit (emptyShard newId) unit
          warnings =
            case maybeWarning of
              Nothing -> psWarnings state
              Just warningText -> psWarnings state ++ [warningText]
       in Right
            state
              { psShards = psShards state ++ [shard]
              , psUnitToShard = Map.insert (unitId unit) newId (psUnitToShard state)
              , psNextShardId = newId + 1
              , psWarnings = warnings
              }

emptyShard :: Int -> Shard
emptyShard sid =
  Shard
    { shardId = sid
    , shardUnits = []
    , shardTokens = 0
    , shardFiles = 0
    , shardUnitIds = Set.empty
    }

addUnit :: Shard -> Unit -> Shard
addUnit shard unit =
  shard
    { shardUnits = shardUnits shard ++ [unit]
    , shardTokens = shardTokens shard + unitTokens unit
    , shardFiles = shardFiles shard + length (unitPaths unit)
    , shardUnitIds = Set.insert (unitId unit) (shardUnitIds shard)
    }

buildReport :: Config -> [Unit] -> [FileNode] -> [String] -> [Shard] -> Report
buildReport cfg units nodes warnings shards =
  Report
    { repFileCount = length nodes
    , repUnitCount = length units
    , repShardCount = length shards
    , repCrossShardEdges = length crossEdges
    , repWarnings = warnings
    , repShards = map toShardView shards
    }
  where
    pathToShard =
      Map.fromList
        [ (fnPath node, shardId shard)
        | shard <- shards
        , unit <- shardUnits shard
        , node <- unitMembers unit
        ]
    nodeMap = Map.fromList [(fnPath node, node) | node <- nodes]
    crossEdges =
      [ (sourcePath, targetPath, sourceShard, targetShard)
      | node <- nodes
      , let sourcePath = fnPath node
      , dep <- Set.toList (fnDeps node)
      , Just sourceShard <- [Map.lookup sourcePath pathToShard]
      , Just targetShard <- [Map.lookup dep pathToShard]
      , sourceShard /= targetShard
      , let targetPath = dep
      ]
    outgoingByShard =
      Map.fromListWith (+)
        [ (sourceShard, 1 :: Int)
        | (_, _, sourceShard, _) <- crossEdges
        ]
    incomingByShard =
      Map.fromListWith (+)
        [ (targetShard, 1 :: Int)
        | (_, _, _, targetShard) <- crossEdges
        ]
    neighborShardMap =
      Map.fromListWith Set.union
        [ (sourceShard, Set.singleton targetShard)
        | (_, _, sourceShard, targetShard) <- crossEdges
        ]
    externalDepsByFile =
      Map.fromListWith (+)
        [ (sourcePath, 1 :: Int)
        | (sourcePath, _, _, _) <- crossEdges
        ]
    toShardView shard =
      let memberNodes =
            sortOn fnPath
              [ node
              | unit <- shardUnits shard
              , node <- unitMembers unit
              ]
          outgoing = Map.findWithDefault 0 (shardId shard) outgoingByShard
          incoming = Map.findWithDefault 0 (shardId shard) incomingByShard
          neighbors = Set.toList (Map.findWithDefault Set.empty (shardId shard) neighborShardMap)
       in ShardView
            { svId = shardId shard
            , svTokens = shardTokens shard
            , svFileCount = shardFiles shard
            , svUtilizationPct =
                percent (shardTokens shard) (usableTokens cfg)
            , svOutgoingCrossEdges = outgoing
            , svIncomingCrossEdges = incoming
            , svNeighborShards = neighbors
            , svOversize = any unitOversize (shardUnits shard)
            , svFiles =
                [ FileView
                    { fvPath = fnPath node
                    , fvTokens = fnTokens node
                    , fvPriority = fnPriority node
                    , fvTags = Set.toList (fnTags node)
                    , fvExternalDeps = Map.findWithDefault 0 (fnPath node) externalDepsByFile
                    }
                | node <- memberNodes
                , Map.member (fnPath node) nodeMap
                ]
            }

renderReport :: Config -> Report -> String
renderReport cfg report =
  case cfgFormat cfg of
    FormatPretty -> renderPretty cfg report
    FormatJson -> renderJson cfg report
    FormatTsv -> renderTsv report

renderPretty :: Config -> Report -> String
renderPretty cfg report =
  unlines $
    summaryLines
      ++ warningLines
      ++ concatMap renderShard (repShards report)
  where
    summaryLines =
      [ "AgentContextShardPlanner"
      , "files: " ++ show (repFileCount report)
      , "units: " ++ show (repUnitCount report)
      , "shards: " ++ show (repShardCount report)
      , "max_tokens: " ++ show (cfgMaxTokens cfg)
      , "reserve_tokens: " ++ show (cfgReserveTokens cfg)
      , "usable_tokens: " ++ show (usableTokens cfg)
      , "cross_shard_edges: " ++ show (repCrossShardEdges report)
      ]
    warningLines =
      case repWarnings report of
        [] -> []
        warnings ->
          ""
            : "warnings:"
            : [ "  - " ++ warningText | warningText <- warnings ]
            ++ [""]
    renderShard shardView =
      let header =
            "shard "
              ++ show (svId shardView)
              ++ " | tokens "
              ++ show (svTokens shardView)
              ++ "/"
              ++ show (usableTokens cfg)
              ++ " | utilization "
              ++ formatDouble (svUtilizationPct shardView)
              ++ "%"
              ++ " | files "
              ++ show (svFileCount shardView)
              ++ " | cross out "
              ++ show (svOutgoingCrossEdges shardView)
              ++ " | cross in "
              ++ show (svIncomingCrossEdges shardView)
       in [header]
            ++ [ "neighbors: "
                   ++ if null (svNeighborShards shardView)
                     then "-"
                     else intercalate "," (map show (svNeighborShards shardView))
               ]
            ++ [ "oversize: " ++ yesNo (svOversize shardView)
               ]
            ++ [ "  - "
                   ++ fvPath fileView
                   ++ " | "
                   ++ show (fvTokens fileView)
                   ++ " tok"
                   ++ " | priority "
                   ++ formatDouble (fvPriority fileView)
                   ++ " | external deps "
                   ++ show (fvExternalDeps fileView)
                   ++ " | tags "
                   ++ renderTags (fvTags fileView)
               | fileView <- svFiles shardView
               ]
            ++ [""]

renderJson :: Config -> Report -> String
renderJson cfg report =
  jsonObject
    [ ( "summary"
      , jsonObject
          [ ("file_count", show (repFileCount report))
          , ("unit_count", show (repUnitCount report))
          , ("shard_count", show (repShardCount report))
          , ("max_tokens", show (cfgMaxTokens cfg))
          , ("reserve_tokens", show (cfgReserveTokens cfg))
          , ("usable_tokens", show (usableTokens cfg))
          , ("cross_shard_edges", show (repCrossShardEdges report))
          , ("warning_count", show (length (repWarnings report)))
          ]
      )
    , ("warnings", jsonArray (map jsonString (repWarnings report)))
    , ("shards", jsonArray (map renderShardJson (repShards report)))
    ]
    ++ "\n"
  where
    renderShardJson shardView =
      jsonObject
        [ ("id", show (svId shardView))
        , ("token_count", show (svTokens shardView))
        , ("file_count", show (svFileCount shardView))
        , ("utilization_pct", renderNumber (svUtilizationPct shardView))
        , ("outgoing_cross_edges", show (svOutgoingCrossEdges shardView))
        , ("incoming_cross_edges", show (svIncomingCrossEdges shardView))
        , ("neighbor_shards", jsonArray (map show (svNeighborShards shardView)))
        , ("oversize", jsonBool (svOversize shardView))
        , ("files", jsonArray (map renderFileJson (svFiles shardView)))
        ]
    renderFileJson fileView =
      jsonObject
        [ ("path", jsonString (fvPath fileView))
        , ("tokens", show (fvTokens fileView))
        , ("priority", renderNumber (fvPriority fileView))
        , ("external_deps", show (fvExternalDeps fileView))
        , ("tags", jsonArray (map jsonString (fvTags fileView)))
        ]

renderTsv :: Report -> String
renderTsv report =
  unlines $
    "shard_id\tpath\ttokens\tpriority\texternal_deps\ttags"
      : concatMap shardRows (repShards report)
  where
    shardRows shardView =
      [ intercalate
          "\t"
          [ show (svId shardView)
          , fvPath fileView
          , show (fvTokens fileView)
          , formatDouble (fvPriority fileView)
          , show (fvExternalDeps fileView)
          , renderTags (fvTags fileView)
          ]
      | fileView <- svFiles shardView
      ]

jsonObject :: [(String, String)] -> String
jsonObject fields =
  "{"
    ++ intercalate "," [jsonString key ++ ":" ++ value | (key, value) <- fields]
    ++ "}"

jsonArray :: [String] -> String
jsonArray values = "[" ++ intercalate "," values ++ "]"

jsonString :: String -> String
jsonString raw =
  "\"" ++ concatMap escapeChar raw ++ "\""
  where
    escapeChar '"' = "\\\""
    escapeChar '\\' = "\\\\"
    escapeChar '\n' = "\\n"
    escapeChar '\r' = "\\r"
    escapeChar '\t' = "\\t"
    escapeChar c
      | fromEnum c < 32 = "\\u" ++ leftPad 4 '0' (showHex4 (fromEnum c))
      | otherwise = [c]

showHex4 :: Int -> String
showHex4 n =
  reverse (go n)
  where
    go value
      | value < 16 = [hexDigit value]
      | otherwise =
          let (quotient, remainder) = value `quotRem` 16
           in hexDigit remainder : go quotient
    hexDigit value
      | value < 10 = toEnum (fromEnum '0' + value)
      | otherwise = toEnum (fromEnum 'a' + value - 10)

jsonBool :: Bool -> String
jsonBool True = "true"
jsonBool False = "false"

renderNumber :: Double -> String
renderNumber value = trimTrailingZeros (showFFloat (Just 6) value "")

trimTrailingZeros :: String -> String
trimTrailingZeros raw =
  case break (== '.') raw of
    (_, "") -> raw
    (whole, frac) ->
      let stripped = reverse (dropWhile (== '0') (reverse frac))
       in case stripped of
            "." -> whole
            _ -> whole ++ stripped

renderTags :: [String] -> String
renderTags [] = "-"
renderTags tags = intercalate "," tags

formatDouble :: Double -> String
formatDouble value = showFFloat (Just 2) value ""

percent :: Int -> Int -> Double
percent _ 0 = 0.0
percent numerator denominator =
  (fromIntegral numerator * 100.0) / fromIntegral denominator

componentUnitId :: Int -> String
componentUnitId cid = "c" ++ show cid

splitUnitId :: Int -> Int -> String
splitUnitId cid piece = "c" ++ show cid ++ "-p" ++ show piece

hasTag :: String -> Set String -> Bool
hasTag tagName tags = lower tagName `Set.member` tags

boolScore :: Bool -> Int
boolScore True = 1
boolScore False = 0

parseCommaField :: String -> [String]
parseCommaField raw =
  case trim raw of
    "" -> []
    "-" -> []
    stripped ->
      [ trim part
      | part <- splitOn ',' stripped
      , not (null (trim part))
      ]

splitOn :: Char -> String -> [String]
splitOn delimiter text =
  case break (== delimiter) text of
    (chunk, []) -> [chunk]
    (chunk, _ : rest) -> chunk : splitOn delimiter rest

trim :: String -> String
trim = dropWhileEnd isSpace . dropWhile isSpace

lower :: String -> String
lower = map toLower

headOr :: a -> [a] -> a
headOr fallback values =
  case values of
    [] -> fallback
    value : _ -> value

leftPad :: Int -> Char -> String -> String
leftPad width padChar value =
  replicate (max 0 (width - length value)) padChar ++ value

findDuplicates :: Ord a => [a] -> [a]
findDuplicates values =
  Map.keys
    (Map.filter (> (1 :: Int)) (Map.fromListWith (+) [(value, 1 :: Int) | value <- values]))

uniquePreserve :: Ord a => [a] -> [a]
uniquePreserve =
  reverse . fst . foldl' step ([], Set.empty)
  where
    step (acc, seen) value
      | value `Set.member` seen = (acc, seen)
      | otherwise = (value : acc, Set.insert value seen)

yesNo :: Bool -> String
yesNo True = "yes"
yesNo False = "no"

usage :: String
usage =
  unlines
    [ "Usage:"
    , "  AgentContextShardPlanner.hs [manifest.tsv|-]"
    , "    [--input path]"
    , "    [--max-tokens 32000]"
    , "    [--reserve 4000]"
    , "    [--max-files 24]"
    , "    [--max-shards 12]"
    , "    [--format pretty|json|tsv]"
    , "    [--deny-oversize-singletons]"
    , ""
    , "Manifest format (tab-separated, one file per line):"
    , "  path<TAB>tokens<TAB>priority<TAB>deps<TAB>tags"
    , ""
    , "Fields:"
    , "  path     repo-relative file path"
    , "  tokens   estimated prompt tokens for the file"
    , "  priority non-negative score from your own heuristics"
    , "  deps     comma-separated repo-relative dependencies, or -"
    , "  tags     comma-separated labels like pin,entry,test,leaf, or -"
    , ""
    , "Example:"
    , "  src/App.tsx<TAB>2100<TAB>8.5<TAB>src/router.ts,src/auth.ts<TAB>pin,entry"
    ]

{-
This solves a real AI coding workflow problem that keeps showing up in 2026: one repo is too large for a single clean prompt, but splitting it badly makes agents lose dependency context and create noisy patches. Built because the hard part is not counting tokens. The hard part is keeping strongly related files together, isolating huge cycles safely, and doing it in a deterministic way so teams can rerun the planner in CI and get stable shard boundaries.

Use it when you already have a file manifest with token estimates, dependency edges, and maybe a few priority hints from churn, ownership, failing tests, or entrypoint status. The output is meant for multi-agent coding systems, repo summarizers, planning bots, review assistants, and any pipeline that needs prompt-sized code shards without random fragmentation.

The trick: it first collapses dependency cycles, then only breaks oversized components with a locality-aware binning pass, and finally places those bins into prompt-sized shards by minimizing already-known cross-shard edges before chasing perfect packing. That keeps the result practical instead of mathematically cute and operationally useless.

Drop this into an internal developer platform, a build step that prepares AI context packs, a Codex or Claude orchestration repo, or a research harness that compares parallel-agent planning quality. I wrote it so someone can fork it, feed in a TSV manifest from their own tokenizer and dependency extractor, and immediately get deterministic context shards that are actually usable for production AI tooling, agentic coding, repo-scale prompt planning, and code review automation.
-}
