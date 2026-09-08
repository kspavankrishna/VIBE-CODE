module ModelDriftTriage
  ( Observation(..)
  , Window(..)
  , Policy(..)
  , defaultPolicy
  , TriageError(..)
  , Analysis(..)
  , SliceReport(..)
  , Stats(..)
  , Finding(..)
  , FindingCode(..)
  , Severity(..)
  , Interval(..)
  , analyze
  , rankReports
  , shouldBlock
  , rowsInWindow
  , statsOf
  , wilsonInterval
  , quantile
  , relativeChange
  ) where

import Data.List (foldl', sort, sortBy)
import qualified Data.Map.Strict as Map
import Data.Maybe (mapMaybe)

data Observation key = Observation
  { observationSlice :: key
  , observationDay :: Int
  , observationPromptTokens :: !Double
  , observationCompletionTokens :: !Double
  , observationLatencyMs :: !Double
  , observationFailed :: !Bool
  , observationCostUsd :: !Double
  , observationQualityScore :: !(Maybe Double)
  , observationCarbonGrams :: !Double
  } deriving (Eq, Show)

data Window = Window
  { windowStartDay :: !Int
  , windowEndDay :: !Int
  } deriving (Eq, Show)

data Policy = Policy
  { minBaselineSamples :: !Int
  , minCandidateSamples :: !Int
  , warnErrorRateAbsolute :: !Double
  , blockErrorRateAbsolute :: !Double
  , warnLatencyRelative :: !Double
  , blockLatencyRelative :: !Double
  , warnCostRelative :: !Double
  , blockCostRelative :: !Double
  , warnCarbonRelative :: !Double
  , blockCarbonRelative :: !Double
  , warnQualityDropAbsolute :: !Double
  , blockQualityDropAbsolute :: !Double
  , warnQualityDropRelative :: !Double
  , blockQualityDropRelative :: !Double
  } deriving (Eq, Show)

data TriageError
  = InvalidPolicy
  | InvalidBaselineWindow
  | InvalidCandidateWindow
  | InvalidObservation
  | EmptyBaselineWindow
  | EmptyCandidateWindow
  deriving (Eq, Show)

data Severity
  = Informational
  | Warning
  | Blocking
  deriving (Eq, Ord, Show)

data FindingCode
  = LowSample
  | MissingBaseline
  | MissingCandidate
  | ErrorRateRegression
  | LatencyRegression
  | CostRegression
  | CarbonRegression
  | QualityRegression
  deriving (Eq, Ord, Show)

data Finding = Finding
  { findingSeverity :: !Severity
  , findingCode :: !FindingCode
  , findingBaselineValue :: !Double
  , findingCandidateValue :: !Double
  , findingAbsoluteDelta :: !Double
  , findingRelativeDelta :: !(Maybe Double)
  , findingConfidence :: !Double
  } deriving (Eq, Show)

data Stats = Stats
  { statCount :: !Int
  , statErrors :: !Int
  , statTokens :: !Double
  , statCostUsd :: !Double
  , statCarbonGrams :: !Double
  , statLatencyP50 :: !Double
  , statLatencyP95 :: !Double
  , statErrorRate :: !Double
  , statCostPerThousandTokens :: !Double
  , statCarbonPerThousandTokens :: !Double
  , statQualityP50 :: !(Maybe Double)
  } deriving (Eq, Show)

data SliceReport key = SliceReport
  { reportSlice :: key
  , reportBaseline :: !Stats
  , reportCandidate :: !Stats
  , reportFindings :: [Finding]
  , reportSeverity :: !Severity
  , reportRiskScore :: !Double
  } deriving (Eq, Show)

data Analysis key = Analysis
  { analysisPolicy :: !Policy
  , analysisBaselineWindow :: !Window
  , analysisCandidateWindow :: !Window
  , analysisInputRows :: !Int
  , analysisBaselineRows :: !Int
  , analysisCandidateRows :: !Int
  , analysisReports :: [SliceReport key]
  } deriving (Eq, Show)

data Interval = Interval
  { intervalLower :: !Double
  , intervalUpper :: !Double
  } deriving (Eq, Show)

defaultPolicy :: Policy
defaultPolicy = Policy
  { minBaselineSamples = 50
  , minCandidateSamples = 50
  , warnErrorRateAbsolute = 0.01
  , blockErrorRateAbsolute = 0.05
  , warnLatencyRelative = 0.25
  , blockLatencyRelative = 0.60
  , warnCostRelative = 0.15
  , blockCostRelative = 0.40
  , warnCarbonRelative = 0.25
  , blockCarbonRelative = 0.75
  , warnQualityDropAbsolute = 0.03
  , blockQualityDropAbsolute = 0.08
  , warnQualityDropRelative = 0.05
  , blockQualityDropRelative = 0.12
  }

analyze :: Ord key => Policy -> Window -> Window -> [Observation key] -> Either TriageError (Analysis key)
analyze policy baselineWindow candidateWindow observations
  | not (validPolicy policy) = Left InvalidPolicy
  | not (validWindow baselineWindow) = Left InvalidBaselineWindow
  | not (validWindow candidateWindow) = Left InvalidCandidateWindow
  | any (not . validObservation) observations = Left InvalidObservation
  | null baselineRows = Left EmptyBaselineWindow
  | null candidateRows = Left EmptyCandidateWindow
  | otherwise = Right analysis
  where
    baselineRows = rowsInWindow baselineWindow observations
    candidateRows = rowsInWindow candidateWindow observations
    baselineMap = groupBySlice baselineRows
    candidateMap = groupBySlice candidateRows
    slices = sort (Map.keys (Map.union baselineMap candidateMap))
    reports = rankReports (map (buildReport policy baselineMap candidateMap) slices)
    analysis = Analysis
      { analysisPolicy = policy
      , analysisBaselineWindow = baselineWindow
      , analysisCandidateWindow = candidateWindow
      , analysisInputRows = length observations
      , analysisBaselineRows = length baselineRows
      , analysisCandidateRows = length candidateRows
      , analysisReports = reports
      }

rowsInWindow :: Window -> [Observation key] -> [Observation key]
rowsInWindow window = filter inside
  where
    inside row = observationDay row >= windowStartDay window && observationDay row <= windowEndDay window

buildReport :: Ord key => Policy -> Map.Map key [Observation key] -> Map.Map key [Observation key] -> key -> SliceReport key
buildReport policy baselineMap candidateMap slice = SliceReport
  { reportSlice = slice
  , reportBaseline = baselineStats
  , reportCandidate = candidateStats
  , reportFindings = fs
  , reportSeverity = severityOf fs
  , reportRiskScore = riskScore fs
  }
  where
    baselineRows = Map.findWithDefault [] slice baselineMap
    candidateRows = Map.findWithDefault [] slice candidateMap
    baselineStats = statsOf baselineRows
    candidateStats = statsOf candidateRows
    fs = findingsFor policy baselineStats candidateStats

findingsFor :: Policy -> Stats -> Stats -> [Finding]
findingsFor policy baseline candidate
  | statCount baseline == 0 = [missingFinding MissingBaseline]
  | statCount candidate == 0 = [missingFinding MissingCandidate]
  | otherwise = concat
      [ sampleFindings policy baseline candidate
      , errorFinding policy baseline candidate
      , relativeMetricFinding LatencyRegression (warnLatencyRelative policy) (blockLatencyRelative policy) (statLatencyP95 baseline) (statLatencyP95 candidate)
      , relativeMetricFinding CostRegression (warnCostRelative policy) (blockCostRelative policy) (statCostPerThousandTokens baseline) (statCostPerThousandTokens candidate)
      , relativeMetricFinding CarbonRegression (warnCarbonRelative policy) (blockCarbonRelative policy) (statCarbonPerThousandTokens baseline) (statCarbonPerThousandTokens candidate)
      , qualityFinding policy baseline candidate
      ]
  where
    missingFinding code = Finding Blocking code (fromIntegral (statCount baseline)) (fromIntegral (statCount candidate)) (fromIntegral (statCount candidate - statCount baseline)) Nothing 1

sampleFindings :: Policy -> Stats -> Stats -> [Finding]
sampleFindings policy baseline candidate =
  [ Finding Warning LowSample baseCount candidateCount (candidateCount - baseCount) Nothing 0.25
  | statCount baseline < minBaselineSamples policy || statCount candidate < minCandidateSamples policy
  ]
  where
    baseCount = fromIntegral (statCount baseline)
    candidateCount = fromIntegral (statCount candidate)

errorFinding :: Policy -> Stats -> Stats -> [Finding]
errorFinding policy baseline candidate
  | delta < warnErrorRateAbsolute policy = []
  | otherwise = [Finding severity ErrorRateRegression (statErrorRate baseline) (statErrorRate candidate) delta (relativeChange (statErrorRate baseline) (statErrorRate candidate)) confidence]
  where
    delta = statErrorRate candidate - statErrorRate baseline
    baseInterval = wilsonInterval (statErrors baseline) (statCount baseline)
    candidateInterval = wilsonInterval (statErrors candidate) (statCount candidate)
    separated = intervalLower candidateInterval > intervalUpper baseInterval
    doubled = statErrorRate candidate >= statErrorRate baseline * 2 + warnErrorRateAbsolute policy
    severity
      | delta >= blockErrorRateAbsolute policy || doubled = Blocking
      | otherwise = Warning
    confidence
      | separated = 1
      | otherwise = bounded01 (delta / blockErrorRateAbsolute policy)

relativeMetricFinding :: FindingCode -> Double -> Double -> Double -> Double -> [Finding]
relativeMetricFinding code warnThreshold blockThreshold baseline candidate =
  case relativeChange baseline candidate of
    Just delta
      | delta >= blockThreshold -> [Finding Blocking code baseline candidate (candidate - baseline) (Just delta) (bounded01 (delta / blockThreshold))]
      | delta >= warnThreshold -> [Finding Warning code baseline candidate (candidate - baseline) (Just delta) (bounded01 (delta / blockThreshold))]
      | otherwise -> []
    Nothing
      | candidate > baseline -> [Finding Warning code baseline candidate (candidate - baseline) Nothing 0.5]
      | otherwise -> []

qualityFinding :: Policy -> Stats -> Stats -> [Finding]
qualityFinding policy baseline candidate =
  case (statQualityP50 baseline, statQualityP50 candidate) of
    (Just baseQuality, Just candidateQuality)
      | candidateQuality >= baseQuality -> []
      | blocking -> [mk Blocking]
      | warning -> [mk Warning]
      | otherwise -> []
      where
        absoluteDrop = baseQuality - candidateQuality
        relativeDrop = qualityRelativeDrop baseQuality candidateQuality
        warning = absoluteDrop >= warnQualityDropAbsolute policy || maybe False (>= warnQualityDropRelative policy) relativeDrop
        blocking = absoluteDrop >= blockQualityDropAbsolute policy || maybe False (>= blockQualityDropRelative policy) relativeDrop
        confidence = bounded01 (max (absoluteDrop / blockQualityDropAbsolute policy) (maybe 0 (/ blockQualityDropRelative policy) relativeDrop))
        mk severity = Finding severity QualityRegression baseQuality candidateQuality (candidateQuality - baseQuality) (relativeChange baseQuality candidateQuality) confidence
    _ -> []

statsOf :: [Observation key] -> Stats
statsOf rows = Stats
  { statCount = count
  , statErrors = errors
  , statTokens = tokens
  , statCostUsd = cost
  , statCarbonGrams = carbon
  , statLatencyP50 = quantile 0.50 latencies
  , statLatencyP95 = quantile 0.95 latencies
  , statErrorRate = rate errors count
  , statCostPerThousandTokens = perThousand cost tokens
  , statCarbonPerThousandTokens = perThousand carbon tokens
  , statQualityP50 = qualityP50
  }
  where
    count = length rows
    errors = length (filter observationFailed rows)
    tokens = sum (map observationTokens rows)
    cost = sum (map observationCostUsd rows)
    carbon = sum (map observationCarbonGrams rows)
    latencies = map observationLatencyMs rows
    qualities = mapMaybe observationQualityScore rows
    qualityP50 = if null qualities then Nothing else Just (quantile 0.50 qualities)

groupBySlice :: Ord key => [Observation key] -> Map.Map key [Observation key]
groupBySlice = foldl' insert Map.empty
  where
    insert acc row = Map.insertWith (++) (observationSlice row) [row] acc

rankReports :: [SliceReport key] -> [SliceReport key]
rankReports = sortBy compareReport

compareReport :: SliceReport key -> SliceReport key -> Ordering
compareReport left right = chainOrdering
  [ compare (reportSeverity right) (reportSeverity left)
  , compare (reportRiskScore right) (reportRiskScore left)
  , compare (statCount (reportCandidate right)) (statCount (reportCandidate left))
  ]

shouldBlock :: Analysis key -> Bool
shouldBlock = any hasBlocking . analysisReports
  where
    hasBlocking report = reportSeverity report == Blocking

severityOf :: [Finding] -> Severity
severityOf findings = maximum (Informational : map findingSeverity findings)

riskScore :: [Finding] -> Double
riskScore findings = maximum (0 : map findingScore findings)

findingScore :: Finding -> Double
findingScore finding = severityWeight (findingSeverity finding) * findingConfidence finding

severityWeight :: Severity -> Double
severityWeight Informational = 0.10
severityWeight Warning = 0.55
severityWeight Blocking = 1.00

observationTokens :: Observation key -> Double
observationTokens row = observationPromptTokens row + observationCompletionTokens row

wilsonInterval :: Int -> Int -> Interval
wilsonInterval _ 0 = Interval 0 1
wilsonInterval successes total = Interval lower upper
  where
    n = fromIntegral total
    phat = fromIntegral successes / n
    z = 1.96
    z2 = z * z
    denominator = 1 + z2 / n
    center = phat + z2 / (2 * n)
    margin = z * sqrt ((phat * (1 - phat) + z2 / (4 * n)) / n)
    lower = (center - margin) / denominator
    upper = (center + margin) / denominator

quantile :: Double -> [Double] -> Double
quantile _ [] = 0
quantile q values = quantileSorted (bounded01 q) (sort values)

quantileSorted :: Double -> [Double] -> Double
quantileSorted _ [] = 0
quantileSorted q values = lowValue + fraction * (highValue - lowValue)
  where
    len = length values
    position = q * fromIntegral (len - 1)
    lowIndex = floor position
    highIndex = ceiling position
    fraction = position - fromIntegral lowIndex
    lowValue = values !! lowIndex
    highValue = values !! highIndex

relativeChange :: Double -> Double -> Maybe Double
relativeChange baseline candidate
  | abs baseline < 1.0e-12 && abs candidate < 1.0e-12 = Just 0
  | abs baseline < 1.0e-12 = Nothing
  | otherwise = Just ((candidate - baseline) / abs baseline)

qualityRelativeDrop :: Double -> Double -> Maybe Double
qualityRelativeDrop baseline candidate
  | abs baseline < 1.0e-12 = Nothing
  | candidate >= baseline = Just 0
  | otherwise = Just ((baseline - candidate) / abs baseline)

rate :: Int -> Int -> Double
rate _ 0 = 0
rate value total = fromIntegral value / fromIntegral total

perThousand :: Double -> Double -> Double
perThousand amount tokens
  | tokens <= 0 = 0
  | otherwise = amount * 1000 / tokens

validWindow :: Window -> Bool
validWindow window = windowStartDay window <= windowEndDay window

validPolicy :: Policy -> Bool
validPolicy policy =
  minBaselineSamples policy >= 0 &&
  minCandidateSamples policy >= 0 &&
  all finite nonNegativeThresholds &&
  all (>= 0) nonNegativeThresholds &&
  blockErrorRateAbsolute policy >= warnErrorRateAbsolute policy &&
  blockLatencyRelative policy >= warnLatencyRelative policy &&
  blockCostRelative policy >= warnCostRelative policy &&
  blockCarbonRelative policy >= warnCarbonRelative policy &&
  blockQualityDropAbsolute policy >= warnQualityDropAbsolute policy &&
  blockQualityDropRelative policy >= warnQualityDropRelative policy
  where
    nonNegativeThresholds =
      [ warnErrorRateAbsolute policy
      , blockErrorRateAbsolute policy
      , warnLatencyRelative policy
      , blockLatencyRelative policy
      , warnCostRelative policy
      , blockCostRelative policy
      , warnCarbonRelative policy
      , blockCarbonRelative policy
      , warnQualityDropAbsolute policy
      , blockQualityDropAbsolute policy
      , warnQualityDropRelative policy
      , blockQualityDropRelative policy
      ]

validObservation :: Observation key -> Bool
validObservation row =
  observationDay row > 0 &&
  all finite numericValues &&
  all (>= 0) nonNegativeValues &&
  maybe True finite (observationQualityScore row)
  where
    numericValues = nonNegativeValues ++ maybe [] pure (observationQualityScore row)
    nonNegativeValues =
      [ observationPromptTokens row
      , observationCompletionTokens row
      , observationLatencyMs row
      , observationCostUsd row
      , observationCarbonGrams row
      ]

finite :: Double -> Bool
finite value = not (isNaN value) && not (isInfinite value)

bounded01 :: Double -> Double
bounded01 value
  | value < 0 = 0
  | value > 1 = 1
  | otherwise = value

chainOrdering :: [Ordering] -> Ordering
chainOrdering [] = EQ
chainOrdering (EQ:rest) = chainOrdering rest
chainOrdering (result:_) = result

{-
This solves the April 2026 problem where teams change an LLM model, provider, prompt, routing policy, fine tune, quantization setting, or edge inference path and then struggle to prove whether the new path is actually safer than the old one. Built because I wanted a small Haskell module that a serious platform engineer or researcher can drop behind a CSV loader, OpenTelemetry export, warehouse query, model evaluation job, or GitHub Actions gate without trusting a vendor dashboard or a loose spreadsheet. Use it when you need AI model drift detection, LLM rollout risk scoring, inference cost regression checks, p95 latency regression alerts, carbon aware AI telemetry, eval quality drop detection, AI gateway observability, and deterministic CI policy for production machine learning systems. The trick: it compares baseline and candidate windows by slice, uses Wilson intervals for error-rate movement, keeps p95 latency away from average hiding, normalizes spend and carbon by tokens, and treats quality drops as first class release blockers. Drop this into a Haskell service, a research pipeline, or a thin runghc wrapper and wire your own parser around the typed Observation records. I wrote it this way because Pavan would want the core decision logic to be readable, forkable, boring in the good sense, and clear enough to defend during a rollout review.
-}
