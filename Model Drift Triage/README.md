# Model Drift Triage

You swap an LLM model, provider, prompt or routing policy and the aggregate dashboard looks fine, but one customer slice is now failing twice as often and costing 40 percent more per thousand tokens. This is a Haskell module that compares a baseline window against a candidate window per slice and returns a typed, deterministic verdict you can gate a rollout on.

**Language:** Haskell | **Lines:** 445 | **Added:** 2026-07-07

## What this solves

Model drift in production is rarely one clean signal. You ship a new model version, a quantized build, a fine tune, a different provider or a new edge inference path, and the damage lands unevenly. Global error rate moves from 0.8 percent to 1.1 percent and nobody blinks. Underneath that average the Japanese locale slice went from 0.5 percent to 6 percent, the long context slice went from p95 latency 1.2s to 3.4s, and cost per thousand tokens on summarization doubled because the new model is chattier on completions. Averages hide it, and so do vendor dashboards.

The second failure mode is the argument that follows. Somebody says the new model is worse. Somebody else says the sample was small and it is noise. Both are guessing because nobody wrote down what counts as a regression. This module makes the rule explicit: a `Policy` record with warn and block thresholds for error rate, p95 latency, cost per thousand tokens, carbon per thousand tokens and quality drop. Same inputs, same policy, same answer.

Cost and carbon regressions are the ones nothing watches. Latency and errors have pagers. A model that quietly emits 30 percent more completion tokens has no pager, it has a bill at month end. Normalizing spend and emissions by tokens catches that on day one of a canary, not day thirty. Quality is the same story: eval scores get checked once during model selection and never again, so a quality drop here is a first class blocking finding.

## Why I built it

Vendor observability tools report their aggregate traffic, not your slices, and they decide for you what a regression means. Generic drift libraries are heavy, aimed at feature distributions rather than the operational reality of an inference path, and awkward to run as a hard gate because the output is a chart rather than a boolean. I wanted the decision logic small enough to read in one sitting: pure functions, no IO, no network, no config format to learn.

Haskell because the shape of the answer matters more than the plumbing. `analyze` returns `Either TriageError (Analysis key)`, the slice key is polymorphic so you pick your own dimension, and every finding carries baseline value, candidate value, deltas and a confidence number.

## When to use it

- A canary is running and you need a yes or no on promoting the new model before the on call window ends.
- You moved traffic to another provider and want proof per locale, per tenant or per prompt template.
- A CI job should fail the build when the nightly eval shows a quality or cost regression on any slice.
- Finance flagged an inference bill jump and you want the slice that changed cost per thousand tokens.
- You report carbon per thousand tokens for an edge inference path and need regression checks on it.
- A colleague says the new model is worse and you want it settled by a written policy.

## How it works

`analyze` takes a `Policy`, a baseline `Window`, a candidate `Window` and a list of `Observation key` records, and it fails closed. `validPolicy` rejects negative or non finite thresholds and any block threshold below its warn threshold, `validWindow` rejects an inverted range, and `validObservation` rejects a non positive day, a NaN or infinite number, or a negative token, latency, cost or carbon value. Empty baseline or candidate selections are errors, not silently empty reports. Every failure comes back as a `TriageError` in `Left`.

Rows are selected with `rowsInWindow`, an inclusive day filter on `observationDay`, then bucketed by `groupBySlice` into a strict `Data.Map` keyed on `observationSlice`. The slice list is the sorted union of baseline and candidate keys, so a slice present on only one side still gets a report. For each slice `statsOf` builds a `Stats` record: count, errors, total tokens (prompt plus completion), cost, carbon, p50 and p95 latency, error rate, cost per thousand tokens, carbon per thousand tokens and an optional median quality score. Quality is `Maybe Double` per row, so a slice with no scored samples yields `Nothing` and skips the quality check rather than inventing a number. `quantile` sorts and linearly interpolates between neighbouring order statistics. Latency is judged on p95 deliberately: the mean hides tail blowups, and tail blowups are what users feel.

`findingsFor` produces the `Finding` list. A missing side gives an immediate `Blocking` `MissingBaseline` or `MissingCandidate`. `sampleFindings` adds a `Warning` `LowSample` when either side falls under `minBaselineSamples` or `minCandidateSamples` (both 50 in `defaultPolicy`), a caveat rather than a block. `errorFinding` is the statistically careful one. It fires only once the absolute rate delta clears the warn threshold, then computes a Wilson score interval on both sides with z of 1.96 via `wilsonInterval`. Wilson rather than the normal approximation because error rates live near zero at small counts, exactly where the naive interval breaks or runs past the end of the range. Non overlapping intervals give the finding confidence 1, otherwise confidence scales with how far the delta moved toward the block threshold. Severity escalates to `Blocking` on the absolute block threshold or on a doubling rule: candidate rate at or above twice baseline plus the warn threshold, which catches a 0.4 to 1.5 percent jump an absolute threshold alone would shrug off.

Latency, cost and carbon share `relativeMetricFinding`, a ratio check against warn and block thresholds. `relativeChange` guards a near zero baseline: both sides zero gives 0, a zero baseline against a nonzero candidate gives `Nothing` and is emitted as a `Warning` at confidence 0.5 instead of an infinite ratio. `qualityFinding` checks absolute and relative drop against separate thresholds and takes the worse of the two.

Each report carries `reportSeverity`, the maximum severity across its findings, and `reportRiskScore`, the maximum of `severityWeight` times `findingConfidence` with weights 0.10, 0.55 and 1.00. `rankReports` sorts descending by severity, then risk score, then candidate sample count. `shouldBlock` is the gate: true if any report is `Blocking`.

## Usage

No CLI and no parser here. It is a library module. Wire your own loader around the `Observation` constructor and call `analyze`.

```haskell
import ModelDriftTriage

rows :: [Observation String]
rows =
  [ Observation
      { observationSlice = "locale:ja"
      , observationDay = 12
      , observationPromptTokens = 820
      , observationCompletionTokens = 240
      , observationLatencyMs = 1180
      , observationFailed = False
      , observationCostUsd = 0.0043
      , observationQualityScore = Just 0.87
      , observationCarbonGrams = 0.21
      }
  -- one row per request, from your CSV, warehouse query or OTel export
  ]

main :: IO ()
main =
  case analyze defaultPolicy (Window 1 7) (Window 8 14) rows of
    Left err -> print err
    Right analysis -> do
      mapM_ print (analysisReports analysis)   -- already ranked worst first
      if shouldBlock analysis
        then putStrLn "BLOCK rollout"
        else putStrLn "OK to promote"
```

Tighten the gate by overriding `defaultPolicy` fields:

```haskell
strictPolicy :: Policy
strictPolicy = defaultPolicy
  { blockErrorRateAbsolute = 0.02
  , blockLatencyRelative = 0.30
  , minCandidateSamples = 200
  }
```

Lower level helpers are exported for your own reports and tests: `rowsInWindow`, `statsOf`, `quantile`, `wilsonInterval`, `relativeChange` and `rankReports`.

## Notes

- Pure module, no IO. Needs only `base` and `containers`. No `main`, no CSV or JSON parsing, no output formatting. Bring your own loader and printer.
- `observationDay` is a plain `Int` day index you define, greater than zero. No timezone handling. Windows are inclusive at both ends and may overlap, in which case rows count on both sides.
- Validation runs over every row you pass, not just rows inside a window, so one bad row anywhere fails the whole call with `InvalidObservation`.
- Only error rate gets an interval based check. Latency, cost, carbon and quality are threshold comparisons on point estimates, so `LowSample` is your signal that a slice is thin. `LowSample` is a `Warning` and never makes `shouldBlock` true on its own.
- Improvements are not reported. A slice that got faster or cheaper produces no finding. This detects regressions, not change in general.
- `quantile` sorts then indexes with `!!`, fine per slice and slow if you hand one slice millions of rows. `statLatencyP50` is exposed but never gated.
- Cost and carbon grams are whatever you put in. The module normalizes by tokens and compares windows, it does not source emission factors or price lists.
