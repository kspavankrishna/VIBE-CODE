# Carbon Aware Eval Planner

Nightly AI eval suites, embedding backfills and CI batch jobs all fire at midnight, into whatever the grid happens to be doing at midnight. This is a single file Dart CLI that takes a carbon and price forecast plus a list of deadline bound jobs and returns a JSON schedule that pushes the flexible work into the cleanest and cheapest slots.

**Language:** Dart | **Lines:** 941 | **Added:** 2026-05-24

## What this solves

The default for background compute is "run now". A cron entry says `0 0 * * *` and the RAG regression eval, the embedding backfill and the pipeline compaction all start at the top of the hour. In most grids that is not the clean hour and often not the cheap hour. The work was never urgent: it has a deadline eight hours out and nobody would notice if it ran at 03:00. Nothing in the stack knows that, so it burns dirty kilowatt hours at peak price for no reason.

The usual fix is worse. Somebody staggers the cron offsets by hand. Then the eval suite grows from 60 minutes to 105 because someone added a tool calling audit, and it overruns its deadline. Nobody notices until the morning report is missing and an engineer spends an hour working out which job ate the window. Hand tuned offsets model neither duration nor deadline nor capacity, so they rot on every change. Capacity bites from the other side too: two heavy jobs land on the same GPU pool, it thrashes, both run long and both miss.

The last failure shows up in a meeting, not a log. Someone asks what the eval pipeline emits or costs and there is no number. This emits `estimatedEnergyKwh`, `estimatedCo2Grams` and `estimatedPowerCostUsd` per job and again in a summary block. An estimate, not metering, but it moves when you move the schedule.

## Why I built it

Cluster schedulers optimise for bin packing and fairness. They do not know electricity has a carbon intensity that changes hour to hour. Carbon aware SDKs go the other way and hand you an intensity number, leaving you the part that reconciles duration, deadline, capacity and priority. CI systems have no deadline model at all. Nothing in that chain answers the plain question: given these eight jobs and this forecast, what should run when.

So I wrote the planning part alone with no packages. It imports `dart:convert`, `dart:io` and `dart:math` and nothing else, so it forks cleanly from GitHub, pastes into a build runner and runs inside locked down automation where pulling a dependency just to sort jobs would be silly.

## When to use it

- A nightly GitHub Actions or Jenkins queue runs model regression tests and RAG quality checks that must finish by standup but do not care which hour.
- An embedding backfill needs 180 minutes of GPU time before an 11:30 cutoff and you want it in the overnight trough.
- You have a WattTime, Electricity Maps or internal grid forecast and no code that turns it into an actual job plan.
- A Dart backend or Flutter operations console must show engineers when batch work runs and what it emits.
- Finance or a sustainability report needs per job kWh, CO2 and dollar estimates for background compute.
- Serverless or edge batch workers with soft deadlines that currently just fire on a timer.

## How it works

Input is one JSON object with `options`, `forecast` and `workloads`, validated up front by `CarbonAwareEvalPlanner.fromJson`. Every field is range checked: intensity 0 to 2500 gCO2/kWh, confidence 0 to 1, priority 0 to 100, watts above zero and under a million. Workload ids are matched against `_safeId`, a regex capping length at 160 characters and restricting the character set, so ids from an untrusted queue cannot smuggle anything into the output. Timestamps must carry a timezone: `_dateField` checks `_utcSuffix` first, so a naive `2026-04-15T00:00:00` is rejected rather than read as local time.

`_expandForecast` turns forecast points into a uniform grid. Each point is chopped into `slotMinutes` chunks by `ForecastPoint.expand`, keyed into a map by `millisecondsSinceEpoch` and sorted by start, so overlapping points collapse to one slot per instant. From there the problem is an indexed array of `ForecastSlot`, shadowed by `_PlanningState` as a parallel `List<int>` of used counts. Only `hasCapacity` and `claim` touch it, and `claim` throws `PlannerException` if asked to overbook, turning an internal bug into a loud failure instead of a corrupt plan.

Jobs are placed greedily in the order set by `_compareWorkloadOrder`: least slack first, then earliest deadline, then highest priority, then longest duration, then id. Least slack first is the right cut: a job with 30 spare minutes has almost no choice about where it goes, one with six spare hours absorbs whatever is left. Each candidate slot is priced by `ForecastSlot.metricsFor`, where the policy lives. Energy is `averageWatts * assignedMinutes / 60000` and the score sums five terms: carbon using `effectiveCarbon`, which inflates raw intensity by `(1 - confidence) * lowConfidenceCarbonPremium` so a shaky far out forecast loses to a confident near term one; price scaled by `priceWeight`; a monetised carbon term from `carbonPriceUsdPerTonne`; an occupancy penalty of `alreadyUsed * occupancyPenalty` that spreads work off busy slots; and an urgency nudge of `(101 - priority) * 0.0001`.

Placement splits on `preemptible`. `_schedulePreemptible` scores every feasible slot in the window, sorts ascending by score with start time as tiebreak, and takes the cheapest N where N is `_ceilDiv(durationMinutes, slotMinutes)`. It ignores adjacency, so a 105 minute eval can spread across disjoint clean pockets. `_scheduleContiguous` slides a fixed width window of N slots across the grid instead, rejecting any that breaks the earliest or deadline bounds or hits a full slot, and keeps the best by `_CandidateBlock.compareTo`: score, then CO2, then earliest start. Adding `contiguityPenalty` to that score says what a job will pay for an unbroken run. Whatever does not fit gets an `UnscheduledWorkload` carrying a reason from `_unscheduledReason`.

## Usage

```bash
# see the CLI contract and the full input shape
dart CarbonAwareEvalPlanner.dart --help

# emit a working two workload request you can edit
dart CarbonAwareEvalPlanner.dart --example > request.json

# plan, then branch on the exit code
dart CarbonAwareEvalPlanner.dart < request.json > plan.json
case $? in
  0) echo "all workloads scheduled" ;;
  2) echo "some unscheduled"; jq '.unscheduled' plan.json ;;
  *) echo "input or planner error"; exit 1 ;;
esac
```

Calling it in process uses the same two steps `main` does:

```dart
final planner = CarbonAwareEvalPlanner.fromJson(jsonDecode(rawJson));
final PlanResult plan = planner.plan();
for (final job in plan.scheduled) {
  print('${job.id} ${job.start} -> ${job.end}  ${job.estimatedCo2Grams} gCO2');
}
```

## Notes

- Exit codes carry meaning. 0 means every workload was placed, 2 means the plan is valid but something went unscheduled, 64 means empty stdin, 65 means validation failed and 70 means an internal planner error. Exit 2 is a signal, not a crash.
- Greedy and single pass. Nothing reallocates once a job claims slots, so you get a good plan and not a proven optimum. The contiguous search is O(slots x slotsNeeded) per job: fine for a day of 15 minute slots, slow for a week at 1 minute.
- Region is recorded and reported but never constrained. A preemptible job can spread across regions, and since slots dedupe by timestamp, two regions sharing a start time collapse into one. Shard the input if that matters.
- `estimatedCo2Grams` uses the raw `carbonGramsPerKwh`. The low confidence premium bends ranking through `effectiveCarbon` only, it deliberately does not inflate reported emissions.
- For preemptible work, `start` and `end` span first to last assignment, so wall clock span can far exceed `requestedMinutes`. This produces a plan only. Pausing and resuming is your runner's problem.
- Two warts. `_compareWorkloadOrder` calls `slackMinutes(15)` with a hardcoded 15 whatever your `slotMinutes` is, which shifts consideration order but never feasibility, and `occupancyPenalty` is a real option defaulting to 0.015 that `--help` never mentions. `generatedAt` uses `DateTime.now().toUtc()`, so output is not byte identical between runs.
