# Hourly Energy Matching Ledger

Your company says it runs on 100 percent renewable power and the certificates add up over the year. This tool checks how many of those kilowatt hours were actually covered by clean supply in the same hour, so prices the gap.

**Language:** Clojure | **Lines:** 985 | **Added:** 2026-10-07

## What this solves

Annual renewable energy certificate matching is easy to pass and hard to defend. Buy enough solar certificates over twelve months and the spreadsheet says 100 percent. Meanwhile the data centre ran all night on whatever the grid had left, so nobody can say how much carbon that was. Hourly matching, also called 24/7 carbon free energy or time based matching, closes that hole by asking a stricter question: for each hour, how much of the load was covered by certificates generated in that hour, in a region that could deliver power to the load?

Answering that for real data is not a one line SQL join. Certificates can serve several regions. A certificate can only be spent once. A load hour can only be covered once. If two regions share an interconnector and one has a surplus, the best allocation depends on everything else in the dataset. A greedy loop that walks hours in order will quietly under match, so every missed kilowatt hour shows up as extra reported emissions. This ledger computes the true optimum with a maximum flow, so the matched number is the best the inputs allow and not whatever order your CSV happened to be in.

It also catches the boring data problems that wreck these reports. Duplicate certificate ids, duplicated meter exports, half hour timestamps in an hourly file, gaps in the load series, missing emission factors and negative readings. Each one is rejected or reported with a file name and a line number.

## Why I built it

I work on carbon projects and I keep seeing the same pattern. A buyer holds a pile of certificates, a sustainability team reports an annual percentage, so an auditor or a customer asks for hourly numbers. The answer is then produced in a notebook with a loop nobody wants to review. I wanted something small enough to read in one sitting, strict enough to trust, so boring enough to run in CI.

The design choices all come from that. Energy is stored as whole watt hours in long integers, so there is no floating point drift and the same inputs always give the same bytes out. The matcher is an exact algorithm and the tests check it against a second, independent max flow written in the plainest way I could manage. Nothing is silently dropped: if a row is bad it lands in the rejects list and the exit code changes.

## When to use it

Use it when you need to:

- Report an hourly or sub hourly carbon free energy score next to an annual renewable claim.
- Show the gap between the two so finance and sustainability agree on the same numbers.
- Test a procurement plan before signing it. Add a candidate wind farm to the supply file and see how many extra matched kilowatt hours it buys.
- Gate a monthly pipeline in CI with a minimum CFE threshold.
- Compare strict hourly matching with a relaxed window, for example the same day or the same week, to see how much of your score depends on the loosest rule you can get away with.
- Audit a certificate allocation list. The output says which certificate id covered which load interval and how many kilowatt hours.

Do not use it as a registry. It does not retire certificates, issue anything or replace the rules of a specific scheme. It is the calculation layer you can put in front of those processes.

## How it works

Input is three CSV files. `parse-csv` handles quoted fields, escaped quotes, a leading byte order mark, CRLF line endings and newlines inside quotes. An unterminated quote is fatal, because otherwise the rest of the file becomes one cell. `read-table` lower cases the header, rejects duplicate or missing required columns and rejects single rows with the wrong cell count.

`ingest-load`, `ingest-supply` and `ingest-grid` turn rows into maps keyed by region and interval. Timestamps go through `parse-slot`, which needs ISO 8601 with an offset and an alignment to the interval in UTC. A reading at 00:30 in an hourly file is rejected, not rounded. Numbers go through `parse-double-field`, which only accepts plain decimals, so Java style suffixes such as `1d` and values like NaN or negative energy are rejected. Anything above the sanity caps `max-kwh-per-row` and `max-g-per-kwh` is rejected too. Kilowatt hours become integer watt hours in `parse-wh`. Rows for different meters in the same interval add up, but the same meter twice is a duplicate. A certificate id seen twice is rejected even when the rows look identical.

The policy comes from `validate-policy`. It holds `:interval-minutes` (one of `valid-intervals`), `:window-before` and `:window-after` in intervals, a `:deliverability` map and `:residual-fallback`. Deliverability says which supply regions may serve each load region. A region always serves itself. The window says how far from the load interval a certificate may have been generated. Zero and zero is strict hourly matching.

`match-supply` builds the graph. Supply nodes are region and interval pairs with the summed watt hours of their certificates. Load nodes are region and interval pairs with positive load. An edge exists only where the policy allows it. `tier-of` ranks edges: same region and same interval first, then same region other interval, then other region same interval, then everything else. The edges are activated one tier at a time and `dinic-phase!` runs to completion after each tier. Dinic is a standard maximum flow algorithm. Mine uses primitive arrays and an iterative depth first search, so a long residual path cannot overflow the stack. Because later tiers only add edges, the final flow is a true maximum. The tiers just make ties resolve toward the local, same hour pairing. If the graph would pass `max-flow-edges` the program refuses with an error instead of running out of memory.

`attribute-certs` then splits the flow leaving each supply node across its certificates in id order, which gives the audit trail.

`analyze` joins matching with the grid factors. For every load interval it computes hourly CFE, location based emissions at the grid average and market based emissions. Market based emissions are the lifecycle factor of the certificates used plus the unmatched energy at the residual mix. If the residual mix is missing the grid average is used and a warning is raised, unless the policy says `:fail`. It also builds an annual style coverage figure per region and overall, which is the number a yearly claim would report, so the gap in percentage points between that and hourly CFE. The report includes a UTC time of day profile, the ten worst unmatched intervals by emissions and warnings for load gaps, missing factors, fallback use and certificates in regions nobody accepts.

Output is JSON by default through `to-json` or plain text through `to-text`. `hourly-csv` and `alloc-csv` write detail files. `csv-cell` prefixes cells that start with a spreadsheet formula character, since certificate ids and asset names come from files you do not control. `write-atomically!` writes to a temp file and renames it, so refuses to overwrite an input.

## Usage

Run the report on the bundled sample data:

```
clojure -M HourlyEnergyMatchingLedger.clj \
  --load SampleLoad.csv --supply SampleSupply.csv --grid SampleGrid.csv \
  --policy SamplePolicy.edn --format text
```

Add a gate, plus detail files:

```
clojure -M HourlyEnergyMatchingLedger.clj \
  --load SampleLoad.csv --supply SampleSupply.csv --grid SampleGrid.csv \
  --policy SamplePolicy.edn --min-cfe 0.75 --max-rejects 0 \
  --hourly-out hourly.csv --alloc-out allocations.csv
```

Flags: `--load`, `--supply`, `--grid`, `--policy`, `--interval-minutes`, `--window-before`, `--window-after`, `--min-cfe`, `--max-rejects`, `--format json|text`, `--hourly-out`, `--alloc-out` and `--help`. Command line values override the policy file.

Input columns:

- Load: `hour,region,kwh` plus an optional `meter`.
- Supply: `certificate_id,region,hour,kwh` plus optional `asset` and `g_per_kwh`.
- Grid: `hour,region,grid_g_per_kwh` plus optional `residual_g_per_kwh`.

A policy file is one EDN map:

```
{:interval-minutes 60
 :window-before 0
 :window-after 0
 :deliverability {"DE" #{"NL"} "NL" #{"DE"}}
 :residual-fallback :grid}
```

Exit codes: 0 pass, 1 CFE below the threshold, 2 usage or fatal input error, 3 more rejected rows than `--max-rejects`. The program entry point is `-main`, so `run` does the same work without calling `System/exit`, so you can call it from a REPL or a test.

Run the tests with `clojure -M HourlyEnergyMatchingLedgerTest.clj`. They compare `match-supply` with the oracle on 150 random instances and check that no load or certificate is over used and that every allocation respects the window and the deliverability map.

## Notes

- Everything is in UTC. Convert local time before you export. Half hour zones such as India work with `--interval-minutes 30`.
- Energy resolution is one watt hour. Smaller fractions are rounded.
- Hourly matching treats surplus as lost. Certificates generated in an hour with no load to cover are reported as surplus and are not banked.
- The annual style market figure is an approximation of what a yearly claim would report: uncovered load at the residual mix, covered load at the average certificate lifecycle factor.
- Deliverability here is a policy you supply. It does not model transmission limits. If your scheme counts interconnector capacity, put that in the deliverability map and the window.
- The sample data is synthetic. Do not read a real conclusion from it.
- The ledger needs only Clojure 1.11 or later and the JDK. No other libraries.
- The tie breaking tiers affect which certificate serves which load, never the total.
