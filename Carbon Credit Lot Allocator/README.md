# Carbon Credit Lot Allocator

An Ada command line tool that hands out carbon credit serial ranges from issued lots to buyer orders. Every tonne goes to exactly one order, nothing is sold twice and no fraction of a tonne is lost to rounding.

**Language:** Ada | **Lines:** 818 | **Added:** 2026-10-25

## What this solves

If you run a carbon credit desk, a registry integration or a retirement service, you eventually face the same boring and expensive question. You hold a few lots of credits, each one a block of serial numbers with a vintage year. Several buyers want tonnes. Who gets which serials?

Most people answer that in a spreadsheet. The spreadsheet divides, rounds and moves on. Then the totals are off by a tonne, or two orders end up holding the same serial, or a buyer who asked for vintage 2022 or newer gets a 2019 block. Each of those is a real problem. A double sold serial is a double counting claim. A wrong vintage is a rejected claim at the buyer's auditor. A missing tonne is a reconciliation ticket that takes a week.

This tool treats the allocation as an integer problem with hard invariants. You give it lots and orders as plain CSV. It gives you back exact fills: order, lot, vintage, first serial, last serial and tonnes. It then checks its own answer before it prints anything.

Search terms that should lead here: carbon credit allocation, carbon credit serial number ranges, VCS and Gold Standard lot allocation, pro rata allocation with largest remainder, Hamilton apportionment, carbon credit buffer pool withholding, FIFO retirement of carbon credits, Ada CLI.

## Why I built it

I work around carbon projects and I keep seeing the same failure. The credit data is clean. The allocation logic is a loose script that uses floating point for a job that is pure counting. Once a project has a buffer pool requirement and three buyers asking for more than exists, the rounding bugs show up.

I also wanted a tool that fails loudly. Plenty of allocation scripts will happily accept overlapping serial ranges and produce a plausible looking result. Here an overlap is an input error and the run stops with the line number.

I wrote it in Ada because the job is about types, ranges and checked arithmetic. Integer overflow raises an exception in Ada. Numbers are range checked. That fits a program whose whole value is that the totals are right. It compiles to a single static binary with GNAT and has no dependencies beyond the standard library.

## When to use it

- You need to split issued credits across buyer orders and keep a serial level audit trail.
- Demand is higher than supply and you need a fair split that adds up to the exact supply.
- A project must keep a buffer pool share of every lot and you want that share taken from a known place.
- You want a CI step or a cron job that rejects bad lot data before it reaches a registry.
- You want deterministic output. The same input always gives the same bytes out, so you can diff two runs.

Do not use it as a registry. It does not talk to Verra, Gold Standard or any other system. It does not know whether a serial is already retired somewhere else. It allocates within the lots you give it.

## How it works

**Input.** One record per line. Lines starting with `#` are comments and blank lines are skipped. CRLF line endings are accepted. A UTF-8 byte order mark is rejected with a clear message, since it would silently corrupt the first record type. Quoted fields are rejected because ids and numbers never need quotes.

- `LOT,<id>,<vintage>,<first_serial>,<last_serial>`
- `ORDER,<id>,<tonnes>[,<min_vintage>[,<priority>]]`

One serial is one tonne and ranges are inclusive. Ids use letters, digits, dot, dash and underscore, up to 64 characters, enforced by `Valid_Id`. That is also why the JSON writer never needs string escaping.

**Parsing and limits.** `Parse_Number` accepts digits only, at most 18 of them, then range checks the value against explicit bounds. Serials stop at `Max_Serial`, vintages run from `Min_Vintage` (1990) to `Max_Vintage` (2100), priority tops out at `Max_Priority` (1000). Total supply and total demand are each capped at `Max_Total_Units` (2 billion tonnes). That cap is not arbitrary. It guarantees the largest product in the pro rata step stays under 4e18, which fits in a 64 bit `Long_Long_Integer` without overflow. Duplicate lot ids and duplicate order ids are caught through a hashed set in `Check_Id`.

**Overlap rejection.** `Reject_Overlapping_Lots` sorts the lots by first serial and compares neighbours. If a lot starts at or before the previous lot's last serial, the run stops with both lot ids and both ranges in the message.

**Buffer withholding.** `--buffer-bps N` takes N basis points of every lot, rounded up, off the tail of the range. `Apply_Buffer` computes `(Size * Buffer_Bps + 9999) / 10000`. Rounding up means the pool is never short by a fraction. The limit is 5000 basis points. The withheld serials are never handed to an order, and the report shows them per lot.

**Pro rata entitlements.** With `--policy prorata`, `Compute_Entitlements` runs when demand exceeds supply. It uses the largest remainder method, also called Hamilton apportionment. Each order first gets `quantity * supply / demand` rounded down. The leftover tonnes then go one at a time to the orders with the biggest remainders. Ties go to the earlier line in the file. The shares always add up to exactly the supply. If demand fits inside supply, everyone is entitled to everything they asked for. With `--policy fifo` entitlement equals the request.

**Drawing tonnes.** `Allocate` sorts lots by vintage, oldest first by default or newest first with `--vintage newest`. Orders are processed by priority, highest first. Among equal priorities, the order with the higher `min_vintage` goes first, because it has fewer lots to choose from. The inner `Draw` procedure walks the sorted lots, skips any lot older than the order's minimum vintage and takes contiguous serials from each lot's cursor. A single order can span several lots, which produces one fill line per lot.

**Self check.** `Self_Check` re-verifies the result before output. Each fill must sit inside its lot's available range. Fills inside one lot must never reuse a serial. Order totals, lot totals and fill totals must all match. No order may receive a vintage below its minimum. If any of that fails, the program raises `Check_Error`, prints `internal check failed` and exits with code 1. You should never see it, and if you do, do not trust the output.

**Known limit.** Greedy draw by priority is not a full matching solver. If a high priority order with a loose vintage rule takes the only lot that a stricter order could use, the stricter order is short even though a different split could have served both. The shortfall is shown in the report and the unused remainder stays visible in the lot summary, so you can spot it. I chose a predictable and explainable rule over a clever one.

**Exit codes.** 0 for success, 1 for a failed internal check, 2 for bad input or usage, 3 for a shortfall when `--fail-on-shortfall` is set.

## Usage

Build with GNAT. The compiler warns that the file name does not match the unit name because the file uses PascalCase. That warning is harmless.

```
gnatmake -gnat2012 -gnata -gnato -O2 -o carboncreditlotallocator CarbonCreditLotAllocator.adb
```

Make an input file called `lots.csv`:

```
# lots then orders
LOT,VCS-2021-A,2021,1000,1999
LOT,VCS-2023-B,2023,5000,5499
LOT,GS-2022-C,2022,9000,9099
ORDER,acme,600,2021,5
ORDER,globex,700,2023
ORDER,initech,400
```

Run it:

```
./carboncreditlotallocator lots.csv
./carboncreditlotallocator --policy prorata --buffer-bps 1000 --vintage newest --json lots.csv
cat lots.csv | ./carboncreditlotallocator --fail-on-shortfall -
```

Options: `--policy fifo|prorata`, `--vintage oldest|newest`, `--buffer-bps N`, `--json`, `--fail-on-shortfall`, `--help`. A dash as the file name reads stdin.

The CSV report prints a `POLICY` line, then `FILL` lines with order, lot, vintage, first serial, last serial and tonnes. After that come `ORDER` lines with requested, entitled, allocated and shortfall, `LOT` lines with size, withheld, allocated and remaining, a `TOTAL` line and a final `STATUS` line that reads `FILLED` or `SHORTFALL`.

For the sample above the FIFO run gives acme serials 1000 to 1599, globex serials 5000 to 5499 and initech serials 1600 to 1999. Globex asked for 700 tonnes of vintage 2023 or newer, only 500 exist, so it shows a shortfall of 200 and the run reports `SHORTFALL`.

## Notes

- Everything is integer arithmetic. There is no floating point anywhere in the file.
- The sort uses explicit tie breaking on the original line index, so output does not depend on the sort algorithm.
- Memory is allocated once on the heap for up to 50,000 lots and 50,000 orders.
- Error messages carry the line number of the bad record.
- I tested it with a few hundred random inputs across both policies, both vintage orders and several buffer sizes, and every run passed its own self check. I also ran it with overflow checks and stack checks on.
- It does not read live registry data and it does not retire anything. Pair it with your registry's own retirement step.
- Possible next steps: a max flow matcher for the strict vintage case, and a per buyer price column.
