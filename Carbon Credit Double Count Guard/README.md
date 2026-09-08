# Carbon Credit Double Count Guard

The same tonne of CO2 gets retired, sold or claimed twice because nobody compared serial numbers before the money moved. This is a single file PHP 8 auditor that treats every carbon credit claim as a serial number interval and catches the overlap before the deal closes.

**Language:** PHP | **Lines:** 1096 | **Added:** 2026-09-08

## What this solves

Registries do not issue carbon credits as a number. Verra, Gold Standard, ACR, CAR, GCC and Puro Earth issue them as serial number ranges bound to a project, a vintage year and a unit type. A retirement of 500 VCUs is really a claim on serials 1000000 through 1000499 of project 1529, vintage 2021. Double counting happens when two of those ranges quietly overlap and nothing in the chain ever puts them side by side. One broker retires 1000000-1000999 in November. Another desk sells 1000500-1001200 in February. Both trades look fine on their own invoice. 500 credits were sold twice, and nobody finds out until an auditor or a registry reconciliation says so.

Without a check like this the control is a human opening a registry portal and eyeballing numbers. That works until the batch has forty rows. Then the failure modes stack up: a declared quantity of 500 on a range that holds 701 credits, a vintage year that has not happened yet because someone typed 2027 instead of 2017, the same claim_id submitted twice because a retry fired on a timeout, an old claim resubmitted months later against a ledger nobody re-reads. Each one is boring, and each one costs money at six and seven figure sizes.

The overlap is invisible at the row level. Every individual claim validates. The problem exists only in the relationship between two claims, and between a new claim and every claim you already accepted. That check gets skipped in hand rolled intake forms because writing it properly means holding a persistent history and doing interval math against it. Who notices: the counterparty who bought the second copy, the registry that rejects the filing, the buyer's auditor during assurance. By then the credits are spent and the refund conversation is a legal one.

## Why I built it

I run a carbon project and outreach business, and I got tired of watching "just check the registry manually" be the entire control on transactions that size. The tooling that exists is either registry side, so it only sees its own book, or a full trading platform you have to adopt wholesale. Nothing sat in between.

The gap is narrow: nobody had packaged interval overlap detection over registry serial ranges, with a persistent ledger, as something you wire into an intake pipeline or a CI job in an afternoon. So this has zero dependencies, no composer, no database, and works as both a CLI and a library you can `require`.

## When to use it

- Before filing a retirement with a registry, to confirm the range you are about to burn has not already been retired by your own desk.
- During due diligence on credits offered by a new counterparty, where you have their serial ranges and your own history of what you hold.
- As a CI gate on the data export that feeds accounting, so a double sold range fails the build instead of failing an audit six months later.
- Inside a Laravel or Symfony intake controller, called as a library, to reject a batch that collides with itself before it hits the database.
- Reconciling two spreadsheets after a merger or a broker handover, when you inherit somebody else's claim history and need to know if it contradicts yours.
- Catching mundane bulk upload errors: quantity that disagrees with range size, impossible vintages, resubmitted claim ids.

## How it works

The unit of risk is `CcSerialRange`: registry, project id, vintage year, unit type, start and end. It parses either the compact form `REGISTRY:PROJECT:VINTAGE:UNIT:START-END` through a single anchored regex, or six explicit JSON fields. Registry codes go through `CcRegistry::normalize`, which folds aliases so `VCS` and `VERRA` are the same book, and unknown codes pass through uppercased rather than being rejected. The class exposes `groupKey()`, joining registry, project, vintage and unit into one string. Two ranges can only collide if their group keys match, and that idea drives every optimisation here. `overlaps()` is the standard interval test, `overlapQuantity()` returns `min(end) - max(start) + 1`.

`CcLedger` holds previously accepted claims in `array<string, list<CcLedgerEntry>>` keyed by that group key, plus a hash set of seen claim ids. A lookup only scans entries sharing the new claim's coordinates, so a ledger spread over many projects and vintages stays fast as it grows instead of degrading into a full scan.

Within a single incoming batch it uses a sweep line. `CcBatchOverlapScanner::scan` buckets claims by group key, sorts each bucket by range start, then walks it once tracking a running `activeEnd`. Ranges starting at or before `activeEnd` accumulate into a cluster; anything past it flushes the cluster and begins a new one. Only inside a cluster does it expand to pairwise comparison. The sort is O(n log n), the sweep is O(n), and the quadratic step stays confined to genuinely overlapping groups instead of the naive all pairs O(n squared) most people write first.

`CcAuditor::audit` runs both passes and emits `CcFinding` objects on a five step ladder from `info` to `critical`, ranked by `CcSeverity`. Ledger overlaps emit `double_count_vs_ledger` at critical. Batch overlaps emit `double_count_within_batch` at critical twice, once for each side, so both claim ids carry the finding. Alongside those: `quantity_mismatch`, `duplicate_claim_id_in_batch` and `future_vintage` at high, `implausible_vintage` (before `MIN_VINTAGE_YEAR`, 1996) and `claim_id_already_ledgered` at medium. `CcAuditResult::addFinding` maps severity to a per claim decision that only ever escalates: high or above turns the claim into `reject`, anything lower into `review`. When a claim carries `price_usd_per_credit`, overlapping quantity times price accumulates into `exposureCents`, giving you the dollar value at risk. Money is integer cents throughout via `CcMoney`, never floats.

Persistence is `CcLedgerWriter::commit`. It opens the ledger with `c+`, takes an exclusive `flock`, merges existing rows keyed by claim id, writes to a temp file named with the pid plus random bytes, then renames it into place, so a crash mid write leaves the old ledger intact rather than truncated JSON. `CcReportRenderer` emits pretty printed JSON or Markdown. `CcSelfTest` carries nine regression checks including a disk round trip of the writer and a check that identical serials in different registries never collide.

## Usage

```bash
# see the embedded regression checks pass
php CarbonCreditDoubleCountGuard.php --self-test

# print starter ledger.json and claims.json to copy
php CarbonCreditDoubleCountGuard.php --example

# audit a batch against a ledger, JSON report
php CarbonCreditDoubleCountGuard.php --ledger ledger.json --claims claims.json

# human readable report instead
php CarbonCreditDoubleCountGuard.php --ledger ledger.json --claims claims.json --format markdown

# CI gate: audit, persist non rejected claims, exit 2 if anything is high or worse
php CarbonCreditDoubleCountGuard.php \
  --ledger ledger.json \
  --claims claims.json \
  --commit \
  --fail-on high
```

Claim JSON, either compact or explicit:

```json
[
  {
    "claim_id": "batch-2026-0101",
    "serial_range": "VERRA:1529:2021:VCU:1000500-1001200",
    "action": "retire",
    "declared_quantity": 701,
    "counterparty": "Northwind Carbon Desk",
    "price_usd_per_credit": 8.75
  }
]
```

As a library, the bottom guard checks `realpath($argv[0]) === __FILE__` so requiring the file does not run the CLI:

```php
require __DIR__ . '/CarbonCreditDoubleCountGuard.php';

$ledger = CcLedger::loadFromFile('/var/data/ledger.json');
$claims = array_map(fn(array $r) => CcClaim::fromArray($r), $rows);
$result = (new CcAuditor($ledger))->audit($claims);

if ($result->decisionFor('batch-2026-0101') === 'reject') {
    echo CcReportRenderer::renderMarkdown($result);
}
```

## Notes

- Exit codes: 0 clean, 2 when `--fail-on` threshold is met or exceeded, 1 on a runtime error or a failed `--self-test`, 64 on a bad command line argument.
- `--format` accepts `json` or `markdown` only. The built in help text abbreviates it as `md`, which the parser rejects. Use the full word.
- `--commit` persists every claim whose decision is not `reject`, so claims marked `review` still land in the ledger. Medium severity findings do not block a commit. Gate on the JSON report first if you want them held back.
- The ledger is one JSON file loaded fully into memory. Fine for tens of thousands of entries. It is not a database, there is no pagination and no on disk index.
- `flock` plus rename protects against interleaved and half written files, but the rename replaces the inode the lock was taken on. Safe for concurrent CLI runs, not a distributed lock on a network filesystem.
- Fully offline. It never calls a registry API, so it cannot confirm a serial range exists, that the project is real, or that the registry agrees with your record. It compares what you feed it against what you already accepted.
- Out of scope by design: corresponding adjustments under Article 6, national inventory reconciliation, credit quality or additionality assessment, and partial retirement accounting beyond interval overlap. It answers one question, which is whether two claims touch the same serials.
