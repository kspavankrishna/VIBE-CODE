<?php
declare(strict_types=1);

/**
 * CarbonCreditDoubleCountGuard
 *
 * A dependency-free PHP 8 CLI + library that audits batches of carbon
 * credit retirement/transfer/issuance claims against a persistent ledger
 * and against each other, to catch double counting before money moves.
 */

final class CcException extends RuntimeException
{
}

final class CcSeverity
{
    private const RANK = [
        'info' => 0,
        'low' => 1,
        'medium' => 2,
        'high' => 3,
        'critical' => 4,
    ];

    public static function normalize(string $value): string
    {
        $value = strtolower(trim($value));
        if (!array_key_exists($value, self::RANK)) {
            throw new CcException("unknown severity: $value");
        }
        return $value;
    }

    public static function rank(string $value): int
    {
        return self::RANK[self::normalize($value)];
    }

    public static function worst(string $a, string $b): string
    {
        return self::rank($a) >= self::rank($b) ? self::normalize($a) : self::normalize($b);
    }

    public static function atLeast(string $actual, string $floor): bool
    {
        return self::rank($actual) >= self::rank($floor);
    }
}

final class CcMoney
{
    public static function centsFromUsd(float|int|string $usd): int
    {
        if (is_string($usd)) {
            $usd = trim($usd);
            if ($usd === '') {
                return 0;
            }
            $usd = (float)$usd;
        }
        return (int)round(((float)$usd) * 100.0);
    }

    public static function centsToUsd(int $cents): string
    {
        $negative = $cents < 0;
        $cents = abs($cents);
        $whole = intdiv($cents, 100);
        $fraction = str_pad((string)($cents % 100), 2, '0', STR_PAD_LEFT);
        $text = $whole . '.' . $fraction;
        return $negative ? '-' . $text : $text;
    }
}

final class CcRegistry
{
    private const ALIASES = [
        'VCS' => 'VERRA',
        'VERRA' => 'VERRA',
        'GS' => 'GOLD_STANDARD',
        'GOLD-STANDARD' => 'GOLD_STANDARD',
        'GOLDSTANDARD' => 'GOLD_STANDARD',
        'ACR' => 'ACR',
        'AMERICAN-CARBON-REGISTRY' => 'ACR',
        'CAR' => 'CAR',
        'CLIMATE-ACTION-RESERVE' => 'CAR',
        'GCC' => 'GCC',
        'GLOBAL-CARBON-COUNCIL' => 'GCC',
        'PURO' => 'PURO',
        'PURO-EARTH' => 'PURO',
        'ART' => 'ART_TREES',
        'ART-TREES' => 'ART_TREES',
    ];

    public static function normalize(string $value): string
    {
        $key = strtoupper(str_replace(['_', ' '], '-', trim($value)));
        if ($key === '') {
            throw new CcException('registry code must not be empty');
        }
        return self::ALIASES[$key] ?? str_replace('-', '_', $key);
    }
}

/**
 * A serial range is the unit of double-counting risk: a contiguous block
 * of serial numbers minted by one registry for one project, one vintage
 * year, and one unit type (e.g. VCU, GS VER, CRT). Two ranges can only
 * collide if they share all three of those coordinates.
 */
final class CcSerialRange
{
    private const SERIAL_PATTERN = '/^([A-Za-z0-9_]+):([A-Za-z0-9._\-]+):(\d{4}):([A-Za-z0-9_]+):(\d+)-(\d+)$/';

    public function __construct(
        public readonly string $registry,
        public readonly string $projectId,
        public readonly int $vintageYear,
        public readonly string $unitType,
        public readonly int $start,
        public readonly int $end,
    ) {
        if ($this->end < $this->start) {
            throw new CcException("serial range end ({$this->end}) precedes start ({$this->start})");
        }
        if ($this->start < 0) {
            throw new CcException('serial range start must be non-negative');
        }
    }

    public static function fromSerialString(string $serial): self
    {
        if (!preg_match(self::SERIAL_PATTERN, trim($serial), $m)) {
            throw new CcException("cannot parse serial range: $serial (expected REGISTRY:PROJECT:VINTAGE:UNIT:START-END)");
        }
        return new self(
            CcRegistry::normalize($m[1]),
            $m[2],
            (int)$m[3],
            strtoupper($m[4]),
            (int)$m[5],
            (int)$m[6],
        );
    }

    public static function fromArray(array $data): self
    {
        if (isset($data['serial_range']) && is_string($data['serial_range'])) {
            return self::fromSerialString($data['serial_range']);
        }
        foreach (['registry', 'project_id', 'vintage_year', 'unit_type', 'serial_start', 'serial_end'] as $field) {
            if (!array_key_exists($field, $data)) {
                throw new CcException("missing field '$field' and no 'serial_range' string was given");
            }
        }
        return new self(
            CcRegistry::normalize((string)$data['registry']),
            (string)$data['project_id'],
            (int)$data['vintage_year'],
            strtoupper((string)$data['unit_type']),
            (int)$data['serial_start'],
            (int)$data['serial_end'],
        );
    }

    public function quantity(): int
    {
        return $this->end - $this->start + 1;
    }

    public function groupKey(): string
    {
        return implode('|', [$this->registry, $this->projectId, (string)$this->vintageYear, $this->unitType]);
    }

    public function overlaps(self $other): bool
    {
        return $this->groupKey() === $other->groupKey()
            && $this->start <= $other->end
            && $other->start <= $this->end;
    }

    public function overlapQuantity(self $other): int
    {
        if (!$this->overlaps($other)) {
            return 0;
        }
        return min($this->end, $other->end) - max($this->start, $other->start) + 1;
    }

    public function label(): string
    {
        return sprintf(
            '%s:%s:%d:%s:%d-%d',
            $this->registry,
            $this->projectId,
            $this->vintageYear,
            $this->unitType,
            $this->start,
            $this->end,
        );
    }

    public function toArray(): array
    {
        return [
            'registry' => $this->registry,
            'project_id' => $this->projectId,
            'vintage_year' => $this->vintageYear,
            'unit_type' => $this->unitType,
            'serial_start' => $this->start,
            'serial_end' => $this->end,
        ];
    }
}

final class CcClaim
{
    private const ACTIONS = ['issue', 'transfer', 'retire'];

    public function __construct(
        public readonly string $claimId,
        public readonly CcSerialRange $range,
        public readonly string $action,
        public readonly ?int $declaredQuantity,
        public readonly ?string $counterparty,
        public readonly ?int $priceCentsPerCredit,
    ) {
        if (!in_array($this->action, self::ACTIONS, true)) {
            throw new CcException("unknown action '{$this->action}' (expected one of: " . implode(', ', self::ACTIONS) . ')');
        }
    }

    public static function fromArray(array $data): self
    {
        $claimId = (string)($data['claim_id'] ?? '');
        if ($claimId === '') {
            throw new CcException('claim is missing a non-empty claim_id');
        }
        $range = CcSerialRange::fromArray($data);
        $price = null;
        if (isset($data['price_usd_per_credit'])) {
            $price = CcMoney::centsFromUsd((string)$data['price_usd_per_credit']);
        }
        return new self(
            $claimId,
            $range,
            strtolower((string)($data['action'] ?? 'retire')),
            isset($data['declared_quantity']) ? (int)$data['declared_quantity'] : null,
            isset($data['counterparty']) ? (string)$data['counterparty'] : null,
            $price,
        );
    }
}

final class CcFinding
{
    public function __construct(
        public readonly string $severity,
        public readonly string $code,
        public readonly string $claimId,
        public readonly string $message,
        public readonly array $evidence = [],
    ) {
        CcSeverity::normalize($severity);
    }

    public function toArray(): array
    {
        return [
            'severity' => $this->severity,
            'code' => $this->code,
            'claim_id' => $this->claimId,
            'message' => $this->message,
            'evidence' => $this->evidence,
        ];
    }
}

final class CcLedgerEntry
{
    public function __construct(
        public readonly string $claimId,
        public readonly CcSerialRange $range,
        public readonly string $action,
        public readonly string $acceptedAtUtc,
    ) {
    }

    public function toArray(): array
    {
        return array_merge($this->range->toArray(), [
            'claim_id' => $this->claimId,
            'action' => $this->action,
            'accepted_at_utc' => $this->acceptedAtUtc,
        ]);
    }

    public static function fromArray(array $data): self
    {
        return new self(
            (string)$data['claim_id'],
            CcSerialRange::fromArray($data),
            (string)($data['action'] ?? 'retire'),
            (string)($data['accepted_at_utc'] ?? gmdate('c')),
        );
    }
}

/**
 * Holds previously accepted ranges, grouped by (registry, project,
 * vintage, unit) so a lookup only ever scans the entries that could
 * possibly collide with a new claim instead of the whole ledger. A
 * ledger with credits spread across many projects and vintages stays
 * fast even as it grows, because each group is its own small list.
 */
final class CcLedger
{
    /** @var array<string, list<CcLedgerEntry>> */
    private array $groups = [];
    /** @var array<string, bool> */
    private array $claimIds = [];

    public function __construct(array $entries = [])
    {
        foreach ($entries as $entry) {
            $this->index($entry);
        }
    }

    public static function loadFromFile(string $path): self
    {
        if (!is_file($path)) {
            return new self();
        }
        $raw = file_get_contents($path);
        if ($raw === false) {
            throw new CcException("could not read ledger file: $path");
        }
        $raw = trim($raw);
        if ($raw === '') {
            return new self();
        }
        $decoded = json_decode($raw, true);
        if (!is_array($decoded)) {
            throw new CcException("ledger file is not a JSON array: $path");
        }
        $entries = [];
        foreach ($decoded as $row) {
            $entries[] = CcLedgerEntry::fromArray($row);
        }
        return new self($entries);
    }

    private function index(CcLedgerEntry $entry): void
    {
        $this->groups[$entry->range->groupKey()][] = $entry;
        $this->claimIds[$entry->claimId] = true;
    }

    public function hasClaimId(string $claimId): bool
    {
        return isset($this->claimIds[$claimId]);
    }

    /** @return list<CcLedgerEntry> */
    public function findOverlaps(CcSerialRange $range): array
    {
        $group = $this->groups[$range->groupKey()] ?? [];
        $hits = [];
        foreach ($group as $entry) {
            if ($entry->range->overlaps($range)) {
                $hits[] = $entry;
            }
        }
        return $hits;
    }

    public function withAccepted(CcLedgerEntry $entry): void
    {
        $this->index($entry);
    }

    /** @return list<array> */
    public function toArray(): array
    {
        $out = [];
        foreach ($this->groups as $group) {
            foreach ($group as $entry) {
                $out[] = $entry->toArray();
            }
        }
        usort($out, static fn(array $a, array $b): int => $a['accepted_at_utc'] <=> $b['accepted_at_utc']);
        return $out;
    }
}

/**
 * Finds overlaps *within* a single incoming batch, i.e. double counting a
 * submitter introduces before anything ever reaches the ledger. Ranges
 * are grouped by coordinate key, sorted by start, and swept once: total
 * cost is O(n log n) for the sort plus O(n) for the sweep, versus the
 * naive O(n^2) pairwise comparison a batch of any real size would need.
 */
final class CcBatchOverlapScanner
{
    /** @return list<array{a: CcClaim, b: CcClaim, quantity: int}> */
    public static function scan(array $claims): array
    {
        $groups = [];
        foreach ($claims as $claim) {
            $groups[$claim->range->groupKey()][] = $claim;
        }

        $pairs = [];
        foreach ($groups as $group) {
            usort($group, static fn(CcClaim $a, CcClaim $b): int => $a->range->start <=> $b->range->start);
            $activeEnd = null;
            $activeClaim = null;
            $cluster = [];
            foreach ($group as $claim) {
                if ($activeEnd !== null && $claim->range->start <= $activeEnd) {
                    if ($cluster === []) {
                        $cluster[] = $activeClaim;
                    }
                    $cluster[] = $claim;
                    $activeEnd = max($activeEnd, $claim->range->end);
                    continue;
                }
                if (count($cluster) > 1) {
                    array_push($pairs, ...self::pairwise($cluster));
                }
                $cluster = [];
                $activeEnd = $claim->range->end;
                $activeClaim = $claim;
            }
            if (count($cluster) > 1) {
                array_push($pairs, ...self::pairwise($cluster));
            }
        }
        return $pairs;
    }

    /** @param list<CcClaim> $cluster */
    private static function pairwise(array $cluster): array
    {
        $out = [];
        $n = count($cluster);
        for ($i = 0; $i < $n; $i++) {
            for ($j = $i + 1; $j < $n; $j++) {
                $overlap = $cluster[$i]->range->overlapQuantity($cluster[$j]->range);
                if ($overlap > 0) {
                    $out[] = ['a' => $cluster[$i], 'b' => $cluster[$j], 'quantity' => $overlap];
                }
            }
        }
        return $out;
    }
}

final class CcAuditResult
{
    /** @var array<string, string> claimId => decision */
    public array $decisions = [];
    /** @var list<CcFinding> */
    public array $findings = [];
    public int $exposureCents = 0;

    public function addFinding(CcFinding $finding): void
    {
        $this->findings[] = $finding;
        $current = $this->decisions[$finding->claimId] ?? 'accept';
        $decision = CcSeverity::atLeast($finding->severity, 'high') ? 'reject' : 'review';
        $rank = ['accept' => 0, 'review' => 1, 'reject' => 2];
        if ($rank[$decision] > $rank[$current]) {
            $this->decisions[$finding->claimId] = $decision;
        }
    }

    public function decisionFor(string $claimId): string
    {
        return $this->decisions[$claimId] ?? 'accept';
    }

    public function worstSeverity(): string
    {
        $worst = 'info';
        foreach ($this->findings as $finding) {
            $worst = CcSeverity::worst($worst, $finding->severity);
        }
        return $worst;
    }

    public function summary(): array
    {
        $bySeverity = ['info' => 0, 'low' => 0, 'medium' => 0, 'high' => 0, 'critical' => 0];
        foreach ($this->findings as $finding) {
            $bySeverity[$finding->severity]++;
        }
        $byDecision = ['accept' => 0, 'review' => 0, 'reject' => 0];
        foreach ($this->decisions as $decision) {
            $byDecision[$decision]++;
        }
        return [
            'findings_by_severity' => $bySeverity,
            'claims_by_decision' => $byDecision,
            'exposure_usd' => CcMoney::centsToUsd($this->exposureCents),
        ];
    }
}

final class CcAuditor
{
    private const MIN_VINTAGE_YEAR = 1996;

    public function __construct(private readonly CcLedger $ledger)
    {
    }

    /** @param list<CcClaim> $claims */
    public function audit(array $claims): CcAuditResult
    {
        $result = new CcAuditResult();
        $currentYear = (int)gmdate('Y');
        $seenInBatch = [];

        foreach ($claims as $claim) {
            $result->decisions[$claim->claimId] = $result->decisions[$claim->claimId] ?? 'accept';

            if (isset($seenInBatch[$claim->claimId])) {
                $result->addFinding(new CcFinding(
                    'high',
                    'duplicate_claim_id_in_batch',
                    $claim->claimId,
                    "claim_id '{$claim->claimId}' appears more than once in this submission",
                ));
            }
            $seenInBatch[$claim->claimId] = true;

            if ($this->ledger->hasClaimId($claim->claimId)) {
                $result->addFinding(new CcFinding(
                    'medium',
                    'claim_id_already_ledgered',
                    $claim->claimId,
                    "claim_id '{$claim->claimId}' was already accepted previously; treat as a resubmission unless intentional",
                ));
            }

            if ($claim->range->vintageYear > $currentYear + 1) {
                $result->addFinding(new CcFinding(
                    'high',
                    'future_vintage',
                    $claim->claimId,
                    "vintage year {$claim->range->vintageYear} is more than one year in the future",
                ));
            } elseif ($claim->range->vintageYear < self::MIN_VINTAGE_YEAR) {
                $result->addFinding(new CcFinding(
                    'medium',
                    'implausible_vintage',
                    $claim->claimId,
                    "vintage year {$claim->range->vintageYear} predates the modern carbon registry era",
                ));
            }

            if ($claim->declaredQuantity !== null && $claim->declaredQuantity !== $claim->range->quantity()) {
                $result->addFinding(new CcFinding(
                    'high',
                    'quantity_mismatch',
                    $claim->claimId,
                    sprintf(
                        'declared quantity %d does not match serial range size %d for %s',
                        $claim->declaredQuantity,
                        $claim->range->quantity(),
                        $claim->range->label(),
                    ),
                    ['declared' => $claim->declaredQuantity, 'range_size' => $claim->range->quantity()],
                ));
            }

            foreach ($this->ledger->findOverlaps($claim->range) as $entry) {
                $overlap = $entry->range->overlapQuantity($claim->range);
                if ($overlap <= 0) {
                    continue;
                }
                $result->addFinding(new CcFinding(
                    'critical',
                    'double_count_vs_ledger',
                    $claim->claimId,
                    sprintf(
                        '%d credits in %s overlap a previously accepted claim %s (%s)',
                        $overlap,
                        $claim->range->label(),
                        $entry->claimId,
                        $entry->range->label(),
                    ),
                    ['overlap_quantity' => $overlap, 'prior_claim_id' => $entry->claimId],
                ));
                if ($claim->priceCentsPerCredit !== null) {
                    $result->exposureCents += $overlap * $claim->priceCentsPerCredit;
                }
            }
        }

        foreach (CcBatchOverlapScanner::scan($claims) as $pair) {
            /** @var CcClaim $a */
            $a = $pair['a'];
            /** @var CcClaim $b */
            $b = $pair['b'];
            $quantity = $pair['quantity'];
            $message = sprintf(
                'claims %s and %s overlap by %d credits within the same submission (%s vs %s)',
                $a->claimId,
                $b->claimId,
                $quantity,
                $a->range->label(),
                $b->range->label(),
            );
            $result->addFinding(new CcFinding('critical', 'double_count_within_batch', $a->claimId, $message, [
                'overlap_quantity' => $quantity,
                'other_claim_id' => $b->claimId,
            ]));
            $result->addFinding(new CcFinding('critical', 'double_count_within_batch', $b->claimId, $message, [
                'overlap_quantity' => $quantity,
                'other_claim_id' => $a->claimId,
            ]));
            $price = $a->priceCentsPerCredit ?? $b->priceCentsPerCredit;
            if ($price !== null) {
                $result->exposureCents += $quantity * $price;
            }
        }

        return $result;
    }
}

final class CcReportRenderer
{
    public static function renderJson(CcAuditResult $result): string
    {
        $findings = array_map(static fn(CcFinding $f): array => $f->toArray(), $result->findings);
        $payload = [
            'summary' => $result->summary(),
            'decisions' => $result->decisions,
            'findings' => $findings,
        ];
        return (string)json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
    }

    public static function renderMarkdown(CcAuditResult $result): string
    {
        $summary = $result->summary();
        $lines = [];
        $lines[] = '# Carbon Credit Double Count Audit';
        $lines[] = '';
        $lines[] = '## Summary';
        $lines[] = sprintf('- Worst severity: **%s**', $result->worstSeverity());
        $lines[] = sprintf('- Estimated exposure: **$%s**', $summary['exposure_usd']);
        foreach ($summary['claims_by_decision'] as $decision => $count) {
            $lines[] = sprintf('- Claims %s: %d', $decision, $count);
        }
        $lines[] = '';
        $lines[] = '## Findings';
        if ($result->findings === []) {
            $lines[] = 'No findings. All claims are clean against the ledger and against each other.';
        }
        foreach ($result->findings as $finding) {
            $lines[] = sprintf(
                '- **[%s]** `%s` claim `%s`: %s',
                strtoupper($finding->severity),
                $finding->code,
                $finding->claimId,
                $finding->message,
            );
        }
        return implode("\n", $lines) . "\n";
    }
}

/**
 * Persists accepted claims back into the ledger file. Uses an exclusive
 * flock plus write-to-temp-then-rename so two CLI invocations racing on
 * the same ledger cannot interleave writes or leave a half-written file.
 */
final class CcLedgerWriter
{
    public static function commit(string $path, array $newEntries): void
    {
        if ($newEntries === []) {
            return;
        }
        $dir = dirname($path);
        if (!is_dir($dir)) {
            throw new CcException("ledger directory does not exist: $dir");
        }
        $handle = fopen($path, 'c+');
        if ($handle === false) {
            throw new CcException("could not open ledger for writing: $path");
        }
        try {
            if (!flock($handle, LOCK_EX)) {
                throw new CcException("could not lock ledger file: $path");
            }
            $existingRaw = stream_get_contents($handle);
            $existing = [];
            if (is_string($existingRaw) && trim($existingRaw) !== '') {
                $decoded = json_decode($existingRaw, true);
                if (is_array($decoded)) {
                    $existing = $decoded;
                }
            }
            $byClaimId = [];
            foreach ($existing as $row) {
                $byClaimId[(string)($row['claim_id'] ?? '')] = $row;
            }
            foreach ($newEntries as $entry) {
                $byClaimId[$entry->claimId] = $entry->toArray();
            }
            $merged = array_values($byClaimId);
            $tmpPath = $path . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
            $written = file_put_contents($tmpPath, (string)json_encode($merged, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
            if ($written === false) {
                throw new CcException("could not write temporary ledger file: $tmpPath");
            }
            if (!rename($tmpPath, $path)) {
                @unlink($tmpPath);
                throw new CcException("could not replace ledger file: $path");
            }
        } finally {
            flock($handle, LOCK_UN);
            fclose($handle);
        }
    }
}

final class CcExampleData
{
    public static function ledgerJson(): string
    {
        $entries = [
            [
                'claim_id' => 'seed-2025-0001',
                'serial_range' => 'VERRA:1529:2021:VCU:1000000-1000999',
                'action' => 'retire',
                'accepted_at_utc' => '2025-11-03T00:00:00+00:00',
            ],
        ];
        return (string)json_encode($entries, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
    }

    public static function claimsJson(): string
    {
        $claims = [
            [
                'claim_id' => 'batch-2026-0100',
                'serial_range' => 'VERRA:1529:2021:VCU:2000000-2000499',
                'action' => 'retire',
                'declared_quantity' => 500,
                'counterparty' => 'Acme Offsets Ltd',
                'price_usd_per_credit' => 8.75,
            ],
            [
                'claim_id' => 'batch-2026-0101',
                'serial_range' => 'VERRA:1529:2021:VCU:1000500-1001200',
                'action' => 'retire',
                'declared_quantity' => 701,
                'counterparty' => 'Northwind Carbon Desk',
                'price_usd_per_credit' => 8.75,
            ],
        ];
        return (string)json_encode($claims, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
    }
}

final class CcSelfTest
{
    public static function run(): bool
    {
        $ok = true;
        $ok = self::check('non-overlapping ranges are clean', self::testCleanBatch()) && $ok;
        $ok = self::check('ledger overlap is flagged critical with correct quantity', self::testLedgerOverlap()) && $ok;
        $ok = self::check('within-batch overlap is flagged for both claims', self::testBatchOverlap()) && $ok;
        $ok = self::check('quantity mismatch is flagged', self::testQuantityMismatch()) && $ok;
        $ok = self::check('future vintage is flagged', self::testFutureVintage()) && $ok;
        $ok = self::check('duplicate claim_id in batch is flagged', self::testDuplicateClaimId()) && $ok;
        $ok = self::check('claim_id already in ledger is flagged', self::testResubmission()) && $ok;
        $ok = self::check('different registries never collide on identical serials', self::testCrossRegistryIsolation()) && $ok;
        $ok = self::check('ledger writer round-trips through disk atomically', self::testLedgerWriterRoundTrip()) && $ok;
        return $ok;
    }

    private static function check(string $label, bool $passed): bool
    {
        echo ($passed ? 'PASS' : 'FAIL') . ' - ' . $label . PHP_EOL;
        return $passed;
    }

    private static function claim(string $id, string $range, ?int $qty = null, ?float $price = null): CcClaim
    {
        return CcClaim::fromArray(array_filter([
            'claim_id' => $id,
            'serial_range' => $range,
            'action' => 'retire',
            'declared_quantity' => $qty,
            'price_usd_per_credit' => $price,
        ], static fn($v) => $v !== null));
    }

    private static function testCleanBatch(): bool
    {
        $auditor = new CcAuditor(new CcLedger());
        $result = $auditor->audit([
            self::claim('c1', 'VERRA:100:2022:VCU:1-500'),
            self::claim('c2', 'VERRA:100:2022:VCU:501-1000'),
        ]);
        return $result->findings === [];
    }

    private static function testLedgerOverlap(): bool
    {
        $ledger = new CcLedger([
            new CcLedgerEntry('prior-1', CcSerialRange::fromSerialString('VERRA:100:2022:VCU:1-1000'), 'retire', gmdate('c')),
        ]);
        $auditor = new CcAuditor($ledger);
        $result = $auditor->audit([self::claim('c1', 'VERRA:100:2022:VCU:900-1200', null, 10.0)]);
        $hits = array_filter($result->findings, static fn(CcFinding $f) => $f->code === 'double_count_vs_ledger');
        $hit = array_values($hits)[0] ?? null;
        return $hit !== null
            && $hit->evidence['overlap_quantity'] === 101
            && $result->decisionFor('c1') === 'reject'
            && $result->exposureCents === 101 * 1000;
    }

    private static function testBatchOverlap(): bool
    {
        $auditor = new CcAuditor(new CcLedger());
        $result = $auditor->audit([
            self::claim('c1', 'ACR:55:2023:VCU:1-100'),
            self::claim('c2', 'ACR:55:2023:VCU:50-150'),
        ]);
        $codes = array_map(static fn(CcFinding $f) => $f->code, $result->findings);
        return in_array('double_count_within_batch', $codes, true)
            && $result->decisionFor('c1') === 'reject'
            && $result->decisionFor('c2') === 'reject';
    }

    private static function testQuantityMismatch(): bool
    {
        $auditor = new CcAuditor(new CcLedger());
        $result = $auditor->audit([self::claim('c1', 'GS:7:2024:VER:1-100', 50)]);
        $codes = array_map(static fn(CcFinding $f) => $f->code, $result->findings);
        return in_array('quantity_mismatch', $codes, true);
    }

    private static function testFutureVintage(): bool
    {
        $futureYear = (int)gmdate('Y') + 5;
        $auditor = new CcAuditor(new CcLedger());
        $result = $auditor->audit([self::claim('c1', "VERRA:1:$futureYear:VCU:1-10")]);
        $codes = array_map(static fn(CcFinding $f) => $f->code, $result->findings);
        return in_array('future_vintage', $codes, true);
    }

    private static function testDuplicateClaimId(): bool
    {
        $auditor = new CcAuditor(new CcLedger());
        $result = $auditor->audit([
            self::claim('dup', 'VERRA:1:2022:VCU:1-10'),
            self::claim('dup', 'VERRA:1:2022:VCU:2000-2010'),
        ]);
        $codes = array_map(static fn(CcFinding $f) => $f->code, $result->findings);
        return in_array('duplicate_claim_id_in_batch', $codes, true);
    }

    private static function testResubmission(): bool
    {
        $ledger = new CcLedger([
            new CcLedgerEntry('seen', CcSerialRange::fromSerialString('CAR:9:2020:CRT:1-5'), 'retire', gmdate('c')),
        ]);
        $auditor = new CcAuditor($ledger);
        $result = $auditor->audit([self::claim('seen', 'CAR:9:2020:CRT:9000-9005')]);
        $codes = array_map(static fn(CcFinding $f) => $f->code, $result->findings);
        return in_array('claim_id_already_ledgered', $codes, true);
    }

    private static function testCrossRegistryIsolation(): bool
    {
        $auditor = new CcAuditor(new CcLedger());
        $result = $auditor->audit([
            self::claim('c1', 'VERRA:1:2022:VCU:1-100'),
            self::claim('c2', 'ACR:1:2022:VCU:1-100'),
        ]);
        return $result->findings === [];
    }

    private static function testLedgerWriterRoundTrip(): bool
    {
        $tmpDir = sys_get_temp_dir() . '/ccdcg_selftest_' . bin2hex(random_bytes(4));
        mkdir($tmpDir);
        $path = $tmpDir . '/ledger.json';
        try {
            $entry = new CcLedgerEntry('rt-1', CcSerialRange::fromSerialString('PURO:1:2023:CORC:1-9'), 'retire', gmdate('c'));
            CcLedgerWriter::commit($path, [$entry]);
            $reloaded = CcLedger::loadFromFile($path);
            $again = new CcLedgerEntry('rt-2', CcSerialRange::fromSerialString('PURO:1:2023:CORC:100-109'), 'retire', gmdate('c'));
            CcLedgerWriter::commit($path, [$again]);
            $final = CcLedger::loadFromFile($path);
            return $reloaded->hasClaimId('rt-1') && $final->hasClaimId('rt-1') && $final->hasClaimId('rt-2');
        } finally {
            @unlink($path);
            @rmdir($tmpDir);
        }
    }
}

final class CcCli
{
    public static function run(array $argv): int
    {
        try {
            $options = self::parseArgs($argv);
        } catch (Throwable $e) {
            fwrite(STDERR, 'CarbonCreditDoubleCountGuard: ' . $e->getMessage() . PHP_EOL);
            return 64;
        }

        if ($options['help']) {
            echo self::help();
            return 0;
        }

        if ($options['self_test']) {
            return CcSelfTest::run() ? 0 : 1;
        }

        if ($options['example']) {
            echo "# ledger.json\n" . CcExampleData::ledgerJson() . "\n\n# claims.json\n" . CcExampleData::claimsJson() . "\n";
            return 0;
        }

        try {
            if ($options['claims'] === null) {
                throw new CcException('--claims PATH is required (or use --self-test / --example)');
            }
            $ledger = $options['ledger'] !== null ? CcLedger::loadFromFile($options['ledger']) : new CcLedger();
            $claimsRaw = json_decode((string)file_get_contents($options['claims']), true);
            if (!is_array($claimsRaw)) {
                throw new CcException("claims file is not a JSON array: {$options['claims']}");
            }
            $claims = array_map(static fn(array $row): CcClaim => CcClaim::fromArray($row), $claimsRaw);

            $auditor = new CcAuditor($ledger);
            $result = $auditor->audit($claims);

            echo $options['format'] === 'markdown'
                ? CcReportRenderer::renderMarkdown($result)
                : CcReportRenderer::renderJson($result) . PHP_EOL;

            if ($options['commit'] && $options['ledger'] !== null) {
                $accepted = array_values(array_filter(
                    $claims,
                    static fn(CcClaim $c) => $result->decisionFor($c->claimId) !== 'reject',
                ));
                $entries = array_map(
                    static fn(CcClaim $c): CcLedgerEntry => new CcLedgerEntry($c->claimId, $c->range, $c->action, gmdate('c')),
                    $accepted,
                );
                CcLedgerWriter::commit($options['ledger'], $entries);
            }

            if ($options['fail_on'] !== null && CcSeverity::atLeast($result->worstSeverity(), $options['fail_on'])) {
                return 2;
            }
            return 0;
        } catch (Throwable $e) {
            fwrite(STDERR, 'CarbonCreditDoubleCountGuard: ' . $e->getMessage() . PHP_EOL);
            return 1;
        }
    }

    private static function parseArgs(array $argv): array
    {
        $options = [
            'ledger' => null,
            'claims' => null,
            'format' => 'json',
            'fail_on' => null,
            'commit' => false,
            'self_test' => false,
            'example' => false,
            'help' => false,
        ];
        for ($i = 0; $i < count($argv); $i++) {
            $arg = $argv[$i];
            switch ($arg) {
                case '--ledger':
                    $options['ledger'] = self::value($argv, ++$i, $arg);
                    break;
                case '--claims':
                    $options['claims'] = self::value($argv, ++$i, $arg);
                    break;
                case '--format':
                    $options['format'] = self::value($argv, ++$i, $arg);
                    break;
                case '--fail-on':
                    $options['fail_on'] = CcSeverity::normalize(self::value($argv, ++$i, $arg));
                    break;
                case '--commit':
                    $options['commit'] = true;
                    break;
                case '--self-test':
                    $options['self_test'] = true;
                    break;
                case '--example':
                    $options['example'] = true;
                    break;
                case '--help':
                case '-h':
                    $options['help'] = true;
                    break;
                default:
                    throw new CcException('unknown option ' . $arg);
            }
        }
        if (!in_array($options['format'], ['json', 'markdown'], true)) {
            throw new CcException('--format must be json or markdown');
        }
        return $options;
    }

    private static function value(array $argv, int $index, string $flag): string
    {
        if ($index >= count($argv)) {
            throw new CcException('missing value after ' . $flag);
        }
        return $argv[$index];
    }

    private static function help(): string
    {
        return <<<TXT
Usage: php CarbonCreditDoubleCountGuard.php [options]

Options:
  --ledger PATH       JSON file of previously accepted claims (created on first --commit).
  --claims PATH       JSON array of incoming retirement/transfer/issue claims to audit.
  --format json|md    Report format. Default: json.
  --fail-on SEVERITY  Exit 2 if any finding is at least this severe (info|low|medium|high|critical).
  --commit            Persist non-rejected claims from this run into --ledger.
  --example           Print a sample ledger.json and claims.json and exit.
  --self-test         Run embedded regression checks and exit.
  --help              Show this message.

Claim JSON fields:
  claim_id (required), action (issue|transfer|retire, default retire),
  serial_range "REGISTRY:PROJECT:VINTAGE:UNIT:START-END" OR the five
  explicit fields registry/project_id/vintage_year/unit_type/serial_start/
  serial_end, plus optional declared_quantity, counterparty, and
  price_usd_per_credit for exposure estimation.

TXT;
    }
}

if (PHP_SAPI === 'cli' && isset($argv) && realpath($argv[0] ?? '') === __FILE__) {
    exit(CcCli::run(array_slice($argv, 1)));
}

/*
This solves the double counting problem that quietly wrecks trust in carbon markets: the same
tonne of CO2 getting retired, sold, or claimed more than once because two spreadsheets, two
brokers, or two badly-synced systems never actually compared serial numbers before a deal
closed. Built because I run a carbon project and outreach business myself and I got tired of
watching "just check the registry manually" be the entire control on six and seven figure
transactions. Real registries like Verra, Gold Standard, ACR, CAR, GCC, and Puro Earth issue
credits as serial number ranges tied to a project, a vintage year, and a unit type, and every
scandal I have read about eventually traces back to two ranges quietly overlapping, or the
same claim id getting submitted twice by mistake or on purpose. Use it when you are building
or running any part of a carbon credit pipeline in PHP: a Laravel or Symfony backend for a
carbon desk, an internal tool before a retirement is filed with a registry, a due diligence
step before you buy credits from a new counterparty, or a CI gate on a data export that feeds
your books. The trick is treating every claim as an interval and doing the two comparisons
that actually catch fraud and honest mistakes alike: sort and sweep the new batch against
itself in O(n log n) instead of comparing every claim to every other claim, and check each new
range against a persistent ledger grouped by registry, project, vintage, and unit type so old
claims and new claims are held to the same standard. It also catches the boring stuff that
still costs money: a declared quantity that does not match the actual range size, a vintage
year that has not happened yet, and a claim id getting resubmitted by accident. Drop this
single file into any PHP 8 project, run it with --self-test to see the checks pass, run
--example to get starter JSON, then wire php CarbonCreditDoubleCountGuard.php --ledger
ledger.json --claims claims.json --commit --fail-on high into your intake pipeline or CI so a
double-sold serial range fails the build instead of failing an audit. If you searched for
carbon credit double counting checker, Verra serial range overlap, retirement ledger PHP, VCU
double sell detector, carbon registry reconciliation tool, or offset fraud prevention script,
this is built to be exactly that, forkable and ready for real transactions.
*/
