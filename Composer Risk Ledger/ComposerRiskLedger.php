<?php
declare(strict_types=1);

/**
 * ComposerRiskLedger turns composer.lock plus optional composer audit output into
 * a deterministic CI risk ledger. It has no external dependencies and performs
 * no network calls, which makes it safe for private builds and air-gapped review.
 */
final class Finding
{
    /** @param array<string, mixed> $evidence */
    public function __construct(
        public readonly string $id,
        public readonly string $package,
        public readonly string $severity,
        public readonly int $score,
        public readonly string $message,
        public readonly array $evidence,
        public readonly string $remediation
    ) {
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'package' => $this->package,
            'severity' => $this->severity,
            'score' => $this->score,
            'message' => $this->message,
            'evidence' => $this->evidence,
            'remediation' => $this->remediation,
        ];
    }
}

final class ComposerRiskLedger
{
    /** @var array<string, mixed> */
    private array $policy;

    /** @var array<string, list<array<string, mixed>>> */
    private array $advisories;

    /** @var array<string, mixed> */
    private array $lock;

    /** @param array<string, mixed> $lock @param array<string, mixed> $policy @param array<string, list<array<string, mixed>>> $advisories */
    public function __construct(array $lock, array $policy = [], array $advisories = [])
    {
        $this->lock = $lock;
        $this->policy = self::mergePolicy(self::defaultPolicy(), $policy);
        $this->advisories = $advisories;
    }

    /** @return array<string, mixed> */
    public function analyze(bool $productionOnly = false): array
    {
        $packages = $this->packages($productionOnly);
        $findings = [];
        $packageScores = [];

        foreach ($packages as $package) {
            $packageFindings = $this->findingsForPackage($package);
            foreach ($packageFindings as $finding) {
                $findings[] = $finding;
            }

            $scores = array_map(static fn(Finding $finding): int => $finding->score, $packageFindings);
            $packageScores[$package['name']] = [
                'name' => $package['name'],
                'version' => $package['version'],
                'type' => $package['type'],
                'isDev' => $package['isDev'],
                'score' => self::combineScores($scores),
                'findingCount' => count($packageFindings),
            ];
        }

        usort(
            $findings,
            static fn(Finding $left, Finding $right): int => [$right->score, $left->package, $left->id] <=> [$left->score, $right->package, $right->id]
        );

        usort(
            $packageScores,
            static fn(array $left, array $right): int => [$right['score'], $left['name']] <=> [$left['score'], $right['name']]
        );

        $overallRisk = self::overallRisk($packageScores);

        return [
            'schema' => 'composer-risk-ledger.v1',
            'generatedAt' => gmdate('c'),
            'composerPluginApiVersion' => $this->lock['plugin-api-version'] ?? null,
            'policyHash' => hash('sha256', self::json($this->policy)),
            'productionOnly' => $productionOnly,
            'packageCount' => count($packages),
            'findingCount' => count($findings),
            'overallRisk' => $overallRisk,
            'highestPackageRisk' => $packageScores[0]['score'] ?? 0,
            'packages' => $packageScores,
            'findings' => array_map(static fn(Finding $finding): array => $finding->toArray(), $findings),
        ];
    }

    /** @return array<string, mixed> */
    public static function defaultPolicy(): array
    {
        return [
            'allowedLicenses' => [],
            'blockedLicenses' => ['proprietary', 'unlicensed', 'unknown'],
            'copyleftLicenses' => ['AGPL-1.0', 'AGPL-3.0', 'GPL-2.0', 'GPL-3.0', 'LGPL-2.1', 'LGPL-3.0', 'SSPL-1.0'],
            'allowCopyleft' => false,
            'trustedSourceHosts' => ['github.com', 'gitlab.com', 'bitbucket.org', 'packagist.org'],
            'blockedVendors' => [],
            'criticalPackages' => [],
            'maxReleaseAgeDays' => 730,
            'failOnComposerPlugins' => false,
            'scoreWeights' => [
                'advisoryCritical' => 100,
                'advisoryHigh' => 78,
                'advisoryMedium' => 48,
                'advisoryLow' => 22,
                'abandoned' => 82,
                'blockedVendor' => 80,
                'blockedLicense' => 74,
                'copyleftLicense' => 58,
                'unknownLicense' => 36,
                'untrustedSource' => 42,
                'staleRelease' => 28,
                'unstableVersion' => 35,
                'composerPlugin' => 46,
                'wildcardConstraint' => 24,
            ],
        ];
    }

    /** @return list<array{name: string, version: string, type: string, isDev: bool, raw: array<string, mixed>}> */
    private function packages(bool $productionOnly): array
    {
        $packages = [];
        foreach (($this->lock['packages'] ?? []) as $package) {
            if (is_array($package)) {
                $packages[] = $this->normalizePackage($package, false);
            }
        }

        if (!$productionOnly) {
            foreach (($this->lock['packages-dev'] ?? []) as $package) {
                if (is_array($package)) {
                    $packages[] = $this->normalizePackage($package, true);
                }
            }
        }

        usort($packages, static fn(array $left, array $right): int => $left['name'] <=> $right['name']);
        return $packages;
    }

    /** @param array<string, mixed> $package @return array{name: string, version: string, type: string, isDev: bool, raw: array<string, mixed>} */
    private function normalizePackage(array $package, bool $isDev): array
    {
        return [
            'name' => strtolower((string)($package['name'] ?? 'unknown/unknown')),
            'version' => (string)($package['version'] ?? '0.0.0'),
            'type' => (string)($package['type'] ?? 'library'),
            'isDev' => $isDev,
            'raw' => $package,
        ];
    }

    /** @param array{name: string, version: string, type: string, isDev: bool, raw: array<string, mixed>} $package @return list<Finding> */
    private function findingsForPackage(array $package): array
    {
        $findings = [];
        $raw = $package['raw'];
        $name = $package['name'];
        $weights = $this->policy['scoreWeights'];

        foreach ($this->advisories[$name] ?? [] as $advisory) {
            $severity = self::severity((string)($advisory['severity'] ?? 'medium'));
            $scoreKey = 'advisory' . ucfirst($severity);
            $score = (int)($weights[$scoreKey] ?? $weights['advisoryMedium']);
            $findingId = 'COMPOSER-AUDIT-' . strtoupper($severity);
            $cve = self::firstText($advisory, ['cve', 'cveId', 'CVE', 'id']);
            $title = self::firstText($advisory, ['title', 'advisory', 'summary']);

            $findings[] = new Finding(
                $findingId,
                $name,
                $severity,
                self::devAdjustedScore($score, $package['isDev']),
                $name . ' has a ' . $severity . ' Composer advisory' . ($title !== '' ? ': ' . $title : ''),
                array_filter([
                    'version' => $package['version'],
                    'cve' => $cve,
                    'affectedVersions' => self::firstText($advisory, ['affectedVersions', 'affected_versions', 'constraint']),
                    'link' => self::firstText($advisory, ['link', 'url', 'source']),
                    'devDependency' => $package['isDev'],
                ], static fn($value): bool => $value !== '' && $value !== null),
                'Upgrade the package to a patched version or replace it before releasing this lockfile.'
            );
        }

        $abandoned = $raw['abandoned'] ?? false;
        if ($abandoned !== false && $abandoned !== null) {
            $replacement = is_string($abandoned) ? $abandoned : '';
            $findings[] = new Finding(
                'COMPOSER-ABANDONED-PACKAGE',
                $name,
                'high',
                self::devAdjustedScore((int)$weights['abandoned'], $package['isDev']),
                $name . ' is marked abandoned in composer.lock' . ($replacement !== '' ? '; suggested replacement is ' . $replacement : ''),
                ['version' => $package['version'], 'replacement' => $replacement, 'devDependency' => $package['isDev']],
                'Replace abandoned dependencies because they usually stop receiving vulnerability fixes and runtime compatibility updates.'
            );
        }

        $vendor = explode('/', $name, 2)[0] ?? $name;
        if (self::containsCaseInsensitive((array)$this->policy['blockedVendors'], $vendor)) {
            $findings[] = new Finding(
                'COMPOSER-BLOCKED-VENDOR',
                $name,
                'high',
                self::devAdjustedScore((int)$weights['blockedVendor'], $package['isDev']),
                $name . ' is published by a vendor blocked by policy',
                ['vendor' => $vendor, 'devDependency' => $package['isDev']],
                'Remove the package or document an approved exception in the policy file.'
            );
        }

        $licenseFinding = $this->licenseFinding($package);
        if ($licenseFinding !== null) {
            $findings[] = $licenseFinding;
        }

        $sourceUrl = (string)($raw['source']['url'] ?? $raw['dist']['url'] ?? '');
        $sourceHost = self::host($sourceUrl);
        if ($sourceHost !== '' && !self::hostAllowed($sourceHost, (array)$this->policy['trustedSourceHosts'])) {
            $findings[] = new Finding(
                'COMPOSER-UNTRUSTED-SOURCE-HOST',
                $name,
                'medium',
                self::devAdjustedScore((int)$weights['untrustedSource'], $package['isDev']),
                $name . ' is locked to an untrusted source host: ' . $sourceHost,
                ['sourceUrl' => $sourceUrl, 'sourceHost' => $sourceHost, 'devDependency' => $package['isDev']],
                'Mirror the package through a trusted registry, pin the expected host in policy, or remove the dependency.'
            );
        }

        $age = self::releaseAgeDays($raw['time'] ?? null);
        if ($age !== null && $age > (int)$this->policy['maxReleaseAgeDays']) {
            $severity = $age > ((int)$this->policy['maxReleaseAgeDays'] * 2) ? 'medium' : 'low';
            $score = $severity === 'medium' ? (int)$weights['staleRelease'] + 12 : (int)$weights['staleRelease'];
            $findings[] = new Finding(
                'COMPOSER-STALE-RELEASE',
                $name,
                $severity,
                self::devAdjustedScore(min(65, $score), $package['isDev']),
                $name . ' has not shipped a locked release for ' . $age . ' days',
                ['releaseTime' => (string)$raw['time'], 'ageDays' => $age, 'devDependency' => $package['isDev']],
                'Check maintainer activity and upgrade, fork, or replace the package if it is no longer actively maintained.'
            );
        }

        if (self::isUnstableVersion($package['version'])) {
            $findings[] = new Finding(
                'COMPOSER-UNSTABLE-VERSION',
                $name,
                'medium',
                self::devAdjustedScore((int)$weights['unstableVersion'], $package['isDev']),
                $name . ' is locked to an unstable version: ' . $package['version'],
                ['version' => $package['version'], 'devDependency' => $package['isDev']],
                'Prefer a stable release for production paths or isolate this package behind an explicit risk exception.'
            );
        }

        if ($package['type'] === 'composer-plugin') {
            $score = (int)$weights['composerPlugin'];
            $severity = (bool)$this->policy['failOnComposerPlugins'] ? 'high' : 'medium';
            $findings[] = new Finding(
                'COMPOSER-PLUGIN-CODE-EXECUTION',
                $name,
                $severity,
                self::devAdjustedScore($score, $package['isDev']),
                $name . ' is a Composer plugin and can execute during install/update',
                ['type' => $package['type'], 'devDependency' => $package['isDev']],
                'Review plugin code ownership, pin the plugin, and keep allow-plugins locked down in composer.json.'
            );
        }

        foreach (['require', 'require-dev', 'conflict', 'replace'] as $section) {
            foreach (($raw[$section] ?? []) as $dependency => $constraint) {
                if (self::isLooseConstraint((string)$constraint)) {
                    $findings[] = new Finding(
                        'COMPOSER-LOOSE-CONSTRAINT',
                        $name,
                        'low',
                        self::devAdjustedScore((int)$weights['wildcardConstraint'], $package['isDev']),
                        $name . ' declares a loose dependency constraint on ' . (string)$dependency,
                        ['section' => $section, 'dependency' => (string)$dependency, 'constraint' => (string)$constraint, 'devDependency' => $package['isDev']],
                        'Tighten wildcard or development constraints so lockfile updates do not silently admit unsafe ranges.'
                    );
                }
            }
        }

        return $findings;
    }

    /** @param array{name: string, version: string, type: string, isDev: bool, raw: array<string, mixed>} $package */
    private function licenseFinding(array $package): ?Finding
    {
        $rawLicenses = $package['raw']['license'] ?? [];
        $licenses = is_array($rawLicenses) ? array_values(array_map('strval', $rawLicenses)) : [(string)$rawLicenses];
        $licenses = array_values(array_filter($licenses, static fn(string $value): bool => trim($value) !== ''));
        $weights = $this->policy['scoreWeights'];

        if ($licenses === []) {
            return new Finding(
                'COMPOSER-MISSING-LICENSE',
                $package['name'],
                'medium',
                self::devAdjustedScore((int)$weights['unknownLicense'], $package['isDev']),
                $package['name'] . ' has no license metadata in composer.lock',
                ['version' => $package['version'], 'devDependency' => $package['isDev']],
                'Ask the maintainer for license metadata or block the package until legal review approves it.'
            );
        }

        foreach ($licenses as $license) {
            if (self::containsCaseInsensitive((array)$this->policy['blockedLicenses'], $license)) {
                return new Finding(
                    'COMPOSER-BLOCKED-LICENSE',
                    $package['name'],
                    'high',
                    self::devAdjustedScore((int)$weights['blockedLicense'], $package['isDev']),
                    $package['name'] . ' uses a blocked license: ' . $license,
                    ['licenses' => $licenses, 'devDependency' => $package['isDev']],
                    'Replace the dependency or record a policy exception approved by legal and security owners.'
                );
            }
        }

        $allowed = (array)$this->policy['allowedLicenses'];
        if ($allowed !== [] && !self::intersectsCaseInsensitive($allowed, $licenses)) {
            return new Finding(
                'COMPOSER-LICENSE-NOT-ALLOWLISTED',
                $package['name'],
                'medium',
                self::devAdjustedScore((int)$weights['unknownLicense'], $package['isDev']),
                $package['name'] . ' license is not on the allow list: ' . implode(', ', $licenses),
                ['licenses' => $licenses, 'allowedLicenses' => $allowed, 'devDependency' => $package['isDev']],
                'Add an explicit approved license to policy or choose a package with a known compatible license.'
            );
        }

        if (!(bool)$this->policy['allowCopyleft'] && self::intersectsCaseInsensitive((array)$this->policy['copyleftLicenses'], $licenses)) {
            return new Finding(
                'COMPOSER-COPYLEFT-LICENSE',
                $package['name'],
                'medium',
                self::devAdjustedScore((int)$weights['copyleftLicense'], $package['isDev']),
                $package['name'] . ' uses a copyleft license that is blocked by default policy: ' . implode(', ', $licenses),
                ['licenses' => $licenses, 'devDependency' => $package['isDev']],
                'Review distribution obligations before shipping this package in a commercial or embedded product.'
            );
        }

        return null;
    }

    /** @param array<string, mixed> $base @param array<string, mixed> $override @return array<string, mixed> */
    private static function mergePolicy(array $base, array $override): array
    {
        foreach ($override as $key => $value) {
            if ($key === 'scoreWeights' && is_array($value)) {
                $base['scoreWeights'] = array_replace($base['scoreWeights'], $value);
                continue;
            }
            if (array_key_exists($key, $base)) {
                $base[$key] = $value;
            }
        }
        return $base;
    }

    /** @param list<int> $scores */
    private static function combineScores(array $scores): int
    {
        $survival = 1.0;
        foreach ($scores as $score) {
            $bounded = max(0, min(100, $score));
            $survival *= (1.0 - ($bounded / 100.0));
        }
        return (int)round((1.0 - $survival) * 100.0);
    }

    /** @param list<array<string, mixed>> $packages */
    private static function overallRisk(array $packages): int
    {
        if ($packages === []) {
            return 0;
        }

        $scores = array_column($packages, 'score');
        rsort($scores, SORT_NUMERIC);
        $top = array_slice($scores, 0, 8);
        $meanTop = array_sum($top) / max(1, count($top));
        $max = (int)($scores[0] ?? 0);
        return (int)round(($max * 0.68) + ($meanTop * 0.32));
    }

    private static function devAdjustedScore(int $score, bool $isDev): int
    {
        if (!$isDev) {
            return $score;
        }
        return max(8, (int)round($score * 0.72));
    }

    private static function severity(string $severity): string
    {
        $severity = strtolower(trim($severity));
        return match ($severity) {
            'critical', 'crit' => 'critical',
            'high' => 'high',
            'low', 'info', 'informational' => 'low',
            default => 'medium',
        };
    }

    /** @param array<string, mixed> $advisory @param list<string> $keys */
    private static function firstText(array $advisory, array $keys): string
    {
        foreach ($keys as $key) {
            if (!array_key_exists($key, $advisory)) {
                continue;
            }
            $value = $advisory[$key];
            if (is_scalar($value)) {
                return (string)$value;
            }
            if (is_array($value)) {
                return implode(', ', array_map('strval', $value));
            }
        }
        return '';
    }

    private static function host(string $url): string
    {
        if ($url === '') {
            return '';
        }
        $host = parse_url($url, PHP_URL_HOST);
        return is_string($host) ? strtolower($host) : '';
    }

    /** @param list<string> $trustedHosts */
    private static function hostAllowed(string $host, array $trustedHosts): bool
    {
        foreach ($trustedHosts as $trustedHost) {
            $trustedHost = strtolower(trim((string)$trustedHost));
            if ($trustedHost === '') {
                continue;
            }
            if ($host === $trustedHost || str_ends_with($host, '.' . $trustedHost)) {
                return true;
            }
        }
        return false;
    }

    private static function releaseAgeDays(mixed $time): ?int
    {
        if (!is_string($time) || trim($time) === '') {
            return null;
        }

        try {
            $release = new DateTimeImmutable($time, new DateTimeZone('UTC'));
            $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
            return (int)$release->diff($now)->format('%a');
        } catch (Throwable) {
            return null;
        }
    }

    private static function isUnstableVersion(string $version): bool
    {
        return preg_match('/(^dev-|dev$|alpha|beta|rc|snapshot|nightly)/i', $version) === 1;
    }

    private static function isLooseConstraint(string $constraint): bool
    {
        $constraint = strtolower(trim($constraint));
        if ($constraint === '' || $constraint === 'self.version') {
            return false;
        }
        return $constraint === '*' || str_contains($constraint, '@dev') || str_contains($constraint, 'dev-') || preg_match('/(^|\s)[<>=~^]*\s*x(\.|$)/', $constraint) === 1;
    }

    /** @param list<string> $haystack */
    private static function containsCaseInsensitive(array $haystack, string $needle): bool
    {
        $needle = strtolower(trim($needle));
        foreach ($haystack as $value) {
            if (strtolower(trim((string)$value)) === $needle) {
                return true;
            }
        }
        return false;
    }

    /** @param list<string> $left @param list<string> $right */
    private static function intersectsCaseInsensitive(array $left, array $right): bool
    {
        $normalized = [];
        foreach ($left as $value) {
            $normalized[strtolower(trim((string)$value))] = true;
        }
        foreach ($right as $value) {
            if (isset($normalized[strtolower(trim((string)$value))])) {
                return true;
            }
        }
        return false;
    }

    /** @param mixed $value */
    public static function json($value): string
    {
        return json_encode($value, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
    }
}

final class ComposerRiskCli
{
    /** @param list<string> $argv */
    public static function run(array $argv): int
    {
        try {
            $options = self::parseArgs($argv);
            if ($options['help']) {
                fwrite(STDOUT, self::usage());
                return 0;
            }

            $lock = self::readJsonFile($options['lock']);
            $policy = $options['policy'] !== null ? self::readJsonFile($options['policy']) : [];
            $advisories = $options['audit'] !== null ? self::readAuditFile($options['audit']) : [];

            $ledger = new ComposerRiskLedger($lock, $policy, $advisories);
            $report = $ledger->analyze($options['production']);

            $output = match ($options['format']) {
                'sarif' => self::sarif($report),
                'markdown' => self::markdown($report),
                default => ComposerRiskLedger::json($report),
            };

            fwrite(STDOUT, $output . PHP_EOL);
            return ((int)$report['overallRisk'] >= $options['failAt']) ? 2 : 0;
        } catch (Throwable $error) {
            fwrite(STDERR, 'ComposerRiskLedger: ' . $error->getMessage() . PHP_EOL);
            return 1;
        }
    }

    /** @param list<string> $argv @return array{lock: string, audit: ?string, policy: ?string, format: string, failAt: int, production: bool, help: bool} */
    private static function parseArgs(array $argv): array
    {
        $options = [
            'lock' => 'composer.lock',
            'audit' => null,
            'policy' => null,
            'format' => 'json',
            'failAt' => 75,
            'production' => false,
            'help' => false,
        ];

        for ($i = 1; $i < count($argv); $i++) {
            $arg = $argv[$i];
            switch ($arg) {
                case '--lock':
                    $options['lock'] = self::nextValue($argv, ++$i, '--lock');
                    break;
                case '--audit':
                    $options['audit'] = self::nextValue($argv, ++$i, '--audit');
                    break;
                case '--policy':
                    $options['policy'] = self::nextValue($argv, ++$i, '--policy');
                    break;
                case '--format':
                    $format = self::nextValue($argv, ++$i, '--format');
                    if (!in_array($format, ['json', 'sarif', 'markdown'], true)) {
                        throw new InvalidArgumentException('Unsupported format: ' . $format);
                    }
                    $options['format'] = $format;
                    break;
                case '--fail-at':
                    $value = self::nextValue($argv, ++$i, '--fail-at');
                    if (!ctype_digit($value) || (int)$value < 0 || (int)$value > 100) {
                        throw new InvalidArgumentException('--fail-at must be an integer from 0 to 100.');
                    }
                    $options['failAt'] = (int)$value;
                    break;
                case '--production':
                    $options['production'] = true;
                    break;
                case '--help':
                case '-h':
                    $options['help'] = true;
                    break;
                default:
                    throw new InvalidArgumentException('Unknown argument: ' . $arg);
            }
        }

        return $options;
    }

    /** @param list<string> $argv */
    private static function nextValue(array $argv, int $index, string $flag): string
    {
        if (!isset($argv[$index]) || str_starts_with($argv[$index], '--')) {
            throw new InvalidArgumentException($flag . ' requires a value.');
        }
        return $argv[$index];
    }

    /** @return array<string, mixed> */
    private static function readJsonFile(string $path): array
    {
        if (!is_file($path) || !is_readable($path)) {
            throw new RuntimeException('Cannot read JSON file: ' . $path);
        }
        $contents = file_get_contents($path);
        if ($contents === false) {
            throw new RuntimeException('Failed reading JSON file: ' . $path);
        }
        $data = json_decode($contents, true, 512, JSON_THROW_ON_ERROR);
        if (!is_array($data)) {
            throw new RuntimeException('JSON root must be an object or array: ' . $path);
        }
        return $data;
    }

    /** @return array<string, list<array<string, mixed>>> */
    private static function readAuditFile(string $path): array
    {
        $audit = self::readJsonFile($path);
        $advisories = [];

        foreach (($audit['advisories'] ?? []) as $packageName => $packageAdvisories) {
            if (!is_array($packageAdvisories)) {
                continue;
            }
            foreach ($packageAdvisories as $advisory) {
                if (is_array($advisory)) {
                    $advisories[strtolower((string)$packageName)][] = $advisory;
                }
            }
        }

        foreach (($audit['packages'] ?? []) as $packageName => $packageData) {
            if (!is_array($packageData)) {
                continue;
            }
            foreach (($packageData['advisories'] ?? []) as $advisory) {
                if (is_array($advisory)) {
                    $advisories[strtolower((string)$packageName)][] = $advisory;
                }
            }
        }

        return $advisories;
    }

    /** @param array<string, mixed> $report */
    private static function markdown(array $report): string
    {
        $lines = [];
        $lines[] = '# Composer Risk Ledger';
        $lines[] = '';
        $lines[] = '- Generated: `' . $report['generatedAt'] . '`';
        $lines[] = '- Overall risk: `' . $report['overallRisk'] . '/100`';
        $lines[] = '- Packages checked: `' . $report['packageCount'] . '`';
        $lines[] = '- Findings: `' . $report['findingCount'] . '`';
        $lines[] = '';
        $lines[] = '| Package | Version | Scope | Score | Findings |';
        $lines[] = '|---|---:|---:|---:|---:|';

        foreach (array_slice($report['packages'], 0, 25) as $package) {
            if ((int)$package['score'] === 0) {
                continue;
            }
            $lines[] = '| `' . self::escapeMarkdown((string)$package['name']) . '` | `' . self::escapeMarkdown((string)$package['version']) . '` | ' . ((bool)$package['isDev'] ? 'dev' : 'prod') . ' | ' . (int)$package['score'] . ' | ' . (int)$package['findingCount'] . ' |';
        }

        $lines[] = '';
        $lines[] = '## Findings';
        foreach (array_slice($report['findings'], 0, 50) as $finding) {
            $lines[] = '- **' . self::escapeMarkdown((string)$finding['severity']) . '** `' . self::escapeMarkdown((string)$finding['package']) . '` [' . (int)$finding['score'] . ']: ' . self::escapeMarkdown((string)$finding['message']);
        }

        return implode(PHP_EOL, $lines);
    }

    /** @param array<string, mixed> $report */
    private static function sarif(array $report): string
    {
        $rules = [];
        $results = [];

        foreach ($report['findings'] as $finding) {
            $id = (string)$finding['id'];
            $rules[$id] = [
                'id' => $id,
                'name' => $id,
                'shortDescription' => ['text' => $id],
                'fullDescription' => ['text' => (string)$finding['remediation']],
                'properties' => ['precision' => 'high', 'security-severity' => (string)max(0.1, ((int)$finding['score']) / 10)],
            ];

            $results[] = [
                'ruleId' => $id,
                'level' => self::sarifLevel((string)$finding['severity']),
                'message' => ['text' => (string)$finding['message']],
                'locations' => [[
                    'physicalLocation' => [
                        'artifactLocation' => ['uri' => 'composer.lock'],
                        'region' => ['startLine' => 1],
                    ],
                ]],
                'properties' => [
                    'package' => $finding['package'],
                    'score' => $finding['score'],
                    'evidence' => $finding['evidence'],
                ],
            ];
        }

        return ComposerRiskLedger::json([
            'version' => '2.1.0',
            '$schema' => 'https://json.schemastore.org/sarif-2.1.0.json',
            'runs' => [[
                'tool' => [
                    'driver' => [
                        'name' => 'ComposerRiskLedger',
                        'informationUri' => 'https://github.com/kspavankrishna/VIBE-CODE',
                        'rules' => array_values($rules),
                    ],
                ],
                'automationDetails' => ['id' => 'composer-risk-ledger'],
                'results' => $results,
                'properties' => [
                    'overallRisk' => $report['overallRisk'],
                    'packageCount' => $report['packageCount'],
                    'findingCount' => $report['findingCount'],
                ],
            ]],
        ]);
    }

    private static function sarifLevel(string $severity): string
    {
        return match ($severity) {
            'critical', 'high' => 'error',
            'medium' => 'warning',
            default => 'note',
        };
    }

    private static function escapeMarkdown(string $value): string
    {
        return str_replace(['|', "\n", "\r"], ['\\|', ' ', ' '], $value);
    }

    private static function usage(): string
    {
        return <<<'TEXT'
ComposerRiskLedger.php

Usage:
  php ComposerRiskLedger.php [--lock composer.lock] [--audit audit.json] [--policy policy.json] [--format json|sarif|markdown] [--fail-at 75] [--production]

Examples:
  composer audit --format=json > audit.json
  php ComposerRiskLedger.php --audit audit.json --format sarif --fail-at 70 > composer-risk.sarif
  php ComposerRiskLedger.php --production --policy composer-risk-policy.json --format markdown

Policy keys:
  allowedLicenses, blockedLicenses, copyleftLicenses, allowCopyleft,
  trustedSourceHosts, blockedVendors, criticalPackages,
  maxReleaseAgeDays, failOnComposerPlugins, scoreWeights

Exit codes:
  0 risk is below threshold
  1 input or runtime error
  2 risk is at or above --fail-at
TEXT;
    }
}

if (PHP_SAPI === 'cli' && isset($argv) && realpath((string)($argv[0] ?? '')) === __FILE__) {
    exit(ComposerRiskCli::run($argv));
}

/*
This solves the April 2026 PHP supply chain problem where composer.lock is treated like a boring artifact even though it decides exactly what code ships to production, CI runners, Laravel apps, Symfony services, WordPress tooling, and internal platform jobs. Built because I wanted a Composer security gate that works without sending private package names to a SaaS scanner, understands composer audit JSON, checks abandoned packages, risky licenses, stale releases, Composer plugins, untrusted source hosts, wildcard constraints, and produces JSON, Markdown, or SARIF for GitHub code scanning. Use it when a team needs a practical Composer lockfile risk score before deploys, vendor reviews, release approvals, or dependency update pull requests. The trick: the score is deterministic and combines independent findings per package instead of hiding everything behind one vague pass or fail flag, so the worst dependency and the overall dependency graph are both visible. Drop this into any PHP repository, run composer audit --format=json, pass that file to ComposerRiskLedger.php, and wire --fail-at into GitHub Actions, GitLab CI, Buildkite, Jenkins, or a local pre-release checklist.
*/