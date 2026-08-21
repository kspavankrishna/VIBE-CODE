<?php
declare(strict_types=1);

final class MgSeverity
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
            throw new InvalidArgumentException("unknown severity: $value");
        }
        return $value;
    }

    public static function rank(string $value): int
    {
        return self::RANK[self::normalize($value)];
    }

    public static function atLeast(string $actual, string $floor): bool
    {
        return self::rank($actual) >= self::rank($floor);
    }
}

final class MgMoney
{
    public static function usdToMicros(float|int|string $usd): int
    {
        if (is_string($usd)) {
            $usd = trim($usd);
            if ($usd === '') {
                return 0;
            }
            $usd = (float)$usd;
        }
        return (int)round(((float)$usd) * 1_000_000.0);
    }

    public static function microsToUsd(int $micros): string
    {
        $negative = $micros < 0;
        $micros = abs($micros);
        $whole = intdiv($micros, 1_000_000);
        $fraction = $micros % 1_000_000;
        $trimmed = rtrim(str_pad((string)$fraction, 6, '0', STR_PAD_LEFT), '0');
        $text = $trimmed === '' ? (string)$whole : $whole . '.' . $trimmed;
        return $negative ? '-' . $text : $text;
    }

    public static function tokenCostMicros(int $tokens, int $microsPerMillionTokens): int
    {
        if ($tokens <= 0 || $microsPerMillionTokens <= 0) {
            return 0;
        }
        return intdiv($tokens * $microsPerMillionTokens + 999_999, 1_000_000);
    }
}

final class MgText
{
    public static function wildcard(string $pattern, string $value): bool
    {
        $pattern = strtolower(trim($pattern));
        $value = strtolower(trim($value));
        if ($pattern === '*' || $pattern === $value) {
            return true;
        }
        $quoted = preg_quote($pattern, '/');
        $regex = '/^' . str_replace('\\*', '.*', $quoted) . '$/i';
        return (bool)preg_match($regex, $value);
    }

    public static function anyWildcard(array $patterns, string $value): bool
    {
        foreach ($patterns as $pattern) {
            if (self::wildcard((string)$pattern, $value)) {
                return true;
            }
        }
        return false;
    }

    public static function shortHash(string $text): string
    {
        return substr(hash('sha256', $text), 0, 16);
    }

    public static function flatten(mixed $value, int $limit = 16000): string
    {
        if (is_string($value)) {
            $text = $value;
        } else {
            $encoded = json_encode($value, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
            $text = $encoded === false ? var_export($value, true) : $encoded;
        }
        return strlen($text) > $limit ? substr($text, 0, $limit) : $text;
    }

    public static function csvList(string $value): array
    {
        $parts = preg_split('/[\s,]+/', trim($value)) ?: [];
        return array_values(array_filter($parts, static fn($item) => $item !== ''));
    }
}

final class MgFinding
{
    public function __construct(
        public readonly string $id,
        public readonly string $severity,
        public readonly string $message,
        public readonly ?string $tenant = null,
        public readonly ?string $provider = null,
        public readonly ?string $model = null,
        public readonly ?string $eventId = null,
        public readonly array $evidence = [],
    ) {}

    public function toArray(): array
    {
        return array_filter([
            'id' => $this->id,
            'severity' => $this->severity,
            'message' => $this->message,
            'tenant' => $this->tenant,
            'provider' => $this->provider,
            'model' => $this->model,
            'event_id' => $this->eventId,
            'evidence' => $this->evidence,
        ], static fn($value) => $value !== null && $value !== []);
    }
}

final class MgJson
{
    public static function string(array $row, array $keys, string $default = ''): string
    {
        foreach ($keys as $key) {
            if (array_key_exists($key, $row) && $row[$key] !== null) {
                return trim((string)$row[$key]);
            }
        }
        return $default;
    }

    public static function integer(array $row, array $keys, int $default = 0): int
    {
        foreach ($keys as $key) {
            if (array_key_exists($key, $row) && $row[$key] !== null && $row[$key] !== '') {
                return max(0, (int)$row[$key]);
            }
        }
        return $default;
    }

    public static function boolean(array $row, array $keys, bool $default = false): bool
    {
        foreach ($keys as $key) {
            if (!array_key_exists($key, $row)) {
                continue;
            }
            $value = $row[$key];
            if (is_bool($value)) {
                return $value;
            }
            if (is_numeric($value)) {
                return (int)$value !== 0;
            }
            $text = strtolower(trim((string)$value));
            return in_array($text, ['1', 'true', 'yes', 'on'], true);
        }
        return $default;
    }

    public static function map(array $row, array $keys): array
    {
        foreach ($keys as $key) {
            if (array_key_exists($key, $row) && is_array($row[$key])) {
                return $row[$key];
            }
        }
        return [];
    }

    public static function list(array $row, array $keys): array
    {
        foreach ($keys as $key) {
            if (!array_key_exists($key, $row) || $row[$key] === null) {
                continue;
            }
            $value = $row[$key];
            if (is_array($value)) {
                return array_values(array_map('strval', $value));
            }
            return MgText::csvList((string)$value);
        }
        return [];
    }
}

final class MgEvent
{
    public function __construct(
        public readonly string $id,
        public readonly string $tenant,
        public readonly string $provider,
        public readonly string $model,
        public readonly string $region,
        public readonly int $timestamp,
        public readonly int $inputTokens,
        public readonly int $outputTokens,
        public readonly int $cachedInputTokens,
        public readonly int $maxOutputTokens,
        public readonly bool $streaming,
        public readonly bool $retryable,
        public readonly string $idempotencyKey,
        public readonly string $endpoint,
        public readonly string $promptPreview,
        public readonly array $metadata,
        public readonly string $source,
        public readonly int $line,
    ) {}

    public static function fromRow(array $row, string $source, int $line): self
    {
        $usage = MgJson::map($row, ['usage', 'token_usage']);
        $request = MgJson::map($row, ['request', 'span', 'attributes']);
        $metadata = array_merge(MgJson::map($row, ['metadata', 'tags', 'labels']), $request);
        $timestamp = self::parseTime(MgJson::string($row, ['timestamp', 'time', 'created_at'], ''));
        $input = MgJson::integer($row, ['input_tokens', 'prompt_tokens'], MgJson::integer($usage, ['input_tokens', 'prompt_tokens']));
        $output = MgJson::integer($row, ['output_tokens', 'completion_tokens'], MgJson::integer($usage, ['output_tokens', 'completion_tokens']));
        $cached = MgJson::integer($row, ['cached_input_tokens', 'cached_tokens'], MgJson::integer($usage, ['cached_input_tokens', 'cached_tokens']));
        $maxOutput = MgJson::integer($row, ['max_output_tokens', 'max_tokens'], MgJson::integer($request, ['max_output_tokens', 'max_tokens']));
        $provider = MgJson::string($row, ['provider', 'vendor'], MgJson::string($request, ['provider', 'vendor'], 'unknown'));
        $model = MgJson::string($row, ['model', 'model_id', 'deployment'], MgJson::string($request, ['model', 'model_id', 'deployment'], 'unknown'));
        return new self(
            MgJson::string($row, ['id', 'request_id', 'trace_id'], 'event-' . $line),
            MgJson::string($row, ['tenant', 'tenant_id', 'workspace', 'project'], 'default'),
            $provider,
            $model,
            MgJson::string($row, ['region', 'data_region'], MgJson::string($request, ['region', 'data_region'], 'global')),
            $timestamp,
            $input,
            $output,
            min($cached, $input),
            $maxOutput,
            MgJson::boolean($row, ['streaming', 'stream'], MgJson::boolean($request, ['streaming', 'stream'])),
            MgJson::boolean($row, ['retryable', 'will_retry'], MgJson::boolean($request, ['retryable', 'will_retry'])),
            MgJson::string($row, ['idempotency_key', 'idempotencyKey'], MgJson::string($request, ['idempotency_key', 'idempotencyKey'])),
            MgJson::string($row, ['endpoint', 'route', 'path'], MgJson::string($request, ['endpoint', 'route', 'path'])),
            MgJson::string($row, ['prompt_preview', 'prompt', 'input_preview'], MgJson::string($request, ['prompt_preview', 'prompt', 'input_preview'])),
            $metadata,
            $source,
            $line,
        );
    }

    public function totalTokens(): int
    {
        return $this->inputTokens + $this->outputTokens;
    }

    public function minuteBucket(): int
    {
        return intdiv($this->timestamp, 60) * 60;
    }

    private static function parseTime(string $value): int
    {
        if ($value === '') {
            return time();
        }
        if (ctype_digit($value)) {
            return (int)$value;
        }
        $parsed = strtotime($value);
        return $parsed === false ? time() : $parsed;
    }
}

final class MgPolicy
{
    public function __construct(
        public readonly array $priceBook,
        public readonly array $contextWindows,
        public readonly array $tenantDailyBudgets,
        public readonly int $defaultDailyBudgetMicros,
        public readonly int $perRequestCapMicros,
        public readonly int $rpmLimit,
        public readonly int $tpmLimit,
        public readonly int $maxOutputTokens,
        public readonly array $allowedProviders,
        public readonly array $blockedProviders,
        public readonly array $blockedModels,
        public readonly array $allowedRegions,
        public readonly float $minimumCacheRatio,
        public readonly string $failOn,
        public readonly array $secretPatterns,
    ) {}

    public static function fromArray(array $raw): self
    {
        $priceBook = is_array($raw['price_book'] ?? null) ? $raw['price_book'] : self::defaultPriceBook();
        $contextWindows = is_array($raw['context_windows'] ?? null) ? $raw['context_windows'] : self::defaultContextWindows();
        $tenantBudgets = [];
        foreach (($raw['tenant_daily_budgets_usd'] ?? []) as $tenant => $usd) {
            $tenantBudgets[(string)$tenant] = MgMoney::usdToMicros($usd);
        }
        return new self(
            $priceBook,
            $contextWindows,
            $tenantBudgets,
            MgMoney::usdToMicros($raw['default_daily_budget_usd'] ?? 50),
            MgMoney::usdToMicros($raw['per_request_cap_usd'] ?? 2.50),
            max(1, (int)($raw['rpm_limit'] ?? 120)),
            max(1, (int)($raw['tpm_limit'] ?? 400000)),
            max(1, (int)($raw['max_output_tokens'] ?? 8192)),
            array_values(array_map('strval', $raw['allowed_providers'] ?? [])),
            array_values(array_map('strval', $raw['blocked_providers'] ?? [])),
            array_values(array_map('strval', $raw['blocked_models'] ?? [])),
            array_values(array_map('strval', $raw['allowed_regions'] ?? [])),
            max(0.0, min(1.0, (float)($raw['minimum_cache_ratio'] ?? 0.15))),
            MgSeverity::normalize((string)($raw['fail_on'] ?? 'high')),
            array_values(array_map('strval', $raw['secret_patterns'] ?? self::defaultSecretPatterns())),
        );
    }

    public static function exampleJson(): string
    {
        return json_encode([
            'default_daily_budget_usd' => 75,
            'tenant_daily_budgets_usd' => ['research' => 125, 'free-tier' => 8],
            'per_request_cap_usd' => 1.75,
            'rpm_limit' => 90,
            'tpm_limit' => 250000,
            'max_output_tokens' => 12000,
            'allowed_providers' => ['openai', 'anthropic', 'google', 'mistral', 'local'],
            'blocked_providers' => [],
            'blocked_models' => ['*-preview-unsafe', 'legacy-*'],
            'allowed_regions' => ['us', 'eu', 'global'],
            'minimum_cache_ratio' => 0.20,
            'price_book' => self::defaultPriceBook(),
            'context_windows' => self::defaultContextWindows(),
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n";
    }

    public function priceFor(MgEvent $event): ?array
    {
        $providerPrices = $this->priceBook[strtolower($event->provider)] ?? null;
        if (!is_array($providerPrices)) {
            return null;
        }
        foreach ($providerPrices as $pattern => $price) {
            if (MgText::wildcard((string)$pattern, strtolower($event->model)) && is_array($price)) {
                return [
                    'input' => MgMoney::usdToMicros($price['input_per_mtok_usd'] ?? 0),
                    'output' => MgMoney::usdToMicros($price['output_per_mtok_usd'] ?? 0),
                    'cached' => MgMoney::usdToMicros($price['cached_input_per_mtok_usd'] ?? ($price['input_per_mtok_usd'] ?? 0)),
                    'matched_model' => (string)$pattern,
                ];
            }
        }
        return null;
    }

    public function contextWindowFor(MgEvent $event): ?int
    {
        foreach ($this->contextWindows as $pattern => $tokens) {
            if (MgText::wildcard((string)$pattern, strtolower($event->model))) {
                return max(1, (int)$tokens);
            }
        }
        return null;
    }

    public function budgetForTenant(string $tenant): int
    {
        return $this->tenantDailyBudgets[$tenant] ?? $this->defaultDailyBudgetMicros;
    }

    private static function defaultPriceBook(): array
    {
        return [
            'openai' => [
                'gpt-5*' => ['input_per_mtok_usd' => 1.25, 'output_per_mtok_usd' => 10.00, 'cached_input_per_mtok_usd' => 0.125],
                'gpt-4.1*' => ['input_per_mtok_usd' => 2.00, 'output_per_mtok_usd' => 8.00, 'cached_input_per_mtok_usd' => 0.50],
                'o4-mini*' => ['input_per_mtok_usd' => 1.10, 'output_per_mtok_usd' => 4.40, 'cached_input_per_mtok_usd' => 0.275],
            ],
            'anthropic' => [
                'claude-opus-4*' => ['input_per_mtok_usd' => 15.00, 'output_per_mtok_usd' => 75.00, 'cached_input_per_mtok_usd' => 1.50],
                'claude-sonnet-4*' => ['input_per_mtok_usd' => 3.00, 'output_per_mtok_usd' => 15.00, 'cached_input_per_mtok_usd' => 0.30],
            ],
            'google' => [
                'gemini-2.5-pro*' => ['input_per_mtok_usd' => 1.25, 'output_per_mtok_usd' => 10.00, 'cached_input_per_mtok_usd' => 0.31],
                'gemini-2.5-flash*' => ['input_per_mtok_usd' => 0.30, 'output_per_mtok_usd' => 2.50, 'cached_input_per_mtok_usd' => 0.075],
            ],
            'mistral' => [
                'mistral-large*' => ['input_per_mtok_usd' => 2.00, 'output_per_mtok_usd' => 6.00, 'cached_input_per_mtok_usd' => 0.20],
                'codestral*' => ['input_per_mtok_usd' => 0.20, 'output_per_mtok_usd' => 0.60, 'cached_input_per_mtok_usd' => 0.05],
            ],
            'local' => [
                '*' => ['input_per_mtok_usd' => 0.02, 'output_per_mtok_usd' => 0.08, 'cached_input_per_mtok_usd' => 0.01],
            ],
        ];
    }

    private static function defaultContextWindows(): array
    {
        return [
            'gpt-5*' => 400000,
            'gpt-4.1*' => 1000000,
            'o4-mini*' => 200000,
            'claude-opus-4*' => 200000,
            'claude-sonnet-4*' => 200000,
            'gemini-2.5-pro*' => 1000000,
            'gemini-2.5-flash*' => 1000000,
            'mistral-large*' => 128000,
            'codestral*' => 256000,
            '*' => 128000,
        ];
    }

    private static function defaultSecretPatterns(): array
    {
        return [
            'AKIA[0-9A-Z]{16}',
            'ASIA[0-9A-Z]{16}',
            'gh[pousr]_[A-Za-z0-9_]{30,}',
            'sk-[A-Za-z0-9]{20,}',
            'xox[baprs]-[A-Za-z0-9-]{20,}',
            '(?i)(api[_-]?key|secret|token|password|authorization)\\s*[:=]\\s*[A-Za-z0-9._/+\\-=]{12,}',
        ];
    }
}

final class MgInputParser
{
    public static function parsePaths(array $paths): array
    {
        if ($paths === []) {
            return self::parseText((string)file_get_contents('php://stdin'), 'stdin');
        }
        $events = [];
        foreach ($paths as $path) {
            if (!is_file($path)) {
                throw new RuntimeException("input file not found: $path");
            }
            array_push($events, ...self::parseText((string)file_get_contents($path), $path));
        }
        return $events;
    }

    public static function parseText(string $text, string $source): array
    {
        $trimmed = trim($text);
        if ($trimmed === '') {
            return [];
        }
        try {
            $decoded = json_decode($trimmed, true, 512, JSON_THROW_ON_ERROR);
            $rows = self::rowsFromDocument($decoded);
            return self::eventsFromRows($rows, $source, 1);
        } catch (JsonException) {
            return self::parseJsonLines($text, $source);
        }
    }

    private static function rowsFromDocument(mixed $decoded): array
    {
        if (is_array($decoded) && array_is_list($decoded)) {
            return $decoded;
        }
        if (is_array($decoded)) {
            foreach (['events', 'requests', 'rows', 'data', 'spans'] as $key) {
                if (isset($decoded[$key]) && is_array($decoded[$key])) {
                    return $decoded[$key];
                }
            }
            return [$decoded];
        }
        throw new RuntimeException('input JSON must be an object, an array, or JSONL rows');
    }

    private static function parseJsonLines(string $text, string $source): array
    {
        $events = [];
        foreach (preg_split('/\R/', $text) ?: [] as $lineNumber => $line) {
            if (trim($line) === '') {
                continue;
            }
            try {
                $row = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            } catch (JsonException $error) {
                throw new RuntimeException($source . ':' . ($lineNumber + 1) . ': invalid JSONL: ' . $error->getMessage());
            }
            if (!is_array($row)) {
                throw new RuntimeException($source . ':' . ($lineNumber + 1) . ': JSONL row must be an object');
            }
            $events[] = MgEvent::fromRow($row, $source, $lineNumber + 1);
        }
        return $events;
    }

    private static function eventsFromRows(array $rows, string $source, int $firstLine): array
    {
        $events = [];
        foreach ($rows as $offset => $row) {
            if (!is_array($row)) {
                throw new RuntimeException($source . ': event row must be an object');
            }
            $events[] = MgEvent::fromRow($row, $source, $firstLine + $offset);
        }
        return $events;
    }
}

final class MgFirewall
{
    private array $findings = [];
    private array $eventCosts = [];
    private array $tenantDaySpend = [];
    private array $tenantMinuteRequests = [];
    private array $tenantMinuteTokens = [];

    public function __construct(private readonly MgPolicy $policy) {}

    public function analyze(array $events): self
    {
        foreach ($events as $event) {
            if (!$event instanceof MgEvent) {
                throw new RuntimeException('analyze expects MgEvent objects');
            }
            $this->analyzeEvent($event);
        }
        $this->analyzeAggregates($events);
        return $this;
    }

    public function findings(): array
    {
        usort($this->findings, static function (MgFinding $left, MgFinding $right): int {
            $rank = MgSeverity::rank($right->severity) <=> MgSeverity::rank($left->severity);
            return $rank !== 0 ? $rank : strcmp($left->id, $right->id);
        });
        return $this->findings;
    }

    public function summary(): array
    {
        $bySeverity = [];
        foreach ($this->findings as $finding) {
            $bySeverity[$finding->severity] = ($bySeverity[$finding->severity] ?? 0) + 1;
        }
        return [
            'events' => count($this->eventCosts),
            'estimated_cost_usd' => MgMoney::microsToUsd(array_sum($this->eventCosts)),
            'findings' => count($this->findings),
            'findings_by_severity' => $bySeverity,
            'failed' => $this->failed(),
            'fail_on' => $this->policy->failOn,
        ];
    }

    public function failed(): bool
    {
        foreach ($this->findings as $finding) {
            if (MgSeverity::atLeast($finding->severity, $this->policy->failOn)) {
                return true;
            }
        }
        return false;
    }

    private function analyzeEvent(MgEvent $event): void
    {
        $price = $this->policy->priceFor($event);
        if ($price === null) {
            $this->add('UNKNOWN_MODEL_PRICE', 'high', 'No price book entry matched this provider and model, so the gateway cannot defend spend decisions.', $event, [
                'provider' => $event->provider,
                'model' => $event->model,
            ]);
            $cost = 0;
        } else {
            $paidInput = max(0, $event->inputTokens - $event->cachedInputTokens);
            $cost = MgMoney::tokenCostMicros($paidInput, $price['input'])
                + MgMoney::tokenCostMicros($event->cachedInputTokens, $price['cached'])
                + MgMoney::tokenCostMicros($event->outputTokens, $price['output']);
            if ($cost > $this->policy->perRequestCapMicros) {
                $this->add('REQUEST_COST_CAP_EXCEEDED', 'high', 'A single model call exceeds the per-request spend cap.', $event, [
                    'estimated_usd' => MgMoney::microsToUsd($cost),
                    'cap_usd' => MgMoney::microsToUsd($this->policy->perRequestCapMicros),
                    'matched_price' => $price['matched_model'],
                ]);
            }
        }
        $this->eventCosts[$event->id] = $cost;
        $dayKey = $event->tenant . '|' . gmdate('Y-m-d', $event->timestamp);
        $minuteKey = $event->tenant . '|' . $event->minuteBucket();
        $this->tenantDaySpend[$dayKey] = ($this->tenantDaySpend[$dayKey] ?? 0) + $cost;
        $this->tenantMinuteRequests[$minuteKey] = ($this->tenantMinuteRequests[$minuteKey] ?? 0) + 1;
        $this->tenantMinuteTokens[$minuteKey] = ($this->tenantMinuteTokens[$minuteKey] ?? 0) + $event->totalTokens();
        $this->checkProviderAndModel($event);
        $this->checkRegion($event);
        $this->checkContext($event);
        $this->checkStreamingAndRetries($event);
        $this->checkSecrets($event);
        $this->checkCacheEfficiency($event);
        $this->suggestCheaperRoute($event, $cost);
    }

    private function analyzeAggregates(array $events): void
    {
        $seenDays = [];
        foreach ($events as $event) {
            $dayKey = $event->tenant . '|' . gmdate('Y-m-d', $event->timestamp);
            if (!isset($seenDays[$dayKey])) {
                $seenDays[$dayKey] = true;
                $spend = $this->tenantDaySpend[$dayKey] ?? 0;
                $budget = $this->policy->budgetForTenant($event->tenant);
                if ($spend > $budget) {
                    $this->add('TENANT_DAILY_BUDGET_EXCEEDED', 'critical', 'Tenant daily model spend exceeds the configured budget.', $event, [
                        'date' => gmdate('Y-m-d', $event->timestamp),
                        'estimated_usd' => MgMoney::microsToUsd($spend),
                        'budget_usd' => MgMoney::microsToUsd($budget),
                    ]);
                }
            }
            $minuteKey = $event->tenant . '|' . $event->minuteBucket();
            $rpm = $this->tenantMinuteRequests[$minuteKey] ?? 0;
            $tpm = $this->tenantMinuteTokens[$minuteKey] ?? 0;
            if ($rpm > $this->policy->rpmLimit) {
                $this->add('TENANT_RPM_LIMIT_EXCEEDED', 'medium', 'Tenant request rate exceeds the gateway minute limit.', $event, [
                    'minute_utc' => gmdate('c', $event->minuteBucket()),
                    'requests' => $rpm,
                    'limit' => $this->policy->rpmLimit,
                ]);
            }
            if ($tpm > $this->policy->tpmLimit) {
                $this->add('TENANT_TPM_LIMIT_EXCEEDED', 'high', 'Tenant token rate exceeds the gateway minute limit.', $event, [
                    'minute_utc' => gmdate('c', $event->minuteBucket()),
                    'tokens' => $tpm,
                    'limit' => $this->policy->tpmLimit,
                ]);
            }
        }
    }

    private function checkProviderAndModel(MgEvent $event): void
    {
        if ($this->policy->allowedProviders !== [] && !MgText::anyWildcard($this->policy->allowedProviders, $event->provider)) {
            $this->add('PROVIDER_NOT_ALLOWED', 'critical', 'Provider is outside the gateway allow list.', $event, [
                'allowed_providers' => $this->policy->allowedProviders,
            ]);
        }
        if (MgText::anyWildcard($this->policy->blockedProviders, $event->provider)) {
            $this->add('PROVIDER_BLOCKED', 'critical', 'Provider is blocked by policy.', $event);
        }
        if (MgText::anyWildcard($this->policy->blockedModels, $event->model)) {
            $this->add('MODEL_BLOCKED', 'critical', 'Model is blocked by policy.', $event, [
                'blocked_models' => $this->policy->blockedModels,
            ]);
        }
    }

    private function checkRegion(MgEvent $event): void
    {
        if ($this->policy->allowedRegions === []) {
            return;
        }
        if (!MgText::anyWildcard($this->policy->allowedRegions, $event->region)) {
            $this->add('DATA_REGION_NOT_ALLOWED', 'high', 'Request uses a model data region that is outside the policy allow list.', $event, [
                'allowed_regions' => $this->policy->allowedRegions,
            ]);
        }
    }

    private function checkContext(MgEvent $event): void
    {
        $window = $this->policy->contextWindowFor($event);
        if ($window !== null && $event->totalTokens() > $window) {
            $this->add('CONTEXT_WINDOW_EXCEEDED', 'high', 'Request token usage exceeds the configured model context window.', $event, [
                'tokens' => $event->totalTokens(),
                'context_window' => $window,
            ]);
        }
        if ($event->maxOutputTokens > $this->policy->maxOutputTokens) {
            $this->add('MAX_OUTPUT_TOO_HIGH', 'medium', 'Request asks for more output tokens than the gateway permits.', $event, [
                'requested_max_output_tokens' => $event->maxOutputTokens,
                'limit' => $this->policy->maxOutputTokens,
            ]);
        }
    }

    private function checkStreamingAndRetries(MgEvent $event): void
    {
        if ($event->streaming && $event->maxOutputTokens === 0) {
            $this->add('UNBOUNDED_STREAMING_OUTPUT', 'high', 'Streaming request has no max output token bound, so spend can run away during partial failures.', $event);
        }
        if ($event->retryable && $event->idempotencyKey === '') {
            $this->add('MISSING_IDEMPOTENCY_KEY', 'medium', 'Retryable model request has no idempotency key, which can double charge after gateway timeouts.', $event);
        }
    }

    private function checkSecrets(MgEvent $event): void
    {
        $text = $event->promptPreview . "\n" . MgText::flatten($event->metadata);
        foreach ($this->policy->secretPatterns as $pattern) {
            if (@preg_match('/' . str_replace('/', '\\/', $pattern) . '/', $text)) {
                if (preg_match('/' . str_replace('/', '\\/', $pattern) . '/', $text)) {
                    $this->add('SECRET_LIKE_PROMPT_MATERIAL', 'critical', 'Prompt preview or metadata appears to contain secret material.', $event, [
                        'input_digest' => MgText::shortHash($text),
                    ]);
                    return;
                }
            }
        }
    }

    private function checkCacheEfficiency(MgEvent $event): void
    {
        if ($event->inputTokens < 8000 || $event->cachedInputTokens <= 0) {
            return;
        }
        $ratio = $event->cachedInputTokens / max(1, $event->inputTokens);
        if ($ratio < $this->policy->minimumCacheRatio) {
            $this->add('LOW_PROMPT_CACHE_RATIO', 'low', 'Large prompt is barely using the provider prompt cache.', $event, [
                'cache_ratio' => round($ratio, 4),
                'minimum_cache_ratio' => $this->policy->minimumCacheRatio,
            ]);
        }
    }

    private function suggestCheaperRoute(MgEvent $event, int $actualCost): void
    {
        if ($actualCost <= 0) {
            return;
        }
        $best = null;
        foreach ($this->policy->priceBook as $provider => $models) {
            if ($this->policy->allowedProviders !== [] && !MgText::anyWildcard($this->policy->allowedProviders, (string)$provider)) {
                continue;
            }
            if (MgText::anyWildcard($this->policy->blockedProviders, (string)$provider)) {
                continue;
            }
            foreach ($models as $pattern => $price) {
                if (MgText::anyWildcard($this->policy->blockedModels, (string)$pattern)) {
                    continue;
                }
                $candidate = new MgEvent($event->id, $event->tenant, (string)$provider, (string)$pattern, $event->region, $event->timestamp, $event->inputTokens, $event->outputTokens, $event->cachedInputTokens, $event->maxOutputTokens, $event->streaming, $event->retryable, $event->idempotencyKey, $event->endpoint, '', [], $event->source, $event->line);
                $window = $this->policy->contextWindowFor($candidate);
                if ($window !== null && $event->totalTokens() > $window) {
                    continue;
                }
                $inputMicros = MgMoney::usdToMicros($price['input_per_mtok_usd'] ?? 0);
                $outputMicros = MgMoney::usdToMicros($price['output_per_mtok_usd'] ?? 0);
                $cachedMicros = MgMoney::usdToMicros($price['cached_input_per_mtok_usd'] ?? ($price['input_per_mtok_usd'] ?? 0));
                $paidInput = max(0, $event->inputTokens - $event->cachedInputTokens);
                $cost = MgMoney::tokenCostMicros($paidInput, $inputMicros)
                    + MgMoney::tokenCostMicros($event->cachedInputTokens, $cachedMicros)
                    + MgMoney::tokenCostMicros($event->outputTokens, $outputMicros);
                if ($best === null || $cost < $best['cost']) {
                    $best = ['provider' => (string)$provider, 'model' => (string)$pattern, 'cost' => $cost];
                }
            }
        }
        if ($best !== null && $best['cost'] < intdiv($actualCost * 65, 100)) {
            $this->add('CHEAPER_ROUTE_AVAILABLE', 'info', 'A cheaper configured route appears to fit the same token shape.', $event, [
                'current_estimated_usd' => MgMoney::microsToUsd($actualCost),
                'candidate_provider' => $best['provider'],
                'candidate_model' => $best['model'],
                'candidate_estimated_usd' => MgMoney::microsToUsd($best['cost']),
            ]);
        }
    }

    private function add(string $id, string $severity, string $message, MgEvent $event, array $evidence = []): void
    {
        $this->findings[] = new MgFinding(
            $id,
            MgSeverity::normalize($severity),
            $message,
            $event->tenant,
            $event->provider,
            $event->model,
            $event->id,
            array_merge([
                'source' => $event->source,
                'line' => $event->line,
                'region' => $event->region,
                'endpoint' => $event->endpoint,
            ], array_filter($evidence, static fn($value) => $value !== null && $value !== [])),
        );
    }
}

final class MgReporter
{
    public static function render(MgFirewall $firewall, string $format): string
    {
        return match ($format) {
            'json' => self::json($firewall),
            'markdown' => self::markdown($firewall),
            'sarif' => self::sarif($firewall),
            default => throw new RuntimeException('format must be json, markdown, or sarif'),
        };
    }

    private static function json(MgFirewall $firewall): string
    {
        return json_encode([
            'summary' => $firewall->summary(),
            'findings' => array_map(static fn(MgFinding $finding) => $finding->toArray(), $firewall->findings()),
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n";
    }

    private static function markdown(MgFirewall $firewall): string
    {
        $lines = ['# Model Gateway Spend Firewall', ''];
        foreach ($firewall->summary() as $key => $value) {
            if (is_array($value)) {
                $value = json_encode($value, JSON_UNESCAPED_SLASHES);
            }
            $lines[] = '- ' . str_replace('_', ' ', $key) . ': ' . (string)$value;
        }
        $lines[] = '';
        if ($firewall->findings() === []) {
            $lines[] = 'No spend, quota, routing, or prompt material findings were detected.';
            return implode("\n", $lines) . "\n";
        }
        $lines[] = '| Severity | Rule | Tenant | Provider | Model | Message |';
        $lines[] = '| --- | --- | --- | --- | --- | --- |';
        foreach ($firewall->findings() as $finding) {
            $cells = [$finding->severity, $finding->id, $finding->tenant ?? '', $finding->provider ?? '', $finding->model ?? '', $finding->message];
            $lines[] = '| ' . implode(' | ', array_map([self::class, 'cell'], $cells)) . ' |';
        }
        return implode("\n", $lines) . "\n";
    }

    private static function sarif(MgFirewall $firewall): string
    {
        $rules = [];
        foreach ($firewall->findings() as $finding) {
            $rules[$finding->id] = [
                'id' => $finding->id,
                'shortDescription' => ['text' => str_replace('_', ' ', strtolower($finding->id))],
                'helpUri' => 'https://github.com/kspavankrishna/VIBE-CODE',
            ];
        }
        $results = [];
        foreach ($firewall->findings() as $finding) {
            $results[] = [
                'ruleId' => $finding->id,
                'level' => in_array($finding->severity, ['critical', 'high'], true) ? 'error' : ($finding->severity === 'medium' ? 'warning' : 'note'),
                'message' => ['text' => $finding->message],
                'locations' => [[
                    'physicalLocation' => [
                        'artifactLocation' => ['uri' => (string)($finding->evidence['source'] ?? 'stdin')],
                        'region' => ['startLine' => (int)($finding->evidence['line'] ?? 1)],
                    ],
                ]],
                'properties' => $finding->toArray(),
            ];
        }
        return json_encode([
            'version' => '2.1.0',
            '$schema' => 'https://json.schemastore.org/sarif-2.1.0.json',
            'runs' => [[
                'tool' => ['driver' => [
                    'name' => 'ModelGatewaySpendFirewall',
                    'semanticVersion' => '1.0.0',
                    'informationUri' => 'https://github.com/kspavankrishna/VIBE-CODE',
                    'rules' => array_values($rules),
                ]],
                'invocations' => [['executionSuccessful' => !$firewall->failed()]],
                'results' => $results,
            ]],
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n";
    }

    private static function cell(string $value): string
    {
        return str_replace(["|", "\n", "\r"], ['\\|', ' ', ' '], $value);
    }
}

final class MgSelfTest
{
    public static function run(): string
    {
        $fixture = [
            [
                'id' => 'expensive-1',
                'tenant' => 'free-tier',
                'provider' => 'anthropic',
                'model' => 'claude-opus-4-20260401',
                'region' => 'eu',
                'timestamp' => '2026-04-10T12:00:00Z',
                'input_tokens' => 900000,
                'output_tokens' => 60000,
                'cached_input_tokens' => 1000,
                'max_output_tokens' => 0,
                'streaming' => true,
                'retryable' => true,
                'prompt_preview' => 'authorization: sk-abcdefghijklmnopqrstuvwxyz123456',
            ],
            [
                'id' => 'blocked-1',
                'tenant' => 'research',
                'provider' => 'unknownlab',
                'model' => 'legacy-preview-unsafe',
                'region' => 'moon',
                'timestamp' => '2026-04-10T12:00:30Z',
                'input_tokens' => 1200,
                'output_tokens' => 800,
                'max_output_tokens' => 20000,
            ],
        ];
        $policy = MgPolicy::fromArray([
            'tenant_daily_budgets_usd' => ['free-tier' => 1],
            'per_request_cap_usd' => 0.50,
            'allowed_providers' => ['openai', 'anthropic'],
            'blocked_models' => ['legacy-*', '*unsafe'],
            'allowed_regions' => ['us', 'eu'],
            'max_output_tokens' => 4096,
            'fail_on' => 'high',
        ]);
        $events = MgInputParser::parseText(json_encode(['events' => $fixture], JSON_THROW_ON_ERROR), 'self-test.json');
        $firewall = (new MgFirewall($policy))->analyze($events);
        $ids = array_map(static fn(MgFinding $finding) => $finding->id, $firewall->findings());
        foreach (['REQUEST_COST_CAP_EXCEEDED', 'TENANT_DAILY_BUDGET_EXCEEDED', 'UNBOUNDED_STREAMING_OUTPUT', 'MISSING_IDEMPOTENCY_KEY', 'SECRET_LIKE_PROMPT_MATERIAL', 'PROVIDER_NOT_ALLOWED', 'MODEL_BLOCKED', 'DATA_REGION_NOT_ALLOWED'] as $expected) {
            if (!in_array($expected, $ids, true)) {
                throw new RuntimeException('self-test missing finding ' . $expected);
            }
        }
        if (!$firewall->failed()) {
            throw new RuntimeException('self-test expected fail_on gate to fail');
        }
        return 'self-test ok: ' . count($firewall->findings()) . ' findings';
    }
}

final class MgCli
{
    public static function run(array $argv): int
    {
        try {
            $options = self::parse($argv);
            if ($options['example_policy']) {
                echo MgPolicy::exampleJson();
                return 0;
            }
            if ($options['self_test']) {
                echo MgSelfTest::run() . "\n";
                return 0;
            }
            $policyRaw = $options['policy'] === null ? [] : json_decode((string)file_get_contents($options['policy']), true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($policyRaw)) {
                throw new RuntimeException('policy JSON must be an object');
            }
            foreach ($options['overrides'] as $key => $value) {
                $policyRaw[$key] = $value;
            }
            $policy = MgPolicy::fromArray($policyRaw);
            $events = MgInputParser::parsePaths($options['inputs']);
            $firewall = (new MgFirewall($policy))->analyze($events);
            echo MgReporter::render($firewall, $options['format']);
            return $firewall->failed() ? 2 : 0;
        } catch (Throwable $error) {
            fwrite(STDERR, 'ModelGatewaySpendFirewall: ' . $error->getMessage() . "\n");
            return 1;
        }
    }

    private static function parse(array $argv): array
    {
        $options = [
            'inputs' => [],
            'policy' => null,
            'format' => 'json',
            'self_test' => false,
            'example_policy' => false,
            'overrides' => [],
        ];
        for ($i = 0; $i < count($argv); $i++) {
            $arg = $argv[$i];
            switch ($arg) {
                case '--input':
                    $options['inputs'][] = self::value($argv, ++$i, $arg);
                    break;
                case '--policy':
                    $options['policy'] = self::value($argv, ++$i, $arg);
                    break;
                case '--format':
                    $options['format'] = self::value($argv, ++$i, $arg);
                    break;
                case '--fail-on':
                    $options['overrides']['fail_on'] = self::value($argv, ++$i, $arg);
                    break;
                case '--per-request-cap-usd':
                    $options['overrides']['per_request_cap_usd'] = self::value($argv, ++$i, $arg);
                    break;
                case '--rpm-limit':
                    $options['overrides']['rpm_limit'] = (int)self::value($argv, ++$i, $arg);
                    break;
                case '--tpm-limit':
                    $options['overrides']['tpm_limit'] = (int)self::value($argv, ++$i, $arg);
                    break;
                case '--allow-provider':
                    $options['overrides']['allowed_providers'][] = self::value($argv, ++$i, $arg);
                    break;
                case '--block-provider':
                    $options['overrides']['blocked_providers'][] = self::value($argv, ++$i, $arg);
                    break;
                case '--block-model':
                    $options['overrides']['blocked_models'][] = self::value($argv, ++$i, $arg);
                    break;
                case '--allow-region':
                    $options['overrides']['allowed_regions'][] = self::value($argv, ++$i, $arg);
                    break;
                case '--self-test':
                    $options['self_test'] = true;
                    break;
                case '--example-policy':
                    $options['example_policy'] = true;
                    break;
                case '--help':
                    echo self::help();
                    exit(0);
                default:
                    if (str_starts_with($arg, '--')) {
                        throw new RuntimeException('unknown option ' . $arg);
                    }
                    $options['inputs'][] = $arg;
            }
        }
        if (!in_array($options['format'], ['json', 'markdown', 'sarif'], true)) {
            throw new RuntimeException('--format must be json, markdown, or sarif');
        }
        return $options;
    }

    private static function value(array $argv, int $index, string $flag): string
    {
        if ($index >= count($argv)) {
            throw new RuntimeException('missing value after ' . $flag);
        }
        return $argv[$index];
    }

    private static function help(): string
    {
        return <<<TXT
Usage: php ModelGatewaySpendFirewall.php [options] [events.json|events.jsonl ...]

Options:
  --input PATH                 Read JSON or JSONL events. May be repeated.
  --policy PATH                Read policy JSON with prices, budgets, and limits.
  --format json|markdown|sarif Render the report format. Default: json.
  --fail-on SEVERITY           Exit 2 on this severity or worse.
  --per-request-cap-usd USD    Override the single request cap.
  --rpm-limit N                Override tenant requests per minute.
  --tpm-limit N                Override tenant tokens per minute.
  --allow-provider NAME        Add an allowed provider pattern.
  --block-provider NAME        Add a blocked provider pattern.
  --block-model PATTERN        Add a blocked model pattern.
  --allow-region REGION        Add an allowed data region.
  --example-policy             Print a starter policy document.
  --self-test                  Run embedded regression checks.
TXT;
    }
}

if (PHP_SAPI === 'cli' && isset($argv) && realpath($argv[0] ?? '') === __FILE__) {
    exit(MgCli::run(array_slice($argv, 1)));
}

/*
This solves the quiet money leak and safety gap inside modern AI model gateway routing, where OpenAI, Anthropic, Google, Mistral, local inference, Laravel jobs, Symfony workers, edge functions, batch research pipelines, and DevOps bots all send token-heavy requests but nobody can prove the request was inside budget, inside quota, inside the allowed data region, and free of secret-looking prompt material before the invoice arrives. Built because in April 2026 many teams are no longer calling one model from one backend; they are brokering multi-provider AI traffic, retrying streamed responses, caching giant prompts, testing fallback routes, and letting agents call tools from production workflows, which makes spend governance a real engineering problem instead of a finance spreadsheet. Use it when you need a production-ready PHP CLI for AI gateway cost controls, LLM spend firewall checks, model routing audits, tenant token budgets, prompt cache efficiency, SARIF reporting, CI gates, provider allow lists, blocked model policies, context window checks, data residency guardrails, idempotency enforcement, and secret detection in request metadata. The trick: it normalizes raw JSON or JSONL gateway events, prices input tokens, cached input tokens, and output tokens in integer micros, then combines per-request caps, daily tenant budgets, RPM and TPM windows, streaming bounds, retry safety, model context limits, and cheaper-route suggestions in one deterministic pass. Drop this into a PHP repository, a GitHub Action, a Laravel scheduler, a Symfony console workflow, a self-hosted inference gateway, or a research batch runner before the model call is accepted, and it becomes searchable for AI gateway spend firewall, LLM cost guardrail, multi-provider model routing, PHP token budget CLI, prompt cache audit, agent DevOps quota gate, and production AI infrastructure cost control.
*/
