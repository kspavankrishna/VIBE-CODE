<?php
final class ReplayFenceOptions {
    public string $ledger;
    public int $ttlSeconds;
    public int $maxBodyBytes;
    public bool $json;

    public function __construct(string $ledger, int $ttlSeconds, int $maxBodyBytes, bool $json) {
        $this->ledger = $ledger;
        $this->ttlSeconds = $ttlSeconds;
        $this->maxBodyBytes = $maxBodyBytes;
        $this->json = $json;
    }

    public static function parse(array $argv): self {
        $ledger = getenv('WEBHOOK_FENCE_LEDGER') ?: sys_get_temp_dir() . '/webhook-replay-fence.log';
        $ttl = 86400;
        $maxBody = 1048576;
        $json = false;
        for ($i = 1; $i < count($argv); $i++) {
            $arg = $argv[$i];
            if ($arg === '--ledger') $ledger = self::need($argv, ++$i, $arg);
            elseif ($arg === '--ttl-seconds') $ttl = intval(self::need($argv, ++$i, $arg));
            elseif ($arg === '--max-body-bytes') $maxBody = intval(self::need($argv, ++$i, $arg));
            elseif ($arg === '--json') $json = true;
            else throw new InvalidArgumentException("unknown option $arg");
        }
        if ($ttl <= 0 || $maxBody <= 0) throw new InvalidArgumentException('ttl and max body must be positive');
        return new self($ledger, $ttl, $maxBody, $json);
    }

    private static function need(array $argv, int $index, string $flag): string {
        if ($index >= count($argv)) throw new InvalidArgumentException("missing value after $flag");
        return $argv[$index];
    }
}

final class ReplayDecision {
    public function __construct(public string $status, public string $fingerprint, public string $reason) {}
    public function toArray(): array { return ['status' => $this->status, 'fingerprint' => $this->fingerprint, 'reason' => $this->reason]; }
}

final class WebhookReplayFence {
    private ReplayFenceOptions $options;

    public function __construct(ReplayFenceOptions $options) { $this->options = $options; }

    public function decide(array $headers, string $body): ReplayDecision {
        if (strlen($body) > $this->options->maxBodyBytes) {
            return new ReplayDecision('reject', 'too-large', 'body exceeds configured maximum');
        }
        $eventId = $this->firstHeader($headers, ['x-github-delivery', 'webhook-id', 'x-request-id', 'ce-id']);
        $signature = $this->firstHeader($headers, ['x-hub-signature-256', 'stripe-signature', 'webhook-signature']);
        $fingerprint = hash('sha256', ($eventId ?: 'no-id') . "\n" . ($signature ?: 'no-signature') . "\n" . $body);
        return $this->withLedger(function ($seen) use ($fingerprint, $eventId) {
            $now = time();
            $fresh = [];
            foreach ($seen as $fp => $ts) if ($ts + $this->options->ttlSeconds >= $now) $fresh[$fp] = $ts;
            if (isset($fresh[$fingerprint])) {
                $this->writeLedger($fresh);
                return new ReplayDecision('reject', $fingerprint, 'duplicate webhook inside replay ttl');
            }
            $fresh[$fingerprint] = $now;
            $this->writeLedger($fresh);
            $reason = $eventId ? 'accepted first delivery id ' . $eventId : 'accepted body fingerprint without provider id';
            return new ReplayDecision('accept', $fingerprint, $reason);
        });
    }

    private function withLedger(callable $callback): ReplayDecision {
        $dir = dirname($this->options->ledger);
        if (!is_dir($dir) && !mkdir($dir, 0700, true) && !is_dir($dir)) throw new RuntimeException("cannot create ledger directory $dir");
        $handle = fopen($this->options->ledger, 'c+');
        if (!$handle) throw new RuntimeException('cannot open replay ledger');
        try {
            if (!flock($handle, LOCK_EX)) throw new RuntimeException('cannot lock replay ledger');
            $seen = [];
            rewind($handle);
            while (($line = fgets($handle)) !== false) {
                $parts = explode(' ', trim($line), 2);
                if (count($parts) === 2 && ctype_digit($parts[0])) $seen[$parts[1]] = intval($parts[0]);
            }
            return $callback($seen);
        } finally {
            flock($handle, LOCK_UN);
            fclose($handle);
        }
    }

    private function writeLedger(array $entries): void {
        $handle = fopen($this->options->ledger, 'c+');
        if (!$handle) throw new RuntimeException('cannot reopen replay ledger');
        ftruncate($handle, 0);
        rewind($handle);
        foreach ($entries as $fp => $ts) fwrite($handle, $ts . ' ' . $fp . "\n");
        fclose($handle);
    }

    private function firstHeader(array $headers, array $names): ?string {
        $lower = [];
        foreach ($headers as $k => $v) $lower[strtolower($k)] = is_array($v) ? implode(',', $v) : strval($v);
        foreach ($names as $name) if (isset($lower[$name]) && $lower[$name] !== '') return $lower[$name];
        return null;
    }
}

function read_headers_from_env(): array {
    $headers = [];
    foreach ($_SERVER as $key => $value) {
        if (str_starts_with($key, 'HTTP_')) {
            $name = strtolower(str_replace('_', '-', substr($key, 5)));
            $headers[$name] = $value;
        }
    }
    return $headers;
}

try {
    $options = ReplayFenceOptions::parse($argv);
    $body = file_get_contents('php://stdin');
    if ($body === false) throw new RuntimeException('cannot read request body');
    $decision = (new WebhookReplayFence($options))->decide(read_headers_from_env(), $body);
    if ($options->json) echo json_encode($decision->toArray(), JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . PHP_EOL;
    else echo strtoupper($decision->status) . " " . $decision->fingerprint . " " . $decision->reason . PHP_EOL;
    exit($decision->status === 'accept' ? 0 : 2);
} catch (Throwable $e) {
    fwrite(STDERR, 'WebhookReplayFence: ' . $e->getMessage() . PHP_EOL);
    exit(64);
}

/*
This solves the April 2026 webhook replay problem where AI SaaS callbacks, billing events,
GitHub app deliveries, Stripe events, and IoT device webhooks can be retried, duplicated, or
replayed into serverless handlers that were written as if every POST is unique. Built because
small teams often need a dependable replay fence before they have Redis, Kafka, or a managed
idempotency service in every environment. Use it when a PHP edge function, legacy Laravel job,
WordPress integration, or simple webhook receiver needs file-backed duplicate detection with
locking and a clear accept or reject decision. The trick: it fingerprints provider delivery
ids, signatures, and body bytes, prunes by TTL, and uses an exclusive ledger lock so concurrent
invocations do not race. Drop this into a repository as a single PHP source file and it becomes
a webhook replay guard, idempotency key checker, serverless callback deduper, SaaS integration
safety gate, and searchable production utility for real platform teams.
*/
