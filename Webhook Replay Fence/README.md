# Webhook Replay Fence

Webhook providers retry. Your handler charges the card twice, sends the email twice or provisions the account twice. This is a single file PHP gate that fingerprints an incoming webhook, checks a file backed ledger under an exclusive lock and answers accept or reject before your business logic runs.

**Language:** PHP | **Lines:** 143 | **Added:** 2026-05-24

## What this solves

This solves the April 2026 webhook replay problem where AI SaaS callbacks, billing events, GitHub app deliveries, Stripe events and IoT device webhooks can be retried, duplicated or replayed into serverless handlers that were written as if every POST is unique. Almost every provider retries on timeout, on a 5xx, on a connection reset, and some retry when your 200 response was slow enough that their client gave up before reading it. The delivery succeeded. The provider does not know that. You get the same payload again, sometimes minutes later, sometimes in the same second from a parallel worker.

The failure mode is quiet and expensive. A duplicated `invoice.payment_succeeded` extends a subscription twice. A duplicated GitHub `push` delivery kicks off a second CI run and doubles your build minutes. A duplicated device webhook writes two rows for one physical event and every downstream aggregate is wrong from that point on. Nobody notices at the moment it happens. Support notices a week later when a customer sees two charges, or finance notices at month end when the usage numbers do not reconcile with the provider dashboard. By then you are reconstructing state from logs.

The second failure mode is adversarial. If an attacker captures one signed webhook body, they can post it back to your public endpoint as many times as they like. Signature verification passes every time, because the signature is valid, it just is not fresh. Signature checks prove origin, not uniqueness. Without a replay fence a valid signature is a reusable token.

The usual answer is Redis with a SETNX on the delivery id, or a unique index in Postgres, or a managed idempotency service. All fine when they exist. The problem is the environments where they do not: a PHP edge function, a staging box, a WordPress site, a legacy Laravel app whose queue driver is still the database, a customer self hosted deployment you do not control. That is the gap this file fills.

## Why I built it

Small teams often need a dependable replay fence before they have Redis, Kafka or a managed idempotency service in every environment. The tooling that does exist tends to be framework bound: an idempotency middleware that only works inside one framework's request cycle, or a SaaS product that wants an outbound call on the hot path of an inbound webhook. Neither is usable from a shell pipe, a cron script or a handler in a language you did not pick.

So this is deliberately boring. One PHP source file, no Composer dependencies, no daemon. It reads the body from stdin, reads headers from the environment, writes one append style ledger file and exits with a status code you can branch on from bash. If you later move to Redis, you delete this file and nothing else changes, because the contract is just an exit code.

## When to use it

- A Stripe or GitHub webhook endpoint that triggers a charge, a provisioning step or an email, where running it twice is visibly wrong to the customer.
- A serverless PHP handler that has no shared cache attached and no realistic path to getting one this quarter.
- A cron or shell pipeline that consumes queued webhook payloads and needs a duplicate check before handing off to the real worker.
- A self hosted or customer installed integration where you cannot assume Redis, a message broker or even a writable database exists.
- A public endpoint you want fenced against someone replaying one captured, validly signed payload over and over.
- A staging environment where a provider is aggressively retrying and you want to see, in a log, exactly which deliveries are duplicates.

## How it works

`ReplayFenceOptions::parse()` reads `$argv` and supports four flags: `--ledger`, `--ttl-seconds`, `--max-body-bytes` and `--json`. The ledger path also falls back to the `WEBHOOK_FENCE_LEDGER` environment variable and then to `sys_get_temp_dir() . '/webhook-replay-fence.log'`. TTL defaults to 86400 seconds and the body cap to 1048576 bytes. Any unknown flag, a flag missing its value or a non positive TTL or body cap throws `InvalidArgumentException`, which the top level try block turns into exit code 64.

The body arrives on stdin via `file_get_contents('php://stdin')`. Headers do not arrive on stdin. `read_headers_from_env()` walks `$_SERVER`, picks every key starting with `HTTP_`, strips the prefix, lowercases it and converts underscores to dashes, so `HTTP_X_GITHUB_DELIVERY` becomes `x-github-delivery`. That is the CGI convention, which means this drops straight into a PHP CGI or FastCGI setup, and from a plain shell you have to export those variables yourself.

`WebhookReplayFence::decide()` does the actual work. First the size guard: if the body is longer than `maxBodyBytes` it returns a reject immediately, before touching the ledger, so an oversized payload cannot make you buffer or hash unbounded input. Then `firstHeader()` looks for a provider delivery id among `x-github-delivery`, `webhook-id`, `x-request-id` and `ce-id` (that last one is the CloudEvents id), and for a signature among `x-hub-signature-256`, `stripe-signature` and `webhook-signature`. Header lookup is case insensitive and array values are joined with commas. The fingerprint is a SHA-256 of the delivery id, the signature and the raw body joined by newlines, with the literal strings `no-id` and `no-signature` substituted when a header is absent. That composition matters: the delivery id alone would miss providers that do not send one, and the body alone would collide across two legitimately identical events that carry different ids. Including all three means a repeat is only a repeat when the provider identity, the signature and the bytes all match.

Concurrency is handled by `withLedger()`, which is the part worth reading closely. It creates the ledger directory at mode 0700 if missing, opens the ledger with `fopen($path, 'c+')`, which creates the file without truncating it, and takes `flock($handle, LOCK_EX)`. Everything after that, reading, TTL pruning, the duplicate test and the rewrite, happens inside that exclusive lock, and the lock is released in a `finally` block so a thrown exception cannot leave it held. Two PHP processes handling two simultaneous copies of the same delivery serialise here: one wins and gets `accept`, the other blocks, then reads the fresh entry and gets `reject`. Without the lock the classic read then write race would let both through.

The ledger is a plain text file, one entry per line, formatted as `timestamp fingerprint`. Parsing splits on the first space and skips any line whose first field is not all digits, so a truncated or hand edited file degrades to dropped entries instead of a crash. On every decision the fence rebuilds a `$fresh` map containing only entries where `$ts + ttlSeconds >= $now`, which is the TTL prune, then either rejects (fingerprint already present) or inserts the new fingerprint at the current time. Either way `writeLedger()` truncates the file and writes the pruned set back, so expired fingerprints are garbage collected as a side effect of normal traffic and the file does not grow forever. Note that `writeLedger()` opens its own handle rather than reusing the locked one, which is safe here because the caller still holds the exclusive lock for the duration.

The result is a `ReplayDecision` with three fields: `status`, `fingerprint` and `reason`. The entry point prints it as a single line by default, or as pretty printed JSON with `--json`, then exits 0 on accept and 2 on reject.

## Usage

```bash
# Accept or reject a single webhook. Body on stdin, headers as HTTP_* env vars.
HTTP_X_GITHUB_DELIVERY=8f2a1c30-1111-2222-3333-444455556666 \
HTTP_X_HUB_SIGNATURE_256=sha256=abc123 \
  php WebhookReplayFence.php --ledger /var/lib/fence/webhooks.log \
                             --ttl-seconds 86400 \
                             --max-body-bytes 1048576 < payload.json
# ACCEPT 4f1c... accepted first delivery id 8f2a1c30-1111-2222-3333-444455556666

# Same payload again inside the TTL
HTTP_X_GITHUB_DELIVERY=8f2a1c30-1111-2222-3333-444455556666 \
HTTP_X_HUB_SIGNATURE_256=sha256=abc123 \
  php WebhookReplayFence.php --json < payload.json
# {
#     "status": "reject",
#     "fingerprint": "4f1c...",
#     "reason": "duplicate webhook inside replay ttl"
# }

# Gate a real handler on the exit code
if php WebhookReplayFence.php --ledger /var/lib/fence/webhooks.log < payload.json; then
  php process_webhook.php < payload.json
else
  echo "duplicate, skipping"
fi

# Ledger path from the environment instead of a flag
export WEBHOOK_FENCE_LEDGER=/var/lib/fence/webhooks.log
php WebhookReplayFence.php --json < payload.json
```

Calling it in process instead of by shell:

```php
$options  = new ReplayFenceOptions('/var/lib/fence/webhooks.log', 86400, 1048576, false);
$decision = (new WebhookReplayFence($options))->decide($headers, $rawBody);
if ($decision->status === 'reject') { http_response_code(200); return; }
```

## Notes

- Exit codes: 0 accept, 2 reject, 64 for any thrown error (bad flags, unreadable stdin, unwritable ledger). Errors go to STDERR with a `WebhookReplayFence:` prefix.
- It does not verify signatures. The signature header is fingerprint input only. Keep your existing HMAC check, this runs beside it, not instead of it.
- Headers are read from `$_SERVER` `HTTP_*` keys only. Nothing on the command line or stdin sets a header, so from a bare shell you must export them, and a missing delivery id silently degrades to body only fingerprinting under the `no-id` placeholder.
- The whole ledger is loaded into memory and rewritten on every decision, so cost is linear in the number of live fingerprints. Fine for thousands within a TTL window, wrong choice for very high volume endpoints. Shorten `--ttl-seconds` to keep the file small.
- `flock` is advisory and depends on the filesystem. It is reliable on a local disk, not dependable on NFS or on separate machines sharing a network mount. Single host only.
- An oversized body is rejected with the literal string `too-large` in the `fingerprint` field rather than a hash, since it never gets hashed. Anything parsing that field should expect it.
- Requires PHP 8.0 or newer for `str_starts_with` and constructor property promotion. No Composer packages, no extensions beyond the defaults.
