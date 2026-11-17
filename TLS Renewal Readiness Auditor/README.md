# TLS Renewal Readiness Auditor

A PowerShell 7 script that checks every address behind every TLS endpoint you own and tells you whether your certificate renewal will survive the shrinking lifetimes coming from the CA/Browser Forum. It catches the outages that a plain "days until expiry" check misses: half renewed load balancer pools, missing intermediates, intermediates that expire before the leaf and stale certificates that come back after a rollback.

**Language:** PowerShell | **Lines:** 1526 | **Added:** 2026-11-17

## What this solves

Public TLS certificates are getting shorter. Under CA/Browser Forum ballot SC-081 the maximum lifetime of a publicly trusted TLS certificate dropped from 398 days to 200 days for certificates issued from 15 March 2026. It drops to 100 days from 15 March 2027 and to 47 days from 15 March 2029. A team that renewed by hand once a year now renews twice a year. Next year it is four times. By 2029 it is roughly eight or more times a year per certificate. Anything manual in that loop is going to break and most certificate monitoring was designed for the old world.

The usual expiry check connects to one hostname, reads the leaf certificate and compares `NotAfter` with a fixed threshold such as 30 days. That approach has five blind spots that cause real outages:

1. **One name, many servers.** DNS returns several addresses. The check lands on whichever one the resolver hands out. If renewal reached three nodes out of four, the fourth node keeps serving the old certificate until it expires and a quarter of your users see an error.
2. **The chain, not the leaf.** A leaf can be valid for months while the intermediate it chains to expires next week. Renewing the leaf from the same stale bundle does not fix it.
3. **Missing intermediates.** Browsers quietly repair a server that forgets to send its intermediate by fetching it through the AIA URL or using a cache. curl, Go, Java, Python requests, Node and most embedded or IoT clients do not. The browser test passes while your API clients fail.
4. **Fixed thresholds stop making sense.** A 30 day warning on a 47 day certificate fires almost all the time. The same warning on a 6 day short lived certificate fires forever. Thresholds need to scale with the lifetime.
5. **Regressions.** A node restored from an old image, a Kubernetes secret rolled back or a CDN config reverted can put an older certificate back in service. Its expiry still looks fine today, so nothing alerts until it is too late.

This script handles all five. It also covers mail and database endpoints through STARTTLS, which is where forgotten certificates tend to live.

## Why I built it

I kept seeing the same incident shape. Monitoring said green, a customer said "your API is down" and the cause turned out to be one node behind a load balancer. Other times it was a chain file that was never updated or a mail relay nobody had looked at in a year. Each tool I tried answered a narrower question. `openssl s_client` shows one connection at a time. Browser based checkers report the browser view, which hides missing intermediates. Blackbox exporters give you one expiry number per probe and stop there.

With lifetimes going down to 200 days this year and 100 days next year, renewal mistakes stop being rare events. They become routine. I wanted one script I could run in CI, from cron or on a laptop, that gives a straight answer per endpoint, writes metrics Prometheus can scrape and keeps a baseline so it notices when things go backwards.

## When to use it

- As a scheduled job that writes a Prometheus textfile, so alerting rules can page on `tls_readiness_target_severity` or on the effective expiry timestamp.
- In a deploy pipeline right after a certificate rotation, to prove every node picked up the new certificate and that the chain is complete.
- Before the 15 March 2027 cutover, to inventory which certificates are still on long lifetimes and how much your renewal cadence will increase.
- Against certificate files in a repository, a secrets mount or a build output, with `-Path`, to catch expired bundles before they ship.
- For internal services on a private CA, with `-TrustedRootPath`, so trust is judged against your own roots instead of the public ones.

It is not a vulnerability scanner. It does not enumerate every cipher suite a server offers. It tells you what a modern client actually negotiates and whether the certificate side of things will hold up.

## How it works

The script has two halves: a compiled network probe and a PowerShell analysis layer.

**Target parsing.** `ConvertTo-TargetSpec` accepts bare hosts, `host:port`, bracketed IPv6 and URLs with the schemes `https`, `tls`, `wss`, `ldaps`, `smtps`, `imaps`, `smtp`, `imap`, `postgres` and `postgresql`. Paths are ignored. A `?sni=name` query overrides the server name sent in the handshake, which is how you test a specific backend IP with the public hostname. User info such as `user:password@` is stripped before anything else happens, so a database URL pasted from a config file never shows up in output, baselines or metrics. Host names go through IDN conversion so a Unicode name is compared in its ASCII form. `Read-TargetList` merges `-Target` and `-TargetFile`, drops comments and removes duplicates.

**Probing.** The network work lives in a small C# class, `TlsRenewalReadinessV1.Prober`, compiled once per process by `Import-ProbeType`. I did it this way because a PowerShell script block used as a TLS validation callback can only run safely on the calling thread, which rules out real concurrency and real timeouts. `Prober.Run` first resolves every target with `Dns.GetHostAddressesAsync`, filters by `-AddressFamily`, sorts IPv4 first and keeps at most `-MaxAddressesPerHost` addresses. Then `ProbeOne` connects to each address under one `CancellationTokenSource`, so connect, plaintext upgrade and handshake together never exceed `-TimeoutMs`. A semaphore caps work at `-Concurrency`.

For STARTTLS, `Preamble` speaks just enough SMTP (`EHLO`, `STARTTLS`, multi line reply handling in `ExpectSmtp`), IMAP (`a1 STARTTLS`) or PostgreSQL (the 8 byte SSLRequest with code 80877103) to upgrade the connection. `ReadLine` reads one byte at a time on purpose: reading ahead would swallow the first bytes of the TLS handshake. The validation callback accepts every certificate so a broken certificate is still captured and analysed rather than turned into an opaque handshake error. It copies the certificates the server actually sent out of the chain's `ExtraStore`.

**Chain analysis.** `Invoke-ChainBuild` rebuilds the chain with the time pinned to the audit time, revocation off unless `-CheckRevocation` is set and AIA downloads on unless `-NoAiaFetch` is set. With `-TrustedRootPath` it switches to `CustomRootTrust`, so only your roots count. Statuses listed in `$TrustFailureStatuses` become a `ChainUntrusted` failure. Time problems are judged separately, so an expired leaf is reported as `Expired` and not buried in a chain error.

The key check for missing intermediates is structural. `Measure-Certificate` takes the validated path and checks that every certificate except the leaf and a self signed anchor is among the certificates the server actually sent. That works the same on Windows, Linux and macOS, even when the operating system has the intermediate cached and would build the chain anyway. If the runtime reported no peer certificates at all, the script marks the observation `PresentedKnown = false` and skips this check rather than guessing.

**Expiry thresholds that scale.** The effective expiry is the earliest `NotAfter` across the whole path, not just the leaf. If an intermediate is the limiter you get `ChainExpiresFirst`. The warning line is the smaller of `-WarnDays` and the part of the lifetime left after the renewal point set by `-RenewAtFraction`. The critical line is the smaller of `-CriticalDays` and 10 percent of the lifetime. With the defaults (21 days, 7 days and a renewal point at two thirds of the lifetime) a 398 day certificate warns at 21 days left. A 90 day ACME certificate warns at 21 days left, about a week after automation should have renewed it. A 47 day certificate warns at about 15.7 days left and a 6 day certificate at 2 days left. Nothing warns forever just because the lifetime is short.

**The lifetime schedule.** `$CabLifetimeSchedule` holds the SC-081 dates and `Get-CabMaxLifetimeDays` returns the cap for any issue date. For publicly trusted leaves `Measure-Certificate` raises `ExceedsLifetimeCap` if a certificate is longer than the cap allowed when it was issued. It also raises `CadenceShrink` when the next renewal will fall under a lower cap, with the renewals per year before and after. That finding becomes a warning when the cadence more than doubles.

**Other certificate rules.** Missing or unparsable subjectAltName, host name mismatch (`Test-DnsPattern` follows the rules modern clients use: a wildcard covers exactly one left label and never sits right above a public suffix like `*.com`), RSA keys below 2048 bits, EC keys below 256 bits, SHA1 or MD5 signatures anywhere except a root's self signature, OCSP Must-Staple, missing revocation information on certificates longer than `$ShortLivedDays`, missing embedded SCTs, server sent certificates that are already expired (the old cross signed root trap) and certificates sent but not used on the path. On the protocol side anything below TLS 1.2 fails. On TLS 1.2 it also flags weak ciphers, static RSA key exchange and CBC suites.

**Per target folding.** `Merge-TargetReport` groups observations by target. A finding that hits every address is reported once. A finding that hits some addresses lists them. Different leaf certificates across addresses raise `MixedCertificates` with the fingerprint, expiry and addresses for each. If no address answered you get `Unreachable`. If some failed you get `PartialReachability`, except when this machine simply has no IPv6 route while IPv4 works, which is reported as `IPv6PathUnavailable` at info level so a v4 only CI runner does not spam warnings. The reported days left always comes from the worst address, because that is what your unluckiest user sees.

**Baselines.** `Compare-Baseline` compares current leaves with the stored ones. A newest expiry more than a day older than the baseline is `CertificateRegressed`. A new certificate that reuses the old public key (same SPKI SHA-256) is `KeyReused`. A new issuer is `IssuerChanged`, which is your cue to check CAA records and anything that pins an intermediate. `Save-Baseline` only replaces entries for targets that answered, so a network blip does not wipe history. `Write-FileAtomic` writes to a temporary file in the same directory and renames it, so a crash never leaves a half written baseline or metrics file.

**Metrics.** `Write-PrometheusFile` writes `tls_readiness_probe_success`, `tls_readiness_effective_not_after_seconds`, `tls_readiness_leaf_not_after_seconds`, `tls_readiness_leaf_lifetime_seconds`, `tls_readiness_findings`, `tls_readiness_target_severity` and `tls_readiness_last_run_timestamp_seconds` in the text exposition format, ready for the node_exporter textfile collector.

## Usage

Requires PowerShell 7.4 or newer on Windows, Linux or macOS. There are no modules to install.

```powershell
# Quick look at a few endpoints, including a mail relay and a database
./TlsRenewalReadinessAuditor.ps1 -Target example.com, api.example.com:8443, smtp://mail.example.com:587, postgres://db.internal:5432

# Test one backend directly while sending the public name as SNI
./TlsRenewalReadinessAuditor.ps1 -Target 'tls://10.0.4.17:443?sni=www.example.com'

# Scheduled run: target list, baseline, Prometheus textfile, exit 1 on warnings
./TlsRenewalReadinessAuditor.ps1 -TargetFile ./targets.txt `
    -BaselinePath ./state/tls-baseline.json -UpdateBaseline `
    -PrometheusPath /var/lib/node_exporter/textfile/tls_readiness.prom `
    -FailOnWarn

# Internal services on a private CA, IPv4 only, JSON for another tool
./TlsRenewalReadinessAuditor.ps1 -TargetFile ./internal.txt -TrustedRootPath ./pki/root-ca.pem -AddressFamily IPv4 -Format Json

# Certificate files in a repository or secrets mount
./TlsRenewalReadinessAuditor.ps1 -Path ./deploy/certs -Format Ndjson
```

A target file has one target per line. Anything after `#` is a comment.

```text
# public edge
example.com
api.example.com:8443
smtp://mail.example.com:587
imap://mail.example.com
postgres://reporting-db.internal
```

Parameters: `-Target`, `-TargetFile`, `-Path`, `-Format` (Table, Json or Ndjson), `-Concurrency` (default 16), `-TimeoutMs` (default 8000), `-MaxAddressesPerHost` (default 8), `-AddressFamily` (Any, IPv4 or IPv6), `-WarnDays` (default 21), `-CriticalDays` (default 7), `-RenewAtFraction` (default 0.6667), `-TrustedRootPath`, `-NoAiaFetch`, `-CheckRevocation`, `-BaselinePath`, `-UpdateBaseline`, `-PrometheusPath`, `-FailOnWarn` and `-Now` for replaying an audit at a fixed time.

Exit codes: 0 when nothing failed, 1 when warnings exist and `-FailOnWarn` is set, 2 when any target failed and 3 for usage or internal errors. The error text for code 3 goes to stderr.

## Notes

- Only PowerShell 7.4 and newer are supported. The typed subjectAltName and AIA parsers come from .NET 7 or later. Windows PowerShell 5.1 cannot cancel a handshake cleanly.
- The handshake uses the operating system's default protocol set. A server that only speaks TLS 1.0 or 1.1 will fail the handshake. That failure is reported as `Unreachable` with the handshake error. It is broken for modern clients either way.
- The missing intermediate check depends on .NET reporting the certificates the peer sent. When it does not, the check is skipped and `PresentedKnown` is false in the JSON. It never guesses.
- `-CheckRevocation` makes real CRL and OCSP requests and can be slow. It is off by default because many public CAs have retired OCSP and rely on CRLs.
- Certificate files can be PEM bundles or single DER certificates. PKCS#12 and PKCS#7 files are refused on purpose, so the script never touches a password protected key store. If a PEM file also holds a private key you get a `PrivateKeyColocated` note. The key itself is never decoded.
- The lifetime caps are defined in one table, `$CabLifetimeSchedule`, so if the CA/Browser Forum changes dates again it is a one line edit.
- Running it against hosts you do not operate is allowed by TLS itself, but keep `-Concurrency` modest and point it at your own infrastructure.
