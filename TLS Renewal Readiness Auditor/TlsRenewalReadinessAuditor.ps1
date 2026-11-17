#Requires -Version 7.4
<#
.SYNOPSIS
    Audits live TLS endpoints and certificate files for renewal readiness under shrinking certificate lifetimes.

.DESCRIPTION
    Probes every resolved address of every target (direct TLS, SMTP STARTTLS, IMAP STARTTLS or the
    PostgreSQL SSLRequest), rebuilds the chain, and reports expiry against lifetime relative thresholds,
    the CA/Browser Forum lifetime schedule, chains that only work through AIA fetching, mixed certificates
    behind one name, regressions against a stored baseline and weak keys or protocols.
    See README.md next to this file for the full explanation.

.EXAMPLE
    ./TlsRenewalReadinessAuditor.ps1 -Target example.com, smtp://mail.example.com:587 -Format Table

.EXAMPLE
    ./TlsRenewalReadinessAuditor.ps1 -TargetFile ./targets.txt -BaselinePath ./tls-baseline.json -UpdateBaseline -PrometheusPath /var/lib/node_exporter/tls.prom
#>
[CmdletBinding()]
param(
    [string[]] $Target,

    [string] $TargetFile,

    [string[]] $Path,

    [ValidateSet('Table', 'Json', 'Ndjson')]
    [string] $Format = 'Table',

    [ValidateRange(1, 256)]
    [int] $Concurrency = 16,

    [ValidateRange(500, 120000)]
    [int] $TimeoutMs = 8000,

    [ValidateRange(1, 64)]
    [int] $MaxAddressesPerHost = 8,

    [ValidateSet('Any', 'IPv4', 'IPv6')]
    [string] $AddressFamily = 'Any',

    [ValidateRange(0.0, 400.0)]
    [double] $WarnDays = 21,

    [ValidateRange(0.0, 400.0)]
    [double] $CriticalDays = 7,

    [ValidateRange(0.5, 0.95)]
    [double] $RenewAtFraction = 0.6667,

    [string[]] $TrustedRootPath,

    [switch] $NoAiaFetch,

    [switch] $CheckRevocation,

    [string] $BaselinePath,

    [switch] $UpdateBaseline,

    [string] $PrometheusPath,

    [switch] $FailOnWarn,

    [datetime] $Now = [datetime]::UtcNow
)

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------------------------

$script:ProbeTypeName = 'TlsRenewalReadinessV1.Prober'
$script:SeverityRank = @{ ok = 0; info = 0; warn = 1; fail = 2 }

# CA/Browser Forum ballot SC-081 maximum validity for publicly trusted TLS leaf certificates.
# Each row applies to certificates issued on or after From (UTC). Ordered newest first.
$script:CabLifetimeSchedule = @(
    [pscustomobject]@{ From = [datetime]::new(2029, 3, 15, 0, 0, 0, [DateTimeKind]::Utc); MaxDays = 47 }
    [pscustomobject]@{ From = [datetime]::new(2027, 3, 15, 0, 0, 0, [DateTimeKind]::Utc); MaxDays = 100 }
    [pscustomobject]@{ From = [datetime]::new(2026, 3, 15, 0, 0, 0, [DateTimeKind]::Utc); MaxDays = 200 }
    [pscustomobject]@{ From = [datetime]::MinValue; MaxDays = 398 }
)

# Certificates at or below this lifetime are short lived and exempt from revocation information.
$script:ShortLivedDays = 10.0

$script:WeakSignatureOids = @{
    '1.2.840.113549.1.1.2' = 'md2WithRSA'
    '1.2.840.113549.1.1.4' = 'md5WithRSA'
    '1.2.840.113549.1.1.5' = 'sha1WithRSA'
    '1.2.840.10045.4.1'    = 'ecdsa-with-SHA1'
    '1.2.840.10040.4.3'    = 'dsa-with-SHA1'
}

$script:LegacyProtocols = @('Ssl2', 'Ssl3', 'Tls', 'Tls11')

# Chain statuses that mean "a client will not trust this". Time validity is judged separately.
$script:TrustFailureStatuses = @(
    'NotSignatureValid', 'UntrustedRoot', 'Cyclic', 'InvalidExtension', 'InvalidPolicyConstraints',
    'InvalidBasicConstraints', 'InvalidNameConstraints', 'HasNotSupportedNameConstraint',
    'HasNotDefinedNameConstraint', 'HasNotPermittedNameConstraint', 'HasExcludedNameConstraint',
    'PartialChain', 'ExplicitDistrust', 'HasNotSupportedCriticalExtension', 'NotValidForUsage'
)

$script:CertFileExtensions = @('.pem', '.crt', '.cer', '.der', '.cert')
$script:MaxCertFileBytes = 4MB

# ---------------------------------------------------------------------------------------------
# Network probe. Compiled once per process so it can run on thread pool threads with real
# cancellation, which a PowerShell script block callback cannot do safely.
# ---------------------------------------------------------------------------------------------

$script:ProbeSource = @'
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TlsRenewalReadinessV1
{
    public sealed class TargetSpec
    {
        public string Label;
        public string HostName;
        public int Port;
        public string Mode;
        public string Sni;
    }

    public sealed class ProbeResult
    {
        public string Label;
        public string HostName;
        public int Port;
        public string Mode;
        public string Sni;
        public string Address = "";
        public string Family = "";
        public bool Success;
        public string Error;
        public string ErrorStage;
        public string SocketError;
        public byte[] Leaf;
        public List<byte[]> Presented = new List<byte[]>();
        public string Protocol;
        public string CipherSuite;
        public long ElapsedMs;
    }

    internal sealed class ProbeRequest
    {
        public TargetSpec Spec;
        public IPAddress Address;
    }

    public static class Prober
    {
        public static ProbeResult[] Run(TargetSpec[] specs, int concurrency, int timeoutMs, int maxAddresses, string family)
        {
            var gate = new SemaphoreSlim(Math.Max(1, concurrency));
            try
            {
                var resolved = new List<ProbeRequest>[specs.Length];
                var dnsFailures = new ProbeResult[specs.Length];
                var dnsTasks = new Task[specs.Length];
                for (int i = 0; i < specs.Length; i++)
                {
                    int idx = i;
                    dnsTasks[idx] = Task.Run(async () =>
                    {
                        await gate.WaitAsync().ConfigureAwait(false);
                        try
                        {
                            resolved[idx] = await Resolve(specs[idx], timeoutMs, maxAddresses, family).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            var r = NewResult(specs[idx], null);
                            r.ErrorStage = "dns";
                            r.Error = ex is OperationCanceledException ? "name resolution timed out after " + timeoutMs + " ms" : Flatten(ex);
                            dnsFailures[idx] = r;
                        }
                        finally
                        {
                            gate.Release();
                        }
                    });
                }
                Task.WaitAll(dnsTasks);

                var requests = new List<ProbeRequest>();
                for (int i = 0; i < specs.Length; i++)
                {
                    if (resolved[i] != null) requests.AddRange(resolved[i]);
                }

                var results = new ProbeResult[requests.Count];
                var probeTasks = new Task[requests.Count];
                for (int i = 0; i < requests.Count; i++)
                {
                    int idx = i;
                    probeTasks[idx] = Task.Run(async () =>
                    {
                        await gate.WaitAsync().ConfigureAwait(false);
                        try
                        {
                            results[idx] = await ProbeOne(requests[idx], timeoutMs).ConfigureAwait(false);
                        }
                        finally
                        {
                            gate.Release();
                        }
                    });
                }
                Task.WaitAll(probeTasks);

                var all = new List<ProbeResult>();
                foreach (var f in dnsFailures) if (f != null) all.Add(f);
                all.AddRange(results);
                return all.ToArray();
            }
            finally
            {
                gate.Dispose();
            }
        }

        static ProbeResult NewResult(TargetSpec s, IPAddress address)
        {
            var r = new ProbeResult();
            r.Label = s.Label;
            r.HostName = s.HostName;
            r.Port = s.Port;
            r.Mode = s.Mode;
            r.Sni = s.Sni;
            if (address != null)
            {
                r.Address = address.ToString();
                r.Family = address.AddressFamily == AddressFamily.InterNetworkV6 ? "IPv6" : "IPv4";
            }
            return r;
        }

        static async Task<List<ProbeRequest>> Resolve(TargetSpec s, int timeoutMs, int maxAddresses, string family)
        {
            IPAddress literal;
            IPAddress[] addresses;
            if (IPAddress.TryParse(s.HostName, out literal))
            {
                addresses = new[] { literal };
            }
            else
            {
                using (var cts = new CancellationTokenSource(timeoutMs))
                {
                    addresses = await Dns.GetHostAddressesAsync(s.HostName, cts.Token).ConfigureAwait(false);
                }
            }

            var filtered = addresses
                .Where(a => family == "Any"
                    || (family == "IPv4" && a.AddressFamily == AddressFamily.InterNetwork)
                    || (family == "IPv6" && a.AddressFamily == AddressFamily.InterNetworkV6))
                .GroupBy(a => a.ToString())
                .Select(g => g.First())
                .OrderBy(a => a.AddressFamily == AddressFamily.InterNetwork ? 0 : 1)
                .ThenBy(a => a.ToString(), StringComparer.Ordinal)
                .Take(maxAddresses)
                .ToList();

            if (filtered.Count == 0)
            {
                throw new IOException("no " + family + " addresses for " + s.HostName);
            }

            var list = new List<ProbeRequest>();
            foreach (var a in filtered) list.Add(new ProbeRequest { Spec = s, Address = a });
            return list;
        }

        static async Task<ProbeResult> ProbeOne(ProbeRequest req, int timeoutMs)
        {
            var res = NewResult(req.Spec, req.Address);
            var watch = System.Diagnostics.Stopwatch.StartNew();
            string stage = "connect";
            using (var cts = new CancellationTokenSource(timeoutMs))
            {
                try
                {
                    using (var tcp = new TcpClient(req.Address.AddressFamily))
                    {
                        tcp.NoDelay = true;
                        tcp.ReceiveTimeout = timeoutMs;
                        tcp.SendTimeout = timeoutMs;
                        await tcp.ConnectAsync(req.Address, req.Spec.Port, cts.Token).ConfigureAwait(false);
                        var net = tcp.GetStream();

                        stage = "starttls";
                        await Preamble(net, req.Spec.Mode, cts.Token).ConfigureAwait(false);

                        stage = "handshake";
                        var presented = new List<byte[]>();
                        RemoteCertificateValidationCallback callback = (sender, certificate, chain, errors) =>
                        {
                            if (chain != null)
                            {
                                foreach (var c in chain.ChainPolicy.ExtraStore) presented.Add(c.RawData);
                            }
                            return true;
                        };

                        using (var ssl = new SslStream(net, false, callback))
                        {
                            var options = new SslClientAuthenticationOptions();
                            options.TargetHost = string.IsNullOrEmpty(req.Spec.Sni) ? req.Address.ToString() : req.Spec.Sni;
                            options.CertificateRevocationCheckMode = X509RevocationMode.NoCheck;
                            options.EnabledSslProtocols = SslProtocols.None;
                            await ssl.AuthenticateAsClientAsync(options, cts.Token).ConfigureAwait(false);

                            res.Protocol = ssl.SslProtocol.ToString();
                            try { res.CipherSuite = ssl.NegotiatedCipherSuite.ToString(); } catch (Exception) { res.CipherSuite = null; }
                            var remote = ssl.RemoteCertificate;
                            if (remote != null) res.Leaf = remote.GetRawCertData();
                            res.Presented = presented;
                            res.Success = res.Leaf != null;
                            if (!res.Success)
                            {
                                res.ErrorStage = stage;
                                res.Error = "server completed the handshake without a certificate";
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    res.Success = false;
                    res.ErrorStage = stage;
                    if (cts.IsCancellationRequested) res.Error = "timed out after " + timeoutMs + " ms";
                    else res.Error = Flatten(ex);
                    var sock = Find<SocketException>(ex);
                    if (sock != null) res.SocketError = sock.SocketErrorCode.ToString();
                }
            }
            res.ElapsedMs = watch.ElapsedMilliseconds;
            return res;
        }

        // Plaintext upgrade dialogues. Reads one byte at a time so no TLS record bytes are consumed.
        static async Task Preamble(NetworkStream s, string mode, CancellationToken ct)
        {
            switch (mode)
            {
                case "tls":
                    return;
                case "smtp":
                    await ExpectSmtp(s, "220", ct).ConfigureAwait(false);
                    await Send(s, "EHLO tls-renewal-readiness.invalid\r\n", ct).ConfigureAwait(false);
                    await ExpectSmtp(s, "250", ct).ConfigureAwait(false);
                    await Send(s, "STARTTLS\r\n", ct).ConfigureAwait(false);
                    await ExpectSmtp(s, "220", ct).ConfigureAwait(false);
                    return;
                case "imap":
                    var greeting = await ReadLine(s, ct).ConfigureAwait(false);
                    if (!greeting.StartsWith("* OK", StringComparison.OrdinalIgnoreCase))
                        throw new IOException("unexpected IMAP greeting: " + Clip(greeting));
                    await Send(s, "a1 STARTTLS\r\n", ct).ConfigureAwait(false);
                    for (int i = 0; i < 64; i++)
                    {
                        var line = await ReadLine(s, ct).ConfigureAwait(false);
                        if (line.StartsWith("a1 ", StringComparison.OrdinalIgnoreCase))
                        {
                            if (line.StartsWith("a1 OK", StringComparison.OrdinalIgnoreCase)) return;
                            throw new IOException("IMAP server refused STARTTLS: " + Clip(line));
                        }
                    }
                    throw new IOException("IMAP server never answered STARTTLS");
                case "postgres":
                    // SSLRequest: length 8, code 80877103 (0x04D2162F).
                    var request = new byte[] { 0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F };
                    await s.WriteAsync(request, 0, request.Length, ct).ConfigureAwait(false);
                    var reply = new byte[1];
                    int n = await s.ReadAsync(reply, 0, 1, ct).ConfigureAwait(false);
                    if (n != 1) throw new IOException("PostgreSQL server closed the connection after SSLRequest");
                    if (reply[0] != (byte)'S') throw new IOException("PostgreSQL server refused SSLRequest (reply '" + (char)reply[0] + "'), TLS is disabled");
                    return;
                default:
                    throw new ArgumentException("unknown probe mode " + mode);
            }
        }

        static async Task ExpectSmtp(NetworkStream s, string code, CancellationToken ct)
        {
            for (int i = 0; i < 128; i++)
            {
                var line = await ReadLine(s, ct).ConfigureAwait(false);
                if (line.Length < 3) throw new IOException("malformed SMTP reply: " + Clip(line));
                if (!line.StartsWith(code, StringComparison.Ordinal))
                    throw new IOException("SMTP expected " + code + " but got: " + Clip(line));
                if (line.Length == 3 || line[3] == ' ') return;
                if (line[3] != '-') throw new IOException("malformed SMTP reply: " + Clip(line));
            }
            throw new IOException("SMTP reply exceeded 128 lines");
        }

        static async Task Send(NetworkStream s, string text, CancellationToken ct)
        {
            var bytes = Encoding.ASCII.GetBytes(text);
            await s.WriteAsync(bytes, 0, bytes.Length, ct).ConfigureAwait(false);
        }

        static async Task<string> ReadLine(NetworkStream s, CancellationToken ct)
        {
            var buffer = new List<byte>(128);
            var one = new byte[1];
            while (true)
            {
                int n = await s.ReadAsync(one, 0, 1, ct).ConfigureAwait(false);
                if (n == 0) throw new IOException("connection closed during plaintext negotiation");
                if (one[0] == (byte)'\n') break;
                if (one[0] != (byte)'\r') buffer.Add(one[0]);
                if (buffer.Count > 2048) throw new IOException("plaintext line longer than 2048 bytes");
            }
            return Encoding.ASCII.GetString(buffer.ToArray());
        }

        static string Clip(string s)
        {
            if (s == null) return "";
            return s.Length > 120 ? s.Substring(0, 120) + "..." : s;
        }

        static T Find<T>(Exception ex) where T : Exception
        {
            for (var e = ex; e != null; e = e.InnerException)
            {
                var hit = e as T;
                if (hit != null) return hit;
            }
            return null;
        }

        static string Flatten(Exception ex)
        {
            var parts = new List<string>();
            for (var e = ex; e != null; e = e.InnerException)
            {
                var m = (e.Message ?? "").Trim();
                if (m.Length > 0 && !parts.Contains(m)) parts.Add(m);
            }
            return string.Join(" -> ", parts);
        }
    }
}
'@

function Import-ProbeType {
    if (-not ($script:ProbeTypeName -as [type])) {
        Add-Type -TypeDefinition $script:ProbeSource -Language CSharp -IgnoreWarnings -WarningAction SilentlyContinue
    }
}

# ---------------------------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------------------------

function New-Finding {
    param([string] $Severity, [string] $Code, [string] $Message)
    [pscustomobject]@{ Severity = $Severity; Code = $Code; Message = $Message }
}

function Get-MaxSeverity {
    param([object[]] $Findings)
    $best = 'ok'
    foreach ($f in @($Findings)) {
        if ($null -eq $f -or $f.Severity -eq 'info') { continue }
        if ($script:SeverityRank[$f.Severity] -gt $script:SeverityRank[$best]) { $best = $f.Severity }
    }
    return $best
}

function ConvertTo-Utc {
    param([datetime] $Value)
    if ($Value.Kind -eq [DateTimeKind]::Utc) { return $Value }
    if ($Value.Kind -eq [DateTimeKind]::Unspecified) { return [datetime]::SpecifyKind($Value, [DateTimeKind]::Utc) }
    return $Value.ToUniversalTime()
}

function Format-Utc {
    param([datetime] $Value)
    return (ConvertTo-Utc $Value).ToString('yyyy-MM-ddTHH:mm:ssZ', [Globalization.CultureInfo]::InvariantCulture)
}

function Format-Days {
    param([double] $Days)
    return $Days.ToString('0.0', [Globalization.CultureInfo]::InvariantCulture)
}

function Resolve-UserPath {
    param([string] $Value)
    return $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Value)
}

function Write-FileAtomic {
    param([string] $File, [string] $Content)
    $full = Resolve-UserPath $File
    $dir = [IO.Path]::GetDirectoryName($full)
    if ($dir -and -not [IO.Directory]::Exists($dir)) { [void][IO.Directory]::CreateDirectory($dir) }
    $tmp = [IO.Path]::Combine($dir, '.' + [IO.Path]::GetFileName($full) + '.' + [Guid]::NewGuid().ToString('N') + '.tmp')
    [IO.File]::WriteAllText($tmp, $Content, [Text.UTF8Encoding]::new($false))
    try {
        [IO.File]::Move($tmp, $full, $true)
    }
    catch {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
        throw
    }
}

function Get-ShortName {
    param([System.Security.Cryptography.X509Certificates.X509Certificate2] $Certificate, [switch] $Issuer)
    $name = $Certificate.GetNameInfo([System.Security.Cryptography.X509Certificates.X509NameType]::SimpleName, $Issuer.IsPresent)
    if ([string]::IsNullOrWhiteSpace($name)) {
        if ($Issuer) { return $Certificate.Issuer } else { return $Certificate.Subject }
    }
    return $name
}

# ---------------------------------------------------------------------------------------------
# Target parsing
# ---------------------------------------------------------------------------------------------

function ConvertTo-TargetSpec {
    param([string] $Text)

    $raw = $Text.Trim()
    if (-not $raw) { throw "empty target" }

    $mode = 'tls'
    $port = 443
    $rest = $raw
    $schemeLabel = $null

    if ($raw -match '^(?<scheme>[A-Za-z][A-Za-z0-9+.\-]*)://(?<rest>.*)$') {
        $scheme = $Matches['scheme'].ToLowerInvariant()
        $rest = $Matches['rest']
        switch ($scheme) {
            'https'      { $mode = 'tls'; $port = 443 }
            'tls'        { $mode = 'tls'; $port = 443 }
            'wss'        { $mode = 'tls'; $port = 443 }
            'ldaps'      { $mode = 'tls'; $port = 636; $schemeLabel = 'ldaps' }
            'smtps'      { $mode = 'tls'; $port = 465; $schemeLabel = 'smtps' }
            'imaps'      { $mode = 'tls'; $port = 993; $schemeLabel = 'imaps' }
            'smtp'       { $mode = 'smtp'; $port = 25 }
            'imap'       { $mode = 'imap'; $port = 143 }
            'postgres'   { $mode = 'postgres'; $port = 5432 }
            'postgresql' { $mode = 'postgres'; $port = 5432 }
            default      { throw "unsupported scheme '$scheme' in target '$raw'" }
        }
    }

    $sniOverride = $null
    $q = $rest.IndexOf('?')
    if ($q -ge 0) {
        $query = $rest.Substring($q + 1)
        $rest = $rest.Substring(0, $q)
        foreach ($pair in $query.Split('&')) {
            $kv = $pair.Split('=', 2)
            if ($kv.Count -eq 2 -and $kv[0] -eq 'sni' -and $kv[1]) { $sniOverride = [Uri]::UnescapeDataString($kv[1]) }
        }
    }
    $slash = $rest.IndexOf('/')
    if ($slash -ge 0) { $rest = $rest.Substring(0, $slash) }
    # Drop user info such as postgres://user:secret@db. It must never reach output, baselines or metrics.
    $at = $rest.LastIndexOf('@')
    if ($at -ge 0) { $rest = $rest.Substring($at + 1) }

    $hostName = $null
    if ($rest.StartsWith('[')) {
        $close = $rest.IndexOf(']')
        if ($close -lt 0) { throw "unterminated IPv6 literal in target '$raw'" }
        $hostName = $rest.Substring(1, $close - 1)
        $tail = $rest.Substring($close + 1)
        if ($tail) {
            if (-not $tail.StartsWith(':')) { throw "unexpected text after IPv6 literal in target '$raw'" }
            $parsedPort = 0
            if (-not [int]::TryParse($tail.Substring(1), [ref] $parsedPort)) { throw "invalid port in target '$raw'" }
            $port = $parsedPort
        }
    }
    elseif (($rest.ToCharArray() | Where-Object { $_ -eq ':' }).Count -gt 1) {
        $hostName = $rest
    }
    elseif ($rest.Contains(':')) {
        $parts = $rest.Split(':', 2)
        $hostName = $parts[0]
        $parsed = 0
        if (-not [int]::TryParse($parts[1], [ref] $parsed)) { throw "invalid port in target '$raw'" }
        $port = $parsed
    }
    else {
        $hostName = $rest
    }

    $hostName = $hostName.TrimEnd('.')
    if (-not $hostName) { throw "missing host in target '$raw'" }
    if ($port -lt 1 -or $port -gt 65535) { throw "port out of range in target '$raw'" }

    $ip = $null
    $isIp = [System.Net.IPAddress]::TryParse($hostName, [ref] $ip)
    if (-not $isIp) {
        try { $hostName = $script:Idn.GetAscii($hostName).ToLowerInvariant() }
        catch { throw "invalid host name '$hostName' in target '$raw'" }
    }
    else {
        $hostName = $ip.ToString()
    }

    if ($sniOverride) {
        try { $sniOverride = $script:Idn.GetAscii($sniOverride.TrimEnd('.')).ToLowerInvariant() }
        catch { throw "invalid sni '$sniOverride' in target '$raw'" }
    }
    $sni = if ($sniOverride) { $sniOverride } elseif ($isIp) { $null } else { $hostName }

    $hostPart = if ($isIp -and $hostName.Contains(':')) { "[$hostName]" } else { $hostName }
    $label = switch ($mode) {
        'tls'      { if ($schemeLabel) { "${schemeLabel}://${hostPart}:$port" } else { "${hostPart}:$port" } }
        'smtp'     { "smtp://${hostPart}:$port" }
        'imap'     { "imap://${hostPart}:$port" }
        'postgres' { "postgres://${hostPart}:$port" }
    }
    if ($sniOverride) { $label += "?sni=$sni" }

    [pscustomobject]@{ Label = $label; HostName = $hostName; Port = $port; Mode = $mode; Sni = $sni; IsIp = $isIp }
}

function Read-TargetList {
    $items = [System.Collections.Generic.List[string]]::new()
    foreach ($t in @($Target)) { if ($t) { foreach ($piece in $t.Split(',')) { if ($piece.Trim()) { $items.Add($piece.Trim()) } } } }
    if ($TargetFile) {
        $file = Resolve-UserPath $TargetFile
        if (-not [IO.File]::Exists($file)) { throw "target file not found: $TargetFile" }
        foreach ($line in [IO.File]::ReadAllLines($file)) {
            $clean = $line
            $hash = $clean.IndexOf('#')
            if ($hash -ge 0) { $clean = $clean.Substring(0, $hash) }
            $clean = $clean.Trim()
            if ($clean) { $items.Add($clean) }
        }
    }
    $seen = @{}
    $specs = [System.Collections.Generic.List[object]]::new()
    foreach ($item in $items) {
        $spec = ConvertTo-TargetSpec $item
        if (-not $seen.ContainsKey($spec.Label)) {
            $seen[$spec.Label] = $true
            $specs.Add($spec)
        }
    }
    return , $specs.ToArray()
}

# ---------------------------------------------------------------------------------------------
# Certificate file reading
# ---------------------------------------------------------------------------------------------

function Read-CertificateFile {
    param([string] $File)
    $info = [IO.FileInfo]::new($File)
    if ($info.Length -gt $script:MaxCertFileBytes) { throw "file larger than $($script:MaxCertFileBytes) bytes" }
    $bytes = [IO.File]::ReadAllBytes($File)
    $certs = [System.Collections.Generic.List[System.Security.Cryptography.X509Certificates.X509Certificate2]]::new()
    $hasPrivateKey = $false
    $text = [Text.Encoding]::ASCII.GetString($bytes)
    $blocks = [regex]::Matches($text, '-----BEGIN (?<label>[A-Z0-9 ]+)-----(?<body>[\s\S]*?)-----END \k<label>-----')
    if ($blocks.Count -gt 0) {
        foreach ($m in $blocks) {
            $label = $m.Groups['label'].Value
            if ($label -like '*PRIVATE KEY*') { $hasPrivateKey = $true; continue }
            if ($label -ne 'CERTIFICATE') { continue }
            $body = [regex]::Replace($m.Groups['body'].Value, '\s', '')
            $der = [Convert]::FromBase64String($body)
            $certs.Add([System.Security.Cryptography.X509Certificates.X509Certificate2]::new($der))
        }
    }
    else {
        $kind = [System.Security.Cryptography.X509Certificates.X509Certificate2]::GetCertContentType($bytes)
        if ($kind -ne [System.Security.Cryptography.X509Certificates.X509ContentType]::Cert) {
            throw "unsupported content type $kind, only PEM bundles and single DER certificates are read"
        }
        $certs.Add([System.Security.Cryptography.X509Certificates.X509Certificate2]::new($bytes))
    }
    [pscustomobject]@{ Certificates = $certs.ToArray(); HasPrivateKey = $hasPrivateKey }
}

function Get-CertificateFileList {
    $files = [System.Collections.Generic.List[string]]::new()
    foreach ($p in @($Path)) {
        if (-not $p) { continue }
        $resolved = Resolve-UserPath $p
        if ([IO.Directory]::Exists($resolved)) {
            foreach ($f in [IO.Directory]::EnumerateFiles($resolved, '*', [IO.SearchOption]::AllDirectories)) {
                if ($script:CertFileExtensions -contains [IO.Path]::GetExtension($f).ToLowerInvariant()) { $files.Add($f) }
            }
        }
        elseif ([IO.File]::Exists($resolved)) {
            $files.Add($resolved)
        }
        else {
            throw "path not found: $p"
        }
    }
    return , @($files | Sort-Object -Unique)
}

# ---------------------------------------------------------------------------------------------
# Certificate facts and policy
# ---------------------------------------------------------------------------------------------

function Get-CabMaxLifetimeDays {
    param([datetime] $IssuedUtc)
    foreach ($row in $script:CabLifetimeSchedule) {
        if ($IssuedUtc -ge $row.From) { return $row.MaxDays }
    }
    return 398
}

function Get-CertificateFacts {
    param([System.Security.Cryptography.X509Certificates.X509Certificate2] $Certificate)

    $c = $Certificate
    $notBefore = $c.NotBefore.ToUniversalTime()
    $notAfter = $c.NotAfter.ToUniversalTime()
    # RFC 5280 validity is inclusive of both ends, which is how the CA/B Forum counts it.
    $lifetimeSeconds = ($notAfter - $notBefore).TotalSeconds + 1

    $dns = @(); $ips = @(); $sanPresent = $false; $sanError = $null
    $ocsp = @(); $hasCrl = $false; $mustStaple = $false; $hasSct = $false; $isCa = $false

    foreach ($ext in $c.Extensions) {
        $oid = $ext.Oid.Value
        if ($oid -eq '2.5.29.17') {
            $sanPresent = $true
            try {
                $san = [System.Security.Cryptography.X509Certificates.X509SubjectAlternativeNameExtension]::new($ext.RawData, $ext.Critical)
                $dns = @($san.EnumerateDnsNames())
                $ips = @(foreach ($a in $san.EnumerateIPAddresses()) { $a.ToString() })
            }
            catch { $sanError = $_.Exception.Message }
        }
        elseif ($oid -eq '1.3.6.1.5.5.7.1.1') {
            try {
                $aia = [System.Security.Cryptography.X509Certificates.X509AuthorityInformationAccessExtension]::new($ext.RawData, $ext.Critical)
                $ocsp = @($aia.EnumerateOcspUris())
            }
            catch { $ocsp = @() }
        }
        elseif ($oid -eq '2.5.29.31') { $hasCrl = $true }
        elseif ($oid -eq '1.3.6.1.5.5.7.1.24') { $mustStaple = $true }
        elseif ($oid -eq '1.3.6.1.4.1.11129.2.4.2') { $hasSct = $true }
        elseif ($oid -eq '2.5.29.19') {
            try {
                $bc = [System.Security.Cryptography.X509Certificates.X509BasicConstraintsExtension]::new($ext, $ext.Critical)
                $isCa = $bc.CertificateAuthority
            }
            catch { $isCa = $false }
        }
    }

    $keyName = 'unknown'; $keyBits = 0; $curve = $null
    try {
        switch ($c.PublicKey.Oid.Value) {
            '1.2.840.113549.1.1.1' {
                $keyName = 'RSA'
                $k = $c.GetRSAPublicKey()
                try { $keyBits = $k.KeySize } finally { if ($k) { $k.Dispose() } }
            }
            '1.2.840.10045.2.1' {
                $keyName = 'ECDSA'
                $k = $c.GetECDsaPublicKey()
                try {
                    $keyBits = $k.KeySize
                    try { $curve = $k.ExportParameters($false).Curve.Oid.FriendlyName } catch { $curve = $null }
                }
                finally { if ($k) { $k.Dispose() } }
            }
            '1.3.101.112' { $keyName = 'Ed25519'; $keyBits = 256 }
            '1.3.101.113' { $keyName = 'Ed448'; $keyBits = 456 }
        }
    }
    catch { $keyName = 'unreadable' }

    $spki = $null
    try {
        $spki = [Convert]::ToBase64String([System.Security.Cryptography.SHA256]::HashData($c.PublicKey.ExportSubjectPublicKeyInfo()))
    }
    catch { $spki = $null }

    [pscustomobject]@{
        Certificate     = $c
        Subject         = $c.Subject
        CommonName      = Get-ShortName $c
        Issuer          = $c.Issuer
        IssuerName      = Get-ShortName $c -Issuer
        NotBefore       = $notBefore
        NotAfter        = $notAfter
        LifetimeDays    = $lifetimeSeconds / 86400.0
        Sha256          = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData($c.RawData))
        SpkiSha256      = $spki
        DnsNames        = $dns
        IpAddresses     = $ips
        SanPresent      = $sanPresent
        SanError        = $sanError
        OcspUris        = $ocsp
        HasCrl          = $hasCrl
        MustStaple      = $mustStaple
        HasSct          = $hasSct
        IsCa            = $isCa
        IsSelfIssued    = ([Convert]::ToBase64String($c.SubjectName.RawData) -eq [Convert]::ToBase64String($c.IssuerName.RawData))
        KeyAlgorithm    = $keyName
        KeyBits         = $keyBits
        Curve           = $curve
        SignatureOid    = $c.SignatureAlgorithm.Value
        SignatureName   = $c.SignatureAlgorithm.FriendlyName
    }
}

function Test-DnsPattern {
    param([string] $HostName, [string] $Pattern)
    $p = $Pattern.Trim().TrimEnd('.').ToLowerInvariant()
    if (-not $p) { return $false }
    if ($p.StartsWith('*.')) {
        $suffix = $p.Substring(1)
        if ($suffix.Contains('*')) { return $false }
        try { $suffix = '.' + $script:Idn.GetAscii($suffix.Substring(1)).ToLowerInvariant() } catch { return $false }
        # A wildcard must sit above at least two labels: *.com never matches.
        if (@($suffix.Split('.') | Where-Object { $_ }).Count -lt 2) { return $false }
        if (-not $HostName.EndsWith($suffix, [StringComparison]::Ordinal)) { return $false }
        $label = $HostName.Substring(0, $HostName.Length - $suffix.Length)
        return ($label.Length -gt 0 -and -not $label.Contains('.'))
    }
    if ($p.Contains('*')) { return $false }  # partial label wildcards are not honoured by modern clients
    try { $p = $script:Idn.GetAscii($p).ToLowerInvariant() } catch { return $false }
    return ($p -eq $HostName)
}

function Test-NameMatch {
    param([string] $Name, [object] $Facts)
    $ip = $null
    if ([System.Net.IPAddress]::TryParse($Name, [ref] $ip)) {
        $wanted = $ip.ToString()
        foreach ($a in @($Facts.IpAddresses)) { if ($a -eq $wanted) { return $true } }
        return $false
    }
    foreach ($d in @($Facts.DnsNames)) { if (Test-DnsPattern -HostName $Name -Pattern $d) { return $true } }
    return $false
}

function Invoke-ChainBuild {
    param(
        [System.Security.Cryptography.X509Certificates.X509Certificate2] $Leaf,
        [object[]] $Extra,
        [bool] $AllowDownload,
        [bool] $Revocation
    )
    $chain = [System.Security.Cryptography.X509Certificates.X509Chain]::new()
    try {
        $policy = $chain.ChainPolicy
        $policy.VerificationTime = $script:NowUtc.ToLocalTime()
        $policy.RevocationMode = if ($Revocation) { [System.Security.Cryptography.X509Certificates.X509RevocationMode]::Online } else { [System.Security.Cryptography.X509Certificates.X509RevocationMode]::NoCheck }
        $policy.RevocationFlag = [System.Security.Cryptography.X509Certificates.X509RevocationFlag]::ExcludeRoot
        $policy.DisableCertificateDownloads = -not $AllowDownload
        $policy.UrlRetrievalTimeout = [TimeSpan]::FromSeconds(5)
        foreach ($c in @($Extra)) { if ($c) { [void]$policy.ExtraStore.Add($c) } }
        if ($script:CustomRoots.Count -gt 0) {
            $policy.TrustMode = [System.Security.Cryptography.X509Certificates.X509ChainTrustMode]::CustomRootTrust
            foreach ($r in $script:CustomRoots) { [void]$policy.CustomTrustStore.Add($r) }
        }
        $ok = $chain.Build($Leaf)
        $elements = @(foreach ($e in $chain.ChainElements) { [System.Security.Cryptography.X509Certificates.X509Certificate2]::new($e.Certificate.RawData) })
        $status = @($chain.ChainStatus | ForEach-Object { $_.Status.ToString() } | Where-Object { $_ -ne 'NoError' } | Sort-Object -Unique)
        [pscustomobject]@{ Ok = $ok; Status = $status; Elements = $elements }
    }
    finally {
        $chain.Reset()
        $chain.Dispose()
    }
}

function Measure-Certificate {
    <#
      Applies every certificate level rule to one observation: a leaf, the certificates that came
      with it (null when the runtime could not tell us), the name a client asks for and the TLS
      parameters, when there were any.
    #>
    param(
        [System.Security.Cryptography.X509Certificates.X509Certificate2] $Leaf,
        [object[]] $Presented,
        [bool] $PresentedKnown,
        [string] $ExpectedName,
        [string] $Protocol,
        [string] $CipherSuite
    )

    $findings = [System.Collections.Generic.List[object]]::new()
    $leafFacts = Get-CertificateFacts $Leaf
    $now = $script:NowUtc

    $build = Invoke-ChainBuild -Leaf $Leaf -Extra $Presented -AllowDownload (-not $NoAiaFetch.IsPresent) -Revocation $CheckRevocation.IsPresent
    $chainFacts = @(foreach ($e in $build.Elements) { Get-CertificateFacts $e })
    if ($chainFacts.Count -eq 0) { $chainFacts = @($leafFacts) }

    $trustProblems = @($build.Status | Where-Object { $script:TrustFailureStatuses -contains $_ })
    $publiclyTrusted = ($script:CustomRoots.Count -eq 0 -and $trustProblems.Count -eq 0 -and $chainFacts.Count -gt 1)

    # --- validity window and effective expiry -------------------------------------------------
    $effective = $leafFacts.NotAfter
    $limiter = $leafFacts
    foreach ($f in $chainFacts) {
        if ($f.NotAfter -lt $effective) { $effective = $f.NotAfter; $limiter = $f }
    }
    $remainingDays = ($effective - $now).TotalDays
    $life = [math]::Max($leafFacts.LifetimeDays, 0.0001)
    $critDays = [math]::Min($CriticalDays, $life * 0.10)
    $warnDays = [math]::Max([math]::Min($WarnDays, $life * (1.0 - $RenewAtFraction)), $critDays)

    if ($now -lt $leafFacts.NotBefore) {
        $findings.Add((New-Finding 'fail' 'NotYetValid' ("leaf is not valid until {0}, clients with correct clocks reject it" -f (Format-Utc $leafFacts.NotBefore))))
    }
    if ($now -gt $leafFacts.NotAfter) {
        $findings.Add((New-Finding 'fail' 'Expired' ("leaf expired {0} ({1} days ago)" -f (Format-Utc $leafFacts.NotAfter), (Format-Days (($now - $leafFacts.NotAfter).TotalDays)))))
    }
    foreach ($f in $chainFacts) {
        if ($f.Sha256 -eq $leafFacts.Sha256) { continue }
        if ($now -gt $f.NotAfter) {
            $findings.Add((New-Finding 'fail' 'ChainCertificateExpired' ("chain certificate '{0}' expired {1}" -f $f.CommonName, (Format-Utc $f.NotAfter))))
        }
    }
    if ($now -le $effective) {
        if ($remainingDays -lt $critDays) {
            $findings.Add((New-Finding 'fail' 'ExpiryCritical' ("{0} days left, below the critical line of {1} days for a {2} day certificate" -f (Format-Days $remainingDays), (Format-Days $critDays), (Format-Days $life))))
        }
        elseif ($remainingDays -lt $warnDays) {
            $findings.Add((New-Finding 'warn' 'ExpirySoon' ("{0} days left, renewal should already have happened (warn line {1} days for a {2} day certificate)" -f (Format-Days $remainingDays), (Format-Days $warnDays), (Format-Days $life))))
        }
    }
    if ($limiter.Sha256 -ne $leafFacts.Sha256) {
        $findings.Add((New-Finding 'warn' 'ChainExpiresFirst' ("'{0}' in the chain expires {1}, before the leaf ({2}); renewing the leaf alone will not help" -f $limiter.CommonName, (Format-Utc $limiter.NotAfter), (Format-Utc $leafFacts.NotAfter))))
    }

    # --- lifetime policy under the shrinking CA/B schedule -------------------------------------
    if ($publiclyTrusted) {
        $capAtIssue = Get-CabMaxLifetimeDays $leafFacts.NotBefore
        if ($leafFacts.LifetimeDays -gt $capAtIssue + 0.0001) {
            $findings.Add((New-Finding 'fail' 'ExceedsLifetimeCap' ("{0} day lifetime exceeds the {1} day maximum for public TLS certificates issued {2}" -f (Format-Days $leafFacts.LifetimeDays), $capAtIssue, $leafFacts.NotBefore.ToString('yyyy-MM-dd'))))
        }
        $renewDue = $leafFacts.NotBefore.AddSeconds($leafFacts.LifetimeDays * 86400.0 * $RenewAtFraction)
        if ($renewDue -lt $now) { $renewDue = $now }
        $capNext = Get-CabMaxLifetimeDays $renewDue
        if ($leafFacts.LifetimeDays -gt $capNext + 0.0001) {
            $perYearNow = [math]::Ceiling(365.25 / ($leafFacts.LifetimeDays * $RenewAtFraction))
            $perYearNext = [math]::Ceiling(365.25 / ($capNext * $RenewAtFraction))
            $sev = if ($capNext -lt $leafFacts.LifetimeDays / 2.0) { 'warn' } else { 'info' }
            $findings.Add((New-Finding $sev 'CadenceShrink' ("next renewal (due about {0}) can be at most {1} days; renewals per year go from {2} to {3}" -f $renewDue.ToString('yyyy-MM-dd'), $capNext, $perYearNow, $perYearNext)))
        }
    }

    # --- names ---------------------------------------------------------------------------------
    if (-not $leafFacts.SanPresent) {
        $findings.Add((New-Finding 'fail' 'NoSubjectAltName' 'leaf has no subjectAltName extension; browsers and modern TLS stacks ignore the CN'))
    }
    elseif ($leafFacts.SanError) {
        $findings.Add((New-Finding 'fail' 'MalformedSubjectAltName' ("subjectAltName could not be parsed: {0}" -f $leafFacts.SanError)))
    }
    elseif ($ExpectedName -and -not (Test-NameMatch -Name $ExpectedName -Facts $leafFacts)) {
        $names = @($leafFacts.DnsNames) + @($leafFacts.IpAddresses)
        $shown = ($names | Select-Object -First 6) -join ', '
        if ($names.Count -gt 6) { $shown += ", +$($names.Count - 6) more" }
        $findings.Add((New-Finding 'fail' 'NameMismatch' ("'{0}' is not covered by the certificate names: {1}" -f $ExpectedName, $shown)))
    }

    # --- trust ---------------------------------------------------------------------------------
    if ($trustProblems.Count -gt 0) {
        $hint = ''
        if ($trustProblems -contains 'PartialChain' -and $NoAiaFetch) { $hint = ' (AIA fetching is off, so a missing intermediate is fatal here)' }
        if ($trustProblems -contains 'UntrustedRoot' -and $script:CustomRoots.Count -eq 0) { $hint = ' (private PKI? pass -TrustedRootPath)' }
        $findings.Add((New-Finding 'fail' 'ChainUntrusted' ("chain does not validate: {0}{1}" -f ($trustProblems -join ', '), $hint)))
    }
    if ($build.Status -contains 'Revoked') {
        $findings.Add((New-Finding 'fail' 'Revoked' 'a certificate in the chain is revoked'))
    }
    elseif ($CheckRevocation -and (($build.Status -contains 'RevocationStatusUnknown') -or ($build.Status -contains 'OfflineRevocation'))) {
        $findings.Add((New-Finding 'warn' 'RevocationUnknown' 'revocation status could not be determined (CRL or OCSP unreachable)'))
    }

    # --- what the server actually sent ---------------------------------------------------------
    if ($PresentedKnown -and $trustProblems.Count -eq 0 -and $chainFacts.Count -gt 1) {
        $sent = @{}
        foreach ($p in @($Presented)) { $sent[[Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData($p.RawData))] = $p }
        $missing = @()
        for ($i = 1; $i -lt $chainFacts.Count; $i++) {
            $f = $chainFacts[$i]
            $isAnchor = ($i -eq $chainFacts.Count - 1 -and $f.IsSelfIssued)
            if ($isAnchor) { continue }
            if (-not $sent.ContainsKey($f.Sha256)) { $missing += $f.CommonName }
        }
        if ($missing.Count -gt 0) {
            $findings.Add((New-Finding 'warn' 'IncompleteChain' ("server does not send intermediate(s) {0}; browsers repair this through AIA or caches, but curl, Go, Java, Python and most IoT clients fail" -f (($missing | ForEach-Object { "'$_'" }) -join ', '))))
        }
        $inChain = @{}
        foreach ($f in $chainFacts) { $inChain[$f.Sha256] = $true }
        $extras = @()
        $expiredSent = @()
        foreach ($key in $sent.Keys) {
            $p = $sent[$key]
            if ($key -eq $leafFacts.Sha256) { continue }
            if ($p.NotAfter.ToUniversalTime() -lt $now) { $expiredSent += (Get-ShortName $p) }
            elseif (-not $inChain.ContainsKey($key)) { $extras += (Get-ShortName $p) }
        }
        if ($expiredSent.Count -gt 0) {
            $findings.Add((New-Finding 'warn' 'SendsExpiredCertificate' ("server still sends expired certificate(s) {0}; older OpenSSL and embedded stacks may pick that path and fail" -f (($expiredSent | ForEach-Object { "'$_'" }) -join ', '))))
        }
        if ($extras.Count -gt 0) {
            $findings.Add((New-Finding 'info' 'SendsUnusedCertificate' ("server sends certificate(s) not on the validated path: {0}" -f (($extras | ForEach-Object { "'$_'" }) -join ', '))))
        }
    }

    # --- keys and signatures -------------------------------------------------------------------
    if ($leafFacts.KeyAlgorithm -eq 'RSA' -and $leafFacts.KeyBits -lt 2048) {
        $findings.Add((New-Finding 'fail' 'WeakKey' ("RSA {0} bit key; 2048 is the minimum any public CA or modern client accepts" -f $leafFacts.KeyBits)))
    }
    elseif ($leafFacts.KeyAlgorithm -eq 'ECDSA' -and $leafFacts.KeyBits -lt 256) {
        $findings.Add((New-Finding 'fail' 'WeakKey' ("ECDSA {0} bit curve" -f $leafFacts.KeyBits)))
    }
    elseif ($leafFacts.KeyAlgorithm -eq 'unknown' -or $leafFacts.KeyAlgorithm -eq 'unreadable') {
        $findings.Add((New-Finding 'info' 'UnknownKeyType' ("public key algorithm {0} was not recognised" -f $Leaf.PublicKey.Oid.Value)))
    }
    foreach ($f in $chainFacts) {
        if ($f.IsSelfIssued -and $f.Sha256 -ne $leafFacts.Sha256) { continue }  # a root's self signature is not checked by clients
        if ($script:WeakSignatureOids.ContainsKey($f.SignatureOid)) {
            $findings.Add((New-Finding 'fail' 'WeakSignature' ("'{0}' is signed with {1}" -f $f.CommonName, $script:WeakSignatureOids[$f.SignatureOid])))
        }
    }

    # --- revocation information and transparency ----------------------------------------------
    if ($leafFacts.MustStaple) {
        $findings.Add((New-Finding 'warn' 'MustStaple' 'leaf carries the OCSP Must-Staple extension; if the server stops stapling or the CA retires OCSP, Firefox hard fails'))
    }
    if ($publiclyTrusted -and $leafFacts.LifetimeDays -gt $script:ShortLivedDays -and $leafFacts.OcspUris.Count -eq 0 -and -not $leafFacts.HasCrl) {
        $findings.Add((New-Finding 'warn' 'NoRevocationInfo' 'leaf has neither an OCSP URL nor a CRL distribution point'))
    }
    if ($publiclyTrusted -and -not $leafFacts.HasSct) {
        $findings.Add((New-Finding 'info' 'NoEmbeddedSct' 'no embedded SCTs; Chrome and Safari need them via TLS extension or OCSP staple instead'))
    }

    # --- negotiated protocol -------------------------------------------------------------------
    if ($Protocol) {
        if ($script:LegacyProtocols -contains $Protocol) {
            $findings.Add((New-Finding 'fail' 'LegacyProtocol' ("negotiated {0}; TLS 1.2 is the floor" -f $Protocol)))
        }
        if ($Protocol -eq 'Tls12' -and $CipherSuite) {
            if ($CipherSuite -match 'RC4|3DES|_DES_|NULL|EXPORT|_MD5|anon') {
                $findings.Add((New-Finding 'fail' 'WeakCipher' ("negotiated {0}" -f $CipherSuite)))
            }
            elseif ($CipherSuite -notmatch 'ECDHE|DHE') {
                $findings.Add((New-Finding 'warn' 'NoForwardSecrecy' ("negotiated {0} with static RSA key exchange" -f $CipherSuite)))
            }
            elseif ($CipherSuite -match '_CBC_') {
                $findings.Add((New-Finding 'info' 'CbcCipher' ("negotiated CBC suite {0}" -f $CipherSuite)))
            }
        }
    }

    [pscustomobject]@{
        Leaf              = $leafFacts
        Chain             = $chainFacts
        ChainStatus       = $build.Status
        PubliclyTrusted   = $publiclyTrusted
        EffectiveNotAfter = $effective
        RemainingDays     = $remainingDays
        WarnDays          = $warnDays
        CriticalDays      = $critDays
        Findings          = $findings.ToArray()
    }
}

# ---------------------------------------------------------------------------------------------
# Observations, targets and baselines
# ---------------------------------------------------------------------------------------------

function New-Observation {
    param([string] $Label, [string] $Mode, [string] $Address, [string] $Family, [bool] $Success, [string] $ErrorText, [string] $ErrorStage,
          [string] $SocketError, [string] $Protocol, [string] $CipherSuite, [long] $ElapsedMs, [object] $Analysis, [bool] $PresentedKnown)
    [pscustomobject]@{
        Label = $Label; Mode = $Mode; Address = $Address; Family = $Family; Success = $Success; Error = $ErrorText
        ErrorStage = $ErrorStage; SocketError = $SocketError; Protocol = $Protocol; CipherSuite = $CipherSuite
        ElapsedMs = $ElapsedMs; Analysis = $Analysis; PresentedKnown = $PresentedKnown
    }
}

function Measure-ProbeResult {
    param([object] $Result, [object] $Spec)
    if (-not $Result.Success) {
        return New-Observation -Label $Result.Label -Mode $Result.Mode -Address $Result.Address -Family $Result.Family -Success $false `
            -ErrorText $Result.Error -ErrorStage $Result.ErrorStage -SocketError $Result.SocketError -Protocol $null -CipherSuite $null `
            -ElapsedMs $Result.ElapsedMs -Analysis $null -PresentedKnown $false
    }
    $leaf = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new([byte[]] $Result.Leaf)
    $presented = @(foreach ($raw in $Result.Presented) { [System.Security.Cryptography.X509Certificates.X509Certificate2]::new([byte[]] $raw) })
    # An empty peer store means the runtime did not report what was sent, not that nothing was sent:
    # every TLS server sends at least its leaf. Only judge chain completeness when we really know.
    $known = $presented.Count -gt 0
    $expected = if ($Spec.Sni) { $Spec.Sni } else { $Spec.HostName }
    $analysis = Measure-Certificate -Leaf $leaf -Presented $presented -PresentedKnown $known -ExpectedName $expected -Protocol $Result.Protocol -CipherSuite $Result.CipherSuite
    return New-Observation -Label $Result.Label -Mode $Result.Mode -Address $Result.Address -Family $Result.Family -Success $true `
        -ErrorText $null -ErrorStage $null -SocketError $null -Protocol $Result.Protocol -CipherSuite $Result.CipherSuite `
        -ElapsedMs $Result.ElapsedMs -Analysis $analysis -PresentedKnown $known
}

function Measure-CertificateFileEntry {
    param([string] $File)
    try {
        $read = Read-CertificateFile $File
    }
    catch {
        return New-Observation -Label $File -Mode 'file' -Address '' -Family '' -Success $false -ErrorText $_.Exception.Message `
            -ErrorStage 'read' -SocketError $null -Protocol $null -CipherSuite $null -ElapsedMs 0 -Analysis $null -PresentedKnown $false
    }
    if ($read.Certificates.Count -eq 0) {
        return New-Observation -Label $File -Mode 'file' -Address '' -Family '' -Success $false -ErrorText 'no certificates found' `
            -ErrorStage 'read' -SocketError $null -Protocol $null -CipherSuite $null -ElapsedMs 0 -Analysis $null -PresentedKnown $false
    }
    $leaf = $read.Certificates[0]
    $analysis = Measure-Certificate -Leaf $leaf -Presented $read.Certificates -PresentedKnown $true -ExpectedName $null -Protocol $null -CipherSuite $null
    if ($read.HasPrivateKey) {
        $analysis.Findings = @($analysis.Findings) + @(New-Finding 'info' 'PrivateKeyColocated' 'file also contains a private key; check its permissions and that it is not shipped in images')
    }
    return New-Observation -Label $File -Mode 'file' -Address '' -Family '' -Success $true -ErrorText $null -ErrorStage $null `
        -SocketError $null -Protocol $null -CipherSuite $null -ElapsedMs 0 -Analysis $analysis -PresentedKnown $true
}

function Compare-Baseline {
    param([object[]] $Healthy, [object] $Entry)
    $out = [System.Collections.Generic.List[object]]::new()
    if ($null -eq $Entry -or $Healthy.Count -eq 0) { return , $out.ToArray() }

    $old = @($Entry.leaves)
    if ($old.Count -eq 0) { return , $out.ToArray() }
    $oldThumbs = @($old | ForEach-Object { $_.sha256 })
    $oldSpki = @($old | ForEach-Object { $_.spki } | Where-Object { $_ })
    $oldIssuers = @($old | ForEach-Object { $_.issuer })
    $oldLatest = $null
    foreach ($l in $old) {
        if ($null -ne $l.notAfter -and ($null -eq $oldLatest -or $l.notAfter -gt $oldLatest)) { $oldLatest = $l.notAfter }
    }

    $leaves = @($Healthy | ForEach-Object { $_.Analysis.Leaf } | Group-Object Sha256 | ForEach-Object { $_.Group[0] })
    $newLatest = $leaves[0].NotAfter
    foreach ($l in $leaves) { if ($l.NotAfter -gt $newLatest) { $newLatest = $l.NotAfter } }

    if ($null -ne $oldLatest -and $newLatest -lt $oldLatest.AddDays(-1)) {
        $out.Add((New-Finding 'fail' 'CertificateRegressed' ("now serving a certificate that expires {0}, older than the baseline's {1}; a stale node, image or secret was restored" -f (Format-Utc $newLatest), (Format-Utc $oldLatest))))
    }
    foreach ($leaf in $leaves) {
        if ($oldThumbs -contains $leaf.Sha256) { continue }
        if ($leaf.SpkiSha256 -and ($oldSpki -contains $leaf.SpkiSha256)) {
            $out.Add((New-Finding 'warn' 'KeyReused' ("certificate rotated but the key pair did not (SPKI {0}); a leaked key stays valid across renewals" -f $leaf.SpkiSha256)))
        }
        else {
            $out.Add((New-Finding 'info' 'Rotated' ("new certificate {0} with a new key since the baseline" -f $leaf.Sha256.Substring(0, 16))))
        }
        if ($oldIssuers -notcontains $leaf.Issuer) {
            $out.Add((New-Finding 'info' 'IssuerChanged' ("issuer changed to '{0}'; check CAA records, pinned intermediates and client trust stores" -f $leaf.IssuerName)))
        }
    }
    return , $out.ToArray()
}

function Merge-TargetReport {
    param([string] $Label, [string] $Mode, [object[]] $Observations, [object] $BaselineEntry)

    $findings = [System.Collections.Generic.List[object]]::new()
    $healthy = @($Observations | Where-Object { $_.Success })
    $broken = @($Observations | Where-Object { -not $_.Success })

    if ($healthy.Count -eq 0) {
        $why = @($broken | ForEach-Object {
                $where = if ($_.Address) { $_.Address } else { 'target' }
                "${where}: $($_.ErrorStage): $($_.Error)"
            } | Sort-Object -Unique)
        $findings.Add([pscustomobject]@{ Severity = 'fail'; Code = 'Unreachable'; Message = ($why -join '; '); Addresses = @() })
    }
    elseif ($broken.Count -gt 0) {
        $v4Healthy = @($healthy | Where-Object { $_.Family -eq 'IPv4' }).Count
        foreach ($b in $broken) {
            $noRoute = $b.Family -eq 'IPv6' -and $v4Healthy -gt 0 -and $b.ErrorStage -eq 'connect' -and
                @('NetworkUnreachable', 'HostUnreachable', 'AddressFamilyNotSupported', 'AddressNotAvailable') -contains $b.SocketError
            if ($noRoute) {
                $findings.Add([pscustomobject]@{ Severity = 'info'; Code = 'IPv6PathUnavailable'; Message = "this machine has no IPv6 route to $($b.Address), so that address was not checked"; Addresses = @($b.Address) })
            }
            else {
                $findings.Add([pscustomobject]@{ Severity = 'warn'; Code = 'PartialReachability'; Message = "$($b.ErrorStage) failed: $($b.Error)"; Addresses = @($b.Address) })
            }
        }
    }

    # Per address findings, folded so one problem on every node reads as one line.
    $flat = @(foreach ($o in $healthy) { foreach ($f in $o.Analysis.Findings) { [pscustomobject]@{ Severity = $f.Severity; Code = $f.Code; Message = $f.Message; Address = $o.Address } } })
    foreach ($g in @($flat | Group-Object { "$($_.Severity)|$($_.Code)|$($_.Message)" })) {
        $first = $g.Group[0]
        $addrs = @($g.Group | ForEach-Object { $_.Address } | Where-Object { $_ } | Sort-Object -Unique)
        if ($addrs.Count -eq $healthy.Count) { $addrs = @() }
        $findings.Add([pscustomobject]@{ Severity = $first.Severity; Code = $first.Code; Message = $first.Message; Addresses = $addrs })
    }

    # Different certificates behind one name: the classic half renewed load balancer pool.
    $byLeaf = @($healthy | Group-Object { $_.Analysis.Leaf.Sha256 })
    if ($byLeaf.Count -gt 1) {
        $parts = @(foreach ($g in ($byLeaf | Sort-Object { $_.Group[0].Analysis.Leaf.NotAfter })) {
                $leaf = $g.Group[0].Analysis.Leaf
                "{0} expires {1} on {2}" -f $leaf.Sha256.Substring(0, 12), $leaf.NotAfter.ToString('yyyy-MM-dd'), (($g.Group | ForEach-Object { $_.Address }) -join ', ')
            })
        $findings.Add([pscustomobject]@{ Severity = 'warn'; Code = 'MixedCertificates'; Message = ("{0} addresses serve {1} different leaf certificates: {2}" -f $healthy.Count, $byLeaf.Count, ($parts -join '; ')); Addresses = @() })
    }

    foreach ($f in (Compare-Baseline -Healthy $healthy -Entry $BaselineEntry)) {
        $findings.Add([pscustomobject]@{ Severity = $f.Severity; Code = $f.Code; Message = $f.Message; Addresses = @() })
    }

    $worst = $null
    foreach ($o in $healthy) {
        if ($null -eq $worst -or $o.Analysis.EffectiveNotAfter -lt $worst.Analysis.EffectiveNotAfter) { $worst = $o }
    }

    $sorted = @($findings | Sort-Object @{ Expression = { $script:SeverityRank[$_.Severity] }; Descending = $true }, @{ Expression = { $_.Code } })
    $severity = Get-MaxSeverity $sorted

    [pscustomobject]@{
        Target            = $Label
        Mode              = $Mode
        Severity          = $severity
        DaysLeft          = if ($worst) { [math]::Round($worst.Analysis.RemainingDays, 1) } else { $null }
        EffectiveNotAfter = if ($worst) { Format-Utc $worst.Analysis.EffectiveNotAfter } else { $null }
        LifetimeDays      = if ($worst) { [math]::Round($worst.Analysis.Leaf.LifetimeDays, 1) } else { $null }
        WarnBelowDays     = if ($worst) { [math]::Round($worst.Analysis.WarnDays, 1) } else { $null }
        CriticalBelowDays = if ($worst) { [math]::Round($worst.Analysis.CriticalDays, 1) } else { $null }
        AddressesOk       = $healthy.Count
        AddressesTotal    = @($Observations | Where-Object { $_.Address -or $_.Mode -eq 'file' }).Count
        Leaf              = if ($worst) { ConvertTo-LeafDto $worst.Analysis.Leaf } else { $null }
        Chain             = if ($worst) { @($worst.Analysis.Chain | ForEach-Object { [pscustomobject]@{ Subject = $_.CommonName; NotAfter = Format-Utc $_.NotAfter; Sha256 = $_.Sha256 } }) } else { @() }
        Observations      = @($Observations | ForEach-Object { ConvertTo-ObservationDto $_ })
        Findings          = $sorted
        _Healthy          = $healthy
    }
}

function ConvertTo-LeafDto {
    param([object] $Facts)
    [pscustomobject]@{
        Subject      = $Facts.Subject
        Issuer       = $Facts.Issuer
        NotBefore    = Format-Utc $Facts.NotBefore
        NotAfter     = Format-Utc $Facts.NotAfter
        LifetimeDays = [math]::Round($Facts.LifetimeDays, 2)
        Sha256       = $Facts.Sha256
        SpkiSha256   = $Facts.SpkiSha256
        DnsNames     = @($Facts.DnsNames)
        IpAddresses  = @($Facts.IpAddresses)
        Key          = if ($Facts.Curve) { "$($Facts.KeyAlgorithm) $($Facts.KeyBits) $($Facts.Curve)" } else { "$($Facts.KeyAlgorithm) $($Facts.KeyBits)" }
        Signature    = $Facts.SignatureName
        OcspUris     = @($Facts.OcspUris)
        HasCrl       = $Facts.HasCrl
        HasSct       = $Facts.HasSct
    }
}

function ConvertTo-ObservationDto {
    param([object] $Observation)
    [pscustomobject]@{
        Address           = $Observation.Address
        Success           = $Observation.Success
        Error             = $Observation.Error
        ErrorStage        = $Observation.ErrorStage
        Protocol          = $Observation.Protocol
        CipherSuite       = $Observation.CipherSuite
        ElapsedMs         = $Observation.ElapsedMs
        LeafSha256        = if ($Observation.Analysis) { $Observation.Analysis.Leaf.Sha256 } else { $null }
        EffectiveNotAfter = if ($Observation.Analysis) { Format-Utc $Observation.Analysis.EffectiveNotAfter } else { $null }
        PresentedKnown    = $Observation.PresentedKnown
    }
}

function ConvertTo-BaselineDate {
    param([object] $Value)
    # ConvertFrom-Json already turns ISO strings into DateTime values, sometimes in local time.
    if ($null -eq $Value) { return $null }
    if ($Value -is [datetime]) { return ConvertTo-Utc $Value }
    if ($Value -is [DateTimeOffset]) { return $Value.UtcDateTime }
    $styles = [Globalization.DateTimeStyles]::AdjustToUniversal -bor [Globalization.DateTimeStyles]::AssumeUniversal
    $parsed = [datetime]::MinValue
    if ([datetime]::TryParse([string]$Value, [Globalization.CultureInfo]::InvariantCulture, $styles, [ref] $parsed)) { return ConvertTo-Utc $parsed }
    return $null
}

function Read-Baseline {
    if (-not $BaselinePath) { return @{} }
    $file = Resolve-UserPath $BaselinePath
    if (-not [IO.File]::Exists($file)) { return @{} }
    $doc = [IO.File]::ReadAllText($file) | ConvertFrom-Json -AsHashtable
    if ($null -eq $doc -or $doc['version'] -ne 1 -or $null -eq $doc['targets']) { throw "baseline $BaselinePath is not a version 1 baseline" }
    $map = @{}
    foreach ($key in $doc['targets'].Keys) {
        $leaves = @(foreach ($l in @($doc['targets'][$key]['leaves'])) {
                if ($null -eq $l) { continue }
                $notAfter = ConvertTo-BaselineDate $l['notAfter']
                [pscustomobject]@{ sha256 = [string]$l['sha256']; spki = [string]$l['spki']; issuer = [string]$l['issuer']; notAfter = $notAfter }
            })
        $map[$key] = [pscustomobject]@{ leaves = $leaves }
    }
    return $map
}

function Save-Baseline {
    param([hashtable] $Existing, [object[]] $Reports)
    $targets = [ordered]@{}
    foreach ($key in ($Existing.Keys | Sort-Object)) {
        $targets[$key] = @{ leaves = @(foreach ($l in @($Existing[$key].leaves)) {
                    [ordered]@{ sha256 = $l.sha256; spki = $l.spki; issuer = $l.issuer; notAfter = if ($l.notAfter) { Format-Utc $l.notAfter } else { $null } }
                }) }
    }
    foreach ($r in $Reports) {
        # Only targets that answered replace history; a transient outage must not erase what we knew.
        if (@($r._Healthy).Count -eq 0) { continue }
        $leaves = @($r._Healthy | ForEach-Object { $_.Analysis.Leaf } | Group-Object Sha256 | ForEach-Object {
                $l = $_.Group[0]
                [ordered]@{ sha256 = $l.Sha256; spki = $l.SpkiSha256; issuer = $l.Issuer; notAfter = Format-Utc $l.NotAfter }
            })
        $targets[$r.Target] = @{ leaves = $leaves }
    }
    $sortedTargets = [ordered]@{}
    foreach ($k in ($targets.Keys | Sort-Object)) { $sortedTargets[$k] = $targets[$k] }
    $doc = [ordered]@{ version = 1; updated = Format-Utc $script:NowUtc; targets = $sortedTargets }
    Write-FileAtomic -File $BaselinePath -Content (($doc | ConvertTo-Json -Depth 8) + "`n")
}

# ---------------------------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------------------------

function Format-PromLabel {
    param([string] $Value)
    if ($null -eq $Value) { return '' }
    return $Value.Replace('\', '\\').Replace('"', '\"').Replace("`n", '\n')
}

function ConvertTo-UnixSeconds {
    param([datetime] $Value)
    return ([DateTimeOffset]::new((ConvertTo-Utc $Value))).ToUnixTimeSeconds()
}

function Add-PromFamily {
    param([Text.StringBuilder] $Builder, [string] $Name, [string] $Help, [System.Collections.Generic.List[string]] $Samples)
    [void]$Builder.Append("# HELP $Name $Help`n")
    [void]$Builder.Append("# TYPE $Name gauge`n")
    foreach ($line in $Samples) { [void]$Builder.Append("$Name$line`n") }
}

function Write-PrometheusFile {
    param([object[]] $Reports)
    $success = [System.Collections.Generic.List[string]]::new()
    $effective = [System.Collections.Generic.List[string]]::new()
    $leafNotAfter = [System.Collections.Generic.List[string]]::new()
    $lifetime = [System.Collections.Generic.List[string]]::new()
    $findingCount = [System.Collections.Generic.List[string]]::new()
    $targetSeverity = [System.Collections.Generic.List[string]]::new()

    foreach ($r in $Reports) {
        $t = Format-PromLabel $r.Target
        foreach ($o in $r.Observations) {
            $success.Add(('{{target="{0}",address="{1}"}} {2}' -f $t, (Format-PromLabel $o.Address), [int][bool]$o.Success))
        }
        foreach ($o in @($r._Healthy)) {
            $labels = '{{target="{0}",address="{1}"}}' -f $t, (Format-PromLabel $o.Address)
            $effective.Add("$labels $(ConvertTo-UnixSeconds $o.Analysis.EffectiveNotAfter)")
            $leafNotAfter.Add("$labels $(ConvertTo-UnixSeconds $o.Analysis.Leaf.NotAfter)")
            $lifetime.Add("$labels $([long][math]::Round($o.Analysis.Leaf.LifetimeDays * 86400))")
        }
        foreach ($sev in @('fail', 'warn', 'info')) {
            $n = @($r.Findings | Where-Object { $_.Severity -eq $sev }).Count
            $findingCount.Add(('{{target="{0}",severity="{1}"}} {2}' -f $t, $sev, $n))
        }
        $targetSeverity.Add(('{{target="{0}"}} {1}' -f $t, $script:SeverityRank[$r.Severity]))
    }
    $run = [System.Collections.Generic.List[string]]::new()
    $run.Add(" $(ConvertTo-UnixSeconds $script:NowUtc)")

    $sb = [Text.StringBuilder]::new()
    Add-PromFamily $sb 'tls_readiness_probe_success' 'Whether the handshake or file read for this address succeeded.' $success
    Add-PromFamily $sb 'tls_readiness_effective_not_after_seconds' 'Earliest NotAfter across leaf and chain as Unix time.' $effective
    Add-PromFamily $sb 'tls_readiness_leaf_not_after_seconds' 'Leaf NotAfter as Unix time.' $leafNotAfter
    Add-PromFamily $sb 'tls_readiness_leaf_lifetime_seconds' 'Leaf validity period in seconds.' $lifetime
    Add-PromFamily $sb 'tls_readiness_findings' 'Number of findings per target and severity.' $findingCount
    Add-PromFamily $sb 'tls_readiness_target_severity' 'Worst severity per target: 0 ok, 1 warn, 2 fail.' $targetSeverity
    Add-PromFamily $sb 'tls_readiness_last_run_timestamp_seconds' 'When this audit ran as Unix time.' $run
    Write-FileAtomic -File $PrometheusPath -Content $sb.ToString()
}

function Write-TableReport {
    param([object[]] $Reports)
    $rows = foreach ($r in $Reports) {
        [pscustomobject]@{
            Severity = $r.Severity.ToUpperInvariant()
            Target   = $r.Target
            DaysLeft = if ($null -ne $r.DaysLeft) { $r.DaysLeft.ToString('0.0', [Globalization.CultureInfo]::InvariantCulture) } else { '-' }
            Expires  = if ($r.EffectiveNotAfter) { $r.EffectiveNotAfter.Substring(0, 10) } else { '-' }
            Life     = if ($null -ne $r.LifetimeDays) { [string][math]::Round($r.LifetimeDays) } else { '-' }
            Addrs    = "$($r.AddressesOk)/$($r.AddressesTotal)"
            Issuer   = if ($r.Leaf) { ([regex]::Match($r.Leaf.Issuer, 'CN=([^,]+)')).Groups[1].Value } else { '-' }
            Codes    = (@($r.Findings | Where-Object { $_.Severity -ne 'info' } | ForEach-Object { $_.Code } | Select-Object -Unique) -join ',')
        }
    }
    ($rows | Format-Table -AutoSize | Out-String -Width 4096).TrimEnd() | Write-Output
    Write-Output ''
    foreach ($r in $Reports) {
        foreach ($f in $r.Findings) {
            $scope = if (@($f.Addresses).Count -gt 0) { " [only $(@($f.Addresses) -join ', ')]" } else { '' }
            Write-Output ("{0,-4} {1}  {2}: {3}{4}" -f $f.Severity, $r.Target, $f.Code, $f.Message, $scope)
        }
    }
    $fails = @($Reports | Where-Object { $_.Severity -eq 'fail' }).Count
    $warns = @($Reports | Where-Object { $_.Severity -eq 'warn' }).Count
    Write-Output ''
    Write-Output ("{0} targets: {1} fail, {2} warn, {3} ok. Checked at {4}." -f $Reports.Count, $fails, $warns, ($Reports.Count - $fails - $warns), (Format-Utc $script:NowUtc))
}

function Select-PublicReport {
    param([object] $Report)
    $Report | Select-Object -Property * -ExcludeProperty _Healthy
}

# ---------------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------------

$exitCode = 0
try {
    $script:NowUtc = ConvertTo-Utc $Now
    $script:Idn = [Globalization.IdnMapping]::new()

    if (-not $Target -and -not $TargetFile -and -not $Path) {
        throw 'nothing to audit: pass -Target, -TargetFile or -Path'
    }
    if ($UpdateBaseline -and -not $BaselinePath) { throw '-UpdateBaseline needs -BaselinePath' }
    if ($CriticalDays -gt $WarnDays) { throw '-CriticalDays must not exceed -WarnDays' }

    $script:CustomRoots = [System.Collections.Generic.List[System.Security.Cryptography.X509Certificates.X509Certificate2]]::new()
    foreach ($rootFile in @($TrustedRootPath)) {
        if (-not $rootFile) { continue }
        $resolvedRoot = Resolve-UserPath $rootFile
        foreach ($c in (Read-CertificateFile $resolvedRoot).Certificates) { $script:CustomRoots.Add($c) }
    }
    if (@($TrustedRootPath | Where-Object { $_ }).Count -gt 0 -and $script:CustomRoots.Count -eq 0) {
        throw '-TrustedRootPath was given but no certificates were read from it'
    }

    $baseline = Read-Baseline
    $specs = Read-TargetList
    $reports = [System.Collections.Generic.List[object]]::new()

    if ($specs.Count -gt 0) {
        Import-ProbeType
        $netSpecs = foreach ($s in $specs) {
            $t = New-Object -TypeName 'TlsRenewalReadinessV1.TargetSpec'
            $t.Label = $s.Label; $t.HostName = $s.HostName; $t.Port = $s.Port; $t.Mode = $s.Mode; $t.Sni = $s.Sni
            $t
        }
        $typedSpecs = [Array]::CreateInstance(('TlsRenewalReadinessV1.TargetSpec' -as [type]), @($netSpecs).Count)
        $i = 0
        foreach ($t in @($netSpecs)) { $typedSpecs[$i] = $t; $i++ }

        $results = ('TlsRenewalReadinessV1.Prober' -as [type])::Run($typedSpecs, $Concurrency, $TimeoutMs, $MaxAddressesPerHost, $AddressFamily)

        $specByLabel = @{}
        foreach ($s in $specs) { $specByLabel[$s.Label] = $s }
        $observations = @(foreach ($res in $results) { Measure-ProbeResult -Result $res -Spec $specByLabel[$res.Label] })
        foreach ($s in $specs) {
            $mine = @($observations | Where-Object { $_.Label -eq $s.Label } | Sort-Object Address)
            $reports.Add((Merge-TargetReport -Label $s.Label -Mode $s.Mode -Observations $mine -BaselineEntry $baseline[$s.Label]))
        }
    }

    foreach ($file in (Get-CertificateFileList)) {
        $obs = Measure-CertificateFileEntry $file
        $reports.Add((Merge-TargetReport -Label $file -Mode 'file' -Observations @($obs) -BaselineEntry $baseline[$file]))
    }

    $ordered = @($reports | Sort-Object @{ Expression = { $script:SeverityRank[$_.Severity] }; Descending = $true },
        @{ Expression = { if ($null -ne $_.DaysLeft) { [double]$_.DaysLeft } else { [double]::MinValue } } },
        @{ Expression = { $_.Target } })

    if ($UpdateBaseline) { Save-Baseline -Existing $baseline -Reports $ordered }
    if ($PrometheusPath) { Write-PrometheusFile -Reports $ordered }

    switch ($Format) {
        'Table'  { Write-TableReport -Reports $ordered }
        'Json'   { Write-Output (ConvertTo-Json -InputObject @($ordered | ForEach-Object { Select-PublicReport $_ }) -Depth 8) }
        'Ndjson' { foreach ($r in $ordered) { Write-Output (Select-PublicReport $r | ConvertTo-Json -Depth 8 -Compress) } }
    }

    if (@($ordered | Where-Object { $_.Severity -eq 'fail' }).Count -gt 0) { $exitCode = 2 }
    elseif ($FailOnWarn -and @($ordered | Where-Object { $_.Severity -eq 'warn' }).Count -gt 0) { $exitCode = 1 }
}
catch {
    [Console]::Error.WriteLine("TlsRenewalReadinessAuditor: $($_.Exception.Message)")
    $exitCode = 3
}
exit $exitCode
