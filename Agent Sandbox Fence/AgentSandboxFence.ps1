#Requires -Version 5.1
<#
    AgentSandboxFence.ps1

    Runs a child command (an AI coding agent, an MCP server, anything you do not
    fully trust) inside a Windows Job Object with a hard memory ceiling and a
    process-count ceiling enforced by the kernel, a polled CPU-time and
    wall-clock budget enforced by this script, and a loopback HTTP/HTTPS proxy
    that only lets the child reach hostnames you explicitly allowlisted.

    See README.md in this folder for the full design notes, threat model and
    known limitations.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$AllowListPath,

    [ValidateRange(16, 65536)]
    [int]$MaxMemoryMB = 512,

    [ValidateRange(1, 86400)]
    [int]$MaxCpuSeconds = 120,

    [ValidateRange(1, 4096)]
    [int]$MaxProcesses = 32,

    [ValidateRange(1, 86400)]
    [int]$TimeoutSeconds = 600,

    [ValidateRange(1, 65535)]
    [int]$ProxyPort = 8899,

    [string]$LogPath = ".\agent-fence-$(Get-Date -Format 'yyyyMMdd-HHmmss').jsonl",

    [ValidateRange(50, 60000)]
    [int]$PollIntervalMs = 500,

    [switch]$DryRun,
    [switch]$FirewallEnforce,
    [switch]$FailOnDeniedConnections,

    [Parameter(Mandatory = $true, ValueFromRemainingArguments = $true)]
    [string[]]$Command
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ExitCodeBreach = 124
$ExitCodeDeniedPolicy = 3

# ---------------------------------------------------------------------------
# Embedded native layer. Everything that touches Win32 (Job Objects, raw
# CreateProcess) or needs real socket concurrency (the egress proxy) lives in
# C#, compiled once per run with Add-Type. PowerShell only orchestrates.
# ---------------------------------------------------------------------------

$CSharpSource = @'
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace AgentFence
{
    [StructLayout(LayoutKind.Sequential)]
    public struct JOBOBJECT_BASIC_LIMIT_INFORMATION
    {
        public long PerProcessUserTimeLimit;
        public long PerJobUserTimeLimit;
        public uint LimitFlags;
        public UIntPtr MinimumWorkingSetSize;
        public UIntPtr MaximumWorkingSetSize;
        public uint ActiveProcessLimit;
        public UIntPtr Affinity;
        public uint PriorityClass;
        public uint SchedulingClass;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct IO_COUNTERS
    {
        public ulong ReadOperationCount;
        public ulong WriteOperationCount;
        public ulong OtherOperationCount;
        public ulong ReadTransferCount;
        public ulong WriteTransferCount;
        public ulong OtherTransferCount;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
    {
        public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
        public IO_COUNTERS IoInfo;
        public UIntPtr ProcessMemoryLimit;
        public UIntPtr JobMemoryLimit;
        public UIntPtr PeakProcessMemoryUsed;
        public UIntPtr PeakJobMemoryUsed;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct JOBOBJECT_BASIC_ACCOUNTING_INFORMATION
    {
        public long TotalUserTime;
        public long TotalKernelTime;
        public long ThisPeriodTotalUserTime;
        public long ThisPeriodTotalKernelTime;
        public uint TotalPageFaultCount;
        public uint TotalProcesses;
        public uint ActiveProcesses;
        public uint TotalTerminatedProcesses;
    }

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    public struct STARTUPINFO
    {
        public int cb;
        public string lpReserved;
        public string lpDesktop;
        public string lpTitle;
        public uint dwX;
        public uint dwY;
        public uint dwXSize;
        public uint dwYSize;
        public uint dwXCountChars;
        public uint dwYCountChars;
        public uint dwFillAttribute;
        public uint dwFlags;
        public short wShowWindow;
        public short cbReserved2;
        public IntPtr lpReserved2;
        public IntPtr hStdInput;
        public IntPtr hStdOutput;
        public IntPtr hStdError;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct PROCESS_INFORMATION
    {
        public IntPtr hProcess;
        public IntPtr hThread;
        public uint dwProcessId;
        public uint dwThreadId;
    }

    internal static class Interop
    {
        public const int JobObjectBasicAccountingInformation = 1;
        public const int JobObjectExtendedLimitInformation = 9;

        public const uint JOB_OBJECT_LIMIT_ACTIVE_PROCESS = 0x00000008;
        public const uint JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100;
        public const uint JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000;

        public const uint CREATE_SUSPENDED = 0x00000004;
        public const uint CREATE_UNICODE_ENVIRONMENT = 0x00000400;
        public const uint STARTF_USESTDHANDLES = 0x00000100;

        public const int STD_INPUT_HANDLE = -10;
        public const int STD_OUTPUT_HANDLE = -11;
        public const int STD_ERROR_HANDLE = -12;

        public const uint WAIT_TIMEOUT = 258;

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern IntPtr CreateJobObject(IntPtr lpJobAttributes, string lpName);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool SetInformationJobObject(IntPtr hJob, int infoClass, IntPtr lpJobObjectInfo, uint cbJobObjectInfoLength);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool QueryInformationJobObject(IntPtr hJob, int infoClass, IntPtr lpJobObjectInfo, uint cbJobObjectInfoLength, IntPtr lpReturnLength);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool AssignProcessToJobObject(IntPtr hJob, IntPtr hProcess);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool TerminateJobObject(IntPtr hJob, uint uExitCode);

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern bool CreateProcess(
            string lpApplicationName,
            StringBuilder lpCommandLine,
            IntPtr lpProcessAttributes,
            IntPtr lpThreadAttributes,
            bool bInheritHandles,
            uint dwCreationFlags,
            IntPtr lpEnvironment,
            string lpCurrentDirectory,
            ref STARTUPINFO lpStartupInfo,
            out PROCESS_INFORMATION lpProcessInformation);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern uint ResumeThread(IntPtr hThread);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern uint WaitForSingleObject(IntPtr hHandle, uint dwMilliseconds);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool GetExitCodeProcess(IntPtr hProcess, out uint lpExitCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool TerminateProcess(IntPtr hProcess, uint uExitCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern IntPtr GetStdHandle(int nStdHandle);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool CloseHandle(IntPtr hObject);
    }

    public static class CommandLineBuilder
    {
        // Implements the documented Windows argv quoting algorithm (the same
        // one .NET's own process launcher uses) so arguments with spaces,
        // quotes or trailing backslashes survive CreateProcess unchanged.
        public static string Build(string[] args)
        {
            var parts = new string[args.Length];
            for (int i = 0; i < args.Length; i++)
            {
                parts[i] = QuoteArgument(args[i]);
            }
            return string.Join(" ", parts);
        }

        private static string QuoteArgument(string arg)
        {
            if (arg.Length > 0 && arg.IndexOfAny(new[] { ' ', '\t', '\n', '\v', '"' }) < 0)
            {
                return arg;
            }

            var sb = new StringBuilder();
            sb.Append('"');
            int i = 0;
            while (i < arg.Length)
            {
                int backslashes = 0;
                while (i < arg.Length && arg[i] == '\\')
                {
                    backslashes++;
                    i++;
                }

                if (i == arg.Length)
                {
                    sb.Append('\\', backslashes * 2);
                    break;
                }
                else if (arg[i] == '"')
                {
                    sb.Append('\\', backslashes * 2 + 1);
                    sb.Append('"');
                    i++;
                }
                else
                {
                    sb.Append('\\', backslashes);
                    sb.Append(arg[i]);
                    i++;
                }
            }
            sb.Append('"');
            return sb.ToString();
        }
    }

    public sealed class JobFence : IDisposable
    {
        private IntPtr _handle;

        public JobFence(long maxProcessMemoryBytes, int maxActiveProcesses)
        {
            _handle = Interop.CreateJobObject(IntPtr.Zero, null);
            if (_handle == IntPtr.Zero)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error(), "CreateJobObject failed");
            }

            var info = new JOBOBJECT_EXTENDED_LIMIT_INFORMATION();
            info.BasicLimitInformation.LimitFlags =
                Interop.JOB_OBJECT_LIMIT_PROCESS_MEMORY |
                Interop.JOB_OBJECT_LIMIT_ACTIVE_PROCESS |
                Interop.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            info.BasicLimitInformation.ActiveProcessLimit = (uint)maxActiveProcesses;
            info.ProcessMemoryLimit = (UIntPtr)(ulong)maxProcessMemoryBytes;

            int size = Marshal.SizeOf(typeof(JOBOBJECT_EXTENDED_LIMIT_INFORMATION));
            IntPtr ptr = Marshal.AllocHGlobal(size);
            try
            {
                Marshal.StructureToPtr(info, ptr, false);
                if (!Interop.SetInformationJobObject(_handle, Interop.JobObjectExtendedLimitInformation, ptr, (uint)size))
                {
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "SetInformationJobObject failed");
                }
            }
            finally
            {
                Marshal.FreeHGlobal(ptr);
            }
        }

        // Builds the environment block, launches the process suspended,
        // assigns it to this job while it cannot yet spawn anything of its
        // own, then resumes it. Doing the assignment before the first
        // instruction runs is what stops a fast-launching child from
        // spawning a grandchild that escapes the job.
        public PROCESS_INFORMATION StartSuspendedInJob(string commandLine, IDictionary<string, string> environment, string workingDirectory)
        {
            var si = new STARTUPINFO();
            si.cb = Marshal.SizeOf(typeof(STARTUPINFO));
            si.dwFlags = Interop.STARTF_USESTDHANDLES;
            si.hStdInput = Interop.GetStdHandle(Interop.STD_INPUT_HANDLE);
            si.hStdOutput = Interop.GetStdHandle(Interop.STD_OUTPUT_HANDLE);
            si.hStdError = Interop.GetStdHandle(Interop.STD_ERROR_HANDLE);

            IntPtr envBlock = BuildEnvironmentBlock(environment);
            var cmd = new StringBuilder(commandLine, commandLine.Length + 8);
            PROCESS_INFORMATION pi;
            try
            {
                uint flags = Interop.CREATE_SUSPENDED | Interop.CREATE_UNICODE_ENVIRONMENT;
                bool ok = Interop.CreateProcess(null, cmd, IntPtr.Zero, IntPtr.Zero, true, flags, envBlock, workingDirectory, ref si, out pi);
                if (!ok)
                {
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "CreateProcess failed for: " + commandLine);
                }
            }
            finally
            {
                Marshal.FreeHGlobal(envBlock);
            }

            if (!Interop.AssignProcessToJobObject(_handle, pi.hProcess))
            {
                int err = Marshal.GetLastWin32Error();
                Interop.TerminateProcess(pi.hProcess, 1);
                Interop.CloseHandle(pi.hThread);
                Interop.CloseHandle(pi.hProcess);
                throw new Win32Exception(err, "AssignProcessToJobObject failed; terminated the orphaned process");
            }

            Interop.ResumeThread(pi.hThread);
            Interop.CloseHandle(pi.hThread);
            return pi;
        }

        public uint Wait(IntPtr hProcess, int milliseconds)
        {
            return Interop.WaitForSingleObject(hProcess, (uint)milliseconds);
        }

        public uint GetExitCode(IntPtr hProcess)
        {
            uint code;
            Interop.GetExitCodeProcess(hProcess, out code);
            return code;
        }

        public void CloseProcessHandle(IntPtr hProcess)
        {
            Interop.CloseHandle(hProcess);
        }

        public bool TryGetAccounting(out TimeSpan totalUserTime, out int activeProcesses)
        {
            int size = Marshal.SizeOf(typeof(JOBOBJECT_BASIC_ACCOUNTING_INFORMATION));
            IntPtr buf = Marshal.AllocHGlobal(size);
            try
            {
                bool ok = Interop.QueryInformationJobObject(_handle, Interop.JobObjectBasicAccountingInformation, buf, (uint)size, IntPtr.Zero);
                if (!ok)
                {
                    totalUserTime = TimeSpan.Zero;
                    activeProcesses = 0;
                    return false;
                }
                var info = (JOBOBJECT_BASIC_ACCOUNTING_INFORMATION)Marshal.PtrToStructure(buf, typeof(JOBOBJECT_BASIC_ACCOUNTING_INFORMATION));
                totalUserTime = TimeSpan.FromTicks(info.TotalUserTime);
                activeProcesses = (int)info.ActiveProcesses;
                return true;
            }
            finally
            {
                Marshal.FreeHGlobal(buf);
            }
        }

        public void Terminate(uint exitCode)
        {
            Interop.TerminateJobObject(_handle, exitCode);
        }

        private static IntPtr BuildEnvironmentBlock(IDictionary<string, string> vars)
        {
            var keys = new List<string>(vars.Keys);
            keys.Sort(StringComparer.OrdinalIgnoreCase);
            var sb = new StringBuilder();
            foreach (var k in keys)
            {
                sb.Append(k).Append('=').Append(vars[k]).Append('\0');
            }
            sb.Append('\0');
            return Marshal.StringToHGlobalUni(sb.ToString());
        }

        public void Dispose()
        {
            if (_handle != IntPtr.Zero)
            {
                Interop.CloseHandle(_handle);
                _handle = IntPtr.Zero;
            }
        }
    }

    // A minimal HTTP/HTTPS forward proxy. It never terminates TLS: for
    // CONNECT it reads only the plaintext request line and headers, decides
    // allow or deny from the hostname, then splices raw bytes between the
    // two sockets. It cannot see anything inside the TLS session and does
    // not try to.
    public sealed class EgressGateway : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly HashSet<string> _exactHosts;
        private readonly List<string> _wildcardSuffixes;
        private readonly bool _dryRun;
        private readonly StreamWriter _log;
        private readonly object _logLock = new object();
        private volatile bool _running;
        private long _allowedCount;
        private long _deniedCount;

        public EgressGateway(IPAddress bindAddress, int port, IEnumerable<string> allowRules, bool dryRun, string logPath)
        {
            _listener = new TcpListener(bindAddress, port);
            _exactHosts = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _wildcardSuffixes = new List<string>();
            foreach (var rawRule in allowRules)
            {
                string rule = rawRule.Trim();
                if (rule.Length == 0 || rule[0] == '#')
                {
                    continue;
                }
                if (rule.StartsWith("*.", StringComparison.Ordinal))
                {
                    _wildcardSuffixes.Add(rule.Substring(1));
                }
                else
                {
                    _exactHosts.Add(rule);
                }
            }
            _dryRun = dryRun;
            _log = new StreamWriter(new FileStream(logPath, FileMode.Append, FileAccess.Write, FileShare.Read));
            _log.AutoFlush = true;
        }

        public long AllowedCount { get { return Interlocked.Read(ref _allowedCount); } }
        public long DeniedCount { get { return Interlocked.Read(ref _deniedCount); } }
        public int BoundPort { get { return ((IPEndPoint)_listener.LocalEndpoint).Port; } }

        public void Start()
        {
            _running = true;
            _listener.Start(128);
            _listener.BeginAcceptTcpClient(OnAccept, null);
        }

        public void Stop()
        {
            _running = false;
            try { _listener.Stop(); } catch { }
            try { _log.Flush(); _log.Dispose(); } catch { }
        }

        private void OnAccept(IAsyncResult ar)
        {
            TcpClient client = null;
            try
            {
                client = _listener.EndAcceptTcpClient(ar);
            }
            catch (ObjectDisposedException) { return; }
            catch (SocketException) { }

            if (_running)
            {
                try { _listener.BeginAcceptTcpClient(OnAccept, null); }
                catch (ObjectDisposedException) { }
            }

            if (client != null)
            {
                ThreadPool.QueueUserWorkItem(HandleClient, client);
            }
        }

        private void HandleClient(object state)
        {
            var client = (TcpClient)state;
            try
            {
                HandleClientCore(client);
            }
            catch
            {
                // One malformed or hostile connection must never take down
                // the gateway thread pool.
            }
            finally
            {
                try { client.Close(); } catch { }
            }
        }

        private void HandleClientCore(TcpClient client)
        {
            client.NoDelay = true;
            NetworkStream stream = client.GetStream();

            byte[] headerBytes = ReadHttpHeaderBytes(stream);
            if (headerBytes.Length == 0)
            {
                return;
            }

            string headerText = Encoding.ASCII.GetString(headerBytes);
            string[] lines = headerText.Split(new[] { "\r\n" }, StringSplitOptions.None);
            string[] requestParts = lines[0].Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (requestParts.Length < 2)
            {
                return;
            }

            string method = requestParts[0];
            string target = requestParts[1];
            bool isConnect = string.Equals(method, "CONNECT", StringComparison.OrdinalIgnoreCase);

            string host;
            int port;
            if (isConnect)
            {
                ParseHostPort(target, 443, out host, out port);
            }
            else
            {
                Uri uri;
                if (Uri.TryCreate(target, UriKind.Absolute, out uri))
                {
                    host = uri.Host;
                    port = uri.IsDefaultPort
                        ? (string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase) ? 443 : 80)
                        : uri.Port;
                }
                else
                {
                    string hostHeader = FindHeader(lines, "Host");
                    if (hostHeader == null)
                    {
                        return;
                    }
                    ParseHostPort(hostHeader, 80, out host, out port);
                }
            }

            bool allowed = IsAllowed(host);
            string kind = isConnect ? "connect" : "http";

            if (!allowed && !_dryRun)
            {
                Interlocked.Increment(ref _deniedCount);
                LogEvent(kind + "_deny", host, port, "deny", "not_in_allowlist");
                WriteAscii(stream, isConnect
                    ? "HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n"
                    : "HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
                return;
            }

            if (!allowed)
            {
                LogEvent(kind + "_deny", host, port, "would_deny_dryrun", "not_in_allowlist");
            }
            else
            {
                Interlocked.Increment(ref _allowedCount);
                LogEvent(kind + "_allow", host, port, "allow", "allowlisted");
            }

            TcpClient upstream;
            try
            {
                upstream = new TcpClient();
                upstream.Connect(host, port);
            }
            catch (Exception ex)
            {
                LogEvent("upstream_error", host, port, "error", ex.Message);
                WriteAscii(stream, "HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n");
                return;
            }

            using (upstream)
            {
                NetworkStream upstreamStream = upstream.GetStream();
                if (isConnect)
                {
                    WriteAscii(stream, "HTTP/1.1 200 Connection Established\r\n\r\n");
                }
                else
                {
                    upstreamStream.Write(headerBytes, 0, headerBytes.Length);
                }
                PumpBidirectional(stream, upstreamStream);
            }
        }

        private bool IsAllowed(string host)
        {
            if (_exactHosts.Contains(host))
            {
                return true;
            }
            for (int i = 0; i < _wildcardSuffixes.Count; i++)
            {
                string suffix = _wildcardSuffixes[i];
                if (host.Length > suffix.Length && host.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }

        private static void WriteAscii(Stream stream, string text)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(text);
            try { stream.Write(bytes, 0, bytes.Length); } catch { }
        }

        private static byte[] ReadHttpHeaderBytes(NetworkStream stream)
        {
            var buffer = new List<byte>(512);
            while (true)
            {
                int b = stream.ReadByte();
                if (b == -1)
                {
                    break;
                }
                buffer.Add((byte)b);
                int n = buffer.Count;
                if (n >= 4 &&
                    buffer[n - 4] == (byte)'\r' && buffer[n - 3] == (byte)'\n' &&
                    buffer[n - 2] == (byte)'\r' && buffer[n - 1] == (byte)'\n')
                {
                    break;
                }
                if (n > 65536)
                {
                    throw new IOException("request header exceeded 64 KiB");
                }
            }
            return buffer.ToArray();
        }

        private static string FindHeader(string[] lines, string name)
        {
            for (int i = 1; i < lines.Length; i++)
            {
                int c = lines[i].IndexOf(':');
                if (c <= 0)
                {
                    continue;
                }
                if (string.Equals(lines[i].Substring(0, c).Trim(), name, StringComparison.OrdinalIgnoreCase))
                {
                    return lines[i].Substring(c + 1).Trim();
                }
            }
            return null;
        }

        private static void ParseHostPort(string hostPort, int defaultPort, out string host, out int port)
        {
            hostPort = hostPort.Trim();
            if (hostPort.Length > 0 && hostPort[0] == '[')
            {
                int end = hostPort.IndexOf(']');
                host = hostPort.Substring(1, end - 1);
                string rest = hostPort.Substring(end + 1);
                int bracketPort;
                if (rest.Length > 0 && rest[0] == ':' && int.TryParse(rest.Substring(1), out bracketPort))
                {
                    port = bracketPort;
                }
                else
                {
                    port = defaultPort;
                }
                return;
            }
            int idx = hostPort.LastIndexOf(':');
            if (idx > 0)
            {
                host = hostPort.Substring(0, idx);
                int p;
                port = int.TryParse(hostPort.Substring(idx + 1), out p) ? p : defaultPort;
            }
            else
            {
                host = hostPort;
                port = defaultPort;
            }
        }

        private static void PumpBidirectional(NetworkStream a, NetworkStream b)
        {
            var worker = new Thread(delegate () { PumpOneWay(a, b); });
            worker.IsBackground = true;
            worker.Start();
            PumpOneWay(b, a);
            try { worker.Join(2000); } catch { }
        }

        private static void PumpOneWay(Stream from, Stream to)
        {
            var buffer = new byte[16384];
            try
            {
                int read;
                while ((read = from.Read(buffer, 0, buffer.Length)) > 0)
                {
                    to.Write(buffer, 0, read);
                }
            }
            catch { }
            finally
            {
                try { to.Dispose(); } catch { }
            }
        }

        private void LogEvent(string evt, string host, int port, string verdict, string reason)
        {
            string line = "{\"ts\":\"" + DateTime.UtcNow.ToString("o") +
                "\",\"event\":\"" + JsonEscape(evt) +
                "\",\"host\":\"" + JsonEscape(host) +
                "\",\"port\":" + port +
                ",\"verdict\":\"" + JsonEscape(verdict) +
                "\",\"reason\":\"" + JsonEscape(reason) + "\"}";
            lock (_logLock)
            {
                _log.WriteLine(line);
            }
        }

        private static string JsonEscape(string s)
        {
            if (s == null)
            {
                return "";
            }
            var sb = new StringBuilder();
            foreach (char c in s)
            {
                switch (c)
                {
                    case '"': sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    default:
                        if (c < 0x20)
                        {
                            sb.Append("\\u").Append(((int)c).ToString("x4"));
                        }
                        else
                        {
                            sb.Append(c);
                        }
                        break;
                }
            }
            return sb.ToString();
        }

        public void Dispose()
        {
            Stop();
        }
    }
}
'@

$loadedAssemblies = [System.AppDomain]::CurrentDomain.GetAssemblies() |
    Where-Object { -not $_.IsDynamic -and $_.Location } |
    Select-Object -ExpandProperty Location -Unique

Add-Type -TypeDefinition $CSharpSource -ReferencedAssemblies $loadedAssemblies -Language CSharp -ErrorAction Stop

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

function Read-AllowList {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Allowlist file not found: $Path"
    }
    $rules = @(Get-Content -LiteralPath $Path | ForEach-Object { $_.Trim() } |
        Where-Object { $_.Length -gt 0 -and -not $_.StartsWith('#') })
    if ($rules.Count -eq 0) {
        Write-Warning "Allowlist at '$Path' has no rules. Every outbound host will be denied unless -DryRun is set."
    }
    # The unary comma stops PowerShell from unrolling a single-element array
    # into a bare string when it crosses the function's return boundary,
    # which would otherwise break the IEnumerable<string> constructor call.
    return ,$rules
}

$resolvedCommand = Get-Command -Name $Command[0] -ErrorAction SilentlyContinue
if (-not $resolvedCommand -or $resolvedCommand.CommandType -ne 'Application') {
    throw "'$($Command[0])' did not resolve to a real executable. CreateProcess launches .exe files directly and does not consult file associations, so a .cmd, .bat or .ps1 entry point must be invoked through its interpreter, for example: -Command cmd.exe /c agent.cmd, or pwsh.exe -File agent.ps1."
}
$resolvedExePath = $resolvedCommand.Source

$allowRules = Read-AllowList -Path $AllowListPath

$logDir = Split-Path -Parent $LogPath
if ($logDir -and -not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}
$LogPath = (Resolve-Path -LiteralPath (New-Item -ItemType File -Path $LogPath -Force).FullName).ProviderPath

$gateway = [AgentFence.EgressGateway]::new([System.Net.IPAddress]::Loopback, $ProxyPort, $allowRules, [bool]$DryRun, $LogPath)
$gateway.Start()
$boundPort = $gateway.BoundPort

$envTable = New-Object 'System.Collections.Generic.Dictionary[string,string]' ([StringComparer]::OrdinalIgnoreCase)
foreach ($entry in [System.Environment]::GetEnvironmentVariables().GetEnumerator()) {
    $envTable[[string]$entry.Key] = [string]$entry.Value
}
$proxyUrl = "http://127.0.0.1:$boundPort"
foreach ($name in @('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy')) {
    $envTable[$name] = $proxyUrl
}
foreach ($name in @('NO_PROXY', 'no_proxy')) {
    # Deliberately empty: this fence proxies everything, including
    # localhost and link-local addresses. A confused agent reaching for
    # 169.254.169.254 (the cloud metadata endpoint on AWS/Azure/GCP) hits
    # the allowlist like any other host instead of sailing through on a
    # default "skip the proxy for local addresses" exemption.
    $envTable[$name] = ''
}

$commandLine = [AgentFence.CommandLineBuilder]::Build($Command)

$firewallRuleName = $null
if ($FirewallEnforce) {
    $isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    if (-not $isAdmin) {
        $gateway.Stop()
        throw "-FirewallEnforce requires an elevated (Run as Administrator) PowerShell session."
    }
    $firewallRuleName = "AgentSandboxFence-$PID"
    New-NetFirewallRule -DisplayName $firewallRuleName -Direction Outbound -Program $resolvedExePath -Action Block -Profile Any | Out-Null
}

$jobFence = [AgentFence.JobFence]::new([long]$MaxMemoryMB * 1MB, $MaxProcesses)
$breach = $null
$processInfo = $null
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

try {
    $processInfo = $jobFence.StartSuspendedInJob($commandLine, $envTable, (Get-Location).ProviderPath)

    while ($true) {
        $waitResult = $jobFence.Wait($processInfo.hProcess, $PollIntervalMs)
        if ($waitResult -eq 0) {
            break
        }

        $totalUserTime = [TimeSpan]::Zero
        $activeProcesses = 0
        [void]$jobFence.TryGetAccounting([ref]$totalUserTime, [ref]$activeProcesses)

        if ($totalUserTime.TotalSeconds -ge $MaxCpuSeconds) {
            $breach = 'cpu_budget_exceeded'
            $jobFence.Terminate([uint32]$ExitCodeBreach)
            $jobFence.Wait($processInfo.hProcess, 5000) | Out-Null
            break
        }
        if ($stopwatch.Elapsed.TotalSeconds -ge $TimeoutSeconds) {
            $breach = 'wall_clock_timeout'
            $jobFence.Terminate([uint32]$ExitCodeBreach)
            $jobFence.Wait($processInfo.hProcess, 5000) | Out-Null
            break
        }
    }

    $childExitCode = $jobFence.GetExitCode($processInfo.hProcess)
}
finally {
    if ($processInfo) {
        $jobFence.CloseProcessHandle($processInfo.hProcess)
    }
    $gateway.Stop()
    if ($firewallRuleName) {
        Remove-NetFirewallRule -DisplayName $firewallRuleName -ErrorAction SilentlyContinue | Out-Null
    }
    $jobFence.Dispose()
}

$summary = [PSCustomObject]@{
    ChildExitCode      = [int]$childExitCode
    Breach             = $breach
    AllowedConnections = $gateway.AllowedCount
    DeniedConnections  = $gateway.DeniedCount
    WallClockSeconds   = [math]::Round($stopwatch.Elapsed.TotalSeconds, 2)
    LogPath            = $LogPath
    DryRun             = [bool]$DryRun
}
Add-Content -LiteralPath $LogPath -Value ("{`"event`":`"summary`",`"data`":" + ($summary | ConvertTo-Json -Compress) + "}")
Write-Output $summary

if ($breach) {
    exit $ExitCodeBreach
}
if ($FailOnDeniedConnections -and $gateway.DeniedCount -gt 0) {
    exit $ExitCodeDeniedPolicy
}
exit $childExitCode
