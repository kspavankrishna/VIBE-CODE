#Requires -Version 5.1
<#
    PathShadowDriftAuditor.ps1

    Audits the effective PATH of the current process: which directories are
    missing, duplicated, relative or writable by untrusted users, which tools
    are shadowed by an earlier directory (including the Windows Store alias
    stubs), and whether the whole picture still matches a signed baseline you
    took earlier. Works in Windows PowerShell 5.1 and PowerShell 7 on Windows,
    Linux and macOS.

    Exit codes: 0 clean, 1 findings at or above -FailOn, 2 usage or runtime
    error, 3 baseline missing, corrupt, edited or from another schema.

    See README.md in this folder for the design notes and limits.
#>

[CmdletBinding()]
param(
    [ValidateSet('Audit', 'Snapshot', 'Verify')]
    [string]$Mode = 'Audit',

    [string]$BaselinePath = (Join-Path -Path (Get-Location).Path -ChildPath 'path-baseline.json'),

    [string[]]$Tool = @('python', 'python3', 'pip', 'node', 'npm', 'npx', 'git', 'pwsh', 'dotnet', 'java', 'go', 'rustc', 'cargo', 'uv'),

    [string[]]$AllowShadow = @(),

    [string[]]$IgnoreDir = @(),

    [ValidateSet('Text', 'Json')]
    [string]$Format = 'Text',

    [string]$OutputPath,

    [ValidateSet('None', 'Info', 'Warning', 'Error')]
    [string]$FailOn = 'Error',

    [ValidateRange(1, 60)]
    [int]$VersionTimeoutSeconds = 5,

    [switch]$SkipVersionProbe,

    [switch]$AllShadows,

    [switch]$Force
)

Set-StrictMode -Version 3.0
$ErrorActionPreference = 'Stop'

$script:SchemaVersion = 1
$script:LegacyPathWarnLength = 2047
$script:SeverityRank = @{ Info = 0; Warning = 1; Error = 2 }
$script:Findings = New-Object 'System.Collections.Generic.List[object]'
$script:IsWindowsHost = ([Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT)
$script:Separator = [IO.Path]::PathSeparator
$script:ProbeArguments = @{ go = 'version'; java = '-version' }

# Rights that let an untrusted principal plant or replace a binary in a directory:
# CreateFiles, AppendData, DeleteSubdirectoriesAndFiles, Delete, ChangePermissions, TakeOwnership.
$script:WriteMask = [long](0x2 -bor 0x4 -bor 0x40 -bor 0x10000 -bor 0x40000 -bor 0x80000)
$script:UntrustedSids = @('S-1-1-0', 'S-1-5-11', 'S-1-5-32-545', 'S-1-5-4')

class AuditAbort : System.Exception {
    [int]$Code
    AuditAbort([string]$message, [int]$code) : base($message) { $this.Code = $code }
}

function Add-Finding {
    param(
        [Parameter(Mandatory)][string]$Id,
        [Parameter(Mandatory)][ValidateSet('Info', 'Warning', 'Error')][string]$Severity,
        [Parameter(Mandatory)][string]$Subject,
        [Parameter(Mandatory)][string]$Message,
        [string]$Evidence = ''
    )
    $script:Findings.Add([pscustomobject][ordered]@{
            id       = $Id
            severity = $Severity
            subject  = $Subject
            message  = $Message
            evidence = $Evidence
        })
}

function Get-Key {
    param([string]$Value)
    if ($script:IsWindowsHost) { return $Value.ToLowerInvariant() }
    return $Value
}

function Test-IsAbsolutePath {
    param([string]$Value)
    if ($script:IsWindowsHost) { return ($Value -match '^[A-Za-z]:[\\/]' -or $Value -match '^\\\\') }
    return $Value.StartsWith('/')
}

function ConvertTo-NormalPath {
    # Quotes stripped, variables expanded, separators unified, trailing separator removed
    # except on a root. Never touches the disk, so it is safe on dead network shares.
    param([string]$Raw)
    $value = $Raw.Trim()
    if ($value.Length -ge 2 -and $value.StartsWith('"') -and $value.EndsWith('"')) {
        $value = $value.Substring(1, $value.Length - 2).Trim()
    }
    $value = [Environment]::ExpandEnvironmentVariables($value)
    if ($script:IsWindowsHost) {
        $value = $value.Replace('/', '\')
        while ($value.Length -gt 3 -and $value.EndsWith('\')) { $value = $value.Substring(0, $value.Length - 1) }
    }
    else {
        while ($value.Length -gt 1 -and $value.EndsWith('/')) { $value = $value.Substring(0, $value.Length - 1) }
    }
    return $value
}

function Get-RegistryPath {
    # Returns the raw (unexpanded) PATH value and its registry kind, or $null.
    param([ValidateSet('Machine', 'User')][string]$Scope)
    if (-not $script:IsWindowsHost) { return $null }
    if ($Scope -eq 'Machine') {
        $key = [Microsoft.Win32.Registry]::LocalMachine.OpenSubKey('SYSTEM\CurrentControlSet\Control\Session Manager\Environment')
    }
    else {
        $key = [Microsoft.Win32.Registry]::CurrentUser.OpenSubKey('Environment')
    }
    if ($null -eq $key) { return $null }
    try {
        $raw = $key.GetValue('Path', $null, [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
        if ($null -eq $raw) { return $null }
        return [pscustomobject]@{ Raw = [string]$raw; Kind = $key.GetValueKind('Path').ToString() }
    }
    finally { $key.Dispose() }
}

function Get-PathEntries {
    $raw = [Environment]::GetEnvironmentVariable('PATH')
    if ([string]::IsNullOrEmpty($raw)) { throw [AuditAbort]::new('PATH is empty or unset in this process.', 2) }
    $entries = New-Object 'System.Collections.Generic.List[object]'
    $parts = $raw.Split($script:Separator)
    for ($i = 0; $i -lt $parts.Count; $i++) {
        $entries.Add([pscustomobject]@{
                Index      = $i
                Raw        = $parts[$i]
                Normalized = (ConvertTo-NormalPath -Raw $parts[$i])
            })
    }
    return [pscustomobject]@{ Raw = $raw; Entries = $entries }
}

function Test-IgnoredDir {
    param([string]$Path)
    foreach ($pattern in $IgnoreDir) { if ($Path -like $pattern) { return $true } }
    return $false
}

function Get-UntrustedWriter {
    # Returns a string naming who can write into the directory, or $null when nobody untrusted can.
    # Returns the marker 'unknown' only when the check itself could not run.
    param([string]$Dir)
    if ($script:IsWindowsHost) {
        try { $acl = Get-Acl -LiteralPath $Dir } catch { return $null }
        foreach ($ace in $acl.Access) {
            if ($ace.AccessControlType -ne 'Allow') { continue }
            try { $sid = $ace.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value } catch { continue }
            if ($script:UntrustedSids -notcontains $sid) { continue }
            $rights = [long]$ace.FileSystemRights
            if (($rights -band $script:WriteMask) -ne 0) {
                return ('{0} ({1})' -f $ace.IdentityReference.Value, $ace.FileSystemRights)
            }
        }
        return $null
    }
    try {
        $mode = [int][IO.File]::GetUnixFileMode($Dir)
    }
    catch { return $null }
    $sticky = (($mode -band 512) -ne 0)
    if (($mode -band 2) -ne 0 -and -not $sticky) { return 'other-write' }
    if (($mode -band 16) -ne 0 -and -not $sticky) { return 'group-write' }
    return $null
}

function Get-TrustAnchorIndex {
    # Index of the first PATH entry holding the OS core binaries. Anything writable before it
    # can shadow system tools.
    param($Entries)
    if ($script:IsWindowsHost) {
        $anchor = Get-Key -Value (Join-Path -Path $env:SystemRoot -ChildPath 'System32')
    }
    else { $anchor = '/usr/bin' }
    foreach ($e in $Entries) { if ((Get-Key -Value $e.Normalized) -eq $anchor) { return $e.Index } }
    return [int]::MaxValue
}

function Test-PathHygiene {
    param($PathInfo)
    $entries = $PathInfo.Entries
    $anchorIndex = Get-TrustAnchorIndex -Entries $entries
    $seen = @{}

    if ($PathInfo.Raw.Length -gt $script:LegacyPathWarnLength) {
        Add-Finding -Id 'P009' -Severity Warning -Subject 'PATH' `
            -Message 'PATH is longer than 2047 characters. Older installers and some SDK tools truncate it silently.' `
            -Evidence ("length={0}" -f $PathInfo.Raw.Length)
    }

    foreach ($e in $entries) {
        $label = ('#{0} {1}' -f $e.Index, $e.Raw)
        $path = $e.Normalized

        if ([string]::IsNullOrWhiteSpace($e.Raw)) {
            $sev = 'Warning'
            if (-not $script:IsWindowsHost) { $sev = 'Error' }
            Add-Finding -Id 'P004' -Severity $sev -Subject $label `
                -Message 'Empty PATH segment. On Unix shells it means the current directory, so any repository you cd into can plant a command.'
            continue
        }
        if ($e.Raw.Contains('"')) {
            Add-Finding -Id 'P007' -Severity Warning -Subject $label `
                -Message 'Entry contains quote characters. cmd.exe and some launchers mis-split quoted PATH entries that contain a separator.'
        }
        if ($path -match '%[^%]+%' -or $path.StartsWith('~') -or $path -match '\$\{?\w+') {
            Add-Finding -Id 'P005' -Severity Warning -Subject $label `
                -Message 'Entry still has an unexpanded variable or tilde after expansion. It will not resolve in most programs.' `
                -Evidence $path
            continue
        }
        if (-not (Test-IsAbsolutePath -Value $path)) {
            Add-Finding -Id 'P003' -Severity Error -Subject $label `
                -Message 'Relative PATH entry. It resolves against whatever directory the process happens to be in, which is a command hijack primitive.' `
                -Evidence $path
            continue
        }
        if (Test-IgnoredDir -Path $path) { continue }

        $key = Get-Key -Value $path
        if ($seen.ContainsKey($key)) {
            Add-Finding -Id 'P002' -Severity Warning -Subject $label `
                -Message ('Duplicate of entry #{0}. It costs a lookup per command and hides which copy you meant to prioritise.' -f $seen[$key]) `
                -Evidence $path
            continue
        }
        $seen[$key] = $e.Index

        if (Test-Path -LiteralPath $path -PathType Leaf) {
            Add-Finding -Id 'P008' -Severity Warning -Subject $label -Message 'Entry is a file, not a directory.' -Evidence $path
            continue
        }
        if (-not (Test-Path -LiteralPath $path -PathType Container)) {
            Add-Finding -Id 'P001' -Severity Warning -Subject $label `
                -Message 'Directory does not exist. A missing entry is a free slot for whoever creates that directory first.' -Evidence $path
            continue
        }

        $writer = Get-UntrustedWriter -Dir $path
        if ($null -ne $writer) {
            $sev = 'Warning'
            $why = 'It sits after the OS directory, so only commands it uniquely provides are exposed.'
            if ($e.Index -lt $anchorIndex) {
                $sev = 'Error'
                $why = 'It sits before the OS directory, so a planted file can replace system commands.'
            }
            Add-Finding -Id 'P006' -Severity $sev -Subject $label `
                -Message ('Writable by non-administrators. ' + $why) -Evidence $writer
        }
    }
}

function Test-RegistryPaths {
    if (-not $script:IsWindowsHost) { return }
    $composed = New-Object 'System.Collections.Generic.List[string]'
    foreach ($scope in 'Machine', 'User') {
        $reg = Get-RegistryPath -Scope $scope
        if ($null -eq $reg) { continue }
        if ($reg.Raw -match '%[^%]+%' -and $reg.Kind -eq 'String') {
            Add-Finding -Id 'R001' -Severity Error -Subject ("{0} PATH" -f $scope) `
                -Message 'Contains a %VARIABLE% but is stored as REG_SZ, not REG_EXPAND_SZ. Windows will never expand it. A tool that rewrote PATH dropped the type.' `
                -Evidence $reg.Raw
        }
        foreach ($part in $reg.Raw.Split(';')) {
            if (-not [string]::IsNullOrWhiteSpace($part)) { $composed.Add((Get-Key -Value (ConvertTo-NormalPath -Raw $part))) }
        }
    }
    if ($composed.Count -eq 0) { return }

    $registrySet = New-Object 'System.Collections.Generic.HashSet[string]'
    foreach ($c in $composed) { [void]$registrySet.Add($c) }
    $processSet = New-Object 'System.Collections.Generic.HashSet[string]'
    foreach ($part in [Environment]::GetEnvironmentVariable('PATH').Split(';')) {
        if (-not [string]::IsNullOrWhiteSpace($part)) { [void]$processSet.Add((Get-Key -Value (ConvertTo-NormalPath -Raw $part))) }
    }
    foreach ($c in $registrySet) {
        if (-not $processSet.Contains($c)) {
            Add-Finding -Id 'D002' -Severity Warning -Subject $c `
                -Message 'In the registry PATH but not in this process. The session predates an install or edit, so restart the shell or IDE.'
        }
    }
    foreach ($p in $processSet) {
        if (-not $registrySet.Contains($p)) {
            Add-Finding -Id 'D001' -Severity Info -Subject $p `
                -Message 'In this process PATH but not in the registry. A shell profile, virtualenv, IDE or agent runner added it for this session only.'
        }
    }
}

function Get-PathExtensions {
    if (-not $script:IsWindowsHost) { return @('') }
    $raw = $env:PATHEXT
    if ([string]::IsNullOrWhiteSpace($raw)) { $raw = '.COM;.EXE;.BAT;.CMD' }
    return @($raw.Split(';') | Where-Object { $_ } | ForEach-Object { $_.ToLowerInvariant() })
}

function Test-IsExecutableUnix {
    param([IO.FileInfo]$File)
    try { return (([int][IO.File]::GetUnixFileMode($File.FullName) -band 73) -ne 0) } catch { return $true }
}

function Get-CommandCandidates {
    # Builds name -> ordered candidates, one per directory, in the same precedence cmd.exe uses:
    # directory order first, then PATHEXT order inside a directory.
    param($Entries, [string[]]$Names)
    $wanted = $null
    if (-not $AllShadows) {
        $wanted = New-Object 'System.Collections.Generic.HashSet[string]'
        foreach ($n in $Names) { [void]$wanted.Add((Get-Key -Value $n)) }
    }
    $extensions = Get-PathExtensions
    $index = @{}
    $visited = @{}

    foreach ($e in $Entries) {
        $dir = $e.Normalized
        if ([string]::IsNullOrWhiteSpace($dir) -or -not (Test-IsAbsolutePath -Value $dir)) { continue }
        $dirKey = Get-Key -Value $dir
        if ($visited.ContainsKey($dirKey)) { continue }
        $visited[$dirKey] = $true
        if (-not (Test-Path -LiteralPath $dir -PathType Container)) { continue }

        $best = @{}
        $files = Get-ChildItem -LiteralPath $dir -File -Force -ErrorAction SilentlyContinue
        foreach ($f in $files) {
            if ($script:IsWindowsHost) {
                $ext = $f.Extension.ToLowerInvariant()
                $rank = [Array]::IndexOf($extensions, $ext)
                if ($rank -lt 0) { continue }
                $cmd = Get-Key -Value $f.BaseName
            }
            else {
                if (-not (Test-IsExecutableUnix -File $f)) { continue }
                $rank = 0
                $cmd = $f.Name
            }
            if ($null -ne $wanted -and -not $wanted.Contains($cmd)) { continue }
            if (-not $best.ContainsKey($cmd) -or $best[$cmd].Rank -gt $rank) {
                $best[$cmd] = [pscustomobject]@{ Rank = $rank; File = $f }
            }
        }
        foreach ($cmd in $best.Keys) {
            if (-not $index.ContainsKey($cmd)) { $index[$cmd] = New-Object 'System.Collections.Generic.List[object]' }
            $file = $best[$cmd].File
            $isStub = $false
            if ($script:IsWindowsHost) {
                $isStub = (($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) -and
                ($file.FullName -match '\\WindowsApps\\') -and ($file.Length -eq 0)
            }
            $index[$cmd].Add([pscustomobject]@{
                    DirIndex = $e.Index
                    Path     = $file.FullName
                    Stub     = $isStub
                    Sha256   = $null
                    Version  = $null
                })
        }
    }
    return $index
}

function Invoke-VersionProbe {
    # Runs "<tool> --version" (or the tool specific form) with a hard timeout and output cap.
    param([string]$Path, [string]$Name)
    $arguments = '--version'
    if ($script:ProbeArguments.ContainsKey($Name)) { $arguments = $script:ProbeArguments[$Name] }
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $Path
    $psi.Arguments = $arguments
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.RedirectStandardInput = $true
    $psi.CreateNoWindow = $true
    $proc = $null
    try {
        $proc = [System.Diagnostics.Process]::Start($psi)
        $proc.StandardInput.Close()
        $outTask = $proc.StandardOutput.ReadToEndAsync()
        $errTask = $proc.StandardError.ReadToEndAsync()
        if (-not $proc.WaitForExit($VersionTimeoutSeconds * 1000)) {
            try { $proc.Kill() } catch { Write-Verbose "kill failed: $($_.Exception.Message)" }
            return [pscustomobject]@{ Version = $null; Note = 'timeout' }
        }
        [void][Threading.Tasks.Task]::WaitAll(@($outTask, $errTask), 2000)
        $text = ''
        if ($outTask.IsCompleted) { $text += $outTask.Result }
        if ($errTask.IsCompleted) { $text += "`n" + $errTask.Result }
        if ($text.Length -gt 2000) { $text = $text.Substring(0, 2000) }
        if ($text -match '(\d+(?:\.\d+){1,3})') {
            return [pscustomobject]@{ Version = $Matches[1]; Note = '' }
        }
        return [pscustomobject]@{ Version = $null; Note = 'no version in output' }
    }
    catch {
        return [pscustomobject]@{ Version = $null; Note = $_.Exception.Message }
    }
    finally { if ($null -ne $proc) { $proc.Dispose() } }
}

function Get-FileSha256 {
    param([string]$Path)
    try { return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() } catch { return $null }
}

function Resolve-Tools {
    param($Entries, $PathWritable)
    $index = Get-CommandCandidates -Entries $Entries -Names $Tool
    $names = @($Tool | ForEach-Object { Get-Key -Value $_ })
    if ($AllShadows) { $names = @($index.Keys | Sort-Object -Unique) }
    $results = New-Object 'System.Collections.Generic.List[object]'

    foreach ($name in $names) {
        $tracked = ($Tool | ForEach-Object { Get-Key -Value $_ }) -contains $name
        if (-not $index.ContainsKey($name)) {
            if ($tracked) {
                Add-Finding -Id 'S004' -Severity Info -Subject $name -Message 'Tracked tool is not on PATH.'
            }
            continue
        }
        $cands = $index[$name]
        $real = @($cands | Where-Object { -not $_.Stub })
        $winner = $cands[0]

        foreach ($c in $cands) {
            if (-not $c.Stub) { $c.Sha256 = Get-FileSha256 -Path $c.Path }
        }

        if ($winner.Stub -and $real.Count -gt 0) {
            Add-Finding -Id 'S002' -Severity Error -Subject $name `
                -Message 'The winning match is a Microsoft Store alias stub. It opens the Store instead of running a real install further down PATH.' `
                -Evidence ("stub={0} real={1}" -f $winner.Path, $real[0].Path)
        }

        $distinct = @($real | Where-Object { $null -ne $_.Sha256 } | Select-Object -ExpandProperty Sha256 -Unique)
        $allowed = ($AllowShadow | ForEach-Object { Get-Key -Value $_ }) -contains $name
        if ($distinct.Count -gt 1) {
            $sev = 'Warning'
            if ($allowed -or -not $tracked) { $sev = 'Info' }
            $others = ($real | Select-Object -Skip 1 | ForEach-Object { $_.Path }) -join '; '
            Add-Finding -Id 'S001' -Severity $sev -Subject $name `
                -Message ('{0} different binaries share this name. #{1} wins and the rest are shadowed.' -f $distinct.Count, $winner.DirIndex) `
                -Evidence ("winner={0} shadowed={1}" -f $winner.Path, $others)
        }

        if (-not $SkipVersionProbe -and $tracked) {
            foreach ($c in $real) {
                $dirEntry = $Entries | Where-Object { $_.Index -eq $c.DirIndex } | Select-Object -First 1
                $dirKey = Get-Key -Value $dirEntry.Normalized
                if ($PathWritable.ContainsKey($dirKey)) {
                    Add-Finding -Id 'S005' -Severity Info -Subject $name `
                        -Message 'Version probe skipped: the binary lives in a directory untrusted users can write to, so it was not executed.' -Evidence $c.Path
                    continue
                }
                $probe = Invoke-VersionProbe -Path $c.Path -Name $name
                $c.Version = $probe.Version
                if ($null -eq $probe.Version) {
                    Add-Finding -Id 'S006' -Severity Info -Subject $name `
                        -Message ('Version probe gave no version: ' + $probe.Note) -Evidence $c.Path
                }
            }
            $versions = @($real | Where-Object { $null -ne $_.Version } | Select-Object -ExpandProperty Version -Unique)
            if ($versions.Count -gt 1 -and $distinct.Count -gt 1) {
                $sev = 'Warning'
                if ($allowed) { $sev = 'Info' }
                Add-Finding -Id 'S003' -Severity $sev -Subject $name `
                    -Message ('Shadowed copies report different versions: ' + ($versions -join ', ') + '. The first one on PATH is what scripts get.') `
                    -Evidence (($real | ForEach-Object { "{0} => {1}" -f $_.Path, $_.Version }) -join '; ')
            }
        }

        $winnerSha = $null
        $winnerVersion = $null
        if (-not $winner.Stub) { $winnerSha = $winner.Sha256; $winnerVersion = $winner.Version }
        if ($tracked) {
            $results.Add([pscustomobject][ordered]@{
                    name        = $name
                    winner      = $winner.Path
                    stub        = $winner.Stub
                    sha256      = $winnerSha
                    version     = $winnerVersion
                    copies      = $cands.Count
                })
        }
    }
    return $results
}

function Get-WritableDirMap {
    param($Entries)
    $map = @{}
    foreach ($e in $Entries) {
        if (-not (Test-IsAbsolutePath -Value $e.Normalized)) { continue }
        if (-not (Test-Path -LiteralPath $e.Normalized -PathType Container)) { continue }
        if ($null -ne (Get-UntrustedWriter -Dir $e.Normalized)) { $map[(Get-Key -Value $e.Normalized)] = $true }
    }
    return $map
}

function Get-Sha256OfText {
    param([string]$Text)
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
        return ([BitConverter]::ToString($sha.ComputeHash($bytes)) -replace '-', '').ToLowerInvariant()
    }
    finally { $sha.Dispose() }
}

function New-BaselinePayload {
    param($Entries, $Tools)
    $normalised = @($Entries | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Raw) } | ForEach-Object { $_.Normalized })
    return [ordered]@{
        schema     = $script:SchemaVersion
        createdUtc = [DateTime]::UtcNow.ToString('o')
        host       = [ordered]@{
            os         = [Environment]::OSVersion.VersionString
            machine    = [Environment]::MachineName
            powershell = $PSVersionTable.PSVersion.ToString()
        }
        path       = $normalised
        tools      = @($Tools)
    }
}

function Save-Baseline {
    param($Payload)
    if ((Test-Path -LiteralPath $BaselinePath) -and -not $Force) {
        throw [AuditAbort]::new("Baseline already exists at $BaselinePath. Use -Force to replace it.", 2)
    }
    # The payload is stored as a JSON string so the hash covers the exact bytes that were
    # written, independent of how a later PowerShell version re-serialises objects.
    $payloadJson = $Payload | ConvertTo-Json -Depth 8 -Compress
    $envelope = [ordered]@{
        schema      = $script:SchemaVersion
        payloadSha256 = (Get-Sha256OfText -Text $payloadJson)
        payload     = $payloadJson
    }
    $tmp = '{0}.{1}.tmp' -f $BaselinePath, [Guid]::NewGuid().ToString('N')
    try {
        $envelope | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $tmp -Encoding UTF8
        Move-Item -LiteralPath $tmp -Destination $BaselinePath -Force
    }
    finally { if (Test-Path -LiteralPath $tmp) { Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue } }
}

function Read-Baseline {
    if (-not (Test-Path -LiteralPath $BaselinePath -PathType Leaf)) {
        throw [AuditAbort]::new("Baseline not found at $BaselinePath. Run with -Mode Snapshot first.", 3)
    }
    try { $envelope = Get-Content -LiteralPath $BaselinePath -Raw -Encoding UTF8 | ConvertFrom-Json }
    catch { throw [AuditAbort]::new("Baseline is not valid JSON: $($_.Exception.Message)", 3) }
    foreach ($field in 'schema', 'payloadSha256', 'payload') {
        if (-not ($envelope.PSObject.Properties.Name -contains $field)) {
            throw [AuditAbort]::new("Baseline is missing the '$field' field.", 3)
        }
    }
    if ([int]$envelope.schema -ne $script:SchemaVersion) {
        throw [AuditAbort]::new("Baseline schema $($envelope.schema) is not supported, expected $script:SchemaVersion.", 3)
    }
    if ((Get-Sha256OfText -Text ([string]$envelope.payload)) -ne ([string]$envelope.payloadSha256).ToLowerInvariant()) {
        throw [AuditAbort]::new('Baseline hash mismatch: the file was edited or truncated after it was written.', 3)
    }
    return ($envelope.payload | ConvertFrom-Json)
}

function Compare-Baseline {
    param($Baseline, $Entries, $Tools)
    $current = @($Entries | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Raw) } | ForEach-Object { $_.Normalized })
    $before = @($Baseline.path)
    $curKeys = @($current | ForEach-Object { Get-Key -Value $_ })
    $befKeys = @($before | ForEach-Object { Get-Key -Value $_ })

    for ($i = 0; $i -lt $before.Count; $i++) {
        if ($curKeys -notcontains $befKeys[$i]) {
            Add-Finding -Id 'B001' -Severity Warning -Subject $before[$i] -Message 'PATH entry present in the baseline is gone.'
        }
    }
    for ($i = 0; $i -lt $current.Count; $i++) {
        if ($befKeys -notcontains $curKeys[$i]) {
            Add-Finding -Id 'B002' -Severity Warning -Subject $current[$i] `
                -Message ('New PATH entry not in the baseline, at position {0}.' -f $i)
        }
    }
    $commonBefore = @($befKeys | Where-Object { $curKeys -contains $_ })
    $commonNow = @($curKeys | Where-Object { $befKeys -contains $_ })
    if (($commonBefore -join "`n") -ne ($commonNow -join "`n")) {
        Add-Finding -Id 'B003' -Severity Warning -Subject 'PATH order' `
            -Message 'Entries kept from the baseline now appear in a different order. Precedence changed even though nothing was added or removed.'
    }

    $now = @{}
    foreach ($t in $Tools) { $now[$t.name] = $t }
    foreach ($old in @($Baseline.tools)) {
        if (-not $now.ContainsKey($old.name)) {
            Add-Finding -Id 'B007' -Severity Error -Subject $old.name -Message 'Tool was resolvable at baseline time and is no longer on PATH.' -Evidence ([string]$old.winner)
            continue
        }
        $new = $now[$old.name]
        if ((Get-Key -Value ([string]$old.winner)) -ne (Get-Key -Value ([string]$new.winner))) {
            Add-Finding -Id 'B004' -Severity Error -Subject $old.name `
                -Message 'The binary that wins resolution changed.' -Evidence ("was={0} now={1}" -f $old.winner, $new.winner)
            continue
        }
        $oldSha = [string]$old.sha256
        $newSha = [string]$new.sha256
        $oldVer = [string]$old.version
        $newVer = [string]$new.version
        if ($oldSha -and $newSha -and $oldSha -ne $newSha) {
            if ($oldVer -and $oldVer -eq $newVer) {
                Add-Finding -Id 'B005' -Severity Error -Subject $old.name `
                    -Message 'Same path and same reported version but different file contents. Treat as tampering until proven otherwise.' -Evidence $new.winner
            }
            else {
                Add-Finding -Id 'B005' -Severity Warning -Subject $old.name `
                    -Message 'File contents changed at the same path.' -Evidence ("version {0} -> {1}" -f $oldVer, $newVer)
            }
        }
        elseif ($oldVer -and $newVer -and $oldVer -ne $newVer) {
            Add-Finding -Id 'B006' -Severity Warning -Subject $old.name -Message 'Reported version changed.' -Evidence ("{0} -> {1}" -f $oldVer, $newVer)
        }
    }
}

function Write-Report {
    param($Tools)
    $sorted = @($script:Findings | Sort-Object @{ Expression = { -$script:SeverityRank[$_.severity] } }, id, subject)
    if ($Format -eq 'Json') {
        $doc = [ordered]@{
            schema   = $script:SchemaVersion
            mode     = $Mode
            findings = $sorted
            tools    = @($Tools)
            counts   = [ordered]@{
                error   = @($sorted | Where-Object { $_.severity -eq 'Error' }).Count
                warning = @($sorted | Where-Object { $_.severity -eq 'Warning' }).Count
                info    = @($sorted | Where-Object { $_.severity -eq 'Info' }).Count
            }
        }
        $text = $doc | ConvertTo-Json -Depth 8
    }
    else {
        $sb = New-Object System.Text.StringBuilder
        foreach ($f in $sorted) {
            [void]$sb.AppendLine(('[{0}] {1} {2}' -f $f.severity.ToUpperInvariant(), $f.id, $f.subject))
            [void]$sb.AppendLine('    ' + $f.message)
            if ($f.evidence) { [void]$sb.AppendLine('    ' + $f.evidence) }
        }
        $e = @($sorted | Where-Object { $_.severity -eq 'Error' }).Count
        $w = @($sorted | Where-Object { $_.severity -eq 'Warning' }).Count
        $i = @($sorted | Where-Object { $_.severity -eq 'Info' }).Count
        [void]$sb.AppendLine(('{0} error, {1} warning, {2} info' -f $e, $w, $i))
        $text = $sb.ToString()
    }
    if ($OutputPath) { Set-Content -LiteralPath $OutputPath -Value $text -Encoding UTF8 }
    else { Write-Output $text }
}

function Get-ExitCode {
    if ($FailOn -eq 'None') { return 0 }
    $threshold = $script:SeverityRank[$FailOn]
    foreach ($f in $script:Findings) {
        if ($script:SeverityRank[$f.severity] -ge $threshold) { return 1 }
    }
    return 0
}

function Invoke-Main {
    $pathInfo = Get-PathEntries
    Test-PathHygiene -PathInfo $pathInfo
    Test-RegistryPaths
    $writable = Get-WritableDirMap -Entries $pathInfo.Entries
    $tools = Resolve-Tools -Entries $pathInfo.Entries -PathWritable $writable

    switch ($Mode) {
        'Snapshot' {
            Save-Baseline -Payload (New-BaselinePayload -Entries $pathInfo.Entries -Tools $tools)
            Write-Verbose "Baseline written to $BaselinePath"
        }
        'Verify' {
            Compare-Baseline -Baseline (Read-Baseline) -Entries $pathInfo.Entries -Tools $tools
        }
    }
    Write-Report -Tools $tools
    return (Get-ExitCode)
}

try {
    $code = Invoke-Main
}
catch [AuditAbort] {
    [Console]::Error.WriteLine('PathShadowDriftAuditor: ' + $_.Exception.Message)
    $code = $_.Exception.Code
}
catch {
    [Console]::Error.WriteLine('PathShadowDriftAuditor: unexpected failure: ' + $_.Exception.Message)
    $code = 2
}
exit $code
