# PATH Shadow Drift Auditor

A PowerShell script that audits the PATH your process really has, finds the tools that are being shadowed by an earlier directory, and tells you when the whole picture changed since a baseline you trust. It runs on Windows PowerShell 5.1 and on PowerShell 7 for Windows, Linux and macOS.

**Language:** PowerShell | **Lines:** 717 | **Added:** 2026-10-20

## What this solves

"It works on my machine" very often means "my PATH resolves `python` to a different binary than yours". Nobody looks at PATH until something strange happens, and when it does happen the symptoms are confusing: a build picks up Python 3.9 from an old installer while you installed 3.12, `git` comes from a bundled copy inside an IDE, `node` resolves to a version manager shim in one terminal and a system install in another.

Windows adds its own traps. The Microsoft Store ships zero byte alias stubs for `python.exe` and `python3.exe` in the WindowsApps folder. If that folder sits before your real Python install, running `python` opens the Store instead of running Python. Machine and user PATH values in the registry can be stored as `REG_SZ` instead of `REG_EXPAND_SZ`, which means a `%JAVA_HOME%\bin` entry is never expanded by Windows. A shell opened before an install keeps a stale PATH until you restart it.

Then there is the security side, which matters more now that AI coding agents and MCP servers launch subprocesses by bare name. If a directory early on PATH is writable by ordinary users, anything that can write there can replace `git` or `node` and every agent run after that executes it. An empty segment in a Unix PATH means the current directory, so cloning a repository and running a command inside it can run a file from that repository.

This script checks all of that in one pass, prints findings with stable rule IDs, and returns an exit code your CI or login script can act on.

## Why I built it

I work on agent tooling and a lot of the failures I debugged were not in the agent. They were in the environment the agent was launched from. One runner had `python` resolve to a Store stub, another had two copies of `node` with different majors and a third had a user writable folder sitting in front of System32. Each time I fixed it by hand and each time it came back after the next installer ran.

Existing answers were thin. `where.exe` and `which -a` show you the order but make no judgement. Most "PATH cleaner" scripts only remove duplicates and missing folders, and some of them rewrite the registry, which is exactly the kind of change that breaks `REG_EXPAND_SZ`. I wanted something read only that explains what is wrong, can be diffed against a known good state, and is safe to run on a shared build machine.

## When to use it

Use it at the start of a CI job on a Windows or Linux runner, so a drifted image fails fast with a clear message instead of failing later inside a build step.

Use it when you onboard a developer machine or a new agent sandbox and want a record of which `python`, `node`, `git` and `dotnet` it resolves to.

Use `-Mode Snapshot` once on a machine you trust and `-Mode Verify` on a schedule or at the start of every agent session. If an update swaps a binary behind the same path and the same reported version, you get an error, not a silent pass.

Use plain audit mode when you are just asking why a tool behaves differently in two terminals.

Do not use it as a general malware scanner. It looks at the directories on PATH and at the tools you name, nothing else.

## How it works

The script is one file with small functions and a `Invoke-Main` that wires them together.

**Reading PATH.** `Get-PathEntries` takes the process PATH and splits it on the platform separator without dropping empty segments, because an empty segment is itself a finding. `ConvertTo-NormalPath` strips quotes, expands environment variables, unifies separators and removes trailing separators except on a root. It never touches the disk, so a dead network share cannot hang normalisation. `Get-Key` lower cases paths on Windows only, so comparisons match how the file system behaves on each platform.

**Hygiene rules.** `Test-PathHygiene` walks the entries in order and emits findings:

- `P001` directory does not exist
- `P002` duplicate of an earlier entry
- `P003` relative entry, which is an error
- `P004` empty segment, a warning on Windows and an error on Unix
- `P005` unexpanded variable or tilde
- `P006` writable by untrusted users
- `P007` quote characters in the entry
- `P008` entry is a file
- `P009` PATH longer than `LegacyPathWarnLength`, which is 2047 characters

For `P006` the function `Get-UntrustedWriter` reads the ACL on Windows and checks Everyone, Authenticated Users, Users and Interactive against `WriteMask`, which covers create files, append, delete, change permissions and take ownership. On Unix it reads the mode through `GetUnixFileMode` and flags world write and group write, ignoring sticky directories. `Get-TrustAnchorIndex` finds where `System32` (or `/usr/bin`) sits. A writable directory before that anchor is an error, because a planted file can replace a system command. After the anchor it is a warning.

**Registry checks.** On Windows, `Get-RegistryPath` reads the machine and user PATH with `DoNotExpandEnvironmentNames` and also reads the value kind. `Test-RegistryPaths` raises `R001` when a value contains a percent variable but is stored as `REG_SZ`. It then compares the registry set with the process set. `D002` means the registry has an entry this process does not, so the session is stale. `D001` means the process has an entry the registry does not, which usually comes from a shell profile, a virtualenv or an IDE.

**Resolving tools.** `Get-CommandCandidates` builds an index of command name to ordered candidates, one per directory. It follows the same precedence as cmd.exe: directory order first, then `PATHEXT` order inside a directory. On Unix it requires an execute bit. By default it only indexes the names in `-Tool`, which keeps it fast. `-AllShadows` indexes everything. A Store alias is recognised by being a reparse point, zero bytes, inside a `WindowsApps` folder.

`Resolve-Tools` hashes every real candidate with `Get-FileSha256` and then reports:

- `S001` several different binaries share a name, with the winner and the shadowed paths
- `S002` the winner is a Store alias stub while a real install exists further down
- `S003` shadowed copies report different versions
- `S004` a tracked tool is not on PATH at all
- `S005` a version probe was skipped because the binary lives in a user writable directory
- `S006` the probe ran but produced no version

Two identical copies in two directories are not reported as a conflict, because the hashes match. Names you pass in `-AllowShadow` are downgraded to Info, so intentional shims from pyenv or similar tools do not fail your build.

**Version probes.** `Invoke-VersionProbe` starts the binary with `--version` (the `ProbeArguments` table switches `go` to `version` and `java` to `-version`). Standard input is closed straight away, output is read asynchronously, the wait is capped by `-VersionTimeoutSeconds` and the process is killed on timeout. Python 2 prints its version on stderr, so both streams are read. Binaries in writable directories are never executed.

**Baselines.** `New-BaselinePayload` records the normalised PATH order and, for each tracked tool, the winning path, SHA-256 and version. `Save-Baseline` stores that payload as a JSON string inside an envelope next to its own SHA-256, then writes through a temp file and `Move-Item`, so a crash cannot leave half a baseline. `Read-Baseline` rejects missing files, bad JSON, a different schema and any hash mismatch with exit code 3. `Compare-Baseline` then emits:

- `B001` entry removed
- `B002` entry added
- `B003` order of kept entries changed
- `B004` the winning binary for a tool changed, an error
- `B005` file contents changed at the same path, and an error when the reported version did not change
- `B006` version changed
- `B007` tool disappeared, an error

**Exit codes.** `Get-ExitCode` returns 1 when any finding meets `-FailOn`. Aborts use `AuditAbort`: 2 for runtime or usage errors and 3 for baseline problems.

## Usage

Audit the current session and print a text report:

```powershell
.\PathShadowDriftAuditor.ps1
```

Audit specific tools, allow a known shim and write JSON for CI:

```powershell
.\PathShadowDriftAuditor.ps1 -Tool python,node,git -AllowShadow python -Format Json -OutputPath .\path-report.json -FailOn Warning
```

Take a baseline on a machine you trust, then verify later:

```powershell
.\PathShadowDriftAuditor.ps1 -Mode Snapshot -BaselinePath .\path-baseline.json
.\PathShadowDriftAuditor.ps1 -Mode Verify -BaselinePath .\path-baseline.json -FailOn Warning
```

Replace an existing baseline after an intended upgrade:

```powershell
.\PathShadowDriftAuditor.ps1 -Mode Snapshot -BaselinePath .\path-baseline.json -Force
```

Skip executing any binary and ignore some directories:

```powershell
.\PathShadowDriftAuditor.ps1 -SkipVersionProbe -IgnoreDir 'C:\Tools\Scratch*'
```

All parameters: `-Mode`, `-BaselinePath`, `-Tool`, `-AllowShadow`, `-IgnoreDir`, `-Format`, `-OutputPath`, `-FailOn`, `-VersionTimeoutSeconds`, `-SkipVersionProbe`, `-AllShadows` and `-Force`.

In a CI step, check the exit code:

```powershell
pwsh -File .\PathShadowDriftAuditor.ps1 -Mode Verify -FailOn Error
if ($LASTEXITCODE -ne 0) { throw "PATH drift, see report above" }
```

## Notes

- It is read only. It never edits the registry, PATH or any file except the baseline and the optional report.
- The version probe executes the tool. That is why it skips binaries in untrusted writable directories and why `-SkipVersionProbe` exists. If you are auditing a machine you do not trust at all, use that switch.
- The ACL check looks at four well known untrusted groups. It does not walk nested group membership or deny entries, so a custom group with write access will not be flagged.
- `D001` and `D002` only run on Windows, since Linux and macOS have no registry equivalent that every shell shares.
- On Unix the tool needs .NET 7 or newer, which ships with PowerShell 7.3 and later, for `GetUnixFileMode`. On older builds the writable directory and execute bit checks quietly skip, and every file in a PATH directory counts as a candidate.
- A baseline records absolute paths and hashes, so it is per machine and per image. Treat it like a lock file for one environment, not something to share between different laptops.
- A baseline hash catches accidental edits and truncation. It is not a signature, so anyone who can write the file can recompute it. Keep baselines somewhere only your pipeline can write.
- Hashing large binaries on a slow disk takes a moment. The default tool list is short on purpose.
- I could not run this on a real PowerShell host while writing it in a Linux sandbox, so run it once on your own machines and send me anything that looks wrong.
