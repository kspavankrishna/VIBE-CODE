# Agent Sandbox Fence

You gave an AI coding agent a real shell on your Windows laptop, and now it can install packages, run scripts and open network connections just like you can. This is the wrapper that puts a real ceiling on what it can do: a kernel enforced memory and process limit, a polled CPU and wall clock budget, and a loopback proxy that only lets the agent talk to hosts you named.

**Language:** PowerShell | **Lines:** 946 | **Added:** 2026-09-24

## What this solves

Everyone running an AI coding agent locally has hit the same moment: you told it to fix a failing test, and forty minutes later your fans are screaming because it got stuck in a retry loop, or it quietly ran `npm install` against a typosquatted package name, or it decided to "double check the environment" by curling a URL you never expected. On Linux you would reach for a container, a network namespace, or a ptrace based syscall filter. On Windows, the honest answer for most developers is nothing. Docker Desktop is often blocked by corporate policy, WSL2 containers do not give you a native Win32 process tree, and most "run this safely" advice for Windows either assumes a VM you do not want to spin up for every coding session or just says "be careful."

Meanwhile the actual risk on Windows is concrete. An agent process with your user's network access can reach the same places your browser and your cloud CLI can reach, including link local addresses. If that agent is running inside a container on AWS, Azure or GCP, or even a VM with instance metadata enabled, an SSRF style call to `169.254.169.254` is a known and current path to stealing short lived cloud credentials. A tool that "sandboxes" the agent by loosely capping CPU but exempts localhost and link local traffic from its proxy has not actually closed that path, it has just made it feel safer.

This is the Windows native answer built from parts that ship with every current Windows install: Job Objects for hard resource limits, and a raw `CreateProcess` call instead of `Start-Process` so the process can be assigned to that job before it gets a chance to run a single instruction. No third party runtime, no admin rights required for the core sandbox, nothing to download.

## Why I built it

I kept seeing the same gap: every "agent sandbox" writeup assumed Linux, and every Windows answer for constraining a process assumed you already had a container platform installed. Windows has had Job Objects since Windows 2000 and most engineers have never touched the API directly because `Process.Start` in .NET does not expose it. Once you go one level down to the actual Win32 calls, you get real kernel enforced limits: `JOB_OBJECT_LIMIT_PROCESS_MEMORY` kills a process the instant it exceeds its memory ceiling, no polling involved, and `JOB_OBJECT_LIMIT_ACTIVE_PROCESS` makes the OS itself refuse to create a process past your count limit. That is a much stronger guarantee than anything you can build by watching `Get-Process` in a loop.

The other half, the egress proxy, came from a specific worry: most Windows corporate proxies and most homemade "block this app's network" scripts exempt loopback and private ranges by default, because that is the sane default for a normal application. It is exactly the wrong default for an AI agent, which is the one kind of process where "reach out to a local looking address" is a plausible attack outcome rather than normal behavior. So this fence proxies everything, including 127.0.0.1 and the cloud metadata range, and if the agent legitimately needs to reach a local service, you add it to the allowlist like anything else. Nothing gets a free pass.

## When to use it

- Running Claude Code, an MCP server, or any other AI coding agent on a Windows dev machine where Docker Desktop is unavailable or against policy
- A CI runner on Windows that executes generated or third party code and needs a hard memory and process ceiling, not just a timeout
- Giving an agent shell access during a demo or a hackathon where you do not fully trust what it will decide to run
- Anywhere you want an auditable JSONL record of exactly which hosts an agent process tried to reach and whether each one was allowed

## How it works

The script is a single file, `AgentSandboxFence.ps1`. It opens with an embedded C# source string compiled once per run through `Add-Type`, because Job Objects and raw `CreateProcess` are not exposed by any built in PowerShell cmdlet. To dodge the split between Windows PowerShell 5.1 (.NET Framework, where socket types live in `System.dll`) and PowerShell 7 (.NET, where they live in `System.Net.Sockets.dll`), the script does not guess assembly names. It walks `[System.AppDomain]::CurrentDomain.GetAssemblies()`, takes every already loaded, non dynamic assembly's file path, and hands that whole list to `Add-Type -ReferencedAssemblies`. That one line is what makes the same source compile cleanly on both PowerShell editions.

Inside the compiled types, `JobFence` wraps `CreateJobObject` and `SetInformationJobObject`, setting `JOB_OBJECT_LIMIT_PROCESS_MEMORY`, `JOB_OBJECT_LIMIT_ACTIVE_PROCESS` and `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` on the job's `JOBOBJECT_EXTENDED_LIMIT_INFORMATION`. That last flag is the quiet hero of the whole design: it means that if the wrapper's own PowerShell process is killed outright, with `taskkill /F`, a closed terminal, or a crash, Windows itself closes the job handle as part of process cleanup, and the kill on close flag takes it from there and terminates every process still inside the job. You do not get orphaned `node.exe` processes chewing CPU after you gave up on a stuck agent.

The method that matters most is `JobFence.StartSuspendedInJob`. It calls `CreateProcess` directly with the `CREATE_SUSPENDED` flag, gets back a `PROCESS_INFORMATION` with the new process still frozen at its entry point, calls `AssignProcessToJobObject` while it is still frozen, and only then calls `ResumeThread`. A naive `Process.Start` followed by `AssignProcessToJobObject` has a real race: a fast launching script host can spawn its own child before your code gets around to the assignment call, and that grandchild is never inside the job. Freezing the process first closes that gap.

CPU time and wall clock are handled differently, because turning `JOB_OBJECT_LIMIT_JOB_TIME` into an instant kill needs an I/O completion port wired up to the job, which is a lot of extra native plumbing for a marginal gain. Instead the main loop calls `WaitForSingleObject` on the process handle with a `PollIntervalMs` timeout (500ms by default), and on each wakeup calls `JobFence.TryGetAccounting`, which reads `JOBOBJECT_BASIC_ACCOUNTING_INFORMATION.TotalUserTime` and converts the 100 nanosecond ticks straight into a `TimeSpan`. Once that crosses `MaxCpuSeconds`, or the stopwatch crosses `TimeoutSeconds`, the script calls `JobFence.Terminate`, which is `TerminateJobObject` under the hood, and every process in the job dies together.

The network side is `EgressGateway`, a small forward proxy bound to `127.0.0.1` on `ProxyPort`. The script sets `HTTP_PROXY`, `HTTPS_PROXY` and their lowercase twins to point at it, and deliberately sets `NO_PROXY` to an empty string, so nothing gets to skip the check. For an HTTPS `CONNECT` request the gateway reads only the plaintext request line, checks the hostname against the allowlist inside `IsAllowed`, either refuses with `403` or opens the real upstream socket and splices raw bytes both ways. It never touches the TLS handshake itself, so it cannot see or tamper with anything inside the encrypted session, it only decides whether the tunnel opens at all. Every decision, allow, deny, or a dry run "would have denied", gets written as one JSON line to `LogPath`, and `CommandLineBuilder.Build` handles the actual Windows argv quoting so arguments with spaces or quotes reach the child intact.

## Usage

First write an allowlist, one hostname per line, blank lines and `#` comments ignored, `*.example.com` matching subdomains but not the bare domain unless you list both:

```
api.anthropic.com
registry.npmjs.org
github.com
*.githubusercontent.com
```

Then run the fence in front of the agent's real executable:

```powershell
.\AgentSandboxFence.ps1 -AllowListPath .\allow.txt -MaxMemoryMB 2048 -MaxCpuSeconds 900 -MaxProcesses 64 -TimeoutSeconds 3600 claude
```

The child command must name a real `.exe`, because raw `CreateProcess` does not consult file associations the way double clicking a file does. To sandbox a `.cmd`, `.bat` or `.ps1` entry point, invoke its interpreter explicitly:

```powershell
.\AgentSandboxFence.ps1 -AllowListPath .\allow.txt cmd.exe /c agent.cmd
.\AgentSandboxFence.ps1 -AllowListPath .\allow.txt pwsh.exe -File agent.ps1
```

Add `-DryRun` on a first pass to see what the agent actually tries to reach without blocking anything, then tighten the allowlist from the JSONL log before removing the switch. Add `-FailOnDeniedConnections` in a CI job so a denied connection fails the build even if the child process itself exited zero. Add `-FirewallEnforce` from an elevated session to also block the resolved executable's direct outbound traffic at the Windows Firewall, which closes the gap left by any tool that ignores `HTTP_PROXY` entirely, such as raw socket code or `ssh`; the rule is named `AgentSandboxFence-<pid>` and removed in the script's `finally` block. On the exit code: a normal run returns the child's own exit code, a breach of the memory, process, CPU or wall clock budget returns `124`, and a denied connection with `-FailOnDeniedConnections` set returns `3`.

## Notes

- The memory and process count limits are enforced by the Windows kernel the instant they are crossed. The CPU and wall clock limits are polled at `PollIntervalMs`, so a breach is caught within one poll interval, not instantly.
- The proxy only constrains clients that honor `HTTP_PROXY` and `HTTPS_PROXY`. Most language runtimes, `curl`, `npm`, `pip` and `git` do. Anything that opens raw sockets or ignores proxy environment variables bypasses it, which is exactly what `-FirewallEnforce` is for.
- If your host was forcibly killed mid run while `-FirewallEnforce` was active and the `finally` block never got to run, sweep up any leftover rule with `Get-NetFirewallRule -DisplayName 'AgentSandboxFence-*' | Remove-NetFirewallRule`.
- `NO_PROXY` is intentionally left empty. If the agent needs a local service, such as an Ollama server on `127.0.0.1`, add `127.0.0.1` or `localhost` to the allowlist rather than exempting it from the proxy.
- For plain HTTP, the gateway forwards the request line exactly as the client sent it, including the absolute form URI that `HTTP_PROXY` clients typically send. That is legal per the HTTP specification, but a small minority of very old origin servers reject it. HTTPS traffic through `CONNECT` is unaffected since the tunnel is transparent.
- Tested against Windows PowerShell 5.1 and PowerShell 7 on Windows 11. If `Add-Type` cannot resolve an assembly on your build, it is almost always because the `-ReferencedAssemblies` list built from loaded assemblies is missing something unusual your profile loaded; running the script fresh, without other modules preloaded, avoids that.
