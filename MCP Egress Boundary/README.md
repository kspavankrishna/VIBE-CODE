# MCP Egress Boundary

An MCP server needs one token, one repository checkout and one API destination. It usually gets your entire home directory, every credential in your environment and unrestricted outbound internet. This is a NixOS module that makes the narrow configuration the easy one.

**Language:** Nix | **Lines:** 785 | **Added:** 2026-05-27

## What this solves

Model Context Protocol servers and agent side tool workers are ordinary processes. You launch them from a config file, they inherit the shell that started them, and nothing in the usual setup says which files they may read or which hosts they may reach. On a developer workstation that means a repository indexer started to scan one project can walk `/home`, read your SSH keys, read every other checkout on the box and POST whatever it finds to any address that resolves. Nobody notices, because nothing failed.

The failure mode is not theoretical and it is not always malicious. A prompt injection buried in a README the agent was told to summarise, a compromised transitive npm dependency inside the server, a tool that decides to "helpfully" upload a diff for context: all three produce the same event, which is credential material or source code leaving the machine over a connection nobody reviewed. When it happens the blast radius is whatever the process could see, and by default the process could see everything the user could see. Incident response then becomes archaeology across shell history and journal logs, because there was never a declared boundary to compare the behaviour against.

The second failure is resource, not security. Indexers and research workers are bursty. One that starts a full reindex during a build will eat memory until the kernel picks a victim, and on a machine that is also running local inference the victim is frequently the thing you actually care about. Swap thrash on a shared build host takes latency from milliseconds to seconds for everybody on it.

This module generates one hardened systemd service per declared tool server. Credentials arrive through systemd's credential mechanism instead of environment variables or the Nix store, the filesystem is denied by default with explicit read only paths added back, outbound IP traffic is denied by default with a numeric allowlist added back, and memory, CPU, task count, file descriptors and optional IO weight are all capped. The point is that a reviewer reading the Nix file can state exactly which code, files, secrets, destinations and machine resources a tool is permitted to use.

## Why I built it

systemd already has every primitive needed here: `LoadCredential`, `DynamicUser`, `ProtectSystem=strict`, `IPAddressAllow`, cgroup limits. The problem is that writing all of that correctly per unit is tedious enough that people skip it, and the failure is silent when they do. The MCP ecosystem shipped a launcher story built around `command`, `args` and `env` in a JSON file, which puts secrets in process environments and gives every server the same ambient authority.

There was no reusable NixOS module for this. So this file encodes the safe defaults as the defaults, adds assertions that refuse to build an incoherent policy, and prints warnings when a configuration widens the boundary. Loosening it is possible, it just has to be typed out and it shows up in the diff.

## When to use it

- A local MCP server holding a GitHub or Jira token that should only ever reach that one API and one checkout.
- A repository indexer on a shared build host that keeps starving the compiler for IO and memory.
- A research or scraping worker that must run continuously and must never touch `/home`.
- An internal developer assistant on a controlled workstation fleet, where the same policy has to be reviewed and reproduced across machines.
- Any agent tool where you want `systemd-analyze security` to give an honest score you can show a security reviewer.
- Third party MCP servers you did not write and have not audited.

## How it works

The module exposes `services.mcpEgressBoundary` with a `servers` attribute set typed by `serverType`, a `types.submodule` that carries the whole policy surface: `command`, `arguments`, `transport`, `credentials`, `identity`, `files`, `network`, `resources`, `lifecycle`, `logging` and `sandbox`. Enabled entries are selected by `filterAttrs` into `enabledServers`, then `mapAttrs'` plus `nameValuePair` turns each one into a systemd unit named by `serviceName`, which is `unitPrefix` (default `mcp-boundary-`) plus the attribute name. A disabled entry generates nothing, which makes it useful for sharing defaults across hosts.

`serviceFor` builds the unit. Every service gets `Type=exec`, `DynamicUser` on by default, `UMask=0077`, an empty `CapabilityBoundingSet` and `AmbientCapabilities`, `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectProc=invisible` with `ProcSubset=pid`, `PrivateTmp`, `RemoveIPC`, `LockPersonality`, `RestrictRealtime`, `RestrictSUIDSGID` and `SystemCallArchitectures=native`. Syscall filtering is a deny list built by prefixing each entry of `sandbox.deniedSystemCallGroups` with `~`, defaulting to `@mount`, `@obsolete`, `@privileged`, `@raw-io`, `@reboot` and `@swap`, with `SystemCallErrorNumber=EPERM` so a blocked call returns an error instead of a SIGSYS kill. A deny list rather than an allow list is a deliberate choice: it keeps Node, Python and JVM runtimes working while still removing host administration. `MemoryDenyWriteExecute` is available but defaults to false for exactly that reason, since JIT runtimes need writable executable mappings.

Egress is the interesting part. `network.policy` is an enum of `none`, `loopback`, `allowlist` and `unrestricted`. `PrivateNetwork` is set when the policy is `none`, which puts the process in its own network namespace, and `RestrictAddressFamilies` collapses to `AF_UNIX` only in that case. For every policy other than `unrestricted`, `hasIPAddressFilter` adds `IPAddressDeny=any`, and `allowedIPAddresses` then computes the `IPAddressAllow` set: empty for `none`, `localhost` for `loopback`, and `localhost` plus `network.allowedCIDRs` for `allowlist`. These are cgroup level BPF filters enforced by the kernel, not iptables rules the process can route around. CIDRs must be numeric. Domain names are refused on purpose, because a DNS answer can change after the review that approved it and is therefore not a boundary.

Secrets never enter the Nix store. `credentials` and `encryptedCredentials` are typed `attrsOf absoluteRuntimePath`, a `types.strMatching "^/.*"`, so the values stay strings rather than Nix paths and never get copied into a world readable store. They become `LoadCredential` and `LoadCredentialEncrypted` entries pointing at agenix, sops-nix or systemd encrypted credential files under `/run`. The generated `script` then runs `set -eu`, emits one `export VAR="$CREDENTIALS_DIRECTORY/NAME"` line per `credentialEnvironment` mapping via `credentialExports`, and `exec`s the command line built with `escapeShellArgs`. The exported value is a path, never the secret. Arguments are escaped individually so nothing goes through a shell parse.

Filesystem access is subtractive then additive. `files.inaccessiblePaths` defaults to `/root`, `/home` and `/run/user`, `ProtectHome` is true unless `allowReadOnlyHome` flips it to `read-only`, and real work happens through `StateDirectory`, `CacheDirectory` and `RuntimeDirectory` at mode `0700`, or through explicit `readOnlyPaths` and `writablePaths`.

The last layer is `assertionsFor` and `warningsFor`, and this is what stops policies that look fine and are not. Assertions reject a non absolute `command`, a server name that is not systemd safe, `identity.user` set while `dynamicUser` is true or missing while it is false, `allowlist` with no CIDRs, CIDRs set under any other policy, a credential name present in both `credentials` and `encryptedCredentials`, a `credentialEnvironment` value naming a credential that does not exist, a path in both `readOnlyPaths` and `writablePaths`, managed directory names containing slashes, non positive `tasksMax` or `openFilesMax` and an `ioWeight` outside 1 to 10000. It also refuses `transport = "http"` with `network.policy = "none"`, since that unit could never be reached. Warnings fire on `unrestricted` egress, on an HTTP transport using an allowlist, and on any key in `environment` matching `.*(TOKEN|SECRET|PASSWORD|API_KEY|PRIVATE_KEY|ACCESS_KEY).*`, which catches the most common mistake of pasting a token into a plain env var.

## Usage

```nix
# configuration.nix
{ pkgs, ... }:
{
  imports = [ ./McpEgressBoundary.nix ];

  services.mcpEgressBoundary = {
    enable = true;
    unitPrefix = "mcp-boundary-";

    servers.github-tools = {
      enable = true;
      command = "${pkgs.my-mcp-github}/bin/mcp-github";
      arguments = [ "--transport" "stdio" ];
      transport = "stdio";
      runtimePackages = [ pkgs.git pkgs.cacert ];

      environment.LOG_LEVEL = "info";

      # value is a runtime path, not a Nix path, so nothing lands in the store
      credentials.GITHUB_TOKEN = "/run/agenix/mcp-github-token";
      credentialEnvironment.GITHUB_TOKEN_FILE = "GITHUB_TOKEN";

      files.readOnlyPaths = [ "/srv/repositories/product-api" ];
      files.inaccessiblePaths = [ "/root" "/home" "/run/user" ];

      network.policy = "allowlist";
      network.allowedCIDRs = [ "140.82.112.0/20" ];

      resources = {
        memoryMax = "2G";
        memorySwapMax = "0";
        cpuQuota = "150%";
        tasksMax = 256;
        openFilesMax = 4096;
        ioWeight = 50;
      };

      lifecycle.runtimeMaxSec = "6h";
    };
  };
}
```

```bash
sudo nixos-rebuild switch
systemctl status mcp-boundary-github-tools.service
systemd-analyze security mcp-boundary-github-tools.service
journalctl -u mcp-boundary-github-tools.service -f
```

## Notes

- NixOS only. It is a NixOS module built on systemd unit options, cgroup v2 and `LoadCredential`, so it needs a reasonably recent systemd and does not apply to macOS or non systemd Linux.
- It does not manage the secrets themselves. You supply an existing runtime path under `/run` from agenix, sops-nix or `systemd-creds`. The module refers to that path, it does not create or rotate it.
- `network.allowedCIDRs` takes numeric ranges only. Allowlisting an API behind a CDN means tracking published IP ranges, and a wide range is a wide hole. Domain based egress control is out of scope by design.
- `IPAddressAllow` and `IPAddressDeny` are IP level. They cannot distinguish two hosts sharing an address, and they say nothing about what is inside the connection.
- The syscall filter is a deny list, not an allow list. It is chosen for compatibility with common runtimes and is weaker than a hand written allow list would be.
- Defaults are deliberately tight and will break some servers. `MemoryDenyWriteExecute` is off for JIT runtimes, but `restrictNamespaces` is on and will stop containerised or browser driving tools until you turn it off in a reviewed change.
- Nothing here validates that an HTTP transport server actually binds to loopback. The module warns and the assertion only covers the `none` policy case. Binding and authentication remain the application's job.
- Warnings are advisory and do not block a rebuild. Assertions do fail the build, which is how misconfiguration is caught.
