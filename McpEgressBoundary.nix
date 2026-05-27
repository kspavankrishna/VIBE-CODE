# McpEgressBoundary.nix
#
# A NixOS module for running MCP servers and agent-side tool workers with
# explicit credentials, filesystem access, network egress, and resource limits.
{ config, lib, pkgs, ... }:

let
  inherit (lib)
    attrNames
    attrValues
    concatLists
    concatStringsSep
    escapeShellArgs
    filterAttrs
    hasAttr
    intersectLists
    mapAttrs'
    mapAttrsToList
    mkEnableOption
    mkIf
    mkOption
    nameValuePair
    optional
    optionalAttrs
    types;

  cfg = config.services.mcpEgressBoundary;

  absoluteRuntimePath = types.strMatching "^/.*";
  unitFragmentPattern = "^[A-Za-z0-9][A-Za-z0-9_.-]*$";
  environmentVariablePattern = "^[A-Za-z_][A-Za-z0-9_]*$";
  likelySecretEnvironmentPattern =
    ".*(TOKEN|SECRET|PASSWORD|API_KEY|PRIVATE_KEY|ACCESS_KEY).*";

  serverType = types.submodule ({ name, ... }: {
    options = {
      enable = mkEnableOption "this isolated MCP server or tool worker";

      description = mkOption {
        type = types.str;
        default = "MCP tool boundary for ${name}";
        description = ''
          Human-readable description included in the systemd unit and journal.
        '';
      };

      command = mkOption {
        type = types.str;
        example = lib.literalExpression ''"${pkgs.my-mcp-server}/bin/server"'';
        description = ''
          Absolute executable path. Interpolate a Nix package path here so the
          deployed binary is pinned by the NixOS configuration.
        '';
      };

      arguments = mkOption {
        type = types.listOf types.str;
        default = [ ];
        example = [ "--transport" "stdio" ];
        description = ''
          Individual arguments passed without shell parsing. Do not place
          secrets here because process arguments are observable on a host.
        '';
      };

      runtimePackages = mkOption {
        type = types.listOf types.package;
        default = [ ];
        example = lib.literalExpression ''[ pkgs.git pkgs.cacert ]'';
        description = ''
          Packages placed on PATH for a server which deliberately delegates to
          helper executables. Leave this empty unless those helpers are needed.
        '';
      };

      startAtBoot = mkOption {
        type = types.bool;
        default = true;
        description = "Whether the generated service starts at boot.";
      };

      after = mkOption {
        type = types.listOf types.str;
        default = [ "network.target" ];
        description = "Systemd units ordered before this server.";
      };

      wants = mkOption {
        type = types.listOf types.str;
        default = [ ];
        description = "Systemd units weakly required by this server.";
      };

      requires = mkOption {
        type = types.listOf types.str;
        default = [ ];
        description = "Systemd units strictly required by this server.";
      };

      transport = mkOption {
        type = types.enum [ "stdio" "http" "worker" ];
        default = "stdio";
        description = ''
          Operational transport. An http service must opt into at least
          loopback networking; stdio and queue workers may use no networking.
        '';
      };

      environment = mkOption {
        type = types.attrsOf types.str;
        default = { };
        example = { LOG_LEVEL = "info"; };
        description = ''
          Non-secret environment variables. Tokens, private keys, passwords,
          and API keys belong in credentials or encryptedCredentials instead.
        '';
      };

      credentials = mkOption {
        type = types.attrsOf absoluteRuntimePath;
        default = { };
        example = {
          GITHUB_TOKEN = "/run/agenix/mcp-github-token";
        };
        description = ''
          Credential name to absolute runtime source path. Use paths under
          /run supplied by sops-nix, agenix, or another secret manager. These
          values are strings rather than Nix paths so Nix does not copy secret
          material into the world-readable Nix store. The server receives each
          item under $CREDENTIALS_DIRECTORY.
        '';
      };

      encryptedCredentials = mkOption {
        type = types.attrsOf absoluteRuntimePath;
        default = { };
        description = ''
          Credential name to a systemd encrypted credential file. At service
          start systemd decrypts each file into its protected credential
          directory. The same name must not also occur in credentials.
        '';
      };

      credentialEnvironment = mkOption {
        type = types.attrsOf types.str;
        default = { };
        example = { GITHUB_TOKEN_FILE = "GITHUB_TOKEN"; };
        description = ''
          Environment variable to credential name mapping for programs that
          accept a *_FILE setting. The exported value is a file path inside
          $CREDENTIALS_DIRECTORY, never the credential content itself.
        '';
      };

      identity.dynamicUser = mkOption {
        type = types.bool;
        default = true;
        description = ''
          Run with a systemd allocated identity. This prevents a tool server
          from quietly inheriting access owned by a long-lived service user.
        '';
      };

      identity.user = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = ''
          Existing account used only when dynamicUser is false. This is useful
          when access to a narrowly scoped Unix socket cannot use ACLs.
        '';
      };

      identity.group = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "Existing group used only when dynamicUser is false.";
      };

      files.stateDirectories = mkOption {
        type = types.listOf types.str;
        default = [ "mcp-boundary-${name}" ];
        description = ''
          Relative StateDirectory names managed by systemd below /var/lib.
          Each directory is private and writable by the isolated identity.
        '';
      };

      files.cacheDirectories = mkOption {
        type = types.listOf types.str;
        default = [ "mcp-boundary-${name}" ];
        description = "Relative CacheDirectory names managed below /var/cache.";
      };

      files.runtimeDirectories = mkOption {
        type = types.listOf types.str;
        default = [ "mcp-boundary-${name}" ];
        description = "Relative RuntimeDirectory names managed below /run.";
      };

      files.readOnlyPaths = mkOption {
        type = types.listOf absoluteRuntimePath;
        default = [ ];
        example = [ "/srv/repositories/product-api" ];
        description = ''
          Explicit host paths made visible read-only. A repository analysis
          server generally needs a selected checkout here, not all of /home.
        '';
      };

      files.writablePaths = mkOption {
        type = types.listOf absoluteRuntimePath;
        default = [ ];
        description = ''
          Exceptional host paths made writable. Prefer stateDirectories or
          cacheDirectories so lifecycle and ownership remain declarative.
        '';
      };

      files.inaccessiblePaths = mkOption {
        type = types.listOf absoluteRuntimePath;
        default = [ "/root" "/home" "/run/user" ];
        description = ''
          Host paths hidden from the service. Remove a path only after adding
          a smaller read-only or writable path that documents actual need.
        '';
      };

      files.allowReadOnlyHome = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Expose home directories read-only through ProtectHome. This should
          rarely be needed because selected paths are safer and auditable.
        '';
      };

      network.policy = mkOption {
        type = types.enum [ "none" "loopback" "allowlist" "unrestricted" ];
        default = "none";
        description = ''
          Network policy implemented by systemd. none uses a private network
          namespace plus an IP deny rule; loopback exposes local endpoints;
          allowlist permits loopback and allowedCIDRs; unrestricted disables
          IP filtering and should be exceptional.
        '';
      };

      network.allowedCIDRs = mkOption {
        type = types.listOf types.str;
        default = [ ];
        example = [ "192.0.2.40/32" "2001:db8:101::/64" ];
        description = ''
          Numeric IPv4 or IPv6 ranges permitted for allowlist policy. Domain
          names are intentionally unsupported because DNS answers change after
          review and are not an egress boundary.
        '';
      };

      network.addressFamilies = mkOption {
        type = types.listOf types.str;
        default = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
        description = ''
          Socket families allowed when networking is enabled. Remove AF_INET6
          only when the destination and local resolver genuinely do not use it.
        '';
      };

      resources.memoryMax = mkOption {
        type = types.str;
        default = "1G";
        example = "4G";
        description = "Hard memory ceiling understood by systemd.";
      };

      resources.memorySwapMax = mkOption {
        type = types.str;
        default = "0";
        description = ''
          Swap budget. Defaulting to zero keeps an overloaded tool from causing
          latency collapse across other inference and developer workloads.
        '';
      };

      resources.cpuQuota = mkOption {
        type = types.str;
        default = "200%";
        description = "CPU ceiling; 100% corresponds to one CPU core.";
      };

      resources.tasksMax = mkOption {
        type = types.int;
        default = 256;
        description = "Maximum task count for fork and thread containment.";
      };

      resources.openFilesMax = mkOption {
        type = types.int;
        default = 4096;
        description = "Maximum number of file descriptors.";
      };

      resources.ioWeight = mkOption {
        type = types.nullOr types.int;
        default = null;
        description = ''
          Optional cgroup IO weight from 1 through 10000. Set this for indexers
          that otherwise interfere with builds on shared developer machines.
        '';
      };

      lifecycle.restart = mkOption {
        type = types.enum [ "no" "on-failure" "always" ];
        default = "on-failure";
        description = "Systemd restart policy.";
      };

      lifecycle.restartSec = mkOption {
        type = types.str;
        default = "5s";
        description = "Backoff duration before restart.";
      };

      lifecycle.timeoutStartSec = mkOption {
        type = types.str;
        default = "45s";
        description = "Maximum startup duration.";
      };

      lifecycle.timeoutStopSec = mkOption {
        type = types.str;
        default = "30s";
        description = "Graceful shutdown duration before termination.";
      };

      lifecycle.runtimeMaxSec = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "6h";
        description = ''
          Optional maximum lifetime for one process invocation. This is useful
          for workers which should recycle rather than accumulate stale state.
        '';
      };

      lifecycle.startLimitIntervalSec = mkOption {
        type = types.str;
        default = "5min";
        description = "Interval used for crash-loop rate limiting.";
      };

      lifecycle.startLimitBurst = mkOption {
        type = types.int;
        default = 5;
        description = "Starts permitted during the rate-limit interval.";
      };

      logging.rateLimitIntervalSec = mkOption {
        type = types.str;
        default = "30s";
        description = "Journal rate-limit interval for this service.";
      };

      logging.rateLimitBurst = mkOption {
        type = types.int;
        default = 1000;
        description = "Journal messages retained per rate-limit interval.";
      };

      sandbox.restrictNamespaces = mkOption {
        type = types.bool;
        default = true;
        description = ''
          Block namespace creation. Turn off only for an explicitly reviewed
          browser or containerized tool which genuinely requires namespaces.
        '';
      };

      sandbox.memoryDenyWriteExecute = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Prohibit writable executable memory. Enable for native servers after
          testing; JavaScript and JVM runtimes commonly need JIT mappings.
        '';
      };

      sandbox.privateDevices = mkOption {
        type = types.bool;
        default = true;
        description = "Expose only systemd's minimal private device set.";
      };

      sandbox.deniedSystemCallGroups = mkOption {
        type = types.listOf types.str;
        default = [
          "@mount"
          "@obsolete"
          "@privileged"
          "@raw-io"
          "@reboot"
          "@swap"
        ];
        description = ''
          Systemd syscall groups denied with EPERM. This deny-list keeps common
          language runtimes working while blocking host-administration actions.
        '';
      };
    };
  });

  enabledServers = filterAttrs (_: server: server.enable) cfg.servers;

  serviceName = name: "${cfg.unitPrefix}${name}";

  credentialFiles = server:
    server.credentials // server.encryptedCredentials;

  allowedIPAddresses = server:
    if server.network.policy == "none" then [ ]
    else if server.network.policy == "loopback" then [ "localhost" ]
    else if server.network.policy == "allowlist" then
      [ "localhost" ] ++ server.network.allowedCIDRs
    else [ ];

  hasIPAddressFilter = server:
    server.network.policy != "unrestricted";

  credentialExports = server:
    mapAttrsToList
      (environmentName: credentialName:
        ''export ${environmentName}="$CREDENTIALS_DIRECTORY/${credentialName}"'')
      server.credentialEnvironment;

  serviceFor = name: server:
    let
      ipAddresses = allowedIPAddresses server;
      commandLine = escapeShellArgs ([ server.command ] ++ server.arguments);
      credentialLines = credentialExports server;
    in
    {
      description = server.description;
      wantedBy = optional server.startAtBoot "multi-user.target";
      inherit (server) after wants requires;
      path = server.runtimePackages;
      environment = server.environment;

      unitConfig = {
        StartLimitIntervalSec = server.lifecycle.startLimitIntervalSec;
        StartLimitBurst = server.lifecycle.startLimitBurst;
      };

      serviceConfig = {
        Type = "exec";
        DynamicUser = server.identity.dynamicUser;
        UMask = "0077";

        Restart = server.lifecycle.restart;
        RestartSec = server.lifecycle.restartSec;
        TimeoutStartSec = server.lifecycle.timeoutStartSec;
        TimeoutStopSec = server.lifecycle.timeoutStopSec;
        KillMode = "mixed";
        KillSignal = "SIGTERM";
        OOMPolicy = "kill";

        MemoryMax = server.resources.memoryMax;
        MemorySwapMax = server.resources.memorySwapMax;
        CPUQuota = server.resources.cpuQuota;
        TasksMax = server.resources.tasksMax;
        LimitNOFILE = server.resources.openFilesMax;

        StandardOutput = "journal";
        StandardError = "journal";
        SyslogIdentifier = serviceName name;
        LogRateLimitIntervalSec = server.logging.rateLimitIntervalSec;
        LogRateLimitBurst = server.logging.rateLimitBurst;

        StateDirectory = server.files.stateDirectories;
        StateDirectoryMode = "0700";
        CacheDirectory = server.files.cacheDirectories;
        CacheDirectoryMode = "0700";
        RuntimeDirectory = server.files.runtimeDirectories;
        RuntimeDirectoryMode = "0700";
        ReadOnlyPaths = server.files.readOnlyPaths;
        ReadWritePaths = server.files.writablePaths;
        InaccessiblePaths = server.files.inaccessiblePaths;

        LoadCredential = mapAttrsToList
          (credentialName: path: "${credentialName}:${path}")
          server.credentials;
        LoadCredentialEncrypted = mapAttrsToList
          (credentialName: path: "${credentialName}:${path}")
          server.encryptedCredentials;

        NoNewPrivileges = true;
        CapabilityBoundingSet = "";
        AmbientCapabilities = "";
        LockPersonality = true;
        MemoryDenyWriteExecute = server.sandbox.memoryDenyWriteExecute;
        PrivateDevices = server.sandbox.privateDevices;
        PrivateTmp = true;
        ProtectClock = true;
        ProtectControlGroups = true;
        ProtectHome = if server.files.allowReadOnlyHome then "read-only" else true;
        ProtectHostname = true;
        ProtectKernelLogs = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        ProtectProc = "invisible";
        ProcSubset = "pid";
        ProtectSystem = "strict";
        RemoveIPC = true;
        RestrictNamespaces = server.sandbox.restrictNamespaces;
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        SystemCallArchitectures = "native";
        SystemCallErrorNumber = "EPERM";
        SystemCallFilter = map (group: "~${group}") server.sandbox.deniedSystemCallGroups;

        PrivateNetwork = server.network.policy == "none";
        RestrictAddressFamilies =
          if server.network.policy == "none" then [ "AF_UNIX" ]
          else server.network.addressFamilies;
      }
      // optionalAttrs (!server.identity.dynamicUser) {
        User = server.identity.user;
      }
      // optionalAttrs (!server.identity.dynamicUser && server.identity.group != null) {
        Group = server.identity.group;
      }
      // optionalAttrs (server.resources.ioWeight != null) {
        IOWeight = server.resources.ioWeight;
      }
      // optionalAttrs (server.lifecycle.runtimeMaxSec != null) {
        RuntimeMaxSec = server.lifecycle.runtimeMaxSec;
      }
      // optionalAttrs (hasIPAddressFilter server) {
        IPAddressDeny = "any";
      }
      // optionalAttrs (ipAddresses != [ ]) {
        IPAddressAllow = ipAddresses;
      };

      script = concatStringsSep "\n" (
        [ "set -eu" ]
        ++ credentialLines
        ++ [ "exec ${commandLine}" ]
      );
    };

  assertionsFor = name: server:
    let
      allCredentialNames = attrNames (credentialFiles server);
      exportedCredentialNames = attrValues server.credentialEnvironment;
      managedDirectories =
        server.files.stateDirectories
        ++ server.files.cacheDirectories
        ++ server.files.runtimeDirectories;
    in
    [
      {
        assertion = builtins.match unitFragmentPattern name != null;
        message = ''
          services.mcpEgressBoundary.servers.${name}: the server name must be a
          systemd-safe identifier containing letters, digits, dot, underscore,
          or dash and beginning with a letter or digit.
        '';
      }
      {
        assertion = server.identity.dynamicUser
          || server.identity.user != null;
        message = ''
          services.mcpEgressBoundary.servers.${name}: identity.user is required
          when identity.dynamicUser is false.
        '';
      }
      {
        assertion = !server.identity.dynamicUser
          || (server.identity.user == null && server.identity.group == null);
        message = ''
          services.mcpEgressBoundary.servers.${name}: do not provide user or
          group while identity.dynamicUser is true.
        '';
      }
      {
        assertion = server.network.policy != "allowlist"
          || server.network.allowedCIDRs != [ ];
        message = ''
          services.mcpEgressBoundary.servers.${name}: allowlist policy requires
          at least one numeric network.allowedCIDRs entry.
        '';
      }
      {
        assertion = server.network.policy == "allowlist"
          || server.network.allowedCIDRs == [ ];
        message = ''
          services.mcpEgressBoundary.servers.${name}: allowedCIDRs only has an
          effect with allowlist policy; remove it or choose allowlist.
        '';
      }
      {
        assertion = server.transport != "http"
          || server.network.policy != "none";
        message = ''
          services.mcpEgressBoundary.servers.${name}: an HTTP server cannot be
          reached with network.policy set to none. Prefer loopback for a local
          MCP client and bind the server itself to a loopback address.
        '';
      }
      {
        assertion = intersectLists
          (attrNames server.credentials)
          (attrNames server.encryptedCredentials) == [ ];
        message = ''
          services.mcpEgressBoundary.servers.${name}: credential names must be
          unique across credentials and encryptedCredentials.
        '';
      }
      {
        assertion = lib.all
          (environmentName:
            builtins.match environmentVariablePattern environmentName != null)
          (attrNames server.credentialEnvironment);
        message = ''
          services.mcpEgressBoundary.servers.${name}: credentialEnvironment
          keys must be valid environment variable names.
        '';
      }
      {
        assertion = lib.all
          (credentialName: hasAttr credentialName (credentialFiles server))
          exportedCredentialNames;
        message = ''
          services.mcpEgressBoundary.servers.${name}: every value in
          credentialEnvironment must name a configured credential.
        '';
      }
      {
        assertion = intersectLists
          server.files.readOnlyPaths
          server.files.writablePaths == [ ];
        message = ''
          services.mcpEgressBoundary.servers.${name}: a host path cannot be in
          both readOnlyPaths and writablePaths.
        '';
      }
      {
        assertion = lib.all
          (directory: builtins.match unitFragmentPattern directory != null)
          managedDirectories;
        message = ''
          services.mcpEgressBoundary.servers.${name}: managed directory names
          must be simple relative systemd names without slashes.
        '';
      }
    ];

  warningsFor = name: server:
    optional
      (server.network.policy == "unrestricted")
      "mcpEgressBoundary ${name}: unrestricted network egress is enabled; document why a fixed destination allowlist is insufficient."
    ++ optional
      (server.transport == "http" && server.network.policy == "allowlist")
      "mcpEgressBoundary ${name}: bind the HTTP listener to localhost; an allowlist is intended for outbound API calls, not remote tool exposure."
    ++ optional
      (lib.any
        (environmentName:
          builtins.match likelySecretEnvironmentPattern environmentName != null)
        (attrNames server.environment))
      "mcpEgressBoundary ${name}: environment contains a likely secret name; prefer credentials plus a *_FILE variable.";

in
{
  options.services.mcpEgressBoundary = {
    enable = mkEnableOption ''
      constrained MCP servers and agent-side tool workers with explicit egress
      and credential boundaries
    '';

    unitPrefix = mkOption {
      type = types.str;
      default = "mcp-boundary-";
      description = "Prefix used for generated systemd service unit names.";
    };

    servers = mkOption {
      type = types.attrsOf serverType;
      default = { };
      description = ''
        Named MCP servers and workers. Disabled entries create no service and
        are useful for sharing policy defaults between NixOS hosts.
      '';
    };
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = builtins.match unitFragmentPattern cfg.unitPrefix != null;
        message = ''
          services.mcpEgressBoundary.unitPrefix must begin with a letter or
          digit and contain only letters, digits, dot, underscore, or dash.
        '';
      }
    ] ++ concatLists (mapAttrsToList assertionsFor enabledServers);

    warnings = concatLists (mapAttrsToList warningsFor enabledServers);

    systemd.services = mapAttrs'
      (name: server: nameValuePair (serviceName name) (serviceFor name server))
      enabledServers;
  };
}

/*
This solves the practical problem of running Model Context Protocol servers and
agent tool workers on NixOS without silently giving each process the whole
machine, the whole home directory, and open internet access. Built because, in
April 2026, I keep seeing useful coding and research tools that need one token,
one checkout, and one API destination, but are launched with far more access
than that. A prompt mistake or a compromised dependency should not be able to
read unrelated repositories or send credentials anywhere it likes.

Use it when a local MCP server, CI research worker, repository indexer, or
internal developer assistant must run continuously and be boring to operate.
Import `McpEgressBoundary.nix`, enable `services.mcpEgressBoundary`, and define
a server command from a pinned Nix package. Put API tokens in an agenix,
sops-nix, or systemd encrypted credential file under `/run`; map only a
`GITHUB_TOKEN_FILE`-style variable to it. Mount the exact repository as
read-only, choose `network.policy = "allowlist"` for fixed API ranges, and set
resource budgets based on real load tests. For a local HTTP MCP endpoint, bind
the application to localhost and keep its authentication in front of it.

The trick: this module makes the narrow configuration the easy configuration.
It uses systemd credentials instead of plaintext Nix store secrets, a dynamic
user instead of a shared service account, `ProtectSystem=strict` plus explicit
paths instead of an unrestricted filesystem, and cgroup IP filtering instead
of trusting a tool not to call an unexpected host. It also leaves deliberate
escape hatches for JIT runtimes, browser tools, and high-memory index builds,
while printing warnings when a configuration expands the risk.

Drop this into a NixOS infrastructure repository as a reusable NixOS MCP server
security module, import it on developer workstations or controlled build
hosts, and review the resulting `mcp-boundary-*.service` units with
`systemd-analyze security`. Search terms this file addresses directly are
NixOS MCP server sandbox, Model Context Protocol credential isolation, systemd
IPAddressAllow egress firewall, agent tool security, reproducible DevOps tool
runner, and secret-safe Nix configuration. I wrote it so another engineer can
read the service policy in a code review and know exactly which code, files,
secrets, network destinations, and machine resources a tool is allowed to use.
*/
