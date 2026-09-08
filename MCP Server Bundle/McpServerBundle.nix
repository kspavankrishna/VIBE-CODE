/*
McpServerBundle.nix

Hermetic stdio MCP server bundling for local agent stacks.

Example:
let
  pkgs = import <nixpkgs> { };
  bundle = import ./McpServerBundle.nix {
    inherit pkgs;
    bundleName = "research-agents";
    defaultOptionalFromEnv = [ "HTTPS_PROXY" "HTTP_PROXY" ];
    servers = {
      filesystem = {
        command = "${pkgs.nodejs_22}/bin/npx";
        args = [ "-y" "@modelcontextprotocol/server-filesystem" "/srv/workspace" ];
      };

      fetch = {
        command = "${pkgs.uv}/bin/uvx";
        args = [ "--from" "mcp-server-fetch" "mcp-server-fetch" ];
        fromEnv = [ "OPENAI_API_KEY" ];
      };
    };
  };
in
  bundle.package
*/
{ pkgs ? import <nixpkgs> { }
, bundleName ? "mcp-bundle"
, servers ? { }
, defaultPackages ? [ ]
, defaultEnv ? { }
, defaultFromEnv ? [ ]
, defaultOptionalFromEnv ? [ ]
, defaultWorkingDirectory ? null
, includeDoctor ? true
, manifestVersion ? 1
, extraManifest ? { }
}:

let
  lib = pkgs.lib;
  jsonFormat = pkgs.formats.json { };

  inherit (builtins)
    attrNames
    concatStringsSep
    elem
    filter
    hasAttr
    head
    length
    listToAttrs
    map
    sort
    tail
    toString
    typeOf;

  inherit (lib)
    filterAttrs
    hasPrefix
    isDerivation
    optional
    optionalString
    recursiveUpdate
    unique;

  fail = message:
    throw "McpServerBundle.nix: ${message}";

  sortStrings = values:
    sort builtins.lessThan values;

  attrNamesSorted = attrs:
    sortStrings (attrNames attrs);

  attrValuesSorted = attrs:
    map (name: attrs.${name}) (attrNamesSorted attrs);

  ensureNonEmptyString = context: value:
    if typeOf value != "string" || value == "" then
      fail "${context} must be a non-empty string"
    else
      value;

  ensureEnvName = context: value:
    let
      name = ensureNonEmptyString context value;
    in
      if builtins.match "[A-Za-z_][A-Za-z0-9_]*" name == null then
        fail "${context} must be a valid shell environment variable name"
      else
        name;

  ensureAttrset = context: value:
    if value == null then
      { }
    else if typeOf value == "set" then
      value
    else
      fail "${context} must be an attribute set";

  ensureBool = context: value:
    if typeOf value != "bool" then
      fail "${context} must be a boolean"
    else
      value;

  ensureStringList = context: value:
    if value == null then
      [ ]
    else if typeOf value != "list" then
      fail "${context} must be a list"
    else
      map
        (entry:
          if typeOf entry == "string" then
            entry
          else
            fail "${context} entries must be strings")
        value;

  ensureNoReservedKeys = context: attrs: reserved:
    let
      badKeys = filter (key: hasAttr key attrs) reserved;
    in
      if badKeys == [ ] then
        attrs
      else
        fail "${context} cannot define reserved key(s): ${concatStringsSep ", " badKeys}";

  scalarToString = context: value:
    let
      kind = typeOf value;
    in
      if kind == "string" then
        value
      else if kind == "path" then
        toString value
      else if kind == "int" then
        toString value
      else if kind == "float" then
        toString value
      else if kind == "bool" then
        if value then "1" else "0"
      else if isDerivation value then
        toString value
      else
        fail "${context} must be a string, path, number, boolean, or derivation";

  normalizeScalarList = context: value:
    if value == null then
      [ ]
    else if typeOf value != "list" then
      fail "${context} must be a list"
    else
      map (entry: scalarToString "${context} entry" entry) value;

  normalizePackageList = context: value:
    if value == null then
      [ ]
    else if isDerivation value then
      [ value ]
    else if typeOf value == "list" then
      map
        (pkg:
          if isDerivation pkg then
            pkg
          else
            fail "${context} entries must be derivations")
        value
    else
      fail "${context} must be a derivation or a list of derivations";

  normalizeEnvAttrset = context: value:
    let
      raw = ensureAttrset context value;
    in
      listToAttrs
        (map
          (name: {
            name = ensureEnvName "${context}.${name}" name;
            value = scalarToString "${context}.${name}" raw.${name};
          })
          (attrNamesSorted raw));

  normalizeEnvRefs = context: value:
    if value == null then
      [ ]
    else if typeOf value == "list" then
      map
        (entry:
          let
            name = ensureEnvName "${context} entry" entry;
          in
          {
            target = name;
            source = name;
          })
        value
    else if typeOf value == "set" then
      map
        (target:
          let
            targetName = ensureEnvName "${context}.${target}" target;
            sourceName = ensureEnvName "${context}.${target}" value.${target};
          in
          {
            target = targetName;
            source = sourceName;
          })
        (attrNamesSorted value)
    else
      fail "${context} must be a list or attribute set";

  envRefsToAttrset = refs:
    listToAttrs (map (ref: { name = ref.target; value = ref; }) refs);

  mergeEnvRefs = left: right:
    attrValuesSorted (envRefsToAttrset left // envRefsToAttrset right);

  removeEnvTargets = refs: targets:
    filter (ref: !(elem ref.target targets)) refs;

  listEnvTargets = refs:
    map (ref: ref.target) refs;

  renderShellWords = values:
    concatStringsSep " " (map lib.escapeShellArg values);

  renderPreviewCommand = command: args:
    concatStringsSep " " ([ command ] ++ args);

  renderStringList = values:
    if values == [ ] then
      "(none)"
    else
      concatStringsSep ", " values;

  renderEnvRefSummary = refs:
    renderStringList
      (map
        (ref:
          if ref.target == ref.source then
            ref.source
          else
            "${ref.target}<-${ref.source}")
        refs);

  shellReference = name:
    "\${" + name + "-}";

  shellValue = name:
    "\${" + name + "}";

  joinSections = sections:
    concatStringsSep "\n\n" (filter (section: section != "") sections);

  makeLauncherName = raw:
    let
      safe = lib.strings.sanitizeDerivationName (lib.strings.toLower raw);
    in
      if safe == "" then
        fail "launcher names must contain at least one usable character"
      else if hasPrefix "mcp-" safe then
        safe
      else
        "mcp-${safe}";

  defaultPackagesNormalized = unique (normalizePackageList "defaultPackages" defaultPackages);
  defaultEnvNormalized = normalizeEnvAttrset "defaultEnv" defaultEnv;
  defaultFromEnvNormalized = normalizeEnvRefs "defaultFromEnv" defaultFromEnv;
  defaultOptionalFromEnvNormalized = normalizeEnvRefs "defaultOptionalFromEnv" defaultOptionalFromEnv;
  defaultWorkingDirectoryNormalized =
    if defaultWorkingDirectory == null then
      null
    else
      scalarToString "defaultWorkingDirectory" defaultWorkingDirectory;

  normalizedBundleName = ensureNonEmptyString "bundleName" bundleName;
  safeBundleName = lib.strings.sanitizeDerivationName normalizedBundleName;

  normalizeCommandSpec = context: spec:
    let
      hasExec = hasAttr "exec" spec;
      hasCommand = hasAttr "command" spec;
      hasBin = hasAttr "bin" spec;
      selectorCount = length (filter (flag: flag) [ hasExec hasCommand hasBin ]);
      execVector =
        if hasExec then
          let
            entries = normalizeScalarList "${context}.exec" spec.exec;
          in
            if entries == [ ] then
              fail "${context}.exec must contain at least one item"
            else
              entries
        else
          [ ];
      selection =
        if selectorCount != 1 then
          fail "${context} must define exactly one of exec, command, or bin"
        else if hasExec then
          {
            command = head execVector;
            args = tail execVector;
          }
        else if hasCommand then
          {
            command = scalarToString "${context}.command" spec.command;
            args = [ ];
          }
        else
          {
            command = ensureNonEmptyString "${context}.bin" spec.bin;
            args = [ ];
          };
      trailingArgs =
        if hasAttr "args" spec then
          normalizeScalarList "${context}.args" spec.args
        else
          [ ];
    in
      {
        command = selection.command;
        args = selection.args ++ trailingArgs;
      };

  normalizeServer = name: value:
    let
      context = "server `${name}`";
      spec =
        if typeOf value == "set" then
          value
        else
          fail "${context} must be an attribute set";
      hasCwd = hasAttr "cwd" spec;
      hasWorkingDirectory = hasAttr "workingDirectory" spec;
      _cwdCheck =
        if hasCwd && hasWorkingDirectory then
          fail "${context} cannot define both cwd and workingDirectory"
        else
          null;
      enabled =
        if hasAttr "enable" spec then
          ensureBool "${context}.enable" spec.enable
        else
          true;
      systems =
        if hasAttr "systems" spec then
          ensureStringList "${context}.systems" spec.systems
        else
          [ ];
      selectedPackages = unique (
        defaultPackagesNormalized
        ++ (if hasAttr "packages" spec then normalizePackageList "${context}.packages" spec.packages else [ ])
        ++ (if hasAttr "package" spec then normalizePackageList "${context}.package" spec.package else [ ])
      );
      literalEnv = defaultEnvNormalized // normalizeEnvAttrset "${context}.env" (if hasAttr "env" spec then spec.env else { });
      requiredEnv = mergeEnvRefs defaultFromEnvNormalized (normalizeEnvRefs "${context}.fromEnv" (if hasAttr "fromEnv" spec then spec.fromEnv else [ ]));
      optionalEnvRaw = mergeEnvRefs defaultOptionalFromEnvNormalized (normalizeEnvRefs "${context}.optionalFromEnv" (if hasAttr "optionalFromEnv" spec then spec.optionalFromEnv else [ ]));
      optionalEnv = removeEnvTargets optionalEnvRaw (listEnvTargets requiredEnv);
      selectedWorkingDirectory =
        if hasWorkingDirectory then
          spec.workingDirectory
        else if hasCwd then
          spec.cwd
        else
          defaultWorkingDirectory;
      workingDirectory =
        if selectedWorkingDirectory == null then
          defaultWorkingDirectoryNormalized
        else
          scalarToString "${context}.workingDirectory" selectedWorkingDirectory;
      manifestExtras = ensureNoReservedKeys "${context}.manifestExtras" (ensureAttrset "${context}.manifestExtras" (if hasAttr "manifestExtras" spec then spec.manifestExtras else { })) [ "command" "args" ];
      commandSpec = normalizeCommandSpec context spec;
      launcherName =
        if hasAttr "launcherName" spec then
          makeLauncherName (ensureNonEmptyString "${context}.launcherName" spec.launcherName)
        else
          makeLauncherName name;
      transport =
        if hasAttr "transport" spec then
          let
            valueText = ensureNonEmptyString "${context}.transport" spec.transport;
          in
            if valueText != "stdio" then
              fail "${context}.transport must be `stdio` when using McpServerBundle.nix"
            else
              valueText
        else
          "stdio";
      description =
        if hasAttr "description" spec then
          scalarToString "${context}.description" spec.description
        else
          null;
      beforeExec =
        if hasAttr "beforeExec" spec then
          scalarToString "${context}.beforeExec" spec.beforeExec
        else
          "";
      allowedOnCurrentSystem = systems == [ ] || elem pkgs.stdenv.hostPlatform.system systems;
      packagePath = lib.makeBinPath selectedPackages;
    in
      {
        inherit
          name
          enabled
          systems
          allowedOnCurrentSystem
          launcherName
          literalEnv
          requiredEnv
          optionalEnv
          workingDirectory
          manifestExtras
          description
          beforeExec
          transport
          packagePath;
        command = commandSpec.command;
        args = commandSpec.args;
        packages = selectedPackages;
      };

  normalizedServers = lib.mapAttrs normalizeServer servers;

  eligibleServers =
    filterAttrs
      (_: spec: spec.enabled && spec.allowedOnCurrentSystem)
      normalizedServers;

  eligibleServerNames = attrNamesSorted eligibleServers;

  _nonEmptyServerCheck =
    if eligibleServerNames == [ ] then
      fail "no enabled servers matched the current system"
    else
      null;

  launcherNames = map (name: eligibleServers.${name}.launcherName) eligibleServerNames;

  duplicateLaunchers =
    let
      folded =
        lib.foldl'
          (state: launcher:
            if hasAttr launcher state.seen then
              {
                seen = state.seen;
                duplicates = unique (state.duplicates ++ [ launcher ]);
              }
            else
              {
                seen = state.seen // listToAttrs [ { name = launcher; value = true; } ];
                duplicates = state.duplicates;
              })
          {
            seen = { };
            duplicates = [ ];
          }
          launcherNames;
    in
      folded.duplicates;

  _duplicateLauncherCheck =
    if duplicateLaunchers == [ ] then
      null
    else
      fail "launcher name collision(s): ${concatStringsSep ", " duplicateLaunchers}";

  renderLiteralEnvBlock = envAttrs:
    concatStringsSep "\n"
      (map
        (name: "export ${name}=${lib.escapeShellArg envAttrs.${name}}")
        (attrNamesSorted envAttrs));

  renderRequiredEnvBlock = serverName: refs:
    concatStringsSep "\n"
      (map
        (ref: ''
          if [[ -z "${shellReference ref.source}" ]]; then
            echo "McpServerBundle: server ${serverName} requires environment variable ${ref.source}" >&2
            exit 64
          fi
          export ${ref.target}="${shellValue ref.source}"
        '')
        refs);

  renderOptionalEnvBlock = refs:
    concatStringsSep "\n"
      (map
        (ref: ''
          if [[ -n "${shellReference ref.source}" ]]; then
            export ${ref.target}="${shellValue ref.source}"
          fi
        '')
        refs);

  renderWorkingDirectoryBlock = serverName: workingDirectory:
    if workingDirectory == null then
      ""
    else
      ''
        if [[ ! -d ${lib.escapeShellArg workingDirectory} ]]; then
          echo "McpServerBundle: working directory ${workingDirectory} for server ${serverName} does not exist" >&2
          exit 66
        fi
        cd ${lib.escapeShellArg workingDirectory}
      '';

  renderCommandResolutionBlock = spec:
    ''
      resolved_command=${lib.escapeShellArg spec.command}
      if [[ "$resolved_command" != */* ]]; then
        resolved_command="$(command -v "$resolved_command" || true)"
        if [[ -z "$resolved_command" ]]; then
          echo "McpServerBundle: command ${spec.command} for server ${spec.name} is not on PATH" >&2
          exit 127
        fi
      elif [[ ! -x "$resolved_command" ]]; then
        echo "McpServerBundle: command ${spec.command} for server ${spec.name} is not executable" >&2
        exit 127
      fi
    '';

  renderExecLine = spec:
    let
      renderedArgs = renderShellWords spec.args;
    in
      if renderedArgs == "" then
        ''exec "$resolved_command"''
      else
        ''exec "$resolved_command" ${renderedArgs}'';

  launcherFor = name: spec:
    pkgs.writeShellScriptBin spec.launcherName (joinSections [
      "set -euo pipefail"
      (optionalString (spec.packagePath != "") ''
        export PATH=${lib.escapeShellArg spec.packagePath}:''${PATH:-}
      '')
      (renderLiteralEnvBlock spec.literalEnv)
      (renderRequiredEnvBlock name spec.requiredEnv)
      (renderOptionalEnvBlock spec.optionalEnv)
      (renderWorkingDirectoryBlock name spec.workingDirectory)
      spec.beforeExec
      (renderCommandResolutionBlock spec)
      (renderExecLine spec)
    ]);

  launchers = lib.mapAttrs launcherFor eligibleServers;

  launcherPaths =
    listToAttrs
      (map
        (name: {
          name = name;
          value = "${launchers.${name}}/bin/${eligibleServers.${name}.launcherName}";
        })
        eligibleServerNames);

  manifestServers =
    listToAttrs
      (map
        (name:
          let
            spec = eligibleServers.${name};
          in
          {
            inherit name;
            value = recursiveUpdate {
              command = launcherPaths.${name};
              args = [ ];
            } spec.manifestExtras;
          })
        eligibleServerNames);

  normalizedExtraManifest =
    ensureNoReservedKeys "extraManifest" (ensureAttrset "extraManifest" extraManifest) [ "mcpServers" "xBundle" ];

  manifestRelativePath = "share/mcp/${safeBundleName}.json";

  manifest = recursiveUpdate {
    version = manifestVersion;
    mcpServers = manifestServers;
    xBundle = {
      name = normalizedBundleName;
      system = pkgs.stdenv.hostPlatform.system;
      manifestPath = manifestRelativePath;
      generatedBy = "McpServerBundle.nix";
      serverCount = length eligibleServerNames;
    };
  } normalizedExtraManifest;

  manifestFile = jsonFormat.generate "${safeBundleName}.json" manifest;

  manifestTree = pkgs.runCommand "${safeBundleName}-manifest-tree" { } ''
    mkdir -p "$out/share/mcp"
    ln -s ${manifestFile} "$out/${manifestRelativePath}"
  '';

  doctorPayload = {
    bundleName = normalizedBundleName;
    system = pkgs.stdenv.hostPlatform.system;
    manifestFile = toString manifestFile;
    manifestRelativePath = manifestRelativePath;
    servers =
      map
        (name:
          let
            spec = eligibleServers.${name};
          in
          {
            inherit name;
            launcherName = spec.launcherName;
            launcherPath = launcherPaths.${name};
            command = spec.command;
            args = spec.args;
            commandPreview = renderPreviewCommand spec.command spec.args;
            workingDirectory = spec.workingDirectory;
            requiredEnv = spec.requiredEnv;
            optionalEnv = spec.optionalEnv;
            literalEnvKeys = attrNamesSorted spec.literalEnv;
            packageCount = length spec.packages;
          })
        eligibleServerNames;
  };

  doctorPayloadFile = jsonFormat.generate "${safeBundleName}-doctor.json" doctorPayload;

  renderDoctorServer = name:
    let
      spec = eligibleServers.${name};
    in
      ''
      - ${name}
        launcher: ${spec.launcherName}
        command: ${renderPreviewCommand spec.command spec.args}
        required env: ${renderEnvRefSummary spec.requiredEnv}
        optional env: ${renderEnvRefSummary spec.optionalEnv}
        literal env keys: ${renderStringList (attrNamesSorted spec.literalEnv)}
        working directory: ${if spec.workingDirectory == null then "(none)" else spec.workingDirectory}
        package count: ${toString (length spec.packages)}'';

  doctorReport = concatStringsSep "\n" (map renderDoctorServer eligibleServerNames);

  doctorEnvChecks =
    concatStringsSep "\n"
      (map
        (name:
          let
            spec = eligibleServers.${name};
          in
            concatStringsSep "\n"
              (map
                (ref: ''
                  if [[ -z "${shellReference ref.source}" ]]; then
                    echo "missing: ${ref.source} required by server ${name}" >&2
                    missing=1
                  fi
                '')
                spec.requiredEnv))
        eligibleServerNames);

  doctorName = "${safeBundleName}-mcp-doctor";
  doctorRuntimePath = lib.makeBinPath [ pkgs.coreutils ];

  doctorPackage =
    if includeDoctor then
      pkgs.writeShellScriptBin doctorName (joinSections [
        "set -euo pipefail"
        (optionalString (doctorRuntimePath != "") ''
          export PATH=${lib.escapeShellArg doctorRuntimePath}:''${PATH:-}
        '')
        ''
          mode="''${1-}"
          case "$mode" in
            ""|--check-env|--json|--manifest)
              ;;
            *)
              echo "usage: ${doctorName} [--json|--manifest|--check-env]" >&2
              exit 64
              ;;
          esac

          self_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
          bundle_root="$(CDPATH= cd -- "$self_dir/.." && pwd -P)"
          manifest_path="$bundle_root/${manifestRelativePath}"
          if [[ ! -e "$manifest_path" ]]; then
            manifest_path=${lib.escapeShellArg (toString manifestFile)}
          fi

          if [[ "$mode" == "--manifest" ]]; then
            printf '%s\n' "$manifest_path"
            exit 0
          fi

          if [[ "$mode" == "--json" ]]; then
            cat ${lib.escapeShellArg (toString doctorPayloadFile)}
            exit 0
          fi

          if [[ "$mode" == "--check-env" ]]; then
            missing=0
            ${doctorEnvChecks}
            if [[ "$missing" -ne 0 ]]; then
              exit 1
            fi
            echo "all required environment variables are present"
            exit 0
          fi
        ''
        ''
          cat <<EOF
          McpServerBundle
          bundle: ${normalizedBundleName}
          system: ${pkgs.stdenv.hostPlatform.system}
          manifest: $manifest_path
          servers: ${toString (length eligibleServerNames)}
          EOF

          if [[ -n ${lib.escapeShellArg doctorReport} ]]; then
            printf '%s\n' ${lib.escapeShellArg doctorReport}
          fi
        ''
      ])
    else
      null;

  bundlePackage = pkgs.symlinkJoin {
    name = "${safeBundleName}-mcp-bundle";
    paths = attrValuesSorted launchers ++ [ manifestTree ] ++ optional includeDoctor doctorPackage;
  };

in
{
  package = bundlePackage;
  doctor = if includeDoctor then doctorPackage else null;
  manifest = manifest;
  manifestFile = manifestFile;
  manifestRelativePath = manifestRelativePath;
  launchers = launchers;
  launcherPaths = launcherPaths;
  servers = eligibleServers;
  bundleInfo = doctorPayload;
}

/*
This solves the April 2026 problem of shipping local MCP server stacks in a way that is actually reproducible across laptops, CI workers, devcontainers, and agent hosts. A lot of teams now run a mix of Node, Python, uvx, npx, and compiled binaries behind Codex, Claude Desktop, Cursor, Windsurf, or internal agent runners, but the painful part is not the server code. The painful part is getting the exact launch command, PATH, working directory, and secret handling consistent everywhere without copying the same JSON config into five places.

Built because the normal setup is still messy: someone hardcodes `OPENAI_API_KEY` or `GITHUB_TOKEN` into a config file, someone else depends on `npx` existing globally, another machine has the right package but the wrong working directory, and CI cannot reproduce the bundle that worked on a single developer laptop. Then you lose time debugging agent startup, not the actual tool behavior. I wanted one Nix file that wraps the runtime, keeps secrets out of the store, emits a ready `mcpServers` manifest, and gives a doctor command that explains what was bundled.

Use it when you need a serious Nix solution for MCP infrastructure, AI tooling platforms, internal developer portals, research stacks, or edge agent hosts where the server command line has to be dependable. It is especially useful when your MCP fleet is heterogeneous: one server comes from `uvx`, another from `npx`, another from a Go or Rust binary, and all of them need stable launchers plus environment passthrough rules. It is also useful when you need to fork a bundle per team or per environment without rewriting every launcher by hand.

The trick: the generated manifest never needs to carry real secrets. Each server launcher can inherit selected environment variables at runtime through `fromEnv` and `optionalFromEnv`, while literal non-secret settings can still be embedded through `env`. That means the manifest points clients to a hermetic wrapper, not to a fragile handwritten command. The wrapper can set PATH from derivations, verify required environment variables, move into the right working directory, run a pre-exec hook, and then exec the real server. The companion doctor command gives you a fast check for missing environment variables and a readable summary of the bundle shape.

Drop this into a repo as `McpServerBundle.nix`, import it from a flake or `nix build` entrypoint, define your servers in the `servers` attribute set, and expose `bundle.package` to the machines that need the MCP config. The generated manifest lives under `share/mcp/<bundle>.json`, the wrapper launchers live under `bin/`, and the doctor command helps you verify the final output before anyone opens an agent client. I wrote it to be easy to fork, easy to search for on GitHub or Google when someone is looking for a Nix MCP manifest generator, and solid enough that a senior platform engineer can put it into a real workflow instead of a demo.
*/