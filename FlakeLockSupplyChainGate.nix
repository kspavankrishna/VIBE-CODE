# FlakeLockSupplyChainGate.nix
#
# Import this file from CI to audit a flake.lock without vendoring nixpkgs,
# flake-utils, jq, or a shell script. The expression returns a structured report,
# a Markdown summary, SARIF JSON, and a lazy assertion that fails only when read.
#
# Example:
#
#   nix eval --impure --expr '
#     let
#       gate = import ./FlakeLockSupplyChainGate.nix {
#         lockFile = ./flake.lock;
#         policy.nowEpoch = 1775000000;
#       };
#     in gate.assertNoBlockers
#   '
#
# For machine-readable output:
#
#   nix eval --json --expr '(import ./FlakeLockSupplyChainGate.nix {}).summary'
#   nix eval --raw  --expr '(import ./FlakeLockSupplyChainGate.nix {}).sarifJson'

{
  lockFile ? ./flake.lock,
  lockJson ? builtins.fromJSON (builtins.readFile lockFile),
  policy ? {}
}:

let
  typeOf = builtins.typeOf;
  hasAttr = builtins.hasAttr;
  attrNames = builtins.attrNames;
  elem = builtins.elem;
  elemAt = builtins.elemAt;
  length = builtins.length;
  map = builtins.map;
  filter = builtins.filter;
  foldl' = builtins.foldl';
  concatLists = builtins.concatLists;
  concatStringsSep = builtins.concatStringsSep;
  listToAttrs = builtins.listToAttrs;
  match = builtins.match;
  toJSON = builtins.toJSON;
  toString = builtins.toString;

  isAttrs = value: typeOf value == "set";
  isList = value: typeOf value == "list";
  isString = value: typeOf value == "string";
  isIntLike = value: typeOf value == "int";

  defaultPolicy = {
    allowedHosts = [
      "github.com"
      "gitlab.com"
      "git.sr.ht"
      "sourcehut.org"
      "codeberg.org"
    ];
    allowRegistries = false;
    allowPathInputs = false;
    requireNarHash = true;
    requireRevisionPins = true;
    forbidMovingRefs = true;
    maxNixpkgsPins = 1;
    requiredRootInputs = [];
    failOn = [ "critical" "high" ];
    ignoreFindingIds = [];
    ignoreNodes = [];
    ignoreRootInputs = [];
    severityOverrides = {};
    defaultMaxAgeDays = 180;
    maxAgeDaysByType = {
      github = 90;
      gitlab = 90;
      sourcehut = 90;
      git = 45;
      mercurial = 45;
      tarball = 45;
      file = 45;
      path = 0;
      indirect = 0;
      unknown = 0;
    };
    nowEpoch = null;
    ciPath = "flake.lock";
    disallowedMovingRefs = [
      "main"
      "master"
      "trunk"
      "develop"
      "development"
      "dev"
      "latest"
      "stable"
      "release"
    ];
  };

  cfg = defaultPolicy // policy;

  getAttr =
    name: attrs:
    if isAttrs attrs && hasAttr name attrs then attrs.${name} else null;

  firstNotNull =
    values:
    foldl' (acc: value: if acc != null then acc else value) null values;

  optional = condition: value: if condition then [ value ] else [];

  unique =
    values:
    foldl' (seen: value: if elem value seen then seen else seen ++ [ value ]) [] values;

  countWhere = predicate: values: length (filter predicate values);

  normalizeHost =
    host:
    if ! isString host then
      null
    else
      let
        userHost = match ".*@([^@]+)" host;
      in
      if userHost == null then host else elemAt userHost 0;

  schemeHostFromUrl =
    url:
    if ! isString url then
      null
    else
      let
        parsed = match "^[A-Za-z][A-Za-z0-9+.-]*://([^/:?#]+).*" url;
      in
      if parsed == null then null else normalizeHost (elemAt parsed 0);

  scpHostFromUrl =
    url:
    if ! isString url then
      null
    else
      let
        parsed = match "^[^@]+@([^:]+):.*" url;
      in
      if parsed == null then null else normalizeHost (elemAt parsed 0);

  hostFromUrl =
    url:
    firstNotNull [
      (schemeHostFromUrl url)
      (scpHostFromUrl url)
    ];

  sourceType =
    locked: original:
    firstNotNull [
      (getAttr "type" locked)
      (getAttr "type" original)
      "unknown"
    ];

  hostFromSource =
    sourceKind: locked: original:
    let
      explicitHost = firstNotNull [
        (getAttr "host" locked)
        (getAttr "host" original)
      ];
      urlHost = firstNotNull [
        (hostFromUrl (getAttr "url" locked))
        (hostFromUrl (getAttr "url" original))
      ];
    in
    if explicitHost != null then
      explicitHost
    else if sourceKind == "github" then
      "github.com"
    else if sourceKind == "gitlab" then
      "gitlab.com"
    else if sourceKind == "sourcehut" then
      "git.sr.ht"
    else
      urlHost;

  sourceLabel =
    sourceKind: locked: original:
    let
      owner = firstNotNull [
        (getAttr "owner" locked)
        (getAttr "owner" original)
      ];
      repo = firstNotNull [
        (getAttr "repo" locked)
        (getAttr "repo" original)
      ];
      url = firstNotNull [
        (getAttr "url" locked)
        (getAttr "url" original)
      ];
      id = firstNotNull [
        (getAttr "id" original)
        (getAttr "id" locked)
      ];
    in
    if owner != null && repo != null then
      "${sourceKind}:${owner}/${repo}"
    else if url != null then
      "${sourceKind}:${url}"
    else if id != null then
      "${sourceKind}:${id}"
    else
      sourceKind;

  validNarHash =
    value:
    isString value
    && (
      match "sha256-.+" value != null
      || match "sha512-.+" value != null
      || match "sha1-.+" value != null
    );

  validRevision =
    value:
    isString value
    && (
      match "[0-9a-fA-F]{40}" value != null
      || match "[0-9a-fA-F]{64}" value != null
    );

  hasHttpsScheme =
    value:
    isString value && match "https://.*" value != null;

  isMovingRef =
    value:
    isString value && elem value cfg.disallowedMovingRefs;

  severityToSarifLevel =
    severity:
    if severity == "critical" || severity == "high" then
      "error"
    else if severity == "medium" then
      "warning"
    else
      "note";

  makeFinding =
    attrs:
    let
      overriddenSeverity =
        if hasAttr attrs.id cfg.severityOverrides then cfg.severityOverrides.${attrs.id} else attrs.severity;
      inputName = getAttr "input" attrs;
      nodeName = getAttr "node" attrs;
    in
    attrs
    // {
      severity = overriddenSeverity;
      input = inputName;
      node = nodeName;
      blocking = elem overriddenSeverity cfg.failOn;
      sarifLevel = severityToSarifLevel overriddenSeverity;
    };

  nodes =
    let
      candidate = getAttr "nodes" lockJson;
    in
    if isAttrs candidate then candidate else {};

  rootName =
    let
      candidate = getAttr "root" lockJson;
    in
    if isString candidate then candidate else "root";

  rootNode =
    let
      candidate = getAttr rootName nodes;
    in
    if isAttrs candidate then candidate else {};

  rootInputs =
    let
      candidate = getAttr "inputs" rootNode;
    in
    if isAttrs candidate then candidate else {};

  targetNameFromRootInput =
    raw:
    if isString raw then
      raw
    else if isList raw && length raw > 0 then
      elemAt raw (length raw - 1)
    else
      null;

  rootEdges =
    map
      (
        inputName:
        let
          raw = rootInputs.${inputName};
        in
        {
          inherit inputName raw;
          target = targetNameFromRootInput raw;
        }
      )
      (attrNames rootInputs);

  nodeNames = attrNames nodes;

  mkNode =
    name:
    let
      node = nodes.${name};
      locked = getAttr "locked" node;
      original = getAttr "original" node;
      sourceKind = sourceType locked original;
      inputMap = getAttr "inputs" node;
    in
    {
      inherit name node locked original;
      type = sourceKind;
      rev = getAttr "rev" locked;
      narHash = getAttr "narHash" locked;
      lastModified = getAttr "lastModified" locked;
      ref = firstNotNull [
        (getAttr "ref" locked)
        (getAttr "ref" original)
      ];
      url = firstNotNull [
        (getAttr "url" locked)
        (getAttr "url" original)
      ];
      host = hostFromSource sourceKind locked original;
      source = sourceLabel sourceKind locked original;
      inputs = if isAttrs inputMap then inputMap else {};
    };

  nodeRecords = map mkNode nodeNames;

  recordsByName =
    listToAttrs (map (record: { name = record.name; value = record; }) nodeRecords);

  getRecord = name: getAttr name recordsByName;

  narHashRequiredTypes = [
    "github"
    "gitlab"
    "sourcehut"
    "git"
    "mercurial"
    "tarball"
    "file"
  ];

  revisionRequiredTypes = [
    "github"
    "gitlab"
    "sourcehut"
    "git"
    "mercurial"
  ];

  staleLimitForType =
    sourceKind:
    if hasAttr sourceKind cfg.maxAgeDaysByType then cfg.maxAgeDaysByType.${sourceKind} else cfg.defaultMaxAgeDays;

  isStale =
    record:
    let
      limitDays = staleLimitForType record.type;
    in
    cfg.nowEpoch != null
    && isIntLike cfg.nowEpoch
    && isIntLike record.lastModified
    && limitDays > 0
    && (cfg.nowEpoch - record.lastModified) > limitDays * 86400;

  staleAgeSeconds =
    record:
    if cfg.nowEpoch != null && isIntLike cfg.nowEpoch && isIntLike record.lastModified then
      cfg.nowEpoch - record.lastModified
    else
      null;

  nodeFindings =
    record:
    let
      nonRoot = record.name != rootName;
      staleLimitDays = staleLimitForType record.type;
    in
    concatLists [
      (optional (nonRoot && record.locked == null) (makeFinding {
        id = "MissingLockedSource";
        severity = "critical";
        node = record.name;
        message = "The input node has no locked source metadata.";
        evidence = { inherit (record) type source; };
        remediation = "Regenerate flake.lock from a trusted machine and require locked metadata before CI builds.";
      }))

      (optional (nonRoot && record.type == "unknown") (makeFinding {
        id = "UnknownSourceType";
        severity = "medium";
        node = record.name;
        message = "The input node source type is unknown, so the evaluator cannot apply host, hash, or revision checks.";
        evidence = { inherit (record) source; };
        remediation = "Use a supported flake input type or add a local severity override with a written owner.";
      }))

      (optional (record.type == "indirect" && ! cfg.allowRegistries) (makeFinding {
        id = "IndirectRegistryInput";
        severity = "high";
        node = record.name;
        message = "The lock keeps an indirect registry input, which can hide the real source behind client configuration.";
        evidence = { inherit (record) source; original = record.original; };
        remediation = "Replace registry inputs with explicit URLs and commit the resulting locked source.";
      }))

      (optional (record.type == "path" && ! cfg.allowPathInputs && nonRoot) (makeFinding {
        id = "PathInputInCi";
        severity = "high";
        node = record.name;
        message = "The lock references a local path input that may not exist or may differ on CI, builders, or developer laptops.";
        evidence = { inherit (record) source url; };
        remediation = "Publish the dependency as a pinned repository input or explicitly allow the path input for this repository.";
      }))

      (optional (record.host != null && ! elem record.host cfg.allowedHosts) (makeFinding {
        id = "UnapprovedSourceHost";
        severity = "high";
        node = record.name;
        message = "The input resolves from a host that is not in the approved host allowlist.";
        evidence = { inherit (record) source host url; allowedHosts = cfg.allowedHosts; };
        remediation = "Move the source to an approved host or add a documented exception in policy.allowedHosts.";
      }))

      (optional (cfg.requireNarHash && elem record.type narHashRequiredTypes && record.narHash == null && nonRoot) (makeFinding {
        id = "MissingNarHash";
        severity = "critical";
        node = record.name;
        message = "The locked source is missing narHash, so Nix cannot verify the fetched content against a store hash.";
        evidence = { inherit (record) type source; };
        remediation = "Refresh flake.lock with a modern Nix version and commit the generated narHash.";
      }))

      (optional (record.narHash != null && ! validNarHash record.narHash) (makeFinding {
        id = "SuspiciousNarHashFormat";
        severity = "high";
        node = record.name;
        message = "The narHash is present but does not look like a Nix SRI hash.";
        evidence = { inherit (record) source narHash; };
        remediation = "Regenerate the lock entry and verify that the hash starts with sha256-, sha512-, or another accepted SRI prefix.";
      }))

      (optional (cfg.requireRevisionPins && elem record.type revisionRequiredTypes && record.rev == null && nonRoot) (makeFinding {
        id = "MissingRevisionPin";
        severity = "critical";
        node = record.name;
        message = "The source type should be locked to an immutable revision but no rev value is present.";
        evidence = { inherit (record) type source ref; };
        remediation = "Pin the flake input to a commit revision and commit the updated lockfile.";
      }))

      (optional (record.rev != null && elem record.type revisionRequiredTypes && ! validRevision record.rev) (makeFinding {
        id = "SuspiciousRevisionFormat";
        severity = "medium";
        node = record.name;
        message = "The revision is present but does not look like a 40 or 64 character Git or Mercurial content hash.";
        evidence = { inherit (record) source rev; };
        remediation = "Confirm that the revision is immutable. If this host uses another safe format, set a severity override.";
      }))

      (optional (cfg.forbidMovingRefs && isMovingRef record.ref) (makeFinding {
        id = "MovingReferenceRequested";
        severity = "low";
        node = record.name;
        message = "The original input tracks a moving ref. The lock may be pinned today, but updates can silently pull a broad branch.";
        evidence = { inherit (record) source ref rev; };
        remediation = "Use a release tag or documented update cadence for production flakes.";
      }))

      (optional (record.type == "tarball" && record.url != null && ! hasHttpsScheme record.url) (makeFinding {
        id = "InsecureTarballTransport";
        severity = "high";
        node = record.name;
        message = "A tarball input is not fetched over HTTPS.";
        evidence = { inherit (record) source url; };
        remediation = "Use HTTPS or mirror the tarball behind an approved internal binary cache with a narHash.";
      }))

      (optional (isStale record) (makeFinding {
        id = "StaleLockedInput";
        severity = "medium";
        node = record.name;
        message = "The locked input is older than the configured freshness window.";
        evidence = {
          inherit (record) source lastModified;
          ageSeconds = staleAgeSeconds record;
          limitDays = staleLimitDays;
        };
        remediation = "Review upstream changes, refresh the input deliberately, and keep the old pin only with a tracked exception.";
      }))
    ];

  rootEdgeFindings =
    concatLists (map
      (
        edge:
        concatLists [
          (optional (edge.target == null) (makeFinding {
            id = "UnresolvedRootInputPointer";
            severity = "critical";
            node = rootName;
            input = edge.inputName;
            message = "A root input does not point to a concrete lock node.";
            evidence = { raw = edge.raw; };
            remediation = "Regenerate flake.lock and verify that every root input maps to a node name.";
          }))

          (optional (edge.target != null && isString edge.target && ! hasAttr edge.target nodes) (makeFinding {
            id = "MissingRootInputTarget";
            severity = "critical";
            node = rootName;
            input = edge.inputName;
            message = "A root input points to a node that is absent from the lock graph.";
            evidence = { target = edge.target; raw = edge.raw; };
            remediation = "Regenerate flake.lock from the flake root and inspect any merge conflict in the lockfile.";
          }))
        ]
      )
      rootEdges);

  requiredRootInputFindings =
    map
      (
        inputName:
        makeFinding {
          id = "MissingRequiredRootInput";
          severity = "critical";
          node = rootName;
          input = inputName;
          message = "A required root input is missing from the lockfile.";
          evidence = { requiredRootInput = inputName; };
          remediation = "Add the required flake input or remove it from policy.requiredRootInputs with reviewer approval.";
        }
      )
      (filter (inputName: ! hasAttr inputName rootInputs) cfg.requiredRootInputs);

  isNixpkgs =
    record:
    let
      owner = firstNotNull [
        (getAttr "owner" record.locked)
        (getAttr "owner" record.original)
      ];
      repo = firstNotNull [
        (getAttr "repo" record.locked)
        (getAttr "repo" record.original)
      ];
      id = firstNotNull [
        (getAttr "id" record.original)
        (getAttr "id" record.locked)
      ];
    in
    record.name == "nixpkgs" || id == "nixpkgs" || (owner == "NixOS" && repo == "nixpkgs");

  nixpkgsNodes = filter isNixpkgs nodeRecords;
  nixpkgsPins = unique (map (record: if record.rev != null then record.rev else record.name) nixpkgsNodes);

  nixpkgsFindings =
    optional (length nixpkgsPins > cfg.maxNixpkgsPins) (makeFinding {
      id = "DuplicateNixpkgsPins";
      severity = "medium";
      node = rootName;
      message = "The lockfile carries more nixpkgs revisions than the configured maximum.";
      evidence = {
        pinCount = length nixpkgsPins;
        pins = nixpkgsPins;
        nodes = map (record: record.name) nixpkgsNodes;
        maxNixpkgsPins = cfg.maxNixpkgsPins;
      };
      remediation = "Make dependent flakes follow the root nixpkgs input where practical, then keep intentional duplicate pins documented.";
    });

  rawFindings =
    concatLists (map nodeFindings nodeRecords)
    ++ rootEdgeFindings
    ++ requiredRootInputFindings
    ++ nixpkgsFindings;

  isIgnored =
    finding:
    elem finding.id cfg.ignoreFindingIds
    || (finding.node != null && elem finding.node cfg.ignoreNodes)
    || (finding.input != null && elem finding.input cfg.ignoreRootInputs);

  findings = filter (finding: ! isIgnored finding) rawFindings;

  blockerFindings = filter (finding: finding.blocking) findings;
  pass = length blockerFindings == 0;

  severityCounts = {
    critical = countWhere (finding: finding.severity == "critical") findings;
    high = countWhere (finding: finding.severity == "high") findings;
    medium = countWhere (finding: finding.severity == "medium") findings;
    low = countWhere (finding: finding.severity == "low") findings;
  };

  summary = {
    inherit pass rootName;
    nodeCount = length nodeRecords;
    rootInputCount = length rootEdges;
    findingCount = length findings;
    blockerCount = length blockerFindings;
    nixpkgsPinCount = length nixpkgsPins;
    countsBySeverity = severityCounts;
  };

  rootInputMatrix =
    map
      (
        edge:
        let
          record = if edge.target != null && isString edge.target then getRecord edge.target else null;
        in
        {
          input = edge.inputName;
          target = edge.target;
          type = if record == null then "unresolved" else record.type;
          source = if record == null then null else record.source;
          host = if record == null then null else record.host;
          rev = if record == null then null else record.rev;
          narHash = if record == null then null else record.narHash;
          lastModified = if record == null then null else record.lastModified;
          findings = filter
            (
              finding:
              finding.input == edge.inputName || (record != null && finding.node == record.name)
            )
            findings;
        }
      )
      rootEdges;

  byNode =
    listToAttrs (map
      (
        record:
        {
          name = record.name;
          value = {
            inherit (record) type source host rev narHash lastModified ref inputs;
            findings = filter (finding: finding.node == record.name) findings;
          };
        }
      )
      nodeRecords);

  renderFinding =
    finding:
    "- [${finding.severity}] ${finding.id} at ${cfg.ciPath} node ${finding.node}: ${finding.message} Evidence: ${toJSON finding.evidence}";

  markdown =
    concatStringsSep "\n" (
      [
        "# Flake lock supply-chain gate"
        ""
        "Pass: ${if pass then "true" else "false"}"
        "Nodes: ${toString summary.nodeCount}"
        "Root inputs: ${toString summary.rootInputCount}"
        "Findings: ${toString summary.findingCount}"
        "Blockers: ${toString summary.blockerCount}"
        ""
        "## Findings"
      ]
      ++ (if findings == [] then [ "No findings under the active policy." ] else map renderFinding findings)
    );

  sarifRule =
    finding:
    {
      id = finding.id;
      name = finding.id;
      shortDescription = { text = finding.message; };
      fullDescription = { text = finding.remediation; };
      help = { text = finding.remediation; };
    };

  sarifResult =
    finding:
    {
      ruleId = finding.id;
      level = finding.sarifLevel;
      message = { text = "${finding.message} ${finding.remediation}"; };
      locations = [
        {
          physicalLocation = {
            artifactLocation = { uri = cfg.ciPath; };
            region = { startLine = 1; };
          };
        }
      ];
      properties = {
        node = finding.node;
        input = finding.input;
        severity = finding.severity;
        blocking = finding.blocking;
        evidence = finding.evidence;
      };
    };

  sarif = {
    version = "2.1.0";
    "$schema" = "https://json.schemastore.org/sarif-2.1.0.json";
    runs = [
      {
        tool = {
          driver = {
            name = "FlakeLockSupplyChainGate";
            informationUri = "https://github.com/kspavankrishna/VIBE-CODE";
            rules = map sarifRule findings;
          };
        };
        results = map sarifResult findings;
      }
    ];
  };

  failureMessage =
    "flake.lock supply-chain gate failed with ${toString summary.blockerCount} blocking findings: "
    + concatStringsSep "; " (map (finding: "${finding.id}:${finding.node}") blockerFindings);
in
{
  version = 1;
  policy = cfg;
  inherit
    rootName
    rootInputs
    rootInputMatrix
    nodeRecords
    byNode
    rawFindings
    findings
    blockerFindings
    summary
    markdown
    sarif;
  summaryJson = toJSON summary;
  sarifJson = toJSON sarif;
  assertNoBlockers = if pass then true else throw failureMessage;
  recommendedCiCommand =
    "nix eval --impure --expr '(import ./FlakeLockSupplyChainGate.nix { lockFile = ./flake.lock; policy.nowEpoch = builtins.currentTime; }).assertNoBlockers'";
}

/*
This solves the Nix flake.lock supply chain audit problem that shows up when AI agents, build farms, GitHub Actions, and developer laptops all update inputs at different times. Built because a pinned lockfile can still hide risky registry indirection, local path inputs, missing narHash values, duplicate nixpkgs pins, stale source revisions, moving refs, and unapproved hosts. Use it when a repository needs a pure Nix CI gate for reproducible builds, flake input policy, Nix supply chain security, DevOps release checks, and SARIF reporting without pulling in nixpkgs or jq. The trick: the file evaluates the lock graph directly, keeps every finding structured, and makes the failure assertion lazy so teams can inspect JSON, Markdown, or SARIF before choosing to fail the build. Drop this into any flake-based repo, tune the policy attrset, and let reviewers see exactly why a lockfile update is safe or risky in April 2026 production work.
*/
