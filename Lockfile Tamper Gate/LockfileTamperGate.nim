import std/[json, os, parseopt, strutils, sets, tables, algorithm, uri]

const
  Version = "1.0.0"
  MaxLockBytes = 96 * 1024 * 1024
  DefaultRegistryHost = "registry.npmjs.org"
  GitSchemes = ["git", "git+ssh", "git+https", "git+http", "git+file", "ssh", "github", "gitlab", "bitbucket", "gist"]
  GitHosts = ["github.com", "codeload.github.com", "gitlab.com", "bitbucket.org"]
  HexChars = {'0'..'9', 'a'..'f', 'A'..'F'}
  B64Chars = {'A'..'Z', 'a'..'z', '0'..'9', '+', '/'}

type
  Severity = enum
    sevInfo = "INFO"
    sevWarn = "WARN"
    sevError = "ERROR"

  Layout = enum
    layPackages = "packages"
    layDependencies = "dependencies"

  Entry = object
    path, name, version, resolved, integrity: string
    hasInstallScript, isLink, inBundle: bool

  Lock = object
    layout: Layout
    lockfileVersion: int
    entries: seq[Entry]

  Finding = object
    rule: string
    severity: Severity
    pkg, path, message: string

  Options = object
    lockPath, basePath: string
    allowHosts: seq[string]
    allowGit, ignore: HashSet[string]
    jsonOut: bool
    failOn: Severity
    maxNewScripts: int

  LockError = object of CatchableError

  Rule = tuple[id: string, severity: Severity, text: string]

const Rules: seq[Rule] = @[
  ("R001", sevError, "resolved URL host is not on the registry allowlist"),
  ("R002", sevError, "resolved URL uses an insecure or unsupported transport"),
  ("R003", sevError, "registry package has no integrity hash (warn when resolved is missing too)"),
  ("R004", sevError, "integrity hash is malformed (warn when only sha1 is present)"),
  ("R005", sevError, "tarball path does not match the package name and version"),
  ("R006", sevWarn, "git, local file or tarball dependency outside the registry"),
  ("R007", sevError, "git dependency is not pinned to a full commit hash"),
  ("R008", sevError, "the same name and version carries two different integrity hashes"),
  ("R009", sevWarn, "install script added since the base lockfile (error past the cap)"),
  ("R010", sevError, "resolved URL changed for an unchanged version (warn if only the path moved)"),
  ("R011", sevWarn, "version went backwards compared with the base lockfile"),
]

proc fail(msg: string) {.noReturn.} =
  raise newException(LockError, msg)

proc strField(n: JsonNode, key: string): string =
  let v = n{key}
  if v != nil and v.kind == JString: v.getStr() else: ""

proc boolField(n: JsonNode, key: string): bool =
  let v = n{key}
  v != nil and v.kind == JBool and v.getBool()

proc nameFromPath(path: string): string =
  let marker = "node_modules/"
  let i = path.rfind(marker)
  if i >= 0: path[i + marker.len .. ^1] else: path

proc walkV1(deps: JsonNode, prefix: string, acc: var seq[Entry]) =
  for name, d in deps:
    if d.kind != JObject: continue
    let path = prefix & name
    var e = Entry(path: path, name: name, version: d.strField("version"),
                  resolved: d.strField("resolved"), integrity: d.strField("integrity"),
                  inBundle: d.boolField("bundled"))
    # v1 stores git and file sources in the version field with no resolved URL
    if e.resolved.len == 0 and e.version.contains(':'):
      e.resolved = e.version
    acc.add e
    let nested = d{"dependencies"}
    if nested != nil and nested.kind == JObject:
      walkV1(nested, path & ">", acc)

proc parseLock(doc: JsonNode): Lock =
  if doc.kind != JObject: fail("lockfile root is not a JSON object")
  let lv = doc{"lockfileVersion"}
  if lv == nil or lv.kind != JInt: fail("missing lockfileVersion: this is not an npm lockfile")
  result.lockfileVersion = lv.getInt()
  if result.lockfileVersion < 1 or result.lockfileVersion > 3:
    fail("unsupported lockfileVersion " & $result.lockfileVersion)
  let pk = doc{"packages"}
  let dp = doc{"dependencies"}
  if pk != nil and pk.kind == JObject:
    result.layout = layPackages
    for path, p in pk:
      if path.len == 0 or p.kind != JObject: continue
      let declared = p.strField("name")
      result.entries.add Entry(
        path: path, name: if declared.len > 0: declared else: nameFromPath(path),
        version: p.strField("version"), resolved: p.strField("resolved"),
        integrity: p.strField("integrity"), hasInstallScript: p.boolField("hasInstallScript"),
        isLink: p.boolField("link"), inBundle: p.boolField("inBundle"))
  elif dp != nil and dp.kind == JObject:
    result.layout = layDependencies
    walkV1(dp, "", result.entries)
  else:
    result.layout = layPackages

proc loadLock(path: string): Lock =
  if not fileExists(path): fail("lockfile not found: " & path)
  if getFileSize(path) > MaxLockBytes:
    fail("lockfile is larger than " & $(MaxLockBytes div (1024 * 1024)) & " MiB: " & path)
  var doc: JsonNode
  try:
    doc = parseJson(readFile(path))
  except JsonParsingError as ex:
    fail("invalid JSON in " & path & ": " & ex.msg)
  except IOError as ex:
    fail("cannot read " & path & ": " & ex.msg)
  parseLock(doc)

proc hostAllowed(host: string, patterns: seq[string]): bool =
  let h = host.toLowerAscii()
  for p in patterns:
    let q = p.toLowerAscii()
    if q.startsWith("*."):
      if h.endsWith(q[1 .. ^1]): return true
    elif h == q:
      return true
  false

proc isGitLike(scheme, host: string): bool =
  scheme.toLowerAscii() in GitSchemes or host.toLowerAscii() in GitHosts

proc isFullCommit(s: string): bool =
  (s.len == 40 or s.len == 64) and s.allCharsInSet(HexChars)

proc gitRef(u: Uri): string =
  if u.anchor.len > 0: return u.anchor
  # codeload tarballs are /owner/repo/tar.gz/<ref>
  let parts = u.path.split('/')
  if parts.len >= 2 and parts[^2] in ["tar.gz", "legacy.tar.gz", "zip"]: return parts[^1]
  ""

proc b64DecodedLen(s: string): int =
  # -1 when the string is not canonical padded base64
  if s.len == 0 or s.len mod 4 != 0: return -1
  var pad = 0
  for i, c in s:
    if c == '=':
      if i < s.len - 2: return -1
      inc pad
    elif c notin B64Chars or pad > 0:
      return -1
  s.len div 4 * 3 - pad

proc expectedDigestBytes(algo: string): int =
  case algo
  of "sha1": 20
  of "sha256": 32
  of "sha384": 48
  of "sha512": 64
  else: -1

# algo -> digest, plus the problems found while parsing
proc parseIntegrity(s: string): (Table[string, string], seq[string]) =
  var digests = initTable[string, string]()
  var problems: seq[string]
  for tok in s.splitWhitespace():
    let clean = tok.split('?')[0]
    let dash = clean.find('-')
    if dash <= 0:
      problems.add "token without an algorithm prefix: " & tok
      continue
    let algo = clean[0 ..< dash].toLowerAscii()
    let digest = clean[dash + 1 .. ^1]
    let want = expectedDigestBytes(algo)
    if want < 0:
      problems.add "unknown algorithm " & algo
    elif b64DecodedLen(digest) != want:
      problems.add algo & " digest is not " & $want & " bytes of valid base64"
    else:
      digests[algo] = digest
  (digests, problems)

proc report(fs: var seq[Finding], opt: Options, rule: string, sev: Severity,
            e: Entry, msg: string) =
  if rule in opt.ignore: return
  fs.add Finding(rule: rule, severity: sev, pkg: e.name & "@" & e.version,
                 path: e.path, message: msg)

proc checkTarballPath(fs: var seq[Finding], opt: Options, e: Entry, u: Uri) =
  let path = decodeUrl(u.path, decodePlus = false)
  let sep = path.find("/-/")
  if sep < 0 or e.name.len == 0 or e.version.len == 0: return
  let owner = path[0 ..< sep]
  let file = path[sep + 3 .. ^1]
  let bare = if e.name.startsWith("@") and e.name.contains('/'): e.name.split('/', 1)[1] else: e.name
  if not owner.toLowerAscii().endsWith(e.name.toLowerAscii()):
    fs.report(opt, "R005", sevError, e, "tarball path names `" & owner & "` but the lock entry is `" & e.name & "`")
  elif file.toLowerAscii() != (bare & "-" & e.version & ".tgz").toLowerAscii():
    fs.report(opt, "R005", sevError, e, "tarball file `" & file & "` does not match version " & e.version)

proc checkIntegrity(fs: var seq[Finding], opt: Options, e: Entry) =
  if e.integrity.len == 0:
    fs.report(opt, "R003", sevError, e, "resolved from the registry but no integrity hash is recorded")
    return
  let (digests, problems) = parseIntegrity(e.integrity)
  for p in problems:
    fs.report(opt, "R004", sevError, e, p)
  if problems.len == 0 and digests.len == 1 and "sha1" in digests:
    fs.report(opt, "R004", sevWarn, e, "only a sha1 digest is recorded")

proc isWorkspaceMember(e: Entry, layout: Layout): bool =
  # v2 and v3 paths for real installs always pass through node_modules
  layout == layPackages and not e.path.contains("node_modules") and
    e.resolved.len == 0 and e.integrity.len == 0

proc checkEntry(fs: var seq[Finding], opt: Options, layout: Layout, e: Entry) =
  if e.isLink or e.inBundle: return
  # workspace members live outside node_modules and have nothing to verify
  if e.isWorkspaceMember(layout): return
  if e.resolved.len == 0:
    if e.integrity.len == 0:
      fs.report(opt, "R003", sevWarn, e, "no resolved URL and no integrity hash recorded")
    return
  let u = parseUri(e.resolved)
  let scheme = u.scheme.toLowerAscii()
  if scheme == "file":
    if e.name notin opt.allowGit:
      fs.report(opt, "R006", sevWarn, e, "local file dependency: " & e.resolved)
    return
  if isGitLike(scheme, u.hostname):
    if e.name notin opt.allowGit:
      fs.report(opt, "R006", sevWarn, e, "git source: " & e.resolved)
    let r = gitRef(u)
    if not r.isFullCommit():
      fs.report(opt, "R007", sevError, e,
                if r.len == 0: "git source has no ref, it tracks the default branch"
                else: "git ref `" & r & "` is not a full commit hash, branches and tags can move")
    return
  if scheme != "https":
    fs.report(opt, "R002", sevError, e, "transport `" & (if scheme.len == 0: "none" else: scheme) & "` is not https: " & e.resolved)
  if u.hostname.len == 0 or not hostAllowed(u.hostname, opt.allowHosts):
    if e.name notin opt.allowGit:
      fs.report(opt, "R001", sevError, e, "host `" & u.hostname & "` is not on the allowlist")
  checkIntegrity(fs, opt, e)
  checkTarballPath(fs, opt, e, u)

proc checkConflicts(fs: var seq[Finding], opt: Options, lock: Lock) =
  # name@version -> (algo -> digest) first seen, and where
  var seen = initTable[string, (Table[string, string], string)]()
  for e in lock.entries:
    if e.isLink or e.integrity.len == 0 or e.version.len == 0: continue
    let (digests, problems) = parseIntegrity(e.integrity)
    if problems.len > 0: continue
    let key = e.name & "@" & e.version
    if key notin seen:
      seen[key] = (digests, e.path)
      continue
    let (prev, prevPath) = seen[key]
    for algo, d in digests:
      if algo in prev and prev[algo] != d:
        fs.report(opt, "R008", sevError, e,
                  "same " & key & " has a different " & algo & " digest at `" & prevPath & "`")
        break

proc coreVersion(v: string): seq[int] =
  var core = v.split({'-', '+'})[0]
  for part in core.split('.'):
    try:
      result.add parseInt(part)
    except ValueError:
      return @[]

proc isDowngrade(headV, baseV: string): bool =
  let a = coreVersion(headV)
  let b = coreVersion(baseV)
  if a.len == 0 or b.len == 0: return false
  for i in 0 ..< max(a.len, b.len):
    let x = if i < a.len: a[i] else: 0
    let y = if i < b.len: b[i] else: 0
    if x != y: return x < y
  false

proc hostOf(resolved: string): string =
  if resolved.len == 0: "" else: parseUri(resolved).hostname.toLowerAscii()

proc checkDiff(fs: var seq[Finding], opt: Options, base, head: Lock) =
  if base.layout != head.layout:
    fail("base uses the `" & $base.layout & "` layout and head uses `" & $head.layout &
         "`: regenerate both with the same npm major version before comparing")
  var byPath = initTable[string, Entry]()
  for e in base.entries: byPath[e.path] = e
  var scriptEntries: seq[Entry]
  for e in head.entries:
    if e.isLink: continue
    if e.path notin byPath:
      if e.hasInstallScript:
        scriptEntries.add e
        fs.report(opt, "R009", sevWarn, e, "new package runs an install script")
      continue
    let b = byPath[e.path]
    if b.version == e.version:
      if b.integrity.len > 0 and e.integrity.len > 0:
        let (bd, bp) = parseIntegrity(b.integrity)
        let (hd, hp) = parseIntegrity(e.integrity)
        if bp.len == 0 and hp.len == 0:
          for algo, d in hd:
            if algo in bd and bd[algo] != d:
              fs.report(opt, "R008", sevError, e,
                        "version is unchanged but the " & algo & " digest differs from the base lockfile")
              break
      if b.resolved != e.resolved and b.resolved.len > 0 and e.resolved.len > 0:
        if hostOf(b.resolved) != hostOf(e.resolved):
          fs.report(opt, "R010", sevError, e,
                    "version is unchanged but the host moved from " & hostOf(b.resolved) & " to " & hostOf(e.resolved))
        else:
          fs.report(opt, "R010", sevWarn, e, "version is unchanged but the resolved path changed")
    elif isDowngrade(e.version, b.version):
      fs.report(opt, "R011", sevWarn, e, "version went from " & b.version & " to " & e.version)
    if e.hasInstallScript and not b.hasInstallScript:
      scriptEntries.add e
      fs.report(opt, "R009", sevWarn, e, "package gained an install script")
  if opt.maxNewScripts >= 0 and scriptEntries.len > opt.maxNewScripts:
    fs.report(opt, "R009", sevError,
              Entry(name: "(lockfile)", version: $scriptEntries.len, path: opt.lockPath),
              $scriptEntries.len & " packages added install scripts, the cap is " & $opt.maxNewScripts)

proc analyze(head: Lock, base: Lock, hasBase: bool, opt: Options): seq[Finding] =
  for e in head.entries:
    result.checkEntry(opt, head.layout, e)
  result.checkConflicts(opt, head)
  if hasBase:
    result.checkDiff(opt, base, head)
  result.sort(proc (a, b: Finding): int =
    if a.severity != b.severity: return cmp(b.severity, a.severity)
    if a.rule != b.rule: return cmp(a.rule, b.rule)
    cmp(a.path, b.path))

proc count(fs: seq[Finding], sev: Severity): int =
  for f in fs:
    if f.severity == sev: inc result

proc render(fs: seq[Finding], opt: Options, scanned: int): string =
  if opt.jsonOut:
    var arr = newJArray()
    for f in fs:
      arr.add %*{"rule": f.rule, "severity": $f.severity, "package": f.pkg,
                 "path": f.path, "message": f.message}
    return pretty(%*{
      "tool": "LockfileTamperGate", "version": Version, "lockfile": opt.lockPath,
      "base": opt.basePath, "packagesScanned": scanned,
      "errors": fs.count(sevError), "warnings": fs.count(sevWarn), "findings": arr})
  var lines: seq[string]
  for f in fs:
    lines.add $f.severity & " " & f.rule & " " & f.pkg & "  " & f.message
    lines.add "      at " & f.path
  lines.add "scanned " & $scanned & " lock entries: " & $fs.count(sevError) & " errors, " &
            $fs.count(sevWarn) & " warnings"
  lines.join("\n")

const Usage = """LockfileTamperGate """ & Version & """

usage:
  LockfileTamperGate check <package-lock.json> [options]
  LockfileTamperGate rules
  LockfileTamperGate selftest

options (use the = form for values):
  --base=FILE                    lockfile from the target branch, enables diff rules
  --allow-host=HOST              extra registry host, repeatable, "*.example.com" allowed
  --allow-git=NAME               package allowed to come from git, a file or another host
  --ignore=RULE                  silence a rule id such as R006, repeatable
  --fail-on=error|warn           exit 1 at this severity or above (default error)
  --max-new-install-scripts=N    error when more than N packages add install scripts
  --format=text|json             output format (default text)

exit codes: 0 clean, 1 findings at or above --fail-on, 2 usage or parse error
"""

proc parseArgs(): (string, Options) =
  var opt = Options(allowHosts: @[DefaultRegistryHost], failOn: sevError, maxNewScripts: -1)
  var positional: seq[string]
  var p = initOptParser(commandLineParams())
  while true:
    p.next()
    case p.kind
    of cmdEnd: break
    of cmdArgument: positional.add p.key
    of cmdShortOption, cmdLongOption:
      case p.key
      of "base": opt.basePath = p.val
      of "allow-host": opt.allowHosts.add p.val
      of "allow-git": opt.allowGit.incl p.val
      of "ignore": opt.ignore.incl p.val.toUpperAscii()
      of "format":
        if p.val notin ["text", "json"]: fail("--format must be text or json")
        opt.jsonOut = p.val == "json"
      of "fail-on":
        case p.val
        of "error": opt.failOn = sevError
        of "warn": opt.failOn = sevWarn
        else: fail("--fail-on must be error or warn")
      of "max-new-install-scripts":
        try: opt.maxNewScripts = parseInt(p.val)
        except ValueError: fail("--max-new-install-scripts needs a whole number")
      of "help", "h": (stdout.write Usage; quit 0)
      else: fail("unknown option --" & p.key)
  if positional.len == 0: fail("missing command")
  if positional.len > 1: opt.lockPath = positional[1]
  (positional[0], opt)

# Self test: each case is a tiny lockfile and the rule ids it must trigger.
proc selfTest(): int =
  let good = "sha512-" & "A".repeat(86) & "=="
  let weak = "sha1-" & "A".repeat(27) & "="
  proc lock(pkgs: string): Lock =
    parseLock(parseJson("""{"lockfileVersion":3,"packages":{"":{"name":"root"},""" & pkgs & "}}"))
  proc has(fs: seq[Finding], rule: string, sev: Severity): bool =
    for f in fs:
      if f.rule == rule and f.severity == sev: return true
  let opt = Options(allowHosts: @[DefaultRegistryHost], failOn: sevError, maxNewScripts: 0)
  var failures = 0
  proc expect(label: string, ok: bool) =
    if not ok:
      inc failures
      stderr.writeLine "FAIL " & label
  let reg = "https://registry.npmjs.org/"
  let clean = lock("\"node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"" & reg &
                   "left/-/left-1.2.3.tgz\",\"integrity\":\"" & good & "\"}")
  expect("clean lock has no findings", analyze(clean, clean, false, opt).len == 0)
  let rogue = lock("\"node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"http://evil.test/left/-/left-1.2.3.tgz\",\"integrity\":\"" & good & "\"}")
  let rf = analyze(rogue, rogue, false, opt)
  expect("R001 rogue host", rf.has("R001", sevError))
  expect("R002 plain http", rf.has("R002", sevError))
  let noint = lock("\"node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"" & reg & "left/-/left-1.2.3.tgz\"}")
  expect("R003 missing integrity", analyze(noint, noint, false, opt).has("R003", sevError))
  let sha1 = lock("\"node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"" & reg & "left/-/left-1.2.3.tgz\",\"integrity\":\"" & weak & "\"}")
  expect("R004 sha1 only", analyze(sha1, sha1, false, opt).has("R004", sevWarn))
  let bad = lock("\"node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"" & reg & "left/-/left-1.2.3.tgz\",\"integrity\":\"sha512-short\"}")
  expect("R004 malformed", analyze(bad, bad, false, opt).has("R004", sevError))
  let swap = lock("\"node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"" & reg & "right/-/right-9.9.9.tgz\",\"integrity\":\"" & good & "\"}")
  expect("R005 swapped tarball", analyze(swap, swap, false, opt).has("R005", sevError))
  let scoped = lock("\"node_modules/@s/pkg\":{\"version\":\"2.0.0\",\"resolved\":\"" & reg & "@s/pkg/-/pkg-2.0.0.tgz\",\"integrity\":\"" & good & "\"}")
  expect("scoped tarball accepted", analyze(scoped, scoped, false, opt).len == 0)
  let git = lock("\"node_modules/g\":{\"version\":\"1.0.0\",\"resolved\":\"git+ssh://git@github.com/o/g.git#main\"}")
  let gf = analyze(git, git, false, opt)
  expect("R006 git source", gf.has("R006", sevWarn))
  expect("R007 branch ref", gf.has("R007", sevError))
  let pinned = lock("\"node_modules/g\":{\"version\":\"1.0.0\",\"resolved\":\"git+ssh://git@github.com/o/g.git#" & "a".repeat(40) & "\"}")
  expect("pinned git has no R007", not analyze(pinned, pinned, false, opt).has("R007", sevError))
  let dup = lock("\"node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"" & reg & "left/-/left-1.2.3.tgz\",\"integrity\":\"" & good &
                 "\"},\"node_modules/a/node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"" & reg & "left/-/left-1.2.3.tgz\",\"integrity\":\"sha512-" & "B".repeat(86) & "==\"}")
  expect("R008 conflicting digests", analyze(dup, dup, false, opt).has("R008", sevError))
  let tampered = lock("\"node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"" & reg & "left/-/left-1.2.3.tgz\",\"integrity\":\"sha512-" & "C".repeat(86) & "==\"}")
  expect("R008 diff tamper", analyze(tampered, clean, true, opt).has("R008", sevError))
  let moved = lock("\"node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"https://registry.npmjs.org.evil.test/left/-/left-1.2.3.tgz\",\"integrity\":\"" & good & "\"}")
  expect("R010 host moved", analyze(moved, clean, true, opt).has("R010", sevError))
  let script = lock("\"node_modules/left\":{\"version\":\"1.2.3\",\"resolved\":\"" & reg & "left/-/left-1.2.3.tgz\",\"integrity\":\"" & good & "\",\"hasInstallScript\":true}")
  let sf = analyze(script, clean, true, opt)
  expect("R009 install script warn", sf.has("R009", sevWarn))
  expect("R009 cap error", sf.has("R009", sevError))
  let older = lock("\"node_modules/left\":{\"version\":\"1.2.2\",\"resolved\":\"" & reg & "left/-/left-1.2.2.tgz\",\"integrity\":\"" & good & "\"}")
  expect("R011 downgrade", analyze(older, clean, true, opt).has("R011", sevWarn))
  let v1 = parseLock(parseJson("""{"lockfileVersion":1,"dependencies":{"a":{"version":"1.0.0","resolved":"http://registry.npmjs.org/a/-/a-1.0.0.tgz","integrity":"""" & good & """","dependencies":{"b":{"version":"2.0.0"}}}}}"""))
  expect("v1 layout flattened", v1.entries.len == 2 and v1.entries[1].path == "a>b")
  expect("v1 R002", analyze(v1, v1, false, opt).has("R002", sevError))
  expect("layout mismatch rejected", (try: (discard analyze(v1, clean, true, opt); false) except LockError: true))
  if failures == 0: echo "selftest ok"
  failures

proc main(): int =
  var cmd: string
  var opt: Options
  try:
    (cmd, opt) = parseArgs()
    case cmd
    of "selftest":
      return if selfTest() == 0: 0 else: 1
    of "rules":
      for r in Rules: echo r.id & " " & $r.severity & "  " & r.text
      return 0
    of "check":
      if opt.lockPath.len == 0: fail("check needs a lockfile path")
      let head = loadLock(opt.lockPath)
      var base: Lock
      if opt.basePath.len > 0: base = loadLock(opt.basePath)
      let findings = analyze(head, base, opt.basePath.len > 0, opt)
      echo render(findings, opt, head.entries.len)
      for f in findings:
        if f.severity >= opt.failOn: return 1
      return 0
    else:
      fail("unknown command `" & cmd & "`")
  except LockError as ex:
    stderr.writeLine "error: " & ex.msg
    stderr.write Usage
    return 2

when isMainModule:
  quit main()
