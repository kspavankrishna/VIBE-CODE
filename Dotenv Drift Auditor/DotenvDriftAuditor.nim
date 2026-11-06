# DotenvDriftAuditor.nim
# Build: nim c -d:release DotenvDriftAuditor.nim
import std/[os, strutils, tables, sets, algorithm, json, math, parseopt]

type
  Severity = enum
    sevInfo = "info", sevWarn = "warn", sevError = "error"

  Finding = object
    sev: Severity
    code: string
    file: string
    line: int
    key: string
    msg: string

  Entry = object
    key: string
    value: string
    line: int
    quote: char
    exported: bool
    expands: bool

  ParsedEnv = object
    path: string
    entries: seq[Entry]
    findings: seq[Finding]

  Usage = object
    file: string
    line: int

const
  MaxScanBytes = 2 * 1024 * 1024
  EntropyFloor = 3.6
  SecretMinLen = 20
  SkipDirs = ["node_modules", ".git", "vendor", "dist", "build", "target",
              "__pycache__", ".venv", "venv", ".next", ".nuxt", ".gradle",
              ".idea", "coverage", "out", "bin", "obj", ".terraform"]
  ScanExts = [".js", ".jsx", ".mjs", ".cjs", ".ts", ".tsx", ".py", ".rb",
              ".go", ".rs", ".java", ".kt", ".cs", ".php", ".sh", ".bash",
              ".zsh", ".yml", ".yaml", ".nim", ".ex", ".exs", ".swift",
              ".dart", ".lua", ".c", ".cc", ".cpp", ".h", ".hpp", ".scala"]
  ShellLikeExts = [".yml", ".yaml", ".sh", ".bash", ".zsh"]
  DefaultIgnore = ["PATH", "HOME", "USER", "SHELL", "PWD", "TERM", "LANG",
                   "LC_ALL", "TMPDIR", "TEMP", "TMP", "HOSTNAME", "CI",
                   "NODE_ENV", "DEBUG", "TZ", "LOGNAME", "EDITOR", "PAGER"]
  DefaultIgnorePrefixes = ["GITHUB_", "RUNNER_", "LC_", "npm_", "XDG_"]
  SecretHints = ["SECRET", "TOKEN", "PASSWORD", "PASSWD", "PRIVATE",
                 "APIKEY", "API_KEY", "CREDENTIAL", "ACCESS_KEY", "_KEY"]
  PlaceholderWords = ["changeme", "change_me", "change-me", "todo", "tbd",
                      "xxx", "xxxx", "secret", "password", "replace_me",
                      "replaceme", "none", "null", "undefined", "example",
                      "placeholder", "dummy", "test", "foo", "bar"]
  # prefix, mode. mode 0 = identifier follows directly, 1 = quoted name follows
  UsagePatterns = [
    ("process.env.", 0), ("process.env[", 1), ("import.meta.env.", 0),
    ("Deno.env.get(", 1), ("Bun.env.", 0),
    ("environ[", 1), ("environ.get(", 1), ("os.getenv(", 1),
    ("ENV[", 1), ("ENV.fetch(", 1),
    ("env::var(", 1), ("env!(", 1), ("option_env!(", 1),
    ("os.Getenv(", 1), ("os.LookupEnv(", 1),
    ("System.getenv(", 1), ("getenv(", 1),
    ("GetEnvironmentVariable(", 1), ("$_ENV[", 1),
    ("getEnv(", 1), ("System.get_env(", 1), ("System.fetch_env!(", 1),
    ("Platform.environment[", 1)
  ]

# ---------------------------------------------------------------- helpers

proc note(r: var ParsedEnv, sev: Severity, code: string, line: int,
          key, msg: string) =
  r.findings.add Finding(sev: sev, code: code, file: r.path, line: line,
                         key: key, msg: msg)

proc eolFrom(text: string, i: int): int =
  result = i
  while result < text.len and text[result] != '\n':
    inc result

proc isPortableKey(k: string): bool =
  if k.len == 0: return false
  if k[0] notin IdentStartChars: return false
  for c in k:
    if c notin IdentChars: return false
  true

proc hasExpansion(s: string): bool =
  var i = 0
  while i < s.len:
    if s[i] == '\\':
      i += 2
      continue
    if s[i] == '$' and i + 1 < s.len and
       (s[i + 1] == '{' or s[i + 1] in IdentStartChars):
      return true
    inc i
  false

proc shannon(s: string): float =
  if s.len == 0: return 0.0
  var counts: array[256, int]
  for c in s: inc counts[ord(c)]
  let n = float(s.len)
  for c in counts:
    if c > 0:
      let p = float(c) / n
      result -= p * log2(p)

proc isSecretish(key: string): bool =
  let u = key.toUpperAscii()
  for h in SecretHints:
    if h in u: return true
  false

proc isPlaceholder(v: string): bool =
  let l = v.strip().toLowerAscii()
  if l.len == 0: return false
  for w in PlaceholderWords:
    if l == w: return true
  if l.startsWith("your") or l.startsWith("example") or
     l.startsWith("replace") or l.startsWith("insert"):
    return true
  if l.len >= 2 and l[0] == '<' and l[^1] == '>': return true
  if "changeme" in l or "change_me" in l or "replace_me" in l: return true
  if l.len >= 3:
    var allMask = true
    for c in l:
      if c notin {'x', '*', '.', '-', '_'}:
        allMask = false
        break
    if allMask: return true
  false

proc urlPassword(v: string): string =
  ## Password part of scheme://user:password@host, or "" when absent.
  let s = v.find("://")
  if s < 0: return ""
  let rest = v[s + 3 .. ^1]
  var slash = rest.find('/')
  if slash < 0: slash = rest.len
  let authority = rest[0 ..< slash]
  let at = authority.rfind('@')
  if at <= 0: return ""
  let userinfo = authority[0 ..< at]
  let colon = userinfo.find(':')
  if colon < 0: return ""
  userinfo[colon + 1 .. ^1]

proc looksLikeRealSecret(key, value: string): bool =
  if value.len == 0 or isPlaceholder(value): return false
  if hasExpansion(value): return false
  let pw = urlPassword(value)
  if pw.len >= 8 and not isPlaceholder(pw) and not hasExpansion(pw):
    return true
  if not isSecretish(key): return false
  if value.len < SecretMinLen or ' ' in value: return false
  shannon(value) >= EntropyFloor

# ------------------------------------------------------------------ parser

proc parseEnv(path, raw: string): ParsedEnv =
  ## Dialect: KEY=value, optional "export ", # comments, single quotes literal,
  ## double quotes with \n \t \r \" \\ escapes and multiline values, backticks
  ## literal. Unquoted values end at EOL or at " #".
  result.path = path
  var text = raw
  if text.len >= 3 and text[0] == '\xEF' and text[1] == '\xBB' and
     text[2] == '\xBF':
    text = text[3 .. ^1]
    result.note(sevInfo, "I201", 1, "",
      "file starts with a UTF-8 BOM, some parsers glue it onto the first key")
  if "\r\n" in text:
    text = text.replace("\r\n", "\n")
    result.note(sevInfo, "I202", 1, "",
      "file uses CRLF line endings, shell source and some parsers keep the CR")
  let n = text.len
  var i = 0
  var line = 1
  var firstLine = initTable[string, int]()
  var firstVal = initTable[string, string]()
  while i < n:
    let startLine = line
    var j = i
    while j < n and text[j] in {' ', '\t'}: inc j
    if j >= n: break
    if text[j] == '\n':
      i = j + 1
      inc line
      continue
    if text[j] == '#':
      i = eolFrom(text, j) + 1
      inc line
      continue
    var p = j
    var exported = false
    if text.continuesWith("export", p) and p + 6 < n and
       text[p + 6] in {' ', '\t'}:
      exported = true
      p += 6
      while p < n and text[p] in {' ', '\t'}: inc p
    let ks = p
    while p < n and text[p] notin {'=', '\n', ' ', '\t', '#'}: inc p
    let key = text[ks ..< p]
    var q = p
    while q < n and text[q] in {' ', '\t'}: inc q
    if key.len == 0 or q >= n or text[q] != '=':
      result.note(sevError, "E002", startLine, key,
        "line has no KEY=value shape, every parser will skip or reject it")
      i = eolFrom(text, ks) + 1
      inc line
      continue
    if q > p:
      result.note(sevWarn, "W102", startLine, key,
        "space before '=', shell source fails on this and parsers disagree")
    inc q
    let afterEq = q
    while q < n and text[q] in {' ', '\t'}: inc q
    var value = ""
    var quote = '\0'
    var expands = false
    var lineEnd = q
    if q < n and text[q] in {'"', '\'', '`'}:
      quote = text[q]
      var r = q + 1
      var closed = false
      var sb = ""
      while r < n:
        let c = text[r]
        if c == '\\' and quote == '"' and r + 1 < n:
          let d = text[r + 1]
          case d
          of 'n': sb.add '\n'
          of 't': sb.add '\t'
          of 'r': sb.add '\r'
          of '"': sb.add '"'
          of '\\': sb.add '\\'
          of '$': sb.add '$'
          else:
            sb.add c
            sb.add d
          if d == '\n': inc line
          r += 2
          continue
        if c == quote:
          closed = true
          break
        if c == '\n': inc line
        sb.add c
        inc r
      if not closed:
        result.note(sevError, "E001", startLine, key,
          "quote opened here is never closed, parsers swallow following lines")
        i = eolFrom(text, q) + 1
        line = startLine + 1
        continue
      value = sb
      if quote == '"': expands = hasExpansion(text[q + 1 ..< r])
      var t = r + 1
      while t < n and text[t] in {' ', '\t'}: inc t
      if t < n and text[t] != '\n' and text[t] != '#':
        result.note(sevWarn, "W105", line, key,
          "text after the closing quote, parsers keep or drop it differently")
      lineEnd = eolFrom(text, t)
    else:
      let e = eolFrom(text, q)
      var seg = text[q ..< e]
      var cut = -1
      for k in 0 ..< seg.len:
        if seg[k] == '#' and (k == 0 or seg[k - 1] in {' ', '\t'}):
          cut = k
          break
      if cut >= 0: seg = seg[0 ..< cut]
      seg = seg.strip(leading = false)
      if '#' in seg:
        result.note(sevInfo, "I204", startLine, key,
          "'#' without a space before it stays in the value here, " &
          "some parsers cut there")
      if ' ' in seg or '\t' in seg:
        result.note(sevWarn, "W103", startLine, key,
          "unquoted value contains spaces, shell source will split it")
      if seg.endsWith("\\"):
        result.note(sevWarn, "W110", startLine, key,
          "value ends with a backslash, some parsers treat it as a continuation")
      if q > afterEq and seg.len > 0:
        result.note(sevWarn, "W102", startLine, key,
          "space after '=', shell source runs the value as a command")
      value = seg
      expands = hasExpansion(seg)
      lineEnd = e
    if not isPortableKey(key):
      result.note(sevWarn, "W104", startLine, key,
        "key is not [A-Za-z_][A-Za-z0-9_]*, shells and many runtimes drop it")
    if exported:
      result.note(sevInfo, "I206", startLine, key,
        "'export' prefix works for shell source, compose ignores it")
    if expands:
      result.note(sevInfo, "I203", startLine, key,
        "value expands $VAR, compose and python-dotenv do, node dotenv does not")
    if firstLine.hasKey(key):
      if firstVal[key] != value:
        result.note(sevError, "E003", startLine, key,
          "duplicate key with a different value, first at line " &
          $firstLine[key] & ", last wins in compose, first wins in some tools")
      else:
        result.note(sevWarn, "W101", startLine, key,
          "duplicate key with the same value, first at line " & $firstLine[key])
    else:
      firstLine[key] = startLine
      firstVal[key] = value
    result.entries.add Entry(key: key, value: value, line: startLine,
                             quote: quote, exported: exported, expands: expands)
    i = lineEnd + 1
    inc line

proc lastValues(p: ParsedEnv): Table[string, Entry] =
  result = initTable[string, Entry]()
  for e in p.entries: result[e.key] = e

# ------------------------------------------------------------- comparisons

proc isRequired(e: Entry): bool =
  e.value.len == 0 or isPlaceholder(e.value)

proc checkReference(ref0: ParsedEnv, out0: var seq[Finding]) =
  for e in ref0.entries:
    if looksLikeRealSecret(e.key, e.value):
      out0.add Finding(sev: sevError, code: "E006", file: ref0.path,
        line: e.line, key: e.key,
        msg: "example file holds a value that looks like a real secret " &
             "(length " & $e.value.len & ", entropy " &
             formatFloat(shannon(e.value), ffDecimal, 2) & " bits per char)")

proc compareEnv(ref0, env: ParsedEnv, out0: var seq[Finding]) =
  let refMap = lastValues(ref0)
  let envMap = lastValues(env)
  for key, re in refMap:
    if not envMap.hasKey(key):
      if isRequired(re):
        out0.add Finding(sev: sevError, code: "E005", file: env.path, line: 0,
          key: key, msg: "required key from " & ref0.path & " is missing")
      else:
        out0.add Finding(sev: sevWarn, code: "W106", file: env.path, line: 0,
          key: key, msg: "key from " & ref0.path &
          " is missing, the example default will not apply unless code has one")
      continue
    let ee = envMap[key]
    if ee.value.len == 0 and isRequired(re) and re.value.len > 0:
      out0.add Finding(sev: sevWarn, code: "W111", file: env.path,
        line: ee.line, key: key, msg: "value is empty but the example marks it required")
    elif ee.value.len == 0 and re.value.len == 0:
      out0.add Finding(sev: sevWarn, code: "W111", file: env.path,
        line: ee.line, key: key, msg: "value is empty, the example requires one")
    elif ee.value.len > 0 and isPlaceholder(ee.value):
      out0.add Finding(sev: sevError, code: "E004", file: env.path,
        line: ee.line, key: key, msg: "placeholder value was never replaced")
    elif ee.value.len > 0 and isSecretish(key) and ee.value == re.value:
      out0.add Finding(sev: sevWarn, code: "W108", file: env.path,
        line: ee.line, key: key,
        msg: "secret style key reuses the exact value from the example file")
  for key, ee in envMap:
    if not refMap.hasKey(key):
      out0.add Finding(sev: sevWarn, code: "W107", file: env.path,
        line: ee.line, key: key,
        msg: "key is not documented in " & ref0.path)

# ------------------------------------------------------------ usage scanner

proc isEnvName(s: string): bool =
  if s.len < 2: return false
  if s[0] notin {'A' .. 'Z', '_'}: return false
  for c in s:
    if c notin {'A' .. 'Z', '0' .. '9', '_'}: return false
  true

proc lineStarts(text: string): seq[int] =
  result = @[0]
  for i, c in text:
    if c == '\n': result.add i + 1

proc lineAt(starts: seq[int], pos: int): int =
  starts.upperBound(pos) # 1 based line number

proc readName(text: string, start, mode: int): string =
  var p = start
  if mode == 1:
    while p < text.len and text[p] in {' ', '\t'}: inc p
    if p >= text.len or text[p] notin {'"', '\'', '`'}: return ""
    let q = text[p]
    inc p
    let s = p
    while p < text.len and text[p] != q and text[p] != '\n': inc p
    if p >= text.len or text[p] != q: return ""
    return text[s ..< p]
  let s = p
  while p < text.len and text[p] in {'A' .. 'Z', '0' .. '9', '_'}: inc p
  if p < text.len and text[p] in {'a' .. 'z'}: return ""
  text[s ..< p]

proc scanText(rel, text: string, shellLike: bool,
              found: var Table[string, seq[Usage]]) =
  let starts = lineStarts(text)
  var seen = initHashSet[string]()
  proc record(name: string, pos: int, seen: var HashSet[string],
              found: var Table[string, seq[Usage]]) =
    if not isEnvName(name): return
    let ln = lineAt(starts, pos)
    let id = name & ":" & $ln
    if id in seen: return
    seen.incl id
    found.mgetOrPut(name, @[]).add Usage(file: rel, line: ln)
  for (prefix, mode) in UsagePatterns:
    var at = text.find(prefix)
    while at >= 0:
      let before = if at > 0: text[at - 1] else: ' '
      if before notin IdentChars:
        let name = readName(text, at + prefix.len, mode)
        record(name, at, seen, found)
      at = text.find(prefix, at + prefix.len)
  if shellLike:
    var at = text.find("${")
    while at >= 0:
      let before = if at > 0: text[at - 1] else: ' '
      if before != '$':
        let name = readName(text, at + 2, 0)
        let after = at + 2 + name.len
        if name.len > 0 and after < text.len and
           text[after] in {'}', ':', '-', '?', '+', '='}:
          record(name, at, seen, found)
      at = text.find("${", at + 2)

proc walkScan(dir, root: string, skipPath: HashSet[string], maxBytes: int,
              found: var Table[string, seq[Usage]], scanned: var int) =
  var entries: seq[(PathComponent, string)] = @[]
  try:
    for kind, path in walkDir(dir):
      entries.add (kind, path)
  except OSError:
    return
  entries.sort(proc(a, b: (PathComponent, string)): int = cmp(a[1], b[1]))
  for (kind, path) in entries:
    let (_, name, ext) = splitFile(path)
    if kind == pcDir:
      if name in SkipDirs: continue
      walkScan(path, root, skipPath, maxBytes, found, scanned)
    elif kind == pcFile:
      if path in skipPath or name.startsWith(".env"): continue
      let isDockerfile = name.startsWith("Dockerfile")
      if not isDockerfile and ext.toLowerAscii() notin ScanExts: continue
      var size = 0'i64
      try: size = getFileSize(path)
      except OSError: continue
      if size > maxBytes: continue
      var text = ""
      try: text = readFile(path)
      except IOError: continue
      if '\0' in text: continue
      inc scanned
      let shellLike = isDockerfile or ext.toLowerAscii() in ShellLikeExts
      scanText(relativePath(path, root), text, shellLike, found)

# ------------------------------------------------------------------ output

proc cmpFinding(a, b: Finding): int =
  result = cmp(a.file, b.file)
  if result == 0: result = cmp(a.line, b.line)
  if result == 0: result = cmp(a.code, b.code)
  if result == 0: result = cmp(a.key, b.key)

proc renderText(fs: seq[Finding]): string =
  for f in fs:
    let loc = if f.line > 0: f.file & ":" & $f.line else: f.file
    let k = if f.key.len > 0: " " & f.key else: ""
    result.add toUpperAscii($f.sev) & " " & f.code & " " & loc & k & ": " &
               f.msg & "\n"

proc renderJson(fs: seq[Finding], scanned: int): string =
  var arr = newJArray()
  var errs, warns, infos = 0
  for f in fs:
    case f.sev
    of sevError: inc errs
    of sevWarn: inc warns
    of sevInfo: inc infos
    arr.add(%*{"severity": $f.sev, "code": f.code, "file": f.file,
               "line": f.line, "key": f.key, "message": f.msg})
  let doc = %*{"summary": {"errors": errs, "warnings": warns, "info": infos,
                           "filesScanned": scanned}, "findings": arr}
  pretty(doc) & "\n"

const HelpText = """
DotenvDriftAuditor: lint dotenv files, diff them against a reference and
cross check keys against the code that reads them. Values are never printed.

Usage:
  DotenvDriftAuditor --reference .env.example [options] [ENV_FILE ...]

Options:
  -r, --reference FILE   example file that documents every key
  -e, --env FILE         env file to audit, repeatable (or pass as arguments)
  -s, --scan DIR         scan source under DIR for env reads, repeatable
      --ignore NAME      env name to leave out of usage checks, repeatable
      --ignore-prefix P  env name prefix to leave out of usage checks
      --no-dead          do not report documented keys that code never reads
      --max-file-kb N    skip source files larger than N KiB (default 2048)
      --fail-on LEVEL    error (default), warn or never
      --json             machine readable output
      --self-test        run the built in parser checks and exit
  -h, --help             show this text

Exit codes: 0 clean for the chosen level, 1 findings, 2 usage or read error.
"""

proc selfTest(): int =
  var fails = 0
  proc check(name: string, cond: bool) =
    if not cond:
      echo "FAIL ", name
      inc fails
  let a = parseEnv("t", "A=1\nB = 2\nexport C=\"x y\"\nD='$HOME'\nE=v # c\nA=3\n")
  check "entries", a.entries.len == 6
  var codes: seq[string] = @[]
  for f in a.findings: codes.add f.code
  check "dup differs", "E003" in codes
  check "space before eq", "W102" in codes
  check "export info", "I206" in codes
  check "single quote literal", a.entries[3].value == "$HOME" and
    not a.entries[3].expands
  check "inline comment cut", a.entries[4].value == "v"
  let b = parseEnv("t", "K=\"open\nZ=1\n")
  check "unterminated", b.findings.len > 0 and b.findings[0].code == "E001"
  let c = parseEnv("t", "M=\"line1\nline2\"\nN=2\n")
  check "multiline", c.entries.len == 2 and c.entries[1].line == 3
  check "placeholder", isPlaceholder("<your token>") and not isPlaceholder("a1b2")
  var found = initTable[string, seq[Usage]]()
  scanText("x.ts", "const a = process.env.API_HOST; os.getenv('DB_URL')", false, found)
  check "usage scan", found.hasKey("API_HOST") and found.hasKey("DB_URL")
  if fails == 0: echo "self-test ok"
  fails

# -------------------------------------------------------------------- main

proc main(): int =
  var reference = ""
  var envFiles: seq[string] = @[]
  var scanDirs: seq[string] = @[]
  var ignore = initHashSet[string]()
  var ignorePrefixes: seq[string] = @[]
  for n in DefaultIgnore: ignore.incl n
  for p in DefaultIgnorePrefixes: ignorePrefixes.add p
  var asJson = false
  var reportDead = true
  var failOn = "error"
  var maxBytes = MaxScanBytes
  var p = initOptParser(commandLineParams(), shortNoVal = {'h'},
    longNoVal = @["json", "help", "no-dead", "self-test"])
  for kind, key, val in p.getopt():
    case kind
    of cmdArgument: envFiles.add key
    of cmdLongOption, cmdShortOption:
      case key
      of "reference", "r": reference = val
      of "env", "e": envFiles.add val
      of "scan", "s": scanDirs.add val
      of "ignore": ignore.incl val
      of "ignore-prefix": ignorePrefixes.add val
      of "no-dead": reportDead = false
      of "json": asJson = true
      of "fail-on": failOn = val
      of "max-file-kb":
        try: maxBytes = parseInt(val) * 1024
        except ValueError:
          stderr.writeLine "bad --max-file-kb value"
          return 2
      of "self-test": return (if selfTest() == 0: 0 else: 1)
      of "help", "h":
        stdout.write HelpText
        return 0
      else:
        stderr.writeLine "unknown option: " & key
        return 2
    of cmdEnd: discard
  if failOn notin ["error", "warn", "never"]:
    stderr.writeLine "--fail-on must be error, warn or never"
    return 2
  if reference.len == 0 and envFiles.len == 0:
    stderr.write HelpText
    return 2
  if scanDirs.len > 0 and reference.len == 0:
    stderr.writeLine "--scan needs --reference so usage has something to compare with"
    return 2

  var findings: seq[Finding] = @[]
  var refParsed: ParsedEnv
  if reference.len > 0:
    try: refParsed = parseEnv(reference, readFile(reference))
    except IOError:
      stderr.writeLine "cannot read reference file: " & reference
      return 2
    findings.add refParsed.findings
    checkReference(refParsed, findings)
  var skipPaths = initHashSet[string]()
  if reference.len > 0: skipPaths.incl reference
  for path in envFiles:
    var parsed: ParsedEnv
    try: parsed = parseEnv(path, readFile(path))
    except IOError:
      stderr.writeLine "cannot read env file: " & path
      return 2
    skipPaths.incl path
    findings.add parsed.findings
    if reference.len > 0: compareEnv(refParsed, parsed, findings)

  var scanned = 0
  if scanDirs.len > 0:
    var found = initTable[string, seq[Usage]]()
    for d in scanDirs:
      if not dirExists(d):
        stderr.writeLine "scan directory not found: " & d
        return 2
      walkScan(d, d, skipPaths, maxBytes, found, scanned)
    let documented = lastValues(refParsed)
    proc ignored(name: string): bool =
      if name in ignore: return true
      for pre in ignorePrefixes:
        if name.startsWith(pre): return true
      false
    for name, uses in found:
      if ignored(name) or documented.hasKey(name): continue
      var where0 = ""
      for k in 0 ..< min(3, uses.len):
        if k > 0: where0.add ", "
        where0.add uses[k].file & ":" & $uses[k].line
      findings.add Finding(sev: sevWarn, code: "W109", file: reference,
        line: 0, key: name,
        msg: "read in code (" & where0 & ") but not documented here")
    if reportDead:
      for key, e in documented:
        if not found.hasKey(key) and not ignored(key):
          findings.add Finding(sev: sevInfo, code: "I205", file: reference,
            line: e.line, key: key,
            msg: "documented but no scanned source reads it")

  findings.sort(cmpFinding)
  if asJson: stdout.write renderJson(findings, scanned)
  else:
    stdout.write renderText(findings)
    var errs, warns = 0
    for f in findings:
      if f.sev == sevError: inc errs
      elif f.sev == sevWarn: inc warns
    stdout.write "\n" & $errs & " errors, " & $warns & " warnings, " &
      $(findings.len - errs - warns) & " info, " & $scanned &
      " source files scanned\n"
  var bad = false
  for f in findings:
    if failOn == "error" and f.sev == sevError: bad = true
    if failOn == "warn" and f.sev >= sevWarn: bad = true
  result = if bad: 1 else: 0

when isMainModule:
  quit(main())
