import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamReader
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant

class Exit extends RuntimeException {
    final int code
    Exit(int code, String message) {
        super(message)
        this.code = code
    }
}

class RunSpec {
    String commit
    Path path
}

class Config {
    static final int EXIT_OK = 0
    static final int EXIT_BLOCKING = 1
    static final int EXIT_USAGE = 2
    static final int EXIT_INTEGRITY = 3
    static final int EXIT_COVERAGE = 4

    List<RunSpec> runs = []
    Path ledger
    boolean write = true
    String format = 'text'
    Path out
    Path quarantineOut
    int window = 50
    int minFlakyEvidence = 2
    int brokenStreak = 3
    int releaseStreak = 20
    int flipLimit = 4
    int expireDays = 60
    double vanishRatio = 0.25d
    boolean strictCoverage = false
    boolean distinctDuplicates = false
    boolean failOnNewFlaky = false
    boolean allowUnreadable = false
    Instant now = Instant.now()

    static final Set<String> FORMATS = ['text', 'json', 'markdown', 'github'] as Set

    static String usage() {
        return '''\
JUnitFlakeLedger: tell flaky tests from broken ones across CI runs, keep a ledger, gate on real failures.

Usage:
  groovy JUnitFlakeLedger.groovy --ledger flake-ledger.json --run <commit>=<reports> [--run ...] [options]

Required:
  --ledger <file>              Ledger JSON. Created on first run. Keep one per branch you care about.
  --run <commit>=<path>        A directory (searched recursively) or a single JUnit XML file.
                               Repeat for several attempts of one commit or for several commits.
                               The LAST --run is the head commit the gate judges.

Options:
  --format text|json|markdown|github   Report style (default text)
  --out <file>                 Write the report here instead of stdout
  --quarantine-out <file>      Write quarantined test ids, one per line
  --window <n>                 Observations kept per test (default 50)
  --min-flaky-evidence <n>     Commits with mixed outcomes needed to call a test flaky (default 2)
  --broken-streak <n>          Consecutive failing commits that make a test broken (default 3)
  --release-streak <n>         Clean passes in a row before a quarantine is lifted (default 20)
  --flip-limit <n>             Pass/fail flips across commits that make a test suspect (default 4)
  --expire-days <n>            Forget tests not seen for this long (default 60)
  --max-vanish-ratio <r>       Share of last commit's tests allowed to vanish (default 0.25)
  --strict-coverage            Exit 4 when more than that share vanished
  --distinct-duplicates        Treat repeated test ids in one run as separate tests, not retries
  --fail-on-new-flaky          Exit 1 when a test was newly quarantined in this invocation
  --allow-unreadable           Keep going when a report file is malformed or truncated
  --now <iso-instant>          Override the clock, for replays and tests
  --no-write                   Analyse only, leave the ledger untouched

Exit codes:
  0 clean   1 blocking failure   2 usage or ledger error   3 report integrity   4 coverage drop
'''
    }

    static Config parse(String[] args) {
        Config c = new Config()
        Set<String> flags = ['no-write', 'strict-coverage', 'distinct-duplicates', 'fail-on-new-flaky',
                             'allow-unreadable', 'help'] as Set
        int i = 0
        while (i < args.length) {
            String a = args[i++]
            if (!a.startsWith('--')) {
                throw new Exit(Config.EXIT_USAGE, "Unexpected argument: ${a}")
            }
            String key = a.substring(2)
            String val = null
            int eq = key.indexOf('=')
            if (eq >= 0) {
                val = key.substring(eq + 1)
                key = key.substring(0, eq)
            }
            if (flags.contains(key)) {
                if (val != null) {
                    throw new Exit(Config.EXIT_USAGE, "--${key} does not take a value")
                }
            } else if (val == null) {
                if (i >= args.length) {
                    throw new Exit(Config.EXIT_USAGE, "--${key} needs a value")
                }
                val = args[i++]
            }
            switch (key) {
                case 'help': throw new Exit(Config.EXIT_OK, usage())
                case 'ledger': c.ledger = Paths.get(val); break
                case 'run': c.runs << parseRun(val); break
                case 'format':
                    if (!FORMATS.contains(val)) {
                        throw new Exit(Config.EXIT_USAGE, "--format must be one of ${FORMATS.sort().join(', ')}")
                    }
                    c.format = val
                    break
                case 'out': c.out = Paths.get(val); break
                case 'quarantine-out': c.quarantineOut = Paths.get(val); break
                case 'window': c.window = positiveInt(key, val); break
                case 'min-flaky-evidence': c.minFlakyEvidence = positiveInt(key, val); break
                case 'broken-streak': c.brokenStreak = positiveInt(key, val); break
                case 'release-streak': c.releaseStreak = positiveInt(key, val); break
                case 'flip-limit': c.flipLimit = positiveInt(key, val); break
                case 'expire-days': c.expireDays = positiveInt(key, val); break
                case 'max-vanish-ratio':
                    try {
                        c.vanishRatio = Double.parseDouble(val)
                    } catch (NumberFormatException ignored) {
                        throw new Exit(Config.EXIT_USAGE, "--max-vanish-ratio needs a number, got '${val}'")
                    }
                    if (c.vanishRatio < 0d || c.vanishRatio > 1d) {
                        throw new Exit(Config.EXIT_USAGE, '--max-vanish-ratio must be between 0 and 1')
                    }
                    break
                case 'strict-coverage': c.strictCoverage = true; break
                case 'distinct-duplicates': c.distinctDuplicates = true; break
                case 'fail-on-new-flaky': c.failOnNewFlaky = true; break
                case 'allow-unreadable': c.allowUnreadable = true; break
                case 'no-write': c.write = false; break
                case 'now':
                    try {
                        c.now = Instant.parse(val)
                    } catch (Exception ignored) {
                        throw new Exit(Config.EXIT_USAGE, "--now needs an ISO instant like 2026-01-01T00:00:00Z, got '${val}'")
                    }
                    break
                default: throw new Exit(Config.EXIT_USAGE, "Unknown option --${key}")
            }
        }
        if (c.ledger == null) {
            throw new Exit(Config.EXIT_USAGE, "--ledger is required\n\n" + usage())
        }
        if (c.runs.isEmpty()) {
            throw new Exit(Config.EXIT_USAGE, "At least one --run is required\n\n" + usage())
        }
        return c
    }

    static int positiveInt(String key, String val) {
        try {
            int n = Integer.parseInt(val)
            if (n < 1) {
                throw new NumberFormatException()
            }
            return n
        } catch (NumberFormatException ignored) {
            throw new Exit(Config.EXIT_USAGE, "--${key} needs a positive integer, got '${val}'")
        }
    }

    static RunSpec parseRun(String raw) {
        int eq = raw.indexOf('=')
        if (eq <= 0 || eq == raw.length() - 1) {
            throw new Exit(Config.EXIT_USAGE, "--run must look like <commit>=<path>, got '${raw}'")
        }
        String commit = raw.substring(0, eq)
        if (!(commit ==~ /[A-Za-z0-9._\/@+-]{1,120}/)) {
            throw new Exit(Config.EXIT_USAGE, "Commit label '${commit}' may only use letters, digits and . _ / @ + -")
        }
        return new RunSpec(commit: commit, path: Paths.get(raw.substring(eq + 1)))
    }
}

class TestCase {
    String id
    String outcome
}

class ParsedFile {
    Path path
    List<TestCase> cases = []
    boolean report = true
    String error
}

class ReportParser {
    static final Set<String> FAIL_TAGS = ['failure', 'error'] as Set
    static final Set<String> FLAKY_TAGS = ['flakyFailure', 'flakyError'] as Set
    static final Set<String> RERUN_TAGS = ['rerunFailure', 'rerunError'] as Set

    static XMLInputFactory factory() {
        XMLInputFactory f = XMLInputFactory.newFactory()
        f.setProperty(XMLInputFactory.SUPPORT_DTD, false)
        f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
        f.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false)
        return f
    }

    static List<Path> discover(Path root) {
        if (!Files.exists(root)) {
            throw new Exit(Config.EXIT_INTEGRITY, "Report path does not exist: ${root}")
        }
        if (Files.isRegularFile(root)) {
            return [root]
        }
        List<Path> found = []
        Files.walk(root).withCloseable { stream ->
            stream.filter { Path p -> Files.isRegularFile(p, java.nio.file.LinkOption.NOFOLLOW_LINKS) }
                    .filter { Path p -> p.fileName.toString().toLowerCase().endsWith('.xml') }
                    .forEach { Path p -> found << p }
        }
        found.sort { Path p -> root.relativize(p).toString() }
        return found
    }

    static ParsedFile parse(Path path) {
        ParsedFile result = new ParsedFile(path: path)
        XMLStreamReader r = null
        InputStream is = null
        try {
            is = Files.newInputStream(path)
            r = factory().createXMLStreamReader(is)
            boolean sawRoot = false
            List<String> suiteStack = []
            String cname = null
            String tname = null
            int depth = -1
            boolean fail = false
            boolean flaky = false
            boolean rerun = false
            boolean skipped = false
            boolean inCase = false
            while (r.hasNext()) {
                int ev = r.next()
                if (ev == XMLStreamConstants.START_ELEMENT) {
                    String n = r.getLocalName()
                    if (!sawRoot) {
                        sawRoot = true
                        if (n != 'testsuite' && n != 'testsuites') {
                            result.report = false
                            return result
                        }
                    }
                    if (inCase) {
                        if (depth == 0) {
                            if (FAIL_TAGS.contains(n)) fail = true
                            else if (FLAKY_TAGS.contains(n)) flaky = true
                            else if (RERUN_TAGS.contains(n)) rerun = true
                            else if (n == 'skipped') skipped = true
                        }
                        depth++
                    } else if (n == 'testsuite') {
                        suiteStack << (r.getAttributeValue(null, 'name') ?: '')
                    } else if (n == 'testcase') {
                        inCase = true
                        depth = 0
                        fail = false
                        flaky = false
                        rerun = false
                        skipped = false
                        cname = r.getAttributeValue(null, 'classname')
                        tname = r.getAttributeValue(null, 'name')
                    }
                } else if (ev == XMLStreamConstants.END_ELEMENT) {
                    String n = r.getLocalName()
                    if (inCase) {
                        if (n == 'testcase' && depth == 0) {
                            inCase = false
                            String suite = suiteStack ? suiteStack.last() : ''
                            String cls = (cname ?: '').trim() ?: suite.trim()
                            String nm = (tname ?: '').trim()
                            if (nm) {
                                String outcome
                                if (skipped && !fail) outcome = 'S'
                                else if (fail) outcome = 'F'
                                else if (flaky) outcome = 'R'
                                else if (rerun) outcome = 'F'
                                else outcome = 'P'
                                result.cases << new TestCase(id: cls ? "${cls}#${nm}".toString() : nm, outcome: outcome)
                            }
                        } else {
                            depth--
                        }
                    } else if (n == 'testsuite' && suiteStack) {
                        suiteStack.remove(suiteStack.size() - 1)
                    }
                }
            }
            if (!sawRoot) {
                result.report = false
            } else if (inCase) {
                throw new IllegalStateException('file ended inside a testcase')
            }
        } catch (Exception e) {
            result.error = "${e.class.simpleName}: ${(e.message ?: '').split('\n')[0]}".toString()
            result.cases = []
        } finally {
            try { r?.close() } catch (Exception ignored) { }
            try { is?.close() } catch (Exception ignored) { }
        }
        return result
    }

    static String fingerprint(String commit, Path root, List<Path> files) {
        MessageDigest outer = MessageDigest.getInstance('SHA-256')
        outer.update(commit.getBytes(StandardCharsets.UTF_8))
        Path base = Files.isDirectory(root) ? root : root.parent
        files.each { Path p ->
            MessageDigest inner = MessageDigest.getInstance('SHA-256')
            Files.newInputStream(p).withCloseable { InputStream s ->
                byte[] buf = new byte[65536]
                int n
                while ((n = s.read(buf)) > 0) {
                    inner.update(buf, 0, n)
                }
            }
            String rel = base == null ? p.fileName.toString() : base.relativize(p).toString()
            outer.update((byte) 0)
            outer.update(rel.getBytes(StandardCharsets.UTF_8))
            outer.update((byte) 0)
            outer.update(inner.digest())
        }
        return outer.digest().encodeHex().toString()
    }

    /** Collapse repeated ids inside one run. A retry plugin writes the same test several times. */
    static Map<String, String> fold(List<TestCase> cases, boolean distinct) {
        Map<String, List<String>> byId = new LinkedHashMap<>()
        Map<String, Integer> seen = new HashMap<>()
        cases.each { TestCase c ->
            if (c.outcome == 'S') return
            String id = c.id
            if (distinct) {
                int k = (seen[id] ?: 0) + 1
                seen[id] = k
                if (k > 1) id = "${c.id} [${k}]".toString()
            }
            byId.computeIfAbsent(id) { new ArrayList<String>() } << c.outcome
        }
        Map<String, String> folded = new LinkedHashMap<>()
        byId.each { String id, List<String> outs ->
            boolean anyFail = outs.contains('F')
            boolean anyPass = outs.contains('P') || outs.contains('R')
            if (outs.contains('R') || (anyFail && anyPass)) folded[id] = 'R'
            else folded[id] = anyFail ? 'F' : 'P'
        }
        return folded
    }
}

class Ledger {
    static final int SCHEMA = 1
    static final int MAX_FINGERPRINTS = 1000
    static final int MAX_COMMITS = 500

    long seq = 0
    List<String> commits = []
    List<String> ingested = []
    Map<String, Map> tests = new TreeMap<>()

    static Ledger load(Path file) {
        Ledger l = new Ledger()
        if (!Files.exists(file)) {
            return l
        }
        Object raw
        try {
            raw = new JsonSlurper().parseText(new String(Files.readAllBytes(file), StandardCharsets.UTF_8))
        } catch (Exception e) {
            throw new Exit(Config.EXIT_USAGE, "Ledger ${file} is not valid JSON, refusing to overwrite it: ${e.message}")
        }
        if (!(raw instanceof Map) || raw.schema != SCHEMA) {
            throw new Exit(Config.EXIT_USAGE, "Ledger ${file} has schema '${(raw instanceof Map) ? raw.schema : 'unknown'}', this tool reads schema ${SCHEMA}")
        }
        l.seq = (raw.seq ?: 0) as long
        l.commits = (raw.commits ?: []).collect { it.toString() }
        l.ingested = (raw.ingested ?: []).collect { it.toString() }
        (raw.tests ?: [:]).each { String id, Object rec ->
            Map m = [
                    firstSeen  : (rec.firstSeen ?: 0) as long,
                    lastSeen   : (rec.lastSeen ?: 0) as long,
                    quarantined: (rec.quarantined ?: false) as boolean,
                    since      : (rec.since ?: 0) as long,
                    floorSeq   : (rec.floorSeq ?: 0) as long,
                    lastClass  : (rec.lastClass ?: 'HEALTHY').toString(),
                    obs        : (rec.obs ?: []).collect { Object o ->
                        [c: o.c.toString(), o: o.o.toString(), s: (o.s as long), t: (o.t as long)]
                    }
            ]
            l.tests[id] = m
        }
        return l
    }

    Map toMap() {
        Map out = new LinkedHashMap()
        out.schema = SCHEMA
        out.seq = seq
        out.commits = commits.size() > MAX_COMMITS ? commits.subList(commits.size() - MAX_COMMITS, commits.size()) : commits
        out.ingested = ingested.size() > MAX_FINGERPRINTS ? ingested.subList(ingested.size() - MAX_FINGERPRINTS, ingested.size()) : ingested
        Map t = new LinkedHashMap()
        tests.keySet().sort().each { String id -> t[id] = tests[id] }
        out.tests = t
        return out
    }

    void save(Path file) {
        Path dir = file.toAbsolutePath().parent
        Files.createDirectories(dir)
        Path tmp = Files.createTempFile(dir, '.' + file.fileName.toString(), '.tmp')
        try {
            Files.write(tmp, JsonOutput.toJson(toMap()).getBytes(StandardCharsets.UTF_8))
            try {
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING)
            }
        } finally {
            Files.deleteIfExists(tmp)
        }
    }
}

class Analysis {
    String id
    String cls
    int observations
    int commits
    int mixedCommits
    int trailingFailStreak
    int flips
    int failures
    int attempts
    double rate
    double lo
    double hi
    boolean quarantined
    int cleanTrail

    Map toMap() {
        [id: id, class: cls, quarantined: quarantined, failureRate: round(rate), wilsonLow: round(lo),
         wilsonHigh: round(hi), mixedCommits: mixedCommits, trailingFailStreak: trailingFailStreak,
         flips: flips, observations: observations, commits: commits]
    }

    static double round(double v) {
        return Math.round(v * 10000d) / 10000d
    }
}

class Classifier {
    final Config cfg

    Classifier(Config cfg) {
        this.cfg = cfg
    }

    static double[] wilson(int fails, int total) {
        if (total == 0) {
            return [0d, 1d] as double[]
        }
        double z = 1.96d
        double z2 = z * z
        double p = fails / (double) total
        double denom = 1d + z2 / total
        double centre = (p + z2 / (2d * total)) / denom
        double half = z * Math.sqrt(p * (1d - p) / total + z2 / (4d * total * total)) / denom
        return [Math.max(0d, centre - half), Math.min(1d, centre + half)] as double[]
    }

    Analysis analyse(String id, Map rec) {
        long floor = rec.floorSeq as long
        List<Map> obs = (rec.obs as List<Map>).findAll { Map o -> (o.s as long) > floor }
        Map<String, int[]> groups = new LinkedHashMap<>()
        int pass = 0, fail = 0, retry = 0
        obs.each { Map o ->
            int[] g = groups.computeIfAbsent(o.c as String) { new int[3] }
            switch (o.o) {
                case 'P': g[0]++; pass++; break
                case 'F': g[1]++; fail++; break
                default: g[2]++; retry++
            }
        }
        List<String> states = groups.values().collect { int[] g ->
            boolean mixed = g[2] > 0 || (g[0] > 0 && g[1] > 0)
            mixed ? 'M' : (g[1] > 0 ? 'F' : 'P')
        }
        int mixedCommits = states.count { it == 'M' } as int
        int trail = 0
        for (int i = states.size() - 1; i >= 0 && states[i] == 'F'; i--) {
            trail++
        }
        int flips = 0
        String prev = null
        states.findAll { it != 'M' }.each { String s ->
            if (prev != null && prev != s) flips++
            prev = s
        }
        int failures = fail + retry
        int attempts = pass + fail + 2 * retry
        double[] w = wilson(failures, attempts)
        int cleanTrail = 0
        for (int i = obs.size() - 1; i >= 0 && obs[i].o == 'P'; i--) {
            cleanTrail++
        }

        String cls
        if (trail >= cfg.brokenStreak) cls = 'BROKEN'
        else if (mixedCommits >= cfg.minFlakyEvidence) cls = 'FLAKY'
        else if (mixedCommits >= 1) cls = 'SUSPECT'
        else if (trail >= 1 && states.size() > trail && states[states.size() - trail - 1] == 'P') cls = 'REGRESSION'
        else if (flips >= cfg.flipLimit && states.size() >= 8) cls = 'SUSPECT'
        else if (trail >= 1) cls = 'REGRESSION'
        else cls = 'HEALTHY'

        return new Analysis(id: id, cls: cls, observations: obs.size(), commits: groups.size(),
                mixedCommits: mixedCommits, trailingFailStreak: trail, flips: flips, failures: failures,
                attempts: attempts, rate: attempts == 0 ? 0d : failures / (double) attempts, lo: w[0], hi: w[1],
                quarantined: rec.quarantined as boolean, cleanTrail: cleanTrail)
    }
}

class Report {
    String headCommit
    int testsInLedger
    int runsIngested
    List<String> runsSkipped = []
    List<String> warnings = []
    List<String> unreadable = []
    List<Map> blocking = []
    List<Map> suppressed = []
    List<Map> flakedAtHead = []
    List<String> newlyQuarantined = []
    List<String> released = []
    List<Map> watchlist = []
    Map<String, Integer> classCounts = new TreeMap<>()
    double flakeTax
    double vanishShare = -1d
    int pruned
    int exitCode
    String exitReason = 'clean'
}

class Engine {
    final Config cfg
    final Classifier classifier

    Engine(Config cfg) {
        this.cfg = cfg
        this.classifier = new Classifier(cfg)
    }

    Report run(Ledger ledger) {
        Report rep = new Report()
        long nowSec = cfg.now.epochSecond

        // Stage every run before touching the ledger so a bad report cannot half-apply.
        List<Map> staged = []
        int totalCases = 0
        Set<String> batchFingerprints = new HashSet<>()
        cfg.runs.each { RunSpec spec ->
            List<Path> files = ReportParser.discover(spec.path)
            if (files.isEmpty()) {
                throw new Exit(Config.EXIT_INTEGRITY, "No .xml files under ${spec.path} for commit ${spec.commit}")
            }
            String fp = ReportParser.fingerprint(spec.commit, spec.path, files)
            List<TestCase> cases = []
            int reports = 0
            files.each { Path f ->
                ParsedFile pf = ReportParser.parse(f)
                if (pf.error != null) {
                    rep.unreadable << "${f}: ${pf.error}".toString()
                } else if (pf.report) {
                    reports++
                    cases.addAll(pf.cases)
                }
            }
            boolean duplicate = ledger.ingested.contains(fp) || !batchFingerprints.add(fp)
            staged << [spec: spec, fp: fp, cases: cases, reports: reports, duplicate: duplicate]
            totalCases += cases.size()
        }
        if (!rep.unreadable.isEmpty() && !cfg.allowUnreadable) {
            throw new Exit(Config.EXIT_INTEGRITY, "Unreadable or truncated reports, ledger left untouched:\n  " + rep.unreadable.join('\n  '))
        }
        staged.each { Map s ->
            if (!s.duplicate && s.reports == 0) {
                throw new Exit(Config.EXIT_INTEGRITY, "Run ${(s.spec as RunSpec).commit} has no JUnit reports under ${(s.spec as RunSpec).path}")
            }
            if (!s.duplicate && (s.cases as List).findAll { TestCase c -> c.outcome != 'S' }.isEmpty()) {
                throw new Exit(Config.EXIT_INTEGRITY, "Run ${(s.spec as RunSpec).commit} parsed zero executed tests, a green build with no tests is not a green build")
            }
        }
        rep.warnings.addAll(rep.unreadable.collect { "unreadable report ignored: ${it}".toString() })

        String head = cfg.runs.last().commit
        rep.headCommit = head
        List<String> order = ledger.commits
        int headAt = order.indexOf(head)
        String previousCommit = headAt > 0 ? order[headAt - 1] : (headAt < 0 && !order.isEmpty() ? order.last() : null)
        Set<String> previousTests = new HashSet<>()
        if (previousCommit != null) {
            ledger.tests.each { String id, Map rec ->
                if ((rec.obs as List<Map>).any { Map o -> o.c == previousCommit }) previousTests << id
            }
        }

        Map<String, List<String>> headOutcomes = new TreeMap<>()
        Set<String> headSeen = new HashSet<>()
        staged.each { Map s ->
            RunSpec spec = s.spec as RunSpec
            if (s.duplicate) {
                rep.runsSkipped << "${spec.commit}=${spec.path} (already ingested)".toString()
                return
            }
            ledger.seq++
            Map<String, String> folded = ReportParser.fold(s.cases as List<TestCase>, cfg.distinctDuplicates)
            folded.each { String id, String outcome ->
                Map rec = ledger.tests.computeIfAbsent(id) {
                    [firstSeen: nowSec, lastSeen: nowSec, quarantined: false, since: 0L, floorSeq: 0L,
                     lastClass: 'HEALTHY', obs: []]
                }
                (rec.obs as List) << [c: spec.commit, o: outcome, s: ledger.seq, t: nowSec]
                rec.lastSeen = nowSec
                List obsList = rec.obs as List
                if (obsList.size() > cfg.window) {
                    rec.obs = new ArrayList(obsList.subList(obsList.size() - cfg.window, obsList.size()))
                }
                if (spec.commit == head) {
                    headOutcomes.computeIfAbsent(id) { new ArrayList<String>() } << outcome
                    headSeen << id
                }
            }
            if (!ledger.commits.contains(spec.commit)) ledger.commits << spec.commit
            ledger.ingested << (s.fp as String)
            rep.runsIngested++
        }

        if (rep.runsIngested == 0) {
            rep.warnings << 'every run was already in the ledger, so nothing new was judged and this exit code is not a verdict on the head commit'
        }

        // Prune tests that disappeared long ago: renamed or deleted tests must not live forever.
        long cutoff = nowSec - Duration.ofDays(cfg.expireDays).seconds
        List<String> stale = ledger.tests.findAll { String id, Map rec -> (rec.lastSeen as long) < cutoff }.keySet().toList()
        stale.each { ledger.tests.remove(it) }
        rep.pruned = stale.size()

        // Coverage: how much of the previous commit's suite went missing at head.
        if (previousTests.size() > 0 && !headSeen.isEmpty()) {
            int missing = previousTests.count { String id -> !headSeen.contains(id) } as int
            rep.vanishShare = missing / (double) previousTests.size()
            if (rep.vanishShare > cfg.vanishRatio) {
                rep.warnings << String.format('%d of %d tests from %s did not appear at %s (%.1f%%), a shard or report may be missing',
                        missing, previousTests.size(), previousCommit, head, rep.vanishShare * 100d)
            }
        }

        Map<String, Analysis> analyses = new TreeMap<>()
        double keep = 1d
        ledger.tests.each { String id, Map rec ->
            Analysis a = classifier.analyse(id, rec)
            boolean wasQuarantined = rec.quarantined as boolean
            String previousClass = rec.lastClass as String
            if (wasQuarantined) {
                if (a.cls != 'BROKEN' && a.cleanTrail >= cfg.releaseStreak) {
                    rec.quarantined = false
                    rec.since = 0L
                    rec.floorSeq = ledger.seq
                    rep.released << id
                    a = classifier.analyse(id, rec)
                }
            } else if (a.cls == 'FLAKY') {
                rec.quarantined = true
                rec.since = nowSec
                rep.newlyQuarantined << id
            }
            a.quarantined = rec.quarantined as boolean
            rec.lastClass = a.cls
            analyses[id] = a
            rep.classCounts[a.cls] = (rep.classCounts[a.cls] ?: 0) + 1
            if (a.cls == 'FLAKY' || a.quarantined) {
                keep *= (1d - Math.min(a.rate, 0.999999d))
            }
            if (a.cls != 'HEALTHY' || a.quarantined || previousClass != a.cls && previousClass != 'HEALTHY') {
                rep.watchlist << a.toMap()
            }
        }
        rep.testsInLedger = ledger.tests.size()
        rep.flakeTax = 1d - keep
        rep.watchlist.sort { Map m -> [-((m.failureRate as double) * 1000d), m.id] }

        // The gate: judge only the head commit, only tests that failed in every attempt.
        headOutcomes.each { String id, List<String> outs ->
            Analysis a = analyses[id]
            if (a == null) return
            boolean allFailed = outs.every { it == 'F' }
            boolean anyFailed = outs.any { it == 'F' || it == 'R' }
            if (allFailed) {
                if (a.quarantined && a.cls != 'BROKEN') {
                    rep.suppressed << [id: id, reason: 'quarantined flaky test failed', failureRate: Analysis.round(a.rate)]
                } else {
                    String why
                    switch (a.cls) {
                        case 'BROKEN': why = "failing on ${a.trailingFailStreak} commits in a row"; break
                        case 'REGRESSION': why = 'started failing after passing'; break
                        case 'SUSPECT': why = 'failed every attempt, evidence of flaking is not strong enough to excuse it'; break
                        default: why = 'failed every attempt'
                    }
                    rep.blocking << [id: id, class: a.cls, reason: why, trailingFailStreak: a.trailingFailStreak]
                }
            } else if (anyFailed) {
                rep.flakedAtHead << [id: id, class: a.cls, attempts: outs.join('')]
            }
        }

        // Quarantine file reflects the final state, not the state before this run.
        if (rep.blocking) {
            rep.exitCode = Config.EXIT_BLOCKING
            rep.exitReason = "${rep.blocking.size()} blocking failure(s)"
        } else if (cfg.failOnNewFlaky && rep.newlyQuarantined) {
            rep.exitCode = Config.EXIT_BLOCKING
            rep.exitReason = "${rep.newlyQuarantined.size()} test(s) newly quarantined"
        } else if (cfg.strictCoverage && rep.vanishShare > cfg.vanishRatio) {
            rep.exitCode = Config.EXIT_COVERAGE
            rep.exitReason = 'test coverage dropped against the previous commit'
        }
        return rep
    }
}

class Renderer {
    static String render(Config cfg, Report r) {
        switch (cfg.format) {
            case 'json': return json(r)
            case 'markdown': return markdown(r)
            case 'github': return github(r)
            default: return text(r)
        }
    }

    static String pct(double v) {
        return String.format('%.1f%%', v * 100d)
    }

    static String json(Report r) {
        Map m = [
                head            : r.headCommit,
                exitCode        : r.exitCode,
                exitReason      : r.exitReason,
                testsInLedger   : r.testsInLedger,
                runsIngested    : r.runsIngested,
                runsSkipped     : r.runsSkipped,
                classes         : r.classCounts,
                flakeTax        : Analysis.round(r.flakeTax),
                vanishShare     : r.vanishShare < 0 ? null : Analysis.round(r.vanishShare),
                pruned          : r.pruned,
                blocking        : r.blocking,
                suppressed      : r.suppressed,
                flakedAtHead    : r.flakedAtHead,
                newlyQuarantined: r.newlyQuarantined,
                released        : r.released,
                warnings        : r.warnings,
                watchlist       : r.watchlist
        ]
        return JsonOutput.prettyPrint(JsonOutput.toJson(m)) + '\n'
    }

    static String text(Report r) {
        StringBuilder sb = new StringBuilder()
        sb << "JUnit Flake Ledger  head=${r.headCommit}  tests=${r.testsInLedger}  runs ingested=${r.runsIngested}\n"
        sb << "Result: ${r.exitReason} (exit ${r.exitCode})\n"
        sb << "Classes: ${r.classCounts.collect { k, v -> "${k}=${v}" }.join('  ') ?: 'none'}\n"
        sb << "Chance a full run goes red from flakes alone: ${pct(r.flakeTax)}\n"
        r.warnings.each { sb << "WARNING: ${it}\n" }
        r.runsSkipped.each { sb << "skipped: ${it}\n" }
        section(sb, 'Blocking failures', r.blocking.collect { "${it.id}  [${it.class}] ${it.reason}".toString() })
        section(sb, 'Failed but quarantined', r.suppressed.collect { "${it.id}  (${pct(it.failureRate as double)} failure rate)".toString() })
        section(sb, 'Flaked at head, passed on another attempt', r.flakedAtHead.collect { "${it.id}  attempts=${it.attempts}".toString() })
        section(sb, 'Newly quarantined', r.newlyQuarantined)
        section(sb, 'Released from quarantine', r.released)
        section(sb, 'Watchlist', r.watchlist.collect {
            "${it.id}  [${it.class}${it.quarantined ? ', quarantined' : ''}] rate=${pct(it.failureRate as double)} ci=${pct(it.wilsonLow as double)}..${pct(it.wilsonHigh as double)} mixedCommits=${it.mixedCommits} failStreak=${it.trailingFailStreak}".toString()
        })
        if (r.pruned) sb << "Pruned ${r.pruned} test(s) not seen within the expiry window.\n"
        return sb.toString()
    }

    static void section(StringBuilder sb, String title, List<String> lines) {
        if (lines.isEmpty()) return
        sb << "\n${title} (${lines.size()})\n"
        lines.each { sb << "  ${it}\n" }
    }

    static String markdown(Report r) {
        StringBuilder sb = new StringBuilder()
        sb << "## JUnit Flake Ledger\n\n"
        sb << "**Head:** `${r.headCommit}` | **Result:** ${r.exitReason} | **Tests tracked:** ${r.testsInLedger} | **Flake tax:** ${pct(r.flakeTax)}\n\n"
        r.warnings.each { sb << "> Warning: ${it}\n" }
        if (r.warnings) sb << '\n'
        mdList(sb, 'Blocking failures', r.blocking.collect { "`${it.id}` ${it.class}: ${it.reason}".toString() })
        mdList(sb, 'Failed but quarantined', r.suppressed.collect { "`${it.id}`".toString() })
        mdList(sb, 'Flaked at head', r.flakedAtHead.collect { "`${it.id}` attempts ${it.attempts}".toString() })
        mdList(sb, 'Newly quarantined', r.newlyQuarantined.collect { "`${it}`".toString() })
        mdList(sb, 'Released from quarantine', r.released.collect { "`${it}`".toString() })
        if (r.watchlist) {
            sb << "### Watchlist\n\n| Test | Class | Failure rate | 95% interval | Mixed commits | Fail streak |\n|---|---|---|---|---|---|\n"
            r.watchlist.take(50).each {
                sb << "| `${it.id}` | ${it.class}${it.quarantined ? ' (quarantined)' : ''} | ${pct(it.failureRate as double)} | ${pct(it.wilsonLow as double)} to ${pct(it.wilsonHigh as double)} | ${it.mixedCommits} | ${it.trailingFailStreak} |\n"
            }
            if (r.watchlist.size() > 50) sb << "\n${r.watchlist.size() - 50} more not shown.\n"
        }
        return sb.toString()
    }

    static void mdList(StringBuilder sb, String title, List<String> lines) {
        if (lines.isEmpty()) return
        sb << "### ${title}\n\n"
        lines.each { sb << "- ${it}\n" }
        sb << '\n'
    }

    static String esc(String s) {
        return s.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
    }

    static String escProp(String s) {
        return esc(s).replace(':', '%3A').replace(',', '%2C')
    }

    static String github(Report r) {
        StringBuilder sb = new StringBuilder()
        r.blocking.each { sb << "::error title=${escProp('Blocking test failure')}::${esc("${it.id}: ${it.reason}".toString())}\n" }
        r.warnings.each { sb << "::warning title=${escProp('Flake ledger')}::${esc(it)}\n" }
        r.newlyQuarantined.each { sb << "::warning title=${escProp('Test quarantined as flaky')}::${esc(it)}\n" }
        r.flakedAtHead.each { sb << "::notice title=${escProp('Flaked at head')}::${esc("${it.id} attempts ${it.attempts}".toString())}\n" }
        sb << "::notice title=${escProp('Flake ledger')}::${esc("${r.exitReason}; flake tax ${pct(r.flakeTax)}".toString())}\n"
        return sb.toString()
    }
}

class Main {
    static void main(String[] args) {
        int code
        try {
            code = execute(args)
        } catch (Exit e) {
            if (e.code == Config.EXIT_OK) {
                System.out.println(e.message)
            } else {
                System.err.println("JUnitFlakeLedger: ${e.message}")
            }
            code = e.code
        }
        System.exit(code)
    }

    static int execute(String[] args) {
        Config cfg = Config.parse(args)
        Path lockPath = cfg.ledger.toAbsolutePath().resolveSibling(cfg.ledger.fileName.toString() + '.lock')
        Files.createDirectories(lockPath.parent)
        FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        FileLock lock = null
        try {
            try {
                lock = channel.tryLock()
            } catch (java.nio.channels.OverlappingFileLockException ignored) {
                lock = null
            }
            if (lock == null) {
                throw new Exit(Config.EXIT_USAGE, "Ledger ${cfg.ledger} is locked by another process, retry in a moment")
            }
            Ledger ledger = Ledger.load(cfg.ledger)
            Report rep = new Engine(cfg).run(ledger)
            if (cfg.write) {
                ledger.save(cfg.ledger)
            }
            if (cfg.quarantineOut != null) {
                List<String> ids = ledger.tests.findAll { String id, Map rec -> rec.quarantined }.keySet().toList().sort()
                Path q = cfg.quarantineOut.toAbsolutePath()
                Files.createDirectories(q.parent)
                Files.write(q, (ids.join('\n') + (ids ? '\n' : '')).getBytes(StandardCharsets.UTF_8))
            }
            String body = Renderer.render(cfg, rep)
            if (cfg.out != null) {
                Path o = cfg.out.toAbsolutePath()
                Files.createDirectories(o.parent)
                Files.write(o, body.getBytes(StandardCharsets.UTF_8))
            } else {
                System.out.print(body)
            }
            return rep.exitCode
        } finally {
            try { lock?.release() } catch (Exception ignored) { }
            channel.close()
        }
    }
}

Main.main(args)
