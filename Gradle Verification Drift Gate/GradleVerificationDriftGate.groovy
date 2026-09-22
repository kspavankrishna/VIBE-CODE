#!/usr/bin/env groovy
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/*
 * GradleVerificationDriftGate.groovy
 *
 * Diffs two Gradle dependency verification metadata files (gradle/verification-metadata.xml)
 * and fails CI when the diff weakens supply chain protection instead of extending it.
 * See README.md in this folder for the full explanation, rule list and usage.
 */

enum Severity {
    CRITICAL, HIGH, MEDIUM, LOW, INFO

    String label() { name().toLowerCase() }
}

class Finding {
    String id
    Severity severity
    String component
    String artifact
    String message

    Map toMap() {
        def m = [id: id, severity: severity.label(), message: message]
        if (component) m.component = component
        if (artifact) m.artifact = artifact
        return m
    }
}

class TrustEntry {
    String group
    String name
    String version
    String file
    boolean regex

    String key() { "${group}|${name}|${version}|${file}|${regex}".toString() }

    String describe() {
        def parts = []
        if (group) parts << "group=${group}"
        if (name) parts << "name=${name}"
        if (version) parts << "version=${version}"
        if (file) parts << "file=${file}"
        if (regex) parts << "regex=true"
        return parts ? parts.join(' ') : '(empty trust entry)'
    }
}

class IgnoredKeyEntry {
    String id
    String reason
}

class ArtifactEntry {
    String name
    Map<String, Set<String>> hashes = [:]
}

class ComponentEntry {
    String group
    String name
    String version
    Map<String, ArtifactEntry> artifacts = [:]

    String key() { "${group}:${name}:${version}".toString() }

    String groupName() { "${group}:${name}".toString() }
}

class VerificationModel {
    boolean verifyMetadata = true
    boolean verifySignatures = false
    List<TrustEntry> trustedArtifacts = []
    List<IgnoredKeyEntry> ignoredKeys = []
    Map<String, ComponentEntry> components = [:]
}

class Policy {
    List<String> strongHashAlgorithms = ['sha256', 'sha512']
    String failOn = 'high'
}

class MetadataParseException extends RuntimeException {
    MetadataParseException(String msg, Throwable cause = null) { super(msg, cause) }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

static String attr(node, String name) {
    def value = node.attributes()[name]
    return value == null ? null : value.toString()
}

// XmlSlurper lives in groovy.xml since Groovy 3, groovy.util before that.
// Resolving it by name keeps this script running unmodified on both the
// Groovy release bundled with your Gradle wrapper and a standalone install.
static Object newXmlSlurper() {
    try {
        return Class.forName('groovy.xml.XmlSlurper').getDeclaredConstructor().newInstance()
    } catch (ClassNotFoundException ignored) {
        return Class.forName('groovy.util.XmlSlurper').getDeclaredConstructor().newInstance()
    }
}

static VerificationModel parseModelText(String xmlText, String sourceLabel) {
    def root
    try {
        root = newXmlSlurper().parseText(xmlText)
    } catch (Exception e) {
        throw new MetadataParseException("Failed to parse ${sourceLabel}: ${e.message}", e)
    }
    if (root.name() != 'verification-metadata') {
        throw new MetadataParseException("${sourceLabel} has root element <${root.name()}>, expected <verification-metadata>")
    }

    def model = new VerificationModel()
    def cfg = root.configuration

    def verifyMetadataNode = cfg.'verify-metadata'
    if (verifyMetadataNode.size() > 0) model.verifyMetadata = verifyMetadataNode.text().trim() == 'true'

    def verifySignaturesNode = cfg.'verify-signatures'
    if (verifySignaturesNode.size() > 0) model.verifySignatures = verifySignaturesNode.text().trim() == 'true'

    cfg.'trusted-artifacts'.trust.each { t ->
        model.trustedArtifacts << new TrustEntry(
            group: attr(t, 'group'),
            name: attr(t, 'name'),
            version: attr(t, 'version'),
            file: attr(t, 'file'),
            regex: attr(t, 'regex') == 'true'
        )
    }

    cfg.'ignored-keys'.'ignored-key'.each { k ->
        model.ignoredKeys << new IgnoredKeyEntry(id: attr(k, 'id'), reason: attr(k, 'reason'))
    }

    root.components.component.each { c ->
        def group = attr(c, 'group')
        def name = attr(c, 'name')
        def version = attr(c, 'version')
        if (!group || !name || !version) {
            throw new MetadataParseException("${sourceLabel} has a <component> missing group, name or version")
        }
        def key = "${group}:${name}:${version}".toString()
        def comp = model.components.computeIfAbsent(key) { new ComponentEntry(group: group, name: name, version: version) }

        c.artifact.each { a ->
            def fname = attr(a, 'name')
            if (!fname) throw new MetadataParseException("${sourceLabel} has an <artifact> under ${key} with no name")
            def art = comp.artifacts.computeIfAbsent(fname) { new ArtifactEntry(name: fname) }
            a.children().each { h ->
                def tag = h.name()
                if (tag in ['sha256', 'sha512', 'sha1', 'md5', 'pgp']) {
                    def v = attr(h, 'value')
                    if (v) {
                        def set = art.hashes.computeIfAbsent(tag) { new LinkedHashSet<String>() }
                        set << v.trim().toLowerCase()
                    }
                }
            }
        }
    }

    return model
}

static VerificationModel parseModelFile(File f) {
    if (!f.exists()) throw new MetadataParseException("File not found: ${f.path}")
    return parseModelText(f.text, f.path)
}

// ---------------------------------------------------------------------------
// Lockfile parsing (single unified gradle.lockfile or per-configuration files)
// ---------------------------------------------------------------------------

static List<String> parseLockfileCoords(File f) {
    List<String> out = []
    f.eachLine { rawLine ->
        def line = rawLine.trim()
        if (!line || line.startsWith('#')) return
        if (line.startsWith('empty=')) return
        def withoutConfigs = line.contains('=') ? line.substring(0, line.indexOf('=')) : line
        def segs = withoutConfigs.tokenize(':')
        if (segs.size() >= 3) out << "${segs[0]}:${segs[1]}:${segs[2]}".toString()
    }
    return out
}

static List<String> resolveLockfileCoords(List<String> paths) {
    List<File> files = []
    paths.each { p ->
        def f = new File(p)
        if (!f.exists()) throw new MetadataParseException("Lockfile path not found: ${p}")
        if (f.isDirectory()) {
            f.eachFileRecurse { child ->
                if (child.isFile() && child.name.endsWith('.lockfile')) files << child
            }
        } else {
            files << f
        }
    }
    Set<String> coords = new LinkedHashSet<>()
    files.each { coords.addAll(parseLockfileCoords(it)) }
    return coords.toList()
}

// ---------------------------------------------------------------------------
// Version heuristic (not a full Maven/semver comparator, see README Notes)
// ---------------------------------------------------------------------------

static List<String> versionTokens(String v) {
    List<String> out = []
    def m = (v =~ /[0-9]+|[A-Za-z]+/)
    while (m.find()) out << m.group()
    return out
}

static boolean isNumericToken(String s) { s ==~ /[0-9]+/ }

static int compareVersions(String v1, String v2) {
    if (v1 == v2) return 0
    List<String> t1 = versionTokens(v1)
    List<String> t2 = versionTokens(v2)
    int n = Math.max(t1.size(), t2.size())
    for (int i = 0; i < n; i++) {
        String a = i < t1.size() ? t1[i] : null
        String b = i < t2.size() ? t2[i] : null
        if (a == null && b == null) return 0
        if (a == null) return isNumericToken(b) && b.replaceFirst('^0+(?=.)', '') == '0' ? 0 : -1
        if (b == null) return isNumericToken(a) && a.replaceFirst('^0+(?=.)', '') == '0' ? 0 : 1
        if (isNumericToken(a) && isNumericToken(b)) {
            int cmp = Long.valueOf(a) <=> Long.valueOf(b)
            if (cmp != 0) return cmp
        } else {
            int cmp = a.compareToIgnoreCase(b)
            if (cmp != 0) return cmp < 0 ? -1 : 1
        }
    }
    return 0
}

// ---------------------------------------------------------------------------
// Diff engine
// ---------------------------------------------------------------------------

static Finding finding(String id, Severity sev, String component, String artifact, String message) {
    new Finding(id: id, severity: sev, component: component, artifact: artifact, message: message)
}

static Map diffModels(VerificationModel baseline, VerificationModel candidate, List<String> lockedCoords, Policy policy) {
    List<Finding> findings = []
    Map stats = [componentsAdded: 0, componentsRemoved: 0, componentsUnchanged: 0, artifactsAdded: 0,
                 trustEntriesAdded: 0, ignoredKeysAdded: 0]

    if (baseline.verifyMetadata && !candidate.verifyMetadata) {
        findings << finding('VerificationDisabled', Severity.CRITICAL, null, null,
            'verify-metadata flipped from true to false: Gradle will stop checking any artifact hash or signature for this project')
    }
    if (baseline.verifySignatures && !candidate.verifySignatures) {
        findings << finding('SignatureVerificationDisabled', Severity.CRITICAL, null, null,
            'verify-signatures flipped from true to false: PGP signature checks are now skipped for every artifact')
    }

    Map<String, String> baselineMaxVersion = [:]
    baseline.components.values().each { c ->
        def gn = c.groupName()
        def cur = baselineMaxVersion[gn]
        if (cur == null || compareVersions(c.version, cur) > 0) baselineMaxVersion[gn] = c.version
    }

    candidate.components.each { key, cc ->
        def bc = baseline.components[key]
        if (bc == null) {
            stats.componentsAdded++
            def gn = cc.groupName()
            def maxSeen = baselineMaxVersion[gn]
            if (maxSeen != null && compareVersions(cc.version, maxSeen) < 0) {
                findings << finding('StaleArtifactReintroduced', Severity.HIGH, key, null,
                    "Component reappeared at version ${cc.version}, older than the highest previously verified version ${maxSeen} of ${gn}. Confirm this is a deliberate rollback and not a reintroduced or substituted artifact")
            }
            boolean anyStrong = cc.artifacts.values().any { art -> art.hashes.keySet().any { policy.strongHashAlgorithms.contains(it) } }
            if (!anyStrong) {
                findings << finding('WeakHashOnlyNewComponent', Severity.LOW, key, null,
                    "New component has no ${policy.strongHashAlgorithms.join('/')} hash recorded, only weaker algorithms")
            }
        } else {
            stats.componentsUnchanged++
            cc.artifacts.each { fname, ca ->
                def ba = bc.artifacts[fname]
                if (ba == null) {
                    stats.artifactsAdded++
                    return
                }
                ba.hashes.each { alg, bvals ->
                    def cvals = ca.hashes[alg]
                    if (cvals != null && bvals != cvals) {
                        findings << finding('ArtifactHashChanged', Severity.CRITICAL, key, fname,
                            "${alg} changed from [${bvals.join(', ')}] to [${cvals.join(', ')}]. A release artifact at a fixed coordinate should never change hash; treat this as a compromised or substituted artifact until proven otherwise")
                    }
                }
                def baselineStrong = ba.hashes.keySet().find { policy.strongHashAlgorithms.contains(it) }
                def candidateStrong = ca.hashes.keySet().find { policy.strongHashAlgorithms.contains(it) }
                if (baselineStrong && !candidateStrong) {
                    findings << finding('ArtifactHashAlgorithmDowngraded', Severity.HIGH, key, fname,
                        "Lost its ${baselineStrong} hash; only weaker algorithms remain for this artifact")
                }
            }
        }
    }

    baseline.components.each { key, bc ->
        if (!candidate.components.containsKey(key)) {
            stats.componentsRemoved++
            if (lockedCoords.contains(key)) {
                findings << finding('ArtifactVerificationRemoved', Severity.HIGH, key, null,
                    'Component was removed from verification metadata but a supplied lockfile still resolves it at this exact version. Gradle will accept any artifact at this coordinate with no check at all')
            }
        }
    }

    Set<String> baselineTrustKeys = baseline.trustedArtifacts.collect { it.key() } as Set
    candidate.trustedArtifacts.each { t ->
        if (!baselineTrustKeys.contains(t.key())) {
            stats.trustEntriesAdded++
            if (t.regex || !t.group || !t.name) {
                findings << finding('BroadTrustEntryAdded', Severity.HIGH, null, null,
                    "New trust entry (${t.describe()}) bypasses checksum verification for a pattern of artifacts instead of one pinned coordinate")
            } else if (!t.version) {
                findings << finding('VersionlessTrustEntry', Severity.MEDIUM, null, null,
                    "New trust entry (${t.describe()}) has no version, trusting every version ever published at this coordinate")
            }
        }
    }

    Set<String> baselineIgnoredIds = baseline.ignoredKeys.collect { it.id } as Set
    candidate.ignoredKeys.each { k ->
        if (!baselineIgnoredIds.contains(k.id)) {
            stats.ignoredKeysAdded++
            if (!k.reason?.trim()) {
                findings << finding('NewIgnoredKeyWithoutReason', Severity.HIGH, null, null,
                    "PGP key ${k.id} added to ignored-keys with no reason recorded")
            } else {
                findings << finding('WeakeningIgnoredKeyAdded', Severity.MEDIUM, null, null,
                    "PGP key ${k.id} added to ignored-keys: \"${k.reason}\". Signature failures for this key are now silently accepted")
            }
        }
    }

    lockedCoords.each { coord ->
        if (candidate.components.containsKey(coord)) return
        if (baseline.components.containsKey(coord)) return
        findings << finding('UnverifiedLockedDependency', Severity.HIGH, coord, null,
            'A supplied lockfile resolves this exact coordinate but it has never had a verification metadata entry. Gradle will not verify this dependency at all')
    }

    return [findings: findings, stats: stats]
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

static List<Severity> severityOrder() {
    [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW, Severity.INFO]
}

static boolean atOrAboveThreshold(Severity sev, String failOn) {
    if (failOn == 'none') return false
    def threshold = Severity.valueOf(failOn.toUpperCase())
    def order = severityOrder()
    return order.indexOf(sev) <= order.indexOf(threshold)
}

static String renderText(String baselinePath, String candidatePath, Map diffResult, String failOn) {
    List<Finding> findings = diffResult.findings
    Map stats = diffResult.stats
    def sb = new StringBuilder()
    sb << "Gradle Verification Drift Gate\n"
    sb << "  baseline:  ${baselinePath}\n"
    sb << "  candidate: ${candidatePath}\n\n"

    def order = severityOrder()
    def bySeverity = order.collectEntries { [(it): findings.findAll { f -> f.severity == it } ] }
    order.each { sev ->
        def list = bySeverity[sev]
        if (!list) return
        sb << "${sev.label().toUpperCase()} (${list.size()})\n"
        list.each { f ->
            def loc = [f.component, f.artifact].findAll { it }.join(' / ')
            sb << "  [${f.id}]${loc ? ' ' + loc : ''}\n      ${f.message}\n"
        }
        sb << "\n"
    }

    sb << "Summary: ${stats.componentsAdded} components added, ${stats.componentsRemoved} removed, " +
        "${stats.componentsUnchanged} unchanged, ${stats.artifactsAdded} artifacts added, " +
        "${stats.trustEntriesAdded} trust entries added, ${stats.ignoredKeysAdded} ignored keys added\n"

    boolean blocking = findings.any { atOrAboveThreshold(it.severity, failOn) }
    sb << "Gate: ${blocking ? 'FAIL' : 'PASS'} (fail-on=${failOn})\n"
    return sb.toString()
}

static String renderJson(String baselinePath, String candidatePath, Map diffResult, String failOn) {
    List<Finding> findings = diffResult.findings
    boolean blocking = findings.any { atOrAboveThreshold(it.severity, failOn) }
    def payload = [
        baseline : baselinePath,
        candidate: candidatePath,
        failOn   : failOn,
        pass     : !blocking,
        stats    : diffResult.stats,
        findings : findings.collect { it.toMap() }
    ]
    return JsonOutput.prettyPrint(JsonOutput.toJson(payload))
}

static String sarifLevel(Severity sev) {
    switch (sev) {
        case Severity.CRITICAL:
        case Severity.HIGH:
            return 'error'
        case Severity.MEDIUM:
            return 'warning'
        default:
            return 'note'
    }
}

static String renderSarif(String candidatePath, Map diffResult) {
    List<Finding> findings = diffResult.findings
    def ruleIds = findings.collect { it.id }.unique()
    def rules = ruleIds.collect { id ->
        [id: id, shortDescription: [text: id]]
    }
    def results = findings.collect { f ->
        [
            ruleId  : f.id,
            level   : sarifLevel(f.severity),
            message : [text: f.message],
            locations: [[physicalLocation: [artifactLocation: [uri: candidatePath], region: [startLine: 1]]]]
        ]
    }
    def sarif = [
        version: '2.1.0',
        '$schema': 'https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json',
        runs: [[
            tool: [driver: [name: 'GradleVerificationDriftGate', informationUri: 'https://gradle.org/', rules: rules]],
            results: results
        ]]
    ]
    return JsonOutput.prettyPrint(JsonOutput.toJson(sarif))
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

static void printUsage() {
    println '''Usage: groovy GradleVerificationDriftGate.groovy --baseline FILE --candidate FILE [options]

Required:
  --baseline FILE      Last approved gradle/verification-metadata.xml
  --candidate FILE     Newly generated verification-metadata.xml to check

Options:
  --lockfile PATH       gradle.lockfile, a dependency-locks directory, or a single
                         per-configuration *.lockfile. Repeatable.
  --policy FILE          JSON file overriding { "strongHashAlgorithms": [...], "failOn": "..." }
  --format text|json|sarif   Output format (default: text)
  --fail-on critical|high|medium|low|info|none   Severity that fails the gate (default: high)
  --out FILE              Write the report to FILE instead of stdout
  --selftest              Run the embedded self-test suite and exit
  --help                  Show this message

Exit codes: 0 = pass, 1 = gate failed, 2 = usage or parse error'''
}

static Map parseArgs(List<String> args) {
    Map opts = [lockfiles: []]
    int i = 0
    while (i < args.size()) {
        def a = args[i]
        switch (a) {
            case '--baseline': opts.baseline = args[++i]; break
            case '--candidate': opts.candidate = args[++i]; break
            case '--lockfile': opts.lockfiles << args[++i]; break
            case '--policy': opts.policyFile = args[++i]; break
            case '--format': opts.format = args[++i]; break
            case '--fail-on': opts.failOn = args[++i]; break
            case '--out': opts.out = args[++i]; break
            case '--selftest': opts.selftest = true; break
            case '--help': opts.help = true; break
            default:
                throw new IllegalArgumentException("Unknown argument: ${a}")
        }
        i++
    }
    return opts
}

static Policy loadPolicy(String policyFile) {
    def policy = new Policy()
    if (policyFile) {
        def f = new File(policyFile)
        if (!f.exists()) throw new MetadataParseException("Policy file not found: ${policyFile}")
        def data = new JsonSlurper().parse(f)
        if (data.strongHashAlgorithms) policy.strongHashAlgorithms = data.strongHashAlgorithms.collect { it.toString().toLowerCase() }
        if (data.failOn) policy.failOn = data.failOn.toString()
    }
    return policy
}

static int runGate(List<String> args) {
    Map opts
    try {
        opts = parseArgs(args)
    } catch (IllegalArgumentException e) {
        System.err.println(e.message)
        printUsage()
        return 2
    }

    if (opts.help) {
        printUsage()
        return 0
    }
    if (opts.selftest) {
        return SelfTest.run() ? 0 : 1
    }
    if (!opts.baseline || !opts.candidate) {
        System.err.println('Both --baseline and --candidate are required.')
        printUsage()
        return 2
    }

    def format = opts.format ?: 'text'
    if (!(format in ['text', 'json', 'sarif'])) {
        System.err.println("Unknown --format: ${format}")
        return 2
    }

    try {
        Policy policy = loadPolicy(opts.policyFile)
        if (opts.failOn) policy.failOn = opts.failOn
        if (!(policy.failOn.toUpperCase() in Severity.values()*.name()) && policy.failOn != 'none') {
            System.err.println("Unknown --fail-on value: ${policy.failOn}")
            return 2
        }

        def baselineModel = parseModelFile(new File(opts.baseline))
        def candidateModel = parseModelFile(new File(opts.candidate))
        def lockedCoords = opts.lockfiles ? resolveLockfileCoords(opts.lockfiles) : []

        def diffResult = diffModels(baselineModel, candidateModel, lockedCoords, policy)

        String report
        if (format == 'json') {
            report = renderJson(opts.baseline, opts.candidate, diffResult, policy.failOn)
        } else if (format == 'sarif') {
            report = renderSarif(opts.candidate, diffResult)
        } else {
            report = renderText(opts.baseline, opts.candidate, diffResult, policy.failOn)
        }

        if (opts.out) {
            new File(opts.out).text = report
        } else {
            println report
        }

        if (policy.failOn == 'none') return 0
        boolean blocking = diffResult.findings.any { atOrAboveThreshold(it.severity, policy.failOn) }
        return blocking ? 1 : 0
    } catch (MetadataParseException e) {
        System.err.println(e.message)
        return 2
    }
}

// ---------------------------------------------------------------------------
// Self-test: exercises every rule against embedded fixtures, no files on disk
// ---------------------------------------------------------------------------

class SelfTest {
    static String BASELINE_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<verification-metadata>
  <configuration>
    <verify-metadata>true</verify-metadata>
    <verify-signatures>true</verify-signatures>
    <trusted-artifacts>
      <trust group="org.gradle" name="gradle-tooling-api" version="7.0"/>
    </trusted-artifacts>
    <ignored-keys>
      <ignored-key id="AAAA1111" reason="legacy key"/>
    </ignored-keys>
  </configuration>
  <components>
    <component group="org.apache.commons" name="commons-lang3" version="3.12.0">
      <artifact name="commons-lang3-3.12.0.jar"><sha256 value="hash-lang3-jar-old"/></artifact>
      <artifact name="commons-lang3-3.12.0.pom"><sha256 value="hash-lang3-pom"/></artifact>
    </component>
    <component group="com.google.guava" name="guava" version="31.1-jre">
      <artifact name="guava-31.1-jre.jar"><sha256 value="hash-guava-31"/></artifact>
    </component>
    <component group="com.example" name="widget" version="1.0.0">
      <artifact name="widget-1.0.0.jar"><sha256 value="hash-widget"/></artifact>
    </component>
    <component group="com.example" name="legacy-lib" version="2.0.0">
      <artifact name="legacy-lib-2.0.0.jar"><sha256 value="hash-legacy"/></artifact>
    </component>
  </components>
</verification-metadata>'''

    static String CANDIDATE_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<verification-metadata>
  <configuration>
    <verify-metadata>true</verify-metadata>
    <verify-signatures>false</verify-signatures>
    <trusted-artifacts>
      <trust group="org.gradle" name="gradle-tooling-api" version="7.0"/>
      <trust file="charsets-*.jar" regex="true"/>
      <trust group="org.mockito" name="mockito-core"/>
    </trusted-artifacts>
    <ignored-keys>
      <ignored-key id="AAAA1111" reason="legacy key"/>
      <ignored-key id="DEADBEEF" reason=""/>
      <ignored-key id="CAFEBABE" reason="internal signing key rotated"/>
    </ignored-keys>
  </configuration>
  <components>
    <component group="org.apache.commons" name="commons-lang3" version="3.12.0">
      <artifact name="commons-lang3-3.12.0.jar"><sha256 value="hash-lang3-jar-NEW"/></artifact>
      <artifact name="commons-lang3-3.12.0.pom"><sha256 value="hash-lang3-pom"/></artifact>
    </component>
    <component group="com.google.guava" name="guava" version="31.1-jre">
      <artifact name="guava-31.1-jre.jar"><sha256 value="hash-guava-31"/></artifact>
    </component>
    <component group="com.google.guava" name="guava" version="29.0-jre">
      <artifact name="guava-29.0-jre.jar"><md5 value="hash-guava-29-md5"/></artifact>
    </component>
    <component group="com.example" name="widget" version="1.0.0">
      <artifact name="widget-1.0.0.jar"><md5 value="hash-widget-md5-only"/></artifact>
    </component>
    <component group="com.example" name="fresh-lib" version="1.0.0">
      <artifact name="fresh-lib-1.0.0.jar"><sha256 value="hash-fresh"/></artifact>
    </component>
  </components>
</verification-metadata>'''

    static boolean run() {
        List<String> lockedCoords = [
            'com.example:legacy-lib:2.0.0',
            'com.example:never-verified:9.9.9'
        ]
        def baseline = GradleVerificationDriftGate.parseModelText(BASELINE_XML, 'selftest-baseline')
        def candidate = GradleVerificationDriftGate.parseModelText(CANDIDATE_XML, 'selftest-candidate')
        def policy = new Policy()
        def result = GradleVerificationDriftGate.diffModels(baseline, candidate, lockedCoords, policy)
        Set<String> actualIds = result.findings.collect { it.id } as Set

        Set<String> expectedIds = [
            'SignatureVerificationDisabled',
            'ArtifactHashChanged',
            'ArtifactHashAlgorithmDowngraded',
            'ArtifactVerificationRemoved',
            'StaleArtifactReintroduced',
            'WeakHashOnlyNewComponent',
            'BroadTrustEntryAdded',
            'VersionlessTrustEntry',
            'NewIgnoredKeyWithoutReason',
            'WeakeningIgnoredKeyAdded',
            'UnverifiedLockedDependency'
        ] as Set

        boolean ok = true
        expectedIds.each { id ->
            boolean present = actualIds.contains(id)
            println "${present ? 'PASS' : 'FAIL'}  expected finding: ${id}"
            if (!present) ok = false
        }
        Set<String> unexpected = actualIds - expectedIds
        if (unexpected) {
            println "FAIL  unexpected findings raised: ${unexpected}"
            ok = false
        }
        boolean noFindingForFreshLib = result.findings.every { it.component != 'com.example:fresh-lib:1.0.0' }
        println "${noFindingForFreshLib ? 'PASS' : 'FAIL'}  clean new component raised no finding"
        ok = ok && noFindingForFreshLib

        boolean cmpOk = GradleVerificationDriftGate.compareVersions('1.2.3', '1.2.10') < 0 &&
            GradleVerificationDriftGate.compareVersions('31.1-jre', '29.0-jre') > 0 &&
            GradleVerificationDriftGate.compareVersions('1.0.0', '1.0.0') == 0 &&
            GradleVerificationDriftGate.compareVersions('2.0', '2.0.0') == 0
        println "${cmpOk ? 'PASS' : 'FAIL'}  version comparator sanity checks"
        ok = ok && cmpOk

        println ok ? "\nSelf-test: ALL PASSED (${expectedIds.size()} rules exercised)" : "\nSelf-test: FAILED"
        return ok
    }
}

System.exit(runGate(args as List<String>))
