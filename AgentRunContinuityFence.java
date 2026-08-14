import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AgentRunContinuityFence {
    private static final int EXIT_USAGE = 2;
    private static final int EXIT_INPUT = 3;
    private static final int DEFAULT_SHOW_LINES = 10;
    private static final Pattern ISO_INSTANT = Pattern.compile(
            "\\b(20\\d{2}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?Z)\\b");
    private static final Pattern COMMIT_SHA = Pattern.compile("\\b[0-9a-f]{7,40}\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern FILE_TOKEN = Pattern.compile(
            "\\b[A-Z][A-Za-z0-9]*(?:\\.[A-Za-z0-9]+)?\\.(?:py|ts|tsx|js|mjs|rs|go|sh|kt|java|cs|swift|cpp|cc|cxx|hpp|dart|rb|php|zig|lua|ex|exs|scala|hs|ml|c|h|R|jl|nix|json|jsonl|yml|yaml|md)\\b");
    private static final Pattern HARD_CONSTRAINT = Pattern.compile(
            "\\b(must|required|never|do not|don't|only|exactly|without asking|forbidden|stop and report|exclusive|single permitted|real error)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DECISION = Pattern.compile(
            "\\b(decided|selected|chose|derived|computed|count\\s*%|slot|language|candidate|commit message|branch tip|latest commit)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PENDING = Pattern.compile(
            "\\b(todo|pending|next|needs|remaining|waiting|blocked|blocker|before returning|before committing|follow up)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ERROR = Pattern.compile(
            "\\b(error|failed|failure|exception|traceback|transport send error|http(?:error)?\\s*[45]\\d\\d|rejected|denied|timeout)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern COMPLETION = Pattern.compile(
            "\\b(done|pushed|published|created|committed|validated|readback|succeeded|passed|complete)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern WRITE_ACTION = Pattern.compile(
            "\\b(git clone|git push|git commit|git checkout|https://github\\.com|gh repo|curl\\s+-|_create_file|_update_file|_delete_file)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SECRET = Pattern.compile(
            "\\b(api[_-]?key|authorization:|bearer\\s+[a-z0-9._=-]{20,}|password\\s*=|secret\\s*=|token\\s*=)\\b",
            Pattern.CASE_INSENSITIVE);

    private AgentRunContinuityFence() {
    }

    public static void main(String[] args) {
        int exitCode;
        try {
            exitCode = run(args, System.out, System.err);
        } catch (UsageException e) {
            System.err.println(e.getMessage());
            exitCode = EXIT_USAGE;
        } catch (InputException e) {
            System.err.println(e.getMessage());
            exitCode = EXIT_INPUT;
        } catch (Exception e) {
            System.err.println("AgentRunContinuityFence failed: " + e.getMessage());
            e.printStackTrace(System.err);
            exitCode = 1;
        }
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, PrintStream out, PrintStream err) throws Exception {
        Config config = Config.parse(args);
        if (config.help) {
            printHelp(out);
            return 0;
        }
        if (config.selfTest) {
            selfTest();
            out.println("AgentRunContinuityFence self-test passed");
            return 0;
        }

        List<SourceLine> lines = readAll(config.inputs);
        ContinuityReport report = analyze(lines, config);
        Renderer.emit(report, config, out);
        if (config.failOn != Severity.NONE && report.maxSeverity.atLeast(config.failOn)) {
            err.println("continuity fence failed at severity " + report.maxSeverity.label);
            return 1;
        }
        return 0;
    }

    private static void printHelp(PrintStream out) {
        out.println("AgentRunContinuityFence");
        out.println("Build a replay-safe continuity packet from AI agent logs, Codex automation notes, MCP transcripts, or CI resumes.");
        out.println();
        out.println("Usage:");
        out.println("  java AgentRunContinuityFence [options] --input run.log");
        out.println("  cat transcript.jsonl | java AgentRunContinuityFence --format markdown --expect \"MCP-only\"");
        out.println();
        out.println("Options:");
        out.println("  --input <path|->             Read a transcript, memory file, or JSONL trace. Repeatable. Default: -");
        out.println("  --format <markdown|text|json> Output format. Default: markdown");
        out.println("  --expect <text>              Required phrase or token that must appear somewhere in the inputs. Repeatable");
        out.println("  --expected-file <path>       Expected artifact filename that must be visible in the run evidence");
        out.println("  --expected-commit <sha>      Expected commit SHA that must be visible in the run evidence");
        out.println("  --automation-id <id>         Automation identifier to require in the evidence when present");
        out.println("  --max-age-minutes <n>        Warn when the newest ISO-8601 timestamp is older than this many minutes");
        out.println("  --fail-on <none|info|warn|high|critical> Exit 1 when the max issue severity reaches this level");
        out.println("  --show-lines <n>             Evidence lines per section. Default: " + DEFAULT_SHOW_LINES);
        out.println("  --self-test                  Run built-in regression checks");
        out.println("  --help                       Show this help");
    }

    static ContinuityReport analyze(List<SourceLine> lines, Config config) {
        EvidenceIndex index = new EvidenceIndex(config.showLines);
        Set<String> allText = new LinkedHashSet<>();
        Instant newest = null;
        int writeActions = 0;
        int completionLines = 0;

        for (SourceLine line : lines) {
            String text = line.text;
            String normalized = normalize(text);
            if (!normalized.isEmpty()) {
                allText.add(normalized);
            }
            if (HARD_CONSTRAINT.matcher(text).find()) {
                index.add("hard constraints", line);
            }
            if (DECISION.matcher(text).find()) {
                index.add("decisions", line);
            }
            if (PENDING.matcher(text).find()) {
                index.add("pending work", line);
            }
            if (ERROR.matcher(text).find()) {
                index.add("errors and blockers", line);
            }
            if (COMPLETION.matcher(text).find()) {
                index.add("completion evidence", line);
                completionLines++;
            }
            if (WRITE_ACTION.matcher(text).find()) {
                index.add("write path evidence", line);
                writeActions++;
            }
            if (SECRET.matcher(text).find()) {
                index.add("secret-like text", line);
            }
            for (String file : matches(FILE_TOKEN, text)) {
                index.addArtifact(file, line);
            }
            for (String sha : matches(COMMIT_SHA, text)) {
                index.addCommit(sha.toLowerCase(Locale.ROOT), line);
            }
            Instant found = newestInstant(text);
            if (found != null && (newest == null || found.isAfter(newest))) {
                newest = found;
            }
        }

        List<Issue> issues = new ArrayList<>();
        addExpectationIssues(config, allText, index, issues);
        addContinuityIssues(config, lines, index, newest, writeActions, completionLines, issues);
        Severity max = Severity.maxOf(issues);
        String digest = digest(index.canonicalFacts());
        return new ContinuityReport(lines.size(), newest, digest, index, issues, max);
    }

    private static void addExpectationIssues(
            Config config,
            Set<String> allText,
            EvidenceIndex index,
            List<Issue> issues) {
        for (String expected : config.expectations) {
            String needle = normalize(expected);
            boolean present = false;
            for (String line : allText) {
                if (line.contains(needle)) {
                    present = true;
                    break;
                }
            }
            if (!present) {
                issues.add(new Issue(Severity.HIGH, "missing expected evidence", "Did not find required text: " + expected));
            }
        }
        if (config.expectedFile != null && !index.artifacts.containsKey(config.expectedFile)) {
            issues.add(new Issue(Severity.HIGH, "missing expected file", "Did not find artifact token: " + config.expectedFile));
        }
        if (config.expectedCommit != null && !index.commits.containsKey(config.expectedCommit.toLowerCase(Locale.ROOT))) {
            issues.add(new Issue(Severity.HIGH, "missing expected commit", "Did not find commit SHA: " + config.expectedCommit));
        }
        if (config.automationId != null) {
            String needle = normalize(config.automationId);
            boolean present = false;
            for (String line : allText) {
                if (line.contains(needle)) {
                    present = true;
                    break;
                }
            }
            if (!present) {
                issues.add(new Issue(Severity.WARN, "automation id absent", "No line contains automation id: " + config.automationId));
            }
        }
    }

    private static void addContinuityIssues(
            Config config,
            List<SourceLine> lines,
            EvidenceIndex index,
            Instant newest,
            int writeActions,
            int completionLines,
            List<Issue> issues) {
        if (lines.isEmpty()) {
            issues.add(new Issue(Severity.CRITICAL, "empty input", "No transcript, memory, or trace lines were available."));
            return;
        }
        if (index.bucket("hard constraints").isEmpty()) {
            issues.add(new Issue(Severity.HIGH, "no hard constraints", "A resumed agent run has no explicit must, never, only, or exactly constraints."));
        }
        if (index.bucket("decisions").isEmpty()) {
            issues.add(new Issue(Severity.WARN, "no decisions found", "No count, slot, branch, candidate, or commit decision evidence was extracted."));
        }
        if (!index.bucket("secret-like text").isEmpty()) {
            issues.add(new Issue(Severity.CRITICAL, "secret-like text", "The continuity packet contains possible credentials or bearer tokens."));
        }
        boolean mcpOnlyConstraint = containsAllTerms(index.bucket("hard constraints"), "mcp", "only");
        if (mcpOnlyConstraint && containsAnyTerm(index.bucket("write path evidence"), "git clone", "git push", "git commit", "https://github.com")) {
            issues.add(new Issue(Severity.CRITICAL, "write path contradiction", "The run has MCP-only language and also evidence of local git or HTTPS write attempts."));
        }
        boolean transportFailure = containsAllTerms(index.bucket("errors and blockers"), "transport send error");
        if (transportFailure && writeActions > 0 && completionLines == 0) {
            issues.add(new Issue(Severity.HIGH, "unsafe replay after transport failure", "A connector transport failure appears before any completion evidence."));
        }
        if (!index.bucket("pending work").isEmpty() && completionLines == 0) {
            issues.add(new Issue(Severity.WARN, "pending work without completion", "The run mentions pending work but no completion evidence."));
        }
        if (config.maxAgeMinutes != null && newest != null) {
            long age = Duration.between(newest, Instant.now()).toMinutes();
            if (age > config.maxAgeMinutes) {
                issues.add(new Issue(Severity.WARN, "stale latest timestamp", "Newest timestamp is " + age + " minutes old."));
            }
        }
        if (newest == null) {
            issues.add(new Issue(Severity.INFO, "no timestamp", "No ISO-8601 UTC timestamp was found for recency checks."));
        }
    }

    private static boolean containsAllTerms(List<SourceLine> lines, String... terms) {
        for (SourceLine line : lines) {
            String normalized = normalize(line.text);
            boolean all = true;
            for (String term : terms) {
                if (!normalized.contains(normalize(term))) {
                    all = false;
                    break;
                }
            }
            if (all) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAnyTerm(List<SourceLine> lines, String... terms) {
        for (SourceLine line : lines) {
            String normalized = normalize(line.text);
            for (String term : terms) {
                if (normalized.contains(normalize(term))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<SourceLine> readAll(List<String> inputs) throws IOException {
        List<String> effective = inputs.isEmpty() ? Collections.singletonList("-") : inputs;
        List<SourceLine> lines = new ArrayList<>();
        for (String input : effective) {
            if ("-".equals(input)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                    readFrom(reader, "<stdin>", lines);
                }
            } else {
                Path path = Path.of(input);
                if (!Files.exists(path)) {
                    throw new InputException("input does not exist: " + input);
                }
                if (!Files.isRegularFile(path)) {
                    throw new InputException("input is not a regular file: " + input);
                }
                try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                    readFrom(reader, input, lines);
                }
            }
        }
        return lines;
    }

    private static void readFrom(BufferedReader reader, String source, List<SourceLine> lines) throws IOException {
        String line;
        int number = 1;
        while ((line = reader.readLine()) != null) {
            lines.add(new SourceLine(source, number, line));
            number++;
        }
    }

    private static List<String> matches(Pattern pattern, String text) {
        List<String> values = new ArrayList<>();
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            values.add(matcher.group());
        }
        return values;
    }

    private static Instant newestInstant(String text) {
        Matcher matcher = ISO_INSTANT.matcher(text);
        Instant newest = null;
        while (matcher.find()) {
            try {
                Instant instant = Instant.parse(matcher.group(1));
                if (newest == null || instant.isAfter(newest)) {
                    newest = instant;
                }
            } catch (DateTimeParseException ignored) {
                // Regex already narrows the shape; invalid dates are simply not useful evidence.
            }
        }
        return newest;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
    }

    private static String digest(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] bytes = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                out.append(String.format(Locale.ROOT, "%02x", b & 0xff));
            }
            return out.substring(0, 24);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void selfTest() {
        List<SourceLine> good = List.of(
                new SourceLine("good.log", 1, "2026-04-20T10:15:30Z Automation ID: vibecodedaily"),
                new SourceLine("good.log", 2, "MUST use GitHub MCP only and create exactly one file."),
                new SourceLine("good.log", 3, "Computed 128 % 24 = 8 so selected Java."),
                new SourceLine("good.log", 4, "Created AgentRunContinuityFence.java at commit 1234567890abcdef1234567890abcdef12345678."),
                new SourceLine("good.log", 5, "Readback validated and pushed."));
        Config goodConfig = new Config();
        goodConfig.expectedFile = "AgentRunContinuityFence.java";
        goodConfig.expectedCommit = "1234567890abcdef1234567890abcdef12345678";
        ContinuityReport goodReport = analyze(good, goodConfig);
        require(!goodReport.maxSeverity.atLeast(Severity.HIGH), "good sample should not be high risk");

        List<SourceLine> bad = List.of(
                new SourceLine("bad.log", 1, "MUST use MCP only."),
                new SourceLine("bad.log", 2, "Transport send error while fetching main."),
                new SourceLine("bad.log", 3, "git push origin main"));
        ContinuityReport badReport = analyze(bad, new Config());
        require(badReport.maxSeverity == Severity.CRITICAL, "bad sample should detect write path contradiction");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    static final class Config {
        final List<String> inputs = new ArrayList<>();
        final List<String> expectations = new ArrayList<>();
        String format = "markdown";
        String expectedFile;
        String expectedCommit;
        String automationId;
        Integer maxAgeMinutes;
        Severity failOn = Severity.NONE;
        int showLines = DEFAULT_SHOW_LINES;
        boolean help;
        boolean selfTest;

        static Config parse(String[] args) {
            Config config = new Config();
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--input":
                        config.inputs.add(requireValue(args, ++i, arg));
                        break;
                    case "--format":
                        config.format = requireValue(args, ++i, arg).toLowerCase(Locale.ROOT);
                        if (!List.of("markdown", "text", "json").contains(config.format)) {
                            throw new UsageException("unknown format: " + config.format);
                        }
                        break;
                    case "--expect":
                        config.expectations.add(requireValue(args, ++i, arg));
                        break;
                    case "--expected-file":
                        config.expectedFile = requireValue(args, ++i, arg);
                        break;
                    case "--expected-commit":
                        config.expectedCommit = requireValue(args, ++i, arg);
                        break;
                    case "--automation-id":
                        config.automationId = requireValue(args, ++i, arg);
                        break;
                    case "--max-age-minutes":
                        config.maxAgeMinutes = positiveInt(requireValue(args, ++i, arg), arg);
                        break;
                    case "--fail-on":
                        config.failOn = Severity.parse(requireValue(args, ++i, arg));
                        break;
                    case "--show-lines":
                        config.showLines = positiveInt(requireValue(args, ++i, arg), arg);
                        break;
                    case "--self-test":
                        config.selfTest = true;
                        break;
                    case "--help":
                    case "-h":
                        config.help = true;
                        break;
                    default:
                        throw new UsageException("unknown option: " + arg);
                }
            }
            return config;
        }

        private static String requireValue(String[] args, int index, String option) {
            if (index >= args.length || args[index].startsWith("--")) {
                throw new UsageException("missing value for " + option);
            }
            return args[index];
        }

        private static int positiveInt(String value, String option) {
            try {
                int parsed = Integer.parseInt(value);
                if (parsed <= 0) {
                    throw new NumberFormatException("non-positive");
                }
                return parsed;
            } catch (NumberFormatException e) {
                throw new UsageException(option + " must be a positive integer: " + value);
            }
        }
    }

    enum Severity {
        NONE(0, "none"),
        INFO(1, "info"),
        WARN(2, "warn"),
        HIGH(3, "high"),
        CRITICAL(4, "critical");

        final int rank;
        final String label;

        Severity(int rank, String label) {
            this.rank = rank;
            this.label = label;
        }

        boolean atLeast(Severity other) {
            return this.rank >= other.rank;
        }

        static Severity parse(String value) {
            for (Severity severity : values()) {
                if (severity.label.equalsIgnoreCase(value)) {
                    return severity;
                }
            }
            throw new UsageException("unknown severity: " + value);
        }

        static Severity maxOf(List<Issue> issues) {
            Severity max = NONE;
            for (Issue issue : issues) {
                if (issue.severity.rank > max.rank) {
                    max = issue.severity;
                }
            }
            return max;
        }
    }

    static final class SourceLine {
        final String source;
        final int line;
        final String text;

        SourceLine(String source, int line, String text) {
            this.source = Objects.requireNonNull(source, "source");
            this.line = line;
            this.text = Objects.requireNonNull(text, "text");
        }

        String location() {
            return source + ":" + line;
        }
    }

    static final class Issue {
        final Severity severity;
        final String title;
        final String detail;

        Issue(Severity severity, String title, String detail) {
            this.severity = severity;
            this.title = title;
            this.detail = detail;
        }
    }

    static final class ContinuityReport {
        final int lineCount;
        final Instant newestTimestamp;
        final String digest;
        final EvidenceIndex index;
        final List<Issue> issues;
        final Severity maxSeverity;

        ContinuityReport(
                int lineCount,
                Instant newestTimestamp,
                String digest,
                EvidenceIndex index,
                List<Issue> issues,
                Severity maxSeverity) {
            this.lineCount = lineCount;
            this.newestTimestamp = newestTimestamp;
            this.digest = digest;
            this.index = index;
            this.issues = issues;
            this.maxSeverity = maxSeverity;
        }
    }

    static final class EvidenceIndex {
        final int limit;
        final Map<String, List<SourceLine>> buckets = new LinkedHashMap<>();
        final Map<String, SourceLine> artifacts = new LinkedHashMap<>();
        final Map<String, SourceLine> commits = new LinkedHashMap<>();

        EvidenceIndex(int limit) {
            this.limit = limit;
        }

        void add(String bucket, SourceLine line) {
            List<SourceLine> lines = buckets.computeIfAbsent(bucket, ignored -> new ArrayList<>());
            if (lines.size() < limit) {
                lines.add(line);
            }
        }

        void addArtifact(String artifact, SourceLine line) {
            artifacts.putIfAbsent(artifact, line);
        }

        void addCommit(String commit, SourceLine line) {
            commits.putIfAbsent(commit, line);
        }

        List<SourceLine> bucket(String bucket) {
            return buckets.getOrDefault(bucket, Collections.emptyList());
        }

        String canonicalFacts() {
            StringBuilder out = new StringBuilder();
            buckets.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> {
                        out.append("[").append(entry.getKey()).append("]\n");
                        for (SourceLine line : entry.getValue()) {
                            out.append(normalize(line.text)).append('\n');
                        }
                    });
            artifacts.keySet().stream().sorted().forEach(value -> out.append("artifact=").append(value).append('\n'));
            commits.keySet().stream().sorted().forEach(value -> out.append("commit=").append(value).append('\n'));
            return out.toString();
        }
    }

    static final class Renderer {
        static void emit(ContinuityReport report, Config config, PrintStream out) {
            switch (config.format) {
                case "json":
                    emitJson(report, out);
                    break;
                case "text":
                    emitText(report, out);
                    break;
                default:
                    emitMarkdown(report, out);
                    break;
            }
        }

        private static void emitMarkdown(ContinuityReport report, PrintStream out) {
            out.println("# Agent Run Continuity Fence");
            out.println();
            out.println("- max severity: " + report.maxSeverity.label);
            out.println("- lines scanned: " + report.lineCount);
            out.println("- newest timestamp: " + (report.newestTimestamp == null ? "not found" : report.newestTimestamp));
            out.println("- continuity digest: `" + report.digest + "`");
            out.println();
            emitIssuesMarkdown(report, out);
            emitEvidenceMarkdown(report, out);
        }

        private static void emitIssuesMarkdown(ContinuityReport report, PrintStream out) {
            out.println("## Issues");
            if (report.issues.isEmpty()) {
                out.println("- none");
            } else {
                for (Issue issue : sortedIssues(report.issues)) {
                    out.println("- [" + issue.severity.label + "] " + issue.title + ": " + issue.detail);
                }
            }
            out.println();
        }

        private static void emitEvidenceMarkdown(ContinuityReport report, PrintStream out) {
            out.println("## Evidence");
            for (Map.Entry<String, List<SourceLine>> entry : report.index.buckets.entrySet()) {
                out.println("### " + entry.getKey());
                for (SourceLine line : entry.getValue()) {
                    out.println("- `" + escapeMarkdown(line.location()) + "` " + escapeMarkdown(line.text));
                }
                out.println();
            }
            out.println("### artifacts");
            emitMap(report.index.artifacts, out);
            out.println();
            out.println("### commits");
            emitMap(report.index.commits, out);
        }

        private static void emitText(ContinuityReport report, PrintStream out) {
            out.println("max_severity=" + report.maxSeverity.label);
            out.println("lines_scanned=" + report.lineCount);
            out.println("newest_timestamp=" + (report.newestTimestamp == null ? "" : report.newestTimestamp));
            out.println("continuity_digest=" + report.digest);
            for (Issue issue : sortedIssues(report.issues)) {
                out.println("issue." + issue.severity.label + "=" + issue.title + " - " + issue.detail);
            }
        }

        private static void emitJson(ContinuityReport report, PrintStream out) {
            StringBuilder json = new StringBuilder();
            json.append("{");
            field(json, "maxSeverity", report.maxSeverity.label).append(',');
            json.append("\"lineCount\":").append(report.lineCount).append(',');
            field(json, "newestTimestamp", report.newestTimestamp == null ? null : report.newestTimestamp.toString()).append(',');
            field(json, "continuityDigest", report.digest).append(',');
            json.append("\"issues\":[");
            for (int i = 0; i < sortedIssues(report.issues).size(); i++) {
                Issue issue = sortedIssues(report.issues).get(i);
                if (i > 0) {
                    json.append(',');
                }
                json.append('{');
                field(json, "severity", issue.severity.label).append(',');
                field(json, "title", issue.title).append(',');
                field(json, "detail", issue.detail);
                json.append('}');
            }
            json.append("],\"artifacts\":[");
            appendKeys(json, report.index.artifacts.keySet());
            json.append("],\"commits\":[");
            appendKeys(json, report.index.commits.keySet());
            json.append("]}");
            out.println(json);
        }

        private static List<Issue> sortedIssues(List<Issue> issues) {
            List<Issue> copy = new ArrayList<>(issues);
            copy.sort(Comparator.comparing((Issue i) -> i.severity.rank).reversed().thenComparing(i -> i.title));
            return copy;
        }

        private static void emitMap(Map<String, SourceLine> map, PrintStream out) {
            if (map.isEmpty()) {
                out.println("- none");
                return;
            }
            for (Map.Entry<String, SourceLine> entry : map.entrySet()) {
                out.println("- `" + escapeMarkdown(entry.getKey()) + "` at `" + escapeMarkdown(entry.getValue().location()) + "`");
            }
        }

        private static StringBuilder field(StringBuilder json, String name, String value) {
            json.append('"').append(escapeJson(name)).append("\":");
            if (value == null) {
                json.append("null");
            } else {
                json.append('"').append(escapeJson(value)).append('"');
            }
            return json;
        }

        private static void appendKeys(StringBuilder json, Set<String> keys) {
            int index = 0;
            for (String key : keys) {
                if (index++ > 0) {
                    json.append(',');
                }
                json.append('"').append(escapeJson(key)).append('"');
            }
        }

        private static String escapeJson(String value) {
            StringBuilder out = new StringBuilder(value.length() + 16);
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '\\':
                        out.append("\\\\");
                        break;
                    case '"':
                        out.append("\\\"");
                        break;
                    case '\n':
                        out.append("\\n");
                        break;
                    case '\r':
                        out.append("\\r");
                        break;
                    case '\t':
                        out.append("\\t");
                        break;
                    default:
                        if (c < 0x20) {
                            out.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                        } else {
                            out.append(c);
                        }
                }
            }
            return out.toString();
        }

        private static String escapeMarkdown(String value) {
            return value.replace("`", "'");
        }
    }

    static final class UsageException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        UsageException(String message) {
            super(message);
        }
    }

    static final class InputException extends IOException {
        private static final long serialVersionUID = 1L;

        InputException(String message) {
            super(message);
        }
    }
}

/*
This solves the quiet failure that happens when a long running AI coding agent, Codex automation, MCP connector job, CI repair loop, or research pipeline resumes after context compaction and forgets the hard rules that made the previous work safe. Built because by April 2026 more developer workflows depend on autonomous agent transcripts, GitHub MCP publish logs, JSONL traces, and automation memory files, but most teams still resume from a vague summary instead of a verifiable continuity packet. Use it when you need an agent run continuity checker, context compaction guard, MCP-only workflow auditor, AI agent replay fence, Codex task memory validator, or CI gate that proves the next step still knows the required branch, filename, commit, blocker, write path, and final response contract. The trick: it does not try to be a chatbot; it extracts hard constraints, decisions, artifacts, commits, timestamps, errors, pending work, completion evidence, and write-path contradictions, then builds a small digest that can be compared across resumes. Drop this into agentic DevOps repos, AI tooling automation, eval pipelines, model release workflows, and developer productivity systems where a stale resume can push the wrong file, use the wrong connector, leak a token, ignore a transport failure, or report success before readback actually happened.
*/