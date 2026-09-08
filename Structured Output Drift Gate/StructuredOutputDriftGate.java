import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public final class StructuredOutputDriftGate {
    private static final int EXIT_USAGE = 2;
    private static final int EXIT_INPUT = 3;
    private static final int DEFAULT_SUMMARY_LIMIT = 12;
    private static final String DEFAULT_COHORT = "default";

    private StructuredOutputDriftGate() {
    }

    public static void main(String[] args) {
        int exitCode = 0;
        try {
            exitCode = run(args);
        } catch (UsageException e) {
            System.err.println(e.getMessage());
            exitCode = EXIT_USAGE;
        } catch (InputException e) {
            System.err.println(e.getMessage());
            exitCode = EXIT_INPUT;
        } catch (Exception e) {
            System.err.println("StructuredOutputDriftGate failed: " + e.getMessage());
            e.printStackTrace(System.err);
            exitCode = 1;
        }
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static int run(String[] args) throws Exception {
        Config config = Config.parse(args);
        if (config.help) {
            printHelp();
            return 0;
        }

        List<RunRecord> records = readRecords(config);
        AnalysisReport report = analyze(records, config);

        if (config.jsonOutput != null) {
            writeJsonReport(report, config.jsonOutput);
        }
        emitHumanSummary(report, config);

        return report.unstableGroups > 0 && config.failOnUnstable ? 1 : 0;
    }

    private static void printHelp() {
        System.out.println("StructuredOutputDriftGate");
        System.out.println("Find schema drift and value instability in repeated LLM structured-output runs.");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java StructuredOutputDriftGate [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --input <path|->                NDJSON input file. Default: - (stdin)");
        System.out.println("  --scenario-key <field|/ptr>     Grouping key for the scenario id. Default: scenario");
        System.out.println("  --cohort-key <field|/ptr>       Optional key for model, prompt, or release cohort");
        System.out.println("  --output-key <field|/ptr>       Key that holds the structured output. Default: output");
        System.out.println("  --min-runs <n>                  Minimum runs required per scenario/cohort. Default: 2");
        System.out.println("  --numeric-abs-tolerance <n>     Allowed absolute numeric drift before flagging. Default: 0");
        System.out.println("  --numeric-rel-tolerance <n>     Allowed relative numeric drift before flagging. Default: 0");
        System.out.println("  --ignore-pointer <glob>         Ignore JSON pointers like /request_id or /items/*/id");
        System.out.println("  --json-output <path|->          Write a machine-readable JSON report");
        System.out.println("  --summary-limit <n>             Max pointers and groups to show in stderr summary. Default: 12");
        System.out.println("  --fail-on-unstable              Exit 1 when unstable groups exist. Default: on");
        System.out.println("  --no-fail-on-unstable           Always exit 0 after analysis");
        System.out.println("  --help                          Show this help");
        System.out.println();
        System.out.println("Notes:");
        System.out.println("  Keys can be plain top-level fields like scenario or RFC 6901 JSON pointers like /meta/case_id.");
        System.out.println("  If the output field is a string that looks like a JSON object or array, it is parsed automatically.");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java StructuredOutputDriftGate \\");
        System.out.println("    --input eval.ndjson \\");
        System.out.println("    --cohort-key model_release \\");
        System.out.println("    --ignore-pointer '/request_id' \\");
        System.out.println("    --ignore-pointer '/items/*/generated_at' \\");
        System.out.println("    --json-output drift-report.json");
    }

    private static List<RunRecord> readRecords(Config config) throws IOException {
        BufferedReader reader = null;
        boolean closeReader = true;
        try {
            if ("-".equals(config.input)) {
                reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
                closeReader = false;
            } else {
                reader = Files.newBufferedReader(Paths.get(config.input), StandardCharsets.UTF_8);
            }

            List<RunRecord> records = new ArrayList<>();
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }

                Object parsed;
                try {
                    parsed = Json.parse(line);
                } catch (IllegalArgumentException e) {
                    throw new InputException("Line " + lineNumber + " is not valid JSON: " + e.getMessage());
                }

                if (!(parsed instanceof Map<?, ?> rawRecord)) {
                    throw new InputException("Line " + lineNumber + " must be a JSON object.");
                }

                Map<String, Object> record = asObjectMap(rawRecord);
                String scenario = readGroupingKey(record, config.scenarioKey, lineNumber, "scenario");
                String cohort = config.cohortKey == null
                        ? DEFAULT_COHORT
                        : readGroupingKey(record, config.cohortKey, lineNumber, "cohort");

                Object outputRaw = resolve(record, config.outputKey);
                if (outputRaw == Missing.VALUE) {
                    throw new InputException("Line " + lineNumber + " is missing output at " + config.outputKey + ".");
                }

                Object output = coerceOutput(outputRaw, lineNumber, config.outputKey);
                Metrics metrics = Metrics.fromRecord(record);
                records.add(new RunRecord(lineNumber, scenario, cohort, output, metrics));
            }

            if (records.isEmpty()) {
                throw new InputException("No JSON records were read from " + config.input + ".");
            }
            return records;
        } finally {
            if (reader != null && closeReader) {
                reader.close();
            }
        }
    }

    private static String readGroupingKey(
            Map<String, Object> record,
            String keySpec,
            int lineNumber,
            String label) {
        Object value = resolve(record, keySpec);
        if (value == Missing.VALUE) {
            throw new InputException("Line " + lineNumber + " is missing " + label + " at " + keySpec + ".");
        }
        if (value instanceof Map<?, ?> || value instanceof List<?>) {
            throw new InputException("Line " + lineNumber + " has non-scalar " + label + " at " + keySpec + ".");
        }

        String text = scalarToText(value).trim();
        if (text.isEmpty()) {
            throw new InputException("Line " + lineNumber + " has empty " + label + " at " + keySpec + ".");
        }
        return text;
    }

    private static Object coerceOutput(Object outputRaw, int lineNumber, String outputKey) {
        if (outputRaw instanceof String text) {
            String trimmed = text.trim();
            if (looksLikeJsonContainer(trimmed)) {
                try {
                    return Json.parse(trimmed);
                } catch (IllegalArgumentException e) {
                    throw new InputException(
                            "Line " + lineNumber + " contains malformed JSON text at " + outputKey + ": " + e.getMessage());
                }
            }
            return text;
        }
        return outputRaw;
    }

    private static boolean looksLikeJsonContainer(String text) {
        return (text.startsWith("{") && text.endsWith("}")) || (text.startsWith("[") && text.endsWith("]"));
    }

    private static AnalysisReport analyze(List<RunRecord> records, Config config) {
        Map<GroupKey, List<RunRecord>> grouped = new LinkedHashMap<>();
        Map<String, CohortStats> cohortStatsByName = new LinkedHashMap<>();

        for (RunRecord record : records) {
            GroupKey key = new GroupKey(record.cohort, record.scenario);
            grouped.computeIfAbsent(key, unused -> new ArrayList<>()).add(record);
            cohortStatsByName.computeIfAbsent(record.cohort, CohortStats::new).observeRun(record.metrics);
        }

        List<GroupReport> groups = new ArrayList<>();
        Map<String, AggregatedIssue> aggregated = new LinkedHashMap<>();
        int analyzedGroups = 0;
        int skippedGroups = 0;
        int unstableGroups = 0;

        for (Map.Entry<GroupKey, List<RunRecord>> entry : grouped.entrySet()) {
            GroupKey key = entry.getKey();
            List<RunRecord> runs = entry.getValue();
            CohortStats cohortStats = cohortStatsByName.get(key.cohort);

            cohortStats.groupsTotal++;
            if (runs.size() < config.minRuns) {
                cohortStats.groupsSkipped++;
                skippedGroups++;
                continue;
            }

            GroupReport group = analyzeGroup(key, runs, config);
            groups.add(group);
            cohortStats.groupsAnalyzed++;
            analyzedGroups++;

            if (group.unstable) {
                cohortStats.unstableGroups++;
                unstableGroups++;
            }

            for (Issue issue : group.issues) {
                String aggregateKey = issue.cohort + "\u0000" + issue.pointer + "\u0000" + issue.kind.wireName;
                aggregated.computeIfAbsent(
                                aggregateKey,
                                unused -> new AggregatedIssue(issue.cohort, issue.pointer, issue.kind))
                        .observe(issue);
            }
        }

        groups.sort(Comparator
                .comparing((GroupReport group) -> !group.unstable)
                .thenComparing(Comparator.comparingInt((GroupReport group) -> group.issues.size()).reversed())
                .thenComparing(Comparator.comparingInt((GroupReport group) -> group.runs).reversed())
                .thenComparing(group -> group.cohort)
                .thenComparing(group -> group.scenario));

        List<AggregatedIssue> topPointers = new ArrayList<>(aggregated.values());
        topPointers.sort(Comparator
                .comparingInt((AggregatedIssue issue) -> issue.kind.severity).reversed()
                .thenComparing(Comparator.comparingInt((AggregatedIssue issue) -> issue.groupsAffected).reversed())
                .thenComparing(issue -> issue.cohort)
                .thenComparing(issue -> issue.pointer));

        List<CohortStats> cohorts = new ArrayList<>(cohortStatsByName.values());
        cohorts.sort(Comparator
                .comparingInt((CohortStats cohort) -> cohort.unstableGroups).reversed()
                .thenComparing(cohort -> cohort.name));

        return new AnalysisReport(
                Instant.now().toString(),
                config,
                records.size(),
                grouped.size(),
                analyzedGroups,
                skippedGroups,
                unstableGroups,
                cohorts,
                topPointers,
                groups);
    }

    private static GroupReport analyzeGroup(GroupKey key, List<RunRecord> runs, Config config) {
        Map<String, PathAccumulator> paths = new LinkedHashMap<>();
        Map<String, Integer> rootVariants = new LinkedHashMap<>();
        NumericStats latencyMs = new NumericStats();
        NumericStats inputTokens = new NumericStats();
        NumericStats outputTokens = new NumericStats();
        NumericStats reasoningTokens = new NumericStats();

        for (RunRecord run : runs) {
            latencyMs.add(run.metrics.latencyMs);
            inputTokens.add(run.metrics.inputTokens);
            outputTokens.add(run.metrics.outputTokens);
            reasoningTokens.add(run.metrics.reasoningTokens);

            String rootCanonical = Json.toCanonicalJson(run.output);
            rootVariants.merge(rootCanonical, 1, Integer::sum);

            Map<String, NodeValue> nodes = new LinkedHashMap<>();
            flatten("", run.output, nodes);
            for (Map.Entry<String, NodeValue> entry : nodes.entrySet()) {
                String pointer = entry.getKey();
                if (config.isIgnored(pointer)) {
                    continue;
                }
                paths.computeIfAbsent(pointer, PathAccumulator::new).observe(entry.getValue());
            }
        }

        List<Issue> issues = new ArrayList<>();
        List<String> pointers = new ArrayList<>(paths.keySet());
        Collections.sort(pointers);
        for (String pointer : pointers) {
            issues.addAll(paths.get(pointer).finish(key, runs.size(), config));
        }

        issues.sort(Comparator
                .comparingInt((Issue issue) -> issue.kind.severity).reversed()
                .thenComparing(issue -> issue.pointer));

        return new GroupReport(
                key.cohort,
                key.scenario,
                runs.size(),
                !issues.isEmpty(),
                issues,
                rootVariants.size(),
                toSampleCounts(rootVariants, 3, 180),
                latencyMs,
                inputTokens,
                outputTokens,
                reasoningTokens);
    }

    // Container nodes are recorded to catch presence and type drift; scalar and array-length drift are derived later.
    private static void flatten(String pointer, Object value, Map<String, NodeValue> sink) {
        String normalizedPointer = pointer.isEmpty() ? "/" : pointer;
        sink.put(normalizedPointer, NodeValue.from(value));

        if (value instanceof Map<?, ?> rawObject) {
            Map<String, Object> object = asObjectMap(rawObject);
            List<String> keys = new ArrayList<>(object.keySet());
            Collections.sort(keys);
            for (String key : keys) {
                flatten(appendPointer(pointer, key), object.get(key), sink);
            }
            return;
        }

        if (value instanceof List<?> rawList) {
            List<Object> list = asList(rawList);
            for (int i = 0; i < list.size(); i++) {
                flatten(appendPointer(pointer, Integer.toString(i)), list.get(i), sink);
            }
        }
    }

    private static Object resolve(Map<String, Object> root, String keySpec) {
        if (keySpec == null || keySpec.isEmpty()) {
            return Missing.VALUE;
        }

        if (!keySpec.startsWith("/")) {
            return root.containsKey(keySpec) ? root.get(keySpec) : Missing.VALUE;
        }

        if ("/".equals(keySpec)) {
            return root;
        }

        Object current = root;
        String[] tokens = keySpec.substring(1).split("/", -1);
        for (String rawToken : tokens) {
            String token = decodePointerToken(rawToken);
            if (current instanceof Map<?, ?> rawObject) {
                Map<String, Object> object = asObjectMap(rawObject);
                if (!object.containsKey(token)) {
                    return Missing.VALUE;
                }
                current = object.get(token);
                continue;
            }
            if (current instanceof List<?> rawList) {
                List<Object> list = asList(rawList);
                int index = parseIndex(token);
                if (index < 0 || index >= list.size()) {
                    return Missing.VALUE;
                }
                current = list.get(index);
                continue;
            }
            return Missing.VALUE;
        }

        return current;
    }

    private static String appendPointer(String parentPointer, String token) {
        String encoded = encodePointerToken(token);
        return parentPointer.isEmpty() ? "/" + encoded : parentPointer + "/" + encoded;
    }

    private static String encodePointerToken(String token) {
        return token.replace("~", "~0").replace("/", "~1");
    }

    private static String decodePointerToken(String token) {
        return token.replace("~1", "/").replace("~0", "~");
    }

    private static int parseIndex(String token) {
        try {
            return Integer.parseInt(token);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static String scalarToText(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof BigDecimal decimal) {
            return normalizeNumber(decimal);
        }
        return value.toString();
    }

    private static void writeJsonReport(AnalysisReport report, String target) throws IOException {
        String payload = Json.toCanonicalJson(report.toJsonMap()) + System.lineSeparator();
        if ("-".equals(target)) {
            System.out.print(payload);
            return;
        }
        Files.writeString(Paths.get(target), payload, StandardCharsets.UTF_8);
    }

    private static void emitHumanSummary(AnalysisReport report, Config config) {
        StringBuilder out = new StringBuilder();
        out.append("StructuredOutputDriftGate").append('\n');
        out.append("records=").append(report.totalRecords)
                .append(" groups=").append(report.totalGroups)
                .append(" analyzed=").append(report.analyzedGroups)
                .append(" skipped=").append(report.skippedGroups)
                .append(" unstable=").append(report.unstableGroups)
                .append(" stable=").append(report.analyzedGroups - report.unstableGroups)
                .append('\n');

        if (!report.cohorts.isEmpty()) {
            out.append('\n').append("Cohorts").append('\n');
            for (CohortStats cohort : report.cohorts) {
                out.append("  - ").append(cohort.name)
                        .append(": groups=").append(cohort.groupsAnalyzed)
                        .append(" unstable=").append(cohort.unstableGroups);
                if (cohort.latencyMs.hasData()) {
                    out.append(" avg_latency_ms=").append(formatDouble(cohort.latencyMs.mean()));
                }
                if (cohort.reasoningTokens.hasData()) {
                    out.append(" avg_reasoning_tokens=").append(formatDouble(cohort.reasoningTokens.mean()));
                }
                out.append('\n');
            }
        }

        if (!report.topPointers.isEmpty()) {
            out.append('\n').append("Top Unstable Pointers").append('\n');
            int limit = Math.min(config.summaryLimit, report.topPointers.size());
            for (int i = 0; i < limit; i++) {
                AggregatedIssue issue = report.topPointers.get(i);
                out.append("  ").append(i + 1).append(". ")
                        .append("cohort=").append(issue.cohort)
                        .append(" pointer=").append(issue.pointer)
                        .append(" kind=").append(issue.kind.wireName)
                        .append(" groups=").append(issue.groupsAffected);
                if (!issue.types.isEmpty()) {
                    out.append(" types=").append(String.join(",", issue.types));
                }
                if (!issue.samples().isEmpty()) {
                    out.append(" samples=").append(renderSamples(issue.samples(), 3));
                }
                if (issue.kind == IssueKind.PRESENCE_DRIFT && issue.totalRunsSum > 0) {
                    out.append(" avg_present=").append(issue.presentRunsSum).append("/").append(issue.totalRunsSum);
                }
                if (issue.kind == IssueKind.NUMERIC_DRIFT) {
                    out.append(" max_rel_spread=").append(formatDouble(issue.maxRelativeSpread));
                }
                out.append('\n');
            }
        }

        List<GroupReport> unstableGroups = new ArrayList<>();
        for (GroupReport group : report.groups) {
            if (group.unstable) {
                unstableGroups.add(group);
            }
        }
        if (!unstableGroups.isEmpty()) {
            out.append('\n').append("Worst Groups").append('\n');
            int limit = Math.min(config.summaryLimit, unstableGroups.size());
            for (int i = 0; i < limit; i++) {
                GroupReport group = unstableGroups.get(i);
                out.append("  ").append(i + 1).append(". ")
                        .append("cohort=").append(group.cohort)
                        .append(" scenario=").append(group.scenario)
                        .append(" issues=").append(group.issues.size())
                        .append(" runs=").append(group.runs)
                        .append(" root_variants=").append(group.rootVariantCount);
                if (!group.issues.isEmpty()) {
                    Issue first = group.issues.get(0);
                    out.append(" top_issue=").append(first.kind.wireName)
                            .append("@").append(first.pointer);
                }
                out.append('\n');
            }
        }

        System.err.print(out.toString());
    }

    private static String renderSamples(List<SampleCount> samples, int limit) {
        List<String> parts = new ArrayList<>();
        int count = Math.min(limit, samples.size());
        for (int i = 0; i < count; i++) {
            SampleCount sample = samples.get(i);
            parts.add(sample.count + "x " + sample.value);
        }
        return String.join(", ", parts);
    }

    private static String formatDouble(double value) {
        return String.format(Locale.ROOT, "%.4f", value);
    }

    private static String truncate(String text, int maxChars) {
        if (text.length() <= maxChars) {
            return text;
        }
        if (maxChars <= 3) {
            return text.substring(0, Math.max(0, maxChars));
        }
        return text.substring(0, maxChars - 3) + "...";
    }

    private static String normalizeNumber(BigDecimal value) {
        BigDecimal normalized = value.stripTrailingZeros();
        if (normalized.scale() < 0) {
            normalized = normalized.setScale(0);
        }
        return normalized.toPlainString();
    }

    private static double relativeSpread(BigDecimal min, BigDecimal max) {
        BigDecimal absSpread = max.subtract(min).abs();
        BigDecimal absMin = min.abs();
        BigDecimal absMax = max.abs();
        BigDecimal denominator = absMin.max(absMax);
        if (denominator.compareTo(BigDecimal.ZERO) == 0) {
            return absSpread.compareTo(BigDecimal.ZERO) == 0 ? 0.0d : Double.POSITIVE_INFINITY;
        }
        return absSpread.doubleValue() / denominator.doubleValue();
    }

    private static List<String> toTypeLabels(Map<JsonType, Integer> typeCounts) {
        List<String> labels = new ArrayList<>();
        for (JsonType type : typeCounts.keySet()) {
            labels.add(type.wireName);
        }
        Collections.sort(labels);
        return labels;
    }

    private static List<SampleCount> toSampleCounts(Map<String, Integer> counts, int limit, int maxValueChars) {
        List<SampleCount> samples = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            samples.add(new SampleCount(truncate(entry.getKey(), maxValueChars), entry.getValue()));
        }
        samples.sort(Comparator
                .comparingInt((SampleCount sample) -> sample.count).reversed()
                .thenComparing(sample -> sample.value));
        if (samples.size() <= limit) {
            return samples;
        }
        return new ArrayList<>(samples.subList(0, limit));
    }

    private static List<SampleCount> toSampleCountsFromIntegers(Map<Integer, Integer> counts, int limit) {
        Map<String, Integer> converted = new LinkedHashMap<>();
        for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
            converted.put(Integer.toString(entry.getKey()), entry.getValue());
        }
        return toSampleCounts(converted, limit, 48);
    }

    private static final class Config {
        final String input;
        final String scenarioKey;
        final String cohortKey;
        final String outputKey;
        final int minRuns;
        final BigDecimal numericAbsTolerance;
        final double numericRelTolerance;
        final List<Pattern> ignorePatterns;
        final String jsonOutput;
        final int summaryLimit;
        final boolean failOnUnstable;
        final boolean help;

        private Config(
                String input,
                String scenarioKey,
                String cohortKey,
                String outputKey,
                int minRuns,
                BigDecimal numericAbsTolerance,
                double numericRelTolerance,
                List<Pattern> ignorePatterns,
                String jsonOutput,
                int summaryLimit,
                boolean failOnUnstable,
                boolean help) {
            this.input = input;
            this.scenarioKey = scenarioKey;
            this.cohortKey = cohortKey;
            this.outputKey = outputKey;
            this.minRuns = minRuns;
            this.numericAbsTolerance = numericAbsTolerance;
            this.numericRelTolerance = numericRelTolerance;
            this.ignorePatterns = ignorePatterns;
            this.jsonOutput = jsonOutput;
            this.summaryLimit = summaryLimit;
            this.failOnUnstable = failOnUnstable;
            this.help = help;
        }

        static Config parse(String[] args) {
            String input = "-";
            String scenarioKey = "scenario";
            String cohortKey = null;
            String outputKey = "output";
            int minRuns = 2;
            BigDecimal numericAbsTolerance = BigDecimal.ZERO;
            double numericRelTolerance = 0.0d;
            List<String> ignoreGlobs = new ArrayList<>();
            String jsonOutput = null;
            int summaryLimit = DEFAULT_SUMMARY_LIMIT;
            boolean failOnUnstable = true;
            boolean help = false;

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--input":
                        input = requireValue(args, ++i, arg);
                        break;
                    case "--scenario-key":
                        scenarioKey = requireValue(args, ++i, arg);
                        break;
                    case "--cohort-key":
                        cohortKey = requireValue(args, ++i, arg);
                        break;
                    case "--output-key":
                        outputKey = requireValue(args, ++i, arg);
                        break;
                    case "--min-runs":
                        minRuns = parseInt(requireValue(args, ++i, arg), arg);
                        break;
                    case "--numeric-abs-tolerance":
                        numericAbsTolerance = parseDecimal(requireValue(args, ++i, arg), arg);
                        break;
                    case "--numeric-rel-tolerance":
                        numericRelTolerance = parseDouble(requireValue(args, ++i, arg), arg);
                        break;
                    case "--ignore-pointer":
                        ignoreGlobs.add(requireValue(args, ++i, arg));
                        break;
                    case "--json-output":
                        jsonOutput = requireValue(args, ++i, arg);
                        break;
                    case "--summary-limit":
                        summaryLimit = parseInt(requireValue(args, ++i, arg), arg);
                        break;
                    case "--fail-on-unstable":
                        failOnUnstable = true;
                        break;
                    case "--no-fail-on-unstable":
                        failOnUnstable = false;
                        break;
                    case "--help":
                        help = true;
                        break;
                    default:
                        throw new UsageException("Unknown option: " + arg);
                }
            }

            if (minRuns < 2) {
                throw new UsageException("--min-runs must be at least 2.");
            }
            if (numericAbsTolerance.compareTo(BigDecimal.ZERO) < 0) {
                throw new UsageException("--numeric-abs-tolerance must be non-negative.");
            }
            if (numericRelTolerance < 0.0d) {
                throw new UsageException("--numeric-rel-tolerance must be non-negative.");
            }
            if (summaryLimit < 1) {
                throw new UsageException("--summary-limit must be at least 1.");
            }

            List<Pattern> ignorePatterns = new ArrayList<>();
            for (String glob : ignoreGlobs) {
                ignorePatterns.add(globToPattern(glob));
            }

            return new Config(
                    input,
                    scenarioKey,
                    cohortKey,
                    outputKey,
                    minRuns,
                    numericAbsTolerance,
                    numericRelTolerance,
                    ignorePatterns,
                    jsonOutput,
                    summaryLimit,
                    failOnUnstable,
                    help);
        }

        boolean isIgnored(String pointer) {
            for (Pattern pattern : ignorePatterns) {
                if (pattern.matcher(pointer).matches()) {
                    return true;
                }
            }
            return false;
        }

        private static String requireValue(String[] args, int index, String flag) {
            if (index >= args.length) {
                throw new UsageException("Missing value for " + flag + ".");
            }
            return args[index];
        }

        private static int parseInt(String value, String flag) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new UsageException(flag + " expects an integer, got: " + value);
            }
        }

        private static BigDecimal parseDecimal(String value, String flag) {
            try {
                return new BigDecimal(value);
            } catch (NumberFormatException e) {
                throw new UsageException(flag + " expects a decimal number, got: " + value);
            }
        }

        private static double parseDouble(String value, String flag) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new UsageException(flag + " expects a number, got: " + value);
            }
        }

        private static Pattern globToPattern(String glob) {
            StringBuilder regex = new StringBuilder();
            regex.append('^');
            for (int i = 0; i < glob.length(); i++) {
                char ch = glob.charAt(i);
                if (ch == '*') {
                    regex.append(".*");
                    continue;
                }
                if ("\\.[]{}()+-^$?|".indexOf(ch) >= 0) {
                    regex.append('\\');
                }
                regex.append(ch);
            }
            regex.append('$');
            return Pattern.compile(regex.toString());
        }

        Map<String, Object> toJsonMap() {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("input", input);
            root.put("scenario_key", scenarioKey);
            root.put("cohort_key", cohortKey);
            root.put("output_key", outputKey);
            root.put("min_runs", minRuns);
            root.put("numeric_abs_tolerance", numericAbsTolerance);
            root.put("numeric_rel_tolerance", numericRelTolerance);
            root.put("json_output", jsonOutput);
            root.put("summary_limit", summaryLimit);
            root.put("fail_on_unstable", failOnUnstable);
            return root;
        }
    }

    private static final class AnalysisReport {
        final String generatedAtUtc;
        final Config config;
        final int totalRecords;
        final int totalGroups;
        final int analyzedGroups;
        final int skippedGroups;
        final int unstableGroups;
        final List<CohortStats> cohorts;
        final List<AggregatedIssue> topPointers;
        final List<GroupReport> groups;

        private AnalysisReport(
                String generatedAtUtc,
                Config config,
                int totalRecords,
                int totalGroups,
                int analyzedGroups,
                int skippedGroups,
                int unstableGroups,
                List<CohortStats> cohorts,
                List<AggregatedIssue> topPointers,
                List<GroupReport> groups) {
            this.generatedAtUtc = generatedAtUtc;
            this.config = config;
            this.totalRecords = totalRecords;
            this.totalGroups = totalGroups;
            this.analyzedGroups = analyzedGroups;
            this.skippedGroups = skippedGroups;
            this.unstableGroups = unstableGroups;
            this.cohorts = cohorts;
            this.topPointers = topPointers;
            this.groups = groups;
        }

        Map<String, Object> toJsonMap() {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("tool", "StructuredOutputDriftGate");
            root.put("generated_at_utc", generatedAtUtc);
            root.put("config", config.toJsonMap());
            root.put("total_records", totalRecords);
            root.put("total_groups", totalGroups);
            root.put("analyzed_groups", analyzedGroups);
            root.put("skipped_groups", skippedGroups);
            root.put("unstable_groups", unstableGroups);

            List<Object> cohortMaps = new ArrayList<>();
            for (CohortStats cohort : cohorts) {
                cohortMaps.add(cohort.toJsonMap());
            }
            root.put("cohorts", cohortMaps);

            List<Object> pointerMaps = new ArrayList<>();
            for (AggregatedIssue issue : topPointers) {
                pointerMaps.add(issue.toJsonMap());
            }
            root.put("top_pointers", pointerMaps);

            List<Object> groupMaps = new ArrayList<>();
            for (GroupReport group : groups) {
                groupMaps.add(group.toJsonMap());
            }
            root.put("groups", groupMaps);
            return root;
        }
    }

    private static final class CohortStats {
        final String name;
        int groupsTotal;
        int groupsAnalyzed;
        int groupsSkipped;
        int unstableGroups;
        final NumericStats latencyMs = new NumericStats();
        final NumericStats inputTokens = new NumericStats();
        final NumericStats outputTokens = new NumericStats();
        final NumericStats reasoningTokens = new NumericStats();

        private CohortStats(String name) {
            this.name = name;
        }

        void observeRun(Metrics metrics) {
            latencyMs.add(metrics.latencyMs);
            inputTokens.add(metrics.inputTokens);
            outputTokens.add(metrics.outputTokens);
            reasoningTokens.add(metrics.reasoningTokens);
        }

        Map<String, Object> toJsonMap() {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("name", name);
            root.put("groups_total", groupsTotal);
            root.put("groups_analyzed", groupsAnalyzed);
            root.put("groups_skipped", groupsSkipped);
            root.put("unstable_groups", unstableGroups);
            root.put("latency_ms", latencyMs.toJsonMap());
            root.put("input_tokens", inputTokens.toJsonMap());
            root.put("output_tokens", outputTokens.toJsonMap());
            root.put("reasoning_tokens", reasoningTokens.toJsonMap());
            return root;
        }
    }

    private static final class GroupReport {
        final String cohort;
        final String scenario;
        final int runs;
        final boolean unstable;
        final List<Issue> issues;
        final int rootVariantCount;
        final List<SampleCount> rootSamples;
        final NumericStats latencyMs;
        final NumericStats inputTokens;
        final NumericStats outputTokens;
        final NumericStats reasoningTokens;

        private GroupReport(
                String cohort,
                String scenario,
                int runs,
                boolean unstable,
                List<Issue> issues,
                int rootVariantCount,
                List<SampleCount> rootSamples,
                NumericStats latencyMs,
                NumericStats inputTokens,
                NumericStats outputTokens,
                NumericStats reasoningTokens) {
            this.cohort = cohort;
            this.scenario = scenario;
            this.runs = runs;
            this.unstable = unstable;
            this.issues = issues;
            this.rootVariantCount = rootVariantCount;
            this.rootSamples = rootSamples;
            this.latencyMs = latencyMs;
            this.inputTokens = inputTokens;
            this.outputTokens = outputTokens;
            this.reasoningTokens = reasoningTokens;
        }

        Map<String, Object> toJsonMap() {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("cohort", cohort);
            root.put("scenario", scenario);
            root.put("runs", runs);
            root.put("unstable", unstable);
            root.put("root_variant_count", rootVariantCount);

            List<Object> rootSampleMaps = new ArrayList<>();
            for (SampleCount sample : rootSamples) {
                rootSampleMaps.add(sample.toJsonMap());
            }
            root.put("root_samples", rootSampleMaps);

            List<Object> issueMaps = new ArrayList<>();
            for (Issue issue : issues) {
                issueMaps.add(issue.toJsonMap());
            }
            root.put("issues", issueMaps);

            root.put("latency_ms", latencyMs.toJsonMap());
            root.put("input_tokens", inputTokens.toJsonMap());
            root.put("output_tokens", outputTokens.toJsonMap());
            root.put("reasoning_tokens", reasoningTokens.toJsonMap());
            return root;
        }
    }

    private static final class AggregatedIssue {
        final String cohort;
        final String pointer;
        final IssueKind kind;
        int groupsAffected;
        int presentRunsSum;
        int totalRunsSum;
        double maxRelativeSpread;
        final LinkedHashSet<String> scenarioSamples = new LinkedHashSet<>();
        final LinkedHashSet<String> types = new LinkedHashSet<>();
        final Map<String, Integer> sampleCounts = new LinkedHashMap<>();

        private AggregatedIssue(String cohort, String pointer, IssueKind kind) {
            this.cohort = cohort;
            this.pointer = pointer;
            this.kind = kind;
        }

        void observe(Issue issue) {
            groupsAffected++;
            presentRunsSum += issue.presentRuns;
            totalRunsSum += issue.totalRuns;
            if (issue.relativeSpread != null) {
                maxRelativeSpread = Math.max(maxRelativeSpread, issue.relativeSpread);
            }
            types.addAll(issue.types);
            if (scenarioSamples.size() < 6) {
                scenarioSamples.add(issue.scenario);
            }
            for (SampleCount sample : issue.samples) {
                sampleCounts.merge(sample.value, sample.count, Integer::sum);
            }
        }

        List<SampleCount> samples() {
            return toSampleCounts(sampleCounts, 4, 96);
        }

        Map<String, Object> toJsonMap() {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("cohort", cohort);
            root.put("pointer", pointer);
            root.put("kind", kind.wireName);
            root.put("severity", kind.severity);
            root.put("groups_affected", groupsAffected);
            root.put("present_runs_sum", presentRunsSum);
            root.put("total_runs_sum", totalRunsSum);
            root.put("max_relative_spread", maxRelativeSpread);

            List<String> typeList = new ArrayList<>(types);
            Collections.sort(typeList);
            root.put("types", new ArrayList<>(typeList));

            List<Object> scenarioList = new ArrayList<>(scenarioSamples);
            root.put("scenario_samples", scenarioList);

            List<Object> sampleList = new ArrayList<>();
            for (SampleCount sample : samples()) {
                sampleList.add(sample.toJsonMap());
            }
            root.put("samples", sampleList);
            return root;
        }
    }

    private static final class Issue {
        final IssueKind kind;
        final String cohort;
        final String scenario;
        final String pointer;
        final int presentRuns;
        final int totalRuns;
        final List<String> types;
        final int distinctValues;
        final String minValue;
        final String maxValue;
        final Double relativeSpread;
        final List<SampleCount> samples;

        private Issue(
                IssueKind kind,
                String cohort,
                String scenario,
                String pointer,
                int presentRuns,
                int totalRuns,
                List<String> types,
                int distinctValues,
                String minValue,
                String maxValue,
                Double relativeSpread,
                List<SampleCount> samples) {
            this.kind = kind;
            this.cohort = cohort;
            this.scenario = scenario;
            this.pointer = pointer;
            this.presentRuns = presentRuns;
            this.totalRuns = totalRuns;
            this.types = types;
            this.distinctValues = distinctValues;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.relativeSpread = relativeSpread;
            this.samples = samples;
        }

        static Issue presence(GroupKey key, String pointer, int presentRuns, int totalRuns, List<String> types) {
            return new Issue(
                    IssueKind.PRESENCE_DRIFT,
                    key.cohort,
                    key.scenario,
                    pointer,
                    presentRuns,
                    totalRuns,
                    types,
                    0,
                    null,
                    null,
                    null,
                    List.of());
        }

        static Issue type(GroupKey key, String pointer, int presentRuns, int totalRuns, List<String> types) {
            return new Issue(
                    IssueKind.TYPE_DRIFT,
                    key.cohort,
                    key.scenario,
                    pointer,
                    presentRuns,
                    totalRuns,
                    types,
                    0,
                    null,
                    null,
                    null,
                    List.of());
        }

        static Issue value(
                GroupKey key,
                String pointer,
                int presentRuns,
                int totalRuns,
                List<String> types,
                int distinctValues,
                List<SampleCount> samples) {
            return new Issue(
                    IssueKind.VALUE_DRIFT,
                    key.cohort,
                    key.scenario,
                    pointer,
                    presentRuns,
                    totalRuns,
                    types,
                    distinctValues,
                    null,
                    null,
                    null,
                    samples);
        }

        static Issue numeric(
                GroupKey key,
                String pointer,
                int presentRuns,
                int totalRuns,
                int distinctValues,
                String minValue,
                String maxValue,
                double relativeSpread,
                List<SampleCount> samples) {
            return new Issue(
                    IssueKind.NUMERIC_DRIFT,
                    key.cohort,
                    key.scenario,
                    pointer,
                    presentRuns,
                    totalRuns,
                    List.of(JsonType.NUMBER.wireName),
                    distinctValues,
                    minValue,
                    maxValue,
                    relativeSpread,
                    samples);
        }

        static Issue arrayLength(
                GroupKey key,
                String pointer,
                int presentRuns,
                int totalRuns,
                int distinctValues,
                String minValue,
                String maxValue,
                List<SampleCount> samples) {
            return new Issue(
                    IssueKind.ARRAY_LENGTH_DRIFT,
                    key.cohort,
                    key.scenario,
                    pointer,
                    presentRuns,
                    totalRuns,
                    List.of(JsonType.ARRAY.wireName),
                    distinctValues,
                    minValue,
                    maxValue,
                    null,
                    samples);
        }

        Map<String, Object> toJsonMap() {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("kind", kind.wireName);
            root.put("severity", kind.severity);
            root.put("cohort", cohort);
            root.put("scenario", scenario);
            root.put("pointer", pointer);
            root.put("present_runs", presentRuns);
            root.put("total_runs", totalRuns);
            root.put("types", new ArrayList<>(types));
            root.put("distinct_values", distinctValues);
            root.put("min_value", minValue);
            root.put("max_value", maxValue);
            root.put("relative_spread", relativeSpread);

            List<Object> sampleMaps = new ArrayList<>();
            for (SampleCount sample : samples) {
                sampleMaps.add(sample.toJsonMap());
            }
            root.put("samples", sampleMaps);
            return root;
        }
    }

    private static final class SampleCount {
        final String value;
        final int count;

        private SampleCount(String value, int count) {
            this.value = value;
            this.count = count;
        }

        Map<String, Object> toJsonMap() {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("value", value);
            root.put("count", count);
            return root;
        }
    }

    private static final class PathAccumulator {
        final String pointer;
        int presentCount;
        final EnumMap<JsonType, Integer> typeCounts = new EnumMap<>(JsonType.class);
        final Map<String, Integer> scalarCounts = new LinkedHashMap<>();
        final Map<Integer, Integer> arrayLengthCounts = new LinkedHashMap<>();
        BigDecimal minNumber;
        BigDecimal maxNumber;

        private PathAccumulator(String pointer) {
            this.pointer = pointer;
        }

        void observe(NodeValue node) {
            presentCount++;
            typeCounts.merge(node.type, 1, Integer::sum);

            if (node.type == JsonType.ARRAY && node.arrayLength != null) {
                arrayLengthCounts.merge(node.arrayLength, 1, Integer::sum);
                return;
            }

            if (node.type == JsonType.NUMBER && node.numericValue != null) {
                scalarCounts.merge(node.scalarCanonical, 1, Integer::sum);
                if (minNumber == null || node.numericValue.compareTo(minNumber) < 0) {
                    minNumber = node.numericValue;
                }
                if (maxNumber == null || node.numericValue.compareTo(maxNumber) > 0) {
                    maxNumber = node.numericValue;
                }
                return;
            }

            if (node.type.isScalar()) {
                scalarCounts.merge(node.scalarCanonical, 1, Integer::sum);
            }
        }

        List<Issue> finish(GroupKey key, int totalRuns, Config config) {
            List<Issue> issues = new ArrayList<>();
            List<String> types = toTypeLabels(typeCounts);

            if (presentCount < totalRuns) {
                issues.add(Issue.presence(key, pointer, presentCount, totalRuns, types));
            }

            if (typeCounts.size() > 1) {
                issues.add(Issue.type(key, pointer, presentCount, totalRuns, types));
                return issues;
            }

            JsonType onlyType = typeCounts.keySet().isEmpty() ? null : typeCounts.keySet().iterator().next();
            if (onlyType == null) {
                return issues;
            }

            if (onlyType == JsonType.ARRAY) {
                if (arrayLengthCounts.size() > 1) {
                    int min = Collections.min(arrayLengthCounts.keySet());
                    int max = Collections.max(arrayLengthCounts.keySet());
                    issues.add(Issue.arrayLength(
                            key,
                            pointer,
                            presentCount,
                            totalRuns,
                            arrayLengthCounts.size(),
                            Integer.toString(min),
                            Integer.toString(max),
                            toSampleCountsFromIntegers(arrayLengthCounts, 4)));
                }
                return issues;
            }

            if (onlyType == JsonType.NUMBER) {
                if (scalarCounts.size() > 1 && minNumber != null && maxNumber != null) {
                    BigDecimal spread = maxNumber.subtract(minNumber).abs();
                    double relativeSpread = relativeSpread(minNumber, maxNumber);
                    boolean exceedsAbsolute = spread.compareTo(config.numericAbsTolerance) > 0;
                    boolean exceedsRelative = relativeSpread > config.numericRelTolerance;
                    if (exceedsAbsolute && exceedsRelative) {
                        issues.add(Issue.numeric(
                                key,
                                pointer,
                                presentCount,
                                totalRuns,
                                scalarCounts.size(),
                                normalizeNumber(minNumber),
                                normalizeNumber(maxNumber),
                                relativeSpread,
                                toSampleCounts(scalarCounts, 4, 48)));
                    }
                }
                return issues;
            }

            if (onlyType.isScalar() && scalarCounts.size() > 1) {
                issues.add(Issue.value(
                        key,
                        pointer,
                        presentCount,
                        totalRuns,
                        types,
                        scalarCounts.size(),
                        toSampleCounts(scalarCounts, 4, 96)));
            }

            return issues;
        }
    }

    private static final class NodeValue {
        final JsonType type;
        final String scalarCanonical;
        final BigDecimal numericValue;
        final Integer arrayLength;

        private NodeValue(JsonType type, String scalarCanonical, BigDecimal numericValue, Integer arrayLength) {
            this.type = type;
            this.scalarCanonical = scalarCanonical;
            this.numericValue = numericValue;
            this.arrayLength = arrayLength;
        }

        static NodeValue from(Object value) {
            if (value == null) {
                return new NodeValue(JsonType.NULL, "null", null, null);
            }
            if (value instanceof String text) {
                return new NodeValue(JsonType.STRING, Json.toCanonicalJson(text), null, null);
            }
            if (value instanceof Boolean flag) {
                return new NodeValue(JsonType.BOOLEAN, flag ? "true" : "false", null, null);
            }
            if (value instanceof BigDecimal decimal) {
                return new NodeValue(JsonType.NUMBER, normalizeNumber(decimal), decimal, null);
            }
            if (value instanceof Number number) {
                BigDecimal decimal = new BigDecimal(number.toString());
                return new NodeValue(JsonType.NUMBER, normalizeNumber(decimal), decimal, null);
            }
            if (value instanceof Map<?, ?>) {
                return new NodeValue(JsonType.OBJECT, null, null, null);
            }
            if (value instanceof List<?> rawList) {
                return new NodeValue(JsonType.ARRAY, null, null, rawList.size());
            }
            throw new IllegalArgumentException("Unsupported JSON value type: " + value.getClass().getName());
        }
    }

    private static final class NumericStats {
        int count;
        double sum;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        void add(Double value) {
            if (value == null || Double.isNaN(value) || Double.isInfinite(value)) {
                return;
            }
            count++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }

        boolean hasData() {
            return count > 0;
        }

        double mean() {
            return count == 0 ? 0.0d : sum / count;
        }

        Map<String, Object> toJsonMap() {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("count", count);
            root.put("mean", hasData() ? mean() : null);
            root.put("min", hasData() ? min : null);
            root.put("max", hasData() ? max : null);
            return root;
        }
    }

    private static final class Metrics {
        final Double latencyMs;
        final Double inputTokens;
        final Double outputTokens;
        final Double reasoningTokens;

        private Metrics(Double latencyMs, Double inputTokens, Double outputTokens, Double reasoningTokens) {
            this.latencyMs = latencyMs;
            this.inputTokens = inputTokens;
            this.outputTokens = outputTokens;
            this.reasoningTokens = reasoningTokens;
        }

        static Metrics fromRecord(Map<String, Object> record) {
            return new Metrics(
                    readOptionalNumber(record.get("latency_ms")),
                    readOptionalNumber(record.get("input_tokens")),
                    readOptionalNumber(record.get("output_tokens")),
                    readOptionalNumber(record.get("reasoning_tokens")));
        }
    }

    private static Double readOptionalNumber(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.doubleValue();
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String text) {
            try {
                return Double.parseDouble(text.trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static final class RunRecord {
        final int lineNumber;
        final String scenario;
        final String cohort;
        final Object output;
        final Metrics metrics;

        private RunRecord(int lineNumber, String scenario, String cohort, Object output, Metrics metrics) {
            this.lineNumber = lineNumber;
            this.scenario = scenario;
            this.cohort = cohort;
            this.output = output;
            this.metrics = metrics;
        }
    }

    private static final class GroupKey {
        final String cohort;
        final String scenario;

        private GroupKey(String cohort, String scenario) {
            this.cohort = cohort;
            this.scenario = scenario;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof GroupKey that)) {
                return false;
            }
            return Objects.equals(this.cohort, that.cohort) && Objects.equals(this.scenario, that.scenario);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cohort, scenario);
        }
    }

    private enum JsonType {
        OBJECT("object"),
        ARRAY("array"),
        STRING("string"),
        NUMBER("number"),
        BOOLEAN("boolean"),
        NULL("null");

        final String wireName;

        JsonType(String wireName) {
            this.wireName = wireName;
        }

        boolean isScalar() {
            return this == STRING || this == NUMBER || this == BOOLEAN || this == NULL;
        }
    }

    private enum IssueKind {
        TYPE_DRIFT("type-drift", 5),
        PRESENCE_DRIFT("presence-drift", 4),
        NUMERIC_DRIFT("numeric-drift", 3),
        ARRAY_LENGTH_DRIFT("array-length-drift", 3),
        VALUE_DRIFT("value-drift", 2);

        final String wireName;
        final int severity;

        IssueKind(String wireName, int severity) {
            this.wireName = wireName;
            this.severity = severity;
        }
    }

    private static final class Missing {
        static final Object VALUE = new Object();

        private Missing() {
        }
    }

    private static final class UsageException extends RuntimeException {
        private UsageException(String message) {
            super(message);
        }
    }

    private static final class InputException extends RuntimeException {
        private InputException(String message) {
            super(message);
        }
    }

    private static final class Json {
        private Json() {
        }

        static Object parse(String text) {
            JsonParser parser = new JsonParser(text);
            Object value = parser.parseValue();
            parser.skipWhitespace();
            if (!parser.isDone()) {
                throw new IllegalArgumentException("Unexpected trailing content at character " + parser.index + ".");
            }
            return value;
        }

        static String toCanonicalJson(Object value) {
            StringBuilder out = new StringBuilder();
            writeCanonical(value, out);
            return out.toString();
        }

        private static void writeCanonical(Object value, StringBuilder out) {
            if (value == null) {
                out.append("null");
                return;
            }
            if (value instanceof String text) {
                writeQuoted(text, out);
                return;
            }
            if (value instanceof BigDecimal decimal) {
                out.append(normalizeNumber(decimal));
                return;
            }
            if (value instanceof Number number) {
                out.append(normalizeNumber(new BigDecimal(number.toString())));
                return;
            }
            if (value instanceof Boolean flag) {
                out.append(flag ? "true" : "false");
                return;
            }
            if (value instanceof Map<?, ?> rawObject) {
                Map<String, Object> object = asObjectMap(rawObject);
                List<String> keys = new ArrayList<>(object.keySet());
                Collections.sort(keys);
                out.append('{');
                boolean first = true;
                for (String key : keys) {
                    if (!first) {
                        out.append(',');
                    }
                    first = false;
                    writeQuoted(key, out);
                    out.append(':');
                    writeCanonical(object.get(key), out);
                }
                out.append('}');
                return;
            }
            if (value instanceof List<?> rawList) {
                List<Object> list = asList(rawList);
                out.append('[');
                for (int i = 0; i < list.size(); i++) {
                    if (i > 0) {
                        out.append(',');
                    }
                    writeCanonical(list.get(i), out);
                }
                out.append(']');
                return;
            }
            throw new IllegalArgumentException("Unsupported JSON value type: " + value.getClass().getName());
        }

        private static void writeQuoted(String text, StringBuilder out) {
            out.append('"');
            for (int i = 0; i < text.length(); i++) {
                char ch = text.charAt(i);
                switch (ch) {
                    case '"':
                        out.append("\\\"");
                        break;
                    case '\\':
                        out.append("\\\\");
                        break;
                    case '\b':
                        out.append("\\b");
                        break;
                    case '\f':
                        out.append("\\f");
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
                        if (ch < 0x20) {
                            out.append(String.format(Locale.ROOT, "\\u%04x", (int) ch));
                        } else {
                            out.append(ch);
                        }
                        break;
                }
            }
            out.append('"');
        }
    }

    private static final class JsonParser {
        final String text;
        int index;

        private JsonParser(String text) {
            this.text = text;
        }

        boolean isDone() {
            return index >= text.length();
        }

        void skipWhitespace() {
            while (index < text.length()) {
                char ch = text.charAt(index);
                if (ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t') {
                    index++;
                    continue;
                }
                break;
            }
        }

        Object parseValue() {
            skipWhitespace();
            if (index >= text.length()) {
                throw error("Unexpected end of input");
            }

            char ch = text.charAt(index);
            switch (ch) {
                case '{':
                    return parseObject();
                case '[':
                    return parseArray();
                case '"':
                    return parseString();
                case 't':
                    return parseLiteral("true", Boolean.TRUE);
                case 'f':
                    return parseLiteral("false", Boolean.FALSE);
                case 'n':
                    return parseLiteral("null", null);
                default:
                    if (ch == '-' || Character.isDigit(ch)) {
                        return parseNumber();
                    }
                    throw error("Unexpected character '" + ch + "'");
            }
        }

        private Map<String, Object> parseObject() {
            expect('{');
            Map<String, Object> object = new LinkedHashMap<>();
            skipWhitespace();
            if (consume('}')) {
                return object;
            }

            while (true) {
                skipWhitespace();
                if (!peek('"')) {
                    throw error("Expected a string object key");
                }
                String key = parseString();
                skipWhitespace();
                expect(':');
                Object value = parseValue();
                object.put(key, value);
                skipWhitespace();
                if (consume('}')) {
                    break;
                }
                expect(',');
            }
            return object;
        }

        private List<Object> parseArray() {
            expect('[');
            List<Object> list = new ArrayList<>();
            skipWhitespace();
            if (consume(']')) {
                return list;
            }

            while (true) {
                list.add(parseValue());
                skipWhitespace();
                if (consume(']')) {
                    break;
                }
                expect(',');
            }
            return list;
        }

        private String parseString() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (index < text.length()) {
                char ch = text.charAt(index++);
                if (ch == '"') {
                    return out.toString();
                }
                if (ch == '\\') {
                    if (index >= text.length()) {
                        throw error("Unterminated escape sequence");
                    }
                    char escape = text.charAt(index++);
                    switch (escape) {
                        case '"':
                            out.append('"');
                            break;
                        case '\\':
                            out.append('\\');
                            break;
                        case '/':
                            out.append('/');
                            break;
                        case 'b':
                            out.append('\b');
                            break;
                        case 'f':
                            out.append('\f');
                            break;
                        case 'n':
                            out.append('\n');
                            break;
                        case 'r':
                            out.append('\r');
                            break;
                        case 't':
                            out.append('\t');
                            break;
                        case 'u':
                            out.append(parseUnicodeEscape());
                            break;
                        default:
                            throw error("Unsupported escape sequence '\\" + escape + "'");
                    }
                    continue;
                }
                if (ch < 0x20) {
                    throw error("Control character in string");
                }
                out.append(ch);
            }
            throw error("Unterminated string literal");
        }

        private char parseUnicodeEscape() {
            if (index + 4 > text.length()) {
                throw error("Incomplete unicode escape");
            }
            int value = 0;
            for (int i = 0; i < 4; i++) {
                char ch = text.charAt(index++);
                int digit = Character.digit(ch, 16);
                if (digit < 0) {
                    throw error("Invalid hex digit in unicode escape");
                }
                value = (value << 4) + digit;
            }
            return (char) value;
        }

        private Object parseLiteral(String literal, Object value) {
            if (!text.startsWith(literal, index)) {
                throw error("Expected " + literal);
            }
            index += literal.length();
            return value;
        }

        private BigDecimal parseNumber() {
            int start = index;
            if (peek('-')) {
                index++;
            }
            if (peek('0')) {
                index++;
            } else {
                readDigits();
            }

            if (peek('.')) {
                index++;
                readDigits();
            }

            if (peek('e') || peek('E')) {
                index++;
                if (peek('+') || peek('-')) {
                    index++;
                }
                readDigits();
            }

            String token = text.substring(start, index);
            try {
                return new BigDecimal(token);
            } catch (NumberFormatException e) {
                throw error("Invalid number " + token);
            }
        }

        private void readDigits() {
            int start = index;
            while (index < text.length() && Character.isDigit(text.charAt(index))) {
                index++;
            }
            if (start == index) {
                throw error("Expected digit");
            }
        }

        private boolean peek(char expected) {
            return index < text.length() && text.charAt(index) == expected;
        }

        private boolean consume(char expected) {
            if (!peek(expected)) {
                return false;
            }
            index++;
            return true;
        }

        private void expect(char expected) {
            if (!consume(expected)) {
                throw error("Expected '" + expected + "'");
            }
        }

        private IllegalArgumentException error(String message) {
            return new IllegalArgumentException(message + " at character " + index);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asObjectMap(Map<?, ?> rawObject) {
        return (Map<String, Object>) rawObject;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(List<?> rawList) {
        return (List<Object>) rawList;
    }
}

/*
This solves structured output drift in AI systems, agent pipelines, eval harnesses, and MCP tool workflows where the same scenario quietly starts returning different JSON after a model upgrade, prompt change, router tweak, or provider failover. Built because the painful production breakages in 2026 are usually not loud crashes. They are missing keys, type flips, unstable arrays, numeric spread, and response shapes that still look valid until one downstream parser or scorer starts failing.

Use it when you run the same structured task multiple times and want a real gate before shipping a new model, reasoning configuration, prompt template, or agent release. The trick: it reads NDJSON logs, groups repeated runs by scenario and optional cohort, flattens every output into stable JSON pointers, ignores key order, and then measures presence drift, type drift, scalar drift, numeric drift, and array length drift with optional ignore rules for volatile fields like ids, timestamps, and request metadata.

Drop this into any Java 17+ repository, compile it as a single file, and point it at logs from OpenAI, Anthropic, Gemini, Azure OpenAI, Bedrock, LangGraph, MCP servers, tool-calling agents, or your own structured-output test harness. I wrote it to be boring in the best way: no external dependencies, predictable exit codes, machine-readable JSON output, stderr summaries that fit CI logs, and enough detail to explain exactly why a candidate model or prompt is flaky instead of just saying the evaluation failed.
*/