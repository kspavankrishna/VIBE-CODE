import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitor;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StableCodeChunkPlanner {
    private static final long DEFAULT_TARGET_BYTES = 3_200L;
    private static final long DEFAULT_MAX_BYTES = 4_600L;
    private static final long DEFAULT_MAX_FILE_BYTES = 1_200_000L;
    private static final Set<String> DEFAULT_SKIPPED_DIRECTORIES = Set.of(
            ".git", ".hg", ".svn", ".idea", ".vscode", ".gradle", ".next", ".turbo",
            ".cache", ".pnpm-store", ".venv", "venv", "__pycache__", "node_modules",
            "dist", "build", "target", "coverage", "vendor", "tmp", "out");

    private static final Pattern CHUNK_ID_PATTERN =
            Pattern.compile("\"chunkId\"\\s*:\\s*\"([0-9a-f]{12,64})\"");
    private static final Pattern DECLARATION_PATTERN = Pattern.compile(
            "^(?:export\\s+)?(?:(?:public|private|protected|internal|sealed|abstract|final|static|async|inline|suspend)\\s+)*"
                    + "(?:class|interface|enum|record|struct|trait|impl|protocol|object|module|namespace|type|fn|func|function|def|macro_rules!)\\b");
    private static final Pattern IMPORT_PATTERN = Pattern.compile(
            "^(?:package|import|from|using|use|namespace|module|include|require|load|export\\s+\\{)\\b");
    private static final Pattern COMMENT_HEADER_PATTERN = Pattern.compile(
            "^(?:/\\*\\*?|<!--|###?\\s|//\\s*[A-Z0-9]|#\\s*[A-Z0-9]|--\\s*[A-Z0-9]|'''|\"\"\")");
    private static final Pattern ANNOTATION_PATTERN = Pattern.compile("^[@\\[]\\w+");

    private StableCodeChunkPlanner() {
    }

    public static void main(String[] args) {
        int exitCode = 0;
        try {
            exitCode = run(args);
        } catch (UsageException e) {
            System.err.println(e.getMessage());
            exitCode = 2;
        } catch (Exception e) {
            System.err.println("StableCodeChunkPlanner failed: " + e.getMessage());
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

        IgnoreRules ignoreRules = IgnoreRules.load(config.root);
        Set<String> previousChunkIds = loadPreviousChunkIds(config.previousManifest);

        List<Path> files = collectFiles(config, ignoreRules);
        List<Chunk> chunks = new ArrayList<>();
        int skippedBinary = 0;
        int skippedLarge = 0;
        int skippedEmpty = 0;

        for (Path file : files) {
            String relative = normalizeRelative(config.root.relativize(file));
            ReadOutcome outcome = readTextFile(file, config.maxFileBytes);
            if (outcome.status == ReadStatus.BINARY_OR_INVALID_UTF8) {
                skippedBinary++;
                continue;
            }
            if (outcome.status == ReadStatus.TOO_LARGE) {
                skippedLarge++;
                continue;
            }
            if (outcome.text == null || outcome.text.isBlank()) {
                skippedEmpty++;
                continue;
            }
            chunks.addAll(planChunks(relative, outcome.text, config, previousChunkIds));
        }

        writeManifest(config, chunks);
        if (!config.noSummary) {
            emitSummary(files.size(), chunks, skippedBinary, skippedLarge, skippedEmpty);
        }
        return 0;
    }

    private static void printHelp() {
        System.out.println("StableCodeChunkPlanner");
        System.out.println("Deterministic chunk planning for AI code indexing, embedding, and retrieval pipelines.");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java StableCodeChunkPlanner [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --root <path>            Repository root to scan. Default: current directory");
        System.out.println("  --output <path|->        Write NDJSON manifest to file or stdout. Default: stdout");
        System.out.println("  --previous <path>        Existing NDJSON manifest. Used to mark unchanged chunks");
        System.out.println("  --target-bytes <n>       Preferred chunk size before a semantic split");
        System.out.println("  --max-bytes <n>          Hard chunk cap. Long lines are soft-split when needed");
        System.out.println("  --max-file-bytes <n>     Skip files larger than this many bytes");
        System.out.println("  --include-hidden         Include hidden files and directories");
        System.out.println("  --extensions <list>      Comma-separated extension allowlist like java,kt,py,ts");
        System.out.println("  --no-summary             Suppress stderr summary");
        System.out.println("  --help                   Show this help");
    }

    private static List<Path> collectFiles(Config config, IgnoreRules ignoreRules) throws IOException {
        List<Path> files = new ArrayList<>();
        FileVisitor<Path> visitor = new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (dir.equals(config.root)) {
                    return FileVisitResult.CONTINUE;
                }

                Path relative = config.root.relativize(dir);
                String relativeText = normalizeRelative(relative);
                String name = dir.getFileName() == null ? relativeText : dir.getFileName().toString();

                if (!config.includeHidden && name.startsWith(".")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                if (DEFAULT_SKIPPED_DIRECTORIES.contains(name) && !ignoreRules.isExplicitlyIncluded(relativeText, true)) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                if (ignoreRules.isIgnored(relativeText, true)) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!attrs.isRegularFile()) {
                    return FileVisitResult.CONTINUE;
                }
                Path relative = config.root.relativize(file);
                String relativeText = normalizeRelative(relative);
                String name = file.getFileName() == null ? relativeText : file.getFileName().toString();
                if (!config.includeHidden && name.startsWith(".")) {
                    return FileVisitResult.CONTINUE;
                }
                if (ignoreRules.isIgnored(relativeText, false)) {
                    return FileVisitResult.CONTINUE;
                }
                if (!config.extensions.isEmpty()) {
                    String extension = extensionOf(name);
                    if (!config.extensions.contains(extension.toLowerCase(Locale.ROOT))) {
                        return FileVisitResult.CONTINUE;
                    }
                }
                files.add(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
            }
        };

        Files.walkFileTree(config.root, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, visitor);
        files.sort(Comparator.comparing(path -> normalizeRelative(config.root.relativize(path))));
        return files;
    }

    private static List<Chunk> planChunks(
            String relativePath,
            String content,
            Config config,
            Set<String> previousChunkIds) {

        List<Segment> segments = segmentize(normalizeNewlines(content), config.maxBytes);
        List<Chunk> chunks = new ArrayList<>();
        if (segments.isEmpty()) {
            return chunks;
        }

        int cursor = 0;
        int ordinal = 0;
        while (cursor < segments.size()) {
            int bestEnd = -1;
            int bestScore = Integer.MIN_VALUE;
            long bytes = 0L;
            int probe = cursor;

            while (probe < segments.size()) {
                bytes += segments.get(probe).byteSize;
                int candidateEnd = probe + 1;
                if (bytes >= config.targetBytes) {
                    int score = boundaryScore(segments, cursor, candidateEnd, bytes, config.maxBytes);
                    if (score >= bestScore) {
                        bestScore = score;
                        bestEnd = candidateEnd;
                    }
                }
                if (bytes >= config.maxBytes) {
                    break;
                }
                probe++;
            }

            if (bestEnd < 0) {
                bestEnd = Math.min(segments.size(), Math.max(cursor + 1, probe + 1));
            }

            String chunkText = joinSegments(segments, cursor, bestEnd);
            int lineStart = segments.get(cursor).lineNumber;
            int lineEnd = segments.get(bestEnd - 1).lineNumber;
            ordinal++;
            String chunkId = shortHash(relativePath + "\u0000" + lineStart + "\u0000" + lineEnd + "\u0000" + chunkText);
            boolean changed = !previousChunkIds.contains(chunkId);
            chunks.add(new Chunk(
                    chunkId,
                    relativePath,
                    detectLanguage(relativePath),
                    ordinal,
                    lineStart,
                    lineEnd,
                    utf8Size(chunkText),
                    changed,
                    chunkText));
            cursor = bestEnd;
        }

        return chunks;
    }

    private static List<Segment> segmentize(String normalizedContent, long maxChunkBytes) {
        List<Segment> segments = new ArrayList<>();
        String[] lines = normalizedContent.split("\n", -1);
        long softCap = Math.max(256L, maxChunkBytes - 512L);

        for (int i = 0; i < lines.length; i++) {
            String lineWithTerminator = i == lines.length - 1 ? lines[i] : lines[i] + "\n";
            if (utf8Size(lineWithTerminator) <= softCap) {
                segments.add(new Segment(lineWithTerminator, i + 1, 0, 1));
                continue;
            }

            int start = 0;
            int fragmentIndex = 0;
            List<String> parts = new ArrayList<>();
            while (start < lineWithTerminator.length()) {
                int bestEnd = chooseSoftSplit(lineWithTerminator, start, softCap);
                parts.add(lineWithTerminator.substring(start, bestEnd));
                start = bestEnd;
            }
            for (int j = 0; j < parts.size(); j++) {
                segments.add(new Segment(parts.get(j), i + 1, j, parts.size()));
            }
        }
        return segments;
    }

    private static int chooseSoftSplit(String line, int start, long softCapBytes) {
        int limit = start;
        while (limit < line.length()) {
            int next = limit + 1;
            if (utf8Size(line.substring(start, next)) > softCapBytes) {
                break;
            }
            limit = next;
        }
        if (limit <= start + 1) {
            return Math.min(line.length(), start + 1);
        }

        int best = limit;
        for (int i = limit; i > start + 1; i--) {
            char c = line.charAt(i - 1);
            if (Character.isWhitespace(c) || c == ',' || c == ';' || c == '}' || c == ')' || c == ']') {
                best = i;
                break;
            }
        }
        return best;
    }

    private static int boundaryScore(List<Segment> segments, int start, int endExclusive, long byteSize, long maxBytes) {
        Segment last = segments.get(endExclusive - 1);
        Segment next = endExclusive < segments.size() ? segments.get(endExclusive) : null;

        int score = 0;
        if (last.isLastFragment()) {
            score += 14;
        } else {
            score -= 40;
        }

        String tail = last.text.stripTrailing();
        if (tail.isEmpty()) {
            score += 20;
        }
        if (tail.endsWith("}") || tail.endsWith("};") || tail.endsWith("end")) {
            score += 18;
        }
        if (tail.endsWith(",")) {
            score -= 4;
        }

        if (next == null) {
            score += 40;
        } else {
            String nextTrimmed = next.text.stripLeading();
            if (nextTrimmed.isEmpty()) {
                score += 10;
            }
            if (DECLARATION_PATTERN.matcher(nextTrimmed).find()) {
                score += 34;
            }
            if (IMPORT_PATTERN.matcher(nextTrimmed).find()) {
                score += 24;
            }
            if (COMMENT_HEADER_PATTERN.matcher(nextTrimmed).find()) {
                score += 20;
            }
            if (ANNOTATION_PATTERN.matcher(nextTrimmed).find()) {
                score += 10;
            }
            if (leadingWhitespace(next.text) == 0) {
                score += 10;
            }
        }

        int spanLines = last.lineNumber - segments.get(start).lineNumber + 1;
        if (spanLines < 4) {
            score -= 18;
        }
        if (byteSize > maxBytes) {
            score -= 50;
        }
        if (byteSize > maxBytes - 256) {
            score -= 12;
        }
        return score;
    }

    private static void writeManifest(Config config, List<Chunk> chunks) throws IOException {
        BufferedWriter writer;
        boolean closeWriter;
        if (config.output == null) {
            writer = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
            closeWriter = false;
        } else {
            Path parent = config.output.getParent();
            if (parent != null && Files.notExists(parent)) {
                Files.createDirectories(parent);
            }
            writer = Files.newBufferedWriter(config.output, StandardCharsets.UTF_8);
            closeWriter = true;
        }

        try {
            writer.write("{\"recordType\":\"manifest\"");
            writer.write(",\"tool\":\"StableCodeChunkPlanner\"");
            writer.write(",\"version\":1");
            writer.write(",\"generatedAt\":\"");
            writer.write(jsonEscape(Instant.now().toString()));
            writer.write("\"");
            writer.write(",\"root\":\"");
            writer.write(jsonEscape(config.root.toAbsolutePath().normalize().toString()));
            writer.write("\"");
            writer.write(",\"targetBytes\":");
            writer.write(Long.toString(config.targetBytes));
            writer.write(",\"maxBytes\":");
            writer.write(Long.toString(config.maxBytes));
            writer.write("}");
            writer.newLine();

            for (Chunk chunk : chunks) {
                writer.write("{\"recordType\":\"chunk\"");
                writer.write(",\"chunkId\":\"");
                writer.write(chunk.chunkId);
                writer.write("\"");
                writer.write(",\"path\":\"");
                writer.write(jsonEscape(chunk.path));
                writer.write("\"");
                writer.write(",\"language\":\"");
                writer.write(jsonEscape(chunk.language));
                writer.write("\"");
                writer.write(",\"ordinal\":");
                writer.write(Integer.toString(chunk.ordinal));
                writer.write(",\"lineStart\":");
                writer.write(Integer.toString(chunk.lineStart));
                writer.write(",\"lineEnd\":");
                writer.write(Integer.toString(chunk.lineEnd));
                writer.write(",\"byteSize\":");
                writer.write(Long.toString(chunk.byteSize));
                writer.write(",\"changed\":");
                writer.write(chunk.changed ? "true" : "false");
                writer.write(",\"content\":\"");
                writer.write(jsonEscape(chunk.content));
                writer.write("\"}");
                writer.newLine();
            }
            writer.flush();
        } finally {
            if (closeWriter) {
                writer.close();
            }
        }
    }

    private static void emitSummary(
            int totalFiles,
            List<Chunk> chunks,
            int skippedBinary,
            int skippedLarge,
            int skippedEmpty) {
        long changed = chunks.stream().filter(chunk -> chunk.changed).count();
        long totalBytes = 0L;
        Set<String> uniqueFiles = new HashSet<>();
        for (Chunk chunk : chunks) {
            totalBytes += chunk.byteSize;
            uniqueFiles.add(chunk.path);
        }
        System.err.println("StableCodeChunkPlanner summary");
        System.err.println("  scanned files:   " + totalFiles);
        System.err.println("  emitted files:   " + uniqueFiles.size());
        System.err.println("  emitted chunks:  " + chunks.size());
        System.err.println("  changed chunks:  " + changed);
        System.err.println("  emitted bytes:   " + totalBytes);
        System.err.println("  skipped binary:  " + skippedBinary);
        System.err.println("  skipped large:   " + skippedLarge);
        System.err.println("  skipped empty:   " + skippedEmpty);
    }

    private static Set<String> loadPreviousChunkIds(Path previousManifest) throws IOException {
        if (previousManifest == null || !Files.exists(previousManifest)) {
            return Set.of();
        }
        Set<String> chunkIds = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(previousManifest, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher matcher = CHUNK_ID_PATTERN.matcher(line);
                if (matcher.find()) {
                    chunkIds.add(matcher.group(1));
                }
            }
        }
        return chunkIds;
    }

    private static ReadOutcome readTextFile(Path file, long maxFileBytes) throws IOException {
        long size = Files.size(file);
        if (size > maxFileBytes) {
            return ReadOutcome.tooLarge();
        }
        byte[] bytes = Files.readAllBytes(file);
        for (byte b : bytes) {
            if (b == 0) {
                return ReadOutcome.binary();
            }
        }
        try {
            String text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
            return ReadOutcome.text(text);
        } catch (CharacterCodingException e) {
            return ReadOutcome.binary();
        }
    }

    private static String detectLanguage(String path) {
        String extension = extensionOf(path).toLowerCase(Locale.ROOT);
        return switch (extension) {
            case "java" -> "java";
            case "kt", "kts" -> "kotlin";
            case "scala" -> "scala";
            case "groovy" -> "groovy";
            case "py" -> "python";
            case "js", "cjs", "mjs" -> "javascript";
            case "ts", "tsx", "mts", "cts" -> "typescript";
            case "rs" -> "rust";
            case "go" -> "go";
            case "rb" -> "ruby";
            case "php" -> "php";
            case "swift" -> "swift";
            case "c", "h" -> "c";
            case "cc", "cpp", "cxx", "hpp", "hh" -> "cpp";
            case "cs" -> "csharp";
            case "dart" -> "dart";
            case "ex", "exs" -> "elixir";
            case "clj", "cljs", "cljc" -> "clojure";
            case "sh", "bash", "zsh" -> "shell";
            case "sql" -> "sql";
            case "json", "jsonl" -> "json";
            case "yaml", "yml" -> "yaml";
            case "toml" -> "toml";
            case "md" -> "markdown";
            default -> "text";
        };
    }

    private static String joinSegments(List<Segment> segments, int start, int endExclusive) {
        StringBuilder builder = new StringBuilder();
        for (int i = start; i < endExclusive; i++) {
            builder.append(segments.get(i).text);
        }
        return builder.toString();
    }

    private static String normalizeNewlines(String text) {
        return text.replace("\r\n", "\n").replace('\r', '\n');
    }

    private static String normalizeRelative(Path relative) {
        return relative.toString().replace('\\', '/');
    }

    private static String extensionOf(String fileName) {
        int index = fileName.lastIndexOf('.');
        if (index < 0 || index == fileName.length() - 1) {
            return "";
        }
        return fileName.substring(index + 1);
    }

    private static long utf8Size(String text) {
        return text.getBytes(StandardCharsets.UTF_8).length;
    }

    private static int leadingWhitespace(String text) {
        int count = 0;
        while (count < text.length() && Character.isWhitespace(text.charAt(count))) {
            count++;
        }
        return count;
    }

    private static String shortHash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                builder.append(Character.forDigit((b >> 4) & 0xF, 16));
                builder.append(Character.forDigit(b & 0xF, 16));
            }
            return builder.substring(0, 24);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Missing SHA-256 support", e);
        }
    }

    private static String jsonEscape(String text) {
        StringBuilder builder = new StringBuilder(text.length() + 16);
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '\\' -> builder.append("\\\\");
                case '"' -> builder.append("\\\"");
                case '\b' -> builder.append("\\b");
                case '\f' -> builder.append("\\f");
                case '\n' -> builder.append("\\n");
                case '\r' -> builder.append("\\r");
                case '\t' -> builder.append("\\t");
                default -> {
                    if (c < 0x20) {
                        builder.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        builder.append(c);
                    }
                }
            }
        }
        return builder.toString();
    }

    private static final class Config {
        private final Path root;
        private final Path output;
        private final Path previousManifest;
        private final long targetBytes;
        private final long maxBytes;
        private final long maxFileBytes;
        private final boolean includeHidden;
        private final boolean noSummary;
        private final boolean help;
        private final Set<String> extensions;

        private Config(
                Path root,
                Path output,
                Path previousManifest,
                long targetBytes,
                long maxBytes,
                long maxFileBytes,
                boolean includeHidden,
                boolean noSummary,
                boolean help,
                Set<String> extensions) {
            this.root = root;
            this.output = output;
            this.previousManifest = previousManifest;
            this.targetBytes = targetBytes;
            this.maxBytes = maxBytes;
            this.maxFileBytes = maxFileBytes;
            this.includeHidden = includeHidden;
            this.noSummary = noSummary;
            this.help = help;
            this.extensions = extensions;
        }

        private static Config parse(String[] args) {
            Path root = Paths.get(".").toAbsolutePath().normalize();
            Path output = null;
            Path previous = null;
            long targetBytes = DEFAULT_TARGET_BYTES;
            long maxBytes = DEFAULT_MAX_BYTES;
            long maxFileBytes = DEFAULT_MAX_FILE_BYTES;
            boolean includeHidden = false;
            boolean noSummary = false;
            boolean help = false;
            Set<String> extensions = new LinkedHashSet<>();

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--root" -> root = requirePath(args, ++i, "--root");
                    case "--output" -> output = requireOutputPath(args, ++i, "--output");
                    case "--previous" -> previous = requirePath(args, ++i, "--previous");
                    case "--target-bytes" -> targetBytes = requireLong(args, ++i, "--target-bytes");
                    case "--max-bytes" -> maxBytes = requireLong(args, ++i, "--max-bytes");
                    case "--max-file-bytes" -> maxFileBytes = requireLong(args, ++i, "--max-file-bytes");
                    case "--extensions" -> extensions = parseExtensions(requireValue(args, ++i, "--extensions"));
                    case "--include-hidden" -> includeHidden = true;
                    case "--no-summary" -> noSummary = true;
                    case "--help", "-h" -> help = true;
                    default -> throw new UsageException("Unknown argument: " + arg);
                }
            }

            if (targetBytes <= 0 || maxBytes <= 0 || maxFileBytes <= 0) {
                throw new UsageException("Byte-oriented flags must be positive integers");
            }
            if (targetBytes > maxBytes) {
                throw new UsageException("--target-bytes must be less than or equal to --max-bytes");
            }

            return new Config(
                    root.toAbsolutePath().normalize(),
                    output == null ? null : output.toAbsolutePath().normalize(),
                    previous == null ? null : previous.toAbsolutePath().normalize(),
                    targetBytes,
                    maxBytes,
                    maxFileBytes,
                    includeHidden,
                    noSummary,
                    help,
                    extensions);
        }

        private static Path requirePath(String[] args, int index, String flag) {
            return Paths.get(requireValue(args, index, flag));
        }

        private static Path requireOutputPath(String[] args, int index, String flag) {
            String value = requireValue(args, index, flag);
            if ("-".equals(value)) {
                return null;
            }
            return Paths.get(value);
        }

        private static String requireValue(String[] args, int index, String flag) {
            if (index >= args.length) {
                throw new UsageException("Missing value for " + flag);
            }
            return args[index];
        }

        private static long requireLong(String[] args, int index, String flag) {
            String value = requireValue(args, index, flag);
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new UsageException("Expected an integer for " + flag + ", got: " + value);
            }
        }

        private static Set<String> parseExtensions(String value) {
            Set<String> extensions = new LinkedHashSet<>();
            String[] parts = value.split(",");
            for (String part : parts) {
                String cleaned = part.trim().toLowerCase(Locale.ROOT);
                if (cleaned.startsWith(".")) {
                    cleaned = cleaned.substring(1);
                }
                if (!cleaned.isEmpty()) {
                    extensions.add(cleaned);
                }
            }
            return extensions;
        }
    }

    private static final class IgnoreRules {
        private final List<Rule> rules;

        private IgnoreRules(List<Rule> rules) {
            this.rules = rules;
        }

        private static IgnoreRules load(Path root) throws IOException {
            List<Rule> rules = new ArrayList<>();
            loadRulesFile(root.resolve(".gitignore"), rules);
            loadRulesFile(root.resolve(".chunkignore"), rules);
            return new IgnoreRules(rules);
        }

        private static void loadRulesFile(Path file, List<Rule> rules) throws IOException {
            if (!Files.exists(file) || !Files.isRegularFile(file)) {
                return;
            }
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            for (String rawLine : lines) {
                String line = rawLine.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                boolean negated = line.startsWith("!");
                if (negated) {
                    line = line.substring(1).trim();
                    if (line.isEmpty()) {
                        continue;
                    }
                }

                boolean directoryOnly = line.endsWith("/");
                boolean basenameOnly = !line.contains("/");
                boolean anchored = line.startsWith("/");
                String pattern = line;
                if (anchored) {
                    pattern = pattern.substring(1);
                }
                if (directoryOnly) {
                    pattern = pattern + "**";
                }
                String globPattern;
                if (basenameOnly) {
                    globPattern = pattern;
                } else if (anchored) {
                    globPattern = pattern;
                } else {
                    globPattern = "**/" + pattern;
                }

                PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + globPattern);
                rules.add(new Rule(matcher, basenameOnly, directoryOnly, negated));
            }
        }

        private boolean isIgnored(String relativePath, boolean directory) {
            boolean ignored = false;
            Path relative = Paths.get(relativePath);
            Path fileName = relative.getFileName() == null ? relative : relative.getFileName();
            for (Rule rule : rules) {
                if (rule.directoryOnly && !directory) {
                    continue;
                }
                Path candidate = rule.basenameOnly ? fileName : relative;
                if (rule.matcher.matches(candidate)) {
                    ignored = !rule.negated;
                }
            }
            return ignored;
        }

        private boolean isExplicitlyIncluded(String relativePath, boolean directory) {
            Path relative = Paths.get(relativePath);
            Path fileName = relative.getFileName() == null ? relative : relative.getFileName();
            boolean included = false;
            for (Rule rule : rules) {
                if (!rule.negated) {
                    continue;
                }
                if (rule.directoryOnly && !directory) {
                    continue;
                }
                Path candidate = rule.basenameOnly ? fileName : relative;
                if (rule.matcher.matches(candidate)) {
                    included = true;
                }
            }
            return included;
        }
    }

    private static final class Rule {
        private final PathMatcher matcher;
        private final boolean basenameOnly;
        private final boolean directoryOnly;
        private final boolean negated;

        private Rule(PathMatcher matcher, boolean basenameOnly, boolean directoryOnly, boolean negated) {
            this.matcher = matcher;
            this.basenameOnly = basenameOnly;
            this.directoryOnly = directoryOnly;
            this.negated = negated;
        }
    }

    private static final class Segment {
        private final String text;
        private final int lineNumber;
        private final int fragmentIndex;
        private final int fragmentCount;
        private final long byteSize;

        private Segment(String text, int lineNumber, int fragmentIndex, int fragmentCount) {
            this.text = text;
            this.lineNumber = lineNumber;
            this.fragmentIndex = fragmentIndex;
            this.fragmentCount = fragmentCount;
            this.byteSize = utf8Size(text);
        }

        private boolean isLastFragment() {
            return fragmentIndex == fragmentCount - 1;
        }
    }

    private static final class Chunk {
        private final String chunkId;
        private final String path;
        private final String language;
        private final int ordinal;
        private final int lineStart;
        private final int lineEnd;
        private final long byteSize;
        private final boolean changed;
        private final String content;

        private Chunk(
                String chunkId,
                String path,
                String language,
                int ordinal,
                int lineStart,
                int lineEnd,
                long byteSize,
                boolean changed,
                String content) {
            this.chunkId = chunkId;
            this.path = path;
            this.language = language;
            this.ordinal = ordinal;
            this.lineStart = lineStart;
            this.lineEnd = lineEnd;
            this.byteSize = byteSize;
            this.changed = changed;
            this.content = content;
        }
    }

    private enum ReadStatus {
        TEXT,
        TOO_LARGE,
        BINARY_OR_INVALID_UTF8
    }

    private static final class ReadOutcome {
        private final ReadStatus status;
        private final String text;

        private ReadOutcome(ReadStatus status, String text) {
            this.status = status;
            this.text = text;
        }

        private static ReadOutcome text(String text) {
            return new ReadOutcome(ReadStatus.TEXT, text);
        }

        private static ReadOutcome tooLarge() {
            return new ReadOutcome(ReadStatus.TOO_LARGE, null);
        }

        private static ReadOutcome binary() {
            return new ReadOutcome(ReadStatus.BINARY_OR_INVALID_UTF8, null);
        }
    }

    private static final class UsageException extends RuntimeException {
        private UsageException(String message) {
            super(message);
        }
    }
}

/*
This solves a real 2026 problem for teams feeding codebases into embeddings, retrieval, code search, eval harnesses, and agent memory systems. The common failure mode is unstable chunking: every small edit reshuffles chunk boundaries, invalidates cache keys, and forces expensive re-indexing. Built because most “chunkers” are either toy scripts or framework-specific wrappers that ignore .gitignore rules, binary files, long-line edge cases, and deterministic IDs. Use it when you need a clean NDJSON manifest of semantically sized code chunks that stays steady across runs and only marks the chunks that truly changed. The trick: it respects repo ignore files, skips garbage, soft-splits pathological long lines, then prefers declaration and block boundaries instead of cutting blindly on byte counts. Drop this into any Java-friendly repo or internal tooling stack when you want reproducible code chunk manifests for RAG pipelines, code intelligence systems, embedding refresh jobs, or large-scale repository ingestion without dragging in external dependencies.
*/