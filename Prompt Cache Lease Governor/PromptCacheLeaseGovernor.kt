import java.io.File
import java.security.MessageDigest
import java.util.Locale
import kotlin.math.exp
import kotlin.math.max
import kotlin.math.min
import kotlin.math.round

private const val APP = "PromptCacheLeaseGovernor"
private const val APP_VERSION = "2026.04"

fun main(args: Array<String>) {
    val config = try {
        Config.parse(args.toList())
    } catch (error: IllegalArgumentException) {
        System.err.println("$APP: ${error.message}")
        System.err.println(Config.help())
        kotlin.system.exitProcess(64)
    }

    if (config.help) {
        print(Config.help())
        return
    }

    val text = try {
        if (config.input == null || config.input == "-") {
            System.`in`.bufferedReader().readText()
        } else {
            File(config.input).readText()
        }
    } catch (error: Exception) {
        System.err.println("$APP: cannot read input: ${error.message}")
        kotlin.system.exitProcess(66)
    }

    val blocks = try {
        Csv.read(text).rows.mapIndexed { index, row ->
            PromptBlock.from(row, index + 2, config.defaultPricePerMillion)
        }
    } catch (error: IllegalArgumentException) {
        System.err.println("$APP: ${error.message}")
        kotlin.system.exitProcess(65)
    }

    val report = LeaseGovernor(config.policy()).score(blocks)
    when (config.format) {
        Format.TABLE -> print(Render.table(report))
        Format.JSONL -> print(Render.jsonl(report))
        Format.MARKDOWN -> print(Render.markdown(report))
    }

    if (config.failOn.fails(report.maxSeverity)) kotlin.system.exitProcess(2)
}

private data class Config(
    val input: String?,
    val format: Format,
    val minHitRate: Double,
    val minRequestsPerDay: Double,
    val minSavingsPerDay: Double,
    val maxBudgetShare: Double,
    val minTtlMinutes: Long,
    val maxTtlMinutes: Long,
    val defaultPricePerMillion: Double,
    val strictResidency: Boolean,
    val failOn: FailOn,
    val help: Boolean
) {
    fun policy(): Policy = Policy(
        minHitRate = minHitRate,
        minRequestsPerDay = minRequestsPerDay,
        minSavingsPerDay = minSavingsPerDay,
        maxBudgetShare = maxBudgetShare,
        minTtlMinutes = minTtlMinutes,
        maxTtlMinutes = maxTtlMinutes,
        strictResidency = strictResidency
    )

    companion object {
        fun parse(args: List<String>): Config {
            var input: String? = null
            var format = Format.TABLE
            var minHitRate = 0.35
            var minRequests = 50.0
            var minSavings = 0.25
            var maxBudgetShare = 0.20
            var minTtl = 5L
            var maxTtl = 1440L
            var defaultPrice = 5.0
            var strictResidency = true
            var failOn = FailOn.CRITICAL
            var help = false

            var index = 0
            while (index < args.size) {
                val token = args[index]
                when (token) {
                    "-h", "--help" -> help = true
                    "-i", "--input" -> input = args.next(++index, token)
                    "-f", "--format" -> format = Format.parse(args.next(++index, token))
                    "--min-hit-rate" -> minHitRate = parseRate(args.next(++index, token), token)
                    "--min-requests" -> minRequests = args.next(++index, token).positiveDouble(token)
                    "--min-savings" -> minSavings = args.next(++index, token).positiveDouble(token)
                    "--max-budget-share" -> maxBudgetShare = parseRate(args.next(++index, token), token)
                    "--min-ttl-minutes" -> minTtl = args.next(++index, token).positiveLong(token)
                    "--max-ttl-minutes" -> maxTtl = args.next(++index, token).positiveLong(token)
                    "--default-price-per-mtok" -> defaultPrice = args.next(++index, token).positiveDouble(token)
                    "--allow-cross-region" -> strictResidency = false
                    "--strict-residency" -> strictResidency = true
                    "--fail-on" -> failOn = FailOn.parse(args.next(++index, token))
                    else -> throw IllegalArgumentException("unknown argument '$token'")
                }
                index += 1
            }
            require(minTtl <= maxTtl) { "--min-ttl-minutes must not exceed --max-ttl-minutes" }
            return Config(input, format, minHitRate, minRequests, minSavings, maxBudgetShare, minTtl, maxTtl, defaultPrice, strictResidency, failOn, help)
        }

        fun help(): String = """
$APP $APP_VERSION

Usage:
  kotlin PromptCacheLeaseGovernor.kt --input prompt-cache.csv --format markdown
  java -jar PromptCacheLeaseGovernor.jar --input - --format jsonl --fail-on high

Required CSV columns:
  tenant, provider, region, block_id, tokens, requests_per_day, hit_rate

Optional CSV columns:
  allowed_regions, sensitivity, contains_pii, contains_secret, retention_safe,
  age_minutes, max_staleness_minutes, change_rate_per_day, build_millis,
  price_per_mtok, budget_dollars_per_day, priority, currently_cached,
  observed_failures, state_hash, source_path, tool_name

Options:
  -i, --input PATH                 CSV file, or '-' / omitted for stdin
  -f, --format table|jsonl|markdown
  --min-hit-rate RATE              0.42 or 42 style rate, default 0.35
  --min-requests COUNT             default 50 requests per day
  --min-savings DOLLARS            default 0.25 daily net dollars
  --max-budget-share RATE          default 0.20 of tenant cache budget
  --min-ttl-minutes MINUTES        default 5
  --max-ttl-minutes MINUTES        default 1440
  --default-price-per-mtok PRICE   default 5.0 when row omits price
  --allow-cross-region             warn through region mismatches instead of quarantine
  --fail-on none|warn|high|critical
""".trimIndent() + "\n"

        private fun List<String>.next(index: Int, flag: String): String {
            if (index >= size) throw IllegalArgumentException("$flag requires a value")
            return this[index]
        }

        private fun parseRate(value: String, flag: String): Double {
            val raw = value.toDoubleOrNull() ?: throw IllegalArgumentException("$flag must be numeric")
            val normalized = if (raw > 1.0 && raw <= 100.0) raw / 100.0 else raw
            require(normalized in 0.0..1.0) { "$flag must be between 0 and 1, or 0 and 100" }
            return normalized
        }
    }
}

private enum class Format {
    TABLE,
    JSONL,
    MARKDOWN;

    companion object {
        fun parse(value: String): Format = when (value.lower()) {
            "table", "text" -> TABLE
            "jsonl", "json" -> JSONL
            "markdown", "md" -> MARKDOWN
            else -> throw IllegalArgumentException("--format must be table, jsonl, or markdown")
        }
    }
}

private enum class FailOn(private val floor: Severity?) {
    NONE(null),
    WARN(Severity.WARN),
    HIGH(Severity.HIGH),
    CRITICAL(Severity.CRITICAL);

    fun fails(severity: Severity): Boolean = floor != null && severity.rank >= floor.rank

    companion object {
        fun parse(value: String): FailOn = when (value.lower()) {
            "none", "off" -> NONE
            "warn", "warning" -> WARN
            "high" -> HIGH
            "critical", "crit" -> CRITICAL
            else -> throw IllegalArgumentException("--fail-on must be none, warn, high, or critical")
        }
    }
}

private enum class Sensitivity(val risk: Double) {
    PUBLIC(0.0),
    INTERNAL(0.15),
    PRIVATE(0.55),
    SECRET(1.0);

    companion object {
        fun parse(value: String?): Sensitivity = when (value?.lower()) {
            null, "" -> INTERNAL
            "public", "open" -> PUBLIC
            "internal", "company" -> INTERNAL
            "private", "pii", "customer", "regulated" -> PRIVATE
            "secret", "credential", "credentials", "key" -> SECRET
            else -> throw IllegalArgumentException("unknown sensitivity '$value'")
        }
    }
}

private enum class Decision { PIN, REFRESH, BYPASS, EVICT, QUARANTINE }

private enum class Severity(val rank: Int) { INFO(0), WARN(1), HIGH(2), CRITICAL(3) }

private data class Policy(
    val minHitRate: Double,
    val minRequestsPerDay: Double,
    val minSavingsPerDay: Double,
    val maxBudgetShare: Double,
    val minTtlMinutes: Long,
    val maxTtlMinutes: Long,
    val strictResidency: Boolean
)

private data class PromptBlock(
    val tenant: String,
    val provider: String,
    val region: String,
    val blockId: String,
    val tokens: Int,
    val requestsPerDay: Double,
    val hitRate: Double,
    val sensitivity: Sensitivity,
    val allowedRegions: Set<String>,
    val containsPii: Boolean,
    val containsSecret: Boolean,
    val retentionSafe: Boolean,
    val ageMinutes: Long,
    val maxStalenessMinutes: Long,
    val changeRatePerDay: Double,
    val buildMillis: Long,
    val pricePerMillion: Double,
    val budgetDollarsPerDay: Double,
    val priority: Double,
    val currentlyCached: Boolean,
    val observedFailures: Int,
    val stateHash: String,
    val sourcePath: String,
    val toolName: String,
    val rowNumber: Int
) {
    companion object {
        fun from(row: Row, line: Int, defaultPrice: Double): PromptBlock {
            val tenant = row.required("tenant")
            val provider = row.required("provider")
            val region = row.required("region", "cache_region")
            val blockId = row.required("block_id", "blockid", "prompt_block", "cache_key")
            val sensitivity = Sensitivity.parse(row.optional("sensitivity", "data_class"))
            val sourcePath = row.optional("source_path", "path") ?: ""
            val stateHash = row.optional("state_hash", "digest", "content_hash")
                ?: stableHash(listOf(tenant, provider, region, blockId, sourcePath))
            return PromptBlock(
                tenant = tenant,
                provider = provider,
                region = region,
                blockId = blockId,
                tokens = row.int("tokens", "token_count", min = 1),
                requestsPerDay = row.double("requests_per_day", "requests", "rpd", min = 0.0),
                hitRate = row.rate("hit_rate", "cache_hit_rate", default = 0.0),
                sensitivity = sensitivity,
                allowedRegions = allowed(row.optional("allowed_regions", "allowed_region"), region),
                containsPii = row.bool("contains_pii", "pii", default = sensitivity == Sensitivity.PRIVATE),
                containsSecret = row.bool("contains_secret", "secret", default = sensitivity == Sensitivity.SECRET),
                retentionSafe = row.bool("retention_safe", "zero_retention", "no_training", default = false),
                ageMinutes = row.long("age_minutes", "cache_age_minutes", default = 0L, min = 0L),
                maxStalenessMinutes = row.long("max_staleness_minutes", "freshness_sla_minutes", default = 240L, min = 1L),
                changeRatePerDay = row.double("change_rate_per_day", "changes_per_day", default = 0.0, min = 0.0),
                buildMillis = row.long("build_millis", "materialize_millis", default = 0L, min = 0L),
                pricePerMillion = row.double("price_per_mtok", "unit_price_per_mtok", "input_price_per_mtok", default = defaultPrice, min = 0.0),
                budgetDollarsPerDay = row.double("budget_dollars_per_day", "tenant_budget_per_day", default = Double.POSITIVE_INFINITY, min = 0.0),
                priority = row.rate("priority", "business_priority", default = 0.5),
                currentlyCached = row.bool("currently_cached", "cached", default = true),
                observedFailures = row.int("observed_failures", "failures", default = 0, min = 0),
                stateHash = stateHash,
                sourcePath = sourcePath,
                toolName = row.optional("tool_name", "tool") ?: "",
                rowNumber = line
            )
        }

        private fun allowed(value: String?, fallback: String): Set<String> {
            val raw = value?.trim()
            if (raw.isNullOrEmpty()) return setOf(fallback.slug())
            return raw.split('|', ';', ',').map { it.slug() }.filter { it.isNotEmpty() }.toSet().ifEmpty { setOf(fallback.slug()) }
        }
    }
}

private data class LeaseResult(
    val row: Int,
    val tenant: String,
    val provider: String,
    val region: String,
    val blockId: String,
    val decision: Decision,
    val severity: Severity,
    val reasons: List<String>,
    val ttlMinutes: Long,
    val confidence: Double,
    val grossSavings: Double,
    val netSavings: Double,
    val budgetShare: Double,
    val leaseKey: String,
    val sourcePath: String,
    val toolName: String
) {
    fun json(): String = buildString {
        append('{')
        jsonField("row", row); comma()
        jsonField("tenant", tenant); comma()
        jsonField("provider", provider); comma()
        jsonField("region", region); comma()
        jsonField("block_id", blockId); comma()
        jsonField("decision", decision.name.lower()); comma()
        jsonField("severity", severity.name.lower()); comma()
        jsonField("ttl_minutes", ttlMinutes); comma()
        jsonField("confidence", confidence); comma()
        jsonField("gross_savings_dollars", grossSavings); comma()
        jsonField("net_savings_dollars", netSavings); comma()
        jsonField("budget_share", budgetShare); comma()
        jsonField("lease_key", leaseKey); comma()
        jsonField("source_path", sourcePath); comma()
        jsonField("tool_name", toolName); comma()
        append("\"reasons\":")
        append(reasons.joinToString(prefix = "[", postfix = "]") { quote(it) })
        append('}')
    }
}

private data class Report(val results: List<LeaseResult>) {
    val maxSeverity: Severity = results.fold(Severity.INFO) { current, item ->
        if (item.severity.rank > current.rank) item.severity else current
    }
    val pinned: Int = results.count { it.decision == Decision.PIN }
    val refreshed: Int = results.count { it.decision == Decision.REFRESH }
    val bypassed: Int = results.count { it.decision == Decision.BYPASS }
    val evicted: Int = results.count { it.decision == Decision.EVICT }
    val quarantined: Int = results.count { it.decision == Decision.QUARANTINE }
    val positiveNetSavings: Double = results.fold(0.0) { total, item -> total + max(0.0, item.netSavings) }
}

private class LeaseGovernor(private val policy: Policy) {
    fun score(blocks: List<PromptBlock>): Report = Report(blocks.map { scoreOne(it) })

    private fun scoreOne(block: PromptBlock): LeaseResult {
        val reasons = mutableListOf<String>()
        val region = block.region.slug()
        val residencyOk = !policy.strictResidency || block.allowedRegions.contains("any") || block.allowedRegions.contains(region)
        val staleRatio = block.ageMinutes.toDouble() / max(1.0, block.maxStalenessMinutes.toDouble())
        val failurePenalty = min(0.8, block.observedFailures * 0.12)
        val stability = exp(-block.changeRatePerDay / 3.5) * (1.0 - failurePenalty)
        val gross = block.tokens * block.requestsPerDay * block.hitRate * block.pricePerMillion / 1_000_000.0
        val refreshes = block.changeRatePerDay + if (staleRatio > 1.0) 1.0 else 0.0
        val rebuild = block.tokens * refreshes * block.pricePerMillion / 1_000_000.0
        val materialize = block.buildMillis.toDouble() / 1000.0 * 0.000002
        val net = gross - rebuild - materialize
        val budgetShare = if (block.budgetDollarsPerDay.isFinite() && block.budgetDollarsPerDay > 0.0) gross / block.budgetDollarsPerDay else 0.0
        var privacy = block.sensitivity.risk
        if (block.containsPii) privacy += 0.25
        if (!block.retentionSafe && block.sensitivity != Sensitivity.PUBLIC) privacy += 0.15
        privacy = min(1.0, privacy)
        val volume = min(1.0, block.requestsPerDay / max(1.0, policy.minRequestsPerDay * 4.0))
        val hit = min(1.0, block.hitRate / max(0.01, policy.minHitRate))
        val confidence = (volume * hit * stability * (0.5 + block.priority / 2.0) * (1.0 - privacy)).bounded(0.0, 1.0)
        val ttl = round(block.maxStalenessMinutes * stability * (0.40 + confidence) / (1.0 + block.changeRatePerDay))
            .toLong()
            .bounded(policy.minTtlMinutes, policy.maxTtlMinutes)
        val key = stableHash(listOf(APP_VERSION, block.tenant.slug(), block.provider.slug(), region, block.blockId.slug(), block.stateHash))

        fun result(decision: Decision, severity: Severity): LeaseResult {
            return LeaseResult(block.rowNumber, block.tenant, block.provider, block.region, block.blockId, decision, severity, reasons.toList(), ttl, confidence, gross, net, budgetShare, key, block.sourcePath, block.toolName)
        }

        if (block.containsSecret || block.sensitivity == Sensitivity.SECRET) {
            reasons += "contains secret material"
            return result(Decision.QUARANTINE, Severity.CRITICAL)
        }
        if (!residencyOk) {
            reasons += "cache region is outside allowed_regions"
            return result(Decision.QUARANTINE, Severity.CRITICAL)
        }
        if ((block.containsPii || block.sensitivity == Sensitivity.PRIVATE) && !block.retentionSafe) {
            reasons += "private context lacks a retention safe provider path"
            return result(Decision.QUARANTINE, Severity.HIGH)
        }
        if (block.observedFailures >= 5) {
            reasons += "recent cache failures are too high"
            return result(lowValue(block), Severity.HIGH)
        }
        if (block.hitRate < policy.minHitRate) {
            reasons += "hit rate ${block.hitRate.percent()} is below ${policy.minHitRate.percent()}"
            return result(lowValue(block), Severity.WARN)
        }
        if (block.requestsPerDay < policy.minRequestsPerDay) {
            reasons += "request volume is too low for a durable lease"
            return result(lowValue(block), Severity.INFO)
        }
        if (budgetShare > policy.maxBudgetShare) {
            reasons += "one block consumes ${budgetShare.percent()} of tenant cache budget"
            return result(Decision.REFRESH, Severity.HIGH)
        }
        if (net < policy.minSavingsPerDay) {
            reasons += "net daily savings ${net.money()} is below ${policy.minSavingsPerDay.money()}"
            return result(lowValue(block), Severity.INFO)
        }
        if (staleRatio > 1.0) {
            reasons += "cache age is past its freshness limit"
            return result(Decision.REFRESH, Severity.WARN)
        }
        if (block.changeRatePerDay > 3.0 && staleRatio > 0.5) {
            reasons += "context changes often and is already halfway through freshness"
            return result(Decision.REFRESH, Severity.WARN)
        }
        if (confidence < 0.35) {
            reasons += "confidence is low after traffic, freshness, and privacy scoring"
            return result(Decision.REFRESH, Severity.WARN)
        }
        reasons += "safe cache candidate with measurable daily savings"
        return result(Decision.PIN, Severity.INFO)
    }

    private fun lowValue(block: PromptBlock): Decision = if (block.currentlyCached) Decision.EVICT else Decision.BYPASS
}

private object Render {
    fun jsonl(report: Report): String = report.results.joinToString(separator = "\n", postfix = "\n") { it.json() }

    fun table(report: Report): String {
        val out = StringBuilder()
        out.append("$APP $APP_VERSION rows=${report.results.size} pin=${report.pinned} refresh=${report.refreshed} bypass=${report.bypassed} evict=${report.evicted} quarantine=${report.quarantined} net=${report.positiveNetSavings.money()} max=${report.maxSeverity.name.lower()}\n\n")
        out.append("decision    severity  ttl   confidence  net/day   tenant/block\n")
        out.append("----------  --------  ----  ----------  --------  ------------------------------\n")
        for (item in report.results.sortedWith(compareByDescending<LeaseResult> { it.severity.rank }.thenBy { it.tenant })) {
            out.append(item.decision.name.lower().padEnd(10)).append("  ")
            out.append(item.severity.name.lower().padEnd(8)).append("  ")
            out.append(item.ttlMinutes.toString().padStart(4)).append("  ")
            out.append(item.confidence.fixed(2).padStart(10)).append("  ")
            out.append(item.netSavings.money().padStart(8)).append("  ")
            out.append(item.tenant).append('/').append(item.blockId).append(" - ")
            out.append(item.reasons.joinToString("; ")).append('\n')
        }
        return out.toString()
    }

    fun markdown(report: Report): String {
        val out = StringBuilder()
        out.append("# Prompt Cache Lease Report\n\n")
        out.append("- Rows: ${report.results.size}\n- Pin: ${report.pinned}\n- Refresh: ${report.refreshed}\n- Bypass: ${report.bypassed}\n- Evict: ${report.evicted}\n- Quarantine: ${report.quarantined}\n- Positive daily net savings: ${report.positiveNetSavings.money()}\n- Max severity: ${report.maxSeverity.name.lower()}\n\n")
        out.append("| Decision | Severity | TTL | Confidence | Net/day | Tenant | Block | Reason |\n")
        out.append("| --- | --- | ---: | ---: | ---: | --- | --- | --- |\n")
        for (item in report.results.sortedWith(compareByDescending<LeaseResult> { it.severity.rank }.thenBy { it.tenant })) {
            out.append("| ${item.decision.name.lower()} | ${item.severity.name.lower()} | ${item.ttlMinutes} | ${item.confidence.fixed(2)} | ${item.netSavings.money()} | ${cell(item.tenant)} | ${cell(item.blockId)} | ${cell(item.reasons.joinToString("; "))} |\n")
        }
        return out.toString()
    }

    private fun cell(value: String): String = value.replace("|", "\\|").replace("\n", " ")
}

private data class CsvTable(val rows: List<Row>)

private data class Row(private val values: Map<String, String>) {
    fun required(vararg names: String): String = optional(*names) ?: throw IllegalArgumentException("missing required column ${names.joinToString("/")}")

    fun optional(vararg names: String): String? {
        for (name in names) {
            val value = values[normalize(name)]?.trim()
            if (!value.isNullOrEmpty()) return value
        }
        return null
    }

    fun bool(vararg names: String, default: Boolean): Boolean {
        return when (val value = optional(*names)?.lower()) {
            null -> default
            "1", "true", "yes", "y", "on" -> true
            "0", "false", "no", "n", "off" -> false
            else -> throw IllegalArgumentException("${names.first()} must be boolean, got '$value'")
        }
    }

    fun int(vararg names: String, default: Int? = null, min: Int = Int.MIN_VALUE): Int {
        val value = optional(*names) ?: return default ?: throw IllegalArgumentException("missing required column ${names.joinToString("/")}")
        val parsed = value.toIntOrNull() ?: throw IllegalArgumentException("${names.first()} must be an integer")
        require(parsed >= min) { "${names.first()} must be at least $min" }
        return parsed
    }

    fun long(vararg names: String, default: Long? = null, min: Long = Long.MIN_VALUE): Long {
        val value = optional(*names) ?: return default ?: throw IllegalArgumentException("missing required column ${names.joinToString("/")}")
        val parsed = value.toLongOrNull() ?: throw IllegalArgumentException("${names.first()} must be an integer")
        require(parsed >= min) { "${names.first()} must be at least $min" }
        return parsed
    }

    fun double(vararg names: String, default: Double? = null, min: Double = -Double.MAX_VALUE): Double {
        val value = optional(*names) ?: return default ?: throw IllegalArgumentException("missing required column ${names.joinToString("/")}")
        val parsed = value.toDoubleOrNull() ?: throw IllegalArgumentException("${names.first()} must be numeric")
        require(parsed >= min) { "${names.first()} must be at least $min" }
        return parsed
    }

    fun rate(vararg names: String, default: Double): Double {
        val raw = optional(*names)?.toDoubleOrNull() ?: return default
        val normalized = if (raw > 1.0 && raw <= 100.0) raw / 100.0 else raw
        require(normalized in 0.0..1.0) { "${names.first()} must be between 0 and 1, or 0 and 100" }
        return normalized
    }
}

private object Csv {
    fun read(text: String): CsvTable {
        val records = records(text).filterNot { row -> row.all { it.trim().isEmpty() } }
        require(records.isNotEmpty()) { "CSV input is empty" }
        val header = records.first().map(::normalize)
        require(header.none { it.isEmpty() }) { "CSV header has an empty column" }
        require(header.toSet().size == header.size) { "CSV header has duplicate columns" }
        val rows = records.drop(1).mapIndexed { index, fields ->
            require(fields.size <= header.size) { "line ${index + 2}: too many fields" }
            Row(header.indices.associate { i -> header[i] to fields.getOrElse(i) { "" } })
        }
        return CsvTable(rows)
    }

    private fun records(text: String): List<List<String>> {
        val rows = mutableListOf<List<String>>()
        val row = mutableListOf<String>()
        val field = StringBuilder()
        var quoted = false
        var i = 0
        while (i < text.length) {
            val ch = text[i]
            when {
                quoted && ch == '"' && i + 1 < text.length && text[i + 1] == '"' -> {
                    field.append('"')
                    i += 1
                }
                ch == '"' -> quoted = !quoted
                !quoted && ch == ',' -> {
                    row += field.toString()
                    field.setLength(0)
                }
                !quoted && (ch == '\n' || ch == '\r') -> {
                    row += field.toString()
                    field.setLength(0)
                    rows += row.toList()
                    row.clear()
                    if (ch == '\r' && i + 1 < text.length && text[i + 1] == '\n') i += 1
                }
                else -> field.append(ch)
            }
            i += 1
        }
        require(!quoted) { "unterminated quoted CSV field" }
        if (field.isNotEmpty() || row.isNotEmpty()) {
            row += field.toString()
            rows += row.toList()
        }
        return rows
    }
}

private fun normalize(value: String): String = value.trim().lower().replace('-', '_').replace(' ', '_')

private fun String.lower(): String = trim().lowercase(Locale.ROOT)

private fun String.slug(): String = lower().replace(Regex("[^a-z0-9._:/]+"), "_").trim('_')

private fun String.positiveDouble(flag: String): Double {
    val value = toDoubleOrNull() ?: throw IllegalArgumentException("$flag must be numeric")
    require(value > 0.0) { "$flag must be positive" }
    return value
}

private fun String.positiveLong(flag: String): Long {
    val value = toLongOrNull() ?: throw IllegalArgumentException("$flag must be an integer")
    require(value > 0L) { "$flag must be positive" }
    return value
}

private fun stableHash(parts: List<String>): String {
    val bytes = MessageDigest.getInstance("SHA-256")
        .digest(parts.joinToString(separator = "\u001f").toByteArray(Charsets.UTF_8))
    return bytes.joinToString("") { "%02x".format(it.toInt() and 0xff) }.take(24)
}

private fun StringBuilder.jsonField(name: String, value: String) {
    append(quote(name)).append(':').append(quote(value))
}

private fun StringBuilder.jsonField(name: String, value: Int) {
    append(quote(name)).append(':').append(value)
}

private fun StringBuilder.jsonField(name: String, value: Long) {
    append(quote(name)).append(':').append(value)
}

private fun StringBuilder.jsonField(name: String, value: Double) {
    append(quote(name)).append(':').append(value.fixed(6))
}

private fun StringBuilder.comma() {
    append(',')
}

private fun quote(value: String): String {
    val out = StringBuilder(value.length + 8)
    out.append('"')
    for (ch in value) {
        when (ch) {
            '\\' -> out.append("\\\\")
            '"' -> out.append("\\\"")
            '\n' -> out.append("\\n")
            '\r' -> out.append("\\r")
            '\t' -> out.append("\\t")
            else -> if (ch.toInt() < 32) out.append("\\u").append(ch.toInt().toString(16).padStart(4, '0')) else out.append(ch)
        }
    }
    out.append('"')
    return out.toString()
}

private fun Double.money(): String = if (isFinite()) "$" + fixed(4) else "unbounded"

private fun Double.percent(): String = (this * 100.0).fixed(1) + "%"

private fun Double.fixed(scale: Int): String = "%.${scale}f".format(Locale.US, this)

private fun Double.bounded(minimum: Double, maximum: Double): Double = max(minimum, min(maximum, this))

private fun Long.bounded(minimum: Long, maximum: Long): Long = max(minimum, min(maximum, this))

/*
This solves the prompt cache lease problem that shows up when AI agents, RAG systems, MCP tools, long context applications, and prompt caching platforms start reusing the same expensive system prompts, tool schemas, retrieval preambles, policy text, and customer context. Built because in April 2026 a cache miss can waste real money, but a bad cache hit can leak private data, break data residency, or serve stale instructions that quietly change model behavior. Use it when you need a practical prompt cache CLI, context cache policy checker, prompt cache cost optimizer, RAG cache governance tool, LLM cache risk scanner, or AI infrastructure FinOps gate before rollout. The trick: it scores money, hit rate, freshness, tenant budget, allowed regions, retention safety, sensitivity, and failure history together instead of pretending token savings alone are enough. Drop this into CI, a Gradle task, a data pipeline, or an internal platform job and feed it CSV from logs, traces, billing exports, prompt registry metadata, or cache metadata so the team can pin, refresh, evict, bypass, or quarantine cache blocks with reasons a human can defend.
*/
