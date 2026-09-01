import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.time.Instant
import java.time.format.DateTimeParseException
import kotlin.system.exitProcess

private const val TOOL_NAME = "AgentDelegationChainAuditor"
private const val DEFAULT_MAX_DEPTH = 6
private const val DEFAULT_STALE_ORPHAN_MINUTES = 15.0
private const val DEFAULT_DANGEROUS_TOOLS =
    "shell_exec,payment_transfer,send_email,delete_repository,rotate_credentials,wire_transfer,sudo_exec,ssh_exec"

enum class Severity(val rank: Int, val label: String) {
    LOW(0, "low"),
    MEDIUM(1, "medium"),
    HIGH(2, "high"),
    CRITICAL(3, "critical");

    companion object {
        fun parse(raw: String): Severity =
            entries.firstOrNull { it.label == raw.trim().lowercase() }
                ?: throw IllegalArgumentException("unknown severity '$raw', expected one of ${entries.joinToString(",") { it.label }}")
    }
}

data class Finding(
    val severity: Severity,
    val code: String,
    val agentId: String,
    val message: String,
    val detail: Map<String, String> = emptyMap(),
)

class CliConfig(
    val tracePath: String?,
    val maxDepth: Int,
    val staleOrphanMinutes: Double,
    val failOn: Severity?,
    val format: String,
    val dangerousTools: Set<String>,
    val quiet: Boolean,
    val showHelp: Boolean,
) {
    companion object {
        fun usage(): String = """
            Usage: kotlin $TOOL_NAME.kt [options]

              --trace <path>              JSONL trace file (default: read stdin, or pass '-')
              --max-depth <int>           maximum allowed delegation depth (default: $DEFAULT_MAX_DEPTH)
              --stale-orphan-minutes <n>  minutes since last trace event before an unfinished agent is "stale" (default: $DEFAULT_STALE_ORPHAN_MINUTES)
              --fail-on <severity>        critical|high|medium|low|none; exit 1 if a finding at/above this severity exists (default: critical)
              --format <json|table>       output format (default: table)
              --dangerous-tools <csv>     tool names that make a scope escalation CRITICAL (default: builtin list)
              --quiet                     suppress the findings table/summary lines, print only the exit-relevant line
              --help                      show this message

            Reads a JSONL trace of multi-agent delegation events (spawn, charge, complete, fail)
            and audits the delegation graph for cycles, budget escalation, privilege escalation
            across handoff boundaries, deadline-inheritance violations, and orphaned agents.

        """.trimIndent() + "\n"

        fun parse(args: Array<String>): CliConfig {
            var tracePath: String? = null
            var maxDepth = DEFAULT_MAX_DEPTH
            var staleOrphanMinutes = DEFAULT_STALE_ORPHAN_MINUTES
            var failOn: Severity? = Severity.CRITICAL
            var format = "table"
            var dangerousTools = DEFAULT_DANGEROUS_TOOLS.split(',').map { it.trim().lowercase() }.toSet()
            var quiet = false
            var showHelp = false

            var i = 0
            while (i < args.size) {
                when (val arg = args[i]) {
                    "--trace" -> { tracePath = args.requireValue(i, arg); i += 1 }
                    "--max-depth" -> { maxDepth = args.requireValue(i, arg).toIntOrNull() ?: throw IllegalArgumentException("--max-depth must be an integer"); i += 1 }
                    "--stale-orphan-minutes" -> { staleOrphanMinutes = args.requireValue(i, arg).toDoubleOrNull() ?: throw IllegalArgumentException("--stale-orphan-minutes must be numeric"); i += 1 }
                    "--fail-on" -> {
                        val value = args.requireValue(i, arg)
                        failOn = if (value.trim().lowercase() == "none") null else Severity.parse(value)
                        i += 1
                    }
                    "--format" -> {
                        format = args.requireValue(i, arg).trim().lowercase()
                        require(format == "json" || format == "table") { "--format must be json or table" }
                        i += 1
                    }
                    "--dangerous-tools" -> {
                        dangerousTools = args.requireValue(i, arg).split(',').map { it.trim().lowercase() }.filter { it.isNotEmpty() }.toSet()
                        i += 1
                    }
                    "--quiet" -> quiet = true
                    "--help", "-h" -> showHelp = true
                    else -> throw IllegalArgumentException("unrecognized argument '$arg'")
                }
                i += 1
            }

            require(maxDepth >= 1) { "--max-depth must be at least 1" }
            require(staleOrphanMinutes >= 0.0) { "--stale-orphan-minutes must be non-negative" }

            return CliConfig(tracePath, maxDepth, staleOrphanMinutes, failOn, format, dangerousTools, quiet, showHelp)
        }

        private fun Array<String>.requireValue(index: Int, flag: String): String {
            if (index + 1 >= size) throw IllegalArgumentException("$flag requires a value")
            return this[index + 1]
        }
    }
}

sealed class JsonValue {
    data class Obj(val fields: LinkedHashMap<String, JsonValue>) : JsonValue()
    data class Arr(val items: MutableList<JsonValue>) : JsonValue()
    data class Str(val value: String) : JsonValue()
    data class Num(val value: Double) : JsonValue()
    data class Bool(val value: Boolean) : JsonValue()
    object Null : JsonValue()
}

class JsonParseException(message: String, val position: Int) : Exception("$message at position $position")

object JsonParser {
    fun parse(text: String): JsonValue {
        val cursor = intArrayOf(0)
        val value = parseValue(text, cursor)
        skipWhitespace(text, cursor)
        if (cursor[0] != text.length) throw JsonParseException("trailing content after JSON value", cursor[0])
        return value
    }

    private fun parseValue(text: String, cursor: IntArray): JsonValue {
        skipWhitespace(text, cursor)
        if (cursor[0] >= text.length) throw JsonParseException("unexpected end of input", cursor[0])
        return when (text[cursor[0]]) {
            '{' -> parseObject(text, cursor)
            '[' -> parseArray(text, cursor)
            '"' -> JsonValue.Str(parseString(text, cursor))
            't' -> { expectLiteral(text, cursor, "true"); JsonValue.Bool(true) }
            'f' -> { expectLiteral(text, cursor, "false"); JsonValue.Bool(false) }
            'n' -> { expectLiteral(text, cursor, "null"); JsonValue.Null }
            else -> parseNumber(text, cursor)
        }
    }

    private fun parseObject(text: String, cursor: IntArray): JsonValue.Obj {
        val fields = LinkedHashMap<String, JsonValue>()
        cursor[0] += 1
        skipWhitespace(text, cursor)
        if (peek(text, cursor) == '}') { cursor[0] += 1; return JsonValue.Obj(fields) }
        while (true) {
            skipWhitespace(text, cursor)
            val key = parseString(text, cursor)
            skipWhitespace(text, cursor)
            expectChar(text, cursor, ':')
            val value = parseValue(text, cursor)
            fields[key] = value
            skipWhitespace(text, cursor)
            when (peek(text, cursor)) {
                ',' -> { cursor[0] += 1 }
                '}' -> { cursor[0] += 1; return JsonValue.Obj(fields) }
                else -> throw JsonParseException("expected ',' or '}' in object", cursor[0])
            }
        }
    }

    private fun parseArray(text: String, cursor: IntArray): JsonValue.Arr {
        val items = mutableListOf<JsonValue>()
        cursor[0] += 1
        skipWhitespace(text, cursor)
        if (peek(text, cursor) == ']') { cursor[0] += 1; return JsonValue.Arr(items) }
        while (true) {
            items += parseValue(text, cursor)
            skipWhitespace(text, cursor)
            when (peek(text, cursor)) {
                ',' -> { cursor[0] += 1 }
                ']' -> { cursor[0] += 1; return JsonValue.Arr(items) }
                else -> throw JsonParseException("expected ',' or ']' in array", cursor[0])
            }
        }
    }

    private fun parseString(text: String, cursor: IntArray): String {
        expectChar(text, cursor, '"')
        val out = StringBuilder()
        while (true) {
            if (cursor[0] >= text.length) throw JsonParseException("unterminated string", cursor[0])
            val ch = text[cursor[0]]
            cursor[0] += 1
            when (ch) {
                '"' -> return out.toString()
                '\\' -> {
                    if (cursor[0] >= text.length) throw JsonParseException("unterminated escape", cursor[0])
                    val esc = text[cursor[0]]
                    cursor[0] += 1
                    when (esc) {
                        '"' -> out.append('"')
                        '\\' -> out.append('\\')
                        '/' -> out.append('/')
                        'b' -> out.append('\b')
                        'f' -> out.append('\u000C')
                        'n' -> out.append('\n')
                        'r' -> out.append('\r')
                        't' -> out.append('\t')
                        'u' -> {
                            if (cursor[0] + 4 > text.length) throw JsonParseException("truncated unicode escape", cursor[0])
                            val hex = text.substring(cursor[0], cursor[0] + 4)
                            out.append(hex.toInt(16).toChar())
                            cursor[0] += 4
                        }
                        else -> throw JsonParseException("invalid escape '\\$esc'", cursor[0])
                    }
                }
                else -> out.append(ch)
            }
        }
    }

    private fun parseNumber(text: String, cursor: IntArray): JsonValue.Num {
        val start = cursor[0]
        if (peek(text, cursor) == '-') cursor[0] += 1
        while (cursor[0] < text.length && text[cursor[0]].isDigit()) cursor[0] += 1
        if (peek(text, cursor) == '.') {
            cursor[0] += 1
            while (cursor[0] < text.length && text[cursor[0]].isDigit()) cursor[0] += 1
        }
        if (peek(text, cursor) == 'e' || peek(text, cursor) == 'E') {
            cursor[0] += 1
            if (peek(text, cursor) == '+' || peek(text, cursor) == '-') cursor[0] += 1
            while (cursor[0] < text.length && text[cursor[0]].isDigit()) cursor[0] += 1
        }
        val slice = text.substring(start, cursor[0])
        val value = slice.toDoubleOrNull() ?: throw JsonParseException("invalid number literal '$slice'", start)
        return JsonValue.Num(value)
    }

    private fun expectLiteral(text: String, cursor: IntArray, literal: String) {
        if (cursor[0] + literal.length > text.length || text.substring(cursor[0], cursor[0] + literal.length) != literal) {
            throw JsonParseException("expected literal '$literal'", cursor[0])
        }
        cursor[0] += literal.length
    }

    private fun expectChar(text: String, cursor: IntArray, expected: Char) {
        if (peek(text, cursor) != expected) throw JsonParseException("expected '$expected'", cursor[0])
        cursor[0] += 1
    }

    private fun peek(text: String, cursor: IntArray): Char? = if (cursor[0] < text.length) text[cursor[0]] else null

    private fun skipWhitespace(text: String, cursor: IntArray) {
        while (cursor[0] < text.length && text[cursor[0]].isWhitespace()) cursor[0] += 1
    }
}

private fun JsonValue.Obj.stringOrNull(key: String): String? = (fields[key] as? JsonValue.Str)?.value
private fun JsonValue.Obj.string(key: String): String = stringOrNull(key) ?: throw IllegalArgumentException("missing required string field '$key'")
private fun JsonValue.Obj.numberOrNull(key: String): Double? = (fields[key] as? JsonValue.Num)?.value
private fun JsonValue.Obj.stringList(key: String): List<String> =
    (fields[key] as? JsonValue.Arr)?.items?.mapNotNull { (it as? JsonValue.Str)?.value } ?: emptyList()

enum class AgentStatus { RUNNING, COMPLETED, FAILED }

class AgentNode(
    val id: String,
    val parentId: String?,
    val role: String,
    val spawnTs: Instant,
    val deadline: Instant?,
    val budgetUsd: Double?,
    val permittedTools: Set<String>,
) {
    var depth: Int = 0
    var status: AgentStatus = AgentStatus.RUNNING
    var terminalTs: Instant? = null
    var spentUsd: Double = 0.0
    var allocatedToChildrenUsd: Double = 0.0
}

class ChainAuditor(
    private val maxDepth: Int,
    private val staleOrphanMinutes: Double,
    private val dangerousTools: Set<String>,
) {
    private val agents = LinkedHashMap<String, AgentNode>()
    private val findings = mutableListOf<Finding>()
    private var lastEventTs: Instant? = null

    fun findings(): List<Finding> = findings

    fun onSpawn(obj: JsonValue.Obj) {
        val id = obj.string("agent_id")
        val parentId = obj.stringOrNull("parent_id")
        val ts = parseTimestamp(obj.string("ts"))
        touchClock(ts)

        if (agents.containsKey(id)) {
            findings += Finding(Severity.HIGH, "DUPLICATE_SPAWN", id, "agent_id '$id' was spawned more than once in this trace")
            return
        }

        val deadline = obj.stringOrNull("deadline_ts")?.let { parseTimestamp(it) }
        val budget = obj.numberOrNull("budget_usd")
        val tools = obj.stringList("permitted_tools").map { it.trim().lowercase() }.toSet()
        val node = AgentNode(id, parentId, obj.stringOrNull("role") ?: "unknown", ts, deadline, budget, tools)
        agents[id] = node

        if (parentId == null) {
            node.depth = 0
            return
        }

        val parent = agents[parentId]
        if (parent == null) {
            findings += Finding(
                Severity.LOW, "UNKNOWN_PARENT", id,
                "agent '$id' declares parent_id '$parentId' which never appears as a spawn event (trace may be truncated)",
            )
            node.depth = 1
            return
        }

        if (detectsCycle(parentId, id)) {
            findings += Finding(
                Severity.CRITICAL, "DELEGATION_CYCLE", id,
                "agent '$id' delegation chain loops back through an ancestor via parent '$parentId'",
            )
        }

        node.depth = parent.depth + 1
        if (node.depth > maxDepth) {
            findings += Finding(
                Severity.MEDIUM, "DEPTH_EXCEEDED", id,
                "delegation depth ${node.depth} exceeds configured max of $maxDepth",
                mapOf("depth" to node.depth.toString(), "max_depth" to maxDepth.toString()),
            )
        }

        if (parent.permittedTools.isNotEmpty()) {
            val escalated = node.permittedTools - parent.permittedTools
            if (escalated.isNotEmpty()) {
                val dangerous = escalated.intersect(dangerousTools)
                val severity = if (dangerous.isNotEmpty()) Severity.CRITICAL else Severity.HIGH
                findings += Finding(
                    severity, "SCOPE_ESCALATION", id,
                    "agent '$id' was granted tool(s) its parent '$parentId' did not have: ${escalated.sorted().joinToString(", ")}",
                    mapOf("escalated_tools" to escalated.sorted().joinToString(","), "dangerous" to dangerous.isNotEmpty().toString()),
                )
            }
        }

        if (parent.budgetUsd != null && node.budgetUsd != null) {
            parent.allocatedToChildrenUsd += node.budgetUsd
            val parentAvailable = parent.budgetUsd - parent.spentUsd
            if (parent.allocatedToChildrenUsd > parentAvailable + 1e-9) {
                findings += Finding(
                    Severity.HIGH, "BUDGET_ESCALATION", parentId,
                    "agent '$parentId' has allocated \$${"%.4f".format(parent.allocatedToChildrenUsd)} to children against \$${"%.4f".format(parentAvailable)} it actually has available",
                    mapOf("allocated_to_children_usd" to "%.4f".format(parent.allocatedToChildrenUsd), "parent_available_usd" to "%.4f".format(parentAvailable)),
                )
            }
        }

        if (parent.deadline != null && node.deadline != null && node.deadline.isAfter(parent.deadline)) {
            findings += Finding(
                Severity.HIGH, "DEADLINE_INHERITANCE_VIOLATION", id,
                "agent '$id' deadline ${node.deadline} is later than parent '$parentId' deadline ${parent.deadline}; child deadline should be clamped, not reset",
                mapOf("child_deadline" to node.deadline.toString(), "parent_deadline" to parent.deadline.toString()),
            )
        }
    }

    fun onCharge(obj: JsonValue.Obj) {
        val id = obj.string("agent_id")
        val ts = parseTimestamp(obj.string("ts"))
        touchClock(ts)
        val amount = obj.numberOrNull("amount_usd") ?: throw IllegalArgumentException("charge event missing amount_usd")
        val node = agents[id] ?: run {
            findings += Finding(Severity.LOW, "CHARGE_UNKNOWN_AGENT", id, "charge of \$${"%.4f".format(amount)} recorded for an agent_id never spawned")
            return
        }
        node.spentUsd += amount
        if (node.budgetUsd != null && node.spentUsd > node.budgetUsd + 1e-9) {
            findings += Finding(
                Severity.CRITICAL, "BUDGET_BREACH", id,
                "agent '$id' spent \$${"%.4f".format(node.spentUsd)} against an allocated budget of \$${"%.4f".format(node.budgetUsd)}",
                mapOf("spent_usd" to "%.4f".format(node.spentUsd), "budget_usd" to "%.4f".format(node.budgetUsd)),
            )
        }
    }

    fun onComplete(obj: JsonValue.Obj) {
        val id = obj.string("agent_id")
        val ts = parseTimestamp(obj.string("ts"))
        touchClock(ts)
        val node = agents[id] ?: return
        node.status = AgentStatus.COMPLETED
        node.terminalTs = ts
    }

    fun onFail(obj: JsonValue.Obj) {
        val id = obj.string("agent_id")
        val ts = parseTimestamp(obj.string("ts"))
        touchClock(ts)
        val node = agents[id] ?: return
        node.status = AgentStatus.FAILED
        node.terminalTs = ts
    }

    fun finalize() {
        val clock = lastEventTs ?: return
        for (node in agents.values) {
            if (node.status != AgentStatus.RUNNING) continue
            val ageMinutes = java.time.Duration.between(node.spawnTs, clock).toMillis() / 60000.0
            if (ageMinutes >= staleOrphanMinutes) {
                findings += Finding(
                    Severity.MEDIUM, "STALE_ORPHAN", node.id,
                    "agent '${node.id}' spawned ${"%.1f".format(ageMinutes)} minutes before the last trace event and never completed or failed",
                    mapOf("age_minutes" to "%.1f".format(ageMinutes)),
                )
            } else {
                findings += Finding(
                    Severity.LOW, "ORPHAN_AGENT", node.id,
                    "agent '${node.id}' has no completion or failure event by the end of the trace",
                )
            }
        }
    }

    fun summary(): Map<String, String> {
        val maxDepthReached = agents.values.maxOfOrNull { it.depth } ?: 0
        val totalSpent = agents.values.sumOf { it.spentUsd }
        val orphanCount = agents.values.count { it.status == AgentStatus.RUNNING }
        val completed = agents.values.count { it.status == AgentStatus.COMPLETED }
        val failed = agents.values.count { it.status == AgentStatus.FAILED }
        return linkedMapOf(
            "total_agents" to agents.size.toString(),
            "max_depth_reached" to maxDepthReached.toString(),
            "total_spent_usd" to "%.4f".format(totalSpent),
            "completed" to completed.toString(),
            "failed" to failed.toString(),
            "still_running_or_orphaned" to orphanCount.toString(),
        )
    }

    private fun touchClock(ts: Instant) {
        val previous = lastEventTs
        if (previous == null || ts.isAfter(previous)) lastEventTs = ts
    }

    private fun detectsCycle(parentId: String, newId: String): Boolean {
        var cursor: String? = parentId
        var hops = 0
        while (cursor != null && hops <= agents.size + 1) {
            if (cursor == newId) return true
            cursor = agents[cursor]?.parentId
            hops += 1
        }
        return false
    }

    private fun parseTimestamp(raw: String): Instant = try {
        Instant.parse(raw)
    } catch (error: DateTimeParseException) {
        throw IllegalArgumentException("invalid ISO-8601 timestamp '$raw': ${error.message}")
    }
}

private fun StringBuilder.jsonField(name: String, value: String, trailingComma: Boolean = true) {
    append(quoteJson(name)).append(':').append(quoteJson(value))
    if (trailingComma) append(',')
}

private fun StringBuilder.jsonRaw(name: String, value: String, trailingComma: Boolean = true) {
    append(quoteJson(name)).append(':').append(value)
    if (trailingComma) append(',')
}

private fun quoteJson(value: String): String {
    val out = StringBuilder(value.length + 8)
    out.append('"')
    for (ch in value) {
        when (ch) {
            '\\' -> out.append("\\\\")
            '"' -> out.append("\\\"")
            '\n' -> out.append("\\n")
            '\r' -> out.append("\\r")
            '\t' -> out.append("\\t")
            else -> if (ch.code < 32) out.append("\\u").append(ch.code.toString(16).padStart(4, '0')) else out.append(ch)
        }
    }
    out.append('"')
    return out.toString()
}

private fun renderJson(findings: List<Finding>, summary: Map<String, String>): String {
    val sb = StringBuilder()
    sb.append("{")
    sb.append("\"tool\":").append(quoteJson(TOOL_NAME)).append(',')
    sb.append("\"summary\":{")
    val summaryKeys = summary.keys.toList()
    summaryKeys.forEachIndexed { index, key ->
        sb.jsonRaw(key, summary.getValue(key), trailingComma = index != summaryKeys.lastIndex)
    }
    sb.append("},")
    sb.append("\"findings\":[")
    findings.forEachIndexed { index, finding ->
        sb.append("{")
        sb.jsonField("severity", finding.severity.label)
        sb.jsonField("code", finding.code)
        sb.jsonField("agent_id", finding.agentId)
        sb.jsonField("message", finding.message, trailingComma = finding.detail.isNotEmpty())
        val detailKeys = finding.detail.keys.toList()
        if (detailKeys.isNotEmpty()) {
            sb.append("\"detail\":{")
            detailKeys.forEachIndexed { detailIndex, key ->
                sb.jsonField(key, finding.detail.getValue(key), trailingComma = detailIndex != detailKeys.lastIndex)
            }
            sb.append("}")
        }
        sb.append("}")
        if (index != findings.lastIndex) sb.append(",")
    }
    sb.append("]}")
    return sb.toString()
}

private fun renderTable(findings: List<Finding>, summary: Map<String, String>, quiet: Boolean): String {
    val sb = StringBuilder()
    if (!quiet) {
        sb.append("$TOOL_NAME\n")
        sb.append("-".repeat(TOOL_NAME.length)).append("\n\n")
        for ((key, value) in summary) sb.append(key.padEnd(28)).append(value).append("\n")
        sb.append("\n")
        if (findings.isEmpty()) {
            sb.append("no findings\n")
        } else {
            val bySeverity = findings.sortedByDescending { it.severity.rank }
            for (finding in bySeverity) {
                sb.append("[").append(finding.severity.label.uppercase().padEnd(8)).append("] ")
                sb.append(finding.code.padEnd(28)).append(" ")
                sb.append(finding.agentId.padEnd(16)).append(" ")
                sb.append(finding.message).append("\n")
            }
        }
    }
    val counts = Severity.entries.associateWith { severity -> findings.count { it.severity == severity } }
    sb.append("\ncounts: ")
    sb.append(Severity.entries.joinToString(" ") { "${it.label}=${counts.getValue(it)}" })
    sb.append("\n")
    return sb.toString()
}

fun main(args: Array<String>) {
    val config = try {
        CliConfig.parse(args)
    } catch (error: IllegalArgumentException) {
        System.err.println("$TOOL_NAME: ${error.message}")
        System.err.println()
        System.err.print(CliConfig.usage())
        exitProcess(64)
    }

    if (config.showHelp) {
        print(CliConfig.usage())
        exitProcess(0)
    }

    val reader: BufferedReader = if (config.tracePath == null || config.tracePath == "-") {
        BufferedReader(InputStreamReader(System.`in`, Charsets.UTF_8))
    } else {
        val file = File(config.tracePath)
        if (!file.isFile) {
            System.err.println("$TOOL_NAME: trace file not found: ${config.tracePath}")
            exitProcess(66)
        }
        file.bufferedReader(Charsets.UTF_8)
    }

    val auditor = ChainAuditor(config.maxDepth, config.staleOrphanMinutes, config.dangerousTools)

    reader.useLines { lines ->
        var lineNumber = 0
        for (rawLine in lines) {
            lineNumber += 1
            val line = rawLine.trim()
            if (line.isEmpty()) continue
            try {
                val parsed = JsonParser.parse(line)
                val obj = parsed as? JsonValue.Obj ?: throw IllegalArgumentException("trace line is not a JSON object")
                when (val type = obj.string("type")) {
                    "spawn" -> auditor.onSpawn(obj)
                    "charge" -> auditor.onCharge(obj)
                    "complete" -> auditor.onComplete(obj)
                    "fail" -> auditor.onFail(obj)
                    else -> throw IllegalArgumentException("unknown event type '$type'")
                }
            } catch (error: Exception) {
                System.err.println("$TOOL_NAME: skipping malformed trace line $lineNumber: ${error.message}")
            }
        }
    }

    auditor.finalize()
    val findings = auditor.findings()
    val summary = auditor.summary()

    val output = if (config.format == "json") renderJson(findings, summary) else renderTable(findings, summary, config.quiet)
    println(output)

    val worst = findings.maxOfOrNull { it.severity }
    val shouldFail = config.failOn != null && worst != null && worst.rank >= config.failOn.rank
    exitProcess(if (shouldFail) 1 else 0)
}

/*
This solves the blind spot that shows up once an AI product stops being one model answering one prompt and becomes a tree of agents spawning other agents: a Claude Agent SDK orchestrator kicking off subagents, a LangGraph graph routing between nodes, a CrewAI crew delegating tasks, or any in-house multi-agent framework where agent A can decide to hand work off to agent B, who can hand it off to agent C. Built because in April 2026 that handoff boundary is where the real incidents happen and almost nobody logs it: a routing bug sends work back to an ancestor and the system loops until the API bill notices before a human does, a child agent quietly inherits or requests a tool the parent was never allowed to use so a "read-only research agent" ends up able to send email or run shell commands, a parent hands out more budget to children than it was actually given so three siblings each think they have the full allowance, or a spawned agent just never reports back and keeps burning tokens as an invisible zombie process nobody is watching. Use it when you already emit or can cheaply emit a JSONL trace of spawn, charge, complete, and fail events from your orchestrator and you want a fast, dependency-free auditor to run in CI, in a pre-deploy gate, or against production logs on a schedule, so a human can see the delegation graph's actual failure modes instead of trusting that structured concurrency or "the framework handles it" quietly kept everyone honest. The trick: it does not just replay individual tool calls, it reconstructs the delegation tree itself and checks the boundary between parent and child at every handoff, walking the live ancestor chain to catch cycles the moment they would form, comparing a child's permitted tool set against its parent's so privilege escalation is caught structurally instead of by an auditor reading logs after the fact, tracking allocated-to-children budget against what the parent actually has left instead of trusting each agent's self-reported spend in isolation, and separately flagging a deadline a child inherited correctly from one it was simply reset to be longer than what its parent had left, because those are different bugs with different fixes. Drop this single file into a JVM or Android backend repo, run it with plain kotlinc or as a Gradle exec task with no external dependencies at all, wire your orchestrator's event log into it as JSONL over stdin or a file path, and use --fail-on to turn cycles, budget breaches, and dangerous-tool escalation into a hard CI gate before the multi-agent system you shipped last week is trusted with a production budget.
*/
