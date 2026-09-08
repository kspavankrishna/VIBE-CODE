import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.EOFException
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.util.ArrayDeque
import java.util.LinkedHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private const val WATCHDOG_NAME = "McpStdioWatchdog"
private const val TIMEOUT_ERROR_CODE = -32001
private const val UNAVAILABLE_ERROR_CODE = -32002
private const val DUPLICATE_ID_ERROR_CODE = -32600
private const val QUEUE_FULL_ERROR_CODE = -32003

fun main(args: Array<String>) {
    val config = try {
        Config.parse(args)
    } catch (error: IllegalArgumentException) {
        System.err.println("$WATCHDOG_NAME: ${error.message}")
        System.err.println()
        System.err.print(Config.usage())
        exitProcess(64)
    }

    if (config.showHelp) {
        print(Config.usage())
        return
    }

    val watchdog = McpStdioWatchdog(config)
    Runtime.getRuntime().addShutdownHook(Thread {
        watchdog.close("shutdown-hook")
    })
    exitProcess(watchdog.run())
}

private data class Config(
    val childCommand: List<String>,
    val cwd: String?,
    val defaultRequestTimeoutMs: Long,
    val methodTimeouts: Map<String, Long>,
    val idleTimeoutMs: Long,
    val restartOnTimeout: Boolean,
    val replayInitialize: Boolean,
    val maxRestarts: Int,
    val restartWindowMs: Long,
    val restartBackoffMs: Long,
    val killGraceMs: Long,
    val logFile: String?,
    val stderrFile: String?,
    val stderrMaxBytes: Long,
    val maxQueuedFrames: Int,
    val housekeepingIntervalMs: Long,
    val showHelp: Boolean
) {
    companion object {
        private val builtInTimeouts = linkedMapOf(
            "initialize" to 30_000L,
            "tools/list" to 20_000L,
            "resources/list" to 20_000L,
            "prompts/list" to 20_000L,
            "resources/read" to 120_000L,
            "tools/call" to 300_000L,
            "sampling/createMessage" to 600_000L,
            "completion/complete" to 60_000L
        )

        fun parse(args: Array<String>): Config {
            if (args.isEmpty()) {
                throw IllegalArgumentException("missing arguments")
            }

            var cwd: String? = null
            var defaultRequestTimeoutMs = 90_000L
            val methodTimeouts = linkedMapOf<String, Long>()
            var idleTimeoutMs = 0L
            var restartOnTimeout = true
            var replayInitialize = true
            var maxRestarts = 6
            var restartWindowMs = 300_000L
            var restartBackoffMs = 1_500L
            var killGraceMs = 2_000L
            var logFile: String? = null
            var stderrFile: String? = null
            var stderrMaxBytes = 1_048_576L
            var maxQueuedFrames = 256
            var housekeepingIntervalMs = 250L
            var showHelp = false
            val childCommand = mutableListOf<String>()

            var index = 0
            while (index < args.size) {
                val argument = args[index]
                if (argument == "--") {
                    childCommand.addAll(args.copyOfRange(index + 1, args.size))
                    break
                }

                when {
                    argument == "--help" || argument == "-h" -> {
                        showHelp = true
                        index += 1
                    }
                    argument == "--cwd" -> {
                        cwd = requireValue(argument, args, index)
                        index += 2
                    }
                    argument.startsWith("--cwd=") -> {
                        cwd = argument.substringAfter('=')
                        index += 1
                    }
                    argument == "--default-request-timeout-ms" -> {
                        defaultRequestTimeoutMs = parsePositiveLong(argument, requireValue(argument, args, index))
                        index += 2
                    }
                    argument.startsWith("--default-request-timeout-ms=") -> {
                        defaultRequestTimeoutMs = parsePositiveLong(argument, argument.substringAfter('='))
                        index += 1
                    }
                    argument == "--method-timeout" -> {
                        val pair = parseMethodTimeout(requireValue(argument, args, index))
                        methodTimeouts[pair.first] = pair.second
                        index += 2
                    }
                    argument.startsWith("--method-timeout=") -> {
                        val pair = parseMethodTimeout(argument.substringAfter('='))
                        methodTimeouts[pair.first] = pair.second
                        index += 1
                    }
                    argument == "--idle-timeout-ms" -> {
                        idleTimeoutMs = parseNonNegativeLong(argument, requireValue(argument, args, index))
                        index += 2
                    }
                    argument.startsWith("--idle-timeout-ms=") -> {
                        idleTimeoutMs = parseNonNegativeLong(argument, argument.substringAfter('='))
                        index += 1
                    }
                    argument == "--restart-on-timeout" -> {
                        restartOnTimeout = true
                        index += 1
                    }
                    argument == "--no-restart-on-timeout" -> {
                        restartOnTimeout = false
                        index += 1
                    }
                    argument == "--disable-replay-initialize" -> {
                        replayInitialize = false
                        index += 1
                    }
                    argument == "--max-restarts" -> {
                        maxRestarts = parseNonNegativeInt(argument, requireValue(argument, args, index))
                        index += 2
                    }
                    argument.startsWith("--max-restarts=") -> {
                        maxRestarts = parseNonNegativeInt(argument, argument.substringAfter('='))
                        index += 1
                    }
                    argument == "--restart-window-ms" -> {
                        restartWindowMs = parsePositiveLong(argument, requireValue(argument, args, index))
                        index += 2
                    }
                    argument.startsWith("--restart-window-ms=") -> {
                        restartWindowMs = parsePositiveLong(argument, argument.substringAfter('='))
                        index += 1
                    }
                    argument == "--restart-backoff-ms" -> {
                        restartBackoffMs = parseNonNegativeLong(argument, requireValue(argument, args, index))
                        index += 2
                    }
                    argument.startsWith("--restart-backoff-ms=") -> {
                        restartBackoffMs = parseNonNegativeLong(argument, argument.substringAfter('='))
                        index += 1
                    }
                    argument == "--kill-grace-ms" -> {
                        killGraceMs = parsePositiveLong(argument, requireValue(argument, args, index))
                        index += 2
                    }
                    argument.startsWith("--kill-grace-ms=") -> {
                        killGraceMs = parsePositiveLong(argument, argument.substringAfter('='))
                        index += 1
                    }
                    argument == "--log-file" -> {
                        logFile = requireValue(argument, args, index)
                        index += 2
                    }
                    argument.startsWith("--log-file=") -> {
                        logFile = argument.substringAfter('=')
                        index += 1
                    }
                    argument == "--stderr-file" -> {
                        stderrFile = requireValue(argument, args, index)
                        index += 2
                    }
                    argument.startsWith("--stderr-file=") -> {
                        stderrFile = argument.substringAfter('=')
                        index += 1
                    }
                    argument == "--stderr-max-bytes" -> {
                        stderrMaxBytes = parsePositiveLong(argument, requireValue(argument, args, index))
                        index += 2
                    }
                    argument.startsWith("--stderr-max-bytes=") -> {
                        stderrMaxBytes = parsePositiveLong(argument, argument.substringAfter('='))
                        index += 1
                    }
                    argument == "--max-queued-frames" -> {
                        maxQueuedFrames = parsePositiveInt(argument, requireValue(argument, args, index))
                        index += 2
                    }
                    argument.startsWith("--max-queued-frames=") -> {
                        maxQueuedFrames = parsePositiveInt(argument, argument.substringAfter('='))
                        index += 1
                    }
                    argument == "--housekeeping-interval-ms" -> {
                        housekeepingIntervalMs = parsePositiveLong(argument, requireValue(argument, args, index))
                        index += 2
                    }
                    argument.startsWith("--housekeeping-interval-ms=") -> {
                        housekeepingIntervalMs = parsePositiveLong(argument, argument.substringAfter('='))
                        index += 1
                    }
                    argument.startsWith("-") -> {
                        throw IllegalArgumentException("unknown option: $argument")
                    }
                    else -> {
                        childCommand.addAll(args.copyOfRange(index, args.size))
                        break
                    }
                }
            }

            if (!showHelp && childCommand.isEmpty()) {
                throw IllegalArgumentException("missing child command after watchdog options")
            }

            val mergedTimeouts = LinkedHashMap<String, Long>()
            mergedTimeouts.putAll(builtInTimeouts)
            mergedTimeouts.putAll(methodTimeouts)

            return Config(
                childCommand = childCommand.toList(),
                cwd = cwd,
                defaultRequestTimeoutMs = defaultRequestTimeoutMs,
                methodTimeouts = mergedTimeouts,
                idleTimeoutMs = idleTimeoutMs,
                restartOnTimeout = restartOnTimeout,
                replayInitialize = replayInitialize,
                maxRestarts = maxRestarts,
                restartWindowMs = restartWindowMs,
                restartBackoffMs = restartBackoffMs,
                killGraceMs = killGraceMs,
                logFile = logFile,
                stderrFile = stderrFile,
                stderrMaxBytes = stderrMaxBytes,
                maxQueuedFrames = maxQueuedFrames,
                housekeepingIntervalMs = housekeepingIntervalMs,
                showHelp = showHelp
            )
        }

        private fun requireValue(option: String, args: Array<String>, index: Int): String {
            if (index + 1 >= args.size) {
                throw IllegalArgumentException("$option requires a value")
            }
            return args[index + 1]
        }

        private fun parseMethodTimeout(raw: String): Pair<String, Long> {
            val separator = raw.lastIndexOf('=')
            if (separator <= 0 || separator == raw.length - 1) {
                throw IllegalArgumentException("--method-timeout expects method=milliseconds")
            }
            val method = raw.substring(0, separator).trim()
            val timeout = parsePositiveLong("--method-timeout", raw.substring(separator + 1).trim())
            if (method.isEmpty()) {
                throw IllegalArgumentException("--method-timeout method name cannot be empty")
            }
            return method to timeout
        }

        private fun parsePositiveLong(name: String, value: String): Long {
            val parsed = value.toLongOrNull()
                ?: throw IllegalArgumentException("$name must be an integer, got: $value")
            if (parsed <= 0L) {
                throw IllegalArgumentException("$name must be greater than zero, got: $value")
            }
            return parsed
        }

        private fun parseNonNegativeLong(name: String, value: String): Long {
            val parsed = value.toLongOrNull()
                ?: throw IllegalArgumentException("$name must be an integer, got: $value")
            if (parsed < 0L) {
                throw IllegalArgumentException("$name must be zero or greater, got: $value")
            }
            return parsed
        }

        private fun parsePositiveInt(name: String, value: String): Int {
            val parsed = value.toIntOrNull()
                ?: throw IllegalArgumentException("$name must be an integer, got: $value")
            if (parsed <= 0) {
                throw IllegalArgumentException("$name must be greater than zero, got: $value")
            }
            return parsed
        }

        private fun parseNonNegativeInt(name: String, value: String): Int {
            val parsed = value.toIntOrNull()
                ?: throw IllegalArgumentException("$name must be an integer, got: $value")
            if (parsed < 0) {
                throw IllegalArgumentException("$name must be zero or greater, got: $value")
            }
            return parsed
        }

        fun usage(): String = """
            |$WATCHDOG_NAME
            |
            |Supervise a stdio MCP server, enforce request deadlines, restart hung or crashed
            |children, replay the initialize handshake after restart, and emit structured
            |watchdog logs without modifying the client.
            |
            |Usage:
            |  $WATCHDOG_NAME [options] -- child-command [args...]
            |  $WATCHDOG_NAME [options] child-command [args...]
            |
            |Options:
            |  --cwd PATH                          Working directory for the child process.
            |  --default-request-timeout-ms N      Default deadline for request/response pairs.
            |  --method-timeout name=N             Override timeout for one method. Repeatable.
            |  --idle-timeout-ms N                 Restart idle child after N ms with no traffic. 0 disables.
            |  --restart-on-timeout                Restart child after a timed-out request. Default.
            |  --no-restart-on-timeout             Do not restart child after a timeout.
            |  --disable-replay-initialize         Require the client to reinitialize after restart.
            |  --max-restarts N                    Maximum restarts inside --restart-window-ms.
            |  --restart-window-ms N               Rolling window for restart budgeting.
            |  --restart-backoff-ms N              Sleep between restart attempts.
            |  --kill-grace-ms N                   Wait before forcing child termination.
            |  --log-file PATH                     Write watchdog JSONL events to PATH.
            |  --stderr-file PATH                  Write child stderr to PATH instead of stderr.
            |  --stderr-max-bytes N                Cap stderr file size before truncating.
            |  --max-queued-frames N               Max frames queued while replaying initialize.
            |  --housekeeping-interval-ms N        Timeout and health-check cadence.
            |  --help, -h                          Show this help.
            |
            |Examples:
            |  $WATCHDOG_NAME --log-file watchdog.jsonl -- npx @modelcontextprotocol/server-filesystem /srv/data
            |  $WATCHDOG_NAME --method-timeout tools/call=300000 -- uvx some-mcp-server --transport stdio
            |""".trimMargin() + "\n"
    }
}

private data class JsonRpcFrameMeta(
    val idToken: String?,
    val method: String?,
    val cancelIdToken: String?,
    val hasResult: Boolean,
    val hasError: Boolean
) {
    val isRequest: Boolean
        get() = method != null && idToken != null

    val isNotification: Boolean
        get() = method != null && idToken == null

    val isResponse: Boolean
        get() = method == null && idToken != null && (hasResult || hasError)
}

private data class ClientFrame(
    val payload: ByteArray,
    val json: String,
    val meta: JsonRpcFrameMeta,
    val enqueuedAtMs: Long
)

private data class PendingRequest(
    val idToken: String,
    val method: String,
    val startedAtMs: Long,
    val deadlineAtMs: Long,
    val generation: Long,
    var cancelled: Boolean = false
)

private class McpStdioWatchdog(private val config: Config) : Closeable {
    private val redactor = Redactor()
    private val logger = JsonlLogger(config.logFile, redactor)
    private val stderrSink = StderrSink(config.stderrFile, config.stderrMaxBytes, redactor)
    private val stateLock = Any()
    private val childWriteLock = Any()
    private val clientWriteLock = Any()
    private val scheduler = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory("mcp-watchdog-housekeeping"))

    @Volatile
    private var stopRequested = false

    @Volatile
    private var exitCode = 0

    private var childProcess: Process? = null
    private var childInput: OutputStream? = null
    private var generation: Long = 0
    private var restartInProgress = false
    private var replayingInitialize = false
    private var requiresClientInitialize = false
    private var sessionReady = false
    private var replayRequestIdToken: String? = null
    private var cachedInitializeRequest: ClientFrame? = null
    private var cachedInitializedNotification: ClientFrame? = null
    private var lastActivityAtMs = nowMs()
    private var lastChildOutputAtMs = nowMs()
    private var restartTimestamps = ArrayDeque<Long>()
    private val queuedFrames = ArrayDeque<ClientFrame>()
    private val pending = LinkedHashMap<String, PendingRequest>()
    private val timedOutResponsesToDrop = linkedSetOf<String>()

    fun run(): Int {
        if (config.childCommand.isEmpty()) {
            System.err.println("$WATCHDOG_NAME: missing child command")
            return 64
        }

        if (!startChild("initial-start", initial = true)) {
            return exitCode
        }

        scheduler.scheduleWithFixedDelay(
            { safeHousekeeping() },
            config.housekeepingIntervalMs,
            config.housekeepingIntervalMs,
            TimeUnit.MILLISECONDS
        )

        thread(name = "mcp-watchdog-client-reader", isDaemon = true) {
            readClientLoop()
        }

        while (!stopRequested) {
            Thread.sleep(100)
        }

        shutdown("main-exit")
        return exitCode
    }

    override fun close() {
        close("close")
    }

    fun close(reason: String) {
        requestStop(0, reason)
    }

    private fun readClientLoop() {
        val input = BufferedInputStream(System.`in`)
        try {
            while (!stopRequested) {
                val payload = ContentLengthFramer.readMessage(input) ?: break
                handleClientPayload(payload)
            }
            requestStop(0, "client-stdin-closed")
        } catch (error: Throwable) {
            logger.log("client_read_error", mapOf("message" to (error.message ?: error.javaClass.name)))
            requestStop(70, "client-read-error")
        }
    }

    private fun handleClientPayload(payload: ByteArray) {
        val json = decodeUtf8(payload)
        val meta = JsonRpcInspector.inspect(json)
        val now = nowMs()

        var rejectError: String? = null
        var rejectCode: Int? = null
        var queueInstead = false
        var writeGeneration = 0L
        var writePayload: ByteArray? = null
        var pendingQueueDelayMs: Long? = null
        var markInitializeAttempt = false

        synchronized(stateLock) {
            if (stopRequested) {
                return
            }

            lastActivityAtMs = now

            if (meta.method == "initialize" && meta.idToken != null) {
                cachedInitializeRequest = ClientFrame(payload, json, meta, now)
                requiresClientInitialize = false
                markInitializeAttempt = true
            } else if (meta.method == "notifications/initialized" && meta.idToken == null) {
                cachedInitializedNotification = ClientFrame(payload, json, meta, now)
            }

            if (meta.method == "$/cancelRequest" && meta.cancelIdToken != null) {
                pending[meta.cancelIdToken]?.cancelled = true
                val iterator = queuedFrames.iterator()
                while (iterator.hasNext()) {
                    val queued = iterator.next()
                    if (queued.meta.idToken != null && queued.meta.idToken == meta.cancelIdToken) {
                        iterator.remove()
                    }
                }
            }

            if (replayingInitialize || restartInProgress) {
                if (queuedFrames.size >= config.maxQueuedFrames) {
                    if (meta.isRequest) {
                        rejectCode = QUEUE_FULL_ERROR_CODE
                        rejectError = "watchdog recovery queue is full"
                    } else {
                        logger.log(
                            "queue_drop_notification",
                            mapOf("method" to meta.method, "queue_depth" to queuedFrames.size)
                        )
                        return
                    }
                } else {
                    queuedFrames.addLast(ClientFrame(payload, json, meta, now))
                    queueInstead = true
                }
            } else if (requiresClientInitialize && meta.method != "initialize" && meta.method != "notifications/initialized") {
                if (meta.isRequest) {
                    rejectCode = UNAVAILABLE_ERROR_CODE
                    rejectError = "child restarted; the client must send initialize again"
                } else {
                    logger.log("drop_before_reinitialize", mapOf("method" to meta.method))
                    return
                }
            } else if (meta.isRequest && meta.idToken != null) {
                val existing = pending[meta.idToken]
                if (existing != null) {
                    rejectCode = DUPLICATE_ID_ERROR_CODE
                    rejectError = "duplicate JSON-RPC id is already in flight"
                } else {
                    val timeoutMs = timeoutFor(meta.method)
                    pending[meta.idToken] = PendingRequest(
                        idToken = meta.idToken,
                        method = meta.method ?: "<unknown>",
                        startedAtMs = now,
                        deadlineAtMs = now + timeoutMs,
                        generation = generation
                    )
                    writeGeneration = generation
                    writePayload = payload
                }
            } else {
                writeGeneration = generation
                writePayload = payload
            }
        }

        if (queueInstead) {
            logger.log(
                "queue_frame",
                mapOf("method" to meta.method, "id" to meta.idToken, "queue_depth" to queuedFrames.size)
            )
            return
        }

        if (rejectCode != null && rejectError != null && meta.idToken != null) {
            sendSyntheticError(
                idToken = meta.idToken,
                code = rejectCode,
                message = rejectError,
                data = mapOf("method" to meta.method)
            )
            logger.log("reject_request", mapOf("method" to meta.method, "id" to meta.idToken, "code" to rejectCode))
            return
        }

        if (writePayload != null) {
            if (sendToChild(writeGeneration, writePayload!!)) {
                pendingQueueDelayMs = if (markInitializeAttempt) 0L else null
            } else if (meta.idToken != null) {
                synchronized(stateLock) {
                    pending.remove(meta.idToken)
                }
                sendSyntheticError(
                    idToken = meta.idToken,
                    code = UNAVAILABLE_ERROR_CODE,
                    message = "child process is unavailable",
                    data = mapOf("method" to meta.method)
                )
            }
        }

        if (pendingQueueDelayMs != null) {
            logger.log("forward_initialize", mapOf("generation" to writeGeneration))
        }
    }

    private fun handleServerPayload(frameGeneration: Long, payload: ByteArray) {
        val json = decodeUtf8(payload)
        val meta = JsonRpcInspector.inspect(json)
        val now = nowMs()

        var forwardToClient = false
        var forwardPayload: ByteArray? = null
        var flushQueued = false
        var notificationAfterReplay: ClientFrame? = null
        var completedRequest: PendingRequest? = null
        var replayFailed = false

        synchronized(stateLock) {
            if (frameGeneration != generation || stopRequested) {
                return
            }

            lastActivityAtMs = now
            lastChildOutputAtMs = now

            if (meta.idToken != null && timedOutResponsesToDrop.remove(meta.idToken)) {
                logger.log("drop_late_response", mapOf("id" to meta.idToken))
                return
            }

            if (replayingInitialize && replayRequestIdToken != null && replayRequestIdToken == meta.idToken) {
                if (meta.hasError) {
                    replayFailed = true
                } else {
                    replayingInitialize = false
                    replayRequestIdToken = null
                    sessionReady = true
                    requiresClientInitialize = false
                    notificationAfterReplay = cachedInitializedNotification
                    flushQueued = true
                }
                logger.log("replay_response", mapOf("success" to (!meta.hasError)))
                return
            }

            if (meta.isResponse && meta.idToken != null) {
                completedRequest = pending.remove(meta.idToken)
                if (completedRequest != null && completedRequest!!.method == "initialize" && !meta.hasError) {
                    sessionReady = true
                    requiresClientInitialize = false
                }
            }

            if (replayingInitialize) {
                logger.log("drop_during_replay", mapOf("method" to meta.method, "id" to meta.idToken))
                return
            }

            forwardToClient = true
            forwardPayload = payload
        }

        if (replayFailed) {
            triggerRestart("initialize-replay-failed")
            return
        }

        if (completedRequest != null) {
            logger.log(
                "response_complete",
                mapOf(
                    "method" to completedRequest!!.method,
                    "id" to completedRequest!!.idToken,
                    "duration_ms" to (now - completedRequest!!.startedAtMs)
                )
            )
        }

        if (notificationAfterReplay != null) {
            if (!sendToChild(frameGeneration, notificationAfterReplay!!.payload)) {
                triggerRestart("initialized-notification-replay-failed")
                return
            }
        }

        if (flushQueued) {
            flushQueuedFrames()
        }

        if (forwardToClient && forwardPayload != null) {
            sendToClient(forwardPayload!!)
        }
    }

    private fun flushQueuedFrames() {
        val drained = mutableListOf<ClientFrame>()
        synchronized(stateLock) {
            while (queuedFrames.isNotEmpty()) {
                drained += queuedFrames.removeFirst()
            }
        }

        if (drained.isEmpty()) {
            return
        }

        logger.log("flush_queue", mapOf("count" to drained.size))
        for (frame in drained) {
            val now = nowMs()
            var rejectCode: Int? = null
            var rejectMessage: String? = null
            var generationToWrite = 0L
            synchronized(stateLock) {
                if (stopRequested) {
                    return
                }
                if (requiresClientInitialize && frame.meta.method != "initialize" && frame.meta.method != "notifications/initialized") {
                    if (frame.meta.isRequest) {
                        rejectCode = UNAVAILABLE_ERROR_CODE
                        rejectMessage = "child restarted; the client must send initialize again"
                    } else {
                        logger.log("drop_queued_before_reinitialize", mapOf("method" to frame.meta.method))
                    }
                } else if (frame.meta.isRequest && frame.meta.idToken != null) {
                    val timeoutMs = timeoutFor(frame.meta.method)
                    pending[frame.meta.idToken] = PendingRequest(
                        idToken = frame.meta.idToken,
                        method = frame.meta.method ?: "<unknown>",
                        startedAtMs = now,
                        deadlineAtMs = now + timeoutMs,
                        generation = generation
                    )
                    generationToWrite = generation
                } else {
                    generationToWrite = generation
                }
            }

            if (rejectCode != null && rejectMessage != null && frame.meta.idToken != null) {
                sendSyntheticError(
                    idToken = frame.meta.idToken,
                    code = rejectCode!!,
                    message = rejectMessage!!,
                    data = mapOf("method" to frame.meta.method)
                )
                continue
            }

            if (!sendToChild(generationToWrite, frame.payload) && frame.meta.idToken != null) {
                synchronized(stateLock) {
                    pending.remove(frame.meta.idToken)
                }
                sendSyntheticError(
                    idToken = frame.meta.idToken,
                    code = UNAVAILABLE_ERROR_CODE,
                    message = "child process is unavailable",
                    data = mapOf("method" to frame.meta.method)
                )
            }
        }
    }

    private fun sendToChild(targetGeneration: Long, payload: ByteArray): Boolean {
        val output = synchronized(stateLock) {
            if (targetGeneration != generation || stopRequested) {
                null
            } else {
                childInput
            }
        } ?: return false

        return try {
            synchronized(childWriteLock) {
                ContentLengthFramer.writeMessage(output, payload)
            }
            true
        } catch (error: IOException) {
            logger.log(
                "child_write_error",
                mapOf("generation" to targetGeneration, "message" to (error.message ?: error.javaClass.name))
            )
            triggerRestart("child-write-failure")
            false
        }
    }

    private fun sendToClient(payload: ByteArray) {
        try {
            synchronized(clientWriteLock) {
                ContentLengthFramer.writeMessage(System.out, payload)
            }
        } catch (error: IOException) {
            logger.log("client_write_error", mapOf("message" to (error.message ?: error.javaClass.name)))
            requestStop(70, "client-write-error")
        }
    }

    private fun sendSyntheticError(idToken: String, code: Int, message: String, data: Map<String, Any?>) {
        val json = buildSyntheticError(idToken, code, message, data)
        sendToClient(json.toByteArray(StandardCharsets.UTF_8))
    }

    private fun buildSyntheticError(idToken: String, code: Int, message: String, data: Map<String, Any?>): String {
        val builder = StringBuilder(256)
        builder.append('{')
        appendJsonField(builder, "jsonrpc", "2.0", true)
        builder.append(",\"id\":").append(idToken)
        builder.append(",\"error\":{")
        appendJsonField(builder, "code", code, true)
        appendJsonField(builder, "message", message, false)
        if (data.isNotEmpty()) {
            builder.append(",\"data\":")
            appendJsonValue(builder, data)
        }
        builder.append("}}")
        return builder.toString()
    }

    private fun timeoutFor(method: String?): Long {
        if (method == null) {
            return config.defaultRequestTimeoutMs
        }
        return config.methodTimeouts[method] ?: config.defaultRequestTimeoutMs
    }

    private fun safeHousekeeping() {
        try {
            housekeeping()
        } catch (error: Throwable) {
            logger.log("housekeeping_error", mapOf("message" to (error.message ?: error.javaClass.name)))
            requestStop(70, "housekeeping-error")
        }
    }

    private fun housekeeping() {
        val now = nowMs()
        val expired = mutableListOf<PendingRequest>()
        var idleRestart = false

        synchronized(stateLock) {
            if (stopRequested) {
                return
            }

            val iterator = pending.entries.iterator()
            while (iterator.hasNext()) {
                val (_, pendingRequest) = iterator.next()
                if (!pendingRequest.cancelled && pendingRequest.deadlineAtMs <= now) {
                    iterator.remove()
                    timedOutResponsesToDrop += pendingRequest.idToken
                    expired += pendingRequest
                }
            }

            if (!restartInProgress &&
                !replayingInitialize &&
                config.idleTimeoutMs > 0L &&
                pending.isEmpty() &&
                now - lastActivityAtMs >= config.idleTimeoutMs
            ) {
                idleRestart = true
            }
        }

        for (request in expired) {
            sendSyntheticError(
                idToken = request.idToken,
                code = TIMEOUT_ERROR_CODE,
                message = "MCP request timed out",
                data = mapOf(
                    "method" to request.method,
                    "timeout_ms" to (request.deadlineAtMs - request.startedAtMs),
                    "generation" to request.generation
                )
            )
            logger.log(
                "request_timeout",
                mapOf("method" to request.method, "id" to request.idToken, "generation" to request.generation)
            )
        }

        if (expired.isNotEmpty() && config.restartOnTimeout) {
            triggerRestart("request-timeout")
            return
        }

        if (idleRestart) {
            triggerRestart("idle-timeout")
        }
    }

    private fun startChild(reason: String, initial: Boolean): Boolean {
        val builder = ProcessBuilder(config.childCommand)
        if (config.cwd != null) {
            builder.directory(File(config.cwd))
        }

        val process = try {
            builder.start()
        } catch (error: IOException) {
            logger.log("child_start_failed", mapOf("message" to (error.message ?: error.javaClass.name)))
            requestStop(70, "child-start-failed")
            return false
        }

        val currentGeneration: Long
        synchronized(stateLock) {
            generation += 1
            currentGeneration = generation
            childProcess = process
            childInput = BufferedOutputStream(process.outputStream)
            lastActivityAtMs = nowMs()
            lastChildOutputAtMs = nowMs()
            restartInProgress = false

            if (!initial) {
                sessionReady = false
                if (config.replayInitialize && cachedInitializeRequest?.meta?.idToken != null) {
                    replayingInitialize = true
                    requiresClientInitialize = false
                    replayRequestIdToken = cachedInitializeRequest!!.meta.idToken
                } else {
                    replayingInitialize = false
                    replayRequestIdToken = null
                    requiresClientInitialize = cachedInitializeRequest != null
                }
            } else {
                replayingInitialize = false
                replayRequestIdToken = null
                requiresClientInitialize = false
            }
        }

        logger.log(
            "child_started",
            mapOf(
                "pid" to process.pid(),
                "generation" to currentGeneration,
                "reason" to reason,
                "command" to config.childCommand.joinToString(" ")
            )
        )

        thread(name = "mcp-watchdog-child-stdout-$currentGeneration", isDaemon = true) {
            readChildStdoutLoop(currentGeneration, BufferedInputStream(process.inputStream))
        }
        thread(name = "mcp-watchdog-child-stderr-$currentGeneration", isDaemon = true) {
            readChildStderrLoop(currentGeneration, BufferedInputStream(process.errorStream))
        }
        thread(name = "mcp-watchdog-child-exit-$currentGeneration", isDaemon = true) {
            waitForChildExit(currentGeneration, process)
        }

        if (!initial && replayingInitialize) {
            val replayFrame = cachedInitializeRequest
            if (replayFrame != null) {
                val wrote = sendToChild(currentGeneration, replayFrame.payload)
                if (!wrote) {
                    triggerRestart("initialize-replay-send-failed")
                    return false
                }
                logger.log("replay_initialize", mapOf("generation" to currentGeneration))
            }
        }

        return true
    }

    private fun waitForChildExit(childGeneration: Long, process: Process) {
        val code = try {
            process.waitFor()
        } catch (_: InterruptedException) {
            return
        }

        synchronized(stateLock) {
            if (stopRequested || childGeneration != generation || restartInProgress) {
                return
            }
        }

        logger.log("child_exited", mapOf("generation" to childGeneration, "exit_code" to code))
        triggerRestart("child-exited")
    }

    private fun readChildStdoutLoop(childGeneration: Long, input: BufferedInputStream) {
        try {
            while (!stopRequested) {
                val payload = ContentLengthFramer.readMessage(input) ?: break
                handleServerPayload(childGeneration, payload)
            }
            synchronized(stateLock) {
                if (stopRequested || childGeneration != generation || restartInProgress) {
                    return
                }
            }
            logger.log("child_stdout_closed", mapOf("generation" to childGeneration))
            triggerRestart("child-stdout-closed")
        } catch (error: Throwable) {
            logger.log(
                "child_stdout_error",
                mapOf("generation" to childGeneration, "message" to (error.message ?: error.javaClass.name))
            )
            triggerRestart("child-stdout-error")
        }
    }

    private fun readChildStderrLoop(childGeneration: Long, input: InputStream) {
        val lineBuffer = ByteArrayOutputStream()
        try {
            while (!stopRequested) {
                val byteValue = input.read()
                if (byteValue == -1) {
                    flushStderrLine(childGeneration, lineBuffer)
                    break
                }
                if (byteValue == '\n'.code) {
                    flushStderrLine(childGeneration, lineBuffer)
                } else if (byteValue != '\r'.code) {
                    lineBuffer.write(byteValue)
                }
            }
        } catch (error: IOException) {
            logger.log(
                "child_stderr_error",
                mapOf("generation" to childGeneration, "message" to (error.message ?: error.javaClass.name))
            )
        }
    }

    private fun flushStderrLine(childGeneration: Long, lineBuffer: ByteArrayOutputStream) {
        if (lineBuffer.size() == 0) {
            return
        }
        val line = decodeUtf8(lineBuffer.toByteArray())
        lineBuffer.reset()
        stderrSink.writeLine("[generation=$childGeneration] $line")
    }

    private fun triggerRestart(reason: String) {
        val shouldRestart: Boolean
        synchronized(stateLock) {
            if (stopRequested || restartInProgress) {
                return
            }
            restartInProgress = true
            shouldRestart = registerRestartAttempt(nowMs())
        }

        if (!shouldRestart) {
            failQueuedAndPending("restart budget exhausted: $reason")
            requestStop(70, "restart-budget-exhausted")
            return
        }

        val pendingToFail = synchronized(stateLock) {
            val values = pending.values.toList()
            pending.clear()
            values
        }

        for (item in pendingToFail) {
            sendSyntheticError(
                idToken = item.idToken,
                code = UNAVAILABLE_ERROR_CODE,
                message = "child process restarted before responding",
                data = mapOf("method" to item.method, "reason" to reason)
            )
        }

        logger.log("restart_begin", mapOf("reason" to reason))

        val processToStop = synchronized(stateLock) { childProcess }
        stopChildProcess(processToStop)

        if (config.restartBackoffMs > 0L) {
            Thread.sleep(config.restartBackoffMs)
        }

        if (!startChild(reason, initial = false)) {
            return
        }
    }

    private fun registerRestartAttempt(now: Long): Boolean {
        while (restartTimestamps.isNotEmpty() && now - restartTimestamps.first() > config.restartWindowMs) {
            restartTimestamps.removeFirst()
        }
        if (restartTimestamps.size >= config.maxRestarts) {
            return false
        }
        restartTimestamps.addLast(now)
        return true
    }

    private fun failQueuedAndPending(reason: String) {
        val queued: List<ClientFrame>
        val pendingRequests: List<PendingRequest>
        synchronized(stateLock) {
            queued = queuedFrames.toList()
            queuedFrames.clear()
            pendingRequests = pending.values.toList()
            pending.clear()
        }

        for (frame in queued) {
            if (frame.meta.idToken != null) {
                sendSyntheticError(
                    idToken = frame.meta.idToken,
                    code = UNAVAILABLE_ERROR_CODE,
                    message = "watchdog stopped while the request was queued",
                    data = mapOf("reason" to reason, "method" to frame.meta.method)
                )
            }
        }

        for (request in pendingRequests) {
            sendSyntheticError(
                idToken = request.idToken,
                code = UNAVAILABLE_ERROR_CODE,
                message = "watchdog stopped before the child could respond",
                data = mapOf("reason" to reason, "method" to request.method)
            )
        }
    }

    private fun requestStop(code: Int, reason: String) {
        val shouldStop: Boolean
        synchronized(stateLock) {
            if (stopRequested) {
                return
            }
            stopRequested = true
            exitCode = code
            shouldStop = true
        }
        if (shouldStop) {
            logger.log("watchdog_stop", mapOf("reason" to reason, "exit_code" to code))
            failQueuedAndPending(reason)
            shutdown(reason)
        }
    }

    private fun shutdown(reason: String) {
        scheduler.shutdownNow()
        stopChildProcess(synchronized(stateLock) { childProcess })
        stderrSink.close()
        logger.close()
    }

    private fun stopChildProcess(process: Process?) {
        if (process == null) {
            return
        }
        try {
            process.outputStream.close()
        } catch (_: IOException) {
        }
        process.destroy()
        try {
            if (!process.waitFor(config.killGraceMs, TimeUnit.MILLISECONDS)) {
                process.destroyForcibly()
                process.waitFor(config.killGraceMs, TimeUnit.MILLISECONDS)
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }
}

private object ContentLengthFramer {
    fun readMessage(input: InputStream): ByteArray? {
        var contentLength: Int? = null
        while (true) {
            val line = readAsciiLine(input) ?: return if (contentLength == null) null else throw EOFException("unexpected EOF in headers")
            if (line.isEmpty()) {
                break
            }
            val separator = line.indexOf(':')
            if (separator <= 0) {
                throw IOException("invalid frame header: $line")
            }
            val name = line.substring(0, separator).trim().lowercase()
            val value = line.substring(separator + 1).trim()
            if (name == "content-length") {
                contentLength = value.toIntOrNull() ?: throw IOException("invalid Content-Length: $value")
            }
        }

        val length = contentLength ?: throw IOException("missing Content-Length header")
        val payload = ByteArray(length)
        var offset = 0
        while (offset < length) {
            val read = input.read(payload, offset, length - offset)
            if (read == -1) {
                throw EOFException("unexpected EOF while reading message body")
            }
            offset += read
        }
        return payload
    }

    fun writeMessage(output: OutputStream, payload: ByteArray) {
        val header = "Content-Length: ${payload.size}\r\n\r\n".toByteArray(StandardCharsets.US_ASCII)
        output.write(header)
        output.write(payload)
        output.flush()
    }

    private fun readAsciiLine(input: InputStream): String? {
        val buffer = ByteArrayOutputStream(64)
        while (true) {
            val value = input.read()
            if (value == -1) {
                return if (buffer.size() == 0) null else throw EOFException("unexpected EOF while reading line")
            }
            if (value == '\n'.code) {
                return buffer.toString(StandardCharsets.US_ASCII.name())
            }
            if (value != '\r'.code) {
                buffer.write(value)
            }
        }
    }
}

private object JsonRpcInspector {
    fun inspect(json: String): JsonRpcFrameMeta {
        val members = extractTopLevelMembers(json)
        val idToken = members["id"]?.trim()
        val method = members["method"]?.let { decodeJsonStringToken(it) }
        val cancelIdToken = if (method == "$/cancelRequest") {
            members["params"]?.let { extractTopLevelMembers(it)["id"]?.trim() }
        } else {
            null
        }
        return JsonRpcFrameMeta(
            idToken = idToken,
            method = method,
            cancelIdToken = cancelIdToken,
            hasResult = members.containsKey("result"),
            hasError = members.containsKey("error")
        )
    }

    private fun extractTopLevelMembers(json: String): Map<String, String> {
        val members = LinkedHashMap<String, String>()
        var index = skipWhitespace(json, 0)
        if (index >= json.length || json[index] != '{') {
            return members
        }
        index += 1

        while (true) {
            index = skipWhitespace(json, index)
            if (index >= json.length) {
                return members
            }
            if (json[index] == '}') {
                return members
            }
            if (json[index] != '"') {
                return members
            }

            val keyToken = readJsonString(json, index)
            index = skipWhitespace(json, keyToken.nextIndex)
            if (index >= json.length || json[index] != ':') {
                return members
            }
            index += 1
            index = skipWhitespace(json, index)
            if (index >= json.length) {
                return members
            }

            val valueStart = index
            val valueEnd = skipJsonValue(json, index)
            members[keyToken.value] = json.substring(valueStart, valueEnd)
            index = skipWhitespace(json, valueEnd)
            if (index >= json.length) {
                return members
            }
            if (json[index] == ',') {
                index += 1
                continue
            }
            if (json[index] == '}') {
                return members
            }
            return members
        }
    }

    private fun skipWhitespace(json: String, start: Int): Int {
        var index = start
        while (index < json.length && json[index].isWhitespace()) {
            index += 1
        }
        return index
    }

    private fun skipJsonValue(json: String, start: Int): Int {
        return when (val current = json[start]) {
            '"' -> skipJsonString(json, start)
            '{', '[' -> skipComposite(json, start)
            't' -> expectLiteral(json, start, "true")
            'f' -> expectLiteral(json, start, "false")
            'n' -> expectLiteral(json, start, "null")
            else -> if (current == '-' || current.isDigit()) {
                skipNumber(json, start)
            } else {
                throw IllegalArgumentException("unexpected JSON token at index $start")
            }
        }
    }

    private fun skipComposite(json: String, start: Int): Int {
        val stack = ArrayDeque<Char>()
        stack.addLast(if (json[start] == '{') '}' else ']')
        var index = start + 1
        while (index < json.length) {
            when (val current = json[index]) {
                '"' -> index = skipJsonString(json, index)
                '{' -> {
                    stack.addLast('}')
                    index += 1
                }
                '[' -> {
                    stack.addLast(']')
                    index += 1
                }
                '}', ']' -> {
                    if (stack.isEmpty()) {
                        throw IllegalArgumentException("unexpected closing bracket at index $index")
                    }
                    val expected = stack.removeLast()
                    if (current != expected) {
                        throw IllegalArgumentException("mismatched closing bracket at index $index")
                    }
                    index += 1
                    if (stack.isEmpty()) {
                        return index
                    }
                }
                else -> index += 1
            }
        }
        throw IllegalArgumentException("unterminated JSON composite value")
    }

    private fun skipJsonString(json: String, start: Int): Int {
        var index = start + 1
        while (index < json.length) {
            when (json[index]) {
                '\\' -> index += 2
                '"' -> return index + 1
                else -> index += 1
            }
        }
        throw IllegalArgumentException("unterminated JSON string")
    }

    private fun skipNumber(json: String, start: Int): Int {
        var index = start
        while (index < json.length) {
            val current = json[index]
            if (current.isDigit() || current == '-' || current == '+' || current == '.' || current == 'e' || current == 'E') {
                index += 1
            } else {
                break
            }
        }
        return index
    }

    private fun expectLiteral(json: String, start: Int, literal: String): Int {
        if (!json.regionMatches(start, literal, 0, literal.length)) {
            throw IllegalArgumentException("expected literal $literal at index $start")
        }
        return start + literal.length
    }

    private fun readJsonString(json: String, start: Int): ParsedString {
        if (json[start] != '"') {
            throw IllegalArgumentException("expected string at index $start")
        }
        val builder = StringBuilder()
        var index = start + 1
        while (index < json.length) {
            val current = json[index]
            when (current) {
                '\\' -> {
                    if (index + 1 >= json.length) {
                        throw IllegalArgumentException("unterminated escape sequence")
                    }
                    when (val escape = json[index + 1]) {
                        '"', '\\', '/' -> builder.append(escape)
                        'b' -> builder.append('\b')
                        'f' -> builder.append('\u000c')
                        'n' -> builder.append('\n')
                        'r' -> builder.append('\r')
                        't' -> builder.append('\t')
                        'u' -> {
                            if (index + 5 >= json.length) {
                                throw IllegalArgumentException("invalid unicode escape")
                            }
                            val hex = json.substring(index + 2, index + 6)
                            builder.append(hex.toInt(16).toChar())
                            index += 4
                        }
                        else -> throw IllegalArgumentException("unsupported escape: \\$escape")
                    }
                    index += 2
                }
                '"' -> return ParsedString(builder.toString(), index + 1)
                else -> {
                    builder.append(current)
                    index += 1
                }
            }
        }
        throw IllegalArgumentException("unterminated string token")
    }

    private fun decodeJsonStringToken(rawToken: String): String {
        val trimmed = rawToken.trim()
        if (trimmed.isEmpty() || trimmed[0] != '"') {
            return trimmed
        }
        return readJsonString(trimmed, 0).value
    }

    private data class ParsedString(val value: String, val nextIndex: Int)
}

private class Redactor {
    private val patterns = listOf(
        Regex("""sk-[A-Za-z0-9]{20,}""") to "<redacted:openai-key>",
        Regex("""github_pat_[A-Za-z0-9_]{20,}""") to "<redacted:github-pat>",
        Regex("""gh[pousr]_[A-Za-z0-9]{20,}""") to "<redacted:github-token>",
        Regex("""AIza[0-9A-Za-z_-]{35}""") to "<redacted:google-api-key>",
        Regex("""Bearer\s+[A-Za-z0-9._=-]{20,}""", RegexOption.IGNORE_CASE) to "<redacted:bearer-token>",
        Regex("""[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+""") to "<redacted:jwt>"
    )

    fun redact(input: String): String {
        var output = input
        for ((pattern, replacement) in patterns) {
            output = pattern.replace(output, replacement)
        }
        return output
    }
}

private class JsonlLogger(pathString: String?, private val redactor: Redactor) : Closeable {
    private val writer: BufferedWriter? = pathString?.let {
        val path = Paths.get(it)
        path.parent?.let(Files::createDirectories)
        Files.newBufferedWriter(
            path,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
        )
    }

    @Synchronized
    fun log(event: String, fields: Map<String, Any?> = emptyMap()) {
        val target = writer ?: return
        val builder = StringBuilder(256)
        builder.append('{')
        appendJsonField(builder, "timestamp", Instant.now().toString(), true)
        appendJsonField(builder, "event", event, false)
        for ((key, value) in fields) {
            appendJsonField(builder, key, value, false)
        }
        builder.append('}')
        target.write(builder.toString())
        target.newLine()
        target.flush()
    }

    override fun close() {
        writer?.close()
    }

    private fun appendJsonField(builder: StringBuilder, key: String, value: Any?, first: Boolean) {
        if (!first) {
            builder.append(',')
        }
        builder.append('"').append(escapeJson(key)).append('"').append(':')
        when (value) {
            null -> builder.append("null")
            is String -> builder.append('"').append(escapeJson(redactor.redact(value))).append('"')
            is Number, is Boolean -> builder.append(value.toString())
            is Map<*, *> -> appendJsonValue(builder, value)
            is Iterable<*> -> appendJsonValue(builder, value)
            else -> builder.append('"').append(escapeJson(redactor.redact(value.toString()))).append('"')
        }
    }
}

private class StderrSink(pathString: String?, private val maxBytes: Long, private val redactor: Redactor) : Closeable {
    private val writer: BufferedWriter? = pathString?.let {
        val path = Paths.get(it)
        path.parent?.let(Files::createDirectories)
        Files.newBufferedWriter(
            path,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
        )
    }
    private var bytesWritten = 0L
    private var truncated = false

    @Synchronized
    fun writeLine(line: String) {
        val safeLine = redactor.redact(line)
        if (writer == null) {
            System.err.println(safeLine)
            return
        }
        if (truncated) {
            return
        }
        val encodedSize = safeLine.toByteArray(StandardCharsets.UTF_8).size + 1L
        if (bytesWritten + encodedSize > maxBytes) {
            writer.write("[stderr truncated after $bytesWritten bytes]")
            writer.newLine()
            writer.flush()
            truncated = true
            return
        }
        writer.write(safeLine)
        writer.newLine()
        writer.flush()
        bytesWritten += encodedSize
    }

    override fun close() {
        writer?.close()
    }
}

private class DaemonThreadFactory(private val name: String) : ThreadFactory {
    override fun newThread(runnable: Runnable): Thread {
        return Thread(runnable, name).apply {
            isDaemon = true
        }
    }
}

private fun appendJsonField(builder: StringBuilder, key: String, value: Any?, first: Boolean) {
    if (!first) {
        builder.append(',')
    }
    builder.append('"').append(escapeJson(key)).append('"').append(':')
    appendJsonValue(builder, value)
}

private fun appendJsonValue(builder: StringBuilder, value: Any?) {
    when (value) {
        null -> builder.append("null")
        is String -> builder.append('"').append(escapeJson(value)).append('"')
        is Number, is Boolean -> builder.append(value.toString())
        is Map<*, *> -> {
            builder.append('{')
            var first = true
            for ((key, child) in value) {
                appendJsonField(builder, key.toString(), child, first)
                first = false
            }
            builder.append('}')
        }
        is Iterable<*> -> {
            builder.append('[')
            var first = true
            for (child in value) {
                if (!first) {
                    builder.append(',')
                }
                appendJsonValue(builder, child)
                first = false
            }
            builder.append(']')
        }
        else -> builder.append('"').append(escapeJson(value.toString())).append('"')
    }
}

private fun escapeJson(value: String): String {
    val builder = StringBuilder(value.length + 16)
    for (character in value) {
        when (character) {
            '\\' -> builder.append("\\\\")
            '"' -> builder.append("\\\"")
            '\b' -> builder.append("\\b")
            '\u000c' -> builder.append("\\f")
            '\n' -> builder.append("\\n")
            '\r' -> builder.append("\\r")
            '\t' -> builder.append("\\t")
            else -> {
                if (character < ' ') {
                    builder.append("\\u")
                    builder.append(character.code.toString(16).padStart(4, '0'))
                } else {
                    builder.append(character)
                }
            }
        }
    }
    return builder.toString()
}

private fun decodeUtf8(bytes: ByteArray): String = String(bytes, StandardCharsets.UTF_8)

private fun nowMs(): Long = System.currentTimeMillis()

/*
This solves a real April 2026 pain point around Model Context Protocol infrastructure: local and remote MCP servers
that run over stdio often hang, crash, leak state, or silently stop answering after a bad tool call, and most clients
do not have a clean process supervisor between the editor and the child server. Built because the ugly failure mode is
always the same: the client looks healthy, the MCP child is wedged, requests stack up, and a developer loses time
guessing whether the bug is in the agent, the transport, or the tool server itself. This file gives you a practical
drop-in wrapper that enforces request deadlines, emits JSONL restart and timeout logs, preserves stdio framing, and can
replay the initialize handshake after a crash so the session recovers instead of forcing a full editor restart.

Use it when you run MCP servers with `uvx`, `npx`, Docker sidecars, JVM launchers, Python virtualenv shims, or any
other child process that speaks JSON-RPC over `Content-Length` frames. It is especially useful for AI tooling teams,
IDE extension authors, platform engineers, and agent builders who are debugging flaky `tools/call`, `resources/read`,
or `initialize` flows and need a watchdog that is small, portable, and easy to put in front of an existing server.

The trick: the watchdog does not try to be the server and it does not mutate the client protocol. It simply watches
every request ID, tracks a method-aware deadline budget, returns a synthetic JSON-RPC error when the child misses that
budget, restarts the child inside a bounded restart window, and optionally replays the last initialize sequence so the
recovered child has the same handshake context as the original one. That combination matters because most real-world
MCP outages are not clean process exits. They are partial hangs, deadlocked tool calls, or transport stalls that leave
the parent process alive but unusable.

Drop this into a repository when you want a Kotlin MCP watchdog, JSON-RPC stdio supervisor, MCP server auto-restart
wrapper, MCP initialize replay helper, or request-timeout guard for agent infrastructure. Pavan would explain it this
way: I wrote this because I wanted one file I could compile fast, ship in CI or local tooling, and trust when an MCP
server started behaving badly under real load. It is meant to be practical, readable, and good enough that another
engineer can fork it, wire it into their own MCP stack, and stop losing hours to invisible stdio failures.
*/