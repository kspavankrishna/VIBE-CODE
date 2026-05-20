import java.security.MessageDigest
import java.security.SecureRandom
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.ArrayDeque
import java.util.LinkedHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class AiSseResumeLedger(private val config: Config = Config()) {

    data class Config(
        val maxSessions: Int = 4_096,
        val idleTtl: Duration = Duration.ofMinutes(30),
        val maxRetainedFramesPerStream: Int = 256,
        val maxReplayBytesPerStream: Int = 512 * 1024,
        val maxAssistantTextChars: Int = 128 * 1024,
        val defaultReplayLimit: Int = 128,
        val checkpointEveryChars: Int = 512,
        val checkpointEveryFrames: Int = 24,
        val maxToolStates: Int = 64,
        val clock: Clock = Clock.systemUTC()
    ) {
        init {
            require(maxSessions > 0) { "maxSessions must be positive" }
            require(!idleTtl.isNegative && !idleTtl.isZero) { "idleTtl must be positive" }
            require(maxRetainedFramesPerStream >= 4) { "maxRetainedFramesPerStream must be at least 4" }
            require(maxReplayBytesPerStream >= 8 * 1024) { "maxReplayBytesPerStream must be at least 8192 bytes" }
            require(maxAssistantTextChars >= 512) { "maxAssistantTextChars must be at least 512" }
            require(defaultReplayLimit > 0) { "defaultReplayLimit must be positive" }
            require(checkpointEveryChars > 0) { "checkpointEveryChars must be positive" }
            require(checkpointEveryFrames > 0) { "checkpointEveryFrames must be positive" }
            require(maxToolStates > 0) { "maxToolStates must be positive" }
        }
    }

    data class OpenedStream(
        val streamId: String,
        val assistantMessageId: String,
        val epoch: String,
        val startedFrame: SseFrame
    )

    data class SseFrame(
        val id: String,
        val event: String,
        val data: String,
        val retryMillis: Long? = null
    ) {
        fun encode(): String {
            val builder = StringBuilder(data.length + event.length + id.length + 32)
            builder.append("id: ").append(id).append('\n')
            builder.append("event: ").append(event).append('\n')
            if (retryMillis != null) {
                builder.append("retry: ").append(retryMillis).append('\n')
            }
            if (data.isEmpty()) {
                builder.append("data:\n")
            } else {
                data.lineSequence().forEach { line ->
                    builder.append("data: ").append(line).append('\n')
                }
            }
            builder.append('\n')
            return builder.toString()
        }

        fun encodedBytes(): Int = encode().toByteArray(Charsets.UTF_8).size
    }

    enum class ReplayMode {
        LIVE_DELTA,
        SNAPSHOT,
        GONE
    }

    enum class ToolStatus(val wireValue: String) {
        STARTED("started"),
        PARTIAL("partial"),
        COMPLETED("completed"),
        FAILED("failed")
    }

    data class ReplayBatch(
        val mode: ReplayMode,
        val frames: List<SseFrame>,
        val streamOpen: Boolean,
        val latestEventId: String?,
        val checkpointEventId: String?
    ) {
        fun encode(): String = frames.joinToString(separator = "") { it.encode() }
    }

    data class StreamStats(
        val streamId: String,
        val epoch: String,
        val open: Boolean,
        val retainedFrames: Int,
        val retainedBytes: Int,
        val assistantChars: Int,
        val truncatedPrefixChars: Int,
        val toolStates: Int,
        val latestEventId: String?,
        val checkpointEventId: String?,
        val lastTouchedAt: Instant
    )

    data class LedgerStats(
        val streams: Int,
        val openStreams: Int,
        val retainedFrames: Int,
        val retainedBytes: Long,
        val oldestTouchedAt: Instant?,
        val newestTouchedAt: Instant?,
        val perStream: List<StreamStats>
    )

    private val lock = ReentrantLock()
    private val sessions = LinkedHashMap<String, StreamSession>(16, 0.75f, true)

    fun open(
        streamId: String,
        assistantMessageId: String? = null,
        metadataJson: String? = null
    ): OpenedStream = lock.withLock {
        val normalizedStreamId = requireStreamId(streamId)
        val now = now()
        pruneExpiredLocked(now)

        val existing = sessions[normalizedStreamId]
        if (existing != null && existing.open) {
            throw IllegalStateException("stream '$normalizedStreamId' is already open")
        }

        val session = StreamSession(
            streamId = normalizedStreamId,
            assistantMessageId = assistantMessageId?.let { requireLooseText(it, "assistantMessageId", 256) } ?: "msg_${randomHex(8)}",
            epoch = randomHex(6),
            createdAt = now,
            metadataJson = metadataJson?.takeUnless { it.isBlank() }?.trim()
        )
        sessions[normalizedStreamId] = session
        evictIfNeededLocked()

        val startedFrame = session.appendStartLocked(now)
        OpenedStream(
            streamId = normalizedStreamId,
            assistantMessageId = session.assistantMessageId,
            epoch = session.epoch,
            startedFrame = startedFrame
        )
    }

    fun appendAssistantText(streamId: String, delta: String): SseFrame = lock.withLock {
        val normalizedStreamId = requireStreamId(streamId)
        require(delta.isNotEmpty()) { "delta must not be empty" }
        val session = requireOpenSessionLocked(normalizedStreamId)
        session.appendAssistantDeltaLocked(delta, now())
    }

    fun appendToolCall(
        streamId: String,
        toolCallId: String,
        toolName: String,
        argumentsJson: String,
        status: ToolStatus = ToolStatus.STARTED
    ): SseFrame = lock.withLock {
        val session = requireOpenSessionLocked(requireStreamId(streamId))
        session.appendToolCallLocked(
            toolCallId = requireLooseText(toolCallId, "toolCallId", 256),
            toolName = requireLooseText(toolName, "toolName", 256),
            argumentsJson = requireJsonish(argumentsJson, "argumentsJson"),
            status = status,
            now = now()
        )
    }

    fun appendToolResult(
        streamId: String,
        toolCallId: String,
        resultJson: String,
        toolName: String? = null,
        isError: Boolean = false,
        status: ToolStatus = if (isError) ToolStatus.FAILED else ToolStatus.COMPLETED
    ): SseFrame = lock.withLock {
        val session = requireOpenSessionLocked(requireStreamId(streamId))
        session.appendToolResultLocked(
            toolCallId = requireLooseText(toolCallId, "toolCallId", 256),
            toolName = toolName?.let { requireLooseText(it, "toolName", 256) },
            resultJson = requireJsonish(resultJson, "resultJson"),
            isError = isError,
            status = status,
            now = now()
        )
    }

    fun appendMetadata(streamId: String, metadataJson: String): SseFrame = lock.withLock {
        val session = requireOpenSessionLocked(requireStreamId(streamId))
        session.appendMetadataLocked(
            metadataJson = requireJsonish(metadataJson, "metadataJson"),
            now = now()
        )
    }

    fun complete(
        streamId: String,
        finishReason: String = "stop",
        usageJson: String? = null
    ): SseFrame = lock.withLock {
        val session = requireSessionLocked(requireStreamId(streamId))
        session.completeLocked(
            finishReason = requireLooseText(finishReason, "finishReason", 64),
            usageJson = usageJson?.takeUnless { it.isBlank() }?.let { requireJsonish(it, "usageJson") },
            now = now()
        )
    }

    fun fail(
        streamId: String,
        code: String,
        message: String,
        retriable: Boolean = true
    ): SseFrame = lock.withLock {
        val session = requireSessionLocked(requireStreamId(streamId))
        session.failLocked(
            code = requireLooseText(code, "code", 128),
            message = requireLooseText(message, "message", 4_096),
            retriable = retriable,
            now = now()
        )
    }

    fun resume(
        streamId: String,
        lastEventId: String?,
        limit: Int = config.defaultReplayLimit
    ): ReplayBatch = lock.withLock {
        require(limit > 0) { "limit must be positive" }
        val session = sessions[requireStreamId(streamId)] ?: return ReplayBatch(
            mode = ReplayMode.GONE,
            frames = emptyList(),
            streamOpen = false,
            latestEventId = null,
            checkpointEventId = null
        )
        session.resumeLocked(Cursor.parse(lastEventId), limit.coerceAtMost(config.maxRetainedFramesPerStream), now())
    }

    fun drop(streamId: String): Boolean = lock.withLock {
        sessions.remove(requireStreamId(streamId)) != null
    }

    fun prune() {
        lock.withLock {
            val now = now()
            pruneExpiredLocked(now)
            sessions.values.forEach { it.enforceBudgetsLocked(now) }
            evictIfNeededLocked()
        }
    }

    fun stats(): LedgerStats = lock.withLock {
        val now = now()
        pruneExpiredLocked(now)
        val perStream = ArrayList<StreamStats>(sessions.size)
        var openStreams = 0
        var retainedFrames = 0
        var retainedBytes = 0L
        var oldestTouchedAt: Instant? = null
        var newestTouchedAt: Instant? = null

        sessions.values.forEach { session ->
            val stats = session.statsLocked()
            if (stats.open) {
                openStreams += 1
            }
            retainedFrames += stats.retainedFrames
            retainedBytes += stats.retainedBytes.toLong()
            if (oldestTouchedAt == null || stats.lastTouchedAt.isBefore(oldestTouchedAt)) {
                oldestTouchedAt = stats.lastTouchedAt
            }
            if (newestTouchedAt == null || stats.lastTouchedAt.isAfter(newestTouchedAt)) {
                newestTouchedAt = stats.lastTouchedAt
            }
            perStream.add(stats)
        }

        LedgerStats(
            streams = sessions.size,
            openStreams = openStreams,
            retainedFrames = retainedFrames,
            retainedBytes = retainedBytes,
            oldestTouchedAt = oldestTouchedAt,
            newestTouchedAt = newestTouchedAt,
            perStream = perStream
        )
    }

    private inner class StreamSession(
        val streamId: String,
        val assistantMessageId: String,
        val epoch: String,
        val createdAt: Instant,
        var metadataJson: String?
    ) {
        val frames = ArrayDeque<FrameRecord>()
        val toolStates = LinkedHashMap<String, ToolState>(16, 0.75f, true)
        val dedupeIndex = LinkedHashMap<String, FrameRecord>(32, 0.75f, true)

        val assistantText = StringBuilder()

        var nextSeq = 0L
        var open = true
        var lastTouchedAt = createdAt
        var closedAt: Instant? = null
        var terminalState: TerminalState? = null
        var latestCheckpointSeq: Long? = null
        var latestCheckpointId: String? = null
        var charsSinceCheckpoint = 0
        var framesSinceCheckpoint = 0
        var retainedBytes = 0
        var truncatedPrefixChars = 0

        fun appendStartLocked(now: Instant): SseFrame {
            val payload = buildStartJson(now)
            return appendFrameLocked(
                event = "response.started",
                data = payload,
                now = now,
                checkpoint = false,
                dedupeKey = fingerprint("start", streamId, epoch)
            )
        }

        fun appendAssistantDeltaLocked(delta: String, now: Instant): SseFrame {
            ensureAppendableLocked("assistant text")
            assistantText.append(delta)
            trimAssistantTextLocked()
            charsSinceCheckpoint += delta.length
            val payload = buildAssistantDeltaJson(delta, now)
            val frame = appendFrameLocked(
                event = "assistant.delta",
                data = payload,
                now = now,
                checkpoint = false,
                dedupeKey = null
            )
            maybeCheckpointLocked(now)
            return frame
        }

        fun appendToolCallLocked(
            toolCallId: String,
            toolName: String,
            argumentsJson: String,
            status: ToolStatus,
            now: Instant
        ): SseFrame {
            ensureAppendableLocked("tool call")
            val dedupeKey = fingerprint("tool.call", toolCallId, toolName, status.wireValue, argumentsJson.trim())
            dedupeIndex[dedupeKey]?.let { existing ->
                lastTouchedAt = now
                return existing.frame
            }

            val state = toolStates[toolCallId] ?: ToolState(
                toolCallId = toolCallId,
                toolName = toolName,
                status = status,
                argumentsJson = argumentsJson.trim(),
                resultJson = null,
                isError = false,
                updatedAt = now
            )
            state.toolName = toolName
            state.status = status
            state.argumentsJson = argumentsJson.trim()
            state.updatedAt = now
            toolStates[toolCallId] = state
            trimToolStatesLocked()

            val payload = buildToolCallJson(state, now)
            val frame = appendFrameLocked(
                event = "tool.call",
                data = payload,
                now = now,
                checkpoint = false,
                dedupeKey = dedupeKey
            )
            maybeCheckpointLocked(now)
            return frame
        }

        fun appendToolResultLocked(
            toolCallId: String,
            toolName: String?,
            resultJson: String,
            isError: Boolean,
            status: ToolStatus,
            now: Instant
        ): SseFrame {
            ensureAppendableLocked("tool result")
            val dedupeKey = fingerprint("tool.result", toolCallId, status.wireValue, isError.toString(), resultJson.trim())
            dedupeIndex[dedupeKey]?.let { existing ->
                lastTouchedAt = now
                return existing.frame
            }

            val state = toolStates[toolCallId] ?: ToolState(
                toolCallId = toolCallId,
                toolName = toolName ?: "unknown",
                status = status,
                argumentsJson = "{}",
                resultJson = null,
                isError = isError,
                updatedAt = now
            )
            if (toolName != null) {
                state.toolName = toolName
            }
            state.status = status
            state.resultJson = resultJson.trim()
            state.isError = isError
            state.updatedAt = now
            toolStates[toolCallId] = state
            trimToolStatesLocked()

            val payload = buildToolResultJson(state, now)
            val frame = appendFrameLocked(
                event = "tool.result",
                data = payload,
                now = now,
                checkpoint = false,
                dedupeKey = dedupeKey
            )
            maybeCheckpointLocked(now)
            return frame
        }

        fun appendMetadataLocked(metadataJson: String, now: Instant): SseFrame {
            ensureAppendableLocked("metadata")
            val normalized = metadataJson.trim()
            val dedupeKey = fingerprint("metadata", normalized)
            dedupeIndex[dedupeKey]?.let { existing ->
                lastTouchedAt = now
                return existing.frame
            }

            this.metadataJson = normalized
            val payload = buildMetadataJson(normalized, now)
            val frame = appendFrameLocked(
                event = "assistant.metadata",
                data = payload,
                now = now,
                checkpoint = false,
                dedupeKey = dedupeKey
            )
            maybeCheckpointLocked(now)
            return frame
        }

        fun completeLocked(finishReason: String, usageJson: String?, now: Instant): SseFrame {
            val dedupeKey = fingerprint("complete", finishReason, usageJson.orEmpty())
            dedupeIndex[dedupeKey]?.let { existing ->
                lastTouchedAt = now
                return existing.frame
            }
            if (!open) {
                throw IllegalStateException("stream '$streamId' is already closed")
            }
            ensureCheckpointBeforeTerminalLocked(now)
            open = false
            closedAt = now
            terminalState = TerminalState(
                kind = "completed",
                code = null,
                message = null,
                finishReason = finishReason,
                usageJson = usageJson,
                retriable = false,
                issuedAt = now
            )
            val payload = buildCompletedJson(terminalState!!, now)
            return appendFrameLocked(
                event = "response.completed",
                data = payload,
                now = now,
                checkpoint = false,
                dedupeKey = dedupeKey
            )
        }

        fun failLocked(code: String, message: String, retriable: Boolean, now: Instant): SseFrame {
            val dedupeKey = fingerprint("error", code, message, retriable.toString())
            dedupeIndex[dedupeKey]?.let { existing ->
                lastTouchedAt = now
                return existing.frame
            }
            if (!open) {
                throw IllegalStateException("stream '$streamId' is already closed")
            }
            ensureCheckpointBeforeTerminalLocked(now)
            open = false
            closedAt = now
            terminalState = TerminalState(
                kind = "failed",
                code = code,
                message = message,
                finishReason = null,
                usageJson = null,
                retriable = retriable,
                issuedAt = now
            )
            val payload = buildErrorJson(terminalState!!, now)
            return appendFrameLocked(
                event = "response.error",
                data = payload,
                now = now,
                checkpoint = false,
                dedupeKey = dedupeKey
            )
        }

        fun resumeLocked(cursor: Cursor?, limit: Int, now: Instant): ReplayBatch {
            lastTouchedAt = now
            if (frames.isEmpty()) {
                return ReplayBatch(
                    mode = ReplayMode.GONE,
                    frames = emptyList(),
                    streamOpen = false,
                    latestEventId = null,
                    checkpointEventId = null
                )
            }

            val allFrames = frames.toList()
            val latestEventId = allFrames.last().frame.id
            val checkpointEventId = latestCheckpointId

            if (cursor == null || cursor.epoch != epoch) {
                return snapshotBatchLocked(limit, latestEventId, checkpointEventId)
            }

            val firstRetainedSeq = allFrames.first().seq
            val lastRetainedSeq = allFrames.last().seq

            if (cursor.seq < firstRetainedSeq) {
                return snapshotBatchLocked(limit, latestEventId, checkpointEventId)
            }

            if (cursor.seq >= lastRetainedSeq) {
                return ReplayBatch(
                    mode = ReplayMode.LIVE_DELTA,
                    frames = emptyList(),
                    streamOpen = open,
                    latestEventId = latestEventId,
                    checkpointEventId = checkpointEventId
                )
            }

            val deltaFrames = allFrames
                .asSequence()
                .filter { it.seq > cursor.seq }
                .map { it.frame }
                .take(limit)
                .toList()

            return ReplayBatch(
                mode = ReplayMode.LIVE_DELTA,
                frames = deltaFrames,
                streamOpen = open,
                latestEventId = latestEventId,
                checkpointEventId = checkpointEventId
            )
        }

        fun statsLocked(): StreamStats = StreamStats(
            streamId = streamId,
            epoch = epoch,
            open = open,
            retainedFrames = frames.size,
            retainedBytes = retainedBytes,
            assistantChars = assistantText.length,
            truncatedPrefixChars = truncatedPrefixChars,
            toolStates = toolStates.size,
            latestEventId = frames.lastOrNull()?.frame?.id,
            checkpointEventId = latestCheckpointId,
            lastTouchedAt = lastTouchedAt
        )

        fun enforceBudgetsLocked(now: Instant) {
            if (frames.isEmpty()) {
                return
            }
            if (frames.size <= config.maxRetainedFramesPerStream && retainedBytes <= config.maxReplayBytesPerStream) {
                trimToolStatesLocked()
                trimDedupeIndexLocked()
                return
            }

            if (latestCheckpointSeq == null || latestCheckpointSeq != frames.last().seq) {
                appendCheckpointLocked(now, "budget")
            }

            while ((frames.size > config.maxRetainedFramesPerStream || retainedBytes > config.maxReplayBytesPerStream) && frames.size > 1) {
                dropOldestReplayRecordLocked()
            }
            trimToolStatesLocked()
            trimDedupeIndexLocked()
        }

        private fun snapshotBatchLocked(
            limit: Int,
            latestEventId: String,
            checkpointEventId: String?
        ): ReplayBatch {
            val snapshotSeq = latestCheckpointSeq
            val batch = if (snapshotSeq == null) {
                frames.toList().takeLast(limit).map { it.frame }
            } else {
                frames
                    .toList()
                    .asSequence()
                    .filter { it.seq >= snapshotSeq }
                    .map { it.frame }
                    .take(limit)
                    .toList()
            }

            return ReplayBatch(
                mode = ReplayMode.SNAPSHOT,
                frames = batch,
                streamOpen = open,
                latestEventId = latestEventId,
                checkpointEventId = checkpointEventId
            )
        }

        private fun maybeCheckpointLocked(now: Instant) {
            if (charsSinceCheckpoint >= config.checkpointEveryChars || framesSinceCheckpoint >= config.checkpointEveryFrames) {
                appendCheckpointLocked(now, "threshold")
            }
            enforceBudgetsLocked(now)
        }

        private fun ensureCheckpointBeforeTerminalLocked(now: Instant) {
            if (latestCheckpointSeq == null || charsSinceCheckpoint > 0 || framesSinceCheckpoint > 0) {
                appendCheckpointLocked(now, "pre-terminal")
            }
        }

        private fun appendCheckpointLocked(now: Instant, reason: String): SseFrame {
            val payload = buildSnapshotJson(now, reason, Cursor(epoch, nextSeq + 1).render())
            val frame = appendFrameLocked(
                event = "assistant.snapshot",
                data = payload,
                now = now,
                checkpoint = true,
                dedupeKey = null
            )
            compactToLatestCheckpointLocked()
            return frame
        }

        private fun appendFrameLocked(
            event: String,
            data: String,
            now: Instant,
            checkpoint: Boolean,
            dedupeKey: String?
        ): SseFrame {
            val cursor = Cursor(epoch, ++nextSeq)
            val frame = SseFrame(
                id = cursor.render(),
                event = event,
                data = data
            )
            val record = FrameRecord(
                seq = cursor.seq,
                frame = frame,
                checkpoint = checkpoint,
                bytes = frame.encodedBytes(),
                dedupeKey = dedupeKey
            )
            frames.addLast(record)
            retainedBytes += record.bytes
            lastTouchedAt = now

            if (dedupeKey != null) {
                dedupeIndex[dedupeKey] = record
            }

            if (checkpoint) {
                latestCheckpointSeq = cursor.seq
                latestCheckpointId = frame.id
                charsSinceCheckpoint = 0
                framesSinceCheckpoint = 0
            } else {
                framesSinceCheckpoint += 1
            }

            trimDedupeIndexLocked()
            return frame
        }

        private fun compactToLatestCheckpointLocked() {
            val checkpointSeq = latestCheckpointSeq ?: return
            while (frames.isNotEmpty() && frames.first().seq < checkpointSeq) {
                removeFirstRecordLocked()
            }
        }

        private fun dropOldestReplayRecordLocked() {
            if (frames.isEmpty()) {
                return
            }
            val first = frames.removeFirst()
            if (first.checkpoint) {
                if (frames.isEmpty()) {
                    frames.addFirst(first)
                    return
                }
                forgetRecordLocked(frames.removeFirst())
                frames.addFirst(first)
                return
            }
            forgetRecordLocked(first)
        }

        private fun removeFirstRecordLocked() {
            if (frames.isEmpty()) {
                return
            }
            val record = frames.removeFirst()
            forgetRecordLocked(record)
        }

        private fun forgetRecordLocked(record: FrameRecord) {
            retainedBytes -= record.bytes
            if (retainedBytes < 0) {
                retainedBytes = 0
            }
            if (record.dedupeKey != null) {
                val indexed = dedupeIndex[record.dedupeKey]
                if (indexed != null && indexed.seq == record.seq) {
                    dedupeIndex.remove(record.dedupeKey)
                }
            }
        }

        private fun trimToolStatesLocked() {
            if (toolStates.size <= config.maxToolStates) {
                return
            }

            val removals = ArrayList<String>()
            toolStates.entries.forEach { entry ->
                if (toolStates.size - removals.size <= config.maxToolStates) {
                    return@forEach
                }
                if (entry.value.status == ToolStatus.COMPLETED || entry.value.status == ToolStatus.FAILED) {
                    removals.add(entry.key)
                }
            }
            removals.forEach { toolStates.remove(it) }

            if (toolStates.size <= config.maxToolStates) {
                return
            }

            val iterator = toolStates.entries.iterator()
            while (toolStates.size > config.maxToolStates && iterator.hasNext()) {
                iterator.next()
                iterator.remove()
            }
        }

        private fun trimDedupeIndexLocked() {
            val limit = config.maxRetainedFramesPerStream * 4
            val iterator = dedupeIndex.entries.iterator()
            while (dedupeIndex.size > limit && iterator.hasNext()) {
                iterator.next()
                iterator.remove()
            }
        }

        private fun trimAssistantTextLocked() {
            val overflow = assistantText.length - config.maxAssistantTextChars
            if (overflow > 0) {
                assistantText.delete(0, overflow)
                truncatedPrefixChars += overflow
            }
        }

        private fun ensureAppendableLocked(action: String) {
            if (!open) {
                throw IllegalStateException("cannot append $action to closed stream '$streamId'")
            }
        }

        private fun buildStartJson(now: Instant): String {
            val builder = StringBuilder(192)
            builder.append('{')
            var comma = false
            comma = appendStringField(builder, "streamId", streamId, comma)
            comma = appendStringField(builder, "assistantMessageId", assistantMessageId, comma)
            comma = appendStringField(builder, "epoch", epoch, comma)
            comma = appendStringField(builder, "issuedAt", now.toString(), comma)
            comma = appendBooleanField(builder, "resumeSupported", true, comma)
            comma = appendIntField(builder, "checkpointEveryChars", config.checkpointEveryChars, comma)
            appendIntField(builder, "checkpointEveryFrames", config.checkpointEveryFrames, comma)
            builder.append('}')
            return builder.toString()
        }

        private fun buildAssistantDeltaJson(delta: String, now: Instant): String {
            val builder = StringBuilder(delta.length + 160)
            builder.append('{')
            var comma = false
            comma = appendStringField(builder, "streamId", streamId, comma)
            comma = appendStringField(builder, "assistantMessageId", assistantMessageId, comma)
            comma = appendStringField(builder, "delta", delta, comma)
            comma = appendIntField(builder, "totalChars", truncatedPrefixChars + assistantText.length, comma)
            comma = appendIntField(builder, "truncatedPrefixChars", truncatedPrefixChars, comma)
            appendStringField(builder, "issuedAt", now.toString(), comma)
            builder.append('}')
            return builder.toString()
        }

        private fun buildToolCallJson(state: ToolState, now: Instant): String {
            val builder = StringBuilder(256)
            builder.append('{')
            var comma = false
            comma = appendStringField(builder, "streamId", streamId, comma)
            comma = appendStringField(builder, "assistantMessageId", assistantMessageId, comma)
            comma = appendStringField(builder, "toolCallId", state.toolCallId, comma)
            comma = appendStringField(builder, "toolName", state.toolName, comma)
            comma = appendStringField(builder, "status", state.status.wireValue, comma)
            comma = appendRawField(builder, "arguments", state.argumentsJson, comma)
            appendStringField(builder, "issuedAt", now.toString(), comma)
            builder.append('}')
            return builder.toString()
        }

        private fun buildToolResultJson(state: ToolState, now: Instant): String {
            val builder = StringBuilder(256)
            builder.append('{')
            var comma = false
            comma = appendStringField(builder, "streamId", streamId, comma)
            comma = appendStringField(builder, "assistantMessageId", assistantMessageId, comma)
            comma = appendStringField(builder, "toolCallId", state.toolCallId, comma)
            comma = appendStringField(builder, "toolName", state.toolName, comma)
            comma = appendStringField(builder, "status", state.status.wireValue, comma)
            comma = appendBooleanField(builder, "error", state.isError, comma)
            comma = appendRawField(builder, "result", state.resultJson, comma)
            appendStringField(builder, "issuedAt", now.toString(), comma)
            builder.append('}')
            return builder.toString()
        }

        private fun buildMetadataJson(metadataJson: String, now: Instant): String {
            val builder = StringBuilder(128 + metadataJson.length)
            builder.append('{')
            var comma = false
            comma = appendStringField(builder, "streamId", streamId, comma)
            comma = appendStringField(builder, "assistantMessageId", assistantMessageId, comma)
            comma = appendRawField(builder, "metadata", metadataJson, comma)
            appendStringField(builder, "issuedAt", now.toString(), comma)
            builder.append('}')
            return builder.toString()
        }

        private fun buildCompletedJson(terminal: TerminalState, now: Instant): String {
            val builder = StringBuilder(192)
            builder.append('{')
            var comma = false
            comma = appendStringField(builder, "streamId", streamId, comma)
            comma = appendStringField(builder, "assistantMessageId", assistantMessageId, comma)
            comma = appendStringField(builder, "finishReason", terminal.finishReason ?: "stop", comma)
            comma = appendRawField(builder, "usage", terminal.usageJson, comma)
            appendStringField(builder, "issuedAt", now.toString(), comma)
            builder.append('}')
            return builder.toString()
        }

        private fun buildErrorJson(terminal: TerminalState, now: Instant): String {
            val builder = StringBuilder(256)
            builder.append('{')
            var comma = false
            comma = appendStringField(builder, "streamId", streamId, comma)
            comma = appendStringField(builder, "assistantMessageId", assistantMessageId, comma)
            comma = appendStringField(builder, "code", terminal.code ?: "stream_error", comma)
            comma = appendStringField(builder, "message", terminal.message ?: "stream failed", comma)
            comma = appendBooleanField(builder, "retriable", terminal.retriable, comma)
            appendStringField(builder, "issuedAt", now.toString(), comma)
            builder.append('}')
            return builder.toString()
        }

        private fun buildSnapshotJson(now: Instant, reason: String, projectedCursor: String): String {
            val builder = StringBuilder(512 + assistantText.length + toolStates.size * 96)
            builder.append('{')
            var comma = false
            comma = appendStringField(builder, "streamId", streamId, comma)
            comma = appendStringField(builder, "assistantMessageId", assistantMessageId, comma)
            comma = appendStringField(builder, "cursor", projectedCursor, comma)
            comma = appendStringField(builder, "epoch", epoch, comma)
            comma = appendBooleanField(builder, "open", open, comma)
            comma = appendStringField(builder, "reason", reason, comma)
            comma = appendStringField(builder, "issuedAt", now.toString(), comma)
            comma = appendStringField(builder, "text", assistantText.toString(), comma)
            comma = appendIntField(builder, "truncatedPrefixChars", truncatedPrefixChars, comma)
            comma = appendRawField(builder, "metadata", metadataJson, comma)

            if (comma) builder.append(',')
            builder.append(jsonString("tools")).append(':').append('[')
            var firstTool = true
            toolStates.values.forEach { state ->
                if (!firstTool) {
                    builder.append(',')
                }
                firstTool = false
                builder.append('{')
                var toolComma = false
                toolComma = appendStringField(builder, "toolCallId", state.toolCallId, toolComma)
                toolComma = appendStringField(builder, "toolName", state.toolName, toolComma)
                toolComma = appendStringField(builder, "status", state.status.wireValue, toolComma)
                toolComma = appendBooleanField(builder, "error", state.isError, toolComma)
                toolComma = appendRawField(builder, "arguments", state.argumentsJson, toolComma)
                toolComma = appendRawField(builder, "result", state.resultJson, toolComma)
                appendStringField(builder, "updatedAt", state.updatedAt.toString(), toolComma)
                builder.append('}')
            }
            builder.append(']')
            comma = true

            if (comma) builder.append(',')
            builder.append(jsonString("terminal")).append(':')
            if (terminalState == null) {
                builder.append("null")
            } else {
                builder.append('{')
                var terminalComma = false
                terminalComma = appendStringField(builder, "kind", terminalState!!.kind, terminalComma)
                terminalComma = appendNullableStringField(builder, "code", terminalState!!.code, terminalComma)
                terminalComma = appendNullableStringField(builder, "message", terminalState!!.message, terminalComma)
                terminalComma = appendNullableStringField(builder, "finishReason", terminalState!!.finishReason, terminalComma)
                terminalComma = appendBooleanField(builder, "retriable", terminalState!!.retriable, terminalComma)
                terminalComma = appendRawField(builder, "usage", terminalState!!.usageJson, terminalComma)
                appendStringField(builder, "issuedAt", terminalState!!.issuedAt.toString(), terminalComma)
                builder.append('}')
            }

            builder.append('}')
            return builder.toString()
        }
    }

    private data class FrameRecord(
        val seq: Long,
        val frame: SseFrame,
        val checkpoint: Boolean,
        val bytes: Int,
        val dedupeKey: String?
    )

    private class ToolState(
        val toolCallId: String,
        var toolName: String,
        var status: ToolStatus,
        var argumentsJson: String,
        var resultJson: String?,
        var isError: Boolean,
        var updatedAt: Instant
    )

    private data class TerminalState(
        val kind: String,
        val code: String?,
        val message: String?,
        val finishReason: String?,
        val usageJson: String?,
        val retriable: Boolean,
        val issuedAt: Instant
    )

    private data class Cursor(val epoch: String, val seq: Long) {
        fun render(): String = "$epoch:$seq"

        companion object {
            fun parse(value: String?): Cursor? {
                if (value.isNullOrBlank()) {
                    return null
                }
                val separator = value.lastIndexOf(':')
                if (separator <= 0 || separator == value.lastIndex) {
                    return null
                }
                val epoch = value.substring(0, separator)
                val seq = value.substring(separator + 1).toLongOrNull() ?: return null
                if (epoch.isBlank() || seq < 0) {
                    return null
                }
                return Cursor(epoch, seq)
            }
        }
    }

    private fun requireSessionLocked(streamId: String): StreamSession {
        return sessions[streamId] ?: throw NoSuchElementException("unknown stream '$streamId'")
    }

    private fun requireOpenSessionLocked(streamId: String): StreamSession {
        val session = requireSessionLocked(streamId)
        if (!session.open) {
            throw IllegalStateException("stream '$streamId' is already closed")
        }
        return session
    }

    private fun pruneExpiredLocked(now: Instant) {
        val iterator = sessions.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (Duration.between(entry.value.lastTouchedAt, now) > config.idleTtl) {
                iterator.remove()
            }
        }
    }

    private fun evictIfNeededLocked() {
        while (sessions.size > config.maxSessions) {
            if (removeOldestLocked(preferClosed = true)) {
                continue
            }
            if (!removeOldestLocked(preferClosed = false)) {
                return
            }
        }
    }

    private fun removeOldestLocked(preferClosed: Boolean): Boolean {
        val iterator = sessions.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (!preferClosed || !entry.value.open) {
                iterator.remove()
                return true
            }
        }
        return false
    }

    private fun now(): Instant = config.clock.instant()

    companion object {
        private val STREAM_ID_REGEX = Regex("[A-Za-z0-9._:-]{1,128}")
        private val secureRandom = SecureRandom()

        private fun requireStreamId(streamId: String): String {
            val normalized = requireLooseText(streamId, "streamId", 128)
            require(STREAM_ID_REGEX.matches(normalized)) {
                "streamId must match ${STREAM_ID_REGEX.pattern}"
            }
            return normalized
        }

        private fun requireLooseText(value: String, fieldName: String, maxLength: Int): String {
            val normalized = value.trim()
            require(normalized.isNotEmpty()) { "$fieldName must not be blank" }
            require(normalized.length <= maxLength) { "$fieldName exceeds $maxLength characters" }
            require(normalized.none { it == '\u0000' || it == '\n' || it == '\r' }) {
                "$fieldName contains unsupported control characters"
            }
            return normalized
        }

        private fun requireJsonish(value: String, fieldName: String): String {
            val normalized = value.trim()
            require(normalized.isNotEmpty()) { "$fieldName must not be blank" }
            require(looksLikeJson(normalized)) { "$fieldName must look like JSON" }
            return normalized
        }

        private fun randomHex(bytes: Int): String {
            val buffer = ByteArray(bytes)
            secureRandom.nextBytes(buffer)
            return buffer.joinToString(separator = "") { each -> "%02x".format(each.toInt() and 0xff) }
        }

        private fun fingerprint(vararg parts: String): String {
            val digest = MessageDigest.getInstance("SHA-256")
            parts.forEach { part ->
                digest.update(part.toByteArray(Charsets.UTF_8))
                digest.update(0)
            }
            return digest.digest().joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }
        }

        private fun looksLikeJson(value: String): Boolean {
            val normalized = value.trim()
            if (normalized.isEmpty()) {
                return false
            }
            return when {
                normalized.startsWith("{") -> normalized.endsWith("}")
                normalized.startsWith("[") -> normalized.endsWith("]")
                normalized.startsWith("\"") -> normalized.endsWith("\"")
                normalized == "true" || normalized == "false" || normalized == "null" -> true
                normalized.first().isDigit() || normalized.first() == '-' -> true
                else -> false
            }
        }

        private fun appendStringField(builder: StringBuilder, name: String, value: String, comma: Boolean): Boolean {
            if (comma) builder.append(',')
            builder.append(jsonString(name)).append(':').append(jsonString(value))
            return true
        }

        private fun appendNullableStringField(builder: StringBuilder, name: String, value: String?, comma: Boolean): Boolean {
            if (comma) builder.append(',')
            builder.append(jsonString(name)).append(':')
            if (value == null) {
                builder.append("null")
            } else {
                builder.append(jsonString(value))
            }
            return true
        }

        private fun appendBooleanField(builder: StringBuilder, name: String, value: Boolean, comma: Boolean): Boolean {
            if (comma) builder.append(',')
            builder.append(jsonString(name)).append(':').append(if (value) "true" else "false")
            return true
        }

        private fun appendIntField(builder: StringBuilder, name: String, value: Int, comma: Boolean): Boolean {
            if (comma) builder.append(',')
            builder.append(jsonString(name)).append(':').append(value)
            return true
        }

        private fun appendRawField(builder: StringBuilder, name: String, value: String?, comma: Boolean): Boolean {
            if (comma) builder.append(',')
            builder.append(jsonString(name)).append(':')
            if (value == null) {
                builder.append("null")
            } else if (looksLikeJson(value)) {
                builder.append(value.trim())
            } else {
                builder.append(jsonString(value))
            }
            return true
        }

        private fun jsonString(value: String): String {
            val builder = StringBuilder(value.length + 16)
            builder.append('"')
            value.forEach { character ->
                when (character) {
                    '\\' -> builder.append("\\\\")
                    '"' -> builder.append("\\\"")
                    '\b' -> builder.append("\\b")
                    '\u000C' -> builder.append("\\f")
                    '\n' -> builder.append("\\n")
                    '\r' -> builder.append("\\r")
                    '\t' -> builder.append("\\t")
                    else -> {
                        if (character < ' ') {
                            val hex = character.code.toString(16).padStart(4, '0')
                            builder.append("\\u").append(hex)
                        } else {
                            builder.append(character)
                        }
                    }
                }
            }
            builder.append('"')
            return builder.toString()
        }
    }
}

/*
This solves flaky resumable AI streaming over Server-Sent Events in Kotlin backends, especially when OpenAI, Anthropic, Gemini, or gateway responses are still mid-flight and the browser or mobile client reconnects with Last-Event-ID. Built because one of the most annoying production bugs in 2026 is not the first stream, it is the reconnect: duplicated tool events, lost completion markers, or replay buffers that grow until they hurt latency and memory. Use it when you run Ktor, Spring Boot, Micronaut, or a plain JVM service that streams assistant tokens and tool activity to web or mobile clients and you need replay, checkpointing, and deterministic recovery.

The trick: this keeps a bounded in-memory ledger, emits compact snapshot frames on a cadence, preserves tool state, trims oversized assistant text safely, and falls back to a snapshot when a reconnect cursor is too old. That means you do not have to retain every token forever just to support resume. Drop this into a Kotlin service that already emits SSE, wire `open`, `appendAssistantText`, `appendToolCall`, `appendToolResult`, `complete`, and `resume` around your model gateway, and you get a production-ready starting point for resumable LLM token streaming, SSE replay, tool call recovery, mobile reconnect handling, and backend memory control without dragging in a framework or a pile of extra dependencies.
*/