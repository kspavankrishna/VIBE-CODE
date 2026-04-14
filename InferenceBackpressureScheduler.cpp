#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace vibe {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Milliseconds = std::chrono::milliseconds;

enum class OverflowPolicy {
    RejectIncoming,
    DropOldest,
    EvictStream
};

enum class EvictionReason {
    None,
    StreamQueueLimit,
    GlobalQueueLimit,
    MaxQueueLatency,
    ExplicitCancel
};

struct StreamPolicy {
    std::size_t maxBufferedBytes = 256 * 1024;
    std::size_t maxBufferedChunks = 256;
    std::size_t burstBytes = 8 * 1024;
    std::size_t targetChunkBytes = 1024;
    std::size_t weight = 1;
    Milliseconds maxQueueLatency{1500};
    OverflowPolicy overflowPolicy = OverflowPolicy::DropOldest;
    bool preserveTerminalChunk = true;
};

struct SchedulerOptions {
    std::size_t globalMaxBufferedBytes = 32 * 1024 * 1024;
    std::size_t maxStreams = 4096;
    std::size_t maxBatchRecords = 128;
    Milliseconds idleTtl{30000};
};

struct EnqueueOptions {
    bool terminal = false;
};

enum class Admission {
    Accepted,
    Coalesced,
    Rejected
};

struct EnqueueResult {
    Admission admission = Admission::Rejected;
    bool createdStream = false;
    bool streamEvicted = false;
    std::size_t droppedBytes = 0;
    std::size_t droppedChunks = 0;
    std::size_t totalQueuedBytes = 0;
    std::string message;
};

struct DispatchRecord {
    std::string streamId;
    std::string payload;
    bool terminal = false;
    std::uint64_t firstSequence = 0;
    std::uint64_t lastSequence = 0;
    Milliseconds queueDelay{0};
};

struct DispatchBatch {
    std::vector<DispatchRecord> records;
    std::size_t totalBytes = 0;
};

struct StreamView {
    std::string streamId;
    std::size_t queuedBytes = 0;
    std::size_t queuedChunks = 0;
    std::size_t deficitBytes = 0;
    std::size_t droppedBytes = 0;
    std::size_t droppedChunks = 0;
    std::size_t evictions = 0;
    std::size_t oldestQueuedMs = 0;
    EvictionReason lastEvictionReason = EvictionReason::None;
};

struct SchedulerSnapshot {
    std::size_t totalStreams = 0;
    std::size_t totalQueuedBytes = 0;
    std::size_t totalQueuedChunks = 0;
    std::vector<StreamView> streams;
};

class InferenceBackpressureScheduler {
public:
    explicit InferenceBackpressureScheduler(SchedulerOptions options = {})
        : options_(normalizeOptions(options)) {}

    bool registerStream(std::string streamId, StreamPolicy policy = {}) {
        if (streamId.empty()) {
            return false;
        }

        std::scoped_lock lock(mutex_);
        if (streams_.find(streamId) != streams_.end()) {
            streams_[streamId].policy = normalizePolicy(policy);
            return false;
        }

        if (streams_.size() >= options_.maxStreams) {
            return false;
        }

        StreamState state;
        state.streamId = std::move(streamId);
        state.policy = normalizePolicy(policy);
        state.lastActivity = Clock::now();
        ring_.push_back(state.streamId);
        streams_.emplace(state.streamId, std::move(state));
        return true;
    }

    bool updateStreamPolicy(std::string_view streamId, StreamPolicy policy) {
        std::scoped_lock lock(mutex_);
        auto it = streams_.find(std::string(streamId));
        if (it == streams_.end()) {
            return false;
        }
        it->second.policy = normalizePolicy(policy);
        return true;
    }

    EnqueueResult enqueue(std::string_view streamId, std::string payload, EnqueueOptions options = {}) {
        EnqueueResult result;
        if (streamId.empty()) {
            result.message = "stream id is required";
            return result;
        }

        const std::size_t incomingBytes = payload.size();
        const TimePoint now = Clock::now();
        std::scoped_lock lock(mutex_);
        trimExpiredStreamsLocked(now);
        trimIdleStreamsLocked(now);

        bool createdStream = false;
        StreamState* state = ensureStreamLocked(streamId, &createdStream);
        if (state == nullptr) {
            result.message = "stream limit reached";
            return result;
        }

        result.createdStream = createdStream;
        state->lastActivity = now;

        if (incomingBytes == 0 && !options.terminal) {
            result.admission = Admission::Accepted;
            result.totalQueuedBytes = totalQueuedBytes_;
            result.message = "empty non-terminal payload ignored";
            return result;
        }

        if (incomingBytes > state->policy.maxBufferedBytes) {
            result.message = "payload exceeds per-stream byte limit";
            return result;
        }

        if (options.terminal && incomingBytes == 0 && state->queue.empty()) {
            Chunk terminalChunk;
            terminalChunk.terminal = true;
            terminalChunk.enqueuedAt = now;
            terminalChunk.firstSequence = ++nextSequence_;
            terminalChunk.lastSequence = terminalChunk.firstSequence;
            state->queue.push_back(std::move(terminalChunk));
            state->queuedChunks += 1;
            totalQueuedChunks_ += 1;
            result.admission = Admission::Accepted;
            result.totalQueuedBytes = totalQueuedBytes_;
            result.message = "queued terminal marker";
            return result;
        }

        if (tryCoalesceLocked(*state, payload, options, now)) {
            result.admission = Admission::Coalesced;
            result.totalQueuedBytes = totalQueuedBytes_;
            result.message = "payload coalesced into trailing chunk";
            return result;
        }

        if (state->policy.overflowPolicy == OverflowPolicy::RejectIncoming &&
            wouldExceedStreamLimitAfterAppendLocked(*state, incomingBytes, 1)) {
            result.message = "stream queue limit would be exceeded";
            return result;
        }

        Chunk chunk;
        chunk.payload = std::move(payload);
        chunk.terminal = options.terminal;
        chunk.enqueuedAt = now;
        chunk.firstSequence = ++nextSequence_;
        chunk.lastSequence = chunk.firstSequence;

        state->queuedBytes += chunk.payload.size();
        state->queuedChunks += 1;
        totalQueuedBytes_ += chunk.payload.size();
        totalQueuedChunks_ += 1;
        state->queue.push_back(std::move(chunk));

        if (!enforceStreamLimitLocked(*state, result)) {
            state->lastActivity = now;
            result.totalQueuedBytes = totalQueuedBytes_;
            return result;
        }

        result.streamEvicted = enforceGlobalBudgetLocked(state->streamId, result, now);
        result.admission = result.streamEvicted ? Admission::Rejected : Admission::Accepted;
        result.totalQueuedBytes = totalQueuedBytes_;
        if (result.message.empty()) {
            result.message = "payload admitted";
        }
        return result;
    }

    DispatchBatch drain(std::size_t maxBytes, TimePoint now = Clock::now()) {
        DispatchBatch batch;
        if (maxBytes == 0) {
            return batch;
        }

        std::scoped_lock lock(mutex_);
        trimExpiredStreamsLocked(now);
        trimIdleStreamsLocked(now);

        if (ring_.empty()) {
            return batch;
        }

        std::size_t stalls = 0;
        while (!ring_.empty() &&
               batch.records.size() < options_.maxBatchRecords &&
               stalls < ring_.size()) {
            if (dispatchCursor_ >= ring_.size()) {
                dispatchCursor_ = 0;
            }

            auto it = streams_.find(ring_[dispatchCursor_]);
            if (it == streams_.end()) {
                removeRingSlotLocked(dispatchCursor_);
                continue;
            }

            StreamState& state = it->second;
            if (state.queue.empty()) {
                state.deficitBytes = 0;
                ++dispatchCursor_;
                ++stalls;
                continue;
            }

            const std::size_t quantum = std::max(state.policy.burstBytes, state.policy.targetChunkBytes) *
                                        std::max<std::size_t>(1, state.policy.weight);
            state.deficitBytes = saturatingAdd(state.deficitBytes, quantum, state.policy.burstBytes * 8);

            Chunk& head = state.queue.front();
            const std::size_t headBytes = head.payload.size();
            const bool allowBudgetOverrun = batch.records.empty();
            const bool budgetFits = headBytes <= maxBytes || allowBudgetOverrun;
            if (head.terminal) {
                state.deficitBytes = std::max(state.deficitBytes, headBytes);
            }

            if (!budgetFits || headBytes > state.deficitBytes) {
                ++dispatchCursor_;
                ++stalls;
                continue;
            }

            DispatchRecord record;
            record.streamId = state.streamId;
            record.payload = std::move(head.payload);
            record.terminal = head.terminal;
            record.firstSequence = head.firstSequence;
            record.lastSequence = head.lastSequence;
            record.queueDelay =
                std::chrono::duration_cast<Milliseconds>(now - head.enqueuedAt);

            state.deficitBytes -= headBytes;
            state.queuedBytes -= headBytes;
            state.queuedChunks -= 1;
            state.dispatchedBytes += headBytes;
            state.lastActivity = now;
            totalQueuedBytes_ -= headBytes;
            totalQueuedChunks_ -= 1;
            state.queue.pop_front();

            batch.totalBytes += headBytes;
            maxBytes = (headBytes > maxBytes) ? 0 : (maxBytes - headBytes);
            batch.records.push_back(std::move(record));

            if (state.queue.empty()) {
                state.deficitBytes = 0;
            }

            ++dispatchCursor_;
            stalls = 0;

            if (maxBytes == 0) {
                break;
            }
        }

        return batch;
    }

    bool cancelStream(std::string_view streamId) {
        std::scoped_lock lock(mutex_);
        return evictStreamLocked(std::string(streamId), EvictionReason::ExplicitCancel);
    }

    SchedulerSnapshot snapshot(TimePoint now = Clock::now()) const {
        std::scoped_lock lock(mutex_);
        SchedulerSnapshot view;
        view.totalStreams = streams_.size();
        view.totalQueuedBytes = totalQueuedBytes_;
        view.totalQueuedChunks = totalQueuedChunks_;
        view.streams.reserve(streams_.size());

        for (const auto& entry : streams_) {
            const StreamState& state = entry.second;
            StreamView streamView;
            streamView.streamId = state.streamId;
            streamView.queuedBytes = state.queuedBytes;
            streamView.queuedChunks = state.queuedChunks;
            streamView.deficitBytes = state.deficitBytes;
            streamView.droppedBytes = state.droppedBytes;
            streamView.droppedChunks = state.droppedChunks;
            streamView.evictions = state.evictions;
            streamView.lastEvictionReason = state.lastEvictionReason;
            if (!state.queue.empty()) {
                streamView.oldestQueuedMs = static_cast<std::size_t>(
                    std::chrono::duration_cast<Milliseconds>(now - state.queue.front().enqueuedAt).count());
            }
            view.streams.push_back(std::move(streamView));
        }

        std::sort(view.streams.begin(), view.streams.end(),
                  [](const StreamView& lhs, const StreamView& rhs) {
                      if (lhs.queuedBytes != rhs.queuedBytes) {
                          return lhs.queuedBytes > rhs.queuedBytes;
                      }
                      return lhs.streamId < rhs.streamId;
                  });
        return view;
    }

private:
    struct Chunk {
        std::string payload;
        bool terminal = false;
        TimePoint enqueuedAt{};
        std::uint64_t firstSequence = 0;
        std::uint64_t lastSequence = 0;
    };

    struct StreamState {
        std::string streamId;
        StreamPolicy policy;
        std::deque<Chunk> queue;
        std::size_t queuedBytes = 0;
        std::size_t queuedChunks = 0;
        std::size_t deficitBytes = 0;
        std::size_t dispatchedBytes = 0;
        std::size_t droppedBytes = 0;
        std::size_t droppedChunks = 0;
        std::size_t evictions = 0;
        TimePoint lastActivity{};
        EvictionReason lastEvictionReason = EvictionReason::None;
    };

    static StreamPolicy normalizePolicy(StreamPolicy policy) {
        policy.maxBufferedBytes = std::max<std::size_t>(policy.maxBufferedBytes, 1024);
        policy.maxBufferedChunks = std::max<std::size_t>(policy.maxBufferedChunks, 4);
        policy.burstBytes = std::max<std::size_t>(policy.burstBytes, 1024);
        policy.targetChunkBytes = std::max<std::size_t>(policy.targetChunkBytes, 128);
        policy.weight = std::max<std::size_t>(policy.weight, 1);
        policy.maxQueueLatency = std::max(policy.maxQueueLatency, Milliseconds{50});
        return policy;
    }

    static SchedulerOptions normalizeOptions(SchedulerOptions options) {
        options.globalMaxBufferedBytes = std::max<std::size_t>(options.globalMaxBufferedBytes, 1024 * 1024);
        options.maxStreams = std::max<std::size_t>(options.maxStreams, 1);
        options.maxBatchRecords = std::max<std::size_t>(options.maxBatchRecords, 1);
        options.idleTtl = std::max(options.idleTtl, Milliseconds{1000});
        return options;
    }

    StreamState* ensureStreamLocked(std::string_view streamId, bool* createdStream) {
        auto it = streams_.find(std::string(streamId));
        if (it != streams_.end()) {
            return &it->second;
        }

        if (streams_.size() >= options_.maxStreams) {
            return nullptr;
        }

        StreamState state;
        state.streamId = std::string(streamId);
        state.policy = normalizePolicy(StreamPolicy{});
        state.lastActivity = Clock::now();
        ring_.push_back(state.streamId);
        auto [inserted, ok] = streams_.emplace(state.streamId, std::move(state));
        if (!ok) {
            return nullptr;
        }
        if (createdStream != nullptr) {
            *createdStream = true;
        }
        return &inserted->second;
    }

    bool tryCoalesceLocked(StreamState& state,
                           const std::string& payload,
                           const EnqueueOptions& options,
                           TimePoint now) {
        if (state.queue.empty()) {
            return false;
        }

        Chunk& tail = state.queue.back();
        if (tail.terminal) {
            return false;
        }

        if (tail.payload.size() + payload.size() > state.policy.targetChunkBytes) {
            return false;
        }

        tail.payload += payload;
        tail.terminal = options.terminal;
        tail.lastSequence = ++nextSequence_;
        state.queuedBytes += payload.size();
        totalQueuedBytes_ += payload.size();
        state.lastActivity = now;
        return true;
    }

    bool wouldExceedStreamLimitAfterAppendLocked(const StreamState& state,
                                                 std::size_t incomingBytes,
                                                 std::size_t incomingChunks) const {
        return state.queuedBytes + incomingBytes > state.policy.maxBufferedBytes ||
               state.queuedChunks + incomingChunks > state.policy.maxBufferedChunks;
    }

    bool isOverStreamLimitLocked(const StreamState& state) const {
        return state.queuedBytes > state.policy.maxBufferedBytes ||
               state.queuedChunks > state.policy.maxBufferedChunks;
    }

    bool enforceStreamLimitLocked(StreamState& state, EnqueueResult& result) {
        if (!isOverStreamLimitLocked(state)) {
            return true;
        }

        switch (state.policy.overflowPolicy) {
        case OverflowPolicy::RejectIncoming: {
            Chunk rejected = std::move(state.queue.back());
            state.queue.pop_back();
            state.queuedBytes -= rejected.payload.size();
            state.queuedChunks -= 1;
            totalQueuedBytes_ -= rejected.payload.size();
            totalQueuedChunks_ -= 1;
            result.message = "stream queue limit reached";
            return false;
        }
        case OverflowPolicy::EvictStream: {
            evictStateContentsLocked(state, EvictionReason::StreamQueueLimit, &result);
            result.streamEvicted = true;
            result.message = "stream evicted after exceeding queue limit";
            return false;
        }
        case OverflowPolicy::DropOldest:
            while (isOverStreamLimitLocked(state)) {
                if (!dropOldestChunkLocked(state, EvictionReason::StreamQueueLimit, &result)) {
                    result.message = "stream queue limit reached and no chunk could be dropped";
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    bool enforceGlobalBudgetLocked(std::string_view admittedStreamId,
                                   EnqueueResult& result,
                                   TimePoint now) {
        bool admittedStreamEvicted = false;
        while (totalQueuedBytes_ > options_.globalMaxBufferedBytes) {
            StreamState* victim = selectVictimLocked(now);
            if (victim == nullptr) {
                break;
            }

            if (victim->policy.overflowPolicy == OverflowPolicy::DropOldest &&
                victim->queue.size() > 1 &&
                dropOldestChunkLocked(*victim, EvictionReason::GlobalQueueLimit, &result)) {
                continue;
            }

            if (victim->streamId == admittedStreamId) {
                admittedStreamEvicted = true;
            }
            evictStateContentsLocked(*victim, EvictionReason::GlobalQueueLimit, &result);
        }
        return admittedStreamEvicted;
    }

    StreamState* selectVictimLocked(TimePoint now) {
        StreamState* victim = nullptr;
        double bestScore = -1.0;

        for (auto& entry : streams_) {
            StreamState& state = entry.second;
            if (state.queue.empty()) {
                continue;
            }

            const double backlogRatio =
                static_cast<double>(state.queuedBytes) /
                static_cast<double>(std::max<std::size_t>(1, state.policy.maxBufferedBytes));
            const double ageRatio =
                static_cast<double>(
                    std::chrono::duration_cast<Milliseconds>(now - state.queue.front().enqueuedAt).count()) /
                static_cast<double>(std::max<std::int64_t>(1, state.policy.maxQueueLatency.count()));
            const double score = backlogRatio * 2.0 + ageRatio;

            if (score > bestScore) {
                bestScore = score;
                victim = &state;
            }
        }

        return victim;
    }

    bool dropOldestChunkLocked(StreamState& state,
                               EvictionReason reason,
                               EnqueueResult* result) {
        if (state.queue.empty()) {
            return false;
        }

        std::size_t dropIndex = 0;
        if (state.policy.preserveTerminalChunk &&
            state.queue.front().terminal &&
            state.queue.size() == 1) {
            return false;
        }

        if (state.policy.preserveTerminalChunk && state.queue.front().terminal) {
            auto candidate = std::find_if(state.queue.begin(), state.queue.end(),
                                          [](const Chunk& chunk) { return !chunk.terminal; });
            if (candidate == state.queue.end()) {
                return false;
            }
            dropIndex = static_cast<std::size_t>(std::distance(state.queue.begin(), candidate));
        }

        Chunk dropped = std::move(state.queue[dropIndex]);
        state.queue.erase(state.queue.begin() + static_cast<std::ptrdiff_t>(dropIndex));
        const std::size_t bytes = dropped.payload.size();
        state.queuedBytes -= bytes;
        state.queuedChunks -= 1;
        state.droppedBytes += bytes;
        state.droppedChunks += 1;
        state.lastEvictionReason = reason;
        totalQueuedBytes_ -= bytes;
        totalQueuedChunks_ -= 1;

        if (result != nullptr) {
            result->droppedBytes += bytes;
            result->droppedChunks += 1;
        }

        return true;
    }

    void evictStateContentsLocked(StreamState& state,
                                  EvictionReason reason,
                                  EnqueueResult* result) {
        while (!state.queue.empty()) {
            Chunk dropped = std::move(state.queue.front());
            state.queue.pop_front();
            const std::size_t bytes = dropped.payload.size();
            state.queuedBytes -= bytes;
            state.queuedChunks -= 1;
            state.droppedBytes += bytes;
            state.droppedChunks += 1;
            totalQueuedBytes_ -= bytes;
            totalQueuedChunks_ -= 1;

            if (result != nullptr) {
                result->droppedBytes += bytes;
                result->droppedChunks += 1;
            }
        }

        state.evictions += 1;
        state.lastEvictionReason = reason;
        state.deficitBytes = 0;
        state.lastActivity = Clock::now();
    }

    bool evictStreamLocked(const std::string& streamId, EvictionReason reason) {
        auto it = streams_.find(streamId);
        if (it == streams_.end()) {
            return false;
        }

        evictStateContentsLocked(it->second, reason, nullptr);
        removeRingIdLocked(streamId);
        streams_.erase(it);
        return true;
    }

    void trimExpiredStreamsLocked(TimePoint now) {
        for (auto& entry : streams_) {
            StreamState& state = entry.second;
            if (state.queue.empty()) {
                continue;
            }

            const auto age = std::chrono::duration_cast<Milliseconds>(now - state.queue.front().enqueuedAt);
            if (age > state.policy.maxQueueLatency) {
                evictStateContentsLocked(state, EvictionReason::MaxQueueLatency, nullptr);
            }
        }
    }

    void trimIdleStreamsLocked(TimePoint now) {
        std::vector<std::string> toErase;
        toErase.reserve(streams_.size());
        for (const auto& entry : streams_) {
            const StreamState& state = entry.second;
            if (!state.queue.empty()) {
                continue;
            }
            if (now - state.lastActivity >= options_.idleTtl) {
                toErase.push_back(state.streamId);
            }
        }

        for (const std::string& streamId : toErase) {
            removeRingIdLocked(streamId);
            streams_.erase(streamId);
        }
    }

    void removeRingIdLocked(const std::string& streamId) {
        auto it = std::find(ring_.begin(), ring_.end(), streamId);
        if (it == ring_.end()) {
            return;
        }

        const std::size_t index = static_cast<std::size_t>(std::distance(ring_.begin(), it));
        removeRingSlotLocked(index);
    }

    void removeRingSlotLocked(std::size_t index) {
        if (index >= ring_.size()) {
            return;
        }

        ring_.erase(ring_.begin() + static_cast<std::ptrdiff_t>(index));
        if (dispatchCursor_ > index) {
            --dispatchCursor_;
        }
        if (dispatchCursor_ >= ring_.size() && !ring_.empty()) {
            dispatchCursor_ = 0;
        }
    }

    static std::size_t saturatingAdd(std::size_t lhs, std::size_t rhs, std::size_t cap) {
        const std::size_t sum = lhs > cap - std::min(rhs, cap) ? cap : lhs + rhs;
        return std::min(sum, cap);
    }

    SchedulerOptions options_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, StreamState> streams_;
    std::vector<std::string> ring_;
    std::size_t dispatchCursor_ = 0;
    std::uint64_t nextSequence_ = 0;
    std::size_t totalQueuedBytes_ = 0;
    std::size_t totalQueuedChunks_ = 0;
};

} // namespace vibe

/*
This solves LLM streaming backpressure in C++ gateways where one slow SSE or WebSocket client can quietly build a huge in-memory queue and hurt every other stream. Built because local inference stacks in 2026 often mix vLLM, llama.cpp, TGI, and custom edge proxies, and the weak spot is usually downstream delivery rather than token generation. Use it when you need fair per-stream dispatch, bounded buffering, chunk coalescing, and safe eviction of lagging consumers. The trick: it uses deficit-based fair scheduling, stream-local queue limits, a global memory budget, and queue-age eviction so stale tokens do not pile up forever. Drop this into an inference proxy, realtime AI gateway, SSE fanout service, edge worker shim, or C++ model-serving sidecar when you need production-ready stream backpressure control, slow consumer handling, fair token delivery, and memory-safe AI response streaming.
*/
