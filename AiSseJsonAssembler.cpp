#include <algorithm>
#include <cctype>
#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace vibe::streaming {

enum class JsonValueKind {
    kObject,
    kArray,
    kString,
    kNumber,
    kBoolean,
    kNull
};

struct CompletedJsonValue {
    std::string json;
    JsonValueKind kind = JsonValueKind::kObject;
    bool from_sse = false;
};

struct AssemblerStats {
    std::uint64_t completed_values = 0;
    std::uint64_t discarded_noise_bytes = 0;
    std::uint64_t discarded_invalid_candidates = 0;
    std::uint64_t sse_events_seen = 0;
    std::uint64_t sse_done_markers = 0;
    std::size_t max_depth_observed = 0;
    std::size_t buffered_json_bytes = 0;
    std::size_t buffered_transport_bytes = 0;
};

struct AssemblerLimits {
    std::size_t max_buffered_transport_bytes = 256 * 1024;
    std::size_t max_buffered_json_bytes = 2 * 1024 * 1024;
    std::size_t max_json_depth = 128;
    std::size_t max_single_json_bytes = 1 * 1024 * 1024;
    std::size_t max_completed_queue = 256;
    bool accept_top_level_primitives = true;
};

class AssemblerError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

namespace detail {

inline bool IsWhitespace(char ch) {
    switch (ch) {
        case ' ':
        case '\n':
        case '\r':
        case '\t':
            return true;
        default:
            return false;
    }
}

inline std::size_t SkipWhitespace(std::string_view input, std::size_t index) {
    while (index < input.size() && IsWhitespace(input[index])) {
        ++index;
    }
    return index;
}

inline std::string_view Trim(std::string_view input) {
    const std::size_t start = SkipWhitespace(input, 0);
    std::size_t end = input.size();
    while (end > start && IsWhitespace(input[end - 1])) {
        --end;
    }
    return input.substr(start, end - start);
}

inline std::string_view TrimLeft(std::string_view input) {
    return input.substr(SkipWhitespace(input, 0));
}

class Utf8Validator {
public:
    void Consume(std::string_view chunk) {
        for (const unsigned char byte : chunk) {
            ConsumeByte(byte);
        }
    }

    bool IsBoundary() const {
        return expected_continuations_ == 0;
    }

private:
    void ConsumeByte(unsigned char byte) {
        if (expected_continuations_ == 0) {
            if ((byte & 0x80u) == 0) {
                return;
            }
            if ((byte & 0xE0u) == 0xC0u) {
                expected_continuations_ = 1;
                if (byte < 0xC2u) {
                    throw AssemblerError("invalid UTF-8 leading byte");
                }
                return;
            }
            if ((byte & 0xF0u) == 0xE0u) {
                expected_continuations_ = 2;
                return;
            }
            if ((byte & 0xF8u) == 0xF0u) {
                if (byte > 0xF4u) {
                    throw AssemblerError("invalid UTF-8 leading byte");
                }
                expected_continuations_ = 3;
                return;
            }
            throw AssemblerError("invalid UTF-8 leading byte");
        }

        if ((byte & 0xC0u) != 0x80u) {
            throw AssemblerError("invalid UTF-8 continuation byte");
        }

        --expected_continuations_;
    }

    int expected_continuations_ = 0;
};

enum class ParseStatus {
    kComplete,
    kIncomplete,
    kError
};

struct ParseResult {
    ParseStatus status = ParseStatus::kError;
    std::size_t next = 0;
    JsonValueKind kind = JsonValueKind::kObject;
    std::size_t max_depth_observed = 0;
    std::string error;
};

class JsonParser {
public:
    JsonParser(std::string_view input, const AssemblerLimits& limits)
        : input_(input), limits_(limits) {}

    ParseResult ParseSingleValue(std::size_t start) {
        const std::size_t begin = SkipWhitespace(input_, start);
        if (begin >= input_.size()) {
            return {.status = ParseStatus::kIncomplete, .next = begin};
        }

        std::size_t cursor = begin;
        const ParseResult value = ParseValue(cursor, 0);
        if (value.status != ParseStatus::kComplete) {
            return value;
        }

        cursor = SkipWhitespace(input_, cursor);
        return {
            .status = ParseStatus::kComplete,
            .next = cursor,
            .kind = value.kind,
            .max_depth_observed = max_depth_observed_
        };
    }

private:
    ParseResult ParseValue(std::size_t& index, std::size_t depth) {
        if (depth > limits_.max_json_depth) {
            return Error("JSON depth limit exceeded", index);
        }
        max_depth_observed_ = std::max(max_depth_observed_, depth);

        if (index >= input_.size()) {
            return {.status = ParseStatus::kIncomplete, .next = index};
        }

        const char ch = input_[index];
        if (ch == '{') {
            return ParseObject(index, depth + 1);
        }
        if (ch == '[') {
            return ParseArray(index, depth + 1);
        }
        if (ch == '"') {
            return ParseStringValue(index);
        }
        if (ch == '-' || (ch >= '0' && ch <= '9')) {
            return ParseNumber(index);
        }
        if (ch == 't' || ch == 'f') {
            return ParseBoolean(index);
        }
        if (ch == 'n') {
            return ParseNull(index);
        }

        return Error("unexpected JSON token", index);
    }

    ParseResult ParseObject(std::size_t& index, std::size_t depth) {
        ++index;
        index = SkipWhitespace(input_, index);
        if (index >= input_.size()) {
            return {.status = ParseStatus::kIncomplete, .next = index};
        }
        if (input_[index] == '}') {
            ++index;
            return {.status = ParseStatus::kComplete, .next = index, .kind = JsonValueKind::kObject};
        }

        for (;;) {
            ParseResult key = ParseStringValue(index);
            if (key.status != ParseStatus::kComplete) {
                return key;
            }
            index = SkipWhitespace(input_, index);
            if (index >= input_.size()) {
                return {.status = ParseStatus::kIncomplete, .next = index};
            }
            if (input_[index] != ':') {
                return Error("expected ':' after object key", index);
            }
            ++index;
            index = SkipWhitespace(input_, index);
            ParseResult value = ParseValue(index, depth);
            if (value.status != ParseStatus::kComplete) {
                return value;
            }

            index = SkipWhitespace(input_, index);
            if (index >= input_.size()) {
                return {.status = ParseStatus::kIncomplete, .next = index};
            }
            const char ch = input_[index];
            if (ch == '}') {
                ++index;
                return {.status = ParseStatus::kComplete, .next = index, .kind = JsonValueKind::kObject};
            }
            if (ch != ',') {
                return Error("expected ',' or '}' inside object", index);
            }
            ++index;
            index = SkipWhitespace(input_, index);
            if (index >= input_.size()) {
                return {.status = ParseStatus::kIncomplete, .next = index};
            }
        }
    }

    ParseResult ParseArray(std::size_t& index, std::size_t depth) {
        ++index;
        index = SkipWhitespace(input_, index);
        if (index >= input_.size()) {
            return {.status = ParseStatus::kIncomplete, .next = index};
        }
        if (input_[index] == ']') {
            ++index;
            return {.status = ParseStatus::kComplete, .next = index, .kind = JsonValueKind::kArray};
        }

        for (;;) {
            ParseResult value = ParseValue(index, depth);
            if (value.status != ParseStatus::kComplete) {
                return value;
            }

            index = SkipWhitespace(input_, index);
            if (index >= input_.size()) {
                return {.status = ParseStatus::kIncomplete, .next = index};
            }
            const char ch = input_[index];
            if (ch == ']') {
                ++index;
                return {.status = ParseStatus::kComplete, .next = index, .kind = JsonValueKind::kArray};
            }
            if (ch != ',') {
                return Error("expected ',' or ']' inside array", index);
            }
            ++index;
            index = SkipWhitespace(input_, index);
            if (index >= input_.size()) {
                return {.status = ParseStatus::kIncomplete, .next = index};
            }
        }
    }

    ParseResult ParseStringValue(std::size_t& index) {
        ParseResult string_result = ParseString(index);
        if (string_result.status != ParseStatus::kComplete) {
            return string_result;
        }
        string_result.kind = JsonValueKind::kString;
        return string_result;
    }

    ParseResult ParseString(std::size_t& index) {
        if (index >= input_.size() || input_[index] != '"') {
            return Error("expected string", index);
        }
        ++index;

        while (index < input_.size()) {
            const unsigned char ch = static_cast<unsigned char>(input_[index]);
            if (ch < 0x20u) {
                return Error("control characters are not allowed in JSON strings", index);
            }
            if (ch == '"') {
                ++index;
                return {.status = ParseStatus::kComplete, .next = index};
            }
            if (ch == '\\') {
                ++index;
                if (index >= input_.size()) {
                    return {.status = ParseStatus::kIncomplete, .next = index};
                }
                const char escaped = input_[index];
                switch (escaped) {
                    case '"':
                    case '\\':
                    case '/':
                    case 'b':
                    case 'f':
                    case 'n':
                    case 'r':
                    case 't':
                        ++index;
                        break;
                    case 'u':
                        ++index;
                        for (int count = 0; count < 4; ++count) {
                            if (index >= input_.size()) {
                                return {.status = ParseStatus::kIncomplete, .next = index};
                            }
                            if (!std::isxdigit(static_cast<unsigned char>(input_[index]))) {
                                return Error("invalid unicode escape in JSON string", index);
                            }
                            ++index;
                        }
                        break;
                    default:
                        return Error("invalid escape sequence in JSON string", index);
                }
                continue;
            }
            ++index;
        }

        return {.status = ParseStatus::kIncomplete, .next = index};
    }

    ParseResult ParseNumber(std::size_t& index) {
        const std::size_t start = index;
        if (input_[index] == '-') {
            ++index;
            if (index >= input_.size()) {
                return {.status = ParseStatus::kIncomplete, .next = index};
            }
        }

        if (input_[index] == '0') {
            ++index;
        } else if (input_[index] >= '1' && input_[index] <= '9') {
            while (index < input_.size() && input_[index] >= '0' && input_[index] <= '9') {
                ++index;
            }
        } else {
            return Error("invalid number", index);
        }

        if (index < input_.size() && input_[index] == '.') {
            ++index;
            if (index >= input_.size()) {
                return {.status = ParseStatus::kIncomplete, .next = index};
            }
            if (input_[index] < '0' || input_[index] > '9') {
                return Error("invalid fraction in number", index);
            }
            while (index < input_.size() && input_[index] >= '0' && input_[index] <= '9') {
                ++index;
            }
        }

        if (index < input_.size() && (input_[index] == 'e' || input_[index] == 'E')) {
            ++index;
            if (index >= input_.size()) {
                return {.status = ParseStatus::kIncomplete, .next = index};
            }
            if (input_[index] == '+' || input_[index] == '-') {
                ++index;
                if (index >= input_.size()) {
                    return {.status = ParseStatus::kIncomplete, .next = index};
                }
            }
            if (input_[index] < '0' || input_[index] > '9') {
                return Error("invalid exponent in number", index);
            }
            while (index < input_.size() && input_[index] >= '0' && input_[index] <= '9') {
                ++index;
            }
        }

        if (index == start) {
            return Error("invalid number", index);
        }
        return {.status = ParseStatus::kComplete, .next = index, .kind = JsonValueKind::kNumber};
    }

    ParseResult ParseBoolean(std::size_t& index) {
        if (input_.compare(index, 4, "true") == 0) {
            index += 4;
            return {.status = ParseStatus::kComplete, .next = index, .kind = JsonValueKind::kBoolean};
        }
        if (input_.compare(index, 5, "false") == 0) {
            index += 5;
            return {.status = ParseStatus::kComplete, .next = index, .kind = JsonValueKind::kBoolean};
        }
        if (input_.size() - index < 5 && std::string_view("false").substr(0, input_.size() - index) == input_.substr(index)) {
            return {.status = ParseStatus::kIncomplete, .next = input_.size()};
        }
        if (input_.size() - index < 4 && std::string_view("true").substr(0, input_.size() - index) == input_.substr(index)) {
            return {.status = ParseStatus::kIncomplete, .next = input_.size()};
        }
        return Error("invalid boolean literal", index);
    }

    ParseResult ParseNull(std::size_t& index) {
        if (input_.compare(index, 4, "null") == 0) {
            index += 4;
            return {.status = ParseStatus::kComplete, .next = index, .kind = JsonValueKind::kNull};
        }
        if (input_.size() - index < 4 && std::string_view("null").substr(0, input_.size() - index) == input_.substr(index)) {
            return {.status = ParseStatus::kIncomplete, .next = input_.size()};
        }
        return Error("invalid null literal", index);
    }

    ParseResult Error(std::string message, std::size_t index) const {
        return {.status = ParseStatus::kError, .next = index, .error = std::move(message)};
    }

    std::string_view input_;
    const AssemblerLimits& limits_;
    std::size_t max_depth_observed_ = 0;
};

inline bool IsLikelyJsonStart(char ch, bool accept_top_level_primitives) {
    if (ch == '{' || ch == '[') {
        return true;
    }
    if (!accept_top_level_primitives) {
        return false;
    }
    return ch == '"' || ch == '-' || ch == 't' || ch == 'f' || ch == 'n' || (ch >= '0' && ch <= '9');
}

inline std::size_t FindNextJsonStart(std::string_view input, std::size_t from, bool accept_top_level_primitives) {
    for (std::size_t index = from; index < input.size(); ++index) {
        if (IsLikelyJsonStart(input[index], accept_top_level_primitives)) {
            return index;
        }
    }
    return std::string_view::npos;
}

}  // namespace detail

class JsonFragmentAssembler {
public:
    explicit JsonFragmentAssembler(AssemblerLimits limits = {})
        : limits_(limits) {}

    std::vector<CompletedJsonValue> Feed(std::string_view chunk, bool from_sse) {
        utf8_.Consume(chunk);
        buffer_.append(chunk.data(), chunk.size());
        EnforceBufferedJsonLimit();

        std::vector<CompletedJsonValue> out;
        for (;;) {
            std::size_t start = detail::FindNextJsonStart(buffer_, 0, limits_.accept_top_level_primitives);
            if (start == std::string_view::npos) {
                stats_.discarded_noise_bytes += buffer_.size();
                buffer_.clear();
                break;
            }

            if (start > 0) {
                stats_.discarded_noise_bytes += start;
                buffer_.erase(0, start);
            }

            detail::JsonParser parser(buffer_, limits_);
            detail::ParseResult result = parser.ParseSingleValue(0);
            stats_.max_depth_observed = std::max(stats_.max_depth_observed, result.max_depth_observed);

            if (result.status == detail::ParseStatus::kComplete) {
                if (result.next > limits_.max_single_json_bytes) {
                    throw AssemblerError("single JSON payload exceeded configured limit");
                }

                out.push_back({
                    .json = buffer_.substr(0, result.next),
                    .kind = result.kind,
                    .from_sse = from_sse
                });
                buffer_.erase(0, result.next);
                ++stats_.completed_values;
                if (out.size() > limits_.max_completed_queue) {
                    throw AssemblerError("completed JSON queue exceeded configured limit");
                }
                continue;
            }

            if (result.status == detail::ParseStatus::kIncomplete) {
                if (buffer_.size() > limits_.max_single_json_bytes) {
                    throw AssemblerError("incomplete JSON payload exceeded configured limit");
                }
                break;
            }

            ++stats_.discarded_invalid_candidates;
            buffer_.erase(0, 1);
        }

        stats_.buffered_json_bytes = buffer_.size();
        return out;
    }

    std::string_view PendingJson() const {
        return buffer_;
    }

    const AssemblerStats& stats() const {
        return stats_;
    }

private:
    void EnforceBufferedJsonLimit() {
        if (buffer_.size() > limits_.max_buffered_json_bytes) {
            throw AssemblerError("buffered JSON exceeded configured limit");
        }
    }

    AssemblerLimits limits_;
    detail::Utf8Validator utf8_;
    std::string buffer_;
    AssemblerStats stats_;
};

class AiSseJsonAssembler {
public:
    explicit AiSseJsonAssembler(AssemblerLimits limits = {})
        : limits_(limits),
          json_assembler_(limits) {}

    std::vector<CompletedJsonValue> FeedTransportBytes(std::string_view chunk) {
        transport_utf8_.Consume(chunk);
        transport_buffer_.append(chunk.data(), chunk.size());
        EnforceTransportLimit();

        std::vector<CompletedJsonValue> out;
        std::size_t line_start = 0;
        while (true) {
            const std::size_t newline = transport_buffer_.find('\n', line_start);
            if (newline == std::string::npos) {
                transport_buffer_.erase(0, line_start);
                stats_.buffered_transport_bytes = transport_buffer_.size();
                return out;
            }

            std::string_view line(transport_buffer_.data() + line_start, newline - line_start);
            if (!line.empty() && line.back() == '\r') {
                line.remove_suffix(1);
            }
            line_start = newline + 1;

            std::vector<CompletedJsonValue> line_out = ConsumeSseLine(line);
            out.insert(out.end(),
                       std::make_move_iterator(line_out.begin()),
                       std::make_move_iterator(line_out.end()));
        }
    }

    std::vector<CompletedJsonValue> FeedJsonText(std::string_view chunk) {
        std::vector<CompletedJsonValue> out = json_assembler_.Feed(chunk, false);
        SyncStats();
        return out;
    }

    bool done() const {
        return done_;
    }

    std::string_view PendingTransportBytes() const {
        return transport_buffer_;
    }

    std::string_view PendingSseEventData() const {
        return event_data_;
    }

    std::string_view PendingJson() const {
        return json_assembler_.PendingJson();
    }

    AssemblerStats stats() const {
        return stats_;
    }

private:
    std::vector<CompletedJsonValue> ConsumeSseLine(std::string_view line) {
        std::vector<CompletedJsonValue> out;
        if (line.empty()) {
            DispatchEvent(out);
            return out;
        }

        if (line[0] == ':') {
            return out;
        }

        const std::size_t colon = line.find(':');
        std::string_view field = colon == std::string_view::npos ? line : line.substr(0, colon);
        std::string_view value = colon == std::string_view::npos ? std::string_view() : line.substr(colon + 1);
        if (!value.empty() && value.front() == ' ') {
            value.remove_prefix(1);
        }

        if (field == "data") {
            if (!event_data_.empty()) {
                event_data_.push_back('\n');
            }
            event_data_.append(value.data(), value.size());
            return out;
        }

        if (field == "event") {
            event_name_.assign(value.data(), value.size());
            return out;
        }

        if (field == "id" || field == "retry") {
            return out;
        }

        if (colon == std::string_view::npos && detail::Trim(line) == "[DONE]") {
            done_ = true;
            ++stats_.sse_done_markers;
            return out;
        }

        return out;
    }

    void DispatchEvent(std::vector<CompletedJsonValue>& out) {
        if (event_data_.empty()) {
            event_name_.clear();
            return;
        }

        ++stats_.sse_events_seen;

        const std::string_view payload = detail::Trim(event_data_);
        if (payload == "[DONE]") {
            done_ = true;
            ++stats_.sse_done_markers;
            event_data_.clear();
            event_name_.clear();
            return;
        }

        std::vector<CompletedJsonValue> event_out = json_assembler_.Feed(payload, true);
        out.insert(out.end(),
                   std::make_move_iterator(event_out.begin()),
                   std::make_move_iterator(event_out.end()));

        event_data_.clear();
        event_name_.clear();
        SyncStats();
    }

    void EnforceTransportLimit() {
        if (transport_buffer_.size() > limits_.max_buffered_transport_bytes) {
            throw AssemblerError("buffered transport bytes exceeded configured limit");
        }
    }

    void SyncStats() {
        const AssemblerStats& json_stats = json_assembler_.stats();
        stats_.completed_values = json_stats.completed_values;
        stats_.discarded_noise_bytes = json_stats.discarded_noise_bytes;
        stats_.discarded_invalid_candidates = json_stats.discarded_invalid_candidates;
        stats_.max_depth_observed = json_stats.max_depth_observed;
        stats_.buffered_json_bytes = json_stats.buffered_json_bytes;
        stats_.buffered_transport_bytes = transport_buffer_.size();
    }

    AssemblerLimits limits_;
    detail::Utf8Validator transport_utf8_;
    std::string transport_buffer_;
    std::string event_name_;
    std::string event_data_;
    JsonFragmentAssembler json_assembler_;
    bool done_ = false;
    AssemblerStats stats_;
};

}  // namespace vibe::streaming

/*
This solves the annoying April 2026 problem where AI gateways, edge workers, desktop copilots, and inference proxies receive tool-call arguments and structured outputs as broken-up Server-Sent Events instead of one clean JSON blob. Built because modern model APIs stream partial objects, arrays, and literals across many chunks, and production systems still need hard limits, UTF-8 safety, and deterministic completion before they can execute tools or persist results. Use it when you are wiring OpenAI-style, Anthropic-style, or homegrown SSE streams into a C++ service and you need a reusable parser that can survive noise before the JSON, incomplete frames, blank-line dispatch, and the common [DONE] sentinel. The trick: it separates transport assembly from JSON assembly, validates bytes as they arrive, trims only the noise you do not want, and keeps strict caps so one bad stream cannot quietly eat memory. Drop this into an inference gateway, agent runtime, streaming adapter, reverse proxy, embedded edge collector, or a desktop AI app that wants to turn fragmented event payloads into complete JSON values you can trust before acting on them.
*/