#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

struct Config {
    std::size_t max_line_bytes = 512 * 1024;
    std::size_t max_value_chars = 2048;
    bool hash_redacted = true;
    bool pretty_stats = false;
};

struct Stats {
    std::uint64_t lines_seen = 0;
    std::uint64_t lines_emitted = 0;
    std::uint64_t line_too_large = 0;
    std::uint64_t json_keys_redacted = 0;
    std::uint64_t inline_tokens_redacted = 0;
    std::uint64_t high_entropy_redacted = 0;
    std::uint64_t values_truncated = 0;
};

constexpr std::array<std::string_view, 20> kSensitiveKeyFragments = {
    "api_key", "apikey", "authorization", "auth", "bearer", "token", "secret",
    "client_secret", "password", "passwd", "session", "cookie", "set-cookie",
    "private_key", "access_key", "refresh_token", "id_token", "webhook_secret",
    "connection_string", "dsn"
};

constexpr std::array<std::string_view, 8> kSuspiciousPrefixes = {
    "sk-", "ghp_", "github_pat_", "xoxb-", "xoxp-", "AIza", "AKIA", "eyJ"
};

std::string ToLower(std::string_view input) {
    std::string out;
    out.reserve(input.size());
    for (unsigned char ch : input) {
        out.push_back(static_cast<char>(std::tolower(ch)));
    }
    return out;
}

bool ContainsSensitiveKeyFragment(std::string_view key) {
    std::string lowered = ToLower(key);
    for (const auto fragment : kSensitiveKeyFragments) {
        if (lowered.find(fragment) != std::string::npos) {
            return true;
        }
    }
    return false;
}

bool IsLikelyBase64ish(char ch) {
    return std::isalnum(static_cast<unsigned char>(ch)) || ch == '+' || ch == '/' || ch == '_' || ch == '-' || ch == '=';
}

std::uint64_t Fnv1a64(std::string_view value) {
    std::uint64_t hash = 1469598103934665603ull;
    for (unsigned char ch : value) {
        hash ^= static_cast<std::uint64_t>(ch);
        hash *= 1099511628211ull;
    }
    return hash;
}

std::string Hex64(std::uint64_t value) {
    std::ostringstream out;
    out << std::hex << std::setfill('0') << std::setw(16) << value;
    return out.str();
}

std::string BuildRedaction(std::string_view reason, std::string_view original, const Config& config) {
    std::string out = "[REDACTED:";
    out += reason;
    if (config.hash_redacted) {
        out += ":fnv1a64=";
        out += Hex64(Fnv1a64(original));
    }
    out += "]";
    return out;
}

bool HasMixedClasses(std::string_view token) {
    bool has_lower = false;
    bool has_upper = false;
    bool has_digit = false;
    for (unsigned char ch : token) {
        has_lower |= std::islower(ch) != 0;
        has_upper |= std::isupper(ch) != 0;
        has_digit |= std::isdigit(ch) != 0;
    }
    return (has_lower && has_upper) || (has_lower && has_digit) || (has_upper && has_digit);
}

bool LooksHighEntropyToken(std::string_view token) {
    if (token.size() < 24) {
        return false;
    }
    for (const auto prefix : kSuspiciousPrefixes) {
        if (token.rfind(prefix, 0) == 0) {
            return true;
        }
    }

    std::size_t valid_chars = 0;
    std::array<bool, 256> seen{};
    for (unsigned char ch : token) {
        if (!IsLikelyBase64ish(static_cast<char>(ch))) {
            return false;
        }
        ++valid_chars;
        seen[ch] = true;
    }

    std::size_t distinct = 0;
    for (bool bit : seen) {
        distinct += bit ? 1u : 0u;
    }

    return valid_chars >= 24 && distinct >= 10 && HasMixedClasses(token);
}

std::string MaybeTruncateValue(std::string_view value, Stats& stats, const Config& config) {
    if (value.size() <= config.max_value_chars) {
        return std::string(value);
    }
    ++stats.values_truncated;
    std::string out(value.substr(0, config.max_value_chars));
    out += "...[TRUNCATED len=";
    out += std::to_string(value.size());
    out += "]";
    return out;
}

bool ParseJsonString(const std::string& input, std::size_t start, std::size_t& end, std::string& decoded) {
    if (start >= input.size() || input[start] != '"') {
        return false;
    }
    decoded.clear();
    bool escape = false;
    for (std::size_t i = start + 1; i < input.size(); ++i) {
        char ch = input[i];
        if (escape) {
            switch (ch) {
                case '"': decoded.push_back('"'); break;
                case '\\': decoded.push_back('\\'); break;
                case '/': decoded.push_back('/'); break;
                case 'b': decoded.push_back('\b'); break;
                case 'f': decoded.push_back('\f'); break;
                case 'n': decoded.push_back('\n'); break;
                case 'r': decoded.push_back('\r'); break;
                case 't': decoded.push_back('\t'); break;
                case 'u':
                    decoded.push_back('?');
                    break;
                default:
                    decoded.push_back(ch);
                    break;
            }
            escape = false;
            continue;
        }
        if (ch == '\\') {
            escape = true;
            continue;
        }
        if (ch == '"') {
            end = i + 1;
            return true;
        }
        decoded.push_back(ch);
    }
    return false;
}

std::string JsonEscape(std::string_view input) {
    std::string out;
    out.reserve(input.size() + 8);
    for (unsigned char ch : input) {
        switch (ch) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (ch < 0x20) {
                    std::ostringstream oss;
                    oss << "\\u" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(ch);
                    out += oss.str();
                } else {
                    out.push_back(static_cast<char>(ch));
                }
                break;
        }
    }
    return out;
}

std::string RedactInlineText(const std::string& input, Stats& stats, const Config& config) {
    std::string out;
    out.reserve(input.size());
    std::size_t i = 0;
    while (i < input.size()) {
        bool matched = false;
        for (const auto prefix : kSuspiciousPrefixes) {
            if (i + prefix.size() <= input.size() && input.compare(i, prefix.size(), prefix.data(), prefix.size()) == 0) {
                std::size_t j = i + prefix.size();
                while (j < input.size() && IsLikelyBase64ish(input[j])) {
                    ++j;
                }
                if (j - i >= 12) {
                    out += BuildRedaction("token", std::string_view(input).substr(i, j - i), config);
                    ++stats.inline_tokens_redacted;
                    i = j;
                    matched = true;
                    break;
                }
            }
        }
        if (matched) {
            continue;
        }

        if (IsLikelyBase64ish(input[i])) {
            std::size_t j = i;
            while (j < input.size() && IsLikelyBase64ish(input[j])) {
                ++j;
            }
            std::string_view token(input.data() + i, j - i);
            if (LooksHighEntropyToken(token)) {
                out += BuildRedaction("entropy", token, config);
                ++stats.high_entropy_redacted;
            } else {
                out.append(token);
            }
            i = j;
            continue;
        }

        out.push_back(input[i]);
        ++i;
    }
    return out;
}

std::string ProcessJsonLikeLine(const std::string& line, Stats& stats, const Config& config) {
    std::string out;
    out.reserve(line.size() + 32);

    std::vector<std::string> key_stack;
    std::size_t i = 0;
    while (i < line.size()) {
        if (line[i] == '"') {
            std::size_t end = i;
            std::string decoded;
            if (!ParseJsonString(line, i, end, decoded)) {
                return RedactInlineText(line, stats, config);
            }

            std::size_t lookahead = end;
            while (lookahead < line.size() && std::isspace(static_cast<unsigned char>(line[lookahead]))) {
                ++lookahead;
            }

            bool is_key = lookahead < line.size() && line[lookahead] == ':';
            if (is_key) {
                key_stack.push_back(decoded);
                out.append(line, i, end - i);
                i = end;
                continue;
            }

            bool sensitive_context = !key_stack.empty() && ContainsSensitiveKeyFragment(key_stack.back());
            if (sensitive_context) {
                std::string replacement = BuildRedaction(key_stack.back(), decoded, config);
                out.push_back('"');
                out += JsonEscape(replacement);
                out.push_back('"');
                ++stats.json_keys_redacted;
            } else {
                std::string candidate = MaybeTruncateValue(decoded, stats, config);
                candidate = RedactInlineText(candidate, stats, config);
                out.push_back('"');
                out += JsonEscape(candidate);
                out.push_back('"');
            }
            i = end;
            continue;
        }

        if (!key_stack.empty()) {
            if (line[i] == ',') {
                key_stack.pop_back();
            } else if (line[i] == '}' || line[i] == ']') {
                key_stack.pop_back();
            }
        }

        out.push_back(line[i]);
        ++i;
    }

    return out;
}

bool LooksJsonLike(std::string_view line) {
    for (char ch : line) {
        if (std::isspace(static_cast<unsigned char>(ch))) {
            continue;
        }
        return ch == '{' || ch == '[' || ch == '"';
    }
    return false;
}

void PrintUsage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << " [--input PATH] [--output PATH] [--max-line-bytes N]"
        << " [--max-value-chars N] [--no-hash] [--stats]\n";
}

bool ParseSizeArg(std::string_view value, std::size_t& out) {
    unsigned long long parsed = 0;
    auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
        return false;
    }
    if (parsed > std::numeric_limits<std::size_t>::max()) {
        return false;
    }
    out = static_cast<std::size_t>(parsed);
    return true;
}

int Run(int argc, char** argv) {
    Config config;
    std::optional<std::string> input_path;
    std::optional<std::string> output_path;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        auto require_value = [&](std::string_view name) -> std::string_view {
            if (i + 1 >= argc) {
                throw std::runtime_error("missing value for " + std::string(name));
            }
            return argv[++i];
        };

        if (arg == "--input") {
            input_path = std::string(require_value(arg));
        } else if (arg == "--output") {
            output_path = std::string(require_value(arg));
        } else if (arg == "--max-line-bytes") {
            if (!ParseSizeArg(require_value(arg), config.max_line_bytes)) {
                throw std::runtime_error("invalid --max-line-bytes");
            }
        } else if (arg == "--max-value-chars") {
            if (!ParseSizeArg(require_value(arg), config.max_value_chars)) {
                throw std::runtime_error("invalid --max-value-chars");
            }
        } else if (arg == "--no-hash") {
            config.hash_redacted = false;
        } else if (arg == "--stats") {
            config.pretty_stats = true;
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return 0;
        } else {
            throw std::runtime_error("unknown argument: " + std::string(arg));
        }
    }

    std::ifstream file_in;
    std::ofstream file_out;
    std::istream* in = &std::cin;
    std::ostream* out = &std::cout;

    if (input_path) {
        file_in.open(*input_path, std::ios::binary);
        if (!file_in) {
            throw std::runtime_error("failed to open input: " + *input_path);
        }
        in = &file_in;
    }
    if (output_path) {
        file_out.open(*output_path, std::ios::binary | std::ios::trunc);
        if (!file_out) {
            throw std::runtime_error("failed to open output: " + *output_path);
        }
        out = &file_out;
    }

    Stats stats;
    std::string line;
    while (std::getline(*in, line)) {
        ++stats.lines_seen;
        if (line.size() > config.max_line_bytes) {
            ++stats.line_too_large;
            *out << "{\"redaction_error\":\"line_too_large\",\"bytes\":" << line.size() << "}\n";
            ++stats.lines_emitted;
            continue;
        }

        std::string sanitized = LooksJsonLike(line)
            ? ProcessJsonLikeLine(line, stats, config)
            : RedactInlineText(line, stats, config);

        *out << sanitized << '\n';
        ++stats.lines_emitted;
    }

    if (!*out) {
        throw std::runtime_error("write failed");
    }

    if (config.pretty_stats) {
        std::cerr
            << "lines_seen=" << stats.lines_seen
            << " lines_emitted=" << stats.lines_emitted
            << " line_too_large=" << stats.line_too_large
            << " json_keys_redacted=" << stats.json_keys_redacted
            << " inline_tokens_redacted=" << stats.inline_tokens_redacted
            << " high_entropy_redacted=" << stats.high_entropy_redacted
            << " values_truncated=" << stats.values_truncated
            << '\n';
    }

    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        return Run(argc, argv);
    } catch (const std::exception& ex) {
        std::cerr << "JsonlSecretFirewall error: " << ex.what() << '\n';
        return 1;
    }
}

/*
This solves secret leaks and oversized payload leaks in JSONL logs, trace exports, and AI gateway stream archives. Built because modern LLM apps, MCP servers, CI jobs, and edge workers still dump bearer tokens, webhook secrets, cookies, and giant prompt blobs into logs when something fails at 2 AM. Use it when you need a fast single-file C++ log scrubber before shipping data to S3, ClickHouse, BigQuery, Datadog, Loki, or any internal data pipeline. The trick: it handles JSON-looking lines with key-aware redaction, also catches inline token patterns and high-entropy blobs in plain text, and keeps output line-oriented so downstream tools do not break. Drop this into a security sidecar, observability agent, build pipeline, ingest worker, or incident cleanup path when you want practical secret redaction for JSONL, NDJSON, AI traces, SSE archives, and developer logs without pulling in a large dependency stack.
*/
