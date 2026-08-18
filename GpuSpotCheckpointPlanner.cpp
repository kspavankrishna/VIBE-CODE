#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

struct ParseError {
    std::size_t line = 0;
    std::string message;
};

struct Node {
    std::string id;
    std::string region = "unknown";
    std::string pool = "spot";
    int gpus = 0;
    int free_gpus = 0;
    double price_per_gpu_hour = 0.0;
    double eviction_rate_per_hour = 0.0;
    double checkpoint_write_mbps = 400.0;
    double watts_per_gpu = 650.0;
    double carbon_g_per_kwh = 450.0;
    double scratch_gb = 100.0;
    bool spot = true;
};

struct Job {
    std::string id;
    int gpus = 1;
    int business_priority = 3;
    double gpu_hours = 0.0;
    double checkpoint_gb = 1.0;
    double max_checkpoint_minutes = 20.0;
    double queue_age_minutes = 0.0;
    double slo_minutes = 240.0;
    double interrupt_cost_minutes = 15.0;
    double max_cost_usd = std::numeric_limits<double>::infinity();
    double min_checkpoint_minutes = 2.0;
};

struct Candidate {
    Node node;
    bool has_node = false;
    double checkpoint_minutes = 0.0;
    double checkpoint_write_minutes = 0.0;
    double expected_overhead_minutes = 0.0;
    double eviction_probability = 0.0;
    double expected_interrupts = 0.0;
    double cost_usd = 0.0;
    double carbon_kg = 0.0;
    double finish_minutes = 0.0;
    double risk_limit = 0.0;
    double score = 0.0;
    bool feasible = false;
    std::vector<std::string> reasons;
};

struct PlanRow {
    Job job;
    std::optional<Candidate> selected;
    std::vector<Candidate> rejected;
    std::string action;
};

struct Options {
    std::string input_path;
    std::string format = "table";
    std::string fail_on = "none";
    double max_spot_risk = 0.25;
    bool prefer_low_carbon = false;
    bool self_test = false;
};

std::string trim(const std::string& value) {
    std::size_t begin = 0;
    while (begin < value.size() && std::isspace(static_cast<unsigned char>(value[begin])) != 0) {
        ++begin;
    }
    std::size_t end = value.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
        --end;
    }
    return value.substr(begin, end - begin);
}

std::string lower_copy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

bool starts_with(const std::string& value, const std::string& prefix) {
    return value.size() >= prefix.size() && value.compare(0, prefix.size(), prefix) == 0;
}

bool is_finite_number(double value) {
    return std::isfinite(value) != 0;
}

std::string json_escape(const std::string& value) {
    std::ostringstream out;
    for (char ch : value) {
        switch (ch) {
            case '\\':
                out << "\\\\";
                break;
            case '"':
                out << "\\\"";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\r':
                out << "\\r";
                break;
            case '\t':
                out << "\\t";
                break;
            default:
                if (static_cast<unsigned char>(ch) < 0x20U) {
                    out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                        << static_cast<int>(static_cast<unsigned char>(ch)) << std::dec << std::setfill(' ');
                } else {
                    out << ch;
                }
                break;
        }
    }
    return out.str();
}

std::string number_to_string(double value, int places) {
    if (!is_finite_number(value)) {
        return "unbounded";
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(places) << value;
    return out.str();
}

double parse_double(const std::map<std::string, std::string>& fields,
                    const std::string& key,
                    double fallback) {
    const auto it = fields.find(key);
    if (it == fields.end() || trim(it->second).empty()) {
        return fallback;
    }
    try {
        std::size_t consumed = 0;
        const double parsed = std::stod(trim(it->second), &consumed);
        if (consumed != trim(it->second).size()) {
            throw std::invalid_argument("trailing characters");
        }
        return parsed;
    } catch (const std::exception&) {
        throw std::runtime_error("field '" + key + "' is not a valid number: " + it->second);
    }
}

int parse_int(const std::map<std::string, std::string>& fields,
              const std::string& key,
              int fallback) {
    const double value = parse_double(fields, key, static_cast<double>(fallback));
    if (std::floor(value) != value) {
        throw std::runtime_error("field '" + key + "' must be an integer");
    }
    if (value < static_cast<double>(std::numeric_limits<int>::min()) ||
        value > static_cast<double>(std::numeric_limits<int>::max())) {
        throw std::runtime_error("field '" + key + "' is outside int range");
    }
    return static_cast<int>(value);
}

bool parse_bool_text(const std::string& text, bool fallback) {
    const std::string value = lower_copy(trim(text));
    if (value.empty()) {
        return fallback;
    }
    if (value == "true" || value == "yes" || value == "1" || value == "spot") {
        return true;
    }
    if (value == "false" || value == "no" || value == "0" || value == "on-demand" || value == "ondemand") {
        return false;
    }
    throw std::runtime_error("invalid boolean value: " + text);
}

bool parse_bool(const std::map<std::string, std::string>& fields,
                const std::string& key,
                bool fallback) {
    const auto it = fields.find(key);
    if (it == fields.end()) {
        return fallback;
    }
    return parse_bool_text(it->second, fallback);
}

std::string field_or(const std::map<std::string, std::string>& fields,
                     const std::string& key,
                     const std::string& fallback) {
    const auto it = fields.find(key);
    if (it == fields.end() || trim(it->second).empty()) {
        return fallback;
    }
    return trim(it->second);
}

std::string read_quoted(const std::string& text, std::size_t& pos) {
    if (pos >= text.size() || text[pos] != '"') {
        throw std::runtime_error("expected quoted string");
    }
    ++pos;
    std::string result;
    while (pos < text.size()) {
        const char ch = text[pos++];
        if (ch == '"') {
            return result;
        }
        if (ch == '\\') {
            if (pos >= text.size()) {
                throw std::runtime_error("dangling JSON escape");
            }
            const char escaped = text[pos++];
            switch (escaped) {
                case '"':
                case '\\':
                case '/':
                    result.push_back(escaped);
                    break;
                case 'b':
                    result.push_back('\b');
                    break;
                case 'f':
                    result.push_back('\f');
                    break;
                case 'n':
                    result.push_back('\n');
                    break;
                case 'r':
                    result.push_back('\r');
                    break;
                case 't':
                    result.push_back('\t');
                    break;
                default:
                    throw std::runtime_error("unsupported JSON escape");
            }
        } else {
            result.push_back(ch);
        }
    }
    throw std::runtime_error("unterminated quoted string");
}

void skip_ws(const std::string& text, std::size_t& pos) {
    while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos])) != 0) {
        ++pos;
    }
}

std::map<std::string, std::string> parse_json_object(const std::string& text) {
    std::size_t pos = 0;
    skip_ws(text, pos);
    if (pos >= text.size() || text[pos] != '{') {
        throw std::runtime_error("expected JSON object");
    }
    ++pos;
    std::map<std::string, std::string> fields;
    skip_ws(text, pos);
    if (pos < text.size() && text[pos] == '}') {
        return fields;
    }
    while (pos < text.size()) {
        skip_ws(text, pos);
        const std::string key = lower_copy(read_quoted(text, pos));
        skip_ws(text, pos);
        if (pos >= text.size() || text[pos] != ':') {
            throw std::runtime_error("expected ':' after JSON key");
        }
        ++pos;
        skip_ws(text, pos);
        std::string value;
        if (pos < text.size() && text[pos] == '"') {
            value = read_quoted(text, pos);
        } else {
            const std::size_t begin = pos;
            while (pos < text.size() && text[pos] != ',' && text[pos] != '}') {
                ++pos;
            }
            value = trim(text.substr(begin, pos - begin));
        }
        fields[key] = value;
        skip_ws(text, pos);
        if (pos < text.size() && text[pos] == ',') {
            ++pos;
            continue;
        }
        if (pos < text.size() && text[pos] == '}') {
            ++pos;
            skip_ws(text, pos);
            if (pos != text.size()) {
                throw std::runtime_error("trailing content after JSON object");
            }
            return fields;
        }
        throw std::runtime_error("expected ',' or '}' in JSON object");
    }
    throw std::runtime_error("unterminated JSON object");
}

std::vector<std::string> split_unquoted(const std::string& text) {
    std::vector<std::string> parts;
    std::string current;
    bool in_quotes = false;
    char quote = '\0';
    for (char ch : text) {
        if ((ch == '"' || ch == '\'') && (quote == '\0' || quote == ch)) {
            in_quotes = !in_quotes;
            quote = in_quotes ? ch : '\0';
            current.push_back(ch);
            continue;
        }
        if (!in_quotes && (ch == ',' || std::isspace(static_cast<unsigned char>(ch)) != 0)) {
            if (!trim(current).empty()) {
                parts.push_back(trim(current));
            }
            current.clear();
        } else {
            current.push_back(ch);
        }
    }
    if (!trim(current).empty()) {
        parts.push_back(trim(current));
    }
    return parts;
}

std::string unquote_token(std::string value) {
    value = trim(value);
    if (value.size() >= 2 &&
        ((value.front() == '"' && value.back() == '"') ||
         (value.front() == '\'' && value.back() == '\''))) {
        return value.substr(1, value.size() - 2);
    }
    return value;
}

std::map<std::string, std::string> parse_key_values(const std::string& text) {
    std::map<std::string, std::string> fields;
    for (const std::string& part : split_unquoted(text)) {
        const std::size_t eq = part.find('=');
        if (eq == std::string::npos || eq == 0) {
            throw std::runtime_error("expected key=value token, got: " + part);
        }
        const std::string key = lower_copy(trim(part.substr(0, eq)));
        const std::string value = unquote_token(part.substr(eq + 1));
        fields[key] = value;
    }
    return fields;
}

std::map<std::string, std::string> parse_record(const std::string& line) {
    const std::string text = trim(line);
    if (text.empty() || starts_with(text, "#")) {
        return {};
    }
    if (text.front() == '{') {
        return parse_json_object(text);
    }
    return parse_key_values(text);
}

Node build_node(const std::map<std::string, std::string>& fields) {
    Node node;
    node.id = field_or(fields, "id", "");
    node.region = field_or(fields, "region", node.region);
    node.pool = lower_copy(field_or(fields, "pool", node.pool));
    node.gpus = parse_int(fields, "gpus", 0);
    node.free_gpus = parse_int(fields, "free_gpus", node.gpus);
    node.price_per_gpu_hour = parse_double(fields, "price_per_gpu_hour", node.price_per_gpu_hour);
    node.eviction_rate_per_hour = parse_double(fields, "eviction_rate_per_hour", node.eviction_rate_per_hour);
    node.checkpoint_write_mbps = parse_double(fields, "checkpoint_write_mbps", node.checkpoint_write_mbps);
    node.watts_per_gpu = parse_double(fields, "watts_per_gpu", node.watts_per_gpu);
    node.carbon_g_per_kwh = parse_double(fields, "carbon_g_per_kwh", node.carbon_g_per_kwh);
    node.scratch_gb = parse_double(fields, "scratch_gb", node.scratch_gb);
    node.spot = parse_bool(fields, "spot", node.pool != "on-demand" && node.pool != "ondemand");
    if (node.id.empty()) {
        throw std::runtime_error("node id is required");
    }
    if (node.gpus <= 0 || node.free_gpus < 0 || node.free_gpus > node.gpus) {
        throw std::runtime_error("node " + node.id + " has invalid GPU counts");
    }
    if (node.price_per_gpu_hour < 0.0 || node.eviction_rate_per_hour < 0.0 ||
        node.checkpoint_write_mbps <= 0.0 || node.watts_per_gpu < 0.0 ||
        node.carbon_g_per_kwh < 0.0 || node.scratch_gb < 0.0) {
        throw std::runtime_error("node " + node.id + " has invalid numeric capacity fields");
    }
    return node;
}

Job build_job(const std::map<std::string, std::string>& fields) {
    Job job;
    job.id = field_or(fields, "id", "");
    job.gpus = parse_int(fields, "gpus", job.gpus);
    job.business_priority = parse_int(fields, "business_priority", job.business_priority);
    job.gpu_hours = parse_double(fields, "gpu_hours", job.gpu_hours);
    job.checkpoint_gb = parse_double(fields, "checkpoint_gb", job.checkpoint_gb);
    job.max_checkpoint_minutes = parse_double(fields, "max_checkpoint_minutes", job.max_checkpoint_minutes);
    job.queue_age_minutes = parse_double(fields, "queue_age_minutes", job.queue_age_minutes);
    job.slo_minutes = parse_double(fields, "slo_minutes", job.slo_minutes);
    job.interrupt_cost_minutes = parse_double(fields, "interrupt_cost_minutes", job.interrupt_cost_minutes);
    job.max_cost_usd = parse_double(fields, "max_cost_usd", job.max_cost_usd);
    job.min_checkpoint_minutes = parse_double(fields, "min_checkpoint_minutes", job.min_checkpoint_minutes);
    if (job.id.empty()) {
        throw std::runtime_error("job id is required");
    }
    if (job.gpus <= 0 || job.gpu_hours <= 0.0 || job.checkpoint_gb < 0.0 ||
        job.max_checkpoint_minutes <= 0.0 || job.queue_age_minutes < 0.0 ||
        job.slo_minutes <= 0.0 || job.interrupt_cost_minutes < 0.0 ||
        job.min_checkpoint_minutes <= 0.0 || job.business_priority < 1 ||
        job.business_priority > 5) {
        throw std::runtime_error("job " + job.id + " has invalid numeric fields");
    }
    if (job.min_checkpoint_minutes > job.max_checkpoint_minutes) {
        throw std::runtime_error("job " + job.id + " has min_checkpoint_minutes above max_checkpoint_minutes");
    }
    return job;
}

std::string record_kind(const std::map<std::string, std::string>& fields) {
    const std::string kind = lower_copy(field_or(fields, "kind", field_or(fields, "type", "")));
    if (!kind.empty()) {
        return kind;
    }
    if (fields.find("price_per_gpu_hour") != fields.end() || fields.find("free_gpus") != fields.end()) {
        return "node";
    }
    if (fields.find("gpu_hours") != fields.end() || fields.find("slo_minutes") != fields.end()) {
        return "job";
    }
    return "";
}

void parse_input(std::istream& input,
                 std::vector<Node>& nodes,
                 std::vector<Job>& jobs,
                 std::vector<ParseError>& errors) {
    std::string line;
    std::size_t line_no = 0;
    std::set<std::string> node_ids;
    std::set<std::string> job_ids;
    while (std::getline(input, line)) {
        ++line_no;
        try {
            const auto fields = parse_record(line);
            if (fields.empty()) {
                continue;
            }
            const std::string kind = record_kind(fields);
            if (kind == "node" || kind == "capacity") {
                Node node = build_node(fields);
                if (!node_ids.insert(node.id).second) {
                    throw std::runtime_error("duplicate node id " + node.id);
                }
                nodes.push_back(node);
            } else if (kind == "job" || kind == "workload") {
                Job job = build_job(fields);
                if (!job_ids.insert(job.id).second) {
                    throw std::runtime_error("duplicate job id " + job.id);
                }
                jobs.push_back(job);
            } else {
                throw std::runtime_error("record kind must be node or job");
            }
        } catch (const std::exception& ex) {
            errors.push_back(ParseError{line_no, ex.what()});
        }
    }
}

double clamp(double value, double low, double high) {
    return std::max(low, std::min(value, high));
}

double risk_limit_for(const Job& job, const Options& options) {
    double limit = options.max_spot_risk;
    if (job.business_priority >= 5) {
        limit = std::min(limit, 0.08);
    } else if (job.business_priority == 4) {
        limit = std::min(limit, 0.14);
    } else if (job.business_priority <= 2) {
        limit = std::max(limit, 0.35);
    }
    return clamp(limit, 0.0, 0.95);
}

double checkpoint_write_minutes(const Job& job, const Node& node) {
    const double megabytes = job.checkpoint_gb * 1024.0;
    return megabytes / node.checkpoint_write_mbps / 60.0;
}

double recommended_checkpoint_minutes(const Job& job, const Node& node) {
    const double write_minutes = std::max(0.05, checkpoint_write_minutes(job, node));
    if (node.eviction_rate_per_hour <= 0.000001) {
        return job.max_checkpoint_minutes;
    }
    const double mtbf_minutes = 60.0 / node.eviction_rate_per_hour;
    const double young_minutes = std::sqrt(2.0 * write_minutes * mtbf_minutes);
    return clamp(young_minutes, job.min_checkpoint_minutes, job.max_checkpoint_minutes);
}

Candidate evaluate_candidate(const Job& job, const Node& node, const Options& options) {
    Candidate candidate;
    candidate.node = node;
    candidate.has_node = true;
    candidate.checkpoint_write_minutes = checkpoint_write_minutes(job, node);
    candidate.checkpoint_minutes = recommended_checkpoint_minutes(job, node);
    candidate.risk_limit = risk_limit_for(job, options);
    candidate.cost_usd = job.gpu_hours * static_cast<double>(job.gpus) * node.price_per_gpu_hour;
    candidate.carbon_kg = job.gpu_hours * static_cast<double>(job.gpus) *
                          (node.watts_per_gpu / 1000.0) *
                          (node.carbon_g_per_kwh / 1000.0);
    candidate.eviction_probability = 1.0 - std::exp(-node.eviction_rate_per_hour * job.gpu_hours);
    candidate.expected_interrupts = node.eviction_rate_per_hour * job.gpu_hours;

    const double runtime_minutes = job.gpu_hours * 60.0;
    const double checkpoints = std::max(1.0, std::ceil(runtime_minutes / candidate.checkpoint_minutes));
    const double lost_minutes_per_interrupt =
        (candidate.checkpoint_minutes / 2.0) + job.interrupt_cost_minutes + candidate.checkpoint_write_minutes;
    candidate.expected_overhead_minutes =
        checkpoints * candidate.checkpoint_write_minutes +
        candidate.expected_interrupts * lost_minutes_per_interrupt;
    candidate.finish_minutes = job.queue_age_minutes + runtime_minutes + candidate.expected_overhead_minutes;

    if (node.free_gpus < job.gpus) {
        candidate.reasons.push_back("insufficient_free_gpus");
    }
    if (node.scratch_gb < std::max(1.0, job.checkpoint_gb * 2.0)) {
        candidate.reasons.push_back("checkpoint_scratch_too_small");
    }
    if (candidate.cost_usd > job.max_cost_usd) {
        candidate.reasons.push_back("job_budget_exceeded");
    }
    if (candidate.finish_minutes > job.slo_minutes) {
        candidate.reasons.push_back("slo_would_be_missed");
    }
    if (node.spot && candidate.eviction_probability > candidate.risk_limit) {
        candidate.reasons.push_back("spot_eviction_risk_too_high");
    }

    const double lateness = std::max(0.0, candidate.finish_minutes - job.slo_minutes);
    const double priority_weight = 1.0 + static_cast<double>(job.business_priority) * 0.8;
    const double carbon_weight = options.prefer_low_carbon ? 0.18 : 0.035;
    const double risk_penalty = node.spot ? candidate.eviction_probability * priority_weight * 35.0 : 0.0;
    const double overhead_penalty = candidate.expected_overhead_minutes * 0.08 * priority_weight;
    const double lateness_penalty = lateness * 2.0 * priority_weight;
    const double infeasible_penalty = static_cast<double>(candidate.reasons.size()) * 10000.0;
    candidate.score = candidate.cost_usd +
                      candidate.carbon_kg * carbon_weight +
                      risk_penalty +
                      overhead_penalty +
                      lateness_penalty +
                      infeasible_penalty;
    candidate.feasible = candidate.reasons.empty();
    return candidate;
}

std::string action_for(const Job& job, const Candidate* selected) {
    if (selected == nullptr) {
        return "split_job_or_use_reserved_capacity";
    }
    if (selected->has_node && selected->node.spot && selected->eviction_probability > selected->risk_limit * 0.75) {
        return "run_with_aggressive_checkpointing";
    }
    if (selected->finish_minutes > job.slo_minutes * 0.85) {
        return "run_now_and_drain_queue";
    }
    return "run";
}

std::vector<PlanRow> build_plan(std::vector<Node> nodes,
                                std::vector<Job> jobs,
                                const Options& options) {
    std::stable_sort(jobs.begin(), jobs.end(), [](const Job& left, const Job& right) {
        if (left.business_priority != right.business_priority) {
            return left.business_priority > right.business_priority;
        }
        if (left.slo_minutes != right.slo_minutes) {
            return left.slo_minutes < right.slo_minutes;
        }
        return left.queue_age_minutes > right.queue_age_minutes;
    });

    std::vector<PlanRow> plan;
    for (const Job& job : jobs) {
        std::vector<Candidate> candidates;
        for (const Node& node : nodes) {
            candidates.push_back(evaluate_candidate(job, node, options));
        }
        std::stable_sort(candidates.begin(), candidates.end(), [](const Candidate& left, const Candidate& right) {
            return left.score < right.score;
        });

        PlanRow row;
        row.job = job;
        for (const Candidate& candidate : candidates) {
            if (candidate.feasible && !row.selected.has_value()) {
                row.selected = candidate;
            } else {
                row.rejected.push_back(candidate);
            }
        }
        if (row.selected.has_value() && row.selected->has_node) {
            for (Node& node : nodes) {
                if (node.id == row.selected->node.id) {
                    node.free_gpus -= job.gpus;
                    break;
                }
            }
        }
        row.action = action_for(job, row.selected ? &*row.selected : nullptr);
        plan.push_back(row);
    }
    return plan;
}

std::string join_reasons(const std::vector<std::string>& reasons) {
    if (reasons.empty()) {
        return "ok";
    }
    std::ostringstream out;
    for (std::size_t i = 0; i < reasons.size(); ++i) {
        if (i != 0) {
            out << ";";
        }
        out << reasons[i];
    }
    return out.str();
}

void render_table(const std::vector<PlanRow>& plan, std::ostream& out) {
    out << std::left
        << std::setw(24) << "job"
        << std::setw(22) << "node"
        << std::setw(12) << "cost"
        << std::setw(12) << "risk"
        << std::setw(13) << "ckpt_min"
        << std::setw(14) << "finish_min"
        << std::setw(12) << "carbon_kg"
        << "action\n";
    out << std::string(121, '-') << "\n";
    for (const PlanRow& row : plan) {
        if (row.selected.has_value()) {
            const Candidate& c = *row.selected;
            out << std::left
                << std::setw(24) << row.job.id.substr(0, 23)
                << std::setw(22) << c.node.id.substr(0, 21)
                << std::setw(12) << number_to_string(c.cost_usd, 2)
                << std::setw(12) << number_to_string(c.eviction_probability, 3)
                << std::setw(13) << number_to_string(c.checkpoint_minutes, 1)
                << std::setw(14) << number_to_string(c.finish_minutes, 1)
                << std::setw(12) << number_to_string(c.carbon_kg, 3)
                << row.action << "\n";
        } else {
            std::string reason = "no_candidate";
            if (!row.rejected.empty()) {
                reason = join_reasons(row.rejected.front().reasons);
            }
            out << std::left
                << std::setw(24) << row.job.id.substr(0, 23)
                << std::setw(22) << "UNSCHEDULED"
                << std::setw(12) << "-"
                << std::setw(12) << "-"
                << std::setw(13) << "-"
                << std::setw(14) << "-"
                << std::setw(12) << "-"
                << row.action << ":" << reason << "\n";
        }
    }
}

void render_csv(const std::vector<PlanRow>& plan, std::ostream& out) {
    out << "job,node,cost_usd,eviction_probability,checkpoint_minutes,expected_overhead_minutes,"
           "finish_minutes,carbon_kg,action,reasons\n";
    for (const PlanRow& row : plan) {
        if (row.selected.has_value()) {
            const Candidate& c = *row.selected;
            out << row.job.id << ","
                << c.node.id << ","
                << number_to_string(c.cost_usd, 4) << ","
                << number_to_string(c.eviction_probability, 6) << ","
                << number_to_string(c.checkpoint_minutes, 4) << ","
                << number_to_string(c.expected_overhead_minutes, 4) << ","
                << number_to_string(c.finish_minutes, 4) << ","
                << number_to_string(c.carbon_kg, 6) << ","
                << row.action << ",ok\n";
        } else {
            std::string reason = "no_candidate";
            if (!row.rejected.empty()) {
                reason = join_reasons(row.rejected.front().reasons);
            }
            out << row.job.id << ",UNSCHEDULED,,,,,,," << row.action << "," << reason << "\n";
        }
    }
}

void render_json(const std::vector<PlanRow>& plan, std::ostream& out) {
    out << "{\n  \"plan\": [\n";
    for (std::size_t i = 0; i < plan.size(); ++i) {
        const PlanRow& row = plan[i];
        out << "    {\n";
        out << "      \"job\": \"" << json_escape(row.job.id) << "\",\n";
        out << "      \"action\": \"" << json_escape(row.action) << "\",\n";
        if (row.selected.has_value()) {
            const Candidate& c = *row.selected;
            out << "      \"scheduled\": true,\n";
            out << "      \"node\": \"" << json_escape(c.node.id) << "\",\n";
            out << "      \"region\": \"" << json_escape(c.node.region) << "\",\n";
            out << "      \"pool\": \"" << json_escape(c.node.pool) << "\",\n";
            out << "      \"cost_usd\": " << number_to_string(c.cost_usd, 4) << ",\n";
            out << "      \"eviction_probability\": " << number_to_string(c.eviction_probability, 6) << ",\n";
            out << "      \"risk_limit\": " << number_to_string(c.risk_limit, 6) << ",\n";
            out << "      \"checkpoint_minutes\": " << number_to_string(c.checkpoint_minutes, 4) << ",\n";
            out << "      \"expected_overhead_minutes\": " << number_to_string(c.expected_overhead_minutes, 4) << ",\n";
            out << "      \"finish_minutes\": " << number_to_string(c.finish_minutes, 4) << ",\n";
            out << "      \"carbon_kg\": " << number_to_string(c.carbon_kg, 6) << "\n";
        } else {
            std::string reason = "no_candidate";
            if (!row.rejected.empty()) {
                reason = join_reasons(row.rejected.front().reasons);
            }
            out << "      \"scheduled\": false,\n";
            out << "      \"node\": null,\n";
            out << "      \"reason\": \"" << json_escape(reason) << "\"\n";
        }
        out << "    }" << (i + 1 == plan.size() ? "\n" : ",\n");
    }
    out << "  ]\n}\n";
}

bool should_fail(const std::vector<PlanRow>& plan, const Options& options) {
    const std::string mode = lower_copy(options.fail_on);
    if (mode == "none") {
        return false;
    }
    for (const PlanRow& row : plan) {
        if (mode == "unscheduled" && !row.selected.has_value()) {
            return true;
        }
        if (mode == "risk" && row.selected.has_value() &&
            row.selected->has_node &&
            row.selected->node.spot &&
            row.selected->eviction_probability > row.selected->risk_limit * 0.75) {
            return true;
        }
        if (mode == "budget" && row.selected.has_value() &&
            row.selected->cost_usd > row.job.max_cost_usd) {
            return true;
        }
    }
    return false;
}

void render_plan(const std::vector<PlanRow>& plan, const Options& options, std::ostream& out) {
    const std::string format = lower_copy(options.format);
    if (format == "table") {
        render_table(plan, out);
    } else if (format == "json") {
        render_json(plan, out);
    } else if (format == "csv") {
        render_csv(plan, out);
    } else {
        throw std::runtime_error("unknown format: " + options.format);
    }
}

Options parse_options(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto need_value = [&](const std::string& flag) -> std::string {
            if (i + 1 >= argc) {
                throw std::runtime_error(flag + " requires a value");
            }
            return argv[++i];
        };
        if (arg == "--input" || arg == "-i") {
            options.input_path = need_value(arg);
        } else if (arg == "--format" || arg == "-f") {
            options.format = need_value(arg);
        } else if (arg == "--fail-on") {
            options.fail_on = need_value(arg);
        } else if (arg == "--max-spot-risk") {
            options.max_spot_risk = std::stod(need_value(arg));
        } else if (arg == "--prefer-low-carbon") {
            options.prefer_low_carbon = true;
        } else if (arg == "--self-test") {
            options.self_test = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout
                << "Usage: GpuSpotCheckpointPlanner [--input file] [--format table|json|csv]\n"
                << "       [--max-spot-risk 0.25] [--prefer-low-carbon]\n"
                << "       [--fail-on none|unscheduled|risk|budget] [--self-test]\n\n"
                << "Input accepts JSONL or key=value lines. Use kind=node for GPU capacity\n"
                << "and kind=job for workloads. Required node fields: id,gpus,price_per_gpu_hour.\n"
                << "Required job fields: id,gpu_hours. Practical fields include free_gpus,\n"
                << "eviction_rate_per_hour,checkpoint_write_mbps,checkpoint_gb,slo_minutes,\n"
                << "business_priority,max_cost_usd,watts_per_gpu,carbon_g_per_kwh.\n";
            std::exit(0);
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }
    if (options.max_spot_risk < 0.0 || options.max_spot_risk > 0.95) {
        throw std::runtime_error("--max-spot-risk must be between 0 and 0.95");
    }
    const std::string fail_mode = lower_copy(options.fail_on);
    if (fail_mode != "none" && fail_mode != "unscheduled" &&
        fail_mode != "risk" && fail_mode != "budget") {
        throw std::runtime_error("--fail-on must be none, unscheduled, risk, or budget");
    }
    return options;
}

int run_with_stream(std::istream& input, const Options& options, std::ostream& out, std::ostream& err) {
    std::vector<Node> nodes;
    std::vector<Job> jobs;
    std::vector<ParseError> errors;
    parse_input(input, nodes, jobs, errors);
    if (!errors.empty()) {
        for (const ParseError& error : errors) {
            err << "line " << error.line << ": " << error.message << "\n";
        }
        return 1;
    }
    if (nodes.empty()) {
        err << "no node records found\n";
        return 1;
    }
    if (jobs.empty()) {
        err << "no job records found\n";
        return 1;
    }
    const std::vector<PlanRow> plan = build_plan(nodes, jobs, options);
    render_plan(plan, options, out);
    return should_fail(plan, options) ? 2 : 0;
}

std::string self_test_input() {
    return R"({"kind":"node","id":"cheap_spot_a","region":"us-east-1","pool":"spot","spot":true,"gpus":4,"free_gpus":4,"price_per_gpu_hour":0.72,"eviction_rate_per_hour":0.24,"checkpoint_write_mbps":900,"watts_per_gpu":690,"carbon_g_per_kwh":210,"scratch_gb":900}
{"kind":"node","id":"reserved_gate_a","region":"us-east-1","pool":"on-demand","spot":false,"gpus":2,"free_gpus":2,"price_per_gpu_hour":2.35,"eviction_rate_per_hour":0.002,"checkpoint_write_mbps":750,"watts_per_gpu":720,"carbon_g_per_kwh":390,"scratch_gb":600}
{"kind":"job","id":"release_gate","gpus":1,"business_priority":5,"gpu_hours":1.8,"checkpoint_gb":14,"max_checkpoint_minutes":8,"queue_age_minutes":22,"slo_minutes":170,"interrupt_cost_minutes":25,"max_cost_usd":6.0}
{"kind":"job","id":"nightly_eval","gpus":2,"business_priority":2,"gpu_hours":2.9,"checkpoint_gb":18,"max_checkpoint_minutes":14,"queue_age_minutes":4,"slo_minutes":420,"interrupt_cost_minutes":18,"max_cost_usd":9.0}
{"kind":"job","id":"oversized_research_sweep","gpus":4,"business_priority":4,"gpu_hours":7.5,"checkpoint_gb":180,"max_checkpoint_minutes":20,"queue_age_minutes":12,"slo_minutes":260,"interrupt_cost_minutes":40,"max_cost_usd":4.0}
)";
}

int run_self_test() {
    Options options;
    options.format = "json";
    options.fail_on = "none";
    std::istringstream input(self_test_input());
    std::ostringstream output;
    std::ostringstream errors;
    const int rc = run_with_stream(input, options, output, errors);
    const std::string json = output.str();
    if (rc != 0) {
        std::cerr << "self-test failed: planner returned " << rc << "\n" << errors.str();
        return 1;
    }
    if (json.find("\"job\": \"release_gate\"") == std::string::npos ||
        json.find("\"node\": \"reserved_gate_a\"") == std::string::npos) {
        std::cerr << "self-test failed: high-priority release gate was not kept off risky spot capacity\n";
        return 1;
    }
    if (json.find("\"job\": \"oversized_research_sweep\"") == std::string::npos ||
        json.find("\"scheduled\": false") == std::string::npos) {
        std::cerr << "self-test failed: oversized budget-breaking sweep should remain unscheduled\n";
        return 1;
    }
    std::cout << "self-test passed\n";
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parse_options(argc, argv);
        if (options.self_test) {
            return run_self_test();
        }
        if (!options.input_path.empty()) {
            std::ifstream input(options.input_path);
            if (!input) {
                std::cerr << "cannot open input file: " << options.input_path << "\n";
                return 1;
            }
            return run_with_stream(input, options, std::cout, std::cerr);
        }
        return run_with_stream(std::cin, options, std::cout, std::cerr);
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << "\n";
        return 1;
    }
}

/*
This solves the ugly April 2026 problem where an AI infra team wants cheap spot GPU capacity but cannot explain which inference eval, model regression test, batch RAG rebuild, or research sweep is safe to run there. Built because teams keep discovering the failure after the node dies: the checkpoint interval was too slow, the queue SLO was already half burned, the retry cost was hidden, and the cheap GPU was not cheap after lost work. Use it when you have JSONL from a scheduler, a notebook, a CI eval runner, or a Kubernetes capacity report and need a C++20 GPU spot instance checkpoint planner that works without a service, database, Python package, or cloud SDK. The trick: it scores every job against every GPU pool using checkpoint write time, spot eviction probability, expected lost minutes, deadline pressure, budget ceiling, free GPUs, scratch space, carbon intensity, and business priority, then it gives a concrete run, drain, split, or reserved-capacity decision. Drop this into a DevOps repo, AI inference platform, LLM evaluation pipeline, preemptible GPU scheduler, edge compute batch runner, carbon aware GPU planning workflow, or research system that needs a forkable command line tool for GPU spot checkpointing, SLO-safe scheduling, budget guardrails, and production incident prevention.
*/