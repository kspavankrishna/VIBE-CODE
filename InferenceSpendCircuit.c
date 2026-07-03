// InferenceSpendCircuit.c
// Zero-dependency rolling spend and token admission gate for LLM gateways.

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define ISC_MAX_LINE 65536
#define ISC_MAX_TENANTS 4096
#define ISC_MAX_BUCKETS 240
#define ISC_MAX_FIELD 160
#define ISC_HASH_OFFSET 1469598103934665603ULL
#define ISC_HASH_PRIME 1099511628211ULL

typedef struct {
    uint64_t epoch;
    uint64_t tokens;
    uint64_t cost_micros;
    uint64_t accepted;
    uint64_t denied;
} SpendBucket;

typedef struct {
    bool used;
    uint64_t hash;
    uint64_t last_seen_ms;
    char tenant[ISC_MAX_FIELD];
    SpendBucket buckets[ISC_MAX_BUCKETS];
} TenantState;

typedef struct {
    uint64_t tokens;
    uint64_t cost_micros;
    uint64_t accepted;
    uint64_t denied;
} WindowSum;

typedef struct {
    uint64_t window_ms;
    uint64_t bucket_ms;
    uint64_t max_tokens;
    uint64_t max_cost_micros;
    uint64_t input_micros_per_1k;
    uint64_t output_micros_per_1k;
    size_t tenant_slots;
    int bucket_count;
    int bypass_priority;
    bool shadow_mode;
} Config;

typedef struct {
    bool valid;
    bool cost_provided;
    uint64_t ts_ms;
    uint64_t input_tokens;
    uint64_t output_tokens;
    uint64_t cost_micros;
    int priority;
    char tenant[ISC_MAX_FIELD];
    char model[ISC_MAX_FIELD];
    char request_id[ISC_MAX_FIELD];
    char error[ISC_MAX_FIELD];
} Event;

static TenantState tenants[ISC_MAX_TENANTS];

static uint64_t now_ms(void) {
    return (uint64_t)time(NULL) * 1000ULL;
}

static uint64_t sat_add(uint64_t a, uint64_t b) {
    if (UINT64_MAX - a < b) {
        return UINT64_MAX;
    }
    return a + b;
}

static uint64_t sat_mul_div_up(uint64_t value, uint64_t multiplier, uint64_t divisor) {
    if (divisor == 0) {
        return UINT64_MAX;
    }
    if (value == 0 || multiplier == 0) {
        return 0;
    }
    if (value > UINT64_MAX / multiplier) {
        return UINT64_MAX;
    }
    uint64_t product = value * multiplier;
    return product / divisor + ((product % divisor) ? 1ULL : 0ULL);
}

static uint64_t fnv1a(const char *text) {
    uint64_t hash = ISC_HASH_OFFSET;
    while (*text) {
        hash ^= (unsigned char)*text++;
        hash *= ISC_HASH_PRIME;
    }
    return hash;
}

static void rstrip(char *line) {
    size_t n = strlen(line);
    while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) {
        line[--n] = '\0';
    }
}

static const char *skip_ws(const char *p) {
    while (*p && isspace((unsigned char)*p)) {
        ++p;
    }
    return p;
}

static const char *parse_json_string(const char *p, char *out, size_t out_cap) {
    size_t n = 0;
    if (*p != '"') {
        return NULL;
    }
    ++p;
    while (*p && *p != '"') {
        unsigned char c = (unsigned char)*p;
        if (c == '\\') {
            ++p;
            if (!*p) {
                return NULL;
            }
            switch (*p) {
                case '"': c = '"'; break;
                case '\\': c = '\\'; break;
                case '/': c = '/'; break;
                case 'b': c = '\b'; break;
                case 'f': c = '\f'; break;
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                case 'u':
                    c = '?';
                    for (int i = 0; i < 4 && isxdigit((unsigned char)p[1 + i]); ++i) {
                    }
                    p += 4;
                    break;
                default:
                    return NULL;
            }
        }
        if (n + 1 < out_cap) {
            out[n++] = (char)c;
        }
        ++p;
    }
    if (*p != '"') {
        return NULL;
    }
    if (out_cap > 0) {
        out[n] = '\0';
    }
    return p + 1;
}

static bool find_json_value(const char *json, const char *key, const char **value) {
    char parsed[ISC_MAX_FIELD];
    const char *p = json;
    while (*p) {
        if (*p != '"') {
            ++p;
            continue;
        }
        const char *after = parse_json_string(p, parsed, sizeof(parsed));
        if (!after) {
            return false;
        }
        const char *q = skip_ws(after);
        if (*q == ':') {
            q = skip_ws(q + 1);
            if (strcmp(parsed, key) == 0) {
                *value = q;
                return true;
            }
            p = q;
        } else {
            p = after;
        }
    }
    return false;
}

static bool parse_u64_text(const char *text, uint64_t *out) {
    if (!text || *text == '-') {
        return false;
    }
    errno = 0;
    char *end = NULL;
    unsigned long long value = strtoull(text, &end, 10);
    if (errno == ERANGE || end == text) {
        return false;
    }
    *out = (uint64_t)value;
    return true;
}

static bool json_get_string(const char *json, const char *key, char *out, size_t out_cap) {
    const char *value = NULL;
    if (!find_json_value(json, key, &value) || *value != '"') {
        return false;
    }
    return parse_json_string(value, out, out_cap) != NULL;
}

static bool json_get_u64(const char *json, const char *key, uint64_t *out) {
    const char *value = NULL;
    if (!find_json_value(json, key, &value)) {
        return false;
    }
    if (*value == '"') {
        char tmp[64];
        if (!parse_json_string(value, tmp, sizeof(tmp))) {
            return false;
        }
        return parse_u64_text(tmp, out);
    }
    return parse_u64_text(value, out);
}

static bool json_get_int(const char *json, const char *key, int *out) {
    uint64_t value = 0;
    if (!json_get_u64(json, key, &value) || value > 2147483647ULL) {
        return false;
    }
    *out = (int)value;
    return true;
}

static void json_write_string(FILE *out, const char *text) {
    fputc('"', out);
    for (const unsigned char *p = (const unsigned char *)text; *p; ++p) {
        switch (*p) {
            case '"': fputs("\\\"", out); break;
            case '\\': fputs("\\\\", out); break;
            case '\b': fputs("\\b", out); break;
            case '\f': fputs("\\f", out); break;
            case '\n': fputs("\\n", out); break;
            case '\r': fputs("\\r", out); break;
            case '\t': fputs("\\t", out); break;
            default:
                if (*p < 0x20) {
                    fprintf(out, "\\u%04x", *p);
                } else {
                    fputc(*p, out);
                }
        }
    }
    fputc('"', out);
}

static void usage(FILE *out) {
    fputs(
        "Usage: InferenceSpendCircuit [options] < events.ndjson\n"
        "\n"
        "Each input line is JSON with fields like tenant, model, ts_ms, input_tokens,\n"
        "output_tokens or output_tokens_estimate, optional cost_micros, priority, request_id.\n"
        "The program writes one compact JSON decision per input line.\n"
        "\n"
        "Options:\n"
        "  --window-sec N              Rolling budget window in seconds (default 60)\n"
        "  --buckets N                 Ring buckets inside the window, 1..240 (default 120)\n"
        "  --max-tokens N              Tenant token ceiling per window, 0 disables (default 200000)\n"
        "  --max-cost-micros N         Tenant spend ceiling per window, 0 disables (default 5000000)\n"
        "  --input-micros-per-1k N     Input-token price estimate when cost_micros is absent\n"
        "  --output-micros-per-1k N    Output-token price estimate when cost_micros is absent\n"
        "  --tenant-slots N            Tenant hash slots, 1..4096 (default 1024)\n"
        "  --bypass-priority N         Priority >= N bypasses limits; 10 disables (default 10)\n"
        "  --shadow                    Emit would_allow but never block; useful before enforcement\n"
        "  --help                      Show this help\n",
        out);
}

static bool parse_arg_u64(int argc, char **argv, int *i, const char *flag, uint64_t *out) {
    if (strcmp(argv[*i], flag) != 0) {
        return false;
    }
    if (*i + 1 >= argc || !parse_u64_text(argv[*i + 1], out)) {
        fprintf(stderr, "invalid value for %s\n", flag);
        exit(2);
    }
    ++*i;
    return true;
}

static Config parse_config(int argc, char **argv) {
    Config cfg;
    cfg.window_ms = 60000ULL;
    cfg.bucket_ms = 500ULL;
    cfg.max_tokens = 200000ULL;
    cfg.max_cost_micros = 5000000ULL;
    cfg.input_micros_per_1k = 150ULL;
    cfg.output_micros_per_1k = 600ULL;
    cfg.tenant_slots = 1024;
    cfg.bucket_count = 120;
    cfg.bypass_priority = 10;
    cfg.shadow_mode = false;

    for (int i = 1; i < argc; ++i) {
        uint64_t value = 0;
        if (strcmp(argv[i], "--help") == 0) {
            usage(stdout);
            exit(0);
        } else if (parse_arg_u64(argc, argv, &i, "--window-sec", &value)) {
            cfg.window_ms = value > UINT64_MAX / 1000ULL ? UINT64_MAX : value * 1000ULL;
        } else if (parse_arg_u64(argc, argv, &i, "--buckets", &value)) {
            if (value < 1 || value > ISC_MAX_BUCKETS) {
                fprintf(stderr, "--buckets must be between 1 and %d\n", ISC_MAX_BUCKETS);
                exit(2);
            }
            cfg.bucket_count = (int)value;
        } else if (parse_arg_u64(argc, argv, &i, "--max-tokens", &cfg.max_tokens)) {
        } else if (parse_arg_u64(argc, argv, &i, "--max-cost-micros", &cfg.max_cost_micros)) {
        } else if (parse_arg_u64(argc, argv, &i, "--input-micros-per-1k", &cfg.input_micros_per_1k)) {
        } else if (parse_arg_u64(argc, argv, &i, "--output-micros-per-1k", &cfg.output_micros_per_1k)) {
        } else if (parse_arg_u64(argc, argv, &i, "--tenant-slots", &value)) {
            if (value < 1 || value > ISC_MAX_TENANTS) {
                fprintf(stderr, "--tenant-slots must be between 1 and %d\n", ISC_MAX_TENANTS);
                exit(2);
            }
            cfg.tenant_slots = (size_t)value;
        } else if (parse_arg_u64(argc, argv, &i, "--bypass-priority", &value)) {
            if (value > 10) {
                fprintf(stderr, "--bypass-priority must be 0..10\n");
                exit(2);
            }
            cfg.bypass_priority = (int)value;
        } else if (strcmp(argv[i], "--shadow") == 0) {
            cfg.shadow_mode = true;
        } else {
            fprintf(stderr, "unknown option: %s\n", argv[i]);
            usage(stderr);
            exit(2);
        }
    }

    if (cfg.window_ms == 0) {
        fprintf(stderr, "--window-sec must be greater than zero\n");
        exit(2);
    }
    cfg.bucket_ms = (cfg.window_ms + (uint64_t)cfg.bucket_count - 1ULL) / (uint64_t)cfg.bucket_count;
    if (cfg.bucket_ms == 0) {
        cfg.bucket_ms = 1;
    }
    return cfg;
}

static uint64_t estimate_cost_micros(const Config *cfg, uint64_t input_tokens, uint64_t output_tokens) {
    uint64_t input = sat_mul_div_up(input_tokens, cfg->input_micros_per_1k, 1000ULL);
    uint64_t output = sat_mul_div_up(output_tokens, cfg->output_micros_per_1k, 1000ULL);
    return sat_add(input, output);
}

static Event parse_event(const Config *cfg, const char *line) {
    Event ev;
    memset(&ev, 0, sizeof(ev));
    ev.valid = true;
    ev.ts_ms = now_ms();
    ev.priority = 0;
    strcpy(ev.tenant, "default");
    strcpy(ev.model, "unknown");

    if (!json_get_string(line, "tenant", ev.tenant, sizeof(ev.tenant))) {
        json_get_string(line, "workspace_id", ev.tenant, sizeof(ev.tenant));
    }
    json_get_string(line, "model", ev.model, sizeof(ev.model));
    json_get_string(line, "request_id", ev.request_id, sizeof(ev.request_id));
    json_get_u64(line, "ts_ms", &ev.ts_ms);
    json_get_u64(line, "input_tokens", &ev.input_tokens);
    if (!json_get_u64(line, "output_tokens_estimate", &ev.output_tokens)) {
        if (!json_get_u64(line, "max_output_tokens", &ev.output_tokens)) {
            json_get_u64(line, "output_tokens", &ev.output_tokens);
        }
    }
    json_get_int(line, "priority", &ev.priority);
    ev.cost_provided = json_get_u64(line, "cost_micros", &ev.cost_micros);
    if (!ev.cost_provided) {
        ev.cost_provided = json_get_u64(line, "estimated_cost_micros", &ev.cost_micros);
    }
    if (!ev.cost_provided) {
        ev.cost_micros = estimate_cost_micros(cfg, ev.input_tokens, ev.output_tokens);
    }
    if (ev.tenant[0] == '\0') {
        ev.valid = false;
        strcpy(ev.error, "empty_tenant");
    } else if (UINT64_MAX - ev.input_tokens < ev.output_tokens) {
        ev.valid = false;
        strcpy(ev.error, "token_overflow");
    } else if (ev.input_tokens == 0 && ev.output_tokens == 0 && ev.cost_micros == 0) {
        ev.valid = false;
        strcpy(ev.error, "empty_spend_estimate");
    }
    return ev;
}

static TenantState *tenant_acquire(const char *tenant, uint64_t ts_ms, size_t slots) {
    uint64_t hash = fnv1a(tenant);
    size_t start = (size_t)(hash % slots);
    for (size_t step = 0; step < slots; ++step) {
        size_t idx = (start + step) % slots;
        TenantState *state = &tenants[idx];
        if (!state->used) {
            memset(state, 0, sizeof(*state));
            state->used = true;
            state->hash = hash;
            state->last_seen_ms = ts_ms;
            snprintf(state->tenant, sizeof(state->tenant), "%s", tenant);
            return state;
        }
        if (state->hash == hash && strcmp(state->tenant, tenant) == 0) {
            state->last_seen_ms = ts_ms;
            return state;
        }
    }

    size_t victim = 0;
    uint64_t oldest = UINT64_MAX;
    for (size_t i = 0; i < slots; ++i) {
        if (tenants[i].last_seen_ms < oldest) {
            oldest = tenants[i].last_seen_ms;
            victim = i;
        }
    }
    TenantState *state = &tenants[victim];
    memset(state, 0, sizeof(*state));
    state->used = true;
    state->hash = hash;
    state->last_seen_ms = ts_ms;
    snprintf(state->tenant, sizeof(state->tenant), "%s", tenant);
    return state;
}

static bool bucket_live(const Config *cfg, const SpendBucket *bucket, uint64_t now_epoch) {
    if (bucket->tokens == 0 && bucket->cost_micros == 0 && bucket->accepted == 0 && bucket->denied == 0) {
        return false;
    }
    return now_epoch >= bucket->epoch && now_epoch - bucket->epoch < (uint64_t)cfg->bucket_count;
}

static void sweep_state(const Config *cfg, TenantState *state, uint64_t now_epoch) {
    for (int i = 0; i < cfg->bucket_count; ++i) {
        if (!bucket_live(cfg, &state->buckets[i], now_epoch)) {
            memset(&state->buckets[i], 0, sizeof(state->buckets[i]));
        }
    }
}

static WindowSum window_sum(const Config *cfg, const TenantState *state, uint64_t now_epoch) {
    WindowSum sum;
    memset(&sum, 0, sizeof(sum));
    for (int i = 0; i < cfg->bucket_count; ++i) {
        const SpendBucket *bucket = &state->buckets[i];
        if (!bucket_live(cfg, bucket, now_epoch)) {
            continue;
        }
        sum.tokens = sat_add(sum.tokens, bucket->tokens);
        sum.cost_micros = sat_add(sum.cost_micros, bucket->cost_micros);
        sum.accepted = sat_add(sum.accepted, bucket->accepted);
        sum.denied = sat_add(sum.denied, bucket->denied);
    }
    return sum;
}

static void record_event(const Config *cfg, TenantState *state, uint64_t epoch,
                         uint64_t tokens, uint64_t cost_micros, bool charged, bool allowed) {
    size_t idx = (size_t)(epoch % (uint64_t)cfg->bucket_count);
    SpendBucket *bucket = &state->buckets[idx];
    if (bucket->epoch != epoch) {
        memset(bucket, 0, sizeof(*bucket));
        bucket->epoch = epoch;
    }
    if (charged) {
        bucket->tokens = sat_add(bucket->tokens, tokens);
        bucket->cost_micros = sat_add(bucket->cost_micros, cost_micros);
    }
    if (allowed) {
        bucket->accepted = sat_add(bucket->accepted, 1ULL);
    } else {
        bucket->denied = sat_add(bucket->denied, 1ULL);
    }
}

static uint64_t retry_after_ms(const Config *cfg, const TenantState *state, uint64_t now_epoch,
                               uint64_t need_tokens, uint64_t need_cost, WindowSum before) {
    uint64_t best = cfg->window_ms;
    for (int i = 0; i < cfg->bucket_count; ++i) {
        const SpendBucket *bucket = &state->buckets[i];
        if (!bucket_live(cfg, bucket, now_epoch)) {
            continue;
        }
        WindowSum after = before;
        if (after.tokens >= bucket->tokens) {
            after.tokens -= bucket->tokens;
        }
        if (after.cost_micros >= bucket->cost_micros) {
            after.cost_micros -= bucket->cost_micros;
        }
        bool token_ok = cfg->max_tokens == 0 || sat_add(after.tokens, need_tokens) <= cfg->max_tokens;
        bool cost_ok = cfg->max_cost_micros == 0 || sat_add(after.cost_micros, need_cost) <= cfg->max_cost_micros;
        if (token_ok && cost_ok) {
            uint64_t expires_epoch = bucket->epoch + (uint64_t)cfg->bucket_count;
            uint64_t delta = expires_epoch > now_epoch ? expires_epoch - now_epoch : 1ULL;
            uint64_t candidate = sat_mul_div_up(delta, cfg->bucket_ms, 1ULL);
            if (candidate < best) {
                best = candidate;
            }
        }
    }
    return best == 0 ? cfg->bucket_ms : best;
}

static const char *decide(const Config *cfg, const Event *ev, WindowSum before,
                          uint64_t need_tokens, uint64_t need_cost, bool *would_allow) {
    *would_allow = true;
    if (!ev->valid) {
        *would_allow = false;
        return ev->error;
    }
    if (cfg->bypass_priority <= 9 && ev->priority >= cfg->bypass_priority) {
        return "priority_bypass";
    }
    if (cfg->max_tokens != 0 && need_tokens > cfg->max_tokens) {
        *would_allow = false;
        return "single_request_exceeds_token_window";
    }
    if (cfg->max_cost_micros != 0 && need_cost > cfg->max_cost_micros) {
        *would_allow = false;
        return "single_request_exceeds_cost_window";
    }
    if (cfg->max_tokens != 0 && sat_add(before.tokens, need_tokens) > cfg->max_tokens) {
        *would_allow = false;
        return "tenant_token_window_exhausted";
    }
    if (cfg->max_cost_micros != 0 && sat_add(before.cost_micros, need_cost) > cfg->max_cost_micros) {
        *would_allow = false;
        return "tenant_cost_window_exhausted";
    }
    return "ok";
}

static void emit_decision(uint64_t line_no, const Event *ev, const Config *cfg, WindowSum before,
                          bool allow, bool would_allow, const char *reason, uint64_t retry_ms) {
    uint64_t need_tokens = sat_add(ev->input_tokens, ev->output_tokens);
    fputs("{\"line\":", stdout);
    fprintf(stdout, "%