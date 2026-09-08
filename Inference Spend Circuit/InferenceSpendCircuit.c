// InferenceSpendCircuit.c
// Streaming token and spend admission control for LLM gateways and agent runtimes.

#include <ctype.h>
#include <errno.h>
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
} Bucket;

typedef struct {
    bool used;
    uint64_t hash;
    uint64_t last_seen_ms;
    char tenant[ISC_MAX_FIELD];
    Bucket buckets[ISC_MAX_BUCKETS];
} Tenant;

typedef struct {
    uint64_t tokens;
    uint64_t cost_micros;
    uint64_t accepted;
    uint64_t denied;
} Window;

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
    bool shadow;
} Config;

typedef struct {
    bool valid;
    bool cost_given;
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

static Tenant g_tenants[ISC_MAX_TENANTS];

static uint64_t now_ms(void) {
    return (uint64_t)time(NULL) * 1000ULL;
}

static uint64_t add_sat(uint64_t a, uint64_t b) {
    return UINT64_MAX - a < b ? UINT64_MAX : a + b;
}

static uint64_t mul_div_up_sat(uint64_t a, uint64_t b, uint64_t d) {
    if (d == 0) return UINT64_MAX;
    if (a == 0 || b == 0) return 0;
    if (a > UINT64_MAX / b) return UINT64_MAX;
    uint64_t p = a * b;
    return p / d + ((p % d) != 0);
}

static uint64_t fnv1a(const char *s) {
    uint64_t h = ISC_HASH_OFFSET;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= ISC_HASH_PRIME;
    }
    return h;
}

static void strip_eol(char *s) {
    size_t n = strlen(s);
    while (n && (s[n - 1] == '\n' || s[n - 1] == '\r')) s[--n] = 0;
}

static const char *skip_ws(const char *p) {
    while (*p && isspace((unsigned char)*p)) ++p;
    return p;
}

static const char *parse_json_string(const char *p, char *out, size_t cap) {
    size_t n = 0;
    if (*p != '"') return NULL;
    for (++p; *p && *p != '"'; ++p) {
        unsigned char c = (unsigned char)*p;
        if (c == '\\') {
            c = (unsigned char)*++p;
            if (!c) return NULL;
            switch (c) {
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
                    p += 4;
                    break;
                default:
                    return NULL;
            }
        }
        if (n + 1 < cap) out[n++] = (char)c;
    }
    if (*p != '"') return NULL;
    if (cap) out[n] = 0;
    return p + 1;
}

static bool find_value(const char *json, const char *key, const char **value) {
    char got[ISC_MAX_FIELD];
    for (const char *p = json; *p;) {
        if (*p != '"') {
            ++p;
            continue;
        }
        const char *after = parse_json_string(p, got, sizeof(got));
        if (!after) return false;
        const char *q = skip_ws(after);
        if (*q == ':') {
            q = skip_ws(q + 1);
            if (strcmp(got, key) == 0) {
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

static bool parse_u64_text(const char *s, uint64_t *out) {
    if (!s || *s == '-') return false;
    errno = 0;
    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno == ERANGE || end == s) return false;
    *out = (uint64_t)v;
    return true;
}

static bool json_string(const char *json, const char *key, char *out, size_t cap) {
    const char *v = NULL;
    return find_value(json, key, &v) && *v == '"' && parse_json_string(v, out, cap);
}

static bool json_u64(const char *json, const char *key, uint64_t *out) {
    const char *v = NULL;
    char tmp[64];
    if (!find_value(json, key, &v)) return false;
    if (*v == '"') return parse_json_string(v, tmp, sizeof(tmp)) && parse_u64_text(tmp, out);
    return parse_u64_text(v, out);
}

static bool json_int(const char *json, const char *key, int *out) {
    uint64_t v;
    if (!json_u64(json, key, &v) || v > 2147483647ULL) return false;
    *out = (int)v;
    return true;
}

static void json_out_string(FILE *out, const char *s) {
    fputc('"', out);
    for (const unsigned char *p = (const unsigned char *)s; *p; ++p) {
        switch (*p) {
            case '"': fputs("\\\"", out); break;
            case '\\': fputs("\\\\", out); break;
            case '\b': fputs("\\b", out); break;
            case '\f': fputs("\\f", out); break;
            case '\n': fputs("\\n", out); break;
            case '\r': fputs("\\r", out); break;
            case '\t': fputs("\\t", out); break;
            default:
                if (*p < 0x20) fprintf(out, "\\u%04x", *p);
                else fputc(*p, out);
        }
    }
    fputc('"', out);
}

static void usage(FILE *out) {
    fputs(
        "Usage: InferenceSpendCircuit [options] < events.ndjson\n"
        "Reads JSON lines with tenant, model, input_tokens, output_tokens_estimate,\n"
        "cost_micros, priority, request_id, and ts_ms. Writes JSON allow/deny decisions.\n\n"
        "  --window-sec N             rolling budget window seconds, default 60\n"
        "  --buckets N                buckets inside window, 1..240, default 120\n"
        "  --max-tokens N             per-tenant token limit per window, 0 disables\n"
        "  --max-cost-micros N        per-tenant spend limit per window, 0 disables\n"
        "  --input-micros-per-1k N    estimate price when cost_micros is missing\n"
        "  --output-micros-per-1k N   estimate price when cost_micros is missing\n"
        "  --tenant-slots N           hash slots, 1..4096, default 1024\n"
        "  --bypass-priority N        priority >= N bypasses limits; 10 disables\n"
        "  --shadow                   report would_allow but never block\n",
        out);
}

static bool arg_u64(int argc, char **argv, int *i, const char *flag, uint64_t *out) {
    if (strcmp(argv[*i], flag) != 0) return false;
    if (*i + 1 >= argc || !parse_u64_text(argv[*i + 1], out)) {
        fprintf(stderr, "invalid value for %s\n", flag);
        exit(2);
    }
    ++*i;
    return true;
}

static Config config_from_args(int argc, char **argv) {
    Config c;
    c.window_ms = 60000;
    c.bucket_ms = 500;
    c.max_tokens = 200000;
    c.max_cost_micros = 5000000;
    c.input_micros_per_1k = 150;
    c.output_micros_per_1k = 600;
    c.tenant_slots = 1024;
    c.bucket_count = 120;
    c.bypass_priority = 10;
    c.shadow = false;

    for (int i = 1; i < argc; ++i) {
        uint64_t v = 0;
        if (strcmp(argv[i], "--help") == 0) {
            usage(stdout);
            exit(0);
        } else if (arg_u64(argc, argv, &i, "--window-sec", &v)) {
            c.window_ms = v > UINT64_MAX / 1000ULL ? UINT64_MAX : v * 1000ULL;
        } else if (arg_u64(argc, argv, &i, "--buckets", &v)) {
            if (v < 1 || v > ISC_MAX_BUCKETS) exit(2);
            c.bucket_count = (int)v;
        } else if (arg_u64(argc, argv, &i, "--max-tokens", &c.max_tokens)) {
        } else if (arg_u64(argc, argv, &i, "--max-cost-micros", &c.max_cost_micros)) {
        } else if (arg_u64(argc, argv, &i, "--input-micros-per-1k", &c.input_micros_per_1k)) {
        } else if (arg_u64(argc, argv, &i, "--output-micros-per-1k", &c.output_micros_per_1k)) {
        } else if (arg_u64(argc, argv, &i, "--tenant-slots", &v)) {
            if (v < 1 || v > ISC_MAX_TENANTS) exit(2);
            c.tenant_slots = (size_t)v;
        } else if (arg_u64(argc, argv, &i, "--bypass-priority", &v)) {
            if (v > 10) exit(2);
            c.bypass_priority = (int)v;
        } else if (strcmp(argv[i], "--shadow") == 0) {
            c.shadow = true;
        } else {
            fprintf(stderr, "unknown option: %s\n", argv[i]);
            usage(stderr);
            exit(2);
        }
    }
    if (c.window_ms == 0) exit(2);
    c.bucket_ms = (c.window_ms + (uint64_t)c.bucket_count - 1ULL) / (uint64_t)c.bucket_count;
    if (c.bucket_ms == 0) c.bucket_ms = 1;
    return c;
}

static uint64_t estimated_cost(const Config *c, uint64_t in_tok, uint64_t out_tok) {
    uint64_t in = mul_div_up_sat(in_tok, c->input_micros_per_1k, 1000);
    uint64_t out = mul_div_up_sat(out_tok, c->output_micros_per_1k, 1000);
    return add_sat(in, out);
}

static Event parse_event(const Config *c, const char *line) {
    Event e;
    memset(&e, 0, sizeof(e));
    e.valid = true;
    e.ts_ms = now_ms();
    strcpy(e.tenant, "default");
    strcpy(e.model, "unknown");
    json_string(line, "tenant", e.tenant, sizeof(e.tenant));
    if (strcmp(e.tenant, "default") == 0) json_string(line, "workspace_id", e.tenant, sizeof(e.tenant));
    json_string(line, "model", e.model, sizeof(e.model));
    json_string(line, "request_id", e.request_id, sizeof(e.request_id));
    json_u64(line, "ts_ms", &e.ts_ms);
    json_u64(line, "input_tokens", &e.input_tokens);
    if (!json_u64(line, "output_tokens_estimate", &e.output_tokens) &&
        !json_u64(line, "max_output_tokens", &e.output_tokens)) {
        json_u64(line, "output_tokens", &e.output_tokens);
    }
    json_int(line, "priority", &e.priority);
    e.cost_given = json_u64(line, "cost_micros", &e.cost_micros);
    if (!e.cost_given) e.cost_given = json_u64(line, "estimated_cost_micros", &e.cost_micros);
    if (!e.cost_given) e.cost_micros = estimated_cost(c, e.input_tokens, e.output_tokens);

    if (e.tenant[0] == 0) {
        e.valid = false;
        strcpy(e.error, "empty_tenant");
    } else if (UINT64_MAX - e.input_tokens < e.output_tokens) {
        e.valid = false;
        strcpy(e.error, "token_overflow");
    } else if (e.input_tokens == 0 && e.output_tokens == 0 && e.cost_micros == 0) {
        e.valid = false;
        strcpy(e.error, "empty_spend_estimate");
    }
    return e;
}

static Tenant *tenant_get(const char *name, uint64_t ts_ms, size_t slots) {
    uint64_t h = fnv1a(name);
    size_t start = (size_t)(h % slots);
    for (size_t step = 0; step < slots; ++step) {
        Tenant *t = &g_tenants[(start + step) % slots];
        if (!t->used) {
            memset(t, 0, sizeof(*t));
            t->used = true;
            t->hash = h;
            snprintf(t->tenant, sizeof(t->tenant), "%s", name);
            t->last_seen_ms = ts_ms;
            return t;
        }
        if (t->hash == h && strcmp(t->tenant, name) == 0) {
            t->last_seen_ms = ts_ms;
            return t;
        }
    }
    size_t victim = 0;
    uint64_t oldest = UINT64_MAX;
    for (size_t i = 0; i < slots; ++i) {
        if (g_tenants[i].last_seen_ms < oldest) {
            oldest = g_tenants[i].last_seen_ms;
            victim = i;
        }
    }
    Tenant *t = &g_tenants[victim];
    memset(t, 0, sizeof(*t));
    t->used = true;
    t->hash = h;
    snprintf(t->tenant, sizeof(t->tenant), "%s", name);
    t->last_seen_ms = ts_ms;
    return t;
}

static bool live_bucket(const Config *c, const Bucket *b, uint64_t epoch) {
    if (b->tokens == 0 && b->cost_micros == 0 && b->accepted == 0 && b->denied == 0) return false;
    return epoch >= b->epoch && epoch - b->epoch < (uint64_t)c->bucket_count;
}

static void sweep(const Config *c, Tenant *t, uint64_t epoch) {
    for (int i = 0; i < c->bucket_count; ++i) {
        if (!live_bucket(c, &t->buckets[i], epoch)) memset(&t->buckets[i], 0, sizeof(t->buckets[i]));
    }
}

static Window sum_window(const Config *c, const Tenant *t, uint64_t epoch) {
    Window w;
    memset(&w, 0, sizeof(w));
    for (int i = 0; i < c->bucket_count; ++i) {
        const Bucket *b = &t->buckets[i];
        if (!live_bucket(c, b, epoch)) continue;
        w.tokens = add_sat(w.tokens, b->tokens);
        w.cost_micros = add_sat(w.cost_micros, b->cost_micros);
        w.accepted = add_sat(w.accepted, b->accepted);
        w.denied = add_sat(w.denied, b->denied);
    }
    return w;
}

static void record(const Config *c, Tenant *t, uint64_t epoch, uint64_t tokens,
                   uint64_t cost_micros, bool charge, bool would_allow) {
    Bucket *b = &t->buckets[epoch % (uint64_t)c->bucket_count];
    if (b->epoch != epoch) {
        memset(b, 0, sizeof(*b));
        b->epoch = epoch;
    }
    if (charge) {
        b->tokens = add_sat(b->tokens, tokens);
        b->cost_micros = add_sat(b->cost_micros, cost_micros);
    }
    if (would_allow) b->accepted = add_sat(b->accepted, 1);
    else b->denied = add_sat(b->denied, 1);
}

static const char *decide(const Config *c, const Event *e, Window before,
                          uint64_t tokens, uint64_t cost, bool *would_allow) {
    *would_allow = false;
    if (!e->valid) return e->error;
    if (c->bypass_priority <= 9 && e->priority >= c->bypass_priority) {
        *would_allow = true;
        return "priority_bypass";
    }
    if (c->max_tokens && tokens > c->max_tokens) return "single_request_exceeds_token_window";
    if (c->max_cost_micros && cost > c->max_cost_micros) return "single_request_exceeds_cost_window";
    if (c->max_tokens && add_sat(before.tokens, tokens) > c->max_tokens) return "tenant_token_window_exhausted";
    if (c->max_cost_micros && add_sat(before.cost_micros, cost) > c->max_cost_micros) return "tenant_cost_window_exhausted";
    *would_allow = true;
    return "ok";
}

static uint64_t retry_after(const Config *c, const Tenant *t, uint64_t epoch,
                            uint64_t tokens, uint64_t cost, Window before) {
    uint64_t best = c->window_ms;
    for (int i = 0; i < c->bucket_count; ++i) {
        const Bucket *b = &t->buckets[i];
        if (!live_bucket(c, b, epoch)) continue;
        Window after = before;
        if (after.tokens >= b->tokens) after.tokens -= b->tokens;
        if (after.cost_micros >= b->cost_micros) after.cost_micros -= b->cost_micros;
        bool tok_ok = !c->max_tokens || add_sat(after.tokens, tokens) <= c->max_tokens;
        bool cost_ok = !c->max_cost_micros || add_sat(after.cost_micros, cost) <= c->max_cost_micros;
        if (tok_ok && cost_ok) {
            uint64_t expires = b->epoch + (uint64_t)c->bucket_count;
            uint64_t delta = expires > epoch ? expires - epoch : 1;
            uint64_t ms = mul_div_up_sat(delta, c->bucket_ms, 1);
            if (ms < best) best = ms;
        }
    }
    return best ? best : c->bucket_ms;
}

static void emit_decision(uint64_t line, const Event *e, Window before, bool allow,
                          bool would_allow, const char *reason, uint64_t retry_ms,
                          uint64_t tokens) {
    fputs("{\"line\":", stdout);
    fprintf(stdout, "%llu", (unsigned long long)line);
    fputs(",\"allow\":", stdout);
    fputs(allow ? "true" : "false", stdout);
    fputs(",\"would_allow\":", stdout);
    fputs(would_allow ? "true" : "false", stdout);
    fputs(",\"reason\":", stdout);
    json_out_string(stdout, reason);
    fputs(",\"tenant\":", stdout);
    json_out_string(stdout, e->tenant);
    fputs(",\"model\":", stdout);
    json_out_string(stdout, e->model);
    if (e->request_id[0]) {
        fputs(",\"request_id\":", stdout);
        json_out_string(stdout, e->request_id);
    }
    fputs(",\"tokens\":", stdout);
    fprintf(stdout, "%llu", (unsigned long long)tokens);
    fputs(",\"cost_micros\":", stdout);
    fprintf(stdout, "%llu", (unsigned long long)e->cost_micros);
    fputs(",\"window_tokens_before\":", stdout);
    fprintf(stdout, "%llu", (unsigned long long)before.tokens);
    fputs(",\"window_cost_micros_before\":", stdout);
    fprintf(stdout, "%llu", (unsigned long long)before.cost_micros);
    if (!would_allow) {
        fputs(",\"retry_after_ms\":", stdout);
        fprintf(stdout, "%llu", (unsigned long long)retry_ms);
    }
    fputs("}\n", stdout);
}

int main(int argc, char **argv) {
    Config c = config_from_args(argc, argv);
    char line[ISC_MAX_LINE];
    uint64_t line_no = 0;

    while (fgets(line, sizeof(line), stdin)) {
        ++line_no;
        size_t len = strlen(line);
        bool truncated = len == sizeof(line) - 1 && line[len - 1] != '\n';
        if (truncated) {
            int ch;
            while ((ch = getchar()) != '\n' && ch != EOF) {}
            Event bad;
            memset(&bad, 0, sizeof(bad));
            strcpy(bad.error, "line_too_long");
            emit_decision(line_no, &bad, (Window){0, 0, 0, 0}, false, false, bad.error, 0, 0);
            continue;
        }
        strip_eol(line);
        if (line[0] == 0) continue;

        Event e = parse_event(&c, line);
        Window before = {0, 0, 0, 0};
        uint64_t tokens = add_sat(e.input_tokens, e.output_tokens);
        uint64_t retry_ms = 0;
        bool would_allow = false;
        bool allow = false;
        const char *reason = e.valid ? "ok" : e.error;

        if (e.valid) {
            Tenant *t = tenant_get(e.tenant, e.ts_ms, c.tenant_slots);
            uint64_t epoch = e.ts_ms / c.bucket_ms;
            sweep(&c, t, epoch);
            before = sum_window(&c, t, epoch);
            reason = decide(&c, &e, before, tokens, e.cost_micros, &would_allow);
            allow = c.shadow || would_allow;
            if (!would_allow) retry_ms = retry_after(&c, t, epoch, tokens, e.cost_micros, before);
            record(&c, t, epoch, tokens, e.cost_micros, allow, would_allow);
        }
        emit_decision(line_no, &e, before, allow, would_allow, reason, retry_ms, tokens);
    }
    if (ferror(stdin)) {
        perror("stdin");
        return 1;
    }
    return 0;
}

/*
This solves the April 2026 problem where LLM gateways, MCP servers, agent workers, and edge inference routes can burn token budget faster than normal rate limiters can notice. Built because a lot of production AI tooling now makes several model calls for one user action, and teams need a small deterministic guard before the expensive call, not only a dashboard after the bill lands. Use it when you have newline JSON request estimates from Envoy, NGINX, OpenTelemetry collectors, serverless functions, background agents, or a batch inference queue and you want per tenant token budget control, inference cost admission control, rolling spend protection, and shadow-mode rollout in one portable C file. The trick: it uses fixed memory hash slots and rolling buckets, so the decision is fast, local, auditable, and does not need Redis, Postgres, Kafka, or a cloud vendor SDK just to say yes or no. Drop this into an AI gateway sidecar, a CI load test, a model routing service, a carbon aware inference scheduler, or a developer productivity platform that must keep agentic workflows useful without letting one tenant, prompt loop, or stuck eval run drain the whole budget.
*/
