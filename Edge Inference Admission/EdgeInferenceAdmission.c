#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

#define FIELD_KEY_MAX 64
#define FIELD_VALUE_MAX 512
#define RECORD_FIELD_MAX 96
#define DEFAULT_TABLE_CAPACITY 128

typedef struct {
    char key[FIELD_KEY_MAX];
    char value[FIELD_VALUE_MAX];
} Field;

typedef struct {
    Field fields[RECORD_FIELD_MAX];
    size_t count;
} Record;

typedef struct {
    long ts;
    double usd;
    double tokens;
    unsigned long accepted;
    unsigned long rejected;
} Bucket;

typedef struct {
    char *name;
    Bucket *buckets;
    bool in_use;
} TenantState;

typedef struct {
    TenantState *items;
    size_t capacity;
    size_t count;
    size_t bucket_count;
} TenantTable;

typedef struct {
    long window_sec;
    double tenant_usd;
    double global_usd;
    double tenant_tokens;
    double global_tokens;
    double burst;
    double default_input_usd_per_mtok;
    double default_output_usd_per_mtok;
    double max_carbon_g;
    bool require_region_match;
    bool shadow;
    bool fail_open;
} Config;

typedef struct {
    const char *event;
    const char *id;
    const char *tenant;
    const char *region;
    const char *required_region;
    double input_tokens;
    double output_tokens;
    double total_tokens;
    double cost_usd;
    double carbon_g;
    int priority;
} Estimate;

typedef struct {
    double usd;
    double tokens;
    unsigned long accepted;
    unsigned long rejected;
} WindowTotals;

static void die(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    fputs("error: ", stderr);
    vfprintf(stderr, fmt, args);
    fputc('\n', stderr);
    va_end(args);
    exit(2);
}

static void *xcalloc(size_t count, size_t size) {
    void *ptr = calloc(count, size);
    if (!ptr) {
        die("out of memory allocating %zu bytes", count * size);
    }
    return ptr;
}

static void *xrealloc(void *ptr, size_t size) {
    void *next = realloc(ptr, size);
    if (!next) {
        die("out of memory reallocating %zu bytes", size);
    }
    return next;
}

static char *xstrdup(const char *value) {
    size_t len = strlen(value);
    char *copy = (char *)xcalloc(len + 1, 1);
    memcpy(copy, value, len + 1);
    return copy;
}

static bool streq(const char *left, const char *right) {
    return strcmp(left, right) == 0;
}

static bool is_key_char(int c) {
    return isalnum((unsigned char)c) || c == '_' || c == '-' || c == '.';
}

static void append_error(char *err, size_t err_cap, const char *fmt, ...) {
    if (err_cap == 0) {
        return;
    }
    va_list args;
    va_start(args, fmt);
    vsnprintf(err, err_cap, fmt, args);
    va_end(args);
    err[err_cap - 1] = '\0';
}

static char *read_line(FILE *fp) {
    size_t cap = 4096;
    size_t len = 0;
    char *buf = (char *)xcalloc(cap, 1);
    int ch;
    while ((ch = fgetc(fp)) != EOF) {
        if (len + 2 >= cap) {
            cap *= 2;
            buf = (char *)xrealloc(buf, cap);
        }
        buf[len++] = (char)ch;
        if (ch == '\n') {
            break;
        }
    }
    if (len == 0 && ch == EOF) {
        free(buf);
        return NULL;
    }
    buf[len] = '\0';
    return buf;
}

static bool add_field(Record *record, const char *key, const char *value, char *err, size_t err_cap) {
    for (size_t i = 0; i < record->count; i++) {
        if (streq(record->fields[i].key, key)) {
            append_error(err, err_cap, "duplicate field '%s'", key);
            return false;
        }
    }
    if (record->count >= RECORD_FIELD_MAX) {
        append_error(err, err_cap, "too many fields, max is %d", RECORD_FIELD_MAX);
        return false;
    }
    snprintf(record->fields[record->count].key, FIELD_KEY_MAX, "%s", key);
    snprintf(record->fields[record->count].value, FIELD_VALUE_MAX, "%s", value);
    record->count++;
    return true;
}

static bool append_value_char(char *value, size_t *len, char c, char *err, size_t err_cap) {
    if (*len + 1 >= FIELD_VALUE_MAX) {
        append_error(err, err_cap, "field value exceeds %d bytes", FIELD_VALUE_MAX - 1);
        return false;
    }
    value[(*len)++] = c;
    value[*len] = '\0';
    return true;
}

static bool parse_record(const char *line, Record *record, char *err, size_t err_cap) {
    const char *p = line;
    record->count = 0;
    while (*p) {
        while (*p && isspace((unsigned char)*p)) {
            p++;
        }
        if (*p == '\0' || *p == '#') {
            return true;
        }

        char key[FIELD_KEY_MAX] = {0};
        size_t key_len = 0;
        while (*p && *p != '=' && !isspace((unsigned char)*p)) {
            if (!is_key_char((unsigned char)*p)) {
                append_error(err, err_cap, "invalid key character '%c'", *p);
                return false;
            }
            if (key_len + 1 >= FIELD_KEY_MAX) {
                append_error(err, err_cap, "field key exceeds %d bytes", FIELD_KEY_MAX - 1);
                return false;
            }
            key[key_len++] = (char)tolower((unsigned char)*p);
            p++;
        }
        key[key_len] = '\0';
        if (key_len == 0 || *p != '=') {
            append_error(err, err_cap, "expected key=value field");
            return false;
        }
        p++;

        char value[FIELD_VALUE_MAX] = {0};
        size_t value_len = 0;
        if (*p == '"') {
            p++;
            while (*p && *p != '"') {
                char c = *p++;
                if (c == '\\') {
                    if (*p == '\0') {
                        append_error(err, err_cap, "truncated quoted escape");
                        return false;
                    }
                    char escaped = *p++;
                    switch (escaped) {
                        case 'n': c = '\n'; break;
                        case 'r': c = '\r'; break;
                        case 't': c = '\t'; break;
                        case '\\': c = '\\'; break;
                        case '"': c = '"'; break;
                        default:
                            append_error(err, err_cap, "unsupported quoted escape \\%c", escaped);
                            return false;
                    }
                }
                if (!append_value_char(value, &value_len, c, err, err_cap)) {
                    return false;
                }
            }
            if (*p != '"') {
                append_error(err, err_cap, "unterminated quoted value");
                return false;
            }
            p++;
            if (*p && !isspace((unsigned char)*p)) {
                append_error(err, err_cap, "expected whitespace after quoted value");
                return false;
            }
        } else {
            while (*p && !isspace((unsigned char)*p)) {
                if (!append_value_char(value, &value_len, *p, err, err_cap)) {
                    return false;
                }
                p++;
            }
        }
        if (!add_field(record, key, value, err, err_cap)) {
            return false;
        }
    }
    return true;
}

static const char *record_get(const Record *record, const char *key) {
    for (size_t i = 0; i < record->count; i++) {
        if (streq(record->fields[i].key, key)) {
            return record->fields[i].value;
        }
    }
    return NULL;
}

static const char *record_get_any(const Record *record, const char *const *keys) {
    for (size_t i = 0; keys[i]; i++) {
        const char *value = record_get(record, keys[i]);
        if (value) {
            return value;
        }
    }
    return NULL;
}

static bool parse_double_strict(const char *value, double *out) {
    char *end = NULL;
    errno = 0;
    double parsed = strtod(value, &end);
    if (errno != 0 || end == value) {
        return false;
    }
    while (*end) {
        if (!isspace((unsigned char)*end)) {
            return false;
        }
        end++;
    }
    *out = parsed;
    return true;
}

static bool read_double_any(const Record *record, const char *const *keys, double def, double *out, char *err, size_t err_cap) {
    const char *raw = record_get_any(record, keys);
    if (!raw) {
        *out = def;
        return true;
    }
    if (!parse_double_strict(raw, out)) {
        append_error(err, err_cap, "field '%s' must be a number, got '%s'", keys[0], raw);
        return false;
    }
    return true;
}

static bool read_long_flag(const char *raw, long min_value, long *out, const char *name) {
    char *end = NULL;
    errno = 0;
    long value = strtol(raw, &end, 10);
    if (errno != 0 || end == raw || *end != '\0' || value < min_value) {
        fprintf(stderr, "error: %s expects an integer >= %ld, got '%s'\n", name, min_value, raw);
        return false;
    }
    *out = value;
    return true;
}

static bool read_double_flag(const char *raw, double min_value, double *out, const char *name) {
    double value = 0.0;
    if (!parse_double_strict(raw, &value) || value < min_value) {
        fprintf(stderr, "error: %s expects a number >= %.6f, got '%s'\n", name, min_value, raw);
        return false;
    }
    *out = value;
    return true;
}

static uint64_t hash_string(const char *s) {
    uint64_t hash = 1469598103934665603ULL;
    while (*s) {
        hash ^= (unsigned char)*s++;
        hash *= 1099511628211ULL;
    }
    return hash;
}

static void tenant_table_init(TenantTable *table, size_t bucket_count) {
    table->capacity = DEFAULT_TABLE_CAPACITY;
    table->count = 0;
    table->bucket_count = bucket_count;
    table->items = (TenantState *)xcalloc(table->capacity, sizeof(TenantState));
}

static void tenant_table_destroy(TenantTable *table) {
    for (size_t i = 0; i < table->capacity; i++) {
        if (table->items[i].in_use) {
            free(table->items[i].name);
            free(table->items[i].buckets);
        }
    }
    free(table->items);
}

static void tenant_table_place_existing(TenantTable *table, TenantState state) {
    size_t idx = (size_t)(hash_string(state.name) % table->capacity);
    while (table->items[idx].in_use) {
        idx = (idx + 1) % table->capacity;
    }
    table->items[idx] = state;
    table->count++;
}

static void tenant_table_rehash(TenantTable *table) {
    TenantState *old_items = table->items;
    size_t old_capacity = table->capacity;
    table->capacity *= 2;
    table->items = (TenantState *)xcalloc(table->capacity, sizeof(TenantState));
    table->count = 0;
    for (size_t i = 0; i < old_capacity; i++) {
        if (old_items[i].in_use) {
            tenant_table_place_existing(table, old_items[i]);
        }
    }
    free(old_items);
}

static TenantState *tenant_table_get(TenantTable *table, const char *name) {
    if ((table->count + 1) * 10 >= table->capacity * 7) {
        tenant_table_rehash(table);
    }
    size_t idx = (size_t)(hash_string(name) % table->capacity);
    while (table->items[idx].in_use) {
        if (streq(table->items[idx].name, name)) {
            return &table->items[idx];
        }
        idx = (idx + 1) % table->capacity;
    }
    table->items[idx].name = xstrdup(name);
    table->items[idx].buckets = (Bucket *)xcalloc(table->bucket_count, sizeof(Bucket));
    table->items[idx].in_use = true;
    table->count++;
    return &table->items[idx];
}

static Bucket *bucket_for(TenantState *state, size_t bucket_count, long ts) {
    size_t idx = (size_t)(ts >= 0 ? ts : 0) % bucket_count;
    Bucket *bucket = &state->buckets[idx];
    if (bucket->ts != ts) {
        memset(bucket, 0, sizeof(*bucket));
        bucket->ts = ts;
    }
    return bucket;
}

static void add_usage(TenantState *state, size_t bucket_count, long ts, double usd, double tokens, bool accepted) {
    Bucket *bucket = bucket_for(state, bucket_count, ts);
    if (accepted) {
        bucket->usd += usd;
        bucket->tokens += tokens;
        bucket->accepted++;
    } else {
        bucket->rejected++;
    }
}

static WindowTotals window_totals(const TenantState *state, size_t bucket_count, long now) {
    WindowTotals totals = {0};
    long earliest = now - (long)bucket_count + 1;
    for (size_t i = 0; i < bucket_count; i++) {
        const Bucket *bucket = &state->buckets[i];
        if (bucket->ts >= earliest && bucket->ts <= now) {
            totals.usd += bucket->usd;
            totals.tokens += bucket->tokens;
            totals.accepted += bucket->accepted;
            totals.rejected += bucket->rejected;
        }
    }
    if (totals.usd < 0.0) {
        totals.usd = 0.0;
    }
    if (totals.tokens < 0.0) {
        totals.tokens = 0.0;
    }
    return totals;
}

static bool is_limited(double limit) {
    return limit < DBL_MAX / 4.0;
}

static double bucket_metric_at(const TenantState *state, size_t bucket_count, long ts, bool tokens) {
    size_t idx = (size_t)(ts >= 0 ? ts : 0) % bucket_count;
    const Bucket *bucket = &state->buckets[idx];
    if (bucket->ts != ts) {
        return 0.0;
    }
    return tokens ? bucket->tokens : bucket->usd;
}

static long retry_after_ms(const TenantState *state, size_t bucket_count, long now, double add, double limit, bool tokens) {
    if (!is_limited(limit)) {
        return 0;
    }
    WindowTotals totals = window_totals(state, bucket_count, now);
    double current = tokens ? totals.tokens : totals.usd;
    if (current + add <= limit) {
        return 0;
    }
    long earliest = now - (long)bucket_count + 1;
    for (long ts = earliest; ts <= now; ts++) {
        current -= bucket_metric_at(state, bucket_count, ts, tokens);
        long wait_ms = (ts + (long)bucket_count - now) * 1000L;
        if (current + add <= limit) {
            return wait_ms > 0 ? wait_ms : 1000L;
        }
    }
    return (long)bucket_count * 1000L;
}

static bool printable_value_char(unsigned char c) {
    return isalnum(c) || c == '_' || c == '-' || c == '.' || c == ':' || c == '/' || c == '@';
}

static void print_value(const char *value) {
    if (!value || !*value) {
        putchar('-');
        return;
    }
    for (const unsigned char *p = (const unsigned char *)value; *p; p++) {
        if (printable_value_char(*p)) {
            putchar((int)*p);
        } else {
            printf("%%%02X", *p);
        }
    }
}

static bool build_estimate(const Config *cfg, const Record *record, Estimate *estimate, char *err, size_t err_cap) {
    static const char *const input_keys[] = {"input_tokens", "prompt_tokens", "tokens_in", NULL};
    static const char *const output_keys[] = {"max_output_tokens", "output_tokens_est", "output_tokens", "completion_tokens", "tokens_out", NULL};
    static const char *const total_keys[] = {"total_tokens", "tokens", NULL};
    static const char *const cost_keys[] = {"cost_usd", "usd", NULL};
    static const char *const input_price_keys[] = {"usd_in_per_mtok", "input_usd_per_mtok", NULL};
    static const char *const output_price_keys[] = {"usd_out_per_mtok", "output_usd_per_mtok", NULL};
    static const char *const total_price_keys[] = {"usd_per_mtok", "price_usd_per_mtok", NULL};
    static const char *const carbon_keys[] = {"carbon_gco2e", "gco2e", NULL};
    static const char *const carbon_rate_keys[] = {"gco2e_per_1k_tokens", "carbon_g_per_1k_tokens", NULL};
    static const char *const priority_keys[] = {"priority", "urgency", NULL};

    memset(estimate, 0, sizeof(*estimate));
    estimate->event = record_get(record, "event");
    if (!estimate->event) {
        estimate->event = "request";
    }
    estimate->id = record_get(record, "id");
    if (!estimate->id) {
        estimate->id = record_get(record, "request_id");
    }
    estimate->tenant = record_get(record, "tenant");
    if (!estimate->tenant || !*estimate->tenant) {
        estimate->tenant = "anonymous";
    }
    estimate->region = record_get(record, "region");
    estimate->required_region = record_get(record, "required_region");
    if (!estimate->required_region) {
        estimate->required_region = record_get(record, "data_region");
    }

    double input_tokens = 0.0;
    double output_tokens = 0.0;
    double total_tokens = 0.0;
    double explicit_cost = -1.0;
    double input_price = cfg->default_input_usd_per_mtok;
    double output_price = cfg->default_output_usd_per_mtok;
    double total_price = -1.0;
    double explicit_carbon = -1.0;
    double carbon_rate = 0.0;
    double priority = 0.0;

    if (!read_double_any(record, input_keys, 0.0, &input_tokens, err, err_cap)) return false;
    if (!read_double_any(record, output_keys, 0.0, &output_tokens, err, err_cap)) return false;
    if (!read_double_any(record, total_keys, 0.0, &total_tokens, err, err_cap)) return false;
    if (!read_double_any(record, cost_keys, -1.0, &explicit_cost, err, err_cap)) return false;
    if (!read_double_any(record, input_price_keys, input_price, &input_price, err, err_cap)) return false;
    if (!read_double_any(record, output_price_keys, output_price, &output_price, err, err_cap)) return false;
    if (!read_double_any(record, total_price_keys, -1.0, &total_price, err, err_cap)) return false;
    if (!read_double_any(record, carbon_keys, -1.0, &explicit_carbon, err, err_cap)) return false;
    if (!read_double_any(record, carbon_rate_keys, 0.0, &carbon_rate, err, err_cap)) return false;
    if (!read_double_any(record, priority_keys, 0.0, &priority, err, err_cap)) return false;

    if (input_tokens < 0.0 || output_tokens < 0.0 || total_tokens < 0.0) {
        append_error(err, err_cap, "token counts must be non-negative");
        return false;
    }
    if (total_tokens == 0.0) {
        total_tokens = input_tokens + output_tokens;
    }
    if (output_tokens == 0.0 && total_tokens > input_tokens) {
        output_tokens = total_tokens - input_tokens;
    }
    if (explicit_cost >= 0.0) {
        estimate->cost_usd = explicit_cost;
    } else if (total_price >= 0.0) {
        estimate->cost_usd = total_tokens * total_price / 1000000.0;
    } else {
        estimate->cost_usd = (input_tokens * input_price + output_tokens * output_price) / 1000000.0;
    }
    estimate->input_tokens = input_tokens;
    estimate->output_tokens = output_tokens;
    estimate->total_tokens = total_tokens;
    estimate->carbon_g = explicit_carbon >= 0.0 ? explicit_carbon : (carbon_rate * total_tokens / 1000.0);
    if (priority < 0.0) priority = 0.0;
    if (priority > 10.0) priority = 10.0;
    estimate->priority = (int)(priority + 0.5);
    return true;
}

static bool region_mismatch(const Estimate *estimate) {
    if (!estimate->region || !estimate->required_region) {
        return false;
    }
    return !streq(estimate->region, estimate->required_region);
}

static void emit_parse_error(long line_no, const char *line, const char *error, bool fail_open) {
    printf("ts=%ld event=decision id=- tenant=- decision=%s reason=parse_error line=%ld detail=", line_no,
           fail_open ? "allow" : "error", line_no);
    print_value(error);
    printf(" raw=");
    print_value(line);
    putchar('\n');
}

static void emit_observed(long ts, const Estimate *estimate, WindowTotals tenant_after, WindowTotals global_after) {
    printf("ts=%ld event=observed id=", ts);
    print_value(estimate->id);
    printf(" tenant=");
    print_value(estimate->tenant);
    printf(" cost_usd=%.8f tokens=%.0f tenant_window_usd=%.8f tenant_window_tokens=%.0f global_window_usd=%.8f global_window_tokens=%.0f\n",
           estimate->cost_usd, estimate->total_tokens, tenant_after.usd, tenant_after.tokens, global_after.usd, global_after.tokens);
}

static void emit_decision(long ts, const Estimate *estimate, bool allow, bool shadowed, const char *reason,
                          long retry_ms, WindowTotals tenant_before, WindowTotals global_before,
                          double tenant_usd_limit, double global_usd_limit, double tenant_token_limit,
                          double global_token_limit) {
    printf("ts=%ld event=decision id=", ts);
    print_value(estimate->id);
    printf(" tenant=");
    print_value(estimate->tenant);
    printf(" decision=%s reason=", allow ? "allow" : "throttle");
    print_value(reason);
    printf(" shadow=%s priority=%d estimate_usd=%.8f estimate_tokens=%.0f carbon_gco2e=%.4f retry_after_ms=%ld",
           shadowed ? "true" : "false", estimate->priority, estimate->cost_usd, estimate->total_tokens,
           estimate->carbon_g, retry_ms);
    printf(" tenant_window_usd=%.8f tenant_window_tokens=%.0f global_window_usd=%.8f global_window_tokens=%.0f",
           tenant_before.usd, tenant_before.tokens, global_before.usd, global_before.tokens);
    printf(" tenant_usd_limit=%.8f global_usd_limit=%.8f tenant_token_limit=%.0f global_token_limit=%.0f\n",
           tenant_usd_limit, global_usd_limit, tenant_token_limit, global_token_limit);
}

static bool exceeds(double current, double add, double limit) {
    return is_limited(limit) && current + add > limit;
}

static int process_record(const Config *cfg, TenantTable *tenants, TenantState *global_state,
                          const Record *record, long line_no, const char *raw_line) {
    if (record->count == 0) {
        return 0;
    }
    char err[256] = {0};
    Estimate estimate;
    if (!build_estimate(cfg, record, &estimate, err, sizeof(err))) {
        emit_parse_error(line_no, raw_line, err, cfg->fail_open);
        return cfg->fail_open ? 0 : 2;
    }

    static const char *const ts_keys[] = {"ts", "time", "timestamp", NULL};
    double ts_value = (double)line_no;
    if (!read_double_any(record, ts_keys, (double)line_no, &ts_value, err, sizeof(err))) {
        emit_parse_error(line_no, raw_line, err, cfg->fail_open);
        return cfg->fail_open ? 0 : 2;
    }
    long ts = (long)ts_value;
    TenantState *tenant = tenant_table_get(tenants, estimate.tenant);

    if (streq(estimate.event, "observe") || streq(estimate.event, "settle") || streq(estimate.event, "usage")) {
        add_usage(tenant, tenants->bucket_count, ts, estimate.cost_usd, estimate.total_tokens, true);
        add_usage(global_state, tenants->bucket_count, ts, estimate.cost_usd, estimate.total_tokens, true);
        emit_observed(ts, &estimate, window_totals(tenant, tenants->bucket_count, ts), window_totals(global_state, tenants->bucket_count, ts));
        return 0;
    }
    if (!streq(estimate.event, "request")) {
        snprintf(err, sizeof(err), "unsupported event '%s'", estimate.event);
        emit_parse_error(line_no, raw_line, err, cfg->fail_open);
        return cfg->fail_open ? 0 : 2;
    }

    WindowTotals tenant_before = window_totals(tenant, tenants->bucket_count, ts);
    WindowTotals global_before = window_totals(global_state, tenants->bucket_count, ts);
    double priority_borrow = 1.0 + ((double)estimate.priority * 0.015);
    double effective_burst = cfg->burst * priority_borrow;
    double tenant_usd_limit = cfg->tenant_usd * effective_burst;
    double global_usd_limit = cfg->global_usd * effective_burst;
    double tenant_token_limit = cfg->tenant_tokens * effective_burst;
    double global_token_limit = cfg->global_tokens * effective_burst;

    bool allow = true;
    const char *reason = "ok";
    long retry_ms = 0;
    if (is_limited(cfg->max_carbon_g) && estimate.carbon_g > cfg->max_carbon_g) {
        allow = false;
        reason = "carbon_budget";
    } else if (cfg->require_region_match && region_mismatch(&estimate)) {
        allow = false;
        reason = "region_mismatch";
    } else if (exceeds(tenant_before.usd, estimate.cost_usd, tenant_usd_limit)) {
        allow = false;
        reason = "tenant_usd_budget";
        retry_ms = retry_after_ms(tenant, tenants->bucket_count, ts, estimate.cost_usd, tenant_usd_limit, false);
    } else if (exceeds(tenant_before.tokens, estimate.total_tokens, tenant_token_limit)) {
        allow = false;
        reason = "tenant_token_budget";
        retry_ms = retry_after_ms(tenant, tenants->bucket_count, ts, estimate.total_tokens, tenant_token_limit, true);
    } else if (exceeds(global_before.usd, estimate.cost_usd, global_usd_limit)) {
        allow = false;
        reason = "global_usd_budget";
        retry_ms = retry_after_ms(global_state, tenants->bucket_count, ts, estimate.cost_usd, global_usd_limit, false);
    } else if (exceeds(global_before.tokens, estimate.total_tokens, global_token_limit)) {
        allow = false;
        reason = "global_token_budget";
        retry_ms = retry_after_ms(global_state, tenants->bucket_count, ts, estimate.total_tokens, global_token_limit, true);
    }

    bool shadowed = false;
    if (!allow && cfg->shadow) {
        shadowed = true;
        allow = true;
    }

    if (allow) {
        add_usage(tenant, tenants->bucket_count, ts, estimate.cost_usd, estimate.total_tokens, true);
        add_usage(global_state, tenants->bucket_count, ts, estimate.cost_usd, estimate.total_tokens, true);
    } else {
        add_usage(tenant, tenants->bucket_count, ts, 0.0, 0.0, false);
        add_usage(global_state, tenants->bucket_count, ts, 0.0, 0.0, false);
    }
    emit_decision(ts, &estimate, allow, shadowed, reason, retry_ms, tenant_before, global_before,
                  tenant_usd_limit, global_usd_limit, tenant_token_limit, global_token_limit);
    return 0;
}

static Config default_config(void) {
    Config cfg;
    cfg.window_sec = 60;
    cfg.tenant_usd = 0.25;
    cfg.global_usd = 10.0;
    cfg.tenant_tokens = 250000.0;
    cfg.global_tokens = 5000000.0;
    cfg.burst = 1.10;
    cfg.default_input_usd_per_mtok = 0.15;
    cfg.default_output_usd_per_mtok = 0.60;
    cfg.max_carbon_g = DBL_MAX;
    cfg.require_region_match = false;
    cfg.shadow = false;
    cfg.fail_open = false;
    return cfg;
}

static const char *require_value(int *index, int argc, char **argv, const char *flag) {
    if (*index + 1 >= argc) {
        die("%s requires a value", flag);
    }
    (*index)++;
    return argv[*index];
}

static void usage(FILE *out) {
    fputs(
        "Usage: EdgeInferenceAdmission [options] < requests.kv\n"
        "\n"
        "Reads one key=value record per line and emits key=value admission decisions.\n"
        "Events: event=request gates an estimated LLM call; event=observe, usage, or settle records known usage.\n"
        "Common fields: ts, id, tenant, input_tokens, max_output_tokens, total_tokens, cost_usd,\n"
        "usd_in_per_mtok, usd_out_per_mtok, priority, region, required_region, carbon_gco2e.\n"
        "\n"
        "Options:\n"
        "  --window-sec N                  rolling budget window, default 60\n"
        "  --tenant-usd N                  tenant USD budget per window, default 0.25\n"
        "  --global-usd N                  global USD budget per window, default 10.0\n"
        "  --tenant-tokens N               tenant token budget per window, default 250000\n"
        "  --global-tokens N               global token budget per window, default 5000000\n"
        "  --burst N                       burst multiplier, default 1.10\n"
        "  --default-input-usd-per-mtok N   fallback input price, default 0.15\n"
        "  --default-output-usd-per-mtok N  fallback output price, default 0.60\n"
        "  --max-carbon-g N                reject requests above grams CO2e estimate\n"
        "  --require-region-match          require region and required_region to match\n"
        "  --shadow                        print throttles but allow and account requests\n"
        "  --fail-open                     malformed lines become allow decisions\n"
        "  --help                          show this help\n",
        out);
}

static Config parse_args(int argc, char **argv) {
    Config cfg = default_config();
    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];
        if (streq(arg, "--help")) {
            usage(stdout);
            exit(0);
        } else if (streq(arg, "--window-sec")) {
            if (!read_long_flag(require_value(&i, argc, argv, arg), 1, &cfg.window_sec, arg)) exit(2);
        } else if (streq(arg, "--tenant-usd")) {
            if (!read_double_flag(require_value(&i, argc, argv, arg), 0.0, &cfg.tenant_usd, arg)) exit(2);
        } else if (streq(arg, "--global-usd")) {
            if (!read_double_flag(require_value(&i, argc, argv, arg), 0.0, &cfg.global_usd, arg)) exit(2);
        } else if (streq(arg, "--tenant-tokens")) {
            if (!read_double_flag(require_value(&i, argc, argv, arg), 0.0, &cfg.tenant_tokens, arg)) exit(2);
        } else if (streq(arg, "--global-tokens")) {
            if (!read_double_flag(require_value(&i, argc, argv, arg), 0.0, &cfg.global_tokens, arg)) exit(2);
        } else if (streq(arg, "--burst")) {
            if (!read_double_flag(require_value(&i, argc, argv, arg), 1.0, &cfg.burst, arg)) exit(2);
        } else if (streq(arg, "--default-input-usd-per-mtok")) {
            if (!read_double_flag(require_value(&i, argc, argv, arg), 0.0, &cfg.default_input_usd_per_mtok, arg)) exit(2);
        } else if (streq(arg, "--default-output-usd-per-mtok")) {
            if (!read_double_flag(require_value(&i, argc, argv, arg), 0.0, &cfg.default_output_usd_per_mtok, arg)) exit(2);
        } else if (streq(arg, "--max-carbon-g")) {
            if (!read_double_flag(require_value(&i, argc, argv, arg), 0.0, &cfg.max_carbon_g, arg)) exit(2);
        } else if (streq(arg, "--require-region-match")) {
            cfg.require_region_match = true;
        } else if (streq(arg, "--shadow")) {
            cfg.shadow = true;
        } else if (streq(arg, "--fail-open")) {
            cfg.fail_open = true;
        } else {
            die("unknown option '%s'", arg);
        }
    }
    return cfg;
}

int main(int argc, char **argv) {
    Config cfg = parse_args(argc, argv);
    TenantTable tenants;
    tenant_table_init(&tenants, (size_t)cfg.window_sec);

    TenantState global_state;
    memset(&global_state, 0, sizeof(global_state));
    global_state.name = (char *)"__global__";
    global_state.buckets = (Bucket *)xcalloc((size_t)cfg.window_sec, sizeof(Bucket));
    global_state.in_use = true;

    int exit_code = 0;
    long line_no = 0;
    char *line = NULL;
    while ((line = read_line(stdin)) != NULL) {
        line_no++;
        Record record;
        char err[256] = {0};
        if (!parse_record(line, &record, err, sizeof(err))) {
            emit_parse_error(line_no, line, err, cfg.fail_open);
            if (!cfg.fail_open) {
                exit_code = 2;
                free(line);
                break;
            }
            free(line);
            continue;
        }
        int rc = process_record(&cfg, &tenants, &global_state, &record, line_no, line);
        free(line);
        if (rc != 0) {
            exit_code = rc;
            break;
        }
    }

    free(global_state.buckets);
    tenant_table_destroy(&tenants);
    return exit_code;
}

/*
This solves the April 2026 problem where AI gateways, agent platforms, edge workers, and internal developer tools need to stop runaway LLM spend before a streaming request starts, not hours later in a billing dashboard. Built because Pavan has watched teams wire model calls straight into product code, then discover that one noisy tenant, one long tool loop, one wrong max_output_tokens value, or one expensive region can burn the shared budget and hurt everyone else. Use it when you need a small C17 admission controller for per tenant token budgets, rolling cost limits, AI infrastructure throttling, LLM request governance, carbon aware inference routing, and data residency checks without Redis, Kafka, OpenTelemetry collectors, or another SaaS dependency in the hot path. The trick: the program reads plain key=value records, keeps exact rolling buckets per tenant plus a global bucket, prices input and output tokens separately, applies priority aware burst math, emits retry_after_ms, and can run in shadow mode so a team can compare policy results before enforcing them. Drop this into an Envoy external processor, Nginx njs bridge, Fastly Compute build, Cloudflare Worker sidecar, GitHub Actions load test, model gateway, research batch runner, or DevOps incident script when search terms like LLM cost guardrail, AI gateway rate limiter, token budget admission control, edge inference throttle, carbon aware AI routing, and multi tenant model spend control need to map to a real source file that an engineer can compile and use today.
*/
