/*
 * McpTraceSanitizer.c
 *
 * Streaming sanitizer for MCP, LLM gateway, browser automation, and CI traces.
 * It redacts common AI/API secrets without needing jq, Python, or a JSON schema.
 *
 * Build:
 *   cc -std=c11 -O2 -Wall -Wextra -pedantic McpTraceSanitizer.c -o mcp-trace-sanitizer
 *
 * Examples:
 *   ./mcp-trace-sanitizer --input trace.jsonl --output trace.safe.jsonl --report report.json
 *   cat tool.log | ./mcp-trace-sanitizer --fail-on-leak > tool.safe.log
 */

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXIT_CONFIG 64
#define EXIT_DATA 65
#define EXIT_LEAK 2

typedef enum {
    KIND_NAMED_SECRET = 0,
    KIND_AUTH_HEADER,
    KIND_QUERY_PARAM,
    KIND_OPENAI_STYLE_KEY,
    KIND_GITHUB_TOKEN,
    KIND_SLACK_TOKEN,
    KIND_AWS_ACCESS_KEY,
    KIND_GOOGLE_API_KEY,
    KIND_JWT,
    KIND_PRIVATE_KEY,
    KIND_HIGH_ENTROPY,
    KIND_COUNT
} RedactionKind;

typedef struct {
    size_t start;
    size_t end;
    RedactionKind kind;
    uint64_t fingerprint;
} Range;

typedef struct {
    Range *items;
    size_t len;
    size_t cap;
} RangeVec;

typedef struct {
    char **items;
    size_t len;
    size_t cap;
} StringVec;

typedef struct {
    const char *input_path;
    const char *output_path;
    const char *report_path;
    bool fail_on_leak;
    bool entropy_enabled;
    size_t min_token_len;
    StringVec allowed_keys;
} Options;

typedef struct {
    uint64_t lines;
    uint64_t bytes_in;
    uint64_t bytes_out;
    uint64_t redactions;
    uint64_t by_kind[KIND_COUNT];
} Stats;

typedef struct {
    bool pem_active;
} ScanState;

typedef struct {
    FILE *file;
    bool should_close;
} FileHandle;

static const char *kind_label(RedactionKind kind) {
    switch (kind) {
        case KIND_NAMED_SECRET: return "named_secret";
        case KIND_AUTH_HEADER: return "auth_header";
        case KIND_QUERY_PARAM: return "query_param";
        case KIND_OPENAI_STYLE_KEY: return "ai_api_key";
        case KIND_GITHUB_TOKEN: return "github_token";
        case KIND_SLACK_TOKEN: return "slack_token";
        case KIND_AWS_ACCESS_KEY: return "aws_access_key";
        case KIND_GOOGLE_API_KEY: return "google_api_key";
        case KIND_JWT: return "jwt";
        case KIND_PRIVATE_KEY: return "private_key";
        case KIND_HIGH_ENTROPY: return "high_entropy";
        case KIND_COUNT: return "unknown";
    }
    return "unknown";
}

static void die_usage(const char *message);

static void *xmalloc(size_t size) {
    void *ptr = malloc(size == 0 ? 1 : size);
    if (ptr == NULL) {
        fprintf(stderr, "allocation failed\n");
        exit(EXIT_DATA);
    }
    return ptr;
}

static void *xrealloc(void *ptr, size_t size) {
    void *next = realloc(ptr, size == 0 ? 1 : size);
    if (next == NULL) {
        fprintf(stderr, "allocation failed\n");
        exit(EXIT_DATA);
    }
    return next;
}

static bool add_overflow_size(size_t a, size_t b, size_t *out) {
    if (a > ((size_t)-1) - b) {
        return true;
    }
    *out = a + b;
    return false;
}

static bool mul_overflow_size(size_t a, size_t b, size_t *out) {
    if (a != 0 && b > ((size_t)-1) / a) {
        return true;
    }
    *out = a * b;
    return false;
}

static bool is_space_char(char c) {
    return isspace((unsigned char)c) != 0;
}

static bool is_key_char(char c) {
    unsigned char u = (unsigned char)c;
    return isalnum(u) || c == '_' || c == '-' || c == '.';
}

static bool is_token_char(char c) {
    unsigned char u = (unsigned char)c;
    return isalnum(u) || c == '_' || c == '-' || c == '.' || c == '~' ||
           c == '/' || c == '+' || c == '=';
}

static bool is_base64url_char(char c) {
    unsigned char u = (unsigned char)c;
    return isalnum(u) || c == '-' || c == '_' || c == '.';
}

static char lower_ascii(char c) {
    if (c >= 'A' && c <= 'Z') {
        return (char)(c - 'A' + 'a');
    }
    return c;
}

static bool ascii_equal_ci(char a, char b) {
    return lower_ascii(a) == lower_ascii(b);
}

static bool starts_with_ci_at(const char *s, size_t n, size_t pos, const char *prefix) {
    size_t pn = strlen(prefix);
    if (pos > n || pn > n - pos) {
        return false;
    }
    for (size_t i = 0; i < pn; i++) {
        if (!ascii_equal_ci(s[pos + i], prefix[i])) {
            return false;
        }
    }
    return true;
}

static bool contains_ci_slice(const char *s, size_t n, const char *needle) {
    size_t nn = strlen(needle);
    if (nn == 0 || nn > n) {
        return false;
    }
    for (size_t i = 0; i + nn <= n; i++) {
        if (starts_with_ci_at(s, n, i, needle)) {
            return true;
        }
    }
    return false;
}

static bool ends_with_str(const char *s, const char *suffix) {
    size_t n = strlen(s);
    size_t sn = strlen(suffix);
    if (sn > n) {
        return false;
    }
    return strcmp(s + n - sn, suffix) == 0;
}

static void string_vec_push(StringVec *vec, char *value) {
    if (vec->len == vec->cap) {
        size_t next_cap = vec->cap == 0 ? 8 : vec->cap * 2;
        size_t bytes = 0;
        if (mul_overflow_size(next_cap, sizeof(char *), &bytes)) {
            fprintf(stderr, "too many allowed keys\n");
            exit(EXIT_CONFIG);
        }
        vec->items = (char **)xrealloc(vec->items, bytes);
        vec->cap = next_cap;
    }
    vec->items[vec->len++] = value;
}

static void string_vec_free(StringVec *vec) {
    for (size_t i = 0; i < vec->len; i++) {
        free(vec->items[i]);
    }
    free(vec->items);
    vec->items = NULL;
    vec->len = 0;
    vec->cap = 0;
}

static char *normalize_key_alloc(const char *key) {
    size_t n = strlen(key);
    char *out = (char *)xmalloc(n + 1);
    size_t j = 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)key[i];
        if (isalnum(c)) {
            out[j++] = lower_ascii((char)c);
        }
    }
    out[j] = '\0';
    return out;
}

static size_t normalize_key_slice(const char *s, size_t start, size_t end, char *out, size_t cap) {
    size_t j = 0;
    for (size_t i = start; i < end && j + 1 < cap; i++) {
        unsigned char c = (unsigned char)s[i];
        if (isalnum(c)) {
            out[j++] = lower_ascii((char)c);
        }
    }
    out[j] = '\0';
    return j;
}

static bool option_key_allowed(const Options *opt, const char *normalized) {
    for (size_t i = 0; i < opt->allowed_keys.len; i++) {
        if (strcmp(opt->allowed_keys.items[i], normalized) == 0) {
            return true;
        }
    }
    return false;
}

static bool normalized_contains(const char *s, const char *needle) {
    return strstr(s, needle) != NULL;
}

static bool is_sensitive_normalized_key(const Options *opt, const char *normalized) {
    static const char *exact[] = {
        "apikey", "xapikey", "openaiapikey", "anthropicapikey", "geminiapikey",
        "googleapikey", "firecrawlapikey", "mistralapikey", "perplexityapikey",
        "authorization", "proxyauthorization", "cookie", "setcookie",
        "accesstoken", "refreshtoken", "idtoken", "clientsecret",
        "githubtoken", "slacktoken", "sessiontoken", "bearertoken",
        "password", "passwd", "privatekey", "secretkey", "signingkey",
        "webhooksecret", "databaseurl", "dburl", "connectionstring",
        "awssecretaccesskey", "awsaccesskeyid", "sshprivatekey"
    };

    if (normalized[0] == '\0' || option_key_allowed(opt, normalized)) {
        return false;
    }

    for (size_t i = 0; i < sizeof(exact) / sizeof(exact[0]); i++) {
        if (strcmp(normalized, exact[i]) == 0) {
            return true;
        }
    }

    if (normalized_contains(normalized, "secret") ||
        normalized_contains(normalized, "password") ||
        normalized_contains(normalized, "passwd") ||
        normalized_contains(normalized, "credential") ||
        normalized_contains(normalized, "privatekey")) {
        return true;
    }

    if (strcmp(normalized, "token") == 0 || ends_with_str(normalized, "token")) {
        if (normalized_contains(normalized, "prompttoken") ||
            normalized_contains(normalized, "completiontoken") ||
            normalized_contains(normalized, "totaltoken") ||
            normalized_contains(normalized, "maxtoken")) {
            return false;
        }
        return true;
    }

    return false;
}

static uint64_t fnv1a64_slice(const char *s, size_t start, size_t end) {
    uint64_t hash = UINT64_C(1469598103934665603);
    for (size_t i = start; i < end; i++) {
        hash ^= (unsigned char)s[i];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static void range_vec_init(RangeVec *vec) {
    vec->items = NULL;
    vec->len = 0;
    vec->cap = 0;
}

static void range_vec_free(RangeVec *vec) {
    free(vec->items);
    vec->items = NULL;
    vec->len = 0;
    vec->cap = 0;
}

static void range_vec_push(RangeVec *vec, const char *line, size_t start, size_t end, RedactionKind kind) {
    while (start < end && is_space_char(line[start])) {
        start++;
    }
    while (end > start && is_space_char(line[end - 1])) {
        end--;
    }
    if (end <= start) {
        return;
    }
    if (vec->len == vec->cap) {
        size_t next_cap = vec->cap == 0 ? 16 : vec->cap * 2;
        size_t bytes = 0;
        if (mul_overflow_size(next_cap, sizeof(Range), &bytes)) {
            fprintf(stderr, "too many redaction ranges on one line\n");
            exit(EXIT_DATA);
        }
        vec->items = (Range *)xrealloc(vec->items, bytes);
        vec->cap = next_cap;
    }
    vec->items[vec->len].start = start;
    vec->items[vec->len].end = end;
    vec->items[vec->len].kind = kind;
    vec->items[vec->len].fingerprint = fnv1a64_slice(line, start, end);
    vec->len++;
}

static int compare_ranges(const void *a, const void *b) {
    const Range *ra = (const Range *)a;
    const Range *rb = (const Range *)b;
    if (ra->start < rb->start) return -1;
    if (ra->start > rb->start) return 1;
    size_t la = ra->end - ra->start;
    size_t lb = rb->end - rb->start;
    if (la > lb) return -1;
    if (la < lb) return 1;
    return (int)ra->kind - (int)rb->kind;
}

static size_t skip_spaces(const char *s, size_t n, size_t pos) {
    while (pos < n && is_space_char(s[pos])) {
        pos++;
    }
    return pos;
}

static size_t find_closing_quote(const char *s, size_t n, size_t pos, char quote) {
    bool escaped = false;
    for (size_t i = pos; i < n; i++) {
        if (escaped) {
            escaped = false;
            continue;
        }
        if (s[i] == '\\') {
            escaped = true;
            continue;
        }
        if (s[i] == quote) {
            return i;
        }
    }
    return n;
}

static size_t trim_right_value(const char *s, size_t start, size_t end) {
    while (end > start) {
        char c = s[end - 1];
        if (is_space_char(c) || c == '"' || c == '\'') {
            end--;
        } else {
            break;
        }
    }
    return end;
}

static void add_named_value_range(const char *s, size_t n, size_t value_pos, char separator, RangeVec *ranges) {
    size_t p = skip_spaces(s, n, value_pos);
    if (p >= n || s[p] == '\n' || s[p] == '\r') {
        return;
    }

    if (s[p] == '"' || s[p] == '\'') {
        char quote = s[p];
        size_t value_start = p + 1;
        size_t value_end = find_closing_quote(s, n, value_start, quote);
        range_vec_push(ranges, s, value_start, value_end, KIND_NAMED_SECRET);
        return;
    }

    size_t end = p;
    while (end < n && s[end] != '\n' && s[end] != '\r') {
        if (s[end] == '&' || s[end] == ';') {
            break;
        }
        if (separator == '=' && (is_space_char(s[end]) || s[end] == ',')) {
            break;
        }
        if (separator == ':' && s[end] == ',') {
            break;
        }
        end++;
    }

    end = trim_right_value(s, p, end);
    range_vec_push(ranges, s, p, end, KIND_NAMED_SECRET);
}

static void scan_named_values(const Options *opt, const char *s, size_t n, RangeVec *ranges) {
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '"' || s[i] == '\'') {
            char quote = s[i];
            size_t key_start = i + 1;
            size_t key_end = find_closing_quote(s, n, key_start, quote);
            if (key_end >= n) {
                break;
            }
            size_t sep = skip_spaces(s, n, key_end + 1);
            if (sep < n && (s[sep] == ':' || s[sep] == '=')) {
                char normalized[160];
                normalize_key_slice(s, key_start, key_end, normalized, sizeof(normalized));
                if (is_sensitive_normalized_key(opt, normalized)) {
                    add_named_value_range(s, n, sep + 1, s[sep], ranges);
                }
            }
            i = key_end;
            continue;
        }

        if (!is_key_char(s[i]) || (i > 0 && is_key_char(s[i - 1]))) {
            continue;
        }

        size_t key_start = i;
        size_t key_end = i;
        while (key_end < n && is_key_char(s[key_end])) {
            key_end++;
        }
        size_t sep = skip_spaces(s, n, key_end);
        if (sep < n && (s[sep] == ':' || s[sep] == '=')) {
            char normalized[160];
            normalize_key_slice(s, key_start, key_end, normalized, sizeof(normalized));
            if (is_sensitive_normalized_key(opt, normalized)) {
                add_named_value_range(s, n, sep + 1, s[sep], ranges);
            }
        }
        i = key_end;
    }
}

static size_t token_end_from(const char *s, size_t n, size_t pos) {
    size_t end = pos;
    while (end < n && is_token_char(s[end])) {
        end++;
    }
    return end;
}

static bool boundary_before(const char *s, size_t pos) {
    return pos == 0 || !is_token_char(s[pos - 1]);
}

static bool boundary_after(const char *s, size_t n, size_t pos) {
    return pos >= n || !is_token_char(s[pos]);
}

static void scan_auth_headers(const char *s, size_t n, RangeVec *ranges) {
    for (size_t i = 0; i < n; i++) {
        if (starts_with_ci_at(s, n, i, "bearer")) {
            size_t p = i + 6;
            if (p < n && is_space_char(s[p])) {
                p = skip_spaces(s, n, p);
                size_t end = token_end_from(s, n, p);
                if (end > p + 8) {
                    range_vec_push(ranges, s, p, end, KIND_AUTH_HEADER);
                }
            }
        } else if (starts_with_ci_at(s, n, i, "basic")) {
            size_t p = i + 5;
            if (p < n && is_space_char(s[p])) {
                p = skip_spaces(s, n, p);
                size_t end = token_end_from(s, n, p);
                if (end > p + 12) {
                    range_vec_push(ranges, s, p, end, KIND_AUTH_HEADER);
                }
            }
        }
    }
}

typedef struct {
    const char *prefix;
    size_t min_len;
    RedactionKind kind;
} SecretPrefix;

static void scan_prefixed_secrets(const char *s, size_t n, RangeVec *ranges) {
    static const SecretPrefix prefixes[] = {
        {"sk-or-v1-", 30, KIND_OPENAI_STYLE_KEY},
        {"sk-ant-", 24, KIND_OPENAI_STYLE_KEY},
        {"sk-proj-", 30, KIND_OPENAI_STYLE_KEY},
        {"sk-live-", 24, KIND_OPENAI_STYLE_KEY},
        {"sk_live_", 24, KIND_OPENAI_STYLE_KEY},
        {"rk_live_", 24, KIND_OPENAI_STYLE_KEY},
        {"sk-", 24, KIND_OPENAI_STYLE_KEY},
        {"gsk_", 24, KIND_OPENAI_STYLE_KEY},
        {"hf_", 30, KIND_OPENAI_STYLE_KEY},
        {"nvapi-", 30, KIND_OPENAI_STYLE_KEY},
        {"pplx-", 24, KIND_OPENAI_STYLE_KEY},
        {"github_pat_", 40, KIND_GITHUB_TOKEN},
        {"ghp_", 30, KIND_GITHUB_TOKEN},
        {"gho_", 30, KIND_GITHUB_TOKEN},
        {"ghu_", 30, KIND_GITHUB_TOKEN},
        {"ghs_", 30, KIND_GITHUB_TOKEN},
        {"ghr_", 30, KIND_GITHUB_TOKEN},
        {"xoxb-", 24, KIND_SLACK_TOKEN},
        {"xoxp-", 24, KIND_SLACK_TOKEN},
        {"xoxa-", 24, KIND_SLACK_TOKEN},
        {"xoxc-", 24, KIND_SLACK_TOKEN},
        {"AIza", 30, KIND_GOOGLE_API_KEY}
    };

    for (size_t i = 0; i < n; i++) {
        if (!boundary_before(s, i)) {
            continue;
        }
        for (size_t p = 0; p < sizeof(prefixes) / sizeof(prefixes[0]); p++) {
            const SecretPrefix *pref = &prefixes[p];
            if (starts_with_ci_at(s, n, i, pref->prefix)) {
                size_t end = token_end_from(s, n, i);
                if (end - i >= pref->min_len && boundary_after(s, n, end)) {
                    range_vec_push(ranges, s, i, end, pref->kind);
                }
            }
        }

        if ((starts_with_ci_at(s, n, i, "AKIA") || starts_with_ci_at(s, n, i, "ASIA")) &&
            i + 20 <= n && boundary_after(s, n, i + 20)) {
            bool ok = true;
            for (size_t j = i; j < i + 20; j++) {
                unsigned char c = (unsigned char)s[j];
                if (!(isupper(c) || isdigit(c))) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                range_vec_push(ranges, s, i, i + 20, KIND_AWS_ACCESS_KEY);
            }
        }
    }
}

static void scan_jwt(const char *s, size_t n, RangeVec *ranges) {
    for (size_t i = 0; i < n; i++) {
        if (!boundary_before(s, i) || !starts_with_ci_at(s, n, i, "eyJ")) {
            continue;
        }
        size_t end = i;
        int dots = 0;
        while (end < n && is_base64url_char(s[end])) {
            if (s[end] == '.') {
                dots++;
            }
            end++;
        }
        if (dots == 2 && end - i >= 36) {
            range_vec_push(ranges, s, i, end, KIND_JWT);
        }
    }
}

static void scan_url_query_params(const Options *opt, const char *s, size_t n, RangeVec *ranges) {
    for (size_t i = 0; i < n; i++) {
        if (s[i] != '?') {
            continue;
        }
        size_t query_start = i + 1;
        size_t query_end = query_start;
        while (query_end < n &&
               !is_space_char(s[query_end]) &&
               s[query_end] != '"' &&
               s[query_end] != '\'' &&
               s[query_end] != '<' &&
               s[query_end] != '>' &&
               s[query_end] != ')' &&
               s[query_end] != ']') {
            query_end++;
        }

        size_t param = query_start;
        while (param < query_end) {
            size_t next = param;
            while (next < query_end && s[next] != '&') {
                next++;
            }
            size_t eq = param;
            while (eq < next && s[eq] != '=') {
                eq++;
            }
            if (eq > param && eq + 1 < next) {
                char normalized[160];
                normalize_key_slice(s, param, eq, normalized, sizeof(normalized));
                if (is_sensitive_normalized_key(opt, normalized)) {
                    range_vec_push(ranges, s, eq + 1, next, KIND_QUERY_PARAM);
                }
            }
            param = next + 1;
        }
    }
}

static bool line_contains_private_key_begin(const char *s, size_t n, size_t *begin_pos) {
    for (size_t i = 0; i < n; i++) {
        if (starts_with_ci_at(s, n, i, "-----BEGIN ") &&
            contains_ci_slice(s + i, n - i, "PRIVATE KEY-----")) {
            *begin_pos = i;
            return true;
        }
    }
    return false;
}

static bool line_contains_private_key_end(const char *s, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (starts_with_ci_at(s, n, i, "-----END ") &&
            contains_ci_slice(s + i, n - i, "PRIVATE KEY-----")) {
            return true;
        }
    }
    return false;
}

static void scan_private_key_blocks(ScanState *state, const char *s, size_t n, RangeVec *ranges) {
    if (state->pem_active) {
        range_vec_push(ranges, s, 0, n, KIND_PRIVATE_KEY);
        if (line_contains_private_key_end(s, n)) {
            state->pem_active = false;
        }
        return;
    }

    size_t begin_pos = 0;
    if (line_contains_private_key_begin(s, n, &begin_pos)) {
        range_vec_push(ranges, s, begin_pos, n, KIND_PRIVATE_KEY);
        if (!line_contains_private_key_end(s, n)) {
            state->pem_active = true;
        }
    }
}

static bool all_hex(const char *s, size_t start, size_t end) {
    if (end <= start) {
        return false;
    }
    for (size_t i = start; i < end; i++) {
        if (!isxdigit((unsigned char)s[i])) {
            return false;
        }
    }
    return true;
}

static bool looks_like_uuidish(const char *s, size_t start, size_t end) {
    size_t dashes = 0;
    size_t hexes = 0;
    for (size_t i = start; i < end; i++) {
        if (s[i] == '-') {
            dashes++;
        } else if (isxdigit((unsigned char)s[i])) {
            hexes++;
        } else {
            return false;
        }
    }
    return dashes >= 4 && hexes >= 24;
}

static bool looks_like_path_or_url(const char *s, size_t start, size_t end) {
    size_t slash_count = 0;
    bool has_scheme = false;
    for (size_t i = start; i < end; i++) {
        if (s[i] == '/') {
            slash_count++;
        }
        if (i + 2 < end && s[i] == ':' && s[i + 1] == '/' && s[i + 2] == '/') {
            has_scheme = true;
        }
    }
    if (has_scheme) {
        return true;
    }
    return slash_count >= 2 && !contains_ci_slice(s + start, end - start, "=");
}

static int token_class_count(const char *s, size_t start, size_t end) {
    bool lower = false;
    bool upper = false;
    bool digit = false;
    bool symbol = false;
    for (size_t i = start; i < end; i++) {
        unsigned char c = (unsigned char)s[i];
        if (islower(c)) lower = true;
        else if (isupper(c)) upper = true;
        else if (isdigit(c)) digit = true;
        else symbol = true;
    }
    return (lower ? 1 : 0) + (upper ? 1 : 0) + (digit ? 1 : 0) + (symbol ? 1 : 0);
}

static bool has_secretish_symbol(const char *s, size_t start, size_t end) {
    for (size_t i = start; i < end; i++) {
        if (s[i] == '_' || s[i] == '-' || s[i] == '+' || s[i] == '/' || s[i] == '=') {
            return true;
        }
    }
    return false;
}

static void scan_high_entropy(const Options *opt, const char *s, size_t n, RangeVec *ranges) {
    if (!opt->entropy_enabled) {
        return;
    }

    size_t i = 0;
    while (i < n) {
        if (!is_token_char(s[i])) {
            i++;
            continue;
        }
        size_t start = i;
        size_t end = token_end_from(s, n, i);
        size_t len = end - start;

        if (len >= opt->min_token_len &&
            token_class_count(s, start, end) >= 3 &&
            has_secretish_symbol(s, start, end) &&
            !all_hex(s, start, end) &&
            !looks_like_uuidish(s, start, end) &&
            !looks_like_path_or_url(s, start, end)) {
            range_vec_push(ranges, s, start, end, KIND_HIGH_ENTROPY);
        }
        i = end;
    }
}

static int read_line_dynamic(FILE *fp, char **buffer, size_t *cap, size_t *len) {
    if (*buffer == NULL) {
        *cap = 4096;
        *buffer = (char *)xmalloc(*cap);
    }

    *len = 0;
    int ch = 0;
    while ((ch = fgetc(fp)) != EOF) {
        size_t needed = 0;
        if (add_overflow_size(*len, 2, &needed)) {
            fprintf(stderr, "line is too large\n");
            return -1;
        }
        if (needed > *cap) {
            size_t next_cap = *cap;
            while (needed > next_cap) {
                if (next_cap > ((size_t)-1) / 2) {
                    fprintf(stderr, "line is too large\n");
                    return -1;
                }
                next_cap *= 2;
            }
            *buffer = (char *)xrealloc(*buffer, next_cap);
            *cap = next_cap;
        }
        (*buffer)[(*len)++] = (char)ch;
        if (ch == '\n') {
            break;
        }
    }

    if (ferror(fp)) {
        return -1;
    }
    if (*len == 0 && ch == EOF) {
        return 0;
    }
    (*buffer)[*len] = '\0';
    return 1;
}

static bool write_exact(FILE *out, const char *data, size_t len, Stats *stats) {
    if (len == 0) {
        return true;
    }
    if (fwrite(data, 1, len, out) != len) {
        return false;
    }
    stats->bytes_out += (uint64_t)len;
    return true;
}

static bool write_redaction(FILE *out, const Range *range, Stats *stats) {
    char replacement[96];
    int n = snprintf(replacement, sizeof(replacement), "[REDACTED:%s:%016" PRIx64 "]",
                     kind_label(range->kind), range->fingerprint);
    if (n < 0 || (size_t)n >= sizeof(replacement)) {
        return false;
    }
    if (!write_exact(out, replacement, (size_t)n, stats)) {
        return false;
    }
    stats->redactions++;
    stats->by_kind[range->kind]++;
    return true;
}

static bool emit_sanitized_line(FILE *out, const char *line, size_t len, RangeVec *ranges, Stats *stats) {
    if (ranges->len == 0) {
        return write_exact(out, line, len, stats);
    }

    qsort(ranges->items, ranges->len, sizeof(Range), compare_ranges);

    size_t cursor = 0;
    for (size_t i = 0; i < ranges->len; i++) {
        Range r = ranges->items[i];
        if (r.end > len) {
            r.end = len;
        }
        if (r.start >= r.end || r.start < cursor) {
            continue;
        }
        if (!write_exact(out, line + cursor, r.start - cursor, stats)) {
            return false;
        }
        if (!write_redaction(out, &r, stats)) {
            return false;
        }
        cursor = r.end;
    }

    return write_exact(out, line + cursor, len - cursor, stats);
}

static bool sanitize_stream(FILE *in, FILE *out, const Options *opt, Stats *stats) {
    ScanState state;
    state.pem_active = false;

    char *line = NULL;
    size_t cap = 0;
    size_t len = 0;
    int rc = 0;

    while ((rc = read_line_dynamic(in, &line, &cap, &len)) == 1) {
        RangeVec ranges;
        range_vec_init(&ranges);
        stats->lines++;
        stats->bytes_in += (uint64_t)len;

        scan_private_key_blocks(&state, line, len, &ranges);
        scan_url_query_params(opt, line, len, &ranges);
        scan_named_values(opt, line, len, &ranges);
        scan_auth_headers(line, len, &ranges);
        scan_prefixed_secrets(line, len, &ranges);
        scan_jwt(line, len, &ranges);
        scan_high_entropy(opt, line, len, &ranges);

        bool ok = emit_sanitized_line(out, line, len, &ranges, stats);
        range_vec_free(&ranges);
        if (!ok) {
            free(line);
            return false;
        }
    }

    free(line);
    return rc == 0;
}

static void json_escape_write(FILE *out, const char *s) {
    fputc('"', out);
    for (; *s; s++) {
        unsigned char c = (unsigned char)*s;
        switch (c) {
            case '"': fputs("\\\"", out); break;
            case '\\': fputs("\\\\", out); break;
            case '\b': fputs("\\b", out); break;
            case '\f': fputs("\\f", out); break;
            case '\n': fputs("\\n", out); break;
            case '\r': fputs("\\r", out); break;
            case '\t': fputs("\\t", out); break;
            default:
                if (c < 32) {
                    fprintf(out, "\\u%04x", c);
                } else {
                    fputc(c, out);
                }
        }
    }
    fputc('"', out);
}

static bool write_report(const Options *opt, const Stats *stats) {
    FILE *out = NULL;
    bool close_out = false;

    if (opt->report_path == NULL) {
        return true;
    }
    if (strcmp(opt->report_path, "-") == 0) {
        out = stderr;
    } else {
        out = fopen(opt->report_path, "wb");
        if (out == NULL) {
            fprintf(stderr, "cannot open report '%s': %s\n", opt->report_path, strerror(errno));
            return false;
        }
        close_out = true;
    }

    fprintf(out, "{\n");
    fprintf(out, "  \"tool\": \"McpTraceSanitizer\",\n");
    fprintf(out, "  \"schema_version\": 1,\n");
    fprintf(out, "  \"lines\": %" PRIu64 ",\n", stats->lines);
    fprintf(out, "  \"bytes_in\": %" PRIu64 ",\n", stats->bytes_in);
    fprintf(out, "  \"bytes_out\": %" PRIu64 ",\n", stats->bytes_out);
    fprintf(out, "  \"redactions\": %" PRIu64 ",\n", stats->redactions);
    fprintf(out, "  \"fail_on_leak\": %s,\n", opt->fail_on_leak ? "true" : "false");
    fprintf(out, "  \"entropy_enabled\": %s,\n", opt->entropy_enabled ? "true" : "false");
    fprintf(out, "  \"min_token_len\": %zu,\n", opt->min_token_len);
    fprintf(out, "  \"by_kind\": {\n");
    for (int i = 0; i < KIND_COUNT; i++) {
        fprintf(out, "    ");
        json_escape_write(out, kind_label((RedactionKind)i));
        fprintf(out, ": %" PRIu64 "%s\n", stats->by_kind[i], i + 1 == KIND_COUNT ? "" : ",");
    }
    fprintf(out, "  }\n");
    fprintf(out, "}\n");

    if (ferror(out)) {
        if (close_out) fclose(out);
        return false;
    }
    if (close_out && fclose(out) != 0) {
        fprintf(stderr, "cannot close report '%s': %s\n", opt->report_path, strerror(errno));
        return false;
    }
    return true;
}

static FileHandle open_input(const char *path) {
    FileHandle handle;
    handle.file = stdin;
    handle.should_close = false;

    if (path != NULL && strcmp(path, "-") != 0) {
        handle.file = fopen(path, "rb");
        if (handle.file == NULL) {
            fprintf(stderr, "cannot open input '%s': %s\n", path, strerror(errno));
            exit(EXIT_DATA);
        }
        handle.should_close = true;
    }
    return handle;
}

static FileHandle open_output(const char *path) {
    FileHandle handle;
    handle.file = stdout;
    handle.should_close = false;

    if (path != NULL && strcmp(path, "-") != 0) {
        handle.file = fopen(path, "wb");
        if (handle.file == NULL) {
            fprintf(stderr, "cannot open output '%s': %s\n", path, strerror(errno));
            exit(EXIT_DATA);
        }
        handle.should_close = true;
    }
    return handle;
}

static void close_handle(FileHandle *handle, const char *label, const char *path) {
    if (handle->should_close) {
        if (fclose(handle->file) != 0) {
            fprintf(stderr, "cannot close %s '%s': %s\n", label, path, strerror(errno));
            exit(EXIT_DATA);
        }
    } else if (fflush(handle->file) != 0) {
        fprintf(stderr, "cannot flush %s: %s\n", label, strerror(errno));
        exit(EXIT_DATA);
    }
}

static size_t parse_size_arg(const char *name, const char *value) {
    char *end = NULL;
    errno = 0;
    unsigned long long parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0 || parsed > (unsigned long long)((size_t)-1)) {
        fprintf(stderr, "%s must be a positive integer, got '%s'\n", name, value);
        exit(EXIT_CONFIG);
    }
    return (size_t)parsed;
}

static const char *require_arg(int argc, char **argv, int *i, const char *flag) {
    if (*i + 1 >= argc) {
        fprintf(stderr, "missing value after %s\n", flag);
        exit(EXIT_CONFIG);
    }
    (*i)++;
    return argv[*i];
}

static void print_usage(FILE *out) {
    fputs(
        "McpTraceSanitizer redacts secrets from MCP, LLM, CI, and tool-call traces.\n"
        "\n"
        "Usage:\n"
        "  McpTraceSanitizer [options]\n"
        "\n"
        "Options:\n"
        "  --input PATH          Read from PATH instead of stdin. Use '-' for stdin.\n"
        "  --output PATH         Write sanitized trace to PATH instead of stdout. Use '-' for stdout.\n"
        "  --report PATH         Write a JSON report. Use '-' for stderr.\n"
        "  --fail-on-leak        Exit 2 if any secret-like value was redacted.\n"
        "  --allow-key NAME      Do not treat this field name as sensitive. Repeatable.\n"
        "  --no-entropy          Disable generic high-entropy token detection.\n"
        "  --min-token-len N     Minimum length for high-entropy detection. Default: 32.\n"
        "  --help                Show this help.\n"
        "\n"
        "Redaction classes include Authorization headers, token/password/secret fields,\n"
        "secret URL query params, common OpenAI/GitHub/Slack/AWS/Gemini style keys,\n"
        "JWTs, PEM private key blocks, and high-entropy bearer strings.\n",
        out);
}

static void die_usage(const char *message) {
    if (message != NULL) {
        fprintf(stderr, "%s\n", message);
    }
    print_usage(stderr);
    exit(EXIT_CONFIG);
}

static Options parse_options(int argc, char **argv) {
    Options opt;
    memset(&opt, 0, sizeof(opt));
    opt.entropy_enabled = true;
    opt.min_token_len = 32;

    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];
        if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
            print_usage(stdout);
            exit(0);
        } else if (strcmp(arg, "--input") == 0 || strcmp(arg, "-i") == 0) {
            opt.input_path = require_arg(argc, argv, &i, arg);
        } else if (strcmp(arg, "--output") == 0 || strcmp(arg, "-o") == 0) {
            opt.output_path = require_arg(argc, argv, &i, arg);
        } else if (strcmp(arg, "--report") == 0) {
            opt.report_path = require_arg(argc, argv, &i, arg);
        } else if (strcmp(arg, "--fail-on-leak") == 0) {
            opt.fail_on_leak = true;
        } else if (strcmp(arg, "--no-entropy") == 0) {
            opt.entropy_enabled = false;
        } else if (strcmp(arg, "--min-token-len") == 0) {
            opt.min_token_len = parse_size_arg(arg, require_arg(argc, argv, &i, arg));
        } else if (strcmp(arg, "--allow-key") == 0) {
            const char *raw = require_arg(argc, argv, &i, arg);
            string_vec_push(&opt.allowed_keys, normalize_key_alloc(raw));
        } else {
            char message[256];
            snprintf(message, sizeof(message), "unknown option: %s", arg);
            die_usage(message);
        }
    }

    if (opt.input_path != NULL && opt.output_path != NULL &&
        strcmp(opt.input_path, "-") != 0 &&
        strcmp(opt.output_path, "-") != 0 &&
        strcmp(opt.input_path, opt.output_path) == 0) {
        fprintf(stderr, "--output must not overwrite --input in place\n");
        exit(EXIT_CONFIG);
    }

    return opt;
}

int main(int argc, char **argv) {
    Options opt = parse_options(argc, argv);
    Stats stats;
    memset(&stats, 0, sizeof(stats));

    FileHandle input = open_input(opt.input_path);
    FileHandle output = open_output(opt.output_path);

    bool ok = sanitize_stream(input.file, output.file, &opt, &stats);
    close_handle(&input, "input", opt.input_path == NULL ? "stdin" : opt.input_path);
    close_handle(&output, "output", opt.output_path == NULL ? "stdout" : opt.output_path);

    if (!ok) {
        fprintf(stderr, "failed while reading or writing trace data\n");
        string_vec_free(&opt.allowed_keys);
        return EXIT_DATA;
    }

    if (!write_report(&opt, &stats)) {
        string_vec_free(&opt.allowed_keys);
        return EXIT_DATA;
    }

    bool leak_found = stats.redactions > 0;
    string_vec_free(&opt.allowed_keys);
    if (opt.fail_on_leak && leak_found) {
        return EXIT_LEAK;
    }
    return 0;
}

/*
This solves the very practical April 2026 problem where MCP tool traces, AI agent debug logs, LLM gateway JSONL, browser automation transcripts, CI output, and support bundles quietly carry real API keys, bearer tokens, private keys, cookies, signed URLs, and high entropy credentials into places where they should never land. Built because Pavan has seen that teams now paste raw agent traces into GitHub issues, Slack, vendor tickets, eval reports, and research notebooks, and one missed Authorization header can turn a helpful debugging artifact into an incident. Use it when you need a fast C command line sanitizer for Model Context Protocol logs, OpenAI API traces, GitHub token cleanup, Slack bot token redaction, AWS key detection, Gemini API key hygiene, JWT removal, prompt engineering audit logs, DevOps release evidence, or AI infrastructure support files. The trick: it streams line by line, records overlap-safe redaction ranges, keeps the surrounding JSON or text readable, and replaces each secret with a stable non-secret fingerprint so repeated leaks can still be correlated without exposing the value. Drop this into a repository as a production-ready MCP trace sanitizer, LLM log redaction utility, AI tool-call secret scanner, CI leak guard, secure developer productivity wrapper, or research trace cleanup step before any trace leaves a laptop, build runner, private incident room, or regulated data boundary.
*/
