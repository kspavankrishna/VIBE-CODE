#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char tenant[96];
    char route[160];
    double tokens;
    double bytes;
    double latency_ms;
    int failed;
} Sample;

typedef struct {
    char key[288];
    char tenant[96];
    char route[160];
    double tokens;
    double bytes;
    double latency_ms;
    double max_latency_ms;
    int samples;
    int failed;
} Rollup;

typedef struct {
    double max_bytes_per_token;
    double max_failure_rate;
    double max_latency_ms;
    int json;
} Options;

static void trim(char *s) {
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n - 1])) s[--n] = '\0';
    size_t i = 0;
    while (s[i] && isspace((unsigned char)s[i])) i++;
    if (i > 0) memmove(s, s + i, strlen(s + i) + 1);
}

static double parse_double(const char *raw, const char *name) {
    char *end = NULL;
    errno = 0;
    double value = strtod(raw, &end);
    if (errno != 0 || end == raw) {
        fprintf(stderr, "TokenEgressBudget: bad number for %s: %s\n", name, raw);
        exit(64);
    }
    return value;
}

static Options parse_options(int argc, char **argv) {
    Options o;
    o.max_bytes_per_token = 9.0;
    o.max_failure_rate = 0.04;
    o.max_latency_ms = 30000.0;
    o.json = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--json") == 0) {
            o.json = 1;
        } else if (strcmp(argv[i], "--max-bytes-per-token") == 0 && i + 1 < argc) {
            o.max_bytes_per_token = parse_double(argv[++i], "max bytes per token");
        } else if (strcmp(argv[i], "--max-failure-rate") == 0 && i + 1 < argc) {
            o.max_failure_rate = parse_double(argv[++i], "max failure rate");
        } else if (strcmp(argv[i], "--max-latency-ms") == 0 && i + 1 < argc) {
            o.max_latency_ms = parse_double(argv[++i], "max latency");
        } else {
            fprintf(stderr, "TokenEgressBudget: unknown or incomplete option %s\n", argv[i]);
            exit(64);
        }
    }
    return o;
}

static int split_csv(char *line, char **cells, int max_cells) {
    int count = 0;
    int quoted = 0;
    cells[count++] = line;
    for (char *p = line; *p && count < max_cells; p++) {
        if (*p == '"') quoted = !quoted;
        else if (*p == ',' && !quoted) {
            *p = '\0';
            cells[count++] = p + 1;
        }
    }
    for (int i = 0; i < count; i++) trim(cells[i]);
    return count;
}

static Sample parse_sample(char *line, int row) {
    char *cells[8];
    int n = split_csv(line, cells, 8);
    if (n < 6) {
        fprintf(stderr, "TokenEgressBudget: row %d needs tenant,route,tokens,bytes,latency_ms,status\n", row);
        exit(64);
    }
    Sample s;
    memset(&s, 0, sizeof(s));
    snprintf(s.tenant, sizeof(s.tenant), "%s", cells[0]);
    snprintf(s.route, sizeof(s.route), "%s", cells[1]);
    s.tokens = parse_double(cells[2], "tokens");
    s.bytes = parse_double(cells[3], "bytes");
    s.latency_ms = parse_double(cells[4], "latency_ms");
    for (char *p = cells[5]; *p; p++) *p = (char)tolower((unsigned char)*p);
    s.failed = strcmp(cells[5], "ok") != 0 && strcmp(cells[5], "success") != 0;
    return s;
}

static Rollup *find_rollup(Rollup *items, int *count, Sample s) {
    char key[288];
    snprintf(key, sizeof(key), "%s/%s", s.tenant, s.route);
    for (int i = 0; i < *count; i++) {
        if (strcmp(items[i].key, key) == 0) return &items[i];
    }
    if (*count >= 4096) {
        fprintf(stderr, "TokenEgressBudget: too many tenant route pairs\n");
        exit(70);
    }
    Rollup *r = &items[(*count)++];
    memset(r, 0, sizeof(*r));
    snprintf(r->key, sizeof(r->key), "%s", key);
    snprintf(r->tenant, sizeof(r->tenant), "%s", s.tenant);
    snprintf(r->route, sizeof(r->route), "%s", s.route);
    return r;
}

static void add_sample(Rollup *r, Sample s) {
    r->samples++;
    r->failed += s.failed;
    r->tokens += s.tokens;
    r->bytes += s.bytes;
    r->latency_ms += s.latency_ms;
    if (s.latency_ms > r->max_latency_ms) r->max_latency_ms = s.latency_ms;
}

static int violates(Rollup r, Options o) {
    double bpt = r.tokens <= 0.0 ? r.bytes : r.bytes / r.tokens;
    double failure_rate = r.samples == 0 ? 0.0 : (double)r.failed / (double)r.samples;
    double avg_latency = r.samples == 0 ? 0.0 : r.latency_ms / (double)r.samples;
    return bpt > o.max_bytes_per_token || failure_rate > o.max_failure_rate || avg_latency > o.max_latency_ms;
}

static void print_json(Rollup *items, int count, Options o) {
    int failed = 0;
    printf("{\"routes\":[");
    for (int i = 0; i < count; i++) {
        Rollup r = items[i];
        double bpt = r.tokens <= 0.0 ? r.bytes : r.bytes / r.tokens;
        double failure_rate = r.samples == 0 ? 0.0 : (double)r.failed / (double)r.samples;
        double avg_latency = r.samples == 0 ? 0.0 : r.latency_ms / (double)r.samples;
        int bad = violates(r, o);
        failed = failed || bad;
        if (i) printf(",");
        printf("{\"tenant\":\"%s\",\"route\":\"%s\",\"samples\":%d,\"bytes_per_token\":%.6f,\"failure_rate\":%.6f,\"avg_latency_ms\":%.2f,\"status\":\"%s\"}", r.tenant, r.route, r.samples, bpt, failure_rate, avg_latency, bad ? "fail" : "pass");
    }
    printf("],\"status\":\"%s\"}\n", failed ? "fail" : "pass");
}

static void print_text(Rollup *items, int count, Options o) {
    printf("status\ttenant\troute\tsamples\tbytes_per_token\tfailure_rate\tavg_latency_ms\tmax_latency_ms\n");
    for (int i = 0; i < count; i++) {
        Rollup r = items[i];
        double bpt = r.tokens <= 0.0 ? r.bytes : r.bytes / r.tokens;
        double failure_rate = r.samples == 0 ? 0.0 : (double)r.failed / (double)r.samples;
        double avg_latency = r.samples == 0 ? 0.0 : r.latency_ms / (double)r.samples;
        printf("%s\t%s\t%s\t%d\t%.6f\t%.6f\t%.2f\t%.2f\n", violates(r, o) ? "FAIL" : "PASS", r.tenant, r.route, r.samples, bpt, failure_rate, avg_latency, r.max_latency_ms);
    }
}

int main(int argc, char **argv) {
    Options options = parse_options(argc, argv);
    Rollup rollups[4096];
    int count = 0;
    char line[8192];
    int row = 0;
    while (fgets(line, sizeof(line), stdin)) {
        row++;
        trim(line);
        if (line[0] == '\0') continue;
        if (row == 1 && strncmp(line, "tenant,", 7) == 0) continue;
        Sample s = parse_sample(line, row);
        add_sample(find_rollup(rollups, &count, s), s);
    }
    if (options.json) print_json(rollups, count, options);
    else print_text(rollups, count, options);
    for (int i = 0; i < count; i++) if (violates(rollups[i], options)) return 2;
    return 0;
}

/*
This solves the April 2026 streaming token egress problem where AI gateways and realtime
apps pay for model tokens, network bytes, and slow client drains at the same time, but most
budgets only watch tokens. Built because a route can look cheap in token accounting while it
is quietly expensive in Server-Sent Events overhead, verbose JSON framing, retries, or mobile
network backpressure. Use it when gateway logs can export tenant, route, tokens, bytes,
latency_ms, and status as CSV from Nginx, Envoy, Cloudflare Workers, Vercel Functions, or an
internal inference proxy. The trick: it groups by tenant and route, compares bytes per token,
failure rate, average latency, and max latency, then exits nonzero when any route breaks the
budget. Drop this into a C-friendly platform repository as a single source file and it becomes
a token egress budget gate, LLM streaming cost analyzer, AI network efficiency profiler,
edge compute regression check, and searchable DevOps utility for production traffic reviews.
*/
