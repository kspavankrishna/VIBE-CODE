// QueueSloAllocator.c
// Deterministic concurrency allocator for AI, eval, and data pipeline queues.

#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define QSA_MAX_LINE 8192
#define QSA_MAX_FIELDS 16
#define QSA_MAX_FIELD 256
#define QSA_MAX_QUEUES 4096
#define QSA_NAME 96

typedef struct {
    int total_workers;
    double horizon_sec;
    double target_utilization;
    bool json;
} Options;

typedef struct {
    char tenant[QSA_NAME];
    char queue[QSA_NAME];
    double backlog;
    double arrival_per_sec;
    double service_per_sec_per_worker;
    double p95_ms;
    double deadline_ms;
    double priority;
    int min_workers;
    int max_workers;
    int assigned;
    int required;
    int required_uncapped;
    double risk;
    double demand_per_sec;
    double latency_ratio;
    double backlog_seconds;
    char status[48];
} Queue;

typedef struct {
    Queue items[QSA_MAX_QUEUES];
    int count;
} QueueSet;

static void die(const char *message) {
    fprintf(stderr, "QueueSloAllocator: %s\n", message);
    exit(64);
}

static void usage(FILE *out) {
    fputs(
        "Usage: QueueSloAllocator --workers N [options] < queues.csv\n"
        "\n"
        "CSV columns:\n"
        "  tenant,queue,backlog,arrival_per_sec,service_per_sec_per_worker,p95_ms,deadline_ms,priority[,min_workers,max_workers]\n"
        "\n"
        "Options:\n"
        "  --workers N             Total workers or concurrent slots available. Required.\n"
        "  --horizon-sec N         Backlog drain planning horizon. Default 300.\n"
        "  --target-utilization X  Per-worker target utilization, 0 < X <= 1. Default 0.82.\n"
        "  --json                  Emit JSON instead of TSV.\n"
        "  --help                  Show this help.\n",
        out);
}

static bool streq(const char *a, const char *b) {
    return strcmp(a, b) == 0;
}

static bool field_is_header(const char *text) {
    char buf[QSA_MAX_FIELD];
    size_t n = strlen(text);
    if (n >= sizeof(buf)) n = sizeof(buf) - 1;
    for (size_t i = 0; i < n; ++i) buf[i] = (char)tolower((unsigned char)text[i]);
    buf[n] = '\0';
    return streq(buf, "tenant") || streq(buf, "queue");
}

static double parse_double(const char *raw, const char *name, int line_no) {
    errno = 0;
    char *end = NULL;
    double value = strtod(raw, &end);
    while (end && isspace((unsigned char)*end)) ++end;
    if (errno == ERANGE || end == raw || (end && *end != '\0') || !isfinite(value)) {
        fprintf(stderr, "QueueSloAllocator: line %d has bad %s: %s\n", line_no, name, raw);
        exit(64);
    }
    return value;
}

static int parse_int_field(const char *raw, const char *name, int line_no) {
    errno = 0;
    char *end = NULL;
    long value = strtol(raw, &end, 10);
    while (end && isspace((unsigned char)*end)) ++end;
    if (errno == ERANGE || end == raw || (end && *end != '\0') || value < 0 || value > INT_MAX) {
        fprintf(stderr, "QueueSloAllocator: line %d has bad %s: %s\n", line_no, name, raw);
        exit(64);
    }
    return (int)value;
}

static void copy_field(char *dst, size_t cap, const char *src) {
    while (isspace((unsigned char)*src)) ++src;
    size_t n = strlen(src);
    while (n > 0 && isspace((unsigned char)src[n - 1])) --n;
    if (n >= cap) n = cap - 1;
    memcpy(dst, src, n);
    dst[n] = '\0';
}

static void trim_eol(char *line) {
    size_t n = strlen(line);
    while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) {
        line[--n] = '\0';
    }
}

static int parse_csv(char *line, char fields[QSA_MAX_FIELDS][QSA_MAX_FIELD]) {
    int count = 0;
    size_t out = 0;
    bool quoted = false;
    memset(fields, 0, (size_t)QSA_MAX_FIELDS * (size_t)QSA_MAX_FIELD);

    for (size_t i = 0;; ++i) {
        char ch = line[i];
        if (quoted && ch == '"' && line[i + 1] == '"') {
            if (out + 1 < QSA_MAX_FIELD) fields[count][out++] = '"';
            ++i;
            continue;
        }
        if (ch == '"') {
            quoted = !quoted;
            continue;
        }
        if ((ch == ',' && !quoted) || ch == '\0') {
            fields[count][out] = '\0';
            ++count;
            out = 0;
            if (count >= QSA_MAX_FIELDS || ch == '\0') break;
            continue;
        }
        if (out + 1 < QSA_MAX_FIELD) fields[count][out++] = ch;
    }

    if (quoted) die("unterminated quoted CSV field");
    return count;
}

static Options parse_options(int argc, char **argv) {
    Options options;
    options.total_workers = -1;
    options.horizon_sec = 300.0;
    options.target_utilization = 0.82;
    options.json = false;

    for (int i = 1; i < argc; ++i) {
        if (streq(argv[i], "--help")) {
            usage(stdout);
            exit(0);
        } else if (streq(argv[i], "--json")) {
            options.json = true;
        } else if (streq(argv[i], "--workers")) {
            if (++i >= argc) die("--workers requires a value");
            options.total_workers = parse_int_field(argv[i], "workers", 0);
        } else if (streq(argv[i], "--horizon-sec")) {
            if (++i >= argc) die("--horizon-sec requires a value");
            options.horizon_sec = parse_double(argv[i], "horizon-sec", 0);
        } else if (streq(argv[i], "--target-utilization")) {
            if (++i >= argc) die("--target-utilization requires a value");
            options.target_utilization = parse_double(argv[i], "target-utilization", 0);
        } else {
            fprintf(stderr, "QueueSloAllocator: unknown option %s\n", argv[i]);
            exit(64);
        }
    }

    if (options.total_workers < 0) {
        usage(stderr);
        die("--workers is required");
    }
    if (options.horizon_sec <= 0.0) die("--horizon-sec must be positive");
    if (options.target_utilization <= 0.0 || options.target_utilization > 1.0) {
        die("--target-utilization must be in the range (0, 1]");
    }
    return options;
}

static Queue parse_queue(char fields[QSA_MAX_FIELDS][QSA_MAX_FIELD], int field_count, int line_no) {
    Queue q;
    memset(&q, 0, sizeof(q));
    if (field_count < 8) {
        fprintf(stderr, "QueueSloAllocator: line %d needs at least 8 columns\n", line_no);
        exit(64);
    }

    copy_field(q.tenant, sizeof(q.tenant), fields[0]);
    copy_field(q.queue, sizeof(q.queue), fields[1]);
    q.backlog = parse_double(fields[2], "backlog", line_no);
    q.arrival_per_sec = parse_double(fields[3], "arrival_per_sec", line_no);
    q.service_per_sec_per_worker = parse_double(fields[4], "service_per_sec_per_worker", line_no);
    q.p95_ms = parse_double(fields[5], "p95_ms", line_no);
    q.deadline_ms = parse_double(fields[6], "deadline_ms", line_no);
    q.priority = parse_double(fields[7], "priority", line_no);
    q.min_workers = field_count >= 9 && fields[8][0] != '\0' ? parse_int_field(fields[8], "min_workers", line_no) : 0;
    q.max_workers = field_count >= 10 && fields[9][0] != '\0' ? parse_int_field(fields[9], "max_workers", line_no) : INT_MAX / 4;

    if (q.tenant[0] == '\0' || q.queue[0] == '\0') die("tenant and queue must not be empty");
    if (q.backlog < 0.0 || q.arrival_per_sec < 0.0 || q.p95_ms < 0.0 || q.priority < 0.0) {
        die("backlog, arrival, p95, and priority must be non-negative");
    }
    if (q.service_per_sec_per_worker <= 0.0) die("service_per_sec_per_worker must be positive");
    if (q.deadline_ms <= 0.0) die("deadline_ms must be positive");
    if (q.max_workers < q.min_workers) die("max_workers must be >= min_workers");
    return q;
}

static QueueSet read_queues(void) {
    QueueSet set;
    char line[QSA_MAX_LINE];
    int line_no = 0;
    memset(&set, 0, sizeof(set));

    while (fgets(line, sizeof(line), stdin)) {
        ++line_no;
        if (strlen(line) == sizeof(line) - 1 && line[sizeof(line) - 2] != '\n') {
            die("input line is too long");
        }
        trim_eol(line);
        char *p = line;
        while (isspace((unsigned char)*p)) ++p;
        if (*p == '\0' || *p == '#') continue;

        char fields[QSA_MAX_FIELDS][QSA_MAX_FIELD];
        int field_count = parse_csv(p, fields);
        if (field_count > 0 && field_is_header(fields[0])) continue;
        if (set.count >= QSA_MAX_QUEUES) die("too many queues; increase QSA_MAX_QUEUES and rebuild");
        set.items[set.count++] = parse_queue(fields, field_count, line_no);
    }

    if (ferror(stdin)) die("failed while reading stdin");
    if (set.count == 0) die("no queue rows found");
    return set;
}

static double clamp_double(double value, double low, double high) {
    if (value < low) return low;
    if (value > high) return high;
    return value;
}

static int ceil_to_int(double value) {
    if (value <= 0.0) return 0;
    if (value > (double)(INT_MAX / 4)) return INT_MAX / 4;
    return (int)ceil(value);
}

static void compute_queue(Queue *q, const Options *options) {
    double deadline_sec = q->deadline_ms / 1000.0;
    double drain_window = options->horizon_sec;
    if (deadline_sec < drain_window) drain_window = deadline_sec;
    if (drain_window < 1.0) drain_window = 1.0;

    double backlog_drain_per_sec = q->backlog / drain_window;
    q->demand_per_sec = q->arrival_per_sec + backlog_drain_per_sec;
    q->latency_ratio = q->p95_ms / q->deadline_ms;
    q->backlog_seconds = q->backlog / q->service_per_sec_per_worker;

    double base_workers = q->demand_per_sec / (q->service_per_sec_per_worker * options->target_utilization);
    double latency_multiplier = q->latency_ratio > 1.0 ? clamp_double(q->latency_ratio, 1.0, 4.0) : 1.0;
    q->required_uncapped = ceil_to_int(base_workers * latency_multiplier);
    if (q->required_uncapped < q->min_workers) q->required_uncapped = q->min_workers;
    q->required = q->required_uncapped > q->max_workers ? q->max_workers : q->required_uncapped;

    double backlog_pressure = q->backlog_seconds / drain_window;
    double latency_pressure = q->latency_ratio * q->latency_ratio;
    q->risk = (1.0 + q->priority) * (latency_pressure + backlog_pressure + 0.25);
}

static void compute_all(QueueSet *set, const Options *options) {
    for (int i = 0; i < set->count; ++i) compute_queue(&set->items[i], options);
}

static double allocation_score(const Queue *q) {
    int target = q->assigned < q->min_workers ? q->min_workers : q->required;
    int deficit = target - q->assigned;
    double phase = q->assigned < q->min_workers ? 1000000.0 : 0.0;
    return phase + q->risk * 100.0 + (double)deficit * (1.0 + q->priority);
}

static void allocate(QueueSet *set, const Options *options) {
    int remaining = options->total_workers;
    for (int i = 0; i < set->count; ++i) set->items[i].assigned = 0;

    while (remaining > 0) {
        int best = -1;
        double best_score = -DBL_MAX;
        for (int i = 0; i < set->count; ++i) {
            Queue *q = &set->items[i];
            int target = q->assigned < q->min_workers ? q->min_workers : q->required;
            if (q->assigned >= target || q->assigned >= q->max_workers) continue;
            double score = allocation_score(q);
            if (score > best_score) {
                best_score = score;
                best = i;
            }
        }
        if (best < 0) break;
        set->items[best].assigned += 1;
        --remaining;
    }
}

static void classify(Queue *q) {
    if (q->assigned < q->min_workers) {
        snprintf(q->status, sizeof(q->status), "minimum_deficit");
    } else if (q->required_uncapped > q->max_workers && q->assigned >= q->max_workers) {
        snprintf(q->status, sizeof(q->status), "capped_by_max_workers");
    } else if (q->assigned < q->required) {
        snprintf(q->status, sizeof(q->status), "worker_deficit");
    } else if (q->latency_ratio > 1.0) {
        snprintf(q->status, sizeof(q->status), "latency_recovering");
    } else {
        snprintf(q->status, sizeof(q->status), "ok");
    }
}

static void classify_all(QueueSet *set) {
    for (int i = 0; i < set->count; ++i) classify(&set->items[i]);
}

static int used_workers(const QueueSet *set) {
    int used = 0;
    for (int i = 0; i < set->count; ++i) used += set->items[i].assigned;
    return used;
}

static bool has_deficit(const QueueSet *set) {
    for (int i = 0; i < set->count; ++i) {
        const Queue *q = &set->items[i];
        if (q->assigned < q->required || q->required_uncapped > q->max_workers) return true;
    }
    return false;
}

static void json_string(FILE *out, const char *text) {
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
                if (*p < 0x20) fprintf(out, "\\u%04x", *p);
                else fputc(*p, out);
        }
    }
    fputc('"', out);
}

static void print_tsv(const QueueSet *set, const Options *options) {
    printf("status\ttenant\tqueue\tassigned\trequired\trequired_uncapped\tmin\tmax\tdemand_per_sec\tlatency_ratio\tbacklog_seconds\trisk\n");
    for (int i = 0; i < set->count; ++i) {
        const Queue *q = &set->items[i];
        printf("%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%.6f\t%.6f\t%.2f\t%.6f\n",
               q->status, q->tenant, q->queue, q->assigned, q->required,
               q->required_uncapped, q->min_workers, q->max_workers,
               q->demand_per_sec, q->latency_ratio, q->backlog_seconds, q->risk);
    }
    fprintf(stderr, "workers_used=%d workers_total=%d unused=%d queues=%d status=%s\n",
            used_workers(set), options->total_workers, options->total_workers - used_workers(set),
            set->count, has_deficit(set) ? "deficit" : "ok");
}

static void print_json(const QueueSet *set, const Options *options) {
    printf("{\"workers_total\":%d,\"workers_used\":%d,\"unused\":%d,\"status\":",
           options->total_workers, used_workers(set), options->total_workers - used_workers(set));
    json_string(stdout, has_deficit(set) ? "deficit" : "ok");
    printf(",\"queues\":[");
    for (int i = 0; i < set->count; ++i) {
        const Queue *q = &set->items[i];
        if (i) putchar(',');
        printf("{\"status\":"); json_string(stdout, q->status);
        printf(",\"tenant\":"); json_string(stdout, q->tenant);
        printf(",\"queue\":"); json_string(stdout, q->queue);
        printf(",\"assigned\":%d,\"required\":%d,\"required_uncapped\":%d,\"min\":%d,\"max\":%d",
               q->assigned, q->required, q->required_uncapped, q->min_workers, q->max_workers);
        printf(",\"demand_per_sec\":%.10g,\"latency_ratio\":%.10g,\"backlog_seconds\":%.10g,\"risk\":%.10g}",
               q->demand_per_sec, q->latency_ratio, q->backlog_seconds, q->risk);
    }
    printf("]}\n");
}

int main(int argc, char **argv) {
    Options options = parse_options(argc, argv);
    QueueSet set = read_queues();
    compute_all(&set, &options);
    allocate(&set, &options);
    classify_all(&set);
    if (options.json) print_json(&set, &options);
    else print_tsv(&set, &options);
    return has_deficit(&set) ? 2 : 0;
}

/*
This solves the April 2026 queue capacity problem where AI eval runners, batch inference jobs, ETL backfills, MCP tool workers, edge image pipelines, and smart IoT ingest queues all compete for the same limited concurrency while users still expect latency SLOs to hold. Built because most teams can see backlog, arrival rate, worker throughput, p95 latency, deadline, and priority in Prometheus or Datadog, but they still hand tune worker counts during launches, provider incidents, model migrations, and end of month billing freezes. Use it when you need a deterministic C command line allocator that turns queue CSV into an auditable worker plan for Kubernetes HPA limits, Nomad task groups, Celery pools, Sidekiq shards, GitHub Actions runners, Vercel background jobs, Cloudflare queues, or an internal AI platform scheduler. The trick: it calculates demand from arrival rate plus backlog drain pressure, multiplies by latency pressure when p95 is already past the deadline, respects min and max worker fences, and then allocates scarce slots by risk instead of alphabetical order or whoever paged first. Drop this into an infra repository, CI runbook, incident response tool, model evaluation platform, data pipeline scheduler, or developer productivity control plane when you want searchable production code for AI queue SLO allocation, batch inference capacity planning, eval runner fairness, DevOps concurrency budgeting, edge compute backlog control, and real time worker right sizing without pulling in a database, broker client, or cloud SDK.
*/