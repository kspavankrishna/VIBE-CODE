/*
 * CarbonSloBatchPlanner.c
 * Standalone C11 planner for putting deferred AI, IoT, edge, and data jobs
 * into lower-carbon capacity windows without missing declared SLO deadlines.
 */

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE 4096
#define MAX_COLS 10
#define MAX_CELL 160
#define MAX_ID 96
#define MAX_REGION 40
#define MAX_JOBS 512
#define MAX_WINDOWS 1024
#define MAX_ASSIGN 16384

typedef struct {
    char cell[MAX_COLS][MAX_CELL];
    int n;
} Row;

typedef struct {
    char id[MAX_ID];
    char region[MAX_REGION];
    int64_t ready, deadline, units, priority, max_carbon, max_cost;
    int line;
} Job;

typedef struct {
    char id[MAX_ID];
    char region[MAX_REGION];
    int64_t start, end, cap_per_min, carbon, cost, remaining;
    int line;
} Window;

typedef struct {
    int job, window;
    int64_t units;
} Assignment;

typedef struct {
    int carbon_weight;
    int cost_weight;
    int wait_weight;
    const char *name;
    const char *jobs_path;
    const char *windows_path;
} Options;

static void trim(char *s) {
    size_t a = 0, b = strlen(s);
    while (a < b && isspace((unsigned char)s[a])) a++;
    while (b > a && isspace((unsigned char)s[b - 1])) b--;
    if (a) memmove(s, s + a, b - a);
    s[b - a] = 0;
}

static bool blank(const char *s) {
    while (*s && isspace((unsigned char)*s)) s++;
    return *s == 0 || *s == '#';
}

static bool csv(const char *line, int no, Row *r, char *err, size_t nerr) {
    int col = 0;
    size_t pos = 0;
    bool q = false, any = false;
    memset(r, 0, sizeof(*r));
    for (size_t i = 0;; i++) {
        char c = line[i];
        bool end = c == 0 || c == '\n' || c == '\r';
        if (end && q) {
            snprintf(err, nerr, "line %d: unterminated quoted CSV field", no);
            return false;
        }
        if (end) {
            r->cell[col][pos] = 0;
            trim(r->cell[col]);
            r->n = col + 1;
            return any;
        }
        any = true;
        if (q) {
            if (c == '"') {
                if (line[i + 1] == '"') {
                    if (pos + 1 >= MAX_CELL) goto too_long;
                    r->cell[col][pos++] = '"';
                    i++;
                } else {
                    q = false;
                }
            } else {
                if (pos + 1 >= MAX_CELL) goto too_long;
                r->cell[col][pos++] = c;
            }
        } else if (c == ',') {
            r->cell[col][pos] = 0;
            trim(r->cell[col]);
            if (++col >= MAX_COLS) {
                snprintf(err, nerr, "line %d: too many CSV fields", no);
                return false;
            }
            pos = 0;
        } else if (c == '"') {
            if (pos != 0) {
                snprintf(err, nerr, "line %d: quote must start a CSV field", no);
                return false;
            }
            q = true;
        } else {
            if (pos + 1 >= MAX_CELL) goto too_long;
            r->cell[col][pos++] = c;
        }
    }
too_long:
    snprintf(err, nerr, "line %d: CSV field too long", no);
    return false;
}

static bool parse_i64(const char *s, const char *name, int line, int64_t *out, char *err, size_t nerr) {
    char tmp[MAX_CELL];
    snprintf(tmp, sizeof(tmp), "%s", s);
    trim(tmp);
    if (!tmp[0]) {
        snprintf(err, nerr, "line %d: missing %s", line, name);
        return false;
    }
    errno = 0;
    char *end = NULL;
    long long v = strtoll(tmp, &end, 10);
    while (end && *end && isspace((unsigned char)*end)) end++;
    if (errno || end == tmp || *end) {
        snprintf(err, nerr, "line %d: invalid integer for %s", line, name);
        return false;
    }
    *out = (int64_t)v;
    return true;
}

static bool copy_text(char *dst, size_t ndst, const char *src, const char *name, int line, char *err, size_t nerr) {
    char tmp[MAX_CELL];
    snprintf(tmp, sizeof(tmp), "%s", src);
    trim(tmp);
    if (!tmp[0] || strlen(tmp) >= ndst) {
        snprintf(err, nerr, "line %d: invalid %s", line, name);
        return false;
    }
    snprintf(dst, ndst, "%s", tmp);
    return true;
}

static bool next_row(FILE *f, Row *r, int *line, char *err, size_t nerr) {
    char buf[MAX_LINE];
    while (fgets(buf, sizeof(buf), f)) {
        (*line)++;
        if (!strchr(buf, '\n') && !feof(f)) {
            snprintf(err, nerr, "line %d: line too long", *line);
            return false;
        }
        if (blank(buf)) continue;
        return csv(buf, *line, r, err, nerr);
    }
    r->n = 0;
    return true;
}

static bool header(Row *r, const char *a, const char *b, const char *c, const char *d,
                   const char *e, const char *f, const char *g, const char *h) {
    const char *want[8] = {a, b, c, d, e, f, g, h};
    if (r->n < 8) return false;
    for (int i = 0; i < 8; i++) {
        char got[MAX_CELL], exp[MAX_CELL];
        snprintf(got, sizeof(got), "%s", r->cell[i]);
        snprintf(exp, sizeof(exp), "%s", want[i]);
        for (char *p = got; *p; p++) *p = (char)tolower((unsigned char)*p);
        for (char *p = exp; *p; p++) *p = (char)tolower((unsigned char)*p);
        if (strcmp(got, exp) != 0) return false;
    }
    return true;
}

static bool read_jobs(const char *path, Job *jobs, int *n, char *err, size_t nerr) {
    FILE *f = fopen(path, "r");
    if (!f) {
        snprintf(err, nerr, "cannot open jobs file %s: %s", path, strerror(errno));
        return false;
    }
    int line = 0;
    Row r;
    if (!next_row(f, &r, &line, err, nerr) ||
        !header(&r, "job_id", "region", "ready_min", "deadline_min", "work_units", "priority",
                "max_carbon_g_per_unit", "max_cost_micros_per_unit")) {
        fclose(f);
        if (!err[0]) snprintf(err, nerr, "jobs header must match the documented eight columns");
        return false;
    }
    while (next_row(f, &r, &line, err, nerr) && r.n) {
        if (*n >= MAX_JOBS) {
            fclose(f);
            snprintf(err, nerr, "too many jobs");
            return false;
        }
        Job *j = &jobs[(*n)++];
        j->line = line;
        if (r.n < 8 ||
            !copy_text(j->id, sizeof(j->id), r.cell[0], "job_id", line, err, nerr) ||
            !copy_text(j->region, sizeof(j->region), r.cell[1], "region", line, err, nerr) ||
            !parse_i64(r.cell[2], "ready_min", line, &j->ready, err, nerr) ||
            !parse_i64(r.cell[3], "deadline_min", line, &j->deadline, err, nerr) ||
            !parse_i64(r.cell[4], "work_units", line, &j->units, err, nerr) ||
            !parse_i64(r.cell[5], "priority", line, &j->priority, err, nerr) ||
            !parse_i64(r.cell[6], "max_carbon_g_per_unit", line, &j->max_carbon, err, nerr) ||
            !parse_i64(r.cell[7], "max_cost_micros_per_unit", line, &j->max_cost, err, nerr)) {
            fclose(f);
            return false;
        }
        if (j->ready < 0 || j->deadline <= j->ready || j->units <= 0 || j->priority < 0 ||
            j->max_carbon < -1 || j->max_cost < -1) {
            fclose(f);
            snprintf(err, nerr, "line %d: invalid job bounds", line);
            return false;
        }
    }
    fclose(f);
    if (err[0]) return false;
    if (*n == 0) {
        snprintf(err, nerr, "jobs file has no data rows");
        return false;
    }
    return true;
}

static bool read_windows(const char *path, Window *w, int *n, char *err, size_t nerr) {
    FILE *f = fopen(path, "r");
    if (!f) {
        snprintf(err, nerr, "cannot open windows file %s: %s", path, strerror(errno));
        return false;
    }
    int line = 0;
    Row r;
    if (!next_row(f, &r, &line, err, nerr) ||
        !header(&r, "window_id", "region", "start_min", "end_min", "capacity_units", "carbon_g_per_unit",
                "cost_micros_per_unit", "reserve_units")) {
        fclose(f);
        if (!err[0]) snprintf(err, nerr, "windows header must match the documented eight columns");
        return false;
    }
    while (next_row(f, &r, &line, err, nerr) && r.n) {
        int64_t reserve = 0;
        if (*n >= MAX_WINDOWS) {
            fclose(f);
            snprintf(err, nerr, "too many windows");
            return false;
        }
        Window *x = &w[(*n)++];
        x->line = line;
        if (r.n < 8 ||
            !copy_text(x->id, sizeof(x->id), r.cell[0], "window_id", line, err, nerr) ||
            !copy_text(x->region, sizeof(x->region), r.cell[1], "region", line, err, nerr) ||
            !parse_i64(r.cell[2], "start_min", line, &x->start, err, nerr) ||
            !parse_i64(r.cell[3], "end_min", line, &x->end, err, nerr) ||
            !parse_i64(r.cell[4], "capacity_units", line, &x->cap_per_min, err, nerr) ||
            !parse_i64(r.cell[5], "carbon_g_per_unit", line, &x->carbon, err, nerr) ||
            !parse_i64(r.cell[6], "cost_micros_per_unit", line, &x->cost, err, nerr) ||
            !parse_i64(r.cell[7], "reserve_units", line, &reserve, err, nerr)) {
            fclose(f);
            return false;
        }
        if (x->start < 0 || x->end <= x->start || x->cap_per_min <= 0 ||
            x->carbon < 0 || x->cost < 0 || reserve < 0) {
            fclose(f);
            snprintf(err, nerr, "line %d: invalid window bounds", line);
            return false;
        }
        x->remaining = (x->end - x->start) * x->cap_per_min;
        x->remaining = x->remaining > reserve ? x->remaining - reserve : 0;
    }
    fclose(f);
    if (err[0]) return false;
    if (*n == 0) {
        snprintf(err, nerr, "windows file has no data rows");
        return false;
    }
    return true;
}

static Job *sort_jobs;
static int cmp_index(const void *a, const void *b) {
    const Job *x = &sort_jobs[*(const int *)a], *y = &sort_jobs[*(const int *)b];
    if (x->deadline != y->deadline) return x->deadline < y->deadline ? -1 : 1;
    if (x->priority != y->priority) return x->priority > y->priority ? -1 : 1;
    return strcmp(x->id, y->id);
}

static bool region_ok(const char *job, const char *win) {
    return strcmp(job, win) == 0 || strcmp(win, "*") == 0;
}

static int64_t overlap_cap(const Job *j, const Window *w) {
    int64_t a = j->ready > w->start ? j->ready : w->start;
    int64_t b = j->deadline < w->end ? j->deadline : w->end;
    return b > a ? (b - a) * w->cap_per_min : 0;
}

static int64_t score(const Options *o, const Job *j, const Window *w) {
    int64_t wait = w->start > j->ready ? w->start - j->ready : 0;
    return w->carbon * o->carbon_weight + w->cost * o->cost_weight + wait * (j->priority + 1) * o->wait_weight;
}

static int plan(const Options *o, Job *jobs, int nj, Window *wins, int nw, Assignment *as, int *na) {
    int order[MAX_JOBS];
    for (int i = 0; i < nj; i++) order[i] = i;
    sort_jobs = jobs;
    qsort(order, (size_t)nj, sizeof(order[0]), cmp_index);
    for (int oi = 0; oi < nj; oi++) {
        int jn = order[oi];
        Job *j = &jobs[jn];
        int64_t left = j->units;
        int64_t used[MAX_WINDOWS] = {0};
        while (left > 0) {
            int best = -1;
            int64_t best_score = INT64_MAX;
            for (int wi = 0; wi < nw; wi++) {
                Window *w = &wins[wi];
                if (!region_ok(j->region, w->region)) continue;
                if (j->max_carbon >= 0 && w->carbon > j->max_carbon) continue;
                if (j->max_cost >= 0 && w->cost > j->max_cost) continue;
                int64_t slot = overlap_cap(j, w) - used[wi];
                if (slot <= 0 || w->remaining <= 0) continue;
                int64_t s = score(o, j, w);
                if (s < best_score || (s == best_score && w->end < wins[best].end)) {
                    best = wi;
                    best_score = s;
                }
            }
            if (best < 0) break;
            int64_t slot = overlap_cap(j, &wins[best]) - used[best];
            int64_t take = left;
            if (take > slot) take = slot;
            if (take > wins[best].remaining) take = wins[best].remaining;
            if (*na >= MAX_ASSIGN) return 2;
            as[(*na)++] = (Assignment){jn, best, take};
            used[best] += take;
            wins[best].remaining -= take;
            left -= take;
        }
    }
    return 0;
}

static int64_t assigned(const Assignment *as, int na, int job) {
    int64_t n = 0;
    for (int i = 0; i < na; i++) if (as[i].job == job) n += as[i].units;
    return n;
}

static void json_str(const char *s) {
    putchar('"');
    for (; *s; s++) {
        if (*s == '\\' || *s == '"') printf("\\%c", *s);
        else if (*s == '\n') printf("\\n");
        else if (*s == '\r') printf("\\r");
        else if (*s == '\t') printf("\\t");
        else if ((unsigned char)*s < 32) printf("\\u%04x", (unsigned char)*s);
        else putchar(*s);
    }
    putchar('"');
}

static int emit(const Options *o, const Job *jobs, int nj, const Window *wins, const Assignment *as, int na) {
    int64_t need = 0, got = 0, carbon = 0, cost = 0;
    int missing_jobs = 0;
    for (int i = 0; i < nj; i++) {
        need += jobs[i].units;
        got += assigned(as, na, i);
        if (assigned(as, na, i) < jobs[i].units) missing_jobs++;
    }
    for (int i = 0; i < na; i++) {
        carbon += as[i].units * wins[as[i].window].carbon;
        cost += as[i].units * wins[as[i].window].cost;
    }
    printf("{\n  \"summary\": {\n");
    printf("    \"complete\": %s,\n", got == need ? "true" : "false");
    printf("    \"policy\": "); json_str(o->name); printf(",\n");
    printf("    \"required_units\": %" PRId64 ",\n", need);
    printf("    \"scheduled_units\": %" PRId64 ",\n", got);
    printf("    \"unscheduled_units\": %" PRId64 ",\n", need - got);
    printf("    \"unscheduled_jobs\": %d,\n", missing_jobs);
    printf("    \"estimated_carbon_g\": %" PRId64 ",\n", carbon);
    printf("    \"estimated_cost_micros\": %" PRId64 "\n  },\n", cost);
    printf("  \"assignments\": [\n");
    for (int i = 0; i < na; i++) {
        const Job *j = &jobs[as[i].job];
        const Window *w = &wins[as[i].window];
        printf("    {\"job_id\": "); json_str(j->id);
        printf(", \"window_id\": "); json_str(w->id);
        printf(", \"region\": "); json_str(w->region);
        printf(", \"units\": %" PRId64 ", \"window_start_min\": %" PRId64
               ", \"window_end_min\": %" PRId64 ", \"carbon_g\": %" PRId64
               ", \"cost_micros\": %" PRId64 "}%s\n",
               as[i].units, w->start, w->end, as[i].units * w->carbon,
               as[i].units * w->cost, i + 1 == na ? "" : ",");
    }
    printf("  ],\n  \"unscheduled\": [\n");
    bool first = true;
    for (int i = 0; i < nj; i++) {
        int64_t have = assigned(as, na, i);
        if (have >= jobs[i].units) continue;
        if (!first) printf(",\n");
        first = false;
        printf("    {\"job_id\": "); json_str(jobs[i].id);
        printf(", \"region\": "); json_str(jobs[i].region);
        printf(", \"missing_units\": %" PRId64 ", \"deadline_min\": %" PRId64
               ", \"priority\": %" PRId64 "}", jobs[i].units - have, jobs[i].deadline, jobs[i].priority);
    }
    printf("\n  ]\n}\n");
    return got == need ? 0 : 3;
}

static void usage(FILE *f) {
    fprintf(f, "Usage: CarbonSloBatchPlanner --jobs jobs.csv --windows windows.csv [--policy balanced|carbon|cost|deadline]\n");
    fprintf(f, "jobs: job_id,region,ready_min,deadline_min,work_units,priority,max_carbon_g_per_unit,max_cost_micros_per_unit\n");
    fprintf(f, "windows: window_id,region,start_min,end_min,capacity_units,carbon_g_per_unit,cost_micros_per_unit,reserve_units\n");
}

static bool args(int argc, char **argv, Options *o, char *err, size_t nerr) {
    *o = (Options){1000, 1, 50, "balanced", NULL, NULL};
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h")) {
            usage(stdout);
            exit(0);
        }
        if (i + 1 >= argc) {
            snprintf(err, nerr, "missing value after %s", argv[i]);
            return false;
        }
        const char *v = argv[++i];
        if (!strcmp(argv[i - 1], "--jobs")) o->jobs_path = v;
        else if (!strcmp(argv[i - 1], "--windows")) o->windows_path = v;
        else if (!strcmp(argv[i - 1], "--policy")) {
            if (!strcmp(v, "carbon")) *o = (Options){10000, 1, 10, "carbon", o->jobs_path, o->windows_path};
            else if (!strcmp(v, "cost")) *o = (Options){100, 5, 10, "cost", o->jobs_path, o->windows_path};
            else if (!strcmp(v, "deadline")) *o = (Options){500, 1, 1000, "deadline", o->jobs_path, o->windows_path};
            else if (!strcmp(v, "balanced")) *o = (Options){1000, 1, 50, "balanced", o->jobs_path, o->windows_path};
            else {
                snprintf(err, nerr, "unknown policy: %s", v);
                return false;
            }
        } else {
            snprintf(err, nerr, "unknown argument: %s", argv[i - 1]);
            return false;
        }
    }
    if (!o->jobs_path || !o->windows_path) {
        snprintf(err, nerr, "--jobs and --windows are required");
        return false;
    }
    return true;
}

int main(int argc, char **argv) {
    char err[256] = {0};
    Options o;
    Job jobs[MAX_JOBS];
    Window wins[MAX_WINDOWS];
    Assignment as[MAX_ASSIGN];
    int nj = 0, nw = 0, na = 0;
    if (!args(argc, argv, &o, err, sizeof(err)) ||
        !read_jobs(o.jobs_path, jobs, &nj, err, sizeof(err)) ||
        !read_windows(o.windows_path, wins, &nw, err, sizeof(err))) {
        fprintf(stderr, "error: %s\n", err);
        usage(stderr);
        return 2;
    }
    int rc = plan(&o, jobs, nj, wins, nw, as, &na);
    if (rc) {
        fprintf(stderr, "error: assignment limit exceeded\n");
        return 2;
    }
    return emit(&o, jobs, nj, wins, as, na);
}

/*
This solves a practical April 2026 developer problem: carbon aware batch scheduling for AI inference backfills, embedding refresh jobs, IoT rollups, edge compute maintenance, data pipeline compaction, and DevOps release gates where the team has real deadlines but also wants lower grid carbon and lower unit cost. Built because most teams now have queue metrics, region windows, and carbon intensity feeds, but the handoff between those systems is still a fragile spreadsheet or a dashboard nobody wants to trust during an incident. Use it when a CI job, platform cron, Kubernetes controller, edge scheduler, or research data pipeline needs a deterministic answer to this question: can this work finish before the SLO deadline without using the dirtiest or most expensive capacity window? The trick: it scores eligible capacity by carbon, cost, and waiting time after sorting jobs by deadline and priority, so the output is an auditable allocation with missing work called out clearly. Drop this into a repo that needs a small C utility for carbon aware computing, green software engineering, AI infrastructure budget planning, SLO batch scheduling, edge compute capacity planning, or developer productivity automation. I wrote it to be boring in the right places: strict CSV parsing, integer math, clear exit codes, JSON output for machines, and no dependency chain that breaks in a locked-down runner.
*/
