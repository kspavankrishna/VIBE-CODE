#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <pwd.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define GLB_VERSION "1"
#define GLB_DEFAULT_STATE_DIR "/tmp/gpu-lease-broker"
#define GLB_DEFAULT_TTL_MS 60000LL
#define GLB_DEFAULT_WAIT_MS 0LL
#define GLB_DEFAULT_RENEW_MS 20000LL
#define GLB_DEFAULT_STALE_GRACE_MS 5000LL
#define GLB_MAX_RESOURCES 128
#define GLB_MAX_VALUE 1024
#define GLB_MAX_TOKEN 80
#define GLB_POLL_MS 200LL

typedef struct {
    bool json;
    bool cuda_from_resource;
    bool quiet;
    char state_dir[PATH_MAX];
    long long ttl_ms;
    long long wait_ms;
    long long renew_ms;
    long long stale_grace_ms;
    char token[GLB_MAX_TOKEN];
    char owner[256];
    char note[512];
    const char *resources[GLB_MAX_RESOURCES];
    size_t resource_count;
    char **exec_argv;
} CliOptions;

typedef struct {
    bool valid;
    bool pid_checked;
    char resource[256];
    char token[GLB_MAX_TOKEN];
    char host[256];
    char user[256];
    char owner[256];
    char note[512];
    long long created_at_ms;
    long long updated_at_ms;
    long long expires_at_ms;
    pid_t pid;
    uid_t uid;
} LeaseMeta;

typedef struct {
    char sanitized[256];
    char lease_dir[PATH_MAX];
    char meta_path[PATH_MAX];
} LeasePath;

typedef struct {
    char local_host[256];
    char local_user[256];
    uid_t uid;
    pid_t pid;
} RuntimeInfo;

static volatile sig_atomic_t g_forward_signal = 0;

static void failf(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    va_end(ap);
}

static long long now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long) tv.tv_sec * 1000LL + (long long) tv.tv_usec / 1000LL;
}

static void sleep_ms(long long ms) {
    if (ms <= 0) {
        return;
    }
    struct timespec req;
    req.tv_sec = (time_t) (ms / 1000LL);
    req.tv_nsec = (long) ((ms % 1000LL) * 1000000LL);
    while (nanosleep(&req, &req) == -1 && errno == EINTR) {
    }
}

static unsigned long hash_u64(unsigned long seed, unsigned long value) {
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
    return seed;
}

static void random_hex_token(char *out, size_t out_size) {
    static const char hex[] = "0123456789abcdef";
    unsigned char bytes[24];
    bool have_random = false;
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd >= 0) {
        ssize_t n = read(fd, bytes, sizeof(bytes));
        close(fd);
        have_random = (n == (ssize_t) sizeof(bytes));
    }
    if (!have_random) {
        unsigned long seed = (unsigned long) now_ms();
        seed = hash_u64(seed, (unsigned long) getpid());
        seed = hash_u64(seed, (unsigned long) getppid());
        for (size_t i = 0; i < sizeof(bytes); ++i) {
            seed = hash_u64(seed, (unsigned long) i);
            bytes[i] = (unsigned char) (seed & 0xffU);
        }
    }
    size_t want = (sizeof(bytes) * 2U) + 1U;
    if (out_size < want) {
        if (out_size > 0) {
            out[0] = '\0';
        }
        return;
    }
    for (size_t i = 0; i < sizeof(bytes); ++i) {
        out[i * 2U] = hex[(bytes[i] >> 4U) & 0x0fU];
        out[(i * 2U) + 1U] = hex[bytes[i] & 0x0fU];
    }
    out[sizeof(bytes) * 2U] = '\0';
}

static void json_print_string(FILE *out, const char *value) {
    fputc('"', out);
    for (const unsigned char *p = (const unsigned char *) value; *p != '\0'; ++p) {
        switch (*p) {
            case '\\':
                fputs("\\\\", out);
                break;
            case '"':
                fputs("\\\"", out);
                break;
            case '\b':
                fputs("\\b", out);
                break;
            case '\f':
                fputs("\\f", out);
                break;
            case '\n':
                fputs("\\n", out);
                break;
            case '\r':
                fputs("\\r", out);
                break;
            case '\t':
                fputs("\\t", out);
                break;
            default:
                if (*p < 0x20U) {
                    fprintf(out, "\\u%04x", (unsigned int) *p);
                } else {
                    fputc(*p, out);
                }
        }
    }
    fputc('"', out);
}

static int get_runtime(RuntimeInfo *rt) {
    memset(rt, 0, sizeof(*rt));
    rt->uid = getuid();
    rt->pid = getpid();
    if (gethostname(rt->local_host, sizeof(rt->local_host) - 1U) != 0) {
        return -1;
    }
    rt->local_host[sizeof(rt->local_host) - 1U] = '\0';
    const char *user = getenv("USER");
    if (user == NULL || user[0] == '\0') {
        struct passwd *pwd = getpwuid(rt->uid);
        user = (pwd != NULL && pwd->pw_name != NULL) ? pwd->pw_name : "unknown";
    }
    snprintf(rt->local_user, sizeof(rt->local_user), "%s", user);
    return 0;
}

static bool str_to_ll(const char *text, long long *out) {
    if (text == NULL || *text == '\0') {
        return false;
    }
    char *end = NULL;
    errno = 0;
    long long value = strtoll(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return false;
    }
    *out = value;
    return true;
}

static bool str_to_pid(const char *text, pid_t *out) {
    long long value = 0;
    if (!str_to_ll(text, &value) || value <= 0 || value > INT_MAX) {
        return false;
    }
    *out = (pid_t) value;
    return true;
}

static bool str_to_uid(const char *text, uid_t *out) {
    long long value = 0;
    if (!str_to_ll(text, &value) || value < 0 || (unsigned long long) value > UINT_MAX) {
        return false;
    }
    *out = (uid_t) value;
    return true;
}

static void sanitize_resource_name(const char *resource, char *out, size_t out_size) {
    size_t j = 0;
    for (size_t i = 0; resource[i] != '\0' && j + 1U < out_size; ++i) {
        unsigned char ch = (unsigned char) resource[i];
        if (isalnum(ch) || ch == '.' || ch == '_' || ch == '-') {
            out[j++] = (char) ch;
        } else {
            out[j++] = '_';
        }
    }
    out[j] = '\0';
    if (j == 0) {
        snprintf(out, out_size, "unnamed");
    }
}

static int mkdir_p(const char *path, mode_t mode) {
    char buffer[PATH_MAX];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(buffer)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    snprintf(buffer, sizeof(buffer), "%s", path);
    for (size_t i = 1; i < len; ++i) {
        if (buffer[i] == '/') {
            buffer[i] = '\0';
            if (mkdir(buffer, mode) != 0 && errno != EEXIST) {
                return -1;
            }
            buffer[i] = '/';
        }
    }
    if (mkdir(buffer, mode) != 0 && errno != EEXIST) {
        return -1;
    }
    return 0;
}

static int build_lease_path(const char *state_dir, const char *resource, LeasePath *out) {
    memset(out, 0, sizeof(*out));
    sanitize_resource_name(resource, out->sanitized, sizeof(out->sanitized));
    int n1 = snprintf(out->lease_dir, sizeof(out->lease_dir), "%s/%s.lease", state_dir, out->sanitized);
    int n2 = snprintf(out->meta_path, sizeof(out->meta_path), "%s/lease.meta", out->lease_dir);
    if (n1 < 0 || n2 < 0 || (size_t) n1 >= sizeof(out->lease_dir) || (size_t) n2 >= sizeof(out->meta_path)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int kv_escape(const char *src, char *dst, size_t dst_size) {
    size_t j = 0;
    for (size_t i = 0; src[i] != '\0'; ++i) {
        const char *rep = NULL;
        char one[2] = {src[i], '\0'};
        switch (src[i]) {
            case '\\':
                rep = "\\\\";
                break;
            case '\n':
                rep = "\\n";
                break;
            case '\r':
                rep = "\\r";
                break;
            case '\t':
                rep = "\\t";
                break;
            default:
                rep = one;
                break;
        }
        size_t rep_len = strlen(rep);
        if (j + rep_len + 1U > dst_size) {
            errno = ENOSPC;
            return -1;
        }
        memcpy(dst + j, rep, rep_len);
        j += rep_len;
    }
    dst[j] = '\0';
    return 0;
}

static void kv_unescape_inplace(char *text) {
    size_t j = 0;
    for (size_t i = 0; text[i] != '\0'; ++i) {
        if (text[i] == '\\' && text[i + 1] != '\0') {
            ++i;
            switch (text[i]) {
                case 'n':
                    text[j++] = '\n';
                    break;
                case 'r':
                    text[j++] = '\r';
                    break;
                case 't':
                    text[j++] = '\t';
                    break;
                case '\\':
                    text[j++] = '\\';
                    break;
                default:
                    text[j++] = text[i];
                    break;
            }
        } else {
            text[j++] = text[i];
        }
    }
    text[j] = '\0';
}

static int write_all(int fd, const char *buffer, size_t size) {
    size_t offset = 0;
    while (offset < size) {
        ssize_t n = write(fd, buffer + offset, size - offset);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        offset += (size_t) n;
    }
    return 0;
}

static int write_file_atomic(const char *path, const char *content, mode_t mode) {
    char tmp_path[PATH_MAX];
    int n = snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%ld.%lld", path, (long) getpid(), now_ms());
    if (n < 0 || (size_t) n >= sizeof(tmp_path)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0) {
        return -1;
    }
    size_t size = strlen(content);
    if (write_all(fd, content, size) != 0 || fsync(fd) != 0 || close(fd) != 0) {
        int saved = errno;
        unlink(tmp_path);
        errno = saved;
        return -1;
    }
    if (rename(tmp_path, path) != 0) {
        int saved = errno;
        unlink(tmp_path);
        errno = saved;
        return -1;
    }
    return 0;
}

static int lease_meta_serialize(const LeaseMeta *meta, char *buffer, size_t buffer_size) {
    char esc_resource[GLB_MAX_VALUE];
    char esc_token[GLB_MAX_VALUE];
    char esc_host[GLB_MAX_VALUE];
    char esc_user[GLB_MAX_VALUE];
    char esc_owner[GLB_MAX_VALUE];
    char esc_note[GLB_MAX_VALUE];
    if (kv_escape(meta->resource, esc_resource, sizeof(esc_resource)) != 0 ||
        kv_escape(meta->token, esc_token, sizeof(esc_token)) != 0 ||
        kv_escape(meta->host, esc_host, sizeof(esc_host)) != 0 ||
        kv_escape(meta->user, esc_user, sizeof(esc_user)) != 0 ||
        kv_escape(meta->owner, esc_owner, sizeof(esc_owner)) != 0 ||
        kv_escape(meta->note, esc_note, sizeof(esc_note)) != 0) {
        return -1;
    }
    int written = snprintf(
        buffer,
        buffer_size,
        "version=%s\n"
        "resource=%s\n"
        "token=%s\n"
        "host=%s\n"
        "user=%s\n"
        "owner=%s\n"
        "note=%s\n"
        "pid_checked=%d\n"
        "pid=%ld\n"
        "uid=%lu\n"
        "created_at_ms=%lld\n"
        "updated_at_ms=%lld\n"
        "expires_at_ms=%lld\n",
        GLB_VERSION,
        esc_resource,
        esc_token,
        esc_host,
        esc_user,
        esc_owner,
        esc_note,
        meta->pid_checked ? 1 : 0,
        (long) meta->pid,
        (unsigned long) meta->uid,
        meta->created_at_ms,
        meta->updated_at_ms,
        meta->expires_at_ms
    );
    if (written < 0 || (size_t) written >= buffer_size) {
        errno = ENOSPC;
        return -1;
    }
    return 0;
}

static int lease_meta_write(const char *meta_path, const LeaseMeta *meta) {
    char buffer[4096];
    if (lease_meta_serialize(meta, buffer, sizeof(buffer)) != 0) {
        return -1;
    }
    return write_file_atomic(meta_path, buffer, 0600);
}

static int lease_meta_load(const char *meta_path, LeaseMeta *out) {
    memset(out, 0, sizeof(*out));
    FILE *fp = fopen(meta_path, "r");
    if (fp == NULL) {
        return -1;
    }
    char line[2048];
    while (fgets(line, sizeof(line), fp) != NULL) {
        char *eq = strchr(line, '=');
        if (eq == NULL) {
            continue;
        }
        *eq = '\0';
        char *key = line;
        char *value = eq + 1;
        size_t len = strlen(value);
        while (len > 0 && (value[len - 1] == '\n' || value[len - 1] == '\r')) {
            value[--len] = '\0';
        }
        kv_unescape_inplace(value);
        if (strcmp(key, "resource") == 0) {
            snprintf(out->resource, sizeof(out->resource), "%s", value);
        } else if (strcmp(key, "token") == 0) {
            snprintf(out->token, sizeof(out->token), "%s", value);
        } else if (strcmp(key, "host") == 0) {
            snprintf(out->host, sizeof(out->host), "%s", value);
        } else if (strcmp(key, "user") == 0) {
            snprintf(out->user, sizeof(out->user), "%s", value);
        } else if (strcmp(key, "owner") == 0) {
            snprintf(out->owner, sizeof(out->owner), "%s", value);
        } else if (strcmp(key, "note") == 0) {
            snprintf(out->note, sizeof(out->note), "%s", value);
        } else if (strcmp(key, "pid_checked") == 0) {
            long long enabled = 0;
            if (str_to_ll(value, &enabled)) {
                out->pid_checked = enabled != 0;
            }
        } else if (strcmp(key, "created_at_ms") == 0) {
            str_to_ll(value, &out->created_at_ms);
        } else if (strcmp(key, "updated_at_ms") == 0) {
            str_to_ll(value, &out->updated_at_ms);
        } else if (strcmp(key, "expires_at_ms") == 0) {
            str_to_ll(value, &out->expires_at_ms);
        } else if (strcmp(key, "pid") == 0) {
            str_to_pid(value, &out->pid);
        } else if (strcmp(key, "uid") == 0) {
            str_to_uid(value, &out->uid);
        }
    }
    fclose(fp);
    out->valid = out->resource[0] != '\0' && out->token[0] != '\0' && out->host[0] != '\0' && out->pid > 0;
    return out->valid ? 0 : -1;
}

static bool is_process_alive(pid_t pid) {
    if (pid <= 0) {
        return false;
    }
    if (kill(pid, 0) == 0) {
        return true;
    }
    return errno == EPERM;
}

static int stat_mtime_ms(const char *path, long long *out_ms) {
    struct stat st;
    if (stat(path, &st) != 0) {
        return -1;
    }
    *out_ms = (long long) st.st_mtime * 1000LL;
    return 0;
}

static bool lease_is_stale(const LeasePath *path, const LeaseMeta *meta, const CliOptions *opts, const RuntimeInfo *rt, long long now) {
    long long dir_mtime = 0;
    if (!meta->valid) {
        if (stat_mtime_ms(path->lease_dir, &dir_mtime) != 0) {
            return false;
        }
        return now - dir_mtime >= opts->stale_grace_ms;
    }
    if (meta->expires_at_ms > 0 && meta->expires_at_ms <= now) {
        return true;
    }
    if (meta->pid_checked && strcmp(meta->host, rt->local_host) == 0 && meta->pid > 0 && !is_process_alive(meta->pid)) {
        return true;
    }
    return false;
}

static int remove_dir_recursive(const char *path) {
    DIR *dir = opendir(path);
    if (dir == NULL) {
        if (errno == ENOENT) {
            return 0;
        }
        return -1;
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        char child[PATH_MAX];
        int n = snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
        if (n < 0 || (size_t) n >= sizeof(child)) {
            closedir(dir);
            errno = ENAMETOOLONG;
            return -1;
        }
        struct stat st;
        if (lstat(child, &st) != 0) {
            continue;
        }
        if (S_ISDIR(st.st_mode)) {
            if (remove_dir_recursive(child) != 0) {
                closedir(dir);
                return -1;
            }
        } else if (unlink(child) != 0 && errno != ENOENT) {
            closedir(dir);
            return -1;
        }
    }
    closedir(dir);
    if (rmdir(path) != 0 && errno != ENOENT) {
        return -1;
    }
    return 0;
}

static int move_lease_dir(const LeasePath *path, const char *suffix, char *moved_path, size_t moved_size) {
    const char *slash = strrchr(path->lease_dir, '/');
    char dir_root[PATH_MAX];
    char lease_name[256];
    if (slash == NULL) {
        snprintf(dir_root, sizeof(dir_root), ".");
        snprintf(lease_name, sizeof(lease_name), "%s", path->lease_dir);
    } else {
        size_t root_len = (size_t) (slash - path->lease_dir);
        if (root_len >= sizeof(dir_root)) {
            errno = ENAMETOOLONG;
            return -1;
        }
        memcpy(dir_root, path->lease_dir, root_len);
        dir_root[root_len] = '\0';
        snprintf(lease_name, sizeof(lease_name), "%s", slash + 1);
    }
    int n = snprintf(moved_path, moved_size, "%s/.%s.%s.%ld.%lld", dir_root, lease_name, suffix, (long) getpid(), now_ms());
    if (n < 0 || (size_t) n >= moved_size) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return rename(path->lease_dir, moved_path);
}

static int reap_lease_dir(const LeasePath *path, const char *reason) {
    char moved[PATH_MAX];
    if (move_lease_dir(path, reason, moved, sizeof(moved)) != 0) {
        return -1;
    }
    return remove_dir_recursive(moved);
}

static void print_meta_json(const LeaseMeta *meta, bool stale) {
    fputs("{", stdout);
    fputs("\"resource\":", stdout);
    json_print_string(stdout, meta->resource);
    fputs(",\"token\":", stdout);
    json_print_string(stdout, meta->token);
    fputs(",\"host\":", stdout);
    json_print_string(stdout, meta->host);
    fputs(",\"user\":", stdout);
    json_print_string(stdout, meta->user);
    fputs(",\"owner\":", stdout);
    json_print_string(stdout, meta->owner);
    fputs(",\"note\":", stdout);
    json_print_string(stdout, meta->note);
    fprintf(
        stdout,
        ",\"pid\":%ld,\"uid\":%lu,\"created_at_ms\":%lld,\"updated_at_ms\":%lld,\"expires_at_ms\":%lld,\"stale\":%s}",
        (long) meta->pid,
        (unsigned long) meta->uid,
        meta->created_at_ms,
        meta->updated_at_ms,
        meta->expires_at_ms,
        stale ? "true" : "false"
    );
}

static void print_acquire_result(const LeaseMeta *meta, bool json) {
    if (json) {
        print_meta_json(meta, false);
        fputc('\n', stdout);
        return;
    }
    printf("%s %s\n", meta->resource, meta->token);
}

static void fill_new_meta(const char *resource, const CliOptions *opts, const RuntimeInfo *rt, bool pid_checked, LeaseMeta *meta) {
    memset(meta, 0, sizeof(*meta));
    meta->valid = true;
    meta->pid_checked = pid_checked;
    snprintf(meta->resource, sizeof(meta->resource), "%s", resource);
    snprintf(meta->token, sizeof(meta->token), "%s", opts->token);
    snprintf(meta->host, sizeof(meta->host), "%s", rt->local_host);
    snprintf(meta->user, sizeof(meta->user), "%s", rt->local_user);
    snprintf(meta->owner, sizeof(meta->owner), "%s", opts->owner);
    snprintf(meta->note, sizeof(meta->note), "%s", opts->note);
    meta->created_at_ms = now_ms();
    meta->updated_at_ms = meta->created_at_ms;
    meta->expires_at_ms = meta->created_at_ms + opts->ttl_ms;
    meta->pid = rt->pid;
    meta->uid = rt->uid;
}

static int try_acquire_single(const char *resource, const CliOptions *opts, const RuntimeInfo *rt, LeaseMeta *acquired) {
    LeasePath path;
    if (build_lease_path(opts->state_dir, resource, &path) != 0) {
        return -1;
    }
    if (mkdir(path.lease_dir, 0700) == 0) {
        LeaseMeta meta;
        fill_new_meta(resource, opts, rt, opts->exec_argv != NULL, &meta);
        if (lease_meta_write(path.meta_path, &meta) != 0) {
            int saved = errno;
            remove_dir_recursive(path.lease_dir);
            errno = saved;
            return -1;
        }
        *acquired = meta;
        return 0;
    }
    if (errno != EEXIST) {
        return -1;
    }

    LeaseMeta existing;
    memset(&existing, 0, sizeof(existing));
    if (lease_meta_load(path.meta_path, &existing) != 0) {
        existing.valid = false;
    }

    long long now = now_ms();
    if (lease_is_stale(&path, &existing, opts, rt, now)) {
        if (reap_lease_dir(&path, "reaped") == 0) {
            return try_acquire_single(resource, opts, rt, acquired);
        }
        if (errno == ENOENT) {
            return try_acquire_single(resource, opts, rt, acquired);
        }
        return -1;
    }
    errno = EBUSY;
    return -1;
}

static int acquire_any(const CliOptions *opts, const RuntimeInfo *rt, LeaseMeta *acquired) {
    if (opts->resource_count == 0) {
        errno = EINVAL;
        return -1;
    }
    long long start_time = now_ms();
    long long deadline = opts->wait_ms > 0 ? start_time + opts->wait_ms : start_time;
    unsigned long seed = hash_u64((unsigned long) start_time, (unsigned long) rt->pid);
    int last_errno = EBUSY;
    for (;;) {
        size_t start = (size_t) (seed % opts->resource_count);
        for (size_t i = 0; i < opts->resource_count; ++i) {
            const char *resource = opts->resources[(start + i) % opts->resource_count];
            if (try_acquire_single(resource, opts, rt, acquired) == 0) {
                return 0;
            }
            last_errno = errno;
            if (errno != EBUSY) {
                return -1;
            }
        }
        long long now = now_ms();
        if (opts->wait_ms <= 0 || now >= deadline) {
            errno = last_errno;
            return -1;
        }
        seed = hash_u64(seed, (unsigned long) now);
        sleep_ms(50LL + (long long) (seed % 101UL));
    }
}

static int load_existing_lease(const CliOptions *opts, const char *resource, LeasePath *path, LeaseMeta *meta) {
    if (build_lease_path(opts->state_dir, resource, path) != 0) {
        return -1;
    }
    if (lease_meta_load(path->meta_path, meta) != 0) {
        errno = ENOENT;
        return -1;
    }
    return 0;
}

static int renew_lease(const CliOptions *opts, const RuntimeInfo *rt, const char *resource, LeaseMeta *renewed) {
    LeasePath path;
    LeaseMeta meta;
    if (load_existing_lease(opts, resource, &path, &meta) != 0) {
        return -1;
    }
    if (strcmp(meta.token, opts->token) != 0) {
        errno = EPERM;
        return -1;
    }
    if (meta.pid_checked && strcmp(meta.host, rt->local_host) == 0 && meta.pid > 0 && !is_process_alive(meta.pid)) {
        errno = ESRCH;
        return -1;
    }
    meta.updated_at_ms = now_ms();
    meta.expires_at_ms = meta.updated_at_ms + opts->ttl_ms;
    if (lease_meta_write(path.meta_path, &meta) != 0) {
        return -1;
    }
    *renewed = meta;
    return 0;
}

static int release_lease(const CliOptions *opts, const char *resource) {
    LeasePath path;
    LeaseMeta meta;
    if (load_existing_lease(opts, resource, &path, &meta) != 0) {
        return -1;
    }
    if (strcmp(meta.token, opts->token) != 0) {
        errno = EPERM;
        return -1;
    }
    return reap_lease_dir(&path, "released");
}

typedef struct {
    char resource[256];
    LeaseMeta meta;
    bool stale;
} LeaseRow;

static int compare_lease_row(const void *lhs, const void *rhs) {
    const LeaseRow *a = (const LeaseRow *) lhs;
    const LeaseRow *b = (const LeaseRow *) rhs;
    return strcmp(a->resource, b->resource);
}

static int scan_leases(const CliOptions *opts, const RuntimeInfo *rt, LeaseRow **rows_out, size_t *count_out) {
    *rows_out = NULL;
    *count_out = 0;
    DIR *dir = opendir(opts->state_dir);
    if (dir == NULL) {
        if (errno == ENOENT) {
            return 0;
        }
        return -1;
    }
    size_t cap = 16;
    size_t count = 0;
    LeaseRow *rows = calloc(cap, sizeof(*rows));
    if (rows == NULL) {
        closedir(dir);
        return -1;
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        size_t name_len = strlen(entry->d_name);
        if (name_len <= 6 || strcmp(entry->d_name + name_len - 6U, ".lease") != 0) {
            continue;
        }
        if (count == cap) {
            cap *= 2U;
            LeaseRow *next = realloc(rows, cap * sizeof(*rows));
            if (next == NULL) {
                free(rows);
                closedir(dir);
                return -1;
            }
            rows = next;
        }
        char resource[256];
        size_t copy_len = name_len - 6U;
        if (copy_len >= sizeof(resource)) {
            copy_len = sizeof(resource) - 1U;
        }
        memcpy(resource, entry->d_name, copy_len);
        resource[copy_len] = '\0';

        LeasePath path;
        int n = snprintf(path.lease_dir, sizeof(path.lease_dir), "%s/%s", opts->state_dir, entry->d_name);
        int m = snprintf(path.meta_path, sizeof(path.meta_path), "%s/lease.meta", path.lease_dir);
        if (n < 0 || m < 0 || (size_t) n >= sizeof(path.lease_dir) || (size_t) m >= sizeof(path.meta_path)) {
            continue;
        }
        snprintf(path.sanitized, sizeof(path.sanitized), "%s", resource);

        LeaseMeta meta;
        memset(&meta, 0, sizeof(meta));
        if (lease_meta_load(path.meta_path, &meta) != 0) {
            meta.valid = false;
            snprintf(meta.resource, sizeof(meta.resource), "%s", resource);
        }
        rows[count].meta = meta;
        snprintf(rows[count].resource, sizeof(rows[count].resource), "%s", resource);
        rows[count].stale = lease_is_stale(&path, &meta, opts, rt, now_ms());
        ++count;
    }
    closedir(dir);
    qsort(rows, count, sizeof(*rows), compare_lease_row);
    *rows_out = rows;
    *count_out = count;
    return 0;
}

static int parse_cuda_index(const char *resource) {
    size_t len = strlen(resource);
    if (len == 0) {
        return -1;
    }
    size_t start = len;
    while (start > 0 && isdigit((unsigned char) resource[start - 1U])) {
        --start;
    }
    if (start == len) {
        return -1;
    }
    if (start == 0) {
        return atoi(resource);
    }
    char prefix[32];
    size_t prefix_len = start < sizeof(prefix) - 1U ? start : sizeof(prefix) - 1U;
    memcpy(prefix, resource, prefix_len);
    prefix[prefix_len] = '\0';
    if (strcasecmp(prefix, "gpu") != 0 && strcasecmp(prefix, "cuda") != 0 && strcasecmp(prefix, "device") != 0) {
        return -1;
    }
    return atoi(resource + start);
}

static void signal_handler(int signum) {
    g_forward_signal = signum;
}

static int install_signal_handlers(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) != 0) {
        return -1;
    }
    if (sigaction(SIGTERM, &sa, NULL) != 0) {
        return -1;
    }
    if (sigaction(SIGHUP, &sa, NULL) != 0) {
        return -1;
    }
    return 0;
}

static int run_command_with_lease(const CliOptions *opts, const RuntimeInfo *rt) {
    LeaseMeta acquired;
    if (acquire_any(opts, rt, &acquired) != 0) {
        return -1;
    }
    if (opts->cuda_from_resource) {
        int idx = parse_cuda_index(acquired.resource);
        if (idx >= 0) {
            char buffer[32];
            snprintf(buffer, sizeof(buffer), "%d", idx);
            setenv("CUDA_VISIBLE_DEVICES", buffer, 1);
        }
    }
    setenv("GPU_LEASE_RESOURCE", acquired.resource, 1);
    setenv("GPU_LEASE_TOKEN", acquired.token, 1);
    setenv("GPU_LEASE_DIR", opts->state_dir, 1);

    if (install_signal_handlers() != 0) {
        int saved = errno;
        CliOptions release_opts = *opts;
        snprintf(release_opts.token, sizeof(release_opts.token), "%s", acquired.token);
        release_lease(&release_opts, acquired.resource);
        errno = saved;
        return -1;
    }

    pid_t child = fork();
    if (child < 0) {
        int saved = errno;
        CliOptions release_opts = *opts;
        snprintf(release_opts.token, sizeof(release_opts.token), "%s", acquired.token);
        release_lease(&release_opts, acquired.resource);
        errno = saved;
        return -1;
    }
    if (child == 0) {
        execvp(opts->exec_argv[0], opts->exec_argv);
        perror("execvp");
        _exit(127);
    }

    long long next_renew = now_ms() + opts->renew_ms;
    int status = 0;
    bool lease_lost = false;
    bool sent_term = false;
    long long kill_deadline = 0;
    for (;;) {
        int result = waitpid(child, &status, WNOHANG);
        if (result == child) {
            break;
        }
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            status = 125 << 8;
            break;
        }

        if (g_forward_signal != 0) {
            kill(child, g_forward_signal);
            g_forward_signal = 0;
        }

        long long now = now_ms();
        if (!lease_lost && now >= next_renew) {
            CliOptions renew_opts = *opts;
            snprintf(renew_opts.token, sizeof(renew_opts.token), "%s", acquired.token);
            LeaseMeta renewed;
            if (renew_lease(&renew_opts, rt, acquired.resource, &renewed) != 0) {
                lease_lost = true;
                sent_term = true;
                kill_deadline = now + 5000LL;
                kill(child, SIGTERM);
            } else {
                next_renew = now + opts->renew_ms;
            }
        }

        if (lease_lost && sent_term && now >= kill_deadline) {
            kill(child, SIGKILL);
            sent_term = false;
        }
        sleep_ms(GLB_POLL_MS);
    }

    CliOptions release_opts = *opts;
    snprintf(release_opts.token, sizeof(release_opts.token), "%s", acquired.token);
    if (release_lease(&release_opts, acquired.resource) != 0 && errno != ENOENT) {
        failf("warning: failed to release lease for %s: %s", acquired.resource, strerror(errno));
    }

    if (lease_lost) {
        failf("lease renewal failed for resource %s", acquired.resource);
        return 70;
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status)) {
        return 128 + WTERMSIG(status);
    }
    return 125;
}

static void usage(FILE *out) {
    fprintf(
        out,
        "GpuLeaseBroker - single-file GPU lease manager for local AI workloads\n"
        "\n"
        "Usage:\n"
        "  GpuLeaseBroker acquire --resource gpu0 [options]\n"
        "  GpuLeaseBroker renew --resource gpu0 --token TOKEN [options]\n"
        "  GpuLeaseBroker release --resource gpu0 --token TOKEN [options]\n"
        "  GpuLeaseBroker list [--json] [options]\n"
        "  GpuLeaseBroker gc [--json] [options]\n"
        "  GpuLeaseBroker run --resource gpu0 [options] -- command args...\n"
        "\n"
        "Options:\n"
        "  --dir PATH               lease directory (default %s)\n"
        "  --resource NAME          lease target; repeat to allow any-of acquisition\n"
        "  --ttl-ms N               lease ttl in milliseconds (default %lld)\n"
        "  --wait-ms N              wait budget for acquire in milliseconds\n"
        "  --renew-ms N             renewal interval for run (default %lld)\n"
        "  --stale-grace-ms N       invalid lease reap grace (default %lld)\n"
        "  --owner TEXT             logical owner label for list output\n"
        "  --note TEXT              free-form context string\n"
        "  --token TOKEN            explicit token for acquire/renew/release\n"
        "  --json                   emit JSON\n"
        "  --quiet                  suppress non-essential stdout for renew/release/gc\n"
        "  --cuda-from-resource     set CUDA_VISIBLE_DEVICES from gpuN/cudaN resource name during run\n"
        "  --help                   show this message\n",
        GLB_DEFAULT_STATE_DIR,
        GLB_DEFAULT_TTL_MS,
        GLB_DEFAULT_RENEW_MS,
        GLB_DEFAULT_STALE_GRACE_MS
    );
}

static int require_positive_ms(const char *flag, long long value) {
    if (value <= 0) {
        failf("%s must be > 0", flag);
        return -1;
    }
    return 0;
}

static int add_resource(CliOptions *opts, const char *value) {
    if (opts->resource_count >= GLB_MAX_RESOURCES) {
        failf("too many resources; max %d", GLB_MAX_RESOURCES);
        return -1;
    }
    opts->resources[opts->resource_count++] = value;
    return 0;
}

static int parse_cli(int argc, char **argv, const char **command_out, CliOptions *opts) {
    memset(opts, 0, sizeof(*opts));
    snprintf(opts->state_dir, sizeof(opts->state_dir), "%s", getenv("GPU_LEASE_BROKER_DIR") != NULL ? getenv("GPU_LEASE_BROKER_DIR") : GLB_DEFAULT_STATE_DIR);
    opts->ttl_ms = GLB_DEFAULT_TTL_MS;
    opts->wait_ms = GLB_DEFAULT_WAIT_MS;
    opts->renew_ms = GLB_DEFAULT_RENEW_MS;
    opts->stale_grace_ms = GLB_DEFAULT_STALE_GRACE_MS;

    if (argc < 2) {
        usage(stderr);
        return -1;
    }
    *command_out = argv[1];
    enum {
        OPT_DIR = 1000,
        OPT_RESOURCE,
        OPT_TTL_MS,
        OPT_WAIT_MS,
        OPT_RENEW_MS,
        OPT_STALE_GRACE_MS,
        OPT_OWNER,
        OPT_NOTE,
        OPT_TOKEN,
        OPT_JSON,
        OPT_QUIET,
        OPT_CUDA_FROM_RESOURCE,
        OPT_HELP
    };
    static const struct option long_options[] = {
        {"dir", required_argument, NULL, OPT_DIR},
        {"resource", required_argument, NULL, OPT_RESOURCE},
        {"ttl-ms", required_argument, NULL, OPT_TTL_MS},
        {"wait-ms", required_argument, NULL, OPT_WAIT_MS},
        {"renew-ms", required_argument, NULL, OPT_RENEW_MS},
        {"stale-grace-ms", required_argument, NULL, OPT_STALE_GRACE_MS},
        {"owner", required_argument, NULL, OPT_OWNER},
        {"note", required_argument, NULL, OPT_NOTE},
        {"token", required_argument, NULL, OPT_TOKEN},
        {"json", no_argument, NULL, OPT_JSON},
        {"quiet", no_argument, NULL, OPT_QUIET},
        {"cuda-from-resource", no_argument, NULL, OPT_CUDA_FROM_RESOURCE},
        {"help", no_argument, NULL, OPT_HELP},
        {0, 0, 0, 0}
    };

    optind = 2;
    for (;;) {
        int option_index = 0;
        int c = getopt_long(argc, argv, "", long_options, &option_index);
        if (c == -1) {
            break;
        }
        switch (c) {
            case OPT_DIR:
                snprintf(opts->state_dir, sizeof(opts->state_dir), "%s", optarg);
                break;
            case OPT_RESOURCE:
                if (add_resource(opts, optarg) != 0) {
                    return -1;
                }
                break;
            case OPT_TTL_MS:
                if (!str_to_ll(optarg, &opts->ttl_ms) || require_positive_ms("--ttl-ms", opts->ttl_ms) != 0) {
                    return -1;
                }
                break;
            case OPT_WAIT_MS:
                if (!str_to_ll(optarg, &opts->wait_ms) || opts->wait_ms < 0) {
                    failf("--wait-ms must be >= 0");
                    return -1;
                }
                break;
            case OPT_RENEW_MS:
                if (!str_to_ll(optarg, &opts->renew_ms) || require_positive_ms("--renew-ms", opts->renew_ms) != 0) {
                    return -1;
                }
                break;
            case OPT_STALE_GRACE_MS:
                if (!str_to_ll(optarg, &opts->stale_grace_ms) || require_positive_ms("--stale-grace-ms", opts->stale_grace_ms) != 0) {
                    return -1;
                }
                break;
            case OPT_OWNER:
                snprintf(opts->owner, sizeof(opts->owner), "%s", optarg);
                break;
            case OPT_NOTE:
                snprintf(opts->note, sizeof(opts->note), "%s", optarg);
                break;
            case OPT_TOKEN:
                snprintf(opts->token, sizeof(opts->token), "%s", optarg);
                break;
            case OPT_JSON:
                opts->json = true;
                break;
            case OPT_QUIET:
                opts->quiet = true;
                break;
            case OPT_CUDA_FROM_RESOURCE:
                opts->cuda_from_resource = true;
                break;
            case OPT_HELP:
                usage(stdout);
                exit(0);
            default:
                usage(stderr);
                return -1;
        }
    }

    if (strcmp(*command_out, "run") == 0) {
        if (optind < argc && strcmp(argv[optind], "--") == 0) {
            ++optind;
        }
        if (optind >= argc) {
            failf("run requires a command after --");
            return -1;
        }
        opts->exec_argv = &argv[optind];
    } else if (optind < argc) {
        failf("unexpected positional argument: %s", argv[optind]);
        return -1;
    }

    if (opts->token[0] == '\0' && (strcmp(*command_out, "acquire") == 0 || strcmp(*command_out, "run") == 0)) {
        random_hex_token(opts->token, sizeof(opts->token));
    }
    if (opts->renew_ms >= opts->ttl_ms) {
        opts->renew_ms = opts->ttl_ms / 2LL;
        if (opts->renew_ms < 1000LL) {
            opts->renew_ms = 1000LL;
        }
    }
    return 0;
}

static int command_acquire(const CliOptions *opts, const RuntimeInfo *rt) {
    if (opts->resource_count == 0) {
        failf("acquire requires at least one --resource");
        return 2;
    }
    LeaseMeta acquired;
    if (acquire_any(opts, rt, &acquired) != 0) {
        failf("acquire failed: %s", strerror(errno));
        return errno == EBUSY ? 75 : 1;
    }
    print_acquire_result(&acquired, opts->json);
    return 0;
}

static int command_renew(const CliOptions *opts, const RuntimeInfo *rt) {
    if (opts->resource_count != 1) {
        failf("renew requires exactly one --resource");
        return 2;
    }
    if (opts->token[0] == '\0') {
        failf("renew requires --token");
        return 2;
    }
    LeaseMeta renewed;
    if (renew_lease(opts, rt, opts->resources[0], &renewed) != 0) {
        failf("renew failed: %s", strerror(errno));
        return 1;
    }
    if (!opts->quiet) {
        print_acquire_result(&renewed, opts->json);
    }
    return 0;
}

static int command_release(const CliOptions *opts) {
    if (opts->resource_count != 1) {
        failf("release requires exactly one --resource");
        return 2;
    }
    if (opts->token[0] == '\0') {
        failf("release requires --token");
        return 2;
    }
    if (release_lease(opts, opts->resources[0]) != 0) {
        failf("release failed: %s", strerror(errno));
        return 1;
    }
    if (!opts->quiet) {
        if (opts->json) {
            printf("{\"resource\":");
            json_print_string(stdout, opts->resources[0]);
            fputs(",\"released\":true}\n", stdout);
        } else {
            printf("%s released\n", opts->resources[0]);
        }
    }
    return 0;
}

static int command_list(const CliOptions *opts, const RuntimeInfo *rt) {
    LeaseRow *rows = NULL;
    size_t count = 0;
    if (scan_leases(opts, rt, &rows, &count) != 0) {
        failf("list failed: %s", strerror(errno));
        return 1;
    }
    if (opts->json) {
        fputc('[', stdout);
        for (size_t i = 0; i < count; ++i) {
            if (i > 0) {
                fputc(',', stdout);
            }
            print_meta_json(&rows[i].meta, rows[i].stale);
        }
        fputs("]\n", stdout);
    } else {
        printf("%-18s %-8s %-8s %-12s %-14s %s\n", "RESOURCE", "STALE", "PID", "EXPIRES_MS", "OWNER", "NOTE");
        for (size_t i = 0; i < count; ++i) {
            printf(
                "%-18s %-8s %-8ld %-12lld %-14.14s %s\n",
                rows[i].meta.resource[0] != '\0' ? rows[i].meta.resource : rows[i].resource,
                rows[i].stale ? "yes" : "no",
                (long) rows[i].meta.pid,
                rows[i].meta.expires_at_ms,
                rows[i].meta.owner,
                rows[i].meta.note
            );
        }
    }
    free(rows);
    return 0;
}

static int command_gc(const CliOptions *opts, const RuntimeInfo *rt) {
    LeaseRow *rows = NULL;
    size_t count = 0;
    if (scan_leases(opts, rt, &rows, &count) != 0) {
        failf("gc failed: %s", strerror(errno));
        return 1;
    }
    size_t removed = 0;
    for (size_t i = 0; i < count; ++i) {
        if (!rows[i].stale) {
            continue;
        }
        LeasePath path;
        if (build_lease_path(opts->state_dir, rows[i].resource, &path) != 0) {
            continue;
        }
        if (reap_lease_dir(&path, "gc") == 0) {
            ++removed;
        }
    }
    if (!opts->quiet) {
        if (opts->json) {
            printf("{\"removed\":%zu,\"scanned\":%zu}\n", removed, count);
        } else {
            printf("removed %zu stale leases out of %zu\n", removed, count);
        }
    }
    free(rows);
    return 0;
}

int main(int argc, char **argv) {
    const char *command = NULL;
    CliOptions opts;
    if (parse_cli(argc, argv, &command, &opts) != 0) {
        return 2;
    }
    if (mkdir_p(opts.state_dir, 0700) != 0) {
        failf("failed to initialize state dir %s: %s", opts.state_dir, strerror(errno));
        return 1;
    }

    RuntimeInfo rt;
    if (get_runtime(&rt) != 0) {
        failf("failed to discover runtime context: %s", strerror(errno));
        return 1;
    }

    if (strcmp(command, "acquire") == 0) {
        return command_acquire(&opts, &rt);
    }
    if (strcmp(command, "renew") == 0) {
        return command_renew(&opts, &rt);
    }
    if (strcmp(command, "release") == 0) {
        return command_release(&opts);
    }
    if (strcmp(command, "list") == 0) {
        return command_list(&opts, &rt);
    }
    if (strcmp(command, "gc") == 0) {
        return command_gc(&opts, &rt);
    }
    if (strcmp(command, "run") == 0) {
        if (opts.resource_count == 0) {
            failf("run requires at least one --resource");
            return 2;
        }
        return run_command_with_lease(&opts, &rt);
    }

    usage(stderr);
    return 2;
}

/*
This solves local GPU lease management for shared AI workstations, self-hosted runners, edge inference boxes, and research machines where several jobs can start at the same time and stomp on the same device. Built because in April 2026 a lot of real model work still happens outside a full scheduler, and "CUDA out of memory" often comes from two unrelated tools launching together.

Use it when you need a small production-ready C utility for GPU lock files, TTL-based device ownership, stale process cleanup, automatic renewals while a command runs, and machine-readable lease inspection without adding Redis, Postgres, Kubernetes, or Slurm. The trick: each GPU lease is an atomic directory created with `mkdir`, so acquisition is race-safe, and stale ownership gets reaped from expiry plus same-host PID checks instead of trusting a fragile pidfile alone.

Drop this into a container image, ML build agent, benchmark harness, lab workstation, remote devbox, or on-prem inference node when you want a single-file GPU scheduler helper, CUDA_VISIBLE_DEVICES bridge, and local lease broker that is easy to compile, easy to audit, and easy to fork for custom AI infrastructure.
*/