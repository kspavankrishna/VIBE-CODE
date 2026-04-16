/*
 * SseReplayShield.c
 *
 * C11 single-file library for parsing Server-Sent Events incrementally and
 * suppressing safe duplicates after reconnects. This is intended for AI
 * gateways, SSE bridges, reverse proxies, terminal streaming clients, and
 * edge sidecars that cannot afford token duplication or malformed framing.
 *
 * Compile:
 *   cc -std=c11 -O2 -c SseReplayShield.c
 *
 * Optional self-test:
 *   cc -std=c11 -Wall -Wextra -pedantic -DSSE_REPLAY_SHIELD_SELF_TEST \
 *      SseReplayShield.c && ./a.out
 */

#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum SrsStatus {
    SRS_STATUS_OK = 0,
    SRS_STATUS_BAD_ARGUMENT,
    SRS_STATUS_OUT_OF_MEMORY,
    SRS_STATUS_LINE_TOO_LARGE,
    SRS_STATUS_EVENT_TYPE_TOO_LARGE,
    SRS_STATUS_EVENT_ID_TOO_LARGE,
    SRS_STATUS_EVENT_DATA_TOO_LARGE,
    SRS_STATUS_MALFORMED_INPUT,
    SRS_STATUS_STOPPED_BY_CALLBACK
} SrsStatus;

typedef struct SrsConfig {
    size_t max_line_bytes;
    size_t max_event_type_bytes;
    size_t max_event_id_bytes;
    size_t max_data_bytes;
    size_t replay_window;
    int dedupe_without_event_id;
    int deliver_duplicates;
} SrsConfig;

typedef struct SrsStats {
    uint64_t bytes_processed;
    uint64_t lines_processed;
    uint64_t events_seen;
    uint64_t events_delivered;
    uint64_t duplicates_suppressed;
    uint64_t callback_stops;
    int reconnect_delay_ms;
} SrsStats;

typedef struct SrsEvent {
    const char *event_type;
    size_t event_type_len;
    const char *event_id;
    size_t event_id_len;
    int event_id_is_explicit;
    const char *data;
    size_t data_len;
    uint64_t ordinal;
    uint64_t fingerprint;
    int is_duplicate;
    int reconnect_delay_ms;
} SrsEvent;

typedef int (*SrsEventCallback)(void *user, const SrsEvent *event);

typedef struct SrsDedupeEntry {
    uint64_t fingerprint;
    uint64_t sequence;
    int in_use;
} SrsDedupeEntry;

typedef struct SrsDedupeTable {
    SrsDedupeEntry *entries;
    size_t capacity;
    size_t replay_window;
} SrsDedupeTable;

typedef struct Srs {
    SrsConfig config;
    SrsStats stats;

    char *line_buf;
    size_t line_len;

    char *event_type_buf;
    size_t event_type_len;

    char *event_id_buf;
    size_t event_id_len;
    int event_id_explicit;

    char *last_event_id_buf;
    size_t last_event_id_len;

    char *data_buf;
    size_t data_len;

    int saw_cr;
    uint64_t event_sequence;
    SrsDedupeTable dedupe;
} Srs;

static const char SRS_DEFAULT_EVENT_NAME[] = "message";
static const char SRS_LAST_EVENT_ID_PREFIX[] = "Last-Event-ID: ";

static size_t srs_next_pow2(size_t n) {
    size_t p;

    if (n <= 1u) {
        return 1u;
    }

    p = 1u;
    while (p < n) {
        if (p > (SIZE_MAX >> 1u)) {
            return 0u;
        }
        p <<= 1u;
    }
    return p;
}

static void srs_clear_text(char *buf, size_t *len) {
    if (buf != NULL) {
        buf[0] = '\0';
    }
    if (len != NULL) {
        *len = 0u;
    }
}

static SrsStatus srs_set_text(
    char *buf,
    size_t cap,
    size_t *len,
    const char *src,
    size_t src_len
) {
    if (buf == NULL || len == NULL || (src == NULL && src_len != 0u)) {
        return SRS_STATUS_BAD_ARGUMENT;
    }
    if (src_len > cap) {
        return SRS_STATUS_EVENT_DATA_TOO_LARGE;
    }
    if (src_len != 0u) {
        memcpy(buf, src, src_len);
    }
    buf[src_len] = '\0';
    *len = src_len;
    return SRS_STATUS_OK;
}

static SrsStatus srs_append_text(
    char *buf,
    size_t cap,
    size_t *len,
    const char *src,
    size_t src_len,
    SrsStatus too_large_status
) {
    if (buf == NULL || len == NULL || (src == NULL && src_len != 0u)) {
        return SRS_STATUS_BAD_ARGUMENT;
    }
    if (*len > cap || src_len > (cap - *len)) {
        return too_large_status;
    }
    if (src_len != 0u) {
        memcpy(buf + *len, src, src_len);
    }
    *len += src_len;
    buf[*len] = '\0';
    return SRS_STATUS_OK;
}

static uint64_t srs_fnv1a64_bytes(uint64_t hash, const void *data, size_t len) {
    const unsigned char *bytes;
    size_t i;

    bytes = (const unsigned char *) data;
    for (i = 0u; i < len; ++i) {
        hash ^= (uint64_t) bytes[i];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static uint64_t srs_fnv1a64_u64(uint64_t hash, uint64_t value) {
    unsigned char bytes[8];
    size_t i;

    for (i = 0u; i < sizeof(bytes); ++i) {
        bytes[i] = (unsigned char) ((value >> (i * 8u)) & UINT64_C(0xff));
    }
    return srs_fnv1a64_bytes(hash, bytes, sizeof(bytes));
}

static SrsStatus srs_alloc_text_buffer(char **buf, size_t cap) {
    if (buf == NULL) {
        return SRS_STATUS_BAD_ARGUMENT;
    }
    *buf = (char *) calloc(cap + 1u, sizeof(char));
    if (*buf == NULL) {
        return SRS_STATUS_OUT_OF_MEMORY;
    }
    return SRS_STATUS_OK;
}

static void srs_dedupe_destroy(SrsDedupeTable *table) {
    if (table == NULL) {
        return;
    }
    free(table->entries);
    table->entries = NULL;
    table->capacity = 0u;
    table->replay_window = 0u;
}

static SrsStatus srs_dedupe_init(SrsDedupeTable *table, size_t replay_window) {
    size_t capacity;

    if (table == NULL) {
        return SRS_STATUS_BAD_ARGUMENT;
    }

    table->entries = NULL;
    table->capacity = 0u;
    table->replay_window = replay_window;

    if (replay_window == 0u) {
        return SRS_STATUS_OK;
    }

    if (replay_window > (SIZE_MAX / 2u)) {
        return SRS_STATUS_BAD_ARGUMENT;
    }

    capacity = srs_next_pow2(replay_window * 2u);
    if (capacity == 0u) {
        return SRS_STATUS_BAD_ARGUMENT;
    }
    if (capacity < 8u) {
        capacity = 8u;
    }

    table->entries = (SrsDedupeEntry *) calloc(capacity, sizeof(SrsDedupeEntry));
    if (table->entries == NULL) {
        return SRS_STATUS_OUT_OF_MEMORY;
    }

    table->capacity = capacity;
    return SRS_STATUS_OK;
}

static void srs_dedupe_reset(SrsDedupeTable *table) {
    if (table == NULL || table->entries == NULL) {
        return;
    }
    memset(table->entries, 0, table->capacity * sizeof(SrsDedupeEntry));
}

static int srs_dedupe_check_and_remember(SrsDedupeTable *table, uint64_t fingerprint, uint64_t seq) {
    size_t mask;
    size_t start;
    size_t free_slot;
    size_t oldest_slot;
    uint64_t oldest_seq;
    size_t probe;

    if (table == NULL || fingerprint == 0u || table->capacity == 0u || table->entries == NULL) {
        return 0;
    }

    mask = table->capacity - 1u;
    start = (size_t) fingerprint & mask;
    free_slot = SIZE_MAX;
    oldest_slot = SIZE_MAX;
    oldest_seq = UINT64_MAX;

    for (probe = 0u; probe < table->capacity; ++probe) {
        size_t slot;
        SrsDedupeEntry *entry;

        slot = (start + probe) & mask;
        entry = &table->entries[slot];

        if (!entry->in_use) {
            free_slot = slot;
            break;
        }

        if (entry->fingerprint == fingerprint) {
            uint64_t age;

            age = seq - entry->sequence;
            entry->sequence = seq;
            if (age <= table->replay_window) {
                return 1;
            }
            return 0;
        }

        if (entry->sequence < oldest_seq) {
            oldest_seq = entry->sequence;
            oldest_slot = slot;
        }

        if ((seq - entry->sequence) > table->replay_window && free_slot == SIZE_MAX) {
            free_slot = slot;
        }
    }

    if (free_slot == SIZE_MAX) {
        free_slot = (oldest_slot != SIZE_MAX) ? oldest_slot : start;
    }

    table->entries[free_slot].in_use = 1;
    table->entries[free_slot].fingerprint = fingerprint;
    table->entries[free_slot].sequence = seq;
    return 0;
}

void srs_config_init(SrsConfig *cfg) {
    if (cfg == NULL) {
        return;
    }
    cfg->max_line_bytes = 16u * 1024u;
    cfg->max_event_type_bytes = 128u;
    cfg->max_event_id_bytes = 1024u;
    cfg->max_data_bytes = 1024u * 1024u;
    cfg->replay_window = 1024u;
    cfg->dedupe_without_event_id = 0;
    cfg->deliver_duplicates = 0;
}

SrsConfig srs_default_config(void) {
    SrsConfig cfg;

    srs_config_init(&cfg);
    return cfg;
}

static void srs_clear_current_event(Srs *parser) {
    if (parser == NULL) {
        return;
    }
    srs_clear_text(parser->event_type_buf, &parser->event_type_len);
    srs_clear_text(parser->event_id_buf, &parser->event_id_len);
    srs_clear_text(parser->data_buf, &parser->data_len);
    parser->event_id_explicit = 0;
}

static void srs_clear_inflight_state(Srs *parser) {
    if (parser == NULL) {
        return;
    }
    srs_clear_text(parser->line_buf, &parser->line_len);
    srs_clear_current_event(parser);
    parser->saw_cr = 0;
}

void srs_note_reconnect(Srs *parser) {
    srs_clear_inflight_state(parser);
}

static int srs_parse_retry_ms(const char *value, size_t value_len, int *out_ms) {
    size_t i;
    uint64_t accum;

    if (value == NULL || out_ms == NULL || value_len == 0u) {
        return 0;
    }

    accum = 0u;
    for (i = 0u; i < value_len; ++i) {
        unsigned char c;

        c = (unsigned char) value[i];
        if (!isdigit(c)) {
            return 0;
        }
        accum = (accum * 10u) + (uint64_t) (c - (unsigned char) '0');
        if (accum > (uint64_t) INT_MAX) {
            return 0;
        }
    }

    *out_ms = (int) accum;
    return 1;
}

static SrsStatus srs_commit_explicit_id(Srs *parser) {
    if (parser == NULL) {
        return SRS_STATUS_BAD_ARGUMENT;
    }
    if (!parser->event_id_explicit) {
        return SRS_STATUS_OK;
    }
    return srs_set_text(
        parser->last_event_id_buf,
        parser->config.max_event_id_bytes,
        &parser->last_event_id_len,
        parser->event_id_buf,
        parser->event_id_len
    );
}

static uint64_t srs_event_fingerprint(
    const Srs *parser,
    const char *event_type,
    size_t event_type_len,
    const char *event_id,
    size_t event_id_len,
    int event_id_is_explicit,
    const char *data,
    size_t data_len
) {
    uint64_t hash;

    if (parser == NULL) {
        return 0u;
    }

    hash = UINT64_C(1469598103934665603);
    if (event_id_is_explicit && event_id_len > 0u) {
        hash = srs_fnv1a64_bytes(hash, "srs:id:", 7u);
        hash = srs_fnv1a64_bytes(hash, event_type, event_type_len);
        hash = srs_fnv1a64_u64(hash, (uint64_t) event_type_len);
        hash = srs_fnv1a64_bytes(hash, event_id, event_id_len);
        hash = srs_fnv1a64_u64(hash, (uint64_t) event_id_len);
        hash = srs_fnv1a64_bytes(hash, data, data_len);
        hash = srs_fnv1a64_u64(hash, (uint64_t) data_len);
        return (hash == 0u) ? 1u : hash;
    }

    if (!parser->config.dedupe_without_event_id) {
        return 0u;
    }

    hash = srs_fnv1a64_bytes(hash, "srs:data:", 9u);
    hash = srs_fnv1a64_bytes(hash, event_type, event_type_len);
    hash = srs_fnv1a64_u64(hash, (uint64_t) event_type_len);
    hash = srs_fnv1a64_bytes(hash, data, data_len);
    hash = srs_fnv1a64_u64(hash, (uint64_t) data_len);
    return (hash == 0u) ? 1u : hash;
}

static SrsStatus srs_dispatch_current_event(Srs *parser, SrsEventCallback cb, void *user) {
    const char *event_type;
    size_t event_type_len;
    const char *event_id;
    size_t event_id_len;
    size_t data_len;
    uint64_t fingerprint;
    uint64_t ordinal;
    int is_duplicate;
    SrsStatus status;
    SrsEvent event;
    int callback_rc;

    if (parser == NULL) {
        return SRS_STATUS_BAD_ARGUMENT;
    }

    status = srs_commit_explicit_id(parser);
    if (status != SRS_STATUS_OK) {
        return status;
    }

    if (parser->data_len == 0u) {
        srs_clear_current_event(parser);
        return SRS_STATUS_OK;
    }

    data_len = parser->data_len;
    if (data_len > 0u && parser->data_buf[data_len - 1u] == '\n') {
        data_len -= 1u;
    }

    event_type = parser->event_type_len != 0u ? parser->event_type_buf : SRS_DEFAULT_EVENT_NAME;
    event_type_len = parser->event_type_len != 0u ? parser->event_type_len : sizeof(SRS_DEFAULT_EVENT_NAME) - 1u;

    event_id = parser->event_id_explicit ? parser->event_id_buf : parser->last_event_id_buf;
    event_id_len = parser->event_id_explicit ? parser->event_id_len : parser->last_event_id_len;

    ordinal = ++parser->event_sequence;
    fingerprint = srs_event_fingerprint(
        parser,
        event_type,
        event_type_len,
        event_id,
        event_id_len,
        parser->event_id_explicit,
        parser->data_buf,
        data_len
    );
    is_duplicate = srs_dedupe_check_and_remember(&parser->dedupe, fingerprint, ordinal);

    parser->stats.events_seen += 1u;

    event.event_type = event_type;
    event.event_type_len = event_type_len;
    event.event_id = event_id;
    event.event_id_len = event_id_len;
    event.event_id_is_explicit = parser->event_id_explicit;
    event.data = parser->data_buf;
    event.data_len = data_len;
    event.ordinal = ordinal;
    event.fingerprint = fingerprint;
    event.is_duplicate = is_duplicate;
    event.reconnect_delay_ms = parser->stats.reconnect_delay_ms;

    if (is_duplicate && !parser->config.deliver_duplicates) {
        parser->stats.duplicates_suppressed += 1u;
        srs_clear_current_event(parser);
        return SRS_STATUS_OK;
    }

    if (cb != NULL) {
        callback_rc = cb(user, &event);
        if (callback_rc != 0) {
            parser->stats.callback_stops += 1u;
            srs_clear_current_event(parser);
            return SRS_STATUS_STOPPED_BY_CALLBACK;
        }
    }

    parser->stats.events_delivered += 1u;
    srs_clear_current_event(parser);
    return SRS_STATUS_OK;
}

static SrsStatus srs_apply_field(Srs *parser, const char *field, size_t field_len, const char *value, size_t value_len) {
    SrsStatus status;
    int retry_ms;

    if (parser == NULL || field == NULL || value == NULL) {
        return SRS_STATUS_BAD_ARGUMENT;
    }

    if (field_len == 4u && memcmp(field, "data", 4u) == 0) {
        status = srs_append_text(
            parser->data_buf,
            parser->config.max_data_bytes,
            &parser->data_len,
            value,
            value_len,
            SRS_STATUS_EVENT_DATA_TOO_LARGE
        );
        if (status != SRS_STATUS_OK) {
            return status;
        }
        return srs_append_text(
            parser->data_buf,
            parser->config.max_data_bytes,
            &parser->data_len,
            "\n",
            1u,
            SRS_STATUS_EVENT_DATA_TOO_LARGE
        );
    }

    if (field_len == 5u && memcmp(field, "event", 5u) == 0) {
        if (value_len > parser->config.max_event_type_bytes) {
            return SRS_STATUS_EVENT_TYPE_TOO_LARGE;
        }
        return srs_set_text(
            parser->event_type_buf,
            parser->config.max_event_type_bytes,
            &parser->event_type_len,
            value,
            value_len
        );
    }

    if (field_len == 2u && memcmp(field, "id", 2u) == 0) {
        if (memchr(value, '\0', value_len) != NULL) {
            return SRS_STATUS_MALFORMED_INPUT;
        }
        if (value_len > parser->config.max_event_id_bytes) {
            return SRS_STATUS_EVENT_ID_TOO_LARGE;
        }
        parser->event_id_explicit = 1;
        return srs_set_text(
            parser->event_id_buf,
            parser->config.max_event_id_bytes,
            &parser->event_id_len,
            value,
            value_len
        );
    }

    if (field_len == 5u && memcmp(field, "retry", 5u) == 0) {
        if (srs_parse_retry_ms(value, value_len, &retry_ms)) {
            parser->stats.reconnect_delay_ms = retry_ms;
        }
        return SRS_STATUS_OK;
    }

    return SRS_STATUS_OK;
}

static SrsStatus srs_process_line(Srs *parser, SrsEventCallback cb, void *user) {
    const char *line;
    size_t line_len;
    const char *colon;
    size_t field_len;
    const char *value;
    size_t value_len;
    SrsStatus status;

    if (parser == NULL) {
        return SRS_STATUS_BAD_ARGUMENT;
    }

    parser->stats.lines_processed += 1u;

    if (parser->line_len == 0u) {
        return srs_dispatch_current_event(parser, cb, user);
    }

    line = parser->line_buf;
    line_len = parser->line_len;

    if (line[0] == ':') {
        parser->line_len = 0u;
        parser->line_buf[0] = '\0';
        return SRS_STATUS_OK;
    }

    colon = (const char *) memchr(line, ':', line_len);
    if (colon == NULL) {
        field_len = line_len;
        value = line + line_len;
        value_len = 0u;
    } else {
        field_len = (size_t) (colon - line);
        value = colon + 1;
        value_len = line_len - field_len - 1u;
        if (value_len != 0u && value[0] == ' ') {
            value += 1;
            value_len -= 1u;
        }
    }

    status = srs_apply_field(parser, line, field_len, value, value_len);
    parser->line_len = 0u;
    parser->line_buf[0] = '\0';
    return status;
}

SrsStatus srs_init(Srs *parser, const SrsConfig *cfg) {
    SrsStatus status;

    if (parser == NULL) {
        return SRS_STATUS_BAD_ARGUMENT;
    }

    memset(parser, 0, sizeof(*parser));
    parser->config = (cfg != NULL) ? *cfg : srs_default_config();

    if (parser->config.max_line_bytes == 0u ||
        parser->config.max_event_type_bytes == 0u ||
        parser->config.max_event_id_bytes == 0u ||
        parser->config.max_data_bytes == 0u) {
        return SRS_STATUS_BAD_ARGUMENT;
    }

    status = srs_alloc_text_buffer(&parser->line_buf, parser->config.max_line_bytes);
    if (status != SRS_STATUS_OK) {
        goto fail;
    }

    status = srs_alloc_text_buffer(&parser->event_type_buf, parser->config.max_event_type_bytes);
    if (status != SRS_STATUS_OK) {
        goto fail;
    }

    status = srs_alloc_text_buffer(&parser->event_id_buf, parser->config.max_event_id_bytes);
    if (status != SRS_STATUS_OK) {
        goto fail;
    }

    status = srs_alloc_text_buffer(&parser->last_event_id_buf, parser->config.max_event_id_bytes);
    if (status != SRS_STATUS_OK) {
        goto fail;
    }

    status = srs_alloc_text_buffer(&parser->data_buf, parser->config.max_data_bytes);
    if (status != SRS_STATUS_OK) {
        goto fail;
    }

    status = srs_dedupe_init(&parser->dedupe, parser->config.replay_window);
    if (status != SRS_STATUS_OK) {
        goto fail;
    }

    parser->stats.reconnect_delay_ms = -1;
    srs_clear_inflight_state(parser);
    srs_clear_text(parser->last_event_id_buf, &parser->last_event_id_len);
    return SRS_STATUS_OK;

fail:
    free(parser->line_buf);
    free(parser->event_type_buf);
    free(parser->event_id_buf);
    free(parser->last_event_id_buf);
    free(parser->data_buf);
    srs_dedupe_destroy(&parser->dedupe);
    memset(parser, 0, sizeof(*parser));
    return status;
}

void srs_reset(Srs *parser) {
    if (parser == NULL) {
        return;
    }
    srs_clear_inflight_state(parser);
    srs_clear_text(parser->last_event_id_buf, &parser->last_event_id_len);
    parser->stats = (SrsStats) {0};
    parser->stats.reconnect_delay_ms = -1;
    parser->event_sequence = 0u;
    srs_dedupe_reset(&parser->dedupe);
}

void srs_destroy(Srs *parser) {
    if (parser == NULL) {
        return;
    }
    free(parser->line_buf);
    free(parser->event_type_buf);
    free(parser->event_id_buf);
    free(parser->last_event_id_buf);
    free(parser->data_buf);
    parser->line_buf = NULL;
    parser->event_type_buf = NULL;
    parser->event_id_buf = NULL;
    parser->last_event_id_buf = NULL;
    parser->data_buf = NULL;
    srs_dedupe_destroy(&parser->dedupe);
    memset(parser, 0, sizeof(*parser));
}

const SrsStats *srs_stats(const Srs *parser) {
    if (parser == NULL) {
        return NULL;
    }
    return &parser->stats;
}

const char *srs_last_event_id(const Srs *parser) {
    if (parser == NULL || parser->last_event_id_buf == NULL) {
        return NULL;
    }
    return parser->last_event_id_buf;
}

size_t srs_last_event_id_len(const Srs *parser) {
    if (parser == NULL) {
        return 0u;
    }
    return parser->last_event_id_len;
}

size_t srs_format_last_event_id_header(const Srs *parser, char *dst, size_t cap) {
    size_t prefix_len;
    size_t required;

    if (parser == NULL || parser->last_event_id_len == 0u) {
        return 0u;
    }

    prefix_len = sizeof(SRS_LAST_EVENT_ID_PREFIX) - 1u;
    required = prefix_len + parser->last_event_id_len + 2u;

    if (dst != NULL && cap > required) {
        memcpy(dst, SRS_LAST_EVENT_ID_PREFIX, prefix_len);
        memcpy(dst + prefix_len, parser->last_event_id_buf, parser->last_event_id_len);
        dst[prefix_len + parser->last_event_id_len] = '\r';
        dst[prefix_len + parser->last_event_id_len + 1u] = '\n';
        dst[required] = '\0';
    }

    return required;
}

SrsStatus srs_feed(Srs *parser, const void *chunk, size_t chunk_len, SrsEventCallback cb, void *user) {
    const unsigned char *bytes;
    size_t i;

    if (parser == NULL || (chunk == NULL && chunk_len != 0u)) {
        return SRS_STATUS_BAD_ARGUMENT;
    }

    bytes = (const unsigned char *) chunk;
    for (i = 0u; i < chunk_len; ++i) {
        unsigned char byte;
        SrsStatus status;

        byte = bytes[i];
        parser->stats.bytes_processed += 1u;

        if (byte == '\0') {
            return SRS_STATUS_MALFORMED_INPUT;
        }

        if (parser->saw_cr) {
            parser->saw_cr = 0;
            if (byte == '\n') {
                continue;
            }
        }

        if (byte == '\r') {
            status = srs_process_line(parser, cb, user);
            if (status != SRS_STATUS_OK) {
                return status;
            }
            parser->saw_cr = 1;
            continue;
        }

        if (byte == '\n') {
            status = srs_process_line(parser, cb, user);
            if (status != SRS_STATUS_OK) {
                return status;
            }
            continue;
        }

        if (parser->line_len == parser->config.max_line_bytes) {
            return SRS_STATUS_LINE_TOO_LARGE;
        }

        parser->line_buf[parser->line_len] = (char) byte;
        parser->line_len += 1u;
        parser->line_buf[parser->line_len] = '\0';
    }

    return SRS_STATUS_OK;
}

const char *srs_status_string(SrsStatus status) {
    switch (status) {
        case SRS_STATUS_OK:
            return "ok";
        case SRS_STATUS_BAD_ARGUMENT:
            return "bad argument";
        case SRS_STATUS_OUT_OF_MEMORY:
            return "out of memory";
        case SRS_STATUS_LINE_TOO_LARGE:
            return "line too large";
        case SRS_STATUS_EVENT_TYPE_TOO_LARGE:
            return "event type too large";
        case SRS_STATUS_EVENT_ID_TOO_LARGE:
            return "event id too large";
        case SRS_STATUS_EVENT_DATA_TOO_LARGE:
            return "event data too large";
        case SRS_STATUS_MALFORMED_INPUT:
            return "malformed input";
        case SRS_STATUS_STOPPED_BY_CALLBACK:
            return "stopped by callback";
        default:
            return "unknown";
    }
}

#ifdef SSE_REPLAY_SHIELD_SELF_TEST

#include <assert.h>

typedef struct TestSink {
    int count;
    int duplicates;
    char last_event_type[128];
    char last_event_id[128];
    char last_data[512];
    size_t last_data_len;
    uint64_t last_fingerprint;
} TestSink;

static void test_copy(char *dst, size_t cap, const char *src, size_t len) {
    size_t n;

    assert(dst != NULL);
    assert(cap > 0u);

    n = (len < (cap - 1u)) ? len : (cap - 1u);
    if (n != 0u) {
        memcpy(dst, src, n);
    }
    dst[n] = '\0';
}

static int test_sink_cb(void *user, const SrsEvent *event) {
    TestSink *sink;

    sink = (TestSink *) user;
    sink->count += 1;
    sink->duplicates += event->is_duplicate ? 1 : 0;
    test_copy(sink->last_event_type, sizeof(sink->last_event_type), event->event_type, event->event_type_len);
    test_copy(sink->last_event_id, sizeof(sink->last_event_id), event->event_id, event->event_id_len);
    test_copy(sink->last_data, sizeof(sink->last_data), event->data, event->data_len);
    sink->last_data_len = event->data_len;
    sink->last_fingerprint = event->fingerprint;
    return 0;
}

static void test_multiline_and_crlf(void) {
    Srs parser;
    SrsConfig cfg;
    TestSink sink;
    SrsStatus status;
    char header[256];
    const char *chunk1;
    const char *chunk2;

    cfg = srs_default_config();
    status = srs_init(&parser, &cfg);
    assert(status == SRS_STATUS_OK);

    memset(&sink, 0, sizeof(sink));

    chunk1 = "event: response.output_text.delta\r\ndata: hel";
    chunk2 = "lo\r\nid: evt-42\r\n\r\n";
    status = srs_feed(&parser, chunk1, strlen(chunk1), test_sink_cb, &sink);
    assert(status == SRS_STATUS_OK);
    status = srs_feed(&parser, chunk2, strlen(chunk2), test_sink_cb, &sink);
    assert(status == SRS_STATUS_OK);

    assert(sink.count == 1);
    assert(strcmp(sink.last_event_type, "response.output_text.delta") == 0);
    assert(strcmp(sink.last_event_id, "evt-42") == 0);
    assert(strcmp(sink.last_data, "hello") == 0);
    assert(srs_last_event_id_len(&parser) == 6u);
    assert(strcmp(srs_last_event_id(&parser), "evt-42") == 0);
    assert(srs_format_last_event_id_header(&parser, header, sizeof(header)) == strlen("Last-Event-ID: evt-42\r\n"));
    assert(strcmp(header, "Last-Event-ID: evt-42\r\n") == 0);

    srs_destroy(&parser);
}

static void test_duplicate_suppression_with_explicit_ids(void) {
    Srs parser;
    SrsConfig cfg;
    TestSink sink;
    SrsStatus status;
    const SrsStats *stats;
    const char *payload;

    cfg = srs_default_config();
    status = srs_init(&parser, &cfg);
    assert(status == SRS_STATUS_OK);

    memset(&sink, 0, sizeof(sink));
    payload = "id: chunk-9\ndata: abc\n\nid: chunk-9\ndata: abc\n\n";
    status = srs_feed(&parser, payload, strlen(payload), test_sink_cb, &sink);
    assert(status == SRS_STATUS_OK);

    stats = srs_stats(&parser);
    assert(sink.count == 1);
    assert(stats != NULL);
    assert(stats->events_seen == 2u);
    assert(stats->events_delivered == 1u);
    assert(stats->duplicates_suppressed == 1u);

    srs_destroy(&parser);
}

static void test_no_id_duplicates_are_not_suppressed_by_default(void) {
    Srs parser;
    SrsConfig cfg;
    TestSink sink;
    SrsStatus status;
    const char *payload;

    cfg = srs_default_config();
    status = srs_init(&parser, &cfg);
    assert(status == SRS_STATUS_OK);

    memset(&sink, 0, sizeof(sink));
    payload = "data: x\n\ndata: x\n\n";
    status = srs_feed(&parser, payload, strlen(payload), test_sink_cb, &sink);
    assert(status == SRS_STATUS_OK);
    assert(sink.count == 2);

    srs_destroy(&parser);
}

static void test_payload_fallback_after_reconnect(void) {
    Srs parser;
    SrsConfig cfg;
    TestSink sink;
    SrsStatus status;
    const SrsStats *stats;
    const char *payload;

    cfg = srs_default_config();
    cfg.dedupe_without_event_id = 1;
    status = srs_init(&parser, &cfg);
    assert(status == SRS_STATUS_OK);

    memset(&sink, 0, sizeof(sink));
    payload = "event: delta\ndata: token\n\n";
    status = srs_feed(&parser, payload, strlen(payload), test_sink_cb, &sink);
    assert(status == SRS_STATUS_OK);
    srs_note_reconnect(&parser);
    status = srs_feed(&parser, payload, strlen(payload), test_sink_cb, &sink);
    assert(status == SRS_STATUS_OK);

    stats = srs_stats(&parser);
    assert(sink.count == 1);
    assert(stats != NULL);
    assert(stats->duplicates_suppressed == 1u);

    srs_destroy(&parser);
}

int main(void) {
    test_multiline_and_crlf();
    test_duplicate_suppression_with_explicit_ids();
    test_no_id_duplicates_are_not_suppressed_by_default();
    test_payload_fallback_after_reconnect();
    puts("SseReplayShield self-test passed");
    return 0;
}

#endif

/*
This solves duplicate Server-Sent Events, broken reconnect handling, and partial stream framing bugs in AI gateways, OpenAI-compatible proxies, Anthropic stream relays, SSE reverse proxies, edge sidecars, and terminal token stream clients. Built because in April 2026 a lot of LLM products still reconnect mid-stream and accidentally append the same delta twice or replay the same tool-call chunk after a timeout. Use it when your C service needs incremental `text/event-stream` parsing, `Last-Event-ID` tracking, CRLF or LF tolerance, and replay suppression without pulling in a framework. The trick: it follows the SSE field rules exactly, keeps a bounded replay window, and only suppresses low-risk duplicates by default unless you explicitly enable payload fallback. Drop this into an inference gateway, local model proxy, MCP bridge, observability tap, edge worker sidecar, or CLI streaming SDK when you need a single-file C SSE parser for AI streaming reliability, duplicate token suppression, and safe reconnect behavior.
*/