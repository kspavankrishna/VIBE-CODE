#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

typedef enum {
  SSE_DECODER_OK = 0,
  SSE_DECODER_INVALID_ARGUMENT = 1,
  SSE_DECODER_OUT_OF_MEMORY = 2,
  SSE_DECODER_LINE_TOO_LONG = 3,
  SSE_DECODER_EVENT_TOO_LARGE = 4,
  SSE_DECODER_INTEGER_OVERFLOW = 5,
  SSE_DECODER_CALLBACK_ABORTED = 6
} SseDecoderStatus;

typedef struct {
  size_t max_line_bytes;
  size_t max_event_bytes;
  bool dispatch_empty_events;
  bool strip_utf8_bom;
} SseDecoderConfig;

typedef struct {
  const char *event_type;
  size_t event_type_len;
  const char *data;
  size_t data_len;
  const char *last_event_id;
  size_t last_event_id_len;
  uint64_t reconnect_delay_ms;
  bool has_reconnect_delay;
  bool retry_updated;
  uint64_t sequence;
} SseEventView;

typedef bool (*SseDecoderOnEvent)(void *user_data, const SseEventView *event);

typedef struct {
  char *data;
  size_t len;
  size_t cap;
} SseBuffer;

typedef struct {
  SseDecoderConfig config;
  SseBuffer line;
  SseBuffer data;
  SseBuffer event_type;
  SseBuffer last_event_id;
  bool skip_next_lf;
  bool saw_first_line;
  bool saw_any_field_in_block;
  bool retry_updated_in_block;
  bool has_reconnect_delay;
  uint64_t reconnect_delay_ms;
  uint64_t bytes_seen;
  uint64_t events_seen;
} SseDecoder;

SseDecoderConfig sse_decoder_config_default(void);
const char *sse_decoder_status_string(SseDecoderStatus status);
SseDecoderStatus sse_decoder_init(SseDecoder *decoder, const SseDecoderConfig *config);
void sse_decoder_reset(SseDecoder *decoder);
void sse_decoder_destroy(SseDecoder *decoder);
uint64_t sse_decoder_bytes_seen(const SseDecoder *decoder);
uint64_t sse_decoder_event_count(const SseDecoder *decoder);
bool sse_decoder_get_reconnect_delay(const SseDecoder *decoder, uint64_t *value_out);
const char *sse_decoder_last_event_id(const SseDecoder *decoder, size_t *len_out);
SseDecoderStatus sse_decoder_feed(
    SseDecoder *decoder,
    const void *chunk,
    size_t chunk_len,
    SseDecoderOnEvent on_event,
    void *user_data,
    size_t *events_emitted);
SseDecoderStatus sse_decoder_finish(
    SseDecoder *decoder,
    SseDecoderOnEvent on_event,
    void *user_data,
    size_t *events_emitted);
bool sse_event_is_done_sentinel(const SseEventView *event);

static const char SSE_DEFAULT_EVENT_TYPE[] = "message";
static const unsigned char SSE_UTF8_BOM[] = {0xEFu, 0xBBu, 0xBFu};

static bool sse_try_add_size(size_t a, size_t b, size_t *out) {
  if (a > SIZE_MAX - b) {
    return false;
  }
  *out = a + b;
  return true;
}

static void sse_buffer_init(SseBuffer *buffer) {
  buffer->data = NULL;
  buffer->len = 0u;
  buffer->cap = 0u;
}

static void sse_buffer_clear(SseBuffer *buffer) {
  buffer->len = 0u;
  if (buffer->data != NULL) {
    buffer->data[0] = '\0';
  }
}

static void sse_buffer_destroy(SseBuffer *buffer) {
  free(buffer->data);
  buffer->data = NULL;
  buffer->len = 0u;
  buffer->cap = 0u;
}

static SseDecoderStatus sse_buffer_reserve(SseBuffer *buffer, size_t payload_bytes) {
  size_t required;
  if (!sse_try_add_size(payload_bytes, 1u, &required)) {
    return SSE_DECODER_INTEGER_OVERFLOW;
  }
  if (buffer->cap >= required) {
    return SSE_DECODER_OK;
  }

  size_t next = buffer->cap == 0u ? 64u : buffer->cap;
  while (next < required) {
    if (next > SIZE_MAX / 2u) {
      next = required;
      break;
    }
    next *= 2u;
  }

  char *grown = (char *)realloc(buffer->data, next);
  if (grown == NULL) {
    return SSE_DECODER_OUT_OF_MEMORY;
  }

  buffer->data = grown;
  buffer->cap = next;
  if (buffer->len == 0u) {
    buffer->data[0] = '\0';
  }
  return SSE_DECODER_OK;
}

static SseDecoderStatus sse_buffer_assign(SseBuffer *buffer, const char *data, size_t len) {
  if (len != 0u && data == NULL) {
    return SSE_DECODER_INVALID_ARGUMENT;
  }
  SseDecoderStatus status = sse_buffer_reserve(buffer, len);
  if (status != SSE_DECODER_OK) {
    return status;
  }
  if (len != 0u) {
    memmove(buffer->data, data, len);
  }
  buffer->len = len;
  buffer->data[len] = '\0';
  return SSE_DECODER_OK;
}

static SseDecoderStatus sse_buffer_append(SseBuffer *buffer, const char *data, size_t len) {
  if (len == 0u) {
    return SSE_DECODER_OK;
  }
  if (data == NULL) {
    return SSE_DECODER_INVALID_ARGUMENT;
  }

  size_t next_len;
  if (!sse_try_add_size(buffer->len, len, &next_len)) {
    return SSE_DECODER_INTEGER_OVERFLOW;
  }

  SseDecoderStatus status = sse_buffer_reserve(buffer, next_len);
  if (status != SSE_DECODER_OK) {
    return status;
  }

  memmove(buffer->data + buffer->len, data, len);
  buffer->len = next_len;
  buffer->data[buffer->len] = '\0';
  return SSE_DECODER_OK;
}

static SseDecoderStatus sse_buffer_append_char(SseBuffer *buffer, char value) {
  return sse_buffer_append(buffer, &value, 1u);
}

static void sse_buffer_strip_prefix(SseBuffer *buffer, size_t count) {
  if (count == 0u || count > buffer->len) {
    return;
  }
  memmove(buffer->data, buffer->data + count, buffer->len - count);
  buffer->len -= count;
  buffer->data[buffer->len] = '\0';
}

static bool sse_bytes_equal(const char *lhs, size_t lhs_len, const char *rhs, size_t rhs_len) {
  if (lhs_len != rhs_len) {
    return false;
  }
  if (lhs_len == 0u) {
    return true;
  }
  return memcmp(lhs, rhs, lhs_len) == 0;
}

static bool sse_slice_contains_nul(const char *data, size_t len) {
  return len != 0u && memchr(data, '\0', len) != NULL;
}

static SseDecoderStatus sse_check_block_size(
    const SseDecoder *decoder,
    size_t data_len,
    size_t event_type_len,
    size_t last_event_id_len) {
  size_t total;
  if (!sse_try_add_size(data_len, event_type_len, &total)) {
    return SSE_DECODER_INTEGER_OVERFLOW;
  }
  if (!sse_try_add_size(total, last_event_id_len, &total)) {
    return SSE_DECODER_INTEGER_OVERFLOW;
  }
  if (total > decoder->config.max_event_bytes) {
    return SSE_DECODER_EVENT_TOO_LARGE;
  }
  return SSE_DECODER_OK;
}

static bool sse_parse_retry_ms(const char *data, size_t len, uint64_t *value_out) {
  if (len == 0u) {
    return false;
  }

  uint64_t value = 0u;
  for (size_t i = 0u; i < len; ++i) {
    unsigned char ch = (unsigned char)data[i];
    if (ch < '0' || ch > '9') {
      return false;
    }
    uint64_t digit = (uint64_t)(ch - (unsigned char)'0');
    if (value > (UINT64_MAX - digit) / 10u) {
      return false;
    }
    value = value * 10u + digit;
  }

  *value_out = value;
  return true;
}

static void sse_reset_current_block(SseDecoder *decoder) {
  sse_buffer_clear(&decoder->data);
  sse_buffer_clear(&decoder->event_type);
  decoder->saw_any_field_in_block = false;
  decoder->retry_updated_in_block = false;
}

static SseDecoderStatus sse_append_data_line(SseDecoder *decoder, const char *value, size_t value_len) {
  size_t next_data_len;
  if (!sse_try_add_size(decoder->data.len, value_len, &next_data_len)) {
    return SSE_DECODER_INTEGER_OVERFLOW;
  }
  if (!sse_try_add_size(next_data_len, 1u, &next_data_len)) {
    return SSE_DECODER_INTEGER_OVERFLOW;
  }

  SseDecoderStatus status =
      sse_check_block_size(decoder, next_data_len, decoder->event_type.len, decoder->last_event_id.len);
  if (status != SSE_DECODER_OK) {
    return status;
  }

  status = sse_buffer_append(&decoder->data, value, value_len);
  if (status != SSE_DECODER_OK) {
    return status;
  }
  status = sse_buffer_append_char(&decoder->data, '\n');
  if (status != SSE_DECODER_OK) {
    return status;
  }

  decoder->saw_any_field_in_block = true;
  return SSE_DECODER_OK;
}

static SseDecoderStatus sse_set_event_type(SseDecoder *decoder, const char *value, size_t value_len) {
  SseDecoderStatus status =
      sse_check_block_size(decoder, decoder->data.len, value_len, decoder->last_event_id.len);
  if (status != SSE_DECODER_OK) {
    return status;
  }

  status = sse_buffer_assign(&decoder->event_type, value, value_len);
  if (status != SSE_DECODER_OK) {
    return status;
  }

  decoder->saw_any_field_in_block = true;
  return SSE_DECODER_OK;
}

static SseDecoderStatus sse_set_last_event_id(SseDecoder *decoder, const char *value, size_t value_len) {
  if (sse_slice_contains_nul(value, value_len)) {
    return SSE_DECODER_OK;
  }

  SseDecoderStatus status =
      sse_check_block_size(decoder, decoder->data.len, decoder->event_type.len, value_len);
  if (status != SSE_DECODER_OK) {
    return status;
  }

  status = sse_buffer_assign(&decoder->last_event_id, value, value_len);
  if (status != SSE_DECODER_OK) {
    return status;
  }

  decoder->saw_any_field_in_block = true;
  return SSE_DECODER_OK;
}

static SseDecoderStatus sse_maybe_set_retry(SseDecoder *decoder, const char *value, size_t value_len) {
  uint64_t retry_ms = 0u;
  if (!sse_parse_retry_ms(value, value_len, &retry_ms)) {
    return SSE_DECODER_OK;
  }

  decoder->reconnect_delay_ms = retry_ms;
  decoder->has_reconnect_delay = true;
  decoder->retry_updated_in_block = true;
  decoder->saw_any_field_in_block = true;
  return SSE_DECODER_OK;
}

static SseDecoderStatus sse_dispatch_event(
    SseDecoder *decoder,
    SseDecoderOnEvent on_event,
    void *user_data,
    size_t *events_emitted) {
  bool should_dispatch =
      decoder->data.len != 0u || (decoder->config.dispatch_empty_events && decoder->saw_any_field_in_block);

  if (!should_dispatch) {
    sse_reset_current_block(decoder);
    return SSE_DECODER_OK;
  }

  const char *event_type = decoder->event_type.len != 0u ? decoder->event_type.data : SSE_DEFAULT_EVENT_TYPE;
  size_t event_type_len =
      decoder->event_type.len != 0u ? decoder->event_type.len : (sizeof(SSE_DEFAULT_EVENT_TYPE) - 1u);

  const char *data = "";
  size_t data_len = decoder->data.len;
  char saved_terminator = '\0';
  if (decoder->data.data != NULL) {
    if (data_len != 0u && decoder->data.data[data_len - 1u] == '\n') {
      data_len -= 1u;
    }
    saved_terminator = decoder->data.data[data_len];
    decoder->data.data[data_len] = '\0';
    data = decoder->data.data;
  }

  const char *last_event_id = decoder->last_event_id.data != NULL ? decoder->last_event_id.data : "";
  size_t last_event_id_len = decoder->last_event_id.len;

  SseEventView event = {
      .event_type = event_type,
      .event_type_len = event_type_len,
      .data = data,
      .data_len = data_len,
      .last_event_id = last_event_id,
      .last_event_id_len = last_event_id_len,
      .reconnect_delay_ms = decoder->reconnect_delay_ms,
      .has_reconnect_delay = decoder->has_reconnect_delay,
      .retry_updated = decoder->retry_updated_in_block,
      .sequence = decoder->events_seen + 1u,
  };

  bool keep_going = true;
  if (on_event != NULL) {
    keep_going = on_event(user_data, &event);
  }

  if (decoder->data.data != NULL) {
    decoder->data.data[data_len] = saved_terminator;
  }

  if (!keep_going) {
    return SSE_DECODER_CALLBACK_ABORTED;
  }

  decoder->events_seen += 1u;
  if (events_emitted != NULL) {
    *events_emitted += 1u;
  }
  sse_reset_current_block(decoder);
  return SSE_DECODER_OK;
}

static SseDecoderStatus sse_process_completed_line(
    SseDecoder *decoder,
    SseDecoderOnEvent on_event,
    void *user_data,
    size_t *events_emitted) {
  if (!decoder->saw_first_line) {
    decoder->saw_first_line = true;
    if (decoder->config.strip_utf8_bom &&
        decoder->line.len >= sizeof(SSE_UTF8_BOM) &&
        memcmp(decoder->line.data, SSE_UTF8_BOM, sizeof(SSE_UTF8_BOM)) == 0) {
      sse_buffer_strip_prefix(&decoder->line, sizeof(SSE_UTF8_BOM));
    }
  }

  if (decoder->line.len == 0u) {
    return sse_dispatch_event(decoder, on_event, user_data, events_emitted);
  }

  if (decoder->line.data[0] == ':') {
    return SSE_DECODER_OK;
  }

  size_t field_len = decoder->line.len;
  size_t value_len = 0u;
  const char *field = decoder->line.data;
  const char *value = "";

  for (size_t i = 0u; i < decoder->line.len; ++i) {
    if (decoder->line.data[i] == ':') {
      field_len = i;
      value = decoder->line.data + i + 1u;
      value_len = decoder->line.len - i - 1u;
      if (value_len != 0u && value[0] == ' ') {
        value += 1u;
        value_len -= 1u;
      }
      break;
    }
  }

  if (sse_bytes_equal(field, field_len, "data", 4u)) {
    return sse_append_data_line(decoder, value, value_len);
  }
  if (sse_bytes_equal(field, field_len, "event", 5u)) {
    return sse_set_event_type(decoder, value, value_len);
  }
  if (sse_bytes_equal(field, field_len, "id", 2u)) {
    return sse_set_last_event_id(decoder, value, value_len);
  }
  if (sse_bytes_equal(field, field_len, "retry", 5u)) {
    return sse_maybe_set_retry(decoder, value, value_len);
  }

  return SSE_DECODER_OK;
}

SseDecoderConfig sse_decoder_config_default(void) {
  SseDecoderConfig config;
  config.max_line_bytes = 16u * 1024u;
  config.max_event_bytes = 4u * 1024u * 1024u;
  config.dispatch_empty_events = false;
  config.strip_utf8_bom = true;
  return config;
}

const char *sse_decoder_status_string(SseDecoderStatus status) {
  switch (status) {
    case SSE_DECODER_OK:
      return "ok";
    case SSE_DECODER_INVALID_ARGUMENT:
      return "invalid argument";
    case SSE_DECODER_OUT_OF_MEMORY:
      return "out of memory";
    case SSE_DECODER_LINE_TOO_LONG:
      return "line too long";
    case SSE_DECODER_EVENT_TOO_LARGE:
      return "event too large";
    case SSE_DECODER_INTEGER_OVERFLOW:
      return "integer overflow";
    case SSE_DECODER_CALLBACK_ABORTED:
      return "callback aborted";
    default:
      return "unknown";
  }
}

SseDecoderStatus sse_decoder_init(SseDecoder *decoder, const SseDecoderConfig *config) {
  if (decoder == NULL) {
    return SSE_DECODER_INVALID_ARGUMENT;
  }

  SseDecoderConfig resolved = config != NULL ? *config : sse_decoder_config_default();
  if (resolved.max_line_bytes == 0u || resolved.max_event_bytes == 0u) {
    return SSE_DECODER_INVALID_ARGUMENT;
  }

  decoder->config = resolved;
  sse_buffer_init(&decoder->line);
  sse_buffer_init(&decoder->data);
  sse_buffer_init(&decoder->event_type);
  sse_buffer_init(&decoder->last_event_id);
  decoder->skip_next_lf = false;
  decoder->saw_first_line = false;
  decoder->saw_any_field_in_block = false;
  decoder->retry_updated_in_block = false;
  decoder->has_reconnect_delay = false;
  decoder->reconnect_delay_ms = 0u;
  decoder->bytes_seen = 0u;
  decoder->events_seen = 0u;

  SseDecoderStatus status = sse_buffer_reserve(&decoder->line, 256u);
  if (status != SSE_DECODER_OK) {
    sse_decoder_destroy(decoder);
    return status;
  }
  status = sse_buffer_reserve(&decoder->data, 1024u);
  if (status != SSE_DECODER_OK) {
    sse_decoder_destroy(decoder);
    return status;
  }
  status = sse_buffer_reserve(&decoder->event_type, 64u);
  if (status != SSE_DECODER_OK) {
    sse_decoder_destroy(decoder);
    return status;
  }
  status = sse_buffer_reserve(&decoder->last_event_id, 64u);
  if (status != SSE_DECODER_OK) {
    sse_decoder_destroy(decoder);
    return status;
  }

  return SSE_DECODER_OK;
}

void sse_decoder_reset(SseDecoder *decoder) {
  if (decoder == NULL) {
    return;
  }

  sse_buffer_clear(&decoder->line);
  sse_buffer_clear(&decoder->data);
  sse_buffer_clear(&decoder->event_type);
  sse_buffer_clear(&decoder->last_event_id);
  decoder->skip_next_lf = false;
  decoder->saw_first_line = false;
  decoder->saw_any_field_in_block = false;
  decoder->retry_updated_in_block = false;
  decoder->has_reconnect_delay = false;
  decoder->reconnect_delay_ms = 0u;
  decoder->bytes_seen = 0u;
  decoder->events_seen = 0u;
}

void sse_decoder_destroy(SseDecoder *decoder) {
  if (decoder == NULL) {
    return;
  }

  sse_buffer_destroy(&decoder->line);
  sse_buffer_destroy(&decoder->data);
  sse_buffer_destroy(&decoder->event_type);
  sse_buffer_destroy(&decoder->last_event_id);
  decoder->skip_next_lf = false;
  decoder->saw_first_line = false;
  decoder->saw_any_field_in_block = false;
  decoder->retry_updated_in_block = false;
  decoder->has_reconnect_delay = false;
  decoder->reconnect_delay_ms = 0u;
  decoder->bytes_seen = 0u;
  decoder->events_seen = 0u;
}

uint64_t sse_decoder_bytes_seen(const SseDecoder *decoder) {
  return decoder != NULL ? decoder->bytes_seen : 0u;
}

uint64_t sse_decoder_event_count(const SseDecoder *decoder) {
  return decoder != NULL ? decoder->events_seen : 0u;
}

bool sse_decoder_get_reconnect_delay(const SseDecoder *decoder, uint64_t *value_out) {
  if (decoder == NULL) {
    return false;
  }
  if (decoder->has_reconnect_delay && value_out != NULL) {
    *value_out = decoder->reconnect_delay_ms;
  }
  return decoder->has_reconnect_delay;
}

const char *sse_decoder_last_event_id(const SseDecoder *decoder, size_t *len_out) {
  if (decoder == NULL) {
    if (len_out != NULL) {
      *len_out = 0u;
    }
    return "";
  }

  if (len_out != NULL) {
    *len_out = decoder->last_event_id.len;
  }
  return decoder->last_event_id.data != NULL ? decoder->last_event_id.data : "";
}

SseDecoderStatus sse_decoder_feed(
    SseDecoder *decoder,
    const void *chunk,
    size_t chunk_len,
    SseDecoderOnEvent on_event,
    void *user_data,
    size_t *events_emitted) {
  if (decoder == NULL || (chunk == NULL && chunk_len != 0u)) {
    return SSE_DECODER_INVALID_ARGUMENT;
  }
  if (events_emitted != NULL) {
    *events_emitted = 0u;
  }
  if (chunk_len == 0u) {
    return SSE_DECODER_OK;
  }

  const unsigned char *bytes = (const unsigned char *)chunk;
  for (size_t i = 0u; i < chunk_len; ++i) {
    unsigned char byte = bytes[i];
    decoder->bytes_seen += 1u;

    if (decoder->skip_next_lf) {
      decoder->skip_next_lf = false;
      if (byte == '\n') {
        continue;
      }
    }

    if (byte == '\r') {
      SseDecoderStatus status = sse_process_completed_line(decoder, on_event, user_data, events_emitted);
      sse_buffer_clear(&decoder->line);
      if (status != SSE_DECODER_OK) {
        return status;
      }
      decoder->skip_next_lf = true;
      continue;
    }

    if (byte == '\n') {
      SseDecoderStatus status = sse_process_completed_line(decoder, on_event, user_data, events_emitted);
      sse_buffer_clear(&decoder->line);
      if (status != SSE_DECODER_OK) {
        return status;
      }
      continue;
    }

    if (decoder->line.len == decoder->config.max_line_bytes) {
      return SSE_DECODER_LINE_TOO_LONG;
    }

    SseDecoderStatus status = sse_buffer_append_char(&decoder->line, (char)byte);
    if (status != SSE_DECODER_OK) {
      return status;
    }
  }

  return SSE_DECODER_OK;
}

SseDecoderStatus sse_decoder_finish(
    SseDecoder *decoder,
    SseDecoderOnEvent on_event,
    void *user_data,
    size_t *events_emitted) {
  if (decoder == NULL) {
    return SSE_DECODER_INVALID_ARGUMENT;
  }
  if (events_emitted != NULL) {
    *events_emitted = 0u;
  }

  decoder->skip_next_lf = false;

  if (decoder->line.len != 0u) {
    SseDecoderStatus status = sse_process_completed_line(decoder, on_event, user_data, events_emitted);
    sse_buffer_clear(&decoder->line);
    if (status != SSE_DECODER_OK) {
      return status;
    }
  }

  return sse_dispatch_event(decoder, on_event, user_data, events_emitted);
}

bool sse_event_is_done_sentinel(const SseEventView *event) {
  return event != NULL && sse_bytes_equal(event->data, event->data_len, "[DONE]", 6u);
}

/*
This solves incremental Server-Sent Events parsing in C for AI streaming APIs, reverse proxies, edge gateways, CLI tools, and embedded runtimes that consume OpenAI, Anthropic, Gemini, or any normal SSE feed. Built because the bug that keeps wasting time in 2026 is still the same one: split lines, CRLF weirdness, reconnect metadata, and proxy buffering that passes tests but breaks in production. Use it when you need a small C SSE parser, a C Server-Sent Events decoder, or a reliable Last-Event-ID and retry handler inside a daemon, sidecar, data plane process, NGINX module, libcurl callback, or WASI binary. The trick: it keeps only the current line and current event state, follows the SSE field rules, carries last-event-id correctly across events, handles UTF-8 BOM, comments, empty data lines, and the common [DONE] sentinel without needing a framework. Drop this into a low-level networking project, feed bytes from your socket or HTTP client into `sse_decoder_feed`, call `sse_decoder_finish` on EOF, and pass each emitted event straight into your JSON parser or stream multiplexer.
*/
