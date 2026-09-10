# SSE Stream Decoder

Server-Sent Events arrive in arbitrary chunks, so a single `data:` line routinely gets split across two TCP reads. This is a single file C decoder that eats those chunks byte by byte and emits complete SSE events, with no allocator surprises and no assumption that a read boundary is a line boundary.

**Language:** C | **Lines:** 700 | **Added:** 2026-04-17

## What this solves

Incremental Server-Sent Events parsing in C for AI streaming APIs, reverse proxies, edge gateways, CLI tools and embedded runtimes that consume OpenAI, Anthropic, Gemini or any normal SSE feed. The bugs that keep wasting time are the same three: split lines, CRLF weirdness and unbounded buffers that all pass tests but break in production.

The first failure looks like this. You read from a socket, split on `\n` and treat each piece as a line. It works locally because the test server writes one event per `write()` call. Then a CDN gets in front of it and a chunk boundary lands in the middle of `data: {"choices":[{"de`. Your parser sees a line that is not valid JSON and drops half a token, emits a corrupt event or crashes. In production that is a truncated answer in one response out of a few hundred, the kind of bug that survives a quarter of blame between the model vendor and your proxy.

The second is line endings. SSE allows `\n`, `\r` and `\r\n`. A parser that only handles `\n` leaves a trailing `\r` glued to every value, so your `event` type becomes `"delta\r"` and never matches what you compare it against. Special case the pair instead and a chunk boundary between the two dispatches a phantom event.

The third is unbounded memory: a stream that never sends a newline is an exhaustion attack on any parser that appends until it finds a delimiter. Then there is the metadata everyone skips, `id:` for Last-Event-ID resume, `retry:` for the reconnect delay and `:` comment lines that heartbeat proxies inject. Miss the comments and your JSON parser eats a keepalive. Miss the id and every reconnect replays from the start.

## Why I built it

Every language has an SSE client except the one where you need it. In Node or Python you import something. In C you are writing a libcurl write callback, an NGINX module or a WASI binary, and the choice is a full HTTP client dependency just for its event parser, or the split on newline loop that has the bugs above. Neither is a good trade when the problem fits in one file with nothing beyond libc.

The other reason is ownership. Most parsers hand you a heap allocated event struct and expect you to free it. This one passes a borrowed view into the callback and reuses its buffers, so the steady state after warmup is zero allocations per event.

## When to use it

- A libcurl `CURLOPT_WRITEFUNCTION` callback taking chunks of an OpenAI or Anthropic streaming response that needs whole events out of it.
- A C proxy between clients and an LLM vendor that inspects or counts events without buffering the whole response.
- You need Last-Event-ID and `retry:` tracked so a dropped connection resumes instead of restarting the stream.
- An embedded or WASI target where a full HTTP client just for its event parser is not acceptable.
- Hardening an SSE consumer that fell over on a hostile upstream and needs hard caps on line and event size.
- Testing an SSE producer against a reference consumer that follows the field rules exactly.

## How it works

The core is a push parser and you own the I/O. `sse_decoder_feed` walks a raw chunk one byte at a time, and the only state persisting between chunks lives in `SseDecoder`: a `line` buffer for the partial line, a `data` buffer for the current block payload, plus `event_type` and `last_event_id`. Because the loop assumes nothing about where a chunk ends, a line split across ten chunks decodes identically to one that arrives whole.

Line endings use a one bit lookahead rather than a lookback. On `\r` the decoder finishes the line and sets `skip_next_lf`; on the next byte, if that flag is set and the byte is `\n`, the byte is swallowed. One flag makes all three terminators behave the same, and it survives a chunk boundary landing between the `\r` and the `\n`, the case that breaks most hand written state machines.

Completed lines go through `sse_process_completed_line`. On the first line only, if `strip_utf8_bom` is set, a leading UTF-8 BOM is removed by `sse_buffer_strip_prefix`. An empty line ends the block and calls `sse_dispatch_event`. A line starting with `:` is a comment and is dropped, which keeps proxy heartbeats out of your JSON parser. Otherwise the line splits at the first colon and exactly one leading space is stripped from the value, per spec. Four fields are recognised: `data` appends the value plus a newline via `sse_append_data_line`, `event` sets the type, `id` sets the last event id unless the value holds a NUL byte, and `retry` goes through `sse_parse_retry_ms`, a digits only parse with an overflow guard. Unknown fields are dropped.

Dispatch is where the borrowed view matters. `sse_dispatch_event` builds an `SseEventView` pointing into the decoder's own buffers. The trailing newline that data accumulation adds is trimmed by writing a NUL over it and restoring the saved byte after the callback returns, so the caller gets a proper C string with no copy and no shrink. The view also carries `sequence`, the reconnect delay and a `retry_updated` flag saying this block changed it. With no `event:` line the type is the spec default `"message"`. Your callback returns `bool`: return `false` and the feed stops with `SSE_DECODER_CALLBACK_ABORTED`, which is how you cancel from inside the parse loop.

Memory is bounded on two axes. `max_line_bytes` defaults to 16 KB and trips `SSE_DECODER_LINE_TOO_LONG` the moment a line would exceed it, so a newline free stream cannot grow the buffer without limit. `max_event_bytes` defaults to 4 MB and is checked by `sse_check_block_size` against the combined data, event type and last event id lengths before every append. Growth is amortised doubling from 64 bytes with a `SIZE_MAX / 2` guard, and every size addition goes through `sse_try_add_size` so a length cannot silently wrap.

State that should survive an event does: `sse_reset_current_block` clears data and event type but leaves `last_event_id` and the reconnect delay alone, which is what resume needs. On EOF, `sse_decoder_finish` flushes a trailing partial line then dispatches the pending block, so a stream ending without a final blank line still delivers its last event.

## Usage

No `main` and no CLI. It is a library file: compile it in and drive it from your own I/O loop.

```c
#include <stdio.h>
/* declarations live at the top of SseStreamDecoder.c */

static bool on_event(void *user_data, const SseEventView *event) {
  (void)user_data;
  if (sse_event_is_done_sentinel(event)) {
    return false;                 /* stops the feed, returns CALLBACK_ABORTED */
  }
  printf("#%llu [%.*s] %.*s\n",
         (unsigned long long)event->sequence,
         (int)event->event_type_len, event->event_type,
         (int)event->data_len, event->data);
  return true;                    /* keep going */
}

int main(void) {
  SseDecoderConfig config = sse_decoder_config_default();
  config.max_line_bytes  = 16u * 1024u;        /* default */
  config.max_event_bytes = 4u * 1024u * 1024u; /* default */
  config.dispatch_empty_events = false;
  config.strip_utf8_bom = true;

  SseDecoder decoder;
  if (sse_decoder_init(&decoder, &config) != SSE_DECODER_OK) {
    return 1;
  }

  char chunk[4096];
  size_t n, emitted = 0u;
  SseDecoderStatus status = SSE_DECODER_OK;

  while ((n = fread(chunk, 1u, sizeof(chunk), stdin)) > 0u) {
    status = sse_decoder_feed(&decoder, chunk, n, on_event, NULL, &emitted);
    if (status != SSE_DECODER_OK) {
      break;
    }
  }
  if (status == SSE_DECODER_OK) {
    status = sse_decoder_finish(&decoder, on_event, NULL, &emitted);
  }

  size_t id_len = 0u;
  const char *last_id = sse_decoder_last_event_id(&decoder, &id_len);
  uint64_t retry_ms = 0u;
  bool has_retry = sse_decoder_get_reconnect_delay(&decoder, &retry_ms);

  fprintf(stderr, "status=%s bytes=%llu events=%llu last-id=%.*s retry=%s\n",
          sse_decoder_status_string(status),
          (unsigned long long)sse_decoder_bytes_seen(&decoder),
          (unsigned long long)sse_decoder_event_count(&decoder),
          (int)id_len, last_id, has_retry ? "set" : "unset");

  sse_decoder_destroy(&decoder);
  return status == SSE_DECODER_OK ? 0 : 1;
}
```

```sh
cc -std=c99 -O2 -Wall -Wextra -c SseStreamDecoder.c -o SseStreamDecoder.o
curl -sN https://example.com/stream | ./your_binary
```

## Notes

- Not a network client and not a JSON parser. It takes bytes you already have and hands back event payloads. The `SseEventView` borrows the decoder's buffers and is valid only inside that callback, so copy anything you keep.
- `SseDecoderStatus`: 0 ok, 1 invalid argument, 2 out of memory, 3 line too long, 4 event too large, 5 integer overflow, 6 callback aborted. `sse_decoder_status_string` renders any of them.
- After a line too long or event too large the decoder still holds the offending state. Reset it before reuse, or destroy it. It does not self recover. `sse_decoder_reset` also wipes `last_event_id`, the retry delay, the BOM flag and both counters, so read what a reconnect needs first.
- Malformed `retry:` values and `id:` values holding a NUL byte are ignored rather than raising an error, as the spec asks. No UTF-8 validation: bytes pass through untouched.
- A block with fields but no `data` produces no event by default. Set `dispatch_empty_events` to true to get `event:` or `id:` only blocks. No threading and no internal locking: one decoder per stream, one thread.
