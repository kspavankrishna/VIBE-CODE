# AI SSE JSON Assembler

Model APIs stream tool call arguments and structured outputs as Server-Sent Events, so one JSON object arrives split across dozens of frames. This is a single file C++ streaming JSON assembler that turns that byte stream back into complete, validated JSON values before your service acts on them.

**Language:** C++ | **Lines:** 723 | **Added:** 2026-04-23

## What this solves

The naive version parses every SSE `data:` payload and drops whatever fails. It works in a demo and falls apart in production, because one tool call argument object routinely spans ten or more events. The second naive version buffers text and counts braces until they balance. That is worse. A `}` inside a string literal, normal in any model output containing code or prose, fires the counter early and hands downstream code a truncated object that still looks plausible. The failure is silent: a function call runs with half its arguments, a record is persisted with a missing field and no exception appears in the trace.

The next thing that breaks is memory. A gateway holds one buffer per open stream. If the upstream stalls, emits a pathologically large payload, or the client disconnects and nobody notices, those buffers grow without limit. On a container with a fixed ceiling the process gets OOM killed, taking every other in flight stream with it. One misbehaving upstream kills unrelated traffic, and that shows up at 3am rather than in testing.

Then there is UTF-8. TCP does not respect character boundaries, so a multi byte character split across two chunks becomes mojibake as soon as anything logs or length checks the joined string. And there is noise: comment lines starting with `:`, keepalives, `event:` and `id:` fields, preamble before the first `{`, and the `[DONE]` sentinel that OpenAI style APIs send as literal text rather than JSON. Each of those is a parse error if you feed the raw stream to a JSON library.

## Why I built it

The gap is structural. SSE client libraries hand you a decoded event as a string and consider the job done. JSON libraries want a complete document and treat a truncated one as an error rather than a state. Nothing sits in between for C++, so every team building a gateway or an agent runtime writes the same fragile glue and gets the brace counting wrong the same way.

RapidJSON and simdjson have push modes, but you still write the frame logic, the resynchronisation, the limits and the UTF-8 boundary handling yourself, and take on a dependency you may not want in an edge binary. I wanted one file with the memory ceiling visible at the top of a struct, so a reviewer can see it without reading the parser.

## When to use it

- You are proxying an OpenAI style or Anthropic style SSE stream in C++ and need complete tool call arguments before dispatching the tool.
- Your gateway must not let one stalled or hostile upstream grow a buffer past a fixed byte count.
- You are collecting structured model output on an edge node where a JSON dependency is not worth its size.
- The upstream emits several values per event, or splits one value across events, and you want both on the same code path.
- You are reading newline delimited JSON with no SSE framing and want the same completion guarantees.
- You need per stream counters for discarded noise and nesting depth, for logging or alerting.

## How it works

Two layers, deliberately separate. `AiSseJsonAssembler` owns the transport. `JsonFragmentAssembler` owns the JSON. Use the inner one alone if there is no SSE framing.

`FeedTransportBytes` takes an arbitrary chunk, pushes it through an incremental `detail::Utf8Validator`, appends it to `transport_buffer_` and checks that against `max_buffered_transport_bytes` (256 KiB by default), throwing `AssemblerError` if it is over. It scans for `\n`, strips a trailing `\r` and hands each complete line to `ConsumeSseLine`. Any tail without a newline stays buffered, which is what makes chunk boundaries irrelevant. `ConsumeSseLine` implements the SSE field grammar: a leading `:` is a comment, the field name is everything before the first colon, one optional leading space is stripped from the value, `data` values accumulate into `event_data_` joined by newlines, `event` sets `event_name_`, `id` and `retry` are ignored, and a bare colonless `[DONE]` line sets the terminal flag. A blank line calls `DispatchEvent`, which is the real dispatch rule rather than a per line guess. The validator carries `expected_continuations_` across chunks, so a character split down the middle is fine while a bad leading or continuation byte throws immediately.

`JsonFragmentAssembler::Feed` is the core loop, a hand written recursive descent parser with an explicit incomplete state. `detail::FindNextJsonStart` scans for a plausible value start using `IsLikelyJsonStart`, which accepts `{` and `[` always and string, number, `true`, `false` and `null` starts only when `accept_top_level_primitives` is on. Skipped bytes go into `discarded_noise_bytes` and are erased, so preamble costs a counter increment rather than a parse failure. Then `JsonParser::ParseSingleValue` runs from offset zero. Every production, `ParseObject`, `ParseArray`, `ParseString`, `ParseNumber`, `ParseBoolean` and `ParseNull`, returns `kComplete`, `kIncomplete` or `kError` instead of throwing, and running off the end of the buffer is always `kIncomplete`. That distinction is what makes the parser restartable.

On `kComplete` the byte range is sliced into a `CompletedJsonValue` carrying the raw JSON, the `JsonValueKind` and the `from_sse` flag, then erased, and the loop tries again for the next value in the same chunk. On `kIncomplete` it stops after checking the pending buffer against `max_single_json_bytes`. On `kError` it drops exactly one byte, bumps `discarded_invalid_candidates` and retries. That byte at a time resynchronisation guarantees forward progress on a corrupt stream instead of wedging.

The string parser is where brace counting loses. `ParseString` tracks escapes properly: eight legal single character escapes, exactly four hex digits after `\u`, raw control bytes below 0x20 rejected, and `kIncomplete` rather than `kError` when the buffer ends mid escape. A `}` inside a string never terminates a value early. `ParseBoolean` and `ParseNull` do the same for literals by testing whether the remaining bytes are a proper prefix of `true`, `false` or `null`. Depth is capped by `max_json_depth` (128), with the deepest depth seen in `max_depth_observed`. Every limit lives in `AssemblerLimits`, every breach throws `AssemblerError` derived from `std::runtime_error`, and `stats()` reports completed values, noise bytes, invalid candidates, SSE events, `[DONE]` markers and both live buffer sizes.

## Usage

One translation unit, no `main`, no header guard, everything in namespace `vibe::streaming`. Include it directly or rename it to a header.

```cpp
#include "AiSseJsonAssembler.cpp"   // or rename to AiSseJsonAssembler.h

vibe::streaming::AssemblerLimits limits;
limits.max_buffered_transport_bytes = 256 * 1024;
limits.max_single_json_bytes        = 1 * 1024 * 1024;
limits.max_json_depth               = 64;
limits.accept_top_level_primitives  = false;   // objects and arrays only

vibe::streaming::AiSseJsonAssembler assembler(limits);

try {
    // Feed raw socket bytes. Chunk boundaries do not matter.
    for (std::string_view chunk : ReadFromUpstream()) {
        for (auto& value : assembler.FeedTransportBytes(chunk)) {
            std::printf("complete %s json (%zu bytes): %s\n",
                        value.from_sse ? "sse" : "raw",
                        value.json.size(),
                        value.json.c_str());
        }
        if (assembler.done()) break;   // saw the [DONE] sentinel
    }
} catch (const vibe::streaming::AssemblerError& e) {
    std::fprintf(stderr, "stream aborted: %s\n", e.what());
}

const auto s = assembler.stats();
// s.completed_values, s.sse_events_seen, s.discarded_noise_bytes,
// s.max_depth_observed, s.buffered_json_bytes
```

With no SSE framing, call `FeedJsonText(chunk)` instead. Inspect pending state with `PendingTransportBytes()`, `PendingSseEventData()` and `PendingJson()`. Build with `c++ -std=c++20 -O2 main.cpp`. C++20 is required for the designated initialisers used throughout the parser.

## Notes

- It validates and delimits JSON, it does not decode it. You get the raw byte range and a `JsonValueKind`. Escapes are checked for legality but never unescaped, and there is no DOM, no duplicate key detection and no number range check.
- The UTF-8 validator checks structure, not canonical validity. It rejects bad leading and continuation bytes, `0xC0` and `0xC1` overlongs and anything above `0xF4`, but not three byte overlongs or encoded surrogates. `IsBoundary()` is exposed and never called inside the file.
- The parser restarts from offset zero on every feed, so a value arriving across many small chunks is rescanned each time. Quadratic in the worst case: fine for typical tool call payloads, wrong for a multi megabyte single value.
- Top level bare numbers are greedy with no terminator, so with `accept_top_level_primitives` on, a number split across a chunk boundary can emit as two values. Turn the flag off, the right setting for tool call streams anyway.
- Emitted JSON can carry trailing whitespace, because `ParseSingleValue` advances past it before reporting the end offset. `max_completed_queue` bounds the values returned by one `Feed` call, not a persistent queue.
- Every breach throws and leaves the object as it was. There is no `Reset()`, so recovery means building a new assembler. Not thread safe: one per stream, owned by the thread reading that socket.
