# Interleaved Tool Call Assembler

Streaming LLM tool calls arrive as fragments: a partial function name here, a slice of JSON arguments there, two calls interleaved on the same stream, and sometimes the same SSE chunk delivered twice. This is a single Rust file that reassembles those deltas into complete, validated tool calls with no JSON crate and no framework.

**Language:** Rust | **Lines:** 1105 | **Added:** 2026-04-20

## What this solves

This solves Rust streaming tool call assembly, interleaved function call parsing, LLM tool delta reconstruction, OpenAI Responses API tool-call merging, Anthropic tool use streaming, Vercel AI SDK backend normalization and agent runtime reliability when fragments arrive out of order or get replayed. Built because modern agent backends keep dealing with partial tool names, split JSON arguments, duplicate SSE chunks and streams that start with an index and only later reveal the call id. That sounds small until a production agent replays the same tool twice or closes JSON too early and hits a real API with bad arguments.

The failure modes are boring and expensive. A model emits two tool calls in one turn, the transport interleaves them, and a naive accumulator that appends every `arguments` fragment to one buffer produces `{"query":"rust{"url":"https://`. That either fails to parse and the whole turn is retried, or worse it parses into something plausible and you call a real endpoint with the wrong payload. A proxy retries an SSE frame, the same delta lands twice, and the tool executes twice: two charges, two emails sent, two rows written. A provider starts a call with only `index: 0` and reveals `call_id` three chunks later, so keying naively on `call_id` creates a phantom second call that never completes and leaks for the life of the process. And a stream that dies mid call leaves half assembled state in a map until the worker gets OOM killed at 3am.

This file handles the index to call id transition, suppresses replayed deltas by fingerprint, refuses regressed sequence numbers, tracks JSON structure as bytes stream in, and evicts anything that has gone quiet. Every rejection is a typed value you can log and count, not a swallowed error.

## Why I built it

Every agent framework has this logic buried somewhere, usually in TypeScript, tangled with the HTTP client and the provider adapter, and not extractable. On the Rust side the choice was pulling in `serde_json` plus a streaming parser and still writing the key resolution and dedup logic myself, or writing the whole thing. The parsing part is small if you only need to know where the JSON value ends, not what is inside it.

The other gap is observability. Most implementations panic, swallow, or return a generic error when a stream misbehaves. In production you want to know that six calls this hour died on `IncompleteJson`, three on `IdleTimeout` and one on `SequenceRegression`, because those numbers point at three different bugs in three different places.

## When to use it

- A gateway or proxy sits between your app and a model provider and must hand downstream consumers whole tool calls, not fragments.
- One model turn emits several tool calls and the transport interleaves their deltas on a single stream.
- Your SSE or WebSocket layer retries frames, so the same chunk can arrive twice.
- A provider identifies calls by array index first and only later attaches a stable call id.
- You need hard memory bounds on an agent worker: caps on in flight calls, argument bytes, fragments and idle time.
- You want structural validation of streamed arguments before executing anything, with no JSON dependency in the crate graph.

## How it works

Entry is `InterleavedToolCallAssembler::ingest(ToolCallDelta) -> Result<AssemblerUpdate, AssemblyError>`. A `ToolCallDelta` carries `stream_id`, `observed_at_ms`, optional `sequence`, `provider`, `call_id`, `index`, optional `name_fragment` and `arguments_fragment`, plus an `is_final` flag, built through chainable helpers like `with_index` and `final_fragment`. Every `ingest` first runs `collect_idle`, so eviction happens on the normal path with no timer thread.

Identity is resolved by `resolve_key_for_delta`. Three key shapes exist: `stream::id::<call_id>`, `stream::index::<n>` and `stream::anonymous`. When a delta finally supplies both a `call_id` and an `index`, and an index keyed state already exists, that state is removed from the map, rekeyed, given the `call_id` and reinserted under the id key. That single migration step is what stops the phantom second call, and the first test exercises exactly that path. Provider and call id are filled in on first sight only, never overwritten.

Deduplication uses FNV-1a 64 in `fingerprint_delta`, hashing the delta's identifying fields, both fragments and the final flag. `remember_fingerprint` keeps those hashes in a `HashSet` for lookup and a parallel `VecDeque` for FIFO eviction once `max_duplicate_fingerprints_per_call` is exceeded, so it is a bounded recent set rather than unbounded history. A hit sets `duplicate_suppressed`, refreshes `last_update_ms` so the call does not idle out, and returns without incrementing `fragment_count` or touching the buffers. FNV-1a fits: fast, no allocation beyond a scratch buffer, and this is collision tolerance not security. `fingerprint_call` separately hashes the finished name and arguments so consumers can dedupe completed calls.

Argument validation is `JsonBoundaryTracker`, a character state machine with a `stack: Vec<char>` of open delimiters, `in_string` and `escape_next` flags, a `root_kind` of `Object` or `Array`, plus `started` and `complete`. It skips leading whitespace, rejects any root that is not `{` or `[` with `InvalidStart`, ignores braces inside string literals, honours backslash escapes, and marks `complete` the moment the stack empties. After that, any non whitespace character is `TrailingNonWhitespace`. It is a balanced delimiter tracker, not a JSON parser: it tells you exactly where the value ends and catches mismatched or unexpected closers.

Completion fires when `is_final` is set, or when `auto_complete_on_valid_json` is on, `require_explicit_final` is off and the tracker reports complete. `complete_state` gates on four things: a non empty trimmed name, non empty trimmed arguments, `validate_for_finish` which separates `UnterminatedString` from an incomplete structure, and `require_json_object_or_array`. Pass and you get an `AssembledToolCall` with a `FinishReason` of `JsonCompleted`, `ExplicitFinal` or `FinalizedByCaller`. Fail and you get a `RejectedToolCall` carrying a `RejectReason`, the partial name and arguments, fragment count and both timestamps, so the wreckage stays inspectable.

Everything else is bounds. `AssemblerConfig` defaults to 128 in flight calls, 256 name bytes, 1 MiB of arguments, 4096 fragments per call, 256 remembered fingerprints and a 120000 ms idle window. Exceeding the in flight cap is an `AssemblyError` of kind `MaxInflightCallsExceeded`, the rest become rejections. `AssemblerUpdate` returns the accepted key, completed calls, rejected calls, the duplicate flag and a snapshot of everything pending, sorted by `first_seen_ms` then key so output is deterministic.

## Usage

```rust
// Single file, no dependencies. Drop it in as a module:
//   mod interleaved_tool_call_assembler;
//   use interleaved_tool_call_assembler::*;

let mut assembler = InterleavedToolCallAssembler::with_config(AssemblerConfig {
    max_idle_ms: 30_000,
    max_argument_bytes: 262_144,
    require_explicit_final: false,
    ..AssemblerConfig::default()
});

// Chunk 1: name plus the start of the arguments, keyed only by index.
let update = assembler.ingest(
    ToolCallDelta::new("stream-a", now_ms)
        .with_provider("openai")
        .with_index(0)
        .with_sequence(1)
        .with_name_fragment("web_search")
        .with_arguments_fragment("{\"query\":\"rust"),
)?;
assert!(update.completed.is_empty());

// Chunk 2: the provider now reveals the call id. The index keyed state is
// migrated to the id key rather than forking into a second call.
let update = assembler.ingest(
    ToolCallDelta::new("stream-a", now_ms + 40)
        .with_index(0)
        .with_call_id("call-0")
        .with_sequence(2)
        .with_arguments_fragment(" tool calls\"}"),
)?;

for call in &update.completed {
    // call.name, call.arguments, call.fingerprint, call.finish_reason
    dispatch(&call.name, &call.arguments);
}
for bad in &update.rejected {
    eprintln!("dropped {}: {} ({})", bad.key, bad.reason, bad.detail);
}

// On stream close, drain whatever is left.
let tail = assembler.finalize_all(now_ms + 5_000);

// Or run eviction on a tick without ingesting.
let expired: Vec<RejectedToolCall> = assembler.expire_idle(now_ms + 60_000);
```

Run the seven bundled tests with `cargo test`: interleaved assembly with index to call id upgrade, duplicate replay suppression, sequence regression, name changes after arguments, malformed JSON at finalize, idle expiry and auto completion without an explicit final flag.

## Notes

- The tracker validates structure only: delimiter balance, string and escape state, and the root being an object or array. It does not check commas, colons, numbers or literals, so `{"a" 1}` passes. Parse with `serde_json` downstream if you need full correctness.
- Two concurrent calls on one stream with no `call_id` and no `index` collapse into the same `stream::anonymous` key and their fragments merge. Providers that supply neither identifier cannot be disambiguated. The `InvalidAnonymousUpgrade` error kind exists for this case but the exact key match is checked first.
- Idle eviction only runs when `ingest`, `expire_idle` or `finalize_all` is called, so a silent process evicts nothing. `max_idle_ms` of 0 disables eviction entirely.
- Time is caller supplied via `observed_at_ms` and `now_ms`. No clock inside the file, so tests are deterministic and replay from a recorded stream is exact.
- Sequence checking is opt in per delta and duplicate suppression is exact content matching. A delta with no `sequence` never triggers `SequenceRegression`, equal sequence numbers are accepted, and a retried chunk differing by one byte is appended as new data.
- Synchronous and single threaded. Wrap it in a `Mutex` or give each connection its own instance in an async runtime. Byte limits are checked after appending, so a single oversized fragment briefly exceeds the cap before rejection.
