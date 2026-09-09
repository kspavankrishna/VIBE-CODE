# JSON RPC Frame Codec

A stdin pipe hands you bytes, not messages. This is an incremental Content-Length frame decoder and encoder in OCaml for MCP and LSP style JSON-RPC transports, built to survive split headers, mixed line endings and hostile payload sizes.

**Language:** OCaml | **Lines:** 429 | **Added:** 2026-04-17

## What this solves

Broken MCP stdio framing and JSON-RPC Content-Length parsing, especially when an MCP server, an MCP client, an LSP bridge or an AI agent runtime reads partial chunks from stdin or a pipe. The failures in 2026 are still the boring ones: split headers, mixed line endings, oversized payloads, duplicate Content-Length values and retries that look like new messages.

The concrete failure mode is this. Your read loop calls `input` on a pipe and gets 4096 bytes back. That boundary lands in the middle of `Content-Len` and the rest arrives in the next syscall. Naive code that splits on `\r\n\r\n` and calls `int_of_string` sees nothing, drops the chunk or throws. Now the stream is desynchronised. Every following byte is read as a header, the JSON parser gets fed a header block, and the client hangs waiting on a response id that will never come. This never shows up in tests, because tests write one whole frame at a time. It shows up under load, on a slow peer, or when a large tool result crosses the pipe buffer size.

The second failure mode is ambiguity, not truncation. Two Content-Length headers in one frame is a request smuggling question, not a parse question: one reader believes the first value, another believes the second, and the two disagree about where the next message starts. A declared body length of hundreds of megabytes handed straight to `Bytes.create` is an OOM with no stack trace, and nobody notices until the runtime dies mid tool call and the user sees a truncated answer.

The third is duplication. Transports retry, and a runtime that reconnects and replays its outbound queue can deliver the same frame twice. A server with no identity for a frame will execute the same tool call again: a second write, a second charge, a second side effect. This codec gives every decoded frame a stable 64 bit fingerprint so dedupe and log correlation are one function call.

## Why I built it

The OCaml ecosystem has excellent JSON libraries and no obvious answer for the layer underneath them. You either pull in a large protocol framework that assumes it owns your event loop, or you write the framing by hand in twenty lines and quietly get every edge case wrong. I wanted something that sits between the transport and the JSON layer, owns nothing, allocates predictably and has no dependency beyond the standard library.

It is also written to be audited. Every failure is a named constructor in `decode_error`, not a string or an exception, so the compiler tells you when you have forgotten a case. The strictness is deliberate: the ambiguous header cases that cause request smuggling and ghost tool calls are rejected rather than guessed at.

## When to use it

- Writing an MCP server in OCaml that reads JSON-RPC frames from stdin and must not desync when the parent process writes in small chunks.
- Building an LSP client or language server bridge that has to accept both CRLF framing from real editors and bare LF framing from test fixtures and shell pipelines.
- Putting a hard ceiling on header and body size at the transport boundary, before an untrusted peer can make you allocate.
- Deduplicating replayed frames after a reconnect, using a content fingerprint instead of trusting the JSON-RPC id.
- Encoding outbound frames with the correct `application/vscode-jsonrpc; charset=utf-8` content type without hand rolling the header block every time.
- Parsing a captured transcript of a whole session in one shot to reproduce a framing bug offline.

## How it works

The decoder is a growable byte buffer plus a length. `create ?config ()` allocates `initial_buffer_size` bytes, floored at 128. `feed_bytes decoder chunk offset length` appends into it: `ensure_capacity` grows the buffer by repeated doubling through `next_capacity`, blits the chunk in and bumps `used`. Then `drain_frames` runs. Nothing about the shape of the input matters, so a caller can feed one byte at a time or a megabyte at a time and get identical results.

`drain_frames` is a tail recursive loop that pulls out as many complete frames as the buffer currently holds. Each pass calls `find_header_terminator`, which scans for `\r\n\r\n` first and falls back to `\n\n`, returning both the offset and the delimiter length so mixed line endings are handled without normalising the buffer. If no terminator is found the accumulated frames are returned, unless the unterminated prefix already exceeds `max_header_bytes`, which yields `Header_too_large`. If a terminator is found but the buffer does not yet hold `header_end + delimiter_length + content_length` bytes, the loop stops and waits. That is the whole trick against split headers and split bodies: the codec never consumes what it cannot fully interpret.

Header parsing runs through a `let*` Result monad, so the first error short circuits. `parse_headers` splits the block on `\n`, drops a trailing `\r` per line, filters blanks and rejects the block with `Too_many_headers` past `max_headers`. `parse_header_line` requires a colon, a non empty name and a name made only of the token characters `a-z`, `A-Z`, `0-9` and `-`. Names are compared after `normalize_header_name`, which trims and lowercases, so wire casing is irrelevant. A second `content-length` or `content-type` is a hard `Duplicate_header` error, which is the smuggling defence. Other repeated headers survive in order, since `headers` is an association list handed back to the caller.

`parse_content_length` rejects negatives and rejects any declared length above `max_body_bytes` with `Body_too_large` before a single body byte is allocated. `validate_content_type` splits on `;`, unquotes each parameter value and, only if a `charset` parameter is present, checks it against `accepted_charsets`, which defaults to `utf-8` and `utf8`. A content type with no charset parameter is accepted as is, which is what real editors and MCP clients send. A completed body is copied out with `Bytes.sub`, then `discard_prefix` compacts the buffer by blitting the remainder to offset zero.

The fingerprint is FNV-1a over 64 bits, with `fnv_offset_basis` and `fnv_prime` as `Int64` constants. `frame_fingerprint64` folds every header as normalized name, colon, raw value and newline, then folds the body into the same running state, so two frames differing only in header casing hash the same and two differing anywhere in the body do not. FNV-1a fits because it is non cryptographic, allocation free, single pass and stable across runs and machines, which is all a dedupe key needs.

Encoding is deliberately dull. `encode_bytes ?content_type ?headers body` emits `Content-Length` from the actual buffer length, then `Content-Type` if given, then any extra headers with `strip_reserved_headers` dropping user supplied `content-length` and `content-type` so they cannot contradict the real ones. `render_headers` joins with CRLF and terminates with CRLFCRLF.

## Usage

```ocaml
(* Streaming: wire feed_bytes to your reader loop. *)
let decoder = JsonRpcFrameCodec.create () in

let rec pump input_channel =
  let chunk = Bytes.create 4096 in
  let n = input input_channel chunk 0 4096 in
  if n = 0 then
    match JsonRpcFrameCodec.close decoder with
    | Ok frames -> List.iter handle frames
    | Error e -> prerr_endline (JsonRpcFrameCodec.decode_error_to_string e)
  else
    match JsonRpcFrameCodec.feed_bytes decoder chunk 0 n with
    | Ok frames -> List.iter handle frames; pump input_channel
    | Error e -> prerr_endline (JsonRpcFrameCodec.decode_error_to_string e)

and handle frame =
  (* frame.body is bytes, hand it to your JSON layer *)
  print_endline (JsonRpcFrameCodec.body_text frame);
  print_endline (JsonRpcFrameCodec.frame_fingerprint_hex frame);
  match JsonRpcFrameCodec.header frame "content-type" with
  | Some ct -> print_endline ct
  | None -> ()

(* Writing a frame. *)
let out =
  JsonRpcFrameCodec.encode_jsonrpc_string
    {|{"jsonrpc":"2.0","id":1,"result":{}}|}
in
output_bytes stdout out

(* One shot parse of a captured transcript. *)
match JsonRpcFrameCodec.decode_all transcript with
| Ok frames -> Printf.printf "%d frames\n" (List.length frames)
| Error e -> prerr_endline (JsonRpcFrameCodec.decode_error_to_string e)

(* Tighter limits for an untrusted peer. *)
let config =
  { JsonRpcFrameCodec.default_config with
    max_body_bytes = 1024 * 1024;
    max_headers = 16 }
in
let decoder = JsonRpcFrameCodec.create ~config ()
```

Other entry points in the file: `feed_string`, `reset`, `is_closed`, `has_pending_bytes`, `buffered_bytes`, `body_preview ?limit`, `encode_string ?content_type ?headers`, `encode_bytes`, `encode_jsonrpc_bytes` and `fold_frames decoder input ~init ~f`.

## Notes

- No recovery after a decode error. The offending bytes stay in the buffer, so feeding more data returns the same error forever. Treat any `Error` as fatal for that connection, or call `reset` and resynchronise yourself.
- `close` returning `Incomplete_frame_at_eof` discards the frames it drained on that call. Drain with `feed_bytes` first and treat `close` purely as the EOF check.
- Content-Length goes through `int_of_string_opt`, which also accepts OCaml literal forms such as `0x10`, `1_000` and a leading `+`. A strict HTTP parser would reject those. Tighten it if a proxy sits in front of you.
- Header names are restricted to alphanumerics and `-`, so anything with an underscore is `Invalid_header_name`. Folded or continued header lines are not supported. `feed_bytes` raises `Invalid_argument` for out of range offsets rather than returning an error.
- The codec does nothing with the body. No JSON parsing, no UTF-8 validation, no check that the payload is JSON-RPC at all. Pair it with your own JSON library.
- The buffer grows by doubling and never shrinks, so one oversized frame leaves the decoder holding that capacity for life. Create a fresh decoder per connection. Single file, no `.mli`, standard library only.
