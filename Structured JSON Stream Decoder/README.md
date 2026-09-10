# Structured JSON Stream Decoder

An LLM streams you JSON over SSE and the transport splits it mid string, mid escape sequence, mid nested object. Calling `jsonDecode` on each delta throws. This Dart decoder buffers the deltas, tracks JSON structure character by character and hands you a value only once it is genuinely complete and verified.

**Language:** Dart | **Lines:** 719 | **Added:** 2026-04-14

## What this solves

This solves the annoying JSON streaming mess you hit when LLMs send partial objects over SSE, chunked HTTP or WebSockets and your app needs to act before the full response is finished. The model call is rarely the weak point. The weak point is turning messy streamed text into safe structured data without random parser crashes, giant buffers or brittle regex hacks.

The concrete failure mode looks like this. Your handler receives `{"tool":"search","args":{"q":"kilo` in one frame and the rest in the next. A naive implementation calls `jsonDecode` on every chunk and eats a `FormatException` on every partial, so you wrap it in a try/catch and swallow the error. Now you are silently dropping frames. The next fix is usually to concatenate everything and decode once at the end, which works until the model emits two objects in one stream, or wraps the payload in a markdown fence, or writes a sentence of commentary before the object. Then the concatenated blob is not valid JSON at all and the whole response is lost.

The regex stage comes after that. Someone writes `RegExp(r'\{.*\}', dotAll: true)` to pull the object out. It works in dev and breaks the first time a string value contains a `}` or an escaped quote, because a regex has no notion of nesting or escape state. The user sees a tool call that never fires, or a form that renders half filled. The on call engineer sees an exception with no payload attached, because the buffer was already cleared.

Memory is the other half. If the model never closes the object, or the connection stalls mid document, a plain accumulating buffer grows without a ceiling. On a server handling many concurrent streams that is a slow leak that ends in an OOM kill at peak traffic. This decoder caps the working buffer, caps any single document and caps how much non JSON text it will skip before deciding the source is not producing structured output at all. Each cap throws a typed `JsonStreamDecodeException` naming the limit you hit.

## Why I built it

Dart and Flutter teams are wiring models into mobile apps, edge workers, CLIs and backend tools, and the standard library gives you `jsonDecode` for a complete string and nothing for a stream. `JsonDecoder` has a chunked conversion sink but it assumes the byte stream is one well formed document from the first character to the last. It has no answer for leading prose, markdown fences, several documents in a row or a partial trailer at the end. Every project ends up writing the same half correct brace counter, usually without escape handling and always without limits.

I wanted one file I could drop into a Dart backend, a Flutter desktop tool, an AI gateway, a worker process, an inference proxy or a developer CLI. Simple to fork, easy to audit, no dependencies beyond `dart:async` and `dart:convert`. It keeps a streaming state machine for nested JSON, validates every completed boundary with the real Dart JSON decoder, preserves the skipped noise so you can inspect bad prompts and enforces hard memory limits so one bad stream does not quietly blow up your process.

## When to use it

- A model streams a tool call or a structured answer over SSE and you want to dispatch the moment the object closes, not after the whole response finishes.
- The provider wraps the payload in a ```` ```json ```` fence and you are currently stripping it with string replace.
- The model writes a line of explanation before the object and your parser chokes on the prose.
- One response contains several JSON documents back to back and you need each one separately, in order.
- You run many concurrent streams on a server and need a hard memory ceiling per stream instead of an unbounded accumulator.
- You are debugging prompt drift and want to see exactly what non JSON text the model emitted around the payload.

## How it works

`StructuredJsonStreamDecoder` is a hand written scanner over an append only `StringBuffer`. `feed(String chunk)` appends the delta, then loops `_consumeNextDocument` until no further document can be extracted, returning a list of `JsonStreamDocument` for whatever completed in that call. Nothing is emitted speculatively. The scanner never returns a partial value.

Finding the start is `_findDocumentStart`. It walks forward skipping whitespace, skipping any line beginning with three backticks when `stripMarkdownCodeFences` is on, and skipping a bare `json` or `JSON` word followed by whitespace, which is the common shape of a fence label that survived a bad strip upstream. Everything it walks past is recorded in a noise buffer rather than thrown away. The first character that could legally open a JSON value decides the document kind through `_TopLevelKindX.tryStart`, which maps `{` to object, `[` to array, `"` to string, a digit or minus to number and `t`/`f`/`n` to literal. With `allowTopLevelPrimitives: false` only `{` and `[` are accepted as starts.

Once a start is found, `_ScanState` runs the actual boundary detection in `_advanceActiveState`. For objects and arrays this is a depth counting scanner with proper string and escape tracking: `inString` flips on an unescaped quote, `escapeNext` swallows the character after a backslash, and brackets only count when the scanner is outside a string. That is the part regex solutions get wrong. Depth returning to zero marks completion. An unmatched closing bracket throws immediately instead of corrupting the rest of the stream. Top level strings complete on the closing quote. Numbers and literals are trickier because they have no terminator, so `_ScanState` validates the accumulated prefix as it goes: literals must stay a prefix of `true`, `false` or `null` via `_literalCouldStillMatch`, numbers are checked against the JSON grammar regex in `_numberCouldStillBeValid`, and a complete looking primitive sets `awaitingPrimitiveDelimiter` so the value is only closed when whitespace follows or the stream ends.

Boundary detection alone is not trust. Every candidate is passed to `_decodeStrict`, which runs the real `jsonDecode` from `dart:convert`. If the payload turns out to be malformed, you get a `JsonStreamDecodeException` naming the underlying error rather than a wrong value. Set `requireStrictJson: false` and a failed decode returns the raw substring instead of throwing, which is useful in a logging path where you would rather capture the garbage than lose the stream. Structural detection plus a real decode is deliberately belt and braces: the scanner finds the edges cheaply, the decoder proves the contents.

Memory is managed by `_compactBufferIfNeeded`. Once the cursor passes 64 KB the buffer is rewritten from either the start of the in flight document or the cursor, whichever is earlier, the dropped count is added to `discardedChars` and the active `_ScanState` is rebased with `shiftedLeft`. A long stream of many small documents therefore runs in roughly constant memory. Three ceilings guard the pathological cases: `maxBufferedChars`, `maxDocumentChars` and `maxSkippedChars`.

`close()` drains what remains, allowing a trailing primitive to finish at end of input through `canCloseAtEndOfInput`, and throws if a document is still open. `transformer()` wraps the whole thing as a `StreamTransformer<String, JsonStreamDocument>` so you can pipe an SSE text stream straight into it, with errors forwarded to the sink instead of crashing the isolate.

## Usage

```dart
import 'dart:convert';

// Streaming pipeline: pipe text deltas straight through the transformer.
final decoder = StructuredJsonStreamDecoder(
  maxBufferedChars: 512 * 1024,
  maxDocumentChars: 256 * 1024,
  maxSkippedChars: 128 * 1024,
  allowTopLevelPrimitives: true,
  stripMarkdownCodeFences: true,
  requireStrictJson: true,
);

await for (final doc in sseTextDeltas.transform(decoder.transformer())) {
  print('doc ${doc.index} kind=${doc.topLevelKind}');
  if (doc.topLevelKind == JsonTopLevelKind.object) {
    final call = doc.expectObject();
    dispatchToolCall(call['tool'] as String, call['args']);
  }
  if (doc.prefixNoise.trim().isNotEmpty) {
    log.warning('model emitted prose before JSON: ${doc.prefixNoise}');
  }
}

// Manual pull model: feed arbitrary chunks, then close.
final manual = StructuredJsonStreamDecoder();
for (final chunk in ['```json\n{"a":', '1,"b":"x}y"}', '\n```']) {
  for (final doc in manual.feed(chunk)) {
    print(doc.rawJson);              // {"a":1,"b":"x}y"}
    print(doc.asPrettyJson(indent: '    '));
  }
}
for (final doc in manual.close()) {
  print(doc.value);
}

print('${manual.bufferedChars} buffered, ${manual.discardedChars} discarded');
print(manual.isInsideDocument);      // false when idle between documents
```

## Notes

- Pure Dart, no packages. Only `dart:async` and `dart:convert`.
- Not a partial value parser. It gives you complete documents only. If you want to render half an object as it arrives, this is the wrong tool.
- Every limit breach throws `JsonStreamDecodeException`, including buffer overflow in `feed`, oversized single documents, too much skipped noise, an unmatched closing bracket and an invalid number or literal prefix. `feed` after `close` throws `StateError`.
- `close()` throws if the stream ends mid object, mid array or mid string. Trailing top level numbers and literals are allowed to complete at end of input.
- Trailing whitespace and fences after a document land in that document's `suffixNoise` and are also carried forward into the next document's `prefixNoise`. Do not treat the two fields as disjoint.
- Fence handling is line based: any line starting with three backticks is skipped whole. Fences are recognised only when at least one character follows the third backtick in the current buffer.
- With `requireStrictJson: false` a failed decode yields the raw substring, so `JsonStreamDocument.value` can be a `String` even when `topLevelKind` says object. `expectObject` will throw there.
- Indexes on `JsonStreamDocument` are per decoder instance and monotonic. Create a fresh decoder per stream.
