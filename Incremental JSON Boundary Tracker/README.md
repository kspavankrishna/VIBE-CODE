# Incremental JSON Boundary Tracker

An LLM streams you prose, then a markdown fence, then JSON, then more prose. You need to know the exact character where the JSON payload ends so you can parse it once, immediately, without guessing.

**Language:** Python | **Lines:** 109 | **Added:** 2026-04-10

## What this solves

This solves a nasty streaming problem in real LLM apps. The model starts with chatter, wraps output in markdown fences, then sends JSON piece by piece. If you parse too early, you crash. If you wait blindly, you add latency and still miss boundary bugs. It shows up constantly in tool calling and structured output pipelines, and every team hits it in the same order: first they wait for the stream to close, then they add a regex to strip fences, then they get paged because a string value contained a brace.

The concrete failure looks like this. Your model returns `{"user":"Pavan","meta":{"note":"brace } inside string is fine"}}`. A depth counter that does not understand JSON strings sees that `}` inside the string value and declares the payload finished. You slice the buffer there, hand the truncated text to `json.loads` and get a `JSONDecodeError`. The tool call fails, the retry fires, you pay for the generation twice and the bug never reproduces in tests because no fixture contains a brace inside a string. Escaped quotes break it the other way: `"he said \"hi\""` ends your string early if you toggle on every quote you see.

The other half of the problem is where the JSON starts. A regex that grabs everything between the first `{` and the last `}` breaks the moment there is trailing prose containing a brace, a second fenced block or a closing fence followed by commentary. The common fallback of calling `json.loads` on the accumulated buffer after every token is quadratic: a 20 KB payload arriving in 4000 tokens means thousands of full reparses of a growing string.

Waiting for the stream to close avoids all of it and costs you the thing streaming was for. You cannot execute the tool call until the model finishes explaining what it just did, which is seconds of dead time on every request.

## Why I built it

The standard library has no incremental JSON API. `json.loads` needs the whole document. `json.JSONDecoder.raw_decode` tolerates trailing data and reports where a value ended, but it still needs the complete value in the buffer, it raises on anything truncated and it will not skip prose sitting in front of the payload. Streaming parsers built for large files pull events out of a byte stream, but they assume the stream *is* JSON. None of them answer the question here: where inside this noisy text does the first complete JSON value begin and end.

So this is one small class between the token stream and the parser. No dependencies, one file, constant work per character. It answers one question, and it answers it correctly for the two cases that actually break in production: braces inside strings and escaped quotes inside strings.

## When to use it

- You are streaming a tool call from an LLM and want to dispatch the moment the JSON closes, not when the model stops talking.
- The model wraps its JSON in `` ```json `` fences with commentary before and after, so a plain `json.loads` on the buffer never works.
- You are seeing intermittent `JSONDecodeError` you cannot reproduce, and the payloads that fail contain braces or quotes inside string values.
- You are calling `json.loads` in a try and except on every token and the reparse cost shows up in your latency profile.
- You are writing a proxy or a logging layer that has to pull the payload out of a passthrough stream without buffering the whole response.
- You need to know a payload is complete before the upstream connection closes, to start a database write or a downstream call early.

## How it works

The core is a character level state machine split across two phases. All mutable state lives in one `JsonCaptureState` dataclass so it can be reset, inspected or logged as a single object: `started`, `complete`, `start_char`, `in_string`, `escape`, `fence_ticks`, `depth` and `started_at`.

`feed(chunk)` is the only entry point you drive. It walks the chunk one character at a time, breaks the instant `state.complete` is set and returns the completion flag so a caller can write `if tracker.feed(piece): ...`. Everything after the closing character is dropped, which is what you want: trailing prose, a closing fence and any second JSON block never enter the buffer.

Phase one is `_scan_prefix`. Before capture begins, every character goes into a `prefix` list trimmed to the last 32 characters with a `pop(0)`, so leading chatter of any length costs constant memory. The scan looks for the first `[` or `{`. When it finds one it flips `started`, records `start_char`, sets `depth` to 1, notes `started_at` and pushes that opening character into the capture buffer. It also counts consecutive backticks into `fence_ticks`. Be precise about those three fields: `fence_ticks`, `start_char` and `started_at` are recorded but nothing branches on them. They are diagnostic state, not part of the completion decision.

Phase two is `_advance_capture`, and this is the part that earns the file. If `in_string` is true, nothing structural is counted: a pending `escape` consumes exactly one character and clears, a backslash sets `escape`, an unescaped `"` closes the string, everything else is ignored. That ordering is what makes `"brace } inside string is fine"` and `\"` both safe. Only outside a string does a `"` open one, and only outside a string do `[` and `{` increment `depth` while `]` and `}` decrement it. When `depth` hits zero the payload is complete. It is the scanner half of a JSON tokenizer, reduced to the minimum needed to find a boundary, so the cost is O(1) per character and O(n) over the stream with no reparsing.

Parsing is deliberately deferred. `raw()` joins the captured buffer and hands you the exact source text. `value()` refuses to guess: it raises `ValueError("JSON payload is not complete")` if `state.complete` is false, otherwise it runs `json.loads` on the joined buffer. You pay for one parse, at the end, on text you already know is balanced. `reset()` rebuilds a fresh state and clears the buffer and the prefix, so one instance handles the next payload on the same connection.

So the tracker never needs to understand fences, prose, model quirks or provider formats. It only needs to know where JSON strings begin and end. Everything else is depth counting.

## Usage

```bash
python3 "IncrementalJsonBoundaryTracker.py"
```

Running the file executes the `__main__` block, which feeds a four chunk stream containing chatter, a `` ```json `` fence, a split array, a brace inside a string value and trailing text. It prints progress per chunk, then the parsed object.

```python
from IncrementalJsonBoundaryTracker import IncrementalJsonBoundaryTracker

tracker = IncrementalJsonBoundaryTracker()

for piece in response_stream:          # any iterable of str chunks
    if tracker.feed(piece):            # True the moment depth returns to zero
        break

print(tracker.raw())                   # exact captured source text
payload = tracker.value()              # json.loads, raises ValueError if incomplete

tracker.reset()                        # reuse the instance for the next payload
```

## Notes

- Requires Python 3.9 or later for the builtin generic annotation. Imports are `json`, `dataclasses` and `typing` only, no third party dependencies.
- `feed` takes `str`, not `bytes`. Reading raw bytes off a socket means decoding upstream with an incremental decoder so a multi byte character is never split across chunks.
- Depth counting does not check that closers match openers. `{"a":1]` reaches depth zero and is reported complete, then `value()` raises `JSONDecodeError`. The tracker finds boundaries, `json.loads` is still your validator.
- It captures objects and arrays only. A bare top level scalar such as `42`, `true` or `"a string"` never sets `started` and is never captured.
- The first `[` or `{` anywhere in the prefix starts capture, including one inside prose, so `here you go [see below]` triggers a capture and completes on that bracket. There is also no size cap and no timeout: if the model never closes the structure the buffer grows as long as you keep feeding it. Bound it at the caller.
- Only the first payload is captured and everything after it is discarded, so a response with two JSON blocks needs a `reset()` and a second pass over text this class does not hand back to you. `value()` reparses on every call and caches nothing, the class is not thread safe and it expects one stream per instance.
