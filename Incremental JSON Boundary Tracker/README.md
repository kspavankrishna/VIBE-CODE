# Incremental JSON Boundary Tracker

An LLM streams you tokens: some chatter, a markdown fence, then a JSON object arriving a few characters at a time. This is a character level state machine that tells you the exact moment the first complete JSON value has landed, so you can parse it once and parse it right.

**Language:** Python | **Lines:** 109 | **Added:** 2026-04-10

## What this solves

This solves a nasty streaming problem in real LLM apps. The model starts with chatter, wraps output in markdown fences, then sends JSON piece by piece. If you parse too early, you crash. If you wait blindly, you add latency and still miss boundary bugs. Anyone who has shipped a structured output pipeline has seen `json.JSONDecodeError: Expecting value` on a payload that was perfectly valid two chunks later.

The naive fixes all break. Calling `json.loads` on the accumulated buffer after every chunk re-parses a growing string on every token. Counting braces with a plain counter works until a model writes `"note": "brace } inside string is fine"` and your counter hits zero halfway through the object. Regex on triple backticks works until the model forgets the fence or emits the object with no wrapper at all. Waiting for end of stream throws away the point of streaming, and still leaves you guessing which slice of the final text is the payload.

The damage is in the tail. A brace counter that trips on a string literal returns a partial object that deserializes into a half filled model with silently missing fields, which is worse than a crash because nothing alerts. The person who notices is the on call engineer reading a stack trace, or the customer whose extraction job quietly dropped three fields.

This tracker sits between the token stream and the parser and answers one question: has the first complete JSON value arrived yet. It ignores prose, ignores fences and handles quoted strings and backslash escapes. It says done on the character that closes the payload, not a chunk later.

## Why I built it

Built because this shows up all the time in tool calling and structured output pipelines, and every codebase reinvents it badly. The standard library gives you `json.JSONDecoder.raw_decode`, which decodes a prefix happily enough, but it still throws on incomplete input and expects you to have already found the start of the value. Third party incremental parsers build a full parse tree as they go, which is more machinery than you need when the only thing you want is a boundary signal. Streaming SDKs solve this for their own tool call format and leave you alone the moment a model returns JSON inside plain text.

The gap is a small, dependency free component that does boundary detection and nothing else. No schema, no parse tree, no partial object reconstruction. Find the start, track the nesting, say when it closes, hand back the raw text.

## When to use it

- A chat completion streams prose then a fenced JSON block, and you want to fire the downstream action the instant the block closes.
- A tool calling loop where arguments arrive as a token stream and you need to dispatch with the lowest possible latency.
- An extraction job where the model wraps its answer in explanation and you need the object without another fence regex.
- You are seeing intermittent decode errors on payloads that contain braces or brackets inside string values.
- You want a cheap guard in front of an expensive parse or validation step.
- SSE or log processing where JSON documents sit embedded in a line oriented text stream.

## How it works

The core is a two phase character scanner. `IncrementalJsonBoundaryTracker` holds a `JsonCaptureState` dataclass plus two lists: `buffer` for the captured payload and `prefix` for the chatter before it. `feed(chunk)` walks the chunk one character at a time and returns `state.complete` as a boolean, so the caller just checks the return value after every chunk.

Phase one is `_scan_prefix`. While `state.started` is false, every character goes into `prefix`, capped at 32 characters by popping from the front, because the preamble can be arbitrarily long and there is no reason to hold it. The moment a character is `[` or `{`, capture begins: `started` flips true, `start_char` records which bracket opened it, `depth` is set to 1, `started_at` records the offset inside the trimmed window, and the opening bracket goes into `buffer`. Markdown fences need no special case. Backticks are not `[` or `{`, so they fall through and get discarded like any other chatter.

Phase two is `_advance_capture`, where the real work happens. It is a state machine with three pieces of state: `in_string`, `escape` and `depth`. Inside a string only three things matter. If `escape` is set the character is consumed and `escape` clears, which is what makes `\"` safe. A backslash sets `escape`. A closing quote clears `in_string`. Nothing inside a string touches `depth`, which is exactly why `"brace } inside string is fine"` does not end the capture early. Outside a string, a quote opens one, `[` or `{` increments depth, `]` or `}` decrements it, and depth reaching zero sets `complete`.

Every character from the start of capture onward is appended to `buffer` before the state machine sees it, so `raw()` returns the exact source text including whitespace. `value()` calls `json.loads` on that text and raises `ValueError` if you call it early. That is the contract: the tracker finds boundaries, `json.loads` does validation. Cost is linear in characters seen, memory is bounded by the payload plus a 32 character window, and there is no re-parsing, backtracking or regex anywhere. `reset()` rebuilds the state and clears both lists so one instance serves many requests.

## Usage

```python
from IncrementalJsonBoundaryTracker import IncrementalJsonBoundaryTracker

tracker = IncrementalJsonBoundaryTracker()

for piece in token_stream:          # any iterable of str chunks
    if tracker.feed(piece):         # True once the payload closes
        break

print(tracker.raw())                # exact source text of the payload
print(tracker.value())              # parsed Python object

tracker.reset()                     # reuse for the next payload
```

Run the file directly for the built in demo. It feeds a four chunk stream with preamble text, a json fence, an array split across chunks and a nested object holding a brace inside a string value:

```bash
python3 IncrementalJsonBoundaryTracker.py
```

## Notes

- Only object and array payloads are detected. A top level scalar such as `"hello"`, `42` or `true` never starts capture, because `_scan_prefix` triggers only on `[` or `{`.
- The first bracket wins. A stray `[` or `{` in the preamble, for example "here is a list [of things]", starts capture on the wrong text. If your prompts put brackets in prose, strip the preamble before feeding it.
- Bracket types are not matched. `depth` treats `[`, `{`, `]` and `}` as interchangeable, so `{"a": 1]` is reported complete and then fails in `json.loads`. Boundary first, validation in the parser.
- Characters after completion are dropped. `feed` breaks out of its loop as soon as `complete` is set, so the rest of that chunk is neither buffered nor returned. Track trailing text yourself if you need it.
- One payload per instance. Call `reset()` before feeding a second document, otherwise `feed` returns immediately on the already complete state.
- `fence_ticks`, `start_char` and `started_at` are recorded but nothing reads them. They are hooks, not active behaviour, and `started_at` is an offset into the trimmed 32 character window, not an absolute stream position.
- Standard library only: `json`, `dataclasses` and `typing`. No third party dependencies, no CLI parser and no exit codes. The `__main__` block is a demo that prints per chunk progress and the parsed result.
