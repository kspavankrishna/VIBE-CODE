# Context Window Packer

Your chat history grows one turn past the model context window and the API call fails at request time, in production, after the user already waited. This is a small Python packer that decides what to keep and what to drop before you send.

**Language:** Python | **Lines:** 182 | **Added:** 2026-04-09

## What this solves

The failure is boring and expensive. A conversation accumulates turns. Someone pastes a stack trace, a CSV dump or a whole file. On turn nineteen the request is 200 tokens over the limit and the provider rejects the entire call with a context length exceeded error. You did not get a partial answer. You got a 400, a retry, double latency and in most stacks a broken user session. Nothing in that request was wrong except the size.

The second failure is quieter and worse. Teams that hit this once patch it with a rule like keep the last twenty messages. That works until the system prompt scrolls out of the window. Now the model has no formatting contract, no tool policy and no safety instruction. It answers in prose where your parser expects JSON, or it discusses something the developer message forbade. Nobody sees a stack trace. You see a slow rise in malformed responses and a support ticket saying the assistant got weird.

The third failure is budget arithmetic. Input and output share the same window. Fill it with history and the model has no room to answer, so generation stops mid sentence or mid object. The fix is to reserve output tokens up front and treat the remainder as the real input budget. That is `available_input_tokens` here: window minus reserved output minus a safety margin, computed once in the constructor and enforced on every pack.

## Why I built it

Token counting and context policy are two different problems and the tooling only solves the first. A tokenizer gives you an exact count and stops there. It will not tell you which message to sacrifice, and it does not know your system prompt is load bearing. Framework memory classes do make that decision, but they bury it. You cannot see the policy, you cannot unit test it and when a message gets dropped you never find out which one.

I wanted the policy visible in one file, no dependencies, returning a plain object that says how many tokens were used, what was kept and what was dropped. Estimate token cost, lock the critical messages, prefer recent turns, truncate only when needed.

## When to use it

- A support or coding assistant whose threads run long and occasionally break with a context length exceeded error
- An agent loop where tool output is unpredictable in size and one fat response blows the window
- Any pipeline where the system or developer message must survive no matter what else gets cut
- Streaming responses that keep getting cut off because input ate the output budget
- You need to log which parts of history were dropped, not silently lose them
- You want a dependency free pre flight check in front of whichever provider SDK you use

## How it works

`ChatMessage` is a slotted dataclass holding `role`, `content`, a `pinned` flag, an optional `name` and a `metadata` dict. `Role` is a `Literal` covering system, developer, user, assistant and tool. Cost comes from `estimated_tokens()`, a deliberate heuristic rather than a real tokenizer: a flat base of 6 tokens for role and message framing, 2 more if a `name` is set, `ceil(len(content) / 4)` for the text at the usual four characters per token ratio, and `ceil((len(key) + len(value)) / 6)` per metadata pair. No tiktoken, no network, no model specific vocabulary. It is an estimate, and the safety margin exists to absorb the error.

`ContextWindowPacker.__init__` takes `model_context_window`, `reserved_output_tokens` and `safety_margin_tokens` which defaults to 256. It rejects a non positive window and negative reservations, computes `available = window - reserved - margin` and refuses to construct at all if that is zero or below. Failing in the constructor rather than at call time is the point: you learn the configuration is impossible when you build the packer, not while a user is waiting.

`pack()` is a greedy fill with a priority prefix. It splits history into pinned and regular, sums the pinned cost first and raises `ValueError` if the pinned set alone will not fit, because there is no useful answer in that case. Then `_last_index()` scans backwards for the last user message and the last assistant message in the regular set. Those go into a `prioritized` list tracked by `id()`, so identity based dedupe stops the same object being counted twice. The rest of the regular history is walked in reverse as `tail_regular`, newest first. The loop adds each message if `used + cost` still fits and appends it to `dropped` otherwise. It does not stop at the first message that does not fit, so a huge old message gets skipped while a smaller older one after it can still land.

Both lists are then restored to conversation order with `packed.sort(key=lambda m: messages.index(m))`, a stable chronological re sort against the original sequence. A pinned system prompt that started at position zero comes back at position zero, so the model sees a coherent transcript rather than a reordered one. The result is a `PackedContext` carrying the kept messages, `used_tokens`, `dropped_messages`, `available_input_tokens` and `reserved_output_tokens`, enough to log exactly what happened.

`with_truncation()` wraps `pack()` in a try block and, on `ValueError`, falls into a second strategy: rebuild from pinned, walk the regular messages newest first, and for any message that will not fit whole, keep its tail. Tail length is `max(min_tail_chars, remaining * 4 - 32)` characters, converting the remaining token budget back into characters at the same ratio, prefixed with a `[TRUNCATED EARLIER CONTENT]` marker so the model knows the message is partial. Tails under 40 characters get dropped instead. Read the Notes before relying on this path.

## Usage

Run the file directly for the built in demo, which packs a five message history against an 8192 token window with 1200 tokens reserved for output.

```bash
python3 "ContextWindowPacker.py"
```

```
Used input tokens: 931/6736
Kept messages: 5 | Dropped messages: 0
- system: You are a precise coding assistant.
- developer: Never reveal secrets. Prefer direct answers.
...
```

As a library:

```python
from ContextWindowPacker import ChatMessage, ContextWindowPacker

history = [
    ChatMessage("system", "You are a precise coding assistant.", pinned=True),
    ChatMessage("developer", "Never reveal secrets. Prefer direct answers.", pinned=True),
    ChatMessage("user", long_repo_context),
    ChatMessage("assistant", previous_answer),
    ChatMessage("user", "Now patch the retry logic and keep the API stable."),
]

packer = ContextWindowPacker(
    model_context_window=8192,
    reserved_output_tokens=1200,
    safety_margin_tokens=256,   # default
)

result = packer.pack(history)          # raises ValueError if pinned alone overflow
# result = packer.with_truncation(history, min_tail_chars=400)

print(result.used_tokens, "/", result.available_input_tokens)
for m in result.dropped_messages:
    log.info("dropped %s message, %d tokens", m.role, m.estimated_tokens())

payload = [{"role": m.role, "content": m.content} for m in result.messages]
```

## Notes

- Needs Python 3.10 or newer, for `dataclass(slots=True)` and the union syntax in annotations. Standard library only, no third party dependency.
- The token count is an estimate, not a tokenizer. Four characters per token holds up for English prose. It under counts CJK text, emoji, base64 blobs and dense code. Raise `safety_margin_tokens` when your traffic looks like that.
- The truncation branch of `with_truncation()` is currently unreachable. It only runs when `pack()` raises `ValueError`, the only `ValueError` `pack()` raises is the pinned overflow check, and the fallback re runs the identical sum then re raises immediately. Today `with_truncation()` behaves as `pack()` with an extra re raise. The tail clipping logic is written and wired but not exercised. Fix that before depending on it.
- Chronological restore uses `list.index`, which is equality based and linear per lookup. Two messages with identical role, content, name and metadata resolve to the same sort key, and packing a very long history is quadratic. Fine for chat sized inputs, not for tens of thousands of messages.
- Priority is fixed and structural: pinned, then last user, then last assistant, then everything else newest first. The `tool` role is accepted but gets no special treatment, so a critical tool result in the middle of a long history can be dropped. Pin it if it matters.
- No summarization, no embedding or relevance ranking, no async, no I/O and no provider calls. It decides what to keep. Sending the request is still your job.
