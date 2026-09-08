# Context Window Optimizer

Long LLM conversations eventually blow past the model's context limit, and the usual fix is to drop the oldest messages until it fits. That throws away the system prompt behaviour, the constraint the user stated twenty turns ago and the one answer that actually mattered.

**Language:** Python | **Lines:** 156 | **Added:** 2026-04-09

## What this solves

Context overflow in a chat loop is not a subtle failure. You append one more turn, the API returns a 400 saying the request exceeds the model's maximum context length and the request dies. So most people bolt on the first thing that works: keep the last N messages, or keep messages until the token count fits and discard the rest from the front. That gets you past the error and straight into a worse problem.

The worse problem is silent quality collapse. A support agent forgets the account ID the user gave in message three. A coding assistant forgets the project is on Python 3.9 and starts emitting match statements. An agent forgets its own system prompt, because "drop oldest" happily evicts the very first message in the list. Nobody sees a stack trace. The user just sees the assistant get dumber the longer they talk to it, and the ticket says "it forgot what we were discussing" with no reproduction steps attached.

There is a cost angle too. You resend the full history every turn, so a 40 turn thread charges you for the whole prefix on every call. If you are trimming anyway, trimming by usefulness instead of by age gets the same bill with better answers. Recency is a decent proxy for usefulness. It is not the same thing.

This file scores every message by three signals, then evicts the lowest scorer until the total fits a budget you set. Pinned messages are never evicted, which is how the system prompt survives. Scoring blends recency decay, role importance and keyword overlap with the current query, so an old message that answers the question outranks a recent one about nothing.

## Why I built it

Naive "drop oldest" strategies kill relevance fast once threads run deep. The framework wrappers that offer history management give you two options: a fixed window, or a summarizing memory that makes an extra LLM call per trim, adds latency to your hot path and quietly rewrites facts. Neither is what you want when you already have a working chat loop and just need the message list to fit.

I wanted no dependencies, no network calls and no framework buy in. One file, standard library only, taking a list of messages and giving back a shorter list in the same OpenAI compatible dict shape. If it does not fit your loop you can read the whole thing in five minutes and change the scoring function.

## When to use it

- Your chat endpoint returns context length exceeded errors as sessions get long and you are currently truncating from the front.
- You run an agent with a long system prompt of tool descriptions and behaviour rules that must never be dropped.
- Your assistant is measurably worse at turn 30 than at turn 5 and you suspect the trimming is eating relevant history.
- You want to cap history without adding a summarization LLM call and its latency to every request.
- You want deterministic, inspectable trimming you can reason about instead of a black box memory class.

## How it works

The core type is a `Message` dataclass holding `role`, `content`, a `pinned` flag and a `token_count` computed in `__post_init__` rather than passed in. Counting goes through `estimate_tokens`, a cl100k compatible approximation: `ceil(len(text) / 4) + 4`. Four characters per token is the usual rule of thumb for English under a BPE tokenizer, and the constant four covers the per message role and delimiter overhead the API adds. This is an estimate, not a tokenizer, which is why the class carries a `reply_buffer`. `__init__` computes `self.budget = max_tokens - reply_buffer`, headroom for the model's own reply, since the context limit covers prompt plus completion. Default is 6000 total with 1000 reserved.

Scoring happens in `_score`. For each message at index `i` in a history of length `n`, recency is `decay ** (n - 1 - i)`, so the newest scores 1.0 and older ones fall off geometrically. At the default 0.92 a message ten turns back is worth about 0.43 of a fresh one. That factor is multiplied by a role weight from `ROLE_WEIGHT`: system 3.0, user 1.5, assistant 1.0. The multiplication matters. A system message still decays but stays three times more valuable at any age, and user turns outrank the assistant's own output because user turns carry the constraints and the facts.

Relevance is added on top rather than multiplied. `tfidf_score` lowercases and splits both sides with `tokenize`, a plain `[a-z0-9]+` regex, then returns `len(overlap) / log1p(len(terms))`. The log denominator is length normalization: without it a long rambling message wins on raw overlap by containing more words, so dividing by `log1p` of the term count rewards density of matching terms instead of volume. It is a simplified TF-IDF, an overlap count with a sublinear length penalty and no corpus level IDF. With no query, relevance is zero and scoring falls back to recency times role weight.

`fit(current_query)` is the entry point. It tokenizes the query, scores the history, sums the token counts, then loops while the total exceeds the budget. Each pass filters to unpinned messages, sorts ascending by score, removes the single lowest scorer from `_history`, subtracts its tokens and rescores everything. The rescore is deliberate: removing a message changes `n`, which shifts every remaining recency exponent, so the next victim is chosen against the updated ranking rather than a stale one. Cost is quadratic in the number of evictions, fine for chat sized histories and not fine for tens of thousands. If nothing evictable is left the loop breaks. Output is a fresh list of `{"role": ..., "content": ...}` dicts in chronological order.

`token_usage()` sums the current estimated count for logging or test assertions. `clear(keep_pinned=True)` resets the thread while keeping the pinned system prompt, for when the user starts a new conversation but the agent's instructions stay the same.

## Usage

```python
from ContextWindowOptimizer import ContextWindowOptimizer

ctx = ContextWindowOptimizer(max_tokens=6000, reply_buffer=1000, recency_decay=0.92)
ctx.add("system", "You are a senior software engineer assistant.", pin=True)

ctx.add("user", "How do I implement a circuit breaker in Python?")
ctx.add("assistant", "Use a state machine with CLOSED, OPEN and HALF_OPEN states.")

query = "How do I reset the circuit breaker?"
ctx.add("user", query)

messages = ctx.fit(query)          # list[dict] in OpenAI chat format
print(ctx.token_usage())           # estimated tokens currently held

# response = client.chat.completions.create(model="...", messages=messages)

ctx.clear(keep_pinned=True)        # new thread, same system prompt
```

Run the built in demo to watch eviction happen. It builds a seven turn circuit breaker conversation under a tight 500 token limit with a 100 token reply buffer, so the real budget is 400 and several turns get dropped:

```bash
python3 "ContextWindowOptimizer.py"
```

## Notes

- `fit()` mutates the optimizer permanently. Evicted messages are gone from `_history`, so keep your own full transcript for logging or replay.
- Token counts are estimated at roughly four characters per token, not measured with a real tokenizer. Code, non English text and heavy punctuation drift from the estimate. `reply_buffer` absorbs that, so widen it if you run close to the limit.
- Eviction discards, it does not summarize. No compression pass and no extra LLM call, which is the point, but dropped content is gone.
- Nothing enforces user and assistant pairing. A user turn can be evicted while its assistant reply survives, leaving an answer with no visible question. Order is preserved, pairing is not.
- If every message is pinned, or one pinned message alone exceeds the budget, the loop breaks and `fit()` returns a history still over budget. Pin sparingly and check `token_usage()`.
- Eviction uses `list.remove`, which matches by dataclass equality, so exact duplicates are interchangeable and the earlier one goes first.
- Standard library only: `re`, `math`, `dataclasses` and `typing`. Needs Python 3.9 or newer for the built in generic annotations.
