"""
ContextWindowOptimizer.py

Manages LLM conversation history under token limits without losing critical context.
Scores each message by recency, role weight and keyword overlap with the current query,
then drops the lowest-scoring turns until the history fits the budget.
"""

import re
import math
from dataclasses import dataclass, field
from typing import Literal

Role = Literal["system", "user", "assistant"]

ROLE_WEIGHT: dict[Role, float] = {
    "system": 3.0,
    "user": 1.5,
    "assistant": 1.0,
}


@dataclass
class Message:
    role: Role
    content: str
    token_count: int = field(init=False)
    pinned: bool = False

    def __post_init__(self) -> None:
        self.token_count = estimate_tokens(self.content)


def estimate_tokens(text: str) -> int:
    # cl100k-compatible approximation: ~4 chars per token + overhead per message
    return math.ceil(len(text) / 4) + 4


def tfidf_score(query_terms: set[str], content: str) -> float:
    terms = set(tokenize(content))
    if not terms:
        return 0.0
    overlap = query_terms & terms
    return len(overlap) / math.log1p(len(terms))


def tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


class ContextWindowOptimizer:
    """
    Drop-in history manager for any OpenAI-compatible chat loop.

    Usage:
        ctx = ContextWindowOptimizer(max_tokens=6000)
        ctx.add("system", "You are a helpful assistant.", pin=True)
        ctx.add("user", user_message)
        messages = ctx.fit(current_query)   # pass directly to client.chat.completions.create
    """

    def __init__(
        self,
        max_tokens: int = 6000,
        reply_buffer: int = 1000,
        recency_decay: float = 0.92,
    ) -> None:
        self.budget = max_tokens - reply_buffer
        self.decay = recency_decay
        self._history: list[Message] = []

    def add(self, role: Role, content: str, pin: bool = False) -> None:
        msg = Message(role=role, content=content)
        msg.pinned = pin
        self._history.append(msg)

    def fit(self, current_query: str = "") -> list[dict]:
        query_terms = set(tokenize(current_query))
        candidates = self._score(query_terms)
        total = sum(m.token_count for m in self._history)

        while total > self.budget:
            # find lowest-scoring unpinned message
            evictable = [
                (score, i, msg)
                for i, (msg, score) in enumerate(candidates)
                if not msg.pinned
            ]
            if not evictable:
                break
            evictable.sort(key=lambda x: x[0])
            _, idx, victim = evictable[0]
            total -= victim.token_count
            self._history.remove(victim)
            candidates = self._score(query_terms)

        return [{"role": m.role, "content": m.content} for m in self._history]

    def _score(self, query_terms: set[str]) -> list[tuple[Message, float]]:
        n = len(self._history)
        scored = []
        for i, msg in enumerate(self._history):
            recency = self.decay ** (n - 1 - i)
            role_w = ROLE_WEIGHT.get(msg.role, 1.0)
            relevance = tfidf_score(query_terms, msg.content) if query_terms else 0.0
            score = recency * role_w + relevance
            scored.append((msg, score))
        return scored

    def token_usage(self) -> int:
        return sum(m.token_count for m in self._history)

    def clear(self, keep_pinned: bool = True) -> None:
        if keep_pinned:
            self._history = [m for m in self._history if m.pinned]
        else:
            self._history.clear()


# ---------------------------------------------------------------------------
# Quick demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    ctx = ContextWindowOptimizer(max_tokens=500, reply_buffer=100)
    ctx.add("system", "You are a senior software engineer assistant.", pin=True)

    exchanges = [
        ("user", "How do I implement a circuit breaker in Python?"),
        ("assistant", "Use a state machine with CLOSED, OPEN and HALF_OPEN states tracked per endpoint."),
        ("user", "What threshold should I use for failure rate?"),
        ("assistant", "50 percent over a 10-second sliding window is a reasonable starting point."),
        ("user", "Can you show me a minimal class for this?"),
        ("assistant", "Sure. Track failure count and timestamp, flip state when threshold is exceeded."),
        ("user", "How do I reset the breaker after a cooldown period?"),
    ]

    for role, content in exchanges:
        ctx.add(role, content)

    current = "How do I reset the circuit breaker?"
    fitted = ctx.fit(current)

    print(f"Token usage after fit: {ctx.token_usage()} / 400")
    print(f"Messages retained: {len(fitted)}")
    for m in fitted:
        print(f"  [{m['role']}] {m['content'][:80]}")

"""
This solves context overflow in long LLM conversations without blind truncation.
Built because naive "drop oldest" strategies kill relevance fast once threads run deep.
Use it when your chat loop approaches the model's context limit and you need
to keep the most useful history rather than just the most recent.
The trick: score every message by recency decay multiplied by role weight plus
TF-IDF overlap with the current query then evict the weakest until it fits.
Drop this into any OpenAI-compatible chat loop as a wrapper around your message list.
"""
