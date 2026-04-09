from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Literal, Sequence
import math


Role = Literal["system", "developer", "user", "assistant", "tool"]


@dataclass(slots=True)
class ChatMessage:
    role: Role
    content: str
    pinned: bool = False
    name: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    def estimated_tokens(self) -> int:
        base = 6
        name_cost = 2 if self.name else 0
        text_cost = math.ceil(len(self.content) / 4)
        metadata_cost = sum(math.ceil((len(k) + len(v)) / 6) for k, v in self.metadata.items())
        return base + name_cost + text_cost + metadata_cost


@dataclass(slots=True)
class PackedContext:
    messages: List[ChatMessage]
    used_tokens: int
    dropped_messages: List[ChatMessage]
    available_input_tokens: int
    reserved_output_tokens: int


class ContextWindowPacker:
    def __init__(
        self,
        model_context_window: int,
        reserved_output_tokens: int,
        safety_margin_tokens: int = 256,
    ) -> None:
        if model_context_window <= 0:
            raise ValueError("model_context_window must be positive")
        if reserved_output_tokens < 0 or safety_margin_tokens < 0:
            raise ValueError("reserved_output_tokens and safety_margin_tokens must be non-negative")

        available = model_context_window - reserved_output_tokens - safety_margin_tokens
        if available <= 0:
            raise ValueError("No usable input budget after output reservation and safety margin")

        self.model_context_window = model_context_window
        self.reserved_output_tokens = reserved_output_tokens
        self.safety_margin_tokens = safety_margin_tokens
        self.available_input_tokens = available

    def pack(self, messages: Sequence[ChatMessage]) -> PackedContext:
        pinned = [m for m in messages if m.pinned]
        regular = [m for m in messages if not m.pinned]

        used = sum(m.estimated_tokens() for m in pinned)
        if used > self.available_input_tokens:
            raise ValueError("Pinned messages alone exceed available input budget")

        packed: List[ChatMessage] = list(pinned)
        dropped: List[ChatMessage] = []

        latest_user_idx = self._last_index(regular, "user")
        latest_assistant_idx = self._last_index(regular, "assistant")

        prioritized: List[ChatMessage] = []
        seen_ids: set[int] = set()

        for idx in (latest_user_idx, latest_assistant_idx):
            if idx is not None:
                msg = regular[idx]
                prioritized.append(msg)
                seen_ids.add(id(msg))

        tail_regular = [m for m in reversed(regular) if id(m) not in seen_ids]

        for msg in prioritized + tail_regular:
            cost = msg.estimated_tokens()
            if used + cost <= self.available_input_tokens:
                packed.append(msg)
                used += cost
            else:
                dropped.append(msg)

        packed.sort(key=lambda m: messages.index(m))
        dropped.sort(key=lambda m: messages.index(m))

        return PackedContext(
            messages=packed,
            used_tokens=used,
            dropped_messages=dropped,
            available_input_tokens=self.available_input_tokens,
            reserved_output_tokens=self.reserved_output_tokens,
        )

    def with_truncation(self, messages: Sequence[ChatMessage], min_tail_chars: int = 400) -> PackedContext:
        try:
            return self.pack(messages)
        except ValueError:
            pinned = [m for m in messages if m.pinned]
            regular = [m for m in messages if not m.pinned]

            trimmed: List[ChatMessage] = list(pinned)
            used = sum(m.estimated_tokens() for m in pinned)
            dropped: List[ChatMessage] = []

            if used > self.available_input_tokens:
                raise

            for msg in reversed(regular):
                remaining = self.available_input_tokens - used
                if remaining <= 10:
                    dropped.append(msg)
                    continue

                if msg.estimated_tokens() <= remaining:
                    trimmed.append(msg)
                    used += msg.estimated_tokens()
                    continue

                tail = msg.content[-max(min_tail_chars, remaining * 4 - 32):].strip()
                if len(tail) < 40:
                    dropped.append(msg)
                    continue

                clipped = ChatMessage(
                    role=msg.role,
                    content="[TRUNCATED EARLIER CONTENT]\n" + tail,
                    pinned=False,
                    name=msg.name,
                    metadata=msg.metadata.copy(),
                )
                cost = clipped.estimated_tokens()
                if used + cost <= self.available_input_tokens:
                    trimmed.append(clipped)
                    used += cost
                else:
                    dropped.append(msg)

            trimmed.sort(key=lambda m: pinned.index(m) if m in pinned else len(pinned) + regular[::-1].index(m))
            return PackedContext(
                messages=trimmed,
                used_tokens=used,
                dropped_messages=list(reversed(dropped)),
                available_input_tokens=self.available_input_tokens,
                reserved_output_tokens=self.reserved_output_tokens,
            )

    @staticmethod
    def _last_index(messages: Sequence[ChatMessage], role: Role) -> int | None:
        for i in range(len(messages) - 1, -1, -1):
            if messages[i].role == role:
                return i
        return None


if __name__ == "__main__":
    history = [
        ChatMessage("system", "You are a precise coding assistant.", pinned=True),
        ChatMessage("developer", "Never reveal secrets. Prefer direct answers.", pinned=True),
        ChatMessage("user", "Here is the repo context and architecture... " * 30),
        ChatMessage("assistant", "Understood. I will inspect the repo and propose a fix." * 18),
        ChatMessage("user", "Now patch the retry logic and keep the API stable." * 24),
    ]

    packer = ContextWindowPacker(model_context_window=8192, reserved_output_tokens=1200)
    result = packer.with_truncation(history)

    print(f"Used input tokens: {result.used_tokens}/{result.available_input_tokens}")
    print(f"Kept messages: {len(result.messages)} | Dropped messages: {len(result.dropped_messages)}")
    for msg in result.messages:
        print(f"- {msg.role}: {msg.content[:80].replace(chr(10), ' ')}")
    

"""
This solves the dumb but expensive problem of blowing the context window right before a model call. Built because chat stacks keep growing and most teams still pack history with rough guesses. Use it when you need to keep pinned instructions, preserve the latest turn, and stay inside budget without random failures. The trick is simple: estimate token cost, lock critical messages, prefer recent turns, and truncate only when needed. Drop this into any chat or agent pipeline.
"""
