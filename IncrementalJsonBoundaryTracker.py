import json
from dataclasses import dataclass
from typing import Any


@dataclass
class JsonCaptureState:
    started: bool = False
    complete: bool = False
    start_char: str = ""
    in_string: bool = False
    escape: bool = False
    fence_ticks: int = 0
    depth: int = 0
    started_at: int = -1


class IncrementalJsonBoundaryTracker:
    def __init__(self) -> None:
        self.state = JsonCaptureState()
        self.buffer: list[str] = []
        self.prefix: list[str] = []

    def feed(self, chunk: str) -> bool:
        for ch in chunk:
            if self.state.complete:
                break
            if not self.state.started:
                self._scan_prefix(ch)
                continue
            self.buffer.append(ch)
            self._advance_capture(ch)
        return self.state.complete

    def value(self) -> Any:
        if not self.state.complete:
            raise ValueError("JSON payload is not complete")
        return json.loads("".join(self.buffer))

    def raw(self) -> str:
        return "".join(self.buffer)

    def reset(self) -> None:
        self.state = JsonCaptureState()
        self.buffer.clear()
        self.prefix.clear()

    def _scan_prefix(self, ch: str) -> None:
        self.prefix.append(ch)
        if len(self.prefix) > 32:
            self.prefix.pop(0)

        if ch == "`":
            self.state.fence_ticks += 1
        else:
            self.state.fence_ticks = 0

        if ch in "[{":
            self.state.started = True
            self.state.start_char = ch
            self.state.depth = 1
            self.state.started_at = len(self.prefix) - 1
            self.buffer.append(ch)

    def _advance_capture(self, ch: str) -> None:
        if self.state.in_string:
            if self.state.escape:
                self.state.escape = False
            elif ch == "\\":
                self.state.escape = True
            elif ch == '"':
                self.state.in_string = False
            return

        if ch == '"':
            self.state.in_string = True
            return

        if ch in "[{":
            self.state.depth += 1
        elif ch in "]}":
            self.state.depth -= 1
            if self.state.depth == 0:
                self.state.complete = True


if __name__ == "__main__":
    tracker = IncrementalJsonBoundaryTracker()
    stream = [
        "model says here you go\n```json\n",
        '{"user":"Pavan","items":[1,2,',
        '3],"meta":{"ok":true,"note":"brace } inside string is fine"}}',
        "\n``` trailing text"
    ]

    for piece in stream:
        done = tracker.feed(piece)
        print({"chunk": piece[:24], "complete": done, "raw_len": len(tracker.raw())})
        if done:
            break

    print(tracker.value())

"""
================================================================================
EXPLANATION
This solves a nasty streaming problem in real LLM apps. The model starts with chatter, wraps output in markdown fences, then sends JSON piece by piece. If you parse too early, you crash. If you wait blindly, you add latency and still miss boundary bugs. Built because this shows up all the time in tool calling and structured output pipelines. Use it between the token stream and your parser. The trick is simple: track strings, escapes, and nesting depth so you know the exact moment the first full JSON payload is complete.
================================================================================
"""
