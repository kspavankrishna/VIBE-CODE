import json
import re
from typing import Any


class StructuredJsonRepair:
    FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)
    KEY_RE = re.compile(r'([\{\[,]\s*)([A-Za-z_][A-Za-z0-9_\-]*)(\s*:)')
    SQ_VALUE_RE = re.compile(r":\s*'([^'\\]*(?:\\.[^'\\]*)*)'")
    SQ_KEY_RE = re.compile(r"'([A-Za-z_][A-Za-z0-9_\-]*)'\s*:")
    TRAILING_COMMA_RE = re.compile(r",\s*([}\]])")

    @classmethod
    def loads(cls, text: str) -> Any:
        candidate = cls._extract_candidate(text)
        attempts = [
            cls._normalize_quotes(candidate),
            cls._strip_comments(cls._normalize_quotes(candidate)),
        ]

        for raw in list(attempts):
            attempts.extend([
                cls._fix_python_literals(raw),
                cls._quote_keys(cls._fix_python_literals(raw)),
                cls._fix_single_quotes(cls._quote_keys(cls._fix_python_literals(raw))),
                cls._remove_trailing_commas(
                    cls._fix_single_quotes(cls._quote_keys(cls._fix_python_literals(raw)))
                ),
            ])

        seen = set()
        for attempt in attempts:
            if attempt in seen:
                continue
            seen.add(attempt)
            try:
                return json.loads(attempt)
            except json.JSONDecodeError:
                pass

        raise ValueError(f"Could not repair JSON payload: {candidate[:280]}")

    @classmethod
    def dumps(cls, text: str, *, indent: int = 2) -> str:
        return json.dumps(cls.loads(text), indent=indent, ensure_ascii=False, sort_keys=True)

    @classmethod
    def _extract_candidate(cls, text: str) -> str:
        text = text.strip().lstrip("\ufeff")
        fence = cls.FENCE_RE.search(text)
        if fence:
            return fence.group(1).strip()

        start = min((i for i in [text.find("{"), text.find("[")] if i >= 0), default=-1)
        if start < 0:
            return text

        stack, in_string, escape, quote = [], False, False, ""
        for idx, ch in enumerate(text[start:], start=start):
            if in_string:
                escape = ch == "\\" and not escape
                if ch == quote and not escape:
                    in_string = False
                elif ch != "\\":
                    escape = False
                continue
            if ch in ('"', "'"):
                in_string, quote = True, ch
            elif ch in "[{":
                stack.append("}" if ch == "{" else "]")
            elif ch in "]}":
                if stack and ch == stack[-1]:
                    stack.pop()
                    if not stack:
                        return text[start:idx + 1]
        return text[start:]

    @staticmethod
    def _normalize_quotes(text: str) -> str:
        return (text.replace("“", '"').replace("”", '"')
                    .replace("’", "'").replace("‘", "'"))

    @staticmethod
    def _strip_comments(text: str) -> str:
        text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
        lines = []
        for line in text.splitlines():
            cut, in_string, escape, quote = len(line), False, False, ""
            for i, ch in enumerate(line):
                if in_string:
                    escape = ch == "\\" and not escape
                    if ch == quote and not escape:
                        in_string = False
                    elif ch != "\\":
                        escape = False
                    continue
                if ch in ('"', "'"):
                    in_string, quote = True, ch
                elif ch == "/" and i + 1 < len(line) and line[i + 1] == "/":
                    cut = i
                    break
            lines.append(line[:cut])
        return "\n".join(lines)

    @classmethod
    def _fix_python_literals(cls, text: str) -> str:
        text = re.sub(r"\bTrue\b", "true", text)
        text = re.sub(r"\bFalse\b", "false", text)
        return re.sub(r"\bNone\b", "null", text)

    @classmethod
    def _quote_keys(cls, text: str) -> str:
        while True:
            updated = cls.KEY_RE.sub(r'\1"\2"\3', text)
            if updated == text:
                return updated
            text = updated

    @classmethod
    def _fix_single_quotes(cls, text: str) -> str:
        text = cls.SQ_KEY_RE.sub(r'"\1":', text)
        return cls.SQ_VALUE_RE.sub(lambda m: ': ' + json.dumps(m.group(1)), text)

    @classmethod
    def _remove_trailing_commas(cls, text: str) -> str:
        while True:
            updated = cls.TRAILING_COMMA_RE.sub(r"\1", text)
            if updated == text:
                return updated
            text = updated


if __name__ == "__main__":
    messy = """
    model said:
    ```json
    {
      user: 'Pavan',
      active: True,
      tags: ['llm', 'ops',], // trailing comma
      profile: {city: 'Bangalore', notes: None,},
    }
    ```
    """
    print(StructuredJsonRepair.dumps(messy))

"""
================================================================================
EXPLANATION
This fixes one of the most annoying real failures in LLM apps: the model gives you almost JSON, then your pipeline blows up on a fence, comment, bare key, Python boolean, or trailing comma. Built because retrying the model is expensive and still not reliable. Use it between model output and your schema validation step. The trick is deterministic repair in small passes, not guesswork, so you can recover common breakage patterns without hiding bigger structural errors.
================================================================================
"""
