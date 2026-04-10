import hashlib
import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any


class TraceSecretScrubber:
    SECRET_KEYS = {
        "api_key", "apikey", "token", "secret", "password", "passwd",
        "authorization", "cookie", "session", "access_token", "refresh_token"
    }

    VALUE_PATTERNS = [
        re.compile(r"\b(sk|rk|pk)_[A-Za-z0-9]{20,}\b"),
        re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
        re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
        re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9._-]+\.[A-Za-z0-9._-]+\b"),
        re.compile(r"-----BEGIN [A-Z ]+PRIVATE KEY-----[\s\S]+?-----END [A-Z ]+PRIVATE KEY-----"),
        re.compile(r"(?i)(bearer\s+)([A-Za-z0-9._-]{16,})"),
        re.compile(r'(?i)(token|secret|password|api[_-]?key)\s*[:=]\s*(["\']?)([^\s,"\']{8,})(\2)')
    ]

    def __init__(self, salt: str = "trace-scrubber") -> None:
        self.salt = salt
        self.cache: dict[str, str] = {}

    def scrub(self, payload: Any) -> Any:
        if isinstance(payload, Mapping):
            return {k: self._scrub_value(k, v) for k, v in payload.items()}
        if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
            return [self.scrub(item) for item in payload]
        if isinstance(payload, str):
            return self._scrub_text(payload)
        return payload

    def _scrub_value(self, key: str, value: Any) -> Any:
        if key.lower() in self.SECRET_KEYS:
            return self._mask(str(value), f"key:{key.lower()}")
        return self.scrub(value)

    def _scrub_text(self, text: str) -> str:
        scrubbed = text
        for pattern in self.VALUE_PATTERNS:
            scrubbed = pattern.sub(self._replace_match, scrubbed)
        return self._mask_entropy_tokens(scrubbed)

    def _replace_match(self, match: re.Match[str]) -> str:
        groups = match.groups()
        if len(groups) == 4:
            return f"{groups[0]}={self._mask(groups[2], groups[0].lower())}"
        if len(groups) == 2 and str(groups[0]).lower().startswith("bearer"):
            return f"{groups[0]}{self._mask(groups[1], 'bearer')}"
        return self._mask(match.group(0), "pattern")

    def _mask_entropy_tokens(self, text: str) -> str:
        def replace(match: re.Match[str]) -> str:
            token = match.group(0)
            if len(token) < 20 or not self._looks_sensitive(token):
                return token
            return self._mask(token, "entropy")

        return re.sub(r"[A-Za-z0-9+/=_\-.]{20,}", replace, text)

    def _looks_sensitive(self, token: str) -> bool:
        charset = len(set(token))
        if charset < 8:
            return False
        return self._entropy(token) >= 3.6

    def _entropy(self, value: str) -> float:
        counts = Counter(value)
        length = len(value)
        return -sum((count / length) * math.log2(count / length) for count in counts.values())

    def _mask(self, raw: str, label: str) -> str:
        if raw in self.cache:
            return self.cache[raw]
        digest = hashlib.sha256(f"{self.salt}:{raw}".encode()).hexdigest()[:10]
        masked = f"<redacted:{label}:{digest}>"
        self.cache[raw] = masked
        return masked


if __name__ == "__main__":
    sample = {
        "provider": "openai",
        "api_key": "demo_key_value_that_should_never_leave_the_process",
        "headers": {"Authorization": "Bearer demo.jwt.token.value.for.local.testing.only"},
        "prompt": "call tool with token=demo_token_value_for_safe_testing_only and keep going",
        "nested": [
            "temporary key KEY1234567890safeExampleValue for local test",
            {"cookie": "sessionid=safeSyntheticCookieValue1234567890"}
        ]
    }
    print(TraceSecretScrubber().scrub(sample))

"""
================================================================================
EXPLANATION
This solves secret leakage in logs, traces, and LLM telemetry. Built because teams now dump prompts, tool calls, headers, and retries into observability systems, then realize too late that keys and bearer tokens went with them. Use it before shipping data to Langfuse, OpenTelemetry, Datadog, or plain app logs. The trick is simple: catch known token shapes, catch suspicious high entropy strings, and replace them with stable masks so debugging still works.
================================================================================
"""
