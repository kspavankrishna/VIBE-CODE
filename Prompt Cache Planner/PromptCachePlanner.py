#!/usr/bin/env python3
"""Plan cache-friendly prompt prefixes and explain cache misses for LLM requests.

PromptCachePlanner helps teams using OpenAI Responses/Chat, Anthropic Messages,
Gemini, and generic JSON payloads keep long-lived prompt prefixes stable. It
splits a request into a candidate cacheable prefix and a live suffix, extracts
volatile metadata, computes deterministic hashes, and compares two requests to
show why prompt caching hit or missed.
"""
from __future__ import annotations

import argparse
import copy
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from email.utils import parsedate_to_datetime
import hashlib
import json
import re
import sys
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qsl, urlparse

PathPart = str | int
JsonValue = Any

USERISH_ROLES = frozenset({"user"})
PROMPT_TEXT_KEYS = frozenset(
    {
        "text",
        "content",
        "prompt",
        "instructions",
        "system",
        "developer",
        "input_text",
        "output_text",
        "reasoning",
        "thinking",
        "query",
    }
)
PROMPT_PAYLOAD_SEGMENTS = frozenset({"messages", "input", "contents", "parts", "attachments"})
VOLATILE_FIELD_NAMES = frozenset(
    {
        "request_id",
        "trace_id",
        "span_id",
        "parent_span_id",
        "conversation_id",
        "session_id",
        "run_id",
        "response_id",
        "message_id",
        "completion_id",
        "tool_call_id",
        "call_id",
        "invocation_id",
        "attempt_id",
        "nonce",
        "seed",
        "timestamp",
        "ts",
        "generated_at",
        "started_at",
        "ended_at",
        "created_at",
        "updated_at",
        "expires_at",
        "deadline",
        "retry_after",
        "signature",
        "signed_url",
        "temporary_url",
        "temp_url",
        "presigned_url",
    }
)
VOLATILE_SUBSTRINGS = (
    "timestamp",
    "trace",
    "nonce",
    "signature",
    "signed",
    "expiry",
    "expires",
    "deadline",
    "request_id",
    "response_id",
    "conversation",
    "session",
    "tool_call",
    "attempt",
    "span_id",
)
SCHEMA_CONTEXT_NAMES = frozenset(
    {"schema", "json_schema", "input_schema", "output_schema", "parameters", "properties", "definitions", "$defs", "response_format"}
)
METADATA_CONTEXT_NAMES = frozenset(
    {"metadata", "request_metadata", "client_metadata", "debug", "telemetry", "trace", "headers", "http_headers", "query", "params"}
)
SIGNED_URL_QUERY_KEYS = frozenset(
    {
        "x-amz-algorithm",
        "x-amz-credential",
        "x-amz-date",
        "x-amz-expires",
        "x-amz-security-token",
        "x-amz-signature",
        "googleaccessid",
        "expires",
        "signature",
        "sig",
        "se",
        "sp",
        "spr",
        "sv",
        "sr",
        "skoid",
        "sktid",
    }
)
UUID_FULL_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
UUID_FIND_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"
)
ISO_TS_FULL_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
ISO_TS_FIND_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\b"
)

_OMIT = object()


@dataclass(frozen=True)
class PlannerConfig:
    preserve_last_user_turn: bool = True
    hash_algorithm: str = "sha256"
    max_preview_chars: int = 88
    max_diff_entries: int = 64
    stable_text_keys: frozenset[str] = PROMPT_TEXT_KEYS
    volatile_field_names: frozenset[str] = VOLATILE_FIELD_NAMES


@dataclass(frozen=True)
class VolatilityMatch:
    path: str
    reason: str
    preview: str
    moved_to_live_suffix: bool

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass(frozen=True)
class ProviderHint:
    title: str
    detail: str

    def to_dict(self) -> dict[str, str]:
        return dataclasses.asdict(self)


@dataclass(frozen=True)
class SplitStats:
    stable_message_count: int | None = None
    live_message_count: int | None = None
    split_field: str | None = None
    split_index: int | None = None


@dataclass(frozen=True)
class DiffEntry:
    path: str
    kind: str
    left: str | None
    right: str | None

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class CachePlan:
    provider: str
    shape: str
    stable_payload: JsonValue
    live_suffix: JsonValue
    stable_hash: str
    full_hash: str
    stable_size_bytes: int
    full_size_bytes: int
    volatility: list[VolatilityMatch] = field(default_factory=list)
    hints: list[ProviderHint] = field(default_factory=list)
    split_stats: SplitStats = field(default_factory=SplitStats)

    def to_dict(self, include_payloads: bool = True) -> dict[str, Any]:
        result = {
            "provider": self.provider,
            "shape": self.shape,
            "stable_hash": self.stable_hash,
            "full_hash": self.full_hash,
            "stable_size_bytes": self.stable_size_bytes,
            "full_size_bytes": self.full_size_bytes,
            "stable_message_count": self.split_stats.stable_message_count,
            "live_message_count": self.split_stats.live_message_count,
            "split_field": self.split_stats.split_field,
            "split_index": self.split_stats.split_index,
            "volatility": [item.to_dict() for item in self.volatility],
            "hints": [item.to_dict() for item in self.hints],
        }
        if include_payloads:
            result["stable_payload"] = self.stable_payload
            result["live_suffix"] = self.live_suffix
        return result

    def render_text(self, include_payloads: bool = False) -> str:
        lines = [
            f"Provider: {self.provider}",
            f"Shape: {self.shape}",
            f"Stable hash: {self.stable_hash}",
            f"Full hash: {self.full_hash}",
            f"Stable size: {self.stable_size_bytes} bytes",
            f"Full size: {self.full_size_bytes} bytes",
        ]
        if self.split_stats.split_field:
            lines.append(
                "Sequence split: "
                f"{self.split_stats.split_field} at index {self.split_stats.split_index}"
            )
        if self.split_stats.stable_message_count is not None:
            lines.append(
                "Stable/live items: "
                f"{self.split_stats.stable_message_count}/{self.split_stats.live_message_count}"
            )
        if self.volatility:
            lines.append("Volatile fields:")
            for item in self.volatility:
                movement = "moved to live suffix" if item.moved_to_live_suffix else "observed in place"
                lines.append(f"- {item.path}: {item.reason} ({movement})")
        else:
            lines.append("Volatile fields: none detected")
        if self.hints:
            lines.append("Suggestions:")
            for hint in self.hints:
                lines.append(f"- {hint.title}: {hint.detail}")
        if include_payloads:
            lines.append("Stable payload JSON:")
            lines.append(canonical_json(self.stable_payload, pretty=True))
            lines.append("Live suffix JSON:")
            lines.append(canonical_json(self.live_suffix, pretty=True))
        return "\n".join(lines)


@dataclass(frozen=True)
class CompareResult:
    left: CachePlan
    right: CachePlan
    stable_hash_equal: bool
    full_hash_equal: bool
    stable_diffs: list[DiffEntry]
    live_diffs: list[DiffEntry]
    diagnosis: list[str]

    def to_dict(self, include_payloads: bool = True) -> dict[str, Any]:
        return {
            "stable_hash_equal": self.stable_hash_equal,
            "full_hash_equal": self.full_hash_equal,
            "stable_diffs": [item.to_dict() for item in self.stable_diffs],
            "live_diffs": [item.to_dict() for item in self.live_diffs],
            "diagnosis": self.diagnosis,
            "left": self.left.to_dict(include_payloads=include_payloads),
            "right": self.right.to_dict(include_payloads=include_payloads),
        }

    def render_text(self, include_payloads: bool = False) -> str:
        lines = [
            f"Stable hash equal: {'yes' if self.stable_hash_equal else 'no'}",
            f"Full hash equal: {'yes' if self.full_hash_equal else 'no'}",
        ]
        if self.diagnosis:
            lines.append("Diagnosis:")
            for item in self.diagnosis:
                lines.append(f"- {item}")
        if self.stable_diffs:
            lines.append("Stable prefix diffs:")
            for item in self.stable_diffs:
                lines.append(f"- {item.path}: {item.kind}")
        if self.live_diffs:
            lines.append("Live suffix diffs:")
            for item in self.live_diffs:
                lines.append(f"- {item.path}: {item.kind}")
        if include_payloads:
            lines.append("Left plan:")
            lines.append(self.left.render_text(include_payloads=True))
            lines.append("Right plan:")
            lines.append(self.right.render_text(include_payloads=True))
        return "\n".join(lines)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def canonical_json(value: Any, *, pretty: bool = False) -> str:
    kwargs: dict[str, Any] = {"ensure_ascii": False, "sort_keys": True, "default": _json_default}
    if pretty:
        kwargs["indent"] = 2
    else:
        kwargs["separators"] = (",", ":")
    return json.dumps(value, **kwargs)


def digest_json(value: Any, algorithm: str) -> str:
    try:
        digest = hashlib.new(algorithm)
    except ValueError as exc:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}") from exc
    digest.update(canonical_json(value).encode("utf-8"))
    return f"{algorithm}:{digest.hexdigest()}"


def preview_value(value: Any, max_chars: int) -> str:
    if isinstance(value, str):
        text = value.strip().replace("\n", "\\n")
    else:
        text = canonical_json(value)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


def path_to_str(path: Sequence[PathPart]) -> str:
    if not path:
        return "$"
    result = "$"
    for part in path:
        if isinstance(part, int):
            result += f"[{part}]"
        else:
            result += f".{part}"
    return result


def path_segments_lower(path: Sequence[PathPart]) -> list[str]:
    return [part.lower() for part in path if isinstance(part, str)]


def tail_key(path: Sequence[PathPart]) -> str:
    if path and isinstance(path[-1], str):
        return path[-1].lower()
    return ""


def is_prompt_payload_context(path: Sequence[PathPart]) -> bool:
    return any(segment in PROMPT_PAYLOAD_SEGMENTS for segment in path_segments_lower(path))


def is_prompt_text_context(path: Sequence[PathPart], config: PlannerConfig) -> bool:
    segments = path_segments_lower(path)
    key = tail_key(path)
    if key in config.stable_text_keys:
        return True
    return len(segments) >= 2 and segments[-2] == "parts" and key == "text"


def is_schema_context(path: Sequence[PathPart]) -> bool:
    return any(segment in SCHEMA_CONTEXT_NAMES for segment in path_segments_lower(path))


def is_metadata_context(path: Sequence[PathPart]) -> bool:
    return any(segment in METADATA_CONTEXT_NAMES for segment in path_segments_lower(path))


def looks_like_uuid(value: Any) -> bool:
    return isinstance(value, str) and bool(UUID_FULL_RE.match(value.strip()))


def looks_like_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    candidate = value.strip()
    if ISO_TS_FULL_RE.match(candidate):
        return True
    try:
        parsedate_to_datetime(candidate)
    except (TypeError, ValueError, IndexError, OverflowError):
        return False
    return True


def looks_like_epoch(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    absolute = abs(float(value))
    return 1_500_000_000 <= absolute <= 2_500_000_000 or 1_500_000_000_000 <= absolute <= 2_500_000_000_000


def looks_like_signed_url(value: Any) -> bool:
    if not isinstance(value, str) or "://" not in value:
        return False
    try:
        parsed = urlparse(value)
    except ValueError:
        return False
    if not parsed.scheme or not parsed.netloc:
        return False
    query_keys = {key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
    if query_keys & SIGNED_URL_QUERY_KEYS:
        return True
    return any(key.startswith("x-amz-") or key.startswith("x-goog-") for key in query_keys)


def looks_like_entropy_token(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    candidate = value.strip()
    if len(candidate) < 24 or " " in candidate:
        return False
    has_lower = any(char.islower() for char in candidate)
    has_upper = any(char.isupper() for char in candidate)
    has_digit = any(char.isdigit() for char in candidate)
    has_symbol = any(char in "-_./+=" for char in candidate)
    buckets = sum((has_lower, has_upper, has_digit, has_symbol))
    return buckets >= 2 and any(char.isalnum() for char in candidate)


def contains_inline_volatility(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    if UUID_FIND_RE.search(value):
        return True
    if ISO_TS_FIND_RE.search(value):
        return True
    if "X-Amz-Signature=" in value or "x-amz-signature=" in value:
        return True
    lowered = value.lower()
    return "request_id=" in lowered or "trace_id=" in lowered or "session_id=" in lowered


def detect_provider(request: Any) -> tuple[str, str]:
    if not isinstance(request, Mapping):
        return "generic-json", "scalar-or-array"
    keys = {str(key).lower() for key in request.keys()}
    if "contents" in keys:
        return "google-gemini", "google-gemini-generate-content"
    if "input" in keys:
        return "openai-responses", "openai-responses"
    if "messages" in keys and ("anthropic-version" in keys or "anthropic_version" in keys):
        return "anthropic-messages", "anthropic-messages"
    if "messages" in keys and "system" in keys and "max_tokens" in keys and "model" in keys:
        return "anthropic-messages", "anthropic-messages"
    if "messages" in keys:
        return "openai-chat-like", "chat-completions-like"
    return "generic-json", "generic-json"


def extract_role(item: Any) -> str:
    if not isinstance(item, Mapping):
        return ""
    role = item.get("role")
    if isinstance(role, str):
        return role.lower()
    return ""


def split_sequence_before_latest_user(items: Sequence[Any]) -> tuple[list[Any], list[Any], int | None]:
    latest_user_index: int | None = None
    for index, item in enumerate(items):
        if extract_role(item) in USERISH_ROLES:
            latest_user_index = index
    if latest_user_index is None:
        return list(items), [], None
    return list(items[:latest_user_index]), list(items[latest_user_index:]), latest_user_index


def split_request_payload(
    request: Any,
    provider: str,
    config: PlannerConfig,
) -> tuple[Any, Any, SplitStats]:
    if not isinstance(request, Mapping):
        return copy.deepcopy(request), {}, SplitStats()

    stable = copy.deepcopy(dict(request))
    live: dict[str, Any] = {}
    if not config.preserve_last_user_turn:
        return stable, live, SplitStats()

    sequence_field: str | None = None
    sequence_value: Any = None
    if provider == "google-gemini" and isinstance(request.get("contents"), list):
        sequence_field = "contents"
        sequence_value = request.get("contents")
    elif provider == "openai-responses" and isinstance(request.get("input"), list):
        sequence_field = "input"
        sequence_value = request.get("input")
    elif isinstance(request.get("messages"), list):
        sequence_field = "messages"
        sequence_value = request.get("messages")

    if sequence_field is None or not isinstance(sequence_value, list):
        return stable, live, SplitStats()

    stable_items, live_items, split_index = split_sequence_before_latest_user(sequence_value)
    stable[sequence_field] = stable_items
    if live_items:
        live[sequence_field] = live_items
    stats = SplitStats(
        stable_message_count=len(stable_items),
        live_message_count=len(live_items),
        split_field=sequence_field,
        split_index=split_index,
    )
    return stable, live, stats


def detect_volatility(
    path: Sequence[PathPart],
    value: Any,
    config: PlannerConfig,
) -> VolatilityMatch | None:
    key = tail_key(path)
    if not key:
        return None

    prompt_payload = is_prompt_payload_context(path)
    prompt_text = is_prompt_text_context(path, config)
    schema_context = is_schema_context(path)
    metadata_context = is_metadata_context(path)
    move_allowed = not prompt_payload and not prompt_text and not schema_context

    reason: str | None = None
    moved = False

    if key in config.volatile_field_names and not schema_context:
        reason = f"volatile field name `{key}`"
        moved = move_allowed
    elif any(fragment in key for fragment in VOLATILE_SUBSTRINGS) and not schema_context:
        reason = f"field name `{key}` looks request-scoped"
        moved = move_allowed
    elif isinstance(value, str) and looks_like_signed_url(value):
        reason = "signed or expiring URL"
        moved = move_allowed
    elif isinstance(value, str) and looks_like_uuid(value) and (metadata_context or key.endswith("_id") or key == "id"):
        reason = "request-scoped identifier"
        moved = move_allowed
    elif looks_like_epoch(value) and (metadata_context or key.endswith("at") or "time" in key or key == "ts"):
        reason = "epoch-like timestamp"
        moved = move_allowed
    elif isinstance(value, str) and looks_like_timestamp(value) and (metadata_context or key.endswith("at") or "time" in key or key == "ts"):
        reason = "timestamp-like value"
        moved = move_allowed
    elif isinstance(value, str) and looks_like_entropy_token(value) and any(
        fragment in key for fragment in ("token", "sig", "signature", "secret", "nonce", "session")
    ):
        reason = "high-entropy session token"
        moved = move_allowed
    elif prompt_text and contains_inline_volatility(value):
        reason = "volatile value embedded in prompt text"
        moved = False

    if reason is None:
        return None

    return VolatilityMatch(
        path=path_to_str(path),
        reason=reason,
        preview=preview_value(value, config.max_preview_chars),
        moved_to_live_suffix=moved,
    )


def separate_volatile_fields(
    value: Any,
    path: Sequence[PathPart],
    config: PlannerConfig,
) -> tuple[Any, Any, list[VolatilityMatch]]:
    if isinstance(value, Mapping):
        stable: dict[str, Any] = {}
        overlay: dict[str, Any] = {}
        matches: list[VolatilityMatch] = []
        for key, child in value.items():
            child_path = tuple(path) + (str(key),)
            stable_child, overlay_child, child_matches = separate_volatile_fields(child, child_path, config)
            if stable_child is not _OMIT:
                stable[str(key)] = stable_child
            if overlay_child is not _OMIT:
                overlay[str(key)] = overlay_child
            matches.extend(child_matches)
        return stable, overlay if overlay else _OMIT, matches

    if isinstance(value, list):
        stable_list: list[Any] = []
        overlay_list: list[Any] = []
        any_overlay = False
        matches: list[VolatilityMatch] = []
        for index, child in enumerate(value):
            child_path = tuple(path) + (index,)
            stable_child, overlay_child, child_matches = separate_volatile_fields(child, child_path, config)
            stable_list.append(child if stable_child is _OMIT else stable_child)
            if overlay_child is _OMIT:
                overlay_list.append(None)
            else:
                overlay_list.append(overlay_child)
                any_overlay = True
            matches.extend(child_matches)
        return stable_list, overlay_list if any_overlay else _OMIT, matches

    match = detect_volatility(path, value, config)
    if match is None:
        return value, _OMIT, []

    if match.moved_to_live_suffix and path and isinstance(path[-1], str):
        return _OMIT, value, [match]
    return value, _OMIT, [match]


def overlay_json(base: Any, overlay: Any) -> Any:
    if overlay is _OMIT or overlay is None:
        return copy.deepcopy(base)

    if isinstance(overlay, Mapping):
        result = copy.deepcopy(base) if isinstance(base, Mapping) else {}
        for key, value in overlay.items():
            if value is None:
                continue
            existing = result.get(key) if isinstance(result, dict) else None
            result[key] = overlay_json(existing, value)
        return result

    if isinstance(overlay, list):
        result = copy.deepcopy(base) if isinstance(base, list) else []
        if len(result) < len(overlay):
            result.extend([None] * (len(overlay) - len(result)))
        for index, value in enumerate(overlay):
            if value is None:
                continue
            existing = result[index] if index < len(result) else None
            result[index] = overlay_json(existing, value)
        return result

    return copy.deepcopy(overlay)


def extract_tool_names(request: Any) -> list[str]:
    if not isinstance(request, Mapping):
        return []
    tools = request.get("tools")
    if not isinstance(tools, list):
        return []
    names: list[str] = []
    for tool in tools:
        if not isinstance(tool, Mapping):
            continue
        name = tool.get("name")
        if isinstance(name, str):
            names.append(name)
            continue
        function = tool.get("function")
        if isinstance(function, Mapping):
            function_name = function.get("name")
            if isinstance(function_name, str):
                names.append(function_name)
                continue
        declarations = tool.get("functionDeclarations")
        if isinstance(declarations, list):
            for declaration in declarations:
                if isinstance(declaration, Mapping) and isinstance(declaration.get("name"), str):
                    names.append(declaration["name"])
    return names


def build_hints(
    request: Any,
    plan: CachePlan,
) -> list[ProviderHint]:
    hints: list[ProviderHint] = []

    if plan.split_stats.live_message_count:
        hints.append(
            ProviderHint(
                "Keep the newest turn live",
                "The planner split the conversational sequence before the latest user turn so the earlier context can stay reusable.",
            )
        )

    if any(match.reason == "signed or expiring URL" for match in plan.volatility):
        hints.append(
            ProviderHint(
                "Stop caching signed asset URLs",
                "Signed blob, S3, GCS, or Azure URLs change even when the underlying asset does not. Keep a stable asset identifier in the prefix and resolve fresh URLs at send time.",
            )
        )

    if any(
        "timestamp" in match.reason and not match.moved_to_live_suffix
        for match in plan.volatility
    ):
        hints.append(
            ProviderHint(
                "Remove timestamps from prompt text",
                "A timestamp inside a message body looks harmless but it changes the token sequence every call and destroys prompt-prefix reuse.",
            )
        )

    tool_names = extract_tool_names(request)
    if len(tool_names) > 1 and tool_names != sorted(tool_names, key=str.lower):
        hints.append(
            ProviderHint(
                "Sort tools before serialization",
                "Tool order affects the exact token prefix. Emit tools in a deterministic order, usually by function name, so semantically identical requests hash the same way.",
            )
        )

    if len(tool_names) != len(set(tool_names)):
        hints.append(
            ProviderHint(
                "Deduplicate repeated tool schemas",
                "Repeated tools often come from layered builders and they waste prefix budget. Reuse one canonical schema per tool name.",
            )
        )

    if plan.stable_size_bytes >= 8_192:
        hints.append(
            ProviderHint(
                "Big enough to justify caching",
                "This stable prefix is large enough that even moderate cache-hit rates should pay back in latency and cost.",
            )
        )

    if plan.provider == "openai-responses" or plan.provider == "openai-chat-like":
        hints.append(
            ProviderHint(
                "Keep request metadata outside the OpenAI prefix",
                "OpenAI prompt caching depends on exact token prefixes. Request IDs, telemetry, signed URLs, and changing trace fields should stay outside the cacheable body.",
            )
        )
    elif plan.provider == "anthropic-messages":
        hints.append(
            ProviderHint(
                "Mark stable Anthropic blocks explicitly",
                "System instructions, tool schemas, and large retrieved context work best when you emit them as stable blocks with cache controls instead of rebuilding them every turn.",
            )
        )
    elif plan.provider == "google-gemini":
        hints.append(
            ProviderHint(
                "Materialize cachedContent for Gemini",
                "Long-lived system context and retrieved documents are usually better as a reusable cached content resource than as inline text rebuilt on every call.",
            )
        )

    deduped: list[ProviderHint] = []
    seen_titles: set[str] = set()
    for hint in hints:
        if hint.title in seen_titles:
            continue
        seen_titles.add(hint.title)
        deduped.append(hint)
    return deduped


def plan_request(
    request: Any,
    *,
    config: PlannerConfig | None = None,
    provider_override: str | None = None,
) -> CachePlan:
    config = config or PlannerConfig()
    detected_provider, detected_shape = detect_provider(request)
    provider = provider_override or detected_provider
    shape = provider_override or detected_shape

    stable_candidate, live_candidate, split_stats = split_request_payload(request, provider, config)
    stable_payload, volatile_overlay, volatility = separate_volatile_fields(stable_candidate, (), config)
    live_suffix = overlay_json(live_candidate, volatile_overlay)

    stable_hash = digest_json(stable_payload, config.hash_algorithm)
    full_hash = digest_json(request, config.hash_algorithm)
    plan = CachePlan(
        provider=provider,
        shape=shape,
        stable_payload=stable_payload,
        live_suffix=live_suffix,
        stable_hash=stable_hash,
        full_hash=full_hash,
        stable_size_bytes=len(canonical_json(stable_payload).encode("utf-8")),
        full_size_bytes=len(canonical_json(request).encode("utf-8")),
        volatility=volatility,
        split_stats=split_stats,
    )
    plan.hints = build_hints(request, plan)
    return plan


def diff_json(
    left: Any,
    right: Any,
    *,
    limit: int,
    path: Sequence[PathPart] = (),
    output: list[DiffEntry] | None = None,
) -> list[DiffEntry]:
    if output is None:
        output = []
    if len(output) >= limit:
        return output

    if type(left) is not type(right):
        output.append(
            DiffEntry(
                path=path_to_str(path),
                kind="type_changed",
                left=preview_value(left, 72),
                right=preview_value(right, 72),
            )
        )
        return output

    if isinstance(left, Mapping):
        left_keys = set(left.keys())
        right_keys = set(right.keys())
        for key in sorted(left_keys | right_keys, key=str):
            if len(output) >= limit:
                break
            child_path = tuple(path) + (str(key),)
            if key not in right:
                output.append(
                    DiffEntry(
                        path=path_to_str(child_path),
                        kind="removed",
                        left=preview_value(left[key], 72),
                        right=None,
                    )
                )
            elif key not in left:
                output.append(
                    DiffEntry(
                        path=path_to_str(child_path),
                        kind="added",
                        left=None,
                        right=preview_value(right[key], 72),
                    )
                )
            else:
                diff_json(left[key], right[key], limit=limit, path=child_path, output=output)
        return output

    if isinstance(left, list):
        if len(left) != len(right):
            output.append(
                DiffEntry(
                    path=path_to_str(path),
                    kind="length_changed",
                    left=str(len(left)),
                    right=str(len(right)),
                )
            )
        common = min(len(left), len(right))
        for index in range(common):
            if len(output) >= limit:
                break
            diff_json(left[index], right[index], limit=limit, path=tuple(path) + (index,), output=output)
        for index in range(common, len(left)):
            if len(output) >= limit:
                break
            output.append(
                DiffEntry(
                    path=path_to_str(tuple(path) + (index,)),
                    kind="removed",
                    left=preview_value(left[index], 72),
                    right=None,
                )
            )
        for index in range(common, len(right)):
            if len(output) >= limit:
                break
            output.append(
                DiffEntry(
                    path=path_to_str(tuple(path) + (index,)),
                    kind="added",
                    left=None,
                    right=preview_value(right[index], 72),
                )
            )
        return output

    if left != right:
        output.append(
            DiffEntry(
                path=path_to_str(path),
                kind="changed",
                left=preview_value(left, 72),
                right=preview_value(right, 72),
            )
        )
    return output


def compare_requests(
    left: Any,
    right: Any,
    *,
    config: PlannerConfig | None = None,
    provider_override: str | None = None,
) -> CompareResult:
    config = config or PlannerConfig()
    left_plan = plan_request(left, config=config, provider_override=provider_override)
    right_plan = plan_request(right, config=config, provider_override=provider_override)

    stable_diffs = diff_json(
        left_plan.stable_payload,
        right_plan.stable_payload,
        limit=config.max_diff_entries,
    )
    live_diffs = diff_json(
        left_plan.live_suffix,
        right_plan.live_suffix,
        limit=config.max_diff_entries,
    )

    diagnosis: list[str] = []
    stable_equal = left_plan.stable_hash == right_plan.stable_hash
    full_equal = left_plan.full_hash == right_plan.full_hash

    if stable_equal and not full_equal:
        diagnosis.append("The cacheable prefix stayed stable. Only the live suffix changed.")
    elif not stable_equal:
        diagnosis.append("The stable prefix changed, so exact prompt-cache reuse will miss.")

    if left_plan.provider != right_plan.provider:
        diagnosis.append("The provider shape changed between requests, so the hashes are not directly comparable one-to-one.")

    if any(match.reason == "signed or expiring URL" for match in left_plan.volatility + right_plan.volatility):
        diagnosis.append("Signed URLs are present. Stable asset IDs or object references are usually safer than presigned links inside the prefix.")

    if any("timestamp" in match.reason for match in left_plan.volatility + right_plan.volatility):
        diagnosis.append("Timestamps are part of the request. Even a single changing clock value is enough to invalidate the prefix.")

    if stable_equal and not live_diffs:
        diagnosis.append("Both the stable prefix and the live suffix are identical. Any cache miss is probably outside this request JSON, such as model versioning or provider-side cache expiry.")

    return CompareResult(
        left=left_plan,
        right=right_plan,
        stable_hash_equal=stable_equal,
        full_hash_equal=full_equal,
        stable_diffs=stable_diffs,
        live_diffs=live_diffs,
        diagnosis=diagnosis,
    )


def load_json_input(path: str) -> Any:
    if path == "-":
        text = sys.stdin.read()
    else:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    return json.loads(text)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plan cache-friendly LLM request prefixes and explain prompt-cache misses."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="Split one request into stable prefix and live suffix.")
    plan_parser.add_argument("request", help="Path to a JSON request file, or - for stdin.")
    plan_parser.add_argument("--json", action="store_true", dest="as_json", help="Emit machine-readable JSON.")
    plan_parser.add_argument(
        "--include-payloads",
        action="store_true",
        help="Include the stable payload and live suffix in the output.",
    )
    plan_parser.add_argument(
        "--hash",
        default="sha256",
        help="Hash algorithm for stable and full request digests. Default: sha256.",
    )
    plan_parser.add_argument(
        "--provider",
        default=None,
        help="Optional provider override. Example: openai-responses or google-gemini.",
    )

    compare_parser = subparsers.add_parser("compare", help="Compare two requests and explain what changed.")
    compare_parser.add_argument("left", help="First JSON request file, or - for stdin.")
    compare_parser.add_argument("right", help="Second JSON request file.")
    compare_parser.add_argument("--json", action="store_true", dest="as_json", help="Emit machine-readable JSON.")
    compare_parser.add_argument(
        "--include-payloads",
        action="store_true",
        help="Include the underlying plans in the output.",
    )
    compare_parser.add_argument(
        "--hash",
        default="sha256",
        help="Hash algorithm for stable and full request digests. Default: sha256.",
    )
    compare_parser.add_argument(
        "--provider",
        default=None,
        help="Optional provider override. Example: anthropic-messages.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = PlannerConfig(hash_algorithm=args.hash)

    try:
        if args.command == "plan":
            request = load_json_input(args.request)
            plan = plan_request(request, config=config, provider_override=args.provider)
            if args.as_json:
                print(json.dumps(plan.to_dict(include_payloads=args.include_payloads), ensure_ascii=False, indent=2))
            else:
                print(plan.render_text(include_payloads=args.include_payloads))
            return 0

        if args.command == "compare":
            left = load_json_input(args.left)
            right = load_json_input(args.right)
            result = compare_requests(left, right, config=config, provider_override=args.provider)
            if args.as_json:
                print(json.dumps(result.to_dict(include_payloads=args.include_payloads), ensure_ascii=False, indent=2))
            else:
                print(result.render_text(include_payloads=args.include_payloads))
            return 0

        parser.error(f"Unknown command: {args.command}")
        return 2
    except json.JSONDecodeError as exc:
        print(f"JSON parse error: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"I/O error: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1


__all__ = [
    "CachePlan",
    "CompareResult",
    "DiffEntry",
    "PlannerConfig",
    "ProviderHint",
    "SplitStats",
    "VolatilityMatch",
    "compare_requests",
    "plan_request",
]


if __name__ == "__main__":
    raise SystemExit(main())


# This solves the April 2026 prompt caching problem that shows up when teams move
# fast with OpenAI Responses, Anthropic Messages, Gemini, or mixed provider
# request builders and then wonder why cache-hit rate is bad even though the
# prompts “look the same”. In practice, they are usually not the same. A request
# ID, signed blob URL, timestamp, tool order shuffle, or one extra debug field is
# enough to change the token prefix and throw away the reuse you expected. This
# file gives you a concrete planner instead of guesswork: it splits one request
# into a candidate stable prefix and a live suffix, hashes both parts, surfaces
# volatile fields, and compares two requests so you can see the exact reason the
# prefix changed.
#
# Built because prompt caching is one of those features that sounds simple in a
# product note and then gets messy in a real codebase. Teams compose requests from
# middleware, tracing layers, RAG pipelines, tool registries, and web handlers.
# By the time the JSON leaves the process, it often contains session-scoped IDs,
# expiring asset URLs, reordered tool definitions, or timestamps embedded inside
# message text. Those mistakes are expensive because they increase latency and
# model spend at the same time, and they are hard to spot by eye when the request
# body is large. I wanted something production-friendly that a backend engineer,
# platform engineer, or researcher can run directly on captured request JSON and
# use during incident response or optimization work.
#
# Use it when you are debugging low prompt-cache reuse, validating a refactor in a
# shared LLM client, or trying to decide what should live in a stable prefix versus
# what should stay request-local. It is especially useful for AI agents, RAG
# systems, eval runners, copilots, and multi-tenant orchestration services where
# small request-shape drift happens all the time. The `plan` command gives you a
# stable-prefix candidate, a live suffix, size numbers, deterministic hashes, and
# field-level volatility notes. The `compare` command is for before-and-after
# analysis when one deploy suddenly tanks cache-hit rate and you need a straight
# answer quickly.
#
# The trick: this module stays conservative about what it rewrites automatically.
# It will move obvious metadata-like volatility such as request IDs, trace IDs,
# or top-level signed URLs into the live suffix, but it does not pretend it can
# safely rewrite the meaning of your prompt text. If a timestamp or presigned URL
# is embedded inside a message body, the planner calls that out as an observed
# cache breaker instead of silently mutating the prompt. That makes the output
# useful in production because it tells you what to fix in the request builder
# rather than hiding the problem with an unsafe transformation. It also uses one
# deterministic JSON representation for hashing so teams can compare request shape
# drift in CI, local debugging, and recorded traces with the same result.
#
# Drop this into a repo as `PromptCachePlanner.py` when you need a standalone,
# search-friendly, senior-level Python utility for LLM prompt caching analysis,
# OpenAI prompt cache debugging, Anthropic cache control planning, Gemini cached
# content planning, stable tool schema ordering, signed URL cache bust detection,
# and request prefix optimization. Run `python PromptCachePlanner.py plan
# request.json --json` to inspect one request, or `python PromptCachePlanner.py
# compare before.json after.json` to explain why a deploy changed cache behavior.
# The whole point is to give you a practical prompt cache planner for real AI
# systems work, not another vague note about “keep prompts stable”.
