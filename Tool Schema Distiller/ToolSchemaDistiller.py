#!/usr/bin/env python3
from __future__ import annotations

"""
ToolSchemaDistiller

Query-aware distillation for MCP and OpenAPI-style tool catalogs.

Large tool manifests are common in 2026 agent stacks. That is fine for runtime
validation, but it is often wasteful at prompt time. This module selects the
most relevant tools for the current user task and compresses their JSON Schema
payloads so a model sees the parts that matter most: tool names, short
intent-rich descriptions, required fields, shape, enums, formats, and bounds.

The distilled manifest is meant for prompt-time tool selection. Keep the
original schema for runtime validation on the server side.

Example:
    python ToolSchemaDistiller.py \
      --input tools.json \
      --query "search recent GitHub pull requests and summarize comments" \
      --budget 2800
"""

import argparse
import hashlib
import json
import math
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

WORD_RE = re.compile(r"[A-Za-z0-9_]+")
SENTENCE_RE = re.compile(r"(?<=[.!?])\s+")

STOPWORDS = {
    "a",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "been",
    "before",
    "between",
    "both",
    "by",
    "can",
    "could",
    "did",
    "do",
    "does",
    "doing",
    "done",
    "for",
    "from",
    "get",
    "give",
    "got",
    "had",
    "has",
    "have",
    "help",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "just",
    "let",
    "make",
    "maybe",
    "me",
    "more",
    "most",
    "my",
    "need",
    "now",
    "of",
    "on",
    "or",
    "our",
    "out",
    "over",
    "please",
    "run",
    "set",
    "should",
    "show",
    "so",
    "some",
    "than",
    "that",
    "the",
    "their",
    "them",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "to",
    "too",
    "up",
    "use",
    "using",
    "want",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "why",
    "will",
    "with",
    "would",
    "you",
    "your",
}

PRIMITIVE_CONSTRAINT_KEYS = (
    "format",
    "pattern",
    "const",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
    "contentEncoding",
    "contentMediaType",
)

OBJECT_CONSTRAINT_KEYS = (
    "minProperties",
    "maxProperties",
    "additionalProperties",
)

MISSING = object()


@dataclass(frozen=True)
class DistillConfig:
    total_budget_tokens: int = 3200
    max_tools: int = 12
    max_description_chars: int = 220
    tool_floor_tokens: int = 110
    branch_floor_tokens: int = 56
    definition_floor_tokens: int = 72
    include_examples: bool = False
    strict: bool = False


@dataclass
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, Any]
    source_index: int


@dataclass
class Omission:
    tool_name: str
    path: str
    risk: str
    reason: str


@dataclass
class DistilledTool:
    name: str
    description: str
    input_schema: dict[str, Any]
    score: float
    original_tokens: int
    distilled_tokens: int

    def to_manifest(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
        }


@dataclass(frozen=True)
class NodeContext:
    tool_name: str
    root_schema: Mapping[str, Any]
    query_terms: frozenset[str]
    budget_tokens: int
    path: str = "$"
    depth: int = 0
    required: bool = False
    definition_mode: bool = False

    def child(
        self,
        *,
        path: str,
        budget_tokens: Optional[int] = None,
        required: Optional[bool] = None,
        definition_mode: Optional[bool] = None,
    ) -> "NodeContext":
        return NodeContext(
            tool_name=self.tool_name,
            root_schema=self.root_schema,
            query_terms=self.query_terms,
            budget_tokens=self.budget_tokens if budget_tokens is None else budget_tokens,
            path=path,
            depth=self.depth + 1,
            required=self.required if required is None else required,
            definition_mode=self.definition_mode if definition_mode is None else definition_mode,
        )


def sanitize_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): sanitize_json(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [sanitize_json(inner) for inner in value]
    if isinstance(value, tuple):
        return [sanitize_json(inner) for inner in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def stable_json(value: Any) -> str:
    return json.dumps(sanitize_json(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def estimate_tokens(value: Any) -> int:
    text = value if isinstance(value, str) else stable_json(value)
    return max(1, math.ceil(len(text) / 4.0) + 1)


def compact_text(text: str) -> str:
    return " ".join(text.split())


def explode_identifier(text: str) -> str:
    expanded = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    expanded = expanded.replace("_", " ").replace("-", " ").replace("/", " ")
    return expanded


def normalize_term(term: str) -> str:
    term = term.strip().lower()
    if len(term) > 5 and term.endswith("ies"):
        return term[:-3] + "y"
    if len(term) > 4 and term.endswith("sses"):
        return term[:-2]
    if len(term) > 4 and term.endswith("s") and not term.endswith("ss"):
        return term[:-1]
    return term


def keyword_set(text: str) -> set[str]:
    expanded = explode_identifier(text)
    terms: set[str] = set()
    for raw in WORD_RE.findall(expanded.lower()):
        term = normalize_term(raw)
        if len(term) < 2 or term in STOPWORDS:
            continue
        terms.add(term)
    return terms


def lexical_overlap(query_terms: set[str] | frozenset[str], candidate_terms: set[str]) -> float:
    if not query_terms or not candidate_terms:
        return 0.0
    return len(set(query_terms) & candidate_terms) / max(1, len(set(query_terms)))


def split_sentences(text: str) -> list[str]:
    text = compact_text(text)
    if not text:
        return []
    parts = [part.strip() for part in SENTENCE_RE.split(text) if part.strip()]
    return parts if parts else [text]


def trim_description(text: str, max_chars: int, query_terms: set[str] | frozenset[str]) -> str:
    text = compact_text(text)
    if len(text) <= max_chars:
        return text

    sentences = split_sentences(text)
    if not sentences:
        return text[: max_chars - 1].rstrip() + "…"

    scored = sorted(
        sentences,
        key=lambda sentence: (
            lexical_overlap(query_terms, keyword_set(sentence)),
            -len(sentence),
        ),
        reverse=True,
    )

    chosen: list[str] = []
    used = 0
    for sentence in scored:
        extra = len(sentence) + (1 if chosen else 0)
        if used + extra > max_chars:
            continue
        chosen.append(sentence)
        used += extra
        if used >= max_chars * 0.85:
            break

    if not chosen:
        words = text.split()
        output: list[str] = []
        used = 0
        for word in words:
            extra = len(word) + (1 if output else 0)
            if used + extra > max_chars - 1:
                break
            output.append(word)
            used += extra
        return " ".join(output).rstrip(",;:") + "…"

    ordered = [sentence for sentence in sentences if sentence in set(chosen)]
    result = " ".join(ordered)
    if len(result) < len(text):
        result = result.rstrip(". ") + "…"
    return result


def is_small_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (int, float, bool)) or (isinstance(value, str) and len(value) <= 96)


def unescape_ref_part(part: str) -> str:
    return part.replace("~1", "/").replace("~0", "~")


def resolve_local_ref(root: Mapping[str, Any], ref: str) -> Optional[Any]:
    if not ref.startswith("#/"):
        return None
    node: Any = root
    for part in ref[2:].split("/"):
        part = unescape_ref_part(part)
        if not isinstance(node, Mapping) or part not in node:
            return None
        node = node[part]
    return node


def ref_leaf_name(ref: str) -> str:
    return unescape_ref_part(ref.rsplit("/", 1)[-1]) or "Anonymous"


def schema_kind(schema: Any, root: Mapping[str, Any], seen_refs: Optional[set[str]] = None) -> str:
    if isinstance(schema, bool):
        return "boolean-schema"
    if not isinstance(schema, Mapping):
        return "object"

    seen_refs = set() if seen_refs is None else set(seen_refs)
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/") and ref not in seen_refs:
        target = resolve_local_ref(root, ref)
        if target is not None:
            seen_refs.add(ref)
            return schema_kind(target, root, seen_refs)

    for key in ("oneOf", "anyOf", "allOf"):
        branches = schema.get(key)
        if isinstance(branches, Sequence) and not isinstance(branches, (str, bytes, bytearray)):
            return key

    schema_type = schema.get("type")
    if isinstance(schema_type, str):
        return schema_type
    if isinstance(schema_type, Sequence) and not isinstance(schema_type, (str, bytes, bytearray)):
        types = [str(value) for value in schema_type if isinstance(value, str) and value != "null"]
        if len(types) == 1:
            return types[0]
        return "anyOf"

    if "properties" in schema or "required" in schema:
        return "object"
    if "items" in schema or "prefixItems" in schema:
        return "array"
    if "enum" in schema or "const" in schema:
        return "string"
    return "object"


def collect_schema_terms(
    schema: Any,
    root: Mapping[str, Any],
    *,
    depth: int = 0,
    limit: int = 96,
    seen_refs: Optional[set[str]] = None,
    acc: Optional[set[str]] = None,
) -> set[str]:
    acc = set() if acc is None else acc
    if len(acc) >= limit or depth > 3:
        return acc
    if isinstance(schema, bool) or not isinstance(schema, Mapping):
        return acc

    seen_refs = set() if seen_refs is None else set(seen_refs)
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/") and ref not in seen_refs:
        target = resolve_local_ref(root, ref)
        if target is not None:
            seen_refs.add(ref)
            collect_schema_terms(target, root, depth=depth + 1, limit=limit, seen_refs=seen_refs, acc=acc)

    for key in ("title", "description"):
        value = schema.get(key)
        if isinstance(value, str):
            acc.update(keyword_set(value))
            if len(acc) >= limit:
                return acc

    properties = schema.get("properties")
    if isinstance(properties, Mapping):
        for name, child in properties.items():
            acc.update(keyword_set(name))
            collect_schema_terms(child, root, depth=depth + 1, limit=limit, seen_refs=seen_refs, acc=acc)
            if len(acc) >= limit:
                return acc

    for key in ("items", "additionalProperties"):
        child = schema.get(key)
        if isinstance(child, (Mapping, bool)):
            collect_schema_terms(child, root, depth=depth + 1, limit=limit, seen_refs=seen_refs, acc=acc)
            if len(acc) >= limit:
                return acc

    for key in ("oneOf", "anyOf", "allOf", "prefixItems"):
        branches = schema.get(key)
        if isinstance(branches, Sequence) and not isinstance(branches, (str, bytes, bytearray)):
            for child in list(branches)[:6]:
                collect_schema_terms(child, root, depth=depth + 1, limit=limit, seen_refs=seen_refs, acc=acc)
                if len(acc) >= limit:
                    return acc

    return acc


def schema_complexity(
    schema: Any,
    root: Mapping[str, Any],
    *,
    depth: int = 0,
    seen_refs: Optional[set[str]] = None,
) -> int:
    if depth > 5:
        return 1
    if isinstance(schema, bool) or not isinstance(schema, Mapping):
        return 1

    seen_refs = set() if seen_refs is None else set(seen_refs)
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/") and ref not in seen_refs:
        target = resolve_local_ref(root, ref)
        if target is not None:
            seen_refs.add(ref)
            return 1 + schema_complexity(target, root, depth=depth + 1, seen_refs=seen_refs)

    total = 1
    properties = schema.get("properties")
    if isinstance(properties, Mapping):
        for child in properties.values():
            total += schema_complexity(child, root, depth=depth + 1, seen_refs=seen_refs)
    items = schema.get("items")
    if isinstance(items, Mapping):
        total += schema_complexity(items, root, depth=depth + 1, seen_refs=seen_refs)
    for key in ("oneOf", "anyOf", "allOf", "prefixItems"):
        branches = schema.get(key)
        if isinstance(branches, Sequence) and not isinstance(branches, (str, bytes, bytearray)):
            for child in branches:
                total += schema_complexity(child, root, depth=depth + 1, seen_refs=seen_refs)
    enum_values = schema.get("enum")
    if isinstance(enum_values, Sequence) and not isinstance(enum_values, (str, bytes, bytearray)):
        total += min(len(enum_values), 24)
    return total


def summarize_omissions(omissions: Sequence[Omission]) -> dict[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0}
    for omission in omissions:
        counts[omission.risk] = counts.get(omission.risk, 0) + 1
    return counts


def manifest_fingerprint(manifest: Any) -> str:
    digest = hashlib.blake2b(stable_json(manifest).encode("utf-8"), digest_size=12)
    return digest.hexdigest()


def normalize_tool(item: Any, index: int) -> Optional[ToolSpec]:
    if not isinstance(item, Mapping):
        return None

    candidate: Mapping[str, Any] = item
    function_block = candidate.get("function")
    if candidate.get("type") == "function" and isinstance(function_block, Mapping):
        candidate = function_block
    elif isinstance(function_block, Mapping) and isinstance(function_block.get("name"), str):
        candidate = function_block

    name = candidate.get("name") or item.get("name")
    if not isinstance(name, str) or not name.strip():
        return None

    description = candidate.get("description") or item.get("description") or ""
    schema = candidate.get("inputSchema")
    if schema is None:
        schema = candidate.get("parameters")
    if schema is None:
        schema = candidate.get("schema")
    if schema is None:
        schema = item.get("inputSchema") or item.get("parameters") or item.get("schema")
    if not isinstance(schema, Mapping):
        schema = {"type": "object", "properties": {}}

    return ToolSpec(
        name=name.strip(),
        description=str(description or "").strip(),
        input_schema=sanitize_json(schema),
        source_index=index,
    )


def extract_tools(payload: Any) -> list[ToolSpec]:
    items: list[Any]
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, Mapping):
        if isinstance(payload.get("tools"), list):
            items = list(payload["tools"])
        elif isinstance(payload.get("functions"), list):
            items = list(payload["functions"])
        else:
            items = [payload]
    else:
        raise ValueError("Input must be a tool object, a list of tools, or an object with a 'tools' array.")

    tools = [tool for index, item in enumerate(items) if (tool := normalize_tool(item, index)) is not None]
    if not tools:
        raise ValueError("No tool definitions were found in the input payload.")
    return tools


class SchemaDistiller:
    def __init__(self, config: DistillConfig) -> None:
        self.config = config
        self.omissions: list[Omission] = []
        self._omission_keys: set[tuple[str, str, str]] = set()
        self._current_refs: set[str] = set()

    def distill_tools(self, tools: Sequence[ToolSpec], query: str) -> dict[str, Any]:
        self.omissions.clear()
        self._omission_keys.clear()

        query_terms = frozenset(keyword_set(query))
        ranked: list[tuple[ToolSpec, float, int, int]] = []
        for tool in tools:
            score = self._tool_score(tool, query_terms)
            complexity = schema_complexity(tool.input_schema, tool.input_schema)
            floor = self._tool_floor(tool, query_terms)
            ranked.append((tool, score, complexity, floor))

        ranked.sort(key=lambda item: (-item[1], item[0].name.lower(), item[0].source_index))
        selected = self._select_tools(ranked)
        allocations = self._allocate_budgets(selected)
        selected_ids = {entry[0].source_index for entry in selected}
        dropped = [tool.name for tool, _score, _complexity, _floor in ranked if tool.source_index not in selected_ids]

        distilled_tools: list[DistilledTool] = []
        for tool, score, _complexity, _floor in selected:
            distilled_tools.append(self._distill_tool(tool, query_terms, allocations[tool.source_index], score))

        manifest = [tool.to_manifest() for tool in distilled_tools]
        manifest_tokens = estimate_tokens(manifest)
        omission_summary = summarize_omissions(self.omissions)

        result: dict[str, Any] = {
            "query": query,
            "budgetTokens": self.config.total_budget_tokens,
            "manifestTokens": manifest_tokens,
            "selectedToolCount": len(distilled_tools),
            "sourceToolCount": len(tools),
            "fingerprint": manifest_fingerprint(manifest),
            "selectedTools": [
                {
                    "name": tool.name,
                    "score": round(tool.score, 4),
                    "originalTokens": tool.original_tokens,
                    "distilledTokens": tool.distilled_tokens,
                }
                for tool in distilled_tools
            ],
            "droppedTools": dropped,
            "omissionSummary": omission_summary,
            "manifest": manifest,
        }
        if self.omissions:
            result["omissions"] = [
                {
                    "tool": omission.tool_name,
                    "path": omission.path,
                    "risk": omission.risk,
                    "reason": omission.reason,
                }
                for omission in self.omissions
            ]
        return result

    def _tool_score(self, tool: ToolSpec, query_terms: frozenset[str]) -> float:
        name_terms = keyword_set(tool.name)
        description_terms = keyword_set(tool.description)
        schema_terms = collect_schema_terms(tool.input_schema, tool.input_schema)

        score = 1.0
        if query_terms:
            score += 5.0 * lexical_overlap(query_terms, name_terms)
            score += 3.2 * lexical_overlap(query_terms, description_terms)
            score += 2.2 * lexical_overlap(query_terms, schema_terms)
            if compact_text(explode_identifier(tool.name)).lower().find(" ".join(sorted(query_terms))[:32]) >= 0:
                score += 0.4
        else:
            score += 0.8
        score += min(schema_complexity(tool.input_schema, tool.input_schema) / 40.0, 1.25)
        return score

    def _tool_floor(self, tool: ToolSpec, query_terms: frozenset[str]) -> int:
        preview: dict[str, Any] = {
            "name": tool.name,
            "description": trim_description(tool.description or tool.name, min(120, self.config.max_description_chars), query_terms),
            "inputSchema": {
                "type": schema_kind(tool.input_schema, tool.input_schema),
            },
        }
        required = tool.input_schema.get("required")
        if isinstance(required, Sequence) and not isinstance(required, (str, bytes, bytearray)) and required:
            preview["inputSchema"]["required"] = list(required)[:6]
        return max(self.config.tool_floor_tokens, estimate_tokens(preview))

    def _select_tools(self, ranked: Sequence[tuple[ToolSpec, float, int, int]]) -> list[tuple[ToolSpec, float, int, int]]:
        selected: list[tuple[ToolSpec, float, int, int]] = []
        used = 0
        for entry in ranked:
            if len(selected) >= self.config.max_tools:
                break
            floor = entry[3]
            if not selected or used + floor <= self.config.total_budget_tokens:
                selected.append(entry)
                used += floor
        if not selected and ranked:
            selected.append(ranked[0])
        return selected

    def _allocate_budgets(self, selected: Sequence[tuple[ToolSpec, float, int, int]]) -> dict[int, int]:
        base = sum(entry[3] for entry in selected)
        remaining = max(0, self.config.total_budget_tokens - base)
        weights = [max(entry[1], 0.25) * (1.0 + math.log1p(entry[2])) for entry in selected]
        weight_total = sum(weights) or 1.0

        allocations: dict[int, int] = {entry[0].source_index: entry[3] for entry in selected}
        if remaining <= 0:
            return allocations

        raw_shares = [remaining * weight / weight_total for weight in weights]
        whole_shares = [int(share) for share in raw_shares]
        leftover = remaining - sum(whole_shares)
        ranked_fractionals = sorted(
            range(len(raw_shares)),
            key=lambda index: (raw_shares[index] - whole_shares[index], weights[index]),
            reverse=True,
        )

        for index, entry in enumerate(selected):
            allocations[entry[0].source_index] += whole_shares[index]
        for index in ranked_fractionals[:leftover]:
            allocations[selected[index][0].source_index] += 1
        return allocations

    def _distill_tool(
        self,
        tool: ToolSpec,
        query_terms: frozenset[str],
        budget_tokens: int,
        score: float,
    ) -> DistilledTool:
        self._current_refs = set()
        description = trim_description(tool.description or tool.name, self.config.max_description_chars, query_terms)
        shell = {"name": tool.name, "description": description}
        schema_budget = max(48, budget_tokens - estimate_tokens(shell))

        ctx = NodeContext(
            tool_name=tool.name,
            root_schema=tool.input_schema,
            query_terms=query_terms,
            budget_tokens=schema_budget,
            path="$",
            depth=0,
            required=True,
        )
        input_schema = self._distill_node(tool.input_schema, ctx, set())
        input_schema = self._attach_ref_definitions(input_schema, tool.input_schema, ctx)

        manifest = {"name": tool.name, "description": description, "inputSchema": input_schema}
        if estimate_tokens(manifest) > budget_tokens and manifest["description"]:
            manifest["description"] = trim_description(manifest["description"], max(56, len(manifest["description"]) // 2), query_terms)
        if estimate_tokens(manifest) > budget_tokens:
            self._strip_descriptions(manifest["inputSchema"])
        if estimate_tokens(manifest) > budget_tokens:
            self._strip_examples_and_defaults(manifest["inputSchema"])

        distilled_tokens = estimate_tokens(manifest)
        return DistilledTool(
            name=tool.name,
            description=manifest["description"],
            input_schema=manifest["inputSchema"],
            score=score,
            original_tokens=estimate_tokens(tool.input_schema),
            distilled_tokens=distilled_tokens,
        )

    def _distill_node(self, schema: Any, ctx: NodeContext, seen_refs: set[str]) -> Any:
        if isinstance(schema, bool):
            return schema
        if not isinstance(schema, Mapping):
            return {}

        ref = schema.get("$ref")
        if isinstance(ref, str):
            return self._distill_ref(schema, ctx, seen_refs)

        kind = schema_kind(schema, ctx.root_schema, seen_refs)
        if kind == "object":
            return self._distill_object(schema, ctx, seen_refs)
        if kind == "array":
            return self._distill_array(schema, ctx, seen_refs)
        if kind in {"oneOf", "anyOf", "allOf"}:
            return self._distill_union(schema, ctx, seen_refs)
        return self._distill_primitive(schema, ctx)

    def _distill_ref(self, schema: Mapping[str, Any], ctx: NodeContext, seen_refs: set[str]) -> dict[str, Any]:
        ref = str(schema.get("$ref"))
        self._current_refs.add(ref)
        out: dict[str, Any] = {"$ref": ref}
        self._copy_common_metadata(schema, out, ctx)
        if ref in seen_refs:
            return out
        target = resolve_local_ref(ctx.root_schema, ref)
        if target is None:
            self._record_omission(ctx.tool_name, ctx.path, "high", f"could not resolve local $ref target {ref}")
        return out

    def _distill_object(self, schema: Mapping[str, Any], ctx: NodeContext, seen_refs: set[str]) -> dict[str, Any]:
        out: dict[str, Any] = {"type": "object"}
        self._copy_common_metadata(schema, out, ctx)

        for key in OBJECT_CONSTRAINT_KEYS:
            if key not in schema:
                continue
            value = schema[key]
            if key == "additionalProperties" and isinstance(value, Mapping):
                value = self._minimal_fragment(value, ctx.child(path=f"{ctx.path}.additionalProperties", budget_tokens=max(24, ctx.budget_tokens // 4), required=False), set(seen_refs))
            self._copy_if_fit(out, key, value, ctx.budget_tokens)

        for key in ("propertyNames", "dependentRequired"):
            value = schema.get(key)
            if value is None:
                continue
            if estimate_tokens({key: value}) <= max(36, ctx.budget_tokens // 3):
                self._copy_if_fit(out, key, value, ctx.budget_tokens)
            else:
                self._record_omission(ctx.tool_name, f"{ctx.path}.{key}", "low", f"skipped verbose {key} section to stay inside prompt budget")

        pattern_properties = schema.get("patternProperties")
        if pattern_properties is not None:
            if estimate_tokens({"patternProperties": pattern_properties}) <= max(36, ctx.budget_tokens // 3):
                self._copy_if_fit(out, "patternProperties", pattern_properties, ctx.budget_tokens)
            else:
                self._record_omission(ctx.tool_name, f"{ctx.path}.patternProperties", "medium", "skipped large patternProperties block to stay inside prompt budget")

        properties = schema.get("properties")
        if not isinstance(properties, Mapping) or not properties:
            return out

        out_props: dict[str, Any] = {}
        out["properties"] = out_props
        required_order = [name for name in schema.get("required", []) if name in properties]
        required_set = set(required_order)
        candidates: list[tuple[str, Any, float, bool]] = []
        for name, node in properties.items():
            required = name in required_set
            score = self._field_score(name, node, ctx.query_terms, ctx.root_schema, required, ctx.depth + 1)
            candidates.append((name, node, score, required))
        candidates.sort(key=lambda item: (not item[3], -item[2], item[0]))

        included_required: list[str] = []
        total_score = sum(max(item[2], 0.1) for item in candidates)
        remaining_budget = max(12, ctx.budget_tokens - estimate_tokens(out))

        for name, node, score, required in candidates:
            share = max(18 if required else 12, int(remaining_budget * max(score, 0.1) / max(total_score, 0.1)))
            child_ctx = ctx.child(
                path=f"{ctx.path}.properties.{name}",
                budget_tokens=share,
                required=required,
            )
            fragment = self._distill_node(node, child_ctx, set(seen_refs))
            out_props[name] = fragment

            if estimate_tokens(out) <= ctx.budget_tokens:
                if required:
                    included_required.append(name)
                continue

            if required:
                out_props[name] = self._minimal_fragment(node, child_ctx, set(seen_refs))
                if estimate_tokens(out) <= ctx.budget_tokens:
                    included_required.append(name)
                    continue
                del out_props[name]
                self._record_omission(
                    ctx.tool_name,
                    child_ctx.path,
                    "high",
                    "required property could not fit inside the per-tool prompt budget",
                )
            else:
                del out_props[name]
                risk = "medium" if score >= 2.5 else "low"
                self._record_omission(
                    ctx.tool_name,
                    child_ctx.path,
                    risk,
                    "dropped low-relevance optional property to stay inside the prompt budget",
                )

        if included_required:
            out["required"] = included_required
        if not out_props:
            out.pop("properties", None)
        if estimate_tokens(out) > ctx.budget_tokens and "description" in out:
            out.pop("description", None)
        return out

    def _distill_array(self, schema: Mapping[str, Any], ctx: NodeContext, seen_refs: set[str]) -> dict[str, Any]:
        out: dict[str, Any] = {"type": "array"}
        self._copy_common_metadata(schema, out, ctx)

        for key in ("minItems", "maxItems", "uniqueItems"):
            if key in schema:
                self._copy_if_fit(out, key, schema[key], ctx.budget_tokens)

        items = schema.get("items")
        if items is not None:
            if isinstance(items, bool):
                self._copy_if_fit(out, "items", items, ctx.budget_tokens)
            elif isinstance(items, Mapping):
                child_ctx = ctx.child(path=f"{ctx.path}.items", budget_tokens=max(18, ctx.budget_tokens - estimate_tokens(out)), required=ctx.required)
                distilled = self._distill_node(items, child_ctx, set(seen_refs))
                if not self._copy_if_fit(out, "items", distilled, ctx.budget_tokens):
                    self._copy_if_fit(out, "items", self._minimal_fragment(items, child_ctx, set(seen_refs)), ctx.budget_tokens)

        prefix_items = schema.get("prefixItems")
        if isinstance(prefix_items, Sequence) and not isinstance(prefix_items, (str, bytes, bytearray)):
            distilled_prefix: list[Any] = []
            out["prefixItems"] = distilled_prefix
            dropped = 0
            for index, child in enumerate(prefix_items):
                child_ctx = ctx.child(
                    path=f"{ctx.path}.prefixItems[{index}]",
                    budget_tokens=max(12, self.config.branch_floor_tokens),
                    required=index == 0,
                )
                distilled_prefix.append(self._minimal_fragment(child, child_ctx, set(seen_refs)))
                if estimate_tokens(out) > ctx.budget_tokens:
                    distilled_prefix.pop()
                    dropped += 1
                    break
            if not distilled_prefix:
                out.pop("prefixItems", None)
            elif dropped or len(distilled_prefix) < len(prefix_items):
                self._record_omission(
                    ctx.tool_name,
                    f"{ctx.path}.prefixItems",
                    "low",
                    "trimmed prefixItems tuple validation to stay inside the prompt budget",
                )

        if estimate_tokens(out) > ctx.budget_tokens and "description" in out:
            out.pop("description", None)
        return out

    def _distill_union(self, schema: Mapping[str, Any], ctx: NodeContext, seen_refs: set[str]) -> dict[str, Any]:
        branch_key = next(
            key
            for key in ("oneOf", "anyOf", "allOf")
            if isinstance(schema.get(key), Sequence) and not isinstance(schema.get(key), (str, bytes, bytearray))
        )
        branches = list(schema.get(branch_key, []))
        out: dict[str, Any] = {branch_key: []}
        self._copy_common_metadata(schema, out, ctx)
        discriminator = schema.get("discriminator")
        if isinstance(discriminator, Mapping):
            self._copy_if_fit(out, "discriminator", discriminator, ctx.budget_tokens)

        indexed = list(enumerate(branches))
        if branch_key != "allOf":
            indexed.sort(
                key=lambda item: (-self._branch_score(item[1], ctx.query_terms, ctx.root_schema), item[0])
            )

        for original_index, branch in indexed:
            share = max(self.config.branch_floor_tokens, int((ctx.budget_tokens - estimate_tokens(out)) / max(1, len(branches))))
            child_ctx = ctx.child(
                path=f"{ctx.path}.{branch_key}[{original_index}]",
                budget_tokens=share,
                required=branch_key == "allOf" or original_index == 0,
            )
            fragment = self._distill_node(branch, child_ctx, set(seen_refs))
            out[branch_key].append(fragment)
            if estimate_tokens(out) <= ctx.budget_tokens:
                continue

            out[branch_key].pop()
            if branch_key == "allOf":
                minimal = self._minimal_fragment(branch, child_ctx, set(seen_refs))
                out[branch_key].append(minimal)
                if estimate_tokens(out) <= ctx.budget_tokens:
                    continue
                out[branch_key].pop()
                self._record_omission(
                    ctx.tool_name,
                    child_ctx.path,
                    "high",
                    "dropped an allOf branch because the combined schema could not fit inside the prompt budget",
                )
            else:
                risk = "medium" if original_index == 0 else "low"
                self._record_omission(
                    ctx.tool_name,
                    child_ctx.path,
                    risk,
                    "dropped a low-relevance union branch to stay inside the prompt budget",
                )

        if not out[branch_key] and branches:
            forced_ctx = ctx.child(path=f"{ctx.path}.{branch_key}[0]", budget_tokens=self.config.branch_floor_tokens, required=True)
            out[branch_key].append(self._minimal_fragment(branches[0], forced_ctx, set(seen_refs)))
        if estimate_tokens(out) > ctx.budget_tokens and "description" in out:
            out.pop("description", None)
        return out

    def _distill_primitive(self, schema: Mapping[str, Any], ctx: NodeContext) -> dict[str, Any]:
        out: dict[str, Any] = {}
        self._copy_common_metadata(schema, out, ctx)

        schema_type = schema.get("type")
        if schema_type is not None:
            out["type"] = sanitize_json(schema_type)
        else:
            inferred = schema_kind(schema, ctx.root_schema)
            if inferred not in {"object", "array", "oneOf", "anyOf", "allOf", "boolean-schema"}:
                out["type"] = inferred

        for key in PRIMITIVE_CONSTRAINT_KEYS:
            if key in schema:
                self._copy_if_fit(out, key, schema[key], ctx.budget_tokens)

        if "enum" in schema and isinstance(schema["enum"], Sequence) and not isinstance(schema["enum"], (str, bytes, bytearray)):
            enum_values, truncated = self._distill_enum(schema["enum"], out, ctx.budget_tokens)
            if enum_values is not None:
                out["enum"] = enum_values
                if truncated:
                    out["x-distilledEnumCount"] = len(schema["enum"])
                    self._record_omission(
                        ctx.tool_name,
                        f"{ctx.path}.enum",
                        "medium" if ctx.required else "low",
                        "truncated a long enum to stay inside the prompt budget",
                    )
            else:
                self._record_omission(
                    ctx.tool_name,
                    f"{ctx.path}.enum",
                    "medium" if ctx.required else "low",
                    "dropped an enum that was too large for the available prompt budget",
                )

        if "default" in schema and is_small_scalar(schema["default"]):
            self._copy_if_fit(out, "default", schema["default"], ctx.budget_tokens)
        if self.config.include_examples and "examples" in schema and estimate_tokens(schema["examples"]) <= max(18, ctx.budget_tokens // 4):
            self._copy_if_fit(out, "examples", schema["examples"], ctx.budget_tokens)

        if not out:
            return {"type": "string"}
        return out

    def _minimal_fragment(self, schema: Any, ctx: NodeContext, seen_refs: set[str]) -> Any:
        if isinstance(schema, bool):
            return schema
        if not isinstance(schema, Mapping):
            return {"type": "object"}

        ref = schema.get("$ref")
        if isinstance(ref, str):
            self._current_refs.add(ref)
            return {"$ref": ref}

        kind = schema_kind(schema, ctx.root_schema, seen_refs)
        out: dict[str, Any] = {}
        schema_type = schema.get("type")
        if schema_type is not None:
            out["type"] = sanitize_json(schema_type)
        elif kind not in {"oneOf", "anyOf", "allOf", "boolean-schema"}:
            out["type"] = kind

        if kind == "object" and schema.get("additionalProperties") is False:
            out["additionalProperties"] = False
        if kind == "array" and "items" in schema:
            items = schema["items"]
            if isinstance(items, bool):
                out["items"] = items
            elif isinstance(items, Mapping):
                out["items"] = self._minimal_fragment(items, ctx.child(path=f"{ctx.path}.items", budget_tokens=max(12, ctx.budget_tokens // 2)), set(seen_refs))

        if "const" in schema:
            out["const"] = sanitize_json(schema["const"])
        elif "enum" in schema and isinstance(schema["enum"], Sequence) and not isinstance(schema["enum"], (str, bytes, bytearray)):
            enum_values = list(schema["enum"])[:8]
            out["enum"] = sanitize_json(enum_values)
            if len(schema["enum"]) > len(enum_values):
                out["x-distilledEnumCount"] = len(schema["enum"])

        for key in PRIMITIVE_CONSTRAINT_KEYS:
            if key in schema:
                out[key] = sanitize_json(schema[key])

        if not out:
            out["type"] = "object"
        return out

    def _attach_ref_definitions(self, distilled_root: dict[str, Any], original_root: Mapping[str, Any], ctx: NodeContext) -> dict[str, Any]:
        if not self._current_refs:
            return distilled_root

        queue = sorted(self._current_refs)
        processed: set[str] = set()
        defs_out: dict[str, Any] = {}
        legacy_defs: dict[str, Any] = {}
        remaining_budget = max(0, ctx.budget_tokens - estimate_tokens(distilled_root))

        while queue:
            ref = queue.pop(0)
            if ref in processed:
                continue
            target = resolve_local_ref(original_root, ref)
            if target is None:
                self._record_omission(ctx.tool_name, ref, "high", f"could not resolve local $ref target {ref}")
                processed.add(ref)
                continue

            budget = max(self.config.definition_floor_tokens, remaining_budget // max(1, len(queue) + 1))
            def_ctx = ctx.child(path=ref, budget_tokens=budget, required=True, definition_mode=True)
            fragment = self._distill_node(target, def_ctx, {ref})
            leaf = ref_leaf_name(ref)
            if ref.startswith("#/$defs/"):
                defs_out[leaf] = fragment
            elif ref.startswith("#/definitions/"):
                legacy_defs[leaf] = fragment
            else:
                defs_out[leaf] = fragment

            processed.add(ref)
            for new_ref in sorted(self._current_refs - processed - set(queue)):
                queue.append(new_ref)
            remaining_budget = max(0, ctx.budget_tokens - estimate_tokens(distilled_root))

        if defs_out:
            distilled_root["$defs"] = defs_out
        if legacy_defs:
            distilled_root["definitions"] = legacy_defs
        return distilled_root

    def _field_score(
        self,
        name: str,
        node: Any,
        query_terms: frozenset[str],
        root_schema: Mapping[str, Any],
        required: bool,
        depth: int,
    ) -> float:
        description = node.get("description", "") if isinstance(node, Mapping) else ""
        score = 0.1
        score += 5.0 if required else 0.0
        score += 4.0 * lexical_overlap(query_terms, keyword_set(name))
        score += 1.8 * lexical_overlap(query_terms, keyword_set(str(description)))
        kind = schema_kind(node, root_schema) if isinstance(node, Mapping) else "object"
        if kind in {"object", "array", "oneOf", "anyOf", "allOf"}:
            score += 0.6
        if isinstance(node, Mapping) and ("enum" in node or "const" in node):
            score += 1.2
        if isinstance(node, Mapping) and any(key in node for key in PRIMITIVE_CONSTRAINT_KEYS):
            score += 0.7
        if isinstance(node, Mapping) and node.get("deprecated"):
            score -= 1.5
        score -= depth * 0.15
        return max(0.05, score)

    def _branch_score(self, branch: Any, query_terms: frozenset[str], root_schema: Mapping[str, Any]) -> float:
        terms = collect_schema_terms(branch, root_schema, limit=40)
        score = 0.5 + 4.0 * lexical_overlap(query_terms, terms)
        tag = self._const_tag(branch, root_schema)
        if tag:
            score += 1.2
        return score

    def _const_tag(self, schema: Any, root_schema: Mapping[str, Any]) -> Optional[str]:
        if not isinstance(schema, Mapping):
            return None
        ref = schema.get("$ref")
        if isinstance(ref, str):
            target = resolve_local_ref(root_schema, ref)
            if target is not None:
                return self._const_tag(target, root_schema)

        properties = schema.get("properties")
        if not isinstance(properties, Mapping):
            return None
        for key in ("kind", "type", "action", "operation", "event", "mode"):
            value = properties.get(key)
            if not isinstance(value, Mapping):
                continue
            if "const" in value:
                return f"{key}={value['const']}"
            enum_values = value.get("enum")
            if isinstance(enum_values, Sequence) and not isinstance(enum_values, (str, bytes, bytearray)) and len(enum_values) == 1:
                return f"{key}={list(enum_values)[0]}"
        return None

    def _copy_common_metadata(self, schema: Mapping[str, Any], out: dict[str, Any], ctx: NodeContext) -> None:
        title = schema.get("title")
        if isinstance(title, str) and title.strip():
            self._copy_if_fit(out, "title", trim_description(title, min(96, self.config.max_description_chars), ctx.query_terms), ctx.budget_tokens)

        description = schema.get("description")
        if isinstance(description, str) and description.strip():
            limit = min(self.config.max_description_chars, 140 if ctx.depth else self.config.max_description_chars)
            trimmed = trim_description(description, limit, ctx.query_terms)
            self._copy_if_fit(out, "description", trimmed, ctx.budget_tokens)

        for key in ("deprecated", "readOnly", "writeOnly", "nullable"):
            if schema.get(key) is True:
                self._copy_if_fit(out, key, True, ctx.budget_tokens)

    def _copy_if_fit(self, out: dict[str, Any], key: str, value: Any, budget_tokens: int) -> bool:
        previous = out.get(key, MISSING)
        out[key] = sanitize_json(value)
        if estimate_tokens(out) <= budget_tokens:
            return True
        if previous is MISSING:
            out.pop(key, None)
        else:
            out[key] = previous
        return False

    def _distill_enum(self, values: Sequence[Any], current_obj: dict[str, Any], budget_tokens: int) -> tuple[Optional[list[Any]], bool]:
        sanitized = [sanitize_json(value) for value in values]
        trial = dict(current_obj)
        trial["enum"] = sanitized
        if estimate_tokens(trial) <= budget_tokens:
            return sanitized, False

        kept: list[Any] = []
        for value in sanitized:
            candidate = kept + [value]
            trial = dict(current_obj)
            trial["enum"] = candidate
            trial["x-distilledEnumCount"] = len(sanitized)
            if estimate_tokens(trial) <= budget_tokens or not kept:
                kept = candidate
            else:
                break

        if kept:
            return kept, len(kept) < len(sanitized)
        return None, True

    def _record_omission(self, tool_name: str, path: str, risk: str, reason: str) -> None:
        key = (tool_name, path, reason)
        if key in self._omission_keys:
            return
        self._omission_keys.add(key)
        self.omissions.append(Omission(tool_name=tool_name, path=path, risk=risk, reason=reason))

    def _strip_descriptions(self, node: Any) -> None:
        if isinstance(node, dict):
            node.pop("description", None)
            for value in list(node.values()):
                self._strip_descriptions(value)
        elif isinstance(node, list):
            for value in node:
                self._strip_descriptions(value)

    def _strip_examples_and_defaults(self, node: Any) -> None:
        if isinstance(node, dict):
            node.pop("examples", None)
            node.pop("default", None)
            for value in list(node.values()):
                self._strip_examples_and_defaults(value)
        elif isinstance(node, list):
            for value in node:
                self._strip_examples_and_defaults(value)


def load_payload(path: str) -> Any:
    if path == "-":
        text = sys.stdin.read()
    else:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    if not text.strip():
        raise ValueError("Input payload was empty.")
    return json.loads(text)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Distill verbose MCP/OpenAPI tool schemas into a prompt-sized manifest.")
    parser.add_argument("--input", default="-", help="Path to a JSON file or '-' to read JSON from stdin.")
    parser.add_argument("--query", default="", help="Current user task or prompt used to rank tools and fields.")
    parser.add_argument("--budget", type=int, default=3200, help="Prompt token budget for the distilled manifest.")
    parser.add_argument("--top-tools", type=int, default=12, help="Maximum number of tools to keep in the distilled manifest.")
    parser.add_argument(
        "--max-description-chars",
        type=int,
        default=220,
        help="Maximum characters to keep for any single description before query-aware trimming.",
    )
    parser.add_argument("--include-examples", action="store_true", help="Keep small example payloads when they fit.")
    parser.add_argument("--manifest-only", action="store_true", help="Print only the distilled manifest array instead of the full report.")
    parser.add_argument("--strict", action="store_true", help="Exit with code 2 when high-risk omissions are recorded.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        payload = load_payload(args.input)
        tools = extract_tools(payload)
        config = DistillConfig(
            total_budget_tokens=max(128, args.budget),
            max_tools=max(1, args.top_tools),
            max_description_chars=max(40, args.max_description_chars),
            include_examples=args.include_examples,
            strict=args.strict,
        )
        distiller = SchemaDistiller(config)
        report = distiller.distill_tools(tools, args.query)
        output = report["manifest"] if args.manifest_only else report
        json.dump(output, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
        if args.strict and report["omissionSummary"].get("high", 0) > 0:
            return 2
        return 0
    except json.JSONDecodeError as error:
        print(f"ToolSchemaDistiller: invalid JSON input: {error}", file=sys.stderr)
        return 64
    except ValueError as error:
        print(f"ToolSchemaDistiller: {error}", file=sys.stderr)
        return 64
    except Exception as error:  # pragma: no cover - CLI guardrail
        print(f"ToolSchemaDistiller: unexpected failure: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""
This solves the real 2026 problem where MCP tool catalogs, OpenAPI function lists, and JSON Schema-heavy agent manifests get too large for a model prompt long before the runtime is in trouble. Built because a lot of agent bugs are not runtime validation bugs anymore, they are prompt-shape bugs: too many tools, too much schema text, and not enough budget left for the actual user request.

Use it when your agent has to choose from many tools, when an OpenAI or Anthropic tool payload is bloated, when an MCP server exposes dozens of actions, or when a long schema is hurting tool selection quality. The trick: it does not blindly truncate. It ranks tools against the current task, keeps required fields, preserves enums and structural constraints, tracks local $ref definitions, and records exactly what got dropped so a human can audit the risk.

Drop this into an agent gateway, MCP proxy, tool-router service, eval harness, or prompt-build step right before model invocation. It is especially useful for Python agent backends, prompt compilers, tool-calling middleware, and research systems that need prompt budget control, JSON Schema distillation, MCP schema compression, OpenAPI tool filtering, and safer LLM tool selection without throwing away the parts that actually change behavior.
"""
