#!/usr/bin/env python3
"""
DataResidencyRoutePlanner.py

A dependency-free planner for routing AI inference requests across provider,
region, and model endpoints while respecting data residency, retention,
training-use, certification, capacity, latency, cost, and carbon constraints.

Input is a JSON config with a `routes` array and optional `policy` object, plus
JSONL requests. The CLI emits one JSON decision per request and exits non-zero
when any request cannot be routed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence


VERSION = "2026.04"
SENSITIVE_CLASSES = {"pii", "phi", "pci", "secrets", "biometric", "financial", "health"}
DEFAULT_WEIGHTS = {
    "latency": 0.28,
    "cost": 0.22,
    "carbon": 0.14,
    "headroom": 0.18,
    "residency": 0.12,
    "health": 0.06,
}


class PlannerConfigError(ValueError):
    """Raised when the planner configuration is not usable."""


class RequestParseError(ValueError):
    """Raised when a request JSON object cannot be planned safely."""


@dataclass(frozen=True)
class Route:
    name: str
    provider: str
    model: str
    region: str
    jurisdiction: str
    residency_groups: frozenset[str]
    endpoint: str = ""
    enabled: bool = True
    health: float = 1.0
    max_context_tokens: int = 128_000
    capacity_tokens_per_minute: int = 1_000_000
    latency_p95_ms: float = 1_000.0
    decode_ms_per_output_token: float = 0.0
    cost_usd_per_1k_input: float = 0.0
    cost_usd_per_1k_output: float = 0.0
    carbon_g_per_1k_tokens: float = 0.0
    retention_days: int = 0
    training_disabled: bool = True
    certifications: frozenset[str] = field(default_factory=frozenset)
    allowed_data_classes: frozenset[str] = field(default_factory=frozenset)
    allowed_tenants: frozenset[str] = field(default_factory=frozenset)
    denied_tenants: frozenset[str] = field(default_factory=frozenset)
    tags: frozenset[str] = field(default_factory=frozenset)
    weight: float = 1.0

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "Route":
        name = require_text(raw, "name")
        provider = require_text(raw, "provider")
        model = require_text(raw, "model")
        region = require_text(raw, "region")
        jurisdiction = text_value(raw, "jurisdiction", region)
        residency_groups = text_set(get_any(raw, "residency_groups", "residencyGroups", "residency", default=[]))
        if not residency_groups:
            residency_groups = frozenset({jurisdiction.lower(), region.lower()})
        return cls(
            name=name,
            provider=provider.lower(),
            model=model,
            region=region.lower(),
            jurisdiction=jurisdiction.lower(),
            residency_groups=residency_groups,
            endpoint=text_value(raw, "endpoint", ""),
            enabled=bool_value(raw, "enabled", default=True),
            health=bounded_float(raw, "health", 1.0, 0.0, 1.0),
            max_context_tokens=int_value(raw, "max_context_tokens", "maxContextTokens", default=128_000, minimum=1),
            capacity_tokens_per_minute=int_value(raw, "capacity_tokens_per_minute", "capacityTokensPerMinute", default=1_000_000, minimum=1),
            latency_p95_ms=float_value(raw, "latency_p95_ms", "latencyP95Ms", default=1_000.0, minimum=0.0),
            decode_ms_per_output_token=float_value(raw, "decode_ms_per_output_token", "decodeMsPerOutputToken", default=0.0, minimum=0.0),
            cost_usd_per_1k_input=float_value(raw, "cost_usd_per_1k_input", "costUsdPer1kInput", default=0.0, minimum=0.0),
            cost_usd_per_1k_output=float_value(raw, "cost_usd_per_1k_output", "costUsdPer1kOutput", default=0.0, minimum=0.0),
            carbon_g_per_1k_tokens=float_value(raw, "carbon_g_per_1k_tokens", "carbonGPer1kTokens", default=0.0, minimum=0.0),
            retention_days=int_value(raw, "retention_days", "retentionDays", default=0, minimum=0),
            training_disabled=bool_value(raw, "training_disabled", "trainingDisabled", default=True),
            certifications=text_set(get_any(raw, "certifications", "certs", default=[])),
            allowed_data_classes=text_set(get_any(raw, "allowed_data_classes", "allowedDataClasses", default=[])),
            allowed_tenants=text_set(get_any(raw, "allowed_tenants", "allowedTenants", default=[])),
            denied_tenants=text_set(get_any(raw, "denied_tenants", "deniedTenants", default=[])),
            tags=text_set(get_any(raw, "tags", default=[])),
            weight=bounded_float(raw, "weight", 1.0, 0.0, 10.0),
        )

    @property
    def id(self) -> str:
        return f"{self.provider}/{self.model}/{self.region}/{self.name}"

    def estimate_latency_ms(self, request: "InferenceRequest") -> float:
        return self.latency_p95_ms + self.decode_ms_per_output_token * request.output_tokens

    def estimate_cost_usd(self, request: "InferenceRequest") -> float:
        return (
            request.input_tokens * self.cost_usd_per_1k_input
            + request.output_tokens * self.cost_usd_per_1k_output
        ) / 1000.0

    def estimate_carbon_g(self, request: "InferenceRequest") -> float:
        return request.total_tokens * self.carbon_g_per_1k_tokens / 1000.0


@dataclass(frozen=True)
class Policy:
    version: str = VERSION
    provider_allowlist: frozenset[str] = field(default_factory=frozenset)
    provider_denylist: frozenset[str] = field(default_factory=frozenset)
    model_denylist: frozenset[str] = field(default_factory=frozenset)
    region_denylist: frozenset[str] = field(default_factory=frozenset)
    required_certifications_by_data_class: Mapping[str, frozenset[str]] = field(default_factory=dict)
    allowed_residency_by_country: Mapping[str, frozenset[str]] = field(default_factory=dict)
    allowed_residency_by_data_class: Mapping[str, frozenset[str]] = field(default_factory=dict)
    max_retention_days_by_data_class: Mapping[str, int] = field(default_factory=dict)
    require_training_disabled_for_sensitive: bool = True
    min_route_health: float = 0.65
    default_deadline_ms: float | None = None
    max_alternatives: int = 3
    weights: Mapping[str, float] = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> "Policy":
        raw = raw or {}
        weights = dict(DEFAULT_WEIGHTS)
        weights.update({k: parse_float(v, f"weights.{k}", minimum=0.0) for k, v in dict_value(raw, "weights", {}).items()})
        total = sum(weights.values())
        if total <= 0:
            raise PlannerConfigError("policy.weights must contain at least one positive value")
        weights = {k: v / total for k, v in weights.items()}
        return cls(
            version=text_value(raw, "version", VERSION),
            provider_allowlist=text_set(get_any(raw, "provider_allowlist", "providerAllowlist", default=[])),
            provider_denylist=text_set(get_any(raw, "provider_denylist", "providerDenylist", default=[])),
            model_denylist=text_set(get_any(raw, "model_denylist", "modelDenylist", default=[])),
            region_denylist=text_set(get_any(raw, "region_denylist", "regionDenylist", default=[])),
            required_certifications_by_data_class=map_of_sets(raw, "required_certifications_by_data_class", "requiredCertificationsByDataClass"),
            allowed_residency_by_country=map_of_sets(raw, "allowed_residency_by_country", "allowedResidencyByCountry"),
            allowed_residency_by_data_class=map_of_sets(raw, "allowed_residency_by_data_class", "allowedResidencyByDataClass"),
            max_retention_days_by_data_class=map_of_ints(raw, "max_retention_days_by_data_class", "maxRetentionDaysByDataClass"),
            require_training_disabled_for_sensitive=bool_value(raw, "require_training_disabled_for_sensitive", "requireTrainingDisabledForSensitive", default=True),
            min_route_health=bounded_float(raw, "min_route_health", 0.65, 0.0, 1.0),
            default_deadline_ms=optional_float(get_any(raw, "default_deadline_ms", "defaultDeadlineMs", default=None), "default_deadline_ms", minimum=0.0),
            max_alternatives=int_value(raw, "max_alternatives", "maxAlternatives", default=3, minimum=0),
            weights=weights,
        )


@dataclass(frozen=True)
class InferenceRequest:
    request_id: str
    tenant: str
    user_country: str
    data_classes: frozenset[str]
    input_tokens: int
    output_tokens: int
    required_residency_groups: frozenset[str] = field(default_factory=frozenset)
    required_certifications: frozenset[str] = field(default_factory=frozenset)
    allowed_providers: frozenset[str] = field(default_factory=frozenset)
    denied_providers: frozenset[str] = field(default_factory=frozenset)
    preferred_regions: frozenset[str] = field(default_factory=frozenset)
    deadline_ms: float | None = None
    sticky_key: str = ""
    metadata_hash: str = ""

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any], line: int | None = None) -> "InferenceRequest":
        prefix = f"line {line}: " if line is not None else ""
        request_id = text_value(raw, "request_id", text_value(raw, "requestId", ""))
        if not request_id:
            request_id = stable_hash(canonical_json(raw))[:20]
        tenant = text_value(raw, "tenant", "default").lower()
        user_country = text_value(raw, "user_country", text_value(raw, "userCountry", "unknown")).lower()
        data_classes = text_set(get_any(raw, "data_classes", "dataClasses", default=["internal"]))
        input_tokens = int_value(raw, "input_tokens", "inputTokens", default=0, minimum=0)
        output_tokens = int_value(raw, "output_tokens", "outputTokens", default=0, minimum=0)
        if input_tokens + output_tokens <= 0:
            raise RequestParseError(f"{prefix}input_tokens + output_tokens must be positive")
        metadata = get_any(raw, "metadata", default={})
        metadata_hash = stable_hash(canonical_json(metadata)) if metadata else ""
        return cls(
            request_id=request_id,
            tenant=tenant,
            user_country=user_country,
            data_classes=data_classes,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            required_residency_groups=text_set(get_any(raw, "required_residency_groups", "requiredResidencyGroups", default=[])),
            required_certifications=text_set(get_any(raw, "required_certifications", "requiredCertifications", default=[])),
            allowed_providers=text_set(get_any(raw, "allowed_providers", "allowedProviders", default=[])),
            denied_providers=text_set(get_any(raw, "denied_providers", "deniedProviders", default=[])),
            preferred_regions=text_set(get_any(raw, "preferred_regions", "preferredRegions", default=[])),
            deadline_ms=optional_float(get_any(raw, "deadline_ms", "deadlineMs", default=None), "deadline_ms", minimum=0.0),
            sticky_key=text_value(raw, "sticky_key", text_value(raw, "stickyKey", "")),
            metadata_hash=metadata_hash,
        )

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def is_sensitive(self) -> bool:
        return bool(self.data_classes & SENSITIVE_CLASSES)


@dataclass(frozen=True)
class Candidate:
    route: Route
    score: float
    components: Mapping[str, float]
    estimated_latency_ms: float
    estimated_cost_usd: float
    estimated_carbon_g: float
    headroom_tokens: int

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "route": route_public_dict(self.route),
            "score": round(self.score, 8),
            "components": {k: round(v, 8) for k, v in sorted(self.components.items())},
            "estimated_latency_ms": round(self.estimated_latency_ms, 3),
            "estimated_cost_usd": round(self.estimated_cost_usd, 8),
            "estimated_carbon_g": round(self.estimated_carbon_g, 6),
            "headroom_tokens": self.headroom_tokens,
        }


class DataResidencyRoutePlanner:
    def __init__(self, routes: Sequence[Route], policy: Policy | None = None) -> None:
        if not routes:
            raise PlannerConfigError("at least one route is required")
        names = [route.name for route in routes]
        if len(names) != len(set(names)):
            raise PlannerConfigError("route names must be unique")
        self.routes = tuple(routes)
        self.policy = policy or Policy()
        self._reserved_tokens: dict[str, int] = {route.name: 0 for route in routes}

    @classmethod
    def from_config(cls, raw: Mapping[str, Any]) -> "DataResidencyRoutePlanner":
        route_objects = get_any(raw, "routes", default=None)
        if not isinstance(route_objects, list):
            raise PlannerConfigError("config.routes must be an array")
        routes = [Route.from_mapping(item) for item in route_objects if isinstance(item, Mapping)]
        if len(routes) != len(route_objects):
            raise PlannerConfigError("every item in config.routes must be an object")
        return cls(routes=routes, policy=Policy.from_mapping(dict_value(raw, "policy", {})))

    def decide(self, request: InferenceRequest, reserve_capacity: bool = True) -> dict[str, Any]:
        rejected: dict[str, list[str]] = {}
        eligible: list[tuple[Route, float, float, float, int]] = []
        for route in self.routes:
            reasons = self._reject_reasons(route, request)
            if reasons:
                rejected[route.name] = reasons
                continue
            latency = route.estimate_latency_ms(request)
            cost = route.estimate_cost_usd(request)
            carbon = route.estimate_carbon_g(request)
            headroom = route.capacity_tokens_per_minute - self._reserved_tokens[route.name] - request.total_tokens
            eligible.append((route, latency, cost, carbon, headroom))

        if not eligible:
            return self._unroutable_decision(request, rejected)

        candidates = self._score_candidates(request, eligible)
        selected = candidates[0]
        if reserve_capacity:
            self._reserved_tokens[selected.route.name] += request.total_tokens
        decision = {
            "status": "routed",
            "planner_version": VERSION,
            "policy_version": self.policy.version,
            "request_id": request.request_id,
            "tenant": request.tenant,
            "selected": selected.to_public_dict(),
            "alternatives": [candidate.to_public_dict() for candidate in candidates[1 : 1 + self.policy.max_alternatives]],
            "hard_rejections": rejected,
            "evidence_hash": self._evidence_hash(request, selected.route),
        }
        return decision

    def _reject_reasons(self, route: Route, request: InferenceRequest) -> list[str]:
        reasons: list[str] = []
        policy = self.policy
        if not route.enabled:
            reasons.append("route is disabled")
        if route.health < policy.min_route_health:
            reasons.append(f"route health {route.health:.3f} is below policy minimum {policy.min_route_health:.3f}")
        if policy.provider_allowlist and route.provider not in policy.provider_allowlist:
            reasons.append(f"provider {route.provider} is not in policy provider_allowlist")
        if route.provider in policy.provider_denylist or route.provider in request.denied_providers:
            reasons.append(f"provider {route.provider} is denied")
        if request.allowed_providers and route.provider not in request.allowed_providers:
            reasons.append(f"provider {route.provider} is not allowed by request")
        if route.model.lower() in policy.model_denylist:
            reasons.append(f"model {route.model} is denied")
        if route.region in policy.region_denylist:
            reasons.append(f"region {route.region} is denied")
        if route.denied_tenants and request.tenant in route.denied_tenants:
            reasons.append(f"tenant {request.tenant} is denied on route")
        if route.allowed_tenants and request.tenant not in route.allowed_tenants:
            reasons.append(f"tenant {request.tenant} is not allowed on route")
        if request.total_tokens > route.max_context_tokens:
            reasons.append(f"request needs {request.total_tokens} tokens but max_context_tokens is {route.max_context_tokens}")
        reserved = self._reserved_tokens.get(route.name, 0)
        if reserved + request.total_tokens > route.capacity_tokens_per_minute:
            reasons.append("route capacity_tokens_per_minute would be exceeded in this planning window")
        if route.allowed_data_classes and not request.data_classes <= route.allowed_data_classes:
            missing = sorted(request.data_classes - route.allowed_data_classes)
            reasons.append(f"route does not allow data classes: {', '.join(missing)}")
        if request.is_sensitive and policy.require_training_disabled_for_sensitive and not route.training_disabled:
            reasons.append("sensitive request requires training_disabled route")
        missing_certs = self._missing_certifications(route, request)
        if missing_certs:
            reasons.append(f"route is missing certifications: {', '.join(missing_certs)}")
        residency_reasons = self._residency_reasons(route, request)
        reasons.extend(residency_reasons)
        retention_reason = self._retention_reason(route, request)
        if retention_reason:
            reasons.append(retention_reason)
        deadline = request.deadline_ms if request.deadline_ms is not None else policy.default_deadline_ms
        if deadline is not None and route.estimate_latency_ms(request) > deadline:
            reasons.append(f"estimated latency {route.estimate_latency_ms(request):.1f}ms exceeds deadline {deadline:.1f}ms")
        return reasons

    def _missing_certifications(self, route: Route, request: InferenceRequest) -> list[str]:
        required = set(request.required_certifications)
        for data_class in request.data_classes:
            required.update(self.policy.required_certifications_by_data_class.get(data_class, frozenset()))
        return sorted(required - set(route.certifications))

    def _residency_reasons(self, route: Route, request: InferenceRequest) -> list[str]:
        reasons: list[str] = []
        required_sets: list[tuple[str, frozenset[str]]] = []
        if request.required_residency_groups:
            required_sets.append(("request", request.required_residency_groups))
        country_groups = self.policy.allowed_residency_by_country.get(request.user_country)
        if country_groups:
            required_sets.append((f"country:{request.user_country}", country_groups))
        for data_class in request.data_classes:
            groups = self.policy.allowed_residency_by_data_class.get(data_class)
            if groups:
                required_sets.append((f"data_class:{data_class}", groups))
        for label, groups in required_sets:
            if not route.residency_groups.intersection(groups):
                reasons.append(f"route residency {sorted(route.residency_groups)} does not satisfy {label} groups {sorted(groups)}")
        return reasons

    def _retention_reason(self, route: Route, request: InferenceRequest) -> str:
        limits = [self.policy.max_retention_days_by_data_class[data_class] for data_class in request.data_classes if data_class in self.policy.max_retention_days_by_data_class]
        if not limits:
            return ""
        limit = min(limits)
        if route.retention_days > limit:
            return f"route retention_days {route.retention_days} exceeds limit {limit}"
        return ""

    def _score_candidates(self, request: InferenceRequest, eligible: Sequence[tuple[Route, float, float, float, int]]) -> list[Candidate]:
        latencies = [item[1] for item in eligible]
        costs = [item[2] for item in eligible]
        carbons = [item[3] for item in eligible]
        headrooms = [float(item[4]) for item in eligible]
        candidates: list[Candidate] = []
        for route, latency, cost, carbon, headroom in eligible:
            components = {
                "latency": lower_is_better(latency, latencies),
                "cost": lower_is_better(cost, costs),
                "carbon": lower_is_better(carbon, carbons),
                "headroom": higher_is_better(float(headroom), headrooms),
                "residency": self._residency_affinity(route, request),
                "health": route.health,
            }
            score = sum(self.policy.weights.get(name, 0.0) * value for name, value in components.items())
            score *= max(route.weight, 0.0)
            score += stable_tiebreaker(request.sticky_key or request.request_id, route.id)
            candidates.append(Candidate(route, score, components, latency, cost, carbon, headroom))
        candidates.sort(key=lambda candidate: (-candidate.score, candidate.estimated_latency_ms, candidate.estimated_cost_usd, candidate.route.id))
        return candidates

    def _residency_affinity(self, route: Route, request: InferenceRequest) -> float:
        desired = set(request.required_residency_groups)
        desired.update(self.policy.allowed_residency_by_country.get(request.user_country, frozenset()))
        for data_class in request.data_classes:
            desired.update(self.policy.allowed_residency_by_data_class.get(data_class, frozenset()))
        if not desired:
            return 1.0
        return len(route.residency_groups.intersection(desired)) / len(desired)

    def _unroutable_decision(self, request: InferenceRequest, rejected: Mapping[str, list[str]]) -> dict[str, Any]:
        return {
            "status": "unroutable",
            "planner_version": VERSION,
            "policy_version": self.policy.version,
            "request_id": request.request_id,
            "tenant": request.tenant,
            "selected": None,
            "alternatives": [],
            "hard_rejections": rejected,
            "evidence_hash": stable_hash(canonical_json({"request": request_public_dict(request), "rejected": rejected})),
        }

    def _evidence_hash(self, request: InferenceRequest, route: Route) -> str:
        return stable_hash(canonical_json({
            "policy_version": self.policy.version,
            "request": request_public_dict(request),
            "route": route_public_dict(route),
        }))


def route_public_dict(route: Route) -> dict[str, Any]:
    return {
        "name": route.name,
        "provider": route.provider,
        "model": route.model,
        "region": route.region,
        "jurisdiction": route.jurisdiction,
        "residency_groups": sorted(route.residency_groups),
        "endpoint": route.endpoint,
        "retention_days": route.retention_days,
        "training_disabled": route.training_disabled,
        "certifications": sorted(route.certifications),
        "tags": sorted(route.tags),
    }


def request_public_dict(request: InferenceRequest) -> dict[str, Any]:
    return {
        "request_id": request.request_id,
        "tenant": request.tenant,
        "user_country": request.user_country,
        "data_classes": sorted(request.data_classes),
        "input_tokens": request.input_tokens,
        "output_tokens": request.output_tokens,
        "required_residency_groups": sorted(request.required_residency_groups),
        "required_certifications": sorted(request.required_certifications),
        "allowed_providers": sorted(request.allowed_providers),
        "denied_providers": sorted(request.denied_providers),
        "preferred_regions": sorted(request.preferred_regions),
        "deadline_ms": request.deadline_ms,
        "metadata_hash": request.metadata_hash,
    }


def load_json_file(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except OSError as exc:
        raise PlannerConfigError(f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise PlannerConfigError(f"invalid JSON in {path}: {exc}") from exc


def iter_jsonl(handle: Iterable[str]) -> Iterable[tuple[int, Mapping[str, Any]]]:
    for line_number, line in enumerate(handle, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise RequestParseError(f"line {line_number}: invalid JSON: {exc}") from exc
        if not isinstance(parsed, Mapping):
            raise RequestParseError(f"line {line_number}: request must be a JSON object")
        yield line_number, parsed


def read_requests(path: str | None) -> list[InferenceRequest]:
    if path is None or path == "-":
        source = sys.stdin
        close = False
    else:
        try:
            source = open(path, "r", encoding="utf-8")
        except OSError as exc:
            raise RequestParseError(f"cannot read {path}: {exc}") from exc
        close = True
    try:
        return [InferenceRequest.from_mapping(raw, line) for line, raw in iter_jsonl(source)]
    finally:
        if close:
            source.close()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def stable_tiebreaker(key: str, route_id: str) -> float:
    digest = hashlib.blake2b(f"{key}\0{route_id}".encode("utf-8"), digest_size=8).digest()
    value = int.from_bytes(digest, "big") / float(2**64 - 1)
    return value * 1e-9


def lower_is_better(value: float, values: Sequence[float]) -> float:
    finite = [item for item in values if math.isfinite(item)]
    if not finite:
        return 0.0
    lo = min(finite)
    hi = max(finite)
    if hi == lo:
        return 1.0
    return 1.0 - ((value - lo) / (hi - lo))


def higher_is_better(value: float, values: Sequence[float]) -> float:
    finite = [item for item in values if math.isfinite(item)]
    if not finite:
        return 0.0
    lo = min(finite)
    hi = max(finite)
    if hi == lo:
        return 1.0
    return (value - lo) / (hi - lo)


def get_any(raw: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in raw:
            return raw[name]
    return default


def require_text(raw: Mapping[str, Any], name: str) -> str:
    value = text_value(raw, name, "")
    if not value:
        raise PlannerConfigError(f"route.{name} is required")
    return value


def text_value(raw: Mapping[str, Any], name: str, default: str) -> str:
    value = raw.get(name, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise PlannerConfigError(f"{name} must be a string")
    return value.strip()


def text_set(value: Any) -> frozenset[str]:
    if value is None:
        return frozenset()
    if isinstance(value, str):
        values = [item.strip() for item in value.split(",")]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        values = []
        for item in value:
            if not isinstance(item, str):
                raise PlannerConfigError("string lists must contain only strings")
            values.append(item.strip())
    else:
        raise PlannerConfigError("expected a string or list of strings")
    return frozenset(item.lower() for item in values if item)


def bool_value(raw: Mapping[str, Any], *names: str, default: bool) -> bool:
    value = get_any(raw, *names, default=default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    raise PlannerConfigError(f"{names[0]} must be a boolean")


def int_value(raw: Mapping[str, Any], *names: str, default: int, minimum: int | None = None) -> int:
    value = get_any(raw, *names, default=default)
    if isinstance(value, bool):
        raise PlannerConfigError(f"{names[0]} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise PlannerConfigError(f"{names[0]} must be an integer") from exc
    if minimum is not None and parsed < minimum:
        raise PlannerConfigError(f"{names[0]} must be >= {minimum}")
    return parsed


def float_value(raw: Mapping[str, Any], *names: str, default: float, minimum: float | None = None) -> float:
    value = get_any(raw, *names, default=default)
    return parse_float(value, names[0], minimum=minimum)


def bounded_float(raw: Mapping[str, Any], name: str, default: float, lower: float, upper: float) -> float:
    parsed = parse_float(raw.get(name, default), name, minimum=lower)
    if parsed > upper:
        raise PlannerConfigError(f"{name} must be <= {upper}")
    return parsed


def parse_float(value: Any, name: str, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise PlannerConfigError(f"{name} must be a number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise PlannerConfigError(f"{name} must be a number") from exc
    if not math.isfinite(parsed):
        raise PlannerConfigError(f"{name} must be finite")
    if minimum is not None and parsed < minimum:
        raise PlannerConfigError(f"{name} must be >= {minimum}")
    return parsed


def optional_float(value: Any, name: str, minimum: float | None = None) -> float | None:
    if value is None or value == "":
        return None
    return parse_float(value, name, minimum=minimum)


def dict_value(raw: Mapping[str, Any], name: str, default: Mapping[str, Any]) -> Mapping[str, Any]:
    value = raw.get(name, default)
    if not isinstance(value, Mapping):
        raise PlannerConfigError(f"{name} must be an object")
    return value


def map_of_sets(raw: Mapping[str, Any], snake: str, camel: str) -> dict[str, frozenset[str]]:
    value = get_any(raw, snake, camel, default={})
    if not isinstance(value, Mapping):
        raise PlannerConfigError(f"{snake} must be an object")
    return {str(key).lower(): text_set(item) for key, item in value.items()}


def map_of_ints(raw: Mapping[str, Any], snake: str, camel: str) -> dict[str, int]:
    value = get_any(raw, snake, camel, default={})
    if not isinstance(value, Mapping):
        raise PlannerConfigError(f"{snake} must be an object")
    parsed: dict[str, int] = {}
    for key, item in value.items():
        try:
            number = int(item)
        except (TypeError, ValueError) as exc:
            raise PlannerConfigError(f"{snake}.{key} must be an integer") from exc
        if number < 0:
            raise PlannerConfigError(f"{snake}.{key} must be >= 0")
        parsed[str(key).lower()] = number
    return parsed


def build_sample_config() -> dict[str, Any]:
    return {
        "policy": {
            "version": "customer-prod-2026-04",
            "allowed_residency_by_country": {"de": ["eu"], "fr": ["eu"], "us": ["us"]},
            "allowed_residency_by_data_class": {"phi": ["us"], "pci": ["us", "eu"]},
            "required_certifications_by_data_class": {"phi": ["hipaa"], "pci": ["pci-dss"]},
            "max_retention_days_by_data_class": {"pii": 0, "phi": 0, "pci": 1},
            "require_training_disabled_for_sensitive": True,
            "default_deadline_ms": 2400,
        },
        "routes": [
            {
                "name": "eu-private-fast",
                "provider": "acme-ai",
                "model": "large-2026-04",
                "region": "eu-west-1",
                "jurisdiction": "eu",
                "residency_groups": ["eu"],
                "latency_p95_ms": 900,
                "decode_ms_per_output_token": 1.4,
                "capacity_tokens_per_minute": 600000,
                "cost_usd_per_1k_input": 0.002,
                "cost_usd_per_1k_output": 0.008,
                "carbon_g_per_1k_tokens": 1.8,
                "retention_days": 0,
                "training_disabled": True,
                "certifications": ["iso27001", "soc2", "pci-dss"],
            },
            {
                "name": "us-health-hipaa",
                "provider": "acme-ai",
                "model": "large-2026-04",
                "region": "us-east-1",
                "jurisdiction": "us",
                "residency_groups": ["us"],
                "latency_p95_ms": 1100,
                "decode_ms_per_output_token": 1.2,
                "capacity_tokens_per_minute": 800000,
                "cost_usd_per_1k_input": 0.0018,
                "cost_usd_per_1k_output": 0.0075,
                "carbon_g_per_1k_tokens": 2.5,
                "retention_days": 0,
                "training_disabled": True,
                "certifications": ["iso27001", "soc2", "hipaa", "pci-dss"],
            },
        ],
    }


def run_self_test() -> None:
    planner = DataResidencyRoutePlanner.from_config(build_sample_config())
    eu_request = InferenceRequest.from_mapping({
        "request_id": "req-eu-1",
        "tenant": "alpha",
        "user_country": "DE",
        "data_classes": ["pii"],
        "input_tokens": 1200,
        "output_tokens": 400,
    })
    eu_decision = planner.decide(eu_request, reserve_capacity=False)
    assert eu_decision["status"] == "routed", eu_decision
    assert eu_decision["selected"]["route"]["name"] == "eu-private-fast", eu_decision
    phi_request = InferenceRequest.from_mapping({
        "request_id": "req-phi-1",
        "tenant": "clinic",
        "user_country": "US",
        "data_classes": ["phi"],
        "input_tokens": 1800,
        "output_tokens": 700,
    })
    phi_decision = planner.decide(phi_request, reserve_capacity=False)
    assert phi_decision["status"] == "routed", phi_decision
    assert phi_decision["selected"]["route"]["name"] == "us-health-hipaa", phi_decision


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan compliant AI inference routes from JSON config and JSONL requests.")
    parser.add_argument("--config", help="Path to planner config JSON. Required unless --sample-config or --self-test is used.")
    parser.add_argument("--requests", default="-", help="Path to request JSONL, or '-' for stdin. Default: stdin.")
    parser.add_argument("--no-reserve", action="store_true", help="Do not reserve route capacity while processing the JSONL stream.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print each decision JSON object.")
    parser.add_argument("--sample-config", action="store_true", help="Print a production-shaped sample config and exit.")
    parser.add_argument("--self-test", action="store_true", help="Run built-in assertions and exit.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.sample_config:
        print(json.dumps(build_sample_config(), indent=2, sort_keys=True))
        return 0
    if args.self_test:
        run_self_test()
        return 0
    if not args.config:
        raise PlannerConfigError("--config is required")
    config = load_json_file(args.config)
    if not isinstance(config, Mapping):
        raise PlannerConfigError("config root must be a JSON object")
    planner = DataResidencyRoutePlanner.from_config(config)
    routed = True
    for request in read_requests(args.requests):
        decision = planner.decide(request, reserve_capacity=not args.no_reserve)
        routed = routed and decision["status"] == "routed"
        if args.pretty:
            print(json.dumps(decision, indent=2, sort_keys=True))
        else:
            print(canonical_json(decision))
    return 0 if routed else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (PlannerConfigError, RequestParseError) as exc:
        print(f"DataResidencyRoutePlanner: {exc}", file=sys.stderr)
        raise SystemExit(2)


# Explanation:
# This solves the April 2026 problem where an AI gateway, MCP server, RAG service, or agent platform has to choose a model route without accidentally sending PII, PHI, PCI, secrets, customer documents, or regulated prompts to the wrong country, provider, retention bucket, or training-enabled endpoint. Built because Pavan has seen teams keep this logic in scattered YAML, random feature flags, and tribal memory, then discover during review that cost routing broke data residency or a fallback path ignored HIPAA, PCI-DSS, EU, US, tenant, latency, carbon, and capacity rules. Use it when you need a plain Python data residency route planner for LLM infrastructure, AI inference routing, sovereign cloud deployments, edge compute, OpenTelemetry request pipelines, GitHub Actions checks, CI policy gates, internal developer platforms, or production AI governance where every routing decision needs a hard rejection list and a searchable evidence hash. The trick: hard legal and customer rules run before scoring, then the remaining routes are ranked with deterministic latency, token cost, carbon, capacity headroom, residency affinity, provider health, and stable sticky tie breaking, so the cheapest route never wins if it is not allowed. Drop this into an AI gateway, FastAPI middleware, Kubernetes admission job, batch replay tool, Vercel or Netlify function, Datadog log pipeline, or research eval harness as a small auditable file that makes model routing explainable enough for engineers, security reviewers, and platform owners to trust.
