#!/usr/bin/env python3
'''
AgentEgressPolicyGate audits AI agent, MCP, browser automation, and CI egress
traces before those traces are trusted, replayed, archived, or promoted. It is a
single-file, dependency-free Python CLI because incident runners and locked-down
CI jobs should not need a package install just to answer where an agent sent
data, what it carried, and whether that call broke policy.
'''

from __future__ import annotations

import argparse
import csv
import fnmatch
import hashlib
import ipaddress
import json
import re
import sys
import time
from dataclasses import dataclass
from io import StringIO
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


VERSION = '1.0.0'
SEVERITIES = ('critical', 'high', 'medium', 'low')
URL_KEYS = {'url', 'uri', 'href', 'endpoint', 'target_url', 'destination', 'egress_url', 'webhook'}
METHOD_KEYS = {'method', 'http_method', 'verb', 'request_method'}
REGION_KEYS = {'region', 'cloud_region', 'residency_region', 'data_region', 'location'}
BODY_KEYS = {'body', 'payload', 'arguments', 'args', 'content', 'prompt', 'input', 'message'}
NETWORK_HINTS = ('http', 'fetch', 'request', 'curl', 'browser', 'mcp', 'webhook', 'slack', 'email', 'drive', 'sql', 's3', 'queue')
SENSITIVE_KEYS = {
    'authorization',
    'cookie',
    'set-cookie',
    'x-api-key',
    'api_key',
    'apikey',
    'access_token',
    'refresh_token',
    'id_token',
    'password',
    'secret',
    'client_secret',
    'private_key',
}


DEFAULT_POLICY: Dict[str, Any] = {
    'allow_unlisted_domains': False,
    'allowed_domains': [],
    'denied_domains': [
        '*.ngrok-free.app',
        '*.trycloudflare.com',
        '*.loca.lt',
        '*.localtunnel.me',
        'pastebin.com',
        '*.pastebin.com',
        'webhook.site',
        '*.webhook.site',
        'requestbin.com',
        '*.requestbin.com',
    ],
    'allow_private_networks': False,
    'require_https': True,
    'allowed_methods': ['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE'],
    'blocked_methods': ['CONNECT', 'TRACE'],
    'required_headers': ['x-request-id'],
    'require_idempotency_for_methods': ['POST', 'PUT', 'PATCH', 'DELETE'],
    'idempotency_headers': ['idempotency-key', 'x-idempotency-key'],
    'allowed_regions': [],
    'safe_content_types': ['application/json', 'application/x-ndjson', 'text/plain', 'text/csv'],
    'max_payload_bytes': 262144,
    'max_tokens_per_event': 128000,
    'max_cost_usd_per_event': 5.0,
    'max_timeout_ms': 60000,
    'fail_on': ['critical', 'high'],
    'domain_owners': {},
    'redact_fields': sorted(SENSITIVE_KEYS),
    'warn_on_prompt_injection_text': True,
    'secret_patterns': [
        {'name': 'Generic API token', 'regex': r'(?i)\b(api[_-]?key|access[_-]?token|secret|bearer)\b\s*[:=]\s*[\'"]?([A-Za-z0-9._~+/=-]{16,})'},
        {'name': 'OpenAI style key', 'regex': r'\bsk-[A-Za-z0-9_-]{20,}\b'},
        {'name': 'GitHub token', 'regex': r'\bgh[pousr]_[A-Za-z0-9_]{20,}\b'},
        {'name': 'AWS access key', 'regex': r'\bAKIA[0-9A-Z]{16}\b'},
        {'name': 'JWT', 'regex': r'\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b'},
    ],
}


class GateError(ValueError):
    pass


@dataclass
class Event:
    index: int
    source: str
    raw: Dict[str, Any]
    tool: Optional[str]
    url: Optional[str]
    method: str
    headers: Dict[str, str]
    content_type: Optional[str]
    region: Optional[str]
    timeout_ms: Optional[int]
    payload_bytes: int
    total_tokens: int
    cost_usd: Optional[float]
    record_hash: str


@dataclass
class Finding:
    rule_id: str
    severity: str
    message: str
    event_index: int
    source: str
    remediation: str
    evidence: Dict[str, Any]

    def to_dict(self, fail_on: Sequence[str]) -> Dict[str, Any]:
        return {
            'rule_id': self.rule_id,
            'severity': self.severity,
            'blocking': self.severity in set(fail_on),
            'event_index': self.event_index,
            'source': self.source,
            'message': self.message,
            'remediation': self.remediation,
            'evidence': {k: v for k, v in self.evidence.items() if v not in (None, '', [], {})},
        }


def compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(compact_json(value).encode('utf-8')).hexdigest()[:16]


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = deep_merge(result[key], value)  # type: ignore[arg-type]
        else:
            result[key] = value
    return result


def read_json(path: str) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            return json.load(handle)
    except json.JSONDecodeError as exc:
        raise GateError(f'{path}: invalid JSON at line {exc.lineno}: {exc.msg}') from exc
    except OSError as exc:
        raise GateError(f'{path}: {exc}') from exc


def load_policy(path: Optional[str]) -> Dict[str, Any]:
    policy = dict(DEFAULT_POLICY)
    if path:
        loaded = read_json(path)
        if not isinstance(loaded, Mapping):
            raise GateError('policy file must contain a JSON object')
        policy = deep_merge(policy, loaded)
    validate_policy(policy)
    return policy


def validate_policy(policy: Mapping[str, Any]) -> None:
    list_fields = (
        'allowed_domains',
        'denied_domains',
        'allowed_methods',
        'blocked_methods',
        'required_headers',
        'require_idempotency_for_methods',
        'idempotency_headers',
        'allowed_regions',
        'safe_content_types',
        'fail_on',
        'redact_fields',
    )
    for field in list_fields:
        if not isinstance(policy.get(field), list):
            raise GateError(f'policy.{field} must be a list')
    for field in ('max_payload_bytes', 'max_tokens_per_event', 'max_timeout_ms'):
        number = to_int(policy.get(field))
        if number is None or number < 0:
            raise GateError(f'policy.{field} must be a non-negative integer')
    cost = to_float(policy.get('max_cost_usd_per_event'))
    if cost is None or cost < 0:
        raise GateError('policy.max_cost_usd_per_event must be a non-negative number')
    for entry in policy.get('secret_patterns', []):
        if not isinstance(entry, Mapping) or not isinstance(entry.get('regex'), str):
            raise GateError('each policy.secret_patterns item needs a regex')
        try:
            re.compile(entry['regex'])
        except re.error as exc:
            raise GateError(f'invalid secret pattern {entry.get("name", "unnamed")}: {exc}') from exc


def iter_records(path: str) -> Iterator[Tuple[int, Dict[str, Any]]]:
    if path == '-':
        content = sys.stdin.read()
        label = '<stdin>'
    else:
        try:
            with open(path, 'r', encoding='utf-8-sig', newline='') as handle:
                content = handle.read()
        except OSError as exc:
            raise GateError(f'{path}: {exc}') from exc
        label = path
    text = content.strip()
    if not text:
        return
    if text[0] in '[{':
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise GateError(f'{label}: invalid JSON at line {exc.lineno}: {exc.msg}') from exc
        for index, item in enumerate(extract_records(parsed), 1):
            yield index, dict(item) if isinstance(item, Mapping) else {'value': item}
        return
    if looks_like_csv(text):
        reader = csv.DictReader(text.splitlines())
        for index, row in enumerate(reader, 1):
            yield index, dict(row)
        return
    for index, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise GateError(f'{label}:{index}: invalid JSONL record: {exc.msg}') from exc
        yield index, dict(parsed) if isinstance(parsed, Mapping) else {'value': parsed}


def extract_records(parsed: Any) -> List[Any]:
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, Mapping):
        for key in ('events', 'data', 'records', 'trace', 'logs', 'tool_calls', 'requests'):
            if isinstance(parsed.get(key), list):
                return list(parsed[key])
        return [parsed]
    return [{'value': parsed}]


def looks_like_csv(text: str) -> bool:
    first = {cell.strip().lower() for cell in text.splitlines()[0].split(',')}
    return bool(first & (URL_KEYS | METHOD_KEYS | {'tool', 'tool_name', 'name'}))


def flatten(value: Any, prefix: str = '') -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(value, Mapping):
        for key, nested in value.items():
            leaf = str(key)
            path = f'{prefix}.{leaf}' if prefix else leaf
            out[path] = nested
            out[leaf] = nested
            out.update(flatten(nested, path))
    elif isinstance(value, list):
        for index, nested in enumerate(value[:50]):
            path = f'{prefix}.{index}' if prefix else str(index)
            out[path] = nested
            out.update(flatten(nested, path))
    return out


def scalar(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return compact_json(value)


def first_by_leaf(flat: Mapping[str, Any], names: Iterable[str]) -> Any:
    wanted = {name.lower() for name in names}
    for key, value in flat.items():
        if key.rsplit('.', 1)[-1].lower() in wanted:
            return value
    return None


def first_text(flat: Mapping[str, Any], names: Iterable[str]) -> Optional[str]:
    value = first_by_leaf(flat, names)
    if value is None:
        return None
    text = scalar(value).strip()
    return text or None


def first_url(flat: Mapping[str, Any]) -> Optional[str]:
    direct = first_text(flat, URL_KEYS)
    if direct:
        return direct
    matcher = re.compile(r'https?://[^\s\'"<>]+', re.I)
    for value in flat.values():
        if isinstance(value, str):
            match = matcher.search(value)
            if match:
                return match.group(0).rstrip(').,;')
    return None


def normalize_headers(raw: Mapping[str, Any], flat: Mapping[str, Any]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for key, value in flat.items():
        lower = key.lower()
        if lower.endswith('headers') and isinstance(value, Mapping):
            for header, header_value in value.items():
                headers[str(header).lower()] = scalar(header_value)
        elif lower.startswith('headers.') or lower.startswith('request.headers.'):
            headers[lower.rsplit('.', 1)[-1]] = scalar(value)
    if 'authorization' in raw and 'authorization' not in headers:
        headers['authorization'] = scalar(raw['authorization'])
    return headers


def parse_timeout(value: Any) -> Optional[int]:
    if value in (None, ''):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().lower()
    try:
        if text.endswith('ms'):
            return int(float(text[:-2]))
        if text.endswith('s'):
            return int(float(text[:-1]) * 1000)
        return int(float(text))
    except ValueError:
        return None


def to_int(value: Any) -> Optional[int]:
    if value in (None, ''):
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> Optional[float]:
    if value in (None, ''):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def payload_size(raw: Mapping[str, Any], flat: Mapping[str, Any]) -> int:
    payloads = [scalar(value) for key, value in flat.items() if key.rsplit('.', 1)[-1].lower() in BODY_KEYS]
    payloads = payloads or [compact_json(raw)]
    return max(len(value.encode('utf-8', errors='replace')) for value in payloads)


def token_count(flat: Mapping[str, Any]) -> int:
    direct = to_int(flat.get('total_tokens') or flat.get('usage.total_tokens') or flat.get('tokens.total'))
    if direct is not None:
        return direct
    total = 0
    for key, value in flat.items():
        if key.rsplit('.', 1)[-1].lower() in {'prompt_tokens', 'completion_tokens', 'input_tokens', 'output_tokens', 'cached_tokens'}:
            total += to_int(value) or 0
    return total


def normalize_event(index: int, raw: Dict[str, Any], source: str) -> Event:
    flat = flatten(raw)
    method = (first_text(flat, METHOD_KEYS) or 'GET').upper()
    if not re.fullmatch(r'[A-Z]{2,12}', method):
        method = 'GET'
    return Event(
        index=index,
        source=source,
        raw=raw,
        tool=first_text(flat, ('tool', 'tool_name', 'name', 'type', 'operation', 'span.name')),
        url=first_url(flat),
        method=method,
        headers=normalize_headers(raw, flat),
        content_type=first_text(flat, ('content_type', 'content-type', 'mime', 'mime_type')),
        region=first_text(flat, REGION_KEYS),
        timeout_ms=parse_timeout(first_by_leaf(flat, ('timeout', 'timeout_ms', 'request_timeout', 'deadline_ms'))),
        payload_bytes=payload_size(raw, flat),
        total_tokens=token_count(flat),
        cost_usd=to_float(flat.get('cost_usd') or flat.get('estimated_cost_usd') or flat.get('usage.cost_usd')),
        record_hash=stable_hash(raw),
    )


def host_of(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    host = urlsplit(url).hostname
    if not host:
        return None
    host = host.strip().lower().rstrip('.')
    return host[1:-1] if host.startswith('[') and host.endswith(']') else host


def host_matches(host: str, patterns: Iterable[str]) -> bool:
    for raw in patterns:
        pattern = str(raw).lower().rstrip('.')
        if fnmatch.fnmatchcase(host, pattern) or (pattern.startswith('*.') and host == pattern[2:]):
            return True
    return False


def private_host(host: str) -> bool:
    if host in {'localhost', 'localhost.localdomain'} or host.endswith('.local'):
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return address.is_private or address.is_loopback or address.is_link_local or address.is_reserved


def contains_secret(text: str, policy: Mapping[str, Any]) -> bool:
    for entry in policy.get('secret_patterns', []):
        try:
            if re.search(str(entry['regex']), text):
                return True
        except re.error:
            continue
    return False


def redact_text(text: str, policy: Mapping[str, Any]) -> str:
    redacted = text
    for entry in policy.get('secret_patterns', []):
        try:
            redacted = re.sub(str(entry['regex']), '[REDACTED]', redacted)
        except re.error:
            pass
    return redacted if len(redacted) <= 240 else redacted[:240] + '...[truncated]'


def redact_url(url: str, policy: Mapping[str, Any]) -> str:
    parts = urlsplit(url)
    sensitive = {str(item).lower() for item in policy.get('redact_fields', [])} | SENSITIVE_KEYS
    query = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        query.append((key, '[REDACTED]' if key.lower() in sensitive or contains_secret(value, policy) else value))
    netloc = parts.netloc
    if '@' in netloc:
        netloc = '[REDACTED]@' + netloc.rsplit('@', 1)[-1]
    return urlunsplit((parts.scheme, netloc, parts.path, urlencode(query), parts.fragment))


def injection_text(raw: Mapping[str, Any]) -> bool:
    text = compact_json(raw).lower()
    needles = ('ignore previous instructions', 'system prompt', 'reveal your prompt', 'exfiltrate', 'send secrets', 'disable safety')
    return any(needle in text for needle in needles)


def add_finding(findings: List[Finding], event: Event, rule: str, severity: str, message: str, remediation: str, **evidence: Any) -> None:
    findings.append(Finding(rule, severity, message, event.index, event.source, remediation, evidence))


def audit(event: Event, policy: Mapping[str, Any]) -> List[Finding]:
    findings: List[Finding] = []
    split = urlsplit(event.url or '')
    host = host_of(event.url)
    network_like = bool(event.url) or any(hint in ' '.join(map(str, (event.tool, event.raw.get('type'), event.raw.get('name')))).lower() for hint in NETWORK_HINTS)

    if network_like and not event.url:
        add_finding(findings, event, 'MissingDestination', 'medium', 'A network-capable tool call has no destination URL.', 'Log request.url or destination on every tool call.', tool=event.tool, record_hash=event.record_hash)
    if event.url and policy.get('require_https', True) and split.scheme.lower() != 'https':
        add_finding(findings, event, 'InsecureTransport', 'high', 'The request uses a non-HTTPS destination.', 'Use HTTPS or document a narrow exception.', url=redact_url(event.url, policy), scheme=split.scheme)
    if host and host_matches(host, policy.get('denied_domains', [])):
        add_finding(findings, event, 'DeniedDestination', 'critical', 'The destination matches an explicitly denied egress domain.', 'Remove the call or route it through an approved integration.', host=host)
    if host and not policy.get('allow_unlisted_domains', False) and not host_matches(host, policy.get('allowed_domains', [])):
        add_finding(findings, event, 'UnlistedDestination', 'high', 'The destination is not in the approved egress allowlist.', 'Add the domain only after owner, purpose, and data class are reviewed.', host=host)
    if host and private_host(host) and not policy.get('allow_private_networks', False):
        add_finding(findings, event, 'PrivateNetworkEgress', 'critical', 'The request targets localhost, link-local, loopback, or private network space.', 'Block private network egress for hosted agents unless the runner is isolated.', host=host)
    if event.url and contains_secret(event.url, policy):
        add_finding(findings, event, 'SecretInUrl', 'critical', 'The destination URL appears to contain a credential.', 'Move credentials to secret storage and rotate exposed values.', url=redact_url(event.url, policy))
    if event.method in set(policy.get('blocked_methods', [])):
        add_finding(findings, event, 'BlockedHttpMethod', 'high', 'The request uses a method blocked by policy.', 'Use a safer method or add a reviewed exception.', method=event.method)
    elif policy.get('allowed_methods') and event.method not in set(policy.get('allowed_methods', [])):
        add_finding(findings, event, 'UnexpectedHttpMethod', 'medium', 'The request method is not allowed by policy.', 'Tighten the adapter or add the method with a reason.', method=event.method)

    for header in policy.get('required_headers', []):
        if str(header).lower() not in event.headers:
            add_finding(findings, event, 'MissingRequiredHeader', 'low', 'A required request header is missing.', 'Add the header so incident review can correlate egress to a run.', header=str(header).lower())
    if event.method in set(policy.get('require_idempotency_for_methods', [])):
        accepted = {str(header).lower() for header in policy.get('idempotency_headers', [])}
        if accepted and not accepted.intersection(event.headers):
            add_finding(findings, event, 'MissingIdempotencyKey', 'medium', 'A mutating request has no idempotency key.', 'Attach idempotency keys to mutating agent requests.', method=event.method)
    for header, value in event.headers.items():
        if header in SENSITIVE_KEYS or contains_secret(value, policy):
            add_finding(findings, event, 'SensitiveHeaderLogged', 'critical', 'A sensitive request header is present in the trace.', 'Redact trace storage and rotate any leaked credential.', header=header, value=redact_text(value, policy))

    allowed_regions = {str(region).lower() for region in policy.get('allowed_regions', [])}
    if allowed_regions and event.region and event.region.lower() not in allowed_regions:
        add_finding(findings, event, 'RegionNotAllowed', 'high', 'The event ran in or targeted a region outside the approved list.', 'Pin the model route, queue, or worker pool to an allowed region.', region=event.region)
    content_type = (event.content_type or event.headers.get('content-type') or '').split(';', 1)[0].lower()
    safe_types = {str(item).lower() for item in policy.get('safe_content_types', [])}
    if content_type and safe_types and content_type not in safe_types:
        add_finding(findings, event, 'UnexpectedContentType', 'medium', 'The request content type is outside the safe list.', 'Use structured content types that downstream policy can inspect.', content_type=content_type)
    if event.payload_bytes > int(policy.get('max_payload_bytes', 0)):
        add_finding(findings, event, 'PayloadBudgetExceeded', 'high', 'The request payload is larger than the configured budget.', 'Chunk the payload or require human approval for large exports.', payload_bytes=event.payload_bytes)
    if event.total_tokens > int(policy.get('max_tokens_per_event', 0)):
        add_finding(findings, event, 'TokenBudgetExceeded', 'high', 'The event exceeds the maximum token budget.', 'Route large model calls through approval, caching, or chunking.', total_tokens=event.total_tokens)
    if event.cost_usd is not None and event.cost_usd > float(policy.get('max_cost_usd_per_event', 0.0)):
        add_finding(findings, event, 'CostBudgetExceeded', 'high', 'The event exceeds the maximum estimated cost.', 'Apply model routing or approval before unattended runs.', cost_usd=round(event.cost_usd, 6))
    if event.timeout_ms and event.timeout_ms > int(policy.get('max_timeout_ms', 0)):
        add_finding(findings, event, 'TimeoutTooLarge', 'medium', 'The request timeout is longer than policy allows.', 'Lower the timeout and use retryable jobs for slow integrations.', timeout_ms=event.timeout_ms)
    if contains_secret(compact_json(event.raw), policy):
        add_finding(findings, event, 'SecretInTracePayload', 'critical', 'The trace payload contains text matching a secret pattern.', 'Store sanitized traces and rotate any exposed credentials.', record_hash=event.record_hash)
    if policy.get('warn_on_prompt_injection_text', True) and injection_text(event.raw):
        add_finding(findings, event, 'PromptInjectionForwarded', 'medium', 'Prompt-injection text appears to cross a tool boundary.', 'Classify retrieved instructions before forwarding them to tools.', tool=event.tool, destination=host)
    return findings


def sanitize(value: Any, policy: Mapping[str, Any]) -> Any:
    sensitive = {str(item).lower() for item in policy.get('redact_fields', [])} | SENSITIVE_KEYS
    if isinstance(value, Mapping):
        return {str(key): ('[REDACTED]' if str(key).lower() in sensitive else sanitize(nested, policy)) for key, nested in value.items()}
    if isinstance(value, list):
        return [sanitize(item, policy) for item in value]
    if isinstance(value, str):
        return redact_text(value, policy)
    return value


def build_report(events: Sequence[Event], findings: Sequence[Finding], policy: Mapping[str, Any]) -> Dict[str, Any]:
    fail_on = policy.get('fail_on', [])
    finding_rows = [finding.to_dict(fail_on) for finding in findings]
    counts = {severity: 0 for severity in SEVERITIES}
    for row in finding_rows:
        counts[row['severity']] = counts.get(row['severity'], 0) + 1
    blockers = [row for row in finding_rows if row['blocking']]
    destinations = sorted({host_of(event.url) for event in events if host_of(event.url)})
    return {
        'tool': 'AgentEgressPolicyGate',
        'version': VERSION,
        'generated_at_epoch': int(time.time()),
        'pass': not blockers,
        'summary': {
            'event_count': len(events),
            'destination_count': len(destinations),
            'finding_count': len(finding_rows),
            'blocker_count': len(blockers),
            'counts_by_severity': counts,
            'destinations': destinations,
        },
        'findings': finding_rows,
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    summary = report['summary']
    lines = [
        '# Agent egress policy gate',
        '',
        f'Pass: {str(report["pass"]).lower()}',
        f'Events: {summary["event_count"]}',
        f'Destinations: {summary["destination_count"]}',
        f'Findings: {summary["finding_count"]}',
        f'Blockers: {summary["blocker_count"]}',
        '',
        '## Findings',
    ]
    if not report['findings']:
        lines.append('No findings under the active policy.')
    for finding in report['findings']:
        lines.append(f'- [{finding["severity"]}] {finding["rule_id"]} event {finding["event_index"]}: {finding["message"]} Remediation: {finding["remediation"]} Evidence: {compact_json(finding["evidence"])}')
    return '\n'.join(lines) + '\n'


def render_csv(report: Mapping[str, Any]) -> str:
    output = StringIO()
    fields = ['rule_id', 'severity', 'blocking', 'event_index', 'source', 'message', 'remediation', 'evidence']
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for row in report['findings']:
        writer.writerow({field: compact_json(row[field]) if field == 'evidence' else row.get(field, '') for field in fields})
    return output.getvalue()


def render_sarif(report: Mapping[str, Any]) -> str:
    rules: Dict[str, Dict[str, Any]] = {}
    results = []
    for finding in report['findings']:
        rule_id = finding['rule_id']
        rules.setdefault(rule_id, {'id': rule_id, 'name': rule_id, 'shortDescription': {'text': finding['message']}, 'help': {'text': finding['remediation']}})
        results.append({
            'ruleId': rule_id,
            'level': 'error' if finding['severity'] in {'critical', 'high'} else 'warning' if finding['severity'] == 'medium' else 'note',
            'message': {'text': f'{finding["message"]} {finding["remediation"]}'},
            'locations': [{'physicalLocation': {'artifactLocation': {'uri': str(finding['source'])}, 'region': {'startLine': int(finding['event_index'])}}}],
            'properties': {'severity': finding['severity'], 'blocking': finding['blocking'], 'evidence': finding['evidence']},
        })
    sarif = {
        'version': '2.1.0',
        '$schema': 'https://json.schemastore.org/sarif-2.1.0.json',
        'runs': [{'tool': {'driver': {'name': 'AgentEgressPolicyGate', 'informationUri': 'https://github.com/kspavankrishna/VIBE-CODE', 'rules': list(rules.values())}}, 'results': results}],
    }
    return json.dumps(sarif, indent=2, sort_keys=True, ensure_ascii=True) + '\n'


def write_text(path: Optional[str], content: str) -> None:
    if not path or path == '-':
        sys.stdout.write(content)
        return
    with open(path, 'w', encoding='utf-8', newline='') as handle:
        handle.write(content)


def write_sanitized(path: str, records: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]) -> None:
    with open(path, 'w', encoding='utf-8', newline='\n') as handle:
        for record in records:
            handle.write(json.dumps(sanitize(record, policy), sort_keys=True, ensure_ascii=True))
            handle.write('\n')


def parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Audit AI agent, MCP, browser, and CI egress traces against a production policy.')
    parser.add_argument('--trace', required=True, help='Trace input path: JSON, JSONL, CSV, or - for stdin.')
    parser.add_argument('--policy', help='Optional JSON policy file. Defaults are strict.')
    parser.add_argument('--format', choices=('json', 'markdown', 'csv', 'sarif'), default='json')
    parser.add_argument('--output', help='Write report to this path instead of stdout.')
    parser.add_argument('--redacted-output', help='Write sanitized JSONL records for sharing.')
    parser.add_argument('--allow-unlisted-domains', action='store_true', help='Override policy.allow_unlisted_domains for discovery runs.')
    parser.add_argument('--fail-on', help='Comma-separated severities that should exit 1, such as critical,high,medium.')
    parser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')
    return parser.parse_args(argv)


def run(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        policy = load_policy(args.policy)
        if args.allow_unlisted_domains:
            policy['allow_unlisted_domains'] = True
        if args.fail_on:
            policy['fail_on'] = [item.strip().lower() for item in args.fail_on.split(',') if item.strip()]
        records = list(iter_records(args.trace))
        events = [normalize_event(index, raw, args.trace) for index, raw in records]
        findings = [finding for event in events for finding in audit(event, policy)]
        report = build_report(events, findings, policy)
        content = {
            'json': lambda: json.dumps(report, indent=2, sort_keys=True, ensure_ascii=True) + '\n',
            'markdown': lambda: render_markdown(report),
            'csv': lambda: render_csv(report),
            'sarif': lambda: render_sarif(report),
        }[args.format]()
        write_text(args.output, content)
        if args.redacted_output:
            write_sanitized(args.redacted_output, [raw for _, raw in records], policy)
        return 0 if report['pass'] else 1
    except GateError as exc:
        print(f'AgentEgressPolicyGate: {exc}', file=sys.stderr)
        return 2
    except BrokenPipeError:
        return 1


if __name__ == '__main__':
    raise SystemExit(run())


# This solves the practical AI agent egress audit problem that became unavoidable by April 2026 as teams wired OpenAI Responses API calls, MCP tools, browser automation, data pipelines, webhooks, vector stores, SaaS APIs, and CI jobs into production workflows. Built because a successful tool call trace is not enough when a reviewer needs to know which domain received data, whether a private network was touched, whether a token leaked into a URL or header, whether a mutating request had an idempotency key, and whether cost, token, timeout, region, and payload budgets were respected. Use it when you need a dependency-free Python security gate for agent traces, MCP audit logs, LangGraph exports, edge compute review, DevOps incident evidence, SOC reports, SARIF upload, and pre-release policy checks. The trick: it normalizes messy JSON, JSONL, and CSV records into one event model, keeps every finding structured, and can emit redacted JSONL so teams can share evidence without spreading secrets. Drop this into any repo that ships AI tooling, infrastructure automation, research agents, or data platforms and run it in CI before traces become trusted production evidence.
