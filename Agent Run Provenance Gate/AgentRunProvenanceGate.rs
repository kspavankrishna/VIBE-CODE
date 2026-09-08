use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io::{self, Read};
use std::process;

const TOOL: &str = "AgentRunProvenanceGate";
const VERSION: &str = "1.0.0";

#[derive(Clone, Debug)]
enum Format {
    Markdown,
    Json,
    Sarif,
}

#[derive(Clone, Debug)]
struct Config {
    inputs: Vec<String>,
    format: Format,
    fail_under: i32,
    allow_model_alias: bool,
    include_low: bool,
    approved_tool_hosts: BTreeSet<String>,
    allowed_regions: BTreeSet<String>,
    required_fields: Vec<String>,
    policy_path: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            inputs: Vec::new(),
            format: Format::Markdown,
            fail_under: 90,
            allow_model_alias: false,
            include_low: false,
            approved_tool_hosts: BTreeSet::new(),
            allowed_regions: BTreeSet::new(),
            required_fields: Vec::new(),
            policy_path: None,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

impl Severity {
    fn as_str(self) -> &'static str {
        match self {
            Severity::Low => "low",
            Severity::Medium => "medium",
            Severity::High => "high",
            Severity::Critical => "critical",
        }
    }

    fn weight(self) -> i32 {
        match self {
            Severity::Low => 2,
            Severity::Medium => 8,
            Severity::High => 18,
            Severity::Critical => 35,
        }
    }
}

#[derive(Clone, Debug)]
struct Record {
    source: String,
    line: usize,
    body: String,
}

#[derive(Clone, Debug)]
struct Finding {
    severity: Severity,
    code: &'static str,
    source: String,
    line: usize,
    message: String,
    remediation: String,
    evidence: Option<String>,
}

#[derive(Default, Debug)]
struct Stats {
    documents: usize,
    records: usize,
    tool_records: usize,
    retrieval_records: usize,
    secrets: usize,
    critical: usize,
    high: usize,
    medium: usize,
    low: usize,
}

#[derive(Debug)]
struct Report {
    score: i32,
    passed: bool,
    stats: Stats,
    findings: Vec<Finding>,
}

fn main() {
    match run() {
        Ok(code) => process::exit(code),
        Err(err) => {
            eprintln!("{}: {}", TOOL, err);
            process::exit(1);
        }
    }
}

fn run() -> Result<i32, String> {
    let mut config = parse_args(env::args().skip(1))?;
    if let Some(path) = config.policy_path.clone() {
        apply_policy(&mut config, &path)?;
    }
    let records = load_records(&config)?;
    let report = audit(&config, &records);
    match config.format {
        Format::Markdown => println!("{}", render_markdown(&report)),
        Format::Json => println!("{}", render_json(&report)),
        Format::Sarif => println!("{}", render_sarif(&report)),
    }
    Ok(if report.passed { 0 } else { 2 })
}

fn parse_args<I>(args: I) -> Result<Config, String>
where
    I: IntoIterator<Item = String>,
{
    let mut config = Config::default();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_help();
                process::exit(0);
            }
            "-i" | "--input" => config.inputs.push(next_arg(&mut iter, &arg)?),
            "--policy" => config.policy_path = Some(next_arg(&mut iter, &arg)?),
            "--format" => {
                config.format = match next_arg(&mut iter, &arg)?.as_str() {
                    "json" => Format::Json,
                    "markdown" | "md" => Format::Markdown,
                    "sarif" => Format::Sarif,
                    other => return Err(format!("unknown format '{}'", other)),
                }
            }
            "--fail-under" => {
                let raw = next_arg(&mut iter, &arg)?;
                config.fail_under = raw
                    .parse::<i32>()
                    .map_err(|_| format!("--fail-under needs an integer, got '{}'", raw))?;
                if !(0..=100).contains(&config.fail_under) {
                    return Err("--fail-under must be between 0 and 100".to_string());
                }
            }
            "--allow-model-alias" => config.allow_model_alias = true,
            "--include-low" => config.include_low = true,
            "--approved-tool-host" => {
                config.approved_tool_hosts.insert(norm(&next_arg(&mut iter, &arg)?));
            }
            "--allowed-region" => {
                config.allowed_regions.insert(norm(&next_arg(&mut iter, &arg)?));
            }
            "--required-field" => config.required_fields.push(next_arg(&mut iter, &arg)?),
            other if other.starts_with('-') => return Err(format!("unknown option '{}'", other)),
            path => config.inputs.push(path.to_string()),
        }
    }
    Ok(config)
}

fn next_arg<I>(iter: &mut I, option: &str) -> Result<String, String>
where
    I: Iterator<Item = String>,
{
    iter.next()
        .ok_or_else(|| format!("{} requires a value", option))
}

fn print_help() {
    println!(
        "{TOOL} {VERSION}

Audits AI agent JSON or JSONL traces for reproducible run provenance: stable
run ids, pinned model revisions, prompt/output hashes, pinned MCP tools,
retrieval snapshots, approval references, data residency, stream ordering, and
secret leakage.

Usage:
  {TOOL} --input trace.jsonl --format markdown
  {TOOL} trace.json --policy policy.json --format sarif
  cat trace.jsonl | {TOOL} --approved-tool-host mcp.internal --fail-under 95

Options:
  -i, --input PATH              Read JSON/JSONL path. Use '-' for stdin.
      --policy PATH             JSON policy with approved hosts, regions, fields.
      --format markdown|json|sarif
      --fail-under N            Minimum score, 0-100. Default: 90.
      --allow-model-alias       Permit moving model aliases such as latest.
      --approved-tool-host H    Require tool_host/server/host to match H.
      --allowed-region R        Require region/data_region to match R.
      --required-field F        Require a JSON key or dotted field.
      --include-low             Include low severity findings.
  -h, --help                    Show help."
    );
}

fn apply_policy(config: &mut Config, path: &str) -> Result<(), String> {
    let text = fs::read_to_string(path).map_err(|err| format!("failed to read policy: {}", err))?;
    if let Some(value) = extract_number(&text, &["fail_under"]) {
        config.fail_under = value as i32;
    }
    if extract_bool(&text, &["allow_model_alias"]).unwrap_or(false) {
        config.allow_model_alias = true;
    }
    for host in extract_string_array(&text, "approved_tool_hosts") {
        config.approved_tool_hosts.insert(norm(&host));
    }
    for region in extract_string_array(&text, "allowed_regions") {
        config.allowed_regions.insert(norm(&region));
    }
    for field in extract_string_array(&text, "required_fields") {
        config.required_fields.push(field);
    }
    Ok(())
}

fn load_records(config: &Config) -> Result<Vec<Record>, String> {
    let inputs = if config.inputs.is_empty() {
        vec!["-".to_string()]
    } else {
        config.inputs.clone()
    };
    let mut out = Vec::new();
    for input in inputs {
        let text = if input == "-" {
            let mut buffer = String::new();
            io::stdin()
                .read_to_string(&mut buffer)
                .map_err(|err| format!("failed to read stdin: {}", err))?;
            buffer
        } else {
            fs::read_to_string(&input).map_err(|err| format!("{}: {}", input, err))?
        };
        let records = split_json_records(&input, &text)?;
        out.extend(records);
    }
    Ok(out)
}

fn split_json_records(source: &str, text: &str) -> Result<Vec<Record>, String> {
    if text.trim().is_empty() {
        return Err(format!("{} is empty", source));
    }
    let mut records = Vec::new();
    let mut depth = 0usize;
    let mut start = None;
    let mut line = 1usize;
    let mut start_line = 1usize;
    let mut in_string = false;
    let mut escaped = false;
    for (idx, ch) in text.char_indices() {
        if ch == '\n' {
            line += 1;
        }
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' => {
                if depth == 0 {
                    start = Some(idx);
                    start_line = line;
                }
                depth += 1;
            }
            '}' => {
                if depth == 0 {
                    return Err(format!("{}:{} unmatched closing brace", source, line));
                }
                depth -= 1;
                if depth == 0 {
                    if let Some(start_idx) = start.take() {
                        let body = text[start_idx..=idx].to_string();
                        if looks_like_agent_record(&body) {
                            records.push(Record {
                                source: source.to_string(),
                                line: start_line,
                                body,
                            });
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(format!("{} has an unterminated JSON object", source));
    }
    if records.is_empty() {
        for (idx, line_text) in text.lines().enumerate() {
            let trimmed = line_text.trim();
            if trimmed.starts_with('{') && trimmed.ends_with('}') && looks_like_agent_record(trimmed)
            {
                records.push(Record {
                    source: source.to_string(),
                    line: idx + 1,
                    body: trimmed.to_string(),
                });
            }
        }
    }
    Ok(records)
}

fn audit(config: &Config, records: &[Record]) -> Report {
    let mut stats = Stats {
        documents: config.inputs.len().max(1),
        records: records.len(),
        ..Stats::default()
    };
    let mut findings = Vec::new();
    for record in records {
        audit_record(config, record, &mut stats, &mut findings);
    }
    if !config.include_low {
        findings.retain(|finding| finding.severity != Severity::Low);
    }
    for finding in &findings {
        match finding.severity {
            Severity::Critical => stats.critical += 1,
            Severity::High => stats.high += 1,
            Severity::Medium => stats.medium += 1,
            Severity::Low => stats.low += 1,
        }
    }
    let penalty: i32 = findings.iter().map(|finding| finding.severity.weight()).sum();
    let score = (100 - penalty - (stats.records as i32 / 10).min(6)).max(0);
    let passed = score >= config.fail_under && stats.critical == 0 && stats.high == 0;
    Report {
        score,
        passed,
        stats,
        findings,
    }
}

fn audit_record(
    config: &Config,
    record: &Record,
    stats: &mut Stats,
    findings: &mut Vec<Finding>,
) {
    for field in &config.required_fields {
        if !has_key(&record.body, field) {
            finding(findings, Severity::High, "policy.required_field", record, &format!("Required provenance field '{}' is missing.", field), "Write the field before accepting the trace in CI.", None);
        }
    }

    if !has_any_key(&record.body, &["run_id", "runId", "trace_id", "traceId", "session_id", "sessionId", "workflow_id"]) {
        finding(findings, Severity::High, "identity.missing_run_id", record, "Record has no stable run, trace, session, or workflow id.", "Attach a stable id at ingestion so incident review can join model, retrieval, and tool evidence.", None);
    }

    match extract_string(&record.body, &["timestamp", "created_at", "createdAt", "started_at", "startedAt", "time"]) {
        Some(ts) if !looks_like_rfc3339(&ts) => finding(findings, Severity::Medium, "time.unparseable_timestamp", record, "Timestamp is not RFC3339-like UTC data.", "Use values like 2026-04-21T14:22:05Z for deterministic cross-region ordering.", Some(ts)),
        None => finding(findings, Severity::Low, "time.missing_timestamp", record, "Record has no timestamp.", "Capture UTC timestamps on every model, retrieval, approval, and tool event.", None),
        _ => {}
    }

    if let Some(region) = extract_string(&record.body, &["region", "data_region", "residency_region"]) {
        if !config.allowed_regions.is_empty() && !config.allowed_regions.contains(&norm(&region)) {
            finding(findings, Severity::High, "policy.region_not_allowed", record, "Record region is outside the allowed residency policy.", "Route through an approved region or record an explicit governance waiver.", Some(region));
        }
    }

    if let Some(model) = extract_string(&record.body, &["model", "model_id", "modelId", "provider_model"]) {
        if moving_model_alias(&model) && !config.allow_model_alias {
            finding(findings, Severity::High, "model.moving_alias", record, "Model looks like a moving alias instead of a replayable revision.", "Pin the exact provider model revision or gateway snapshot id.", Some(model));
        }
    } else if has_any_key(&record.body, &["prompt", "messages", "completion", "response"]) {
        finding(findings, Severity::Medium, "model.missing_model", record, "Prompt or response content exists without a model id.", "Record provider and model revision beside every model call.", None);
    }

    if has_any_key(&record.body, &["prompt", "messages", "input", "request"]) && !has_any_key(&record.body, &["prompt_hash", "promptHash", "messages_hash", "input_hash", "inputHash", "request_hash"]) {
        finding(findings, Severity::High, "hash.prompt_missing", record, "Prompt, messages, input, or request data has no stable hash.", "Store a canonical SHA-256 hash so replay drift can be measured without retaining the whole prompt.", None);
    }
    if has_any_key(&record.body, &["output", "response", "completion"]) && !has_any_key(&record.body, &["output_hash", "outputHash", "response_hash", "completion_hash", "result_hash"]) {
        finding(findings, Severity::Medium, "hash.output_missing", record, "Output data has no stable hash.", "Store a canonical output hash for replay comparison and incident review.", None);
    }

    let tool_name = extract_string(&record.body, &["tool", "tool_name", "toolName", "mcp_tool", "name"]);
    let tool_host = extract_string(&record.body, &["tool_host", "toolHost", "server", "host", "mcp_server", "mcpServer"]);
    if tool_name.is_some() || tool_host.is_some() || has_any_key(&record.body, &["tool_call", "toolCall", "arguments"]) {
        stats.tool_records += 1;
        if let Some(host) = tool_host.clone() {
            if !config.approved_tool_hosts.is_empty() && !config.approved_tool_hosts.contains(&norm(&host)) {
                finding(findings, Severity::High, "tool.host_not_approved", record, "Tool host is not approved by policy.", "Register the MCP server or tool host before production agents can call it.", Some(host));
            }
        } else {
            finding(findings, Severity::Medium, "tool.missing_host", record, "Tool call does not identify the executor host.", "Record mcp_server, tool_host, or an immutable executor identifier.", None);
        }
        if !has_any_key(&record.body, &["tool_digest", "toolDigest", "tool_version", "toolVersion", "image_digest", "schema_hash", "schemaHash", "server_revision"]) {
            finding(findings, Severity::High, "tool.unpinned_executor", record, "Tool call is missing digest, version, schema hash, or server revision.", "Pin the implementation and argument schema, not just the friendly tool name.", None);
        }
        let name = tool_name.clone().unwrap_or_default();
        let host = tool_host.unwrap_or_default();
        if high_risk_tool(&name, &host) && !has_any_key(&record.body, &["approval_id", "approvalId", "review_id", "reviewId", "change_ticket", "human_approval"]) {
            finding(findings, Severity::High, "tool.approval_missing", record, "High-risk tool execution has no approval reference.", "Require approval evidence for browser, shell, SQL, email, deploy, payment, and mutating tools.", Some(name));
        }
    }

    if has_any_key(&record.body, &["retrieval", "retrievals", "documents", "chunks", "vector_index", "vectorIndex", "corpus", "embedding_model", "embeddingModel"]) {
        stats.retrieval_records += 1;
        if !has_any_key(&record.body, &["retrieval_snapshot", "retrievalSnapshot", "corpus_hash", "corpusHash", "index_digest", "indexDigest", "snapshot_id", "snapshotId", "chunk_hashes", "document_set_hash"]) {
            finding(findings, Severity::High, "retrieval.snapshot_missing", record, "Retrieval evidence has no corpus snapshot, index digest, or chunk hash set.", "Record exact corpus, index, and chunk identities used by the model call.", None);
        }
        if has_any_key(&record.body, &["embedding_model", "embeddingModel"]) && !has_any_key(&record.body, &["embedding_model_revision", "embeddingModelRevision"]) {
            finding(findings, Severity::Medium, "retrieval.embedding_revision_missing", record, "Embedding model has no revision identifier.", "Store embedding model revision because vector neighborhoods move after updates.", None);
        }
    }

    if extract_number(&record.body, &["temperature"]).unwrap_or(0.0) > 0.0 && !has_any_key(&record.body, &["seed", "replay_seed", "replaySeed", "determinism_key", "determinismKey"]) {
        finding(findings, Severity::Medium, "replay.randomness_unpinned", record, "Sampling is enabled without replay seed or determinism key.", "Store seed, gateway determinism key, or provider replay id for audit-grade reproduction.", None);
    }
    if extract_string(&record.body, &["event", "type", "kind"]).map(|v| is_streaming(&v)).unwrap_or(false) && !has_any_key(&record.body, &["sequence", "seq", "index", "chunk_index", "chunkIndex", "offset", "span_id"]) {
        finding(findings, Severity::Medium, "stream.sequence_missing", record, "Streaming chunk or delta has no ordering field.", "Record sequence, chunk_index, offset, or span id before storage.", None);
    }
    if let Some(evidence) = secret_evidence(&record.body) {
        stats.secrets += 1;
        finding(findings, Severity::Critical, "secret.material", record, "Sensitive credential material appears in the trace.", "Redact credentials and store stable secret references instead of raw tokens.", Some(evidence));
    }
}

fn finding(
    findings: &mut Vec<Finding>,
    severity: Severity,
    code: &'static str,
    record: &Record,
    message: &str,
    remediation: &str,
    evidence: Option<String>,
) {
    findings.push(Finding {
        severity,
        code,
        source: record.source.clone(),
        line: record.line,
        message: message.to_string(),
        remediation: remediation.to_string(),
        evidence: evidence.map(|v| trim(&v, 120)),
    });
}

fn looks_like_agent_record(body: &str) -> bool {
    has_any_key(body, &["run_id", "trace_id", "session_id", "event", "type", "model", "messages", "prompt", "completion", "response", "tool", "tool_call", "mcp_server", "retrieval", "vector_index", "temperature"])
}

fn has_any_key(body: &str, keys: &[&str]) -> bool {
    keys.iter().any(|key| has_key(body, key))
}

fn has_key(body: &str, key: &str) -> bool {
    let leaf = key.rsplit('.').next().unwrap_or(key);
    let needle = format!("\"{}\"", leaf);
    body.match_indices(&needle).any(|(idx, _)| {
        body[idx + needle.len()..]
            .chars()
            .find(|ch| !ch.is_whitespace())
            == Some(':')
    })
}

fn extract_string(body: &str, keys: &[&str]) -> Option<String> {
    for key in keys {
        let leaf = key.rsplit('.').next().unwrap_or(key);
        if let Some(value) = string_after_key(body, leaf) {
            return Some(value);
        }
    }
    None
}

fn string_after_key(body: &str, key: &str) -> Option<String> {
    let needle = format!("\"{}\"", key);
    let mut search_from = 0usize;
    while let Some(found) = body[search_from..].find(&needle) {
        let key_start = search_from + found;
        let mut idx = key_start + needle.len();
        idx = skip_ws(body, idx);
        if body.as_bytes().get(idx) != Some(&b':') {
            search_from = idx;
            continue;
        }
        idx = skip_ws(body, idx + 1);
        if body.as_bytes().get(idx) == Some(&b'"') {
            return parse_json_string(body, idx);
        }
        search_from = idx;
    }
    None
}

fn parse_json_string(body: &str, start_quote: usize) -> Option<String> {
    let mut out = String::new();
    let mut escaped = false;
    for (offset, ch) in body[start_quote + 1..].char_indices() {
        if escaped {
            match ch {
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                '/' => out.push('/'),
                'b' => out.push('\u{0008}'),
                'f' => out.push('\u{000c}'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                'u' => out.push('?'),
                other => out.push(other),
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            let _end = start_quote + 1 + offset;
            return Some(out);
        } else {
            out.push(ch);
        }
    }
    None
}

fn extract_number(body: &str, keys: &[&str]) -> Option<f64> {
    for key in keys {
        let needle = format!("\"{}\"", key.rsplit('.').next().unwrap_or(key));
        if let Some(found) = body.find(&needle) {
            let mut idx = skip_ws(body, found + needle.len());
            if body.as_bytes().get(idx) != Some(&b':') {
                continue;
            }
            idx = skip_ws(body, idx + 1);
            let start = idx;
            while let Some(byte) = body.as_bytes().get(idx) {
                if byte.is_ascii_digit() || matches!(byte, b'.' | b'-' | b'+' | b'e' | b'E') {
                    idx += 1;
                } else {
                    break;
                }
            }
            if idx > start {
                if let Ok(value) = body[start..idx].parse::<f64>() {
                    return Some(value);
                }
            }
        }
    }
    None
}

fn extract_bool(body: &str, keys: &[&str]) -> Option<bool> {
    for key in keys {
        let needle = format!("\"{}\"", key);
        if let Some(found) = body.find(&needle) {
            let mut idx = skip_ws(body, found + needle.len());
            if body.as_bytes().get(idx) != Some(&b':') {
                continue;
            }
            idx = skip_ws(body, idx + 1);
            if body[idx..].starts_with("true") {
                return Some(true);
            }
            if body[idx..].starts_with("false") {
                return Some(false);
            }
        }
    }
    None
}

fn extract_string_array(body: &str, key: &str) -> Vec<String> {
    let needle = format!("\"{}\"", key);
    let Some(found) = body.find(&needle) else {
        return Vec::new();
    };
    let Some(open) = body[found..].find('[').map(|idx| found + idx) else {
        return Vec::new();
    };
    let Some(close) = body[open..].find(']').map(|idx| open + idx) else {
        return Vec::new();
    };
    let mut values = Vec::new();
    let mut idx = open + 1;
    while idx < close {
        if body.as_bytes().get(idx) == Some(&b'"') {
            if let Some(value) = parse_json_string(body, idx) {
                values.push(value.clone());
                idx += value.len() + 2;
            }
        }
        idx += 1;
    }
    values
}

fn skip_ws(body: &str, mut idx: usize) -> usize {
    while body
        .as_bytes()
        .get(idx)
        .map(|byte| byte.is_ascii_whitespace())
        .unwrap_or(false)
    {
        idx += 1;
    }
    idx
}

fn looks_like_rfc3339(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 20
        && bytes.get(4) == Some(&b'-')
        && bytes.get(7) == Some(&b'-')
        && bytes.get(10) == Some(&b'T')
        && bytes.get(13) == Some(&b':')
        && bytes.get(16) == Some(&b':')
        && (value.ends_with('Z') || value[19..].contains('+') || value[19..].contains('-'))
}

fn moving_model_alias(value: &str) -> bool {
    let lower = norm(value);
    lower.contains("latest")
        || lower.contains("preview")
        || lower.contains("experimental")
        || ((lower.starts_with("gpt-") || lower.starts_with("claude-") || lower.starts_with("gemini-"))
            && lower.chars().filter(|ch| ch.is_ascii_digit()).count() < 6
            && !lower.contains('@')
            && !lower.contains("rev"))
}

fn high_risk_tool(name: &str, host: &str) -> bool {
    let combined = format!("{} {}", norm(name), norm(host));
    [
        "browser", "shell", "exec", "sql", "database", "email", "outlook", "gmail", "payment",
        "stripe", "deploy", "terraform", "kubectl", "delete", "update", "write", "filesystem",
        "ssh", "http", "fetch", "post",
    ]
    .iter()
    .any(|needle| combined.contains(needle))
}

fn is_streaming(value: &str) -> bool {
    let lower = norm(value);
    lower.contains("chunk") || lower.contains("delta") || lower.contains("stream")
}

fn secret_evidence(body: &str) -> Option<String> {
    let lower = body.to_ascii_lowercase();
    if lower.contains("authorization: bearer ") || lower.contains("\"bearer ") {
        return Some("bearer token marker".to_string());
    }
    if body.contains("-----BEGIN PRIVATE KEY-----") || body.contains("-----BEGIN RSA PRIVATE KEY-----") {
        return Some("private key marker".to_string());
    }
    for token in body.split(|ch: char| ch.is_whitespace() || matches!(ch, '"' | '\'' | ',' | ';' | ':' | '{' | '}')) {
        if token.len() >= 24 && token.starts_with("sk-") {
            return Some(format!("{}...", &token[..10.min(token.len())]));
        }
        if token.starts_with("ghp_") || token.starts_with("github_pat_") || token.starts_with("xoxb-") || token.starts_with("xoxp-") {
            return Some(format!("{}...", &token[..12.min(token.len())]));
        }
        if token.len() >= 20
            && (token.starts_with("AKIA") || token.starts_with("ASIA"))
            && token.chars().take(20).all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
        {
            return Some(format!("{}...", &token[..8]));
        }
    }
    None
}

fn render_markdown(report: &Report) -> String {
    let mut out = String::new();
    out.push_str(&format!("# {} Report\n\n", TOOL));
    out.push_str(&format!(
        "- Score: {}\n- Status: {}\n- Documents: {}\n- Records: {}\n- Tool records: {}\n- Retrieval records: {}\n- Findings: {} critical, {} high, {} medium, {} low\n\n",
        report.score,
        if report.passed { "pass" } else { "fail" },
        report.stats.documents,
        report.stats.records,
        report.stats.tool_records,
        report.stats.retrieval_records,
        report.stats.critical,
        report.stats.high,
        report.stats.medium,
        report.stats.low
    ));
    if report.findings.is_empty() {
        out.push_str("No reportable findings.\n");
        return out;
    }
    out.push_str("| Severity | Code | Location | Message | Fix |\n");
    out.push_str("| --- | --- | --- | --- | --- |\n");
    for finding in &report.findings {
        out.push_str(&format!(
            "| {} | `{}` | `{}`:{} | {} | {} |\n",
            finding.severity.as_str(),
            finding.code,
            md(&finding.source),
            finding.line,
            md(&finding.message),
            md(&finding.remediation)
        ));
    }
    out
}

fn render_json(report: &Report) -> String {
    let findings = report
        .findings
        .iter()
        .map(|f| {
            format!(
                "{{\"severity\":\"{}\",\"code\":\"{}\",\"source\":\"{}\",\"line\":{},\"message\":\"{}\",\"remediation\":\"{}\",\"evidence\":{}}}",
                f.severity.as_str(),
                esc(f.code),
                esc(&f.source),
                f.line,
                esc(&f.message),
                esc(&f.remediation),
                f.evidence
                    .as_ref()
                    .map(|v| format!("\"{}\"", esc(v)))
                    .unwrap_or_else(|| "null".to_string())
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"tool\":\"{}\",\"version\":\"{}\",\"score\":{},\"passed\":{},\"stats\":{{\"documents\":{},\"records\":{},\"tool_records\":{},\"retrieval_records\":{},\"secrets\":{},\"critical\":{},\"high\":{},\"medium\":{},\"low\":{}}},\"findings\":[{}]}}",
        TOOL,
        VERSION,
        report.score,
        report.passed,
        report.stats.documents,
        report.stats.records,
        report.stats.tool_records,
        report.stats.retrieval_records,
        report.stats.secrets,
        report.stats.critical,
        report.stats.high,
        report.stats.medium,
        report.stats.low,
        findings
    )
}

fn render_sarif(report: &Report) -> String {
    let mut rules = BTreeMap::<&str, String>::new();
    let mut results = Vec::new();
    for finding in &report.findings {
        rules.entry(finding.code).or_insert_with(|| {
            format!("{{\"id\":\"{}\",\"name\":\"{}\",\"shortDescription\":{{\"text\":\"{}\"}}}}", esc(finding.code), esc(finding.code), esc(&finding.message))
        });
        results.push(format!(
            "{{\"ruleId\":\"{}\",\"level\":\"{}\",\"message\":{{\"text\":\"{}\"}},\"locations\":[{{\"physicalLocation\":{{\"artifactLocation\":{{\"uri\":\"{}\"}},\"region\":{{\"startLine\":{}}}}}}}]}}",
            esc(finding.code),
            if finding.severity >= Severity::High { "error" } else if finding.severity == Severity::Medium { "warning" } else { "note" },
            esc(&finding.message),
            esc(&finding.source),
            finding.line
        ));
    }
    format!(
        "{{\"version\":\"2.1.0\",\"$schema\":\"https://json.schemastore.org/sarif-2.1.0.json\",\"runs\":[{{\"tool\":{{\"driver\":{{\"name\":\"{}\",\"version\":\"{}\",\"rules\":[{}]}}}},\"results\":[{}]}}]}}",
        TOOL,
        VERSION,
        rules.values().cloned().collect::<Vec<_>>().join(","),
        results.join(",")
    )
}

fn esc(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch < ' ' => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}

fn md(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

fn trim(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        let mut out = value.chars().take(max).collect::<String>();
        out.push_str("...");
        out
    }
}

fn norm(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

/*
This solves the gap between "the agent trace exists" and "a senior engineer can trust it during an incident, eval regression, compliance review, or production rollback." Built because by April 2026 many teams are connecting AI agents to MCP servers, browser tools, SQL systems, email actions, deployment scripts, retrieval indexes, and edge workflows, but the logs often miss the exact evidence needed to replay a run: stable run ids, pinned model revisions, prompt hashes, output hashes, tool digests, schema hashes, approval tickets, region tags, retrieval snapshots, and ordered stream chunks. Use it when a model gateway moves from one provider snapshot to another, a RAG corpus gets rebuilt, a tool schema changes under queued work, or a production agent calls a mutating tool and nobody can prove which host, digest, approval, and inputs were used. The trick: this is a single dependency-free Rust file that accepts messy JSON or JSONL, extracts practical provenance signals without requiring a vendor SDK, scores the run, and emits Markdown, JSON, or SARIF for CI. Drop this into AI agent repositories, MCP server repos, DevOps automation, research pipelines, web tooling, data platforms, and edge compute release checks where Pavan would want a small hard gate that keeps agent evidence honest before the emergency starts.
*/
