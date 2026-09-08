# Agent Run Provenance Gate

Your AI agent logged the run, but the log cannot prove which model revision, which tool digest, which corpus snapshot or which approval produced the action. This is a single file Rust CLI that audits JSON or JSONL agent traces for replayable provenance and fails CI when the evidence is missing.

**Language:** Rust | **Lines:** 839 | **Added:** 2026-08-08

## What this solves

The gap is between "the agent trace exists" and "a senior engineer can trust it during an incident, eval regression, compliance review or rollback." Teams wire agents to MCP servers, browser tools, SQL systems, email actions, deployment scripts and retrieval indexes, then log a friendly tool name, a model alias and a blob of prompt text. That is enough to build a dashboard. It is not enough to replay anything.

Here is what breaks. A model gateway silently moves `gpt-4o-latest` from one snapshot to the next and eval quality drops overnight. Every record says `"model": "gpt-4o-latest"`, so you cannot tell which runs used which snapshot and the regression is unattributable. Same shape with retrieval: someone rebuilds the RAG corpus, answers change, and the traces carry `"documents": [...]` with no corpus hash and no chunk hash set. Same with tools: an agent calls a mutating tool, the schema changed underneath queued work, and the trace names the tool but never pins the server revision. Then there is the review that finds a raw API key in a prompt field, retained for ninety days and copied to whatever observability vendor you use.

The people who notice have the worst timing. The on call engineer at 2am asking whether the agent's SQL delete had an approval ticket. The auditor asking why a run touching EU data went through a region not on the residency list. Neither gets an answer if the field was never written at ingestion. This gate moves that discovery to the pull request that changed the logger.

## Why I built it

Observability vendors give you traces, spans and pretty waterfalls. They do not tell you the trace is missing a prompt hash, that a tool call has a name but no digest, or that sampling is on with no seed recorded. The schema is whatever your SDK emitted, and the SDK had no opinion about what an auditor would need six months later. Every provenance linter I found wanted a vendor SDK, a database or a Python environment nobody wants inside a release gate. So this is one dependency free Rust file that reads messy JSON or JSONL, scores it and prints Markdown, JSON or SARIF.

## When to use it

- A model gateway moved snapshots and you need to know which past runs are still replayable.
- Someone rebuilt a vector index or embedding model and RAG answers shifted.
- A production agent can call browser, shell, SQL, email, deploy or payment tools and you want approval evidence enforced.
- You changed the agent logger and want a check that run ids, hashes and tool digests survived.
- A residency policy has to hold on every record, not just the reviewed ones.
- You want secret material caught before retention keeps it for ninety days.

## How it works

Input parsing is deliberately forgiving. `split_json_records` walks the text character by character with a brace depth counter, tracking string state and backslash escapes so braces inside string values do not confuse it. Every balanced top level object becomes a `Record` carrying its source path and starting line, so JSONL, pretty printed JSON, concatenated objects and arrays all take one code path. If that scan yields nothing, a second pass treats each trimmed line starting with `{` and ending with `}` as a candidate. Records survive only when `looks_like_agent_record` sees a plausible agent key such as `run_id`, `model`, `tool_call` or `retrieval`, so config noise is skipped rather than flagged.

Field lookup is a scanner, not a deserializer. `has_key` finds a quoted key and confirms the next non whitespace character is a colon, which avoids matching the same word appearing as a value. `string_after_key` walks past the colon into `parse_json_string`, and `extract_number` scans digit, sign and exponent characters. Hence no serde and no schema dependency.

`audit_record` holds the rules, each with a stable code. `identity.missing_run_id` fires High when none of `run_id`, `trace_id`, `session_id` or `workflow_id` exist. `moving_model_alias` treats anything containing `latest`, `preview` or `experimental` as unpinned, and also flags a `gpt-`, `claude-` or `gemini-` string carrying fewer than six digits with no `@` and no `rev`, the shape of a family alias rather than a dated revision. Prompt content with no `prompt_hash` key is High, output with no output hash is Medium. Any record with a tool name, host or arguments must carry a digest, version, schema hash or server revision, and `high_risk_tool` substring matches `browser`, `shell`, `sql`, `deploy`, `terraform`, `kubectl`, `stripe` and `ssh` against the combined name and host, demanding an `approval_id` or `change_ticket`. Retrieval records need a corpus snapshot, index digest or chunk hash set. `replay.randomness_unpinned` fires when temperature is above zero with no seed, `stream.sequence_missing` when a chunk or delta event has no ordering field, and `secret_evidence` emits Critical on bearer markers, PEM key headers, long `sk-` tokens and `ghp_`, `xoxb-`, `AKIA` style prefixes.

Scoring is a weighted penalty: Low 2, Medium 8, High 18, Critical 35. The score is 100 minus total penalty minus a volume allowance of `records / 10` capped at 6, floored at zero. The pass condition in `audit` is stricter than the number: `score >= fail_under` plus zero Critical and zero High findings. A single unapproved shell tool call therefore fails the gate on an otherwise clean trace. Low findings are dropped before counting unless you pass `--include-low`.

Three renderers write output by hand. `render_markdown` prints score, status, counts and a severity table. `render_json` emits one object with stats and findings. `render_sarif` emits SARIF 2.1.0 with rules deduplicated by code in a `BTreeMap`, mapping High and above to `error`, Medium to `warning` and Low to `note`, which is what GitHub code scanning ingests.

## Usage

```bash
# build, no dependencies
rustc -O AgentRunProvenanceGate.rs -o agent-provenance-gate

# markdown report from a JSONL trace
./agent-provenance-gate --input trace.jsonl --format markdown

# CI gate with an approved MCP host, region policy and a stricter threshold
./agent-provenance-gate trace.jsonl \
  --approved-tool-host mcp.internal \
  --allowed-region eu-west-1 \
  --required-field prompt_hash \
  --fail-under 95 \
  --format sarif > provenance.sarif

# stdin, JSON output, include low severity findings
cat traces/*.jsonl | ./agent-provenance-gate --format json --include-low

# load thresholds and allowlists from a policy file
./agent-provenance-gate -i trace.json --policy policy.json --allow-model-alias

./agent-provenance-gate --help
```

Policy file shape read by `apply_policy`:

```json
{
  "fail_under": 95,
  "allow_model_alias": false,
  "approved_tool_hosts": ["mcp.internal", "tools.corp.example"],
  "allowed_regions": ["eu-west-1", "eu-central-1"],
  "required_fields": ["prompt_hash", "run_id"]
}
```

## Notes

- Exit codes: `0` on pass, `2` on gate failure, `1` on any error such as a missing file, empty input, unterminated JSON or an unknown flag. `--help` exits `0`.
- Key detection is textual, not structural. `has_key` matches a quoted key followed by a colon anywhere in the record body, nested objects included. A dotted `--required-field a.b.c` is reduced to the leaf `c`.
- It checks that provenance fields are present and plausibly shaped. It does not verify them. A `prompt_hash` that is a lie still passes, and no hash is recomputed against the payload.
- Model alias detection is heuristic, so a pinned revision with an unusual naming scheme can be flagged. `--allow-model-alias` is the escape hatch. `high_risk_tool` is substring matching, so `report_writer` matches on `write` and will demand an approval reference.
- Findings are not deduplicated. A thousand line trace missing prompt hashes produces a thousand High findings and the score floors at zero, so read the counts rather than the score on large traces.
- Secret detection covers common prefix formats only and will miss a bare high entropy string or a custom credential format. Evidence is truncated to 120 characters, token matches to the first 8 to 12.
