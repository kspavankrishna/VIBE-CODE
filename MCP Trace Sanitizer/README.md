# MCP Trace Sanitizer

MCP tool traces, LLM gateway JSONL, browser automation transcripts and CI logs quietly carry real API keys, bearer tokens, cookies, PEM private keys and signed URLs. This is a single file C11 command line tool that streams a trace through and replaces every secret it finds with a stable non secret fingerprint, no jq, no Python and no JSON schema required.

**Language:** C | **Lines:** 1157 | **Added:** 2026-07-24

## What this solves

The failure mode is boring and it happens constantly. An agent run misbehaves, someone captures the raw Model Context Protocol trace or the LLM gateway log, and pastes it into a GitHub issue, a Slack thread, a vendor support ticket, an eval report or a research notebook. That trace contains the `Authorization: Bearer ...` header that the tool call actually sent. It contains the `sk-ant-` or `sk-proj-` key the gateway used. It contains session cookies from the browser automation step and the `?token=` on a presigned URL. Nobody reads 4000 lines of JSONL before pasting it. The key is live from the moment it lands in a public issue.

What it costs is rotation and blast radius. A leaked provider key means someone else's inference bill on your account. A leaked `ghp_` or `github_pat_` means repo write access. A leaked `AKIA` access key ID plus the secret next to it in the same log line means cloud account access. The people who notice are, in rough order, a secret scanning bot, an attacker crawling public issues, then your on call. Rotation is the easy part. The audit of what ran with that credential between leak and revocation is the expensive part.

The second failure mode is quieter. Support bundles and CI artifacts get archived, and archived traces cross data boundaries that live traffic never would. A trace that was fine on a laptop becomes a compliance problem once it is attached to a ticket in a third party helpdesk or committed to a build artifact bucket that has broader read access than the runtime environment.

This tool sits in the pipe before any of that. It reads a trace on stdin or from a file, redacts in place as it streams, and can exit non zero when it found anything, so a CI job can refuse to publish a trace that contained a credential.

## Why I built it

Existing secret scanners are built for source repositories, not for logs. They want a git history, a working tree or a diff, they report findings rather than producing a cleaned artifact, and most of them need a Python or Node runtime you may not have on a build runner or inside a minimal container. The other common answer is a `jq` filter that deletes known key paths, which fails the moment a secret sits inside a free text field, inside a nested stringified JSON payload or in a stack trace, which is exactly where agent traces put them.

I wanted something that treats the trace as text, not as a schema, produces a sanitized copy you can actually read afterwards, and builds anywhere with `cc` and nothing else. Keeping the redaction fingerprint stable matters too: if the same key leaks in three places you want to know it is the same key without the value ever being present.

## When to use it

- Before pasting an MCP or agent tool call trace into a GitHub issue, a Slack channel or a vendor support ticket
- As a CI step that runs with `--fail-on-leak` so a job fails instead of uploading a trace artifact containing a credential
- Cleaning LLM gateway JSONL before it goes into an eval set, a dataset or a shared notebook
- Sanitizing browser automation transcripts that captured cookies, `Set-Cookie` headers or signed URLs
- Preparing an incident support bundle that has to leave a regulated boundary
- Correlating repeated leaks across many files without ever handling the secret, using the emitted fingerprint

## How it works

The core loop is `sanitize_stream`. It pulls one line at a time through `read_line_dynamic`, a hand rolled reader that starts at a 4096 byte buffer and doubles with overflow checked arithmetic, so it does not depend on `getline` and does not truncate long JSONL records. For each line it runs seven independent scanners, each of which appends candidate byte ranges to a `RangeVec` rather than mutating the line. Nothing is rewritten until every scanner has had a look.

The scanners are: `scan_private_key_blocks`, `scan_url_query_params`, `scan_named_values`, `scan_auth_headers`, `scan_prefixed_secrets`, `scan_jwt` and `scan_high_entropy`. `scan_named_values` is the general purpose one. It walks the line finding both quoted keys and bare identifiers followed by `:` or `=`, normalizes the key by stripping every non alphanumeric character and lowercasing it, then tests it with `is_sensitive_normalized_key`. That test has an exact list (`apikey`, `authorization`, `cookie`, `clientsecret`, `awssecretaccesskeyid` and friends), substring rules for `secret`, `password`, `credential` and `privatekey`, and a suffix rule for anything ending in `token`. The suffix rule carves out `prompttoken`, `completiontoken`, `totaltoken` and `maxtoken`, because LLM traces are full of token counts and redacting `"total_tokens": 1843` would be useless noise. `--allow-key` pushes your own exceptions through the same normalizer.

`scan_prefixed_secrets` is a table of `SecretPrefix` entries, each with a minimum token length and a kind: `sk-ant-`, `sk-proj-`, `sk-or-v1-`, `gsk_`, `hf_`, `nvapi-`, `pplx-`, `github_pat_`, the five `gh?_` GitHub variants, the `xox?-` Slack family and `AIza` for Google. Each match must sit on a token boundary, checked by `boundary_before` and `boundary_after`, and must reach the prefix's minimum length, which keeps `sk-` from firing on ordinary prose. AWS access key IDs get their own check: `AKIA` or `ASIA` followed by exactly 20 characters that are all uppercase or digits. `scan_jwt` looks for `eyJ`, walks base64url characters, and only fires when it counted exactly two dots over at least 36 characters. PEM blocks are the one stateful case: `ScanState.pem_active` carries across lines so everything between `-----BEGIN ... PRIVATE KEY-----` and the matching `END` line is redacted whole.

`scan_high_entropy` is the catch all, on by default with a 32 character minimum. It is deliberately a heuristic stack rather than a Shannon entropy calculation: a candidate must span at least three of the four character classes, contain at least one of `_ - + / =`, and then survive three negative filters. `all_hex` drops git SHAs and hex digests, `looks_like_uuidish` drops UUIDs, and `looks_like_path_or_url` drops file paths and URLs. That combination is what keeps false positives low enough that the output is still readable. Turn it off with `--no-entropy` if your trace is dense with base64 payloads.

Rewriting happens in `emit_sanitized_line`. The ranges are sorted by `compare_ranges`, which orders by start offset ascending then by length descending, so when two scanners overlap the longer match wins. A single cursor then walks the line, copying clean bytes and skipping any range that starts before the cursor, which makes overlapping detections safe without a merge pass. Each redaction is written as `[REDACTED:<kind>:<16 hex digits>]` where the hex is an FNV-1a 64 bit hash of the exact secret bytes, computed in `fnv1a64_slice`. Same secret, same fingerprint, across files and across runs.

Counters accumulate in `Stats`, including a per kind histogram over the eleven `RedactionKind` values, and `write_report` emits them as hand written JSON with proper string escaping. `--fail-on-leak` turns any non zero redaction count into exit code 2.

## Usage

```sh
# build
cc -std=c11 -O2 -Wall -Wextra -pedantic McpTraceSanitizer.c -o mcp-trace-sanitizer

# file in, file out, JSON report on the side
./mcp-trace-sanitizer --input trace.jsonl --output trace.safe.jsonl --report report.json

# in a pipe, fail the build if anything was redacted
cat tool.log | ./mcp-trace-sanitizer --fail-on-leak > tool.safe.log

# keep a field that the default rules flag, and send the report to stderr
./mcp-trace-sanitizer -i agent.log -o agent.safe.log \
  --allow-key session_token --allow-key request_id --report -

# turn off the generic entropy heuristic and only match known patterns
./mcp-trace-sanitizer --no-entropy --input gateway.jsonl --output gateway.safe.jsonl

# be stricter about what counts as a high entropy token
./mcp-trace-sanitizer --min-token-len 24 -i trace.log -o trace.safe.log
```

Full flag set: `--input/-i`, `--output/-o`, `--report`, `--fail-on-leak`, `--allow-key NAME` (repeatable), `--no-entropy`, `--min-token-len N` (default 32) and `--help/-h`. `-` means stdin, stdout or stderr depending on the flag.

## Notes

- Exit codes: 0 clean, 2 with `--fail-on-leak` when at least one redaction happened, 64 for bad configuration or an unknown flag, 65 for IO and allocation failures. `--fail-on-leak` fires on any redaction, including a high entropy false positive, so tune `--min-token-len` before wiring it into CI.
- Detection is line scoped. A secret split across two physical lines is missed. PEM blocks are the only exception, and they are handled by redacting whole lines between BEGIN and END, so any legitimate text sharing those lines goes with them.
- It does not parse JSON. Escaped quotes inside string values are handled by `find_closing_quote`, but `\u` escapes are not decoded and base64 wrapped or otherwise encoded payloads are only caught if the entropy heuristic happens to fire.
- The fingerprint is FNV-1a 64, chosen for stability and speed, not for cryptographic strength. It is not salted, so it is not a safe way to publish a hash of a short or guessable secret. Treat it as a correlation ID only.
- Normalized key names are truncated to 160 characters and the replacement string to 96, and `--input` is refused when it equals `--output` so you cannot destroy a trace in place.
- Only the C11 standard library is used. No regex engine, no JSON library, no external dependency. Memory use is bounded by the longest single line plus the ranges found on it.
- Redacted output is usually longer than the input, since a placeholder is wider than most tokens it replaces. Byte counts for both directions are in the report.
