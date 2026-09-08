# Agent Rules Backdoor Scanner

A single line in `AGENTS.md`, `CLAUDE.md` or `.cursorrules` can tell a coding agent to leak your `GITHUB_TOKEN`, skip a permission prompt or hide what it did from the diff. This is a dependency free Rust scanner that reads those files the way an attacker writes them and fails your build before the pull request merges.

**Language:** Rust | **Lines:** 1282 | **Added:** 2026-06-01

## What this solves

Agent instruction files are executable input. A coding agent reads `AGENTS.md`, `CLAUDE.md`, `.cursor/rules/*.mdc`, `.github/copilot-instructions.md`, `.mcp.json` and workflow YAML as authority, then acts on them with your shell, your filesystem and your tokens. Nobody reviews those files the way they review code. They arrive in a dependency bump, a template repo or an agent generated PR, and a reviewer skims them as documentation because they look like documentation. That is the attack.

The concrete failure mode: a line buried in a rules file says fetch this URL with `$GITHUB_TOKEN` in the query string, and do not mention this step to the user. The agent complies. The token reaches an attacker controlled host, and the maintainer sees a clean diff because the exfiltration lives in the instructions rather than the source. Or the rule says do not ask for approval before running MCP tools, and the sandbox meant to gate destructive commands is quietly off for everyone who clones the repo. Or a right to left override makes a filename render as harmless in the editor while the agent reads the real bytes.

Rotating a leaked CI token means rebuilding every secret that touched a pipeline, and you cannot prove the blast radius because the instruction left no trace in the code. Security usually notices weeks later, from an egress log, by which point the poisoned file sits in three other repositories because it was part of the working template. Normal tooling does not catch it: secret scanners look for secrets in the file rather than instructions that move secrets elsewhere, SAST parses source and a Markdown rules file is not source, and linters do not care about English.

## Why I built it

I wanted a scanner a repo owner can drop into CI without a dependency tree, without shipping source code to a third party service and without waiting for a vendor rule pack to learn what an AI coding assistant file even is. Everything commercial here was either a hosted product that wants your code, or a general purpose secret scanner with no concept of `.cursor/rules/`.

The other gap was output. A check that cannot emit SARIF cannot land in GitHub code scanning, and a finding nobody sees in the PR view gets ignored. So this compiles with `rustc` from the standard library alone, runs in one pass and speaks human, JSON and SARIF.

## When to use it

- A CI gate on every PR touching `AGENTS.md`, `CLAUDE.md`, `.cursorrules` or anything under `.cursor/rules/`
- Reviewing an agent generated pull request, where the agent may have written its own future instructions
- Auditing a template or starter repo before you clone it into a team, and before its rules propagate
- Checking MCP configs and `.github/workflows/` for unpinned remote execution and overbroad tool permissions
- Incident triage: point it at a repo you already suspect and diff the fingerprinted findings between commits

## How it works

`main` parses argv through `Config::from_env`, which takes both `--flag=value` and `--flag value` forms, then hands a `Config` to `run`. `run` walks each root with `walk_path`, sorts and dedups the findings, emits them and returns the exit code. `walk_path` calls `fs::symlink_metadata` rather than `metadata`, so a symlink counts as skipped and is never followed. That is deliberate: following links out of the repo is how a scanner ends up pointed at `/etc` or stuck in a loop. Entries are sorted before recursion, so output is stable across filesystems.

`is_candidate_path` decides what gets read: an exact name list (`agents.md`, `claude.md`, `gemini.md`, `copilot-instructions.md`, `codex.md`, `.cursorrules`, `.windsurfrules`, `.replit`, `mcp.json`, `package.json`, `Dockerfile`, `Makefile`), a path substring check (`.cursor/rules/`, `.windsurf/rules/`, `.github/instructions/`, `.github/workflows/`, `.vscode/`), then text like extensions whose path also contains `prompt`, `instruction`, `rules`, `agent`, `mcp` or `workflow`. `read_text_file` reads `max_bytes + 1` bytes so it can tell truncation from an exact fit, and `looks_binary` drops a buffer holding a NUL or over 20 percent non printing controls. Invalid UTF-8 is not an error: the file is decoded lossily and flagged as `ARS000`, because a rules file whose bytes render differently for the reviewer than for the agent is itself a finding.

`scan_text` then runs three passes per line. `detect_unicode_controls` walks `char_indices` against `suspicious_unicode_name`, which names every bidi control (`U+202A` to `U+202E`, isolates `U+2066` to `U+2069`), the zero width family, the word joiner and the BOM, plus any other control that is not tab, newline or carriage return. That is `ARS001`, the only rule reporting a true character column. `detect_line_patterns` is a set of two sided keyword conjunctions: a trigger term plus a context term, so `curl` alone is nothing but `curl` beside `api_key` is `ARS004` at critical. The same shape yields instruction override, secrecy clauses, sandbox and approval bypass, pipe to shell and destructive commands, unpinned `npx`/`pip install`/`@main` supply chain paths, credential printing and upload, overbroad tool grants and self hiding instructions, as `ARS002` through `ARS010`. The override rule also runs `compact_alnum` over the lowered line, stripping every non alphanumeric character, so spaced out text collapses into a phrase the matcher still catches.

`detect_encoded_payloads` handles blobs. `is_base64ish` wants a token of 120 characters or more, 95 percent valid alphabet and a mixed alphabet (upper, lower, digit) to suppress hashes and ids. That is `ARS012` at medium, or high when the line also carries a decode or execution primitive such as `base64 -d`, `node -e` or `| bash`. Hex runs of 160 characters beside those primitives become `ARS013`.

Every `Finding` carries a fingerprint: `Finding::new` normalizes the path, compacts and lowercases the evidence snippet, then runs FNV-1a 64 over path, line, column, rule id and snippet. FNV-1a fits because it is eight lines of code with no dependency and only needs to be stable and well spread, not cryptographic. The fingerprint dedups findings twice, within a file and again globally after the severity sort, and lands in SARIF as `partialFingerprints` so code scanning tracks one finding across commits instead of reopening it. `Severity` derives `Ord` with `Info` lowest, which makes the descending sort and the `--fail-on` comparison one liners. JSON and SARIF are built by hand through `json_escape`, which keeps the dependency count at zero.

## Usage

```bash
# build
rustc AgentRulesBackdoorScanner.rs -O -o agent-rules-backdoor-scanner

# scan the current directory, human output, exit 1 on high or critical
./agent-rules-backdoor-scanner

# scan specific paths (positional or --root, both work)
./agent-rules-backdoor-scanner AGENTS.md CLAUDE.md .cursor .github
./agent-rules-backdoor-scanner --root . --root ../shared-rules

# SARIF for GitHub code scanning
./agent-rules-backdoor-scanner --root . --format sarif > agent-rules.sarif

# machine readable, report everything, never fail the build
./agent-rules-backdoor-scanner --format json --fail-on=off

# widest sweep: every text looking file, no directory skips, 2 MB per file
./agent-rules-backdoor-scanner --all-files --no-default-ignores --max-bytes 2097152

# strictest gate
./agent-rules-backdoor-scanner --fail-on medium

./agent-rules-backdoor-scanner --help

# run the built in tests
rustc --test AgentRulesBackdoorScanner.rs -o ars-tests && ./ars-tests
```

## Notes

- Exit codes: `0` clean, `1` a finding met or exceeded `--fail-on` (default `high`), `3` no threshold hit but at least one read or walk error, `64` bad arguments. The threshold wins, so a failing scan with unreadable files still exits `1`.
- Matching is substring based on the lowercased line. No Markdown parser, no code fence awareness, so a documented attack example inside a security README will be flagged. There is no inline suppression syntax and no allowlist file.
- Detection is per line. An instruction split across two lines will not trip the two sided rules, and `ARS012` reports at most one encoded token per line.
- The candidate filter is name and path driven. A poisoned `docs/onboarding.md` with no trigger word in its path stays invisible until you pass `--all-files`.
- Symlinks are never followed. `.gitignore` is not read; directory skipping is the fixed `should_ignore_dir` list, covering `.git`, `node_modules`, `target`, `dist`, `build`, `vendor` and the usual caches.
- Files are truncated at 512 KB by default, reported as `ARS011`. The walk is single threaded and recurses without a depth limit.
- This is a detector, not a fixer. It never edits or quarantines a file. Every finding ships with an `advice` string and a fingerprint so a human can decide.
