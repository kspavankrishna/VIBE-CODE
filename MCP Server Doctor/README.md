# MCP Server Doctor

An MCP stdio server can pass its own tests, publish a clean README and still break the moment a real client connects to it. This is a Bash health check that behaves like a disciplined MCP client, runs the initialize handshake, probes every capability the server advertises, validates the returned schemas and exits with a CI friendly status code.

**Language:** Bash | **Lines:** 1094 | **Added:** 2026-05-18

## What this solves

The failure modes of a Model Context Protocol server are almost never loud. The server starts. It prints a banner. Then a client wires it up and something quietly does not work. The handshake returns a protocol version the client did not ask for. The initialize result advertises a `resources` capability but `resources/list` returns an error. A tool declares `required: ["path"]` while `inputSchema.properties` has no `path` key, so the model keeps generating calls the server rejects. Two tools ship with the same name after a refactor and the client silently keeps one.

The worst one is stdout pollution. An MCP stdio server owns its stdout as a JSON-RPC channel. One stray `print()`, one dependency that logs on import, one progress bar and the framing is dead. The client sees garbage, drops the connection and reports something useless like "server disconnected". You then spend an hour bisecting a dependency tree because the actual error never made it anywhere. This script catches that on the first non-JSON line and shows you exactly what the server wrote.

Pagination is the other silent killer. A server that returns the same `nextCursor` on every page loops a naive client forever. With three tools locally you never hit it. With a few hundred resources in production you hang the session on a request that never terminates.

Who notices without this? The user, mid task, when the tool list is empty or a call fails with a schema error that reads like nonsense. By then the server has shipped. The cost is not a crash, it is slow erosion of trust in an integration nobody wants to debug.

## Why I built it

MCP server debugging in 2026 is still mostly manual. You launch the server, pipe JSON into it by hand, stare at the response and guess whether the problem is framing, protocol negotiation, a bad schema or capability drift between what initialize claims and what the server serves. The official inspector is interactive and browser based, fine for exploration and useless as a pipeline gate.

I wanted one file, no install step beyond `jq`, that answers a single question: if I hand this server to another engineer right now, will it behave like a real MCP service or like a demo that only works on my machine? A shell script was the right shape because it has to run in whatever CI container the server already builds in, without adding a runtime just to test the thing.

## When to use it

- As a pre merge gate in the repo that ships your MCP server, so a broken tool schema fails the build instead of a user session.
- Right after adding or renaming tools, to catch duplicate names and `required` keys that do not exist in `properties`.
- When a client reports "server disconnected" and you need to know whether the server is writing non-JSON to stdout.
- When you inherit someone else's MCP server and want an inventory of what it actually exposes versus what it claims.
- When upgrading the protocol version and you want to see what the server negotiates back instead of what you asked for.
- Before wiring a third party server into Claude, Cursor, VS Code or any other MCP host.

## How it works

`start_server` creates a temp directory under `TMPDIR`, makes two named pipes with `mkfifo` and opens them read write on file descriptors 3 and 4. The server command you pass after `--` runs in the background with its stdin bound to one pipe, stdout to the other and stderr redirected to `server.stderr.log`. Opening the FIFOs read write is the trick that keeps them from blocking on open and from signalling EOF when the writer has nothing to send. Client writes are teed to `client.requests.jsonl` and server output to `server.messages.jsonl`, so you get a transcript of both sides.

Two wire formats are supported. `write_wire_message` either terminates each compacted JSON object with a newline, or emits LSP style `Content-Length` headers. On the read side, `receive_message` peeks at the first line, and if it matches the `Content-Length` pattern it hands off to `parse_content_length_message`, which drains the header block, then reads exactly N bytes with `dd bs=1 count=$length`. Byte exact reads matter here because a JSON body can legally contain newlines. Any line that is neither a framing header nor valid JSON is recorded as an error, which is how stdout pollution gets caught.

Request correlation is handled by `await_response`. Each request gets a monotonic id of the form `doctor-N` from `REQUEST_COUNTER`, then the loop reads until it sees a matching `.id`, recomputing the remaining budget against a wall clock deadline every iteration so a chatty server cannot extend the effective timeout. Anything else that arrives goes to `handle_unsolicited_message`, which classifies by the presence of `method` and `id`: a method with no id is a notification and gets counted, with `notifications/message` log entries surfaced separately; a method with an id is a server to client request, which this client does not implement, so it replies with a JSON-RPC `-32601` error instead of deadlocking; an id with no method is a stale response and gets a warning.

Capability probing is gated on the initialize result, not on optimism. `server_advertises_capability` runs a jq expression against the stored `INIT_RESPONSE`, so `tools/list` is only called when `capabilities.tools` is non null. Listing goes through `collect_paginated`, which follows `nextCursor` up to `--max-pages`, appends each page into a combined array with jq, and records every cursor it has already seen in a file. A repeated cursor is an immediate error instead of an infinite loop. The collected arrays land in `TOOLS_JSON`, `PROMPTS_JSON`, `RESOURCES_JSON` and `RESOURCE_TEMPLATES_JSON` via `printf -v` indirect assignment.

The validators do the contract checking. `validate_tool_collection` groups by name to find duplicates, requires `inputSchema.type == "object"`, rejects an `outputSchema` that is not an object, warns on a missing description, and does a jq set subtraction of `inputSchema.required` minus the keys of `inputSchema.properties` to flag required fields that were never declared. `validate_resource_collection` checks duplicate URIs, a plausible scheme, a `mimeType` containing a slash and a numeric non negative `size`. `validate_prompt_collection` catches duplicate argument names and non boolean `required` flags. `sample_resource_reads` then calls `resources/read` on the first N listed URIs, and `validate_read_result` asserts a non empty `contents` array where every item has a string `uri` and exactly one of `text` or `blob`, written as an XOR so a server returning both, or neither, fails.

Findings accumulate in `ERRORS`, `WARNINGS` and `INFOS`. `diagnose_stderr` greps the captured stderr for panic, traceback, fatal, segmentation fault and exception text. `write_report` assembles everything into one JSON document with `jq -n`: negotiated protocol, server info, counts, findings, artifact paths and the full raw payloads. A `trap cleanup EXIT` runs `terminate_server`, which closes the descriptors first, then escalates to `SIGTERM` and `SIGKILL` only if the process is still alive.

## Usage

```bash
chmod +x McpServerDoctor.sh

# smoke test any stdio server, command goes after --
./McpServerDoctor.sh -- npx -y @modelcontextprotocol/server-filesystem /tmp

# archive a JSON report as a CI artifact, fail the build on warnings too
./McpServerDoctor.sh --strict --report doctor.json -- node dist/server.js

# a server that speaks LSP style Content-Length framing
./McpServerDoctor.sh --wire-format content-length -- python legacy_server.py

# deep inventory: more pages, more sampled reads, keep the raw transcripts
./McpServerDoctor.sh \
  --protocol-version 2025-11-25 \
  --timeout 20 \
  --max-pages 20 \
  --sample-resource-reads 10 \
  --verbose \
  --keep-artifacts \
  -- ./my-server

./McpServerDoctor.sh --help
./McpServerDoctor.sh --version
```

Full flag list: `--protocol-version`, `--timeout` (default 8s per request), `--sample-resource-reads` (default 2, 0 disables), `--max-pages` (default 5), `--wire-format` (`newline` or `content-length`), `--report PATH`, `--stderr-tail-lines` (default 80), `--strict`, `--verbose`, `--keep-artifacts`, `--help`, `--version`.

## Notes

- Exit codes: 0 clean, 1 when any error was recorded, 2 when `--strict` is set and only warnings were found, 64 for bad arguments or a missing `jq`.
- `jq` is the only hard dependency, plus a Bash with `BASH_REMATCH`, `printf -v` and process substitution. It also uses `mkfifo`, `dd`, `date -u` and `awk`, all standard on macOS and Linux.
- stdio transport only. There is no HTTP or SSE support and no authentication handling, so remote servers are out of scope.
- It never calls `tools/call`. Tools are validated by schema, not by execution, which keeps the run free of side effects but means a tool that lists cleanly can still fail at call time.
- Each probe stage in `main` is gated on the error list being empty, so the first hard failure suppresses the later checks. Fix errors top down and rerun rather than expecting one pass to surface everything.
- Server to client requests such as sampling, roots and elicitation are answered with method not found and counted as warnings. This is a diagnostic client, not a full host.
- Temp artifacts are deleted on exit unless you pass `--keep-artifacts`. The report written by `--report` records the artifact paths either way, so those paths are dangling in the default case.
