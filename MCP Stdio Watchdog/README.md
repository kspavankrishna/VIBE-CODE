# MCP Stdio Watchdog

A stdio MCP server hangs mid `tools/call`, the client keeps waiting forever, and nobody can tell whether the agent, the transport or the tool server is at fault. This is a single file Kotlin supervisor that sits between the client and the child process, enforces per method deadlines, restarts wedged children and replays the initialize handshake so the session survives.

**Language:** Kotlin | **Lines:** 1652 | **Added:** 2026-04-22

## What this solves

Model Context Protocol servers that run over stdio fail in an ugly way. They rarely die cleanly. A `tools/call` reaches out to a slow API and never returns. A Python virtualenv shim deadlocks on a lock it will never get. A Docker sidecar loses its network and the child stays alive but stops writing frames. From the client's side everything looks normal: the process is running, stdin is open, no error was ever emitted. The request just sits there. The developer waits, retries, restarts the editor, then spends an hour bisecting an agent that was never broken.

The second failure is worse because it is silent. When an MCP child crashes and something restarts it, the new process has no memory of the handshake. It has not seen `initialize`, it does not know the client's protocol version or capabilities, and every subsequent `tools/list` or `tools/call` either errors out or returns garbage. Most clients do not detect this. They keep sending requests into a server that considers the session uninitialized. You get a stream of confusing protocol errors that look like a bug in your tool code.

Third, request ids leak. If a client sends id 42, the watchdog times it out, and the child answers three minutes later, that late response now collides with whatever the client is doing with id 42 next. A supervisor that only restarts processes and does not track ids will happily forward a stale answer to the wrong caller. That is the kind of bug that produces one wrong tool result per day and never reproduces on demand.

Without something in the middle you pay for all three: lost developer hours, mysterious flaky tool calls, and no log that tells you which of your MCP servers is the bad one. The person who notices is whoever is trying to ship a feature, not whoever owns the server.

## Why I built it

The MCP ecosystem grew fast around `uvx`, `npx` and Docker launchers, and none of those give you a process supervisor that understands JSON-RPC. systemd and supervisord restart a dead process, but they cannot see that a live process stopped answering request 17. Client side timeouts exist in some SDKs, but they are inconsistent, they do not restart anything, and they do not replay the handshake. What was missing is a small thing you can wedge in front of an existing server command without touching the client config beyond the command line.

So this is one file, no dependencies beyond the JDK, that you compile once and put in front of any stdio MCP server. It reads the same framing the server reads, it writes the same framing the client expects, and it stays out of the way otherwise.

## When to use it

- A `tools/call` against a slow upstream API hangs and your editor sits there with no timeout of its own
- An MCP server crashes on a specific input and you want the session to recover instead of forcing an editor restart
- You are debugging a flaky third party server and need JSONL evidence of exactly when it timed out or restarted
- You run MCP servers in CI and want a bounded restart budget so a crash loop fails the job instead of spinning forever
- You suspect a server is leaking state over long sessions and want an idle timeout that cycles it during quiet periods
- Child stderr is noisy and you want it captured to a size capped file with credentials stripped out

## How it works

The core is `McpStdioWatchdog`, a class holding one child `Process` and a monotonically increasing `generation` counter. Every frame read from the child carries the generation of the process that produced it, and `handleServerPayload` drops anything whose generation no longer matches the current one. That is how stale output from a killed child never reaches the client. Restart bumps the counter, and everything in flight from the old process becomes invisible in a single comparison.

Framing is handled by `ContentLengthFramer`, which speaks the LSP style `Content-Length: N\r\n\r\n` header plus body. It reads headers byte by byte with `readAsciiLine`, then does a bounded read of exactly N bytes. Writes take a lock, `childWriteLock` for the child and `clientWriteLock` for stdout, so two threads never interleave a header with someone else's body.

Parsing is deliberately not a JSON library. `JsonRpcInspector.inspect` walks the top level object once with `extractTopLevelMembers`, recording the raw substring for `id`, `method`, `result`, `error` and `params`, skipping over nested composites with a bracket stack in `skipComposite`. It never materializes the payload into objects. That matters for two reasons: a `resources/read` result can be megabytes and you do not want to parse it to learn its id, and the original bytes are forwarded verbatim so the watchdog cannot corrupt a payload it did not fully understand. The id is kept as a raw token, so string ids and numeric ids round trip exactly.

Deadlines are per method. `Config` ships a built in table: 30s for `initialize`, 20s for the three `list` methods, 120s for `resources/read`, 300s for `tools/call`, 600s for `sampling/createMessage`, 60s for `completion/complete`, with a 90s default for everything else. `--method-timeout name=N` overrides any entry. Each in flight request goes into a `pending` LinkedHashMap as a `PendingRequest` with a `deadlineAtMs`. A single daemon scheduler runs `housekeeping` every 250ms by default, sweeps expired entries, sends the client a synthetic JSON-RPC error with code `-32001`, and adds the id to `timedOutResponsesToDrop`. When the child finally answers that id, the response is dropped instead of forwarded. That set is the fix for the stale id collision.

Restarts are budgeted with a rolling window. `registerRestartAttempt` keeps an `ArrayDeque` of restart timestamps, evicts anything older than `--restart-window-ms`, and refuses once the deque reaches `--max-restarts`. Six restarts in five minutes is the default. Blow the budget and the watchdog fails every queued and pending request with `-32002`, then exits 70 rather than looping. On a legitimate restart, `triggerRestart` fails all pending requests with `-32002`, closes the child's stdin, calls `destroy`, waits `--kill-grace-ms` and escalates to `destroyForcibly`, sleeps the backoff, then starts a fresh child.

Recovery is the part that keeps the session alive. The watchdog caches the client's last `initialize` request and last `notifications/initialized` frame as they pass through. After a restart it replays the cached `initialize` to the new child and sets `replayingInitialize`, during which every client frame is buffered into `queuedFrames` instead of being forwarded. When the replayed handshake succeeds it sends the cached `initialized` notification and calls `flushQueuedFrames`, which reassigns fresh deadlines and forwards the backlog. If the queue exceeds `--max-queued-frames` (256 by default) further requests get `-32003` and notifications are dropped with a log line. If replay is disabled the watchdog instead sets `requiresClientInitialize` and answers anything else with `-32002` until the client reinitializes on its own. Duplicate in flight ids are rejected with `-32600`, and `$/cancelRequest` marks the target pending request cancelled so housekeeping stops counting it, while also purging any matching frame still sitting in the queue.

Everything observable goes through `JsonlLogger`, one JSON object per line with an ISO timestamp and an event name: `child_started`, `request_timeout`, `restart_begin`, `replay_response`, `drop_late_response`, `flush_queue`, `watchdog_stop` and others. Child stderr goes to `StderrSink`, tagged with its generation and capped at `--stderr-max-bytes` with an explicit truncation marker. Both run every string through `Redactor`, which strips OpenAI style keys, GitHub PATs and tokens, Google API keys, bearer tokens and JWT shaped strings before anything hits disk.

## Usage

```bash
# compile once
kotlinc McpStdioWatchdog.kt -include-runtime -d mcp-stdio-watchdog.jar

# wrap any stdio MCP server, everything after -- is the child command
java -jar mcp-stdio-watchdog.jar \
  --log-file watchdog.jsonl \
  --stderr-file child-stderr.log \
  -- npx @modelcontextprotocol/server-filesystem /srv/data

# tighter deadlines, no restart on timeout, cycle the child after 10 minutes idle
java -jar mcp-stdio-watchdog.jar \
  --default-request-timeout-ms 45000 \
  --method-timeout tools/call=120000 \
  --method-timeout resources/read=60000 \
  --idle-timeout-ms 600000 \
  --no-restart-on-timeout \
  -- uvx some-mcp-server --transport stdio

# tune the restart budget and force the client to reinitialize itself
java -jar mcp-stdio-watchdog.jar \
  --max-restarts 3 --restart-window-ms 120000 --restart-backoff-ms 3000 \
  --kill-grace-ms 5000 --disable-replay-initialize \
  --cwd /opt/server -- ./server

java -jar mcp-stdio-watchdog.jar --help
```

In an MCP client config, replace the server command with the `java -jar ... --` prefix and leave the rest untouched. The client sees the same protocol on the same stdio pipes.

## Notes

- Only `Content-Length` framing is supported. A server that writes newline delimited JSON will not work, `ContentLengthFramer.readMessage` throws on a missing header.
- Initialize replay assumes the child is stateless enough that replaying the original handshake is valid. A server that ties the session to external state a restart destroys will come back handshaken but wrong.
- The JWT redaction regex matches any three dot separated base64ish segments, so it can over redact ordinary log text. That is deliberate, disk safety over log fidelity.
- Logging is opt in. Without `--log-file` the JSONL events are discarded, and without `--stderr-file` child stderr passes through to the watchdog's own stderr uncapped.
- Exit codes: 0 for a clean stop or client stdin close, 64 for bad arguments or a missing child command, 70 for a client read or write failure, a child that would not start, a housekeeping error or an exhausted restart budget.
- Timeouts are enforced per request id, not per byte. A child that streams a partial response slowly still counts against the same deadline, and there is no separate stall detector on the wire.
- Notifications and responses carry no deadline, only requests with an id are tracked. A child that silently swallows notifications will not trigger a restart unless `--idle-timeout-ms` is set.
- The main thread polls a stop flag every 100ms and housekeeping runs on one daemon scheduler thread, so shutdown latency is bounded by that plus `--kill-grace-ms`.
