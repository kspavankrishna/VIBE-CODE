# Edge SSE Replay Ledger

A browser tab sleeps mid stream, the mobile radio drops, or a CDN hop recycles the connection, and the client reconnects with a `Last-Event-ID` header that nothing on your side can answer. This is a single file Lua ledger that parses Server-Sent Events, keeps a bounded replay buffer, redacts secrets before anything is retained and answers that reconnect deterministically.

**Language:** Lua | **Lines:** 822 | **Added:** 2026-06-16

## What this solves

This solves the April 2026 developer problem of keeping AI chat, coding agent, RAG and tool-calling Server-Sent Events reliable when a browser tab, mobile network, CDN hop or edge worker disconnects halfway through a streaming response. SSE has a reconnect protocol in the spec. The browser remembers the last `id:` it saw and sends it back as `Last-Event-ID`. Almost nobody implements the server half. The reconnect arrives, the gateway has no memory of what it already sent, and the client either gets the whole response again or gets nothing and sits on a half rendered answer.

The failure is quiet and it costs money. A token stream that restarts from zero bills the model call twice. A stream that resumes at the wrong offset leaves a hole in the middle of the answer, and support gets a ticket nobody can reproduce because the evidence lived in a socket that is now closed. On a multi tenant gateway it is worse: replay without a tenant filter can hand one customer another customer's deltas, which is a data incident, not a bug.

The other half is what ends up in the buffer. Retaining stream bodies for replay creates a log of model output, and model output routinely carries keys pasted into a prompt, bearer tokens echoed by a tool call or `sk-` credentials in an error payload. Bolt replay on late and redaction never happens, so the secret sits in a buffer that gets dumped into a support ticket during an incident.

Then there is the parsing. SSE looks trivial until you hit multi line `data:` fields joined with a newline, keepalive comment lines, `retry:` values that must be rejected unless they are digits, ids carrying a NUL byte, chunk boundaries that split a line in half and `\r\n` from one proxy next to `\n` from another. Every gateway that hand rolls this gets at least one wrong.

## Why I built it

I kept seeing teams treat `Last-Event-ID` as an afterthought, then lose model deltas, leak secrets into replay logs or replay the wrong tenant's stream during a support incident. Existing tooling sits at the wrong altitude. Client libraries parse SSE well and hold no state. Message brokers hold state well and want infrastructure you do not have at the edge. Neither gives you a bounded, redacting, tenant aware replay buffer you can drop into an OpenResty worker or a Lua sidecar.

So this file does the boring parts carefully in one place: multi line data fields, generated ids, duplicate and conflict detection, byte capped retention, tenant filters, safe `text/event-stream` serialization and embedded self tests. Pure standard library Lua, no database, no dependencies.

## When to use it

- An OpenResty or nginx Lua gateway proxying a streaming LLM API where reconnects restart the whole generation.
- A multi tenant AI proxy where replay must never cross a tenant boundary during an incident.
- A CI or local harness replaying a captured `.sse` file to reproduce a truncated agent run.
- A streaming observability pipeline that needs a few thousand events retained without a broker.
- An MCP or agent layer where tool-calling events arrive over SSE and duplicate delivery must be detected instead of re-executed.
- Auditing a captured stream for leaked keys before it goes into a ticket.

## How it works

Ingest runs through a line oriented state machine built by `new_parser`, `parser_feed`, `parser_line`, `parser_dispatch` and `parser_finish`. `parser_feed` appends the chunk to a string buffer and consumes only complete lines, so a chunk that splits a field in half leaves the remainder buffered until the next feed. Each line is normalized for a trailing `\r`, so mixed framing parses the same. A line starting with `:` increments a comment counter and dispatches nothing. Field parsing follows the spec pattern `^([^:]*): ?(.*)$`. `data` lines accumulate into a table joined later with `\n`, `id` is rejected if it contains a NUL byte and `retry` is accepted only when it matches `^%d+$`. A blank line dispatches, but only when `fields_seen` is true, which stops keepalive blocks from emitting empty events. `parser_finish` flushes a trailing partial line and a trailing unterminated event.

Each event then goes through `normalize`. Data is redacted first by `redact_data`, which runs `redact_json_keys` over ten credential key names in double and single quoted JSON forms, then applies `DEFAULT_PATTERNS`: `Bearer` prefixes, `sk-` keys, GitHub `gh[pousr]_` tokens, `api_key:` and `secret:` assignments. Redaction happens before storage, so the retained copy is the redacted copy and there is no window where the raw secret sits in the buffer. If the event has no id, `make_id` generates `edge-<n>-<hash32>` seeded from the clock, a counter, the event name and the data. `normalize` computes a `digest` with `hash32` over id, event name, data and tenant joined by the ASCII record separator `\30`, plus a `bytes` estimate. `hash32` is djb2 in eight hex characters, used for content identity and id generation only. It is not a security boundary.

Retention is a FIFO ring: a sparse `events` table keyed by a monotonically increasing `seq`, with `first_seq` and `next_seq` as head and tail cursors, plus a `by_id` map from id to seq for O(1) lookup. `append` checks `by_id` first. Same id and same digest returns `"duplicate"` and stores nothing, which makes ingest idempotent under at-least-once delivery. Same id with a different digest returns `"conflict"`, surfacing an upstream that reuses ids for different payloads instead of silently corrupting the ledger. Otherwise the event is stored and `evict_if_needed` drops from the head until both `max_events` and `max_bytes` are satisfied. Defaults are 4096 events and 8 MiB, so memory is bounded by construction.

`replay_after` is the reconnect path. Given a `Last-Event-ID` it looks up the seq, starts at `seq + 1` and walks to `next_seq - 1`, applying an optional `tenant` filter and stopping on `limit_events` or `limit_bytes`. If the id is not in `by_id` the cursor has aged out: status becomes `"stale"`, the `stale_replay` metric increments and it returns an empty list with `first_available_id` and `last_available_id` so the client can resynchronize. Pass `include_tail_on_stale` and it returns the retained tail instead, still marked stale. The tenant filter is applied inside the walk, which keeps one tenant's replay from ever including another's events.

On the way out, `to_sse` serializes back to wire format: `id:` only when non empty, `event:` only when it is not the default `message`, `retry:` floored to an integer, one `data:` line per line of payload and a terminating blank line. `clean_header_value` strips CR and LF from ids and event names, closing the response splitting hole naive SSE serializers leave open. `backpressure_headers` returns the `x-sse-ledger-*` counters and ids for your logs, plus `retry-after: 1` when the replay came back stale. `summary` and `summary_json` expose accepted, duplicate, conflict, evicted, redacted and stale_replay.

## Usage

As a CLI over a captured stream:

```bash
# summary of a captured stream
lua EdgeSseReplayLedger.lua < stream.sse

# machine readable summary
lua EdgeSseReplayLedger.lua --json < stream.sse

# what should a client that last saw id "evt-42" receive
lua EdgeSseReplayLedger.lua --last-event-id evt-42 --tenant acme < stream.sse

# same, re-emitted as text/event-stream ready to write to a socket
lua EdgeSseReplayLedger.lua --last-event-id evt-42 --raw-replay < stream.sse

# cursor aged out: send the retained tail anyway
lua EdgeSseReplayLedger.lua --last-event-id old-id --include-tail-on-stale < stream.sse

# tighter retention, redaction off for local debugging
lua EdgeSseReplayLedger.lua --max-events 512 --max-bytes 262144 --no-redact < stream.sse

# embedded checks
lua EdgeSseReplayLedger.lua --self-test
lua EdgeSseReplayLedger.lua --help
```

As a module inside a gateway worker:

```lua
local Ledger = require("EdgeSseReplayLedger")

local ledger = Ledger.new({
  max_events = 2048,
  max_bytes  = 4 * 1024 * 1024,
  default_tenant = "acme",
  redact = true,
})

-- upstream chunks as they arrive
ledger:ingest_sse(chunk, { tenant = tenant_id })

-- or append a single event yourself
local status, event = ledger:append(
  { id = "evt-42", event = "delta", data = "hello" },
  { tenant = tenant_id }
)
-- status is "accepted", "duplicate" or "conflict"

-- reconnect handler
local replay = ledger:replay_after(headers["Last-Event-ID"], {
  tenant = tenant_id,
  limit_events = 500,
  limit_bytes = 262144,
  include_tail_on_stale = false,
})

for name, value in pairs(ledger:backpressure_headers(replay)) do
  ngx.header[name] = value
end

if replay.status == "stale" then
  -- replay.first_available_id / last_available_id tell the client where to resync
end

for i = 1, #replay.events do
  ngx.print(Ledger.to_sse(replay.events[i]))
end

ngx.log(ngx.INFO, ledger:summary_json())
```

## Notes

- In memory only. A worker restart loses the ledger and every in flight client sees a stale cursor on its next reconnect. That is the trade for zero dependencies.
- The default clock is `os.time() * 1000`, second resolution despite the millisecond unit. Pass your own `clock` option, for example `ngx.now() * 1000`.
- No locking. One ledger per Lua state. Sharing across nginx workers needs a shared dict or a per worker instance plus sticky routing.
- Eviction is global FIFO, not per tenant. A loud tenant can push a quiet tenant's events out of the window. Run separate ledgers per tenant if that matters.
- A conflict is reported, never resolved. `append` keeps the original event, so an upstream reusing ids for different payloads has its later payloads dropped. Watch the `conflict` counter.
- Redaction is ten key names and five patterns. It catches common shapes and will miss novel credential formats. Supply your own `patterns` table and treat `--no-redact` as a local debugging flag only.
- CLI exit codes are 0 on success and 1 on any error, message on stderr. `--json` is ignored when `--last-event-id` is given, since that path prints replay JSON unless `--raw-replay` is set.
- No HTTP layer included. It parses, retains and serializes, you wire it to your own handler. `to_sse` omits the tenant field on purpose, since tenant is a server side label.
