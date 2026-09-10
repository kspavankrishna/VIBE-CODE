# Stream Replay Fence

A replay manifest looks fine in a spreadsheet and still wrecks your downstream tables: duplicate idempotency keys, offset gaps, late event-time rows, retry loops, mixed schema versions and events from the wrong cloud region. This is one Julia file that audits the manifest before any worker replays it, then writes a batch plan marking each batch replay, inspect or hold.

**Language:** Julia | **Lines:** 410 | **Added:** 2026-07-27

## What this solves

This solves the April 2026 event replay problem that keeps hurting data engineering, AI ingestion, RAG indexing, webhook delivery, CDC repair and DevOps incident recovery. Teams need to replay records after a bad deploy, a schema migration, a model eval rebuild or a streaming outage, but they do not know whether the manifest is safe. Someone exports a CSV of events to reprocess out of Kafka, Kinesis, Pub/Sub, Redpanda or an SQS dead letter queue, eyeballs it, and starts the workers. The workers write side effects.

The failure modes are boring and expensive. The same `event_id` appears twice because the export joined two snapshots, so a payment or a webhook fires twice. Two rows share an `idempotency_key` but carry different `payload_hash` values, so the sink picks a winner and you do not know which. A partition holds offsets 10, 11 and 14, so three records never come back and nobody notices until a reconciliation report next quarter. Arrival timestamps sit an hour past event time, so an aggregation window that already closed gets rewritten. Some rows carry an EU region while the worker runs in us-east-1, which turns a recovery job into a residency incident.

None of that shows up as a red cell in Excel. It shows up hours later as duplicate charges, missing rows, corrupted aggregates or a compliance question you cannot answer, and the person who notices is rarely the one who ran the replay. So the fence runs first, checks identity, ordering, watermark, retry budget, schema and residency in one pass, and leaves evidence you can attach to the incident.

## Why I built it

Streaming tooling assumes the happy path of live consumption. Kafka gives you consumer lag, not manifest sanity. Data quality frameworks validate a table you already loaded, which is the wrong side of the operation when the write itself is the risk. The ad hoc pandas notebook written during every incident gets the duplicate check right and the offset regression check wrong, then gets thrown away.

I wanted one file, no dependencies, no service to send customer payloads to, runnable in CI or off a laptop mid incident, exiting nonzero when the manifest is unsafe. Julia because the whole job is grouping, sorting and interval arithmetic, and the standard library covers it.

## When to use it

- A bad deploy corrupted six hours of Kafka consumption and you must replay offsets before the on call window closes
- A CDC pipeline dropped records during a failover and the repair manifest was hand assembled from two snapshots
- A webhook queue needs redelivery and you cannot afford a duplicate charge or a duplicate notification
- A RAG index or model eval set is being rebuilt from a raw event archive and each document must reach the index once
- A schema migration is half rolled out, so the manifest mixes v2 and v3 rows and the transformer handles one
- A replay batch must stay inside a residency boundary and you need proof, in CI, that no out of region rows were queued

## How it works

Input is a CSV or TSV manifest. `readtable` picks the delimiter by counting tabs against commas on the header line unless you force it with `--delimiter`, normalizes headers through `header_name`, and stops if two headers collapse to the same name. Rows go through `splitrow`, a hand written RFC 4180 style scanner that tracks a quoted flag and turns a doubled `""` into a literal quote. A row whose field count does not match the header does not abort the run: it raises an SRF001 finding, then gets padded or truncated so the rest of the audit still happens. Columns are resolved logically through the `ALIASES` table, so `offset`, `sequence`, `seq` and `lsn` all satisfy `offset`. `parsetime` strips a trailing `Z` or numeric UTC offset, truncates sub millisecond precision and tries five `DateFormat` patterns in order. Everything lands in an immutable `Event` struct where a missing offset or timestamp is `nothing` rather than a sentinel, so later checks must handle it explicitly.

`audit!` is the core. The per row pass covers schema membership against `--expected-schema`, region against `--allowed-regions`, arrival lag against `--max-lag-seconds` and retries against `--max-retry-count`. Severity is graded, not binary: lag over budget is a warning, lag over `max(2 * maxlag, maxlag + 3600)` is an error, and the same doubling rule applies to retries with a `+3` floor. Arrival more than 60 seconds before event time is flagged as clock skew rather than lateness.

The cross row pass uses `buckets`, a grouping helper that maps a key function over the events and skips empty keys. Grouping by `event_id` gives SRF010, escalated to error when the duplicates disagree on payload hash or operation, which separates a benign double export from two records wearing the same id. Grouping by `idempotency_key` gives SRF011 as an error when payload hashes conflict, and SRF040 as a warning when they agree, because identical retries are write amplification rather than a correctness break. Grouping by `payload_hash` gives SRF012 when one payload maps to several event ids.

Ordering is checked per stream and partition through `partkey`. Two rows on one offset is an SRF020 error. Walking the offset sorted sequence flags any gap wider than `--max-gap`. The partition is then sorted by arrival time and walked with a running high water offset and high water event time: an offset arriving below the high water mark is an ordering regression warning, and an event time falling behind the high water mark by more than the lag budget is an SRF030 error, the watermark violation that quietly rewrites closed windows.

Output is three files, written atomically by `writelines` through a `tempname` plus a forced `mv`, so a killed run leaves no half written report. `writefindings` emits rule id, severity, message, affected rows, key, stream, partition and a recommendation. `writesarif` hand builds SARIF 2.1.0 with the `RULES` dictionary as the rule catalog. `writeplan` packs each partition into batches under `--batch-bytes`, and `planrow` labels a batch `hold` if any row in it carries an error level finding, `inspect` if the batch max retry exceeds the budget, and `replay` otherwise.

## Usage

```bash
# audit a replay manifest with defaults
julia StreamReplayFence.jl --input replay.csv

# full run: schema and residency enforcement, tighter watermark, CI gate
julia StreamReplayFence.jl \
  --input replay.csv \
  --output stream-replay-findings.csv \
  --sarif stream-replay-fence.sarif \
  --plan stream-replay-plan.csv \
  --max-lag-seconds 120 \
  --max-gap 0 \
  --max-retry-count 5 \
  --batch-bytes 50000000 \
  --expected-schema v3,v4 \
  --allowed-regions us-east-1,us-west-2 \
  --fail-on error

# run the built in bad fixture end to end
julia StreamReplayFence.jl --self-test
```

Required logical columns, aliases accepted: `stream`, `partition`, `offset`, `event_id`, `idempotency_key`, `event_time`, `arrival_time`, `payload_hash`. Optional and used when present: `schema_version`, `region`, `operation`, `retry_count`, `byte_size`.

## Notes

- Dependencies are `Dates` and `Printf` from the Julia standard library. No packages, no network calls. The manifest never leaves the machine.
- The file is read whole with `readlines` and every event is held in memory. Fine for a few million rows, not for a multi gigabyte manifest.
- `splitrow` works line by line, so a quoted field containing a real newline is unsupported. It raises an unclosed quoted field error rather than guessing.
- `parsetime` strips a trailing timezone offset instead of converting it. Feed it UTC. Mixed offset manifests compare wrong.
- `--expected-schema` and `--allowed-regions` are inert when empty, which is the default. No schema or residency finding is raised until you supply the allowed set.
- Rows missing an offset still load and appear in the plan, sorted to the end of their partition, but are skipped by the gap and duplicate offset checks. `byte_size` defaults to 1 when absent.
- Exit codes: 0 clean or `--fail-on none`, 2 when findings meet the threshold, 64 on a tool error such as a bad flag or unreadable input.
- It audits the manifest. It does not read your broker, does not run the replay and does not verify that the manifest matches what is in the topic.
