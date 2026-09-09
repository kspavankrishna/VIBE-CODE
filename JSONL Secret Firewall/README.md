# JSONL Secret Firewall

Your JSONL logs contain bearer tokens. A single file C++ filter that scrubs secrets, API keys, cookies and oversized payload blobs out of NDJSON log lines before they leave the machine.

**Language:** C++ | **Lines:** 462 | **Added:** 2026-04-14

## What this solves

Secret leaks and oversized payload leaks in JSONL logs, trace exports and AI gateway stream archives. Modern LLM apps, MCP servers, CI jobs and edge workers still dump bearer tokens, webhook secrets, cookies and giant prompt blobs into logs when something fails at 2 AM. The failure path is almost always the same. An HTTP client throws, someone catches it and logs the whole request object, and now the `Authorization` header sits in plaintext inside a line that ships to S3, ClickHouse, BigQuery, Datadog or Loki within seconds.

What breaks next is not the app. It is your security posture. The token is now replicated across a log pipeline, an object store with a 90 day retention policy, a search index and probably a Slack alert. Rotating it is the easy part. The hard part is proving where else it went, which means a scoped incident review and a conversation with whoever owns compliance. Vendor log platforms will happily index the secret and make it searchable to every engineer with a read seat. That is how a one line mistake becomes a week of work.

The second failure is quieter and costs money instead of trust. LLM traces carry full prompts and full completions. One retry storm of 200 KB prompt bodies logged as strings will blow through your ingest quota and make the noisy lines slow to render in any viewer. Nobody notices until the invoice arrives. By then the data is already stored.

This tool sits between the producer and the pipeline. It reads JSONL on stdin, rewrites every line and writes JSONL on stdout. Sensitive values become a stable marker, long values are truncated with their original length recorded, and the output stays strictly line oriented so nothing downstream breaks.

## Why I built it

Every existing option has a tax. Full JSON parsers pull in a dependency, allocate a DOM per line and choke the moment a producer emits a line that is not valid JSON, which happens constantly in real log streams. Vendor side scrubbing runs after ingest, which means the secret was already stored before it was masked. Regex scrubbers written in shell or Python are fine at low volume and fall over at pipeline rates, and they have no concept of JSON keys so they cannot tell that `"password": "hunter2"` is a secret while `"note": "hunter2"` might not be.

I wanted one C++ file with no dependency stack that could be compiled into a sidecar, an observability agent, a build step or an incident cleanup script. Key aware where the line looks like JSON, pattern and entropy aware everywhere else, and never crashing on malformed input.

## When to use it

- Piping application or gateway logs to S3, ClickHouse, BigQuery, Datadog or Loki and you want redaction to happen before upload, not after
- Sanitizing an LLM or MCP trace export before handing it to a teammate, a vendor support ticket or a public bug report
- Cleaning a CI job log that captured `curl -v` output with an `Authorization: Bearer` line in it
- Trimming a captured SSE or streaming archive where completion bodies ran to hundreds of kilobytes per line
- Running a one off cleanup pass over an incident bundle before it goes into a ticket system
- Adding a redaction stage inside an ingest worker where a heavy JSON dependency is not welcome

## How it works

Everything runs in `Run(int, char**)` under a top level try block in `main`, so any argument or IO failure surfaces on stderr with exit code 1. Input defaults to `std::cin` and output to `std::cout`, with `--input` and `--output` swapping in binary mode `ifstream` and `ofstream`. The main loop is a plain `std::getline`, and every line follows the same three step path: size gate, dispatch, emit.

The size gate compares the line against `Config::max_line_bytes`, default 512 KB. An oversized line is never scrubbed. It is replaced wholesale with `{"redaction_error":"line_too_large","bytes":N}` and counted in `Stats::line_too_large`. A line that big is more likely to be a dumped binary body than something worth pattern matching, and emitting a valid JSON object in its place keeps the stream parseable.

Dispatch is decided by `LooksJsonLike`, which skips leading whitespace and checks whether the first real character is `{`, `[` or `"`. JSON shaped lines go to `ProcessJsonLikeLine`. Everything else goes to `RedactInlineText`. This is a single pass character scanner, not a parser. `ProcessJsonLikeLine` walks the line, and when it hits a `"` it calls `ParseJsonString` to decode the string with its escapes. It then peeks past whitespace: if the next character is `:` the string was a key, so it is pushed onto a `key_stack` and copied through unchanged. Otherwise it is a value, and the top of `key_stack` tells it what context it is in. If `ContainsSensitiveKeyFragment` matches the key against `kSensitiveKeyFragments`, a 20 entry list covering `api_key`, `authorization`, `bearer`, `token`, `secret`, `password`, `session`, `cookie`, `private_key`, `refresh_token`, `connection_string` and friends, the value is replaced outright. The stack is popped on `,`, `}` and `]`. If `ParseJsonString` fails to find a closing quote the whole line falls back to `RedactInlineText`, so a truncated or malformed line still gets scrubbed rather than passed through raw.

Values that are not in a sensitive key context still get two more passes. `MaybeTruncateValue` caps them at `Config::max_value_chars`, default 2048, and appends `...[TRUNCATED len=N]` so you keep the original size for debugging. Then `RedactInlineText` runs over the result. That function does two things at once. First it tries the eight entries in `kSuspiciousPrefixes`: `sk-`, `ghp_`, `github_pat_`, `xoxb-`, `xoxp-`, `AIza`, `AKIA` and `eyJ`, which cover OpenAI style keys, GitHub tokens and PATs, Slack bot and user tokens, Google API keys, AWS access key IDs and the leading bytes of a base64url encoded JWT header. A prefix match consumes the following base64ish run and redacts it if the whole thing is at least 12 characters. Second, for any other base64ish run it calls `LooksHighEntropyToken`, which requires 24 characters minimum, a fully base64ish alphabet, at least 10 distinct byte values and mixed character classes from `HasMixedClasses`. That distinct byte count is a cheap entropy proxy: it catches random key material without flagging long repetitive identifiers or hex strings that are all one class.

Redactions are built by `BuildRedaction`, which emits `[REDACTED:<reason>:fnv1a64=<16 hex digits>]`. The hash is FNV-1a 64, chosen because it is a few lines of code with no dependency and it is fast enough to run on every redacted value. It is not a security primitive and is not meant to be. It gives you a stable fingerprint so you can tell whether two log lines carry the same secret, correlate a leaked credential across a pipeline or confirm a rotation actually changed the value, all without storing the value. `--no-hash` turns it off when even a fingerprint is too much. Rewritten values go back through `JsonEscape` before emission, so control characters and quotes are re-encoded and the output line stays valid JSON.

## Usage

```bash
# build (C++17, needs from_chars, string_view, optional)
g++ -std=c++17 -O2 -o jsonl-secret-firewall JsonlSecretFirewall.cpp

# stdin to stdout, the normal pipeline shape
cat app.jsonl | ./jsonl-secret-firewall > app.clean.jsonl

# explicit files, with counters printed to stderr on completion
./jsonl-secret-firewall --input traces.jsonl --output traces.clean.jsonl --stats

# tighter value cap, no fingerprint in the redaction marker
./jsonl-secret-firewall --max-value-chars 512 --no-hash < raw.jsonl > safe.jsonl

# raise the oversized line threshold to 1 MB
./jsonl-secret-firewall --max-line-bytes 1048576 --input big.jsonl

./jsonl-secret-firewall --help
```

Flags, all of them real and parsed in `Run`: `--input PATH`, `--output PATH`, `--max-line-bytes N`, `--max-value-chars N`, `--no-hash`, `--stats`, `--help` or `-h`. The `--stats` line goes to stderr and reports `lines_seen`, `lines_emitted`, `line_too_large`, `json_keys_redacted`, `inline_tokens_redacted`, `high_entropy_redacted` and `values_truncated`.

## Notes

- Exit codes are 0 on success or `--help`, and 1 on any thrown error: unknown argument, missing flag value, unparseable size, unopenable input or output, or a failed write. Errors print as `JsonlSecretFirewall error: ...` on stderr.
- This is not a JSON parser. `key_stack` is a heuristic that pops on `,`, `}` and `]`, so deeply nested objects or arrays of objects can associate a value with the wrong key. It errs toward scanning more text, not less, but do not treat key context as exact.
- `ParseJsonString` does not decode `\uXXXX` escapes. It substitutes a single `?`. Any string the tool rewrites loses its escaped Unicode, so this is lossy on non ASCII payloads by design.
- Only JSON string values are inspected. A secret stored as a number, a boolean or an unquoted token under a sensitive key passes through untouched.
- Size arguments are plain decimal only. There is no `1M` or `512K` suffix parsing, and `--max-line-bytes` is checked after the full line is already in memory, so it limits work, not allocation.
- The FNV-1a 64 fingerprint is not cryptographic. It is short enough to brute force against a known candidate value, so treat `--no-hash` as the right choice when the log destination is untrusted.
- Detection is prefix and entropy based, so it will miss a secret that looks like ordinary prose and it will occasionally redact a long random looking identifier such as a content hash or a trace ID. Check the `--stats` counters after a first run against real data before wiring it into a pipeline.
- Requires C++17. The file includes `<sstream>` and friends but not `<stdexcept>`, and relies on `std::runtime_error` arriving transitively. Add the include if your standard library is strict about it.
