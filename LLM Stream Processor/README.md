# LLM Stream Processor

Streaming LLM responses arrive as half objects. A chunk gets cut mid brace, the socket stalls, the API rate limits you, and your shell pipeline hands `jq` a fragment that is not valid JSON. This is a small Bash processor that buffers a stream until a JSON object is actually complete, validates it, then emits it.

**Language:** Bash | **Lines:** 111 | **Added:** 2026-04-07

## What this solves

Every streaming LLM API sends you bytes, not objects. Claude, OpenAI and local runtimes like llama.cpp and Ollama all stream server sent events or newline delimited JSON, and the transport has no obligation to align a TCP read with a JSON boundary. So a single `read` from the socket gives you something like `{"type":"content_block_delta","delta":{"text":"hel` and nothing else until the next flush. Pipe that straight into `jq` and it dies. Pipe it into `grep` and you silently lose the tail of the message.

The failure mode in production is not a crash, it is quiet truncation. A batch job summarising 4000 support tickets pipes the model stream through a shell script, and 3% of the summaries end mid sentence because a chunk boundary fell inside a JSON string. Nobody notices for two weeks, because the output looks plausible. When somebody does, you cannot tell which rows are bad without re running the whole batch and paying for the tokens twice. That is the real cost: silently corrupted output that already went downstream.

The second failure mode is the hang. The API stops sending, the connection stays open and your `while read` loop sits there forever. A cron job that should take four minutes is still running the next morning, holding a lock, and the retry that was meant to fix it never fires because the first attempt never finished. Third is retries: rate limits and 529s are normal, not exceptional, and hammering the endpoint straight after a 429 makes it worse. You need backoff in the same script, not in a wrapper you forgot to write.

## Why I built it

Every language has a decent streaming SSE client. Bash does not. The moment your pipeline is `curl | something | something`, you are on your own: `jq --stream` assumes well formed input, `read -d` assumes you know the delimiter, and every blog post on parsing streaming JSON in shell says "use Python". Often you cannot, or will not, add a Python dependency to a container that already has curl and jq.

So this is the missing piece: self contained Bash functions that take a stream of partial JSON, hold a buffer, count braces to find where an object ends, check it with jq, and hand you complete objects one at a time. No runtime, no packages, no virtualenv. Source it or run it.

## When to use it

- Piping `curl -N` against a streaming chat completions endpoint inside a shell script and needing whole JSON events, not fragments.
- A cron or CI job that calls a model API and must not hang forever when the endpoint stalls mid response.
- A container that has bash, curl and jq and nothing else, where adding a Python or Node dependency is not on the table.
- Shell retry logic around a rate limited endpoint, where you want 1s, 2s then 4s backoff instead of an immediate hammer.
- Capturing a raw stream to disk for replay while still processing it live.
- Pulling a single field out of a JSON event when jq may or may not be installed on the host.

## How it works

The core is `process_stream`, which takes the input stream path, an output file (default `.stream_output`) and an optional per chunk handler command. It reads with `while IFS= read -r -n $BUFFER_SIZE line`, where `BUFFER_SIZE` is 8192, so each iteration takes up to 8 KB or up to the next newline, whichever comes first. That read is deliberately not line oriented, because streaming APIs do not promise newlines where you want them.

Object boundary detection is a brace depth counter, the simplest thing that works. For every character in the chunk it increments `json_depth` on `{` and decrements on `}`, then appends the chunk to `buffer`. When `json_depth` returns to 0 and the buffer matches the regex `\{.*\}`, that buffer is a candidate object and goes to `validate_json`, which pipes it through `jq empty`. If jq accepts it, the buffer is appended to the output file, `chunk_count` is bumped, the object is piped to the handler and the buffer resets. If jq rejects it, the buffer is kept and reading continues, because the most likely explanation is that more bytes are coming.

There is a bound on that patience. If a buffer that keeps failing validation grows past 65536 bytes, it is truncated to its last 32768 bytes: `buffer="${buffer: -32768}"`. A deliberate trade: throw away the head of a hopeless buffer rather than let a garbage stream grow process memory without limit. When the loop ends, anything still in `buffer` is flushed to the output file unvalidated, so a truncated final object is preserved rather than dropped.

Stall protection is the process substitution feeding the loop: `< <(timeout $TIMEOUT cat "$input_stream" || echo "TIMEOUT")`, with `TIMEOUT` at 30 seconds. That is a wall clock cap on the whole stream, not an idle timeout, so a legitimately long generation gets cut at 30s the same as a dead socket. On expiry the literal string `TIMEOUT` enters the stream and shows up in the output as a marker you can grep for.

`retry_with_backoff` is separate and standalone. It takes a command string, runs it through `eval`, and on failure sleeps `BACKOFF_BASE * 2^(attempt-1)`, so 1s, 2s then 4s across `MAX_RETRIES` of 3. Progress lines go to stderr, and after the third failure it prints `[ERROR] Failed after 3 retries` and returns 1. Classic exponential backoff, no jitter.

`extract_field` is the convenience accessor. With jq present it runs `jq -r ".${field}"`. Without jq it falls back to `grep -o "\"${field}\":[^,}]*"` piped through `cut` and `tr`, which handles flat scalar fields and nothing more. `main` wires it together: source defaults to `/dev/stdin`, output to `.llm_output`, banners go to stderr, `process_stream` output is teed to `${output}.log`, and the final count comes from `wc -l < "$output"`. The `[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main "$@"` guard means you can source the file for the functions without running anything.

## Usage

```bash
# Run directly, stream in on stdin
curl -N -s https://api.example.com/v1/messages \
  -H "content-type: application/json" \
  -d @request.json \
  | ./LLMStreamProcessor.sh /dev/stdin ./events.jsonl

# Or point it at a fifo or a captured file
mkfifo /tmp/llm.fifo
./LLMStreamProcessor.sh /tmp/llm.fifo ./events.jsonl

# Source it and use the functions directly
source ./LLMStreamProcessor.sh

# third argument is the per chunk handler command
process_stream /dev/stdin ./events.jsonl "jq -c .delta.text"

retry_with_backoff "curl -sf https://api.example.com/v1/messages -d @request.json -o resp.json"

extract_field "$(cat resp.json)" "model"
```

Defaults: `main` uses `/dev/stdin` as the source and `.llm_output` as the output file, and also writes `.llm_output.log`. Constants at the top are `BUFFER_SIZE=8192`, `TIMEOUT=30`, `MAX_RETRIES=3`, `BACKOFF_BASE=1`.

## Notes

- Brace counting is not JSON aware. A `{` or `}` inside a string value shifts `json_depth`, so a model that emits code or JSON inside its text can push the depth off. The `jq empty` check is what catches the resulting garbage, and the buffer keeps growing until the 64 KB trim fires.
- `validate_json` returns success when jq is not installed, so on a host without jq every candidate buffer is accepted unchecked. Install jq if you care about correctness.
- The 30s timeout covers the entire stream, not idle time. Long generations get cut. Raise `TIMEOUT` for anything that legitimately runs longer, and expect a literal `TIMEOUT` line in the output when it fires.
- The handler default is written as `${3:cat}`, which is substring expansion, not a fallback. Pass a handler explicitly if you want one. With two arguments the per chunk pipeline expands to nothing and each object is only appended to the output file.
- Output files are opened with `>>`, so runs accumulate. Delete or rotate the output and its `.log` between runs, otherwise the final chunk count from `wc -l` includes previous runs.
- `retry_with_backoff` uses `eval` on its argument. Only pass commands you construct yourself, never anything built from model output or untrusted input.
- The explanation block at the end of the file uses C style `/* */` delimiters, which Bash does not understand. Strip it or convert it to `#` comments before running the script.
- No jitter on the backoff and no handling of `Retry-After`. Many parallel workers retrying together will still align on the same 1, 2, 4 second boundaries.
