# Prompt Cache Planner

Your prompt cache hit rate collapsed after a deploy and the request bodies still look identical to the eye. This tool splits an LLM request into a stable cacheable prefix and a live suffix, hashes both halves and tells you exactly which field broke reuse.

**Language:** Python | **Lines:** 1132 | **Added:** 2026-04-18

## What this solves

Prompt caching on OpenAI, Anthropic and Gemini is a prefix match. The provider reuses work only when the leading tokens of your request are byte for byte what they were last time. That sounds easy until you look at how a real request is assembled: a web handler builds the base body, tracing middleware injects a trace ID, a RAG layer appends retrieved documents with presigned S3 links, a tool registry serializes whatever order its dict iterated in and an eval runner stamps the current time into the system prompt. Every layer is written by a different person. None of them think they are touching the cache.

The failure mode is quiet and expensive. Nothing errors. Latency goes up, spend goes up and cached input tokens sit near zero. When someone finally opens two captured bodies side by side they face eight thousand lines of JSON with one changed UUID buried at `$.metadata.trace_id`, or an `X-Amz-Signature` parameter on an image URL that rotates every fifteen minutes.

Diffing raw JSON does not answer the real question. A plain diff shows everything that changed, including the parts that are supposed to change. The newest user turn is always different. That is the point of a conversation. What you need to know is whether anything changed before the live part, because only that can invalidate a prefix. This tool draws that line, hashes each side separately and gives you a yes or no on prefix stability plus a named list of the fields that moved it. It also catches the breakers you cannot fix by reordering keys. A timestamp inside a message body is prompt content, not metadata, so it gets flagged rather than edited.

## Why I built it

Prompt caching sounds simple in a product note and gets messy in a real codebase. Provider docs tell you to keep prompts stable and dashboards tell you your hit rate, but nothing in between takes a captured request and names the byte that moved. Teams end up bisecting deploys or adding print statements to a shared LLM client, which is a slow way to find a UUID.

I wanted something a backend or platform engineer can run directly on request JSON during an incident, with no SDK, no API key and no network call. It reads a file, prints an answer and exits. That makes it usable as a CI regression gate on request shape, in a local shell while debugging and against recorded traces after the fact, with identical results in all three.

## When to use it

- Cache hit rate dropped after a deploy and you need to name the field that changed, not guess at it.
- You are refactoring a shared LLM client and want a CI check that the stable prefix hash did not move for a fixture request.
- A RAG pipeline injects retrieved documents with presigned URLs and you suspect those links bust the prefix on every call.
- You are deciding what belongs in a long lived cached prefix versus what stays request local, and you want size numbers first.
- You inherited an agent loop with tool schemas built by three different registries and want to know whether the tool order is deterministic.

## How it works

`plan_request` is the whole pipeline. It calls `detect_provider`, which sniffs top level keys: `contents` means Gemini, `input` means OpenAI Responses, `messages` plus `anthropic-version` (or the `system` + `max_tokens` + `model` combination) means Anthropic Messages, a bare `messages` means chat completions shaped, everything else is generic JSON. `--provider` overrides the guess.

Next comes the conversational split. `split_request_payload` picks the sequence field for the detected provider and hands it to `split_sequence_before_latest_user`, which scans for the last element whose `role` is `user` and cuts there. Everything before that index is prefix candidate, everything from it onward is live suffix. That matches how the providers cache: prior turns are reusable, the new turn is not. The cut point lands in a `SplitStats` record.

Then `separate_volatile_fields` walks the prefix candidate recursively, carrying a JSON path, then calls `detect_volatility` on every leaf. That is where the real work lives. It checks the tail key against `VOLATILE_FIELD_NAMES` (request_id, trace_id, session_id, nonce, seed, created_at, presigned_url and about thirty more) and against `VOLATILE_SUBSTRINGS` for partial matches. Then value level detectors: `looks_like_signed_url` parses the URL and tests its query keys against `SIGNED_URL_QUERY_KEYS`, covering the AWS `x-amz-*` set, the GCS `googleaccessid` form and the Azure SAS parameters `sv`, `sr`, `se`, `sp`; `looks_like_uuid` uses an RFC style regex; `looks_like_timestamp` tries ISO 8601 then falls back to `parsedate_to_datetime` for RFC 2822; `looks_like_epoch` accepts numbers in the plausible seconds or milliseconds range; `looks_like_entropy_token` scores character class buckets on strings of 24 characters or more with no spaces.

Context gates the behaviour. `is_prompt_payload_context` and `is_prompt_text_context` mark paths inside message bodies and text fields, `is_schema_context` marks anything under `properties`, `$defs` or `json_schema`. A field moves to the live suffix only when none of those hold, so a `parameters.properties.timestamp` in a tool schema is left alone and a UUID inside a user message is reported as observed in place with `moved_to_live_suffix: false`. `contains_inline_volatility` runs a separate pass over prompt text for embedded UUIDs, ISO timestamps and signature fragments.

Hashing is deterministic by construction. `canonical_json` serializes with `sort_keys=True`, no whitespace and `ensure_ascii=False`, so key insertion order in your builder cannot change the digest. `digest_json` prefixes the algorithm name onto the hex, giving values like `sha256:ab12...` and accepting anything `hashlib.new` knows. `overlay_json` merges the moved fields onto the live suffix, padding lists with `None` so index positions survive.

`compare_requests` plans both sides, then runs `diff_json`, a bounded recursive walk emitting `DiffEntry` records tagged `changed`, `added`, `removed`, `type_changed` or `length_changed`, stopping at `max_diff_entries` (64 by default) so a pathological pair cannot flood your terminal. Prefix and suffix are diffed separately. `build_hints` and the `diagnosis` list then add plain sentences: whether tools are out of sorted order, whether tool names repeat, whether the prefix clears 8192 bytes and the case worth knowing about, where both halves hash identically and the miss must be provider side, model version drift or cache expiry.

## Usage

```bash
# Split one request into stable prefix and live suffix
python PromptCachePlanner.py plan request.json

# Machine readable, with the actual payloads included
python PromptCachePlanner.py plan request.json --json --include-payloads

# Read from stdin, force a provider shape, use a different digest
cat request.json | python PromptCachePlanner.py plan - --provider google-gemini --hash blake2b

# Explain why a deploy changed cache behaviour
python PromptCachePlanner.py compare before.json after.json
python PromptCachePlanner.py compare before.json after.json --json
```

As a library:

```python
from PromptCachePlanner import PlannerConfig, plan_request, compare_requests

plan = plan_request(request_dict)
print(plan.stable_hash, plan.stable_size_bytes)
for match in plan.volatility:
    print(match.path, match.reason, match.moved_to_live_suffix)

result = compare_requests(before, after, config=PlannerConfig(max_diff_entries=200))
assert result.stable_hash_equal, result.render_text()
```

`PlannerConfig` exposes `preserve_last_user_turn`, `hash_algorithm`, `max_preview_chars`, `max_diff_entries`, `stable_text_keys` and `volatile_field_names`. Set `preserve_last_user_turn=False` to skip the split and treat the whole request as prefix candidate.

## Notes

- Standard library only. No dependencies, no network calls and nothing is sent to a provider. Needs Python 3.10 or newer for the `str | int` union syntax.
- It does not count tokens and does not know provider cache minimums. A prefix under the provider's threshold still misses, and this tool will happily tell you the hash matched. Byte size is the only size signal it gives you.
- The split keys on `role == "user"`. Gemini `contents` entries use that role too, so it works there, but a payload with no user role anywhere is treated as entirely stable with an empty live suffix.
- Volatility detection is heuristic and errs both ways. A field genuinely named `session` gets flagged, and a bespoke correlation ID the substring list does not cover gets missed. Extend `volatile_field_names` through `PlannerConfig` when that happens.
- `overlay_json` uses `None` as its list placeholder, so a moved field whose real value is `null` is indistinguishable from an empty slot in the reconstructed suffix.
- Exit codes: 0 on success, 1 on JSON parse error, file I/O error or an unsupported hash algorithm, 2 from argparse on bad arguments. `-` for stdin works only on the first positional argument, so `compare` still needs a real file for `right`.
