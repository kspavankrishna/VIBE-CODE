# Trace Secret Scrubber

Your service logs a prompt, a tool call and a dict of request headers. One of them carries a live API key. It is now sitting in your observability vendor's storage, replicated, indexed and searchable by everyone with a dashboard login.

**Language:** Python | **Lines:** 104 | **Added:** 2026-04-10

## What this solves

This solves secret leakage in logs, traces and LLM telemetry. Teams now dump prompts, tool calls, headers and retry payloads into observability systems, then realize too late that keys and bearer tokens went with them. The failure is quiet. Nothing crashes, no alert fires, and the first sign of trouble is a vendor rotation email or a bill for compute you did not run.

The specific shape of the problem in LLM systems is that the payload is not structured the way your log redaction rules expect. A traditional web app leaks secrets in a known place: the `Authorization` header, a query string, a config dump. You can write a header allowlist and be done. An agent trace leaks them everywhere. The secret is inside a free text prompt because a user pasted it. It is inside a tool call argument because the planner decided to pass credentials downstream. It is inside an error string because an HTTP client echoed the failing request back. It is three levels deep in a nested dict of retries. Header based redaction catches none of that.

The cost is concrete. A leaked provider key means somebody else is spending your inference budget. A leaked GitHub token means somebody can push to your repos. A leaked JWT means session takeover for whatever user issued it. Even when nothing is exploited, you still pay: secrets in a SaaS log store are a compliance finding, and the cleanup means rotating every credential that might have been in the window, purging indexes you may not fully control and writing an incident report. Who notices is usually not you. It is a security researcher, a vendor's automated scanner or an attacker.

The other half of the problem is that naive redaction destroys debugging. Replace every secret with `***` and you can no longer tell whether request 4 and request 9 used the same key, whether a retry storm came from one bad credential or fifty, or whether the token in the failing trace matches the one in the working trace. You lose the exact signal you opened the trace for. This scrubber replaces each secret with a stable mask instead, so identity and correlation survive while the plaintext does not.

## Why I built it

Most existing tooling sits at the wrong layer. Secret scanners like the ones bolted onto git run over repositories after the fact and tell you a key was committed. Log processors from observability vendors let you write redaction rules, but those rules live in the vendor's config, run after the data has already crossed the network and usually work on flat strings or a fixed header list. Neither helps when the payload is an arbitrarily nested Python object about to be handed to a tracing SDK inside your own process.

What I wanted was a single class I could call one line before the export, with no dependencies, no service to run and no config file to sync. Catch the known token shapes, catch suspicious high entropy strings that do not match any known shape, and replace them with stable masks so debugging still works. That is the whole idea, and it is small enough to read in one sitting and audit before you trust it.

## When to use it

- Before handing a prompt, completion or tool call payload to Langfuse, OpenTelemetry, Datadog or any other trace exporter
- Inside an exception handler that logs the full request dict of a failed provider call, headers included
- When you log agent state between steps and that state carries whatever the last tool returned
- When users paste credentials into a chat box and your transcript storage keeps everything verbatim
- Before writing eval fixtures or replay traces to disk from real production traffic
- In a log formatter or logging filter, as the last transform before the record is serialized

## How it works

The entry point is `TraceSecretScrubber.scrub(payload)`. It walks the payload recursively and dispatches on type. A `Mapping` is rebuilt as a plain dict with every value passed through `_scrub_value`. A `Sequence` that is not `str`, `bytes` or `bytearray` is rebuilt as a list with every element scrubbed. A `str` goes to `_scrub_text`. Anything else is returned as is. That type check ordering matters: strings are sequences in Python, so the explicit exclusion is what stops the walker from shredding text into characters.

`_scrub_value` handles the structural case. If the key lowercases into `SECRET_KEYS`, a set holding `api_key`, `apikey`, `token`, `secret`, `password`, `passwd`, `authorization`, `cookie`, `session`, `access_token` and `refresh_token`, the value is masked outright with the label `key:<name>` and never inspected further. Otherwise the value recurses back into `scrub`. This is the cheap, high confidence path: if the field is called `password` there is nothing to reason about.

`_scrub_text` handles the hard case, free text. It runs seven compiled regexes in `VALUE_PATTERNS` in order. Those cover Stripe style `sk_`, `rk_` and `pk_` prefixes, GitHub `ghp_`, `gho_`, `ghu_`, `ghs_` and `ghr_` tokens, AWS `AKIA` access key ids, JWTs matched by the `eyJ` header prefix plus two dot separated segments, full PEM private key blocks matched non greedily from BEGIN to END, `Bearer` followed by at least sixteen token characters, and a generic assignment form where a key name like `token`, `secret`, `password` or `api_key` is followed by `:` or `=` and a value of eight or more characters with optional matching quotes. Every match goes through `_replace_match`, which decides the label by group count: four groups means the assignment pattern, so the output is rewritten as `name=<redacted:name:digest>`, two groups starting with `bearer` preserves the `Bearer ` prefix and masks only the token, and anything else masks the whole match with the label `pattern`.

Pattern matching only catches what you thought to enumerate, so the last stage is `_mask_entropy_tokens`. It scans for any run of twenty or more characters from `[A-Za-z0-9+/=_\-.]`, which is the alphabet base64, base64url and most opaque credential formats live in, and decides per token. `_looks_sensitive` first requires at least eight distinct characters, a cheap filter that kills long repeated or low variety strings, then computes Shannon entropy over the token with `_entropy` and requires at least 3.6 bits per character. Random looking high entropy blobs get masked. English words, snake case identifiers and repetitive filler do not clear the bar. This is the catch all for credential formats that did not exist when the regex list was written.

Masking itself is in `_mask`. It takes `sha256(f"{salt}:{raw}")`, truncates the hex digest to ten characters and emits `<redacted:{label}:{digest}>`. The salt defaults to `trace-scrubber` and is set per instance in `__init__`. Because the hash is deterministic, the same secret produces the same mask everywhere in the trace, which is what makes correlation possible after redaction. A `self.cache` dict memoizes raw to masked so repeated secrets in a large payload cost one hash instead of many. The transform is one way: there is no unmask, by design.

## Usage

```python
from TraceSecretScrubber import TraceSecretScrubber

scrubber = TraceSecretScrubber(salt="your-own-secret-salt")

payload = {
    "provider": "openai",
    "api_key": "sk_live_<example-placeholder>",
    "headers": {"Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.body.sig"},
    "prompt": "call the tool with token=hunter2hunter2 and continue",
}

safe = scrubber.scrub(payload)
tracer.log(safe)
```

Run the built in demo, which scrubs a sample nested payload and prints the result:

```bash
python3 "TraceSecretScrubber.py"
```

Reuse one instance across a process so the mask cache and the salt stay consistent. A new instance with a different salt produces different digests for the same secret.

## Notes

- Python 3.9 or newer, standard library only. No dependencies, no network calls, no config file.
- Key matching is exact lowercase membership in `SECRET_KEYS`. Common variants like `x-api-key` or `client_secret` are not in the set and will only be caught if their value trips a regex or the entropy check.
- The entropy stage has real false positives. Git commit SHAs, UUIDs and long dotted or slashed identifiers can clear 3.6 bits per character and get masked. It is deliberately biased toward over redacting.
- The entropy stage also has false negatives. Anything shorter than twenty characters or with fewer than eight distinct characters is left alone regardless of how sensitive it is.
- The mask is a truncated hash, not encryption. With the default salt it is guessable for short or low entropy inputs, so set your own salt and treat it as a secret.
- `self.cache` is unbounded and keyed by the plaintext secret, so a long lived instance holds those values in memory for the life of the process.
- Containers are normalized, not preserved. Mappings come back as plain dicts and sequences as lists, so tuples and custom container types change shape. Sets, bytes, dataclasses and model objects pass through untouched and are not scrubbed.
- Dict keys are never scrubbed, only values. The assignment regex also normalizes `key: "value"` to `key=<redacted:...>`, so quoting inside scrubbed text changes.
- There is no CLI, no argparse and no exit codes. It is a class with a `__main__` demo block, meant to be imported.
