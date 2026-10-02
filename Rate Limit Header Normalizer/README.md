# Rate Limit Header Normalizer

Every LLM API tells you how long to wait in a different format, and most retry code guesses. This is a single file ReScript module that turns the real headers from Anthropic, OpenAI style and IETF style responses into one snapshot, then decides whether to proceed, wait or give up.

**Language:** ReScript | **Lines:** 585 | **Added:** 2026-10-02

## What this solves

A 429 response is not one thing. Anthropic sends `anthropic-ratelimit-tokens-reset` as an RFC 3339 timestamp. OpenAI style gateways send `x-ratelimit-reset-requests` as a duration such as `1m30.5s` or `20ms`. Other servers send the IETF draft `RateLimit-Reset` as bare seconds, or the structured `RateLimit: limit=10, remaining=0, reset=7` form. Then `Retry-After` shows up as whole seconds, fractional seconds or an HTTP date, and some proxies add `retry-after-ms`. Some send epoch seconds. A few send epoch milliseconds.

Most retry loops pick one of these, ignore the rest and fall back to exponential backoff. The result is two bad outcomes. Either you retry too early and burn another request against a bucket that is still empty, or you sleep far longer than needed and your agent looks frozen. If you run many workers, they all wake at the same instant and hit the limit together.

There is also a quieter bug. If the server clock and your clock disagree by a few seconds, an absolute timestamp gives you the wrong wait. And some requests can never succeed: if you ask for 5000 tokens through a key whose limit is 1000 per minute, waiting will not help. A retry loop that does not know this will spin until its attempt cap.

This module handles all of that in one place and returns a decision you can act on.

## Why I built it

I kept seeing the same retry helper copied between projects, each one handling exactly the header format of the provider the author tested on. Switching a gateway or adding a second provider broke it silently. Nothing crashed. The waits were just wrong.

I wanted something that treats the headers as untrusted input, says plainly when it could not parse one, and never makes the caller wait less than the server asked for. I also wanted it testable. Everything is a pure function except the tiny CLI at the bottom, and the jitter comes from a seeded generator so a plan can be replayed exactly from its inputs.

ReScript was a good fit because the decision is a variant type. `Proceed`, `Wait` and `GiveUp` are different shapes and the compiler forces the caller to handle each one. The give up reasons are their own variant too: `NeverFits`, `DeadlineExceeded`, `ServerSaidNoRetry` and `WaitAboveCap`.

## When to use it

Use it when you call an LLM API or any rate limited HTTP service and you want one retry brain regardless of provider. It suits agent runners that fan out many calls, gateways that sit in front of several vendors and batch jobs with a hard deadline.

It also works as a standalone CLI in a shell pipeline or as a library from other ReScript code. If you only ever talk to one provider and a simple `Retry-After` honouring loop is enough, you do not need this.

It does not send requests and it does not sleep. It decides. You keep your own HTTP client and your own scheduler.

## How it works

The module has four parts: parsing, normalising, planning and the JSON edge.

**Parsing.** `parseDuration` reads strings like `6m0s`, `1.5s`, `20ms` and `1h2m3s`. It uses a global regex over the units `ms`, `h`, `m`, `s` and `d`, and it only accepts the string if every character was consumed. A value like `5x` or `1s junk` returns `None` instead of a half parsed number. `parseResetMs` accepts a duration first, then a bare number. A bare number at or above `1e12` is treated as epoch milliseconds, at or above `1e9` as epoch seconds and anything smaller as relative seconds. If that fails it tries a date string. Absolute values are converted to a relative wait and clamped at zero, so a timestamp in the past means zero rather than a negative sleep.

`parseRetryAfter` prefers `retry-after-ms` when present. Otherwise it reads `Retry-After` as seconds (fractions allowed), then as a duration, then as an HTTP date. `parseStructured` handles the `RateLimit: limit=..., remaining=..., reset=...` form.

**Normalising.** `normalize` takes a header dictionary and the current time. `lowerKeys` makes lookup case insensitive and `firstHeader` tries several header names in order. The function first works out clock skew: if a `Date` header is present, `skewMs` is the server time minus your `nowMs`, and every absolute timestamp is corrected by it. That is how an Anthropic style RFC 3339 reset stays right even when your machine clock drifts.

`readBucket` builds a `bucket` of `limit`, `remaining` and `resetMs` for each of `requests`, `tokens`, `input-tokens` and `output-tokens`. It reads both the `anthropic-ratelimit-<name>-*` family and the `x-ratelimit-*-<name>` family. If `remaining` is above `limit` the header set is stale or mixed, so it clamps to the limit and adds a note to `warnings`. A negative `remaining` becomes zero. Anything unparseable is skipped and recorded in `warnings`, never thrown. If no vendor specific request headers exist, the IETF `ratelimit-*` headers fill the request bucket.

**Planning.** `plan` takes the `snapshot`, a `need` record (`needRequests`, `needTokens`, `needInput`, `needOutput`), a `policy` and a few labelled arguments: `~attempt`, `~ageMs`, `~deadlineMs` and `~seed`.

First, if `x-should-retry` was `false` and this is a retry, it gives up with `ServerSaidNoRetry`. Second, if any requested amount is greater than that bucket's `limit`, it gives up with `NeverFits` and names the dimension. No amount of waiting fixes that, so the caller should shrink the request instead.

Then it looks at every dimension where `remaining` is lower than what you need. The wait for that dimension is its `resetMs` minus `ageMs`, which is how old the headers are by the time you decide. If the reset is unknown it uses `exponentialBackoff`, which is `baseBackoffMs` times two to the attempt, capped at `maxBackoffMs`. The largest wait wins and its name becomes the `binding` field, so you can see which limit is holding you up. `Retry-After` acts as a floor: if it is larger than the bucket wait it becomes the binding one.

If no header gives guidance and this is a retry, the plan falls back to plain backoff and says so with the binding `backoff`.

Jitter is applied last through `rand01`, a seeded mulberry32 generator. It only ever adds time, up to `jitterRatio` of the wait, plus `safetyPadMs`. It never subtracts, so the caller never wakes before the server's stated time. Many workers with different seeds spread out instead of waking together. If the result is above `maxWaitMs` you get `WaitAboveCap`. If it is above `deadlineMs` you get `DeadlineExceeded`, which lets a caller fail fast or hedge to another provider instead of sleeping past its own timeout.

**JSON edge.** `run` is the entry point. It takes one parsed JSON request and returns one JSON object containing the `snapshot` and the `decision`. Header values may be strings, numbers or arrays, because different HTTP clients hand you different shapes. Arrays use the first value. `main` reads stdin and prints the answer. Invalid JSON prints an error to stderr and exits with code 2.

The defaults live in `defaultPolicy`: 500 ms base backoff, 60 s backoff cap, 300 s wait cap, 250 ms safety pad and 0.15 jitter ratio.

## Usage

Build with ReScript 11. Put `RateLimitHeaderNormalizer.res` and the included `rescript.json` in a folder, install `rescript` and run the build:

```sh
npm install rescript@11
npx rescript build
```

That produces `RateLimitHeaderNormalizer.bs.js`. Feed it a JSON request on stdin:

```sh
echo '{
  "nowMs": 1700000000000,
  "headers": {
    "Date": "Tue, 14 Nov 2023 22:13:20 GMT",
    "anthropic-ratelimit-tokens-limit": "80000",
    "anthropic-ratelimit-tokens-remaining": "1000",
    "anthropic-ratelimit-tokens-reset": "2023-11-14T22:13:50Z",
    "retry-after": "12"
  },
  "need": { "tokens": 5000 },
  "attempt": 1,
  "seed": 3
}' | node RateLimitHeaderNormalizer.bs.js
```

The output is one line of JSON. It holds a `snapshot` with the four buckets, `retryAfterMs`, `skewMs` and `warnings`. It also holds a `decision` that is one of three shapes:

```json
{"action":"proceed"}
{"action":"wait","waitMs":31000,"binding":"tokens"}
{"action":"give_up","reason":"never_fits","detail":"..."}
```

Request fields: `headers`, `need` (`requests` defaults to 1, plus `tokens`, `inputTokens`, `outputTokens`), `attempt`, `ageMs`, `deadlineMs`, `seed`, `nowMs` and an optional `policy` object with `baseBackoffMs`, `maxBackoffMs`, `maxWaitMs`, `safetyPadMs` and `jitterRatio`. Missing fields fall back to defaults. If `nowMs` is missing it uses the current time.

From ReScript code you can call the pieces directly: `normalize(headers, nowMs)` and then `plan(snap, need, defaultPolicy, ~attempt=1, ~ageMs=0.0, ~deadlineMs=Some(30000.0), ~seed=7)`.

## Notes

- Give up reasons are exposed as `never_fits`, `deadline_exceeded`, `server_said_no_retry` and `wait_above_cap`. Treat `never_fits` as a signal to split the request, not to retry.
- The `ageMs` field matters. If you queue a request for four seconds after reading the headers, pass `4000` so the reset window shrinks by that much.
- The bare number heuristic for resets is a guess by design. Values from `1e9` upward are read as epoch time. If a provider sends relative seconds above that, you will get a zero wait, and the warning list will not catch it.
- A bucket with a known `limit` but no `remaining` is never treated as exhausted. Only a reported `remaining` below the need triggers a wait.
- IETF style headers are assumed to describe the request quota. If your gateway uses them for tokens, map them to vendor style names before calling.
- Jitter is deterministic for a given `seed`. Use a different seed per worker, such as a worker id, to spread wake ups.
- I tested the build with ReScript 11.1 and checked the CLI against clock skew, stale headers, deadline overruns, unfittable requests and bad JSON. There is no test suite file in this folder.
