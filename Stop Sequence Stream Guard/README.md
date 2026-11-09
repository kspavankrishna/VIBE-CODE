# Stop Sequence Stream Guard

A Mojo stop sequence matcher for streamed LLM output. It holds back only the bytes that could still turn into a stop sequence, so your client never sees half of a stop marker and never sees a broken UTF-8 character.

**Language:** Mojo | **Lines:** 452 | **Added:** 2026-11-09

## What this solves

Every team that streams tokens to a user eventually writes the same bug. The model has a stop list, something like a role marker, a closing tag or a code fence. The tokenizer does not respect those boundaries. A stop such as `\nUser:` can arrive as `\nUs`, then `er`, then `:`. If you check each chunk on its own you miss the stop and the model's imagined next turn leaks into the UI. If you check the joined text after every chunk you pay quadratic cost and still have to decide which bytes were already sent.

The usual patch is to hold back a fixed number of trailing characters. That is wrong in both directions. It delays output that was never at risk, and it can release bytes that are the start of a longer stop. It also ignores bytes. Hold back three characters and you can still emit the first two bytes of a four byte emoji, and the client shows a replacement character.

This file solves the whole thing in one pass. It tells you exactly which bytes are safe to release after each chunk, which stop fired, and where to cut. The answer does not depend on how the stream was chunked. I test that directly by running every chunk size against a brute force oracle.

## Why I built it

I kept seeing stop handling done three different ways in three codebases. One used `endswith` on the last chunk. One used a regex over the full buffer. One held back the longest stop length minus one, always. All three were wrong in ways that only show up in production, because production is where chunks split at awkward places and where users paste emoji.

The other reason is cost. Stop checking sits on the hot path of every streamed token, for every concurrent request. An Aho Corasick automaton does constant work per byte whatever the size of the stop list, and Mojo lets me keep the transition table as a flat array of 32 bit integers without an allocation per step. I wanted something I could drop into an inference gateway and forget about.

I also wanted the semantics written down. Different providers disagree about what happens when stops overlap. This file picks the behaviour hosted APIs document and tests it: the output is cut at the earliest start position of any stop sequence.

## When to use it

Use it when you proxy or post process a token stream and you have to enforce stop sequences yourself. That covers a gateway in front of a self hosted model, a local runtime that does not support stops natively, an agent loop that stops on a tool call marker and any pipeline that wants to cut on a delimiter without buffering the whole response.

It is also useful as a reference. If you have an existing implementation in another language, port the self checks and see whether yours survives one byte chunks.

Do not use it for regex stops or for stops that depend on token ids. It matches literal byte strings. It also does not decode text for you. It hands back bytes, and you decide how to write them to your socket.

## How it works

`StopSequenceGuard.__init__` takes a `List[String]` of stops and builds an Aho Corasick automaton. States live in `goto_table`, a flat `List[Int32]` with `ALPHABET` (256) entries per state. After the trie is built, a breadth first pass fills in failure links and turns every missing edge into a real transition, so the matcher never backtracks. The same pass computes two arrays. `out_len[state]` is the length of the longest stop that ends at that state, inherited through failure links. `out_id[state]` is the index of that stop. `depth[state]` is the length of the longest live partial match.

The constructor refuses input that would cause trouble later. An empty stop raises, because it would match everywhere. A stop that begins with a UTF-8 continuation byte raises, because it could fire in the middle of a character. A stop list that needs more than `MAX_STATES` (65,536) states raises, so a huge user supplied list cannot eat memory. Duplicate stops keep the first index so reports stay stable.

`feed_bytes` is the hot path. For each byte it takes one table step, appends the byte to the `held` buffer and checks `out_len`. When a stop ends here, the start offset is `consumed - out_len`. The guard keeps the smallest start it has seen in `best_start`. It cannot confirm the cut immediately, because a longer stop that starts earlier might still be in flight. A stop starting before `best_start` has length at most `max_len`, so it would already have completed by the time `consumed` reaches `best_start + max_len - 1`. That is the confirm point. At that moment `finished` flips, the bytes before `best_start` are released and the rest of the stream is counted in `dropped_after_stop` and ignored.

While no stop is confirmed, the release limit is `consumed - depth[state]`. Everything before that offset cannot be the beginning of any stop, because `depth` is the longest suffix that is still a prefix of one. If a match is pending, the limit is also capped at `best_start`. The helper `_release` moves that many bytes out of `held` and advances `emitted`.

Then the UTF-8 rule applies. If the limit lands on a continuation byte, the cut backs up to the lead byte. If the limit is the end of the buffer, `_incomplete_tail` checks whether the tail is an unfinished multibyte character and holds those bytes too. Invalid UTF-8 returns zero, so garbage input is released instead of being held forever. Memory stays bounded by `max_len` plus one chunk, and `peak_held` records the high water mark so you can watch it.

`finish` handles end of stream. A stop that completed in the last few bytes is still a real stop, so a pending `best_start` cuts the output. Otherwise the held tail is released, because it never became a stop. Both `feed_bytes` and `finish` return an `Emission` with `bytes`, `stopped` and `stop_index`. After a stop, further calls return nothing and keep reporting the same stop.

Tie rules: the earliest start wins. If two stops start at the same position, the shorter one is reported, since it completes first. The cut is the same either way.

## Usage

```mojo
from collections import List

fn stream_with_stops() raises:
    var stops = List[String]()
    stops.append("\nUser:")
    stops.append("</answer>")

    var guard = StopSequenceGuard(stops)

    var e = guard.feed("Here is the plan.\nUs")
    # e.bytes holds what is safe to send now. The partial marker stays held.
    e = guard.feed("er: more")
    if e.stopped:
        print("stopped on stop number " + String(e.stop_index))

    var tail = guard.finish()
    # tail.bytes is empty after a stop. Without one it holds the final bytes.
```

`feed` takes a `String` chunk. `feed_bytes` takes a `List[UInt8]` if you already have raw bytes from a socket. Write `e.bytes` to the client as they come. `guard.is_stopped()` tells you when to cancel the upstream request, which is where the saved token spend comes from. `guard.state_count()`, `guard.held_bytes()` and `guard.peak_held` are there for metrics.

Running the file directly with `mojo run StopSequenceStreamGuard.mojo` executes `main`. It checks that an empty stop is rejected, then runs `_check_text` over a set of cases: a stop that straddles chunks, a closing tag split in the middle, a fence at the very end, text with no stop, a stop at offset zero, overlapping stops where the earliest start beats the earliest end, failure link recovery, duplicate stops, multibyte stops and an emoji stop. Each case is streamed with every chunk size from one byte to the whole text plus a ragged pattern, and the output is compared with the brute force `_reference_cut`. It also asserts that no released chunk ends inside a UTF-8 character. It finishes with a printed trace so you can watch the holdback behave.

## Notes

I wrote this against current Mojo syntax and I have not been able to compile it in the environment that produced it, so treat the first `mojo run` as the real test. Mojo moves fast. The likely friction is a renamed standard library call or a change to the `owned` and `deinit` keywords on move initialisers. The automaton and the checks do not depend on those details.

The table costs 1 KB per state. A typical stop list of a dozen short strings is a few dozen states, which is nothing. If you must accept very large user supplied lists, lower `MAX_STATES` or switch to a sparse transition layout.

`feed` copies the incoming string into a `List[UInt8]` before scanning. If that shows up in a profile, call `feed_bytes` with a buffer you already own.

Matching is on exact bytes. It is case sensitive and it does not normalise Unicode, so a stop written with a composed character will not match the decomposed form. Decide on normalisation before the text reaches the guard.

One guard serves one stream. Make a new one per request. The automaton build is cheap, but if you run thousands of streams with the same stop list you can refactor the table into a shared read only value and keep only the per stream cursor fields. I left that out because it adds ownership rules that depend on the Mojo version you are on.
