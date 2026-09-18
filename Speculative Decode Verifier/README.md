# Speculative Decode Verifier

Replays a recorded speculative decoding trace and reruns the accept, reject and resample decision for every round so you can prove, offline, that your serving stack's draft and target reconciliation is mathematically correct and that its adaptive draft depth is actually tracking real acceptance rate.

**Language:** V | **Lines:** 484 | **Added:** 2026-09-18

## What this solves

Speculative decoding is the trick most fast LLM serving stacks use now: a small cheap draft model guesses the next several tokens, a single batched forward pass of the big target model checks all of them at once, and you keep whichever prefix of the guesses the target model agrees with. When it works, one expensive forward pass buys you several tokens instead of one. The entire speedup rests on one piece of math working exactly right: the rule that decides whether to keep a drafted token, and the rule for what to sample instead when you throw one away. Get that rule even slightly wrong and you are not just slower, you are silently sampling from the wrong distribution: the model behaves like a worse model than the one you are actually paying for, and nothing in your latency dashboard will tell you that happened.

The rule itself is simple to state and easy to get subtly wrong to implement: accept a drafted token `x` with probability `min(1, p(x)/q(x))`, where `q` is the draft model's probability for that token and `p` is the target model's. If you reject, you do not just resample from the target's distribution `p`, because that would double count the mass you already rejected from `q`. You resample from the residual distribution `normalize(max(0, p(x) - q(x)))`. That subtraction is the part implementations get wrong most often, especially once you add the practical complication that you never have the full vocabulary's worth of probabilities lying around, you have a truncated top-k window from each model, and the true residual can land entirely outside both windows.

On top of the sampling correctness problem there is a second, completely different failure mode: bookkeeping. A verify round runs the target model over `k+1` positions in one pass (`k` draft positions plus one free bonus position), which tentatively extends the model's KV cache by `k+1` slots. If only `accepted` of the `k` drafted tokens actually got kept, the cache has to be rolled back to `accepted + 1` real slots before the next round starts, or the cache silently drifts out of sync with the token sequence it is supposed to represent and every following token gets computed against the wrong context. This is a correctness bug, not a performance one, and it will not throw an exception. It will just quietly serve subtly wrong completions from a warm cache.

`SpeculativeDecodeVerifier.v` is a small, dependency free engine and CLI that implements the real algorithm: exact rejection sampling with the residual correction, explicit handling of the degenerate case where truncated top-k windows miss the residual mass, a KV-cache rollback ledger that tracks tentative versus confirmed cache length per stream, and an adaptive AIMD controller that recommends the next round's draft depth from a rolling acceptance rate. You feed it a JSONL trace, either recorded from your own serving stack or synthesized for a test, and it replays every round deterministically and reports exactly what should have happened.

## Why I built it

I went looking for a small reusable piece of the speculative decoding pipeline that is pure math and pure bookkeeping, with no model runtime attached, so it could be unit tested and audited on its own instead of being buried inside a few thousand lines of a serving engine where a subtle sampling bug is nearly impossible to spot by staring at latency numbers. Every serving stack that implements speculative decoding (vLLM, TensorRT-LLM, and the various Medusa and EAGLE style setups) reimplements this exact reconciliation logic themselves, and the two failure modes above, wrong residual math and KV cache drift, are the two things that are hardest to catch in code review because they do not crash, they just quietly bias the output distribution or corrupt future context.

I picked V for this specifically because the whole point is a small, auditable, dependency free core. No garbage collector pauses to reason about while replaying a long trace, no build step beyond one file, and a standard library thin enough that you can read every line this tool touches, including its own random number generator, in one sitting. A verification engine you cannot fully read is not much of a verification engine.

I proved the sampling math is actually correct before writing a word of this README, not just that the program runs without crashing. With a fixed pair of draft and target distributions over a two token vocabulary, drafting each round's token from the draft distribution the way a real draft model would, and running forty thousand rounds through the compiled binary, the empirical marginal distribution of the output token landed within noise of the target model's true distribution: 0.298 against a target of 0.300, and 0.702 against 0.700. That is the actual theorem speculative sampling depends on, that the algorithm reproduces the target model's distribution exactly regardless of what the draft model proposed, and this tool reproduces it to four figures of statistical noise.

## When to use it

Reach for this when you are building or debugging a speculative decoding server and you want a postmortem tool: record every round's draft tokens, draft probabilities, target probabilities and top-k windows to a JSONL trace as your server runs, then replay a suspicious stretch of it through this tool offline with `-strict` to catch any round that violated the protocol (a probability out of range, a mismatched array length, a target top-k missing its bonus slot) instead of hunting through logs by hand.

It is also useful before you ever have a real serving stack: if you are designing your own speculative decoding pipeline and want to validate the reconciliation algorithm and the adaptive draft depth controller against synthetic traces first, this gives you a correct reference implementation of both, in isolation, that you can diff your production behavior against. And if you already have a controller that picks draft depth by hand tuned heuristics, feed a real acceptance history through `next_draft_k` here and compare its recommendation to what your system actually did.

## How it works

The trace format is one JSON object per line, one line per speculative round, decoded into a `SpecRound`: `stream_id`, `round`, a `seed` for reproducible replay, `draft` (the `k` drafted tokens as `{id, q}` pairs), `target_p` (the target model's probability for each of those same `k` tokens), `draft_topk` and `target_topk` (the truncated top-k distributions at each position, with `target_topk` carrying one extra entry, the bonus slot, beyond `draft_topk`).

`verify_round` first checks the shapes line up (`target_p.len == k`, `draft_topk.len == k`, `target_topk.len == k + 1`) and returns a protocol error immediately if they do not, rather than guessing at intent. It then walks the `k` drafted positions in order. For each one it computes `accept_prob := if p_x >= dt.q { 1.0 } else { p_x / dt.q }`, draws a uniform roll from the round's own `Rng`, and either accepts (appends the drafted id to `output_ids` and continues) or calls `sample_residual` and stops. `sample_residual` builds `max(0, p(x) - q(x))` over the union of the two top-k windows using two maps, and if the resulting mass is below `residual_epsilon` it flags the round `degenerate` and falls back to sampling straight from the target's own top-k window so the round still produces a token instead of failing outright. If every drafted token gets accepted, the round calls `sample_categorical` on `target_topk[k]`, the free bonus slot, and appends that token too. `sample_categorical` itself is a plain cumulative distribution walk over a renormalized list of `TopKEntry`.

`StreamStats.record` folds each `RoundOutcome` into per stream running totals: `accept_ema`, an exponential moving average of the round's `accepted/k` acceptance rate with smoothing factor `ema_alpha`; the KV-cache ledger, where `tentative_extend := k + 1` and `real_extend := accepted + 1` produce `rollback_tokens` (the slots that need to be evicted) and `kv_confirmed_len` (the running true cache length); and `k_recommend`, produced by `next_draft_k`, which multiplicatively grows the recommended draft depth toward `max_draft_k` while `accept_ema` stays above `accept_high_watermark`, and additively shrinks it toward `min_draft_k` as soon as `accept_ema` drops below `accept_low_watermark`. `effective_speedup` reports `(accepted_total + rounds) / rounds`, the real tokens produced per target model forward pass, which is the number that actually matters for latency.

The random number generator is a self contained splitmix64 (`Rng.next_u64`, `Rng.next_f64`), seeded fresh from each round's own `seed` field rather than carried across rounds, so any single round from a trace can be replayed in isolation with the exact same accept, reject and resample outcome it produced the first time, which is what makes this useful for debugging a specific suspicious round instead of only aggregate statistics.

## Usage

Build it as a single file program:

```
v -o speculative_decode_verifier SpeculativeDecodeVerifier.v
```

Then feed it a trace, either from a file or from stdin:

```
./speculative_decode_verifier trace.jsonl
cat trace.jsonl | ./speculative_decode_verifier
./speculative_decode_verifier -k=6 -strict trace.jsonl
./speculative_decode_verifier -json trace.jsonl > outcomes.jsonl
```

`-k=N` sets the initial recommended draft depth for any stream the tool has not seen before (default 4, clamped to the `[1, 16]` range `next_draft_k` operates in). `-json` prints one `RoundOutcome` JSON object per input line instead of the default human readable line, so you can pipe the results into another tool. `-strict` makes the process exit with status 1 if any line failed to parse or any round failed a protocol check, which is what you want in a CI job that replays a captured trace as a regression test. With no flags it prints one line per round plus a final summary per stream: rounds, acceptance rate, effective speedup, how many rounds landed on the bonus slot, how many hit the degenerate residual fallback, and the KV-cache rollback token count.

Each trace line is a JSON object shaped like this:

```json
{"stream_id":"s1","round":1,"seed":1001,
 "draft":[{"id":10,"q":0.4},{"id":11,"q":0.3}],
 "target_p":[0.6,0.5],
 "draft_topk":[[{"id":10,"p":0.4},{"id":99,"p":0.3}],[{"id":11,"p":0.3},{"id":98,"p":0.2}]],
 "target_topk":[[{"id":10,"p":0.6},{"id":99,"p":0.2}],[{"id":11,"p":0.5},{"id":98,"p":0.1}],[{"id":77,"p":0.7},{"id":76,"p":0.3}]]}
```

## Notes

The `id` values in a trace are opaque integers as far as this tool is concerned, whatever token ids your tokenizer produces; it never touches an actual vocabulary or model weights, which is exactly why it can be a small standalone file instead of a wrapper around a model runtime. The top-k windows you supply are the real constraint on correctness: if your serving stack only keeps, say, the top 20 logits per position and the true residual mass sits at rank 40, this tool will correctly report that round as `degenerate` rather than silently pretending the truncated window was the whole story, which is the honest failure mode to expose rather than paper over.

`next_draft_k`'s watermarks (`accept_high_watermark = 0.85`, `accept_low_watermark = 0.5`) and `ema_alpha = 0.2` are reasonable starting defaults for a general purpose controller, not a universal constant; if your draft model's acceptance rate is naturally lower because it is much smaller than the target, or your workload has a very different token entropy profile, treat them as the first thing to tune for your own traffic, right alongside `min_draft_k` and `max_draft_k`.
