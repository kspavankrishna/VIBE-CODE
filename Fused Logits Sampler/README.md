# Fused Logits Sampler

A single CUDA kernel that turns a batch of LLM logits into token ids with a grammar mask, temperature, top-k, top-p and min-p, and gives the same token for the same request no matter what else is in the batch. No sort, no per step host sync, no second kernel for the random draw.

**Language:** CUDA | **Lines:** 602 | **Added:** 2026-10-05

## What this solves

Sampling looks like a solved problem until you run an inference server in production and someone asks why the same prompt with the same seed gave a different answer yesterday. In most stacks the token sampler is a pile of small framework ops: a softmax, a sort over a 128k or 256k vocabulary, a cumulative sum, a mask, a multinomial draw. Each of those is a separate kernel launch, the sort is the expensive part, and the random draw usually comes from a generator that is advanced once per launch. That last detail is the real trap. If the generator state is shared across the batch, a request's random number depends on which row it landed in and which other requests were scheduled next to it. Continuous batching makes that row assignment change every step. You get the same seed and the same logits and a different token.

This file fixes that class of problem. Every row is sampled by one thread block, and the draw is a pure function of the request's own `seed` and `offset` using Philox4x32 10 rounds (`philox4x32_10`, `philox_uniform`). The batch row never enters the counter. The kernel `fused_sampler_kernel` also does the truncation without sorting. Top-k, top-p and min-p are all found as exact thresholds by bisecting over the bit pattern of the probabilities, so the cost is a fixed number of passes over the vocabulary instead of a sort.

The other thing it solves is failure visibility. A naive sampler returns a token id for anything. A fully masked grammar state, a row of NaN logits, a +inf logit or a negative temperature all produce some number and the server carries on. Here every row also writes a status byte (`Status`), and rows that cannot produce a token return -1 with a reason.

## Why I built it

I kept seeing the same three complaints in inference repos and issue trackers. First, seeded generation is not reproducible under continuous batching. Second, structured output (a JSON grammar or a tool call schema) hands the sampler a mask and nobody checks what happens when the mask ends up empty. Third, top-p implemented by full sort is a visible chunk of decode latency at large vocabularies and small batch sizes, where the GPU is otherwise idle waiting on memory.

I wanted one file I could read top to bottom in an hour, that has no dependency beyond the CUDA toolkit, and whose behaviour I could state precisely. I also wanted a self test that does not need a model: it checks the generator against the published Random123 answer, compares draws against a double precision reference, and proves the batch invariance claim directly by running the same rows alone, in a different batch size and in reverse order.

## When to use it

- You run continuous batching and need reproducible seeded sampling per request
- You decode with a grammar or schema constraint and need an explicit signal for a dead end state instead of a garbage token
- Your logits are fp16 or bf16 and you do not want to upcast the whole tensor just to sample
- Sampling shows up in your decode profile at a 128k plus vocabulary
- You want a reference to port into vLLM style or custom engine code and need the exact semantics written down
- You are debugging a determinism bug and want a sampler whose output you can reason about from the inputs alone

Do not use it if you need beam search, repetition penalties, logit bias tables or per token logprob output. Those are not here.

## How it works

Everything lives in the `fused_sampler` namespace and the whole pipeline runs inside `fused_sampler_kernel`, one block of `kThreads` threads per sequence row. `kThreads` defaults to 512 and can be overridden with the `FUSED_SAMPLER_THREADS` macro. It must be a multiple of 32 up to 1024.

**Parameters and rejection.** Each row reads a `SamplingParams` struct: `temperature`, `top_k`, `top_p`, `min_p`, `seed` and `offset`. `params_are_bad` rejects NaN, infinite or negative temperature, negative `top_k`, `top_p` outside (0, 1] and `min_p` outside [0, 1]. A rejected row writes token -1 and `kBadParams`. The host helper `validate_params` applies the same rules so you can fail early before launch.

**Masking and NaN.** `load_logit` applies the optional allowed token bitmask (bit set means allowed) and turns NaN into minus infinity. A masked or NaN token can never be chosen.

**Pass 1, argmax.** `block_argmax` finds the best logit and the lowest index among ties. If everything is minus infinity the row ends with `kNoValidToken`. If the best value is plus infinity the row returns the lowest index among the infinities with `kInfLogit`, because softmax over an infinite logit has no meaningful answer. If temperature is exactly 0 the row returns the argmax with `kGreedy`.

**Pass 2, probabilities.** Each candidate gets the unnormalised value `exp((x - best) / temperature)` written to the workspace buffer. The best token is exactly 1.0, nothing overflows and everything else is in (0, 1]. There is an explicit equality branch so that a denormal temperature, where `1 / temperature` overflows to infinity, cannot produce 0 times infinity.

**Thresholds by bit bisection.** Non negative floats sort exactly like their uint32 bit patterns, so `count_ge` and `mass_ge` count and sum the candidates at or above a bit pattern `tau`, and the kernel bisects over `tau`. That gives the exact value of the boundary token and not a tolerance based approximation, in about 30 iterations because `kOneBits` (the pattern of 1.0) is below 2^30. Three cuts are combined:

- `tau_k`: the largest threshold with at least `top_k` candidates at or above it, which is the k-th largest value
- `tau_p`: the nucleus cut. It is computed over the top-k survivors and renormalised, which matches the common top-k then top-p order. It is the largest threshold whose kept mass still reaches `top_p` times the mass of the top-k set
- `tau_m`: min-p. Because the best token is exactly 1.0, the cut is the bit pattern of `min_p` itself

The final threshold is the maximum of `tau_p` and `tau_m`. The smallest allowed bit pattern is `kMinPositiveBits`, so zero probability tokens (masked or underflowed) are never kept. Ties at a boundary are all kept. That is deliberate: it makes the kept set independent of token index order.

**The draw.** `philox_uniform(seed, offset)` gives a float in [0, 1) from 24 random bits. It is multiplied by the kept mass to get a target on the cumulative line. The kernel then walks the vocabulary in coalesced tiles of `kThreads` tokens. Each tile does a warp scan with `__shfl_up_sync`, adds the warp prefix, and stores the inclusive sums in shared memory. Thread t owns the half open interval from the previous thread's inclusive sum to its own, plus the running carry. Because the interval ends are read from the same stored values, the intervals tile the line exactly and one thread claims the target. The scan stops as soon as a tile contains the answer.

**Float rounding fallback.** If the target lands at or past the scan total, which can only happen through rounding, the row returns the last kept token and sets `kRoundingFallback`. This is rare and it is reported instead of hidden.

**Determinism.** All block reductions (`block_sum`, `block_sum_u32`, `block_max_i32`, `block_argmax`) use a fixed butterfly order, so for a given `kThreads` the sums are bitwise identical on every run. Do not change `kThreads` between a recorded run and its replay if you need bit exact replays, because summation order changes the last bits of the mass.

**Element types.** `fused_sampler_kernel` is a template. `to_float` has overloads for `float`, `__half` and `__nv_bfloat16`, so half precision logits are read in place and widened one element at a time.

**Memory.** `workspace_bytes(batch, vocab)` returns the scratch size for the probability buffer, 4 bytes per token per row. The threshold search rereads it from L2 around 60 times, which is far cheaper than recomputing `exp` each pass.

## Usage

Include the file in your own CUDA code, or build the self test directly:

```
nvcc -O3 -std=c++17 -arch=sm_80 FusedLogitsSampler.cu -o fused_sampler_selftest -DFUSED_SAMPLER_SELFTEST
./fused_sampler_selftest
```

The self test prints PASS or FAIL per check and exits with 0 only if everything passes. Its size is controlled with `FS_TEST_VOCAB`, `FS_TEST_ROWS` and `FS_TEST_ROUNDS`.

Calling it from your own code:

```cpp
#include "FusedLogitsSampler.cu"
using namespace fused_sampler;

// d_logits: float, __half or __nv_bfloat16, [batch, vocab] on the device
// d_params: SamplingParams[batch] on the device
SamplingParams p{0.8f, 50, 0.9f, 0.01f, /*seed*/ 1234, /*offset*/ step};

LaunchArgs<float> a;
a.logits = d_logits;
a.row_stride = vocab;              // elements between rows, 0 reuses one row for all sequences
a.batch = batch;
a.vocab = vocab;
a.params = d_params;
a.allowed_bits = d_mask;           // optional, allowed_words_per_row(vocab) words per row, nullptr to disable
a.workspace = d_workspace;         // workspace_bytes(batch, vocab) bytes
a.out_tokens = d_tokens;           // int32 [batch]
a.out_status = d_status;           // uint8 [batch], values of Status
cudaError_t err = launch(a, stream);
```

Increment `offset` by one for every token you sample for a request. Keep `seed` fixed per request. If the status byte is `kNoValidToken` or `kBadParams` the token is -1 and you must handle it, usually by ending the request or backing out of the grammar state.

## Notes

- I could not run this on a physical GPU while writing it. I validated the kernel logic by running the kernel body on the CPU with a thread emulator that implements `__syncthreads` and the warp shuffles, using the same self test. Run the self test on your own card before you rely on it
- Top-k, top-p and min-p are applied in that order, and the nucleus is renormalised over the top-k survivors. If your engine applies top-p on the full distribution first, results will differ for the same parameters
- Equal probability tokens at a cut boundary are all kept, so the kept set can be slightly larger than `top_k`
- Very large vocabularies cost around 60 reads of the probability buffer per row. At small batch sizes this is memory bound and still cheaper than a sort. At very large batch sizes with a small vocabulary a sort based sampler may win
- Rows are independent blocks, so a batch smaller than the number of SMs leaves the GPU partly idle. Sampling is a small part of a decode step, but keep it in mind for batch size 1
- The status byte costs one byte per row. Read it. A sampler that never says it failed is the reason these bugs take a week to find
