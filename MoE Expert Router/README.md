# MoE Expert Router

A CUDA top-k router for Mixture of Experts layers that gives the same answer on every run, on every GPU, and tells you exactly which tokens it dropped and why. One file, no atomics in the routing path, and a CPU reference that checks it.

**Language:** CUDA | **Lines:** 719 | **Added:** 2026-11-01

## What this solves

Almost every MoE router I have read has the same quiet problem. It picks the top k experts per token, then hands out slots inside each expert's capacity buffer with `atomicAdd`. That works, but the order the atomics land in changes from run to run. When an expert is over capacity, which tokens get dropped depends on thread scheduling. Same input, same weights, different dropped tokens, different loss. If you have ever chased a training run that would not reproduce bit for bit, or an inference service where the same prompt gave different answers depending on what else was in the batch, this is one of the places it hides.

There are three more problems in the same area:

- **Tail bias.** If capacity is handed out in token order, the last tokens of every sequence are always the ones that get dropped. Over thousands of steps the model sees less training signal from the end of each sequence.
- **Bad logits.** NaN or infinity in the router logits is rare but real, especially in half precision. Naive code picks a garbage expert, or turns the whole softmax into NaN and poisons the aux loss for the entire batch.
- **Silent loss accounting.** The load balancing loss and the router z loss are usually computed in a separate pass with float atomics, so they also change in the last bits between runs.

MoE Expert Router handles all of this in four small kernels plus one stats kernel. Routing output is a pure function of the logits, the config and one integer `rotate`.

## Why I built it

I wanted a router I could trust when debugging. When a loss curve diverges between two runs of the same job, the first thing I want to rule out is the router. That is hard if the router itself is nondeterministic. I also wanted something I could drop into a CUDA graph, so there is no allocation and no host sync inside `route`.

The other reason is the edge cases. Most reference code assumes clean fp32 logits. Real systems run bf16 or fp16, the logits go bad sometimes, and the answer to a bad row should be a counter you can alert on, not a crash and not a silent wrong expert.

## When to use it

- You are training or serving a MoE model and want routing that reproduces exactly across runs and across GPU models.
- You need to know how many tokens were dropped per step and how many experts overflowed, as numbers you can log.
- You want a drop policy that does not always punish the end of the sequence.
- You need a router that tolerates NaN, infinity and tied logits and reports them instead of failing.
- You are building a custom MoE layer and want a clear reference for the dispatch and combine tables.

It is not a full MoE layer. It does the routing and the bookkeeping. The expert matmuls, the gather of token activations and the weighted scatter back are yours.

## How it works

The pipeline runs on one stream in this order: `gate_kernel`, `tile_count_kernel`, `tile_scan_kernel`, `assign_kernel`, `finalize_kernel`. Buffers for dispatch and combine are first filled with `0xFF`, which is `-1` as an int, so empty means `-1` everywhere.

**gate_kernel.** One warp owns one token at a time. Lane L holds experts L, L+32, L+64 and so on, up to `kMaxExpertsPerLane` which is 8, so the router supports up to `kMaxExperts` = 256 experts and loads are coalesced. NaN becomes minus infinity, plus infinity is clamped to `FLT_MAX`. If no logit in the row is finite the token gets `kStatusNonFiniteToken` and routes nowhere. Otherwise it computes a stable softmax, then runs `top_k` rounds of warp argmax using `__shfl_xor_sync`. Ties break to the lowest expert index. Selection happens on raw logits, so which experts are chosen never depends on how `expf` rounds. If a row has fewer finite logits than `top_k`, the extra slots stay `-1` with weight zero and the token gets `kStatusShortToken`. With `renormalize` on, the selected probabilities are rescaled to sum to one.

The same kernel accumulates the full softmax mass per expert in registers, plus the squared log sum exp for the z loss. Each block reduces its four warps in a fixed order and writes one row of `partial`. The grid size comes from `gate_blocks_for`, which depends only on the token count and never on the SM count. That is why the loss values are identical on an A100 and an H100.

**tile_count_kernel.** Tokens are grouped in tiles of `kTileTokens` = 128 in logical order. A block stages its tile's expert ids in shared memory and each thread, one per expert, counts how many assignments target its expert. No atomics, so no ordering problem.

**tile_scan_kernel.** One thread per expert walks the tiles in order and writes an exclusive prefix sum into `tile_offset`, plus the total per expert into `load`.

**assign_kernel.** Same staging, but now each expert thread walks the tile in order, starting from its tile offset. The running counter is the slot position. If the position is below the capacity, the flat assignment index goes into `dispatch[e * capacity + pos]` and `combine[a]` gets `e * capacity + pos`. Otherwise `combine[a]` stays `-1` and the combine weight is set to zero. The flat index `a` is `token * top_k + slot`.

**The rotate argument.** Logical order is the physical token order shifted by `rotate`. Pass a different value each step, for example the step number, and the capacity race starts at a different token every time. The result is still fully deterministic for a given `rotate`, but over many steps no part of the sequence is systematically dropped.

**finalize_kernel.** A single block sums the partial rows in order, then computes the Switch style balance loss as `num_experts * sum(f_e * P_e)`, where `f_e` is the share of assignments sent to expert e before capacity and `P_e` is the mean softmax probability. It also writes the z loss, total assignments, dropped count, max load, overflow expert count and the number of bad and short tokens into a `Stats` struct. Bad tokens are excluded from the mean.

**Capacity.** `capacity_for` computes `ceil(capacity_factor * tokens * top_k / experts)` and rounds up to a multiple of `capacity_align`, so your expert GEMM shapes stay friendly.

**Router class.** `Router::init` validates the config with `Router::validate`, checks the 32 bit index limits and allocates everything once. It returns an empty string on success or a readable reason. `Router::route` does only async memsets and kernel launches.

The built in `main` is a self test. It runs a CPU `reference` against the GPU for fp32, fp16 and bf16, runs each case twice and compares the outputs with `memcmp`, and covers rotated starts, a skewed load that forces drops, a tiny 7 token batch, and a block of nasty rows: all NaN, only two finite logits, all tied, one infinity and all minus infinity.

## Usage

```
nvcc -O3 -std=c++17 -arch=sm_80 MoeExpertRouter.cu -o moe_router_selftest
./moe_router_selftest
```

The self test prints one PASS or FAIL line per case and exits non zero on any failure. To use the router in your own code, include the file or lift the `moe` namespace into a header:

```
moe::Config cfg;
cfg.num_tokens = 8192;
cfg.num_experts = 64;
cfg.top_k = 2;
cfg.capacity_factor = 1.25f;
cfg.capacity_align = 8;
cfg.renormalize = true;
cfg.dtype = moe::DType::kBFloat16;

moe::Router router;
std::string err = router.init(cfg);
if (!err.empty()) { /* report err */ }

// d_logits is a device pointer to [num_tokens][num_experts] in the chosen dtype
cudaError_t rc = router.route(d_logits, step, stream);

// device tensors: expert_ids(), weights(), dispatch(), combine(), load()
// copy the Stats struct from router.device_stats() when you want the counters
```

`dispatch()` is `[E][capacity]` and holds flat assignment indices, so token = index / top_k and slot = index % top_k. `combine()` is `[T][K]` and tells you where each assignment landed, or `-1` if it was dropped or empty.

## Notes

- Logits must be contiguous row major `[num_tokens][num_experts]`. Experts max out at 256 and top k at 8.
- I could not run nvcc in the environment where I wrote this, so the first thing to do is run the self test on your GPU. The kernel logic follows the CPU reference line for line, but treat a compile warning or a FAIL line as real and open an issue.
- Weights of dropped assignments are zeroed, not renormalized. If you prefer to rescale the surviving slots, do it in your combine step using `combine()`.
- The aux loss counts all `top_k` assignments, normalized to sum to one. For top 1 this is the Switch Transformer formula. For larger k it is the natural extension, not the GShard top 1 only variant.
- Tile counting is O(tile size times top k) per expert thread. That is fine for typical batches and keeps the code free of atomics. For very large expert counts with huge batches, a warp level ballot rank would be the next optimization, at the cost of more code.
- Bit stable results hold for a fixed token count, expert count, top k and dtype. Changing the token count changes the grid shape and therefore the order of the float sums in the loss, though routing decisions stay the same.
