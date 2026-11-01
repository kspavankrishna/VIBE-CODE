// MoeExpertRouter.cu
// Deterministic top-k expert routing with capacity limits for Mixture of Experts layers.
// Build: nvcc -O3 -std=c++17 -arch=sm_80 MoeExpertRouter.cu -o moe_router_selftest
// Run:   ./moe_router_selftest

#include <cuda_runtime.h>
#include <cuda_fp16.h>
#include <cuda_bf16.h>

#include <algorithm>
#include <cfloat>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace moe {

constexpr int kMaxExperts = 256;
constexpr int kMaxTopK = 8;
constexpr int kMaxExpertsPerLane = kMaxExperts / 32;
constexpr int kGateWarps = 4;
constexpr int kGateThreads = kGateWarps * 32;
constexpr int kTileTokens = 128;
constexpr int kTileThreads = 256;
constexpr int kMaxGateBlocks = 1024;
constexpr int kTokensPerGateWarpTarget = 4;

constexpr uint32_t kStatusNonFiniteToken = 1u;
constexpr uint32_t kStatusShortToken = 2u;

enum class DType : int { kFloat32 = 0, kFloat16 = 1, kBFloat16 = 2 };

struct Config {
  int num_tokens = 0;
  int num_experts = 0;
  int top_k = 2;
  float capacity_factor = 1.25f;
  int capacity_align = 8;
  bool renormalize = true;
  DType dtype = DType::kFloat32;
};

struct Stats {
  float aux_loss;
  float z_loss;
  int assignments;
  int dropped;
  int invalid_tokens;
  int short_tokens;
  int max_load;
  int overflow_experts;
};

__host__ __device__ inline float to_float(float x) { return x; }
__host__ __device__ inline float to_float(__half x) { return __half2float(x); }
__host__ __device__ inline float to_float(__nv_bfloat16 x) { return __bfloat162float(x); }

inline int capacity_for(const Config& c) {
  double raw = std::ceil(static_cast<double>(c.capacity_factor) * static_cast<double>(c.num_tokens) *
                         static_cast<double>(c.top_k) / static_cast<double>(c.num_experts));
  long long cap = static_cast<long long>(raw);
  if (cap < 1) cap = 1;
  long long a = c.capacity_align < 1 ? 1 : c.capacity_align;
  cap = ((cap + a - 1) / a) * a;
  return static_cast<int>(cap);
}

inline int gate_blocks_for(int tokens) {
  int per_block = kGateWarps * kTokensPerGateWarpTarget;
  int blocks = (tokens + per_block - 1) / per_block;
  return std::max(1, std::min(blocks, kMaxGateBlocks));
}

inline int tiles_for(int tokens) { return (tokens + kTileTokens - 1) / kTileTokens; }

// One warp owns one token at a time. Lane L holds experts L, L+32, L+64 ... so loads are coalesced.
// Selection runs on raw logits with a lowest index tie break, so the chosen experts never depend on exp rounding.
// Per warp softmax mass is kept in registers and reduced in a fixed order, so the aux loss is bit stable
// for a given token count regardless of GPU model or SM count.
template <typename T>
__global__ void gate_kernel(const T* __restrict__ logits, int num_tokens, int num_experts, int top_k,
                            bool renormalize, int32_t* __restrict__ out_ids, float* __restrict__ out_w,
                            uint32_t* __restrict__ token_status, float* __restrict__ partial) {
  __shared__ float sh[kGateWarps][kMaxExperts + 2];
  const unsigned full = 0xffffffffu;
  const int lane = threadIdx.x & 31;
  const int warp = threadIdx.x >> 5;
  const int pstride = num_experts + 2;
  const int total_warps = gridDim.x * kGateWarps;

  float acc[kMaxExpertsPerLane];
#pragma unroll
  for (int j = 0; j < kMaxExpertsPerLane; ++j) acc[j] = 0.f;
  float zacc = 0.f;
  float valid_count = 0.f;

  for (int tok = blockIdx.x * kGateWarps + warp; tok < num_tokens; tok += total_warps) {
    const T* row = logits + static_cast<size_t>(tok) * num_experts;
    float v[kMaxExpertsPerLane];
    float e[kMaxExpertsPerLane];
    bool finite_any = false;
    float lmax = -FLT_MAX;
#pragma unroll
    for (int j = 0; j < kMaxExpertsPerLane; ++j) {
      int idx = lane + 32 * j;
      float x = -INFINITY;
      if (idx < num_experts) {
        x = to_float(row[idx]);
        if (x != x) x = -INFINITY;
        else if (x == INFINITY) x = FLT_MAX;
      }
      v[j] = x;
      if (x > -INFINITY) {
        finite_any = true;
        lmax = fmaxf(lmax, x);
      }
    }
    int any_finite = __any_sync(full, finite_any);
    int32_t sel_i[kMaxTopK];
    float sel_p[kMaxTopK];
#pragma unroll
    for (int r = 0; r < kMaxTopK; ++r) {
      sel_i[r] = -1;
      sel_p[r] = 0.f;
    }
    uint32_t status = 0;
    if (!any_finite) {
      status = kStatusNonFiniteToken;
    } else {
#pragma unroll
      for (int off = 16; off > 0; off >>= 1) lmax = fmaxf(lmax, __shfl_xor_sync(full, lmax, off));
      float lsum = 0.f;
#pragma unroll
      for (int j = 0; j < kMaxExpertsPerLane; ++j) {
        e[j] = (v[j] > -INFINITY) ? expf(v[j] - lmax) : 0.f;
        lsum += e[j];
      }
#pragma unroll
      for (int off = 16; off > 0; off >>= 1) lsum += __shfl_xor_sync(full, lsum, off);
      const float inv = 1.f / lsum;

      uint32_t taken = 0;
      float psel_sum = 0.f;
#pragma unroll
      for (int r = 0; r < kMaxTopK; ++r) {
        if (r >= top_k) break;
        float bv = -INFINITY;
        int bi = INT_MAX;
#pragma unroll
        for (int j = 0; j < kMaxExpertsPerLane; ++j) {
          int idx = lane + 32 * j;
          if (idx < num_experts && !((taken >> j) & 1u) && v[j] > bv) {
            bv = v[j];
            bi = idx;
          }
        }
#pragma unroll
        for (int off = 16; off > 0; off >>= 1) {
          float ov = __shfl_xor_sync(full, bv, off);
          int oi = __shfl_xor_sync(full, bi, off);
          if (ov > bv || (ov == bv && oi < bi)) {
            bv = ov;
            bi = oi;
          }
        }
        if (bi == INT_MAX) {
          status |= kStatusShortToken;
          break;
        }
        float pv = 0.f;
        if (lane == (bi & 31)) {
          const int jj = bi >> 5;
          taken |= 1u << jj;
#pragma unroll
          for (int j = 0; j < kMaxExpertsPerLane; ++j)
            if (j == jj) pv = e[j] * inv;
        }
        pv = __shfl_sync(full, pv, bi & 31);
        sel_i[r] = bi;
        sel_p[r] = pv;
        psel_sum += pv;
      }
      if (renormalize && psel_sum > 0.f) {
        const float rn = 1.f / psel_sum;
#pragma unroll
        for (int r = 0; r < kMaxTopK; ++r) sel_p[r] *= rn;
      }
#pragma unroll
      for (int j = 0; j < kMaxExpertsPerLane; ++j) acc[j] += e[j] * inv;
      const float lse = lmax + logf(lsum);
      zacc += lse * lse;
      valid_count += 1.f;
    }
    if (lane == 0) {
      const size_t base = static_cast<size_t>(tok) * top_k;
#pragma unroll
      for (int r = 0; r < kMaxTopK; ++r) {
        if (r >= top_k) break;
        out_ids[base + r] = sel_i[r];
        out_w[base + r] = sel_p[r];
      }
      token_status[tok] = status;
    }
  }

#pragma unroll
  for (int j = 0; j < kMaxExpertsPerLane; ++j) {
    int idx = lane + 32 * j;
    if (idx < num_experts) sh[warp][idx] = acc[j];
  }
  if (lane == 0) {
    sh[warp][num_experts] = valid_count;
    sh[warp][num_experts + 1] = zacc;
  }
  __syncthreads();
  for (int i = threadIdx.x; i < pstride; i += blockDim.x) {
    float s = 0.f;
#pragma unroll
    for (int w = 0; w < kGateWarps; ++w) s += sh[w][i];
    partial[static_cast<size_t>(blockIdx.x) * pstride + i] = s;
  }
}

__device__ inline int physical_token(int logical, int rot, int num_tokens) {
  int t = logical + rot;
  return t >= num_tokens ? t - num_tokens : t;
}

__device__ inline void stage_tile(const int32_t* ids, int num_tokens, int top_k, int rot, int tile,
                                  int32_t* s_ids) {
  const int l0 = tile * kTileTokens;
  for (int i = threadIdx.x; i < kTileTokens * top_k; i += blockDim.x) {
    int lt = i / top_k;
    int j = i - lt * top_k;
    int l = l0 + lt;
    int32_t id = -1;
    if (l < num_tokens) id = ids[static_cast<size_t>(physical_token(l, rot, num_tokens)) * top_k + j];
    s_ids[i] = id;
  }
}

// Thread e counts how many assignments in this tile target expert e. The scan order is the logical token
// order, so no atomics are involved and the result is identical on every run.
__global__ void tile_count_kernel(const int32_t* __restrict__ ids, int num_tokens, int num_experts, int top_k,
                                  int rot, int32_t* __restrict__ tile_count) {
  __shared__ int32_t s_ids[kTileTokens * kMaxTopK];
  const int tile = blockIdx.x;
  stage_tile(ids, num_tokens, top_k, rot, tile, s_ids);
  __syncthreads();
  const int e = threadIdx.x;
  if (e >= num_experts) return;
  int c = 0;
  const int n = kTileTokens * top_k;
  for (int i = 0; i < n; ++i) c += (s_ids[i] == e);
  tile_count[static_cast<size_t>(tile) * num_experts + e] = c;
}

__global__ void tile_scan_kernel(const int32_t* __restrict__ tile_count, int num_tiles, int num_experts,
                                 int32_t* __restrict__ tile_offset, int32_t* __restrict__ load) {
  int e = blockIdx.x * blockDim.x + threadIdx.x;
  if (e >= num_experts) return;
  int running = 0;
  for (int t = 0; t < num_tiles; ++t) {
    size_t at = static_cast<size_t>(t) * num_experts + e;
    tile_offset[at] = running;
    running += tile_count[at];
  }
  load[e] = running;
}

__global__ void assign_kernel(const int32_t* __restrict__ ids, int num_tokens, int num_experts, int top_k,
                              int rot, int capacity, const int32_t* __restrict__ tile_offset,
                              int32_t* __restrict__ dispatch, int32_t* __restrict__ combine,
                              float* __restrict__ weights) {
  __shared__ int32_t s_ids[kTileTokens * kMaxTopK];
  const int tile = blockIdx.x;
  stage_tile(ids, num_tokens, top_k, rot, tile, s_ids);
  __syncthreads();
  const int e = threadIdx.x;
  if (e >= num_experts) return;
  int run = tile_offset[static_cast<size_t>(tile) * num_experts + e];
  const int l0 = tile * kTileTokens;
  const int n = kTileTokens * top_k;
  for (int i = 0; i < n; ++i) {
    if (s_ids[i] != e) continue;
    int lt = i / top_k;
    int j = i - lt * top_k;
    int t = physical_token(l0 + lt, rot, num_tokens);
    size_t a = static_cast<size_t>(t) * top_k + j;
    int pos = run++;
    if (pos < capacity) {
      dispatch[static_cast<size_t>(e) * capacity + pos] = static_cast<int32_t>(a);
      combine[a] = e * capacity + pos;
    } else {
      combine[a] = -1;
      weights[a] = 0.f;
    }
  }
}

__global__ void finalize_kernel(const int32_t* __restrict__ load, const float* __restrict__ partial,
                                const uint32_t* __restrict__ token_status, int num_tokens, int num_experts,
                                int gate_blocks, int capacity, Stats* __restrict__ stats) {
  __shared__ float s_term[kMaxExperts];
  __shared__ int s_load[kMaxExperts];
  __shared__ float s_valid;
  __shared__ float s_z;
  __shared__ int s_bad[2];
  __shared__ int s_cnt[kTileThreads][2];
  const int pstride = num_experts + 2;
  const int e = threadIdx.x;
  float psum = 0.f;
  if (e < num_experts) {
    for (int b = 0; b < gate_blocks; ++b) psum += partial[static_cast<size_t>(b) * pstride + e];
    s_load[e] = load[e];
  } else {
    s_load[e] = 0;
  }
  if (e == 0) {
    float valid = 0.f, z = 0.f;
    for (int b = 0; b < gate_blocks; ++b) {
      valid += partial[static_cast<size_t>(b) * pstride + num_experts];
      z += partial[static_cast<size_t>(b) * pstride + num_experts + 1];
    }
    s_valid = valid;
    s_z = z;
  }
  {
    int bad = 0, shorty = 0;
    for (int t = e; t < num_tokens; t += blockDim.x) {
      uint32_t st = token_status[t];
      bad += (st & kStatusNonFiniteToken) ? 1 : 0;
      shorty += (st & kStatusShortToken) ? 1 : 0;
    }
    s_cnt[e][0] = bad;
    s_cnt[e][1] = shorty;
  }
  __syncthreads();
  if (e < num_experts) {
    int total = 0;
    for (int i = 0; i < num_experts; ++i) total += s_load[i];
    float f = total > 0 ? static_cast<float>(s_load[e]) / static_cast<float>(total) : 0.f;
    float p = s_valid > 0.f ? psum / s_valid : 0.f;
    s_term[e] = f * p;
  }
  __syncthreads();
  if (e == 0) {
    int bad = 0, shorty = 0;
    for (int i = 0; i < blockDim.x; ++i) { bad += s_cnt[i][0]; shorty += s_cnt[i][1]; }
    s_bad[0] = bad;
    s_bad[1] = shorty;
    float dot = 0.f;
    int total = 0, dropped = 0, maxl = 0, over = 0;
    for (int i = 0; i < num_experts; ++i) {
      dot += s_term[i];
      total += s_load[i];
      int d = s_load[i] - capacity;
      if (d > 0) {
        dropped += d;
        ++over;
      }
      maxl = max(maxl, s_load[i]);
    }
    Stats s;
    s.aux_loss = static_cast<float>(num_experts) * dot;
    s.z_loss = s_valid > 0.f ? s_z / s_valid : 0.f;
    s.assignments = total;
    s.dropped = dropped;
    s.invalid_tokens = s_bad[0];
    s.short_tokens = s_bad[1];
    s.max_load = maxl;
    s.overflow_experts = over;
    *stats = s;
  }
}

template <typename T>
void launch_gate(const void* logits, const Config& c, int gate_blocks, int32_t* ids, float* w,
                 uint32_t* status, float* partial, cudaStream_t s) {
  gate_kernel<T><<<gate_blocks, kGateThreads, 0, s>>>(static_cast<const T*>(logits), c.num_tokens,
                                                      c.num_experts, c.top_k, c.renormalize, ids, w,
                                                      status, partial);
}

// Owns every device buffer, allocated once. route() performs no allocation and no host sync, so it can be
// captured in a CUDA graph. Output tensors stay valid until the next route() call on the same router.
class Router {
 public:
  Router() = default;
  Router(const Router&) = delete;
  Router& operator=(const Router&) = delete;
  ~Router() { release(); }

  // Returns an empty string on success, otherwise a readable reason.
  std::string init(const Config& cfg) {
    release();
    std::string err = validate(cfg);
    if (!err.empty()) return err;
    cfg_ = cfg;
    capacity_ = capacity_for(cfg);
    gate_blocks_ = gate_blocks_for(cfg.num_tokens);
    tiles_ = tiles_for(cfg.num_tokens);
    const size_t T = cfg.num_tokens, E = cfg.num_experts, K = cfg.top_k;
    if (!alloc(&ids_, T * K * sizeof(int32_t)) || !alloc(&weights_, T * K * sizeof(float)) ||
        !alloc(&status_, T * sizeof(uint32_t)) ||
        !alloc(&partial_, static_cast<size_t>(gate_blocks_) * (E + 2) * sizeof(float)) ||
        !alloc(&tile_count_, static_cast<size_t>(tiles_) * E * sizeof(int32_t)) ||
        !alloc(&tile_offset_, static_cast<size_t>(tiles_) * E * sizeof(int32_t)) ||
        !alloc(&load_, E * sizeof(int32_t)) ||
        !alloc(&dispatch_, E * static_cast<size_t>(capacity_) * sizeof(int32_t)) ||
        !alloc(&combine_, T * K * sizeof(int32_t)) || !alloc(&stats_, sizeof(Stats))) {
      std::string msg = std::string("cudaMalloc failed: ") + cudaGetErrorString(last_);
      release();
      return msg;
    }
    ready_ = true;
    return std::string();
  }

  static std::string validate(const Config& c) {
    if (c.num_tokens < 1) return "num_tokens must be at least 1";
    if (c.num_experts < 1 || c.num_experts > kMaxExperts) return "num_experts must be in 1..256";
    if (c.top_k < 1 || c.top_k > kMaxTopK) return "top_k must be in 1..8";
    if (c.top_k > c.num_experts) return "top_k cannot exceed num_experts";
    if (!(c.capacity_factor > 0.f) || !std::isfinite(c.capacity_factor)) return "capacity_factor must be positive and finite";
    if (c.capacity_align < 1) return "capacity_align must be at least 1";
    if (static_cast<long long>(c.num_tokens) * c.top_k > INT_MAX / 2) return "num_tokens * top_k is too large for 32 bit indices";
    long long cap = static_cast<long long>(std::ceil(static_cast<double>(c.capacity_factor) * c.num_tokens * c.top_k / c.num_experts)) + c.capacity_align;
    if (cap * c.num_experts > INT_MAX / 2) return "capacity * num_experts is too large for 32 bit indices";
    return std::string();
  }

  // rotate shifts which token counts as first in the capacity race. Pass a different value each step
  // (for example step % num_tokens) so the tail of every sequence is not the one that always gets dropped.
  cudaError_t route(const void* logits, int rotate, cudaStream_t stream = 0) {
    if (!ready_) return cudaErrorNotReady;
    const int rot = ((rotate % cfg_.num_tokens) + cfg_.num_tokens) % cfg_.num_tokens;
    const size_t T = cfg_.num_tokens, K = cfg_.top_k;
    cudaError_t err;
    if ((err = cudaMemsetAsync(dispatch_, 0xFF, static_cast<size_t>(cfg_.num_experts) * capacity_ * sizeof(int32_t), stream)) != cudaSuccess) return err;
    if ((err = cudaMemsetAsync(combine_, 0xFF, T * K * sizeof(int32_t), stream)) != cudaSuccess) return err;
    switch (cfg_.dtype) {
      case DType::kFloat32: launch_gate<float>(logits, cfg_, gate_blocks_, ids_, weights_, status_, partial_, stream); break;
      case DType::kFloat16: launch_gate<__half>(logits, cfg_, gate_blocks_, ids_, weights_, status_, partial_, stream); break;
      case DType::kBFloat16: launch_gate<__nv_bfloat16>(logits, cfg_, gate_blocks_, ids_, weights_, status_, partial_, stream); break;
    }
    if ((err = cudaGetLastError()) != cudaSuccess) return err;
    tile_count_kernel<<<tiles_, kTileThreads, 0, stream>>>(ids_, cfg_.num_tokens, cfg_.num_experts, cfg_.top_k, rot, tile_count_);
    if ((err = cudaGetLastError()) != cudaSuccess) return err;
    tile_scan_kernel<<<(cfg_.num_experts + 127) / 128, 128, 0, stream>>>(tile_count_, tiles_, cfg_.num_experts, tile_offset_, load_);
    if ((err = cudaGetLastError()) != cudaSuccess) return err;
    assign_kernel<<<tiles_, kTileThreads, 0, stream>>>(ids_, cfg_.num_tokens, cfg_.num_experts, cfg_.top_k, rot, capacity_, tile_offset_, dispatch_, combine_, weights_);
    if ((err = cudaGetLastError()) != cudaSuccess) return err;
    finalize_kernel<<<1, kTileThreads, 0, stream>>>(load_, partial_, status_, cfg_.num_tokens, cfg_.num_experts, gate_blocks_, capacity_, stats_);
    return cudaGetLastError();
  }

  int capacity() const { return capacity_; }
  const Config& config() const { return cfg_; }
  // [T][K] expert id per slot, -1 when the slot is empty.
  const int32_t* expert_ids() const { return ids_; }
  // [T][K] combine weight, 0 for empty or dropped slots.
  const float* weights() const { return weights_; }
  // [E][capacity] flat assignment index (token * K + slot) that fills each expert row, -1 when unused.
  const int32_t* dispatch() const { return dispatch_; }
  // [T][K] expert * capacity + position, or -1 when dropped or empty.
  const int32_t* combine() const { return combine_; }
  // [E] pre capacity assignment count per expert.
  const int32_t* load() const { return load_; }
  const Stats* device_stats() const { return stats_; }

 private:
  template <typename P>
  bool alloc(P** p, size_t bytes) {
    last_ = cudaMalloc(reinterpret_cast<void**>(p), bytes > 0 ? bytes : 1);
    if (last_ != cudaSuccess) { *p = nullptr; return false; }
    return true;
  }
  template <typename P>
  void free_one(P*& p) { if (p) cudaFree(p); p = nullptr; }
  void release() {
    free_one(ids_); free_one(weights_); free_one(status_); free_one(partial_); free_one(tile_count_);
    free_one(tile_offset_); free_one(load_); free_one(dispatch_); free_one(combine_); free_one(stats_);
    ready_ = false;
  }

  Config cfg_{};
  int capacity_ = 0, gate_blocks_ = 0, tiles_ = 0;
  bool ready_ = false;
  cudaError_t last_ = cudaSuccess;
  int32_t* ids_ = nullptr; float* weights_ = nullptr; uint32_t* status_ = nullptr; float* partial_ = nullptr;
  int32_t* tile_count_ = nullptr; int32_t* tile_offset_ = nullptr; int32_t* load_ = nullptr;
  int32_t* dispatch_ = nullptr; int32_t* combine_ = nullptr; Stats* stats_ = nullptr;
};

}  // namespace moe

// ---------------------------------------------------------------------------------------------------------
// Self test: CPU reference, determinism check, edge cases (NaN rows, ties, infinities, tiny inputs).
// ---------------------------------------------------------------------------------------------------------

namespace {

#define CHECK_CUDA(x)                                                                          \
  do {                                                                                         \
    cudaError_t _e = (x);                                                                      \
    if (_e != cudaSuccess) {                                                                   \
      std::fprintf(stderr, "CUDA error %s at %s:%d\n", cudaGetErrorString(_e), __FILE__, __LINE__); \
      return false;                                                                            \
    }                                                                                          \
  } while (0)

struct Ref {
  std::vector<int32_t> ids, dispatch, combine, load;
  std::vector<float> weights;
  double aux = 0, z = 0;
  int dropped = 0, invalid = 0, shorty = 0;
};

Ref reference(const std::vector<float>& logits, const moe::Config& c, int rotate) {
  const int T = c.num_tokens, E = c.num_experts, K = c.top_k;
  const int cap = moe::capacity_for(c);
  Ref r;
  r.ids.assign(static_cast<size_t>(T) * K, -1);
  r.weights.assign(static_cast<size_t>(T) * K, 0.f);
  r.combine.assign(static_cast<size_t>(T) * K, -1);
  r.dispatch.assign(static_cast<size_t>(E) * cap, -1);
  r.load.assign(E, 0);
  std::vector<double> psum(E, 0.0);
  double valid = 0, z = 0;
  for (int t = 0; t < T; ++t) {
    std::vector<float> v(E);
    bool any = false;
    for (int i = 0; i < E; ++i) {
      float x = logits[static_cast<size_t>(t) * E + i];
      if (x != x) x = -INFINITY;
      else if (x == INFINITY) x = FLT_MAX;
      v[i] = x;
      any = any || x > -INFINITY;
    }
    if (!any) { ++r.invalid; continue; }
    float m = -FLT_MAX;
    for (float x : v) if (x > -INFINITY) m = std::max(m, x);
    double s = 0;
    std::vector<double> p(E, 0.0);
    for (int i = 0; i < E; ++i) if (v[i] > -INFINITY) { p[i] = std::exp(static_cast<double>(v[i]) - m); s += p[i]; }
    for (int i = 0; i < E; ++i) { p[i] /= s; psum[i] += p[i]; }
    z += (m + std::log(s)) * (m + std::log(s));
    valid += 1;
    std::vector<bool> taken(E, false);
    double sel = 0;
    std::vector<double> sp(K, 0.0);
    for (int k = 0; k < K; ++k) {
      int bi = -1;
      for (int i = 0; i < E; ++i) if (!taken[i] && v[i] > -INFINITY && (bi < 0 || v[i] > v[bi])) bi = i;
      if (bi < 0) { ++r.shorty; break; }
      taken[bi] = true;
      r.ids[static_cast<size_t>(t) * K + k] = bi;
      sp[k] = p[bi];
      sel += p[bi];
    }
    for (int k = 0; k < K; ++k) r.weights[static_cast<size_t>(t) * K + k] = static_cast<float>(c.renormalize && sel > 0 ? sp[k] / sel : sp[k]);
  }
  std::vector<int> run(E, 0);
  for (int l = 0; l < T; ++l) {
    int t = (l + rotate) % T;
    for (int k = 0; k < K; ++k) {
      size_t a = static_cast<size_t>(t) * K + k;
      int e = r.ids[a];
      if (e < 0) continue;
      int pos = run[e]++;
      if (pos < cap) { r.dispatch[static_cast<size_t>(e) * cap + pos] = static_cast<int32_t>(a); r.combine[a] = e * cap + pos; }
      else { r.combine[a] = -1; r.weights[a] = 0.f; }
    }
  }
  int total = 0;
  for (int e = 0; e < E; ++e) { r.load[e] = run[e]; total += run[e]; r.dropped += std::max(0, run[e] - cap); }
  double dot = 0;
  for (int e = 0; e < E; ++e) dot += (total ? static_cast<double>(run[e]) / total : 0.0) * (valid > 0 ? psum[e] / valid : 0.0);
  r.aux = E * dot;
  r.z = valid > 0 ? z / valid : 0.0;
  return r;
}

template <typename H>
std::vector<H> convert(const std::vector<float>& in);
template <> std::vector<float> convert<float>(const std::vector<float>& in) { return in; }
template <> std::vector<__half> convert<__half>(const std::vector<float>& in) {
  std::vector<__half> o(in.size()); for (size_t i = 0; i < in.size(); ++i) o[i] = __float2half(in[i]); return o;
}
template <> std::vector<__nv_bfloat16> convert<__nv_bfloat16>(const std::vector<float>& in) {
  std::vector<__nv_bfloat16> o(in.size()); for (size_t i = 0; i < in.size(); ++i) o[i] = __float2bfloat16(in[i]); return o;
}
inline float roundtrip(float x, moe::DType d) {
  if (d == moe::DType::kFloat16) return __half2float(__float2half(x));
  if (d == moe::DType::kBFloat16) return __bfloat162float(__float2bfloat16(x));
  return x;
}

template <typename H>
bool run_case(const char* name, moe::Config c, int rotate, const std::vector<float>& raw) {
  std::vector<float> logits(raw.size());
  for (size_t i = 0; i < raw.size(); ++i) logits[i] = roundtrip(raw[i], c.dtype);
  std::vector<H> host = convert<H>(raw);
  H* d_in = nullptr;
  CHECK_CUDA(cudaMalloc(&d_in, host.size() * sizeof(H)));
  CHECK_CUDA(cudaMemcpy(d_in, host.data(), host.size() * sizeof(H), cudaMemcpyHostToDevice));
  moe::Router router;
  std::string err = router.init(c);
  if (!err.empty()) { std::fprintf(stderr, "%s: init failed: %s\n", name, err.c_str()); return false; }
  const size_t TK = static_cast<size_t>(c.num_tokens) * c.top_k;
  const size_t DE = static_cast<size_t>(c.num_experts) * router.capacity();

  std::vector<int32_t> ids1(TK), disp1(DE), comb1(TK);
  std::vector<float> w1(TK);
  moe::Stats st1{};
  std::vector<int32_t> ids2(TK), disp2(DE), comb2(TK);
  std::vector<float> w2(TK);
  moe::Stats st2{};
  for (int pass = 0; pass < 2; ++pass) {
    CHECK_CUDA(router.route(d_in, rotate));
    CHECK_CUDA(cudaDeviceSynchronize());
    auto& ids = pass ? ids2 : ids1; auto& disp = pass ? disp2 : disp1; auto& comb = pass ? comb2 : comb1;
    auto& w = pass ? w2 : w1; auto& st = pass ? st2 : st1;
    CHECK_CUDA(cudaMemcpy(ids.data(), router.expert_ids(), TK * 4, cudaMemcpyDeviceToHost));
    CHECK_CUDA(cudaMemcpy(disp.data(), router.dispatch(), DE * 4, cudaMemcpyDeviceToHost));
    CHECK_CUDA(cudaMemcpy(comb.data(), router.combine(), TK * 4, cudaMemcpyDeviceToHost));
    CHECK_CUDA(cudaMemcpy(w.data(), router.weights(), TK * 4, cudaMemcpyDeviceToHost));
    CHECK_CUDA(cudaMemcpy(&st, router.device_stats(), sizeof(st), cudaMemcpyDeviceToHost));
  }
  cudaFree(d_in);

  bool ok = true;
  if (std::memcmp(ids1.data(), ids2.data(), TK * 4) || std::memcmp(disp1.data(), disp2.data(), DE * 4) ||
      std::memcmp(comb1.data(), comb2.data(), TK * 4) || std::memcmp(w1.data(), w2.data(), TK * 4) ||
      std::memcmp(&st1, &st2, sizeof(st1))) {
    std::fprintf(stderr, "%s: two runs differ bit for bit\n", name);
    ok = false;
  }
  Ref ref = reference(logits, c, rotate);
  if (ids1 != ref.ids) { std::fprintf(stderr, "%s: expert ids differ from reference\n", name); ok = false; }
  if (disp1 != ref.dispatch) { std::fprintf(stderr, "%s: dispatch table differs from reference\n", name); ok = false; }
  if (comb1 != ref.combine) { std::fprintf(stderr, "%s: combine table differs from reference\n", name); ok = false; }
  float worst = 0;
  for (size_t i = 0; i < TK; ++i) worst = std::max(worst, std::fabs(w1[i] - ref.weights[i]));
  if (worst > 2e-5f) { std::fprintf(stderr, "%s: weight error %.3g\n", name, worst); ok = false; }
  if (st1.dropped != ref.dropped || st1.invalid_tokens != ref.invalid || st1.short_tokens != ref.shorty) {
    std::fprintf(stderr, "%s: counters differ (dropped %d vs %d)\n", name, st1.dropped, ref.dropped);
    ok = false;
  }
  if (std::fabs(st1.aux_loss - ref.aux) > 1e-3 * std::max(1.0, std::fabs(ref.aux))) {
    std::fprintf(stderr, "%s: aux loss %.6f vs %.6f\n", name, st1.aux_loss, ref.aux);
    ok = false;
  }
  if (std::fabs(st1.z_loss - ref.z) > 1e-3 * std::max(1.0, std::fabs(ref.z))) {
    std::fprintf(stderr, "%s: z loss %.6f vs %.6f\n", name, st1.z_loss, ref.z);
    ok = false;
  }
  std::printf("%-34s %s  cap=%d dropped=%d aux=%.4f z=%.3f invalid=%d\n", name, ok ? "PASS" : "FAIL",
              router.capacity(), st1.dropped, st1.aux_loss, st1.z_loss, st1.invalid_tokens);
  return ok;
}

std::vector<float> make_logits(int T, int E, uint32_t seed, float skew) {
  std::mt19937 g(seed);
  std::normal_distribution<float> n(0.f, 1.f);
  std::vector<float> v(static_cast<size_t>(T) * E);
  for (int t = 0; t < T; ++t)
    for (int e = 0; e < E; ++e) v[static_cast<size_t>(t) * E + e] = n(g) + (e < 4 ? skew : 0.f);
  return v;
}

}  // namespace

int main() {
  int dev = 0;
  if (cudaGetDeviceCount(&dev) != cudaSuccess || dev == 0) {
    std::fprintf(stderr, "no CUDA device available\n");
    return 2;
  }
  bool ok = true;
  {
    moe::Config c; c.num_tokens = 1000; c.num_experts = 64; c.top_k = 2;
    ok &= run_case<float>("fp32 balanced 1000x64 k2", c, 0, make_logits(1000, 64, 1, 0.f));
  }
  {
    moe::Config c; c.num_tokens = 4097; c.num_experts = 256; c.top_k = 8; c.capacity_factor = 1.0f;
    ok &= run_case<float>("fp32 4097x256 k8 cf1.0", c, 17, make_logits(4097, 256, 2, 0.7f));
  }
  {
    moe::Config c; c.num_tokens = 2048; c.num_experts = 16; c.top_k = 2; c.capacity_factor = 1.0f; c.dtype = moe::DType::kBFloat16;
    ok &= run_case<__nv_bfloat16>("bf16 skewed 2048x16 rot", c, 999, make_logits(2048, 16, 3, 2.5f));
  }
  {
    moe::Config c; c.num_tokens = 513; c.num_experts = 40; c.top_k = 4; c.renormalize = false; c.dtype = moe::DType::kFloat16;
    ok &= run_case<__half>("fp16 no renorm 513x40 k4", c, -5, make_logits(513, 40, 4, 0.f));
  }
  {
    moe::Config c; c.num_tokens = 7; c.num_experts = 8; c.top_k = 1; c.capacity_align = 1;
    ok &= run_case<float>("tiny 7x8 k1", c, 3, make_logits(7, 8, 5, 0.f));
  }
  {
    moe::Config c; c.num_tokens = 300; c.num_experts = 33; c.top_k = 3;
    std::vector<float> v = make_logits(300, 33, 6, 0.f);
    for (int e = 0; e < 33; ++e) v[5 * 33 + e] = NAN;
    for (int e = 0; e < 33; ++e) v[6 * 33 + e] = (e < 2) ? 1.f : -INFINITY;
    for (int e = 0; e < 33; ++e) v[7 * 33 + e] = 0.5f;
    v[8 * 33 + 9] = INFINITY;
    for (int e = 0; e < 33; ++e) v[9 * 33 + e] = -INFINITY;
    ok &= run_case<float>("nan inf ties short rows", c, 11, v);
  }
  std::printf("%s\n", ok ? "ALL PASS" : "FAILURES");
  return ok ? 0 : 1;
}
