// FusedLogitsSampler.cu
// Batch invariant, single kernel LLM token sampler: grammar mask, temperature, top-k, top-p and min-p.
// Build: nvcc -O3 -std=c++17 -arch=sm_80 FusedLogitsSampler.cu -o fused_sampler_selftest -DFUSED_SAMPLER_SELFTEST

#include <cuda_runtime.h>
#include <cuda_fp16.h>
#include <cuda_bf16.h>

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <random>
#include <vector>

#ifndef FUSED_SAMPLER_THREADS
#define FUSED_SAMPLER_THREADS 512
#endif

namespace fused_sampler {

constexpr int kThreads = FUSED_SAMPLER_THREADS;
constexpr int kWarps = kThreads / 32;
static_assert(kThreads % 32 == 0 && kThreads >= 32 && kThreads <= 1024, "kThreads must be a multiple of 32 in [32, 1024]");
static_assert(kWarps <= 32, "second stage reductions assume at most 32 warps");

constexpr uint32_t kFullMask = 0xffffffffu;
constexpr uint32_t kOneBits = 0x3f800000u;   // bit pattern of 1.0f, the largest probability we ever see
constexpr uint32_t kMinPositiveBits = 1u;    // smallest positive denormal, anything below is "not a candidate"

enum Status : uint8_t {
  kSampled = 0,          // normal draw from the truncated distribution
  kGreedy = 1,           // temperature == 0, returned the argmax
  kNoValidToken = 2,     // every token was masked or NaN, token id is -1
  kInfLogit = 3,         // a +inf logit was present, returned the lowest index among them
  kRoundingFallback = 4, // float rounding left the target past the scan total, returned the last kept token
  kBadParams = 5         // NaN, negative or out of range sampling parameters, token id is -1
};

struct SamplingParams {
  float temperature;   // 0 means greedy. Must be finite and >= 0.
  int32_t top_k;       // 0 disables. Must be >= 0.
  float top_p;         // 1 disables. Must be in (0, 1].
  float min_p;         // 0 disables. Must be in [0, 1]. Relative to the most likely token.
  uint64_t seed;       // per request seed. Never mix the batch row into this.
  uint64_t offset;     // per request draw counter. Increment once per sampled token.
};

struct Philox4x32 {
  uint32_t v[4];
};

__host__ __device__ inline uint32_t mulhi32(uint32_t a, uint32_t b) {
  return static_cast<uint32_t>((static_cast<uint64_t>(a) * static_cast<uint64_t>(b)) >> 32);
}

// Philox4x32 with 10 rounds, the same generator cuRAND and PyTorch use. Counter based, so a draw is a pure
// function of (seed, offset) and never depends on which batch row or launch it landed in.
__host__ __device__ inline Philox4x32 philox4x32_10(uint64_t seed, uint64_t offset) {
  uint32_t c0 = static_cast<uint32_t>(offset), c1 = static_cast<uint32_t>(offset >> 32), c2 = 0u, c3 = 0u;
  uint32_t k0 = static_cast<uint32_t>(seed), k1 = static_cast<uint32_t>(seed >> 32);
  for (int r = 0; r < 10; ++r) {
    const uint32_t hi0 = mulhi32(0xD2511F53u, c0), lo0 = 0xD2511F53u * c0;
    const uint32_t hi1 = mulhi32(0xCD9E8D57u, c2), lo1 = 0xCD9E8D57u * c2;
    c0 = hi1 ^ c1 ^ k0;
    c1 = lo1;
    c2 = hi0 ^ c3 ^ k1;
    c3 = lo0;
    k0 += 0x9E3779B9u;
    k1 += 0xBB67AE85u;
  }
  return Philox4x32{{c0, c1, c2, c3}};
}

// Uniform float in [0, 1). 24 random bits, so 1.0 can never be produced.
__host__ __device__ inline float philox_uniform(uint64_t seed, uint64_t offset) {
  const Philox4x32 r = philox4x32_10(seed, offset);
  return static_cast<float>(r.v[0] >> 8) * (1.0f / 16777216.0f);
}

__device__ inline float to_float(float x) { return x; }
__device__ inline float to_float(__half x) { return __half2float(x); }
__device__ inline float to_float(__nv_bfloat16 x) { return __bfloat162float(x); }

// ---------------------------------------------------------------------------------------------------------------
// Block wide reductions. Fixed butterfly order, so the result is bitwise identical for a given blockDim and every
// thread gets the same value back. Every thread of the block must call these.
// ---------------------------------------------------------------------------------------------------------------

__device__ inline float block_sum(float v, float* sh) {
  for (int off = 16; off > 0; off >>= 1) v += __shfl_xor_sync(kFullMask, v, off);
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  if (lane == 0) sh[warp] = v;
  __syncthreads();
  float r = lane < kWarps ? sh[lane] : 0.0f;
  for (int off = 16; off > 0; off >>= 1) r += __shfl_xor_sync(kFullMask, r, off);
  __syncthreads();
  return r;
}

__device__ inline uint32_t block_sum_u32(uint32_t v, uint32_t* sh) {
  for (int off = 16; off > 0; off >>= 1) v += __shfl_xor_sync(kFullMask, v, off);
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  if (lane == 0) sh[warp] = v;
  __syncthreads();
  uint32_t r = lane < kWarps ? sh[lane] : 0u;
  for (int off = 16; off > 0; off >>= 1) r += __shfl_xor_sync(kFullMask, r, off);
  __syncthreads();
  return r;
}

__device__ inline int block_max_i32(int v, int* sh) {
  for (int off = 16; off > 0; off >>= 1) v = max(v, __shfl_xor_sync(kFullMask, v, off));
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  if (lane == 0) sh[warp] = v;
  __syncthreads();
  int r = lane < kWarps ? sh[lane] : INT_MIN;
  for (int off = 16; off > 0; off >>= 1) r = max(r, __shfl_xor_sync(kFullMask, r, off));
  __syncthreads();
  return r;
}

// Larger value wins, equal values resolve to the lower index. Symmetric, so the butterfly agrees on every lane.
__device__ inline bool better(float av, int ai, float bv, int bi) { return av > bv || (av == bv && ai < bi); }

__device__ inline void block_argmax(float& v, int& idx, float* shv, int* shi) {
  for (int off = 16; off > 0; off >>= 1) {
    const float ov = __shfl_xor_sync(kFullMask, v, off);
    const int oi = __shfl_xor_sync(kFullMask, idx, off);
    if (better(ov, oi, v, idx)) { v = ov; idx = oi; }
  }
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  if (lane == 0) { shv[warp] = v; shi[warp] = idx; }
  __syncthreads();
  v = lane < kWarps ? shv[lane] : -INFINITY;
  idx = lane < kWarps ? shi[lane] : INT_MAX;
  for (int off = 16; off > 0; off >>= 1) {
    const float ov = __shfl_xor_sync(kFullMask, v, off);
    const int oi = __shfl_xor_sync(kFullMask, idx, off);
    if (better(ov, oi, v, idx)) { v = ov; idx = oi; }
  }
  __syncthreads();
}

// Candidate probabilities are stored as unnormalised exp values in (0, 1]. Non negative floats order exactly like
// their bit patterns, so thresholds are searched over uint32 and the answer is an exact element value, not an
// approximation with a tolerance.
__device__ inline uint32_t count_ge(const float* ex, int vocab, uint32_t tau, uint32_t* sh) {
  uint32_t c = 0;
  for (int i = threadIdx.x; i < vocab; i += kThreads) c += (__float_as_uint(ex[i]) >= tau) ? 1u : 0u;
  return block_sum_u32(c, sh);
}

__device__ inline float mass_ge(const float* ex, int vocab, uint32_t tau, float* sh) {
  float m = 0.0f;
  for (int i = threadIdx.x; i < vocab; i += kThreads) {
    const float e = ex[i];
    m += (__float_as_uint(e) >= tau) ? e : 0.0f;
  }
  return block_sum(m, sh);
}

__device__ inline bool params_are_bad(const SamplingParams& p) {
  return !(p.temperature >= 0.0f) || isinf(p.temperature) || p.top_k < 0 || !(p.top_p > 0.0f && p.top_p <= 1.0f) ||
         !(p.min_p >= 0.0f && p.min_p <= 1.0f);
}

template <typename T>
__device__ inline float load_logit(const T* row, const uint32_t* mask, int i) {
  if (mask != nullptr && ((mask[i >> 5] >> (i & 31)) & 1u) == 0u) return -INFINITY;
  const float x = to_float(row[i]);
  return isnan(x) ? -INFINITY : x;
}

// One block per sequence. The whole pipeline below is a function of that row's logits, mask, params, seed and
// offset only, which is what makes the result batch invariant.
template <typename T>
__global__ void __launch_bounds__(kThreads)
fused_sampler_kernel(const T* __restrict__ logits, long long row_stride, int vocab,
                     const SamplingParams* __restrict__ params, const uint32_t* __restrict__ allowed,
                     long long allowed_stride_words, float* __restrict__ workspace, int32_t* __restrict__ out_tokens,
                     uint8_t* __restrict__ out_status) {
  __shared__ float s_f[32];
  __shared__ uint32_t s_u[32];
  __shared__ int s_i[32];
  __shared__ float s_incl[kThreads];
  __shared__ int s_chosen;

  const int row = blockIdx.x;
  const SamplingParams p = params[row];
  const T* lg = logits + static_cast<long long>(row) * row_stride;
  const uint32_t* mask = allowed ? allowed + static_cast<long long>(row) * allowed_stride_words : nullptr;
  float* ex = workspace + static_cast<long long>(row) * vocab;

  if (params_are_bad(p)) {
    if (threadIdx.x == 0) { out_tokens[row] = -1; out_status[row] = kBadParams; }
    return;
  }

  // Pass 1: masked argmax. NaN counts as -inf, ties go to the lowest index.
  float best = -INFINITY;
  int best_idx = INT_MAX;
  for (int i = threadIdx.x; i < vocab; i += kThreads) {
    const float x = load_logit(lg, mask, i);
    if (x > best) { best = x; best_idx = i; }
  }
  block_argmax(best, best_idx, s_f, s_i);

  if (best == -INFINITY) {
    if (threadIdx.x == 0) { out_tokens[row] = -1; out_status[row] = kNoValidToken; }
    return;
  }
  if (isinf(best) || p.temperature == 0.0f) {
    if (threadIdx.x == 0) {
      out_tokens[row] = best_idx;
      out_status[row] = isinf(best) ? kInfLogit : kGreedy;
    }
    return;
  }

  // Pass 2: unnormalised probabilities relative to the max, so the best token is exactly 1.0 and nothing overflows.
  // The explicit equality branch avoids 0 * inf when 1 / temperature overflows for denormal temperatures.
  const float inv_t = 1.0f / p.temperature;
  for (int i = threadIdx.x; i < vocab; i += kThreads) {
    const float x = load_logit(lg, mask, i);
    float e;
    if (x == -INFINITY) e = 0.0f;
    else if (x == best) e = 1.0f;
    else e = expf((x - best) * inv_t);
    ex[i] = e;
  }
  __syncthreads();

  // Threshold search. tau_k: largest tau with at least k candidates at or above it, i.e. the k-th largest value.
  // Ties at the boundary are kept, which keeps the result independent of index order.
  uint32_t tau_k = kMinPositiveBits;
  if (p.top_k > 0 && p.top_k < vocab) {
    uint32_t lo = kMinPositiveBits, hi = kOneBits;
    const uint32_t k = static_cast<uint32_t>(p.top_k);
    while (lo < hi) {
      const uint32_t mid = lo + (hi - lo + 1u) / 2u;
      if (count_ge(ex, vocab, mid, s_u) >= k) lo = mid; else hi = mid - 1u;
    }
    tau_k = lo;
  }

  // tau_p: nucleus over the top-k survivors, renormalised. Largest tau whose kept mass still reaches p times the
  // mass of the top-k set. mass_ge is monotone non increasing in tau, so bisection is exact.
  uint32_t tau_p = tau_k;
  if (p.top_p < 1.0f) {
    const float mass_k = mass_ge(ex, vocab, tau_k, s_f);
    const float target = p.top_p * mass_k;
    uint32_t lo = tau_k, hi = kOneBits;
    while (lo < hi) {
      const uint32_t mid = lo + (hi - lo + 1u) / 2u;
      if (mass_ge(ex, vocab, mid, s_f) >= target) lo = mid; else hi = mid - 1u;
    }
    tau_p = lo;
  }

  // min-p is relative to the best token, whose value is exactly 1.0, so the cut is min_p itself.
  uint32_t tau_m = kMinPositiveBits;
  if (p.min_p > 0.0f) tau_m = max(kMinPositiveBits, __float_as_uint(p.min_p));

  const uint32_t tau = max(tau_p, tau_m);
  const float total = mass_ge(ex, vocab, tau, s_f);
  const float target = philox_uniform(p.seed, p.offset) * total;

  // Inverse CDF over coalesced tiles. Each tile does a block scan. Thread t owns the half open interval
  // [prefix before t, prefix through t) built from the same stored values, so the intervals partition the line
  // exactly and one thread claims the target.
  if (threadIdx.x == 0) s_chosen = -1;
  __syncthreads();
  float carry = 0.0f;
  int last_kept = -1;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  for (int base = 0; base < vocab; base += kThreads) {
    const int i = base + threadIdx.x;
    float v = 0.0f;
    if (i < vocab) {
      const float e = ex[i];
      if (__float_as_uint(e) >= tau) { v = e; last_kept = i; }
    }
    float incl = v;
    for (int off = 1; off < 32; off <<= 1) {
      const float n = __shfl_up_sync(kFullMask, incl, off);
      if (lane >= off) incl += n;
    }
    if (lane == 31) s_f[warp] = incl;
    __syncthreads();
    float wprefix = 0.0f;
    for (int w = 0; w < warp; ++w) wprefix += s_f[w];
    incl += wprefix;
    s_incl[threadIdx.x] = incl;
    __syncthreads();
    const float lo = threadIdx.x > 0 ? s_incl[threadIdx.x - 1] : 0.0f;
    const float cum_lo = carry + lo, cum_hi = carry + incl;
    if (v > 0.0f && target >= cum_lo && target < cum_hi) s_chosen = i;
    carry = carry + s_incl[kThreads - 1];
    __syncthreads();
    if (s_chosen >= 0) break;
  }

  int chosen = s_chosen;
  uint8_t status = kSampled;
  if (chosen < 0) {
    // target landed at or beyond the scan total through float rounding. Take the last kept token.
    chosen = block_max_i32(last_kept, s_i);
    status = kRoundingFallback;
  }
  if (threadIdx.x == 0) { out_tokens[row] = chosen; out_status[row] = status; }
}

// ---------------------------------------------------------------------------------------------------------------
// Host API
// ---------------------------------------------------------------------------------------------------------------

inline size_t workspace_bytes(int batch, int vocab) {
  return static_cast<size_t>(batch) * static_cast<size_t>(vocab) * sizeof(float);
}

inline size_t allowed_words_per_row(int vocab) { return (static_cast<size_t>(vocab) + 31u) / 32u; }

// Returns nullptr when the parameters are usable, otherwise a static message. The kernel also rejects bad rows on
// its own with kBadParams, this is for failing early on the host before a launch.
inline const char* validate_params(const SamplingParams& p) {
  if (!(p.temperature >= 0.0f) || std::isinf(p.temperature)) return "temperature must be finite and >= 0";
  if (p.top_k < 0) return "top_k must be >= 0";
  if (!(p.top_p > 0.0f && p.top_p <= 1.0f)) return "top_p must be in (0, 1]";
  if (!(p.min_p >= 0.0f && p.min_p <= 1.0f)) return "min_p must be in [0, 1]";
  return nullptr;
}

template <typename T>
struct LaunchArgs {
  const T* logits = nullptr;            // device, [batch, vocab] with row_stride elements between rows
  long long row_stride = 0;             // elements between rows. 0 is allowed and reuses one row for every sequence.
  int batch = 0;
  int vocab = 0;
  const SamplingParams* params = nullptr;   // device, [batch]
  const uint32_t* allowed_bits = nullptr;   // optional device bitmask [batch, allowed_words_per_row(vocab)], bit set = allowed
  long long allowed_stride_words = 0;       // 0 means allowed_words_per_row(vocab)
  float* workspace = nullptr;           // device, workspace_bytes(batch, vocab)
  int32_t* out_tokens = nullptr;        // device, [batch]
  uint8_t* out_status = nullptr;        // device, [batch], values of Status
};

template <typename T>
inline cudaError_t launch(const LaunchArgs<T>& a, cudaStream_t stream = nullptr) {
  if (a.batch <= 0 || a.vocab <= 0 || a.row_stride < 0 || a.allowed_stride_words < 0) return cudaErrorInvalidValue;
  if (a.row_stride != 0 && a.row_stride < a.vocab) return cudaErrorInvalidValue;
  if (!a.logits || !a.params || !a.workspace || !a.out_tokens || !a.out_status) return cudaErrorInvalidValue;
  const long long aw = a.allowed_stride_words ? a.allowed_stride_words : static_cast<long long>(allowed_words_per_row(a.vocab));
  const T* logits = a.logits;
  long long row_stride = a.row_stride;
  int vocab = a.vocab;
  const SamplingParams* params = a.params;
  const uint32_t* allowed = a.allowed_bits;
  float* ws = a.workspace;
  int32_t* tokens = a.out_tokens;
  uint8_t* status = a.out_status;
  void* args[] = {&logits, &row_stride, &vocab, &params, &allowed, const_cast<long long*>(&aw), &ws, &tokens, &status};
  cudaError_t err = cudaLaunchKernel(reinterpret_cast<const void*>(&fused_sampler_kernel<T>), dim3(a.batch),
                                     dim3(kThreads), args, 0, stream);
  return err != cudaSuccess ? err : cudaGetLastError();
}

}  // namespace fused_sampler

// ===================================================================================================================
// Self test. Build with -DFUSED_SAMPLER_SELFTEST and run on a GPU. Exit code 0 means every check passed.
// ===================================================================================================================
#ifdef FUSED_SAMPLER_SELFTEST

#ifndef FS_TEST_ROWS
#define FS_TEST_ROWS 2048
#endif
#ifndef FS_TEST_VOCAB
#define FS_TEST_VOCAB 1000
#endif
#ifndef FS_TEST_ROUNDS
#define FS_TEST_ROUNDS 60
#endif

namespace {

using namespace fused_sampler;

int g_failures = 0;

void check(bool ok, const char* what) {
  std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", what);
  if (!ok) ++g_failures;
}

#define CU(x) do { cudaError_t e_ = (x); if (e_ != cudaSuccess) { std::printf("CUDA error %s at line %d\n", cudaGetErrorString(e_), __LINE__); std::exit(2); } } while (0)

struct Device {
  float* logits = nullptr;
  SamplingParams* params = nullptr;
  uint32_t* allowed = nullptr;
  float* ws = nullptr;
  int32_t* tokens = nullptr;
  uint8_t* status = nullptr;
};

struct Result {
  std::vector<int32_t> tokens;
  std::vector<uint8_t> status;
};

Result run(const std::vector<float>& logits, long long row_stride, int batch, int vocab,
           const std::vector<SamplingParams>& params, const std::vector<uint32_t>* allowed) {
  Device d;
  CU(cudaMalloc(&d.logits, logits.size() * sizeof(float)));
  CU(cudaMalloc(&d.params, params.size() * sizeof(SamplingParams)));
  CU(cudaMalloc(&d.ws, workspace_bytes(batch, vocab)));
  CU(cudaMalloc(&d.tokens, batch * sizeof(int32_t)));
  CU(cudaMalloc(&d.status, batch));
  CU(cudaMemcpy(d.logits, logits.data(), logits.size() * sizeof(float), cudaMemcpyHostToDevice));
  CU(cudaMemcpy(d.params, params.data(), params.size() * sizeof(SamplingParams), cudaMemcpyHostToDevice));
  if (allowed) {
    CU(cudaMalloc(&d.allowed, allowed->size() * sizeof(uint32_t)));
    CU(cudaMemcpy(d.allowed, allowed->data(), allowed->size() * sizeof(uint32_t), cudaMemcpyHostToDevice));
  }
  LaunchArgs<float> a;
  a.logits = d.logits; a.row_stride = row_stride; a.batch = batch; a.vocab = vocab; a.params = d.params;
  a.allowed_bits = d.allowed; a.workspace = d.ws; a.out_tokens = d.tokens; a.out_status = d.status;
  CU(launch(a));
  CU(cudaDeviceSynchronize());
  Result r;
  r.tokens.resize(batch);
  r.status.resize(batch);
  CU(cudaMemcpy(r.tokens.data(), d.tokens, batch * sizeof(int32_t), cudaMemcpyDeviceToHost));
  CU(cudaMemcpy(r.status.data(), d.status, batch, cudaMemcpyDeviceToHost));
  cudaFree(d.logits); cudaFree(d.params); cudaFree(d.ws); cudaFree(d.tokens); cudaFree(d.status);
  if (d.allowed) cudaFree(d.allowed);
  return r;
}

// Double precision reference for the kept set and its renormalised distribution.
std::vector<double> reference_distribution(const std::vector<float>& lg, float temp, int top_k, double top_p, double min_p) {
  const int V = static_cast<int>(lg.size());
  std::vector<int> order(V);
  for (int i = 0; i < V; ++i) order[i] = i;
  std::sort(order.begin(), order.end(), [&](int a, int b) { return lg[a] > lg[b] || (lg[a] == lg[b] && a < b); });
  const double mx = lg[order[0]];
  std::vector<double> e(V);
  for (int i = 0; i < V; ++i) e[i] = std::exp((lg[i] - mx) / temp);
  const int kk = (top_k > 0 && top_k < V) ? top_k : V;
  std::vector<char> keep(V, 0);
  double mass_k = 0;
  for (int r = 0; r < kk; ++r) { keep[order[r]] = 1; mass_k += e[order[r]]; }
  if (top_p < 1.0) {
    double cum = 0;
    int cut = kk;
    for (int r = 0; r < kk; ++r) {
      cum += e[order[r]];
      if (cum >= top_p * mass_k) { cut = r + 1; break; }
    }
    for (int r = cut; r < kk; ++r) keep[order[r]] = 0;
  }
  double mass = 0;
  for (int i = 0; i < V; ++i) {
    if (keep[i] && e[i] < min_p) keep[i] = 0;
    if (keep[i]) mass += e[i];
  }
  std::vector<double> pr(V, 0.0);
  for (int i = 0; i < V; ++i) if (keep[i]) pr[i] = e[i] / mass;
  return pr;
}

void test_philox_known_answer() {
  // Random123 known answer for counter 0 and key 0.
  const Philox4x32 r = philox4x32_10(0, 0);
  check(r.v[0] == 0x6627e8d5u && r.v[1] == 0xe169c58du && r.v[2] == 0xbc57ac4cu && r.v[3] == 0x9b00dbd8u,
        "philox4x32-10 matches the Random123 known answer");
  bool in_range = true;
  for (uint64_t i = 0; i < 100000; ++i) {
    const float u = philox_uniform(i * 7919u, i);
    if (!(u >= 0.0f && u < 1.0f)) in_range = false;
  }
  check(in_range, "philox_uniform stays in [0, 1)");
}

void test_distribution_and_invariance() {
  const int V = FS_TEST_VOCAB, R = FS_TEST_ROWS, N = FS_TEST_ROUNDS;
  std::mt19937 rng(1234);
  std::normal_distribution<float> nd(0.0f, 2.0f);
  std::vector<float> lg(V);
  for (auto& x : lg) x = nd(rng);

  const float temp = 0.8f;
  const int top_k = std::min(50, V / 4);
  const float top_p = 0.9f, min_p = 0.01f;
  std::vector<int> counts(V, 0);
  std::vector<int32_t> first_round;
  long long total = 0;
  bool all_sampled = true;
  const std::vector<double> ref = reference_distribution(lg, temp, top_k, top_p, min_p);
  const std::vector<double> ref_wide = reference_distribution(lg, temp, top_k, top_p * 1.001, min_p * 0.999);
  bool subset = true;
  for (int round = 0; round < N; ++round) {
    std::vector<SamplingParams> ps(R);
    for (int r = 0; r < R; ++r) ps[r] = SamplingParams{temp, top_k, top_p, min_p, 1000u + static_cast<uint64_t>(r), static_cast<uint64_t>(round)};
    const Result res = run(lg, 0, R, V, ps, nullptr);
    if (round == 0) first_round = res.tokens;
    for (int r = 0; r < R; ++r) {
      const int t = res.tokens[r];
      if (t < 0 || t >= V || res.status[r] == kBadParams) { all_sampled = false; continue; }
      if (res.status[r] == kRoundingFallback) continue;
      if (ref_wide[t] == 0.0) subset = false;
      ++counts[t];
      ++total;
    }
  }
  check(all_sampled, "every row produced a valid token");
  check(subset, "every sampled token lies inside the reference top-k, top-p and min-p set");

  double worst = 0;
  for (int t = 0; t < V; ++t) {
    if (ref[t] * total < 30) continue;
    const double emp = static_cast<double>(counts[t]) / total;
    const double sigma = std::sqrt(ref[t] * (1 - ref[t]) / total);
    worst = std::max(worst, std::fabs(emp - ref[t]) / sigma);
  }
  std::printf("       worst z over well populated tokens: %.2f on %lld draws\n", worst, total);
  check(worst < 6.0, "empirical frequencies match the reference distribution within 6 sigma");

  // Batch invariance: same (logits, seed, offset) must give the same token alone, in a small batch and reversed.
  const int S = std::min(24, R);
  std::vector<SamplingParams> sub(S);
  for (int r = 0; r < S; ++r) sub[r] = SamplingParams{temp, top_k, top_p, min_p, 1000u + static_cast<uint64_t>(r), 0u};
  const Result small = run(lg, 0, S, V, sub, nullptr);
  bool same_small = true;
  for (int r = 0; r < S; ++r) same_small = same_small && small.tokens[r] == first_round[r];
  check(same_small, "tokens are identical in a small batch and a large batch");

  std::vector<SamplingParams> rev(sub.rbegin(), sub.rend());
  const Result rr = run(lg, 0, S, V, rev, nullptr);
  bool same_rev = true;
  for (int r = 0; r < S; ++r) same_rev = same_rev && rr.tokens[S - 1 - r] == first_round[r];
  check(same_rev, "tokens are identical when the batch order is reversed");

  const Result again = run(lg, 0, S, V, sub, nullptr);
  check(again.tokens == small.tokens, "repeat launch is bitwise reproducible");
}

void test_edge_cases() {
  const int V = 777, B = 9;
  std::mt19937 rng(99);
  std::normal_distribution<float> nd(0.0f, 1.5f);
  std::vector<float> lg(static_cast<size_t>(B) * V);
  for (auto& x : lg) x = nd(rng);
  auto argmax_of = [&](int row) {
    int best = 0;
    for (int i = 1; i < V; ++i) if (lg[static_cast<size_t>(row) * V + i] > lg[static_cast<size_t>(row) * V + best]) best = i;
    return best;
  };
  for (int i = 0; i < V; ++i) lg[4 * V + i] = std::numeric_limits<float>::quiet_NaN();
  lg[4 * V + 10] = 3.0f;
  lg[5 * V + 20] = INFINITY;
  lg[5 * V + 30] = INFINITY;

  std::vector<SamplingParams> ps(B, SamplingParams{1.0f, 0, 1.0f, 0.0f, 5u, 0u});
  ps[0].temperature = 0.0f;                 // greedy
  ps[3].temperature = -1.0f;                // bad params
  ps[6].top_k = 1;                          // top-k of one is greedy
  ps[7].top_p = 1e-6f;                      // tiny nucleus keeps only the best token
  ps[8].min_p = 1.0f;                       // min-p of one keeps only the best token

  const size_t W = allowed_words_per_row(V);
  std::vector<uint32_t> mask(static_cast<size_t>(B) * W, 0xffffffffu);
  for (size_t w = 0; w < W; ++w) { mask[1 * W + w] = 0; mask[2 * W + w] = 0; }
  mask[1 * W + 0] = (1u << 3) | (1u << 7);  // row 1 may only emit token 3 or 7. Row 2 has nothing allowed.

  const Result r = run(lg, V, B, V, ps, &mask);
  check(r.tokens[0] == argmax_of(0) && r.status[0] == kGreedy, "temperature 0 returns the argmax");
  check(r.tokens[1] == 3 || r.tokens[1] == 7, "grammar mask restricts the draw to allowed tokens");
  check(r.tokens[2] == -1 && r.status[2] == kNoValidToken, "fully masked row reports kNoValidToken");
  check(r.tokens[3] == -1 && r.status[3] == kBadParams, "negative temperature reports kBadParams");
  check(r.tokens[4] == 10, "NaN logits are ignored and the only finite token wins");
  check(r.tokens[5] == 20 && r.status[5] == kInfLogit, "+inf logit returns the lowest index among infinities");
  check(r.tokens[6] == argmax_of(6), "top_k 1 returns the argmax");
  check(r.tokens[7] == argmax_of(7), "tiny top_p returns the argmax");
  check(r.tokens[8] == argmax_of(8), "min_p 1 returns the argmax");
}

}  // namespace

int main() {
  test_philox_known_answer();
  test_distribution_and_invariance();
  test_edge_cases();
  std::printf("%s\n", g_failures == 0 ? "ALL CHECKS PASSED" : "SELF TEST FAILED");
  return g_failures == 0 ? 0 : 1;
}

#endif  // FUSED_SAMPLER_SELFTEST
