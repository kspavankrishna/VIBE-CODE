// PagedKvCacheAllocator - block-based KV-cache memory manager for LLM inference
// servers, with content-addressed prefix sharing (automatic prefix caching),
// copy-on-write sequence forking, and LRU eviction of unreferenced blocks.
//
// Build & run the demo:
//   g++ -std=c++17 -O2 -pthread PagedKvCacheAllocator.cpp -o kvcache_demo
//   ./kvcache_demo

#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <list>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvcache {

using BlockId = int32_t;
using SeqId = int64_t;
constexpr BlockId kInvalidBlock = -1;

namespace detail {

// FNV-1a over the raw token ids in a single block.
inline uint64_t HashTokens(const std::vector<int32_t>& tokens) {
  uint64_t h = 1469598103934665603ULL;  // FNV offset basis
  for (int32_t t : tokens) {
    h ^= static_cast<uint64_t>(static_cast<uint32_t>(t));
    h *= 1099511628211ULL;  // FNV prime
  }
  return h;
}

// boost::hash_combine-style mix. Chaining each block's hash with the previous
// block's hash is what makes this a radix/prefix hash rather than a plain
// per-block content hash: two sequences only collide on block N if every
// block before it, in order, was byte-identical.
inline uint64_t ChainHash(uint64_t prev, uint64_t block_hash) {
  prev ^= block_hash + 0x9e3779b97f4a7c15ULL + (prev << 6) + (prev >> 2);
  return prev;
}

}  // namespace detail

// A single physical KV-cache block (block_size_ tokens worth of K/V slots on
// the accelerator; the tensor storage itself is out of scope for this file -
// callers map BlockId -> device buffer however their runtime does that).
struct Block {
  uint32_t ref_count = 0;
  bool has_hash = false;
  uint64_t content_hash = 0;
};

struct AllocatorStats {
  size_t total_blocks = 0;
  size_t free_blocks = 0;
  size_t cached_evictable_blocks = 0;
  size_t active_blocks = 0;
  uint64_t prefix_cache_hits = 0;
  uint64_t prefix_cache_misses = 0;
  uint64_t evictions = 0;

  double HitRate() const {
    uint64_t total = prefix_cache_hits + prefix_cache_misses;
    return total == 0 ? 0.0 : static_cast<double>(prefix_cache_hits) / total;
  }
};

// PagedKvCacheAllocator manages a fixed pool of `num_blocks` fixed-size
// blocks. Sequences append tokens one at a time; once a run of tokens fills
// a block, that block becomes immutable and content-addressed, so a second
// sequence that produced the exact same prefix (a shared system prompt, a
// few-shot preamble, a common tool schema, the common branch of a beam
// search) reuses the same physical block instead of paying for a fresh copy.
// Blocks that fall to zero references are not freed immediately: they sit in
// an LRU list so a third sequence hitting the same prefix moments later
// still gets the cache hit. Only when the pool is fully exhausted does
// allocation evict the least-recently-unreferenced cached block.
//
// Thread-safety: every public method takes a single mutex. That is a
// deliberate simplification, not an oversight - correctness of the
// ref-counting and hash-chaining invariants matters far more than lock
// granularity at the scale (a few thousand blocks, microsecond-scale calls)
// this class targets. Shard by block-pool if profiling ever shows
// contention here.
class PagedKvCacheAllocator {
 public:
  PagedKvCacheAllocator(size_t num_blocks, size_t block_size_tokens)
      : block_size_(block_size_tokens), blocks_(num_blocks) {
    if (block_size_tokens == 0) {
      throw std::invalid_argument("block_size_tokens must be > 0");
    }
    if (num_blocks == 0) {
      throw std::invalid_argument("num_blocks must be > 0");
    }
    free_list_.reserve(num_blocks);
    for (size_t i = 0; i < num_blocks; ++i) {
      free_list_.push_back(static_cast<BlockId>(i));
    }
  }

  PagedKvCacheAllocator(const PagedKvCacheAllocator&) = delete;
  PagedKvCacheAllocator& operator=(const PagedKvCacheAllocator&) = delete;

  SeqId CreateSequence() {
    std::lock_guard<std::mutex> lock(mu_);
    SeqId id = next_seq_id_++;
    sequences_.emplace(id, Sequence{});
    return id;
  }

  // Appends tokens to a sequence, allocating and interning blocks as they
  // fill. Returns false if the pool is exhausted (free list and LRU cache
  // both empty) and no further block could be allocated; the sequence is
  // left in a consistent, already-appended-up-to-the-failure-point state so
  // the caller can retry after freeing other sequences.
  bool AppendTokens(SeqId seq_id, const std::vector<int32_t>& new_tokens) {
    std::lock_guard<std::mutex> lock(mu_);
    Sequence& seq = GetSeqOrThrow(seq_id);
    for (int32_t tok : new_tokens) {
      if (seq.last_block_full) {
        BlockId nb = AllocateBlock();
        if (nb == kInvalidBlock) return false;
        blocks_[nb].ref_count = 1;
        seq.block_table.push_back(nb);
        seq.partial_tokens.clear();
        seq.last_block_full = false;
      }
      seq.partial_tokens.push_back(tok);
      if (seq.partial_tokens.size() == block_size_) {
        InternLastBlock(seq);
        seq.last_block_full = true;
      }
    }
    return true;
  }

  // Forks a sequence (parallel sampling / beam search branch points). All
  // completed, content-addressed blocks are shared via a simple refcount
  // bump - true copy-on-write, no data movement. The trailing partial block
  // (if any) is still privately mutable, so it is physically duplicated:
  // sharing it would let the parent and child overwrite each other's
  // in-progress tokens in what is supposed to be independent state.
  SeqId ForkSequence(SeqId parent_id) {
    std::lock_guard<std::mutex> lock(mu_);
    Sequence& parent = GetSeqOrThrow(parent_id);
    Sequence child;
    child.block_table.reserve(parent.block_table.size());

    for (size_t i = 0; i < parent.block_table.size(); ++i) {
      bool is_open_partial =
          !parent.last_block_full && i + 1 == parent.block_table.size();
      if (is_open_partial) {
        BlockId copy = AllocateBlock();
        if (copy == kInvalidBlock) {
          for (BlockId b : child.block_table) DecRef(b);
          throw std::runtime_error(
              "PagedKvCacheAllocator: out of blocks while forking sequence");
        }
        blocks_[copy].ref_count = 1;
        child.block_table.push_back(copy);
      } else {
        BlockId shared = parent.block_table[i];
        TouchForReuse(shared);
        child.block_table.push_back(shared);
      }
    }

    child.partial_tokens = parent.partial_tokens;
    child.prev_hash = parent.prev_hash;
    child.last_block_full = parent.last_block_full;

    SeqId child_id = next_seq_id_++;
    sequences_.emplace(child_id, std::move(child));
    return child_id;
  }

  void FreeSequence(SeqId seq_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = sequences_.find(seq_id);
    if (it == sequences_.end()) return;
    for (BlockId b : it->second.block_table) DecRef(b);
    sequences_.erase(it);
  }

  std::vector<BlockId> GetBlockTable(SeqId seq_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    return GetSeqOrThrow(seq_id).block_table;
  }

  size_t NumCachedTokens(SeqId seq_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const Sequence& seq = GetSeqOrThrow(seq_id);
    size_t full_blocks = seq.block_table.size() - (seq.last_block_full ? 0 : 1);
    return full_blocks * block_size_ + seq.partial_tokens.size();
  }

  AllocatorStats GetStats() const {
    std::lock_guard<std::mutex> lock(mu_);
    AllocatorStats s;
    s.total_blocks = blocks_.size();
    s.free_blocks = free_list_.size();
    s.cached_evictable_blocks = lru_.size();
    s.active_blocks = s.total_blocks - s.free_blocks - s.cached_evictable_blocks;
    s.prefix_cache_hits = prefix_cache_hits_;
    s.prefix_cache_misses = prefix_cache_misses_;
    s.evictions = evictions_;
    return s;
  }

 private:
  struct Sequence {
    std::vector<BlockId> block_table;
    std::vector<int32_t> partial_tokens;
    uint64_t prev_hash = 0;
    // true == no open partial block; the next appended token must allocate
    // a fresh one. Starts true so the very first token allocates block 0.
    bool last_block_full = true;
  };

  Sequence& GetSeqOrThrow(SeqId id) {
    auto it = sequences_.find(id);
    if (it == sequences_.end()) throw std::out_of_range("unknown sequence id");
    return it->second;
  }
  const Sequence& GetSeqOrThrow(SeqId id) const {
    auto it = sequences_.find(id);
    if (it == sequences_.end()) throw std::out_of_range("unknown sequence id");
    return it->second;
  }

  // Pops a physical block from the free list, or - if the pool is fully
  // committed - reclaims the least-recently-unreferenced cached block. A
  // reclaimed block's old identity is scrubbed from the hash index before
  // it is handed back out, so a stale prefix can never be "hit" again after
  // its backing block was repurposed.
  BlockId AllocateBlock() {
    if (!free_list_.empty()) {
      BlockId id = free_list_.back();
      free_list_.pop_back();
      return id;
    }
    if (!lru_.empty()) {
      BlockId victim = lru_.front();
      lru_.pop_front();
      lru_pos_.erase(victim);
      Block& b = blocks_[victim];
      if (b.has_hash) hash_to_block_.erase(b.content_hash);
      b.has_hash = false;
      b.content_hash = 0;
      b.ref_count = 0;
      ++evictions_;
      return victim;
    }
    return kInvalidBlock;
  }

  // Increments a block's refcount, pulling it out of the LRU eviction list
  // first if it had fallen to zero references. Safe to call on a block that
  // is already actively referenced (the fork path does this unconditionally
  // for every shared block).
  void TouchForReuse(BlockId id) {
    Block& b = blocks_[id];
    if (b.ref_count == 0) {
      auto it = lru_pos_.find(id);
      if (it != lru_pos_.end()) {
        lru_.erase(it->second);
        lru_pos_.erase(it);
      }
    }
    ++b.ref_count;
  }

  void DecRef(BlockId id) {
    Block& b = blocks_[id];
    if (b.ref_count == 0) {
      throw std::logic_error("PagedKvCacheAllocator: double free of block " +
                              std::to_string(id));
    }
    if (--b.ref_count == 0) {
      if (b.has_hash) {
        auto it = lru_.insert(lru_.end(), id);
        lru_pos_[id] = it;
      } else {
        free_list_.push_back(id);
      }
    }
  }

  // Called exactly once, the instant a sequence's trailing block fills up.
  // Computes the chained content hash and either merges into an existing
  // cached block (prefix hit) or promotes the freshly-filled private block
  // into the cache (prefix miss, first writer wins).
  void InternLastBlock(Sequence& seq) {
    uint64_t block_hash =
        detail::ChainHash(seq.prev_hash, detail::HashTokens(seq.partial_tokens));
    BlockId private_block = seq.block_table.back();

    auto it = hash_to_block_.find(block_hash);
    if (it != hash_to_block_.end() && it->second != private_block) {
      BlockId shared = it->second;
      TouchForReuse(shared);
      seq.block_table.back() = shared;
      DecRef(private_block);
      ++prefix_cache_hits_;
    } else {
      blocks_[private_block].has_hash = true;
      blocks_[private_block].content_hash = block_hash;
      hash_to_block_[block_hash] = private_block;
      ++prefix_cache_misses_;
    }

    seq.prev_hash = block_hash;
    seq.partial_tokens.clear();
  }

  size_t block_size_;
  std::vector<Block> blocks_;
  std::vector<BlockId> free_list_;
  std::unordered_map<uint64_t, BlockId> hash_to_block_;
  std::list<BlockId> lru_;
  std::unordered_map<BlockId, std::list<BlockId>::iterator> lru_pos_;
  std::unordered_map<SeqId, Sequence> sequences_;
  SeqId next_seq_id_ = 1;
  uint64_t prefix_cache_hits_ = 0;
  uint64_t prefix_cache_misses_ = 0;
  uint64_t evictions_ = 0;
  mutable std::mutex mu_;
};

}  // namespace kvcache

// --------------------------------------------------------------------------
// Demo / smoke test. Simulates three requests sharing a long system prompt,
// a beam-search fork, and eviction under a deliberately small pool.
// --------------------------------------------------------------------------
namespace {

std::vector<int32_t> Range(int32_t start, int32_t count) {
  std::vector<int32_t> v(count);
  for (int32_t i = 0; i < count; ++i) v[i] = start + i;
  return v;
}

void PrintStats(const char* label, const kvcache::AllocatorStats& s) {
  std::cout << label << ": total=" << s.total_blocks
            << " free=" << s.free_blocks << " cached=" << s.cached_evictable_blocks
            << " active=" << s.active_blocks << " hits=" << s.prefix_cache_hits
            << " misses=" << s.prefix_cache_misses << " evictions=" << s.evictions
            << " hit_rate=" << std::fixed << std::setprecision(2)
            << (s.HitRate() * 100.0) << "%\n";
}

}  // namespace

int main() {
  using namespace kvcache;
  constexpr size_t kBlockSize = 8;

  {
    std::cout << "== Prefix sharing across independent requests ==\n";
    PagedKvCacheAllocator alloc(/*num_blocks=*/64, kBlockSize);
    std::vector<int32_t> shared_system_prompt = Range(100, 32);  // 4 blocks

    SeqId a = alloc.CreateSequence();
    alloc.AppendTokens(a, shared_system_prompt);
    alloc.AppendTokens(a, Range(1, 5));  // request-specific tail, partial block

    SeqId b = alloc.CreateSequence();
    alloc.AppendTokens(b, shared_system_prompt);  // identical prefix -> reuse
    alloc.AppendTokens(b, Range(2000, 3));

    SeqId c = alloc.CreateSequence();
    alloc.AppendTokens(c, Range(999, 32));  // different prompt entirely -> misses

    PrintStats("after a, b, c", alloc.GetStats());
    std::cout << "cached tokens for a: " << alloc.NumCachedTokens(a) << "\n";
    std::cout << "cached tokens for b: " << alloc.NumCachedTokens(b) << "\n";

    std::cout << "\n== Copy-on-write fork (beam search branch) ==\n";
    SeqId beam2 = alloc.ForkSequence(a);
    alloc.AppendTokens(a, Range(1, 3));      // parent continues one way
    alloc.AppendTokens(beam2, Range(500, 3));  // child diverges
    PrintStats("after fork + divergent continuations", alloc.GetStats());

    alloc.FreeSequence(a);
    alloc.FreeSequence(b);
    alloc.FreeSequence(c);
    alloc.FreeSequence(beam2);
    PrintStats("after freeing all sequences (blocks go to LRU cache, not free list)",
               alloc.GetStats());
  }

  {
    std::cout << "\n== Eviction under memory pressure ==\n";
    PagedKvCacheAllocator alloc(/*num_blocks=*/4, kBlockSize);
    SeqId s1 = alloc.CreateSequence();
    alloc.AppendTokens(s1, Range(1, 32));  // fills all 4 blocks
    alloc.FreeSequence(s1);                // all 4 become evictable-but-cached
    PrintStats("all blocks cached, none free", alloc.GetStats());

    SeqId s2 = alloc.CreateSequence();
    bool ok = alloc.AppendTokens(s2, Range(9999, 32));  // forces 4 evictions
    std::cout << "s2 append ok=" << std::boolalpha << ok << "\n";
    PrintStats("after forced eviction for a brand new prefix", alloc.GetStats());
  }

  return 0;
}

// ===========================================================================
// What this is, in plain terms (Pavan here)
//
// This solves the memory-management problem every self-hosted LLM inference
// stack runs into the moment more than one request hits the server at once:
// where do the K/V attention tensors for each token live in GPU memory, and
// how do you stop that memory from being wasted the second two requests
// share any text at all. If you've stood up vLLM or TensorRT-LLM you've
// already benefited from this exact idea (PagedAttention plus automatic
// prefix caching) - this file is a from-scratch, dependency-free C++
// implementation of that idea you can actually read start to finish in one
// sitting, adapt into your own serving engine, or use to teach the concept.
//
// Built because most people who use vLLM never see how the block manager
// actually works internally, and the handful of write-ups on it either wave
// hands at "radix tree" or paste real production code with fifteen other
// concerns tangled in. I wanted the minimal version that is still correct:
// real reference counting, real copy-on-write, real eviction, no shortcuts
// that would silently corrupt a running sequence.
//
// Use it when you're building or studying an LLM inference server, an
// agent runtime that fans out many parallel model calls sharing a system
// prompt or tool schema, or a batched inference pipeline where the same
// prefix (a template, a few-shot block, a RAG-injected preamble) shows up
// across thousands of requests per minute and you don't want to pay for
// the K/V compute or the memory more than once.
//
// The trick: every full block gets a hash of its own tokens chained onto
// the hash of every block before it, so two sequences only collide on
// block N if their entire history up to that point was identical, byte for
// byte - that's what makes this safe to reuse instead of just "similar
// enough." A block only becomes shareable once it's completely full and
// therefore immutable; the one block a sequence is still writing into stays
// private and gets a real copy on fork, never a shared reference. And blocks
// that drop to zero references don't get freed immediately, they sit in an
// LRU list so the next request with the same prefix thirty seconds later
// still gets the free hit - eviction only reclaims them once the pool is
// genuinely out of fresh blocks.
//
// Drop this into a C++ inference server, a research prototype comparing
// prefix-caching strategies, or a systems course on LLM serving. Swap the
// Block struct's bookkeeping for real device-memory handles and this is a
// working allocator, not a toy.
// ===========================================================================
