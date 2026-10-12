from collections import List
from math import exp, log

alias GOLDEN: UInt64 = 0x9E3779B97F4A7C15
alias MIX_A: UInt64 = 0xBF58476D1CE4E5B9
alias MIX_B: UInt64 = 0x94D049BB133111EB
alias FNV_OFFSET: UInt64 = 0xCBF29CE484222325
alias FNV_PRIME: UInt64 = 0x100000001B3

alias STATUS_KEPT = 0
alias STATUS_DUPLICATE = 1
alias STATUS_EMPTY = 2

alias DEFAULT_SHINGLE_BYTES = 5
alias DEFAULT_NUM_PERM = 128
alias DEFAULT_MAX_BUCKET_SCAN = 256


fn _mix(x: UInt64) -> UInt64:
    var z = x + GOLDEN
    z = (z ^ (z >> 30)) * MIX_A
    z = (z ^ (z >> 27)) * MIX_B
    return z ^ (z >> 31)


fn _normalize(text: String) -> List[UInt8]:
    # Lowercases ASCII, collapses every run of whitespace and control bytes
    # into one space and trims both ends. Bytes >= 128 pass through so UTF-8
    # text keeps its identity instead of being mangled.
    var out = List[UInt8]()
    var bytes = text.as_bytes()
    var pending_space = False
    for i in range(len(bytes)):
        var b = bytes[i]
        if b <= 32 or b == 127:
            pending_space = len(out) > 0
            continue
        if pending_space:
            out.append(UInt8(32))
            pending_space = False
        if b >= 65 and b <= 90:
            b = b + 32
        out.append(b)
    return out^


fn _sift_down(mut a: List[UInt64], start: Int, end: Int):
    var root = start
    while True:
        var child = 2 * root + 1
        if child >= end:
            break
        if child + 1 < end and a[child] < a[child + 1]:
            child += 1
        if a[root] < a[child]:
            var tmp = a[root]
            a[root] = a[child]
            a[child] = tmp
            root = child
        else:
            break


fn _heapsort(mut a: List[UInt64]):
    var n = len(a)
    var i = n // 2 - 1
    while i >= 0:
        _sift_down(a, i, n)
        i -= 1
    var end = n - 1
    while end > 0:
        var tmp = a[0]
        a[0] = a[end]
        a[end] = tmp
        _sift_down(a, 0, end)
        end -= 1


fn _shingle_hashes(norm: List[UInt8], k: Int) -> List[UInt64]:
    # Sorted, unique 64 bit hashes of every k byte window. A document shorter
    # than k becomes a single shingle of the whole text so tiny strings still
    # compare by content instead of all colliding on an empty set.
    var hashes = List[UInt64]()
    var n = len(norm)
    if n == 0:
        return hashes^
    var window = k if n >= k else n
    var count = n - window + 1
    for start in range(count):
        var h = FNV_OFFSET
        for j in range(window):
            h = (h ^ UInt64(norm[start + j])) * FNV_PRIME
        hashes.append(_mix(h))
    _heapsort(hashes)
    var unique = List[UInt64]()
    for i in range(len(hashes)):
        if i == 0 or hashes[i] != hashes[i - 1]:
            unique.append(hashes[i])
    return unique^


fn exact_jaccard(a: List[UInt64], b: List[UInt64]) -> Float64:
    # Both inputs must be sorted and unique, which _shingle_hashes guarantees.
    if len(a) == 0 and len(b) == 0:
        return 1.0
    var i = 0
    var j = 0
    var inter = 0
    while i < len(a) and j < len(b):
        if a[i] == b[j]:
            inter += 1
            i += 1
            j += 1
        elif a[i] < b[j]:
            i += 1
        else:
            j += 1
    var union = len(a) + len(b) - inter
    return Float64(inter) / Float64(union)


struct BandPlan(Copyable, Movable):
    var bands: Int
    var rows: Int
    var false_positive_area: Float64
    var false_negative_area: Float64

    fn __init__(out self, bands: Int, rows: Int, fp: Float64, fn_area: Float64):
        self.bands = bands
        self.rows = rows
        self.false_positive_area = fp
        self.false_negative_area = fn_area

    fn __copyinit__(out self, existing: Self):
        self.bands = existing.bands
        self.rows = existing.rows
        self.false_positive_area = existing.false_positive_area
        self.false_negative_area = existing.false_negative_area

    fn __moveinit__(out self, owned existing: Self):
        self.bands = existing.bands
        self.rows = existing.rows
        self.false_positive_area = existing.false_positive_area
        self.false_negative_area = existing.false_negative_area


fn collision_probability(s: Float64, bands: Int, rows: Int) -> Float64:
    # Chance two documents with true Jaccard s share at least one band.
    if s <= 0.0:
        return 0.0
    if s >= 1.0:
        return 1.0
    var s_pow_r = exp(Float64(rows) * log(s))
    var miss_one = 1.0 - s_pow_r
    if miss_one <= 0.0:
        return 1.0
    return 1.0 - exp(Float64(bands) * log(miss_one))


fn choose_band_plan(
    threshold: Float64,
    num_perm: Int,
    false_positive_weight: Float64 = 0.5,
) raises -> BandPlan:
    # Tries every bands x rows split of num_perm and keeps the one with the
    # smallest weighted area under the S curve on the wrong side of threshold.
    if threshold <= 0.0 or threshold >= 1.0:
        raise Error("choose_band_plan: threshold must be in (0, 1)")
    if num_perm < 2:
        raise Error("choose_band_plan: num_perm must be at least 2")
    if false_positive_weight < 0.0 or false_positive_weight > 1.0:
        raise Error("choose_band_plan: false_positive_weight must be in [0, 1]")
    var steps = 400
    var best_bands = 1
    var best_rows = num_perm
    var best_fp = 0.0
    var best_fn = 0.0
    var best_cost = Float64(1.0e18)
    for bands in range(1, num_perm + 1):
        if num_perm % bands != 0:
            continue
        var rows = num_perm // bands
        var fp = 0.0
        var fn_area = 0.0
        for i in range(steps):
            var s = (Float64(i) + 0.5) / Float64(steps)
            var p = collision_probability(s, bands, rows)
            if s < threshold:
                fp += p / Float64(steps)
            else:
                fn_area += (1.0 - p) / Float64(steps)
        var cost = false_positive_weight * fp + (1.0 - false_positive_weight) * fn_area
        if cost < best_cost:
            best_cost = cost
            best_bands = bands
            best_rows = rows
            best_fp = fp
            best_fn = fn_area
    return BandPlan(best_bands, best_rows, best_fp, best_fn)


struct DedupDecision(Copyable, Movable):
    var doc_id: Int
    var status: Int
    var duplicate_of: Int
    var estimated_jaccard: Float64
    var exact_jaccard: Float64
    var candidates_checked: Int

    fn __init__(
        out self,
        doc_id: Int,
        status: Int,
        duplicate_of: Int,
        estimated: Float64,
        exact: Float64,
        checked: Int,
    ):
        self.doc_id = doc_id
        self.status = status
        self.duplicate_of = duplicate_of
        self.estimated_jaccard = estimated
        self.exact_jaccard = exact
        self.candidates_checked = checked

    fn __copyinit__(out self, existing: Self):
        self.doc_id = existing.doc_id
        self.status = existing.status
        self.duplicate_of = existing.duplicate_of
        self.estimated_jaccard = existing.estimated_jaccard
        self.exact_jaccard = existing.exact_jaccard
        self.candidates_checked = existing.candidates_checked

    fn __moveinit__(out self, owned existing: Self):
        self.doc_id = existing.doc_id
        self.status = existing.status
        self.duplicate_of = existing.duplicate_of
        self.estimated_jaccard = existing.estimated_jaccard
        self.exact_jaccard = existing.exact_jaccard
        self.candidates_checked = existing.candidates_checked


struct BandTable(Movable):
    # Open addressing table from a band key to the newest chain entry. Older
    # entries with the same key hang off a per entry next pointer kept by the
    # deduplicator, so a bucket never needs its own allocation.
    var keys: List[UInt64]
    var heads: List[Int]
    var mask: Int
    var used: Int

    fn __init__(out self, capacity_pow2: Int):
        self.keys = List[UInt64]()
        self.heads = List[Int]()
        for _ in range(capacity_pow2):
            self.keys.append(UInt64(0))
            self.heads.append(-1)
        self.mask = capacity_pow2 - 1
        self.used = 0

    fn __moveinit__(out self, owned existing: Self):
        self.keys = existing.keys^
        self.heads = existing.heads^
        self.mask = existing.mask
        self.used = existing.used

    fn slot_for(self, key: UInt64) -> Int:
        var slot = Int(key & UInt64(self.mask))
        while self.heads[slot] >= 0 and self.keys[slot] != key:
            slot = (slot + 1) & self.mask
        return slot

    fn grow(mut self):
        var old_keys = self.keys^
        var old_heads = self.heads^
        var new_cap = (self.mask + 1) * 2
        self.keys = List[UInt64]()
        self.heads = List[Int]()
        for _ in range(new_cap):
            self.keys.append(UInt64(0))
            self.heads.append(-1)
        self.mask = new_cap - 1
        for i in range(len(old_heads)):
            if old_heads[i] >= 0:
                var slot = self.slot_for(old_keys[i])
                self.keys[slot] = old_keys[i]
                self.heads[slot] = old_heads[i]


struct MinHashDeduplicator:
    var num_perm: Int
    var bands: Int
    var rows: Int
    var shingle_bytes: Int
    var threshold: Float64
    var verify_exact: Bool
    var max_bucket_scan: Int
    var seeds: List[UInt64]
    var signatures: List[UInt64]
    var shingle_pool: List[UInt64]
    var shingle_start: List[Int]
    var shingle_len: List[Int]
    var chain_next: List[Int]
    var parent: List[Int]
    var stamp: List[Int]
    var table: BandTable
    var doc_count: Int
    var scans_truncated: Int

    fn __init__(
        out self,
        threshold: Float64,
        num_perm: Int = DEFAULT_NUM_PERM,
        shingle_bytes: Int = DEFAULT_SHINGLE_BYTES,
        verify_exact: Bool = True,
        max_bucket_scan: Int = DEFAULT_MAX_BUCKET_SCAN,
        seed: UInt64 = 0x5EEDBA5E,
    ) raises:
        if shingle_bytes < 1:
            raise Error("MinHashDeduplicator: shingle_bytes must be at least 1")
        if max_bucket_scan < 1:
            raise Error("MinHashDeduplicator: max_bucket_scan must be at least 1")
        var plan = choose_band_plan(threshold, num_perm)
        self.num_perm = num_perm
        self.bands = plan.bands
        self.rows = plan.rows
        self.shingle_bytes = shingle_bytes
        self.threshold = threshold
        self.verify_exact = verify_exact
        self.max_bucket_scan = max_bucket_scan
        self.seeds = List[UInt64]()
        for i in range(num_perm):
            self.seeds.append(_mix(seed + UInt64(i) * GOLDEN))
        self.signatures = List[UInt64]()
        self.shingle_pool = List[UInt64]()
        self.shingle_start = List[Int]()
        self.shingle_len = List[Int]()
        self.chain_next = List[Int]()
        self.parent = List[Int]()
        self.stamp = List[Int]()
        self.table = BandTable(1024)
        self.doc_count = 0
        self.scans_truncated = 0

    fn _find(mut self, x: Int) -> Int:
        var root = x
        while self.parent[root] != root:
            root = self.parent[root]
        var cur = x
        while self.parent[cur] != root:
            var nxt = self.parent[cur]
            self.parent[cur] = root
            cur = nxt
        return root

    fn _union_keep_oldest(mut self, a: Int, b: Int):
        var ra = self._find(a)
        var rb = self._find(b)
        if ra == rb:
            return
        if ra < rb:
            self.parent[rb] = ra
        else:
            self.parent[ra] = rb

    fn _band_key(self, sig_base: Int, band: Int) -> UInt64:
        var key = _mix(UInt64(band) + 1)
        for r in range(self.rows):
            key = _mix(key ^ self.signatures[sig_base + band * self.rows + r])
        return key

    fn _estimate(self, a: Int, b: Int) -> Float64:
        var matches = 0
        var base_a = a * self.num_perm
        var base_b = b * self.num_perm
        for i in range(self.num_perm):
            if self.signatures[base_a + i] == self.signatures[base_b + i]:
                matches += 1
        return Float64(matches) / Float64(self.num_perm)

    fn _exact(self, a: Int, b: Int) -> Float64:
        var sa = List[UInt64]()
        var sb = List[UInt64]()
        for i in range(self.shingle_len[a]):
            sa.append(self.shingle_pool[self.shingle_start[a] + i])
        for i in range(self.shingle_len[b]):
            sb.append(self.shingle_pool[self.shingle_start[b] + i])
        return exact_jaccard(sa, sb)

    fn _reserve_empty_doc(mut self) -> Int:
        # Empty documents still take a doc id so ids match input order, but
        # they get an all max signature and never enter the band table.
        var id = self.doc_count
        for _ in range(self.num_perm):
            self.signatures.append(UInt64.MAX)
        for _ in range(self.bands):
            self.chain_next.append(-1)
        self.shingle_start.append(len(self.shingle_pool))
        self.shingle_len.append(0)
        self.parent.append(id)
        self.stamp.append(-1)
        self.doc_count += 1
        return id

    fn add(mut self, text: String) -> DedupDecision:
        var norm = _normalize(text)
        if len(norm) == 0:
            var empty_id = self._reserve_empty_doc()
            return DedupDecision(empty_id, STATUS_EMPTY, -1, 0.0, -1.0, 0)

        var shingles = _shingle_hashes(norm, self.shingle_bytes)
        var id = self.doc_count
        var sig_base = id * self.num_perm

        for p in range(self.num_perm):
            var seed = self.seeds[p]
            var best = UInt64.MAX
            for i in range(len(shingles)):
                var v = _mix(shingles[i] ^ seed)
                if v < best:
                    best = v
            self.signatures.append(best)

        self.shingle_start.append(len(self.shingle_pool))
        self.shingle_len.append(len(shingles))
        for i in range(len(shingles)):
            self.shingle_pool.append(shingles[i])
        self.parent.append(id)
        self.stamp.append(-1)
        self.doc_count += 1

        var best_doc = -1
        var best_estimate = 0.0
        var best_exact = -1.0
        var checked = 0
        var matched = List[Int]()

        for band in range(self.bands):
            var key = self._band_key(sig_base, band)
            if (self.table.used + 1) * 2 > self.table.mask + 1:
                self.table.grow()
            var slot = self.table.slot_for(key)
            var entry = id * self.bands + band
            var head = self.table.heads[slot]
            if head < 0:
                self.table.keys[slot] = key
                self.table.used += 1
            self.chain_next.append(head)
            self.table.heads[slot] = entry

            var cursor = head
            var scanned = 0
            while cursor >= 0:
                if scanned >= self.max_bucket_scan:
                    self.scans_truncated += 1
                    break
                scanned += 1
                var other = cursor // self.bands
                cursor = self.chain_next[cursor]
                if self.stamp[other] == id:
                    continue
                self.stamp[other] = id
                checked += 1
                var est = self._estimate(id, other)
                if est < self.threshold:
                    continue
                var exact = -1.0
                if self.verify_exact:
                    exact = self._exact(id, other)
                    if exact < self.threshold:
                        continue
                matched.append(other)
                var score = exact if self.verify_exact else est
                var best_score = best_exact if self.verify_exact else best_estimate
                if best_doc < 0 or score > best_score:
                    best_doc = other
                    best_estimate = est
                    best_exact = exact

        if best_doc < 0:
            return DedupDecision(id, STATUS_KEPT, -1, 0.0, -1.0, checked)

        for i in range(len(matched)):
            self._union_keep_oldest(id, matched[i])
        var root = self._find(id)
        return DedupDecision(id, STATUS_DUPLICATE, root, best_estimate, best_exact, checked)

    fn cluster_of(mut self, doc_id: Int) raises -> Int:
        if doc_id < 0 or doc_id >= self.doc_count:
            raise Error("cluster_of: unknown doc id " + String(doc_id))
        return self._find(doc_id)

    fn cluster_count(mut self) -> Int:
        var roots = 0
        for i in range(self.doc_count):
            if self._find(i) == i:
                roots += 1
        return roots


fn _word(index: UInt64) -> String:
    var syllables = List[String]()
    syllables.append("ka")
    syllables.append("lo")
    syllables.append("mi")
    syllables.append("ren")
    syllables.append("tu")
    syllables.append("sha")
    syllables.append("vo")
    syllables.append("dri")
    var h = _mix(index)
    var w = String("")
    var n = Int(h % 3) + 2
    for _ in range(n):
        w += syllables[Int(h % 8)]
        h = _mix(h)
    return w


fn _synthetic_doc(doc: Int, words: Int, edits: Int) -> String:
    var out = String("")
    for i in range(words):
        var token = UInt64(doc) * 100003 + UInt64(i)
        if edits > 0 and i % (words // edits) == 3:
            token = token + 7777777
        if i > 0:
            out += " "
        out += _word(token)
    return out


fn _expect(cond: Bool, message: String) raises:
    if not cond:
        raise Error("self check failed: " + message)


fn main() raises:
    var plan = choose_band_plan(0.8, 128)
    print(
        "plan for threshold 0.8 over 128 permutations: bands="
        + String(plan.bands) + " rows=" + String(plan.rows)
        + " fp_area=" + String(plan.false_positive_area)
        + " fn_area=" + String(plan.false_negative_area)
    )
    _expect(plan.bands * plan.rows == 128, "plan must use every permutation")

    var dedup = MinHashDeduplicator(0.8)
    var base_count = 200
    var words = 150
    for d in range(base_count):
        var r = dedup.add(_synthetic_doc(d, words, 0))
        _expect(r.status == STATUS_KEPT, "unique doc " + String(d) + " flagged as duplicate")

    var exact_copy = dedup.add(_synthetic_doc(17, words, 0))
    _expect(exact_copy.status == STATUS_DUPLICATE, "exact copy not detected")
    _expect(exact_copy.duplicate_of == 17, "exact copy should point at original doc 17")

    var shouted = dedup.add("  " + _synthetic_doc(42, words, 0).upper() + "\n\n")
    _expect(shouted.duplicate_of == 42, "case and whitespace variant should normalize to doc 42")

    var near = dedup.add(_synthetic_doc(99, words, 3))
    _expect(near.status == STATUS_DUPLICATE, "three word edit not detected")
    _expect(near.duplicate_of == 99, "near duplicate should cluster with doc 99")
    _expect(near.exact_jaccard >= 0.8, "near duplicate exact jaccard below threshold")

    var other = dedup.add(_synthetic_doc(5000, words, 0))
    _expect(other.status == STATUS_KEPT, "unrelated doc flagged as duplicate")

    var blank = dedup.add("   \t\n ")
    _expect(blank.status == STATUS_EMPTY, "blank doc should be reported empty")

    var tiny_a = dedup.add("ok")
    var tiny_b = dedup.add("ok")
    _expect(tiny_a.status == STATUS_KEPT, "first tiny doc should be kept")
    _expect(tiny_b.status == STATUS_DUPLICATE, "identical tiny doc should be a duplicate")

    print("documents seen: " + String(dedup.doc_count))
    print("clusters: " + String(dedup.cluster_count()))
    print("bucket scans truncated: " + String(dedup.scans_truncated))
    print("near duplicate: doc=" + String(near.doc_id) + " of=" + String(near.duplicate_of)
          + " estimated=" + String(near.estimated_jaccard)
          + " exact=" + String(near.exact_jaccard))
    print("all self checks passed")
