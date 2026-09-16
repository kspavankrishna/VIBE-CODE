from collections import List
from memory import UnsafePointer
from math import sin, sqrt
from algorithm import parallelize
from sys.info import simdwidthof

alias SIMD_WIDTH = simdwidthof[DType.float32]()
alias EPSILON: Float32 = 1e-12


fn _is_finite(x: Float32) -> Bool:
    if x != x:
        return False
    if x > Float32(3.4e38) or x < Float32(-3.4e38):
        return False
    return True


fn _dot(a: UnsafePointer[Float32], b: UnsafePointer[Float32], dim: Int) -> Float32:
    var acc = SIMD[DType.float32, SIMD_WIDTH](0.0)
    var i = 0
    while i + SIMD_WIDTH <= dim:
        var va = a.load[width=SIMD_WIDTH](i)
        var vb = b.load[width=SIMD_WIDTH](i)
        acc += va * vb
        i += SIMD_WIDTH
    var total = acc.reduce_add()
    while i < dim:
        total += a[i] * b[i]
        i += 1
    return total


struct SearchResult(Copyable, Movable):
    var index: Int
    var score: Float32

    fn __init__(out self, index: Int, score: Float32):
        self.index = index
        self.score = score

    fn __copyinit__(out self, existing: Self):
        self.index = existing.index
        self.score = existing.score

    fn __moveinit__(out self, owned existing: Self):
        self.index = existing.index
        self.score = existing.score


fn _better(a_score: Float32, a_index: Int, b_score: Float32, b_index: Int) -> Bool:
    if a_score != b_score:
        return a_score > b_score
    return a_index < b_index


fn _weaker(a: SearchResult, b: SearchResult) -> Bool:
    return _better(b.score, b.index, a.score, a.index)


fn _sift_up(mut heap: List[SearchResult], start: Int):
    var i = start
    while i > 0:
        var parent = (i - 1) // 2
        if _weaker(heap[i], heap[parent]):
            var tmp = heap[i]
            heap[i] = heap[parent]
            heap[parent] = tmp
            i = parent
        else:
            break


fn _sift_down(mut heap: List[SearchResult], start: Int, n: Int):
    var i = start
    while True:
        var left = 2 * i + 1
        var right = 2 * i + 2
        var smallest = i
        if left < n and _weaker(heap[left], heap[smallest]):
            smallest = left
        if right < n and _weaker(heap[right], heap[smallest]):
            smallest = right
        if smallest == i:
            break
        var tmp = heap[i]
        heap[i] = heap[smallest]
        heap[smallest] = tmp
        i = smallest


fn _offer(mut heap: List[SearchResult], k: Int, idx: Int, score: Float32):
    if len(heap) < k:
        heap.append(SearchResult(idx, score))
        _sift_up(heap, len(heap) - 1)
    elif _weaker(heap[0], SearchResult(idx, score)):
        heap[0] = SearchResult(idx, score)
        _sift_down(heap, 0, len(heap))


fn _sort_descending(mut items: List[SearchResult]):
    var n = len(items)
    var i = 1
    while i < n:
        var key = items[i]
        var j = i - 1
        while j >= 0 and _better(key.score, key.index, items[j].score, items[j].index):
            items[j + 1] = items[j]
            j -= 1
        items[j + 1] = key
        i += 1


fn _prepare_buffer(dim: Int, vector: List[Float32], label: String) raises -> UnsafePointer[Float32]:
    if len(vector) != dim:
        raise Error(label + ": length " + String(len(vector)) + " does not match dimension " + String(dim))
    var buf = UnsafePointer[Float32].alloc(dim)
    for i in range(dim):
        if not _is_finite(vector[i]):
            buf.free()
            raise Error(label + ": contains NaN or Inf at position " + String(i))
        buf[i] = vector[i]
    return buf


struct EmbeddingIndex:
    var dim: Int
    var capacity: Int
    var count: Int
    var data: UnsafePointer[Float32]   # row major, capacity * dim slots
    var norms: UnsafePointer[Float32]  # precomputed L2 norm per stored row

    fn __init__(out self, dim: Int, capacity: Int) raises:
        if dim <= 0:
            raise Error("EmbeddingIndex: dim must be positive")
        if capacity <= 0:
            raise Error("EmbeddingIndex: capacity must be positive")
        self.dim = dim
        self.capacity = capacity
        self.count = 0
        self.data = UnsafePointer[Float32].alloc(dim * capacity)
        self.norms = UnsafePointer[Float32].alloc(capacity)

    fn __moveinit__(out self, owned existing: Self):
        self.dim = existing.dim
        self.capacity = existing.capacity
        self.count = existing.count
        self.data = existing.data
        self.norms = existing.norms

    fn __del__(owned self):
        self.data.free()
        self.norms.free()

    fn add(mut self, vector: List[Float32]) raises:
        if len(vector) != self.dim:
            raise Error(
                "EmbeddingIndex.add: vector length " + String(len(vector))
                + " does not match index dimension " + String(self.dim)
            )
        if self.count >= self.capacity:
            raise Error(
                "EmbeddingIndex.add: index is full at capacity " + String(self.capacity)
                + ", rebuild with a larger capacity"
            )
        # Validate every element before writing anything, so a rejected
        # vector never leaves a half written row behind at self.count.
        for i in range(self.dim):
            if not _is_finite(vector[i]):
                raise Error("EmbeddingIndex.add: vector contains NaN or Inf at position " + String(i))
        var row_offset = self.count * self.dim
        var sq_sum: Float32 = 0.0
        for i in range(self.dim):
            var v = vector[i]
            self.data[row_offset + i] = v
            sq_sum += v * v
        self.norms[self.count] = sqrt(sq_sum)
        self.count += 1

    fn _scan_rows(
        self,
        query_buf: UnsafePointer[Float32],
        q_norm: Float32,
        start: Int,
        end: Int,
        k: Int,
        min_score: Float32,
    ) -> List[SearchResult]:
        var heap = List[SearchResult]()
        if start >= end:
            return heap^
        var eff_k = k
        if eff_k > (end - start):
            eff_k = end - start
        for row in range(start, end):
            var row_norm = self.norms[row]
            if row_norm < EPSILON:
                continue
            var row_ptr = self.data + row * self.dim
            var raw = _dot(query_buf, row_ptr, self.dim)
            var cosine = raw / (q_norm * row_norm)
            if cosine < min_score:
                continue
            _offer(heap, eff_k, row, cosine)
        return heap^

    fn search(self, query: List[Float32], k: Int, min_score: Float32 = -1.0) raises -> List[SearchResult]:
        if k <= 0:
            raise Error("EmbeddingIndex.search: k must be positive")
        var query_buf = _prepare_buffer(self.dim, query, "EmbeddingIndex.search")
        # The query's own norm never changes the ranking of candidates against
        # each other (it is a positive constant factor shared by every score),
        # but dividing it in keeps the returned number a true cosine value
        # in [-1, 1], which is what min_score and callers expect.
        var q_norm = sqrt(_dot(query_buf, query_buf, self.dim))
        if q_norm < EPSILON:
            query_buf.free()
            raise Error("EmbeddingIndex.search: query is a zero vector, cosine similarity is undefined")
        var results = self._scan_rows(query_buf, q_norm, 0, self.count, k, min_score)
        query_buf.free()
        _sort_descending(results)
        return results^

    fn search_parallel(
        self,
        query: List[Float32],
        k: Int,
        num_workers: Int,
        min_score: Float32 = -1.0,
    ) raises -> List[SearchResult]:
        if k <= 0:
            raise Error("EmbeddingIndex.search_parallel: k must be positive")
        if num_workers <= 1 or self.count <= num_workers:
            return self.search(query, k, min_score)

        var query_buf = _prepare_buffer(self.dim, query, "EmbeddingIndex.search_parallel")
        var q_norm = sqrt(_dot(query_buf, query_buf, self.dim))
        if q_norm < EPSILON:
            query_buf.free()
            raise Error("EmbeddingIndex.search_parallel: query is a zero vector, cosine similarity is undefined")

        var chunk = (self.count + num_workers - 1) // num_workers
        var partials = List[List[SearchResult]]()
        for _ in range(num_workers):
            partials.append(List[SearchResult]())

        @parameter
        fn worker(w: Int):
            var start = w * chunk
            var end = start + chunk
            if end > self.count:
                end = self.count
            partials[w] = self._scan_rows(query_buf, q_norm, start, end, k, min_score)

        parallelize[worker](num_workers, num_workers)
        query_buf.free()

        var k_clamped = k
        if k_clamped > self.count:
            k_clamped = self.count

        var merged = List[SearchResult]()
        for w in range(num_workers):
            for i in range(len(partials[w])):
                _offer(merged, k_clamped, partials[w][i].index, partials[w][i].score)

        _sort_descending(merged)
        return merged^


fn _synthetic_row(row: Int, dim: Int) -> List[Float32]:
    var vec = List[Float32]()
    for d in range(dim):
        vec.append(sin(Float32(row) * 0.017 + Float32(d) * 0.31))
    return vec^


fn main() raises:
    var dim = 64
    var count = 2000
    var capacity = count + 4
    var index = EmbeddingIndex(dim, capacity)

    for i in range(count):
        index.add(_synthetic_row(i, dim))

    var target = count // 2
    var query = _synthetic_row(target, dim)
    var k = 10

    var sequential = index.search(query, k)
    var parallel = index.search_parallel(query, k, 4)

    if len(sequential) != len(parallel):
        raise Error("determinism check failed: result counts differ")
    for i in range(len(sequential)):
        if sequential[i].index != parallel[i].index or sequential[i].score != parallel[i].score:
            raise Error("determinism check failed: ranking differs at position " + String(i))

    print("sequential and parallel search agree on all " + String(k) + " results")
    for i in range(len(sequential)):
        print(String(i) + ": row=" + String(sequential[i].index) + " cosine=" + String(sequential[i].score))

    var nan_row = _synthetic_row(0, dim)
    nan_row[0] = Float32(0.0) / Float32(0.0)
    var nan_guard_triggered = False
    try:
        index.add(nan_row)
    except:
        nan_guard_triggered = True
    if not nan_guard_triggered:
        raise Error("NaN guard did not trigger")
    print("NaN guard confirmed: rejected a vector containing NaN")

    for i in range(4):
        index.add(_synthetic_row(count + i, dim))
    var capacity_guard_triggered = False
    try:
        index.add(_synthetic_row(count + 4, dim))
    except:
        capacity_guard_triggered = True
    if not capacity_guard_triggered:
        raise Error("capacity guard did not trigger")
    print("capacity guard confirmed: rejected an insert past capacity " + String(capacity))
