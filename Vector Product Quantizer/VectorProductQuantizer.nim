## Vector Product Quantizer
## Trains product quantization codebooks for float32 embedding vectors,
## encodes/decodes them to compact byte codes, and runs asymmetric
## distance computation (ADC) search directly on the compressed codes.

import std/[os, streams, strutils, strformat, random, heapqueue, math, times, algorithm, tables, sets]

const
  MagicCodebook = "VPQ1"
  MagicCodes = "VPQC"
  MaxClusters = 256          # a code must fit in one byte
  ReadBatch = 4096           # vectors per streaming batch
  ProgressEvery = 5          # k-means iterations between stderr updates

type
  VpqError = object of CatchableError

  Codebook = object
    dim: int                # full vector dimension D
    subCount: int           # number of subquantizers M
    subDim: int             # dimension of each subvector, D div M
    clusters: int           # K, centroids per subspace
    centroids: seq[float32] # flattened [subCount][clusters][subDim]

  Metric = enum
    metL2, metIP

proc fail(msg: string) {.noReturn.} =
  raise newException(VpqError, msg)

proc centroidOffset(cb: Codebook, sub, cluster: int): int {.inline.} =
  (sub * cb.clusters + cluster) * cb.subDim

# ---------------------------------------------------------------------------
# Raw float32 vector IO. Files are a flat stream of little endian float32
# values, D per vector, with no header. This matches what numpy's
# `array.tofile()` or any language's raw buffer dump produces, so no
# conversion step is needed to feed embeddings from an existing pipeline.
# ---------------------------------------------------------------------------

proc vectorCount(path: string, dim: int): int64 =
  let sz = getFileSize(path)
  let recBytes = int64(dim) * 4
  if sz mod recBytes != 0:
    fail(&"{path}: file size {sz} bytes is not a multiple of dim*4 ({recBytes}); " &
         "the file is truncated, or --dim does not match how it was written")
  sz div recBytes

proc readVectorsBatch(s: Stream, dim, count: int): seq[float32] =
  ## Reads `count` vectors (count*dim float32 values) from an open stream.
  result = newSeq[float32](count * dim)
  if count > 0:
    let want = count * dim * 4
    let got = s.readData(addr result[0], want)
    if got != want:
      fail(&"unexpected end of file: wanted {want} bytes, got {got}")

proc checkFinite(v: openArray[float32], vectorIndex: int64) =
  for x in v:
    if x.classify in {fcNan, fcInf, fcNegInf}:
      fail(&"vector {vectorIndex} contains a non finite value ({x}); " &
           "product quantization cannot train or encode on NaN/Inf input")

# ---------------------------------------------------------------------------
# Codebook persistence
# ---------------------------------------------------------------------------

proc writeCodebook(cb: Codebook, path: string) =
  var s = newFileStream(path, fmWrite)
  if s.isNil: fail(&"cannot open {path} for writing")
  defer: s.close()
  s.write(MagicCodebook)
  s.write(uint32(1))            # format version
  s.write(uint32(cb.dim))
  s.write(uint32(cb.subCount))
  s.write(uint32(cb.subDim))
  s.write(uint32(cb.clusters))
  if cb.centroids.len > 0:
    s.writeData(unsafeAddr cb.centroids[0], cb.centroids.len * 4)

proc readCodebook(path: string): Codebook =
  var s = newFileStream(path, fmRead)
  if s.isNil: fail(&"cannot open codebook {path}")
  defer: s.close()
  let magic = s.readStr(4)
  if magic != MagicCodebook:
    fail(&"{path} is not a Vector Product Quantizer codebook (bad magic {magic.escape})")
  let version = s.readUint32()
  if version != 1'u32:
    fail(&"{path} was written by an unsupported codebook format version {version}")
  result.dim = int(s.readUint32())
  result.subCount = int(s.readUint32())
  result.subDim = int(s.readUint32())
  result.clusters = int(s.readUint32())
  let n = result.subCount * result.clusters * result.subDim
  result.centroids = newSeq[float32](n)
  if n > 0:
    let got = s.readData(addr result.centroids[0], n * 4)
    if got != n * 4:
      fail(&"{path} is truncated: expected {n} centroid floats, got {got div 4}")

# ---------------------------------------------------------------------------
# K-means per subspace, with k-means++ seeding and empty cluster recovery.
# Accumulation happens in float64 so a subspace with millions of sample
# points does not lose precision the way a running float32 mean would.
# ---------------------------------------------------------------------------

proc distSq(a: openArray[float32], aOff: int, b: openArray[float32], bOff, n: int): float64 {.inline.} =
  var acc = 0.0'f64
  for i in 0 ..< n:
    let d = float64(a[aOff + i]) - float64(b[bOff + i])
    acc += d * d
  acc

proc kmeansPlusPlusInit(data: seq[float32], n, dim, k: int, rng: var Rand): seq[float32] =
  ## Picks k initial centroids from n sample points of width dim, biasing
  ## toward points far from centroids already chosen. Falls back to a
  ## uniform pick when duplicate points collapse every distance to zero,
  ## which prevents an infinite reweighting loop on degenerate inputs.
  result = newSeq[float32](k * dim)
  var minDist = newSeq[float64](n)
  for i in 0 ..< n: minDist[i] = Inf
  let first = rng.rand(n - 1)
  copyMem(addr result[0], unsafeAddr data[first * dim], dim * 4)
  for chosen in 1 ..< k:
    let lastOff = (chosen - 1) * dim
    var total = 0.0'f64
    for i in 0 ..< n:
      let d = distSq(data, i * dim, result, lastOff, dim)
      if d < minDist[i]: minDist[i] = d
      total += minDist[i]
    var pick = n - 1
    if total > 0:
      var target = rng.rand(total)
      for i in 0 ..< n:
        if target <= minDist[i]:
          pick = i
          break
        target -= minDist[i]
    else:
      pick = rng.rand(n - 1)
    copyMem(addr result[chosen * dim], unsafeAddr data[pick * dim], dim * 4)

proc trainSubspace(data: seq[float32], n, dim, k, iters: int, rng: var Rand,
                    subLabel: string): seq[float32] =
  if n < k:
    fail(&"subspace {subLabel}: only {n} training vectors but {k} clusters requested; " &
         "lower --clusters or raise --sample")
  var centroids = kmeansPlusPlusInit(data, n, dim, k, rng)
  var assign = newSeq[int](n)
  var sums = newSeq[float64](k * dim)
  var counts = newSeq[int](k)
  var prevInertia = Inf
  for iter in 0 ..< iters:
    for i in 0 ..< k * dim: sums[i] = 0.0
    for i in 0 ..< k: counts[i] = 0
    var inertia = 0.0'f64
    for i in 0 ..< n:
      var best = 0
      var bestD = distSq(data, i * dim, centroids, 0, dim)
      for c in 1 ..< k:
        let d = distSq(data, i * dim, centroids, c * dim, dim)
        if d < bestD:
          bestD = d
          best = c
      assign[i] = best
      inertia += bestD
      counts[best] += 1
      let so = best * dim
      let doff = i * dim
      for j in 0 ..< dim:
        sums[so + j] += float64(data[doff + j])
    # Recompute centroids; an empty cluster is reseeded from the point in
    # the largest cluster that sits farthest from that cluster's mean, so
    # one bad initial draw cannot permanently waste a whole code value.
    for c in 0 ..< k:
      if counts[c] > 0:
        let co = c * dim
        for j in 0 ..< dim:
          centroids[co + j] = float32(sums[co + j] / float64(counts[c]))
      else:
        var donor = 0
        for cc in 1 ..< k:
          if counts[cc] > counts[donor]: donor = cc
        if counts[donor] < 2:
          continue # nothing safe to split; keep the stale centroid this round
        var farthest = -1
        var farthestD = -1.0'f64
        for i in 0 ..< n:
          if assign[i] == donor:
            let d = distSq(data, i * dim, centroids, donor * dim, dim)
            if d > farthestD:
              farthestD = d
              farthest = i
        copyMem(addr centroids[c * dim], unsafeAddr data[farthest * dim], dim * 4)
        counts[donor] -= 1
    if (iter + 1) mod ProgressEvery == 0 or iter == iters - 1:
      stderr.writeLine(&"  subspace {subLabel}: iter {iter+1}/{iters} inertia={inertia:.3f}")
    if abs(prevInertia - inertia) < prevInertia * 1e-6:
      stderr.writeLine(&"  subspace {subLabel}: converged at iter {iter+1}")
      break
    prevInertia = inertia
  centroids

# ---------------------------------------------------------------------------
# Reservoir sampling so training never requires the whole database in RAM.
# ---------------------------------------------------------------------------

proc reservoirSample(path: string, dim: int, total: int64, sampleSize: int,
                      seed: int64): seq[float32] =
  var rng = initRand(seed)
  let take = int(min(int64(sampleSize), total))
  result = newSeq[float32](take * dim)
  var s = newFileStream(path, fmRead)
  if s.isNil: fail(&"cannot open {path}")
  defer: s.close()
  for i in 0 ..< take:
    let v = readVectorsBatch(s, dim, 1)
    checkFinite(v, int64(i))
    copyMem(addr result[i * dim], unsafeAddr v[0], dim * 4)
  var idx = int64(take)
  var buf = newSeq[float32](dim)
  while idx < total:
    let got = s.readData(addr buf[0], dim * 4)
    if got != dim * 4: fail("reservoir sampling hit an unexpectedly short read")
    let j = rng.rand(idx)
    if j < int64(take):
      checkFinite(buf, idx)
      copyMem(addr result[int(j) * dim], addr buf[0], dim * 4)
    idx += 1

# ---------------------------------------------------------------------------
# train
# ---------------------------------------------------------------------------

proc cmdTrain(input: string, dim, subCount, clusters, iters, sampleSize: int, seed: int64, outPath: string) =
  if dim mod subCount != 0:
    var divisors: seq[int]
    for d in 1 .. dim:
      if dim mod d == 0: divisors.add d
    fail(&"--dim {dim} is not divisible by --subquant {subCount}; " &
         &"valid subquantizer counts for this dim are: {divisors.join(\", \")}")
  if clusters < 1 or clusters > MaxClusters:
    fail(&"--clusters must be between 1 and {MaxClusters} (a code is one byte)")
  let subDim = dim div subCount
  let total = vectorCount(input, dim)
  if total == 0: fail(&"{input} contains zero vectors")
  stderr.writeLine(&"training on {min(int64(sampleSize), total)} of {total} vectors, " &
                    &"D={dim} M={subCount} subDim={subDim} K={clusters}")
  let sample = reservoirSample(input, dim, total, sampleSize, seed)
  let n = sample.len div dim
  var cb = Codebook(dim: dim, subCount: subCount, subDim: subDim, clusters: clusters,
                     centroids: newSeq[float32](subCount * clusters * subDim))
  var sub = newSeq[float32](n * subDim)
  for m in 0 ..< subCount:
    for i in 0 ..< n:
      copyMem(addr sub[i * subDim], unsafeAddr sample[i * dim + m * subDim], subDim * 4)
    var rng = initRand(seed + int64(m) * 104729)
    let centroids = trainSubspace(sub, n, subDim, clusters, iters, rng, $m)
    copyMem(addr cb.centroids[m * clusters * subDim], unsafeAddr centroids[0], centroids.len * 4)
  writeCodebook(cb, outPath)
  let rawBytes = total * int64(dim) * 4
  let codeBytes = total * int64(subCount)
  stderr.writeLine(&"codebook written to {outPath}")
  stderr.writeLine(&"projected compression: {rawBytes} -> {codeBytes} bytes " &
                    &"({float64(rawBytes) / float64(codeBytes):.1f}x)")

# ---------------------------------------------------------------------------
# encode / decode, both streamed in fixed size batches
# ---------------------------------------------------------------------------

proc encodeOne(cb: Codebook, vec: openArray[float32], vecOff: int, codeOut: var openArray[uint8]) =
  for m in 0 ..< cb.subCount:
    var best = 0
    var bestD = distSq(vec, vecOff + m * cb.subDim, cb.centroids, cb.centroidOffset(m, 0), cb.subDim)
    for c in 1 ..< cb.clusters:
      let d = distSq(vec, vecOff + m * cb.subDim, cb.centroids, cb.centroidOffset(m, c), cb.subDim)
      if d < bestD:
        bestD = d
        best = c
    codeOut[m] = uint8(best)

proc cmdEncode(input, codebookPath: string, dim: int, outPath: string) =
  let cb = readCodebook(codebookPath)
  if cb.dim != dim:
    fail(&"codebook was trained for dim {cb.dim}, but --dim {dim} was given")
  let total = vectorCount(input, dim)
  var inS = newFileStream(input, fmRead)
  if inS.isNil: fail(&"cannot open {input}")
  defer: inS.close()
  var outS = newFileStream(outPath, fmWrite)
  if outS.isNil: fail(&"cannot open {outPath} for writing")
  defer: outS.close()
  outS.write(MagicCodes)
  outS.write(uint32(cb.subCount))
  outS.write(uint64(total))
  var done: int64 = 0
  var codeBuf = newSeq[uint8](ReadBatch * cb.subCount)
  let started = epochTime()
  while done < total:
    let batch = int(min(int64(ReadBatch), total - done))
    let vecs = readVectorsBatch(inS, dim, batch)
    checkFinite(vecs, done)
    for i in 0 ..< batch:
      encodeOne(cb, vecs, i * dim, toOpenArray(codeBuf, i * cb.subCount, (i + 1) * cb.subCount - 1))
    outS.writeData(addr codeBuf[0], batch * cb.subCount)
    done += batch
    if done mod (ReadBatch * 20) == 0 or done == total:
      stderr.writeLine(&"encoded {done}/{total} ({float64(done)/float64(total)*100:.1f}%) " &
                        &"in {epochTime()-started:.1f}s")
  stderr.writeLine(&"wrote {total} codes ({cb.subCount} bytes each) to {outPath}")

proc decodeOne(cb: Codebook, code: openArray[uint8], codeOff: int, vecOut: var openArray[float32], vecOffOut: int) =
  for m in 0 ..< cb.subCount:
    let c = int(code[codeOff + m])
    let co = cb.centroidOffset(m, c)
    for j in 0 ..< cb.subDim:
      vecOut[vecOffOut + m * cb.subDim + j] = cb.centroids[co + j]

proc readCodesHeader(path: string): tuple[s: FileStream, subCount: int, total: int64] =
  var s = newFileStream(path, fmRead)
  if s.isNil: fail(&"cannot open codes file {path}")
  let magic = s.readStr(4)
  if magic != MagicCodes:
    fail(&"{path} is not a Vector Product Quantizer codes file (bad magic {magic.escape})")
  let subCount = int(s.readUint32())
  let total = int64(s.readUint64())
  (s, subCount, total)

proc cmdDecode(codesPath, codebookPath, outPath: string) =
  let cb = readCodebook(codebookPath)
  let (inS, subCount, total) = readCodesHeader(codesPath)
  defer: inS.close()
  if subCount != cb.subCount:
    fail(&"codes file has M={subCount} but codebook has M={cb.subCount}")
  var outS = newFileStream(outPath, fmWrite)
  if outS.isNil: fail(&"cannot open {outPath} for writing")
  defer: outS.close()
  var done: int64 = 0
  var codeBuf = newSeq[uint8](ReadBatch * subCount)
  var vecBuf = newSeq[float32](ReadBatch * cb.dim)
  while done < total:
    let batch = int(min(int64(ReadBatch), total - done))
    let got = inS.readData(addr codeBuf[0], batch * subCount)
    if got != batch * subCount: fail("codes file ended early")
    for i in 0 ..< batch:
      decodeOne(cb, codeBuf, i * subCount, vecBuf, i * cb.dim)
    outS.writeData(addr vecBuf[0], batch * cb.dim * 4)
    done += batch
  stderr.writeLine(&"decoded {total} vectors to {outPath}")

# ---------------------------------------------------------------------------
# search: asymmetric distance computation against the codes file
# ---------------------------------------------------------------------------

type ScoredHit = object
  score: float64
  idx: int64

# Reversed on purpose: HeapQueue is a min-heap over `<`, and inverting the
# relation turns it into a max-heap keyed on score. That puts the current
# worst of the kept top-k at the root (index 0), which is what a bounded
# top-k selection needs to test against on every new candidate.
proc `<`(a, b: ScoredHit): bool = a.score > b.score

proc buildDistanceTable(cb: Codebook, query: openArray[float32], metric: Metric): seq[float64] =
  ## table[m * clusters + c] = subspace distance/similarity contribution
  result = newSeq[float64](cb.subCount * cb.clusters)
  for m in 0 ..< cb.subCount:
    for c in 0 ..< cb.clusters:
      let co = cb.centroidOffset(m, c)
      var acc = 0.0'f64
      case metric
      of metL2:
        for j in 0 ..< cb.subDim:
          let d = float64(query[m * cb.subDim + j]) - float64(cb.centroids[co + j])
          acc += d * d
      of metIP:
        for j in 0 ..< cb.subDim:
          acc += float64(query[m * cb.subDim + j]) * float64(cb.centroids[co + j])
      result[m * cb.clusters + c] = acc

proc adcScore(table: seq[float64], code: openArray[uint8], codeOff, subCount, clusters: int): float64 {.inline.} =
  var acc = 0.0'f64
  for m in 0 ..< subCount:
    acc += table[m * clusters + int(code[codeOff + m])]
  acc

proc cmdSearch(queryPath, codebookPath, codesPath: string, dim, topK: int, metric: Metric, outPath: string) =
  let cb = readCodebook(codebookPath)
  if cb.dim != dim:
    fail(&"codebook was trained for dim {cb.dim}, but --dim {dim} was given")
  let qTotal = vectorCount(queryPath, dim)
  if qTotal == 0: fail(&"{queryPath} contains zero query vectors")
  var qS = newFileStream(queryPath, fmRead)
  if qS.isNil: fail(&"cannot open {queryPath}")
  defer: qS.close()
  let queries = readVectorsBatch(qS, dim, int(qTotal))
  checkFinite(queries, 0)

  # metL2: smaller is better, so a bounded max-heap evicts the worst of the
  # current top-k. metIP: larger is better, so evict the smallest instead.
  # Negating the score for IP lets both cases reuse one max-heap type.
  var heaps = newSeq[HeapQueue[ScoredHit]](qTotal)
  var tables = newSeq[seq[float64]](qTotal)
  for qi in 0 ..< int(qTotal):
    tables[qi] = buildDistanceTable(cb, toOpenArray(queries, qi * dim, (qi + 1) * dim - 1), metric)

  let (codeS, subCount, total) = readCodesHeader(codesPath)
  defer: codeS.close()
  if subCount != cb.subCount:
    fail(&"codes file has M={subCount} but codebook has M={cb.subCount}")

  var codeBuf = newSeq[uint8](ReadBatch * subCount)
  var done: int64 = 0
  let sign = if metric == metL2: 1.0'f64 else: -1.0'f64
  while done < total:
    let batch = int(min(int64(ReadBatch), total - done))
    let got = codeS.readData(addr codeBuf[0], batch * subCount)
    if got != batch * subCount: fail("codes file ended early")
    for i in 0 ..< batch:
      let vecIdx = done + int64(i)
      for qi in 0 ..< int(qTotal):
        let raw = adcScore(tables[qi], codeBuf, i * subCount, subCount, cb.clusters)
        let hit = ScoredHit(score: raw * sign, idx: vecIdx)
        if heaps[qi].len < topK:
          heaps[qi].push(hit)
        elif hit.score < heaps[qi][0].score:
          discard heaps[qi].pop()
          heaps[qi].push(hit)
    done += batch

  var outS = newFileStream(outPath, fmWrite)
  if outS.isNil: fail(&"cannot open {outPath} for writing")
  defer: outS.close()
  outS.writeLine("query_index\trank\tvector_index\tdistance_or_score")
  for qi in 0 ..< int(qTotal):
    var results: seq[ScoredHit]
    while heaps[qi].len > 0: results.add heaps[qi].pop()
    results.reverse() # heap pops worst-first; reverse gives best-first
    for rank, hit in results:
      let reported = if metric == metL2: hit.score else: -hit.score
      outS.writeLine(&"{qi}\t{rank+1}\t{hit.idx}\t{reported:.6f}")
  let qWord = if qTotal == 1: "query" else: "queries"
  stderr.writeLine(&"searched {total} codes for {qTotal} {qWord}, wrote {outPath}")

# ---------------------------------------------------------------------------
# eval: reconstruction error plus recall@k against brute force ground truth.
# Loads the full input into memory, so this is a diagnostic step meant for
# a sample or a dataset that already fits in RAM, not the full production
# corpus (encode/search stay streaming for that).
# ---------------------------------------------------------------------------

proc cmdEval(input, codebookPath, codesPath: string, dim, sampleSize, topK: int, seed: int64) =
  let cb = readCodebook(codebookPath)
  if cb.dim != dim:
    fail(&"codebook was trained for dim {cb.dim}, but --dim {dim} was given")
  let total = vectorCount(input, dim)
  var inS = newFileStream(input, fmRead)
  if inS.isNil: fail(&"cannot open {input}")
  defer: inS.close()
  let vectors = readVectorsBatch(inS, dim, int(total))
  checkFinite(vectors, 0)

  let (codeStream, subCount, codeTotal) = readCodesHeader(codesPath)
  defer: codeStream.close()
  if codeTotal != total:
    fail(&"{codesPath} has {codeTotal} codes but {input} has {total} vectors")
  if subCount != cb.subCount:
    fail(&"codes file has M={subCount} but codebook has M={cb.subCount}")
  var codes = newSeq[uint8](int(total) * subCount)
  let got = codeStream.readData(addr codes[0], codes.len)
  if got != codes.len: fail("codes file ended early")

  var rng = initRand(seed)
  let n = int(min(int64(sampleSize), total))
  var sumSq = 0.0'f64
  var sumNormSq = 0.0'f64
  var maxErr = 0.0'f64
  var chosen: seq[int]
  var seen = initHashSet[int]()
  while chosen.len < n:
    let idx = rng.rand(int(total) - 1)
    if idx notin seen:
      seen.incl idx
      chosen.add idx

  var recon = newSeq[float32](dim)
  for idx in chosen:
    decodeOne(cb, codes, idx * subCount, recon, 0)
    var errSq = 0.0'f64
    var normSq = 0.0'f64
    for j in 0 ..< dim:
      let d = float64(vectors[idx * dim + j]) - float64(recon[j])
      errSq += d * d
      normSq += float64(vectors[idx * dim + j]) * float64(vectors[idx * dim + j])
    sumSq += errSq
    sumNormSq += normSq
    maxErr = max(maxErr, sqrt(errSq))

  let rmse = sqrt(sumSq / float64(n))
  let relErr = sqrt(sumSq / max(sumNormSq, 1e-12))
  echo &"sampled {n} of {total} vectors"
  echo &"reconstruction RMSE: {rmse:.6f}"
  echo &"relative L2 error (||x-x'||/||x||, aggregate): {relErr*100:.2f}%"
  echo &"worst single vector L2 error: {maxErr:.6f}"

  # recall@k: for a small probe set, compare brute force exact top-k on the
  # original vectors against ADC top-k on the codes for the same query.
  let probes = min(50, n)
  var hits = 0
  var totalK = 0
  for p in 0 ..< probes:
    let qIdx = chosen[p]
    let qOff = qIdx * dim
    var exact = newSeq[ScoredHit](0)
    for i in 0 ..< int(total):
      if i == qIdx: continue
      exact.add ScoredHit(score: distSq(vectors, qOff, vectors, i * dim, dim), idx: int64(i))
    exact.sort(proc(a, b: ScoredHit): int = cmp(a.score, b.score))
    let k = min(topK, exact.len)
    var exactSet = initHashSet[int64]()
    for i in 0 ..< k: exactSet.incl exact[i].idx

    let table = buildDistanceTable(cb, toOpenArray(vectors, qOff, qOff + dim - 1), metL2)
    var approx: seq[ScoredHit]
    for i in 0 ..< int(total):
      if i == qIdx: continue
      approx.add ScoredHit(score: adcScore(table, codes, i * subCount, subCount, cb.clusters), idx: int64(i))
    approx.sort(proc(a, b: ScoredHit): int = cmp(a.score, b.score))
    for i in 0 ..< k:
      if approx[i].idx in exactSet: hits += 1
    totalK += k
  if probes > 0:
    let pWord = if probes == 1: "query" else: "queries"
    echo &"recall@{topK} over {probes} probe {pWord}: {float64(hits)/float64(totalK)*100:.1f}%"

# ---------------------------------------------------------------------------
# stats
# ---------------------------------------------------------------------------

proc cmdStats(codebookPath: string) =
  let cb = readCodebook(codebookPath)
  echo &"dim (D):          {cb.dim}"
  echo &"subquantizers (M): {cb.subCount}"
  echo &"subvector dim:     {cb.subDim}"
  echo &"clusters (K):      {cb.clusters}"
  echo &"bytes per code:    {cb.subCount}"
  echo &"codebook size:     {cb.centroids.len * 4} bytes"
  echo &"compression ratio: {float64(cb.dim * 4) / float64(cb.subCount):.1f}x vs raw float32"

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

proc usage() =
  echo """
Vector Product Quantizer: compress float32 embeddings into byte codes and
search them with asymmetric distance computation (ADC), no decompression
required at query time.

  vpq train  --input FILE --dim N --subquant M [--clusters 256] [--iters 25]
             [--sample 200000] [--seed 42] --out codebook.vpq
  vpq encode --input FILE --dim N --codebook codebook.vpq --out codes.vpqc
  vpq decode --codes codes.vpqc --codebook codebook.vpq --out reconstructed.f32
  vpq search --query FILE --dim N --codebook codebook.vpq --codes codes.vpqc
             [--topk 10] [--metric l2|ip] --out results.tsv
  vpq eval   --input FILE --dim N --codebook codebook.vpq --codes codes.vpqc
             [--sample 1000] [--topk 10] [--seed 42]
  vpq stats  --codebook codebook.vpq

Vector files are a flat stream of little endian float32 values, D per
vector, with no header (the same layout numpy's array.tofile() produces).
"""

proc parseArgs(): tuple[cmd: string, opts: Table[string, string]] =
  ## Accepts `--flag value` and `--flag=value` for every option, and the
  ## command name as the one bare positional argument (`vpq train ...`).
  let params = commandLineParams()
  var opts = initTable[string, string]()
  var cmd = ""
  var i = 0
  while i < params.len:
    let p = params[i]
    if p.len > 2 and p[0] == '-' and p[1] == '-':
      let body = p[2 .. ^1]
      let eq = body.find('=')
      if eq >= 0:
        opts[body[0 ..< eq]] = body[eq + 1 .. ^1]
      elif i + 1 < params.len:
        opts[body] = params[i + 1]
        inc i
      else:
        fail(&"--{body} needs a value")
    elif cmd == "":
      cmd = p
    else:
      fail(&"unexpected argument: {p}")
    inc i
  (cmd, opts)

proc need(opts: Table[string, string], key: string): string =
  if key notin opts: fail(&"missing required --{key}")
  opts[key]

proc needInt(opts: Table[string, string], key: string): int =
  parseInt(need(opts, key))

proc getInt(opts: Table[string, string], key: string, default: int): int =
  if key in opts: parseInt(opts[key]) else: default

proc getStr(opts: Table[string, string], key, default: string): string =
  if key in opts: opts[key] else: default

when isMainModule:
  let (cmd, opts) = parseArgs()
  try:
    case cmd
    of "train":
      cmdTrain(need(opts, "input"), needInt(opts, "dim"), needInt(opts, "subquant"),
                getInt(opts, "clusters", 256), getInt(opts, "iters", 25),
                getInt(opts, "sample", 200_000), int64(getInt(opts, "seed", 42)),
                need(opts, "out"))
    of "encode":
      cmdEncode(need(opts, "input"), need(opts, "codebook"), needInt(opts, "dim"), need(opts, "out"))
    of "decode":
      cmdDecode(need(opts, "codes"), need(opts, "codebook"), need(opts, "out"))
    of "search":
      let metricStr = getStr(opts, "metric", "l2")
      if metricStr != "l2" and metricStr != "ip":
        fail("--metric must be l2 or ip")
      let metric = if metricStr == "l2": metL2 else: metIP
      cmdSearch(need(opts, "query"), need(opts, "codebook"), need(opts, "codes"),
                 needInt(opts, "dim"), getInt(opts, "topk", 10), metric, need(opts, "out"))
    of "eval":
      cmdEval(need(opts, "input"), need(opts, "codebook"), need(opts, "codes"),
               needInt(opts, "dim"), getInt(opts, "sample", 1000), getInt(opts, "topk", 10),
               int64(getInt(opts, "seed", 42)))
    of "stats":
      cmdStats(need(opts, "codebook"))
    else:
      usage()
      if cmd.len > 0: quit(&"unknown command: {cmd}", 1)
  except VpqError as e:
    stderr.writeLine(&"error: {e.msg}")
    quit(1)
  except IOError as e:
    stderr.writeLine(&"error: {e.msg}")
    quit(1)
  except OSError as e:
    stderr.writeLine(&"error: {e.msg}")
    quit(1)
  except ValueError as e:
    stderr.writeLine(&"error: invalid argument: {e.msg}")
    quit(1)
