import scala.io.Source
import scala.util.Try

final case class PairSample(id: String, baseline: Double, candidate: Double, weight: Double) {
  def delta: Double = (candidate - baseline) * weight
}

final case class Options(samples: Int = 2000, seed: Long = 17L, minLift: Double = 0.0, maxRegressionRisk: Double = 0.05, markdown: Boolean = false)

object StreamingEvalBootstrap {
  def main(args: Array[String]): Unit = {
    try {
      val options = parse(args.toList)
      val data = readSamples().toVector
      require(data.nonEmpty, "no eval rows supplied")
      val result = bootstrap(data, options)
      if (options.markdown) println(renderMarkdown(result, options, data.size)) else println(renderText(result, options, data.size))
      if (result.regressionRisk > options.maxRegressionRisk || result.meanLift < options.minLift) sys.exit(2) else sys.exit(0)
    } catch {
      case e: Throwable => Console.err.println("StreamingEvalBootstrap: " + e.getMessage); sys.exit(64)
    }
  }

  def parse(args: List[String]): Options = args match {
    case Nil => Options()
    case "--samples" :: value :: tail => parse(tail).copy(samples = value.toInt)
    case "--seed" :: value :: tail => parse(tail).copy(seed = value.toLong)
    case "--min-lift" :: value :: tail => parse(tail).copy(minLift = value.toDouble)
    case "--max-regression-risk" :: value :: tail => parse(tail).copy(maxRegressionRisk = value.toDouble)
    case "--markdown" :: tail => parse(tail).copy(markdown = true)
    case other :: _ => throw new IllegalArgumentException("unknown option " + other)
  }

  def readSamples(): Iterator[PairSample] = {
    Source.stdin.getLines().zipWithIndex.filterNot { case (line, _) => line.trim.isEmpty }.filterNot(_._1.startsWith("id,")).map {
      case (line, index) =>
        val cells = line.split(",", -1).map(_.trim)
        if (cells.length < 3) throw new IllegalArgumentException(s"line ${index + 1} needs id,baseline,candidate[,weight]")
        PairSample(cells(0), number(cells(1), index), number(cells(2), index), if (cells.length > 3 && cells(3).nonEmpty) number(cells(3), index) else 1.0)
    }
  }

  def number(raw: String, index: Int): Double = Try(raw.toDouble).getOrElse(throw new IllegalArgumentException(s"bad number on line ${index + 1}"))

  final case class Result(meanLift: Double, lower: Double, upper: Double, regressionRisk: Double, pWin: Double, pTie: Double)

  def bootstrap(data: Vector[PairSample], options: Options): Result = {
    val rng = new XorShift64(options.seed)
    val means = Array.fill(options.samples)(0.0)
    var regressions = 0
    var wins = 0
    var ties = 0
    var i = 0
    while (i < options.samples) {
      var sum = 0.0
      var weight = 0.0
      var j = 0
      while (j < data.length) {
        val sample = data((rng.nextPositiveLong() % data.length).toInt)
        sum += sample.delta
        weight += sample.weight
        j += 1
      }
      val mean = if (weight == 0.0) 0.0 else sum / weight
      means(i) = mean
      if (mean < options.minLift) regressions += 1
      if (mean > 0) wins += 1 else if (mean == 0) ties += 1
      i += 1
    }
    scala.util.Sorting.quickSort(means)
    Result(mean(data), pct(means, 2.5), pct(means, 97.5), regressions.toDouble / options.samples, wins.toDouble / options.samples, ties.toDouble / options.samples)
  }

  def mean(data: Vector[PairSample]): Double = {
    val totalWeight = data.map(_.weight).sum
    if (totalWeight == 0.0) 0.0 else data.map(_.delta).sum / totalWeight
  }

  def pct(sorted: Array[Double], p: Double): Double = {
    if (sorted.isEmpty) 0.0
    else {
      val rank = (p / 100.0) * (sorted.length - 1)
      val lo = math.floor(rank).toInt
      val hi = math.ceil(rank).toInt
      sorted(lo) + (sorted(hi) - sorted(lo)) * (rank - lo)
    }
  }

  def renderText(r: Result, o: Options, n: Int): String = {
    f"rows=$n samples=${o.samples} mean_lift=${r.meanLift}%.6f ci95=[${r.lower}%.6f,${r.upper}%.6f] regression_risk=${r.regressionRisk}%.4f win_probability=${r.pWin}%.4f"
  }

  def renderMarkdown(r: Result, o: Options, n: Int): String = {
    s"""# Streaming eval bootstrap
       |
       |- Rows: $n
       |- Bootstrap samples: ${o.samples}
       |- Mean lift: ${"%.6f".format(r.meanLift)}
       |- 95% interval: [${"%.6f".format(r.lower)}, ${"%.6f".format(r.upper)}]
       |- Regression risk: ${"%.4f".format(r.regressionRisk)}
       |- Win probability: ${"%.4f".format(r.pWin)}
       |""".stripMargin
  }
}

final class XorShift64(private var state: Long) {
  if (state == 0L) state = 88172645463393265L
  def nextPositiveLong(): Long = {
    var x = state
    x ^= (x << 13)
    x ^= (x >>> 7)
    x ^= (x << 17)
    state = x
    x & Long.MaxValue
  }
}

/*
This solves the April 2026 evaluation shipping problem where LLM apps, coding agents,
retrievers, rerankers, and tool planners need to prove a candidate is better than baseline
while eval rows are still streaming from CI. Built because a single average hides paired
variance, and teams need a reproducible regression risk number before switching traffic.
Use it when CSV rows contain id, baseline score, candidate score, and optional weight from
human evals, LLM judges, retrieval metrics, latency penalties, or task success signals. The
trick: it uses deterministic paired bootstrap resampling so the answer is stable in pull
requests and can fail CI when lift is too small or regression risk is too high. Drop this into
a Scala data or platform repository as one source file and it becomes a streaming eval gate,
LLM regression risk calculator, paired bootstrap analyzer, AI release confidence checker,
and research-grade developer utility people can fork without adopting a full statistics stack.
*/
