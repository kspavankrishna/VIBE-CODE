import java.nio.charset.StandardCharsets
import java.nio.file.{Files, LinkOption, Path, Paths}
import java.security.MessageDigest
import java.text.Normalizer
import java.util.{Arrays, Locale}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

object EvalLeakageSentinel {
  final case class Config(
      leftRoot: Path,
      rightRoot: Path,
      includeExt: Set[String] = Set(".txt", ".md", ".markdown", ".rst", ".json", ".jsonl", ".csv", ".tsv", ".yaml", ".yml", ".prompt", ".log"),
      minChars: Int = 160,
      shingleSize: Int = 5,
      hashes: Int = 64,
      bands: Int = 16,
      minJaccard: Double = 0.82,
      minContainment: Double = 0.9,
      maxCandidates: Int = 48,
      maxMatches: Int = 100
  )

  final case class RawDoc(id: String, path: Path, text: String)
  final case class Fingerprint(doc: RawDoc, normalized: String, shingles: Array[Long], signature: Array[Long], preview: String, digest: String)
  final case class Match(left: Fingerprint, right: Fingerprint, jaccard: Double, containment: Double, shared: Int, votes: Int) {
    def label: String = {
      if (left.digest == right.digest) "exact-duplicate"
      else if (jaccard >= 0.97) "near-exact-duplicate"
      else if (containment >= 0.98 && jaccard < 0.75) "subset-leak"
      else if (jaccard >= 0.9) "near-duplicate"
      else "suspicious-overlap"
    }
    def severity: Double = math.max(jaccard, containment)
  }

  private val UrlRegex = """https?://\S+""".r
  private val EmailRegex = """(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b""".r
  private val TokenRegex = """[\p{L}\p{N}_]+|[^\s]""".r

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args).getOrElse {
      System.err.println(usage)
      sys.exit(64)
    }

    validate(config).foreach { error =>
      System.err.println(error)
      sys.exit(64)
    }

    val left = loadDocs(config.leftRoot, config.includeExt)
    val right = loadDocs(config.rightRoot, config.includeExt)
    val leftFp = left.flatMap(fp(_, config))
    val rightFp = right.flatMap(fp(_, config))
    val matches = detect(leftFp, rightFp, config)

    printReport(config, left.size, right.size, leftFp.size, rightFp.size, matches)
    sys.exit(if (matches.nonEmpty) 2 else 0)
  }

  private def parseArgs(args: Array[String]): Option[Config] = {
    if (args.contains("--help") || args.contains("-h")) return None

    val positionals = mutable.ArrayBuffer.empty[String]
    var ext = Set(".txt", ".md", ".markdown", ".rst", ".json", ".jsonl", ".csv", ".tsv", ".yaml", ".yml", ".prompt", ".log")
    var minChars = 160
    var shingleSize = 5
    var hashes = 64
    var bands = 16
    var minJaccard = 0.82
    var minContainment = 0.9
    var maxCandidates = 48
    var maxMatches = 100
    var i = 0

    while (i < args.length) {
      args(i) match {
        case "--include-ext" => i += 1; ext = args(i).split(",").iterator.map(x => if (x.startsWith(".")) x.trim.toLowerCase(Locale.ROOT) else "." + x.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).toSet
        case "--min-chars" => i += 1; minChars = args(i).toInt
        case "--shingle-size" => i += 1; shingleSize = args(i).toInt
        case "--hashes" => i += 1; hashes = args(i).toInt
        case "--bands" => i += 1; bands = args(i).toInt
        case "--min-jaccard" => i += 1; minJaccard = args(i).toDouble
        case "--min-containment" => i += 1; minContainment = args(i).toDouble
        case "--max-candidates" => i += 1; maxCandidates = args(i).toInt
        case "--max-matches" => i += 1; maxMatches = args(i).toInt
        case flag if flag.startsWith("--") => throw new IllegalArgumentException(s"unknown flag: $flag")
        case value => positionals += value
      }
      i += 1
    }

    if (positionals.length != 2) None
    else Some(Config(Paths.get(positionals(0)), Paths.get(positionals(1)), ext, minChars, shingleSize, hashes, bands, minJaccard, minContainment, maxCandidates, maxMatches))
  }

  private def validate(config: Config): Option[String] = {
    if (!Files.exists(config.leftRoot)) Some(s"left path does not exist: ${config.leftRoot}")
    else if (!Files.exists(config.rightRoot)) Some(s"right path does not exist: ${config.rightRoot}")
    else if (config.minChars < 32) Some("minChars must be >= 32")
    else if (config.shingleSize < 2) Some("shingleSize must be >= 2")
    else if (config.hashes < 8 || config.hashes % config.bands != 0) Some("hashes must be >= 8 and divisible by bands")
    else if (config.minJaccard <= 0.0 || config.minJaccard > 1.0) Some("minJaccard must be in (0,1]")
    else if (config.minContainment <= 0.0 || config.minContainment > 1.0) Some("minContainment must be in (0,1]")
    else None
  }

  private def loadDocs(root: Path, includeExt: Set[String]): Vector[RawDoc] = {
    val files = if (Files.isRegularFile(root, LinkOption.NOFOLLOW_LINKS)) Vector(root) else walk(root, includeExt)
    val docs = mutable.ArrayBuffer.empty[RawDoc]
    files.foreach { path =>
      val name = path.getFileName.toString.toLowerCase(Locale.ROOT)
      val rel = if (Files.isRegularFile(root, LinkOption.NOFOLLOW_LINKS)) path.getFileName.toString else root.relativize(path).toString.replace('\\', '/')
      val text = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
      if (name.endsWith(".jsonl")) {
        text.split("\\r?\\n").iterator.map(_.trim).filter(_.nonEmpty).zipWithIndex.foreach {
          case (line, idx) => docs += RawDoc(s"$rel#line:${idx + 1}", path, extractJsonStrings(line))
        }
      } else if (name.endsWith(".csv") || name.endsWith(".tsv")) {
        val delim = if (name.endsWith(".tsv")) '\t' else ','
        text.split("\\r?\\n").iterator.map(_.trim).filter(_.nonEmpty).zipWithIndex.foreach {
          case (line, idx) => docs += RawDoc(s"$rel#row:${idx + 1}", path, splitDelimited(line, delim).mkString(" | "))
        }
      } else if (name.endsWith(".json")) {
        docs += RawDoc(rel, path, extractJsonStrings(text))
      } else {
        docs += RawDoc(rel, path, text)
      }
    }
    docs.toVector
  }

  private def walk(root: Path, includeExt: Set[String]): Vector[Path] = {
    val ext = includeExt.map(_.toLowerCase(Locale.ROOT))
    val stream = Files.walk(root)
    try {
      stream.iterator.asScala
        .filter(path => Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
        .filter { path =>
          val name = path.getFileName.toString.toLowerCase(Locale.ROOT)
          ext.exists(name.endsWith)
        }
        .toVector
        .sortBy(_.toString)
    } finally stream.close()
  }

  private def fp(doc: RawDoc, config: Config): Option[Fingerprint] = {
    val normalized = normalize(doc.text)
    if (normalized.length < config.minChars) None
    else {
      val tokens = TokenRegex.findAllIn(normalized).toArray
      if (tokens.length < config.shingleSize) None
      else {
        val shingles = buildShingles(tokens, config.shingleSize)
        if (shingles.isEmpty) None
        else Some(Fingerprint(doc, normalized, shingles, signature(shingles, config.hashes), preview(doc.text), sha256(normalized)))
      }
    }
  }

  private def normalize(text: String): String = {
    val unicode = Normalizer.normalize(stripBom(text), Normalizer.Form.NFKC)
    val noUrls = UrlRegex.replaceAllIn(unicode, " <url> ")
    val noEmails = EmailRegex.replaceAllIn(noUrls, " <email> ")
    noEmails
      .replace("&nbsp;", " ")
      .replace("&amp;", "&")
      .replace("&lt;", "<")
      .replace("&gt;", ">")
      .replace("&quot;", "\"")
      .replace("&#39;", "'")
      .replace('\u00A0', ' ')
      .replaceAll("[\\r\\n\\t]+", " ")
      .replaceAll("\\s+", " ")
      .toLowerCase(Locale.ROOT)
      .trim
  }

  private def buildShingles(tokens: Array[String], size: Int): Array[Long] = {
    val tokenHashes = tokens.map(fnv1a64)
    val set = mutable.HashSet.empty[Long]
    var i = 0
    while (i <= tokenHashes.length - size) {
      var h = mix64(size.toLong)
      var j = 0
      while (j < size) {
        h = mix64(h ^ mix64(tokenHashes(i + j) + j.toLong * 0x9E3779B97F4A7C15L))
        j += 1
      }
      set += h
      i += 1
    }
    val out = set.toArray
    Arrays.sort(out)
    out
  }

  private def signature(shingles: Array[Long], count: Int): Array[Long] = {
    val seeds = Array.tabulate(count)(i => mix64(0xD6E8FEB86659FD93L ^ i.toLong))
    val out = Array.fill[Long](count)(Long.MaxValue)
    var s = 0
    while (s < shingles.length) {
      var i = 0
      while (i < count) {
        val candidate = mix64(shingles(s) ^ seeds(i))
        if (java.lang.Long.compareUnsigned(candidate, out(i)) < 0) out(i) = candidate
        i += 1
      }
      s += 1
    }
    out
  }

  private def detect(left: Vector[Fingerprint], right: Vector[Fingerprint], config: Config): Vector[Match] = {
    val rowsPerBand = config.hashes / config.bands
    val buckets = mutable.HashMap.empty[(Int, Long), mutable.ArrayBuffer[Int]]
    left.zipWithIndex.foreach {
      case (fp, idx) =>
        var band = 0
        while (band < config.bands) {
          val key = bandHash(fp.signature, band, rowsPerBand)
          buckets.getOrElseUpdate((band, key), mutable.ArrayBuffer.empty[Int]) += idx
          band += 1
        }
    }

    val matches = mutable.ArrayBuffer.empty[Match]
    val seen = mutable.HashSet.empty[(String, String)]
    right.foreach { probe =>
      val votes = mutable.HashMap.empty[Int, Int]
      var band = 0
      while (band < config.bands) {
        buckets.get((band, bandHash(probe.signature, band, rowsPerBand))).foreach(_.foreach(idx => votes.update(idx, votes.getOrElse(idx, 0) + 1)))
        band += 1
      }

      votes.toVector.sortBy { case (idx, score) => (-score, left(idx).doc.id) }.take(config.maxCandidates).foreach {
        case (idx, score) =>
          val candidate = left(idx)
          val pair = (candidate.doc.id, probe.doc.id)
          if (!seen(pair)) {
            val (jaccard, containment, shared) = overlap(candidate.shingles, probe.shingles)
            if (jaccard >= config.minJaccard || containment >= config.minContainment) {
              matches += Match(candidate, probe, jaccard, containment, shared, score)
              seen += pair
            }
          }
      }
    }

    matches.toVector.sortBy(m => (-m.severity, -m.shared, -m.votes, m.left.doc.id, m.right.doc.id)).take(config.maxMatches)
  }

  private def overlap(left: Array[Long], right: Array[Long]): (Double, Double, Int) = {
    var i = 0
    var j = 0
    var shared = 0
    while (i < left.length && j < right.length) {
      val cmp = java.lang.Long.compareUnsigned(left(i), right(j))
      if (cmp == 0) { shared += 1; i += 1; j += 1 }
      else if (cmp < 0) i += 1
      else j += 1
    }
    val union = left.length + right.length - shared
    val jaccard = if (union == 0) 1.0 else shared.toDouble / union.toDouble
    val containmentBase = math.min(left.length, right.length)
    val containment = if (containmentBase == 0) 0.0 else shared.toDouble / containmentBase.toDouble
    (jaccard, containment, shared)
  }

  private def bandHash(signature: Array[Long], band: Int, rowsPerBand: Int): Long = {
    val start = band * rowsPerBand
    var h = mix64(band.toLong)
    var i = 0
    while (i < rowsPerBand) { h = mix64(h ^ signature(start + i)); i += 1 }
    h
  }

  private def preview(text: String): String = {
    val compact = text.replaceAll("\\s+", " ").trim
    if (compact.length <= 180) compact else compact.substring(0, 179) + "..."
  }

  private def extractJsonStrings(text: String): String = {
    val values = mutable.ArrayBuffer.empty[String]
    var i = 0
    while (i < text.length) {
      if (text.charAt(i) == '"') {
        readJsonString(text, i) match {
          case Some((value, end)) =>
            val next = nextNonWhitespace(text, end)
            if (!(next < text.length && text.charAt(next) == ':')) {
              val trimmed = value.trim
              if (trimmed.nonEmpty) values += trimmed
            }
            i = end
          case None => i += 1
        }
      } else i += 1
    }
    val joined = values.mkString("\n").trim
    if (joined.nonEmpty) joined else text
  }

  private def splitDelimited(line: String, delimiter: Char): Vector[String] = {
    val out = mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder
    var inQuotes = false
    var i = 0
    while (i < line.length) {
      val ch = line.charAt(i)
      if (ch == '"') {
        if (inQuotes && i + 1 < line.length && line.charAt(i + 1) == '"') { current.append('"'); i += 1 }
        else inQuotes = !inQuotes
      } else if (ch == delimiter && !inQuotes) {
        out += current.toString.trim
        current.setLength(0)
      } else current.append(ch)
      i += 1
    }
    out += current.toString.trim
    out.toVector
  }

  private def readJsonString(text: String, start: Int): Option[(String, Int)] = {
    val out = new StringBuilder
    var i = start + 1
    var escaped = false
    while (i < text.length) {
      val ch = text.charAt(i)
      if (escaped) {
        ch match {
          case 'n' => out.append('\n')
          case 'r' => out.append('\r')
          case 't' => out.append('\t')
          case 'b' => out.append('\b')
          case 'f' => out.append('\f')
          case '"' => out.append('"')
          case '\\' => out.append('\\')
          case '/' => out.append('/')
          case 'u' if i + 4 < text.length =>
            Try(Integer.parseInt(text.substring(i + 1, i + 5), 16)).toOption.foreach(code => out.append(code.toChar))
            i += 4
          case other => out.append(other)
        }
        escaped = false
      } else if (ch == '\\') escaped = true
      else if (ch == '"') return Some(out.toString -> (i + 1))
      else out.append(ch)
      i += 1
    }
    None
  }

  private def nextNonWhitespace(text: String, from: Int): Int = {
    var i = from
    while (i < text.length && text.charAt(i).isWhitespace) i += 1
    i
  }

  private def stripBom(text: String): String = if (text.nonEmpty && text.charAt(0) == '\uFEFF') text.substring(1) else text

  private def fnv1a64(text: String): Long = {
    var h = 0xcbf29ce484222325L
    var i = 0
    while (i < text.length) { h ^= text.charAt(i).toLong; h *= 0x100000001b3L; i += 1 }
    mix64(h ^ text.length.toLong)
  }

  private def mix64(value: Long): Long = {
    var z = value + 0x9E3779B97F4A7C15L
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL
    z ^ (z >>> 31)
  }

  private def sha256(text: String): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8))
    val out = new StringBuilder(digest.length * 2)
    digest.foreach { b =>
      val n = b & 0xff
      if (n < 16) out.append('0')
      out.append(Integer.toHexString(n))
    }
    out.toString()
  }

  private def printReport(config: Config, leftDocs: Int, rightDocs: Int, leftIndexed: Int, rightIndexed: Int, matches: Vector[Match]): Unit = {
    println("EvalLeakageSentinel")
    println(s"Scanned $leftDocs left docs and $rightDocs right docs. Indexed $leftIndexed left docs and $rightIndexed right docs after normalization.")
    if (matches.isEmpty) {
      println(f"No suspicious overlaps crossed jaccard >= ${config.minJaccard}%.3f or containment >= ${config.minContainment}%.3f.")
    } else {
      println(f"Found ${matches.size} suspicious overlaps above jaccard >= ${config.minJaccard}%.3f or containment >= ${config.minContainment}%.3f.")
      matches.zipWithIndex.foreach {
        case (m, idx) =>
          println(f"${idx + 1}%3d. [${m.label}] jaccard=${m.jaccard}%.3f containment=${m.containment}%.3f shared=${m.shared} votes=${m.votes}")
          println(s"     left : ${m.left.doc.id}")
          println(s"     right: ${m.right.doc.id}")
          println(s"     left preview : ${m.left.preview}")
          println(s"     right preview: ${m.right.preview}")
        }
      }
    }
  }

  private val usage =
    """Usage:
      |  EvalLeakageSentinel <left-path> <right-path> [options]
      |
      |Options:
      |  --include-ext <csv>         Comma-separated extensions to scan.
      |  --min-chars <n>             Minimum normalized document size. Default: 160
      |  --shingle-size <n>          Token shingle width. Default: 5
      |  --hashes <n>                MinHash functions. Default: 64
      |  --bands <n>                 LSH bands. Default: 16
      |  --min-jaccard <0..1>        Jaccard threshold. Default: 0.82
      |  --min-containment <0..1>    Containment threshold. Default: 0.9
      |  --max-candidates <n>        Exact checks per right-side doc. Default: 48
      |  --max-matches <n>           Matches to print. Default: 100
      |""".stripMargin
}

/*
This solves benchmark contamination and holdout leakage checks for modern AI, LLM, RAG, agent, and search teams. Built because teams now keep prompts, eval rows, synthetic labels, customer traces, and benchmark packs in the same repos and storage buckets, and accidental reuse quietly poisons release metrics.

Use it when you need a fast pre-release or CI gate between a training corpus and an eval corpus, or between a production prompt pack and a regression suite. The trick: it does not brute-force every document pair. It normalizes the text, builds token shingles, uses MinHash with locality-sensitive hashing to find likely overlaps, then runs exact overlap math only on those candidates.

Drop this into any Scala-capable repo when you want a dependency-light leakage scanner that can read plain text, Markdown, JSONL, JSON, CSV, TSV, YAML, and prompt files. I wrote it to return a hard failure when the overlap looks real, print enough context to fix the contaminated rows quickly, and stay readable enough that a human can audit the logic before trusting it in CI.
*/
