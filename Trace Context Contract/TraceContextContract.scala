import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.Locale
import scala.collection.mutable
import scala.util.control.NonFatal

object TraceContextContract {
  final case class Source(path: String, line: Int, text: String)
  final case class Config(
      inputs: Vector[String] = Vector("-"),
      jsonOut: Option[String] = None,
      markdownOut: Option[String] = None,
      failAt: Int = 85,
      strict: Boolean = false,
      required: Set[String] = Set("service.name"),
      allowedServices: Set[String] = Set.empty,
      maxClockSkewMs: Long = 30000L,
      maxOrphanToolPercent: Double = 2.5,
      selfTest: Boolean = false
  )

  final case class Span(
      traceId: String,
      spanId: String,
      parentSpanId: Option[String],
      name: String,
      service: String,
      startNanos: Option[Long],
      endNanos: Option[Long],
      fields: Map[String, Vector[String]],
      source: Source
  ) {
    val lowerName: String = name.toLowerCase(Locale.ROOT)
    def first(keys: String*): Option[String] = TraceContextContract.first(fields, keys: _*)
    def has(keys: String*): Boolean = keys.exists(fields.contains)
  }

  final case class Issue(rule: String, score: Int, traceId: String, spanId: String, source: Source, message: String, fix: String) {
    def severity: String =
      if (score >= 90) "critical" else if (score >= 80) "high" else if (score >= 60) "medium" else "low"
  }

  final case class Result(spans: Vector[Span], parseErrors: Vector[Issue], issues: Vector[Issue], status: String, maxScore: Int)

  def main(args: Array[String]): Unit = {
    if (args.contains("--help") || args.contains("-h")) {
      println(help)
      sys.exit(0)
    }
    parseArgs(args.toVector) match {
      case Left(problem) =>
        Console.err.println(problem)
        Console.err.println(help)
        sys.exit(2)
      case Right(config) if config.selfTest =>
        sys.exit(selfTest())
      case Right(config) =>
        val result = run(config)
        val json = renderJson(result, config)
        val markdown = renderMarkdown(result, config)
        config.jsonOut.foreach(path => Files.write(Paths.get(path), json.getBytes(StandardCharsets.UTF_8)))
        config.markdownOut.foreach(path => Files.write(Paths.get(path), markdown.getBytes(StandardCharsets.UTF_8)))
        if (config.jsonOut.isEmpty && config.markdownOut.isEmpty) print(json)
        sys.exit(if (result.status == "fail") 1 else 0)
    }
  }

  def parseArgs(args: Vector[String]): Either[String, Config] = {
    def loop(rest: Vector[String], config: Config): Either[String, Config] = rest match {
      case Vector() => Right(if (config.inputs.isEmpty) config.copy(inputs = Vector("-")) else config)
      case "--self-test" +: tail => loop(tail, config.copy(selfTest = true))
      case "--strict" +: tail => loop(tail, config.copy(strict = true))
      case "--input" +: path +: tail => loop(tail, config.copy(inputs = appendInput(config.inputs, path)))
      case "--json-out" +: path +: tail => loop(tail, config.copy(jsonOut = Some(path)))
      case "--markdown-out" +: path +: tail => loop(tail, config.copy(markdownOut = Some(path)))
      case "--require-attribute" +: key +: tail => loop(tail, config.copy(required = config.required + key))
      case "--allow-service" +: service +: tail => loop(tail, config.copy(allowedServices = config.allowedServices + service))
      case "--fail-at" +: raw +: tail =>
        parseInt(raw, 1, 100, "fail-at") match {
          case Left(problem) => Left(problem)
          case Right(n) => loop(tail, config.copy(failAt = n))
        }
      case "--max-clock-skew-ms" +: raw +: tail =>
        parseLong(raw, 0L, 86400000L, "max-clock-skew-ms") match {
          case Left(problem) => Left(problem)
          case Right(n) => loop(tail, config.copy(maxClockSkewMs = n))
        }
      case "--max-orphan-tool-percent" +: raw +: tail =>
        parseDouble(raw, 0.0, 100.0, "max-orphan-tool-percent") match {
          case Left(problem) => Left(problem)
          case Right(n) => loop(tail, config.copy(maxOrphanToolPercent = n))
        }
      case flag +: _ if flag.startsWith("--") => Left("unknown option " + flag)
      case path +: tail => loop(tail, config.copy(inputs = appendInput(config.inputs, path)))
    }
    loop(args, Config(inputs = Vector.empty))
  }

  private def appendInput(inputs: Vector[String], path: String): Vector[String] =
    if (inputs == Vector("-")) Vector(path) else inputs :+ path

  private def parseInt(raw: String, min: Int, max: Int, label: String): Either[String, Int] =
    try {
      val n = raw.toInt
      if (n >= min && n <= max) Right(n) else Left(label + " must be between " + min + " and " + max)
    } catch { case NonFatal(_) => Left(label + " must be an integer") }

  private def parseLong(raw: String, min: Long, max: Long, label: String): Either[String, Long] =
    try {
      val n = raw.toLong
      if (n >= min && n <= max) Right(n) else Left(label + " must be between " + min + " and " + max)
    } catch { case NonFatal(_) => Left(label + " must be an integer") }

  private def parseDouble(raw: String, min: Double, max: Double, label: String): Either[String, Double] =
    try {
      val n = raw.toDouble
      if (n >= min && n <= max) Right(n) else Left(label + " must be between " + min + " and " + max)
    } catch { case NonFatal(_) => Left(label + " must be a number") }

  def run(config: Config): Result = {
    val parsed = config.inputs.flatMap(readLines).map(parseSpan(_, config))
    val parseErrors = parsed.collect { case Left(issue) => issue }
    val spans = parsed.collect { case Right(span) => span }
    val allIssues = (parseErrors.filter(_ => config.strict) ++ validate(spans, config)).sortBy(i => (-i.score, i.source.path, i.source.line, i.rule))
    val maxScore = if (allIssues.isEmpty) 0 else allIssues.map(_.score).max
    val status = if (maxScore >= config.failAt) "fail" else if (allIssues.nonEmpty) "warn" else "pass"
    Result(spans, parseErrors, allIssues, status, maxScore)
  }

  private def readLines(path: String): Vector[Source] = {
    val reader =
      if (path == "-") new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))
      else Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8)
    try {
      val out = Vector.newBuilder[Source]
      var lineNo = 1
      var line = reader.readLine()
      while (line != null) {
        if (line.trim.nonEmpty) out += Source(path, lineNo, line)
        lineNo += 1
        line = reader.readLine()
      }
      out.result()
    } finally reader.close()
  }

  private def parseSpan(source: Source, config: Config): Either[Issue, Span] =
    JsonPairs.validateObject(source.text) match {
      case Some(problem) =>
        Left(issue("TCC001", 72, "", "", source, "line is not valid span JSON: " + problem, "Emit one JSON object per span in JSONL."))
      case None =>
        val fields = JsonPairs.collect(source.text)
        val trace = first(fields, "traceId", "trace_id", "trace.id").getOrElse("")
        val span = first(fields, "spanId", "span_id", "span.id").getOrElse("")
        val parent = first(fields, "parentSpanId", "parent_span_id", "parent.span.id").filter(_.nonEmpty)
        val name = first(fields, "name", "span.name", "operation").getOrElse("")
        val service = first(fields, "service.name", "serviceName", "service_name", "resource.service.name").getOrElse("")
        val start = first(fields, "startTimeUnixNano", "start_time_unix_nano", "start_nanos", "start").flatMap(toNanos)
        val end = first(fields, "endTimeUnixNano", "end_time_unix_nano", "end_nanos", "end").flatMap(toNanos)
        if (trace.isEmpty) Left(issue("TCC002", 78, "", span, source, "missing trace id", "Preserve traceId or trace_id in the export."))
        else if (span.isEmpty) Left(issue("TCC003", 78, trace, "", source, "missing span id", "Preserve spanId or span_id in the export."))
        else if (name.isEmpty) Left(issue("TCC004", 68, trace, span, source, "missing span name", "Keep a stable operation name on every span."))
        else if (config.allowedServices.nonEmpty && service.nonEmpty && !config.allowedServices.contains(service)) {
          Left(issue("TCC005", 70, trace, span, source, "service " + service + " is not allowed", "Pass --allow-service for every expected producer or fix routing."))
        } else Right(Span(trace, span, parent, name, service, start, end, fields, source))
    }

  private def validate(spans: Vector[Span], config: Config): Vector[Issue] = {
    val out = Vector.newBuilder[Issue]
    spans.foreach { span =>
      config.required.foreach { key =>
        if (first(span.fields, key).isEmpty && !(key == "service.name" && span.service.nonEmpty)) {
          out += issue("TCC010", 54, span, "missing required attribute " + key, "Add " + key + " to the span contract or narrow --require-attribute.")
        }
      }
      if (span.startNanos.isEmpty || span.endNanos.isEmpty) out += issue("TCC020", 58, span, "missing start or end timestamp", "Export startTimeUnixNano and endTimeUnixNano.")
      for (start <- span.startNanos; end <- span.endNanos if end < start) {
        out += issue("TCC021", 88, span, "span end time is before start time", "Normalize clocks before merging spans.")
      }
      aiChecks(span, out)
      payloadChecks(span, out)
      retryChecks(span, out)
    }
    spans.groupBy(_.traceId).values.foreach(trace => topologyChecks(trace, config, out))
    out.result()
  }

  private def aiChecks(span: Span, out: mutable.Builder[Issue, Vector[Issue]]): Unit = {
    val keys = span.fields.keySet
    val ai = containsAny(span.lowerName, "llm", "model", "completion", "embedding", "rerank", "agent", "tool") ||
      keys.exists(k => k.startsWith("gen_ai.") || k.startsWith("llm.") || k.startsWith("ai."))
    if (ai) {
      if (!span.has("gen_ai.request.model", "gen_ai.response.model", "llm.model", "ai.model", "model")) out += issue("TCC030", 68, span, "AI span has no model evidence", "Emit the model name at the inference boundary.")
      if (!span.has("gen_ai.system", "llm.provider", "ai.provider", "provider")) out += issue("TCC031", 62, span, "AI span has no provider evidence", "Emit provider or runtime for routing and incident review.")
      if (!span.has("gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens", "llm.usage.prompt_tokens", "llm.usage.completion_tokens", "ai.tokens.input", "ai.tokens.output")) out += issue("TCC032", 64, span, "AI span has no token usage evidence", "Capture input and output tokens before sampling.")
      if (!span.has("ai.cost.usd", "gen_ai.usage.cost_usd", "llm.cost.usd", "billing.cost_usd")) out += issue("TCC033", 46, span, "AI span has no cost estimate", "Attach a cost estimate while model, region, and token counts are available.")
    }
  }

  private def payloadChecks(span: Span, out: mutable.Builder[Issue, Vector[Issue]]): Unit =
    span.fields.foreach { case (key, values) =>
      val lowerKey = key.toLowerCase(Locale.ROOT)
      if (containsAny(lowerKey, "prompt", "completion", "body", "message")) {
        values.foreach { value =>
          val lower = value.toLowerCase(Locale.ROOT)
          val sensitive = containsAny(lower, "api_key", "password", "secret", "bearer ", "authorization:", "-----begin")
          val redacted = containsAny(lower, "[redacted]", "<redacted>", "***", "sha256:")
          if (sensitive && !redacted) out += issue("TCC040", 92, span, "attribute " + key + " appears to contain unredacted sensitive payload text", "Store hashes, labels, or redacted excerpts instead of raw prompt bodies.")
          else if (value.length > 16000 && !redacted) out += issue("TCC041", 66, span, "attribute " + key + " stores a large raw payload", "Move large payloads to controlled evidence storage.")
        }
      }
    }

  private def retryChecks(span: Span, out: mutable.Builder[Issue, Vector[Issue]]): Unit =
    if (containsAny(span.lowerName, "retry", "replay", "resume", "idempot") &&
        !span.has("idempotency_key", "idempotency.key", "ai.idempotency_key", "tool.idempotency_key", "request.idempotency_key")) {
      out += issue("TCC050", 79, span, "retry or replay span has no idempotency key", "Emit the idempotency key used at the provider, queue, or tool boundary.")
    }

  private def topologyChecks(trace: Vector[Span], config: Config, out: mutable.Builder[Issue, Vector[Issue]]): Unit = {
    val byId = trace.map(s => s.spanId -> s).toMap
    val duplicates = trace.groupBy(_.spanId).filter(_._2.size > 1).keySet
    trace.foreach { span =>
      if (duplicates.contains(span.spanId)) out += issue("TCC060", 91, span, "duplicate span id inside trace", "Ensure span ids are unique per trace after merging exporters.")
      span.parentSpanId.foreach { parent =>
        if (!byId.contains(parent)) out += issue("TCC061", 73, span, "parent span " + parent + " is missing", "Export complete trace envelopes or mark sampled parents.")
      }
      for {
        parentId <- span.parentSpanId
        parent <- byId.get(parentId)
        childStart <- span.startNanos
        childEnd <- span.endNanos
        parentStart <- parent.startNanos
        parentEnd <- parent.endNanos
      } {
        val earlyMs = (parentStart - childStart) / 1000000L
        val lateMs = (childEnd - parentEnd) / 1000000L
        if (earlyMs > config.maxClockSkewMs) out += issue("TCC080", 67, span, "child span starts " + earlyMs + "ms before its parent", "Normalize clocks before joining traces.")
        if (lateMs > config.maxClockSkewMs && !containsAny(span.lowerName, "async", "queue", "callback")) out += issue("TCC082", 53, span, "child span ends " + lateMs + "ms after its parent", "Model async work explicitly.")
      }
    }
    val toolSpans = trace.filter(s => containsAny(s.lowerName, "tool", "function", "mcp") || s.fields.keys.exists(_.contains("tool.call")))
    if (toolSpans.nonEmpty) {
      val orphaned = toolSpans.filterNot(_.has("tool.call.id", "tool_call_id", "mcp.tool.call_id", "gen_ai.tool.call.id", "function.call.id"))
      val percent = orphaned.size.toDouble * 100.0 / toolSpans.size.toDouble
      orphaned.foreach { span =>
        out += issue("TCC070", if (percent > config.maxOrphanToolPercent) 84 else 57, span, "tool or MCP span has no stable call id", "Use one tool call id across assistant delta, execution, result, and retry spans.")
      }
    }
  }

  object JsonPairs {
    def validateObject(text: String): Option[String] = {
      val trimmed = text.trim
      if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) return Some("record is not a JSON object")
      var i = 0
      var string = false
      var escape = false
      val stack = mutable.Stack[Char]()
      while (i < text.length) {
        val c = text.charAt(i)
        if (string) {
          if (escape) escape = false
          else if (c == '\\') escape = true
          else if (c == '"') string = false
        } else if (c == '"') string = true
        else if (c == '{' || c == '[') stack.push(c)
        else if (c == '}' || c == ']') {
          if (stack.isEmpty) return Some("extra closing " + c)
          val open = stack.pop()
          if ((open == '{' && c != '}') || (open == '[' && c != ']')) return Some("mismatched " + open + " and " + c)
        }
        i += 1
      }
      if (string) Some("unterminated string") else if (stack.nonEmpty) Some("unclosed " + stack.top) else None
    }

    def collect(text: String): Map[String, Vector[String]] = {
      val scanner = new Scanner(text)
      scanner.scan()
    }

    private final class Scanner(text: String) {
      private var i = 0
      private val out = mutable.LinkedHashMap.empty[String, Vector[String]]

      def scan(): Map[String, Vector[String]] = {
        while (i < text.length) {
          if (text.charAt(i) == '"') {
            val save = i
            readString() match {
              case Some(key) =>
                skipWs()
                if (i < text.length && text.charAt(i) == ':') {
                  i += 1
                  readValue().foreach(value => out.put(key, out.getOrElse(key, Vector.empty) :+ value))
                }
              case None => i = save + 1
            }
          } else i += 1
        }
        out.toMap
      }

      private def readValue(): Option[String] = {
        skipWs()
        if (i >= text.length) None
        else if (text.charAt(i) == '"') readString()
        else {
          val start = i
          while (i < text.length && !",}] \r\n\t".contains(text.charAt(i))) i += 1
          val raw = text.substring(start, i).trim
          if (raw.isEmpty) None else Some(raw)
        }
      }

      private def readString(): Option[String] = {
        if (i >= text.length || text.charAt(i) != '"') return None
        i += 1
        val b = new StringBuilder
        var escape = false
        while (i < text.length) {
          val c = text.charAt(i)
          i += 1
          if (escape) {
            b.append(c match {
              case '"' => '"'
              case '\\' => '\\'
              case '/' => '/'
              case 'b' => '\b'
              case 'f' => '\f'
              case 'n' => '\n'
              case 'r' => '\r'
              case 't' => '\t'
              case other => other
            })
            escape = false
          } else if (c == '\\') escape = true
          else if (c == '"') return Some(b.toString)
          else b.append(c)
        }
        None
      }

      private def skipWs(): Unit =
        while (i < text.length && text.charAt(i).isWhitespace) i += 1
    }
  }

  private def first(fields: Map[String, Vector[String]], keys: String*): Option[String] =
    keys.iterator.map(k => fields.get(k).flatMap(_.headOption)).collectFirst { case Some(value) => value }

  private def toNanos(raw: String): Option[Long] =
    try Some(raw.toLong)
    catch { case NonFatal(_) =>
      try {
        val instant = Instant.parse(raw)
        Some(instant.getEpochSecond * 1000000000L + instant.getNano)
      } catch { case NonFatal(_) => None }
    }

  private def containsAny(text: String, words: String*): Boolean = words.exists(text.contains)
  private def issue(rule: String, score: Int, span: Span, message: String, fix: String): Issue =
    issue(rule, score, span.traceId, span.spanId, span.source, message, fix)
  private def issue(rule: String, score: Int, traceId: String, spanId: String, source: Source, message: String, fix: String): Issue =
    Issue(rule, score, traceId, spanId, source, message, fix)

  def renderJson(result: Result, config: Config): String = {
    val body = Vector(
      "generated_at" -> quote(Instant.now.toString),
      "status" -> quote(result.status),
      "max_score" -> result.maxScore.toString,
      "fail_at" -> config.failAt.toString,
      "spans_scanned" -> result.spans.size.toString,
      "traces_scanned" -> result.spans.map(_.traceId).distinct.size.toString,
      "services_scanned" -> result.spans.map(_.service).filter(_.nonEmpty).distinct.size.toString,
      "parse_errors" -> result.parseErrors.size.toString,
      "issues" -> result.issues.map(issueJson).mkString("[", ",", "]")
    ).map { case (k, v) => quote(k) + ":" + v }.mkString("{", ",", "}")
    body + "\n"
  }

  private def issueJson(i: Issue): String =
    Vector(
      "rule" -> quote(i.rule),
      "severity" -> quote(i.severity),
      "score" -> i.score.toString,
      "trace_id" -> quote(i.traceId),
      "span_id" -> quote(i.spanId),
      "file" -> quote(i.source.path),
      "line" -> i.source.line.toString,
      "message" -> quote(i.message),
      "fix" -> quote(i.fix)
    ).map { case (k, v) => quote(k) + ":" + v }.mkString("{", ",", "}")

  def renderMarkdown(result: Result, config: Config): String = {
    val b = new StringBuilder
    b.append("# TraceContextContract Report\n\n")
    b.append("- Status: ").append(result.status).append('\n')
    b.append("- Max score: ").append(result.maxScore).append(" / fail at ").append(config.failAt).append('\n')
    b.append("- Spans scanned: ").append(result.spans.size).append('\n')
    b.append("- Parse errors: ").append(result.parseErrors.size).append("\n\n")
    b.append("| Severity | Score | Rule | Location | Trace | Span | Finding | Fix |\n")
    b.append("| --- | ---: | --- | --- | --- | --- | --- | --- |\n")
    result.issues.take(200).foreach { i =>
      b.append("| ").append(cell(i.severity)).append(" | ").append(i.score).append(" | ").append(cell(i.rule)).append(" | ")
        .append(cell(i.source.path + ":" + i.source.line)).append(" | ").append(cell(i.traceId)).append(" | ")
        .append(cell(i.spanId)).append(" | ").append(cell(i.message)).append(" | ").append(cell(i.fix)).append(" |\n")
    }
    if (result.issues.isEmpty) b.append("| pass | 0 | - | - | - | - | No contract issues found. | Keep this gate in CI. |\n")
    b.toString
  }

  private def quote(text: String): String = {
    val b = new StringBuilder(text.length + 8)
    b.append('"')
    text.foreach {
      case '"' => b.append("\\\"")
      case '\\' => b.append("\\\\")
      case '\n' => b.append("\\n")
      case '\r' => b.append("\\r")
      case '\t' => b.append("\\t")
      case c if c < ' ' => b.append("\\u%04x".format(c.toInt))
      case c => b.append(c)
    }
    b.append('"').toString
  }

  private def cell(text: String): String = text.replace("|", "\\|").replaceAll("\\s+", " ").take(180)

  def selfTest(): Int = {
    val good = Source("selftest.jsonl", 1, """{"traceId":"0123456789abcdef0123456789abcdef","spanId":"0123456789abcdef","name":"llm completion","startTimeUnixNano":10,"endTimeUnixNano":20,"attributes":{"service.name":"agent-api","gen_ai.request.model":"gpt-5.1","gen_ai.system":"openai","gen_ai.usage.input_tokens":10,"gen_ai.usage.output_tokens":5,"ai.cost.usd":0.001}}""")
    val bad = Source("selftest.jsonl", 2, """{"traceId":"abc","spanId":"tool1","parentSpanId":"missing","name":"mcp tool retry","startTimeUnixNano":30,"endTimeUnixNano":20,"attributes":{"service.name":"agent-api","prompt.body":"Authorization: Bearer secret"}}""")
    val parsed = Vector(good, bad).map(parseSpan(_, Config(strict = true)))
    val spans = parsed.collect { case Right(span) => span }
    val rules = validate(spans, Config(strict = true)).map(_.rule).toSet
    val ok = spans.size == 2 && Set("TCC021", "TCC030", "TCC040", "TCC050", "TCC061", "TCC070").subsetOf(rules)
    if (ok) { println("TraceContextContract self-test passed"); 0 }
    else { Console.err.println("TraceContextContract self-test failed: " + rules.toVector.sorted.mkString(",")); 1 }
  }

  val help: String =
    """TraceContextContract.scala
      |
      |Validate AI agent and OpenTelemetry span JSONL before traces become incident evidence.
      |
      |Usage:
      |  scala TraceContextContract.scala [options] trace.jsonl [more.jsonl]
      |  cat trace.jsonl | scala TraceContextContract.scala --json-out report.json -
      |
      |Options:
      |  --input PATH                  Read JSONL span input. Repeatable. Default: stdin.
      |  --json-out PATH               Write machine-readable JSON report.
      |  --markdown-out PATH           Write Markdown report for PR comments or incident notes.
      |  --fail-at N                   Exit 1 when max score is at least N. Default: 85.
      |  --strict                      Promote malformed JSONL records into failing issues.
      |  --require-attribute NAME      Require an attribute on every parsed span. Repeatable.
      |  --allow-service NAME          Restrict accepted service.name values. Repeatable.
      |  --max-clock-skew-ms N         Allowed parent-child clock skew. Default: 30000.
      |  --max-orphan-tool-percent N   High-severity threshold for missing tool call ids. Default: 2.5.
      |  --self-test                   Run built-in fixture checks.
      |  --help                        Show help.
      |""".stripMargin
}

/*
This solves the April 2026 problem where AI agent traces look complete in dashboards but break during incident review because the span data is missing model names, provider evidence, token counts, cost estimates, tool call ids, retry idempotency keys, parent spans, or redaction proof. Built because Pavan keeps seeing OpenTelemetry, MCP servers, streaming LLM calls, queues, and internal agent frameworks wired together without a shared trace contract, so the team finds out too late that a trace cannot explain which model ran, what it cost, why a tool executed twice, or whether a prompt leaked a secret. Use it when you need a Scala OpenTelemetry JSONL validator, AI agent observability contract gate, LLM cost attribution checker, MCP tool-call correlation auditor, prompt payload redaction scanner, or CI-ready trace quality firewall before production deploys. The trick: this file stays dependency-free, scans common OTel and GenAI field spellings, scores the evidence gaps that matter operationally, and writes JSON or Markdown that can fail CI or drop straight into a pull request. Drop this into a backend, research platform, DevOps repository, data pipeline, or internal AI tooling repo and run it on exported spans before releases, migrations, eval jobs, retros, and invoice reconciliation.
*/
