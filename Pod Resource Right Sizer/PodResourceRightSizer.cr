require "json"
require "option_parser"
require "set"

module PodResourceRightSizer
  VERSION = "1.0.0"

  MIB                       = 1024.0 * 1024.0
  DEFAULT_MIN_SAMPLES       = 200
  DEFAULT_MIN_SPAN_HOURS    = 24.0
  DEFAULT_WARMUP_SECONDS    = 120_i64
  DEFAULT_CPU_QUANTILE      = 0.95
  DEFAULT_CPU_HEADROOM      = 0.20
  DEFAULT_MEM_QUANTILE      = 0.99
  DEFAULT_MEM_HEADROOM      = 0.15
  DEFAULT_PEAK_HEADROOM     = 0.10
  DEFAULT_OOM_BUMP          = 0.25
  DEFAULT_DEADBAND          = 0.10
  DEFAULT_MAX_DECREASE      = 0.50
  DEFAULT_MAX_RESTARTS      = 5
  DEFAULT_MAX_BAD_FRACTION  = 0.05
  CENSOR_RATIO              = 0.97
  MIN_CPU_MILLICORES        = 10.0
  MIN_MEMORY_BYTES          = 32.0 * MIB
  MEMORY_STEP_BYTES         = 16.0 * MIB
  GAP_FACTOR                = 3.0
  REPLICA_WINDOW_SECONDS    = 3600_i64

  EXIT_OK        = 0
  EXIT_RISK      = 1
  EXIT_USAGE     = 2
  EXIT_BAD_INPUT = 3

  # One scrape of one container in one pod. Nilable fields are optional in the input.
  class RawSample
    include JSON::Serializable

    property ts : Int64
    property namespace : String
    property workload : String
    property container : String
    property pod : String = ""
    property cpu_millicores : Float64
    property memory_bytes : Float64
    property cpu_request_millicores : Float64?
    property memory_request_bytes : Float64?
    property memory_limit_bytes : Float64?
    property oom_killed : Bool = false
    property restarts : Int32 = 0
    property revision : String = ""
  end

  class Config
    property min_samples : Int32 = DEFAULT_MIN_SAMPLES
    property min_span_hours : Float64 = DEFAULT_MIN_SPAN_HOURS
    property warmup_seconds : Int64 = DEFAULT_WARMUP_SECONDS
    property cpu_quantile : Float64 = DEFAULT_CPU_QUANTILE
    property cpu_headroom : Float64 = DEFAULT_CPU_HEADROOM
    property mem_quantile : Float64 = DEFAULT_MEM_QUANTILE
    property mem_headroom : Float64 = DEFAULT_MEM_HEADROOM
    property peak_headroom : Float64 = DEFAULT_PEAK_HEADROOM
    property oom_bump : Float64 = DEFAULT_OOM_BUMP
    property deadband : Float64 = DEFAULT_DEADBAND
    property max_decrease : Float64 = DEFAULT_MAX_DECREASE
    property max_restarts : Int32 = DEFAULT_MAX_RESTARTS
    property max_bad_fraction : Float64 = DEFAULT_MAX_BAD_FRACTION
    property namespace_filter : String? = nil
    property format : String = "table"
    property input : String = "-"
    property fail_on_risk : Bool = false

    def validate!
      raise ArgumentError.new("cpu quantile must be in (0, 1]") unless cpu_quantile > 0.0 && cpu_quantile <= 1.0
      raise ArgumentError.new("memory quantile must be in (0, 1]") unless mem_quantile > 0.0 && mem_quantile <= 1.0
      raise ArgumentError.new("headroom values must be zero or more") if cpu_headroom < 0.0 || mem_headroom < 0.0 || peak_headroom < 0.0 || oom_bump < 0.0
      raise ArgumentError.new("deadband must be in [0, 1)") unless deadband >= 0.0 && deadband < 1.0
      raise ArgumentError.new("max decrease must be in (0, 1]") unless max_decrease > 0.0 && max_decrease <= 1.0
      raise ArgumentError.new("min samples must be at least 2") if min_samples < 2
      raise ArgumentError.new("format must be table or json") unless {"table", "json"}.includes?(format)
    end
  end

  class Recommendation
    property key : String
    property status : String = "ok"
    property notes : Array(String) = [] of String
    property samples : Int32 = 0
    property pods : Int32 = 0
    property replicas : Int32 = 1
    property span_hours : Float64 = 0.0
    property cpu_current : Float64? = nil
    property cpu_target : Float64? = nil
    property cpu_action : String = "none"
    property mem_current : Float64? = nil
    property mem_target : Float64? = nil
    property mem_action : String = "none"
    property urgent : Bool = false
    property cpu_saving_millicores : Float64 = 0.0
    property mem_saving_bytes : Float64 = 0.0

    def initialize(@key : String)
    end

    def write(json : JSON::Builder)
      json.object do
        json.field "workload", key
        json.field "status", status
        json.field "urgent", urgent
        json.field "samples", samples
        json.field "pods", pods
        json.field "replicas", replicas
        json.field "span_hours", span_hours.round(2)
        json.field "cpu_millicores" do
          json.object do
            json.field "current", cpu_current
            json.field "recommended", cpu_target
            json.field "action", cpu_action
          end
        end
        json.field "memory_bytes" do
          json.object do
            json.field "current", mem_current
            json.field "recommended", mem_target
            json.field "action", mem_action
          end
        end
        json.field "saving_cpu_millicores_total", cpu_saving_millicores
        json.field "saving_memory_bytes_total", mem_saving_bytes
        json.field "notes", notes
      end
    end
  end

  class LoadStats
    property lines : Int32 = 0
    property bad : Int32 = 0
    property first_bad : Array(String) = [] of String
  end

  # Reads JSONL. A malformed line is counted and skipped, never fatal on its own, because
  # metric exports routinely contain a truncated last line. The caller decides on the bad fraction.
  def self.load(io : IO, cfg : Config, stats : LoadStats) : Hash(String, Array(RawSample))
    groups = Hash(String, Array(RawSample)).new
    io.each_line do |line|
      stripped = line.strip
      next if stripped.empty?
      stats.lines += 1
      begin
        s = RawSample.from_json(stripped)
        unless valid_sample?(s)
          raise ArgumentError.new("negative or non finite value")
        end
        if (f = cfg.namespace_filter) && s.namespace != f
          next
        end
        key = "#{s.namespace}/#{s.workload}/#{s.container}"
        (groups[key] ||= [] of RawSample) << s
      rescue ex
        stats.bad += 1
        stats.first_bad << "line #{stats.lines}: #{ex.message.to_s.lines.first? || "parse error"}" if stats.first_bad.size < 5
      end
    end
    groups
  end

  def self.valid_sample?(s : RawSample) : Bool
    return false unless s.cpu_millicores.finite? && s.memory_bytes.finite?
    return false if s.cpu_millicores < 0.0 || s.memory_bytes < 0.0
    return false if s.ts < 0
    true
  end

  def self.median(values : Array(Float64)) : Float64
    return 0.0 if values.empty?
    sorted = values.sort
    sorted[sorted.size // 2]
  end

  # Quantile where each sample counts for the time it represents. A scrape that stood for
  # 15 seconds weighs less than one that stood for 60, so irregular scraping cannot skew p95.
  def self.weighted_quantile(pairs : Array({Float64, Float64}), q : Float64) : Float64
    return 0.0 if pairs.empty?
    sorted = pairs.sort_by { |pair| pair[0] }
    total = sorted.sum { |pair| pair[1] }
    return sorted.last[0] if total <= 0.0
    target = q * total
    acc = 0.0
    sorted.each do |pair|
      acc += pair[1]
      return pair[0] if acc >= target
    end
    sorted.last[0]
  end

  def self.cpu_step(v : Float64) : Float64
    if v < 100.0
      5.0
    elsif v < 1000.0
      10.0
    else
      50.0
    end
  end

  def self.round_up(v : Float64, step : Float64) : Float64
    (v / step).ceil * step
  end

  def self.analyze(key : String, raw : Array(RawSample), cfg : Config) : Recommendation
    rec = Recommendation.new(key)

    # Duplicate scrapes of the same pod at the same second would double count.
    seen = Set({String, Int64}).new
    samples = raw.select { |s| seen.add?({s.pod, s.ts}) }.sort_by(&.ts)
    rec.samples = samples.size

    if samples.empty?
      rec.status = "insufficient_data"
      return rec
    end

    # Only the newest revision describes what the code does now. Older revisions are used
    # only when the newest one has not run long enough to size from.
    latest_rev = samples.last.revision
    revisions = samples.map(&.revision).uniq
    if revisions.size > 1
      current = samples.select { |s| s.revision == latest_rev }
      if current.size >= cfg.min_samples
        rec.notes << "sized from revision #{latest_rev.empty? ? "(unlabeled)" : latest_rev} only, #{samples.size - current.size} older samples ignored"
        samples = current
      else
        rec.notes << "newest revision has only #{current.size} samples, older revisions included so the result may be stale"
      end
    end

    # Drop each pod's startup window. JIT, cache fill and migrations inflate early usage.
    by_pod = Hash(String, Array(RawSample)).new
    samples.each { |s| (by_pod[s.pod] ||= [] of RawSample) << s }
    trimmed = [] of RawSample
    by_pod.each_value do |list|
      first_ts = list.first.ts
      kept = list.select { |s| s.ts >= first_ts + cfg.warmup_seconds }
      trimmed.concat(kept.empty? ? list : kept)
    end
    samples = trimmed.sort_by(&.ts)
    by_pod = Hash(String, Array(RawSample)).new
    samples.each { |s| (by_pod[s.pod] ||= [] of RawSample) << s }

    rec.samples = samples.size
    rec.pods = by_pod.size
    span = (samples.last.ts - samples.first.ts).to_f
    rec.span_hours = span / 3600.0

    if samples.size < cfg.min_samples || rec.span_hours < cfg.min_span_hours
      rec.status = "insufficient_data"
      rec.notes << "need at least #{cfg.min_samples} samples over #{cfg.min_span_hours} hours, have #{samples.size} over #{rec.span_hours.round(1)}"
      return rec
    end

    # Per pod gaps decide weights. A gap above the cap is treated as missing data, not as
    # a long stretch of the last observed value.
    all_deltas = [] of Float64
    by_pod.each_value do |list|
      list.each_cons(2) { |pair| all_deltas << (pair[1].ts - pair[0].ts).to_f }
    end
    med = median(all_deltas)
    med = 1.0 if med <= 0.0
    cap = med * 2.0
    gap_count = all_deltas.count { |d| d > med * GAP_FACTOR }
    if !all_deltas.empty? && gap_count.to_f / all_deltas.size > 0.20
      rec.notes << "more than 20 percent of scrape intervals are gaps, percentiles are less trustworthy"
    end

    cpu_pairs = [] of {Float64, Float64}
    mem_pairs = [] of {Float64, Float64}
    by_pod.each_value do |list|
      list.each_with_index do |s, i|
        w = i + 1 < list.size ? Math.min((list[i + 1].ts - s.ts).to_f, cap) : med
        w = med if w <= 0.0
        cpu_pairs << {s.cpu_millicores, w}
        mem_pairs << {s.memory_bytes, w}
      end
    end

    peak_mem = samples.max_of(&.memory_bytes)
    peak_cpu = samples.max_of(&.cpu_millicores)

    # Restart accounting per pod. Counters can reset when a pod is replaced, so only
    # positive deltas count.
    restarts = 0
    by_pod.each_value do |list|
      list.each_cons(2) do |pair|
        d = pair[1].restarts - pair[0].restarts
        restarts += d if d > 0
      end
    end

    oom = samples.any?(&.oom_killed)
    limit = samples.last.memory_limit_bytes
    limit = samples.compact_map(&.memory_limit_bytes).max? if limit.nil? || limit <= 0.0
    censored = false
    if (l = limit) && l > 0.0
      censored = samples.any? { |s| s.memory_bytes >= l * CENSOR_RATIO }
    end

    if restarts > cfg.max_restarts && !oom
      rec.status = "unstable"
      rec.notes << "#{restarts} restarts without an OOM flag, usage during crash loops is biased low, fix stability before resizing"
    end

    # CPU: a time weighted quantile plus headroom. No CPU limit is ever suggested because
    # a limit causes CFS throttling long before the node is actually busy.
    cpu_q = weighted_quantile(cpu_pairs, cfg.cpu_quantile)
    cpu_raw = Math.max(cpu_q * (1.0 + cfg.cpu_headroom), MIN_CPU_MILLICORES)
    cpu_target = round_up(cpu_raw, cpu_step(cpu_raw))
    if peak_cpu > cpu_target * 4.0
      rec.notes << "cpu peak #{peak_cpu.round(0)}m is more than 4x the recommendation, bursty workload, check for throttling sensitive latency"
    end

    # Memory: the request equals the limit, so the pod is Guaranteed for memory and the
    # kernel never has to guess. Usage at or near the limit is censored: real demand is
    # unknown, so the target is pushed up instead of trusting the clipped numbers.
    mem_q = weighted_quantile(mem_pairs, cfg.mem_quantile)
    mem_raw = Math.max(mem_q * (1.0 + cfg.mem_headroom), peak_mem * (1.0 + cfg.peak_headroom))
    if oom || censored
      basis = Math.max(limit || 0.0, peak_mem)
      mem_raw = Math.max(mem_raw, basis * (1.0 + cfg.oom_bump))
      rec.notes << (oom ? "OOM kill observed, memory raised #{(cfg.oom_bump * 100).round(0)}% above the limit" : "usage reached #{(CENSOR_RATIO * 100).round(0)}% of the limit, demand is censored, memory raised")
      rec.urgent = true
    end
    mem_target = round_up(Math.max(mem_raw, MIN_MEMORY_BYTES), MEMORY_STEP_BYTES)

    rec.cpu_current = samples.reverse_each.find { |s| s.cpu_request_millicores }.try(&.cpu_request_millicores)
    rec.mem_current = samples.reverse_each.find { |s| s.memory_request_bytes }.try(&.memory_request_bytes)

    replica_cut = samples.last.ts - REPLICA_WINDOW_SECONDS
    rec.replicas = Math.max(1, samples.select { |s| s.ts >= replica_cut }.map(&.pod).uniq.size)

    decide(rec, cfg, cpu_target, mem_target, oom || censored)
    rec
  end

  # Turns a raw target into an action. Increases apply in full. Decreases are limited to
  # max_decrease per run, so a quiet week cannot strip a service before a busy one.
  def self.decide(rec : Recommendation, cfg : Config, cpu_target : Float64, mem_target : Float64, memory_at_risk : Bool)
    unstable = rec.status == "unstable"

    cur = rec.cpu_current
    if cur.nil? || cur <= 0.0
      rec.cpu_target = cpu_target
      rec.cpu_action = "set"
    else
      ratio = cpu_target / cur
      if ratio > 1.0 + cfg.deadband
        rec.cpu_target = cpu_target
        rec.cpu_action = "increase"
        rec.urgent = true if ratio > 1.5 && !unstable
      elsif ratio < 1.0 - cfg.deadband && !unstable
        floor = cur * (1.0 - cfg.max_decrease)
        applied = Math.max(cpu_target, floor)
        rec.cpu_target = round_up(applied, cpu_step(applied))
        rec.cpu_action = applied > cpu_target ? "decrease_stepped" : "decrease"
        rec.cpu_saving_millicores = (cur - rec.cpu_target.not_nil!) * rec.replicas
      else
        rec.cpu_target = cur
        rec.cpu_action = "keep"
      end
    end

    cur = rec.mem_current
    if cur.nil? || cur <= 0.0
      rec.mem_target = mem_target
      rec.mem_action = "set"
    else
      ratio = mem_target / cur
      if ratio > 1.0 + cfg.deadband
        rec.mem_target = mem_target
        rec.mem_action = "increase"
        rec.urgent = true if ratio > 1.5 && !unstable
      elsif ratio < 1.0 - cfg.deadband && !unstable && !memory_at_risk
        floor = cur * (1.0 - cfg.max_decrease)
        applied = Math.max(mem_target, floor)
        rec.mem_target = round_up(applied, MEMORY_STEP_BYTES)
        rec.mem_action = applied > mem_target ? "decrease_stepped" : "decrease"
        rec.mem_saving_bytes = (cur - rec.mem_target.not_nil!) * rec.replicas
      else
        rec.mem_target = cur
        rec.mem_action = "keep"
      end
    end

    if unstable
      rec.cpu_action = "hold"
      rec.mem_action = "hold" unless memory_at_risk
    end
    rec.urgent = false if rec.urgent && rec.status == "unstable" && !memory_at_risk
  end

  def self.fmt_cpu(v : Float64?) : String
    v ? "#{v.round.to_i}m" : "-"
  end

  def self.fmt_mem(v : Float64?) : String
    v ? "#{(v / MIB).round.to_i}Mi" : "-"
  end

  def self.render_table(recs : Array(Recommendation), io : IO)
    header = ["WORKLOAD", "STATUS", "CPU NOW", "CPU NEW", "CPU ACT", "MEM NOW", "MEM NEW", "MEM ACT", "PODS", "HOURS"]
    rows = recs.map do |r|
      name = r.urgent ? "#{r.key} !" : r.key
      [name, r.status, fmt_cpu(r.cpu_current), fmt_cpu(r.cpu_target), r.cpu_action,
       fmt_mem(r.mem_current), fmt_mem(r.mem_target), r.mem_action, r.pods.to_s, r.span_hours.round(1).to_s]
    end
    widths = header.map_with_index { |h, i| Math.max(h.size, rows.max_of? { |row| row[i].size } || 0) }
    line = ->(cells : Array(String)) { cells.map_with_index { |c, i| c.ljust(widths[i]) }.join("  ").rstrip }
    io.puts line.call(header)
    rows.each { |row| io.puts line.call(row) }
    cpu_save = recs.sum(&.cpu_saving_millicores)
    mem_save = recs.sum(&.mem_saving_bytes)
    io.puts
    io.puts "Reclaimable if applied: #{cpu_save.round.to_i}m CPU and #{(mem_save / MIB).round.to_i}Mi memory across all replicas."
    recs.each do |r|
      next if r.notes.empty?
      r.notes.each { |n| io.puts "  #{r.key}: #{n}" }
    end
    io.puts "Rows marked with ! need attention before the next deploy."
  end

  def self.render_json(recs : Array(Recommendation), io : IO)
    json = JSON::Builder.new(io)
    json.indent = 2
    json.document do
      json.object do
        json.field "version", VERSION
        json.field "recommendations" do
          json.array { recs.each { |r| r.write(json) } }
        end
      end
    end
    io.puts
  end

  def self.parse_args(argv : Array(String)) : Config
    cfg = Config.new
    OptionParser.parse(argv) do |p|
      p.banner = "Usage: pod_resource_right_sizer [options] < samples.jsonl"
      p.on("--input FILE", "JSONL file, or - for stdin") { |v| cfg.input = v }
      p.on("--format FORMAT", "table or json") { |v| cfg.format = v }
      p.on("--namespace NAME", "only this namespace") { |v| cfg.namespace_filter = v }
      p.on("--min-samples N", "samples needed per container") { |v| cfg.min_samples = v.to_i }
      p.on("--min-span-hours H", "history needed per container") { |v| cfg.min_span_hours = v.to_f }
      p.on("--warmup-seconds S", "ignored at the start of each pod") { |v| cfg.warmup_seconds = v.to_i64 }
      p.on("--cpu-quantile Q", "CPU quantile, default 0.95") { |v| cfg.cpu_quantile = v.to_f }
      p.on("--cpu-headroom F", "CPU headroom fraction, default 0.20") { |v| cfg.cpu_headroom = v.to_f }
      p.on("--mem-quantile Q", "memory quantile, default 0.99") { |v| cfg.mem_quantile = v.to_f }
      p.on("--mem-headroom F", "memory headroom fraction, default 0.15") { |v| cfg.mem_headroom = v.to_f }
      p.on("--peak-headroom F", "headroom over the observed peak, default 0.10") { |v| cfg.peak_headroom = v.to_f }
      p.on("--oom-bump F", "extra memory after an OOM or censoring, default 0.25") { |v| cfg.oom_bump = v.to_f }
      p.on("--deadband F", "ignore changes smaller than this, default 0.10") { |v| cfg.deadband = v.to_f }
      p.on("--max-decrease F", "largest cut per run, default 0.50") { |v| cfg.max_decrease = v.to_f }
      p.on("--max-restarts N", "restarts before a container is called unstable") { |v| cfg.max_restarts = v.to_i }
      p.on("--max-bad-fraction F", "fraction of unreadable lines tolerated") { |v| cfg.max_bad_fraction = v.to_f }
      p.on("--fail-on-risk", "exit 1 when any workload is urgent") { cfg.fail_on_risk = true }
      p.on("-h", "--help", "show help") do
        puts p
        exit EXIT_OK
      end
      p.invalid_option { |flag| raise ArgumentError.new("unknown option #{flag}") }
      p.missing_option { |flag| raise ArgumentError.new("missing value for #{flag}") }
    end
    cfg.validate!
    cfg
  end

  def self.run(argv : Array(String)) : Int32
    cfg = begin
      parse_args(argv)
    rescue ex
      STDERR.puts "error: #{ex.message}"
      return EXIT_USAGE
    end

    stats = LoadStats.new
    groups = begin
      if cfg.input == "-"
        load(STDIN, cfg, stats)
      else
        File.open(cfg.input) { |f| load(f, cfg, stats) }
      end
    rescue ex
      STDERR.puts "error: cannot read input: #{ex.message}"
      return EXIT_BAD_INPUT
    end

    if stats.lines == 0 || groups.empty?
      STDERR.puts "error: no usable samples found"
      return EXIT_BAD_INPUT
    end
    if stats.bad.to_f / stats.lines > cfg.max_bad_fraction
      STDERR.puts "error: #{stats.bad} of #{stats.lines} lines unreadable, above the allowed fraction"
      stats.first_bad.each { |m| STDERR.puts "  #{m}" }
      return EXIT_BAD_INPUT
    end
    STDERR.puts "warning: skipped #{stats.bad} unreadable lines" if stats.bad > 0

    recs = groups.keys.sort.map { |k| analyze(k, groups[k], cfg) }
    if cfg.format == "json"
      render_json(recs, STDOUT)
    else
      render_table(recs, STDOUT)
    end
    cfg.fail_on_risk && recs.any?(&.urgent) ? EXIT_RISK : EXIT_OK
  end
end

exit PodResourceRightSizer.run(ARGV)
