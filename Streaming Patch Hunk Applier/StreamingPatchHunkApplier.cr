require "option_parser"
require "digest/sha256"
require "json"

module StreamingPatchHunkApplier
  VERSION = "1.0.0"

  enum LineKind
    Context
    Add
    Remove
  end

  record HunkLine, kind : LineKind, text : String

  class Hunk
    property old_start : Int32
    property old_count : Int32
    property new_start : Int32
    property new_count : Int32
    property lines : Array(HunkLine)
    property old_no_newline : Bool
    property new_no_newline : Bool

    def initialize(@old_start, @old_count, @new_start, @new_count)
      @lines = [] of HunkLine
      @old_no_newline = false
      @new_no_newline = false
    end

    def old_block : Array(String)
      arr = [] of String
      lines.each { |l| arr << l.text unless l.kind.add? }
      arr
    end

    def new_block : Array(String)
      arr = [] of String
      lines.each { |l| arr << l.text unless l.kind.remove? }
      arr
    end
  end

  class FileDiff
    property old_path : String
    property new_path : String
    property hunks : Array(Hunk)

    def initialize(@old_path, @new_path)
      @hunks = [] of Hunk
    end

    def creation? : Bool
      old_path == "/dev/null"
    end

    def deletion? : Bool
      new_path == "/dev/null"
    end

    def target_path : String
      deletion? ? old_path : new_path
    end
  end

  module HunkLocator
    DEFAULT_WINDOW  =  200
    FUZZY_THRESHOLD = 0.85

    enum FuzzLevel
      Exact
      Shifted
      Whitespace
      Fuzzy
    end

    record Match, offset : Int32, fuzz : FuzzLevel, confidence : Float64

    class NoMatch < Exception
      getter candidates : Array(String)

      def initialize(message : String, @candidates = [] of String)
        super(message)
      end
    end

    def self.locate(original : Array(String), hunk : Hunk, window : Int32) : Match
      block = hunk.old_block
      return locate_pure_insert(original, hunk) if block.empty?

      declared = hunk.old_start - 1
      declared = 0 if declared < 0

      if offset = find_exact(original, block, declared, window)
        fuzz = offset == declared ? FuzzLevel::Exact : FuzzLevel::Shifted
        return Match.new(offset, fuzz, 1.0)
      end

      normalized = block.map(&.rstrip)
      if offset = find_normalized(original, normalized, declared, window)
        return Match.new(offset, FuzzLevel::Whitespace, 0.99)
      end

      if match = find_fuzzy(original, block, declared, window)
        return match
      end

      raise NoMatch.new(
        "could not locate hunk context declared at line #{hunk.old_start}",
        best_effort_hint(original, block, declared, window)
      )
    end

    private def self.candidate_offsets(declared : Int32, window : Int32) : Array(Int32)
      offsets = [declared]
      (1..window).each { |delta| offsets << declared + delta << declared - delta }
      offsets
    end

    private def self.find_exact(original : Array(String), block : Array(String), declared : Int32, window : Int32) : Int32?
      candidate_offsets(declared, window).each do |offset|
        next if offset < 0 || offset + block.size > original.size
        return offset if original[offset, block.size] == block
      end
      nil
    end

    private def self.find_normalized(original : Array(String), normalized : Array(String), declared : Int32, window : Int32) : Int32?
      candidate_offsets(declared, window).each do |offset|
        next if offset < 0 || offset + normalized.size > original.size
        candidate = original[offset, normalized.size].map(&.rstrip)
        return offset if candidate == normalized
      end
      nil
    end

    private def self.score_at(original : Array(String), block : Array(String), offset : Int32) : Float64
      matches = 0
      block.each_with_index { |expected, i| matches += 1 if original[offset + i].rstrip == expected.rstrip }
      matches.to_f / block.size
    end

    private def self.fuzzy_window(original_size : Int32, block_size : Int32, declared : Int32, window : Int32) : {Int32, Int32}?
      max_offset = original_size - block_size
      return nil if max_offset < 0

      lo = declared - window
      lo = 0 if lo < 0
      hi = declared + window
      hi = max_offset if hi > max_offset
      return nil if hi < lo

      {lo, hi}
    end

    private def self.find_fuzzy(original : Array(String), block : Array(String), declared : Int32, window : Int32) : Match?
      bounds = fuzzy_window(original.size, block.size, declared, window)
      return nil unless bounds
      lo, hi = bounds

      best_offset = -1
      best_score = 0.0
      tie = false

      (lo..hi).each do |offset|
        score = score_at(original, block, offset)
        if score > best_score
          best_score = score
          best_offset = offset
          tie = false
        elsif score == best_score
          tie = true
        end
      end

      return nil if best_offset < 0 || best_score < FUZZY_THRESHOLD || tie
      Match.new(best_offset, FuzzLevel::Fuzzy, best_score)
    end

    private def self.best_effort_hint(original : Array(String), block : Array(String), declared : Int32, window : Int32) : Array(String)
      bounds = fuzzy_window(original.size, block.size, declared, window)
      return [] of String unless bounds
      lo, hi = bounds

      best_offset = -1
      best_score = 0.0
      (lo..hi).each do |offset|
        score = score_at(original, block, offset)
        if score > best_score
          best_score = score
          best_offset = offset
        end
      end
      return [] of String if best_offset < 0
      ["closest candidate at line #{best_offset + 1}, #{(best_score * 100).round(1)}% of context lines matched"]
    end

    private def self.locate_pure_insert(original : Array(String), hunk : Hunk) : Match
      offset = hunk.old_start
      offset = 0 if offset < 0
      offset = original.size if offset > original.size
      Match.new(offset, FuzzLevel::Exact, 1.0)
    end
  end

  module DiffParser
    class ParseError < Exception; end

    def self.parse_stream(io : IO, &block : FileDiff ->)
      current : FileDiff? = nil
      current_hunk : Hunk? = nil
      pending_old_path : String? = nil

      io.each_line(chomp: true) do |line|
        if line.starts_with?("--- ")
          flush(current, current_hunk, block)
          current = nil
          current_hunk = nil
          pending_old_path = parse_diff_path(line)
        elsif line.starts_with?("+++ ")
          raise ParseError.new("+++ line without a preceding --- line: #{line}") unless pending_old_path
          current = FileDiff.new(pending_old_path.not_nil!, parse_diff_path(line))
          pending_old_path = nil
        elsif line.starts_with?("@@ ")
          raise ParseError.new("hunk header before any file header: #{line}") unless current
          current.hunks << current_hunk if current_hunk
          current_hunk = parse_hunk_header(line)
        elsif (hunk = current_hunk)
          parse_hunk_body_line(hunk, line)
        end
      end

      flush(current, current_hunk, block)
    end

    private def self.flush(file : FileDiff?, hunk : Hunk?, block : FileDiff ->)
      return unless file
      file.hunks << hunk if hunk
      block.call(file)
    end

    private def self.parse_hunk_body_line(hunk : Hunk, line : String)
      case line[0]?
      when '+'
        hunk.lines << HunkLine.new(LineKind::Add, rest(line))
      when '-'
        hunk.lines << HunkLine.new(LineKind::Remove, rest(line))
      when ' '
        hunk.lines << HunkLine.new(LineKind::Context, rest(line))
      when '\\'
        if last = hunk.lines.last?
          if last.kind.add?
            hunk.new_no_newline = true
          else
            hunk.old_no_newline = true
          end
        end
      when nil
        hunk.lines << HunkLine.new(LineKind::Context, "")
      else
        raise ParseError.new("unrecognized hunk line prefix #{line[0]?.inspect} in: #{line}")
      end
    end

    private def self.rest(line : String) : String
      line.size <= 1 ? "" : line[1..-1]
    end

    private def self.parse_diff_path(line : String) : String
      raw = line[4..-1].split('\t').first.strip
      raw.starts_with?("a/") || raw.starts_with?("b/") ? raw[2..-1] : raw
    end

    private def self.parse_hunk_header(line : String) : Hunk
      m = line.match(/^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/)
      raise ParseError.new("malformed hunk header: #{line}") unless m
      Hunk.new(m[1].to_i, m[2]?.try(&.to_i) || 1, m[3].to_i, m[4]?.try(&.to_i) || 1)
    end
  end

  module PathSafety
    class Violation < Exception; end

    def self.resolve!(root : String, relative : String) : String
      stripped = relative.strip
      raise Violation.new("empty path") if stripped.empty?
      raise Violation.new("absolute path not allowed: #{relative}") if stripped.starts_with?("/")

      root_full = File.expand_path(root)
      full = File.expand_path(File.join(root_full, stripped))

      unless full == root_full || full.starts_with?(root_full + "/")
        raise Violation.new("path escapes target directory: #{relative}")
      end

      full
    end
  end

  module PatchApplier
    record HunkResult, hunk_index : Int32, offset : Int32, fuzz : HunkLocator::FuzzLevel, confidence : Float64

    record FileResult,
      path : String,
      action : Symbol,
      original_sha256 : String?,
      result_sha256 : String?,
      content : String?,
      hunk_results : Array(HunkResult),
      trailing_newline : Bool

    class ApplyError < Exception; end

    def self.apply_file_diff(diff : FileDiff, original_content : String?, window : Int32) : FileResult
      if diff.creation?
        raise ApplyError.new("#{diff.new_path}: refuses to overwrite an existing file with a creation diff") unless original_content.nil?
        return apply_creation(diff)
      end

      raise ApplyError.new("#{diff.old_path}: file not found on disk but diff does not mark it as new") if original_content.nil?
      content = original_content.not_nil!

      ends_with_newline = !content.empty? && content.ends_with?("\n")
      lines = content.empty? ? ([] of String) : content.split("\n")
      lines.pop if ends_with_newline && lines.last? == ""
      original_sha = Digest::SHA256.hexdigest(content)

      if diff.deletion?
        raise ApplyError.new("#{diff.old_path}: deletion diff must contain exactly one hunk covering the whole file") unless diff.hunks.size == 1
        expected = diff.hunks.first.old_block
        raise ApplyError.new("#{diff.old_path}: refusing deletion, on-disk content does not match the diff's removed lines") unless lines == expected
        return FileResult.new(diff.old_path, :deleted, original_sha, nil, nil, [] of HunkResult, ends_with_newline)
      end

      raise ApplyError.new("#{diff.old_path}: diff contains no hunks") if diff.hunks.empty?

      located = [] of {Hunk, Int32, HunkLocator::Match}
      diff.hunks.each_with_index do |hunk, idx|
        located << {hunk, idx, HunkLocator.locate(lines, hunk, window)}
      end
      located = located.sort_by { |tuple| tuple[2].offset }

      located.each_cons(2) do |pair|
        prev_hunk, _prev_idx, prev_match = pair[0]
        _curr_hunk, _curr_idx, curr_match = pair[1]
        prev_end = prev_match.offset + prev_hunk.old_block.size
        if curr_match.offset < prev_end
          raise ApplyError.new("#{diff.old_path}: hunks overlap after matching, refusing to apply")
        end
      end

      new_lines = [] of String
      results = [] of HunkResult
      cursor = 0
      located.each do |hunk, idx, match|
        new_lines.concat(lines[cursor...match.offset])
        new_lines.concat(hunk.new_block)
        cursor = match.offset + hunk.old_block.size
        results << HunkResult.new(idx, match.offset, match.fuzz, match.confidence)
      end
      new_lines.concat(lines[cursor..-1])

      trailing_newline = ends_with_newline
      if (last_located = located.last?) && cursor >= lines.size
        trailing_newline = !last_located[0].new_no_newline
      end

      new_content = new_lines.join("\n")
      new_content += "\n" if trailing_newline

      results = results.sort_by { |r| r.hunk_index }

      FileResult.new(diff.new_path, :modified, original_sha, Digest::SHA256.hexdigest(new_content), new_content, results, trailing_newline)
    end

    private def self.apply_creation(diff : FileDiff) : FileResult
      raise ApplyError.new("#{diff.new_path}: new file diff must contain exactly one hunk") unless diff.hunks.size == 1
      hunk = diff.hunks.first
      raise ApplyError.new("#{diff.new_path}: new file hunk must not contain context or removed lines") unless hunk.old_block.empty?
      content = hunk.new_block.join("\n")
      content += "\n" unless hunk.new_no_newline
      FileResult.new(
        diff.new_path, :created, nil, Digest::SHA256.hexdigest(content), content,
        [HunkResult.new(0, 0, HunkLocator::FuzzLevel::Exact, 1.0)], !hunk.new_no_newline
      )
    end
  end

  module CLI
    def self.run(argv : Array(String)) : Int32
      target_dir = "."
      diff_path : String? = nil
      dry_run = false
      per_file = false
      window = HunkLocator::DEFAULT_WINDOW
      expects = {} of String => String
      expect_file : String? = nil
      json_out : String? = nil

      parser = OptionParser.new do |p|
        p.banner = "Usage: streaming_patch_hunk_applier [options] < diff.patch"
        p.on("-d DIR", "--dir=DIR", "Target directory root (default: current directory)") { |v| target_dir = v }
        p.on("-f FILE", "--diff-file=FILE", "Read the diff from FILE instead of stdin") { |v| diff_path = v }
        p.on("--dry-run", "Compute and report results without writing anything") { dry_run = true }
        p.on("--per-file", "Write each file as soon as it applies instead of all-or-nothing") { per_file = true }
        p.on("--window=N", "Fuzzy search window in lines, each side (default #{HunkLocator::DEFAULT_WINDOW})") do |v|
          parsed = v.to_i?
          if parsed
            window = parsed
          else
            STDERR.puts "invalid --window value: #{v}"
            exit 1
          end
        end
        p.on("--expect PATH=SHA256", "Refuse to patch PATH unless its current sha256 matches (repeatable)") do |v|
          parts = v.split('=', 2)
          if parts.size == 2
            expects[parts[0]] = parts[1].downcase
          else
            STDERR.puts "invalid --expect value, expected PATH=SHA256: #{v}"
            exit 1
          end
        end
        p.on("--expect-file=FILE", "JSON object of path to expected sha256, merged with --expect") { |v| expect_file = v }
        p.on("-o FILE", "--json-out=FILE", "Write the JSON report to FILE instead of stdout") { |v| json_out = v }
        p.on("--version", "Print the version and exit") { puts VERSION; exit 0 }
        p.on("-h", "--help", "Show this help") { puts p; exit 0 }
      end

      begin
        parser.parse(argv)
      rescue option_ex
        STDERR.puts option_ex.message
        return 1
      end

      if ef = expect_file
        begin
          data = JSON.parse(File.read(ef))
          data.as_h.each { |k, v| expects[k] = v.as_s.downcase }
        rescue expect_file_ex
          STDERR.puts "failed to read --expect-file #{ef}: #{expect_file_ex.message}"
          return 1
        end
      end

      input : IO = diff_path ? File.open(diff_path.not_nil!) : STDIN
      results = [] of PatchApplier::FileResult
      failures = [] of {String, String}
      security_violation = false
      parse_error : String? = nil

      begin
        DiffParser.parse_stream(input) do |file_diff|
          begin
            rel = file_diff.target_path
            full_path = PathSafety.resolve!(target_dir, rel)
            original = File.exists?(full_path) ? File.read(full_path) : nil

            if expected_hash = expects[rel]?
              actual_hash = original ? Digest::SHA256.hexdigest(original) : nil
              if actual_hash != expected_hash
                failures << {rel, "staleness guard failed: expected sha256 #{expected_hash}, found #{actual_hash || "no file on disk"}"}
                next
              end
            end

            result = PatchApplier.apply_file_diff(file_diff, original, window)
            write_result(target_dir, result) if per_file && !dry_run
            results << result
          rescue path_ex : PathSafety::Violation
            security_violation = true
            failures << {file_diff.target_path, path_ex.message || "path safety violation"}
          rescue apply_ex : PatchApplier::ApplyError
            failures << {file_diff.target_path, apply_ex.message || apply_ex.class.name}
          rescue match_ex : HunkLocator::NoMatch
            failures << {file_diff.target_path, match_ex.message || match_ex.class.name}
          end
        end
      rescue parse_ex : DiffParser::ParseError
        parse_error = parse_ex.message || "malformed diff"
      ensure
        input.close if diff_path
      end

      if pe = parse_error
        STDERR.puts "diff parse error: #{pe}"
        return 1
      end

      if !per_file && failures.empty? && !dry_run
        results.each { |r| write_result(target_dir, r) }
      end

      report = build_report(results, failures, dry_run, per_file)
      if json_out_path = json_out
        File.write(json_out_path, report)
      else
        puts report
      end

      return 3 if security_violation
      return 2 unless failures.empty?
      0
    end

    private def self.write_result(target_dir : String, result : PatchApplier::FileResult)
      full_path = PathSafety.resolve!(target_dir, result.path)
      if result.action == :deleted
        File.delete(full_path) if File.exists?(full_path)
        return
      end

      content = result.content
      raise PatchApplier::ApplyError.new("#{result.path}: missing computed content") unless content

      dir = File.dirname(full_path)
      Dir.mkdir_p(dir) unless Dir.exists?(dir)
      tmp = File.tempfile("shpa", dir: dir)
      tmp.print(content)
      tmp.close
      File.rename(tmp.path, full_path)
    end

    private def self.build_report(results : Array(PatchApplier::FileResult), failures : Array({String, String}), dry_run : Bool, per_file : Bool) : String
      JSON.build do |json|
        json.object do
          json.field "dry_run", dry_run
          json.field "mode", per_file ? "per_file" : "atomic"
          json.field "files" do
            json.array do
              results.each do |r|
                json.object do
                  json.field "path", r.path
                  json.field "action", r.action.to_s
                  json.field "original_sha256", r.original_sha256
                  json.field "result_sha256", r.result_sha256
                  json.field "trailing_newline", r.trailing_newline
                  json.field "hunks" do
                    json.array do
                      r.hunk_results.each do |hr|
                        json.object do
                          json.field "index", hr.hunk_index
                          json.field "matched_at_line", hr.offset + 1
                          json.field "fuzz", hr.fuzz.to_s.downcase
                          json.field "confidence", hr.confidence
                        end
                      end
                    end
                  end
                end
              end
            end
          end
          json.field "failures" do
            json.array do
              failures.each do |path, message|
                json.object do
                  json.field "path", path
                  json.field "error", message
                end
              end
            end
          end
          json.field "applied", !dry_run && failures.empty?
        end
      end
    end
  end
end

exit(StreamingPatchHunkApplier::CLI.run(ARGV))
