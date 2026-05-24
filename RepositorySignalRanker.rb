#!/usr/bin/env ruby
# frozen_string_literal: true

Change = Struct.new(:path, :added, :deleted, :owners, :signals, keyword_init: true) do
  def churn
    added + deleted
  end
end

class RepositorySignalRanker
  SECURITY_PATTERNS = [/auth/i, /token/i, /secret/i, /crypto/i, /session/i, /permission/i].freeze
  AI_PATTERNS = [/prompt/i, /eval/i, /model/i, /tool/i, /agent/i, /embedding/i].freeze
  INFRA_PATTERNS = [/docker/i, /kube/i, /terraform/i, /helm/i, /workflow/i, /deploy/i].freeze

  def initialize(max_files:, owner_weight:, churn_weight:)
    @max_files = max_files
    @owner_weight = owner_weight
    @churn_weight = churn_weight
  end

  def rank(changes)
    changes.map { |change| [score(change), change] }
           .sort_by { |score, change| [-score, change.path] }
           .first(@max_files)
  end

  def score(change)
    base = Math.log2(change.churn + 2) * @churn_weight
    base += 30 if SECURITY_PATTERNS.any? { |pattern| pattern.match?(change.path) }
    base += 18 if AI_PATTERNS.any? { |pattern| pattern.match?(change.path) }
    base += 16 if INFRA_PATTERNS.any? { |pattern| pattern.match?(change.path) }
    base += 12 if change.path.end_with?('.lock', '.yaml', '.yml', '.tf')
    base += 8 if change.path.split('/').length <= 2
    base += change.owners.length * @owner_weight
    base += change.signals.sum
    base.round(2)
  end
end

class Parser
  def self.from_numstat(lines)
    lines.each_with_index.filter_map do |line, index|
      next if line.strip.empty?

      cells = line.split(/\s+/, 3)
      raise ArgumentError, "line #{index + 1} needs added deleted path" unless cells.length == 3

      added = numeric(cells[0])
      deleted = numeric(cells[1])
      Change.new(path: normalize_path(cells[2]), added: added, deleted: deleted, owners: [], signals: [])
    end
  end

  def self.numeric(value)
    value == '-' ? 0 : Integer(value, 10)
  rescue ArgumentError
    raise ArgumentError, "bad numeric value #{value.inspect}"
  end

  def self.normalize_path(path)
    path.sub(/^"/, '').sub(/"$/, '').sub(/.* => /, '')
  end
end

class Owners
  def self.load(path)
    return {} unless path && File.exist?(path)

    rules = []
    File.readlines(path, chomp: true).each do |line|
      stripped = line.strip
      next if stripped.empty? || stripped.start_with?('#')

      pattern, *owners = stripped.split(/\s+/)
      rules << [pattern_to_regexp(pattern), owners]
    end
    rules
  end

  def self.apply(changes, rules)
    changes.each do |change|
      change.owners = rules.flat_map { |pattern, owners| pattern.match?(change.path) ? owners : [] }.uniq
    end
  end

  def self.pattern_to_regexp(pattern)
    escaped = Regexp.escape(pattern).gsub('\\*\\*', '.*').gsub('\\*', '[^/]*')
    %r{\A#{escaped}\z}
  end
end

class SignalFile
  def self.apply(changes, path)
    return unless path && File.exist?(path)

    weights = {}
    File.readlines(path, chomp: true).each do |line|
      next if line.strip.empty? || line.start_with?('#')

      pattern, weight = line.split(/\s+/, 2)
      weights[Owners.pattern_to_regexp(pattern)] = Float(weight)
    end
    changes.each do |change|
      change.signals = weights.filter_map { |pattern, weight| weight if pattern.match?(change.path) }
    end
  end
end

class Cli
  def self.parse(argv)
    options = { max_files: 25, owner_weight: 6.0, churn_weight: 7.0, owners: nil, signals: nil, json: false }
    until argv.empty?
      flag = argv.shift
      case flag
      when '--max-files' then options[:max_files] = Integer(argv.shift)
      when '--owner-weight' then options[:owner_weight] = Float(argv.shift)
      when '--churn-weight' then options[:churn_weight] = Float(argv.shift)
      when '--owners' then options[:owners] = argv.shift
      when '--signals' then options[:signals] = argv.shift
      when '--json' then options[:json] = true
      else raise ArgumentError, "unknown option #{flag}"
      end
    end
    options
  end

  def self.run(argv, input)
    options = parse(argv)
    changes = Parser.from_numstat(input.each_line)
    Owners.apply(changes, Owners.load(options[:owners]))
    SignalFile.apply(changes, options[:signals])
    ranked = RepositorySignalRanker.new(**options.slice(:max_files, :owner_weight, :churn_weight)).rank(changes)
    options[:json] ? puts_json(ranked) : puts_table(ranked)
  end

  def self.puts_table(ranked)
    puts 'score\tchurn\towners\tpath'
    ranked.each { |score, c| puts [score, c.churn, c.owners.join(','), c.path].join("\t") }
  end

  def self.puts_json(ranked)
    require 'json'
    puts JSON.pretty_generate(ranked.map { |score, c| { score: score, churn: c.churn, owners: c.owners, path: c.path } })
  end
end

class Hash
  def slice(*keys)
    keys.to_h { |key| [key, fetch(key)] }
  end
end

begin
  Cli.run(ARGV, STDIN.read)
rescue StandardError => e
  warn "RepositorySignalRanker: #{e.message}"
  exit 64
end

=begin
This solves the April 2026 review overload problem where agent-written pull requests touch
many files, but the human reviewer still needs to know which files deserve real attention
first. Built because code review bots can summarize everything and still miss the practical
question: what should Pavan inspect before approving? Use it when a pipeline can pipe git
diff --numstat into Ruby and optionally provide CODEOWNERS-like rules or custom signal
weights for security, AI, infra, data, or compiler code. The trick: it combines churn,
path risk, owner spread, and local scoring hints into a deterministic ranking that is easy
to audit in a pull request comment. Drop this into any repository as a single Ruby source
file and it becomes a changed-file priority ranker, AI pull request review planner, codeowner
risk scorer, repository signal analyzer, and developer productivity tool worth forking.
=end
