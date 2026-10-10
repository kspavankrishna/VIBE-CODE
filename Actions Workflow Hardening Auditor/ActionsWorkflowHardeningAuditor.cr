require "yaml"
require "json"
require "digest/sha256"
require "option_parser"

module ActionsWorkflowHardeningAuditor
  VERSION         = "1.0.0"
  MAX_NODES       = 250_000
  MAX_DEPTH       = 64
  DEFAULT_MAX_FILES = 5_000
  DEFAULT_MAX_BYTES = 2_000_000
  SUPPRESS_RE     = /#\s*wfa-ignore:\s*([A-Za-z0-9_,\s\-]+)/

  PRIVILEGED_TRIGGERS = Set{"pull_request_target", "workflow_run", "issue_comment", "issues", "discussion", "discussion_comment"}
  FORK_EXPOSED_TRIGGERS = Set{"pull_request", "pull_request_target", "pull_request_review", "pull_request_review_comment", "issue_comment", "issues", "workflow_run"}
  OFFICIAL_OWNERS = Set{"actions", "github"}
  BRANCH_REFS = Set{"main", "master", "develop", "dev", "trunk", "head", "latest", "stable", "release", "nightly"}

  # Context paths whose value an outside party can choose. "*" matches exactly one segment.
  ATTACKER_PATHS = [
    "github.event.issue.title", "github.event.issue.body",
    "github.event.issue.labels.*.name",
    "github.event.pull_request.title", "github.event.pull_request.body",
    "github.event.pull_request.head.ref", "github.event.pull_request.head.label",
    "github.event.pull_request.head.repo.default_branch",
    "github.event.pull_request.labels.*.name",
    "github.event.comment.body", "github.event.review.body", "github.event.review_comment.body",
    "github.event.discussion.title", "github.event.discussion.body",
    "github.event.pages.*.page_name",
    "github.event.commits.*.message", "github.event.commits.*.author.email", "github.event.commits.*.author.name",
    "github.event.head_commit.message", "github.event.head_commit.author.email", "github.event.head_commit.author.name",
    "github.event.head_commit.committer.email", "github.event.head_commit.committer.name",
    "github.event.workflow_run.head_branch", "github.event.workflow_run.display_title",
    "github.event.workflow_run.head_commit.message",
    "github.event.workflow_run.head_commit.author.email", "github.event.workflow_run.head_commit.author.name",
    "github.event.workflow_run.head_repository.description",
    "github.event.workflow_run.pull_requests.*.head.ref",
    "github.event.check_suite.head_branch", "github.event.check_suite.head_commit.message",
    "github.event.check_suite.head_commit.author.email", "github.event.check_suite.head_commit.author.name",
    "github.event.release.name", "github.event.release.body", "github.event.release.tag_name",
    "github.event.client_payload",
    "github.head_ref",
  ].map(&.split('.'))

  CALLER_PATHS = [["inputs"], ["github", "event", "inputs"]]

  UNTRUSTED_CHECKOUT_RE = /github\.event\.pull_request\.(head|merge_commit_sha)|github\.head_ref|refs\/pull\/|github\.event\.workflow_run\.head_(sha|branch|repository)|github\.event\.workflow_run\.pull_requests/i
  PR_CHECKOUT_CMD_RE    = /\bgh\s+pr\s+checkout\b|\bgit\s+(fetch|pull|checkout)\b[^\n]*(refs\/pull\/|pull\/\d+\/(head|merge)|\$\{\{\s*github\.(head_ref|event\.pull_request))/i
  REF_RE = /(?<![\w.\-])(?:github|inputs|steps|env|matrix|needs|secrets|vars|job|runner|strategy)(?:\s*\.\s*(?:[A-Za-z0-9_\-]+|\*))+/i

  enum Severity
    Info
    Low
    Medium
    High
    Critical

    def label : String
      to_s.upcase
    end

    def sarif_level : String
      case self
      when Critical, High then "error"
      when Medium         then "warning"
      else                     "note"
      end
    end

    def score : String
      case self
      when Critical then "9.5"
      when High     then "8.0"
      when Medium   then "5.5"
      when Low      then "3.0"
      else               "1.0"
      end
    end
  end

  record Rule, id : String, title : String, severity : Severity, help : String

  RULES = [
    Rule.new("WFA000", "Workflow could not be analysed", Severity::High,
      "The file is not valid YAML, is too large, is not UTF-8, or expands past the alias budget. An unreadable workflow is an audit gap, so it fails closed."),
    Rule.new("WFA001", "Expression injection into a script", Severity::High,
      "A ${{ }} expression that an outside party controls is pasted into run: or github-script before the shell or JavaScript parses it. Pass the value through env: and read it as a variable."),
    Rule.new("WFA002", "Untrusted code checked out in a privileged workflow", Severity::Critical,
      "pull_request_target, workflow_run and issue_comment run with a write token and secrets. Checking out the pull request head there runs attacker code with those rights."),
    Rule.new("WFA003", "Action or image not pinned to an immutable reference", Severity::Medium,
      "Tags and branches can be moved after review. Pin third party actions to a full commit SHA and docker images to a sha256 digest."),
    Rule.new("WFA004", "Token permissions are missing or too broad", Severity::Medium,
      "GITHUB_TOKEN rights come from repository defaults unless permissions: is set. Declare the least access per job."),
    Rule.new("WFA005", "Secrets passed too widely", Severity::High,
      "secrets: inherit and toJSON(secrets) hand every secret to code that needs one."),
    Rule.new("WFA006", "Git credentials persisted next to an artifact upload", Severity::Medium,
      "actions/checkout leaves the token in .git/config. If a later step uploads the workspace, the token travels with the artifact."),
    Rule.new("WFA007", "Self hosted runner reachable from fork driven triggers", Severity::High,
      "Self hosted runners are long lived. A pull request or comment event can execute code on them and persist."),
    Rule.new("WFA008", "Deprecated unsafe workflow commands", Severity::High,
      "set-env and add-path commands let any logged line change the environment of later steps."),
    Rule.new("WFA009", "Remote script piped into a shell", Severity::Medium,
      "curl or wget piped to a shell runs whatever the server returns at that moment, with no checksum."),
    Rule.new("WFA010", "Job has no timeout", Severity::Low,
      "A hung job runs for 360 minutes by default and bills for all of them."),
    Rule.new("WFA011", "Artifact consumed by a workflow_run workflow", Severity::Medium,
      "Artifacts built by a pull request run are attacker controlled. A privileged workflow that unpacks one must validate it."),
  ]
  RULE_INDEX = RULES.to_h { |r| {r.id, r} }

  class Error < Exception; end

  # ---------------------------------------------------------------------------
  # YAML tree with line numbers. YAML.parse drops positions, so build our own.
  # ---------------------------------------------------------------------------

  class Node
    enum Kind
      Scalar
      Map
      Seq
    end

    getter kind : Kind
    getter line : Int32
    getter value : String
    getter block : Bool
    getter quoted : Bool
    getter pairs : Array({Node, Node}) = [] of {Node, Node}
    getter items : Array(Node) = [] of Node
    property size : Int32 = 1

    def initialize(@kind, @line, @value = "", @block = false, @quoted = false)
    end

    def scalar? : Bool
      @kind.scalar?
    end

    def map? : Bool
      @kind.map?
    end

    def seq? : Bool
      @kind.seq?
    end

    def str : String?
      scalar? ? @value : nil
    end

    def []?(key : String) : Node?
      return nil unless map?
      @pairs.each { |k, v| return v if k.scalar? && k.value == key }
      nil
    end

    def keys : Array(String)
      @pairs.compact_map { |k, _| k.str }
    end

    def truthy? : Bool
      scalar? && !@quoted && {"true", "yes", "on", "1"}.includes?(@value.downcase)
    end

    def falsy_string? : Bool
      scalar? && {"false", "no", "off", "0"}.includes?(@value.downcase)
    end
  end

  class TreeBuilder
    def initialize(@parser : YAML::PullParser)
      @anchors = {} of String => Node
      @count = 0
    end

    def document : Node?
      result = nil
      @parser.read_stream do
        return nil if @parser.kind.stream_end?
        @parser.read_document do
          result = build(0)
        end
      end
      result
    end

    private def charge(n : Int32)
      @count += n
      raise Error.new("document expands past #{MAX_NODES} nodes (alias bomb or oversized file)") if @count > MAX_NODES
    end

    private def build(depth : Int32) : Node
      raise Error.new("nesting deeper than #{MAX_DEPTH} levels") if depth > MAX_DEPTH
      line = @parser.start_line
      anchor = @parser.anchor
      node = case @parser.kind
             when .scalar?
               style = @parser.scalar_style
               n = Node.new(Node::Kind::Scalar, line, @parser.value,
                 block: style.literal? || style.folded?,
                 quoted: style.single_quoted? || style.double_quoted?)
               @parser.read_next
               charge(1)
               n
             when .alias?
               name = @parser.read_alias
               target = name ? @anchors[name]? : nil
               raise Error.new("alias *#{name} at line #{line} has no anchor") unless target
               charge(target.size)
               return target
             when .sequence_start?
               n = Node.new(Node::Kind::Seq, line)
               @parser.read_next
               charge(1)
               until @parser.kind.sequence_end?
                 child = build(depth + 1)
                 n.items << child
                 n.size += child.size
               end
               @parser.read_next
               n
             when .mapping_start?
               n = Node.new(Node::Kind::Map, line)
               @parser.read_next
               charge(1)
               until @parser.kind.mapping_end?
                 k = build(depth + 1)
                 v = build(depth + 1)
                 n.pairs << {k, v}
                 n.size += k.size + v.size
               end
               @parser.read_next
               merge_keys(n)
               n
             else
               raise Error.new("unexpected YAML event #{@parser.kind} at line #{line}")
             end
      @anchors[anchor] = node if anchor
      node
    end

    private def merge_keys(map : Node)
      merged = [] of {Node, Node}
      map.pairs.reject! do |k, v|
        next false unless k.scalar? && k.value == "<<"
        sources = v.map? ? [v] : (v.seq? ? v.items.select(&.map?) : [] of Node)
        sources.each { |s| merged.concat(s.pairs) }
        true
      end
      merged.each do |k, v|
        map.pairs << {k, v} unless map.keys.includes?(k.value)
      end
    end
  end

  def self.load_tree(text : String) : Node?
    parser = YAML::PullParser.new(text)
    begin
      TreeBuilder.new(parser).document
    ensure
      parser.close
    end
  rescue ex : YAML::ParseException
    raise Error.new("YAML syntax error: #{ex.message}")
  end

  # ---------------------------------------------------------------------------
  # Findings
  # ---------------------------------------------------------------------------

  class Finding
    getter rule : String
    getter severity : Severity
    getter file : String
    getter line : Int32
    getter job : String?
    getter step : Int32?
    getter message : String
    getter evidence : String
    getter fix : String
    getter fingerprint : String

    def initialize(@rule, @severity, @file, @line, @job, @step, @message, @evidence, @fix)
      key = [@rule, @file, @job.to_s, @step.to_s, @evidence.gsub(/\s+/, " ")].join("|")
      @fingerprint = Digest::SHA256.hexdigest(key)[0, 32]
    end

    def to_json(json : JSON::Builder)
      json.object do
        json.field "rule", @rule
        json.field "severity", @severity.label.downcase
        json.field "file", @file
        json.field "line", @line
        json.field "job", @job
        json.field "step", @step
        json.field "message", @message
        json.field "evidence", @evidence
        json.field "fix", @fix
        json.field "fingerprint", @fingerprint
      end
    end
  end

  record Expr, body : String, line : Int32, refs : Array(String), boolean : Bool, serializer : Bool

  # ---------------------------------------------------------------------------
  # Expression handling
  # ---------------------------------------------------------------------------

  def self.mask_strings(s : String) : String
    s.gsub(/'(?:[^']|'')*'/, "''")
  end

  def self.normalize_expression(body : String) : String
    s = body.gsub(/\[\s*'([^']*)'\s*\]/) { ".#{$1}" }
    s.gsub(/\[\s*(?:\d+|\*)\s*\]/, ".*")
  end

  def self.boolean_only?(masked : String) : Bool
    s = masked.strip
    return true if s =~ /\A!?\s*(contains|startsWith|endsWith)\s*\(.*\)\z/im && s !~ /&&|\|\|/
    s =~ /==|!=|<=|>=/ && s !~ /&&|\|\|/ ? true : false
  end

  def self.expressions(text : String, base_line : Int32, block : Bool) : Array(Expr)
    acc = [] of Expr
    text.scan(/\$\{\{(.*?)\}\}/m) do |m|
      body = m[1]
      nl = text[0, m.begin(0)].count('\n')
      line = base_line + (block ? 1 : 0) + nl
      masked = mask_strings(normalize_expression(body))
      refs = masked.scan(REF_RE).map { |r| r[0].gsub(/\s+/, "").downcase }.uniq!
      serializer = !!(masked =~ /\b(tojson|format|join)\s*\(/i)
      refs << "github" if masked =~ /tojson\(\s*github\s*\)/i
      acc << Expr.new(body.strip, line, refs, boolean_only?(masked), serializer)
    end
    acc
  end

  private def self.match_path?(segs : Array(String), pattern : Array(String)) : Bool
    return false if segs.size < pattern.size
    pattern.each_with_index.all? { |p, i| p == "*" || p == segs[i] }
  end

  private def self.prefix_of_untrusted?(segs : Array(String)) : Bool
    ATTACKER_PATHS.any? do |pat|
      pat.size > segs.size && segs.each_with_index.all? { |s, i| pat[i] == "*" || pat[i] == s }
    end
  end

  enum Taint
    Attacker
    Caller
    StepOutput
    Object
  end

  def self.classify(ref : String, serializer : Bool) : Taint?
    segs = ref.split('.')
    return Taint::Attacker if ATTACKER_PATHS.any? { |p| match_path?(segs, p) }
    return Taint::Caller if CALLER_PATHS.any? { |p| match_path?(segs, p) }
    return Taint::StepOutput if (segs[0] == "steps" || segs[0] == "needs") && segs.includes?("outputs")
    return Taint::Object if serializer && prefix_of_untrusted?(segs)
    nil
  end

  def self.env_name_for(ref : String) : String
    ref.split('.').reject { |s| s == "*" }.last(2).join("_").upcase.gsub(/[^A-Z0-9_]/, "_")
  end

  # ---------------------------------------------------------------------------
  # Per file analysis
  # ---------------------------------------------------------------------------

  class Analyzer
    getter findings = [] of Finding

    @triggers = Set(String).new
    @privileged = false
    @workflow = true

    def initialize(@path : String, @root : Node, @lines : Array(String))
    end

    def run : Array(Finding)
      if @root["jobs"]?
        analyze_workflow
      elsif (runs = @root["runs"]?) && runs.map?
        @workflow = false
        analyze_steps_container("(composite action)", runs, nil)
      end
      scan_generic(@root)
      @findings
    end

    private def analyze_workflow
      on = @root["on"]? || @root["true"]?
      @triggers = triggers_of(on)
      @privileged = @triggers.any? { |t| PRIVILEGED_TRIGGERS.includes?(t) }
      jobs = @root["jobs"]?
      return unless jobs && jobs.map?
      top_perm = @root["permissions"]?
      check_top_permissions(top_perm, jobs)
      jobs.pairs.each do |k, job|
        next unless k.scalar? && job.map?
        analyze_job(k.value, k.line, job, top_perm)
      end
    end

    private def triggers_of(on : Node?) : Set(String)
      set = Set(String).new
      return set unless on
      if on.scalar?
        set << on.value
      elsif on.seq?
        on.items.each { |i| set << i.value if i.scalar? }
      elsif on.map?
        on.keys.each { |k| set << k }
      end
      set
    end

    private def evidence_at(line : Int32) : String
      (@lines[line - 1]? || "").strip[0, 200]
    end

    private def emit(rule : String, sev : Severity, line : Int32, job : String?, step : Int32?, msg : String, fix : String, evidence : String? = nil)
      @findings << Finding.new(rule, sev, @path, line, job, step, msg, evidence || evidence_at(line), fix)
    end

    # --- permissions ---------------------------------------------------------

    private def write_scopes(perm : Node?) : Array(String)
      return [] of String unless perm
      if perm.scalar?
        return perm.value == "write-all" ? ["write-all"] : [] of String
      end
      return [] of String unless perm.map?
      perm.pairs.compact_map { |k, v| k.str if v.scalar? && v.value == "write" }
    end

    private def check_top_permissions(top : Node?, jobs : Node)
      scopes = write_scopes(top)
      return if scopes.empty? || jobs.pairs.size < 2 || top.nil?
      sev = scopes.includes?("write-all") ? Severity::High : Severity::Low
      emit("WFA004", sev, top.line, nil, nil,
        "Workflow level permissions grant #{scopes.join(", ")} to all #{jobs.pairs.size} jobs.",
        "Set permissions: {} or read only at the top and grant write scopes inside the one job that needs them.")
    end

    private def check_job_permissions(id : String, line : Int32, job : Node, top : Node?)
      jperm = job["permissions"]?
      effective = jperm || top
      if effective.nil?
        sev = @privileged ? Severity::Medium : Severity::Low
        emit("WFA004", sev, line, id, nil,
          "Job #{id} has no permissions block and none is set for the workflow, so the token takes the repository default.",
          "Add permissions: contents: read at workflow level, then widen per job only where needed.")
        return
      end
      scopes = write_scopes(effective)
      return if scopes.empty?
      if scopes.includes?("write-all")
        emit("WFA004", @privileged ? Severity::Critical : Severity::High, effective.line, id, nil,
          "Job #{id} runs with write-all token permissions.",
          "List only the scopes this job uses.")
      elsif @privileged
        sev = scopes.includes?("id-token") ? Severity::High : Severity::Medium
        emit("WFA004", sev, effective.line, id, nil,
          "Job #{id} holds write scopes (#{scopes.join(", ")}) under a privileged trigger (#{(@triggers & PRIVILEGED_TRIGGERS).to_a.join(", ")}).",
          "Split the privileged write step into its own job and keep anything that touches pull request content on read only.")
      end
    end

    # --- jobs ----------------------------------------------------------------

    private def analyze_job(id : String, line : Int32, job : Node, top_perm : Node?)
      check_job_permissions(id, line, job, top_perm)

      if (uses = job["uses"]?) && uses.scalar?
        check_uses(uses, id, nil)
        if (sec = job["secrets"]?) && sec.scalar? && sec.value == "inherit"
          emit("WFA005", Severity::Medium, sec.line, id, nil,
            "Job #{id} calls a reusable workflow with secrets: inherit, which forwards every secret.",
            "Pass only the named secrets the called workflow declares.")
        end
      else
        unless job["timeout-minutes"]?
          emit("WFA010", Severity::Low, line, id, nil,
            "Job #{id} has no timeout-minutes, so a hang bills for up to 360 minutes.",
            "Set timeout-minutes to roughly twice the normal run time.")
        end
        check_runner(id, job)
      end
      analyze_steps_container(id, job, job)
    end

    private def check_runner(id : String, job : Node)
      ro = job["runs-on"]?
      return unless ro
      labels = [] of String
      group = false
      if ro.scalar?
        labels << ro.value
      elsif ro.seq?
        ro.items.each { |i| labels << i.value if i.scalar? }
      elsif ro.map?
        group = !ro["group"]?.nil?
        if l = ro["labels"]?
          labels << l.value if l.scalar?
          l.items.each { |i| labels << i.value if i.scalar? } if l.seq?
        end
      end
      selfhosted = group || labels.any? { |l| l.downcase == "self-hosted" }
      exposed = @triggers & FORK_EXPOSED_TRIGGERS
      return unless selfhosted && !exposed.empty?
      emit("WFA007", Severity::High, ro.line, id, nil,
        "Job #{id} targets a self hosted runner and the workflow fires on #{exposed.to_a.join(", ")}.",
        "Use GitHub hosted runners for fork driven events, or gate the job on an environment with required reviewers and ephemeral runners.")
    end

    private def analyze_steps_container(id : String, holder : Node, job : Node?)
      steps = holder["steps"]?
      return unless steps && steps.seq?
      uploads = steps.items.any? do |s|
        u = s.map? ? s["uses"]?.try(&.str) : nil
        !!(u && u.downcase.starts_with?("actions/upload-artifact"))
      end
      steps.items.each_with_index do |step, idx|
        next unless step.map?
        analyze_step(id, idx, step, uploads)
      end
    end

    # --- steps ---------------------------------------------------------------

    private def analyze_step(id : String, idx : Int32, step : Node, uploads : Bool)
      if u = step["uses"]?
        if u.scalar?
          check_uses(u, id, idx)
          name = u.value.split('@').first.downcase
          with_ = step["with"]?
          if name == "actions/checkout"
            check_checkout(id, idx, step, with_, uploads)
          elsif name == "actions/github-script" && with_ && (script = with_["script"]?) && script.scalar?
            check_expressions(script, id, idx, "github-script")
          end
          if @triggers.includes?("workflow_run") && name.includes?("download-artifact")
            emit("WFA011", Severity::Medium, u.line, id, idx,
              "A workflow_run workflow downloads an artifact built by another run. Its contents are attacker controlled when the source run came from a fork.",
              "Treat every file as untrusted: never execute it, never source it, validate names and sizes, and read numbers rather than shell text.")
          end
        end
      end

      if run = step["run"]?
        if run.scalar?
          check_expressions(run, id, idx, "run")
          check_shell(run, id, idx)
        end
      end
    end

    private def check_checkout(id : String, idx : Int32, step : Node, with_ : Node?, uploads : Bool)
      if with_ && with_.map?
        {"ref", "repository"}.each do |key|
          v = with_[key]?
          next unless v && v.scalar?
          if v.value =~ UNTRUSTED_CHECKOUT_RE && (@triggers.includes?("pull_request_target") || @triggers.includes?("workflow_run") || @triggers.includes?("issue_comment"))
            emit("WFA002", Severity::Critical, v.line, id, idx,
              "checkout #{key} points at pull request content inside a #{(@triggers & PRIVILEGED_TRIGGERS).to_a.join(", ")} workflow. Anything run after this executes attacker code with the base repository token and secrets.",
              "Check out the default ref and treat the pull request as data. If a build is unavoidable, do it in a plain pull_request workflow with no secrets and hand results over as an artifact.")
          end
        end
      end
      pc = with_.try(&.[]?("persist-credentials"))
      if uploads && !(pc && pc.falsy_string?)
        emit("WFA006", Severity::Medium, step.line, id, idx,
          "Job #{id} uploads an artifact but this checkout keeps the token in .git/config.",
          "Add persist-credentials: false to this checkout, or upload only a dedicated output directory.")
      end
    end

    private def check_shell(run : Node, id : String, idx : Int32)
      text = run.value
      base = run.line + (run.block ? 1 : 0)
      text.each_line.with_index do |l, i|
        ln = base + i
        if l =~ /(?:\bcurl|\bwget)\b[^\n|;]*\|\s*(?:sudo\s+(?:-\S+\s+)*)?(?:ba|z|da)?sh\b/ || l =~ /\b(?:ba|z)?sh\s+<\(\s*(?:curl|wget)/ || l =~ /\|\s*iex\b/i
          emit("WFA009", Severity::Medium, ln, id, idx,
            "A remote script is piped straight into a shell.",
            "Download to a file, verify a pinned sha256 with sha256sum -c, then run it.", l.strip[0, 200])
        end
        if l =~ /::(set-env|add-path)\b/
          emit("WFA008", Severity::High, ln, id, idx,
            "The #{$1} workflow command is disabled by default because any logged text can trigger it.",
            "Append to $GITHUB_ENV or $GITHUB_PATH instead, and never write untrusted text there.", l.strip[0, 200])
        end
        if @privileged && l =~ PR_CHECKOUT_CMD_RE
          emit("WFA002", Severity::Critical, ln, id, idx,
            "A shell step fetches pull request code inside a privileged workflow.",
            "Do not fetch pull request refs in pull_request_target, workflow_run or issue_comment workflows.", l.strip[0, 200])
        end
      end
    end

    private def check_expressions(node : Node, id : String, idx : Int32, sink : String)
      text = node.value
      lines = text.lines
      seen = Set(String).new
      ActionsWorkflowHardeningAuditor.expressions(text, node.line, node.block).each do |e|
        next if e.boolean
        e.refs.each do |ref|
          taint = ActionsWorkflowHardeningAuditor.classify(ref, e.serializer)
          next unless taint
          next unless seen.add?("#{e.line}|#{ref}")
          source_line = lines[e.line - node.line - (node.block ? 1 : 0)]? || ""
          writes_env = !!(source_line =~ /GITHUB_(ENV|PATH)/ || text =~ /GITHUB_(ENV|PATH)/)
          name = ActionsWorkflowHardeningAuditor.env_name_for(ref)
          fix = if sink == "run"
                  "Set env: #{name}: ${{ #{ref} }} on the step and use \"$#{name}\" in the script."
                else
                  "Set env: #{name}: ${{ #{ref} }} on the step and read process.env.#{name} in the script."
                end
          case taint
          when Taint::Attacker, Taint::Object
            sev = (@privileged || writes_env) ? Severity::Critical : Severity::High
            what = taint.object? ? "serialises an object that contains attacker text" : "is chosen by whoever opens the issue, pull request or branch"
            emit("WFA001", sev, e.line, id, idx,
              "${{ #{e.body} }} is expanded into #{sink} source and #{what}.#{writes_env ? " The script also writes to GITHUB_ENV or GITHUB_PATH, so injection persists into later steps." : ""}",
              fix, evidence_at(e.line))
          when Taint::Caller
            emit("WFA001", Severity::Medium, e.line, id, idx,
              "${{ #{e.body} }} is a caller supplied input expanded into #{sink} source. Anyone who can dispatch or call this workflow controls it.",
              fix, evidence_at(e.line))
          when Taint::StepOutput
            emit("WFA001", Severity::Low, e.line, id, idx,
              "${{ #{e.body} }} is expanded into #{sink} source. Outputs carry whatever the producing step computed, which may include attacker text.",
              fix, evidence_at(e.line))
          end
        end
      end
    end

    # --- uses ----------------------------------------------------------------

    private def check_uses(node : Node, id : String, idx : Int32?)
      v = node.value.strip
      return if v.starts_with?("./") || v.starts_with?("../")
      if v.starts_with?("docker://")
        unless v =~ /@sha256:[0-9a-f]{64}\z/
          emit("WFA003", Severity::Medium, node.line, id, idx,
            "Docker image #{v} is referenced by tag, which the registry owner can repoint.",
            "Append @sha256:<digest> from docker buildx imagetools inspect.")
        end
        return
      end
      unless m = v.match(/\A([^\/@\s]+)\/([^\/@\s]+)(\/[^@\s]*)?@(\S+)\z/)
        emit("WFA003", Severity::Medium, node.line, id, idx,
          "uses: #{v} has no @ref, so it floats on the default branch.",
          "Pin to a full commit SHA with the version in a trailing comment.")
        return
      end
      owner = m[1].downcase
      ref = m[4]
      return if ref =~ /\A[0-9a-f]{40}\z/ || ref =~ /\A[0-9a-f]{64}\z/
      official = OFFICIAL_OWNERS.includes?(owner)
      sev, why = if ref =~ /\A[0-9a-f]{7,39}\z/
                   {Severity::Medium, "is an abbreviated SHA, which is ambiguous and rejected by newer runners"}
                 elsif BRANCH_REFS.includes?(ref.downcase) || ref.includes?('/')
                   {Severity::High, "is a branch, so every push to it changes what runs here"}
                 elsif ref =~ /\Av?\d+(\.\d+)*\z/
                   {official ? Severity::Low : Severity::Medium, "is a tag that the owner or an attacker with their token can move"}
                 else
                   {Severity::Medium, "is a mutable reference"}
                 end
      emit("WFA003", sev, node.line, id, idx,
        "#{m[1]}/#{m[2]}#{m[3]?}@#{ref} #{why}.",
        "Resolve the reference to its commit with git ls-remote and write uses: #{m[1]}/#{m[2]}#{m[3]?}@<40 hex sha> # #{ref}")
    end

    # --- whole document scan -------------------------------------------------

    private def scan_generic(node : Node, seen = Set(UInt64).new)
      return unless seen.add?(node.object_id)
      case node.kind
      when .map?
        node.pairs.each do |k, v|
          if k.scalar? && k.value == "ACTIONS_ALLOW_UNSECURE_COMMANDS" && v.scalar? && v.truthy?
            emit("WFA008", Severity::High, k.line, nil, nil,
              "ACTIONS_ALLOW_UNSECURE_COMMANDS re-enables set-env and add-path.",
              "Delete the variable and write to $GITHUB_ENV and $GITHUB_PATH.")
          end
          scan_generic(v, seen)
        end
      when .seq?
        node.items.each { |i| scan_generic(i, seen) }
      else
        if node.value =~ /tojson\(\s*secrets\s*\)/i
          emit("WFA005", Severity::High, node.line, nil, nil,
            "toJSON(secrets) serialises every secret into one string.",
            "Reference the single secret you need by name.")
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # File handling
  # ---------------------------------------------------------------------------

  def self.suppressed?(f : Finding, lines : Array(String)) : Bool
    return false if f.rule == "WFA000"
    candidates = [lines[f.line - 1]?]
    prev = lines[f.line - 2]?
    candidates << prev if prev && prev.lstrip.starts_with?("#")
    candidates.compact.any? do |l|
      if m = l.match(SUPPRESS_RE)
        ids = m[1].split(/[,\s]+/).map(&.upcase)
        ids.includes?(f.rule) || ids.includes?("ALL")
      else
        false
      end
    end
  end

  def self.analyze_text(path : String, text : String) : {Array(Finding), Int32}
    lines = text.lines
    begin
      root = load_tree(text)
      return {[] of Finding, 0} if root.nil? || !root.map?
      found = Analyzer.new(path, root, lines).run
    rescue ex : Error
      return {[Finding.new("WFA000", Severity::High, path, 1, nil, nil,
        "Could not analyse this file: #{ex.message}", "", "Fix the file or exclude it deliberately. Unreadable workflows are reported rather than skipped.")], 0}
    end
    kept = found.reject { |f| suppressed?(f, lines) }
    {kept, found.size - kept.size}
  end

  def self.yaml_children(dir : String) : Array(String)
    Dir.children(dir).select { |n| n.ends_with?(".yml") || n.ends_with?(".yaml") }.sort.map { |n| File.join(dir, n) }.select { |p| File.file?(p) }
  end

  def self.action_files(dir : String, depth = 0) : Array(String)
    acc = [] of String
    return acc if depth > 6 || !Dir.exists?(dir)
    Dir.children(dir).sort.each do |n|
      p = File.join(dir, n)
      if File.directory?(p) && !File.symlink?(p)
        acc.concat(action_files(p, depth + 1))
      elsif n == "action.yml" || n == "action.yaml"
        acc << p
      end
    end
    acc
  end

  def self.discover(arg : String) : Array(String)
    return [arg] if File.file?(arg)
    raise Error.new("no such file or directory: #{arg}") unless Dir.exists?(arg)
    wf = File.join(arg, ".github", "workflows")
    files = [] of String
    if Dir.exists?(wf)
      files.concat(yaml_children(wf))
      files.concat(action_files(File.join(arg, ".github", "actions")))
      {"action.yml", "action.yaml"}.each { |n| files << File.join(arg, n) if File.file?(File.join(arg, n)) }
    else
      files.concat(yaml_children(arg))
    end
    files
  end

  def self.read_file(path : String, max_bytes : Int32) : String
    size = File.size(path)
    raise Error.new("file is #{size} bytes, over the #{max_bytes} byte limit") if size > max_bytes
    text = File.read(path)
    raise Error.new("file is not valid UTF-8") unless text.valid_encoding?
    text.lchop('﻿')
  end

  # ---------------------------------------------------------------------------
  # Reports
  # ---------------------------------------------------------------------------

  def self.sort_findings(list : Array(Finding)) : Array(Finding)
    list.sort_by { |f| {f.file, f.line, -f.severity.value, f.rule} }
  end

  def self.text_report(findings : Array(Finding), files : Int32, suppressed : Int32, baselined : Int32) : String
    String.build do |io|
      findings.each do |f|
        loc = f.job ? " #{f.job}#{f.step ? ".steps[#{f.step}]" : ""}" : ""
        io << f.file << ':' << f.line << ": " << f.severity.label << ' ' << f.rule << loc << '\n'
        io << "    " << f.message << '\n'
        io << "    evidence: " << f.evidence << '\n' unless f.evidence.empty?
        io << "    fix: " << f.fix << '\n'
      end
      counts = findings.group_by(&.severity).transform_values(&.size)
      parts = Severity.values.reverse.compact_map { |s| counts[s]?.try { |n| "#{n} #{s.label.downcase}" } }
      io << files << " files, " << findings.size << " findings"
      io << " (" << parts.join(", ") << ")" unless parts.empty?
      io << ", " << suppressed << " suppressed inline, " << baselined << " in baseline\n"
    end
  end

  def self.json_report(findings : Array(Finding), files : Int32, suppressed : Int32, baselined : Int32) : String
    JSON.build(indent: 2) do |j|
      j.object do
        j.field "tool", "ActionsWorkflowHardeningAuditor"
        j.field "version", VERSION
        j.field "files", files
        j.field "suppressed", suppressed
        j.field "baselined", baselined
        j.field "findings" do
          j.array { findings.each(&.to_json(j)) }
        end
      end
    end
  end

  def self.sarif_report(findings : Array(Finding)) : String
    JSON.build(indent: 2) do |j|
      j.object do
        j.field "$schema", "https://json.schemastore.org/sarif-2.1.0.json"
        j.field "version", "2.1.0"
        j.field "runs" do
          j.array do
            j.object do
              j.field "tool" do
                j.object do
                  j.field "driver" do
                    j.object do
                      j.field "name", "ActionsWorkflowHardeningAuditor"
                      j.field "version", VERSION
                      j.field "rules" do
                        j.array do
                          RULES.each do |r|
                            j.object do
                              j.field "id", r.id
                              j.field "name", r.title
                              j.field "shortDescription" { j.object { j.field "text", r.title } }
                              j.field "fullDescription" { j.object { j.field "text", r.help } }
                              j.field "defaultConfiguration" { j.object { j.field "level", r.severity.sarif_level } }
                              j.field "properties" { j.object { j.field "security-severity", r.severity.score } }
                            end
                          end
                        end
                      end
                    end
                  end
                end
              end
              j.field "results" do
                j.array do
                  findings.each do |f|
                    j.object do
                      j.field "ruleId", f.rule
                      j.field "level", f.severity.sarif_level
                      j.field "message" { j.object { j.field "text", "#{f.message} Fix: #{f.fix}" } }
                      j.field "locations" do
                        j.array do
                          j.object do
                            j.field "physicalLocation" do
                              j.object do
                                j.field "artifactLocation" { j.object { j.field "uri", f.file.gsub('\\', '/') } }
                                j.field "region" { j.object { j.field "startLine", f.line } }
                              end
                            end
                          end
                        end
                      end
                      j.field "partialFingerprints" { j.object { j.field "wfa/v1", f.fingerprint } }
                      j.field "properties" { j.object { j.field "security-severity", f.severity.score } }
                    end
                  end
                end
              end
            end
          end
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # CLI
  # ---------------------------------------------------------------------------

  def self.run(argv : Array(String)) : Int32
    format = "text"
    fail_on : Severity? = Severity::High
    min = Severity::Low
    ignore = Set(String).new
    only = Set(String).new
    baseline_path : String? = nil
    write_baseline : String? = nil
    max_files = DEFAULT_MAX_FILES
    max_bytes = DEFAULT_MAX_BYTES
    list_rules = false

    parser = OptionParser.new do |o|
      o.banner = "Usage: actions-workflow-hardening-auditor [options] PATH [PATH...]\n\nPATH is a repository root, a workflows directory or a single YAML file."
      o.on("--format FORMAT", "text, json or sarif (default text)") { |v| format = v }
      o.on("--fail-on LEVEL", "info, low, medium, high, critical or none (default high)") do |v|
        fail_on = v.downcase == "none" ? nil : (Severity.parse?(v) || raise Error.new("unknown severity #{v}"))
      end
      o.on("--min-severity LEVEL", "hide findings below this level (default low)") { |v| min = Severity.parse?(v) || raise Error.new("unknown severity #{v}") }
      o.on("--ignore RULES", "comma separated rule ids to skip") { |v| v.split(',').each { |r| ignore << r.strip.upcase } }
      o.on("--only RULES", "comma separated rule ids to run") { |v| v.split(',').each { |r| only << r.strip.upcase } }
      o.on("--baseline FILE", "suppress findings whose fingerprint is listed in FILE") { |v| baseline_path = v }
      o.on("--write-baseline FILE", "write current fingerprints to FILE and exit 0") { |v| write_baseline = v }
      o.on("--max-files N", "refuse to scan more than N files (default #{DEFAULT_MAX_FILES})") { |v| max_files = v.to_i }
      o.on("--max-bytes N", "skip files larger than N bytes (default #{DEFAULT_MAX_BYTES})") { |v| max_bytes = v.to_i }
      o.on("--list-rules", "print the rule table") { list_rules = true }
      o.on("--version", "print version") { puts VERSION; exit 0 }
      o.on("-h", "--help", "show help") { puts o; exit 0 }
    end

    begin
      parser.parse(argv)
    rescue ex : OptionParser::Exception | Error
      STDERR.puts "error: #{ex.message}"
      return 2
    end

    if list_rules
      RULES.each { |r| puts "#{r.id}  #{r.severity.label.ljust(8)} #{r.title}\n        #{r.help}" }
      return 0
    end
    unless {"text", "json", "sarif"}.includes?(format)
      STDERR.puts "error: unknown format #{format}"
      return 2
    end
    paths = argv.dup
    if paths.empty?
      STDERR.puts "error: give at least one PATH\n#{parser}"
      return 2
    end

    baseline = Set(String).new
    if bp = baseline_path
      begin
        data = JSON.parse(File.read(bp))
        data["fingerprints"].as_a.each { |x| baseline << x.as_s }
      rescue ex
        STDERR.puts "error: cannot read baseline #{bp}: #{ex.message}"
        return 2
      end
    end

    files = [] of String
    begin
      paths.each { |p| files.concat(discover(p)) }
    rescue ex : Error
      STDERR.puts "error: #{ex.message}"
      return 2
    end
    files.uniq!
    if files.size > max_files
      STDERR.puts "error: #{files.size} files found, limit is #{max_files}. Raise --max-files if that is intended."
      return 2
    end
    if files.empty?
      STDERR.puts "error: no workflow files found under #{paths.join(", ")}"
      return 2
    end

    all = [] of Finding
    suppressed = 0
    scanned = 0
    files.each do |path|
      begin
        text = read_file(path, max_bytes)
        found, sup = analyze_text(path, text)
        suppressed += sup
        all.concat(found)
        scanned += 1
      rescue ex : Error | File::Error
        all << Finding.new("WFA000", Severity::High, path, 1, nil, nil, "Could not read this file: #{ex.message}", "", "Fix permissions or size, or exclude the file deliberately.")
      end
    end

    all.select! { |f| only.empty? || only.includes?(f.rule) }
    all.reject! { |f| ignore.includes?(f.rule) }
    all.select! { |f| f.severity >= min }

    if wb = write_baseline
      File.write(wb, JSON.build(indent: 2) do |j|
        j.object do
          j.field "version", 1
          j.field "fingerprints" { j.array { all.map(&.fingerprint).uniq!.sort!.each { |x| j.string x } } }
        end
      end + "\n")
      STDERR.puts "wrote #{all.size} fingerprints to #{wb}"
      return 0
    end

    baselined = 0
    matched = Set(String).new
    unless baseline.empty?
      all.reject! do |f|
        if baseline.includes?(f.fingerprint)
          matched << f.fingerprint
          baselined += 1
          true
        else
          false
        end
      end
      stale = baseline.size - matched.size
      STDERR.puts "note: #{stale} baseline entries matched nothing and can be removed" if stale > 0
    end

    sorted = sort_findings(all)
    case format
    when "json"  then puts json_report(sorted, scanned, suppressed, baselined)
    when "sarif" then puts sarif_report(sorted)
    else              print text_report(sorted, scanned, suppressed, baselined)
    end

    threshold = fail_on
    return 1 if threshold && sorted.any? { |f| f.severity >= threshold }
    0
  end
end

exit ActionsWorkflowHardeningAuditor.run(ARGV)
