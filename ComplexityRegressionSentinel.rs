use std::collections::HashMap;
use std::env;
use std::fs;
use std::process::{self, Command};

const VERSION: &str = "1.0.0";
const DEFAULT_MIN_NESTING: usize = 3;

const FUNC_KW: &[&str] = &["fn", "func", "function", "def"];
const MODIFIER_KW: &[&str] = &[
    "public", "private", "protected", "internal", "static", "virtual", "override", "async",
    "export",
];
const CONTROL_KW: &[&str] = &[
    "if", "for", "while", "switch", "catch", "else", "do", "match", "when", "foreach",
];
const LOOP_KW: &[&str] = &["for", "while", "loop", "foreach"];
const LOOKUP_HINTS: &[&str] = &[
    ".contains(",
    ".find(",
    ".index_of(",
    ".indexof(",
    ".position(",
    ".includes(",
];

// ---------------------------------------------------------------------------
// Severity / output format
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Severity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

impl Severity {
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "info" => Some(Self::Info),
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            "critical" => Some(Self::Critical),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }

    fn sarif_level(self) -> &'static str {
        match self {
            Self::Critical | Self::High => "error",
            Self::Medium => "warning",
            Self::Low | Self::Info => "note",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputFormat {
    Human,
    Json,
    Sarif,
}

impl OutputFormat {
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "human" | "text" => Some(Self::Human),
            "json" => Some(Self::Json),
            "sarif" => Some(Self::Sarif),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Language detection
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Language {
    Rust,
    Go,
    JavaScript,
    TypeScript,
    Java,
    C,
    Cpp,
    CSharp,
    Kotlin,
    Swift,
    Php,
    Scala,
    Python,
    Unknown,
}

impl Language {
    fn from_path(path: &str) -> Self {
        let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
        match ext.as_str() {
            "rs" => Self::Rust,
            "go" => Self::Go,
            "js" | "mjs" | "cjs" | "jsx" => Self::JavaScript,
            "ts" | "tsx" => Self::TypeScript,
            "java" => Self::Java,
            "c" | "h" => Self::C,
            "cpp" | "cc" | "cxx" | "hpp" | "hh" => Self::Cpp,
            "cs" => Self::CSharp,
            "kt" | "kts" => Self::Kotlin,
            "swift" => Self::Swift,
            "php" => Self::Php,
            "scala" => Self::Scala,
            "py" => Self::Python,
            _ => Self::Unknown,
        }
    }

    fn is_indent(self) -> bool {
        matches!(self, Self::Python)
    }
}

// ---------------------------------------------------------------------------
// Source masking: blank out comments and string/char literals so keyword and
// brace scanning never trips over text that only looks like code.
// ---------------------------------------------------------------------------

fn mask_non_code(chars: &[char], lang: Language) -> Vec<char> {
    #[derive(PartialEq)]
    enum St {
        Code,
        Line,
        Block,
        Str(char),
        Triple(char),
    }

    let n = chars.len();
    let mut out = chars.to_vec();
    let mut state = St::Code;
    let mut i = 0;

    while i < n {
        let c = chars[i];
        match state {
            St::Code => {
                if !lang.is_indent() && c == '/' && i + 1 < n && chars[i + 1] == '/' {
                    out[i] = ' ';
                    state = St::Line;
                } else if lang.is_indent() && c == '#' {
                    out[i] = ' ';
                    state = St::Line;
                } else if !lang.is_indent() && c == '/' && i + 1 < n && chars[i + 1] == '*' {
                    out[i] = ' ';
                    state = St::Block;
                } else if c == '"' {
                    if i + 2 < n && chars[i + 1] == '"' && chars[i + 2] == '"' {
                        out[i] = ' ';
                        out[i + 1] = ' ';
                        out[i + 2] = ' ';
                        state = St::Triple('"');
                        i += 2;
                    } else {
                        out[i] = ' ';
                        state = St::Str('"');
                    }
                } else if c == '\'' {
                    if lang.is_indent() {
                        if i + 2 < n && chars[i + 1] == '\'' && chars[i + 2] == '\'' {
                            out[i] = ' ';
                            out[i + 1] = ' ';
                            out[i + 2] = ' ';
                            state = St::Triple('\'');
                            i += 2;
                        } else {
                            out[i] = ' ';
                            state = St::Str('\'');
                        }
                    } else {
                        // Guard against Rust/C++ lifetime and label markers such as
                        // 'a or 'static, which are not char literals and must be
                        // left alone or brace/keyword scanning downstream breaks.
                        let looks_like_char_literal = (i + 2 < n
                            && chars[i + 1] != '\\'
                            && chars[i + 2] == '\'')
                            || (i + 3 < n && chars[i + 1] == '\\' && chars[i + 3] == '\'');
                        if looks_like_char_literal {
                            out[i] = ' ';
                            state = St::Str('\'');
                        }
                    }
                }
            }
            St::Line => {
                if c == '\n' {
                    state = St::Code;
                } else {
                    out[i] = ' ';
                }
            }
            St::Block => {
                out[i] = ' ';
                if c == '*' && i + 1 < n && chars[i + 1] == '/' {
                    out[i + 1] = ' ';
                    i += 1;
                    state = St::Code;
                }
            }
            St::Str(q) => {
                if c == '\n' {
                    // Unterminated literal guard: never let a stray quote mask
                    // the rest of the file.
                    state = St::Code;
                } else {
                    out[i] = ' ';
                    if c == '\\' && i + 1 < n {
                        out[i + 1] = ' ';
                        i += 1;
                    } else if c == q {
                        state = St::Code;
                    }
                }
            }
            St::Triple(q) => {
                out[i] = ' ';
                if c == q && i + 2 < n && chars[i + 1] == q && chars[i + 2] == q {
                    out[i + 1] = ' ';
                    out[i + 2] = ' ';
                    i += 2;
                    state = St::Code;
                }
            }
        }
        i += 1;
    }
    out
}

fn line_of(code: &[char], idx: usize) -> usize {
    code[..idx.min(code.len())]
        .iter()
        .filter(|&&c| c == '\n')
        .count()
        + 1
}

// ---------------------------------------------------------------------------
// Brace-language function extraction
// ---------------------------------------------------------------------------

struct Word {
    text: String,
    start: usize,
    end: usize,
}

fn collect_words(code: &[char], start: usize, end: usize) -> Vec<Word> {
    let mut out = Vec::new();
    let mut i = start;
    while i < end {
        if code[i].is_alphabetic() || code[i] == '_' {
            let s = i;
            let mut j = i;
            while j < end && (code[j].is_alphanumeric() || code[j] == '_') {
                j += 1;
            }
            out.push(Word {
                text: code[s..j].iter().collect(),
                start: s,
                end: j,
            });
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

fn extract_function_name(code: &[char], line_start: usize, line_end: usize) -> Option<String> {
    let words = collect_words(code, line_start, line_end);
    if words.is_empty() {
        return None;
    }
    if CONTROL_KW.contains(&words[0].text.as_str()) {
        return None;
    }
    let has_paren = (line_start..line_end).any(|i| code[i] == '(');
    if !has_paren {
        return None;
    }

    for (wi, w) in words.iter().enumerate() {
        if FUNC_KW.contains(&w.text.as_str()) {
            let mut p = w.end;
            while p < line_end && code[p].is_whitespace() {
                p += 1;
            }
            if p < line_end && code[p] == '(' {
                // Receiver or anonymous form, e.g. Go's `func (r *T) Name(...)`.
                let mut depth = 0i32;
                let mut q = p;
                while q < line_end {
                    match code[q] {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                q += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                    q += 1;
                }
                return words.iter().find(|w2| w2.start >= q).map(|w2| w2.text.clone());
            } else if let Some(next) = words.get(wi + 1) {
                return Some(next.text.clone());
            }
        }
    }

    if words.iter().any(|w| MODIFIER_KW.contains(&w.text.as_str())) {
        if let Some(paren_idx) = (line_start..line_end).find(|&i| code[i] == '(') {
            if let Some(w) = words.iter().rev().find(|w| w.end <= paren_idx) {
                if !CONTROL_KW.contains(&w.text.as_str()) {
                    return Some(w.text.clone());
                }
            }
        }
    }
    None
}

fn find_body_start(code: &[char], from: usize, n: usize) -> Option<(usize, bool)> {
    let limit = (from + 4000).min(n);
    let mut paren_depth = 0i32;
    let mut seen_paren = false;
    let mut i = from;
    while i < limit {
        match code[i] {
            '(' => {
                paren_depth += 1;
                seen_paren = true;
            }
            ')' => paren_depth -= 1,
            '{' if seen_paren && paren_depth <= 0 => return Some((i, false)),
            ';' if seen_paren && paren_depth <= 0 => return Some((i, true)),
            _ => {}
        }
        i += 1;
    }
    None
}

fn find_matching_close(code: &[char], open_idx: usize, n: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open_idx;
    while i < n {
        match code[i] {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

struct FunctionSpan {
    name: String,
    start_line: usize,
    body_start: usize,
    body_end: usize,
}

fn extract_functions_brace(code: &[char]) -> Vec<FunctionSpan> {
    let n = code.len();
    let mut out = Vec::new();
    let mut idx = 0;
    while idx < n {
        let line_start = idx;
        let mut line_end = idx;
        while line_end < n && code[line_end] != '\n' {
            line_end += 1;
        }
        if let Some(name) = extract_function_name(code, line_start, line_end) {
            if let Some((open_idx, decl_only)) = find_body_start(code, line_start, n) {
                if !decl_only {
                    if let Some(close_idx) = find_matching_close(code, open_idx, n) {
                        out.push(FunctionSpan {
                            name,
                            start_line: line_of(code, line_start),
                            body_start: open_idx + 1,
                            body_end: close_idx,
                        });
                    }
                }
            }
        }
        idx = if line_end < n { line_end + 1 } else { n };
    }
    out
}

// ---------------------------------------------------------------------------
// Complexity profiling
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
struct ComplexityProfile {
    line: usize,
    max_nesting: usize,
    lookup_hits: usize,
    recursive_calls: usize,
}

fn walk_loops_brace(code: &[char], start: usize, end: usize) -> (usize, usize) {
    let mut depth = 0usize;
    let mut loop_stack: Vec<usize> = Vec::new();
    let mut max_nest = 0usize;
    let mut hits = 0usize;
    let mut pending_loop = false;
    let mut i = start;

    while i < end {
        let c = code[i];
        if c == '{' {
            depth += 1;
            if pending_loop {
                loop_stack.push(depth);
                pending_loop = false;
                if loop_stack.len() > max_nest {
                    max_nest = loop_stack.len();
                }
            }
        } else if c == '}' {
            if let Some(&top) = loop_stack.last() {
                if top == depth {
                    loop_stack.pop();
                }
            }
            depth = depth.saturating_sub(1);
        } else if c == ';' {
            pending_loop = false;
        } else if c.is_alphabetic() || c == '_' {
            let s = i;
            let mut j = i;
            while j < end && (code[j].is_alphanumeric() || code[j] == '_') {
                j += 1;
            }
            let word: String = code[s..j].iter().collect();
            if LOOP_KW.contains(&word.as_str()) {
                pending_loop = true;
            }
            i = j;
            continue;
        } else if !loop_stack.is_empty() {
            for hint in LOOKUP_HINTS {
                let hlen = hint.chars().count();
                if i + hlen <= end {
                    let slice: String = code[i..i + hlen].iter().collect();
                    if slice.eq_ignore_ascii_case(hint) {
                        hits += 1;
                    }
                }
            }
        }
        i += 1;
    }
    (max_nest, hits)
}

fn count_calls(code: &[char], start: usize, end: usize, name: &str) -> usize {
    let mut count = 0;
    let mut i = start;
    let name_chars: Vec<char> = name.chars().collect();
    while i < end {
        if code[i].is_alphabetic() || code[i] == '_' {
            let s = i;
            let mut j = i;
            while j < end && (code[j].is_alphanumeric() || code[j] == '_') {
                j += 1;
            }
            if code[s..j] == name_chars[..] {
                let mut k = j;
                while k < end && code[k].is_whitespace() {
                    k += 1;
                }
                if k < end && code[k] == '(' {
                    count += 1;
                }
            }
            i = j;
        } else {
            i += 1;
        }
    }
    count
}

fn analyze_brace_function(code: &[char], f: &FunctionSpan) -> ComplexityProfile {
    let (max_nesting, lookup_hits) = walk_loops_brace(code, f.body_start, f.body_end);
    let recursive_calls = count_calls(code, f.body_start, f.body_end, &f.name);
    ComplexityProfile {
        line: f.start_line,
        max_nesting,
        lookup_hits,
        recursive_calls,
    }
}

// ---------------------------------------------------------------------------
// Python (indentation) extraction and analysis
// ---------------------------------------------------------------------------

struct PyFunctionSpan {
    name: String,
    def_line_idx: usize,
    body_start_line: usize,
    body_end_line: usize,
}

fn extract_functions_indent(lines: &[&str]) -> Vec<PyFunctionSpan> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        let after = if let Some(rest) = trimmed.strip_prefix("async def ") {
            Some(rest)
        } else {
            trimmed.strip_prefix("def ")
        };
        if let Some(after) = after {
            let name: String = after
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            let def_line_idx = i;
            let mut j = i + 1;
            let mut end_line = i;
            while j < lines.len() {
                let l = lines[j];
                if l.trim().is_empty() {
                    j += 1;
                    continue;
                }
                let ind2 = l.len() - l.trim_start().len();
                if ind2 <= indent {
                    break;
                }
                end_line = j;
                j += 1;
            }
            if !name.is_empty() {
                out.push(PyFunctionSpan {
                    name,
                    def_line_idx,
                    body_start_line: def_line_idx + 1,
                    body_end_line: end_line,
                });
            }
            i = j;
            continue;
        }
        i += 1;
    }
    out
}

fn analyze_python_function(lines: &[&str], f: &PyFunctionSpan) -> ComplexityProfile {
    let mut stack: Vec<usize> = Vec::new();
    let mut max_nest = 0usize;
    let mut hits = 0usize;
    let mut recursive_calls = 0usize;
    let call_pattern = format!("{}(", f.name);
    let last = f.body_end_line.min(lines.len().saturating_sub(1));

    for ln in f.body_start_line..=last {
        if ln >= lines.len() || ln < f.body_start_line {
            continue;
        }
        let line = lines[ln];
        if line.trim().is_empty() {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        while let Some(&top) = stack.last() {
            if indent <= top {
                stack.pop();
            } else {
                break;
            }
        }
        let trimmed = line.trim_start();
        let first_word: String = trimmed
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        let is_loop_header =
            (first_word == "for" || first_word == "while") && trimmed.trim_end().ends_with(':');
        if is_loop_header {
            stack.push(indent);
            if stack.len() > max_nest {
                max_nest = stack.len();
            }
        }
        if !stack.is_empty() {
            let lower = line.to_ascii_lowercase();
            for hint in LOOKUP_HINTS {
                if lower.contains(hint) {
                    hits += 1;
                }
            }
        }
        if line.contains(&call_pattern) {
            recursive_calls += 1;
        }
    }

    ComplexityProfile {
        line: f.def_line_idx + 1,
        max_nesting: max_nest,
        lookup_hits: hits,
        recursive_calls,
    }
}

// ---------------------------------------------------------------------------
// Profile building and diffing
// ---------------------------------------------------------------------------

fn insert_unique(map: &mut HashMap<String, ComplexityProfile>, name: String, profile: ComplexityProfile) {
    if !map.contains_key(&name) {
        map.insert(name, profile);
        return;
    }
    let mut n = 2;
    loop {
        let candidate = format!("{name}#{n}");
        if !map.contains_key(&candidate) {
            map.insert(candidate, profile);
            return;
        }
        n += 1;
    }
}

fn build_profiles(path: &str, src: &str) -> HashMap<String, ComplexityProfile> {
    let lang = Language::from_path(path);
    let mut map = HashMap::new();
    if lang.is_indent() {
        let lines: Vec<&str> = src.lines().collect();
        for f in extract_functions_indent(&lines) {
            let profile = analyze_python_function(&lines, &f);
            insert_unique(&mut map, f.name.clone(), profile);
        }
    } else if lang != Language::Unknown {
        let chars: Vec<char> = src.chars().collect();
        let code = mask_non_code(&chars, lang);
        for f in extract_functions_brace(&code) {
            let profile = analyze_brace_function(&code, &f);
            insert_unique(&mut map, f.name.clone(), profile);
        }
    }
    map
}

struct Finding {
    file: String,
    function: String,
    line: usize,
    severity: Severity,
    kind: &'static str,
    message: String,
}

fn classify_regression(base: &ComplexityProfile, head: &ComplexityProfile) -> Option<(Severity, String)> {
    let nesting_delta = head.max_nesting as i64 - base.max_nesting as i64;
    let new_lookup = head.lookup_hits.saturating_sub(base.lookup_hits);
    let new_recursion = head.recursive_calls > 0 && base.recursive_calls == 0;

    if new_recursion && head.max_nesting >= 2 {
        return Some((
            Severity::Critical,
            format!(
                "function became recursive while already {} loops deep; check for exponential blowup",
                head.max_nesting
            ),
        ));
    }
    if nesting_delta >= 2 {
        return Some((
            Severity::Critical,
            format!(
                "loop nesting jumped from {} to {} (roughly O(n^{}))",
                base.max_nesting, head.max_nesting, head.max_nesting
            ),
        ));
    }
    if nesting_delta == 1 && head.max_nesting >= 3 {
        return Some((
            Severity::High,
            format!(
                "loop nesting increased from {} to {} inside an already deep loop",
                base.max_nesting, head.max_nesting
            ),
        ));
    }
    if nesting_delta == 1 {
        return Some((
            Severity::Medium,
            format!("loop nesting increased from {} to {}", base.max_nesting, head.max_nesting),
        ));
    }
    if new_lookup > 0 {
        return Some((
            Severity::Medium,
            format!(
                "{new_lookup} new linear lookup call(s) added inside an existing loop (possible O(n^2))"
            ),
        ));
    }
    if new_recursion {
        return Some((Severity::Medium, "function became recursive".to_string()));
    }
    None
}

fn classify_absolute(p: &ComplexityProfile, min_nesting: usize) -> Option<(Severity, String)> {
    if p.max_nesting >= min_nesting + 2 {
        Some((
            Severity::Critical,
            format!("{} nested loops — verify this isn't accidental combinatorial blowup", p.max_nesting),
        ))
    } else if p.max_nesting >= min_nesting + 1 {
        Some((Severity::High, format!("{} nested loops", p.max_nesting)))
    } else if p.max_nesting >= min_nesting {
        Some((
            Severity::Medium,
            format!("{} nested loops, at the configured threshold", p.max_nesting),
        ))
    } else if p.lookup_hits > 0 {
        Some((
            Severity::Low,
            format!("{} linear lookup call(s) found inside a loop", p.lookup_hits),
        ))
    } else if p.recursive_calls > 0 && p.max_nesting >= 1 {
        Some((Severity::Low, "recursive function also contains a loop".to_string()))
    } else {
        None
    }
}

fn analyze_diff(path: &str, base_src: &str, head_src: &str, config: &Config, findings: &mut Vec<Finding>) {
    let base_map = build_profiles(path, base_src);
    let head_map = build_profiles(path, head_src);
    for (name, head_p) in &head_map {
        if let Some(base_p) = base_map.get(name) {
            if let Some((severity, message)) = classify_regression(base_p, head_p) {
                findings.push(Finding {
                    file: path.to_string(),
                    function: name.clone(),
                    line: head_p.line,
                    severity,
                    kind: "regression",
                    message,
                });
            }
        } else if let Some((severity, message)) = classify_absolute(head_p, config.min_nesting) {
            findings.push(Finding {
                file: path.to_string(),
                function: name.clone(),
                line: head_p.line,
                severity,
                kind: "new-hotspot",
                message,
            });
        }
    }
}

fn analyze_static(path: &str, src: &str, config: &Config, findings: &mut Vec<Finding>) {
    let map = build_profiles(path, src);
    for (name, p) in &map {
        if let Some((severity, message)) = classify_absolute(p, config.min_nesting) {
            findings.push(Finding {
                file: path.to_string(),
                function: name.clone(),
                line: p.line,
                severity,
                kind: "hotspot",
                message,
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Git plumbing
// ---------------------------------------------------------------------------

fn run_git(args: &[&str]) -> Result<String, String> {
    let out = Command::new("git").args(args).output().map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

fn git_changed_files(base: &str, head: &str) -> Result<Vec<String>, String> {
    let triple = format!("{base}...{head}");
    let output = match run_git(&["diff", "--name-only", &triple]) {
        Ok(s) => s,
        Err(_) => run_git(&["diff", "--name-only", base, head])?,
    };
    Ok(output.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect())
}

fn git_show(rev: &str, path: &str) -> Option<String> {
    let spec = format!("{rev}:{path}");
    run_git(&["show", &spec]).ok()
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Config {
    paths: Vec<String>,
    base: Option<String>,
    head: String,
    format: OutputFormat,
    fail_on: Option<Severity>,
    min_nesting: usize,
    no_diff: bool,
}

impl Config {
    fn parse(args: &[String]) -> Result<Config, String> {
        let mut paths = Vec::new();
        let mut base = None;
        let mut head = "HEAD".to_string();
        let mut format = OutputFormat::Human;
        let mut fail_on = Some(Severity::High);
        let mut min_nesting = DEFAULT_MIN_NESTING;
        let mut no_diff = false;
        let mut i = 0;

        while i < args.len() {
            match args[i].as_str() {
                "--path" => {
                    i += 1;
                    paths.push(args.get(i).ok_or("--path needs a value")?.clone());
                }
                "--base" => {
                    i += 1;
                    base = Some(args.get(i).ok_or("--base needs a value")?.clone());
                }
                "--head" => {
                    i += 1;
                    head = args.get(i).ok_or("--head needs a value")?.clone();
                }
                "--format" => {
                    i += 1;
                    let v = args.get(i).ok_or("--format needs a value")?;
                    format = OutputFormat::parse(v).ok_or_else(|| format!("unknown format: {v}"))?;
                }
                "--fail-on" => {
                    i += 1;
                    let v = args.get(i).ok_or("--fail-on needs a value")?;
                    fail_on = if v.eq_ignore_ascii_case("off") {
                        None
                    } else {
                        Some(Severity::parse(v).ok_or_else(|| format!("unknown severity: {v}"))?)
                    };
                }
                "--min-nesting" => {
                    i += 1;
                    let v = args.get(i).ok_or("--min-nesting needs a value")?;
                    min_nesting = v.parse().map_err(|_| format!("invalid --min-nesting: {v}"))?;
                }
                "--no-diff" => no_diff = true,
                other if other.starts_with("--") => return Err(format!("unknown option: {other}")),
                other => paths.push(other.to_string()),
            }
            i += 1;
        }

        Ok(Config { paths, base, head, format, fail_on, min_nesting, no_diff })
    }
}

fn json_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            _ => out.push(ch),
        }
    }
    out
}

fn print_human(findings: &[Finding]) {
    if findings.is_empty() {
        println!("ComplexityRegressionSentinel: no complexity regressions found.");
        return;
    }
    for f in findings {
        println!(
            "[{}] {}:{} {}() — {}",
            f.severity.as_str(),
            f.file,
            f.line,
            f.function,
            f.message
        );
    }
    println!("\n{} finding(s).", findings.len());
}

fn print_json(findings: &[Finding]) {
    let mut out = String::from("[");
    for (i, f) in findings.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "{{\"file\":\"{}\",\"function\":\"{}\",\"line\":{},\"severity\":\"{}\",\"kind\":\"{}\",\"message\":\"{}\"}}",
            json_escape(&f.file),
            json_escape(&f.function),
            f.line,
            f.severity.as_str(),
            f.kind,
            json_escape(&f.message)
        ));
    }
    out.push(']');
    println!("{out}");
}

fn print_sarif(findings: &[Finding]) {
    let mut results = String::new();
    for (i, f) in findings.iter().enumerate() {
        if i > 0 {
            results.push(',');
        }
        results.push_str(&format!(
            "{{\"ruleId\":\"{}\",\"level\":\"{}\",\"message\":{{\"text\":\"{}\"}},\"locations\":[{{\"physicalLocation\":{{\"artifactLocation\":{{\"uri\":\"{}\"}},\"region\":{{\"startLine\":{}}}}}}}]}}",
            f.kind,
            f.severity.sarif_level(),
            json_escape(&format!("{}: {}", f.function, f.message)),
            json_escape(&f.file),
            f.line.max(1)
        ));
    }
    println!(
        "{{\"version\":\"2.1.0\",\"$schema\":\"https://json.schemastore.org/sarif-2.1.0.json\",\"runs\":[{{\"tool\":{{\"driver\":{{\"name\":\"ComplexityRegressionSentinel\",\"version\":\"{VERSION}\"}}}},\"results\":[{results}]}}]}}"
    );
}

fn usage() -> String {
    format!(
        "ComplexityRegressionSentinel v{VERSION}\n\n\
Usage:\n  rustc ComplexityRegressionSentinel.rs -O -o complexity-sentinel\n  ./complexity-sentinel [OPTIONS]\n\n\
Git diff mode (default): compares --base against --head and flags functions\nwhose loop nesting or in-loop linear lookups grew.\n\n\
Static mode: pass one or more --path files (or bare paths) and every function\nis scored on its own, no git repository required.\n\n\
Options:\n  \
--path PATH             Analyze PATH directly (repeatable). Enables static mode.\n  \
--base REF              Git ref to diff from. Default: HEAD~1.\n  \
--head REF              Git ref to diff to. Default: HEAD.\n  \
--format FORMAT         human, json, or sarif. Default: human.\n  \
--fail-on SEVERITY      off, info, low, medium, high, or critical. Default: high.\n  \
--min-nesting N         Loop nesting depth treated as a hotspot on its own. Default: {DEFAULT_MIN_NESTING}.\n  \
--no-diff               Force static mode (requires --path).\n\n\
Supported languages: Rust, Go, JavaScript, TypeScript, Java, C, C++, C#,\nKotlin, Swift, PHP, Scala, Python. Detection is heuristic text analysis,\nnot a real parser — treat findings as a prompt to look, not a proof.\n"
    )
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print!("{}", usage());
        return;
    }
    if args.iter().any(|a| a == "--version") {
        println!("ComplexityRegressionSentinel {VERSION}");
        return;
    }

    let config = match Config::parse(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}\n\n{}", usage());
            process::exit(2);
        }
    };

    let mut findings: Vec<Finding> = Vec::new();

    if config.no_diff || !config.paths.is_empty() {
        if config.paths.is_empty() {
            eprintln!("error: --no-diff requires at least one --path");
            process::exit(2);
        }
        for path in &config.paths {
            match fs::read_to_string(path) {
                Ok(content) => analyze_static(path, &content, &config, &mut findings),
                Err(e) => eprintln!("warn: could not read {path}: {e}"),
            }
        }
    } else {
        let base = config.base.clone().unwrap_or_else(|| "HEAD~1".to_string());
        let head = config.head.clone();
        let changed = match git_changed_files(&base, &head) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("error: git diff failed: {e}");
                process::exit(2);
            }
        };
        for path in changed {
            if Language::from_path(&path) == Language::Unknown {
                continue;
            }
            let base_content = git_show(&base, &path);
            let head_content = git_show(&head, &path);
            match (base_content, head_content) {
                (Some(b), Some(h)) => analyze_diff(&path, &b, &h, &config, &mut findings),
                (None, Some(h)) => analyze_static(&path, &h, &config, &mut findings),
                _ => {}
            }
        }
    }

    findings.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then_with(|| a.file.cmp(&b.file))
            .then_with(|| a.line.cmp(&b.line))
    });

    match config.format {
        OutputFormat::Human => print_human(&findings),
        OutputFormat::Json => print_json(&findings),
        OutputFormat::Sarif => print_sarif(&findings),
    }

    if let Some(threshold) = config.fail_on {
        if findings.iter().any(|f| f.severity >= threshold) {
            process::exit(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_comments_and_strings_without_breaking_rust_lifetimes() {
        let src = "fn f<'a>(s: &'a str) -> &'a str { \"x\" } // done";
        let chars: Vec<char> = src.chars().collect();
        let code = mask_non_code(&chars, Language::Rust);
        let masked: String = code.iter().collect();
        assert!(masked.contains("'a"));
        assert!(!masked.contains('x'));
        assert!(!masked.contains("done"));
    }

    #[test]
    fn detects_single_and_double_nested_loops() {
        let src = "fn f(items: &Vec<i32>) {\n    for a in items {\n        for b in items {\n            println!(\"{a} {b}\");\n        }\n    }\n}\n";
        let chars: Vec<char> = src.chars().collect();
        let code = mask_non_code(&chars, Language::Rust);
        let funcs = extract_functions_brace(&code);
        assert_eq!(funcs.len(), 1);
        let profile = analyze_brace_function(&code, &funcs[0]);
        assert_eq!(profile.max_nesting, 2);
    }

    #[test]
    fn flags_lookup_call_inside_loop() {
        let src = "fn f(a: &Vec<i32>, b: &Vec<i32>) {\n    for x in a {\n        if b.contains(x) {\n            println!(\"hit\");\n        }\n    }\n}\n";
        let chars: Vec<char> = src.chars().collect();
        let code = mask_non_code(&chars, Language::Rust);
        let funcs = extract_functions_brace(&code);
        let profile = analyze_brace_function(&code, &funcs[0]);
        assert_eq!(profile.lookup_hits, 1);
    }

    #[test]
    fn detects_self_recursion() {
        let src = "fn fib(n: u64) -> u64 {\n    if n < 2 { return n; }\n    fib(n - 1) + fib(n - 2)\n}\n";
        let chars: Vec<char> = src.chars().collect();
        let code = mask_non_code(&chars, Language::Rust);
        let funcs = extract_functions_brace(&code);
        let profile = analyze_brace_function(&code, &funcs[0]);
        assert_eq!(profile.recursive_calls, 2);
    }

    #[test]
    fn go_receiver_methods_get_the_receiver_less_name() {
        let src = "func (s *Server) Handle(req *Request) {\n    for i := range req.Items {\n        for j := range req.Items {\n            _ = i + j\n        }\n    }\n}\n";
        let chars: Vec<char> = src.chars().collect();
        let code = mask_non_code(&chars, Language::Go);
        let funcs = extract_functions_brace(&code);
        assert_eq!(funcs.len(), 1);
        assert_eq!(funcs[0].name, "Handle");
        let profile = analyze_brace_function(&code, &funcs[0]);
        assert_eq!(profile.max_nesting, 2);
    }

    #[test]
    fn python_indentation_nesting_is_tracked() {
        let src = "def f(items):\n    total = 0\n    for a in items:\n        for b in items:\n            total += a * b\n    return total\n";
        let lines: Vec<&str> = src.lines().collect();
        let funcs = extract_functions_indent(&lines);
        assert_eq!(funcs.len(), 1);
        let profile = analyze_python_function(&lines, &funcs[0]);
        assert_eq!(profile.max_nesting, 2);
    }

    #[test]
    fn classify_regression_flags_new_nesting_as_critical() {
        let base = ComplexityProfile { line: 1, max_nesting: 1, lookup_hits: 0, recursive_calls: 0 };
        let head = ComplexityProfile { line: 1, max_nesting: 3, lookup_hits: 0, recursive_calls: 0 };
        let (severity, _) = classify_regression(&base, &head).expect("should flag");
        assert_eq!(severity, Severity::Critical);
    }

    #[test]
    fn classify_regression_is_quiet_when_nothing_changed() {
        let base = ComplexityProfile { line: 1, max_nesting: 1, lookup_hits: 0, recursive_calls: 0 };
        let head = ComplexityProfile { line: 1, max_nesting: 1, lookup_hits: 0, recursive_calls: 0 };
        assert!(classify_regression(&base, &head).is_none());
    }

    #[test]
    fn classify_absolute_respects_threshold() {
        let hot = ComplexityProfile { line: 1, max_nesting: 5, lookup_hits: 0, recursive_calls: 0 };
        let (severity, _) = classify_absolute(&hot, 3).expect("should flag");
        assert_eq!(severity, Severity::Critical);
        let cold = ComplexityProfile { line: 1, max_nesting: 1, lookup_hits: 0, recursive_calls: 0 };
        assert!(classify_absolute(&cold, 3).is_none());
    }

    #[test]
    fn json_escape_handles_control_characters() {
        assert_eq!(json_escape("a\n\"b\""), "a\\n\\\"b\\\"");
    }

    #[test]
    fn duplicate_function_names_get_disambiguated() {
        let mut map = HashMap::new();
        insert_unique(&mut map, "overload".to_string(), ComplexityProfile::default());
        insert_unique(&mut map, "overload".to_string(), ComplexityProfile::default());
        assert!(map.contains_key("overload"));
        assert!(map.contains_key("overload#2"));
    }
}

/*
================================================================================
EXPLANATION
This solves the complexity regression problem that slips past most code
review in 2026: a PR looks small and reasonable, but it quietly turns a
single loop into a nested loop, or drops a linear ".contains()" search
inside a loop that already runs per item, and nobody notices until the
service falls over at ten times last quarter's data volume. Human reviewers
and even LLM reviewers read diffs line by line and rarely trace nesting
depth across a whole function, so an O(n) function becoming O(n^2) is one
of the easiest regressions to miss and one of the most expensive to find in
production. Built because I wanted something I could drop into CI that
reads a diff the way an experienced engineer skims one: not proving Big-O
formally, but asking "did the shape of this function's loops get worse
between base and head, and is there a new lookup buried inside a loop that
used to be clean." Use it when you review agent-generated pull requests,
gate merges on a CI job, or want a fast pre-merge signal on services where
input size grows with customers and nobody is running a profiler on every
change. The trick: strip comments and strings first so keyword matching
never gets fooled by a stray "for" in a docstring or a Rust lifetime like
'a being mistaken for a character literal, then walk the brace or
indentation structure once to fingerprint each function's loop nesting
depth, in-loop lookup calls, and self recursion, and diff that fingerprint
between the base ref and the head ref instead of diffing raw text. It works
across Rust, Go, JavaScript, TypeScript, Java, C, C++, C#, Kotlin, Swift,
PHP, Scala, and Python using one shared heuristic engine, needs nothing but
the Rust standard library and a git binary on PATH, and emits human, JSON,
or SARIF output so it plugs straight into GitHub code scanning. Drop this
into a repository as ComplexityRegressionSentinel.rs, compile it once with
rustc, and run it as `complexity-sentinel --base origin/main --head HEAD
--fail-on high` in your pull request pipeline so a nesting-depth regression
fails the build before it ever reaches production traffic.
================================================================================
*/
