# Complexity Regression Sentinel

A three line pull request can turn an O(n) function into O(n^2) and no reviewer will notice. This is a single file Rust CLI that fingerprints every function's loop nesting, diffs that fingerprint between two git refs and fails the build when the shape of the code gets worse.

**Language:** Rust | **Lines:** 1229 | **Added:** 2026-08-28

## What this solves

This solves the complexity regression problem that slips past most code review: a PR looks small and reasonable, but it quietly turns a single loop into a nested loop, or drops a linear `.contains()` search inside a loop that already runs per item, and nobody notices until the service falls over at ten times last quarter's data volume. Human reviewers and even LLM reviewers read diffs line by line and rarely trace nesting depth across a whole function. An O(n) function becoming O(n^2) is one of the easiest regressions to miss and one of the most expensive to find in production.

The failure mode is always the same. A function that walked a list once now walks it inside another walk. The diff shows four added lines and one changed indent level. Tests pass, because tests run on twenty rows. Staging passes, because staging has a thousand rows. Production has four million, and the endpoint that answered in 40 ms now takes 90 seconds or times out. The person who notices is not the reviewer. It is the on call engineer reading a p99 graph at 2 am, or the customer whose import job died.

The lookup version is quieter. Somebody adds `if seen.contains(id)` inside an existing loop because a `Vec` was already sitting there, and a clean O(n) pass becomes O(n^2) with a small constant, invisible until the input triples. None of this is visible in normal review, because the information you need is not local to the diff. You need the nesting depth of the whole enclosing function before and after. That is what this computes and compares.

## Why I built it

I wanted something I could drop into CI that reads a diff the way an experienced engineer skims one: not proving Big-O formally, but asking "did the shape of this function's loops get worse between base and head, and is there a new lookup buried inside a loop that used to be clean." Cyclomatic complexity linters count branches and report an absolute number per file, so on any repo with history you get a wall of pre existing findings and everybody mutes the rule. Profilers answer properly but need a workload and a run, which is not a pre merge gate.

The gap is the comparison. Nothing standard says "this function was one loop deep on main and is three loops deep on your branch." That is a diff between two structural fingerprints, not two blobs of text, and it is cheap enough to run on every pull request. It matters more now that so many PRs are agent generated, where the code compiles, reads plausibly and still nests a loop it did not need to.

## When to use it

- Gating merges in CI on a service where input size grows with customer count and nobody profiles every change.
- Reviewing agent generated pull requests, where the code reads fine and the loop structure is the thing you cannot see.
- Triaging a legacy file before you touch it: static mode ranks its nesting hotspots by severity.
- Feeding GitHub code scanning, since SARIF output drops into the security tab with file and line annotations.
- Hunting the cause after a latency regression already shipped, by diffing the release tag against the previous one.
- Enforcing a house rule like "nothing deeper than two nested loops" using `--min-nesting`.

## How it works

Everything starts with masking. `mask_non_code` runs a state machine with states for code, line comments, block comments, quoted strings and triple quoted strings, and blanks out everything that is not code. The rest of the tool is keyword and brace scanning, so a `for` inside a docstring or a `}` inside a string literal would corrupt the depth counter. Two guards matter. Rust and C++ lifetime markers like `'a` and `'static` are explicitly not treated as char literals, checked by looking ahead for a closing quote in the right position, otherwise every generic function breaks brace matching. And an unterminated string resets to code state at the newline, so one stray quote cannot mask half the file.

Function extraction splits by language family, with `Language::from_path` mapping the extension onto thirteen languages and `is_indent` true only for Python. For brace languages `extract_function_name` tokenises each line with `collect_words`, rejects lines starting with a control keyword, requires a paren, and looks for `fn`, `func`, `function` or `def`. Go's receiver form `func (s *Server) Handle(...)` is handled by counting paren depth past the receiver group and taking the next word. With no function keyword it falls back to modifiers like `public`, `static` or `export` and takes the last identifier before the paren, which covers Java, C# and TypeScript methods. `find_body_start` then separates a real body `{` from a bare `;` declaration so prototypes are skipped, and `find_matching_close` counts brace depth to find the end.

The measurement is `walk_loops_brace`. It keeps a brace depth counter and a `loop_stack` of the depths at which loop bodies opened. Reading `for`, `while`, `loop` or `foreach` sets a `pending_loop` flag and the next `{` pushes onto the stack. The high water mark of that stack is `max_nesting`. While the stack is non empty, and only then, it also scans case insensitively for `LOOKUP_HINTS` (`.contains(`, `.find(`, `.index_of(`, `.indexof(`, `.position(`, `.includes(`) and counts those as in loop linear lookups. `count_calls` counts self recursion by matching the function's own identifier followed by an open paren. Those three numbers become a `ComplexityProfile`. Python takes a parallel path where `analyze_python_function` swaps the brace stack for an indent stack: a `for` or `while` header ending in `:` pushes its column and a shallower line pops it.

Comparison is the point. `build_profiles` returns a map from function name to profile, with `insert_unique` disambiguating overloads as `name#2` and up. In diff mode `git_changed_files` runs `git diff --name-only base...head`, falls back to the two dot form, and `git_show` pulls each version as `rev:path`. `analyze_diff` joins the two maps by name and calls `classify_regression`, an ordered rule ladder: new recursion while already two loops deep is Critical, a nesting jump of two or more is Critical, plus one landing at three or deeper is High, plus one otherwise is Medium, and new in loop lookups or plain new recursion are Medium. A function in head but absent from base falls to `classify_absolute`, which scores against `--min-nesting` and is also the whole of static mode. Findings sort by severity then file then line, print as human text, JSON or SARIF 2.1.0, and exit 1 if anything meets `--fail-on`.

## Usage

```bash
# Compile once. No crates, no Cargo, std only.
rustc ComplexityRegressionSentinel.rs -O -o complexity-sentinel

# Default diff mode: HEAD~1 against HEAD, human output, fail on high or worse.
./complexity-sentinel

# The CI gate. Fails the job on a nesting regression.
./complexity-sentinel --base origin/main --head HEAD --fail-on high

# SARIF for GitHub code scanning.
./complexity-sentinel --base origin/main --head HEAD --format sarif > complexity.sarif

# Static mode: score files on their own, no git needed. Bare paths work too.
./complexity-sentinel --path src/pipeline.rs --path worker/queue.go
./complexity-sentinel --no-diff --path app/views.py --min-nesting 4

# Machine readable, never fails the build.
./complexity-sentinel --format json --fail-on off

./complexity-sentinel --help
./complexity-sentinel --version

# Run the 11 built in tests.
rustc --test ComplexityRegressionSentinel.rs -o sentinel-tests && ./sentinel-tests
```

## Notes

- Heuristic text analysis, not a parser and not a proof. Treat a finding as a prompt to look at the function, not a verdict. The usage text says the same thing.
- Loop headers containing semicolons are missed. `pending_loop` is cleared by `;`, so the three clause `for (i = 0; i < n; i++)` is not counted as a loop. Range loops, `for...of`, `for...in`, `while`, `foreach` and Rust's `loop` are counted. This mostly affects older C, C++, Java and C# code.
- Lookup detection is name based, so `.find(` on a `BTreeMap` or a C++ `std::map` is logarithmic but still counts as a hit. Expect some Low severity noise.
- Functions are matched between base and head by name only. A rename reads as a deletion plus a new function, and overload disambiguation (`name#2`) is positional, so inserting an overload above another shifts the pairing.
- Exit codes: 0 clean or below threshold, 1 when a finding meets `--fail-on` (default `high`, `off` disables), 2 for a bad flag, a git diff failure, or `--no-diff` with no `--path`. An unreadable file in static mode warns on stderr and continues.
- In diff mode, unrecognised extensions are skipped, deleted files ignored, and files added in the branch scored with the absolute rules as `new-hotspot`. Only `git` on PATH is required. Nothing outside the Rust standard library.
