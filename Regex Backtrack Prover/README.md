# Regex Backtrack Prover

A Racket tool that takes a list of regular expressions, decides which ones can freeze a backtracking engine and hands you the exact attack string that proves it. It is a ReDoS scanner that does not stop at "this looks scary": every finding is replayed against a step counting matcher so you see the growth curve yourself.

**Language:** Racket | **Lines:** 1352 | **Added:** 2026-10-26

## What this solves

Regular expression denial of service is still one of the cheapest ways to take down a Node, Python, Java, Ruby or PHP service. One pattern in a validator, a log parser, a markdown renderer or an LLM output guardrail, one hostile string of forty characters and a worker spins at 100 percent CPU for minutes. The usual defence is a linter that pattern matches on shapes like `(a+)+`. That catches the textbook case and misses almost everything real: `(\w+\s?)*`, `^(([a-z])+.)+[A-Z]`, `(?:a|b|ab)*c`, `\s+$` and the email regex somebody copied from a forum in 2014.

The other common defence is "just fuzz it". Fuzzers find the easy cases and miss the ones that need a long, structured input to start hurting. Neither approach tells you whether a pattern is exponential or only quadratic and that matters. Quadratic on a 100 KB request body is still a problem, but it is a different problem from exponential on 30 characters and you fix them differently.

This tool answers three questions for every pattern:

1. Can it blow up as the input grows and is the growth exponential or polynomial?
2. Which input triggers it? You get a prefix, a pump string repeated k times and a suffix that forces the engine to fail.
3. Does it really happen? A reference backtracking matcher counts steps on that input and the result is part of the report.

If the answer to the third question is no, the pattern is reported as SUSPECT, not VULNERABLE. I wanted a tool that is allowed to say "the static analysis worried me but I could not reproduce it" instead of crying wolf.

## Why I built it

I kept running into the same situation in code review. Someone adds a regex to validate a header, strip a prompt template or split tool output from an agent. A reviewer says "this might be ReDoS", nobody can prove it either way and the pattern ships or gets rewritten on a hunch. I wanted a command I could run in CI that gives a yes or no with evidence.

The second reason is that most ReDoS tooling is written in the same language as the engine it analyses, or ships as a thin wrapper around a research paper nobody wants to reimplement. The paper itself is good. Weber and Seidl showed in 1991 that the cost of backtracking is controlled by the ambiguity of the automaton: exponential when some state has two different ways back to itself on the same word, polynomial when two different states can pump the same word between them. Racket is a pleasant language for this kind of thing: symbolic data, structs, closures for continuation passing and an easy time writing a graph algorithm without ceremony. The whole thing is one file with no dependencies beyond the base distribution.

The third reason is the search mode problem. Most tools analyse a pattern as if it were anchored at the start. A real `regex.test(str)` call retries from every start offset, which turns `\d+$` into a quadratic on a long run of digits. I model that explicitly, so the tool finds the same quadratic your profiler will.

## When to use it

- In CI, over every regex literal you extract from a codebase, with `--fail-on` set to the level you can tolerate.
- Before accepting a regex from a user, a config file, a plugin manifest or an LLM that writes validation rules.
- When triaging a CPU spike in a service that parses text, to confirm or rule out a pattern.
- When replacing a regex with a safer one, to check the replacement and show the reviewer the numbers.
- As a teaching tool. The attack strings and step tables make the difference between exponential and polynomial concrete in a way a paper does not.

Do not use it as proof that a pattern is safe on an engine with its own protections. RE2, Go regexp and Rust regex run in linear time regardless and recent V8 can fall back to a linear engine. The tool models the classic backtracking behaviour of Perl, PCRE, Java, Python, Ruby before memoisation and older V8.

## How it works

The pipeline has five stages and each one is a named function in `RegexBacktrackProver.rkt`.

**1. Parsing.** `parse-regex` reads JavaScript style syntax plus the PCRE extras people paste in: atomic groups `(?>...)`, possessive quantifiers `a++`, named groups with `?P<`, `\k<name>` backreferences and lookarounds. Character classes are stored as sorted interval lists over the full Unicode range, so `\w`, `\s`, `.`, negated classes and case folding (the `i` flag) are exact and cheap to intersect. Counted repeats are kept as `{n,m}` nodes.

**2. Position automaton.** `build-nfa` builds a Glushkov automaton: one state per character class leaf and no epsilon edges. Two accepting paths on one word are then two ways for the engine to match. There is one trap here that sinks naive implementations. In `(a+)+` the inner loop and the outer loop both create an edge from the same state to itself and a normal automaton merges them into one. That merge erases exactly the ambiguity you are looking for. `build-nfa` detects the duplicate edge in `add-follow!` and gives the second one its own clone of the target state (`clone!`). Counted repeats are expanded, with optional copies nested the way a backtracking engine really explores them. The `max-positions` option caps the expansion and a pattern over the cap is reported as UNANALYZED, never guessed at. For unanchored patterns an extra state matching any character models the engine retrying every start offset. The automaton is trimmed to states that are both reachable and able to reach a final state.

**3. Ambiguity search.** `find-eda` looks for exponential ambiguity. It walks the pair graph of the automaton with Tarjan's algorithm and looks for a strongly connected component that holds both a diagonal node and an off diagonal node. That is the signature of two different paths from a state back to itself on the same word. The same code extracts a witness: a prefix that reaches the state and a pump word that goes around the cycle. `find-ida` does the polynomial case. It searches the triple product for a path from `(p,p,q)` to `(p,q,q)`, the pattern where state p loops, moves to q and q loops on the same word. It collects up to `max-witnesses` of them under a node budget, because the right witness is often not the first one found. `find-ida` is skipped above 80 positions and the report says so.

**4. Measurement.** `measure-witness` builds attack strings of the form prefix, pump repeated k times, suffix. The suffix is chosen by `outside-chars` and a quick probe: it is a character that cannot continue the match, so the engine has to exhaust every path before it gives up. `run-match` is the reference backtracking matcher. It is written in continuation passing style with Perl semantics: greedy and lazy loops, ordered alternation, captures that restore on backtrack, empty iteration rejection, atomic groups, lookahead and backreferences. It counts every node entry and raises when the step budget is spent. The schedule in `SCHEDULE` runs k from 1 to 16 and then doubles up to 512, stopping at the budget or at `max-attack` characters.

**5. Classification.** `classify` turns the series into growth numbers. Slopes are taken against input length, not against k, so a long pump does not distort the degree. `final-class` then combines the measurement with what the automaton proved: exponential ambiguity plus accelerating or budget exhausting growth means exponential, polynomial ambiguity gives a polynomial degree and when the budget ran out the degree is reported as "at least" a conservative floor, not an inflated guess. The report also carries the input length where the cost reaches `STEP-TARGET`, which is 100 million steps. That number is what you use to set an input length cap.

One more safety net. When the automaton proves nothing, `probe-witnesses` floods the matcher with runs of each distinct character in the pattern. This catches growth that is capped by counted repeats, such as `^(a?){25}a{25}$` or `^(?:a|a){1,60}$`. Those have no unbounded cycle, so no pumping argument applies, but they are still catastrophic. They are reported as "bounded explosion".

Verdicts:

| Status | Meaning |
|---|---|
| vulnerable | Ambiguity proven and growth reproduced. Verdict is exponential, polynomial (with a degree) or bounded. |
| suspect | Ambiguity proven but the reference matcher stayed linear. Usually a match is found before backtracking starts, or an atomic group cuts it off. |
| safe | No ambiguity found and the flood probes stayed linear. |
| unanalyzed | Position budget exceeded. |
| error | The pattern did not parse. The message includes the offset. |

## Usage

You need Racket 8 or newer. There is nothing to install beyond that.

```
racket RegexBacktrackProver.rkt -p '^(a+)+$' -p '\s+$' -p '^[a-z0-9_]{3,16}$'
```

One pattern per line from a file or from stdin. Lines starting with `#` are comments. A line can be a bare pattern, a literal such as `/a+b/i`, or a JSON object with `pattern` and optionally `id` and `flags`:

```
racket RegexBacktrackProver.rkt -f patterns.txt
echo '{"id":"login-user","pattern":"^(\\w+\\s?)*$","flags":"i"}' | racket RegexBacktrackProver.rkt --json
```

Options:

| Flag | Meaning |
|---|---|
| `-p`, `--pattern` | Analyse this pattern. Repeatable. |
| `-f`, `--file` | Read patterns from this file instead of stdin. |
| `--json` | One JSON object per pattern and a final summary object. |
| `--fail-on` | `exponential`, `cubic`, `quadratic` (default), `suspect` or `none`. |
| `--flags` | Flags added to every pattern. The letters `i` and `s` change the analysis. |
| `--mode` | `search` (default, engine retries every offset) or `anchored`. |
| `--max-steps` | Step budget per match, default 2000000. |
| `--max-positions` | Position budget after expanding repeats, default 300. |
| `--max-attack` | Longest attack string in characters, default 1024. |

Exit codes: 0 when nothing at or above the `--fail-on` level was found, 1 when something was, 2 for a usage error or an unreadable file and 3 when nothing failed the gate but some pattern did not parse or could not be analysed. That last one matters in CI: a pattern the tool could not look at must not pass silently.

Sample output:

```
VULNERABLE exponential  1  "^(\\w+\\s?)*$"
  attack: "a" + "aa" x k + "!"
  steps: k=1 64  k=2 256  k=4 4.1k  k=8 1.0M
  budget exceeded at k=9 (input length 20)
  about 100M steps at 25 characters of hostile input
```

From Racket you can call the same pieces: `analyze-pattern` takes a pattern and a flag string and returns a hash, `analyze-lines` handles a list of input lines, `render-text` formats a result and `exit-code-for` applies a `--fail-on` level. Run the tests with `raco test RegexBacktrackProver.rkt`.

## Notes

- The matcher is a reference model, not a copy of any production engine. Real engines have optimisations: V8 has a linear fallback, PCRE has a match limit, Java caches some failures and Ruby 3.2 and later memoises. A VULNERABLE verdict means a classic backtracking engine will suffer. It does not mean your specific runtime will.
- Lookarounds, backreferences and word boundaries are not part of the static model. They are executed correctly by the matcher, so growth still shows up in the measurement, but a SAFE verdict on a pattern that uses them carries a note saying so.
- Unicode property escapes like `\p{L}` are approximated as any character. That can only add ambiguity, never hide it, so the cost is an occasional SUSPECT.
- Multiline mode is ignored: `^` and `$` are the input boundaries.
- The polynomial search is skipped above 80 positions. Large patterns are still covered by the exponential check and the flood probes and the report tells you when the search was skipped.
- The degree of a polynomial is an estimate from measured slopes. When the step budget runs out it is printed as "at least" and the real degree can be higher. For an input length cap, trust the 100 million step figure more than the degree.
- The tool studies growth in the length of the input. A pattern with a huge fixed cost, whatever the input, is out of scope.
- The fix hints are generic on purpose. Rewriting a pattern safely needs a human who knows what the pattern is for. Making the competing parts disjoint, anchoring, using an atomic group or possessive quantifier where the engine supports them, capping input length or moving to a linear engine are the usual answers.
- The attack strings are for testing your own patterns. Do not point them at systems you do not own.
