# Structured Output Drift Gate

The same prompt, run twice against the same model, returns JSON that is subtly different: a key vanishes, a number becomes a string, an array grows by one element. This is a single file Java CLI that reads your eval logs and tells you exactly which JSON pointers are unstable, then fails the build.

**Language:** Java | **Lines:** 1914 | **Added:** 2026-05-20

## What this solves

This solves structured output drift in AI systems, agent pipelines, eval harnesses and MCP tool workflows, where the same scenario quietly starts returning different JSON after a model upgrade, a prompt change, a router tweak or a provider failover. The painful production breakages are usually not loud crashes. They are missing keys, type flips, unstable arrays, numeric spread and response shapes that still look valid until one downstream parser or scorer starts failing.

Picture the concrete version. Your extraction service asks a model for `{"invoice_total": 1240.50, "line_items": [...], "currency": "INR"}`. On the old model `invoice_total` was always a number. On the new one it comes back as `"1240.50"` about one run in twenty. The schema validator misses it because you typed the field loosely, or it catches it and you now have a 5 percent hard failure rate in a batch job that runs at 2am. Nobody notices for three days. Then finance notices, because the reconciliation is off.

The other shape of this is the array that changes length. A model that returned four line items every time now returns four mostly and three occasionally, and the item that goes missing is not always the same one. Every response passes schema validation. Every one is plausible. The failure is visible only across repeated runs of the same input, which is exactly the view no logging stack gives you by default. The cost lands downstream: parsers throw on type flips, scorers compare against the wrong field and comparisons between two model releases turn into arguments, because "the eval score dropped two points" says nothing about whether the model got worse or just got less consistent about its output shape. This tool answers the second question: which field, which type, how far apart, in which cohort.

## Why I built it

JSON schema validation checks one response at a time, which is the wrong unit of measurement for non determinism. A schema will happily accept a hundred responses that disagree with each other, as long as each one is individually well formed. Snapshot testing goes the other way and is too strict: it fails on any diff at all, so timestamps, request ids and trace ids drown you in noise until you turn it off. Eval frameworks give you an aggregate score, not a per field stability breakdown.

I wanted the thing in between: no dependencies, runs on whatever NDJSON the harness already writes and returns a real exit code so CI can gate on it. No pip install, no service, no config file. One Java file, `javac`, done.

## When to use it

- Deciding whether to promote a new model version, and you want proof it is not less consistent than the one in production.
- A prompt rewrite improved the eval score and you want to know whether it also made the output shape flakier.
- You run each eval case N times and need to know which fields are genuinely deterministic before writing a strict parser against them.
- A downstream service started throwing type errors intermittently and you need to find which field flips and how often.
- Comparing two providers or two reasoning effort settings on the same scenario set, and you want a per cohort stability report.
- You want a CI job that fails the branch when a field that used to be stable stops being stable.

## How it works

Input is NDJSON, one JSON object per line, from a file or stdin. `readRecords` parses each line with a hand written parser (`JsonParser`, `Json.parse`) producing `LinkedHashMap`, `ArrayList`, `String`, `Boolean`, `BigDecimal` and null. Numbers become `BigDecimal` deliberately, so `1.0` and `1.00` compare equal after `normalizeNumber` strips trailing zeros and no float rounding creeps into the comparison. Keys are resolved by `resolve`, which takes either a plain top level field name or an RFC 6901 JSON pointer like `/meta/case_id`, with `~0` and `~1` token escaping. If the output field is a string that starts and ends with braces or brackets, `coerceOutput` parses it, so logs that store the model reply as an escaped JSON string work without preprocessing.

Records are grouped by `GroupKey`, the cohort plus scenario pair. Groups with fewer than `--min-runs` records are counted as skipped, because you cannot measure stability from one sample. For each group, `analyzeGroup` walks every run's output through `flatten`, a recursive descent emitting one entry per node keyed by its JSON pointer. Container nodes are recorded too, not just leaves, which is what catches a field that changes from object to array. The root is pointer `/`. Array elements get positional pointers like `/line_items/0/sku`, so this is order sensitive by design.

Each pointer feeds a `PathAccumulator`, the core of the algorithm. It counts how many runs contained that pointer, tallies the JSON type per run in an `EnumMap<JsonType, Integer>`, keeps canonical scalar strings with counts, tracks array lengths with counts and holds running min and max `BigDecimal` for numbers. `finish` derives issues in a fixed precedence. Present count below run count is `presence-drift`. More than one type is `type-drift`, and evaluation of that pointer stops there, since comparing values across types is meaningless. Otherwise arrays with more than one distinct length give `array-length-drift`, numbers give `numeric-drift` and other scalars give `value-drift`.

Numeric drift is the one with tolerances. It fires only when the absolute spread exceeds `--numeric-abs-tolerance` and the relative spread exceeds `--numeric-rel-tolerance`. `relativeSpread` divides the absolute difference by the larger of the two magnitudes, returning positive infinity when min and max are both zero magnitude but differ. Both defaults are zero, so out of the box any difference is reported. Raise them when a confidence score is allowed to wobble.

Issues carry a severity from the `IssueKind` enum: type-drift 5, presence-drift 4, numeric-drift and array-length-drift 3, value-drift 2. That ordering drives the sorts. Groups sort unstable first, then by issue count, then by run count. Issues aggregate across groups into `AggregatedIssue` keyed by cohort plus pointer plus kind, so "Top Unstable Pointers" tells you one pointer is broken in 40 scenarios instead of printing 40 lines. `analyzeGroup` also canonicalizes each whole output with `Json.toCanonicalJson`, which sorts object keys, and counts distinct root variants: a one number answer to how many different responses a scenario actually produced. `NumericStats` tracks mean, min and max for `latency_ms`, `input_tokens`, `output_tokens` and `reasoning_tokens` per group and per cohort when your records carry them.

Output goes to two channels. The human summary goes to stderr so it does not pollute a piped report. The full machine readable report goes to `--json-output`, or to stdout when that path is `-`. `--ignore-pointer` takes globs, converted to anchored regex by `globToPattern` where `*` becomes `.*`, and matching pointers are dropped before accumulation. That is how you silence `/request_id` and `/items/*/generated_at`.

## Usage

```bash
javac StructuredOutputDriftGate.java

# minimal: NDJSON on stdin, fields named "scenario" and "output"
cat eval.ndjson | java StructuredOutputDriftGate

# real gate in CI
java StructuredOutputDriftGate \
  --input eval.ndjson \
  --scenario-key /meta/case_id \
  --cohort-key model_release \
  --output-key output \
  --min-runs 5 \
  --numeric-abs-tolerance 0.01 \
  --numeric-rel-tolerance 0.02 \
  --ignore-pointer '/request_id' \
  --ignore-pointer '/items/*/generated_at' \
  --json-output drift-report.json \
  --summary-limit 20 \
  --fail-on-unstable

# report to stdout, never fail the build
java StructuredOutputDriftGate --input eval.ndjson --json-output - --no-fail-on-unstable

java StructuredOutputDriftGate --help
```

An input line looks like this:

```json
{"scenario":"invoice-042","model_release":"v3","latency_ms":812,"output":{"invoice_total":1240.5,"currency":"INR"}}
```

## Notes

- Exit codes: 0 clean, 1 unstable groups with `--fail-on-unstable` (on by default) or an unexpected internal error, 2 bad CLI usage, 3 bad input such as a malformed line or a missing scenario or output key.
- One unparseable line aborts the whole run with exit 3. There is no skip and continue mode.
- All records are held in memory and the report is built before anything is written. Fine for eval sets, not for streaming millions of lines.
- Tolerances are global. There is no per pointer tolerance, so loosening one confidence score loosens every number in the file.
- Array comparison is positional. The same set of items in a different order is reported as drift.
- `--min-runs` must be at least 2 and defaults to 2. Groups below it are counted as skipped, so check the `skipped` count before trusting a clean result.
- Metrics fields (`latency_ms`, `input_tokens`, `output_tokens`, `reasoning_tokens`) are read only from the top level of each record, by those exact names. Optional, reported, never gated on.
- Java 17 or later, since it uses pattern matching for `instanceof`. No external dependencies, no build tool.
