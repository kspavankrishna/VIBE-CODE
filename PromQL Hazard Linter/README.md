# PromQL Hazard Linter

A PromQL linter written in Raku that parses your Prometheus alerting rules and recording rules, type checks every expression and reports the ones that are valid but will return nothing, flap or tell you the wrong number. It reads plain rule files, PrometheusRule manifests and lists of queries. It needs no running Prometheus.

**Language:** Raku | **Lines:** 1472 | **Added:** 2026-11-14

## What this solves

The worst Prometheus alert is the one that parses, loads, shows green in the rules page and can never fire. `promtool check rules` tells you the syntax is fine. It does not tell you that the query is empty by construction. These are the ones I keep finding in real rule repos:

- `histogram_quantile(0.99, sum by (job) (rate(http_request_duration_seconds_bucket[5m])))`. The `le` label is gone before the quantile is taken, so the result is NaN or nothing. The latency alert built on it is dead.
- `count(up{job="api"} == 1) == 0`. When no pod is up, `count` returns no series at all, not zero. The "no pods" alert is silent exactly when there are no pods.
- `sum by (job) (rate(errors_total[5m])) / sum(rate(requests_total[5m]))`. One side has a `job` label and the other has none. PromQL pairs series only when the labels are equal, so the ratio is always empty.
- `rate(http_requests_total[1m])` with a one minute scrape interval. `rate` needs two samples in the window and usually finds one.
- `rate(sum(http_requests_total)[5m:])`. Rate after sum. One pod restarts, the sum dips, `rate` reads the dip as a counter reset of the whole total and you get a spike that never happened.
- `sum(errors_total) > 100`. A raw counter against a threshold. It fires some time after deploy and stays on until the process restarts.
- `increase(restarts_total[10m]) == 1`. `increase` extrapolates, so the value is 1.03 or 0.97 and almost never 1.
- An alert that groups by `job` and then writes `{{ $labels.instance }}` in its summary. The page arrives with an empty string where the host should be.
- `expr: >` with a `#` comment on the first line. YAML folds the lines into one, so the comment swallows the rest of the query.

None of these are syntax errors. All of them have cost somebody an incident. PromQL Hazard Linter finds them from the files on disk, before the rule ships.

## Why I built it

I wanted a CI check for alert rules that answers one question: can this expression ever do what its author thinks it does. Syntax checkers stop at parsing. The heavier linters want a live Prometheus to query, which is fine for a nightly job and awkward in a pull request from a fork.

Most of the classic PromQL mistakes can be proven from the text alone if you track two things through the expression tree: the type of each node and the set of labels that survive it. `sum by (job)` leaves exactly `job`. `sum without (pod)` leaves an unknown set that certainly does not contain `pod`. `histogram_quantile` removes `le`. `on (node)` leaves exactly `node`. Once you carry that information upward, label mismatches, lost `le` labels and dead template variables fall out as plain set comparisons, with no guessing about what data exists.

I wrote it in Raku because the language is good at this kind of text work and the result is one file with no modules to install. The parser is hand written instead of using a Raku grammar on purpose: a grammar that fails just says no. A linter has to say where and why.

## When to use it

- As a pull request check on a repo of Prometheus rule files or PrometheusRule manifests for the Prometheus Operator.
- Before you import rules from a mixin or copy an alert from a blog post into production.
- After an incident where an alert stayed quiet, to see whether it ever could have fired.
- When you change the scrape interval and want to know which `rate` windows are now too short.
- On a file of dashboard queries, one per line, to catch the same mistakes in panels.

Do not use it as a replacement for `promtool check rules` or `promtool test rules`. It does not validate the YAML schema and it does not evaluate anything against data. It sits between the two.

## How it works

**Tokenizer.** `tokenize` turns an expression into `Token` objects with a kind, the text, the start and end offsets and a decoded value. It knows durations such as `1h30m` (converted by `dur-seconds` using the `%UNIT` table), hex and float numbers, the three string quote styles and `#` comments. It tracks bracket depth so that the colon in `[1h:5m]` is a separator while the colon in `job:requests:rate5m` stays part of the name. A `$` gets a parse error that says it is a dashboard variable and `{{` gets one that says it is a template placeholder, because those are the two reasons a rule file fails to parse in practice.

**Parser.** The `Parser` class is a precedence climbing parser driven by the `%PREC` table, with the same binding rules as Prometheus: `^` is right associative and binds tighter than unary minus, then `* / % atan2`, then `+ -`, then comparisons, then `and` and `unless`, then `or`. It builds `Selector`, `Subquery`, `Call`, `Agg`, `Binary`, `Unary`, `Paren`, `NumLit` and `StrLit` nodes, each with its source span. It handles `by` and `without` before or after an aggregation, `bool`, `on`, `ignoring`, `group_left`, `group_right`, `offset` with negative values, the `@` modifier with `start()` and `end()`, subqueries and quoted metric names inside braces. Every failure throws `X::PromQL` with the offset of the token that broke.

**Types.** `type-of` gives each node one of four types: scalar, instant vector, range vector or string. The `%FUNCS` table holds the return type, the argument types and the number of optional arguments for every function. `%PARAM-AGGS` does the same for `topk`, `quantile`, `count_values` and friends. The `Analyzer` reports a `TYPE_ERROR` for the things Prometheus itself would reject at load time: `rate(metric)` without a range, a set operator against a scalar, a scalar comparison without `bool`, a wrong argument count.

**Label tracking.** `labels-of` returns a `LabelInfo` for every vector node. It has an `exact` set when the output labels are known for certain and an `absent` set of labels that are certainly gone. A bare selector is unknown. `sum by (a, b)` is exactly `a` and `b`. `without`, `ignoring` and `histogram_quantile` add to the absent set. `topk` and the other operators in `%KEEPS-SERIES` pass the inner labels through, `label_replace` adds its destination label, `group_left (team)` adds `team`, `absent(up{job="api"})` yields exactly `job`. The method `lacks` answers the only question the checks ask: is this label provably missing here.

**Checks.** Each one has a code and a severity from `@SEVERITIES` (INFO, LOW, MEDIUM, HIGH, ERROR).

- `check-range-arg` handles windows. `RANGE_TOO_SHORT` when a function from `%NEEDS-TWO-SAMPLES` gets less than two scrape intervals, `RANGE_FRAGILE` under four, `RANGE_SHORTER_THAN_EVAL` when a rule looks back less than its evaluation interval. It also raises `RATE_OF_AGGREGATE` and `RATE_OF_GAUGE` for counter functions over a subquery, `COUNTER_FUNCTION_ON_GAUGE` when `looks-like-gauge` matches the name against `@GAUGE-SUFFIXES`. It raises `GAUGE_FUNCTION_ON_COUNTER` when `looks-like-counter` sees `_total` or `_bucket` under a function from `%GAUGE-ONLY-FUNCS`.
- `check-histogram` raises `HISTOGRAM_MISSING_LE` and `HISTOGRAM_WITHOUT_RATE`. `check-phi` raises `QUANTILE_OUT_OF_RANGE` for `histogram_quantile(99, ...)`.
- `check-matching` raises `LABEL_SET_MISMATCH`, `ON_LABEL_MISSING` and `MANY_TO_ONE_NEEDS_GROUP` from the two `LabelInfo` values of a binary operator.
- `check-threshold` raises `COUNT_NEVER_ZERO`, `RAW_COUNTER_THRESHOLD`, `FRACTIONAL_EQUALITY` and `NAN_COMPARISON`. It uses `const-value` to fold literals and `%FLIP` so that `1 > count(x)` is treated the same as `count(x) < 1`.
- The `Selector` visitor raises `SELECTOR_MATCHES_EMPTY`, `SELECTOR_NEVER_MATCHES`, `REGEX_WITHOUT_PATTERN`, `REGEX_MATCH_ALL`, `REGEX_REDUNDANT_ANCHOR` and `REGEX_EMPTY_ALTERNATIVE`. The regex checks exist because PromQL anchors every regex at both ends, so `job=~"api"` does not match `api-gateway`.
- The `Subquery` visitor raises `SUBQUERY_STEP_EXCEEDS_RANGE` and `SUBQUERY_UNDERSAMPLES`. The `Agg` visitor raises `AGGREGATE_OF_QUANTILES`. `DEPRECATED_FUNCTION` comes from the `%DEPRECATED` table.
- `lint-rule` adds the checks that need the whole rule: `IRATE_IN_RULE`, `BOOL_IN_ALERT`, `ALERT_WITHOUT_FOR`, `FOLDED_COMMENT` and `TEMPLATE_LABEL_MISSING`. The last one uses `template-labels` to pull every `$labels.name`, `.Labels.name` and `index $labels "name"` out of the labels and annotations and asks `lacks` about each. Labels set in the rule's own `labels:` block are exempt.

**Reading files.** `read-source` decides between two readers. `read-rule-file` is a line based reader that finds rule items by their keys from `%RULE-KEYS`, at any indentation, so the same code reads a plain rule file and a PrometheusRule manifest nested under `spec`. `decode-scalar` handles plain values, single and double quoted strings and block scalars with `|` and `>`. It records where the expression starts so findings in a multi line expression point at the real file line. The group `interval` overrides `--eval-interval` for the rules in that group. `read-expression-list` treats every non comment line as one query.

**Suppression.** `note-directives` reads comments of the form `# promql-lint: ignore CODE, CODE` placed directly above a rule or inside it. `*` ignores everything for that rule.

**Output.** `render-text` prints file, line, severity, code, the explanation and the offending part of the expression from `snippet`. `render-github` prints workflow commands so findings show up as annotations on the pull request diff. `json` is a small built in encoder for the machine readable form.

## Usage

You need Rakudo. On Debian or Ubuntu that is `apt-get install rakudo`.

```
raku PromQLHazardLinter.raku rules/*.yml
raku PromQLHazardLinter.raku --scrape-interval=15s --eval-interval=30s rules/api.yml
raku PromQLHazardLinter.raku --format=github --fail-on=MEDIUM k8s/prometheus-rules.yaml
raku PromQLHazardLinter.raku --format=json --min-severity=INFO queries.txt
raku PromQLHazardLinter.raku --expr='histogram_quantile(0.99, sum(rate(latency_bucket[5m])))'
cat queries.txt | raku PromQLHazardLinter.raku -
raku PromQLHazardLinter.raku --disable=ALERT_WITHOUT_FOR,REGEX_REDUNDANT_ANCHOR rules.yml
raku PromQLHazardLinter.raku --self-test
raku PromQLHazardLinter.raku --version
```

Options:

| Option | Default | Meaning |
|---|---|---|
| `--scrape-interval` | `1m` | Scrape interval used for the window checks. Set it to your real one. |
| `--eval-interval` | `1m` | Rule evaluation interval when a group has no `interval`. Also the default subquery step. |
| `--format` | `text` | `text`, `json` or `github`. |
| `--fail-on` | `HIGH` | Lowest severity that makes the exit code 1. `NEVER` always exits 0. |
| `--min-severity` | `LOW` | Lowest severity that is printed. |
| `--disable` | empty | Comma separated codes to switch off everywhere. |
| `--expr` | none | Lint one expression given on the command line. |

Example output:

```
rules/api.yml:14 [HIGH] HISTOGRAM_MISSING_LE (alert ApiLatencyHigh)
    The le label is aggregated away before histogram_quantile sees the buckets. Without le it cannot tell the buckets apart and returns NaN or nothing. Keep le in the grouping, for example sum by (le, job).
    > histogram_quantile(0.99, sum by (job) (rate(http_request_duration_seconds_bucket[5m])) )

rules/api.yml:23 [HIGH] COUNT_NEVER_ZERO (alert NoApiPods)
    count over nothing returns no series at all, not 0, so this condition can never be true. Use absent(...) or append: or vector(0).
    > count(up{job="api"} == 1) == 0

Checked 7 expressions in 1 file: 2 findings (2 HIGH)
```

Suppressing a finding for one rule:

```yaml
# promql-lint: ignore RAW_COUNTER_THRESHOLD
- alert: TooManyRestarts
  expr: kube_pod_container_status_restarts_total > 3
  for: 1m
```

Exit codes: 0 when nothing reaches `--fail-on`, 1 when something does, 2 for a bad option or an unreadable file.

## Notes

- The default scrape interval is one minute because that is the Prometheus default. If you scrape every 15 seconds and do not pass `--scrape-interval=15s`, the window checks will be too strict.
- Label tracking is deliberately conservative. A bare selector has an unknown label set, so `sum(rate(a[5m])) / b` is not flagged even though it is usually empty. The tool only reports a mismatch when both sides are known for certain. That keeps false positives low at the cost of missing some real ones.
- `MANY_TO_ONE_NEEDS_GROUP` is MEDIUM, not HIGH, because the query does work while each group has exactly one series. It breaks the day a second one appears.
- Counter and gauge detection goes by name. `_total` and `_bucket` are treated as counters, the suffixes in `@GAUGE-SUFFIXES` and the metric `up` as gauges. Recording rule names that contain a colon are skipped. Old metrics that do not follow the naming rules will not be classified.
- Native histograms have no `_bucket` series and no `le` label, so the histogram checks only run when a `_bucket` selector is present.
- The regex checks are heuristics on the pattern text. The tool does not compile RE2.
- The YAML reader is not a YAML parser. It understands the shape Prometheus rule files have: rule items in a list, scalar values in plain, quoted, literal or folded form and one level of `labels` and `annotations`. Anchors, aliases and flow style rule lists are not supported. Helm templates must be rendered first. A `{{` inside an expression is reported as a parse error that says so.
- Grafana variables such as `$__rate_interval` are reported as parse errors. Substitute them before linting dashboard queries.
- Keywords used as metric names (a metric literally called `on` or `by`) are not supported.
- Rakudo compiles the script on every run, which takes a few seconds before any linting starts. The linting itself is fast. For a large repo pass all files in one invocation instead of one process per file.
- `--self-test` runs the built in cases from `@SELF-TESTS` and a small rule file through the same code path as a normal run. Run it after editing the tables.
- Tested with Rakudo 2022.12. It uses nothing outside the core language.
