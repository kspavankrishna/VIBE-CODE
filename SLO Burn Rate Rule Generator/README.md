# SLO Burn Rate Rule Generator

A single file Tcl script that turns a short SLO spec into Prometheus recording rules and multi window, multi burn rate alerts. It also refuses to write rules that could never fire, which is the mistake most hand written burn rate alerts ship with.

**Language:** Tcl | **Lines:** 676 | **Added:** 2026-10-03

## What this solves

Burn rate alerting is the right way to page on an SLO. The Google SRE workbook describes it well: alert when the error budget is being spent too fast, check a long window and a short window together, and use several tiers so a fast burn pages someone while a slow burn opens a ticket. The idea is simple. Writing the rules by hand is not.

For one SLO you need around eight recording rules and four alerts. Every threshold is a product of three numbers: the budget fraction you are willing to lose, the SLO period and the long window. Change the objective from 99.9 to 99.5 and every threshold changes. People copy the numbers 14.4 and 6 from a blog post and paste them into rules for a 99% service or a 7 day period, where the numbers mean something else entirely.

The failure that hurts most is silent. If you take the 14.4x tier and apply it to a 90% objective, the alert threshold works out to an error ratio of 1.44. An error ratio can never go above 1. The rule loads fine, the dashboard is green and the alert will never fire, ever. Nobody finds out until the outage.

This script computes the thresholds from the spec and stops with an error when a tier is mathematically unreachable. It also catches the smaller things that bite later: a query with no window placeholder so every window reads the same data, an unclosed bracket that Prometheus only rejects after deploy, an objective written as 0.999 instead of 99.9, a typo in a key name, a short window so small it flaps on a 30 second scrape interval.

## Why I built it

I keep seeing the same three setups. Someone uses Sloth or Pyrra, which are good, but they want a Go binary, a CRD or a service in the loop. Someone else writes the rules in Jsonnet and nobody dares touch the file. Or the rules are copied between teams with the numbers unchanged. I wanted something I could drop into a repo, run in CI with nothing installed except a Tcl 8.6 interpreter, which is one package away on any distro, and read top to bottom in ten minutes.

Tcl was a deliberate choice. The spec is a plain Tcl dict, so the parser is the language itself. The script reads the spec file as data and never evaluates it, so a hostile or broken spec cannot run code. A brace imbalance shows up as a normal error message. There is no YAML parser to get wrong and no dependency to pin.

The output is deterministic. Same spec in, same bytes out, no timestamps. That means you can commit the generated rule file and a CI job can regenerate it and fail on any diff.

## When to use it

Use it when you run Prometheus, Thanos, Mimir, VictoriaMetrics or anything that reads Prometheus rule files, and you want SLO alerts you can explain to the next on call engineer.

Use it when you have several SLOs with different objectives and periods and want the thresholds to stay correct when someone edits the spec.

Use it as a CI check. The `--check` flag validates the spec and writes nothing, and `--strict` turns warnings into a failing exit code.

Do not use it if you need an SLO status page, error budget dashboards beyond one gauge, or a Kubernetes operator. This is a rule generator. It does one job.

## How it works

The spec is a bare Tcl dict with a `slos` key and an optional global `tiers` key. Each SLO needs `objective` as a percentage, `period`, and two PromQL queries named `errors` and `total`. Each query must contain the `%WINDOW%` token, held in the `windowToken` variable. The generator replaces it with each window it needs.

`loadSpec` reads the file in UTF-8 and checks it with `dict size`, which treats the text as a list and never evaluates it. It rejects unknown top level keys. Then `resolveSlo` validates each SLO. The allowed keys live in `allowedSloKeys`, so a misspelled `objectve` is an error, not a silently ignored line.

`resolveSlo` rejects an objective of 1 or less with a hint that you probably wrote a ratio. It rejects 100 or more because a 100% SLO has no error budget. It warns below 90. The budget is `(100 - objective) / 100`. Each query goes through `collapse` to normalise whitespace and through `checkBalanced`, a small scanner that tracks quotes with escapes and a stack of brackets so it reports the exact offset of a mismatch.

Tiers come from three places in order: the SLO's own `tiers`, the global `tiers` or `defaultTiers`. The defaults are the workbook set: 2% of budget in 1h as `page-fast`, 5% in 6h as `page-slow`, 10% in 1d as `ticket-fast` and 10% in 3d as `ticket-slow`.

`resolveTiers` does the math. For each tier it computes the burn rate as `fraction * period / long` and the threshold as `burn * budget`. For a 99.9% objective over 30 days that gives 14.4, 6, 3 and 1 as burn rates and thresholds of 0.0144, 0.006, 0.003 and 0.001. The short window defaults to one twelfth of the long window, so 1h pairs with 5m and 1d pairs with 2h, unless the tier sets `short` itself.

Then it applies the checks that matter:

1. A threshold of 1.0 or more is an error. The message states the maximum possible burn rate for the objective, which is `1 / budget`, so you can see how far off the tier is.
2. A long window longer than the SLO period is an error.
3. A short window that is not smaller than the long window is an error.
4. A short window under `minShortSeconds` (120) is a warning.
5. A burn rate under 1 is a warning, because that alert fires on a pace that would never use up the budget inside the period. This is what you get when you apply 3 day tiers to a 7 day SLO.
6. Duplicate window and fraction pairs are a warning.

Windows are deduplicated and sorted, so the long window of one tier can be the short window of another and is only recorded once. A warning appears if one SLO needs more than `maxRecordedWindows` distinct windows.

`renderRules` writes the Prometheus file. For each SLO there is a recording group with one rule per window named `slo:sli_error:ratio_rate<window>`, built as `(errors) / (total)` with the window substituted. Durations are written by `formatDuration`, which skips weeks so 30d stays 30d. An alert group follows with one `ErrorBudgetBurn` rule per tier. Each alert compares the long and short window ratios to the same numeric threshold with `and`, carries labels `slo`, `severity`, `tier` and `burn_rate` plus your own labels, and has a summary and description that state the objective, the burn multiple and how long until the budget is gone. Label names are checked against `reservedLabels` so you cannot overwrite the ones Alertmanager routes on. Every string goes through `yamlQuote`, so colons, braces and quotes in your queries are safe.

Two optional fields are worth knowing. `min_rate` adds a guard so the alert also needs the total request rate over the long window to exceed a floor. Without it one failed request on a quiet service at 3am can look like a 100% error ratio and page somebody. `budget_record` adds a `slo:error_budget_remaining:ratio` rule over the full period, and warns when that window is longer than Prometheus default retention of 15 days.

`renderReport` prints a table per SLO: burn rate, threshold, windows, severity, how long a total outage takes to fire the alert and how long the budget lasts at that burn. The outage time is `threshold * long window`, because a 100% error ratio fills the long window at that rate. For the defaults at 99.9% a full outage fires `page-fast` in about 52 seconds. That table is the quickest way to sanity check a tier before you ship it.

`main` handles the command line and exit codes: 0 for success, 1 for spec errors, 2 for warnings under `--strict` and 64 for bad usage. `writeAtomic` writes to a temp file and renames it, so a crash never leaves a half written rule file where Prometheus will read it. All errors are collected and printed together, so you fix the spec once, not one error per run.

## Usage

Save the spec as `ExampleCheckoutSlo.tcl` (included in this folder). The file is a bare dict with no outer braces:

```tcl
slos {
  checkout-availability {
    objective 99.9
    period 30d
    errors {sum(rate(http_requests_total{job="checkout",code=~"5.."}[%WINDOW%]))}
    total  {sum(rate(http_requests_total{job="checkout"}[%WINDOW%]))}
    labels {team payments}
    min_rate 0.05
    budget_record yes
    runbook https://runbooks.example.com/checkout
  }
}
```

Run it:

```sh
# Write Prometheus rules
tclsh SloBurnRateRuleGenerator.tcl ExampleCheckoutSlo.tcl --out slo-rules.yml

# Print the thresholds and detection times
tclsh SloBurnRateRuleGenerator.tcl ExampleCheckoutSlo.tcl --format report

# CI: validate only, fail on warnings too
tclsh SloBurnRateRuleGenerator.tcl ExampleCheckoutSlo.tcl --check --strict
```

Custom tiers go under `tiers` at the top level or inside one SLO. Each tier needs `fraction`, `long` and `severity`, and takes optional `short` and `for`:

```tcl
tiers {
  fast { fraction 0.02 long 1h severity page for 2m }
  slow { fraction 0.10 long 3d severity ticket for 3h }
}
slos {
  api-latency { ... }
}
```

Check the output with `promtool check rules slo-rules.yml` before you load it.

## Notes

The script needs Tcl 8.6, which Debian, Ubuntu and Alpine package as tcl. macOS ships an older tclsh, so install 8.6 with Homebrew there. There are no Tcl packages to install.

Error ratios are `errors / total`. When `total` is zero the division gives NaN and the alert does not fire. That is the right behaviour for a service with no traffic, but remember it if you expect a dead service to page. Pair burn rate alerts with an absent traffic alert.

The `for` clause on each tier is an extra delay on top of the two windows. The defaults are small. Raise them if your pages are noisy.

If your queries aggregate with `by (label)`, the recorded series keep those labels and the long and short windows are joined on all of them, so each group alerts on its own. That is usually what you want, but check the report and the output before you rely on it.

The report numbers assume a constant error ratio. Real incidents ramp, so treat detection times as the best case for a total outage and not a promise.

The spec is only ever read as data. Even so, review generated rule files in pull requests like any other production config.
