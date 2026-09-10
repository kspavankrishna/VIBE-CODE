# Prompt Cache Cutover

You are paying full input token price on prompts that barely change between requests, and no dashboard tells you which routes are worth switching to prompt caching first. This is a single file OCaml tool that reads a CSV of route level token usage and prices, does the break even math with cache write cost included and tells you which routes to cut over.

**Language:** OCaml | **Lines:** 129 | **Added:** 2026-05-24

## What this solves

Prompt caching looks like free money until you run the numbers. A long system prompt, a retrieval pack, a big tool schema block: all of it is re-sent every request and billed at full input rate. Enable caching and the read price drops sharply, but you now pay a write price to put those tokens in the cache. On a route with thousands of requests a month the write is noise. On a route with forty requests a month the write eats the whole saving, and you have made the bill worse while adding a cache invalidation failure mode to the inference path.

The failure mode in production is quiet. Someone reads a provider blog post, flips caching on across every route in the gateway, and the monthly bill moves three percent instead of the thirty that was promised. The finance owner asks why. The platform team has a dashboard that aggregates token spend by model, not by route, so nobody can say which of the twenty routes actually paid off. Some routes got more fragile for zero benefit, because a cache write on a prompt that is never reused is pure overhead.

The reverse failure is worse. A route with a 40k token system prompt and heavy traffic sits uncached for six months because nobody did the arithmetic, and it is quietly the largest line item in the inference budget. It never surfaces because in the aggregate view it looks like every other route.

This tool makes the decision per route and per model, with real prices as input, and returns an enable or do not enable verdict plus the request count at which the cutover pays for itself. It exits non zero when it finds a route worth enabling, so CI can tell you when traffic growth has finally justified a cutover.

## Why I built it

Provider consoles report aggregate token spend. They do not model your routing table, they do not know that route A is a chat endpoint with a 2k prompt while route B is document QA with a 40k prompt, and they will not tell you the request count at which a cache write amortises. Every team I have seen make this call makes it in a spreadsheet that lives on one laptop, goes stale in a month and disagrees with the real price sheet.

Spreadsheets also cannot fail a build. Break even is not a one time analysis, it is a standing check that should re-run whenever traffic or prices move, so it belongs in a program with a defined exit code. OCaml because the money math should be typed, the parser should be total and the whole thing compiles to one binary you can drop into a CI image with no runtime and no dependency tree.

## When to use it

- Your gateway can export per route token counts and you want to know which three routes to cache first, in order of dollars saved.
- Someone asked why prompt caching only saved three percent and you need per route attribution instead of a provider aggregate.
- A low traffic internal route has caching enabled and you suspect the write cost is larger than the read saving.
- You want a CI gate that fails the moment a previously uneconomic route crosses its break even request count.
- You are comparing the same route across two models and want the savings and break even for each side by side.

## How it works

Input is CSV on stdin with eight columns: `route,model,prompt_tokens,cached_tokens,requests,input_price,cache_price,write_price`. `parse_row` requires exactly those eight fields and rejects anything else with a message listing the expected header. `read_all_rows` loops over stdin, skipping blank lines and any line beginning with `route,` so you can pipe a file that still carries its header. Splitting is `split_comma`, a hand written character scanner that accumulates into a `Buffer` and cuts on every comma. Numeric cells go through `float_of_cell`, which names the offending column instead of throwing a bare `Failure "float_of_string"`.

The cost model is four small functions. `monthly_input_cost` is the baseline: `prompt_tokens * requests * input_price / 1_000_000`. The cached scenario is the sum of three terms. `monthly_cached_read_cost` charges the cached portion at the cache read price for every request. `monthly_uncached_tail` charges the remainder of the prompt, clamped at zero with `max 0.0` so a row where `cached_tokens` exceeds `prompt_tokens` does not produce a negative credit, at the full input price. `cache_write_cost` charges the cached tokens once at the write price, not once per request. That single decision is the whole point of the tool: the write is an amortised fixed cost and everything else is per request.

`decide` turns those into a verdict. Savings is baseline minus cached. Per request savings is `cached_tokens * (input_price - cache_price) / 1_000_000`, the money you recover on each subsequent call. Break even requests is the write cost divided by that per request saving, and when the per request saving is zero or negative the break even is set to `infinity` rather than a misleading large number, which is the honest answer for a model whose cache read price is not actually cheaper than its input price. A second guard, `write_ratio`, is the write cost as a fraction of baseline spend. A row is enabled only when savings clear `min_savings` and the write ratio stays under `max_write_ratio`. When baseline is exactly zero the write ratio is forced to `1.0`, so a zero traffic row can never be recommended.

Options come from `parse_args`, a recursive pattern match over the argument list, with `default_options` of `min_savings = 1.0`, `max_write_ratio = 0.35` and text output. Results are sorted descending by savings with `compare b.savings a.savings`, so the biggest win is the first line. `render_text` prints a tab separated table with a header. `render_json` emits one `{"decisions":[...]}` object, passing route and model through `esc`, which escapes double quotes and backslashes. The entry point wraps all of it in a `try`: it exits `2` if any decision is enabled and `0` otherwise, and any exception, a bad number, a short row or an unknown flag, is printed to stderr prefixed with `PromptCacheCutover:` before exiting `64`.

## Usage

```bash
# compile to a single binary, no dependencies beyond the OCaml compiler
ocamlfind ocamlopt -package str PromptCacheCutover.ml -o prompt-cache-cutover 2>/dev/null \
  || ocamlopt PromptCacheCutover.ml -o prompt-cache-cutover

# CSV on stdin, header line optional
cat routes.csv | ./prompt-cache-cutover

# routes.csv
# route,model,prompt_tokens,cached_tokens,requests,input_price,cache_price,write_price
# /v1/docqa,claude-opus,42000,38000,18000,15.00,1.50,18.75
# /v1/chat,claude-sonnet,2100,1200,240000,3.00,0.30,3.75
# /internal/eval,claude-sonnet,31000,29000,40,3.00,0.30,3.75

# only recommend routes saving at least 250 dollars a month
./prompt-cache-cutover --min-savings 250 < routes.csv

# tighten the write cost guard to 10 percent of baseline spend
./prompt-cache-cutover --max-write-ratio 0.10 < routes.csv

# machine readable, for a CI step or a dashboard
./prompt-cache-cutover --json < routes.csv

# run it as a gate: exit 2 means at least one route is worth enabling
./prompt-cache-cutover --min-savings 100 < routes.csv || echo "cutover candidates found"
```

## Notes

- Exit codes are inverted from the usual convention. `0` means nothing is worth enabling, `2` means at least one route is a cutover candidate, `64` means bad input or bad flags. A green CI run means no action needed.
- The CSV splitter has no quoting support. A comma inside a route name or model name will split the field and produce a row count error. Keep route names comma free.
- A flag given without its value falls through to the catch all branch and reports `unknown option --min-savings`. The message is not precise about the real cause.
- Cache write cost is charged exactly once per row, which models a cache that stays warm for the whole period you are measuring. There is no TTL model, no eviction, no re-write on prompt drift. If your prompt changes daily you should inflate `write_price` or split the row.
- Provider minimum cacheable token thresholds are not modelled. A row with 400 cached tokens still produces a savings number even if the provider would refuse to cache it.
- The JSON output carries `enable`, `savings_usd`, `break_even_requests`, `route` and `model` only. Baseline and cached cost appear in the text output but not in the JSON.
- `String.starts_with` requires OCaml 4.13 or later. Nothing else outside the standard library is used.
