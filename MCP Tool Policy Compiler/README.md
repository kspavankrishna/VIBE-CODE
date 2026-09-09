# MCP Tool Policy Compiler

Agents are calling MCP tools faster than anyone can review them, and there is no single place that says which tool call was allowed and why. This is a single file Lua policy engine that reads a plain text rule file, replays a JSONL stream of tool calls through it and emits a deterministic allow or deny decision for every one.

**Language:** Lua | **Lines:** 938 | **Added:** 2026-07-17

## What this solves

The failure mode is not one dramatic breach. It is drift. A team wires up an MCP server for GitHub, another for a database, a shell helper for build tasks, a browser worker for scraping. Each one gets approved in a Slack thread. Six weeks later nobody can answer a simple question: can the CI agent write to production repos, and if it can, who decided that? The permission surface lives in five different config files, three of which are YAML dialects nobody else uses, and the actual behaviour lives in whatever the model decided to call that hour.

What breaks in production is usually cheap to describe and expensive to fix. An agent reads a path matching `**/.env` and the contents land in a log sink that ships to a third party. A retry loop calls a paid tool six hundred times because nothing capped spend per call. A shell tool receives a command containing `rm -rf` because the prompt was poisoned upstream and no layer between the model and the process checked the argument. Nobody notices until the invoice or the incident review. The security team asks for the audit trail and gets a pile of agent traces with no verdicts in them.

The other cost is review throughput. When policy is scattered across code, a change means a pull request, a deploy and a regression risk. When it is a text file of `effect|subject|tool|resource|constraints|budget_ms|reason` lines, a human reads it in two minutes and a diff shows exactly what changed. This file exists to make that trade concrete: a boring policy language, one matcher, three output formats, no service to operate. Malformed input is a decision too, not a crash. A JSONL line that fails to parse produces a deny with the parse error as the reason, so a corrupt log line can never quietly become an allow.

## Why I built it

Everything in this space is either too big or too vague. Full authorization services (OPA, Cedar and friends) are real systems with real operational cost, and pulling one in to gate a handful of MCP tools is out of proportion. The alternative most teams reach for is an ad hoc allowlist in the agent runner, which is code, which means it is untested, undocumented and different in CI than it is in production.

I wanted something that fits next to an OpenResty or Kong edge, a CI job or an agent runner, makes the same decision every time and can be read end to end by the person who has to sign off on it. Lua because it is already embedded where the traffic is, and because a dependency free script with its own JSON parser will run anywhere Lua runs.

## When to use it

- Gating MCP tool calls at an AI gateway before they reach an expensive or destructive backend.
- Auditing a day of agent tool call logs after the fact to find calls that should never have been permitted.
- Blocking shell or filesystem tools whose arguments touch secrets, with rules like `args.path~**/.env`.
- Enforcing a per call spend cap so a retry storm hits a `cost_usd<=0.25` rule instead of the billing page.
- Compiling a reviewed text policy into a Lua table that an OpenResty worker loads at startup.
- Running the policy in CI as a review gate on a captured JSONL trace so rule changes get tested like code.

## How it works

The file is self contained. It ships its own JSON parser (`JsonParser` with `parse_string`, `parse_number`, `parse_array`, `parse_object`) including UTF-16 surrogate pair decoding through `utf8_from_codepoint`, rejection of raw control characters inside strings and a trailing data check in `json_decode`. Encoding goes back out through `json_encode`, which sorts object keys with `sorted_keys` so identical decisions serialize byte identical, formats numbers with `%.17g` and refuses NaN and infinities. Determinism is the point: two runs over the same inputs produce the same bytes.

A policy line is seven pipe separated fields, parsed by `parse_policy_line`: effect, subject, tool, resource, constraints, budget_ms and reason. Splitting is done by `split_escaped`, which honours a backslash escape so a pipe or semicolon can appear inside a value. Empty subject, tool or resource fields default to `*`. Blank lines, `#` comments and a `effect|...` header row are skipped by `load_policy`, which dies with an error if the file yields zero rules.

Matching on the three positional fields uses a glob translated to a Lua pattern by `glob_to_pattern`: `**` becomes `.*`, a single `*` becomes `[^/]*` and `?` becomes `[^/]`, with every other character escaped by `escape_lua_pattern`. That path aware distinction is what makes `repo:kspavankrishna/**` behave the way you expect. Events are read loosely: `normalized_field` accepts `subject`, `actor`, `principal`, `agent`, `agent_id` or `user` for the subject, `tool`, `tool_name`, `name`, `method` or `rpc` for the tool, and `resource`, `target`, `repository`, `url`, `path` or nested `args.path` for the resource, falling back to `unknown` or `*`. So a trace from one runner and a trace from another both work without a translation layer.

Constraints are semicolon separated expressions over dotted JSON paths, resolved by `path_get`. `parse_constraint` scans the `OPERATORS` list `!~ >= <= != ~ = > <` and supports glob match, glob non match and the four ordering comparisons. Comparison in `compare_values` is three tiered: if both sides parse as numbers, compare numerically; otherwise if both sides are known severity words, compare by the `SEVERITY` rank table (`none` 0 through `critical` 4 and `block` 5, with `moderate` aliased to `medium`); otherwise compare as strings. That is why `risk<=medium` works on a string field without anyone writing an enum.

Rule ordering is a static specificity score, not file order. `rule_score` sums the length of the concatenated subject, tool and resource, subtracts eight per wildcard character and adds twenty per constraint, and `load_policy` sorts descending on that score with the original line number as a stable tiebreak. `evaluate_event` then collects every matching rule, and deny always wins over allow regardless of score. With no match at all, `--fail-closed` (the default) denies and `--fail-open` allows with an explicit reason recorded in the decision. A matched allow whose rule carries a `budget_ms` and whose event carries `latency_ms`, `duration_ms` or `elapsed_ms` above that budget still allows, but attaches a warning string.

Output comes in three shapes. `emit_jsonl` prints one decision object per line with subject, tool, resource, action, reason, `matched_rules`, `warnings` and the source line number. `emit_markdown` prints a summary report with rule count, event count, allow and deny totals, a warning count and a table of every decision, escaping pipes in reason text. `emit_lua_policy` skips events entirely and prints the sorted, parsed rule set as a Lua table literal, ready to be `require`d by a gateway worker so the edge never parses the text policy itself. `--self-test` runs `self_test`, which exercises JSON decoding, nested path lookup, both glob forms, a rule match, an allow decision and deny precedence.

## Usage

```bash
# audit a JSONL trace, one decision object per line
lua McpToolPolicyCompiler.lua --policy policy.txt --events calls.jsonl --format jsonl

# human readable report with allow/deny counts
lua McpToolPolicyCompiler.lua --policy policy.txt --events calls.jsonl --format markdown

# read events from stdin, allow anything no rule covers
cat calls.jsonl | lua McpToolPolicyCompiler.lua --policy policy.txt --fail-open

# compile the policy to a Lua table for an OpenResty worker
lua McpToolPolicyCompiler.lua --policy policy.txt --format lua > compiled_policy.lua

# verify the engine itself
lua McpToolPolicyCompiler.lua --self-test
```

Policy file, one rule per line, `effect|subject|tool|resource|constraints|budget_ms|reason`:

```
# comments and blank lines are ignored
allow|agent:ci|github.*|repo:kspavankrishna/**|risk<=medium;cost_usd<=0.25|30000|approved CI automation
deny|*|shell.exec|*|args.command~*rm -rf*||dangerous shell command
deny|*|github.*|*|args.path~**/.env||secret file access
```

Flags: `--policy PATH` (required unless `--self-test`), `--events PATH` (defaults to stdin), `--format jsonl|markdown|lua` (default `jsonl`), `--fail-open`, `--fail-closed` (default), `--self-test`, `--help`.

## Notes

- The process exits 0 even when events are denied. It reports, it does not signal. Wire your own check on the decision stream if you want CI to fail on a deny.
- Errors exit 1 (`die`), bad CLI usage exits 2: unknown option, unknown format, missing `--policy`.
- Constraint parsing picks the operator by scanning the `OPERATORS` list in order, not by leftmost position. A field name or expected value containing `~` or `=` can split in a surprising place. Keep field names plain.
- Globs are translated to Lua patterns, not regex. No alternation, no character classes, no capture groups. `glob_to_pattern` also runs on every comparison with no caching, so very large policies over very large traces will show it.
- `budget_ms` produces a warning only. It never flips an allow into a deny.
- Malformed JSONL lines are not skipped. They become a synthetic deny with subject `invalid-json`, tool `parse` and the parser error as the reason, so a corrupt log cannot silently pass.
- This decides. It does not intercept. Nothing here hooks an MCP transport or blocks a live call on its own. You place it in front of the call, in a gateway or a runner, or you run it over recorded traces.
- Pure Lua standard library, no external modules. Tested against the reference interpreter invoked as `lua`.
