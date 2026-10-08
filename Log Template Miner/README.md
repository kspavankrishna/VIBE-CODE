# Log Template Miner

A single pass F# log parser that turns millions of raw log lines into a few hundred templates, with hard memory limits and a drift check you can run in CI. Feed it a log file, get back "this shape happened 2.1 million times, this new one appeared after the deploy".

**Language:** F# | **Lines:** 855 | **Added:** 2026-10-08

## What this solves

Nobody can read a million log lines. Grep works when you already know the string you want. It does not help when you ask the harder question: what is in this log that I have never seen before?

Log template mining answers that. Lines like `user 4812 logged in from 10.2.3.4` and `user 9 logged in from 10.9.9.1` are the same event with different parameters. A miner folds them into one template, `user <NUM> logged in from <IP>`, and counts it. After that you can sort by count, sort by error share, or diff today against last week.

Most miners I found have the same problems when you point them at real traffic:

- They keep every cluster forever, so one noisy service with a request id in the middle of the message grows the table until the process dies.
- They choke on one 40 MB line, because they read with a plain `ReadLine`.
- They treat a Java stack trace as 60 separate log events and bury the real signal.
- They cannot read JSON logs, which is what most services emit now.
- They give you a nice report once and nothing you can gate a deploy on.
- A single bad regex on a hostile line can stall the whole pipeline.

This project is a Drain style miner written to survive those cases. It is also useful as the preprocessing step before you hand logs to an LLM. Sending 3 million lines to a model is impossible. Sending 200 templates with counts, error shares and one sample each is cheap and gives the model something it can actually reason about.

## Why I built it

I kept getting the same request in different clothes: "something is wrong after the release, can you look at the logs". The honest answer was always a manual hunt. Count the lines, eyeball the top messages, open the file in an editor, search for the word error. That does not scale and it does not repeat.

I wanted two things. First, a tool I can run over any log stream with a fixed memory ceiling, so it is safe on a laptop and safe on a build runner. Second, a way to say "compare this run to the last good run and tell me what changed", with an exit code. That second part is what makes it a CI step instead of a toy.

F# suits this well. The data is small records and discriminated unions, the tree walk is plain loops, and the .NET base library already ships a fast JSON reader and a regex engine with match timeouts. There are no NuGet packages. The whole thing is one source file and one project file.

## When to use it

- You are on call and need the shape of a noisy log in under a minute.
- You want a deploy gate: fail the pipeline when more than a few unseen log templates show up in the canary.
- You want to cut log volume. Sorting templates by count shows which three messages produce half your ingest bill.
- You are preparing logs for an LLM and need a compact, faithful summary instead of a raw dump.
- You want to find messages that only appear in errors, using the per template error and warning counts.
- You need a stable cluster id for each record so you can join the result back to your own tooling.

It is not a log shipper and it is not a search index. It reads text and reports.

## How it works

The core is the `Miner` type. It follows the Drain idea: a fixed depth prefix tree, then a similarity check inside a small leaf.

**Masking.** Before tokenising, `applyMasks` runs the list in `builtInMasks` over the message. Timestamps become `<TS>`, UUIDs `<UUID>`, emails `<EMAIL>`, IPv4 addresses with an optional port `<IP>`, hex values `<HEX>` and plain numbers with an optional unit such as `ms`, `MB` or `%` become `<NUM>`. Each pattern is built by `mkRegex` with a 50 millisecond match timeout. If a pattern times out on a line, that mask is skipped for that line and `MaskTimeouts` goes up. You can add your own with `--mask NAME=REGEX`. Those run first and produce `<NAME>`.

**The tree.** `leafFor` walks the tree. Level one is the token count. The next `Depth - 2` levels are the leading tokens. A token that looks like data goes to the shared wildcard branch. `keyOf` decides that: placeholders, anything with a digit and anything longer than 64 characters count as data. Each node can hold at most `MaxChildren` children. Once a node is full, new keys overflow into the `Wildcard` child, which is the literal `<*>`. That keeps the tree finite even when the first word of every line is unique.

**The leaf.** Inside the leaf, `similarity` compares the new tokens to each cluster template. Wildcard positions are skipped, so a template that has already generalised cannot swallow everything. The best score wins, ties go to the cluster with the higher count. If the score reaches `SimThreshold` (0.5 by default) the line joins that cluster and every differing position in the template turns into `<*>`. Otherwise a new `Cluster` is created.

**Bounded memory.** `evictIfNeeded` enforces `MaxClusters`. When the live set goes over the cap it sorts by `LastLine` and drops the least recently seen clusters in one batch, the overflow plus ten percent. Doing it in batches keeps eviction amortised, so a stream of all unique messages does not turn into a quadratic scan. Evicted clusters are flagged and pruned from their leaf lazily. The `Stats` record reports `EvictedClusters` and `EvictedRecords`, so you can see exactly how much the cap cost you instead of silently losing data.

**Hostile input.** `readLines` is a bounded reader. It reads in 16 KB chunks and keeps at most `MaxLineChars` per line. Anything past that is dropped until the newline and counted as truncated. It handles `\r\n`, a final line with no newline, and empty input. Token count per message is capped by `MaxTokens`.

**Stack traces.** `Feed` treats lines that start with whitespace, `Caused by`, `Suppressed:` or `Traceback (most recent call last)` as continuations of the previous record. They are not clustered. They increment `Folded`, and the first one is stored as the cluster `Hint`, so you see `at com.example.Handler.run(Handler.java:42)` next to the template that produced it. Turn this off with `--no-fold`.

**JSON logs.** When a line starts with `{`, `extractJson` parses it with `JsonDocument` at a depth limit of 16. It takes the message from the first of `msg`, `message`, `log`, `body`, `text` or `event`, and the level from `level`, `severity`, `lvl` or `levelname`. If the line is not valid JSON or has no usable message, it is clustered as raw text and `JsonFallbacks` is incremented. `--raw` disables this.

**Levels.** For plain text, `detectLevel` looks at the first eight tokens for words such as ERROR, FATAL, WARN or PANIC. Each cluster tracks `Errors` and `Warns`.

**Snapshots and drift.** `saveSnapshot` writes the templates and counts to JSON, atomically through a temp file and `File.Move`. `loadSnapshot` reads it back and refuses any version other than 1. `compareBaseline` then reports three things:

- New templates: no baseline template covers them. A baseline template covers a current one when lengths match and every literal token agrees. A baseline wildcard matches anything.
- Vanished templates: a baseline template with no current coverage and at least `--min-count` hits.
- Shifted templates: the share of total volume changed by at least `--shift` times in either direction. Each current template is credited to its most specific covering baseline template, so counts are not double counted.

`--fail-on-new N` turns this into a gate with exit code 3.

## Usage

Build and run with the .NET 8 SDK. The project file sits next to the source.

```
dotnet build -c Release

# summarise a file
dotnet run -c Release -- app.log

# read stdin, top 20 templates, JSON output
cat app.log | dotnet run -c Release -- --top 20 --format json

# save a baseline from a known good run
dotnet run -c Release -- --save-snapshot good.json good.log

# compare a canary run against it and fail if more than 3 new templates appear
dotnet run -c Release -- --baseline good.json --fail-on-new 3 canary.log

# write record to cluster id pairs for joining
dotnet run -c Release -- --annotate ids.tsv app.log

# add a custom mask for order ids like ORD-123456
dotnet run -c Release -- --mask ORDER=ORD-[0-9]+ app.log

# run the built in checks
dotnet run -c Release -- --self-test
```

Options: `--sim`, `--depth`, `--max-children`, `--max-clusters`, `--max-tokens`, `--max-line`, `--mask`, `--raw`, `--no-fold`, `--top`, `--format text|json`, `--annotate`, `--save-snapshot`, `--baseline`, `--shift`, `--min-count`, `--min-new`, `--fail-on-new`, `--self-test`, `--help`.

Exit codes: 0 success, 1 runtime error such as a missing file, 2 bad usage, 3 drift gate tripped, 4 self test failed.

## Notes

- I could not run the .NET compiler in the environment where I wrote this, so run `--self-test` first on your machine. It checks masking, wildcard generalisation, stack trace folding, JSON handling, the eviction bound, the bounded reader and baseline drift. If your SDK reports a compile error, open an issue with the message.
- Tuning: lower `--sim` merges more aggressively, higher splits more. Raise `--depth` when many different messages share the same first two words.
- Worst case tree size is roughly `MaxTokens` times `MaxChildren` to the power `Depth - 2` nodes, but that needs adversarial input. Normal logs use a tiny fraction.
- Multi line pretty printed JSON is not supported. Use one JSON object per line.
- Templates drift as clusters generalise, so the cluster id is stable but the template text for an id can get more general over a run. Snapshots store the final text.
- Stack trace folding is heuristic. A log format whose real messages start with a space will fold wrongly, so pass `--no-fold` for those.
- JSON output includes one raw sample line per template and the text report shows stack trace hints. Masks hide common identifiers but do not scrub secrets, so treat both as sensitive.
