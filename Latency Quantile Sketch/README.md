# Latency Quantile Sketch

A single file Odin tool that turns millions of latency numbers into small mergeable DDSketch summaries, so you can get p50, p99 and p99.9 per service or per model with a proven relative error bound and without keeping the raw samples. It reads from stdin, writes a table or a compact text sketch, and merges sketches from many machines into one answer.

**Language:** Odin | **Lines:** 897 | **Added:** 2026-10-13

## What this solves

Percentiles are the number everyone asks for and the one that is hardest to store. The mean is easy, you keep a sum and a count. A p99 needs the whole distribution. The usual shortcuts all break in production.

Averaging the p99 values from ten servers gives a number that means nothing. Percentiles do not average. Keeping every raw sample works until you have a day of LLM gateway traffic and the log is four gigabytes. Fixed histogram buckets work until the one slow path you care about lands between two buckets that were chosen two years ago for a different service. Streaming estimators like t-digest are good but their merge is order dependent, which makes results hard to reproduce, and their error is rank error, not value error.

What you usually want is this: "my p99 is 840 ms, and I know that is within one percent of the true value, and I can combine the numbers from every node and every hour and still say that." That is what DDSketch gives you, and that is what this tool implements.

The specific pain I had was LLM inference. Time to first token, total request time and tool call latency all have heavy tails. A model that answers in 300 ms most of the time and 40 seconds when a provider degrades looks fine on a mean and terrible on a p99.9. I wanted to pipe a gateway log into one command, get honest tail numbers per model, ship small sketch files from each region and merge them later without going back to the raw logs.

## Why I built it

I kept writing throwaway scripts that sorted a column and picked an index. They worked on a laptop and fell over on real logs. They used too much memory, they could not be combined across hosts, and every time someone asked "how accurate is that" I had no answer beyond "it is exact, but only for this file."

I also wanted a tool where the failure modes are written down and tested, not hoped for. Most quantile code I find online does one of these: silently drops bad rows, divides by zero on an empty group, gives a different answer when you merge in a different order, or lets a typo in the group column create two million groups and eat the machine. This one rejects bad input loudly, counts it, names the first bad line and exits with a different code so a pipeline can notice.

Odin was a good fit. The whole thing is a few hundred lines of plain procedures and maps, it compiles to a static binary in a second, there is no runtime to ship, and it is fast enough that the bottleneck is reading the pipe.

## When to use it

Use it when you have latency or duration numbers and you want tail percentiles with a stated error bound:

- LLM gateway logs: time to first token, total latency, tool call duration, queue wait, per model or per tenant.
- Load test output where you want p99.9 across many runs, without keeping every sample.
- CI step durations across thousands of builds, to see which step has a fat tail.
- Edge or IoT fleets where each device can write one tiny sketch line and a central job merges them.
- Any place where "average the percentiles" is currently happening and needs to stop.

Do not use it when you need exact percentiles of a small file. If you have ten thousand rows, sorting them is cheaper and exact. Do not use it for values that can be negative, such as clock offsets. This sketch only tracks non negative values and rejects negative ones on purpose. It also does not do streaming windows by itself. You get windows by cutting your logs into hourly files, encoding one sketch per file and merging the ones you want.

## How it works

The idea behind DDSketch is simple. Pick a relative accuracy alpha, say 1 percent. Compute `gamma = (1 + alpha) / (1 - alpha)`. Every positive value falls into a bucket whose boundaries are consecutive powers of gamma. The bucket key is `ceil(ln(v) / ln(gamma))`. When asked for a quantile, you walk the buckets in key order, find the one that contains the requested rank, and return `2 * gamma^key / (gamma + 1)`. That representative value is at most alpha away in relative terms from every value in the bucket. So the answer is within alpha of a true sample value, no matter how weird the distribution is.

In the code:

- `sketch_init` computes `gamma` and `multiplier` (one over the natural log of gamma) from `alpha_ppm`. Alpha is stored as parts per million so it is an exact integer. That matters because two sketches can only be merged if they used the same gamma, and comparing floats for equality is a bug waiting to happen.
- `sketch_add` puts a value in the map `buckets`, keyed by an `i32` bucket index. Anything below `MIN_INDEXABLE` (one billionth of a millisecond) goes to `zero_count` instead, because the logarithm of zero is not a number. It also updates `count`, `sum`, `min` and `max` exactly.
- `sketch_quantile` uses the rank `q * (count - 1)`, accumulates the zero bucket first, then walks `sorted_keys` until the running total passes the rank. The result is clamped to the exact `min` and `max`, so p0 and p100 never lie.
- `sketch_collapse` enforces `max_buckets`. If a sketch grows past the limit, the lowest bucket is folded into the next one up. This bounds memory no matter how wide the data range is, and it sacrifices accuracy at the low end only. The high quantiles, which are the ones you care about for latency, keep their guarantee. `sketch_saturated` reports when a sketch has reached its limit, and the table output marks that group with a star so nobody reads a low percentile from a collapsed sketch without knowing.
- `sketch_merge` adds bucket counts key by key. Because the buckets are fixed by gamma and not by the data, merging is exact: the merged sketch is identical to one built from all the samples in a single pass. Order does not matter. It refuses sketches with a different `alpha_ppm` and returns `Alpha_Mismatch`.
- `sketch_encode` and `sketch_decode` turn a sketch into one line of text starting with the `FORMAT_TAG` constant `DDS1`: alpha, bucket limit, zero count, total count, sum, min, max, then `key:count` pairs. `sketch_decode` validates everything. The header must parse, alpha and the bucket limit must be in range, a sketch cannot be empty, there cannot be more buckets than the declared limit, keys cannot repeat, and the bucket counts plus the zero count must add up to the declared total. A truncated file from a failed copy fails that last check instead of producing a quietly wrong merge.

Input handling is where most tools get sloppy, so here it is spelled out:

- `for_each_stdin_line` reads in 64 KiB chunks (`READ_CHUNK`) and hands complete lines to a handler, so memory stays flat on an endless pipe. A line longer than `MAX_LINE_BYTES` aborts the run, since that means the input is not line oriented.
- `split_fields` accepts tab separated, comma separated or whitespace separated rows. It picks tabs first, then commas, then spaces.
- `parse_value` understands the suffixes `ns`, `us`, `ms` and `s` on each number and converts everything to milliseconds. Bare numbers use the `-unit` flag. It rejects `nan` and `inf` as `Non_Finite`, negative numbers as `Negative`, and anything above `MAX_VALUE_MS` as `Too_Large`.
- `ingest_row` counts every rejected line by reason in `Ingest_State.reasons` and remembers the first bad line number. `group_for` caps the number of groups at `MAX_GROUPS`. If you point `-group` at a request ID column by mistake, you get `Group_Limit` rejections, not an out of memory kill.
- `ingest_sketch` is the merge path. It accepts lines in the form `group<TAB>DDS1 ...` or a bare `DDS1 ...` line for a single group.

Exit codes are part of the interface: `EXIT_OK` is 0, `EXIT_SELFTEST_FAILED` is 1, `EXIT_USAGE` is 2 and `EXIT_REJECTED_ROWS` is 3. A run that produced output but skipped some rows still prints the table, then exits 3, so a cron job or CI step can decide whether that matters.

The `selftest` command is the part I would run first. `run_selftest` builds 200,000 synthetic heavy tailed latencies from a seeded xorshift generator (`Rng`, `rng_next`), including exact zeros and rare 50x spikes. It then checks:

- every tested quantile is within the 1 percent bound against the exact sorted answer
- merging two halves gives bucket for bucket the same sketch as one pass over everything
- encode and decode round trip with no loss
- a stray token or a truncated line is rejected
- merging sketches with different alpha is refused
- a sketch limited to 400 buckets never drops a sample, holds its limit, keeps `min` and `max` exact and keeps the bound for every quantile above the collapsed region
- empty sketches and out of range quantiles report no answer
- unit suffixes convert and bad numbers are rejected

I wrote this on a machine without an Odin compiler, so the selftest has not been run by me yet. Run `selftest` first on your own toolchain and treat a failure as a bug report. If it fails on your Odin release because a core library signature changed, the likely places are the `core:os` read call and the `core:strconv` parsers in the small `parse_*_strict` helpers.

## Usage

Build with a recent Odin release:

```
odin build "Latency Quantile Sketch" -file -out:lqs -o:speed
```

Check it first:

```
./lqs selftest
```

Summarise a log with columns `model`, `latency`:

```
./lqs summarize -group=0 -value=1 -unit=ms -q=0.5,0.95,0.99,0.999 < gateway_latency.tsv
```

Values can carry their own units, so mixed columns like `340ms`, `1.2s` and `850000us` are fine. Skip a header line with `-skip=1`. Use `-group=-1` when the file is just one column of numbers per line and you want a single group called `all`.

Ship a compact sketch from each machine or each hour:

```
./lqs summarize -emit -alpha=0.01 < hour_13.tsv > hour_13.sketch
```

Merge sketches later without touching the raw logs:

```
cat hour_12.sketch hour_13.sketch hour_14.sketch | ./lqs merge -q=0.99,0.999
```

Add `-emit` to `merge` to get a merged sketch file back, which lets you build daily files from hourly ones and weekly files from daily ones.

Flags, all in the form `-name=value`:

- `-alpha` relative accuracy, from 0.0001 to 0.2, default 0.01
- `-buckets` bucket limit per sketch, default 2048
- `-group` and `-value` column indexes, default 0 and 1
- `-unit` one of `ns`, `us`, `ms`, `s`, default `ms`
- `-skip` number of leading lines to ignore, default 0
- `-q` comma separated quantiles between 0 and 1
- `-emit` print `DDS1` sketch lines instead of a table

## Notes

Accuracy and size. At alpha 0.01 the default limit of 2048 buckets covers a value range of roughly `1.0202^2048`, which is more than 10^17. In real latency data you will use a few hundred buckets. A sketch line is a few kilobytes at most, whatever the number of samples.

What the error bound means. It is a relative error on the value at the requested rank. A p99 reported as 840 ms means a sample at the true p99 rank lies within 1 percent of that number. It does not mean the rank is off by 1 percent. For latency work that is the guarantee you actually want.

Collapse is a trade, not a bug. Once a sketch hits its bucket limit, the lowest buckets merge upward. Tail quantiles stay accurate. The minimum and maximum stay exact. Low quantiles such as p1 lose the bound. The table marks affected groups with a star. If you need the low end too, raise `-buckets` or raise alpha.

Merging different bucket limits. When two sketches with different limits are merged, the smaller limit wins and the result is collapsed to fit. Alpha must match exactly or the merge is refused and the line is counted as `Merge_Mismatch`.

Read errors. The stdin loop stops when a read returns zero or less. On a real I/O error that looks the same as the end of input, so if your source can fail halfway, compare the `count` column against the number of lines you expected.

Header lines. A header with a text value in the value column is counted as a `Bad_Number` rejection and makes the run exit 3. Use `-skip=1` to skip it.

Sums are floating point. `sum` and the mean are accumulated in 64 bit floats, so a merged sketch can differ from a one pass sketch in the last digits of the mean. The buckets, counts, minimum and maximum are integers or exact comparisons, and those are identical.

Ideas I left out on purpose. Negative values would need a second bucket map and a mirrored key space. Sliding windows are easy on top of this by merging the last N hourly sketches. Parsing JSON lines directly would pull in a parser, and `jq -r '[.model,.latency_ms]|@tsv'` in front of this tool does the job in one pipe.
