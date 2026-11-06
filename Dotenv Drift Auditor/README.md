# Dotenv Drift Auditor

A single file Nim CLI that lints your dotenv files, diffs them against `.env.example` and cross checks every key against the code that actually reads it. It finds the missing variable that only breaks in production. It never prints a secret value while doing it.

**Language:** Nim | **Lines:** 663 | **Added:** 2026-11-06

## What this solves

Every project I have worked on has the same slow leak. Someone adds `STRIPE_WEBHOOK_TOKEN` to the code, sets it locally and forgets to add it to `.env.example`. Someone else renames a variable in the staging file but not in production. A new hire copies the example file, leaves `changeme` in three places and wonders why the app boots and then fails on the first real request. None of this shows up in a type check or a unit test. It shows up at 2am.

The second problem is that "dotenv" is not one format. Docker Compose, python-dotenv, the Node dotenv package, Ruby dotenv and plain `source .env` in a shell all read the same file and disagree on the details. A duplicate key is last wins in Compose and first wins in some tools. An unquoted value with spaces works in one parser and breaks `source`. A `#` glued to a value is a comment in one library and data in another. A UTF-8 BOM silently renames your first key. CRLF line endings from a Windows editor leave a carriage return inside the value. You only learn which parser you really have when something breaks.

Dotenv Drift Auditor looks at all of that in one pass, offline, with no dependencies beyond the Nim standard library. Search terms people use for this problem: dotenv linter, .env.example validator, missing environment variable checker, env file diff, find unused environment variables, dotenv duplicate keys. This is that tool.

## Why I built it

I wanted a CI step that answers three plain questions. Is every file syntactically safe for all the common parsers? Does every environment file have every key the example promises? Does the code read any variable that nobody documented?

The existing linters I tried only did the first question, and most of them print the offending line, which means a leaked token ends up in CI logs. The tools that do diffs did not look at source code. None of them told me which findings were "parsers disagree" problems versus "you forgot a key" problems, which matter very differently when you are deciding whether to fail a build.

I picked Nim because the result is one static binary with a quick start, easy to drop into a CI image. The standard library has everything needed: a directory walker, JSON output and an option parser. No package manager step, no runtime.

## When to use it

Run it in CI on every pull request. It is cheap, so it fits in a pre commit hook too. It is most useful when:

- you keep a committed `.env.example` and want it to stay the truth
- you have separate staging and production env files in a deploy repo
- several people or several AI coding agents edit config and nobody owns the file
- you are moving between Compose and a hosted platform and need to know which keys the code really reads
- you want to be sure the example file never contains a real looking secret

It is not a secret scanner for your whole repo and it does not talk to a vault. It also does not know about variables that only your platform injects, so you may need `--ignore` for those.

## How it works

The code has four parts: a parser, a set of comparison checks, a source scanner and the output layer.

### The parser

`parseEnv` reads one file and returns a `ParsedEnv` with a list of `Entry` records and a list of `Finding` records. It strips a BOM and reports `I201`. It converts CRLF to LF and reports `I202`. Then it walks the text by index, because double quoted values can span lines and a line by line split would get the line numbers wrong.

For each assignment it handles an optional `export` prefix, keys up to the first `=` or whitespace, and three quoting styles. Double quotes support `\n`, `\t`, `\r`, `\"`, `\\` and `\$` escapes and can run over several lines. Single quotes and backticks are literal. Unquoted values end at the end of the line or at the first `#` that follows whitespace.

Each oddity gets a stable code so you can grep or filter them:

- `E001` an unterminated quote, with the line counter reset so the rest of the file still parses
- `E002` a line that has no `KEY=value` shape
- `E003` a duplicate key with a different value, the case where parsers genuinely disagree
- `W101` a duplicate with the same value
- `W102` a space before or after `=`
- `W103` an unquoted value with spaces
- `W104` a key that is not portable to shells, checked by `isPortableKey`
- `W105` text after a closing quote
- `W110` a trailing backslash
- `I203` interpolation, found by `hasExpansion`, which skips escaped dollars
- `I204` a `#` kept inside an unquoted value
- `I206` an `export` prefix

### The comparisons

`lastValues` builds a table of the final value per key, matching what Compose does. `checkReference` runs only on the example file. It calls `looksLikeRealSecret`, which flags a value when the key name matches one of `SecretHints` and the value is at least `SecretMinLen` characters with a Shannon entropy above `EntropyFloor`, computed by `shannon`. It also flags a connection URL with a real looking password, found by `urlPassword`. That is `E006`.

`compareEnv` diffs one env file against the reference. A key missing from the env file is `E005` when the example marks it required and `W106` when the example carries a default. "Required" is decided by `isRequired`: an empty value or something `isPlaceholder` recognises, such as `changeme`, `<your token>`, `xxxx` or anything starting with `your`. A placeholder that survived into a real env file is `E004`. An empty required value is `W111`. A secret style key that reuses the exact example value is `W108`. A key present in the env file but absent from the example is `W107`.

### The source scanner

`walkScan` recurses through the directories you give it, skipping the names in `SkipDirs`, symlinked directories, files over `MaxScanBytes`, binary files and anything named `.env*`. `scanText` then looks for the read patterns in `UsagePatterns`: `process.env.X`, `process.env["X"]`, `import.meta.env.X`, `os.environ[...]`, `os.getenv(...)`, `ENV.fetch(...)`, `env::var(...)`, `os.Getenv(...)`, `System.getenv(...)` and a handful more. In YAML, shell files and Dockerfiles it also reads `${NAME}` forms. `readName` pulls the name out and `isEnvName` accepts only uppercase names, which keeps noise down.

Names read in code but missing from the example become `W109` with up to three file and line locations. Documented keys that no scanned file reads become `I205`. Platform names such as `PATH`, `CI` and the `GITHUB_` prefix are skipped through `DefaultIgnore` and `DefaultIgnorePrefixes`.

### Output and exit codes

`renderText` prints one line per finding. `renderJson` prints a summary plus a findings array. Findings are sorted by file, line and code so output is stable between runs and diffs cleanly. Exit code is 1 when a finding reaches the `--fail-on` level, 0 otherwise and 2 for usage or read errors.

Values are never printed. For `E006` the message states length and entropy only. That choice is deliberate: CI logs are often readable by far more people than the secrets are.

## Usage

Build it:

```bash
nim c -d:release DotenvDriftAuditor.nim
```

Run the parser self checks first:

```bash
./DotenvDriftAuditor --self-test
```

Audit two environment files against the example and scan the source tree:

```bash
./DotenvDriftAuditor --reference .env.example --env .env.staging --env .env.production --scan src --scan deploy
```

Same thing as JSON for a CI annotation step, failing on warnings too:

```bash
./DotenvDriftAuditor -r .env.example --json --fail-on warn --scan . .env.production
```

Ignore variables your platform injects and skip the unused key report:

```bash
./DotenvDriftAuditor -r .env.example -s . --ignore RAILWAY_ENVIRONMENT --ignore-prefix FLY_ --no-dead
```

The real options are `--reference` or `-r`, `--env` or `-e`, `--scan` or `-s`, `--ignore`, `--ignore-prefix`, `--no-dead`, `--max-file-kb`, `--fail-on`, `--json`, `--self-test` and `--help`. Any bare argument is treated as an env file. `--scan` needs `--reference`, because the usage check compares against it.

A typical text finding looks like this:

```
ERROR E005 .env.production STRIPE_WEBHOOK_TOKEN: required key from .env.example is missing
WARN W109 .env.example INTERNAL_FEATURE_FLAG: read in code (src/flags.ts:12) but not documented here
```

## Notes

I wrote this in an environment where I could not run a Nim compiler, so treat the first build as a check, not a given. The file is plain standard library Nim and the `--self-test` flag exercises the parser, the placeholder rules and the scanner so a bad build is easy to spot. If you hit a compile error, open an issue with the message and I will fix it.

Limits you should know about:

- The scanner is pattern based, not a parser for each language. Dynamic names such as `process.env[name]` are invisible to it.
- It only reports uppercase names. A lowercase variable read in code will not appear in `W109`.
- Code that has a fallback default still shows up as undocumented. That is intended, since a default in code is configuration hidden from the example file.
- Entropy based secret detection has false positives and false negatives. It is tuned to be quiet. It only runs on the example file and on connection URLs.
- Only the final value of a duplicated key is used in comparisons.
- The dialect it enforces is the common intersection of Compose, python-dotenv and node dotenv. If you rely on one parser's special features, some `I` level findings are expected and you can ignore them.

Fail on `error` in CI to start, move to `warn` once the existing noise is cleaned up and keep `I` findings as documentation of where parsers differ.
