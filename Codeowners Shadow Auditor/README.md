# Codeowners Shadow Auditor

A CODEOWNERS linter for GitHub that finds the rules that never fire, the owners that silently disappear and the files nobody owns. It is written in PureScript, runs on Node and is built to sit in CI as a gate.

**Language:** PureScript | **Lines:** 950 | **Added:** 2026-10-28

## What this solves

A CODEOWNERS file looks like a list, but GitHub reads it like a stack: the last matching pattern wins and it wins completely. Owners from an earlier line are not merged in. They are replaced. That one rule produces three kinds of damage that nobody notices until a review goes wrong.

The first is the shadowed rule. Someone adds `* @acme/platform` or `/src/**` near the bottom of the file, usually while tidying. Every rule above it that covers the same paths is now dead. The file still looks full of careful ownership, but none of it applies. The second is the dropped owner. You write `/src/ @core @security` and later `/src/auth/ @identity`. People read that as "identity as well", but the security team no longer gets a review request on auth code. The third is the quiet failure of syntax GitHub does not support: a `!` negation, a `[a-z]` range, a typo in an owner handle. GitHub skips the whole line and the path falls back to whatever matched before.

Existing tools mostly check that owners exist and that the syntax parses. That misses all three. This tool answers the harder questions: which lines can never decide ownership, which lines override owners from an earlier line, which patterns match nothing in your real file tree and which files end up with no owner at all.

## Why I built it

I kept seeing CODEOWNERS files in large repositories that had grown by accretion. Every reorganisation added a few lines on the bottom and nobody removed anything. Branch protection said "require review from code owners" and everyone assumed that meant the people named in the file. In practice a path could be owned by the wrong team for a year.

I wanted a check that reasons about the pattern language instead of just parsing it. That means deciding when one pattern covers another, and that is a small language inclusion problem. It is a good fit for PureScript: the patterns are an algebraic data type, the matcher and the cover proof are pure functions and the whole analysis is testable without touching a file system. The only effects are in the thin command line layer.

## When to use it

Use it as a pull request check on any repository that turns on required code owner review. Run it with the list of tracked files and it will tell you what ownership looks like today, not what the file intends. Use it when you inherit a monorepo and want to know whether the CODEOWNERS file is trustworthy. Use it before a team reorganisation to see which lines are dead and can be deleted. Use it in a scheduled job to catch drift when directories are renamed and rules go stale.

It is not a replacement for GitHub's own CODEOWNERS error view. It does not call the GitHub API, so it does not check that a team exists or has write access. It checks everything that can be decided from the file and the file list alone.

## How it works

The code is split into four modules plus a small JavaScript file for the Node bindings.

`CodeownersPattern.purs` is the core. `compilePattern` turns a pattern string into a `Pattern` record with a list of `Token` values (`DoubleStar` or `Glob`), an `anchored` flag, a `dirOnly` flag for a trailing slash and a `cascade` flag. A pattern with no slash floats, so it gets a leading `DoubleStar`. A trailing `/**` is folded into `dirOnly`, because `/src/**` and `/src/` claim the same files. A pattern like `**` or `/**` becomes the catch all token pair. A pattern such as `/` that has no segment compiles to `Nothing`.

The `cascade` flag is where GitHub differs from plain gitignore. A directory name or a trailing slash owns everything below it. A trailing wildcard such as `/docs/*` owns only the direct children. `compilePattern` sets `cascade` when the pattern is `dirOnly` or when the last glob has no `*` or `?`. I took that from the GitHub documentation and kept it in one place so it is easy to change if the behaviour moves.

`matchesPath` answers "does this pattern claim this file" for a path split by `splitPath`. It tries every prefix length, applies the `dirOnly` and `cascade` rules and walks the tokens with `matchTokens`. Each segment is compared by `segmentGlobMatch`, which tracks a set of glob positions per character, so a hostile name like many `a` characters against `*a*a*a*b` stays linear instead of blowing up.

The interesting function is `covers`. `covers b a` is true when every path claimed by `a` is also claimed by `b`. It is conservative on purpose: false means "could not prove it", never "they are disjoint", so a warning from this tool is a proof and not a guess. The implementation is `simulate`, a symbolic walk of the tokens of `a` through the tokens of `b` treated as a small automaton. Each `Glob` in `a` is an unknown segment drawn from that glob. `segmentGlobCovers` decides whether a glob in `b` accepts everything that glob can produce. Each `DoubleStar` in `a` stands for any number of unknown segments, so only a `DoubleStar` or an all star glob in `b` can absorb them. The loop for that case, `orbit`, keeps only the positions that hold for every length, which is why it stays sound. It has a fuel limit of 64 steps and returns the empty set if it runs out, which again errs on the side of silence. The directory rules decide how `a`'s file set is extended and where `b` is allowed to stop matching, so `/docs/*` correctly fails to cover `/docs/api/x.md`.

`CodeownersFile.purs` parses the file. `parseCodeowners` handles comments, inline comments, tabs, carriage returns and the escaped `\#` start. It records an `Issue` per problem: `BadOwner`, `Negation`, `CharRange`, `Escape`, `EmptyPattern` and `RepeatedOwner`. `validOwner` accepts `@user`, `@org/team` and email addresses. `isEffective` mirrors GitHub: a line with a syntax error is skipped, so it takes no part in the analysis.

`CodeownersAnalysis.purs` turns rules into findings. `lint` produces these codes:

- `invalid-owner`, `unsupported-negation`, `unsupported-range`, `unsupported-escape` and `empty-pattern` as errors.
- `shadowed` and `duplicate-pattern` as warnings, naming the later line that wins.
- `repeated-owner` as a warning.
- `owner-override` as info, listing exactly which owners from an earlier line are dropped. It stays quiet when the earlier line is the `*` default, because every narrower rule would otherwise trigger it.
- `clears-ownership` as info for a rule with no owners.

`coverage` and `analyze` add the file list. Every file gets the last matching effective rule, and the result gives `total`, `owned`, the `unowned` list, a count per owner and a `RuleStat` per rule with `matched` and `won`. From those, `coverageFindings` adds `unmatched-rule` for a pattern that matches no file and `never-wins` for a rule that matches files but loses every one of them. That second check catches the case a pairwise proof cannot, where two or more later rules together cover an earlier one.

`CodeownersShadowAuditor.purs` is the command line. `parseArgs` reads the options, `renderText` and `renderJson` print the report and `gate` decides the exit code from `--fail-on` and `--max-unowned`. `CodeownersShadowAuditor.js` holds the Node bindings for reading files or stdin, writing output and setting the exit code. `RunCodeownersShadowAuditor.mjs` calls `main`.

## Usage

You need PureScript 0.15 and the library sources for prelude, effect, console, either, maybe, tuples, arrays, foldable-traversable, strings, integers, exceptions and ordered-collections. With spago, list those as dependencies and put the files from this folder in `src`. Without spago you can compile straight with `purs`:

```bash
purs compile 'path/to/package/sources/*/src/**/*.purs' *.purs -o output
node RunCodeownersShadowAuditor.mjs [options] <CODEOWNERS file>
```

I built and tested it with purs 0.15.16, compiling against library sources checked out from the package set.

Options, exactly as `parseArgs` reads them:

- `--files FILE` is a newline separated list of repository paths. Use `-` to read it from stdin.
- `--format text|json` picks the report format. The default is text.
- `--fail-on info|warn|error` sets the lowest severity that fails the run. The default is `error`.
- `--max-unowned N` fails the run when more than N files have no owner.
- `--unowned-limit N` caps how many unowned paths the text report lists. The default is 25.

Exit code 0 means the gate passed, 1 means it failed and 2 means a usage or input error. The CODEOWNERS file and `--files` cannot both read stdin.

Lint the file on its own:

```bash
node RunCodeownersShadowAuditor.mjs .github/CODEOWNERS
```

Check it against the real tree in a pull request job:

```bash
git ls-files | node RunCodeownersShadowAuditor.mjs --files - --fail-on warn --max-unowned 0 .github/CODEOWNERS
```

Try it on the sample in this folder:

```bash
node RunCodeownersShadowAuditor.mjs Examples/CODEOWNERS.bad --files Examples/Files.txt
```

On that sample the report flags `/src/auth/`, `/src/payments/*` and `/src/payments/legacy/` as shadowed by the `/src/**` line near the bottom, flags `/src/` as a duplicate of it and reports `/docs/` as matching a file that `*.md` then takes. It marks the `!` and `[a-z]` lines as unsupported, the `someone@@example` owner as invalid, `/ops/deploy.sh` as repeated and `/legacy-api/` as matching nothing. In the coverage section it lists `third_party/x/y.c` as the one unowned file.

To run the tests, compile everything including `TestCodeownersShadowAuditor.purs` and run:

```bash
node -e "import('./output/TestCodeownersShadowAuditor/index.js').then(m => m.main())"
```

There are 48 cases. They cover the matching rules, the cover proofs in both directions, the parser, every finding code, the coverage numbers, the argument parser, the gate and both renderers.

## Notes

Know the limits before you trust it. The cover proof looks at one later rule at a time, so a rule that is dead only because several later rules together cover it is caught by `never-wins` when you supply a file list and missed when you do not. Segment globs support only `*` and `?`, as in CODEOWNERS. GitLab sections and the `^` optional section marker are not parsed. Section headers would be read as patterns and reported, so run this on GitHub style files.

The `cascade` rule for trailing wildcards follows the GitHub documentation as I read it. If GitHub changes how it treats a directory matched by a wildcard, the one place to change is `compilePattern`.

Matching a file list costs rules times files. A repository with 100,000 files and 500 rules is tens of millions of checks, which is fine for CI but not for an editor on every keystroke. Duplicate paths in the list are removed before counting.

The tool only reads. It never edits your CODEOWNERS file, because choosing which of two overlapping lines to keep is a human decision about who should review the code.
