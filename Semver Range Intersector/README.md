# Semver Range Intersector

Take every npm style version range that touches one package and tell me, exactly, whether any version can satisfy all of them at once. When the answer is no, it names the smallest set of constraints that cannot live together.

**Language:** Haxe | **Lines:** 991 | **Added:** 2026-10-31

## What this solves

Agent stacks pull in a lot of packages that all want the same shared dependency. A root package.json asks for `zod ^3.22.0`. One MCP server declares a peer dependency of `>=3.23.0 <4`. Another wants `~3.24.1`. A build tool insists on something else. Your package manager prints a wall of ERESOLVE text and you are left guessing which pair is the real problem.

The question underneath is simple: what is the intersection of these semver ranges and is it empty? Answering it correctly is not simple at all. Most quick implementations get at least one of these wrong:

- Prerelease rules. In node-semver a prerelease version only matches a range if some comparator in that same range names a prerelease of the same major.minor.patch tuple. So `^1.0.0-beta.1` and `<1.0.0` look like they overlap on `1.0.0-beta.2`, but they do not, because `<1.0.0` does not opt in to prereleases.
- Partial versions and x ranges. `>1.2` means `>=1.3.0`. `<=1.2` means `<1.3.0-0`. `^0.0` is not the same as `^0.0.0`.
- Exclusive bounds that look like they leave a gap. `>1.0.0` and `<1.0.1` leave nothing, because no release sits between them and prereleases are not opted in.
- `||` alternatives. Intersecting two ranges with several alternatives each is a cross product and some of the products are dead.
- Very large version numbers. Anything that parses components into a float or a 32 bit integer silently breaks on build numbers.

This tool implements the real rules, finds the smallest version that satisfies the intersection. When a package cannot be resolved it reduces the clash to an irreducible core of constraints, each tagged with where it came from.

## Why I built it

I kept losing time on dependency conflicts in repos that wire together several MCP servers and an agent runtime. Each server is published by a different team, each declares its own peer ranges and they drift. The error from the package manager tells you something failed. It does not tell you the three lines that matter out of forty.

I also wanted something I could run in CI against a plain text list of constraints without installing the whole dependency tree. That means the logic has to be correct on its own, not a thin wrapper around a package manager. And I wanted it to be portable. Haxe compiles to JVM, HashLink, C++, Python, JavaScript, Lua and more, so the same file can run as a Java jar in a build step, as a native binary in a tool host or as an interpreted script on a laptop.

The existing npm semver library can test a version against a range and can check whether two ranges intersect. It cannot give you the intersected range, the smallest satisfying version or the conflicting core across many constraints. Those three things are what you actually need when you are fixing a resolution failure.

## When to use it

- You maintain a monorepo or an agent platform and want a CI step that fails with a readable explanation when peer ranges stop overlapping.
- You publish an MCP server or a library and want to check, before release, that your peer ranges can coexist with the ranges of the packages you integrate with.
- You want to know the lowest version that satisfies a set of constraints, for example to test your minimum supported dependency.
- You have a list of versions that are actually published and want the highest one that meets every constraint, or a clear signal that the ranges overlap only on versions nobody released.
- You are debugging an ERESOLVE and want the minimal conflicting set rather than the full tree.

It does not read package.json or lockfiles and it does not talk to a registry. You give it constraint lines and, optionally, the list of published versions. That keeps it deterministic and offline.

## How it works

**Exact numbers.** The `Num` class stores every numeric component as a decimal string. `Num.cmp` compares by length then by text, `Num.inc` does carry arithmetic on the digits and `Num.norm` rejects leading zeros and anything longer than `Num.MAX_LEN` (16 digits, the npm limit). Nothing is ever converted to a float.

**Versions.** `Version` holds major, minor, patch and a prerelease identifier list. `Version.compare` follows the SemVer precedence rules: a release is greater than any of its prereleases, numeric identifiers are lower than alphanumeric ones and a longer identifier list wins when the shared prefix is equal. `Version.MIN` is `0.0.0-0`, the smallest version that exists. `succPre` and `nextPatch` give the smallest strictly greater prerelease and release, which the emptiness check needs.

**Parsing.** `RangeParser.parseSet` handles one comparator set. It glues a separated operator to its version (`>= 1.2.3`), recognises hyphen ranges (`1.2.3 - 2.3`) and passes each token to `parseComparator`. `parsePartial` reads a version with optional x, X or `*` wildcards. `desugar` is the heart of it: one case per operator (`""`, `=`, `~`, `~>`, `^`, `>`, `>=`, `<`, `<=`) that expands a partial version into explicit comparators, matching node-semver. Exclusive upper bounds are written with the `-0` floor (`<2.0.0-0`) so they exclude every prerelease of that tuple, which is what modern node-semver does.

**Intervals.** `Interval.of` collapses a comparator list into one lower bound and one upper bound, always keeping the tighter one and preferring the exclusive bound on a tie. `Interval.canonical` turns it back into at most two comparators, or a single exact version.

**The prerelease rule.** A `CSet` is one AND group: a canonical interval plus an `allow` field. `allow` holds one list of major.minor.patch tuples per source range that was merged in. `CSet.accepts` lets a prerelease through only if every list contains its tuple. This is the part naive tools skip. Merging two ranges simply concatenates the lists, so the rule is applied to each original range separately, exactly as if you tested the version against both. `CSet.fromComps` ignores the synthetic `<X-0` bounds when building the list, since no prerelease of X can sit below them.

**Emptiness without enumerating versions.** `CSet.minVersion` returns the smallest accepted version or null. The smallest member of a nonempty set is always one of a small set of candidates: the lower bound, the first release above it, the first prerelease above it, or the `-0` prerelease of an allowed tuple. It tests only those. That makes `Range.isEmpty` exact and cheap, with no sampling.

**Ranges and the cross product.** `Range` is a list of `CSet` alternatives. `Range.parse` drops alternatives that can never match and records them in `dead`. `Range.intersect` merges every pair, drops the empty ones and dedupes by `CSet.key`. Two limits protect you from a blowup: a pre check on the number of pairs and `Range.MAX_SETS` (4096) on the result. Both fail with a plain message telling you to simplify the `||` branches.

**Conflict cores.** `Analyzer.conflictCore` runs a deletion filter. It tries removing each constraint in turn and keeps the removal whenever the rest is still unsatisfiable. What is left is irreducible: drop any one member and the conflict disappears. Those are the lines you need to change.

**Published versions.** If you add an `available` line for a package, `Analyzer.analyze` calls `Range.maxSatisfying` on it. If the ranges overlap but no published version fits, the status is `no_published_match` and the exit code is 1. This is the case that wastes the most time in practice.

**Input and output.** `Analyzer.load` parses the constraint file and collects every error with its line number instead of stopping at the first. `SemverRangeIntersector.main` dispatches the commands and chooses the exit code. `SelfTest.run` checks a table of known cases, including the prerelease and partial version edge cases above.

## Usage

Build once to a jar:

```
haxe Build.hxml
java -jar SemverRangeIntersector.jar selftest
```

Or run without building:

```
haxe -cp . --run SemverRangeIntersector selftest
```

Commands:

```
check <file> [--json]            analyse a constraint file
satisfies <version> <range>      prints true or false
intersect <range> <range> ...    prints the intersection and its smallest version
max <range> <version> ...        prints the highest version that satisfies the range
selftest                         run the built in table
```

Constraint file format, one directive per line, fields separated by a pipe:

```
constraint zod | ^3.22.0 | root package.json
constraint zod | >=3.23.0 <4 | mcp-server-files peerDependencies
available zod | 3.22.4 3.23.8 3.24.0
```

`ExampleConstraints.txt` in this folder has one package that resolves, one that conflicts on plain ranges, one that conflicts on the prerelease rule and one where the ranges overlap only on versions that were never published. Try:

```
java -jar SemverRangeIntersector.jar check ExampleConstraints.txt
java -jar SemverRangeIntersector.jar intersect "^1 || ^3" ">=2 <4"
java -jar SemverRangeIntersector.jar satisfies 1.2.3-beta.3 "^1.2.3-beta.2"
```

Exit codes: 0 when every package can be resolved, 1 when any package conflicts or has no published match, 2 for bad input such as an unparseable range. With `--json` the same result is printed as a JSON document with a `reports` array and an `errors` array.

You can also use the classes directly from your own Haxe code:

```
var r = Range.parse("^1.2.0").intersect(Range.parse("~1.4.0"));
trace(r.show());
trace(r.minVersion().toString());
```

## Notes

- The intersection is printed in canonical form: one lower bound and one upper bound per alternative. When a prerelease allowance survives the merge it is shown as `{prerelease: ...}` after the comparators. That annotation is for reading, not for pasting back into a package.json.
- Build metadata after a `+` is accepted and ignored, as the SemVer spec says.
- Loose mode is not supported. Leading zeros, a missing patch in a full version and similar sloppy input are errors, not guesses.
- The prerelease behaviour follows node-semver without the `includePrerelease` option. If you rely on that option the answers will differ.
- Only the node-semver range grammar is covered. Tags such as `latest`, git URLs, file paths and `workspace:` ranges are rejected as unparseable.
- A constraint with a dead alternative, for example `>2 <1 || ^3`, still works but gets a warning on its line.
- `selftest` is the first thing to run after building on a new target. It covers the cases that most often go wrong: prerelease opt in, partial versions, exclusive bounds and big numbers.
- Output order follows the order in which packages first appear in the file, so diffs between runs stay readable.
