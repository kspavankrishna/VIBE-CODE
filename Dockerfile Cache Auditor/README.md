# Dockerfile Cache Auditor

Your Docker build reinstalls every dependency after a one line change and nobody can say why. This is a PureScript tool that reads a Dockerfile, flags the instructions that wreck layer caching, then takes a list of changed files and predicts which layers will rebuild and how much of the build that costs.

**Language:** PureScript | **Lines:** 1729 | **Added:** 2026-10-01

## What this solves

A slow Docker build is almost never slow because Docker is slow. It is slow because one early instruction changes on every build and drags every layer below it along. The usual suspects are `COPY . .` sitting above `npm ci`, an `ARG BUILD_DATE` declared before the compiler runs, an `apt-get update` frozen into its own layer, a base image that floats on `latest`, or a missing `.dockerignore` that lets the `.git` folder into the build context. Each one is easy to write and hard to see, because the Dockerfile still builds fine. You only notice when CI minutes double.

Linters like hadolint check style and a lot of good practice, but they look at one instruction at a time. They do not model the cache as a chain. Layer caching is a chain: the first instruction whose input changed rebuilds. Every instruction after it rebuilds too, even if nothing about it changed. That chain is the thing worth analysing. So this tool does two jobs.

First it audits the file with a set of cache focused rules. Second it simulates the chain. You give it the paths that changed in a pull request, the build args that changed and optionally a target stage. It tells you the first line that misses the cache, every instruction that rebuilds because of it and which of those rebuilds are pure position: layers like `npm ci` or `apt-get install` that reran only because they sit below something that changed, not because their own inputs changed. That last list is the actionable one. It is a list of instructions you can move.

## Why I built it

I kept answering the same question in code reviews: why did this build take eleven minutes when the diff was one line in a README? The answer was always an ordering mistake or a broad COPY. I was working it out by hand each time by reading the Dockerfile top to bottom and tracking what would invalidate what. That is mechanical work. A program should do it.

I also wanted something I could run in CI as a gate. A rule like "a change that only touches source files must not rebuild more than 40 percent of the build weight" is a real budget you can enforce. It catches the regression the day someone adds `COPY . .` above the install, not three weeks later when the bill arrives.

PureScript is a good fit for this. The whole problem is parsing text into a typed structure and then running pure functions over it. The parser, the rules, the glob matcher and the planner are all total functions with no hidden state. The only effects live in one small module that reads files and sets the exit code. The type checker caught a surprising number of mistakes while I wrote it, mostly in the stage graph code where an index and a name are easy to mix up.

## When to use it

- A pull request touches a few source files and the image rebuild is slow. Run it with the changed paths and see where the first miss lands.
- You want a CI check that fails when someone reorders a Dockerfile in a way that destroys caching.
- You are reviewing a Dockerfile you did not write and want a quick list of cache and layer problems.
- You maintain a monorepo with several Dockerfiles and want a number, the predicted rebuild percentage, to compare them.
- You are moving to BuildKit cache mounts and want to know which installs would benefit.
- You want to know what a changed build arg actually invalidates before you pass it from CI.

It is not a replacement for building the image. It predicts from the text of the Dockerfile and the paths you give it. It does not read file contents, so it cannot know that a COPY of a directory would hash the same after a whitespace edit. It only knows which paths changed.

## How it works

The code is split into five PureScript modules plus a small JavaScript foreign file for file access.

**Parsing.** `parseDockerfile` in `DockerfileParser.purs` turns text into stages. It strips a byte order mark, normalises line endings and reads parser directives at the top of the file, so `# escape=` with a backtick works for Windows Dockerfiles. `readLogical` joins continuation lines using the active escape character. It drops comment and blank lines that appear inside a continuation, which Docker allows. It keeps the leading whitespace of the next line so `install \` followed by an indented `curl` does not become `installcurl`. `readHeredocs` handles `RUN <<EOF` and `COPY <<-CONF` bodies, including the tab stripping form. It uses a negative lookbehind so `<<<` here strings are not mistaken for heredocs. `extractFlags` pulls `--from`, `--mount`, `--checksum` and `--platform` off RUN, COPY, ADD and FROM. `collect` walks the file and `parseDockerfile` groups instructions under the FROM that owns them, with global ARGs kept in a preamble. `resolveStage` accepts a name or a numeric index and `stageDeps` builds the dependency edges from FROM, `COPY --from` and `RUN --mount=from`.

**Rules.** `auditDockerfile` in `CacheRules.purs` runs a list of stage rules and sorts the findings by line. Each finding has an id, a severity, a line, a message and a concrete fix.

- `broadCopyBeforeInstall` is DCA001. It uses `installTool`, which recognises npm, yarn, pnpm, pip, uv, poetry, go mod, cargo fetch, bundler, composer, maven, gradle, dotnet, mix and pub. It reports a broad COPY that comes before a dependency install and writes the exact manifest files to copy first. `pip install -e .` is skipped on purpose because it needs the source.
- `fromPinning` is DCA002. It reports untagged and `latest` base images, tags without a digest and images chosen by a build arg. It ignores `scratch` and earlier stage names.
- `aptRules` is DCA003. It catches `apt-get update` in its own layer, installs without `-y`, missing `--no-install-recommends`, missing index cleanup, `apk add` without `--no-cache` and dnf or yum installs without cleanup. A cache mount on the relevant directory silences the cleanup warnings.
- `packageManagerRules` is DCA004. It covers pip without `--no-cache-dir`, bare `npm install` instead of `npm ci` and yarn or pnpm installs that may rewrite the lockfile.
- `addRules` is DCA005. It flags remote ADD without `--checksum` and local ADD that should be COPY.
- `volatileRules` is DCA006. It matches names like `BUILD_DATE`, `GIT_SHA`, `VCS_REF` and `CI_*` in ARG and also in LABEL or ENV values that expand them when expensive instructions follow. `refsIn` and `declaredArgs` extract the names.
- `deleteRules` is DCA007. It finds RUN instructions that only delete files, which cannot shrink an earlier layer.
- `singleStageBuild` is DCA008, `stageGraphRules` is DCA009 for forward references, duplicate names, unknown stages and dead stages. `cacheMountRules` is DCA010 and `contextRules` is DCA011, which checks `.dockerignore` for `.git` and `.env`.

`runWeight` gives every instruction a cost: 10 for a dependency install, 8 for a build step, 6 for system package work, 2 for any other RUN, 1 for COPY and ADD and 0 for metadata. These are heuristics, not measurements. They exist so the rebuild percentage means something better than counting lines.

**Matching paths.** `PathGlob.purs` implements Docker style matching. `contextMatches` treats a COPY source as a pattern that also matches everything underneath a directory it hits, `*` and `?` stay inside one path segment and `**` crosses segments. `dockerignoreIgnores` applies `.dockerignore` lines in order with last match wins and `!` re-inclusion. Patterns are anchored to the context root, which is how Docker behaves and is different from gitignore.

**Simulating the chain.** `planInvalidation` in `CacheInvalidation.purs` picks the target stage and uses `closure` to find every stage it needs, since BuildKit skips the rest. Changed paths are filtered through the ignore rules first. For each needed stage in order, `planStage` calls `trigger` on every instruction. `trigger` decides whether that instruction's own input changed: a COPY source matching a changed path, a `COPY --from` of a stage that already rebuilt, a bind mount, a base stage that rebuilt, or a changed build arg. Per the Docker docs, a changed ARG counts as a miss at the first RUN after its declaration, so `trigger` carries a pending arg forward to the next RUN. The first instruction with a reason is the first miss and everything from there on is rebuilt. Reasons are kept only for instructions that triggered themselves, so the rest are identified as chained. The `positional` list in the result is the chained dependency installs and system package layers.

**The command line.** `parseArgs` and `audit` in `DockerfileCacheAuditor.purs` handle options and the gate. `renderText` and `renderJson` print the report. The JSON writer is a small hand written encoder with proper string escaping, so there is no extra dependency. `main` reads the Dockerfile, finds a `.dockerignore` (a file named after the Dockerfile with `.dockerignore` appended wins over the one in the context root), runs the audit and sets the exit code.

## Usage

You need PureScript 0.15 and the library sources. With spago, create a project that depends on prelude, effect, console, either, maybe, tuples, arrays, foldable-traversable, strings, integers, enums and exceptions, then put the files from this folder in `src`. Without spago you can compile straight with `purs`:

```bash
purs compile 'path/to/package/sources/*/src/**/*.purs' *.purs -o output
node RunDockerfileCacheAuditor.mjs [options] <Dockerfile>
```

I built and tested it with purs 0.15.16, compiling against library sources checked out from the package set.

Options, exactly as `parseArgs` reads them:

- `--context DIR` is the build context root used to find `.dockerignore`. It defaults to the Dockerfile directory.
- `--changed FILE` is a newline separated list of changed paths. Use `-` to read from stdin.
- `--arg NAME` marks a build arg whose value changed. Repeat it for more.
- `--target STAGE` picks the stage by name or index. It defaults to the last stage.
- `--format text|json` picks the report format.
- `--fail-on info|warn|error` sets the lowest severity that fails the run. The default is `error`.
- `--max-rebuild PERCENT` fails the run when the predicted rebuild weight is above that percent.

Exit code 0 means the gate passed, 1 means it failed and 2 means a usage or input error.

Audit a file:

```bash
node RunDockerfileCacheAuditor.mjs Examples/Bad.Dockerfile
```

Predict the damage of a pull request, with a budget:

```bash
git diff --name-only origin/main | node RunDockerfileCacheAuditor.mjs --changed - --max-rebuild 40 --fail-on warn Dockerfile
```

On the good example in `Examples`, a change to `src/server.ts` reports that the dependency stage stays cached and the build stage misses at its `COPY . .`. It predicts 10 of 22 weight rebuilt. On `Examples/Bad.Dockerfile` the same change rebuilds 100 percent. The tool also lists the `npm install` and apt layers as rebuilds that happen only because of position.

To run the tests, compile everything including `TestDockerfileCacheAuditor.purs` and run:

```bash
node -e "import('./output/TestDockerfileCacheAuditor/index.js').then(m => m.main())"
```

There are 20 cases covering continuation lines, heredocs, the stage graph, glob and ignore semantics, the planner and the argument parser.

## Notes

- Weights are my own rough numbers. If your builds are dominated by something else, change `runWeight` in `CacheRules.purs`. The percentage is a guide for comparing changes, not a timing.
- A path that matches a COPY source counts as changed. The tool does not hash file contents, so a no op edit still reports a miss.
- A change to the Dockerfile itself is not modelled. Docker rebuilds from the edited instruction down. You already know which one you edited.
- Build secrets, `--mount=type=secret` and `ONBUILD` triggers are ignored by the planner.
- Remote ADD sources and external base images are assumed unchanged. A moved `latest` tag will not show up in the prediction, which is one reason DCA002 exists.
- The rules assume BuildKit, the default builder in current Docker. Cache mount advice does nothing on the legacy builder.
- Cache mount paths in the fixes assume the build runs as root. Adjust them if you set a `USER` earlier.
- The `Examples` folder holds one Dockerfile with most of the mistakes and one that passes clean. The base image digest in the good example is a placeholder, so replace it before using that file for real.
