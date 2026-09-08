# Build Kit Cache Forensics

A Docker BuildKit build that used to take 40 seconds now takes 14 minutes, and the log only tells you which step was slow, not which layer lost its cache three steps earlier. This reads buildx rawjson or plain progress output and names the first uncached vertex that caused the whole rebuild fanout.

**Language:** JavaScript | **Lines:** 34 | **Added:** 2026-05-19

## What this solves

Cache misses in `docker buildx build` are invisible in the place people actually look. The progress log prints a flat list of steps with durations. You see `#14 RUN npm ci` took 6 minutes and conclude npm is slow. It is not. The real event was `#9 COPY . .`, where a changed README invalidated the layer, which invalidated everything downstream, which is why the dependency restore ran at all. The slowest line in the log is almost never the line you need to change.

Without something that reconstructs the vertex graph, the debugging loop is guesswork. Someone edits the Dockerfile, pushes, waits ten minutes for CI, reads another flat log and guesses again. On a multi platform build it is worse, because the same uncached download runs once per architecture and nobody notices that the registry cache was never shared between them. The cost lands as CI minutes, registry egress and developer wait time, and the people who notice first are the ones whose PR checks went from three minutes to twenty.

The other failure mode is the build context. BuildKit uploads it before anything else, so a repo with `dist/`, `coverage/` and a few hundred megabytes of artifacts and no real `.dockerignore` ships all of that every time. The upload is slow and it invalidates COPY layers, and nobody sees the number because the transferring context lines scroll past in a second. This file measures all three: the earliest cache break and how much downstream work it forced, the context bytes, and the Dockerfile instructions that guarantee it happens again.

## Why I built it

Existing tooling stops at observability. `--progress=plain` gives you timings, `docker buildx du` gives you cache disk usage. Neither reconstructs the dependency edges between vertices, so neither can tell you that one COPY step is the root of eleven downstream misses. Hosted build scan services want your logs, and CI logs from a private monorepo are not always something you can hand to a third party.

The gap that mattered to me was the link between the trace and the fix. Knowing the cache hit rate was 12 percent is not actionable. Knowing that stage `builder` runs `npm ci` on line 14 after a broad `COPY . .` on line 9, with no lockfile copy in between, is actionable in thirty seconds. So this parses both the trace and the Dockerfile, and joins them.

## When to use it

- A GitHub Actions container build went from two minutes to fifteen and you want the root cause before the next push, not after five more.
- Your buildx pipeline builds `linux/amd64` and `linux/arm64` and you suspect the same install step runs uncached twice.
- A monorepo image rebuilds dependencies on every commit even when only docs changed.
- You are writing a new Dockerfile and want a second opinion on layer ordering and cache mounts before it ships.
- You want a CI gate that warns when the cache hit rate on main drops below a threshold.
- The context upload feels slow and you need the byte count to justify writing a `.dockerignore`.

## How it works

Ingestion is format agnostic. `parseBuildKitTrace` sniffs the input: an already parsed object with a `vertices` array passes through, a JSON blob carrying `vertexes`, `logs` or `statuses` goes to `ingestRawJsonChunk`, newline delimited JSON goes to `parseRawJsonText`, and anything else falls to `parsePlainProgressText`. That last path is a state machine over regexes for `#N [stage] instruction`, `#N CACHED`, `#N DONE 1.2s`, `#N ERROR ...` and the generic `#N <log line>` form. Plain logs carry no wall clock timestamps, so it synthesises pseudo timestamps from the line index and takes real durations from the `DONE` suffix via `parseDurationToMs`, which understands `ms`, `s`, `m` and `h`.

Every vertex then goes through `parseVertexDescriptor`, which pulls the stage name, the `3/17` step index, the platform triple and the instruction kind (`RUN`, `COPY`, `ADD`, `FROM`, `WORKDIR`, `LOAD_CONTEXT`, `LOAD_METADATA`) out of the vertex name. `normalizeCommandSignature` strips the bracket prefix, replaces `sha256:` digests with a placeholder and collapses `n/m` counters, giving a stable key for grouping identical commands across stages and architectures. `mergeVertex` folds duplicate sightings together without overwriting a real value with null, and logs cap at the last 250 entries per vertex.

The core is `computeMissRoots`. `buildGraph` builds parent and child maps from each vertex's `inputs`, resolving digest references against the key map and keeping traces namespaced by `traceSource` so two ingested builds do not cross wire. A miss root is an uncached, non errored vertex with no uncached parent: the frontier of the rebuild, the first place where cache reuse stopped. From each root it runs a breadth first traversal over the children, staying inside the uncached set, accumulating `impactCount` and `impactDurationMs`. Roots sort by impact duration, so the top of the list is the single change that would have saved the most wall clock time. `inferLikelyCause` attaches a plain English reason based on whether the root is a broad copy, a context load, a dependency restore or a heavy RUN.

Dockerfile analysis is a separate static pass. `parseDockerfile` joins backslash continuations, tracks stages through `FROM ... AS name` and records line numbers. `analyzeDockerfileInstructions` walks each stage carrying two flags: whether a lockfile only copy has been seen (`isLikelyLockfileCopy`, twenty patterns spanning npm, pnpm, yarn, bun, pip, poetry, go, cargo, composer, bundler, maven and gradle) and whether a broad copy has been seen (`isBroadContextCopy`, which treats `.`, `*`, trailing slashes, globstars and bare source directories as broad). A `RUN` matching `PACKAGE_INSTALL_MATCHERS` after a broad copy with no lockfile copy in between is the `install-after-broad-copy` finding. Missing `--mount=type=cache` on an install or an `apt-get update` gives the cache mount findings, and `suggestCacheMount` emits the right target path for the detected ecosystem rather than a generic hint.

`buildRecommendations` merges all of it into a severity ranked list: context above `contextWarnBytes` (50 MiB default, high severity at double that), the top broad copy and cache mount findings, the largest miss root when it fans out to three or more steps, a cross platform duplicate found by `computeRepeatedCommands`, and a cache hit rate below 35 percent on a build with at least six vertices. If nothing fires it says so and suggests capturing rawjson for a more precise trace.

## Usage

```bash
# Pipe a live build straight in
docker buildx build --progress=plain . 2>&1 | node BuildKitCacheForensics.js

# Analyse a saved rawjson trace with the Dockerfile for structural findings
docker buildx build --progress=rawjson . > build.rawjson 2>&1
node BuildKitCacheForensics.js --input build.rawjson --dockerfile Dockerfile

# JSON mode for a CI quality gate
node BuildKitCacheForensics.js --input build.rawjson --format json > cache-report.json

# Two traces at once, for example amd64 and arm64
node BuildKitCacheForensics.js --input amd64.log --input arm64.log --dockerfile Dockerfile

node BuildKitCacheForensics.js --help
```

Flags: `--input <file>` (repeatable, bare arguments count as inputs too), `--dockerfile <path>`, `--format text|json`, `--slow-step-ms <n>`, `--context-warn-mb <n>`, `--help` / `-h`. With no input files it reads stdin. As a module:

```js
const {
  BuildKitCacheForensics,
  analyzeBuildKitTrace,
  parseBuildKitTrace,
  parseDockerfile,
  formatTextReport,
  VERSION,
} = require("./BuildKitCacheForensics.js");

const tool = new BuildKitCacheForensics({ contextWarnBytes: 25 * 1024 * 1024, hotspotLimit: 20 });
tool.ingest(rawJsonText, "amd64").ingest(plainLogText, "arm64").setDockerfile(dockerfileText);
const report = tool.analyze();

console.log(report.summary.cacheHitRate, report.missRoots[0]);
console.log(formatTextReport(report));

// one shot form
const quick = analyzeBuildKitTrace(logText, { hotspotLimit: 5 });
```

The report contains `version`, `analyzedAt`, `sourceTypes`, `summary`, `missRoots`, `slowSteps`, `repeatedCommands`, `dockerfileFindings` and `recommendations`. The file is a UMD wrapper, so in a browser it attaches `BuildKitCacheForensics` and `BuildKitCacheForensicsApi` to the global object.

## Notes

- Zero dependencies. The only require is `node:fs` inside `runCli`, so the analysis core runs unchanged in a browser or a worker.
- Plain progress logs carry no vertex `inputs`, so the graph is empty for that format and every uncached vertex looks like its own miss root. Impact numbers are only meaningful on rawjson, and plain log timestamps are synthetic, though `DONE` durations are real.
- `--slow-step-ms` is parsed and passed to the constructor, but nothing reads `slowStepMs`. It has no effect today. `hotspotLimit`, which caps how many slow steps come back, is constructor only with no CLI flag.
- `isBroadContextCopy` is deliberately loose: it flags anything ending in a slash, containing `src`, containing a dot, or a bare token longer than three characters. False positives on unusual COPY forms are expected, so read the finding before acting on it.
- A malformed rawjson line throws `Invalid BuildKit rawjson line N`, and the CLI prints the stack to stderr with exit code 1. It never exits non zero on findings, so a quality gate has to read the JSON and decide for itself. Context byte totals also depend on BuildKit emitting parseable status lines; if the runner suppresses them, `contextBytes` is zero and that recommendation stays silent rather than guessing.
- It analyses traces. It does not run builds, patch Dockerfiles, talk to a registry or inspect the local build cache on disk.
