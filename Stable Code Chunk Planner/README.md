# Stable Code Chunk Planner

Chunking a repository for embeddings is easy. Chunking it so the boundaries do not move every time someone edits a file is the hard part. This is a single file Java CLI that walks a repo, splits code at declaration and block boundaries and emits an NDJSON chunk manifest with deterministic content hashed IDs, so a re-index only touches what actually changed.

**Language:** Java | **Lines:** 929 | **Added:** 2026-04-22

## What this solves

The failure mode is unstable chunking. Naive chunkers cut on a fixed byte or token window from the top of the file. Add three import lines and every window below shifts. The chunk text changes, the hash changes, the vector store sees a thousand new chunks instead of two and the embedding job re-embeds the entire repository. On a mid sized monorepo that is real money per commit, plus a full index rebuild, plus an index that is briefly inconsistent while the write batch lands. Nobody notices until the bill arrives or someone asks why code search returned yesterday's version of a function.

The second failure mode is garbage in the index. A chunker that ignores `.gitignore` will happily embed `node_modules`, `dist`, `target`, minified bundles, lockfiles and PNGs decoded as mojibake, which pollutes the retrieval space and burns the token budget downstream. Then there is the pathological long line: one minified JS file or a 400 KB base64 blob on a single line, and a line based splitter either emits a chunk far over the model context limit or crashes.

The third is the boundary itself. Cut mid function and the retrieved chunk is half a function with no signature: the embedding is meaningless and retrieval quality drops in a way that is hard to attribute back to the chunker. This tool addresses all three. It respects repo ignore rules, skips binaries and oversized files, soft splits long lines at safe characters and scores candidate cut points so it prefers to end just before a declaration, an import block, a comment header or a blank line. Every chunk carries a SHA-256 derived ID, and with the previous manifest passed in it marks each chunk `changed: true` or `false` so the indexer can skip the unchanged ones.

## Why I built it

Every chunker I found was either a fifty line script looping over lines, or a component bolted into a framework that dragged the framework in with it. The framework ones were tied to a specific vector store or embedding client, and none treated `.gitignore` and binary detection as table stakes. I wanted something dependency free I could drop into a CI step, point at a repo and diff two manifests to see which chunks moved.

The chunk plan is also worth separating from the indexing. A manifest is inspectable. Commit it, diff it, count changed chunks in a PR check and decide whether an index refresh is warranted at all, before spending a single embedding call.

## When to use it

- Your embedding refresh runs on every merge to main and cost scales with commit size instead of change size.
- You are building code search or agent memory and want the retrieved unit to start at a function or class boundary, not mid statement.
- You want a reproducible artifact to diff in CI: plan the base commit and the head commit, compare changed chunk counts.
- Your index keeps ingesting `node_modules`, build output or binary assets because the ingestion script has no ignore handling.
- You feed a repo into an eval harness and need the same chunk set every run, on every machine.
- You have minified or generated lines thousands of bytes long that break line splitters.

## How it works

`main` delegates to `run`, which parses argv into an immutable `Config`, loads `IgnoreRules`, reads the previous chunk ID set and walks the tree. `collectFiles` uses a `SimpleFileVisitor` that prunes hidden directories unless `--include-hidden`, prunes `DEFAULT_SKIPPED_DIRECTORIES` (`.git`, `node_modules`, `dist`, `build`, `target`, `venv`, `__pycache__` and the rest) unless an ignore rule negation re-includes them, and applies the extension allowlist. The list is sorted by normalized relative path, which makes the run independent of filesystem enumeration order. `readTextFile` enforces `--max-file-bytes`, then detects binaries in two steps: a scan for any NUL byte, then a strict UTF-8 decode with `CodingErrorAction.REPORT` on malformed input and unmappable characters. Anything failing either check is counted as skipped binary. Deliberately conservative: a file that is not clean UTF-8 does not belong in a text embedding index.

The chunker is two passes. `segmentize` normalizes newlines and turns the file into `Segment` objects, one per line, each carrying text, line number and UTF-8 byte size. A line over the soft cap (`maxBytes - 512`, floored at 256) is broken by `chooseSoftSplit`, which walks forward to the byte limit then backtracks to the nearest whitespace, comma, semicolon, closing brace, paren or bracket. Fragments keep the original line number and record `fragmentIndex` and `fragmentCount`, so the scorer knows when it is looking at the middle of a split line.

`planChunks` then runs a greedy scan with lookahead. From the cursor it accumulates segments, and once the running byte total crosses `targetBytes` it calls `boundaryScore` on each candidate end point, continuing until it hits `maxBytes`. The scorer weighs the tail of the candidate chunk against the head of the next one: ending on a complete line is +14 and ending mid fragment costs 40, a blank tail is +20, a tail closing on `}` or `};` or `end` is +18, and a next segment matching `DECLARATION_PATTERN` is +34, `IMPORT_PATTERN` +24, `COMMENT_HEADER_PATTERN` +20, `ANNOTATION_PATTERN` +10, column zero indentation +10. Chunks under four lines lose 18, going over `maxBytes` loses 50. Highest score wins, so the cut lands just before the next class, function or import block rather than at an arbitrary byte offset. `DECLARATION_PATTERN` covers roughly seventeen declaration keywords across Java, Kotlin, Rust, Go, Python, TypeScript and friends, with optional modifier prefixes.

Identity comes from `shortHash`: SHA-256 over `path \0 lineStart \0 lineEnd \0 chunkText`, truncated to 24 hex characters. `loadPreviousChunkIds` pulls IDs from an existing manifest with `CHUNK_ID_PATTERN`, a line by line regex that needs no JSON parser, and membership in that set drives the `changed` flag. `writeManifest` emits a `recordType: "manifest"` header carrying tool name, version, timestamp, root and size settings, then one `recordType: "chunk"` line per chunk with path, language, ordinal, line range, byte size, changed flag and escaped content. `emitSummary` writes its counters to stderr so stdout stays clean NDJSON for piping.

## Usage

```bash
javac StableCodeChunkPlanner.java   # or: java StableCodeChunkPlanner.java --help

# Plan chunks for the current directory, NDJSON to stdout, summary to stderr
java StableCodeChunkPlanner

# Incremental run: mark chunks unchanged since the last manifest
java StableCodeChunkPlanner \
  --root /path/to/repo \
  --previous build/chunks.prev.ndjson \
  --output build/chunks.ndjson

# Tune sizes, restrict to specific languages, stream to stdout
java StableCodeChunkPlanner --root . \
  --target-bytes 2400 --max-bytes 3600 \
  --extensions java,kt,py,ts,tsx --output -

# Include dotfiles and dot directories, suppress the stderr summary
java StableCodeChunkPlanner --include-hidden --no-summary --output chunks.ndjson

# Count how many chunks actually changed
java StableCodeChunkPlanner --previous prev.ndjson --output - \
  | grep -c '"changed":true'

# Full flag list: --root --output --previous --target-bytes --max-bytes
# --max-file-bytes --include-hidden --extensions --no-summary --help
java StableCodeChunkPlanner --help
```

## Notes

- Boundary detection is regex driven, not AST driven. No parser, no language server, so it will occasionally cut inside a long method that offers no scoring signal. That is the price of covering any language with zero dependencies.
- The chunk ID hashes the line range along with the content, so inserting lines above a chunk changes its ID even when the chunk text is byte identical. Stable across machines and runs and stable under edits elsewhere in the repo, not fully position independent. If you need that, hash the content alone.
- Ignore handling is a useful subset of gitignore, not a full implementation. It reads `.gitignore` and `.chunkignore` from the repo root only, never nested ones. Comments, negation with `!`, leading `/` anchoring, trailing `/` directory rules and basename patterns all work, mapped onto `PathMatcher` globs. Character class and `**` edge cases will not match git exactly.
- Files are read fully into memory and the whole chunk list is held before the manifest is written. `--max-file-bytes` is the guard. On a very large repo, watch heap. Single threaded by design: parallelism makes output ordering nondeterministic, and determinism is the point. Symlinks are not followed and unreadable files are silently skipped.
- Exit codes: 0 on success, 2 on a usage error such as an unknown flag or `--target-bytes` above `--max-bytes`, 1 on any other failure with a stack trace on stderr. Java 17 or newer, no external dependencies.
- It plans chunks and nothing else. No embeddings, no vector store writes, no tokenizer. Sizes are UTF-8 bytes, not model tokens, so calibrate `--target-bytes` against your own tokenizer.
