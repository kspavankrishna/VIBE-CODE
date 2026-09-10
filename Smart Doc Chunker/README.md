# Smart Doc Chunker

Splitting a folder of documents into token bounded chunks before you push them into a vector database, without writing another one off Python script. One Bash file, no dependencies, and a `manifest.json` with per chunk token and byte counts.

**Language:** Bash | **Lines:** 91 | **Added:** 2026-04-05

## What this solves

This solves the friction of prepping document collections for RAG pipelines. Every time you set up semantic search you hit the same wall: how do you chunk without blowing your token budget or breaking context at the wrong boundary? Most people answer it by pasting a snippet into a notebook, running it once, then losing it. Three weeks later the corpus grows, you write the same thing again slightly differently, and now your old and new chunks came from two different rules.

The failure mode in production is quiet and expensive. You embed a chunk bigger than you thought, the endpoint rejects it and your ingest job dies halfway through a 4,000 file run. Or worse, it does not die: the provider silently truncates at the model's input limit, you get an embedding for the first two thirds of the chunk, and the tail of that document is now unsearchable. Nobody notices until a user asks a question whose answer lived in the truncated part. There is no error in the logs. The index just has a hole in it.

The other cost is retrieval quality. Chunk by fixed byte count and you cut sentences and code blocks in half at arbitrary offsets, and the embedding for a half sentence is noise. Line boundaries keep each chunk syntactically whole for line oriented content: markdown, logs, notes, transcripts, config dumps. Far better than a blind `split -b`.

Then there is accounting. Feeding retrieved chunks into a prompt means knowing each one's token cost before you assemble the context window. The manifest exists so the retrieval layer can budget without reopening every file.

## Why I built it

The chunkers inside RAG frameworks are fine, but they arrive attached to a framework. To split 200 markdown files you install a package tree, pin a version, deal with a virtualenv and accept whatever the library's default splitter does this month. Inside a Docker layer that is a lot of surface area for one shell step. The plain Unix tools, meanwhile, do not know what a token is: `split` counts bytes, `csplit` counts patterns, neither will tell you a chunk costs 480 tokens.

I wanted the smallest thing that sits at the front of an ingest pipeline, runs anywhere Bash runs, and hands the next stage a machine readable index of what it produced. Drop it into your RAG setup where you are preparing knowledge bases for Claude or your vector store, and let the heavier tooling start after the corpus is sliced and counted.

## When to use it

- Seeding a vector database from a folder of markdown docs, release notes or meeting transcripts and every chunk must stay under the embedding model's input limit.
- Your ingest job keeps failing partway through and you suspect one oversized document.
- A retrieval layer needs per chunk token counts to assemble prompts against a fixed context budget.
- Chunking on a CI runner or inside a container where installing a Python stack for one step is not worth it.
- A pile of `.log` files from an incident that you want sliced into model sized pieces for analysis.
- You want the chunking rule pinned in a file you can read in one sitting, not buried in a library default.

## How it works

The script takes three positional arguments with defaults baked in: `INPUT_DIR` (default `.`), `OUTPUT_DIR` (default `./chunks`) and `MAX_TOKENS` (default `2000`). It runs under `set -euo pipefail`, creates the output directory, and writes the manifest to `$OUTPUT_DIR/manifest.json`.

Token counting is `estimate_tokens`, deliberately the crudest useful heuristic: character length divided by four. That ratio is the usual rule of thumb for English text on byte pair encoders. It is an estimate, not a tokenizer, and it costs one shell arithmetic expansion instead of a Python process per line. For prose it lands close. For dense code, minified JSON or non Latin scripts it underestimates, which is called out in the notes.

`chunk_file` is the core. It strips the extension with `sed 's/\.[^.]*$//'` for a base name, then reads the file with `while IFS= read -r line || [[ -n "$line" ]]`. The `|| [[ -n "$line" ]]` half catches a final line with no trailing newline, a case a plain `read` loop silently drops. Each line goes through a greedy accumulate and flush rule: if `current_tokens + line_tokens` would exceed `MAX_TOKENS` and the buffer is not empty, it writes the buffer to `${base}_chunk_${N}.txt`, increments the chunk id and starts a new buffer with the current line. Otherwise it appends the line and adds its tokens. After the loop it flushes what is left. That is a single pass first fit greedy packer over line boundaries: O(n) in lines, constant state, no lookahead and no backtracking. A line is the atomic unit, so every boundary is a real line boundary.

`build_manifest` is a separate pass over the output directory rather than a byproduct of chunking, which is intentional given the shell it runs in: the `find | while read` pipeline puts the loop body in a subshell, so any array the chunking function built would not survive back to the parent. The manifest instead globs `$OUTPUT_DIR/*.txt` and, per file, recomputes tokens from the actual contents with `estimate_tokens "$(cat "$chunk")"` and reads byte size with `wc -c`, emitting JSON by hand with a `first` flag to place commas correctly. The numbers describe the chunk files as they exist on disk, which is what a downstream reader wants.

File selection is `find "$INPUT_DIR" -maxdepth 1 -type f` restricted to `*.txt`, `*.md` and `*.log`. Flat directory, three extensions, everything else ignored.

## Usage

```bash
# defaults: current directory in, ./chunks out, 2000 token budget
bash SmartDocChunker.sh

# explicit: input dir, output dir, max tokens per chunk
bash SmartDocChunker.sh ./docs ./chunks 1500

# tight budget for a small embedding model
bash SmartDocChunker.sh ~/notes /tmp/out 512
```

Output:

```
chunks/
  mydoc_chunk_0.txt
  mydoc_chunk_1.txt
  manifest.json
```

```json
{
  "chunks": [
    {"path": "mydoc_chunk_0.txt", "tokens": 204, "bytes": 819},
    {"path": "mydoc_chunk_1.txt", "tokens": 189, "bytes": 759}
  ]
}
```

## Notes

- The trailing `====` separator lines under the closing comment block are not comments, so the shell tries to execute them. Chunking and the manifest finish normally first, but the script exits **127**. Do not gate a pipeline on `$?` until those two lines are commented out or deleted.
- `estimate_tokens` is chars divided by 4, a heuristic and not a real tokenizer. Set `MAX_TOKENS` with headroom, roughly 80 percent of the hard model limit, especially for code, JSON or non English text where the real count runs higher.
- A single line longer than the budget is never split. A 20,000 character one line file with a budget of 100 gives you one 20,000 character chunk. Minified JSON and single line CSV dumps defeat it.
- Every chunk starts with a leading blank line, an artifact of appending `\n` before the first buffered line. Harmless for embeddings, worth knowing if you diff or hash chunks.
- Manifest entries follow the shell glob, which is lexicographic, so `chunk_10` sorts before `chunk_2`. Sort numerically yourself if reading order matters.
- The manifest globs every `*.txt` in the output directory, not only this run's output. Use a fresh directory or stale chunks get listed as current.
- Filenames are interpolated into JSON with no escaping. A document name containing a quote or backslash produces a broken manifest.
- Non recursive by design: `-maxdepth 1`, and only `.txt`, `.md` and `.log`. No PDF, DOCX or HTML extraction.
- No overlap between chunks. If retrieval quality depends on sliding window overlap, this is not the splitter you want.
- Requires Bash, `find`, `sed`, `wc` and `basename`. Verified on Bash 3.2 on macOS, so the stock system shell is enough.
