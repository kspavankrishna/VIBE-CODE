# Content Defined Chunker

A chunker that splits text or byte streams at boundaries chosen by the data itself, not by a fixed byte count, so a small edit near the start of a document only changes the one or two chunks touching that edit instead of shifting every chunk hash after it.

**Language:** Odin | **Lines:** 452 | **Added:** 2026-09-17

## What this solves

If you build anything that chunks documents for retrieval augmented generation, or anything that tries to cache and reuse pieces of a prompt, you have probably hit this problem without naming it. You split a document into fixed size blocks, say 1024 bytes each. You hash each block so you can tell later which blocks changed. That works fine until someone edits a sentence near the top of the file. Now every byte after that sentence shifts by however many characters were added or removed. Every fixed size block boundary after the edit lands in a different place than before. Every hash after the edit is now different, even though almost none of the actual content changed. Your dedup layer, your embedding cache, your prompt cache all think the whole back half of the document is new.

This is the classic "shifting boundary" problem. It is the same problem backup tools like restic, borg and rsync solved years ago for file deduplication. Their answer is content defined chunking: instead of cutting every N bytes, you compute a rolling hash as you scan the bytes and you cut at positions where that rolling hash satisfies some condition, like its low bits being all zero. Because the cut decision depends only on a small local window of bytes, an insertion or deletion only disturbs the chunk boundaries near the edit. Everything before the edit and everything a chunk or two after it lands on exactly the same cut points as before, so the hashes match and the old chunks get reused.

`ContentDefinedChunker.odin` is a self contained implementation of this idea, using the FastCDC normalized chunking approach (gear hash rolling fingerprint with two probability masks that bias cut points toward the target average size). It ships as a small command line tool with three real jobs: cut a file or a stdin stream into a manifest of chunks, diff two manifests to measure how many bytes were actually reused and run a side by side demo against naive fixed size chunking on your own file so you can see the difference with real numbers instead of taking my word for it.

## Why I built it

I kept seeing the same shortcut in RAG pipelines and prompt caching setups: chunk by token count or by byte count, hash each chunk, cache by hash. It is easy to write and it looks fine in a demo because demo documents do not change. Real documents change constantly. A knowledge base article gets a typo fixed. A retrieved web page gets re-scraped with one paragraph reworded. A long running agent transcript gets a new turn appended in the middle because of how the context was reassembled. Every one of these ordinary events breaks fixed size chunking's hash stability. Every one of these ordinary events is exactly the case content defined chunking was built for.

I also wanted something that did not pull in a dependency tree to do it. A gear hash chunker is maybe two hundred lines of real logic. Odin is a good fit for this kind of tool: no garbage collector pauses to worry about while you are scanning gigabytes of text one byte at a time, explicit memory management so you can see exactly what gets allocated and freed, plus a standard library small enough that you are not fighting an opaque framework to read one byte at a time from a file or from stdin.

## When to use it

Reach for this when you are building the layer underneath an LLM pipeline that assembles context from documents that change: a RAG system where source documents get updated, a semantic cache that wants to reuse embeddings or LLM output for chunks it has already seen, or your own prefix cache management on top of a provider that only caches exact byte prefixes. If your context changes shape between calls (documents reordered, one paragraph edited, a new source added in the middle) fixed offset caching gives you close to zero reuse. Chunk defined boundaries give you reuse proportional to how much content actually changed, which is usually the number you actually want your cache hit rate to track.

It is also useful as a plain deduplication primitive outside the LLM world: diffing two versions of a large text corpus, building an incremental index, or just understanding how much two versions of a file actually differ at the byte level rather than the line level. The `demo` command exists specifically so you do not have to trust the pitch. Point it at one of your own files and it will show you the reuse percentage this tool gets against the reuse percentage a naive fixed block chunker gets on the exact same edit.

## How it works

The core cut decision lives in `cut`. It scans forward through a byte slice maintaining a rolling gear hash fingerprint: `fp = (fp << 1) + gear[data[i]]` for each byte, where `gear` is a 256 entry table of pseudo random 64 bit values. That table is not hardcoded; `init_gear` fills it deterministically at startup using `splitmix64_next`, a small splitmix64 generator seeded with a fixed constant, so the table (and therefore every cut decision) is identical on every machine and every run.

`cut` uses two regions with two different masks, which is the "normalized" part of FastCDC normalized chunking. Below the target average size (`cfg.avg_size`, computed as `1 << avg_bits`) it checks the fingerprint against `cfg.mask_s`, a stricter mask with more bits set, which makes an early cut less likely and pushes chunks away from being too small. Past the average size it switches to `cfg.mask_l`, a looser mask with fewer bits set, which makes a cut more likely and pushes chunks toward landing near the target instead of running out all the way to `cfg.max_size`. Both masks are derived in `finalize_config` from `avg_bits` and `normalization_level`: `mask_s` uses `avg_bits + normalization_level` bits, `mask_l` uses `avg_bits - normalization_level` bits. `min_size` and `max_size` fall out automatically as a quarter and four times the average size, which are the usual FastCDC defaults. `finalize_config` also rejects out of range settings, like a normalization level that would leave zero bits for the mask, instead of silently producing a chunker that never cuts.

`chunk_buffer` calls `cut` repeatedly, advancing an offset through the input and hashing each resulting slice with `fnv1a64`, a plain 64 bit FNV-1a hash used purely as a fast content fingerprint for spotting identical chunks, not as a security hash. Each result becomes a `Chunk`, a small struct of `offset`, `length` and `hash`. `chunk_fixed` is the naive baseline: it slices the input into flat `block_size` pieces with no rolling hash at all. It exists only so the `demo` command has something honest to compare against.

The `chunk` subcommand (`cmd_chunk`) reads a file or, if you pass `-` or nothing, reads stdin in full through `read_stdin`, runs `chunk_buffer` and prints one JSON object per chunk to stdout in the form `{"offset":N,"length":N,"hash":"..."}`, with the hash written as sixteen hex characters by `write_hex64`. This manifest format is intentionally tiny and line oriented so it is easy to store, diff or pipe into other tools.

The `diff` subcommand (`cmd_diff`) loads two manifests with `load_manifest`, which parses each JSONL line back into a `Chunk` with `parse_manifest_line` (a small hand written parser that looks for the `"offset":`, `"length":` and `"hash":"` keys rather than a general JSON parser, since the manifest format is fixed and self produced) and converts the hex hash back to a `u64` with `hex_to_u64`. `report_diff` then builds a multiset of hashes from manifest A and walks manifest B counting how many bytes in B match an unused hash in A, printing unchanged, added and removed chunk counts plus a reuse percentage.

The `demo` subcommand (`cmd_demo`) takes one file, builds a mutated copy with a fixed editorial note spliced into the middle, chunks both the original and the mutated copy with both `chunk_buffer` and `chunk_fixed` using the same target size, then prints `report_diff` for each pair side by side so you can see the reuse percentage this chunker gets against the reuse percentage naive fixed size chunking gets on your exact file.

## Usage

Build it as a single file program:

```
odin build ContentDefinedChunker.odin -file -out:content_defined_chunker
```

Then run one of the three commands:

```
./content_defined_chunker chunk mydocument.txt > manifest.jsonl
cat mydocument.txt | ./content_defined_chunker chunk - > manifest.jsonl
./content_defined_chunker chunk --avg-bits 12 --norm 2 mydocument.txt > manifest.jsonl
./content_defined_chunker diff manifest_old.jsonl manifest_new.jsonl
./content_defined_chunker demo mydocument.txt
```

`--avg-bits` sets the target chunk size as a power of two in bytes; the default is 10, meaning an average chunk around 1024 bytes, with `min_size` and `max_size` following automatically at a quarter and four times that. `--norm` sets the normalization level used to split `mask_s` and `mask_l`; the default is 2, which matches the usual FastCDC recommendation. Running with no arguments, or with `-h`, `--help` or `help`, prints the same usage text baked into `print_usage`.

## Notes

The hashes here are for identifying identical chunks quickly, not for anything adversarial. `fnv1a64` is not collision resistant against someone deliberately constructing colliding input, so do not use this manifest format as a security boundary or as proof of integrity against a hostile party. For plain deduplication and cache key generation between your own pipeline stages it is more than fine. It also keeps the tool free of any dependency beyond Odin's own `core:fmt`, `core:os` and `core:strings`.

The gear table is generated the same way on every run because `init_gear` seeds `splitmix64_next` with a fixed constant rather than a random one. That is deliberate: two different machines running this tool need to produce the same cut points for the same bytes, or the whole point of comparing manifests across machines falls apart. If you ever need a different chunking "shape" for a different dataset, change `avg_bits` and `normalization_level`, not the seed.

One thing worth knowing if you are chunking very small inputs: `cut` returns the whole remaining slice as one chunk whenever what is left is at or under `min_size`, so a short file, or the last few bytes of any file, always ends up as a single tail chunk instead of forcing a cut that would produce a tiny, one or two byte fragment.
