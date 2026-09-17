package main

import "core:fmt"
import "core:os"
import "core:strings"

HEX_DIGITS :: "0123456789abcdef"

DEFAULT_DEMO_INSERT :: "\n\n[EDITOR NOTE: this paragraph was inserted during the demo run to simulate a normal mid document edit that a real chunk cache should survive without rehashing everything downstream of it]\n\n"

Chunk :: struct {
	offset: int,
	length: int,
	hash:   u64,
}

Chunker_Config :: struct {
	avg_bits:            uint,
	normalization_level: uint,
	avg_size:            int,
	min_size:            int,
	max_size:            int,
	mask_s:              u64,
	mask_l:              u64,
}

gear: [256]u64

splitmix64_next :: proc(state: ^u64) -> u64 {
	state^ += 0x9E3779B97F4A7C15
	z := state^
	z = (z ~ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ~ (z >> 27)) * 0x94D049BB133111EB
	z = z ~ (z >> 31)
	return z
}

init_gear :: proc() {
	state: u64 = 0x2545F4914F6CDD1D
	for i in 0 ..< 256 {
		gear[i] = splitmix64_next(&state)
	}
}

fnv1a64 :: proc(data: []byte) -> u64 {
	h: u64 = 0xcbf29ce484222325
	for b in data {
		h = h ~ u64(b)
		h = h * 0x100000001b3
	}
	return h
}

write_hex64 :: proc(buf: ^[16]byte, h: u64) {
	for i in 0 ..< 16 {
		shift := uint(60 - i * 4)
		nibble := (h >> shift) & 0xF
		buf[i] = HEX_DIGITS[int(nibble)]
	}
}

hex_to_u64 :: proc(s: string) -> (u64, bool) {
	if len(s) != 16 {
		return 0, false
	}
	v: u64 = 0
	for i in 0 ..< 16 {
		c := s[i]
		d: u64
		switch {
		case c >= '0' && c <= '9':
			d = u64(c - '0')
		case c >= 'a' && c <= 'f':
			d = u64(c - 'a') + 10
		case:
			return 0, false
		}
		v = (v << 4) | d
	}
	return v, true
}

default_config :: proc() -> Chunker_Config {
	return Chunker_Config{avg_bits = 10, normalization_level = 2}
}

finalize_config :: proc(cfg: ^Chunker_Config) -> bool {
	if cfg.avg_bits < 6 || cfg.avg_bits > 24 {
		fmt.eprintln("error: --avg-bits must be between 6 and 24")
		return false
	}
	if cfg.normalization_level < 1 || cfg.normalization_level >= cfg.avg_bits {
		fmt.eprintln("error: --norm must be between 1 and avg-bits minus 1")
		return false
	}
	cfg.avg_size = 1 << cfg.avg_bits
	cfg.min_size = cfg.avg_size / 4
	cfg.max_size = cfg.avg_size * 4
	cfg.mask_s = (u64(1) << (cfg.avg_bits + cfg.normalization_level)) - 1
	cfg.mask_l = (u64(1) << (cfg.avg_bits - cfg.normalization_level)) - 1
	return true
}

cut :: proc(data: []byte, cfg: Chunker_Config) -> int {
	n := len(data)
	if n <= cfg.min_size {
		return n
	}
	max_len := min(n, cfg.max_size)
	normal_len := min(n, cfg.avg_size)

	fp: u64 = 0
	i := cfg.min_size
	for i < normal_len {
		fp = (fp << 1) + gear[int(data[i])]
		if fp & cfg.mask_s == 0 {
			return i + 1
		}
		i += 1
	}
	for i < max_len {
		fp = (fp << 1) + gear[int(data[i])]
		if fp & cfg.mask_l == 0 {
			return i + 1
		}
		i += 1
	}
	return max_len
}

chunk_buffer :: proc(data: []byte, cfg: Chunker_Config) -> [dynamic]Chunk {
	chunks := make([dynamic]Chunk, 0)
	offset := 0
	for offset < len(data) {
		remaining := data[offset:]
		length := cut(remaining, cfg)
		h := fnv1a64(remaining[:length])
		append(&chunks, Chunk{offset = offset, length = length, hash = h})
		offset += length
	}
	return chunks
}

chunk_fixed :: proc(data: []byte, block_size: int) -> [dynamic]Chunk {
	chunks := make([dynamic]Chunk, 0)
	offset := 0
	for offset < len(data) {
		length := min(block_size, len(data) - offset)
		h := fnv1a64(data[offset:offset + length])
		append(&chunks, Chunk{offset = offset, length = length, hash = h})
		offset += length
	}
	return chunks
}

parse_uint_at :: proc(s: string, start: int) -> (val: int, next: int) {
	i := start
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		val = val * 10 + int(s[i] - '0')
		i += 1
	}
	return val, i
}

parse_int :: proc(s: string) -> (int, bool) {
	if len(s) == 0 {
		return 0, false
	}
	v := 0
	for ch in s {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		v = v * 10 + int(ch - '0')
	}
	return v, true
}

parse_manifest_line :: proc(line: string) -> (Chunk, bool) {
	off_key := "\"offset\":"
	len_key := "\"length\":"
	hash_key := "\"hash\":\""

	oi := strings.index(line, off_key)
	li := strings.index(line, len_key)
	hi := strings.index(line, hash_key)
	if oi < 0 || li < 0 || hi < 0 {
		return Chunk{}, false
	}

	offset, _ := parse_uint_at(line, oi + len(off_key))
	length, _ := parse_uint_at(line, li + len(len_key))

	hash_start := hi + len(hash_key)
	if hash_start + 16 > len(line) {
		return Chunk{}, false
	}
	hash, ok := hex_to_u64(line[hash_start:hash_start + 16])
	if !ok {
		return Chunk{}, false
	}

	return Chunk{offset = offset, length = length, hash = hash}, true
}

load_manifest :: proc(path: string) -> ([dynamic]Chunk, bool) {
	data, ok := os.read_entire_file(path)
	if !ok {
		return nil, false
	}
	defer delete(data)

	lines := strings.split(string(data), "\n")
	defer delete(lines)

	chunks := make([dynamic]Chunk, 0)
	for line in lines {
		trimmed := strings.trim_space(line)
		if len(trimmed) == 0 {
			continue
		}
		c, ok2 := parse_manifest_line(trimmed)
		if !ok2 {
			fmt.eprintf("error: malformed manifest line in %s: %s\n", path, trimmed)
			delete(chunks)
			return nil, false
		}
		append(&chunks, c)
	}
	return chunks, true
}

report_diff :: proc(a: [dynamic]Chunk, b: [dynamic]Chunk) {
	counts := make(map[u64]int)
	defer delete(counts)

	total_a := 0
	for c in a {
		counts[c.hash] = counts[c.hash] + 1
		total_a += c.length
	}

	total_b := 0
	unchanged_bytes := 0
	unchanged_count := 0
	for c in b {
		total_b += c.length
		cnt := counts[c.hash]
		if cnt > 0 {
			unchanged_bytes += c.length
			unchanged_count += 1
			counts[c.hash] = cnt - 1
		}
	}

	added_count := len(b) - unchanged_count
	removed_count := len(a) - unchanged_count
	reuse_pct := 0.0
	if total_b > 0 {
		reuse_pct = f64(unchanged_bytes) / f64(total_b) * 100.0
	}

	fmt.printf("manifest a: %d chunks, %d bytes\n", len(a), total_a)
	fmt.printf("manifest b: %d chunks, %d bytes\n", len(b), total_b)
	fmt.printf("unchanged:  %d chunks, %d bytes\n", unchanged_count, unchanged_bytes)
	fmt.printf("added:      %d chunks\n", added_count)
	fmt.printf("removed:    %d chunks\n", removed_count)
	fmt.printf("reuse:      %.2f%%\n", reuse_pct)
}

read_stdin :: proc() -> []byte {
	buf := make([dynamic]byte, 0)
	block: [65536]byte
	for {
		n, _ := os.read(os.stdin, block[:])
		if n <= 0 {
			break
		}
		append(&buf, ..block[:n])
	}
	return buf[:]
}

print_usage :: proc() {
	fmt.println("content defined chunker: stable-boundary chunking for RAG context and prompt cache reuse")
	fmt.println()
	fmt.println("usage:")
	fmt.println("  content_defined_chunker chunk [--avg-bits N] [--norm N] <file|->")
	fmt.println("  content_defined_chunker diff <manifest_a.jsonl> <manifest_b.jsonl>")
	fmt.println("  content_defined_chunker demo <file>")
	fmt.println()
	fmt.println("chunk writes one JSON object per line: {\"offset\":N,\"length\":N,\"hash\":\"...\"}")
	fmt.println("diff compares two manifests and reports byte-level chunk reuse")
	fmt.println("demo edits a copy of <file> and compares this tool against naive fixed-size chunking")
}

cmd_chunk :: proc(args: []string) {
	cfg := default_config()
	path := ""

	i := 0
	for i < len(args) {
		switch args[i] {
		case "--avg-bits":
			i += 1
			if i >= len(args) {
				fmt.eprintln("error: --avg-bits requires a value")
				os.exit(1)
			}
			v, ok := parse_int(args[i])
			if !ok {
				fmt.eprintln("error: invalid --avg-bits value")
				os.exit(1)
			}
			cfg.avg_bits = uint(v)
		case "--norm":
			i += 1
			if i >= len(args) {
				fmt.eprintln("error: --norm requires a value")
				os.exit(1)
			}
			v, ok := parse_int(args[i])
			if !ok {
				fmt.eprintln("error: invalid --norm value")
				os.exit(1)
			}
			cfg.normalization_level = uint(v)
		case:
			path = args[i]
		}
		i += 1
	}

	if !finalize_config(&cfg) {
		os.exit(1)
	}

	data: []byte
	if path == "" || path == "-" {
		data = read_stdin()
	} else {
		ok: bool
		data, ok = os.read_entire_file(path)
		if !ok {
			fmt.eprintf("error: could not read file '%s'\n", path)
			os.exit(1)
		}
	}
	defer delete(data)

	chunks := chunk_buffer(data, cfg)
	defer delete(chunks)

	for c in chunks {
		hexbuf: [16]byte
		write_hex64(&hexbuf, c.hash)
		fmt.printf("{\"offset\":%d,\"length\":%d,\"hash\":\"%s\"}\n", c.offset, c.length, string(hexbuf[:]))
	}
}

cmd_diff :: proc(args: []string) {
	if len(args) < 2 {
		fmt.eprintln("usage: content_defined_chunker diff <manifest_a> <manifest_b>")
		os.exit(1)
	}

	chunks_a, ok_a := load_manifest(args[0])
	if !ok_a {
		fmt.eprintf("error: could not read manifest '%s'\n", args[0])
		os.exit(1)
	}
	defer delete(chunks_a)

	chunks_b, ok_b := load_manifest(args[1])
	if !ok_b {
		fmt.eprintf("error: could not read manifest '%s'\n", args[1])
		os.exit(1)
	}
	defer delete(chunks_b)

	report_diff(chunks_a, chunks_b)
}

cmd_demo :: proc(args: []string) {
	if len(args) < 1 {
		fmt.eprintln("usage: content_defined_chunker demo <file>")
		os.exit(1)
	}

	data, ok := os.read_entire_file(args[0])
	if !ok {
		fmt.eprintf("error: could not read file '%s'\n", args[0])
		os.exit(1)
	}
	defer delete(data)

	insert_text := DEFAULT_DEMO_INSERT
	insert := transmute([]byte)insert_text
	pos := len(data) / 2

	mutated := make([dynamic]byte, 0, len(data) + len(insert))
	append(&mutated, ..data[:pos])
	append(&mutated, ..insert)
	append(&mutated, ..data[pos:])
	defer delete(mutated)

	cfg := default_config()
	if !finalize_config(&cfg) {
		os.exit(1)
	}

	cdc_a := chunk_buffer(data, cfg)
	defer delete(cdc_a)
	cdc_b := chunk_buffer(mutated[:], cfg)
	defer delete(cdc_b)

	fixed_a := chunk_fixed(data, cfg.avg_size)
	defer delete(fixed_a)
	fixed_b := chunk_fixed(mutated[:], cfg.avg_size)
	defer delete(fixed_b)

	fmt.println("=== content-defined chunking (this tool) ===")
	report_diff(cdc_a, cdc_b)
	fmt.println()
	fmt.println("=== naive fixed-size chunking (baseline) ===")
	report_diff(fixed_a, fixed_b)
}

main :: proc() {
	init_gear()

	args := os.args
	if len(args) < 2 {
		print_usage()
		os.exit(1)
	}

	switch args[1] {
	case "chunk":
		cmd_chunk(args[2:])
	case "diff":
		cmd_diff(args[2:])
	case "demo":
		cmd_demo(args[2:])
	case "-h", "--help", "help":
		print_usage()
	case:
		fmt.eprintf("unknown command: %s\n\n", args[1])
		print_usage()
		os.exit(1)
	}
}
