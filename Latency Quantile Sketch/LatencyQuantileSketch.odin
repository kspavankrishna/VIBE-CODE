package main

import "core:fmt"
import "core:math"
import "core:os"
import "core:slice"
import "core:strconv"
import "core:strings"

FORMAT_TAG :: "DDS1"
MIN_INDEXABLE :: 1e-9
MAX_VALUE_MS :: 1e15
MIN_ALPHA_PPM :: 100
MAX_ALPHA_PPM :: 200_000
MIN_BUCKETS :: 2
MAX_BUCKETS_LIMIT :: 1 << 20
DEFAULT_ALPHA_PPM :: 10_000
DEFAULT_MAX_BUCKETS :: 2048
MAX_GROUPS :: 100_000
MAX_LINE_BYTES :: 1 << 20
READ_CHUNK :: 65536

EXIT_OK :: 0
EXIT_SELFTEST_FAILED :: 1
EXIT_USAGE :: 2
EXIT_REJECTED_ROWS :: 3

Sketch :: struct {
	alpha_ppm:   int,
	gamma:       f64,
	multiplier:  f64,
	max_buckets: int,
	buckets:     map[i32]u64,
	zero_count:  u64,
	count:       u64,
	sum:         f64,
	min:         f64,
	max:         f64,
}

Merge_Error :: enum {
	None,
	Alpha_Mismatch,
}

Row_Error :: enum {
	None,
	Too_Few_Columns,
	Bad_Number,
	Non_Finite,
	Negative,
	Too_Large,
	Group_Limit,
	Bad_Sketch,
	Merge_Mismatch,
}

Command :: enum {
	Summarize,
	Merge,
	Selftest,
}

Config :: struct {
	command:     Command,
	alpha_ppm:   int,
	max_buckets: int,
	group_col:   int,
	value_col:   int,
	skip_lines:  int,
	unit_scale:  f64,
	emit:        bool,
	quantiles:   [dynamic]f64,
	labels:      [dynamic]string,
}

Line_Handler :: proc(user: rawptr, line: string, line_no: int)

Ingest_State :: struct {
	cfg:          ^Config,
	groups:       map[string]^Sketch,
	accepted:     int,
	rejected:     int,
	reasons:      [Row_Error]int,
	first_line:   int,
	first_reason: Row_Error,
}

sketch_init :: proc(s: ^Sketch, alpha_ppm, max_buckets: int) {
	alpha := f64(alpha_ppm) / 1e6
	s.alpha_ppm = alpha_ppm
	s.gamma = (1.0 + alpha) / (1.0 - alpha)
	s.multiplier = 1.0 / math.ln(s.gamma)
	s.max_buckets = max_buckets
	s.buckets = make(map[i32]u64)
}

sketch_destroy :: proc(s: ^Sketch) {
	delete(s.buckets)
	s.buckets = nil
}

bucket_value :: proc(s: ^Sketch, key: i32) -> f64 {
	return 2.0 * math.pow(s.gamma, f64(key)) / (s.gamma + 1.0)
}

sketch_saturated :: proc(s: ^Sketch) -> bool {
	return len(s.buckets) >= s.max_buckets
}

sketch_collapse :: proc(s: ^Sketch) {
	for len(s.buckets) > s.max_buckets {
		lowest, second: i32
		have_lowest, have_second := false, false
		for k in s.buckets {
			if !have_lowest || k < lowest {
				second, have_second = lowest, have_lowest
				lowest, have_lowest = k, true
			} else if !have_second || k < second {
				second, have_second = k, true
			}
		}
		if !have_second {
			return
		}
		moved := s.buckets[lowest]
		delete_key(&s.buckets, lowest)
		s.buckets[second] += moved
	}
}

sketch_add :: proc(s: ^Sketch, v: f64, n: u64 = 1) {
	if n == 0 {
		return
	}
	if v < MIN_INDEXABLE {
		s.zero_count += n
	} else {
		key := i32(math.ceil(math.ln(v) * s.multiplier))
		s.buckets[key] += n
		sketch_collapse(s)
	}
	if s.count == 0 {
		s.min = v
		s.max = v
	} else {
		s.min = min(s.min, v)
		s.max = max(s.max, v)
	}
	s.count += n
	s.sum += v * f64(n)
}

sorted_keys :: proc(s: ^Sketch, allocator := context.temp_allocator) -> [dynamic]i32 {
	keys := make([dynamic]i32, 0, len(s.buckets), allocator)
	for k in s.buckets {
		append(&keys, k)
	}
	slice.sort(keys[:])
	return keys
}

sketch_quantile :: proc(s: ^Sketch, q: f64) -> (value: f64, ok: bool) {
	if s.count == 0 || q < 0 || q > 1 || math.is_nan(q) {
		return 0, false
	}
	rank := q * f64(s.count - 1)
	acc := f64(s.zero_count)
	if rank < acc {
		return s.min, true
	}
	keys := sorted_keys(s)
	for k in keys {
		acc += f64(s.buckets[k])
		if acc > rank {
			return clamp(bucket_value(s, k), s.min, s.max), true
		}
	}
	return s.max, true
}

sketch_merge :: proc(dst, src: ^Sketch) -> Merge_Error {
	if dst.alpha_ppm != src.alpha_ppm {
		return .Alpha_Mismatch
	}
	if src.count == 0 {
		return .None
	}
	dst.max_buckets = min(dst.max_buckets, src.max_buckets)
	for k, c in src.buckets {
		dst.buckets[k] += c
	}
	dst.zero_count += src.zero_count
	if dst.count == 0 {
		dst.min = src.min
		dst.max = src.max
	} else {
		dst.min = min(dst.min, src.min)
		dst.max = max(dst.max, src.max)
	}
	dst.count += src.count
	dst.sum += src.sum
	sketch_collapse(dst)
	return .None
}

sketch_encode :: proc(b: ^strings.Builder, s: ^Sketch) {
	fmt.sbprintf(
		b,
		"%s %d %d %d %d %v %v %v",
		FORMAT_TAG,
		s.alpha_ppm,
		s.max_buckets,
		s.zero_count,
		s.count,
		s.sum,
		s.min,
		s.max,
	)
	keys := sorted_keys(s)
	for k in keys {
		fmt.sbprintf(b, " %d:%d", k, s.buckets[k])
	}
}

parse_int_strict :: proc(text: string) -> (value: int, ok: bool) {
	n: int
	v, good := strconv.parse_int(text, 10, &n)
	return v, good && n == len(text) && len(text) > 0
}

parse_u64_strict :: proc(text: string) -> (value: u64, ok: bool) {
	n: int
	v, good := strconv.parse_u64(text, 10, &n)
	return v, good && n == len(text) && len(text) > 0
}

parse_f64_strict :: proc(text: string) -> (value: f64, ok: bool) {
	n: int
	v, good := strconv.parse_f64(text, &n)
	return v, good && n == len(text) && len(text) > 0
}

is_finite :: proc(v: f64) -> bool {
	return !math.is_nan(v) && !math.is_inf(v)
}

sketch_decode :: proc(s: ^Sketch, line: string) -> (err: string) {
	t := strings.fields(line, context.temp_allocator)
	if len(t) < 8 || t[0] != FORMAT_TAG {
		return "missing DDS1 header or fields"
	}
	alpha_ppm, ok_alpha := parse_int_strict(t[1])
	max_buckets, ok_max := parse_int_strict(t[2])
	zero_count, ok_zero := parse_u64_strict(t[3])
	count, ok_count := parse_u64_strict(t[4])
	sum, ok_sum := parse_f64_strict(t[5])
	lo, ok_lo := parse_f64_strict(t[6])
	hi, ok_hi := parse_f64_strict(t[7])
	if !(ok_alpha && ok_max && ok_zero && ok_count && ok_sum && ok_lo && ok_hi) {
		return "unparseable header field"
	}
	if count == 0 {
		return "empty sketch"
	}
	if alpha_ppm < MIN_ALPHA_PPM || alpha_ppm > MAX_ALPHA_PPM {
		return "alpha out of range"
	}
	if max_buckets < MIN_BUCKETS || max_buckets > MAX_BUCKETS_LIMIT {
		return "bucket limit out of range"
	}
	if !is_finite(sum) || !is_finite(lo) || !is_finite(hi) || sum < 0 || lo < 0 || hi < lo {
		return "non finite or inconsistent sum, min or max"
	}
	if len(t) - 8 > max_buckets {
		return "more buckets than the declared limit"
	}
	sketch_init(s, alpha_ppm, max_buckets)
	s.zero_count = zero_count
	s.count = count
	s.sum = sum
	s.min = lo
	s.max = hi
	total := zero_count
	for tok in t[8:] {
		colon := strings.index_byte(tok, ':')
		if colon <= 0 {
			sketch_destroy(s)
			return "malformed bucket token"
		}
		key, ok_key := parse_int_strict(tok[:colon])
		c, ok_c := parse_u64_strict(tok[colon + 1:])
		if !ok_key || !ok_c || c == 0 || key < int(min(i32)) || key > int(max(i32)) {
			sketch_destroy(s)
			return "bad bucket key or count"
		}
		if i32(key) in s.buckets {
			sketch_destroy(s)
			return "duplicate bucket key"
		}
		s.buckets[i32(key)] = c
		total += c
	}
	if total != count {
		sketch_destroy(s)
		return "bucket counts do not add up to the declared count (truncated or corrupt)"
	}
	return ""
}

unit_scale :: proc(name: string) -> (scale: f64, ok: bool) {
	switch name {
	case "ns":
		return 1e-6, true
	case "us":
		return 1e-3, true
	case "ms":
		return 1.0, true
	case "s":
		return 1000.0, true
	}
	return 0, false
}

parse_value :: proc(tok: string, default_scale: f64) -> (ms: f64, e: Row_Error) {
	body := tok
	scale := default_scale
	switch {
	case strings.has_suffix(tok, "ns"):
		body = tok[:len(tok) - 2]
		scale = 1e-6
	case strings.has_suffix(tok, "us"):
		body = tok[:len(tok) - 2]
		scale = 1e-3
	case strings.has_suffix(tok, "ms"):
		body = tok[:len(tok) - 2]
		scale = 1.0
	case strings.has_suffix(tok, "s"):
		body = tok[:len(tok) - 1]
		scale = 1000.0
	}
	v, ok := parse_f64_strict(body)
	if !ok {
		return 0, .Bad_Number
	}
	if !is_finite(v) {
		return 0, .Non_Finite
	}
	if v < 0 {
		return 0, .Negative
	}
	v *= scale
	if v > MAX_VALUE_MS {
		return 0, .Too_Large
	}
	return v, .None
}

split_fields :: proc(line: string) -> []string {
	if strings.contains_rune(line, '\t') {
		return strings.split(line, "\t", context.temp_allocator)
	}
	if strings.contains_rune(line, ',') {
		return strings.split(line, ",", context.temp_allocator)
	}
	return strings.fields(line, context.temp_allocator)
}

for_each_stdin_line :: proc(user: rawptr, handler: Line_Handler) -> bool {
	chunk: [READ_CHUNK]byte
	pending: [dynamic]byte
	defer delete(pending)
	line_no := 0
	for {
		n, _ := os.read(os.stdin, chunk[:])
		if n <= 0 {
			break
		}
		append(&pending, ..chunk[:n])
		start := 0
		for i in 0 ..< len(pending) {
			if pending[i] == '\n' {
				line_no += 1
				handler(user, string(pending[start:i]), line_no)
				start = i + 1
			}
		}
		if start > 0 {
			remaining := len(pending) - start
			copy(pending[:remaining], pending[start:])
			resize(&pending, remaining)
		}
		if len(pending) > MAX_LINE_BYTES {
			fmt.eprintfln("error: a line is longer than %d bytes, input is not line oriented", MAX_LINE_BYTES)
			return false
		}
	}
	if len(pending) > 0 {
		line_no += 1
		handler(user, string(pending[:]), line_no)
	}
	return true
}

reject :: proc(st: ^Ingest_State, line_no: int, reason: Row_Error) {
	st.rejected += 1
	st.reasons[reason] += 1
	if st.first_line == 0 {
		st.first_line = line_no
		st.first_reason = reason
	}
}

group_for :: proc(st: ^Ingest_State, name: string) -> (sk: ^Sketch, ok: bool) {
	existing, found := st.groups[name]
	if found {
		return existing, true
	}
	if len(st.groups) >= MAX_GROUPS {
		return nil, false
	}
	sk = new(Sketch)
	sketch_init(sk, st.cfg.alpha_ppm, st.cfg.max_buckets)
	st.groups[strings.clone(name)] = sk
	return sk, true
}

ingest_row :: proc(user: rawptr, line: string, line_no: int) {
	st := cast(^Ingest_State)user
	defer free_all(context.temp_allocator)
	if line_no <= st.cfg.skip_lines {
		return
	}
	text := strings.trim_space(line)
	if len(text) == 0 || text[0] == '#' {
		return
	}
	cols := split_fields(text)
	need := max(st.cfg.value_col, st.cfg.group_col) + 1
	if len(cols) < need {
		reject(st, line_no, .Too_Few_Columns)
		return
	}
	v, e := parse_value(strings.trim_space(cols[st.cfg.value_col]), st.cfg.unit_scale)
	if e != .None {
		reject(st, line_no, e)
		return
	}
	name := "all"
	if st.cfg.group_col >= 0 {
		name = strings.trim_space(cols[st.cfg.group_col])
	}
	sk, ok := group_for(st, name)
	if !ok {
		reject(st, line_no, .Group_Limit)
		return
	}
	sketch_add(sk, v)
	st.accepted += 1
}

ingest_sketch :: proc(user: rawptr, line: string, line_no: int) {
	st := cast(^Ingest_State)user
	defer free_all(context.temp_allocator)
	if line_no <= st.cfg.skip_lines {
		return
	}
	text := strings.trim_space(line)
	if len(text) == 0 || text[0] == '#' {
		return
	}
	name := "all"
	payload := text
	if !strings.has_prefix(text, FORMAT_TAG) {
		tab := strings.index_byte(text, '\t')
		if tab <= 0 {
			reject(st, line_no, .Bad_Sketch)
			return
		}
		name = text[:tab]
		payload = strings.trim_space(text[tab + 1:])
	}
	incoming: Sketch
	if sketch_decode(&incoming, payload) != "" {
		reject(st, line_no, .Bad_Sketch)
		return
	}
	existing, found := st.groups[name]
	if !found {
		if len(st.groups) >= MAX_GROUPS {
			sketch_destroy(&incoming)
			reject(st, line_no, .Group_Limit)
			return
		}
		sk := new(Sketch)
		sk^ = incoming
		st.groups[strings.clone(name)] = sk
		st.accepted += 1
		return
	}
	defer sketch_destroy(&incoming)
	if sketch_merge(existing, &incoming) != .None {
		reject(st, line_no, .Merge_Mismatch)
		return
	}
	st.accepted += 1
}

sorted_group_names :: proc(groups: map[string]^Sketch) -> [dynamic]string {
	names := make([dynamic]string, 0, len(groups), context.temp_allocator)
	for name in groups {
		append(&names, name)
	}
	slice.sort(names[:])
	return names
}

print_table :: proc(groups: map[string]^Sketch, cfg: ^Config) {
	fmt.printf("%-28s %10s %12s", "group", "count", "min")
	for label in cfg.labels {
		fmt.printf(" %12s", fmt.tprintf("q%s", label))
	}
	fmt.printfln(" %12s %12s", "max", "mean")
	names := sorted_group_names(groups)
	saturated := false
	for name in names {
		sk := groups[name]
		shown := name
		if sketch_saturated(sk) {
			shown = fmt.tprintf("%s*", name)
			saturated = true
		}
		fmt.printf("%-28s %10d %12.3f", shown, sk.count, sk.min)
		for q in cfg.quantiles {
			v, _ := sketch_quantile(sk, q)
			fmt.printf(" %12.3f", v)
		}
		mean := sk.sum / f64(sk.count)
		fmt.printfln(" %12.3f %12.3f", sk.max, mean)
	}
	fmt.printfln("values in milliseconds, relative error bound per quantile: see alpha of each sketch")
	if saturated {
		fmt.printfln("* sketch reached its bucket limit, lowest buckets were merged, low quantiles lose the error bound")
	}
}

print_sketches :: proc(groups: map[string]^Sketch) {
	names := sorted_group_names(groups)
	for name in names {
		b := strings.builder_make(context.temp_allocator)
		sketch_encode(&b, groups[name])
		fmt.printfln("%s\t%s", name, strings.to_string(b))
	}
}

report_rejects :: proc(st: ^Ingest_State) {
	if st.rejected == 0 {
		return
	}
	fmt.eprintfln("rejected %d of %d data lines, first at line %d (%v)", st.rejected, st.rejected + st.accepted, st.first_line, st.first_reason)
	for count, reason in st.reasons {
		if count > 0 {
			fmt.eprintfln("  %v: %d", reason, count)
		}
	}
}

usage :: proc() {
	fmt.eprintln("usage: latency_quantile_sketch <summarize|merge|selftest> [-flag=value ...] < input")
	fmt.eprintln("  summarize  read rows from stdin, one value per row, optionally grouped")
	fmt.eprintln("  merge      read DDS1 sketch lines from stdin and combine them per group")
	fmt.eprintln("  selftest   check the error bound, merge and codec against exact quantiles")
	fmt.eprintln("flags:")
	fmt.eprintln("  -alpha=0.01      relative accuracy, between 0.0001 and 0.2")
	fmt.eprintln("  -buckets=2048    bucket limit per sketch, at least 2")
	fmt.eprintln("  -group=0         group column index, -1 for a single group")
	fmt.eprintln("  -value=1         value column index")
	fmt.eprintln("  -unit=ms         unit of bare numbers: ns, us, ms or s")
	fmt.eprintln("  -skip=0          leading lines to ignore, such as a header")
	fmt.eprintln("  -q=0.5,0.9,0.99  quantiles to print")
	fmt.eprintln("  -emit            print DDS1 sketch lines instead of a table")
}

parse_args :: proc(args: []string, cfg: ^Config) -> (err: string) {
	cfg.alpha_ppm = DEFAULT_ALPHA_PPM
	cfg.max_buckets = DEFAULT_MAX_BUCKETS
	cfg.group_col = 0
	cfg.value_col = 1
	cfg.unit_scale = 1.0
	q_text := "0.5,0.9,0.99,0.999"
	if len(args) == 0 {
		return "missing command"
	}
	switch args[0] {
	case "summarize":
		cfg.command = .Summarize
	case "merge":
		cfg.command = .Merge
	case "selftest":
		cfg.command = .Selftest
	case:
		return "unknown command"
	}
	for arg in args[1:] {
		body := strings.trim_left(arg, "-")
		if body == "emit" {
			cfg.emit = true
			continue
		}
		eq := strings.index_byte(body, '=')
		if eq <= 0 {
			return fmt.tprintf("flag %q needs the form -name=value", arg)
		}
		name := body[:eq]
		val := body[eq + 1:]
		switch name {
		case "alpha":
			a, ok := parse_f64_strict(val)
			if !ok || !is_finite(a) {
				return "alpha is not a number"
			}
			cfg.alpha_ppm = int(math.round(a * 1e6))
			if cfg.alpha_ppm < MIN_ALPHA_PPM || cfg.alpha_ppm > MAX_ALPHA_PPM {
				return "alpha must be between 0.0001 and 0.2"
			}
		case "buckets":
			n, ok := parse_int_strict(val)
			if !ok || n < MIN_BUCKETS || n > MAX_BUCKETS_LIMIT {
				return "buckets must be an integer from 2 to 1048576"
			}
			cfg.max_buckets = n
		case "group":
			n, ok := parse_int_strict(val)
			if !ok || n < -1 {
				return "group must be a column index or -1"
			}
			cfg.group_col = n
		case "value":
			n, ok := parse_int_strict(val)
			if !ok || n < 0 {
				return "value must be a column index"
			}
			cfg.value_col = n
		case "skip":
			n, ok := parse_int_strict(val)
			if !ok || n < 0 {
				return "skip must be a non negative integer"
			}
			cfg.skip_lines = n
		case "unit":
			s, ok := unit_scale(val)
			if !ok {
				return "unit must be ns, us, ms or s"
			}
			cfg.unit_scale = s
		case "q":
			q_text = val
		case:
			return fmt.tprintf("unknown flag %q", name)
		}
	}
	for part in strings.split(q_text, ",", context.temp_allocator) {
		q, ok := parse_f64_strict(strings.trim_space(part))
		if !ok || !is_finite(q) || q < 0 || q > 1 {
			return "every quantile must be a number from 0 to 1"
		}
		append(&cfg.quantiles, q)
		append(&cfg.labels, strings.clone(strings.trim_space(part)))
	}
	if cfg.group_col == cfg.value_col {
		return "group and value columns must differ"
	}
	return ""
}

Test :: struct {
	total:  int,
	failed: int,
}

expect :: proc(t: ^Test, cond: bool, name: string) {
	t.total += 1
	if cond {
		fmt.printfln("PASS %s", name)
	} else {
		t.failed += 1
		fmt.printfln("FAIL %s", name)
	}
}

Rng :: struct {
	state: u64,
}

rng_next :: proc(r: ^Rng) -> u64 {
	x := r.state
	x ~= x >> 12
	x ~= x << 25
	x ~= x >> 27
	r.state = x
	return x * 0x2545F4914F6CDD1D
}

rng_f64 :: proc(r: ^Rng) -> f64 {
	return f64(rng_next(r) >> 11) / f64(u64(1) << 53)
}

synthetic_latency :: proc(r: ^Rng) -> f64 {
	z := -6.0
	for _ in 0 ..< 12 {
		z += rng_f64(r)
	}
	v := math.exp(math.ln(120.0) + 1.1 * z)
	if rng_f64(r) < 0.002 {
		v *= 50
	}
	if rng_f64(r) < 0.01 {
		v = 0
	}
	return v
}

same_buckets :: proc(a, b: ^Sketch) -> bool {
	if len(a.buckets) != len(b.buckets) || a.count != b.count || a.zero_count != b.zero_count {
		return false
	}
	for k, c in a.buckets {
		other, found := b.buckets[k]
		if !found || other != c {
			return false
		}
	}
	return a.min == b.min && a.max == b.max
}

within_bound :: proc(s: ^Sketch, sorted: []f64, q: f64, floor_value: f64) -> bool {
	est, ok := sketch_quantile(s, q)
	if !ok {
		return false
	}
	exact := sorted[int(q * f64(len(sorted) - 1))]
	if exact < floor_value {
		return true
	}
	alpha := f64(s.alpha_ppm) / 1e6
	return abs(est - exact) <= alpha * exact * 1.0001 + MIN_INDEXABLE
}

run_selftest :: proc() -> int {
	t: Test
	rng := Rng{0x9E3779B97F4A7C15}
	samples := make([dynamic]f64, 0, 200_000)
	defer delete(samples)
	for _ in 0 ..< 200_000 {
		append(&samples, synthetic_latency(&rng))
	}
	sorted := slice.clone(samples[:])
	defer delete(sorted)
	slice.sort(sorted)

	qs := [?]f64{0, 0.01, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1}

	direct, left, right: Sketch
	sketch_init(&direct, 10_000, 4096)
	sketch_init(&left, 10_000, 4096)
	sketch_init(&right, 10_000, 4096)
	defer sketch_destroy(&direct)
	defer sketch_destroy(&left)
	defer sketch_destroy(&right)
	for v, i in samples {
		sketch_add(&direct, v)
		if i % 2 == 0 {
			sketch_add(&left, v)
		} else {
			sketch_add(&right, v)
		}
	}

	all_ok := true
	for q in qs {
		all_ok &&= within_bound(&direct, sorted, q, 0)
	}
	expect(&t, all_ok, "direct sketch stays inside the 1 percent relative error bound")
	expect(&t, !sketch_saturated(&direct), "uncollapsed configuration did not hit the bucket limit")

	merged: Sketch
	sketch_init(&merged, 10_000, 4096)
	defer sketch_destroy(&merged)
	expect(&t, sketch_merge(&merged, &left) == .None, "merge of the left half succeeds")
	expect(&t, sketch_merge(&merged, &right) == .None, "merge of the right half succeeds")
	expect(&t, same_buckets(&merged, &direct), "merging two halves equals one pass over everything")

	b := strings.builder_make()
	defer strings.builder_destroy(&b)
	sketch_encode(&b, &merged)
	line := strings.clone(strings.to_string(b))
	defer delete(line)
	decoded: Sketch
	expect(&t, sketch_decode(&decoded, line) == "", "encoded sketch decodes")
	expect(&t, same_buckets(&decoded, &merged), "codec round trip keeps every bucket")
	if decoded.buckets != nil {
		sketch_destroy(&decoded)
	}

	broken := fmt.tprintf("%s 9999", line)
	broken_sketch: Sketch
	expect(&t, sketch_decode(&broken_sketch, broken) != "", "a sketch with an extra stray token is rejected")
	truncated := line[:strings.last_index_byte(line, ' ')]
	truncated_sketch: Sketch
	expect(&t, sketch_decode(&truncated_sketch, truncated) != "", "a truncated sketch fails the count check")

	coarse: Sketch
	sketch_init(&coarse, 50_000, 4096)
	defer sketch_destroy(&coarse)
	expect(&t, sketch_merge(&coarse, &left) == .Alpha_Mismatch, "merging sketches with different alpha is refused")

	small: Sketch
	sketch_init(&small, 10_000, 400)
	defer sketch_destroy(&small)
	for v in samples {
		sketch_add(&small, v)
	}
	expect(&t, small.count == direct.count, "collapse never drops a sample")
	expect(&t, len(small.buckets) == 400, "collapse holds the bucket limit")
	small_keys := sorted_keys(&small)
	lowest := small_keys[0]
	floor_value := bucket_value(&small, lowest) * small.gamma
	upper_ok := true
	for q in qs {
		upper_ok &&= within_bound(&small, sorted, q, floor_value)
	}
	expect(&t, upper_ok, "collapsed sketch keeps the bound for every quantile above the merged region")
	expect(&t, small.max == sorted[len(sorted) - 1] && small.min == sorted[0], "min and max stay exact after collapse")

	empty: Sketch
	sketch_init(&empty, 10_000, 16)
	defer sketch_destroy(&empty)
	_, empty_ok := sketch_quantile(&empty, 0.5)
	expect(&t, !empty_ok, "empty sketch reports no quantile")
	_, range_ok := sketch_quantile(&direct, 1.5)
	expect(&t, !range_ok, "quantile above 1 is refused")

	_, e_nan := parse_value("nan", 1)
	_, e_neg := parse_value("-3ms", 1)
	_, e_big := parse_value("1e30", 1)
	us_value, e_us := parse_value("2500us", 1)
	s_value, e_s := parse_value("1.5s", 1)
	expect(&t, e_nan == .Non_Finite, "nan is rejected")
	expect(&t, e_neg == .Negative, "negative duration is rejected")
	expect(&t, e_big == .Too_Large, "absurd duration is rejected")
	expect(&t, e_us == .None && abs(us_value - 2.5) < 1e-9, "microsecond suffix converts to milliseconds")
	expect(&t, e_s == .None && abs(s_value - 1500) < 1e-9, "second suffix converts to milliseconds")

	fmt.printfln("%d checks, %d failed", t.total, t.failed)
	if t.failed > 0 {
		return EXIT_SELFTEST_FAILED
	}
	return EXIT_OK
}

main :: proc() {
	cfg: Config
	cfg.quantiles = make([dynamic]f64)
	cfg.labels = make([dynamic]string)
	if err := parse_args(os.args[1:], &cfg); err != "" {
		fmt.eprintfln("error: %s", err)
		usage()
		os.exit(EXIT_USAGE)
	}
	if cfg.command == .Selftest {
		os.exit(run_selftest())
	}

	st := Ingest_State{cfg = &cfg}
	st.groups = make(map[string]^Sketch)
	handler: Line_Handler = ingest_row
	if cfg.command == .Merge {
		handler = ingest_sketch
	}
	if !for_each_stdin_line(&st, handler) {
		os.exit(EXIT_USAGE)
	}
	report_rejects(&st)
	if st.accepted == 0 {
		fmt.eprintln("error: no usable input rows")
		os.exit(EXIT_USAGE)
	}
	if cfg.emit {
		print_sketches(st.groups)
	} else {
		print_table(st.groups, &cfg)
	}
	if st.rejected > 0 {
		os.exit(EXIT_REJECTED_ROWS)
	}
	os.exit(EXIT_OK)
}
