module main

import os
import json
import math
import strconv

// Exit codes: 0 clean, 1 bad input or usage, 2 a gate threshold failed.
const exit_ok = 0
const exit_input = 1
const exit_gate = 2

const algo_names = ['ring', 'rendezvous', 'jump', 'maglev']
const default_vnodes = 160
const default_table = 65537
const default_count = 200000
const max_nodes = 100000

// ---------------------------------------------------------------------------
// Hashing. FNV-1a folds the bytes, then a splitmix64 finalizer scrambles the
// result so short, similar names ("cache-1", "cache-2") do not land next to
// each other on the ring. The seed lets you re run the audit under a second
// hash universe to see whether a verdict depends on lucky placement.
// ---------------------------------------------------------------------------

fn mix64(x u64) u64 {
	mut z := x + u64(0x9e3779b97f4a7c15)
	z = (z ^ (z >> 30)) * u64(0xbf58476d1ce4e5b9)
	z = (z ^ (z >> 27)) * u64(0x94d049bb133111eb)
	return z ^ (z >> 31)
}

fn hash64(s string, seed u64) u64 {
	mut h := u64(14695981039346656037) ^ seed
	for b in s.bytes() {
		h ^= u64(b)
		h *= u64(1099511628211)
	}
	return mix64(h)
}

// unit_open maps a hash to the open interval (0, 1) so ln() is always finite.
fn unit_open(h u64) f64 {
	return (f64(h >> 11) + 0.5) / 9007199254740992.0
}

// ---------------------------------------------------------------------------
// Node set loading and validation.
// ---------------------------------------------------------------------------

struct Node {
	name   string
	weight f64
}

fn load_nodes(path string) ![]Node {
	lines := os.read_lines(path) or { return error('cannot read ${path}: ${err}') }
	mut nodes := []Node{}
	mut seen := map[string]int{}
	for i, raw in lines {
		mut line := raw
		if hash_at := line.index('#') {
			line = line[..hash_at]
		}
		line = line.trim_space()
		if line.len == 0 {
			continue
		}
		fields := line.replace('\t', ' ').split(' ').filter(it.len > 0)
		if fields.len > 2 {
			return error('${path}:${i + 1}: expected "name" or "name weight", got ${fields.len} fields')
		}
		name := fields[0]
		mut weight := 1.0
		if fields.len == 2 {
			weight = strconv.atof64(fields[1]) or {
				return error('${path}:${i + 1}: weight "${fields[1]}" is not a number')
			}
		}
		if !(weight > 0.0) || weight > 1e9 || math.is_nan(weight) || math.is_inf(weight, 0) {
			return error('${path}:${i + 1}: weight for ${name} must be a finite number above 0 and at most 1e9')
		}
		if name in seen {
			return error('${path}:${i + 1}: duplicate node ${name} (first on line ${seen[name]})')
		}
		seen[name] = i + 1
		nodes << Node{
			name:   name
			weight: weight
		}
	}
	if nodes.len == 0 {
		return error('${path}: no nodes found')
	}
	if nodes.len > max_nodes {
		return error('${path}: ${nodes.len} nodes exceeds the limit of ${max_nodes}')
	}
	return nodes
}

fn sorted_by_name(nodes []Node) []Node {
	mut out := nodes.clone()
	out.sort(a.name < b.name)
	return out
}

fn is_prime(n int) bool {
	if n < 2 {
		return false
	}
	if n % 2 == 0 {
		return n == 2
	}
	mut d := 3
	for d * d <= n {
		if n % d == 0 {
			return false
		}
		d += 2
	}
	return true
}

// ---------------------------------------------------------------------------
// Routers. Each one answers "which local node owns this key hash" and reports
// its effective weights, because jump hash cannot honour weights at all and
// the audit must judge it against the shares it can actually deliver.
// ---------------------------------------------------------------------------

struct RingPoint {
	h    u64
	node int
}

struct Router {
	kind    string
	names   []string
	weights []f64
mut:
	points []RingPoint
	table  []int
	hashes []u64
}

// build_router orders nodes by name for every algorithm except jump. Jump
// hash is positional by construction, so it keeps the order you gave it.
fn build_router(kind string, nodes []Node, vnodes int, table_size int, seed u64) !Router {
	ordered := if kind == 'jump' { nodes.clone() } else { sorted_by_name(nodes) }
	mut r := Router{
		kind:    kind
		names:   ordered.map(it.name)
		weights: if kind == 'jump' { ordered.map(f64(1.0)) } else { ordered.map(it.weight) }
	}
	r.hashes = ordered.map(hash64(it.name, seed))
	match kind {
		'ring' {
			for i, n in ordered {
				count := int(math.max(1.0, math.round(n.weight * f64(vnodes))))
				for v in 0 .. count {
					r.points << RingPoint{
						h:    hash64('${n.name}#${v}', seed)
						node: i
					}
				}
			}
			r.points.sort_with_compare(fn (a &RingPoint, b &RingPoint) int {
				if a.h != b.h {
					return if a.h < b.h { -1 } else { 1 }
				}
				return if a.node < b.node {
					-1
				} else if a.node > b.node {
					1
				} else {
					0
				}
			})
		}
		'maglev' {
			r.table = populate_maglev(ordered, table_size, seed)!
		}
		'rendezvous', 'jump' {}
		else {
			return error('unknown algorithm ${kind}')
		}
	}

	return r
}

// populate_maglev follows the Maglev paper: every node walks its own
// permutation of the table and claims the next free slot, round robin. A
// node with a larger weight earns proportionally more turns per round.
fn populate_maglev(nodes []Node, m int, seed u64) ![]int {
	if !is_prime(m) {
		return error('maglev table size ${m} must be prime')
	}
	if m < nodes.len {
		return error('maglev table size ${m} is smaller than the node count ${nodes.len}')
	}
	mut max_w := 0.0
	for n in nodes {
		max_w = math.max(max_w, n.weight)
	}
	mut offset := []u64{len: nodes.len}
	mut skip := []u64{len: nodes.len}
	mut next := []u64{len: nodes.len}
	mut credit := []f64{len: nodes.len}
	for i, n in nodes {
		offset[i] = hash64(n.name + '/offset', seed) % u64(m)
		skip[i] = hash64(n.name + '/skip', seed) % u64(m - 1) + 1
	}
	mut table := []int{len: m, init: -1}
	mut filled := 0
	for filled < m {
		for i in 0 .. nodes.len {
			credit[i] += nodes[i].weight / max_w
			for credit[i] >= 1.0 && filled < m {
				credit[i] -= 1.0
				mut slot := (offset[i] + skip[i] * next[i]) % u64(m)
				for table[int(slot)] != -1 {
					next[i]++
					slot = (offset[i] + skip[i] * next[i]) % u64(m)
				}
				table[int(slot)] = i
				next[i]++
				filled++
			}
		}
	}
	return table
}

fn jump_bucket(key u64, buckets int) int {
	mut k := key
	mut b := -1
	mut j := 0
	for j < buckets {
		b = j
		k = k * u64(2862933555777941757) + 1
		j = int(f64(b + 1) * (2147483648.0 / f64((k >> 33) + 1)))
	}
	return b
}

fn (r &Router) route(kh u64) int {
	match r.kind {
		'ring' {
			mut lo := 0
			mut hi := r.points.len
			for lo < hi {
				mid := (lo + hi) / 2
				if r.points[mid].h < kh {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			if lo == r.points.len {
				lo = 0
			}
			return r.points[lo].node
		}
		'rendezvous' {
			mut best := 0
			mut best_score := -1.0
			for i in 0 .. r.names.len {
				u := unit_open(mix64(kh ^ r.hashes[i]))
				score := r.weights[i] / -math.log(u)
				if score > best_score {
					best_score = score
					best = i
				}
			}
			return best
		}
		'jump' {
			return jump_bucket(kh, r.names.len)
		}
		else {
			return r.table[int(kh % u64(r.table.len))]
		}
	}
}

// ---------------------------------------------------------------------------
// Keys.
// ---------------------------------------------------------------------------

fn load_key_hashes(keys_path string, count int, seed u64) ![]u64 {
	mut out := []u64{}
	if keys_path.len > 0 {
		lines := os.read_lines(keys_path) or { return error('cannot read ${keys_path}: ${err}') }
		for l in lines {
			k := l.trim_right('\r')
			if k.len > 0 {
				out << hash64(k, seed ^ u64(0x5bd1e995))
			}
		}
		if out.len == 0 {
			return error('${keys_path}: no keys found')
		}
		return out
	}
	if count < 1 {
		return error('key count must be at least 1')
	}
	out = []u64{cap: count}
	for i in 0 .. count {
		out << hash64('key:${i}', seed ^ u64(0x5bd1e995))
	}
	return out
}

// ---------------------------------------------------------------------------
// Analysis.
// ---------------------------------------------------------------------------

struct NodeDelta {
	name         string
	share_before f64
	share_after  f64
	load_before  f64
	load_after   f64
	gained       int
	lost         int
}

struct AlgoReport {
	algorithm           string
	note                string
	keys                int
	moved               int
	moved_fraction      f64
	ideal_fraction      f64
	excess_ratio        f64
	moved_without_ideal bool
	needless_moves      f64
	imbalance_before    f64
	imbalance_after     f64
	min_load_after      f64
	cv_after            f64
	starved_nodes       []string
	nodes               []NodeDelta
}

fn shares(names []string, weights []f64, universe []string) []f64 {
	mut total := 0.0
	for w in weights {
		total += w
	}
	mut idx := map[string]int{}
	for i, n in names {
		idx[n] = i
	}
	mut out := []f64{len: universe.len}
	for u, n in universe {
		if n in idx {
			out[u] = weights[idx[n]] / total
		}
	}
	return out
}

fn analyze(kind string, before []Node, after []Node, kh []u64, vnodes int, table_size int, seed u64) !AlgoReport {
	rb := build_router(kind, before, vnodes, table_size, seed)!
	ra := build_router(kind, after, vnodes, table_size, seed)!

	mut allset := map[string]bool{}
	for n in before {
		allset[n.name] = true
	}
	for n in after {
		allset[n.name] = true
	}
	mut universe := allset.keys()
	universe.sort()
	mut uid := map[string]int{}
	for i, n in universe {
		uid[n] = i
	}
	map_b := rb.names.map(uid[it])
	map_a := ra.names.map(uid[it])

	mut in_before := []bool{len: universe.len}
	mut in_after := []bool{len: universe.len}
	for n in rb.names {
		in_before[uid[n]] = true
	}
	for n in ra.names {
		in_after[uid[n]] = true
	}

	sb := shares(rb.names, rb.weights, universe)
	sa := shares(ra.names, ra.weights, universe)
	mut cnt_b := []int{len: universe.len}
	mut cnt_a := []int{len: universe.len}
	mut gained := []int{len: universe.len}
	mut lost := []int{len: universe.len}
	mut moved := 0
	mut needless := 0
	for h in kh {
		ob := map_b[rb.route(h)]
		oa := map_a[ra.route(h)]
		cnt_b[ob]++
		cnt_a[oa]++
		if ob != oa {
			moved++
			lost[ob]++
			gained[oa]++
			if sa[ob] >= sb[ob] - 1e-12 || sa[oa] <= sb[oa] + 1e-12 {
				needless++
			}
		}
	}

	mut ideal := 0.0
	for i in 0 .. universe.len {
		if sa[i] > sb[i] {
			ideal += sa[i] - sb[i]
		}
	}
	total := f64(kh.len)
	moved_frac := f64(moved) / total

	mut excess := 1.0
	mut without_ideal := false
	if ideal > 1e-12 {
		excess = moved_frac / ideal
	} else if moved > 0 {
		without_ideal = true
	}

	mut max_a := 0.0
	mut min_a := 1e18
	mut max_b := 0.0
	mut sum_a := 0.0
	mut sumsq_a := 0.0
	mut live := 0
	mut starved := []string{}
	mut deltas := []NodeDelta{}
	for i in 0 .. universe.len {
		mut nl_b := 0.0
		mut nl_a := 0.0
		if in_before[i] {
			nl_b = f64(cnt_b[i]) / (total * sb[i])
			max_b = math.max(max_b, nl_b)
		}
		if in_after[i] {
			nl_a = f64(cnt_a[i]) / (total * sa[i])
			max_a = math.max(max_a, nl_a)
			min_a = math.min(min_a, nl_a)
			sum_a += nl_a
			sumsq_a += nl_a * nl_a
			live++
			if cnt_a[i] == 0 {
				starved << universe[i]
			}
		}
		deltas << NodeDelta{
			name:         universe[i]
			share_before: sb[i]
			share_after:  sa[i]
			load_before:  nl_b
			load_after:   nl_a
			gained:       gained[i]
			lost:         lost[i]
		}
	}
	mean_a := sum_a / f64(live)
	var_a := math.max(0.0, sumsq_a / f64(live) - mean_a * mean_a)
	cv := if mean_a > 0.0 { math.sqrt(var_a) / mean_a } else { 0.0 }

	mut note := ''
	if kind == 'jump' {
		note = 'jump hash ignores weights and only grows or shrinks at the tail of the node list'
	} else if kind == 'ring' {
		note = '${vnodes} virtual points per unit of weight'
	} else if kind == 'maglev' {
		note = 'table size ${table_size}'
	}
	return AlgoReport{
		algorithm:           kind
		note:                note
		keys:                kh.len
		moved:               moved
		moved_fraction:      moved_frac
		ideal_fraction:      ideal
		excess_ratio:        excess
		moved_without_ideal: without_ideal
		needless_moves:      f64(needless) / total
		imbalance_before:    max_b
		imbalance_after:     max_a
		min_load_after:      if live > 0 { min_a } else { 0.0 }
		cv_after:            cv
		starved_nodes:       starved
		nodes:               deltas
	}
}

// ---------------------------------------------------------------------------
// CLI.
// ---------------------------------------------------------------------------

struct Options {
mut:
	before_path   string
	after_path    string
	keys_path     string
	algos         []string = algo_names.clone()
	count         int      = default_count
	vnodes        int      = default_vnodes
	table_size    int      = default_table
	seed          u64
	as_json       bool
	max_excess    f64 = -1.0
	max_needless  f64 = -1.0
	max_imbalance f64 = -1.0
	top           int = 8
}

struct Output {
	before_nodes int
	after_nodes  int
	keys         int
	seed         u64
	reports      []AlgoReport
	failures     []string
}

fn usage() string {
	return 'ConsistentHashRebalanceAuditor <before.txt> <after.txt> [options]

Node files hold one node per line: "name" or "name weight". Lines starting
with # and trailing # comments are ignored.

Options:
  -algo a,b       ring, rendezvous, jump, maglev (default: all four)
  -keys file      one key per line, repeat a key to weight its traffic
  -count N        synthetic key count when -keys is absent (default ${default_count})
  -vnodes N       ring points per unit of weight (default ${default_vnodes})
  -table N        maglev table size, must be prime (default ${default_table})
  -seed N         hash seed, change it to test placement luck (default 0)
  -top N          nodes shown per algorithm in text mode (default 8)
  -json           machine readable output
Gates, each exits 2 when broken:
  -max-excess X     moved fraction divided by the theoretical minimum
  -max-needless X   fraction of keys moved for no reason the node change explains
  -max-imbalance X  busiest node load divided by its fair share, after'
}

fn parse_args(args []string) !Options {
	mut o := Options{}
	mut positional := []string{}
	mut i := 0
	for i < args.len {
		a := args[i]
		if a == '-json' {
			o.as_json = true
			i++
			continue
		}
		if a.starts_with('-') && a.len > 1 {
			if i + 1 >= args.len {
				return error('${a} needs a value')
			}
			v := args[i + 1]
			match a {
				'-algo' {
					o.algos = v.split(',').map(it.trim_space())
					for x in o.algos {
						if x !in algo_names {
							return error('unknown algorithm "${x}", expected one of ${algo_names}')
						}
					}
				}
				'-keys' {
					o.keys_path = v
				}
				'-count' {
					o.count = v.int()
				}
				'-vnodes' {
					o.vnodes = v.int()
				}
				'-table' {
					o.table_size = v.int()
				}
				'-top' {
					o.top = v.int()
				}
				'-seed' {
					o.seed = v.u64()
				}
				'-max-excess' {
					o.max_excess = strconv.atof64(v) or {
						return error('bad number for ${a}: ${v}')
					}
				}
				'-max-needless' {
					o.max_needless = strconv.atof64(v) or {
						return error('bad number for ${a}: ${v}')
					}
				}
				'-max-imbalance' {
					o.max_imbalance = strconv.atof64(v) or {
						return error('bad number for ${a}: ${v}')
					}
				}
				else {
					return error('unknown option ${a}')
				}
			}

			i += 2
			continue
		}
		positional << a
		i++
	}
	if positional.len != 2 {
		return error('expected two node files, got ${positional.len}')
	}
	o.before_path = positional[0]
	o.after_path = positional[1]
	if o.vnodes < 1 || o.vnodes > 100000 {
		return error('-vnodes must be between 1 and 100000')
	}
	if o.count < 1 {
		return error('-count must be at least 1')
	}
	return o
}

fn pct(x f64) string {
	return '${x * 100.0:.2f}%'
}

fn render_text(out Output, o Options) string {
	mut s := ''
	s += 'before: ${out.before_nodes} nodes   after: ${out.after_nodes} nodes   keys: ${out.keys}   seed: ${out.seed}\n'
	for r in out.reports {
		s += '\n== ${r.algorithm} ==\n'
		if r.note.len > 0 {
			s += '   ${r.note}\n'
		}
		s += '   moved           ${pct(r.moved_fraction)}  (minimum possible ${pct(r.ideal_fraction)})\n'
		if r.moved_without_ideal {
			s += '   excess          undefined: nothing had to move but keys did\n'
		} else {
			s += '   excess          ${r.excess_ratio:.3f}x the minimum\n'
		}
		s += '   needless moves  ${pct(r.needless_moves)}  (keys moved off a node that lost no share or onto one that gained none)\n'
		s += '   busiest node    ${r.imbalance_after:.3f}x fair share after, ${r.imbalance_before:.3f}x before\n'
		s += '   quietest node   ${r.min_load_after:.3f}x fair share after   cv ${r.cv_after:.4f}\n'
		if r.starved_nodes.len > 0 {
			s += '   starved         ${r.starved_nodes.join(', ')} own no keys\n'
		}
		mut rows := r.nodes.clone()
		rows.sort_with_compare(fn (a &NodeDelta, b &NodeDelta) int {
			da := a.gained + a.lost
			db := b.gained + b.lost
			if da != db {
				return if da > db { -1 } else { 1 }
			}
			return if a.name < b.name {
				-1
			} else if a.name > b.name {
				1
			} else {
				0
			}
		})
		shown := if o.top < rows.len { o.top } else { rows.len }
		s += '   top ${shown} movers: name, gained, lost, load after\n'
		for d in rows[..shown] {
			s += '     ${d.name:-24} +${d.gained:-8} -${d.lost:-8} ${d.load_after:.3f}x\n'
		}
	}
	if out.failures.len > 0 {
		s += '\nGATE FAILED\n'
		for f in out.failures {
			s += '  ${f}\n'
		}
	}
	return s
}

fn gate(reports []AlgoReport, o Options) []string {
	mut fails := []string{}
	for r in reports {
		if o.max_excess >= 0.0 && (r.moved_without_ideal || r.excess_ratio > o.max_excess) {
			fails << '${r.algorithm}: excess ${r.excess_ratio:.3f}x is above -max-excess ${o.max_excess:.3f}'
		}
		if o.max_needless >= 0.0 && r.needless_moves > o.max_needless {
			fails << '${r.algorithm}: needless moves ${pct(r.needless_moves)} is above -max-needless ${pct(o.max_needless)}'
		}
		if o.max_imbalance >= 0.0 && r.imbalance_after > o.max_imbalance {
			fails << '${r.algorithm}: busiest node ${r.imbalance_after:.3f}x is above -max-imbalance ${o.max_imbalance:.3f}'
		}
	}
	return fails
}

fn run(args []string) !int {
	o := parse_args(args)!
	before := load_nodes(o.before_path)!
	after := load_nodes(o.after_path)!
	kh := load_key_hashes(o.keys_path, o.count, o.seed)!

	mut reports := []AlgoReport{}
	for kind in o.algos {
		reports << analyze(kind, before, after, kh, o.vnodes, o.table_size, o.seed)!
	}
	failures := gate(reports, o)
	out := Output{
		before_nodes: before.len
		after_nodes:  after.len
		keys:         kh.len
		seed:         o.seed
		reports:      reports
		failures:     failures
	}
	if o.as_json {
		println(json.encode_pretty(out))
	} else {
		print(render_text(out, o))
	}
	return if failures.len > 0 { exit_gate } else { exit_ok }
}

fn main() {
	args := os.args[1..]
	if args.len == 0 || '-h' in args || '--help' in args || 'help' in args {
		println(usage())
		exit(if args.len == 0 { exit_input } else { exit_ok })
	}
	code := run(args) or {
		eprintln('error: ${err}')
		exit(exit_input)
	}
	exit(code)
}
