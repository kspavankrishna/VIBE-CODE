module main

import os
import json

// ---------------------------------------------------------------------------
// Deterministic PRNG (splitmix64). No external dependency: every accept/
// reject roll and every resample is reproducible from the seed recorded on
// the round, so a rejected token can be replayed byte for byte during a
// postmortem instead of trusting whatever the live sampler did.
// ---------------------------------------------------------------------------

struct Rng {
mut:
	state u64
}

fn new_rng(seed u64) Rng {
	mut s := seed
	if s == 0 {
		s = 0x9e3779b97f4a7c15
	}
	return Rng{
		state: s
	}
}

fn (mut r Rng) next_u64() u64 {
	r.state += 0x9e3779b97f4a7c15
	mut z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// next_f64 returns a value in [0, 1) built from the top 53 bits so every
// double mantissa bit is reachable, matching what a real accept/reject
// coin flip needs.
fn (mut r Rng) next_f64() f64 {
	v := r.next_u64() >> 11
	return f64(v) / f64(u64(1) << 53)
}

// ---------------------------------------------------------------------------
// Wire types. One JSON object per line, one line per speculative round.
// ---------------------------------------------------------------------------

const prob_epsilon = 1e-6
const residual_epsilon = 1e-9

struct TopKEntry {
	id int
	p  f64
}

struct DraftTok {
	id int
	q  f64
}

// SpecRound is one speculative decoding round for one stream: the draft
// model proposed `draft` (k tokens), the target model was run once over
// all k+1 positions (k verify slots plus one free bonus slot) and reports
// target_p (its probability for each drafted token) plus the truncated
// top-k distributions needed to build the correction distribution on a
// rejection.
struct SpecRound {
	stream_id   string
	round       int
	seed        u64
	draft       []DraftTok
	target_p    []f64
	target_topk [][]TopKEntry
	draft_topk  [][]TopKEntry
}

// ---------------------------------------------------------------------------
// Categorical and residual sampling. This is the exact modified rejection
// sampling scheme from speculative decoding (Leviathan et al. 2023 /
// Chen et al. 2023): accept a drafted token x with probability
// min(1, p(x)/q(x)); on rejection, resample from
// normalize(max(0, p(x) - q(x))) so the marginal output distribution is
// still exactly the target model's distribution, not the draft's.
// ---------------------------------------------------------------------------

fn sample_categorical(entries []TopKEntry, mut rng Rng) !int {
	if entries.len == 0 {
		return error('empty categorical distribution')
	}
	mut total := 0.0
	for e in entries {
		total += e.p
	}
	if total <= 0.0 {
		return error('categorical distribution has non-positive mass ${total}')
	}
	roll := rng.next_f64() * total
	mut cum := 0.0
	for e in entries {
		cum += e.p
		if roll < cum {
			return e.id
		}
	}
	// floating point rounding: cum can land a hair under roll. Take the
	// last entry rather than fail a round over a sub-ulp gap.
	return entries[entries.len - 1].id
}

struct ResidualSample {
	id         int
	degenerate bool
}

// sample_residual builds max(0, p(x) - q(x)) over the union of the two
// truncated top-k windows and samples from it. If that residual mass is
// ~0 even though a rejection happened, the true correction probably sits
// outside both truncated windows (a real, structural gap in top-k based
// implementations, not a bug): fall back to the target's own top-k so
// decoding still makes forward progress, and flag the round as
// degenerate so the caller can see how often the windows were too narrow.
fn sample_residual(target_dist []TopKEntry, draft_dist []TopKEntry, mut rng Rng) !ResidualSample {
	mut p_map := map[int]f64{}
	mut q_map := map[int]f64{}
	mut ids := map[int]bool{}
	for e in target_dist {
		p_map[e.id] = e.p
		ids[e.id] = true
	}
	for e in draft_dist {
		q_map[e.id] = e.p
		ids[e.id] = true
	}
	mut residual := []TopKEntry{}
	mut total := 0.0
	for id, _ in ids {
		d := p_map[id] - q_map[id]
		if d > 0.0 {
			residual << TopKEntry{
				id: id
				p:  d
			}
			total += d
		}
	}
	if total < residual_epsilon {
		if target_dist.len == 0 {
			return error('degenerate residual and empty target top-k: nothing to fall back to')
		}
		fallback_id := sample_categorical(target_dist, mut rng)!
		return ResidualSample{
			id:         fallback_id
			degenerate: true
		}
	}
	chosen_id := sample_categorical(residual, mut rng)!
	return ResidualSample{
		id:         chosen_id
		degenerate: false
	}
}

// ---------------------------------------------------------------------------
// Round verification: the accept/reject walk plus the bonus token that a
// fully accepted round always earns (the target model was already run on
// one extra position beyond the last draft token, so that token is free).
// ---------------------------------------------------------------------------

struct RoundOutcome {
	stream_id      string
	round          int
	k              int
	accepted       int
	used_bonus     bool
	degenerate     bool
	final_token_id int
	output_ids     []int
}

fn verify_round(r SpecRound, mut rng Rng) !RoundOutcome {
	k := r.draft.len
	if r.target_p.len != k {
		return error('target_p has ${r.target_p.len} entries, expected ${k} (one per drafted token)')
	}
	if r.draft_topk.len != k {
		return error('draft_topk has ${r.draft_topk.len} entries, expected ${k}')
	}
	if r.target_topk.len != k + 1 {
		return error('target_topk has ${r.target_topk.len} entries, expected k+1=${k + 1} (k verify slots plus the bonus slot)')
	}

	mut accepted := 0
	mut output_ids := []int{}
	mut final_id := -1
	mut degenerate := false
	mut used_bonus := false

	for i in 0 .. k {
		dt := r.draft[i]
		if dt.q <= 0.0 || dt.q > 1.0 + prob_epsilon {
			return error('draft prob q=${dt.q} for token ${dt.id} at position ${i} is out of range (0,1]')
		}
		p_x := r.target_p[i]
		if p_x < 0.0 || p_x > 1.0 + prob_epsilon {
			return error('target prob p=${p_x} for token ${dt.id} at position ${i} is out of range [0,1]')
		}
		accept_prob := if p_x >= dt.q { 1.0 } else { p_x / dt.q }
		roll := rng.next_f64()
		if roll < accept_prob {
			accepted++
			output_ids << dt.id
			continue
		}
		res := sample_residual(r.target_topk[i], r.draft_topk[i], mut rng)!
		final_id = res.id
		degenerate = res.degenerate
		output_ids << res.id
		break
	}

	if accepted == k {
		bonus_id := sample_categorical(r.target_topk[k], mut rng)!
		final_id = bonus_id
		used_bonus = true
		output_ids << bonus_id
	}

	return RoundOutcome{
		stream_id:      r.stream_id
		round:          r.round
		k:              k
		accepted:       accepted
		used_bonus:     used_bonus
		degenerate:     degenerate
		final_token_id: final_id
		output_ids:     output_ids
	}
}

// ---------------------------------------------------------------------------
// Per-stream bookkeeping: acceptance EMA, adaptive draft depth, and the
// KV-cache rollback ledger. The target model's forward pass tentatively
// extends the cache by k+1 positions every round; only accepted+1 of those
// turn out to be real, and the server must truncate the rest back out
// before the next round or the cache silently drifts out of sync with the
// emitted sequence, corrupting every future step.
// ---------------------------------------------------------------------------

const min_draft_k = 1
const max_draft_k = 16
const accept_high_watermark = 0.85
const accept_low_watermark = 0.5
const ema_alpha = 0.2

struct StreamStats {
mut:
	rounds            int
	drafted_total     i64
	accepted_total    i64
	bonus_rounds      int
	degenerate_events int
	rollback_tokens   i64
	kv_confirmed_len  i64
	accept_ema        f64
	k_recommend       int
}

fn new_stream_stats(initial_k int) StreamStats {
	return StreamStats{
		accept_ema:  1.0
		k_recommend: initial_k
	}
}

// next_draft_k is a bounded AIMD controller: multiplicative increase while
// the rolling acceptance rate stays high (the draft model is keeping up,
// so spend more of its cheap compute per target-model pass), additive
// decrease as soon as it drops (a wrong guess wastes an entire verify
// slot, so pull back fast).
fn next_draft_k(current int, ema f64) int {
	mut next := current
	if ema >= accept_high_watermark {
		next = int(f64(current) * 1.25) + 1
	} else if ema < accept_low_watermark {
		next = current - 1
	}
	if next < min_draft_k {
		next = min_draft_k
	}
	if next > max_draft_k {
		next = max_draft_k
	}
	return next
}

fn (mut s StreamStats) record(o RoundOutcome) {
	s.rounds++
	s.drafted_total += o.k
	s.accepted_total += o.accepted
	if o.used_bonus {
		s.bonus_rounds++
	}
	if o.degenerate {
		s.degenerate_events++
	}

	rate := if o.k > 0 { f64(o.accepted) / f64(o.k) } else { 1.0 }
	s.accept_ema = ema_alpha * rate + (1.0 - ema_alpha) * s.accept_ema

	tentative_extend := i64(o.k + 1)
	real_extend := i64(o.accepted + 1)
	s.rollback_tokens += tentative_extend - real_extend
	s.kv_confirmed_len += real_extend

	s.k_recommend = next_draft_k(s.k_recommend, s.accept_ema)
}

// effective_speedup approximates the wall-clock win over plain
// autoregressive decoding: one target-model pass produced this many real
// tokens instead of one, at the cost of running the (cheap) draft model
// drafted_total times.
fn (s StreamStats) effective_speedup() f64 {
	if s.rounds == 0 {
		return 0.0
	}
	real_tokens := s.accepted_total + i64(s.rounds)
	return f64(real_tokens) / f64(s.rounds)
}

fn (s StreamStats) acceptance_rate() f64 {
	if s.drafted_total == 0 {
		return 0.0
	}
	return f64(s.accepted_total) / f64(s.drafted_total)
}

// ---------------------------------------------------------------------------
// CLI driver: replays a JSONL trace of speculative rounds and reports the
// reconciliation. Meant to sit next to a real speculative decoding
// serving stack, either fed by its own emitted trace for a postmortem or
// wired straight into it as the verification library.
// ---------------------------------------------------------------------------

fn print_usage() {
	println('SpeculativeDecodeVerifier: replay and reconcile speculative decoding rounds')
	println('')
	println('Usage:')
	println('  SpeculativeDecodeVerifier [flags] [trace.jsonl]')
	println('  cat trace.jsonl | SpeculativeDecodeVerifier [flags]')
	println('')
	println('Flags:')
	println('  -k=N       initial recommended draft depth for streams seen for the first time (default 4)')
	println('  -json      emit one JSON RoundOutcome per input line instead of a text line')
	println('  -strict    exit 1 if any line failed to parse or violated the verification protocol')
	println('  -h         show this help')
	println('')
	println('Each input line is a JSON object: {"stream_id","round","seed",')
	println('"draft":[{"id","q"}...], "target_p":[...], "draft_topk":[[{"id","p"}...]...],')
	println('"target_topk":[[{"id","p"}...]...]} with target_topk carrying one more')
	println('entry than draft (the bonus slot).')
}

fn read_input_lines(path string) []string {
	if path == '' {
		return os.get_raw_lines()
	}
	return os.read_lines(path) or {
		eprintln('cannot read ${path}: ${err}')
		exit(1)
	}
}

fn print_round_line(o RoundOutcome, s StreamStats) {
	outcome_word := if o.degenerate {
		'DEGENERATE'
	} else if o.used_bonus {
		'FULL_ACCEPT'
	} else {
		'PARTIAL_ACCEPT'
	}
	println('${o.stream_id} round=${o.round} k=${o.k} accepted=${o.accepted} ${outcome_word} final=${o.final_token_id} next_k=${s.k_recommend} ema=${s.accept_ema:.3f}')
}

fn print_summary(streams map[string]StreamStats, protocol_errors int) {
	println('')
	println('=== speculative decode summary ===')
	mut ids := streams.keys()
	ids.sort()
	mut total_rounds := 0
	mut total_drafted := i64(0)
	mut total_accepted := i64(0)
	mut total_degenerate := 0
	mut total_rollback := i64(0)
	for id in ids {
		s := streams[id]
		println('stream ${id}: rounds=${s.rounds} acceptance=${s.acceptance_rate() * 100.0:.1f}% effective_speedup=${s.effective_speedup():.2f}x bonus_rounds=${s.bonus_rounds} degenerate=${s.degenerate_events} kv_rollback_tokens=${s.rollback_tokens} kv_confirmed_len=${s.kv_confirmed_len} next_k=${s.k_recommend}')
		total_rounds += s.rounds
		total_drafted += s.drafted_total
		total_accepted += s.accepted_total
		total_degenerate += s.degenerate_events
		total_rollback += s.rollback_tokens
	}
	overall_rate := if total_drafted > 0 {
		f64(total_accepted) / f64(total_drafted) * 100.0
	} else {
		0.0
	}
	println('---')
	println('streams=${ids.len} rounds=${total_rounds} drafted=${total_drafted} accepted=${total_accepted} acceptance=${overall_rate:.1f}% degenerate_events=${total_degenerate} kv_rollback_tokens=${total_rollback} protocol_errors=${protocol_errors}')
}

fn main() {
	mut path := ''
	mut json_out := false
	mut strict := false
	mut initial_k := 4

	for a in os.args[1..] {
		if a == '-json' {
			json_out = true
		} else if a == '-strict' {
			strict = true
		} else if a.starts_with('-k=') {
			initial_k = a[3..].int()
		} else if a == '-h' || a == '-help' || a == '--help' {
			print_usage()
			return
		} else if a.starts_with('-') {
			eprintln('unknown flag: ${a}')
			exit(2)
		} else {
			path = a
		}
	}
	if initial_k < min_draft_k {
		initial_k = min_draft_k
	}
	if initial_k > max_draft_k {
		initial_k = max_draft_k
	}

	lines := read_input_lines(path)

	mut streams := map[string]StreamStats{}
	mut protocol_errors := 0
	mut line_no := 0

	for raw in lines {
		line_no++
		line := raw.trim_space()
		if line.len == 0 || line.starts_with('#') {
			continue
		}
		r := json.decode(SpecRound, line) or {
			eprintln('line ${line_no}: malformed JSON: ${err}')
			protocol_errors++
			continue
		}
		mut rng := new_rng(r.seed)
		outcome := verify_round(r, mut rng) or {
			eprintln('line ${line_no}: protocol error in stream "${r.stream_id}" round ${r.round}: ${err}')
			protocol_errors++
			continue
		}
		if r.stream_id !in streams {
			streams[r.stream_id] = new_stream_stats(initial_k)
		}
		mut st := streams[r.stream_id]
		st.record(outcome)
		streams[r.stream_id] = st

		if json_out {
			println(json.encode(&outcome))
		} else {
			print_round_line(outcome, st)
		}
	}

	print_summary(streams, protocol_errors)

	if strict && protocol_errors > 0 {
		exit(1)
	}
}
