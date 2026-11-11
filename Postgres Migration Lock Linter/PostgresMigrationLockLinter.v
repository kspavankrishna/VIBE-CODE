module main

import os
import strings

const tool_name = 'PostgresMigrationLockLinter'
const tool_version = '1.0.0'

const lm_none = 0
const lm_access_share = 1
const lm_row_share = 2
const lm_row_exclusive = 3
const lm_share_update_exclusive = 4
const lm_share = 5
const lm_share_row_exclusive = 6
const lm_exclusive = 7
const lm_access_exclusive = 8

const lock_mode_names = ['NONE', 'ACCESS SHARE', 'ROW SHARE', 'ROW EXCLUSIVE', 'SHARE UPDATE EXCLUSIVE',
	'SHARE', 'SHARE ROW EXCLUSIVE', 'EXCLUSIVE', 'ACCESS EXCLUSIVE']

const conflict_mask = [0, 256, 384, 480, 496, 472, 504, 508, 510]

const vol_none = 0
const vol_stable = 1
const vol_volatile = 2
const vol_unknown = 3

const volatile_funcs = ['random', 'random_normal', 'gen_random_uuid', 'gen_random_bytes', 'uuid_generate_v1',
	'uuid_generate_v1mc', 'uuid_generate_v4', 'uuid_generate_v7', 'uuidv4', 'uuidv7', 'clock_timestamp',
	'timeofday', 'nextval', 'setval', 'txid_current', 'pg_current_xact_id', 'setseed']

const stable_funcs = ['now', 'transaction_timestamp', 'statement_timestamp', 'current_setting', 'lower',
	'upper', 'coalesce', 'nullif', 'greatest', 'least', 'concat', 'jsonb_build_object', 'json_build_object',
	'jsonb_build_array', 'json_build_array', 'to_jsonb', 'to_json', 'array', 'cast', 'make_interval',
	'make_date', 'date_trunc', 'abs', 'round', 'trim', 'btrim', 'md5', 'interval', 'numeric', 'varchar',
	'char', 'character', 'timestamp', 'timestamptz', 'decimal', 'bit', 'row', 'jsonb', 'json', 'text']

const serial_types = ['serial', 'bigserial', 'smallserial', 'serial2', 'serial4', 'serial8']

const constraint_words = ['default', 'not', 'null', 'primary', 'unique', 'references', 'check', 'constraint',
	'generated', 'collate']

const control_words = ['begin', 'start', 'commit', 'end', 'rollback', 'abort', 'savepoint', 'release']

struct Rule {
	id      string
	slug    string
	summary string
}

const rules = [
	Rule{
		id:      'PGL001'
		slug:    'blocking-index-build'
		summary: 'CREATE INDEX without CONCURRENTLY holds SHARE on an existing table and blocks writes for the whole build'
	},
	Rule{
		id:      'PGL002'
		slug:    'no-transaction-statement'
		summary: 'A statement Postgres refuses to run inside a transaction block is inside one'
	},
	Rule{
		id:      'PGL003'
		slug:    'table-rewrite'
		summary: 'The statement rewrites every row of an existing table while holding ACCESS EXCLUSIVE'
	},
	Rule{
		id:      'PGL004'
		slug:    'validating-scan-under-lock'
		summary: 'A constraint or NOT NULL is proven by a full scan while a blocking lock is held'
	},
	Rule{
		id:      'PGL005'
		slug:    'constraint-builds-index'
		summary: 'ADD PRIMARY KEY, UNIQUE or EXCLUDE builds an index under ACCESS EXCLUSIVE'
	},
	Rule{
		id:      'PGL006'
		slug:    'lock-timeout'
		summary: 'A blocking lock is requested with no lock_timeout, or one above the configured limit'
	},
	Rule{
		id:      'PGL007'
		slug:    'lock-held-across-slow-step'
		summary: 'A slow step runs while the same transaction still holds a blocking lock from an earlier statement'
	},
	Rule{
		id:      'PGL008'
		slug:    'multi-table-exclusive'
		summary: 'One transaction holds ACCESS EXCLUSIVE on two or more existing tables'
	},
	Rule{
		id:      'PGL009'
		slug:    'in-migration-backfill'
		summary: 'UPDATE or DELETE on an existing table inside a schema migration'
	},
	Rule{
		id:      'PGL010'
		slug:    'concurrent-index-if-not-exists'
		summary: 'CREATE INDEX CONCURRENTLY IF NOT EXISTS silently keeps an INVALID index on retry'
	},
	Rule{
		id:      'PGL011'
		slug:    'breaking-change'
		summary: 'DROP or RENAME of a table or column that running application code may still use'
	},
	Rule{
		id:      'PGL012'
		slug:    'not-null-column-without-default'
		summary: 'ADD COLUMN NOT NULL without a default fails on any table that has rows'
	},
	Rule{
		id:      'PGL013'
		slug:    'blocking-maintenance'
		summary: 'DROP INDEX, REINDEX, REFRESH or DETACH without CONCURRENTLY on an existing object'
	},
	Rule{
		id:      'PGL014'
		slug:    'unknown-default-volatility'
		summary: 'ADD COLUMN default calls a function the linter cannot classify as stable or volatile'
	},
	Rule{
		id:      'PGL015'
		slug:    'new-enum-value-same-transaction'
		summary: 'An enum value is used in the same transaction that added it'
	},
	Rule{
		id:      'PGL016'
		slug:    'transaction-control'
		summary: 'BEGIN, COMMIT or SET LOCAL used where it does not do what the author expects'
	},
	Rule{
		id:      'PGL017'
		slug:    'partitioned-concurrent-index'
		summary: 'CREATE INDEX CONCURRENTLY on a partitioned parent, which Postgres rejects'
	},
	Rule{
		id:      'PGL018'
		slug:    'opaque-code'
		summary: 'DO blocks, CALL and data modifying CTEs are not analysed'
	},
]

enum TokKind {
	word
	quoted_ident
	string_lit
	number
	punct
	param
}

struct Token {
	kind TokKind
	text string
	line int
}

struct Statement {
	tokens   []Token
	line     int
	end_line int
	ignores  []string
	index    int
}

fn (st Statement) has_word(w string) bool {
	for t in st.tokens {
		if t.kind == .word && t.text == w {
			return true
		}
	}
	return false
}

struct ParsedFile {
mut:
	statements []Statement
	no_tx      bool
	errors     []string
}

struct Directive {
	ok    bool
	no_tx bool
	ids   []string
}

enum Severity {
	info
	warning
	danger
}

fn sev_label(s Severity) string {
	return match s {
		.info { 'info' }
		.warning { 'warning' }
		.danger { 'error' }
	}
}

enum TxKind {
	implicit
	explicit
	wrapper
}

fn tx_label(k TxKind) string {
	return match k {
		.implicit { 'single statement' }
		.explicit { 'BEGIN ... COMMIT' }
		.wrapper { 'runner transaction' }
	}
}

struct PgType {
	base string
	args []int
}

fn (t PgType) label() string {
	if t.args.len == 0 {
		return t.base
	}
	arg_text := t.args.map(it.str()).join(',')
	return '${t.base}(${arg_text})'
}

@[heap]
struct TableInfo {
mut:
	created_file     string
	partitioned      bool
	columns          map[string]PgType
	proven_not_null  map[string]bool
	pending_checks   map[string]string
	fk_refs          map[string]string
	validated_checks int
}

struct HeldLock {
	table    string
	mode     int
	line     int
	stmt     int
	existing bool
	why      string
}

struct TxState {
mut:
	active          bool
	kind            TxKind
	start_line      int
	start_ignores   []string
	held            map[string]HeldLock
	slow_steps      []string
	local_lt        int = -1
	new_enum_values map[string]int
}

struct TxSummary {
	file       string
	start_line int
	end_line   int
	kind       TxKind
	committed  bool
	locks      []HeldLock
	slow       []string
}

struct Finding {
	file      string
	file_idx  int
	seq       int
	line      int
	rule      string
	severity  Severity
	table     string
	mode      int
	statement string
	message   string
	hint      string
}

struct Config {
mut:
	pg_major               int = 16
	format                 string = 'text'
	fail_on                string = 'error'
	wrap                   string = 'auto'
	max_lock_timeout_ms    int    = 10000
	assume_lock_timeout_ms int    = -1
	disabled               []string
	files                  []string
	show_help              bool
	show_version           bool
	show_rules             bool
}

struct Linter {
	cfg Config
mut:
	tables          map[string]&TableInfo
	index_tables    map[string]string
	findings        []Finding
	suppressed      int
	txs             []TxSummary
	tx              TxState
	file            string
	file_idx        int
	session_lt      int = -1
	lt_checked_stmt int = -1
	statements      int
	seq             int
}

fn conflicts(a int, b int) bool {
	if a <= lm_none || b <= lm_none || a > lm_access_exclusive || b > lm_access_exclusive {
		return false
	}
	return (conflict_mask[a] & (1 << b)) != 0
}

fn blocks_writes(mode int) bool {
	return conflicts(mode, lm_row_exclusive)
}

fn lock_name(mode int) string {
	if mode < 0 || mode >= lock_mode_names.len {
		return 'UNKNOWN'
	}
	return lock_mode_names[mode]
}

fn blocked_traffic(mode int) string {
	if conflicts(mode, lm_access_share) {
		return 'reads and writes'
	}
	if conflicts(mode, lm_row_exclusive) {
		return 'writes'
	}
	if conflicts(mode, lm_share_update_exclusive) {
		return 'other schema changes, VACUUM and ANALYZE'
	}
	if mode > lm_none {
		return 'only explicit table locks'
	}
	return 'nothing'
}

fn combine_modes(a int, b int) int {
	if a == b {
		return a
	}
	if a <= lm_none {
		return b
	}
	if b <= lm_none {
		return a
	}
	want := conflict_mask[a] | conflict_mask[b]
	for m in 1 .. 9 {
		if (conflict_mask[m] & want) == want {
			return m
		}
	}
	return if a > b { a } else { b }
}

fn mode_from_words(words []string) int {
	return match words.join(' ') {
		'access share' { lm_access_share }
		'row share' { lm_row_share }
		'row exclusive' { lm_row_exclusive }
		'share update exclusive' { lm_share_update_exclusive }
		'share' { lm_share }
		'share row exclusive' { lm_share_row_exclusive }
		'exclusive' { lm_exclusive }
		else { lm_access_exclusive }
	}
}

fn is_ident_start(c u8) bool {
	return c.is_letter() || c == `_` || c >= 0x80
}

fn is_ident_part(c u8) bool {
	return c.is_letter() || c.is_digit() || c == `_` || c == `$` || c >= 0x80
}

fn is_tag_char(c u8) bool {
	return c.is_letter() || c.is_digit() || c == `_` || c >= 0x80
}

fn find_from(s string, needle string, start int) int {
	if needle.len == 0 {
		return start
	}
	mut i := start
	for i + needle.len <= s.len {
		if s[i] == needle[0] && s[i..i + needle.len] == needle {
			return i
		}
		i++
	}
	return -1
}

fn count_newlines(s string) int {
	mut n := 0
	for ch in s {
		if ch == `\n` {
			n++
		}
	}
	return n
}

fn parse_directive(body string) Directive {
	prefix := 'migration-lint:'
	if !body.to_lower().starts_with(prefix) {
		return Directive{}
	}
	words := body[prefix.len..].trim_space().fields()
	if words.len == 0 {
		return Directive{}
	}
	head := words[0].to_lower()
	if head == 'no-transaction' {
		return Directive{
			ok:    true
			no_tx: true
		}
	}
	if head == 'ignore' && words.len > 1 {
		mut ids := []string{}
		for part in words[1].split(',') {
			p := part.trim_space().to_upper()
			if p.len > 0 {
				ids << p
			}
		}
		return Directive{
			ok:  true
			ids: ids
		}
	}
	return Directive{}
}

fn lex_file(src string, file string) ParsedFile {
	mut pf := ParsedFile{}
	mut cur := []Token{}
	mut cur_ignores := []string{}
	mut pending := []string{}
	mut stmt_line := 0
	mut line := 1
	mut i := 0
	mut line_start := true
	mut in_atomic := false
	mut case_depth := 0
	mut index := 0
	n := src.len
	for i < n {
		ch := src[i]
		if ch == `\n` {
			line++
			i++
			line_start = true
			continue
		}
		if ch.is_space() {
			i++
			continue
		}
		if line_start && ch == `\\` && cur.len == 0 {
			for i < n && src[i] != `\n` {
				i++
			}
			continue
		}
		line_start = false
		if ch == `-` && i + 1 < n && src[i + 1] == `-` {
			mut j := i + 2
			for j < n && src[j] != `\n` {
				j++
			}
			d := parse_directive(src[i + 2..j].trim_space())
			if d.ok {
				if d.no_tx {
					pf.no_tx = true
				}
				if cur.len > 0 {
					cur_ignores << d.ids
				} else {
					pending << d.ids
				}
			}
			i = j
			continue
		}
		if ch == `/` && i + 1 < n && src[i + 1] == `*` {
			opened := line
			mut depth := 1
			mut j := i + 2
			for j < n && depth > 0 {
				if src[j] == `\n` {
					line++
					j++
				} else if src[j] == `/` && j + 1 < n && src[j + 1] == `*` {
					depth++
					j += 2
				} else if src[j] == `*` && j + 1 < n && src[j + 1] == `/` {
					depth--
					j += 2
				} else {
					j++
				}
			}
			if depth > 0 {
				pf.errors << '${file}:${opened}: unterminated block comment'
				return pf
			}
			i = j
			continue
		}
		tok_line := line
		mut kind := TokKind.punct
		mut text := ''
		is_prefixed := (ch == `e` || ch == `E` || ch == `b` || ch == `B` || ch == `x` || ch == `X`
			|| ch == `n` || ch == `N`) && i + 1 < n && src[i + 1] == `'`
		if ch == `'` || is_prefixed {
			escapes := ch == `e` || ch == `E`
			mut j := if ch == `'` { i + 1 } else { i + 2 }
			mut sb := strings.new_builder(32)
			mut closed := false
			for j < n {
				cj := src[j]
				if cj == `\n` {
					line++
				}
				if escapes && cj == `\\` && j + 1 < n {
					if src[j + 1] == `\n` {
						line++
					}
					sb.write_u8(src[j + 1])
					j += 2
					continue
				}
				if cj == `'` {
					if j + 1 < n && src[j + 1] == `'` {
						sb.write_u8(`'`)
						j += 2
						continue
					}
					closed = true
					j++
					break
				}
				sb.write_u8(cj)
				j++
			}
			if !closed {
				pf.errors << '${file}:${tok_line}: unterminated string literal'
				return pf
			}
			kind = .string_lit
			text = sb.str()
			i = j
		} else if ch == `$` && i + 1 < n && (src[i + 1] == `$` || is_ident_start(src[i + 1])) {
			mut j := i + 1
			for j < n && is_tag_char(src[j]) {
				j++
			}
			if j < n && src[j] == `$` {
				delim := src[i..j + 1]
				close_at := find_from(src, delim, j + 1)
				if close_at < 0 {
					pf.errors << '${file}:${tok_line}: unterminated dollar quoted string ${delim}'
					return pf
				}
				body := src[j + 1..close_at]
				line += count_newlines(body)
				kind = .string_lit
				text = body
				i = close_at + delim.len
			} else {
				kind = .punct
				text = src[i..i + 1]
				i++
			}
		} else if ch == `$` && i + 1 < n && src[i + 1].is_digit() {
			mut j := i + 1
			for j < n && src[j].is_digit() {
				j++
			}
			kind = .param
			text = src[i..j]
			i = j
		} else if ch == `"` {
			mut j := i + 1
			mut sb := strings.new_builder(32)
			mut closed := false
			for j < n {
				if src[j] == `"` {
					if j + 1 < n && src[j + 1] == `"` {
						sb.write_u8(`"`)
						j += 2
						continue
					}
					closed = true
					j++
					break
				}
				if src[j] == `\n` {
					line++
				}
				sb.write_u8(src[j])
				j++
			}
			if !closed {
				pf.errors << '${file}:${tok_line}: unterminated quoted identifier'
				return pf
			}
			kind = .quoted_ident
			text = sb.str()
			i = j
		} else if is_ident_start(ch) {
			mut j := i + 1
			for j < n && is_ident_part(src[j]) {
				j++
			}
			kind = .word
			text = src[i..j].to_lower()
			i = j
		} else if ch.is_digit() || (ch == `.` && i + 1 < n && src[i + 1].is_digit()) {
			mut j := i
			for j < n && (src[j].is_alnum() || src[j] == `.` || src[j] == `_`) {
				j++
			}
			kind = .number
			text = src[i..j].to_lower()
			i = j
		} else {
			kind = .punct
			if ch == `:` && i + 1 < n && src[i + 1] == `:` {
				text = '::'
				i += 2
			} else {
				text = src[i..i + 1]
				i++
			}
		}
		if kind == .punct && text == ';' && !in_atomic {
			if cur.len > 0 {
				pf.statements << Statement{
					tokens:   cur
					line:     stmt_line
					end_line: line
					ignores:  cur_ignores
					index:    index
				}
				index++
				cur = []Token{}
				cur_ignores = []string{}
			}
			case_depth = 0
			continue
		}
		if cur.len == 0 {
			stmt_line = tok_line
			cur_ignores = pending.clone()
			pending = []string{}
		}
		if kind == .word {
			if text == 'atomic' && cur.len > 1 && cur.last().kind == .word && cur.last().text == 'begin'
				&& cur[0].text == 'create' {
				in_atomic = true
			} else if in_atomic && text == 'case' {
				case_depth++
			} else if in_atomic && text == 'end' {
				if case_depth > 0 {
					case_depth--
				} else {
					in_atomic = false
				}
			}
		}
		cur << Token{
			kind: kind
			text: text
			line: tok_line
		}
	}
	if in_atomic {
		pf.errors << '${file}:${stmt_line}: BEGIN ATOMIC body has no matching END'
		return pf
	}
	if cur.len > 0 {
		pf.statements << Statement{
			tokens:   cur
			line:     stmt_line
			end_line: line
			ignores:  cur_ignores
			index:    index
		}
	}
	return pf
}

struct Cur {
	toks []Token
mut:
	pos int
}

fn (c &Cur) at(off int) Token {
	idx := c.pos + off
	if idx < 0 || idx >= c.toks.len {
		return Token{
			kind: .punct
			text: ''
		}
	}
	return c.toks[idx]
}

fn (c &Cur) done() bool {
	return c.pos >= c.toks.len
}

fn (c &Cur) kw(w string) bool {
	t := c.at(0)
	return t.kind == .word && t.text == w
}

fn (c &Cur) kw_at(off int, w string) bool {
	t := c.at(off)
	return t.kind == .word && t.text == w
}

fn (c &Cur) punct(p string) bool {
	t := c.at(0)
	return t.kind == .punct && t.text == p
}

fn (mut c Cur) eat(w string) bool {
	if c.kw(w) {
		c.pos++
		return true
	}
	return false
}

fn (mut c Cur) eat_seq(ws []string) bool {
	if !matches_at(c.toks, c.pos, ws) {
		return false
	}
	c.pos += ws.len
	return true
}

fn (mut c Cur) eat_punct(p string) bool {
	if c.punct(p) {
		c.pos++
		return true
	}
	return false
}

fn (mut c Cur) name() string {
	mut parts := []string{}
	for {
		t := c.at(0)
		if t.kind != .word && t.kind != .quoted_ident {
			break
		}
		parts << t.text
		c.pos++
		nxt := c.at(1)
		if c.punct('.') && (nxt.kind == .word || nxt.kind == .quoted_ident) {
			c.pos++
			continue
		}
		break
	}
	if parts.len > 1 && parts[0] == 'public' {
		parts.delete(0)
	}
	return parts.join('.')
}

fn (mut c Cur) name_list() []string {
	mut out := []string{}
	for {
		nm := c.name()
		if nm == '' {
			break
		}
		out << nm
		c.eat_punct('*')
		if !c.eat_punct(',') {
			break
		}
	}
	return out
}

fn (mut c Cur) take_group() []Token {
	if !c.punct('(') {
		return []Token{}
	}
	start := c.pos + 1
	mut depth := 0
	for c.pos < c.toks.len {
		t := c.toks[c.pos]
		c.pos++
		if t.kind == .punct {
			if t.text == '(' {
				depth++
			} else if t.text == ')' {
				depth--
				if depth == 0 {
					return c.toks[start..c.pos - 1]
				}
			}
		}
	}
	return c.toks[start..]
}

fn (c &Cur) rest() []Token {
	if c.pos >= c.toks.len {
		return []Token{}
	}
	return c.toks[c.pos..]
}

fn matches_at(toks []Token, i int, ws []string) bool {
	if i < 0 || i + ws.len > toks.len {
		return false
	}
	for k, w in ws {
		tk := toks[i + k]
		if tk.kind != .word || tk.text != w {
			return false
		}
	}
	return true
}

fn depth_step(t Token) int {
	if t.kind != .punct {
		return 0
	}
	if t.text == '(' || t.text == '[' {
		return 1
	}
	if t.text == ')' || t.text == ']' {
		return -1
	}
	return 0
}

fn index_of_word_top(toks []Token, w string) int {
	mut depth := 0
	for i, t in toks {
		step := depth_step(t)
		if step != 0 {
			depth += step
			continue
		}
		if depth == 0 && t.kind == .word && t.text == w {
			return i
		}
	}
	return -1
}

fn has_word_top(toks []Token, w string) bool {
	return index_of_word_top(toks, w) >= 0
}

fn has_word_any(toks []Token, w string) bool {
	for t in toks {
		if t.kind == .word && t.text == w {
			return true
		}
	}
	return false
}

fn has_seq_top(toks []Token, ws []string) bool {
	mut depth := 0
	for i, t in toks {
		step := depth_step(t)
		if step != 0 {
			depth += step
			continue
		}
		if depth == 0 && matches_at(toks, i, ws) {
			return true
		}
	}
	return false
}

fn split_top(toks []Token) [][]Token {
	mut out := [][]Token{}
	mut depth := 0
	mut start := 0
	for i, t in toks {
		step := depth_step(t)
		if step != 0 {
			depth += step
			continue
		}
		if t.kind == .punct && t.text == ',' && depth == 0 {
			out << toks[start..i]
			start = i + 1
		}
	}
	out << toks[start..]
	return out
}

fn split_column_def(toks []Token) ([]Token, []Token) {
	mut depth := 0
	for i, t in toks {
		step := depth_step(t)
		if step != 0 {
			depth += step
			continue
		}
		if depth == 0 && t.kind == .word && t.text in constraint_words {
			return toks[..i], toks[i..]
		}
	}
	return toks, []Token{}
}

fn default_expr(cons []Token) []Token {
	mut depth := 0
	mut start := -1
	for i, t in cons {
		step := depth_step(t)
		if step != 0 {
			depth += step
			continue
		}
		if depth != 0 || t.kind != .word {
			continue
		}
		if start < 0 {
			if t.text == 'default' {
				start = i + 1
			}
		} else if i > start && t.text in constraint_words {
			return cons[start..i]
		}
	}
	if start < 0 || start >= cons.len {
		return []Token{}
	}
	return cons[start..]
}

fn default_volatility(expr []Token) (int, string) {
	mut unknown := ''
	mut saw_call := false
	for i, t in expr {
		if t.kind != .word {
			continue
		}
		next_is_paren := i + 1 < expr.len && expr[i + 1].kind == .punct && expr[i + 1].text == '('
		if !next_is_paren {
			continue
		}
		if i > 0 {
			prev := expr[i - 1]
			if (prev.kind == .punct && prev.text == '::') || (prev.kind == .word && prev.text == 'as') {
				continue
			}
		}
		saw_call = true
		if t.text in volatile_funcs {
			return vol_volatile, t.text
		}
		if t.text !in stable_funcs && unknown == '' {
			unknown = t.text
		}
	}
	if unknown != '' {
		return vol_unknown, unknown
	}
	if saw_call {
		return vol_stable, ''
	}
	return vol_none, ''
}

fn normalize_type(raw string) string {
	return match raw {
		'character varying', 'varchar' { 'varchar' }
		'character', 'char', 'bpchar' { 'char' }
		'int', 'integer', 'int4' { 'int4' }
		'bigint', 'int8' { 'int8' }
		'smallint', 'int2' { 'int2' }
		'timestamp', 'timestamp without time zone' { 'timestamp' }
		'timestamptz', 'timestamp with time zone' { 'timestamptz' }
		'time', 'time without time zone' { 'time' }
		'timetz', 'time with time zone' { 'timetz' }
		'decimal', 'numeric' { 'numeric' }
		'bool', 'boolean' { 'bool' }
		'double precision', 'float8', 'float' { 'float8' }
		'real', 'float4' { 'float4' }
		'bit varying', 'varbit' { 'varbit' }
		else { raw }
	}
}

fn parse_type(toks []Token) PgType {
	mut words := []string{}
	mut args := []int{}
	mut depth := 0
	mut is_array := false
	for t in toks {
		if t.kind == .punct {
			if t.text == '(' {
				depth++
			} else if t.text == ')' {
				depth--
			} else if t.text == '[' && depth == 0 {
				is_array = true
			} else if t.text == '.' && depth == 0 {
				words.clear()
			}
			continue
		}
		if depth > 0 {
			if t.kind == .number {
				args << t.text.int()
			}
			continue
		}
		if t.kind == .word || t.kind == .quoted_ident {
			if t.text == 'collate' || t.text == 'using' {
				break
			}
			if t.text == 'array' {
				is_array = true
				continue
			}
			words << t.text
		}
	}
	mut base := normalize_type(words.join(' '))
	if is_array {
		base += '[]'
	}
	return PgType{
		base: base
		args: args
	}
}

fn type_change_verdict(old PgType, has_old bool, new_ty PgType, has_using bool, pg int) (int, string) {
	o := old.base
	nb := new_ty.base
	if has_using {
		return 2, 'the USING clause makes Postgres recompute every value, which rewrites the table and rebuilds its indexes'
	}
	if !has_old {
		if nb == 'text' || (nb in ['varchar', 'numeric', 'varbit'] && new_ty.args.len == 0) {
			return 1, 'the linter has not seen the current type of this column. A change to ${new_ty.label()} skips the rewrite only when the column is already varchar, text or ${nb} of the same family'
		}
		if nb == 'varchar' {
			return 1, 'the linter has not seen the current type of this column. A change to ${new_ty.label()} skips the rewrite only when it widens an existing varchar'
		}
		return 2, 'the linter has not seen the current type of this column, and a change to ${new_ty.label()} rewrites the table and rebuilds its indexes unless the old type is binary coercible to it'
	}
	if o == nb && old.args == new_ty.args {
		return 0, ''
	}
	if o in ['varchar', 'text'] && (nb == 'text' || (nb == 'varchar' && new_ty.args.len == 0)) {
		return 0, ''
	}
	if (o == 'varchar' && nb == 'varchar') || (o == 'varbit' && nb == 'varbit') {
		if old.args.len > 0 && new_ty.args.len > 0 && new_ty.args[0] >= old.args[0] {
			return 0, ''
		}
		return 2, 'shrinking the length limit from ${old.label()} to ${new_ty.label()} makes Postgres check and rewrite every row'
	}
	if o == 'text' && nb == 'varchar' {
		return 2, 'adding a length limit to a text column makes Postgres check and rewrite every row'
	}
	if o == 'numeric' && nb == 'numeric' {
		if new_ty.args.len == 0 {
			return 0, ''
		}
		if old.args.len > 0 {
			old_scale := if old.args.len > 1 { old.args[1] } else { 0 }
			new_scale := if new_ty.args.len > 1 { new_ty.args[1] } else { 0 }
			if new_ty.args[0] >= old.args[0] && new_scale == old_scale {
				return 0, ''
			}
		}
		return 2, 'changing numeric precision or scale from ${old.label()} to ${new_ty.label()} rewrites every row'
	}
	if o == 'cidr' && nb == 'inet' {
		return 0, ''
	}
	if o == 'timestamp' && nb == 'timestamptz' {
		if pg >= 12 {
			return 1, 'Postgres 12 and later skip the rewrite only when the session TimeZone is UTC. Under any other zone every row is rewritten'
		}
		return 2, 'before Postgres 12 timestamp to timestamptz always rewrites the table'
	}
	if (o == 'int2' && nb in ['int4', 'int8']) || (o == 'int4' && nb == 'int8') {
		return 2, '${o} to ${nb} rewrites the whole table and rebuilds every index on it. On a large table this is the classic multi hour outage'
	}
	return 2, '${old.label()} to ${new_ty.label()} is not binary coercible, so Postgres rewrites the table and rebuilds its indexes'
}

fn not_null_check_column(expr []Token) string {
	mut toks := expr.clone()
	for toks.len >= 2 && toks[0].kind == .punct && toks[0].text == '('
		&& toks[toks.len - 1].kind == .punct && toks[toks.len - 1].text == ')' {
		toks = toks[1..toks.len - 1].clone()
	}
	if toks.len == 4 && (toks[0].kind == .word || toks[0].kind == .quoted_ident)
		&& matches_at(toks, 1, ['is', 'not', 'null']) {
		return toks[0].text
	}
	return ''
}

fn parse_duration_ms(raw string) int {
	s := raw.trim_space().to_lower()
	if s.len == 0 {
		return -2
	}
	mut j := 0
	for j < s.len && (s[j].is_digit() || s[j] == `.`) {
		j++
	}
	if j == 0 {
		return -2
	}
	num := s[..j].f64()
	unit := s[j..].trim_space()
	mult := match unit {
		'', 'ms' { 1.0 }
		'us' { 0.001 }
		's' { 1000.0 }
		'min' { 60000.0 }
		'h' { 3600000.0 }
		'd' { 86400000.0 }
		else { -1.0 }
	}
	if mult < 0 {
		return -2
	}
	ms := int(num * mult)
	if ms == 0 && num > 0 {
		return 1
	}
	return ms
}

fn snippet(toks []Token, max int) string {
	mut sb := strings.new_builder(128)
	mut prev := Token{
		kind: .punct
		text: '('
	}
	for i, t in toks {
		mut piece := t.text
		if t.kind == .string_lit {
			body := t.text.replace('\n', ' ').replace("'", "''")
			piece = if body.len > 40 { "'" + body[..37] + "...'" } else { "'" + body + "'" }
		} else if t.kind == .quoted_ident {
			piece = '"' + t.text + '"'
		}
		tight_before := t.kind == .punct && t.text in [',', ')', '.', '::', ']', ';']
		tight_after := prev.kind == .punct && prev.text in ['(', '.', '::', '[']
		call := t.kind == .punct && t.text == '(' && prev.kind == .word
		if i > 0 && !tight_before && !tight_after && !call {
			sb.write_u8(` `)
		}
		sb.write_string(piece)
		prev = t
	}
	out := sb.str()
	if out.len > max {
		return out[..max - 3] + '...'
	}
	return out
}

fn (l &Linter) is_new(table string) bool {
	ti := l.tables[table] or { return false }
	return ti.created_file == l.file
}

fn (l &Linter) in_block() bool {
	return l.tx.active && l.tx.kind != .implicit
}

fn (l &Linter) effective_lock_timeout() int {
	if l.tx.local_lt >= 0 {
		return l.tx.local_lt
	}
	if l.session_lt >= 0 {
		return l.session_lt
	}
	return l.cfg.assume_lock_timeout_ms
}

fn (mut l Linter) tinfo(table string) &TableInfo {
	if table in l.tables {
		return l.tables[table] or { &TableInfo{} }
	}
	ti := &TableInfo{}
	l.tables[table] = ti
	return ti
}

fn (mut l Linter) register_new(table string) &TableInfo {
	ti := &TableInfo{
		created_file: l.file
	}
	l.tables[table] = ti
	return ti
}

fn (mut l Linter) report(line int, ignores []string, rule string, sev Severity, table string, mode int, stmt string, msg string, hint string) {
	if rule in l.cfg.disabled {
		return
	}
	if rule in ignores || 'ALL' in ignores {
		l.suppressed++
		return
	}
	l.seq++
	l.findings << Finding{
		file:      l.file
		file_idx:  l.file_idx
		seq:       l.seq
		line:      line
		rule:      rule
		severity:  sev
		table:     table
		mode:      mode
		statement: stmt
		message:   msg
		hint:      hint
	}
}

fn (mut l Linter) flag(st Statement, rule string, sev Severity, table string, mode int, msg string, hint string) {
	l.report(st.line, st.ignores, rule, sev, table, mode, snippet(st.tokens, 160), msg, hint)
}

fn (mut l Linter) acquire(st Statement, table string, mode int, why string) {
	if table == '' || mode <= lm_none {
		return
	}
	existing := !l.is_new(table)
	mut final_mode := mode
	if table in l.tx.held {
		prev := l.tx.held[table]
		combined := combine_modes(prev.mode, mode)
		if combined == prev.mode {
			return
		}
		final_mode = combined
	}
	l.tx.held[table] = HeldLock{
		table:    table
		mode:     final_mode
		line:     st.line
		stmt:     st.index
		existing: existing
		why:      why
	}
	if !existing || !blocks_writes(mode) || l.lt_checked_stmt == st.index {
		return
	}
	l.lt_checked_stmt = st.index
	if st.has_word('nowait') {
		return
	}
	eff := l.effective_lock_timeout()
	traffic := blocked_traffic(mode)
	if eff <= 0 {
		l.flag(st, 'PGL006', .warning, table, mode, '${why} needs ${lock_name(mode)} on ${table} and no lock_timeout is set. If a long query or an idle transaction already holds any conflicting lock on ${table}, this statement waits in the lock queue and every new query on ${table} queues behind it, so a change that takes milliseconds can stall all ${traffic} on ${table} until the blocker ends.',
			"Run SET lock_timeout = '2s' before it (SET LOCAL inside a transaction) and retry the migration on timeout. Pass -assume-lock-timeout if your runner or role already sets one.")
	} else if eff > l.cfg.max_lock_timeout_ms {
		l.flag(st, 'PGL006', .warning, table, mode, 'lock_timeout is ${eff}ms, above the ${l.cfg.max_lock_timeout_ms}ms limit. While ${why} waits for ${lock_name(mode)} on ${table}, all ${traffic} on ${table} queue behind it for up to that long.',
			'Lower lock_timeout to a few seconds and retry on timeout instead of waiting.')
	}
}

fn (mut l Linter) slow_step(st Statement, table string, what string) {
	if table == '' || l.is_new(table) {
		return
	}
	l.tx.slow_steps << '${what} on ${table} (line ${st.line})'
	if !l.in_block() {
		return
	}
	mut culprits := []string{}
	mut same_table := false
	for _, h in l.tx.held {
		if h.stmt != st.index && h.existing && blocks_writes(h.mode) {
			culprits << '${lock_name(h.mode)} on ${h.table} from line ${h.line}'
			if h.table == table {
				same_table = true
			}
		}
	}
	if culprits.len == 0 {
		return
	}
	hint := if same_table {
		'Commit between the cheap step and this one. For ADD ... NOT VALID followed by VALIDATE, the whole point is that VALIDATE runs in its own transaction with only SHARE UPDATE EXCLUSIVE.'
	} else {
		'Move this step into its own migration or transaction so the earlier locks are released before it starts.'
	}
	culprit_text := culprits.join(', ')
	l.flag(st, 'PGL007', .danger, table, 0, 'The ${what} on ${table} runs while this transaction still holds ${culprit_text}. Those locks were cheap when taken, but now they last as long as the ${what} and every conflicting query waits until commit.',
		hint)
}

fn (mut l Linter) open_tx(line int, kind TxKind, ignores []string) {
	l.tx = TxState{
		active:        true
		kind:          kind
		start_line:    line
		start_ignores: ignores
	}
}

fn (mut l Linter) close_tx(end_line int, committed bool) {
	mut exclusive_tables := []string{}
	mut blocking := []HeldLock{}
	for _, h in l.tx.held {
		if !h.existing {
			continue
		}
		if h.mode == lm_access_exclusive {
			exclusive_tables << h.table
		}
		if blocks_writes(h.mode) {
			blocking << h
		}
	}
	if exclusive_tables.len >= 2 && l.tx.kind != .implicit {
		table_list := exclusive_tables.join(', ')
		l.report(l.tx.start_line, l.tx.start_ignores, 'PGL008', .warning, table_list,
			lm_access_exclusive, '', 'The transaction starting at line ${l.tx.start_line} holds ACCESS EXCLUSIVE on ${exclusive_tables.len} existing tables (${table_list}) until it ends. Reads and writes on all of them stop together, and application transactions that touch them in a different order can deadlock against the migration.',
			'Split the changes into one transaction per table, or make sure every lock is short and lock_timeout is set.')
	}
	if blocking.len > 0 {
		l.txs << TxSummary{
			file:       l.file
			start_line: l.tx.start_line
			end_line:   end_line
			kind:       l.tx.kind
			committed:  committed
			locks:      blocking
			slow:       l.tx.slow_steps.clone()
		}
	}
	l.tx = TxState{}
}

fn (mut l Linter) lint_file(file string, file_idx int, pf ParsedFile) {
	l.file = file
	l.file_idx = file_idx
	l.tx = TxState{}
	l.session_lt = -1
	l.lt_checked_stmt = -1
	mut has_explicit := false
	for st in pf.statements {
		if st.tokens.len > 0 && st.tokens[0].kind == .word
			&& (st.tokens[0].text == 'begin' || matches_at(st.tokens, 0, ['start', 'transaction'])) {
			has_explicit = true
			break
		}
	}
	mode := if pf.no_tx {
		'none'
	} else if l.cfg.wrap == 'auto' {
		if has_explicit { 'none' } else { 'file' }
	} else {
		l.cfg.wrap
	}
	if mode == 'file' && pf.statements.len > 0 {
		l.open_tx(pf.statements[0].line, .wrapper, []string{})
	}
	mut last_line := 1
	for st in pf.statements {
		l.statements++
		l.process(st)
		last_line = st.end_line
	}
	if l.tx.active {
		if l.tx.kind == .explicit {
			l.report(l.tx.start_line, l.tx.start_ignores, 'PGL016', .warning, '', 0, '', 'The transaction opened at line ${l.tx.start_line} is never committed in this file. psql rolls it back at exit and most runners report success anyway.',
				'End the file with COMMIT.')
		}
		l.close_tx(last_line, l.tx.kind != .explicit)
	}
}

fn (mut l Linter) process(st Statement) {
	if st.tokens.len == 0 {
		return
	}
	first := st.tokens[0]
	is_control := first.kind == .word && first.text in control_words
	mut implicit := false
	if !l.tx.active && !is_control {
		l.open_tx(st.line, .implicit, st.ignores)
		implicit = true
	}
	if l.in_block() && l.tx.new_enum_values.len > 0 && !matches_at(st.tokens, 0, ['alter', 'type']) {
		for t in st.tokens {
			if t.kind == .string_lit && t.text in l.tx.new_enum_values {
				added := l.tx.new_enum_values[t.text]
				l.flag(st, 'PGL015', .danger, '', 0, "This statement uses the enum value '${t.text}' that ALTER TYPE added at line ${added} in the same transaction. Postgres rejects it with 'unsafe use of new value' until that ALTER TYPE commits.",
					'Commit after ALTER TYPE ... ADD VALUE and use the new value in a later migration or transaction.')
				break
			}
		}
	}
	l.dispatch(st)
	if implicit && l.tx.active && l.tx.kind == .implicit {
		l.close_tx(st.end_line, true)
	}
}

fn (mut l Linter) dispatch(st Statement) {
	mut c := Cur{
		toks: st.tokens
	}
	head := st.tokens[0].text
	if st.tokens[0].kind != .word {
		return
	}
	match head {
		'begin' {
			l.begin_tx(st)
		}
		'start' {
			if c.kw_at(1, 'transaction') {
				l.begin_tx(st)
			}
		}
		'commit', 'end' {
			l.commit_tx(st)
		}
		'rollback', 'abort' {
			if !st.has_word('to') && l.tx.active {
				l.close_tx(st.line, false)
			}
		}
		'set' {
			l.handle_set(mut c, st)
		}
		'reset' {
			if c.kw_at(1, 'lock_timeout') || c.kw_at(1, 'all') {
				l.session_lt = -1
			}
		}
		'select' {
			l.handle_select(st)
		}
		'create' {
			l.handle_create(mut c, st)
		}
		'alter' {
			l.handle_alter(mut c, st)
		}
		'drop' {
			l.handle_drop(mut c, st)
		}
		'reindex' {
			l.handle_reindex(mut c, st)
		}
		'cluster' {
			l.handle_cluster(mut c, st)
		}
		'vacuum' {
			l.handle_vacuum(mut c, st)
		}
		'refresh' {
			l.handle_refresh(mut c, st)
		}
		'truncate' {
			l.handle_truncate(mut c, st)
		}
		'lock' {
			l.handle_lock(mut c, st)
		}
		'update', 'delete' {
			l.handle_dml(mut c, st)
		}
		'insert' {
			l.handle_insert(mut c, st)
		}
		'do', 'call' {
			l.flag(st, 'PGL018', .info, '', 0, '${head.to_upper()} runs procedural code the linter cannot see into. It can take any lock and run for any length of time.',
				'Keep schema changes in plain SQL statements so they can be checked, or review this block by hand.')
		}
		'with' {
			if has_word_any(st.tokens, 'update') || has_word_any(st.tokens, 'delete')
				|| has_word_any(st.tokens, 'insert') {
				l.flag(st, 'PGL018', .info, '', 0, 'This WITH statement modifies data inside a CTE, which the linter does not analyse.',
					'Review the row count and lock footprint of this statement by hand.')
			}
		}
		else {}
	}
}

fn (mut l Linter) begin_tx(st Statement) {
	if l.tx.active {
		msg := if l.tx.kind == .wrapper {
			'BEGIN inside a migration that the runner already wraps in a transaction. Postgres ignores it with a warning, and the matching COMMIT ends the runner transaction early, so later statements run outside any transaction.'
		} else {
			'BEGIN while a transaction is already open. Postgres ignores it with a warning.'
		}
		l.flag(st, 'PGL016', .warning, '', 0, msg, 'Remove the inner BEGIN and COMMIT, or mark the file with -- migration-lint: no-transaction and disable the runner transaction for it.')
		return
	}
	l.open_tx(st.line, .explicit, st.ignores)
}

fn (mut l Linter) commit_tx(st Statement) {
	if !l.tx.active {
		l.flag(st, 'PGL016', .warning, '', 0, 'COMMIT with no open transaction. Postgres only warns, but it usually means the author expected an earlier BEGIN that is missing.',
			'Add the missing BEGIN or remove the COMMIT.')
		return
	}
	l.close_tx(st.line, true)
}

fn (mut l Linter) apply_lock_timeout(st Statement, ms int, is_local bool) {
	if is_local {
		if l.tx.kind == .implicit {
			l.flag(st, 'PGL016', .warning, '', 0, 'SET LOCAL lock_timeout outside a transaction block lasts only for this one statement, so it protects nothing that follows.',
				'Use plain SET, or put SET LOCAL after BEGIN.')
			return
		}
		l.tx.local_lt = ms
	} else {
		l.session_lt = ms
	}
}

fn (mut l Linter) handle_set(mut c Cur, st Statement) {
	c.eat('set')
	is_local := c.eat('local')
	c.eat('session')
	if !c.eat('lock_timeout') {
		return
	}
	if !c.eat('to') {
		c.eat_punct('=')
	}
	vt := c.at(0)
	if vt.kind == .word && vt.text == 'default' {
		l.apply_lock_timeout(st, -1, is_local)
		return
	}
	ms := parse_duration_ms(vt.text)
	if ms == -2 {
		l.flag(st, 'PGL016', .warning, '', 0, "The lock_timeout value '${vt.text}' is not a duration the linter understands, so it is ignored.",
			"Write it as a quoted duration such as '2s' or '500ms'.")
		return
	}
	l.apply_lock_timeout(st, ms, is_local)
}

fn (mut l Linter) handle_select(st Statement) {
	if !st.has_word('set_config') {
		return
	}
	strs := st.tokens.filter(it.kind == .string_lit)
	if strs.len >= 2 && strs[0].text.to_lower() == 'lock_timeout' {
		ms := parse_duration_ms(strs[1].text)
		if ms >= -1 {
			l.apply_lock_timeout(st, ms, st.has_word('true'))
		}
	}
}

fn (mut l Linter) handle_create(mut c Cur, st Statement) {
	c.eat('create')
	c.eat_seq(['or', 'replace'])
	unique := c.eat('unique')
	if c.eat('index') {
		l.create_index(mut c, st, unique)
		return
	}
	for c.eat('global') || c.eat('local') || c.eat('temp') || c.eat('temporary') || c.eat('unlogged') {
	}
	if c.eat('table') {
		l.create_table(mut c, st)
		return
	}
	if c.eat_seq(['materialized', 'view']) {
		c.eat_seq(['if', 'not', 'exists'])
		l.register_new(c.name())
		return
	}
	c.eat('constraint')
	if c.eat('trigger') || c.eat('policy') || c.eat('rule') {
		kind_word := c.at(-1).text
		idx := index_of_word_top(c.rest(), 'on')
		if idx < 0 {
			return
		}
		mut rc := Cur{
			toks: c.rest()
			pos:  idx + 1
		}
		table := rc.name()
		mode := if kind_word == 'trigger' { lm_share_row_exclusive } else { lm_access_exclusive }
		l.acquire(st, table, mode, 'CREATE ${kind_word.to_upper()}')
	}
}

fn (mut l Linter) create_index(mut c Cur, st Statement, unique bool) {
	concurrently := c.eat('concurrently')
	if_not_exists := c.eat_seq(['if', 'not', 'exists'])
	mut index_name := ''
	if !c.kw('on') {
		index_name = c.name()
	}
	if !c.eat('on') {
		return
	}
	only := c.eat('only')
	table := c.name()
	if table == '' {
		return
	}
	if index_name != '' {
		l.index_tables[index_name] = table
	}
	ti := l.tinfo(table)
	label := if unique { 'CREATE UNIQUE INDEX' } else { 'CREATE INDEX' }
	if concurrently {
		if l.in_block() {
			l.flag(st, 'PGL002', .danger, table, lm_share_update_exclusive, '${label} CONCURRENTLY cannot run inside a transaction block. Postgres aborts the migration with an error the moment it reaches this line.',
				'Put it in its own migration with -- migration-lint: no-transaction and turn off the runner transaction for that file (Rails disable_ddl_transaction!, Django atomic = False, Flyway executeInTransaction=false).')
		}
		if ti.partitioned && !only {
			l.flag(st, 'PGL017', .danger, table, lm_share_update_exclusive, '${table} is partitioned and Postgres rejects CREATE INDEX CONCURRENTLY on a partitioned parent.',
				'Run CREATE INDEX ON ONLY ${table} (cheap, leaves it invalid), build each partition index CONCURRENTLY, then ALTER INDEX ... ATTACH PARTITION for each one. The parent index turns valid once every partition is attached.')
		}
		if if_not_exists && index_name != '' {
			l.flag(st, 'PGL010', .warning, table, lm_share_update_exclusive, 'If this build fails or is cancelled, Postgres leaves ${index_name} behind as an INVALID index. On retry IF NOT EXISTS sees the name and skips the build, so the migration reports success while queries never use the index and writes still pay to maintain it.',
				'Drop IF NOT EXISTS, or precede the build with DROP INDEX CONCURRENTLY IF EXISTS ${index_name} so a retry starts clean.')
		}
		l.acquire(st, table, lm_share_update_exclusive, '${label} CONCURRENTLY')
		return
	}
	l.acquire(st, table, lm_share, label)
	if l.is_new(table) || (only && ti.partitioned) {
		return
	}
	l.flag(st, 'PGL001', .danger, table, lm_share, '${label} on ${table} holds SHARE for the entire build. Reads keep working, but every INSERT, UPDATE and DELETE on ${table} waits until the index is finished, which is minutes to hours on a large table.',
		'Use ${label} CONCURRENTLY in a migration that runs outside a transaction.')
	l.slow_step(st, table, 'index build')
}

fn (mut l Linter) create_table(mut c Cur, st Statement) {
	c.eat_seq(['if', 'not', 'exists'])
	table := c.name()
	if table == '' {
		return
	}
	mut ti := l.register_new(table)
	if c.eat_seq(['partition', 'of']) {
		parent := c.name()
		l.acquire(st, parent, lm_access_exclusive, 'CREATE TABLE ... PARTITION OF')
	}
	if c.punct('(') {
		group := c.take_group()
		for item in split_top(group) {
			l.table_element(st, table, item)
		}
	}
	if has_seq_top(c.rest(), ['partition', 'by']) {
		ti.partitioned = true
	}
	l.acquire(st, table, lm_access_exclusive, 'CREATE TABLE')
}

fn (mut l Linter) table_element(st Statement, table string, toks []Token) {
	if toks.len == 0 {
		return
	}
	mut ti := l.tinfo(table)
	first := toks[0]
	if first.kind == .word
		&& first.text in ['constraint', 'primary', 'unique', 'check', 'foreign', 'exclude', 'like'] {
		ridx := index_of_word_top(toks, 'references')
		if ridx >= 0 {
			mut rc := Cur{
				toks: toks
				pos:  ridx + 1
			}
			l.acquire(st, rc.name(), lm_share_row_exclusive, 'FOREIGN KEY reference')
		}
		pidx := index_of_word_top(toks, 'primary')
		if pidx >= 0 {
			mut pc := Cur{
				toks: toks
				pos:  pidx + 2
			}
			for col_tok in pc.take_group() {
				if col_tok.kind == .word || col_tok.kind == .quoted_ident {
					ti.proven_not_null[col_tok.text] = true
				}
			}
		}
		return
	}
	col := first.text
	type_toks, cons := split_column_def(toks[1..])
	ti.columns[col] = parse_type(type_toks)
	if has_seq_top(cons, ['not', 'null']) || has_word_top(cons, 'primary') {
		ti.proven_not_null[col] = true
	}
	ridx := index_of_word_top(cons, 'references')
	if ridx >= 0 {
		mut rc := Cur{
			toks: cons
			pos:  ridx + 1
		}
		l.acquire(st, rc.name(), lm_share_row_exclusive, 'REFERENCES')
	}
}

fn (mut l Linter) handle_alter(mut c Cur, st Statement) {
	c.eat('alter')
	if c.eat('table') {
		if c.kw('all') {
			return
		}
		c.eat_seq(['if', 'exists'])
		c.eat('only')
		table := c.name()
		if table == '' {
			return
		}
		c.eat_punct('*')
		for sub in split_top(c.rest()) {
			l.alter_table_cmd(st, table, sub)
		}
		return
	}
	if c.eat('type') {
		type_name := c.name()
		if !c.eat_seq(['add', 'value']) {
			return
		}
		c.eat_seq(['if', 'not', 'exists'])
		vt := c.at(0)
		if !l.in_block() {
			return
		}
		if l.cfg.pg_major < 12 {
			l.flag(st, 'PGL002', .danger, '', 0, 'ALTER TYPE ${type_name} ADD VALUE cannot run inside a transaction block before Postgres 12.',
				'Run it in a migration without a transaction, or upgrade.')
		} else if vt.kind == .string_lit {
			l.tx.new_enum_values[vt.text] = st.line
		}
		return
	}
	if c.eat('system') {
		if l.in_block() {
			l.flag(st, 'PGL002', .danger, '', 0, 'ALTER SYSTEM cannot run inside a transaction block.',
				'Run it outside the migration transaction.')
		}
		return
	}
	if c.eat('index') {
		c.eat_seq(['if', 'exists'])
		idx_name := c.name()
		if c.eat_seq(['set', 'tablespace']) {
			table := if idx_name in l.index_tables { l.index_tables[idx_name] } else { 'table of index ${idx_name}' }
			l.acquire(st, table, lm_access_exclusive, 'ALTER INDEX SET TABLESPACE')
			if !l.is_new(table) {
				l.flag(st, 'PGL003', .danger, table, lm_access_exclusive, 'ALTER INDEX ${idx_name} SET TABLESPACE copies the whole index while holding ACCESS EXCLUSIVE on it, so writes to ${table} and reads that use the index stop until the copy ends.',
					'Build a new index CONCURRENTLY in the target tablespace and drop the old one CONCURRENTLY.')
				l.slow_step(st, table, 'index copy')
			}
		}
	}
}

fn (mut l Linter) alter_table_cmd(st Statement, table string, toks []Token) {
	if toks.len == 0 {
		return
	}
	mut c := Cur{
		toks: toks
	}
	fresh := l.is_new(table)
	mut ti := l.tinfo(table)
	if c.eat('add') {
		if c.kw('constraint') || c.kw('primary') || c.kw('unique') || c.kw('check')
			|| c.kw('foreign') || c.kw('exclude') {
			l.add_constraint(mut c, st, table)
		} else {
			c.eat('column')
			c.eat_seq(['if', 'not', 'exists'])
			l.add_column(mut c, st, table)
		}
		return
	}
	if c.eat('drop') {
		if c.eat('constraint') {
			c.eat_seq(['if', 'exists'])
			cname := c.name()
			l.acquire(st, table, lm_access_exclusive, 'DROP CONSTRAINT')
			if cname in ti.fk_refs {
				l.acquire(st, ti.fk_refs[cname], lm_share_row_exclusive, 'DROP CONSTRAINT (referenced table)')
				ti.fk_refs.delete(cname)
			}
			return
		}
		c.eat('column')
		c.eat_seq(['if', 'exists'])
		col := c.name()
		l.acquire(st, table, lm_access_exclusive, 'DROP COLUMN')
		if !fresh {
			l.flag(st, 'PGL011', .warning, table, lm_access_exclusive, 'DROP COLUMN ${table}.${col} is instant for Postgres, but application instances still running the old code break on their next query that names it. ORMs that cache column lists, such as Rails, Django and Hibernate, also fail on SELECT * style queries.',
				'Ship code that stops reading and writing ${col} first (Rails ignored_columns, a Django model change), then drop the column in a later deploy.')
		}
		ti.columns.delete(col)
		ti.proven_not_null.delete(col)
		return
	}
	if c.eat('alter') {
		c.eat('column')
		col := c.name()
		l.alter_column(mut c, st, table, col)
		return
	}
	if c.eat_seq(['validate', 'constraint']) {
		l.validate_constraint(st, table, c.name())
		return
	}
	if c.eat('rename') {
		l.acquire(st, table, lm_access_exclusive, 'ALTER TABLE RENAME')
		if c.eat('constraint') {
			return
		}
		if c.eat('to') {
			new_name := c.name()
			if !fresh {
				l.flag(st, 'PGL011', .warning, table, lm_access_exclusive, 'Renaming table ${table} to ${new_name} breaks every running application instance that still uses the old name until it is redeployed.',
					'Expand and contract: create a view or the new table, switch the code, then remove the old name in a later deploy.')
			}
			if new_name != '' && table in l.tables {
				moved := l.tables[table] or { &TableInfo{} }
				l.tables[new_name] = moved
				l.tables.delete(table)
			}
			return
		}
		c.eat('column')
		old_col := c.name()
		c.eat('to')
		new_col := c.name()
		if !fresh {
			l.flag(st, 'PGL011', .warning, table, lm_access_exclusive, 'Renaming ${table}.${old_col} to ${new_col} breaks every running application instance that still uses the old column name.',
				'Add the new column, write to both, backfill, switch reads, then drop the old column in a later deploy.')
		}
		if old_col in ti.columns {
			ti.columns[new_col] = ti.columns[old_col]
			ti.columns.delete(old_col)
		}
		if ti.proven_not_null[old_col] {
			ti.proven_not_null[new_col] = true
			ti.proven_not_null.delete(old_col)
		}
		return
	}
	if c.kw('set') {
		if c.kw_at(1, 'tablespace') || c.kw_at(1, 'logged') || c.kw_at(1, 'unlogged')
			|| c.kw_at(1, 'access') {
			what := 'SET ${c.at(1).text.to_upper()}'
			l.acquire(st, table, lm_access_exclusive, 'ALTER TABLE ${what}')
			if !fresh {
				l.flag(st, 'PGL003', .danger, table, lm_access_exclusive, 'ALTER TABLE ${table} ${what} rewrites the whole table while holding ACCESS EXCLUSIVE, so reads and writes on ${table} stop until it finishes.',
					'Schedule it in a maintenance window, or use an online tool such as pg_repack for the move.')
				l.slow_step(st, table, 'table rewrite')
			}
			return
		}
		if c.at(1).kind == .punct && c.at(1).text == '(' {
			l.acquire(st, table, lm_share_update_exclusive, 'ALTER TABLE SET (storage parameters)')
			return
		}
		l.acquire(st, table, lm_access_exclusive, 'ALTER TABLE SET')
		return
	}
	if c.eat('reset') {
		l.acquire(st, table, lm_share_update_exclusive, 'ALTER TABLE RESET')
		return
	}
	if c.kw('enable') || c.kw('disable') {
		if has_word_top(toks, 'trigger') {
			l.acquire(st, table, lm_share_row_exclusive, 'ALTER TABLE ENABLE/DISABLE TRIGGER')
		} else {
			l.acquire(st, table, lm_access_exclusive, 'ALTER TABLE ENABLE/DISABLE')
		}
		return
	}
	if c.eat_seq(['attach', 'partition']) {
		child := c.name()
		parent_mode := if l.cfg.pg_major >= 12 { lm_share_update_exclusive } else { lm_access_exclusive }
		l.acquire(st, table, parent_mode, 'ATTACH PARTITION')
		l.acquire(st, child, lm_access_exclusive, 'ATTACH PARTITION (partition)')
		if child != '' && !l.is_new(child) && l.tinfo(child).validated_checks == 0 {
			l.flag(st, 'PGL004', .warning, child, lm_access_exclusive, 'ATTACH PARTITION scans every row of ${child} to prove it fits the partition bound, holding ACCESS EXCLUSIVE on ${child} for the whole scan.',
				'Before attaching, add a CHECK constraint that matches the bound with NOT VALID, VALIDATE it in its own transaction, then attach. Postgres skips the scan when a valid constraint already implies the bound.')
			l.slow_step(st, child, 'partition bound scan')
		}
		return
	}
	if c.eat_seq(['detach', 'partition']) {
		child := c.name()
		if c.eat('concurrently') || c.eat('finalize') {
			if l.in_block() {
				l.flag(st, 'PGL002', .danger, table, lm_share_update_exclusive, 'DETACH PARTITION CONCURRENTLY cannot run inside a transaction block.',
					'Run it in its own migration without a transaction.')
			}
			l.acquire(st, table, lm_share_update_exclusive, 'DETACH PARTITION CONCURRENTLY')
			return
		}
		l.acquire(st, table, lm_access_exclusive, 'DETACH PARTITION')
		l.acquire(st, child, lm_access_exclusive, 'DETACH PARTITION (partition)')
		if !fresh && l.cfg.pg_major >= 14 {
			l.flag(st, 'PGL013', .warning, table, lm_access_exclusive, 'DETACH PARTITION ${child} takes ACCESS EXCLUSIVE on the parent ${table}, so every query on any partition of ${table} waits.',
				'Use DETACH PARTITION ... CONCURRENTLY (Postgres 14+) outside a transaction.')
		}
		return
	}
	if c.eat('cluster') {
		l.acquire(st, table, lm_share_update_exclusive, 'ALTER TABLE CLUSTER ON')
		return
	}
	l.acquire(st, table, lm_access_exclusive, 'ALTER TABLE')
}

fn (mut l Linter) add_column(mut c Cur, st Statement, table string) {
	col := c.name()
	if col == '' {
		return
	}
	type_toks, cons := split_column_def(c.rest())
	ty := parse_type(type_toks)
	mut ti := l.tinfo(table)
	ti.columns[col] = ty
	l.acquire(st, table, lm_access_exclusive, 'ADD COLUMN')
	not_null := has_seq_top(cons, ['not', 'null'])
	dflt := default_expr(cons)
	null_default := dflt.len == 1 && dflt[0].kind == .word && dflt[0].text == 'null'
	has_default := dflt.len > 0 && !null_default
	serial := ty.base in serial_types
	generated := has_word_top(cons, 'generated')
	identity := generated && has_word_top(cons, 'identity')
	stored := generated && has_word_top(cons, 'stored')
	filled := has_default || serial || identity || stored
	if not_null && filled {
		ti.proven_not_null[col] = true
	}
	if l.is_new(table) {
		if not_null {
			ti.proven_not_null[col] = true
		}
		return
	}
	mut rewrite_reason := ''
	if serial {
		rewrite_reason = '${ty.base} adds a nextval() default, which is volatile, so every existing row gets its own value written'
	} else if identity {
		rewrite_reason = 'an identity column must be filled with a fresh sequence value for every existing row'
	} else if stored {
		rewrite_reason = 'a stored generated column is computed and written for every existing row'
	} else if has_default {
		if l.cfg.pg_major < 11 {
			rewrite_reason = 'before Postgres 11 any non null default is written into every existing row'
		} else {
			vol, fname := default_volatility(dflt)
			if vol == vol_volatile {
				rewrite_reason = 'the default calls ${fname}(), which is VOLATILE, so Postgres must compute and store a separate value for every existing row'
			} else if vol == vol_unknown {
				l.flag(st, 'PGL014', .warning, table, lm_access_exclusive, 'The default for ${table}.${col} calls ${fname}(), whose volatility the linter does not know. If it is VOLATILE, this ADD COLUMN rewrites the whole table under ACCESS EXCLUSIVE. If it is STABLE or IMMUTABLE, Postgres 11+ stores it once and the change is instant.',
					'Check with SELECT provolatile FROM pg_proc WHERE proname = ' + "'${fname}'" + '. For a volatile default, add the column with no default, set the default in a second statement, and backfill in batches.')
			}
		}
	}
	if rewrite_reason != '' {
		l.flag(st, 'PGL003', .danger, table, lm_access_exclusive, 'ADD COLUMN ${col} rewrites ${table}: ${rewrite_reason}. Reads and writes on ${table} stop for the whole rewrite.',
			'Add the column with no default (instant), then ALTER COLUMN ${col} SET DEFAULT in a separate statement so only new rows get it, and backfill old rows in batches.')
		l.slow_step(st, table, 'table rewrite')
	}
	if not_null && !filled {
		l.flag(st, 'PGL012', .danger, table, lm_access_exclusive, 'ADD COLUMN ${col} ... NOT NULL with no default fails with a NOT NULL violation as soon as ${table} has a single row.',
			'Add it with a constant default (instant on Postgres 11+), or add it nullable, backfill, and add NOT NULL later through a validated CHECK constraint.')
	}
	if has_word_top(cons, 'primary') || has_word_top(cons, 'unique') {
		l.flag(st, 'PGL005', .danger, table, lm_access_exclusive, 'ADD COLUMN ${col} with PRIMARY KEY or UNIQUE builds an index on ${table} while holding ACCESS EXCLUSIVE, blocking reads and writes for the whole build.',
			'Add the column plain, CREATE UNIQUE INDEX CONCURRENTLY, then ADD CONSTRAINT ... USING INDEX.')
		l.slow_step(st, table, 'index build')
	}
	ridx := index_of_word_top(cons, 'references')
	if ridx >= 0 {
		mut rc := Cur{
			toks: cons
			pos:  ridx + 1
		}
		ref := rc.name()
		l.acquire(st, ref, lm_share_row_exclusive, 'ADD COLUMN ... REFERENCES')
		if has_default {
			l.flag(st, 'PGL004', .warning, table, lm_access_exclusive, 'Every existing row of ${table} now holds the default for ${col}, so the new foreign key to ${ref} is checked row by row while both tables are locked.',
				'Add the column without REFERENCES, then ADD CONSTRAINT ... FOREIGN KEY ... NOT VALID and VALIDATE it in a separate transaction.')
			l.slow_step(st, table, 'foreign key validation')
		}
	}
	if has_word_top(cons, 'check') {
		l.flag(st, 'PGL004', .warning, table, lm_access_exclusive, 'An inline CHECK on the new column ${col} is verified against every existing row of ${table} under ACCESS EXCLUSIVE.',
			'Add the column first, then ADD CONSTRAINT ... CHECK (...) NOT VALID and VALIDATE it in its own transaction.')
		l.slow_step(st, table, 'CHECK validation scan')
	}
}

fn (mut l Linter) alter_column(mut c Cur, st Statement, table string, col string) {
	mut ti := l.tinfo(table)
	fresh := l.is_new(table)
	if c.eat('type') || c.eat_seq(['set', 'data', 'type']) {
		mut type_toks := []Token{}
		for !c.done() && !c.kw('using') && !c.kw('collate') {
			type_toks << c.at(0)
			c.pos++
		}
		has_using := c.kw('using')
		new_ty := parse_type(type_toks)
		l.acquire(st, table, lm_access_exclusive, 'ALTER COLUMN TYPE')
		if !fresh {
			has_old := col in ti.columns
			old_ty := if has_old { ti.columns[col] } else { PgType{} }
			verdict, why := type_change_verdict(old_ty, has_old, new_ty, has_using, l.cfg.pg_major)
			if verdict == 2 {
				l.flag(st, 'PGL003', .danger, table, lm_access_exclusive, 'ALTER COLUMN ${col} TYPE ${new_ty.label()} on ${table}: ${why}. Reads and writes on ${table} stop for the whole rewrite.',
					'Add a new column of the target type, dual write from the application or a trigger, backfill in batches, then swap names in a short transaction.')
				l.slow_step(st, table, 'table rewrite')
			} else if verdict == 1 {
				l.flag(st, 'PGL003', .warning, table, lm_access_exclusive, 'ALTER COLUMN ${col} TYPE ${new_ty.label()} on ${table}: ${why}.',
					'Confirm the current type with \\d ${table} before running this on a large table, or run the linter over the migration that created ${table} as well.')
			}
		}
		ti.columns[col] = new_ty
		return
	}
	if c.eat_seq(['set', 'not', 'null']) {
		l.acquire(st, table, lm_access_exclusive, 'SET NOT NULL')
		if fresh || (ti.proven_not_null[col] && l.cfg.pg_major >= 12) {
			ti.proven_not_null[col] = true
			return
		}
		hint := if l.cfg.pg_major >= 12 {
			'Add CONSTRAINT ${col}_not_null CHECK (${col} IS NOT NULL) NOT VALID, commit, VALIDATE CONSTRAINT in a later transaction, then SET NOT NULL (Postgres 12+ uses the valid check and skips the scan) and drop the check.'
		} else {
			'Before Postgres 12 there is no scan free path. Keep a validated CHECK (${col} IS NOT NULL) instead of a real NOT NULL, or run this in a maintenance window.'
		}
		l.flag(st, 'PGL004', .danger, table, lm_access_exclusive, 'SET NOT NULL on ${table}.${col} scans every row to prove there are no NULLs while holding ACCESS EXCLUSIVE, so reads and writes on ${table} stop for the whole scan.',
			hint)
		l.slow_step(st, table, 'NOT NULL scan')
		ti.proven_not_null[col] = true
		return
	}
	if c.eat_seq(['drop', 'not', 'null']) {
		l.acquire(st, table, lm_access_exclusive, 'DROP NOT NULL')
		ti.proven_not_null.delete(col)
		return
	}
	if c.kw('set') && (c.kw_at(1, 'statistics') || (c.at(1).kind == .punct && c.at(1).text == '(')) {
		l.acquire(st, table, lm_share_update_exclusive, 'ALTER COLUMN SET STATISTICS')
		return
	}
	if c.kw('reset') {
		l.acquire(st, table, lm_share_update_exclusive, 'ALTER COLUMN RESET')
		return
	}
	if c.kw('add') && has_word_top(c.rest(), 'identity') && !fresh {
		l.acquire(st, table, lm_access_exclusive, 'ALTER COLUMN ADD GENERATED AS IDENTITY')
		return
	}
	l.acquire(st, table, lm_access_exclusive, 'ALTER COLUMN')
}

fn (mut l Linter) add_constraint(mut c Cur, st Statement, table string) {
	mut cname := ''
	if c.eat('constraint') {
		cname = c.name()
	}
	rest := c.rest()
	not_valid := has_seq_top(rest, ['not', 'valid'])
	fresh := l.is_new(table)
	mut ti := l.tinfo(table)
	if c.eat('check') {
		l.acquire(st, table, lm_access_exclusive, 'ADD CHECK')
		nn_col := not_null_check_column(c.take_group())
		if not_valid {
			if cname != '' {
				ti.pending_checks[cname] = nn_col
			}
			return
		}
		ti.validated_checks++
		if nn_col != '' {
			ti.proven_not_null[nn_col] = true
		}
		if !fresh {
			l.flag(st, 'PGL004', .danger, table, lm_access_exclusive, 'ADD CONSTRAINT ${cname} CHECK on ${table} checks every existing row while holding ACCESS EXCLUSIVE, so reads and writes stop for the whole scan.',
				'Add it with NOT VALID (instant, enforced for new rows), commit, then VALIDATE CONSTRAINT ${cname} in a separate transaction, which only takes SHARE UPDATE EXCLUSIVE.')
			l.slow_step(st, table, 'CHECK validation scan')
		}
		return
	}
	if c.eat('foreign') {
		c.eat('key')
		c.take_group()
		c.eat('references')
		ref := c.name()
		l.acquire(st, table, lm_share_row_exclusive, 'ADD FOREIGN KEY')
		l.acquire(st, ref, lm_share_row_exclusive, 'ADD FOREIGN KEY (referenced table)')
		if cname != '' {
			ti.fk_refs[cname] = ref
		}
		if not_valid || fresh {
			return
		}
		l.flag(st, 'PGL004', .danger, table, lm_share_row_exclusive, 'ADD FOREIGN KEY ${cname} validates every row of ${table} against ${ref} while holding SHARE ROW EXCLUSIVE on both tables, so writes to ${table} and ${ref} stop for the whole scan.',
			'Add it with NOT VALID, commit, then VALIDATE CONSTRAINT ${cname} in its own transaction. Validation then takes only SHARE UPDATE EXCLUSIVE on ${table} and ROW SHARE on ${ref}.')
		l.slow_step(st, table, 'foreign key validation')
		return
	}
	kind := if c.kw('primary') {
		'PRIMARY KEY'
	} else if c.kw('unique') {
		'UNIQUE'
	} else if c.kw('exclude') {
		'EXCLUDE'
	} else {
		''
	}
	if kind == '' {
		l.acquire(st, table, lm_access_exclusive, 'ADD CONSTRAINT')
		return
	}
	l.acquire(st, table, lm_access_exclusive, 'ADD ${kind}')
	if fresh {
		return
	}
	if has_seq_top(rest, ['using', 'index']) {
		if kind == 'PRIMARY KEY' {
			l.flag(st, 'PGL004', .warning, table, lm_access_exclusive, 'ADD PRIMARY KEY USING INDEX on ${table} runs SET NOT NULL on the index columns. Any of them that is not already NOT NULL is proven with a full scan under ACCESS EXCLUSIVE.',
				'Make the key columns NOT NULL first through a validated CHECK (col IS NOT NULL) constraint, then attach the primary key.')
		}
		return
	}
	l.flag(st, 'PGL005', .danger, table, lm_access_exclusive, 'ADD ${kind} on ${table} builds a new index while holding ACCESS EXCLUSIVE, so reads and writes on ${table} stop for the whole build.',
		if kind == 'EXCLUDE' {
			'Exclusion constraints cannot use an existing index. Run this in a maintenance window or on a new table.'
		} else {
			'CREATE UNIQUE INDEX CONCURRENTLY first, then ADD CONSTRAINT ... ${kind} USING INDEX, which only takes a brief lock.'
		})
	l.slow_step(st, table, 'index build')
}

fn (mut l Linter) validate_constraint(st Statement, table string, cname string) {
	mut ti := l.tinfo(table)
	l.acquire(st, table, lm_share_update_exclusive, 'VALIDATE CONSTRAINT')
	if cname in ti.fk_refs {
		l.acquire(st, ti.fk_refs[cname], lm_row_share, 'VALIDATE CONSTRAINT (referenced table)')
	}
	l.slow_step(st, table, 'constraint validation scan')
	if cname in ti.pending_checks {
		nn_col := ti.pending_checks[cname]
		if nn_col != '' {
			ti.proven_not_null[nn_col] = true
		}
		ti.validated_checks++
		ti.pending_checks.delete(cname)
	}
}

fn (mut l Linter) handle_drop(mut c Cur, st Statement) {
	c.eat('drop')
	if c.eat('index') {
		concurrently := c.eat('concurrently')
		c.eat_seq(['if', 'exists'])
		names := c.name_list()
		if concurrently && names.len > 1 {
			l.flag(st, 'PGL002', .danger, '', 0, 'DROP INDEX CONCURRENTLY accepts exactly one index, and Postgres rejects this statement.',
				'Write one DROP INDEX CONCURRENTLY per index.')
		}
		for nm in names {
			table := if nm in l.index_tables { l.index_tables[nm] } else { 'table of index ${nm}' }
			if concurrently {
				if l.in_block() {
					l.flag(st, 'PGL002', .danger, table, lm_share_update_exclusive, 'DROP INDEX CONCURRENTLY cannot run inside a transaction block. Postgres aborts the migration here.',
						'Put it in its own migration with -- migration-lint: no-transaction and turn off the runner transaction for that file.')
				}
				l.acquire(st, table, lm_share_update_exclusive, 'DROP INDEX CONCURRENTLY')
				continue
			}
			l.acquire(st, table, lm_access_exclusive, 'DROP INDEX')
			if !l.is_new(table) {
				l.flag(st, 'PGL013', .warning, table, lm_access_exclusive, 'DROP INDEX ${nm} takes ACCESS EXCLUSIVE on ${table}. The drop itself is quick, but it queues behind any running query on ${table} and blocks all reads and writes queued after it.',
					'Use DROP INDEX CONCURRENTLY outside a transaction.')
			}
			l.index_tables.delete(nm)
		}
		return
	}
	if c.eat('table') {
		c.eat_seq(['if', 'exists'])
		for nm in c.name_list() {
			l.acquire(st, nm, lm_access_exclusive, 'DROP TABLE')
			if !l.is_new(nm) {
				l.flag(st, 'PGL011', .warning, nm, lm_access_exclusive, 'DROP TABLE ${nm} cannot be undone, and any running application instance that still queries ${nm} starts failing immediately.',
					'Remove every code path that reads or writes ${nm} in an earlier deploy, keep a backup, then drop it.')
			}
			l.tables.delete(nm)
		}
	}
}

fn (mut l Linter) handle_reindex(mut c Cur, st Statement) {
	c.eat('reindex')
	mut concurrently := false
	if c.punct('(') {
		concurrently = has_word_any(c.take_group(), 'concurrently')
	}
	target := c.at(0).text
	c.pos++
	if c.eat('concurrently') {
		concurrently = true
	}
	obj := c.name()
	if target in ['schema', 'database', 'system'] && l.in_block() {
		l.flag(st, 'PGL002', .danger, '', 0, 'REINDEX ${target.to_upper()} cannot run inside a transaction block.',
			'Run it outside the migration, ideally from a maintenance job.')
		return
	}
	table := if target == 'index' {
		if obj in l.index_tables { l.index_tables[obj] } else { 'table of index ${obj}' }
	} else {
		obj
	}
	if concurrently {
		if l.in_block() {
			l.flag(st, 'PGL002', .danger, table, lm_share_update_exclusive, 'REINDEX CONCURRENTLY cannot run inside a transaction block.',
				'Run it in its own migration without a transaction.')
		}
		l.acquire(st, table, lm_share_update_exclusive, 'REINDEX CONCURRENTLY')
		return
	}
	l.acquire(st, table, lm_share, 'REINDEX')
	if l.is_new(table) {
		return
	}
	hint := if l.cfg.pg_major >= 12 {
		'Use REINDEX ... CONCURRENTLY outside a transaction.'
	} else {
		'Build a replacement index CONCURRENTLY and drop the old one, since REINDEX CONCURRENTLY needs Postgres 12.'
	}
	l.flag(st, 'PGL013', .warning, table, lm_share, 'REINDEX ${target.to_upper()} ${obj} blocks writes to ${table} for the whole rebuild, and the ACCESS EXCLUSIVE lock on each index also blocks reads that would use it.',
		hint)
	l.slow_step(st, table, 'reindex')
}

fn (mut l Linter) handle_cluster(mut c Cur, st Statement) {
	c.eat('cluster')
	c.eat('verbose')
	table := c.name()
	if table == '' {
		if l.in_block() {
			l.flag(st, 'PGL002', .danger, '', 0, 'CLUSTER without a table name cannot run inside a transaction block.',
				'Name the table, or run it outside the migration.')
		}
		return
	}
	l.acquire(st, table, lm_access_exclusive, 'CLUSTER')
	if !l.is_new(table) {
		l.flag(st, 'PGL003', .danger, table, lm_access_exclusive, 'CLUSTER ${table} rewrites the whole table in index order under ACCESS EXCLUSIVE, so reads and writes stop until it ends.',
			'Use pg_repack or pg_squeeze for an online reorder, or schedule a maintenance window.')
		l.slow_step(st, table, 'table rewrite')
	}
}

fn (mut l Linter) handle_vacuum(mut c Cur, st Statement) {
	c.eat('vacuum')
	if l.in_block() {
		l.flag(st, 'PGL002', .danger, '', 0, 'VACUUM cannot run inside a transaction block. Postgres aborts the migration here.',
			'Leave vacuuming to autovacuum or a maintenance job.')
	}
	mut full := false
	if c.punct('(') {
		opts := c.take_group()
		full = has_word_any(opts, 'full') && !has_seq_top(opts, ['full', 'false'])
	}
	for c.kw('full') || c.kw('freeze') || c.kw('verbose') || c.kw('analyze') || c.kw('analyse') {
		if c.kw('full') {
			full = true
		}
		c.pos++
	}
	if !full {
		return
	}
	names := c.name_list()
	if names.len == 0 {
		l.flag(st, 'PGL003', .danger, '', lm_access_exclusive, 'VACUUM FULL with no table list rewrites every table in the database, each under ACCESS EXCLUSIVE.',
			'Use pg_repack per table, or a maintenance window.')
		return
	}
	for nm in names {
		l.acquire(st, nm, lm_access_exclusive, 'VACUUM FULL')
		if !l.is_new(nm) {
			l.flag(st, 'PGL003', .danger, nm, lm_access_exclusive, 'VACUUM FULL ${nm} rewrites the whole table under ACCESS EXCLUSIVE, so reads and writes stop until it ends.',
				'Use pg_repack or pg_squeeze for an online compaction.')
		}
	}
}

fn (mut l Linter) handle_refresh(mut c Cur, st Statement) {
	if !c.eat_seq(['refresh', 'materialized', 'view']) {
		return
	}
	concurrently := c.eat('concurrently')
	view := c.name()
	if concurrently {
		l.acquire(st, view, lm_exclusive, 'REFRESH MATERIALIZED VIEW CONCURRENTLY')
		return
	}
	l.acquire(st, view, lm_access_exclusive, 'REFRESH MATERIALIZED VIEW')
	if !l.is_new(view) && !has_seq_top(st.tokens, ['with', 'no', 'data']) {
		l.flag(st, 'PGL013', .warning, view, lm_access_exclusive, 'REFRESH MATERIALIZED VIEW ${view} holds ACCESS EXCLUSIVE for the full recompute, so every SELECT on ${view} waits until it ends.',
			'Add a unique index on the view and use REFRESH MATERIALIZED VIEW CONCURRENTLY.')
		l.slow_step(st, view, 'materialized view refresh')
	}
}

fn (mut l Linter) handle_truncate(mut c Cur, st Statement) {
	c.eat('truncate')
	c.eat('table')
	c.eat('only')
	cascade := has_word_top(st.tokens, 'cascade')
	for nm in c.name_list() {
		l.acquire(st, nm, lm_access_exclusive, 'TRUNCATE')
		if !l.is_new(nm) {
			extra := if cascade { ' CASCADE also empties every table with a foreign key pointing at it.' } else { '' }
			l.flag(st, 'PGL011', .warning, nm, lm_access_exclusive, 'TRUNCATE ${nm} deletes every row and cannot be undone once committed.${extra}',
				'Make sure this runs only where the data is disposable, and never in a migration that also runs in production by accident.')
		}
	}
}

fn (mut l Linter) handle_lock(mut c Cur, st Statement) {
	c.eat('lock')
	c.eat('table')
	c.eat('only')
	names := c.name_list()
	mut mode := lm_access_exclusive
	if c.eat('in') {
		mut words := []string{}
		for !c.done() && !c.kw('mode') {
			words << c.at(0).text
			c.pos++
		}
		mode = mode_from_words(words)
	}
	for nm in names {
		l.acquire(st, nm, mode, 'LOCK TABLE')
	}
}

fn (mut l Linter) handle_dml(mut c Cur, st Statement) {
	verb := st.tokens[0].text.to_upper()
	if verb == 'UPDATE' {
		c.eat('update')
	} else {
		c.eat('delete')
		c.eat('from')
	}
	c.eat('only')
	table := c.name()
	if table == '' {
		return
	}
	l.acquire(st, table, lm_row_exclusive, verb)
	if l.is_new(table) {
		return
	}
	if has_word_top(st.tokens, 'where') {
		l.flag(st, 'PGL009', .info, table, lm_row_exclusive, '${verb} on ${table} runs inside the schema migration. If the WHERE clause matches many rows, they stay locked until commit and the dead tuples land all at once.',
			'Keep data changes small here, or move them to a batched backfill job.')
		return
	}
	l.flag(st, 'PGL009', .warning, table, lm_row_exclusive, '${verb} without WHERE touches every row of ${table} in one statement. Every row stays locked until commit, concurrent writers to those rows wait, and the table gains a full copy of dead tuples for VACUUM to clean up.',
		'Backfill in batches of a few thousand rows by primary key range, outside the schema migration, with a short pause between batches.')
	l.slow_step(st, table, 'backfill ${verb}')
}

fn (mut l Linter) handle_insert(mut c Cur, st Statement) {
	c.eat('insert')
	c.eat('into')
	table := c.name()
	if table == '' {
		return
	}
	l.acquire(st, table, lm_row_exclusive, 'INSERT')
	if has_word_top(st.tokens, 'select') {
		l.slow_step(st, table, 'INSERT ... SELECT copy')
	}
}

fn json_str(s string) string {
	mut sb := strings.new_builder(s.len + 2)
	sb.write_u8(`"`)
	for ch in s {
		if ch == `"` {
			sb.write_string('\\"')
		} else if ch == `\\` {
			sb.write_string('\\\\')
		} else if ch == `\n` {
			sb.write_string('\\n')
		} else if ch == `\r` {
			sb.write_string('\\r')
		} else if ch == `\t` {
			sb.write_string('\\t')
		} else if ch < 0x20 {
			sb.write_string('\\u00${ch:02x}')
		} else {
			sb.write_u8(ch)
		}
	}
	sb.write_u8(`"`)
	return sb.str()
}

fn rule_slug(id string) string {
	for r in rules {
		if r.id == id {
			return r.slug
		}
	}
	return ''
}

fn gh_escape(s string) string {
	return s.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
}

fn gh_prop(s string) string {
	return gh_escape(s).replace(':', '%3A').replace(',', '%2C')
}

fn render_text(l &Linter, files int) string {
	mut sb := strings.new_builder(4096)
	sb.writeln('${tool_name} ${tool_version}: PostgreSQL ${l.cfg.pg_major} lock rules, ${files} file(s), ${l.statements} statement(s)')
	sb.writeln('')
	for f in l.findings {
		mut ctx := []string{}
		if f.table != '' {
			ctx << f.table
		}
		if f.mode > lm_none {
			ctx << '${lock_name(f.mode)} blocks ${blocked_traffic(f.mode)}'
		}
		ctx_joined := ctx.join(', ')
		ctx_text := if ctx.len > 0 { ' [${ctx_joined}]' } else { '' }
		sb.writeln('${f.file}:${f.line}: ${sev_label(f.severity)} ${f.rule} ${rule_slug(f.rule)}${ctx_text}')
		if f.statement != '' {
			sb.writeln('    ${f.statement}')
		}
		sb.writeln('    ${f.message}')
		if f.hint != '' {
			sb.writeln('    fix: ${f.hint}')
		}
		sb.writeln('')
	}
	if l.txs.len > 0 {
		sb.writeln('Lock plan (transactions that block writes on existing tables)')
		for t in l.txs {
			span := if t.start_line == t.end_line { 'line ${t.start_line}' } else { 'lines ${t.start_line}-${t.end_line}' }
			outcome := if t.committed { '' } else { ', rolled back' }
			sb.writeln('  ${t.file} ${span} (${tx_label(t.kind)}${outcome})')
			for h in t.locks {
				sb.writeln('    ${lock_name(h.mode)} on ${h.table} from line ${h.line} (${h.why}), blocks ${blocked_traffic(h.mode)} until commit')
			}
			for s in t.slow {
				sb.writeln('    slow step: ${s}')
			}
		}
		sb.writeln('')
	}
	errors, warnings, infos := count_by_severity(l.findings)
	sb.writeln('${errors} error(s), ${warnings} warning(s), ${infos} info, ${l.suppressed} suppressed')
	return sb.str()
}

fn count_by_severity(findings []Finding) (int, int, int) {
	mut e := 0
	mut w := 0
	mut i := 0
	for f in findings {
		match f.severity {
			.danger { e++ }
			.warning { w++ }
			.info { i++ }
		}
	}
	return e, w, i
}

fn render_json(l &Linter, files int) string {
	mut sb := strings.new_builder(4096)
	sb.write_string('{"tool":${json_str(tool_name)},"version":${json_str(tool_version)},"postgres":${l.cfg.pg_major},"files":${files},"statements":${l.statements},"findings":[')
	for i, f in l.findings {
		if i > 0 {
			sb.write_string(',')
		}
		mode_text := if f.mode > lm_none { lock_name(f.mode) } else { '' }
		blocks_text := if f.mode > lm_none { blocked_traffic(f.mode) } else { '' }
		sb.write_string('{"file":${json_str(f.file)},"line":${f.line},"rule":${json_str(f.rule)},"slug":${json_str(rule_slug(f.rule))},"severity":${json_str(sev_label(f.severity))},"table":${json_str(f.table)},"lock_mode":${json_str(mode_text)},"blocks":${json_str(blocks_text)},"statement":${json_str(f.statement)},"message":${json_str(f.message)},"hint":${json_str(f.hint)}}')
	}
	sb.write_string('],"transactions":[')
	for i, t in l.txs {
		if i > 0 {
			sb.write_string(',')
		}
		sb.write_string('{"file":${json_str(t.file)},"start_line":${t.start_line},"end_line":${t.end_line},"kind":${json_str(tx_label(t.kind))},"committed":${t.committed},"locks":[')
		for k, h in t.locks {
			if k > 0 {
				sb.write_string(',')
			}
			sb.write_string('{"table":${json_str(h.table)},"mode":${json_str(lock_name(h.mode))},"line":${h.line},"reason":${json_str(h.why)},"blocks":${json_str(blocked_traffic(h.mode))}}')
		}
		sb.write_string('],"slow_steps":[')
		for k, s in t.slow {
			if k > 0 {
				sb.write_string(',')
			}
			sb.write_string(json_str(s))
		}
		sb.write_string(']}')
	}
	errors, warnings, infos := count_by_severity(l.findings)
	sb.write_string('],"summary":{"errors":${errors},"warnings":${warnings},"info":${infos},"suppressed":${l.suppressed}}}')
	return sb.str()
}

fn render_github(l &Linter) string {
	mut sb := strings.new_builder(4096)
	for f in l.findings {
		level := match f.severity {
			.danger { 'error' }
			.warning { 'warning' }
			.info { 'notice' }
		}
		msg := if f.hint != '' { '${f.message} Fix: ${f.hint}' } else { f.message }
		title := gh_prop(f.rule + ' ' + rule_slug(f.rule))
		sb.writeln('::${level} file=${gh_prop(f.file)},line=${f.line},title=${title}::${gh_escape(msg)}')
	}
	return sb.str()
}

fn usage() string {
	return 'usage: ${tool_name} [flags] <file.sql | dir | -> ...

Simulates the Postgres locks each migration statement takes, transaction by
transaction, and reports statements that block reads or writes on existing
tables, rewrite them, scan them under a lock or fail at runtime.

flags:
  -pg N                    target PostgreSQL major version (default 16)
  -format text|json|github output format (default text)
  -fail-on error|warning|never
                           exit 1 when a finding at this level exists (default error)
  -wrap auto|file|none     how the runner wraps each file in a transaction
                           auto: one transaction per file unless the file has BEGIN
  -max-lock-timeout D      highest acceptable lock_timeout (default 10s)
  -assume-lock-timeout D   lock_timeout the runner or role already sets (default none)
  -disable PGL001,PGL009   rules to turn off
  -rules                   list every rule and exit
  -version                 print the version and exit

Directories are expanded to their *.sql files in name order. Files are read in
the order given, so pass migrations oldest first: the linter remembers tables,
column types and validated constraints from earlier files.

In SQL:  -- migration-lint: ignore PGL001,PGL009 reason
         -- migration-lint: no-transaction

exit codes: 0 clean, 1 findings at or above -fail-on, 2 usage, read or parse error'
}

fn parse_args(args []string) !Config {
	mut cfg := Config{}
	mut i := 0
	value_flags := ['pg', 'format', 'fail-on', 'wrap', 'max-lock-timeout', 'assume-lock-timeout', 'disable']
	for i < args.len {
		a := args[i]
		if a == '-' || !a.starts_with('-') {
			cfg.files << a
			i++
			continue
		}
		mut key := a.trim_left('-')
		mut val := ''
		mut has_inline := false
		if key.contains('=') {
			val = key.all_after('=')
			key = key.all_before('=')
			has_inline = true
		}
		if key in value_flags && !has_inline {
			if i + 1 >= args.len {
				return error('flag -${key} needs a value')
			}
			i++
			val = args[i]
		}
		match key {
			'h', 'help' {
				cfg.show_help = true
			}
			'version' {
				cfg.show_version = true
			}
			'rules' {
				cfg.show_rules = true
			}
			'pg' {
				cfg.pg_major = val.int()
				if cfg.pg_major < 9 || cfg.pg_major > 30 {
					return error('-pg must be a PostgreSQL major version such as 13 or 16, got "${val}"')
				}
			}
			'format' {
				if val !in ['text', 'json', 'github'] {
					return error('-format must be text, json or github')
				}
				cfg.format = val
			}
			'fail-on' {
				if val !in ['error', 'warning', 'never'] {
					return error('-fail-on must be error, warning or never')
				}
				cfg.fail_on = val
			}
			'wrap' {
				if val !in ['auto', 'file', 'none'] {
					return error('-wrap must be auto, file or none')
				}
				cfg.wrap = val
			}
			'max-lock-timeout' {
				ms := parse_duration_ms(val)
				if ms <= 0 {
					return error('-max-lock-timeout needs a positive duration such as 5s')
				}
				cfg.max_lock_timeout_ms = ms
			}
			'assume-lock-timeout' {
				ms := parse_duration_ms(val)
				if ms < 0 {
					return error('-assume-lock-timeout needs a duration such as 2s')
				}
				cfg.assume_lock_timeout_ms = ms
			}
			'disable' {
				for part in val.split(',') {
					id := part.trim_space().to_upper()
					if id == '' {
						continue
					}
					if rule_slug(id) == '' {
						return error('unknown rule ${id} in -disable')
					}
					cfg.disabled << id
				}
			}
			else {
				return error('unknown flag ${a}')
			}
		}
		i++
	}
	return cfg
}

fn expand_inputs(paths []string) ![]string {
	mut out := []string{}
	for p in paths {
		if p == '-' {
			out << p
			continue
		}
		if os.is_dir(p) {
			mut entries := os.ls(p) or { return error('cannot list ${p}: ${err.msg()}') }
			entries.sort()
			mut found := 0
			for e in entries {
				if e.to_lower().ends_with('.sql') {
					out << os.join_path(p, e)
					found++
				}
			}
			if found == 0 {
				return error('${p} contains no .sql files')
			}
			continue
		}
		if !os.exists(p) {
			return error('${p} does not exist')
		}
		out << p
	}
	return out
}

fn main() {
	cfg := parse_args(os.args[1..]) or {
		eprintln('${tool_name}: ${err.msg()}')
		eprintln('')
		eprintln(usage())
		exit(2)
	}
	if cfg.show_help {
		println(usage())
		return
	}
	if cfg.show_version {
		println('${tool_name} ${tool_version}')
		return
	}
	if cfg.show_rules {
		for r in rules {
			println('${r.id}  ${r.slug}\n        ${r.summary}')
		}
		return
	}
	if cfg.files.len == 0 {
		eprintln('${tool_name}: no input files')
		eprintln('')
		eprintln(usage())
		exit(2)
	}
	files := expand_inputs(cfg.files) or {
		eprintln('${tool_name}: ${err.msg()}')
		exit(2)
	}
	mut parsed := []ParsedFile{}
	mut parse_errors := []string{}
	for path in files {
		mut src := ''
		if path == '-' {
			src = os.get_lines().join('\n')
		} else {
			src = os.read_file(path) or {
				eprintln('${tool_name}: cannot read ${path}: ${err.msg()}')
				exit(2)
			}
		}
		pf := lex_file(src, if path == '-' { '<stdin>' } else { path })
		parse_errors << pf.errors
		parsed << pf
	}
	if parse_errors.len > 0 {
		for e in parse_errors {
			eprintln('parse error: ${e}')
		}
		eprintln('${tool_name}: refusing to lint files that do not tokenize cleanly, since every statement after the error would be misread')
		exit(2)
	}
	mut l := Linter{
		cfg: cfg
	}
	for idx, pf in parsed {
		name := if files[idx] == '-' { '<stdin>' } else { files[idx] }
		l.lint_file(name, idx, pf)
	}
	l.findings.sort_with_compare(fn (a &Finding, b &Finding) int {
		if a.file_idx != b.file_idx {
			return a.file_idx - b.file_idx
		}
		if a.line != b.line {
			return a.line - b.line
		}
		return a.seq - b.seq
	})
	match cfg.format {
		'json' { println(render_json(&l, files.len)) }
		'github' { print(render_github(&l)) }
		else { print(render_text(&l, files.len)) }
	}
	errors, warnings, _ := count_by_severity(l.findings)
	failed := match cfg.fail_on {
		'error' { errors > 0 }
		'warning' { errors + warnings > 0 }
		else { false }
	}
	if failed {
		exit(1)
	}
}
