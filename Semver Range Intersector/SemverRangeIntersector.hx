package;

import haxe.Json;

using StringTools;

enum Op {
	Gt;
	Gte;
	Lt;
	Lte;
	Eq;
}

typedef Partial = {
	major:Null<String>,
	minor:Null<String>,
	patch:Null<String>,
	pre:Array<String>
}

typedef Bound = {
	v:Version,
	incl:Bool
}

typedef Constraint = {
	pkg:String,
	rangeText:String,
	source:String,
	line:Int,
	range:Range
}

/** Exact decimal string arithmetic so 99999999999999999 never overflows or rounds. */
class Num {
	public static inline var MAX_LEN = 16;

	static final DIGITS = ~/^[0-9]+$/;

	public static function isNum(s:String):Bool {
		return DIGITS.match(s);
	}

	public static function norm(s:String, what:String):String {
		if (s.length > MAX_LEN)
			throw 'the $what "$s" is longer than $MAX_LEN digits';
		if (s.length > 1 && s.charAt(0) == "0")
			throw 'the $what "$s" has a leading zero';
		return s;
	}

	public static function cmp(a:String, b:String):Int {
		if (a.length != b.length)
			return a.length < b.length ? -1 : 1;
		return a == b ? 0 : (a < b ? -1 : 1);
	}

	public static function inc(s:String):String {
		var chars = s.split("");
		var i = chars.length - 1;
		while (i >= 0) {
			if (chars[i] == "9") {
				chars[i] = "0";
				i--;
			} else {
				chars[i] = String.fromCharCode(chars[i].charCodeAt(0) + 1);
				return chars.join("");
			}
		}
		return "1" + chars.join("");
	}
}

class Version {
	/** The smallest version in SemVer order. Used as the floor of an unbounded range. */
	public static final MIN = new Version("0", "0", "0", ["0"]);

	public final major:String;
	public final minor:String;
	public final patch:String;
	public final pre:Array<String>;

	public function new(major:String, minor:String, patch:String, ?pre:Array<String>) {
		this.major = major;
		this.minor = minor;
		this.patch = patch;
		this.pre = pre == null ? [] : pre;
	}

	public function tuple():String {
		return major + "." + minor + "." + patch;
	}

	public function withoutPre():Version {
		return new Version(major, minor, patch);
	}

	/** The smallest version strictly greater than this prerelease. */
	public function succPre():Version {
		return new Version(major, minor, patch, pre.concat(["0"]));
	}

	public function nextPatch():Version {
		return new Version(major, minor, Num.inc(patch));
	}

	public function toString():String {
		return tuple() + (pre.length > 0 ? "-" + pre.join(".") : "");
	}

	public static function compare(a:Version, b:Version):Int {
		var c = Num.cmp(a.major, b.major);
		if (c != 0)
			return c;
		c = Num.cmp(a.minor, b.minor);
		if (c != 0)
			return c;
		c = Num.cmp(a.patch, b.patch);
		if (c != 0)
			return c;
		var ap = a.pre.length;
		var bp = b.pre.length;
		if (ap == 0 && bp == 0)
			return 0;
		if (ap == 0)
			return 1;
		if (bp == 0)
			return -1;
		var n = ap < bp ? ap : bp;
		for (i in 0...n) {
			var x = a.pre[i];
			var y = b.pre[i];
			if (x == y)
				continue;
			var xn = Num.isNum(x);
			var yn = Num.isNum(y);
			if (xn && yn)
				return Num.cmp(x, y);
			if (xn)
				return -1;
			if (yn)
				return 1;
			return x < y ? -1 : 1;
		}
		return ap == bp ? 0 : (ap < bp ? -1 : 1);
	}
}

class Comparator {
	public final op:Op;
	public final v:Version;

	public function new(op:Op, v:Version) {
		this.op = op;
		this.v = v;
	}

	public function test(x:Version):Bool {
		var c = Version.compare(x, v);
		return switch (op) {
			case Gt: c > 0;
			case Gte: c >= 0;
			case Lt: c < 0;
			case Lte: c <= 0;
			case Eq: c == 0;
		}
	}

	public function toString():String {
		var s = switch (op) {
			case Gt: ">";
			case Gte: ">=";
			case Lt: "<";
			case Lte: "<=";
			case Eq: "";
		}
		return s + v.toString();
	}
}

/** Turns npm range text into comparators, following the node-semver grammar. */
class RangeParser {
	static final PARTIAL = ~/^[vV]?([0-9]+|[xX*])(?:\.([0-9]+|[xX*])(?:\.([0-9]+|[xX*])(?:-([0-9A-Za-z.\-]+))?(?:\+[0-9A-Za-z.\-]+)?)?)?$/;
	static final PRE_ID = ~/^[0-9A-Za-z\-]+$/;
	static final WS = ~/\s+/g;
	static final OP_ONLY = ~/^(<=|>=|<|>|=|\^|~>|~)$/;
	static final OP_PREFIX = ~/^(<=|>=|~>|<|>|=|\^|~)(.*)$/;

	public static function parsePartial(text:String):Partial {
		if (!PARTIAL.match(text))
			throw 'cannot parse "$text" as a version or a range';
		var parts = [PARTIAL.matched(1), PARTIAL.matched(2), PARTIAL.matched(3)];
		var pre = PARTIAL.matched(4);
		var out:Array<Null<String>> = [null, null, null];
		var wild = false;
		for (i in 0...3) {
			var p = parts[i];
			if (wild || p == null || p == "" || p == "x" || p == "X" || p == "*") {
				wild = true;
				out[i] = null;
			} else {
				out[i] = Num.norm(p, "version component");
			}
		}
		var ids:Array<String> = [];
		if (pre != null && pre != "") {
			if (out[2] == null)
				throw 'prerelease tags need a full major.minor.patch version: "$text"';
			ids = pre.split(".");
			for (id in ids) {
				if (id == "" || !PRE_ID.match(id))
					throw 'bad prerelease identifier "$id" in "$text"';
				if (Num.isNum(id))
					Num.norm(id, "numeric prerelease identifier");
			}
		}
		return {major: out[0], minor: out[1], patch: out[2], pre: ids};
	}

	public static function parseVersion(text:String):Version {
		var p = parsePartial(text.trim());
		if (p.major == null || p.minor == null || p.patch == null)
			throw 'expected a full version like 1.2.3, got "$text"';
		return full(p);
	}

	static function full(p:Partial):Version {
		return new Version(p.major, p.minor, p.patch, p.pre);
	}

	static function gte(a:String, b:String, c:String):Comparator {
		return new Comparator(Gte, new Version(a, b, c));
	}

	/** Exclusive upper bound written with the -0 floor so it excludes every prerelease of that tuple. */
	static function lt(a:String, b:String, c:String):Comparator {
		return new Comparator(Lt, new Version(a, b, c, ["0"]));
	}

	static function desugar(op:String, p:Partial):Array<Comparator> {
		var M = p.major;
		var m = p.minor;
		var pt = p.patch;
		var none = [new Comparator(Lt, Version.MIN)];
		switch (op) {
			case "", "=":
				if (M == null)
					return [];
				if (m == null)
					return [gte(M, "0", "0"), lt(Num.inc(M), "0", "0")];
				if (pt == null)
					return [gte(M, m, "0"), lt(M, Num.inc(m), "0")];
				return [new Comparator(Eq, full(p))];
			case "~", "~>":
				if (M == null)
					return [];
				if (m == null)
					return [gte(M, "0", "0"), lt(Num.inc(M), "0", "0")];
				if (pt == null)
					return [gte(M, m, "0"), lt(M, Num.inc(m), "0")];
				return [new Comparator(Gte, full(p)), lt(M, Num.inc(m), "0")];
			case "^":
				if (M == null)
					return [];
				if (m == null)
					return [gte(M, "0", "0"), lt(Num.inc(M), "0", "0")];
				if (pt == null) {
					if (M != "0")
						return [gte(M, m, "0"), lt(Num.inc(M), "0", "0")];
					return [gte("0", m, "0"), lt("0", Num.inc(m), "0")];
				}
				if (M != "0")
					return [new Comparator(Gte, full(p)), lt(Num.inc(M), "0", "0")];
				if (m != "0")
					return [new Comparator(Gte, full(p)), lt(M, Num.inc(m), "0")];
				return [new Comparator(Gte, full(p)), lt(M, m, Num.inc(pt))];
			case ">":
				if (M == null)
					return none;
				if (m == null)
					return [gte(Num.inc(M), "0", "0")];
				if (pt == null)
					return [gte(M, Num.inc(m), "0")];
				return [new Comparator(Gt, full(p))];
			case ">=":
				if (M == null)
					return [];
				if (m == null)
					return [gte(M, "0", "0")];
				if (pt == null)
					return [gte(M, m, "0")];
				return [new Comparator(Gte, full(p))];
			case "<":
				if (M == null)
					return none;
				if (m == null)
					return [lt(M, "0", "0")];
				if (pt == null)
					return [lt(M, m, "0")];
				return [new Comparator(Lt, full(p))];
			case "<=":
				if (M == null)
					return [];
				if (m == null)
					return [lt(Num.inc(M), "0", "0")];
				if (pt == null)
					return [lt(M, Num.inc(m), "0")];
				return [new Comparator(Lte, full(p))];
			default:
				throw 'unknown operator "$op"';
		}
	}

	static function parseComparator(tok:String):Array<Comparator> {
		var op = "";
		var rest = tok;
		if (OP_PREFIX.match(tok)) {
			op = OP_PREFIX.matched(1);
			rest = OP_PREFIX.matched(2);
		}
		return desugar(op, parsePartial(rest));
	}

	static function hyphen(a:String, b:String):Array<Comparator> {
		var lo = parsePartial(a);
		var hi = parsePartial(b);
		var out:Array<Comparator> = [];
		if (lo.major != null) {
			var f = new Version(lo.major, lo.minor == null ? "0" : lo.minor, lo.patch == null ? "0" : lo.patch, lo.patch == null ? null : lo.pre);
			out.push(new Comparator(Gte, f));
		}
		if (hi.major != null) {
			if (hi.minor == null)
				out.push(lt(Num.inc(hi.major), "0", "0"));
			else if (hi.patch == null)
				out.push(lt(hi.major, Num.inc(hi.minor), "0"));
			else
				out.push(new Comparator(Lte, full(hi)));
		}
		return out;
	}

	/** One comparator set: the text between two || separators. */
	public static function parseSet(text:String):Array<Comparator> {
		var trimmed = text.trim();
		if (trimmed == "")
			return [];
		var raw = WS.split(trimmed);
		var toks:Array<String> = [];
		var i = 0;
		while (i < raw.length) {
			var t = raw[i];
			if (OP_ONLY.match(t)) {
				if (i + 1 >= raw.length)
					throw 'the operator "$t" has no version after it';
				t += raw[i + 1];
				i++;
			}
			toks.push(t);
			i++;
		}
		var out:Array<Comparator> = [];
		var j = 0;
		while (j < toks.length) {
			if (j + 2 < toks.length && toks[j + 1] == "-") {
				out = out.concat(hyphen(toks[j], toks[j + 2]));
				j += 3;
			} else {
				out = out.concat(parseComparator(toks[j]));
				j++;
			}
		}
		return out;
	}
}

class Interval {
	public final lo:Null<Bound>;
	public final hi:Null<Bound>;

	public function new(lo:Null<Bound>, hi:Null<Bound>) {
		this.lo = lo;
		this.hi = hi;
	}

	static function tightenLo(cur:Null<Bound>, v:Version, incl:Bool):Bound {
		if (cur == null)
			return {v: v, incl: incl};
		var c = Version.compare(v, cur.v);
		if (c > 0 || (c == 0 && !incl))
			return {v: v, incl: incl};
		return cur;
	}

	static function tightenHi(cur:Null<Bound>, v:Version, incl:Bool):Bound {
		if (cur == null)
			return {v: v, incl: incl};
		var c = Version.compare(v, cur.v);
		if (c < 0 || (c == 0 && !incl))
			return {v: v, incl: incl};
		return cur;
	}

	public static function of(comps:Array<Comparator>):Interval {
		var lo:Null<Bound> = null;
		var hi:Null<Bound> = null;
		for (c in comps) {
			switch (c.op) {
				case Gt:
					lo = tightenLo(lo, c.v, false);
				case Gte:
					lo = tightenLo(lo, c.v, true);
				case Lt:
					hi = tightenHi(hi, c.v, false);
				case Lte:
					hi = tightenHi(hi, c.v, true);
				case Eq:
					lo = tightenLo(lo, c.v, true);
					hi = tightenHi(hi, c.v, true);
			}
		}
		return new Interval(lo, hi);
	}

	public function canonical():Array<Comparator> {
		var out:Array<Comparator> = [];
		if (lo != null && hi != null && lo.incl && hi.incl && Version.compare(lo.v, hi.v) == 0)
			return [new Comparator(Eq, lo.v)];
		if (lo != null)
			out.push(new Comparator(lo.incl ? Gte : Gt, lo.v));
		if (hi != null)
			out.push(new Comparator(hi.incl ? Lte : Lt, hi.v));
		return out;
	}
}

/**
 * One AND group of comparators, as a single interval plus the prerelease rule.
 * `allow` holds one list of major.minor.patch tuples per source range that was
 * merged in. A prerelease version only passes if every list contains its tuple,
 * which is what node-semver does for each range on its own.
 */
class CSet {
	public final comps:Array<Comparator>;
	public final allow:Array<Array<String>>;
	public final iv:Interval;

	public function new(comps:Array<Comparator>, allow:Array<Array<String>>) {
		this.iv = Interval.of(comps);
		this.comps = iv.canonical();
		this.allow = allow;
	}

	public static function fromComps(comps:Array<Comparator>):CSet {
		var tuples:Array<String> = [];
		for (c in comps) {
			if (c.v.pre.length == 0)
				continue;
			// A "<X-0" bound is synthetic: no prerelease of X can sit below it.
			if (c.op == Lt && c.v.pre.length == 1 && c.v.pre[0] == "0")
				continue;
			var t = c.v.tuple();
			if (tuples.indexOf(t) < 0)
				tuples.push(t);
		}
		return new CSet(comps, [tuples]);
	}

	public function merge(o:CSet):CSet {
		return new CSet(comps.concat(o.comps), allow.concat(o.allow));
	}

	public function accepts(v:Version):Bool {
		for (c in comps)
			if (!c.test(v))
				return false;
		if (v.pre.length > 0) {
			var t = v.tuple();
			for (g in allow)
				if (g.indexOf(t) < 0)
					return false;
		}
		return true;
	}

	public function allowedTuples():Array<String> {
		if (allow.length == 0)
			return [];
		var r = allow[0].copy();
		for (g in allow)
			r = r.filter(t -> g.indexOf(t) >= 0);
		return r;
	}

	/**
	 * Smallest accepted version, or null when the set is empty. The smallest
	 * member of a nonempty set is always the lower bound, the first release or
	 * prerelease above it, or the first prerelease of an allowed tuple.
	 */
	public function minVersion():Null<Version> {
		var floor = iv.lo == null ? Version.MIN : iv.lo.v;
		var cands:Array<Version> = [floor];
		if (floor.pre.length > 0) {
			cands.push(floor.withoutPre());
			cands.push(floor.succPre());
		} else {
			cands.push(floor.nextPatch());
		}
		for (g in allow)
			for (t in g) {
				var p = t.split(".");
				cands.push(new Version(p[0], p[1], p[2], ["0"]));
			}
		var best:Null<Version> = null;
		for (c in cands)
			if (accepts(c) && (best == null || Version.compare(c, best) < 0))
				best = c;
		return best;
	}

	public function key():String {
		var groups = allow.map(function(g:Array<String>):String {
			var s = g.copy();
			s.sort((a, b) -> a < b ? -1 : (a > b ? 1 : 0));
			return s.join(",");
		});
		groups.sort((a, b) -> a < b ? -1 : (a > b ? 1 : 0));
		var uniq:Array<String> = [];
		for (g in groups)
			if (uniq.indexOf(g) < 0)
				uniq.push(g);
		return show() + "|" + uniq.join(";");
	}

	public function show():String {
		var s = comps.length == 0 ? "*" : comps.map(c -> c.toString()).join(" ");
		var t = allowedTuples();
		if (t.length > 0)
			s += " {prerelease: " + t.join(", ") + "}";
		return s;
	}
}

class Range {
	/** Cap on || alternatives after an intersection. The product of two big ranges grows fast. */
	public static inline var MAX_SETS = 4096;

	public final sets:Array<CSet>;
	public final dead:Array<String>;

	public function new(sets:Array<CSet>, ?dead:Array<String>) {
		this.sets = sets;
		this.dead = dead == null ? [] : dead;
	}

	public static function parse(text:String):Range {
		var sets:Array<CSet> = [];
		var dead:Array<String> = [];
		for (part in text.split("||")) {
			var cs = CSet.fromComps(RangeParser.parseSet(part));
			if (cs.minVersion() == null)
				dead.push(part.trim());
			else
				sets.push(cs);
		}
		return new Range(sets, dead);
	}

	public function isEmpty():Bool {
		return sets.length == 0;
	}

	public function accepts(v:Version):Bool {
		for (s in sets)
			if (s.accepts(v))
				return true;
		return false;
	}

	public function minVersion():Null<Version> {
		var best:Null<Version> = null;
		for (s in sets) {
			var m = s.minVersion();
			if (m != null && (best == null || Version.compare(m, best) < 0))
				best = m;
		}
		return best;
	}

	public function maxSatisfying(versions:Array<Version>):Null<Version> {
		var best:Null<Version> = null;
		for (v in versions)
			if (accepts(v) && (best == null || Version.compare(v, best) > 0))
				best = v;
		return best;
	}

	public function intersect(o:Range):Range {
		if (sets.length * o.sets.length > 250000)
			throw 'refusing to intersect ${sets.length} by ${o.sets.length} alternatives, simplify the || branches';
		var out:Array<CSet> = [];
		var seen = new Map<String, Bool>();
		for (a in sets)
			for (b in o.sets) {
				var m = a.merge(b);
				if (m.minVersion() == null)
					continue;
				var k = m.key();
				if (seen.exists(k))
					continue;
				seen.set(k, true);
				out.push(m);
				if (out.length > MAX_SETS)
					throw 'the intersection has more than $MAX_SETS alternatives, simplify the || branches';
			}
		return new Range(out);
	}

	public function show():String {
		return sets.length == 0 ? "<empty>" : sets.map(s -> s.show()).join(" || ");
	}
}

typedef Report = {
	pkg:String,
	status:String,
	constraints:Int,
	resolved:String,
	minVersion:Null<String>,
	pick:Null<String>,
	core:Array<{range:String, source:String, line:Int}>,
	warnings:Array<String>
}

/** Reads a constraint file and decides, per package, whether all constraints can be met at once. */
class Analyzer {
	public final constraints = new Map<String, Array<Constraint>>();
	public final available = new Map<String, Array<Version>>();
	public final order:Array<String> = [];
	public final errors:Array<String> = [];

	public function new() {}

	function touch(pkg:String) {
		if (order.indexOf(pkg) < 0)
			order.push(pkg);
	}

	/**
	 * Line formats, fields separated by a pipe:
	 *   constraint <package> | <range> | <source label>
	 *   available <package> | <version> <version> ...
	 * Blank lines and lines starting with # are ignored.
	 */
	public function load(content:String):Void {
		var n = 0;
		for (raw in content.split("\n")) {
			n++;
			var line = raw.trim();
			if (line == "" || line.charAt(0) == "#")
				continue;
			var fields = line.split("|").map(f -> f.trim());
			var head = fields[0];
			var sp = head.indexOf(" ");
			if (sp < 0) {
				errors.push('line $n: expected "constraint <package> | <range> | <source>" or "available <package> | <versions>"');
				continue;
			}
			var kind = head.substr(0, sp);
			var pkg = head.substr(sp + 1).trim();
			try {
				if (pkg == "")
					throw "missing package name";
				switch (kind) {
					case "constraint":
						if (fields.length < 2)
							throw "missing range";
						var range = Range.parse(fields[1]);
						touch(pkg);
						if (!constraints.exists(pkg))
							constraints.set(pkg, []);
						constraints.get(pkg).push({
							pkg: pkg,
							rangeText: fields[1],
							source: fields.length > 2 ? fields[2] : "unknown source",
							line: n,
							range: range
						});
					case "available":
						if (fields.length < 2)
							throw "missing versions";
						if (!available.exists(pkg))
							available.set(pkg, []);
						var ws = ~/\s+/g;
						for (v in ws.split(fields[1]))
							if (v != "")
								available.get(pkg).push(RangeParser.parseVersion(v));
					default:
						throw 'unknown directive "$kind"';
				}
			} catch (e:String) {
				errors.push('line $n: $e');
			}
		}
	}

	static function intersectAll(cs:Array<Constraint>):Range {
		var acc = cs[0].range;
		for (i in 1...cs.length)
			acc = acc.intersect(cs[i].range);
		return acc;
	}

	/** Deletion filter: drop every constraint whose removal keeps the conflict, leaving an irreducible core. */
	static function conflictCore(cs:Array<Constraint>):Array<Constraint> {
		var core = cs.copy();
		var i = 0;
		while (i < core.length) {
			var trial = core.copy();
			trial.splice(i, 1);
			if (trial.length > 0 && intersectAll(trial).isEmpty())
				core = trial;
			else
				i++;
		}
		return core;
	}

	function describe(c:Constraint) {
		return {range: c.rangeText, source: c.source, line: c.line};
	}

	public function analyze(pkg:String):Report {
		var cs = constraints.get(pkg);
		var warnings:Array<String> = [];
		for (c in cs)
			if (c.range.dead.length > 0)
				warnings.push('line ${c.line}: dead alternative in "${c.rangeText}": ${c.range.dead.join(", ")} can never match');
		var rep:Report = {
			pkg: pkg,
			status: "ok",
			constraints: cs.length,
			resolved: "",
			minVersion: null,
			pick: null,
			core: [],
			warnings: warnings
		};
		try {
			var acc = intersectAll(cs);
			if (acc.isEmpty()) {
				rep.status = "conflict";
				rep.resolved = acc.show();
				rep.core = conflictCore(cs).map(describe);
				return rep;
			}
			rep.resolved = acc.show();
			rep.minVersion = acc.minVersion().toString();
			if (available.exists(pkg)) {
				var best = acc.maxSatisfying(available.get(pkg));
				if (best == null)
					rep.status = "no_published_match";
				else
					rep.pick = best.toString();
			}
		} catch (e:String) {
			rep.status = "too_complex";
			rep.resolved = e;
		}
		return rep;
	}

	public function analyzeAll():Array<Report> {
		var out:Array<Report> = [];
		for (p in order)
			if (constraints.exists(p))
				out.push(analyze(p));
		for (p in available.keys())
			if (!constraints.exists(p))
				errors.push('available versions given for "$p" but it has no constraint lines');
		return out;
	}
}

class SelfTest {
	static final SATISFIES:Array<{r:String, v:String, ok:Bool}> = [
		{r: "^1.2.3", v: "1.2.3", ok: true},
		{r: "^1.2.3", v: "1.9.9", ok: true},
		{r: "^1.2.3", v: "2.0.0", ok: false},
		{r: "^1.2.3", v: "1.2.2", ok: false},
		{r: "^0.2.3", v: "0.2.9", ok: true},
		{r: "^0.2.3", v: "0.3.0", ok: false},
		{r: "^0.0.3", v: "0.0.3", ok: true},
		{r: "^0.0.3", v: "0.0.4", ok: false},
		{r: "^0", v: "0.9.9", ok: true},
		{r: "^0", v: "1.0.0", ok: false},
		{r: "~1.2.3", v: "1.2.9", ok: true},
		{r: "~1.2.3", v: "1.3.0", ok: false},
		{r: "~1", v: "1.9.0", ok: true},
		{r: "1.x", v: "1.5.0", ok: true},
		{r: "1.x", v: "2.0.0", ok: false},
		{r: "*", v: "1.0.0", ok: true},
		{r: "*", v: "1.0.0-beta", ok: false},
		{r: "", v: "7.7.7", ok: true},
		{r: "^1.2.3-beta.2", v: "1.2.3-beta.3", ok: true},
		{r: "^1.2.3-beta.2", v: "1.2.3-beta.1", ok: false},
		{r: "^1.2.3-beta.2", v: "1.2.4-beta.1", ok: false},
		{r: "^1.2.3-beta.2", v: "1.2.3", ok: true},
		{r: "<2.0.0", v: "2.0.0-beta.1", ok: false},
		{r: "1.2.3 - 2.3.4", v: "2.3.4", ok: true},
		{r: "1.2.3 - 2.3", v: "2.3.9", ok: true},
		{r: "1.2.3 - 2.3", v: "2.4.0", ok: false},
		{r: "1.2 - 2", v: "2.9.9", ok: true},
		{r: ">=1.2.3 <2.0.0 || >=3.0.0", v: "3.5.0", ok: true},
		{r: ">=1.2.3 <2.0.0 || >=3.0.0", v: "2.5.0", ok: false},
		{r: ">1.2", v: "1.2.9", ok: false},
		{r: ">1.2", v: "1.3.0", ok: true},
		{r: "<=1.2", v: "1.2.9", ok: true},
		{r: "<=1.2", v: "1.3.0", ok: false},
		{r: ">= 1.2.3", v: "1.2.3", ok: true},
		{r: "=1.2.3", v: "1.2.3", ok: true},
		{r: "v1.2.3", v: "1.2.3", ok: true},
		{r: ">=99999999999999 <99999999999999.1", v: "99999999999999.0.5", ok: true}
	];

	/** min is the expected smallest version of the intersection, or "" when it must be empty. */
	static final INTERSECTS:Array<{a:String, b:String, min:String}> = [
		{a: "^1.2.0", b: "~1.4.0", min: "1.4.0"},
		{a: "^1.2.0", b: "^2.0.0", min: ""},
		{a: ">=1.0.0 <2.0.0", b: ">=2.0.0", min: ""},
		{a: ">1.0.0", b: "<=1.0.1", min: "1.0.1"},
		{a: ">1.0.0", b: "<1.0.1", min: ""},
		{a: "^1.0.0-beta.1", b: "<1.0.0", min: ""},
		{a: "^1.0.0-beta.1", b: "<1.0.0-rc.1", min: "1.0.0-beta.1"},
		{a: "^1 || ^3", b: ">=2 <4", min: "3.0.0"},
		{a: "1.2.3", b: ">1.2.3", min: ""},
		{a: "*", b: "^2.4.1", min: "2.4.1"}
	];

	public static function run():Int {
		var failures = 0;
		var total = 0;
		for (t in SATISFIES) {
			total++;
			try {
				var got = Range.parse(t.r).accepts(RangeParser.parseVersion(t.v));
				if (got != t.ok) {
					failures++;
					Sys.println('FAIL satisfies "${t.v}" in "${t.r}": expected ${t.ok}, got $got');
				}
			} catch (e:String) {
				failures++;
				Sys.println('FAIL satisfies "${t.v}" in "${t.r}": $e');
			}
		}
		for (t in INTERSECTS) {
			total++;
			try {
				var m = Range.parse(t.a).intersect(Range.parse(t.b)).minVersion();
				var got = m == null ? "" : m.toString();
				if (got != t.min) {
					failures++;
					Sys.println('FAIL "${t.a}" and "${t.b}": expected min "${t.min}", got "$got"');
				}
			} catch (e:String) {
				failures++;
				Sys.println('FAIL "${t.a}" and "${t.b}": $e');
			}
		}
		Sys.println('${total - failures} of $total self tests passed');
		return failures == 0 ? 0 : 1;
	}
}

#if sys
class SemverRangeIntersector {
	static function usage():Int {
		Sys.println("SemverRangeIntersector

  check <file> [--json]              analyse a constraint file, exit 1 on any conflict
  satisfies <version> <range>        exit 0 when the version satisfies the range
  intersect <range> <range> ...      print the intersection and its smallest version
  max <range> <version> ...          print the highest version that satisfies the range
  selftest                           run the built in conformance table

Constraint file lines, fields separated by a pipe:
  constraint <package> | <range> | <source label>
  available <package> | <version> <version> ...

Exit codes: 0 all satisfiable, 1 conflict or no published match, 2 bad input.");
		return 2;
	}

	static function printText(reports:Array<Report>):Void {
		for (r in reports) {
			var tag = switch (r.status) {
				case "ok": "[OK]";
				case "conflict": "[CONFLICT]";
				case "no_published_match": "[NO PUBLISHED MATCH]";
				default: "[TOO COMPLEX]";
			}
			var head = '$tag ${r.pkg} (${r.constraints} constraints) ';
			switch (r.status) {
				case "ok":
					Sys.println(head + 'resolved: ${r.resolved}  min: ${r.minVersion}' + (r.pick != null ? '  pick: ${r.pick}' : ""));
				case "conflict":
					Sys.println(head + "no version satisfies all of these together:");
					for (c in r.core)
						Sys.println('    ${c.range}   (${c.source}, line ${c.line})');
				case "no_published_match":
					Sys.println(head + 'resolved: ${r.resolved}  min: ${r.minVersion}  but none of the available versions fit');
				default:
					Sys.println(head + r.resolved);
			}
			for (w in r.warnings)
				Sys.println('    warning: $w');
		}
	}

	static function check(path:String, json:Bool):Int {
		if (!sys.FileSystem.exists(path)) {
			Sys.stderr().writeString('cannot read $path\n');
			return 2;
		}
		var a = new Analyzer();
		a.load(sys.io.File.getContent(path));
		var reports = a.analyzeAll();
		if (json)
			Sys.println(Json.stringify({reports: reports, errors: a.errors}, null, "  "));
		else {
			printText(reports);
			for (e in a.errors)
				Sys.println('error: $e');
		}
		if (a.errors.length > 0)
			return 2;
		for (r in reports)
			if (r.status != "ok")
				return 1;
		return 0;
	}

	static function main():Void {
		var args = Sys.args();
		if (args.length == 0)
			Sys.exit(usage());
		var code = 0;
		try {
			switch (args[0]) {
				case "check":
					if (args.length < 2)
						Sys.exit(usage());
					code = check(args[1], args.indexOf("--json") > 0);
				case "satisfies":
					if (args.length != 3)
						Sys.exit(usage());
					var ok = Range.parse(args[2]).accepts(RangeParser.parseVersion(args[1]));
					Sys.println(ok ? "true" : "false");
					code = ok ? 0 : 1;
				case "intersect":
					if (args.length < 3)
						Sys.exit(usage());
					var acc = Range.parse(args[1]);
					for (i in 2...args.length)
						acc = acc.intersect(Range.parse(args[i]));
					Sys.println('intersection: ${acc.show()}');
					var m = acc.minVersion();
					Sys.println('min version:  ' + (m == null ? "none" : m.toString()));
					code = acc.isEmpty() ? 1 : 0;
				case "max":
					if (args.length < 3)
						Sys.exit(usage());
					var range = Range.parse(args[1]);
					var vs = [for (i in 2...args.length) RangeParser.parseVersion(args[i])];
					var best = range.maxSatisfying(vs);
					Sys.println(best == null ? "none" : best.toString());
					code = best == null ? 1 : 0;
				case "selftest":
					code = SelfTest.run();
				default:
					code = usage();
			}
		} catch (e:String) {
			Sys.stderr().writeString('error: $e\n');
			code = 2;
		}
		Sys.exit(code);
	}
}
#end
