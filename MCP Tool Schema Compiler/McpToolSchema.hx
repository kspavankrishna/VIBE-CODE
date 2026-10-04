#if macro
import haxe.macro.Context;
import haxe.macro.Expr;
import haxe.macro.Type;
import haxe.macro.TypeTools;
#end
import haxe.Json;
import haxe.crypto.Sha256;
import haxe.iterators.StringIteratorUnicode;

typedef McpViolation = {
	var path:String;
	var message:String;
}

class McpToolSchema {
	public static inline var DRAFT = "https://json-schema.org/draft/2020-12/schema";
	public static inline var MAX_DEPTH = 64;
	public static inline var MAX_ENUM_ECHO = 12;

	static final TOOL_NAME = ~/^[A-Za-z0-9_-]{1,64}$/;
	static final URI_FORMAT = ~/^[A-Za-z][A-Za-z0-9+.\-]*:[^\s]+$/;
	static final DATE_FORMAT = ~/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/;
	static final TIME_FORMAT = ~/^([01]\d|2[0-3]):[0-5]\d:([0-5]\d|60)(\.\d+)?(Z|z|[+-]([01]\d|2[0-3]):[0-5]\d)$/;
	static final UUID_FORMAT = ~/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
	static final EMAIL_FORMAT = ~/^[^@\s]+@[^@\s]+\.[^@\s]+$/;

	public static final KNOWN_FORMATS = ["uri", "date", "date-time", "uuid", "email"];

	// ------------------------------------------------------------------
	// Compile time entry points
	// ------------------------------------------------------------------

	/** JSON Schema object literal for any supported Haxe type, built at compile time. */
	public static macro function schemaOf(typeExpr:Expr):Expr {
		var t = SchemaBuilder.resolve(typeExpr);
		var schema = SchemaBuilder.root(t, typeExpr.pos);
		SchemaBuilder.enforceBudget("schema", schema, typeExpr.pos);
		return Context.makeExpr(schema, typeExpr.pos);
	}

	/** MCP tool manifest entry: name, description, inputSchema and a schema fingerprint. */
	public static macro function tool(name:Expr, description:Expr, args:Expr):Expr {
		var toolName = SchemaBuilder.constString(name, "tool name");
		var toolDesc = StringTools.trim(SchemaBuilder.constString(description, "tool description"));
		if (!TOOL_NAME.match(toolName))
			Context.error('Tool name "$toolName" must match [A-Za-z0-9_-]{1,64}, the intersection of what MCP and the major model APIs accept.', name.pos);
		if (toolDesc == "")
			Context.error('Tool "$toolName" needs a non empty description. The model chooses tools from it.', description.pos);
		SchemaBuilder.claim(toolName, name.pos);
		var t = SchemaBuilder.resolve(args);
		var schema = SchemaBuilder.root(t, args.pos);
		if (Reflect.field(schema, "type") != "object")
			Context.error('Tool "$toolName" input must be an object structure, not ${Reflect.field(schema, "type")}.', args.pos);
		Reflect.setField(schema, "$schema", DRAFT);
		SchemaBuilder.enforceBudget('tool "$toolName"', schema, args.pos);
		var meta = {};
		Reflect.setField(meta, "schemaSha256", fingerprint(schema));
		var out = {};
		Reflect.setField(out, "name", toolName);
		Reflect.setField(out, "description", toolDesc);
		Reflect.setField(out, "inputSchema", schema);
		Reflect.setField(out, "_meta", meta);
		return Context.makeExpr(out, name.pos);
	}

	// ------------------------------------------------------------------
	// Canonical form and fingerprint
	// ------------------------------------------------------------------

	/** Stable JSON text with sorted keys, so the same schema always hashes the same. */
	public static function canonical(v:Dynamic):String {
		var b = new StringBuf();
		writeCanonical(b, v);
		return b.toString();
	}

	static function writeCanonical(b:StringBuf, v:Dynamic):Void {
		if (v == null) {
			b.add("null");
		} else if (Std.isOfType(v, Bool)) {
			b.add(v ? "true" : "false");
		} else if (Std.isOfType(v, Int)) {
			b.add(Std.string(v));
		} else if (Std.isOfType(v, Float)) {
			var f:Float = v;
			b.add(Math.floor(f) == f && Math.abs(f) < 1e15 ? Std.string(Std.int(f)) : Std.string(f));
		} else if (Std.isOfType(v, String)) {
			b.add(Json.stringify(v));
		} else if (Std.isOfType(v, Array)) {
			var a:Array<Dynamic> = v;
			b.add("[");
			for (i in 0...a.length) {
				if (i > 0) b.add(",");
				writeCanonical(b, a[i]);
			}
			b.add("]");
		} else {
			var keys = Reflect.fields(v);
			keys.sort(compareStrings);
			b.add("{");
			var first = true;
			for (k in keys) {
				if (!first) b.add(",");
				first = false;
				b.add(Json.stringify(k));
				b.add(":");
				writeCanonical(b, Reflect.field(v, k));
			}
			b.add("}");
		}
	}

	public static function fingerprint(schema:Dynamic):String
		return Sha256.encode(canonical(schema));

	static function compareStrings(a:String, b:String):Int
		return a < b ? -1 : (a > b ? 1 : 0);

	// ------------------------------------------------------------------
	// Runtime validator for the exact subset the compiler emits
	// ------------------------------------------------------------------

	/** Validates a decoded JSON value. Returns every violation, an empty array means valid. */
	public static function validate(schema:Dynamic, value:Dynamic):Array<McpViolation> {
		var out:Array<McpViolation> = [];
		walk(schema, schema, value, "", out, 0);
		return out;
	}

	public static function isValid(schema:Dynamic, value:Dynamic):Bool
		return validate(schema, value).length == 0;

	/** One readable block, handy for returning to a model so it can correct its call. */
	public static function describeViolations(violations:Array<McpViolation>):String
		return [for (v in violations) "- " + (v.path == "" ? "(root)" : v.path) + ": " + v.message].join("\n");

	static function add(out:Array<McpViolation>, path:String, message:String):Void
		out.push({path: path, message: message});

	static function pointer(parent:String, key:String):String
		return parent + "/" + StringTools.replace(StringTools.replace(key, "~", "~0"), "/", "~1");

	static function kindOf(v:Dynamic):String {
		if (v == null) return "null";
		if (Std.isOfType(v, Bool)) return "boolean";
		if (Std.isOfType(v, Int)) return "integer";
		if (Std.isOfType(v, Float)) {
			var f:Float = v;
			if (Math.isNaN(f) || !Math.isFinite(f)) return "non finite number";
			return Math.floor(f) == f ? "integer" : "number";
		}
		if (Std.isOfType(v, String)) return "string";
		if (Std.isOfType(v, Array)) return "array";
		return "object";
	}

	static function codePoints(s:String):Int {
		var n = 0;
		for (_ in new StringIteratorUnicode(s)) n++;
		return n;
	}

	static function resolveRef(root:Dynamic, ref:String):Dynamic {
		var prefix = "#/$defs/";
		if (ref.substr(0, prefix.length) != prefix) return null;
		var defs = Reflect.field(root, "$defs");
		return defs == null ? null : Reflect.field(defs, ref.substr(prefix.length));
	}

	static function walk(root:Dynamic, schema:Dynamic, v:Dynamic, path:String, out:Array<McpViolation>, depth:Int):Void {
		if (depth > MAX_DEPTH) {
			add(out, path, "nested deeper than " + MAX_DEPTH + " levels");
			return;
		}
		var ref:Null<String> = Reflect.field(schema, "$ref");
		if (ref != null) {
			var target = resolveRef(root, ref);
			if (target == null) add(out, path, "schema defect, unresolved reference " + ref);
			else walk(root, target, v, path, out, depth + 1);
			return;
		}
		var anyOf:Array<Dynamic> = Reflect.field(schema, "anyOf");
		if (anyOf != null) {
			for (branch in anyOf) {
				var trial:Array<McpViolation> = [];
				walk(root, branch, v, path, trial, depth + 1);
				if (trial.length == 0) return;
			}
			add(out, path, "does not match any allowed shape");
			return;
		}
		var actual = kindOf(v);
		var declared:Dynamic = Reflect.field(schema, "type");
		if (declared != null) {
			var kinds:Array<String> = Std.isOfType(declared, String) ? [declared] : declared;
			var ok = false;
			for (k in kinds)
				if (k == actual || (k == "number" && actual == "integer")) ok = true;
			if (!ok) {
				add(out, path, "expected " + kinds.join(" or ") + ", got " + actual);
				return;
			}
		}
		var allowed:Array<Dynamic> = Reflect.field(schema, "enum");
		if (allowed != null) {
			var hit = false;
			for (a in allowed)
				if (a == v) hit = true;
			if (!hit) {
				var shown = [for (i in 0...Std.int(Math.min(allowed.length, MAX_ENUM_ECHO))) Json.stringify(allowed[i])];
				add(out, path, "must be one of " + shown.join(", ") + (allowed.length > MAX_ENUM_ECHO ? ", ..." : ""));
				return;
			}
		}
		switch (actual) {
			case "integer", "number":
				var n:Float = v;
				var lo:Null<Float> = Reflect.field(schema, "minimum");
				var hi:Null<Float> = Reflect.field(schema, "maximum");
				if (lo != null && n < lo) add(out, path, "must be >= " + lo);
				if (hi != null && n > hi) add(out, path, "must be <= " + hi);
			case "string":
				var s:String = v;
				var len:Null<Int> = null;
				var minL:Null<Int> = Reflect.field(schema, "minLength");
				var maxL:Null<Int> = Reflect.field(schema, "maxLength");
				if (minL != null || maxL != null) len = codePoints(s);
				if (minL != null && len < minL) add(out, path, "must have at least " + minL + " characters");
				if (maxL != null && len > maxL) add(out, path, "must have at most " + maxL + " characters");
				var pattern:Null<String> = Reflect.field(schema, "pattern");
				if (pattern != null && !new EReg(pattern, "").match(s)) add(out, path, "does not match pattern " + pattern);
				var format:Null<String> = Reflect.field(schema, "format");
				if (format != null && !formatOk(format, s)) add(out, path, "is not a valid " + format);
			case "array":
				var a:Array<Dynamic> = v;
				var minI:Null<Int> = Reflect.field(schema, "minItems");
				var maxI:Null<Int> = Reflect.field(schema, "maxItems");
				if (minI != null && a.length < minI) add(out, path, "must have at least " + minI + " items");
				if (maxI != null && a.length > maxI) add(out, path, "must have at most " + maxI + " items");
				var items = Reflect.field(schema, "items");
				if (items != null)
					for (i in 0...a.length) walk(root, items, a[i], pointer(path, Std.string(i)), out, depth + 1);
			case "object":
				walkObject(root, schema, v, path, out, depth);
			default:
		}
	}

	static function walkObject(root:Dynamic, schema:Dynamic, v:Dynamic, path:String, out:Array<McpViolation>, depth:Int):Void {
		var props = Reflect.field(schema, "properties");
		var required:Array<String> = Reflect.field(schema, "required");
		if (required != null)
			for (r in required)
				if (!Reflect.hasField(v, r)) add(out, pointer(path, r), "is required");
		var extra:Dynamic = Reflect.field(schema, "additionalProperties");
		var keys = Reflect.fields(v);
		keys.sort(compareStrings);
		for (k in keys) {
			var sub = props == null ? null : Reflect.field(props, k);
			if (sub != null) {
				walk(root, sub, Reflect.field(v, k), pointer(path, k), out, depth + 1);
			} else if (extra == false) {
				add(out, pointer(path, k), "is not an accepted property");
			} else if (extra != null && extra != true) {
				walk(root, extra, Reflect.field(v, k), pointer(path, k), out, depth + 1);
			}
		}
	}

	static function formatOk(format:String, s:String):Bool {
		return switch (format) {
			case "uri": URI_FORMAT.match(s);
			case "date": DATE_FORMAT.match(s) && validCalendarDate(s);
			case "date-time":
				var i = s.indexOf("T");
				if (i < 0) i = s.indexOf("t");
				i > 0 && DATE_FORMAT.match(s.substr(0, i)) && validCalendarDate(s.substr(0, i)) && TIME_FORMAT.match(s.substr(i + 1));
			case "uuid": UUID_FORMAT.match(s);
			case "email": EMAIL_FORMAT.match(s);
			default: true;
		}
	}

	static function validCalendarDate(s:String):Bool {
		var y = Std.parseInt(s.substr(0, 4));
		var m = Std.parseInt(s.substr(5, 2));
		var d = Std.parseInt(s.substr(8, 2));
		var leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
		var days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
		return d <= days[m - 1];
	}
}

#if macro
private typedef Ctx = {
	var stack:Array<String>;
	var recursive:Map<String, Bool>;
	var defs:Map<String, Dynamic>;
	var origin:Map<String, String>;
	var order:Array<String>;
}

private class SchemaBuilder {
	static var claimed:Map<String, String> = new Map();
	static var hooked = false;

	// ---- helpers -----------------------------------------------------

	static function fail(msg:String, pos:Position):Dynamic
		return Context.error(msg, pos);

	static function obj():Dynamic
		return {};

	static function set(o:Dynamic, k:String, v:Dynamic):Dynamic {
		Reflect.setField(o, k, v);
		return o;
	}

	public static function constString(e:Expr, what:String):String {
		return switch (e.expr) {
			case EConst(CString(s, _)): s;
			case EParenthesis(inner): constString(inner, what);
			default: Context.error('The $what must be a string literal so it is known at compile time.', e.pos);
		}
	}

	public static function claim(name:String, pos:Position):Void {
		if (!hooked) {
			hooked = true;
			Context.onAfterGenerate(function() claimed = new Map());
		}
		var info = Context.getPosInfos(pos);
		var here = info.file + ":" + info.min;
		var prior = claimed.get(name);
		if (prior != null && prior != here)
			Context.error('Duplicate tool name "$name". First declared at $prior. Clients key tools by name, so one would silently shadow the other.', pos);
		claimed.set(name, here);
	}

	public static function resolve(e:Expr):Type {
		switch (e.expr) {
			case EParenthesis({expr: ECheckType(_, ct)}):
				return Context.resolveType(ct, e.pos);
			case EConst(CIdent(_)), EField(_, _):
				var parts = [];
				var cur = e;
				while (true) {
					switch (cur.expr) {
						case EField(next, name):
							parts.unshift(name);
							cur = next;
							continue;
						case EConst(CIdent(name)):
							parts.unshift(name);
						default:
							Context.error("Pass a type name such as SearchArgs or pack.SearchArgs, or (null : Array<Int>).", e.pos);
					}
					break;
				}
				var name = parts.pop();
				return Context.resolveType(TPath({pack: parts, name: name}), e.pos);
			default:
				return Context.error("Pass a type name such as SearchArgs or pack.SearchArgs, or (null : Array<Int>).", e.pos);
		}
	}

	static function follow(t:Type):Type {
		while (true) {
			switch (t) {
				case TLazy(f): t = f();
				case TMono(r):
					var x = r.get();
					if (x == null) return t;
					t = x;
				default: return t;
			}
		}
	}

	static function stripNull(t:Type):{type:Type, nullable:Bool} {
		var nullable = false;
		while (true) {
			t = follow(t);
			switch (t) {
				case TAbstract(r, [inner]) if (r.get().name == "Null" && r.get().pack.length == 0):
					nullable = true;
					t = inner;
				default:
					return {type: t, nullable: nullable};
			}
		}
	}

	static function isDynamic(t:Type):Bool {
		return switch (follow(t)) {
			case TDynamic(_): true;
			case TAbstract(r, _): r.get().name == "Any" && r.get().pack.length == 0;
			default: false;
		}
	}

	static function clean(doc:Null<String>):String {
		if (doc == null) return "";
		var lines = [];
		for (raw in doc.split("\n")) {
			var l = StringTools.trim(raw);
			while (l.charAt(0) == "*" || l.charAt(0) == "/") l = StringTools.ltrim(l.substr(1));
			if (l != "") lines.push(l);
		}
		return ~/\s+/g.replace(lines.join(" "), " ");
	}

	// ---- root and budget ----------------------------------------------

	public static function root(t:Type, pos:Position):Dynamic {
		var ctx:Ctx = {stack: [], recursive: new Map(), defs: new Map(), origin: new Map(), order: []};
		var s = convert(t, TypeTools.toString(t), pos, ctx);
		var ref:Null<String> = Reflect.field(s, "$ref");
		if (ref != null) s = copy(ctx.defs.get(ref.substr("#/$defs/".length)));
		if (ctx.order.length > 0) {
			var names = ctx.order.copy();
			names.sort(function(a, b) return a < b ? -1 : (a > b ? 1 : 0));
			var defs = obj();
			for (n in names) set(defs, n, ctx.defs.get(n));
			set(s, "$defs", defs);
		}
		return s;
	}

	static function copy(o:Dynamic):Dynamic {
		var c = obj();
		for (k in Reflect.fields(o)) set(c, k, Reflect.field(o, k));
		return c;
	}

	public static function enforceBudget(what:String, schema:Dynamic, pos:Position):Void {
		var raw = Context.definedValue("mcp_schema_max_bytes");
		if (raw == null) return;
		var limit = Std.parseInt(raw);
		if (limit == null || limit <= 0) {
			Context.error('-D mcp_schema_max_bytes must be a positive integer, got "$raw".', pos);
			return;
		}
		var size = McpToolSchema.canonical(schema).length;
		if (size <= limit) return;
		var costs = [];
		var props = Reflect.field(schema, "properties");
		if (props != null)
			for (k in Reflect.fields(props))
				costs.push({name: k, bytes: McpToolSchema.canonical(Reflect.field(props, k)).length});
		costs.sort(function(a, b) return b.bytes - a.bytes);
		var top = [for (c in costs.slice(0, 3)) c.name + " (" + c.bytes + " bytes)"].join(", ");
		Context.error('The schema for $what is $size bytes, about ${Math.ceil(size / 4)} tokens, over the ${limit} byte budget. Largest properties: ${top == "" ? "none" : top}.', pos);
	}

	// ---- conversion ----------------------------------------------------

	static function convert(t:Type, path:String, pos:Position, ctx:Ctx):Dynamic {
		switch (t) {
			case TLazy(f):
				return convert(f(), path, pos, ctx);
			case TMono(r):
				var x = r.get();
				if (x == null) return fail('Cannot infer the type of $path. Annotate it explicitly.', pos);
				return convert(x, path, pos, ctx);
			case TType(r, params):
				var td = r.get();
				var under = TypeTools.applyTypeParameters(td.type, td.params, params);
				if (isAnon(under)) return named(TypeTools.toString(t), under, path, pos, ctx);
				return convert(under, path, pos, ctx);
			case TAnonymous(a):
				return structure(a.get().fields, path, pos, ctx);
			case TDynamic(_):
				return dynamicError(path, pos);
			case TFun(_, _):
				return fail('$path is a function type. Tool arguments are JSON, which cannot carry functions.', pos);
			case TEnum(r, _):
				return fail('$path is the enum ${r.get().name}. Haxe enums have no JSON wire form. Use an enum abstract over String instead.', pos);
			case TInst(r, params):
				var c = r.get();
				var full = c.pack.concat([c.name]).join(".");
				switch (full) {
					case "String":
						return set(obj(), "type", "string");
					case "Array":
						var o = set(obj(), "type", "array");
						return set(o, "items", convert(params[0], path + "[]", pos, ctx));
					default:
						return fail('$path is the class $full. Describe tool input with a typedef structure, classes carry behaviour JSON cannot.', pos);
				}
			case TAbstract(r, params):
				return abstractType(r.get(), params, path, pos, ctx);
		}
	}

	static function isAnon(t:Type):Bool {
		return switch (follow(t)) {
			case TAnonymous(_): true;
			default: false;
		}
	}

	static function dynamicError(path:String, pos:Position):Dynamic
		return fail('$path is Dynamic and JSON Schema cannot describe it. Give it a concrete type, or mark the field @:mcpAny to accept anything on purpose.', pos);

	static function named(key:String, anon:Type, path:String, pos:Position, ctx:Ctx):Dynamic {
		var safe = ~/[^A-Za-z0-9_.\-]/g.replace(key, "_");
		var prior = ctx.origin.get(safe);
		if (prior != null && prior != key)
			return fail('Types "$prior" and "$key" collapse to the same schema name "$safe". Rename one of them.', pos);
		ctx.origin.set(safe, key);
		var ref = set(obj(), "$ref", "#/$defs/" + safe);
		if (ctx.stack.indexOf(key) >= 0) {
			ctx.recursive.set(key, true);
			return ref;
		}
		ctx.stack.push(key);
		var s = convert(anon, path, pos, ctx);
		ctx.stack.pop();
		if (!ctx.recursive.exists(key)) return s;
		if (!ctx.defs.exists(safe)) ctx.order.push(safe);
		ctx.defs.set(safe, s);
		return ref;
	}

	static function abstractType(a:AbstractType, params:Array<Type>, path:String, pos:Position, ctx:Ctx):Dynamic {
		var full = a.pack.concat([a.name]).join(".");
		switch (full) {
			case "Int":
				return set(obj(), "type", "integer");
			case "UInt":
				return set(set(obj(), "type", "integer"), "minimum", 0);
			case "Float":
				return set(obj(), "type", "number");
			case "Bool":
				return set(obj(), "type", "boolean");
			case "Null":
				return convert(params[0], path, pos, ctx);
			case "Any":
				return dynamicError(path, pos);
			case "haxe.Int64", "haxe.Int64Helper":
				return fail('$path is Int64. JSON numbers lose precision past 2^53, send it as a String instead.', pos);
			case "haxe.ds.Map":
				var key = stripNull(params[0]).type;
				var okKey = switch (key) {
					case TInst(r, _): r.get().name == "String" && r.get().pack.length == 0;
					default: false;
				}
				if (!okKey) return fail('$path is a Map whose keys are not String. JSON object keys are always strings.', pos);
				var o = set(obj(), "type", "object");
				return set(o, "additionalProperties", convert(params[1], path + "{}", pos, ctx));
			default:
		}
		if (a.meta.has(":enum")) return enumAbstract(a, params, path, pos);
		if (a.meta.has(":coreType")) return fail('$path uses the core type $full, which has no JSON form.', pos);
		return convert(TypeTools.applyTypeParameters(a.type, a.params, params), path, pos, ctx);
	}

	static function constValue(te:TypedExpr, path:String, pos:Position):Dynamic {
		return switch (te.expr) {
			case TConst(TString(s)): s;
			case TConst(TInt(i)): i;
			case TConst(TFloat(s)): Std.parseFloat(s);
			case TConst(TBool(b)): b;
			case TCast(e, _), TParenthesis(e), TMeta(_, e): constValue(e, path, pos);
			default: fail('Could not read a constant value for an enum abstract member of $path.', pos);
		}
	}

	static function enumAbstract(a:AbstractType, params:Array<Type>, path:String, pos:Position):Dynamic {
		var under = stripNull(TypeTools.applyTypeParameters(a.type, a.params, params)).type;
		var kind = switch (under) {
			case TInst(r, _) if (r.get().name == "String"): "string";
			case TAbstract(r, _) if (r.get().name == "Int"): "integer";
			case TAbstract(r, _) if (r.get().name == "Float"): "number";
			default: fail('$path is an enum abstract over ${TypeTools.toString(under)}. Only String, Int and Float are supported.', pos);
		}
		var values:Array<Dynamic> = [];
		if (a.impl != null)
			for (f in a.impl.get().statics.get()) {
				if (!f.meta.has(":enum")) continue;
				var te = f.expr();
				if (te == null) return fail('Enum abstract member ${f.name} of $path has no value the compiler can read.', pos);
				var v = constValue(te, path, f.pos);
				if (values.indexOf(v) < 0) values.push(v);
			}
		if (values.length == 0) return fail('$path is an enum abstract with no members.', pos);
		var o = set(obj(), "type", kind);
		return set(o, "enum", values);
	}

	// ---- structures and field metadata -------------------------------------

	static function structure(fields:Array<ClassField>, path:String, pos:Position, ctx:Ctx):Dynamic {
		var props = obj();
		var required:Array<String> = [];
		var wireOf = new Map<String, String>();
		var nullableOptionals = Context.defined("mcp_schema_nullable_optionals");
		for (f in fields) {
			var fpath = path + "." + f.name;
			var wire = f.name;
			var description = clean(f.doc);
			var any = false;
			for (e in f.meta.get()) {
				switch (e.name) {
					case ":mcpName":
						wire = stringArg(e, fpath);
						if (!~/^[A-Za-z_][A-Za-z0-9_]*$/.match(wire))
							fail('@:mcpName on $fpath must be an identifier, got "$wire".', e.pos);
					case ":mcpDescription":
						description = StringTools.trim(stringArg(e, fpath));
					case ":mcpAny":
						any = true;
					default:
				}
			}
			if (wireOf.exists(wire))
				fail('Fields ${wireOf.get(wire)} and ${f.name} of $path both serialize as "$wire".', f.pos);
			wireOf.set(wire, f.name);
			var stripped = stripNull(f.type);
			var optional = f.meta.has(":optional") || stripped.nullable;
			var node:Dynamic;
			if (any) {
				if (!isDynamic(stripped.type)) fail('@:mcpAny on $fpath is only meaningful for Dynamic fields.', f.pos);
				node = obj();
			} else {
				node = convert(stripped.type, fpath, f.pos, ctx);
			}
			if (description != "") set(node, "description", description);
			constrain(node, f.meta.get(), fpath, f.pos);
			if (optional && nullableOptionals) {
				var nul = set(obj(), "type", "null");
				node = set(obj(), "anyOf", [node, nul]);
			}
			set(props, wire, node);
			if (!optional) required.push(wire);
		}
		var o = set(obj(), "type", "object");
		set(o, "properties", props);
		set(o, "additionalProperties", false);
		if (required.length > 0) set(o, "required", required);
		return o;
	}

	static function literal(e:Expr, fpath:String):Dynamic {
		return switch (e.expr) {
			case EConst(CInt(s)):
				var i = Std.parseInt(s);
				i != null && Std.string(i) == s ? i : Std.parseFloat(s);
			case EConst(CFloat(s)): Std.parseFloat(s);
			case EConst(CString(s, _)): s;
			case EConst(CIdent("true")): true;
			case EConst(CIdent("false")): false;
			case EParenthesis(inner): literal(inner, fpath);
			case EUnop(OpNeg, false, inner):
				var v:Dynamic = literal(inner, fpath);
				Std.isOfType(v, Int) || Std.isOfType(v, Float) ? -(v : Float) : Context.error("Cannot negate a non number.", e.pos);
			case EArrayDecl(items): [for (i in items) literal(i, fpath)];
			default: Context.error('Metadata on $fpath takes literal values only: numbers, strings, true, false or arrays of them.', e.pos);
		}
	}

	static function arg(e:MetadataEntry, fpath:String):Dynamic {
		if (e.params == null || e.params.length != 1)
			return Context.error('${e.name} on $fpath takes exactly one argument.', e.pos);
		return literal(e.params[0], fpath);
	}

	static function stringArg(e:MetadataEntry, fpath:String):String {
		var v:Dynamic = arg(e, fpath);
		if (!Std.isOfType(v, String)) Context.error('${e.name} on $fpath needs a string.', e.pos);
		return v;
	}

	static function numberArg(e:MetadataEntry, fpath:String):Float {
		var v:Dynamic = arg(e, fpath);
		if (!Std.isOfType(v, Int) && !Std.isOfType(v, Float)) Context.error('${e.name} on $fpath needs a number.', e.pos);
		return v;
	}

	static function countArg(e:MetadataEntry, fpath:String):Int {
		var v = numberArg(e, fpath);
		if (v < 0 || Math.floor(v) != v) Context.error('${e.name} on $fpath needs a whole number of zero or more.', e.pos);
		return Std.int(v);
	}

	static function need(kind:Null<String>, allowed:Array<String>, e:MetadataEntry, fpath:String):Void {
		if (kind == null || allowed.indexOf(kind) < 0)
			Context.error('${e.name} on $fpath cannot apply to ${kind == null ? "a reference or untyped field" : "a " + kind + " field"}. It needs ${allowed.join(" or ")}.', e.pos);
	}

	static function constrain(node:Dynamic, entries:Array<MetadataEntry>, fpath:String, pos:Position):Void {
		var kind:Null<String> = Reflect.field(node, "type");
		var defaultEntry:Null<MetadataEntry> = null;
		for (e in entries) {
			switch (e.name) {
				case ":mcpMin":
					need(kind, ["integer", "number"], e, fpath);
					set(node, "minimum", numberArg(e, fpath));
				case ":mcpMax":
					need(kind, ["integer", "number"], e, fpath);
					set(node, "maximum", numberArg(e, fpath));
				case ":mcpMinLength":
					need(kind, ["string"], e, fpath);
					set(node, "minLength", countArg(e, fpath));
				case ":mcpMaxLength":
					need(kind, ["string"], e, fpath);
					set(node, "maxLength", countArg(e, fpath));
				case ":mcpPattern":
					need(kind, ["string"], e, fpath);
					var p = stringArg(e, fpath);
					var problem = patternProblem(p);
					if (problem != null) Context.error('@:mcpPattern on $fpath is not a usable regular expression: $problem.', e.pos);
					set(node, "pattern", p);
				case ":mcpFormat":
					need(kind, ["string"], e, fpath);
					var fmt = stringArg(e, fpath);
					if (McpToolSchema.KNOWN_FORMATS.indexOf(fmt) < 0)
						Context.error('@:mcpFormat on $fpath: "$fmt" is not one of ${McpToolSchema.KNOWN_FORMATS.join(", ")}.', e.pos);
					set(node, "format", fmt);
				case ":mcpMinItems":
					need(kind, ["array"], e, fpath);
					set(node, "minItems", countArg(e, fpath));
				case ":mcpMaxItems":
					need(kind, ["array"], e, fpath);
					set(node, "maxItems", countArg(e, fpath));
				case ":mcpDefault":
					defaultEntry = e;
				case ":mcpName", ":mcpDescription", ":mcpAny":
				default:
					if (StringTools.startsWith(e.name, ":mcp"))
						Context.error('Unknown metadata ${e.name} on $fpath. Known: :mcpName :mcpDescription :mcpAny :mcpMin :mcpMax :mcpMinLength :mcpMaxLength :mcpPattern :mcpFormat :mcpMinItems :mcpMaxItems :mcpDefault.', e.pos);
			}
		}
		pairCheck(node, "minimum", "maximum", fpath, pos);
		pairCheck(node, "minLength", "maxLength", fpath, pos);
		pairCheck(node, "minItems", "maxItems", fpath, pos);
		if (defaultEntry != null) {
			var d = arg(defaultEntry, fpath);
			if (!Reflect.hasField(node, "$ref")) {
				var problems = McpToolSchema.validate(node, d);
				if (problems.length > 0)
					Context.error('@:mcpDefault on $fpath breaks its own schema: ${problems[0].message}.', defaultEntry.pos);
			}
			set(node, "default", d);
		}
	}

	/** The macro interpreter cannot catch a failed EReg compile, so check structure by hand. */
	static function patternProblem(p:String):Null<String> {
		var depth = 0;
		var inClass = false;
		var i = 0;
		var atomBefore = false;
		while (i < p.length) {
			var c = p.charAt(i);
			if (c == "\\") {
				if (i + 1 >= p.length) return "trailing backslash";
				i += 2;
				atomBefore = true;
				continue;
			}
			if (inClass) {
				if (c == "]") {
					inClass = false;
					atomBefore = true;
				}
			} else if (c == "[") {
				inClass = true;
				if (p.charAt(i + 1) == "^") i++;
				if (p.charAt(i + 1) == "]") i++;
			} else if (c == "(") {
				depth++;
				atomBefore = false;
			} else if (c == ")") {
				if (--depth < 0) return "unmatched )";
				atomBefore = true;
			} else if (c == "*" || c == "+" || c == "?") {
				if (!atomBefore && !(c == "?" && p.charAt(i - 1) == "(")) return "nothing to repeat before " + c;
			} else if (c == "|" || c == "^") {
				atomBefore = false;
			} else {
				atomBefore = true;
			}
			i++;
		}
		if (inClass) return "unclosed [";
		if (depth > 0) return "unclosed (";
		return null;
	}

	static function pairCheck(node:Dynamic, lo:String, hi:String, fpath:String, pos:Position):Void {
		if (Reflect.hasField(node, lo) && Reflect.hasField(node, hi)) {
			var a:Float = Reflect.field(node, lo);
			var b:Float = Reflect.field(node, hi);
			if (a > b) Context.error('$fpath has $lo ($a) above $hi ($b), so no value can pass.', pos);
		}
	}
}
#end
