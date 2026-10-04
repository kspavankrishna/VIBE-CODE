import haxe.Json;
import McpToolSchema.McpViolation;

enum abstract SortOrder(String) {
	var Asc = "asc";
	var Desc = "desc";
}

enum abstract Priority(Int) {
	var Low = 1;
	var High = 2;
}

typedef Label = {
	var name:String;
	@:optional var color:String;
}

typedef TreeNode = {
	var value:Int;
	@:optional var children:Array<TreeNode>;
}

typedef SearchArgs = {
	/** Full text query. Quote phrases for exact matches. */
	@:mcpMinLength(1) @:mcpMaxLength(10) var query:String;

	/** Page size. */
	@:optional @:mcpMin(1) @:mcpMax(50) @:mcpDefault(20) var limit:Int;

	@:optional var order:SortOrder;
	@:optional var priority:Priority;
	@:optional @:mcpMaxItems(2) var labels:Array<Label>;
	@:optional var attrs:Map<String, Int>;
	@:mcpName("repo_url") @:mcpFormat("uri") var repoUrl:String;
	@:optional @:mcpAny var extra:Dynamic;
	var maybe:Null<Bool>;
}

typedef NoArgs = {}

class McpToolSchemaTest {
	static var failures = 0;
	static var checks = 0;

	static function check(label:String, cond:Bool):Void {
		checks++;
		if (!cond) {
			failures++;
			Sys.println("FAIL " + label);
		}
	}

	static function has(label:String, violations:Array<McpViolation>, path:String, fragment:String):Void {
		var hit = false;
		for (v in violations)
			if (v.path == path && v.message.indexOf(fragment) >= 0) hit = true;
		check(label + " -> " + path + " " + fragment + " in " + Json.stringify(violations), hit);
	}

	static function main() {
		var search = McpToolSchema.tool("search_issues", "Search issues by text.", SearchArgs);
		var schema = search.inputSchema;
		check("name", search.name == "search_issues");
		check("object root", schema.type == "object" && schema.additionalProperties == false);
		check("draft", Reflect.field(schema, "$schema") == McpToolSchema.DRAFT);
		check("doc comment becomes description", schema.properties.query.description == "Full text query. Quote phrases for exact matches.");
		check("renamed field", Reflect.hasField(schema.properties, "repo_url") && !Reflect.hasField(schema.properties, "repoUrl"));
		var required:Array<String> = schema.required;
		required.sort(Reflect.compare);
		check("required set", required.join(",") == "query,repo_url");
		check("enum abstract string", (Reflect.field(schema.properties.order, "enum") : Array<Dynamic>).join(",") == "asc,desc");
		check("enum abstract int", schema.properties.priority.type == "integer" && (Reflect.field(schema.properties.priority, "enum") : Array<Dynamic>).join(",") == "1,2");
		check("map becomes additionalProperties", schema.properties.attrs.additionalProperties.type == "integer");
		check("default carried", Reflect.field(schema.properties.limit, "default") == 20);
		check("fingerprint shape", search._meta.schemaSha256.length == 64);
		check("fingerprint is stable", McpToolSchema.fingerprint(schema) == search._meta.schemaSha256);

		var good:Dynamic = {query: "crash", repo_url: "https://example.com/a/b", maybe: true, labels: [{name: "bug"}], attrs: {a: 1}, extra: ([1, "x"] : Array<Dynamic>)};
		check("valid input passes", McpToolSchema.isValid(schema, good));

		var bad:Dynamic = {query: "", limit: 99, order: "sideways", labels: ([{color: "red"}, {name: "a"}, {name: "b"}] : Array<Dynamic>), repo_url: "nope", stray: 1, attrs: {a: "x"}};
		var v = McpToolSchema.validate(schema, bad);
		has("empty query", v, "/query", "at least 1");
		has("limit high", v, "/limit", "<= 50");
		has("enum", v, "/order", "one of");
		has("nested required", v, "/labels/0/name", "required");
		has("too many items", v, "/labels", "at most 2");
		has("uri", v, "/repo_url", "valid uri");
		has("stray", v, "/stray", "not an accepted");
		has("map value", v, "/attrs/a", "expected integer");

		has("float for int", McpToolSchema.validate(schema, {query: "a", repo_url: "x:y", maybe: false, limit: 2.5}), "/limit", "expected integer");
		check("integral float is integer", McpToolSchema.isValid(schema, {query: "a", repo_url: "x:y", maybe: false, limit: 3.0}));
		has("wrong root", McpToolSchema.validate(schema, "text"), "", "expected object");

		var emoji = "\u{1F600}\u{1F600}";
		check("length counts code points", McpToolSchema.isValid(schema, {query: emoji, repo_url: "x:y", maybe: true}));
		has("length over code points", McpToolSchema.validate(schema, {query: "12345678901", repo_url: "x:y", maybe: true}), "/query", "at most 10");

		var tree = McpToolSchema.schemaOf(TreeNode);
		check("recursive type uses defs", tree.type == "object" && Reflect.hasField(tree, "$defs"));
		check("recursive ref", Reflect.field(tree.properties.children.items, "$ref") != null);
		check("tree valid", McpToolSchema.isValid(tree, {value: 1, children: [{value: 2, children: [{value: 3}]}]}));
		has("tree deep error", McpToolSchema.validate(tree, {value: 1, children: [{value: "x"}]}), "/children/0/value", "expected integer");
		var deep:Dynamic = {value: 0};
		var cur = deep;
		for (i in 0...(McpToolSchema.MAX_DEPTH + 5)) {
			var next:Dynamic = {value: i};
			Reflect.setField(cur, "children", [next]);
			cur = next;
		}
		var depthProblems = McpToolSchema.validate(tree, deep);
		check("depth guard stops runaway nesting", depthProblems.length == 1 && depthProblems[0].message.indexOf("nested deeper") >= 0);

		var arr = McpToolSchema.schemaOf((null : Array<Label>));
		check("array root", arr.type == "array" && arr.items.type == "object");
		var none = McpToolSchema.tool("ping", "Health check.", NoArgs);
		check("empty args", McpToolSchema.isValid(none.inputSchema, {}) && !McpToolSchema.isValid(none.inputSchema, {a: 1}));

		check("canonical sorts keys", McpToolSchema.canonical({b: 1, a: ([true, null, "x"] : Array<Dynamic>)}) == '{"a":[true,null,"x"],"b":1}');
		check("date-time accepted", McpToolSchema.isValid({type: "string", format: "date-time"}, "2026-02-28T10:00:00Z"));
		check("impossible date rejected", !McpToolSchema.isValid({type: "string", format: "date"}, "2026-02-30"));
		check("leap day accepted", McpToolSchema.isValid({type: "string", format: "date"}, "2028-02-29"));

		var nullable:Dynamic = {type: "object", properties: {n: {anyOf: [{type: "integer"}, {type: "null"}]}}, additionalProperties: false};
		check("anyOf accepts null", McpToolSchema.isValid(nullable, {n: null}) && McpToolSchema.isValid(nullable, {n: 4}));
		has("anyOf rejects other", McpToolSchema.validate(nullable, {n: "x"}), "/n", "any allowed shape");

		var reg = new McpToolRegistry();
		var calls = 0;
		reg.add(search, function(a) {
			calls++;
			if (a.query == "boom") throw "index offline";
			return {hits: 3, query: a.query};
		});
		reg.add(none, function(_) return "pong");
		var dup = false;
		try
			reg.add(none, function(_) return "x")
		catch (e:Dynamic)
			dup = true;
		check("duplicate registration throws", dup);
		var names = [for (t in (reg.list().tools : Array<Dynamic>)) t.name];
		check("list sorted", names.join(",") == "ping,search_issues");

		var okRes = reg.call("search_issues", good);
		check("call ok", okRes.isError == null && okRes.structuredContent.hits == 3 && okRes.content[0].type == "text");
		var before = calls;
		var rejected = reg.call("search_issues", bad);
		check("invalid args skip handler", calls == before && rejected.isError == true && rejected.content[0].text.indexOf("/limit: must be <= 50") >= 0);
		var thrown = reg.call("search_issues", {query: "boom", repo_url: "x:y", maybe: true});
		check("handler exception becomes isError", thrown.isError == true && thrown.content[0].text.indexOf("index offline") >= 0);
		check("string result", reg.call("ping", null).content[0].text == "pong");

		var list = reg.handleRequest({jsonrpc: "2.0", id: 1, method: "tools/list"});
		check("rpc list", list.id == 1 && list.result.tools.length == 2);
		check("rpc unknown tool", reg.handleRequest({jsonrpc: "2.0", id: 2, method: "tools/call", params: {name: "nope"}}).error.code == McpToolRegistry.INVALID_PARAMS);
		check("rpc unknown method", reg.handleRequest({jsonrpc: "2.0", id: 3, method: "x/y"}).error.code == McpToolRegistry.METHOD_NOT_FOUND);
		check("rpc notification is silent", reg.handleRequest({jsonrpc: "2.0", method: "tools/list"}) == null);

		reg.maxResultChars = 5;
		var clipped = reg.call("ping", {});
		check("results are clipped", clipped.content[0].text == "pong");
		var long = new McpToolRegistry();
		long.maxResultChars = 3;
		long.add(none, function(_) return "\u{1F600}\u{1F600}\u{1F600}\u{1F600}");
		var cut = long.call("ping", {}).content[0].text;
		check("clip keeps whole code points", cut == "\u{1F600}\u{1F600}\u{1F600}" + McpToolRegistry.TRUNCATION_MARKER);

		Sys.println(checks + " checks, " + failures + " failures");
		Sys.exit(failures == 0 ? 0 : 1);
	}
}
