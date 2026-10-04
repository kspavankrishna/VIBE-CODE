import haxe.Json;
import haxe.iterators.StringIteratorUnicode;

typedef McpToolHandler = Dynamic->Dynamic;

private typedef Entry = {
	var manifest:Dynamic;
	var handler:McpToolHandler;
}

class McpToolRegistry {
	public static inline var DEFAULT_MAX_RESULT_CHARS = 100000;
	public static inline var INVALID_PARAMS = -32602;
	public static inline var METHOD_NOT_FOUND = -32601;
	public static inline var TRUNCATION_MARKER = "\n[output truncated by tool registry]";

	/** Text results longer than this many code points are cut and marked. Zero disables the cap. */
	public var maxResultChars:Int = DEFAULT_MAX_RESULT_CHARS;

	final entries:Map<String, Entry> = new Map();

	public function new() {}

	/** Registers a manifest produced by McpToolSchema.tool. Throws on a duplicate or malformed manifest. */
	public function add(manifest:Dynamic, handler:McpToolHandler):McpToolRegistry {
		var name:Null<String> = Reflect.field(manifest, "name");
		var input = Reflect.field(manifest, "inputSchema");
		if (name == null || input == null || Reflect.field(input, "type") != "object")
			throw "McpToolRegistry.add needs a manifest from McpToolSchema.tool with an object inputSchema";
		if (entries.exists(name))
			throw 'McpToolRegistry: tool "$name" is already registered';
		entries.set(name, {manifest: manifest, handler: handler});
		return this;
	}

	public function has(name:String):Bool
		return entries.exists(name);

	/** The tools/list result. Sorted by name so the tool block of a prompt is byte stable between runs. */
	public function list():Dynamic {
		var names = [for (k in entries.keys()) k];
		names.sort(function(a, b) return a < b ? -1 : (a > b ? 1 : 0));
		return {tools: [for (n in names) entries.get(n).manifest]};
	}

	/**
		Validates arguments, runs the handler and always returns a tools/call result.
		Bad arguments and handler exceptions come back as isError results the model can read and correct.
		An unknown name throws, handleRequest turns that into a protocol error.
	**/
	public function call(name:String, args:Dynamic):Dynamic {
		var entry = entries.get(name);
		if (entry == null) throw 'Unknown tool: $name';
		if (args == null) args = {};
		var problems = McpToolSchema.validate(Reflect.field(entry.manifest, "inputSchema"), args);
		if (problems.length > 0)
			return failure('Invalid arguments for tool "$name". Fix these and call again:\n' + McpToolSchema.describeViolations(problems));
		var result:Dynamic;
		try {
			result = entry.handler(args);
		} catch (e:Dynamic) {
			return failure('Tool "$name" failed: ' + Std.string(e));
		}
		return shape(result);
	}

	/** Handles tools/list and tools/call JSON-RPC requests. Returns null for notifications. */
	public function handleRequest(request:Dynamic):Dynamic {
		var id:Dynamic = Reflect.field(request, "id");
		var method:String = Reflect.field(request, "method");
		var params:Dynamic = Reflect.field(request, "params");
		if (id == null) return null;
		switch (method) {
			case "tools/list":
				return ok(id, list());
			case "tools/call":
				var name:Dynamic = params == null ? null : Reflect.field(params, "name");
				if (!Std.isOfType(name, String))
					return error(id, INVALID_PARAMS, "tools/call needs a string params.name");
				if (!entries.exists(name))
					return error(id, INVALID_PARAMS, 'Unknown tool: $name');
				return ok(id, call(name, Reflect.field(params, "arguments")));
			default:
				return error(id, METHOD_NOT_FOUND, 'Method not found: $method');
		}
	}

	function ok(id:Dynamic, result:Dynamic):Dynamic
		return {jsonrpc: "2.0", id: id, result: result};

	function error(id:Dynamic, code:Int, message:String):Dynamic
		return {jsonrpc: "2.0", id: id, error: {code: code, message: message}};

	function failure(message:String):Dynamic
		return {content: [{type: "text", text: clip(message)}], isError: true};

	function shape(result:Dynamic):Dynamic {
		if (result != null && !Std.isOfType(result, String) && !Std.isOfType(result, Array)
			&& !Std.isOfType(result, Bool) && !Std.isOfType(result, Float)
			&& Std.isOfType(Reflect.field(result, "content"), Array))
			return result;
		var text = result == null ? "" : (Std.isOfType(result, String) ? result : Json.stringify(result));
		var out:Dynamic = {content: [{type: "text", text: clip(text)}]};
		var plain = result != null && !Std.isOfType(result, String) && !Std.isOfType(result, Array)
			&& !Std.isOfType(result, Bool) && !Std.isOfType(result, Float);
		if (plain) Reflect.setField(out, "structuredContent", result);
		return out;
	}

	function clip(text:String):String {
		if (maxResultChars <= 0) return text;
		var b = new StringBuf();
		var n = 0;
		for (c in new StringIteratorUnicode(text)) {
			if (n == maxResultChars) return b.toString() + TRUNCATION_MARKER;
			b.addChar(c);
			n++;
		}
		return text;
	}
}
