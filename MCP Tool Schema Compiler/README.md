# MCP Tool Schema Compiler

Write your MCP tool arguments once as a Haxe typedef and let the compiler produce the JSON Schema, the tool manifest and the argument validator. If the schema cannot be expressed or contradicts itself, the build fails with a pointer at the offending field.

**Language:** Haxe | **Lines:** 1231 | **Added:** 2026-10-04

## What this solves

Every MCP server and every function calling agent has the same quiet bug. The tool schema the model sees and the type the handler actually uses are written twice, by hand, in two places. They drift. A field gets renamed in code and the schema still advertises the old name. A `maximum` is added to the schema but never enforced in the handler. A default is documented in the description and implemented differently in the function. The model keeps calling the tool the old way and you find out from a production trace.

The usual fixes are partial. Reflection based schema generators run at startup, so a bad type shows up when the server boots, or worse, when the first request arrives. Hand written schemas are checked by nothing. Libraries that generate a schema from a class usually give you no say over the wire form, so you cannot rename a field to snake case or cap a string length without a second source of truth.

This project moves the whole problem to compile time. A Haxe build macro reads the typedef, walks it, builds the JSON Schema and embeds it in your program as a literal. A runtime validator built from the same code checks real arguments against that literal, so the schema you publish is the schema you enforce. Haxe also compiles to JavaScript, Python, Lua, C++, Java, C# and the HashLink and eval targets, so one definition can serve a Node MCP server, an edge worker and a native tool host.

## Why I built it

I kept finding the same three failures when reviewing agent tool code. First, tool lists that changed order between runs, which quietly breaks prompt caching because the tool block is part of the cached prefix. Second, schemas that were too big. A single sloppy tool with a deeply nested object can cost thousands of tokens on every request, and nobody notices because nobody measures it. Third, argument errors that were reported as protocol failures, so the model never saw what it got wrong and just retried the same bad call.

Haxe macros are a good fit for this because they run inside the compiler with full access to the typed AST. They can see doc comments, metadata, optional markers and enum abstract values. They can also stop the build with an error positioned on the exact field. That is the property I wanted: a bad tool definition should never reach a running server.

I also wanted it to be strict by default. A field typed as Dynamic is rejected. A Haxe enum is rejected because it has no JSON form. An Int64 is rejected because JSON numbers lose precision. A default that violates its own limits is rejected. A typo in a metadata name is rejected. All of these are cases where a lenient generator emits something plausible that breaks later.

## When to use it

- You are writing an MCP server in Haxe, or you want a single schema source that several language targets share.
- You run a tool registry for an agent and want the manifest, the validation and the error shape to come from one definition.
- You want CI to fail when a tool schema grows past a token budget.
- You want a stable fingerprint per tool so you can detect schema changes between releases.

It is not a general purpose JSON Schema library. The validator covers exactly the subset the compiler emits and nothing more. If you need `oneOf`, `if/then` or remote references, use a full validator.

## How it works

The project has two source modules and a test suite.

**McpToolSchema.hx** holds the two macros, the canonical form and the validator.

`McpToolSchema.tool(name, description, ArgsType)` is the main macro. It requires a string literal for the name and the description. The name must match `TOOL_NAME`, which is the intersection of what MCP and the major model APIs accept: letters, digits, underscore and dash, up to 64 characters. The description must not be blank because the model picks tools from it. The macro then resolves the type, builds the schema and returns an object literal with `name`, `description`, `inputSchema` and a `_meta.schemaSha256` fingerprint. The root must be an object, as MCP requires.

`McpToolSchema.schemaOf(Type)` does the same without the tool wrapper, so it works for any supported type, including `(null : Array<Label>)` for an array root.

Inside the macro, a private builder class does the work. `convert` switches over the Haxe type. Int becomes integer, UInt becomes integer with a minimum of zero, Float becomes number, Bool becomes boolean, String becomes string and Array becomes an array with an items schema. `Map<String, T>` becomes an object with `additionalProperties` set to the value schema, and a Map with non string keys fails the build. Enum abstracts over String, Int or Float go through `enumAbstract`, which reads each member constant and emits an `enum` list.

Structures go through `structure`. Every typedef object gets `additionalProperties: false`. A field is optional if it carries the optional marker or has a `Null<T>` type, and otherwise it lands in `required`. Doc comments are cleaned by `clean` and become the field description, which is the cheapest place to teach the model how to use an argument.

Recursive types are handled by `named`. It keeps a stack of type names. When a type refers back to itself, the macro emits a `$ref` into `$defs` and records the type as recursive. `root` then collects the definitions, sorts them for determinism and attaches them. If the root type is itself recursive, `root` inlines the first level so the root still has `type: object`.

Field level metadata is applied by `constrain`. The supported tags are `@:mcpMin`, `@:mcpMax`, `@:mcpMinLength`, `@:mcpMaxLength`, `@:mcpPattern`, `@:mcpFormat`, `@:mcpMinItems`, `@:mcpMaxItems`, `@:mcpDefault`, `@:mcpName`, `@:mcpDescription` and `@:mcpAny`. Each one checks that it fits the field type, so a length limit on an integer fails the build. `pairCheck` rejects a minimum above its maximum. Any other tag starting with `:mcp` is an error, which catches typos. `@:mcpDefault` is checked by running the real `validate` function against the schema node at compile time, so a default of 9 on a field capped at 5 stops the build. `patternProblem` checks regex structure by hand because the macro interpreter cannot recover from a failed regex compile.

`enforceBudget` reads the `mcp_schema_max_bytes` define. When a schema is over budget the error reports the size, an approximate token count and the three most expensive properties, so you know what to trim. `claim` rejects duplicate tool names within one compilation and clears its table after generation so a compilation server does not report false duplicates.

`canonical` writes JSON with sorted keys and `fingerprint` hashes it with SHA 256. The same schema always gives the same hash, whatever order the fields were declared in.

The runtime half is `validate`, `isValid` and `describeViolations`. `validate` returns every violation with a JSON pointer path such as `/labels/0/name`, not just the first. Details that matter in practice: string length counts Unicode code points, not UTF 16 units, so an emoji counts as one character as the JSON Schema spec says. A float with no fractional part counts as an integer. Non finite numbers are rejected. `MAX_DEPTH` stops a hostile deeply nested value from running the validator out of stack. The formats `uri`, `date`, `date-time`, `uuid` and `email` are checked, and `date` checks the calendar, so 2026-02-30 fails and 2028-02-29 passes.

**McpToolRegistry.hx** is the runtime side. `add` takes a manifest and a handler and refuses duplicates or non object schemas. `list` returns the `tools/list` result sorted by name so the tool block is byte stable. `call` validates arguments first and never runs the handler on bad input. Invalid arguments and handler exceptions both come back as a result with `isError` set and text the model can act on, rather than a protocol error it cannot see. Plain object results are also returned as `structuredContent`. `maxResultChars` clips long text at a code point boundary and appends `TRUNCATION_MARKER`. `handleRequest` answers `tools/list` and `tools/call` JSON RPC requests, returns `INVALID_PARAMS` for an unknown tool, `METHOD_NOT_FOUND` for an unknown method and null for notifications.

**McpToolSchemaTest.hx** and **McpToolSchemaNegative.hx** are the tests. The first runs the happy paths and runtime edge cases. The second holds one deliberately bad definition per `case_` define, and **RunTests.sh** compiles each one and checks that the build fails with the expected message. A guard that is never seen to fire is not a guard.

## Usage

Define the arguments as a typedef and build the manifest.

```haxe
import McpToolSchema;

enum abstract SortOrder(String) {
	var Asc = "asc";
	var Desc = "desc";
}

typedef SearchArgs = {
	/** Full text query. Quote phrases for exact matches. */
	@:mcpMinLength(1) @:mcpMaxLength(200) var query:String;

	/** Page size. */
	@:optional @:mcpMin(1) @:mcpMax(50) @:mcpDefault(20) var limit:Int;

	@:optional var order:SortOrder;
	@:mcpName("repo_url") @:mcpFormat("uri") var repoUrl:String;
}

class Server {
	static function main() {
		var search = McpToolSchema.tool("search_issues", "Search issues by text.", SearchArgs);

		var registry = new McpToolRegistry();
		registry.add(search, function(args) {
			return {hits: 3, query: args.query};
		});

		var reply = registry.handleRequest({jsonrpc: "2.0", id: 1, method: "tools/list"});
		trace(haxe.Json.stringify(reply));

		var problems = McpToolSchema.validate(search.inputSchema, {query: "", limit: 99});
		trace(McpToolSchema.describeViolations(problems));
	}
}
```

Run it, and run the suite, from the folder:

```bash
haxe -cp . -main Server --interp
./RunTests.sh
```

Enforce a token budget in CI by adding `-D mcp_schema_max_bytes=2000` to the build. Add `-D mcp_schema_nullable_optionals` if your model tends to send null for optional arguments, and optional fields will then accept null.

## Notes

- Developed and tested on Haxe 4.3.3 with the eval target. The runtime code uses only the standard library, so the other targets should behave the same, but I only ran eval.
- The pattern keyword is matched with Haxe EReg at runtime, which is PCRE style on most targets. Stick to patterns that mean the same in ECMA regex if the schema is also read by other tools.
- Optional fields do not accept null unless you set the nullable define. That is deliberate, because silently accepting null hides model mistakes.
- Handlers receive a decoded Dynamic. Typing the handler argument as your typedef is a cast, not a conversion, and works because validation already passed.
- The tool name check allows 64 characters even though the MCP spec allows more. The shorter limit is what the main model APIs enforce.
- Not supported: unions, plain Haxe enums, classes, Int64 and `oneOf`. Each fails the build with a message that says what to use instead.
- Input errors are returned as tool results so the model can correct itself. Protocol errors are used only for unknown tools and unknown methods.
