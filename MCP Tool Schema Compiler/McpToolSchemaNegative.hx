enum Color {
	Red;
}

enum abstract Mode(String) {
	var A = "a";
}

typedef DynamicArgs = {
	var payload:Dynamic;
}

typedef EnumArgs = {
	var color:Color;
}

typedef FunctionArgs = {
	var callback:String->Void;
}

typedef BadRange = {
	@:mcpMin(10) @:mcpMax(1) var n:Int;
}

typedef BadDefault = {
	@:mcpMin(1) @:mcpMax(5) @:mcpDefault(9) var n:Int;
}

typedef BadTarget = {
	@:mcpMinLength(1) var n:Int;
}

typedef TypoMeta = {
	@:mcpMinimum(1) var n:Int;
}

typedef BadPattern = {
	@:mcpPattern("(") var s:String;
}

typedef BadDefaultEnum = {
	@:mcpDefault("zzz") @:optional var m:Mode;
}

typedef Dupe = {
	@:mcpName("x") var a:String;
	@:mcpName("x") var b:String;
}

typedef BigArgs = {
	var alpha:String;
	var beta:Array<String>;
}

typedef IntKeys = {
	var m:Map<Int, String>;
}

class McpToolSchemaNegative {
	static function main() {
		#if case_dynamic
		McpToolSchema.tool("t", "d", DynamicArgs);
		#elseif case_enum
		McpToolSchema.tool("t", "d", EnumArgs);
		#elseif case_function
		McpToolSchema.tool("t", "d", FunctionArgs);
		#elseif case_range
		McpToolSchema.tool("t", "d", BadRange);
		#elseif case_default
		McpToolSchema.tool("t", "d", BadDefault);
		#elseif case_target
		McpToolSchema.tool("t", "d", BadTarget);
		#elseif case_typo
		McpToolSchema.tool("t", "d", TypoMeta);
		#elseif case_pattern
		McpToolSchema.tool("t", "d", BadPattern);
		#elseif case_default_enum
		McpToolSchema.tool("t", "d", BadDefaultEnum);
		#elseif case_dupe_field
		McpToolSchema.tool("t", "d", Dupe);
		#elseif case_dupe_tool
		McpToolSchema.tool("same", "d", BigArgs);
		McpToolSchema.tool("same", "d", BigArgs);
		#elseif case_name
		McpToolSchema.tool("has space", "d", BigArgs);
		#elseif case_desc
		McpToolSchema.tool("t", "  ", BigArgs);
		#elseif case_budget
		McpToolSchema.tool("t", "d", BigArgs);
		#elseif case_not_object
		McpToolSchema.tool("t", "d", (null : Array<Int>));
		#elseif case_int_keys
		McpToolSchema.tool("t", "d", IntKeys);
		#end
	}
}
