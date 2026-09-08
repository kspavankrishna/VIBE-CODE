// MCPToolRouter.ts — typed dispatch layer for MCP tool calls with inline JSON Schema validation

type Schema = { type: string; properties?: Record<string, Schema>; items?: Schema; required?: string[] };
type Handler = (input: Record<string, unknown>, ctx: CallContext) => Promise<unknown>;
type Middleware = (name: string, input: unknown, ctx: CallContext) => Promise<void>;

interface CallContext {
  requestId: string;
  startedAt: number;
  meta: Record<string, unknown>;
}

interface ToolDef {
  description: string;
  schema: Schema;
  handler: Handler;
}

export class MCPToolRouter {
  private tools = new Map<string, ToolDef>();
  private middleware: Middleware[] = [];
  private calls = 0;
  private errors = 0;

  register(name: string, def: ToolDef): this {
    if (this.tools.has(name)) throw new Error(`Tool already registered: ${name}`);
    this.tools.set(name, def);
    return this;
  }

  use(fn: Middleware): this {
    this.middleware.push(fn);
    return this;
  }

  async dispatch(name: string, raw: unknown): Promise<unknown> {
    const tool = this.tools.get(name);
    if (!tool) throw new Error(`Unknown tool: "${name}". Known: ${[...this.tools.keys()].join(', ')}`);

    const ctx: CallContext = { requestId: crypto.randomUUID(), startedAt: Date.now(), meta: {} };

    for (const mw of this.middleware) await mw(name, raw, ctx);

    const errs = validate(raw, tool.schema, name);
    if (errs.length) throw new Error(`Validation failed:\n${errs.join('\n')}`);

    this.calls++;
    try {
      return await tool.handler(raw as Record<string, unknown>, ctx);
    } catch (e) {
      this.errors++;
      throw e;
    }
  }

  manifest(): Array<{ name: string; description: string; inputSchema: Schema }> {
    return [...this.tools.entries()].map(([name, { description, schema }]) => ({
      name,
      description,
      inputSchema: schema,
    }));
  }

  stats() {
    return { calls: this.calls, errors: this.errors, tools: this.tools.size };
  }
}

function validate(value: unknown, schema: Schema, path: string): string[] {
  const errs: string[] = [];
  if (schema.type === 'object') {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      return [`${path}: expected object, got ${typeof value}`];
    }
    const obj = value as Record<string, unknown>;
    for (const req of schema.required ?? []) {
      if (!(req in obj)) errs.push(`${path}.${req}: required field missing`);
    }
    for (const [k, sub] of Object.entries(schema.properties ?? {})) {
      if (k in obj) errs.push(...validate(obj[k], sub, `${path}.${k}`));
    }
  } else if (schema.type === 'array') {
    if (!Array.isArray(value)) return [`${path}: expected array`];
    if (schema.items) value.forEach((v, i) => errs.push(...validate(v, schema.items!, `${path}[${i}]`)));
  } else {
    const expected = schema.type;
    const actual = Array.isArray(value) ? 'array' : typeof value;
    if (actual !== expected) errs.push(`${path}: expected ${expected}, got ${actual}`);
  }
  return errs;
}

/*
================================================================================
EXPLANATION
This solves the routing and validation mess you hit the moment you have more than
three MCP tools. Every MCP server ends up with a giant if/else dispatch block and
zero input validation — bugs hide until runtime. Built because I was wiring up a
multi-tool agent and kept chasing undefined errors from malformed tool calls.
Register tools with a JSON Schema, add middleware for logging or auth, and dispatch
by name. Validation runs inline — no Zod, no ajv, no dependencies. The manifest()
method spits out the MCP-ready schema array you hand to the model. Drop this into
any MCP server, Cloudflare Worker, or edge API handler managing multiple AI tools.
================================================================================
*/
